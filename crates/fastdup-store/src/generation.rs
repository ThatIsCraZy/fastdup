//! Generation repository facade: shared ownership, opaque proof state and stable public exports.
//! See generation/README.md for ownership and verification boundaries.

mod checkpoint_copy;
mod commit;
mod error;
mod graph;
mod liveness;
mod manifest_edits;
mod manifests;
mod metadata;
mod metadata_gc;
mod pins;
mod recovery;
mod results;
mod verification;

use crate::generation_log::GenerationLog;
use crate::manifest_tree::ManifestTreeSummary;
use crate::{ContainerRepository, StorageIo, StoreError};
use error::map_log_error;
use fastdup_format::{CommitRecord, MetadataObjectId, PolicySetId};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock, Weak};

pub use error::GenerationError;
pub use results::WalTail;
pub use results::{
    CommittedDataGeneration, GenerationLivenessDelta, GenerationScrubSummary,
    MetadataGcExactReason, MetadataGcMarkMode, MetadataGcMetrics, RecoveredDataGeneration,
    RecoveredGeneration, VerifiedCommittedFile,
};
pub(crate) use results::{GenerationLivenessProof, GenerationMetadataGcSummary};

const METADATA_SUFFIX: &str = ".fdm";

// Each growing StorageIo write advances and synchronizes the aligned length
// head. Batch the already encoded immutable object at the backend I/O quantum
// instead of forcing one head write and durability barrier per 4-KiB page.
const WRITE_BLOCK_BYTES: usize = 1024 * 1024;

const MAX_METADATA_OBJECT_BYTES_U64: u64 = 16 * 1_024 * 1_024;

const MAX_METADATA_MARK_DELTA_RUNS: u32 = 32;

type VerifiedManifests = Vec<(u64, ManifestTreeSummary)>;

/// Opaque identity of the Commit Record that an online Successor Graph Proof
/// is allowed to extend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SuccessorPredecessor {
    record: CommitRecord,
}

impl SuccessorPredecessor {
    /// Binds a successor attempt to one record previously returned by a
    /// successful commit. The repository rechecks that this exact record is
    /// still its installed head before advancing the WAL.
    #[must_use]
    pub const fn from_committed_record(record: CommitRecord) -> Self {
        Self { record }
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.record.generation()
    }
}

/// Opaque process-local proof for one newly published or safely reused
/// Manifest graph. Construction is restricted to verified repository paths.
#[derive(Clone, Debug)]
pub struct ManifestSuccessorProof {
    predecessor: SuccessorPredecessor,
    summary: ManifestTreeSummary,
    introduced_chunks: BTreeMap<fastdup_format::ChunkId, u64>,
    introduced_metadata: BTreeSet<MetadataObjectId>,
    metadata_root_pin: MetadataRootPin,
}

impl ManifestSuccessorProof {
    #[must_use]
    pub const fn summary(&self) -> ManifestTreeSummary {
        self.summary
    }
}

#[derive(Clone, Debug)]
pub struct GenerationRepository<I> {
    storage: I,
    supported_policy: PolicySetId,
    commit_lock: Arc<Mutex<()>>,
    manifest_cache: Arc<crate::manifest_cache::ManifestNodeCache>,
    metadata_cache: Arc<crate::metadata_object_cache::MetadataObjectCache>,
    metadata_root_pins: Arc<Mutex<BTreeMap<MetadataObjectId, usize>>>,
    metadata_root_pin_handles: Arc<Mutex<Vec<Weak<MetadataRootPinInner>>>>,
    recovery_checkpoint_root_pins: Arc<Mutex<BTreeMap<MetadataObjectId, usize>>>,
    metadata_gc_barrier: Arc<RwLock<()>>,
    metadata_gc_epoch: Arc<AtomicU64>,
    metadata_gc_clean: Arc<Mutex<Option<MetadataGcCleanState>>>,
    metadata_gc_delta: Arc<Mutex<MetadataGcDeltaJournal>>,
    metadata_gc_run_lock: Arc<Mutex<()>>,
    maintenance_cancellation: Option<crate::MaintenanceCancellation>,
}

#[derive(Clone)]
pub(crate) struct MetadataRootPin {
    inner: Arc<MetadataRootPinInner>,
}

struct MetadataRootPinInner {
    root: MetadataObjectId,
    pins: Arc<Mutex<BTreeMap<MetadataObjectId, usize>>>,
    metadata_gc_epoch: Arc<AtomicU64>,
    release_requires_exact: AtomicBool,
    metadata_gc_delta: Arc<Mutex<MetadataGcDeltaJournal>>,
}

struct RecoveryCheckpointRootPin {
    root: MetadataObjectId,
    pins: Arc<Mutex<BTreeMap<MetadataObjectId, usize>>>,
    metadata_gc_epoch: Arc<AtomicU64>,
    metadata_gc_delta: Arc<Mutex<MetadataGcDeltaJournal>>,
}

struct RecoveryCheckpointCandidate {
    record: CommitRecord,
    _pin: RecoveryCheckpointRootPin,
}

#[derive(Clone, Copy, Debug)]
struct MetadataGcCleanState {
    epoch: u64,
    objects_retained: u64,
    catalog_generation: u64,
    delta_run_count: u32,
}

#[derive(Debug, Default)]
struct MetadataGcDeltaJournal {
    revision: u64,
    exact_required: bool,
    exact_reason: Option<MetadataGcExactReason>,
    unclassified: BTreeSet<MetadataObjectId>,
    additions: BTreeSet<MetadataObjectId>,
}

#[derive(Clone, Copy)]
struct StagedMetadata {
    object_id: MetadataObjectId,
    published_new: bool,
}

#[derive(Clone)]
struct CommittedMetadata {
    record: CommitRecord,
    introduced_namespace_metadata: BTreeSet<MetadataObjectId>,
    wal_rotated: bool,
}

struct PublishedManifestProof {
    summary: ManifestTreeSummary,
    introduced_chunks: BTreeMap<fastdup_format::ChunkId, u64>,
    introduced_metadata: BTreeSet<MetadataObjectId>,
    metadata_root_pin: MetadataRootPin,
}

/// Verifies that every logical Chunk required by one metadata graph has at
/// least one durable, byte-exact physical Location.
///
/// Implementations may use rebuild scans or nonauthoritative acceleration, but
/// success is a complete graph proof. A negative index hint alone can never
/// satisfy this interface.
pub trait RequiredChunkVerifier {
    /// Verifies every unique `(Chunk ID, logical length)` dependency.
    ///
    /// # Errors
    ///
    /// Returns the first missing, conflicting, corrupt, unsupported, or I/O
    /// failure without exposing a partial proof.
    fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError>;
}

/// Complete graph verifier using one pinned Exact-Index generation with the
/// authoritative verified Container scan as a single fallback.
#[derive(Clone, Debug)]
pub struct IndexedRequiredChunkVerifier<C, X> {
    containers: ContainerRepository<C>,
    index: crate::ExactIndexGenerationPin<X>,
    read_cache: Option<Arc<crate::VerifiedReadCache>>,
}

impl<I: StorageIo> GenerationRepository<I> {
    #[must_use]
    pub fn new(storage: I, supported_policy: PolicySetId) -> Self {
        Self {
            storage,
            supported_policy,
            commit_lock: Arc::new(Mutex::new(())),
            manifest_cache: Arc::new(crate::manifest_cache::ManifestNodeCache::system()),
            metadata_cache: Arc::new(crate::metadata_object_cache::MetadataObjectCache::system()),
            metadata_root_pins: Arc::new(Mutex::new(BTreeMap::new())),
            metadata_root_pin_handles: Arc::new(Mutex::new(Vec::new())),
            recovery_checkpoint_root_pins: Arc::new(Mutex::new(BTreeMap::new())),
            metadata_gc_barrier: Arc::new(RwLock::new(())),
            metadata_gc_epoch: Arc::new(AtomicU64::new(0)),
            metadata_gc_clean: Arc::new(Mutex::new(None)),
            metadata_gc_delta: Arc::new(Mutex::new(MetadataGcDeltaJournal::default())),
            metadata_gc_run_lock: Arc::new(Mutex::new(())),
            maintenance_cancellation: None,
        }
    }

    pub(crate) fn with_maintenance_cancellation(
        mut self,
        token: crate::MaintenanceCancellation,
    ) -> Self {
        self.maintenance_cancellation = Some(token);
        self
    }

    fn check_maintenance(&self) -> io::Result<()> {
        crate::maintenance_cancellation::check_io(self.maintenance_cancellation.as_ref())
    }

    /// Reports whether the paired Commit WAL selects at least one Commit
    /// record without traversing its Metadata or DATA graph.
    ///
    /// This is the empty-target gate for disaster recovery. A malformed or
    /// torn existing WAL returns an error and is never treated as an empty
    /// Metadata tier.
    ///
    /// # Errors
    ///
    /// Returns a Generation-Log, format, or storage error if the paired WAL
    /// cannot be inspected as valid durable state.
    ///
    /// # Panics
    ///
    /// Panics if another repository operation poisoned the Commit lock.
    pub fn has_committed_generation(&self) -> Result<bool, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation inspection lock poisoned");
        GenerationLog::new(&self.storage)
            .load_for_recovery()
            .map(|snapshot| snapshot.is_some_and(|snapshot| !snapshot.records().is_empty()))
            .map_err(map_log_error)
    }
}

#[cfg(test)]
#[path = "generation/tests.rs"]
mod tests;
