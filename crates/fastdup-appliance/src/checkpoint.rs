//! Writable Namespace checkpoint orchestration.
//!
//! The public [`DurableNamespace`] interface coordinates recovery and commit
//! lifecycle. Telemetry, frozen-Manifest planning/publication and bounded
//! write-through reduction live in focused internal modules below this seam.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, mpsc};
use std::time::{Duration, Instant};

#[cfg(test)]
use fastdup_format::ContainerId;
use fastdup_format::{
    ChunkId, CommitRecord, ExactIndexEntry, ExactIndexProfileId, MetadataFormatError,
    MetadataObjectId, NamespaceRoot, PolicySetId, PrehashedChunk,
};
#[cfg(test)]
use fastdup_posix::InodeId;
use fastdup_posix::{CommittedFile, Namespace, NamespaceConfig, PosixError, PreparedDataRecipe};
use fastdup_store::{
    CONTAINER_GENERATION_RESERVATION_SPAN_V1, ContainerDescriptorCacheStatus,
    ContainerGenerationAllocator, ContainerPlacement, ContainerRepository,
    ExactIndexPageCacheStatus, ExactIndexRunRepository, ExactRunMembershipStatus, GenerationError,
    GenerationRepository, IndexedRequiredChunkVerifier, ManifestReadError, ManifestTreeSummary,
    PersistentChunkPlan, PersistentReductionIndex, PersistentReductionStatus,
    RequiredChunkVerifier, SeqCdcConfig, SimilarityIndexPageCacheStatus, SimilarityIndexRepository,
    StorageIo, StoreError, SuccessorPredecessor, VerifiedManifestFile, VerifiedReadCache,
    VerifiedReadCacheError, VerifiedReadCacheStatus, WorkerPermits,
};
use hashbrown::HashTable;

#[cfg(test)]
use crate::ManifestCommittedFile;
use crate::historical_proof_cache::{HistoricalProofAdmission, HistoricalProofCache};
use crate::proof_cache_trace::ProofCacheTraceRecorder;
use crate::{
    HistoricalProofCacheStatus, MountError, ProofCacheEvent, ProofCacheReplayError,
    ProofCacheTrace, ProofKey, namespace_from_verified_files_using,
};

const FIRST_REGULAR_INODE: u64 = 2;
/// Production Inode IDs durably reserved per writable appliance start.
///
/// A 2^32 range keeps allocation entirely in-memory for any practical process
/// lifetime while a restart can still skip its unused suffix without reuse.
pub const INODE_RESERVATION_SPAN_V1: u64 = 1_u64 << 32;
const CONTAINER_PAYLOAD_TARGET_BYTES: usize = 32 * 1_024 * 1_024;
const COMPRESSION_REGION_TARGET_BYTES: usize = 512 * 1_024;
const CDC_MINIMUM_BYTES: usize = 16 * 1_024;
const CDC_MAXIMUM_BYTES: usize = 256 * 1_024;
const SEQCDC_CONFIG_V1: SeqCdcConfig = SeqCdcConfig {
    sequence_length: 6,
    skip_trigger: 50,
    skip_bytes: 1_024,
    minimum_bytes: CDC_MINIMUM_BYTES,
    maximum_bytes: CDC_MAXIMUM_BYTES,
};
const EXACT_PUBLICATION_QUEUE_BATCHES: usize = 8;
// ACTIVE additions join one bounded collection buffer and publish as one L0
// family and one activation when the entry bound, the command bound, or the
// collection window is reached. A Flush fence and every non-ACTIVE transition
// force the buffer out first; activation history stays acceleration-only, so
// the bounded window never delays durability, only exact-hit reuse which the
// recent overlay already covers.
const EXACT_PUBLICATION_BATCH_ENTRIES: usize = 16_384;
const EXACT_PUBLICATION_COALESCE_WINDOW: Duration = Duration::from_millis(50);
const MAX_RECENT_EXACT_LOCATIONS: usize = 8_192;
// Shared admission cap for cached Active and Frozen dependency proofs.
// Externalized DATA and short boundary Chunks are not bounded by resident bytes.
const MAX_ONLINE_DEPENDENCY_PROOFS_V1: usize = 65_536;

fn seqcdc_force_scalar() -> bool {
    static FORCE_SCALAR: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FORCE_SCALAR.get_or_init(|| {
        std::env::var("FASTDUP_SEQCDC_FORCE_SCALAR").is_ok_and(|value| value == "1")
    })
}

mod manifest_planning;
mod metrics;
mod write_through;

#[cfg(test)]
use manifest_planning::SeqCdcStream;
pub use manifest_planning::checkpoint_exact_index_profile_v1;
use manifest_planning::{AdaptiveCommitWriter, load_manifest_cache, plan_checkpoint_manifest};
pub use metrics::{
    CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, CheckpointMetrics, CheckpointPhaseMetrics, CpuPhaseStatus,
    ProfiledCheckpoint, WriteThroughStatus,
};
use metrics::{CheckpointStage, CheckpointTimings};
use write_through::{WriteThroughIngest, install_write_through};

/// Returns the immutable identity of the currently implemented durable
/// checkpoint writer policy.
///
/// The canonical bytes pin SeqCDC-v1, region sizing, adaptive Zstd thresholds,
/// Exact publication, the optional coherent Similarity profile, bounded
/// candidate/trial counts, and Depth-1 dependent-codec admission. Every new
/// repository uses this Policy Set from its first Commit. Disabling dependent
/// selection or lacking a usable Similarity snapshot selects the independent
/// RAW/Zstd fallback without changing the Policy Set.
/// The legacy `paired-exact-v1`/`contiguous-only` spelling remains byte-stable
/// for existing repositories: online hint refresh and one-time materialization
/// change candidate admission, not any permitted durable record or codec.
///
/// # Panics
///
/// Panics only if BLAKE3 maps the fixed canonical policy bytes to the reserved
/// all-zero identity, an impossible production `ASSERT` for this pinned input.
#[must_use]
pub fn checkpoint_policy_set() -> PolicySetId {
    PolicySetId::new(
        ChunkId::of(
            b"fastdup/checkpoint-policy-v3/SeqCDC=increasing:seq6:skip-trigger50:skip1024:min16384:max262144:append-tail-anchor-v1/region=524288/Zstd=level3:min4096:min3pct/exact=l0-runs-v2:fanin4:partition262144/proof=installed-successor-delta-v1/similarity=fingerprint-v1:bucket-v1:candidates16:trials4:paired-exact-v1/dependent=codec3-zstd-prefix+codec4-sparse-xor:depth1:min4096:min5pct:contiguous-only",
        )
        .bytes(),
    )
    .expect("ASSERT: the current checkpoint Policy Set hash is nonzero")
}

/// Writable namespace plus the durable generation machinery behind it.
///
/// The module owns the only checkpoint serialization lock. POSIX callers use
/// [`Self::namespace`] and do not need to know about manifests, containers, or
/// Commit Records.
#[derive(Debug)]
pub struct DurableNamespace<M, C> {
    startup_data_verification: Option<fastdup_store::PendingDataVerification>,
    namespace: Arc<Namespace>,
    generations: GenerationRepository<M>,
    containers: ContainerRepository<C>,
    checkpoint_lock: Mutex<()>,
    checkpoint_timings: CheckpointTimings,
    installed_predecessor: Mutex<SuccessorPredecessor>,
    manifests: Mutex<Vec<InstalledManifest>>,
    container_generations: ContainerGenerationAllocator<C>,
    manifest_readers: Arc<dyn ManifestReaderPolicy<C>>,
    checkpoint_workers: NonZeroUsize,
    write_through: Arc<WriteThroughIngest<C>>,
    online_dependency_proofs: Arc<OnlineDependencyProofs>,
}

#[derive(Clone, Copy)]
enum RecoveryMode {
    Full,
    Structural,
    Committed,
}

#[derive(Clone, Copy, Debug)]
struct GenerationProof {
    entry: ExactIndexEntry,
    admission: HistoricalProofAdmission,
}

/// Full identities live once in the arena; table buckets carry only ordinals.
/// Entries never move independently or disappear while the table is queryable.
#[derive(Debug, Default)]
struct GenerationProofMap {
    index: HashTable<u32>,
    entries: Vec<GenerationProof>,
}

impl GenerationProofMap {
    fn key(proof: &GenerationProof) -> (ChunkId, u32) {
        (proof.entry.chunk_id(), proof.entry.logical_length())
    }

    fn hash(key: (ChunkId, u32)) -> u64 {
        let bytes = key.0.bytes();
        u64::from_le_bytes(bytes[..8].try_into().expect("fixed Chunk ID prefix"))
            ^ u64::from_le_bytes(bytes[24..].try_into().expect("fixed Chunk ID suffix"))
                .rotate_left(23)
            ^ u64::from(key.1).wrapping_mul(0x9E37_79B1_85EB_CA87)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn get(&self, key: &(ChunkId, u32)) -> Option<&GenerationProof> {
        let &ordinal = self.index.find(Self::hash(*key), |&ordinal| {
            Self::key(&self.entries[ordinal as usize]) == *key
        })?;
        Some(&self.entries[ordinal as usize])
    }

    fn get_mut(&mut self, key: &(ChunkId, u32)) -> Option<&mut GenerationProof> {
        let &ordinal = self.index.find(Self::hash(*key), |&ordinal| {
            Self::key(&self.entries[ordinal as usize]) == *key
        })?;
        Some(&mut self.entries[ordinal as usize])
    }

    fn insert(
        &mut self,
        key: (ChunkId, u32),
        mut proof: GenerationProof,
    ) -> Option<GenerationProof> {
        assert_eq!(
            key,
            Self::key(&proof),
            "ASSERT: arena key matches proof identity"
        );
        if let Some(previous) = self.get_mut(&key) {
            if previous.admission == HistoricalProofAdmission::ExactReuse {
                proof.admission = previous.admission;
            }
            return Some(std::mem::replace(previous, proof));
        }
        assert!(
            self.len() < MAX_ONLINE_DEPENDENCY_PROOFS_V1,
            "ASSERT: proof arena is bounded"
        );
        let ordinal = u32::try_from(self.len()).expect("ASSERT: bounded proof ordinal fits u32");
        self.entries.push(proof);
        self.index
            .insert_unique(Self::hash(key), ordinal, |&ordinal| {
                Self::hash(Self::key(&self.entries[ordinal as usize]))
            });
        None
    }

    fn into_sorted_values(self) -> impl Iterator<Item = GenerationProof> {
        let Self { index, mut entries } = self;
        drop(index);
        // Historical admission order must remain identical to the old BTree.
        entries.sort_unstable_by_key(Self::key);
        entries.into_iter()
    }

    fn allocated_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<GenerationProof>()
            + self.index.allocation_size()
    }
}

/// Bounded, non-evictable proof ownership for the two live commit generations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GenerationProofSetStatus {
    active_proofs: usize,
    frozen_proofs: usize,
    accounted_bytes: usize,
}

impl GenerationProofSetStatus {
    #[must_use]
    pub const fn active_proofs(self) -> usize {
        self.active_proofs
    }

    #[must_use]
    pub const fn frozen_proofs(self) -> usize {
        self.frozen_proofs
    }

    #[must_use]
    pub const fn accounted_bytes(self) -> usize {
        self.accounted_bytes
    }
}

#[derive(Debug, Default)]
struct GenerationProofState {
    active: GenerationProofMap,
    frozen: Option<GenerationProofMap>,
    // One non-evictable admission per commit epoch, independent of proof-cache
    // capacity. It closes the selection -> Commit race with online GC.
    active_references: Option<Arc<fastdup_store::DataReferenceGuard>>,
    frozen_references: Option<Arc<fastdup_store::DataReferenceGuard>>,
    publishing: BTreeSet<(ChunkId, u32)>,
}

impl GenerationProofState {
    /// Keep admitted proofs pinned until commit completion. At capacity, a new
    /// dependency remains uncached. Online commits resolve it against the
    /// current Exact index before considering independent DATA verification.
    /// Resident Dirty DATA does not bound externalized Chunks.
    fn remember(&mut self, key: (ChunkId, u32), proof: GenerationProof, frozen: bool) -> bool {
        let count = self.active.len() + self.frozen.as_ref().map_or(0, GenerationProofMap::len);
        let target = if frozen {
            self.frozen
                .as_mut()
                .expect("ASSERT: commit-only proof requires one Frozen Generation")
        } else {
            &mut self.active
        };
        if count >= MAX_ONLINE_DEPENDENCY_PROOFS_V1 && target.get(&key).is_none() {
            return false;
        }
        target.insert(key, proof);
        true
    }
}

#[derive(Debug)]
struct OnlineDependencyProofs {
    generation: Mutex<GenerationProofState>,
    publication_completed: Condvar,
    historical: HistoricalProofCache,
    trace: ProofCacheTraceRecorder,
}

#[derive(Clone, Copy)]
enum OnlineProofAdmission {
    Published,
    ExactReuse,
    Touch,
}

impl OnlineDependencyProofs {
    fn reuse_location<C: StorageIo>(
        &self,
        index: &dyn ManifestReaderPolicy<C>,
        containers: &ContainerRepository<C>,
        chunk_id: ChunkId,
        logical_length: u64,
        frozen: bool,
    ) -> Option<ExactIndexEntry> {
        let references = {
            let state = self.generation.lock().expect("Generation Proof Set lock");
            if frozen {
                assert!(state.frozen.is_some(), "Frozen reuse owns a commit epoch");
                state.frozen_references.clone()
            } else {
                state.active_references.clone()
            }
        }
        .or_else(|| containers.try_pin_data_reference().map(Arc::new))?;
        let preferred = self.verified_entry(chunk_id, logical_length);
        let entry = index.exact_location(chunk_id, logical_length, preferred)?;
        self.remember_generation_with_references(
            entry,
            OnlineProofAdmission::ExactReuse,
            frozen,
            Some(references),
        );
        Some(entry)
    }

    fn new() -> Result<Self, DurableNamespaceError> {
        Ok(Self {
            generation: Mutex::new(GenerationProofState::default()),
            publication_completed: Condvar::new(),
            historical: HistoricalProofCache::new_system()
                .map_err(|_| DurableNamespaceError::OutOfMemory)?,
            trace: ProofCacheTraceRecorder::default(),
        })
    }

    fn remember_active(&self, entry: ExactIndexEntry, admission: OnlineProofAdmission) {
        self.remember_generation(entry, admission, false);
    }

    fn remember_frozen(&self, entry: ExactIndexEntry, admission: OnlineProofAdmission) {
        self.remember_generation(entry, admission, true);
    }

    fn remember_generation(
        &self,
        entry: ExactIndexEntry,
        admission: OnlineProofAdmission,
        frozen: bool,
    ) {
        self.remember_generation_with_references(entry, admission, frozen, None);
    }

    fn remember_generation_with_references(
        &self,
        entry: ExactIndexEntry,
        admission: OnlineProofAdmission,
        frozen: bool,
        references: Option<Arc<fastdup_store::DataReferenceGuard>>,
    ) {
        assert_eq!(
            entry.transition(),
            fastdup_format::ExactLocationTransition::Active,
            "ASSERT: only an ACTIVE Location can prove an online dependency"
        );
        let key = (entry.chunk_id(), entry.logical_length());
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        if let Some(references) = references {
            let target = if frozen {
                &mut state.frozen_references
            } else {
                &mut state.active_references
            };
            target.get_or_insert(references);
        }
        let historical_admission = match admission {
            OnlineProofAdmission::Published => HistoricalProofAdmission::Published,
            OnlineProofAdmission::ExactReuse | OnlineProofAdmission::Touch => {
                HistoricalProofAdmission::ExactReuse
            }
        };
        if !state.remember(
            key,
            GenerationProof {
                entry,
                admission: historical_admission,
            },
            frozen,
        ) {
            return;
        }
        drop(state);
        let key = ProofKey::new(entry.chunk_id(), entry.logical_length());
        let verify_bytes = entry.location().record_length();
        match admission {
            OnlineProofAdmission::Published => self
                .trace
                .record(ProofCacheEvent::admit_published(key, verify_bytes)),
            OnlineProofAdmission::ExactReuse | OnlineProofAdmission::Touch => self
                .trace
                .record(ProofCacheEvent::admit_exact_reuse(key, verify_bytes)),
        }
    }

    fn freeze_for_commit(&self) -> bool {
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        if state.frozen.is_some() {
            return false;
        }
        let frozen = std::mem::take(&mut state.active);
        state.frozen = Some(frozen);
        state.frozen_references = state.active_references.take();
        true
    }

    fn cancel_new_freeze(&self, newly_frozen: bool) {
        if !newly_frozen {
            return;
        }
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        let frozen = state
            .frozen
            .take()
            .expect("ASSERT: a new proof freeze must still own Frozen state");
        let references = state.frozen_references.take();
        if state.active_references.is_none() {
            state.active_references = references;
        }
        for proof in frozen.into_sorted_values() {
            let key = GenerationProofMap::key(&proof);
            if state.active.get(&key).is_none() {
                state.active.insert(key, proof);
            }
        }
        assert!(
            state.active.len() <= MAX_ONLINE_DEPENDENCY_PROOFS_V1,
            "ASSERT: canceled proof freeze exceeded the combined Generation Proof budget"
        );
    }

    fn complete_frozen(&self) {
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        let frozen = state
            .frozen
            .take()
            .expect("ASSERT: a successful commit owns one Frozen Generation Proof Set");
        state.frozen_references = None;
        drop(state);
        for proof in frozen.into_sorted_values() {
            self.historical.admit(proof.entry, proof.admission);
        }
    }

    fn generation_entry(&self, key: (ChunkId, u32)) -> Option<ExactIndexEntry> {
        let state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        state
            .active
            .get(&key)
            .or_else(|| state.frozen.as_ref().and_then(|frozen| frozen.get(&key)))
            .map(|proof| proof.entry)
    }

    fn unproven(&self, required: &BTreeMap<ChunkId, u64>) -> BTreeMap<ChunkId, u64> {
        let mut unproven = BTreeMap::new();
        let mut history_hits = Vec::new();
        for (chunk_id, logical_length) in required {
            let Ok(index_length) = u32::try_from(*logical_length) else {
                unproven.insert(*chunk_id, *logical_length);
                continue;
            };
            let key = (*chunk_id, index_length);
            if let Some(entry) = self.generation_entry(key) {
                assert_entry_matches(entry, *chunk_id, index_length);
                continue;
            }
            if let Some(entry) = self.historical.get(*chunk_id, *logical_length) {
                assert_entry_matches(entry, *chunk_id, index_length);
                history_hits.push(entry);
            } else {
                unproven.insert(*chunk_id, *logical_length);
            }
        }
        for entry in history_hits {
            self.remember_frozen(entry, OnlineProofAdmission::Touch);
        }
        for (chunk_id, logical_length) in required {
            if let Ok(logical_length) = u32::try_from(*logical_length) {
                self.trace.record(ProofCacheEvent::lookup(ProofKey::new(
                    *chunk_id,
                    logical_length,
                )));
            }
        }
        unproven
    }

    fn verified_entry(&self, chunk_id: ChunkId, logical_length: u64) -> Option<ExactIndexEntry> {
        let logical_length = u32::try_from(logical_length).ok()?;
        let entry = self
            .generation_entry((chunk_id, logical_length))
            .or_else(|| self.historical.get(chunk_id, u64::from(logical_length)));
        self.trace.record(ProofCacheEvent::lookup(ProofKey::new(
            chunk_id,
            logical_length,
        )));
        entry
    }

    /// Claims one missing Chunk for publication or waits for the current
    /// in-process publisher to install its proof.
    ///
    /// Callers claim keys in ascending `(ChunkId, logical_length)` order. That
    /// ordering prevents two partially overlapping Container batches from
    /// waiting on each other while retaining disjoint claims.
    fn claim_publication(&self, chunk_id: ChunkId, logical_length: u32) -> PublicationClaim {
        let key = (chunk_id, logical_length);
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        loop {
            if let Some(proof) = state.active.get(&key) {
                assert_entry_matches(proof.entry, chunk_id, logical_length);
                return PublicationClaim::Existing(proof.entry);
            }
            if let Some(entry) = state
                .frozen
                .as_ref()
                .and_then(|frozen| frozen.get(&key))
                .map(|proof| proof.entry)
            {
                assert_entry_matches(entry, chunk_id, logical_length);
                drop(state);
                self.remember_active(entry, OnlineProofAdmission::Touch);
                return PublicationClaim::Existing(entry);
            }
            if state.publishing.insert(key) {
                return PublicationClaim::Acquired;
            }
            state = self
                .publication_completed
                .wait(state)
                .expect("ASSERT: Generation Proof Set lock poisoned while awaiting publication");
        }
    }

    fn finish_publications(&self, entries: &[ExactIndexEntry], claimed: &[(ChunkId, u32)]) {
        assert_eq!(
            entries.len(),
            claimed.len(),
            "ASSERT: every publication claim must produce one verified Location"
        );
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        for (entry, key) in entries.iter().zip(claimed) {
            assert_eq!(
                entry.transition(),
                fastdup_format::ExactLocationTransition::Active,
                "ASSERT: only an ACTIVE Location can prove an online dependency"
            );
            assert_entry_matches(*entry, key.0, key.1);
            assert!(
                state.publishing.contains(key),
                "ASSERT: completed publication must own its Chunk claim"
            );
            assert!(
                state.active.get(key).is_none(),
                "ASSERT: an owned publication claim cannot already have an active proof"
            );
            state.remember(
                *key,
                GenerationProof {
                    entry: *entry,
                    admission: HistoricalProofAdmission::Published,
                },
                false,
            );
        }
        for key in claimed {
            assert!(
                state.publishing.remove(key),
                "ASSERT: completed publication must release its Chunk claim"
            );
        }
        let frozen_proofs = state.frozen.as_ref().map_or(0, GenerationProofMap::len);
        assert!(
            state
                .active
                .len()
                .checked_add(frozen_proofs)
                .is_some_and(|total| total <= MAX_ONLINE_DEPENDENCY_PROOFS_V1),
            "ASSERT: completed publication exceeded the combined Generation Proof budget"
        );
        drop(state);
        self.publication_completed.notify_all();
        for entry in entries {
            self.trace.record(ProofCacheEvent::admit_published(
                ProofKey::new(entry.chunk_id(), entry.logical_length()),
                entry.location().record_length(),
            ));
        }
    }

    fn abandon_publications(&self, claimed: &[(ChunkId, u32)]) {
        let mut state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        for key in claimed {
            assert!(
                state.publishing.remove(key),
                "ASSERT: failed publication must own its Chunk claim"
            );
        }
        drop(state);
        self.publication_completed.notify_all();
    }

    fn historical_status(&self) -> HistoricalProofCacheStatus {
        self.historical.status()
    }

    fn generation_status(&self) -> GenerationProofSetStatus {
        let state = self
            .generation
            .lock()
            .expect("ASSERT: Generation Proof Set lock poisoned");
        let active_proofs = state.active.len();
        let frozen_proofs = state.frozen.as_ref().map_or(0, GenerationProofMap::len);
        let accounted_bytes = state
            .active
            .allocated_bytes()
            .checked_add(
                state
                    .frozen
                    .as_ref()
                    .map_or(0, GenerationProofMap::allocated_bytes),
            )
            .expect("ASSERT: bounded Generation Proof allocation accounting cannot overflow");
        GenerationProofSetStatus {
            active_proofs,
            frozen_proofs,
            accounted_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum PublicationClaim {
    Existing(ExactIndexEntry),
    Acquired,
}

struct PublicationClaims<'a> {
    proofs: &'a OnlineDependencyProofs,
    keys: Vec<(ChunkId, u32)>,
    finished: bool,
}

impl<'a> PublicationClaims<'a> {
    fn new(
        proofs: &'a OnlineDependencyProofs,
        capacity: usize,
    ) -> Result<Self, DurableNamespaceError> {
        let mut keys = Vec::new();
        keys.try_reserve(capacity)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        Ok(Self {
            proofs,
            keys,
            finished: false,
        })
    }

    fn claim(&mut self, chunk_id: ChunkId, logical_length: u32) -> PublicationClaim {
        let claim = self.proofs.claim_publication(chunk_id, logical_length);
        if matches!(claim, PublicationClaim::Acquired) {
            self.keys.push((chunk_id, logical_length));
        }
        claim
    }

    fn finish(mut self, entries: &mut [ExactIndexEntry]) {
        // Reduction planning groups ordinary, independent, and dependent
        // records. The resulting writer Locations therefore follow encoding
        // group order, while claims follow Chunk-ID order. Restore the claim
        // order before pairing each verified Location with its key.
        entries.sort_unstable_by_key(|entry| (entry.chunk_id(), entry.logical_length()));
        self.proofs.finish_publications(entries, &self.keys);
        self.finished = true;
    }
}

impl Drop for PublicationClaims<'_> {
    fn drop(&mut self) {
        if !self.finished && !self.keys.is_empty() {
            self.proofs.abandon_publications(&self.keys);
        }
    }
}

fn assert_entry_matches(entry: ExactIndexEntry, chunk_id: ChunkId, logical_length: u32) {
    assert_eq!(
        entry.chunk_id(),
        chunk_id,
        "ASSERT: Generation Proof key matches its verified Location"
    );
    assert_eq!(
        entry.logical_length(),
        logical_length,
        "ASSERT: Generation Proof length matches its verified Location"
    );
}

struct OnlineSuccessorVerifier {
    proofs: Arc<OnlineDependencyProofs>,
    fallback: Box<dyn RequiredChunkVerifier>,
}

/// Commit-only reference checking when the bounded per-Chunk proof set is full.
/// This is deliberately separate from independent graph/payload verification.
struct OnlineExactReferenceVerifier<C, X> {
    indexes: ExactIndexRunRepository<X>,
    containers: ContainerRepository<C>,
    read_cache: Arc<VerifiedReadCache>,
}

impl<C: Clone + StorageIo, X: Clone + StorageIo> RequiredChunkVerifier
    for OnlineExactReferenceVerifier<C, X>
{
    fn verify_required_chunks(&self, required: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
        // Called inside GenerationRepository's Commit lock, also held by GC
        // retirement activation. Select now, not when planning the Commit:
        // a previously planned snapshot could still contain retiring victims.
        let Some(index) = self.indexes.pin_active_generation() else {
            return self.containers.verify_required_chunks(required);
        };
        let mut missing = BTreeMap::new();
        for (id, length) in required {
            let reference = u32::try_from(*length)
                .ok()
                .and_then(|length| index.active_reference(*id, length, None).ok().flatten());
            if reference.is_none() {
                missing.insert(*id, *length);
            }
        }
        IndexedRequiredChunkVerifier::new(self.containers.clone(), index)
            .with_verified_read_cache(Arc::clone(&self.read_cache))
            .verify_required_chunks(&missing)
    }
}

impl RequiredChunkVerifier for OnlineSuccessorVerifier {
    fn verify_required_chunks(&self, required: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
        self.fallback
            .verify_required_chunks(&self.proofs.unproven(required))
    }
}

#[derive(Clone, Copy, Debug)]
struct InstalledManifest {
    inode: u64,
    root: MetadataObjectId,
    logical_size: u64,
    allocated_bytes: u64,
    summary: ManifestTreeSummary,
}

struct VerifiedLocationFile {
    source: Arc<dyn CommittedFile>,
    source_offset: u64,
    entry: ExactIndexEntry,
}

impl fmt::Debug for VerifiedLocationFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedLocationFile")
            .field("chunk_id", &self.entry.chunk_id())
            .field("logical_length", &self.entry.logical_length())
            .field("location", &self.entry.location())
            .finish_non_exhaustive()
    }
}

impl CommittedFile for VerifiedLocationFile {
    fn logical_size(&self) -> u64 {
        u64::from(self.entry.logical_length())
    }

    fn allocated_bytes(&self) -> u64 {
        self.logical_size()
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        let logical_size = self.logical_size();
        let start = offset.min(logical_size);
        let end = offset.saturating_add(length).min(logical_size);
        Ok(end - start)
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.read_shared_at(offset, length).map(Vec::from)
    }

    fn read_shared_at(&self, offset: u64, length: u32) -> Result<bytes::Bytes, PosixError> {
        let start = offset.min(self.logical_size());
        let length = u32::try_from(u64::from(length).min(self.logical_size() - start))
            .map_err(|_| PosixError::Io)?;
        self.source
            .read_shared_at(self.source_offset + start, length)
    }

    fn shared_read_source(&self) -> Option<(Arc<dyn CommittedFile>, u64)> {
        Some((Arc::clone(&self.source), self.source_offset))
    }

    fn matches_complete_bytes(&self, candidate: &[u8]) -> Result<bool, PosixError> {
        Ok(candidate.len()
            == usize::try_from(self.entry.logical_length())
                .expect("ASSERT: Exact Index length fits usize")
            && ChunkId::of(candidate) == self.entry.chunk_id())
    }

    fn matches_complete_segments(&self, segments: &[&[u8]]) -> Result<bool, PosixError> {
        let mut length = 0_usize;
        let mut hasher = blake3::Hasher::new();
        for segment in segments {
            length = length
                .checked_add(segment.len())
                .ok_or(PosixError::FileTooLarge)?;
            hasher.update(segment);
        }
        Ok(length
            == usize::try_from(self.entry.logical_length())
                .expect("ASSERT: Exact Index length fits usize")
            && ChunkId::from_bytes(*hasher.finalize().as_bytes()) == self.entry.chunk_id())
    }

    fn prepared_data_recipe(&self) -> Option<PreparedDataRecipe> {
        Some(PreparedDataRecipe::Chunk {
            chunk_id: self.entry.chunk_id().bytes(),
        })
    }
}

#[derive(Debug)]
struct FillCommittedFile {
    value: u8,
    length: u64,
}

impl CommittedFile for FillCommittedFile {
    fn logical_size(&self) -> u64 {
        self.length
    }

    fn allocated_bytes(&self) -> u64 {
        self.length
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        let start = offset.min(self.length);
        let end = offset.saturating_add(length).min(self.length);
        Ok(end - start)
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        let start = offset.min(self.length);
        let end = offset.saturating_add(u64::from(length)).min(self.length);
        let output_length = usize::try_from(end - start).map_err(|_| PosixError::FileTooLarge)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(output_length)
            .map_err(|_| PosixError::OutOfMemory)?;
        output.resize(output_length, self.value);
        Ok(output)
    }

    fn matches_complete_bytes(&self, candidate: &[u8]) -> Result<bool, PosixError> {
        Ok(
            u64::try_from(candidate.len()).expect("ASSERT: usize fits u64") == self.length
                && candidate.iter().all(|byte| *byte == self.value),
        )
    }

    fn matches_complete_segments(&self, segments: &[&[u8]]) -> Result<bool, PosixError> {
        let mut length = 0_u64;
        for segment in segments {
            length = length
                .checked_add(u64::try_from(segment.len()).expect("ASSERT: usize fits u64"))
                .ok_or(PosixError::FileTooLarge)?;
            if !segment.iter().all(|byte| *byte == self.value) {
                return Ok(false);
            }
        }
        Ok(length == self.length)
    }

    fn prepared_data_recipe(&self) -> Option<PreparedDataRecipe> {
        Some(PreparedDataRecipe::Fill { value: self.value })
    }
}

trait ManifestReaderPolicy<C>: fmt::Debug + Send + Sync {
    fn advanced_reduction_available(&self) -> bool {
        false
    }
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C>;
    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier>;
    fn online_graph_verifier(
        &self,
        containers: ContainerRepository<C>,
    ) -> Box<dyn RequiredChunkVerifier> {
        self.graph_verifier(containers)
    }
    fn exact_index_run_count(&self) -> usize;
    /// Reference selection only; caller holds DATA-reference admission until Commit.
    fn exact_location(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
        preferred: Option<ExactIndexEntry>,
    ) -> Option<ExactIndexEntry>;
    fn plan_similarity_chunk(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
    ) -> (
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    );
    fn plan_similarity_batch(
        &self,
        containers: &ContainerRepository<C>,
        targets: &[PrehashedChunk<'_>],
        _workers: NonZeroUsize,
        _admission: &WorkerPermits,
    ) -> Vec<(
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    )> {
        targets
            .iter()
            .map(|target| self.plan_similarity_chunk(containers, target.chunk_id(), target.bytes()))
            .collect()
    }
    fn publish_level_zero(&self, entries: Vec<ExactIndexEntry>);
    fn publish_reduction_batch(
        &self,
        entries: Vec<ExactIndexEntry>,
        _similarities: Vec<fastdup_format::SimilarityIndexEntry>,
        _guard: Option<fastdup_store::DataReferenceGuard>,
    ) {
        self.publish_level_zero(entries);
    }
    fn flush_level_zero(&self);

    fn publication_timings(&self) -> Vec<fastdup_store::OperationTimingSnapshot> {
        Vec::new()
    }
    fn exact_index_degraded(&self) -> bool;
    fn exact_index_page_cache_status(&self) -> ExactIndexPageCacheStatus;
    fn exact_run_membership_status(&self) -> ExactRunMembershipStatus;
    fn read_cache(&self) -> &Arc<VerifiedReadCache>;
    fn read_cache_status(&self) -> VerifiedReadCacheStatus {
        self.read_cache().status()
    }
    fn advanced_reduction_status(&self) -> PersistentReductionStatus;
    fn similarity_page_cache_status(&self) -> SimilarityIndexPageCacheStatus;
}

#[derive(Debug)]
struct ScanManifestReaders {
    read_cache: Arc<VerifiedReadCache>,
}

impl<C: StorageIo + 'static> ManifestReaderPolicy<C> for ScanManifestReaders {
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C> {
        file.with_verified_read_cache(Arc::clone(&self.read_cache))
    }

    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier> {
        Box::new(containers)
    }

    fn exact_index_run_count(&self) -> usize {
        0
    }

    fn exact_location(
        &self,
        _chunk_id: ChunkId,
        _logical_length: u64,
        preferred: Option<ExactIndexEntry>,
    ) -> Option<ExactIndexEntry> {
        preferred
    }

    fn plan_similarity_chunk(
        &self,
        _containers: &ContainerRepository<C>,
        _target_id: ChunkId,
        _target: &[u8],
    ) -> (
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    ) {
        (PersistentChunkPlan::NoCandidates, None)
    }

    fn publish_level_zero(&self, _entries: Vec<ExactIndexEntry>) {}

    fn flush_level_zero(&self) {}

    fn exact_index_degraded(&self) -> bool {
        false
    }

    fn exact_index_page_cache_status(&self) -> ExactIndexPageCacheStatus {
        ExactIndexPageCacheStatus::default()
    }

    fn exact_run_membership_status(&self) -> ExactRunMembershipStatus {
        ExactRunMembershipStatus::default()
    }

    fn read_cache(&self) -> &Arc<VerifiedReadCache> {
        &self.read_cache
    }

    fn advanced_reduction_status(&self) -> PersistentReductionStatus {
        PersistentReductionStatus::default()
    }

    fn similarity_page_cache_status(&self) -> SimilarityIndexPageCacheStatus {
        SimilarityIndexPageCacheStatus::default()
    }
}

struct IndexedManifestReaders<X> {
    core: Arc<ExactPublisherCore<X>>,
    publisher: ExactPublicationQueue,
    read_cache: Arc<VerifiedReadCache>,
    reduction: Option<Arc<PersistentReductionIndex<X>>>,
}

struct ExactPublisherCore<X> {
    repository: ExactIndexRunRepository<X>,
    profile: ExactIndexProfileId,
    degraded: AtomicBool,
    recent: RwLock<BTreeMap<(ChunkId, u32), ExactIndexEntry>>,
    similarity: Option<SimilarityPublicationQueue<X>>,
    failed_reduction_guard: Mutex<Option<fastdup_store::DataReferenceGuard>>,
}

enum ExactPublicationCommand {
    Publish(
        Vec<ExactIndexEntry>,
        Vec<fastdup_format::SimilarityIndexEntry>,
        Option<fastdup_store::DataReferenceGuard>,
    ),
    Flush(mpsc::SyncSender<()>),
    Shutdown,
}

#[derive(Clone, Debug, Default)]
struct ExactQueueTimings {
    enqueue: fastdup_store::OperationTiming,
    queue_wait: fastdup_store::OperationTiming,
    publish: fastdup_store::OperationTiming,
    batch: fastdup_store::OperationTiming,
    flush: fastdup_store::OperationTiming,
}

struct ExactPublicationQueue {
    sender: mpsc::SyncSender<(
        ExactPublicationCommand,
        Option<fastdup_store::OperationTimer>,
    )>,
    timings: ExactQueueTimings,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

type SimilarityBatch = Vec<fastdup_format::SimilarityIndexEntry>;

struct SimilarityPublicationQueue<X> {
    sender: mpsc::SyncSender<Option<SimilarityBatch>>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    repository: Arc<fastdup_store::OnlineSimilarityRepository<X>>,
}

impl<X: Clone + Send + Sync + StorageIo + 'static> SimilarityPublicationQueue<X> {
    fn start(repository: Arc<fastdup_store::OnlineSimilarityRepository<X>>) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Option<SimilarityBatch>>(2);
        let worker_repository = Arc::clone(&repository);
        let worker = std::thread::Builder::new()
            .name("fastdup-similarity-publisher".to_owned())
            .spawn(move || {
                while let Ok(Some(entries)) = receiver.recv() {
                    if let Err(error) = worker_repository.append_current(&entries) {
                        eprintln!("online Similarity publication degraded: {error}");
                    }
                }
            })?;
        Ok(Self {
            sender,
            worker: Mutex::new(Some(worker)),
            repository,
        })
    }

    fn publish(&self, mut entries: Vec<fastdup_format::SimilarityIndexEntry>) {
        if entries.is_empty() {
            return;
        }
        let limit = fastdup_store::ONLINE_SIMILARITY_BATCH_ENTRIES;
        if entries.len() > limit {
            self.repository.skip(entries.len() - limit);
            entries.truncate(limit);
        }
        let count = entries.len();
        if self.sender.try_send(Some(entries)).is_err() {
            self.repository.skip(count);
        }
    }
}

impl<X> Drop for SimilarityPublicationQueue<X> {
    fn drop(&mut self) {
        let _ = self.sender.send(None);
        if let Some(worker) = self.worker.lock().expect("Similarity worker lock").take() {
            let _ = worker.join();
        }
    }
}

impl<X: Clone + StorageIo> fmt::Debug for IndexedManifestReaders<X> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexedManifestReaders")
            .field("run_count", &self.run_count())
            .field("degraded", &self.core.degraded.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<X: Clone + StorageIo> IndexedManifestReaders<X> {
    fn run_count(&self) -> usize {
        self.core
            .repository
            .pin_active_generation()
            .as_deref()
            .map_or(0, fastdup_store::ActivatedExactIndex::run_count)
    }
}

/// One bounded collection of ACTIVE-addition publication commands waiting for
/// a single combined L0 append and activation. Guards pin every included DATA
/// reference until the combined activation, overlay retirement, and similarity
/// handoff complete.
struct CoalescedAdditions {
    deadline: Instant,
    entries: Vec<ExactIndexEntry>,
    similarity_batches: Vec<Vec<fastdup_format::SimilarityIndexEntry>>,
    guards: Vec<fastdup_store::DataReferenceGuard>,
    publish_timers: Vec<fastdup_store::OperationTimer>,
    commands: usize,
}

impl ExactPublicationQueue {
    fn start<X>(core: Arc<ExactPublisherCore<X>>) -> io::Result<Self>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(EXACT_PUBLICATION_QUEUE_BATCHES);
        let timings = ExactQueueTimings::default();
        let worker_timings = timings.clone();
        let worker = std::thread::Builder::new()
            .name("fastdup-exact-publisher".to_owned())
            .spawn(move || Self::run(&core, &receiver, &worker_timings))?;
        Ok(Self {
            sender,
            timings,
            worker: Mutex::new(Some(worker)),
        })
    }

    fn publish_collected<X>(
        core: &ExactPublisherCore<X>,
        timings: &ExactQueueTimings,
        batch: &mut CoalescedAdditions,
    ) where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        let _batch = timings.batch.begin();
        if batch.commands > 1 {
            // Identical repeated additions are idempotent. Distinct
            // Locations survive; conflicting physical identities
            // still fail the Run writer's canonical validation.
            batch.entries.sort_unstable_by_key(|entry| {
                let location = entry.location();
                (
                    entry.chunk_id(),
                    entry.logical_length(),
                    location.container_id().bytes(),
                    location.record_offset(),
                    location.chunk_ordinal(),
                )
            });
            batch.entries.dedup();
        }
        let entries = std::mem::take(&mut batch.entries);
        let result = core
            .repository
            .append_level_zero(core.profile, entries.clone());
        if result.is_err() {
            core.degraded.store(true, Ordering::Release);
            if let Some(guard) = batch.guards.pop() {
                core.failed_reduction_guard
                    .lock()
                    .expect("failed reduction guard lock")
                    .get_or_insert(guard);
            }
        } else {
            if let Some(queue) = &core.similarity {
                for similarities in batch.similarity_batches.drain(..) {
                    queue.publish(similarities);
                }
            }
            core.degraded.store(false, Ordering::Release);
        }
        core.forget_recent(&entries);
        // All DATA admissions survive activation, similarity handoff
        // and overlay retirement, including error handling above.
        drop(std::mem::take(&mut batch.guards));
        drop(std::mem::take(&mut batch.publish_timers));
    }

    fn flush_collected<X>(
        core: &ExactPublisherCore<X>,
        timings: &ExactQueueTimings,
        buffer: &mut Option<CoalescedAdditions>,
    ) where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        if let Some(batch) = buffer.as_mut() {
            Self::publish_collected(core, timings, batch);
            *buffer = None;
        }
    }

    #[allow(clippy::too_many_lines)]
    fn run<X>(
        core: &ExactPublisherCore<X>,
        receiver: &mpsc::Receiver<(
            ExactPublicationCommand,
            Option<fastdup_store::OperationTimer>,
        )>,
        timings: &ExactQueueTimings,
    ) where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        let mut pending = None;
        let mut buffer: Option<CoalescedAdditions> = None;
        loop {
            let (command, queued) = if let Some(pair) = pending.take() {
                pair
            } else {
                let incoming = if let Some(batch) = &buffer {
                    match receiver
                        .recv_timeout(batch.deadline.saturating_duration_since(Instant::now()))
                    {
                        Ok(item) => Some(item),
                        Err(mpsc::RecvTimeoutError::Disconnected) => None,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            Self::flush_collected(core, timings, &mut buffer);
                            continue;
                        }
                    }
                } else {
                    receiver.recv().ok()
                };
                if let Some(pair) = incoming {
                    pair
                } else {
                    Self::flush_collected(core, timings, &mut buffer);
                    break;
                }
            };
            drop(queued);
            match command {
                ExactPublicationCommand::Publish(entries, similarities, guard)
                    if entries.iter().all(|entry| {
                        entry.transition() == fastdup_format::ExactLocationTransition::Active
                    }) =>
                {
                    // Only ordinary ACTIVE additions may share an activation.
                    let exceeds = buffer.as_ref().is_some_and(|batch| {
                        batch
                            .entries
                            .len()
                            .checked_add(entries.len())
                            .is_none_or(|total| total > EXACT_PUBLICATION_BATCH_ENTRIES)
                            || batch.commands >= EXACT_PUBLICATION_QUEUE_BATCHES
                    });
                    if exceeds {
                        Self::flush_collected(core, timings, &mut buffer);
                    }
                    if let Some(batch) = &mut buffer {
                        batch.publish_timers.push(timings.publish.begin());
                        batch.entries.extend(entries);
                        batch.similarity_batches.push(similarities);
                        batch.guards.extend(guard);
                        batch.commands += 1;
                    } else {
                        let mut batch = CoalescedAdditions {
                            deadline: Instant::now() + EXACT_PUBLICATION_COALESCE_WINDOW,
                            entries,
                            similarity_batches: vec![similarities],
                            guards: guard.into_iter().collect(),
                            publish_timers: vec![timings.publish.begin()],
                            commands: 1,
                        };
                        // An individually bound-reaching command runs alone.
                        if batch.entries.len() >= EXACT_PUBLICATION_BATCH_ENTRIES {
                            Self::publish_collected(core, timings, &mut batch);
                        } else {
                            buffer = Some(batch);
                        }
                    }
                }
                ExactPublicationCommand::Publish(entries, similarities, guard) => {
                    // A transition command retains its own precedence boundary:
                    // everything collected earlier activates first.
                    Self::flush_collected(core, timings, &mut buffer);
                    let mut alone = CoalescedAdditions {
                        deadline: Instant::now(),
                        entries,
                        similarity_batches: vec![similarities],
                        guards: guard.into_iter().collect(),
                        publish_timers: vec![timings.publish.begin()],
                        commands: 1,
                    };
                    Self::publish_collected(core, timings, &mut alone);
                }
                ExactPublicationCommand::Flush(reply) => {
                    // The commit fence forces the collected window out before
                    // it acknowledges; no committed generation ever activates
                    // behind an unprocessed collection.
                    Self::flush_collected(core, timings, &mut buffer);
                    let _ = reply.send(());
                }
                ExactPublicationCommand::Shutdown => {
                    Self::flush_collected(core, timings, &mut buffer);
                    break;
                }
            }
        }
    }

    fn send(&self, command: ExactPublicationCommand) {
        let _enqueue = self.timings.enqueue.begin();
        let waiting = self.timings.queue_wait.begin();
        self.sender
            .send((command, Some(waiting)))
            .expect("ASSERT: permanent Exact publisher remains alive while mounted");
    }

    fn snapshots(&self) -> Vec<fastdup_store::OperationTimingSnapshot> {
        [
            ("exactEnqueue", &self.timings.enqueue),
            ("exactQueueWait", &self.timings.queue_wait),
            ("exactPublish", &self.timings.publish),
            ("exactPublishBatch", &self.timings.batch),
            ("exactFlush", &self.timings.flush),
        ]
        .into_iter()
        .map(|(id, timing)| timing.snapshot(id))
        .collect()
    }

    fn publish(
        &self,
        entries: Vec<ExactIndexEntry>,
        similarities: Vec<fastdup_format::SimilarityIndexEntry>,
        guard: Option<fastdup_store::DataReferenceGuard>,
    ) {
        self.send(ExactPublicationCommand::Publish(
            entries,
            similarities,
            guard,
        ));
    }

    fn flush(&self) {
        let _flush = self.timings.flush.begin();
        let (reply, receive) = mpsc::sync_channel(1);
        self.send(ExactPublicationCommand::Flush(reply));
        receive
            .recv()
            .expect("ASSERT: permanent Exact publisher acknowledges every fence");
    }
}

impl Drop for ExactPublicationQueue {
    fn drop(&mut self) {
        let _ = self.sender.send((ExactPublicationCommand::Shutdown, None));
        if let Some(worker) = self
            .worker
            .get_mut()
            .expect("ASSERT: Exact publisher handle lock poisoned during shutdown")
            .take()
        {
            worker
                .join()
                .expect("ASSERT: permanent Exact publisher must not panic");
        }
    }
}

impl<X> ExactPublisherCore<X>
where
    X: Clone + Send + Sync + StorageIo + 'static,
{
    fn remember_recent(&self, entries: &[ExactIndexEntry]) {
        let mut recent = self
            .recent
            .write()
            .expect("ASSERT: recent Exact Location lock poisoned");
        for entry in entries {
            recent.insert((entry.chunk_id(), entry.logical_length()), *entry);
        }
        while recent.len() > MAX_RECENT_EXACT_LOCATIONS {
            recent
                .pop_first()
                .expect("ASSERT: an oversized recent Exact map is nonempty");
        }
        assert!(
            recent.len() <= MAX_RECENT_EXACT_LOCATIONS,
            "ASSERT: recent Exact Location overlay exceeds its entry bound"
        );
    }

    fn recent_location(&self, chunk_id: ChunkId, logical_length: u64) -> Option<ExactIndexEntry> {
        let length = u32::try_from(logical_length).ok()?;
        self.recent
            .read()
            .expect("ASSERT: recent Exact Location lock poisoned")
            .get(&(chunk_id, length))
            .copied()
    }

    fn forget_recent(&self, entries: &[ExactIndexEntry]) {
        let mut recent = self
            .recent
            .write()
            .expect("ASSERT: recent Exact Location lock poisoned");
        for entry in entries {
            let key = (entry.chunk_id(), entry.logical_length());
            if let std::collections::btree_map::Entry::Occupied(slot) = recent.entry(key)
                && slot.get() == entry
            {
                slot.remove();
            }
        }
    }
}

impl<C, X> ManifestReaderPolicy<C> for IndexedManifestReaders<X>
where
    C: Clone + Send + Sync + StorageIo + 'static,
    X: Clone + Send + Sync + StorageIo + 'static,
{
    fn advanced_reduction_available(&self) -> bool {
        self.reduction.is_some()
            && self
                .core
                .failed_reduction_guard
                .lock()
                .expect("failed reduction guard lock")
                .is_none()
    }
    fn prepare(&self, file: VerifiedManifestFile<C>) -> VerifiedManifestFile<C> {
        let file = file.with_index_repository(&self.core.repository);
        file.with_verified_read_cache(Arc::clone(&self.read_cache))
    }

    fn graph_verifier(&self, containers: ContainerRepository<C>) -> Box<dyn RequiredChunkVerifier> {
        let active = self.core.repository.pin_active_generation();
        match active {
            Some(index) => Box::new(
                IndexedRequiredChunkVerifier::new(containers, index)
                    .with_verified_read_cache(Arc::clone(&self.read_cache)),
            ),
            None => Box::new(containers),
        }
    }

    fn online_graph_verifier(
        &self,
        containers: ContainerRepository<C>,
    ) -> Box<dyn RequiredChunkVerifier> {
        Box::new(OnlineExactReferenceVerifier {
            indexes: self.core.repository.clone(),
            containers,
            read_cache: Arc::clone(&self.read_cache),
        })
    }

    fn exact_index_run_count(&self) -> usize {
        self.run_count()
    }

    fn exact_location(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
        preferred: Option<ExactIndexEntry>,
    ) -> Option<ExactIndexEntry> {
        let active = self.core.repository.pin_active_generation();
        if let Some(recent) = self.core.recent_location(chunk_id, logical_length)
            && active
                .as_deref()
                .is_none_or(|index| index.permits_active_overlay(recent).unwrap_or(false))
        {
            return Some(recent);
        }
        let active = active?;
        if let Ok(location) =
            active.active_reference(chunk_id, u32::try_from(logical_length).ok()?, preferred)
        {
            location
        } else {
            self.core.degraded.store(true, Ordering::Release);
            None
        }
    }

    fn plan_similarity_chunk(
        &self,
        containers: &ContainerRepository<C>,
        target_id: ChunkId,
        target: &[u8],
    ) -> (
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    ) {
        self.reduction
            .as_ref()
            .and_then(|reduction| {
                reduction
                    .plan_chunk_for_publication_cached(
                        containers,
                        target_id,
                        target,
                        Some(&self.read_cache),
                    )
                    .ok()
            })
            .unwrap_or((PersistentChunkPlan::NoCandidates, None))
    }

    fn plan_similarity_batch(
        &self,
        containers: &ContainerRepository<C>,
        targets: &[PrehashedChunk<'_>],
        workers: NonZeroUsize,
        admission: &WorkerPermits,
    ) -> Vec<(
        PersistentChunkPlan,
        Option<fastdup_format::SimilarityIndexEntry>,
    )> {
        self.reduction.as_ref().map_or_else(
            || {
                targets
                    .iter()
                    .map(|_| (PersistentChunkPlan::NoCandidates, None))
                    .collect()
            },
            |reduction| {
                reduction.plan_batch_for_publication_cached(
                    containers,
                    targets,
                    Some(&self.read_cache),
                    workers,
                    admission,
                )
            },
        )
    }

    fn publish_level_zero(&self, entries: Vec<ExactIndexEntry>) {
        if entries.is_empty() {
            return;
        }
        self.core.remember_recent(&entries);
        self.publisher.publish(entries, Vec::new(), None);
    }

    fn publish_reduction_batch(
        &self,
        entries: Vec<ExactIndexEntry>,
        similarities: Vec<fastdup_format::SimilarityIndexEntry>,
        guard: Option<fastdup_store::DataReferenceGuard>,
    ) {
        if entries.is_empty() {
            return;
        }
        self.core.remember_recent(&entries);
        self.publisher.publish(entries, similarities, guard);
    }

    fn flush_level_zero(&self) {
        self.publisher.flush();
    }

    fn publication_timings(&self) -> Vec<fastdup_store::OperationTimingSnapshot> {
        let mut result = self.publisher.snapshots();
        result.extend(self.core.repository.publication_timings());
        result
    }

    fn exact_index_degraded(&self) -> bool {
        self.core.degraded.load(Ordering::Acquire)
    }

    fn exact_index_page_cache_status(&self) -> ExactIndexPageCacheStatus {
        self.core.repository.page_cache_status()
    }

    fn exact_run_membership_status(&self) -> ExactRunMembershipStatus {
        self.core
            .repository
            .pin_active_generation()
            .as_deref()
            .map_or_else(ExactRunMembershipStatus::default, |active| {
                active.membership_status()
            })
    }

    fn read_cache(&self) -> &Arc<VerifiedReadCache> {
        &self.read_cache
    }

    fn advanced_reduction_status(&self) -> PersistentReductionStatus {
        self.reduction.as_deref().map_or_else(
            PersistentReductionStatus::default,
            PersistentReductionIndex::status,
        )
    }

    fn similarity_page_cache_status(&self) -> SimilarityIndexPageCacheStatus {
        self.reduction.as_deref().map_or_else(
            SimilarityIndexPageCacheStatus::default,
            PersistentReductionIndex::similarity_page_cache_status,
        )
    }
}

impl<M, C> DurableNamespace<M, C>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    /// Recovers the newest generation, durably reserves a fresh Inode ID
    /// range, and only then enables mutation admission.
    ///
    /// A new repository first publishes its initial reservation generation.
    /// Reopening an existing repository deliberately skips every unused ID in
    /// the prior reservation before publishing a new range.
    ///
    /// # Errors
    ///
    /// Returns recovery, reservation, graph verification, durability, or
    /// namespace-construction failures. A zero or overflowing reservation span
    /// is rejected before mutation admission.
    pub fn open(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError> {
        let read_cache = Arc::new(VerifiedReadCache::new_system()?);
        Self::open_using(
            config,
            generations,
            containers,
            inode_reservation_span,
            Arc::new(ScanManifestReaders { read_cache }),
            RecoveryMode::Full,
        )
    }

    /// Opens a writable namespace and pins the newest valid Exact Index Run
    /// Set behind every committed Manifest reader.
    ///
    /// Missing or corrupt index acceleration falls back to verified Container
    /// scans and does not make Namespace DATA unavailable. The recovered Run
    /// Set is immutable and remains pinned for this appliance lifetime; newly
    /// committed locations remain readable through the same correctness
    /// fallback until a later checkpoint-index publisher activates them.
    ///
    /// # Errors
    ///
    /// Returns the same recovery, reservation, graph, durability, and
    /// namespace-construction failures as [`Self::open`]. Exact Index recovery
    /// failure is deliberately not a Namespace failure.
    pub fn open_with_index<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        Self::open_with_optional_reduction_index(
            config,
            generations,
            containers,
            indexes,
            None,
            inode_reservation_span,
            RecoveryMode::Full,
        )
    }

    /// Opens a writable namespace with one immutable pool-wide
    /// Exact/Similarity pair pinned for bounded write-through Prefix trials.
    ///
    /// A missing, stale, or corrupt Similarity snapshot disables advanced
    /// reduction without affecting Exact reuse or data availability.
    ///
    /// # Errors
    ///
    /// Returns the same recovery, reservation, graph, durability, and
    /// namespace-construction failures as [`Self::open`]. Exact or Similarity
    /// Index recovery failure only disables the affected acceleration path.
    pub fn open_with_reduction_indexes<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        similarities: &SimilarityIndexRepository<X>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        Self::open_with_optional_reduction_index(
            config,
            generations,
            containers,
            indexes,
            Some(similarities),
            inode_reservation_span,
            RecoveryMode::Full,
        )
    }

    /// Mounts after structural validation; demand reads and new DATA still use
    /// full verification. The owning daemon must scrub in the background and
    /// keep online deletion disabled until that pass succeeds.
    ///
    /// # Errors
    /// Returns structural recovery, reservation, or Namespace setup failures.
    pub fn open_with_structural_recovery<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        similarities: &SimilarityIndexRepository<X>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        Self::open_with_optional_reduction_index(
            config,
            generations,
            containers,
            indexes,
            Some(similarities),
            inode_reservation_span,
            RecoveryMode::Structural,
        )
    }

    /// Opens the committed Metadata graph without scanning Container storage.
    /// The caller must pass `take_startup_data_verification()` to background
    /// scrub and keep online deletion disabled until that check succeeds.
    ///
    /// # Errors
    /// Returns Metadata recovery, reservation, or Namespace setup failures.
    pub fn open_with_committed_recovery<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        similarities: &SimilarityIndexRepository<X>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        Self::open_with_optional_reduction_index(
            config,
            generations,
            containers,
            indexes,
            Some(similarities),
            inode_reservation_span,
            RecoveryMode::Committed,
        )
    }

    /// Transfers the selected commit's outstanding DATA check to initial scrub.
    #[must_use]
    pub fn take_startup_data_verification(
        &mut self,
    ) -> Option<fastdup_store::PendingDataVerification> {
        self.startup_data_verification.take()
    }

    fn open_with_optional_reduction_index<X>(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: &ExactIndexRunRepository<X>,
        similarities: Option<&SimilarityIndexRepository<X>>,
        inode_reservation_span: u64,
        recovery_mode: RecoveryMode,
    ) -> Result<Self, DurableNamespaceError>
    where
        X: Clone + Send + Sync + StorageIo + 'static,
    {
        let read_cache = Arc::new(VerifiedReadCache::new_system()?);
        let exact_started = Instant::now();
        let recovered = indexes.pin_recovered_generation().and_then(|active| {
            if let Some(index) = &active {
                let retiring = indexes.retiring_containers(index)?;
                containers.install_retiring_selection_barrier(&retiring);
            }
            Ok(active)
        });
        let initially_degraded = recovered.is_err();
        let active = recovered.ok().flatten();
        eprintln!(
            "recovery_phase=exact_active state=complete elapsed_ms={} runs={} degraded={}",
            exact_started.elapsed().as_millis(),
            active.as_ref().map_or(0, |active| active.run_count()),
            initially_degraded
        );
        let online =
            similarities.and_then(
                |repository| match fastdup_store::OnlineSimilarityRepository::open(
                    repository.clone(),
                    indexes,
                ) {
                    Ok(online) => Some(Arc::new(online)),
                    Err(error) => {
                        eprintln!("online Similarity recovery disabled: {error}");
                        None
                    }
                },
            );
        let reduction = online
            .as_ref()
            .map(|online| Arc::new(PersistentReductionIndex::online(Arc::clone(online))));
        let similarity = online.map(SimilarityPublicationQueue::start).transpose()?;
        let profile = active
            .as_ref()
            .map_or_else(checkpoint_exact_index_profile_v1, |index| {
                index.run_set().profile()
            });
        let core = Arc::new(ExactPublisherCore {
            repository: indexes.clone(),
            profile,
            degraded: AtomicBool::new(initially_degraded),
            recent: RwLock::new(BTreeMap::new()),
            similarity,
            failed_reduction_guard: Mutex::new(None),
        });
        let publisher = ExactPublicationQueue::start(Arc::clone(&core))?;
        let manifest_readers: Arc<dyn ManifestReaderPolicy<C>> = Arc::new(IndexedManifestReaders {
            core,
            publisher,
            read_cache,
            reduction,
        });
        Self::open_using(
            config,
            generations,
            containers,
            inode_reservation_span,
            manifest_readers,
            recovery_mode,
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keep recovery, inode reservation and mutation admission in lifecycle order"
    )]
    fn open_using(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        inode_reservation_span: u64,
        manifest_readers: Arc<dyn ManifestReaderPolicy<C>>,
        recovery_mode: RecoveryMode,
    ) -> Result<Self, DurableNamespaceError> {
        if inode_reservation_span == 0 {
            return Err(DurableNamespaceError::InvalidReservationSpan);
        }
        let graph_verifier = manifest_readers.graph_verifier(containers.clone());
        let proof_started = Instant::now();
        let phase = match recovery_mode {
            RecoveryMode::Full => "namespace_data_proof",
            RecoveryMode::Structural => "namespace_structure",
            RecoveryMode::Committed => "namespace_commit",
        };
        eprintln!("recovery_phase={phase} state=started");
        let (recovered, startup_data_verification) = match recovery_mode {
            RecoveryMode::Committed => {
                let (recovered, pending) = generations.recover_committed_for_mount(&containers)?;
                (recovered, Some(pending))
            }
            RecoveryMode::Structural => (
                generations.recover_latest_with_structural_files(&containers)?,
                None,
            ),
            RecoveryMode::Full => (
                generations.recover_latest_with_verified_files_using(
                    &containers,
                    graph_verifier.as_ref(),
                )?,
                None,
            ),
        };
        eprintln!(
            "recovery_phase={phase} state=complete elapsed_ms={}",
            proof_started.elapsed().as_millis()
        );
        let reservation_started = Instant::now();
        let (root, next_inode, reservation_end, installed_record, verified_files) = match recovered
        {
            None => {
                let reservation_end = FIRST_REGULAR_INODE
                    .checked_add(inode_reservation_span)
                    .ok_or(DurableNamespaceError::InodeReservationExhausted)?;
                let root = NamespaceRoot::new(
                    reservation_end,
                    FIRST_REGULAR_INODE,
                    0,
                    Vec::new(),
                    Vec::new(),
                )?;
                let installed_record = generations.commit_namespace(&root)?;
                (
                    root,
                    FIRST_REGULAR_INODE,
                    reservation_end,
                    installed_record,
                    Vec::new(),
                )
            }
            Some(recovered) => {
                let (recovered, prior_files) = recovered.into_parts();
                let previous = recovered.namespace_root();
                let next_inode = recovered.inode_reservation_end_high_water();
                let reservation_end = next_inode
                    .checked_add(inode_reservation_span)
                    .ok_or(DurableNamespaceError::InodeReservationExhausted)?;
                let root = NamespaceRoot::new_with_root_metadata(
                    reservation_end,
                    next_inode,
                    previous.namespace_mutation_sequence(),
                    previous.root_metadata().clone(),
                    previous.inodes().to_vec(),
                    previous.entries().to_vec(),
                )?;
                let committed = if recovered.rejected_newer_generations() == 0 {
                    // Recovery established the selected graph under the startup policy.
                    // Reserving IDs preserves every inode/Manifest binding; the
                    // ordinary successor fence must still match the WAL head.
                    let predecessor =
                        SuccessorPredecessor::from_committed_record(recovered.record());
                    let proofs: Vec<_> = prior_files
                        .iter()
                        .map(|file| {
                            generations.reuse_manifest_successor(
                                predecessor,
                                file.manifest_summary().expect(
                                    "ASSERT: recovered files retain verified Manifest roots",
                                ),
                            )
                        })
                        .collect();
                    generations.commit_namespace_with_successor_proofs_using(
                        &root,
                        &containers,
                        predecessor,
                        &proofs,
                        graph_verifier.as_ref(),
                    )?
                } else {
                    // A fallback graph is not the current WAL head. Preserve
                    // the existing complete verification/transition path.
                    generations.commit_namespace_with_verified_files_using(
                        &root,
                        &containers,
                        graph_verifier.as_ref(),
                    )?
                };
                let (installed_record, verified_files) = committed.into_parts();
                (
                    root,
                    next_inode,
                    reservation_end,
                    installed_record,
                    verified_files,
                )
            }
        };
        eprintln!(
            "recovery_phase=inode_reservation state=complete elapsed_ms={}",
            reservation_started.elapsed().as_millis()
        );
        let container_generations =
            containers.open_generation_allocator(CONTAINER_GENERATION_RESERVATION_SPAN_V1)?;
        let manifests = load_manifest_cache(&root, &verified_files)?;
        let namespace = namespace_from_verified_files_using(
            config,
            &root,
            next_inode,
            reservation_end,
            verified_files,
            true,
            |file| manifest_readers.prepare(file),
        )?;
        let available_workers = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        let checkpoint_workers = available_workers;
        let namespace = Arc::new(namespace);
        let online_dependency_proofs = Arc::new(OnlineDependencyProofs::new()?);
        let write_through = install_write_through(
            &namespace,
            containers.clone(),
            container_generations.clone(),
            Arc::clone(&manifest_readers),
            checkpoint_workers,
            Arc::clone(&online_dependency_proofs),
        );
        Ok(Self {
            startup_data_verification,
            namespace,
            generations,
            containers,
            checkpoint_lock: Mutex::new(()),
            checkpoint_timings: CheckpointTimings::default(),
            installed_predecessor: Mutex::new(SuccessorPredecessor::from_committed_record(
                installed_record,
            )),
            manifests: Mutex::new(manifests),
            container_generations,
            manifest_readers,
            checkpoint_workers,
            write_through,
            online_dependency_proofs,
        })
    }

    #[must_use]
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    #[must_use]
    pub fn namespace_arc(&self) -> Arc<Namespace> {
        Arc::clone(&self.namespace)
    }

    /// Returns the number of immutable Exact-Index Runs pinned for ordinary
    /// Manifest demand reads. Zero means this mount is using the verified
    /// Container-scan fallback.
    #[must_use]
    pub fn exact_index_run_count(&self) -> usize {
        self.manifest_readers.exact_index_run_count()
    }

    /// Reports that Exact-Index recovery, lookup, publication, or activation
    /// degraded while Namespace durability remained available.
    #[must_use]
    pub fn exact_index_degraded(&self) -> bool {
        self.manifest_readers.exact_index_degraded()
    }

    /// Returns bounded, pressure-aware Exact-Index hot-page cache evidence.
    #[must_use]
    pub fn exact_index_page_cache_status(&self) -> ExactIndexPageCacheStatus {
        self.manifest_readers.exact_index_page_cache_status()
    }

    /// Returns active immutable-Run membership-filter memory and probe evidence.
    #[must_use]
    pub fn exact_run_membership_status(&self) -> ExactRunMembershipStatus {
        self.manifest_readers.exact_run_membership_status()
    }

    /// Returns pressure-aware Similarity hot-page cache evidence.
    #[must_use]
    pub fn similarity_index_page_cache_status(&self) -> SimilarityIndexPageCacheStatus {
        self.manifest_readers.similarity_page_cache_status()
    }

    /// Returns reduction counters without traversing ingest buffers.
    #[must_use]
    pub fn advanced_reduction_status(&self) -> PersistentReductionStatus {
        self.manifest_readers.advanced_reduction_status()
    }

    /// Returns bounded shared read-cache memory and hit/miss evidence.
    ///
    /// A zero target means memory or Swap pressure disabled admission and
    /// purged all cached payloads. Fixed set metadata is reported separately
    /// from resident payload bytes.
    #[must_use]
    pub fn verified_read_cache_status(&self) -> VerifiedReadCacheStatus {
        self.manifest_readers.read_cache_status()
    }

    /// Shares the existing online cache with background work, including fresh
    /// scrub evidence. It does not create another cache owner or memory quota.
    #[must_use]
    pub fn verified_read_cache(&self) -> Arc<VerifiedReadCache> {
        Arc::clone(self.manifest_readers.read_cache())
    }

    /// Returns process-local verified Container-envelope cache telemetry.
    #[must_use]
    pub fn container_descriptor_cache_status(&self) -> ContainerDescriptorCacheStatus {
        self.containers.descriptor_cache_status()
    }

    /// Returns pressure, admission, and hit evidence for historical S3-FIFO.
    ///
    /// Active and Frozen Generation proofs are pinned separately and therefore
    /// do not contribute to this rebuildable cache's entry count.
    #[must_use]
    pub fn historical_proof_cache_status(&self) -> HistoricalProofCacheStatus {
        self.online_dependency_proofs.historical_status()
    }

    /// Returns memory accounting for the non-evictable Active and Frozen proof sets.
    #[must_use]
    pub fn generation_proof_set_status(&self) -> GenerationProofSetStatus {
        self.online_dependency_proofs.generation_status()
    }

    /// Returns the runtime worker cap for independent Compression Regions.
    #[must_use]
    pub const fn checkpoint_worker_limit(&self) -> NonZeroUsize {
        self.checkpoint_workers
    }

    /// Returns bounded live state of the pre-commit SeqCDC/Container pipeline.
    #[must_use]
    pub fn write_through_status(&self) -> WriteThroughStatus {
        self.write_through.status()
    }

    /// Opens the checkpoint staging escape hatch for currently queued writes.
    ///
    /// Callers may use this only after mutation admission is closed for a
    /// transient checkpoint condition. Already queued Ingest bytes are part of
    /// the write-through queue budget and may then move into the pending Lane
    /// region, preventing a frozen commit cut from waiting on staging bytes that
    /// only that same commit can release.
    pub fn force_checkpoint_staging_gate(&self) {
        self.write_through.force_checkpoint_staging_gate();
    }

    /// Clears the staging escape hatch after checkpoint absorption or another
    /// normal release path has had an opportunity to restore the gate.
    pub fn clear_checkpoint_staging_gate(&self) {
        self.write_through.clear_checkpoint_staging_gate();
    }

    /// Opens the staging gate only for a transient checkpoint admission pause.
    ///
    /// This policy keeps `IntegrityFailure` and Shutdown from bypassing normal
    /// memory admission while allowing a checkpoint retry to drain Ingest bytes
    /// that were already queued before the transient pause closed admission.
    pub fn force_checkpoint_staging_gate_for_transient_pause(&self) -> bool {
        let reason = self.namespace.admission_status().reason;
        let transient = matches!(
            reason,
            Some(
                fastdup_posix::AdmissionPauseReason::CheckpointTimeout
                    | fastdup_posix::AdmissionPauseReason::DirtyPressure
                    | fastdup_posix::AdmissionPauseReason::DurabilityLag
                    | fastdup_posix::AdmissionPauseReason::ProgressFailure
            )
        );
        if !transient {
            return false;
        }
        self.write_through.force_checkpoint_staging_gate();
        true
    }

    /// Reports whether the one-generation staging escape hatch is currently open.
    #[must_use]
    pub fn checkpoint_staging_gate_open(&self) -> bool {
        self.write_through.checkpoint_staging_gate_open()
    }

    /// Cumulative staging batches admitted through the checkpoint escape hatch.
    #[must_use]
    pub fn forced_staging_batches(&self) -> u64 {
        self.write_through.forced_staging_batches()
    }

    /// Starts a bounded, payload-free trace of real online proof-cache events.
    ///
    /// Trace capture is benchmark instrumentation. It never changes cache
    /// authority, admission, eviction, durability, or recovery behavior.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive bounds and a second concurrent capture.
    pub fn start_online_proof_trace(&self, max_events: usize) -> Result<(), ProofCacheReplayError> {
        self.online_dependency_proofs.trace.start(max_events)
    }

    /// Finishes the active online proof-cache trace.
    ///
    /// # Errors
    ///
    /// Returns an error if capture was not active or exceeded its declared
    /// event bound. An overflow never truncates a trace silently.
    pub fn finish_online_proof_trace(&self) -> Result<ProofCacheTrace, ProofCacheReplayError> {
        self.online_dependency_proofs.trace.finish()
    }

    /// Durably commits one complete prefix of accepted mutations.
    ///
    /// DATA containers are sealed and synchronized first, immutable manifests
    /// and the Namespace Root follow, and the Commit WAL is synchronized last.
    /// A failed call leaves the same frozen cut available for retry while later
    /// writes remain live in the next epoch.
    ///
    /// # Errors
    ///
    /// Returns frozen-view, format, container, metadata, graph, or durability
    /// failures. `Ok(None)` means no accepted mutation is waiting for a commit.
    ///
    /// # Panics
    ///
    /// Panics when a prior impossible invariant poisoned the single checkpoint
    /// lock or when a verified installed view disagrees with its own cut.
    pub fn checkpoint(&self) -> Result<Option<CommitRecord>, DurableNamespaceError> {
        self.checkpoint_profiled()
            .map(|profiled| profiled.map(ProfiledCheckpoint::record))
    }

    /// Live and cumulative pipeline observations, including unfinished waits.
    #[must_use]
    pub fn pipeline_timings(&self) -> Vec<fastdup_store::OperationTimingSnapshot> {
        let mut result = self.checkpoint_timings.snapshots();
        result.extend(self.manifest_readers.publication_timings());
        result
    }

    /// Returns whether a checkpoint currently owns the serialization lock.
    ///
    /// The result is transient. Callers may use it only to avoid admitting new
    /// maintenance work; it never replaces a lock or a durable state decision.
    #[must_use]
    pub fn checkpoint_lock_is_held(&self) -> bool {
        self.checkpoint_lock.try_lock().is_err()
    }

    /// Commits the same durable prefix as [`Self::checkpoint`] and returns
    /// bounded phase/counter evidence for observability and benchmarks.
    ///
    /// Metrics are advisory and are returned only for a successful commit.
    /// They never participate in visibility, recovery, or integrity decisions.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::checkpoint`].
    ///
    /// # Panics
    ///
    /// Panics for the same impossible poisoned-lock or paired-proof failures as
    /// [`Self::checkpoint`], or if monotonic duration accounting moves backward.
    pub fn checkpoint_profiled(&self) -> Result<Option<ProfiledCheckpoint>, DurableNamespaceError> {
        let timings = &self.checkpoint_timings;
        let total_started = timings.begin(CheckpointStage::Total);
        let mut metrics = CheckpointMetrics::default();
        let lock_started = timings.begin(CheckpointStage::CheckpointLock);
        let _guard = self
            .checkpoint_lock
            .lock()
            .expect("ASSERT: durable namespace checkpoint lock poisoned");
        lock_started.finish_into(&mut metrics.checkpoint_lock);
        let proof_started = timings.begin(CheckpointStage::ProofFreeze);
        // Freeze proofs before the namespace takes its mutation cut. Anything
        // accepted concurrently afterward stays in the next Active set. A
        // late proof for the selected cut may remain Active for one extra
        // generation, which is conservative and never demotes it too early.
        let newly_frozen_proofs = self.online_dependency_proofs.freeze_for_commit();
        proof_started.finish_into(&mut metrics.proof_freeze);
        let capture_started = timings.begin(CheckpointStage::CutCapture);
        let sealed_at_cut = self.write_through.capture_cut();
        capture_started.finish_into(&mut metrics.cut_capture);
        let freeze_started = timings.begin(CheckpointStage::Freeze);
        let commit = match self.namespace.begin_commit() {
            Ok(Some(commit)) => commit,
            Ok(None) => {
                // A late ingest proof can outlive the cut it helped commit.
                // No dirty namespace means this newly frozen owner contains
                // committed history. Demote it so idle GC is not pinned forever;
                // concurrent post-cut work retains its separate Active owner.
                self.write_through.complete_cut(sealed_at_cut);
                self.write_through.clear_checkpoint_staging_gate();
                if newly_frozen_proofs {
                    self.online_dependency_proofs.complete_frozen();
                }
                return Ok(None);
            }
            Err(error) => {
                self.online_dependency_proofs
                    .cancel_new_freeze(newly_frozen_proofs);
                return Err(error.into());
            }
        };
        freeze_started.finish_into(&mut metrics.freeze);
        self.write_through
            .wait_for_commit_cut(&commit, timings, &mut metrics);
        let (stable, residues) =
            self.write_through
                .flush_stable_for_commit_cut(&commit, timings, &mut metrics)?;
        let attach_started = timings.begin(CheckpointStage::RecipeAttach);
        self.namespace.externalize_verified_extents(stable);
        attach_started.finish_into(&mut metrics.recipe_attach);
        let setup_started = timings.begin(CheckpointStage::WriterSetup);
        let mut writer = AdaptiveCommitWriter::new(
            &self.containers,
            &self.container_generations,
            self.manifest_readers.as_ref(),
            self.checkpoint_workers,
            Arc::clone(&self.online_dependency_proofs),
            residues,
        );
        setup_started.finish_into(&mut metrics.writer_setup);
        let manifest_plan_started = timings.begin(CheckpointStage::ManifestPlan);
        let mut manifests = Vec::new();
        manifests
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let installed_manifests = self
            .manifests
            .lock()
            .expect("ASSERT: installed Manifest cache lock poisoned");
        for inode in commit.inodes() {
            writer.begin_inode(
                inode.inode(),
                if commit.prefers_small_file_tier(inode) {
                    ContainerPlacement::SmallFile
                } else {
                    ContainerPlacement::Data
                },
                self.manifest_readers.advanced_reduction_available()
                    && self.namespace.advanced_reduction_enabled(inode.inode()),
            )?;
            let previous = installed_manifests
                .binary_search_by_key(&inode.inode().get(), |manifest| manifest.inode)
                .ok()
                .map(|index| installed_manifests[index]);
            manifests.push(plan_checkpoint_manifest(
                inode,
                previous,
                &self.generations,
                &mut writer,
            )?);
        }
        drop(installed_manifests);
        let (level_zero_entries, reduction_metrics, retained_ranges) = writer.finish()?;
        manifest_plan_started.finish_into(&mut metrics.manifest_plan);
        metrics.merge_reduction(&reduction_metrics);
        let exact_index_started = timings.begin(CheckpointStage::IndexPublish);
        self.manifest_readers.publish_level_zero(level_zero_entries);
        // A checkpoint may have produced DATA outside the write-through
        // observer (for example, a partial lane drained at the commit cut).
        // Its Exact locations must be part of the durable activation history
        // before the Namespace generation can become visible. Ordinary ingest
        // keeps publication asynchronous until this commit/Sync fence.
        self.manifest_readers.flush_level_zero();
        exact_index_started.finish_into(&mut metrics.exact_index_publish);
        let metadata_started = timings.begin(CheckpointStage::MetadataCommit);
        let record = self.publish_generation(&commit, manifests, &retained_ranges)?;
        self.write_through.complete_cut(sealed_at_cut);
        self.write_through.clear_checkpoint_staging_gate();
        self.online_dependency_proofs.complete_frozen();
        metadata_started.finish_into(&mut metrics.metadata_commit);
        total_started.finish_into(&mut metrics.total);
        Ok(Some(ProfiledCheckpoint { record, metrics }))
    }
}

#[derive(Debug)]
pub enum DurableNamespaceError {
    Io(io::Error),
    Posix(PosixError),
    Metadata(MetadataFormatError),
    Store(StoreError),
    Generation(GenerationError),
    Manifest(ManifestReadError),
    ReadCache(VerifiedReadCacheError),
    Mount(MountError),
    InvalidReservationSpan,
    InodeReservationExhausted,
    ContainerGenerationExhausted,
    ArithmeticOverflow,
    OutOfMemory,
    FrozenViewMismatch,
    ChunkLengthConflict {
        chunk_id: ChunkId,
        first_length: u64,
        second_length: u64,
    },
}

impl fmt::Display for DurableNamespaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DurableNamespaceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Store(error) => Some(error),
            Self::Generation(error) => Some(error),
            Self::Manifest(error) => Some(error),
            Self::ReadCache(error) => Some(error),
            Self::Mount(error) => Some(error),
            Self::Posix(_)
            | Self::InvalidReservationSpan
            | Self::InodeReservationExhausted
            | Self::ContainerGenerationExhausted
            | Self::ArithmeticOverflow
            | Self::OutOfMemory
            | Self::FrozenViewMismatch
            | Self::ChunkLengthConflict { .. } => None,
        }
    }
}

impl From<io::Error> for DurableNamespaceError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<PosixError> for DurableNamespaceError {
    fn from(error: PosixError) -> Self {
        Self::Posix(error)
    }
}

impl From<MetadataFormatError> for DurableNamespaceError {
    fn from(error: MetadataFormatError) -> Self {
        Self::Metadata(error)
    }
}

impl From<StoreError> for DurableNamespaceError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<GenerationError> for DurableNamespaceError {
    fn from(error: GenerationError) -> Self {
        Self::Generation(error)
    }
}

impl From<ManifestReadError> for DurableNamespaceError {
    fn from(error: ManifestReadError) -> Self {
        Self::Manifest(error)
    }
}

impl From<VerifiedReadCacheError> for DurableNamespaceError {
    fn from(error: VerifiedReadCacheError) -> Self {
        Self::ReadCache(error)
    }
}

impl From<MountError> for DurableNamespaceError {
    fn from(error: MountError) -> Self {
        Self::Mount(error)
    }
}

#[cfg(test)]
#[path = "checkpoint/lane_lifetime_tests.rs"]
mod lane_lifetime_tests;

#[cfg(test)]
#[path = "checkpoint/ingest_handoff_tests.rs"]
mod ingest_handoff_tests;

#[cfg(test)]
mod exact_publication_tests;
#[cfg(test)]
mod pipeline_telemetry_tests;
