use crate::generation_log::{GenerationLog, GenerationLogError, LogSnapshot};
use crate::manifest_tree::{
    ManifestRangeExtent, ManifestTreeError, ManifestTreeSummary, append_manifest_tree,
    encode_manifest_tree, flatten_manifest_tree, read_manifest_tree_range,
    rewrite_manifest_tree_range, rewrite_manifest_tree_range_successor, scan_manifest_tree,
    splice_manifest_tree, truncate_manifest_tree,
};
use crate::metadata_mark_catalog::{
    MetadataMarkCatalogError, audit_named as audit_metadata_mark_catalog,
    commit_binding as metadata_mark_commit_binding,
    is_published_name as is_metadata_mark_catalog_name,
    parse_generation as parse_metadata_mark_generation, prepare as prepare_metadata_mark_catalog,
    prepare_addition as prepare_metadata_mark_addition,
};
use crate::{ContainerRepository, StorageIo, StoreError, VerifiedManifestFile};
use fastdup_format::{
    CommitFormatError, CommitRecord, CommitRecordHash, MAX_METADATA_OBJECT_BYTES, ManifestExtent,
    ManifestLeaf, MetadataFormatError, MetadataMarkCatalogRunKind, MetadataObjectId, NamespaceRoot,
    PolicySetId,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

const METADATA_SUFFIX: &str = ".fdm";
const WRITE_BLOCK_BYTES: usize = 4_096;
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

struct RecoveredGraph {
    generation: RecoveredGeneration,
    manifests: VerifiedManifests,
}

struct SelectedGraph {
    record: CommitRecord,
    root: NamespaceRoot,
    manifests: VerifiedManifests,
}

#[derive(Clone, Debug)]
pub struct GenerationRepository<I> {
    storage: I,
    supported_policy: PolicySetId,
    commit_lock: Arc<Mutex<()>>,
    metadata_root_pins: Arc<Mutex<BTreeMap<MetadataObjectId, usize>>>,
    metadata_root_pin_handles: Arc<Mutex<Vec<Weak<MetadataRootPinInner>>>>,
    metadata_gc_barrier: Arc<RwLock<()>>,
    metadata_gc_epoch: Arc<AtomicU64>,
    metadata_gc_clean: Arc<Mutex<Option<MetadataGcCleanState>>>,
    metadata_gc_delta: Arc<Mutex<MetadataGcDeltaJournal>>,
    metadata_gc_run_lock: Arc<Mutex<()>>,
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

#[derive(Clone, Copy)]
struct CommittedMetadata {
    record: CommitRecord,
    namespace_root_published_new: bool,
    wal_rotated: bool,
}

struct PublishedManifestProof {
    summary: ManifestTreeSummary,
    introduced_chunks: BTreeMap<fastdup_format::ChunkId, u64>,
    introduced_metadata: BTreeSet<MetadataObjectId>,
    metadata_root_pin: MetadataRootPin,
}

struct MetadataGcInventory {
    candidates: Vec<(MetadataObjectId, String)>,
    catalog_names: Vec<String>,
    catalog_generation_high_water: u64,
}

impl fmt::Debug for MetadataRootPin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataRootPin")
            .field("root", &self.inner.root)
            .finish_non_exhaustive()
    }
}

impl Drop for MetadataRootPinInner {
    fn drop(&mut self) {
        let mut pins = self
            .pins
            .lock()
            .expect("ASSERT: Metadata root pin registry poisoned during release");
        let remove = match pins.get_mut(&self.root) {
            Some(count) => {
                assert_ne!(
                    *count, 0,
                    "ASSERT: registered Metadata root pin count is nonzero"
                );
                if *count == 1 {
                    true
                } else {
                    *count -= 1;
                    false
                }
            }
            None => panic!("ASSERT: Metadata root pin release has an acquisition"),
        };
        if remove {
            pins.remove(&self.root);
        }
        drop(pins);
        if self.release_requires_exact.load(Ordering::Acquire) {
            mark_metadata_gc_exact_required(
                &self.metadata_gc_epoch,
                &self.metadata_gc_delta,
                MetadataGcExactReason::MetadataRootPinDrain,
            );
        }
    }
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

impl<I: StorageIo> RequiredChunkVerifier for ContainerRepository<I> {
    fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError> {
        ContainerRepository::verify_required_chunks(self, required)
    }
}

/// Complete graph verifier using one pinned Exact-Index generation with the
/// authoritative verified Container scan as a single fallback.
#[derive(Clone, Debug)]
pub struct IndexedRequiredChunkVerifier<C, X> {
    containers: ContainerRepository<C>,
    index: crate::ExactIndexGenerationPin<X>,
}

impl<C, X> IndexedRequiredChunkVerifier<C, X> {
    #[must_use]
    pub const fn new(
        containers: ContainerRepository<C>,
        index: crate::ExactIndexGenerationPin<X>,
    ) -> Self {
        Self { containers, index }
    }
}

impl<C: StorageIo, X: StorageIo> RequiredChunkVerifier for IndexedRequiredChunkVerifier<C, X> {
    fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError> {
        for (chunk_id, logical_length) in required {
            if self
                .containers
                .find_verified_chunk_with_index(&self.index, *chunk_id, *logical_length)?
                .is_none()
            {
                return self.containers.verify_required_chunks(required);
            }
        }
        Ok(())
    }
}

impl<I: StorageIo> GenerationRepository<I> {
    #[must_use]
    pub fn new(storage: I, supported_policy: PolicySetId) -> Self {
        Self {
            storage,
            supported_policy,
            commit_lock: Arc::new(Mutex::new(())),
            metadata_root_pins: Arc::new(Mutex::new(BTreeMap::new())),
            metadata_root_pin_handles: Arc::new(Mutex::new(Vec::new())),
            metadata_gc_barrier: Arc::new(RwLock::new(())),
            metadata_gc_epoch: Arc::new(AtomicU64::new(0)),
            metadata_gc_clean: Arc::new(Mutex::new(None)),
            metadata_gc_delta: Arc::new(Mutex::new(MetadataGcDeltaJournal::default())),
            metadata_gc_run_lock: Arc::new(Mutex::new(())),
        }
    }

    fn pin_metadata_root(&self, root: MetadataObjectId) -> MetadataRootPin {
        let mut pins = self
            .metadata_root_pins
            .lock()
            .expect("ASSERT: Metadata root pin registry poisoned during acquisition");
        let count = pins.entry(root).or_insert(0);
        *count = count
            .checked_add(1)
            .expect("ASSERT: Metadata root pin count cannot overflow");
        drop(pins);
        let inner = Arc::new(MetadataRootPinInner {
            root,
            pins: Arc::clone(&self.metadata_root_pins),
            metadata_gc_epoch: Arc::clone(&self.metadata_gc_epoch),
            release_requires_exact: AtomicBool::new(true),
            metadata_gc_delta: Arc::clone(&self.metadata_gc_delta),
        });
        self.metadata_root_pin_handles
            .lock()
            .expect("ASSERT: Metadata root pin handle registry poisoned")
            .push(Arc::downgrade(&inner));
        MetadataRootPin { inner }
    }

    fn mark_metadata_root_releases_covered_by_commit(&self, roots: &BTreeSet<MetadataObjectId>) {
        let mut handles = self
            .metadata_root_pin_handles
            .lock()
            .expect("ASSERT: Metadata root pin handle registry poisoned");
        handles.retain(|handle| {
            let Some(inner) = handle.upgrade() else {
                return false;
            };
            if roots.contains(&inner.root) {
                inner.release_requires_exact.store(false, Ordering::Release);
            }
            true
        });
    }

    fn mark_all_metadata_root_pin_releases_exact(&self) {
        let mut handles = self
            .metadata_root_pin_handles
            .lock()
            .expect("ASSERT: Metadata root pin handle registry poisoned");
        handles.retain(|handle| {
            let Some(inner) = handle.upgrade() else {
                return false;
            };
            inner.release_requires_exact.store(true, Ordering::Release);
            true
        });
    }

    /// Publishes one verified immutable Manifest metadata object.
    ///
    /// Identical content-addressed objects are reused. A same-name object with
    /// different or invalid bytes fails closed.
    ///
    /// # Errors
    ///
    /// Returns format, bounded-size, identity, or durable-publication errors.
    ///
    /// # Panics
    ///
    /// Panics only if a previously content-identified tree plan disagrees with
    /// the generic Metadata Object writer, an impossible internal invariant.
    pub fn publish_manifest(
        &self,
        manifest: &ManifestLeaf,
    ) -> Result<MetadataObjectId, GenerationError> {
        Ok(self
            .publish_complete_manifest(manifest, true)?
            .summary
            .root())
    }

    /// Publishes and rereads one complete Manifest tree while returning an
    /// opaque proof suitable for an incremental successor commit.
    ///
    /// # Errors
    ///
    /// Returns format, allocation, identity, or durable-publication errors.
    ///
    /// # Panics
    ///
    /// Panics if the deterministic tree plan disagrees with the verified
    /// content-addressed metadata writer.
    pub fn publish_manifest_successor(
        &self,
        predecessor: SuccessorPredecessor,
        manifest: &ManifestLeaf,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        self.publish_manifest_successor_with_sync(predecessor, manifest, true)
    }

    /// Stages one complete Manifest successor without a directory sync.
    ///
    /// The caller must publish a Namespace Root through this repository before
    /// WAL visibility. That publication supplies the shared metadata-directory
    /// durability barrier for every staged object in the generation.
    ///
    /// # Errors
    ///
    /// Returns the same format, allocation, identity, or publication errors as
    /// [`Self::publish_manifest_successor`].
    pub fn stage_manifest_successor(
        &self,
        predecessor: SuccessorPredecessor,
        manifest: &ManifestLeaf,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        self.publish_manifest_successor_with_sync(predecessor, manifest, false)
    }

    fn publish_manifest_successor_with_sync(
        &self,
        predecessor: SuccessorPredecessor,
        manifest: &ManifestLeaf,
        sync_root: bool,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let published = self.publish_complete_manifest(manifest, sync_root)?;
        Ok(ManifestSuccessorProof {
            predecessor,
            summary: published.summary,
            introduced_chunks: published.introduced_chunks,
            introduced_metadata: published.introduced_metadata,
            metadata_root_pin: published.metadata_root_pin,
        })
    }

    fn publish_complete_manifest(
        &self,
        manifest: &ManifestLeaf,
        sync_root: bool,
    ) -> Result<PublishedManifestProof, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        let tree = encode_manifest_tree(manifest)?;
        let mut introduced_metadata = BTreeSet::new();
        for (expected_id, encoded) in tree.objects() {
            let staged = self.stage_metadata_with_status(encoded)?;
            assert_eq!(
                staged.object_id, *expected_id,
                "ASSERT: Manifest tree plan identity must equal published object identity"
            );
            if staged.published_new {
                introduced_metadata.insert(staged.object_id);
            }
        }
        if sync_root {
            self.storage.sync_root()?;
        }
        let summary = ManifestTreeSummary::new(
            tree.root(),
            manifest.file_length(),
            manifest_allocated_bytes(manifest.extents())?,
        );
        Ok(PublishedManifestProof {
            summary,
            introduced_chunks: manifest_dependencies(manifest.extents())?,
            introduced_metadata,
            metadata_root_pin: self.pin_metadata_root(summary.root()),
        })
    }

    /// Appends a locally encoded Manifest suffix by rewriting only the prior
    /// tree's right spine and publishing the new suffix child-first.
    ///
    /// # Errors
    ///
    /// Returns predecessor-tree, suffix-format, identity, arithmetic, or
    /// durable-publication errors.
    ///
    /// # Panics
    ///
    /// Panics if the deterministic append plan disagrees with the verified
    /// content-addressed metadata writer.
    pub fn publish_manifest_append(
        &self,
        predecessor: SuccessorPredecessor,
        previous: ManifestTreeSummary,
        appended: &[ManifestExtent],
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        self.publish_manifest_append_with_sync(predecessor, previous, appended, true)
    }

    /// Stages one append successor for a later shared Namespace metadata sync.
    ///
    /// # Errors
    ///
    /// Returns the same predecessor-tree, suffix-format, identity, arithmetic,
    /// or publication errors as [`Self::publish_manifest_append`].
    pub fn stage_manifest_append(
        &self,
        predecessor: SuccessorPredecessor,
        previous: ManifestTreeSummary,
        appended: &[ManifestExtent],
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        self.publish_manifest_append_with_sync(predecessor, previous, appended, false)
    }

    fn publish_manifest_append_with_sync(
        &self,
        predecessor: SuccessorPredecessor,
        previous: ManifestTreeSummary,
        appended: &[ManifestExtent],
        sync_root: bool,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        let (tree, summary) = append_manifest_tree(previous, appended, |node_id| {
            self.read_manifest_node(node_id)
        })?;
        let mut introduced_metadata = BTreeSet::new();
        for (expected_id, encoded) in tree.objects() {
            let staged = self.stage_metadata_with_status(encoded)?;
            assert_eq!(
                staged.object_id, *expected_id,
                "ASSERT: append-local Manifest plan identity must equal published object identity"
            );
            if staged.published_new {
                introduced_metadata.insert(staged.object_id);
            }
        }
        if sync_root {
            self.storage.sync_root()?;
        }
        Ok(ManifestSuccessorProof {
            predecessor,
            summary,
            introduced_chunks: manifest_dependencies(appended)?,
            introduced_metadata,
            metadata_root_pin: self.pin_metadata_root(summary.root()),
        })
    }

    /// Reuses one graph proof without introducing new DATA dependencies.
    ///
    /// # Panics
    ///
    /// Panics if a prior internal invariant panic poisoned the Metadata-GC
    /// publication barrier.
    #[must_use]
    pub fn reuse_manifest_successor(
        &self,
        predecessor: SuccessorPredecessor,
        summary: ManifestTreeSummary,
    ) -> ManifestSuccessorProof {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        ManifestSuccessorProof {
            predecessor,
            summary,
            introduced_chunks: BTreeMap::new(),
            introduced_metadata: BTreeSet::new(),
            metadata_root_pin: self.pin_metadata_root(summary.root()),
        }
    }

    /// Publishes one equal-length, path-local replacement and extends an
    /// opaque successor proof with the replacement's DATA dependencies.
    /// Successive calls must describe sorted, nonoverlapping edits of the same
    /// planned successor.
    ///
    /// # Errors
    ///
    /// Returns predecessor-tree, replacement-boundary, allocation, identity,
    /// dependency-conflict, or durable-publication errors.
    ///
    /// # Panics
    ///
    /// Panics if the deterministic replacement plan disagrees with the
    /// verified content-addressed Metadata publisher.
    pub fn publish_manifest_replacement_successor(
        &self,
        previous: ManifestSuccessorProof,
        replaced: Range<u64>,
        replacement: &[ManifestExtent],
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        self.publish_manifest_replacement_successor_with_sync(previous, replaced, replacement, true)
    }

    /// Stages one path-local replacement for a shared Namespace metadata sync.
    ///
    /// # Errors
    ///
    /// Returns the same predecessor-tree, boundary, allocation, identity,
    /// dependency, or publication errors as
    /// [`Self::publish_manifest_replacement_successor`].
    pub fn stage_manifest_replacement_successor(
        &self,
        previous: ManifestSuccessorProof,
        replaced: Range<u64>,
        replacement: &[ManifestExtent],
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        self.publish_manifest_replacement_successor_with_sync(
            previous,
            replaced,
            replacement,
            false,
        )
    }

    fn publish_manifest_replacement_successor_with_sync(
        &self,
        mut previous: ManifestSuccessorProof,
        replaced: Range<u64>,
        replacement: &[ManifestExtent],
        sync_root: bool,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        for (chunk_id, logical_length) in manifest_dependencies(replacement)? {
            if let Some(first_length) = previous.introduced_chunks.insert(chunk_id, logical_length)
                && first_length != logical_length
            {
                return Err(GenerationError::ManifestChunkLengthConflict {
                    chunk_id,
                    first_length,
                    second_length: logical_length,
                });
            }
        }
        let (tree, summary) = rewrite_manifest_tree_range_successor(
            previous.summary,
            replaced,
            replacement,
            |node_id| self.read_manifest_node(node_id),
        )?;
        for (expected_id, encoded) in tree.objects() {
            let staged = self.stage_metadata_with_status(encoded)?;
            assert_eq!(
                staged.object_id, *expected_id,
                "ASSERT: replacement-local Manifest plan identity must equal published object identity"
            );
            if staged.published_new {
                previous.introduced_metadata.insert(staged.object_id);
            }
        }
        if sync_root {
            self.storage.sync_root()?;
        }
        self.mark_successor_root_release_durable(&previous)?;
        previous.summary = summary;
        previous.metadata_root_pin = self.pin_metadata_root(summary.root());
        Ok(previous)
    }

    /// Publishes a length-decreasing successor by dropping complete right-hand
    /// subtrees and rewriting only the cutoff path.
    ///
    /// # Errors
    ///
    /// Returns predecessor-tree, missing v2 subtree summary, boundary,
    /// arithmetic, identity, or durable-publication errors.
    ///
    /// # Panics
    ///
    /// Panics if the deterministic truncate plan disagrees with the verified
    /// content-addressed Metadata publisher.
    pub fn publish_manifest_truncate_successor(
        &self,
        previous: ManifestSuccessorProof,
        logical_size: u64,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        self.publish_manifest_truncate_successor_with_sync(previous, logical_size, true)
    }

    /// Stages one truncate successor for a shared Namespace metadata sync.
    ///
    /// # Errors
    ///
    /// Returns the same predecessor-tree, subtree-summary, boundary,
    /// arithmetic, identity, or publication errors as
    /// [`Self::publish_manifest_truncate_successor`].
    pub fn stage_manifest_truncate_successor(
        &self,
        previous: ManifestSuccessorProof,
        logical_size: u64,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        self.publish_manifest_truncate_successor_with_sync(previous, logical_size, false)
    }

    fn publish_manifest_truncate_successor_with_sync(
        &self,
        mut previous: ManifestSuccessorProof,
        logical_size: u64,
        sync_root: bool,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        let (tree, summary) = truncate_manifest_tree(previous.summary, logical_size, |node_id| {
            self.read_manifest_node(node_id)
        })?;
        for (expected_id, encoded) in tree.objects() {
            let staged = self.stage_metadata_with_status(encoded)?;
            assert_eq!(
                staged.object_id, *expected_id,
                "ASSERT: truncate-local Manifest plan identity must equal published object identity"
            );
            if staged.published_new {
                previous.introduced_metadata.insert(staged.object_id);
            }
        }
        if sync_root {
            self.storage.sync_root()?;
        }
        self.mark_successor_root_release_durable(&previous)?;
        previous.summary = summary;
        previous.metadata_root_pin = self.pin_metadata_root(summary.root());
        Ok(previous)
    }

    /// Publishes an arbitrary length-changing middle splice from one verified
    /// Manifest-tree capability. Complete remote prefix and suffix subtrees
    /// retain their exact object identities even when the suffix moves to a
    /// different absolute file offset.
    ///
    /// This maintenance seam returns a new verified scalar capability but no
    /// DATA successor proof. Online Namespace commits use the proof-bearing
    /// variant below.
    ///
    /// # Errors
    ///
    /// Returns predecessor-tree, missing v2 summary, invalid slice,
    /// arithmetic, identity, or durable-publication errors.
    ///
    /// # Panics
    ///
    /// Panics if the deterministic splice plan disagrees with the verified
    /// content-addressed Metadata publisher.
    pub fn publish_manifest_splice(
        &self,
        previous: ManifestTreeSummary,
        replaced: Range<u64>,
        replacement: &[ManifestExtent],
    ) -> Result<ManifestTreeSummary, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        self.publish_manifest_splice_under_guard(previous, replaced, replacement)
            .map(|(summary, _introduced_metadata)| summary)
    }

    fn publish_manifest_splice_under_guard(
        &self,
        previous: ManifestTreeSummary,
        replaced: Range<u64>,
        replacement: &[ManifestExtent],
    ) -> Result<(ManifestTreeSummary, BTreeSet<MetadataObjectId>), GenerationError> {
        let (tree, summary) = splice_manifest_tree(previous, replaced, replacement, |node_id| {
            self.read_manifest_node(node_id)
        })?;
        let mut introduced_metadata = BTreeSet::new();
        for (expected_id, encoded) in tree.objects() {
            let staged = self.stage_metadata_with_status(encoded)?;
            assert_eq!(
                staged.object_id, *expected_id,
                "ASSERT: splice-local Manifest plan identity must equal published object identity"
            );
            if staged.published_new {
                introduced_metadata.insert(staged.object_id);
            }
        }
        self.storage.sync_root()?;
        Ok((summary, introduced_metadata))
    }

    /// Publishes a length-changing middle splice and extends the installed
    /// successor proof with only the replacement's newly introduced DATA.
    ///
    /// # Errors
    ///
    /// Returns predecessor-tree, replacement-boundary, allocation, identity,
    /// dependency-conflict, or durable-publication errors.
    ///
    /// # Panics
    ///
    /// Panics if the deterministic splice plan disagrees with the verified
    /// content-addressed Metadata publisher.
    pub fn publish_manifest_splice_successor(
        &self,
        mut previous: ManifestSuccessorProof,
        replaced: Range<u64>,
        replacement: &[ManifestExtent],
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        for (chunk_id, logical_length) in manifest_dependencies(replacement)? {
            if let Some(first_length) = previous.introduced_chunks.insert(chunk_id, logical_length)
                && first_length != logical_length
            {
                return Err(GenerationError::ManifestChunkLengthConflict {
                    chunk_id,
                    first_length,
                    second_length: logical_length,
                });
            }
        }
        let (summary, introduced_metadata) =
            self.publish_manifest_splice_under_guard(previous.summary, replaced, replacement)?;
        self.mark_successor_root_release_durable(&previous)?;
        previous.summary = summary;
        previous.introduced_metadata.extend(introduced_metadata);
        previous.metadata_root_pin = self.pin_metadata_root(previous.summary.root());
        Ok(previous)
    }

    fn mark_successor_root_release_durable(
        &self,
        proof: &ManifestSuccessorProof,
    ) -> Result<(), GenerationError> {
        let predecessor_root =
            self.read_namespace_root(proof.predecessor.record.namespace_root())?;
        if !record_matches_namespace_root(proof.predecessor.record, &predecessor_root) {
            return Err(GenerationError::PreviousGenerationRecordMismatch);
        }
        if predecessor_root
            .file_inodes()
            .any(|inode| inode.manifest_root() == proof.summary.root())
        {
            proof
                .metadata_root_pin
                .inner
                .release_requires_exact
                .store(false, Ordering::Release);
        }
        Ok(())
    }

    /// Transfers DATA dependencies from one Manifest range in the installed
    /// predecessor into a target successor proof without container I/O.
    ///
    /// The source root is accepted only when the predecessor Namespace Root
    /// names it. The complete intersecting Manifest recipe is reread and
    /// verified before matching dependencies are removed from the successor's
    /// introduced set.
    ///
    /// # Errors
    ///
    /// Returns a stale/foreign source root, invalid range, metadata integrity,
    /// or Chunk-length conflict.
    ///
    /// # Panics
    ///
    /// Panics only if a dependency proven present immediately disappears from
    /// the same private map, which marks an impossible internal mutation.
    pub fn retain_predecessor_manifest_range_successor(
        &self,
        mut successor: ManifestSuccessorProof,
        source_root: MetadataObjectId,
        source_range: Range<u64>,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let predecessor_root =
            self.read_namespace_root(successor.predecessor.record.namespace_root())?;
        let source_inode = predecessor_root
            .file_inodes()
            .find(|inode| inode.manifest_root() == source_root)
            .ok_or(GenerationError::RetainedManifestNotInPredecessor(
                source_root,
            ))?;
        if source_range.start > source_range.end || source_range.end > source_inode.logical_size() {
            return Err(GenerationError::RetainedManifestRangeInvalid {
                root: source_root,
                start: source_range.start,
                end: source_range.end,
                logical_size: source_inode.logical_size(),
            });
        }
        let extents = read_manifest_tree_range(
            source_root,
            source_inode.logical_size(),
            source_range.start,
            source_range.end - source_range.start,
            |node_id| self.read_manifest_node(node_id),
        )?;
        for located in extents {
            let (chunk_id, chunk_length) = match *located.extent() {
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                } => (chunk_id, logical_length),
                ManifestExtent::DataSlice {
                    chunk_id,
                    chunk_length,
                    ..
                } => (chunk_id, u64::from(chunk_length)),
                ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => continue,
            };
            if let Some(introduced_length) = successor.introduced_chunks.get(&chunk_id).copied() {
                if introduced_length != chunk_length {
                    return Err(GenerationError::ManifestChunkLengthConflict {
                        chunk_id,
                        first_length: introduced_length,
                        second_length: chunk_length,
                    });
                }
                let removed = successor.introduced_chunks.remove(&chunk_id);
                assert!(
                    removed.is_some(),
                    "ASSERT: retained predecessor dependency disappeared"
                );
            }
        }
        Ok(successor)
    }

    /// Publishes an equal-length immutable successor by replacing one logical
    /// range and rewriting only the intersecting leaves and their ancestors.
    /// Unchanged subtree object IDs are retained exactly.
    ///
    /// # Errors
    ///
    /// Returns format, predecessor-tree, replacement-boundary, identity, or
    /// durable-publication errors. A replacement boundary inside DATA is
    /// rejected because one DATA extent is the indivisible Chunk identity.
    ///
    /// # Panics
    ///
    /// Panics only if the path-local tree plan disagrees with the generic
    /// content-addressed Metadata Object writer.
    pub fn publish_manifest_replacement(
        &self,
        previous_root: MetadataObjectId,
        expected_logical_size: u64,
        replaced: Range<u64>,
        replacement: &[ManifestExtent],
    ) -> Result<MetadataObjectId, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        let tree = rewrite_manifest_tree_range(
            previous_root,
            expected_logical_size,
            replaced,
            replacement,
            |node_id| self.read_manifest_node(node_id),
        )?;
        for (expected_id, encoded) in tree.objects() {
            let published_id = self.stage_metadata(encoded)?;
            assert_eq!(
                published_id, *expected_id,
                "ASSERT: path-local Manifest plan identity must equal published object identity"
            );
        }
        self.storage.sync_root()?;
        Ok(tree.root())
    }

    /// Loads and fully verifies one immutable Manifest by Metadata Object ID.
    ///
    /// # Errors
    ///
    /// Returns bounded I/O, envelope-identity, or Manifest-format errors.
    pub fn read_manifest(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<ManifestLeaf, GenerationError> {
        flatten_manifest_tree(object_id, |node_id| {
            let name = metadata_name(node_id);
            let length = self.storage.object_len(&name)?;
            if length > MAX_METADATA_OBJECT_BYTES_U64 {
                return Err(ManifestTreeError::IdentityMismatch(node_id));
            }
            let bytes = self.storage.read(&name)?;
            if u64::try_from(bytes.len()) != Ok(length)
                || MetadataObjectId::from_encoded(&bytes)? != node_id
            {
                return Err(ManifestTreeError::IdentityMismatch(node_id));
            }
            Ok(bytes)
        })
        .map_err(Into::into)
    }

    /// Reads and verifies only Manifest tree paths intersecting one range.
    /// Returned extents retain their absolute logical offsets.
    ///
    /// # Errors
    ///
    /// Returns bounded I/O, identity, tree-partition, or arithmetic errors.
    pub fn read_manifest_range(
        &self,
        object_id: MetadataObjectId,
        expected_logical_size: u64,
        range: Range<u64>,
    ) -> Result<Vec<ManifestRangeExtent>, GenerationError> {
        let length = range
            .end
            .checked_sub(range.start)
            .ok_or(ManifestTreeError::InvalidReplacement)?;
        read_manifest_tree_range(
            object_id,
            expected_logical_size,
            range.start,
            length,
            |node_id| self.read_manifest_node(node_id),
        )
        .map_err(Into::into)
    }

    /// Performs a complete offline-style structural scrub of one Manifest
    /// tree, including every v2 subtree allocation summary.
    ///
    /// This verifies Metadata Objects and Manifest invariants only. DATA Chunk
    /// payload verification remains the responsibility of the complete
    /// generation scrub/recovery path.
    ///
    /// # Errors
    ///
    /// Returns missing-object, identity, format, partition, allocation-summary,
    /// arithmetic, or bounded-allocation failures.
    pub fn scrub_manifest_tree_metadata(
        &self,
        root: MetadataObjectId,
    ) -> Result<ManifestTreeSummary, GenerationError> {
        scan_manifest_tree(
            root,
            |node_id| self.read_manifest_node(node_id),
            |_offset, _extent| Ok(()),
        )
        .map_err(Into::into)
    }

    /// Exhaustively audits every generation retained by the selected bounded
    /// Generation-Log segment and every reachable Manifest/DATA dependency.
    ///
    /// Unlike mount recovery, scrub never falls back to an older generation
    /// and never accepts a torn or invalid tail. The inactive Log peer is also
    /// decoded and topology-checked by `GenerationLog` before this traversal.
    /// Historical Metadata Objects no longer reachable from the bounded Log
    /// are orphan/GC input rather than recovery evidence.
    ///
    /// # Errors
    ///
    /// Returns the first Log-tail, policy, Namespace, transition, Manifest,
    /// DATA, identity, I/O, allocation, or arithmetic failure.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn scrub_all_with_data<J: StorageIo>(
        &self,
        containers: &ContainerRepository<J>,
    ) -> Result<GenerationScrubSummary, GenerationError> {
        self.scrub_all_for_gc(containers).map(|proof| proof.summary)
    }

    pub(crate) fn scrub_all_for_gc<J: StorageIo>(
        &self,
        containers: &ContainerRepository<J>,
    ) -> Result<GenerationLivenessProof, GenerationError> {
        let proof = self.scan_generation_liveness(true)?;
        containers.verify_required_chunks(proof.online_chunks())?;
        Ok(proof)
    }

    /// Proves the current logical liveness set from Metadata only.
    ///
    /// This deliberately performs no DATA-Container scan. Online GC can use
    /// the opaque result to shortlist and locally verify a bounded victim set;
    /// the complete scrub path above additionally verifies every required
    /// Chunk before returning the same generation binding.
    pub(crate) fn scan_online_liveness(&self) -> Result<GenerationLivenessProof, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .write()
            .expect("ASSERT: Online-GC publication barrier poisoned");
        let _commit_guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: Online-GC liveness lock poisoned");
        let records = self.load_complete_commit_records_unlocked()?;
        let mut proof = self.scan_generation_liveness_from_records(&records, false)?;
        proof.pinned_roots = self
            .metadata_root_pins
            .lock()
            .expect("ASSERT: Metadata root pin registry poisoned during DATA proof")
            .keys()
            .copied()
            .collect();
        for root in proof.pinned_roots.iter().copied() {
            self.scan_manifest_root_required_chunks(root, &mut proof.online_chunks)?;
        }
        Ok(proof)
    }

    pub(crate) fn audit_metadata_mark_catalogs(&self) -> Result<u64, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned during catalog scrub");
        let mut names = Vec::new();
        let mut inventory_error = None;
        self.storage.visit_names(&mut |name| {
            if inventory_error.is_some() || !is_metadata_mark_catalog_name(name) {
                return;
            }
            let Some(generation) = parse_metadata_mark_generation(name) else {
                inventory_error = Some(GenerationError::MetadataMarkCatalogCorruption);
                return;
            };
            if names.try_reserve(1).is_err() {
                inventory_error = Some(GenerationError::OutOfMemory);
                return;
            }
            names.push((generation, name.to_owned()));
        })?;
        if let Some(error) = inventory_error {
            return Err(error);
        }
        names.sort_unstable_by_key(|entry| entry.0);
        let mut prior_generation = None;
        for (generation, name) in &names {
            let descriptor = audit_metadata_mark_catalog(&self.storage, name)?;
            if descriptor.generation() != *generation {
                return Err(GenerationError::MetadataMarkCatalogCorruption);
            }
            match descriptor.run_kind() {
                MetadataMarkCatalogRunKind::Snapshot => {}
                MetadataMarkCatalogRunKind::Addition
                    if descriptor.base_generation() == prior_generation.unwrap_or(0) => {}
                MetadataMarkCatalogRunKind::Addition => {
                    return Err(GenerationError::MetadataMarkCatalogCorruption);
                }
            }
            prior_generation = Some(*generation);
        }
        u64::try_from(names.len()).map_err(|_| GenerationError::MetadataTooLarge)
    }

    /// Removes fully verified Metadata Objects that are unreachable from every
    /// Commit Record retained by the selected bounded Generation Log and every
    /// live Metadata Root Pin. The exclusive publication barrier and Generation
    /// commit lock make the mark/delete batch safe during online checkpoints.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn garbage_collect_metadata(
        &self,
    ) -> Result<GenerationMetadataGcSummary, GenerationError> {
        let started = Instant::now();
        let _run_guard = self
            .metadata_gc_run_lock
            .lock()
            .expect("ASSERT: Metadata GC run lock poisoned");
        let mark_epoch = self.metadata_gc_epoch.load(Ordering::Acquire);
        let clean_state = *self
            .metadata_gc_clean
            .lock()
            .expect("ASSERT: Metadata GC clean-catalog state poisoned");
        if let Some(clean) = clean_state
            && clean.epoch == mark_epoch
        {
            return Ok(GenerationMetadataGcSummary {
                objects_removed: 0,
                bytes_removed: 0,
                objects_retained: clean.objects_retained,
                mark_mode: MetadataGcMarkMode::Reused,
                exact_reason: None,
                catalog_generation: Some(clean.catalog_generation),
                metrics: MetadataGcMetrics {
                    wall: started.elapsed(),
                    catalog_chain_runs: clean.delta_run_count + 1,
                    ..MetadataGcMetrics::default()
                },
            });
        }
        if let Some(summary) =
            self.try_publish_metadata_mark_delta(clean_state, mark_epoch, started)?
        {
            return Ok(summary);
        }
        let exact_reason = metadata_gc_exact_reason(clean_state, &self.metadata_gc_delta);
        let barrier_started = Instant::now();
        let _publication_guard = self
            .metadata_gc_barrier
            .write()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: Metadata GC generation lock poisoned");
        let barrier_wait = barrier_started.elapsed();
        let mark_epoch = self.metadata_gc_epoch.load(Ordering::Acquire);
        let records = self.load_complete_commit_records_unlocked()?;
        let commit_binding = metadata_mark_commit_binding(&records);
        let (reachable, object_graph_read_bytes) = self.mark_metadata_gc_roots(&records)?;
        let inventory = self.inventory_metadata_gc(&reachable)?;
        let bytes_removed = self.verify_metadata_gc_candidates(&inventory.candidates)?;
        let catalog_generation = inventory
            .catalog_generation_high_water
            .checked_add(1)
            .ok_or(GenerationError::GenerationExhausted)?;
        let prepared_catalog = prepare_metadata_mark_catalog(
            &self.storage,
            catalog_generation,
            commit_binding,
            reachable.iter().copied(),
            u64::try_from(reachable.len()).map_err(|_| GenerationError::MetadataTooLarge)?,
        )?;
        let published_catalog = prepared_catalog.publish(&self.storage)?;
        assert_eq!(
            published_catalog.generation(),
            catalog_generation,
            "ASSERT: prepared Metadata mark catalog publishes under its exact generation"
        );
        assert_eq!(
            published_catalog.row_count(),
            u64::try_from(reachable.len()).expect("ASSERT: reachable Metadata count fits u64"),
            "ASSERT: durable Metadata mark catalog covers the exact mark set"
        );
        for name in &inventory.catalog_names {
            self.storage.remove_file(name)?;
        }
        for (object_id, name) in &inventory.candidates {
            assert!(
                MetadataGcMarkMode::ExactSnapshot.has_deletion_authority(),
                "ASSERT: only an exact Metadata mark can authorize object unlink"
            );
            assert!(
                !reachable.contains(object_id),
                "ASSERT: Metadata GC cannot unlink an object in its verified reachability set"
            );
            self.storage.remove_file(name)?;
        }
        self.storage.sync_root()?;
        let summary = GenerationMetadataGcSummary {
            objects_removed: u64::try_from(inventory.candidates.len())
                .map_err(|_| GenerationError::MetadataTooLarge)?,
            bytes_removed,
            objects_retained: u64::try_from(reachable.len())
                .map_err(|_| GenerationError::MetadataTooLarge)?,
            mark_mode: MetadataGcMarkMode::ExactSnapshot,
            exact_reason: Some(exact_reason),
            catalog_generation: Some(catalog_generation),
            metrics: MetadataGcMetrics {
                wall: started.elapsed(),
                barrier_wait,
                object_graph_read_bytes,
                candidate_read_bytes: bytes_removed,
                catalog_read_bytes: published_catalog.file_length(),
                catalog_write_bytes: published_catalog.file_length(),
                unlinked_bytes: bytes_removed,
                root_syncs: 1,
                catalog_chain_runs: 1,
            },
        };
        if self.metadata_gc_epoch.load(Ordering::Acquire) == mark_epoch {
            *self
                .metadata_gc_delta
                .lock()
                .expect("ASSERT: Metadata GC delta journal poisoned after exact mark") =
                MetadataGcDeltaJournal::default();
            *self
                .metadata_gc_clean
                .lock()
                .expect("ASSERT: Metadata GC clean-catalog state poisoned") =
                Some(MetadataGcCleanState {
                    epoch: mark_epoch,
                    objects_retained: summary.objects_retained,
                    catalog_generation,
                    delta_run_count: 0,
                });
        }
        Ok(summary)
    }

    #[allow(clippy::too_many_lines)]
    fn try_publish_metadata_mark_delta(
        &self,
        clean_state: Option<MetadataGcCleanState>,
        mark_epoch: u64,
        started: Instant,
    ) -> Result<Option<GenerationMetadataGcSummary>, GenerationError> {
        let Some(clean) =
            clean_state.filter(|clean| clean.delta_run_count < MAX_METADATA_MARK_DELTA_RUNS)
        else {
            return Ok(None);
        };
        let delta_snapshot = {
            let journal = self
                .metadata_gc_delta
                .lock()
                .expect("ASSERT: Metadata GC delta journal poisoned during collection");
            (!journal.exact_required
                && journal.unclassified.is_empty()
                && !journal.additions.is_empty())
            .then(|| (journal.revision, journal.additions.clone()))
        };
        let Some((journal_revision, additions)) = delta_snapshot else {
            return Ok(None);
        };

        let records = self.load_complete_commit_records_unlocked()?;
        let catalog_generation = clean
            .catalog_generation
            .checked_add(1)
            .ok_or(GenerationError::GenerationExhausted)?;
        let row_count =
            u64::try_from(additions.len()).map_err(|_| GenerationError::MetadataTooLarge)?;
        let prepared = prepare_metadata_mark_addition(
            &self.storage,
            catalog_generation,
            clean.catalog_generation,
            metadata_mark_commit_binding(&records),
            additions.iter().copied(),
            row_count,
        )?;
        let published = prepared.publish(&self.storage)?;
        assert_eq!(
            published.generation(),
            catalog_generation,
            "ASSERT: Metadata mark delta publishes under its exact generation"
        );
        assert_eq!(
            published.base_generation(),
            clean.catalog_generation,
            "ASSERT: Metadata mark delta extends the installed catalog tail"
        );
        assert_eq!(
            published.row_count(),
            row_count,
            "ASSERT: Metadata mark delta covers every classified addition"
        );
        self.storage.sync_root()?;
        assert!(
            !MetadataGcMarkMode::AdditionDelta.has_deletion_authority(),
            "ASSERT: an additive Metadata catalog run never gains deletion authority"
        );

        let mut journal = self
            .metadata_gc_delta
            .lock()
            .expect("ASSERT: Metadata GC delta journal poisoned after publication");
        for object_id in &additions {
            assert!(
                journal.additions.remove(object_id),
                "ASSERT: published Metadata delta identity remains journaled"
            );
        }
        if journal.revision == journal_revision {
            assert!(
                journal.additions.is_empty(),
                "ASSERT: unchanged Metadata delta journal was published completely"
            );
        }
        drop(journal);

        let objects_retained = clean
            .objects_retained
            .checked_add(row_count)
            .ok_or(GenerationError::MetadataTooLarge)?;
        *self
            .metadata_gc_clean
            .lock()
            .expect("ASSERT: Metadata GC clean-catalog state poisoned") =
            Some(MetadataGcCleanState {
                epoch: mark_epoch,
                objects_retained,
                catalog_generation,
                delta_run_count: clean.delta_run_count + 1,
            });
        Ok(Some(GenerationMetadataGcSummary {
            objects_removed: 0,
            bytes_removed: 0,
            objects_retained,
            mark_mode: MetadataGcMarkMode::AdditionDelta,
            exact_reason: None,
            catalog_generation: Some(catalog_generation),
            metrics: MetadataGcMetrics {
                wall: started.elapsed(),
                catalog_read_bytes: published.file_length(),
                catalog_write_bytes: published.file_length(),
                root_syncs: 1,
                catalog_chain_runs: clean.delta_run_count + 2,
                ..MetadataGcMetrics::default()
            },
        }))
    }

    fn mark_metadata_gc_roots(
        &self,
        records: &[CommitRecord],
    ) -> Result<(BTreeSet<MetadataObjectId>, u64), GenerationError> {
        let mut reachable = BTreeSet::new();
        let mut bytes_read = 0_u64;
        for record in records {
            reachable.insert(record.namespace_root());
            let encoded_root = self.read_metadata(record.namespace_root())?;
            bytes_read = bytes_read
                .checked_add(
                    u64::try_from(encoded_root.len())
                        .map_err(|_| GenerationError::MetadataTooLarge)?,
                )
                .ok_or(GenerationError::MetadataTooLarge)?;
            let root = NamespaceRoot::decode(&encoded_root)?;
            if !record_matches_namespace_root(*record, &root) {
                return Err(GenerationError::PreviousGenerationRecordMismatch);
            }
            for inode in root.file_inodes() {
                scan_manifest_tree(
                    inode.manifest_root(),
                    |node_id| {
                        reachable.insert(node_id);
                        let bytes = self.read_manifest_node(node_id)?;
                        bytes_read = bytes_read
                            .checked_add(
                                u64::try_from(bytes.len())
                                    .map_err(|_| ManifestTreeError::ArithmeticOverflow)?,
                            )
                            .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                        Ok(bytes)
                    },
                    |_logical_offset, _extent| Ok(()),
                )?;
            }
        }
        let pinned_roots = self
            .metadata_root_pins
            .lock()
            .expect("ASSERT: Metadata root pin registry poisoned during GC proof")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for root in pinned_roots {
            if reachable.insert(root) {
                scan_manifest_tree(
                    root,
                    |node_id| {
                        reachable.insert(node_id);
                        let bytes = self.read_manifest_node(node_id)?;
                        bytes_read = bytes_read
                            .checked_add(
                                u64::try_from(bytes.len())
                                    .map_err(|_| ManifestTreeError::ArithmeticOverflow)?,
                            )
                            .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                        Ok(bytes)
                    },
                    |_logical_offset, _extent| Ok(()),
                )?;
            }
        }
        Ok((reachable, bytes_read))
    }

    fn inventory_metadata_gc(
        &self,
        reachable: &BTreeSet<MetadataObjectId>,
    ) -> Result<MetadataGcInventory, GenerationError> {
        let mut inventory = MetadataGcInventory {
            candidates: Vec::new(),
            catalog_names: Vec::new(),
            catalog_generation_high_water: 0,
        };
        let mut inventory_error = None;
        self.storage.visit_names(&mut |name| {
            if inventory_error.is_some() {
                return;
            }
            let result = inventory_metadata_name(&mut inventory, reachable, name);
            if let Err(error) = result {
                inventory_error = Some(error);
            }
        })?;
        if let Some(error) = inventory_error {
            return Err(error);
        }
        Ok(inventory)
    }

    fn verify_metadata_gc_candidates(
        &self,
        candidates: &[(MetadataObjectId, String)],
    ) -> Result<u64, GenerationError> {
        let mut bytes_removed = 0_u64;
        for (object_id, name) in candidates {
            let length = self.storage.object_len(name)?;
            if length > MAX_METADATA_OBJECT_BYTES_U64 {
                return Err(GenerationError::MetadataIdentityCollision(*object_id));
            }
            let bytes = self.storage.read(name)?;
            if u64::try_from(bytes.len()) != Ok(length)
                || MetadataObjectId::from_encoded(&bytes)? != *object_id
            {
                return Err(GenerationError::MetadataIdentityCollision(*object_id));
            }
            bytes_removed = bytes_removed
                .checked_add(length)
                .ok_or(GenerationError::MetadataTooLarge)?;
        }
        Ok(bytes_removed)
    }

    fn scan_generation_liveness(
        &self,
        audit_retained_history: bool,
    ) -> Result<GenerationLivenessProof, GenerationError> {
        let records = self.load_complete_commit_records()?;
        self.scan_generation_liveness_from_records(&records, audit_retained_history)
    }

    fn scan_generation_liveness_from_records(
        &self,
        records: &[CommitRecord],
        audit_retained_history: bool,
    ) -> Result<GenerationLivenessProof, GenerationError> {
        if records.is_empty() {
            return Ok(GenerationLivenessProof::default());
        }
        let mut latest_namespace_inodes = 0_usize;
        let mut latest_manifest_files = 0_usize;
        let mut online_chunks = BTreeMap::new();
        let first_online = records.len().saturating_sub(2);
        let scan_start = if audit_retained_history {
            0
        } else {
            first_online
        };
        for (ordinal, record) in records.iter().copied().enumerate().skip(scan_start) {
            let root = self.read_namespace_root(record.namespace_root())?;
            if !record_matches_namespace_root(record, &root) {
                return Err(GenerationError::PreviousGenerationRecordMismatch);
            }
            let (manifests, required) = self.scan_manifest_graph_with_required(&root)?;
            if ordinal >= first_online {
                for (chunk_id, logical_length) in required {
                    if let Some(previous) = online_chunks.insert(chunk_id, logical_length)
                        && previous != logical_length
                    {
                        return Err(GenerationError::ManifestChunkLengthConflict {
                            chunk_id,
                            first_length: previous,
                            second_length: logical_length,
                        });
                    }
                }
            }
            if ordinal + 1 == records.len() {
                latest_namespace_inodes = root.inodes().len();
                latest_manifest_files = manifests.len();
            }
        }
        let summary = GenerationScrubSummary {
            generations: records.len(),
            first_generation: records.first().copied().map(CommitRecord::generation),
            latest_generation: records.last().copied().map(CommitRecord::generation),
            latest_namespace_inodes,
            latest_manifest_files,
        };
        let online_records = records[first_online..].to_vec();
        Ok(GenerationLivenessProof {
            summary,
            online_records,
            online_chunks,
            pinned_roots: BTreeSet::new(),
        })
    }

    /// Computes the logical reachability changes between one previously
    /// incorporated Commit generation and the current protected online pair.
    ///
    /// This scans immutable Namespace/Manifest metadata only. The result is a
    /// non-authoritative catalog update input; it cannot authorize physical
    /// retirement or deletion.
    ///
    /// Passing `None` uses the empty set as the base and therefore emits one
    /// complete initial liveness population. A nonzero base must still be
    /// present in the bounded Commit WAL.
    ///
    /// # Errors
    ///
    /// Returns WAL, Namespace, Manifest, unavailable-base, length-conflict, or
    /// bounded-allocation failures.
    pub fn liveness_delta_since(
        &self,
        base_generation: Option<u64>,
    ) -> Result<GenerationLivenessDelta, GenerationError> {
        let records = self.load_complete_commit_records()?;
        let latest_generation = records.last().copied().map(CommitRecord::generation);
        if records.is_empty() {
            if base_generation.is_some() {
                return Err(GenerationError::LivenessDeltaBaseUnavailable {
                    requested: base_generation,
                    latest: None,
                });
            }
            return Ok(GenerationLivenessDelta::default());
        }
        let current_start = records.len().saturating_sub(2);
        let current_chunks = self.scan_protected_chunks(&records[current_start..])?;
        let base_chunks = match base_generation {
            None => BTreeMap::new(),
            Some(generation) => {
                let Some(end) = records
                    .iter()
                    .position(|record| record.generation() == generation)
                    .map(|ordinal| ordinal + 1)
                else {
                    return Err(GenerationError::LivenessDeltaBaseUnavailable {
                        requested: Some(generation),
                        latest: latest_generation,
                    });
                };
                let start = end.saturating_sub(2);
                self.scan_protected_chunks(&records[start..end])?
            }
        };
        let mut added = BTreeMap::new();
        let mut removed = BTreeMap::new();
        for (chunk_id, logical_length) in &current_chunks {
            match base_chunks.get(chunk_id) {
                None => {
                    added.insert(*chunk_id, *logical_length);
                }
                Some(previous_length) if previous_length != logical_length => {
                    return Err(GenerationError::ManifestChunkLengthConflict {
                        chunk_id: *chunk_id,
                        first_length: *previous_length,
                        second_length: *logical_length,
                    });
                }
                Some(_) => {}
            }
        }
        for (chunk_id, logical_length) in base_chunks {
            if !current_chunks.contains_key(&chunk_id) {
                removed.insert(chunk_id, logical_length);
            }
        }
        Ok(GenerationLivenessDelta {
            base_generation,
            latest_generation,
            added,
            removed,
            protected_chunk_count: current_chunks.len(),
        })
    }

    fn load_complete_commit_records(&self) -> Result<Vec<CommitRecord>, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation liveness lock poisoned");
        self.load_complete_commit_records_unlocked()
    }

    fn load_complete_commit_records_unlocked(&self) -> Result<Vec<CommitRecord>, GenerationError> {
        let Some(snapshot) = GenerationLog::new(&self.storage)
            .load_for_recovery()
            .map_err(map_log_error)?
        else {
            return Ok(Vec::new());
        };
        if snapshot.tail() != &WalTail::Clean {
            return Err(GenerationError::WalNeedsRepair(snapshot.tail().clone()));
        }
        let valid = self.validate_recovery_transition_prefix(snapshot.records())?;
        if valid.len() != snapshot.records().len() {
            return Err(GenerationError::NoRecoverableGeneration);
        }
        Ok(valid)
    }

    fn scan_protected_chunks(
        &self,
        records: &[CommitRecord],
    ) -> Result<BTreeMap<fastdup_format::ChunkId, u64>, GenerationError> {
        let mut chunks = BTreeMap::new();
        for record in records.iter().copied() {
            let root = self.read_namespace_root(record.namespace_root())?;
            if !record_matches_namespace_root(record, &root) {
                return Err(GenerationError::PreviousGenerationRecordMismatch);
            }
            let (_, required) = self.scan_manifest_graph_with_required(&root)?;
            for (chunk_id, logical_length) in required {
                if let Some(previous) = chunks.insert(chunk_id, logical_length)
                    && previous != logical_length
                {
                    return Err(GenerationError::ManifestChunkLengthConflict {
                        chunk_id,
                        first_length: previous,
                        second_length: logical_length,
                    });
                }
            }
        }
        Ok(chunks)
    }

    pub(crate) fn gc_proof_is_current(
        &self,
        proof: &GenerationLivenessProof,
    ) -> Result<bool, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .write()
            .expect("ASSERT: GC publication revalidation barrier poisoned");
        let _commit_guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: GC generation revalidation lock poisoned");
        self.gc_proof_is_current_unlocked(proof)
    }

    fn gc_proof_is_current_unlocked(
        &self,
        proof: &GenerationLivenessProof,
    ) -> Result<bool, GenerationError> {
        let pinned_roots = self
            .metadata_root_pins
            .lock()
            .expect("ASSERT: Metadata root pin registry poisoned during GC revalidation")
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        if pinned_roots != proof.pinned_roots {
            return Ok(false);
        }
        let Some(snapshot) = GenerationLog::new(&self.storage)
            .load_for_recovery()
            .map_err(map_log_error)?
        else {
            return Ok(proof.online_records.is_empty());
        };
        if snapshot.tail() != &WalTail::Clean {
            return Err(GenerationError::WalNeedsRepair(snapshot.tail().clone()));
        }
        let first_online = snapshot.records().len().saturating_sub(2);
        Ok(snapshot.records()[first_online..] == proof.online_records)
    }

    pub(crate) fn apply_if_gc_proof_current<T, E, F>(
        &self,
        proof: &GenerationLivenessProof,
        operation: F,
    ) -> Result<Option<T>, E>
    where
        E: From<GenerationError>,
        F: FnOnce() -> Result<T, E>,
    {
        let _publication_guard = self
            .metadata_gc_barrier
            .write()
            .expect("ASSERT: GC retirement publication barrier poisoned");
        let _commit_guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: GC retirement generation lock poisoned");
        if !self.gc_proof_is_current_unlocked(proof).map_err(E::from)? {
            return Ok(None);
        }
        operation().map(Some)
    }

    /// Publishes a complete Namespace Root and appends its Commit Record last.
    ///
    /// This checkpoint accepts only manifests made entirely of HOLE/FILL
    /// extents. DATA references are refused until verified Container locations
    /// are connected to generation recovery.
    ///
    /// # Errors
    ///
    /// Returns graph verification, publication, WAL-chain, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn commit_namespace(&self, root: &NamespaceRoot) -> Result<CommitRecord, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        self.verify_manifest_graph(root, None)?;
        self.commit_verified_namespace(root, None)
    }

    /// Publishes a Namespace Root after verifying every reachable DATA Chunk
    /// against one independently supplied durable Container Repository.
    ///
    /// # Errors
    ///
    /// Returns graph, container, publication, WAL-chain, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn commit_namespace_with_data<J: StorageIo>(
        &self,
        root: &NamespaceRoot,
        containers: &ContainerRepository<J>,
    ) -> Result<CommitRecord, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        self.verify_manifest_graph(root, Some(containers))?;
        self.commit_verified_namespace(root, None)
    }

    /// Commits a DATA-bearing Namespace Root and returns the Manifest readers
    /// proven by the same complete graph verification.
    ///
    /// The returned readers do not repeat dependency discovery. Demand reads
    /// still re-verify the selected immutable Container before returning data.
    /// Callers cannot construct this proof or attach an unrelated Manifest.
    ///
    /// # Errors
    ///
    /// Returns graph, container, bounded-allocation, publication, WAL-chain,
    /// or durability errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn commit_namespace_with_verified_files<J>(
        &self,
        root: &NamespaceRoot,
        containers: &ContainerRepository<J>,
    ) -> Result<CommittedDataGeneration<J>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let manifests = self.verify_manifest_graph(root, Some(containers))?;
        let files = verified_files(manifests, self, containers)?;
        let record = self.commit_verified_namespace(root, None)?;
        Ok(CommittedDataGeneration { record, files })
    }

    /// Commits a DATA-bearing Namespace Root using an independently supplied
    /// complete dependency verifier and returns readers backed by `containers`.
    ///
    /// This is the indexed counterpart of
    /// [`Self::commit_namespace_with_verified_files`]. The verifier may use
    /// bounded acceleration, but success must cover every required Chunk and a
    /// miss must fall back or fail closed.
    ///
    /// # Errors
    ///
    /// Returns graph, dependency-integrity, bounded-allocation, publication,
    /// WAL-chain, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn commit_namespace_with_verified_files_using<J>(
        &self,
        root: &NamespaceRoot,
        containers: &ContainerRepository<J>,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<CommittedDataGeneration<J>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let manifests = self.verify_manifest_graph(root, Some(verifier))?;
        let files = verified_files(manifests, self, containers)?;
        let record = self.commit_verified_namespace(root, None)?;
        Ok(CommittedDataGeneration { record, files })
    }

    /// Commits a Namespace successor from opaque Manifest proofs produced by
    /// this repository or retained from the installed verified generation.
    /// Only newly introduced DATA dependencies are sent to `verifier`; reused
    /// immutable subgraphs retain their predecessor proof.
    ///
    /// # Errors
    ///
    /// Returns a proof/root mismatch, dependency conflict, verification,
    /// transition, WAL, or durability error.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant poisoned the single-writer lock.
    pub fn commit_namespace_with_successor_proofs_using<J>(
        &self,
        root: &NamespaceRoot,
        containers: &ContainerRepository<J>,
        predecessor: SuccessorPredecessor,
        proofs: &[ManifestSuccessorProof],
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<CommittedDataGeneration<J>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let snapshot = self.load_append_snapshot(Some(predecessor))?;
        if proofs.len() != root.file_inode_count() {
            return Err(GenerationError::ManifestCountMismatch {
                namespace_inodes: root.file_inode_count(),
                manifests: proofs.len(),
            });
        }
        let mut introduced = BTreeMap::new();
        let mut manifests = Vec::new();
        manifests
            .try_reserve_exact(proofs.len())
            .map_err(|_| GenerationError::OutOfMemory)?;
        for (inode, proof) in root.file_inodes().zip(proofs) {
            if proof.predecessor != predecessor {
                return Err(GenerationError::MixedSuccessorPredecessors {
                    expected_generation: predecessor.generation(),
                    observed_generation: proof.predecessor.generation(),
                });
            }
            if proof.summary.root() != inode.manifest_root()
                || proof.summary.logical_size() != inode.logical_size()
            {
                return Err(GenerationError::ManifestLengthMismatch {
                    inode: inode.inode(),
                    inode_length: inode.logical_size(),
                    manifest_length: proof.summary.logical_size(),
                });
            }
            for (chunk_id, logical_length) in &proof.introduced_chunks {
                if let Some(previous) = introduced.insert(*chunk_id, *logical_length)
                    && previous != *logical_length
                {
                    return Err(GenerationError::ManifestChunkLengthConflict {
                        chunk_id: *chunk_id,
                        first_length: previous,
                        second_length: *logical_length,
                    });
                }
            }
            manifests.push((inode.inode(), proof.summary));
        }
        verifier.verify_required_chunks(&introduced)?;
        let files = verified_files(manifests, self, containers)?;
        let committed = self.commit_verified_namespace_from_snapshot_tracked(root, &snapshot)?;
        if committed.wal_rotated {
            self.mark_all_metadata_root_pin_releases_exact();
        }
        let mut introduced_metadata = BTreeSet::new();
        for proof in proofs {
            introduced_metadata.extend(proof.introduced_metadata.iter().copied());
        }
        if committed.namespace_root_published_new {
            introduced_metadata.insert(committed.record.namespace_root());
        }
        if committed.wal_rotated {
            mark_metadata_gc_exact_required(
                &self.metadata_gc_epoch,
                &self.metadata_gc_delta,
                MetadataGcExactReason::WalRotation,
            );
        } else {
            classify_metadata_gc_additions(
                &self.metadata_gc_epoch,
                &self.metadata_gc_delta,
                &introduced_metadata,
            );
        }
        let committed_manifest_roots = proofs
            .iter()
            .map(|proof| proof.summary.root())
            .collect::<BTreeSet<_>>();
        self.mark_metadata_root_releases_covered_by_commit(&committed_manifest_roots);
        Ok(CommittedDataGeneration {
            record: committed.record,
            files,
        })
    }

    fn commit_verified_namespace(
        &self,
        root: &NamespaceRoot,
        expected_predecessor: Option<SuccessorPredecessor>,
    ) -> Result<CommitRecord, GenerationError> {
        let snapshot = self.load_append_snapshot(expected_predecessor)?;
        self.commit_verified_namespace_from_snapshot(root, &snapshot)
    }

    fn load_append_snapshot(
        &self,
        expected_predecessor: Option<SuccessorPredecessor>,
    ) -> Result<LogSnapshot, GenerationError> {
        let snapshot = GenerationLog::new(&self.storage)
            .load_for_append()
            .map_err(map_log_error)?;
        if snapshot.tail() != &WalTail::Clean {
            return Err(GenerationError::WalNeedsRepair(snapshot.tail().clone()));
        }
        if let Some(expected) = expected_predecessor
            && snapshot.last_record() != Some(expected.record)
        {
            return Err(GenerationError::StaleSuccessorPredecessor {
                proof_generation: expected.generation(),
                installed_generation: snapshot.last_record().map(CommitRecord::generation),
            });
        }
        Ok(snapshot)
    }

    fn commit_verified_namespace_from_snapshot(
        &self,
        root: &NamespaceRoot,
        snapshot: &LogSnapshot,
    ) -> Result<CommitRecord, GenerationError> {
        let committed = self.commit_verified_namespace_from_snapshot_tracked(root, snapshot)?;
        mark_metadata_gc_exact_required(
            &self.metadata_gc_epoch,
            &self.metadata_gc_delta,
            MetadataGcExactReason::LegacyCommit,
        );
        Ok(committed.record)
    }

    fn commit_verified_namespace_from_snapshot_tracked(
        &self,
        root: &NamespaceRoot,
        snapshot: &LogSnapshot,
    ) -> Result<CommittedMetadata, GenerationError> {
        let encoded_root = root.encode()?;
        let staged_root = self.publish_metadata_with_status(&encoded_root)?;
        let root_id = staged_root.object_id;
        if let Some(previous) = snapshot.last_record() {
            self.verify_generation_transition(previous, root)?;
        } else if root.inode_allocation_cursor() != 2 || !root.inodes().is_empty() {
            return Err(GenerationError::InitialInodeReservationRequired);
        }
        let (generation, previous_hash) = match snapshot.last_record() {
            Some(previous) => (
                previous
                    .generation()
                    .checked_add(1)
                    .ok_or(GenerationError::GenerationExhausted)?,
                snapshot
                    .last_hash()
                    .expect("ASSERT: a last Commit Record has encoded bytes"),
            ),
            None => (1, CommitRecordHash::ZERO),
        };
        let record = CommitRecord::new(
            generation,
            previous_hash,
            root_id,
            self.supported_policy,
            root.namespace_mutation_sequence(),
            root.inode_reservation_end(),
            root.inode_allocation_cursor(),
        )?;
        let wal_rotated = snapshot.will_rotate();
        if wal_rotated {
            self.mark_all_metadata_root_pin_releases_exact();
        }
        // Invalidate a clean catalog before the WAL durability attempt. A
        // sync error may still have committed the exact record bytes.
        mark_metadata_gc_dirty(&self.metadata_gc_epoch);
        if let Err(error) = GenerationLog::new(&self.storage).append(snapshot, record) {
            mark_metadata_gc_exact_required(
                &self.metadata_gc_epoch,
                &self.metadata_gc_delta,
                MetadataGcExactReason::UncertainWalDurability,
            );
            return Err(map_log_error(error));
        }
        Ok(CommittedMetadata {
            record,
            namespace_root_published_new: staged_root.published_new,
            wal_rotated,
        })
    }

    /// Recovers the newest wholly verified generation supported by this writer.
    ///
    /// A torn or invalid WAL tail and an invalid newest metadata graph fall back
    /// to an earlier complete generation. No object fragments are merged.
    ///
    /// # Errors
    ///
    /// Returns I/O errors or `NoRecoverableGeneration` when a WAL exists but no
    /// supported record has a complete reachable graph.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn recover_latest(&self) -> Result<Option<RecoveredGeneration>, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        self.recover_latest_using(None)
            .map(|recovered| recovered.map(|graph| graph.generation))
    }

    /// Recovers the newest generation whose metadata graph and reachable DATA
    /// chunks are all independently verified.
    ///
    /// # Errors
    ///
    /// Returns I/O, format, container-integrity, or graph-completeness errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn recover_latest_with_data<J: StorageIo>(
        &self,
        containers: &ContainerRepository<J>,
    ) -> Result<Option<RecoveredGeneration>, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        self.recover_latest_using(Some(containers))
            .map(|recovered| recovered.map(|graph| graph.generation))
    }

    /// Recovers the newest complete DATA generation together with Manifest
    /// readers proven for that selected recovery candidate.
    ///
    /// # Errors
    ///
    /// Returns I/O, format, container-integrity, graph-completeness, or
    /// bounded-allocation errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn recover_latest_with_verified_files<J>(
        &self,
        containers: &ContainerRepository<J>,
    ) -> Result<Option<RecoveredDataGeneration<J>>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let Some(graph) = self.recover_latest_using(Some(containers))? else {
            return Ok(None);
        };
        let files = verified_files(graph.manifests, self, containers)?;
        Ok(Some(RecoveredDataGeneration {
            generation: graph.generation,
            files,
        }))
    }

    /// Recovers the newest complete DATA generation using an independently
    /// supplied complete dependency verifier.
    ///
    /// The verifier may use a pinned Exact Index, but it must fall back or fail
    /// closed when acceleration cannot prove one required Location. Returned
    /// Manifest readers retain the supplied Container Repository for demand
    /// verification.
    ///
    /// # Errors
    ///
    /// Returns I/O, format, dependency-integrity, graph-completeness, or
    /// bounded-allocation errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn recover_latest_with_verified_files_using<J>(
        &self,
        containers: &ContainerRepository<J>,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<Option<RecoveredDataGeneration<J>>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let Some(graph) = self.recover_latest_using(Some(verifier))? else {
            return Ok(None);
        };
        let files = verified_files(graph.manifests, self, containers)?;
        Ok(Some(RecoveredDataGeneration {
            generation: graph.generation,
            files,
        }))
    }

    fn recover_latest_using(
        &self,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<Option<RecoveredGraph>, GenerationError> {
        let Some(snapshot) = GenerationLog::new(&self.storage)
            .load_for_recovery()
            .map_err(map_log_error)?
        else {
            return Ok(None);
        };
        let latest_generation = match snapshot.records().last() {
            Some(record) => record.generation(),
            None => return Err(GenerationError::NoRecoverableGeneration),
        };
        let inode_reservation_end_high_water = snapshot
            .records()
            .iter()
            .map(|record| record.inode_reservation_end())
            .max()
            .ok_or(GenerationError::NoRecoverableGeneration)?;
        let structurally_valid_records =
            self.validate_recovery_transition_prefix(snapshot.records())?;
        let oldest_online_generation = latest_generation.saturating_sub(1);
        let selected = self
            .select_live_recovery_graph(
                &structurally_valid_records,
                oldest_online_generation,
                verifier,
            )?
            .ok_or(GenerationError::NoRecoverableGeneration)?;
        Ok(Some(RecoveredGraph {
            generation: RecoveredGeneration {
                record: selected.record,
                namespace_root: selected.root,
                wal_tail: snapshot.tail().clone(),
                rejected_newer_generations: latest_generation - selected.record.generation(),
                inode_reservation_end_high_water,
            },
            manifests: selected.manifests,
        }))
    }

    fn validate_recovery_transition_prefix(
        &self,
        records: &[CommitRecord],
    ) -> Result<Vec<CommitRecord>, GenerationError> {
        for record in records {
            if record.policy_set() != self.supported_policy {
                return Err(GenerationError::UnsupportedPolicySet {
                    generation: record.generation(),
                    policy_set: record.policy_set(),
                });
            }
        }
        let mut structurally_valid_records = Vec::new();
        structurally_valid_records
            .try_reserve_exact(records.len())
            .map_err(|_| GenerationError::OutOfMemory)?;
        let mut previous: Option<(CommitRecord, NamespaceRoot)> = None;
        for record in records {
            let root = match self.read_namespace_root(record.namespace_root()) {
                Ok(root) => root,
                Err(error) if error.allows_generation_fallback() => break,
                Err(error) => return Err(error),
            };
            if !record_matches_namespace_root(*record, &root) {
                break;
            }
            match &previous {
                Some((previous_record, previous_root)) => {
                    if verify_generation_transition_pair(*previous_record, previous_root, &root)
                        .is_err()
                    {
                        break;
                    }
                }
                None if record.generation() == 1
                    && (root.inode_allocation_cursor() != 2 || !root.inodes().is_empty()) =>
                {
                    break;
                }
                None => {}
            }
            structurally_valid_records.push(*record);
            previous = Some((*record, root));
        }
        Ok(structurally_valid_records)
    }

    fn select_live_recovery_graph(
        &self,
        structurally_valid_records: &[CommitRecord],
        oldest_online_generation: u64,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<Option<SelectedGraph>, GenerationError> {
        for record in structurally_valid_records
            .iter()
            .rev()
            .take_while(|record| record.generation() >= oldest_online_generation)
        {
            let root = match self.read_namespace_root(record.namespace_root()) {
                Ok(root) => root,
                Err(error) if error.allows_generation_fallback() => continue,
                Err(error) => return Err(error),
            };
            if !record_matches_namespace_root(*record, &root) {
                continue;
            }
            let manifests = match self.verify_manifest_graph(&root, verifier) {
                Ok(manifests) => manifests,
                Err(error) if error.allows_generation_fallback() => continue,
                Err(error) => return Err(error),
            };
            return Ok(Some(SelectedGraph {
                record: *record,
                root,
                manifests,
            }));
        }
        Ok(None)
    }

    fn publish_metadata_with_status(
        &self,
        encoded: &[u8],
    ) -> Result<StagedMetadata, GenerationError> {
        let staged = self.stage_metadata_with_status(encoded)?;
        self.storage.sync_root()?;
        Ok(staged)
    }

    fn stage_metadata(&self, encoded: &[u8]) -> Result<MetadataObjectId, GenerationError> {
        Ok(self.stage_metadata_with_status(encoded)?.object_id)
    }

    fn stage_metadata_with_status(
        &self,
        encoded: &[u8],
    ) -> Result<StagedMetadata, GenerationError> {
        if encoded.len() > MAX_METADATA_OBJECT_BYTES {
            return Err(GenerationError::MetadataTooLarge);
        }
        let object_id = MetadataObjectId::from_encoded(encoded)?;
        let published_name = metadata_name(object_id);
        if self.storage.exists(&published_name)? {
            let existing = self.storage.read(&published_name)?;
            let existing_id = MetadataObjectId::from_encoded(&existing)?;
            if existing_id != object_id || existing != encoded {
                return Err(GenerationError::MetadataIdentityCollision(object_id));
            }
            return Ok(StagedMetadata {
                object_id,
                published_new: false,
            });
        }

        let temporary_name = format!(".{}.building", encode_object_id(object_id));
        self.storage.create_new(&temporary_name)?;
        for (ordinal, block) in encoded.chunks(WRITE_BLOCK_BYTES).enumerate() {
            let offset = ordinal
                .checked_mul(WRITE_BLOCK_BYTES)
                .ok_or(GenerationError::MetadataTooLarge)?;
            self.storage.write_at(
                &temporary_name,
                u64::try_from(offset).map_err(|_| GenerationError::MetadataTooLarge)?,
                block,
            )?;
        }
        self.storage.set_len(
            &temporary_name,
            u64::try_from(encoded.len()).map_err(|_| GenerationError::MetadataTooLarge)?,
        )?;
        let reread = self.storage.read(&temporary_name)?;
        if reread != encoded || MetadataObjectId::from_encoded(&reread)? != object_id {
            return Err(GenerationError::PublishVerificationMismatch);
        }
        self.storage.sync_file(&temporary_name)?;
        self.storage
            .publish_noreplace(&temporary_name, &published_name)?;
        mark_metadata_gc_unclassified(&self.metadata_gc_epoch, &self.metadata_gc_delta, object_id);
        Ok(StagedMetadata {
            object_id,
            published_new: true,
        })
    }

    fn read_namespace_root(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<NamespaceRoot, GenerationError> {
        let bytes = self.read_metadata(object_id)?;
        NamespaceRoot::decode(&bytes).map_err(Into::into)
    }

    fn read_metadata(&self, object_id: MetadataObjectId) -> Result<Vec<u8>, GenerationError> {
        let name = metadata_name(object_id);
        let length = self.storage.object_len(&name)?;
        if length > MAX_METADATA_OBJECT_BYTES_U64 {
            return Err(GenerationError::MetadataIdentityCollision(object_id));
        }
        let bytes = self.storage.read(&name)?;
        if u64::try_from(bytes.len()) != Ok(length)
            || MetadataObjectId::from_encoded(&bytes)? != object_id
        {
            return Err(GenerationError::MetadataIdentityCollision(object_id));
        }
        Ok(bytes)
    }

    fn verify_manifest_graph(
        &self,
        root: &NamespaceRoot,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<VerifiedManifests, GenerationError> {
        self.verify_manifest_graph_with_required(root, verifier)
            .map(|(manifests, _required)| manifests)
    }

    fn verify_manifest_graph_with_required(
        &self,
        root: &NamespaceRoot,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<(VerifiedManifests, BTreeMap<fastdup_format::ChunkId, u64>), GenerationError> {
        let (manifests, required_chunks) = self.scan_manifest_graph_with_required(root)?;
        if required_chunks.is_empty() {
            return Ok((manifests, required_chunks));
        }
        let Some(verifier) = verifier else {
            return Err(GenerationError::DataLocationsNotConnected);
        };
        verifier.verify_required_chunks(&required_chunks)?;
        Ok((manifests, required_chunks))
    }

    fn scan_manifest_graph_with_required(
        &self,
        root: &NamespaceRoot,
    ) -> Result<(VerifiedManifests, BTreeMap<fastdup_format::ChunkId, u64>), GenerationError> {
        let mut required_chunks = BTreeMap::new();
        let mut chunk_length_conflict = None;
        let mut manifests = Vec::new();
        manifests
            .try_reserve_exact(root.file_inode_count())
            .map_err(|_| GenerationError::OutOfMemory)?;
        for inode in root.file_inodes() {
            let summary = scan_manifest_tree(
                inode.manifest_root(),
                |node_id| self.read_manifest_node(node_id),
                |_logical_offset, extent| {
                    let (chunk_id, logical_length) = match *extent {
                        ManifestExtent::Data {
                            logical_length,
                            chunk_id,
                        } => (chunk_id, logical_length),
                        ManifestExtent::DataSlice {
                            chunk_id,
                            chunk_length,
                            ..
                        } => (chunk_id, u64::from(chunk_length)),
                        ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => {
                            return Ok(());
                        }
                    };
                    if let Some(previous_length) = required_chunks.get(&chunk_id).copied() {
                        if previous_length != logical_length {
                            chunk_length_conflict =
                                Some((chunk_id, previous_length, logical_length));
                        }
                    } else {
                        required_chunks.insert(chunk_id, logical_length);
                    }
                    Ok(())
                },
            )?;
            if let Some((chunk_id, first_length, second_length)) = chunk_length_conflict.take() {
                return Err(GenerationError::ManifestChunkLengthConflict {
                    chunk_id,
                    first_length,
                    second_length,
                });
            }
            if summary.logical_size() != inode.logical_size() {
                return Err(GenerationError::ManifestLengthMismatch {
                    inode: inode.inode(),
                    inode_length: inode.logical_size(),
                    manifest_length: summary.logical_size(),
                });
            }
            manifests.push((inode.inode(), summary));
        }
        Ok((manifests, required_chunks))
    }

    fn scan_manifest_root_required_chunks(
        &self,
        root: MetadataObjectId,
        required_chunks: &mut BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), GenerationError> {
        let mut chunk_length_conflict = None;
        scan_manifest_tree(
            root,
            |node_id| self.read_manifest_node(node_id),
            |_logical_offset, extent| {
                let (chunk_id, logical_length) = match *extent {
                    ManifestExtent::Data {
                        logical_length,
                        chunk_id,
                    } => (chunk_id, logical_length),
                    ManifestExtent::DataSlice {
                        chunk_id,
                        chunk_length,
                        ..
                    } => (chunk_id, u64::from(chunk_length)),
                    ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => return Ok(()),
                };
                if let Some(previous_length) = required_chunks.get(&chunk_id).copied() {
                    if previous_length != logical_length {
                        chunk_length_conflict = Some((chunk_id, previous_length, logical_length));
                    }
                } else {
                    required_chunks.insert(chunk_id, logical_length);
                }
                Ok(())
            },
        )?;
        if let Some((chunk_id, first_length, second_length)) = chunk_length_conflict {
            return Err(GenerationError::ManifestChunkLengthConflict {
                chunk_id,
                first_length,
                second_length,
            });
        }
        Ok(())
    }

    fn read_manifest_node(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<Vec<u8>, ManifestTreeError> {
        let name = metadata_name(object_id);
        let length = self.storage.object_len(&name)?;
        if length > MAX_METADATA_OBJECT_BYTES_U64 {
            return Err(ManifestTreeError::IdentityMismatch(object_id));
        }
        let bytes = self.storage.read(&name)?;
        if u64::try_from(bytes.len()) != Ok(length)
            || MetadataObjectId::from_encoded(&bytes)? != object_id
        {
            return Err(ManifestTreeError::IdentityMismatch(object_id));
        }
        Ok(bytes)
    }

    fn verify_generation_transition(
        &self,
        previous_record: CommitRecord,
        proposed_root: &NamespaceRoot,
    ) -> Result<(), GenerationError> {
        let previous_root = self.read_namespace_root(previous_record.namespace_root())?;
        if previous_root.namespace_mutation_sequence()
            != previous_record.namespace_mutation_cutoff()
            || previous_root.inode_reservation_end() != previous_record.inode_reservation_end()
            || previous_root.inode_allocation_cursor() != previous_record.inode_allocation_cursor()
        {
            return Err(GenerationError::PreviousGenerationRecordMismatch);
        }
        verify_generation_transition_pair(previous_record, &previous_root, proposed_root)
    }
}

fn record_matches_namespace_root(record: CommitRecord, root: &NamespaceRoot) -> bool {
    root.namespace_mutation_sequence() == record.namespace_mutation_cutoff()
        && root.inode_reservation_end() == record.inode_reservation_end()
        && root.inode_allocation_cursor() == record.inode_allocation_cursor()
}

fn verify_generation_transition_pair(
    previous_record: CommitRecord,
    previous_root: &NamespaceRoot,
    proposed_root: &NamespaceRoot,
) -> Result<(), GenerationError> {
    if proposed_root.namespace_mutation_sequence() < previous_record.namespace_mutation_cutoff() {
        return Err(GenerationError::NonMonotonicNamespaceMutation {
            previous: previous_record.namespace_mutation_cutoff(),
            proposed: proposed_root.namespace_mutation_sequence(),
        });
    }
    if proposed_root.inode_reservation_end() < previous_record.inode_reservation_end() {
        return Err(GenerationError::NonMonotonicInodeReservation {
            previous: previous_record.inode_reservation_end(),
            proposed: proposed_root.inode_reservation_end(),
        });
    }
    if proposed_root.inode_allocation_cursor() < previous_record.inode_allocation_cursor() {
        return Err(GenerationError::NonMonotonicInodeAllocation {
            previous: previous_record.inode_allocation_cursor(),
            proposed: proposed_root.inode_allocation_cursor(),
        });
    }
    if proposed_root.inode_allocation_cursor() > previous_record.inode_reservation_end() {
        return Err(
            GenerationError::AllocationExceededPreviouslyDurableReservation {
                previous_reservation_end: previous_record.inode_reservation_end(),
                proposed_allocation_cursor: proposed_root.inode_allocation_cursor(),
            },
        );
    }
    for proposed_inode in proposed_root.inodes() {
        match previous_root
            .inodes()
            .binary_search_by_key(&proposed_inode.inode(), fastdup_format::DurableInode::inode)
        {
            Ok(previous_index) => {
                let previous_inode = &previous_root.inodes()[previous_index];
                if proposed_inode.mutation_sequence() < previous_inode.mutation_sequence() {
                    return Err(GenerationError::NonMonotonicInodeMutation {
                        inode: proposed_inode.inode(),
                        previous: previous_inode.mutation_sequence(),
                        proposed: proposed_inode.mutation_sequence(),
                    });
                }
            }
            Err(_) if proposed_inode.inode() < previous_record.inode_allocation_cursor() => {
                return Err(GenerationError::ReusedInodeId {
                    inode: proposed_inode.inode(),
                    previous_allocation_cursor: previous_record.inode_allocation_cursor(),
                });
            }
            Err(_) => {}
        }
    }
    Ok(())
}

/// Payload-free evidence from an exhaustive bounded Generation-Log scrub.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GenerationScrubSummary {
    generations: usize,
    first_generation: Option<u64>,
    latest_generation: Option<u64>,
    latest_namespace_inodes: usize,
    latest_manifest_files: usize,
}

/// How one Metadata-GC quantum established its retained-object catalog view.
///
/// Only `ExactSnapshot` has deletion authority. Reuse and additive deltas are
/// acceleration states and cannot authorize an unlink.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataGcMarkMode {
    #[default]
    Reused,
    AdditionDelta,
    ExactSnapshot,
}

/// Why Metadata GC had to rebuild exact deletion authority instead of reusing
/// or extending the process-local clean catalog state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataGcExactReason {
    ProcessStart,
    UnclassifiedPublication,
    MetadataRootPinDrain,
    WalRotation,
    LegacyCommit,
    UncertainWalDurability,
    DeltaChainLimit,
}

impl MetadataGcExactReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProcessStart => "process_start",
            Self::UnclassifiedPublication => "unclassified_publication",
            Self::MetadataRootPinDrain => "metadata_root_pin_drain",
            Self::WalRotation => "wal_rotation",
            Self::LegacyCommit => "legacy_commit",
            Self::UncertainWalDurability => "uncertain_wal_durability",
            Self::DeltaChainLimit => "delta_chain_limit",
        }
    }
}

/// Per-quantum Metadata-GC work visible at the maintenance seam.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataGcMetrics {
    wall: Duration,
    barrier_wait: Duration,
    object_graph_read_bytes: u64,
    candidate_read_bytes: u64,
    catalog_read_bytes: u64,
    catalog_write_bytes: u64,
    unlinked_bytes: u64,
    root_syncs: u64,
    catalog_chain_runs: u32,
}

impl MetadataGcMetrics {
    #[must_use]
    pub const fn wall(self) -> Duration {
        self.wall
    }

    #[must_use]
    pub const fn barrier_wait(self) -> Duration {
        self.barrier_wait
    }

    #[must_use]
    pub const fn object_graph_read_bytes(self) -> u64 {
        self.object_graph_read_bytes
    }

    #[must_use]
    pub const fn candidate_read_bytes(self) -> u64 {
        self.candidate_read_bytes
    }

    #[must_use]
    pub const fn catalog_read_bytes(self) -> u64 {
        self.catalog_read_bytes
    }

    #[must_use]
    pub const fn catalog_write_bytes(self) -> u64 {
        self.catalog_write_bytes
    }

    #[must_use]
    pub const fn unlinked_bytes(self) -> u64 {
        self.unlinked_bytes
    }

    #[must_use]
    pub const fn root_syncs(self) -> u64 {
        self.root_syncs
    }

    #[must_use]
    pub const fn catalog_chain_runs(self) -> u32 {
        self.catalog_chain_runs
    }
}

impl MetadataGcMarkMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reused => "reused",
            Self::AdditionDelta => "addition_delta",
            Self::ExactSnapshot => "exact_snapshot",
        }
    }

    #[must_use]
    pub const fn has_deletion_authority(self) -> bool {
        matches!(self, Self::ExactSnapshot)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GenerationMetadataGcSummary {
    objects_removed: u64,
    bytes_removed: u64,
    objects_retained: u64,
    mark_mode: MetadataGcMarkMode,
    exact_reason: Option<MetadataGcExactReason>,
    catalog_generation: Option<u64>,
    metrics: MetadataGcMetrics,
}

impl GenerationMetadataGcSummary {
    pub(crate) const fn objects_removed(self) -> u64 {
        self.objects_removed
    }

    pub(crate) const fn bytes_removed(self) -> u64 {
        self.bytes_removed
    }

    pub(crate) const fn objects_retained(self) -> u64 {
        self.objects_retained
    }

    pub(crate) const fn mark_mode(self) -> MetadataGcMarkMode {
        self.mark_mode
    }

    pub(crate) const fn exact_reason(self) -> Option<MetadataGcExactReason> {
        self.exact_reason
    }

    pub(crate) const fn catalog_generation(self) -> Option<u64> {
        self.catalog_generation
    }

    pub(crate) const fn metrics(self) -> MetadataGcMetrics {
        self.metrics
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GenerationLivenessProof {
    summary: GenerationScrubSummary,
    online_records: Vec<CommitRecord>,
    online_chunks: BTreeMap<fastdup_format::ChunkId, u64>,
    pinned_roots: BTreeSet<MetadataObjectId>,
}

/// Metadata-only reachability changes for the current and previous protected
/// Commit generations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenerationLivenessDelta {
    base_generation: Option<u64>,
    latest_generation: Option<u64>,
    added: BTreeMap<fastdup_format::ChunkId, u64>,
    removed: BTreeMap<fastdup_format::ChunkId, u64>,
    protected_chunk_count: usize,
}

impl GenerationLivenessDelta {
    #[must_use]
    pub const fn base_generation(&self) -> Option<u64> {
        self.base_generation
    }

    #[must_use]
    pub const fn latest_generation(&self) -> Option<u64> {
        self.latest_generation
    }

    #[must_use]
    pub fn added(&self) -> &BTreeMap<fastdup_format::ChunkId, u64> {
        &self.added
    }

    #[must_use]
    pub fn removed(&self) -> &BTreeMap<fastdup_format::ChunkId, u64> {
        &self.removed
    }

    #[must_use]
    pub const fn protected_chunk_count(&self) -> usize {
        self.protected_chunk_count
    }
}

impl GenerationLivenessProof {
    pub(crate) const fn summary(&self) -> GenerationScrubSummary {
        self.summary
    }

    pub(crate) fn online_chunks(&self) -> &BTreeMap<fastdup_format::ChunkId, u64> {
        &self.online_chunks
    }
}

impl GenerationScrubSummary {
    #[must_use]
    pub const fn generations(self) -> usize {
        self.generations
    }

    #[must_use]
    pub const fn first_generation(self) -> Option<u64> {
        self.first_generation
    }

    #[must_use]
    pub const fn latest_generation(self) -> Option<u64> {
        self.latest_generation
    }

    #[must_use]
    pub const fn latest_namespace_inodes(self) -> usize {
        self.latest_namespace_inodes
    }

    #[must_use]
    pub const fn latest_manifest_files(self) -> usize {
        self.latest_manifest_files
    }
}

pub use crate::generation_log::LogTail as WalTail;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredGeneration {
    record: CommitRecord,
    namespace_root: NamespaceRoot,
    wal_tail: WalTail,
    rejected_newer_generations: u64,
    inode_reservation_end_high_water: u64,
}

/// One committed DATA generation and the Manifest readers proven by its graph
/// verification.
#[derive(Debug)]
pub struct CommittedDataGeneration<I> {
    record: CommitRecord,
    files: Vec<VerifiedCommittedFile<I>>,
}

/// One recovered DATA generation and the Manifest readers proven for that same
/// selected recovery candidate.
#[derive(Debug)]
pub struct RecoveredDataGeneration<I> {
    generation: RecoveredGeneration,
    files: Vec<VerifiedCommittedFile<I>>,
}

impl<I> RecoveredDataGeneration<I> {
    #[must_use]
    pub const fn generation(&self) -> &RecoveredGeneration {
        &self.generation
    }

    #[must_use]
    pub fn into_parts(self) -> (RecoveredGeneration, Vec<VerifiedCommittedFile<I>>) {
        (self.generation, self.files)
    }
}

impl<I> CommittedDataGeneration<I> {
    #[must_use]
    pub const fn record(&self) -> CommitRecord {
        self.record
    }

    #[must_use]
    pub fn into_parts(self) -> (CommitRecord, Vec<VerifiedCommittedFile<I>>) {
        (self.record, self.files)
    }
}

/// One inode-associated Manifest reader that can only originate from a
/// complete committed DATA-graph verification.
#[derive(Debug)]
pub struct VerifiedCommittedFile<I> {
    inode: u64,
    file: VerifiedManifestFile<I>,
}

impl<I: StorageIo> VerifiedCommittedFile<I> {
    #[must_use]
    pub const fn inode(&self) -> u64 {
        self.inode
    }

    #[must_use]
    pub fn manifest_root(&self) -> Option<MetadataObjectId> {
        self.file.manifest_root()
    }

    #[must_use]
    pub fn logical_size(&self) -> u64 {
        self.file.logical_size()
    }

    #[must_use]
    pub fn allocated_bytes(&self) -> u64 {
        self.file.allocated_bytes()
    }

    /// Returns the opaque graph summary established by complete verification
    /// or a verified successor transition.
    #[must_use]
    pub fn manifest_summary(&self) -> Option<ManifestTreeSummary> {
        self.file.manifest_root().map(|root| {
            ManifestTreeSummary::new(root, self.file.logical_size(), self.file.allocated_bytes())
        })
    }

    #[must_use]
    pub fn into_file(self) -> VerifiedManifestFile<I> {
        self.file
    }
}

fn verified_files<M, I>(
    manifests: Vec<(u64, ManifestTreeSummary)>,
    generations: &GenerationRepository<M>,
    containers: &ContainerRepository<I>,
) -> Result<Vec<VerifiedCommittedFile<I>>, GenerationError>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    I: Clone + StorageIo,
{
    let mut files = Vec::new();
    files
        .try_reserve_exact(manifests.len())
        .map_err(|_| GenerationError::OutOfMemory)?;
    for (inode, summary) in manifests {
        files.push(VerifiedCommittedFile {
            inode,
            file: VerifiedManifestFile::from_verified_tree(
                summary,
                generations.storage.clone(),
                containers.clone(),
                generations.pin_metadata_root(summary.root()),
            ),
        });
    }
    Ok(files)
}

fn manifest_allocated_bytes(extents: &[ManifestExtent]) -> Result<u64, GenerationError> {
    extents.iter().try_fold(0_u64, |total, extent| {
        let length = if matches!(extent, ManifestExtent::Hole { .. }) {
            0
        } else {
            manifest_extent_length(extent)
        };
        total
            .checked_add(length)
            .ok_or(GenerationError::ManifestTree(
                ManifestTreeError::ArithmeticOverflow,
            ))
    })
}

fn manifest_dependencies(
    extents: &[ManifestExtent],
) -> Result<BTreeMap<fastdup_format::ChunkId, u64>, GenerationError> {
    let mut dependencies = BTreeMap::new();
    for extent in extents {
        let (chunk_id, logical_length) = match *extent {
            ManifestExtent::Data {
                logical_length,
                chunk_id,
            } => (chunk_id, logical_length),
            ManifestExtent::DataSlice {
                chunk_id,
                chunk_length,
                ..
            } => (chunk_id, u64::from(chunk_length)),
            ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => continue,
        };
        if let Some(previous) = dependencies.insert(chunk_id, logical_length)
            && previous != logical_length
        {
            return Err(GenerationError::ManifestChunkLengthConflict {
                chunk_id,
                first_length: previous,
                second_length: logical_length,
            });
        }
    }
    Ok(dependencies)
}

const fn manifest_extent_length(extent: &ManifestExtent) -> u64 {
    match extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::DataSlice { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => *logical_length,
    }
}

impl RecoveredGeneration {
    #[must_use]
    pub const fn record(&self) -> CommitRecord {
        self.record
    }

    #[must_use]
    pub const fn namespace_root(&self) -> &NamespaceRoot {
        &self.namespace_root
    }

    #[must_use]
    pub const fn wal_tail(&self) -> &WalTail {
        &self.wal_tail
    }

    #[must_use]
    pub const fn rejected_newer_generations(&self) -> u64 {
        self.rejected_newer_generations
    }

    /// Returns the newest reservation carried by the valid WAL prefix, even
    /// when recovery selected an older Namespace Root after corruption.
    #[must_use]
    pub const fn inode_reservation_end_high_water(&self) -> u64 {
        self.inode_reservation_end_high_water
    }
}

#[derive(Debug)]
pub enum GenerationError {
    Io(io::Error),
    MetadataFormat(MetadataFormatError),
    ManifestTree(ManifestTreeError),
    CommitFormat(CommitFormatError),
    Store(StoreError),
    MetadataTooLarge,
    WalTooLarge,
    GenerationExhausted,
    UnsupportedPolicySet {
        generation: u64,
        policy_set: PolicySetId,
    },
    NonMonotonicNamespaceMutation {
        previous: u64,
        proposed: u64,
    },
    NonMonotonicInodeReservation {
        previous: u64,
        proposed: u64,
    },
    NonMonotonicInodeAllocation {
        previous: u64,
        proposed: u64,
    },
    NonMonotonicInodeMutation {
        inode: u64,
        previous: u64,
        proposed: u64,
    },
    PreviousGenerationRecordMismatch,
    InitialInodeReservationRequired,
    AllocationExceededPreviouslyDurableReservation {
        previous_reservation_end: u64,
        proposed_allocation_cursor: u64,
    },
    ReusedInodeId {
        inode: u64,
        previous_allocation_cursor: u64,
    },
    PublishVerificationMismatch,
    MetadataIdentityCollision(MetadataObjectId),
    WalNeedsRepair(WalTail),
    NoRecoverableGeneration,
    DataLocationsNotConnected,
    OutOfMemory,
    ManifestCountMismatch {
        namespace_inodes: usize,
        manifests: usize,
    },
    StaleSuccessorPredecessor {
        proof_generation: u64,
        installed_generation: Option<u64>,
    },
    MixedSuccessorPredecessors {
        expected_generation: u64,
        observed_generation: u64,
    },
    ManifestLengthMismatch {
        inode: u64,
        inode_length: u64,
        manifest_length: u64,
    },
    ManifestChunkLengthConflict {
        chunk_id: fastdup_format::ChunkId,
        first_length: u64,
        second_length: u64,
    },
    LivenessDeltaBaseUnavailable {
        requested: Option<u64>,
        latest: Option<u64>,
    },
    RetainedManifestNotInPredecessor(MetadataObjectId),
    RetainedManifestRangeInvalid {
        root: MetadataObjectId,
        start: u64,
        end: u64,
        logical_size: u64,
    },
    InvalidMetadataObjectName(String),
    MetadataMarkCatalogCorruption,
}

impl GenerationError {
    fn allows_generation_fallback(&self) -> bool {
        match self {
            Self::Io(error)
            | Self::Store(StoreError::Io(error))
            | Self::ManifestTree(ManifestTreeError::Io(error)) => {
                error.kind() == io::ErrorKind::NotFound
            }
            Self::MetadataFormat(_)
            | Self::ManifestTree(
                ManifestTreeError::Metadata(_)
                | ManifestTreeError::Inner(
                    fastdup_format::ManifestInnerNodeError::Metadata(_)
                    | fastdup_format::ManifestInnerNodeError::InvalidLevel
                    | fastdup_format::ManifestInnerNodeError::InvalidChildRange
                    | fastdup_format::ManifestInnerNodeError::InvalidPartition
                    | fastdup_format::ManifestInnerNodeError::InvalidPayload
                    | fastdup_format::ManifestInnerNodeError::ArithmeticOverflow,
                )
                | ManifestTreeError::IdentityMismatch(_)
                | ManifestTreeError::InvalidTree
                | ManifestTreeError::MissingSubtreeAllocation
                | ManifestTreeError::ArithmeticOverflow,
            )
            | Self::MetadataIdentityCollision(_)
            | Self::ManifestLengthMismatch { .. }
            | Self::ManifestChunkLengthConflict { .. }
            | Self::LivenessDeltaBaseUnavailable { .. }
            | Self::Store(
                StoreError::Format(_)
                | StoreError::InvalidPublishedName(_)
                | StoreError::PublishedIdentityMismatch { .. }
                | StoreError::MissingVerifiedChunk { .. }
                | StoreError::ExactLocationMismatch,
            ) => true,
            Self::CommitFormat(_)
            | Self::Store(StoreError::PublishVerificationMismatch)
            | Self::MetadataTooLarge
            | Self::WalTooLarge
            | Self::GenerationExhausted
            | Self::UnsupportedPolicySet { .. }
            | Self::NonMonotonicNamespaceMutation { .. }
            | Self::NonMonotonicInodeReservation { .. }
            | Self::NonMonotonicInodeAllocation { .. }
            | Self::NonMonotonicInodeMutation { .. }
            | Self::PreviousGenerationRecordMismatch
            | Self::InitialInodeReservationRequired
            | Self::AllocationExceededPreviouslyDurableReservation { .. }
            | Self::ReusedInodeId { .. }
            | Self::PublishVerificationMismatch
            | Self::WalNeedsRepair(_)
            | Self::NoRecoverableGeneration
            | Self::DataLocationsNotConnected
            | Self::ManifestCountMismatch { .. }
            | Self::StaleSuccessorPredecessor { .. }
            | Self::MixedSuccessorPredecessors { .. }
            | Self::RetainedManifestNotInPredecessor(_)
            | Self::RetainedManifestRangeInvalid { .. }
            | Self::ManifestTree(
                ManifestTreeError::TreeTooDeep
                | ManifestTreeError::TreeTooLarge
                | ManifestTreeError::InvalidReplacement
                | ManifestTreeError::OutOfMemory
                | ManifestTreeError::Inner(fastdup_format::ManifestInnerNodeError::OutOfMemory),
            )
            | Self::OutOfMemory
            | Self::InvalidMetadataObjectName(_)
            | Self::MetadataMarkCatalogCorruption => false,
        }
    }
}

impl fmt::Display for GenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for GenerationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::MetadataFormat(error) => Some(error),
            Self::ManifestTree(error) => Some(error),
            Self::CommitFormat(error) => Some(error),
            Self::Store(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for GenerationError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<MetadataMarkCatalogError> for GenerationError {
    fn from(error: MetadataMarkCatalogError) -> Self {
        match error {
            MetadataMarkCatalogError::Io(error) => Self::Io(error),
            MetadataMarkCatalogError::Format(
                fastdup_format::MetadataMarkCatalogError::OutOfMemory,
            ) => Self::OutOfMemory,
            MetadataMarkCatalogError::Format(
                fastdup_format::MetadataMarkCatalogError::ArithmeticOverflow,
            ) => Self::MetadataTooLarge,
            MetadataMarkCatalogError::Format(_) => Self::MetadataMarkCatalogCorruption,
        }
    }
}

impl From<MetadataFormatError> for GenerationError {
    fn from(error: MetadataFormatError) -> Self {
        Self::MetadataFormat(error)
    }
}

impl From<ManifestTreeError> for GenerationError {
    fn from(error: ManifestTreeError) -> Self {
        Self::ManifestTree(error)
    }
}

impl From<CommitFormatError> for GenerationError {
    fn from(error: CommitFormatError) -> Self {
        Self::CommitFormat(error)
    }
}

impl From<StoreError> for GenerationError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

fn map_log_error(error: GenerationLogError) -> GenerationError {
    match error {
        GenerationLogError::Io(error) => GenerationError::Io(error),
        GenerationLogError::SegmentTooLarge => GenerationError::WalTooLarge,
        GenerationLogError::NeedsRepair(tail) => GenerationError::WalNeedsRepair(tail),
        GenerationLogError::PublishVerificationMismatch => {
            GenerationError::PublishVerificationMismatch
        }
        GenerationLogError::OutOfMemory => GenerationError::OutOfMemory,
        GenerationLogError::BrokenGenerationChain
        | GenerationLogError::DivergentSlots
        | GenerationLogError::EmptyAfterInitialization => GenerationError::NoRecoverableGeneration,
    }
}

fn metadata_name(object_id: MetadataObjectId) -> String {
    format!("{}{}", encode_object_id(object_id), METADATA_SUFFIX)
}

fn inventory_metadata_name(
    inventory: &mut MetadataGcInventory,
    reachable: &BTreeSet<MetadataObjectId>,
    name: &str,
) -> Result<(), GenerationError> {
    if is_metadata_mark_catalog_name(name) {
        if let Some(generation) = parse_metadata_mark_generation(name) {
            inventory.catalog_generation_high_water =
                inventory.catalog_generation_high_water.max(generation);
        }
        inventory
            .catalog_names
            .try_reserve(1)
            .map_err(|_| GenerationError::OutOfMemory)?;
        inventory.catalog_names.push(name.to_owned());
        return Ok(());
    }
    let Some(object_id) = parse_metadata_name(name)? else {
        return Ok(());
    };
    if !reachable.contains(&object_id) {
        inventory
            .candidates
            .try_reserve(1)
            .map_err(|_| GenerationError::OutOfMemory)?;
        inventory.candidates.push((object_id, name.to_owned()));
    }
    Ok(())
}

fn mark_metadata_gc_dirty(epoch: &AtomicU64) {
    let previous = epoch.fetch_add(1, Ordering::AcqRel);
    assert_ne!(
        previous,
        u64::MAX,
        "ASSERT: Metadata GC liveness epoch cannot overflow"
    );
}

fn advance_metadata_gc_journal_revision(journal: &mut MetadataGcDeltaJournal) {
    journal.revision = journal
        .revision
        .checked_add(1)
        .expect("ASSERT: Metadata GC delta journal revision cannot overflow");
}

fn mark_metadata_gc_unclassified(
    epoch: &AtomicU64,
    journal: &Mutex<MetadataGcDeltaJournal>,
    object_id: MetadataObjectId,
) {
    let mut journal = journal
        .lock()
        .expect("ASSERT: Metadata GC delta journal poisoned during publication");
    let inserted = journal.unclassified.insert(object_id);
    assert!(
        inserted,
        "ASSERT: newly published Metadata identity is not already unclassified"
    );
    advance_metadata_gc_journal_revision(&mut journal);
    drop(journal);
    mark_metadata_gc_dirty(epoch);
}

fn mark_metadata_gc_exact_required(
    epoch: &AtomicU64,
    journal: &Mutex<MetadataGcDeltaJournal>,
    reason: MetadataGcExactReason,
) {
    let mut journal = journal
        .lock()
        .expect("ASSERT: Metadata GC delta journal poisoned during invalidation");
    journal.exact_required = true;
    if journal.exact_reason.is_none() {
        journal.exact_reason = Some(reason);
    }
    advance_metadata_gc_journal_revision(&mut journal);
    drop(journal);
    mark_metadata_gc_dirty(epoch);
}

fn metadata_gc_exact_reason(
    clean: Option<MetadataGcCleanState>,
    journal: &Mutex<MetadataGcDeltaJournal>,
) -> MetadataGcExactReason {
    let Some(clean) = clean else {
        return MetadataGcExactReason::ProcessStart;
    };
    if clean.delta_run_count >= MAX_METADATA_MARK_DELTA_RUNS {
        return MetadataGcExactReason::DeltaChainLimit;
    }
    let journal = journal
        .lock()
        .expect("ASSERT: Metadata GC delta journal poisoned while reporting exact reason");
    if let Some(reason) = journal.exact_reason {
        return reason;
    }
    if !journal.unclassified.is_empty() {
        return MetadataGcExactReason::UnclassifiedPublication;
    }
    MetadataGcExactReason::UncertainWalDurability
}

fn classify_metadata_gc_additions(
    epoch: &AtomicU64,
    journal: &Mutex<MetadataGcDeltaJournal>,
    additions: &BTreeSet<MetadataObjectId>,
) {
    let mut journal = journal
        .lock()
        .expect("ASSERT: Metadata GC delta journal poisoned during commit classification");
    for object_id in additions {
        if journal.unclassified.remove(object_id) {
            let inserted = journal.additions.insert(*object_id);
            assert!(
                inserted,
                "ASSERT: one newly committed Metadata identity enters one delta only"
            );
        }
    }
    advance_metadata_gc_journal_revision(&mut journal);
    drop(journal);
    mark_metadata_gc_dirty(epoch);
}

fn parse_metadata_name(name: &str) -> Result<Option<MetadataObjectId>, GenerationError> {
    let Some(encoded) = name.strip_suffix(METADATA_SUFFIX) else {
        return Ok(None);
    };
    if encoded.len() != 64 {
        return Err(GenerationError::InvalidMetadataObjectName(name.to_owned()));
    }
    let mut bytes = [0_u8; 32];
    for (output, pair) in bytes.iter_mut().zip(encoded.as_bytes().chunks_exact(2)) {
        let (Some(high), Some(low)) = (decode_hex_nibble(pair[0]), decode_hex_nibble(pair[1]))
        else {
            return Err(GenerationError::InvalidMetadataObjectName(name.to_owned()));
        };
        *output = (high << 4) | low;
    }
    MetadataObjectId::new(bytes)
        .map(Some)
        .ok_or_else(|| GenerationError::InvalidMetadataObjectName(name.to_owned()))
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn encode_object_id(object_id: MetadataObjectId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in object_id.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastdup_format::{DurableInode, NamespaceEntry};

    fn inode(inode: u64, mutation_sequence: u64) -> DurableInode {
        DurableInode::new(
            inode,
            0o600,
            1_000,
            1_001,
            1,
            mutation_sequence,
            0,
            MetadataObjectId::new([0xA5; 32]).expect("fixture Manifest ID is nonzero"),
        )
        .expect("fixture regular inode is valid")
    }

    fn root(
        reservation_end: u64,
        allocation_cursor: u64,
        namespace_mutation_sequence: u64,
        inodes: Vec<DurableInode>,
    ) -> NamespaceRoot {
        let entries = inodes
            .iter()
            .map(|inode| {
                NamespaceEntry::new(
                    1,
                    inode.inode(),
                    format!("inode-{}", inode.inode()).into_bytes(),
                )
                .expect("fixture root entry is valid")
            })
            .collect();
        NamespaceRoot::new(
            reservation_end,
            allocation_cursor,
            namespace_mutation_sequence,
            inodes,
            entries,
        )
        .expect("fixture Namespace Root is valid")
    }

    fn record_for(root: &NamespaceRoot) -> CommitRecord {
        CommitRecord::new(
            2,
            CommitRecordHash::from_bytes([0xB6; 32]),
            MetadataObjectId::new([0xC7; 32]).expect("fixture Namespace Root ID is nonzero"),
            PolicySetId::new([0xD8; 32]).expect("fixture Policy Set ID is nonzero"),
            root.namespace_mutation_sequence(),
            root.inode_reservation_end(),
            root.inode_allocation_cursor(),
        )
        .expect("fixture previous Commit Record is valid")
    }

    #[test]
    fn transition_pair_rejects_a_decreasing_per_inode_mutation_sequence() {
        let previous_root = root(128, 16, 20, vec![inode(5, 7)]);
        let previous_record = record_for(&previous_root);
        let proposed_root = root(128, 16, 21, vec![inode(5, 6)]);

        assert!(matches!(
            verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
            Err(GenerationError::NonMonotonicInodeMutation {
                inode: 5,
                previous: 7,
                proposed: 6,
            })
        ));
    }

    #[test]
    fn transition_pair_rejects_a_removed_inode_reused_below_the_allocation_cursor() {
        let previous_root = root(128, 16, 20, Vec::new());
        let previous_record = record_for(&previous_root);
        let proposed_root = root(128, 16, 21, vec![inode(5, 8)]);

        assert!(matches!(
            verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
            Err(GenerationError::ReusedInodeId {
                inode: 5,
                previous_allocation_cursor: 16,
            })
        ));
    }

    #[test]
    fn transition_pair_rejects_consuming_a_reservation_first_enlarged_by_the_proposal() {
        let previous_root = root(128, 16, 20, Vec::new());
        let previous_record = record_for(&previous_root);
        let proposed_root = root(256, 129, 21, vec![inode(128, 8)]);

        assert!(matches!(
            verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
            Err(
                GenerationError::AllocationExceededPreviouslyDurableReservation {
                    previous_reservation_end: 128,
                    proposed_allocation_cursor: 129,
                }
            )
        ));
    }
}
