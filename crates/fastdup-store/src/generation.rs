use crate::generation_log::{GenerationLog, GenerationLogError, LogSnapshot};
use crate::manifest_tree::{
    ManifestRangeExtent, ManifestTreeError, ManifestTreeSummary, append_manifest_tree,
    encode_manifest_tree, flatten_manifest_tree, read_manifest_tree_range,
    rewrite_manifest_tree_range, rewrite_manifest_tree_range_successor, scan_manifest_tree,
    splice_manifest_tree, truncate_manifest_tree,
};
use crate::{
    ActivatedExactIndex, ContainerRepository, StorageIo, StoreError, VerifiedManifestFile,
};
use fastdup_format::{
    CommitFormatError, CommitRecord, CommitRecordHash, MAX_METADATA_OBJECT_BYTES, ManifestExtent,
    ManifestLeaf, MetadataFormatError, MetadataObjectId, NamespaceRoot, PolicySetId,
};
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};

const METADATA_SUFFIX: &str = ".fdm";
const WRITE_BLOCK_BYTES: usize = 4_096;
const MAX_METADATA_OBJECT_BYTES_U64: u64 = 16 * 1_024 * 1_024;
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
    index: Arc<ActivatedExactIndex<X>>,
}

impl<C, X> IndexedRequiredChunkVerifier<C, X> {
    #[must_use]
    pub const fn new(
        containers: ContainerRepository<C>,
        index: Arc<ActivatedExactIndex<X>>,
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
        }
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
        Ok(self.publish_complete_manifest(manifest, true)?.0.root())
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
        let (summary, introduced_chunks) = self.publish_complete_manifest(manifest, sync_root)?;
        Ok(ManifestSuccessorProof {
            predecessor,
            summary,
            introduced_chunks,
        })
    }

    fn publish_complete_manifest(
        &self,
        manifest: &ManifestLeaf,
        sync_root: bool,
    ) -> Result<(ManifestTreeSummary, BTreeMap<fastdup_format::ChunkId, u64>), GenerationError>
    {
        let tree = encode_manifest_tree(manifest)?;
        for (expected_id, encoded) in tree.objects() {
            let published_id = self.stage_metadata(encoded)?;
            assert_eq!(
                published_id, *expected_id,
                "ASSERT: Manifest tree plan identity must equal published object identity"
            );
        }
        if sync_root {
            self.storage.sync_root()?;
        }
        Ok((
            ManifestTreeSummary::new(
                tree.root(),
                manifest.file_length(),
                manifest_allocated_bytes(manifest.extents())?,
            ),
            manifest_dependencies(manifest.extents())?,
        ))
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
        let (tree, summary) = append_manifest_tree(previous, appended, |node_id| {
            self.read_manifest_node(node_id)
        })?;
        for (expected_id, encoded) in tree.objects() {
            let published_id = self.stage_metadata(encoded)?;
            assert_eq!(
                published_id, *expected_id,
                "ASSERT: append-local Manifest plan identity must equal published object identity"
            );
        }
        if sync_root {
            self.storage.sync_root()?;
        }
        Ok(ManifestSuccessorProof {
            predecessor,
            summary,
            introduced_chunks: manifest_dependencies(appended)?,
        })
    }

    /// Reuses one graph proof without introducing new DATA dependencies.
    #[must_use]
    pub fn reuse_manifest_successor(
        &self,
        predecessor: SuccessorPredecessor,
        summary: ManifestTreeSummary,
    ) -> ManifestSuccessorProof {
        ManifestSuccessorProof {
            predecessor,
            summary,
            introduced_chunks: BTreeMap::new(),
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
            let published_id = self.stage_metadata(encoded)?;
            assert_eq!(
                published_id, *expected_id,
                "ASSERT: replacement-local Manifest plan identity must equal published object identity"
            );
        }
        if sync_root {
            self.storage.sync_root()?;
        }
        previous.summary = summary;
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
        let (tree, summary) = truncate_manifest_tree(previous.summary, logical_size, |node_id| {
            self.read_manifest_node(node_id)
        })?;
        for (expected_id, encoded) in tree.objects() {
            let published_id = self.stage_metadata(encoded)?;
            assert_eq!(
                published_id, *expected_id,
                "ASSERT: truncate-local Manifest plan identity must equal published object identity"
            );
        }
        if sync_root {
            self.storage.sync_root()?;
        }
        previous.summary = summary;
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
        let (tree, summary) = splice_manifest_tree(previous, replaced, replacement, |node_id| {
            self.read_manifest_node(node_id)
        })?;
        for (expected_id, encoded) in tree.objects() {
            let published_id = self.stage_metadata(encoded)?;
            assert_eq!(
                published_id, *expected_id,
                "ASSERT: splice-local Manifest plan identity must equal published object identity"
            );
        }
        self.storage.sync_root()?;
        Ok(summary)
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
        previous.summary = self.publish_manifest_splice(previous.summary, replaced, replacement)?;
        Ok(previous)
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
            .inodes()
            .iter()
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
    ) -> Result<GenerationGcScrubProof, GenerationError> {
        let records = {
            let _guard = self
                .commit_lock
                .lock()
                .expect("ASSERT: generation scrub lock poisoned");
            let Some(snapshot) = GenerationLog::new(&self.storage)
                .load_for_recovery()
                .map_err(map_log_error)?
            else {
                return Ok(GenerationGcScrubProof::default());
            };
            if snapshot.tail() != &WalTail::Clean {
                return Err(GenerationError::WalNeedsRepair(snapshot.tail().clone()));
            }
            let valid = self.validate_recovery_transition_prefix(snapshot.records())?;
            if valid.len() != snapshot.records().len() {
                return Err(GenerationError::NoRecoverableGeneration);
            }
            valid
        };
        if records.is_empty() {
            return Ok(GenerationGcScrubProof::default());
        }
        let mut latest_namespace_inodes = 0_usize;
        let mut latest_manifest_files = 0_usize;
        let mut online_chunks = BTreeMap::new();
        let first_online = records.len().saturating_sub(2);
        for (ordinal, record) in records.iter().copied().enumerate() {
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
        containers.verify_required_chunks(&online_chunks)?;
        let summary = GenerationScrubSummary {
            generations: records.len(),
            first_generation: records.first().copied().map(CommitRecord::generation),
            latest_generation: records.last().copied().map(CommitRecord::generation),
            latest_namespace_inodes,
            latest_manifest_files,
        };
        let online_records = records[first_online..].to_vec();
        Ok(GenerationGcScrubProof {
            summary,
            online_records,
            online_chunks,
        })
    }

    pub(crate) fn gc_proof_is_current(
        &self,
        proof: &GenerationGcScrubProof,
    ) -> Result<bool, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: GC generation revalidation lock poisoned");
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
        let files = verified_files(manifests, &self.storage, containers)?;
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
        let files = verified_files(manifests, &self.storage, containers)?;
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
        if proofs.len() != root.inodes().len() {
            return Err(GenerationError::ManifestCountMismatch {
                namespace_inodes: root.inodes().len(),
                manifests: proofs.len(),
            });
        }
        let mut introduced = BTreeMap::new();
        let mut manifests = Vec::new();
        manifests
            .try_reserve_exact(proofs.len())
            .map_err(|_| GenerationError::OutOfMemory)?;
        for (inode, proof) in root.inodes().iter().zip(proofs) {
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
        let files = verified_files(manifests, &self.storage, containers)?;
        let record = self.commit_verified_namespace_from_snapshot(root, &snapshot)?;
        Ok(CommittedDataGeneration { record, files })
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
        let encoded_root = root.encode()?;
        let root_id = self.publish_metadata(&encoded_root)?;
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
        GenerationLog::new(&self.storage)
            .append(snapshot, record)
            .map_err(map_log_error)?;
        Ok(record)
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
        let files = verified_files(graph.manifests, &self.storage, containers)?;
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
        let files = verified_files(graph.manifests, &self.storage, containers)?;
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

    fn publish_metadata(&self, encoded: &[u8]) -> Result<MetadataObjectId, GenerationError> {
        let object_id = self.stage_metadata(encoded)?;
        self.storage.sync_root()?;
        Ok(object_id)
    }

    fn stage_metadata(&self, encoded: &[u8]) -> Result<MetadataObjectId, GenerationError> {
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
            return Ok(object_id);
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
        Ok(object_id)
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
            .try_reserve_exact(root.inodes().len())
            .map_err(|_| GenerationError::OutOfMemory)?;
        for inode in root.inodes() {
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

#[derive(Clone, Debug, Default)]
pub(crate) struct GenerationGcScrubProof {
    summary: GenerationScrubSummary,
    online_records: Vec<CommitRecord>,
    online_chunks: BTreeMap<fastdup_format::ChunkId, u64>,
}

impl GenerationGcScrubProof {
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
    metadata: &M,
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
                metadata.clone(),
                containers.clone(),
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
    RetainedManifestNotInPredecessor(MetadataObjectId),
    RetainedManifestRangeInvalid {
        root: MetadataObjectId,
        start: u64,
        end: u64,
        logical_size: u64,
    },
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
            | Self::OutOfMemory => false,
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
