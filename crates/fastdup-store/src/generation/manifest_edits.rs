//! Path-local Manifest replacement, truncation, splice and successor-proof composition.
use super::graph::record_matches_namespace_root;
use super::manifests::manifest_dependencies;
use super::{GenerationError, GenerationRepository, ManifestSuccessorProof, SuccessorPredecessor};
use crate::StorageIo;
use crate::manifest_tree::{
    ManifestTreeSummary, read_manifest_tree_range, rewrite_manifest_tree_range,
    rewrite_manifest_tree_range_successor, splice_manifest_tree, truncate_manifest_tree,
};
use fastdup_format::{ManifestExtent, MetadataObjectId};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::atomic::Ordering;

impl<I: StorageIo> GenerationRepository<I> {
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

    pub(super) fn mark_successor_root_release_durable(
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
}
