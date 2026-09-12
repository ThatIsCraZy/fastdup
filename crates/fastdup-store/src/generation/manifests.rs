//! Complete/append Manifest publication and public read/structural-scrub operations.
use super::metadata::metadata_name;
use super::{
    GenerationError, GenerationRepository, MAX_METADATA_OBJECT_BYTES_U64, ManifestSuccessorProof,
    PublishedManifestProof, SuccessorPredecessor,
};
use crate::StorageIo;
use crate::manifest_tree::{
    ManifestRangeExtent, ManifestTreeError, ManifestTreeSummary, append_manifest_tree,
    encode_manifest_tree, flatten_manifest_tree, read_manifest_tree_range, scan_manifest_tree,
};
use fastdup_format::{ManifestExtent, ManifestLayout, ManifestLeaf, MetadataObjectId};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

impl<I: StorageIo> GenerationRepository<I> {
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
            .publish_complete_manifest(manifest.file_length(), manifest.extents(), true)?
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

    /// Stages a complete logical layout as bounded physical Manifest leaves.
    /// Namespace publication supplies the metadata-directory durability barrier.
    ///
    /// # Errors
    /// Returns layout, allocation, identity or publication failures.
    pub fn stage_manifest_layout_successor(
        &self,
        predecessor: SuccessorPredecessor,
        layout: &ManifestLayout,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let published =
            self.publish_complete_manifest(layout.file_length(), layout.extents(), false)?;
        Ok(ManifestSuccessorProof {
            predecessor,
            summary: published.summary,
            introduced_chunks: published.introduced_chunks,
            introduced_metadata: published.introduced_metadata,
            metadata_root_pin: published.metadata_root_pin,
        })
    }

    fn publish_manifest_successor_with_sync(
        &self,
        predecessor: SuccessorPredecessor,
        manifest: &ManifestLeaf,
        sync_root: bool,
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let published =
            self.publish_complete_manifest(manifest.file_length(), manifest.extents(), sync_root)?;
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
        logical_size: u64,
        extents: &[ManifestExtent],
        sync_root: bool,
    ) -> Result<PublishedManifestProof, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Metadata GC publication barrier poisoned");
        let tree = encode_manifest_tree(logical_size, extents)?;
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
            logical_size,
            manifest_allocated_bytes(extents)?,
        );
        Ok(PublishedManifestProof {
            summary,
            introduced_chunks: manifest_dependencies(extents)?,
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

    /// Stages an append after earlier edits in the same successor, retaining
    /// their newly introduced DATA and Metadata dependencies.
    ///
    /// # Errors
    ///
    /// Returns append, dependency-length conflict, or predecessor errors.
    ///
    /// # Panics
    ///
    /// Panics only for the same internal publication invariants as
    /// [`Self::stage_manifest_append`].
    pub fn stage_manifest_append_successor(
        &self,
        previous: ManifestSuccessorProof,
        appended: &[ManifestExtent],
    ) -> Result<ManifestSuccessorProof, GenerationError> {
        let mut next =
            self.stage_manifest_append(previous.predecessor, previous.summary, appended)?;
        self.mark_successor_root_release_durable(&previous)?;
        for (chunk_id, logical_length) in previous.introduced_chunks {
            if let Some(first_length) = next.introduced_chunks.insert(chunk_id, logical_length)
                && first_length != logical_length
            {
                return Err(GenerationError::ManifestChunkLengthConflict {
                    chunk_id,
                    first_length,
                    second_length: logical_length,
                });
            }
        }
        next.introduced_metadata
            .extend(previous.introduced_metadata);
        Ok(next)
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
        let _independent = crate::metadata_object_cache::IndependentRead::enter();
        scan_manifest_tree(
            root,
            |node_id| self.read_manifest_node(node_id),
            |_offset, _extent| Ok(()),
        )
        .map_err(Into::into)
    }
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

pub(super) fn manifest_dependencies(
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
