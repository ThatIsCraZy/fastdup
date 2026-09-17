//! Namespace transition rules and complete Manifest graph dependency validation.
use super::{GenerationError, GenerationRepository, RequiredChunkVerifier, VerifiedManifests};
use crate::StorageIo;
use crate::manifest_tree::scan_manifest_tree;
use fastdup_format::{
    CommitRecord, ManifestExtent, MetadataObjectId, NamespaceGcGraph, NamespaceGraphRoot,
    NamespaceRoot,
};
use std::collections::{BTreeMap, BTreeSet};

impl<I: StorageIo> GenerationRepository<I> {
    pub(super) fn read_namespace_root(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<NamespaceRoot, GenerationError> {
        let bytes = self.read_metadata(object_id)?;
        let descriptor = NamespaceGraphRoot::decode(&bytes)?;
        let mut shards = BTreeMap::new();
        for reference in descriptor.shards() {
            let shard_id = reference.object_id();
            if let std::collections::btree_map::Entry::Vacant(entry) = shards.entry(shard_id) {
                entry.insert(self.read_metadata(shard_id)?);
            }
        }
        NamespaceRoot::decode_graph(&bytes, &shards).map_err(Into::into)
    }

    pub(super) fn read_namespace_root_graph(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<(NamespaceRoot, BTreeSet<MetadataObjectId>, u64), GenerationError> {
        let bytes = self.read_metadata(object_id)?;
        let descriptor = NamespaceGraphRoot::decode(&bytes)?;
        let mut object_ids = BTreeSet::new();
        let mut byte_count =
            u64::try_from(bytes.len()).map_err(|_| GenerationError::MetadataTooLarge)?;
        let mut shards = BTreeMap::new();
        for (ordinal, reference) in descriptor.shards().iter().copied().enumerate() {
            if ordinal % 256 == 0 {
                self.check_maintenance()?;
            }
            let shard_id = reference.object_id();
            if object_ids.insert(shard_id) {
                let shard = self.read_metadata(shard_id)?;
                byte_count = byte_count
                    .checked_add(
                        u64::try_from(shard.len())
                            .map_err(|_| GenerationError::MetadataTooLarge)?,
                    )
                    .ok_or(GenerationError::MetadataTooLarge)?;
                shards.insert(shard_id, shard);
            }
        }
        let root = NamespaceRoot::decode_graph(&bytes, &shards)?;
        Ok((root, object_ids, byte_count))
    }

    pub(super) fn read_namespace_root_gc_graph(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<(NamespaceGcGraph, u64), GenerationError> {
        let bytes = self.read_metadata(object_id)?;
        let descriptor = NamespaceGraphRoot::decode(&bytes)?;
        let mut byte_count =
            u64::try_from(bytes.len()).map_err(|_| GenerationError::MetadataTooLarge)?;
        let mut shards = BTreeMap::new();
        for (ordinal, reference) in descriptor.shards().iter().copied().enumerate() {
            if ordinal % 256 == 0 {
                self.check_maintenance()?;
            }
            let shard_id = reference.object_id();
            if let std::collections::btree_map::Entry::Vacant(entry) = shards.entry(shard_id) {
                let shard = self.read_metadata(shard_id)?;
                byte_count = byte_count
                    .checked_add(
                        u64::try_from(shard.len())
                            .map_err(|_| GenerationError::MetadataTooLarge)?,
                    )
                    .ok_or(GenerationError::MetadataTooLarge)?;
                entry.insert(shard);
            }
        }
        let graph = descriptor.decode_gc_graph_with_shards(&shards)?;
        Ok((graph, byte_count))
    }

    pub(super) fn verify_manifest_graph(
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
        let (manifests, required_chunks) = self.scan_manifest_graph_with_required(root, None)?;
        if required_chunks.is_empty() {
            return Ok((manifests, required_chunks));
        }
        let Some(verifier) = verifier else {
            return Err(GenerationError::DataLocationsNotConnected);
        };
        verifier.verify_required_chunks(&required_chunks)?;
        Ok((manifests, required_chunks))
    }

    pub(super) fn scan_manifest_graph_with_required(
        &self,
        root: &NamespaceRoot,
        selected_chunks: Option<&BTreeSet<fastdup_format::ChunkId>>,
    ) -> Result<(VerifiedManifests, BTreeMap<fastdup_format::ChunkId, u64>), GenerationError> {
        let mut required_chunks = BTreeMap::new();
        let mut chunk_length_conflict = None;
        let mut manifests = Vec::new();
        let traversal_probes = std::cell::Cell::new(0_u64);
        manifests
            .try_reserve_exact(root.file_inode_count())
            .map_err(|_| GenerationError::OutOfMemory)?;
        for inode in root.file_inodes() {
            self.check_maintenance()?;
            let summary = scan_manifest_tree(
                inode.manifest_root(),
                |node_id| {
                    traversal_probes.set(traversal_probes.get() + 1);
                    if traversal_probes.get().is_multiple_of(256) {
                        self.check_manifest_maintenance()?;
                    }
                    self.read_manifest_node(node_id)
                },
                |_logical_offset, extent| {
                    traversal_probes.set(traversal_probes.get() + 1);
                    if traversal_probes.get().is_multiple_of(256) {
                        self.check_manifest_maintenance()?;
                    }
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
                    if selected_chunks.is_some_and(|selected| !selected.contains(&chunk_id)) {
                        return Ok(());
                    }
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

    pub(super) fn scan_manifest_root_required_chunks(
        &self,
        root: MetadataObjectId,
        selected_chunks: Option<&BTreeSet<fastdup_format::ChunkId>>,
        required_chunks: &mut BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), GenerationError> {
        let mut chunk_length_conflict = None;
        let traversal_probes = std::cell::Cell::new(0_u64);
        self.check_maintenance()?;
        scan_manifest_tree(
            root,
            |node_id| {
                traversal_probes.set(traversal_probes.get() + 1);
                if traversal_probes.get().is_multiple_of(256) {
                    self.check_manifest_maintenance()?;
                }
                self.read_manifest_node(node_id)
            },
            |_logical_offset, extent| {
                traversal_probes.set(traversal_probes.get() + 1);
                if traversal_probes.get().is_multiple_of(256) {
                    self.check_manifest_maintenance()?;
                }
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
                if selected_chunks.is_some_and(|selected| !selected.contains(&chunk_id)) {
                    return Ok(());
                }
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

    pub(super) fn verify_generation_transition(
        &self,
        previous_record: CommitRecord,
        proposed_root: &NamespaceRoot,
    ) -> Result<(), GenerationError> {
        // The predecessor Root is content-identified, so a repository-local
        // copy keyed by its object ID is as authoritative as a fresh reread
        // for this writer-side transition check. Recovery, mount, and offline
        // scrub always reread and rehash the complete graph independently.
        let object_id = previous_record.namespace_root();
        let previous_root = if let Some(root) = self.cached_previous_namespace_root(object_id) {
            root
        } else {
            let root = std::sync::Arc::new(self.read_namespace_root(object_id)?);
            self.remember_previous_namespace_root(object_id, std::sync::Arc::clone(&root));
            root
        };
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

pub(super) fn record_matches_namespace_root(record: CommitRecord, root: &NamespaceRoot) -> bool {
    root.namespace_mutation_sequence() == record.namespace_mutation_cutoff()
        && root.inode_reservation_end() == record.inode_reservation_end()
        && root.inode_allocation_cursor() == record.inode_allocation_cursor()
}

pub(super) fn record_matches_namespace_gc_graph(
    record: CommitRecord,
    graph: &NamespaceGcGraph,
) -> bool {
    graph.namespace_mutation_sequence() == record.namespace_mutation_cutoff()
        && graph.inode_reservation_end() == record.inode_reservation_end()
        && graph.inode_allocation_cursor() == record.inode_allocation_cursor()
}

pub(super) fn verify_generation_transition_pair_gc_graph(
    previous_record: CommitRecord,
    previous_graph: &NamespaceGcGraph,
    proposed_graph: &NamespaceGcGraph,
) -> Result<(), GenerationError> {
    if proposed_graph.namespace_mutation_sequence() < previous_record.namespace_mutation_cutoff() {
        return Err(GenerationError::NonMonotonicNamespaceMutation {
            previous: previous_record.namespace_mutation_cutoff(),
            proposed: proposed_graph.namespace_mutation_sequence(),
        });
    }
    if proposed_graph.inode_reservation_end() < previous_record.inode_reservation_end() {
        return Err(GenerationError::NonMonotonicInodeReservation {
            previous: previous_record.inode_reservation_end(),
            proposed: proposed_graph.inode_reservation_end(),
        });
    }
    if proposed_graph.inode_allocation_cursor() < previous_record.inode_allocation_cursor() {
        return Err(GenerationError::NonMonotonicInodeAllocation {
            previous: previous_record.inode_allocation_cursor(),
            proposed: proposed_graph.inode_allocation_cursor(),
        });
    }
    if proposed_graph.inode_allocation_cursor() > previous_record.inode_reservation_end() {
        return Err(
            GenerationError::AllocationExceededPreviouslyDurableReservation {
                previous_reservation_end: previous_record.inode_reservation_end(),
                proposed_allocation_cursor: proposed_graph.inode_allocation_cursor(),
            },
        );
    }
    for (inode, mutation_sequence) in proposed_graph.inode_transitions() {
        match previous_graph
            .inode_transitions()
            .binary_search_by_key(inode, |entry| entry.0)
        {
            Ok(previous_index) => {
                let (_, previous_mutation_sequence) =
                    previous_graph.inode_transitions()[previous_index];
                if *mutation_sequence < previous_mutation_sequence {
                    return Err(GenerationError::NonMonotonicInodeMutation {
                        inode: *inode,
                        previous: previous_mutation_sequence,
                        proposed: *mutation_sequence,
                    });
                }
            }
            Err(_) if *inode < previous_record.inode_allocation_cursor() => {
                return Err(GenerationError::ReusedInodeId {
                    inode: *inode,
                    previous_allocation_cursor: previous_record.inode_allocation_cursor(),
                });
            }
            Err(_) => {}
        }
    }
    Ok(())
}

pub(super) fn verify_generation_transition_pair(
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
