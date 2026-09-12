//! Pinned Metadata graph copying to/from DATA-tier Recovery Checkpoints.
use super::error::map_log_error;
use super::graph::record_matches_namespace_root;
use super::metadata_gc::mark_metadata_gc_dirty;
use super::{
    GenerationError, GenerationRepository, RecoveredGeneration, RecoveryCheckpointCandidate,
    RequiredChunkVerifier, WalTail,
};
use crate::StorageIo;
use crate::generation_log::GenerationLog;
use crate::manifest_tree::scan_manifest_tree;
use fastdup_format::{CommitRecord, ManifestExtent, MetadataObjectId};
use std::collections::{BTreeMap, BTreeSet};

impl<I: StorageIo> GenerationRepository<I> {
    pub(crate) fn publish_latest_recovery_checkpoint_to<D: StorageIo>(
        &self,
        checkpoints: &crate::recovery_checkpoint::RecoveryCheckpointRepository<D>,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<
        Option<crate::recovery_checkpoint::RecoveryCheckpointSummary>,
        crate::recovery_checkpoint::RecoveryCheckpointError,
    > {
        let candidates = self.recovery_checkpoint_candidates()?;
        if candidates.is_empty() {
            return Ok(None);
        }
        for candidate in candidates {
            let graph = self.scan_recovery_checkpoint_candidate(candidate.record);
            let (object_ids, required) = match graph {
                Ok(graph) => graph,
                Err(error) if error.allows_generation_fallback() => continue,
                Err(error) => return Err(error.into()),
            };
            if let Some(verifier) = verifier
                && let Err(error) = verifier.verify_required_chunks(&required)
            {
                let error = GenerationError::Store(error);
                if error.allows_generation_fallback() {
                    continue;
                }
                return Err(error.into());
            }
            return checkpoints
                .publish_source(candidate.record, &object_ids, verifier, |object_id| {
                    self.read_metadata(object_id).map_err(Into::into)
                })
                .map(Some);
        }
        Err(GenerationError::NoRecoverableGeneration.into())
    }

    fn recovery_checkpoint_candidates(
        &self,
    ) -> Result<Vec<RecoveryCheckpointCandidate>, GenerationError> {
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Recovery Checkpoint pin barrier poisoned");
        let _commit_guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: Recovery Checkpoint candidate lock poisoned");
        let Some(snapshot) = GenerationLog::new(&self.storage)
            .load_for_recovery()
            .map_err(map_log_error)?
        else {
            return Ok(Vec::new());
        };
        let valid = self.validate_recovery_transition_prefix(snapshot.records())?;
        let start = valid.len().saturating_sub(2);
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(valid.len() - start)
            .map_err(|_| GenerationError::OutOfMemory)?;
        for record in valid[start..].iter().rev().copied() {
            candidates.push(RecoveryCheckpointCandidate {
                record,
                _pin: self.pin_recovery_checkpoint_root(record.namespace_root()),
            });
        }
        Ok(candidates)
    }

    fn scan_recovery_checkpoint_candidate(
        &self,
        record: CommitRecord,
    ) -> Result<
        (
            BTreeSet<MetadataObjectId>,
            BTreeMap<fastdup_format::ChunkId, u64>,
        ),
        GenerationError,
    > {
        let (root, namespace_objects, _) =
            self.read_namespace_root_graph(record.namespace_root())?;
        if !record_matches_namespace_root(record, &root) {
            return Err(GenerationError::PreviousGenerationRecordMismatch);
        }
        let mut objects = BTreeSet::new();
        objects.insert(record.namespace_root());
        objects.extend(namespace_objects);
        let mut required = BTreeMap::new();
        for inode in root.file_inodes() {
            let mut conflict = None;
            let summary = scan_manifest_tree(
                inode.manifest_root(),
                |object_id| {
                    objects.insert(object_id);
                    self.read_manifest_node(object_id)
                },
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
                    if let Some(previous) = required.insert(chunk_id, logical_length)
                        && previous != logical_length
                    {
                        conflict = Some((chunk_id, previous, logical_length));
                    }
                    Ok(())
                },
            )?;
            if summary.logical_size() != inode.logical_size() {
                return Err(GenerationError::ManifestLengthMismatch {
                    inode: inode.inode(),
                    inode_length: inode.logical_size(),
                    manifest_length: summary.logical_size(),
                });
            }
            if let Some((chunk_id, first_length, second_length)) = conflict {
                return Err(GenerationError::ManifestChunkLengthConflict {
                    chunk_id,
                    first_length,
                    second_length,
                });
            }
        }
        Ok((objects, required))
    }

    pub(crate) fn install_recovery_checkpoint<F>(
        &self,
        record: CommitRecord,
        object_ids: &BTreeSet<MetadataObjectId>,
        mut read_object: F,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<RecoveredGeneration, GenerationError>
    where
        F: FnMut(
            MetadataObjectId,
        ) -> Result<Vec<u8>, crate::recovery_checkpoint::RecoveryCheckpointError>,
    {
        let _independent = crate::metadata_object_cache::IndependentRead::enter();
        let _publication_guard = self
            .metadata_gc_barrier
            .read()
            .expect("ASSERT: Recovery Checkpoint installation barrier poisoned");
        let _commit_guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: Recovery Checkpoint installation lock poisoned");
        if let Some(snapshot) = GenerationLog::new(&self.storage)
            .load_for_recovery()
            .map_err(map_log_error)?
        {
            if snapshot.tail() != &WalTail::Clean || snapshot.records() != [record] {
                return Err(GenerationError::RecoveryTargetNotEmpty);
            }
            return self
                .recover_latest_using(Some(verifier))?
                .map(|graph| graph.generation)
                .ok_or(GenerationError::NoRecoverableGeneration);
        }
        if record.policy_set() != self.supported_policy {
            return Err(GenerationError::UnsupportedPolicySet {
                generation: record.generation(),
                policy_set: record.policy_set(),
            });
        }
        for object_id in object_ids.iter().copied() {
            let encoded = read_object(object_id).map_err(|error| match error {
                crate::recovery_checkpoint::RecoveryCheckpointError::Io(error) => {
                    GenerationError::Io(error)
                }
                _ => GenerationError::PublishVerificationMismatch,
            })?;
            if self.stage_metadata(&encoded)? != object_id {
                return Err(GenerationError::MetadataIdentityCollision(object_id));
            }
        }
        self.storage.sync_root()?;
        let root = self.read_namespace_root(record.namespace_root())?;
        if !record_matches_namespace_root(record, &root) {
            return Err(GenerationError::PreviousGenerationRecordMismatch);
        }
        self.verify_manifest_graph(&root, Some(verifier))?;
        mark_metadata_gc_dirty(&self.metadata_gc_epoch);
        GenerationLog::new(&self.storage)
            .install_recovery_anchor(record)
            .map_err(map_log_error)?;
        self.recover_latest_using(Some(verifier))?
            .map(|graph| graph.generation)
            .ok_or(GenerationError::NoRecoverableGeneration)
    }
}
