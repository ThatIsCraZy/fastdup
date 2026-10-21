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
use crate::manifest_tree::{ManifestNodeFacts, ManifestTreeError, scan_unknown_manifest_nodes};
use crate::recovery_checkpoint::{
    CheckpointWriterState, RecoveryCheckpointError, RecoveryCheckpointRepository,
    RecoveryCheckpointSummary,
};
use fastdup_format::{
    ChunkId, CommitRecord, ManifestExtent, MetadataObjectId, NamespaceObjectReferences,
    RecoveryCheckpointObjectFacts, RecoveryCheckpointObjectKind,
    decode_namespace_object_references,
};
use std::collections::{BTreeMap, BTreeSet};

/// Objects of one committed graph that a checkpoint file must embed.
pub(crate) struct CheckpointIncrement {
    pub(crate) objects: BTreeMap<MetadataObjectId, RecoveryCheckpointObjectFacts>,
    /// Chunks named by the embedded Manifest leaves.
    pub(crate) required: BTreeMap<ChunkId, u64>,
}

impl<I: StorageIo> GenerationRepository<I> {
    pub(crate) fn publish_latest_recovery_checkpoint_to<D: StorageIo>(
        &self,
        checkpoints: &RecoveryCheckpointRepository<D>,
        verifier: Option<&dyn RequiredChunkVerifier>,
        writer: &mut Option<CheckpointWriterState>,
    ) -> Result<Option<RecoveryCheckpointSummary>, RecoveryCheckpointError> {
        // Take before fallible work: errors revoke even an earlier writer state.
        let previous = writer.take();
        let candidates = self.recovery_checkpoint_candidates()?;
        if candidates.is_empty() {
            return Ok(None);
        }
        let explicit = verifier.is_some() || crate::read_intent::independent();
        let mut current = match previous {
            Some(state) => Some(state),
            None => checkpoints.adopt_selected_chain()?,
        };
        for candidate in candidates {
            if let Some(state) = current.as_ref() {
                let tip = state.tip_record();
                if tip == candidate.record {
                    if explicit {
                        checkpoints.verify_writer_chain(state, verifier)?;
                    }
                    let summary = state.summary();
                    *writer = current;
                    return Ok(Some(summary));
                }
                if candidate.record.generation() <= tip.generation() {
                    continue;
                }
            }
            let delta = !explicit
                && current
                    .as_ref()
                    .is_some_and(CheckpointWriterState::accepts_delta);
            let known = |object_id| {
                current
                    .as_ref()
                    .filter(|_| delta)
                    .and_then(|state| state.facts(object_id))
            };
            let increment = match self.scan_recovery_checkpoint_increment(candidate.record, known) {
                Ok(increment) => increment,
                Err(error) if error.allows_generation_fallback() => continue,
                Err(error) => return Err(error.into()),
            };
            if let Some(verifier) = verifier
                && let Err(error) = verifier.verify_required_chunks(&increment.required)
            {
                let error = GenerationError::Store(error);
                if error.allows_generation_fallback() {
                    continue;
                }
                return Err(error.into());
            }
            let state = checkpoints.publish_chain_file(
                candidate.record,
                &increment,
                current.take(),
                delta,
                |object_id| self.read_metadata(object_id).map_err(Into::into),
            )?;
            // Only the successful checkpoint/head durability boundary issues
            // writer state. It is not reusable recovery or scrub evidence.
            self.cleanup_recovery_checkpoint_root_pin_handles();
            let summary = state.summary();
            *writer = Some(state);
            return Ok(Some(summary));
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
                _pin: self.pin_recovery_checkpoint_root(record.namespace_root(), true),
            });
        }
        Ok(candidates)
    }

    /// Enumerates the pinned graph of `record` minus every complete subtree
    /// `known` already holds. References into known objects are validated
    /// against their recorded facts; every other object is read and validated.
    pub(crate) fn scan_recovery_checkpoint_increment<K>(
        &self,
        record: CommitRecord,
        known: K,
    ) -> Result<CheckpointIncrement, GenerationError>
    where
        K: Fn(MetadataObjectId) -> Option<RecoveryCheckpointObjectFacts>,
    {
        let (root, namespace_objects, _) =
            self.read_namespace_root_graph(record.namespace_root())?;
        if !record_matches_namespace_root(record, &root) {
            return Err(GenerationError::PreviousGenerationRecordMismatch);
        }
        let mut objects = BTreeMap::new();
        for object_id in std::iter::once(record.namespace_root()).chain(namespace_objects) {
            let is_root = object_id == record.namespace_root();
            let facts = if let Some(facts) = known(object_id) {
                facts
            } else {
                let facts = namespace_object_facts(&self.read_metadata(object_id)?)?;
                objects.insert(object_id, facts);
                facts
            };
            let expected = matches!(
                (is_root, facts.kind()),
                (true, RecoveryCheckpointObjectKind::NamespaceGraphRoot)
                    | (
                        false,
                        RecoveryCheckpointObjectKind::NamespaceInodeShard
                            | RecoveryCheckpointObjectKind::NamespaceEntryShard
                    )
            );
            if !expected {
                return Err(ManifestTreeError::InvalidTree.into());
            }
        }
        let known_manifest = |object_id| {
            known(object_id)
                .filter(|facts| facts.is_manifest())
                .map(|facts| ManifestNodeFacts {
                    level: facts.level(),
                    logical_length: facts.logical_length(),
                    allocated_bytes: facts.allocated_bytes(),
                })
        };
        let mut discovered = BTreeMap::new();
        let mut required = BTreeMap::new();
        let mut conflict = None;
        let mut read = |object_id| self.read_manifest_node(object_id);
        for inode in root.file_inodes() {
            // A root that disagrees with its inode length fails as InvalidTree.
            scan_unknown_manifest_nodes(
                inode.manifest_root(),
                inode.logical_size(),
                known_manifest,
                &mut discovered,
                &mut read,
                |extent| {
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
            if let Some((chunk_id, first_length, second_length)) = conflict {
                return Err(GenerationError::ManifestChunkLengthConflict {
                    chunk_id,
                    first_length,
                    second_length,
                });
            }
        }
        for (object_id, facts) in discovered {
            let facts = if facts.level == 0 {
                RecoveryCheckpointObjectFacts::manifest_leaf(
                    facts.logical_length,
                    facts
                        .allocated_bytes
                        .ok_or(ManifestTreeError::InvalidTree)?,
                )
            } else {
                RecoveryCheckpointObjectFacts::manifest_inner(
                    facts.level,
                    facts.logical_length,
                    facts.allocated_bytes,
                )
            }
            .map_err(|_| ManifestTreeError::InvalidTree)?;
            objects.insert(object_id, facts);
        }
        Ok(CheckpointIncrement { objects, required })
    }

    pub(crate) fn install_recovery_checkpoint<F>(
        &self,
        record: CommitRecord,
        object_ids: &BTreeSet<MetadataObjectId>,
        mut read_object: F,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<RecoveredGeneration, GenerationError>
    where
        F: FnMut(MetadataObjectId) -> Result<std::sync::Arc<Vec<u8>>, RecoveryCheckpointError>,
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
                RecoveryCheckpointError::Io(error) => GenerationError::Io(error),
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
        self.invalidate_wal_writer_cache();
        self.recover_latest_using(Some(verifier))?
            .map(|graph| graph.generation)
            .ok_or(GenerationError::NoRecoverableGeneration)
    }
}

fn namespace_object_facts(
    encoded: &[u8],
) -> Result<RecoveryCheckpointObjectFacts, GenerationError> {
    let kind = match decode_namespace_object_references(encoded)? {
        NamespaceObjectReferences::GraphRoot(_) => RecoveryCheckpointObjectKind::NamespaceGraphRoot,
        NamespaceObjectReferences::InodeShard(_) => {
            RecoveryCheckpointObjectKind::NamespaceInodeShard
        }
        NamespaceObjectReferences::EntryShard => RecoveryCheckpointObjectKind::NamespaceEntryShard,
    };
    RecoveryCheckpointObjectFacts::namespace(kind)
        .map_err(|_| ManifestTreeError::InvalidTree.into())
}
