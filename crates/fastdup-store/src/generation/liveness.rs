//! Generation reachability, scrub summaries and fenced DATA-GC proof application.
use super::error::map_log_error;
use super::graph::record_matches_namespace_root;
use super::{
    GenerationError, GenerationLivenessDelta, GenerationLivenessProof, GenerationRepository,
    GenerationScrubSummary, WalTail,
};
use crate::generation_log::GenerationLog;
use crate::{ContainerRepository, StorageIo};
use fastdup_format::CommitRecord;
use std::collections::{BTreeMap, BTreeSet};

impl<I: StorageIo> GenerationRepository<I> {
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
        let _independent = crate::metadata_object_cache::IndependentRead::enter();
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
        let _cache_read = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
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
        proof.recovery_checkpoint_roots = self
            .recovery_checkpoint_root_pins
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pins poisoned during DATA proof")
            .keys()
            .copied()
            .collect();
        let recovery_checkpoint_roots = proof
            .recovery_checkpoint_roots
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for root_id in recovery_checkpoint_roots {
            let root = self.read_namespace_root(root_id)?;
            let (_, required) = self.scan_manifest_graph_with_required(&root)?;
            proof.extend_protected_chunks(required)?;
        }
        Ok(proof)
    }

    fn scan_generation_liveness(
        &self,
        audit_retained_history: bool,
    ) -> Result<GenerationLivenessProof, GenerationError> {
        let _independent = crate::metadata_object_cache::IndependentRead::enter();
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
                    self.check_maintenance()?;
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
            recovery_checkpoint_roots: BTreeSet::new(),
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

    pub(super) fn load_complete_commit_records_unlocked(
        &self,
    ) -> Result<Vec<CommitRecord>, GenerationError> {
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
        let recovery_checkpoint_roots = self
            .recovery_checkpoint_root_pins
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pins poisoned during GC revalidation")
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        if recovery_checkpoint_roots != proof.recovery_checkpoint_roots {
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
}
