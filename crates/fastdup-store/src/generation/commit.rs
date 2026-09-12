//! Serialized Namespace publication, predecessor fences and Commit-WAL durability ordering.
use super::error::map_log_error;
use super::metadata_gc::{
    classify_metadata_gc_additions, mark_metadata_gc_dirty, mark_metadata_gc_exact_required,
};
use super::results::verified_files;
use super::{
    CommittedDataGeneration, CommittedMetadata, GenerationError, GenerationRepository,
    ManifestSuccessorProof, MetadataGcExactReason, RequiredChunkVerifier, SuccessorPredecessor,
    WalTail,
};
use crate::generation_log::{GenerationLog, LogSnapshot};
use crate::{ContainerRepository, StorageIo};
use fastdup_format::{CommitRecord, CommitRecordHash, NamespaceRoot};
use std::collections::{BTreeMap, BTreeSet};

impl<I: StorageIo> GenerationRepository<I> {
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
        introduced_metadata.extend(committed.introduced_namespace_metadata.iter().copied());
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
        Self::validate_format_epoch_compatibility(snapshot.records())?;
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
            MetadataGcExactReason::UnclassifiedPublication,
        );
        Ok(committed.record)
    }

    fn commit_verified_namespace_from_snapshot_tracked(
        &self,
        root: &NamespaceRoot,
        snapshot: &LogSnapshot,
    ) -> Result<CommittedMetadata, GenerationError> {
        let encoded_graph = root.encode_graph()?;
        let mut introduced_namespace_metadata = BTreeSet::new();
        for shard in encoded_graph.shards() {
            let staged = self.stage_metadata_with_status(shard.bytes())?;
            if staged.object_id != shard.object_id() {
                return Err(GenerationError::PublishVerificationMismatch);
            }
            if staged.published_new {
                introduced_namespace_metadata.insert(staged.object_id);
            }
        }
        let staged_root = self.stage_metadata_with_status(encoded_graph.root())?;
        self.storage.sync_root()?;
        let root_id = staged_root.object_id;
        if staged_root.published_new {
            introduced_namespace_metadata.insert(root_id);
        }
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
            introduced_namespace_metadata,
            wal_rotated,
        })
    }
}
