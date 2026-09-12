//! Committed-prefix selection, compatibility checks and independently verified recovery graphs.
use super::error::map_log_error;
use super::graph::{record_matches_namespace_root, verify_generation_transition_pair};
use super::results::verified_files;
use super::{
    GenerationError, GenerationRepository, RecoveredDataGeneration, RecoveredGeneration,
    RequiredChunkVerifier, VerifiedManifests, WalTail,
};
use crate::generation_log::GenerationLog;
use crate::{ContainerRepository, StorageIo};
use fastdup_format::{CommitRecord, NamespaceRoot};
use std::collections::BTreeMap;

pub(super) struct RecoveredGraph {
    pub(super) generation: RecoveredGeneration,
    manifests: VerifiedManifests,
}

struct SelectedGraph {
    record: CommitRecord,
    root: NamespaceRoot,
    manifests: VerifiedManifests,
}

impl<I: StorageIo> GenerationRepository<I> {
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

    pub(super) fn recover_latest_using(
        &self,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<Option<RecoveredGraph>, GenerationError> {
        self.recover_latest_checked(&mut |required| {
            if required.is_empty() {
                return Ok(());
            }
            verifier
                .ok_or(GenerationError::DataLocationsNotConnected)?
                .verify_required_chunks(&required)
                .map_err(GenerationError::from)
        })
    }

    /// Opens demand-verifying readers after complete Metadata and Container
    /// structure validation. This does not prove payload readability. The caller
    /// must schedule a background scrub and exclude GC until it completes.
    /// A damaged newest graph fails closed instead of silently rolling it back.
    ///
    /// # Errors
    /// Returns graph, seal, dependency, compatibility or I/O failures.
    ///
    /// # Panics
    /// Panics if a prior invariant failure poisoned the commit lock.
    pub fn recover_latest_with_structural_files<J>(
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
            .expect("generation commit lock poisoned");
        let Some(graph) = self.recover_latest_checked(&mut |required| {
            containers
                .verify_required_chunk_structure(&required)
                .map_err(GenerationError::from)
        })?
        else {
            return Ok(None);
        };
        if graph.generation.rejected_newer_generations() != 0 {
            return Err(GenerationError::NoRecoverableGeneration);
        }
        let files = verified_files(graph.manifests, self, containers)?;
        Ok(Some(RecoveredDataGeneration {
            generation: graph.generation,
            files,
        }))
    }

    /// Selects the committed Metadata graph without visiting Container storage.
    /// The returned requirements must be checked by the owning runtime's initial
    /// scrub before it enables online deletion. Demand readers still verify DATA.
    /// The newest committed graph must be valid; no silent rollback is permitted.
    /// After validating that graph, an invalid WAL suffix is durably truncated
    /// under the commit lock. The accepted prefix is never rewritten.
    ///
    /// # Errors
    /// Returns Metadata, WAL, compatibility, allocation, or graph failures.
    ///
    /// # Panics
    /// Panics if a previous invariant failure poisoned the commit lock.
    pub fn recover_committed_for_mount<J>(
        &self,
        containers: &ContainerRepository<J>,
    ) -> Result<
        (
            Option<RecoveredDataGeneration<J>>,
            crate::PendingDataVerification,
        ),
        GenerationError,
    >
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("generation commit lock poisoned");
        let mut required = BTreeMap::new();
        let graph = self.recover_latest_checked(&mut |chunks| {
            required = chunks;
            Ok(())
        })?;
        let recovered = if let Some(mut graph) = graph {
            if graph.generation.rejected_newer_generations() != 0 {
                return Err(GenerationError::NoRecoverableGeneration);
            }
            if graph.generation.wal_tail != WalTail::Clean {
                GenerationLog::new(&self.storage)
                    .repair_tail(graph.generation.record)
                    .map_err(map_log_error)?;
                graph.generation.wal_tail = WalTail::Clean;
            }
            Some(RecoveredDataGeneration {
                generation: graph.generation,
                files: verified_files(graph.manifests, self, containers)?,
            })
        } else {
            None
        };
        Ok((recovered, crate::PendingDataVerification::new(required)))
    }

    fn recover_latest_checked(
        &self,
        verify: &mut impl FnMut(BTreeMap<fastdup_format::ChunkId, u64>) -> Result<(), GenerationError>,
    ) -> Result<Option<RecoveredGraph>, GenerationError> {
        let _independent = crate::metadata_object_cache::IndependentRead::enter();
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
                verify,
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

    pub(super) fn validate_recovery_transition_prefix(
        &self,
        records: &[CommitRecord],
    ) -> Result<Vec<CommitRecord>, GenerationError> {
        self.validate_record_compatibility(records)?;
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

    fn validate_record_compatibility(
        &self,
        records: &[CommitRecord],
    ) -> Result<(), GenerationError> {
        for record in records {
            if record.policy_set() != self.supported_policy {
                return Err(GenerationError::UnsupportedPolicySet {
                    generation: record.generation(),
                    policy_set: record.policy_set(),
                });
            }
        }
        Self::validate_format_epoch_compatibility(records)
    }

    pub(super) fn validate_format_epoch_compatibility(
        records: &[CommitRecord],
    ) -> Result<(), GenerationError> {
        for record in records {
            if record.format_epoch() != fastdup_format::CURRENT_REPOSITORY_FORMAT_EPOCH {
                return Err(GenerationError::UnsupportedFormatEpoch {
                    generation: record.generation(),
                    format_epoch: record.format_epoch(),
                });
            }
        }
        Ok(())
    }

    fn select_live_recovery_graph(
        &self,
        structurally_valid_records: &[CommitRecord],
        oldest_online_generation: u64,
        verify: &mut impl FnMut(BTreeMap<fastdup_format::ChunkId, u64>) -> Result<(), GenerationError>,
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
            let manifests = match self.scan_manifest_graph_with_required(&root).and_then(
                |(manifests, required)| {
                    verify(required)?;
                    Ok(manifests)
                },
            ) {
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
}
