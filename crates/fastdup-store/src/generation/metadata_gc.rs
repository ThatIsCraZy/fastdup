//! Exact Metadata marking, candidate validation, catalog deltas and conservative invalidation.
use super::error::map_log_error;
use super::graph::{record_matches_namespace_gc_graph, verify_generation_transition_pair_gc_graph};
use super::metadata::parse_metadata_name;
use super::{
    GenerationError, GenerationMetadataGcSummary, GenerationRepository,
    MAX_METADATA_MARK_DELTA_RUNS, MAX_METADATA_OBJECT_BYTES_U64, MetadataGcCleanState,
    MetadataGcDeltaJournal, MetadataGcExactReason, MetadataGcMarkMode, MetadataGcMetrics, WalTail,
};
use crate::StorageIo;
use crate::generation_log::GenerationLog;
use crate::manifest_tree::{ManifestTreeError, scan_manifest_tree};
use crate::metadata_mark_catalog::{
    audit_named as audit_metadata_mark_catalog,
    audit_named_collect as audit_metadata_mark_catalog_collect,
    commit_binding as metadata_mark_commit_binding,
    is_published_name as is_metadata_mark_catalog_name,
    parse_generation as parse_metadata_mark_generation, prepare as prepare_metadata_mark_catalog,
    prepare_addition as prepare_metadata_mark_addition,
};
use fastdup_format::{
    CommitRecord, MetadataMarkCatalogRunKind, MetadataObjectId, NamespaceGcGraph,
};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

struct MetadataGcInventory {
    candidates: Vec<(MetadataObjectId, String)>,
    catalog_names: Vec<String>,
    catalog_generation_high_water: u64,
}

struct MetadataMarkCatalogInventory {
    runs: Vec<(u64, String)>,
    high_water: u64,
}

/// Accumulator for one exact Metadata mark.
///
/// Traversal dedup uses `scanned_roots` rather than `reachable`. A Manifest
/// root can enter `reachable` without its subtree, so inferring "already
/// traversed" from `reachable` alone would silently drop every node below that
/// root and authorize unlinking live Metadata.
#[derive(Default)]
struct MetadataMarkSet {
    reachable: BTreeSet<MetadataObjectId>,
    scanned_roots: BTreeSet<MetadataObjectId>,
    bytes_read: u64,
}

impl MetadataMarkSet {
    fn add_bytes(&mut self, bytes: u64) -> Result<(), GenerationError> {
        self.bytes_read = self
            .bytes_read
            .checked_add(bytes)
            .ok_or(GenerationError::MetadataTooLarge)?;
        Ok(())
    }
}

impl<I: StorageIo> GenerationRepository<I> {
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
        let _cache_read = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
        self.check_maintenance()?;
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
        if let Some(summary) = self.try_compact_metadata_mark_catalog(started)? {
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
        // Exact collection supersedes every pre-barrier addition. Retire the
        // old journal and catalog tail BEFORE any fallible publication/unlink:
        // a pin may drain during I/O, or I/O may fail after removing an object
        // that a later writer legitimately republishes under the same ID.
        // Neither case may leave that ID in the previous addition journal.
        let exact_revision = {
            let mut journal = self
                .metadata_gc_delta
                .lock()
                .expect("ASSERT: Metadata GC delta journal poisoned before exact mark");
            journal.unclassified.clear();
            journal.additions.clear();
            journal.exact_required = true;
            journal.exact_reason = Some(exact_reason);
            advance_metadata_gc_journal_revision(&mut journal);
            journal.revision
        };
        *self
            .metadata_gc_clean
            .lock()
            .expect("ASSERT: Metadata GC clean-catalog state poisoned") = None;
        self.check_maintenance()?;
        let (records, mut mark) = self.load_and_mark_metadata_gc_records()?;
        let commit_binding = metadata_mark_commit_binding(&records);
        self.mark_metadata_gc_pins(&mut mark)?;
        let MetadataMarkSet {
            reachable,
            bytes_read: object_graph_read_bytes,
            ..
        } = mark;
        let inventory = self.inventory_metadata_gc(&reachable)?;
        let bytes_removed = self.verify_metadata_gc_candidates(&inventory.candidates)?;
        let catalog_generation = inventory
            .catalog_generation_high_water
            .checked_add(1)
            .ok_or(GenerationError::GenerationExhausted)?;
        self.check_maintenance()?;
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
            self.check_metadata_gc_unlink_stop()?;
            self.storage.remove_file(name)?;
        }
        for (object_id, name) in &inventory.candidates {
            self.check_metadata_gc_unlink_stop()?;
            assert!(
                MetadataGcMarkMode::ExactSnapshot.has_deletion_authority(),
                "ASSERT: only an exact Metadata mark can authorize object unlink"
            );
            assert!(
                !reachable.contains(object_id),
                "ASSERT: Metadata GC cannot unlink an object in its verified reachability set"
            );
            self.metadata_cache.invalidate(*object_id);
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
        let mut journal = self
            .metadata_gc_delta
            .lock()
            .expect("ASSERT: Metadata GC delta journal poisoned after exact mark");
        if journal.revision == exact_revision
            && self.metadata_gc_epoch.load(Ordering::Acquire) == mark_epoch
        {
            *journal = MetadataGcDeltaJournal::default();
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
        self.check_maintenance()?;
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

    #[allow(clippy::too_many_lines)]
    fn try_compact_metadata_mark_catalog(
        &self,
        started: Instant,
    ) -> Result<Option<GenerationMetadataGcSummary>, GenerationError> {
        let chain_candidate = self
            .metadata_gc_clean
            .lock()
            .expect("ASSERT: Metadata GC clean-catalog state poisoned during compaction selection")
            .is_some_and(|clean| clean.delta_run_count >= MAX_METADATA_MARK_DELTA_RUNS);
        if !chain_candidate {
            return Ok(None);
        }
        {
            let journal = self
                .metadata_gc_delta
                .lock()
                .expect("ASSERT: Metadata GC delta journal poisoned during compaction selection");
            if journal.exact_required || !journal.unclassified.is_empty() {
                return Ok(None);
            }
        }

        let barrier_started = Instant::now();
        let _publication_guard = self
            .metadata_gc_barrier
            .write()
            .expect("ASSERT: Metadata GC publication barrier poisoned during catalog compaction");
        let _commit_guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: Metadata GC generation lock poisoned during catalog compaction");
        let barrier_wait = barrier_started.elapsed();
        let mark_epoch = self.metadata_gc_epoch.load(Ordering::Acquire);
        let Some(clean) = self
            .metadata_gc_clean
            .lock()
            .expect("ASSERT: Metadata GC clean-catalog state poisoned during catalog compaction")
            .filter(|clean| clean.delta_run_count >= MAX_METADATA_MARK_DELTA_RUNS)
        else {
            return Ok(None);
        };
        let (journal_revision, additions) = {
            let journal = self
                .metadata_gc_delta
                .lock()
                .expect("ASSERT: Metadata GC delta journal poisoned during catalog compaction");
            if journal.exact_required || !journal.unclassified.is_empty() {
                return Ok(None);
            }
            (journal.revision, journal.additions.clone())
        };
        let Some(inventory) = self.metadata_mark_catalog_inventory()? else {
            return Ok(None);
        };
        if inventory.high_water != clean.catalog_generation {
            return Ok(None);
        }
        let mut reachable = BTreeSet::new();
        let mut catalog_read_bytes = 0_u64;
        let mut prior_generation = None;
        for (generation, name) in &inventory.runs {
            let Ok((descriptor, rows)) = audit_metadata_mark_catalog_collect(&self.storage, name)
            else {
                return Ok(None);
            };
            if descriptor.generation() != *generation {
                return Ok(None);
            }
            catalog_read_bytes = catalog_read_bytes
                .checked_add(descriptor.file_length())
                .ok_or(GenerationError::MetadataTooLarge)?;
            match descriptor.run_kind() {
                MetadataMarkCatalogRunKind::Snapshot => {}
                MetadataMarkCatalogRunKind::Addition
                    if descriptor.base_generation() == prior_generation.unwrap_or(0) => {}
                MetadataMarkCatalogRunKind::Addition => return Ok(None),
            }
            reachable.extend(rows);
            prior_generation = Some(*generation);
        }

        let records = self.load_complete_commit_records_unlocked()?;
        reachable.extend(additions.iter().copied());
        let row_count =
            u64::try_from(reachable.len()).map_err(|_| GenerationError::MetadataTooLarge)?;
        let catalog_generation = inventory
            .high_water
            .checked_add(1)
            .ok_or(GenerationError::GenerationExhausted)?;
        self.check_maintenance()?;
        *self
            .metadata_gc_clean
            .lock()
            .expect("ASSERT: Metadata GC clean-catalog state poisoned during catalog compaction") =
            None;
        let prepared = prepare_metadata_mark_catalog(
            &self.storage,
            catalog_generation,
            metadata_mark_commit_binding(&records),
            reachable.iter().copied(),
            row_count,
        )?;
        let published = prepared.publish(&self.storage)?;
        assert_eq!(
            published.generation(),
            catalog_generation,
            "ASSERT: compacted Metadata mark catalog publishes under its exact generation"
        );
        assert_eq!(
            published.run_kind(),
            MetadataMarkCatalogRunKind::Snapshot,
            "ASSERT: compacted Metadata mark catalog is a new snapshot run"
        );
        assert_eq!(
            published.row_count(),
            row_count,
            "ASSERT: compacted Metadata mark catalog covers its exact row set"
        );
        for (_, name) in &inventory.runs {
            self.check_metadata_gc_unlink_stop()?;
            self.storage.remove_file(name)?;
        }
        self.storage.sync_root()?;

        let mut journal = self
            .metadata_gc_delta
            .lock()
            .expect("ASSERT: Metadata GC delta journal poisoned after catalog compaction");
        for object_id in &additions {
            assert!(
                journal.additions.remove(object_id),
                "ASSERT: compacted Metadata addition identity remains journaled"
            );
        }
        if journal.revision == journal_revision
            && self.metadata_gc_epoch.load(Ordering::Acquire) == mark_epoch
        {
            *journal = MetadataGcDeltaJournal::default();
            *self.metadata_gc_clean.lock().expect(
                "ASSERT: Metadata GC clean-catalog state poisoned after catalog compaction",
            ) = Some(MetadataGcCleanState {
                epoch: mark_epoch,
                objects_retained: row_count,
                catalog_generation,
                delta_run_count: 0,
            });
        } else {
            journal.exact_required = true;
            if journal.exact_reason.is_none() {
                journal.exact_reason = Some(MetadataGcExactReason::DeltaChainLimit);
            }
        }
        Ok(Some(GenerationMetadataGcSummary {
            objects_removed: 0,
            bytes_removed: 0,
            objects_retained: row_count,
            mark_mode: MetadataGcMarkMode::CatalogCompaction,
            exact_reason: None,
            catalog_generation: Some(catalog_generation),
            metrics: MetadataGcMetrics {
                wall: started.elapsed(),
                barrier_wait,
                catalog_read_bytes,
                catalog_write_bytes: published.file_length(),
                root_syncs: 1,
                catalog_chain_runs: 1,
                ..MetadataGcMetrics::default()
            },
        }))
    }

    fn metadata_mark_catalog_inventory(
        &self,
    ) -> Result<Option<MetadataMarkCatalogInventory>, GenerationError> {
        let mut runs = Vec::new();
        let mut invalid = false;
        let mut inventory_error = None;
        self.storage.visit_names(&mut |name| {
            if inventory_error.is_some() || invalid || !is_metadata_mark_catalog_name(name) {
                return;
            }
            let Some(generation) = parse_metadata_mark_generation(name) else {
                invalid = true;
                return;
            };
            if runs.try_reserve(1).is_err() {
                inventory_error = Some(GenerationError::OutOfMemory);
                return;
            }
            runs.push((generation, name.to_owned()));
        })?;
        if let Some(error) = inventory_error {
            return Err(error);
        }
        if invalid {
            return Ok(None);
        }
        runs.sort_unstable_by_key(|entry| entry.0);
        let high_water = runs.last().map_or(0, |(generation, _)| *generation);
        Ok(Some(MetadataMarkCatalogInventory { runs, high_water }))
    }

    fn load_and_mark_metadata_gc_records(
        &self,
    ) -> Result<(Vec<CommitRecord>, MetadataMarkSet), GenerationError> {
        let Some(snapshot) = GenerationLog::new(&self.storage)
            .load_for_recovery()
            .map_err(map_log_error)?
        else {
            return Ok((Vec::new(), MetadataMarkSet::default()));
        };
        if snapshot.tail() != &WalTail::Clean {
            return Err(GenerationError::WalNeedsRepair(snapshot.tail().clone()));
        }
        for record in snapshot.records() {
            if record.policy_set() != self.supported_policy {
                return Err(GenerationError::UnsupportedPolicySet {
                    generation: record.generation(),
                    policy_set: record.policy_set(),
                });
            }
        }
        Self::validate_format_epoch_compatibility(snapshot.records())?;
        let mut mark = MetadataMarkSet::default();
        let mut previous: Option<(CommitRecord, Arc<NamespaceGcGraph>)> = None;
        for record in snapshot.records().iter().copied() {
            self.check_maintenance()?;
            mark.reachable.insert(record.namespace_root());
            let reused = previous.as_ref().is_some_and(|(previous_record, _)| {
                previous_record.namespace_root() == record.namespace_root()
            });
            let graph = if reused {
                Arc::clone(&previous.as_ref().expect("ASSERT: reused graph exists").1)
            } else {
                let (graph, graph_bytes) =
                    self.read_namespace_root_gc_graph(record.namespace_root())?;
                mark.add_bytes(graph_bytes)?;
                Arc::new(graph)
            };
            mark.reachable
                .extend(graph.namespace_object_ids().iter().copied());
            if !record_matches_namespace_gc_graph(record, &graph) {
                return Err(GenerationError::PreviousGenerationRecordMismatch);
            }
            match &previous {
                Some((previous_record, previous_graph)) => {
                    verify_generation_transition_pair_gc_graph(
                        *previous_record,
                        previous_graph,
                        &graph,
                    )?;
                }
                None if record.generation() == 1
                    && (graph.inode_allocation_cursor() != 2
                        || !graph.inode_transitions().is_empty()) =>
                {
                    return Err(GenerationError::PreviousGenerationRecordMismatch);
                }
                None => {}
            }
            for manifest_root in graph.manifest_roots() {
                self.scan_metadata_gc_manifest_root(*manifest_root, &mut mark)?;
            }
            previous = Some((record, graph));
        }
        Ok((snapshot.records().to_vec(), mark))
    }

    fn mark_metadata_gc_pins(&self, mark: &mut MetadataMarkSet) -> Result<(), GenerationError> {
        let pinned_roots = self
            .metadata_root_pins
            .lock()
            .expect("ASSERT: Metadata root pin registry poisoned during GC proof")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for root in pinned_roots {
            self.scan_metadata_gc_manifest_root(root, mark)?;
        }
        let recovery_roots = self
            .recovery_checkpoint_root_pins
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pins poisoned during Metadata mark")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for root_id in recovery_roots {
            if !mark.reachable.insert(root_id) {
                continue;
            }
            let (graph, namespace_bytes) = self.read_namespace_root_gc_graph(root_id)?;
            mark.reachable
                .extend(graph.namespace_object_ids().iter().copied());
            mark.add_bytes(namespace_bytes)?;
            for manifest_root in graph.manifest_roots() {
                self.scan_metadata_gc_manifest_root(*manifest_root, mark)?;
            }
        }
        Ok(())
    }

    fn scan_metadata_gc_manifest_root(
        &self,
        root: MetadataObjectId,
        mark: &mut MetadataMarkSet,
    ) -> Result<(), GenerationError> {
        if !mark.scanned_roots.insert(root) {
            return Ok(());
        }
        let traversal_probes = std::cell::Cell::new(0_u64);
        scan_manifest_tree(
            root,
            |node_id| {
                traversal_probes.set(traversal_probes.get() + 1);
                if traversal_probes.get().is_multiple_of(256) {
                    self.check_manifest_maintenance()?;
                }
                mark.reachable.insert(node_id);
                let bytes = self.read_manifest_node(node_id)?;
                mark.bytes_read = mark
                    .bytes_read
                    .checked_add(
                        u64::try_from(bytes.len())
                            .map_err(|_| ManifestTreeError::ArithmeticOverflow)?,
                    )
                    .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                Ok(bytes)
            },
            |_logical_offset, _extent| {
                traversal_probes.set(traversal_probes.get() + 1);
                if traversal_probes.get().is_multiple_of(256) {
                    self.check_manifest_maintenance()?;
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    fn check_metadata_gc_unlink_stop(&self) -> Result<(), GenerationError> {
        if let Err(stopped) = self.check_maintenance() {
            self.storage.sync_root()?;
            return Err(stopped.into());
        }
        Ok(())
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
            let result = self
                .check_maintenance()
                .map_err(GenerationError::from)
                .and_then(|()| inventory_metadata_name(&mut inventory, reachable, name));
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
            self.check_maintenance()?;
            // Only the reclaimed size is needed here. Reading each unreachable
            // object back would reprove the content addressing of bytes that
            // are about to be unlinked, which cannot change the unlink decision
            // that the reachability set already made.
            let length = self.storage.object_len(name)?;
            if length > MAX_METADATA_OBJECT_BYTES_U64 {
                return Err(GenerationError::MetadataIdentityCollision(*object_id));
            }
            bytes_removed = bytes_removed
                .checked_add(length)
                .ok_or(GenerationError::MetadataTooLarge)?;
        }
        Ok(bytes_removed)
    }
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

pub(super) fn mark_metadata_gc_dirty(epoch: &AtomicU64) {
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

pub(super) fn mark_metadata_gc_unclassified(
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

pub(super) fn mark_metadata_gc_exact_required(
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

pub(super) fn classify_metadata_gc_additions(
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
