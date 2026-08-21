//! Offline integrity audit and rebuild orchestration.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::thread;

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexFormatError, ExactIndexProfileId,
    ExactIndexRun, ExactIndexRunRef, ExactIndexRunSet, ExactIndexRunSetError,
    MAX_LOGICAL_CHUNK_BYTES, SealedContainer,
};

use crate::generation::GenerationGcScrubProof;
use crate::{
    ContainerAuditSummary, ContainerRepository, ExactIndexRunRepository, ExactIndexStoreError,
    GenerationError, GenerationRepository, StorageIo, StoreError,
};

const EXACT_INDEX_COMPACTION_FANIN: usize = 4;
const NORMAL_POOL_PERCENT: u64 = 90;
const NORMAL_RECLAIM_PERCENT: u64 = 20;
const BACKGROUND_NICE_INCREMENT: i32 = 10;
const GC_REPLACEMENT_LOGICAL_TARGET_BYTES: u64 = 48 * 1_024 * 1_024;
const GC_REPLACEMENT_CHUNK_LIMIT: usize = 32_768;
const GC_COMPRESSION_REGION_BYTES: usize = 512 * 1_024;
const GC_RAW_CHUNK_PHYSICAL_OVERHEAD_UPPER_BYTES: u64 = 383;
const GC_CONTAINER_FIXED_PHYSICAL_OVERHEAD_UPPER_BYTES: u64 = 12_351;

/// Scheduling class for one maintenance phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenancePriority {
    Background,
    Normal,
}

/// Exact pool occupancy observation used for maintenance scheduling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataPoolUsage {
    used_bytes: u64,
    capacity_bytes: u64,
}

impl DataPoolUsage {
    /// Constructs one bounded occupancy observation.
    ///
    /// # Errors
    ///
    /// Rejects zero capacity or used bytes above capacity.
    pub const fn new(used_bytes: u64, capacity_bytes: u64) -> Result<Self, DataPoolUsageError> {
        if capacity_bytes == 0 || used_bytes > capacity_bytes {
            return Err(DataPoolUsageError);
        }
        Ok(Self {
            used_bytes,
            capacity_bytes,
        })
    }

    #[must_use]
    pub const fn used_bytes(self) -> u64 {
        self.used_bytes
    }

    #[must_use]
    pub const fn capacity_bytes(self) -> u64 {
        self.capacity_bytes
    }

    #[must_use]
    pub fn scrub_priority(self) -> MaintenancePriority {
        if percentage_at_least(self.used_bytes, self.capacity_bytes, NORMAL_POOL_PERCENT) {
            MaintenancePriority::Normal
        } else {
            MaintenancePriority::Background
        }
    }

    #[must_use]
    pub fn gc_priority(self, reclaimable_bytes: u64, container_bytes: u64) -> MaintenancePriority {
        if self.scrub_priority() == MaintenancePriority::Normal
            || (container_bytes != 0
                && percentage_greater_than(
                    reclaimable_bytes,
                    container_bytes,
                    NORMAL_RECLAIM_PERCENT,
                ))
        {
            MaintenancePriority::Normal
        } else {
            MaintenancePriority::Background
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataPoolUsageError;

impl fmt::Display for DataPoolUsageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("data-pool usage requires 0 <= used <= nonzero capacity")
    }
}

impl std::error::Error for DataPoolUsageError {}

fn percentage_at_least(numerator: u64, denominator: u64, percent: u64) -> bool {
    debug_assert!(denominator != 0);
    u128::from(numerator) * 100 >= u128::from(denominator) * u128::from(percent)
}

fn percentage_greater_than(numerator: u64, denominator: u64, percent: u64) -> bool {
    debug_assert!(denominator != 0);
    u128::from(numerator) * 100 > u128::from(denominator) * u128::from(percent)
}

/// One maintenance owner over the Namespace, DATA, and Exact-Index stores.
///
/// Scrub is read-only. Rebuild methods use Redirect-on-Write publication and
/// activate only after every replacement dependency has been verified.
#[derive(Clone, Debug)]
pub struct MaintenanceRepository<M, C, X> {
    generations: GenerationRepository<M>,
    containers: ContainerRepository<C>,
    indexes: ExactIndexRunRepository<X>,
    exact_profile: ExactIndexProfileId,
    rebuild_lock: Arc<Mutex<()>>,
}

impl<M, C, X> MaintenanceRepository<M, C, X> {
    #[must_use]
    pub fn new(
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        indexes: ExactIndexRunRepository<X>,
        exact_profile: ExactIndexProfileId,
    ) -> Self {
        Self {
            generations,
            containers,
            indexes,
            exact_profile,
            rebuild_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl<M, C, X> MaintenanceRepository<M, C, X>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
    X: Clone + StorageIo,
{
    /// Starts a detached coordinator that runs Scrub and subsequent GC on
    /// dedicated maintenance threads.
    ///
    /// Background phases run at Unix nice +10. A pool at or above 90% starts
    /// Scrub at normal priority. GC runs at normal priority when that same
    /// watermark is active or the successful Scrub proves more than 20% of
    /// Container bytes immediately reclaimable.
    ///
    /// # Errors
    ///
    /// Returns a thread-creation failure before any maintenance work starts.
    pub fn start_scrub_and_gc(
        &self,
        pool_usage: DataPoolUsage,
    ) -> Result<BackgroundMaintenanceJob, MaintenanceError>
    where
        M: Send + 'static,
        C: Send + 'static,
        X: Send + Sync + 'static,
    {
        let repository = self.clone();
        let scrub_priority = pool_usage.scrub_priority();
        let worker = thread::Builder::new()
            .name("fastdup-maintenance".to_owned())
            .spawn(move || {
                let scrub_repository = repository.clone();
                let plan = run_at_priority(scrub_priority, "fastdup-scrub", move || {
                    scrub_repository.scrub_for_gc(pool_usage)
                })?;
                let scrub = plan.scrub_report();
                let gc_priority = plan.gc_priority();
                let gc = run_at_priority(gc_priority, "fastdup-gc", move || {
                    repository.garbage_collect(plan)
                })?;
                Ok(BackgroundMaintenanceReport {
                    scrub,
                    scrub_priority,
                    gc,
                })
            })
            .map_err(MaintenanceError::MaintenanceThread)?;
        Ok(BackgroundMaintenanceJob {
            scrub_priority,
            worker: Some(worker),
        })
    }

    /// Audits every published Container, the newest live Namespace graph and,
    /// when present, the complete selected Exact-Index object graph.
    ///
    /// # Errors
    ///
    /// Returns the first storage, durable-format, graph, or index-integrity
    /// failure. Selecting an older recovery generation is a scrub failure,
    /// even though ordinary crash recovery may safely perform that fallback.
    pub fn scrub(&self) -> Result<EndToEndScrubReport, MaintenanceError> {
        let containers = self.containers.audit_published()?;
        let generations = self.generations.scrub_all_with_data(&self.containers)?;
        let index_audit = self.indexes.audit_active_locations(&self.containers)?;
        if index_audit.is_some_and(|audit| audit.activation().profile() != self.exact_profile) {
            return Err(MaintenanceError::ExactProfileMismatch);
        }
        Ok(EndToEndScrubReport {
            commit_generations_verified: generations.generations(),
            commit_generation: generations.latest_generation(),
            namespace_inodes: generations.latest_namespace_inodes(),
            manifest_files: generations.latest_manifest_files(),
            containers,
            exact_activation_generation: index_audit.map(|audit| audit.activation().generation()),
            exact_active_locations_verified: index_audit
                .map_or(0, crate::ExactIndexLocationAudit::active_locations),
        })
    }

    /// Runs the complete scrub and returns an opaque, generation-bound GC
    /// capability plus exact reclaim accounting.
    ///
    /// Completely unreachable Containers become direct removal candidates.
    /// Two or more mixed Containers become a Compaction Victim Set only when
    /// their unique uncovered live Chunks are predicted to occupy fewer
    /// bounded replacement Containers.
    ///
    /// # Errors
    ///
    /// Returns the first scrub, allocation, identity, or arithmetic failure.
    pub fn scrub_for_gc(
        &self,
        pool_usage: DataPoolUsage,
    ) -> Result<GarbageCollectionPlan, MaintenanceError> {
        let generation_proof = self.generations.scrub_all_for_gc(&self.containers)?;
        let online_chunks = generation_proof.online_chunks();
        let inventory = self.plan_container_gc(online_chunks)?;
        let index_audit = self.indexes.audit_active_locations(&self.containers)?;
        if index_audit.is_some_and(|audit| audit.activation().profile() != self.exact_profile) {
            return Err(MaintenanceError::ExactProfileMismatch);
        }
        let scrub = EndToEndScrubReport {
            commit_generations_verified: generation_proof.summary().generations(),
            commit_generation: generation_proof.summary().latest_generation(),
            namespace_inodes: generation_proof.summary().latest_namespace_inodes(),
            manifest_files: generation_proof.summary().latest_manifest_files(),
            containers: inventory.containers,
            exact_activation_generation: index_audit.map(|audit| audit.activation().generation()),
            exact_active_locations_verified: index_audit
                .map_or(0, crate::ExactIndexLocationAudit::active_locations),
        };
        let gc_priority = pool_usage.gc_priority(
            inventory.estimated_reclaimable_bytes,
            inventory.containers.file_bytes(),
        );
        Ok(GarbageCollectionPlan {
            scrub,
            generation_proof,
            exact_profile: self.exact_profile,
            reclaimable: inventory.reclaimable,
            reclaimable_bytes: inventory.reclaimable_bytes,
            estimated_reclaimable_bytes: inventory.estimated_reclaimable_bytes,
            compaction_victims: inventory.compaction_victims,
            compaction_victim_bytes: inventory.compaction_victim_bytes,
            replacement_chunks: inventory.replacement_chunks,
            partially_live_containers: inventory.partially_live_containers,
            pool_usage,
            gc_priority,
        })
    }

    fn plan_container_gc(
        &self,
        online_chunks: &BTreeMap<ChunkId, u64>,
    ) -> Result<ContainerGcInventory, MaintenanceError> {
        let mut inventory = ContainerGcInventory::default();
        let mut partial = BTreeMap::new();
        let mut partial_coverage = BTreeSet::new();
        let mut retained_coverage = BTreeSet::new();
        let containers = self
            .containers
            .visit_verified_published_pipelined::<MaintenanceError, _>(|container| {
                classify_container(
                    container,
                    online_chunks,
                    &mut inventory,
                    &mut partial,
                    &mut partial_coverage,
                    &mut retained_coverage,
                )
            })?;
        inventory.containers = containers;
        for chunk_id in partial_coverage {
            if retained_coverage.contains(&chunk_id) {
                continue;
            }
            let logical_length = online_chunks
                .get(&chunk_id)
                .copied()
                .expect("ASSERT: partial coverage originates from the online Chunk map");
            inventory
                .replacement_chunks
                .insert(chunk_id, logical_length);
        }
        let estimated = replacement_container_count_upper_bound(&inventory.replacement_chunks)?;
        if inventory.replacement_chunks.is_empty()
            || (partial.len() >= 2 && estimated < partial.len())
        {
            for (key, (container_id, file_length)) in partial {
                inventory.compaction_victims.insert(key, container_id);
                inventory.compaction_victim_bytes = inventory
                    .compaction_victim_bytes
                    .checked_add(file_length)
                    .ok_or(MaintenanceError::ArithmeticOverflow)?;
            }
        } else {
            inventory.replacement_chunks.clear();
        }
        let replacement_upper = replacement_file_bytes_upper_bound(&inventory.replacement_chunks)?;
        inventory.estimated_reclaimable_bytes = inventory
            .reclaimable_bytes
            .checked_add(
                inventory
                    .compaction_victim_bytes
                    .saturating_sub(replacement_upper),
            )
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        Ok(inventory)
    }

    /// Publishes every required replacement Container, activates an Exact Index
    /// that excludes every planned victim, then removes and directory-syncs
    /// those exact names.
    ///
    /// # Errors
    ///
    /// Rejects a plan from another profile or a plan whose current/previous
    /// online generations changed after scrub. No Container is removed before
    /// replacement index activation completes.
    ///
    /// # Panics
    ///
    /// Panics only if immutable candidate lengths change between successful
    /// scrub and the mandatory deletion reread, an impossible internal state.
    pub fn garbage_collect(
        &self,
        plan: GarbageCollectionPlan,
    ) -> Result<GarbageCollectionReport, MaintenanceError> {
        let GarbageCollectionPlan {
            scrub,
            generation_proof,
            exact_profile,
            reclaimable,
            reclaimable_bytes,
            compaction_victims,
            compaction_victim_bytes,
            replacement_chunks,
            gc_priority,
            ..
        } = plan;
        if exact_profile != self.exact_profile {
            return Err(MaintenanceError::GcPlanProfileMismatch);
        }
        if !self.generations.gc_proof_is_current(&generation_proof)? {
            return Err(MaintenanceError::StaleGcPlan);
        }
        if reclaimable.is_empty() && compaction_victims.is_empty() {
            return Ok(GarbageCollectionReport {
                containers_removed: 0,
                bytes_removed: 0,
                replacement_containers: 0,
                replacement_bytes: 0,
                chunks_relocated: 0,
                priority: gc_priority,
            });
        }
        let first_replacement_generation = scrub
            .container_generation_high_water()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        let replacements = self.publish_gc_replacements(
            &compaction_victims,
            &replacement_chunks,
            first_replacement_generation,
        )?;
        if !self.generations.gc_proof_is_current(&generation_proof)? {
            return Err(MaintenanceError::StaleGcPlan);
        }
        let mut removal_candidates = reclaimable;
        for (key, container_id) in &compaction_victims {
            let previous = removal_candidates.insert(*key, *container_id);
            assert!(
                previous.is_none(),
                "ASSERT: a Container cannot be both fully unreachable and partially live"
            );
        }
        self.rebuild_exact_index_excluding(&removal_candidates)?;
        if !self.generations.gc_proof_is_current(&generation_proof)? {
            return Err(MaintenanceError::StaleGcPlan);
        }
        let bytes_removed = self
            .containers
            .remove_verified_published(&removal_candidates)?;
        let expected_removed = reclaimable_bytes
            .checked_add(compaction_victim_bytes)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        assert_eq!(
            bytes_removed, expected_removed,
            "ASSERT: GC deletion reread must match scrub byte accounting"
        );
        Ok(GarbageCollectionReport {
            containers_removed: u64::try_from(removal_candidates.len())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?,
            bytes_removed,
            replacement_containers: replacements.containers,
            replacement_bytes: replacements.bytes,
            chunks_relocated: replacements.chunks,
            priority: gc_priority,
        })
    }

    fn publish_gc_replacements(
        &self,
        victims: &BTreeMap<[u8; 16], ContainerId>,
        required: &BTreeMap<ChunkId, u64>,
        first_generation: u64,
    ) -> Result<ReplacementPublication, MaintenanceError> {
        if required.is_empty() {
            return Ok(ReplacementPublication::default());
        }
        let mut seen = BTreeSet::new();
        let mut batch = Vec::<Vec<u8>>::new();
        let mut batch_bytes = 0_u64;
        let mut generation = first_generation;
        let mut published = ReplacementPublication::default();
        for container_id in victims.values().copied() {
            let container = self.containers.read(container_id)?;
            for record in container.records() {
                let chunk_id = record.chunk_id();
                let Some(expected_length) = required.get(&chunk_id).copied() else {
                    continue;
                };
                if !seen.insert(chunk_id) {
                    continue;
                }
                let observed_length = u64::try_from(record.payload().len())
                    .map_err(|_| MaintenanceError::ArithmeticOverflow)?;
                if observed_length != expected_length {
                    return Err(MaintenanceError::OnlineChunkLengthMismatch {
                        chunk_id,
                        expected: expected_length,
                        observed: observed_length,
                    });
                }
                let would_exceed_bytes = batch_bytes
                    .checked_add(observed_length)
                    .ok_or(MaintenanceError::ArithmeticOverflow)?
                    > GC_REPLACEMENT_LOGICAL_TARGET_BYTES;
                if !batch.is_empty()
                    && (would_exceed_bytes || batch.len() == GC_REPLACEMENT_CHUNK_LIMIT)
                {
                    let completed = self.publish_gc_replacement_batch(generation, &batch)?;
                    published.add(completed)?;
                    generation = generation
                        .checked_add(1)
                        .ok_or(MaintenanceError::ArithmeticOverflow)?;
                    batch.clear();
                    batch_bytes = 0;
                }
                batch_bytes = batch_bytes
                    .checked_add(observed_length)
                    .ok_or(MaintenanceError::ArithmeticOverflow)?;
                batch
                    .try_reserve(1)
                    .map_err(|_| MaintenanceError::OutOfMemory)?;
                let mut payload = Vec::new();
                payload
                    .try_reserve_exact(record.payload().len())
                    .map_err(|_| MaintenanceError::OutOfMemory)?;
                payload.extend_from_slice(record.payload());
                batch.push(payload);
            }
        }
        if !batch.is_empty() {
            published.add(self.publish_gc_replacement_batch(generation, &batch)?)?;
        }
        if seen.len() != required.len() {
            return Err(MaintenanceError::MissingReplacementChunk);
        }
        assert_eq!(
            published.chunks,
            u64::try_from(required.len()).expect("ASSERT: replacement Chunk count fits u64"),
            "ASSERT: every required replacement Chunk is published exactly once"
        );
        let planned_upper = replacement_container_count_upper_bound(required)?;
        assert!(
            usize::try_from(published.containers).is_ok_and(|count| count <= planned_upper),
            "ASSERT: replacement publication cannot exceed the scrub planner's order-independent bound"
        );
        Ok(published)
    }

    fn publish_gc_replacement_batch(
        &self,
        generation: u64,
        chunks: &[Vec<u8>],
    ) -> Result<ReplacementPublication, MaintenanceError> {
        assert!(
            !chunks.is_empty(),
            "ASSERT: GC never publishes an empty Container"
        );
        let container_id = gc_replacement_container_id(generation, chunks)?;
        let regions = gc_compression_regions(chunks)?;
        let mut region_refs = Vec::new();
        region_refs
            .try_reserve_exact(regions.len())
            .map_err(|_| MaintenanceError::OutOfMemory)?;
        region_refs.extend(regions.iter().map(Vec::as_slice));
        let workers = thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        let verified = self.containers.publish_gc_replacement_adaptive_verified(
            container_id,
            generation,
            &region_refs,
            workers,
        )?;
        assert_eq!(
            verified.chunk_count(),
            chunks.len(),
            "ASSERT: replacement writer reread must cover the planned Chunk batch"
        );
        Ok(ReplacementPublication {
            containers: 1,
            bytes: verified.header().layout().file_length,
            chunks: u64::try_from(chunks.len())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?,
        })
    }

    /// Rebuilds the complete Exact Index from fully verified immutable
    /// Containers and atomically activates the replacement Run Set.
    ///
    /// Each Container is released before the next one is decoded. Hidden
    /// immutable Runs and bounded-fanin compactions are published while the
    /// scan advances; none is visible to lookup until the final activation-log
    /// sync succeeds.
    ///
    /// # Errors
    ///
    /// Returns the first Container, format, Run publication, compaction,
    /// activation, allocation, or checked-arithmetic failure. Previously
    /// active index state remains selected before the final commit point.
    ///
    /// # Panics
    ///
    /// Panics if a prior internal invariant panic poisoned the process-local
    /// rebuild lock.
    pub fn rebuild_exact_index(&self) -> Result<ExactIndexRebuildReport, MaintenanceError> {
        self.rebuild_exact_index_excluding(&BTreeMap::new())
    }

    fn rebuild_exact_index_excluding(
        &self,
        excluded: &BTreeMap<[u8; 16], ContainerId>,
    ) -> Result<ExactIndexRebuildReport, MaintenanceError> {
        let _guard = self
            .rebuild_lock
            .lock()
            .expect("ASSERT: Exact-Index rebuild lock poisoned");
        let previous = self.indexes.recover_active()?;
        if previous
            .as_ref()
            .is_some_and(|active| active.record().profile() != self.exact_profile)
        {
            return Err(MaintenanceError::ExactProfileMismatch);
        }
        let run_set_generation = previous.as_ref().map_or(Ok(1), |active| {
            active
                .run_set()
                .generation()
                .checked_add(1)
                .ok_or(MaintenanceError::ArithmeticOverflow)
        })?;
        let mut newest_run_generation = self
            .indexes
            .discover_run_generation_high_water(self.exact_profile)?
            .unwrap_or(0);
        let mut run_refs = Vec::new();
        let mut entries_rebuilt = 0_u64;
        let containers = self
            .containers
            .visit_verified_published_pipelined::<MaintenanceError, _>(|container| {
                if excluded.contains_key(&container.header().container_id().bytes()) {
                    return Ok(());
                }
                let mut entries = Vec::new();
                entries
                    .try_reserve_exact(container.locations().len())
                    .map_err(|_| MaintenanceError::OutOfMemory)?;
                for location in container.locations().iter().copied() {
                    entries.push(ExactIndexEntry::from_verified(location)?);
                }
                entries_rebuilt = entries_rebuilt
                    .checked_add(
                        u64::try_from(entries.len())
                            .map_err(|_| MaintenanceError::ArithmeticOverflow)?,
                    )
                    .ok_or(MaintenanceError::ArithmeticOverflow)?;
                if entries.is_empty() {
                    return Ok(());
                }
                newest_run_generation = newest_run_generation
                    .checked_add(1)
                    .ok_or(MaintenanceError::ArithmeticOverflow)?;
                let run = ExactIndexRun::new(self.exact_profile, newest_run_generation, entries)?;
                let descriptor = self.indexes.publish(&run)?;
                run_refs
                    .try_reserve(1)
                    .map_err(|_| MaintenanceError::OutOfMemory)?;
                run_refs.push(ExactIndexRunRef::new(0, descriptor)?);
                while let Some((source_level, inputs)) = select_compaction_inputs(&run_refs) {
                    let first_output_generation = newest_run_generation
                        .checked_add(1)
                        .ok_or(MaintenanceError::ArithmeticOverflow)?;
                    let target_level = source_level
                        .checked_add(1)
                        .ok_or(MaintenanceError::ArithmeticOverflow)?;
                    let compacted = self.indexes.compact_family(
                        &inputs,
                        target_level,
                        first_output_generation,
                    )?;
                    newest_run_generation = compacted.last_generation();
                    run_refs.retain(|run| {
                        !inputs
                            .iter()
                            .any(|input| input.generation() == run.generation())
                    });
                    run_refs
                        .try_reserve(compacted.runs().len())
                        .map_err(|_| MaintenanceError::OutOfMemory)?;
                    run_refs.extend_from_slice(compacted.runs());
                }
                Ok(())
            })?;
        let run_set = ExactIndexRunSet::new(self.exact_profile, run_set_generation, run_refs)?;
        self.indexes.audit_run_set_global_invariants(&run_set)?;
        let physical_runs = run_set.runs().len();
        let run_families = run_set.family_count();
        let active = self.indexes.activate(&run_set)?;
        Ok(ExactIndexRebuildReport {
            containers_scanned: containers.containers(),
            entries_rebuilt,
            run_families,
            physical_runs,
            run_set_generation,
            activation_generation: active.record().generation(),
        })
    }
}

#[derive(Debug, Default)]
struct ContainerGcInventory {
    containers: ContainerAuditSummary,
    reclaimable: BTreeMap<[u8; 16], ContainerId>,
    reclaimable_bytes: u64,
    estimated_reclaimable_bytes: u64,
    compaction_victims: BTreeMap<[u8; 16], ContainerId>,
    compaction_victim_bytes: u64,
    replacement_chunks: BTreeMap<ChunkId, u64>,
    partially_live_containers: u64,
}

fn classify_container(
    container: &SealedContainer,
    online_chunks: &BTreeMap<ChunkId, u64>,
    inventory: &mut ContainerGcInventory,
    partial: &mut BTreeMap<[u8; 16], (ContainerId, u64)>,
    partial_coverage: &mut BTreeSet<ChunkId>,
    retained_coverage: &mut BTreeSet<ChunkId>,
) -> Result<(), MaintenanceError> {
    let mut live = 0_usize;
    let mut live_chunks = Vec::new();
    for record in container.records() {
        if let Some(expected_length) = online_chunks.get(&record.chunk_id()).copied() {
            let observed_length = u64::try_from(record.payload().len())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?;
            if observed_length != expected_length {
                return Err(MaintenanceError::OnlineChunkLengthMismatch {
                    chunk_id: record.chunk_id(),
                    expected: expected_length,
                    observed: observed_length,
                });
            }
            live = live
                .checked_add(1)
                .ok_or(MaintenanceError::ArithmeticOverflow)?;
            live_chunks
                .try_reserve(1)
                .map_err(|_| MaintenanceError::OutOfMemory)?;
            live_chunks.push(record.chunk_id());
        }
    }
    let container_id = container.header().container_id();
    if live == 0 {
        inventory
            .reclaimable
            .insert(container_id.bytes(), container_id);
        inventory.reclaimable_bytes = inventory
            .reclaimable_bytes
            .checked_add(container.header().layout().file_length)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
    } else if live < container.records().len() {
        inventory.partially_live_containers = inventory
            .partially_live_containers
            .checked_add(1)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        partial.insert(
            container_id.bytes(),
            (container_id, container.header().layout().file_length),
        );
        partial_coverage.extend(live_chunks);
    } else {
        retained_coverage.extend(live_chunks);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReplacementPublication {
    containers: u64,
    bytes: u64,
    chunks: u64,
}

impl ReplacementPublication {
    fn add(&mut self, other: Self) -> Result<(), MaintenanceError> {
        self.containers = self
            .containers
            .checked_add(other.containers)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        self.chunks = self
            .chunks
            .checked_add(other.chunks)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        Ok(())
    }
}

fn replacement_container_count_upper_bound(
    chunks: &BTreeMap<ChunkId, u64>,
) -> Result<usize, MaintenanceError> {
    if chunks.is_empty() {
        return Ok(0);
    }
    let logical_bytes = chunks.values().try_fold(0_u64, |total, length| {
        total
            .checked_add(*length)
            .ok_or(MaintenanceError::ArithmeticOverflow)
    })?;
    let maximum_chunk =
        u64::try_from(MAX_LOGICAL_CHUNK_BYTES).map_err(|_| MaintenanceError::ArithmeticOverflow)?;
    let byte_closure_floor = GC_REPLACEMENT_LOGICAL_TARGET_BYTES
        .checked_sub(maximum_chunk)
        .ok_or(MaintenanceError::ArithmeticOverflow)?;
    let byte_closures = logical_bytes / byte_closure_floor;
    let count_closures = chunks.len() / GC_REPLACEMENT_CHUNK_LIMIT;
    usize::try_from(byte_closures)
        .map_err(|_| MaintenanceError::ArithmeticOverflow)?
        .checked_add(count_closures)
        .and_then(|closures| closures.checked_add(1))
        .ok_or(MaintenanceError::ArithmeticOverflow)
}

fn replacement_file_bytes_upper_bound(
    chunks: &BTreeMap<ChunkId, u64>,
) -> Result<u64, MaintenanceError> {
    let logical_bytes = chunks.values().try_fold(0_u64, |total, length| {
        total
            .checked_add(*length)
            .ok_or(MaintenanceError::ArithmeticOverflow)
    })?;
    let chunk_count =
        u64::try_from(chunks.len()).map_err(|_| MaintenanceError::ArithmeticOverflow)?;
    let container_count = u64::try_from(replacement_container_count_upper_bound(chunks)?)
        .map_err(|_| MaintenanceError::ArithmeticOverflow)?;
    logical_bytes
        .checked_add(
            chunk_count
                .checked_mul(GC_RAW_CHUNK_PHYSICAL_OVERHEAD_UPPER_BYTES)
                .ok_or(MaintenanceError::ArithmeticOverflow)?,
        )
        .and_then(|total| {
            total.checked_add(
                container_count.checked_mul(GC_CONTAINER_FIXED_PHYSICAL_OVERHEAD_UPPER_BYTES)?,
            )
        })
        .ok_or(MaintenanceError::ArithmeticOverflow)
}

fn gc_replacement_container_id(
    generation: u64,
    chunks: &[Vec<u8>],
) -> Result<ContainerId, MaintenanceError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"fastdup-gc-replacement-container-v1\0");
    hasher.update(&generation.to_le_bytes());
    for chunk in chunks {
        hasher.update(&ChunkId::of(chunk).bytes());
        hasher.update(
            &u64::try_from(chunk.len())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?
                .to_le_bytes(),
        );
    }
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    if bytes == [0; 16] {
        bytes[15] = 1;
    }
    ContainerId::new(bytes).map_err(|_| MaintenanceError::ReplacementIdentity)
}

fn gc_compression_regions(chunks: &[Vec<u8>]) -> Result<Vec<Vec<&[u8]>>, MaintenanceError> {
    let mut regions = Vec::<Vec<&[u8]>>::new();
    let mut current = Vec::new();
    let mut current_bytes = 0_usize;
    for chunk in chunks {
        let would_exceed = current_bytes
            .checked_add(chunk.len())
            .ok_or(MaintenanceError::ArithmeticOverflow)?
            > GC_COMPRESSION_REGION_BYTES;
        if !current.is_empty() && would_exceed {
            regions
                .try_reserve(1)
                .map_err(|_| MaintenanceError::OutOfMemory)?;
            regions.push(current);
            current = Vec::new();
            current_bytes = 0;
        }
        current_bytes = current_bytes
            .checked_add(chunk.len())
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        current
            .try_reserve(1)
            .map_err(|_| MaintenanceError::OutOfMemory)?;
        current.push(chunk.as_slice());
    }
    if !current.is_empty() {
        regions
            .try_reserve(1)
            .map_err(|_| MaintenanceError::OutOfMemory)?;
        regions.push(current);
    }
    Ok(regions)
}

fn run_at_priority<R, F>(
    priority: MaintenancePriority,
    name: &str,
    work: F,
) -> Result<R, MaintenanceError>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, MaintenanceError> + Send + 'static,
{
    if priority == MaintenancePriority::Normal {
        return work();
    }
    thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            rustix::process::nice(BACKGROUND_NICE_INCREMENT)
                .map_err(|error| MaintenanceError::BackgroundPriority(io::Error::from(error)))?;
            work()
        })
        .map_err(MaintenanceError::MaintenanceThread)?
        .join()
        .map_err(|_| MaintenanceError::MaintenanceThreadPanicked)?
}

fn select_compaction_inputs(runs: &[ExactIndexRunRef]) -> Option<(u16, Vec<ExactIndexRunRef>)> {
    let mut by_level = BTreeMap::<u16, BTreeMap<u64, Vec<ExactIndexRunRef>>>::new();
    for run in runs.iter().copied() {
        by_level
            .entry(run.level())
            .or_default()
            .entry(run.family_generation())
            .or_default()
            .push(run);
    }
    for (level, families) in by_level {
        if families.len() < EXACT_INDEX_COMPACTION_FANIN {
            continue;
        }
        let mut candidates = Vec::new();
        for (_, mut family) in families.into_iter().take(EXACT_INDEX_COMPACTION_FANIN) {
            family.sort_unstable_by_key(|run| run.partition_ordinal());
            candidates.extend(family);
        }
        return Some((level, candidates));
    }
    None
}

/// Compact evidence from one successful full Exact-Index rebuild.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactIndexRebuildReport {
    containers_scanned: u64,
    entries_rebuilt: u64,
    run_families: usize,
    physical_runs: usize,
    run_set_generation: u64,
    activation_generation: u64,
}

impl ExactIndexRebuildReport {
    #[must_use]
    pub const fn containers_scanned(self) -> u64 {
        self.containers_scanned
    }

    #[must_use]
    pub const fn entries_rebuilt(self) -> u64 {
        self.entries_rebuilt
    }

    #[must_use]
    pub const fn run_families(self) -> usize {
        self.run_families
    }

    #[must_use]
    pub const fn physical_runs(self) -> usize {
        self.physical_runs
    }

    #[must_use]
    pub const fn run_set_generation(self) -> u64 {
        self.run_set_generation
    }

    #[must_use]
    pub const fn activation_generation(self) -> u64 {
        self.activation_generation
    }
}

/// Compact, payload-free evidence returned by one successful scrub.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndToEndScrubReport {
    commit_generations_verified: usize,
    commit_generation: Option<u64>,
    namespace_inodes: usize,
    manifest_files: usize,
    containers: ContainerAuditSummary,
    exact_activation_generation: Option<u64>,
    exact_active_locations_verified: u64,
}

impl EndToEndScrubReport {
    #[must_use]
    pub const fn commit_generations_verified(self) -> usize {
        self.commit_generations_verified
    }

    #[must_use]
    pub const fn commit_generation(self) -> Option<u64> {
        self.commit_generation
    }

    #[must_use]
    pub const fn namespace_inodes(self) -> usize {
        self.namespace_inodes
    }

    #[must_use]
    pub const fn manifest_files(self) -> usize {
        self.manifest_files
    }

    #[must_use]
    pub const fn containers(self) -> u64 {
        self.containers.containers()
    }

    #[must_use]
    pub const fn container_chunks(self) -> u64 {
        self.containers.chunks()
    }

    #[must_use]
    pub const fn container_generation_high_water(self) -> Option<u64> {
        self.containers.generation_high_water()
    }

    #[must_use]
    pub const fn exact_activation_generation(self) -> Option<u64> {
        self.exact_activation_generation
    }

    #[must_use]
    pub const fn exact_active_locations_verified(self) -> u64 {
        self.exact_active_locations_verified
    }
}

/// Opaque deletion authority produced only by one complete successful scrub.
#[derive(Debug)]
pub struct GarbageCollectionPlan {
    scrub: EndToEndScrubReport,
    generation_proof: GenerationGcScrubProof,
    exact_profile: ExactIndexProfileId,
    reclaimable: BTreeMap<[u8; 16], ContainerId>,
    reclaimable_bytes: u64,
    estimated_reclaimable_bytes: u64,
    compaction_victims: BTreeMap<[u8; 16], ContainerId>,
    compaction_victim_bytes: u64,
    replacement_chunks: BTreeMap<ChunkId, u64>,
    partially_live_containers: u64,
    pool_usage: DataPoolUsage,
    gc_priority: MaintenancePriority,
}

impl GarbageCollectionPlan {
    #[must_use]
    pub const fn scrub_report(&self) -> EndToEndScrubReport {
        self.scrub
    }

    #[must_use]
    pub fn scrub_priority(&self) -> MaintenancePriority {
        self.pool_usage.scrub_priority()
    }

    #[must_use]
    pub const fn gc_priority(&self) -> MaintenancePriority {
        self.gc_priority
    }

    #[must_use]
    pub fn reclaimable_containers(&self) -> usize {
        self.reclaimable.len()
    }

    #[must_use]
    pub const fn reclaimable_bytes(&self) -> u64 {
        self.reclaimable_bytes
    }

    /// Conservative physical-byte estimate used only for priority selection.
    ///
    /// Fully unreachable bytes are exact. Mixed-Container gain subtracts a
    /// format-v1 independent-RAW upper bound for every replacement, so adaptive
    /// compression can only improve the eventual result.
    #[must_use]
    pub const fn estimated_reclaimable_bytes(&self) -> u64 {
        self.estimated_reclaimable_bytes
    }

    #[must_use]
    pub const fn container_bytes(&self) -> u64 {
        self.scrub.containers.file_bytes()
    }

    #[must_use]
    pub const fn partially_live_containers(&self) -> u64 {
        self.partially_live_containers
    }

    #[must_use]
    pub fn compaction_victim_containers(&self) -> usize {
        self.compaction_victims.len()
    }

    #[must_use]
    pub fn replacement_chunks(&self) -> usize {
        self.replacement_chunks.len()
    }
}

/// Evidence returned only after replacement-index activation and durable
/// Container-directory deletion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GarbageCollectionReport {
    containers_removed: u64,
    bytes_removed: u64,
    replacement_containers: u64,
    replacement_bytes: u64,
    chunks_relocated: u64,
    priority: MaintenancePriority,
}

impl GarbageCollectionReport {
    #[must_use]
    pub const fn containers_removed(self) -> u64 {
        self.containers_removed
    }

    #[must_use]
    pub const fn bytes_removed(self) -> u64 {
        self.bytes_removed
    }

    #[must_use]
    pub const fn replacement_containers(self) -> u64 {
        self.replacement_containers
    }

    #[must_use]
    pub const fn replacement_bytes(self) -> u64 {
        self.replacement_bytes
    }

    #[must_use]
    pub const fn chunks_relocated(self) -> u64 {
        self.chunks_relocated
    }

    #[must_use]
    pub const fn bytes_reclaimed(self) -> u64 {
        self.bytes_removed.saturating_sub(self.replacement_bytes)
    }

    #[must_use]
    pub const fn priority(self) -> MaintenancePriority {
        self.priority
    }
}

/// Join capability for one asynchronous Scrub followed by safe GC.
#[derive(Debug)]
pub struct BackgroundMaintenanceJob {
    scrub_priority: MaintenancePriority,
    worker: Option<thread::JoinHandle<Result<BackgroundMaintenanceReport, MaintenanceError>>>,
}

impl BackgroundMaintenanceJob {
    #[must_use]
    pub const fn scrub_priority(&self) -> MaintenancePriority {
        self.scrub_priority
    }

    /// Waits for the asynchronous maintenance result.
    ///
    /// # Errors
    ///
    /// Returns the Scrub/GC error or a production-fatal worker panic as an
    /// explicit maintenance failure.
    pub fn wait(mut self) -> Result<BackgroundMaintenanceReport, MaintenanceError> {
        self.worker
            .take()
            .ok_or(MaintenanceError::MaintenanceThreadPanicked)?
            .join()
            .map_err(|_| MaintenanceError::MaintenanceThreadPanicked)?
    }
}

/// Combined evidence from one successful asynchronous maintenance cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackgroundMaintenanceReport {
    scrub: EndToEndScrubReport,
    scrub_priority: MaintenancePriority,
    gc: GarbageCollectionReport,
}

impl BackgroundMaintenanceReport {
    #[must_use]
    pub const fn scrub(self) -> EndToEndScrubReport {
        self.scrub
    }

    #[must_use]
    pub const fn scrub_priority(self) -> MaintenancePriority {
        self.scrub_priority
    }

    #[must_use]
    pub const fn gc(self) -> GarbageCollectionReport {
        self.gc
    }
}

#[derive(Debug)]
pub enum MaintenanceError {
    Store(StoreError),
    Generation(GenerationError),
    ExactIndex(ExactIndexStoreError),
    ExactProfileMismatch,
    GcPlanProfileMismatch,
    StaleGcPlan,
    BackgroundPriority(io::Error),
    MaintenanceThread(io::Error),
    MaintenanceThreadPanicked,
    OnlineChunkLengthMismatch {
        chunk_id: ChunkId,
        expected: u64,
        observed: u64,
    },
    MissingReplacementChunk,
    ReplacementIdentity,
    ArithmeticOverflow,
    OutOfMemory,
}

impl fmt::Display for MaintenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for MaintenanceError {}

impl From<StoreError> for MaintenanceError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<GenerationError> for MaintenanceError {
    fn from(error: GenerationError) -> Self {
        Self::Generation(error)
    }
}

impl From<ExactIndexStoreError> for MaintenanceError {
    fn from(error: ExactIndexStoreError) -> Self {
        Self::ExactIndex(error)
    }
}

impl From<ExactIndexFormatError> for MaintenanceError {
    fn from(error: ExactIndexFormatError) -> Self {
        Self::ExactIndex(ExactIndexStoreError::from(error))
    }
}

impl From<ExactIndexRunSetError> for MaintenanceError {
    fn from(error: ExactIndexRunSetError) -> Self {
        Self::ExactIndex(ExactIndexStoreError::from(error))
    }
}
