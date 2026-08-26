//! Offline integrity audit and rebuild orchestration.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexActivationRecord, ExactIndexEntry, ExactIndexFormatError,
    ExactIndexProfileId, ExactIndexRun, ExactIndexRunRef, ExactIndexRunSet, ExactIndexRunSetError,
    GcCandidateCatalogDescriptor, GcCandidateCatalogRow, MAX_LOGICAL_CHUNK_BYTES, SealedContainer,
};

use crate::generation::GenerationLivenessProof;
use crate::maintenance_ioprio;
use crate::similarity_index_repository::similarity_index_entry_v1_from_verified;
use crate::{
    ContainerAuditSummary, ContainerRepository, ExactIndexGenerationDrain, ExactIndexRunRepository,
    ExactIndexStoreError, GcCandidateCatalogRepository, GcCandidateCatalogStoreError,
    GcCandidateSelectionMode, GcCandidateShortlist, GenerationError, GenerationRepository,
    SimilarityIndexRepository, SimilarityIndexStoreError, StorageIo, StoreError,
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
const GC_CANDIDATE_PROOF_MAX_VICTIMS: usize = 64;
const GC_CANDIDATE_PROOF_MAX_RAW_REPLACEMENT_BYTES: u64 = 64 * 1_024 * 1_024;
const ONLINE_GC_BACKGROUND_SHORTLIST: usize = 16;
const ONLINE_GC_URGENT_SHORTLIST: usize = 64;

/// Scheduling class for one maintenance phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenancePriority {
    Background,
    Normal,
}

/// Resource policy for one explicitly started maintenance cycle.
///
/// `Adaptive` protects frontend I/O without adding observations, atomics, or
/// locks to the write hot loop: every maintenance phase executes in Linux's
/// work-conserving idle I/O class. `FullSpeed` performs no CPU or I/O priority
/// demotion and is therefore restricted by the appliance CLI to acknowledged
/// exclusive offline operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MaintenanceExecutionMode {
    #[default]
    Adaptive,
    FullSpeed,
}

/// Bounded work quantum selected by the operational Online-GC scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnlineGcRunMode {
    /// Small nice+10 quantum while frontend I/O remains active.
    Background,
    /// Larger normal-CPU quantum after the frontend has remained quiet.
    Idle,
    /// Immediate larger quantum requested by pressure or the local control path.
    Urgent,
}

impl OnlineGcRunMode {
    const fn shortlist_limit(self) -> usize {
        match self {
            Self::Background => ONLINE_GC_BACKGROUND_SHORTLIST,
            Self::Idle | Self::Urgent => ONLINE_GC_URGENT_SHORTLIST,
        }
    }

    const fn selection_mode(self) -> GcCandidateSelectionMode {
        match self {
            Self::Background | Self::Idle => GcCandidateSelectionMode::Background,
            Self::Urgent => GcCandidateSelectionMode::Urgent,
        }
    }

    const fn priority(self) -> MaintenancePriority {
        match self {
            Self::Background => MaintenancePriority::Background,
            Self::Idle | Self::Urgent => MaintenancePriority::Normal,
        }
    }
}

impl MaintenanceExecutionMode {
    const fn effective_priority(self, adaptive: MaintenancePriority) -> MaintenancePriority {
        match self {
            Self::Adaptive => adaptive,
            Self::FullSpeed => MaintenancePriority::Normal,
        }
    }
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

fn build_reverse_dependency_generation<X: StorageIo>(
    exact: &crate::ActivatedExactIndex<X>,
    liveness: &GenerationLivenessProof,
) -> Result<ReverseDependencyGeneration, MaintenanceError> {
    let mut required_chunks = liveness.online_chunks().clone();
    let mut dependents_by_base = BTreeMap::<ChunkId, BTreeSet<ChunkId>>::new();
    let mut dependency_edges = 0_u64;
    for (chunk_id, logical_length) in liveness.online_chunks() {
        let logical_length_u32 =
            u32::try_from(*logical_length).map_err(|_| MaintenanceError::ArithmeticOverflow)?;
        let lookup = exact.lookup_transitions(*chunk_id, logical_length_u32)?;
        if !lookup.complete() {
            return Err(MaintenanceError::IncompleteReverseDependencyGeneration {
                chunk_id: *chunk_id,
            });
        }
        let mut seen_locations = Vec::new();
        seen_locations
            .try_reserve_exact(lookup.candidates().len())
            .map_err(|_| MaintenanceError::OutOfMemory)?;
        let mut active_location_seen = false;
        for entry in lookup.candidates() {
            assert_eq!(
                entry.chunk_id(),
                *chunk_id,
                "ASSERT: Exact lookup returned another target while building reverse dependencies"
            );
            assert_eq!(
                entry.logical_length(),
                logical_length_u32,
                "ASSERT: Exact lookup returned another length while building reverse dependencies"
            );
            let location = entry.location();
            if seen_locations.contains(&location) {
                continue;
            }
            seen_locations.push(location);
            if entry.transition() != fastdup_format::ExactLocationTransition::Active {
                continue;
            }
            active_location_seen = true;
            if location.dependency_id() == [0; 32] {
                continue;
            }
            let base_id = ChunkId::from_bytes(location.dependency_id());
            if let Some(previous) = required_chunks.insert(base_id, *logical_length)
                && previous != *logical_length
            {
                return Err(MaintenanceError::OnlineChunkLengthMismatch {
                    chunk_id: base_id,
                    expected: previous,
                    observed: *logical_length,
                });
            }
            if dependents_by_base
                .entry(base_id)
                .or_default()
                .insert(*chunk_id)
            {
                dependency_edges = dependency_edges
                    .checked_add(1)
                    .ok_or(MaintenanceError::ArithmeticOverflow)?;
            }
        }
        if !active_location_seen {
            return Err(MaintenanceError::MissingLiveExactLocation {
                chunk_id: *chunk_id,
            });
        }
    }
    let mapped_edges = dependents_by_base
        .values()
        .map(BTreeSet::len)
        .map(|count| u64::try_from(count).expect("ASSERT: reverse edge count fits u64"))
        .try_fold(0_u64, u64::checked_add)
        .expect("ASSERT: checked Reverse Dependency Generation edge count cannot overflow");
    assert_eq!(
        dependency_edges, mapped_edges,
        "ASSERT: Reverse Dependency Generation edge count matches its Base map"
    );
    assert!(
        dependents_by_base
            .keys()
            .all(|base_id| required_chunks.contains_key(base_id)),
        "ASSERT: every reverse Base edge contributes replacement liveness"
    );
    Ok(ReverseDependencyGeneration {
        exact_activation: exact.record(),
        protected_commit_generation: liveness.summary().latest_generation(),
        protected_targets: liveness.online_chunks().keys().copied().collect(),
        required_chunks,
        dependents_by_base,
        dependency_edges,
    })
}

fn next_gc_catalog_generation<G: Clone + StorageIo>(
    catalog: &GcCandidateCatalogRepository<G>,
) -> Result<u64, MaintenanceError> {
    catalog
        .discover_generation_high_water()?
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(MaintenanceError::ArithmeticOverflow)
}

fn gc_candidate_catalog_is_stale(error: &MaintenanceError) -> bool {
    match error {
        MaintenanceError::GcCandidateIdentityMismatch => true,
        MaintenanceError::Store(StoreError::Io(error)) => error.kind() == io::ErrorKind::NotFound,
        _ => false,
    }
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
    reverse_dependency_cache: Arc<Mutex<Option<Arc<ReverseDependencyGeneration>>>>,
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
            reverse_dependency_cache: Arc::new(Mutex::new(None)),
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
        self.start_scrub_and_gc_with_mode(pool_usage, MaintenanceExecutionMode::Adaptive)
    }

    /// Starts Scrub and subsequent GC using an explicit resource policy.
    ///
    /// Adaptive workers always use Linux idle-class I/O, even when space
    /// pressure promotes their CPU priority. This keeps maintenance requests
    /// out of the frontend's scheduling class without placing any accounting
    /// in the frontend hot loop. Full-speed mode skips both CPU and I/O
    /// demotion and must only be admitted by a caller holding exclusive offline
    /// ownership.
    ///
    /// # Errors
    ///
    /// Returns a thread-creation failure before any maintenance work starts.
    pub fn start_scrub_and_gc_with_mode(
        &self,
        pool_usage: DataPoolUsage,
        mode: MaintenanceExecutionMode,
    ) -> Result<BackgroundMaintenanceJob, MaintenanceError>
    where
        M: Send + 'static,
        C: Send + 'static,
        X: Send + Sync + 'static,
    {
        let repository = self.clone();
        let scrub_priority = mode.effective_priority(pool_usage.scrub_priority());
        let worker = thread::Builder::new()
            .name("fastdup-maintenance".to_owned())
            .spawn(move || {
                let scrub_repository = repository.clone();
                let mut plan = run_at_priority(scrub_priority, mode, "fastdup-scrub", move || {
                    scrub_repository.scrub_for_gc(pool_usage)
                })?;
                let scrub = plan.scrub_report();
                let gc_priority = mode.effective_priority(plan.gc_priority());
                plan.gc_priority = gc_priority;
                let (gc, metadata_gc) =
                    run_at_priority(gc_priority, mode, "fastdup-gc", move || {
                        let gc = repository.garbage_collect(plan)?;
                        let metadata_gc = repository.garbage_collect_metadata()?;
                        Ok((gc, metadata_gc))
                    })?;
                Ok(BackgroundMaintenanceReport {
                    scrub,
                    scrub_priority,
                    gc,
                    metadata_gc,
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
        self.generations.audit_metadata_mark_catalogs()?;
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

    /// Collects verified Metadata Objects outside every retained Commit graph.
    ///
    /// It verifies the complete retained Metadata graph, includes every live
    /// reader/successor root pin, and verifies every deletion candidate identity
    /// before the first unlink. Publication and Commit barriers exclude partial
    /// graph races; one Metadata-directory sync completes the removal batch.
    ///
    /// # Errors
    ///
    /// Returns the first Generation-Log, graph, identity, I/O, or durability
    /// failure without treating an unverified name as garbage.
    pub fn garbage_collect_metadata(
        &self,
    ) -> Result<MetadataGarbageCollectionReport, MaintenanceError> {
        let summary = self.generations.garbage_collect_metadata()?;
        Ok(MetadataGarbageCollectionReport {
            objects_removed: summary.objects_removed(),
            bytes_removed: summary.bytes_removed(),
            objects_retained: summary.objects_retained(),
            mark_mode: summary.mark_mode(),
            exact_reason: summary.exact_reason(),
            catalog_generation: summary.catalog_generation(),
            metrics: summary.metrics(),
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

    /// Builds bounded deletion evidence from a catalog shortlist without a
    /// preceding complete End-to-End Scrub or complete Container-pool scan.
    ///
    /// The proof first projects every protected target through the complete
    /// active Exact generation and adds the Base ID from every effective ACTIVE
    /// dependent Location. It then preserves only protected targets and those
    /// generation-bound Bases found in selected victims. No catalog fanout,
    /// Similarity hint, or Exact negative becomes deletion authority.
    ///
    /// The returned capability binds the current/previous Commit Records, the
    /// active Exact generation, the catalog generation, and fully verified
    /// victim identities. It does not retain payload bytes.
    ///
    /// # Errors
    ///
    /// Returns missing/stale Exact state, victim verification, identity,
    /// bounded-proof, unprofitable-set, or checked-arithmetic failures.
    ///
    /// # Panics
    ///
    /// Panics only if an audited catalog shortlist contains the same
    /// Container identity twice.
    #[allow(clippy::too_many_lines)]
    pub fn prove_gc_candidates(
        &self,
        shortlist: &GcCandidateShortlist,
        pool_usage: DataPoolUsage,
    ) -> Result<GcCandidateProof, MaintenanceError> {
        let exact = self
            .indexes
            .recover_active()?
            .ok_or(MaintenanceError::GcProofRequiresActiveExactIndex)?;
        if exact.record().profile() != self.exact_profile {
            return Err(MaintenanceError::ExactProfileMismatch);
        }
        let generation_proof = self.generations.scan_online_liveness()?;
        let reverse_dependencies = self.reverse_dependency_generation(&exact, &generation_proof)?;
        let mut victims = BTreeMap::new();
        let mut replacement_chunks = BTreeMap::new();
        let mut victim_bytes = 0_u64;
        let mut reachable_victim_chunks = BTreeSet::new();

        for row in shortlist
            .rows()
            .iter()
            .copied()
            .take(GC_CANDIDATE_PROOF_MAX_VICTIMS)
        {
            let container = self
                .containers
                .read_with_index(row.container_id(), &exact)?;
            if container.header().container_generation() != row.container_generation()
                || container.header().layout().file_length != row.physical_bytes()
            {
                return Err(MaintenanceError::GcCandidateIdentityMismatch);
            }
            let mut newly_required = Vec::new();
            for record in container.records() {
                let logical_length = u64::try_from(record.payload().len())
                    .map_err(|_| MaintenanceError::ArithmeticOverflow)?;
                let Some(expected_length) = reverse_dependencies
                    .required_chunks
                    .get(&record.chunk_id())
                    .copied()
                else {
                    continue;
                };
                if expected_length != logical_length {
                    return Err(MaintenanceError::OnlineChunkLengthMismatch {
                        chunk_id: record.chunk_id(),
                        expected: expected_length,
                        observed: logical_length,
                    });
                }
                if let Some(previous) = replacement_chunks.insert(record.chunk_id(), logical_length)
                {
                    if previous != logical_length {
                        return Err(MaintenanceError::OnlineChunkLengthMismatch {
                            chunk_id: record.chunk_id(),
                            expected: previous,
                            observed: logical_length,
                        });
                    }
                } else {
                    newly_required.push(record.chunk_id());
                }
                reachable_victim_chunks.insert(record.chunk_id());
            }
            let projected = replacement_file_bytes_upper_bound(&replacement_chunks)?;
            if projected > GC_CANDIDATE_PROOF_MAX_RAW_REPLACEMENT_BYTES {
                for chunk_id in newly_required {
                    let removed = replacement_chunks.remove(&chunk_id);
                    assert!(
                        removed.is_some(),
                        "ASSERT: proof-budget rollback removes every newly required Chunk"
                    );
                    reachable_victim_chunks.remove(&chunk_id);
                }
                if victims.is_empty() {
                    return Err(MaintenanceError::GcCandidateProofBudgetExceeded);
                }
                break;
            }
            let previous = victims.insert(row.container_id().bytes(), row.container_id());
            assert!(
                previous.is_none(),
                "ASSERT: a canonical catalog shortlist contains each Container once"
            );
            victim_bytes = victim_bytes
                .checked_add(row.physical_bytes())
                .ok_or(MaintenanceError::ArithmeticOverflow)?;
        }
        if victims.is_empty() {
            return Err(MaintenanceError::EmptyGcCandidateProof);
        }
        if generation_proof.online_chunks().is_empty() {
            assert!(
                replacement_chunks.is_empty() && reachable_victim_chunks.is_empty(),
                "ASSERT: an empty protected DATA set cannot require GC replacement coverage"
            );
        }
        let replacement_upper = replacement_file_bytes_upper_bound(&replacement_chunks)?;
        if replacement_upper >= victim_bytes {
            return Err(MaintenanceError::UnprofitableGcCandidateProof {
                victim_bytes,
                replacement_upper,
            });
        }
        let estimated_reclaimable_bytes = victim_bytes - replacement_upper;
        let priority = pool_usage.gc_priority(estimated_reclaimable_bytes, victim_bytes);
        Ok(GcCandidateProof {
            catalog: shortlist.descriptor(),
            generation_proof,
            reverse_dependencies,
            exact_profile: self.exact_profile,
            victims,
            victim_bytes,
            replacement_chunks,
            replacement_upper,
            reachable_victim_chunks: reachable_victim_chunks.len(),
            priority,
        })
    }

    fn reverse_dependency_generation(
        &self,
        exact: &crate::ActivatedExactIndex<X>,
        liveness: &GenerationLivenessProof,
    ) -> Result<Arc<ReverseDependencyGeneration>, MaintenanceError> {
        let exact_activation = exact.record();
        let protected_commit_generation = liveness.summary().latest_generation();
        let mut cache = self
            .reverse_dependency_cache
            .lock()
            .expect("ASSERT: Reverse Dependency Generation cache lock poisoned");
        if let Some(cached) = cache.as_ref()
            && cached.exact_activation == exact_activation
            && cached.protected_commit_generation == protected_commit_generation
            && cached.protected_targets.len() == liveness.online_chunks().len()
            && cached
                .protected_targets
                .iter()
                .copied()
                .eq(liveness.online_chunks().keys().copied())
            && liveness
                .online_chunks()
                .iter()
                .all(|(chunk_id, length)| cached.required_chunks.get(chunk_id) == Some(length))
        {
            return Ok(Arc::clone(cached));
        }
        let built = Arc::new(build_reverse_dependency_generation(exact, liveness)?);
        *cache = Some(Arc::clone(&built));
        Ok(built)
    }

    /// Advances an existing publication-seeded GC catalog to the current
    /// protected Commit pair in one generation-bound operation.
    ///
    /// The module derives the Metadata delta, pins the current Exact
    /// generation for physical attribution, applies only affected Container
    /// rows, and publishes one immutable successor. Callers need not assemble
    /// or interpret the delta themselves.
    ///
    /// # Errors
    ///
    /// Returns missing seed catalog, Metadata delta, active Exact, catalog
    /// freshness/publication, or storage failures.
    pub fn refresh_gc_candidate_catalog<G: Clone + StorageIo>(
        &self,
        catalog: &GcCandidateCatalogRepository<G>,
        catalog_generation: u64,
    ) -> Result<GcCandidateCatalogDescriptor, MaintenanceError> {
        let previous = catalog
            .recover_latest()?
            .ok_or(MaintenanceError::MissingGcCandidateCatalog)?;
        let incorporated = previous.descriptor().incorporated_commit_generation();
        let delta = self
            .generations
            .liveness_delta_since((incorporated != 0).then_some(incorporated))?;
        if delta.latest_generation().unwrap_or(0) == incorporated {
            return Ok(previous.descriptor());
        }
        let exact = self
            .indexes
            .recover_active()?
            .ok_or(MaintenanceError::GcProofRequiresActiveExactIndex)?;
        if exact.record().profile() != self.exact_profile {
            return Err(MaintenanceError::ExactProfileMismatch);
        }
        Ok(catalog.publish_liveness_delta(&previous, catalog_generation, &delta, &exact)?)
    }

    /// Bootstraps one complete GC candidate hint generation from immutable
    /// Container envelopes without reading record payloads or retaining a
    /// pool-sized row map.
    ///
    /// Header/Footer summaries are sufficient because the catalog has no
    /// deletion authority. Candidate proof later fully verifies only the
    /// bounded shortlist. Rows begin with unknown liveness and are advanced by
    /// [`Self::refresh_gc_candidate_catalog`].
    ///
    /// # Errors
    ///
    /// Returns directory, naming, envelope, row, publication, allocation, or
    /// checked-accounting failures.
    pub fn rebuild_gc_candidate_catalog<G: Clone + StorageIo>(
        &self,
        catalog: &GcCandidateCatalogRepository<G>,
        catalog_generation: u64,
    ) -> Result<GcCandidateCatalogDescriptor, MaintenanceError> {
        let row_count = self.containers.published_container_count()?;
        Ok(
            catalog.publish_generated(catalog_generation, 0, 0, row_count, |emit| {
                self.containers
                    .visit_published_intrinsic_summaries::<GcCandidateCatalogStoreError, _>(
                        |container_id, container_generation, physical_bytes, summary| {
                            emit(GcCandidateCatalogRow::from_intrinsic_summary(
                                container_id,
                                container_generation,
                                physical_bytes,
                                summary,
                            )?)
                        },
                    )
            })?,
        )
    }

    /// Runs one bounded Online-GC quantum in the adaptive maintenance I/O
    /// class selected by the scheduler.
    ///
    /// The method bootstraps the hint catalog when absent, advances Metadata
    /// liveness incrementally, proves only a bounded shortlist, executes the
    /// RETIRING/pin-drain protocol, and rebuilds hint rows after physical
    /// relocation. `Urgent` changes CPU priority and candidate ordering but
    /// never leaves Linux idle I/O class.
    ///
    /// # Errors
    ///
    /// Returns worker setup, catalog, proof, relocation, transition, or
    /// recovery failures. Expected empty or unprofitable shortlists are
    /// successful outcomes rather than errors.
    pub fn run_adaptive_online_gc_cycle<G>(
        &self,
        catalog: &GcCandidateCatalogRepository<G>,
        pool_usage: DataPoolUsage,
        mode: OnlineGcRunMode,
    ) -> Result<OnlineGcCycleReport, MaintenanceError>
    where
        M: Send + 'static,
        C: Send + 'static,
        X: Send + Sync + 'static,
        G: Clone + Send + Sync + StorageIo + 'static,
    {
        self.run_adaptive_online_gc_cycle_with_workers(
            catalog,
            pool_usage,
            mode,
            thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
        )
    }

    /// Runs one adaptive quantum while capping relocation encoding workers.
    /// Candidate proof, Exact transitions, and unlink remain serialized; only
    /// the existing bounded replacement encoder uses this CPU limit.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::run_adaptive_online_gc_cycle`].
    pub fn run_adaptive_online_gc_cycle_with_workers<G>(
        &self,
        catalog: &GcCandidateCatalogRepository<G>,
        pool_usage: DataPoolUsage,
        mode: OnlineGcRunMode,
        relocation_workers: NonZeroUsize,
    ) -> Result<OnlineGcCycleReport, MaintenanceError>
    where
        M: Send + 'static,
        C: Send + 'static,
        X: Send + Sync + 'static,
        G: Clone + Send + Sync + StorageIo + 'static,
    {
        let repository = self.clone();
        let catalog = catalog.clone();
        run_at_priority(
            mode.priority(),
            MaintenanceExecutionMode::Adaptive,
            "fastdup-online-gc",
            move || repository.run_online_gc_cycle(&catalog, pool_usage, mode, relocation_workers),
        )
    }

    #[allow(clippy::too_many_lines)]
    fn run_online_gc_cycle<G: Clone + StorageIo>(
        &self,
        catalog: &GcCandidateCatalogRepository<G>,
        pool_usage: DataPoolUsage,
        mode: OnlineGcRunMode,
        relocation_workers: NonZeroUsize,
    ) -> Result<OnlineGcCycleReport, MaintenanceError>
    where
        X: Send + Sync + 'static,
    {
        assert_eq!(
            thread::current().name(),
            Some("fastdup-online-gc"),
            "ASSERT: adaptive Online-GC I/O runs only on its dedicated maintenance worker"
        );
        let started = Instant::now();
        let mut metrics = OnlineGcMetrics {
            relocation_workers: u64::try_from(relocation_workers.get())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?,
            ..OnlineGcMetrics::default()
        };
        let phase_started = Instant::now();
        self.finalize_recovered_online_gc()?;
        metrics.recovery_wall = phase_started.elapsed();
        let phase_started = Instant::now();
        let metadata_gc = self.garbage_collect_metadata()?;
        metrics.metadata_gc_wall = phase_started.elapsed();
        let phase_started = Instant::now();
        if catalog.recover_latest()?.is_none() {
            let rebuilt =
                self.rebuild_gc_candidate_catalog(catalog, next_gc_catalog_generation(catalog)?)?;
            metrics.catalog_write_bytes = metrics
                .catalog_write_bytes
                .checked_add(rebuilt.file_length())
                .ok_or(MaintenanceError::ArithmeticOverflow)?;
        }
        let mut next_generation = next_gc_catalog_generation(catalog)?;
        let before_refresh = catalog
            .recover_latest()?
            .ok_or(MaintenanceError::MissingGcCandidateCatalog)?
            .descriptor();
        match self.refresh_gc_candidate_catalog(catalog, next_generation) {
            Ok(refreshed) => {
                if refreshed.generation() != before_refresh.generation() {
                    metrics.catalog_write_bytes = metrics
                        .catalog_write_bytes
                        .checked_add(refreshed.file_length())
                        .ok_or(MaintenanceError::ArithmeticOverflow)?;
                }
            }
            Err(MaintenanceError::Generation(GenerationError::LivenessDeltaBaseUnavailable {
                ..
            })) => {
                let rebuilt = self.rebuild_gc_candidate_catalog(catalog, next_generation)?;
                metrics.catalog_write_bytes = metrics
                    .catalog_write_bytes
                    .checked_add(rebuilt.file_length())
                    .ok_or(MaintenanceError::ArithmeticOverflow)?;
                next_generation = next_gc_catalog_generation(catalog)?;
                let refreshed = self.refresh_gc_candidate_catalog(catalog, next_generation)?;
                if refreshed.generation() != rebuilt.generation() {
                    metrics.catalog_write_bytes = metrics
                        .catalog_write_bytes
                        .checked_add(refreshed.file_length())
                        .ok_or(MaintenanceError::ArithmeticOverflow)?;
                }
            }
            Err(error) => return Err(error),
        }
        let snapshot = catalog
            .recover_latest()?
            .ok_or(MaintenanceError::MissingGcCandidateCatalog)?;
        let shortlist =
            snapshot.shortlist(mode.selection_mode(), mode.shortlist_limit(), u64::MAX)?;
        metrics.catalog_examined_bytes = snapshot.descriptor().file_length();
        metrics.shortlisted_candidates = u64::try_from(shortlist.rows().len())
            .map_err(|_| MaintenanceError::ArithmeticOverflow)?;
        metrics.candidate_catalog_wall = phase_started.elapsed();
        if shortlist.rows().is_empty() {
            metrics.total_wall = started.elapsed();
            return Ok(OnlineGcCycleReport {
                outcome: OnlineGcCycleOutcome::NoCandidates,
                catalog: snapshot.descriptor(),
                metadata_gc,
                metrics,
            });
        }
        let phase_started = Instant::now();
        let proof = match self.prove_gc_candidates(&shortlist, pool_usage) {
            Ok(proof) => {
                metrics.proved_victims = u64::try_from(proof.victim_containers())
                    .map_err(|_| MaintenanceError::ArithmeticOverflow)?;
                metrics.reverse_dependency_edges = proof.reverse_dependency_edges();
                metrics.reverse_dependency_required_chunks =
                    u64::try_from(proof.reverse_dependency_required_chunks())
                        .map_err(|_| MaintenanceError::ArithmeticOverflow)?;
                metrics.candidate_proof_read_bytes = proof.victim_bytes();
                metrics.candidate_proof_wall = phase_started.elapsed();
                proof
            }
            Err(
                MaintenanceError::EmptyGcCandidateProof
                | MaintenanceError::GcCandidateProofBudgetExceeded
                | MaintenanceError::UnprofitableGcCandidateProof { .. },
            ) => {
                metrics.candidate_proof_wall = phase_started.elapsed();
                metrics.aborted_candidates = metrics.shortlisted_candidates;
                metrics.total_wall = started.elapsed();
                return Ok(OnlineGcCycleReport {
                    outcome: OnlineGcCycleOutcome::NoProfitableCandidates,
                    catalog: snapshot.descriptor(),
                    metadata_gc,
                    metrics,
                });
            }
            Err(error) if gc_candidate_catalog_is_stale(&error) => {
                let generation = next_gc_catalog_generation(catalog)?;
                let catalog = self.rebuild_gc_candidate_catalog(catalog, generation)?;
                metrics.candidate_proof_wall = phase_started.elapsed();
                metrics.aborted_candidates = metrics.shortlisted_candidates;
                metrics.catalog_write_bytes = metrics
                    .catalog_write_bytes
                    .checked_add(catalog.file_length())
                    .ok_or(MaintenanceError::ArithmeticOverflow)?;
                metrics.total_wall = started.elapsed();
                return Ok(OnlineGcCycleReport {
                    outcome: OnlineGcCycleOutcome::CatalogRebuilt,
                    catalog,
                    metadata_gc,
                    metrics,
                });
            }
            Err(error) => return Err(error),
        };
        let victim_bytes = proof.victim_bytes();
        let phase_started = Instant::now();
        let retirement = self.begin_online_gc_retirement_with_workers(proof, relocation_workers)?;
        let collected = self.finish_online_gc_retirement(retirement)?;
        metrics.relocation_wall = phase_started.elapsed();
        metrics.relocation_read_bytes = victim_bytes;
        metrics.relocation_write_bytes = collected.replacement_bytes();
        metrics.unlinked_bytes = collected.bytes_removed();
        metrics.retiring_activation_wall = collected.retiring_activation_wall();
        metrics.pin_drain_wall = collected.pin_drain_wall();
        metrics.victim_verify_wall = collected.victim_verify_wall();
        metrics.unlink_wall = collected.unlink_wall();
        metrics.data_sync_wall = collected.data_sync_wall();
        metrics.removed_activation_wall = collected.removed_activation_wall();
        let phase_started = Instant::now();
        let generation = next_gc_catalog_generation(catalog)?;
        let catalog = self.rebuild_gc_candidate_catalog(catalog, generation)?;
        metrics.catalog_write_bytes = metrics
            .catalog_write_bytes
            .checked_add(catalog.file_length())
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        metrics.post_collection_catalog_wall = phase_started.elapsed();
        metrics.total_wall = started.elapsed();
        Ok(OnlineGcCycleReport {
            outcome: OnlineGcCycleOutcome::Collected(collected),
            catalog,
            metadata_gc,
            metrics,
        })
    }

    /// Executes one locally proved candidate compaction under the existing
    /// exclusive maintenance-ownership rule.
    ///
    /// Every victim Chunk is republished first, so unknown incoming Base
    /// dependencies remain covered. The protected Commit pair and selected
    /// Exact generation are revalidated before and after replacement
    /// publication. The rebuilt Exact Index is activated before any victim is
    /// unlinked.
    ///
    /// # Errors
    ///
    /// Returns stale proof bindings, replacement/rebuild/storage failures, or
    /// checked-accounting failures. No deletion precedes replacement and index
    /// activation.
    ///
    /// # Panics
    ///
    /// Panics only if a fully verified immutable victim changes physical
    /// length between proof and deletion reread.
    pub fn garbage_collect_proved_candidates(
        &self,
        proof: GcCandidateProof,
    ) -> Result<GarbageCollectionReport, MaintenanceError> {
        let GcCandidateProof {
            generation_proof,
            reverse_dependencies,
            exact_profile,
            victims,
            victim_bytes,
            replacement_chunks,
            priority,
            ..
        } = proof;
        let exact_activation = reverse_dependencies.exact_activation;
        assert_eq!(
            reverse_dependencies.protected_commit_generation,
            generation_proof.summary().latest_generation(),
            "ASSERT: reverse dependencies and DATA liveness bind the same Commit generation"
        );
        assert!(
            !victims.is_empty(),
            "ASSERT: GC retirement consumes a nonempty candidate proof"
        );
        if exact_profile != self.exact_profile {
            return Err(MaintenanceError::GcPlanProfileMismatch);
        }
        if !self.generations.gc_proof_is_current(&generation_proof)? {
            return Err(MaintenanceError::StaleGcPlan);
        }
        let exact = self
            .indexes
            .recover_active()?
            .filter(|active| active.record() == exact_activation)
            .ok_or(MaintenanceError::StaleGcPlan)?;
        let first_replacement_generation = self
            .containers
            .discover_container_generation_high_water()?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        let replacements = self.publish_gc_replacements_using(
            &victims,
            &replacement_chunks,
            first_replacement_generation,
            thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
            |container_id| Ok(self.containers.read_with_index(container_id, &exact)?),
        )?;
        if !self.generations.gc_proof_is_current(&generation_proof)?
            || !self.gc_exact_binding_is_current(exact_activation)?
        {
            return Err(MaintenanceError::StaleGcPlan);
        }
        self.rebuild_exact_index_excluding(&victims)?;
        if !self.generations.gc_proof_is_current(&generation_proof)? {
            return Err(MaintenanceError::StaleGcPlan);
        }
        let (bytes_removed, removal_metrics) =
            self.containers.remove_verified_published(&victims)?;
        assert_eq!(
            bytes_removed, victim_bytes,
            "ASSERT: proved victim identities retain their immutable lengths"
        );
        Ok(GarbageCollectionReport {
            containers_removed: u64::try_from(victims.len())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?,
            bytes_removed,
            replacement_containers: replacements.containers,
            replacement_bytes: replacements.bytes,
            chunks_relocated: replacements.chunks,
            priority,
            retiring_activation_wall: Duration::ZERO,
            pin_drain_wall: Duration::ZERO,
            victim_verify_wall: removal_metrics.verify_wall(),
            unlink_wall: removal_metrics.unlink_wall(),
            data_sync_wall: removal_metrics.sync_wall(),
            removed_activation_wall: Duration::ZERO,
        })
    }

    /// Publishes complete replacement coverage and commits one durable
    /// RETIRING barrier for a locally proved victim set.
    ///
    /// The barrier contains ACTIVE replacement Locations and RETIRING victim
    /// Locations in the same newly activated L0 generation. Directory-scan
    /// fallbacks are excluded before activation. Dropping the returned value
    /// leaves a safe, resumable RETIRING generation and never deletes DATA.
    ///
    /// # Errors
    ///
    /// Returns stale proof bindings, victim/replacement verification,
    /// transition publication, or allocation failures. No victim is removed.
    ///
    /// # Panics
    ///
    /// Panics only if an internally constructed candidate proof contains no
    /// victim, which violates the proof constructor's invariant.
    pub fn begin_online_gc_retirement(
        &self,
        proof: GcCandidateProof,
    ) -> Result<OnlineGcRetirement<X>, MaintenanceError>
    where
        X: Send + Sync + 'static,
    {
        self.begin_online_gc_retirement_with_workers(
            proof,
            thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
        )
    }

    fn begin_online_gc_retirement_with_workers(
        &self,
        proof: GcCandidateProof,
        relocation_workers: NonZeroUsize,
    ) -> Result<OnlineGcRetirement<X>, MaintenanceError>
    where
        X: Send + Sync + 'static,
    {
        let retirement_started = Instant::now();
        let GcCandidateProof {
            generation_proof,
            reverse_dependencies,
            exact_profile,
            victims,
            victim_bytes,
            replacement_chunks,
            priority,
            ..
        } = proof;
        let exact_activation = reverse_dependencies.exact_activation;
        assert_eq!(
            reverse_dependencies.protected_commit_generation,
            generation_proof.summary().latest_generation(),
            "ASSERT: reverse dependencies and DATA liveness bind the same Commit generation"
        );
        assert!(
            !victims.is_empty(),
            "ASSERT: Online-GC retirement consumes a nonempty candidate proof"
        );
        if exact_profile != self.exact_profile {
            return Err(MaintenanceError::GcPlanProfileMismatch);
        }
        if !self.generations.gc_proof_is_current(&generation_proof)? {
            return Err(MaintenanceError::StaleGcPlan);
        }
        let exact = self
            .indexes
            .recover_active_generation()?
            .filter(|active| active.record() == exact_activation)
            .ok_or(MaintenanceError::StaleGcPlan)?;
        let first_replacement_generation = self
            .containers
            .discover_container_generation_high_water()?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        let mut retiring_entries = Vec::new();
        let mut replacements = self.publish_gc_replacements_using(
            &victims,
            &replacement_chunks,
            first_replacement_generation,
            relocation_workers,
            |container_id| {
                let container = self.containers.read_with_index(container_id, &exact)?;
                retiring_entries
                    .try_reserve(container.locations().len())
                    .map_err(|_| MaintenanceError::OutOfMemory)?;
                for location in container.locations().iter().copied() {
                    let active = ExactIndexEntry::from_verified(location)?;
                    let retiring = ExactIndexEntry::retiring(active)?;
                    retiring_entries.push(retiring);
                }
                Ok(container)
            },
        )?;
        let transition = self
            .generations
            .apply_if_gc_proof_current(&generation_proof, || {
                if !self.gc_exact_binding_is_current(exact_activation)? {
                    return Err(MaintenanceError::StaleGcPlan);
                }
                let selection_barrier = self
                    .containers
                    .prepare_retiring_selection_barrier(&victims)?;
                let mut transitions = std::mem::take(&mut replacements.locations);
                transitions
                    .try_reserve(retiring_entries.len())
                    .map_err(|_| MaintenanceError::OutOfMemory)?;
                transitions.extend(retiring_entries.iter().copied());
                let transition = match self.indexes.append_level_zero_if_active(
                    self.exact_profile,
                    exact_activation,
                    transitions,
                ) {
                    Ok(transition) => transition,
                    Err(ExactIndexStoreError::ActivationChanged) => {
                        return Err(MaintenanceError::StaleGcPlan);
                    }
                    Err(error) => return Err(error.into()),
                };
                selection_barrier.commit();
                Ok(transition)
            })?
            .ok_or(MaintenanceError::StaleGcPlan)?;
        let drain = transition
            .into_retired()
            .ok_or(MaintenanceError::GcRetirementMissingPreviousGeneration)?;
        Ok(OnlineGcRetirement {
            victims,
            victim_bytes,
            replacements,
            retiring_entries,
            drain,
            priority,
            retiring_activation_wall: retirement_started.elapsed(),
        })
    }

    /// Waits for every displaced Exact generation pin, unlinks the exact
    /// victim identities, synchronizes DATA, and appends REMOVED tombstones.
    ///
    /// # Errors
    ///
    /// Returns victim identity, unlink/directory-sync, or final transition
    /// publication failures. The RETIRING generation remains safe and
    /// recoverable if completion is interrupted.
    ///
    /// # Panics
    ///
    /// Panics if a generation-drain lock is poisoned or immutable victim byte
    /// accounting changes between proof and unlink.
    pub fn finish_online_gc_retirement(
        &self,
        retirement: OnlineGcRetirement<X>,
    ) -> Result<GarbageCollectionReport, MaintenanceError>
    where
        X: Send + Sync + 'static,
    {
        let OnlineGcRetirement {
            victims,
            victim_bytes,
            replacements,
            retiring_entries,
            drain,
            priority,
            retiring_activation_wall,
        } = retirement;
        assert!(
            !victims.is_empty(),
            "ASSERT: victim unlink consumes a nonempty RETIRING capability"
        );
        assert!(
            !retiring_entries.is_empty(),
            "ASSERT: victim unlink follows durable RETIRING location activation"
        );
        let drain_started = Instant::now();
        drain.wait();
        let pin_drain_wall = drain_started.elapsed();
        let (bytes_removed, removal_metrics) =
            self.containers.remove_verified_published(&victims)?;
        assert_eq!(
            bytes_removed, victim_bytes,
            "ASSERT: online GC victim identities retain their immutable lengths"
        );
        let mut removed = Vec::new();
        removed
            .try_reserve_exact(retiring_entries.len())
            .map_err(|_| MaintenanceError::OutOfMemory)?;
        for retiring in retiring_entries {
            removed.push(ExactIndexEntry::removed(retiring)?);
        }
        let removed_activation_started = Instant::now();
        self.indexes
            .append_level_zero(self.exact_profile, removed)?;
        let removed_activation_wall = removed_activation_started.elapsed();
        self.containers.remove_retiring_selection_barrier(&victims);
        Ok(GarbageCollectionReport {
            containers_removed: u64::try_from(victims.len())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?,
            bytes_removed,
            replacement_containers: replacements.containers,
            replacement_bytes: replacements.bytes,
            chunks_relocated: replacements.chunks,
            priority,
            retiring_activation_wall,
            pin_drain_wall,
            victim_verify_wall: removal_metrics.verify_wall(),
            unlink_wall: removal_metrics.unlink_wall(),
            data_sync_wall: removal_metrics.sync_wall(),
            removed_activation_wall,
        })
    }

    /// Finalizes durable RETIRING work left by a terminated process.
    ///
    /// Restart has no surviving predecessor-generation pins. The active Exact
    /// generation is therefore sufficient recovery authority: effective
    /// RETIRING entries install the scan-selection barrier, every still-present
    /// victim must reproduce its complete Location set before unlink, and an
    /// already-absent victim is treated as an interrupted post-sync attempt.
    /// REMOVED tombstones are activated only after the DATA directory sync.
    ///
    /// This operation is idempotent. A generation without effective RETIRING
    /// entries returns an empty report and publishes nothing.
    ///
    /// # Errors
    ///
    /// Returns Exact recovery, victim verification, unlink/directory-sync, or
    /// REMOVED publication failures. Failure leaves the durable RETIRING
    /// selection barrier in force.
    pub fn finalize_recovered_online_gc(&self) -> Result<OnlineGcRecoveryReport, MaintenanceError>
    where
        X: Send + Sync + 'static,
    {
        let Some(active) = self.indexes.recover_active_generation()? else {
            return Ok(OnlineGcRecoveryReport::default());
        };
        if active.record().profile() != self.exact_profile {
            return Err(MaintenanceError::ExactProfileMismatch);
        }
        let retiring_entries = self.indexes.retiring_entries(&active)?;
        if retiring_entries.is_empty() {
            return Ok(OnlineGcRecoveryReport::default());
        }
        let mut victims = BTreeMap::new();
        for entry in &retiring_entries {
            let container_id = entry.location().container_id();
            victims.insert(container_id.bytes(), container_id);
        }
        self.containers.install_retiring_selection_barrier(&victims);
        let removal = self
            .containers
            .remove_recovered_retiring(&retiring_entries)?;
        let mut removed = Vec::new();
        removed
            .try_reserve_exact(retiring_entries.len())
            .map_err(|_| MaintenanceError::OutOfMemory)?;
        for retiring in retiring_entries {
            removed.push(ExactIndexEntry::removed(retiring)?);
        }
        let retiring_locations_finalized =
            u64::try_from(removed.len()).map_err(|_| MaintenanceError::ArithmeticOverflow)?;
        let transition = self
            .indexes
            .append_level_zero(self.exact_profile, removed)?;
        let activation_generation = transition.current().record().generation();
        self.containers.remove_retiring_selection_barrier(&victims);
        Ok(OnlineGcRecoveryReport {
            retiring_containers: u64::try_from(victims.len())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?,
            containers_removed: removal.containers_removed,
            containers_already_absent: removal.containers_already_absent,
            bytes_removed: removal.bytes_removed,
            retiring_locations_finalized,
            activation_generation: Some(activation_generation),
        })
    }

    fn gc_exact_binding_is_current(
        &self,
        expected: ExactIndexActivationRecord,
    ) -> Result<bool, MaintenanceError> {
        Ok(self
            .indexes
            .recover_active()?
            .is_some_and(|active| active.record() == expected))
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
                retiring_activation_wall: Duration::ZERO,
                pin_drain_wall: Duration::ZERO,
                victim_verify_wall: Duration::ZERO,
                unlink_wall: Duration::ZERO,
                data_sync_wall: Duration::ZERO,
                removed_activation_wall: Duration::ZERO,
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
            thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
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
        let (bytes_removed, removal_metrics) = self
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
            retiring_activation_wall: Duration::ZERO,
            pin_drain_wall: Duration::ZERO,
            victim_verify_wall: removal_metrics.verify_wall(),
            unlink_wall: removal_metrics.unlink_wall(),
            data_sync_wall: removal_metrics.sync_wall(),
            removed_activation_wall: Duration::ZERO,
        })
    }

    fn publish_gc_replacements(
        &self,
        victims: &BTreeMap<[u8; 16], ContainerId>,
        required: &BTreeMap<ChunkId, u64>,
        first_generation: u64,
        relocation_workers: NonZeroUsize,
    ) -> Result<ReplacementPublication, MaintenanceError> {
        self.publish_gc_replacements_using(
            victims,
            required,
            first_generation,
            relocation_workers,
            |container_id| Ok(self.containers.read(container_id)?),
        )
    }

    fn publish_gc_replacements_using(
        &self,
        victims: &BTreeMap<[u8; 16], ContainerId>,
        required: &BTreeMap<ChunkId, u64>,
        first_generation: u64,
        relocation_workers: NonZeroUsize,
        mut read_victim: impl FnMut(ContainerId) -> Result<SealedContainer, MaintenanceError>,
    ) -> Result<ReplacementPublication, MaintenanceError> {
        let mut seen = BTreeSet::new();
        let mut batch = Vec::<Vec<u8>>::new();
        let mut batch_bytes = 0_u64;
        let mut generation = first_generation;
        let mut published = ReplacementPublication::default();
        for container_id in victims.values().copied() {
            let container = read_victim(container_id)?;
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
                    let completed =
                        self.publish_gc_replacement_batch(generation, &batch, relocation_workers)?;
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
            published.add(self.publish_gc_replacement_batch(
                generation,
                &batch,
                relocation_workers,
            )?)?;
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
        relocation_workers: NonZeroUsize,
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
        let verified = self.containers.publish_gc_replacement_adaptive_verified(
            container_id,
            generation,
            &region_refs,
            relocation_workers,
        )?;
        assert_eq!(
            verified.chunk_count(),
            chunks.len(),
            "ASSERT: replacement writer reread must cover the planned Chunk batch"
        );
        let mut locations = Vec::new();
        locations
            .try_reserve_exact(verified.locations().len())
            .map_err(|_| MaintenanceError::OutOfMemory)?;
        for location in verified.locations().iter().copied() {
            locations.push(ExactIndexEntry::from_verified(location)?);
        }
        Ok(ReplacementPublication {
            containers: 1,
            bytes: verified.header().layout().file_length,
            chunks: u64::try_from(chunks.len())
                .map_err(|_| MaintenanceError::ArithmeticOverflow)?,
            locations,
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

    /// Rebuilds Exact and Similarity indexes from one verified Container scan.
    ///
    /// Similarity partitions remain hidden while the Exact Run Set is staged.
    /// Exact activation happens first; the bound Similarity family manifest is
    /// the final advanced-reduction commit point.
    ///
    /// # Errors
    ///
    /// Returns the first Container verification, index staging, audit,
    /// activation, I/O, allocation, or checked-arithmetic failure.
    ///
    /// # Panics
    ///
    /// Panics if a prior invariant panic poisoned the process-local rebuild
    /// lock, or if an activated family loses its staged Exact binding.
    pub fn rebuild_pool_indexes<S>(
        &self,
        similarities: &SimilarityIndexRepository<S>,
    ) -> Result<PoolIndexRebuildReport, MaintenanceError>
    where
        S: Clone + StorageIo,
    {
        let _guard = self
            .rebuild_lock
            .lock()
            .expect("ASSERT: pool-index rebuild lock poisoned");
        let exact_generation = self.next_exact_run_set_generation()?;
        let similarity_generation = similarities
            .discover_generation_high_water()?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(MaintenanceError::ArithmeticOverflow)?;
        let mut similarity_stager = similarities.entry_stager(similarity_generation);
        let staged_exact =
            self.stage_exact_index_excluding(exact_generation, &BTreeMap::new(), |container| {
                if container.records().len() != container.locations().len() {
                    return Err(MaintenanceError::ContainerRecordLocationMismatch);
                }
                for (record, location) in container.records().iter().zip(container.locations()) {
                    let logical_length = u32::try_from(record.payload().len())
                        .map_err(|_| MaintenanceError::ArithmeticOverflow)?;
                    if record.chunk_id() != location.chunk_id()
                        || logical_length != location.logical_length()
                    {
                        return Err(MaintenanceError::ContainerRecordLocationMismatch);
                    }
                    if location.dependency_id() != [0; 32] {
                        // A dependent target may be indexed exactly, but it
                        // must never become a Depth-1 Base candidate.
                        continue;
                    }
                    similarity_stager.push(similarity_index_entry_v1_from_verified(
                        record.chunk_id(),
                        record.payload(),
                    )?)?;
                }
                Ok(())
            })?;
        let exact_run_set_id = staged_exact.run_set.id()?;
        let staged_similarity = similarities.finish_staged_entries(
            similarity_generation,
            similarity_stager,
            exact_run_set_id,
        )?;
        let similarity_entries = staged_similarity.family().logical_entry_count();
        let similarity_partitions = staged_similarity.family().partitions().len();
        let exact = self.activate_staged_exact(&staged_exact)?;
        let family = similarities.activate_staged_family(staged_similarity)?;
        assert_eq!(
            family.source_exact_run_set_id(),
            Some(exact_run_set_id),
            "ASSERT: activated Similarity family remains bound to staged Exact Run Set"
        );
        Ok(PoolIndexRebuildReport {
            exact,
            similarity_generation,
            similarity_entries,
            similarity_partitions,
        })
    }

    fn rebuild_exact_index_excluding(
        &self,
        excluded: &BTreeMap<[u8; 16], ContainerId>,
    ) -> Result<ExactIndexRebuildReport, MaintenanceError> {
        let _guard = self
            .rebuild_lock
            .lock()
            .expect("ASSERT: Exact-Index rebuild lock poisoned");
        let run_set_generation = self.next_exact_run_set_generation()?;
        let staged = self.stage_exact_index_excluding(run_set_generation, excluded, |_| Ok(()))?;
        self.activate_staged_exact(&staged)
    }

    fn next_exact_run_set_generation(&self) -> Result<u64, MaintenanceError> {
        let previous = self.indexes.recover_active()?;
        if previous
            .as_ref()
            .is_some_and(|active| active.record().profile() != self.exact_profile)
        {
            return Err(MaintenanceError::ExactProfileMismatch);
        }
        previous.as_ref().map_or(Ok(1), |active| {
            active
                .run_set()
                .generation()
                .checked_add(1)
                .ok_or(MaintenanceError::ArithmeticOverflow)
        })
    }

    fn stage_exact_index_excluding<F>(
        &self,
        run_set_generation: u64,
        excluded: &BTreeMap<[u8; 16], ContainerId>,
        mut visit_for_similarity: F,
    ) -> Result<StagedExactIndexRebuild, MaintenanceError>
    where
        F: FnMut(&SealedContainer) -> Result<(), MaintenanceError>,
    {
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
                visit_for_similarity(container)?;
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
        Ok(StagedExactIndexRebuild {
            containers_scanned: containers.containers(),
            entries_rebuilt,
            run_families,
            physical_runs,
            run_set,
        })
    }

    fn activate_staged_exact(
        &self,
        staged: &StagedExactIndexRebuild,
    ) -> Result<ExactIndexRebuildReport, MaintenanceError> {
        let run_set_generation = staged.run_set.generation();
        let active = self.indexes.activate(&staged.run_set)?;
        Ok(ExactIndexRebuildReport {
            containers_scanned: staged.containers_scanned,
            entries_rebuilt: staged.entries_rebuilt,
            run_families: staged.run_families,
            physical_runs: staged.physical_runs,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ReplacementPublication {
    containers: u64,
    bytes: u64,
    chunks: u64,
    locations: Vec<ExactIndexEntry>,
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
        self.locations
            .try_reserve(other.locations.len())
            .map_err(|_| MaintenanceError::OutOfMemory)?;
        self.locations.extend(other.locations);
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
    mode: MaintenanceExecutionMode,
    name: &str,
    work: F,
) -> Result<R, MaintenanceError>
where
    R: Send + 'static,
    F: FnOnce() -> Result<R, MaintenanceError> + Send + 'static,
{
    if mode == MaintenanceExecutionMode::FullSpeed {
        return work();
    }
    thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            maintenance_ioprio::set_current_thread_idle()
                .map_err(MaintenanceError::MaintenanceIoPriority)?;
            if priority == MaintenancePriority::Background {
                rustix::process::nice(BACKGROUND_NICE_INCREMENT).map_err(|error| {
                    MaintenanceError::BackgroundPriority(io::Error::from(error))
                })?;
            }
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

struct StagedExactIndexRebuild {
    containers_scanned: u64,
    entries_rebuilt: u64,
    run_families: usize,
    physical_runs: usize,
    run_set: ExactIndexRunSet,
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

/// Compact evidence from one successful paired Exact/Similarity rebuild.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolIndexRebuildReport {
    exact: ExactIndexRebuildReport,
    similarity_generation: u64,
    similarity_entries: u64,
    similarity_partitions: usize,
}

impl PoolIndexRebuildReport {
    #[must_use]
    pub const fn exact(self) -> ExactIndexRebuildReport {
        self.exact
    }

    #[must_use]
    pub const fn similarity_generation(self) -> u64 {
        self.similarity_generation
    }

    #[must_use]
    pub const fn similarity_entries(self) -> u64 {
        self.similarity_entries
    }

    #[must_use]
    pub const fn similarity_partitions(self) -> usize {
        self.similarity_partitions
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
    generation_proof: GenerationLivenessProof,
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

/// Opaque generation-bound projection from live target Chunks to every Base
/// Chunk named by an effective ACTIVE Exact Location.
#[derive(Debug)]
pub struct ReverseDependencyGeneration {
    exact_activation: ExactIndexActivationRecord,
    protected_commit_generation: Option<u64>,
    protected_targets: BTreeSet<ChunkId>,
    required_chunks: BTreeMap<ChunkId, u64>,
    dependents_by_base: BTreeMap<ChunkId, BTreeSet<ChunkId>>,
    dependency_edges: u64,
}

impl ReverseDependencyGeneration {
    #[must_use]
    pub fn exact_activation(&self) -> ExactIndexActivationRecord {
        self.exact_activation
    }

    #[must_use]
    pub const fn protected_commit_generation(&self) -> Option<u64> {
        self.protected_commit_generation
    }

    #[must_use]
    pub fn required_chunks(&self) -> usize {
        self.required_chunks.len()
    }

    #[must_use]
    pub fn base_chunks(&self) -> usize {
        self.dependents_by_base.len()
    }

    #[must_use]
    pub const fn dependency_edges(&self) -> u64 {
        self.dependency_edges
    }
}

/// Opaque bounded authority for one candidate-local compaction. Construction
/// fully verifies only selected victims and carries a Reverse Dependency
/// Generation bound to the same Commit and Exact generations.
#[derive(Debug)]
pub struct GcCandidateProof {
    catalog: GcCandidateCatalogDescriptor,
    generation_proof: GenerationLivenessProof,
    reverse_dependencies: Arc<ReverseDependencyGeneration>,
    exact_profile: ExactIndexProfileId,
    victims: BTreeMap<[u8; 16], ContainerId>,
    victim_bytes: u64,
    replacement_chunks: BTreeMap<ChunkId, u64>,
    replacement_upper: u64,
    reachable_victim_chunks: usize,
    priority: MaintenancePriority,
}

/// Opaque post-activation authority for one online GC victim set.
///
/// Construction proves that replacements are durable and the RETIRING barrier
/// is active. Only [`MaintenanceRepository::finish_online_gc_retirement`] may
/// consume it to wait for pins and remove physical Containers.
#[derive(Debug)]
pub struct OnlineGcRetirement<X> {
    victims: BTreeMap<[u8; 16], ContainerId>,
    victim_bytes: u64,
    replacements: ReplacementPublication,
    retiring_entries: Vec<ExactIndexEntry>,
    drain: ExactIndexGenerationDrain<X>,
    priority: MaintenancePriority,
    retiring_activation_wall: Duration,
}

impl<X> OnlineGcRetirement<X> {
    #[must_use]
    pub fn victim_containers(&self) -> usize {
        self.victims.len()
    }

    #[must_use]
    pub const fn victim_bytes(&self) -> u64 {
        self.victim_bytes
    }

    #[must_use]
    pub fn pins_drained(&self) -> bool {
        self.drain.is_drained()
    }
}

impl GcCandidateProof {
    #[must_use]
    pub const fn catalog(&self) -> GcCandidateCatalogDescriptor {
        self.catalog
    }

    #[must_use]
    pub fn victim_containers(&self) -> usize {
        self.victims.len()
    }

    #[must_use]
    pub const fn victim_bytes(&self) -> u64 {
        self.victim_bytes
    }

    #[must_use]
    pub fn replacement_chunks(&self) -> usize {
        self.replacement_chunks.len()
    }

    #[must_use]
    pub const fn replacement_upper_bound(&self) -> u64 {
        self.replacement_upper
    }

    #[must_use]
    pub const fn reachable_victim_chunks(&self) -> usize {
        self.reachable_victim_chunks
    }

    #[must_use]
    pub fn exact_activation(&self) -> ExactIndexActivationRecord {
        self.reverse_dependencies.exact_activation
    }

    #[must_use]
    pub fn reverse_dependency_edges(&self) -> u64 {
        self.reverse_dependencies.dependency_edges
    }

    #[must_use]
    pub fn reverse_dependency_required_chunks(&self) -> usize {
        self.reverse_dependencies.required_chunks.len()
    }

    #[must_use]
    pub const fn priority(&self) -> MaintenancePriority {
        self.priority
    }
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
    retiring_activation_wall: Duration,
    pin_drain_wall: Duration,
    victim_verify_wall: Duration,
    unlink_wall: Duration,
    data_sync_wall: Duration,
    removed_activation_wall: Duration,
}

/// Evidence returned after one exact verified Metadata-object deletion batch
/// and durability barrier, or after an unchanged liveness epoch reused that
/// batch's clean mark-catalog result.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataGarbageCollectionReport {
    objects_removed: u64,
    bytes_removed: u64,
    objects_retained: u64,
    mark_mode: crate::MetadataGcMarkMode,
    exact_reason: Option<crate::MetadataGcExactReason>,
    catalog_generation: Option<u64>,
    metrics: crate::MetadataGcMetrics,
}

impl MetadataGarbageCollectionReport {
    #[must_use]
    pub const fn objects_removed(self) -> u64 {
        self.objects_removed
    }

    #[must_use]
    pub const fn bytes_removed(self) -> u64 {
        self.bytes_removed
    }

    #[must_use]
    pub const fn objects_retained(self) -> u64 {
        self.objects_retained
    }

    /// Returns `true` when this cycle traversed the complete retained graph
    /// and inventoried published Metadata Objects instead of reusing a clean
    /// incrementally invalidated catalog state.
    #[must_use]
    pub const fn exact_mark_performed(self) -> bool {
        self.mark_mode.has_deletion_authority()
    }

    #[must_use]
    pub const fn mark_mode(self) -> crate::MetadataGcMarkMode {
        self.mark_mode
    }

    #[must_use]
    pub const fn exact_reason(self) -> Option<crate::MetadataGcExactReason> {
        self.exact_reason
    }

    #[must_use]
    pub const fn catalog_generation(self) -> Option<u64> {
        self.catalog_generation
    }

    #[must_use]
    pub const fn metrics(self) -> crate::MetadataGcMetrics {
        self.metrics
    }
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

    #[must_use]
    pub const fn retiring_activation_wall(self) -> Duration {
        self.retiring_activation_wall
    }

    #[must_use]
    pub const fn pin_drain_wall(self) -> Duration {
        self.pin_drain_wall
    }

    #[must_use]
    pub const fn victim_verify_wall(self) -> Duration {
        self.victim_verify_wall
    }

    #[must_use]
    pub const fn unlink_wall(self) -> Duration {
        self.unlink_wall
    }

    #[must_use]
    pub const fn data_sync_wall(self) -> Duration {
        self.data_sync_wall
    }

    #[must_use]
    pub const fn removed_activation_wall(self) -> Duration {
        self.removed_activation_wall
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
    metadata_gc: MetadataGarbageCollectionReport,
}

/// Evidence from idempotently finalizing one recovered RETIRING generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OnlineGcRecoveryReport {
    retiring_containers: u64,
    containers_removed: u64,
    containers_already_absent: u64,
    bytes_removed: u64,
    retiring_locations_finalized: u64,
    activation_generation: Option<u64>,
}

/// Result of one bounded adaptive Online-GC scheduler quantum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OnlineGcCycleReport {
    outcome: OnlineGcCycleOutcome,
    catalog: GcCandidateCatalogDescriptor,
    metadata_gc: MetadataGarbageCollectionReport,
    metrics: OnlineGcMetrics,
}

impl OnlineGcCycleReport {
    #[must_use]
    pub const fn outcome(self) -> OnlineGcCycleOutcome {
        self.outcome
    }

    #[must_use]
    pub const fn catalog(self) -> GcCandidateCatalogDescriptor {
        self.catalog
    }

    #[must_use]
    pub const fn metadata_gc(self) -> MetadataGarbageCollectionReport {
        self.metadata_gc
    }

    #[must_use]
    pub const fn metrics(self) -> OnlineGcMetrics {
        self.metrics
    }
}

/// Phase timing and physical work accounting for one bounded Online-GC
/// quantum. Byte counts describe verified Container and immutable catalog
/// work; they deliberately exclude filesystem cache hit/miss guesses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OnlineGcMetrics {
    recovery_wall: Duration,
    metadata_gc_wall: Duration,
    candidate_catalog_wall: Duration,
    candidate_proof_wall: Duration,
    relocation_wall: Duration,
    retiring_activation_wall: Duration,
    pin_drain_wall: Duration,
    victim_verify_wall: Duration,
    unlink_wall: Duration,
    data_sync_wall: Duration,
    removed_activation_wall: Duration,
    post_collection_catalog_wall: Duration,
    total_wall: Duration,
    catalog_examined_bytes: u64,
    catalog_write_bytes: u64,
    candidate_proof_read_bytes: u64,
    relocation_read_bytes: u64,
    relocation_write_bytes: u64,
    unlinked_bytes: u64,
    shortlisted_candidates: u64,
    proved_victims: u64,
    aborted_candidates: u64,
    reverse_dependency_edges: u64,
    reverse_dependency_required_chunks: u64,
    relocation_workers: u64,
}

macro_rules! online_gc_metric_getter {
    ($name:ident, $field:ident, $ty:ty) => {
        #[must_use]
        pub const fn $name(self) -> $ty {
            self.$field
        }
    };
}

impl OnlineGcMetrics {
    online_gc_metric_getter!(recovery_wall, recovery_wall, Duration);
    online_gc_metric_getter!(metadata_gc_wall, metadata_gc_wall, Duration);
    online_gc_metric_getter!(candidate_catalog_wall, candidate_catalog_wall, Duration);
    online_gc_metric_getter!(candidate_proof_wall, candidate_proof_wall, Duration);
    online_gc_metric_getter!(relocation_wall, relocation_wall, Duration);
    online_gc_metric_getter!(retiring_activation_wall, retiring_activation_wall, Duration);
    online_gc_metric_getter!(pin_drain_wall, pin_drain_wall, Duration);
    online_gc_metric_getter!(victim_verify_wall, victim_verify_wall, Duration);
    online_gc_metric_getter!(unlink_wall, unlink_wall, Duration);
    online_gc_metric_getter!(data_sync_wall, data_sync_wall, Duration);
    online_gc_metric_getter!(removed_activation_wall, removed_activation_wall, Duration);
    online_gc_metric_getter!(
        post_collection_catalog_wall,
        post_collection_catalog_wall,
        Duration
    );
    online_gc_metric_getter!(total_wall, total_wall, Duration);
    online_gc_metric_getter!(catalog_examined_bytes, catalog_examined_bytes, u64);
    online_gc_metric_getter!(catalog_write_bytes, catalog_write_bytes, u64);
    online_gc_metric_getter!(candidate_proof_read_bytes, candidate_proof_read_bytes, u64);
    online_gc_metric_getter!(relocation_read_bytes, relocation_read_bytes, u64);
    online_gc_metric_getter!(relocation_write_bytes, relocation_write_bytes, u64);
    online_gc_metric_getter!(unlinked_bytes, unlinked_bytes, u64);
    online_gc_metric_getter!(shortlisted_candidates, shortlisted_candidates, u64);
    online_gc_metric_getter!(proved_victims, proved_victims, u64);
    online_gc_metric_getter!(aborted_candidates, aborted_candidates, u64);
    online_gc_metric_getter!(reverse_dependency_edges, reverse_dependency_edges, u64);
    online_gc_metric_getter!(
        reverse_dependency_required_chunks,
        reverse_dependency_required_chunks,
        u64
    );
    online_gc_metric_getter!(relocation_workers, relocation_workers, u64);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OnlineGcCycleOutcome {
    NoCandidates,
    NoProfitableCandidates,
    CatalogRebuilt,
    Collected(GarbageCollectionReport),
}

impl OnlineGcRecoveryReport {
    #[must_use]
    pub const fn retiring_containers(self) -> u64 {
        self.retiring_containers
    }

    #[must_use]
    pub const fn containers_removed(self) -> u64 {
        self.containers_removed
    }

    #[must_use]
    pub const fn containers_already_absent(self) -> u64 {
        self.containers_already_absent
    }

    #[must_use]
    pub const fn bytes_removed(self) -> u64 {
        self.bytes_removed
    }

    #[must_use]
    pub const fn retiring_locations_finalized(self) -> u64 {
        self.retiring_locations_finalized
    }

    #[must_use]
    pub const fn activation_generation(self) -> Option<u64> {
        self.activation_generation
    }
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

    #[must_use]
    pub const fn metadata_gc(self) -> MetadataGarbageCollectionReport {
        self.metadata_gc
    }
}

#[derive(Debug)]
pub enum MaintenanceError {
    Store(StoreError),
    Generation(GenerationError),
    ExactIndex(ExactIndexStoreError),
    SimilarityIndex(SimilarityIndexStoreError),
    GcCandidateCatalog(GcCandidateCatalogStoreError),
    ExactProfileMismatch,
    GcPlanProfileMismatch,
    StaleGcPlan,
    GcProofRequiresActiveExactIndex,
    MissingGcCandidateCatalog,
    GcCandidateProofBudgetExceeded,
    GcCandidateIdentityMismatch,
    IncompleteReverseDependencyGeneration {
        chunk_id: ChunkId,
    },
    MissingLiveExactLocation {
        chunk_id: ChunkId,
    },
    GcRetirementMissingPreviousGeneration,
    EmptyGcCandidateProof,
    UnprofitableGcCandidateProof {
        victim_bytes: u64,
        replacement_upper: u64,
    },
    BackgroundPriority(io::Error),
    MaintenanceIoPriority(io::Error),
    MaintenanceThread(io::Error),
    MaintenanceThreadPanicked,
    OnlineChunkLengthMismatch {
        chunk_id: ChunkId,
        expected: u64,
        observed: u64,
    },
    MissingReplacementChunk,
    ReplacementIdentity,
    ContainerRecordLocationMismatch,
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

impl From<SimilarityIndexStoreError> for MaintenanceError {
    fn from(error: SimilarityIndexStoreError) -> Self {
        Self::SimilarityIndex(error)
    }
}

impl From<GcCandidateCatalogStoreError> for MaintenanceError {
    fn from(error: GcCandidateCatalogStoreError) -> Self {
        Self::GcCandidateCatalog(error)
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
