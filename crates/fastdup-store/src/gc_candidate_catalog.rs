use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use fastdup_format::{
    ContainerId, ExactLocationTransition, FormatError, GC_CANDIDATE_CATALOG_HEADER_BYTES,
    GC_CANDIDATE_CATALOG_ROW_BYTES, GcCandidateCatalogDescriptor, GcCandidateCatalogError,
    GcCandidateCatalogRow, GcCandidateCatalogStreamEncoder, GcCandidateLocationState,
    VerifiedContainerPublication,
};

use crate::gc_candidate_mmap::ImmutableGcCandidateCatalog;
use crate::{
    ActivatedExactIndex, ExactIndexStoreError, GenerationLivenessDelta, StorageIo, StoreError,
};

const AUDIT_BATCH_ROWS: u64 = 8_192;
const ROW_WRITE_BATCH_BYTES: usize = 8_192 * GC_CANDIDATE_CATALOG_ROW_BYTES;
const MAX_SHORTLIST_ROWS: usize = 4_096;
const PUBLISHED_PREFIX: &str = "gc-candidate-catalog-";
const PUBLISHED_SUFFIX: &str = ".run";

/// Converts payload-free publication evidence into the immutable seed row used
/// by the next catalog generation.
///
/// This scans compact Location evidence only; it performs no Container read,
/// decompression, payload copy, or Chunk hashing.
///
/// # Errors
///
/// Returns a publication-evidence or candidate-row invariant failure.
pub fn gc_candidate_row_from_publication(
    publication: &VerifiedContainerPublication,
) -> Result<GcCandidateCatalogRow, GcCandidateCatalogStoreError> {
    let summary = publication.intrinsic_summary()?;
    Ok(GcCandidateCatalogRow::from_intrinsic_summary(
        publication.header().container_id(),
        publication.header().container_generation(),
        publication.header().layout().file_length,
        summary,
    )?)
}

/// Immutable GC-candidate acceleration with streaming publication and bounded
/// or mmap-backed scans.
#[derive(Clone, Debug)]
pub struct GcCandidateCatalogRepository<I> {
    storage: I,
    publish_lock: Arc<Mutex<()>>,
}

impl<I: Clone + StorageIo> GcCandidateCatalogRepository<I> {
    #[must_use]
    pub fn new(storage: I) -> Self {
        Self {
            storage,
            publish_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Streams one complete sorted catalog generation into a no-replace `RoW`
    /// publication. The implementation retains no pool-sized row collection.
    ///
    /// # Errors
    ///
    /// Returns format, order, count, I/O, reread, collision, or durability
    /// failures.
    ///
    /// # Panics
    ///
    /// Panics if a prior invariant panic poisoned the process-local writer
    /// lock.
    pub fn publish_rows<R>(
        &self,
        generation: u64,
        incorporated_commit_generation: u64,
        incorporated_location_generation: u64,
        row_count: u64,
        rows: R,
    ) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError>
    where
        R: IntoIterator<Item = GcCandidateCatalogRow>,
    {
        self.publish_generated(
            generation,
            incorporated_commit_generation,
            incorporated_location_generation,
            row_count,
            |emit| {
                for row in rows {
                    emit(row)?;
                }
                Ok(())
            },
        )
    }

    /// Merges a bounded sorted set of publication or Metadata/Location updates
    /// into a new complete immutable generation without materializing the
    /// previous pool catalog.
    ///
    /// An update with an existing Container ID may change only estimated and
    /// Location-state fields; immutable publication identity is checked before
    /// replacement. An update with a new ID inserts a newly published
    /// Container. Missing updates retain their prior rows.
    ///
    /// # Errors
    ///
    /// Returns stale/changed immutable identity, update order, source audit,
    /// format, I/O, collision, or durability failures.
    pub fn publish_successor(
        &self,
        previous: &GcCandidateCatalogSnapshot<I>,
        generation: u64,
        incorporated_commit_generation: u64,
        incorporated_location_generation: u64,
        updates: &[GcCandidateCatalogRow],
    ) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError> {
        let prior = previous.descriptor();
        if generation <= prior.generation()
            || incorporated_commit_generation < prior.incorporated_commit_generation()
            || incorporated_location_generation < prior.incorporated_location_generation()
        {
            return Err(GcCandidateCatalogStoreError::StaleSuccessor);
        }
        validate_update_order(updates)?;
        let mut update_index = 0_usize;
        let mut row_count = 0_u64;
        previous.visit_rows(|row| {
            while updates
                .get(update_index)
                .is_some_and(|update| update.container_id().bytes() < row.container_id().bytes())
            {
                row_count = row_count
                    .checked_add(1)
                    .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
                update_index += 1;
            }
            if let Some(update) = updates.get(update_index)
                && update.container_id() == row.container_id()
            {
                require_same_intrinsic_row(row, *update)?;
                update_index += 1;
            }
            row_count = row_count
                .checked_add(1)
                .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
            Ok(())
        })?;
        row_count = row_count
            .checked_add(
                u64::try_from(updates.len() - update_index)
                    .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?,
            )
            .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;

        self.publish_generated(
            generation,
            incorporated_commit_generation,
            incorporated_location_generation,
            row_count,
            |emit| {
                let mut update_index = 0_usize;
                previous.visit_rows(|row| {
                    while updates.get(update_index).is_some_and(|update| {
                        update.container_id().bytes() < row.container_id().bytes()
                    }) {
                        emit(updates[update_index])?;
                        update_index += 1;
                    }
                    if let Some(update) = updates.get(update_index)
                        && update.container_id() == row.container_id()
                    {
                        require_same_intrinsic_row(row, *update)?;
                        emit(*update)?;
                        update_index += 1;
                    } else {
                        emit(row)?;
                    }
                    Ok(())
                })?;
                for update in &updates[update_index..] {
                    emit(*update)?;
                }
                Ok(())
            },
        )
    }

    /// Applies one Metadata-only liveness delta through bounded Exact-Index
    /// lookups and publishes the next immutable hint generation.
    ///
    /// Missing, incomplete, or stale Exact entries can only leave a row
    /// unknown or imprecise. The catalog remains non-authoritative and a later
    /// `GcCandidateProof` never trusts these counts.
    ///
    /// # Errors
    ///
    /// Returns freshness, Exact lookup, row, successor, or storage failures.
    ///
    /// # Panics
    ///
    /// Panics only if a format-validated Exact Location exposes the forbidden
    /// all-zero Container ID.
    pub fn publish_liveness_delta<X: Clone + StorageIo>(
        &self,
        previous: &GcCandidateCatalogSnapshot<I>,
        generation: u64,
        delta: &GenerationLivenessDelta,
        exact: &ActivatedExactIndex<X>,
    ) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError> {
        let descriptor = previous.descriptor();
        if descriptor.incorporated_commit_generation() != delta.base_generation().unwrap_or(0) {
            return Err(GcCandidateCatalogStoreError::LivenessDeltaBaseMismatch);
        }
        let latest = delta.latest_generation().unwrap_or(0);
        let mut changes = BTreeMap::<[u8; 16], i64>::new();
        for (chunk_id, logical_length, direction) in delta
            .added()
            .iter()
            .map(|(id, length)| (*id, *length, 1_i64))
            .chain(
                delta
                    .removed()
                    .iter()
                    .map(|(id, length)| (*id, *length, -1_i64)),
            )
        {
            let logical_length = u32::try_from(logical_length)
                .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
            let lookup = exact.lookup_transitions(chunk_id, logical_length)?;
            let mut seen_locations = BTreeSet::new();
            let mut seen_containers = BTreeSet::new();
            for entry in lookup.candidates() {
                let location = entry.location();
                let location_key = (
                    location.container_id().bytes(),
                    location.record_offset(),
                    location.chunk_ordinal(),
                );
                if !seen_locations.insert(location_key)
                    || entry.transition() != ExactLocationTransition::Active
                    || !seen_containers.insert(location.container_id().bytes())
                {
                    continue;
                }
                let change = changes.entry(location.container_id().bytes()).or_default();
                *change = change
                    .checked_add(direction)
                    .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
            }
        }
        let mut updates = Vec::new();
        updates
            .try_reserve_exact(changes.len())
            .map_err(|_| GcCandidateCatalogStoreError::OutOfMemory)?;
        for (container_id, change) in changes {
            let container_id = ContainerId::new(container_id)
                .expect("ASSERT: Exact Location contains one validated nonzero Container ID");
            let Some(row) = previous.find_row(container_id)? else {
                continue;
            };
            updates.push(row.with_reachable_target_delta(change)?);
        }
        self.publish_successor(
            previous,
            generation,
            latest,
            exact.record().generation(),
            &updates,
        )
    }

    pub(crate) fn publish_generated(
        &self,
        generation: u64,
        incorporated_commit_generation: u64,
        incorporated_location_generation: u64,
        row_count: u64,
        generate: impl FnOnce(
            &mut dyn FnMut(GcCandidateCatalogRow) -> Result<(), GcCandidateCatalogStoreError>,
        ) -> Result<(), GcCandidateCatalogStoreError>,
    ) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError> {
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: GC candidate catalog publication lock poisoned");
        let published_name = published_name(generation);
        let temporary_name = temporary_name(generation);
        let already_published = self.storage.exists(&published_name)?;
        if !already_published {
            match self.storage.create_new(&temporary_name) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    self.storage.set_len(&temporary_name, 0)?;
                }
                Err(error) => return Err(error.into()),
            }
        }

        let mut encoder = GcCandidateCatalogStreamEncoder::new(
            generation,
            incorporated_commit_generation,
            incorporated_location_generation,
            row_count,
        )?;
        let mut row_batch = Vec::new();
        if !already_published {
            row_batch
                .try_reserve_exact(ROW_WRITE_BATCH_BYTES)
                .map_err(|_| GcCandidateCatalogStoreError::OutOfMemory)?;
        }
        let mut row_batch_offset = GC_CANDIDATE_CATALOG_HEADER_BYTES as u64;
        {
            let mut emit = |row| {
                let (offset, bytes) = encoder.push(row)?;
                if !already_published {
                    let expected_offset = row_batch_offset
                        .checked_add(
                            u64::try_from(row_batch.len())
                                .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?,
                        )
                        .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
                    if offset != expected_offset {
                        return Err(GcCandidateCatalogStoreError::IndexCorruption);
                    }
                    row_batch.extend_from_slice(&bytes);
                    if row_batch.len() == ROW_WRITE_BATCH_BYTES {
                        self.storage
                            .write_at(&temporary_name, row_batch_offset, &row_batch)?;
                        row_batch_offset = row_batch_offset
                            .checked_add(
                                u64::try_from(row_batch.len())
                                    .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?,
                            )
                            .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
                        row_batch.clear();
                    }
                }
                Ok(())
            };
            generate(&mut emit)?;
        }
        if !already_published && !row_batch.is_empty() {
            self.storage
                .write_at(&temporary_name, row_batch_offset, &row_batch)?;
        }
        let (expected, header, footer) = encoder.finish()?;
        if already_published {
            let observed = self.audit_named(&published_name)?;
            require_same_descriptor(expected, observed)?;
            self.storage.sync_root()?;
            return Ok(observed);
        }

        self.storage
            .set_len(&temporary_name, expected.file_length())?;
        self.storage.write_at(&temporary_name, 0, &header)?;
        self.storage
            .write_at(&temporary_name, expected.footer_offset(), &footer)?;
        let observed = self.audit_named(&temporary_name)?;
        require_same_descriptor(expected, observed)?;
        self.storage.sync_file(&temporary_name)?;
        match self
            .storage
            .publish_noreplace(&temporary_name, &published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let raced = self.audit_named(&published_name)?;
                require_same_descriptor(expected, raced)?;
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok(observed)
    }

    /// Recovers the newest completely valid catalog generation. A corrupt
    /// newer hint run is ignored in favor of an older valid generation.
    ///
    /// # Errors
    ///
    /// Returns directory or transient storage I/O failures. Catalog corruption
    /// is non-authoritative and therefore causes fallback rather than DATA or
    /// Namespace failure.
    pub fn recover_latest(
        &self,
    ) -> Result<Option<GcCandidateCatalogSnapshot<I>>, GcCandidateCatalogStoreError> {
        let mut generations = self
            .storage
            .list_names()?
            .into_iter()
            .filter_map(|name| {
                parse_published_generation(&name).map(|generation| (generation, name))
            })
            .collect::<Vec<_>>();
        generations.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));
        for (_generation, name) in generations {
            let descriptor = match self.audit_named(&name) {
                Ok(descriptor) => descriptor,
                Err(error) if error.is_catalog_corruption() => continue,
                Err(error) => return Err(error),
            };
            let source = match self
                .storage
                .lease_immutable_file(&name, descriptor.file_length())?
            {
                Some(lease) => CatalogSource::Mapped(Arc::new(ImmutableGcCandidateCatalog::open(
                    lease, descriptor,
                )?)),
                None => CatalogSource::Bounded {
                    storage: self.storage.clone(),
                    name,
                    descriptor,
                },
            };
            return Ok(Some(GcCandidateCatalogSnapshot { source }));
        }
        Ok(None)
    }

    /// Discovers the greatest published catalog generation from canonical
    /// names, including corrupt or orphaned hint objects.
    ///
    /// Allocation after this high-water prevents retry from reusing an
    /// immutable no-replace name that recovery deliberately ignored.
    ///
    /// # Errors
    ///
    /// Returns directory enumeration failures.
    pub fn discover_generation_high_water(
        &self,
    ) -> Result<Option<u64>, GcCandidateCatalogStoreError> {
        Ok(self
            .storage
            .list_names()?
            .into_iter()
            .filter_map(|name| parse_published_generation(&name))
            .max())
    }

    fn audit_named(
        &self,
        name: &str,
    ) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError> {
        audit_named(&self.storage, name)
    }
}

#[derive(Clone)]
pub struct GcCandidateCatalogSnapshot<I> {
    source: CatalogSource<I>,
}

#[derive(Clone)]
enum CatalogSource<I> {
    Mapped(Arc<ImmutableGcCandidateCatalog>),
    Bounded {
        storage: I,
        name: String,
        descriptor: GcCandidateCatalogDescriptor,
    },
}

impl<I: Clone + StorageIo> GcCandidateCatalogSnapshot<I> {
    #[must_use]
    pub fn descriptor(&self) -> GcCandidateCatalogDescriptor {
        match &self.source {
            CatalogSource::Mapped(catalog) => catalog.descriptor(),
            CatalogSource::Bounded { descriptor, .. } => *descriptor,
        }
    }

    #[must_use]
    pub const fn mapped(&self) -> bool {
        matches!(self.source, CatalogSource::Mapped(_))
    }

    /// Finds one Container row by binary search without materializing the
    /// catalog. The bounded adapter reads one 96-byte row per probe.
    ///
    /// # Errors
    ///
    /// Returns the first mapped or positional row failure.
    pub fn find_row(
        &self,
        container_id: ContainerId,
    ) -> Result<Option<GcCandidateCatalogRow>, GcCandidateCatalogStoreError> {
        let mut lower = 0_u64;
        let mut upper = self.descriptor().row_count();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let row = self.row_at(middle)?;
            match row.container_id().bytes().cmp(&container_id.bytes()) {
                std::cmp::Ordering::Less => lower = middle + 1,
                std::cmp::Ordering::Greater => upper = middle,
                std::cmp::Ordering::Equal => return Ok(Some(row)),
            }
        }
        Ok(None)
    }

    fn row_at(&self, ordinal: u64) -> Result<GcCandidateCatalogRow, GcCandidateCatalogStoreError> {
        match &self.source {
            CatalogSource::Mapped(catalog) => catalog.row(ordinal),
            CatalogSource::Bounded {
                storage,
                name,
                descriptor,
            } => {
                let offset = descriptor
                    .row_offset(ordinal)
                    .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
                let bytes = storage.read_exact_at(name, offset, GC_CANDIDATE_CATALOG_ROW_BYTES)?;
                Ok(descriptor.decode_row(ordinal, &bytes)?)
            }
        }
    }

    /// Scans immutable rows and retains only a bounded deterministic victim
    /// shortlist. Every returned row remains a hint tied to the descriptor's
    /// incorporated generations; it cannot authorize `RETIRING` or deletion.
    ///
    /// # Errors
    ///
    /// Returns invalid limits or the first mapped/bounded row audit failure.
    pub fn shortlist(
        &self,
        mode: GcCandidateSelectionMode,
        limit: usize,
        current_container_generation: u64,
    ) -> Result<GcCandidateShortlist, GcCandidateCatalogStoreError> {
        if limit == 0 || limit > MAX_SHORTLIST_ROWS {
            return Err(GcCandidateCatalogStoreError::InvalidShortlistLimit);
        }
        let descriptor = self.descriptor();
        let mut ranked = BinaryHeap::new();
        ranked
            .try_reserve_exact(limit)
            .map_err(|_| GcCandidateCatalogStoreError::OutOfMemory)?;
        self.visit_rows(|row| {
            if row.location_state() != GcCandidateLocationState::Active {
                return Ok(());
            }
            let rank = candidate_rank(row, mode, current_container_generation);
            let candidate = RankedCandidate { rank, row };
            if ranked.len() < limit {
                ranked.push(Reverse(candidate));
            } else if ranked
                .peek()
                .is_some_and(|worst| candidate.rank > worst.0.rank)
            {
                ranked.pop();
                ranked.push(Reverse(candidate));
            }
            Ok(())
        })?;
        let mut ranked = ranked
            .into_iter()
            .map(|candidate| candidate.0)
            .collect::<Vec<_>>();
        ranked.sort_unstable_by_key(|candidate| Reverse(candidate.rank));
        Ok(GcCandidateShortlist {
            descriptor,
            rows: ranked.into_iter().map(|candidate| candidate.row).collect(),
        })
    }

    fn visit_rows(
        &self,
        mut visit: impl FnMut(GcCandidateCatalogRow) -> Result<(), GcCandidateCatalogStoreError>,
    ) -> Result<(), GcCandidateCatalogStoreError> {
        match &self.source {
            CatalogSource::Mapped(catalog) => {
                for ordinal in 0..catalog.descriptor().row_count() {
                    visit(catalog.row(ordinal)?)?;
                }
                Ok(())
            }
            CatalogSource::Bounded {
                storage,
                name,
                descriptor,
            } => {
                let observed = audit_named_with(storage, name, visit)?;
                require_same_descriptor(*descriptor, observed)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GcCandidateSelectionMode {
    Urgent,
    Background,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcCandidateShortlist {
    descriptor: GcCandidateCatalogDescriptor,
    rows: Vec<GcCandidateCatalogRow>,
}

impl GcCandidateShortlist {
    #[must_use]
    pub const fn descriptor(&self) -> GcCandidateCatalogDescriptor {
        self.descriptor
    }

    #[must_use]
    pub fn rows(&self) -> &[GcCandidateCatalogRow] {
        &self.rows
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CandidateRank {
    confidence_tier: u8,
    score: u128,
    reclaim_hint: u64,
    inverse_relocation: u64,
    age: u64,
    inverse_container_id: [u8; 16],
}

#[derive(Clone, Copy, Debug)]
struct RankedCandidate {
    rank: CandidateRank,
    row: GcCandidateCatalogRow,
}

impl PartialEq for RankedCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.rank == other.rank
    }
}

impl Eq for RankedCandidate {}

impl PartialOrd for RankedCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank.cmp(&other.rank)
    }
}

fn candidate_rank(
    row: GcCandidateCatalogRow,
    mode: GcCandidateSelectionMode,
    current_generation: u64,
) -> CandidateRank {
    let age = current_generation.saturating_sub(row.container_generation());
    let dependency_closed_zero = row.estimate_known()
        && row.dependency_estimate_known()
        && row.reachable_target_count() == 0
        && row.incoming_base_fanout() == 0;
    let confidence_tier = if dependency_closed_zero {
        3
    } else if row.estimate_known() {
        2
    } else {
        1
    };
    let reclaim_hint = if dependency_closed_zero {
        row.physical_bytes()
    } else {
        u64::from(row.dead_record_bytes())
    };
    let relocation = if dependency_closed_zero {
        0
    } else {
        row.raw_replacement_upper_bound()
    };
    let score = match mode {
        GcCandidateSelectionMode::Urgent => u128::from(reclaim_hint),
        GcCandidateSelectionMode::Background => {
            let cost = row
                .physical_bytes()
                .saturating_add(row.raw_replacement_upper_bound())
                .max(1);
            u128::from(reclaim_hint).saturating_mul(u128::from(age.max(1))) / u128::from(cost)
        }
    };
    let mut inverse_container_id = row.container_id().bytes();
    for byte in &mut inverse_container_id {
        *byte = !*byte;
    }
    CandidateRank {
        confidence_tier,
        score,
        reclaim_hint,
        inverse_relocation: u64::MAX - relocation,
        age,
        inverse_container_id,
    }
}

fn validate_update_order(
    updates: &[GcCandidateCatalogRow],
) -> Result<(), GcCandidateCatalogStoreError> {
    if updates
        .windows(2)
        .any(|pair| pair[0].container_id().bytes() >= pair[1].container_id().bytes())
    {
        return Err(GcCandidateCatalogStoreError::InvalidUpdateOrder);
    }
    Ok(())
}

fn require_same_intrinsic_row(
    previous: GcCandidateCatalogRow,
    update: GcCandidateCatalogRow,
) -> Result<(), GcCandidateCatalogStoreError> {
    if previous.container_id() != update.container_id()
        || previous.container_generation() != update.container_generation()
        || previous.physical_bytes() != update.physical_bytes()
        || previous.summary_checksum() != update.summary_checksum()
        || previous.raw_replacement_upper_bound() != update.raw_replacement_upper_bound()
        || previous.outgoing_dependency_count() != update.outgoing_dependency_count()
    {
        return Err(GcCandidateCatalogStoreError::ImmutableUpdateMismatch);
    }
    Ok(())
}

fn audit_named<I: StorageIo>(
    storage: &I,
    name: &str,
) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError> {
    audit_named_with(storage, name, |_| Ok(()))
}

fn audit_named_with<I: StorageIo>(
    storage: &I,
    name: &str,
    mut visit: impl FnMut(GcCandidateCatalogRow) -> Result<(), GcCandidateCatalogStoreError>,
) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError> {
    let file_length = storage.object_len(name)?;
    if file_length < (2 * GC_CANDIDATE_CATALOG_HEADER_BYTES) as u64 {
        return Err(GcCandidateCatalogStoreError::IndexCorruption);
    }
    let header = storage.read_exact_at(name, 0, GC_CANDIDATE_CATALOG_HEADER_BYTES)?;
    let footer_offset = file_length
        .checked_sub(GC_CANDIDATE_CATALOG_HEADER_BYTES as u64)
        .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
    let footer = storage.read_exact_at(name, footer_offset, GC_CANDIDATE_CATALOG_HEADER_BYTES)?;
    let descriptor = GcCandidateCatalogDescriptor::decode(&header, &footer, file_length)?;
    let mut audit = descriptor.start_audit();
    let mut ordinal = 0_u64;
    while ordinal < descriptor.row_count() {
        let batch_rows = (descriptor.row_count() - ordinal).min(AUDIT_BATCH_ROWS);
        let offset = descriptor
            .row_offset(ordinal)
            .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
        let length = usize::try_from(
            batch_rows
                .checked_mul(GC_CANDIDATE_CATALOG_ROW_BYTES as u64)
                .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?,
        )
        .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
        let bytes = storage.read_exact_at(name, offset, length)?;
        for row_bytes in bytes.chunks_exact(GC_CANDIDATE_CATALOG_ROW_BYTES) {
            visit(audit.push(row_bytes)?)?;
        }
        ordinal += batch_rows;
    }
    let rows_end = descriptor
        .rows_end()
        .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
    let padding_length = usize::try_from(
        descriptor
            .footer_offset()
            .checked_sub(rows_end)
            .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?,
    )
    .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
    if padding_length != 0
        && storage
            .read_exact_at(name, rows_end, padding_length)?
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(GcCandidateCatalogStoreError::IndexCorruption);
    }
    audit.finish()?;
    Ok(descriptor)
}

fn require_same_descriptor(
    expected: GcCandidateCatalogDescriptor,
    observed: GcCandidateCatalogDescriptor,
) -> Result<(), GcCandidateCatalogStoreError> {
    if expected != observed {
        return Err(GcCandidateCatalogStoreError::PublishVerificationMismatch);
    }
    Ok(())
}

fn published_name(generation: u64) -> String {
    format!("{PUBLISHED_PREFIX}{generation:016x}{PUBLISHED_SUFFIX}")
}

fn temporary_name(generation: u64) -> String {
    format!(".{PUBLISHED_PREFIX}{generation:016x}{PUBLISHED_SUFFIX}.building")
}

fn parse_published_generation(name: &str) -> Option<u64> {
    let digits = name
        .strip_prefix(PUBLISHED_PREFIX)?
        .strip_suffix(PUBLISHED_SUFFIX)?;
    if digits.len() != 16 {
        return None;
    }
    u64::from_str_radix(digits, 16)
        .ok()
        .filter(|value| *value != 0)
}

#[derive(Debug)]
pub enum GcCandidateCatalogStoreError {
    Io(io::Error),
    Container(StoreError),
    Format(GcCandidateCatalogError),
    ContainerFormat(FormatError),
    Exact(ExactIndexStoreError),
    PublishVerificationMismatch,
    IdentityMismatch,
    IndexCorruption,
    InvalidUpdateOrder,
    ImmutableUpdateMismatch,
    StaleSuccessor,
    LivenessDeltaBaseMismatch,
    InvalidShortlistLimit,
    CounterOverflow,
    OutOfMemory,
}

impl GcCandidateCatalogStoreError {
    fn is_catalog_corruption(&self) -> bool {
        matches!(
            self,
            Self::Format(_)
                | Self::PublishVerificationMismatch
                | Self::IdentityMismatch
                | Self::IndexCorruption
        )
    }
}

impl fmt::Display for GcCandidateCatalogStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for GcCandidateCatalogStoreError {}

impl From<io::Error> for GcCandidateCatalogStoreError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<StoreError> for GcCandidateCatalogStoreError {
    fn from(error: StoreError) -> Self {
        Self::Container(error)
    }
}

impl From<GcCandidateCatalogError> for GcCandidateCatalogStoreError {
    fn from(error: GcCandidateCatalogError) -> Self {
        Self::Format(error)
    }
}

impl From<FormatError> for GcCandidateCatalogStoreError {
    fn from(error: FormatError) -> Self {
        Self::ContainerFormat(error)
    }
}

impl From<ExactIndexStoreError> for GcCandidateCatalogStoreError {
    fn from(error: ExactIndexStoreError) -> Self {
        Self::Exact(error)
    }
}
