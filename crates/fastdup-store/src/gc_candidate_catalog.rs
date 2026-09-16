use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use fastdup_format::{
    ContainerId, ExactLocationTransition, FormatError, GC_CANDIDATE_CATALOG_HEADER_BYTES,
    GC_CANDIDATE_CATALOG_ROW_BYTES, GC_FILL_COMPACTION_PHYSICAL_MAX_BYTES,
    GcCandidateCatalogDescriptor, GcCandidateCatalogError, GcCandidateCatalogRow,
    GcCandidateCatalogStreamEncoder, GcCandidateLocationState, VerifiedContainerPublication,
};

use crate::gc_candidate_read::ImmutableGcCandidateCatalog;
use crate::{
    ActivatedExactIndex, ExactIndexStoreError, GenerationLivenessDelta, StorageIo, StoreError,
};

const AUDIT_BATCH_ROWS: u64 = 8_192;
const ROW_WRITE_BATCH_BYTES: usize = 8_192 * GC_CANDIDATE_CATALOG_ROW_BYTES;
const MAX_SHORTLIST_ROWS: usize = 4_096;
pub(crate) const GC_CANDIDATE_QUEUE_CAPACITY: usize = 65_536;
pub(crate) const GC_PENDING_CATALOG_UPDATE_LIMIT: usize = 65_536;
pub(crate) const GC_CATALOG_SCAN_BATCH_ROWS: u64 = 8_192;
const GC_FILL_COMPACTION_PHYSICAL_SCORE_BASIS_BYTES: u64 = 1_024;
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
/// or leased Direct-I/O scans through the common cache.
#[derive(Clone, Debug)]
pub struct GcCandidateCatalogRepository<I> {
    storage: I,
    publish_lock: Arc<Mutex<()>>,
    cached_latest: Arc<Mutex<Option<CachedGcCandidateCatalog>>>,
}

#[derive(Clone, Debug)]
struct CachedGcCandidateCatalog {
    name: String,
    descriptor: GcCandidateCatalogDescriptor,
}

impl<I: Clone + StorageIo> GcCandidateCatalogRepository<I> {
    #[must_use]
    pub fn new(storage: I) -> Self {
        Self {
            storage,
            publish_lock: Arc::new(Mutex::new(())),
            cached_latest: Arc::new(Mutex::new(None)),
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
    pub(crate) fn candidate_updates_from_liveness_delta<X: Clone + StorageIo>(
        previous: &GcCandidateCatalogSnapshot<I>,
        delta: &GenerationLivenessDelta,
        exact: &ActivatedExactIndex<X>,
    ) -> Result<Vec<GcCandidateCatalogRow>, GcCandidateCatalogStoreError> {
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
        Ok(updates)
    }

    /// Publishes one successor that folds `delta` into the rows reachable
    /// through `previous` and the pinned active `exact` generation.
    ///
    /// # Errors
    ///
    /// Returns a base-generation mismatch, catalog lookup, Exact lookup,
    /// successor publication, or storage failure.
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
        let updates = Self::candidate_updates_from_liveness_delta(previous, delta, exact)?;
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
    /// The process-local cache retains only an already-audited immutable lease.
    /// A later call checks the highest canonical name and its Header/Footer
    /// envelope before reusing it; publication, replacement, removal, or a
    /// changed physical descriptor forces one fresh complete audit.
    ///
    /// # Errors
    ///
    /// Returns directory or transient storage I/O failures. Catalog corruption
    /// is non-authoritative and therefore causes fallback rather than DATA or
    /// Namespace failure.
    ///
    /// # Panics
    ///
    /// Panics if a prior invariant panic poisoned the process-local recovery
    /// cache lock.
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
        let cached = self
            .cached_latest
            .lock()
            .expect("ASSERT: GC candidate catalog recovery cache lock poisoned")
            .clone();
        if let Some(cached) = cached
            && let Some((_, name)) = generations
                .first()
                .filter(|(_, cached_name)| *cached_name == cached.name)
        {
            let descriptor = descriptor_named(&self.storage, name)?;
            if descriptor != cached.descriptor {
                self.invalidate_cached_latest();
            } else if let Some(lease) = self
                .storage
                .lease_immutable_file(name, descriptor.file_length())?
            {
                match ImmutableGcCandidateCatalog::open_verified(lease, descriptor) {
                    Ok(catalog) => {
                        return Ok(Some(GcCandidateCatalogSnapshot {
                            source: CatalogSource::Leased(Arc::new(catalog)),
                        }));
                    }
                    Err(error) if error.is_catalog_corruption() => {
                        self.invalidate_cached_latest();
                    }
                    Err(error) => return Err(error),
                }
            } else {
                // Bounded readers repeat the complete row audit while visiting
                // rows. A lease is unnecessary while the cached generation is
                // idle, and its absence keeps snapshot lifetime lease-neutral.
                return Ok(Some(GcCandidateCatalogSnapshot {
                    source: CatalogSource::Bounded {
                        storage: self.storage.clone(),
                        name: name.to_owned(),
                        descriptor,
                    },
                }));
            }
        } else if generations.is_empty() {
            self.invalidate_cached_latest();
        }

        for (_generation, name) in generations {
            let descriptor = match descriptor_named(&self.storage, &name) {
                Ok(descriptor) => descriptor,
                Err(error) if error.is_catalog_corruption() => continue,
                Err(error) => return Err(error),
            };
            if let Some(lease) = self
                .storage
                .lease_immutable_file(&name, descriptor.file_length())?
            {
                // The lease freezes the inode against every repository
                // mutation, so one complete audit of the leased bytes is the
                // reader boundary; no second unleased re-audit precedes use.
                let catalog = match ImmutableGcCandidateCatalog::open(lease, descriptor) {
                    Ok(catalog) => catalog,
                    Err(error) if error.is_catalog_corruption() => continue,
                    Err(error) => return Err(error),
                };
                let snapshot = GcCandidateCatalogSnapshot {
                    source: CatalogSource::Leased(Arc::new(catalog)),
                };
                self.cache_latest(&name, descriptor);
                return Ok(Some(snapshot));
            }

            let audit = match self.audit_named(&name) {
                Ok(audit) => audit,
                Err(error) if error.is_catalog_corruption() => continue,
                Err(error) => return Err(error),
            };
            require_same_descriptor(descriptor, audit)?;
            let snapshot = GcCandidateCatalogSnapshot {
                source: CatalogSource::Bounded {
                    storage: self.storage.clone(),
                    name: name.clone(),
                    descriptor: audit,
                },
            };
            self.cache_latest(&name, audit);
            return Ok(Some(snapshot));
        }
        Ok(None)
    }

    fn cache_latest(&self, name: &str, descriptor: GcCandidateCatalogDescriptor) {
        *self
            .cached_latest
            .lock()
            .expect("ASSERT: GC candidate catalog recovery cache lock poisoned") =
            Some(CachedGcCandidateCatalog {
                name: name.to_owned(),
                descriptor,
            });
    }

    fn invalidate_cached_latest(&self) {
        *self
            .cached_latest
            .lock()
            .expect("ASSERT: GC candidate catalog recovery cache lock poisoned") = None;
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

    /// Retires every superseded immutable catalog generation.
    ///
    /// Only the greatest canonical generation — valid or deliberately
    /// corrupt — carries forward the whole pool view, so every strictly
    /// older published object and every abandoned `.building` temporary
    /// below that high-water is inert. The greatest canonical name is
    /// never unlinked: it alone preserves the allocator high-water that
    /// forbids immutable-name reuse after an unlink. Names whose
    /// removal a live lease or a concurrent sweep refuses, and names
    /// already absent, are retained for a later quantum; a crash
    /// between successor publication and this sweep therefore leaves
    /// only inert files that the next sweep removes.
    ///
    /// # Errors
    ///
    /// Returns directory enumeration or unlink failures other than
    /// benign absence and lease-held removal denial.
    pub fn retire_superseded(&self) -> Result<u64, GcCandidateCatalogStoreError> {
        let mut greatest = None;
        let mut candidates: Vec<(String, u64)> = Vec::new();
        for name in self.storage.list_names()? {
            let generation =
                parse_published_generation(&name).or_else(|| parse_temporary_generation(&name));
            if let Some(generation) = generation {
                greatest = Some(greatest.map_or(generation, |value: u64| value.max(generation)));
                candidates
                    .try_reserve_exact(candidates.len() + 1)
                    .map_err(|_| GcCandidateCatalogStoreError::OutOfMemory)?;
                candidates.push((name, generation));
            }
        }
        let Some(high_water) = greatest else {
            return Ok(0);
        };
        let mut removed = 0_u64;
        for (name, generation) in candidates {
            if generation == high_water {
                continue;
            }
            match self.storage.remove_file(&name) {
                Ok(()) => {
                    removed = removed
                        .checked_add(1)
                        .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                    ) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if removed != 0 {
            self.storage.sync_root()?;
        }
        Ok(removed)
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
    Leased(Arc<ImmutableGcCandidateCatalog>),
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
            CatalogSource::Leased(catalog) => catalog.descriptor(),
            CatalogSource::Bounded { descriptor, .. } => *descriptor,
        }
    }

    #[must_use]
    pub const fn leased(&self) -> bool {
        matches!(self.source, CatalogSource::Leased(_))
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
            CatalogSource::Leased(catalog) => catalog.row(ordinal),
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
        let _scan = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
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

    pub(crate) fn visit_range(
        &self,
        start: u64,
        rows: u64,
        mut visit: impl FnMut(GcCandidateCatalogRow) -> Result<(), GcCandidateCatalogStoreError>,
    ) -> Result<u64, GcCandidateCatalogStoreError> {
        let snapshot_descriptor = self.descriptor();
        if start >= snapshot_descriptor.row_count() || rows == 0 {
            return Ok(0);
        }
        let rows = rows.min(snapshot_descriptor.row_count() - start);
        match &self.source {
            CatalogSource::Leased(catalog) => catalog.visit_range(start, rows, visit),
            CatalogSource::Bounded {
                storage,
                name,
                descriptor,
            } => {
                let _scan = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
                let mut ordinal = start;
                let mut scanned = 0_u64;
                while scanned < rows {
                    let batch = (rows - scanned).min(AUDIT_BATCH_ROWS);
                    let offset = descriptor
                        .row_offset(ordinal)
                        .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
                    let length = usize::try_from(
                        batch
                            .checked_mul(GC_CANDIDATE_CATALOG_ROW_BYTES as u64)
                            .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?,
                    )
                    .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
                    let bytes = storage.read_exact_at(name, offset, length)?;
                    for row_bytes in bytes.chunks_exact(GC_CANDIDATE_CATALOG_ROW_BYTES) {
                        visit(descriptor.decode_row(ordinal, row_bytes)?)?;
                        ordinal += 1;
                        scanned += 1;
                    }
                }
                Ok(scanned)
            }
        }
    }

    fn visit_rows(
        &self,
        visit: impl FnMut(GcCandidateCatalogRow) -> Result<(), GcCandidateCatalogStoreError>,
    ) -> Result<(), GcCandidateCatalogStoreError> {
        let _scan = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
        match &self.source {
            CatalogSource::Leased(catalog) => catalog.visit_rows(visit),
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

#[derive(Debug)]
pub(crate) struct GcCandidateQueueFill {
    pub(crate) scanned_rows: u64,
    pub(crate) scan_complete: bool,
}

#[derive(Debug, Default)]
pub(crate) struct GcCandidateSelectionQueue {
    base_generation: Option<u64>,
    incorporated_commit_generation: u64,
    pending_limit: usize,
    cursor: u64,
    scan_complete: bool,
    requires_rebuild: bool,
    ranked: BTreeMap<CandidateRank, GcCandidateCatalogRow>,
    pending_updates: BTreeMap<[u8; 16], GcCandidateCatalogRow>,
    removed: BTreeSet<[u8; 16]>,
}

impl GcCandidateSelectionQueue {
    pub(crate) fn sync_catalog_generation(
        &mut self,
        descriptor: GcCandidateCatalogDescriptor,
        pending_limit: usize,
    ) {
        if self.base_generation != Some(descriptor.generation())
            || self.pending_limit != pending_limit
        {
            *self = Self {
                base_generation: Some(descriptor.generation()),
                incorporated_commit_generation: descriptor.incorporated_commit_generation(),
                pending_limit,
                ..Self::default()
            };
        }
    }

    #[must_use]
    pub(crate) const fn incorporated_commit_generation(&self) -> u64 {
        self.incorporated_commit_generation
    }

    #[must_use]
    pub(crate) fn retained(&self) -> usize {
        self.ranked.len()
    }

    #[must_use]
    pub(crate) fn pending_updates(&self) -> usize {
        self.pending_updates.len()
    }

    #[must_use]
    pub(crate) const fn requires_rebuild(&self) -> bool {
        self.requires_rebuild
    }

    pub(crate) const fn request_rebuild(&mut self) {
        self.requires_rebuild = true;
    }

    #[must_use]
    pub(crate) fn needs_flush(&self) -> bool {
        self.pending_updates.len() >= self.pending_limit || self.removed.len() >= self.pending_limit
    }

    pub(crate) fn apply_liveness_updates(
        &mut self,
        updates: Vec<GcCandidateCatalogRow>,
        incorporated_commit_generation: u64,
        capacity: usize,
    ) {
        for row in updates {
            self.apply_candidate_update(row, capacity);
        }
        self.incorporated_commit_generation = incorporated_commit_generation;
    }

    pub(crate) fn note_collection(
        &mut self,
        rows: impl IntoIterator<Item = GcCandidateCatalogRow>,
        capacity: usize,
    ) -> Result<(), GcCandidateCatalogStoreError> {
        for row in rows {
            if row.location_state() != GcCandidateLocationState::Active {
                continue;
            }
            let update =
                row.with_estimate(GcCandidateLocationState::Retiring, retired_estimate(row)?)?;
            self.apply_candidate_update(update, capacity);
        }
        Ok(())
    }

    pub(crate) fn invalidate(&mut self) {
        self.ranked.clear();
        self.cursor = 0;
        self.scan_complete = false;
    }

    pub(crate) fn take(&mut self, limit: usize) -> Vec<GcCandidateCatalogRow> {
        let mut selected = Vec::new();
        let mut seen = BTreeSet::new();
        while selected.len() < limit {
            let Some((_, row)) = self.ranked.pop_last() else {
                break;
            };
            let id = row.container_id().bytes();
            if self.removed.contains(&id) || !seen.insert(id) {
                continue;
            }
            let row = self.pending_updates.get(&id).copied().unwrap_or(row);
            if row.location_state() != GcCandidateLocationState::Active {
                continue;
            }
            selected.push(row);
        }
        selected
    }

    pub(crate) fn take_successor_updates(&mut self) -> Vec<GcCandidateCatalogRow> {
        self.removed.clear();
        std::mem::take(&mut self.pending_updates)
            .into_values()
            .collect()
    }

    pub(crate) fn fill<I: Clone + StorageIo>(
        &mut self,
        snapshot: &GcCandidateCatalogSnapshot<I>,
        mode: GcCandidateSelectionMode,
        capacity: usize,
        max_rows: u64,
    ) -> Result<GcCandidateQueueFill, GcCandidateCatalogStoreError> {
        if self.base_generation != Some(snapshot.descriptor().generation()) {
            return Err(GcCandidateCatalogStoreError::StaleSuccessor);
        }
        if self.scan_complete || self.ranked.len() >= capacity {
            return Ok(GcCandidateQueueFill {
                scanned_rows: 0,
                scan_complete: self.scan_complete,
            });
        }
        let descriptor = snapshot.descriptor();
        if self.cursor >= descriptor.row_count() {
            self.scan_complete = true;
            return Ok(GcCandidateQueueFill {
                scanned_rows: 0,
                scan_complete: true,
            });
        }
        let rows = max_rows.min(descriptor.row_count() - self.cursor);
        let mut heap = std::mem::take(&mut self.ranked);
        let pending = &self.pending_updates;
        let removed = &self.removed;
        let mut scanned = 0_u64;
        let result = snapshot.visit_range(self.cursor, rows, |row| {
            scanned = scanned.saturating_add(1);
            let id = row.container_id().bytes();
            if row.location_state() != GcCandidateLocationState::Active
                || removed.contains(&id)
                || pending.contains_key(&id)
            {
                return Ok(());
            }
            insert_bounded_rank(&mut heap, row, mode, capacity);
            Ok(())
        });
        self.ranked = heap;
        self.cursor = self.cursor.saturating_add(scanned);
        self.scan_complete = self.cursor >= descriptor.row_count();
        result?;
        Ok(GcCandidateQueueFill {
            scanned_rows: scanned,
            scan_complete: self.scan_complete,
        })
    }

    fn apply_candidate_update(&mut self, row: GcCandidateCatalogRow, capacity: usize) {
        let id = row.container_id().bytes();
        self.pending_updates.insert(id, row);
        if row.location_state() == GcCandidateLocationState::Active {
            insert_bounded_rank(
                &mut self.ranked,
                row,
                GcCandidateSelectionMode::Urgent,
                capacity,
            );
        } else {
            self.removed.insert(id);
            self.ranked
                .retain(|_, existing| existing.container_id().bytes() != id);
        }
    }
}

fn retired_estimate(
    row: GcCandidateCatalogRow,
) -> Result<fastdup_format::GcCandidateLivenessEstimate, GcCandidateCatalogError> {
    let record_area = row
        .physical_bytes()
        .checked_sub(2 * GC_CANDIDATE_CATALOG_HEADER_BYTES as u64)
        .ok_or(GcCandidateCatalogError::InvalidRow)?;
    let records = fastdup_format::GcRecordLivenessEstimate::new(
        row.dead_record_bytes(),
        row.wholly_live_record_bytes(),
        row.partial_record_bytes(),
        record_area,
    )?;
    let dependencies = row.dependency_estimate_known().then(|| {
        fastdup_format::GcDependencyEstimate::new(
            row.live_independent_bases(),
            row.incoming_base_fanout(),
        )
    });
    fastdup_format::GcCandidateLivenessEstimate::new(
        row.reachable_target_count(),
        row.estimated_encoded_coverage(),
        records,
        dependencies,
        record_area,
    )
}

fn insert_bounded_rank(
    ranked: &mut BTreeMap<CandidateRank, GcCandidateCatalogRow>,
    row: GcCandidateCatalogRow,
    mode: GcCandidateSelectionMode,
    capacity: usize,
) {
    if capacity == 0 {
        return;
    }
    let rank = candidate_rank(row, mode, u64::MAX);
    if ranked.len() < capacity {
        ranked.insert(rank, row);
        return;
    }
    if ranked
        .first_key_value()
        .is_some_and(|(worst, _)| rank > *worst)
    {
        ranked.pop_first();
        ranked.insert(rank, row);
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

    pub(crate) fn from_rows(
        descriptor: GcCandidateCatalogDescriptor,
        rows: impl IntoIterator<Item = GcCandidateCatalogRow>,
        mode: GcCandidateSelectionMode,
        current_container_generation: u64,
    ) -> Self {
        let mut seen = BTreeSet::new();
        let mut ranked = Vec::new();
        for row in rows {
            if row.location_state() != GcCandidateLocationState::Active
                || !seen.insert(row.container_id().bytes())
            {
                continue;
            }
            ranked.push((candidate_rank(row, mode, current_container_generation), row));
        }
        ranked.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.0));
        ranked.truncate(MAX_SHORTLIST_ROWS);
        Self {
            descriptor,
            rows: ranked.into_iter().map(|(_, row)| row).collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CandidateRank {
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

pub(crate) fn candidate_rank(
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
    let fill_hint = fill_compaction_hint(row, age);
    let reclaim_hint = reclaim_hint.max(fill_hint);
    let score = match mode {
        GcCandidateSelectionMode::Urgent => u128::from(reclaim_hint),
        GcCandidateSelectionMode::Background => {
            let cost = row
                .physical_bytes()
                .saturating_add(row.raw_replacement_upper_bound())
                .max(1);
            u128::from(reclaim_hint)
                .saturating_mul(u128::from(age.max(1)))
                .saturating_mul(u128::from(GC_FILL_COMPACTION_PHYSICAL_SCORE_BASIS_BYTES))
                / u128::from(cost)
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

fn fill_compaction_hint(row: GcCandidateCatalogRow, age: u64) -> u64 {
    if age == 0
        || row.location_state() != GcCandidateLocationState::Active
        || !row.estimate_known()
        || row.physical_bytes() >= GC_FILL_COMPACTION_PHYSICAL_MAX_BYTES
    {
        return 0;
    }
    GC_FILL_COMPACTION_PHYSICAL_MAX_BYTES.saturating_sub(row.physical_bytes())
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

/// Decodes and pairs the Header/Footer envelope of one named generation with
/// its physical length, without hashing rows.
///
/// Row audit belongs to the single reader that holds the immutable lease, so
/// one recovery performs exactly one complete content audit per generation.
fn descriptor_named<I: StorageIo>(
    storage: &I,
    name: &str,
) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError> {
    let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
    let file_length = storage.object_len(name)?;
    if file_length < (2 * GC_CANDIDATE_CATALOG_HEADER_BYTES) as u64 {
        return Err(GcCandidateCatalogStoreError::IndexCorruption);
    }
    let header = storage.read_exact_at(name, 0, GC_CANDIDATE_CATALOG_HEADER_BYTES)?;
    let footer_offset = file_length
        .checked_sub(GC_CANDIDATE_CATALOG_HEADER_BYTES as u64)
        .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
    let footer = storage.read_exact_at(name, footer_offset, GC_CANDIDATE_CATALOG_HEADER_BYTES)?;
    Ok(GcCandidateCatalogDescriptor::decode(
        &header,
        &footer,
        file_length,
    )?)
}

fn audit_named_with<I: StorageIo>(
    storage: &I,
    name: &str,
    mut visit: impl FnMut(GcCandidateCatalogRow) -> Result<(), GcCandidateCatalogStoreError>,
) -> Result<GcCandidateCatalogDescriptor, GcCandidateCatalogStoreError> {
    let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
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

fn parse_temporary_generation(name: &str) -> Option<u64> {
    let digits = name
        .strip_prefix('.')?
        .strip_prefix(PUBLISHED_PREFIX)?
        .strip_suffix(&format!("{PUBLISHED_SUFFIX}.building"))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use fastdup_format::{
        ContainerIntrinsicSummary, GcCandidateLivenessEstimate, GcDependencyEstimate,
        GcRecordLivenessEstimate, SealedContainer,
    };

    fn fixture_summary() -> ContainerIntrinsicSummary {
        let id = ContainerId::new([0xA5; 16]).expect("fixture identity is nonzero");
        let (_image, publication) =
            SealedContainer::encode_with_writer_evidence(id, 1, &[b"fill compaction fixture"])
                .expect("fixture Container encodes")
                .into_publication_parts();
        publication
            .intrinsic_summary()
            .expect("publication evidence reconstructs intrinsic summary")
    }

    fn estimated_row(id: u8, generation: u64, physical_bytes: u64) -> GcCandidateCatalogRow {
        let mut bytes = [0_u8; 16];
        bytes[15] = id;
        let container_id = ContainerId::new(bytes).expect("test Container ID");
        let row = GcCandidateCatalogRow::from_intrinsic_summary(
            container_id,
            generation,
            physical_bytes,
            fixture_summary(),
        )
        .expect("valid test row");
        let records = GcRecordLivenessEstimate::new(0, 0, 0, physical_bytes - 8_192)
            .expect("valid record estimate");
        row.with_estimate(
            GcCandidateLocationState::Active,
            GcCandidateLivenessEstimate::new(
                0,
                0,
                records,
                Some(GcDependencyEstimate::new(0, 1)),
                physical_bytes - 8_192,
            )
            .expect("valid liveness estimate"),
        )
        .expect("valid estimated row")
    }

    #[test]
    fn fill_compaction_promotes_underfilled_active_candidates() {
        let current_generation = 100;
        let underfilled = estimated_row(1, 90, 16 * 1024);
        let filled = estimated_row(2, 90, GC_FILL_COMPACTION_PHYSICAL_MAX_BYTES);
        assert!(
            candidate_rank(
                underfilled,
                GcCandidateSelectionMode::Background,
                current_generation
            ) > candidate_rank(
                filled,
                GcCandidateSelectionMode::Background,
                current_generation
            )
        );
    }
}
