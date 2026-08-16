use std::cmp::Reverse;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use fastdup_format::{
    ChunkId, EXACT_INDEX_ACTIVATION_RECORD_BYTES, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES,
    ExactIndexActivationError, ExactIndexActivationHash, ExactIndexActivationRecord,
    ExactIndexEntry, ExactIndexFormatError, ExactIndexPage, ExactIndexPagePosition,
    ExactIndexProfileId, ExactIndexRun, ExactIndexRunDescriptor, ExactIndexRunRef,
    ExactIndexRunSet, ExactIndexRunSetError, ExactIndexRunSetId, MAX_METADATA_OBJECT_BYTES,
};

use crate::StorageIo;

pub const MAX_EXACT_LOOKUP_CANDIDATES: usize = 64;
pub const MAX_ACTIVE_EXACT_INDEX_RUNS: usize = 64;
/// Hard bound for one in-memory compaction input and output.
///
/// This is policy rather than format geometry. It keeps the current pre-MVP
/// merge below roughly 100 MiB of transient entry/output storage while a later
/// streaming partitioned compactor is benchmarked.
pub const MAX_EXACT_COMPACTION_ENTRIES: usize = 262_144;
const ACTIVATION_WAL_NAME: &str = "exact-index.activation.wal";
const MAX_ACTIVATION_WAL_BYTES: u64 = 64 * 1_024 * 1_024;

/// Durable immutable Exact Index run publication and bounded lookup module.
#[derive(Clone, Debug)]
pub struct ExactIndexRunRepository<I> {
    storage: I,
    publish_lock: Arc<Mutex<()>>,
}

impl<I: Clone + StorageIo> ExactIndexRunRepository<I> {
    #[must_use]
    pub fn new(storage: I) -> Self {
        Self {
            storage,
            publish_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Durably publishes one immutable run without activating it.
    ///
    /// Idempotent retry succeeds only when an existing canonical name has the
    /// same profile, generation, and complete run hash. A different run under
    /// the same identity is an integrity failure.
    ///
    /// # Errors
    ///
    /// Returns format, I/O, writer-reread, collision, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics if the repository's writer lock is poisoned, or if a validated
    /// format-v1 object violates its own fixed page geometry. Both are
    /// production-fatal internal `ASSERT` failures.
    pub fn publish(
        &self,
        run: &ExactIndexRun,
    ) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index run publication lock poisoned");
        let encoded = run.encode()?;
        let expected = descriptor_from_complete_bytes(&encoded)?;
        let temporary_name = temporary_name(run.profile(), run.generation());
        let published_name = published_name(run.profile(), run.generation());

        if self.storage.exists(&published_name)? {
            let observed = self.audit_named(&published_name)?;
            verify_expected_descriptor(expected, observed)?;
            self.storage.sync_root()?;
            return Ok(observed);
        }

        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        for (page_ordinal, page) in encoded.chunks(EXACT_INDEX_PAGE_BYTES).enumerate() {
            assert_eq!(
                page.len(),
                EXACT_INDEX_PAGE_BYTES,
                "ASSERT: Exact Index Run v1 always consists of complete 4-KiB pages"
            );
            let offset = page_ordinal
                .checked_mul(EXACT_INDEX_PAGE_BYTES)
                .and_then(|value| u64::try_from(value).ok())
                .expect("ASSERT: a bounded Exact Index run offset fits u64");
            self.storage.write_at(&temporary_name, offset, page)?;
        }
        self.storage.set_len(
            &temporary_name,
            u64::try_from(encoded.len())
                .expect("ASSERT: a bounded Exact Index run length fits u64"),
        )?;
        let observed = self.audit_named(&temporary_name)?;
        verify_expected_descriptor(expected, observed)?;
        self.storage.sync_file(&temporary_name)?;
        match self
            .storage
            .publish_noreplace(&temporary_name, &published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let raced = self.audit_named(&published_name)?;
                verify_expected_descriptor(expected, raced)?;
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok(observed)
    }

    /// Opens one published run using only its exact length, Header, and Footer.
    ///
    /// The returned reader performs bounded 4-KiB page reads. It does not make
    /// negative lookup results authoritative.
    ///
    /// # Errors
    ///
    /// Returns I/O, envelope-integrity, or requested-identity errors.
    pub fn open(
        &self,
        profile: ExactIndexProfileId,
        generation: u64,
    ) -> Result<ExactIndexRunReader<I>, ExactIndexStoreError> {
        let name = published_name(profile, generation);
        let descriptor = self.open_named(&name)?;
        verify_requested_identity(profile, generation, descriptor)?;
        Ok(ExactIndexRunReader {
            storage: self.storage.clone(),
            name,
            descriptor,
        })
    }

    /// Sequentially verifies every page, cross-page ordering, and the complete
    /// run hash without materializing the run or its full key map.
    ///
    /// # Errors
    ///
    /// Returns I/O, format-integrity, requested-identity, or AUDIT failures.
    pub fn audit(
        &self,
        profile: ExactIndexProfileId,
        generation: u64,
    ) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        let descriptor = self.audit_named(&published_name(profile, generation))?;
        verify_requested_identity(profile, generation, descriptor)?;
        Ok(descriptor)
    }

    /// Merges a bounded set of fully audited immutable Runs into one new Run.
    ///
    /// For a repeated physical Location the transition from the newest source
    /// Run generation wins. Every other Location is retained, including
    /// tombstones needed to shadow still-active older Runs. The output is
    /// canonical and independent of source discovery order.
    ///
    /// This publishes the resulting Run but does not activate it. The caller
    /// must activate one complete replacement Run Set only after every retained
    /// dependency is durable.
    ///
    /// # Errors
    ///
    /// Rejects fewer than two inputs, duplicate/mismatched source identities,
    /// a nonmonotonic target generation, an input above the explicit memory
    /// bound, source corruption, Chunk-ID length conflicts, or publication I/O.
    ///
    /// # Panics
    ///
    /// Panics only if a completely audited Run disagrees with the entry count
    /// pinned by its verified reference, or if the bounded count cannot fit
    /// `usize`. Both are impossible production `ASSERT` failures.
    pub fn compact(
        &self,
        inputs: &[ExactIndexRunRef],
        target_generation: u64,
    ) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        if inputs.len() < 2 || inputs.len() > MAX_ACTIVE_EXACT_INDEX_RUNS {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }
        let profile = inputs[0].profile();
        let mut ordered_inputs = Vec::new();
        ordered_inputs
            .try_reserve_exact(inputs.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        ordered_inputs.extend_from_slice(inputs);
        ordered_inputs.sort_unstable_by_key(|run| run.generation());
        if ordered_inputs.iter().any(|run| run.profile() != profile)
            || ordered_inputs
                .windows(2)
                .any(|pair| pair[0].generation() == pair[1].generation())
            || ordered_inputs
                .last()
                .is_none_or(|run| target_generation <= run.generation())
        {
            return Err(ExactIndexStoreError::InvalidCompactionInput);
        }

        let total_entries = ordered_inputs.iter().try_fold(0_usize, |total, run| {
            let count = usize::try_from(run.entry_count())
                .map_err(|_| ExactIndexStoreError::CompactionTooLarge)?;
            total
                .checked_add(count)
                .ok_or(ExactIndexStoreError::CompactionTooLarge)
        })?;
        if total_entries > MAX_EXACT_COMPACTION_ENTRIES {
            return Err(ExactIndexStoreError::CompactionTooLarge);
        }

        let mut merged = Vec::new();
        merged
            .try_reserve_exact(total_entries)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        for run_ref in ordered_inputs {
            let entries = self.read_verified_entries(run_ref)?;
            assert_eq!(
                entries.len(),
                usize::try_from(run_ref.entry_count())
                    .expect("ASSERT: bounded compaction entry count fits usize"),
                "ASSERT: audited Run entry count disagrees with its pinned reference"
            );
            merged.extend(entries.into_iter().map(|entry| CompactionEntry {
                entry,
                source_generation: run_ref.generation(),
            }));
        }
        assert_eq!(
            merged.len(),
            total_entries,
            "ASSERT: bounded compaction lost a source entry"
        );
        merged.sort_unstable_by_key(|item| {
            (
                compaction_location_key(item.entry),
                Reverse(item.source_generation),
            )
        });

        let mut output = Vec::new();
        output
            .try_reserve_exact(merged.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let mut previous_key = None;
        for item in merged {
            let key = compaction_location_key(item.entry);
            if previous_key == Some(key) {
                continue;
            }
            previous_key = Some(key);
            output.push(item.entry);
        }
        let compacted = ExactIndexRun::new(profile, target_generation, output)?;
        self.publish(&compacted)
    }

    /// Publishes and activates one Run Set after fully auditing every named
    /// immutable Run. The final activation-WAL sync is the only commit point.
    ///
    /// # Errors
    ///
    /// Returns dependency, content-address, chain, I/O, reread, or durability
    /// errors. A failed activation never changes Namespace durability.
    ///
    /// # Panics
    ///
    /// Panics if the shared publication lock is poisoned or fixed format-v1
    /// sizes violate their compile-time geometry.
    pub fn activate(
        &self,
        run_set: &ExactIndexRunSet,
    ) -> Result<ActivatedExactIndex<I>, ExactIndexStoreError> {
        let _guard = self
            .publish_lock
            .lock()
            .expect("ASSERT: Exact Index activation lock poisoned");
        let readers = self.verify_run_set_dependencies(run_set)?;
        let encoded = run_set.encode()?;
        let run_set_id = ExactIndexRunSetId::from_encoded(&encoded)?;
        self.publish_run_set(run_set_id, &encoded)?;
        self.ensure_activation_wal()?;
        let tail = self.read_activation_tail()?;
        if !tail.clean {
            return Err(ExactIndexStoreError::ActivationWalCorrupt);
        }
        if let Some(last) = tail.last {
            if last.run_set_id() == run_set_id {
                if last.profile() != run_set.profile()
                    || last.run_set_generation() != run_set.generation()
                {
                    return Err(ExactIndexStoreError::DependencyMismatch);
                }
                self.storage.sync_file(ACTIVATION_WAL_NAME)?;
                return ActivatedExactIndex::new(last, run_set.clone(), readers);
            }
            if run_set.generation() <= last.run_set_generation() {
                return Err(ExactIndexStoreError::NonMonotonicRunSetGeneration);
            }
        }
        let generation = tail.last.map_or(Ok(1), |record| {
            record
                .generation()
                .checked_add(1)
                .ok_or(ExactIndexStoreError::ActivationWalCorrupt)
        })?;
        let previous_hash = tail.last.map_or(ExactIndexActivationHash::ZERO, |record| {
            ExactIndexActivationHash::of(&record.encode())
        });
        let next_length = tail
            .valid_length
            .checked_add(
                u64::try_from(EXACT_INDEX_ACTIVATION_RECORD_BYTES)
                    .expect("ASSERT: activation record length fits u64"),
            )
            .ok_or(ExactIndexStoreError::ActivationWalFull)?;
        if next_length > MAX_ACTIVATION_WAL_BYTES {
            return Err(ExactIndexStoreError::ActivationWalFull);
        }
        let record = ExactIndexActivationRecord::new(
            generation,
            previous_hash,
            run_set_id,
            run_set.profile(),
            run_set.generation(),
        )?;
        let encoded_record = record.encode();
        self.storage
            .write_at(ACTIVATION_WAL_NAME, tail.valid_length, &encoded_record)?;
        let reread = self.storage.read_exact_at(
            ACTIVATION_WAL_NAME,
            tail.valid_length,
            EXACT_INDEX_ACTIVATION_RECORD_BYTES,
        )?;
        if reread != encoded_record || ExactIndexActivationRecord::decode(&reread)? != record {
            return Err(ExactIndexStoreError::PublishVerificationMismatch);
        }
        self.storage.sync_file(ACTIVATION_WAL_NAME)?;
        ActivatedExactIndex::new(record, run_set.clone(), readers)
    }

    /// Recovers the newest contiguous activation record and verifies its exact
    /// Run Set plus every pinned immutable Run dependency.
    ///
    /// A torn final record is ignored. A complete invalid chain or invalid
    /// dependency disables this index generation with an error; it never rolls
    /// Namespace metadata back.
    ///
    /// # Errors
    ///
    /// Returns activation-chain, Run Set, Run, identity, I/O, or integrity
    /// failures.
    pub fn recover_active(&self) -> Result<Option<ActivatedExactIndex<I>>, ExactIndexStoreError> {
        if !self.storage.exists(ACTIVATION_WAL_NAME)? {
            return Ok(None);
        }
        let tail = self.read_activation_tail()?;
        let Some(record) = tail.last else {
            return Ok(None);
        };
        let run_set = self.read_run_set(record.run_set_id())?;
        if run_set.profile() != record.profile()
            || run_set.generation() != record.run_set_generation()
            || run_set.id()? != record.run_set_id()
        {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let readers = self.verify_run_set_dependencies(&run_set)?;
        Ok(Some(ActivatedExactIndex::new(record, run_set, readers)?))
    }

    fn open_named(&self, name: &str) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        Ok(self.read_envelope(name)?.descriptor)
    }

    fn read_envelope(&self, name: &str) -> Result<OpenedRunEnvelope, ExactIndexStoreError> {
        let file_length = self.storage.object_len(name)?;
        if file_length < 2 * u64::try_from(EXACT_INDEX_PAGE_BYTES).expect("ASSERT: 4 KiB fits u64")
        {
            return Err(ExactIndexFormatError::InvalidObjectLength(
                usize::try_from(file_length).unwrap_or(usize::MAX),
            )
            .into());
        }
        let footer_offset = file_length
            .checked_sub(u64::try_from(EXACT_INDEX_PAGE_BYTES).expect("ASSERT: 4 KiB fits u64"))
            .expect("ASSERT: minimum run length was checked");
        let header = self
            .storage
            .read_exact_at(name, 0, EXACT_INDEX_HEADER_BYTES)?;
        let footer = self
            .storage
            .read_exact_at(name, footer_offset, EXACT_INDEX_PAGE_BYTES)?;
        let descriptor = ExactIndexRunDescriptor::decode(&header, &footer, file_length)?;
        Ok(OpenedRunEnvelope {
            descriptor,
            header,
            footer,
            footer_offset,
        })
    }

    fn audit_named(&self, name: &str) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
        let envelope = self.read_envelope(name)?;
        let descriptor = envelope.descriptor;
        let mut audit = descriptor.begin_hash_audit();
        audit.update(0, &envelope.header)?;
        for page_ordinal in 0..descriptor.page_count() {
            let offset = descriptor
                .page_offset(page_ordinal)
                .expect("ASSERT: descriptor page ordinal was prevalidated");
            let bytes = self
                .storage
                .read_exact_at(name, offset, EXACT_INDEX_PAGE_BYTES)?;
            let page = descriptor.decode_page(page_ordinal, &bytes)?;
            audit.verify_page(&page)?;
            audit.update(offset, &bytes)?;
        }
        audit.update(envelope.footer_offset, &envelope.footer)?;
        audit.finish()?;
        Ok(descriptor)
    }

    fn read_verified_entries(
        &self,
        run_ref: ExactIndexRunRef,
    ) -> Result<Vec<ExactIndexEntry>, ExactIndexStoreError> {
        let name = published_name(run_ref.profile(), run_ref.generation());
        let envelope = self.read_envelope(&name)?;
        let descriptor = envelope.descriptor;
        verify_requested_identity(run_ref.profile(), run_ref.generation(), descriptor)?;
        verify_run_reference(run_ref, descriptor)?;
        if descriptor.entry_count() > MAX_EXACT_COMPACTION_ENTRIES {
            return Err(ExactIndexStoreError::CompactionTooLarge);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(descriptor.entry_count())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let mut audit = descriptor.begin_hash_audit();
        audit.update(0, &envelope.header)?;
        for page_ordinal in 0..descriptor.page_count() {
            let offset = descriptor
                .page_offset(page_ordinal)
                .expect("ASSERT: descriptor page ordinal was prevalidated");
            let bytes = self
                .storage
                .read_exact_at(&name, offset, EXACT_INDEX_PAGE_BYTES)?;
            let page = descriptor.decode_page(page_ordinal, &bytes)?;
            audit.verify_page(&page)?;
            entries.extend_from_slice(page.entries());
            audit.update(offset, &bytes)?;
        }
        audit.update(envelope.footer_offset, &envelope.footer)?;
        audit.finish()?;
        if entries.len() != descriptor.entry_count() {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        Ok(entries)
    }

    fn verify_run_set_dependencies(
        &self,
        run_set: &ExactIndexRunSet,
    ) -> Result<Vec<ExactIndexRunReader<I>>, ExactIndexStoreError> {
        if run_set.runs().len() > MAX_ACTIVE_EXACT_INDEX_RUNS {
            return Err(ExactIndexStoreError::TooManyActiveRuns);
        }
        let mut readers = Vec::new();
        readers
            .try_reserve_exact(run_set.runs().len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        for run_ref in run_set.runs().iter().copied() {
            let name = published_name(run_ref.profile(), run_ref.generation());
            let descriptor = self.audit_named(&name)?;
            verify_requested_identity(run_ref.profile(), run_ref.generation(), descriptor)?;
            verify_run_reference(run_ref, descriptor)?;
            readers.push(ExactIndexRunReader {
                storage: self.storage.clone(),
                name,
                descriptor,
            });
        }
        Ok(readers)
    }

    fn publish_run_set(
        &self,
        run_set_id: ExactIndexRunSetId,
        encoded: &[u8],
    ) -> Result<(), ExactIndexStoreError> {
        let published_name = run_set_name(run_set_id);
        if self.storage.exists(&published_name)? {
            let observed = self.read_run_set(run_set_id)?;
            if observed.id()? != run_set_id {
                return Err(ExactIndexStoreError::PublishVerificationMismatch);
            }
            self.storage.sync_root()?;
            return Ok(());
        }
        let temporary_name = format!(".{published_name}.building");
        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        for (ordinal, page) in encoded.chunks(EXACT_INDEX_PAGE_BYTES).enumerate() {
            let offset = ordinal
                .checked_mul(EXACT_INDEX_PAGE_BYTES)
                .and_then(|value| u64::try_from(value).ok())
                .expect("ASSERT: a Metadata-v1 object offset fits u64");
            self.storage.write_at(&temporary_name, offset, page)?;
        }
        self.storage.set_len(
            &temporary_name,
            u64::try_from(encoded.len()).expect("ASSERT: Metadata-v1 length fits u64"),
        )?;
        let reread = self.storage.read(&temporary_name)?;
        if reread != encoded || ExactIndexRunSetId::from_encoded(&reread)? != run_set_id {
            return Err(ExactIndexStoreError::PublishVerificationMismatch);
        }
        self.storage.sync_file(&temporary_name)?;
        match self
            .storage
            .publish_noreplace(&temporary_name, &published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let observed = self.read_run_set(run_set_id)?;
                if observed.id()? != run_set_id {
                    return Err(ExactIndexStoreError::PublishVerificationMismatch);
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok(())
    }

    fn read_run_set(
        &self,
        run_set_id: ExactIndexRunSetId,
    ) -> Result<ExactIndexRunSet, ExactIndexStoreError> {
        let name = run_set_name(run_set_id);
        let length = self.storage.object_len(&name)?;
        if length > u64::try_from(MAX_METADATA_OBJECT_BYTES).expect("ASSERT: 16 MiB fits u64") {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let encoded = self.storage.read(&name)?;
        let run_set = ExactIndexRunSet::decode(&encoded)?;
        if ExactIndexRunSetId::from_encoded(&encoded)? != run_set_id {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        Ok(run_set)
    }

    fn ensure_activation_wal(&self) -> Result<(), ExactIndexStoreError> {
        if self.storage.exists(ACTIVATION_WAL_NAME)? {
            self.storage.sync_root()?;
            return Ok(());
        }
        match self.storage.create_new(ACTIVATION_WAL_NAME) {
            Ok(()) => self.storage.sync_file(ACTIVATION_WAL_NAME)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        Ok(())
    }

    fn read_activation_tail(&self) -> Result<ActivationTail, ExactIndexStoreError> {
        let physical_length = self.storage.object_len(ACTIVATION_WAL_NAME)?;
        if physical_length > MAX_ACTIVATION_WAL_BYTES {
            return Err(ExactIndexStoreError::ActivationWalCorrupt);
        }
        let record_bytes = u64::try_from(EXACT_INDEX_ACTIVATION_RECORD_BYTES)
            .expect("ASSERT: activation record length fits u64");
        let record_count = physical_length / record_bytes;
        let valid_length = record_count
            .checked_mul(record_bytes)
            .ok_or(ExactIndexStoreError::ActivationWalCorrupt)?;
        let mut last: Option<ExactIndexActivationRecord> = None;
        for ordinal in 0..record_count {
            let offset = ordinal
                .checked_mul(record_bytes)
                .ok_or(ExactIndexStoreError::ActivationWalCorrupt)?;
            let encoded = self.storage.read_exact_at(
                ACTIVATION_WAL_NAME,
                offset,
                EXACT_INDEX_ACTIVATION_RECORD_BYTES,
            )?;
            let record = ExactIndexActivationRecord::decode(&encoded)?;
            let expected_generation = ordinal
                .checked_add(1)
                .ok_or(ExactIndexStoreError::ActivationWalCorrupt)?;
            let expected_previous = last.map_or(ExactIndexActivationHash::ZERO, |previous| {
                ExactIndexActivationHash::of(&previous.encode())
            });
            if record.generation() != expected_generation
                || record.previous_record_hash() != expected_previous
                || last.is_some_and(|previous| {
                    record.run_set_generation() <= previous.run_set_generation()
                })
            {
                return Err(ExactIndexStoreError::ActivationWalCorrupt);
            }
            last = Some(record);
        }
        Ok(ActivationTail {
            last,
            valid_length,
            clean: physical_length == valid_length,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ActivatedExactIndex<I> {
    record: ExactIndexActivationRecord,
    run_set: ExactIndexRunSet,
    readers: Vec<ExactIndexRunReader<I>>,
    lookup_order: Vec<usize>,
}

impl<I> ActivatedExactIndex<I> {
    fn new(
        record: ExactIndexActivationRecord,
        run_set: ExactIndexRunSet,
        readers: Vec<ExactIndexRunReader<I>>,
    ) -> Result<Self, ExactIndexStoreError> {
        if readers.len() != run_set.runs().len() || readers.len() > MAX_ACTIVE_EXACT_INDEX_RUNS {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let mut lookup_order = Vec::new();
        lookup_order
            .try_reserve_exact(readers.len())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        lookup_order.extend(0..readers.len());
        lookup_order.sort_unstable_by_key(|index| Reverse(run_set.runs()[*index].generation()));
        Ok(Self {
            record,
            run_set,
            readers,
            lookup_order,
        })
    }

    #[must_use]
    pub const fn record(&self) -> ExactIndexActivationRecord {
        self.record
    }

    #[must_use]
    pub const fn run_set(&self) -> &ExactIndexRunSet {
        &self.run_set
    }

    #[must_use]
    pub fn run_count(&self) -> usize {
        self.readers.len()
    }
}

impl<I: StorageIo> ActivatedExactIndex<I> {
    /// Returns a newest-Run-first bounded transition prefix across the active
    /// Run Set. Callers must merge transitions by complete physical Location
    /// identity and verify any selected ACTIVE candidate against its Container.
    ///
    /// `complete=true` covers this Run Set only. It never makes a negative
    /// result authoritative for durable content.
    ///
    /// # Errors
    ///
    /// Returns touched-page I/O, integrity, or bounded-allocation failures.
    pub fn lookup_transitions(
        &self,
        chunk_id: ChunkId,
        logical_length: u32,
    ) -> Result<ExactIndexLookup, ExactIndexStoreError> {
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(MAX_EXACT_LOOKUP_CANDIDATES)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let mut complete = true;
        for &index in &self.lookup_order {
            let run_ref = self.run_set.runs()[index];
            if chunk_id < run_ref.minimum_chunk_id() || chunk_id > run_ref.maximum_chunk_id() {
                continue;
            }
            let lookup = self.readers[index].lookup(chunk_id, logical_length)?;
            complete &= lookup.complete();
            let remaining = MAX_EXACT_LOOKUP_CANDIDATES - candidates.len();
            if lookup.candidates().len() > remaining {
                candidates.extend_from_slice(&lookup.candidates()[..remaining]);
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: false,
                });
            }
            candidates.extend_from_slice(lookup.candidates());
            if candidates.len() == MAX_EXACT_LOOKUP_CANDIDATES {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: false,
                });
            }
        }
        Ok(ExactIndexLookup {
            candidates,
            complete,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct ActivationTail {
    last: Option<ExactIndexActivationRecord>,
    valid_length: u64,
    clean: bool,
}

#[derive(Clone, Debug)]
struct OpenedRunEnvelope {
    descriptor: ExactIndexRunDescriptor,
    header: Vec<u8>,
    footer: Vec<u8>,
    footer_offset: u64,
}

/// Open immutable run handle retaining only its verified envelope.
#[derive(Clone, Debug)]
pub struct ExactIndexRunReader<I> {
    storage: I,
    name: String,
    descriptor: ExactIndexRunDescriptor,
}

impl<I: StorageIo> ExactIndexRunReader<I> {
    /// Returns a bounded prefix of Location candidates for one exact key.
    ///
    /// `complete=false` means the key has more physical transitions than the
    /// hard candidate bound. Even `complete=true` is complete only for this
    /// immutable run; an Exact Index negative is never content authority.
    ///
    /// # Errors
    ///
    /// Returns exact-range I/O or touched-page integrity failures.
    pub fn lookup(
        &self,
        chunk_id: ChunkId,
        logical_length: u32,
    ) -> Result<ExactIndexLookup, ExactIndexStoreError> {
        let mut lower = 0;
        let mut upper = self.descriptor.page_count();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let page = self.read_page(middle)?;
            match page.position(chunk_id, logical_length) {
                ExactIndexPagePosition::After => lower = middle + 1,
                ExactIndexPagePosition::Before | ExactIndexPagePosition::Within => upper = middle,
            }
        }

        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(MAX_EXACT_LOOKUP_CANDIDATES)
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        let mut page_ordinal = lower;
        while page_ordinal < self.descriptor.page_count() {
            let page = self.read_page(page_ordinal)?;
            let matches = page.candidates(chunk_id, logical_length);
            if matches.is_empty() {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: true,
                });
            }
            let remaining = MAX_EXACT_LOOKUP_CANDIDATES - candidates.len();
            candidates.extend_from_slice(&matches[..matches.len().min(remaining)]);
            let key_reaches_page_end = page.entries().last().is_some_and(|entry| {
                entry.chunk_id() == chunk_id && entry.logical_length() == logical_length
            });
            if matches.len() > remaining {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: false,
                });
            }
            if !key_reaches_page_end || page_ordinal + 1 == self.descriptor.page_count() {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: true,
                });
            }
            if candidates.len() == MAX_EXACT_LOOKUP_CANDIDATES {
                return Ok(ExactIndexLookup {
                    candidates,
                    complete: false,
                });
            }
            page_ordinal += 1;
        }
        Ok(ExactIndexLookup {
            candidates,
            complete: true,
        })
    }

    fn read_page(&self, page_ordinal: usize) -> Result<ExactIndexPage, ExactIndexStoreError> {
        let offset = self
            .descriptor
            .page_offset(page_ordinal)
            .ok_or(ExactIndexFormatError::InvalidPage)?;
        let bytes = self
            .storage
            .read_exact_at(&self.name, offset, EXACT_INDEX_PAGE_BYTES)?;
        Ok(self.descriptor.decode_page(page_ordinal, &bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactIndexLookup {
    candidates: Vec<ExactIndexEntry>,
    complete: bool,
}

impl ExactIndexLookup {
    #[must_use]
    pub fn candidates(&self) -> &[ExactIndexEntry] {
        &self.candidates
    }

    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

#[derive(Debug)]
pub enum ExactIndexStoreError {
    Io(io::Error),
    Format(ExactIndexFormatError),
    IdentityMismatch,
    PublishVerificationMismatch,
    OutOfMemory,
    Activation(ExactIndexActivationError),
    RunSet(ExactIndexRunSetError),
    ActivationWalCorrupt,
    ActivationWalFull,
    DependencyMismatch,
    NonMonotonicRunSetGeneration,
    TooManyActiveRuns,
    InvalidCompactionInput,
    CompactionTooLarge,
}

impl fmt::Display for ExactIndexStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ExactIndexStoreError {}

impl From<io::Error> for ExactIndexStoreError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ExactIndexFormatError> for ExactIndexStoreError {
    fn from(error: ExactIndexFormatError) -> Self {
        Self::Format(error)
    }
}

impl From<ExactIndexActivationError> for ExactIndexStoreError {
    fn from(error: ExactIndexActivationError) -> Self {
        Self::Activation(error)
    }
}

impl From<ExactIndexRunSetError> for ExactIndexStoreError {
    fn from(error: ExactIndexRunSetError) -> Self {
        Self::RunSet(error)
    }
}

fn descriptor_from_complete_bytes(
    bytes: &[u8],
) -> Result<ExactIndexRunDescriptor, ExactIndexStoreError> {
    let footer_offset = bytes
        .len()
        .checked_sub(EXACT_INDEX_PAGE_BYTES)
        .ok_or(ExactIndexFormatError::InvalidObjectLength(bytes.len()))?;
    Ok(ExactIndexRunDescriptor::decode(
        &bytes[..EXACT_INDEX_HEADER_BYTES],
        &bytes[footer_offset..],
        u64::try_from(bytes.len()).map_err(|_| ExactIndexFormatError::ArithmeticOverflow)?,
    )?)
}

fn verify_expected_descriptor(
    expected: ExactIndexRunDescriptor,
    observed: ExactIndexRunDescriptor,
) -> Result<(), ExactIndexStoreError> {
    if expected.profile() != observed.profile()
        || expected.generation() != observed.generation()
        || expected.file_length() != observed.file_length()
        || expected.run_hash() != observed.run_hash()
    {
        return Err(ExactIndexStoreError::PublishVerificationMismatch);
    }
    Ok(())
}

fn verify_requested_identity(
    profile: ExactIndexProfileId,
    generation: u64,
    descriptor: ExactIndexRunDescriptor,
) -> Result<(), ExactIndexStoreError> {
    if descriptor.profile() != profile || descriptor.generation() != generation {
        return Err(ExactIndexStoreError::IdentityMismatch);
    }
    Ok(())
}

fn verify_run_reference(
    run_ref: ExactIndexRunRef,
    descriptor: ExactIndexRunDescriptor,
) -> Result<(), ExactIndexStoreError> {
    if run_ref.profile() != descriptor.profile()
        || run_ref.generation() != descriptor.generation()
        || run_ref.run_hash() != descriptor.run_hash()
        || run_ref.file_length()
            != u64::try_from(descriptor.file_length())
                .map_err(|_| ExactIndexStoreError::DependencyMismatch)?
        || run_ref.entry_count()
            != u64::try_from(descriptor.entry_count())
                .map_err(|_| ExactIndexStoreError::DependencyMismatch)?
        || run_ref.minimum_chunk_id() != descriptor.minimum_chunk_id()
        || run_ref.maximum_chunk_id() != descriptor.maximum_chunk_id()
    {
        return Err(ExactIndexStoreError::DependencyMismatch);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct CompactionEntry {
    entry: ExactIndexEntry,
    source_generation: u64,
}

fn compaction_location_key(entry: ExactIndexEntry) -> (ChunkId, u32, [u8; 16], u64, u32) {
    let location = entry.location();
    (
        entry.chunk_id(),
        entry.logical_length(),
        location.container_id().bytes(),
        location.record_offset(),
        location.chunk_ordinal(),
    )
}

fn temporary_name(profile: ExactIndexProfileId, generation: u64) -> String {
    format!(".{}.building", published_name(profile, generation))
}

fn published_name(profile: ExactIndexProfileId, generation: u64) -> String {
    format!("{}.{generation:016x}.fdx", encode_hex(profile.bytes()))
}

fn run_set_name(run_set_id: ExactIndexRunSetId) -> String {
    format!("{}.fdxset", encode_hex(run_set_id.bytes()))
}

fn encode_hex<const N: usize>(bytes: [u8; N]) -> String {
    let mut encoded = String::with_capacity(N * 2);
    for byte in bytes {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}")
            .expect("ASSERT: writing into an owned String cannot fail");
    }
    encoded
}
