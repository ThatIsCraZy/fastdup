use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use fastdup_format::{
    ChunkId, SIMILARITY_BUCKET_REFERENCES_PER_PAGE, SIMILARITY_INDEX_ENTRIES_PER_PAGE,
    SIMILARITY_INDEX_PAGE_BYTES, SimilarityBucketKey, SimilarityBucketReference,
    SimilarityIndexEntry, SimilarityIndexRunDescriptor, SimilarityIndexRunLayout,
    SimilarityIndexRunStreamEncoder,
};

use crate::StorageIo;
use crate::similarity_index_repository::{
    SIMILARITY_FINGERPRINT_PROFILE_V1, SIMILARITY_REPRESENTATIVE_PROFILE_V1,
    SimilarityIndexStoreError,
};

const SORT_CHUNK_BYTES: usize = 1024 * 1024;
const MERGE_FAN_IN: usize = 32;
const SPOOL_BUFFER_BYTES: usize = 64 * 1024;
const ENTRY_SPOOL_BYTES: usize = 136;
const BUCKET_SPOOL_BYTES: usize = 24;
pub const SIMILARITY_PARTITION_TARGET_REFERENCES: usize = 262_144;

static BUILD_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
struct ExternalSortConfig {
    entry_chunk_records: usize,
    bucket_chunk_records: usize,
    merge_fan_in: usize,
    spool_buffer_bytes: usize,
    partition_target_references: usize,
}

impl ExternalSortConfig {
    const fn production() -> Self {
        Self {
            entry_chunk_records: SORT_CHUNK_BYTES / ENTRY_SPOOL_BYTES,
            bucket_chunk_records: SORT_CHUNK_BYTES / BUCKET_SPOOL_BYTES,
            merge_fan_in: MERGE_FAN_IN,
            spool_buffer_bytes: SPOOL_BUFFER_BYTES,
            partition_target_references: SIMILARITY_PARTITION_TARGET_REFERENCES,
        }
    }

    fn assert_valid(self) {
        assert!(
            self.entry_chunk_records > 0
                && self.bucket_chunk_records > 0
                && self.merge_fan_in >= 2
                && self.spool_buffer_bytes >= ENTRY_SPOOL_BYTES
                && self.partition_target_references > 0,
            "ASSERT: external Similarity sort configuration makes forward progress"
        );
    }
}

pub(crate) struct PartitionedSimilarityBuild {
    pub(crate) logical_entry_count: u64,
    pub(crate) partitions: Vec<BuiltSimilarityPartition>,
}

pub(crate) struct BuiltSimilarityPartition {
    pub(crate) temporary_name: String,
    pub(crate) published_name: String,
    pub(crate) descriptor: SimilarityIndexRunDescriptor,
    pub(crate) minimum_bucket_key: SimilarityBucketKey,
    pub(crate) maximum_bucket_key: SimilarityBucketKey,
}

pub(crate) fn write_partitioned_runs<I, E>(
    storage: &I,
    generation: u64,
    entries: E,
) -> Result<PartitionedSimilarityBuild, SimilarityIndexStoreError>
where
    I: StorageIo,
    E: IntoIterator<Item = SimilarityIndexEntry>,
{
    write_partitioned_runs_with_config(
        storage,
        generation,
        entries,
        ExternalSortConfig::production(),
    )
}

fn write_partitioned_runs_with_config<I, E>(
    storage: &I,
    generation: u64,
    entries: E,
    config: ExternalSortConfig,
) -> Result<PartitionedSimilarityBuild, SimilarityIndexStoreError>
where
    I: StorageIo,
    E: IntoIterator<Item = SimilarityIndexEntry>,
{
    let mut stager = SimilarityEntryStager::with_config(storage, generation, config);
    for entry in entries {
        stager.push(entry)?;
    }
    stager.finish()
}

/// Bounded incremental input for one externally sorted Similarity generation.
///
/// The caller may feed entries while it performs another pool-wide scan. At
/// most one sort chunk remains resident; all earlier chunks are private spools.
pub(crate) struct SimilarityEntryStager<'a, I: StorageIo> {
    scratch: ScratchNames<'a, I>,
    generation: u64,
    config: ExternalSortConfig,
    entry_runs: Vec<SpoolRun>,
    entry_chunk: Vec<EntryRecord>,
    output_names: Vec<String>,
    retain_outputs: bool,
    entries_pushed: u64,
}

impl<'a, I: StorageIo> SimilarityEntryStager<'a, I> {
    pub(crate) fn new(storage: &'a I, generation: u64) -> Self {
        Self::with_config(storage, generation, ExternalSortConfig::production())
    }

    fn with_config(storage: &'a I, generation: u64, config: ExternalSortConfig) -> Self {
        config.assert_valid();
        let nonce = BUILD_NONCE.fetch_add(1, AtomicOrdering::Relaxed);
        let prefix = format!(
            ".similarity-build-{generation:016x}-{:08x}-{nonce:016x}",
            std::process::id()
        );
        Self {
            scratch: ScratchNames::new(storage, prefix),
            generation,
            config,
            entry_runs: Vec::new(),
            entry_chunk: Vec::with_capacity(config.entry_chunk_records),
            output_names: Vec::new(),
            retain_outputs: false,
            entries_pushed: 0,
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.entries_pushed == 0
    }

    pub(crate) fn push(
        &mut self,
        entry: SimilarityIndexEntry,
    ) -> Result<(), SimilarityIndexStoreError> {
        if entry.fingerprint_profile() != SIMILARITY_FINGERPRINT_PROFILE_V1 {
            return Err(SimilarityIndexStoreError::UnsupportedProfile);
        }
        self.entries_pushed = self
            .entries_pushed
            .checked_add(1)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        self.entry_chunk.push(EntryRecord(entry));
        if self.entry_chunk.len() == self.config.entry_chunk_records {
            self.entry_runs.push(write_sorted_chunk(
                &mut self.scratch,
                "entries",
                &mut self.entry_chunk,
                self.config,
            )?);
        }
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
    ) -> Result<PartitionedSimilarityBuild, SimilarityIndexStoreError> {
        if !self.entry_chunk.is_empty() {
            self.entry_runs.push(write_sorted_chunk(
                &mut self.scratch,
                "entries",
                &mut self.entry_chunk,
                self.config,
            )?);
        }
        if self.entry_runs.is_empty() {
            return Err(SimilarityIndexStoreError::InvalidTarget);
        }
        let entry_runs = std::mem::take(&mut self.entry_runs);
        let merged_entry_run =
            merge_all::<I, EntryRecord>(&mut self.scratch, "entries", entry_runs, self.config)?;
        let entry_run = compact_entry_run(
            &mut self.scratch,
            &merged_entry_run,
            self.config.spool_buffer_bytes,
        )?;
        remove_if_present(self.scratch.storage, &merged_entry_run.name)?;
        let (bucket_runs, entry_count, _key_bounds) =
            derive_bucket_runs(&mut self.scratch, &entry_run, self.config)?;
        let raw_bucket_run =
            merge_all::<I, BucketRecord>(&mut self.scratch, "buckets", bucket_runs, self.config)?;
        let (bucket_run, _bucket_count) = truncate_buckets(
            &mut self.scratch,
            &raw_bucket_run,
            self.config.spool_buffer_bytes,
        )?;
        let prepared = PreparedSimilaritySpools {
            entries: entry_run,
            buckets: bucket_run,
            entry_count,
        };
        let build = build_partitioned_run_family_from_spools(
            &mut self.scratch,
            self.generation,
            &prepared,
            self.config,
            &mut self.output_names,
        )?;
        self.scratch.cleanup()?;
        self.retain_outputs = true;
        Ok(build)
    }
}

impl<I: StorageIo> Drop for SimilarityEntryStager<'_, I> {
    fn drop(&mut self) {
        let _ = self.scratch.cleanup();
        if !self.retain_outputs {
            for name in &self.output_names {
                let _ = remove_if_present(self.scratch.storage, name);
            }
        }
    }
}

struct PreparedSimilaritySpools {
    entries: SpoolRun,
    buckets: SpoolRun,
    entry_count: usize,
}

fn build_partitioned_run_family_from_spools<I>(
    scratch: &mut ScratchNames<'_, I>,
    generation: u64,
    prepared: &PreparedSimilaritySpools,
    config: ExternalSortConfig,
    output_names: &mut Vec<String>,
) -> Result<PartitionedSimilarityBuild, SimilarityIndexStoreError>
where
    I: StorageIo,
{
    let logical_entry_count = u64::try_from(prepared.entry_count)
        .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
    let mut bucket_reader = SpoolReader::<I, BucketRecord>::new(
        scratch.storage,
        &prepared.buckets,
        config.spool_buffer_bytes,
    );
    let mut entry_lookup = RandomEntryReader::new(
        scratch.storage,
        &prepared.entries,
        config.spool_buffer_bytes,
    );
    let reference_capacity = config
        .partition_target_references
        .checked_add(64)
        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    let mut references = Vec::new();
    references
        .try_reserve_exact(reference_capacity)
        .map_err(|_| SimilarityIndexStoreError::OutOfMemory)?;
    let mut current_key = None;
    let mut partitions = Vec::new();
    while let Some(record) = bucket_reader.next()? {
        let key = record.0.key();
        if current_key != Some(key)
            && !references.is_empty()
            && references.len() >= config.partition_target_references
        {
            partitions.push(write_bucket_partition(
                scratch,
                generation,
                partitions.len(),
                &references,
                &mut entry_lookup,
                output_names,
            )?);
            references.clear();
        }
        current_key = Some(key);
        references.push(record);
    }
    if !references.is_empty() {
        partitions.push(write_bucket_partition(
            scratch,
            generation,
            partitions.len(),
            &references,
            &mut entry_lookup,
            output_names,
        )?);
    }
    if partitions.is_empty() || partitions.len() > usize::from(u16::MAX) {
        return Err(SimilarityIndexStoreError::TooManyPartitions);
    }
    Ok(PartitionedSimilarityBuild {
        logical_entry_count,
        partitions,
    })
}

fn write_bucket_partition<I: StorageIo>(
    scratch: &ScratchNames<'_, I>,
    generation: u64,
    partition_ordinal: usize,
    global_references: &[BucketRecord],
    entry_lookup: &mut RandomEntryReader<'_, I>,
    output_names: &mut Vec<String>,
) -> Result<BuiltSimilarityPartition, SimilarityIndexStoreError> {
    let minimum_bucket_key = global_references
        .first()
        .ok_or(SimilarityIndexStoreError::IndexCorruption)?
        .0
        .key();
    let maximum_bucket_key = global_references
        .last()
        .ok_or(SimilarityIndexStoreError::IndexCorruption)?
        .0
        .key();
    let mut global_ordinals = Vec::new();
    global_ordinals
        .try_reserve_exact(global_references.len())
        .map_err(|_| SimilarityIndexStoreError::OutOfMemory)?;
    global_ordinals.extend(
        global_references
            .iter()
            .map(|record| record.0.entry_ordinal()),
    );
    global_ordinals.sort_unstable();
    global_ordinals.dedup();
    if global_ordinals.len() > u32::MAX as usize {
        return Err(SimilarityIndexStoreError::CounterOverflow);
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(global_ordinals.len())
        .map_err(|_| SimilarityIndexStoreError::OutOfMemory)?;
    for ordinal in global_ordinals.iter().copied() {
        entries.push(entry_lookup.get(ordinal)?.0);
    }
    let mut references = Vec::new();
    references
        .try_reserve_exact(global_references.len())
        .map_err(|_| SimilarityIndexStoreError::OutOfMemory)?;
    for record in global_references {
        let local_ordinal = global_ordinals
            .binary_search(&record.0.entry_ordinal())
            .map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
        references.push(SimilarityBucketReference::new(
            record.0.key(),
            u32::try_from(local_ordinal).map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
        ));
    }
    let bucket_count = references
        .iter()
        .map(|reference| reference.key())
        .fold((None, 0_usize), |(previous, count), key| {
            (
                Some(key),
                if previous == Some(key) {
                    count
                } else {
                    count + 1
                },
            )
        })
        .1;
    let key_bounds = [
        entries
            .first()
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?
            .chunk_id(),
        entries
            .last()
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?
            .chunk_id(),
    ];
    let layout = SimilarityIndexRunLayout::new(
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        SIMILARITY_REPRESENTATIVE_PROFILE_V1,
        generation,
        entries.len(),
        bucket_count,
        references.len(),
        key_bounds,
    )?;
    let temporary_name = scratch.partition_output_name(partition_ordinal);
    let published_name = partition_published_name(generation, partition_ordinal)?;
    output_names.push(temporary_name.clone());
    let descriptor = stream_partition_vectors(
        scratch.storage,
        &temporary_name,
        layout,
        &entries,
        &references,
    )?;
    Ok(BuiltSimilarityPartition {
        temporary_name,
        published_name,
        descriptor,
        minimum_bucket_key,
        maximum_bucket_key,
    })
}

pub(crate) fn stream_partition_vectors<I: StorageIo>(
    storage: &I,
    output_name: &str,
    layout: SimilarityIndexRunLayout,
    entries: &[SimilarityIndexEntry],
    references: &[SimilarityBucketReference],
) -> Result<SimilarityIndexRunDescriptor, SimilarityIndexStoreError> {
    ensure_object(storage, output_name)?;
    let mut encoder = SimilarityIndexRunStreamEncoder::new(layout)?;
    let page_bytes = u64::try_from(SIMILARITY_INDEX_PAGE_BYTES)
        .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
    let mut offset = 0_u64;
    storage.write_at(output_name, offset, encoder.header())?;
    offset = offset
        .checked_add(page_bytes)
        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    for page_entries in entries.chunks(SIMILARITY_INDEX_ENTRIES_PER_PAGE) {
        let page = encoder.encode_next_entry_page(page_entries)?;
        storage.write_at(output_name, offset, &page)?;
        offset = offset
            .checked_add(page_bytes)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    }
    for page_references in references.chunks(SIMILARITY_BUCKET_REFERENCES_PER_PAGE) {
        let page = encoder.encode_next_bucket_page(page_references)?;
        storage.write_at(output_name, offset, &page)?;
        offset = offset
            .checked_add(page_bytes)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    }
    let (footer, descriptor) = encoder.finish()?;
    storage.write_at(output_name, offset, &footer)?;
    offset = offset
        .checked_add(page_bytes)
        .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    if offset != descriptor.file_length() {
        return Err(SimilarityIndexStoreError::IdentityMismatch);
    }
    storage.set_len(output_name, offset)?;
    Ok(descriptor)
}

fn partition_published_name(
    generation: u64,
    partition_ordinal: usize,
) -> Result<String, SimilarityIndexStoreError> {
    let ordinal = u16::try_from(partition_ordinal)
        .map_err(|_| SimilarityIndexStoreError::TooManyPartitions)?;
    Ok(format!(
        "similarity-part.{SIMILARITY_FINGERPRINT_PROFILE_V1:04x}.{SIMILARITY_REPRESENTATIVE_PROFILE_V1:04x}.{generation:016x}.{ordinal:04x}.fds"
    ))
}

fn write_sorted_chunk<I, R>(
    scratch: &mut ScratchNames<'_, I>,
    stage: &str,
    chunk: &mut Vec<R>,
    config: ExternalSortConfig,
) -> Result<SpoolRun, SimilarityIndexStoreError>
where
    I: StorageIo,
    R: SpoolRecord,
{
    chunk.sort_unstable();
    let name = scratch.create(stage)?;
    let mut writer =
        SpoolWriter::<I, R>::new(scratch.storage, name.clone(), config.spool_buffer_bytes);
    for record in chunk.iter().copied() {
        writer.push(record)?;
    }
    chunk.clear();
    writer.finish()
}

fn merge_all<I, R>(
    scratch: &mut ScratchNames<'_, I>,
    stage: &str,
    mut runs: Vec<SpoolRun>,
    config: ExternalSortConfig,
) -> Result<SpoolRun, SimilarityIndexStoreError>
where
    I: StorageIo,
    R: SpoolRecord,
{
    if runs.is_empty() {
        return Err(SimilarityIndexStoreError::IndexCorruption);
    }
    while runs.len() > 1 {
        let mut merged = Vec::with_capacity(runs.len().div_ceil(config.merge_fan_in));
        for group in runs.chunks(config.merge_fan_in) {
            let name = scratch.create(stage)?;
            let output =
                merge_group::<I, R>(scratch.storage, group, name, config.spool_buffer_bytes)?;
            merged.push(output);
        }
        for run in &runs {
            remove_if_present(scratch.storage, &run.name)?;
        }
        runs = merged;
    }
    runs.pop().ok_or(SimilarityIndexStoreError::IndexCorruption)
}

fn merge_group<I, R>(
    storage: &I,
    inputs: &[SpoolRun],
    output_name: String,
    buffer_bytes: usize,
) -> Result<SpoolRun, SimilarityIndexStoreError>
where
    I: StorageIo,
    R: SpoolRecord,
{
    let per_reader_bytes = (buffer_bytes / inputs.len().max(1)).max(R::BYTES);
    let mut readers = inputs
        .iter()
        .map(|run| SpoolReader::<I, R>::new(storage, run, per_reader_bytes))
        .collect::<Vec<_>>();
    let mut heap = BinaryHeap::with_capacity(readers.len());
    for (ordinal, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = reader.next()? {
            heap.push(Reverse((record, ordinal)));
        }
    }
    let mut writer = SpoolWriter::<I, R>::new(storage, output_name, buffer_bytes);
    while let Some(Reverse((record, ordinal))) = heap.pop() {
        writer.push(record)?;
        if let Some(next) = readers[ordinal].next()? {
            heap.push(Reverse((next, ordinal)));
        }
    }
    writer.finish()
}

fn compact_entry_run<I>(
    scratch: &mut ScratchNames<'_, I>,
    input: &SpoolRun,
    buffer_bytes: usize,
) -> Result<SpoolRun, SimilarityIndexStoreError>
where
    I: StorageIo,
{
    let name = scratch.create("unique-entries")?;
    let mut writer = SpoolWriter::<I, EntryRecord>::new(scratch.storage, name, buffer_bytes);
    let mut reader = SpoolReader::<I, EntryRecord>::new(scratch.storage, input, buffer_bytes);
    let mut previous = None;
    while let Some(record @ EntryRecord(entry)) = reader.next()? {
        if let Some(EntryRecord(previous_entry)) = previous {
            if previous_entry.chunk_id() > entry.chunk_id()
                || (previous_entry.chunk_id() == entry.chunk_id() && previous_entry != entry)
            {
                return Err(SimilarityIndexStoreError::IndexCorruption);
            }
            if previous_entry.chunk_id() == entry.chunk_id() {
                continue;
            }
        }
        writer.push(record)?;
        previous = Some(record);
    }
    writer.finish()
}

fn derive_bucket_runs<I>(
    scratch: &mut ScratchNames<'_, I>,
    entries: &SpoolRun,
    config: ExternalSortConfig,
) -> Result<(Vec<SpoolRun>, usize, [ChunkId; 2]), SimilarityIndexStoreError>
where
    I: StorageIo,
{
    let mut reader =
        SpoolReader::<I, EntryRecord>::new(scratch.storage, entries, config.spool_buffer_bytes);
    let mut bucket_runs = Vec::new();
    let mut chunk = Vec::with_capacity(config.bucket_chunk_records);
    let mut previous: Option<SimilarityIndexEntry> = None;
    let mut minimum = None;
    let mut maximum = None;
    let mut entry_count = 0_usize;
    while let Some(EntryRecord(entry)) = reader.next()? {
        let chunk_id = entry.chunk_id();
        if let Some(previous_entry) = previous {
            if previous_entry.chunk_id() > chunk_id {
                return Err(SimilarityIndexStoreError::IndexCorruption);
            }
            if previous_entry.chunk_id() == chunk_id {
                if previous_entry != entry {
                    return Err(SimilarityIndexStoreError::IndexCorruption);
                }
                continue;
            }
        }
        let ordinal =
            u32::try_from(entry_count).map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
        minimum.get_or_insert(chunk_id);
        maximum = Some(chunk_id);
        previous = Some(entry);
        entry_count = entry_count
            .checked_add(1)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        for (slot, superfeature) in entry.superfeatures().into_iter().enumerate() {
            let key = SimilarityBucketKey::new(
                entry.fingerprint_profile(),
                u8::try_from(slot).map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
                entry.logical_length(),
                superfeature,
            )?;
            chunk.push(BucketRecord(SimilarityBucketReference::new(key, ordinal)));
            if chunk.len() == config.bucket_chunk_records {
                bucket_runs.push(write_sorted_chunk(scratch, "buckets", &mut chunk, config)?);
            }
        }
    }
    if !chunk.is_empty() {
        bucket_runs.push(write_sorted_chunk(scratch, "buckets", &mut chunk, config)?);
    }
    let bounds = [
        minimum.ok_or(SimilarityIndexStoreError::InvalidTarget)?,
        maximum.ok_or(SimilarityIndexStoreError::InvalidTarget)?,
    ];
    Ok((bucket_runs, entry_count, bounds))
}

fn truncate_buckets<I>(
    scratch: &mut ScratchNames<'_, I>,
    input: &SpoolRun,
    buffer_bytes: usize,
) -> Result<(SpoolRun, usize), SimilarityIndexStoreError>
where
    I: StorageIo,
{
    let name = scratch.create("representatives")?;
    let mut writer = SpoolWriter::<I, BucketRecord>::new(scratch.storage, name, buffer_bytes);
    let mut reader = SpoolReader::<I, BucketRecord>::new(scratch.storage, input, buffer_bytes);
    let mut current_key = None;
    let mut in_bucket = 0_usize;
    let mut bucket_count = 0_usize;
    let mut previous_ordinal = None;
    while let Some(record @ BucketRecord(reference)) = reader.next()? {
        if current_key != Some(reference.key()) {
            current_key = Some(reference.key());
            in_bucket = 0;
            previous_ordinal = None;
            bucket_count = bucket_count
                .checked_add(1)
                .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        }
        if previous_ordinal.is_some_and(|ordinal| ordinal >= reference.entry_ordinal()) {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        previous_ordinal = Some(reference.entry_ordinal());
        if in_bucket < 64 {
            writer.push(record)?;
        }
        in_bucket = in_bucket
            .checked_add(1)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
    }
    Ok((writer.finish()?, bucket_count))
}

fn ensure_object<I: StorageIo>(storage: &I, name: &str) -> io::Result<()> {
    match storage.create_new(name) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

fn remove_if_present<I: StorageIo>(storage: &I, name: &str) -> io::Result<()> {
    match storage.remove_file(name) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

struct ScratchNames<'a, I> {
    storage: &'a I,
    prefix: String,
    next: u64,
    names: Vec<String>,
}

impl<'a, I: StorageIo> ScratchNames<'a, I> {
    fn new(storage: &'a I, prefix: String) -> Self {
        Self {
            storage,
            prefix,
            next: 0,
            names: Vec::new(),
        }
    }

    fn create(&mut self, stage: &str) -> io::Result<String> {
        loop {
            let name = format!("{}.{}.{:016x}.spool", self.prefix, stage, self.next);
            self.next = self.next.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Similarity spool name overflow")
            })?;
            match self.storage.create_new(&name) {
                Ok(()) => {
                    self.names.push(name.clone());
                    return Ok(name);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn cleanup(&mut self) -> io::Result<()> {
        let mut first_error = None;
        for name in self.names.drain(..) {
            if let Err(error) = remove_if_present(self.storage, &name) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn partition_output_name(&self, ordinal: usize) -> String {
        format!("{}.partition-{ordinal:08x}.building", self.prefix)
    }
}

#[derive(Clone)]
struct SpoolRun {
    name: String,
    records: usize,
}

trait SpoolRecord: Copy + Ord {
    const BYTES: usize;

    fn encode(self, output: &mut [u8]);
    fn decode(input: &[u8]) -> Result<Self, SimilarityIndexStoreError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EntryRecord(SimilarityIndexEntry);

impl Ord for EntryRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.chunk_id().cmp(&other.0.chunk_id())
    }
}

impl PartialOrd for EntryRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SpoolRecord for EntryRecord {
    const BYTES: usize = ENTRY_SPOOL_BYTES;

    fn encode(self, output: &mut [u8]) {
        assert_eq!(
            output.len(),
            Self::BYTES,
            "ASSERT: fixed entry spool record"
        );
        output[0..32].copy_from_slice(&self.0.chunk_id().bytes());
        output[32..36].copy_from_slice(&self.0.logical_length().to_le_bytes());
        output[36..38].copy_from_slice(&self.0.fingerprint_profile().to_le_bytes());
        output[38..40].fill(0);
        for (ordinal, value) in self.0.superfeatures().into_iter().enumerate() {
            output[40 + ordinal * 8..48 + ordinal * 8].copy_from_slice(&value.to_le_bytes());
        }
        for (ordinal, value) in self.0.sketch().into_iter().enumerate() {
            output[72 + ordinal * 8..80 + ordinal * 8].copy_from_slice(&value.to_le_bytes());
        }
    }

    fn decode(input: &[u8]) -> Result<Self, SimilarityIndexStoreError> {
        if input.len() != Self::BYTES || input[38..40].iter().any(|byte| *byte != 0) {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        let mut id = [0_u8; 32];
        id.copy_from_slice(&input[0..32]);
        let mut superfeatures = [0_u64; 4];
        for (ordinal, value) in superfeatures.iter_mut().enumerate() {
            *value = read_u64(input, 40 + ordinal * 8);
        }
        let mut sketch = [0_u64; 8];
        for (ordinal, value) in sketch.iter_mut().enumerate() {
            *value = read_u64(input, 72 + ordinal * 8);
        }
        Ok(Self(SimilarityIndexEntry::new(
            ChunkId::from_bytes(id),
            read_u32(input, 32),
            read_u16(input, 36),
            superfeatures,
            sketch,
        )?))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BucketRecord(SimilarityBucketReference);

impl Ord for BucketRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0.key(), self.0.entry_ordinal()).cmp(&(other.0.key(), other.0.entry_ordinal()))
    }
}

impl PartialOrd for BucketRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SpoolRecord for BucketRecord {
    const BYTES: usize = BUCKET_SPOOL_BYTES;

    fn encode(self, output: &mut [u8]) {
        assert_eq!(
            output.len(),
            Self::BYTES,
            "ASSERT: fixed bucket spool record"
        );
        let key = self.0.key();
        output[0..2].copy_from_slice(&key.fingerprint_profile().to_le_bytes());
        output[2] = key.slot();
        output[3] = 0;
        output[4..8].copy_from_slice(&key.logical_length().to_le_bytes());
        output[8..16].copy_from_slice(&key.superfeature().to_le_bytes());
        output[16..20].copy_from_slice(&self.0.entry_ordinal().to_le_bytes());
        output[20..24].fill(0);
    }

    fn decode(input: &[u8]) -> Result<Self, SimilarityIndexStoreError> {
        if input.len() != Self::BYTES
            || input[3] != 0
            || input[20..24].iter().any(|byte| *byte != 0)
        {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        let key = SimilarityBucketKey::new(
            read_u16(input, 0),
            input[2],
            read_u32(input, 4),
            read_u64(input, 8),
        )?;
        Ok(Self(SimilarityBucketReference::new(
            key,
            read_u32(input, 16),
        )))
    }
}

struct SpoolWriter<'a, I, R> {
    storage: &'a I,
    name: String,
    buffer: Vec<u8>,
    buffer_bytes: usize,
    offset: u64,
    records: usize,
    marker: std::marker::PhantomData<R>,
}

impl<'a, I: StorageIo, R: SpoolRecord> SpoolWriter<'a, I, R> {
    fn new(storage: &'a I, name: String, buffer_bytes: usize) -> Self {
        Self {
            storage,
            name,
            buffer: Vec::with_capacity(buffer_bytes.max(R::BYTES)),
            buffer_bytes: buffer_bytes.max(R::BYTES),
            offset: 0,
            records: 0,
            marker: std::marker::PhantomData,
        }
    }

    fn push(&mut self, record: R) -> Result<(), SimilarityIndexStoreError> {
        if self.buffer.len() + R::BYTES > self.buffer_bytes {
            self.flush()?;
        }
        let start = self.buffer.len();
        self.buffer.resize(start + R::BYTES, 0);
        record.encode(&mut self.buffer[start..]);
        self.records = self
            .records
            .checked_add(1)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), SimilarityIndexStoreError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.storage
            .write_at(&self.name, self.offset, &self.buffer)?;
        self.offset = self
            .offset
            .checked_add(
                u64::try_from(self.buffer.len())
                    .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
            )
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        self.buffer.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<SpoolRun, SimilarityIndexStoreError> {
        self.flush()?;
        self.storage.set_len(&self.name, self.offset)?;
        Ok(SpoolRun {
            name: self.name,
            records: self.records,
        })
    }
}

struct SpoolReader<'a, I, R> {
    storage: &'a I,
    name: &'a str,
    records: usize,
    consumed: usize,
    records_per_read: usize,
    decoded: Vec<R>,
    cursor: usize,
}

impl<'a, I: StorageIo, R: SpoolRecord> SpoolReader<'a, I, R> {
    fn new(storage: &'a I, run: &'a SpoolRun, buffer_bytes: usize) -> Self {
        Self {
            storage,
            name: &run.name,
            records: run.records,
            consumed: 0,
            records_per_read: (buffer_bytes / R::BYTES).max(1),
            decoded: Vec::new(),
            cursor: 0,
        }
    }

    fn next(&mut self) -> Result<Option<R>, SimilarityIndexStoreError> {
        if let Some(record) = self.decoded.get(self.cursor).copied() {
            self.cursor += 1;
            return Ok(Some(record));
        }
        if self.consumed == self.records {
            return Ok(None);
        }
        let count = (self.records - self.consumed).min(self.records_per_read);
        let byte_offset = self
            .consumed
            .checked_mul(R::BYTES)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        let length = count
            .checked_mul(R::BYTES)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        let bytes = self.storage.read_exact_at(
            self.name,
            u64::try_from(byte_offset).map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
            length,
        )?;
        self.decoded.clear();
        self.decoded
            .try_reserve(count)
            .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
        for record in bytes.chunks_exact(R::BYTES) {
            self.decoded.push(R::decode(record)?);
        }
        if self.decoded.len() != count {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        self.consumed = self
            .consumed
            .checked_add(count)
            .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
        self.cursor = 1;
        Ok(self.decoded.first().copied())
    }
}

struct RandomEntryReader<'a, I> {
    storage: &'a I,
    run: &'a SpoolRun,
    records_per_read: usize,
    cached_block: Option<usize>,
    decoded: Vec<EntryRecord>,
}

impl<'a, I: StorageIo> RandomEntryReader<'a, I> {
    fn new(storage: &'a I, run: &'a SpoolRun, buffer_bytes: usize) -> Self {
        Self {
            storage,
            run,
            records_per_read: (buffer_bytes / ENTRY_SPOOL_BYTES).max(1),
            cached_block: None,
            decoded: Vec::new(),
        }
    }

    fn get(&mut self, ordinal: u32) -> Result<EntryRecord, SimilarityIndexStoreError> {
        let ordinal =
            usize::try_from(ordinal).map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
        if ordinal >= self.run.records {
            return Err(SimilarityIndexStoreError::IndexCorruption);
        }
        let block = ordinal / self.records_per_read;
        if self.cached_block != Some(block) {
            let first_record = block
                .checked_mul(self.records_per_read)
                .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
            let count = (self.run.records - first_record).min(self.records_per_read);
            let byte_offset = first_record
                .checked_mul(ENTRY_SPOOL_BYTES)
                .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
            let byte_length = count
                .checked_mul(ENTRY_SPOOL_BYTES)
                .ok_or(SimilarityIndexStoreError::CounterOverflow)?;
            let bytes = self.storage.read_exact_at(
                &self.run.name,
                u64::try_from(byte_offset)
                    .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?,
                byte_length,
            )?;
            self.decoded.clear();
            self.decoded
                .try_reserve(count)
                .map_err(|_| SimilarityIndexStoreError::OutOfMemory)?;
            for bytes in bytes.chunks_exact(ENTRY_SPOOL_BYTES) {
                self.decoded.push(EntryRecord::decode(bytes)?);
            }
            if self.decoded.len() != count {
                return Err(SimilarityIndexStoreError::IndexCorruption);
            }
            self.cached_block = Some(block);
        }
        self.decoded
            .get(ordinal % self.records_per_read)
            .copied()
            .ok_or(SimilarityIndexStoreError::IndexCorruption)
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::FsStorageIo;

    #[test]
    fn duplicate_chunk_ids_are_compacted_before_partition_streaming() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock is after Unix epoch")
            .as_nanos();
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(".artifacts/tests")
            .join(format!(
                "similarity-external-duplicates-{}-{nonce}",
                std::process::id()
            ));
        let storage = FsStorageIo::open(root).expect("open duplicate fixture root");
        let unique = (0_u64..4)
            .map(|ordinal| {
                SimilarityIndexEntry::new(
                    ChunkId::of(&ordinal.to_le_bytes()),
                    64 * 1_024,
                    SIMILARITY_FINGERPRINT_PROFILE_V1,
                    [ordinal; 4],
                    [ordinal; 8],
                )
                .expect("construct duplicate fixture entry")
            })
            .collect::<Vec<_>>();
        let entries = vec![
            unique[3], unique[1], unique[0], unique[3], unique[2], unique[1],
        ];
        let config = ExternalSortConfig {
            entry_chunk_records: 2,
            bucket_chunk_records: 3,
            merge_fan_in: 2,
            spool_buffer_bytes: ENTRY_SPOOL_BYTES * 2,
            partition_target_references: 5,
        };

        let build = write_partitioned_runs_with_config(&storage, 78, entries, config)
            .expect("compact duplicate Chunk IDs into one canonical entry");

        assert_eq!(build.logical_entry_count, unique.len() as u64);
        for partition in &build.partitions {
            fastdup_format::SimilarityIndexRun::decode(
                &storage
                    .read(&partition.temporary_name)
                    .expect("read duplicate fixture partition"),
            )
            .expect("decode duplicate-free partition");
            remove_if_present(&storage, &partition.temporary_name)
                .expect("remove duplicate fixture partition");
        }
    }

    #[test]
    fn tiny_chunks_force_multilevel_merge_and_bucket_partitions() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock is after Unix epoch")
            .as_nanos();
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(".artifacts/tests")
            .join(format!(
                "similarity-external-multipass-{}-{nonce}",
                std::process::id()
            ));
        let storage = FsStorageIo::open(root).expect("open multipass fixture root");
        let entries = (0_u64..17)
            .rev()
            .map(|ordinal| {
                SimilarityIndexEntry::new(
                    ChunkId::of(&ordinal.to_le_bytes()),
                    64 * 1_024,
                    SIMILARITY_FINGERPRINT_PROFILE_V1,
                    [ordinal % 3, ordinal % 5, ordinal % 7, ordinal % 11],
                    [ordinal; 8],
                )
                .expect("construct multipass fixture entry")
            })
            .collect::<Vec<_>>();
        let config = ExternalSortConfig {
            entry_chunk_records: 2,
            bucket_chunk_records: 3,
            merge_fan_in: 2,
            spool_buffer_bytes: ENTRY_SPOOL_BYTES * 2,
            partition_target_references: 5,
        };

        let build = write_partitioned_runs_with_config(&storage, 77, entries, config)
            .expect("write multipass partition family");
        assert_eq!(build.logical_entry_count, 17);
        assert!(build.partitions.len() > 2);
        assert!(
            build
                .partitions
                .windows(2)
                .all(|pair| { pair[0].maximum_bucket_key < pair[1].minimum_bucket_key })
        );
        for partition in &build.partitions {
            let actual = storage
                .read(&partition.temporary_name)
                .expect("read multipass partition output");
            let decoded = fastdup_format::SimilarityIndexRun::decode(&actual)
                .expect("decode multipass partition output");
            assert_eq!(decoded.generation(), 77);
            assert_eq!(
                partition.descriptor.file_length(),
                u64::try_from(actual.len()).expect("fixture length fits u64")
            );
            remove_if_present(&storage, &partition.temporary_name)
                .expect("remove multipass partition output");
        }
        assert!(
            storage
                .list_names()
                .expect("list multipass objects")
                .iter()
                .all(|name| !name.contains("similarity-build"))
        );
    }
}
