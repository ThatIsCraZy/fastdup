#![forbid(unsafe_code)]

//! Durable container lifecycle behind an injectable storage boundary.

mod exact_index_repository;
mod generation;
mod generation_log;
mod manifest_reader;
mod manifest_tree;
pub use manifest_tree::ManifestRangeExtent;
mod reduction;
mod reduction_codec;
mod reduction_dictionary;
mod reduction_filter;
mod reduction_similarity;

pub use exact_index_repository::{
    ActivatedExactIndex, ExactIndexLookup, ExactIndexRunReader, ExactIndexRunRepository,
    ExactIndexStoreError, MAX_ACTIVE_EXACT_INDEX_RUNS, MAX_EXACT_COMPACTION_ENTRIES,
    MAX_EXACT_LOOKUP_CANDIDATES,
};
pub use generation::{
    CommittedDataGeneration, GenerationError, GenerationRepository, IndexedRequiredChunkVerifier,
    RecoveredDataGeneration, RecoveredGeneration, RequiredChunkVerifier, VerifiedCommittedFile,
    WalTail,
};
pub use manifest_reader::{MAX_MANIFEST_READ_BYTES, ManifestReadError, VerifiedManifestFile};
pub use manifest_tree::ManifestTreeError;
pub use reduction::{
    ReducedObject, ReductionAuditReport, ReductionEngine, ReductionError, ReductionFeatures,
    ReductionPolicy, ReductionReport, ReductionRuntime,
};
pub use reduction_dictionary::{ReductionDictionary, ReductionDictionaryError};
pub use reduction_filter::{
    BlockedBloomHint, BloomLookupHint, HintStructureError, PerWorkerLocationHintCache,
    UnverifiedLocationHint,
};

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fastdup_format::{
    BuildingContainerHeader, ContainerId, ExactIndexEntry, ExactIndexLocation,
    ExactLocationTransition, FOOTER_BYTES, FormatError, HEADER_BYTES, MAX_CONTAINER_BYTES,
    SealedContainer, SealedContainerDescriptor,
};

/// Hard allocation bound for one exact random read through [`StorageIo`].
pub const MAX_STORAGE_RANGE_BYTES: usize = 1_024 * 1_024;

/// Process-cost and byte-accounting evidence for one adaptive Container
/// publication.
///
/// Process CPU includes every thread in this process while the phase is
/// active, including the bounded encoding workers. A caller running unrelated
/// work concurrently must therefore treat CPU attribution as an upper bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdaptiveContainerPublishMetrics {
    encode_wall: Duration,
    encode_process_cpu: Duration,
    publish_wall: Duration,
    publish_process_cpu: Duration,
    file_bytes: u64,
    logical_bytes: u64,
    raw_records: usize,
    zstd_records: usize,
}

impl AdaptiveContainerPublishMetrics {
    #[must_use]
    pub const fn encode_wall(self) -> Duration {
        self.encode_wall
    }

    #[must_use]
    pub const fn encode_process_cpu(self) -> Duration {
        self.encode_process_cpu
    }

    #[must_use]
    pub const fn publish_wall(self) -> Duration {
        self.publish_wall
    }

    #[must_use]
    pub const fn publish_process_cpu(self) -> Duration {
        self.publish_process_cpu
    }

    #[must_use]
    pub const fn file_bytes(self) -> u64 {
        self.file_bytes
    }

    #[must_use]
    pub const fn logical_bytes(self) -> u64 {
        self.logical_bytes
    }

    #[must_use]
    pub const fn raw_records(self) -> usize {
        self.raw_records
    }

    #[must_use]
    pub const fn zstd_records(self) -> usize {
        self.zstd_records
    }
}

#[derive(Clone, Debug)]
pub struct ContainerStore {
    repository: ContainerRepository<FsStorageIo>,
}

impl ContainerStore {
    /// Opens or initializes a filesystem-backed container directory.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the directory cannot be created or opened.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let storage = FsStorageIo::open(root)?;
        Ok(Self {
            repository: ContainerRepository::new(storage),
        })
    }

    /// Durably publishes one immutable RAW container.
    ///
    /// # Errors
    ///
    /// Returns format validation or file/directory durability errors. Existing
    /// published IDs are never replaced.
    ///
    /// # Panics
    ///
    /// Panics only if the validated format writer violates its internal v1
    /// size or cursor bounds. This is a production-fatal `ASSERT`, not an
    /// expected storage error.
    pub fn publish_raw(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        chunks: &[&[u8]],
    ) -> Result<(), StoreError> {
        self.repository
            .publish_raw(container_id, container_generation, chunks)
    }

    /// Durably publishes bounded Compression Regions with adaptive RAW/Zstd
    /// selection under the version-1 complete-record cost rule.
    ///
    /// # Errors
    ///
    /// Returns format validation, compression, file, or directory durability
    /// errors. Existing published IDs are never replaced.
    pub fn publish_adaptive_regions(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
    ) -> Result<(), StoreError> {
        self.repository
            .publish_adaptive_regions(container_id, container_generation, regions)
    }

    /// Opens and fully verifies a published container by identity.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when absent/unreadable or a format integrity error.
    pub fn read(&self, container_id: ContainerId) -> Result<SealedContainer, StoreError> {
        self.repository.read(container_id)
    }

    /// Discovers and fully verifies every published container in ID order.
    ///
    /// Temporary and unrelated names are ignored. A malformed `.fdc` name,
    /// invalid container, or filename/header identity mismatch fails recovery.
    ///
    /// # Errors
    ///
    /// Returns namespace I/O, naming, or container integrity errors.
    pub fn recover_published(&self) -> Result<Vec<SealedContainer>, StoreError> {
        self.repository.recover_published()
    }

    /// Fully verifies every published container while retaining only compact
    /// identity and layout metadata.
    ///
    /// Unlike [`Self::recover_published`], memory use is bounded by one decoded
    /// container plus the compact result vector rather than all payload bytes.
    ///
    /// # Errors
    ///
    /// Returns namespace I/O, naming, container integrity, or identity errors.
    pub fn verify_published(&self) -> Result<Vec<PublishedContainerSummary>, StoreError> {
        self.repository.verify_published()
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.repository.storage.root()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishedContainerSummary {
    container_id: ContainerId,
    container_generation: u64,
    chunk_count: usize,
    raw_record_count: usize,
    zstd_record_count: usize,
    file_length: u64,
}

impl PublishedContainerSummary {
    #[must_use]
    pub const fn container_id(self) -> ContainerId {
        self.container_id
    }

    #[must_use]
    pub const fn container_generation(self) -> u64 {
        self.container_generation
    }

    #[must_use]
    pub const fn chunk_count(self) -> usize {
        self.chunk_count
    }

    #[must_use]
    pub const fn raw_record_count(self) -> usize {
        self.raw_record_count
    }

    #[must_use]
    pub const fn zstd_record_count(self) -> usize {
        self.zstd_record_count
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length
    }
}

pub trait StorageIo {
    /// Creates one empty name without replacing an existing object.
    ///
    /// # Errors
    ///
    /// Returns the backend's creation error.
    fn create_new(&self, name: &str) -> io::Result<()>;
    /// Checks one canonical internal name without enumerating its directory.
    ///
    /// # Errors
    ///
    /// Returns the backend's path or metadata lookup error.
    fn exists(&self, name: &str) -> io::Result<bool>;
    /// Writes the complete byte slice at an exact offset.
    ///
    /// # Errors
    ///
    /// Returns the backend's seek, capacity, or write error.
    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()>;
    /// Reads the complete current object.
    ///
    /// # Errors
    ///
    /// Returns the backend's lookup or read error.
    fn read(&self, name: &str) -> io::Result<Vec<u8>>;
    /// Returns the current exact object length without reading its contents.
    ///
    /// # Errors
    ///
    /// Returns the backend's lookup or metadata error.
    fn object_len(&self, name: &str) -> io::Result<u64>;
    /// Reads exactly one bounded range without materializing the whole object.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` above [`MAX_STORAGE_RANGE_BYTES`],
    /// `UnexpectedEof` for a range outside the current object, or the backend's
    /// exact-read error. Partial bytes are never returned as verified evidence.
    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>>;
    /// Lists the current names in the container publication directory.
    ///
    /// # Errors
    ///
    /// Returns the backend's directory-read or name-decoding error.
    fn list_names(&self) -> io::Result<Vec<String>>;
    /// Fixes the object's exact logical length before validation and sync.
    ///
    /// # Errors
    ///
    /// Returns the backend's lookup, range, or truncation error.
    fn set_len(&self, name: &str, length: u64) -> io::Result<()>;
    /// Makes all object bytes stable before publication.
    ///
    /// # Errors
    ///
    /// Returns the backend's durability error.
    fn sync_file(&self, name: &str) -> io::Result<()>;
    /// Atomically publishes a stable temporary object without replacement.
    ///
    /// # Errors
    ///
    /// Returns lookup, collision, or namespace mutation errors.
    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()>;
    /// Makes namespace publication stable.
    ///
    /// # Errors
    ///
    /// Returns the backend's directory durability error.
    fn sync_root(&self) -> io::Result<()>;
}

#[derive(Clone, Debug)]
pub struct ContainerRepository<I> {
    storage: I,
}

impl<I: StorageIo> ContainerRepository<I> {
    #[must_use]
    pub const fn new(storage: I) -> Self {
        Self { storage }
    }

    /// Runs the format writer and ordered durable publication protocol.
    ///
    /// # Errors
    ///
    /// Returns the first format, backend I/O, or durability error.
    ///
    /// # Panics
    ///
    /// Panics only if the validated format writer violates its internal v1
    /// size or cursor bounds. This is a production-fatal `ASSERT`, not an
    /// expected storage error.
    pub fn publish_raw(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        chunks: &[&[u8]],
    ) -> Result<(), StoreError> {
        let sealed = SealedContainer::encode(container_id, container_generation, chunks)?;
        self.publish_sealed(container_id, container_generation, &sealed)
            .map(drop)
    }

    /// Runs adaptive RAW/Zstd region encoding and the same ordered durable
    /// publication protocol as [`Self::publish_raw`].
    ///
    /// # Errors
    ///
    /// Returns the first format, compression, backend I/O, or durability
    /// error. Existing published IDs are never replaced.
    ///
    /// # Panics
    ///
    /// Panics only if the validated format writer violates its own bounded
    /// geometry, an impossible internal writer state.
    pub fn publish_adaptive_regions(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
    ) -> Result<(), StoreError> {
        let sealed =
            SealedContainer::encode_adaptive_regions(container_id, container_generation, regions)?;
        self.publish_sealed(container_id, container_generation, &sealed)
            .map(drop)
    }

    /// Publishes adaptive RAW/Zstd regions and returns the complete verified
    /// Container evidence from the mandatory writer reread.
    ///
    /// This avoids decoding the just-published object a second time when a
    /// checkpoint constructs an Exact-Index level-zero Run.
    ///
    /// # Errors
    ///
    /// Returns the same format, compression, I/O, and durability errors as
    /// [`Self::publish_adaptive_regions`].
    pub fn publish_adaptive_regions_verified(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
    ) -> Result<SealedContainer, StoreError> {
        self.publish_adaptive_regions_parallel_verified(
            container_id,
            container_generation,
            regions,
            NonZeroUsize::MIN,
        )
    }

    /// Publishes adaptive regions encoded by a bounded scoped worker pool and
    /// returns the mandatory complete writer-reread proof.
    ///
    /// # Errors
    ///
    /// Returns the same format, compression, I/O, and durability errors as
    /// [`Self::publish_adaptive_regions_verified`].
    pub fn publish_adaptive_regions_parallel_verified(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
    ) -> Result<SealedContainer, StoreError> {
        self.publish_adaptive_regions_parallel_profiled(
            container_id,
            container_generation,
            regions,
            workers,
        )
        .map(|(verified, _metrics)| verified)
    }

    /// Publishes adaptive regions while measuring encoding separately from
    /// ordered durable publication and writer reread.
    ///
    /// The returned Container remains the same complete verification evidence
    /// as [`Self::publish_adaptive_regions_parallel_verified`]. Metrics never
    /// authorize bytes or weaken a failed durability operation.
    ///
    /// # Errors
    ///
    /// Returns the same format, compression, I/O, and durability errors as
    /// [`Self::publish_adaptive_regions_parallel_verified`].
    ///
    /// # Panics
    ///
    /// Panics only if a validated format-v1 writer violates its bounded
    /// length/accounting invariants or process CPU time moves backwards.
    pub fn publish_adaptive_regions_parallel_profiled(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
    ) -> Result<(SealedContainer, AdaptiveContainerPublishMetrics), StoreError> {
        let encode_wall_started = Instant::now();
        let encode_cpu_started = process_cpu_time();
        let sealed = SealedContainer::encode_adaptive_regions_parallel(
            container_id,
            container_generation,
            regions,
            workers,
        )?;
        let encode_wall = encode_wall_started.elapsed();
        let encode_process_cpu = process_cpu_elapsed(encode_cpu_started);
        let file_bytes = u64::try_from(sealed.len())
            .expect("ASSERT: a format-v1 Container image length fits u64");
        let publish_wall_started = Instant::now();
        let publish_cpu_started = process_cpu_time();
        let verified = self.publish_sealed(container_id, container_generation, &sealed)?;
        let publish_wall = publish_wall_started.elapsed();
        let publish_process_cpu = process_cpu_elapsed(publish_cpu_started);
        let logical_bytes = verified.records().iter().try_fold(0_u64, |total, record| {
            total.checked_add(
                u64::try_from(record.payload().len())
                    .expect("ASSERT: a bounded decoded Chunk length fits u64"),
            )
        });
        let logical_bytes = logical_bytes
            .expect("ASSERT: a bounded format-v1 Container logical byte sum cannot overflow");
        let metrics = AdaptiveContainerPublishMetrics {
            encode_wall,
            encode_process_cpu,
            publish_wall,
            publish_process_cpu,
            file_bytes,
            logical_bytes,
            raw_records: verified.raw_record_count(),
            zstd_records: verified.zstd_record_count(),
        };
        Ok((verified, metrics))
    }

    fn publish_sealed(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        sealed: &[u8],
    ) -> Result<SealedContainer, StoreError> {
        let building = BuildingContainerHeader::new(container_id, container_generation)?.encode();
        let temporary_name = temporary_name(container_id);
        let published_name = published_name(container_id);
        let sealed_length = u64::try_from(sealed.len())
            .expect("ASSERT: a format-v1 container length always fits u64");
        assert!(
            sealed_length <= MAX_CONTAINER_BYTES,
            "ASSERT: the format writer returned an oversized container"
        );

        self.storage.create_new(&temporary_name)?;
        self.storage.write_at(&temporary_name, 0, &building)?;
        let mut offset = HEADER_BYTES;
        while offset < sealed.len() {
            let end = offset
                .checked_add(HEADER_BYTES)
                .expect("ASSERT: a bounded container write cursor cannot overflow")
                .min(sealed.len());
            self.storage.write_at(
                &temporary_name,
                u64::try_from(offset)
                    .expect("ASSERT: a bounded container write offset always fits u64"),
                &sealed[offset..end],
            )?;
            offset = end;
        }
        self.storage
            .write_at(&temporary_name, 0, &sealed[..HEADER_BYTES])?;
        self.storage.set_len(&temporary_name, sealed_length)?;
        let reread = self.storage.read(&temporary_name)?;
        let verified = SealedContainer::decode(&reread)?;
        if reread != sealed
            || verified.header().container_id() != container_id
            || verified.header().container_generation() != container_generation
        {
            return Err(StoreError::PublishVerificationMismatch);
        }
        self.storage.sync_file(&temporary_name)?;
        self.storage
            .publish_noreplace(&temporary_name, &published_name)?;
        self.storage.sync_root()?;
        Ok(verified)
    }

    /// Reads a published object through the production format verifier.
    ///
    /// # Errors
    ///
    /// Returns the backend read error or any container integrity failure.
    pub fn read(&self, container_id: ContainerId) -> Result<SealedContainer, StoreError> {
        let bytes = self.storage.read(&published_name(container_id))?;
        let container = SealedContainer::decode(&bytes)?;
        let embedded_id = container.header().container_id();
        if embedded_id != container_id {
            return Err(StoreError::PublishedIdentityMismatch {
                name: container_id,
                header: embedded_id,
            });
        }
        Ok(container)
    }

    /// Locates one logical Chunk by identity, fully verifies its containing
    /// immutable container, and returns an owned byte-exact copy.
    ///
    /// This bounded rebuild/read seam intentionally scans published containers;
    /// the persistent Exact Index will later accelerate location selection
    /// without becoming authoritative.
    ///
    /// # Errors
    ///
    /// Returns naming, I/O, container-integrity, identity, or missing-location
    /// errors. A Chunk ID with a different logical length is not accepted.
    pub fn read_verified_chunk(
        &self,
        chunk_id: fastdup_format::ChunkId,
        logical_length: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let mut names = self.storage.list_names()?;
        names.sort_unstable();
        for name in names {
            let Some(expected_id) = parse_published_name(&name)? else {
                continue;
            };
            let bytes = self.storage.read(&name)?;
            let container = SealedContainer::decode(&bytes)?;
            let embedded_id = container.header().container_id();
            if embedded_id != expected_id {
                return Err(StoreError::PublishedIdentityMismatch {
                    name: expected_id,
                    header: embedded_id,
                });
            }
            let Some(payload) = container.chunk(chunk_id) else {
                continue;
            };
            if u64::try_from(payload.len()) == Ok(logical_length) {
                return Ok(payload.to_vec());
            }
        }
        Err(StoreError::MissingVerifiedChunk {
            chunk_id,
            logical_length,
        })
    }

    /// Resolves one Exact Index candidate by its canonical Container name and
    /// returns bytes only after pairing the sealed Header/Footer envelope and
    /// every physical coordinate, then verifying the complete selected Record
    /// CRC and decoded Chunk ID.
    ///
    /// This avoids the directory scan used by [`Self::read_verified_chunk`],
    /// but the index entry remains acceleration state: it cannot construct the
    /// verification proof and a mismatch never returns bytes.
    ///
    /// # Errors
    ///
    /// Returns Container I/O/integrity failures or
    /// [`StoreError::ExactLocationMismatch`] for a non-ACTIVE, stale, forged,
    /// or otherwise unpaired index candidate.
    pub fn read_verified_location(
        &self,
        candidate: ExactIndexEntry,
    ) -> Result<Vec<u8>, StoreError> {
        if candidate.transition() != ExactLocationTransition::Active {
            return Err(StoreError::ExactLocationMismatch);
        }
        let location = candidate.location();
        let name = published_name(location.container_id());
        let actual_length = self.storage.object_len(&name)?;
        let minimum_length = u64::try_from(HEADER_BYTES)
            .map_err(|_| FormatError::ArithmeticOverflow)?
            .checked_add(FOOTER_BYTES)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if actual_length < minimum_length
            || actual_length > MAX_CONTAINER_BYTES
            || !actual_length.is_multiple_of(FOOTER_BYTES)
        {
            return Err(StoreError::Format(FormatError::InvalidContainerLength(
                usize::try_from(actual_length).unwrap_or(usize::MAX),
            )));
        }
        let footer_offset = actual_length
            .checked_sub(FOOTER_BYTES)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let footer = self.storage.read_exact_at(
            &name,
            footer_offset,
            usize::try_from(FOOTER_BYTES).map_err(|_| FormatError::ArithmeticOverflow)?,
        )?;
        let header = self.storage.read_exact_at(&name, 0, HEADER_BYTES)?;
        let descriptor = SealedContainerDescriptor::decode(&header, &footer, actual_length)?;
        let range = descriptor
            .record_range(candidate)
            .map_err(map_exact_location_error)?;
        let encoded_record = self
            .storage
            .read_exact_at(&name, range.offset(), range.length())?;
        let record = descriptor
            .decode_candidate(candidate, &encoded_record)
            .map_err(map_exact_location_error)?;
        Ok(record.payload().to_vec())
    }

    /// Attempts one bounded persistent Exact-Index lookup without falling back
    /// to a Container directory scan.
    ///
    /// Every positive result is paired with and decoded from its immutable
    /// Container record. Missing, corrupt, stale, or unsupported acceleration
    /// returns `Ok(None)` because the index is non-authoritative; callers may
    /// store a duplicate Location or choose an explicit verified slow path.
    ///
    /// # Errors
    ///
    /// Returns only impossible requested-length conversion failures. Candidate
    /// and index I/O/integrity failures deliberately degrade to `Ok(None)`.
    ///
    /// # Panics
    ///
    /// Panics if the activated reader violates its hard candidate bound or
    /// returns a different key after successful page validation.
    pub fn find_verified_chunk_with_index<J: StorageIo>(
        &self,
        index: &ActivatedExactIndex<J>,
        chunk_id: fastdup_format::ChunkId,
        logical_length: u64,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .find_verified_candidate_with_index(index, chunk_id, logical_length)
            .map(|(_, bytes)| bytes))
    }

    /// Resolves one Exact Index candidate and returns its physical descriptor
    /// only after pairing it with and decoding the immutable Container record.
    ///
    /// The returned entry is still not authoritative metadata. A later read
    /// must pass it back through [`Self::read_verified_location`], which repeats
    /// all physical-coordinate, checksum, and Chunk-ID verification.
    ///
    /// # Errors
    ///
    /// Returns only impossible requested-length conversion failures. Candidate
    /// and index I/O/integrity failures deliberately degrade to `Ok(None)`.
    ///
    /// # Panics
    ///
    /// Panics if the activated reader violates its hard candidate bound or
    /// returns a different key after successful page validation.
    pub fn find_verified_location_with_index<J: StorageIo>(
        &self,
        index: &ActivatedExactIndex<J>,
        chunk_id: fastdup_format::ChunkId,
        logical_length: u64,
    ) -> Result<Option<ExactIndexEntry>, StoreError> {
        Ok(self
            .find_verified_candidate_with_index(index, chunk_id, logical_length)
            .map(|(entry, _)| entry))
    }

    fn find_verified_candidate_with_index<J: StorageIo>(
        &self,
        index: &ActivatedExactIndex<J>,
        chunk_id: fastdup_format::ChunkId,
        logical_length: u64,
    ) -> Option<(ExactIndexEntry, Vec<u8>)> {
        let Ok(index_length) = u32::try_from(logical_length) else {
            return None;
        };
        let Ok(lookup) = index.lookup_transitions(chunk_id, index_length) else {
            return None;
        };
        let mut seen_locations: [Option<ExactIndexLocation>; MAX_EXACT_LOOKUP_CANDIDATES] =
            [None; MAX_EXACT_LOOKUP_CANDIDATES];
        let mut seen_count = 0_usize;
        let mut attempted = 0_u8;
        for candidate in lookup.candidates().iter().copied() {
            assert_eq!(
                candidate.chunk_id(),
                chunk_id,
                "ASSERT: Exact Index lookup returned a different Chunk ID"
            );
            assert_eq!(
                candidate.logical_length(),
                index_length,
                "ASSERT: Exact Index lookup returned a different logical length"
            );
            let location = candidate.location();
            if seen_locations[..seen_count].contains(&Some(location)) {
                continue;
            }
            assert!(
                seen_count < seen_locations.len(),
                "ASSERT: bounded lookup returned more candidates than its hard limit"
            );
            seen_locations[seen_count] = Some(location);
            seen_count += 1;
            if candidate.transition() != ExactLocationTransition::Active {
                continue;
            }
            if attempted == 2 {
                break;
            }
            attempted += 1;
            if let Ok(bytes) = self.read_verified_location(candidate) {
                return Some((candidate, bytes));
            }
        }
        None
    }

    /// Resolves one Chunk through an activated persistent Exact Index and
    /// performs bounded demand verification of the selected Container record.
    ///
    /// The newest transition for each repeated physical Location wins. At most
    /// two ACTIVE candidates are attempted. An index error, negative result,
    /// or two unusable candidates falls back to the authoritative verified
    /// Container scan so index damage cannot make committed data unreadable.
    ///
    /// # Errors
    ///
    /// Returns the first candidate integrity error when both bounded candidate
    /// attempts and the verified slow path fail; otherwise returns the slow
    /// path's Container error. No unchecked or partial bytes are returned.
    ///
    /// # Panics
    ///
    /// Panics if an [`ActivatedExactIndex`] violates its own hard candidate
    /// bound or returns a key other than the one requested. Both indicate an
    /// impossible internal contract violation after successful page parsing.
    pub fn read_verified_chunk_with_index<J: StorageIo>(
        &self,
        index: &ActivatedExactIndex<J>,
        chunk_id: fastdup_format::ChunkId,
        logical_length: u64,
    ) -> Result<Vec<u8>, StoreError> {
        if let Some(bytes) = self.find_verified_chunk_with_index(index, chunk_id, logical_length)? {
            return Ok(bytes);
        }
        self.read_verified_chunk(chunk_id, logical_length)
    }

    /// Discovers published names and verifies every complete container.
    ///
    /// # Errors
    ///
    /// Returns namespace I/O, naming, format, or identity-pairing errors.
    pub fn recover_published(&self) -> Result<Vec<SealedContainer>, StoreError> {
        let mut names = self.storage.list_names()?;
        names.sort_unstable();
        let mut recovered = Vec::new();
        for name in names {
            let Some(expected_id) = parse_published_name(&name)? else {
                continue;
            };
            let bytes = self.storage.read(&name)?;
            let container = SealedContainer::decode(&bytes)?;
            let embedded_id = container.header().container_id();
            if embedded_id != expected_id {
                return Err(StoreError::PublishedIdentityMismatch {
                    name: expected_id,
                    header: embedded_id,
                });
            }
            recovered.push(container);
        }
        Ok(recovered)
    }

    /// Discovers the greatest published Container generation using only each
    /// immutable object's fixed Header/Footer envelope.
    ///
    /// This is allocator recovery, not whole-container integrity proof. Both
    /// blocks are checksummed and must agree with the physical length and
    /// canonical filename. Skipping above a structurally valid generation is
    /// safe; promoting Chunk Locations still requires complete verification.
    ///
    /// # Errors
    ///
    /// Returns namespace I/O, naming, envelope-integrity, length, or identity
    /// errors without silently skipping a claimed published Container.
    pub fn discover_container_generation_high_water(&self) -> Result<Option<u64>, StoreError> {
        let mut names = self.storage.list_names()?;
        names.sort_unstable();
        let mut high_water: Option<u64> = None;
        for name in names {
            let Some(expected_id) = parse_published_name(&name)? else {
                continue;
            };
            let actual_length = self.storage.object_len(&name)?;
            let minimum_length = u64::try_from(HEADER_BYTES)
                .map_err(|_| FormatError::ArithmeticOverflow)?
                .checked_add(FOOTER_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?;
            if actual_length < minimum_length || actual_length > MAX_CONTAINER_BYTES {
                return Err(StoreError::Format(FormatError::InvalidContainerLength(
                    usize::try_from(actual_length).unwrap_or(usize::MAX),
                )));
            }
            let footer_offset = actual_length
                .checked_sub(FOOTER_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?;
            let header = self.storage.read_exact_at(&name, 0, HEADER_BYTES)?;
            let footer = self.storage.read_exact_at(
                &name,
                footer_offset,
                usize::try_from(FOOTER_BYTES).map_err(|_| FormatError::ArithmeticOverflow)?,
            )?;
            let descriptor = SealedContainerDescriptor::decode(&header, &footer, actual_length)?;
            let embedded_id = descriptor.container_id();
            if embedded_id != expected_id {
                return Err(StoreError::PublishedIdentityMismatch {
                    name: expected_id,
                    header: embedded_id,
                });
            }
            high_water = Some(
                high_water.map_or(descriptor.container_generation(), |previous| {
                    previous.max(descriptor.container_generation())
                }),
            );
        }
        Ok(high_water)
    }

    /// Fully verifies published objects one at a time and retains no payloads.
    ///
    /// # Errors
    ///
    /// Returns namespace I/O, naming, format, or identity-pairing errors.
    pub fn verify_published(&self) -> Result<Vec<PublishedContainerSummary>, StoreError> {
        let mut names = self.storage.list_names()?;
        names.sort_unstable();
        let mut verified = Vec::new();
        for name in names {
            let Some(expected_id) = parse_published_name(&name)? else {
                continue;
            };
            let bytes = self.storage.read(&name)?;
            let container = SealedContainer::decode(&bytes)?;
            let header = container.header();
            let embedded_id = header.container_id();
            if embedded_id != expected_id {
                return Err(StoreError::PublishedIdentityMismatch {
                    name: expected_id,
                    header: embedded_id,
                });
            }
            verified.push(PublishedContainerSummary {
                container_id: embedded_id,
                container_generation: header.container_generation(),
                chunk_count: container.chunk_count(),
                raw_record_count: container.raw_record_count(),
                zstd_record_count: container.zstd_record_count(),
                file_length: header.layout().file_length,
            });
        }
        Ok(verified)
    }

    pub(crate) fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError> {
        if required.is_empty() {
            return Ok(());
        }
        let mut missing = required.clone();
        let mut names = self.storage.list_names()?;
        names.sort_unstable();
        for name in names {
            let Some(expected_id) = parse_published_name(&name)? else {
                continue;
            };
            let bytes = self.storage.read(&name)?;
            let container = SealedContainer::decode(&bytes)?;
            let embedded_id = container.header().container_id();
            if embedded_id != expected_id {
                return Err(StoreError::PublishedIdentityMismatch {
                    name: expected_id,
                    header: embedded_id,
                });
            }
            for record in container.records() {
                let chunk_id = record.chunk_id();
                let Some(required_length) = missing.get(&chunk_id).copied() else {
                    continue;
                };
                if u64::try_from(record.payload().len()) == Ok(required_length) {
                    missing.remove(&chunk_id);
                }
            }
            if missing.is_empty() {
                return Ok(());
            }
        }
        let Some((&chunk_id, &logical_length)) = missing.first_key_value() else {
            unreachable!("ASSERT: nonempty missing map must have a first key")
        };
        Err(StoreError::MissingVerifiedChunk {
            chunk_id,
            logical_length,
        })
    }

    #[must_use]
    pub const fn storage(&self) -> &I {
        &self.storage
    }

    #[must_use]
    pub fn into_storage(self) -> I {
        self.storage
    }
}

#[derive(Clone, Debug)]
pub struct FsStorageIo {
    root: PathBuf,
}

impl FsStorageIo {
    /// Creates a filesystem adapter rooted at one container directory.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the root cannot be initialized.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        std::fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, name: &str) -> io::Result<PathBuf> {
        if name.is_empty() || name.contains('/') || name == "." || name == ".." {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "container name is not one path component",
            ));
        }
        Ok(self.root.join(name))
    }
}

impl StorageIo for FsStorageIo {
    fn create_new(&self, name: &str) -> io::Result<()> {
        OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(self.path(name)?)?;
        Ok(())
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        self.path(name)?.try_exists()
    }

    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path(name)?)?
            .write_all_at(bytes, offset)
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let mut file = File::open(self.path(name)?)?;
        let declared_length = file.metadata()?.len();
        if declared_length > MAX_CONTAINER_BYTES {
            return Err(container_too_large(declared_length));
        }
        let capacity =
            usize::try_from(declared_length).map_err(|_| container_too_large(declared_length))?;
        let mut bytes = Vec::with_capacity(capacity);
        file.by_ref()
            .take(MAX_CONTAINER_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).map_or(true, |length| length > MAX_CONTAINER_BYTES) {
            return Err(container_too_large(declared_length));
        }
        Ok(bytes)
    }

    fn object_len(&self, name: &str) -> io::Result<u64> {
        Ok(File::open(self.path(name)?)?.metadata()?.len())
    }

    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        if length > MAX_STORAGE_RANGE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bounded storage read exceeds the hard allocation limit",
            ));
        }
        let length_u64 = u64::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "read length is too large"))?;
        let end = offset
            .checked_add(length_u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read range overflows"))?;
        let file = File::open(self.path(name)?)?;
        if end > file.metadata()?.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "bounded storage read exceeds the current object length",
            ));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        bytes.resize(length, 0);
        file.read_exact_at(&mut bytes, offset)?;
        Ok(bytes)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        std::fs::read_dir(&self.root)?
            .map(|entry| {
                entry?.file_name().into_string().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "container directory contains a non-UTF-8 name",
                    )
                })
            })
            .collect()
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        OpenOptions::new()
            .write(true)
            .open(self.path(name)?)?
            .set_len(length)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        File::open(self.path(name)?)?.sync_all()
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        self.path(temporary_name)?;
        self.path(published_name)?;
        let directory = File::open(&self.root)?;
        rustix::fs::renameat_with(
            &directory,
            temporary_name,
            &directory,
            published_name,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(io::Error::from)
    }

    fn sync_root(&self) -> io::Result<()> {
        File::open(&self.root)?.sync_all()
    }
}

fn temporary_name(container_id: ContainerId) -> String {
    format!(".{}.building", encode_id(container_id))
}

fn published_name(container_id: ContainerId) -> String {
    format!("{}.fdc", encode_id(container_id))
}

fn parse_published_name(name: &str) -> Result<Option<ContainerId>, StoreError> {
    let Some(encoded) = name.strip_suffix(".fdc") else {
        return Ok(None);
    };
    if encoded.len() != 32 {
        return Err(StoreError::InvalidPublishedName(name.to_owned()));
    }
    let mut bytes = [0_u8; 16];
    for (output, pair) in bytes.iter_mut().zip(encoded.as_bytes().chunks_exact(2)) {
        let (Some(high), Some(low)) = (decode_hex_nibble(pair[0]), decode_hex_nibble(pair[1]))
        else {
            return Err(StoreError::InvalidPublishedName(name.to_owned()));
        };
        *output = (high << 4) | low;
    }
    ContainerId::new(bytes)
        .map(Some)
        .map_err(|_| StoreError::InvalidPublishedName(name.to_owned()))
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn process_cpu_time() -> rustix::time::Timespec {
    rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime)
}

fn process_cpu_elapsed(started: rustix::time::Timespec) -> Duration {
    Duration::try_from(process_cpu_time() - started)
        .expect("ASSERT: monotonic process CPU time must form a nonnegative Duration")
}

#[derive(Debug)]
pub enum StoreError {
    Io(io::Error),
    Format(FormatError),
    PublishVerificationMismatch,
    InvalidPublishedName(String),
    PublishedIdentityMismatch {
        name: ContainerId,
        header: ContainerId,
    },
    MissingVerifiedChunk {
        chunk_id: fastdup_format::ChunkId,
        logical_length: u64,
    },
    ExactLocationMismatch,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "container I/O failed: {error}"),
            Self::Format(error) => write!(formatter, "container verification failed: {error}"),
            Self::PublishVerificationMismatch => formatter.write_str(
                "writer reread returned valid bytes other than the intended sealed container",
            ),
            Self::InvalidPublishedName(name) => {
                write!(formatter, "invalid published container name {name:?}")
            }
            Self::PublishedIdentityMismatch { name, header } => write!(
                formatter,
                "published name ID {name:?} disagrees with header ID {header:?}"
            ),
            Self::MissingVerifiedChunk {
                chunk_id,
                logical_length,
            } => write!(
                formatter,
                "no verified container location for Chunk ID {chunk_id:?} with length {logical_length}"
            ),
            Self::ExactLocationMismatch => formatter.write_str(
                "Exact Index candidate does not pair with its sealed Container envelope and verified record",
            ),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Format(error) => Some(error),
            Self::PublishVerificationMismatch
            | Self::InvalidPublishedName(_)
            | Self::PublishedIdentityMismatch { .. }
            | Self::MissingVerifiedChunk { .. }
            | Self::ExactLocationMismatch => None,
        }
    }
}

impl From<io::Error> for StoreError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<FormatError> for StoreError {
    fn from(error: FormatError) -> Self {
        Self::Format(error)
    }
}

fn encode_id(container_id: ContainerId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = container_id.bytes();
    let mut encoded = String::with_capacity(32);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn container_too_large(length: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("container length {length} exceeds {MAX_CONTAINER_BYTES} bytes"),
    )
}

fn map_exact_location_error(error: FormatError) -> StoreError {
    if error == FormatError::ExactLocationMismatch {
        StoreError::ExactLocationMismatch
    } else {
        StoreError::Format(error)
    }
}
