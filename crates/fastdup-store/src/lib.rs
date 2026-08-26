#![deny(unsafe_code)]

//! Durable container lifecycle behind an injectable storage boundary.

mod container_descriptor_cache;
mod container_generation_allocator;
mod exact_activation_log;
mod exact_index_repository;
mod gc_candidate_catalog;
mod gc_candidate_mmap;
mod generation;
mod generation_log;
mod manifest_reader;
mod manifest_tree;
mod metadata_mark_catalog;
pub use manifest_tree::{ManifestRangeExtent, ManifestTreeSummary};
mod maintenance;
mod maintenance_ioprio;
mod persistent_reduction;
mod read_cache;
mod reduction;
mod reduction_codec;
mod reduction_dictionary;
mod reduction_filter;
mod reduction_prefix;
mod reduction_similarity;
mod seqcdc;
mod similarity_external_sort;
mod similarity_index_repository;
mod similarity_mmap;
pub use fastdup_format::{SimilarityIndexPartitionRef, SimilarityIndexRunFamily};
pub use similarity_external_sort::SIMILARITY_PARTITION_TARGET_REFERENCES;

pub use container_descriptor_cache::ContainerDescriptorCacheStatus;
pub use container_generation_allocator::{
    CONTAINER_GENERATION_HIGH_WATER_SLOT_0, CONTAINER_GENERATION_HIGH_WATER_SLOT_1,
    CONTAINER_GENERATION_RESERVATION_SPAN_V1, ContainerGenerationAllocator,
};
pub use exact_index_repository::{
    ActivatedExactIndex, EXACT_INDEX_RUN_PARTITION_TARGET_ENTRIES, ExactIndexGenerationDrain,
    ExactIndexGenerationPin, ExactIndexGenerationSnapshot, ExactIndexGenerationTransition,
    ExactIndexLocationAudit, ExactIndexLookup, ExactIndexPageCacheStatus, ExactIndexRunFamily,
    ExactIndexRunReader, ExactIndexRunRepository, ExactIndexStoreError, ExactRunMembershipStatus,
    MAX_ACTIVE_EXACT_INDEX_FAMILIES, MAX_ACTIVE_EXACT_INDEX_RUNS, MAX_EXACT_LOOKUP_CANDIDATES,
};
pub use gc_candidate_catalog::{
    GcCandidateCatalogRepository, GcCandidateCatalogSnapshot, GcCandidateCatalogStoreError,
    GcCandidateSelectionMode, GcCandidateShortlist, gc_candidate_row_from_publication,
};
pub use generation::{
    CommittedDataGeneration, GenerationError, GenerationLivenessDelta, GenerationRepository,
    GenerationScrubSummary, IndexedRequiredChunkVerifier, ManifestSuccessorProof,
    MetadataGcExactReason, MetadataGcMarkMode, MetadataGcMetrics, RecoveredDataGeneration,
    RecoveredGeneration, RepositoryFormatSupport, RequiredChunkVerifier, SuccessorPredecessor,
    VerifiedCommittedFile, WalTail,
};
pub use maintenance::{
    BackgroundMaintenanceJob, BackgroundMaintenanceReport, DataPoolUsage, DataPoolUsageError,
    EndToEndScrubReport, ExactIndexRebuildReport, GarbageCollectionPlan, GarbageCollectionReport,
    GcCandidateProof, MaintenanceError, MaintenanceExecutionMode, MaintenancePriority,
    MaintenanceRepository, MetadataGarbageCollectionReport, OnlineGcCycleOutcome,
    OnlineGcCycleReport, OnlineGcMetrics, OnlineGcRecoveryReport, OnlineGcRetirement,
    OnlineGcRunMode, PoolIndexRebuildReport, ReverseDependencyGeneration,
};
pub use manifest_reader::{MAX_MANIFEST_READ_BYTES, ManifestReadError, VerifiedManifestFile};
pub use manifest_tree::ManifestTreeError;
pub use persistent_reduction::{
    PersistentChunkPlan, PersistentReductionError, PersistentReductionIndex,
};
pub use read_cache::{
    MemoryPressureSnapshot, VerifiedReadCache, VerifiedReadCacheConfig, VerifiedReadCacheError,
    VerifiedReadCacheStatus, shared_cache_reserve_bytes,
};
pub use reduction::{
    ReducedObject, ReductionAuditReport, ReductionEngine, ReductionError, ReductionFeatures,
    ReductionPolicy, ReductionReport, ReductionRuntime,
};
pub use reduction_dictionary::{ReductionDictionary, ReductionDictionaryError};
pub use reduction_filter::{
    BlockedBloomHint, BloomLookupHint, HintStructureError, PerWorkerLocationHintCache,
    UnverifiedLocationHint,
};
pub use reduction_prefix::{
    BaseChunkRef, VerifiedBaseChunk, ZstdPrefixCodec, ZstdPrefixEncoding, ZstdPrefixError,
    ZstdPrefixTrial,
};
pub use seqcdc::{
    SeqCdcConfig, seqcdc_cut, seqcdc_cut_scalar, seqcdc_cut_segmented, seqcdc_cut_segmented_scalar,
};
pub use similarity_index_repository::{
    RecoveredSimilarityIndex, SIMILARITY_FINGERPRINT_PROFILE_V1,
    SIMILARITY_REPRESENTATIVE_PROFILE_V1, SimilarityBaseCandidate, SimilarityIndexAuditStatus,
    SimilarityIndexReadMode, SimilarityIndexRebuildStatus, SimilarityIndexRepository,
    SimilarityIndexStoreError, similarity_index_entry_v1,
};

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};

use fastdup_format::{
    BuildingContainerHeader, ContainerId, ContainerIntrinsicSummary, ExactIndexEntry,
    ExactIndexLocation, ExactLocationTransition, FOOTER_BYTES, FormatError, HEADER_BYTES,
    IncompressibilityGateMetrics, IncompressibilityGatePolicy, MAX_CONTAINER_BYTES,
    PrehashedAdaptiveRegion, PrehashedChunk, PrehashedContiguousRegion, SealedContainer,
    SealedContainerDescriptor, VerifiedContainerPublication,
};
use rayon::prelude::*;

/// Hard allocation bound for one exact random read through [`StorageIo`].
pub const MAX_STORAGE_RANGE_BYTES: usize = 1_024 * 1_024;
/// Bytes read from each sampled Container region during owned publication.
pub const PUBLICATION_SAMPLE_BYTES: usize = HEADER_BYTES;
const MAINTENANCE_VERIFY_WINDOW_BYTES: u64 = 256 * 1_024 * 1_024;

/// One exact range sampled from a just-written Container before publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationSampleRange {
    offset: u64,
    length: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ContainerRemovalMetrics {
    verify: Duration,
    unlink: Duration,
    sync: Duration,
}

impl ContainerRemovalMetrics {
    pub(crate) const fn verify_wall(self) -> Duration {
        self.verify
    }

    pub(crate) const fn unlink_wall(self) -> Duration {
        self.unlink
    }

    pub(crate) const fn sync_wall(self) -> Duration {
        self.sync
    }
}

impl PublicationSampleRange {
    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    #[must_use]
    pub const fn length(self) -> usize {
        self.length
    }
}

/// Returns the Header, aligned middle block, and Footer ranges sampled from an
/// owned Container publication.
///
/// # Errors
///
/// Rejects an image too short to contain three distinct format blocks.
pub fn publication_sample_ranges(
    file_length: usize,
) -> Result<[PublicationSampleRange; 3], StoreError> {
    let footer_bytes = usize::try_from(FOOTER_BYTES)
        .map_err(|_| StoreError::Format(FormatError::ArithmeticOverflow))?;
    let minimum = PUBLICATION_SAMPLE_BYTES
        .checked_mul(3)
        .ok_or(StoreError::Format(FormatError::ArithmeticOverflow))?;
    if file_length < minimum || footer_bytes != PUBLICATION_SAMPLE_BYTES {
        return Err(StoreError::Format(FormatError::InvalidContainerLength(
            file_length,
        )));
    }
    let middle = (file_length / 2 / PUBLICATION_SAMPLE_BYTES) * PUBLICATION_SAMPLE_BYTES;
    let footer = file_length
        .checked_sub(footer_bytes)
        .ok_or(StoreError::Format(FormatError::ArithmeticOverflow))?;
    if middle < PUBLICATION_SAMPLE_BYTES
        || middle
            .checked_add(PUBLICATION_SAMPLE_BYTES)
            .is_none_or(|end| end > footer)
    {
        return Err(StoreError::Format(FormatError::InvalidContainerLength(
            file_length,
        )));
    }
    Ok([
        PublicationSampleRange {
            offset: 0,
            length: PUBLICATION_SAMPLE_BYTES,
        },
        PublicationSampleRange {
            offset: u64::try_from(middle)
                .map_err(|_| StoreError::Format(FormatError::ArithmeticOverflow))?,
            length: PUBLICATION_SAMPLE_BYTES,
        },
        PublicationSampleRange {
            offset: u64::try_from(footer)
                .map_err(|_| StoreError::Format(FormatError::ArithmeticOverflow))?,
            length: footer_bytes,
        },
    ])
}

/// Compares one sampled storage range with the exact writer image.
///
/// # Errors
///
/// Returns a publication mismatch for a wrong range length or any changed byte.
pub fn verify_publication_sample(
    writer_image: &[u8],
    range: PublicationSampleRange,
    actual: &[u8],
) -> Result<(), StoreError> {
    let start = usize::try_from(range.offset)
        .map_err(|_| StoreError::Format(FormatError::ArithmeticOverflow))?;
    let end = start
        .checked_add(range.length)
        .ok_or(StoreError::Format(FormatError::ArithmeticOverflow))?;
    if actual.len() != range.length || writer_image.get(start..end) != Some(actual) {
        return Err(StoreError::PublishVerificationMismatch);
    }
    Ok(())
}

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
    incompressibility_gate: IncompressibilityGateMetrics,
}

/// Opaque encoded Container image awaiting ordered durable publication.
///
/// Construction performs the CPU-heavy adaptive region encoding and retains
/// its Location evidence. Publication samples the stored Header, midpoint
/// block, and Footer before returning that evidence.
#[derive(Clone, Debug)]
pub struct PreparedAdaptiveContainer {
    container_id: ContainerId,
    container_generation: u64,
    sealed: Vec<u8>,
    publication: VerifiedContainerPublication,
    encode_wall: Duration,
    encode_process_cpu: Duration,
    incompressibility_gate: IncompressibilityGateMetrics,
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

    #[must_use]
    pub const fn incompressibility_gate(self) -> IncompressibilityGateMetrics {
        self.incompressibility_gate
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

/// Aggregate evidence from a bounded parallel, canonically reduced audit.
///
/// No decoded payload or per-Chunk map is retained in this report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ContainerAuditSummary {
    containers: u64,
    chunks: u64,
    raw_records: u64,
    zstd_records: u64,
    file_bytes: u64,
    generation_high_water: Option<u64>,
}

impl ContainerAuditSummary {
    #[must_use]
    pub const fn containers(self) -> u64 {
        self.containers
    }

    #[must_use]
    pub const fn chunks(self) -> u64 {
        self.chunks
    }

    #[must_use]
    pub const fn raw_records(self) -> u64 {
        self.raw_records
    }

    #[must_use]
    pub const fn zstd_records(self) -> u64 {
        self.zstd_records
    }

    #[must_use]
    pub const fn file_bytes(self) -> u64 {
        self.file_bytes
    }

    #[must_use]
    pub const fn generation_high_water(self) -> Option<u64> {
        self.generation_high_water
    }
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

/// One complete immutable Container image transferred into its publication
/// adapter exactly once.
///
/// The adapter owns every buffer until publication either fails or the root
/// directory sync makes the no-replace name durable. Implementations must
/// preserve the Building -> Body -> Sealed -> sampled VERIFY -> file sync ->
/// rename -> root sync order encoded by [`StorageIo::publish_owned_container`].
#[derive(Debug)]
pub struct OwnedContainerPublication {
    container_id: ContainerId,
    container_generation: u64,
    building_header: Box<[u8; HEADER_BYTES]>,
    sealed: Vec<u8>,
    publication: VerifiedContainerPublication,
    temporary_name: String,
    published_name: String,
}

impl OwnedContainerPublication {
    #[must_use]
    pub const fn container_id(&self) -> ContainerId {
        self.container_id
    }

    #[must_use]
    pub const fn container_generation(&self) -> u64 {
        self.container_generation
    }

    #[must_use]
    pub fn sealed_len(&self) -> usize {
        self.sealed.len()
    }

    #[must_use]
    pub fn temporary_name(&self) -> &str {
        &self.temporary_name
    }

    #[must_use]
    pub fn published_name(&self) -> &str {
        &self.published_name
    }

    /// Consumes the capability into the exact writer inputs.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ContainerId,
        u64,
        Box<[u8; HEADER_BYTES]>,
        Vec<u8>,
        VerifiedContainerPublication,
        String,
        String,
    ) {
        (
            self.container_id,
            self.container_generation,
            self.building_header,
            self.sealed,
            self.publication,
            self.temporary_name,
            self.published_name,
        )
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
    /// Visits current publication-directory names without requiring callers to
    /// retain a pool-sized name collection. The default preserves compatibility
    /// for adapters that only implement [`Self::list_names`].
    ///
    /// # Errors
    ///
    /// Returns the backend's directory-read or name-decoding error.
    fn visit_names(&self, visitor: &mut dyn FnMut(&str)) -> io::Result<()> {
        for name in self.list_names()? {
            visitor(&name);
        }
        Ok(())
    }
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
    /// Removes exactly one canonical internal name.
    ///
    /// The removal is not crash-durable until a following [`Self::sync_root`].
    /// Callers must establish replacement/liveness safety before invoking this
    /// operation; the storage adapter does not interpret object reachability.
    ///
    /// # Errors
    ///
    /// Returns the backend's lookup or namespace-mutation error.
    fn remove_file(&self, name: &str) -> io::Result<()>;
    /// Makes namespace publication stable.
    ///
    /// # Errors
    ///
    /// Returns the backend's directory durability error.
    fn sync_root(&self) -> io::Result<()>;

    /// Pins one immutable object for zero-copy reads when the adapter can
    /// prevent in-process writes, truncation, replacement, and removal for the
    /// lifetime of the returned lease.
    ///
    /// Adapters without that guarantee return `None`; callers retain their
    /// bounded [`Self::read_exact_at`] path.
    ///
    /// # Errors
    ///
    /// Returns lookup, metadata, length, or lease-acquisition errors.
    fn lease_immutable_file(
        &self,
        _name: &str,
        _expected_length: u64,
    ) -> io::Result<Option<ImmutableFileLease>> {
        Ok(None)
    }

    /// Consumes one complete Container image and runs the ordered publication
    /// protocol. Adapters may keep many owned publications in flight, but may
    /// return only after the captured root-sync cohort is durable.
    ///
    /// # Errors
    ///
    /// Returns the first format, I/O, verification, or durability failure.
    fn publish_owned_container(
        &self,
        publication: OwnedContainerPublication,
    ) -> Result<VerifiedContainerPublication, StoreError> {
        publish_owned_container_synchronously(self, publication)
    }
}

type ImmutableLeaseCounts = BTreeMap<String, usize>;
type SharedImmutableLeaseCounts = Arc<Mutex<ImmutableLeaseCounts>>;

/// An opaque read-only file capability whose lifetime prevents cooperating
/// [`FsStorageIo`] adapters from mutating the same published name.
pub struct ImmutableFileLease {
    file: File,
    name: String,
    counts: SharedImmutableLeaseCounts,
}

impl ImmutableFileLease {
    pub(crate) const fn file(&self) -> &File {
        &self.file
    }
}

impl fmt::Debug for ImmutableFileLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ImmutableFileLease")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Drop for ImmutableFileLease {
    fn drop(&mut self) {
        let Ok(mut counts) = self.counts.lock() else {
            return;
        };
        let remove = match counts.get_mut(&self.name) {
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            Some(_) => true,
            None => {
                debug_assert!(
                    false,
                    "ASSERT: immutable file lease count exists until drop"
                );
                false
            }
        };
        if remove {
            counts.remove(&self.name);
        }
    }
}

#[derive(Clone, Debug)]
pub struct ContainerRepository<I> {
    storage: I,
    descriptors: Arc<container_descriptor_cache::ContainerDescriptorCache>,
    retiring: Arc<RwLock<BTreeMap<[u8; 16], usize>>>,
    generation_allocator_barrier: Arc<Mutex<()>>,
    generation_allocator_registry:
        Arc<container_generation_allocator::ContainerGenerationAllocatorRegistry>,
}

struct RetiringSelectionBarrier {
    retiring: Arc<RwLock<BTreeMap<[u8; 16], usize>>>,
    containers: Vec<[u8; 16]>,
    committed: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RecoveredRetiringRemoval {
    containers_removed: u64,
    containers_already_absent: u64,
    bytes_removed: u64,
}

impl RetiringSelectionBarrier {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for RetiringSelectionBarrier {
    fn drop(&mut self) {
        if self.committed || self.containers.is_empty() {
            return;
        }
        let mut retiring = self
            .retiring
            .write()
            .expect("ASSERT: retiring Container rollback lock poisoned");
        for container_id in &self.containers {
            let remove = match retiring.get_mut(container_id) {
                Some(count) if *count > 1 => {
                    *count -= 1;
                    false
                }
                Some(_) => true,
                None => panic!(
                    "ASSERT: an uncommitted selection barrier retains every prepared Container"
                ),
            };
            if remove {
                retiring.remove(container_id);
            }
        }
    }
}

impl<I: StorageIo> ContainerRepository<I> {
    #[must_use]
    pub fn new(storage: I) -> Self {
        Self {
            storage,
            descriptors: Arc::new(
                container_descriptor_cache::ContainerDescriptorCache::new_system(),
            ),
            retiring: Arc::new(RwLock::new(BTreeMap::new())),
            generation_allocator_barrier: Arc::new(Mutex::new(())),
            generation_allocator_registry: Arc::new(
                container_generation_allocator::ContainerGenerationAllocatorRegistry::default(),
            ),
        }
    }

    /// Opens the durable Container-generation allocator.
    ///
    /// A legacy repository performs one paired-envelope migration scan. Once
    /// the high-water slots exist, healthy reopen reads only those fixed
    /// records and skips every previously reserved generation after a crash.
    ///
    /// # Errors
    ///
    /// Returns invalid span, storage, format, chain, or generation-exhaustion
    /// errors. No generation is returned before its containing range is
    /// durable.
    pub fn open_generation_allocator(
        &self,
        reservation_span: u64,
    ) -> Result<ContainerGenerationAllocator<I>, StoreError>
    where
        I: Clone,
    {
        ContainerGenerationAllocator::open(self.clone(), reservation_span)
    }

    /// Audits the durable allocator against completely verified Containers.
    ///
    /// An absent pair is an accepted legacy repository. Once either canonical
    /// slot exists, both slots and their selected hash chain must validate and
    /// the durable reservation must cover the greatest observed generation.
    ///
    /// # Errors
    ///
    /// Returns storage, record, chain, or insufficient-high-water errors.
    pub fn audit_generation_high_water(
        &self,
        observed_generation: Option<u64>,
    ) -> Result<Option<u64>, StoreError> {
        let _guard = self
            .generation_allocator_barrier
            .lock()
            .map_err(|_| io::Error::other("Container generation allocator barrier is poisoned"))?;
        container_generation_allocator::audit_generation_high_water(
            &self.storage,
            observed_generation,
        )
    }

    /// Creates a maintenance I/O view over the same process-local Container
    /// lifecycle state.
    ///
    /// The supplied adapter must address the same DATA publication directory.
    /// This seam lets online maintenance use an independently scheduled I/O
    /// path while sharing RETIRING barriers and descriptor-cache state with
    /// frontend repositories. No routing branch or maintenance observation is
    /// added to ordinary Container operations.
    #[must_use]
    pub fn with_maintenance_storage<J: StorageIo>(&self, storage: J) -> ContainerRepository<J> {
        ContainerRepository {
            storage,
            descriptors: Arc::clone(&self.descriptors),
            retiring: Arc::clone(&self.retiring),
            generation_allocator_barrier: Arc::clone(&self.generation_allocator_barrier),
            generation_allocator_registry: Arc::clone(&self.generation_allocator_registry),
        }
    }

    /// Constructs a repository with deterministic descriptor-cache pressure.
    ///
    /// This is intended for tests and runtimes with an external memory
    /// governor. Normal appliances use [`Self::new`] and refresh host/cgroup
    /// pressure automatically.
    #[must_use]
    pub fn new_with_descriptor_cache_snapshot(
        storage: I,
        snapshot: MemoryPressureSnapshot,
    ) -> Self {
        Self {
            storage,
            descriptors: Arc::new(
                container_descriptor_cache::ContainerDescriptorCache::new_with_snapshot(snapshot),
            ),
            retiring: Arc::new(RwLock::new(BTreeMap::new())),
            generation_allocator_barrier: Arc::new(Mutex::new(())),
            generation_allocator_registry: Arc::new(
                container_generation_allocator::ContainerGenerationAllocatorRegistry::default(),
            ),
        }
    }

    /// Returns bounded process-local Container-envelope cache telemetry.
    #[must_use]
    pub fn descriptor_cache_status(&self) -> ContainerDescriptorCacheStatus {
        self.descriptors.status()
    }

    /// Excludes RETIRING Containers from directory-scan selection fallbacks.
    ///
    /// Exact-generation pins may still read a physical Location selected
    /// before the retirement barrier. Recovery and Scrub continue to inspect
    /// every published Container regardless of this process-local set.
    ///
    /// # Panics
    ///
    /// Panics if the process-local selection-barrier lock is poisoned.
    pub fn install_retiring_selection_barrier(&self, containers: &BTreeMap<[u8; 16], ContainerId>) {
        let mut retiring = self
            .retiring
            .write()
            .expect("ASSERT: retiring Container selection lock poisoned");
        for container_id in containers.keys().copied() {
            retiring.entry(container_id).or_insert(1);
        }
    }

    fn prepare_retiring_selection_barrier(
        &self,
        containers: &BTreeMap<[u8; 16], ContainerId>,
    ) -> Result<RetiringSelectionBarrier, crate::maintenance::MaintenanceError> {
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(containers.len())
            .map_err(|_| crate::maintenance::MaintenanceError::OutOfMemory)?;
        let mut retiring = self
            .retiring
            .write()
            .expect("ASSERT: retiring Container selection lock poisoned");
        for container_id in containers.keys().copied() {
            let count = retiring.entry(container_id).or_insert(0);
            *count = count
                .checked_add(1)
                .expect("ASSERT: retiring selection-barrier references cannot overflow");
            prepared.push(container_id);
        }
        drop(retiring);
        Ok(RetiringSelectionBarrier {
            retiring: Arc::clone(&self.retiring),
            containers: prepared,
            committed: false,
        })
    }

    fn remove_retiring_selection_barrier(&self, containers: &BTreeMap<[u8; 16], ContainerId>) {
        let mut retiring = self
            .retiring
            .write()
            .expect("ASSERT: retiring Container removal lock poisoned");
        for container_id in containers.keys() {
            retiring.remove(container_id);
        }
    }

    fn selectable_container(&self, container_id: ContainerId) -> bool {
        !self
            .retiring
            .read()
            .expect("ASSERT: retiring Container selection lock poisoned")
            .contains_key(&container_id.bytes())
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
        let encoded = SealedContainer::encode_with_writer_evidence(
            container_id,
            container_generation,
            chunks,
        )?;
        let (sealed, publication) = encoded.into_publication_parts();
        self.publish_sealed(container_id, container_generation, sealed, publication)
            .map(drop)
    }

    /// Publishes codec-3 targets whose named Base Chunks are already durable.
    ///
    /// This writer does not publish or discover Bases. The caller must obtain
    /// each Base through independent verified storage and keep its Exact Index
    /// Location live before making the dependent Container reachable.
    ///
    /// # Errors
    ///
    /// Returns the first Prefix format, backend I/O, sampling, or durability
    /// error. Existing published IDs are never replaced.
    pub fn publish_zstd_prefix_pairs_verified(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        pairs: &[(&[u8], &[u8])],
    ) -> Result<VerifiedContainerPublication, StoreError> {
        let encoded =
            SealedContainer::encode_zstd_prefix_pairs(container_id, container_generation, pairs)?;
        let (sealed, publication) = encoded.into_publication_parts();
        self.publish_sealed(container_id, container_generation, sealed, publication)
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
        let encoded = SealedContainer::encode_adaptive_regions_parallel_profiled(
            container_id,
            container_generation,
            regions,
            NonZeroUsize::MIN,
        )?;
        let (sealed, publication) = encoded.into_publication_parts();
        self.publish_sealed(container_id, container_generation, sealed, publication)
            .map(drop)
    }

    /// Publishes adaptive RAW/Zstd regions and returns writer-produced Location
    /// evidence paired with the mandatory storage samples.
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
    ) -> Result<VerifiedContainerPublication, StoreError> {
        self.publish_adaptive_regions_parallel_verified(
            container_id,
            container_generation,
            regions,
            NonZeroUsize::MIN,
        )
    }

    /// Publishes adaptive regions encoded by the bounded permanent worker pool
    /// and returns writer-produced Location evidence after storage sampling.
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
    ) -> Result<VerifiedContainerPublication, StoreError> {
        self.publish_adaptive_regions_parallel_profiled(
            container_id,
            container_generation,
            regions,
            workers,
        )
        .map(|(verified, _metrics)| verified)
    }

    /// Encodes and durably publishes one GC replacement Container, resuming
    /// the same deterministic temporary name after an interrupted offline
    /// maintenance attempt.
    ///
    /// This seam is deliberately crate-private: ordinary ingest publication
    /// remains no-replace. Offline GC alone owns the deterministic replacement
    /// identity and may overwrite its non-authoritative `.building` object.
    pub(crate) fn publish_gc_replacement_adaptive_verified(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
    ) -> Result<SealedContainer, StoreError> {
        let encoded = SealedContainer::encode_adaptive_regions_parallel_profiled_with_gate(
            container_id,
            container_generation,
            regions,
            workers,
            IncompressibilityGatePolicy::Off,
        )?;
        let sealed = encoded.into_bytes();
        self.publish_sealed_resumable(container_id, container_generation, &sealed)
    }

    /// Publishes adaptive regions while measuring encoding separately from
    /// ordered durable publication and storage sampling.
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
    ) -> Result<
        (
            VerifiedContainerPublication,
            AdaptiveContainerPublishMetrics,
        ),
        StoreError,
    > {
        let prepared = Self::prepare_adaptive_regions_parallel(
            container_id,
            container_generation,
            regions,
            workers,
        )?;
        self.publish_prepared_adaptive_profiled(prepared)
    }

    /// Encodes adaptive Compression Regions without performing storage I/O.
    ///
    /// This split lets a caller retire scarce CPU-worker permits before the
    /// prepared immutable image waits on data-tier durability.
    ///
    /// # Errors
    ///
    /// Returns format, compression, allocation, or worker failures.
    pub fn prepare_adaptive_regions_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
    ) -> Result<PreparedAdaptiveContainer, StoreError> {
        let encode_wall_started = Instant::now();
        let encode_cpu_started = process_cpu_time();
        let encoded = SealedContainer::encode_adaptive_regions_parallel_profiled_with_gate(
            container_id,
            container_generation,
            regions,
            workers,
            IncompressibilityGatePolicy::Off,
        )?;
        let encode_wall = encode_wall_started.elapsed();
        let encode_process_cpu = process_cpu_elapsed(encode_cpu_started);
        let incompressibility_gate = encoded.metrics();
        let (sealed, publication) = encoded.into_publication_parts();
        Ok(PreparedAdaptiveContainer {
            container_id,
            container_generation,
            sealed,
            publication,
            encode_wall,
            encode_process_cpu,
            incompressibility_gate,
        })
    }

    /// Encodes adaptive Compression Regions while preserving Chunk identities
    /// already computed by the ingest stage.
    ///
    /// Publication trusts these identities as prior writer work and samples
    /// stored bytes before the Container name can become visible. Ordinary
    /// reads, recovery, and scrub independently recompute them.
    ///
    /// # Errors
    ///
    /// Returns format, compression, allocation, or worker failures.
    pub fn prepare_prehashed_adaptive_regions_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        workers: NonZeroUsize,
    ) -> Result<PreparedAdaptiveContainer, StoreError> {
        let encode_wall_started = Instant::now();
        let encode_cpu_started = process_cpu_time();
        let encoded =
            SealedContainer::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
                container_id,
                container_generation,
                regions,
                workers,
                IncompressibilityGatePolicy::Off,
            )?;
        let encode_wall = encode_wall_started.elapsed();
        let encode_process_cpu = process_cpu_elapsed(encode_cpu_started);
        let incompressibility_gate = encoded.metrics();
        let (sealed, publication) = encoded.into_publication_parts();
        Ok(PreparedAdaptiveContainer {
            container_id,
            container_generation,
            sealed,
            publication,
            encode_wall,
            encode_process_cpu,
            incompressibility_gate,
        })
    }

    /// Encodes prehashed regions that already own one contiguous decoded
    /// buffer, avoiding another full input materialization before compression.
    ///
    /// # Errors
    ///
    /// Returns format, compression, allocation, or worker failures.
    pub fn prepare_prehashed_contiguous_regions_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedContiguousRegion<'_>],
        workers: NonZeroUsize,
    ) -> Result<PreparedAdaptiveContainer, StoreError> {
        let encode_wall_started = Instant::now();
        let encode_cpu_started = process_cpu_time();
        let encoded =
            SealedContainer::encode_prehashed_contiguous_regions_parallel_profiled_with_gate(
                container_id,
                container_generation,
                regions,
                workers,
                IncompressibilityGatePolicy::Off,
            )?;
        let encode_wall = encode_wall_started.elapsed();
        let encode_process_cpu = process_cpu_elapsed(encode_cpu_started);
        let incompressibility_gate = encoded.metrics();
        let (sealed, publication) = encoded.into_publication_parts();
        Ok(PreparedAdaptiveContainer {
            container_id,
            container_generation,
            sealed,
            publication,
            encode_wall,
            encode_process_cpu,
            incompressibility_gate,
        })
    }

    /// Encodes an ordered mixture of borrowed and already-materialized
    /// prehashed regions.
    ///
    /// # Errors
    ///
    /// Returns format, compression, allocation, or worker failures.
    pub fn prepare_mixed_prehashed_adaptive_regions_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedAdaptiveRegion<'_>],
        workers: NonZeroUsize,
    ) -> Result<PreparedAdaptiveContainer, StoreError> {
        let encode_wall_started = Instant::now();
        let encode_cpu_started = process_cpu_time();
        let encoded =
            SealedContainer::encode_mixed_prehashed_adaptive_regions_parallel_profiled_with_gate(
                container_id,
                container_generation,
                regions,
                workers,
                IncompressibilityGatePolicy::Off,
            )?;
        let encode_wall = encode_wall_started.elapsed();
        let encode_process_cpu = process_cpu_elapsed(encode_cpu_started);
        let incompressibility_gate = encoded.metrics();
        let (sealed, publication) = encoded.into_publication_parts();
        Ok(PreparedAdaptiveContainer {
            container_id,
            container_generation,
            sealed,
            publication,
            encode_wall,
            encode_process_cpu,
            incompressibility_gate,
        })
    }

    /// Encodes ordinary adaptive regions and already-trialled independent or
    /// Prefix records into one immutable Container image.
    ///
    /// Each prepared record is consumed. No candidate target is compressed a
    /// second time after the v1 cost decision.
    ///
    /// # Errors
    ///
    /// Returns bounded format, compression, allocation, or worker failures.
    pub fn prepare_mixed_prehashed_reduction_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedAdaptiveRegion<'_>],
        independent: Vec<fastdup_format::PreparedIndependentRecord>,
        prefixes: Vec<fastdup_format::PreparedZstdPrefixRecord>,
        workers: NonZeroUsize,
    ) -> Result<PreparedAdaptiveContainer, StoreError> {
        let encode_wall_started = Instant::now();
        let encode_cpu_started = process_cpu_time();
        let encoded =
            SealedContainer::encode_mixed_prehashed_reduction_parallel_profiled_with_gate(
                container_id,
                container_generation,
                regions,
                independent,
                prefixes,
                workers,
                IncompressibilityGatePolicy::Off,
            )?;
        let encode_wall = encode_wall_started.elapsed();
        let encode_process_cpu = process_cpu_elapsed(encode_cpu_started);
        let incompressibility_gate = encoded.metrics();
        let (sealed, publication) = encoded.into_publication_parts();
        Ok(PreparedAdaptiveContainer {
            container_id,
            container_generation,
            sealed,
            publication,
            encode_wall,
            encode_process_cpu,
            incompressibility_gate,
        })
    }

    /// Durably publishes one prepared adaptive Container and returns complete
    /// writer-image evidence plus storage-sampling phase metrics.
    ///
    /// # Errors
    ///
    /// Returns publication I/O, durability, sampling, or integrity failures.
    ///
    /// # Panics
    ///
    /// Panics only if a validated format-v1 image violates its bounded length
    /// or decoded-byte accounting invariants.
    pub fn publish_prepared_adaptive_profiled(
        &self,
        prepared: PreparedAdaptiveContainer,
    ) -> Result<
        (
            VerifiedContainerPublication,
            AdaptiveContainerPublishMetrics,
        ),
        StoreError,
    > {
        let PreparedAdaptiveContainer {
            container_id,
            container_generation,
            sealed,
            publication,
            encode_wall,
            encode_process_cpu,
            incompressibility_gate,
        } = prepared;
        let file_bytes = u64::try_from(sealed.len())
            .expect("ASSERT: a format-v1 Container image length fits u64");
        let publish_wall_started = Instant::now();
        let publish_cpu_started = process_cpu_time();
        let verified =
            self.publish_sealed(container_id, container_generation, sealed, publication)?;
        let publish_wall = publish_wall_started.elapsed();
        let publish_process_cpu = process_cpu_elapsed(publish_cpu_started);
        let logical_bytes = verified.logical_bytes();
        let metrics = AdaptiveContainerPublishMetrics {
            encode_wall,
            encode_process_cpu,
            publish_wall,
            publish_process_cpu,
            file_bytes,
            logical_bytes,
            raw_records: verified.raw_record_count(),
            zstd_records: verified.zstd_record_count(),
            incompressibility_gate,
        };
        Ok((verified, metrics))
    }

    fn publish_sealed(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        sealed: Vec<u8>,
        publication: VerifiedContainerPublication,
    ) -> Result<VerifiedContainerPublication, StoreError> {
        let building = BuildingContainerHeader::new(container_id, container_generation)?.encode();
        let temporary_name = temporary_name(container_id);
        let published_name = published_name(container_id);
        let sealed_length = u64::try_from(sealed.len())
            .expect("ASSERT: a format-v1 container length always fits u64");
        assert!(
            sealed_length <= MAX_CONTAINER_BYTES,
            "ASSERT: the format writer returned an oversized container"
        );

        self.storage
            .publish_owned_container(OwnedContainerPublication {
                container_id,
                container_generation,
                building_header: Box::new(building),
                sealed,
                publication,
                temporary_name,
                published_name,
            })
    }

    fn publish_sealed_resumable(
        &self,
        container_id: ContainerId,
        container_generation: u64,
        sealed: &[u8],
    ) -> Result<SealedContainer, StoreError> {
        let temporary_name = temporary_name(container_id);
        let published_name = published_name(container_id);
        if self.storage.exists(&published_name)? {
            let existing = self.storage.read(&published_name)?;
            if existing != sealed {
                return Err(StoreError::PublishVerificationMismatch);
            }
            let verified = SealedContainer::decode(&existing)?;
            if verified.header().container_id() != container_id
                || verified.header().container_generation() != container_generation
            {
                return Err(StoreError::PublishVerificationMismatch);
            }
            return Ok(verified);
        }
        if !self.storage.exists(&temporary_name)? {
            self.storage.create_new(&temporary_name)?;
        }
        let building = BuildingContainerHeader::new(container_id, container_generation)?.encode();
        let sealed_length = u64::try_from(sealed.len())
            .expect("ASSERT: a format-v1 Container image length fits u64");
        assert!(
            sealed_length <= MAX_CONTAINER_BYTES,
            "ASSERT: resumable GC publication cannot exceed the format-v1 Container limit"
        );
        self.storage.write_at(&temporary_name, 0, &building)?;
        self.storage.write_at(
            &temporary_name,
            u64::try_from(HEADER_BYTES).expect("ASSERT: format Header size fits u64"),
            &sealed[HEADER_BYTES..],
        )?;
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
        self.decode_published_bytes(container_id, &bytes)
    }

    fn decode_published_bytes(
        &self,
        expected_id: ContainerId,
        bytes: &[u8],
    ) -> Result<SealedContainer, StoreError> {
        let mut resolve = |dependency: fastdup_format::ZstdPrefixDependency| {
            self.read_verified_independent_chunk(dependency.chunk_id(), dependency.logical_length())
                .map_err(|_| FormatError::ZstdPrefixBaseRequired)
        };
        let container = SealedContainer::decode_with_zstd_prefix_resolver(bytes, &mut resolve)?;
        let embedded_id = container.header().container_id();
        if embedded_id != expected_id {
            return Err(StoreError::PublishedIdentityMismatch {
                name: expected_id,
                header: embedded_id,
            });
        }
        Ok(container)
    }

    fn read_verified_independent_chunk(
        &self,
        chunk_id: fastdup_format::ChunkId,
        logical_length: u32,
    ) -> Result<Vec<u8>, StoreError> {
        let mut names = self.storage.list_names()?;
        names.sort_unstable();
        for name in names {
            let Some(expected_id) = parse_published_name(&name)? else {
                continue;
            };
            if !self.selectable_container(expected_id) {
                continue;
            }
            let bytes = self.storage.read(&name)?;
            let (embedded_id, found) =
                SealedContainer::find_verified_independent_chunk(&bytes, chunk_id, logical_length)?;
            if embedded_id != expected_id {
                return Err(StoreError::PublishedIdentityMismatch {
                    name: expected_id,
                    header: embedded_id,
                });
            }
            if let Some(bytes) = found {
                return Ok(bytes);
            }
        }
        Err(StoreError::MissingVerifiedChunk {
            chunk_id,
            logical_length: u64::from(logical_length),
        })
    }

    /// Reads one Container while resolving codec-3 Bases through the active
    /// persistent Exact Index.
    ///
    /// Base lookup accepts only dependency-free RAW or Zstd Locations. This
    /// enforces Depth 1 even if the index also contains dependent copies of the
    /// same logical Base Chunk.
    ///
    /// # Errors
    ///
    /// Returns Container, index, Base-resolution, or identity failures.
    pub fn read_with_index<J: StorageIo>(
        &self,
        container_id: ContainerId,
        index: &ActivatedExactIndex<J>,
    ) -> Result<SealedContainer, StoreError> {
        let name = published_name(container_id);
        let bytes = self.storage.read(&name)?;
        let mut resolve = |dependency: fastdup_format::ZstdPrefixDependency| {
            self.find_verified_independent_base_with_index(
                index,
                dependency.chunk_id(),
                dependency.logical_length(),
            )
            .ok_or(FormatError::ZstdPrefixBaseRequired)
        };
        let container = SealedContainer::decode_with_zstd_prefix_resolver(&bytes, &mut resolve)?;
        if container.header().container_id() != container_id {
            return Err(StoreError::PublishedIdentityMismatch {
                name: container_id,
                header: container.header().container_id(),
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
            if !self.selectable_container(expected_id) {
                continue;
            }
            let bytes = self.storage.read(&name)?;
            let container = self.decode_published_bytes(expected_id, &bytes)?;
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
        let (descriptor, encoded_record) = self.read_candidate_record(candidate)?;
        let record = descriptor
            .decode_candidate(candidate, &encoded_record)
            .map_err(map_exact_location_error)?;
        Ok(record.payload().to_vec())
    }

    /// Resolves one codec-3 target after its named Base was independently
    /// decoded and verified by the caller.
    ///
    /// The target record and Exact coordinates are paired again here. A wrong
    /// Base identity, a stale target Location, or any reconstruction mismatch
    /// returns no bytes.
    ///
    /// # Errors
    ///
    /// Returns Container I/O/integrity failures or
    /// [`StoreError::ExactLocationMismatch`] for any unpaired dependency or
    /// physical coordinate.
    pub fn read_verified_zstd_prefix_location(
        &self,
        candidate: ExactIndexEntry,
        verified_base: &[u8],
    ) -> Result<Vec<u8>, StoreError> {
        let (descriptor, encoded_record) = self.read_candidate_record(candidate)?;
        let record = descriptor
            .decode_zstd_prefix_candidate(candidate, &encoded_record, verified_base)
            .map_err(map_exact_location_error)?;
        Ok(record.payload().to_vec())
    }

    fn read_candidate_record(
        &self,
        candidate: ExactIndexEntry,
    ) -> Result<(SealedContainerDescriptor, Vec<u8>), StoreError> {
        if candidate.transition() != ExactLocationTransition::Active {
            return Err(StoreError::ExactLocationMismatch);
        }
        let location = candidate.location();
        let name = published_name(location.container_id());
        let descriptor = if let Some(descriptor) = self.descriptors.get(location.container_id()) {
            descriptor
        } else {
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
            self.descriptors.insert(location.container_id(), descriptor);
            descriptor
        };
        let range = descriptor
            .record_range(candidate)
            .map_err(map_exact_location_error)?;
        let encoded_record = self
            .storage
            .read_exact_at(&name, range.offset(), range.length())?;
        Ok((descriptor, encoded_record))
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
            let verified = if location.dependency_id() == [0; 32] {
                self.read_verified_location(candidate)
            } else {
                let base_id = fastdup_format::ChunkId::from_bytes(location.dependency_id());
                let Some(base) =
                    self.find_verified_independent_base_with_index(index, base_id, index_length)
                else {
                    continue;
                };
                self.read_verified_zstd_prefix_location(candidate, &base)
            };
            if let Ok(bytes) = verified {
                return Some((candidate, bytes));
            }
        }
        None
    }

    pub(crate) fn find_verified_independent_base_with_index<J: StorageIo>(
        &self,
        index: &ActivatedExactIndex<J>,
        chunk_id: fastdup_format::ChunkId,
        logical_length: u32,
    ) -> Option<Vec<u8>> {
        let lookup = index.lookup_transitions(chunk_id, logical_length).ok()?;
        let mut seen_locations: [Option<ExactIndexLocation>; MAX_EXACT_LOOKUP_CANDIDATES] =
            [None; MAX_EXACT_LOOKUP_CANDIDATES];
        let mut seen_count = 0_usize;
        let mut attempted = 0_u8;
        for candidate in lookup.candidates().iter().copied() {
            assert_eq!(
                candidate.chunk_id(),
                chunk_id,
                "ASSERT: Base Exact lookup returned a different Chunk ID"
            );
            assert_eq!(
                candidate.logical_length(),
                logical_length,
                "ASSERT: Base Exact lookup returned a different logical length"
            );
            let location = candidate.location();
            if seen_locations[..seen_count].contains(&Some(location)) {
                continue;
            }
            assert!(
                seen_count < seen_locations.len(),
                "ASSERT: bounded Base lookup returned more candidates than its hard limit"
            );
            seen_locations[seen_count] = Some(location);
            seen_count += 1;
            if candidate.transition() != ExactLocationTransition::Active
                || location.dependency_id() != [0; 32]
            {
                continue;
            }
            if attempted == 2 {
                break;
            }
            attempted += 1;
            if let Ok(bytes) = self.read_verified_location(candidate) {
                return Some(bytes);
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
            let container = self.decode_published_bytes(expected_id, &bytes)?;
            recovered.push(container);
        }
        Ok(recovered)
    }

    /// Discovers and fully verifies all published Containers with bounded
    /// Depth-1 Base resolution through an activated Exact Index.
    ///
    /// # Errors
    ///
    /// Returns namespace, naming, Container, Exact-Index, Base, or identity
    /// failures. A dependent Base Location is never accepted as another Base.
    pub fn recover_published_with_index<J: StorageIo>(
        &self,
        index: &ActivatedExactIndex<J>,
    ) -> Result<Vec<SealedContainer>, StoreError> {
        let mut names = self.storage.list_names()?;
        names.sort_unstable();
        let mut recovered = Vec::new();
        for name in names {
            let Some(expected_id) = parse_published_name(&name)? else {
                continue;
            };
            recovered.push(self.read_with_index(expected_id, index)?);
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

    pub(crate) fn published_container_count(&self) -> Result<u64, StoreError> {
        let mut count = 0_u64;
        for name in self.storage.list_names()? {
            if parse_published_name(&name)?.is_some() {
                count = count.checked_add(1).ok_or_else(audit_counter_overflow)?;
            }
        }
        Ok(count)
    }

    pub(crate) fn visit_published_intrinsic_summaries<E, F>(&self, mut visitor: F) -> Result<(), E>
    where
        E: From<StoreError>,
        F: FnMut(ContainerId, u64, u64, ContainerIntrinsicSummary) -> Result<(), E>,
    {
        let mut names = self.storage.list_names().map_err(StoreError::from)?;
        names.sort_unstable();
        for name in names {
            let Some(expected_id) = parse_published_name(&name).map_err(E::from)? else {
                continue;
            };
            let actual_length = self.storage.object_len(&name).map_err(StoreError::from)?;
            let minimum_length = u64::try_from(HEADER_BYTES)
                .map_err(|_| StoreError::Format(FormatError::ArithmeticOverflow))?
                .checked_add(FOOTER_BYTES)
                .ok_or(StoreError::Format(FormatError::ArithmeticOverflow))?;
            if actual_length < minimum_length || actual_length > MAX_CONTAINER_BYTES {
                return Err(E::from(StoreError::Format(
                    FormatError::InvalidContainerLength(
                        usize::try_from(actual_length).unwrap_or(usize::MAX),
                    ),
                )));
            }
            let footer_offset = actual_length
                .checked_sub(FOOTER_BYTES)
                .ok_or(StoreError::Format(FormatError::ArithmeticOverflow))?;
            let header = self
                .storage
                .read_exact_at(&name, 0, HEADER_BYTES)
                .map_err(StoreError::from)?;
            let footer = self
                .storage
                .read_exact_at(
                    &name,
                    footer_offset,
                    usize::try_from(FOOTER_BYTES)
                        .map_err(|_| StoreError::Format(FormatError::ArithmeticOverflow))?,
                )
                .map_err(StoreError::from)?;
            let descriptor = SealedContainerDescriptor::decode(&header, &footer, actual_length)
                .map_err(StoreError::from)?;
            if descriptor.container_id() != expected_id {
                return Err(E::from(StoreError::PublishedIdentityMismatch {
                    name: expected_id,
                    header: descriptor.container_id(),
                }));
            }
            let summary = SealedContainerDescriptor::decode_intrinsic_summary(
                &header,
                &footer,
                actual_length,
            )
            .map_err(StoreError::from)?;
            visitor(
                expected_id,
                descriptor.container_generation(),
                actual_length,
                summary,
            )?;
        }
        Ok(())
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
            let container = self.decode_published_bytes(expected_id, &bytes)?;
            let header = container.header();
            let embedded_id = header.container_id();
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

    /// Sequentially audits every published Container and returns aggregate
    /// evidence without retaining decoded payload or a per-Chunk map.
    ///
    /// # Errors
    ///
    /// Returns the first namespace, naming, format, identity, or counter
    /// overflow failure.
    pub fn audit_published(&self) -> Result<ContainerAuditSummary, StoreError>
    where
        I: Sync,
    {
        self.visit_verified_published_pipelined::<StoreError, _>(|_| Ok(()))
    }

    pub(crate) fn remove_verified_published(
        &self,
        container_ids: &BTreeMap<[u8; 16], ContainerId>,
    ) -> Result<(u64, ContainerRemovalMetrics), StoreError> {
        let verify_started = Instant::now();
        let mut removed_bytes = 0_u64;
        let mut verified_removals = Vec::new();
        verified_removals
            .try_reserve_exact(container_ids.len())
            .map_err(|_| StoreError::Io(io::Error::from(io::ErrorKind::OutOfMemory)))?;
        for container_id in container_ids.values().copied() {
            let name = published_name(container_id);
            let bytes = self.storage.read(&name)?;
            let container = self.decode_published_bytes(container_id, &bytes)?;
            removed_bytes = removed_bytes
                .checked_add(container.header().layout().file_length)
                .ok_or_else(audit_counter_overflow)?;
            verified_removals.push(name);
        }
        let verify_wall = verify_started.elapsed();
        let unlink_started = Instant::now();
        for name in verified_removals {
            self.storage.remove_file(&name)?;
        }
        let unlink_wall = unlink_started.elapsed();
        let sync_started = Instant::now();
        if !container_ids.is_empty() {
            self.storage.sync_root()?;
        }
        let sync_wall = sync_started.elapsed();
        Ok((
            removed_bytes,
            ContainerRemovalMetrics {
                verify: verify_wall,
                unlink: unlink_wall,
                sync: sync_wall,
            },
        ))
    }

    /// Idempotently removes Containers named by effective durable RETIRING
    /// entries after process restart.
    ///
    /// Every still-present Container is fully verified and must reproduce the
    /// complete RETIRING Location set before unlink. An absent canonical name
    /// is accepted because an earlier attempt may have synchronized its
    /// deletion before crashing ahead of the REMOVED Exact transition.
    pub(crate) fn remove_recovered_retiring(
        &self,
        retiring_entries: &[ExactIndexEntry],
    ) -> Result<RecoveredRetiringRemoval, StoreError> {
        let mut victims = BTreeMap::<[u8; 16], (ContainerId, Vec<ExactIndexEntry>)>::new();
        for entry in retiring_entries.iter().copied() {
            if entry.transition() != ExactLocationTransition::Retiring {
                return Err(StoreError::ExactLocationMismatch);
            }
            let container_id = entry.location().container_id();
            let (_, entries) = victims
                .entry(container_id.bytes())
                .or_insert_with(|| (container_id, Vec::new()));
            entries
                .try_reserve(1)
                .map_err(|_| StoreError::Io(io::Error::from(io::ErrorKind::OutOfMemory)))?;
            entries.push(entry);
        }

        let mut report = RecoveredRetiringRemoval::default();
        let mut verified_removals = Vec::new();
        verified_removals
            .try_reserve_exact(victims.len())
            .map_err(|_| StoreError::Io(io::Error::from(io::ErrorKind::OutOfMemory)))?;
        for (container_id, mut expected) in victims.into_values() {
            let name = published_name(container_id);
            if !self.storage.exists(&name)? {
                report.containers_already_absent = report
                    .containers_already_absent
                    .checked_add(1)
                    .ok_or_else(audit_counter_overflow)?;
                continue;
            }
            let bytes = self.storage.read(&name)?;
            let container = self.decode_published_bytes(container_id, &bytes)?;
            let mut observed = Vec::new();
            observed
                .try_reserve_exact(container.locations().len())
                .map_err(|_| StoreError::Io(io::Error::from(io::ErrorKind::OutOfMemory)))?;
            for location in container.locations().iter().copied() {
                let active = ExactIndexEntry::from_verified(location)
                    .map_err(|_| StoreError::ExactLocationMismatch)?;
                observed.push(
                    ExactIndexEntry::retiring(active)
                        .map_err(|_| StoreError::ExactLocationMismatch)?,
                );
            }
            expected.sort_unstable_by(exact_entry_location_order);
            observed.sort_unstable_by(exact_entry_location_order);
            if observed != expected {
                return Err(StoreError::ExactLocationMismatch);
            }
            verified_removals.push((name, container.header().layout().file_length));
        }
        for (name, file_length) in verified_removals {
            self.storage.remove_file(&name)?;
            report.bytes_removed = report
                .bytes_removed
                .checked_add(file_length)
                .ok_or_else(audit_counter_overflow)?;
            report.containers_removed = report
                .containers_removed
                .checked_add(1)
                .ok_or_else(audit_counter_overflow)?;
        }
        if report.containers_removed != 0 {
            self.storage.sync_root()?;
        }
        Ok(report)
    }

    pub(crate) fn visit_verified_published_pipelined<E, F>(
        &self,
        mut visitor: F,
    ) -> Result<ContainerAuditSummary, E>
    where
        I: Sync,
        E: From<StoreError>,
        F: FnMut(&SealedContainer) -> Result<(), E>,
    {
        let mut names = self
            .storage
            .list_names()
            .map_err(|error| E::from(StoreError::from(error)))?;
        names.sort_unstable();
        let worker_limit = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let mut summary = ContainerAuditSummary::default();
        let mut cursor = 0_usize;
        while cursor < names.len() {
            let mut encoded = Vec::new();
            let mut encoded_bytes = 0_u64;
            while cursor < names.len() && encoded.len() < worker_limit {
                let name = &names[cursor];
                let Some(expected_id) = parse_published_name(name).map_err(E::from)? else {
                    cursor += 1;
                    continue;
                };
                let length = self
                    .storage
                    .object_len(name)
                    .map_err(|error| E::from(StoreError::from(error)))?;
                if !encoded.is_empty()
                    && encoded_bytes
                        .checked_add(length)
                        .is_none_or(|total| total > MAINTENANCE_VERIFY_WINDOW_BYTES)
                {
                    break;
                }
                let bytes = self
                    .storage
                    .read(name)
                    .map_err(|error| E::from(StoreError::from(error)))?;
                encoded_bytes = encoded_bytes
                    .checked_add(
                        u64::try_from(bytes.len())
                            .map_err(|_| E::from(audit_counter_overflow()))?,
                    )
                    .ok_or_else(audit_counter_overflow)
                    .map_err(E::from)?;
                encoded.push((expected_id, bytes));
                cursor += 1;
            }
            if encoded.is_empty() {
                assert_eq!(
                    cursor,
                    names.len(),
                    "ASSERT: an empty maintenance verification window is valid only at namespace EOF"
                );
                break;
            }
            assert!(
                encoded_bytes <= MAINTENANCE_VERIFY_WINDOW_BYTES,
                "ASSERT: one format-v1 Container cannot exceed the maintenance verification window"
            );
            let decoded = encoded
                .into_par_iter()
                .map(|(expected_id, bytes)| self.decode_published_bytes(expected_id, &bytes))
                .collect::<Result<Vec<_>, StoreError>>()
                .map_err(E::from)?;
            for container in decoded {
                add_container_to_audit_summary(&mut summary, &container).map_err(E::from)?;
                visitor(&container)?;
            }
        }
        Ok(summary)
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
            if !self.selectable_container(expected_id) {
                continue;
            }
            let bytes = self.storage.read(&name)?;
            let container = self.decode_published_bytes(expected_id, &bytes)?;
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

fn publish_owned_container_synchronously<I: StorageIo + ?Sized>(
    storage: &I,
    publication: OwnedContainerPublication,
) -> Result<VerifiedContainerPublication, StoreError> {
    let (
        container_id,
        container_generation,
        building_header,
        sealed,
        verified,
        temporary_name,
        published_name,
    ) = publication.into_parts();
    let sealed_length =
        u64::try_from(sealed.len()).expect("ASSERT: a format-v1 Container image length fits u64");
    assert!(
        sealed_length <= MAX_CONTAINER_BYTES,
        "ASSERT: owned publication cannot exceed the format-v1 Container limit"
    );
    assert!(
        sealed.len() > HEADER_BYTES,
        "ASSERT: a sealed Container includes its body and Footer"
    );

    storage.create_new(&temporary_name)?;
    storage.write_at(&temporary_name, 0, &building_header[..])?;
    storage.write_at(
        &temporary_name,
        u64::try_from(HEADER_BYTES).expect("ASSERT: format Header size fits u64"),
        &sealed[HEADER_BYTES..],
    )?;
    storage.write_at(&temporary_name, 0, &sealed[..HEADER_BYTES])?;
    storage.set_len(&temporary_name, sealed_length)?;
    if storage.object_len(&temporary_name)? != sealed_length {
        return Err(StoreError::PublishVerificationMismatch);
    }
    for range in publication_sample_ranges(sealed.len())? {
        let sample = storage.read_exact_at(&temporary_name, range.offset(), range.length())?;
        verify_publication_sample(&sealed, range, &sample)?;
    }
    if verified.header().container_id() != container_id
        || verified.header().container_generation() != container_generation
    {
        return Err(StoreError::PublishVerificationMismatch);
    }
    storage.sync_file(&temporary_name)?;
    storage.publish_noreplace(&temporary_name, &published_name)?;
    storage.sync_root()?;
    Ok(verified)
}

static FS_IMMUTABLE_LEASE_REGISTRIES: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<Mutex<ImmutableLeaseCounts>>>>,
> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct FsStorageIo {
    root: PathBuf,
    immutable_leases: SharedImmutableLeaseCounts,
}

impl FsStorageIo {
    /// Creates a filesystem adapter rooted at one container directory.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the root cannot be initialized.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        std::fs::create_dir_all(root.as_ref())?;
        let root = std::fs::canonicalize(root.as_ref())?;
        let registries = FS_IMMUTABLE_LEASE_REGISTRIES.get_or_init(|| Mutex::new(BTreeMap::new()));
        let mut registries = registries
            .lock()
            .map_err(|_| io::Error::other("immutable lease registry is poisoned"))?;
        let immutable_leases = registries
            .get(&root)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let counts = Arc::new(Mutex::new(BTreeMap::new()));
                registries.insert(root.clone(), Arc::downgrade(&counts));
                counts
            });
        Ok(Self {
            root,
            immutable_leases,
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

    fn immutable_lease_guard(&self) -> io::Result<std::sync::MutexGuard<'_, ImmutableLeaseCounts>> {
        self.immutable_leases
            .lock()
            .map_err(|_| io::Error::other("immutable lease registry is poisoned"))
    }

    fn reject_leased(counts: &ImmutableLeaseCounts, name: &str) -> io::Result<()> {
        if counts.get(name).copied().unwrap_or(0) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "published object has active immutable readers",
            ));
        }
        Ok(())
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
        let leases = self.immutable_lease_guard()?;
        Self::reject_leased(&leases, name)?;
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

    fn visit_names(&self, visitor: &mut dyn FnMut(&str)) -> io::Result<()> {
        for entry in std::fs::read_dir(&self.root)? {
            let name = entry?.file_name().into_string().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "container directory contains a non-UTF-8 name",
                )
            })?;
            visitor(&name);
        }
        Ok(())
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        let leases = self.immutable_lease_guard()?;
        Self::reject_leased(&leases, name)?;
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
        let leases = self.immutable_lease_guard()?;
        Self::reject_leased(&leases, temporary_name)?;
        Self::reject_leased(&leases, published_name)?;
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

    fn remove_file(&self, name: &str) -> io::Result<()> {
        let leases = self.immutable_lease_guard()?;
        Self::reject_leased(&leases, name)?;
        std::fs::remove_file(self.path(name)?)
    }

    fn sync_root(&self) -> io::Result<()> {
        File::open(&self.root)?.sync_all()
    }

    fn lease_immutable_file(
        &self,
        name: &str,
        expected_length: u64,
    ) -> io::Result<Option<ImmutableFileLease>> {
        let path = self.path(name)?;
        let mut counts = self.immutable_lease_guard()?;
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != expected_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "immutable object identity or length changed before lease acquisition",
            ));
        }
        let count = counts.entry(name.to_owned()).or_insert(0);
        *count = count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("immutable lease count overflow"))?;
        drop(counts);
        Ok(Some(ImmutableFileLease {
            file,
            name: name.to_owned(),
            counts: Arc::clone(&self.immutable_leases),
        }))
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
    InvalidContainerGenerationReservationSpan,
    ContainerGenerationExhausted,
    ContainerGenerationHighWaterFormat(fastdup_format::ContainerGenerationHighWaterFormatError),
    ContainerGenerationHighWaterChain,
    ContainerGenerationHighWaterBehind {
        reserved_through: u64,
        observed: u64,
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "container I/O failed: {error}"),
            Self::Format(error) => write!(formatter, "container verification failed: {error}"),
            Self::PublishVerificationMismatch => formatter.write_str(
                "sampled storage bytes or publication identity differ from the intended sealed container",
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
            Self::InvalidContainerGenerationReservationSpan => {
                formatter.write_str("Container generation reservation span must be nonzero")
            }
            Self::ContainerGenerationExhausted => {
                formatter.write_str("Container generation space is exhausted")
            }
            Self::ContainerGenerationHighWaterFormat(error) => {
                write!(formatter, "Container generation high-water is invalid: {error}")
            }
            Self::ContainerGenerationHighWaterChain => formatter
                .write_str("Container generation high-water slots do not form one monotonic chain"),
            Self::ContainerGenerationHighWaterBehind {
                reserved_through,
                observed,
            } => write!(
                formatter,
                "Container generation high-water {reserved_through} is below observed generation {observed}"
            ),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Format(error) => Some(error),
            Self::ContainerGenerationHighWaterFormat(error) => Some(error),
            Self::PublishVerificationMismatch
            | Self::InvalidPublishedName(_)
            | Self::PublishedIdentityMismatch { .. }
            | Self::MissingVerifiedChunk { .. }
            | Self::ExactLocationMismatch
            | Self::InvalidContainerGenerationReservationSpan
            | Self::ContainerGenerationExhausted
            | Self::ContainerGenerationHighWaterChain
            | Self::ContainerGenerationHighWaterBehind { .. } => None,
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

fn add_container_to_audit_summary(
    summary: &mut ContainerAuditSummary,
    container: &SealedContainer,
) -> Result<(), StoreError> {
    let header = container.header();
    summary.containers = summary
        .containers
        .checked_add(1)
        .ok_or_else(audit_counter_overflow)?;
    summary.chunks = summary
        .chunks
        .checked_add(u64::try_from(container.chunk_count()).map_err(|_| audit_counter_overflow())?)
        .ok_or_else(audit_counter_overflow)?;
    summary.raw_records = summary
        .raw_records
        .checked_add(
            u64::try_from(container.raw_record_count()).map_err(|_| audit_counter_overflow())?,
        )
        .ok_or_else(audit_counter_overflow)?;
    summary.zstd_records = summary
        .zstd_records
        .checked_add(
            u64::try_from(container.zstd_record_count()).map_err(|_| audit_counter_overflow())?,
        )
        .ok_or_else(audit_counter_overflow)?;
    summary.file_bytes = summary
        .file_bytes
        .checked_add(header.layout().file_length)
        .ok_or_else(audit_counter_overflow)?;
    summary.generation_high_water = Some(
        summary
            .generation_high_water
            .map_or(header.container_generation(), |generation| {
                generation.max(header.container_generation())
            }),
    );
    Ok(())
}

fn audit_counter_overflow() -> StoreError {
    StoreError::Io(io::Error::other("Container audit counter overflow"))
}

fn exact_entry_location_order(
    left: &ExactIndexEntry,
    right: &ExactIndexEntry,
) -> std::cmp::Ordering {
    let left_location = left.location();
    let right_location = right.location();
    left.chunk_id()
        .cmp(&right.chunk_id())
        .then_with(|| left.logical_length().cmp(&right.logical_length()))
        .then_with(|| {
            left_location
                .container_id()
                .bytes()
                .cmp(&right_location.container_id().bytes())
        })
        .then_with(|| {
            left_location
                .container_generation()
                .cmp(&right_location.container_generation())
        })
        .then_with(|| {
            left_location
                .record_offset()
                .cmp(&right_location.record_offset())
        })
        .then_with(|| {
            left_location
                .chunk_ordinal()
                .cmp(&right_location.chunk_ordinal())
        })
}

fn map_exact_location_error(error: FormatError) -> StoreError {
    if error == FormatError::ExactLocationMismatch {
        StoreError::ExactLocationMismatch
    } else {
        StoreError::Format(error)
    }
}
