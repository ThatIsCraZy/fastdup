use crate::manifest_tree::{
    ManifestRangeExtent, ManifestTreeError, ManifestTreeSummary,
    allocated_bytes_in_manifest_tree_range_decoded, read_manifest_tree_range_decoded,
};
use crate::{
    ActivatedExactIndex, ContainerRepository, ExactIndexEntry, ExactIndexGenerationPin,
    ExactIndexRunRepository, StorageIo, StoreError, VerifiedReadCache,
    generation::MetadataRootPin,
    read_cache::{VerifiedChunkPayload, VerifiedChunkRead},
};
use bytes::Bytes;
use fastdup_format::{
    ChunkId, MAX_METADATA_OBJECT_BYTES, ManifestExtent, ManifestLeaf, MetadataObjectId,
};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

pub const MAX_MANIFEST_READ_BYTES: u32 = 1_024 * 1_024;

/// A verified immutable file recipe backed by durable RAW/Zstd containers.
///
/// Construction verifies every DATA dependency as one batch. Demand reads
/// verify either the complete Container slow path or a paired sealed envelope
/// plus the complete selected Record and Chunk; HOLE and FILL extents require
/// no physical data location.
#[derive(Clone, Debug)]
pub struct VerifiedManifestFile<I> {
    recipe: Arc<dyn ManifestRecipe>,
    containers: ContainerRepository<I>,
    indexed_reader: Option<Arc<dyn VerifiedChunkReader>>,
    read_cache: Option<Arc<VerifiedReadCache>>,
    locations: Arc<[ExactIndexEntry]>,
}

trait ManifestRecipe: fmt::Debug + Send + Sync {
    fn root(&self) -> Option<MetadataObjectId>;
    fn logical_size(&self) -> u64;
    fn allocated_bytes(&self) -> u64;
    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, ManifestReadError>;
    fn read_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<ManifestRangeExtent>, ManifestReadError>;
}

#[derive(Debug)]
struct FlatManifestRecipe {
    manifest: ManifestLeaf,
    allocated_bytes: u64,
    // Rebuildable process-local positions, shared by every view of this recipe.
    // Empty and single-extent recipes need no additional allocation.
    extent_ends: Vec<u64>,
}

impl FlatManifestRecipe {
    fn new(manifest: ManifestLeaf) -> Result<Self, ManifestReadError> {
        let mut extent_ends = Vec::new();
        let indexed = manifest.extents().len() > 1;
        if indexed {
            extent_ends
                .try_reserve_exact(manifest.extents().len())
                .map_err(|_| ManifestReadError::OutOfMemory)?;
        }
        let mut end = 0_u64;
        let mut allocated_bytes = 0_u64;
        for extent in manifest.extents() {
            let length = extent_logical_length(extent);
            end = end
                .checked_add(length)
                .ok_or(ManifestReadError::ArithmeticOverflow)?;
            if indexed {
                extent_ends.push(end);
            }
            if !matches!(extent, ManifestExtent::Hole { .. }) {
                allocated_bytes = allocated_bytes
                    .checked_add(length)
                    .ok_or(ManifestReadError::ArithmeticOverflow)?;
            }
        }
        assert_eq!(
            end,
            manifest.file_length(),
            "ASSERT: validated flat recipe partitions EOF"
        );
        Ok(Self {
            manifest,
            allocated_bytes,
            extent_ends,
        })
    }

    fn intersecting(&self, offset: u64, length: u64) -> (&[ManifestExtent], u64) {
        let end = offset
            .saturating_add(length)
            .min(self.manifest.file_length());
        if offset >= end {
            return (&[], 0);
        }
        if self.extent_ends.is_empty() {
            return (self.manifest.extents(), 0);
        }
        let first = self
            .extent_ends
            .partition_point(|&position| position <= offset);
        let last = self.extent_ends.partition_point(|&position| position < end) + 1;
        let start = first
            .checked_sub(1)
            .map_or(0, |ordinal| self.extent_ends[ordinal]);
        (&self.manifest.extents()[first..last], start)
    }
}

impl ManifestRecipe for FlatManifestRecipe {
    fn root(&self) -> Option<MetadataObjectId> {
        None
    }
    fn logical_size(&self) -> u64 {
        self.manifest.file_length()
    }
    fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, ManifestReadError> {
        let end = offset.saturating_add(length).min(self.logical_size());
        let (extents, mut start) = self.intersecting(offset, length);
        extents.iter().try_fold(0_u64, |total, extent| {
            let extent_end = start
                .checked_add(extent_logical_length(extent))
                .ok_or(ManifestReadError::ArithmeticOverflow)?;
            let overlap = extent_end.min(end).saturating_sub(start.max(offset));
            start = extent_end;
            if matches!(extent, ManifestExtent::Hole { .. }) {
                Ok(total)
            } else {
                total
                    .checked_add(overlap)
                    .ok_or(ManifestReadError::ArithmeticOverflow)
            }
        })
    }

    fn read_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<ManifestRangeExtent>, ManifestReadError> {
        let (extents, mut start) = self.intersecting(offset, length);
        let mut located = Vec::new();
        located
            .try_reserve_exact(extents.len())
            .map_err(|_| ManifestReadError::OutOfMemory)?;
        for extent in extents {
            located.push(ManifestRangeExtent::new(start, extent.clone()));
            start = start
                .checked_add(extent_logical_length(extent))
                .ok_or(ManifestReadError::ArithmeticOverflow)?;
        }
        Ok(located)
    }
}

struct TreeManifestRecipe<M> {
    summary: ManifestTreeSummary,
    metadata: M,
    _root_pin: MetadataRootPin,
    cache: Arc<crate::manifest_cache::ManifestNodeCache>,
}

impl<M> fmt::Debug for TreeManifestRecipe<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TreeManifestRecipe")
            .field("root", &self.summary.root())
            .field("logical_size", &self.summary.logical_size())
            .field("allocated_bytes", &self.summary.allocated_bytes())
            .finish_non_exhaustive()
    }
}

impl<M> ManifestRecipe for TreeManifestRecipe<M>
where
    M: Send + Sync + StorageIo,
{
    fn root(&self) -> Option<MetadataObjectId> {
        Some(self.summary.root())
    }

    fn logical_size(&self) -> u64 {
        self.summary.logical_size()
    }

    fn allocated_bytes(&self) -> u64 {
        self.summary.allocated_bytes()
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, ManifestReadError> {
        allocated_bytes_in_manifest_tree_range_decoded(
            self.summary.root(),
            self.summary.logical_size(),
            offset,
            length,
            |object_id| self.cache.read(object_id, || read_tree_metadata(&self.metadata, object_id)),
        )
        .map_err(Into::into)
    }

    fn read_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<ManifestRangeExtent>, ManifestReadError> {
        read_manifest_tree_range_decoded(
            self.summary.root(),
            self.summary.logical_size(),
            offset,
            length,
            |object_id| self.cache.read(object_id, || read_tree_metadata(&self.metadata, object_id)),
        )
        .map_err(Into::into)
    }
}

trait VerifiedChunkReader: fmt::Debug + Send + Sync {
    fn read_verified_chunk(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
        cache: Option<&VerifiedReadCache>,
    ) -> Result<VerifiedChunkRead, StoreError>;

    fn read_locations(
        &self,
        locations: &[ExactIndexEntry],
        requests: &[(ChunkId, u64)],
        cache: Option<&VerifiedReadCache>,
    ) -> Result<VerifiedChunkRead, StoreError>;

    fn read_verified_chunks(
        &self,
        requests: &[(ChunkId, u64)],
        cache: Option<&VerifiedReadCache>,
    ) -> Result<VerifiedChunkRead, StoreError> {
        read_chunks_scalar(requests, |chunk_id, logical_length| {
            self.read_verified_chunk(chunk_id, logical_length, cache)
        })
    }
}

struct ActiveIndexChunkReader<I, J> {
    containers: ContainerRepository<I>,
    index: Box<dyn Fn() -> Option<ExactIndexGenerationPin<J>> + Send + Sync>,
}

impl<I, J> fmt::Debug for ActiveIndexChunkReader<I, J> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveIndexChunkReader")
            .finish_non_exhaustive()
    }
}

impl<I, J> VerifiedChunkReader for ActiveIndexChunkReader<I, J>
where
    I: Send + Sync + StorageIo,
    J: Send + Sync + StorageIo,
{
    fn read_verified_chunk(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
        cache: Option<&VerifiedReadCache>,
    ) -> Result<VerifiedChunkRead, StoreError> {
        let Some(index) = (self.index)() else {
            return self
                .containers
                .read_verified_chunk_payload(chunk_id, logical_length);
        };
        self.containers
            .read_verified_chunk_payload_cached(&index, chunk_id, logical_length, cache)
    }

    fn read_locations(
        &self,
        locations: &[ExactIndexEntry],
        requests: &[(ChunkId, u64)],
        cache: Option<&VerifiedReadCache>,
    ) -> Result<VerifiedChunkRead, StoreError> {
        let pin = (self.index)();
        self.containers.read_verified_chunks_with_locations(
            pin.as_deref(),
            locations,
            requests,
            cache,
        )
    }

    fn read_verified_chunks(
        &self,
        requests: &[(ChunkId, u64)],
        cache: Option<&VerifiedReadCache>,
    ) -> Result<VerifiedChunkRead, StoreError> {
        let Some(index) = (self.index)() else {
            return read_chunks_scalar(requests, |chunk_id, logical_length| {
                self.containers
                    .read_verified_chunk_payload(chunk_id, logical_length)
            });
        };
        self.containers
            .read_verified_chunks_with_index(&index, requests, cache)
    }
}

impl<I: StorageIo> VerifiedManifestFile<I> {
    /// Verifies every DATA dependency and constructs a non-materialized file.
    ///
    /// # Errors
    ///
    /// Returns a container error or a conflicting logical length for one Chunk
    /// ID. No unverified dependency is retained on failure.
    pub fn new(
        manifest: ManifestLeaf,
        containers: ContainerRepository<I>,
    ) -> Result<Self, ManifestReadError> {
        let mut required = BTreeMap::<ChunkId, u64>::new();
        for extent in manifest.extents() {
            let (chunk_id, logical_length) = match *extent {
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                } => (chunk_id, logical_length),
                ManifestExtent::DataSlice {
                    chunk_id,
                    chunk_length,
                    ..
                } => (chunk_id, u64::from(chunk_length)),
                ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => continue,
            };
            if let Some(previous) = required.insert(chunk_id, logical_length)
                && previous != logical_length
            {
                return Err(ManifestReadError::ChunkLengthConflict {
                    chunk_id,
                    first_length: previous,
                    second_length: logical_length,
                });
            }
        }
        containers.verify_required_chunks(&required)?;
        Ok(Self {
            recipe: Arc::new(FlatManifestRecipe::new(manifest)?),
            containers,
            indexed_reader: None,
            read_cache: None,
            locations: Arc::from([]),
        })
    }

    /// Builds a live read recipe from published writer-carried Locations.
    /// Construction is metadata-only; every read independently verifies DATA
    /// and resolves dependent Bases through the configured reader policy.
    /// Locations may precede Exact Index activation and remain mere candidates.
    ///
    /// # Errors
    /// Rejects an invalid or oversized flat recipe.
    pub fn from_published_locations(
        entries: &[ExactIndexEntry],
        containers: ContainerRepository<I>,
    ) -> Result<Self, ManifestReadError> {
        let length = entries
            .iter()
            .try_fold(0_u64, |sum, entry| {
                sum.checked_add(u64::from(entry.logical_length()))
            })
            .ok_or(ManifestReadError::ArithmeticOverflow)?;
        let extents = entries
            .iter()
            .map(|entry| ManifestExtent::Data {
                logical_length: u64::from(entry.logical_length()),
                chunk_id: entry.chunk_id(),
            })
            .collect();
        let manifest = ManifestLeaf::new(length, extents).map_err(ManifestTreeError::from)?;
        let mut locations = entries.to_vec();
        locations.sort_unstable_by_key(ExactIndexEntry::chunk_id);
        locations.dedup_by_key(|entry| entry.chunk_id());
        Ok(Self {
            recipe: Arc::new(FlatManifestRecipe::new(manifest)?),
            containers,
            indexed_reader: None,
            read_cache: None,
            locations: locations.into(),
        })
    }

    pub(crate) fn from_verified_tree<M>(
        summary: ManifestTreeSummary,
        metadata: M,
        containers: ContainerRepository<I>,
        root_pin: MetadataRootPin,
        cache: Arc<crate::manifest_cache::ManifestNodeCache>,
    ) -> Self
    where
        M: Send + Sync + StorageIo + 'static,
    {
        Self {
            recipe: Arc::new(TreeManifestRecipe {
                summary,
                metadata,
                _root_pin: root_pin,
                cache,
            }),
            containers,
            indexed_reader: None,
            read_cache: None,
            locations: Arc::from([]),
        }
    }

    /// Binds one already recovered Exact Index generation behind this Manifest
    /// reader. Each ordinary demand read takes a bounded operation pin and
    /// retains the verified Container scan as its correctness fallback after
    /// retirement closes admission.
    ///
    /// A dormant or cached Manifest reader owns only an uncounted generation
    /// snapshot, so it cannot delay GC pin-drain or extend the lifetime of DATA
    /// objects.
    #[must_use]
    pub fn with_active_index<J>(mut self, index: &ExactIndexGenerationPin<J>) -> Self
    where
        I: Clone + Send + Sync + 'static,
        J: Send + Sync + StorageIo + 'static,
    {
        let snapshot = index.snapshot();
        self.indexed_reader = Some(Arc::new(ActiveIndexChunkReader {
            containers: self.containers.clone(),
            index: Box::new(move || snapshot.try_pin()),
        }));
        self
    }

    /// Uses the repository's current Exact generation for each bounded read.
    ///
    /// Ordinary activations must not strand a long-lived file on the full
    /// Container-scan path. The repository atomically selects and pins the
    /// current generation; the pin lives only through that read. An idle file
    /// holds no operation pin and cannot delay GC retirement. Candidates still
    /// undergo complete Record/Chunk verification, and an absent or unusable
    /// index retains the verified scan fallback.
    #[must_use]
    pub fn with_index_repository<J>(mut self, repository: &ExactIndexRunRepository<J>) -> Self
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + Send + Sync + StorageIo + 'static,
    {
        let repository = repository.clone();
        self.indexed_reader = Some(Arc::new(ActiveIndexChunkReader {
            containers: self.containers.clone(),
            index: Box::new(move || repository.pin_active_generation()),
        }));
        self
    }

    /// Installs one shared, bounded cache behind this immutable Manifest
    /// reader. Only complete bytes returned by the verified Container path are
    /// admitted; recovery and scrub remain independent of cache state.
    #[must_use]
    pub fn with_verified_read_cache(mut self, cache: Arc<VerifiedReadCache>) -> Self {
        self.read_cache = Some(cache);
        self
    }

    #[must_use]
    pub fn manifest_root(&self) -> Option<MetadataObjectId> {
        self.recipe.root()
    }

    #[must_use]
    pub fn logical_size(&self) -> u64 {
        self.recipe.logical_size()
    }

    #[must_use]
    pub fn allocated_bytes(&self) -> u64 {
        self.recipe.allocated_bytes()
    }

    /// Counts allocated DATA/FILL bytes intersecting one logical range using
    /// only the touched Manifest-tree paths.
    ///
    /// # Errors
    ///
    /// Returns a bounded metadata I/O, identity, tree-partition, or arithmetic
    /// error without returning a partial count.
    pub fn allocated_bytes_in_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<u64, ManifestReadError> {
        if length == 0 || offset >= self.logical_size() {
            return Ok(0);
        }
        self.recipe.allocated_bytes_in_range(offset, length)
    }

    /// Returns only Manifest extents intersecting one range.
    ///
    /// This is a metadata-only export used by range-clone admission. Returned
    /// extents retain file coordinates and may extend across the requested
    /// boundaries; callers must clip them while preserving Chunk identity.
    ///
    /// # Errors
    ///
    /// Returns bounded metadata I/O, identity, partition, or arithmetic
    /// failures. No partial recipe is returned.
    pub fn manifest_extents_in_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<ManifestRangeExtent>, ManifestReadError> {
        if length == 0 || offset >= self.logical_size() {
            return Ok(Vec::new());
        }
        self.recipe.read_range(offset, length)
    }

    /// Reads one bounded byte range without materializing the complete file.
    ///
    /// # Errors
    ///
    /// Returns a range, allocation, arithmetic, or durable-container VERIFY
    /// failure. On error no partial byte sequence is returned.
    ///
    /// # Panics
    ///
    /// Panics only when a previously validated Manifest partition fails to
    /// cover the requested range, which is an impossible internal state.
    pub fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, ManifestReadError> {
        self.read_shared_at(offset, length).map(Vec::from)
    }

    /// Reads a verified range, retaining a payload owner when one DATA extent
    /// covers the reply. Mixed sparse/data ranges use bounded assembly.
    ///
    /// # Errors
    /// Returns the same integrity and range failures as `read_at`.
    pub fn read_shared_at(&self, offset: u64, length: u32) -> Result<Bytes, ManifestReadError> {
        join_manifest_segments(self.read_segments_at(offset, length)?)
    }

    /// Reads a bounded range as immutable segments suitable for vectored I/O.
    ///
    /// # Errors
    /// Returns the same integrity and range failures as `read_at`.
    pub fn read_segments_at(
        &self,
        offset: u64,
        length: u32,
    ) -> Result<Vec<Bytes>, ManifestReadError> {
        if !self.locations.is_empty() {
            let read = |requests: &[(ChunkId, u64)]| match &self.indexed_reader {
                Some(reader) => {
                    reader.read_locations(&self.locations, requests, self.read_cache.as_deref())
                }
                None => self.containers.read_verified_chunks_with_locations::<I>(
                    None,
                    &self.locations,
                    requests,
                    self.read_cache.as_deref(),
                ),
            };
            return self.read_at_using(offset, length, |id, length| read(&[(id, length)]), read);
        }
        if let Some(reader) = &self.indexed_reader {
            self.read_at_using(
                offset,
                length,
                |chunk_id, logical_length| {
                    reader.read_verified_chunk(chunk_id, logical_length, self.read_cache.as_deref())
                },
                |requests| reader.read_verified_chunks(requests, self.read_cache.as_deref()),
            )
        } else {
            self.read_at_using(
                offset,
                length,
                |chunk_id, logical_length| {
                    self.containers
                        .read_verified_chunk_payload(chunk_id, logical_length)
                },
                |requests| {
                    read_chunks_scalar(requests, |chunk_id, logical_length| {
                        self.containers
                            .read_verified_chunk_payload(chunk_id, logical_length)
                    })
                },
            )
        }
    }

    /// Reads one bounded byte range using the activated persistent Exact Index
    /// for DATA extents and the verified Container scan only as a correctness
    /// fallback. HOLE and FILL extents remain metadata-only.
    ///
    /// # Errors
    ///
    /// Returns a range, allocation, index-backed Container verification, or
    /// fallback scan failure. On error no partial byte sequence is returned.
    ///
    /// # Panics
    ///
    /// Panics only when a previously validated Manifest partition fails to
    /// cover the requested range, which is an impossible internal state.
    pub fn read_at_with_index<J: StorageIo>(
        &self,
        index: &ActivatedExactIndex<J>,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, ManifestReadError> {
        self.read_at_using(
            offset,
            length,
            |chunk_id, logical_length| {
                self.containers.read_verified_chunk_payload_cached(
                    index,
                    chunk_id,
                    logical_length,
                    self.read_cache.as_deref(),
                )
            },
            |requests| {
                self.containers.read_verified_chunks_with_index(
                    index,
                    requests,
                    self.read_cache.as_deref(),
                )
            },
        )
        .and_then(join_manifest_segments)
        .map(Vec::from)
    }

    fn read_cached<F>(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
        read_verified: F,
    ) -> Result<VerifiedChunkPayload, StoreError>
    where
        F: FnOnce() -> Result<VerifiedChunkRead, StoreError>,
    {
        let Some(cache) = &self.read_cache else {
            let (mut requested, _) = read_verified()?.into_parts();
            return requested.pop().ok_or(StoreError::MissingVerifiedChunk {
                chunk_id,
                logical_length,
            });
        };
        if let Some(bytes) = cache.get(chunk_id, logical_length) {
            return Ok(bytes);
        }
        let (mut requested, admission_groups) = read_verified()?.into_parts();
        let payload = requested.pop().ok_or(StoreError::MissingVerifiedChunk {
            chunk_id,
            logical_length,
        })?;
        assert!(
            requested.is_empty(),
            "ASSERT: one verified Chunk read returns one requested payload"
        );
        for group in admission_groups {
            cache.admit_decoded_group(group);
        }
        Ok(payload)
    }

    fn read_cached_many<F>(
        &self,
        requests: &[(ChunkId, u64)],
        read_verified: F,
    ) -> Result<Vec<VerifiedChunkPayload>, StoreError>
    where
        F: FnOnce(&[(ChunkId, u64)]) -> Result<VerifiedChunkRead, StoreError>,
    {
        let Some(cache) = &self.read_cache else {
            return Ok(read_verified(requests)?.into_parts().0);
        };
        let mut resolved = Vec::new();
        resolved
            .try_reserve_exact(requests.len())
            .map_err(|_| StoreError::from(std::io::Error::from(std::io::ErrorKind::OutOfMemory)))?;
        resolved.resize_with(requests.len(), || None);
        let mut missing = Vec::new();
        let mut missing_ordinals = Vec::new();
        missing
            .try_reserve_exact(requests.len())
            .map_err(|_| StoreError::from(std::io::Error::from(std::io::ErrorKind::OutOfMemory)))?;
        missing_ordinals
            .try_reserve_exact(requests.len())
            .map_err(|_| StoreError::from(std::io::Error::from(std::io::ErrorKind::OutOfMemory)))?;
        for (ordinal, &(chunk_id, logical_length)) in requests.iter().enumerate() {
            if let Some(bytes) = cache.get(chunk_id, logical_length) {
                resolved[ordinal] = Some(bytes);
            } else {
                missing.push((chunk_id, logical_length));
                missing_ordinals.push(ordinal);
            }
        }
        if !missing.is_empty() {
            let (payloads, admission_groups) = read_verified(&missing)?.into_parts();
            assert_eq!(
                payloads.len(),
                missing.len(),
                "ASSERT: a verified Read Plan returns one payload per request"
            );
            for group in admission_groups {
                cache.admit_decoded_group(group);
            }
            for ((ordinal, (_chunk_id, _logical_length)), payload) in
                missing_ordinals.into_iter().zip(missing).zip(payloads)
            {
                resolved[ordinal] = Some(payload);
            }
        }
        Ok(resolved
            .into_iter()
            .map(|payload| payload.expect("ASSERT: every cache request resolved or returned"))
            .collect())
    }

    fn read_at_using<F, G>(
        &self,
        offset: u64,
        length: u32,
        mut read_chunk: F,
        read_chunks: G,
    ) -> Result<Vec<Bytes>, ManifestReadError>
    where
        F: FnMut(ChunkId, u64) -> Result<VerifiedChunkRead, StoreError>,
        G: FnOnce(&[(ChunkId, u64)]) -> Result<VerifiedChunkRead, StoreError>,
    {
        if length > MAX_MANIFEST_READ_BYTES {
            return Err(ManifestReadError::RequestTooLarge(length));
        }
        if length == 0 || offset >= self.logical_size() {
            return Ok(Vec::new());
        }
        let read_end = offset
            .saturating_add(u64::from(length))
            .min(self.logical_size());
        let extents = self.recipe.read_range(offset, read_end - offset)?;
        let data_extent_count = extents
            .iter()
            .filter(|located| {
                matches!(
                    located.extent(),
                    ManifestExtent::Data { .. } | ManifestExtent::DataSlice { .. }
                )
            })
            .count();
        if data_extent_count < 2 {
            return assemble_manifest_read(
                offset,
                read_end,
                &extents,
                |chunk_id, logical_length| {
                    self.read_cached(chunk_id, logical_length, || {
                        read_chunk(chunk_id, logical_length)
                    })
                },
            );
        }

        let mut requests = Vec::new();
        requests
            .try_reserve_exact(data_extent_count)
            .map_err(|_| ManifestReadError::OutOfMemory)?;
        for located in &extents {
            match *located.extent() {
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                } => requests.push((chunk_id, logical_length)),
                ManifestExtent::DataSlice {
                    chunk_id,
                    chunk_length,
                    ..
                } => requests.push((chunk_id, u64::from(chunk_length))),
                ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => {}
            }
        }
        let payloads = self.read_cached_many(&requests, read_chunks)?;
        let mut payloads = payloads.into_iter();
        let output =
            assemble_manifest_read(offset, read_end, &extents, |chunk_id, logical_length| {
                let payload = payloads
                    .next()
                    .expect("ASSERT: every planned DATA extent has one payload");
                assert_eq!(
                    payload.chunk_id(),
                    chunk_id,
                    "ASSERT: a verified Read Plan cannot change Chunk identity"
                );
                assert_eq!(
                    u64::try_from(payload.len()),
                    Ok(logical_length),
                    "ASSERT: a verified Read Plan cannot change logical length"
                );
                Ok(payload)
            })?;
        assert!(
            payloads.next().is_none(),
            "ASSERT: a verified Read Plan cannot return extra payloads"
        );
        Ok(output)
    }
}

fn read_chunks_scalar<F>(
    requests: &[(ChunkId, u64)],
    mut read_chunk: F,
) -> Result<VerifiedChunkRead, StoreError>
where
    F: FnMut(ChunkId, u64) -> Result<VerifiedChunkRead, StoreError>,
{
    let mut payloads = Vec::new();
    let mut admission_groups = Vec::new();
    payloads
        .try_reserve_exact(requests.len())
        .map_err(|_| StoreError::from(std::io::Error::from(std::io::ErrorKind::OutOfMemory)))?;
    for &(chunk_id, logical_length) in requests {
        let (mut requested, groups) = read_chunk(chunk_id, logical_length)?.into_parts();
        let payload = requested.pop().ok_or(StoreError::MissingVerifiedChunk {
            chunk_id,
            logical_length,
        })?;
        assert!(
            requested.is_empty(),
            "ASSERT: scalar verified read returns one requested Chunk"
        );
        payloads.push(payload);
        admission_groups.extend(groups);
    }
    Ok(VerifiedChunkRead::new(payloads, admission_groups))
}

#[allow(
    clippy::too_many_lines,
    reason = "paired DATA/HOLE/FILL coverage and response-owner validation"
)]
fn assemble_manifest_read<F>(
    offset: u64,
    read_end: u64,
    extents: &[ManifestRangeExtent],
    mut read_chunk: F,
) -> Result<Vec<Bytes>, ManifestReadError>
where
    F: FnMut(ChunkId, u64) -> Result<VerifiedChunkPayload, StoreError>,
{
    let output_length =
        usize::try_from(read_end - offset).map_err(|_| ManifestReadError::ArithmeticOverflow)?;
    if let [located] = extents {
        let source = match *located.extent() {
            ManifestExtent::Data {
                logical_length,
                chunk_id,
            } => Some((chunk_id, logical_length, 0)),
            ManifestExtent::DataSlice {
                chunk_id,
                chunk_length,
                chunk_offset,
                ..
            } => Some((chunk_id, u64::from(chunk_length), u64::from(chunk_offset))),
            ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => None,
        };
        if let Some((id, length, base_offset)) = source {
            let start = offset
                .checked_sub(located.logical_offset())
                .and_then(|offset| offset.checked_add(base_offset))
                .and_then(|offset| usize::try_from(offset).ok())
                .ok_or(ManifestReadError::ArithmeticOverflow)?;
            let end = start
                .checked_add(output_length)
                .ok_or(ManifestReadError::ArithmeticOverflow)?;
            let payload = read_chunk(id, length)?;
            if end > payload.len() {
                return Err(ManifestReadError::ArithmeticOverflow);
            }
            let view = payload
                .into_read_view(start..end)
                .expect("ASSERT: resolved single-Chunk read range is within its verified payload");
            return Ok(vec![Bytes::from_owner(view)]);
        }
    }
    let mut output = ManifestReadOutput::new(output_length);
    let mut covered_until = offset;
    for located in extents {
        let extent = located.extent();
        let extent_start = located.logical_offset();
        let extent_length = extent_logical_length(extent);
        let extent_end = extent_start
            .checked_add(extent_length)
            .ok_or(ManifestReadError::ArithmeticOverflow)?;
        let copy_start = extent_start.max(offset);
        let copy_end = extent_end.min(read_end);
        assert_eq!(
            copy_start, covered_until,
            "ASSERT: validated Manifest extents must cover reads without gaps"
        );
        let target_start = usize::try_from(copy_start - offset)
            .map_err(|_| ManifestReadError::ArithmeticOverflow)?;
        let target_end = usize::try_from(copy_end - offset)
            .map_err(|_| ManifestReadError::ArithmeticOverflow)?;
        assert_eq!(
            output.len(),
            target_start,
            "Manifest output remains contiguous"
        );
        match *extent {
            ManifestExtent::Hole { .. } => output.resize(target_end, 0)?,
            ManifestExtent::Fill { value, .. } => {
                output.resize(target_end, value)?;
            }
            ManifestExtent::Data {
                logical_length,
                chunk_id,
            } => {
                let payload = read_chunk(chunk_id, logical_length)?;
                let source_start = usize::try_from(copy_start - extent_start)
                    .map_err(|_| ManifestReadError::ArithmeticOverflow)?;
                let source_end = usize::try_from(copy_end - extent_start)
                    .map_err(|_| ManifestReadError::ArithmeticOverflow)?;
                output.append_payload(&payload, source_start..source_end)?;
            }
            ManifestExtent::DataSlice {
                chunk_id,
                chunk_length,
                chunk_offset,
                ..
            } => {
                let payload = read_chunk(chunk_id, u64::from(chunk_length))?;
                let source_start =
                    usize::try_from(u64::from(chunk_offset) + (copy_start - extent_start))
                        .map_err(|_| ManifestReadError::ArithmeticOverflow)?;
                let source_end = source_start
                    .checked_add(target_end - target_start)
                    .ok_or(ManifestReadError::ArithmeticOverflow)?;
                if source_end > payload.len() {
                    return Err(ManifestReadError::ArithmeticOverflow);
                }
                output.append_payload(&payload, source_start..source_end)?;
            }
        }
        covered_until = copy_end;
    }
    assert_eq!(
        covered_until, read_end,
        "ASSERT: validated Manifest partition must cover every bounded read"
    );
    Ok(output.finish())
}

/// Joins adjacent ranges of the same verified allocation, retaining other
/// owners separately until a caller explicitly needs a contiguous buffer.
struct ManifestReadOutput {
    length: usize,
    view: Option<fastdup_format::VerifiedReadView>,
    segments: Vec<Bytes>,
}

impl ManifestReadOutput {
    fn new(_length: usize) -> Self {
        Self {
            length: 0,
            view: None,
            segments: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.length
    }

    fn flush(&mut self) {
        if let Some(view) = self.view.take() {
            self.segments.push(Bytes::from_owner(view));
        }
    }

    fn append_payload(
        &mut self,
        payload: &VerifiedChunkPayload,
        range: std::ops::Range<usize>,
    ) -> Result<(), ManifestReadError> {
        let length = range.len();
        if let Some(view) = &mut self.view
            && view.try_append(payload, range.clone())
        {
            self.length += length;
            return Ok(());
        }
        self.flush();
        self.view = Some(
            payload
                .read_view(range)
                .ok_or(ManifestReadError::ArithmeticOverflow)?,
        );
        self.length += length;
        Ok(())
    }

    fn resize(&mut self, length: usize, value: u8) -> Result<(), ManifestReadError> {
        self.flush();
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length - self.length)
            .map_err(|_| ManifestReadError::OutOfMemory)?;
        bytes.resize(length - self.length, value);
        self.segments.push(Bytes::from(bytes));
        self.length = length;
        Ok(())
    }

    fn finish(mut self) -> Vec<Bytes> {
        self.flush();
        self.segments
    }
}

fn join_manifest_segments(mut segments: Vec<Bytes>) -> Result<Bytes, ManifestReadError> {
    if segments.len() == 1 {
        return Ok(segments.pop().expect("ASSERT: one segment"));
    }
    let length = segments.iter().try_fold(0_usize, |sum, bytes| {
        sum.checked_add(bytes.len())
            .ok_or(ManifestReadError::ArithmeticOverflow)
    })?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| ManifestReadError::OutOfMemory)?;
    for bytes in segments {
        output.extend_from_slice(&bytes);
    }
    Ok(Bytes::from(output))
}

#[derive(Debug)]
pub enum ManifestReadError {
    Store(StoreError),
    Tree(ManifestTreeError),
    RequestTooLarge(u32),
    OutOfMemory,
    ArithmeticOverflow,
    ChunkLengthConflict {
        chunk_id: ChunkId,
        first_length: u64,
        second_length: u64,
    },
}

impl fmt::Display for ManifestReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ManifestReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Tree(error) => Some(error),
            Self::RequestTooLarge(_)
            | Self::OutOfMemory
            | Self::ArithmeticOverflow
            | Self::ChunkLengthConflict { .. } => None,
        }
    }
}

impl From<StoreError> for ManifestReadError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<ManifestTreeError> for ManifestReadError {
    fn from(error: ManifestTreeError) -> Self {
        Self::Tree(error)
    }
}

fn read_tree_metadata<I: StorageIo>(
    storage: &I,
    object_id: MetadataObjectId,
) -> Result<Vec<u8>, ManifestTreeError> {
    let name = metadata_name(object_id);
    let length = storage.object_len(&name)?;
    if length
        > u64::try_from(MAX_METADATA_OBJECT_BYTES).expect("ASSERT: metadata object bound fits u64")
    {
        return Err(ManifestTreeError::IdentityMismatch(object_id));
    }
    let bytes = storage.read(&name)?;
    // The cache admission path verifies the content identity, CRC and node
    // structure once before retaining the decoded immutable value.
    if u64::try_from(bytes.len()) != Ok(length) {
        return Err(ManifestTreeError::IdentityMismatch(object_id));
    }
    Ok(bytes)
}

fn metadata_name(object_id: MetadataObjectId) -> String {
    let mut name = String::with_capacity(68);
    for byte in object_id.bytes() {
        use std::fmt::Write;
        write!(&mut name, "{byte:02x}").expect("ASSERT: writing to String cannot fail");
    }
    name.push_str(".fdm");
    name
}

const fn extent_logical_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::DataSlice { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_recipe_index_matches_linear_mixed_extent_ranges() {
        for count in [0_u64, 1, 2, 32, 512] {
            let extents = (0..count)
                .map(|ordinal| {
                    let logical_length = ordinal % 31 + 1;
                    match ordinal % 4 {
                        0 => ManifestExtent::Hole { logical_length },
                        1 => ManifestExtent::Fill {
                            logical_length,
                            value: 23,
                        },
                        2 => ManifestExtent::Data {
                            logical_length,
                            chunk_id: ChunkId::of(&ordinal.to_le_bytes()),
                        },
                        _ => ManifestExtent::DataSlice {
                            logical_length,
                            chunk_id: ChunkId::of(&ordinal.to_le_bytes()),
                            chunk_length: 64,
                            chunk_offset: 3,
                        },
                    }
                })
                .collect::<Vec<_>>();
            let size = extents.iter().map(extent_logical_length).sum();
            let recipe =
                FlatManifestRecipe::new(ManifestLeaf::new(size, extents.clone()).unwrap()).unwrap();
            for offset in (0..size + 2).step_by(17).chain([size, size + 1, u64::MAX]) {
                for length in [0, 1, 31, 99, 1000, u64::MAX] {
                    let end = offset.saturating_add(length).min(size);
                    let mut start = 0;
                    let mut expected = Vec::new();
                    let mut allocated = 0;
                    for extent in &extents {
                        let next = start + extent_logical_length(extent);
                        if offset < end && start < end && next > offset {
                            expected.push(ManifestRangeExtent::new(start, extent.clone()));
                            if !matches!(extent, ManifestExtent::Hole { .. }) {
                                allocated += next.min(end) - start.max(offset);
                            }
                        }
                        start = next;
                    }
                    assert_eq!(recipe.read_range(offset, length).unwrap(), expected);
                    assert_eq!(
                        recipe.allocated_bytes_in_range(offset, length).unwrap(),
                        allocated
                    );
                }
            }
        }
    }
}
