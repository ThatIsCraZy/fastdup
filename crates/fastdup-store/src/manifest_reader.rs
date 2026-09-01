use crate::manifest_tree::{
    ManifestRangeExtent, ManifestTreeError, ManifestTreeSummary,
    allocated_bytes_in_manifest_tree_range, read_manifest_tree_range,
};
use crate::{
    ActivatedExactIndex, ContainerRepository, ExactIndexGenerationPin,
    ExactIndexGenerationSnapshot, StorageIo, StoreError, VerifiedReadCache,
    generation::MetadataRootPin,
    read_cache::{VerifiedChunkPayload, VerifiedChunkRead},
};
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
        let mut extent_offset = 0_u64;
        self.manifest
            .extents()
            .iter()
            .try_fold(0_u64, |total, extent| {
                let extent_end = extent_offset
                    .checked_add(extent_logical_length(extent))
                    .ok_or(ManifestReadError::ArithmeticOverflow)?;
                let overlap = extent_end
                    .min(end)
                    .saturating_sub(extent_offset.max(offset));
                extent_offset = extent_end;
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
        let end = offset.saturating_add(length).min(self.logical_size());
        let mut located = Vec::new();
        let mut extent_offset = 0_u64;
        for extent in self.manifest.extents() {
            let extent_end = extent_offset
                .checked_add(extent_logical_length(extent))
                .ok_or(ManifestReadError::ArithmeticOverflow)?;
            if extent_end > offset && extent_offset < end {
                located
                    .try_reserve(1)
                    .map_err(|_| ManifestReadError::OutOfMemory)?;
                located.push(ManifestRangeExtent::new(extent_offset, extent.clone()));
            }
            extent_offset = extent_end;
        }
        Ok(located)
    }
}

struct TreeManifestRecipe<M> {
    summary: ManifestTreeSummary,
    metadata: M,
    _root_pin: MetadataRootPin,
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
        allocated_bytes_in_manifest_tree_range(
            self.summary.root(),
            self.summary.logical_size(),
            offset,
            length,
            |object_id| read_tree_metadata(&self.metadata, object_id),
        )
        .map_err(Into::into)
    }

    fn read_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<ManifestRangeExtent>, ManifestReadError> {
        read_manifest_tree_range(
            self.summary.root(),
            self.summary.logical_size(),
            offset,
            length,
            |object_id| read_tree_metadata(&self.metadata, object_id),
        )
        .map_err(Into::into)
    }
}

trait VerifiedChunkReader: fmt::Debug + Send + Sync {
    fn read_verified_chunk(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
    ) -> Result<VerifiedChunkRead, StoreError>;

    fn read_verified_chunks(
        &self,
        requests: &[(ChunkId, u64)],
    ) -> Result<VerifiedChunkRead, StoreError> {
        read_chunks_scalar(requests, |chunk_id, logical_length| {
            self.read_verified_chunk(chunk_id, logical_length)
        })
    }
}

struct ActiveIndexChunkReader<I, J> {
    containers: ContainerRepository<I>,
    index: ExactIndexGenerationSnapshot<J>,
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
    ) -> Result<VerifiedChunkRead, StoreError> {
        let Some(index) = self.index.try_pin() else {
            return self
                .containers
                .read_verified_chunk_payload(chunk_id, logical_length);
        };
        self.containers
            .read_verified_chunk_payload_with_index(&index, chunk_id, logical_length)
    }

    fn read_verified_chunks(
        &self,
        requests: &[(ChunkId, u64)],
    ) -> Result<VerifiedChunkRead, StoreError> {
        let Some(index) = self.index.try_pin() else {
            return read_chunks_scalar(requests, |chunk_id, logical_length| {
                self.containers
                    .read_verified_chunk_payload(chunk_id, logical_length)
            });
        };
        self.containers
            .read_verified_chunks_with_index(&index, requests)
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
        let allocated_bytes = manifest.extents().iter().try_fold(0_u64, |total, extent| {
            let length = if matches!(extent, ManifestExtent::Hole { .. }) {
                0
            } else {
                extent_logical_length(extent)
            };
            total.checked_add(length)
        });
        let allocated_bytes = allocated_bytes.ok_or(ManifestReadError::ArithmeticOverflow)?;
        Ok(Self {
            recipe: Arc::new(FlatManifestRecipe {
                manifest,
                allocated_bytes,
            }),
            containers,
            indexed_reader: None,
            read_cache: None,
        })
    }

    pub(crate) fn from_verified_tree<M>(
        summary: ManifestTreeSummary,
        metadata: M,
        containers: ContainerRepository<I>,
        root_pin: MetadataRootPin,
    ) -> Self
    where
        M: Send + Sync + StorageIo + 'static,
    {
        Self {
            recipe: Arc::new(TreeManifestRecipe {
                summary,
                metadata,
                _root_pin: root_pin,
            }),
            containers,
            indexed_reader: None,
            read_cache: None,
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
        self.indexed_reader = Some(Arc::new(ActiveIndexChunkReader {
            containers: self.containers.clone(),
            index: index.snapshot(),
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
        if let Some(reader) = &self.indexed_reader {
            self.read_at_using(
                offset,
                length,
                |chunk_id, logical_length| reader.read_verified_chunk(chunk_id, logical_length),
                |requests| reader.read_verified_chunks(requests),
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
                self.containers.read_verified_chunk_payload_with_index(
                    index,
                    chunk_id,
                    logical_length,
                )
            },
            |requests| {
                self.containers
                    .read_verified_chunks_with_index(index, requests)
            },
        )
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
    ) -> Result<Vec<u8>, ManifestReadError>
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

fn assemble_manifest_read<F>(
    offset: u64,
    read_end: u64,
    extents: &[ManifestRangeExtent],
    mut read_chunk: F,
) -> Result<Vec<u8>, ManifestReadError>
where
    F: FnMut(ChunkId, u64) -> Result<VerifiedChunkPayload, StoreError>,
{
    let output_length =
        usize::try_from(read_end - offset).map_err(|_| ManifestReadError::ArithmeticOverflow)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(output_length)
        .map_err(|_| ManifestReadError::OutOfMemory)?;
    output.resize(output_length, 0);
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
        match *extent {
            ManifestExtent::Hole { .. } => {}
            ManifestExtent::Fill { value, .. } => {
                output[target_start..target_end].fill(value);
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
                output[target_start..target_end]
                    .copy_from_slice(&payload.as_slice()[source_start..source_end]);
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
                output[target_start..target_end]
                    .copy_from_slice(&payload.as_slice()[source_start..source_end]);
            }
        }
        covered_until = copy_end;
    }
    assert_eq!(
        covered_until, read_end,
        "ASSERT: validated Manifest partition must cover every bounded read"
    );
    Ok(output)
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
    if u64::try_from(bytes.len()) != Ok(length)
        || MetadataObjectId::from_encoded(&bytes)? != object_id
    {
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
