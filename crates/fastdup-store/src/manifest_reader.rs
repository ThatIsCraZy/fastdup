use crate::manifest_tree::{
    ManifestRangeExtent, ManifestTreeError, ManifestTreeSummary,
    allocated_bytes_in_manifest_tree_range, read_manifest_tree_range,
};
use crate::{ActivatedExactIndex, ContainerRepository, StorageIo, StoreError, VerifiedReadCache};
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
    ) -> Result<Vec<u8>, StoreError>;
}

struct ActiveIndexChunkReader<I, J> {
    containers: ContainerRepository<I>,
    index: Arc<ActivatedExactIndex<J>>,
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
    ) -> Result<Vec<u8>, StoreError> {
        self.containers.read_verified_chunk_with_index(
            self.index.as_ref(),
            chunk_id,
            logical_length,
        )
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
    ) -> Self
    where
        M: Send + Sync + StorageIo + 'static,
    {
        Self {
            recipe: Arc::new(TreeManifestRecipe { summary, metadata }),
            containers,
            indexed_reader: None,
            read_cache: None,
        }
    }

    /// Pins one already recovered Exact Index generation behind this Manifest
    /// reader. Subsequent ordinary demand reads use its bounded candidates and
    /// retain the verified Container scan as their correctness fallback.
    ///
    /// Pinning by [`Arc`] prevents a concurrent activation from changing the
    /// physical-location view halfway through a file read. The index remains
    /// acceleration state and does not extend the lifetime of DATA objects.
    #[must_use]
    pub fn with_active_index<J>(mut self, index: Arc<ActivatedExactIndex<J>>) -> Self
    where
        I: Clone + Send + Sync + 'static,
        J: Send + Sync + StorageIo + 'static,
    {
        self.indexed_reader = Some(Arc::new(ActiveIndexChunkReader {
            containers: self.containers.clone(),
            index,
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
            self.read_at_using(offset, length, |chunk_id, logical_length| {
                self.read_cached(chunk_id, logical_length, || {
                    reader.read_verified_chunk(chunk_id, logical_length)
                })
            })
        } else {
            self.read_at_using(offset, length, |chunk_id, logical_length| {
                self.read_cached(chunk_id, logical_length, || {
                    self.containers
                        .read_verified_chunk(chunk_id, logical_length)
                })
            })
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
        self.read_at_using(offset, length, |chunk_id, logical_length| {
            self.read_cached(chunk_id, logical_length, || {
                self.containers
                    .read_verified_chunk_with_index(index, chunk_id, logical_length)
            })
        })
    }

    fn read_cached<F>(
        &self,
        chunk_id: ChunkId,
        logical_length: u64,
        read_verified: F,
    ) -> Result<Vec<u8>, StoreError>
    where
        F: FnOnce() -> Result<Vec<u8>, StoreError>,
    {
        let Some(cache) = &self.read_cache else {
            return read_verified();
        };
        if let Some(bytes) = cache.get(chunk_id, logical_length) {
            return Ok(bytes);
        }
        let bytes = read_verified()?;
        cache.admit_verified(chunk_id, logical_length, &bytes);
        Ok(bytes)
    }

    fn read_at_using<F>(
        &self,
        offset: u64,
        length: u32,
        mut read_chunk: F,
    ) -> Result<Vec<u8>, ManifestReadError>
    where
        F: FnMut(ChunkId, u64) -> Result<Vec<u8>, StoreError>,
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
        let output_length = usize::try_from(read_end - offset)
            .map_err(|_| ManifestReadError::ArithmeticOverflow)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(output_length)
            .map_err(|_| ManifestReadError::OutOfMemory)?;
        output.resize(output_length, 0);

        let extents = self.recipe.read_range(offset, read_end - offset)?;
        let mut covered_until = offset;
        for located in &extents {
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
                        .copy_from_slice(&payload[source_start..source_end]);
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
                        .copy_from_slice(&payload[source_start..source_end]);
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
