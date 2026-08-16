use crate::{ActivatedExactIndex, ContainerRepository, StorageIo, StoreError};
use fastdup_format::{ChunkId, ManifestExtent, ManifestLeaf};
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
    manifest: ManifestLeaf,
    containers: ContainerRepository<I>,
    indexed_reader: Option<Arc<dyn VerifiedChunkReader>>,
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
            let ManifestExtent::Data {
                logical_length,
                chunk_id,
            } = *extent
            else {
                continue;
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
            manifest,
            containers,
            indexed_reader: None,
        })
    }

    pub(crate) fn from_verified_graph(
        manifest: ManifestLeaf,
        containers: ContainerRepository<I>,
    ) -> Self {
        Self {
            manifest,
            containers,
            indexed_reader: None,
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

    #[must_use]
    pub const fn manifest(&self) -> &ManifestLeaf {
        &self.manifest
    }

    #[must_use]
    pub const fn logical_size(&self) -> u64 {
        self.manifest.file_length()
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
                reader.read_verified_chunk(chunk_id, logical_length)
            })
        } else {
            self.read_at_using(offset, length, |chunk_id, logical_length| {
                self.containers
                    .read_verified_chunk(chunk_id, logical_length)
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
            self.containers
                .read_verified_chunk_with_index(index, chunk_id, logical_length)
        })
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

        let mut extent_start = 0_u64;
        let mut covered_until = offset;
        for extent in self.manifest.extents() {
            let extent_length = extent_logical_length(extent);
            let extent_end = extent_start
                .checked_add(extent_length)
                .ok_or(ManifestReadError::ArithmeticOverflow)?;
            if extent_end <= offset {
                extent_start = extent_end;
                continue;
            }
            if extent_start >= read_end {
                break;
            }
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
            }
            covered_until = copy_end;
            extent_start = extent_end;
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

const fn extent_logical_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
    }
}
