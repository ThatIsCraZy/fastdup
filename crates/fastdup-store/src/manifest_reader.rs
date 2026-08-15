use crate::{ContainerRepository, StorageIo, StoreError};
use fastdup_format::{ChunkId, ManifestExtent, ManifestLeaf};
use std::collections::BTreeMap;
use std::fmt;

pub const MAX_MANIFEST_READ_BYTES: u32 = 1_024 * 1_024;

/// A verified immutable file recipe backed by durable RAW containers.
///
/// Construction verifies every DATA dependency as one batch. Demand reads
/// re-verify the selected immutable container before copying any DATA bytes;
/// HOLE and FILL extents require no physical data location.
#[derive(Clone, Debug)]
pub struct VerifiedManifestFile<I> {
    manifest: ManifestLeaf,
    containers: ContainerRepository<I>,
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
        })
    }

    pub(crate) fn from_verified_graph(
        manifest: ManifestLeaf,
        containers: ContainerRepository<I>,
    ) -> Self {
        Self {
            manifest,
            containers,
        }
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
                    let payload = self
                        .containers
                        .read_verified_chunk(chunk_id, logical_length)?;
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
