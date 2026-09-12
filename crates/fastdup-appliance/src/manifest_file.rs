//! Expose verified Manifest reads and clone recipes through the POSIX file interface.

use std::fmt;

use fastdup_format::ManifestExtent;
use fastdup_posix::{CommittedFile, PosixError, PreparedCommitExtent, PreparedDataRecipe};
use fastdup_store::{StorageIo, VerifiedManifestFile};

pub(crate) struct ManifestCommittedFile<I> {
    file: VerifiedManifestFile<I>,
    logical_size: u64,
    allocated_bytes: u64,
}

impl<I: StorageIo> ManifestCommittedFile<I> {
    pub(crate) fn from_verified(file: VerifiedManifestFile<I>) -> Self {
        let logical_size = file.logical_size();
        let allocated_bytes = file.allocated_bytes();
        Self {
            file,
            logical_size,
            allocated_bytes,
        }
    }
}

impl<I> fmt::Debug for ManifestCommittedFile<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManifestCommittedFile")
            .field("logical_size", &self.logical_size)
            .field("allocated_bytes", &self.allocated_bytes)
            .finish_non_exhaustive()
    }
}

impl<I> CommittedFile for ManifestCommittedFile<I>
where
    I: Send + Sync + StorageIo,
{
    fn logical_size(&self) -> u64 {
        self.logical_size
    }

    fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        if offset == 0 && length >= self.logical_size {
            return Ok(self.allocated_bytes);
        }
        self.file
            .allocated_bytes_in_range(offset, length)
            .map_err(|_| PosixError::Io)
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.file
            .read_at(offset, length)
            .map_err(|_| PosixError::Io)
    }

    fn read_shared_at(&self, offset: u64, length: u32) -> Result<bytes::Bytes, PosixError> {
        self.file
            .read_shared_at(offset, length)
            .map_err(|_| PosixError::Io)
    }

    fn read_segments_at(&self, offset: u64, length: u32) -> Result<Vec<bytes::Bytes>, PosixError> {
        self.file
            .read_segments_at(offset, length)
            .map_err(|_| PosixError::Io)
    }

    fn prepared_clone_extents(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Option<Vec<PreparedCommitExtent>>, PosixError> {
        let end = offset.checked_add(length).ok_or(PosixError::FileTooLarge)?;
        if length == 0 || end > self.logical_size {
            return Err(PosixError::InvalidArgument);
        }
        // The same verified range walk below detects HOLEs and validates the
        // complete partition. A separate allocation traversal repeats metadata I/O.
        let located = self
            .file
            .manifest_extents_in_range(offset, length)
            .map_err(|_| PosixError::Io)?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(located.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        let mut cursor = offset;
        for located_extent in located {
            let extent = located_extent.extent();
            let extent_length = match *extent {
                ManifestExtent::Data { logical_length, .. }
                | ManifestExtent::DataSlice { logical_length, .. }
                | ManifestExtent::Hole { logical_length }
                | ManifestExtent::Fill { logical_length, .. } => logical_length,
            };
            let extent_end = located_extent
                .logical_offset()
                .checked_add(extent_length)
                .ok_or(PosixError::Io)?;
            let selected_start = located_extent.logical_offset().max(offset);
            let selected_end = extent_end.min(end);
            if selected_start != cursor || selected_start >= selected_end {
                return Err(PosixError::Io);
            }
            let selected_length = selected_end - selected_start;
            let recipe = match *extent {
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                } => {
                    if selected_start == located_extent.logical_offset()
                        && selected_length == logical_length
                    {
                        PreparedDataRecipe::Chunk {
                            chunk_id: chunk_id.bytes(),
                        }
                    } else {
                        PreparedDataRecipe::ChunkSlice {
                            chunk_id: chunk_id.bytes(),
                            chunk_length: u32::try_from(logical_length)
                                .map_err(|_| PosixError::Io)?,
                            chunk_offset: u32::try_from(
                                selected_start - located_extent.logical_offset(),
                            )
                            .map_err(|_| PosixError::Io)?,
                        }
                    }
                }
                ManifestExtent::DataSlice {
                    chunk_id,
                    chunk_length,
                    chunk_offset,
                    ..
                } => PreparedDataRecipe::ChunkSlice {
                    chunk_id: chunk_id.bytes(),
                    chunk_length,
                    chunk_offset: chunk_offset
                        .checked_add(
                            u32::try_from(selected_start - located_extent.logical_offset())
                                .map_err(|_| PosixError::Io)?,
                        )
                        .ok_or(PosixError::Io)?,
                },
                ManifestExtent::Fill { value, .. } => PreparedDataRecipe::Fill { value },
                ManifestExtent::Hole { .. } => return Ok(None),
            };
            let prepared_extent = match self.file.manifest_root() {
                Some(root) => PreparedCommitExtent::try_new_retained(
                    selected_start,
                    selected_length,
                    recipe,
                    root.bytes(),
                    selected_start,
                )?,
                None => PreparedCommitExtent::try_new(selected_start, selected_length, recipe)?,
            };
            prepared.push(prepared_extent);
            cursor = selected_end;
        }
        if cursor != end {
            return Err(PosixError::Io);
        }
        Ok(Some(prepared))
    }
}
