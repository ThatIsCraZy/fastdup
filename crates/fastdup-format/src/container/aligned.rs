//! Page-aligned initialized publication buffers; no storage I/O.
use super::{FormatError, HEADER_BYTES, RECORD_CRC_OFFSET, put_u32};
use core::fmt;

/// One page-aligned, page-sized immutable Container publication image.
///
/// The format's Header, and complete file length are all 4 KiB
/// aligned. Retaining that geometry in memory lets a storage adapter use the
/// same owned writer image for Linux Direct I/O without a second full-image
/// copy.
pub struct AlignedContainerBytes {
    allocation: Vec<u8>,
    start: usize,
    length: usize,
}

/// Builds one page-aligned image without first zeroing ranges that later hold
/// already initialized Records or the Recovery Index.
///
/// The backing allocation never reallocates beyond its initial capacity, so
/// its aligned start remains stable while safe `Vec` appends initialize the
/// image in durable byte order. An `AlignedContainerBytes` is exposed only
/// after the complete declared image has been initialized.
pub(super) struct AlignedContainerBuilder {
    allocation: Vec<u8>,
    start: usize,
    length: usize,
}

impl AlignedContainerBytes {
    /// Returns an allocation-free consumed-image sentinel.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            allocation: Vec::new(),
            start: 0,
            length: 0,
        }
    }

    /// Allocates one zero-filled image whose address and length are both
    /// aligned to the Container block size.
    ///
    /// # Panics
    ///
    /// Panics when `length` is zero or is not a multiple of 4 KiB.
    #[must_use]
    pub fn zeroed(length: usize) -> Self {
        assert!(length != 0 && length.is_multiple_of(HEADER_BYTES));
        let allocation_length = length
            .checked_add(HEADER_BYTES - 1)
            .expect("ASSERT: bounded Container alignment allocation cannot overflow");
        let allocation = vec![0; allocation_length];
        let misalignment = allocation.as_ptr().addr() % HEADER_BYTES;
        let start = (HEADER_BYTES - misalignment) % HEADER_BYTES;
        assert!(start + length <= allocation.len());
        Self {
            allocation,
            start,
            length,
        }
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

impl AlignedContainerBuilder {
    pub(super) fn new(length: usize) -> Self {
        assert!(length != 0 && length.is_multiple_of(HEADER_BYTES));
        let allocation_length = length
            .checked_add(HEADER_BYTES - 1)
            .expect("ASSERT: bounded Container alignment allocation cannot overflow");
        let mut allocation: Vec<u8> = Vec::with_capacity(allocation_length);
        let misalignment = allocation.as_ptr().addr() % HEADER_BYTES;
        let start = (HEADER_BYTES - misalignment) % HEADER_BYTES;
        allocation.resize(start, 0);
        Self {
            allocation,
            start,
            length,
        }
    }

    pub(super) fn image_length(&self) -> usize {
        self.allocation.len() - self.start
    }

    pub(super) fn append(&mut self, bytes: &mut Vec<u8>) {
        let next_length = self
            .image_length()
            .checked_add(bytes.len())
            .expect("ASSERT: bounded Container append length cannot overflow");
        assert!(next_length <= self.length);
        self.allocation.append(bytes);
    }

    pub(super) fn append_slice(&mut self, bytes: &[u8]) {
        assert!(bytes.len() <= self.length - self.image_length());
        self.allocation.extend_from_slice(bytes);
    }

    pub(super) fn image(&self) -> &[u8] {
        &self.allocation[self.start..]
    }

    fn image_mut(&mut self) -> &mut [u8] {
        &mut self.allocation[self.start..]
    }

    /// Initializes metadata and padding only, then seals the complete Record.
    /// Appending cannot reallocate beyond the already reserved image capacity.
    pub(super) fn append_record(
        &mut self,
        record_length: usize,
        metadata_length: usize,
        payload: &[u8],
        metadata: impl FnOnce(&mut [u8]) -> Result<(), FormatError>,
    ) -> Result<(), FormatError> {
        let payload_end = metadata_length
            .checked_add(payload.len())
            .ok_or(FormatError::ArithmeticOverflow)?;
        if payload_end > record_length {
            return Err(FormatError::InvalidRecordLength(record_length));
        }
        let start = self.image_length();
        self.append_zeroed(metadata_length);
        metadata(&mut self.image_mut()[start..])?;
        self.append_slice(payload);
        self.append_zeroed(record_length - payload_end);
        let record = &mut self.image_mut()[start..];
        let checksum = crc32c::crc32c(record);
        put_u32(record, RECORD_CRC_OFFSET, checksum);
        Ok(())
    }

    pub(super) fn append_zeroed(&mut self, length: usize) {
        let next_length = self
            .image_length()
            .checked_add(length)
            .expect("ASSERT: bounded Container padding length cannot overflow");
        assert!(next_length <= self.length);
        self.allocation.resize(self.start + next_length, 0);
    }

    pub(super) fn finish(self) -> AlignedContainerBytes {
        assert_eq!(self.image_length(), self.length);
        assert_eq!(
            self.allocation[self.start..].as_ptr().addr() % HEADER_BYTES,
            0
        );
        AlignedContainerBytes {
            allocation: self.allocation,
            start: self.start,
            length: self.length,
        }
    }
}

impl AsRef<[u8]> for AlignedContainerBytes {
    fn as_ref(&self) -> &[u8] {
        &self.allocation[self.start..self.start + self.length]
    }
}

impl AsMut<[u8]> for AlignedContainerBytes {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.allocation[self.start..self.start + self.length]
    }
}

impl Clone for AlignedContainerBytes {
    fn clone(&self) -> Self {
        let mut cloned = Self::zeroed(self.length);
        cloned.copy_from_slice(self);
        cloned
    }
}

impl fmt::Debug for AlignedContainerBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlignedContainerBytes")
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl PartialEq for AlignedContainerBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for AlignedContainerBytes {}

impl std::ops::Deref for AlignedContainerBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::ops::DerefMut for AlignedContainerBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}
