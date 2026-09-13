//! Bounded write batching for unpublished immutable objects. This is transient
//! writer workspace, not a cache: callers explicitly flush before verification,
//! synchronization or publication. Dropping a failed writer publishes nothing.
use std::io;

use crate::StorageIo;

pub(crate) const WRITE_BATCH_BYTES: usize = 1024 * 1024;

/// Writes an already encoded image without allocating another image-sized copy.
pub(crate) fn write_image<I: StorageIo>(storage: &I, name: &str, bytes: &[u8]) -> io::Result<()> {
    for (ordinal, batch) in bytes.chunks(WRITE_BATCH_BYTES).enumerate() {
        storage.write_at(name, (ordinal * WRITE_BATCH_BYTES) as u64, batch)?;
    }
    Ok(())
}

/// Sequential output starts at zero so intermediate writes end at aligned
/// offsets, even when individual format fields and entries are unaligned.
pub(crate) struct ImmutableWriteBuffer {
    bytes: Vec<u8>,
    offset: u64,
}

impl ImmutableWriteBuffer {
    pub(crate) fn new() -> io::Result<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(WRITE_BATCH_BYTES)
            .map_err(io::Error::other)?;
        Ok(Self { bytes, offset: 0 })
    }

    pub(crate) fn append<I: StorageIo>(
        &mut self,
        storage: &I,
        name: &str,
        mut bytes: &[u8],
    ) -> io::Result<()> {
        while !bytes.is_empty() {
            let count = bytes.len().min(WRITE_BATCH_BYTES - self.bytes.len());
            self.bytes.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.bytes.len() == WRITE_BATCH_BYTES {
                self.flush(storage, name)?;
            }
        }
        Ok(())
    }

    pub(crate) fn finish<I: StorageIo>(mut self, storage: &I, name: &str) -> io::Result<()> {
        self.flush(storage, name)
    }

    fn flush<I: StorageIo>(&mut self, storage: &I, name: &str) -> io::Result<()> {
        if !self.bytes.is_empty() {
            let end = self
                .offset
                .checked_add(self.bytes.len() as u64)
                .ok_or(io::ErrorKind::InvalidInput)?;
            storage.write_at(name, self.offset, &self.bytes)?;
            self.offset = end;
            self.bytes.clear();
        }
        Ok(())
    }
}
