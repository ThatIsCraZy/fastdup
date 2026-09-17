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

/// Writes an unpublished temporary image. Its caller must fix the final length
/// and synchronize the file before publication.
pub(crate) fn write_image_unpublished<I: StorageIo + ?Sized>(
    storage: &I,
    name: &str,
    bytes: &[u8],
) -> io::Result<()> {
    for (ordinal, batch) in bytes.chunks(WRITE_BATCH_BYTES).enumerate() {
        storage.write_unpublished_at(name, (ordinal * WRITE_BATCH_BYTES) as u64, batch)?;
    }
    Ok(())
}

/// Default creation of one unpublished object whose complete image is already
/// known. Backends that can bundle the storage envelope and payload into fewer
/// aligned writes override the trait entry point and reuse this fallback for
/// images above their batching bound.
pub(crate) fn write_new_unpublished_image_default<I: StorageIo + ?Sized>(
    storage: &I,
    name: &str,
    bytes: &[u8],
) -> io::Result<()> {
    storage.create_new(name)?;
    write_image_unpublished(storage, name, bytes)?;
    let length =
        u64::try_from(bytes.len()).map_err(|_| io::Error::other("image length must fit u64"))?;
    storage.set_len_unpublished(name, length)
}

/// Sequential output starts at zero so intermediate writes end at aligned
/// offsets, even when individual format fields and entries are unaligned.
pub(crate) struct ImmutableWriteBuffer {
    bytes: Vec<u8>,
    offset: u64,
    unpublished: bool,
}

impl ImmutableWriteBuffer {
    pub(crate) fn new() -> io::Result<Self> {
        Self::with_unpublished(false)
    }

    pub(crate) fn new_unpublished() -> io::Result<Self> {
        Self::with_unpublished(true)
    }

    fn with_unpublished(unpublished: bool) -> io::Result<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(WRITE_BATCH_BYTES)
            .map_err(io::Error::other)?;
        Ok(Self {
            bytes,
            offset: 0,
            unpublished,
        })
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
            if self.unpublished {
                storage.write_unpublished_at(name, self.offset, &self.bytes)?;
            } else {
                storage.write_at(name, self.offset, &self.bytes)?;
            }
            self.offset = end;
            self.bytes.clear();
        }
        Ok(())
    }
}
