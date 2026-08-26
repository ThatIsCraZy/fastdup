#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::path::Path;
use std::time::SystemTime;

use memmap2::{Mmap, MmapOptions};

/// Read-only mapping used only by the controlled benchmark fixture.
pub(crate) struct ImmutableFileMap {
    file: File,
    map: Mmap,
    initial_length: u64,
    initial_modified: Option<SystemTime>,
}

impl ImmutableFileMap {
    pub(crate) fn open(path: &Path, expected_length: u64) -> io::Result<Self> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != expected_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mmap source must be the complete immutable regular Run file",
            ));
        }
        // SAFETY: the benchmark owns a read-only descriptor for an immutable
        // Run fixture, retains that descriptor and the mapping together, and
        // verifies length/mtime again before returning. Production use still
        // requires generation pinning against concurrent truncate/replace.
        let map = unsafe { MmapOptions::new().map(&file)? };
        Ok(Self {
            file,
            map,
            initial_length: metadata.len(),
            initial_modified: metadata.modified().ok(),
        })
    }

    pub(crate) fn verify_unchanged(&self) -> io::Result<()> {
        let metadata = self.file.metadata()?;
        if metadata.len() != self.initial_length
            || metadata.modified().ok() != self.initial_modified
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mapped Similarity Run changed during benchmark",
            ));
        }
        Ok(())
    }

    pub(crate) fn range(&self, start: usize, end: usize) -> io::Result<&[u8]> {
        self.map
            .get(start..end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "mmap page outside Run"))
    }
}
