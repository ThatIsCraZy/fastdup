//! Kernel-owned read capacity. The Vec stays empty until the complete read
//! succeeds; errors and EOF drop capacity without exposing uninitialized bytes.

use std::io;

pub(super) struct ReadBuffer {
    bytes: Vec<u8>,
    length: usize,
}

impl ReadBuffer {
    pub(super) fn new(length: usize) -> io::Result<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        Ok(Self { bytes, length })
    }

    pub(super) const fn len(&self) -> usize {
        self.length
    }

    pub(super) fn remaining_ptr(&mut self, progress: usize) -> *mut u8 {
        // A pointer to spare capacity is valid for kernel writes, but no Rust
        // byte slice may be formed before successful CQEs prove initialization.
        self.bytes.spare_capacity_mut()[progress..self.length]
            .as_mut_ptr()
            .cast()
    }

    /// # Safety
    /// The ring must have completed writes to every byte in `0..length`, and
    /// no outstanding operation may still access this allocation. Positive
    /// partial CQEs alone are insufficient; errors/EOF must drop the owner.
    pub(super) unsafe fn finish(mut self) -> Vec<u8> {
        // SAFETY: the caller pairs cumulative successful CQEs with the exact
        // submitted ranges. Capacity was reserved before the first submission.
        unsafe {
            self.bytes.set_len(self.length);
        }
        self.bytes
    }
}

#[cfg(test)]
impl ReadBuffer {
    pub(super) fn write_fixture(&mut self, offset: usize, bytes: &[u8]) {
        for (slot, byte) in self.bytes.spare_capacity_mut()[offset..offset + bytes.len()]
            .iter_mut()
            .zip(bytes)
        {
            slot.write(*byte);
        }
    }
}
