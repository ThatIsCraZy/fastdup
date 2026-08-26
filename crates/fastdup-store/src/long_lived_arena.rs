#![allow(unsafe_code)]

//! Owned storage for dense, long-lived, rebuildable lookup tables.
//!
//! Large tables receive their own anonymous mapping before Transparent Huge
//! Page advice is applied. This deliberately avoids advising allocator pages
//! that may also contain unrelated Rust objects. Small tables retain the
//! ordinary heap path so mapping setup and page-rounding cannot dominate them.

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};

use memmap2::{Advice, MmapMut};

const TRANSPARENT_HUGE_PAGE_MINIMUM_BYTES: usize = 2 * 1_024 * 1_024;

enum ArenaStorage<T> {
    Heap {
        _values: Box<[T]>,
    },
    Anonymous {
        _mapping: MmapMut,
        huge_page_advised: bool,
        marker: PhantomData<T>,
    },
}

/// One contiguous table whose storage policy is invisible to lookup callers.
pub(crate) struct LongLivedArena<T: Copy> {
    storage: ArenaStorage<T>,
    elements: NonNull<T>,
    len: usize,
}

// SAFETY: `elements` always points into the owned heap slice or anonymous
// mapping in `storage`. Moving the owner does not move either allocation, and
// access is exposed only through shared or exclusive Rust slice borrows.
unsafe impl<T: Copy + Send> Send for LongLivedArena<T> {}
// SAFETY: shared access yields `&[T]`; no interior mutation is introduced by
// the cached pointer, so thread sharing follows `T: Sync`.
unsafe impl<T: Copy + Sync> Sync for LongLivedArena<T> {}

impl<T: Copy> LongLivedArena<T> {
    pub(crate) fn try_filled(count: usize, value: T) -> io::Result<Self> {
        let bytes = count
            .checked_mul(size_of::<T>())
            .ok_or_else(|| io::Error::other("long-lived arena byte size overflow"))?;
        if bytes < TRANSPARENT_HUGE_PAGE_MINIMUM_BYTES || bytes == 0 {
            let mut values = Vec::new();
            values
                .try_reserve_exact(count)
                .map_err(|_| io::Error::other("long-lived arena allocation failed"))?;
            values.resize(count, value);
            let mut values = values.into_boxed_slice();
            let elements = NonNull::new(values.as_mut_ptr()).unwrap_or_else(NonNull::dangling);
            return Ok(Self {
                storage: ArenaStorage::Heap { _values: values },
                elements,
                len: count,
            });
        }

        let mut mapping = MmapMut::map_anon(bytes)?;
        if mapping.as_ptr().addr() % align_of::<T>() != 0 {
            return Err(io::Error::other(
                "anonymous mapping does not satisfy lookup-table alignment",
            ));
        }
        #[cfg(target_os = "linux")]
        let huge_page_advised = mapping.advise(Advice::HugePage).is_ok();
        #[cfg(not(target_os = "linux"))]
        let huge_page_advised = false;

        // SAFETY: `mapping` owns `bytes == count * size_of::<T>()` writable,
        // suitably aligned bytes. Every element is initialized exactly once;
        // `T: Copy` has no destructor whose execution the byte mapping could
        // skip. The mapping remains owned for the complete slice lifetime.
        unsafe {
            let elements = mapping.as_mut_ptr().cast::<T>();
            for index in 0..count {
                ptr::write(elements.add(index), value);
            }
        }
        let elements = NonNull::new(mapping.as_mut_ptr().cast::<T>())
            .expect("ASSERT: a nonempty anonymous mapping has a non-null base");
        Ok(Self {
            storage: ArenaStorage::Anonymous {
                _mapping: mapping,
                huge_page_advised,
                marker: PhantomData,
            },
            elements,
            len: count,
        })
    }

    pub(crate) const fn huge_page_advised(&self) -> bool {
        matches!(
            self.storage,
            ArenaStorage::Anonymous {
                huge_page_advised: true,
                ..
            }
        )
    }

    fn as_slice(&self) -> &[T] {
        // SAFETY: construction initialized `len` aligned elements and retained
        // their allocation in `storage`; this borrow prevents mutable access.
        unsafe { std::slice::from_raw_parts(self.elements.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: construction initialized `len` aligned elements and retained
        // their allocation in `storage`; the exclusive borrow prevents aliases.
        unsafe { std::slice::from_raw_parts_mut(self.elements.as_ptr(), self.len) }
    }
}

impl<T: Copy> Deref for LongLivedArena<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T: Copy> DerefMut for LongLivedArena<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T: Copy> fmt::Debug for LongLivedArena<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LongLivedArena")
            .field("len", &self.len)
            .field("element_bytes", &size_of::<T>())
            .field("huge_page_advised", &self.huge_page_advised())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_large_dedicated_arenas_receive_huge_page_advice() {
        let small = LongLivedArena::try_filled(1_024, 7_u64).expect("small arena allocates");
        assert!(!small.huge_page_advised());
        assert!(small.iter().all(|value| *value == 7));

        let mut large = LongLivedArena::try_filled(TRANSPARENT_HUGE_PAGE_MINIMUM_BYTES / 8, 9_u64)
            .expect("large arena maps");
        #[cfg(target_os = "linux")]
        assert!(large.huge_page_advised());
        assert!(large.iter().all(|value| *value == 9));
        let last = large.len() - 1;
        large[last] = 11;
        assert_eq!(large[last], 11);
    }
}
