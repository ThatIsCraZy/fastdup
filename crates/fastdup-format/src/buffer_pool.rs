//! Reusable initialized codec buffers. Idle retention is controlled externally.
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, Weak};

const SLOTS: usize = 32;

#[derive(Debug)]
struct State {
    idle: [Vec<u8>; SLOTS],
    limit: usize,
    status: BufferPoolStatus,
}

/// Allocation reuse counters, separate from content-cache hit rates.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BufferPoolStatus {
    pub retained_bytes: usize,
    pub active_bytes: usize,
    pub peak_active_bytes: usize,
    pub hits: u64,
    pub reused_bytes: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// Bounded idle buffers; active owners are ordinary working memory.
#[derive(Clone, Debug)]
pub struct BufferPool(Arc<Mutex<State>>);

impl BufferPool {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(State {
            idle: std::array::from_fn(|_| Vec::new()),
            limit: 0,
            status: BufferPoolStatus::default(),
        })))
    }

    /// Conservatively includes fixed slot metadata and allocation bookkeeping.
    #[must_use]
    pub const fn metadata_bytes() -> usize {
        std::mem::size_of::<State>() + 64 + SLOTS * 32
    }

    /// Returns initialized storage of exactly `length` bytes. Reused bytes are
    /// unspecified: codecs must overwrite and validate output before publishing.
    /// No allocator operation runs while the pool lock is held.
    /// # Panics
    /// Panics if the internal pool lock was poisoned or allocation size overflows.
    #[must_use]
    pub fn take(&self, length: usize) -> PooledBuffer {
        let mut bytes = {
            let mut state = self.0.lock().expect("ASSERT: buffer pool lock poisoned");
            let slot = state
                .idle
                .iter()
                .enumerate()
                .filter(|(_, bytes)| {
                    bytes.capacity() >= length
                        && bytes.capacity() <= length.saturating_mul(2)
                        && bytes.capacity() != 0
                })
                .min_by_key(|(_, bytes)| bytes.capacity())
                .map(|(index, _)| index);
            if let Some(slot) = slot {
                let bytes = std::mem::take(&mut state.idle[slot]);
                state.status.retained_bytes -= bytes.capacity();
                state.status.hits = state.status.hits.saturating_add(1);
                state.status.reused_bytes = state
                    .status
                    .reused_bytes
                    .saturating_add(bytes.capacity() as u64);
                bytes
            } else {
                state.status.misses = state.status.misses.saturating_add(1);
                Vec::new()
            }
        };
        bytes.resize(length, 0);
        {
            let mut state = self.0.lock().expect("ASSERT: buffer pool lock poisoned");
            state.status.active_bytes += bytes.capacity();
            state.status.peak_active_bytes = state
                .status
                .peak_active_bytes
                .max(state.status.active_bytes);
        }
        PooledBuffer {
            bytes,
            owner: Arc::downgrade(&self.0),
        }
    }

    /// Atomically shrinks idle retention before the caller returns a RAM lease.
    /// Active readers keep their bytes and obey the new limit on final release.
    /// # Panics
    /// Panics if an internal pool invariant poisoned the lock.
    pub fn set_limit(&self, limit: usize) {
        let mut discarded: [Vec<u8>; SLOTS] = std::array::from_fn(|_| Vec::new());
        {
            let mut state = self.0.lock().expect("ASSERT: buffer pool lock poisoned");
            state.limit = limit;
            for (slot, discard) in discarded.iter_mut().enumerate() {
                if state.status.retained_bytes <= limit {
                    break;
                }
                let bytes = std::mem::take(&mut state.idle[slot]);
                if bytes.capacity() != 0 {
                    state.status.retained_bytes -= bytes.capacity();
                    state.status.evictions = state.status.evictions.saturating_add(1);
                }
                *discard = bytes;
            }
        }
        drop(discarded);
    }

    /// Samples counters without walking any allocator lists.
    /// # Panics
    /// Panics if an internal pool invariant poisoned the lock.
    #[must_use]
    pub fn status(&self) -> BufferPoolStatus {
        self.0
            .lock()
            .expect("ASSERT: buffer pool lock poisoned")
            .status
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Exclusive initialized bytes. Dropping the final owner returns capacity,
/// never a verification certificate or an immutable reader's live allocation.
#[derive(Debug)]
pub struct PooledBuffer {
    bytes: Vec<u8>,
    owner: Weak<Mutex<State>>,
}

impl Deref for PooledBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl PooledBuffer {
    // Explicit ownership transfer preserves the existing into_payload fast path.
    // The caller now owns ordinary working memory, outside idle pool retention.
    pub(crate) fn into_vec(mut self) -> Vec<u8> {
        if let Some(owner) = self.owner.upgrade() {
            owner
                .lock()
                .expect("ASSERT: buffer pool lock poisoned")
                .status
                .active_bytes -= self.bytes.capacity();
        }
        self.owner = Weak::new();
        std::mem::take(&mut self.bytes)
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.bytes.capacity()
    }
}
impl Drop for PooledBuffer {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let mut state = owner.lock().expect("ASSERT: buffer pool lock poisoned");
        let capacity = self.bytes.capacity();
        state.status.active_bytes -= capacity;
        if capacity != 0
            && capacity <= state.limit.saturating_sub(state.status.retained_bytes)
            && let Some(slot) = state.idle.iter_mut().find(|bytes| bytes.capacity() == 0)
        {
            *slot = std::mem::take(&mut self.bytes);
            state.status.retained_bytes += capacity;
            return;
        }
        state.status.evictions = state
            .status
            .evictions
            .saturating_add(u64::from(capacity != 0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warm_buffers_reuse_capacity_and_shrink_revokes_late_returns() {
        let pool = BufferPool::new();
        pool.set_limit(131_072);
        let mut first = pool.take(65_536);
        first.fill(91);
        let address = first.as_ptr();
        drop(first);
        let next = pool.take(65_536);
        assert_eq!(next.as_ptr(), address);
        assert_eq!(pool.status().hits, 1);
        assert_eq!(pool.status().misses, 1);
        pool.set_limit(0);
        assert!(next.iter().all(|byte| *byte == 91));
        drop(next);
        assert_eq!(pool.status().active_bytes, 0);
        assert_eq!(pool.status().retained_bytes, 0);
    }

    #[test]
    fn concurrent_returns_and_pressure_stay_bounded() {
        let pool = BufferPool::new();
        pool.set_limit(4 * 65_536);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..100 {
                        let mut bytes = pool.take(65_536);
                        bytes.fill(17);
                        assert!(bytes.iter().all(|byte| *byte == 17));
                    }
                });
            }
            scope.spawn(|| {
                for i in 0..100 {
                    pool.set_limit((i % 2) * 4 * 65_536);
                }
            });
        });
        assert_eq!(pool.status().active_bytes, 0);
        assert!(pool.status().retained_bytes <= 4 * 65_536);
        pool.set_limit(0);
        assert_eq!(pool.status().retained_bytes, 0);
    }

    #[test]
    fn destroying_pool_does_not_invalidate_borrowers() {
        let pool = BufferPool::new();
        let mut bytes = pool.take(1024);
        bytes.fill(31);
        drop(pool);
        assert!(bytes.iter().all(|byte| *byte == 31));
    }

    #[test]
    fn a_tiny_decode_does_not_pin_a_large_scratch_allocation() {
        let pool = BufferPool::new();
        pool.set_limit(1 << 20);
        drop(pool.take(262_144));
        let small = pool.take(4096);
        assert!(small.capacity() <= 8192);
        assert_eq!(pool.status().misses, 2);
        assert_eq!(pool.status().retained_bytes, 262_144);
    }
}
