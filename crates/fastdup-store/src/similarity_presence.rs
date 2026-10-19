//! In-memory per-partition Bucket-key presence filters.
//!
//! A filter is a pure function of the audited Bucket pages of one immutable
//! Run partition: it is rebuilt on every activation, never stored on disk,
//! and carries no content or Location authority. A definite-absent answer
//! only skips a hint probe against that partition; any doubt (missing
//! filter, exhausted budget, false positive) falls back to the regular
//! validated page walk. The one hard invariant is: no false negatives.
use fastdup_format::SimilarityBucketKey;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bits budgeted per distinct Bucket key (~0.8% false positives at k=7).
const BITS_PER_KEY: u64 = 10;
/// Probes per key inside its block.
const HASHES: u32 = 7;
/// Block width in bits; one key touches exactly one block (8 words).
const BLOCK_BITS: u64 = 512;
const BLOCK_WORDS: usize = (BLOCK_BITS / 64) as usize;
/// Hard per-partition cap; larger partitions stay unfiltered (fail-open).
pub(crate) const MAX_PRESENCE_BYTES_PER_PARTITION: u64 = 4 * 1024 * 1024;
/// Process-wide cap across all live partitions. Filter RAM is deliberately
/// outside the unified-cache budget groups so it can never displace Exact
/// structures; the cap here is the bound instead.
pub(crate) const MAX_PRESENCE_BYTES_TOTAL: u64 = 64 * 1024 * 1024;

/// Shared reservation ledger for all live presence filters of one page cache.
#[derive(Debug)]
pub(crate) struct PresenceBudget {
    remaining: AtomicU64,
    total: u64,
    live_filters: AtomicU64,
}

impl PresenceBudget {
    pub(crate) fn new(total: u64) -> Arc<Self> {
        Arc::new(Self {
            remaining: AtomicU64::new(total),
            total,
            live_filters: AtomicU64::new(0),
        })
    }

    fn reserve(&self, bytes: u64) -> bool {
        let mut current = self.remaining.load(Ordering::Acquire);
        loop {
            if current < bytes {
                return false;
            }
            match self.remaining.compare_exchange_weak(
                current,
                current - bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: u64) {
        self.remaining.fetch_add(bytes, Ordering::AcqRel);
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        self.total - self.remaining.load(Ordering::Acquire)
    }

    pub(crate) fn live_filters(&self) -> u64 {
        self.live_filters.load(Ordering::Acquire)
    }
}

/// One live reservation; returns its bytes to the budget on drop.
struct Reservation {
    budget: Arc<PresenceBudget>,
    bytes: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

/// Blocked Bloom filter over the distinct Bucket keys of one partition.
pub(crate) struct BucketPresenceFilter {
    blocks: Box<[u64]>,
    reservation: Reservation,
}

impl BucketPresenceFilter {
    #[must_use]
    pub(crate) fn might_contain(&self, key: SimilarityBucketKey) -> bool {
        let (block, mut probe) = probes(key, self.blocks.len() / BLOCK_WORDS);
        let words = &self.blocks[block * BLOCK_WORDS..(block + 1) * BLOCK_WORDS];
        (0..HASHES).all(|_| {
            probe = splitmix64(probe);
            let bit = (probe & (BLOCK_BITS - 1)) as usize;
            words[bit / 64] & (1_u64 << (bit % 64)) != 0
        })
    }
}

/// Accumulates keys during the audit Bucket walk, then freezes into a filter.
pub(crate) struct BucketPresenceBuilder {
    blocks: Vec<u64>,
    reservation: Option<Reservation>,
    budget: Arc<PresenceBudget>,
}

impl BucketPresenceBuilder {
    /// Reserves a filter sized for `distinct_keys`; `None` when the
    /// per-partition cap or the shared budget would be exceeded.
    pub(crate) fn new(distinct_keys: usize, budget: Arc<PresenceBudget>) -> Option<Self> {
        let blocks = (distinct_keys as u64)
            .saturating_mul(BITS_PER_KEY)
            .div_ceil(BLOCK_BITS)
            .max(1);
        let bytes = blocks.checked_mul(BLOCK_BITS / 8)?;
        if bytes > MAX_PRESENCE_BYTES_PER_PARTITION || !budget.reserve(bytes) {
            return None;
        }
        let words = usize::try_from(blocks).ok()?.checked_mul(BLOCK_WORDS)?;
        let mut storage = Vec::new();
        if storage.try_reserve_exact(words).is_err() {
            budget.release(bytes);
            return None;
        }
        storage.resize(words, 0);
        Some(Self {
            blocks: storage,
            reservation: Some(Reservation {
                budget: Arc::clone(&budget),
                bytes,
            }),
            budget,
        })
    }

    pub(crate) fn insert(&mut self, key: SimilarityBucketKey) {
        let (block, mut probe) = probes(key, self.blocks.len() / BLOCK_WORDS);
        let words = &mut self.blocks[block * BLOCK_WORDS..(block + 1) * BLOCK_WORDS];
        for _ in 0..HASHES {
            probe = splitmix64(probe);
            let bit = (probe & (BLOCK_BITS - 1)) as usize;
            words[bit / 64] |= 1_u64 << (bit % 64);
        }
    }

    pub(crate) fn finish(mut self) -> BucketPresenceFilter {
        self.budget.live_filters.fetch_add(1, Ordering::AcqRel);
        BucketPresenceFilter {
            blocks: std::mem::take(&mut self.blocks).into_boxed_slice(),
            reservation: self
                .reservation
                .take()
                .expect("ASSERT: builder holds its reservation until finish"),
        }
    }
}

impl Drop for BucketPresenceFilter {
    fn drop(&mut self) {
        self.reservation
            .budget
            .live_filters
            .fetch_sub(1, Ordering::AcqRel);
    }
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// Returns (block index, probe seed) for one key.
fn probes(key: SimilarityBucketKey, blocks: usize) -> (usize, u64) {
    let mixed = key.superfeature()
        ^ u64::from(key.logical_length()).wrapping_mul(0xA24B_AED4_963E_E407)
        ^ (u64::from(key.fingerprint_profile()) << 24)
        ^ (u64::from(key.slot()) << 8);
    let h1 = splitmix64(mixed);
    let block = usize::try_from(h1 % blocks as u64)
        .expect("ASSERT: block count is capped far below usize::MAX");
    (block, splitmix64(h1 ^ 0xD1B5_4A32_D192_ED03))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u64) -> SimilarityBucketKey {
        SimilarityBucketKey::new(1, (n % 4) as u8, 65_536, n.wrapping_mul(0x9E37_79B9)).unwrap()
    }

    #[test]
    fn inserted_keys_are_never_reported_absent() {
        let budget = PresenceBudget::new(MAX_PRESENCE_BYTES_TOTAL);
        let mut builder = BucketPresenceBuilder::new(10_000, Arc::clone(&budget)).unwrap();
        for n in 0..10_000_u64 {
            builder.insert(key(n));
        }
        let filter = builder.finish();
        for n in 0..10_000_u64 {
            assert!(filter.might_contain(key(n)), "false negative for {n}");
        }
        let false_positives = (10_000..110_000_u64)
            .filter(|n| filter.might_contain(key(*n)))
            .count();
        assert!(
            false_positives < 5_000,
            "false-positive rate out of bounds: {false_positives}/100000"
        );
        assert_eq!(budget.live_filters(), 1);
        assert!(budget.used_bytes() > 0);
        drop(filter);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.live_filters(), 0);
    }

    #[test]
    fn budget_and_partition_caps_fail_open() {
        let budget = PresenceBudget::new(1024);
        assert!(BucketPresenceBuilder::new(10_000, Arc::clone(&budget)).is_none());
        assert_eq!(budget.used_bytes(), 0, "failed reserve must not leak");
        let huge = PresenceBudget::new(MAX_PRESENCE_BYTES_TOTAL);
        // 10 bits/key: 4 MiB covers ~3.35M keys; 10M keys exceed the cap.
        assert!(BucketPresenceBuilder::new(10_000_000, huge).is_none());
    }
}
