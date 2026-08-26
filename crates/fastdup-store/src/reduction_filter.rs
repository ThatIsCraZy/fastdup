//! Bounded, non-authoritative in-memory hints for Exact Index lookups.

use std::collections::TryReserveError;
use std::fmt;
use std::mem::size_of;

use fastdup_format::ChunkId;

use crate::long_lived_arena::LongLivedArena;

const CACHE_WAYS: usize = 4;
const BLOOM_BITS_PER_BLOCK: usize = 512;
const BLOOM_BYTES_PER_BLOCK: usize = BLOOM_BITS_PER_BLOCK / 8;
const BLOOM_TARGET_BITS_PER_KEY: usize = 10;
const BLOOM_PROBES: u64 = 7;

/// The result of a Bloom probe. Neither variant establishes an Exact Hit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BloomLookupHint {
    /// The key was not inserted into this filter generation.
    DefinitelyAbsent,
    /// The full Exact Index must decide whether the key exists.
    RequiresExactLookup,
}

/// An opaque cache suggestion that must be verified by the Exact Index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnverifiedLocationHint(u64);

impl UnverifiedLocationHint {
    /// Wraps an implementation-owned location ordinal as an unverified hint.
    #[must_use]
    pub const fn new(ordinal: u64) -> Self {
        Self(ordinal)
    }

    /// Returns the ordinal to present to the authoritative Exact Index.
    #[must_use]
    pub const fn ordinal(self) -> u64 {
        self.0
    }
}

/// Construction failures for bounded reduction hint structures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HintStructureError {
    ZeroCapacity,
    CapacityOverflow,
    BudgetExceeded {
        required_bytes: usize,
        maximum_bytes: usize,
    },
    AllocationFailed,
}

impl fmt::Display for HintStructureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => formatter.write_str("hint structure capacity is zero"),
            Self::CapacityOverflow => {
                formatter.write_str("hint structure capacity arithmetic overflowed")
            }
            Self::BudgetExceeded {
                required_bytes,
                maximum_bytes,
            } => write!(
                formatter,
                "hint structure requires {required_bytes} bytes but its budget is {maximum_bytes}"
            ),
            Self::AllocationFailed => formatter.write_str("hint structure allocation failed"),
        }
    }
}

impl std::error::Error for HintStructureError {}

#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, Default)]
struct BloomBlock {
    words: [u64; 8],
}

const _: () = assert!(size_of::<BloomBlock>() == 64);

/// A cache-line-blocked Bloom hint for `(ChunkId, logical_length)` keys.
///
/// Every probe or insert selects exactly one 64-byte block and checks or sets
/// seven deterministic, distinct bits within that block. A positive result is
/// never an Exact Hit: callers must query the complete Exact Index.
#[derive(Debug)]
pub struct BlockedBloomHint {
    blocks: LongLivedArena<BloomBlock>,
    expected_keys: usize,
}

impl BlockedBloomHint {
    /// Creates a power-of-two block table within an explicit byte budget.
    ///
    /// # Errors
    ///
    /// Returns an error for zero capacity, arithmetic overflow, insufficient
    /// budget, or allocation failure. The budget accounts for block payloads;
    /// the small owning `Box` descriptor is not included.
    pub fn new(expected_keys: usize, maximum_bytes: usize) -> Result<Self, HintStructureError> {
        if expected_keys == 0 {
            return Err(HintStructureError::ZeroCapacity);
        }
        let required_bits = expected_keys
            .checked_mul(BLOOM_TARGET_BITS_PER_KEY)
            .ok_or(HintStructureError::CapacityOverflow)?;
        let minimum_blocks = required_bits.div_ceil(BLOOM_BITS_PER_BLOCK).max(1);
        let block_count = minimum_blocks
            .checked_next_power_of_two()
            .ok_or(HintStructureError::CapacityOverflow)?;
        let required_bytes = block_count
            .checked_mul(BLOOM_BYTES_PER_BLOCK)
            .ok_or(HintStructureError::CapacityOverflow)?;
        ensure_budget(required_bytes, maximum_bytes)?;
        let blocks = LongLivedArena::try_filled(block_count, BloomBlock::default())
            .map_err(|_| HintStructureError::AllocationFailed)?;
        Ok(Self {
            blocks,
            expected_keys,
        })
    }

    /// Records a key as a non-authoritative membership hint.
    ///
    /// Once this returns, subsequent probes of the same key cannot report
    /// `DefinitelyAbsent` because this filter never clears bits.
    pub fn insert_hint(&mut self, chunk_id: ChunkId, logical_length: usize) {
        let (block_hash, bit_hash) = key_hashes(chunk_id, logical_length);
        let block_index = table_index(block_hash, self.blocks.len());
        let block = &mut self.blocks[block_index];
        visit_bloom_bits(bit_hash, |word, mask| block.words[word] |= mask);
    }

    /// Returns only a lookup hint; it never authorizes chunk reuse.
    #[must_use]
    pub fn probe_for_exact_lookup(
        &self,
        chunk_id: ChunkId,
        logical_length: usize,
    ) -> BloomLookupHint {
        let (block_hash, bit_hash) = key_hashes(chunk_id, logical_length);
        let block_index = table_index(block_hash, self.blocks.len());
        let block = &self.blocks[block_index];
        let mut all_set = true;
        visit_bloom_bits(bit_hash, |word, mask| {
            all_set &= block.words[word] & mask != 0;
        });
        if all_set {
            BloomLookupHint::RequiresExactLookup
        } else {
            BloomLookupHint::DefinitelyAbsent
        }
    }

    #[must_use]
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    #[must_use]
    pub const fn expected_keys(&self) -> usize {
        self.expected_keys
    }

    /// Returns the exact block payload size of this filter.
    ///
    /// # Panics
    ///
    /// Panics only if a previously validated block count no longer fits a
    /// `usize` byte size, which is an impossible production `ASSERT`.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        self.blocks
            .len()
            .checked_mul(BLOOM_BYTES_PER_BLOCK)
            .expect("ASSERT: a constructed Bloom allocation size cannot overflow")
    }

    /// Reports whether this dense table was placed in its own THP-advised
    /// anonymous mapping. Advice is a performance hint, not proof that every
    /// page has already been promoted by the kernel.
    #[must_use]
    pub const fn huge_page_advised(&self) -> bool {
        self.blocks.huge_page_advised()
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
struct CacheSet {
    chunk_ids: [[u8; 32]; CACHE_WAYS],
    logical_lengths: [u64; CACHE_WAYS],
    location_ordinals: [u64; CACHE_WAYS],
    valid_mask: u8,
    next_victim: u8,
}

impl Default for CacheSet {
    fn default() -> Self {
        Self {
            chunk_ids: [[0; 32]; CACHE_WAYS],
            logical_lengths: [0; CACHE_WAYS],
            location_ordinals: [0; CACHE_WAYS],
            valid_mask: 0,
            next_victim: 0,
        }
    }
}

const _: () = assert!(size_of::<CacheSet>() == 256);

/// A bounded, pointer-free four-way location hint cache for one worker.
///
/// Sets are contiguous and cache-line aligned. Lookups allocate nothing and do
/// not mutate replacement state. Returned locations remain unverified hints;
/// callers must pair them with the full Exact Index key before reuse.
#[derive(Debug)]
pub struct PerWorkerLocationHintCache {
    sets: Box<[CacheSet]>,
    expected_entries: usize,
}

impl PerWorkerLocationHintCache {
    /// Creates a power-of-two set table within an explicit byte budget.
    ///
    /// # Errors
    ///
    /// Returns an error for zero capacity, arithmetic overflow, insufficient
    /// budget, or allocation failure.
    pub fn new(expected_entries: usize, maximum_bytes: usize) -> Result<Self, HintStructureError> {
        if expected_entries == 0 {
            return Err(HintStructureError::ZeroCapacity);
        }
        let minimum_sets = expected_entries.div_ceil(CACHE_WAYS);
        let set_count = minimum_sets
            .checked_next_power_of_two()
            .ok_or(HintStructureError::CapacityOverflow)?;
        let required_bytes = set_count
            .checked_mul(size_of::<CacheSet>())
            .ok_or(HintStructureError::CapacityOverflow)?;
        ensure_budget(required_bytes, maximum_bytes)?;
        let sets = allocate_filled(set_count, CacheSet::default())?;
        Ok(Self {
            sets,
            expected_entries,
        })
    }

    /// Remembers one unverified location hint using deterministic replacement.
    ///
    /// Existing keys are updated in place. Empty ways are filled from the
    /// lowest index; full sets use round-robin replacement.
    ///
    /// # Panics
    ///
    /// Panics only if the compile-time four-way associativity does not fit its
    /// serialized replacement counter, which is an impossible `ASSERT`.
    pub fn remember_unverified(
        &mut self,
        chunk_id: ChunkId,
        logical_length: usize,
        location: UnverifiedLocationHint,
    ) {
        let logical_length = length_u64(logical_length);
        let chunk_id = chunk_id.bytes();
        let (set_hash, _) = key_hashes_bytes(chunk_id, logical_length);
        let set_index = table_index(set_hash, self.sets.len());
        let set = &mut self.sets[set_index];

        if let Some(way) = matching_way(set, &chunk_id, logical_length) {
            set.location_ordinals[way] = location.ordinal();
            return;
        }

        let way = first_invalid_way(set).unwrap_or_else(|| {
            let victim = usize::from(set.next_victim);
            set.next_victim = (set.next_victim + 1)
                % u8::try_from(CACHE_WAYS).expect("ASSERT: cache associativity fits u8");
            victim
        });
        set.chunk_ids[way] = chunk_id;
        set.logical_lengths[way] = logical_length;
        set.location_ordinals[way] = location.ordinal();
        set.valid_mask |= way_mask(way);
    }

    /// Suggests a location that still requires authoritative Exact validation.
    #[must_use]
    pub fn probe_unverified(
        &self,
        chunk_id: ChunkId,
        logical_length: usize,
    ) -> Option<UnverifiedLocationHint> {
        let logical_length = length_u64(logical_length);
        let chunk_id = chunk_id.bytes();
        let (set_hash, _) = key_hashes_bytes(chunk_id, logical_length);
        let set = &self.sets[table_index(set_hash, self.sets.len())];
        matching_way(set, &chunk_id, logical_length)
            .map(|way| UnverifiedLocationHint::new(set.location_ordinals[way]))
    }

    #[must_use]
    pub fn set_count(&self) -> usize {
        self.sets.len()
    }

    #[must_use]
    pub const fn expected_entries(&self) -> usize {
        self.expected_entries
    }

    /// Returns the exact contiguous set payload size of this cache.
    ///
    /// # Panics
    ///
    /// Panics only if a previously validated set count no longer fits a
    /// `usize` byte size, which is an impossible production `ASSERT`.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        self.sets
            .len()
            .checked_mul(size_of::<CacheSet>())
            .expect("ASSERT: a constructed cache allocation size cannot overflow")
    }
}

fn matching_way(set: &CacheSet, chunk_id: &[u8; 32], logical_length: u64) -> Option<usize> {
    (0..CACHE_WAYS).find(|&way| {
        set.valid_mask & way_mask(way) != 0
            && set.logical_lengths[way] == logical_length
            && set.chunk_ids[way] == *chunk_id
    })
}

fn first_invalid_way(set: &CacheSet) -> Option<usize> {
    (0..CACHE_WAYS).find(|&way| set.valid_mask & way_mask(way) == 0)
}

fn way_mask(way: usize) -> u8 {
    assert!(way < CACHE_WAYS, "ASSERT: cache way is in range");
    1_u8 << u32::try_from(way).expect("ASSERT: cache way fits a shift count")
}

fn visit_bloom_bits(mut hash: u64, mut visitor: impl FnMut(usize, u64)) {
    let base = hash & 511;
    hash >>= 9;
    let stride = ((hash & 255) << 1) | 1;
    for probe in 0..BLOOM_PROBES {
        let position = base.wrapping_add(probe.wrapping_mul(stride)) & 511;
        let word = usize::try_from(position >> 6)
            .expect("ASSERT: a Bloom word position always fits usize");
        let bit =
            u32::try_from(position & 63).expect("ASSERT: a Bloom bit position always fits u32");
        visitor(word, 1_u64 << bit);
    }
}

fn table_index(hash: u64, table_length: usize) -> usize {
    assert!(
        table_length.is_power_of_two(),
        "ASSERT: constructed hint tables have power-of-two lengths"
    );
    let mask = u64::try_from(table_length - 1)
        .expect("ASSERT: a table length always fits the 64-bit hash domain");
    usize::try_from(hash & mask).expect("ASSERT: a masked table index always fits usize")
}

fn key_hashes(chunk_id: ChunkId, logical_length: usize) -> (u64, u64) {
    key_hashes_bytes(chunk_id.bytes(), length_u64(logical_length))
}

fn key_hashes_bytes(chunk_id: [u8; 32], logical_length: u64) -> (u64, u64) {
    let lane0 = read_lane(&chunk_id, 0);
    let lane1 = read_lane(&chunk_id, 8);
    let lane2 = read_lane(&chunk_id, 16);
    let lane3 = read_lane(&chunk_id, 24);
    let first = mix64(lane0 ^ lane2.rotate_left(17) ^ logical_length);
    let second = mix64(lane1 ^ lane3.rotate_left(41) ^ logical_length.rotate_left(29));
    (first, second)
}

fn read_lane(bytes: &[u8; 32], offset: usize) -> u64 {
    let end = offset
        .checked_add(size_of::<u64>())
        .expect("ASSERT: a fixed Chunk ID lane cannot overflow");
    u64::from_le_bytes(
        bytes[offset..end]
            .try_into()
            .expect("ASSERT: a Chunk ID contains four complete u64 lanes"),
    )
}

fn length_u64(logical_length: usize) -> u64 {
    u64::try_from(logical_length)
        .expect("ASSERT: supported targets represent usize logical lengths in u64")
}

const fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn ensure_budget(required_bytes: usize, maximum_bytes: usize) -> Result<(), HintStructureError> {
    if required_bytes > maximum_bytes {
        return Err(HintStructureError::BudgetExceeded {
            required_bytes,
            maximum_bytes,
        });
    }
    Ok(())
}

fn allocate_filled<T: Clone>(count: usize, value: T) -> Result<Box<[T]>, HintStructureError> {
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(map_reserve_error)?;
    values.resize(count, value);
    Ok(values.into_boxed_slice())
}

fn map_reserve_error(_: TryReserveError) -> HintStructureError {
    HintStructureError::AllocationFailed
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::mem::{align_of, size_of};

    use super::*;

    #[test]
    fn bloom_insert_has_no_false_negatives_across_a_deterministic_sweep() {
        const KEY_COUNT: usize = 16_384;
        let mut filter = BlockedBloomHint::new(KEY_COUNT, 256 * 1_024)
            .expect("fixture Bloom capacity fits its explicit budget");
        let inserted = (0..KEY_COUNT)
            .map(|ordinal| {
                let key = fixture_key(
                    u64::try_from(ordinal).expect("fixture ordinal fits the hash seed"),
                );
                let logical_length = 16 * 1_024 + ordinal % (240 * 1_024 + 1);
                filter.insert_hint(key, logical_length);
                assert_eq!(
                    filter.probe_for_exact_lookup(key, logical_length),
                    BloomLookupHint::RequiresExactLookup
                );
                (key, logical_length)
            })
            .collect::<Vec<_>>();

        for (key, logical_length) in inserted {
            assert_eq!(
                filter.probe_for_exact_lookup(key, logical_length),
                BloomLookupHint::RequiresExactLookup
            );
        }
    }

    #[test]
    fn bloom_reports_observed_nonmembers_only_as_non_authoritative_hints() {
        const KEY_COUNT: usize = 4_096;
        let mut filter = BlockedBloomHint::new(KEY_COUNT, 64 * 1_024)
            .expect("fixture Bloom capacity fits its explicit budget");
        for ordinal in 0..KEY_COUNT {
            filter.insert_hint(
                fixture_key(u64::try_from(ordinal).expect("fixture ordinal fits u64")),
                64 * 1_024,
            );
        }

        let definitely_absent = (KEY_COUNT..KEY_COUNT * 2)
            .filter(|ordinal| {
                let hint = filter.probe_for_exact_lookup(
                    fixture_key(u64::try_from(*ordinal).expect("fixture ordinal fits u64")),
                    64 * 1_024,
                );
                matches!(hint, BloomLookupHint::DefinitelyAbsent)
            })
            .count();

        assert!(
            definitely_absent > 0,
            "a bounded deterministic nonmember sweep should observe an absent hint"
        );
        // `RequiresExactLookup` is deliberately not counted as membership: it
        // may be a false positive and only the complete Exact Index may decide.
    }

    #[test]
    fn bloom_key_sets_seven_distinct_bits_in_exactly_one_aligned_block() {
        let mut filter = BlockedBloomHint::new(1_024, 8 * 1_024)
            .expect("fixture Bloom capacity fits its explicit budget");
        assert!(filter.block_count() > 1);
        assert_eq!(size_of::<BloomBlock>(), 64);
        assert_eq!(align_of::<BloomBlock>(), 64);
        assert_eq!(filter.blocks.as_ptr().addr() % 64, 0);

        let key = fixture_key(0xfeed_face_cafe_beef);
        let logical_length = 93_117;
        let before = filter.blocks.to_vec();
        filter.insert_hint(key, logical_length);

        let changed = before
            .iter()
            .zip(filter.blocks.iter())
            .enumerate()
            .filter_map(|(index, (old, new))| (old.words != new.words).then_some(index))
            .collect::<Vec<_>>();
        assert_eq!(changed.len(), 1);
        let changed_bits = filter.blocks[changed[0]]
            .words
            .iter()
            .map(|word| word.count_ones())
            .sum::<u32>();
        assert_eq!(changed_bits, 7);

        let (_, bit_hash) = key_hashes(key, logical_length);
        let mut visited = BTreeSet::new();
        visit_bloom_bits(bit_hash, |word, mask| {
            visited.insert((word, mask));
        });
        assert_eq!(visited.len(), 7);
    }

    #[test]
    fn constructors_reject_zero_insufficient_and_overflowing_capacities() {
        assert_eq!(
            BlockedBloomHint::new(0, usize::MAX).expect_err("zero keys are invalid"),
            HintStructureError::ZeroCapacity
        );
        assert_eq!(
            BlockedBloomHint::new(1, 63).expect_err("one block needs 64 bytes"),
            HintStructureError::BudgetExceeded {
                required_bytes: 64,
                maximum_bytes: 63,
            }
        );
        assert_eq!(
            BlockedBloomHint::new(usize::MAX, usize::MAX)
                .expect_err("Bloom sizing multiplication must be checked"),
            HintStructureError::CapacityOverflow
        );

        assert_eq!(
            PerWorkerLocationHintCache::new(0, usize::MAX)
                .expect_err("zero cache entries are invalid"),
            HintStructureError::ZeroCapacity
        );
        assert_eq!(
            PerWorkerLocationHintCache::new(1, 255).expect_err("one four-way set needs 256 bytes"),
            HintStructureError::BudgetExceeded {
                required_bytes: 256,
                maximum_bytes: 255,
            }
        );
        assert_eq!(
            PerWorkerLocationHintCache::new(usize::MAX, usize::MAX)
                .expect_err("cache byte sizing must be checked"),
            HintStructureError::CapacityOverflow
        );
    }

    #[test]
    fn cache_sets_are_aligned_pointer_free_and_deterministically_four_way() {
        let mut cache = PerWorkerLocationHintCache::new(4, 256)
            .expect("one four-way fixture set fits its budget");
        assert_eq!(cache.set_count(), 1);
        assert_eq!(size_of::<CacheSet>(), 256);
        assert_eq!(align_of::<CacheSet>(), 64);
        assert_eq!(cache.sets.as_ptr().addr() % 64, 0);

        let keys = (0_u64..7).map(fixture_key).collect::<Vec<_>>();
        for (ordinal, key) in keys.iter().take(4).enumerate() {
            cache.remember_unverified(
                *key,
                64 * 1_024,
                UnverifiedLocationHint::new(
                    100 + u64::try_from(ordinal).expect("fixture ordinal fits u64"),
                ),
            );
        }
        for (ordinal, key) in keys.iter().take(4).enumerate() {
            let hint: Option<UnverifiedLocationHint> = cache.probe_unverified(*key, 64 * 1_024);
            assert_eq!(
                hint,
                Some(UnverifiedLocationHint::new(
                    100 + u64::try_from(ordinal).expect("fixture ordinal fits u64")
                ))
            );
        }

        cache.remember_unverified(keys[4], 64 * 1_024, UnverifiedLocationHint::new(104));
        assert_eq!(cache.probe_unverified(keys[0], 64 * 1_024), None);
        cache.remember_unverified(keys[5], 64 * 1_024, UnverifiedLocationHint::new(105));
        assert_eq!(cache.probe_unverified(keys[1], 64 * 1_024), None);

        cache.remember_unverified(keys[3], 64 * 1_024, UnverifiedLocationHint::new(903));
        assert_eq!(
            cache.probe_unverified(keys[3], 64 * 1_024),
            Some(UnverifiedLocationHint::new(903))
        );
        cache.remember_unverified(keys[6], 64 * 1_024, UnverifiedLocationHint::new(106));
        assert_eq!(cache.probe_unverified(keys[2], 64 * 1_024), None);
        assert_eq!(
            cache.probe_unverified(keys[6], 64 * 1_024),
            Some(UnverifiedLocationHint::new(106))
        );
    }

    #[test]
    fn cache_key_includes_logical_length_and_returns_only_unverified_hints() {
        let mut cache =
            PerWorkerLocationHintCache::new(64, 4 * 1_024).expect("fixture cache fits its budget");
        let key = fixture_key(42);
        let unverified = UnverifiedLocationHint::new(7);
        cache.remember_unverified(key, 32 * 1_024, unverified);

        let observed: Option<UnverifiedLocationHint> = cache.probe_unverified(key, 32 * 1_024);
        assert_eq!(observed, Some(unverified));
        assert_eq!(cache.probe_unverified(key, 32 * 1_024 + 1), None);
    }

    fn fixture_key(seed: u64) -> ChunkId {
        let mut bytes = [0_u8; 32];
        let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
        for lane in bytes.chunks_exact_mut(8) {
            state = mix64(state.wrapping_add(0xa076_1d64_78bd_642f));
            lane.copy_from_slice(&state.to_le_bytes());
        }
        ChunkId::of(&bytes)
    }
}
