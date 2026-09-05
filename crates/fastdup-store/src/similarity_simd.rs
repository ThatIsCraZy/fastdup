//! x86-64 SIMD kernels for deterministic Similarity fingerprinting.

#![allow(unsafe_code)]

#[repr(C, align(16))]
#[derive(Clone, Copy)]
struct VoteDeltas([i16; 8]);

#[repr(C, align(32))]
struct VoteDeltaTable([VoteDeltas; 256]);

const VOTE_DELTAS: VoteDeltaTable = VoteDeltaTable(build_vote_deltas());

const fn build_vote_deltas() -> [VoteDeltas; 256] {
    let mut table = [VoteDeltas([0; 8]); 256];
    let mut byte = 0_usize;
    while byte < table.len() {
        let mut bit = 0_usize;
        while bit < 8 {
            table[byte].0[bit] = if byte & (1 << bit) == 0 { -1 } else { 1 };
            bit += 1;
        }
        byte += 1;
    }
    table
}

#[must_use]
pub(crate) fn available() -> bool {
    std::arch::is_x86_feature_detected!("avx2")
}

/// Adds the 512 sign votes represented by eight 64-bit words.
///
/// The safe seam is callable only after [`available`] selected AVX2. Durable
/// fingerprint semantics remain defined by the scalar implementation in the
/// parent module.
pub(crate) fn update_votes(votes: &mut [i16; 512], words: [u64; 8]) {
    assert!(
        available(),
        "ASSERT: Similarity AVX2 dispatch is feature-gated"
    );
    // SAFETY: runtime detection above establishes AVX2. The kernel accesses
    // exactly 512 initialized i16 votes and immutable aligned table entries.
    // Profile v1 admits at most 4096 additions, checked by its accumulator.
    unsafe { update_votes_avx2(votes, words) };
}

/// Returns false as soon as the canonical payload cannot fit the cost cap.
/// The caller discards partial output on rejection.
pub(crate) fn scan_sparse_xor_bounded(
    base: &[u8],
    target: &[u8],
    runs: &mut Vec<(usize, usize)>,
    xor_bytes: &mut Vec<u8>,
    maximum_bytes: usize,
) -> bool {
    assert!(
        available(),
        "ASSERT: sparse-XOR AVX2 dispatch is feature-gated"
    );
    assert_eq!(
        base.len(),
        target.len(),
        "ASSERT: sparse-XOR AVX2 inputs have equal lengths"
    );
    // SAFETY: runtime detection above establishes AVX2. The kernel bounds
    // every unaligned load by the equal input lengths and uses safe Vec pushes
    // for all emitted run and payload bytes.
    unsafe { scan_sparse_xor_avx2(base, target, runs, xor_bytes, maximum_bytes) }
}

#[target_feature(enable = "avx2")]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn scan_sparse_xor_avx2(
    base: &[u8],
    target: &[u8],
    runs: &mut Vec<(usize, usize)>,
    xor_bytes: &mut Vec<u8>,
    maximum_bytes: usize,
) -> bool {
    use std::arch::x86_64::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8};

    runs.clear();
    xor_bytes.clear();
    let mut cursor = 0_usize;
    let mut run_start = None;
    let mut encoded_bytes = 36_usize;
    if encoded_bytes > maximum_bytes {
        return false;
    }
    while cursor.saturating_add(32) <= target.len() {
        // SAFETY: the loop condition and equal input lengths prove both
        // unaligned 32-byte loads lie inside their respective slices.
        let (base_lane, target_lane) = unsafe {
            (
                _mm256_loadu_si256(base.as_ptr().add(cursor).cast::<__m256i>()),
                _mm256_loadu_si256(target.as_ptr().add(cursor).cast::<__m256i>()),
            )
        };
        let equal_mask =
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(base_lane, target_lane)).cast_unsigned();
        if equal_mask == u32::MAX {
            if let Some(start) = run_start.take() {
                runs.push((start, cursor - start));
            }
            cursor += 32;
            continue;
        }

        let changed = !equal_mask;
        let starts = (changed & !(changed << 1)) & !u32::from(run_start.is_some());
        encoded_bytes += changed.count_ones() as usize + 8 * starts.count_ones() as usize;
        if encoded_bytes > maximum_bytes {
            return false;
        }
        for lane in 0..32 {
            if equal_mask & (1_u32 << lane) == 0 {
                run_start.get_or_insert(cursor);
                xor_bytes.push(base[cursor] ^ target[cursor]);
            } else if let Some(start) = run_start.take() {
                runs.push((start, cursor - start));
            }
            cursor += 1;
        }
    }
    while cursor < target.len() {
        if base[cursor] != target[cursor] {
            encoded_bytes += 1 + 8 * usize::from(run_start.is_none());
            if encoded_bytes > maximum_bytes {
                return false;
            }
            run_start.get_or_insert(cursor);
            xor_bytes.push(base[cursor] ^ target[cursor]);
        } else if let Some(start) = run_start.take() {
            runs.push((start, cursor - start));
        }
        cursor += 1;
    }
    if let Some(start) = run_start {
        runs.push((start, cursor - start));
    }
    true
}

#[target_feature(enable = "avx2")]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn update_votes_avx2(votes: &mut [i16; 512], words: [u64; 8]) {
    use std::arch::x86_64::{
        __m128i, __m256i, _mm_load_si128, _mm256_add_epi16, _mm256_loadu_si256, _mm256_set_m128i,
        _mm256_storeu_si256,
    };

    for (word_ordinal, word) in words.into_iter().enumerate() {
        let word_bytes = word.to_le_bytes();
        for (pair_ordinal, pair) in word_bytes.chunks_exact(2).enumerate() {
            let vote_offset = word_ordinal * 64 + pair_ordinal * 16;
            // SAFETY: both loop bounds prove the vote range and table entry
            // contain sixteen i16 votes and two eight-i16 delta rows. Each
            // VoteDeltas row is 16-byte aligned. No load touches a neighboring
            // row, and the caller's v1 bound prevents signed vote overflow.
            unsafe {
                let current = _mm256_loadu_si256(votes.as_ptr().add(vote_offset).cast::<__m256i>());
                let low = _mm_load_si128(
                    VOTE_DELTAS.0[usize::from(pair[0])]
                        .0
                        .as_ptr()
                        .cast::<__m128i>(),
                );
                let high = _mm_load_si128(
                    VOTE_DELTAS.0[usize::from(pair[1])]
                        .0
                        .as_ptr()
                        .cast::<__m128i>(),
                );
                let delta = _mm256_set_m128i(high, low);
                _mm256_storeu_si256(
                    votes.as_mut_ptr().add(vote_offset).cast::<__m256i>(),
                    _mm256_add_epi16(current, delta),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn narrow_votes_match_i32_oracle_at_both_profile_extremes() {
        if !available() {
            return;
        }
        for words in [[0; 8], [u64::MAX; 8], [0x96a5_0123_fedc_ba78; 8]] {
            let mut votes = [0_i16; 512];
            let mut oracle = [0_i32; 512];
            for _ in 0..4096 {
                update_votes(&mut votes, words);
                for (ordinal, word) in words.iter().enumerate() {
                    for bit in 0..64 {
                        oracle[ordinal * 64 + bit] += if word & (1 << bit) == 0 { -1 } else { 1 };
                    }
                }
            }
            assert_eq!(votes.map(i32::from), oracle);
        }
    }

    #[test]
    fn vote_delta_table_maps_low_to_high_bits_to_minus_or_plus_one() {
        for byte in 0_u8..=u8::MAX {
            for bit in 0..8 {
                assert_eq!(
                    VOTE_DELTAS.0[usize::from(byte)].0[bit],
                    if byte & (1 << bit) == 0 { -1 } else { 1 }
                );
            }
        }
    }
}
