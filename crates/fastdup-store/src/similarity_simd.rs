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
    std::arch::is_x86_feature_detected!("avx2") || available_avx512()
}

fn available_avx512() -> bool {
    std::arch::is_x86_feature_detected!("avx512f")
        && std::arch::is_x86_feature_detected!("avx512bw")
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
    if available_avx512() {
        // SAFETY: feature detection proves AVX-512F/BW; fixed arrays bound all loads.
        unsafe { update_votes_avx512(votes, words) };
    } else {
        unsafe { update_votes_avx2(votes, words) };
    }
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
    if available_avx512() {
        // SAFETY: AVX-512F/BW detected; equal lengths checked above and kernel bounds every load.
        unsafe { scan_sparse_xor_avx512(base, target, runs, xor_bytes, maximum_bytes) }
    } else {
        unsafe { scan_sparse_xor_avx2(base, target, runs, xor_bytes, maximum_bytes) }
    }
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

#[target_feature(enable = "avx512f,avx512bw")]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn scan_sparse_xor_avx512(
    base: &[u8],
    target: &[u8],
    runs: &mut Vec<(usize, usize)>,
    xor_bytes: &mut Vec<u8>,
    maximum_bytes: usize,
) -> bool {
    use std::arch::x86_64::{__m512i, _mm512_cmpeq_epi8_mask, _mm512_loadu_si512};

    runs.clear();
    xor_bytes.clear();
    let mut cursor = 0_usize;
    let mut run_start = None;
    let mut encoded_bytes = 36_usize;
    if encoded_bytes > maximum_bytes {
        return false;
    }
    while cursor.saturating_add(64) <= target.len() {
        // SAFETY: the loop condition and equal input lengths prove both
        // unaligned 64-byte loads lie inside their respective slices.
        let (base_lane, target_lane) = unsafe {
            (
                _mm512_loadu_si512(base.as_ptr().add(cursor).cast::<__m512i>()),
                _mm512_loadu_si512(target.as_ptr().add(cursor).cast::<__m512i>()),
            )
        };
        let equal_mask = _mm512_cmpeq_epi8_mask(base_lane, target_lane);
        if equal_mask == u64::MAX {
            if let Some(start) = run_start.take() {
                runs.push((start, cursor - start));
            }
            cursor += 64;
            continue;
        }

        let changed = !equal_mask;
        let starts = (changed & !(changed << 1)) & !u64::from(run_start.is_some());
        encoded_bytes += changed.count_ones() as usize + 8 * starts.count_ones() as usize;
        if encoded_bytes > maximum_bytes {
            return false;
        }
        let mut lane = 0;
        while lane < 64 {
            let mask = equal_mask >> lane;
            if mask & 1 != 0 {
                if let Some(start) = run_start.take() {
                    runs.push((start, cursor - start));
                }
                let count = (mask.trailing_ones() as usize).min(64 - lane);
                cursor += count;
                lane += count;
            } else {
                run_start.get_or_insert(cursor);
                let count = (mask.trailing_zeros() as usize).min(64 - lane);
                xor_bytes.extend(
                    base[cursor..cursor + count]
                        .iter()
                        .zip(&target[cursor..cursor + count])
                        .map(|(left, right)| left ^ right),
                );
                cursor += count;
                lane += count;
            }
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

#[target_feature(enable = "avx512f,avx512bw")]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn update_votes_avx512(votes: &mut [i16; 512], words: [u64; 8]) {
    use std::arch::x86_64::{
        __m512i, _mm512_add_epi16, _mm512_loadu_si512, _mm512_mask_blend_epi16, _mm512_set1_epi16,
        _mm512_storeu_si512,
    };
    let minus = _mm512_set1_epi16(-1);
    let plus = _mm512_set1_epi16(1);
    for (word_index, word) in words.into_iter().enumerate() {
        for half in 0..2 {
            let offset = word_index * 64 + half * 32;
            let mask = u32::try_from((word >> (half * 32)) & u64::from(u32::MAX))
                .expect("masked to 32 bits");
            let delta = _mm512_mask_blend_epi16(mask, minus, plus);
            // SAFETY: offset is 0..=480 in steps of 32, so each unaligned
            // 64-byte access is within votes. At most 4096 +/-1 additions
            // are admitted by the fingerprint profile, preventing i16 overflow.
            unsafe {
                let pointer = votes.as_mut_ptr().add(offset).cast::<__m512i>();
                let current = _mm512_loadu_si512(pointer);
                _mm512_storeu_si512(pointer, _mm512_add_epi16(current, delta));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avx512_matches_avx2_for_votes_and_sparse_xor_edges() {
        if !available_avx512() {
            return;
        }
        for seed in [0, u64::MAX, 0x965a_1032_5476_bafe] {
            let words = std::array::from_fn(|i| seed.rotate_left(u32::try_from(i * 7).unwrap()));
            let mut expected = [0i16; 512];
            let mut actual = expected;
            for _ in 0..4096 {
                // SAFETY: this test is feature gated and the arrays have fixed kernel sizes.
                unsafe {
                    update_votes_avx2(&mut expected, words);
                    update_votes_avx512(&mut actual, words);
                }
            }
            assert_eq!(actual, expected);
        }
        for length in [0, 1, 31, 32, 33, 63, 64, 65, 127, 128, 129, 4096, 262144] {
            let base: Vec<u8> = (0..length)
                .map(|i| u8::try_from(i % 251).unwrap())
                .collect();
            for stride in [1, 2, 31, 32, 63, 64, 65, 4096] {
                let mut target = base.clone();
                for i in (0..length).step_by(stride) {
                    target[i] ^= 0xa5;
                }
                for cap in [0, 35, 36, 40, 100, usize::MAX] {
                    let (mut er, mut eb, mut ar, mut ab) = (vec![], vec![], vec![], vec![]);
                    // SAFETY: feature gated, equally sized initialized inputs.
                    let (expected, actual) = unsafe {
                        (
                            scan_sparse_xor_avx2(&base, &target, &mut er, &mut eb, cap),
                            scan_sparse_xor_avx512(&base, &target, &mut ar, &mut ab, cap),
                        )
                    };
                    assert_eq!(
                        actual, expected,
                        "length={length} stride={stride} cap={cap}"
                    );
                    if actual {
                        assert_eq!(ar, er);
                        assert_eq!(ab, eb);
                    }
                }
            }
        }
    }

    #[test]
    #[ignore = "release-mode AVX2/AVX-512 A/B on an AVX-512 host"]
    fn avx512_kernel_benchmark() {
        use std::{hint::black_box, time::Instant};
        if !available_avx512() {
            panic!("AVX-512F/BW host required");
        }
        let words = [0x96a5_1032_5476_bafe; 8];
        for wide in [false, true] {
            let mut samples = vec![];
            for _ in 0..9 {
                let mut votes = [0i16; 512];
                let start = Instant::now();
                for _ in 0..100_000 {
                    votes.fill(0);
                    unsafe {
                        if wide {
                            update_votes_avx512(black_box(&mut votes), black_box(words));
                        } else {
                            update_votes_avx2(black_box(&mut votes), black_box(words));
                        }
                    }
                    black_box(&votes);
                }
                samples.push(start.elapsed().as_nanos() / 100_000);
            }
            samples.sort_unstable();
            eprintln!("votes avx512={wide} ns_per_update={}", samples[4]);
        }
        let base = vec![19u8; 262144];
        let mut target = base.clone();
        for i in (0..target.len()).step_by(4096) {
            target[i] ^= 0xa5;
        }
        for wide in [false, true] {
            let mut samples = vec![];
            let mut runs = vec![];
            let mut bytes = vec![];
            for _ in 0..9 {
                let start = Instant::now();
                for _ in 0..1000 {
                    unsafe {
                        black_box(if wide {
                            scan_sparse_xor_avx512(
                                black_box(&base),
                                black_box(&target),
                                &mut runs,
                                &mut bytes,
                                usize::MAX,
                            )
                        } else {
                            scan_sparse_xor_avx2(
                                black_box(&base),
                                black_box(&target),
                                &mut runs,
                                &mut bytes,
                                usize::MAX,
                            )
                        });
                    }
                }
                samples.push(start.elapsed().as_nanos() / 1000);
            }
            samples.sort_unstable();
            eprintln!("sparse_xor avx512={wide} ns_per_chunk={}", samples[4]);
        }
    }

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
