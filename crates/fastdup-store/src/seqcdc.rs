//! Scalar reference and AVX2/BMI2 `SeqCDC` boundary scanner.

#![allow(unsafe_code)]

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeqCdcConfig {
    pub sequence_length: u16,
    pub skip_trigger: u16,
    pub skip_bytes: usize,
    pub minimum_bytes: usize,
    pub maximum_bytes: usize,
}

#[must_use]
pub fn seqcdc_cut(bytes: &[u8], config: SeqCdcConfig) -> usize {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("bmi2") {
        // SAFETY: runtime feature detection establishes AVX2 and BMI2 support.
        // The kernel uses unaligned loads only after proving that 32 bytes remain.
        return unsafe { cut_avx2_bmi2(bytes, config) };
    }
    seqcdc_cut_scalar(bytes, config)
}

#[must_use]
pub fn seqcdc_cut_scalar(bytes: &[u8], config: SeqCdcConfig) -> usize {
    validate(config);
    if bytes.len() < config.minimum_bytes {
        return bytes.len();
    }
    let size = bytes.len().min(config.maximum_bytes);
    cut_scalar_from(bytes, size, config, config.minimum_bytes, 0, 0)
}

/// Finds one `SeqCDC` boundary without joining consecutive input slices.
///
/// `total_bytes` must equal the sum of all segment lengths. Segment boundaries
/// do not reset `SeqCDC` state and therefore cannot affect the returned cut.
#[must_use]
pub fn seqcdc_cut_segmented<'a, I>(segments: I, total_bytes: usize, config: SeqCdcConfig) -> usize
where
    I: IntoIterator<Item = &'a [u8]>,
{
    #[cfg(target_arch = "x86_64")]
    let vector =
        std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("bmi2");
    #[cfg(not(target_arch = "x86_64"))]
    let vector = false;
    cut_segmented(segments, total_bytes, config, vector)
}

/// Scalar oracle for [`seqcdc_cut_segmented`].
#[must_use]
pub fn seqcdc_cut_segmented_scalar<'a, I>(
    segments: I,
    total_bytes: usize,
    config: SeqCdcConfig,
) -> usize
where
    I: IntoIterator<Item = &'a [u8]>,
{
    cut_segmented(segments, total_bytes, config, false)
}

#[allow(clippy::too_many_lines)]
fn cut_segmented<'a, I>(
    segments: I,
    total_bytes: usize,
    config: SeqCdcConfig,
    vector: bool,
) -> usize
where
    I: IntoIterator<Item = &'a [u8]>,
{
    #[cfg(not(target_arch = "x86_64"))]
    let _ = vector;
    validate(config);
    if total_bytes < config.minimum_bytes {
        return total_bytes;
    }
    let size = total_bytes.min(config.maximum_bytes);
    let mut position = config.minimum_bytes;
    let mut opposing_slopes = 0_u16;
    let mut sequence_length = 0_u16;
    let mut segment_start = 0_usize;
    let mut previous_segment_byte = None;

    for segment in segments {
        let segment_end = segment_start
            .checked_add(segment.len())
            .expect("ASSERT: bounded SeqCDC segment positions cannot overflow");
        if position >= segment_end {
            previous_segment_byte = segment.last().copied().or(previous_segment_byte);
            segment_start = segment_end;
            continue;
        }
        assert!(
            position >= segment_start,
            "ASSERT: segmented SeqCDC input has no gaps"
        );
        let mut local = position - segment_start;
        let mut previous = if local == 0 {
            previous_segment_byte.expect("ASSERT: SeqCDC minimum owns a preceding byte")
        } else {
            segment[local - 1]
        };

        while position < segment_end && position < size {
            #[cfg(target_arch = "x86_64")]
            if vector
                && local != 0
                && local.saturating_add(32) <= segment.len()
                && position.saturating_add(32) <= size
            {
                // SAFETY: runtime feature detection selected `vector`, and
                // the conditions above prove both unaligned loads are inside
                // this segment.
                let decision = unsafe {
                    classify_avx2_bmi2(
                        segment.as_ptr().add(local - 1),
                        segment.as_ptr().add(local),
                        config,
                        opposing_slopes,
                        sequence_length,
                    )
                };
                match decision {
                    VectorDecision::Boundary(lane) => return position + lane,
                    VectorDecision::Skip(lane) => {
                        position = position
                            .saturating_add(lane + 1)
                            .saturating_add(config.skip_bytes);
                        opposing_slopes = 0;
                        sequence_length = 0;
                        if position >= size {
                            return size;
                        }
                        if position >= segment_end {
                            break;
                        }
                        local = position - segment_start;
                        previous = segment[local - 1];
                    }
                    VectorDecision::Advance { opposing, sequence } => {
                        opposing_slopes = opposing;
                        sequence_length = sequence;
                        position += 32;
                        local += 32;
                        previous = segment[local - 1];
                    }
                }
                continue;
            }

            let current = segment[local];
            position += 1;
            local += 1;
            if current != previous {
                if current < previous {
                    opposing_slopes += 1;
                    sequence_length = 0;
                } else {
                    sequence_length += 1;
                }
                if sequence_length == config.sequence_length {
                    return position - 1;
                }
                if opposing_slopes == config.skip_trigger {
                    position = position.saturating_add(config.skip_bytes);
                    opposing_slopes = 0;
                    sequence_length = 0;
                    if position >= size {
                        return size;
                    }
                    if position >= segment_end {
                        break;
                    }
                    local = position - segment_start;
                    previous = segment[local - 1];
                    continue;
                }
            }
            previous = current;
        }
        if position >= size {
            return size;
        }
        previous_segment_byte = segment.last().copied().or(previous_segment_byte);
        segment_start = segment_end;
    }
    assert!(
        segment_start >= size,
        "ASSERT: segmented SeqCDC input covers its declared byte length"
    );
    size
}

fn cut_scalar_from(
    bytes: &[u8],
    size: usize,
    config: SeqCdcConfig,
    mut position: usize,
    mut opposing_slopes: u16,
    mut sequence_length: u16,
) -> usize {
    while position < size {
        let previous = bytes[position - 1];
        let current = bytes[position];
        position += 1;
        if current == previous {
            continue;
        }
        if current < previous {
            opposing_slopes += 1;
            sequence_length = 0;
        } else {
            sequence_length += 1;
        }
        if sequence_length == config.sequence_length {
            return position - 1;
        }
        if opposing_slopes == config.skip_trigger {
            position = position.saturating_add(config.skip_bytes);
            opposing_slopes = 0;
        }
    }
    size
}

fn validate(config: SeqCdcConfig) {
    assert!(config.sequence_length != 0 && config.sequence_length <= 32);
    assert!(config.skip_trigger != 0);
    assert!(config.skip_bytes != 0);
    assert!(config.minimum_bytes != 0);
    assert!(config.minimum_bytes <= config.maximum_bytes);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi2")]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn cut_avx2_bmi2(bytes: &[u8], config: SeqCdcConfig) -> usize {
    validate(config);
    if bytes.len() < config.minimum_bytes {
        return bytes.len();
    }
    let size = bytes.len().min(config.maximum_bytes);
    let mut position = config.minimum_bytes;
    let mut opposing_slopes = 0_u16;
    let mut sequence_length = 0_u16;

    while position.saturating_add(32) <= size {
        // SAFETY: the loop condition proves that `position..position + 32`
        // lies inside `bytes`. `position` is at least `minimum_bytes`, which
        // is nonzero, so the preceding 32-byte load is also in bounds.
        // SAFETY: the loop condition proves both unaligned loads are inside
        // `bytes`, and this function requires AVX2 plus BMI2.
        let decision = unsafe {
            classify_avx2_bmi2(
                bytes.as_ptr().add(position - 1),
                bytes.as_ptr().add(position),
                config,
                opposing_slopes,
                sequence_length,
            )
        };
        match decision {
            VectorDecision::Boundary(lane) => return position + lane,
            VectorDecision::Skip(lane) => {
                position = position
                    .saturating_add(lane + 1)
                    .saturating_add(config.skip_bytes);
                opposing_slopes = 0;
                sequence_length = 0;
            }
            VectorDecision::Advance { opposing, sequence } => {
                opposing_slopes = opposing;
                sequence_length = sequence;
                position += 32;
            }
        }
    }

    cut_scalar_from(
        bytes,
        size,
        config,
        position,
        opposing_slopes,
        sequence_length,
    )
}

#[cfg(target_arch = "x86_64")]
enum VectorDecision {
    Boundary(usize),
    Skip(usize),
    Advance { opposing: u16, sequence: u16 },
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi2")]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn classify_avx2_bmi2(
    previous: *const u8,
    current: *const u8,
    config: SeqCdcConfig,
    opposing_slopes: u16,
    sequence_length: u16,
) -> VectorDecision {
    use std::arch::x86_64::{
        __m256i, _mm256_cmpgt_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
        _mm256_xor_si256, _pdep_u32, _pext_u32,
    };

    // SAFETY: callers prove that both pointers address 32 readable bytes.
    let (previous, current) = unsafe {
        (
            _mm256_loadu_si256(previous.cast::<__m256i>()),
            _mm256_loadu_si256(current.cast::<__m256i>()),
        )
    };
    let bias = _mm256_set1_epi8(i8::MIN);
    let previous = _mm256_xor_si256(previous, bias);
    let current = _mm256_xor_si256(current, bias);
    let greater = _mm256_movemask_epi8(_mm256_cmpgt_epi8(current, previous)).cast_unsigned();
    let lesser = _mm256_movemask_epi8(_mm256_cmpgt_epi8(previous, current)).cast_unsigned();
    let non_equal = greater | lesser;
    if non_equal == 0 {
        return VectorDecision::Advance {
            opposing: opposing_slopes,
            sequence: sequence_length,
        };
    }

    let packed_greater = _pext_u32(greater, non_equal);
    let packed_count = non_equal.count_ones();
    let prefix = u32::from(sequence_length);
    let combined = (u64::from(packed_greater) << prefix) | low_bits(prefix);
    let mut run_ends = combined;
    for shift in 1..u32::from(config.sequence_length) {
        run_ends &= combined << shift;
    }
    let boundary_lane = if run_ends == 0 {
        None
    } else {
        let packed_end = run_ends.trailing_zeros() - prefix;
        let packed_bit = 1_u32 << packed_end;
        Some(_pdep_u32(packed_bit, non_equal).trailing_zeros())
    };

    let lesser_count = lesser.count_ones();
    let skip_lane = if u32::from(opposing_slopes) + lesser_count >= u32::from(config.skip_trigger) {
        let needed = u32::from(config.skip_trigger - opposing_slopes);
        Some(_pdep_u32(1_u32 << (needed - 1), lesser).trailing_zeros())
    } else {
        None
    };
    match (boundary_lane, skip_lane) {
        (Some(boundary), Some(skip)) if skip < boundary => VectorDecision::Skip(skip as usize),
        (Some(boundary), _) => VectorDecision::Boundary(boundary as usize),
        (None, Some(skip)) => VectorDecision::Skip(skip as usize),
        (None, None) => {
            let opposing = opposing_slopes
                + u16::try_from(lesser_count)
                    .expect("ASSERT: one AVX2 vector has at most 32 opposing slopes");
            let packed_mask = u32::try_from(low_bits(packed_count))
                .expect("ASSERT: one packed AVX2 comparison mask fits u32");
            let sequence = if packed_greater == packed_mask {
                sequence_length
                    + u16::try_from(packed_count)
                        .expect("ASSERT: one AVX2 vector has at most 32 increasing slopes")
            } else {
                let aligned = packed_greater << (32 - packed_count);
                u16::try_from(aligned.leading_ones())
                    .expect("ASSERT: one AVX2 vector has at most 32 increasing slopes")
            };
            VectorDecision::Advance { opposing, sequence }
        }
    }
}

const fn low_bits(bits: u32) -> u64 {
    if bits == 0 { 0 } else { (1_u64 << bits) - 1 }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: SeqCdcConfig = SeqCdcConfig {
        sequence_length: 6,
        skip_trigger: 50,
        skip_bytes: 1_024,
        minimum_bytes: 16 * 1_024,
        maximum_bytes: 256 * 1_024,
    };

    #[test]
    fn dispatcher_matches_scalar_on_random_and_low_entropy_inputs() {
        let mut state = 0x243f_6a88_85a3_08d3_u64;
        for ordinal in 0..128 {
            let mut bytes = vec![0_u8; CONFIG.maximum_bytes + 4_096 + ordinal];
            for byte in &mut bytes {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state.to_le_bytes()[0];
            }
            if ordinal % 3 == 0 {
                bytes[17_000..80_000].fill(0);
            }
            if ordinal % 5 == 0 {
                for (index, byte) in bytes[90_000..140_000].iter_mut().enumerate() {
                    *byte = u8::try_from((index / 97) % 251).expect("fixture byte fits u8");
                }
            }
            assert_eq!(
                seqcdc_cut(&bytes, CONFIG),
                seqcdc_cut_scalar(&bytes, CONFIG),
                "fixture ordinal {ordinal}"
            );
        }
    }

    #[test]
    fn dispatcher_matches_scalar_across_a_complete_stream() {
        let mut state = 0x1319_8a2e_0370_7344_u64;
        let mut bytes = vec![0_u8; 16 * 1_024 * 1_024 + 137];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        let mut scalar_offset = 0_usize;
        let mut vector_offset = 0_usize;
        while scalar_offset < bytes.len() {
            scalar_offset += seqcdc_cut_scalar(&bytes[scalar_offset..], CONFIG);
            vector_offset += seqcdc_cut(&bytes[vector_offset..], CONFIG);
            assert_eq!(vector_offset, scalar_offset);
        }
    }

    #[test]
    fn segmented_dispatcher_matches_contiguous_for_hostile_splits() {
        let assert_matches = |bytes: &[u8]| {
            let expected = seqcdc_cut_scalar(bytes, CONFIG);
            assert_eq!(seqcdc_cut(bytes, CONFIG), expected);
            for widths in [
                &[1_usize][..],
                &[31][..],
                &[32][..],
                &[33][..],
                &[4_095, 1, 32, 4_096, 17, 65_537][..],
                &[CONFIG.minimum_bytes - 1, 1, 31, 32, 33][..],
            ] {
                let mut segments = Vec::new();
                let mut offset = 0_usize;
                let mut ordinal = 0_usize;
                while offset < bytes.len() {
                    let end = offset
                        .saturating_add(widths[ordinal % widths.len()])
                        .min(bytes.len());
                    segments.push(&bytes[offset..end]);
                    offset = end;
                    ordinal += 1;
                }
                assert_eq!(
                    seqcdc_cut_segmented_scalar(segments.iter().copied(), bytes.len(), CONFIG),
                    expected,
                    "scalar segmented widths {widths:?}"
                );
                assert_eq!(
                    seqcdc_cut_segmented(segments.iter().copied(), bytes.len(), CONFIG),
                    expected,
                    "dispatched segmented widths {widths:?}"
                );
            }
        };

        let mut state = 0xa409_3822_299f_31d0_u64;
        let mut bytes = vec![0_u8; CONFIG.maximum_bytes + 8_193];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        bytes[17_003..65_537].fill(0);
        for (index, byte) in bytes[131_063..196_619].iter_mut().enumerate() {
            *byte = u8::try_from((index / 113) % 251).expect("fixture byte fits u8");
        }
        assert_matches(&bytes);

        let mut skip_heavy = vec![0_u8; CONFIG.maximum_bytes + 8_193];
        for (index, byte) in skip_heavy.iter_mut().enumerate() {
            *byte = 250_u8
                .checked_sub(u8::try_from(index % 251).expect("fixture byte fits u8"))
                .expect("fixture subtraction stays non-negative");
        }
        assert_eq!(seqcdc_cut_scalar(&skip_heavy, CONFIG), CONFIG.maximum_bytes);
        assert_matches(&skip_heavy);
    }
}
