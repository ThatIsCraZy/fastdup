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
    use std::arch::x86_64::{
        __m256i, _mm256_cmpgt_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
        _mm256_xor_si256, _pdep_u32, _pext_u32,
    };

    validate(config);
    if bytes.len() < config.minimum_bytes {
        return bytes.len();
    }
    let size = bytes.len().min(config.maximum_bytes);
    let mut position = config.minimum_bytes;
    let mut opposing_slopes = 0_u16;
    let mut sequence_length = 0_u16;
    let bias = _mm256_set1_epi8(i8::MIN);

    while position.saturating_add(32) <= size {
        // SAFETY: the loop condition proves that `position..position + 32`
        // lies inside `bytes`. `position` is at least `minimum_bytes`, which
        // is nonzero, so the preceding 32-byte load is also in bounds.
        let (previous, current) = unsafe {
            (
                _mm256_loadu_si256(bytes.as_ptr().add(position - 1).cast::<__m256i>()),
                _mm256_loadu_si256(bytes.as_ptr().add(position).cast::<__m256i>()),
            )
        };
        let previous = _mm256_xor_si256(previous, bias);
        let current = _mm256_xor_si256(current, bias);
        let greater = _mm256_movemask_epi8(_mm256_cmpgt_epi8(current, previous)).cast_unsigned();
        let lesser = _mm256_movemask_epi8(_mm256_cmpgt_epi8(previous, current)).cast_unsigned();
        let non_equal = greater | lesser;

        if non_equal == 0 {
            position += 32;
            continue;
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
        let skip_lane =
            if u32::from(opposing_slopes) + lesser_count >= u32::from(config.skip_trigger) {
                let needed = u32::from(config.skip_trigger - opposing_slopes);
                Some(_pdep_u32(1_u32 << (needed - 1), lesser).trailing_zeros())
            } else {
                None
            };

        match (boundary_lane, skip_lane) {
            (Some(boundary), Some(skip)) if skip < boundary => {
                position = position
                    .saturating_add(skip as usize + 1)
                    .saturating_add(config.skip_bytes);
                opposing_slopes = 0;
                sequence_length = 0;
            }
            (Some(boundary), _) => return position + boundary as usize,
            (None, Some(skip)) => {
                position = position
                    .saturating_add(skip as usize + 1)
                    .saturating_add(config.skip_bytes);
                opposing_slopes = 0;
                sequence_length = 0;
            }
            (None, None) => {
                opposing_slopes += u16::try_from(lesser_count)
                    .expect("ASSERT: one AVX2 vector has at most 32 opposing slopes");
                let packed_mask = u32::try_from(low_bits(packed_count))
                    .expect("ASSERT: one packed AVX2 comparison mask fits u32");
                if packed_greater == packed_mask {
                    sequence_length += u16::try_from(packed_count)
                        .expect("ASSERT: one AVX2 vector has at most 32 increasing slopes");
                } else {
                    let aligned = packed_greater << (32 - packed_count);
                    sequence_length = u16::try_from(aligned.leading_ones())
                        .expect("ASSERT: one AVX2 vector has at most 32 increasing slopes");
                }
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
}
