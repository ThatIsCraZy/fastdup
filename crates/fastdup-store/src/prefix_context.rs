//! Worker-local Zstd Prefix context. Only lifetime erasure after a complete
//! native reset is unsafe; callers lend Base bytes for one synchronous call.
//! A/B evidence: docs/benchmarks/hotpath-implementation-2026-09-05.md.
#![allow(unsafe_code)]

use crate::reduction_prefix::ZstdPrefixError;
use std::cell::RefCell;
use zstd::zstd_safe::{CCtx, CParameter, ResetDirective};

thread_local! {
    static ENCODER: RefCell<Option<CCtx<'static>>> = const { RefCell::new(None) };
}

pub(crate) fn compress(
    base: &[u8],
    target: &[u8],
    output: &mut [u8],
    level: i32,
) -> Result<Option<usize>, ZstdPrefixError> {
    let context = ENCODER
        .with(|slot| slot.borrow_mut().take())
        .or_else(CCtx::try_create)
        .ok_or(ZstdPrefixError::AllocationFailed)?;
    compress_using(context, base, target, output, level)
}

fn compress_using<'a>(
    mut context: CCtx<'a>,
    base: &'a [u8],
    target: &[u8],
    output: &mut [u8],
    level: i32,
) -> Result<Option<usize>, ZstdPrefixError> {
    let result = (|| {
        context
            .set_parameter(CParameter::CompressionLevel(level))
            .map_err(|_| ZstdPrefixError::CompressionFailed)?;
        context
            .set_parameter(CParameter::NbWorkers(0))
            .map_err(|_| ZstdPrefixError::CompressionFailed)?;
        context
            .set_pledged_src_size(Some(
                u64::try_from(target.len()).map_err(|_| ZstdPrefixError::ArithmeticOverflow)?,
            ))
            .map_err(|_| ZstdPrefixError::CompressionFailed)?;
        context
            .ref_prefix(base)
            .map_err(|_| ZstdPrefixError::CompressionFailed)?;
        match context.compress2(output, target) {
            Ok(written) => Ok(Some(written)),
            Err(error)
                if zstd::zstd_safe::get_error_name(error) == "Destination buffer is too small" =>
            {
                Ok(None)
            }
            Err(_) => Err(ZstdPrefixError::CompressionFailed),
        }
    })();
    // A failed compression may leave the prefix attached. Reset on *every*
    // outcome before Base can die. On reset failure, drop instead of pooling.
    if context.reset(ResetDirective::SessionAndParameters).is_ok() {
        // SAFETY: ZSTD_reset_session_and_parameters discards the active frame,
        // all parameters, dictionaries and borrowed prefix references. NbWorkers
        // is zero, so no worker can retain the caller's Base/output. CCtx's sole
        // lifetime parameter describes those now-absent borrowed dictionaries;
        // erasing it changes neither representation nor ownership. The unique
        // context is moved into one thread-local slot, never aliased. Unwind or
        // reset failure drops it while Base is still alive. No borrowed buffer
        // or pointer is returned to the caller or pool.
        let detached = unsafe { std::mem::transmute::<CCtx<'a>, CCtx<'static>>(context) };
        ENCODER.with(|slot| *slot.borrow_mut() = Some(detached));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hint::black_box;
    use std::time::Instant;

    fn fixture(length: usize, mut state: u64) -> Vec<u8> {
        (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect()
    }

    fn fresh(base: &[u8], target: &[u8], out: &mut [u8]) -> Option<usize> {
        let mut ctx = CCtx::create();
        ctx.set_parameter(CParameter::CompressionLevel(3)).unwrap();
        ctx.set_parameter(CParameter::NbWorkers(0)).unwrap();
        ctx.set_pledged_src_size(Some(u64::try_from(target.len()).unwrap()))
            .unwrap();
        ctx.ref_prefix(base).unwrap();
        ctx.compress2(out, target).ok()
    }

    #[test]
    fn reused_prefix_context_detaches_rejected_and_short_lived_bases() {
        for generation in 1..25 {
            // Each Base allocation dies before the next pool checkout.
            let base = fixture(16 * 1024, generation);
            let mut target = base.clone();
            target[31] ^= 0x63;
            target[4096] ^= 31;
            for cap in [1, 32, 128, target.len()] {
                let mut expected = vec![0; cap];
                let mut actual = vec![0; cap];
                let old = fresh(&base, &target, &mut expected);
                let new = compress(&base, &target, &mut actual, 3).unwrap();
                assert_eq!(old, new);
                if let Some(length) = new {
                    assert_eq!(expected[..length], actual[..length]);
                    let mut dc = zstd::zstd_safe::DCtx::create();
                    dc.ref_prefix(&base).unwrap();
                    let mut restored = vec![0; target.len()];
                    assert_eq!(
                        dc.decompress(&mut restored[..], &actual[..length]).unwrap(),
                        target.len()
                    );
                    assert_eq!(restored, target);
                }
            }
        }
    }

    #[test]
    #[ignore = "release-mode A/B evidence for the scoped Prefix lifetime adapter"]
    fn prefix_context_reuse_ab() {
        for length in [16 * 1024, 64 * 1024, 256 * 1024] {
            let base = fixture(length, 77);
            let mut target = base.clone();
            for offset in (0..length).step_by(4096) {
                target[offset] ^= 0x63;
            }
            let mut samples = [Vec::new(), Vec::new()];
            let rounds = 512_u32;
            for sample in 0..11 {
                for side in 0..2 {
                    let mode = (sample + side) % 2;
                    let started = Instant::now();
                    for _ in 0..rounds {
                        let mut out = vec![0; length - 32];
                        let size = if mode == 0 {
                            fresh(black_box(&base), black_box(&target), &mut out)
                        } else {
                            compress(black_box(&base), black_box(&target), &mut out, 3).unwrap()
                        };
                        black_box((out, size));
                    }
                    samples[mode].push(started.elapsed().as_secs_f64() * 1e9 / f64::from(rounds));
                }
            }
            for sample in &mut samples {
                sample.sort_by(f64::total_cmp);
            }
            eprintln!(
                "prefix-context bytes={length} fresh_ns={:.1} reused_ns={:.1} speedup={:.3}",
                samples[0][5],
                samples[1][5],
                samples[0][5] / samples[1][5]
            );
        }
    }
}
