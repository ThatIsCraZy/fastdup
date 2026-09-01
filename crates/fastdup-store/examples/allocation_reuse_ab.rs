//! Focused A/B benchmark for hot-path allocation ownership and buffer reuse.

use std::hint::black_box;
use std::time::{Duration, Instant};

use fastdup_format::RawRecord;

const SAMPLES: usize = 9;

fn main() {
    let mut arguments = std::env::args().skip(1);
    if let Some(kernel) = arguments.next() {
        let mode = arguments
            .next()
            .unwrap_or_else(|| panic!("MODE is required after {kernel}"));
        assert!(arguments.next().is_none(), "unexpected benchmark argument");
        perf_kernel(&kernel, &mode);
        return;
    }
    for length in [128 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        receive_buffer(length);
    }
    publication_samples();
    for length in [128 * 1024, 256 * 1024] {
        verified_payload(length);
    }
    for length in [128 * 1024, 1024 * 1024] {
        committed_reply(length);
    }
    for length in [4 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024] {
        aligned_image(length);
    }
}

fn perf_kernel(kernel: &str, mode: &str) {
    let reuse = match mode {
        "baseline" => false,
        "reuse" => true,
        _ => panic!("MODE must be baseline or reuse"),
    };
    match kernel {
        "receive" => perf_receive(reuse),
        "sample" => perf_sample(reuse),
        "payload" => perf_payload(reuse),
        "reply" => perf_reply(reuse),
        "image" => perf_image(reuse),
        _ => panic!("KERNEL must be receive, sample, payload, reply, or image"),
    }
}

fn perf_receive(reuse: bool) {
    const LENGTH: usize = 1024 * 1024;
    const ITERATIONS: usize = 4096;
    let mut pooled = vec![0_u8; LENGTH];
    for _ in 0..ITERATIONS {
        if reuse {
            simulate_kernel_overwrite(&mut pooled);
            black_box(pooled[LENGTH / 2]);
        } else {
            let mut bytes = vec![0_u8; LENGTH];
            simulate_kernel_overwrite(&mut bytes);
            black_box(bytes);
        }
    }
}

fn perf_sample(reuse: bool) {
    const LENGTH: usize = 4096;
    const ITERATIONS: usize = 4_000_000;
    let source = fixture(LENGTH);
    let mut pooled = vec![0_u8; LENGTH];
    for _ in 0..ITERATIONS {
        for _ in 0..3 {
            if reuse {
                pooled.copy_from_slice(black_box(&source));
                black_box(pooled[LENGTH / 2]);
            } else {
                let mut bytes = vec![0_u8; LENGTH];
                bytes.copy_from_slice(black_box(&source));
                black_box(bytes);
            }
        }
    }
}

fn perf_payload(reuse: bool) {
    const LENGTH: usize = 256 * 1024;
    const ITERATIONS: usize = 8192;
    let encoded = RawRecord::encode(&fixture(LENGTH)).expect("benchmark RAW Record encodes");
    for _ in 0..ITERATIONS {
        let record = RawRecord::decode(black_box(&encoded)).expect("benchmark Record decodes");
        if reuse {
            black_box(record.into_payload());
        } else {
            black_box(record.payload().to_vec());
        }
    }
}

fn perf_reply(reuse: bool) {
    const LENGTH: usize = 1024 * 1024;
    const ITERATIONS: usize = 4096;
    let source = fixture(LENGTH);
    for _ in 0..ITERATIONS {
        let committed = black_box(&source).clone();
        if reuse {
            black_box(committed);
        } else {
            let mut reply = vec![0_u8; committed.len()];
            reply.copy_from_slice(&committed);
            black_box(reply);
        }
    }
}

fn perf_image(reuse: bool) {
    const LENGTH: usize = 32 * 1024 * 1024;
    const ITERATIONS: usize = 128;
    let body = fixture(LENGTH - 2 * 4096);
    let mut pooled = Vec::with_capacity(LENGTH + 4095);
    for _ in 0..ITERATIONS {
        if reuse {
            pooled = build_aligned(std::mem::take(&mut pooled), black_box(&body), LENGTH);
            black_box(pooled.len());
        } else {
            black_box(build_aligned(Vec::new(), black_box(&body), LENGTH));
        }
    }
}

fn receive_buffer(length: usize) {
    let iterations = (1024 * 1024 * 1024 / length).max(32);
    let mut pooled = vec![0_u8; length];
    let (fresh, reused) = alternating(
        || {
            let mut bytes = vec![0_u8; length];
            simulate_kernel_overwrite(&mut bytes);
            black_box(bytes)
        },
        || {
            simulate_kernel_overwrite(&mut pooled);
            black_box(pooled[black_box(length / 2)])
        },
        iterations,
    );
    report("fuse-receive", length, iterations, fresh, reused);
}

fn publication_samples() {
    const LENGTH: usize = 4096;
    const READS: usize = 3;
    let source = fixture(LENGTH);
    let iterations = 2_000_000;
    let mut pooled = vec![0_u8; LENGTH];
    let (fresh, reused) = alternating(
        || {
            for _ in 0..READS {
                let mut bytes = vec![0_u8; LENGTH];
                bytes.copy_from_slice(black_box(&source));
                black_box(bytes);
            }
        },
        || {
            for _ in 0..READS {
                pooled.copy_from_slice(black_box(&source));
                black_box(pooled[LENGTH / 2]);
            }
        },
        iterations,
    );
    report(
        "publication-three-samples",
        LENGTH * READS,
        iterations,
        fresh,
        reused,
    );
}

fn verified_payload(length: usize) {
    let payload = fixture(length);
    let encoded = RawRecord::encode(&payload).expect("benchmark RAW Record encodes");
    let iterations = (512 * 1024 * 1024 / length).max(64);
    let (copied, moved) = alternating(
        || {
            let record = RawRecord::decode(black_box(&encoded)).expect("benchmark Record decodes");
            black_box(record.payload().to_vec())
        },
        || {
            let record = RawRecord::decode(black_box(&encoded)).expect("benchmark Record decodes");
            black_box(record.into_payload())
        },
        iterations,
    );
    report("verified-payload", length, iterations, copied, moved);
}

fn committed_reply(length: usize) {
    let source = fixture(length);
    let iterations = (1024 * 1024 * 1024 / length).max(64);
    let (double, direct) = alternating(
        || {
            let committed = black_box(&source).clone();
            let mut reply = vec![0_u8; committed.len()];
            reply.copy_from_slice(&committed);
            black_box(reply)
        },
        || {
            let committed = black_box(&source).clone();
            black_box(committed)
        },
        iterations,
    );
    report("committed-reply", length, iterations, double, direct);
}

fn aligned_image(length: usize) {
    const PAGE: usize = 4096;
    let body = fixture(length - 2 * PAGE);
    let iterations = (1024 * 1024 * 1024 / length).max(16);
    let mut pooled = Vec::with_capacity(length + PAGE - 1);
    let (fresh, reused) = alternating(
        || black_box(build_aligned(Vec::new(), black_box(&body), length)),
        || {
            pooled = build_aligned(std::mem::take(&mut pooled), black_box(&body), length);
            black_box(pooled.len())
        },
        iterations,
    );
    report("aligned-container-image", length, iterations, fresh, reused);
}

fn build_aligned(mut allocation: Vec<u8>, body: &[u8], length: usize) -> Vec<u8> {
    const PAGE: usize = 4096;
    allocation.clear();
    if allocation.capacity() < length + PAGE - 1 {
        allocation.reserve_exact(length + PAGE - 1);
    }
    let start = (PAGE - allocation.as_ptr().addr() % PAGE) % PAGE;
    allocation.resize(start + PAGE, 0);
    allocation.extend_from_slice(body);
    allocation.resize(start + length, 0);
    allocation
}

fn simulate_kernel_overwrite(bytes: &mut [u8]) {
    for (ordinal, chunk) in bytes.chunks_mut(4096).enumerate() {
        chunk.fill(u8::try_from(ordinal % 251).expect("bounded page ordinal fits u8"));
    }
}

fn alternating<A, B, T, U>(
    mut baseline: A,
    mut challenger: B,
    iterations: usize,
) -> (Duration, Duration)
where
    A: FnMut() -> T,
    B: FnMut() -> U,
{
    for _ in 0..2 {
        black_box(baseline());
        black_box(challenger());
    }
    let mut baseline_samples = Vec::with_capacity(SAMPLES);
    let mut challenger_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        if sample % 2 == 0 {
            baseline_samples.push(measure(&mut baseline, iterations));
            challenger_samples.push(measure(&mut challenger, iterations));
        } else {
            challenger_samples.push(measure(&mut challenger, iterations));
            baseline_samples.push(measure(&mut baseline, iterations));
        }
    }
    baseline_samples.sort_unstable();
    challenger_samples.sort_unstable();
    (
        baseline_samples[SAMPLES / 2],
        challenger_samples[SAMPLES / 2],
    )
}

fn measure<T>(operation: &mut impl FnMut() -> T, iterations: usize) -> Duration {
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(operation());
    }
    started.elapsed()
}

fn report(name: &str, bytes: usize, iterations: usize, baseline: Duration, challenger: Duration) {
    println!(
        "kernel={name} bytes={bytes} iterations={iterations} baseline_ns={} reuse_ns={} speedup={:.3}x",
        baseline.as_nanos() / iterations as u128,
        challenger.as_nanos() / iterations as u128,
        baseline.as_secs_f64() / challenger.as_secs_f64(),
    );
}

fn fixture(length: usize) -> Vec<u8> {
    let mut state = 0x243f_6a88_85a3_08d3_u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}
