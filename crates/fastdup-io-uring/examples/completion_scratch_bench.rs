//! Allocation A/B for the bounded CQE materialization seam.

use std::hint::black_box;
use std::time::{Duration, Instant};

const RING_ENTRIES: usize = 256;
const ROUNDS: u32 = 1_000_000;
const SAMPLES: usize = 9;

fn main() {
    let source = (0..RING_ENTRIES)
        .map(|index| {
            (
                u64::try_from(index).expect("ring index fits u64") + 1,
                i32::try_from(index).expect("ring index fits i32"),
            )
        })
        .collect::<Vec<_>>();
    for completed in [1, 8, 64, RING_ENTRIES] {
        let collect = median(|| collect_each_round(&source[..completed]));
        let reuse = median(|| reuse_ring_scratch(&source[..completed]));
        println!(
            "completion_scratch cqes_per_reap={completed} rounds={ROUNDS} collect_ns_per_reap={:.3} reuse_ns_per_reap={:.3} speedup={:.3}x",
            collect.as_secs_f64() * 1e9 / f64::from(ROUNDS),
            reuse.as_secs_f64() * 1e9 / f64::from(ROUNDS),
            collect.as_secs_f64() / reuse.as_secs_f64(),
        );
    }
}

fn collect_each_round(source: &[(u64, i32)]) -> Duration {
    let started = Instant::now();
    for _ in 0..ROUNDS {
        let completions = black_box(source)
            .iter()
            .map(|entry| (black_box(entry.0), entry.1))
            .collect::<Vec<_>>();
        black_box(completions);
    }
    started.elapsed()
}

fn reuse_ring_scratch(source: &[(u64, i32)]) -> Duration {
    let mut scratch = Vec::with_capacity(RING_ENTRIES);
    let started = Instant::now();
    for _ in 0..ROUNDS {
        scratch.clear();
        scratch.extend(
            black_box(source)
                .iter()
                .map(|entry| (black_box(entry.0), entry.1)),
        );
        black_box(scratch.as_slice());
    }
    started.elapsed()
}

fn median(mut benchmark: impl FnMut() -> Duration) -> Duration {
    let mut samples = (0..SAMPLES).map(|_| benchmark()).collect::<Vec<_>>();
    samples.sort_unstable();
    samples[SAMPLES / 2]
}
