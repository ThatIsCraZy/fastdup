//! Production RAW Container-writer benchmark for assembly changes.

use std::hint::black_box;
use std::time::{Duration, Instant};

use fastdup_format::{ContainerId, SealedContainer};

const SAMPLES: usize = 9;
const CHUNK_BYTES: usize = 256 * 1024;

fn main() {
    for logical_bytes in [128 * 1024, 4 * 1024 * 1024, 32 * 1024 * 1024] {
        benchmark(logical_bytes);
    }
}

fn benchmark(logical_bytes: usize) {
    let payload = fixture(logical_bytes);
    let chunks = payload.chunks(CHUNK_BYTES).collect::<Vec<_>>();
    let iterations = (512 * 1024 * 1024 / logical_bytes).max(8);

    let image = SealedContainer::encode_with_writer_evidence(
        ContainerId::new([0xa5; 16]).expect("benchmark identity is nonzero"),
        1,
        &chunks,
    )
    .expect("benchmark fixture encodes");
    SealedContainer::decode(image.bytes()).expect("benchmark writer image verifies");

    for _ in 0..2 {
        run(&chunks, iterations / 8 + 1);
    }
    let mut samples = (0..SAMPLES)
        .map(|_| run(&chunks, iterations))
        .collect::<Vec<_>>();
    samples.sort_unstable();
    let elapsed = samples[SAMPLES / 2];
    let seconds = elapsed.as_secs_f64();
    let logical_bytes_f64 =
        f64::from(u32::try_from(logical_bytes).expect("benchmark logical length fits u32"));
    let iterations_f64 =
        f64::from(u32::try_from(iterations).expect("benchmark iteration count fits u32"));
    let mib = logical_bytes_f64 * iterations_f64 / (1024.0 * 1024.0);
    println!(
        "container-assembly logical_kib={} image_kib={} iterations={} median_ms={:.3} ns_per_image={:.1} logical_mib_s={:.1}",
        logical_bytes / 1024,
        image.bytes().len() / 1024,
        iterations,
        seconds * 1_000.0,
        seconds * 1e9 / iterations_f64,
        mib / seconds,
    );
}

fn run(chunks: &[&[u8]], iterations: usize) -> Duration {
    let started = Instant::now();
    for generation in 0..iterations {
        let encoded = SealedContainer::encode_with_writer_evidence(
            ContainerId::new([0xa5; 16]).expect("benchmark identity is nonzero"),
            generation as u64 + 1,
            black_box(chunks),
        )
        .expect("benchmark encode succeeds");
        black_box(encoded.bytes().len());
    }
    started.elapsed()
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
