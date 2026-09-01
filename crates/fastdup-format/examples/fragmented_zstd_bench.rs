use std::hint::black_box;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::{Duration, Instant};

use fastdup_format::{
    ChunkId, ContainerId, IncompressibilityGatePolicy, PrehashedChunk, PrehashedContiguousRegion,
    SealedContainer,
};

const REGION_BYTES: usize = 512 * 1_024;
const CHUNK_BYTES: usize = 64 * 1_024;
const SAMPLES: usize = 9;
const ITERATIONS: usize = 31;

struct Part {
    chunk_id: ChunkId,
    bytes: Vec<u8>,
}

fn main() {
    benchmark("compressible", &compressible_fixture());
    benchmark("incompressible", &incompressible_fixture());
    if let Some(path) = std::env::args_os().nth(1) {
        benchmark_file(Path::new(&path));
    }
}

fn benchmark_file(path: &Path) {
    let mut file = std::fs::File::open(path).expect("open benchmark input");
    let length = file.metadata().expect("stat benchmark input").len();
    let region_bytes = u64::try_from(REGION_BYTES).expect("region length fits u64");
    assert!(
        length >= region_bytes,
        "benchmark input is at least one region"
    );
    for (label, numerator) in [
        ("file-quarter", 1_u64),
        ("file-half", 2),
        ("file-three-quarter", 3),
    ] {
        let offset = (length / 4 * numerator).min(length - region_bytes);
        file.seek(SeekFrom::Start(offset))
            .expect("seek benchmark input");
        let mut input = vec![0_u8; REGION_BYTES];
        file.read_exact(&mut input).expect("read benchmark region");
        benchmark(label, &parts_from_bytes(&input));
    }
}

fn benchmark(label: &str, parts: &[Part]) {
    let borrowed = encode_borrowed(parts);
    let materialized = encode_materialized(parts);
    let borrowed_decoded = SealedContainer::decode(borrowed.bytes()).expect("borrowed verifies");
    let materialized_decoded =
        SealedContainer::decode(materialized.bytes()).expect("materialized verifies");
    assert_eq!(borrowed_decoded.records(), materialized_decoded.records());

    let mut borrowed_samples = Vec::with_capacity(SAMPLES);
    let mut materialized_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        if sample % 2 == 0 {
            materialized_samples.push(measure(|| encode_materialized(parts)));
            borrowed_samples.push(measure(|| encode_borrowed(parts)));
        } else {
            borrowed_samples.push(measure(|| encode_borrowed(parts)));
            materialized_samples.push(measure(|| encode_materialized(parts)));
        }
    }
    let borrowed = median(borrowed_samples);
    let materialized = median(materialized_samples);
    println!(
        "fixture={label} region_bytes={REGION_BYTES} chunks={} samples={SAMPLES} iterations={} materialized_ns={} borrowed_stream_ns={} speedup={:.3}x",
        parts.len(),
        ITERATIONS,
        materialized.as_nanos() / ITERATIONS as u128,
        borrowed.as_nanos() / ITERATIONS as u128,
        materialized.as_secs_f64() / borrowed.as_secs_f64(),
    );
}

fn measure(mut operation: impl FnMut() -> fastdup_format::AdaptiveContainerEncoding) -> Duration {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        black_box(operation());
    }
    started.elapsed()
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[SAMPLES / 2]
}

fn encode_borrowed(parts: &[Part]) -> fastdup_format::AdaptiveContainerEncoding {
    let chunks = parts
        .iter()
        .map(|part| PrehashedChunk::new(part.chunk_id, &part.bytes))
        .collect::<Vec<_>>();
    SealedContainer::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
        ContainerId::new([0xB1; 16]).expect("benchmark Container ID is nonzero"),
        1,
        &[&chunks],
        NonZeroUsize::MIN,
        IncompressibilityGatePolicy::Off,
    )
    .expect("encode borrowed benchmark region")
}

fn encode_materialized(parts: &[Part]) -> fastdup_format::AdaptiveContainerEncoding {
    let mut decoded = Vec::with_capacity(REGION_BYTES);
    for part in parts {
        decoded.extend_from_slice(&part.bytes);
    }
    let mut offset = 0_usize;
    let chunks = parts
        .iter()
        .map(|part| {
            let end = offset + part.bytes.len();
            let chunk = PrehashedChunk::new(part.chunk_id, &decoded[offset..end]);
            offset = end;
            chunk
        })
        .collect::<Vec<_>>();
    let region = PrehashedContiguousRegion::new(&chunks, &decoded)
        .expect("benchmark chunks partition materialized region");
    SealedContainer::encode_prehashed_contiguous_regions_parallel_profiled_with_gate(
        ContainerId::new([0xB1; 16]).expect("benchmark Container ID is nonzero"),
        1,
        &[region],
        NonZeroUsize::MIN,
        IncompressibilityGatePolicy::Off,
    )
    .expect("encode materialized benchmark region")
}

fn compressible_fixture() -> Vec<Part> {
    (0..REGION_BYTES / CHUNK_BYTES)
        .map(|ordinal| {
            let bytes = (0..CHUNK_BYTES)
                .map(|index| {
                    b'A' + u8::try_from((index / 97 + ordinal) % 19).expect("fixture byte fits u8")
                })
                .collect::<Vec<_>>();
            Part {
                chunk_id: ChunkId::of(&bytes),
                bytes,
            }
        })
        .collect()
}

fn incompressible_fixture() -> Vec<Part> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..REGION_BYTES / CHUNK_BYTES)
        .map(|_| {
            let bytes = (0..CHUNK_BYTES)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state.to_le_bytes()[0]
                })
                .collect::<Vec<_>>();
            Part {
                chunk_id: ChunkId::of(&bytes),
                bytes,
            }
        })
        .collect()
}

fn parts_from_bytes(bytes: &[u8]) -> Vec<Part> {
    bytes
        .chunks(CHUNK_BYTES)
        .map(|bytes| Part {
            chunk_id: ChunkId::of(bytes),
            bytes: bytes.to_vec(),
        })
        .collect()
}
