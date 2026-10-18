//! Dry-run deep-repack probe over sampled sealed Containers.
//!
//! Usage: deep_repack_probe <container-or-dir> [more paths...]
//!
//! The probe decodes independent Containers in memory, regroups their
//! verified Chunk payloads, and reports the encoded size at several Zstd
//! levels and region bounds. It writes nothing and never mutates a pool.
//! Containers that need a dependent Base resolver are counted and skipped.

use std::path::{Path, PathBuf};
use std::time::Instant;

use fastdup_format::{ContainerRecoveryEnvelope, FormatError, SealedContainerDescriptor};

const LEVELS: [i32; 4] = [3, 7, 10, 19];
const REGION_512: usize = 512 * 1024;
const REGION_2M: usize = 2 * 1024 * 1024;
const REGION_8M: usize = 8 * 1024 * 1024;
// Serialized per-record header plus per-Chunk table and local-index rows.
const REGION_OVERHEAD_BYTES: u64 = 128;
const CHUNK_OVERHEAD_BYTES: u64 = 64 + 128;

#[derive(Default)]
struct Sample {
    containers: usize,
    skipped: usize,
    current_bytes: u64,
    decoded_bytes: u64,
    chunks: usize,
    // Payloads kept per Container so per-Container and merged-stream
    // variants can both be scored from one decode pass.
    per_container: Vec<Vec<u8>>,
}

fn collect(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(path)
            .expect("read sample directory")
            .map(|entry| entry.expect("sample directory entry").path())
            .filter(|candidate| candidate.extension().is_some_and(|ext| ext == "fdc"))
            .collect();
        entries.sort();
        out.extend(entries);
    } else {
        out.push(path.to_path_buf());
    }
}

fn summarize(paths: &[PathBuf]) {
    let mut files = 0_u64;
    let mut file_bytes = 0_u64;
    let mut raw_records = 0_u64;
    let mut zstd_records = 0_u64;
    let mut dependent_records = 0_u64;
    let mut independent_chunks = 0_u64;
    let mut dependent_chunks = 0_u64;
    let mut raw_encoded = 0_u64;
    let mut zstd_encoded = 0_u64;
    let mut dependent_encoded = 0_u64;
    let mut raw_decoded = 0_u64;
    let mut zstd_decoded = 0_u64;
    let mut dependent_decoded = 0_u64;
    let mut single = 0_u64;
    let mut multi = 0_u64;
    for path in paths {
        let bytes = std::fs::read(path).expect("read sampled container");
        if bytes.len() < 8192 {
            continue;
        }
        let Ok(summary) = SealedContainerDescriptor::decode_intrinsic_summary(
            &bytes[..4096],
            &bytes[bytes.len() - 4096..],
            bytes.len() as u64,
        ) else {
            continue;
        };
        files += 1;
        file_bytes += bytes.len() as u64;
        raw_records += u64::from(summary.raw_record_count());
        zstd_records += u64::from(summary.zstd_record_count());
        dependent_records +=
            u64::from(summary.zstd_prefix_record_count() + summary.sparse_xor_record_count());
        independent_chunks += u64::from(summary.independent_chunk_count());
        dependent_chunks += u64::from(summary.dependent_chunk_count());
        raw_encoded += summary.raw_encoded_bytes();
        zstd_encoded += summary.zstd_encoded_bytes();
        dependent_encoded +=
            summary.zstd_prefix_encoded_bytes() + summary.sparse_xor_encoded_bytes();
        raw_decoded += summary.raw_decoded_bytes();
        zstd_decoded += summary.zstd_decoded_bytes();
        dependent_decoded +=
            summary.zstd_prefix_decoded_bytes() + summary.sparse_xor_decoded_bytes();
        single += u64::from(summary.single_chunk_record_count());
        multi += u64::from(summary.multi_chunk_record_count());
    }
    println!(
        "intrinsic files={files} file_bytes={file_bytes} raw_records={raw_records} \
         zstd_records={zstd_records} dependent_records={dependent_records} \
         independent_chunks={independent_chunks} dependent_chunks={dependent_chunks} \
         raw_encoded={raw_encoded} zstd_encoded={zstd_encoded} \
         dependent_encoded={dependent_encoded} raw_decoded={raw_decoded} \
         zstd_decoded={zstd_decoded} dependent_decoded={dependent_decoded} \
         single_records={single} multi_records={multi}"
    );
}

fn load(paths: &[PathBuf]) -> Sample {
    let mut sample = Sample::default();
    for path in paths {
        let bytes = std::fs::read(path).expect("read sampled container");
        if bytes.len() < 8192 {
            sample.skipped += 1;
            continue;
        }
        let Ok(envelope) = ContainerRecoveryEnvelope::decode(
            &bytes[..4096],
            &bytes[bytes.len() - 4096..],
            bytes.len() as u64,
        ) else {
            sample.skipped += 1;
            continue;
        };
        let Ok(range) = envelope.recovery_index_range() else {
            sample.skipped += 1;
            continue;
        };
        let start = range.offset() as usize;
        let Some(index_bytes) = bytes.get(start..start + range.length()) else {
            sample.skipped += 1;
            continue;
        };
        let Ok(verified) = envelope.verify_recovery_index(index_bytes) else {
            sample.skipped += 1;
            continue;
        };
        // One candidate per physical Record, in file order. Dependent
        // Records are skipped by failing their Base resolution; independent
        // Records decode without any resolver call.
        let mut records = std::collections::BTreeMap::new();
        for candidate in verified.candidates() {
            if let Ok(record) = candidate.record_range() {
                records
                    .entry((record.offset(), record.length()))
                    .or_insert(candidate);
            }
        }
        let mut payload = Vec::new();
        let mut current = 0_u64;
        let mut chunks = 0_usize;
        for ((offset, length), candidate) in records {
            let Some(record) = bytes.get(offset as usize..offset as usize + length) else {
                continue;
            };
            let Ok(decoded) =
                verified.decode_candidate_with_resolver(candidate, record, &mut |_| {
                    Err(FormatError::DependentBaseRequired)
                })
            else {
                continue;
            };
            current += length as u64;
            for chunk in decoded {
                payload.extend_from_slice(chunk.as_slice());
                chunks += 1;
            }
        }
        if chunks == 0 {
            sample.skipped += 1;
            continue;
        }
        sample.containers += 1;
        sample.chunks += chunks;
        sample.current_bytes += current + chunks as u64 * CHUNK_OVERHEAD_BYTES;
        sample.decoded_bytes += payload.len() as u64;
        sample.per_container.push(payload);
    }
    sample
}

fn encoded_size(decoded: &[u8], region_bytes: usize, level: i32) -> u64 {
    let mut total = 0_u64;
    for region in decoded.chunks(region_bytes) {
        let compressed = zstd::bulk::compress(region, level).expect("zstd probe compression");
        // Production keeps RAW unless an encoding wins; mirror that bound.
        total += compressed.len().min(region.len()) as u64 + REGION_OVERHEAD_BYTES;
    }
    total
}

fn report(label: &str, sample: &Sample, streams: &[&[u8]], region_bytes: usize, level: i32) {
    let started = Instant::now();
    let mut encoded = sample.chunks as u64 * CHUNK_OVERHEAD_BYTES;
    for stream in streams {
        encoded += encoded_size(stream, region_bytes, level);
    }
    let seconds = started.elapsed().as_secs_f64();
    let ratio = encoded as f64 / sample.decoded_bytes.max(1) as f64;
    let versus = 100.0 * (encoded as f64 / sample.current_bytes.max(1) as f64 - 1.0);
    println!(
        "variant={label} region={region_bytes} level={level} encoded={encoded} \
         ratio={ratio:.4} vs_current_percent={versus:+.2} encode_seconds={seconds:.2}"
    );
}

fn main() {
    let mut paths = Vec::new();
    for argument in std::env::args_os().skip(1) {
        collect(Path::new(&argument), &mut paths);
    }
    assert!(!paths.is_empty(), "at least one container path is required");
    summarize(&paths);
    let sample = load(&paths);
    println!(
        "sample containers={} skipped_dependent={} chunks={} decoded={} current={}",
        sample.containers,
        sample.skipped,
        sample.chunks,
        sample.decoded_bytes,
        sample.current_bytes
    );
    assert!(sample.containers > 0, "sample needs independent containers");

    let per_container: Vec<&[u8]> = sample.per_container.iter().map(Vec::as_slice).collect();
    for level in LEVELS {
        report("per-container", &sample, &per_container, REGION_512, level);
    }

    let mut merged = Vec::with_capacity(sample.decoded_bytes as usize);
    for payload in &sample.per_container {
        merged.extend_from_slice(payload);
    }
    for level in LEVELS {
        report("merged-stream", &sample, &[&merged], REGION_512, level);
    }
    for region in [REGION_2M, REGION_8M] {
        for level in [3, 19] {
            report("merged-stream", &sample, &[&merged], region, level);
        }
    }
}
