use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fastdup_format::ContainerId;
use fastdup_store::ContainerStore;

const CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_LOGICAL_MIB: usize = 32;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let artifact_root = arguments
        .next()
        .map_or_else(|| PathBuf::from(".artifacts/bench"), PathBuf::from);
    let logical_mib = arguments
        .next()
        .map(|value| value.to_string_lossy().parse::<usize>())
        .transpose()?
        .unwrap_or(DEFAULT_LOGICAL_MIB);
    if arguments.next().is_some() || logical_mib == 0 || logical_mib > 63 {
        return Err("usage: raw_store_bench [artifact-root] [logical-MiB: 1..=63]".into());
    }

    std::fs::create_dir_all(&artifact_root)?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let run_root = artifact_root.join(format!("raw-v1-{}-{nonce}", std::process::id()));
    std::fs::create_dir(&run_root)?;
    sync_directory(&artifact_root)?;

    let logical_bytes = logical_mib
        .checked_mul(1024 * 1024)
        .ok_or("logical byte count overflow")?;
    let chunks = make_chunks(logical_bytes);
    let chunk_refs = chunks.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let container_id = container_id_from_nonce(nonce)?;

    let baseline_start = Instant::now();
    write_baseline(&run_root, &chunks)?;
    let baseline_elapsed = baseline_start.elapsed();

    let store = ContainerStore::open(run_root.join("containers"))?;
    let publish_start = Instant::now();
    store.publish_raw(container_id, 1, &chunk_refs)?;
    let publish_elapsed = publish_start.elapsed();

    let read_start = Instant::now();
    let recovered = store.read(container_id)?;
    let read_elapsed = read_start.elapsed();
    if recovered.chunk_count() != chunks.len() {
        return Err("verified container returned the wrong chunk count".into());
    }

    let recovery_start = Instant::now();
    let discovered = store.recover_published()?;
    let recovery_elapsed = recovery_start.elapsed();
    if discovered.len() != 1 || discovered[0].header().container_id() != container_id {
        return Err("startup recovery did not discover exactly the published container".into());
    }

    println!("artifact_root={}", run_root.display());
    println!("logical_bytes={logical_bytes}");
    println!("chunk_bytes={CHUNK_BYTES}");
    println!("chunk_count={}", chunks.len());
    print_measurement("baseline_write_sync", logical_bytes, baseline_elapsed);
    print_measurement(
        "container_publish_verify_sync",
        logical_bytes,
        publish_elapsed,
    );
    print_measurement(
        "container_hot_read_full_verify",
        logical_bytes,
        read_elapsed,
    );
    print_measurement(
        "single_container_hot_recovery_full_verify",
        logical_bytes,
        recovery_elapsed,
    );
    Ok(())
}

fn make_chunks(logical_bytes: usize) -> Vec<Vec<u8>> {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    let mut remaining = logical_bytes;
    let mut chunks = Vec::with_capacity(logical_bytes.div_ceil(CHUNK_BYTES));
    while remaining != 0 {
        let length = remaining.min(CHUNK_BYTES);
        let mut chunk = vec![0_u8; length];
        for word in chunk.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            word.copy_from_slice(&state.to_le_bytes()[..word.len()]);
        }
        chunks.push(chunk);
        remaining -= length;
    }
    chunks
}

fn container_id_from_nonce(nonce: u128) -> Result<ContainerId, Box<dyn std::error::Error>> {
    let mut bytes = nonce.to_le_bytes();
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    Ok(ContainerId::new(bytes)?)
}

fn write_baseline(root: &Path, chunks: &[Vec<u8>]) -> io::Result<()> {
    let path = root.join("baseline.raw");
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    for chunk in chunks {
        file.write_all(chunk)?;
    }
    file.sync_all()?;
    sync_directory(root)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn print_measurement(name: &str, bytes: usize, elapsed: std::time::Duration) {
    let seconds = elapsed.as_secs_f64();
    let bytes = u32::try_from(bytes).expect("benchmark payload is capped below u32::MAX");
    let mib_per_second = f64::from(bytes) / (1024.0 * 1024.0) / seconds;
    println!("{name}_seconds={seconds:.6}");
    println!("{name}_mib_per_second={mib_per_second:.2}");
}
