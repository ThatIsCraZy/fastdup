use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use fastdup_format::ContainerId;
use fastdup_io_uring::{IoUringStorageConfig, IoUringStorageIo};
use fastdup_store::{ContainerRepository, FsStorageIo, StorageIo};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let mode = arguments.next().ok_or("MODE is required: sync or ring")?;
    let root = arguments.next().ok_or("ROOT is required")?;
    let count = parse_usize(arguments.next(), "COUNT")?;
    let workers = parse_usize(arguments.next(), "WORKERS")?;
    let payload_bytes = match arguments.next() {
        Some(value) => parse_usize(Some(value), "PAYLOAD_BYTES")?,
        None => 128 * 1_024,
    };
    if arguments.next().is_some() || count == 0 || workers == 0 || payload_bytes < 8 {
        return Err(concat!(
            "usage: publisher_bench MODE ROOT COUNT WORKERS [PAYLOAD_BYTES]; ",
            "COUNT/WORKERS must be nonzero and PAYLOAD_BYTES at least 8"
        )
        .into());
    }
    let root = Path::new(&root);
    std::fs::create_dir_all(root)?;

    let mode = mode.to_str().ok_or("MODE must be UTF-8")?;
    match mode {
        "sync" => run(
            &FsStorageIo::open(root)?,
            count,
            workers,
            payload_bytes,
            mode,
        )?,
        "ring" => {
            let storage = IoUringStorageIo::open_required(root, IoUringStorageConfig::default())?;
            run(&storage, count, workers, payload_bytes, mode)?;
            let status = storage.status();
            eprintln!(
                concat!(
                    "ring_status submitted={} completed={} root_callers={} ",
                    "root_submissions={} peak_inflight_bytes={} owned_started={} ",
                    "owned_completed={} borrowed_write_copy_bytes={} verifier_workers={} ",
                    "verification_started={} verification_completed={} verification_failed={} ",
                    "peak_active_verifications={}"
                ),
                status.submitted_operations(),
                status.completed_operations(),
                status.root_sync_callers(),
                status.root_sync_submissions(),
                status.peak_inflight_bytes(),
                status.owned_publications_started(),
                status.owned_publications_completed(),
                status.borrowed_write_copy_bytes(),
                status.verifier_workers(),
                status.verification_jobs_started(),
                status.verification_jobs_completed(),
                status.verification_jobs_failed(),
                status.peak_active_verifications(),
            );
        }
        _ => return Err("MODE must be sync or ring".into()),
    }
    Ok(())
}

fn run<I>(
    storage: &I,
    count: usize,
    workers: usize,
    payload_bytes: usize,
    mode: &str,
) -> io::Result<()>
where
    I: StorageIo + Clone + Send + Sync + 'static,
{
    let next = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(workers + 1));
    let mut publishers = Vec::with_capacity(workers);
    for worker_ordinal in 0..workers {
        let storage = I::clone(storage);
        let next = Arc::clone(&next);
        let start = Arc::clone(&start);
        publishers.push(std::thread::spawn(move || -> io::Result<()> {
            let repository = ContainerRepository::new(storage);
            let fill = u8::try_from((worker_ordinal % 251) + 1)
                .expect("ASSERT: bounded benchmark fill fits u8");
            let payload = vec![fill; payload_bytes];
            let chunks: Vec<&[u8]> = payload.chunks(256 * 1_024).collect();
            start.wait();
            loop {
                let ordinal = next.fetch_add(1, Ordering::Relaxed);
                if ordinal >= count {
                    return Ok(());
                }
                let mut id = [0_u8; 16];
                id[..8].copy_from_slice(
                    &u64::try_from(ordinal + 1)
                        .expect("ASSERT: benchmark Container ordinal fits u64")
                        .to_le_bytes(),
                );
                repository
                    .publish_raw(
                        ContainerId::new(id).expect("ASSERT: benchmark Container ID is nonzero"),
                        u64::try_from(ordinal + 1).expect("ASSERT: benchmark generation fits u64"),
                        &chunks,
                    )
                    .map_err(io::Error::other)?;
            }
        }));
    }
    let wall_started = Instant::now();
    start.wait();
    for publisher in publishers {
        publisher
            .join()
            .map_err(|_| io::Error::other("publisher thread panicked"))??;
    }
    let wall = wall_started.elapsed();
    let count_u128 = u128::try_from(count).expect("ASSERT: count fits u128");
    let containers_per_second = count_u128
        .checked_mul(1_000_000_000)
        .expect("ASSERT: benchmark rate numerator cannot overflow")
        / wall.as_nanos().max(1);
    println!(
        "mode={mode} containers={count} workers={} payload_bytes={payload_bytes} wall_ns={} containers_per_second={containers_per_second}",
        NonZeroUsize::new(workers).expect("ASSERT: workers are nonzero"),
        wall.as_nanos(),
    );
    Ok(())
}

fn parse_usize(value: Option<std::ffi::OsString>, name: &str) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("{name} is required"))?
        .to_str()
        .ok_or_else(|| format!("{name} must be UTF-8"))?
        .parse()
        .map_err(|error| format!("invalid {name}: {error}"))
}
