use std::collections::BTreeSet;
use std::env;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_DURATION_SECONDS: usize = 600;
const DEFAULT_VARIANTS: usize = 50;
const DEFAULT_BUFFER_MIB: usize = 4;
const EDITS_PER_VARIANT: usize = 8;
const MIB: usize = 1_024 * 1_024;
const PLAN_SEED: u64 = 0x9e37_79b9_7f4a_7c15;
const RETRY_DELAY: Duration = Duration::from_millis(25);
const CHECKPOINT_SETTLE: Duration = Duration::from_secs(12);
const CLEANUP_RETRY_LIMIT: Duration = Duration::from_secs(30);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse(env::args_os().skip(1))?;
    let source_length = options.source.metadata()?.len();
    if source_length < u64::try_from(EDITS_PER_VARIANT)? {
        return Err("source is too short for the deterministic edit plan".into());
    }
    let plans = Arc::new(edit_plans(source_length, options.variants)?);
    let progress = Arc::new(Progress::default());
    let outcome = Arc::new(Mutex::new(None));
    let started = Instant::now();
    let deadline = started + options.duration;

    thread::scope(|scope| {
        let worker_options = &options;
        let worker_plans = Arc::clone(&plans);
        let worker_progress = Arc::clone(&progress);
        let worker_outcome = Arc::clone(&outcome);
        scope.spawn(move || {
            let result = run_workload(worker_options, &worker_plans, &worker_progress, deadline);
            worker_progress.finished.store(true, Ordering::Release);
            *worker_outcome
                .lock()
                .expect("ASSERT: workload outcome lock poisoned") = Some(result);
        });

        let mut samples = Vec::new();
        let mut previous = CounterSnapshot::default();
        let mut second = 0_u64;
        while !progress.finished.load(Ordering::Acquire) {
            thread::sleep(Duration::from_secs(1));
            second = second
                .checked_add(1)
                .expect("ASSERT: a ten-minute sample count cannot overflow");
            let current = progress.snapshot();
            let sample = RateSample {
                second,
                write_bytes_per_second: current.write_bytes.saturating_sub(previous.write_bytes),
                read_bytes_per_second: current.read_bytes.saturating_sub(previous.read_bytes),
            };
            println!(
                "sample second={} write_Bps={} read_Bps={} written_files={} verified_files={} deleted_files={}",
                sample.second,
                sample.write_bytes_per_second,
                sample.read_bytes_per_second,
                current.written_files,
                current.verified_files,
                current.deleted_files,
            );
            samples.push(sample);
            previous = current;
        }
        *progress
            .samples
            .lock()
            .expect("ASSERT: rate sample lock poisoned") = samples;
    });

    let result = outcome
        .lock()
        .expect("ASSERT: workload outcome lock poisoned")
        .take()
        .expect("ASSERT: workload thread must publish exactly one outcome");
    let elapsed = started.elapsed();
    let final_snapshot = progress.snapshot();
    let samples = progress
        .samples
        .lock()
        .expect("ASSERT: rate sample lock poisoned");
    report(&options, source_length, elapsed, final_snapshot, &samples);
    result?;
    Ok(())
}

#[derive(Debug)]
struct Options {
    mount: PathBuf,
    source: PathBuf,
    duration: Duration,
    variants: usize,
    workers: usize,
    buffer_bytes: usize,
}

impl Options {
    fn parse(
        arguments: impl Iterator<Item = OsString>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut arguments = arguments.peekable();
        let mut duration_seconds = DEFAULT_DURATION_SECONDS;
        let mut variants = DEFAULT_VARIANTS;
        let mut workers = thread::available_parallelism()?.get().min(4);
        let mut buffer_mib = DEFAULT_BUFFER_MIB;
        let mut positional = Vec::new();
        while let Some(argument) = arguments.next() {
            let Some(text) = argument.to_str() else {
                positional.push(PathBuf::from(argument));
                continue;
            };
            match text {
                "--duration-seconds" => {
                    let value = arguments.next();
                    duration_seconds = parse_positive("--duration-seconds", value.as_ref())?;
                }
                "--variants" => {
                    let value = arguments.next();
                    variants = parse_positive("--variants", value.as_ref())?;
                }
                "--workers" => {
                    let value = arguments.next();
                    workers = parse_positive("--workers", value.as_ref())?;
                }
                "--buffer-mib" => {
                    let value = arguments.next();
                    buffer_mib = parse_positive("--buffer-mib", value.as_ref())?;
                }
                _ if text.starts_with('-') => return Err(format!("unknown option {text}").into()),
                _ => positional.push(PathBuf::from(argument)),
            }
        }
        if positional.len() != 2 {
            return Err(USAGE.into());
        }
        let buffer_bytes = buffer_mib
            .checked_mul(MIB)
            .ok_or("buffer size overflows usize")?;
        Ok(Self {
            mount: positional.remove(0),
            source: positional.remove(0),
            duration: Duration::from_secs(u64::try_from(duration_seconds)?),
            variants,
            workers,
            buffer_bytes,
        })
    }
}

const USAGE: &str = "usage: fuse_iso_churn [--duration-seconds N] [--variants N] \
    [--workers N] [--buffer-mib N] MOUNT SOURCE_ISO";

fn parse_positive(
    name: &str,
    value: Option<&OsString>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let text = value
        .as_ref()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("{name} requires a UTF-8 integer"))?;
    let parsed = text.parse::<usize>()?;
    if parsed == 0 {
        return Err(format!("{name} must be nonzero").into());
    }
    Ok(parsed)
}

#[derive(Clone, Copy, Debug)]
struct Edit {
    offset: u64,
    xor: u8,
}

fn edit_plans(source_length: u64, variants: usize) -> io::Result<Vec<Vec<Edit>>> {
    let total = variants
        .checked_mul(EDITS_PER_VARIANT)
        .ok_or_else(|| invalid_data("edit count overflow"))?;
    let mut used = BTreeSet::new();
    let mut state = PLAN_SEED;
    let mut edits = Vec::new();
    edits
        .try_reserve_exact(total)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    while edits.len() < total {
        state = splitmix64(state);
        let offset = state % source_length;
        if !used.insert(offset) {
            continue;
        }
        let xor =
            u8::try_from((state >> 56) | 1).expect("ASSERT: shifted edit byte always fits u8");
        edits.push(Edit { offset, xor });
    }
    let mut plans = Vec::new();
    plans
        .try_reserve_exact(variants)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    for variant in 0..variants {
        let start = variant * EDITS_PER_VARIANT;
        let mut plan = edits[start..start + EDITS_PER_VARIANT].to_vec();
        plan.sort_unstable_by_key(|edit| edit.offset);
        plans.push(plan);
    }
    assert_eq!(
        used.len(),
        total,
        "ASSERT: every deterministic ISO edit offset must be globally unique"
    );
    Ok(plans)
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn run_workload(
    options: &Options,
    plans: &[Vec<Edit>],
    progress: &Progress,
    deadline: Instant,
) -> io::Result<()> {
    if !options.mount.is_dir() || !options.source.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "mount or source ISO is absent",
        ));
    }
    let mut cycle = 0_usize;
    while Instant::now() < deadline {
        let paths = (0..options.variants)
            .map(|variant| {
                options
                    .mount
                    .join(format!("cycle-{cycle:04}-variant-{variant:02}.iso"))
            })
            .collect::<Vec<_>>();
        if paths.iter().any(|path| path.exists()) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "benchmark destination already exists",
            ));
        }
        let workload: io::Result<bool> = (|| {
            let hashes = parallel_write(options, plans, &paths, progress, deadline)?;
            let completed_writes = hashes.iter().filter(|hash| hash.is_some()).count();
            if completed_writes != options.variants {
                return Ok(false);
            }
            let settle_until = Instant::now()
                .checked_add(CHECKPOINT_SETTLE)
                .expect("ASSERT: checkpoint settle deadline cannot overflow");
            while Instant::now() < settle_until.min(deadline) {
                thread::sleep(Duration::from_millis(100));
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            parallel_read(options, &paths, &hashes, progress, deadline)?;
            if progress.verified_files.load(Ordering::Relaxed)
                < (cycle + 1)
                    .checked_mul(options.variants)
                    .expect("ASSERT: verified file goal cannot overflow")
            {
                return Ok(false);
            }
            Ok(true)
        })();
        let cleanup_result = cleanup(&paths, progress);
        cleanup_result?;
        let complete = workload?;
        if !complete {
            break;
        }
        cycle = cycle
            .checked_add(1)
            .expect("ASSERT: ten-minute cycle count cannot overflow");
        progress.completed_cycles.store(cycle, Ordering::Relaxed);
    }
    Ok(())
}

fn parallel_write(
    options: &Options,
    plans: &[Vec<Edit>],
    paths: &[PathBuf],
    progress: &Progress,
    deadline: Instant,
) -> io::Result<Vec<Option<[u8; 32]>>> {
    let next = AtomicUsize::new(0);
    let hashes = (0..options.variants)
        .map(|_| Mutex::new(None))
        .collect::<Vec<_>>();
    let first_error = Mutex::new(None);
    thread::scope(|scope| {
        for _ in 0..options.workers.min(options.variants) {
            scope.spawn(|| {
                loop {
                    if Instant::now() >= deadline
                        || first_error
                            .lock()
                            .expect("ASSERT: write error lock poisoned")
                            .is_some()
                    {
                        return;
                    }
                    let variant = next.fetch_add(1, Ordering::Relaxed);
                    if variant >= options.variants {
                        return;
                    }
                    match write_variant(
                        &options.source,
                        &paths[variant],
                        &plans[variant],
                        options.buffer_bytes,
                        progress,
                        deadline,
                    ) {
                        Ok(Some(hash)) => {
                            *hashes[variant]
                                .lock()
                                .expect("ASSERT: variant hash lock poisoned") = Some(hash);
                            progress.written_files.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(None) => return,
                        Err(error) => {
                            *first_error
                                .lock()
                                .expect("ASSERT: write error lock poisoned") = Some(error);
                            return;
                        }
                    }
                }
            });
        }
    });
    if let Some(error) = first_error
        .into_inner()
        .expect("ASSERT: write error lock poisoned")
    {
        return Err(error);
    }
    Ok(hashes
        .into_iter()
        .map(|hash| {
            hash.into_inner()
                .expect("ASSERT: variant hash lock poisoned")
        })
        .collect())
}

fn write_variant(
    source_path: &Path,
    destination_path: &Path,
    edits: &[Edit],
    buffer_bytes: usize,
    progress: &Progress,
    deadline: Instant,
) -> io::Result<Option<[u8; 32]>> {
    let mut source = File::open(source_path)?;
    let mut destination = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination_path)?;
    let mut buffer = vec![0_u8; buffer_bytes];
    let mut file_offset = 0_u64;
    let mut edit_ordinal = 0_usize;
    let mut hasher = blake3::Hasher::new();
    loop {
        if Instant::now() >= deadline {
            return Ok(None);
        }
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let read_end = file_offset
            .checked_add(u64::try_from(read).map_err(io::Error::other)?)
            .ok_or_else(|| invalid_data("source cursor overflow"))?;
        while edit_ordinal < edits.len() && edits[edit_ordinal].offset < read_end {
            let edit = edits[edit_ordinal];
            if edit.offset < file_offset {
                return Err(invalid_data("edit plan is not strictly ordered"));
            }
            let index = usize::try_from(edit.offset - file_offset).map_err(io::Error::other)?;
            buffer[index] ^= edit.xor;
            edit_ordinal += 1;
        }
        if !write_all_retry(&mut destination, &buffer[..read], deadline)? {
            return Ok(None);
        }
        hasher.update(&buffer[..read]);
        progress.write_bytes.fetch_add(
            u64::try_from(read).map_err(io::Error::other)?,
            Ordering::Relaxed,
        );
        file_offset = read_end;
    }
    if edit_ordinal != edits.len() {
        return Err(invalid_data("edit plan extends beyond the source"));
    }
    Ok(Some(*hasher.finalize().as_bytes()))
}

fn write_all_retry(writer: &mut File, mut bytes: &[u8], deadline: Instant) -> io::Result<bool> {
    while !bytes.is_empty() {
        if Instant::now() >= deadline {
            return Ok(false);
        }
        match writer.write(bytes) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(written) => bytes = &bytes[written..],
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                thread::sleep(RETRY_DELAY);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

fn parallel_read(
    options: &Options,
    paths: &[PathBuf],
    hashes: &[Option<[u8; 32]>],
    progress: &Progress,
    deadline: Instant,
) -> io::Result<()> {
    let next = AtomicUsize::new(0);
    let first_error = Mutex::new(None);
    thread::scope(|scope| {
        for _ in 0..options.workers.min(options.variants) {
            scope.spawn(|| {
                loop {
                    if Instant::now() >= deadline
                        || first_error
                            .lock()
                            .expect("ASSERT: read error lock poisoned")
                            .is_some()
                    {
                        return;
                    }
                    let variant = next.fetch_add(1, Ordering::Relaxed);
                    if variant >= options.variants {
                        return;
                    }
                    let Some(expected) = hashes[variant] else {
                        return;
                    };
                    match read_and_verify(
                        &paths[variant],
                        expected,
                        options.buffer_bytes,
                        progress,
                        deadline,
                    ) {
                        Ok(true) => {
                            progress.verified_files.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(false) => return,
                        Err(error) => {
                            *first_error
                                .lock()
                                .expect("ASSERT: read error lock poisoned") = Some(error);
                            return;
                        }
                    }
                }
            });
        }
    });
    if let Some(error) = first_error
        .into_inner()
        .expect("ASSERT: read error lock poisoned")
    {
        return Err(error);
    }
    Ok(())
}

fn read_and_verify(
    path: &Path,
    expected: [u8; 32],
    buffer_bytes: usize,
    progress: &Progress,
    deadline: Instant,
) -> io::Result<bool> {
    let mut file = File::open(path)?;
    let mut buffer = vec![0_u8; buffer_bytes];
    let mut hasher = blake3::Hasher::new();
    loop {
        if Instant::now() >= deadline {
            return Ok(false);
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        progress.read_bytes.fetch_add(
            u64::try_from(read).map_err(io::Error::other)?,
            Ordering::Relaxed,
        );
    }
    if hasher.finalize().as_bytes() != &expected {
        return Err(invalid_data("byte-exact BLAKE3 restore mismatch"));
    }
    Ok(true)
}

fn cleanup(paths: &[PathBuf], progress: &Progress) -> io::Result<()> {
    for path in paths {
        let started = Instant::now();
        loop {
            match std::fs::remove_file(path) {
                Ok(()) => {
                    progress.deleted_files.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        && started.elapsed() < CLEANUP_RETRY_LIMIT =>
                {
                    thread::sleep(RETRY_DELAY);
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct Progress {
    write_bytes: AtomicU64,
    read_bytes: AtomicU64,
    written_files: AtomicUsize,
    verified_files: AtomicUsize,
    deleted_files: AtomicUsize,
    completed_cycles: AtomicUsize,
    finished: AtomicBool,
    samples: Mutex<Vec<RateSample>>,
}

impl Progress {
    fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            written_files: self.written_files.load(Ordering::Relaxed),
            verified_files: self.verified_files.load(Ordering::Relaxed),
            deleted_files: self.deleted_files.load(Ordering::Relaxed),
            completed_cycles: self.completed_cycles.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct CounterSnapshot {
    write_bytes: u64,
    read_bytes: u64,
    written_files: usize,
    verified_files: usize,
    deleted_files: usize,
    completed_cycles: usize,
}

#[derive(Clone, Copy, Debug)]
struct RateSample {
    second: u64,
    write_bytes_per_second: u64,
    read_bytes_per_second: u64,
}

fn report(
    options: &Options,
    source_length: u64,
    elapsed: Duration,
    final_snapshot: CounterSnapshot,
    samples: &[RateSample],
) {
    let mut write_rates = samples
        .iter()
        .map(|sample| sample.write_bytes_per_second)
        .collect::<Vec<_>>();
    let mut read_rates = samples
        .iter()
        .map(|sample| sample.read_bytes_per_second)
        .collect::<Vec<_>>();
    write_rates.sort_unstable();
    read_rates.sort_unstable();
    let write_active_rates = write_rates
        .iter()
        .copied()
        .filter(|rate| *rate != 0)
        .collect::<Vec<_>>();
    let read_active_rates = read_rates
        .iter()
        .copied()
        .filter(|rate| *rate != 0)
        .collect::<Vec<_>>();
    println!("result=PASS");
    println!("duration_target_seconds={}", options.duration.as_secs());
    println!("elapsed_seconds={:.6}", elapsed.as_secs_f64());
    println!("source_bytes={source_length}");
    println!("planned_variants={}", options.variants);
    println!("edits_per_variant={EDITS_PER_VARIANT}");
    println!("workers={}", options.workers);
    println!("logical_write_bytes={}", final_snapshot.write_bytes);
    println!("logical_read_bytes={}", final_snapshot.read_bytes);
    println!("written_files={}", final_snapshot.written_files);
    println!(
        "byte_exact_verified_files={}",
        final_snapshot.verified_files
    );
    println!("deleted_files={}", final_snapshot.deleted_files);
    println!("completed_cycles={}", final_snapshot.completed_cycles);
    println!(
        "write_peak_Bps={}",
        write_rates.last().copied().unwrap_or(0)
    );
    println!("write_p95_Bps={}", percentile(&write_active_rates, 95));
    println!("write_wall_p95_Bps={}", percentile(&write_rates, 95));
    println!("read_peak_Bps={}", read_rates.last().copied().unwrap_or(0));
    println!("read_p95_Bps={}", percentile(&read_active_rates, 95));
    println!("read_wall_p95_Bps={}", percentile(&read_rates, 95));
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len().saturating_sub(1));
    sorted.get(index).copied().unwrap_or(0)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::{EDITS_PER_VARIANT, edit_plans};

    #[test]
    fn edit_plan_is_bounded_sorted_and_globally_unique() {
        let plans = edit_plans(2_072_444_928, 50).expect("fixture edit plan is valid");
        assert_eq!(plans.len(), 50);
        let mut offsets = Vec::new();
        for plan in plans {
            assert_eq!(plan.len(), EDITS_PER_VARIANT);
            assert!(plan.windows(2).all(|pair| pair[0].offset < pair[1].offset));
            assert!(plan.iter().all(|edit| edit.xor != 0));
            offsets.extend(plan.into_iter().map(|edit| edit.offset));
        }
        offsets.sort_unstable();
        offsets.dedup();
        assert_eq!(offsets.len(), 50 * EDITS_PER_VARIANT);
    }
}
