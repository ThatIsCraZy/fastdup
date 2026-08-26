use std::env;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use fastdup_appliance::request_online_gc_now;

const DEFAULT_ITERATIONS: usize = 20;
const DEFAULT_GC_INTERVAL_MILLISECONDS: u64 = 1_000;
const COPY_BUFFER_BYTES: usize = 4 * 1_024 * 1_024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse(env::args_os().skip(1))?;
    let source_bytes = options.source.metadata()?.len();
    if source_bytes == 0 {
        return Err("SOURCE must be nonempty".into());
    }

    let run_id = format!("{}", std::process::id());
    let mut created = Vec::with_capacity(options.iterations.saturating_mul(2));
    let baseline = run_phase(&options, &run_id, "baseline", &mut created, None)?;

    let stop = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(AtomicU64::new(0));
    let responses = Arc::new(Mutex::new(Vec::new()));
    let gc_worker = {
        let metadata_root = options.metadata_root.clone();
        let stop = Arc::clone(&stop);
        let requests = Arc::clone(&requests);
        let responses = Arc::clone(&responses);
        let interval = options.gc_interval;
        thread::Builder::new()
            .name("fastdup-gc-interference-control".to_owned())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let result = request_online_gc_now(&metadata_root);
                    requests.fetch_add(1, Ordering::Relaxed);
                    responses
                        .lock()
                        .expect("ASSERT: Online-GC response collection lock poisoned")
                        .push(result);
                    wait_for_stop(&stop, interval);
                }
            })?
    };
    let with_gc = run_phase(&options, &run_id, "online-gc", &mut created, Some(&stop));
    stop.store(true, Ordering::Release);
    gc_worker
        .join()
        .map_err(|_| "Online-GC control worker panicked")?;
    let with_gc = with_gc?;

    let responses = responses
        .lock()
        .expect("ASSERT: Online-GC response collection lock poisoned");
    let gc_errors = responses
        .iter()
        .filter(|response| response.is_err())
        .count();
    let gc_successes = responses
        .iter()
        .filter(|response| {
            response
                .as_ref()
                .is_ok_and(|line| line.starts_with("online_gc_ok=true "))
        })
        .count();

    let baseline_p99 = percentile(&baseline, 99);
    let gc_p99 = percentile(&with_gc, 99);
    let regression_basis_points = relative_basis_points(baseline_p99, gc_p99);
    println!("result=PASS");
    println!("source_bytes={source_bytes}");
    println!("iterations={}", options.iterations);
    println!("online_gc_interval_ms={}", options.gc_interval.as_millis());
    println!("baseline_p50_us={}", percentile(&baseline, 50).as_micros());
    println!("baseline_p99_us={}", baseline_p99.as_micros());
    println!("baseline_max_us={}", maximum(&baseline).as_micros());
    println!("online_gc_p50_us={}", percentile(&with_gc, 50).as_micros());
    println!("online_gc_p99_us={}", gc_p99.as_micros());
    println!("online_gc_max_us={}", maximum(&with_gc).as_micros());
    println!("p99_regression_basis_points={regression_basis_points}");
    println!("online_gc_requests={}", requests.load(Ordering::Relaxed));
    println!("online_gc_successes={gc_successes}");
    println!("online_gc_errors={gc_errors}");

    for path in created {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }

    if gc_errors != 0 || gc_successes == 0 {
        return Err("Online-GC control did not complete every request successfully".into());
    }
    if let Some(limit) = options.maximum_p99_regression_basis_points
        && regression_basis_points > limit
    {
        return Err(format!(
            "p99 regression {regression_basis_points} basis points exceeds limit {limit}"
        )
        .into());
    }
    Ok(())
}

fn run_phase(
    options: &Options,
    run_id: &str,
    phase: &str,
    created: &mut Vec<PathBuf>,
    stop: Option<&AtomicBool>,
) -> io::Result<Vec<Duration>> {
    let mut latencies = Vec::with_capacity(options.iterations);
    for ordinal in 0..options.iterations {
        if stop.is_some_and(|stop| stop.load(Ordering::Acquire)) {
            return Err(io::Error::other(
                "Online-GC worker stopped before the measured phase completed",
            ));
        }
        let destination = options.mount.join(format!(
            ".fastdup-online-gc-interference-{run_id}-{phase}-{ordinal:04}"
        ));
        let started = Instant::now();
        copy_and_sync(&options.source, &destination)?;
        latencies.push(started.elapsed());
        created.push(destination);
    }
    Ok(latencies)
}

fn copy_and_sync(source: &Path, destination: &Path) -> io::Result<()> {
    let source = File::open(source)?;
    let destination = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut reader = BufReader::with_capacity(COPY_BUFFER_BYTES, source);
    let mut writer = BufWriter::with_capacity(COPY_BUFFER_BYTES, destination);
    io::copy(&mut reader, &mut writer)?;
    writer.flush()?;
    writer.get_ref().sync_all()
}

#[derive(Debug)]
struct Options {
    mount: PathBuf,
    metadata_root: PathBuf,
    source: PathBuf,
    iterations: usize,
    gc_interval: Duration,
    maximum_p99_regression_basis_points: Option<u64>,
}

impl Options {
    fn parse(
        arguments: impl Iterator<Item = OsString>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut arguments = arguments.peekable();
        let mut iterations = DEFAULT_ITERATIONS;
        let mut gc_interval_milliseconds = DEFAULT_GC_INTERVAL_MILLISECONDS;
        let mut maximum_p99_regression_basis_points = None;
        let mut positional = Vec::new();
        while let Some(argument) = arguments.next() {
            match argument.to_str() {
                Some("--iterations") => {
                    iterations = parse_nonzero("--iterations", arguments.next().as_ref())?;
                }
                Some("--maximum-p99-regression-percent") => {
                    let percent = parse_u64(
                        "--maximum-p99-regression-percent",
                        arguments.next().as_ref(),
                    )?;
                    maximum_p99_regression_basis_points = Some(
                        percent
                            .checked_mul(100)
                            .ok_or("p99 regression limit overflows u64")?,
                    );
                }
                Some("--gc-interval-ms") => {
                    gc_interval_milliseconds =
                        parse_u64("--gc-interval-ms", arguments.next().as_ref())?;
                    if gc_interval_milliseconds == 0 {
                        return Err("--gc-interval-ms must be nonzero".into());
                    }
                }
                Some(value) if value.starts_with('-') => {
                    return Err(format!("unknown option {value}").into());
                }
                _ => positional.push(PathBuf::from(argument)),
            }
        }
        if positional.len() != 3 {
            return Err(USAGE.into());
        }
        Ok(Self {
            mount: positional.remove(0),
            metadata_root: positional.remove(0),
            source: positional.remove(0),
            iterations,
            gc_interval: Duration::from_millis(gc_interval_milliseconds),
            maximum_p99_regression_basis_points,
        })
    }
}

const USAGE: &str = "usage: online_gc_interference [--iterations N] \
    [--gc-interval-ms N] [--maximum-p99-regression-percent N] \
    MOUNT METADATA_ROOT SOURCE";

fn wait_for_stop(stop: &AtomicBool, duration: Duration) {
    let deadline = Instant::now() + duration;
    while !stop.load(Ordering::Acquire) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn parse_nonzero(
    name: &str,
    value: Option<&OsString>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let parsed = value
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("{name} requires a UTF-8 integer"))?
        .parse::<usize>()?;
    if parsed == 0 {
        return Err(format!("{name} must be nonzero").into());
    }
    Ok(parsed)
}

fn parse_u64(name: &str, value: Option<&OsString>) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(value
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("{name} requires a UTF-8 integer"))?
        .parse::<u64>()?)
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len().saturating_sub(1));
    sorted.get(index).copied().unwrap_or_default()
}

fn maximum(samples: &[Duration]) -> Duration {
    samples.iter().copied().max().unwrap_or_default()
}

fn relative_basis_points(baseline: Duration, observed: Duration) -> u64 {
    let baseline = baseline.as_nanos();
    let observed = observed.as_nanos();
    if baseline == 0 || observed <= baseline {
        return 0;
    }
    let increase = observed - baseline;
    u64::try_from(increase.saturating_mul(10_000) / baseline).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{percentile, relative_basis_points};
    use std::time::Duration;

    #[test]
    fn percentile_and_regression_are_deterministic() {
        let samples = (1..=100).map(Duration::from_millis).collect::<Vec<_>>();
        assert_eq!(percentile(&samples, 50), Duration::from_millis(50));
        assert_eq!(percentile(&samples, 99), Duration::from_millis(99));
        assert_eq!(
            relative_basis_points(Duration::from_secs(2), Duration::from_secs(3)),
            5_000
        );
        assert_eq!(
            relative_basis_points(Duration::from_secs(3), Duration::from_secs(2)),
            0
        );
    }
}
