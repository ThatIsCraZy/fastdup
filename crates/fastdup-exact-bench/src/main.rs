#![deny(unsafe_code)]

use std::env;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef, ExactIndexRunSet,
};
use fastdup_store::{ActivatedExactIndex, ExactIndexRunRepository, FsStorageIo, StorageIo};

const DEFAULT_ENTRIES: usize = 100_000;
const DEFAULT_QUERIES: usize = 100_000;
const DEFAULT_ROUNDS: usize = 7;
const PROFILE_BYTES: [u8; 32] = [0xEB; 32];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse()?;
    let root = config.root();
    let storage = FsStorageIo::open(&root)?;
    ensure_fixture(&storage, config.entries)?;
    let plan = QueryPlan::new(config.entries, config.queries)?;

    let mapped_repository = ExactIndexRunRepository::new(storage.clone());
    let mapped = mapped_repository
        .recover_active()?
        .ok_or_else(|| io::Error::other("Exact benchmark activation is missing"))?;
    let positional_repository = ExactIndexRunRepository::new(PositionalStorage(storage));
    let positional = positional_repository
        .recover_active()?
        .ok_or_else(|| io::Error::other("Exact benchmark activation is missing"))?;

    let mapped_status = mapped.membership_status();
    let positional_status = positional.membership_status();
    if mapped_status.mapped_run_count() == 0
        || mapped_status.positional_run_count() != 0
        || positional_status.mapped_run_count() != 0
        || positional_status.positional_run_count() == 0
    {
        return Err(io::Error::other("benchmark page-source selection is invalid").into());
    }

    let mut mapped_samples = Vec::with_capacity(config.rounds);
    let mut positional_samples = Vec::with_capacity(config.rounds);
    for round in 0..config.rounds {
        if round % 2 == 0 {
            positional_samples.push(measure(&positional, &plan)?);
            mapped_samples.push(measure(&mapped, &plan)?);
        } else {
            mapped_samples.push(measure(&mapped, &plan)?);
            positional_samples.push(measure(&positional, &plan)?);
        }
    }

    let positional = Summary::from_samples(&mut positional_samples, config.queries);
    let mapped = Summary::from_samples(&mut mapped_samples, config.queries);
    println!("Exact activated-lookup benchmark");
    println!("root={}", root.display());
    println!(
        "entries={} queries={} rounds={} run_bytes={}",
        config.entries,
        config.queries,
        config.rounds,
        std::fs::metadata(root.join(run_name()))?.len()
    );
    println!(
        "backend                 median       ns/query  minflt  majflt  peak_rss_kib  peak_swap_kib"
    );
    positional.print("read_exact_at+cache", config.queries);
    mapped.print("mmap+bounds+cache", config.queries);
    println!(
        "mmap_speedup={:.3}x",
        positional.ns_per_query / mapped.ns_per_query
    );
    Ok(())
}

#[derive(Debug)]
struct Config {
    root: Option<PathBuf>,
    entries: usize,
    queries: usize,
    rounds: usize,
}

impl Config {
    fn parse() -> io::Result<Self> {
        let mut config = Self {
            root: None,
            entries: DEFAULT_ENTRIES,
            queries: DEFAULT_QUERIES,
            rounds: DEFAULT_ROUNDS,
        };
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--root" => {
                    config.root = Some(PathBuf::from(next_value(&mut arguments, "--root")?));
                }
                "--entries" => {
                    config.entries = parse_positive(&next_value(&mut arguments, "--entries")?)?;
                }
                "--queries" => {
                    config.queries = parse_positive(&next_value(&mut arguments, "--queries")?)?;
                }
                "--rounds" => {
                    config.rounds = parse_positive(&next_value(&mut arguments, "--rounds")?)?;
                }
                "--help" | "-h" => {
                    println!(
                        "usage: fastdup-exact-lookup-bench [--root PATH] [--entries N] \
                         [--queries N] [--rounds N]"
                    );
                    std::process::exit(0);
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown argument: {argument}"),
                    ));
                }
            }
        }
        if config.entries > u32::MAX as usize || config.queries > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "entry and query counts must fit u32",
            ));
        }
        Ok(config)
    }

    fn root(&self) -> PathBuf {
        self.root.clone().unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(format!(
                    ".artifacts/benchmarks/exact-lookup-v1-{}",
                    self.entries
                ))
        })
    }
}

fn next_value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> io::Result<String> {
    arguments.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing value after {flag}"),
        )
    })
}

fn parse_positive(value: &str) -> io::Result<usize> {
    let parsed = value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid count: {value}"),
        )
    })?;
    if parsed == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "counts must be positive",
        ));
    }
    Ok(parsed)
}

fn ensure_fixture(
    storage: &FsStorageIo,
    entry_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = ExactIndexRunRepository::new(storage.clone());
    if let Some(active) = repository.recover_active()? {
        let observed = active
            .run_set()
            .runs()
            .first()
            .map_or(0, |run| run.entry_count());
        if active.run_count() != 1 || observed != u64::try_from(entry_count)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "existing Exact benchmark fixture has a different entry count",
            )
            .into());
        }
        return Ok(());
    }

    let profile = profile();
    let mut entries = Vec::new();
    entries.try_reserve_exact(entry_count)?;
    for ordinal in 0..entry_count {
        entries.push(entry(ordinal)?);
    }
    let descriptor = repository.publish(&ExactIndexRun::new(profile, 1, entries)?)?;
    let run_set = ExactIndexRunSet::new(profile, 1, vec![ExactIndexRunRef::new(0, descriptor)?])?;
    repository.activate(&run_set)?;
    Ok(())
}

fn profile() -> ExactIndexProfileId {
    ExactIndexProfileId::new(PROFILE_BYTES).expect("ASSERT: benchmark profile is nonzero")
}

fn entry(ordinal: usize) -> Result<ExactIndexEntry, Box<dyn std::error::Error>> {
    let identity = u64::try_from(ordinal)?
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "benchmark ordinal overflow"))?;
    let mut chunk = [0_u8; 32];
    chunk[24..].copy_from_slice(&identity.to_be_bytes());
    let mut container = [0_u8; 16];
    container[..8].copy_from_slice(&identity.to_le_bytes());
    let logical_length = 64 * 1_024;
    let record_length = (logical_length + 255) / 64 * 64;
    let location = ExactIndexLocation::raw(
        ContainerId::new(container).expect("ASSERT: benchmark Container ID is nonzero"),
        identity,
        4_096,
        record_length,
        u32::try_from(ordinal)?,
    )?;
    Ok(ExactIndexEntry::active(
        ChunkId::from_bytes(chunk),
        logical_length,
        location,
    )?)
}

fn run_name() -> String {
    let profile = "eb".repeat(PROFILE_BYTES.len());
    format!("{profile}.0000000000000001.fdx")
}

struct QueryPlan {
    keys: Vec<(ChunkId, u32)>,
}

impl QueryPlan {
    fn new(entry_count: usize, query_count: usize) -> io::Result<Self> {
        let mut keys = Vec::new();
        keys.try_reserve_exact(query_count)
            .map_err(io::Error::other)?;
        let modulus = u64::try_from(entry_count)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "entry count overflow"))?;
        let mut state = 0x243f_6a88_85a3_08d3_u64;
        for _ in 0..query_count {
            state = mix64(state);
            let identity = state % modulus + 1;
            let mut chunk = [0_u8; 32];
            chunk[24..].copy_from_slice(&identity.to_be_bytes());
            keys.push((ChunkId::from_bytes(chunk), 64 * 1_024));
        }
        Ok(Self { keys })
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn measure<I: StorageIo>(
    active: &ActivatedExactIndex<I>,
    plan: &QueryPlan,
) -> Result<Sample, Box<dyn std::error::Error>> {
    let before = ProcessSample::read()?;
    let started = Instant::now();
    let mut checksum = 0_u64;
    for (chunk_id, logical_length) in plan.keys.iter().copied() {
        let lookup = active.lookup_transitions(chunk_id, logical_length)?;
        let candidate = lookup
            .candidates()
            .first()
            .ok_or_else(|| io::Error::other("benchmark hit was not found"))?;
        checksum ^= candidate.location().container_generation();
    }
    let elapsed = started.elapsed();
    let after = ProcessSample::read()?;
    black_box(checksum);
    Ok(Sample {
        elapsed,
        minor_faults: after.minor_faults.saturating_sub(before.minor_faults),
        major_faults: after.major_faults.saturating_sub(before.major_faults),
        rss_kib: after.rss_kib,
        swap_kib: after.swap_kib,
    })
}

struct Summary {
    elapsed: Duration,
    ns_per_query: f64,
    minor_faults: u64,
    major_faults: u64,
    peak_rss_kib: u64,
    peak_swap_kib: u64,
}

impl Summary {
    fn from_samples(samples: &mut [Sample], query_count: usize) -> Self {
        samples.sort_unstable_by_key(|sample| sample.elapsed);
        let elapsed = samples[samples.len() / 2].elapsed;
        let query_count = f64::from(
            u32::try_from(query_count).expect("ASSERT: benchmark query count was bounded by u32"),
        );
        Self {
            elapsed,
            ns_per_query: elapsed.as_secs_f64() * 1_000_000_000.0 / query_count,
            minor_faults: samples.iter().map(|sample| sample.minor_faults).sum(),
            major_faults: samples.iter().map(|sample| sample.major_faults).sum(),
            peak_rss_kib: samples
                .iter()
                .map(|sample| sample.rss_kib)
                .max()
                .unwrap_or(0),
            peak_swap_kib: samples
                .iter()
                .map(|sample| sample.swap_kib)
                .max()
                .unwrap_or(0),
        }
    }

    fn print(&self, label: &str, _query_count: usize) {
        println!(
            "{label:<23} {:>10.3?} {:>12.1} {:>7} {:>7} {:>13} {:>14}",
            self.elapsed,
            self.ns_per_query,
            self.minor_faults,
            self.major_faults,
            self.peak_rss_kib,
            self.peak_swap_kib,
        );
    }
}

struct Sample {
    elapsed: Duration,
    minor_faults: u64,
    major_faults: u64,
    rss_kib: u64,
    swap_kib: u64,
}

#[derive(Clone, Copy)]
struct ProcessSample {
    minor_faults: u64,
    major_faults: u64,
    rss_kib: u64,
    swap_kib: u64,
}

impl ProcessSample {
    fn read() -> io::Result<Self> {
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let tail = stat
            .rsplit_once(')')
            .map(|(_, tail)| tail.trim())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid /proc/self/stat"))?;
        let fields = tail.split_ascii_whitespace().collect::<Vec<_>>();
        let parse_field = |index: usize| -> io::Result<u64> {
            fields
                .get(index)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short /proc/self/stat"))?
                .parse()
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid /proc/self/stat field")
                })
        };
        let status = std::fs::read_to_string("/proc/self/status")?;
        Ok(Self {
            minor_faults: parse_field(7)?,
            major_faults: parse_field(9)?,
            rss_kib: status_kib(&status, "VmRSS:")?,
            swap_kib: status_kib(&status, "VmSwap:")?,
        })
    }
}

fn status_kib(status: &str, key: &str) -> io::Result<u64> {
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix(key)
                .and_then(|value| value.split_ascii_whitespace().next())
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "missing /proc/self/status field",
            )
        })?
        .parse()
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid /proc/self/status field",
            )
        })
}

#[derive(Clone, Debug)]
struct PositionalStorage(FsStorageIo);

impl StorageIo for PositionalStorage {
    fn create_new(&self, name: &str) -> io::Result<()> {
        self.0.create_new(name)
    }
    fn exists(&self, name: &str) -> io::Result<bool> {
        self.0.exists(name)
    }
    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.0.write_at(name, offset, bytes)
    }
    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        self.0.read(name)
    }
    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.0.object_len(name)
    }
    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.0.read_exact_at(name, offset, length)
    }
    fn list_names(&self) -> io::Result<Vec<String>> {
        self.0.list_names()
    }
    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.0.set_len(name, length)
    }
    fn sync_file(&self, name: &str) -> io::Result<()> {
        self.0.sync_file(name)
    }
    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        self.0.publish_noreplace(temporary_name, published_name)
    }
    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.0.remove_file(name)
    }
    fn sync_root(&self) -> io::Result<()> {
        self.0.sync_root()
    }
}
