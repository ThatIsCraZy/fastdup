#![deny(unsafe_code)]

use std::env;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexProfileId, ManifestExtent, ManifestLeaf,
};
use fastdup_io_uring::{IoUringStorageConfig, IoUringStorageIo};
use fastdup_store::{
    ContainerRepository, ExactIndexGenerationPin, ExactIndexRunRepository, MAX_STORAGE_RANGE_BYTES,
    StorageIo, VerifiedManifestFile,
};
use rustix::fs::{Advice, fadvise};

const DEFAULT_CHUNKS: usize = 128;
const DEFAULT_CHUNK_BYTES: usize = 64 * 1_024;
const DEFAULT_ROUNDS: usize = 5;
const CONTAINER_BYTES: [u8; 16] = [0xc1; 16];
const PROFILE_BYTES: [u8; 32] = [0xc2; 32];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse()?;
    std::fs::create_dir_all(&config.root)?;
    let storage = MeasuredStorage::new(IoUringStorageIo::open(
        &config.root,
        IoUringStorageConfig::default(),
    )?);
    let fixture = Fixture::create(
        storage.clone(),
        config.chunks,
        config.chunk_bytes,
        config.block_device.as_deref(),
        config.cold_cache,
    )?;
    let mut planned = Vec::with_capacity(config.rounds);
    let mut scalar = Vec::with_capacity(config.rounds);
    for round in 0..config.rounds {
        if round % 2 == 0 {
            scalar.push(fixture.measure_scalar()?);
            planned.push(fixture.measure_planned()?);
        } else {
            planned.push(fixture.measure_planned()?);
            scalar.push(fixture.measure_scalar()?);
        }
    }
    let planned = median(&mut planned);
    let scalar = median(&mut scalar);
    let report = format!(
        concat!(
            "fastdup_verified_restore_benchmark_v2\n",
            "root={}\nchunks={}\nchunk_bytes={}\nlogical_bytes={}\nrounds={}\n",
            "storage_adapter=io_uring\n",
            "cache_state={}\n",
            "planned_elapsed_ms={:.3}\nplanned_mib_per_s={:.3}\n",
            "planned_data_reads={}\nplanned_average_read_bytes={:.1}\nplanned_nonsequential_reads={}\n",
            "scalar_elapsed_ms={:.3}\nscalar_mib_per_s={:.3}\n",
            "scalar_data_reads={}\nscalar_average_read_bytes={:.1}\nscalar_nonsequential_reads={}\n",
            "block_device={}\n",
            "planned_block_read_ios={}\nplanned_block_reads_merged={}\nplanned_block_sectors_read={}\n",
            "planned_block_read_ticks_ms={}\nplanned_block_io_ticks_ms={}\n",
            "planned_io_uring_submissions={}\n",
            "scalar_block_read_ios={}\nscalar_block_reads_merged={}\nscalar_block_sectors_read={}\n",
            "scalar_block_read_ticks_ms={}\nscalar_block_io_ticks_ms={}\n",
            "scalar_io_uring_submissions={}\n",
            "request_reduction={:.3}x\nthroughput_ratio={:.3}x\n"
        ),
        config.root.display(),
        config.chunks,
        config.chunk_bytes,
        fixture.logical_bytes,
        config.rounds,
        if config.cold_cache {
            "cold-posix-fadvise-dontneed"
        } else {
            "warm"
        },
        planned.elapsed.as_secs_f64() * 1_000.0,
        mib_per_second(fixture.logical_bytes, planned.elapsed),
        planned.reads,
        planned.average_read_bytes(),
        planned.nonsequential,
        scalar.elapsed.as_secs_f64() * 1_000.0,
        mib_per_second(fixture.logical_bytes, scalar.elapsed),
        scalar.reads,
        scalar.average_read_bytes(),
        scalar.nonsequential,
        config.block_device.as_deref().unwrap_or("none"),
        planned.block.read_ios,
        planned.block.reads_merged,
        planned.block.sectors_read,
        planned.block.read_ticks_ms,
        planned.block.io_ticks_ms,
        planned.io_uring_submissions,
        scalar.block.read_ios,
        scalar.block.reads_merged,
        scalar.block.sectors_read,
        scalar.block.read_ticks_ms,
        scalar.block.io_ticks_ms,
        scalar.io_uring_submissions,
        floating_ratio(scalar.reads, planned.reads.max(1)),
        scalar.elapsed.as_secs_f64() / planned.elapsed.as_secs_f64(),
    );
    print!("{report}");
    if let Some(path) = config.report {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, report)?;
    }
    Ok(())
}

struct Config {
    root: PathBuf,
    report: Option<PathBuf>,
    chunks: usize,
    chunk_bytes: usize,
    rounds: usize,
    block_device: Option<String>,
    cold_cache: bool,
}

impl Config {
    fn parse() -> io::Result<Self> {
        let mut config = Self {
            root: Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(".artifacts/benchmarks/verified-restore-current"),
            report: None,
            chunks: DEFAULT_CHUNKS,
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            rounds: DEFAULT_ROUNDS,
            block_device: None,
            cold_cache: true,
        };
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--root" => config.root = PathBuf::from(next_value(&mut arguments, "--root")?),
                "--report" => {
                    config.report = Some(PathBuf::from(next_value(&mut arguments, "--report")?));
                }
                "--chunks" => {
                    config.chunks = parse_positive(&next_value(&mut arguments, "--chunks")?)?;
                }
                "--chunk-bytes" => {
                    config.chunk_bytes =
                        parse_positive(&next_value(&mut arguments, "--chunk-bytes")?)?;
                }
                "--rounds" => {
                    config.rounds = parse_positive(&next_value(&mut arguments, "--rounds")?)?;
                }
                "--block-device" => {
                    let device = next_value(&mut arguments, "--block-device")?;
                    if Path::new(&device)
                        .file_name()
                        .and_then(|name| name.to_str())
                        != Some(&device)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "--block-device must be one sysfs block-device name",
                        ));
                    }
                    config.block_device = Some(device);
                }
                "--warm-cache" => config.cold_cache = false,
                "--help" | "-h" => {
                    println!(
                        "usage: fastdup-verified-restore-bench [--root PATH] [--report PATH] \
                         [--chunks N] [--chunk-bytes N] [--rounds N] [--block-device NAME] \
                         [--warm-cache]"
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
        if config.chunks > u32::MAX as usize
            || config.chunk_bytes > u32::MAX as usize
            || config
                .chunks
                .checked_mul(config.chunk_bytes)
                .is_none_or(|length| length > u32::MAX as usize)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "benchmark geometry exceeds supported bounds",
            ));
        }
        Ok(config)
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

struct Fixture {
    root: PathBuf,
    storage: MeasuredStorage,
    containers: ContainerRepository<MeasuredStorage>,
    active: ExactIndexGenerationPin<MeasuredStorage>,
    file: VerifiedManifestFile<MeasuredStorage>,
    entries: Vec<ExactIndexEntry>,
    expected_hash: [u8; 32],
    logical_bytes: usize,
    chunk_bytes: usize,
    block_stats: Option<BlockStatsSource>,
    cold_cache: bool,
}

impl Fixture {
    fn create(
        storage: MeasuredStorage,
        chunks: usize,
        chunk_bytes: usize,
        block_device: Option<&str>,
        cold_cache: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if !storage.list_names()?.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "benchmark root must be empty; select a fresh --root",
            )
            .into());
        }
        let mut payloads = Vec::new();
        payloads.try_reserve_exact(chunks)?;
        for ordinal in 0..chunks {
            let mut payload = vec![0_u8; chunk_bytes];
            fill_deterministic(&mut payload, u64::try_from(ordinal)? + 1);
            payloads.push(payload);
        }
        let slices = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let containers = ContainerRepository::new(storage.clone());
        let generation = containers
            .open_generation_allocator(1)
            .and_then(|allocator| allocator.reserve_generation())?;
        let container_id = ContainerId::new(CONTAINER_BYTES)
            .expect("ASSERT: benchmark Container identity is nonzero");
        containers.publish_raw(container_id, generation, &slices)?;
        let container = containers.read(container_id)?;
        let entries = container
            .raw_locations()
            .iter()
            .copied()
            .map(ExactIndexEntry::from_verified_raw)
            .collect::<Result<Vec<_>, _>>()?;
        let indexes = ExactIndexRunRepository::new(storage.clone());
        let profile = ExactIndexProfileId::new(PROFILE_BYTES)
            .expect("ASSERT: benchmark Exact profile is nonzero");
        indexes.append_level_zero(profile, entries.clone())?;
        let active = indexes
            .pin_active_generation()
            .ok_or_else(|| io::Error::other("benchmark Exact generation is missing"))?;
        let extents = payloads
            .iter()
            .map(|payload| ManifestExtent::Data {
                logical_length: u64::try_from(payload.len())
                    .expect("ASSERT: benchmark Chunk length fits u64"),
                chunk_id: ChunkId::of(payload),
            })
            .collect::<Vec<_>>();
        let logical_bytes = chunks
            .checked_mul(chunk_bytes)
            .ok_or_else(|| io::Error::other("benchmark length overflow"))?;
        let manifest = ManifestLeaf::new(u64::try_from(logical_bytes)?, extents)?;
        let file =
            VerifiedManifestFile::new(manifest, containers.clone())?.with_active_index(&active);
        let expected_hash = *blake3::hash(&slices.concat()).as_bytes();
        containers.read_verified_location(entries[0])?;
        storage.reset();
        Ok(Self {
            root: storage.root().to_owned(),
            storage,
            containers,
            active,
            file,
            entries,
            expected_hash,
            logical_bytes,
            chunk_bytes,
            block_stats: block_device.map(BlockStatsSource::new).transpose()?,
            cold_cache,
        })
    }

    fn measure_planned(&self) -> Result<Sample, Box<dyn std::error::Error>> {
        self.prepare_cold()?;
        let block_before = self.read_block_stats()?;
        let io_uring_before = self.storage.submitted_operations();
        let started = Instant::now();
        let mut hasher = blake3::Hasher::new();
        let mut offset = 0_usize;
        while offset < self.logical_bytes {
            let length = (self.logical_bytes - offset).min(MAX_STORAGE_RANGE_BYTES);
            hasher.update(
                &self
                    .file
                    .read_at(u64::try_from(offset)?, u32::try_from(length)?)?,
            );
            offset = offset
                .checked_add(length)
                .ok_or_else(|| io::Error::other("planned restore offset overflow"))?;
        }
        let elapsed = started.elapsed();
        let block = self.read_block_stats()?.delta_from(block_before)?;
        let io_uring_submissions =
            checked_counter_delta(self.storage.submitted_operations(), io_uring_before)?;
        if *hasher.finalize().as_bytes() != self.expected_hash {
            return Err(
                io::Error::new(io::ErrorKind::InvalidData, "planned restore mismatch").into(),
            );
        }
        Ok(self.storage.sample(elapsed, block, io_uring_submissions))
    }

    fn measure_scalar(&self) -> Result<Sample, Box<dyn std::error::Error>> {
        self.prepare_cold()?;
        let block_before = self.read_block_stats()?;
        let io_uring_before = self.storage.submitted_operations();
        let started = Instant::now();
        let mut hasher = blake3::Hasher::new();
        let chunks_per_request = MAX_STORAGE_RANGE_BYTES / self.chunk_bytes;
        for entries in self.entries.chunks(chunks_per_request.max(1)) {
            let mut restored = Vec::with_capacity(entries.len() * self.chunk_bytes);
            for entry in entries {
                restored.extend_from_slice(&self.containers.read_verified_chunk_with_index(
                    &self.active,
                    entry.chunk_id(),
                    u64::from(entry.logical_length()),
                )?);
            }
            hasher.update(&restored);
        }
        let elapsed = started.elapsed();
        let block = self.read_block_stats()?.delta_from(block_before)?;
        let io_uring_submissions =
            checked_counter_delta(self.storage.submitted_operations(), io_uring_before)?;
        if *hasher.finalize().as_bytes() != self.expected_hash {
            return Err(
                io::Error::new(io::ErrorKind::InvalidData, "scalar restore mismatch").into(),
            );
        }
        Ok(self.storage.sample(elapsed, block, io_uring_submissions))
    }

    fn prepare_cold(&self) -> io::Result<()> {
        if self.cold_cache {
            let path = self.root.join(format!("{}.fdc", "c1".repeat(16)));
            let file = File::open(path)?;
            fadvise(&file, 0, None, Advice::DontNeed)?;
        }
        self.storage.reset();
        Ok(())
    }

    fn read_block_stats(&self) -> io::Result<BlockStats> {
        self.block_stats
            .as_ref()
            .map_or(Ok(BlockStats::default()), BlockStatsSource::read)
    }
}

#[derive(Clone, Debug)]
struct BlockStatsSource {
    path: PathBuf,
}

impl BlockStatsSource {
    fn new(device: &str) -> io::Result<Self> {
        let path = Path::new("/sys/class/block").join(device).join("stat");
        if !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("block statistics do not exist for {device}"),
            ));
        }
        Ok(Self { path })
    }

    fn read(&self) -> io::Result<BlockStats> {
        let encoded = std::fs::read_to_string(&self.path)?;
        let fields = encoded
            .split_ascii_whitespace()
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if fields.len() < 11 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "block statistics contain fewer than eleven fields",
            ));
        }
        Ok(BlockStats {
            read_ios: fields[0],
            reads_merged: fields[1],
            sectors_read: fields[2],
            read_ticks_ms: fields[3],
            io_ticks_ms: fields[9],
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct BlockStats {
    read_ios: u64,
    reads_merged: u64,
    sectors_read: u64,
    read_ticks_ms: u64,
    io_ticks_ms: u64,
}

impl BlockStats {
    fn delta_from(self, before: Self) -> io::Result<Self> {
        Ok(Self {
            read_ios: checked_counter_delta(self.read_ios, before.read_ios)?,
            reads_merged: checked_counter_delta(self.reads_merged, before.reads_merged)?,
            sectors_read: checked_counter_delta(self.sectors_read, before.sectors_read)?,
            read_ticks_ms: checked_counter_delta(self.read_ticks_ms, before.read_ticks_ms)?,
            io_ticks_ms: checked_counter_delta(self.io_ticks_ms, before.io_ticks_ms)?,
        })
    }
}

fn checked_counter_delta(after: u64, before: u64) -> io::Result<u64> {
    after.checked_sub(before).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "block statistic counter decreased during one sample",
        )
    })
}

fn fill_deterministic(bytes: &mut [u8], mut state: u64) {
    for byte in bytes {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        *byte = state.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes()[0];
    }
}

#[derive(Clone, Debug)]
struct MeasuredStorage {
    inner: IoUringStorageIo,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Default)]
struct Metrics {
    reads: AtomicU64,
    bytes: AtomicU64,
    nonsequential: AtomicU64,
    previous_end: Mutex<Option<u64>>,
}

impl MeasuredStorage {
    fn new(inner: IoUringStorageIo) -> Self {
        Self {
            inner,
            metrics: Arc::new(Metrics::default()),
        }
    }

    fn root(&self) -> &Path {
        self.inner.root()
    }

    fn reset(&self) {
        self.metrics.reads.store(0, Ordering::Relaxed);
        self.metrics.bytes.store(0, Ordering::Relaxed);
        self.metrics.nonsequential.store(0, Ordering::Relaxed);
        *self
            .metrics
            .previous_end
            .lock()
            .expect("benchmark metric lock is valid") = None;
    }

    fn submitted_operations(&self) -> u64 {
        self.inner.status().submitted_operations()
    }

    fn sample(&self, elapsed: Duration, block: BlockStats, io_uring_submissions: u64) -> Sample {
        Sample {
            elapsed,
            reads: self.metrics.reads.load(Ordering::Relaxed),
            bytes: self.metrics.bytes.load(Ordering::Relaxed),
            nonsequential: self.metrics.nonsequential.load(Ordering::Relaxed),
            block,
            io_uring_submissions,
        }
    }
}

impl StorageIo for MeasuredStorage {
    fn create_new(&self, name: &str) -> io::Result<()> {
        self.inner.create_new(name)
    }
    fn exists(&self, name: &str) -> io::Result<bool> {
        self.inner.exists(name)
    }
    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_at(name, offset, bytes)
    }
    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        self.inner.read(name)
    }
    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.inner.object_len(name)
    }
    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let bytes = self.inner.read_exact_at(name, offset, length)?;
        if Path::new(name)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("fdc"))
        {
            self.metrics.reads.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .bytes
                .fetch_add(u64::try_from(length).unwrap_or(u64::MAX), Ordering::Relaxed);
            let mut previous = self
                .metrics
                .previous_end
                .lock()
                .expect("benchmark metric lock is valid");
            if previous.is_some_and(|end| end != offset) {
                self.metrics.nonsequential.fetch_add(1, Ordering::Relaxed);
            }
            *previous = offset.checked_add(u64::try_from(length).unwrap_or(u64::MAX));
        }
        Ok(bytes)
    }
    fn list_names(&self) -> io::Result<Vec<String>> {
        self.inner.list_names()
    }
    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.inner.set_len(name, length)
    }
    fn sync_file(&self, name: &str) -> io::Result<()> {
        self.inner.sync_file(name)
    }
    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        self.inner.publish_noreplace(temporary_name, published_name)
    }
    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.inner.remove_file(name)
    }
    fn sync_root(&self) -> io::Result<()> {
        self.inner.sync_root()
    }
}

#[derive(Clone, Copy)]
struct Sample {
    elapsed: Duration,
    reads: u64,
    bytes: u64,
    nonsequential: u64,
    block: BlockStats,
    io_uring_submissions: u64,
}

impl Sample {
    fn average_read_bytes(self) -> f64 {
        floating_ratio(self.bytes, self.reads.max(1))
    }
}

fn median(samples: &mut [Sample]) -> Sample {
    samples.sort_unstable_by_key(|sample| sample.elapsed);
    samples[samples.len() / 2]
}

#[allow(clippy::cast_precision_loss)]
fn mib_per_second(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

#[allow(clippy::cast_precision_loss)]
fn floating_ratio(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 / denominator as f64
}
