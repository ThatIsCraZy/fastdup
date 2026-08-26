#![deny(unsafe_code)]

mod immutable_mmap;

use std::env;
use std::fs::{self, File};
use std::hint::black_box;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fastdup_format::{
    ChunkId, SIMILARITY_INDEX_HEADER_BYTES, SIMILARITY_INDEX_PAGE_BYTES, SimilarityIndexEntry,
    SimilarityIndexRun, SimilarityIndexRunDescriptor,
};
use immutable_mmap::ImmutableFileMap;

const DEFAULT_ENTRIES: usize = 100_000;
const DEFAULT_QUERIES: usize = 1_000_000;
const DEFAULT_ROUNDS: usize = 7;
const FINGERPRINT_PROFILE: u16 = 1;
const BUCKET_PROFILE: u16 = 1;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::parse()?;
    if config.generate || !config.file.exists() {
        generate_fixture(&config.file, config.entries)?;
    }
    let descriptor = read_descriptor(&config.file)?;
    let plan = QueryPlan::new(descriptor, config.queries)?;
    let pread = PreadPages::open(&config.file, descriptor)?;
    let mapped = ImmutableMappedPages::open(&config.file, descriptor)?;

    let mut pread_samples = Vec::with_capacity(config.rounds);
    let mut mmap_samples = Vec::with_capacity(config.rounds);
    for round in 0..config.rounds {
        if round % 2 == 0 {
            pread_samples.push(measure(&pread, &plan)?);
            mmap_samples.push(measure(&mapped, &plan)?);
        } else {
            mmap_samples.push(measure(&mapped, &plan)?);
            pread_samples.push(measure(&pread, &plan)?);
        }
    }
    mapped.verify_unchanged()?;

    let pread_median = median(&mut pread_samples);
    let mmap_median = median(&mut mmap_samples);
    let pread_ns = nanos_per_query(pread_median, config.queries);
    let mmap_ns = nanos_per_query(mmap_median, config.queries);
    println!("Similarity page-access benchmark");
    println!("file={}", config.file.display());
    println!(
        "bytes={} entry_pages={} bucket_pages={} queries={} rounds={}",
        descriptor.file_length(),
        descriptor.page_count(),
        descriptor.bucket_page_count(),
        config.queries,
        config.rounds
    );
    println!("backend                 median       ns/query");
    println!("read_exact_at+decode  {pread_median:>10.3?}  {pread_ns:>12.1}");
    println!("mmap-slice+decode      {mmap_median:>10.3?}  {mmap_ns:>12.1}");
    println!("mmap_speedup={:.3}x", pread_ns / mmap_ns);
    Ok(())
}

#[derive(Debug)]
struct Config {
    file: PathBuf,
    entries: usize,
    queries: usize,
    rounds: usize,
    generate: bool,
}

impl Config {
    fn parse() -> Result<Self, io::Error> {
        let mut config = Self {
            file: default_fixture_path(),
            entries: DEFAULT_ENTRIES,
            queries: DEFAULT_QUERIES,
            rounds: DEFAULT_ROUNDS,
            generate: false,
        };
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--file" => config.file = PathBuf::from(next_value(&mut arguments, "--file")?),
                "--entries" => {
                    config.entries = parse_positive(&next_value(&mut arguments, "--entries")?)?;
                }
                "--queries" => {
                    config.queries = parse_positive(&next_value(&mut arguments, "--queries")?)?;
                }
                "--rounds" => {
                    config.rounds = parse_positive(&next_value(&mut arguments, "--rounds")?)?;
                }
                "--generate" => config.generate = true,
                "--help" | "-h" => {
                    println!(
                        "usage: fastdup-similarity-page-bench [--file PATH] [--generate] \
                         [--entries N] [--queries N] [--rounds N]"
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
        if config.queries > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "query count must fit u32",
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

fn default_fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/benchmarks/similarity-pages-v2.fds")
}

fn generate_fixture(path: &Path, entry_count: usize) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut entries = Vec::new();
    entries.try_reserve_exact(entry_count)?;
    for ordinal in 0..entry_count {
        let ordinal = u64::try_from(ordinal)?;
        entries.push(SimilarityIndexEntry::new(
            ChunkId::of(&ordinal.to_le_bytes()),
            64 * 1_024,
            FINGERPRINT_PROFILE,
            [
                mix64(ordinal),
                mix64(ordinal ^ 0x5555_5555_5555_5555),
                mix64(ordinal ^ 0xaaaa_aaaa_aaaa_aaaa),
                mix64(!ordinal),
            ],
            [mix64(ordinal.rotate_left(7)); 8],
        )?);
    }
    let encoded =
        SimilarityIndexRun::new(FINGERPRINT_PROFILE, BUCKET_PROFILE, 1, entries)?.encode()?;
    fs::write(path, encoded)?;
    Ok(())
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn read_descriptor(
    path: &Path,
) -> Result<SimilarityIndexRunDescriptor, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    if length < 3 * SIMILARITY_INDEX_PAGE_BYTES as u64 {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "Similarity Run is too short").into(),
        );
    }
    let mut header = [0_u8; SIMILARITY_INDEX_HEADER_BYTES];
    let mut footer = [0_u8; SIMILARITY_INDEX_HEADER_BYTES];
    file.read_exact_at(&mut header, 0)?;
    file.read_exact_at(
        &mut footer,
        length - u64::try_from(SIMILARITY_INDEX_HEADER_BYTES)?,
    )?;
    Ok(SimilarityIndexRunDescriptor::decode(
        &header, &footer, length,
    )?)
}

#[derive(Clone, Copy)]
enum PageAddress {
    Entry { ordinal: usize, offset: u64 },
    Bucket { ordinal: usize, offset: u64 },
}

struct QueryPlan {
    addresses: Vec<PageAddress>,
}

impl QueryPlan {
    fn new(
        descriptor: SimilarityIndexRunDescriptor,
        query_count: usize,
    ) -> Result<Self, io::Error> {
        let total_pages = descriptor
            .page_count()
            .checked_add(descriptor.bucket_page_count())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "page count overflow"))?;
        if total_pages == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Run has no pages",
            ));
        }
        let mut addresses = Vec::new();
        addresses
            .try_reserve_exact(query_count)
            .map_err(io::Error::other)?;
        let mut state = 0x243f_6a88_85a3_08d3_u64;
        for _ in 0..query_count {
            state = mix64(state);
            let combined = usize::try_from(state % u64::try_from(total_pages).unwrap_or(u64::MAX))
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "page ordinal overflow"))?;
            let address = if combined < descriptor.page_count() {
                PageAddress::Entry {
                    ordinal: combined,
                    offset: descriptor.page_offset(combined).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "entry page offset")
                    })?,
                }
            } else {
                let ordinal = combined - descriptor.page_count();
                PageAddress::Bucket {
                    ordinal,
                    offset: descriptor.bucket_page_offset(ordinal).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "bucket page offset")
                    })?,
                }
            };
            addresses.push(address);
        }
        Ok(Self { addresses })
    }
}

trait PageSource {
    fn visit(
        &self,
        address: PageAddress,
        checksum: &mut u64,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

struct PreadPages {
    file: File,
    descriptor: SimilarityIndexRunDescriptor,
}

impl PreadPages {
    fn open(path: &Path, descriptor: SimilarityIndexRunDescriptor) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            descriptor,
        })
    }
}

impl PageSource for PreadPages {
    fn visit(
        &self,
        address: PageAddress,
        checksum: &mut u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bytes = [0_u8; SIMILARITY_INDEX_PAGE_BYTES];
        match address {
            PageAddress::Entry { ordinal, offset } => {
                self.file.read_exact_at(&mut bytes, offset)?;
                let page = self.descriptor.decode_page(ordinal, &bytes)?;
                *checksum ^= u64::try_from(page.entries().len())?;
            }
            PageAddress::Bucket { ordinal, offset } => {
                self.file.read_exact_at(&mut bytes, offset)?;
                let page = self.descriptor.decode_bucket_page(ordinal, &bytes)?;
                *checksum ^= u64::try_from(page.references().len())?.rotate_left(17);
            }
        }
        Ok(())
    }
}

struct ImmutableMappedPages {
    map: ImmutableFileMap,
    descriptor: SimilarityIndexRunDescriptor,
}

impl ImmutableMappedPages {
    fn open(path: &Path, descriptor: SimilarityIndexRunDescriptor) -> io::Result<Self> {
        let map = ImmutableFileMap::open(path, descriptor.file_length())?;
        Ok(Self { map, descriptor })
    }

    fn verify_unchanged(&self) -> io::Result<()> {
        self.map.verify_unchanged()
    }

    fn page(&self, offset: u64) -> io::Result<&[u8]> {
        let start = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "mmap offset overflow"))?;
        let end = start
            .checked_add(SIMILARITY_INDEX_PAGE_BYTES)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "mmap range overflow"))?;
        self.map.range(start, end)
    }
}

impl PageSource for ImmutableMappedPages {
    fn visit(
        &self,
        address: PageAddress,
        checksum: &mut u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match address {
            PageAddress::Entry { ordinal, offset } => {
                let page = self.descriptor.decode_page(ordinal, self.page(offset)?)?;
                *checksum ^= u64::try_from(page.entries().len())?;
            }
            PageAddress::Bucket { ordinal, offset } => {
                let page = self
                    .descriptor
                    .decode_bucket_page(ordinal, self.page(offset)?)?;
                *checksum ^= u64::try_from(page.references().len())?.rotate_left(17);
            }
        }
        Ok(())
    }
}

fn measure(
    source: &impl PageSource,
    plan: &QueryPlan,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let mut checksum = 0_u64;
    let start = Instant::now();
    for address in plan.addresses.iter().copied() {
        source.visit(address, &mut checksum)?;
    }
    let elapsed = start.elapsed();
    black_box(checksum);
    Ok(elapsed)
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn nanos_per_query(duration: Duration, queries: usize) -> f64 {
    let queries = u32::try_from(queries).expect("ASSERT: validated benchmark query count fits u32");
    duration.as_secs_f64() * 1_000_000_000.0 / f64::from(queries)
}
