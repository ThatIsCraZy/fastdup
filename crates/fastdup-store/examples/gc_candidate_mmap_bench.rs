//! Compare mapped and positional GC-candidate catalog lookups.

#![deny(unsafe_code)]

use std::hint::black_box;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use fastdup_format::{ContainerId, GcCandidateCatalogRow, SealedContainer};
use fastdup_store::{
    FsStorageIo, GcCandidateCatalogRepository, GcCandidateCatalogSnapshot, StorageIo,
};

const ROWS: u64 = 100_000;
const QUERIES: usize = 200_000;
const ROUNDS: usize = 7;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/benchmarks/gc-candidate-mmap-v1");
    let storage = FsStorageIo::open(&root)?;
    ensure_fixture(storage.clone())?;

    let mapped = GcCandidateCatalogRepository::new(storage.clone())
        .recover_latest()?
        .ok_or_else(|| io::Error::other("mapped GC benchmark snapshot is missing"))?;
    let positional = GcCandidateCatalogRepository::new(PositionalStorage(storage))
        .recover_latest()?
        .ok_or_else(|| io::Error::other("positional GC benchmark snapshot is missing"))?;
    if !mapped.mapped() || positional.mapped() {
        return Err(io::Error::other("GC benchmark selected the wrong page sources").into());
    }

    let plan = query_plan();
    let mut mapped_samples = Vec::with_capacity(ROUNDS);
    let mut positional_samples = Vec::with_capacity(ROUNDS);
    for round in 0..ROUNDS {
        if round % 2 == 0 {
            positional_samples.push(measure(&positional, &plan)?);
            mapped_samples.push(measure(&mapped, &plan)?);
        } else {
            mapped_samples.push(measure(&mapped, &plan)?);
            positional_samples.push(measure(&positional, &plan)?);
        }
    }
    let mapped = median(&mut mapped_samples);
    let positional = median(&mut positional_samples);
    println!("GC candidate mapped-lookup benchmark");
    println!(
        "rows={ROWS} queries={QUERIES} rounds={ROUNDS} mapped_ns_per_query={:.1} positional_ns_per_query={:.1} mmap_speedup={:.3}x",
        nanos_per_query(mapped),
        nanos_per_query(positional),
        positional.as_secs_f64() / mapped.as_secs_f64(),
    );
    Ok(())
}

fn ensure_fixture(storage: FsStorageIo) -> Result<(), Box<dyn std::error::Error>> {
    let repository = GcCandidateCatalogRepository::new(storage);
    if repository.recover_latest()?.is_some() {
        return Ok(());
    }
    let fixture_id = ContainerId::new([0xA5; 16]).expect("fixture identity is nonzero");
    let (_image, publication) =
        SealedContainer::encode_with_writer_evidence(fixture_id, 1, &[b"GC benchmark summary"])?
            .into_publication_parts();
    let summary = publication.intrinsic_summary()?;
    let rows = (1..=ROWS).map(|ordinal| {
        let container_id = ContainerId::new(u128::from(ordinal).to_be_bytes())
            .expect("positive benchmark ordinal is nonzero");
        GcCandidateCatalogRow::from_intrinsic_summary(container_id, ordinal, 12_288, summary)
            .expect("benchmark row is valid")
    });
    repository.publish_rows(1, 1, 1, ROWS, rows)?;
    Ok(())
}

fn query_plan() -> Vec<ContainerId> {
    let mut state = 0x243f_6a88_85a3_08d3_u64;
    (0..QUERIES)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let ordinal = state % ROWS + 1;
            ContainerId::new(u128::from(ordinal).to_be_bytes())
                .expect("positive benchmark ordinal is nonzero")
        })
        .collect()
}

fn measure<I: Clone + StorageIo>(
    snapshot: &GcCandidateCatalogSnapshot<I>,
    plan: &[ContainerId],
) -> Result<Duration, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut checksum = 0_u64;
    for container_id in plan {
        let row = snapshot
            .find_row(*container_id)?
            .ok_or_else(|| io::Error::other("benchmark row was not found"))?;
        checksum = checksum.rotate_left(7) ^ row.container_generation();
    }
    black_box(checksum);
    Ok(started.elapsed())
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn nanos_per_query(duration: Duration) -> f64 {
    let queries = u32::try_from(QUERIES).expect("benchmark query count fits u32");
    duration.as_secs_f64() * 1_000_000_000.0 / f64::from(queries)
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
