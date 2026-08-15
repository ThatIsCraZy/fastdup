use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fastdup_format::{ChunkId, ContainerId};
use fastdup_store::ContainerStore;

const ISO_BYTES: u64 = 2_072_444_928;
const VARIANT_COUNT: usize = 10;
const CHUNK_BYTES: usize = 64 * 1_024;
const CHUNKS_PER_CONTAINER: usize = 512;
const COMMIT_DEADLINE: Duration = Duration::from_secs(10);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let corpus = arguments.next().map_or_else(default_corpus, PathBuf::from);
    let store_root = arguments.next().map_or_else(default_store, PathBuf::from);
    if arguments.next().is_some() {
        return Err("usage: ingest_iso_variants [corpus-directory] [store-directory]".into());
    }
    if store_root.try_exists()? {
        return Err(format!("refusing existing store {}", store_root.display()).into());
    }
    std::fs::create_dir(&store_root)?;

    let result = ingest_all(&corpus, &store_root)?;
    let restore_started = Instant::now();
    let reopened = ContainerStore::open(&store_root)?;
    for (variant, recipe) in result.recipes.iter().enumerate() {
        verify_restore(&reopened, recipe)?;
        println!("restored variant={variant:02} bytes={ISO_BYTES} status=byte-exact");
    }
    let restore_elapsed = restore_started.elapsed();

    report(&store_root, result, restore_elapsed)
}

fn ingest_all(
    corpus: &Path,
    store_root: &Path,
) -> Result<IngestResult, Box<dyn std::error::Error>> {
    let store = ContainerStore::open(store_root)?;
    let mut recipes = Vec::with_capacity(VARIANT_COUNT);
    let mut observed_chunks = HashSet::new();
    let mut exact_hits = 0_u64;
    let mut exact_hit_bytes = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut publish_latencies = Vec::new();
    let mut next_generation = 1_u64;

    let ingest_started = Instant::now();
    for variant in 0..VARIANT_COUNT {
        let path = variant_path(corpus, variant);
        validate_variant(&path)?;
        let mut reader = BufReader::with_capacity(1024 * 1024, File::open(&path)?);
        let mut containers = Vec::new();
        let mut file_bytes = 0_u64;

        for container_ordinal in 0_u32.. {
            let mut chunks = Vec::with_capacity(CHUNKS_PER_CONTAINER);
            for _ in 0..CHUNKS_PER_CONTAINER {
                let Some(chunk) = read_chunk(&mut reader)? else {
                    break;
                };
                file_bytes = file_bytes
                    .checked_add(u64::try_from(chunk.len())?)
                    .ok_or_else(|| invalid_data("logical byte counter overflow"))?;
                logical_bytes = logical_bytes
                    .checked_add(u64::try_from(chunk.len())?)
                    .ok_or_else(|| invalid_data("logical byte counter overflow"))?;
                let chunk_id = ChunkId::of(&chunk);
                if !observed_chunks.insert(chunk_id.bytes()) {
                    exact_hits = exact_hits
                        .checked_add(1)
                        .ok_or_else(|| invalid_data("exact-hit counter overflow"))?;
                    exact_hit_bytes = exact_hit_bytes
                        .checked_add(u64::try_from(chunk.len())?)
                        .ok_or_else(|| invalid_data("exact-hit byte counter overflow"))?;
                }
                chunks.push(chunk);
            }
            if chunks.is_empty() {
                break;
            }

            let container_id = container_id(variant, container_ordinal)?;
            let chunk_refs = chunks.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let started = Instant::now();
            store.publish_raw(container_id, next_generation, &chunk_refs)?;
            let elapsed = started.elapsed();
            if elapsed > COMMIT_DEADLINE {
                return Err(format!(
                    "container publish exceeded 10-second deadline: variant={variant:02} \
                     container={container_ordinal} elapsed_ms={}",
                    elapsed.as_millis()
                )
                .into());
            }
            publish_latencies.push(elapsed);
            containers.push(ContainerRecipe {
                id: container_id,
                generation: next_generation,
                chunks: chunks
                    .iter()
                    .map(|chunk| (ChunkId::of(chunk), chunk.len()))
                    .collect(),
            });
            next_generation = next_generation
                .checked_add(1)
                .ok_or_else(|| invalid_data("container generation overflow"))?;
        }
        if file_bytes != ISO_BYTES {
            return Err(invalid_data("ingest did not consume the complete ISO").into());
        }
        println!(
            "ingested variant={variant:02} bytes={file_bytes} containers={}",
            containers.len()
        );
        recipes.push(FileRecipe { path, containers });
    }
    let ingest_elapsed = ingest_started.elapsed();
    drop(store);

    Ok(IngestResult {
        recipes,
        unique_chunk_ids: observed_chunks.len(),
        exact_hits,
        exact_hit_bytes,
        logical_bytes,
        publish_latencies,
        ingest_elapsed,
    })
}

fn report(
    store_root: &Path,
    mut result: IngestResult,
    restore_elapsed: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let (published_files, published_file_bytes, allocated_bytes) = store_usage(store_root)?;
    if published_files != result.publish_latencies.len() {
        return Err(invalid_data("published file count disagrees with successful commits").into());
    }
    result.publish_latencies.sort_unstable();
    let maximum = result
        .publish_latencies
        .last()
        .copied()
        .ok_or_else(|| invalid_data("no publication latency samples"))?;

    println!("result=PASS");
    println!("mode=RAW_NO_DEDUP_NO_DELTA");
    println!("logical_bytes={}", result.logical_bytes);
    println!("containers={published_files}");
    println!("physical_file_bytes={published_file_bytes}");
    println!("allocated_bytes={allocated_bytes}");
    println!(
        "fixed_chunks={}",
        result.exact_hits + u64::try_from(result.unique_chunk_ids)?
    );
    println!("unique_chunk_ids={}", result.unique_chunk_ids);
    println!("exact_reuse_candidates={}", result.exact_hits);
    println!("exact_reuse_candidate_bytes={}", result.exact_hit_bytes);
    println!("actual_exact_dedup_hits=0");
    println!("actual_delta_encodings=0");
    println!(
        "publish_p50_ms={}",
        percentile(&result.publish_latencies, 50).as_millis()
    );
    println!(
        "publish_p99_ms={}",
        percentile(&result.publish_latencies, 99).as_millis()
    );
    println!("publish_max_ms={}", maximum.as_millis());
    println!("publishes_over_10s=0");
    println!("ingest_elapsed_ms={}", result.ingest_elapsed.as_millis());
    println!("restore_elapsed_ms={}", restore_elapsed.as_millis());
    println!("store={}", store_root.display());
    Ok(())
}

fn verify_restore(store: &ContainerStore, recipe: &FileRecipe) -> io::Result<()> {
    let mut expected = BufReader::with_capacity(1024 * 1024, File::open(&recipe.path)?);
    let mut restored_bytes = 0_u64;
    for expected_container in &recipe.containers {
        let container = store
            .read(expected_container.id)
            .map_err(io::Error::other)?;
        if container.header().container_generation() != expected_container.generation
            || container.chunk_count() != expected_container.chunks.len()
        {
            return Err(invalid_data("restored container metadata mismatch"));
        }
        for &(chunk_id, length) in &expected_container.chunks {
            let restored = container
                .chunk(chunk_id)
                .ok_or_else(|| invalid_data("manifest chunk is absent from its container"))?;
            if restored.len() != length {
                return Err(invalid_data("restored chunk length mismatch"));
            }
            let mut source = vec![0_u8; length];
            expected.read_exact(&mut source)?;
            if restored != source {
                return Err(invalid_data("restored bytes disagree with source ISO"));
            }
            restored_bytes = restored_bytes
                .checked_add(u64::try_from(length).map_err(io::Error::other)?)
                .ok_or_else(|| invalid_data("restore byte counter overflow"))?;
        }
    }
    let mut trailing = [0_u8; 1];
    if restored_bytes != ISO_BYTES || expected.read(&mut trailing)? != 0 {
        return Err(invalid_data(
            "restore recipe did not cover the complete ISO",
        ));
    }
    Ok(())
}

fn read_chunk(reader: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let mut chunk = vec![0_u8; CHUNK_BYTES];
    let mut filled = 0;
    while filled < chunk.len() {
        match reader.read(&mut chunk[filled..])? {
            0 => break,
            read => {
                filled = filled
                    .checked_add(read)
                    .ok_or_else(|| invalid_data("chunk length overflow"))?;
            }
        }
    }
    if filled == 0 {
        return Ok(None);
    }
    chunk.truncate(filled);
    Ok(Some(chunk))
}

fn container_id(variant: usize, ordinal: u32) -> Result<ContainerId, Box<dyn std::error::Error>> {
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(b"FDISOv1\0");
    bytes[8..12].copy_from_slice(&u32::try_from(variant)?.to_le_bytes());
    bytes[12..16].copy_from_slice(
        &ordinal
            .checked_add(1)
            .ok_or_else(|| invalid_data("container ordinal overflow"))?
            .to_le_bytes(),
    );
    Ok(ContainerId::new(bytes)?)
}

fn validate_variant(path: &Path) -> io::Result<()> {
    let metadata = path.metadata()?;
    if !metadata.is_file() || metadata.len() != ISO_BYTES {
        return Err(invalid_data("variant is not the pinned ISO length"));
    }
    Ok(())
}

fn store_usage(root: &Path) -> io::Result<(usize, u64, u64)> {
    let mut count = 0_usize;
    let mut logical = 0_u64;
    let mut allocated = 0_u64;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("fdc") {
            continue;
        }
        let metadata = path.metadata()?;
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid_data("published file counter overflow"))?;
        logical = logical
            .checked_add(metadata.len())
            .ok_or_else(|| invalid_data("published byte counter overflow"))?;
        allocated = allocated
            .checked_add(
                metadata
                    .blocks()
                    .checked_mul(512)
                    .ok_or_else(|| invalid_data("allocated byte counter overflow"))?,
            )
            .ok_or_else(|| invalid_data("allocated byte counter overflow"))?;
    }
    Ok((count, logical, allocated))
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let index = samples
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[index]
}

fn variant_path(corpus: &Path, variant: usize) -> PathBuf {
    corpus.join(format!(
        "Rocky-10.2-x86_64-minimal.variant-{variant:02}.iso"
    ))
}

fn default_corpus() -> PathBuf {
    PathBuf::from("/source/fastdup/.artifacts/tier-data/corpus/rocky-minimal-variants-v1")
}

fn default_store() -> PathBuf {
    PathBuf::from("/source/fastdup/.artifacts/tier-data/iso-raw-ingest-v1")
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

struct FileRecipe {
    path: PathBuf,
    containers: Vec<ContainerRecipe>,
}

struct ContainerRecipe {
    id: ContainerId,
    generation: u64,
    chunks: Vec<(ChunkId, usize)>,
}

struct IngestResult {
    recipes: Vec<FileRecipe>,
    unique_chunk_ids: usize,
    exact_hits: u64,
    exact_hit_bytes: u64,
    logical_bytes: u64,
    publish_latencies: Vec<Duration>,
    ingest_elapsed: Duration,
}
