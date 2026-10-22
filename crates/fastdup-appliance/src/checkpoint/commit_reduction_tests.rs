//! Exercise the real commit writer against cold, latency-bearing Similarity reads.
use super::*;
use fastdup_posix::NamespaceConfig;
use fastdup_store::{
    ExactIndexRunRepository, MetadataReadReason, MetadataReadScope, SimilarityIndexRepository,
};
use fastdup_testkit::MemoryStorageIo;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
struct MeasuredIndexIo {
    inner: MemoryStorageIo,
    reads: Arc<ReadMeasurements>,
}

#[derive(Default)]
struct ReadMeasurements {
    armed: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
    count: AtomicUsize,
}

impl StorageIo for MeasuredIndexIo {
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
        let measured = self.reads.armed.load(Ordering::SeqCst)
            && std::path::Path::new(name).extension() == Some(std::ffi::OsStr::new("fds"))
            && MetadataReadScope::current() == MetadataReadReason::IndexLookup;
        if measured {
            let active = self.reads.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.reads.peak.fetch_max(active, Ordering::SeqCst);
            self.reads.count.fetch_add(1, Ordering::SeqCst);
            // Match the order of the appliance's measured 1.8--3 ms Metadata reads.
            std::thread::sleep(Duration::from_millis(2));
        }
        let result = self.inner.read_exact_at(name, offset, length);
        if measured {
            self.reads.active.fetch_sub(1, Ordering::SeqCst);
        }
        result
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
    fn publish_noreplace(&self, temporary: &str, published: &str) -> io::Result<()> {
        self.inner.publish_noreplace(temporary, published)
    }
    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.inner.remove_file(name)
    }
    fn sync_root(&self) -> io::Result<()> {
        self.inner.sync_root()
    }
}

struct Measurement {
    elapsed: Duration,
    peak_reads: usize,
    reads: usize,
    file_bytes: u64,
    accepted: u64,
    decisions: Vec<(ChunkId, u16, u32, [u8; 32])>,
}

#[allow(
    clippy::too_many_lines,
    reason = "one fixture keeps setup, timed commit and independent verification together"
)]
fn commit_similar_chunks(workers: usize, count: usize, mixed: bool, held: usize) -> Measurement {
    let mut state = 71_u64;
    let bases = (0..count)
        .map(|_| {
            (0..65_536)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state.to_le_bytes()[0]
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let data = MemoryStorageIo::new();
    let containers = ContainerRepository::new(data.clone());
    let base_id = ContainerId::new([0xb7; 16]).unwrap();
    let refs = bases.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let allocator = containers.open_generation_allocator(1_024).unwrap();
    containers
        .publish_raw(base_id, allocator.reserve_generation().unwrap(), &refs)
        .unwrap();
    let index_io = MeasuredIndexIo::default();
    let exact = ExactIndexRunRepository::new(index_io.clone());
    let similarity = SimilarityIndexRepository::new(index_io.clone());
    let metadata = MemoryStorageIo::new();
    fastdup_store::MaintenanceRepository::new(
        GenerationRepository::new(metadata.clone(), super::super::checkpoint_policy_set()),
        containers.clone(),
        exact.clone(),
        super::super::checkpoint_exact_index_profile_v1(),
    )
    .rebuild_pool_indexes(&similarity)
    .unwrap();
    let appliance = DurableNamespace::open_with_reduction_indexes(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, super::super::checkpoint_policy_set()),
        containers.clone(),
        &exact,
        &similarity,
        32,
    )
    .unwrap();
    appliance.online_dependency_proofs.freeze_for_commit();
    let permits = fastdup_store::WorkerPermits::new(NonZeroUsize::new(workers).unwrap());
    let other_stage = NonZeroUsize::new(held).map(|held| permits.acquire(held));
    let mut writer = AdaptiveCommitWriter::new(
        &appliance.containers,
        &appliance.container_generations,
        appliance.manifest_readers.as_ref(),
        NonZeroUsize::new(workers).unwrap(),
        &permits,
        Arc::clone(&appliance.online_dependency_proofs),
        Vec::new(),
    );
    writer
        .begin_inode(InodeId::new(2).unwrap(), ContainerPlacement::Data, true)
        .unwrap();
    let targets = bases
        .into_iter()
        .enumerate()
        .map(|(i, mut bytes)| {
            if mixed {
                match i % 4 {
                    0 => bytes.extend_from_slice(&[0x53; 127]),
                    2 => return vec![0x42; 65_536],
                    3 => {
                        for byte in &mut bytes {
                            *byte ^= 0xa5;
                        }
                    }
                    _ => (),
                }
            }
            bytes[100 + i] ^= 0x5a;
            bytes
        })
        .collect::<Vec<_>>();
    for bytes in &targets {
        writer.push(ChunkId::of(bytes), bytes.clone()).unwrap();
    }
    index_io.reads.armed.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let (_, metrics, _) = writer.finish().unwrap();
    let elapsed = start.elapsed();
    assert_eq!(
        permits.available(),
        workers - held,
        "commit leaked CPU permits"
    );
    drop(other_stage);
    index_io.reads.armed.store(false, Ordering::SeqCst);
    appliance.manifest_readers.flush_level_zero();
    let active = exact.pin_active_generation().unwrap();
    // Independent reads validate the published target order/identities and Bases.
    let mut decisions = Vec::new();
    for bytes in &targets {
        assert_eq!(
            containers
                .read_verified_chunk_with_index(&active, ChunkId::of(bytes), bytes.len() as u64)
                .unwrap(),
            *bytes
        );
        let entry = active
            .active_reference(
                ChunkId::of(bytes),
                u32::try_from(bytes.len()).unwrap(),
                None,
            )
            .unwrap()
            .unwrap();
        let location = entry.location();
        decisions.push((
            entry.chunk_id(),
            location.codec_id(),
            location.record_payload_length(),
            location.dependency_id(),
        ));
    }
    let reduction = appliance.advanced_reduction_status();
    let accepted = reduction.accepted_prefixes() + reduction.accepted_sparse_xor();
    assert_eq!(
        accepted,
        (if mixed { count / 2 } else { count }) as u64,
        "fixture must exercise real dependent planning"
    );
    assert_eq!(reduction.errors(), 0);
    appliance.online_dependency_proofs.complete_frozen();
    Measurement {
        elapsed,
        peak_reads: index_io.reads.peak.load(Ordering::SeqCst),
        reads: index_io.reads.count.load(Ordering::SeqCst),
        file_bytes: metrics.container_file_bytes,
        accepted,
        decisions,
    }
}

#[test]
fn commit_similarity_overlaps_cold_index_reads() {
    // The overlap invariant must also run on single-vCPU builders or with a
    // one-thread global Rayon pool. The measured I/O releases the CPU.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let measured = pool.install(|| commit_similar_chunks(4, 32, false, 0));
    assert!(
        measured.reads > 0,
        "fixture must reach cold Similarity reads"
    );
    assert!(
        measured.peak_reads > 1,
        "commit serialized all {} cold Similarity reads ({:?})",
        measured.reads,
        measured.elapsed
    );
    assert!(measured.peak_reads <= 4, "commit exceeded its worker limit");
}

#[test]
fn commit_similarity_preserves_mixed_records_and_partial_cpu_admission() {
    let serial = commit_similar_chunks(1, 32, true, 0);
    let parallel = commit_similar_chunks(4, 32, true, 2);
    assert_eq!(serial.decisions, parallel.decisions);
    assert_eq!(serial.file_bytes, parallel.file_bytes);
    assert!(
        parallel.decisions.iter().any(|row| row.1 == 5),
        "fixture includes different-length Prefix Bases"
    );
    assert!(
        parallel.decisions.iter().any(|row| row.3 == [0; 32]),
        "fixture includes independent records"
    );
    assert!(
        parallel.peak_reads <= 2,
        "commit overbooked permits held by another stage"
    );
}

#[test]
#[ignore = "controlled A/B benchmark; run alone in release mode with --nocapture"]
fn checkpoint_similarity_benchmark() {
    for workers in [1, 4] {
        let m = commit_similar_chunks(workers, 128, false, 0);
        println!(
            "checkpoint_similarity workers={workers} chunks=128 elapsed_ms={} peak_reads={} reads={} file_bytes={} accepted={}",
            m.elapsed.as_millis(),
            m.peak_reads,
            m.reads,
            m.file_bytes,
            m.accepted
        );
    }
}
