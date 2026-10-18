use super::*;
use crate::{
    ExactIndexRunRepository, FsStorageIo, MemoryPressureSnapshot, SimilarityIndexRepository,
    VerifiedReadCacheConfig,
};
use fastdup_format::{ContainerId, ExactIndexEntry, ExactIndexProfileId};
use std::io;
use std::path::Path;
use std::sync::Mutex;
#[derive(Clone)]
struct ReadCountingStorage {
    inner: FsStorageIo,
    whole_reads: Arc<Mutex<usize>>,
    range_reads: Arc<Mutex<usize>>,
    namespace_reads: Arc<Mutex<usize>>,
}

impl ReadCountingStorage {
    fn open(root: &Path) -> Self {
        Self {
            inner: FsStorageIo::open(root).expect("create tracking storage"),
            whole_reads: Arc::new(Mutex::new(0)),
            range_reads: Arc::new(Mutex::new(0)),
            namespace_reads: Arc::new(Mutex::new(0)),
        }
    }
}

impl StorageIo for ReadCountingStorage {
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
        *self.whole_reads.lock().expect("whole-read counter") += 1;
        self.inner.read(name)
    }
    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.inner.object_len(name)
    }
    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        *self.range_reads.lock().expect("range-read counter") += 1;
        self.inner.read_exact_at(name, offset, length)
    }
    fn list_names(&self) -> io::Result<Vec<String>> {
        *self.namespace_reads.lock().expect("namespace-read counter") += 1;
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

#[test]
fn misleading_hints_learn_to_skip_data_reads_but_warm_and_demand_reads_still_verify() {
    let root = std::env::temp_dir().join(format!(
        "candidate-read-gate-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let storage = ReadCountingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    let mut seed = 71_u64;
    let base = (0..65536)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            u8::try_from(seed & 0xff).unwrap()
        })
        .collect::<Vec<_>>();
    let container_id = ContainerId::new([0xe9; 16]).unwrap();
    containers.publish_raw(container_id, 1, &[&base]).unwrap();
    let entry =
        ExactIndexEntry::from_verified(containers.read(container_id).unwrap().locations()[0])
            .unwrap();
    let exact = ExactIndexRunRepository::new(storage.clone());
    exact
        .append_level_zero(ExactIndexProfileId::new([0xe8; 32]).unwrap(), vec![entry])
        .unwrap();
    let active = exact.pin_active_generation().unwrap();
    let mut seed = 123_u64;
    let target = (0..65536)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            u8::try_from(seed & 255).unwrap()
        })
        .collect::<Vec<_>>();
    // Deliberately misleading acceleration: a perfect Sketch match is not
    // evidence of matching bytes, nor of a useful compression Base.
    let hint = crate::similarity_index_entry_v1(&target).unwrap();
    let similarities = SimilarityIndexRepository::new(storage.clone());
    let mut stager = similarities.entry_stager(1);
    stager
        .push(
            SimilarityIndexEntry::new(
                ChunkId::of(&base),
                65536,
                hint.fingerprint_profile(),
                hint.superfeatures(),
                hint.sketch(),
            )
            .unwrap(),
        )
        .unwrap();
    let publication = similarities
        .finish_staged_entries(1, stager, active.run_set().id().unwrap())
        .unwrap();
    similarities.activate_staged_family(publication).unwrap();
    let planner = PersistentReductionIndex::new(
        &active,
        Arc::new(similarities.recover_latest().unwrap().unwrap()),
    )
    .unwrap();

    let reads = || *storage.range_reads.lock().unwrap() + *storage.whole_reads.lock().unwrap();
    let plan = |cache| {
        planner
            .plan_chunk_for_publication_cached(&containers, ChunkId::of(&target), &target, cache)
            .unwrap()
            .0
    };
    for _ in 0..8 {
        assert!(matches!(plan(None), PersistentChunkPlan::Independent(_)));
    }
    assert_eq!(planner.status().backend_base_reads(), 8);
    let before = reads();
    for _ in 0..31 {
        assert!(matches!(plan(None), PersistentChunkPlan::Independent(_)));
    }
    // Exact and Similarity pages are now cached. No DATA or Metadata reads.
    assert_eq!(reads(), before);
    assert_eq!(planner.status().skipped_cold_candidates(), 31);
    assert!(matches!(plan(None), PersistentChunkPlan::Independent(_)));
    assert!(reads() > before);
    assert_eq!(planner.status().exploration_reads(), 1);
    assert_eq!(planner.status().backend_base_reads(), 9);

    let cache = VerifiedReadCache::new_with_snapshot(
        VerifiedReadCacheConfig::new(4 * 1024 * 1024, 0, NonZeroUsize::MIN).unwrap(),
        MemoryPressureSnapshot::new(64 * 1024 * 1024, 32 * 1024 * 1024, 0),
    )
    .unwrap();
    // Demand/Base resolution uses the ordinary path, irrespective of admission.
    let payload = containers
        .find_verified_independent_base_payload_with_index(
            &active,
            ChunkId::of(&base),
            65536,
            Some(&cache),
        )
        .unwrap();
    assert_eq!(payload.as_slice(), base);
    let before = reads();
    assert!(matches!(
        plan(Some(&cache)),
        PersistentChunkPlan::Independent(_)
    ));
    assert_eq!(reads(), before);
    assert_eq!(planner.status().warm_base_reuses(), 1);
    assert_eq!(planner.status().backend_base_reads(), 9);

    // A cold corrupted Base is never accepted even on the ordinary ungated path.
    let name = storage
        .list_names()
        .unwrap()
        .into_iter()
        .find(|n| n.ends_with(".fdc"))
        .unwrap();
    storage
        .write_at(&name, entry.location().record_offset(), &[0; 64])
        .unwrap();
    assert!(
        containers
            .find_verified_independent_base_payload_with_index(
                &active,
                ChunkId::of(&base),
                65536,
                None
            )
            .is_none()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn dense_bases_skip_xor_and_saturated_results_stop_base_loads() {
    let root = std::env::temp_dir().join(format!(
        "codec-preselection-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let storage = ReadCountingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    let mut seed = 71_u64;
    let base_a = (0..65536)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            u8::try_from(seed & 0xff).unwrap()
        })
        .collect::<Vec<_>>();
    let mut base_b = base_a.clone();
    base_b[40_000] ^= 0x5a;
    let container_id = ContainerId::new([0xe7; 16]).unwrap();
    containers
        .publish_raw(container_id, 1, &[&base_a, &base_b])
        .unwrap();
    let published = containers.read(container_id).unwrap();
    let entries = vec![
        ExactIndexEntry::from_verified(published.locations()[0]).unwrap(),
        ExactIndexEntry::from_verified(published.locations()[1]).unwrap(),
    ];
    let exact = ExactIndexRunRepository::new(storage.clone());
    exact
        .append_level_zero(ExactIndexProfileId::new([0xe6; 32]).unwrap(), entries)
        .unwrap();
    let active = exact.pin_active_generation().unwrap();
    let similarities = SimilarityIndexRepository::new(storage);
    let mut stager = similarities.entry_stager(1);
    stager
        .push(crate::similarity_index_entry_v1(&base_a).unwrap())
        .unwrap();
    stager
        .push(crate::similarity_index_entry_v1(&base_b).unwrap())
        .unwrap();
    let publication = similarities
        .finish_staged_entries(1, stager, active.run_set().id().unwrap())
        .unwrap();
    similarities.activate_staged_family(publication).unwrap();
    let planner = PersistentReductionIndex::new(
        &active,
        Arc::new(similarities.recover_latest().unwrap().unwrap()),
    )
    .unwrap();
    let plan = |target: &[u8]| {
        planner
            .plan_chunk_for_publication(&containers, ChunkId::of(target), target)
            .unwrap()
            .0
    };

    // Sparse edit outside every probe: the XOR trial runs and the tiny
    // dependent result saturates, so the second candidate Base is never
    // read from DATA even though two candidates were ranked.
    let mut sparse = base_a.clone();
    sparse[100] ^= 0x5a;
    assert!(matches!(plan(&sparse), PersistentChunkPlan::Dependent(_)));
    let after_sparse = planner.status();
    assert!(after_sparse.candidates() >= 2);
    assert_eq!(after_sparse.backend_base_reads(), 1);
    assert!(after_sparse.sparse_xor_trials() >= 1);
    assert_eq!(after_sparse.dense_xor_skips(), 0);

    // One flipped byte inside every sampled probe classifies dense: the XOR
    // trial is skipped and ZSTD_PREFIX carries the dependent result.
    let mut dense = base_a.clone();
    for block in 0..16 {
        dense[block * 4096 + 7] ^= 0x5a;
    }
    assert!(matches!(plan(&dense), PersistentChunkPlan::Dependent(_)));
    let after_dense = planner.status();
    assert!(after_dense.dense_xor_skips() >= 1);
    assert_eq!(
        after_dense.sparse_xor_trials(),
        after_sparse.sparse_xor_trials()
    );
    assert!(after_dense.accepted_prefixes() > after_sparse.accepted_prefixes());

    // Warm-first: with Base B resident, the resident candidate is tried
    // before the Sketch-closer cold Base A, so this plan needs no DATA read.
    let cache = VerifiedReadCache::new_with_snapshot(
        VerifiedReadCacheConfig::new(4 * 1024 * 1024, 0, NonZeroUsize::MIN).unwrap(),
        MemoryPressureSnapshot::new(64 * 1024 * 1024, 32 * 1024 * 1024, 0),
    )
    .unwrap();
    let payload = containers
        .find_verified_independent_base_payload_with_index(
            &active,
            ChunkId::of(&base_b),
            65536,
            Some(&cache),
        )
        .unwrap();
    assert_eq!(payload.as_slice(), base_b);
    let before_warm = planner.status();
    let (warm_plan, _) = planner
        .plan_chunk_for_publication_cached(&containers, ChunkId::of(&sparse), &sparse, Some(&cache))
        .unwrap();
    assert!(matches!(warm_plan, PersistentChunkPlan::Dependent(_)));
    let after_warm = planner.status();
    assert_eq!(
        after_warm.backend_base_reads(),
        before_warm.backend_base_reads()
    );
    assert!(after_warm.warm_base_reuses() > before_warm.warm_base_reuses());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn length_band_candidates_reach_flexible_prefix_trials() {
    let root = std::env::temp_dir().join(format!(
        "length-band-candidates-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let storage = ReadCountingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    let mut seed = 71_u64;
    let stream = (0..70_000)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            u8::try_from(seed & 0xff).unwrap()
        })
        .collect::<Vec<_>>();
    let base = stream[..65_536].to_vec();
    let container_id = ContainerId::new([0xe5; 16]).unwrap();
    containers.publish_raw(container_id, 1, &[&base]).unwrap();
    let entry =
        ExactIndexEntry::from_verified(containers.read(container_id).unwrap().locations()[0])
            .unwrap();
    let exact = ExactIndexRunRepository::new(storage.clone());
    exact
        .append_level_zero(ExactIndexProfileId::new([0xe4; 32]).unwrap(), vec![entry])
        .unwrap();
    let active = exact.pin_active_generation().unwrap();
    let similarities = SimilarityIndexRepository::new(storage);
    let mut stager = similarities.entry_stager(1);
    stager
        .push(crate::similarity_index_entry_v1(&base).unwrap())
        .unwrap();
    let publication = similarities
        .finish_staged_entries(1, stager, active.run_set().id().unwrap())
        .unwrap();
    similarities.activate_staged_family(publication).unwrap();
    let planner = PersistentReductionIndex::new(
        &active,
        Arc::new(similarities.recover_latest().unwrap().unwrap()),
    )
    .unwrap();
    let plan = |target: &[u8]| {
        planner
            .plan_chunk_for_publication(&containers, ChunkId::of(target), target)
            .unwrap()
            .0
    };

    // Same length band (65_536..73_728), different exact length: the Base is
    // a candidate and the flexible Prefix trial wins. Under exact-length
    // buckets this target found no candidate at all.
    let longer = stream.clone();
    assert!(matches!(plan(&longer), PersistentChunkPlan::Dependent(_)));
    let status = planner.status();
    assert!(status.candidates() >= 1);
    assert_eq!(status.accepted_prefixes(), 1);
    assert_eq!(status.errors(), 0);

    // A target in a different length band stays outside the candidate set.
    let shorter = stream[..20_000].to_vec();
    assert!(!matches!(plan(&shorter), PersistentChunkPlan::Dependent(_)));
    std::fs::remove_dir_all(root).unwrap();
}
