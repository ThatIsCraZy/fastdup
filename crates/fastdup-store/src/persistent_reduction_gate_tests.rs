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
