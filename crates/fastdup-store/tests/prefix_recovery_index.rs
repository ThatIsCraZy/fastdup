use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use fastdup_format::{ChunkId, ContainerId};
use fastdup_store::{ContainerRepository, FsStorageIo, StorageIo};

#[derive(Clone)]
struct ReadCountingStorage {
    inner: FsStorageIo,
    whole_reads: Arc<Mutex<usize>>,
    range_reads: Arc<Mutex<usize>>,
    namespace_reads: Arc<Mutex<usize>>,
    range_bytes: Arc<Mutex<usize>>,
    ranges: Arc<Mutex<Vec<(String, u64, usize)>>>,
}

impl ReadCountingStorage {
    fn open(root: &Path) -> Self {
        Self {
            inner: FsStorageIo::open(root).expect("create tracking storage"),
            whole_reads: Arc::new(Mutex::new(0)),
            range_reads: Arc::new(Mutex::new(0)),
            namespace_reads: Arc::new(Mutex::new(0)),
            range_bytes: Arc::new(Mutex::new(0)),
            ranges: Arc::new(Mutex::new(Vec::new())),
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
        *self.range_bytes.lock().unwrap() += length;
        self.ranges
            .lock()
            .unwrap()
            .push((name.to_owned(), offset, length));
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
fn prefix_container_resolves_repeated_base_without_repeated_namespace_or_whole_provider_reads() {
    let root = test_root("prefix-recovery-index");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let storage = ReadCountingStorage::open(&root);
    let repository = ContainerRepository::new(storage.clone());
    let base = deterministic_bytes(64 * 1_024, 29);
    let targets = (0..4_u8)
        .map(|ordinal| {
            let mut target = base.clone();
            target[usize::from(ordinal) * 257] ^= ordinal + 1;
            target
        })
        .collect::<Vec<_>>();
    repository
        .publish_raw(id(0x21), 1, &[base.as_slice()])
        .expect("independent Base publishes");
    let pairs = targets
        .iter()
        .map(|target| (base.as_slice(), target.as_slice()))
        .collect::<Vec<_>>();
    repository
        .publish_zstd_prefix_pairs_verified(id(0x31), 2, &pairs)
        .expect("dependent Container publishes");
    let whole_reads_before = *storage.whole_reads.lock().expect("whole-read counter");
    let namespace_reads_before = *storage
        .namespace_reads
        .lock()
        .expect("namespace-read counter");

    let decoded = repository
        .read(id(0x31))
        .expect("index-free Prefix read resolves its durable Base");

    for target in &targets {
        assert_eq!(decoded.chunk(ChunkId::of(target)), Some(target.as_slice()));
    }
    assert_eq!(
        *storage
            .namespace_reads
            .lock()
            .expect("namespace-read counter")
            - namespace_reads_before,
        1,
        "one Prefix decode owns one pass-local namespace snapshot"
    );
    assert_eq!(
        *storage.whole_reads.lock().expect("whole-read counter") - whole_reads_before,
        1,
        "only the requested dependent Container is read in full"
    );
    std::fs::remove_dir_all(root).expect("remove only this test repository");
}

fn test_root(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}", std::process::id()))
}

fn id(byte: u8) -> ContainerId {
    ContainerId::new([byte; 16]).expect("fixture Container ID is nonzero")
}

fn deterministic_bytes(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

#[test]
fn missing_exact_index_reads_only_required_records_for_demand_and_commit() {
    use fastdup_store::RequiredChunkVerifier;
    use std::collections::BTreeMap;
    let root = test_root("bounded-fallback");
    let storage = ReadCountingStorage::open(&root);
    let repository = ContainerRepository::new(storage.clone());
    let unrelated = deterministic_bytes(256 * 1024, 41);
    let wanted = deterministic_bytes(8192, 42);
    repository.publish_raw(id(0x11), 1, &[&unrelated]).unwrap();
    repository
        .publish_raw(id(0x22), 2, &[&unrelated, &wanted])
        .unwrap();
    *storage.whole_reads.lock().unwrap() = 0;
    *storage.range_bytes.lock().unwrap() = 0;
    assert_eq!(
        repository
            .read_verified_chunk(ChunkId::of(&wanted), wanted.len() as u64)
            .unwrap(),
        wanted
    );
    let required = BTreeMap::from([(ChunkId::of(&wanted), wanted.len() as u64)]);
    repository.verify_required_chunks(&required).unwrap();
    assert_eq!(
        *storage.whole_reads.lock().unwrap(),
        0,
        "a missing Exact hint must not load whole Containers"
    );
    println!(
        "bounded fallback: whole_reads={} range_bytes={}",
        *storage.whole_reads.lock().unwrap(),
        *storage.range_bytes.lock().unwrap()
    );
    assert!(
        *storage.range_bytes.lock().unwrap() < 64 * 1024,
        "read only envelopes, local indexes and required Records"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn fallback_verifies_dependent_targets_and_rejects_damaged_base_and_target() {
    use fastdup_store::RequiredChunkVerifier;
    use std::collections::BTreeMap;
    let root = test_root("fallback-dependent-integrity");
    let storage = ReadCountingStorage::open(&root);
    let repository = ContainerRepository::new(storage.clone());
    let base = deterministic_bytes(64 * 1024, 77);
    let mut target = base.clone();
    target[99] ^= 1;
    repository.publish_raw(id(0x41), 1, &[&base]).unwrap();
    repository
        .publish_zstd_prefix_pairs_verified(id(0x42), 2, &[(&base, &target)])
        .unwrap();
    let required = BTreeMap::from([(ChunkId::of(&target), target.len() as u64)]);
    *storage.whole_reads.lock().unwrap() = 0;
    repository.verify_required_chunks(&required).unwrap();
    assert_eq!(
        repository
            .read_verified_chunk(ChunkId::of(&target), target.len() as u64)
            .unwrap(),
        target
    );
    assert_eq!(*storage.whole_reads.lock().unwrap(), 0);
    for container in [id(0x41), id(0x42)] {
        let name = format!(
            "{}.fdc",
            container
                .bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let offset = 4096 + 192;
        let byte = storage.inner.read_exact_at(&name, offset, 1).unwrap()[0];
        storage.write_at(&name, offset, &[byte ^ 0x80]).unwrap();
        assert!(
            repository
                .read_verified_chunk(ChunkId::of(&target), target.len() as u64)
                .is_err()
        );
        assert!(repository.verify_required_chunks(&required).is_err());
        assert!(
            repository
                .scrub_container::<FsStorageIo>(container, None)
                .is_err()
        );
        storage.write_at(&name, offset, &[byte]).unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incomplete_exact_index_preserves_checked_records_and_uses_later_hints() {
    use fastdup_format::{ExactIndexEntry, ExactIndexProfileId};
    use fastdup_store::{
        ExactIndexRunRepository, IndexedRequiredChunkVerifier, RequiredChunkVerifier,
    };
    use std::collections::BTreeMap;
    let root = test_root("fallback-partial-index");
    let storage = ReadCountingStorage::open(&root);
    let repository = ContainerRepository::new(storage.clone());
    let mut chunks = (1..=3)
        .map(|seed| deterministic_bytes(8192, seed))
        .collect::<Vec<_>>();
    chunks.sort_by_key(|bytes| ChunkId::of(bytes));
    repository
        .publish_raw(
            id(0x51),
            1,
            &chunks.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        )
        .unwrap();
    let container = repository.read(id(0x51)).unwrap();
    let entries = container
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let indexes = ExactIndexRunRepository::new(storage.clone());
    indexes
        .append_level_zero(
            ExactIndexProfileId::new([0x51; 32]).unwrap(),
            vec![entries[0], entries[2]],
        )
        .unwrap();
    let verifier = IndexedRequiredChunkVerifier::new(
        repository.clone(),
        indexes.pin_active_generation().unwrap(),
    );
    let required = chunks
        .iter()
        .map(|bytes| (ChunkId::of(bytes), bytes.len() as u64))
        .collect::<BTreeMap<_, _>>();
    storage.ranges.lock().unwrap().clear();
    *storage.whole_reads.lock().unwrap() = 0;
    verifier.verify_required_chunks(&required).unwrap();
    assert_eq!(*storage.whole_reads.lock().unwrap(), 0);
    for entry in entries {
        assert_eq!(
            storage
                .ranges
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, offset, _)| name.ends_with(".fdc")
                    && *offset == entry.location().record_offset())
                .count(),
            1,
            "each required Record is verified once even when an earlier hint is absent"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn fallback_does_not_read_unrelated_payload_but_scrub_still_checks_it() {
    let root = test_root("fallback-unrelated-corruption");
    let storage = ReadCountingStorage::open(&root);
    let repository = ContainerRepository::new(storage.clone());
    let unrelated = deterministic_bytes(256 * 1024, 90);
    let wanted = deterministic_bytes(8192, 91);
    repository
        .publish_raw(id(0x61), 1, &[&unrelated, &wanted])
        .unwrap();
    let name = "61616161616161616161616161616161.fdc";
    let byte = storage.inner.read_exact_at(name, 4096 + 192, 1).unwrap()[0];
    storage.write_at(name, 4096 + 192, &[byte ^ 1]).unwrap();
    assert_eq!(
        repository
            .read_verified_chunk(ChunkId::of(&wanted), wanted.len() as u64)
            .unwrap(),
        wanted
    );
    assert!(
        repository
            .scrub_container::<FsStorageIo>(id(0x61), None)
            .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn fallback_verifies_all_required_siblings_once_and_rejects_corrupt_index() {
    use fastdup_store::RequiredChunkVerifier;
    let root = test_root("fallback-siblings");
    let storage = ReadCountingStorage::open(&root);
    let repository = ContainerRepository::new(storage.clone());
    let chunks = (1_u8..=8)
        .map(|value| vec![value; 16384])
        .collect::<Vec<_>>();
    let parts = chunks.iter().map(Vec::as_slice).collect::<Vec<_>>();
    repository
        .publish_adaptive_regions(id(0x71), 1, &[&parts])
        .unwrap();
    let required = chunks
        .iter()
        .map(|b| (ChunkId::of(b), b.len() as u64))
        .collect();
    storage.ranges.lock().unwrap().clear();
    repository.verify_required_chunks(&required).unwrap();
    assert_eq!(
        storage
            .ranges
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, offset, _)| *offset == 4096)
            .count(),
        1
    );
    let name = "71717171717171717171717171717171.fdc";
    let image = storage.inner.read(name).unwrap();
    let envelope = fastdup_format::ContainerRecoveryEnvelope::decode(
        &image[..4096],
        &image[image.len() - 4096..],
        image.len() as u64,
    )
    .unwrap();
    let offset = envelope.recovery_index_range().unwrap().offset() + 64;
    let byte = storage.inner.read_exact_at(name, offset, 1).unwrap()[0];
    storage.write_at(name, offset, &[byte ^ 1]).unwrap();
    assert!(repository.verify_required_chunks(&required).is_err());
    assert!(
        repository
            .read_verified_chunk(ChunkId::of(&chunks[0]), chunks[0].len() as u64)
            .is_err()
    );
    assert!(
        repository
            .scrub_container::<FsStorageIo>(id(0x71), None)
            .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}
