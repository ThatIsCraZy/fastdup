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
