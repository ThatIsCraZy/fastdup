use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use fastdup_format::{ContainerId, ExactIndexEntry};
use fastdup_store::{ContainerRepository, FsStorageIo, StorageIo};

#[derive(Clone)]
struct DelayedCountingStorage {
    inner: FsStorageIo,
    count_records: Arc<AtomicBool>,
    record_reads: Arc<AtomicUsize>,
}

impl DelayedCountingStorage {
    fn open(root: &Path) -> Self {
        Self {
            inner: FsStorageIo::open(root).expect("open fixture storage"),
            count_records: Arc::new(AtomicBool::new(false)),
            record_reads: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl StorageIo for DelayedCountingStorage {
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
        if self.count_records.load(Ordering::Acquire)
            && offset == 4_096
            && Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("fdc"))
        {
            self.record_reads.fetch_add(1, Ordering::AcqRel);
            std::thread::sleep(Duration::from_millis(100));
        }
        self.inner.read_exact_at(name, offset, length)
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

#[test]
fn concurrent_sibling_misses_share_one_physical_record_read() {
    let root = test_root("record-read-singleflight");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only prior fixture root");
    }
    let storage = DelayedCountingStorage::open(&root);
    let repository = ContainerRepository::new(storage.clone());
    let first = b"first sibling payload".repeat(9_000);
    let second = b"second sibling payload".repeat(9_000);
    let container_id = ContainerId::new([0x61; 16]).expect("nonzero Container ID");
    repository
        .publish_adaptive_regions(container_id, 1, &[&[first.as_slice(), second.as_slice()]])
        .expect("publish one multi-Chunk Zstd Record");
    let container = repository
        .read(container_id)
        .expect("verify fixture Container");
    assert_eq!(container.zstd_record_count(), 1);
    let entries = container
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<Vec<_>, _>>()
        .expect("derive verified Exact entries");

    repository
        .read_verified_location(entries[0])
        .expect("warm only the descriptor cache");
    storage.record_reads.store(0, Ordering::Release);
    storage.count_records.store(true, Ordering::Release);

    let barrier = Arc::new(Barrier::new(3));
    let left_entry = entries[0];
    let right_entry = entries[1];
    let left_repository = repository.clone();
    let left_barrier = Arc::clone(&barrier);
    let left = std::thread::spawn(move || {
        left_barrier.wait();
        left_repository.read_verified_location(left_entry)
    });
    let right_repository = repository.clone();
    let right_barrier = Arc::clone(&barrier);
    let right = std::thread::spawn(move || {
        right_barrier.wait();
        right_repository.read_verified_location(right_entry)
    });
    barrier.wait();

    assert_eq!(
        left.join().expect("left reader joins").expect("left read"),
        first
    );
    assert_eq!(
        right
            .join()
            .expect("right reader joins")
            .expect("right read"),
        second
    );
    assert_eq!(
        storage.record_reads.load(Ordering::Acquire),
        1,
        "one Record leader must serve both sibling waiters"
    );
    std::fs::remove_dir_all(root).expect("remove only this fixture root");
}

fn test_root(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}", std::process::id()))
}
