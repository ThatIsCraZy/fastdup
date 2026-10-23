use super::*;
use crate::{ExactIndexRunRepository, FsStorageIo, MemoryPressureSnapshot};
use fastdup_format::{ExactIndexEntry, ExactIndexProfileId};
use std::sync::Arc;

struct Fixture {
    root: std::path::PathBuf,
    data: FsStorageIo,
    metadata: FsStorageIo,
    containers: ContainerRepository<FsStorageIo>,
    indexes: ExactIndexRunRepository<FsStorageIo>,
    counters: Arc<crate::metadata_read_telemetry::MetadataReadCounters>,
    target: ContainerId,
    base: ContainerId,
}

impl Fixture {
    fn new(label: &str, bases: usize, targets: usize) -> Self {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.artifacts/tests")
            .join(format!("scrub-lookups-{label}-{}", std::process::id()));
        if root.exists() {
            std::fs::remove_dir_all(&root).unwrap();
        }
        let data = FsStorageIo::open(root.join("data")).unwrap();
        let containers = ContainerRepository::new(data.clone());
        let mut state = 71_u64;
        let bases: Vec<Vec<u8>> = (0..bases)
            .map(|_| {
                (0..65_536)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state.to_le_bytes()[0]
                    })
                    .collect()
            })
            .collect();
        let base = ContainerId::new([0x91; 16]).unwrap();
        let target = ContainerId::new([0x92; 16]).unwrap();
        containers
            .publish_raw(
                base,
                1,
                &bases.iter().map(Vec::as_slice).collect::<Vec<_>>(),
            )
            .unwrap();
        let verified = fastdup_format::SealedContainer::verify_publication_with_hash_workers(
            &data.read(&crate::published_name(base)).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        let targets: Vec<Vec<u8>> = (0..targets)
            .map(|ordinal| {
                let mut bytes = bases[ordinal % bases.len()].clone();
                bytes[500 + ordinal] ^= 0x42;
                bytes
            })
            .collect();
        let pairs: Vec<_> = targets
            .iter()
            .enumerate()
            .map(|(ordinal, bytes)| (bases[ordinal % bases.len()].as_slice(), bytes.as_slice()))
            .collect();
        containers
            .publish_zstd_prefix_pairs_verified(target, 2, &pairs)
            .unwrap();
        let counters = Arc::new(crate::metadata_read_telemetry::MetadataReadCounters::default());
        let mut index_storage = FsStorageIo::open(root.join("metadata")).unwrap();
        index_storage.metadata_reads = Some(Arc::clone(&counters));
        let writer = ExactIndexRunRepository::new(index_storage.clone());
        writer
            .append_level_zero(
                ExactIndexProfileId::new([0x93; 32]).unwrap(),
                verified
                    .locations()
                    .iter()
                    .copied()
                    .map(|location| ExactIndexEntry::from_verified(location).unwrap())
                    .collect(),
            )
            .unwrap();
        drop(writer);
        // A cold reopen audits stored bounds, just like the runtime's initial
        // scrub. Writer-born bounds may not steer Independent lookups.
        let indexes = ExactIndexRunRepository::new_with_memory_snapshot(
            index_storage.clone(),
            MemoryPressureSnapshot::new(8 << 30, 6 << 30, 0),
        );
        indexes.pin_online_generation().unwrap().unwrap();
        Self {
            root,
            data,
            metadata: index_storage,
            containers,
            indexes,
            counters,
            target,
            base,
        }
    }

    fn reads(&self) -> u64 {
        self.counters
            .rows()
            .iter()
            .filter(|row| row.object == "exactIndex")
            .map(|row| row.operations)
            .sum()
    }

    fn scrub(&self) -> Result<u64, StoreError> {
        self.containers.scrub_container(
            self.target,
            Some(&self.indexes.pin_online_generation().unwrap().unwrap()),
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn scrub_batches_repeated_base_index_reads() {
    let fixture = Fixture::new("repeated", 1, 128);
    let before = fixture.reads();
    fixture.scrub().unwrap();
    let reads = fixture.reads() - before;
    eprintln!("Exact reads: {reads}");
    assert!(
        reads <= 4,
        "128 dependents sharing one Base incurred {reads} Exact reads; \
         one bounded scrub operation must merge repeated Base lookups"
    );
    // A second complete scrub still obtains fresh index bytes.
    let before = fixture.reads();
    fixture.scrub().unwrap();
    assert!(fixture.reads() > before);
}

#[test]
fn scrub_batches_distinct_base_index_reads() {
    let fixture = Fixture::new("windows", 160, 160);
    let before = fixture.reads();
    fixture.scrub().unwrap();
    let reads = fixture.reads() - before;
    eprintln!("Exact reads: {reads}");
    assert!(
        reads <= 12,
        "160 Bases incurred {reads} unmerged Exact reads"
    );
}

#[test]
fn scrub_hint_bound_preserves_overflow_resolution() {
    let fixture = Fixture::new("overflow", 320, 320);
    let structure = fixture.containers.read_structure(fixture.target).unwrap();
    let index = fixture.indexes.pin_online_generation().unwrap().unwrap();
    let hints = ScrubBaseHints::new(&structure, Some(&index));
    assert_eq!(hints.keys.len(), SCRUB_BASE_HINT_KEYS);
    assert_eq!(hints.lookups.len(), SCRUB_BASE_HINT_KEYS);
    let before = fixture.reads();
    fixture.scrub().unwrap();
    let reads = fixture.reads() - before;
    assert!(reads <= 80, "overflow fallback incurred {reads} reads");
}

#[test]
fn scrub_batched_hints_do_not_hide_changed_base_payload() {
    let fixture = Fixture::new("base-damage", 1, 8);
    fixture.scrub().unwrap();
    let name = crate::published_name(fixture.base);
    let bytes = fixture.data.read(&name).unwrap();
    let offset = HEADER_BYTES + 256;
    fixture
        .data
        .write_at(&name, offset as u64, &[bytes[offset] ^ 1])
        .unwrap();
    assert!(fixture.scrub().is_err());
}

#[test]
fn scrub_corrupt_batch_hint_falls_back_to_verified_data() {
    let fixture = Fixture::new("index-damage", 1, 8);
    fixture.scrub().unwrap();
    let index = fixture.indexes.pin_online_generation().unwrap().unwrap();
    let run = fixture
        .metadata
        .list_names()
        .unwrap()
        .into_iter()
        .find(|name| name.ends_with(".fdx"))
        .unwrap();
    let bytes = fixture.metadata.read(&run).unwrap();
    let offset = 4096 + 256;
    // Fault injection bypasses the normal immutable-reader write exclusion.
    let file = crate::direct_io::open(&fixture.root.join("metadata").join(&run), true).unwrap();
    crate::direct_io::write(&file, offset as u64, &[bytes[offset] ^ 1]).unwrap();
    // The page must be read freshly despite a previous successful scrub.
    let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
    let structure = fixture.containers.read_structure(fixture.target).unwrap();
    let hints = ScrubBaseHints::new(&structure, Some(&index));
    assert!(hints.lookups.is_empty());
    fixture
        .containers
        .scrub_container(fixture.target, Some(&index))
        .unwrap();
}

#[test]
fn scrub_worker_attributes_exact_reads_to_recovery() {
    let fixture = Fixture::new("attribution", 1, 8);
    let snapshot = MemoryPressureSnapshot::new(8 << 30, 6 << 30, 0);
    let cache = crate::VerifiedReadCache::new_with_snapshot(
        crate::VerifiedReadCacheConfig::conservative(snapshot),
        snapshot,
    )
    .unwrap();
    let pool = crate::ScrubResumePool::new().unwrap();
    let mut coverage = crate::ScrubCoverage::new(PendingDataVerification::new(BTreeMap::new()));
    let before = fixture.reads();
    let index = fixture.indexes.pin_online_generation().unwrap().unwrap();
    pool.verify(
        &fixture.containers,
        &[fixture.target],
        Some(&index),
        &mut coverage,
        1,
        &cache,
    )
    .unwrap();
    let reads = fixture.reads() - before;
    let attributed: u64 = fixture
        .counters
        .rows()
        .iter()
        .filter(|row| row.object == "exactIndex" && row.reason == "recoveryScrub")
        .map(|row| row.operations)
        .sum();
    assert!(reads > 0);
    assert_eq!(attributed, reads);
}
