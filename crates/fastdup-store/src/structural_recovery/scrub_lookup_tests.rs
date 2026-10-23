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

// GC and scrub use different complete-Container verifiers, but the same
// durable Base/index fixture exercises their shared hint trust boundary.
#[test]
fn gc_image_memoizes_verified_indexed_bases_per_container() {
    let fixture = Fixture::new("gc-memo", 1, 128);
    let index = fixture.indexes.pin_online_generation().unwrap().unwrap();
    let _scan = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
    let before = fixture.reads();
    let image = fixture
        .containers
        .read_verified_image_with_index(fixture.target, &index)
        .unwrap();
    let reads = fixture.reads() - before;
    assert!(
        reads <= 4,
        "one Base must not repeat Exact lookup 128 times: {reads}"
    );
    let reference = fixture.containers.read(fixture.target).unwrap();
    for record in reference.records() {
        assert_eq!(
            image.container().chunk(record.chunk_id()),
            Some(record.payload())
        );
    }
}

#[test]
fn gc_image_index_hint_does_not_hide_changed_base_payload() {
    let fixture = Fixture::new("gc-base-damage", 1, 8);
    let index = fixture.indexes.pin_online_generation().unwrap().unwrap();
    let _scan = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
    fixture
        .containers
        .read_verified_image_with_index(fixture.target, &index)
        .unwrap();
    let name = crate::published_name(fixture.base);
    let bytes = fixture.data.read(&name).unwrap();
    let offset = HEADER_BYTES + 256;
    fixture
        .data
        .write_at(&name, offset as u64, &[bytes[offset] ^ 1])
        .unwrap();
    assert!(
        fixture
            .containers
            .read_verified_image_with_index(fixture.target, &index)
            .is_err()
    );
}

#[test]
fn gc_image_bad_index_falls_back_but_missing_base_still_fails() {
    let fixture = Fixture::new("gc-index-damage", 1, 8);
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
    let file = crate::direct_io::open(&fixture.root.join("metadata").join(&run), true).unwrap();
    crate::direct_io::write(&file, offset as u64, &[bytes[offset] ^ 1]).unwrap();
    // Force the damaged Leaf to be observed, even if it was previously cached.
    let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
    fixture
        .containers
        .read_verified_image_with_index(fixture.target, &index)
        .unwrap();
    fixture
        .data
        .remove_file(&crate::published_name(fixture.base))
        .unwrap();
    assert!(matches!(
        fixture
            .containers
            .read_verified_image_with_index(fixture.target, &index),
        Err(StoreError::MissingVerifiedChunk { .. })
    ));
}

#[test]
fn gc_image_unusable_index_candidates_fall_back_to_verified_scan() {
    for case in ["absent", "wrong-location", "retiring", "removed"] {
        let fixture = Fixture::new(&format!("gc-{case}"), 1, 2);
        let reference = fixture.containers.read(fixture.target).unwrap();
        let base = fixture.containers.read(fixture.base).unwrap();
        let actual = ExactIndexEntry::from_verified(base.locations()[0]).unwrap();
        let wrong_id = ContainerId::new([0x10; 16]).unwrap();
        let wrong_bytes = vec![0x5a; actual.logical_length() as usize];
        fixture
            .containers
            .publish_raw(wrong_id, 3, &[&wrong_bytes])
            .unwrap();
        let wrong = fixture.containers.read(wrong_id).unwrap();
        let wrong = ExactIndexEntry::from_verified(wrong.locations()[0]).unwrap();
        let hint = match case {
            "absent" => wrong,
            "wrong-location" => ExactIndexEntry::active(
                actual.chunk_id(),
                actual.logical_length(),
                wrong.location(),
            )
            .unwrap(),
            "retiring" => ExactIndexEntry::retiring(actual).unwrap(),
            "removed" => {
                ExactIndexEntry::removed(ExactIndexEntry::retiring(actual).unwrap()).unwrap()
            }
            _ => unreachable!(),
        };
        let hints =
            ExactIndexRunRepository::new(FsStorageIo::open(fixture.root.join("hints")).unwrap());
        let profile = ExactIndexProfileId::new([0x93; 32]).unwrap();
        let run = hints
            .publish(&fastdup_format::ExactIndexRun::new(profile, 1, vec![hint]).unwrap())
            .unwrap();
        let runs = fastdup_format::ExactIndexRunSet::new(
            profile,
            1,
            vec![fastdup_format::ExactIndexRunRef::new(0, run).unwrap()],
        )
        .unwrap();
        let index = hints.activate(&runs).unwrap();
        let _scan = crate::ReadIntentScope::enter(crate::ReadIntent::Scan);
        let image = fixture
            .containers
            .read_verified_image_with_index(fixture.target, &index)
            .unwrap();
        for record in reference.records() {
            assert_eq!(
                image.container().chunk(record.chunk_id()),
                Some(record.payload()),
                "{case}"
            );
        }
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

#[test]
fn scrub_preparse_preserves_full_verifier_error_precedence() {
    let fixture = Fixture::new("error-order", 1, 1);
    let name = crate::published_name(fixture.target);
    let mut bytes = fixture.data.read(&name).unwrap();
    bytes[0] ^= 1;
    let footer = bytes.len() - usize::try_from(FOOTER_BYTES).unwrap();
    bytes[footer] ^= 1;
    let expected = fastdup_format::SealedContainer::verify_publication_with_dependent_resolver(
        &bytes,
        &mut |_| Err(fastdup_format::FormatError::DependentBaseRequired),
    )
    .unwrap_err();
    fixture.data.write_at(&name, 0, &bytes).unwrap();
    assert_eq!(
        format!("{:?}", fixture.scrub().unwrap_err()),
        format!("{:?}", StoreError::from(expected))
    );
}

#[test]
fn scrub_fresh_storage_heads_fall_back_without_hiding_missing_base() {
    use std::os::unix::fs::FileExt;
    let fixture = Fixture::new("head-damage", 1, 8);
    fixture.scrub().unwrap();
    let index = fixture.indexes.pin_online_generation().unwrap().unwrap();
    let run = fixture
        .metadata
        .list_names()
        .unwrap()
        .into_iter()
        .find(|name| name.ends_with(".fdx"))
        .unwrap();
    // Simulate media damage to both FDIO heads, bypassing immutable writers.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(fixture.root.join("metadata").join(run))
        .unwrap();
    file.write_all_at(&[0; 8192], 0).unwrap();
    file.sync_all().unwrap();
    let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
    let structure = fixture.containers.read_structure(fixture.target).unwrap();
    assert!(
        ScrubBaseHints::new(&structure, Some(&index))
            .lookups
            .is_empty()
    );
    fixture
        .containers
        .scrub_container(fixture.target, Some(&index))
        .unwrap();
    fixture
        .data
        .remove_file(&crate::published_name(fixture.base))
        .unwrap();
    assert!(
        fixture
            .containers
            .scrub_container(fixture.target, Some(&index))
            .is_err()
    );
}

#[test]
fn scrub_scattered_leaf_read_measurement() {
    let fixture = Fixture::new("scattered", 128, 128);
    let verified = fastdup_format::SealedContainer::verify_publication_with_hash_workers(
        &fixture
            .data
            .read(&crate::published_name(fixture.base))
            .unwrap(),
        std::num::NonZeroUsize::MIN,
    )
    .unwrap();
    let actual: Vec<_> = verified
        .locations()
        .iter()
        .copied()
        .map(|location| ExactIndexEntry::from_verified(location).unwrap())
        .collect();
    let writer = ExactIndexRunRepository::new(fixture.metadata.clone());
    for family in 0_u64..2 {
        let mut entries = actual.clone();
        // Untouched acceleration entries distribute requested hashes over a
        // large page space; their payload Locations are never requested.
        entries.extend((0_u64..65_536).map(|ordinal| {
            let key = ChunkId::of(&(ordinal + family * 65_536).to_le_bytes());
            ExactIndexEntry::active(key, actual[0].logical_length(), actual[0].location()).unwrap()
        }));
        writer
            .append_level_zero(ExactIndexProfileId::new([0x93; 32]).unwrap(), entries)
            .unwrap();
    }
    drop(writer);
    let reopened = ExactIndexRunRepository::new_with_memory_snapshot(
        fixture.metadata.clone(),
        MemoryPressureSnapshot::new(8 << 30, 6 << 30, 0),
    );
    let index = reopened.pin_online_generation().unwrap().unwrap();
    let structure = fixture.containers.read_structure(fixture.target).unwrap();
    let keys: BTreeSet<_> = structure
        .chunks()
        .iter()
        .filter_map(|chunk| chunk.dependency)
        .collect();
    let logical_bytes = || {
        fixture
            .counters
            .rows()
            .iter()
            .map(|row| row.returned_bytes)
            .sum::<u64>()
    };
    let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
    let before = logical_bytes();
    crate::direct_io::READ_BYTES.set(0);
    for &(id, length) in &keys {
        index.lookup_transitions(id, length).unwrap();
    }
    let serial_physical = crate::direct_io::READ_BYTES.get() as u64;
    let serial_heads = (serial_physical - (logical_bytes() - before)) / 8192;
    let before = logical_bytes();
    crate::direct_io::READ_BYTES.set(0);
    let hints = ScrubBaseHints::new(&structure, Some(&index));
    let batch_physical = crate::direct_io::READ_BYTES.get() as u64;
    let batch_heads = (batch_physical - (logical_bytes() - before)) / 8192;
    assert_eq!(hints.keys.len(), keys.len());
    assert_eq!(hints.lookups.len(), keys.len());
    assert!(
        batch_heads > 0 && batch_heads * 2 < serial_heads,
        "scattered leaves retain {batch_heads}/{serial_heads} header reads"
    );
    println!(
        "scattered_scrub keys={} serial_heads={serial_heads} batch_heads={batch_heads} serial_physical={serial_physical} batch_physical={batch_physical}",
        keys.len()
    );
    fixture
        .containers
        .scrub_container(fixture.target, Some(&index))
        .unwrap();
}
