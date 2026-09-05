use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexProfileId, ManifestExtent, ManifestLeaf,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, MAX_STORAGE_RANGE_BYTES,
    ManifestReadError, StorageIo, StoreError, VerifiedManifestFile,
};

#[derive(Clone)]
struct RangeTrackingStorage {
    inner: FsStorageIo,
    range_reads: Arc<Mutex<Vec<(String, u64, usize)>>>,
}

impl RangeTrackingStorage {
    fn open(root: &Path) -> Self {
        Self {
            inner: FsStorageIo::open(root).expect("create range-tracking storage"),
            range_reads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn clear_range_reads(&self) {
        self.range_reads.lock().expect("range-read lock").clear();
    }

    fn data_range_reads(&self) -> Vec<(String, u64, usize)> {
        self.range_reads
            .lock()
            .expect("range-read lock")
            .iter()
            .filter(|(name, _, _)| {
                Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("fdc"))
            })
            .cloned()
            .collect()
    }
}

impl StorageIo for RangeTrackingStorage {
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
        self.range_reads
            .lock()
            .expect("range-read lock")
            .push((name.to_owned(), offset, length));
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

fn unique_test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after the Unix epoch")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}-{nonce}", std::process::id()))
}

#[test]
fn huge_fill_hole_and_verified_data_are_read_byte_exactly_without_file_materialization() {
    let root = unique_test_root("manifest-reader");
    let containers = ContainerRepository::new(
        FsStorageIo::open(&root).expect("create workspace-local container repository"),
    );
    let payload = b"abcdefgh";
    containers
        .publish_raw(
            ContainerId::new([0x81; 16]).expect("container identity is nonzero"),
            1,
            &[payload.as_slice()],
        )
        .expect("publish DATA location");

    let fill_length = 1_u64 << 40;
    let manifest = ManifestLeaf::new(
        fill_length + payload.len() as u64 + 9,
        vec![
            ManifestExtent::Fill {
                logical_length: fill_length,
                value: b'a',
            },
            ManifestExtent::Data {
                logical_length: payload.len() as u64,
                chunk_id: ChunkId::of(payload),
            },
            ManifestExtent::Hole { logical_length: 9 },
        ],
    )
    .expect("mixed manifest is valid");
    let file = VerifiedManifestFile::new(manifest, containers)
        .expect("all DATA dependencies verify before reads");

    let bytes = file
        .read_at(fill_length - 2, 14)
        .expect("bounded read crosses FILL, DATA, and HOLE");
    assert_eq!(bytes, b"aaabcdefgh\0\0\0\0");
    assert_eq!(file.logical_size(), fill_length + 17);
    assert_eq!(file.read_at(file.logical_size(), 1).unwrap(), b"");

    let container_path = root.join(format!("{}.fdc", "81".repeat(16)));
    let mut container_bytes = std::fs::read(&container_path).expect("read bounded test container");
    container_bytes[4_288] ^= 1;
    std::fs::write(&container_path, container_bytes).expect("inject durable test corruption");
    assert!(matches!(
        file.read_at(
            fill_length,
            u32::try_from(payload.len()).expect("test payload length fits u32"),
        ),
        Err(ManifestReadError::Store(StoreError::Format(_)))
    ));
}

#[test]
fn a_long_lived_manifest_reader_pins_exact_only_for_each_bounded_read() {
    let root = unique_test_root("manifest-reader-exact-pin-drain");
    let storage = FsStorageIo::open(&root).expect("open shared test repository");
    let containers = ContainerRepository::new(storage.clone());
    containers.install_cpu_admission(Arc::new(fastdup_store::WorkerPermits::new(
        std::num::NonZeroUsize::new(4).unwrap(),
    )));
    let first_id = ContainerId::new([0x82; 16]).expect("container identity is nonzero");
    let first_payload = b"a manifest reader may outlive its Exact generation";
    containers
        .publish_raw(first_id, 1, &[first_payload])
        .expect("publish the manifest DATA");
    let first = containers
        .read(first_id)
        .expect("recover the first verified Container");
    let first_entry = ExactIndexEntry::from_verified_raw(first.raw_locations()[0])
        .expect("derive the first Exact entry from verified evidence");

    let indexes = ExactIndexRunRepository::new(storage);
    let profile = ExactIndexProfileId::new([0x83; 32]).expect("profile identity is nonzero");
    indexes
        .append_level_zero(profile, vec![first_entry])
        .expect("activate the first Exact generation");
    let active = indexes
        .pin_active_generation()
        .expect("pin the first Exact generation");
    let manifest = ManifestLeaf::new(
        first_payload.len() as u64,
        vec![ManifestExtent::Data {
            logical_length: first_payload.len() as u64,
            chunk_id: ChunkId::of(first_payload),
        }],
    )
    .expect("one-extent manifest is valid");
    let file = VerifiedManifestFile::new(manifest, containers.clone())
        .expect("verify the manifest DATA")
        .with_active_index(&active);
    drop(active);

    let second_id = ContainerId::new([0x84; 16]).expect("container identity is nonzero");
    let second_payload = b"an unrelated Exact location advances the generation";
    containers
        .publish_raw(second_id, 2, &[second_payload])
        .expect("publish the second DATA Container");
    let second = containers
        .read(second_id)
        .expect("recover the second verified Container");
    let second_entry = ExactIndexEntry::from_verified_raw(second.raw_locations()[0])
        .expect("derive the second Exact entry from verified evidence");
    let transition = indexes
        .append_level_zero(profile, vec![second_entry])
        .expect("activate the successor Exact generation");
    let drain = transition
        .into_retired()
        .expect("the prior generation needs a drain token");

    assert!(
        drain.is_drained(),
        "a dormant Manifest reader cannot retain a generation pin"
    );
    assert_eq!(
        file.read_at(0, u32::try_from(first_payload.len()).unwrap())
            .expect("a post-retirement read falls back to verified Container discovery"),
        first_payload
    );
}

#[test]
fn adjacent_chunks_in_one_encoding_record_need_one_data_range_read() {
    let root = unique_test_root("manifest-reader-one-record-read");
    let storage = RangeTrackingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    containers.install_cpu_admission(Arc::new(fastdup_store::WorkerPermits::new(
        std::num::NonZeroUsize::new(4).unwrap(),
    )));
    let container_id = ContainerId::new([0x91; 16]).expect("container identity is nonzero");
    let first = (0..192 * 1_024)
        .map(|index| b'a' + u8::try_from(index % 19).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    let second = (0..192 * 1_024)
        .map(|index| b'A' + u8::try_from(index % 17).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    containers
        .publish_adaptive_regions(container_id, 1, &[&[first.as_slice(), second.as_slice()]])
        .expect("publish one multi-Chunk encoding record");
    let container = containers
        .read(container_id)
        .expect("recover verified Exact evidence");
    assert_eq!(container.zstd_record_count(), 1);
    let entries = container
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<Vec<_>, _>>()
        .expect("derive Exact entries from verified evidence");
    assert_eq!(entries.len(), 2);

    let indexes = ExactIndexRunRepository::new(storage.clone());
    let profile = ExactIndexProfileId::new([0x92; 32]).expect("profile identity is nonzero");
    indexes
        .append_level_zero(profile, entries.clone())
        .expect("activate the Exact generation");
    let active = indexes
        .pin_active_generation()
        .expect("pin the Exact generation");
    let manifest = ManifestLeaf::new(
        u64::try_from(first.len() + second.len()).expect("fixture length fits u64"),
        vec![
            ManifestExtent::Data {
                logical_length: u64::try_from(first.len()).expect("fixture length fits u64"),
                chunk_id: ChunkId::of(&first),
            },
            ManifestExtent::Data {
                logical_length: u64::try_from(second.len()).expect("fixture length fits u64"),
                chunk_id: ChunkId::of(&second),
            },
        ],
    )
    .expect("two adjacent DATA extents form a valid Manifest");
    let file = VerifiedManifestFile::new(manifest, containers.clone())
        .expect("verify every Manifest dependency")
        .with_active_index(&active);

    containers
        .read_verified_location(entries[0])
        .expect("warm only the verified Container descriptor");
    storage.clear_range_reads();

    let restored = file
        .read_at(
            0,
            u32::try_from(first.len() + second.len()).expect("fixture read fits u32"),
        )
        .expect("restore both adjacent Chunks");
    let mut expected = first;
    expected.extend_from_slice(&second);
    assert_eq!(restored, expected);
    assert_eq!(
        storage.data_range_reads().len(),
        1,
        "one verified Encoding Record must need only one DATA range read"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn adjacent_chunks_prefer_active_locations_in_the_same_container() {
    let root = unique_test_root("manifest-reader-local-location");
    let storage = RangeTrackingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    containers.install_cpu_admission(Arc::new(fastdup_store::WorkerPermits::new(
        std::num::NonZeroUsize::new(4).unwrap(),
    )));
    let local_id = ContainerId::new([0xa1; 16]).expect("container identity is nonzero");
    let newer_id = ContainerId::new([0xa2; 16]).expect("container identity is nonzero");
    let first = b"first logical Chunk stays in the original Container".to_vec();
    let second = b"second logical Chunk has two equally valid Locations".to_vec();
    containers
        .publish_raw(local_id, 1, &[first.as_slice(), second.as_slice()])
        .expect("publish the restore-local pair");
    containers
        .publish_raw(newer_id, 2, &[second.as_slice()])
        .expect("publish a newer duplicate Location");
    let local = containers
        .read(local_id)
        .expect("recover the local Exact evidence");
    let newer = containers
        .read(newer_id)
        .expect("recover the newer Exact evidence");
    let local_entries = local
        .raw_locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified_raw)
        .collect::<Result<Vec<_>, _>>()
        .expect("derive local Exact entries");
    let newer_entry = ExactIndexEntry::from_verified_raw(newer.raw_locations()[0])
        .expect("derive the newer Exact entry");

    let indexes = ExactIndexRunRepository::new(storage.clone());
    let profile = ExactIndexProfileId::new([0xa3; 32]).expect("profile identity is nonzero");
    indexes
        .append_level_zero(profile, local_entries.clone())
        .expect("activate the local Locations");
    indexes
        .append_level_zero(profile, vec![newer_entry])
        .expect("activate the newer duplicate Location");
    let active = indexes
        .pin_active_generation()
        .expect("pin both Exact generations");
    let manifest = ManifestLeaf::new(
        u64::try_from(first.len() + second.len()).expect("fixture length fits u64"),
        vec![
            ManifestExtent::Data {
                logical_length: u64::try_from(first.len()).expect("fixture length fits u64"),
                chunk_id: ChunkId::of(&first),
            },
            ManifestExtent::Data {
                logical_length: u64::try_from(second.len()).expect("fixture length fits u64"),
                chunk_id: ChunkId::of(&second),
            },
        ],
    )
    .expect("two adjacent DATA extents form a valid Manifest");
    let file = VerifiedManifestFile::new(manifest, containers.clone())
        .expect("verify every Manifest dependency")
        .with_active_index(&active);

    containers
        .read_verified_location(local_entries[0])
        .expect("warm the local Container descriptor");
    containers
        .read_verified_location(newer_entry)
        .expect("warm the newer Container descriptor");
    storage.clear_range_reads();
    let descriptor_hits_before = containers.descriptor_cache_status().hits();

    let restored = file
        .read_at(
            0,
            u32::try_from(first.len() + second.len()).expect("fixture read fits u32"),
        )
        .expect("restore both adjacent Chunks");
    let mut expected = first;
    expected.extend_from_slice(&second);
    assert_eq!(restored, expected);
    let reads = storage.data_range_reads();
    assert_eq!(reads.len(), 1);
    let expected_name = format!("{}.fdc", "a1".repeat(16));
    assert!(
        reads.iter().all(|(name, _, _)| name == &expected_name),
        "the planned read must stay in the restore-local Container: {reads:?}"
    );
    let first_location = local_entries[0].location();
    let second_location = local_entries[1].location();
    assert_eq!(
        first_location
            .record_offset()
            .checked_add(u64::from(first_location.record_length())),
        Some(second_location.record_offset()),
        "the fixture must contain physically adjacent records"
    );
    assert_eq!(reads[0].1, first_location.record_offset());
    assert_eq!(
        reads[0].2,
        usize::try_from(first_location.record_length() + second_location.record_length())
            .expect("combined record range fits usize")
    );
    assert_eq!(
        containers.descriptor_cache_status().hits() - descriptor_hits_before,
        1,
        "one Read Plan must reuse its verified descriptor across local Records"
    );
}

#[test]
fn adjacent_record_coalescing_never_exceeds_the_storage_range_bound() {
    let root = unique_test_root("manifest-reader-bounded-record-coalescing");
    let storage = RangeTrackingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    containers.install_cpu_admission(Arc::new(fastdup_store::WorkerPermits::new(
        std::num::NonZeroUsize::new(4).unwrap(),
    )));
    let container_id = ContainerId::new([0xa4; 16]).expect("container identity is nonzero");
    let chunks = (1_u8..=4)
        .map(|byte| vec![byte; 256 * 1_024])
        .collect::<Vec<_>>();
    let chunk_slices = chunks.iter().map(Vec::as_slice).collect::<Vec<_>>();
    containers
        .publish_raw(container_id, 1, &chunk_slices)
        .expect("publish four adjacent maximum-size RAW records");
    let container = containers
        .read(container_id)
        .expect("recover verified Exact evidence");
    let entries = container
        .raw_locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified_raw)
        .collect::<Result<Vec<_>, _>>()
        .expect("derive Exact entries");
    let indexes = ExactIndexRunRepository::new(storage.clone());
    let profile = ExactIndexProfileId::new([0xa5; 32]).expect("profile identity is nonzero");
    indexes
        .append_level_zero(profile, entries.clone())
        .expect("activate the Exact generation");
    let active = indexes
        .pin_active_generation()
        .expect("pin the Exact generation");
    let manifest = ManifestLeaf::new(
        u64::try_from(4 * 256 * 1_024).expect("fixture length fits u64"),
        chunks
            .iter()
            .map(|chunk| ManifestExtent::Data {
                logical_length: u64::try_from(chunk.len()).expect("chunk length fits u64"),
                chunk_id: ChunkId::of(chunk),
            })
            .collect(),
    )
    .expect("four adjacent DATA extents form a valid Manifest");
    let file = VerifiedManifestFile::new(manifest, containers.clone())
        .expect("verify every Manifest dependency")
        .with_active_index(&active);
    containers
        .read_verified_location(entries[0])
        .expect("warm the verified descriptor");
    storage.clear_range_reads();

    let restored = file
        .read_at(0, u32::try_from(4 * 256 * 1_024).unwrap())
        .expect("restore the bounded range");
    assert_eq!(restored, chunk_slices.concat());
    let reads = storage.data_range_reads();
    assert!(reads.len() >= 2, "the combined records exceed one range");
    assert!(
        reads
            .iter()
            .all(|(_, _, length)| *length <= MAX_STORAGE_RANGE_BYTES),
        "every coalesced DATA read must remain bounded: {reads:?}"
    );
}

#[test]
fn repeated_chunk_in_one_read_reuses_exact_lookup_scratch_and_record_decode() {
    let root = unique_test_root("manifest-reader-repeated-exact-key");
    let storage = RangeTrackingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    containers.install_cpu_admission(Arc::new(fastdup_store::WorkerPermits::new(
        std::num::NonZeroUsize::new(4).unwrap(),
    )));
    let container_id = ContainerId::new([0xb1; 16]).expect("container identity is nonzero");
    let payload = b"one immutable Chunk appears twice in the logical recipe".repeat(512);
    containers
        .publish_raw(container_id, 1, &[payload.as_slice()])
        .expect("publish the repeated Chunk");
    let container = containers
        .read(container_id)
        .expect("recover verified Exact evidence");
    let entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("derive Exact entry");

    let indexes = ExactIndexRunRepository::new(storage.clone());
    let profile = ExactIndexProfileId::new([0xb2; 32]).expect("profile identity is nonzero");
    indexes
        .append_level_zero(profile, vec![entry])
        .expect("activate the Exact generation");
    let active = indexes
        .pin_active_generation()
        .expect("pin the Exact generation");
    let logical_length = u64::try_from(payload.len()).expect("fixture length fits u64");
    let manifest = ManifestLeaf::new(
        logical_length * 2,
        vec![
            ManifestExtent::Data {
                logical_length,
                chunk_id: ChunkId::of(&payload),
            },
            ManifestExtent::Data {
                logical_length,
                chunk_id: ChunkId::of(&payload),
            },
        ],
    )
    .expect("repeated DATA extents form a valid Manifest");
    let file = VerifiedManifestFile::new(manifest, containers.clone())
        .expect("verify the repeated dependency")
        .with_active_index(&active);
    containers
        .read_verified_location(entry)
        .expect("warm the verified descriptor");
    storage.clear_range_reads();
    let probes_before = active.membership_status().probes();

    let restored = file
        .read_at(
            0,
            u32::try_from(payload.len() * 2).expect("fixture read fits u32"),
        )
        .expect("restore both logical appearances");

    assert_eq!(restored, [payload.as_slice(), payload.as_slice()].concat());
    assert_eq!(active.membership_status().probes() - probes_before, 1);
    assert_eq!(storage.data_range_reads().len(), 1);
}

#[test]
#[allow(clippy::too_many_lines)]
fn dependent_reads_reuse_verified_bases_across_requests_but_scrub_reads_storage() {
    use fastdup_store::{MemoryPressureSnapshot, VerifiedReadCache, VerifiedReadCacheConfig};
    use std::num::NonZeroUsize;
    let root = unique_test_root("manifest-base-cache");
    let storage = RangeTrackingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    containers.install_cpu_admission(Arc::new(fastdup_store::WorkerPermits::new(
        std::num::NonZeroUsize::new(4).unwrap(),
    )));
    let base = (0_u32..16384)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect::<Vec<_>>();
    let mut first = base.clone();
    first[31] ^= 0x63;
    let mut second = base.clone();
    second[4096] ^= 31;
    let base_id = ContainerId::new([0xc1; 16]).unwrap();
    let target_id = ContainerId::new([0xc2; 16]).unwrap();
    containers.publish_raw(base_id, 1, &[&base]).unwrap();
    let publication = containers
        .publish_zstd_prefix_pairs_verified(target_id, 2, &[(&base, &first), (&base, &second)])
        .unwrap();
    let base_container = containers.read(base_id).unwrap();
    let mut entries = base_container
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    entries.extend(
        publication
            .locations()
            .iter()
            .copied()
            .map(ExactIndexEntry::from_verified)
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    );
    let indexes = ExactIndexRunRepository::new(storage.clone());
    indexes
        .append_level_zero(ExactIndexProfileId::new([0xc3; 32]).unwrap(), entries)
        .unwrap();
    let active = indexes.pin_active_generation().unwrap();
    let cache = Arc::new(
        VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(
                4 * 1024 * 1024,
                1024 * 1024,
                NonZeroUsize::new(8).unwrap(),
            )
            .unwrap(),
            MemoryPressureSnapshot::new(64 * 1024 * 1024, 32 * 1024 * 1024, 0),
        )
        .unwrap(),
    );
    let manifest = ManifestLeaf::new(
        32768,
        vec![
            ManifestExtent::Data {
                logical_length: 16384,
                chunk_id: ChunkId::of(&first),
            },
            ManifestExtent::Data {
                logical_length: 16384,
                chunk_id: ChunkId::of(&second),
            },
        ],
    )
    .unwrap();
    let file = VerifiedManifestFile::new(manifest, containers.clone())
        .unwrap()
        .with_active_index(&active)
        .with_verified_read_cache(Arc::clone(&cache));
    assert_eq!(file.read_at(0, 16384).unwrap(), first);
    storage.clear_range_reads();
    assert_eq!(file.read_at(16384, 16384).unwrap(), second);
    let base_name = format!("{}.fdc", "c1".repeat(16));
    assert!(
        storage
            .data_range_reads()
            .iter()
            .all(|(name, _, _)| name != &base_name),
        "second Target must reuse the already verified Base"
    );
    storage.clear_range_reads();
    containers.read_with_index(target_id, &active).unwrap();
    assert!(
        storage
            .data_range_reads()
            .iter()
            .any(|(name, _, _)| name == &base_name),
        "independent full verification must reread Base storage"
    );
    cache.update_memory_pressure(MemoryPressureSnapshot::new(
        64 * 1024 * 1024,
        32 * 1024 * 1024,
        4096,
    ));
    storage.clear_range_reads();
    assert_eq!(file.read_at(16384, 16384).unwrap(), second);
    assert!(
        storage
            .data_range_reads()
            .iter()
            .any(|(name, _, _)| name == &base_name)
    );
    assert_eq!(cache.status().resident_bytes(), 0);
}

#[test]
fn coalesced_raw_cache_charges_shared_encoded_backing_once() {
    use fastdup_store::{MemoryPressureSnapshot, VerifiedReadCache, VerifiedReadCacheConfig};
    use std::num::NonZeroUsize;
    let root = unique_test_root("manifest-raw-backing-cache");
    let storage = RangeTrackingStorage::open(&root);
    let containers = ContainerRepository::new(storage.clone());
    containers.install_cpu_admission(Arc::new(fastdup_store::WorkerPermits::new(
        std::num::NonZeroUsize::new(4).unwrap(),
    )));
    let chunks = [vec![31; 16384], vec![91; 16384]];
    let id = ContainerId::new([0xc4; 16]).unwrap();
    containers
        .publish_raw(id, 1, &[&chunks[0], &chunks[1]])
        .unwrap();
    let image = containers.read(id).unwrap();
    let entries = image
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let retained = entries
        .iter()
        .map(|e| usize::try_from(e.location().record_length()).unwrap())
        .sum::<usize>();
    let indexes = ExactIndexRunRepository::new(storage);
    indexes
        .append_level_zero(ExactIndexProfileId::new([0xc5; 32]).unwrap(), entries)
        .unwrap();
    let active = indexes.pin_active_generation().unwrap();
    let cache = Arc::new(
        VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(
                4 * 1024 * 1024,
                1024 * 1024,
                NonZeroUsize::new(8).unwrap(),
            )
            .unwrap(),
            MemoryPressureSnapshot::new(64 * 1024 * 1024, 32 * 1024 * 1024, 0),
        )
        .unwrap(),
    );
    let manifest = ManifestLeaf::new(
        32768,
        chunks
            .iter()
            .map(|b| ManifestExtent::Data {
                logical_length: 16384,
                chunk_id: ChunkId::of(b),
            })
            .collect(),
    )
    .unwrap();
    let file = VerifiedManifestFile::new(manifest, containers)
        .unwrap()
        .with_active_index(&active)
        .with_verified_read_cache(Arc::clone(&cache));
    assert_eq!(file.read_at(0, 32768).unwrap(), chunks.concat());
    assert_eq!(cache.status().resident_bytes(), retained);
    assert_eq!(cache.status().entry_count(), 2);
}

#[test]
fn shared_manifest_reply_retains_verified_owner_and_slice_coordinates() {
    use fastdup_store::{MemoryPressureSnapshot, VerifiedReadCache, VerifiedReadCacheConfig};
    use std::num::NonZeroUsize;
    let root = unique_test_root("manifest-shared-reply");
    let containers = ContainerRepository::new(FsStorageIo::open(&root).unwrap());
    let payload = b"0123456789abcdef";
    containers
        .publish_raw(ContainerId::new([0xd8; 16]).unwrap(), 1, &[payload])
        .unwrap();
    let cache = Arc::new(
        VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(
                4 * 1024 * 1024,
                1024 * 1024,
                NonZeroUsize::new(8).unwrap(),
            )
            .unwrap(),
            MemoryPressureSnapshot::new(64 * 1024 * 1024, 32 * 1024 * 1024, 0),
        )
        .unwrap(),
    );
    let manifest = ManifestLeaf::new(
        16,
        vec![ManifestExtent::Data {
            logical_length: 16,
            chunk_id: ChunkId::of(payload),
        }],
    )
    .unwrap();
    let file = VerifiedManifestFile::new(manifest, containers)
        .unwrap()
        .with_verified_read_cache(cache);
    let whole = file.read_shared_at(0, 16).unwrap();
    let slice = file.read_shared_at(3, 7).unwrap();
    assert_eq!(slice, payload[3..10]);
    assert_eq!(
        slice.as_ptr(),
        whole[3..].as_ptr(),
        "cached immutable payload reaches the reply without a copy"
    );
    drop(whole);
    drop(file);
    assert_eq!(
        slice,
        b"3456789".as_slice(),
        "reply owns the payload after reader and cache drop"
    );
}

fn shared_extent_fixture(label: &str) -> (VerifiedManifestFile<FsStorageIo>, Vec<u8>) {
    use fastdup_store::{MemoryPressureSnapshot, VerifiedReadCache, VerifiedReadCacheConfig};
    use std::num::NonZeroUsize;
    let root = unique_test_root(label);
    let storage = FsStorageIo::open(&root).unwrap();
    let containers = ContainerRepository::new(storage.clone());
    let first = vec![0x51; 128 * 1024];
    let second = vec![0x72; 128 * 1024];
    let id = ContainerId::new([0xdb; 16]).unwrap();
    containers
        .publish_adaptive_regions(id, 1, &[&[&first, &second]])
        .unwrap();
    let verified = containers.read(id).unwrap();
    assert_eq!(verified.zstd_record_count(), 1);
    let entries = verified
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let indexes = ExactIndexRunRepository::new(storage);
    indexes
        .append_level_zero(ExactIndexProfileId::new([0xda; 32]).unwrap(), entries)
        .unwrap();
    let active = indexes.pin_active_generation().unwrap();
    let cache = Arc::new(
        VerifiedReadCache::new_with_snapshot(
            VerifiedReadCacheConfig::new(8 * 1024 * 1024, 0, NonZeroUsize::MIN).unwrap(),
            MemoryPressureSnapshot::new(128 * 1024 * 1024, 64 * 1024 * 1024, 0),
        )
        .unwrap(),
    );
    let manifest = ManifestLeaf::new(
        256 * 1024 + 16,
        vec![
            ManifestExtent::Data {
                logical_length: 128 * 1024,
                chunk_id: ChunkId::of(&first),
            },
            ManifestExtent::Data {
                logical_length: 128 * 1024,
                chunk_id: ChunkId::of(&second),
            },
            ManifestExtent::Hole { logical_length: 16 },
        ],
    )
    .unwrap();
    let file = VerifiedManifestFile::new(manifest, containers)
        .unwrap()
        .with_active_index(&active)
        .with_verified_read_cache(cache);
    (file, [first, second].concat())
}

#[test]
fn multiple_data_extents_share_reply_owner_and_sparse_suffix_assembles_correctly() {
    let (file, expected) = shared_extent_fixture("manifest-multiple-shared-reply");
    let whole = file.read_shared_at(0, 256 * 1024).unwrap();
    let left = file.read_shared_at(0, 128 * 1024).unwrap();
    let across = file.read_shared_at(128 * 1024 - 7, 19).unwrap();
    assert_eq!(whole, expected);
    assert_eq!(
        whole.as_ptr(),
        left.as_ptr(),
        "two DATA extents retain the cache owner"
    );
    assert_eq!(across.as_ptr(), whole[128 * 1024 - 7..].as_ptr());
    assert_eq!(across, expected[128 * 1024 - 7..128 * 1024 + 12]);
    let mixed = file.read_shared_at(0, 256 * 1024 + 16).unwrap();
    assert_eq!(&mixed[..expected.len()], expected);
    assert_eq!(&mixed[expected.len()..], &[0; 16]);
    drop(file);
    drop(whole);
    drop(left);
    assert_eq!(across, expected[128 * 1024 - 7..128 * 1024 + 12]);
}

#[test]
#[ignore = "manual release-mode cached multi-Extent Read Reply A/B"]
fn shared_extent_reply_microbenchmark() {
    use std::hint::black_box;
    use std::time::Instant;
    let (file, expected) = shared_extent_fixture("manifest-multiple-shared-benchmark");
    assert_eq!(file.read_shared_at(0, 256 * 1024).unwrap(), expected);
    for length in [4096, 65536, 262_144] {
        let offset = 131_072 - u64::from(length / 2);
        let mut samples = [Vec::new(), Vec::new()];
        for round in 0..11 {
            for side in 0..2 {
                let side = (side + round) % 2;
                let start = Instant::now();
                for _ in 0..2500 {
                    if side == 0 {
                        black_box(file.read_at(offset, length).unwrap());
                    } else {
                        black_box(file.read_shared_at(offset, length).unwrap());
                    }
                }
                samples[side].push(start.elapsed());
            }
        }
        for samples in &mut samples {
            samples.sort_unstable();
        }
        println!(
            "multi_extent_reply length={length} vec_ns={:.1} shared_ns={:.1} speedup={:.3}",
            samples[0][5].as_secs_f64() * 400_000.0,
            samples[1][5].as_secs_f64() * 400_000.0,
            samples[0][5].as_secs_f64() / samples[1][5].as_secs_f64()
        );
    }
}
