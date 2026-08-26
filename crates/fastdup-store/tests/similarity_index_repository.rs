use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_format::{ChunkId, SimilarityIndexEntry, SimilarityIndexRun};
use fastdup_store::{
    FsStorageIo, SIMILARITY_FINGERPRINT_PROFILE_V1, SIMILARITY_REPRESENTATIVE_PROFILE_V1,
    SimilarityIndexReadMode, SimilarityIndexRepository, SimilarityIndexStoreError, StorageIo,
    similarity_index_entry_v1,
};

#[test]
fn newest_complete_pool_snapshot_survives_restart_and_proposes_old_bases() {
    let root = test_root("restart");
    let storage = FsStorageIo::open(&root).expect("open Similarity repository root");
    let repository = SimilarityIndexRepository::new(storage.clone());
    let base = fixture_bytes(64 * 1_024, 11);
    let unrelated = fixture_bytes(64 * 1_024, 12);
    let first = snapshot(1, &[base.as_slice()]);
    repository.publish(&first).expect("publish first snapshot");
    let second = snapshot(2, &[base.as_slice(), unrelated.as_slice()]);
    repository
        .publish(&second)
        .expect("publish complete successor");
    drop(repository);

    let reopened = SimilarityIndexRepository::new(
        FsStorageIo::open(&root).expect("reopen Similarity repository root"),
    );
    let recovered = reopened
        .recover_latest()
        .expect("stream newest snapshot after restart")
        .expect("one Similarity snapshot is present");
    assert_eq!(recovered.status().generation(), 2);
    assert_eq!(recovered.status().entries_streamed(), 2);
    let audit = reopened
        .audit_latest()
        .expect("offline-audit newest snapshot")
        .expect("one Similarity snapshot is present");
    assert_eq!(audit.generation(), 2);
    assert_eq!(audit.entries_verified(), 2);
    assert_eq!(audit.pages_verified(), 2);
    assert_ne!(audit.run_hash(), [0; 32]);

    let mut target = base.clone();
    target[32 * 1_024] ^= 0x5a;
    let candidates = recovered
        .candidates(&target)
        .expect("query bounded pool-wide candidates");
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.chunk_id() == ChunkId::of(&base))
    );
    assert!(candidates.len() <= 16);
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.logical_length() == 64 * 1_024)
    );
}

#[test]
fn offline_audit_rejects_corrupt_snapshot_page() {
    let root = test_root("scrub-corruption");
    let storage = FsStorageIo::open(&root).expect("open Similarity repository root");
    let repository = SimilarityIndexRepository::new(storage.clone());
    let bytes = fixture_bytes(64 * 1_024, 44);
    repository
        .publish(&snapshot(1, &[bytes.as_slice()]))
        .expect("publish scrub fixture");
    repository
        .audit_latest()
        .expect("healthy snapshot audits")
        .expect("snapshot exists");

    let name = storage
        .list_names()
        .expect("list test repository")
        .into_iter()
        .find(|name| name.starts_with("similarity.") && name.strip_suffix(".fds").is_some())
        .expect("published Similarity name exists");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join(name))
        .expect("open published Similarity run for fault injection");
    let offset = u64::try_from(4_096 + 96 + 50).expect("fixture offset fits u64");
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, offset)
        .expect("read fault-injection byte");
    byte[0] ^= 1;
    file.write_all_at(&byte, offset)
        .expect("inject one page fault");

    assert!(matches!(
        repository.audit_latest(),
        Err(SimilarityIndexStoreError::Format(_))
    ));
    assert!(matches!(
        repository.recover_latest(),
        Err(SimilarityIndexStoreError::Format(_))
    ));
}

#[test]
fn offline_audit_rejects_corrupt_bucket_page() {
    let root = test_root("scrub-bucket-corruption");
    let storage = FsStorageIo::open(&root).expect("open Similarity repository root");
    let repository = SimilarityIndexRepository::new(storage.clone());
    let bytes = fixture_bytes(64 * 1_024, 45);
    repository
        .publish(&snapshot(1, &[bytes.as_slice()]))
        .expect("publish bucket scrub fixture");

    let name = storage
        .list_names()
        .expect("list test repository")
        .into_iter()
        .find(|name| name.starts_with("similarity.") && name.strip_suffix(".fds").is_some())
        .expect("published Similarity name exists");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join(name))
        .expect("open Similarity run for bucket fault injection");
    let offset = u64::try_from(2 * 4_096 + 80).expect("fixture offset fits u64");
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, offset)
        .expect("read bucket fault-injection byte");
    byte[0] ^= 1;
    file.write_all_at(&byte, offset)
        .expect("inject one bucket-page fault");

    assert!(matches!(
        repository.audit_latest(),
        Err(SimilarityIndexStoreError::Format(_))
    ));
}

#[test]
fn recovery_keeps_pool_buckets_on_disk() {
    let root = test_root("bounded-ram");
    let storage = FsStorageIo::open(&root).expect("open Similarity repository root");
    let entries = (0_u64..1_000)
        .map(|ordinal| {
            SimilarityIndexEntry::new(
                ChunkId::of(&ordinal.to_le_bytes()),
                64 * 1_024,
                SIMILARITY_FINGERPRINT_PROFILE_V1,
                [11, 22, 33, 44],
                [ordinal; 8],
            )
            .expect("construct hot-bucket fixture entry")
        })
        .collect();
    let run = SimilarityIndexRun::new(
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        SIMILARITY_REPRESENTATIVE_PROFILE_V1,
        7,
        entries,
    )
    .expect("construct hot-bucket snapshot");
    let repository = SimilarityIndexRepository::new(storage);
    repository
        .publish(&run)
        .expect("publish hot-bucket snapshot");

    let recovered = repository
        .recover_latest()
        .expect("stream hot-bucket snapshot")
        .expect("snapshot is present");
    assert_eq!(recovered.status().entries_streamed(), 1_000);
    assert_eq!(recovered.status().resident_representatives(), 0);
    assert_eq!(recovered.status().buckets(), 4);
}

#[test]
fn unique_pool_buckets_do_not_create_resident_maps() {
    let root = test_root("unique-buckets");
    let storage = FsStorageIo::open(&root).expect("open Similarity repository root");
    let entries = (0_u64..1_000)
        .map(|ordinal| {
            SimilarityIndexEntry::new(
                ChunkId::of(&ordinal.to_le_bytes()),
                64 * 1_024,
                SIMILARITY_FINGERPRINT_PROFILE_V1,
                [
                    ordinal * 4,
                    ordinal * 4 + 1,
                    ordinal * 4 + 2,
                    ordinal * 4 + 3,
                ],
                [ordinal; 8],
            )
            .expect("construct unique-bucket fixture entry")
        })
        .collect();
    let run = SimilarityIndexRun::new(
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        SIMILARITY_REPRESENTATIVE_PROFILE_V1,
        8,
        entries,
    )
    .expect("construct unique-bucket snapshot");
    let repository = SimilarityIndexRepository::new(storage);
    repository
        .publish(&run)
        .expect("publish unique-bucket snapshot");

    let recovered = repository
        .recover_latest()
        .expect("recover unique-bucket snapshot")
        .expect("snapshot is present");
    assert_eq!(recovered.status().entries_streamed(), 1_000);
    assert_eq!(recovered.status().resident_representatives(), 0);
    assert_eq!(recovered.status().buckets(), 4_000);
}

#[test]
fn one_generation_cannot_be_reused_for_different_snapshot_bytes() {
    let root = test_root("collision");
    let storage = FsStorageIo::open(&root).expect("open Similarity repository root");
    let repository = SimilarityIndexRepository::new(storage);
    let first_bytes = fixture_bytes(64 * 1_024, 1);
    let second_bytes = fixture_bytes(64 * 1_024, 2);
    repository
        .publish(&snapshot(1, &[first_bytes.as_slice()]))
        .expect("publish first generation");
    assert!(matches!(
        repository.publish(&snapshot(1, &[second_bytes.as_slice()])),
        Err(SimilarityIndexStoreError::PublishVerificationMismatch)
    ));
}

#[test]
fn external_sort_publication_matches_canonical_run_and_cleans_spools() {
    let external_root = test_root("external-sort");
    let canonical_root = test_root("canonical-sort");
    let external_storage =
        FsStorageIo::open(&external_root).expect("open external-sort repository root");
    let canonical_storage =
        FsStorageIo::open(&canonical_root).expect("open canonical repository root");
    let entries = (0_u64..12_000)
        .rev()
        .map(|ordinal| {
            SimilarityIndexEntry::new(
                ChunkId::of(&ordinal.to_le_bytes()),
                64 * 1_024,
                SIMILARITY_FINGERPRINT_PROFILE_V1,
                [
                    ordinal % 17,
                    (ordinal + 3) % 17,
                    (ordinal + 7) % 17,
                    (ordinal + 11) % 17,
                ],
                [ordinal.rotate_left(9); 8],
            )
            .expect("construct external-sort fixture entry")
        })
        .collect::<Vec<_>>();
    let canonical = SimilarityIndexRun::new(
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        SIMILARITY_REPRESENTATIVE_PROFILE_V1,
        33,
        entries.clone(),
    )
    .expect("construct canonical comparison run");

    let external_family = SimilarityIndexRepository::new(external_storage.clone())
        .publish_entries(33, entries)
        .expect("externally sort and publish entries");
    SimilarityIndexRepository::new(canonical_storage.clone())
        .publish(&canonical)
        .expect("publish canonical in-memory run");

    assert_eq!(external_family.logical_entry_count(), 12_000);
    assert_eq!(external_family.partitions().len(), 1);
    let external_names = external_storage
        .list_names()
        .expect("list external-sort objects");
    assert!(
        external_names
            .iter()
            .all(|name| !name.contains("similarity-build") && !name.ends_with(".building"))
    );
    let partition_name = external_names
        .iter()
        .find(|name| name.starts_with("similarity-part.") && name.strip_suffix(".fds").is_some())
        .expect("external partition exists");
    let partition = SimilarityIndexRun::decode(
        &external_storage
            .read(partition_name)
            .expect("read external partition"),
    )
    .expect("decode external partition");
    let external_references = partition
        .bucket_references()
        .iter()
        .map(|reference| {
            (
                reference.key(),
                partition.entries()[reference.entry_ordinal() as usize].chunk_id(),
            )
        })
        .collect::<Vec<_>>();
    let canonical_references = canonical
        .bucket_references()
        .iter()
        .map(|reference| {
            (
                reference.key(),
                canonical.entries()[reference.entry_ordinal() as usize].chunk_id(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        external_references, canonical_references,
        "external partitioning preserves the global representative set"
    );
}

#[test]
fn external_sort_collapses_identical_chunk_identity_and_rejects_conflicts() {
    let root = test_root("external-sort-duplicate");
    let storage = FsStorageIo::open(&root).expect("open duplicate repository root");
    let entry = SimilarityIndexEntry::new(
        ChunkId::of(b"duplicate"),
        64 * 1_024,
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        [1, 2, 3, 4],
        [5; 8],
    )
    .expect("construct duplicate fixture entry");

    let repository = SimilarityIndexRepository::new(storage.clone());
    let family = repository
        .publish_entries(34, [entry, entry])
        .expect("identical physical occurrences collapse to one logical entry");
    assert_eq!(family.logical_entry_count(), 1);

    let conflicting = SimilarityIndexEntry::new(
        entry.chunk_id(),
        entry.logical_length(),
        entry.fingerprint_profile(),
        [1, 2, 3, 9],
        entry.sketch(),
    )
    .expect("construct conflicting duplicate fixture entry");
    assert!(matches!(
        repository.publish_entries(35, [entry, conflicting]),
        Err(SimilarityIndexStoreError::IndexCorruption)
    ));
    assert!(
        storage
            .list_names()
            .expect("list duplicate fixture objects")
            .iter()
            .all(|name| !name.contains("similarity-build"))
    );
}

#[test]
fn family_query_proposes_the_same_verified_pool_base() {
    let root = test_root("family-query");
    let storage = FsStorageIo::open(&root).expect("open family-query repository root");
    let base = fixture_bytes(64 * 1_024, 81);
    let unrelated = fixture_bytes(64 * 1_024, 82);
    let entries = [&base, &unrelated]
        .into_iter()
        .map(|bytes| similarity_index_entry_v1(bytes).expect("fingerprint family-query fixture"));
    let repository = SimilarityIndexRepository::new(storage);
    repository
        .publish_entries(49, entries)
        .expect("publish family-query snapshot");
    let recovered = repository
        .recover_latest()
        .expect("recover family-query snapshot")
        .expect("family-query snapshot exists");
    assert_eq!(
        recovered.status().read_mode(),
        SimilarityIndexReadMode::Mmap
    );
    let mut target = base.clone();
    target[32 * 1_024] ^= 0x5a;

    assert!(
        recovered
            .candidates(&target)
            .expect("query family snapshot")
            .iter()
            .any(|candidate| candidate.chunk_id() == ChunkId::of(&base))
    );
}

#[test]
fn family_recovery_and_scrub_reject_manifest_corruption() {
    let root = test_root("family-manifest-corruption");
    let storage = FsStorageIo::open(&root).expect("open family-manifest repository root");
    let entry = SimilarityIndexEntry::new(
        ChunkId::of(b"family-manifest-corruption"),
        64 * 1_024,
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        [1, 2, 3, 4],
        [5; 8],
    )
    .expect("construct family-manifest fixture entry");
    let repository = SimilarityIndexRepository::new(storage.clone());
    repository
        .publish_entries(48, [entry])
        .expect("publish family-manifest fixture");
    let family_name = storage
        .list_names()
        .expect("list family-manifest objects")
        .into_iter()
        .find(|name| name.starts_with("similarity-family."))
        .expect("family manifest exists");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join(family_name))
        .expect("open family manifest for fault injection");
    let offset = 4_096_u64 + 40;
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, offset)
        .expect("read family-manifest fault byte");
    byte[0] ^= 1;
    file.write_all_at(&byte, offset)
        .expect("write family-manifest fault byte");

    assert!(matches!(
        repository.recover_latest(),
        Err(SimilarityIndexStoreError::IndexCorruption)
    ));
    assert!(matches!(
        repository.audit_latest(),
        Err(SimilarityIndexStoreError::IndexCorruption)
    ));
}

#[test]
fn partition_family_recovers_as_one_generation_and_requires_every_partition() {
    let root = test_root("partition-family");
    let storage = FsStorageIo::open(&root).expect("open partition-family repository root");
    let entries = (0_u64..65_600)
        .rev()
        .map(|ordinal| {
            SimilarityIndexEntry::new(
                ChunkId::of(&ordinal.to_le_bytes()),
                64 * 1_024,
                SIMILARITY_FINGERPRINT_PROFILE_V1,
                [
                    ordinal * 4,
                    ordinal * 4 + 1,
                    ordinal * 4 + 2,
                    ordinal * 4 + 3,
                ],
                [ordinal; 8],
            )
            .expect("construct partition-family fixture entry")
        })
        .collect::<Vec<_>>();
    let repository = SimilarityIndexRepository::new(storage.clone());
    let family = repository
        .publish_entries(50, entries)
        .expect("publish multi-part Similarity family");

    assert_eq!(family.logical_entry_count(), 65_600);
    assert_eq!(family.partitions().len(), 2);
    assert!(
        family
            .partitions()
            .windows(2)
            .all(|pair| { pair[0].maximum_bucket_key() < pair[1].minimum_bucket_key() })
    );
    let recovered = repository
        .recover_latest()
        .expect("recover complete partition family")
        .expect("partition family is selected");
    assert_eq!(recovered.status().generation(), 50);
    assert_eq!(recovered.status().entries_streamed(), 65_600);
    assert_eq!(
        recovered.status().read_mode(),
        SimilarityIndexReadMode::Mmap
    );
    let audit = repository
        .audit_latest()
        .expect("audit complete partition family")
        .expect("partition family is selected");
    assert_eq!(audit.entries_verified(), 65_600);
    drop(recovered);

    let second_partition = storage
        .list_names()
        .expect("list partition family")
        .into_iter()
        .filter(|name| name.starts_with("similarity-part."))
        .max()
        .expect("second physical partition exists");
    storage
        .remove_file(&second_partition)
        .expect("remove one physical partition for fault injection");
    storage.sync_root().expect("sync missing partition fault");

    assert!(matches!(
        repository.recover_latest(),
        Err(SimilarityIndexStoreError::Io(_))
    ));
    assert!(matches!(
        repository.audit_latest(),
        Err(SimilarityIndexStoreError::Io(_))
    ));
}

#[test]
fn mmap_generation_lease_blocks_mutation_until_the_reader_drops() {
    let root = test_root("mmap-generation-lease");
    let storage = FsStorageIo::open(&root).expect("open mmap lease repository root");
    let repository = SimilarityIndexRepository::new(storage.clone());
    let base = fixture_bytes(64 * 1_024, 91);
    repository
        .publish(&snapshot(61, &[base.as_slice()]))
        .expect("publish mmap lease fixture");
    let run_name = storage
        .list_names()
        .expect("list mmap lease fixture")
        .into_iter()
        .find(|name| name.starts_with("similarity.") && name.strip_suffix(".fds").is_some())
        .expect("published Similarity run exists");
    let run_length = storage
        .object_len(&run_name)
        .expect("read mapped run length");

    let recovered = repository
        .recover_latest()
        .expect("recover mapped fixture")
        .expect("mapped fixture exists");
    assert_eq!(
        recovered.status().read_mode(),
        SimilarityIndexReadMode::Mmap
    );
    let second_reader = repository
        .recover_latest()
        .expect("recover a second mapped reader")
        .expect("mapped fixture still exists");

    let independently_opened =
        FsStorageIo::open(&root).expect("reopen root through an independent adapter");
    let replacement_name = ".similarity-replacement.building";
    independently_opened
        .create_new(replacement_name)
        .expect("create replacement fixture");
    for error in [
        independently_opened
            .write_at(&run_name, 0, &[0])
            .expect_err("mapped run rejects writes"),
        independently_opened
            .set_len(&run_name, run_length)
            .expect_err("mapped run rejects truncation even to its current length"),
        independently_opened
            .remove_file(&run_name)
            .expect_err("mapped run rejects removal"),
        independently_opened
            .publish_noreplace(replacement_name, &run_name)
            .expect_err("mapped run rejects replacement"),
    ] {
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }
    independently_opened
        .remove_file(replacement_name)
        .expect("unleased replacement fixture remains removable");

    drop(recovered);
    assert_eq!(
        independently_opened
            .remove_file(&run_name)
            .expect_err("another mapped reader still pins the run")
            .kind(),
        std::io::ErrorKind::PermissionDenied
    );
    drop(second_reader);
    independently_opened
        .remove_file(&run_name)
        .expect("run reclamation succeeds after the last mapped reader drops");
}

fn snapshot(generation: u64, chunks: &[&[u8]]) -> SimilarityIndexRun {
    let entries = chunks
        .iter()
        .map(|bytes| similarity_index_entry_v1(bytes).expect("fingerprint fixture Chunk"))
        .collect();
    SimilarityIndexRun::new(
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        SIMILARITY_REPRESENTATIVE_PROFILE_V1,
        generation,
        entries,
    )
    .expect("construct complete Similarity snapshot")
}

fn test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock is after the Unix epoch")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!(
            "similarity-index-{name}-{}-{nonce}",
            std::process::id()
        ))
}

fn fixture_bytes(length: usize, seed: u64) -> Vec<u8> {
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
