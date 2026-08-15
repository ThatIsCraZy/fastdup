use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexActivationHash, ExactIndexActivationRecord, ExactIndexEntry,
    ExactIndexLocation, ExactIndexProfileId, ExactIndexRun, ExactIndexRunRef, ExactIndexRunSet,
    ExactLocationTransition,
};
use fastdup_store::{ExactIndexRunRepository, ExactIndexStoreError, FsStorageIo};

fn test_root(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!(
            "exact-index-activation-{name}-{}",
            std::process::id()
        ))
}

fn run(profile: ExactIndexProfileId, generation: u64, ordinal: u8) -> ExactIndexRun {
    let logical_length = 16_384 + u32::from(ordinal);
    let record_length = (logical_length + 255) / 64 * 64;
    let location = ExactIndexLocation::raw(
        ContainerId::new([ordinal + 1; 16]).expect("container identity is nonzero"),
        generation,
        4_096,
        record_length,
        0xFA00_0000 + u32::from(ordinal),
    )
    .expect("worked RAW location is valid");
    let entry =
        ExactIndexEntry::active(ChunkId::from_bytes([ordinal; 32]), logical_length, location)
            .expect("worked active entry is valid");
    ExactIndexRun::new(profile, generation, vec![entry]).expect("worked run is valid")
}

#[test]
fn activation_recovers_only_one_run_set_with_all_durable_dependencies() {
    let root = test_root("recovery");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0x8A; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local index repository"),
    );
    let first = repository
        .publish(&run(profile, 1, 1))
        .expect("publish first immutable Run");
    let second = repository
        .publish(&run(profile, 2, 2))
        .expect("publish second immutable Run");
    let run_set = ExactIndexRunSet::new(
        profile,
        1,
        vec![
            ExactIndexRunRef::new(0, second).expect("second Run reference is valid"),
            ExactIndexRunRef::new(0, first).expect("first Run reference is valid"),
        ],
    )
    .expect("worked Run Set is valid");

    let activated = repository
        .activate(&run_set)
        .expect("activate only after every dependency is durable");
    assert_eq!(activated.record().generation(), 1);
    assert_eq!(activated.run_set(), &run_set);
    assert_eq!(activated.run_count(), 2);

    drop(repository);
    let reopened = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("reopen workspace-local index repository"),
    );
    let recovered = reopened
        .recover_active()
        .expect("recover activation chain and all dependencies")
        .expect("one Run Set was activated");
    assert_eq!(recovered.record(), activated.record());
    assert_eq!(recovered.run_set(), &run_set);
    assert_eq!(recovered.run_count(), 2);
    let lookup = recovered
        .lookup_transitions(ChunkId::from_bytes([2; 32]), 16_386)
        .expect("active Run Set performs bounded newest-Run-first lookup");
    assert!(lookup.complete());
    assert_eq!(lookup.candidates().len(), 1);
    assert_eq!(
        lookup.candidates()[0].transition(),
        ExactLocationTransition::Active
    );
    assert_eq!(lookup.candidates()[0].location().container_generation(), 2);
}

#[test]
fn activation_refuses_to_ack_past_the_recoverable_wal_limit() {
    const RECORD_COUNT_AT_LIMIT: u64 = (64 * 1_024 * 1_024) / 4_096;

    let root = test_root("full-wal");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0x8B; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local index repository"),
    );
    let descriptor = repository
        .publish(&run(profile, 1, 3))
        .expect("publish one immutable dependency");
    let run_set = ExactIndexRunSet::new(
        profile,
        RECORD_COUNT_AT_LIMIT + 1,
        vec![ExactIndexRunRef::new(0, descriptor).expect("Run reference is valid")],
    )
    .expect("worked Run Set is valid");

    let placeholder_id = ExactIndexRunSet::new(profile, 1, Vec::new())
        .expect("empty historical Run Set is valid")
        .id()
        .expect("historical Run Set has a content identity");
    let mut previous_hash = ExactIndexActivationHash::ZERO;
    let mut wal = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join("exact-index.activation.wal"))
        .expect("create the bounded worked WAL fixture");
    for generation in 1..=RECORD_COUNT_AT_LIMIT {
        let record = ExactIndexActivationRecord::new(
            generation,
            previous_hash,
            placeholder_id,
            profile,
            generation,
        )
        .expect("construct one contiguous worked record");
        let encoded = record.encode();
        wal.write_all(&encoded).expect("write one complete record");
        previous_hash = ExactIndexActivationHash::of(&encoded);
    }
    wal.sync_all().expect("make the full fixture readable");
    drop(wal);

    let error = repository
        .activate(&run_set)
        .expect_err("a commit beyond the recovery bound must never be acknowledged");
    assert!(
        matches!(error, ExactIndexStoreError::ActivationWalFull),
        "unexpected error: {error:?}"
    );
}
