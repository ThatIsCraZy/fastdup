use std::{
    fs::OpenOptions,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef, ExactIndexRunSet, ExactLocationTransition,
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
fn repeated_rotation_stays_bounded_and_offline_audit_selects_the_latest_record() {
    const ACTIVATIONS: u64 = 130;
    const SLOT_BYTES: u64 = 64 * 4_096;

    let root = test_root("repeated-rotation");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0x8C; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local index repository"),
    );
    let descriptor = repository
        .publish(&run(profile, 1, 4))
        .expect("publish one immutable dependency");
    let run_ref = ExactIndexRunRef::new(0, descriptor).expect("Run reference is valid");

    for generation in 1..=ACTIVATIONS {
        let run_set = ExactIndexRunSet::new(profile, generation, vec![run_ref])
            .expect("worked Run Set generation is valid");
        let activated = repository
            .activate(&run_set)
            .expect("paired slots admit another lifetime activation");
        assert_eq!(activated.record().generation(), generation);
    }

    for name in ["exact-index.activation.wal", "exact-index.activation.1.wal"] {
        let length = std::fs::metadata(root.join(name))
            .expect("both fixed activation slots exist")
            .len();
        assert!(
            length <= SLOT_BYTES,
            "rotated slot {name} exceeded the lifetime bound: {length}"
        );
    }

    let audited = repository
        .audit_activation_log()
        .expect("offline audit accepts the paired-slot graph")
        .expect("one activation remains selected");
    assert_eq!(audited.generation(), ACTIVATIONS);
    assert_eq!(audited.run_set_generation(), ACTIVATIONS);

    drop(repository);
    let reopened = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("reopen the repeatedly rotated repository"),
    );
    let recovered = reopened
        .recover_active()
        .expect("recovery accepts repeated paired-slot rotations")
        .expect("the newest Run Set remains active");
    assert_eq!(recovered.record(), audited);
}

#[test]
fn writer_recovery_and_offline_audit_reject_a_corrupt_rotation_peer() {
    const FIRST_ROTATED_GENERATION: u64 = 65;

    let root = test_root("corrupt-peer");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0x8D; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local index repository"),
    );
    let descriptor = repository
        .publish(&run(profile, 1, 5))
        .expect("publish one immutable dependency");
    let run_ref = ExactIndexRunRef::new(0, descriptor).expect("Run reference is valid");
    for generation in 1..=FIRST_ROTATED_GENERATION {
        let run_set = ExactIndexRunSet::new(profile, generation, vec![run_ref])
            .expect("worked Run Set generation is valid");
        repository
            .activate(&run_set)
            .expect("seed one complete activation");
    }

    let corrupt_peer = OpenOptions::new()
        .write(true)
        .open(root.join("exact-index.activation.wal"))
        .expect("open the now-inactive rotation peer");
    corrupt_peer
        .write_all_at(&[0], 0)
        .expect("corrupt one authenticated byte");
    corrupt_peer
        .sync_all()
        .expect("make the corruption observable to every reader");

    let successor = ExactIndexRunSet::new(profile, FIRST_ROTATED_GENERATION + 1, vec![run_ref])
        .expect("successor Run Set is valid");
    assert!(matches!(
        repository.activate(&successor),
        Err(ExactIndexStoreError::ActivationWalCorrupt)
    ));
    assert!(matches!(
        repository.recover_active(),
        Err(ExactIndexStoreError::ActivationWalCorrupt)
    ));
    assert!(matches!(
        repository.audit_activation_log(),
        Err(ExactIndexStoreError::ActivationWalCorrupt)
    ));
}

#[test]
fn recovery_and_offline_audit_reject_a_run_set_missing_one_family_partition() {
    let root = test_root("missing-family-partition");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0x8E; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local index repository"),
    );
    let first = repository
        .publish(&run(profile, 10, 10))
        .expect("publish first family partition");
    let second = repository
        .publish(&run(profile, 11, 20))
        .expect("publish second family partition");
    let run_set = ExactIndexRunSet::new(
        profile,
        1,
        vec![
            ExactIndexRunRef::family_partition(1, 10, 0, 2, first)
                .expect("first family reference is valid"),
            ExactIndexRunRef::family_partition(1, 10, 1, 2, second)
                .expect("second family reference is valid"),
        ],
    )
    .expect("complete family Run Set is valid");
    repository
        .activate(&run_set)
        .expect("activate the complete durable family");

    let mut profile_hex = String::with_capacity(64);
    for byte in profile.bytes() {
        use std::fmt::Write as _;
        write!(&mut profile_hex, "{byte:02x}").expect("write into an owned String");
    }
    std::fs::remove_file(root.join(format!("{profile_hex}.{:016x}.fdx", 11)))
        .expect("remove only the selected test partition");

    assert!(matches!(
        repository.recover_active(),
        Err(ExactIndexStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    assert!(matches!(
        repository.audit_activation_log(),
        Err(ExactIndexStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
}
