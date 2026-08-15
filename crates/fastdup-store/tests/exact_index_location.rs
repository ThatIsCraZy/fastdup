use std::path::{Path, PathBuf};

use fastdup_format::{
    ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef, ExactIndexRunSet, ExactLocationTransition,
};
use fastdup_store::{ContainerRepository, ExactIndexRunRepository, FsStorageIo, StoreError};

fn test_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("exact-index-location-{}", std::process::id()))
}

#[test]
fn exact_candidate_is_usable_only_after_pairing_with_its_verified_container() {
    let root = test_root();
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let storage = FsStorageIo::open(&root).expect("open shared repository root");
    let repository = ContainerRepository::new(storage.clone());
    let container_id = ContainerId::new([0x91; 16]).expect("container ID is nonzero");
    let payload = b"the exact-index candidate is never authority by itself";
    repository
        .publish_raw(container_id, 7, &[payload])
        .expect("publish one durable RAW container");
    let container = repository
        .read(container_id)
        .expect("reread the complete verified Container");
    let candidate = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct an acceleration entry only from verified evidence");

    let profile = ExactIndexProfileId::new([0x92; 32]).expect("profile identity is nonzero");
    let index_repository = ExactIndexRunRepository::new(storage);
    let run = ExactIndexRun::new(profile, 1, vec![candidate])
        .expect("build one immutable Exact Index Run");
    let descriptor = index_repository
        .publish(&run)
        .expect("durably publish the Run");
    let run_set = ExactIndexRunSet::new(
        profile,
        1,
        vec![ExactIndexRunRef::new(0, descriptor).expect("pin the complete Run descriptor")],
    )
    .expect("build one immutable Run Set");
    index_repository
        .activate(&run_set)
        .expect("activate only after every index dependency is durable");
    let active = index_repository
        .recover_active()
        .expect("recover the complete activation graph")
        .expect("one Run Set is active");
    let lookup = active
        .lookup_transitions(candidate.chunk_id(), candidate.logical_length())
        .expect("perform bounded persistent lookup");
    assert!(lookup.complete());
    assert_eq!(lookup.candidates(), &[candidate]);
    let durable_candidate = lookup.candidates()[0];

    assert_eq!(
        durable_candidate.transition(),
        ExactLocationTransition::Active
    );
    assert_eq!(
        repository
            .read_verified_location(durable_candidate)
            .expect("pair every candidate field before returning bytes"),
        payload
    );

    let observed = candidate.location();
    let wrong_generation = ExactIndexLocation::raw(
        observed.container_id(),
        observed.container_generation() + 1,
        observed.record_offset(),
        observed.record_length(),
        observed.record_crc32c(),
    )
    .expect("the forged location remains structurally valid");
    let forged = ExactIndexEntry::active(
        candidate.chunk_id(),
        candidate.logical_length(),
        wrong_generation,
    )
    .expect("the forged entry remains structurally valid");
    let error = repository
        .read_verified_location(forged)
        .expect_err("an unpaired physical coordinate must never return bytes");
    assert!(matches!(error, StoreError::ExactLocationMismatch));
}
