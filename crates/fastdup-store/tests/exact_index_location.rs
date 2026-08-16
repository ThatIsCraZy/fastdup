use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_format::{
    ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef, ExactIndexRunSet, ExactLocationTransition,
};
use fastdup_store::{ContainerRepository, ExactIndexRunRepository, FsStorageIo, StoreError};

fn test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock is after the Unix epoch")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!(
            "exact-index-location-{name}-{}-{nonce}",
            std::process::id()
        ))
}

#[test]
fn exact_candidate_is_usable_only_after_pairing_with_its_verified_container() {
    let root = test_root("raw");
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

#[test]
fn zstd_candidates_are_bounded_verified_locations_for_every_logical_chunk() {
    let root = test_root("zstd");
    let storage = FsStorageIo::open(&root).expect("open shared repository root");
    let repository = ContainerRepository::new(storage.clone());
    let container_id = ContainerId::new([0xA1; 16]).expect("container ID is nonzero");
    let first = (0..192 * 1_024)
        .map(|index| b'a' + u8::try_from(index % 19).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    let second = (0..192 * 1_024)
        .map(|index| b'A' + u8::try_from(index % 17).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    repository
        .publish_adaptive_regions(container_id, 9, &[&[first.as_slice(), second.as_slice()]])
        .expect("publish one durable multi-Chunk Zstd record");
    let container = repository
        .read(container_id)
        .expect("reread the complete verified Zstd Container");
    assert_eq!(container.zstd_record_count(), 1);
    let entries = container
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<Vec<_>, _>>()
        .expect("construct acceleration entries only from verified evidence");
    assert_eq!(entries.len(), 2);

    let profile = ExactIndexProfileId::new([0xA2; 32]).expect("profile identity is nonzero");
    let index_repository = ExactIndexRunRepository::new(storage);
    let descriptor = index_repository
        .publish(&ExactIndexRun::new(profile, 1, entries.clone()).expect("build the immutable Run"))
        .expect("publish the immutable Run");
    let active = index_repository
        .activate(
            &ExactIndexRunSet::new(
                profile,
                1,
                vec![ExactIndexRunRef::new(0, descriptor).expect("pin the verified Run")],
            )
            .expect("build the Run Set"),
        )
        .expect("activate the complete Run Set");

    for (entry, expected) in entries.iter().zip([first, second]) {
        let lookup = active
            .lookup_transitions(entry.chunk_id(), entry.logical_length())
            .expect("perform the bounded persistent lookup");
        assert_eq!(lookup.candidates(), &[*entry]);
        assert_eq!(
            repository
                .read_verified_location(lookup.candidates()[0])
                .expect("decode and verify the complete selected Zstd record"),
            expected
        );
    }
}
