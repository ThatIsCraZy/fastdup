use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_format::{
    ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef, ExactIndexRunSet, ExactLocationTransition,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, StorageIo, StoreError,
};

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
fn empty_active_exact_generation_audits_as_zero_active_locations() {
    let root = test_root("empty-active");
    let storage = FsStorageIo::open(&root).expect("open shared repository root");
    let containers = ContainerRepository::new(storage.clone());
    let profile = ExactIndexProfileId::new([0x90; 32]).expect("profile identity is nonzero");
    let indexes = ExactIndexRunRepository::new(storage);
    let empty = ExactIndexRunSet::new(profile, 1, Vec::new())
        .expect("an empty Run Set is a durable Exact tombstone");
    indexes
        .activate(&empty)
        .expect("activate the empty Exact tombstone");

    let audit = indexes
        .audit_active_locations(&containers)
        .expect("scrub accepts an active empty Exact generation")
        .expect("the empty Exact generation remains selected");

    assert_eq!(audit.active_locations(), 0);
    assert_eq!(audit.activation().run_set_generation(), 1);
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
fn retiring_transition_shadows_older_active_location_during_lookup_and_scrub() {
    let root = test_root("retiring-shadow");
    let storage = FsStorageIo::open(&root).expect("open shared repository root");
    let containers = ContainerRepository::new(storage.clone());
    let container_id = ContainerId::new([0x93; 16]).expect("container ID is nonzero");
    let payload = b"retiring locations must never be selected from an older run";
    containers
        .publish_raw(container_id, 1, &[payload])
        .expect("publish victim Container");
    let container = containers
        .read(container_id)
        .expect("verify victim Container");
    let active_entry = ExactIndexEntry::from_verified_raw(container.raw_locations()[0])
        .expect("construct verified ACTIVE transition");
    let profile = ExactIndexProfileId::new([0x94; 32]).expect("profile identity is nonzero");
    let indexes = ExactIndexRunRepository::new(storage.clone());
    indexes
        .append_level_zero(profile, vec![active_entry])
        .expect("activate initial Location");
    let retiring = ExactIndexEntry::retiring(active_entry).expect("ACTIVE may retire");
    let transition = indexes
        .append_level_zero(profile, vec![retiring])
        .expect("activate RETIRING barrier");
    assert!(
        containers
            .find_verified_location_with_index(
                transition.current(),
                active_entry.chunk_id(),
                u64::from(active_entry.logical_length()),
            )
            .expect("transition lookup degrades safely")
            .is_none(),
        "the older ACTIVE occurrence of the same physical Location is shadowed"
    );

    let published = storage
        .list_names()
        .expect("list test objects")
        .into_iter()
        .find(|name| {
            std::path::Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("fdc"))
        })
        .expect("victim canonical name exists");
    storage
        .remove_file(&published)
        .expect("simulate post-drain unlink");
    storage.sync_root().expect("make test unlink durable");
    let audit = indexes
        .audit_active_locations(&containers)
        .expect("scrub ignores shadowed non-ACTIVE physical dependencies")
        .expect("one Exact generation remains active");
    assert_eq!(audit.active_locations(), 0);
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

#[test]
fn zstd_prefix_candidate_resolves_only_an_independent_pool_base() {
    let root = test_root("zstd-prefix");
    let storage = FsStorageIo::open(&root).expect("open shared repository root");
    let repository = ContainerRepository::new(storage.clone());
    let base_container_id = ContainerId::new([0xB1; 16]).expect("Base ID is nonzero");
    let target_container_id = ContainerId::new([0xB2; 16]).expect("target ID is nonzero");
    let base = deterministic_bytes(64 * 1_024, 0x71);
    let mut target = base.clone();
    target.rotate_left(1);

    repository
        .publish_raw(base_container_id, 11, &[base.as_slice()])
        .expect("publish independent Base Container");
    let base_container = repository
        .read(base_container_id)
        .expect("independently verify Base Container");
    let base_entry = ExactIndexEntry::from_verified(base_container.locations()[0])
        .expect("construct Base Exact entry");
    let target_publication = repository
        .publish_zstd_prefix_pairs_verified(
            target_container_id,
            12,
            &[(base.as_slice(), target.as_slice())],
        )
        .expect("publish dependent target Container");
    let target_entry = ExactIndexEntry::from_verified(target_publication.locations()[0])
        .expect("construct dependent Exact entry");
    assert_eq!(
        target_entry.location().dependency_id(),
        base_entry.chunk_id().bytes()
    );
    assert!(repository.read_verified_location(target_entry).is_err());

    let profile = ExactIndexProfileId::new([0xB3; 32]).expect("profile ID is nonzero");
    let index_repository = ExactIndexRunRepository::new(storage);
    let descriptor = index_repository
        .publish(
            &ExactIndexRun::new(profile, 1, vec![base_entry, target_entry])
                .expect("build mixed independent/dependent Run"),
        )
        .expect("publish mixed Run");
    let active = index_repository
        .activate(
            &ExactIndexRunSet::new(
                profile,
                1,
                vec![ExactIndexRunRef::new(0, descriptor).expect("pin verified Run")],
            )
            .expect("build mixed Run Set"),
        )
        .expect("activate mixed Run Set");

    assert_eq!(
        repository
            .find_verified_chunk_with_index(
                &active,
                target_entry.chunk_id(),
                u64::from(target_entry.logical_length()),
            )
            .expect("bounded dependent lookup succeeds")
            .expect("dependent target is present"),
        target
    );
    let recovered = repository
        .recover_published_with_index(&active)
        .expect("recovery resolves dependent Containers through the active index");
    let recovered_target = recovered
        .iter()
        .find(|container| container.header().container_id() == target_container_id)
        .expect("target Container is recovered");
    assert_eq!(
        recovered_target
            .chunk(target_entry.chunk_id())
            .expect("recovered target Chunk is present"),
        target
    );
    index_repository
        .audit_active_locations(&repository)
        .expect("offline Exact Location audit resolves Depth-1 dependencies")
        .expect("one active Run Set exists");
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
