use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
    ExactIndexRunRef,
};
use fastdup_store::{ExactIndexRunRepository, ExactIndexStoreError, FsStorageIo};

fn test_root(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!(
            "exact-index-repository-{name}-{}",
            std::process::id()
        ))
}

fn entry(ordinal: u8) -> ExactIndexEntry {
    entry_with_crc(ordinal, 0xAB00_0000 + u32::from(ordinal))
}

fn entry_with_crc(ordinal: u8, record_crc32c: u32) -> ExactIndexEntry {
    let logical_length = 16_384 + u32::from(ordinal);
    let record_length = (logical_length + 255) / 64 * 64;
    let location = ExactIndexLocation::raw(
        ContainerId::new([ordinal + 1; 16]).expect("container identity is nonzero"),
        u64::from(ordinal) + 1,
        4_096 + u64::from(ordinal) * 64,
        record_length,
        record_crc32c,
    )
    .expect("worked RAW location is valid");
    ExactIndexEntry::active(ChunkId::from_bytes([ordinal; 32]), logical_length, location)
        .expect("worked active entry is valid")
}

#[test]
fn atomic_transition_activation_excludes_the_old_location_and_drains_its_pins() {
    let root = test_root("retiring-generation-drain");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0xE4; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local transition repository"),
    );
    let active_entry = entry(9);
    let first = repository
        .append_level_zero(profile, vec![active_entry])
        .expect("activate the first exact generation");
    assert!(first.into_retired().is_none());
    let old_pin = repository
        .pin_active_generation()
        .expect("the first generation is installed");
    let old_snapshot = old_pin.snapshot();
    assert!(old_snapshot.try_pin().is_some());
    repository
        .append_level_zero(profile, vec![entry(11)])
        .expect("an unrelated L0 generation may advance while the old reader remains pinned");

    let retiring = ExactIndexEntry::retiring(active_entry).expect("ACTIVE may retire");
    let second = repository
        .append_level_zero(profile, vec![retiring])
        .expect("activate the RETIRING barrier");
    let current = second.current().clone();
    let lookup = current
        .lookup_transitions(active_entry.chunk_id(), active_entry.logical_length())
        .expect("lookup the transitioned physical Location");
    assert_eq!(lookup.candidates()[0], retiring);
    assert_eq!(lookup.candidates()[1], active_entry);
    let drain = second
        .into_retired()
        .expect("the first generation was displaced");
    assert!(
        old_snapshot.try_pin().is_none(),
        "displaced reduction snapshots cannot start new DATA reads"
    );
    assert!(!drain.is_drained());

    let waiter = std::thread::spawn(move || drain.wait());
    assert!(!waiter.is_finished());
    drop(old_pin);
    waiter.join().expect("pin drain worker does not panic");
}

#[test]
fn level_zero_append_rejects_resurrection_and_skipped_retirement_states() {
    let root = test_root("transition-state-machine");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0xE6; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local transition repository"),
    );
    let active = entry(10);
    repository
        .append_level_zero(profile, vec![active])
        .expect("activate the physical Location");
    let initial_activation = repository
        .pin_active_generation()
        .expect("the physical Location has an activation")
        .record();
    let retiring = ExactIndexEntry::retiring(active).expect("ACTIVE may retire");
    let removed = ExactIndexEntry::removed(retiring).expect("RETIRING may be removed");
    assert!(matches!(
        repository.append_level_zero(profile, vec![removed]),
        Err(ExactIndexStoreError::InvalidLocationTransition)
    ));
    repository
        .append_level_zero_if_active(profile, initial_activation, vec![retiring])
        .expect("activate the retirement barrier");
    assert!(matches!(
        repository.append_level_zero_if_active(profile, initial_activation, vec![retiring]),
        Err(ExactIndexStoreError::ActivationChanged)
    ));
    assert!(matches!(
        repository.append_level_zero(profile, vec![active]),
        Err(ExactIndexStoreError::InvalidLocationTransition)
    ));
    repository
        .append_level_zero(profile, vec![removed])
        .expect("RETIRING may advance to REMOVED");
}

#[test]
fn family_compaction_uses_family_precedence_and_opens_one_partition_at_a_time() {
    let root = test_root("compact-partitioned-input-family");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0xE5; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local compaction repository"),
    );
    let old_first = repository
        .publish(
            &ExactIndexRun::new(profile, 1, vec![entry_with_crc(7, 0x1111_1111)])
                .expect("old first partition is valid"),
        )
        .expect("publish old first partition");
    let old_second = repository
        .publish(
            &ExactIndexRun::new(profile, 2, vec![entry(8)]).expect("old second partition is valid"),
        )
        .expect("publish old second partition");
    let newer = repository
        .publish(
            &ExactIndexRun::new(profile, 3, vec![entry_with_crc(7, 0x2222_2222)])
                .expect("newer singleton family is valid"),
        )
        .expect("publish newer family");
    let inputs = vec![
        ExactIndexRunRef::family_partition(0, 1, 0, 2, old_first)
            .expect("old first reference is valid"),
        ExactIndexRunRef::family_partition(0, 1, 1, 2, old_second)
            .expect("old second reference is valid"),
        ExactIndexRunRef::new(0, newer).expect("newer singleton reference is valid"),
    ];

    let output = repository
        .compact_family(&inputs, 1, 4)
        .expect("complete source families compact");
    assert_eq!(output.runs().len(), 1);
    let lookup = repository
        .open(profile, 4)
        .expect("open compacted output")
        .lookup(ChunkId::from_bytes([7; 32]), 16_391)
        .expect("lookup repeated Location");
    assert_eq!(lookup.candidates().len(), 1);
    assert_eq!(
        lookup.candidates()[0].location().record_crc32c(),
        0x2222_2222
    );
}

#[test]
fn compaction_fanin_is_bounded_by_families_not_physical_partitions() {
    const FAMILY_GENERATION: u64 = 100;
    const PARTITION_COUNT: u16 = 65;

    let root = test_root("compact-many-source-partitions");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0xE6; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local compaction repository"),
    );
    let mut inputs = Vec::new();
    for ordinal in 0..PARTITION_COUNT {
        let entry_ordinal = u8::try_from(ordinal).expect("worked ordinal fits u8");
        let generation = FAMILY_GENERATION + u64::from(ordinal);
        let descriptor = repository
            .publish(
                &ExactIndexRun::new(profile, generation, vec![entry(entry_ordinal)])
                    .expect("one source partition is valid"),
            )
            .expect("publish one old-family partition");
        inputs.push(
            ExactIndexRunRef::family_partition(
                0,
                FAMILY_GENERATION,
                ordinal,
                PARTITION_COUNT,
                descriptor,
            )
            .expect("old-family partition reference is valid"),
        );
    }
    let newer_generation = 200;
    let newer = repository
        .publish(
            &ExactIndexRun::new(
                profile,
                newer_generation,
                vec![entry_with_crc(42, 0x4242_4242)],
            )
            .expect("newer singleton source is valid"),
        )
        .expect("publish newer singleton family");
    inputs.push(ExactIndexRunRef::new(0, newer).expect("newer reference is valid"));

    let output = repository
        .compact_family(&inputs, 1, 201)
        .expect("66 physical Runs in two logical families remain valid bounded fan-in");
    assert_eq!(output.runs().len(), 1);
    assert_eq!(output.runs()[0].entry_count(), 65);
    let lookup = repository
        .open(profile, 201)
        .expect("open compacted family")
        .lookup(ChunkId::from_bytes([42; 32]), 16_426)
        .expect("lookup newer transition");
    assert_eq!(lookup.candidates().len(), 1);
    assert_eq!(
        lookup.candidates()[0].location().record_crc32c(),
        0x4242_4242
    );
}

fn large_entry(ordinal: u32) -> ExactIndexEntry {
    let mut chunk_id = [0_u8; 32];
    chunk_id[28..].copy_from_slice(&ordinal.to_be_bytes());
    let mut container_id = [0_u8; 16];
    container_id[..4].copy_from_slice(
        &ordinal
            .checked_add(1)
            .expect("fixture ordinal stays below u32::MAX")
            .to_be_bytes(),
    );
    let logical_length = 16_384;
    let record_length = (logical_length + 255) / 64 * 64;
    let location = ExactIndexLocation::raw(
        ContainerId::new(container_id).expect("fixture Container identity is nonzero"),
        u64::from(ordinal) + 1,
        4_096,
        record_length,
        ordinal,
    )
    .expect("worked RAW location is valid");
    ExactIndexEntry::active(ChunkId::from_bytes(chunk_id), logical_length, location)
        .expect("worked large active entry is valid")
}

#[test]
fn published_run_reopens_for_bounded_lookup_and_complete_audit() {
    let root = test_root("lookup");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0xE1; 32]).expect("profile identity is nonzero");
    let run = ExactIndexRun::new(profile, 7, (0_u8..96).rev().map(entry).collect())
        .expect("worked run is canonicalizable");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local index repository"),
    );

    repository.publish(&run).expect("publish immutable run");
    let reader = repository
        .open(profile, 7)
        .expect("reopen run through its verified envelope");
    let found = reader
        .lookup(ChunkId::from_bytes([63; 32]), 16_447)
        .expect("bounded lookup succeeds");
    assert!(found.complete());
    assert_eq!(found.candidates().len(), 1);
    assert_eq!(
        found.candidates()[0].chunk_id(),
        ChunkId::from_bytes([63; 32])
    );

    let missing = reader
        .lookup(ChunkId::from_bytes([0xFE; 32]), 16_384)
        .expect("negative lookup remains a nonauthoritative hint");
    assert!(missing.complete());
    assert!(missing.candidates().is_empty());
    repository
        .audit(profile, 7)
        .expect("offline audit verifies every page and the complete run hash");
}

#[test]
fn compaction_is_order_independent_and_retains_one_newest_transition_per_location() {
    fn compact_in_order(
        name: &str,
        profile: ExactIndexProfileId,
        generations: &[u64],
    ) -> (
        fastdup_format::ExactIndexRunDescriptor,
        ExactIndexRunRepository<FsStorageIo>,
    ) {
        let root = test_root(name);
        if root.exists() {
            std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
        }
        let repository = ExactIndexRunRepository::new(
            FsStorageIo::open(&root).expect("open workspace-local compaction repository"),
        );
        let mut references = Vec::new();
        for generation in generations {
            let run = ExactIndexRun::new(
                profile,
                *generation,
                vec![
                    entry(7),
                    entry(u8::try_from(*generation).expect("fixture fits u8")),
                ],
            )
            .expect("source Run is canonicalizable");
            let descriptor = repository.publish(&run).expect("publish one source Run");
            references
                .push(ExactIndexRunRef::new(0, descriptor).expect("source Run reference is valid"));
        }
        let descriptor = repository
            .compact(&references, 10)
            .expect("compact fully audited source Runs");
        (descriptor, repository)
    }

    let profile = ExactIndexProfileId::new([0xE2; 32]).expect("profile identity is nonzero");
    let (forward, forward_repository) = compact_in_order("compact-forward", profile, &[1, 2, 3, 4]);
    let (reverse, _reverse_repository) =
        compact_in_order("compact-reverse", profile, &[4, 3, 2, 1]);
    assert_eq!(
        forward.run_hash(),
        reverse.run_hash(),
        "source discovery order must not change canonical compacted bytes"
    );
    assert_eq!(forward.entry_count(), 5);

    let lookup = forward_repository
        .open(profile, 10)
        .expect("open the compacted Run")
        .lookup(ChunkId::from_bytes([7; 32]), 16_391)
        .expect("lookup the repeated physical Location");
    assert!(lookup.complete());
    assert_eq!(
        lookup.candidates().len(),
        1,
        "four repeated transitions for one physical Location collapse to the newest one"
    );
    forward_repository
        .audit(profile, 10)
        .expect("offline audit verifies the complete compacted Run");
}

#[test]
fn compaction_rejects_a_corrupt_source_without_publishing_output() {
    let root = test_root("compact-corrupt-source");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0xE3; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local compaction repository"),
    );
    let mut references = Vec::new();
    for generation in [1_u64, 2] {
        let descriptor = repository
            .publish(
                &ExactIndexRun::new(
                    profile,
                    generation,
                    vec![entry(u8::try_from(generation).expect("fixture fits u8"))],
                )
                .expect("source Run is valid"),
            )
            .expect("publish one source Run");
        references
            .push(ExactIndexRunRef::new(0, descriptor).expect("source Run reference is valid"));
    }

    let mut encoded_profile = String::with_capacity(64);
    for byte in profile.bytes() {
        use std::fmt::Write as _;
        write!(&mut encoded_profile, "{byte:02x}").expect("write to an owned String");
    }
    let source_path = root.join(format!("{encoded_profile}.{:016x}.fdx", 1));
    let source = OpenOptions::new()
        .read(true)
        .write(true)
        .open(source_path)
        .expect("open the published source Run");
    let mut byte = [0_u8; 1];
    source
        .read_exact_at(&mut byte, 4_096 + 128)
        .expect("read one source entry byte");
    byte[0] ^= 1;
    source
        .write_all_at(&byte, 4_096 + 128)
        .expect("corrupt one checksummed source entry byte");
    source.sync_all().expect("make fixture corruption visible");

    assert!(matches!(
        repository
            .compact(&references, 3)
            .expect_err("compaction must fail closed on a corrupt input"),
        ExactIndexStoreError::Format(_)
    ));
    assert!(matches!(
        repository.open(profile, 3),
        Err(ExactIndexStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
}

#[test]
fn compaction_streams_more_than_the_legacy_262_144_entry_limit() {
    const FIRST_COUNT: u32 = 131_073;
    const TOTAL_COUNT: u32 = 262_145;

    let root = test_root("compact-above-legacy-entry-limit");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let profile = ExactIndexProfileId::new([0xE4; 32]).expect("profile identity is nonzero");
    let repository = ExactIndexRunRepository::new(
        FsStorageIo::open(&root).expect("open workspace-local compaction repository"),
    );
    let mut references = Vec::new();
    for (generation, range) in [(1_u64, 0..FIRST_COUNT), (2_u64, FIRST_COUNT..TOTAL_COUNT)] {
        let run = ExactIndexRun::new(profile, generation, range.map(large_entry).collect())
            .expect("large source Run is canonicalizable");
        let descriptor = repository
            .publish(&run)
            .expect("publish one large source Run");
        references
            .push(ExactIndexRunRef::new(0, descriptor).expect("large source reference is valid"));
    }

    let family = repository
        .compact_family(&references, 1, 3)
        .expect("compaction must partition beyond the per-Run target");
    assert_eq!(family.family_generation(), 3);
    assert_eq!(family.last_generation(), 4);
    assert_eq!(family.runs().len(), 2);
    assert_eq!(family.runs()[0].entry_count(), 262_144);
    assert_eq!(family.runs()[1].entry_count(), 1);
    assert_eq!(family.runs()[0].partition_ordinal(), 0);
    assert_eq!(family.runs()[1].partition_ordinal(), 1);
    assert_eq!(family.runs()[0].partition_count(), 2);
    assert!(family.runs()[0].maximum_chunk_id() < family.runs()[1].minimum_chunk_id());
    for run_ref in family.runs() {
        repository
            .audit(profile, run_ref.generation())
            .expect("every family partition remains fully auditable");
    }
    for ordinal in [0, TOTAL_COUNT - 1] {
        let expected = large_entry(ordinal);
        let partition = family
            .runs()
            .iter()
            .find(|run| {
                run.minimum_chunk_id() <= expected.chunk_id()
                    && expected.chunk_id() <= run.maximum_chunk_id()
            })
            .expect("exactly one partition covers the key");
        let lookup = repository
            .open(profile, partition.generation())
            .expect("open streamed compacted partition")
            .lookup(expected.chunk_id(), expected.logical_length())
            .expect("bounded lookup in streamed compacted Run");
        assert!(lookup.complete());
        assert_eq!(lookup.candidates(), &[expected]);
    }
}
