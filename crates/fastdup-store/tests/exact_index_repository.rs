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
    let logical_length = 16_384 + u32::from(ordinal);
    let record_length = (logical_length + 255) / 64 * 64;
    let location = ExactIndexLocation::raw(
        ContainerId::new([ordinal + 1; 16]).expect("container identity is nonzero"),
        u64::from(ordinal) + 1,
        4_096 + u64::from(ordinal) * 64,
        record_length,
        0xAB00_0000 + u32::from(ordinal),
    )
    .expect("worked RAW location is valid");
    ExactIndexEntry::active(ChunkId::from_bytes([ordinal; 32]), logical_length, location)
        .expect("worked active entry is valid")
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
