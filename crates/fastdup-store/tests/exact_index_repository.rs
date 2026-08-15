use std::path::{Path, PathBuf};

use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId, ExactIndexRun,
};
use fastdup_store::{ExactIndexRunRepository, FsStorageIo};

fn test_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("exact-index-repository-{}", std::process::id()))
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
    let root = test_root();
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
