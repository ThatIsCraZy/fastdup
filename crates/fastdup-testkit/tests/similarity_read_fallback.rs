use fastdup_format::{ChunkId, SimilarityIndexEntry, SimilarityIndexRun};
use fastdup_store::{
    SIMILARITY_FINGERPRINT_PROFILE_V1, SIMILARITY_REPRESENTATIVE_PROFILE_V1,
    SimilarityIndexReadMode, SimilarityIndexRepository,
};
use fastdup_testkit::MemoryStorageIo;

#[test]
fn non_filesystem_storage_recovers_through_bounded_reads() {
    let entry = SimilarityIndexEntry::new(
        ChunkId::of(b"generic-storage-similarity-entry"),
        64 * 1_024,
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        [11, 22, 33, 44],
        [55; 8],
    )
    .expect("construct generic storage fixture entry");
    let run = SimilarityIndexRun::new(
        SIMILARITY_FINGERPRINT_PROFILE_V1,
        SIMILARITY_REPRESENTATIVE_PROFILE_V1,
        71,
        vec![entry],
    )
    .expect("construct generic storage fixture run");
    let repository = SimilarityIndexRepository::new(MemoryStorageIo::new());
    repository
        .publish(&run)
        .expect("publish through generic storage adapter");

    let recovered = repository
        .recover_latest()
        .expect("recover through generic storage adapter")
        .expect("generic storage snapshot exists");
    assert_eq!(
        recovered.status().read_mode(),
        SimilarityIndexReadMode::ReadExactAt
    );
}
