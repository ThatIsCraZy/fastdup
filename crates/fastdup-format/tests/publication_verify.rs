use std::num::NonZeroUsize;

use fastdup_format::{ChunkId, ContainerId, FormatError, SealedContainer};

#[test]
fn payload_free_publication_verify_matches_full_reader_evidence() {
    let raw = pseudorandom_bytes(192 * 1_024, 0x19d2_3a61_54be_77c1);
    let compressible = vec![b'Q'; 192 * 1_024];
    let raw_region = [raw.as_slice()];
    let zstd_region = [compressible.as_slice()];
    let regions = [raw_region.as_slice(), zstd_region.as_slice()];
    let image = SealedContainer::encode_adaptive_regions_parallel(
        ContainerId::new([0x91; 16]).expect("fixture Container ID is nonzero"),
        41,
        &regions,
        NonZeroUsize::new(2).expect("literal is nonzero"),
    )
    .expect("encode mixed fixture");

    let full = SealedContainer::decode(&image).expect("full reader verifies fixture");
    let publication = SealedContainer::verify_publication_with_hash_workers(
        &image,
        NonZeroUsize::new(2).expect("literal is nonzero"),
    )
    .expect("payload-free publication verifier accepts fixture");

    assert_eq!(publication.header(), full.header());
    assert_eq!(publication.locations(), full.locations());
    assert_eq!(publication.raw_locations(), full.raw_locations());
    assert_eq!(publication.raw_record_count(), full.raw_record_count());
    assert_eq!(publication.zstd_record_count(), full.zstd_record_count());
    assert_eq!(
        publication.logical_bytes(),
        u64::try_from(raw.len() + compressible.len()).expect("fixture length fits u64")
    );
    assert!(full.chunk(ChunkId::of(&raw)).is_some());
    assert!(full.chunk(ChunkId::of(&compressible)).is_some());
}

#[test]
fn payload_free_publication_verify_rejects_corrupt_logical_bytes() {
    let payload = vec![0x2d; 64 * 1_024];
    let mut image = SealedContainer::encode(
        ContainerId::new([0x92; 16]).expect("fixture Container ID is nonzero"),
        42,
        &[payload.as_slice()],
    )
    .expect("encode fixture");
    image[4_096 + 192 + 17] ^= 1;

    assert!(matches!(
        SealedContainer::verify_publication_with_hash_workers(&image, NonZeroUsize::MIN),
        Err(FormatError::RecordChecksumMismatch)
    ));
}

fn pseudorandom_bytes(length: usize, mut state: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(length);
    for _ in 0..length {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.push(state.to_le_bytes()[0]);
    }
    bytes
}
