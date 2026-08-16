use std::num::NonZeroUsize;

use fastdup_format::{ContainerId, FormatError, HEADER_BYTES, SealedContainer};

#[test]
fn zstd_region_round_trips_multiple_logical_chunks_byte_exactly() {
    let first = vec![b'A'; 96 * 1_024];
    let second = (0..160 * 1_024)
        .map(|index| b'a' + u8::try_from(index % 23).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    let chunks = [first.as_slice(), second.as_slice()];
    let encoded = SealedContainer::encode_zstd_regions(
        ContainerId::new([0xC1; 16]).expect("container identity is nonzero"),
        1,
        &[&chunks],
    )
    .expect("encode one bounded Zstd Compression Region");

    let container = SealedContainer::decode(&encoded)
        .expect("reader verifies and decodes the complete Zstd Container");

    assert_eq!(container.chunk_count(), 2);
    assert_eq!(container.zstd_record_count(), 1);
    assert_eq!(container.raw_record_count(), 0);
    assert_eq!(
        container.chunk(fastdup_format::ChunkId::of(&first)),
        Some(first.as_slice())
    );
    assert_eq!(
        container.chunk(fastdup_format::ChunkId::of(&second)),
        Some(second.as_slice())
    );
}

#[test]
fn authenticated_wrong_chunk_identity_is_rejected_after_zstd_decode() {
    let payload = (0..128 * 1_024)
        .map(|index| b'a' + u8::try_from(index % 19).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    let chunks = [payload.as_slice()];
    let mut encoded = SealedContainer::encode_zstd_regions(
        ContainerId::new([0xC2; 16]).expect("container identity is nonzero"),
        2,
        &[&chunks],
    )
    .expect("encode one Zstd record");
    let record_length =
        usize::try_from(get_u32(&encoded, HEADER_BYTES + 32)).expect("record length fits usize");
    let index_offset = usize::try_from(get_u64(&encoded, 72)).expect("index offset fits usize");
    let index_length = usize::try_from(get_u64(&encoded, 80)).expect("index length fits usize");
    let footer_offset = usize::try_from(get_u64(&encoded, 88)).expect("footer offset fits usize");
    encoded[HEADER_BYTES + 128..HEADER_BYTES + 160].fill(0x44);
    encoded[index_offset + 64..index_offset + 96].fill(0x44);
    reauthenticate_crc(&mut encoded[HEADER_BYTES..HEADER_BYTES + record_length], 60);
    reauthenticate_crc(&mut encoded[index_offset..index_offset + index_length], 36);
    encoded[footer_offset + 96..footer_offset + 132].fill(0);
    let hash = blake3::hash(&encoded);
    encoded[footer_offset + 96..footer_offset + 128].copy_from_slice(hash.as_bytes());
    reauthenticate_crc(&mut encoded[footer_offset..], 128);

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::ChunkHashMismatch)
    );
}

#[test]
fn every_zstd_container_prefix_and_single_byte_corruption_is_rejected_without_panicking() {
    let payload = (0..96 * 1_024)
        .map(|index| b'A' + u8::try_from(index % 13).expect("fixture remainder fits u8"))
        .collect::<Vec<_>>();
    let chunks = [payload.as_slice()];
    let encoded = SealedContainer::encode_zstd_regions(
        ContainerId::new([0xC3; 16]).expect("container identity is nonzero"),
        3,
        &[&chunks],
    )
    .expect("encode one Zstd record");

    for prefix_length in 0..encoded.len() {
        let outcome =
            std::panic::catch_unwind(|| SealedContainer::decode(&encoded[..prefix_length]));
        assert!(outcome.is_ok(), "prefix {prefix_length} panicked");
        assert!(outcome.expect("checked above").is_err());
    }
    for offset in 0..encoded.len() {
        let mut corrupted = encoded.clone();
        corrupted[offset] ^= 1;
        let outcome = std::panic::catch_unwind(|| SealedContainer::decode(&corrupted));
        assert!(outcome.is_ok(), "byte offset {offset} panicked");
        assert!(outcome.expect("checked above").is_err());
    }
}

#[test]
fn parallel_adaptive_encoding_is_byte_identical_to_one_worker() {
    let owned = (0..24_usize)
        .map(|ordinal| {
            (0..64 * 1_024)
                .map(|index| {
                    let lane = (index + ordinal * 7) % 29;
                    b'a' + u8::try_from(lane).expect("fixture lane fits u8")
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let regions = owned
        .chunks(4)
        .map(|chunks| chunks.iter().map(Vec::as_slice).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let references = regions.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let container_id = ContainerId::new([0xD4; 16]).expect("container identity is nonzero");
    let one = SealedContainer::encode_adaptive_regions_parallel(
        container_id,
        17,
        &references,
        NonZeroUsize::new(1).expect("one is nonzero"),
    )
    .expect("encode with one worker");
    let four = SealedContainer::encode_adaptive_regions_parallel(
        container_id,
        17,
        &references,
        NonZeroUsize::new(4).expect("four is nonzero"),
    )
    .expect("encode with four workers");
    assert_eq!(four, one);
    let decoded = SealedContainer::decode(&four).expect("decode the parallel writer output");
    assert_eq!(decoded.chunk_count(), owned.len());
}

fn reauthenticate_crc(bytes: &mut [u8], field_offset: usize) {
    bytes[field_offset..field_offset + 4].fill(0);
    let checksum = crc32c::crc32c(bytes);
    bytes[field_offset..field_offset + 4].copy_from_slice(&checksum.to_le_bytes());
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("worked fixture field is four bytes"),
    )
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("worked fixture field is eight bytes"),
    )
}
