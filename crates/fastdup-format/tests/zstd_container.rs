use std::num::NonZeroUsize;

use fastdup_format::{
    ChunkId, ContainerId, FormatError, HEADER_BYTES, IncompressibilityGatePolicy, PrehashedChunk,
    PrehashedContiguousRegion, SealedContainer,
};

#[test]
fn prehashed_adaptive_input_is_byte_identical_to_the_regular_writer() {
    let first = vec![b'A'; 192 * 1_024];
    let second = (0..192 * 1_024)
        .map(|index| u8::try_from((index * 131 + 17) % 251).expect("fixture byte fits u8"))
        .collect::<Vec<_>>();
    let chunks = [first.as_slice(), second.as_slice()];
    let identified = chunks
        .iter()
        .map(|bytes| PrehashedChunk::new(ChunkId::of(bytes), bytes))
        .collect::<Vec<_>>();
    let container_id = ContainerId::new([0xC0; 16]).expect("container identity is nonzero");

    let regular = SealedContainer::encode_adaptive_regions_parallel(
        container_id,
        9,
        &[&chunks],
        NonZeroUsize::MIN,
    )
    .expect("encode ordinary adaptive region");
    let prehashed = SealedContainer::encode_prehashed_adaptive_regions_parallel(
        container_id,
        9,
        &[identified.as_slice()],
        NonZeroUsize::MIN,
    )
    .expect("encode prehashed adaptive region");

    assert_eq!(prehashed, regular);
    SealedContainer::decode(&prehashed).expect("prehashed writer output verifies byte exactly");
}

#[test]
fn contiguous_prehashed_input_is_byte_identical_without_a_second_join_buffer() {
    let mut decoded = Vec::new();
    decoded.extend(std::iter::repeat_n(b'X', 180 * 1_024));
    decoded.extend(
        (0..180 * 1_024)
            .map(|index| u8::try_from((index * 73 + 11) % 251).expect("fixture byte fits u8")),
    );
    let boundary = 180 * 1_024;
    let chunks = [
        PrehashedChunk::new(ChunkId::of(&decoded[..boundary]), &decoded[..boundary]),
        PrehashedChunk::new(ChunkId::of(&decoded[boundary..]), &decoded[boundary..]),
    ];
    let contiguous = PrehashedContiguousRegion::new(&chunks, &decoded)
        .expect("Chunk views exactly partition the decoded buffer");
    let container_id = ContainerId::new([0xC8; 16]).expect("container identity is nonzero");

    let joined = SealedContainer::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
        container_id,
        11,
        &[&chunks],
        NonZeroUsize::MIN,
        IncompressibilityGatePolicy::V1,
    )
    .expect("encode ordinary prehashed region")
    .into_bytes();
    let borrowed =
        SealedContainer::encode_prehashed_contiguous_regions_parallel_profiled_with_gate(
            container_id,
            11,
            &[contiguous],
            NonZeroUsize::MIN,
            IncompressibilityGatePolicy::V1,
        )
        .expect("encode contiguous prehashed region")
        .into_bytes();

    assert_eq!(borrowed, joined);
}

#[test]
fn wrong_prehashed_identity_is_rejected_by_the_mandatory_reader() {
    let payload = vec![b'Q'; 192 * 1_024];
    let wrong = PrehashedChunk::new(ChunkId::from_bytes([0x55; 32]), &payload);
    let region = [wrong];
    let encoded = SealedContainer::encode_prehashed_adaptive_regions_parallel(
        ContainerId::new([0xCF; 16]).expect("container identity is nonzero"),
        10,
        &[&region],
        NonZeroUsize::MIN,
    )
    .expect("non-authoritative writer image is constructed");

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::ChunkHashMismatch)
    );
}

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
    let owned = (0..40_usize)
        .map(|ordinal| {
            let mut state = u64::try_from(ordinal + 1).expect("fixture ordinal fits u64");
            (0..64 * 1_024)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state.to_le_bytes()[0]
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
    let four_workers = NonZeroUsize::new(4).expect("four is nonzero");
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(four_workers.get())
        .build()
        .expect("dedicated test pool");
    let four = pool.install(|| {
        SealedContainer::encode_adaptive_regions_parallel(
            container_id,
            17,
            &references,
            four_workers,
        )
        .expect("encode with four workers")
    });
    assert_eq!(four, one);
    assert_eq!(
        pool.install(|| SealedContainer::container_hash_worker_count(four.len(), four_workers)),
        NonZeroUsize::MIN
    );
    let decoded = pool
        .install(|| SealedContainer::decode_with_hash_workers(&four, four_workers))
        .expect("decode the parallel writer output");
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
