use std::num::NonZeroUsize;

use fastdup_format::{
    ChunkId, ContainerId, FormatError, IncompressibilityGatePolicy, PrehashedChunk,
    SealedContainer, VerifiedContainerImage,
};

fn id(byte: u8) -> ContainerId {
    ContainerId::new([byte; 16]).expect("fixture Container ID is nonzero")
}

#[test]
fn verified_multi_chunk_zstd_record_transplants_byte_for_byte() {
    let first = b"first transplant payload".repeat(8_192);
    let second = b"second transplant payload".repeat(8_192);
    let chunks = [
        PrehashedChunk::new(ChunkId::of(&first), &first),
        PrehashedChunk::new(ChunkId::of(&second), &second),
    ];
    let original = SealedContainer::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
        id(0x41),
        7,
        &[chunks.as_slice()],
        NonZeroUsize::MIN,
        IncompressibilityGatePolicy::Off,
    )
    .expect("encode original multi-Chunk Record")
    .into_bytes();
    let image = VerifiedContainerImage::decode(original.clone()).expect("verify owned image");
    assert_eq!(image.container().zstd_record_count(), 1);
    let location = image.container().locations()[0];
    let prepared = image
        .prepare_encoded_record(location.record_offset())
        .expect("extract verified independent Record");
    assert_eq!(prepared.chunk_count(), 2);

    let replacement = SealedContainer::
        encode_prehashed_adaptive_regions_with_transplants_parallel_profiled_with_gate(
            id(0x42),
            8,
            &[],
            vec![prepared],
            NonZeroUsize::MIN,
            IncompressibilityGatePolicy::Off,
        )
        .expect("assemble replacement around transplanted Record")
        .into_bytes();
    let decoded = SealedContainer::decode(&replacement).expect("verify replacement normally");
    assert_eq!(decoded.chunk(ChunkId::of(&first)), Some(first.as_slice()));
    assert_eq!(decoded.chunk(ChunkId::of(&second)), Some(second.as_slice()));

    let replacement_location = decoded.locations()[0];
    let old_start = usize::try_from(location.record_offset()).expect("old offset fits usize");
    let old_end = old_start + usize::try_from(location.record_length()).expect("old length");
    let new_start =
        usize::try_from(replacement_location.record_offset()).expect("new offset fits usize");
    let new_end = new_start
        + usize::try_from(replacement_location.record_length()).expect("new record length");
    assert_eq!(
        &original[old_start..old_end],
        &replacement[new_start..new_end]
    );

    let mut corrupted = replacement;
    corrupted[new_start + 128] ^= 1;
    assert_eq!(
        SealedContainer::decode(&corrupted),
        Err(FormatError::RecordChecksumMismatch)
    );
}
