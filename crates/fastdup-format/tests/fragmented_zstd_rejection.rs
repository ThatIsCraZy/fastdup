use fastdup_format::{
    ChunkId, ContainerId, IncompressibilityGatePolicy, PrehashedChunk, SealedContainer,
};
use std::num::NonZeroUsize;

#[test]
fn incompressible_fragmented_regions_fall_back_to_raw_across_chunk_boundaries() {
    let mut state = 0x8f31_a7c5_19d2_4e6b_u64;
    let bytes = (0..524288)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect::<Vec<_>>();
    for chunk_size in [4096, 8192, 16384, 32768, 65536, 131072, 196608, 262144] {
        for length in [131072, 262144, 264957, 393216, 524288] {
            let chunks = bytes[..length]
                .chunks(chunk_size)
                .map(|chunk| PrehashedChunk::new(ChunkId::of(chunk), chunk))
                .collect::<Vec<_>>();
            let encoded =
                SealedContainer::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
                    ContainerId::new([0xeb; 16]).unwrap(),
                    1,
                    &[&chunks],
                    NonZeroUsize::MIN,
                    IncompressibilityGatePolicy::Off,
                )
                .unwrap_or_else(|error| {
                    panic!("length={length} chunk_size={chunk_size}: {error:?}")
                });
            let decoded = SealedContainer::decode(encoded.bytes()).unwrap();
            assert_eq!(decoded.chunk_count(), chunks.len());
            assert_eq!(encoded.metrics().target_zstd_rejected(), 1);
            for chunk in bytes[..length].chunks(chunk_size) {
                assert_eq!(decoded.chunk(ChunkId::of(chunk)), Some(chunk));
            }
        }
    }
    // An abandoned bounded stream must not poison the next frame on this worker.
    let compressible = vec![b'A'; 264957];
    let chunks = compressible
        .chunks(8192)
        .map(|chunk| PrehashedChunk::new(ChunkId::of(chunk), chunk))
        .collect::<Vec<_>>();
    let encoded = SealedContainer::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
        ContainerId::new([0xec; 16]).unwrap(),
        2,
        &[&chunks],
        NonZeroUsize::MIN,
        IncompressibilityGatePolicy::Off,
    )
    .unwrap();
    assert_eq!(encoded.metrics().target_zstd_accepted(), 1);
    let decoded = SealedContainer::decode(encoded.bytes()).unwrap();
    for chunk in compressible.chunks(8192) {
        assert_eq!(decoded.chunk(ChunkId::of(chunk)), Some(chunk));
    }
}
