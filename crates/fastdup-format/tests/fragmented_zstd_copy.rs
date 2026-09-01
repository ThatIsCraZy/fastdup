use std::num::NonZeroUsize;

use fastdup_copy_metrics::copy_telemetry;
use fastdup_format::{
    ChunkId, ContainerId, IncompressibilityGatePolicy, PrehashedChunk, SealedContainer,
};

#[test]
fn gate_off_borrowed_region_avoids_the_join_copy() {
    let owned = (0..8_usize)
        .map(|ordinal| {
            b"borrowed-fragment\n"
                .iter()
                .copied()
                .cycle()
                .skip(ordinal)
                .take(64 * 1_024)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let chunks = owned
        .iter()
        .map(|bytes| PrehashedChunk::new(ChunkId::of(bytes), bytes))
        .collect::<Vec<_>>();
    let before = copy_telemetry();
    let encoded = SealedContainer::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
        ContainerId::new([0xDA; 16]).expect("fixture Container ID is nonzero"),
        31,
        &[&chunks],
        NonZeroUsize::MIN,
        IncompressibilityGatePolicy::Off,
    )
    .expect("borrowed region encodes without materialization");
    let after = copy_telemetry();

    assert_eq!(
        after.compression_region_materialization_bytes,
        before.compression_region_materialization_bytes
    );
    let decoded = SealedContainer::decode(encoded.bytes()).expect("streamed Container verifies");
    assert_eq!(decoded.chunk_count(), chunks.len());
}
