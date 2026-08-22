use std::num::NonZeroUsize;

use fastdup_format::{ContainerId, IncompressibilityGatePolicy, SealedContainer};

#[test]
fn incompressible_region_skips_target_zstd_and_restores_byte_exactly() {
    let input = deterministic_bytes(512 * 1_024, 0x9e37_79b9_7f4a_7c15);
    let chunks = input.chunks(64 * 1_024).collect::<Vec<_>>();
    let regions = [chunks.as_slice()];

    let encoded = SealedContainer::encode_adaptive_regions_parallel_profiled(
        ContainerId::new([0x91; 16]).expect("fixture Container ID is nonzero"),
        1,
        &regions,
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
    )
    .expect("adaptive encoding succeeds");

    assert_eq!(encoded.metrics().eligible_regions(), 1);
    assert_eq!(encoded.metrics().lz4_rejected_regions(), 1);
    assert_eq!(encoded.metrics().zstd1_rejected_regions(), 1);
    assert_eq!(encoded.metrics().target_zstd_trials(), 0);
    assert_eq!(encoded.metrics().raw_regions_after_gate(), 1);

    let decoded = SealedContainer::decode(encoded.bytes()).expect("container verifies");
    let restored = decoded
        .records()
        .iter()
        .flat_map(|record| record.payload().iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(restored, input);
}

#[test]
fn compressible_region_passes_lz4_and_uses_target_zstd() {
    let line = br#"{"vm":"rocky","generation":42,"state":"clean"}
"#;
    let input = line
        .iter()
        .copied()
        .cycle()
        .take(512 * 1_024)
        .collect::<Vec<_>>();
    let chunks = input.chunks(64 * 1_024).collect::<Vec<_>>();
    let regions = [chunks.as_slice()];

    let encoded = SealedContainer::encode_adaptive_regions_parallel_profiled(
        ContainerId::new([0x92; 16]).expect("fixture Container ID is nonzero"),
        2,
        &regions,
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
    )
    .expect("adaptive encoding succeeds");

    assert_eq!(encoded.metrics().eligible_regions(), 1);
    assert_eq!(encoded.metrics().lz4_allowed_regions(), 1);
    assert_eq!(encoded.metrics().lz4_rejected_regions(), 0);
    assert_eq!(encoded.metrics().zstd1_allowed_regions(), 0);
    assert_eq!(encoded.metrics().target_zstd_trials(), 1);
    assert_eq!(encoded.metrics().target_zstd_accepted(), 1);
    assert_eq!(encoded.metrics().raw_regions_after_gate(), 0);

    let decoded = SealedContainer::decode(encoded.bytes()).expect("container verifies");
    assert_eq!(decoded.zstd_record_count(), 1);
    let restored = decoded
        .records()
        .iter()
        .flat_map(|record| record.payload().iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(restored, input);
}

#[test]
fn zstd1_rescue_preserves_long_distance_repetition_missed_by_lz4() {
    let base = deterministic_bytes(256 * 1_024, 0xd1b5_4a32_d192_ed03);
    let mut input = base.clone();
    input.extend_from_slice(&base);
    let chunks = input.chunks(64 * 1_024).collect::<Vec<_>>();
    let regions = [chunks.as_slice()];

    let encoded = SealedContainer::encode_adaptive_regions_parallel_profiled(
        ContainerId::new([0x93; 16]).expect("fixture Container ID is nonzero"),
        3,
        &regions,
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
    )
    .expect("adaptive encoding succeeds");

    assert_eq!(encoded.metrics().lz4_rejected_regions(), 1);
    assert_eq!(encoded.metrics().zstd1_allowed_regions(), 1);
    assert_eq!(encoded.metrics().zstd1_rejected_regions(), 0);
    assert_eq!(encoded.metrics().target_zstd_trials(), 1);
    assert_eq!(encoded.metrics().target_zstd_accepted(), 1);

    let decoded = SealedContainer::decode(encoded.bytes()).expect("container verifies");
    let restored = decoded
        .records()
        .iter()
        .flat_map(|record| record.payload().iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(restored, input);
}

#[test]
fn worker_count_does_not_change_gate_decisions_or_container_bytes() {
    let random = deterministic_bytes(512 * 1_024, 0xa409_3822_299f_31d0);
    let text = b"fastdup-incompressibility-gate-v1\n"
        .iter()
        .copied()
        .cycle()
        .take(512 * 1_024)
        .collect::<Vec<_>>();
    let distant = deterministic_bytes(256 * 1_024, 0x082e_fa98_ec4e_6c89);
    let mut repeated = distant.clone();
    repeated.extend_from_slice(&distant);
    let small = deterministic_bytes(64 * 1_024, 0x4528_21e6_38d0_1377);

    let random_chunks = random.chunks(64 * 1_024).collect::<Vec<_>>();
    let text_chunks = text.chunks(64 * 1_024).collect::<Vec<_>>();
    let repeated_chunks = repeated.chunks(64 * 1_024).collect::<Vec<_>>();
    let small_chunks = small.chunks(64 * 1_024).collect::<Vec<_>>();
    let regions = [
        random_chunks.as_slice(),
        text_chunks.as_slice(),
        repeated_chunks.as_slice(),
        small_chunks.as_slice(),
    ];
    let id = ContainerId::new([0x94; 16]).expect("fixture Container ID is nonzero");

    let serial = SealedContainer::encode_adaptive_regions_parallel_profiled(
        id,
        4,
        &regions,
        NonZeroUsize::MIN,
    )
    .expect("serial adaptive encoding succeeds");
    let parallel = SealedContainer::encode_adaptive_regions_parallel_profiled(
        id,
        4,
        &regions,
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
    )
    .expect("parallel adaptive encoding succeeds");

    assert_eq!(parallel.bytes(), serial.bytes());
    assert_eq!(parallel.metrics(), serial.metrics());
    assert_eq!(parallel.metrics().eligible_regions(), 3);
    assert_eq!(parallel.metrics().size_bypassed_regions(), 1);
    assert_eq!(parallel.metrics().raw_regions_after_gate(), 1);
    assert_eq!(parallel.metrics().target_zstd_trials(), 3);
}

#[test]
fn disabled_policy_is_an_explicit_equivalent_output_baseline() {
    let input = deterministic_bytes(512 * 1_024, 0xbe54_66cf_34e9_0c6c);
    let chunks = input.chunks(64 * 1_024).collect::<Vec<_>>();
    let regions = [chunks.as_slice()];
    let id = ContainerId::new([0x95; 16]).expect("fixture Container ID is nonzero");

    let gated = SealedContainer::encode_adaptive_regions_parallel_profiled_with_gate(
        id,
        5,
        &regions,
        NonZeroUsize::MIN,
        IncompressibilityGatePolicy::V1,
    )
    .expect("gated adaptive encoding succeeds");
    let baseline = SealedContainer::encode_adaptive_regions_parallel_profiled_with_gate(
        id,
        5,
        &regions,
        NonZeroUsize::MIN,
        IncompressibilityGatePolicy::Off,
    )
    .expect("ungated adaptive encoding succeeds");

    assert_eq!(gated.bytes(), baseline.bytes());
    assert_eq!(gated.metrics().target_zstd_trials(), 0);
    assert_eq!(gated.metrics().raw_regions_after_gate(), 1);
    assert_eq!(baseline.metrics().disabled_regions(), 1);
    assert_eq!(baseline.metrics().target_zstd_trials(), 1);
    assert_eq!(baseline.metrics().target_zstd_rejected(), 1);
    assert_eq!(baseline.metrics().raw_regions_after_gate(), 0);
}

fn deterministic_bytes(length: usize, mut state: u64) -> Vec<u8> {
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}
