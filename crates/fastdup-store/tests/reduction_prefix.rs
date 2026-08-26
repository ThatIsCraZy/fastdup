use fastdup_store::{VerifiedBaseChunk, ZstdPrefixCodec, ZstdPrefixError};

#[test]
fn zstd_prefix_round_trips_one_verified_depth_one_dependency() {
    let base_bytes = patterned(128 * 1_024, 0);
    let mut target = base_bytes.clone();
    target[4_096..4_224].fill(0xa5);
    target[96_000..96_064].fill(0x3c);
    let base = VerifiedBaseChunk::from_bytes(&base_bytes).expect("base is valid");

    let trial = ZstdPrefixCodec::encode_trial(base, &target, target.len())
        .expect("trial succeeds")
        .expect("similar target fits its useful cap");

    assert_eq!(trial.encoding().base(), base.reference());
    assert_eq!(
        trial.encoding().logical_length(),
        u32::try_from(target.len()).expect("fixture length fits u32")
    );
    assert_eq!(trial.encoding().level(), 3);
    assert_eq!(
        usize::try_from(trial.encoded_payload_bytes()).unwrap(),
        32 + trial.encoding().frame().len()
    );
    assert!(usize::try_from(trial.encoded_payload_bytes()).unwrap() < target.len());
    assert_eq!(trial.encoding().decode(base).unwrap(), target);
}

#[test]
fn prefix_output_is_deterministic_and_the_cost_cap_avoids_oversized_work() {
    let base_bytes = patterned(64 * 1_024, 11);
    let mut target = base_bytes.clone();
    target[32_000..32_128].reverse();
    let base = VerifiedBaseChunk::from_bytes(&base_bytes).expect("base is valid");

    let first = ZstdPrefixCodec::encode_trial(base, &target, target.len())
        .expect("first trial succeeds")
        .expect("first trial fits");
    let second = ZstdPrefixCodec::encode_trial(base, &target, target.len())
        .expect("second trial succeeds")
        .expect("second trial fits");
    assert_eq!(first, second);

    assert_eq!(
        ZstdPrefixCodec::encode_trial(base, &target, 32).expect("small cap is fallback"),
        None
    );
    let below = usize::try_from(first.encoded_payload_bytes()).unwrap() - 1;
    assert_eq!(
        ZstdPrefixCodec::encode_trial(base, &target, below).expect("tight cap is fallback"),
        None
    );
}

#[test]
fn wrong_base_and_unsafe_lengths_fail_before_decode_or_allocation() {
    assert!(matches!(
        VerifiedBaseChunk::from_bytes(&[]),
        Err(ZstdPrefixError::EmptyChunk)
    ));
    assert!(matches!(
        VerifiedBaseChunk::from_bytes(&vec![0; 256 * 1_024 + 1]),
        Err(ZstdPrefixError::ChunkTooLarge)
    ));

    let base_bytes = patterned(32 * 1_024, 1);
    let other_bytes = patterned(32 * 1_024, 2);
    let target = patterned(32 * 1_024, 3);
    let base = VerifiedBaseChunk::from_bytes(&base_bytes).expect("base is valid");
    let other = VerifiedBaseChunk::from_bytes(&other_bytes).expect("other base is valid");
    let encoding = ZstdPrefixCodec::encode_trial(base, &target, target.len())
        .expect("trial succeeds")
        .expect("trial fits")
        .into_encoding();

    assert_eq!(
        encoding.decode(other),
        Err(ZstdPrefixError::BaseIdentityMismatch)
    );
    assert!(matches!(
        VerifiedBaseChunk::from_expected(base.reference(), &other_bytes),
        Err(ZstdPrefixError::BaseIdentityMismatch)
    ));
    assert_eq!(
        ZstdPrefixCodec::encode_trial(base, b"short", target.len()),
        Err(ZstdPrefixError::TargetLengthMismatch)
    );
}

fn patterned(length: usize, generation: u8) -> Vec<u8> {
    (0..length)
        .map(|offset| {
            let lane = u8::try_from(offset % 251).expect("fixture lane fits u8");
            lane.wrapping_add(generation)
        })
        .collect()
}
