use fastdup_format::{
    EXACT_INDEX_ACTIVATION_RECORD_BYTES, ExactIndexActivationHash, ExactIndexActivationRecord,
    ExactIndexProfileId, ExactIndexRunSet,
};

#[test]
fn activation_records_have_stable_bytes_and_form_one_hash_chain() {
    let profile = ExactIndexProfileId::new([0x39; 32]).expect("profile identity is nonzero");
    let first_set = ExactIndexRunSet::new(profile, 3, Vec::new())
        .expect("empty initial Run Set is valid")
        .id()
        .expect("initial Run Set has a content identity");
    let first =
        ExactIndexActivationRecord::new(1, ExactIndexActivationHash::ZERO, first_set, profile, 3)
            .expect("first activation starts the chain");
    let first_bytes = first.encode();
    let first_hash = ExactIndexActivationHash::of(&first_bytes);

    let second_set = ExactIndexRunSet::new(profile, 4, Vec::new())
        .expect("replacement Run Set is valid")
        .id()
        .expect("replacement Run Set has a content identity");
    let second = ExactIndexActivationRecord::new(2, first_hash, second_set, profile, 4)
        .expect("second activation extends the chain");
    let second_bytes = second.encode();

    assert_eq!(first_bytes.len(), EXACT_INDEX_ACTIVATION_RECORD_BYTES);
    assert_eq!(&first_bytes[0..8], b"FDXACT01");
    assert_eq!(&first_bytes[40..48], &1_u64.to_le_bytes());
    assert_eq!(&first_bytes[48..80], &[0; 32]);
    assert_eq!(&second_bytes[48..80], &first_hash.bytes());
    assert_eq!(ExactIndexActivationRecord::decode(&first_bytes), Ok(first));
    assert_eq!(
        ExactIndexActivationRecord::decode(&second_bytes),
        Ok(second)
    );
}

#[test]
fn every_truncated_or_single_byte_corrupt_activation_is_rejected_without_panicking() {
    let profile = ExactIndexProfileId::new([0x49; 32]).expect("profile identity is nonzero");
    let run_set_id = ExactIndexRunSet::new(profile, 1, Vec::new())
        .expect("empty Run Set is valid")
        .id()
        .expect("Run Set has a content identity");
    let encoded =
        ExactIndexActivationRecord::new(1, ExactIndexActivationHash::ZERO, run_set_id, profile, 1)
            .expect("first activation is valid")
            .encode();

    for prefix_length in 0..encoded.len() {
        let result = std::panic::catch_unwind(|| {
            ExactIndexActivationRecord::decode(&encoded[..prefix_length])
        });
        assert!(result.is_ok(), "decoder panicked at prefix {prefix_length}");
        assert!(
            result.expect("panic checked").is_err(),
            "decoder accepted truncated prefix {prefix_length}"
        );
    }
    for offset in 0..encoded.len() {
        let mut corrupted = encoded;
        corrupted[offset] ^= 1;
        assert!(
            ExactIndexActivationRecord::decode(&corrupted).is_err(),
            "decoder accepted corruption at byte {offset}"
        );
    }
}
