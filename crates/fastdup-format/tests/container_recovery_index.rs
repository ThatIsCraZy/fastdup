use fastdup_format::{
    ChunkId, ContainerId, ContainerRecoveryEnvelope, FOOTER_BYTES, FormatError, HEADER_BYTES,
    SealedContainer,
};

#[test]
fn paired_envelope_locates_and_verifies_one_independent_base_record() {
    let base = deterministic_bytes(64 * 1_024, 17);
    let image = SealedContainer::encode(
        ContainerId::new([0x31; 16]).expect("fixture Container ID is nonzero"),
        7,
        &[base.as_slice()],
    )
    .expect("independent Container encodes");
    let footer_bytes = usize::try_from(FOOTER_BYTES).expect("footer length fits memory");
    let envelope = ContainerRecoveryEnvelope::decode(
        &image[..HEADER_BYTES],
        &image[image.len() - footer_bytes..],
        u64::try_from(image.len()).expect("fixture length fits u64"),
    )
    .expect("paired recovery envelope verifies");

    let index_range = envelope
        .recovery_index_range()
        .expect("validated index range fits memory");
    let index_start = usize::try_from(index_range.offset()).expect("index offset fits memory");
    let index = envelope
        .verify_recovery_index(&image[index_start..index_start + index_range.length()])
        .expect("standalone Recovery Index verifies");
    let candidate = index
        .find_independent_candidate(
            ChunkId::of(&base),
            u32::try_from(base.len()).expect("fixture length fits u32"),
        )
        .expect("Recovery Index contains the independent Base");
    let record_range = candidate
        .record_range()
        .expect("validated record range fits memory");
    let record_start = usize::try_from(record_range.offset()).expect("record offset fits memory");
    let decoded = index
        .decode_independent_candidate(
            candidate,
            &image[record_start..record_start + record_range.length()],
        )
        .expect("selected independent record verifies");

    assert_eq!(decoded.payload(), base);
}

#[test]
fn standalone_recovery_index_rejects_crc_corruption() {
    let image = SealedContainer::encode(
        ContainerId::new([0x41; 16]).expect("fixture Container ID is nonzero"),
        8,
        &[b"first independent payload"],
    )
    .expect("Container encodes");
    let footer_bytes = usize::try_from(FOOTER_BYTES).expect("footer length fits memory");
    let envelope = ContainerRecoveryEnvelope::decode(
        &image[..HEADER_BYTES],
        &image[image.len() - footer_bytes..],
        u64::try_from(image.len()).expect("fixture length fits u64"),
    )
    .expect("envelope verifies");
    let range = envelope
        .recovery_index_range()
        .expect("validated index range fits memory");
    let start = usize::try_from(range.offset()).expect("index offset fits memory");
    let mut index = image[start..start + range.length()].to_vec();
    index[64] ^= 1;

    assert_eq!(
        envelope.verify_recovery_index(&index),
        Err(FormatError::IndexChecksumMismatch)
    );
}

fn deterministic_bytes(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}
