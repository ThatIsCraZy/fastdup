use fastdup_format::{
    MANIFEST_CHILD_RANGE_BYTES, MANIFEST_INNER_HEADER_BYTES, METADATA_HEADER_BYTES,
    ManifestChildRange, ManifestInnerNode, ManifestInnerNodeError, MetadataObjectId,
    MetadataObjectKind, metadata_object_kind,
};

fn object_id(byte: u8) -> MetadataObjectId {
    MetadataObjectId::new([byte; 32]).expect("fixture object ID is nonzero")
}

#[test]
fn manifest_inner_node_has_stable_content_addressed_bytes_and_round_trips() {
    let node = ManifestInnerNode::new(
        12,
        1,
        vec![
            ManifestChildRange::new(0, 4, object_id(0x22)).expect("first child range is valid"),
            ManifestChildRange::new(4, 8, object_id(0x88)).expect("second child range is valid"),
        ],
    )
    .expect("worked child partition is valid");

    let encoded = node.encode().expect("bounded inner node must encode");

    assert_eq!(&encoded[0..8], b"FDMDOBJ1");
    assert_eq!(
        metadata_object_kind(&encoded).expect("generic envelope kind verifies"),
        MetadataObjectKind::ManifestInnerNode
    );
    assert_eq!(&encoded[12..14], &4_u16.to_le_bytes());
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES..METADATA_HEADER_BYTES + 8],
        b"FDMANI01"
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 10..METADATA_HEADER_BYTES + 12],
        &u16::try_from(MANIFEST_INNER_HEADER_BYTES)
            .expect("header length fits u16")
            .to_le_bytes()
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 14..METADATA_HEADER_BYTES + 16],
        &1_u16.to_le_bytes()
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 24..METADATA_HEADER_BYTES + 32],
        &12_u64.to_le_bytes()
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 32..METADATA_HEADER_BYTES + 36],
        &2_u32.to_le_bytes()
    );
    let first_child = METADATA_HEADER_BYTES + MANIFEST_INNER_HEADER_BYTES;
    assert_eq!(&encoded[first_child..first_child + 8], &0_u64.to_le_bytes());
    assert_eq!(
        &encoded[first_child + 8..first_child + 16],
        &4_u64.to_le_bytes()
    );
    assert_eq!(&encoded[first_child + 16..first_child + 48], &[0x22; 32]);
    let second_child = first_child + 64;
    assert_eq!(
        &encoded[second_child..second_child + 8],
        &4_u64.to_le_bytes()
    );
    assert_eq!(node.file_length(), 12);
    assert_eq!(node.level(), 1);
    assert_eq!(node.children().len(), 2);
    assert_eq!(
        MetadataObjectId::from_encoded(&encoded).expect("generic envelope identity verifies"),
        MetadataObjectId::from_encoded(
            &ManifestInnerNode::decode(&encoded)
                .expect("worked node decodes")
                .encode()
                .expect("decoded node re-encodes")
        )
        .expect("re-encoded identity verifies")
    );
    assert_eq!(ManifestInnerNode::decode(&encoded), Ok(node));
}

#[test]
fn v2_child_allocation_summaries_are_stable_bounded_and_round_trip() {
    let node = ManifestInnerNode::new_with_allocated_bytes(
        12,
        1,
        vec![
            ManifestChildRange::new_with_allocated_bytes(0, 4, 1, object_id(0x22))
                .expect("sparse child summary is valid"),
            ManifestChildRange::new_with_allocated_bytes(4, 8, 8, object_id(0x88))
                .expect("allocated child summary is valid"),
        ],
    )
    .expect("worked v2 child partition is valid");

    let encoded = node.encode().expect("bounded v2 inner node encodes");
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 8..METADATA_HEADER_BYTES + 10],
        &2_u16.to_le_bytes()
    );
    let first_child = METADATA_HEADER_BYTES + MANIFEST_INNER_HEADER_BYTES;
    assert_eq!(
        &encoded[first_child + 48..first_child + 56],
        &1_u64.to_le_bytes()
    );
    assert_eq!(
        &encoded[first_child + MANIFEST_CHILD_RANGE_BYTES + 48
            ..first_child + MANIFEST_CHILD_RANGE_BYTES + 56],
        &8_u64.to_le_bytes()
    );
    assert_eq!(node.allocated_bytes(), Ok(Some(9)));
    assert_eq!(ManifestInnerNode::decode(&encoded), Ok(node));

    assert_eq!(
        ManifestChildRange::new_with_allocated_bytes(0, 4, 5, object_id(1)),
        Err(ManifestInnerNodeError::InvalidChildRange)
    );
}

#[test]
fn writer_rejects_leaf_level_empty_gapped_overlapping_and_overflowing_ranges() {
    let child = |offset, length, byte| ManifestChildRange::new(offset, length, object_id(byte));
    let complete = || vec![child(0, 4, 1).expect("worked child range is valid")];

    assert_eq!(
        ManifestInnerNode::new(4, 0, complete()),
        Err(ManifestInnerNodeError::InvalidLevel)
    );
    assert_eq!(
        ManifestInnerNode::new(0, 1, Vec::new()),
        Err(ManifestInnerNodeError::InvalidPartition)
    );
    assert_eq!(
        child(0, 0, 1),
        Err(ManifestInnerNodeError::InvalidChildRange)
    );
    assert_eq!(
        child(u64::MAX, 1, 1),
        Err(ManifestInnerNodeError::ArithmeticOverflow)
    );
    assert_eq!(
        ManifestInnerNode::new(
            8,
            1,
            vec![
                child(0, 4, 1).expect("first range is valid"),
                child(5, 3, 2).expect("gapped range is locally valid"),
            ],
        ),
        Err(ManifestInnerNodeError::InvalidPartition)
    );
    assert_eq!(
        ManifestInnerNode::new(
            8,
            1,
            vec![
                child(0, 5, 1).expect("first range is valid"),
                child(4, 4, 2).expect("overlapping range is locally valid"),
            ],
        ),
        Err(ManifestInnerNodeError::InvalidPartition)
    );
    assert_eq!(
        ManifestInnerNode::new(5, 1, complete()),
        Err(ManifestInnerNodeError::InvalidPartition)
    );
}

#[test]
fn decoder_rejects_reauthenticated_noncanonical_child_order() {
    let node = worked_node();
    let mut encoded = node.encode().expect("worked node encodes");
    let first = METADATA_HEADER_BYTES + MANIFEST_INNER_HEADER_BYTES;
    let second = first + MANIFEST_CHILD_RANGE_BYTES;
    for offset in 0..MANIFEST_CHILD_RANGE_BYTES {
        encoded.swap(first + offset, second + offset);
    }
    reauthenticate_metadata_object(&mut encoded);

    assert_eq!(
        ManifestInnerNode::decode(&encoded),
        Err(ManifestInnerNodeError::InvalidPartition)
    );
}

#[test]
fn impossible_child_count_is_rejected_before_allocation_or_panic() {
    let mut encoded = worked_node().encode().expect("worked node encodes");
    let count_offset = METADATA_HEADER_BYTES + 32;
    encoded[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    reauthenticate_metadata_object(&mut encoded);

    let result = std::panic::catch_unwind(|| ManifestInnerNode::decode(&encoded));
    assert!(
        matches!(
            result,
            Ok(Err(
                ManifestInnerNodeError::InvalidPayload | ManifestInnerNodeError::ArithmeticOverflow
            ))
        ),
        "attacker-selected count must fail before reserving its requested memory"
    );
}

#[test]
fn every_truncated_prefix_and_single_byte_mutation_is_rejected_without_panicking() {
    let encoded = worked_node().encode().expect("worked node encodes");

    for prefix_length in 0..encoded.len() {
        let result =
            std::panic::catch_unwind(|| ManifestInnerNode::decode(&encoded[..prefix_length]));
        assert!(result.is_ok(), "decoder panicked at prefix {prefix_length}");
        assert!(
            result.expect("panic checked").is_err(),
            "decoder accepted truncated prefix {prefix_length}"
        );
    }
    for offset in 0..encoded.len() {
        let mut mutated = encoded.clone();
        mutated[offset] ^= 1;
        let result = std::panic::catch_unwind(|| ManifestInnerNode::decode(&mutated));
        assert!(
            result.is_ok(),
            "decoder panicked after mutation at {offset}"
        );
        assert!(
            result.expect("panic checked").is_err(),
            "decoder accepted mutation at byte {offset}"
        );
    }
}

fn worked_node() -> ManifestInnerNode {
    ManifestInnerNode::new(
        12,
        2,
        vec![
            ManifestChildRange::new(0, 4, object_id(0x33)).expect("first child range is valid"),
            ManifestChildRange::new(4, 8, object_id(0x77)).expect("second child range is valid"),
        ],
    )
    .expect("worked child partition is valid")
}

fn reauthenticate_metadata_object(encoded: &mut [u8]) {
    let payload_length = usize::try_from(u64::from_le_bytes(
        encoded[32..40].try_into().expect("fixed payload length"),
    ))
    .expect("fixture payload length fits");
    let kind = u16::from_le_bytes(encoded[12..14].try_into().expect("fixed kind"));
    let (payload_crc, object_id) = {
        let payload = &encoded[METADATA_HEADER_BYTES..METADATA_HEADER_BYTES + payload_length];
        let payload_crc = crc32c::crc32c(payload);
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"fastdup-metadata-object-v1\0");
        hasher.update(&kind.to_le_bytes());
        hasher.update(
            &u64::try_from(payload_length)
                .expect("fixture payload length fits")
                .to_le_bytes(),
        );
        hasher.update(payload);
        (payload_crc, *hasher.finalize().as_bytes())
    };
    encoded[80..84].copy_from_slice(&payload_crc.to_le_bytes());
    encoded[48..80].copy_from_slice(&object_id);

    encoded[84..88].fill(0);
    let header_crc = crc32c::crc32c(&encoded[..METADATA_HEADER_BYTES]);
    encoded[84..88].copy_from_slice(&header_crc.to_le_bytes());
}
