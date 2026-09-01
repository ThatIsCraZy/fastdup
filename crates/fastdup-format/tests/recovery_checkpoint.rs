use fastdup_format::{
    CommitRecord, CommitRecordHash, MetadataObjectId, PolicySetId, RecoveryCheckpointDescriptor,
    RecoveryCheckpointEntryHeader, RecoveryCheckpointHeadRecord,
};

fn record(generation: u64) -> CommitRecord {
    CommitRecord::new(
        generation,
        if generation == 1 {
            CommitRecordHash::ZERO
        } else {
            CommitRecordHash::of(b"previous checkpoint fixture")
        },
        MetadataObjectId::new([0x91; 32]).expect("namespace identity is nonzero"),
        PolicySetId::new([0x92; 32]).expect("policy identity is nonzero"),
        7,
        1_024,
        9,
    )
    .expect("fixture Commit is valid")
}

#[test]
fn current_checkpoint_descriptors_entries_and_head_chain_round_trip() {
    let first_descriptor = RecoveryCheckpointDescriptor::new(record(7), 2, 20_480, [0x93; 32])
        .expect("first descriptor is valid");
    let first_head =
        RecoveryCheckpointHeadRecord::new(first_descriptor, None).expect("first head is valid");
    let second_descriptor = RecoveryCheckpointDescriptor::new(record(8), 3, 24_576, [0x94; 32])
        .expect("second descriptor is valid");
    let second_head = RecoveryCheckpointHeadRecord::new(second_descriptor, Some(first_head))
        .expect("successor head is valid");
    let entry = RecoveryCheckpointEntryHeader::new(
        MetadataObjectId::new([0x95; 32]).expect("object identity is nonzero"),
        4_097,
        0x1234_5678,
    )
    .expect("entry is valid");

    assert_eq!(
        RecoveryCheckpointDescriptor::decode_header(&first_descriptor.encode_header())
            .expect("header round trips"),
        first_descriptor
    );
    assert_eq!(
        RecoveryCheckpointDescriptor::decode_footer(&first_descriptor.encode_footer())
            .expect("footer round trips"),
        first_descriptor
    );
    assert_eq!(
        RecoveryCheckpointEntryHeader::decode(&entry.encode()).expect("entry round trips"),
        entry
    );
    assert_eq!(
        RecoveryCheckpointHeadRecord::decode(&second_head.encode()).expect("head round trips"),
        second_head
    );
    assert_eq!(second_head.previous_generation(), first_head.generation());
    assert_eq!(second_head.previous_record_hash(), first_head.record_hash());
}

#[test]
fn authenticated_checkpoint_fields_reject_single_byte_corruption() {
    let descriptor = RecoveryCheckpointDescriptor::new(record(7), 2, 20_480, [0x96; 32])
        .expect("descriptor is valid");
    let mut header = descriptor.encode_header();
    header[104] ^= 0x01;
    assert!(RecoveryCheckpointDescriptor::decode_header(&header).is_err());

    let head = RecoveryCheckpointHeadRecord::new(descriptor, None).expect("head is valid");
    let mut encoded_head = head.encode();
    encoded_head[32] ^= 0x01;
    assert!(RecoveryCheckpointHeadRecord::decode(&encoded_head).is_err());
}
