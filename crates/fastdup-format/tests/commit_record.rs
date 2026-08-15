use fastdup_format::{
    COMMIT_RECORD_BYTES, CommitRecord, CommitRecordHash, MetadataObjectId, PolicySetId,
};

#[test]
fn commit_records_have_stable_bytes_and_form_a_hash_chain() {
    let first = CommitRecord::new(
        1,
        CommitRecordHash::ZERO,
        MetadataObjectId::new([0x11; 32]).expect("nonzero namespace root"),
        PolicySetId::new([0xa1; 32]).expect("nonzero policy set"),
        7,
        1_024,
        10,
    )
    .expect("first generation must be valid");
    let first_bytes = first.encode();

    assert_eq!(first_bytes.len(), COMMIT_RECORD_BYTES);
    assert_eq!(&first_bytes[0..8], b"FDCMIT01");
    assert_eq!(&first_bytes[8..10], &1_u16.to_le_bytes());
    assert_eq!(&first_bytes[40..48], &1_u64.to_le_bytes());
    assert_eq!(&first_bytes[152..160], &1_024_u64.to_le_bytes());
    assert_eq!(&first_bytes[160..168], &10_u64.to_le_bytes());
    assert_eq!(first.inode_reservation_end(), 1_024);
    assert_eq!(first.inode_allocation_cursor(), 10);
    assert_eq!(CommitRecord::decode(&first_bytes), Ok(first));

    let second = CommitRecord::new(
        2,
        CommitRecordHash::of(&first_bytes),
        MetadataObjectId::new([0x22; 32]).expect("nonzero namespace root"),
        PolicySetId::new([0xa1; 32]).expect("nonzero policy set"),
        9,
        2_048,
        11,
    )
    .expect("second generation must chain to the first");
    let second_bytes = second.encode();
    assert_eq!(
        &second_bytes[48..80],
        &CommitRecordHash::of(&first_bytes).bytes()
    );
    assert_eq!(CommitRecord::decode(&second_bytes), Ok(second));
}
