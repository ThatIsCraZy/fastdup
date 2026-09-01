use fastdup_appliance::{INODE_RESERVATION_SPAN_V1, checkpoint_policy_set};

#[test]
fn production_inode_reservation_outlives_small_file_bursts() {
    assert_eq!(INODE_RESERVATION_SPAN_V1, 1_u64 << 32);
}

#[test]
fn every_new_repository_uses_one_stable_writer_policy_identity() {
    let current = checkpoint_policy_set();
    assert_eq!(
        current.bytes(),
        [
            0x74, 0xa2, 0x9f, 0xc2, 0x27, 0x73, 0x6d, 0xbc, 0xcb, 0xaf, 0xd9, 0x10, 0x96, 0x87,
            0x44, 0xab, 0x67, 0x69, 0xb2, 0xc6, 0x5a, 0x1b, 0xec, 0x84, 0x00, 0xcf, 0xbd, 0x0c,
            0x6b, 0xd5, 0x69, 0x17,
        ],
        "the only current writer policy identity is part of the durable contract",
    );
}
