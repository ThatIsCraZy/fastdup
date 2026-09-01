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
            0xef, 0x86, 0x0a, 0xf9, 0xdf, 0x84, 0xba, 0x6c, 0x4b, 0x1a, 0x23, 0x1b, 0xff, 0x2e,
            0xbb, 0x89, 0x5b, 0x04, 0xeb, 0xfa, 0x94, 0x14, 0xf4, 0x39, 0x94, 0x08, 0x5b, 0x3a,
            0xc1, 0x73, 0x61, 0xe0,
        ],
        "the only current writer policy identity is part of the durable contract",
    );
}
