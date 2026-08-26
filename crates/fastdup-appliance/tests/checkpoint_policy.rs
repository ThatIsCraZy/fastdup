use fastdup_appliance::{checkpoint_policy_set_v1, checkpoint_policy_set_v2};

#[test]
fn advanced_reduction_has_a_distinct_stable_writer_policy_identity() {
    let baseline = checkpoint_policy_set_v1();
    let advanced = checkpoint_policy_set_v2();

    assert_ne!(advanced, baseline);
    assert_eq!(
        advanced.bytes(),
        [
            0x74, 0xa2, 0x9f, 0xc2, 0x27, 0x73, 0x6d, 0xbc,
            0xcb, 0xaf, 0xd9, 0x10, 0x96, 0x87, 0x44, 0xab,
            0x67, 0x69, 0xb2, 0xc6, 0x5a, 0x1b, 0xec, 0x84,
            0x00, 0xcf, 0xbd, 0x0c, 0x6b, 0xd5, 0x69, 0x17,
        ],
        "the advanced writer policy identity is part of the durable contract",
    );
}
