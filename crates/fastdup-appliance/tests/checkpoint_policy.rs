use fastdup_appliance::{checkpoint_policy_set_v1, checkpoint_policy_set_v2};

#[test]
fn advanced_reduction_has_a_distinct_stable_writer_policy_identity() {
    let baseline = checkpoint_policy_set_v1();
    let advanced = checkpoint_policy_set_v2();

    assert_ne!(advanced, baseline);
    assert_eq!(
        advanced.bytes(),
        [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ],
        "the advanced writer policy identity is part of the durable contract",
    );
}
