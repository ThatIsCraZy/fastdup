use fastdup_appliance::CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1;

#[test]
fn v1_size_trigger_is_eight_sixty_four_mib_containers() {
    assert_eq!(CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, 536_870_912);
}
