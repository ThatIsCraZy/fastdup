use std::time::Duration;

use fastdup_appliance::{
    CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, CheckpointAction, CheckpointPressure, CheckpointTrigger,
    checkpoint_action,
};

#[test]
fn v1_size_trigger_is_eight_sixty_four_mib_containers() {
    assert_eq!(CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, 536_870_912);
}

#[test]
fn trigger_policy_coalesces_one_container_but_bounds_age_and_backlog() {
    let action = |mutation_ms: Option<u64>, container_ms: Option<u64>, containers: u64| {
        checkpoint_action(CheckpointPressure {
            oldest_mutation_age: mutation_ms.map(Duration::from_millis),
            oldest_sealed_container_age: container_ms.map(Duration::from_millis),
            sealed_uncommitted_containers: containers,
        })
    };

    assert_eq!(
        action(Some(100), Some(499), 1),
        CheckpointAction::Wait(Duration::from_millis(1))
    );
    assert_eq!(
        action(Some(100), Some(500), 1),
        CheckpointAction::Commit(CheckpointTrigger::ContainerCoalesce)
    );
    assert_eq!(
        action(Some(100), Some(1), 8),
        CheckpointAction::Commit(CheckpointTrigger::SealedContainerLimit)
    );
    assert_eq!(
        action(Some(2_000), None, 0),
        CheckpointAction::Commit(CheckpointTrigger::MutationAge)
    );
    assert_eq!(
        action(Some(5_000), Some(500), 8),
        CheckpointAction::PauseAndCommit(CheckpointTrigger::AdmissionGuard)
    );
}
