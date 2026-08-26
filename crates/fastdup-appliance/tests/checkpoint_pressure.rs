use std::time::Duration;

use fastdup_appliance::{
    CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, CheckpointAction, CheckpointPressure,
    CheckpointProgressAction, CheckpointTrigger, DurabilityObservation, DurabilitySupervisor,
    checkpoint_action,
};

#[test]
fn v1_size_trigger_is_eight_sixty_four_mib_containers() {
    assert_eq!(CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, 536_870_912);
}

#[test]
fn fake_clock_marks_a_running_checkpoint_stalled_at_the_admission_guard() {
    assert_eq!(
        DurabilitySupervisor::checkpoint_progress(Duration::from_millis(4_999)),
        CheckpointProgressAction::Continue
    );
    assert_eq!(
        DurabilitySupervisor::checkpoint_progress(Duration::from_secs(5)),
        CheckpointProgressAction::CloseAdmission
    );
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

#[test]
fn fake_clock_closes_admission_at_five_seconds_without_touching_the_write_path() {
    let mut supervisor = DurabilitySupervisor::new(Duration::ZERO);
    let dirty = DurabilityObservation {
        has_checkpointable_dirty_payload: true,
        oldest_sealed_container_age: None,
        sealed_uncommitted_containers: 0,
    };

    assert_eq!(
        supervisor.observe(Duration::from_secs(1), dirty),
        CheckpointAction::Wait(Duration::from_secs(2))
    );
    assert_eq!(
        supervisor.observe(Duration::from_secs(3), dirty),
        CheckpointAction::Commit(CheckpointTrigger::MutationAge)
    );
    assert_eq!(
        supervisor.observe(Duration::from_secs(6), dirty),
        CheckpointAction::PauseAndCommit(CheckpointTrigger::AdmissionGuard)
    );

    supervisor.record_checkpoint_attempt(Duration::from_secs(6), false);
    assert_eq!(
        supervisor.observe(
            Duration::from_secs(6),
            DurabilityObservation {
                has_checkpointable_dirty_payload: false,
                ..dirty
            }
        ),
        CheckpointAction::Wait(Duration::from_secs(2))
    );
}
