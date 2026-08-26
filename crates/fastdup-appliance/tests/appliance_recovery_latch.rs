use fastdup_appliance::{ApplianceRecoveryLatch, ApplianceRecoveryState};
use fastdup_testkit::MemoryStorageIo;

#[test]
fn recovery_latch_survives_process_loss_and_only_clean_completion_clears_it() {
    let metadata = MemoryStorageIo::new();
    assert_eq!(
        ApplianceRecoveryLatch::audit(&metadata).expect("audit empty repository"),
        ApplianceRecoveryState::Clean
    );

    let first = ApplianceRecoveryLatch::arm(metadata.clone()).expect("arm first owner");
    assert!(!first.prior_recovery_required());
    metadata.crash();
    assert_eq!(
        ApplianceRecoveryLatch::audit(&metadata).expect("audit after process loss"),
        ApplianceRecoveryState::RecoveryRequired
    );
    drop(first);

    let recovered = ApplianceRecoveryLatch::arm(metadata.clone()).expect("arm recovered owner");
    assert!(recovered.prior_recovery_required());
    recovered
        .mark_clean()
        .expect("successful recovery owner clears the latch");
    metadata.crash();
    assert_eq!(
        ApplianceRecoveryLatch::audit(&metadata).expect("audit clean completion"),
        ApplianceRecoveryState::Clean
    );
}

#[test]
fn every_latch_arm_fault_recovers_to_clean_or_recovery_required() {
    let baseline = MemoryStorageIo::new();
    ApplianceRecoveryLatch::arm(baseline.clone()).expect("measure successful arm protocol");
    let operation_count = baseline.operation_count();

    for fail_after_effect in [false, true] {
        for position in 0..operation_count {
            let metadata = if fail_after_effect {
                MemoryStorageIo::with_fail_after(position)
            } else {
                MemoryStorageIo::with_fail_before(position)
            };
            let result = ApplianceRecoveryLatch::arm(metadata.clone());
            metadata.crash();
            let recovered = ApplianceRecoveryLatch::audit(&metadata)
                .expect("arm interruption never leaves a malformed durable latch");
            assert!(matches!(
                recovered,
                ApplianceRecoveryState::Clean | ApplianceRecoveryState::RecoveryRequired
            ));
            if result.is_ok() {
                assert_eq!(recovered, ApplianceRecoveryState::RecoveryRequired);
            }
        }
    }
}

#[test]
fn every_latch_clear_fault_recovers_to_armed_or_clean() {
    let baseline = MemoryStorageIo::new();
    let latch = ApplianceRecoveryLatch::arm(baseline.clone()).expect("measure successful arm");
    let arm_operations = baseline.operation_count();
    latch.mark_clean().expect("measure successful clear");
    let clear_operations = baseline.operation_count() - arm_operations;

    for fail_after_effect in [false, true] {
        for relative in 0..clear_operations {
            let position = arm_operations + relative;
            let metadata = if fail_after_effect {
                MemoryStorageIo::with_fail_after(position)
            } else {
                MemoryStorageIo::with_fail_before(position)
            };
            let latch = ApplianceRecoveryLatch::arm(metadata.clone())
                .expect("fault position follows complete arm protocol");
            let result = latch.mark_clean();
            metadata.crash();
            let recovered = ApplianceRecoveryLatch::audit(&metadata)
                .expect("clear interruption never leaves a malformed durable latch");
            assert!(matches!(
                recovered,
                ApplianceRecoveryState::Clean | ApplianceRecoveryState::RecoveryRequired
            ));
            if result.is_ok() {
                assert_eq!(recovered, ApplianceRecoveryState::Clean);
            }
        }
    }
}
