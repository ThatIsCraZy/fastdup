use std::path::PathBuf;

use fastdup_testkit::{RandomizedSigkillConfig, SigkillRemountConfig};

#[test]
#[ignore = "requires /dev/fuse, mount permission, and FASTDUP_DAEMON_BIN/FASTDUP_SIGKILL_RUN_ROOT"]
fn real_sigkill_remount_matrix_recovers_a_complete_prefix_and_meets_the_deadline() {
    let daemon = PathBuf::from(
        std::env::var_os("FASTDUP_DAEMON_BIN")
            .expect("FASTDUP_DAEMON_BIN names the built fastdup-durable-fuse binary"),
    );
    let run_root = PathBuf::from(
        std::env::var_os("FASTDUP_SIGKILL_RUN_ROOT")
            .expect("FASTDUP_SIGKILL_RUN_ROOT names a new workspace-local directory"),
    );

    let report = SigkillRemountConfig::v1(daemon, run_root)
        .run()
        .expect("the real SIGKILL/remount matrix satisfies every recovery oracle");

    assert!(report.cases().len() >= 7);
    assert!(report.cases().iter().any(|case| !case.deadline_required()));
    assert!(report.cases().iter().any(|case| case.deadline_required()));
    for case in report.cases() {
        assert!(case.recovered_records() <= case.acknowledged_records());
        if case.deadline_required() {
            assert_eq!(case.recovered_records(), case.acknowledged_records());
        }
    }
}

#[test]
#[ignore = "requires /dev/fuse, mount permission, and FASTDUP_DAEMON_BIN/FASTDUP_RANDOM_SIGKILL_RUN_ROOT"]
fn randomized_real_sigkill_soak_recovers_only_acknowledged_namespace_prefixes() {
    let daemon = PathBuf::from(
        std::env::var_os("FASTDUP_DAEMON_BIN")
            .expect("FASTDUP_DAEMON_BIN names the built fastdup-durable-fuse binary"),
    );
    let run_root = PathBuf::from(
        std::env::var_os("FASTDUP_RANDOM_SIGKILL_RUN_ROOT")
            .expect("FASTDUP_RANDOM_SIGKILL_RUN_ROOT names a new workspace-local directory"),
    );
    let seed = std::env::var("FASTDUP_RANDOM_SIGKILL_SEED")
        .ok()
        .map_or(0x8d26_dfc4_69b1_a753, |value| {
            value.parse().expect("decimal randomized SIGKILL seed")
        });
    let cases = std::env::var("FASTDUP_RANDOM_SIGKILL_CASES")
        .ok()
        .map_or(32, |value| {
            value
                .parse()
                .expect("decimal randomized SIGKILL case count")
        });
    let operations = std::env::var("FASTDUP_RANDOM_SIGKILL_OPERATIONS")
        .ok()
        .map_or(256, |value| {
            value
                .parse()
                .expect("decimal randomized SIGKILL operation count")
        });

    let report = RandomizedSigkillConfig::v1(daemon, run_root, seed)
        .with_cases(cases)
        .with_operations_per_case(operations)
        .run()
        .expect("randomized SIGKILL soak recovers an acknowledged public-view prefix");
    assert_eq!(report.seed(), seed);
    assert_eq!(report.cases().len(), cases);
    assert!(
        report
            .cases()
            .iter()
            .all(|case| case.recovered_prefix() <= case.acknowledged_operations())
    );
}
