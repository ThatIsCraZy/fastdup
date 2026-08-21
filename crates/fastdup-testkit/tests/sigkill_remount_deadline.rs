use std::path::PathBuf;

use fastdup_testkit::SigkillRemountConfig;

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
