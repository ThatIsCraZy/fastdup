use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_testkit::FullTierEnospcConfig;

#[test]
#[ignore = "requires root, FUSE, mkfs.xfs, mount, xfs_io, and xfs_quota"]
fn real_xfs_tiers_return_fuse_enospc_and_recover_exactly() {
    let daemon = PathBuf::from(
        std::env::var_os("FASTDUP_DAEMON_BIN")
            .expect("set FASTDUP_DAEMON_BIN to fastdup-durable-fuse"),
    );
    let maintenance = PathBuf::from(
        std::env::var_os("FASTDUP_MAINTENANCE_BIN")
            .expect("set FASTDUP_MAINTENANCE_BIN to fastdup-maintenance"),
    );
    let run_root = std::env::var_os("FASTDUP_FULL_TIER_RUN_ROOT").map_or_else(
        || {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("wall clock is after Unix epoch")
                .as_nanos();
            PathBuf::from(format!(
                "/source/fastdup/.artifacts/full-tier-enospc-{nonce}"
            ))
        },
        PathBuf::from,
    );
    let report = FullTierEnospcConfig::v1(daemon, maintenance, run_root)
        .run()
        .expect("real-tier ENOSPC proof must pass");
    assert!(report.accepted_bytes() > 0);
    assert_eq!(report.rejected_writes(), 3);
    eprintln!(
        "full_tier_enospc_ok=true accepted_bytes={} available_before={} available_at_enospc={} small_file_bytes={} small_file_allocated_bytes={} artifacts={}",
        report.accepted_bytes(),
        report.presented_available_before(),
        report.presented_available_at_enospc(),
        report.small_file_bytes(),
        report.small_file_allocated_bytes(),
        report.run_root().display(),
    );
}
