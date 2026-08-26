use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_appliance::{APPLIANCE_RECOVERY_LATCH_FILE_NAME, ApplianceLease, ApplianceLeaseOwner};

const HOLDER_ENV: &str = "FASTDUP_APPLIANCE_LEASE_HOLDER_ROOT";

fn unique_test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after the Unix epoch")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}-{nonce}", std::process::id()))
}

#[test]
fn appliance_lease_holder_process() {
    let Some(root) = std::env::var_os(HOLDER_ENV) else {
        return;
    };
    let _lease = ApplianceLease::acquire(&root, ApplianceLeaseOwner::WritableDaemon)
        .expect("child acquires appliance lease");
    println!("LEASED");
    std::io::stdout().flush().expect("flush child readiness");
    let mut release = String::new();
    std::io::stdin()
        .read_line(&mut release)
        .expect("wait for parent release");
}

#[test]
fn independent_process_excludes_offline_maintenance_and_crash_releases_lease() {
    let root = unique_test_root("appliance-lease");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    std::fs::create_dir_all(&metadata_root).expect("create metadata root");
    std::fs::create_dir_all(&container_root).expect("create container root");

    let mut child = Command::new(std::env::current_exe().expect("locate test binary"))
        .args(["--exact", "appliance_lease_holder_process", "--nocapture"])
        .env(HOLDER_ENV, &metadata_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn independent lease holder");
    let mut output = BufReader::new(child.stdout.take().expect("pipe holder stdout"));
    let mut ready = String::new();
    loop {
        let mut line = String::new();
        let read = output.read_line(&mut line).expect("read holder readiness");
        ready.push_str(&line);
        if line.contains("LEASED") || read == 0 {
            break;
        }
    }
    assert!(
        ready.contains("LEASED"),
        "ASSERT: independent process must hold the lease before contention: {ready:?}"
    );

    let output = Command::new(env!("CARGO_BIN_EXE_fastdup-maintenance"))
        .args([
            "--offline",
            "scrub",
            metadata_root.to_str().expect("metadata path is UTF-8"),
            container_root.to_str().expect("container path is UTF-8"),
        ])
        .output()
        .expect("execute real offline maintenance CLI");
    assert!(
        !output.status.success(),
        "ASSERT: lease contention must fail"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Appliance Lease"),
        "ASSERT: failure must identify the ownership conflict: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mount_root = root.join("mount");
    std::fs::create_dir_all(&mount_root).expect("create contender mount root");
    let daemon = Command::new(env!("CARGO_BIN_EXE_fastdup-durable-fuse"))
        .args([&mount_root, &metadata_root, &container_root])
        .output()
        .expect("execute real writable daemon contender");
    assert!(!daemon.status.success(), "ASSERT: second daemon must fail");
    assert!(
        String::from_utf8_lossy(&daemon.stderr).contains("Appliance Lease"),
        "ASSERT: daemon contention identifies repository ownership: {}",
        String::from_utf8_lossy(&daemon.stderr)
    );

    child.kill().expect("SIGKILL independent lease holder");
    child.wait().expect("reap independent lease holder");
    let recovered =
        ApplianceLease::acquire(&metadata_root, ApplianceLeaseOwner::OfflineMaintenance)
            .expect("kernel releases appliance lease after owner death");
    assert_eq!(recovered.owner(), ApplianceLeaseOwner::OfflineMaintenance);
}

#[test]
fn invalid_gc_policy_fails_daemon_before_repository_open() {
    let root = unique_test_root("invalid-online-gc-policy");
    let mount_root = root.join("mount");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    std::fs::create_dir_all(&mount_root).expect("create mount root");

    let output = Command::new(env!("CARGO_BIN_EXE_fastdup-durable-fuse"))
        .args([&mount_root, &metadata_root, &container_root])
        .env("FASTDUP_ONLINE_GC_PRESSURE_LOW_BASIS_POINTS", "9500")
        .env("FASTDUP_ONLINE_GC_PRESSURE_HIGH_BASIS_POINTS", "9000")
        .output()
        .expect("execute real writable daemon entry point");

    assert!(
        !output.status.success(),
        "ASSERT: invalid policy must fail startup"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Online-GC pressure requires 0 <= low < high <= 10000 basis points"),
        "ASSERT: startup reports the invalid policy: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !metadata_root.exists(),
        "ASSERT: policy validation precedes creating the Metadata repository"
    );
    assert!(
        !container_root.exists(),
        "ASSERT: policy validation precedes opening or creating the DATA repository"
    );
}

#[test]
fn malformed_recovery_latch_fails_daemon_before_data_repository_open() {
    let root = unique_test_root("malformed-recovery-latch");
    let mount_root = root.join("mount");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    std::fs::create_dir_all(&mount_root).expect("create mount root");
    std::fs::create_dir_all(&metadata_root).expect("create metadata root");
    std::fs::write(
        metadata_root.join(APPLIANCE_RECOVERY_LATCH_FILE_NAME),
        b"not-an-empty-latch",
    )
    .expect("write malformed recovery latch");
    std::fs::write(&container_root, b"DATA sentinel").expect("create invalid DATA root sentinel");

    let output = Command::new(env!("CARGO_BIN_EXE_fastdup-durable-fuse"))
        .args([&mount_root, &metadata_root, &container_root])
        .output()
        .expect("execute daemon against malformed recovery latch");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Appliance Recovery Latch must be an empty canonical object"),
        "ASSERT: startup identifies the malformed latch before opening DATA: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(&container_root).expect("read unchanged DATA sentinel"),
        b"DATA sentinel",
        "ASSERT: latch audit precedes opening or changing the DATA repository"
    );
}

#[test]
fn symlink_recovery_latch_fails_daemon_before_data_repository_open() {
    use std::os::unix::fs::symlink;

    let root = unique_test_root("symlink-recovery-latch");
    let mount_root = root.join("mount");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    let symlink_target = root.join("empty-target");
    std::fs::create_dir_all(&mount_root).expect("create mount root");
    std::fs::create_dir_all(&metadata_root).expect("create metadata root");
    std::fs::write(&symlink_target, b"").expect("create empty symlink target");
    symlink(
        &symlink_target,
        metadata_root.join(APPLIANCE_RECOVERY_LATCH_FILE_NAME),
    )
    .expect("create symlink latch");
    std::fs::write(&container_root, b"DATA sentinel").expect("create invalid DATA root sentinel");

    let output = Command::new(env!("CARGO_BIN_EXE_fastdup-durable-fuse"))
        .args([&mount_root, &metadata_root, &container_root])
        .output()
        .expect("execute daemon against symlink recovery latch");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Appliance Recovery Latch is not a regular file"),
        "ASSERT: startup rejects a symlink latch before opening DATA: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(&container_root).expect("read unchanged DATA sentinel"),
        b"DATA sentinel",
        "ASSERT: latch type audit precedes opening or changing the DATA repository"
    );
}
