use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_appliance::{AppliancePoolBinding, AppliancePoolBindingError, POOL_IDENTITY_FILE_NAME};
use fastdup_format::{PoolIdentityRecord, PoolRole};
use fastdup_store::{FsStorageIo, StorageIo};
use fastdup_testkit::MemoryStorageIo;

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
fn fresh_pool_pair_persists_one_appliance_and_fixed_distinct_roles() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();

    let initialized = AppliancePoolBinding::initialize_or_open(&metadata, &data)
        .expect("initialize a fresh Pool pair");
    metadata.crash();
    data.crash();
    let reopened =
        AppliancePoolBinding::audit(&metadata, &data).expect("reopen the same durable Pool pair");

    assert_eq!(reopened, initialized);
    assert_eq!(reopened.metadata().role(), PoolRole::Metadata);
    assert_eq!(reopened.data().role(), PoolRole::Data);
    assert_eq!(
        reopened.metadata().appliance_id(),
        reopened.data().appliance_id()
    );
    assert_ne!(reopened.metadata().pool_id(), reopened.data().pool_id());
}

#[test]
fn writable_startup_rejects_swapped_pool_arguments_before_repository_recovery() {
    let root = unique_test_root("pool-role-swap");
    let mount_root = root.join("mount");
    let metadata_root = root.join("metadata");
    let data_root = root.join("data");
    std::fs::create_dir_all(&mount_root).expect("create mount root");
    let metadata = FsStorageIo::open(&metadata_root).expect("open Metadata Pool");
    let data = FsStorageIo::open(&data_root).expect("open Data Pool");
    AppliancePoolBinding::initialize_or_open(&metadata, &data)
        .expect("initialize valid roles before swapping arguments");

    let output = Command::new(env!("CARGO_BIN_EXE_fastdup-durable-fuse"))
        .args([&mount_root, &data_root, &metadata_root])
        .env("FASTDUP_POOL_ISOLATION", "lab-allow-shared")
        .output()
        .expect("execute writable startup with swapped Pool paths");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("RoleMismatch"),
        "ASSERT: startup rejects roles from durable identity before recovery: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn offline_scrub_rejects_pools_owned_by_different_appliances() {
    let root = unique_test_root("pool-appliance-mismatch");
    let first_metadata_root = root.join("first-metadata");
    let first_data_root = root.join("first-data");
    let second_metadata_root = root.join("second-metadata");
    let second_data_root = root.join("second-data");
    let first_metadata = FsStorageIo::open(&first_metadata_root).expect("open first Metadata Pool");
    let first_data = FsStorageIo::open(&first_data_root).expect("open first Data Pool");
    AppliancePoolBinding::initialize_or_open(&first_metadata, &first_data)
        .expect("initialize first Appliance");
    let second_metadata =
        FsStorageIo::open(&second_metadata_root).expect("open second Metadata Pool");
    let second_data = FsStorageIo::open(&second_data_root).expect("open second Data Pool");
    AppliancePoolBinding::initialize_or_open(&second_metadata, &second_data)
        .expect("initialize second Appliance");

    let output = Command::new(env!("CARGO_BIN_EXE_fastdup-maintenance"))
        .args([
            "--offline",
            "scrub",
            first_metadata_root
                .to_str()
                .expect("Metadata path is UTF-8"),
            second_data_root.to_str().expect("Data path is UTF-8"),
        ])
        .output()
        .expect("execute offline Scrub with cross-Appliance Pools");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("ApplianceIdMismatch"),
        "ASSERT: offline Scrub verifies shared Appliance ownership first: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn populated_pool_without_identity_is_rejected_instead_of_migrated() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    data.create_new("prototype-container.fdc")
        .expect("create prototype DATA object");
    data.sync_root().expect("make prototype name durable");

    let error = AppliancePoolBinding::initialize_or_open(&metadata, &data)
        .expect_err("current-only startup rejects an unidentified populated Pool");

    assert!(matches!(
        error,
        AppliancePoolBindingError::MissingIdentityInPopulatedPool {
            role: PoolRole::Data,
            ..
        }
    ));
    assert!(!metadata.exists(POOL_IDENTITY_FILE_NAME).unwrap());
    assert!(!data.exists(POOL_IDENTITY_FILE_NAME).unwrap());
}

#[test]
fn duplicate_pool_id_is_rejected_even_with_matching_appliance_and_roles() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let binding = AppliancePoolBinding::initialize_or_open(&metadata, &data)
        .expect("initialize valid Pool identities");
    let duplicate = PoolIdentityRecord::new(
        binding.metadata().appliance_id(),
        binding.metadata().pool_id(),
        PoolRole::Data,
    )
    .encode();
    data.write_at(POOL_IDENTITY_FILE_NAME, 0, &duplicate)
        .expect("inject duplicate durable Pool ID");
    data.sync_file(POOL_IDENTITY_FILE_NAME)
        .expect("make duplicate record durable");

    assert!(matches!(
        AppliancePoolBinding::audit(&metadata, &data),
        Err(AppliancePoolBindingError::DuplicatePoolId)
    ));
}

#[test]
fn corrupted_pool_identity_fails_closed() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    AppliancePoolBinding::initialize_or_open(&metadata, &data)
        .expect("initialize valid Pool identities");
    metadata
        .write_at(POOL_IDENTITY_FILE_NAME, 24, &[0xFF])
        .expect("corrupt checksummed Appliance ID field");
    metadata
        .sync_file(POOL_IDENTITY_FILE_NAME)
        .expect("make corruption durable");

    assert!(matches!(
        AppliancePoolBinding::audit(&metadata, &data),
        Err(AppliancePoolBindingError::Format {
            role: PoolRole::Metadata,
            ..
        })
    ));
}

#[test]
fn every_first_initialization_fault_recovers_to_no_record_or_one_valid_binding() {
    let baseline_metadata = MemoryStorageIo::new();
    let baseline_data = MemoryStorageIo::new();
    AppliancePoolBinding::initialize_or_open(&baseline_metadata, &baseline_data)
        .expect("measure complete initialization protocol");
    let metadata_operations = baseline_metadata.operation_count();
    let data_operations = baseline_data.operation_count();

    for fail_after_effect in [false, true] {
        for position in 0..metadata_operations {
            let metadata = if fail_after_effect {
                MemoryStorageIo::with_fail_after(position)
            } else {
                MemoryStorageIo::with_fail_before(position)
            };
            let data = MemoryStorageIo::new();
            let _ = AppliancePoolBinding::initialize_or_open(&metadata, &data);
            metadata.crash();
            data.crash();
            AppliancePoolBinding::initialize_or_open(&metadata, &data)
                .expect("restart completes or reuses a valid Metadata-side publication");
            metadata.crash();
            data.crash();
            AppliancePoolBinding::audit(&metadata, &data)
                .expect("recovered Metadata fault leaves one valid Pool binding");
        }

        for position in 0..data_operations {
            let metadata = MemoryStorageIo::new();
            let data = if fail_after_effect {
                MemoryStorageIo::with_fail_after(position)
            } else {
                MemoryStorageIo::with_fail_before(position)
            };
            let _ = AppliancePoolBinding::initialize_or_open(&metadata, &data);
            metadata.crash();
            data.crash();
            AppliancePoolBinding::initialize_or_open(&metadata, &data)
                .expect("restart completes or reuses a valid Data-side publication");
            metadata.crash();
            data.crash();
            AppliancePoolBinding::audit(&metadata, &data)
                .expect("recovered Data fault leaves one valid Pool binding");
        }
    }
}

#[test]
fn filesystem_audit_rejects_a_symlinked_identity_record() {
    use std::os::unix::fs::symlink;

    let root = unique_test_root("pool-identity-symlink");
    let source_metadata_root = root.join("source-metadata");
    let data_root = root.join("data");
    let alias_metadata_root = root.join("alias-metadata");
    let source_metadata =
        FsStorageIo::open(&source_metadata_root).expect("open source Metadata Pool");
    let data = FsStorageIo::open(&data_root).expect("open Data Pool");
    AppliancePoolBinding::initialize_or_open(&source_metadata, &data)
        .expect("initialize valid source records");
    std::fs::create_dir_all(&alias_metadata_root).expect("create alias Metadata root");
    symlink(
        source_metadata_root.join(POOL_IDENTITY_FILE_NAME),
        alias_metadata_root.join(POOL_IDENTITY_FILE_NAME),
    )
    .expect("symlink a valid record into a different root");
    let alias_metadata = FsStorageIo::open(&alias_metadata_root).expect("open alias Metadata root");

    assert!(matches!(
        AppliancePoolBinding::audit_filesystem(&alias_metadata, &data),
        Err(AppliancePoolBindingError::NonRegularIdentity {
            role: PoolRole::Metadata
        })
    ));
}
