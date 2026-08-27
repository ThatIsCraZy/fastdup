use std::path::{Path, PathBuf};

use fastdup_appliance::{
    ApplianceLease, ApplianceLeaseOwner, ApplianceRecoveryLatch, ApplianceRecoveryState,
    checkpoint_exact_index_profile_v1, checkpoint_policy_set, request_online_gc_now,
};
use fastdup_store::{
    ContainerRepository, DataPoolUsage, ExactIndexRunRepository, FsStorageIo, GenerationRepository,
    MaintenanceExecutionMode, MaintenanceRepository, SimilarityIndexRepository,
};

mod common;

use common::metadata_gc_status_fields;

const USAGE: &str = "usage:\n  fastdup-maintenance --online gc-now METADATA_ROOT\n  fastdup-maintenance --offline (scrub|scrub-gc|gc-now|metadata-gc|rebuild-exact|rebuild-pool-indexes) METADATA_ROOT CONTAINER_ROOT";

#[allow(clippy::too_many_lines)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let ownership = arguments.next().ok_or(USAGE)?;
    if ownership == std::ffi::OsStr::new("--online") {
        let command = arguments.next().ok_or(USAGE)?;
        if command != std::ffi::OsStr::new("gc-now") {
            return Err(USAGE.into());
        }
        let metadata_root = PathBuf::from(arguments.next().ok_or(USAGE)?);
        if arguments.next().is_some() {
            return Err(USAGE.into());
        }
        validate_root(&metadata_root, "metadata")?;
        let response = request_online_gc_now(&metadata_root)?;
        print!("{response}");
        return Ok(());
    }
    if ownership != std::ffi::OsStr::new("--offline") {
        return Err(
            format!("{USAGE}\n--offline is required; stop and unmount fastdup first").into(),
        );
    }
    let command = arguments.next().ok_or(USAGE)?;
    let metadata_root = PathBuf::from(arguments.next().ok_or(USAGE)?);
    let container_root = PathBuf::from(arguments.next().ok_or(USAGE)?);
    if arguments.next().is_some() {
        return Err(USAGE.into());
    }
    validate_root(&metadata_root, "metadata")?;
    validate_root(&container_root, "container")?;
    if std::fs::canonicalize(&metadata_root)? == std::fs::canonicalize(&container_root)? {
        return Err("metadata and container roots must be distinct".into());
    }
    let _appliance_lease =
        ApplianceLease::acquire(&metadata_root, ApplianceLeaseOwner::OfflineMaintenance)?;

    let metadata = FsStorageIo::open(&metadata_root)?;
    let recovery_required = ApplianceRecoveryLatch::audit_filesystem(&metadata)?
        == ApplianceRecoveryState::RecoveryRequired;
    let recovery_proof_command = command == "scrub" || command == "scrub-gc" || command == "gc-now";
    if recovery_required && !recovery_proof_command {
        return Err(
            "recovery-required repository needs a successful offline scrub before mutation".into(),
        );
    }
    let containers = ContainerRepository::new(FsStorageIo::open(&container_root)?);
    let indexes = ExactIndexRunRepository::new(metadata.clone());
    let maintenance = MaintenanceRepository::new(
        GenerationRepository::new(metadata.clone(), checkpoint_policy_set()),
        containers,
        indexes,
        checkpoint_exact_index_profile_v1(),
    );

    if command == "scrub" {
        print_scrub(maintenance.scrub()?);
    } else if command == "scrub-gc" || command == "gc-now" {
        let usage = data_pool_usage(&container_root)?;
        let mode = if command == "gc-now" {
            MaintenanceExecutionMode::FullSpeed
        } else {
            MaintenanceExecutionMode::Adaptive
        };
        let job = maintenance.start_scrub_and_gc_with_mode(usage, mode)?;
        println!(
            "maintenance_started=true mode={mode:?} scrub_priority={:?} data_pool_used_bytes={} data_pool_capacity_bytes={}",
            job.scrub_priority(),
            usage.used_bytes(),
            usage.capacity_bytes(),
        );
        let completed = job.wait()?;
        print_scrub(completed.scrub());
        println!(
            concat!(
                "gc_ok=true priority={:?} containers_removed={} bytes_removed={} ",
                "replacement_containers={} replacement_bytes={} chunks_relocated={} bytes_reclaimed={} ",
                "retiring_activation_wall_us={} pin_drain_wall_us={} victim_verify_wall_us={} ",
                "unlink_wall_us={} data_sync_wall_us={} removed_activation_wall_us={}"
            ),
            completed.gc().priority(),
            completed.gc().containers_removed(),
            completed.gc().bytes_removed(),
            completed.gc().replacement_containers(),
            completed.gc().replacement_bytes(),
            completed.gc().chunks_relocated(),
            completed.gc().bytes_reclaimed(),
            completed.gc().retiring_activation_wall().as_micros(),
            completed.gc().pin_drain_wall().as_micros(),
            completed.gc().victim_verify_wall().as_micros(),
            completed.gc().unlink_wall().as_micros(),
            completed.gc().data_sync_wall().as_micros(),
            completed.gc().removed_activation_wall().as_micros(),
        );
        print_metadata_gc(completed.metadata_gc());
    } else if command == "metadata-gc" {
        let report = maintenance.garbage_collect_metadata()?;
        print_metadata_gc(report);
        print_scrub(maintenance.scrub()?);
    } else if command == "rebuild-exact" {
        let rebuilt = maintenance.rebuild_exact_index()?;
        println!(
            "rebuild_ok=true containers_scanned={} entries_rebuilt={} run_families={} physical_runs={} run_set_generation={} activation_generation={}",
            rebuilt.containers_scanned(),
            rebuilt.entries_rebuilt(),
            rebuilt.run_families(),
            rebuilt.physical_runs(),
            rebuilt.run_set_generation(),
            rebuilt.activation_generation(),
        );
        print_scrub(maintenance.scrub()?);
    } else if command == "rebuild-pool-indexes" {
        let similarities = SimilarityIndexRepository::new(metadata.clone());
        let rebuilt = maintenance.rebuild_pool_indexes(&similarities)?;
        println!(
            concat!(
                "pool_rebuild_ok=true containers_scanned={} entries_rebuilt={} ",
                "run_families={} physical_runs={} run_set_generation={} activation_generation={} ",
                "source_exact_run_set_id={} similarity_generation={} similarity_entries={} ",
                "similarity_partitions={}"
            ),
            rebuilt.exact().containers_scanned(),
            rebuilt.exact().entries_rebuilt(),
            rebuilt.exact().run_families(),
            rebuilt.exact().physical_runs(),
            rebuilt.exact().run_set_generation(),
            rebuilt.exact().activation_generation(),
            encode_hex(rebuilt.exact_run_set_id().bytes()),
            rebuilt.similarity_generation(),
            rebuilt.similarity_entries(),
            rebuilt.similarity_partitions(),
        );
        print_scrub(maintenance.scrub()?);
    } else {
        return Err(USAGE.into());
    }
    if recovery_required {
        ApplianceRecoveryLatch::clear_filesystem_after_verified_recovery(&metadata)?;
        println!("appliance_recovery_required=false proof=offline_scrub");
    }
    Ok(())
}

fn encode_hex<const N: usize>(bytes: [u8; N]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(N * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}")
            .expect("ASSERT: writing into an owned String cannot fail");
    }
    encoded
}

fn print_metadata_gc(report: fastdup_store::MetadataGarbageCollectionReport) {
    println!(
        "metadata_gc_ok=true {}",
        metadata_gc_status_fields(report, "")
    );
}

fn data_pool_usage(path: &Path) -> Result<DataPoolUsage, Box<dyn std::error::Error>> {
    let statistics = rustix::fs::statvfs(path)?;
    let fragment_bytes = statistics.f_frsize.max(1);
    let capacity = statistics
        .f_blocks
        .checked_mul(fragment_bytes)
        .ok_or("data-pool capacity overflows u64")?;
    let available = statistics
        .f_bavail
        .checked_mul(fragment_bytes)
        .ok_or("data-pool available capacity overflows u64")?;
    let used = capacity
        .checked_sub(available)
        .ok_or("data-pool available capacity exceeds total capacity")?;
    DataPoolUsage::new(used, capacity).map_err(Into::into)
}

fn validate_root(path: &Path, tier: &str) -> Result<(), Box<dyn std::error::Error>> {
    if !path.is_dir() {
        return Err(format!(
            "{tier} root is not an existing directory: {}",
            path.display()
        )
        .into());
    }
    Ok(())
}

fn print_scrub(report: fastdup_store::EndToEndScrubReport) {
    println!(
        "scrub_ok=true commit_generations_verified={} commit_generation={} namespace_inodes={} manifest_files={} containers={} container_chunks={} container_generation_high_water={} exact_activation_generation={} exact_active_locations_verified={}",
        report.commit_generations_verified(),
        display_option(report.commit_generation()),
        report.namespace_inodes(),
        report.manifest_files(),
        report.containers(),
        report.container_chunks(),
        display_option(report.container_generation_high_water()),
        display_option(report.exact_activation_generation()),
        report.exact_active_locations_verified(),
    );
}

fn display_option(value: Option<u64>) -> String {
    value.map_or_else(|| "none".to_owned(), |value| value.to_string())
}
