use std::path::{Path, PathBuf};

use fastdup_appliance::{checkpoint_exact_index_profile_v1, checkpoint_policy_set_v1};
use fastdup_store::{
    ContainerRepository, DataPoolUsage, ExactIndexRunRepository, FsStorageIo, GenerationRepository,
    MaintenanceRepository,
};

const USAGE: &str = "usage: fastdup-maintenance --offline (scrub|scrub-gc|rebuild-exact) METADATA_ROOT CONTAINER_ROOT";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("--offline")) {
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

    let metadata = FsStorageIo::open(&metadata_root)?;
    let containers = ContainerRepository::new(FsStorageIo::open(&container_root)?);
    let maintenance = MaintenanceRepository::new(
        GenerationRepository::new(metadata.clone(), checkpoint_policy_set_v1()),
        containers,
        ExactIndexRunRepository::new(metadata),
        checkpoint_exact_index_profile_v1(),
    );

    if command == "scrub" {
        print_scrub(maintenance.scrub()?);
    } else if command == "scrub-gc" {
        let usage = data_pool_usage(&container_root)?;
        let job = maintenance.start_scrub_and_gc(usage)?;
        println!(
            "maintenance_started=true scrub_priority={:?} data_pool_used_bytes={} data_pool_capacity_bytes={}",
            job.scrub_priority(),
            usage.used_bytes(),
            usage.capacity_bytes(),
        );
        let completed = job.wait()?;
        print_scrub(completed.scrub());
        println!(
            "gc_ok=true priority={:?} containers_removed={} bytes_removed={} replacement_containers={} replacement_bytes={} chunks_relocated={} bytes_reclaimed={}",
            completed.gc().priority(),
            completed.gc().containers_removed(),
            completed.gc().bytes_removed(),
            completed.gc().replacement_containers(),
            completed.gc().replacement_bytes(),
            completed.gc().chunks_relocated(),
            completed.gc().bytes_reclaimed(),
        );
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
    } else {
        return Err(USAGE.into());
    }
    Ok(())
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
