use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fastdup_appliance::DurableNamespace;
use fastdup_format::PolicySetId;
use fastdup_posix::{FuseFilesystem, NamespaceConfig, volatile_mount_options};
use fastdup_store::{ContainerRepository, FsStorageIo, GenerationRepository};
use fuse3::raw::Session;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, sleep};

const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);
const CHECKPOINT_WARNING: Duration = Duration::from_secs(5);
const INODE_RESERVATION_SPAN: u64 = 4_096;
const POLICY_SET_BYTES: [u8; 32] = [0xF1; 32];

type FsAppliance = DurableNamespace<FsStorageIo, FsStorageIo>;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1).map(PathBuf::from);
    let mount_path = arguments
        .next()
        .ok_or("usage: fastdup-durable-fuse MOUNT_PATH METADATA_ROOT CONTAINER_ROOT")?;
    let metadata_root = arguments
        .next()
        .ok_or("usage: fastdup-durable-fuse MOUNT_PATH METADATA_ROOT CONTAINER_ROOT")?;
    let container_root = arguments
        .next()
        .ok_or("usage: fastdup-durable-fuse MOUNT_PATH METADATA_ROOT CONTAINER_ROOT")?;
    if arguments.next().is_some() {
        return Err("usage: fastdup-durable-fuse MOUNT_PATH METADATA_ROOT CONTAINER_ROOT".into());
    }
    if !mount_path.is_dir() {
        return Err(format!("mount path is not a directory: {}", mount_path.display()).into());
    }
    if metadata_root == container_root {
        return Err("metadata and container roots must be distinct".into());
    }

    let policy = PolicySetId::new(POLICY_SET_BYTES)
        .expect("ASSERT: built-in experimental Policy Set ID must be nonzero");
    let appliance = Arc::new(DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(FsStorageIo::open(&metadata_root)?, policy),
        ContainerRepository::new(FsStorageIo::open(&container_root)?),
        INODE_RESERVATION_SPAN,
    )?);
    let filesystem = FuseFilesystem::new(appliance.namespace_arc());
    let session = Session::new(volatile_mount_options());
    let mount = session.mount(filesystem, &mount_path).await?;
    eprintln!(
        "fastdup durable checkpoint mount at {}; metadata={}, containers={}",
        mount_path.display(),
        metadata_root.display(),
        container_root.display()
    );

    let mut ticks = interval(CHECKPOINT_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticks.tick().await;
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
            _ = ticks.tick() => {
                if let Err(error) = checkpoint_cycle(Arc::clone(&appliance)).await {
                    appliance.namespace().pause_mutation_admission();
                    eprintln!(
                        "CRITICAL: durable progress failed; mutation admission remains closed: {error}"
                    );
                }
            }
        }
    }

    appliance.namespace().pause_mutation_admission();
    if let Err(error) = catch_up(Arc::clone(&appliance)).await {
        eprintln!("CRITICAL: final checkpoint failed during shutdown: {error}");
    }
    mount.unmount().await?;
    Ok(())
}

async fn checkpoint_cycle(appliance: Arc<FsAppliance>) -> Result<(), String> {
    let already_paused = !appliance.namespace().mutation_admission_open();
    let worker_appliance = Arc::clone(&appliance);
    let mut worker = tokio::task::spawn_blocking(move || worker_appliance.checkpoint());
    let _result = if already_paused {
        await_worker(worker).await?
    } else {
        tokio::select! {
            result = &mut worker => map_worker_result(result)?,
            () = sleep(CHECKPOINT_WARNING) => {
                appliance.namespace().pause_mutation_admission();
                eprintln!(
                    "CRITICAL: checkpoint exceeded five seconds; mutation admission is closed"
                );
                await_worker(worker).await?
            }
        }
    };
    if !appliance.namespace().mutation_admission_open() {
        catch_up(Arc::clone(&appliance)).await?;
        appliance.namespace().resume_mutation_admission();
        eprintln!("durable progress caught up; mutation admission reopened");
    }
    Ok(())
}

async fn catch_up(appliance: Arc<FsAppliance>) -> Result<(), String> {
    loop {
        let worker_appliance = Arc::clone(&appliance);
        let worker = tokio::task::spawn_blocking(move || worker_appliance.checkpoint());
        if await_worker(worker).await?.is_none() {
            return Ok(());
        }
    }
}

async fn await_worker(
    worker: JoinHandle<
        Result<Option<fastdup_format::CommitRecord>, fastdup_appliance::DurableNamespaceError>,
    >,
) -> Result<Option<fastdup_format::CommitRecord>, String> {
    map_worker_result(worker.await)
}

fn map_worker_result(
    result: Result<
        Result<Option<fastdup_format::CommitRecord>, fastdup_appliance::DurableNamespaceError>,
        tokio::task::JoinError,
    >,
) -> Result<Option<fastdup_format::CommitRecord>, String> {
    result
        .map_err(|error| format!("checkpoint worker failed: {error}"))?
        .map_err(|error| format!("checkpoint failed: {error}"))
}
