use std::sync::Arc;
use std::time::Duration;

use fastdup_appliance::{
    CheckpointProgressAction, DurabilitySupervisor, DurableNamespace, checkpoint_policy_set_v1,
};
use fastdup_posix::{
    InodeId, NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply, RequestContext,
};
use fastdup_store::{ContainerRepository, GenerationRepository, StorageIo};
use fastdup_testkit::{MemoryStorageIo, PausedStorageIo, StorageOperation};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 17,
};

fn seed_one_dirty_write<M, C>(
    appliance: &DurableNamespace<M, C>,
) -> (InodeId, fastdup_posix::HandleId)
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"deadline",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create deadline fixture")
    else {
        panic!("ASSERT: create returns a file and handle");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"visible",
            },
        )
        .expect("acknowledge initial mutation");
    (entry.attr.inode, handle)
}

fn prove_stalled_checkpoint_closes_only_new_mutations<M, C>(
    appliance: DurableNamespace<M, C>,
    paused: &PausedStorageIo,
    tier: &str,
) where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let appliance = Arc::new(appliance);
    let (inode, handle) = seed_one_dirty_write(&appliance);
    paused.arm();
    let checkpointing = Arc::clone(&appliance);
    let checkpoint = std::thread::spawn(move || checkpointing.checkpoint());
    let reached = paused.wait_until_reached(Duration::from_secs(5));

    let progress = DurabilitySupervisor::checkpoint_progress(Duration::from_secs(5));
    if progress == CheckpointProgressAction::CloseAdmission {
        appliance.namespace().pause_mutation_admission();
    }
    let live_read = appliance.namespace().dispatch(
        CALLER,
        Operation::Read {
            inode,
            handle,
            offset: 0,
            length: 7,
        },
    );
    let later_write = appliance.namespace().dispatch(
        CALLER,
        Operation::Write {
            inode,
            handle,
            offset: 7,
            data: b"-blocked",
        },
    );

    paused.resume();
    let committed = checkpoint
        .join()
        .expect("checkpoint worker remains healthy");

    assert!(reached, "{tier} sync reaches its deliberate stall");
    assert_eq!(progress, CheckpointProgressAction::CloseAdmission);
    assert_eq!(live_read, Ok(Reply::Data(b"visible".to_vec())));
    assert_eq!(later_write, Err(PosixError::Again));
    committed
        .expect("stalled checkpoint resumes")
        .expect("dirty generation commits after storage resumes");
    appliance.namespace().resume_mutation_admission();
    assert!(appliance.namespace().mutation_admission_open());
}

#[test]
fn fake_clock_closes_admission_while_metadata_sync_is_stalled() {
    let paused = PausedStorageIo::disarmed_before_name_prefix(
        MemoryStorageIo::new(),
        StorageOperation::SyncFile,
        ".",
    );
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(paused.clone(), checkpoint_policy_set_v1()),
        ContainerRepository::new(MemoryStorageIo::new()),
        4_096,
    )
    .expect("open Metadata-stall fixture");
    prove_stalled_checkpoint_closes_only_new_mutations(appliance, &paused, "Metadata");
}

#[test]
fn fake_clock_closes_admission_while_data_sync_is_stalled() {
    let paused = PausedStorageIo::disarmed_before_name_prefix(
        MemoryStorageIo::new(),
        StorageOperation::SyncFile,
        ".",
    );
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(MemoryStorageIo::new(), checkpoint_policy_set_v1()),
        ContainerRepository::new(paused.clone()),
        4_096,
    )
    .expect("open DATA-stall fixture");
    prove_stalled_checkpoint_closes_only_new_mutations(appliance, &paused, "DATA");
}
