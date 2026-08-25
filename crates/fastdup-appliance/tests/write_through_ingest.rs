use fastdup_appliance::{DurableNamespace, checkpoint_policy_set_v1, recover_mount};
use fastdup_posix::{
    HandleId, InodeId, MutationPayload, Namespace, NamespaceConfig, OpenOptions, Operation,
    ROOT_INODE, Reply, RequestContext,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, GenerationRepository, StorageIo,
};
use fastdup_testkit::{MemoryStorageIo, PausedStorageIo, StorageOperation};
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 7,
};
const STORAGE_REACH_TIMEOUT: Duration = Duration::from_secs(30);

type Appliance = DurableNamespace<MemoryStorageIo, MemoryStorageIo>;

fn open_appliance() -> Appliance {
    open_appliance_on(
        MemoryStorageIo::new(),
        MemoryStorageIo::new(),
        MemoryStorageIo::new(),
    )
}

fn open_appliance_on(
    metadata: MemoryStorageIo,
    containers: MemoryStorageIo,
    indexes: MemoryStorageIo,
) -> Appliance {
    DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata, checkpoint_policy_set_v1()),
        ContainerRepository::new(containers),
        &ExactIndexRunRepository::new(indexes),
        32,
    )
    .expect("open write-through appliance")
}

fn open_appliance_with_paused_containers(
    containers: PausedStorageIo,
) -> DurableNamespace<MemoryStorageIo, PausedStorageIo> {
    DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(MemoryStorageIo::new(), checkpoint_policy_set_v1()),
        ContainerRepository::new(containers),
        &ExactIndexRunRepository::new(MemoryStorageIo::new()),
        32,
    )
    .expect("open write-through appliance with paused data tier")
}

#[test]
fn write_returns_and_is_live_while_container_durability_is_blocked() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode, handle) = create_file(&appliance, b"async-write");
    let block = fixture_block();
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer_appliance = Arc::clone(&appliance);
    let writer = std::thread::spawn(move || {
        for ordinal in 0_u64..34 {
            write_one_mebibyte(&writer_appliance, inode, handle, ordinal, &block);
        }
        finished_tx
            .send(())
            .expect("test receiver remains available");
    });

    assert!(
        paused.wait_until_reached(STORAGE_REACH_TIMEOUT),
        "worked stream must reach Container file durability"
    );
    let completed_while_paused = finished_rx.recv_timeout(Duration::from_millis(500)).is_ok();
    let live = read_fixture(&appliance, inode, handle);
    paused.resume();
    writer.join().expect("writer thread completes");

    assert!(
        completed_while_paused,
        "FUSE-visible writes must not wait for background Container durability"
    );
    assert_eq!(live.len(), 4_096);
}

#[test]
fn one_stream_chunks_a_second_container_while_the_first_waits_for_durability() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode, handle) = create_file(&appliance, b"same-inode-overlap");
    let block = fixture_block();
    let writer_appliance = Arc::clone(&appliance);
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        for ordinal in 0_u64..70 {
            write_one_mebibyte(&writer_appliance, inode, handle, ordinal, &block);
        }
        finished_tx
            .send(())
            .expect("test receiver remains available");
    });

    assert!(
        paused.wait_until_reached(STORAGE_REACH_TIMEOUT),
        "the first detached Container reaches its durability barrier"
    );
    let completed_while_paused = finished_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    paused.resume();
    writer.join().expect("writer thread completes");
    fence_ingest(&appliance, inode, handle);
    let status = appliance.write_through_status();
    let Reply::Data(live_tail) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle,
                offset: 69 * 1_048_576,
                length: 4_096,
            },
        )
        .expect("the newest admitted bytes stay live")
    else {
        panic!("read returned the wrong reply");
    };

    assert!(
        completed_while_paused,
        "same-inode SeqCDC must advance beyond one blocked Container publication: {status:?}"
    );
    assert_eq!(live_tail.len(), 4_096);
    assert_eq!(
        status.sealed_uncommitted_containers(),
        1,
        "queued Containers must share in-flight Chunk publication instead of writing duplicate DATA"
    );
    assert!(
        status.buffered_bytes() <= 384 * 1_024 * 1_024,
        "detached publication work remains inside the process ingest budget: {status:?}"
    );
    assert!(
        status.maximum_ingest_ring_slots() >= 2,
        "a blocked Container publication must leave another SingleStream slot available: {status:?}"
    );
}

#[test]
fn one_stream_reaches_two_container_durability_barriers_in_parallel() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode, handle) = create_file(&appliance, b"same-inode-publication-window");
    let mut block = fixture_block();
    let writer_appliance = Arc::clone(&appliance);
    let writer = std::thread::spawn(move || {
        for ordinal in 0_u64..70 {
            for byte in &mut block {
                *byte = byte.wrapping_add(1);
            }
            write_one_mebibyte(&writer_appliance, inode, handle, ordinal, &block);
        }
    });

    let two_publications_in_flight = paused.wait_until_reached_count(2, Duration::from_secs(5));
    paused.resume();
    writer.join().expect("writer thread completes");
    fence_ingest(&appliance, inode, handle);

    assert!(
        two_publications_in_flight,
        "one active inode must fill both bounded detached-Container publication slots"
    );
}

#[test]
fn one_stream_hashes_stable_chunk_batches_on_multiple_workers() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"parallel-chunk-hashes");
    let mut block = fixture_block();
    for ordinal in 0_u64..34 {
        block[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    fence_ingest(&appliance, inode, handle);

    let status = appliance.write_through_status();
    assert!(status.hash_batches() > 0, "stable Chunks form a hash batch");
    assert!(
        status.maximum_hash_workers() > 1,
        "one long stream must use multiple CPU workers for independent Chunk hashes: {status:?}"
    );
}

#[test]
fn sequential_writes_coalesce_up_to_four_mebibytes_before_sync() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"coalesced-ingest-batches");
    let mut block = fixture_block();
    for ordinal in 0_u64..8 {
        block[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    fence_ingest(&appliance, inode, handle);

    let status = appliance.write_through_status();
    assert!(
        status.ingest_batches() <= 4,
        "eight sequential one-MiB writes must be coalesced despite the age bound: {status:?}"
    );
    assert_eq!(status.ingest_fragments(), 8);
    assert_eq!(status.maximum_ingest_batch_bytes(), 4 * 1_024 * 1_024);
    block[0] = 4;
    assert_eq!(read_fixture(&appliance, inode, handle), block[..4_096]);
}

#[test]
fn partial_ingest_batch_flushes_after_its_age_bound() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"aged-ingest-batch");
    let block = fixture_block();
    write_one_mebibyte(&appliance, inode, handle, 0, &block);

    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while appliance.write_through_status().ingest_batches() == 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    let status = appliance.write_through_status();
    assert_eq!(
        status.ingest_batches(),
        1,
        "an open partial batch must flush without waiting for a caller fence: {status:?}"
    );
    assert_eq!(status.maximum_ingest_batch_bytes(), 1_024 * 1_024);
    fence_ingest(&appliance, inode, handle);
}

#[test]
fn multiple_active_streams_restore_one_mebibyte_fragment_scheduling() {
    let appliance = Arc::new(open_appliance());
    let files = (0..4)
        .map(|ordinal| create_file(&appliance, format!("adaptive-stream-{ordinal}").as_bytes()))
        .collect::<Vec<_>>();
    let payload = MutationPayload::from_owned_bytes(fixture_block());
    let barrier = Arc::new(Barrier::new(files.len() + 1));
    std::thread::scope(|scope| {
        for &(inode, handle) in &files {
            let appliance = Arc::clone(&appliance);
            let payload = payload.clone();
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                appliance
                    .namespace()
                    .dispatch_owned_write(CALLER, inode, handle, 0, payload)
                    .expect("admit one owned stream fragment");
            });
        }
        barrier.wait();
    });

    let status = appliance.write_through_status();
    assert_eq!(status.minimum_ingest_batch_target_bytes(), 1_024 * 1_024);
    for (inode, handle) in files {
        fence_ingest(&appliance, inode, handle);
    }
}

#[test]
fn releasing_the_competing_writer_restores_single_stream_coalescing() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"remaining-writer");
    let (other_inode, other_handle) = create_file(&appliance, b"released-writer");
    let block = fixture_block();
    write_one_mebibyte(&appliance, inode, handle, 0, &block);
    write_one_mebibyte(&appliance, other_inode, other_handle, 0, &block);
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Release {
                inode: other_inode,
                handle: other_handle,
            },
        )
        .expect("release the competing writer");

    for ordinal in 1_u64..=4 {
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    fence_ingest(&appliance, inode, handle);
    assert_eq!(
        appliance
            .write_through_status()
            .maximum_ingest_batch_bytes(),
        4 * 1_024 * 1_024
    );
}

#[test]
fn release_waits_for_its_last_queued_write_without_blocking_write_admission() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode, handle) = create_file(&appliance, b"close-fence");
    let block = fixture_block();
    for ordinal in 0_u64..34 {
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    assert!(
        paused.wait_until_reached(STORAGE_REACH_TIMEOUT),
        "background reduction must reach blocked durability"
    );

    let release_appliance = Arc::clone(&appliance);
    let (released_tx, released_rx) = mpsc::channel();
    let release = std::thread::spawn(move || {
        release_appliance
            .namespace()
            .dispatch(CALLER, Operation::Release { inode, handle })
            .expect("release succeeds after its sequence fence");
        released_tx
            .send(())
            .expect("test receiver remains available");
    });
    let released_while_paused = released_rx.recv_timeout(Duration::from_millis(500)).is_ok();
    paused.resume();
    release.join().expect("release thread completes");

    assert!(
        !released_while_paused,
        "close must fence its accepted writes before retiring the handle"
    );
}

#[test]
fn different_files_reach_container_durability_in_parallel() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode_a, handle_a) = create_file(&appliance, b"parallel-sync-a");
    let (inode_b, handle_b) = create_file(&appliance, b"parallel-sync-b");
    let block_a = fixture_block();
    let mut block_b = fixture_block();
    for byte in &mut block_b {
        *byte = byte.wrapping_add(37);
    }

    let first_stream = Arc::clone(&appliance);
    let writer_a = std::thread::spawn(move || {
        for ordinal in 0_u64..34 {
            write_one_mebibyte(&first_stream, inode_a, handle_a, ordinal, &block_a);
        }
    });
    assert!(
        paused.wait_until_reached_count(1, STORAGE_REACH_TIMEOUT),
        "first file reaches blocked durability"
    );
    let second_stream = Arc::clone(&appliance);
    let writer_b = std::thread::spawn(move || {
        for ordinal in 0_u64..34 {
            write_one_mebibyte(&second_stream, inode_b, handle_b, ordinal, &block_b);
        }
    });

    let both_reached_durability = paused.wait_until_reached_count(2, STORAGE_REACH_TIMEOUT);
    paused.resume();
    writer_a.join().expect("first writer completes");
    writer_b.join().expect("second writer completes");
    assert!(
        both_reached_durability,
        "one blocked file must not retain the complete global encode budget during data-tier I/O"
    );
}

#[test]
fn identical_parallel_files_share_one_inflight_chunk_publication() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode_a, handle_a) = create_file(&appliance, b"singleflight-a");
    let (inode_b, handle_b) = create_file(&appliance, b"singleflight-b");
    let barrier = Arc::new(Barrier::new(2));

    let first_appliance = Arc::clone(&appliance);
    let first_barrier = Arc::clone(&barrier);
    let writer_a = std::thread::spawn(move || {
        first_barrier.wait();
        write_fixture(&first_appliance, inode_a, handle_a)
    });
    let second_appliance = Arc::clone(&appliance);
    let writer_b = std::thread::spawn(move || {
        barrier.wait();
        write_fixture(&second_appliance, inode_b, handle_b)
    });

    assert!(
        paused.wait_until_reached_count(1, STORAGE_REACH_TIMEOUT),
        "one identical stream must own the shared Container publication"
    );
    assert!(
        !paused.wait_until_reached_count(2, Duration::from_millis(500)),
        "the competing identical stream must await the first proof instead of publishing duplicate DATA"
    );
    paused.resume();
    let expected_a = writer_a.join().expect("first identical writer completes");
    let expected_b = writer_b.join().expect("second identical writer completes");
    fence_ingest(&appliance, inode_a, handle_a);
    fence_ingest(&appliance, inode_b, handle_b);

    assert_eq!(expected_a, expected_b);
    assert_eq!(read_fixture(&appliance, inode_a, handle_a), expected_a);
    assert_eq!(read_fixture(&appliance, inode_b, handle_b), expected_b);
    assert_eq!(
        appliance
            .write_through_status()
            .sealed_uncommitted_containers(),
        1,
        "two concurrent identical streams must seal only one physical Container"
    );
}

#[test]
fn admission_blocks_only_after_the_bounded_ingest_queue_fills() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode, handle) = create_file(&appliance, b"queue-pressure");
    let block = fixture_block();
    let writer_appliance = Arc::clone(&appliance);
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        for ordinal in 0_u64..128 {
            write_one_mebibyte(&writer_appliance, inode, handle, ordinal, &block);
        }
        finished_tx
            .send(())
            .expect("test receiver remains available");
    });
    assert!(
        paused.wait_until_reached(STORAGE_REACH_TIMEOUT),
        "stream reaches blocked Container durability"
    );
    let finished_before_space = finished_rx.recv_timeout(Duration::from_millis(500)).is_ok();
    paused.resume();
    writer
        .join()
        .expect("writer completes after queue space returns");

    assert!(
        !finished_before_space,
        "writes beyond two detached Containers and the bounded queue must apply admission backpressure"
    );
}

#[test]
fn status_does_not_deadlock_a_full_single_stream_publication_queue() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode, handle) = create_file(&appliance, b"status-under-queue-pressure");
    let block = fixture_block();
    let writer_appliance = Arc::clone(&appliance);
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        for ordinal in 0_u64..128 {
            write_one_mebibyte(&writer_appliance, inode, handle, ordinal, &block);
        }
        finished_tx
            .send(())
            .expect("writer completion receiver remains available");
    });
    assert!(
        paused.wait_until_reached(STORAGE_REACH_TIMEOUT),
        "single-stream publication reaches blocked Container durability"
    );
    assert!(
        finished_rx.recv_timeout(Duration::from_secs(1)).is_err(),
        "fixture must fill the bounded same-inode publication queue"
    );

    let status_appliance = Arc::clone(&appliance);
    let (status_tx, status_rx) = mpsc::channel();
    let status_reader = std::thread::spawn(move || {
        let status = status_appliance.write_through_status();
        status_tx
            .send(status)
            .expect("status receiver remains available");
    });
    std::thread::sleep(Duration::from_millis(100));
    paused.resume();
    let status = status_rx
        .recv_timeout(STORAGE_REACH_TIMEOUT)
        .expect("status must release the Registry so a publisher can retire queue pressure");
    assert!(
        status.queued_bytes() != 0,
        "fixture must observe real publication-queue pressure"
    );

    writer
        .join()
        .expect("writer completes after durability resumes");
    status_reader.join().expect("status reader completes");
}

#[test]
fn unlink_sequence_barrier_allows_release_to_finish_after_queued_writes() {
    let paused = PausedStorageIo::before(MemoryStorageIo::new(), StorageOperation::SyncFile);
    let appliance = Arc::new(open_appliance_with_paused_containers(paused.clone()));
    let (inode, handle) = create_file(&appliance, b"unlinked-while-reducing");
    let block = fixture_block();
    for ordinal in 0_u64..34 {
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    assert!(
        paused.wait_until_reached(STORAGE_REACH_TIMEOUT),
        "background reduction must reach blocked durability"
    );
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"unlinked-while-reducing",
            },
        )
        .expect("unlink installs an ordered ingest barrier");

    let release_appliance = Arc::clone(&appliance);
    let (released_tx, released_rx) = mpsc::channel();
    let release = std::thread::spawn(move || {
        release_appliance
            .namespace()
            .dispatch(CALLER, Operation::Release { inode, handle })
            .expect("release crosses the unlink sequence barrier");
        released_tx
            .send(())
            .expect("test receiver remains available");
    });
    assert!(
        released_rx
            .recv_timeout(Duration::from_millis(500))
            .is_err(),
        "release must not skip the queued write or unlink barrier"
    );
    paused.resume();
    assert!(
        released_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
        "release must not wait forever on an unobserved unlink sequence"
    );
    release.join().expect("release thread completes");
}

fn create_file<M, C>(appliance: &DurableNamespace<M, C>, name: &[u8]) -> (InodeId, HandleId)
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
                name,
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create ingest file")
    else {
        panic!("create returned the wrong reply");
    };
    (entry.attr.inode, handle)
}

fn fixture_block() -> Vec<u8> {
    (0_usize..1_048_576)
        .map(|index| {
            u8::try_from((index.wrapping_mul(131) ^ (index >> 9)) % 251)
                .expect("fixture byte is bounded")
        })
        .collect()
}

fn write_fixture<M, C>(
    appliance: &DurableNamespace<M, C>,
    inode: InodeId,
    handle: HandleId,
) -> Vec<u8>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    write_salted_fixture(appliance, inode, handle, 0)
}

fn write_salted_fixture<M, C>(
    appliance: &DurableNamespace<M, C>,
    inode: InodeId,
    handle: HandleId,
    salt: u8,
) -> Vec<u8>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let mut block = fixture_block();
    for byte in &mut block {
        *byte = byte.wrapping_add(salt);
    }
    for ordinal in 0_u64..34 {
        block[0] = u8::try_from(ordinal + u64::from(salt)).expect("fixture ordinal is bounded");
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset: ordinal * 1_048_576,
                    data: &block,
                },
            )
            .expect("append one sequential block");
    }
    block[0] = 4_u8.wrapping_add(salt);
    block[..4_096].to_vec()
}

fn read_fixture<M, C>(
    appliance: &DurableNamespace<M, C>,
    inode: InodeId,
    handle: HandleId,
) -> Vec<u8>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let Reply::Data(bytes) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle,
                offset: 4 * 1_048_576,
                length: 4_096,
            },
        )
        .expect("read externalized data")
    else {
        panic!("read returned the wrong reply");
    };
    bytes
}

fn write_one_mebibyte<M, C>(
    appliance: &DurableNamespace<M, C>,
    inode: InodeId,
    handle: HandleId,
    ordinal: u64,
    block: &[u8],
) where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: ordinal * 1_048_576,
                data: block,
            },
        )
        .expect("append one interleaved block");
}

fn fence_ingest<M, C>(appliance: &DurableNamespace<M, C>, inode: InodeId, handle: HandleId)
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Sync {
                inode,
                handle,
                data_only: true,
            },
        )
        .expect("sync fences accepted asynchronous ingest jobs");
}

#[test]
fn sequential_writes_publish_reduced_data_before_the_namespace_commit() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"long-stream");
    let expected = write_fixture(&appliance, inode, handle);
    fence_ingest(&appliance, inode, handle);

    let staged = appliance.write_through_status();
    assert_eq!(staged.sealed_uncommitted_containers(), 1);
    assert!(
        staged.buffered_bytes() <= 32 * 1_024 * 1_024 + 256 * 1_024,
        "the next Container plus one SeqCDC tail is the hard staging bound: {staged:?}"
    );
    assert!(
        appliance.namespace().checkpointable_dirty_payload_bytes() < 4 * 1_024 * 1_024,
        "durable verified chunks must replace the resident POSIX dirty copies"
    );
    assert!(!staged.degraded());
    assert_eq!(read_fixture(&appliance, inode, handle), expected);

    let committed = appliance
        .checkpoint_profiled()
        .expect("commit the pre-published stream")
        .expect("stream has one dirty generation");
    assert!(
        committed.metrics().recipe_reuse_bytes() >= 30 * 1_024 * 1_024,
        "checkpoint must adopt the verified write-through recipe without rereading its bytes"
    );
    assert!(
        committed.metrics().checkpoint_rechunk_bytes() <= 4 * 1_024 * 1_024,
        "only the incomplete SeqCDC tail may be re-read and rechunked"
    );
    assert_eq!(
        appliance
            .write_through_status()
            .sealed_uncommitted_containers(),
        1,
        "the post-cut partial Container remains trigger evidence until an empty cut proves it stale"
    );
    assert!(
        appliance
            .checkpoint_profiled()
            .expect("clear stale post-cut Container evidence")
            .is_none()
    );
    assert_eq!(
        appliance
            .write_through_status()
            .sealed_uncommitted_containers(),
        0
    );
    assert_eq!(read_fixture(&appliance, inode, handle), expected);

    let (duplicate_inode, duplicate_handle) = create_file(&appliance, b"second-long-stream");
    assert_eq!(
        write_fixture(&appliance, duplicate_inode, duplicate_handle),
        expected
    );
    fence_ingest(&appliance, duplicate_inode, duplicate_handle);
    assert!(
        appliance.namespace().checkpointable_dirty_payload_bytes() < 4 * 1_024 * 1_024,
        "checkpoint-spanning Exact hits must also release resident POSIX dirty copies"
    );
    assert_eq!(
        appliance
            .write_through_status()
            .sealed_uncommitted_containers(),
        0,
        "an Exact duplicate stream must not publish another Container"
    );
    assert_eq!(
        read_fixture(&appliance, duplicate_inode, duplicate_handle),
        expected
    );
}

#[test]
fn partial_update_rechunks_only_the_affected_recipe_and_recovers_byte_exact() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let appliance = open_appliance_on(metadata.clone(), containers.clone(), MemoryStorageIo::new());
    let (inode, handle) = create_file(&appliance, b"recipe-boundary");
    let mut block = fixture_block();
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(34 * 1_048_576)
        .expect("fixture allocation is bounded");
    for ordinal in 0_u64..34 {
        block[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
        expected.extend_from_slice(&block);
    }
    fence_ingest(&appliance, inode, handle);
    assert!(
        appliance.namespace().checkpointable_dirty_payload_bytes() < 4 * 1_024 * 1_024,
        "the stable write-through prefix must already be externalized"
    );

    let changed_offset = 4 * 1_048_576 + 12_345;
    write_at(
        &appliance,
        inode,
        handle,
        u64::try_from(changed_offset).expect("fixture offset fits u64"),
        b"X",
    );
    expected[changed_offset] = b'X';

    let committed = appliance
        .checkpoint_profiled()
        .expect("commit the partially overwritten recipe")
        .expect("updated stream has one dirty generation");
    assert!(
        committed.metrics().recipe_reuse_bytes() >= 29 * 1_024 * 1_024,
        "unaffected complete recipes must remain directly reusable"
    );
    assert!(
        committed.metrics().checkpoint_rechunk_bytes() <= 4 * 1_024 * 1_024,
        "the incomplete tail plus the one split Chunk bound checkpoint rereads: {:?}",
        committed.metrics()
    );
    drop(appliance);
    metadata.crash();
    containers.crash();

    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, checkpoint_policy_set_v1()),
        &ContainerRepository::new(containers),
    )
    .expect("recover recipe-backed generation")
    .expect("committed generation exists");
    assert_eq!(read_named_all(&recovered, b"recipe-boundary"), expected);
}

#[test]
fn interleaved_files_keep_independent_bounded_ingest_lanes() {
    let appliance = open_appliance();
    let (inode_a, handle_a) = create_file(&appliance, b"stream-a");
    let (inode_b, handle_b) = create_file(&appliance, b"stream-b");
    let mut block_a = fixture_block();
    let mut block_b = fixture_block();
    for byte in &mut block_b {
        *byte = byte.wrapping_add(17);
    }

    for ordinal in 0_u64..34 {
        block_a[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        block_b[0] = u8::try_from(ordinal + 67).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode_a, handle_a, ordinal, &block_a);
        write_one_mebibyte(&appliance, inode_b, handle_b, ordinal, &block_b);
    }
    fence_ingest(&appliance, inode_a, handle_a);
    fence_ingest(&appliance, inode_b, handle_b);

    let staged = appliance.write_through_status();
    assert_eq!(staged.active_lanes(), 2);
    assert_eq!(staged.sealed_uncommitted_containers(), 2);
    assert!(
        staged.buffered_bytes() <= 2 * (32 * 1_024 * 1_024 + 256 * 1_024),
        "two lanes must each retain at most one Container target plus one CDC suffix: {staged:?}"
    );
    assert!(
        appliance.namespace().checkpointable_dirty_payload_bytes() < 8 * 1_024 * 1_024,
        "both durable lanes must release their resident POSIX dirty copies"
    );
    block_a[0] = 4;
    block_b[0] = 71;
    assert_eq!(
        read_fixture(&appliance, inode_a, handle_a),
        block_a[..4_096]
    );
    assert_eq!(
        read_fixture(&appliance, inode_b, handle_b),
        block_b[..4_096]
    );
}

#[test]
fn parallel_lanes_publish_one_complete_exact_index_history() {
    let indexes = MemoryStorageIo::new();
    let appliance = open_appliance_on(
        MemoryStorageIo::new(),
        MemoryStorageIo::new(),
        indexes.clone(),
    );
    let (inode_a, handle_a) = create_file(&appliance, b"parallel-a");
    let (inode_b, handle_b) = create_file(&appliance, b"parallel-b");

    let barrier = Barrier::new(2);
    let (expected_a, expected_b) = std::thread::scope(|scope| {
        let writer_a = scope
            .spawn(|| write_salted_fixture_in_lockstep(&appliance, inode_a, handle_a, 0, &barrier));
        let writer_b = scope.spawn(|| {
            write_salted_fixture_in_lockstep(&appliance, inode_b, handle_b, 67, &barrier)
        });
        (
            writer_a.join().expect("parallel writer A must not panic"),
            writer_b.join().expect("parallel writer B must not panic"),
        )
    });
    fence_ingest(&appliance, inode_a, handle_a);
    fence_ingest(&appliance, inode_b, handle_b);

    assert_eq!(
        appliance.exact_index_run_count(),
        2,
        "both L0 publications must survive one serialized activation history"
    );
    assert_eq!(read_fixture(&appliance, inode_a, handle_a), expected_a);
    assert_eq!(read_fixture(&appliance, inode_b, handle_b), expected_b);
    assert!(!appliance.exact_index_degraded());
    let recovered = ExactIndexRunRepository::new(indexes)
        .recover_active()
        .expect("recover the serialized activation history")
        .expect("parallel publishers activated one durable Run Set");
    assert_eq!(recovered.run_count(), 2);
}

#[test]
fn frozen_cut_commits_while_the_next_epoch_stays_live_and_recovers_in_order() {
    let metadata = MemoryStorageIo::new();
    let containers = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::new();
    let appliance = open_appliance_on(metadata.clone(), containers.clone(), indexes);
    let (inode, handle) = create_file(&appliance, b"epochs");
    write_at(&appliance, inode, handle, 0, b"generation-one");

    let frozen = appliance
        .namespace()
        .begin_commit()
        .expect("freeze the first epoch")
        .expect("the first epoch is dirty");
    write_at(&appliance, inode, handle, 0, b"generation-two");

    let first = appliance
        .checkpoint()
        .expect("commit the already-frozen epoch")
        .expect("the frozen epoch needs one generation");
    assert_eq!(
        read_named(appliance.namespace(), b"epochs"),
        b"generation-two"
    );
    assert_eq!(
        appliance.namespace().checkpointable_dirty_payload_bytes(),
        14,
        "the post-cut write remains the active dirty epoch"
    );
    let recovered_first = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata.clone(), checkpoint_policy_set_v1()),
        &ContainerRepository::new(containers.clone()),
    )
    .expect("recover first committed epoch")
    .expect("the first generation exists");
    assert_eq!(read_named(&recovered_first, b"epochs"), b"generation-one");

    let successor = appliance
        .namespace()
        .begin_commit()
        .expect("freeze the successor epoch")
        .expect("the successor epoch is dirty");
    assert_ne!(successor.token(), frozen.token());
    let second = appliance
        .checkpoint()
        .expect("commit the active successor epoch")
        .expect("the successor epoch needs one generation");
    assert_eq!(second.generation(), first.generation() + 1);
    metadata.crash();
    containers.crash();
    let recovered_second = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, checkpoint_policy_set_v1()),
        &ContainerRepository::new(containers),
    )
    .expect("recover successor epoch after crash")
    .expect("the successor generation exists");
    assert_eq!(read_named(&recovered_second, b"epochs"), b"generation-two");
}

#[test]
fn ninth_interleaved_stream_does_not_fall_back_to_the_resident_dirty_guard() {
    let appliance = open_appliance();
    let mut streams = Vec::new();
    let mut blocks = Vec::new();
    for ordinal in 0_u8..9 {
        let name = format!("stream-{ordinal}");
        streams.push(create_file(&appliance, name.as_bytes()));
        let mut block = fixture_block();
        for byte in &mut block {
            *byte = byte.wrapping_add(ordinal.wrapping_mul(17));
        }
        blocks.push(block);
    }

    for write_ordinal in 0_u64..33 {
        for ((inode, handle), block) in streams.iter().copied().zip(&mut blocks) {
            block[0] = u8::try_from(write_ordinal)
                .expect("fixture ordinal is bounded")
                .wrapping_add(u8::try_from(inode.get()).expect("fixture inode fits u8"));
            write_one_mebibyte(&appliance, inode, handle, write_ordinal, block);
        }
    }
    for (inode, handle) in streams.iter().copied() {
        fence_ingest(&appliance, inode, handle);
    }

    let dirty = appliance.namespace().checkpointable_dirty_payload_bytes();
    assert!(
        dirty < 32 * 1_024 * 1_024,
        "the ninth hot stream must release stable chunks instead of accumulating {dirty} resident bytes"
    );
}

#[test]
fn container_crossing_a_frozen_cut_releases_active_and_reuses_the_frozen_prefix() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"cross-cut");
    let mut block = fixture_block();
    for ordinal in 0_u64..16 {
        block[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    let _frozen = appliance
        .namespace()
        .begin_commit()
        .expect("freeze the partial Container")
        .expect("the prefix is dirty");

    for ordinal in 16_u64..50 {
        block[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    fence_ingest(&appliance, inode, handle);

    let dirty = appliance.namespace().checkpointable_dirty_payload_bytes();
    assert!(
        dirty < 20 * 1_024 * 1_024,
        "valid post-cut chunks must externalize even when the same Container batch begins in the frozen epoch: {dirty} resident bytes"
    );
    let committed = appliance
        .checkpoint_profiled()
        .expect("commit the cross-cut frozen prefix")
        .expect("the frozen prefix needs one generation");
    assert!(
        committed.metrics().recipe_reuse_bytes() >= 15 * 1_024 * 1_024,
        "durable Chunks completed after the cut must still become frozen-prefix recipes: {:?}",
        committed.metrics()
    );
    assert!(
        committed.metrics().checkpoint_rechunk_bytes() <= 1_024 * 1_024,
        "only the Chunk intersecting the cut may require bounded replay: {:?}",
        committed.metrics()
    );
}

#[test]
fn checkpoint_flushes_stable_partial_lane_before_forming_the_frozen_cut() {
    let appliance = open_appliance();
    let (inode, handle) = create_file(&appliance, b"partial-lane-cut");
    let mut block = fixture_block();
    for ordinal in 0_u64..16 {
        block[0] = u8::try_from(ordinal).expect("fixture ordinal is bounded");
        write_one_mebibyte(&appliance, inode, handle, ordinal, &block);
    }
    fence_ingest(&appliance, inode, handle);
    assert_eq!(
        appliance
            .write_through_status()
            .sealed_uncommitted_containers(),
        0,
        "the lane must still be below its normal Container flush threshold"
    );

    let committed = appliance
        .checkpoint_profiled()
        .expect("checkpoint the partial Ingest Lane")
        .expect("the partial lane has one dirty generation");
    assert!(
        committed.metrics().recipe_reuse_bytes() >= 15 * 1_024 * 1_024,
        "stable partial-lane Chunks must become recipes before the cut: {:?}",
        committed.metrics()
    );
    assert!(
        committed.metrics().checkpoint_rechunk_bytes() <= 1_024 * 1_024,
        "only the bounded incomplete CDC suffix may be rechunked: {:?}",
        committed.metrics()
    );
}

fn write_at(appliance: &Appliance, inode: InodeId, handle: HandleId, offset: u64, data: &[u8]) {
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset,
                data,
            },
        )
        .expect("write epoch bytes");
}

fn read_named(namespace: &Namespace, name: &[u8]) -> Vec<u8> {
    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name,
            },
        )
        .expect("lookup epoch file")
    else {
        panic!("lookup returned the wrong reply");
    };
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open epoch file")
    else {
        panic!("open returned the wrong reply");
    };
    let Reply::Data(bytes) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: u32::try_from(entry.attr.size).expect("fixture length fits u32"),
            },
        )
        .expect("read epoch file")
    else {
        panic!("read returned the wrong reply");
    };
    bytes
}

fn read_named_all(namespace: &Namespace, name: &[u8]) -> Vec<u8> {
    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name,
            },
        )
        .expect("lookup complete fixture")
    else {
        panic!("lookup returned the wrong reply");
    };
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open complete fixture")
    else {
        panic!("open returned the wrong reply");
    };
    let mut output = Vec::new();
    output
        .try_reserve_exact(usize::try_from(entry.attr.size).expect("fixture size fits usize"))
        .expect("fixture output allocation is bounded");
    let mut offset = 0_u64;
    while offset < entry.attr.size {
        let length = (entry.attr.size - offset).min(1_048_576);
        let Reply::Data(bytes) = namespace
            .dispatch(
                CALLER,
                Operation::Read {
                    inode: entry.attr.inode,
                    handle,
                    offset,
                    length: u32::try_from(length).expect("bounded fixture read fits u32"),
                },
            )
            .expect("read complete fixture")
        else {
            panic!("read returned the wrong reply");
        };
        assert_eq!(
            u64::try_from(bytes.len()).expect("fixture read length fits u64"),
            length,
            "recovered reads must not return an early EOF"
        );
        output.extend_from_slice(&bytes);
        offset = offset
            .checked_add(length)
            .expect("fixture read cursor cannot overflow");
    }
    output
}

fn write_salted_fixture_in_lockstep(
    appliance: &Appliance,
    inode: InodeId,
    handle: HandleId,
    salt: u8,
    barrier: &Barrier,
) -> Vec<u8> {
    let mut block = fixture_block();
    for byte in &mut block {
        *byte = byte.wrapping_add(salt);
    }
    for ordinal in 0_u64..34 {
        block[0] = u8::try_from(ordinal + u64::from(salt)).expect("fixture ordinal is bounded");
        barrier.wait();
        write_one_mebibyte(appliance, inode, handle, ordinal, &block);
        barrier.wait();
    }
    block[0] = 4_u8.wrapping_add(salt);
    block[..4_096].to_vec()
}
