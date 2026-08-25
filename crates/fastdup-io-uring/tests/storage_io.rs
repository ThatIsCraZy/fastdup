use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use fastdup_format::ContainerId;
use fastdup_io_uring::{IoUringStorageConfig, IoUringStorageIo};
use fastdup_store::{ContainerRepository, StorageIo};

#[test]
fn ring_setup_failure_is_returned_instead_of_selecting_another_adapter() {
    let root = test_root("required-ring");
    let config = IoUringStorageConfig::new(
        NonZeroU32::MIN,
        NonZeroU64::new(1_024 * 1_024).expect("literal is nonzero"),
    );

    let error = IoUringStorageIo::open(&root, config)
        .expect_err("a ring too small for the wake entry must fail setup");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn dropping_the_last_adapter_stops_the_ring_worker() {
    let root = test_root("worker-shutdown");
    let (finished, completion) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn({
        let root = root.clone();
        move || {
            let storage = IoUringStorageIo::open(&root, IoUringStorageConfig::default())
                .expect("active io_uring backend");
            storage
                .sync_root()
                .expect("exercise the ring before shutdown");
            drop(storage);
            finished.send(()).expect("report completed shutdown");
        }
    });

    completion
        .recv_timeout(Duration::from_secs(2))
        .expect("dropping the adapter must not lose its shutdown wakeup");
    worker.join().expect("shutdown worker did not panic");
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn active_ring_publishes_and_recovers_one_byte_exact_container() {
    let root = test_root("publish");
    let config = IoUringStorageConfig::new(
        NonZeroU32::new(64).expect("literal is nonzero"),
        NonZeroU64::new(128 * 1_024 * 1_024).expect("literal is nonzero"),
    );
    let storage = IoUringStorageIo::open(&root, config).expect("active io_uring backend");

    let repository = ContainerRepository::new(storage.clone());
    let id = ContainerId::new([0x91; 16]).expect("nonzero Container ID");
    let first = vec![0xA5; 128 * 1_024];
    let second = b"io_uring-publication-proof".repeat(4_096);
    repository
        .publish_raw(id, 1, &[&first, &second])
        .expect("publication succeeds");

    drop(repository);
    let reopened = ContainerRepository::new(
        IoUringStorageIo::open(&root, config).expect("reopen active backend"),
    );
    let recovered = reopened.read(id).expect("published container recovers");
    assert_eq!(recovered.chunk_count(), 2);
    assert_eq!(
        recovered.chunk(fastdup_format::ChunkId::of(&first)),
        Some(first.as_slice())
    );
    assert_eq!(
        recovered.chunk(fastdup_format::ChunkId::of(&second)),
        Some(second.as_slice())
    );

    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn concurrent_root_sync_callers_share_only_completed_cohorts() {
    const CALLERS: usize = 32;
    let root = test_root("root-sync-cohort");
    let storage = Arc::new(
        IoUringStorageIo::open(&root, IoUringStorageConfig::default())
            .expect("active io_uring backend"),
    );
    let barrier = Arc::new(Barrier::new(CALLERS));
    let mut callers = Vec::new();
    for _ in 0..CALLERS {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        callers.push(std::thread::spawn(move || {
            barrier.wait();
            storage.sync_root().expect("cohort root sync");
        }));
    }
    for caller in callers {
        caller.join().expect("root-sync caller did not panic");
    }

    let status = storage.status();
    assert_eq!(status.root_sync_callers(), CALLERS as u64);
    assert!(status.root_sync_submissions() >= 1);
    assert!(status.root_sync_submissions() < CALLERS as u64);
    assert_eq!(status.inflight_bytes(), 0);
    assert_eq!(status.submitted_operations(), status.completed_operations());

    drop(storage);
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn oversized_request_is_rejected_before_it_can_exceed_the_byte_budget() {
    let root = test_root("budget");
    let config = IoUringStorageConfig::new(
        NonZeroU32::new(8).expect("literal is nonzero"),
        NonZeroU64::new(1_024).expect("literal is nonzero"),
    );
    let storage = IoUringStorageIo::open(&root, config).expect("active io_uring backend");
    storage.create_new("bounded").expect("create fixture");

    let error = storage
        .write_at("bounded", 0, &[0x5A; 1_025])
        .expect_err("oversized request must fail admission");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    let status = storage.status();
    assert_eq!(status.inflight_bytes(), 0);
    assert_eq!(status.peak_inflight_bytes(), 0);
    assert_eq!(storage.object_len("bounded").expect("fixture length"), 0);

    drop(storage);
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn many_independent_container_publishers_share_one_bounded_ring() {
    const PUBLISHERS: usize = 32;
    let root = test_root("many-publishers");
    let config = IoUringStorageConfig::new(
        NonZeroU32::new(64).expect("literal is nonzero"),
        NonZeroU64::new(1_024 * 1_024).expect("literal is nonzero"),
    );
    let storage = IoUringStorageIo::open(&root, config).expect("active io_uring backend");
    let barrier = Arc::new(Barrier::new(PUBLISHERS));
    let mut publishers = Vec::new();
    for ordinal in 0..PUBLISHERS {
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        publishers.push(std::thread::spawn(move || {
            let mut id = [0_u8; 16];
            id[..8].copy_from_slice(
                &u64::try_from(ordinal + 1)
                    .expect("publisher ordinal fits u64")
                    .to_le_bytes(),
            );
            let id = ContainerId::new(id).expect("publisher ID is nonzero");
            let payload =
                vec![u8::try_from(ordinal).expect("publisher ordinal fits u8"); 128 * 1_024];
            barrier.wait();
            ContainerRepository::new(storage)
                .publish_raw(
                    id,
                    u64::try_from(ordinal + 1).expect("publisher generation fits u64"),
                    &[&payload],
                )
                .expect("parallel publication succeeds");
        }));
    }
    for publisher in publishers {
        publisher.join().expect("publisher did not panic");
    }

    let recovered = ContainerRepository::new(storage.clone())
        .recover_published()
        .expect("all published Containers recover");
    assert_eq!(recovered.len(), PUBLISHERS);
    let status = storage.status();
    assert_eq!(status.inflight_bytes(), 0);
    assert!(status.peak_inflight_bytes() <= status.max_inflight_bytes());
    assert_eq!(status.submitted_operations(), status.completed_operations());
    assert!(status.submitted_operations() >= (PUBLISHERS * 7) as u64);

    drop(storage);
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn prepared_container_transfers_owned_image_once_into_the_ring_publisher() {
    let root = test_root("owned-publication");
    let storage = IoUringStorageIo::open(&root, IoUringStorageConfig::default())
        .expect("active io_uring backend");
    let repository = ContainerRepository::new(storage.clone());
    let id = ContainerId::new([0xB7; 16]).expect("nonzero Container ID");
    let chunk = b"owned-publication-image".repeat(8_192);
    let region = [chunk.as_slice()];
    let regions = [region.as_slice()];
    let prepared = ContainerRepository::<IoUringStorageIo>::prepare_adaptive_regions_parallel(
        id,
        19,
        &regions,
        NonZeroUsize::new(2).expect("literal is nonzero"),
    )
    .expect("prepare Container image");

    let (verified, metrics) = repository
        .publish_prepared_adaptive_profiled(prepared)
        .expect("owned publication succeeds");
    assert_eq!(verified.header().container_id(), id);
    let readable = repository
        .read(id)
        .expect("published Container remains readable through the full reader");
    assert_eq!(
        readable.chunk(fastdup_format::ChunkId::of(&chunk)),
        Some(chunk.as_slice())
    );
    let status = storage.status();
    assert_eq!(status.owned_publications_started(), 1);
    assert_eq!(status.owned_publications_completed(), 1);
    assert_eq!(status.borrowed_write_copy_bytes(), 0);
    assert_eq!(status.peak_inflight_bytes(), metrics.file_bytes());
    assert_eq!(status.inflight_bytes(), 0);

    drop(repository);
    drop(storage);
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn one_large_publication_uses_writer_evidence_without_a_second_verification() {
    let root = test_root("exact-writer-image-proof");
    let storage = IoUringStorageIo::open(&root, IoUringStorageConfig::default())
        .expect("active io_uring backend");
    let repository = ContainerRepository::new(storage.clone());
    let payload = pseudorandom_bytes(8 * 1_024 * 1_024, 0x87ad_190e_44bc_f217);
    let chunks = payload.chunks(256 * 1_024).collect::<Vec<_>>();
    let regions = chunks.chunks(2).collect::<Vec<_>>();
    let prepared = ContainerRepository::<IoUringStorageIo>::prepare_adaptive_regions_parallel(
        ContainerId::new([0xD3; 16]).expect("fixture Container ID is nonzero"),
        29,
        &regions,
        NonZeroUsize::MIN,
    )
    .expect("prepare large fixture Container");

    repository
        .publish_prepared_adaptive_profiled(prepared)
        .expect("publish and verify large fixture Container");

    let status = storage.status();
    assert_eq!(status.inflight_bytes(), 0);

    drop(repository);
    drop(storage);
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn owned_publication_rejected_by_budget_creates_no_temporary_name() {
    let root = test_root("owned-budget-rejection");
    let config = IoUringStorageConfig::new(
        NonZeroU32::new(8).expect("literal is nonzero"),
        NonZeroU64::new(1_024).expect("literal is nonzero"),
    );
    let storage = IoUringStorageIo::open(&root, config).expect("active io_uring backend");
    let id = ContainerId::new([0xC1; 16]).expect("nonzero Container ID");
    let error = ContainerRepository::new(storage.clone())
        .publish_raw(id, 23, &[b"larger-than-the-ring-publication-budget"])
        .expect_err("publication larger than its complete budget is rejected");
    assert_eq!(
        error.to_string(),
        "container I/O failed: one io_uring buffer exceeds the in-flight byte limit"
    );
    assert!(
        storage
            .list_names()
            .expect("list internal names")
            .is_empty()
    );
    assert_eq!(storage.status().owned_publications_started(), 0);

    drop(storage);
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn prepared_publications_do_not_repeat_encoder_work_on_a_cpu_pool() {
    const PUBLISHERS: usize = 8;
    const PAYLOAD_BYTES: usize = 8 * 1_024 * 1_024;
    let root = test_root("parallel-verifier-pool");
    let config = IoUringStorageConfig::new(
        NonZeroU32::new(64).expect("literal is nonzero"),
        NonZeroU64::new(128 * 1_024 * 1_024).expect("literal is nonzero"),
    );
    let storage = IoUringStorageIo::open(&root, config).expect("active io_uring backend");
    let barrier = Arc::new(Barrier::new(PUBLISHERS));
    let mut publishers = Vec::with_capacity(PUBLISHERS);
    for ordinal in 0..PUBLISHERS {
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        publishers.push(std::thread::spawn(move || {
            let payload = pseudorandom_bytes(
                PAYLOAD_BYTES,
                u64::try_from(ordinal + 1).expect("ordinal fits u64"),
            );
            let chunks: Vec<&[u8]> = payload.chunks(256 * 1_024).collect();
            let regions: Vec<&[&[u8]]> = chunks.chunks(2).collect();
            let mut id = [0_u8; 16];
            id[..8].copy_from_slice(
                &u64::try_from(ordinal + 1)
                    .expect("ordinal fits u64")
                    .to_le_bytes(),
            );
            let id = ContainerId::new(id).expect("publisher ID is nonzero");
            let prepared =
                ContainerRepository::<IoUringStorageIo>::prepare_adaptive_regions_parallel(
                    id,
                    u64::try_from(ordinal + 1).expect("generation fits u64"),
                    &regions,
                    NonZeroUsize::new(2).expect("literal is nonzero"),
                )
                .expect("prepare independent Container");
            barrier.wait();
            let (verified, _) = ContainerRepository::new(storage)
                .publish_prepared_adaptive_profiled(prepared)
                .expect("parallel verified publication succeeds");
            assert_eq!(verified.header().container_id(), id);
        }));
    }
    for publisher in publishers {
        publisher.join().expect("publisher did not panic");
    }

    let status = storage.status();
    assert_eq!(status.inflight_bytes(), 0);

    drop(storage);
    std::fs::remove_dir_all(root).expect("remove test root");
}

#[test]
fn root_sync_progresses_while_container_publication_is_running() {
    const PAYLOAD_BYTES: usize = 63 * 1_024 * 1_024;
    let root = test_root("root-sync-during-verification");
    let config = IoUringStorageConfig::new(
        NonZeroU32::new(32).expect("literal is nonzero"),
        NonZeroU64::new(128 * 1_024 * 1_024).expect("literal is nonzero"),
    );
    let storage = IoUringStorageIo::open(&root, config).expect("active io_uring backend");
    let publisher_storage = storage.clone();
    let publisher = std::thread::spawn(move || {
        let payload = pseudorandom_bytes(PAYLOAD_BYTES, 0x100);
        let chunks = payload.chunks(256 * 1_024).collect::<Vec<_>>();
        let regions = chunks.chunks(2).collect::<Vec<_>>();
        let prepared = ContainerRepository::<IoUringStorageIo>::prepare_adaptive_regions_parallel(
            ContainerId::new([0xA4; 16]).expect("publisher ID is nonzero"),
            0x100,
            &regions,
            NonZeroUsize::MIN,
        )
        .expect("prepare independent Container");
        ContainerRepository::new(publisher_storage)
            .publish_prepared_adaptive_profiled(prepared)
            .expect("verified publication succeeds");
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    while storage.status().owned_publications_started() == 0 {
        assert!(Instant::now() < deadline, "publication did not start");
        std::thread::yield_now();
    }

    storage
        .sync_root()
        .expect("independent root sync progresses through the active ring");
    publisher.join().expect("publisher did not panic");
    drop(storage);
    std::fs::remove_dir_all(root).expect("remove test root");
}

fn pseudorandom_bytes(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn test_root(case: &str) -> std::path::PathBuf {
    let root = std::path::PathBuf::from(
        std::env::var_os("TMPDIR").expect("tests require workspace-local TMPDIR"),
    )
    .join(format!(
        "fastdup-io-uring-{case}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    if root.try_exists().expect("inspect test root") {
        std::fs::remove_dir_all(&root).expect("remove stale test root");
    }
    root
}
