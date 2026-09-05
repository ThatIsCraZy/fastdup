use std::path::{Path, PathBuf};

use fastdup_store::{FsStorageIo, MAX_STORAGE_RANGE_BYTES, StorageIo};

fn test_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("storage-range-{}", std::process::id()))
}

#[test]
fn filesystem_adapter_reads_only_one_bounded_exact_range() {
    let root = test_root();
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let storage = FsStorageIo::open(&root).expect("open workspace-local storage adapter");
    let bytes = (0_u16..8_192)
        .map(|value| value.to_le_bytes()[0])
        .collect::<Vec<_>>();
    storage.create_new("range.fixture").expect("create fixture");
    storage
        .write_at("range.fixture", 0, &bytes)
        .expect("write fixture");
    storage
        .set_len("range.fixture", 8_192)
        .expect("finalize fixture length");

    assert_eq!(
        storage
            .object_len("range.fixture")
            .expect("fixture metadata is readable"),
        8_192
    );
    assert_eq!(
        storage
            .read_exact_at("range.fixture", 4_090, 32)
            .expect("in-range exact read succeeds"),
        bytes[4_090..4_122]
    );
    assert_eq!(
        storage
            .read_exact_at("range.fixture", 8_190, 4)
            .expect_err("short reads must fail instead of returning partial evidence")
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        storage
            .read_exact_at("range.fixture", 0, MAX_STORAGE_RANGE_BYTES + 1)
            .expect_err("adapter must reject an attacker-sized allocation before reading")
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[test]
fn cached_container_fds_are_invalidated_across_adapters_before_mutation_and_unlink() {
    let root = test_root().with_file_name(format!("fd-cache-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let first = FsStorageIo::open(&root).unwrap();
    let other = FsStorageIo::open(&root).unwrap();
    let name = "abababababababababababababababab.fdc";
    if first.exists(name).unwrap() {
        first.remove_file(name).unwrap();
    }
    first.create_new(name).unwrap();
    first.write_at(name, 0, b"abcdefgh").unwrap();
    let a = first.open_read_range(name, 0, 8).unwrap();
    let b = other.open_read_range(name, 0, 8).unwrap();
    assert!(
        std::sync::Arc::ptr_eq(&a, &b),
        "adapters share the cached descriptor"
    );
    let weak = std::sync::Arc::downgrade(&a);
    drop(a);
    drop(b);
    other.set_len(name, 4).unwrap();
    assert!(
        weak.upgrade().is_none(),
        "mutation releases the cached descriptor"
    );
    assert_eq!(
        first.read_exact_at(name, 0, 8).unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert_eq!(first.read_exact_at(name, 0, 4).unwrap(), b"abcd");
    let handle = first.open_read_range(name, 0, 4).unwrap();
    let weak = std::sync::Arc::downgrade(&handle);
    drop(handle);
    other.remove_file(name).unwrap();
    assert!(
        weak.upgrade().is_none(),
        "GC unlink leaves no cache-held deleted descriptor"
    );
    other.create_new(name).unwrap();
    other.write_at(name, 0, b"new").unwrap();
    assert_eq!(first.read_exact_at(name, 0, 3).unwrap(), b"new");
    other.remove_file(name).unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[test]
fn unrelated_warm_and_cold_reads_progress_while_one_name_is_mutating() {
    use std::sync::mpsc;
    use std::time::Duration;
    let root = test_root().with_file_name(format!("fd-concurrency-{}", std::process::id()));
    let storage = FsStorageIo::open(&root).unwrap();
    let names = [
        "11111111111111111111111111111111.fdc",
        "22222222222222222222222222222222.fdc",
        "33333333333333333333333333333333.fdc",
    ];
    for name in names {
        if storage.exists(name).unwrap() {
            storage.remove_file(name).unwrap();
        }
        storage.create_new(name).unwrap();
        storage.write_at(name, 0, b"original").unwrap();
    }
    storage.read_exact_at(names[1], 0, 8).unwrap();
    let (entered, started) = mpsc::channel();
    let (release, proceed) = mpsc::channel();
    let (completed, reads) = mpsc::channel();
    std::thread::scope(|scope| {
        let storage = &storage;
        scope.spawn(move || {
            storage
                .with_file_mutation(names[0], || {
                    entered.send(()).unwrap();
                    proceed.recv().unwrap();
                    Err::<(), _>(std::io::Error::other("injected backend failure"))
                })
                .unwrap_err()
        });
        started.recv().unwrap();
        scope.spawn(move || {
            let other = FsStorageIo::open(&root).unwrap();
            let warm = other.read_exact_at(names[1], 0, 8).unwrap();
            let cold = other.read_exact_at(names[2], 0, 8).unwrap();
            completed.send((warm, cold)).unwrap();
        });
        let outcome = reads.recv_timeout(Duration::from_secs(2));
        release.send(()).unwrap();
        assert_eq!(
            outcome.unwrap(),
            (b"original".to_vec(), b"original".to_vec())
        );
    });
    let lease = storage.lease_immutable_file(names[0], 8).unwrap().unwrap();
    assert_eq!(
        storage.set_len(names[0], 0).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        storage
            .publish_noreplace(names[1], names[0])
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::PermissionDenied
    );
    drop(lease);
    for name in names {
        storage.remove_file(name).unwrap();
    }
    std::fs::remove_dir(storage.root()).unwrap();
}

#[test]
fn opposed_rename_requests_acquire_names_in_one_order() {
    use std::sync::{Arc, Barrier, mpsc};
    let root = test_root().with_file_name(format!("fd-rename-order-{}", std::process::id()));
    let storage = FsStorageIo::open(&root).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let (send, receive) = mpsc::channel();
    let mut threads = Vec::new();
    for (old, new) in [("first", "second"), ("second", "first")] {
        let barrier = Arc::clone(&barrier);
        let storage = storage.clone();
        let send = send.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..1000 {
                storage.with_file_rename(old, new, || Ok(())).unwrap();
            }
            send.send(()).unwrap();
        }));
    }
    barrier.wait();
    for _ in 0..2 {
        receive
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
    }
    for thread in threads {
        thread.join().unwrap();
    }
    std::fs::remove_dir(root).unwrap();
}

#[test]
fn sharded_fd_cache_keeps_one_global_bound_and_rename_drops_old_name() {
    use std::sync::Arc;
    let root = test_root().with_file_name(format!("fd-capacity-{}", std::process::id()));
    let storage = FsStorageIo::open(&root).unwrap();
    let mut handles = Vec::new();
    let names = (1..=160)
        .map(|n| format!("{n:032x}.fdc"))
        .collect::<Vec<_>>();
    for name in &names {
        if storage.exists(name).unwrap() {
            storage.remove_file(name).unwrap();
        }
        storage.create_new(name).unwrap();
        storage.write_at(name, 0, b"live").unwrap();
        let file = storage.open_read_range(name, 0, 4).unwrap();
        handles.push(Arc::downgrade(&file));
    }
    assert!(
        handles[..32]
            .iter()
            .all(|handle| handle.upgrade().is_none())
    );
    assert!(
        handles[32..]
            .iter()
            .all(|handle| handle.upgrade().is_some())
    );
    let renamed = "abcdefabcdefabcdefabcdefabcdefab.fdc";
    storage.publish_noreplace(&names[159], renamed).unwrap();
    assert!(handles[159].upgrade().is_none());
    assert_eq!(
        storage.read_exact_at(&names[159], 0, 4).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    assert_eq!(storage.read_exact_at(renamed, 0, 4).unwrap(), b"live");
    for name in &names[..159] {
        storage.remove_file(name).unwrap();
    }
    storage.remove_file(renamed).unwrap();
    assert!(handles.iter().all(|handle| handle.upgrade().is_none()));
    std::fs::remove_dir(root).unwrap();
}

#[test]
fn immutable_lease_keeps_registry_live_after_every_adapter_drops() {
    let root = test_root().with_file_name(format!("fd-lease-registry-{}", std::process::id()));
    let name = "immutable-run.fixture";
    let lease = {
        let storage = FsStorageIo::open(&root).unwrap();
        if storage.exists(name).unwrap() {
            storage.remove_file(name).unwrap();
        }
        storage.create_new(name).unwrap();
        storage.write_at(name, 0, b"mapped bytes").unwrap();
        storage.lease_immutable_file(name, 12).unwrap().unwrap()
    };
    let reopened = FsStorageIo::open(&root).unwrap();
    assert_eq!(
        reopened.set_len(name, 0).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        reopened.write_at(name, 0, b"bad").unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        reopened.remove_file(name).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    drop(lease);
    reopened.remove_file(name).unwrap();
    std::fs::remove_dir(root).unwrap();
}
