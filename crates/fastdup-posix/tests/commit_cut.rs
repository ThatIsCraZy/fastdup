use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use fastdup_posix::{
    CommittedEntry, CommittedFile, CommittedFileInstall, CommittedInode,
    CommittedNamespaceSnapshot, ExternalizedExtent, InodeId, MutationObserver, MutationPayload,
    Namespace, NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply,
    RequestContext,
};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 93,
};

#[derive(Debug)]
struct BytesFile(Vec<u8>);

impl CommittedFile for BytesFile {
    fn logical_size(&self) -> u64 {
        u64::try_from(self.0.len()).expect("ASSERT: fixture length fits u64")
    }

    fn allocated_bytes(&self) -> u64 {
        self.logical_size()
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        let end = offset.saturating_add(length).min(self.logical_size());
        Ok(end.saturating_sub(offset.min(end)))
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        let start = usize::try_from(offset).map_err(|_| PosixError::FileTooLarge)?;
        if start >= self.0.len() {
            return Ok(Vec::new());
        }
        let end = start
            .saturating_add(usize::try_from(length).expect("ASSERT: u32 fits usize"))
            .min(self.0.len());
        Ok(self.0[start..end].to_vec())
    }
}

#[derive(Debug)]
struct SegmentedIdentityFile {
    bytes: Vec<u8>,
    contiguous_checks: AtomicUsize,
    segmented_checks: AtomicUsize,
}

impl CommittedFile for SegmentedIdentityFile {
    fn logical_size(&self) -> u64 {
        u64::try_from(self.bytes.len()).expect("ASSERT: fixture length fits u64")
    }

    fn allocated_bytes(&self) -> u64 {
        self.logical_size()
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        let end = offset.saturating_add(length).min(self.logical_size());
        Ok(end.saturating_sub(offset.min(end)))
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        let start = usize::try_from(offset).map_err(|_| PosixError::FileTooLarge)?;
        let end = start
            .saturating_add(usize::try_from(length).expect("ASSERT: u32 fits usize"))
            .min(self.bytes.len());
        Ok(self.bytes.get(start..end).unwrap_or_default().to_vec())
    }

    fn matches_complete_bytes(&self, candidate: &[u8]) -> Result<bool, PosixError> {
        self.contiguous_checks.fetch_add(1, Ordering::Relaxed);
        Ok(candidate == self.bytes)
    }

    fn matches_complete_segments(&self, segments: &[&[u8]]) -> Result<bool, PosixError> {
        self.segmented_checks.fetch_add(1, Ordering::Relaxed);
        let mut expected = self.bytes.as_slice();
        for segment in segments {
            let Some(prefix) = expected.get(..segment.len()) else {
                return Ok(false);
            };
            if *segment != prefix {
                return Ok(false);
            }
            expected = &expected[segment.len()..];
        }
        Ok(expected.is_empty())
    }
}

#[derive(Debug)]
struct BlockingAllocatedFile {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<mpsc::Receiver<()>>,
}

#[derive(Debug)]
struct BlockingMutationObserver {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl MutationObserver for BlockingMutationObserver {
    fn accepted_write(
        &self,
        _inode: InodeId,
        _offset: u64,
        _mutation_sequence: u64,
        _bytes: MutationPayload,
    ) -> Vec<ExternalizedExtent> {
        if let Some(entered) = self
            .entered
            .lock()
            .expect("ASSERT: fixture entered lock poisoned")
            .take()
        {
            entered.send(()).expect("writer announces observer entry");
            self.release
                .lock()
                .expect("ASSERT: fixture release lock poisoned")
                .recv()
                .expect("test releases mutation observer");
        }
        Vec::new()
    }

    fn accepted_truncate(&self, _inode: InodeId, _mutation_sequence: u64, _length: u64) {}
}

#[derive(Debug, Default)]
struct RetainingMutationObserver {
    payloads: Mutex<Vec<MutationPayload>>,
}

impl MutationObserver for RetainingMutationObserver {
    fn accepted_write(
        &self,
        _inode: InodeId,
        _offset: u64,
        _mutation_sequence: u64,
        bytes: MutationPayload,
    ) -> Vec<ExternalizedExtent> {
        self.payloads
            .lock()
            .expect("ASSERT: fixture payload lock poisoned")
            .push(bytes);
        Vec::new()
    }

    fn accepted_truncate(&self, _inode: InodeId, _mutation_sequence: u64, _length: u64) {}
}

#[test]
fn mutation_observer_owns_accepted_bytes_after_write_returns() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let observer = Arc::new(RetainingMutationObserver::default());
    namespace.install_mutation_observer(observer.clone());
    let Reply::Created { entry, handle } = create(&namespace, b"owned-observer-payload") else {
        panic!("ASSERT: create returned the wrong reply variant");
    };

    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"observer-retains-these-bytes",
            },
        )
        .expect("write fixture bytes");

    let payloads = observer
        .payloads
        .lock()
        .expect("ASSERT: fixture payload lock poisoned");
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].as_bytes(), b"observer-retains-these-bytes");
}

#[test]
fn externalization_trusts_adjacent_resident_payload_provenance_without_rehashing() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = create(&namespace, b"segmented-externalization") else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    for (offset, data) in [(0, b"abcd".as_slice()), (4, b"efgh".as_slice())] {
        namespace
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset,
                    data,
                },
            )
            .expect("write one resident segment");
    }
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 8);

    let source = Arc::new(SegmentedIdentityFile {
        bytes: b"abcdefgh".to_vec(),
        contiguous_checks: AtomicUsize::new(0),
        segmented_checks: AtomicUsize::new(0),
    });
    namespace.externalize_verified_extents(vec![
        ExternalizedExtent::new(inode, 0, 2, source.clone()).expect("construct verified extent"),
    ]);

    assert_eq!(source.segmented_checks.load(Ordering::Relaxed), 0);
    assert_eq!(source.contiguous_checks.load(Ordering::Relaxed), 0);
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 0);
    assert_eq!(read_all(&namespace, inode, handle), b"abcdefgh");
}

#[test]
fn externalization_rejects_a_range_changed_after_its_publication_sequence() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = create(&namespace, b"stale-externalization") else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    for (offset, data) in [(0, b"abcdefgh".as_slice()), (2, b"ZZ".as_slice())] {
        namespace
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset,
                    data,
                },
            )
            .expect("write fixture extent");
    }
    let source = Arc::new(SegmentedIdentityFile {
        bytes: b"abcdefgh".to_vec(),
        contiguous_checks: AtomicUsize::new(0),
        segmented_checks: AtomicUsize::new(0),
    });

    namespace.externalize_verified_extents(vec![
        ExternalizedExtent::new(inode, 0, 1, source.clone()).expect("construct stale extent"),
    ]);

    assert_eq!(source.segmented_checks.load(Ordering::Relaxed), 0);
    assert_eq!(source.contiguous_checks.load(Ordering::Relaxed), 0);
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 8);
    assert_eq!(read_all(&namespace, inode, handle), b"abZZefgh");
}

#[test]
fn externalization_ignores_later_mutations_outside_its_range() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = create(&namespace, b"range-externalization") else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    for (offset, data) in [(0, b"abcdefgh".as_slice()), (8, b"ZZ".as_slice())] {
        namespace
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset,
                    data,
                },
            )
            .expect("write fixture extent");
    }
    let source = Arc::new(SegmentedIdentityFile {
        bytes: b"abcdefgh".to_vec(),
        contiguous_checks: AtomicUsize::new(0),
        segmented_checks: AtomicUsize::new(0),
    });

    namespace.externalize_verified_extents(vec![
        ExternalizedExtent::new(inode, 0, 1, source.clone()).expect("construct current extent"),
    ]);

    assert_eq!(source.segmented_checks.load(Ordering::Relaxed), 0);
    assert_eq!(source.contiguous_checks.load(Ordering::Relaxed), 0);
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 2);
    assert_eq!(read_all(&namespace, inode, handle), b"abcdefghZZ");
}

impl CommittedFile for BlockingAllocatedFile {
    fn logical_size(&self) -> u64 {
        1
    }

    fn allocated_bytes(&self) -> u64 {
        1
    }

    fn allocated_bytes_in_range(&self, _offset: u64, _length: u64) -> Result<u64, PosixError> {
        if let Some(entered) = self
            .entered
            .lock()
            .expect("ASSERT: fixture entered lock poisoned")
            .take()
        {
            entered.send(()).expect("writer announces allocation read");
            self.release
                .lock()
                .expect("ASSERT: fixture release lock poisoned")
                .recv()
                .expect("test releases allocation read");
        }
        Ok(1)
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        if offset == 0 && length > 0 {
            Ok(vec![b'B'])
        } else {
            Ok(Vec::new())
        }
    }
}

#[test]
fn commit_cut_waits_for_the_admitted_writes_mutation_observer() {
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"observer-fence",
                mode: 0o640,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create fixture")
    else {
        panic!("ASSERT: create must return Created");
    };
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    namespace.install_mutation_observer(Arc::new(BlockingMutationObserver {
        entered: Mutex::new(Some(entered_tx)),
        release: Mutex::new(release_rx),
    }));

    let writer_namespace = Arc::clone(&namespace);
    let writer = std::thread::spawn(move || {
        writer_namespace.dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"fenced bytes",
            },
        )
    });
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("write reaches its mutation observer");

    let cut_namespace = Arc::clone(&namespace);
    let (cut_tx, cut_rx) = mpsc::channel();
    let cut = std::thread::spawn(move || {
        cut_tx
            .send(cut_namespace.begin_commit())
            .expect("return commit result");
    });
    assert!(
        matches!(
            cut_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "a Frozen Commit Cut must not overtake an admitted write's observer"
    );

    release_tx.send(()).expect("release mutation observer");
    writer
        .join()
        .expect("writer thread must not panic")
        .expect("write completes");
    let commit = cut_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("cut completes after observer")
        .expect("cut succeeds")
        .expect("the observed write is dirty");
    assert_eq!(commit.inodes().len(), 1);
    assert_eq!(commit.inodes()[0].logical_size(), 12);
    cut.join().expect("cut thread must not panic");
}

#[test]
fn commit_cut_is_retryable_and_later_write_remains_live_after_install() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"vm-image",
                mode: 0o640,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create file before first cut")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"abcdefgh",
            },
        )
        .expect("write first commit prefix");

    let first = namespace
        .begin_commit()
        .expect("cut accepted mutations")
        .expect("create and write require a commit");
    assert_eq!(first.namespace_mutation_sequence(), 1);
    assert_eq!(first.inodes().len(), 1);
    assert_eq!(first.inodes()[0].inode(), inode);
    assert_eq!(first.inodes()[0].mutation_sequence(), 1);
    assert_eq!(first.inodes()[0].read_at(0, 32), Ok(b"abcdefgh".to_vec()));
    assert_eq!(changed_ranges(&first.inodes()[0]), vec![(0, 8)]);

    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 2,
                data: b"XY",
            },
        )
        .expect("write after cut remains admitted");
    let retried = namespace
        .begin_commit()
        .expect("retry returns the outstanding cut")
        .expect("outstanding cut exists");
    assert_eq!(retried.token(), first.token());
    assert_eq!(retried.inodes()[0].read_at(0, 32), Ok(b"abcdefgh".to_vec()));
    assert_eq!(changed_ranges(&retried.inodes()[0]), vec![(0, 8)]);
    assert_eq!(read_all(&namespace, inode, handle), b"abXYefgh");

    namespace
        .complete_commit(
            &first,
            vec![CommittedFileInstall::new(
                inode,
                1,
                Arc::new(BytesFile(b"abcdefgh".to_vec())),
            )],
        )
        .expect("install verified first prefix");
    assert_eq!(read_all(&namespace, inode, handle), b"abXYefgh");

    let second = namespace
        .begin_commit()
        .expect("cut later write")
        .expect("later write still requires durability");
    assert_ne!(second.token(), first.token());
    assert_eq!(second.namespace_mutation_sequence(), 1);
    assert_eq!(second.inodes()[0].mutation_sequence(), 2);
    assert_eq!(second.inodes()[0].read_at(0, 32), Ok(b"abXYefgh".to_vec()));
    assert_eq!(changed_ranges(&second.inodes()[0]), vec![(2, 2)]);
}

fn changed_ranges(inode: &fastdup_posix::CommitInode) -> Vec<(u64, u64)> {
    inode
        .changed_ranges()
        .expect("materialize frozen changed ranges")
        .into_iter()
        .map(|range| (range.offset(), range.length()))
        .collect()
}

#[test]
fn commit_cut_keeps_atomic_namespace_snapshot_while_live_names_advance() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created {
        entry: before,
        handle: before_handle,
    } = create(&namespace, b"before")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let first = namespace
        .begin_commit()
        .expect("cut first create")
        .expect("first create is dirty");
    assert_eq!(entry_names(&first), vec![b"before".to_vec()]);
    assert_eq!(first.namespace_mutation_sequence(), 1);

    let Reply::Created {
        entry: after,
        handle: after_handle,
    } = create(&namespace, b"after")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"before",
            },
        )
        .expect("unlink after cut");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"before",
            },
        ),
        Err(PosixError::NoEntry)
    );
    assert!(
        namespace
            .dispatch(
                CALLER,
                Operation::Lookup {
                    parent: ROOT_INODE,
                    name: b"after",
                },
            )
            .is_ok()
    );
    assert_eq!(entry_names(&first), vec![b"before".to_vec()]);

    namespace
        .complete_commit(
            &first,
            vec![CommittedFileInstall::new(
                before.attr.inode,
                0,
                Arc::new(BytesFile(Vec::new())),
            )],
        )
        .expect("install first namespace generation");
    let second = namespace
        .begin_commit()
        .expect("cut post-generation names")
        .expect("post-generation names are dirty");
    assert_eq!(entry_names(&second), vec![b"after".to_vec()]);
    assert_eq!(second.namespace_mutation_sequence(), 3);
    assert_eq!(second.inodes()[0].inode(), after.attr.inode);

    for (inode, handle) in [
        (before.attr.inode, before_handle),
        (after.attr.inode, after_handle),
    ] {
        namespace
            .dispatch(CALLER, Operation::Release { inode, handle })
            .expect("release fixture handle");
    }
}

#[test]
fn paused_admission_rejects_only_new_mutations_and_preserves_live_reads() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = create(&namespace, b"deadline") else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"visible",
            },
        )
        .expect("admit initial write");

    namespace.pause_mutation_admission();
    assert!(!namespace.mutation_admission_open());
    assert_eq!(read_all(&namespace, inode, handle), b"visible");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 7,
                data: b"blocked",
            },
        ),
        Err(PosixError::Again)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Sync {
                inode,
                handle,
                data_only: false,
            },
        ),
        Ok(Reply::Empty)
    );

    namespace.resume_mutation_admission();
    assert!(namespace.mutation_admission_open());
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 7,
                data: b"-again",
            },
        )
        .expect("resume mutation admission");
    assert_eq!(read_all(&namespace, inode, handle), b"visible-again");
}

#[test]
fn closing_admission_waits_until_an_already_admitted_write_is_applied() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let file = Arc::new(BlockingAllocatedFile {
        entered: Mutex::new(Some(entered_tx)),
        release: Mutex::new(release_rx),
    });
    let snapshot = CommittedNamespaceSnapshot::new(
        3,
        16,
        1,
        vec![
            CommittedInode::new(2, 0o600, 1_000, 1_000, 1, 0, file)
                .expect("fixture committed inode is valid"),
        ],
        vec![CommittedEntry::new(1, 2, b"gate".to_vec()).expect("fixture entry is valid")],
    )
    .expect("fixture snapshot is valid");
    let namespace = Namespace::from_committed_writable(NamespaceConfig::default(), snapshot)
        .expect("mount writable fixture");
    let inode = fastdup_posix::InodeId::new(2).expect("fixture inode is nonzero");
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        )
        .expect("open fixture")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };

    std::thread::scope(|scope| {
        let namespace_ref = &namespace;
        let writer = scope.spawn(|| {
            namespace_ref.dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset: 0,
                    data: b"X",
                },
            )
        });
        entered_rx
            .recv()
            .expect("write reached its committed allocation lookup");
        let (pausing_tx, pausing_rx) = mpsc::channel();
        let (paused_tx, paused_rx) = mpsc::channel();
        let pauser = scope.spawn(move || {
            pausing_tx.send(()).expect("announce admission close");
            namespace_ref.pause_mutation_admission();
            paused_tx.send(()).expect("announce closed admission");
        });
        pausing_rx.recv().expect("pauser started");
        assert!(
            paused_rx.recv_timeout(Duration::from_millis(25)).is_err(),
            "admission closed before the admitted write completed"
        );
        release_tx.send(()).expect("release blocked write");
        assert_eq!(
            writer.join().expect("writer thread did not panic"),
            Ok(Reply::Written {
                bytes: 1,
                mutation_sequence: 1,
            })
        );
        paused_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("admission closes after the admitted write");
        pauser.join().expect("pauser thread did not panic");
    });

    assert_eq!(read_all(&namespace, inode, handle), b"X");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"Y",
            },
        ),
        Err(PosixError::Again)
    );
}

#[tokio::test]
async fn checkpoint_pressure_counts_unique_checkpointable_dirty_payload() {
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let Reply::Created { entry, handle } = create(&namespace, b"pressure") else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;

    let waiting_namespace = Arc::clone(&namespace);
    let waiter = tokio::spawn(async move {
        waiting_namespace
            .wait_for_checkpointable_dirty_payload(11)
            .await
    });
    tokio::task::yield_now().await;
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"abcdefgh",
            },
        )
        .expect("write initial dirty payload");
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 2,
                data: b"XY",
            },
        )
        .expect("overwrite dirty payload without double counting it");
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 16,
                data: b"ij",
            },
        )
        .expect("write beyond a sparse hole");
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 10);

    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            namespace.wait_for_checkpointable_dirty_payload(11),
        )
        .await
        .is_err(),
        "ten unique dirty bytes must not trip an eleven-byte threshold"
    );
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 18,
                data: b"k",
            },
        )
        .expect("cross checkpoint pressure threshold");
    assert_eq!(
        waiter
            .await
            .expect("checkpoint-pressure waiter did not panic"),
        11
    );

    namespace
        .begin_commit()
        .expect("freeze pressure fixture")
        .expect("dirty fixture requires a commit");
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 0);
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"next",
            },
        )
        .expect("write into the next active epoch");
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 4);
}

#[test]
fn open_orphan_dirty_payload_is_not_checkpointable() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = create(&namespace, b"orphan-pressure") else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    let inode = entry.attr.inode;
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"next",
            },
        )
        .expect("write linked fixture");
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 4);

    namespace
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"orphan-pressure",
            },
        )
        .expect("unlink active fixture");
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 0);
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 4,
                data: b"orphan",
            },
        )
        .expect("open orphan remains writable but is not checkpointable");
    assert_eq!(namespace.checkpointable_dirty_payload_bytes(), 0);
}

fn create(namespace: &Namespace, name: &[u8]) -> Reply {
    namespace
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
        .expect("create fixture file")
}

fn entry_names(commit: &fastdup_posix::NamespaceCommit) -> Vec<Vec<u8>> {
    commit
        .entries()
        .iter()
        .map(|entry| entry.name().to_vec())
        .collect()
}

fn read_all(
    namespace: &Namespace,
    inode: fastdup_posix::InodeId,
    handle: fastdup_posix::HandleId,
) -> Vec<u8> {
    let Reply::Data(bytes) = namespace
        .dispatch(
            CALLER,
            Operation::Read {
                inode,
                handle,
                offset: 0,
                length: 64,
            },
        )
        .expect("read live file")
    else {
        panic!("ASSERT: read returned the wrong reply variant");
    };
    bytes
}
