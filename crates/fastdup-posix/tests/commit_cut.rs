use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use fastdup_posix::{
    CommittedEntry, CommittedFile, CommittedFileInstall, CommittedInode,
    CommittedNamespaceSnapshot, Namespace, NamespaceConfig, OpenOptions, Operation, PosixError,
    ROOT_INODE, Reply, RequestContext,
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
struct BlockingAllocatedFile {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<mpsc::Receiver<()>>,
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
