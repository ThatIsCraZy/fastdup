use fastdup_posix::{
    AccessMode, FileLock, HandleId, InodeId, LockKind, Namespace, NamespaceConfig, OpenOptions,
    Operation, PosixError, ROOT_INODE, Reply, RequestContext,
};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 77,
};

fn create_file(namespace: &Namespace) -> (InodeId, HandleId) {
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"locked",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("fixture file is created")
    else {
        panic!("create returned the wrong reply");
    };
    (entry.attr.inode, handle)
}

fn open(namespace: &Namespace, inode: InodeId, options: OpenOptions) -> HandleId {
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options,
                truncate: false,
            },
        )
        .expect("fixture file opens")
    else {
        panic!("open returned the wrong reply");
    };
    handle
}

fn lock(start: u64, end: u64, kind: LockKind, pid: u32) -> FileLock {
    FileLock {
        start,
        end,
        kind,
        pid,
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn record_locks_report_only_conflicting_owners_and_access_modes() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let (inode, first) = create_file(&namespace);
    let second = open(&namespace, inode, OpenOptions::READ_WRITE);
    let read_only = open(&namespace, inode, OpenOptions::READ_ONLY);
    let write_only = open(
        &namespace,
        inode,
        OpenOptions {
            access: AccessMode::WriteOnly,
            append: false,
        },
    );

    namespace
        .dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: first,
                owner: 10,
                lock: lock(0, 99, LockKind::Read, 110),
            },
        )
        .expect("first read lock succeeds");
    namespace
        .dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: second,
                owner: 20,
                lock: lock(50, 149, LockKind::Read, 120),
            },
        )
        .expect("overlapping read locks are compatible");

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: second,
                owner: 30,
                lock: lock(75, 80, LockKind::Write, 130),
            },
        ),
        Err(PosixError::Again)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::GetLock {
                inode,
                handle: second,
                owner: 30,
                lock: lock(75, 80, LockKind::Write, 130),
            },
        ),
        Ok(Reply::Lock(lock(0, 99, LockKind::Read, 110)))
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::GetLock {
                inode,
                handle: first,
                owner: 10,
                lock: lock(0, 149, LockKind::Write, 110),
            },
        ),
        Ok(Reply::Lock(lock(50, 149, LockKind::Read, 120))),
        "a lock owner never conflicts with itself"
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::GetLock {
                inode,
                handle: first,
                owner: 10,
                lock: lock(200, u64::MAX, LockKind::Write, 110),
            },
        ),
        Ok(Reply::Lock(lock(200, u64::MAX, LockKind::Unlock, 0)))
    );

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: read_only,
                owner: 40,
                lock: lock(200, 299, LockKind::Write, 140),
            },
        ),
        Err(PosixError::BadHandle)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: write_only,
                owner: 40,
                lock: lock(200, 299, LockKind::Read, 140),
            },
        ),
        Err(PosixError::BadHandle)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: first,
                owner: 10,
                lock: lock(2, 1, LockKind::Unlock, 110),
            },
        ),
        Err(PosixError::InvalidArgument)
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn owner_updates_split_merge_and_release_byte_ranges() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let (inode, first) = create_file(&namespace);
    let second = open(&namespace, inode, OpenOptions::READ_WRITE);

    namespace
        .dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: first,
                owner: 10,
                lock: lock(0, 99, LockKind::Write, 110),
            },
        )
        .expect("whole range locks");
    namespace
        .dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: first,
                owner: 10,
                lock: lock(25, 74, LockKind::Unlock, 110),
            },
        )
        .expect("middle range unlock splits the owner lock");
    namespace
        .dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: second,
                owner: 20,
                lock: lock(25, 74, LockKind::Write, 120),
            },
        )
        .expect("another owner locks the released middle");

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::GetLock {
                inode,
                handle: second,
                owner: 20,
                lock: lock(0, u64::MAX, LockKind::Write, 120),
            },
        ),
        Ok(Reply::Lock(lock(0, 24, LockKind::Write, 110)))
    );
    namespace
        .dispatch(
            CALLER,
            Operation::UnlockOwner {
                inode,
                handle: first,
                owner: 10,
            },
        )
        .expect("close-style owner cleanup releases every remaining fragment");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::GetLock {
                inode,
                handle: first,
                owner: 30,
                lock: lock(0, 24, LockKind::Write, 130),
            },
        ),
        Ok(Reply::Lock(lock(0, 24, LockKind::Unlock, 0)))
    );

    namespace
        .dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: second,
                owner: 20,
                lock: lock(0, 24, LockKind::Write, 120),
            },
        )
        .expect("adjacent owner ranges merge");
    namespace
        .dispatch(
            CALLER,
            Operation::SetLock {
                inode,
                handle: second,
                owner: 20,
                lock: lock(75, 99, LockKind::Write, 120),
            },
        )
        .expect("second adjacent owner range merges");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::GetLock {
                inode,
                handle: first,
                owner: 30,
                lock: lock(0, 99, LockKind::Read, 130),
            },
        ),
        Ok(Reply::Lock(lock(0, 99, LockKind::Write, 120)))
    );
}
