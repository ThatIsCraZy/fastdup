use std::sync::{Arc, Mutex};

use fastdup_posix::{
    CommittedEntry, CommittedFile, CommittedInode, CommittedNamespaceSnapshot, Namespace,
    NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply, RequestContext,
};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 77,
};

#[derive(Debug)]
struct FillFile {
    length: u64,
    reads: Arc<Mutex<Vec<(u64, u32)>>>,
}

impl CommittedFile for FillFile {
    fn logical_size(&self) -> u64 {
        self.length
    }

    fn allocated_bytes(&self) -> u64 {
        self.length
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        Ok(offset.saturating_add(length).min(self.length) - offset.min(self.length))
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.reads
            .lock()
            .expect("ASSERT: test read log lock poisoned")
            .push((offset, length));
        let end = offset.saturating_add(u64::from(length)).min(self.length);
        Ok(vec![
            b'Z';
            usize::try_from(end - offset)
                .expect("ASSERT: bounded test read must fit usize")
        ])
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn committed_snapshot_is_lazy_byte_exact_and_create_closed_without_a_fresh_reservation() {
    let reads = Arc::new(Mutex::new(Vec::new()));
    let length = 1_u64 << 40;
    let inode = CommittedInode::new(
        2,
        0o640,
        1_000,
        1_001,
        1,
        9,
        Arc::new(FillFile {
            length,
            reads: Arc::clone(&reads),
        }),
    )
    .expect("committed inode is valid");
    let raw_name = b"vm-\xff";
    let snapshot = CommittedNamespaceSnapshot::new(
        4_096,
        4_096,
        11,
        vec![inode],
        vec![CommittedEntry::new(1, 2, raw_name.to_vec()).expect("entry is valid")],
    )
    .expect("snapshot bounds are valid");
    let namespace = Namespace::from_committed(NamespaceConfig::default(), snapshot)
        .expect("verified snapshot mounts");
    assert!(
        reads
            .lock()
            .expect("ASSERT: test read log lock poisoned")
            .is_empty(),
        "mount must not materialize committed file bytes"
    );

    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: raw_name,
            },
        )
        .expect("raw byte name must resolve")
    else {
        panic!("ASSERT: lookup returned the wrong reply variant");
    };
    assert_eq!(entry.attr.size, length);
    assert_eq!(entry.attr.allocated_bytes, length);
    assert_eq!(entry.attr.mutation_sequence, 9);

    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("committed file opens")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: length - 3,
                length: 8,
            },
        ),
        Ok(Reply::Data(b"ZZZ".to_vec()))
    );
    assert_eq!(
        *reads.lock().expect("ASSERT: test read log lock poisoned"),
        vec![(length - 3, 3)]
    );

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        ),
        Err(PosixError::ReadOnly)
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::SetLength {
                inode: entry.attr.inode,
                handle: None,
                length: 3,
            },
        ),
        Err(PosixError::ReadOnly)
    );

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"not-reserved",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        ),
        Err(PosixError::ReadOnly)
    );
}

#[test]
fn committed_snapshot_rejects_dangling_and_link_count_mismatches() {
    let make_inode = || {
        CommittedInode::new(
            2,
            0o600,
            0,
            0,
            2,
            0,
            Arc::new(FillFile {
                length: 0,
                reads: Arc::new(Mutex::new(Vec::new())),
            }),
        )
        .expect("local inode fields are valid")
    };
    let mismatch = CommittedNamespaceSnapshot::new(
        3,
        3,
        0,
        vec![make_inode()],
        vec![CommittedEntry::new(1, 2, b"one".to_vec()).expect("entry is valid")],
    )
    .expect("snapshot bounds are valid");
    assert!(matches!(
        Namespace::from_committed(NamespaceConfig::default(), mismatch),
        Err(PosixError::InvalidArgument)
    ));

    let dangling = CommittedNamespaceSnapshot::new(
        3,
        3,
        0,
        vec![make_inode()],
        vec![CommittedEntry::new(1, 99, b"dangling".to_vec()).expect("entry fields are local")],
    )
    .expect("snapshot bounds are valid");
    assert!(matches!(
        Namespace::from_committed(NamespaceConfig::default(), dangling),
        Err(PosixError::InvalidArgument)
    ));
}
