use std::sync::{Arc, Mutex};

use fastdup_posix::{
    CommittedEntry, CommittedFile, CommittedInode, CommittedNamespaceSnapshot, Namespace,
    NamespaceConfig, OpenOptions, Operation, PosixError, PreparedDataRecipe, ROOT_INODE, Reply,
    RequestContext,
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

#[derive(Debug)]
struct CloneableFile {
    bytes: Vec<u8>,
    reads: Arc<Mutex<Vec<(u64, u32)>>>,
}

impl CommittedFile for CloneableFile {
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
        self.reads
            .lock()
            .expect("ASSERT: fixture read log lock poisoned")
            .push((offset, length));
        let start = usize::try_from(offset).map_err(|_| PosixError::FileTooLarge)?;
        let end = start
            .saturating_add(usize::try_from(length).expect("ASSERT: u32 fits usize"))
            .min(self.bytes.len());
        Ok(self.bytes.get(start..end).unwrap_or_default().to_vec())
    }

    fn prepared_data_recipe(&self) -> Option<PreparedDataRecipe> {
        Some(PreparedDataRecipe::Chunk {
            chunk_id: [0x7a; 32],
        })
    }
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

#[test]
#[allow(clippy::too_many_lines)]
fn clone_range_is_one_metadata_mutation_and_defers_source_data_reads() {
    let source_reads = Arc::new(Mutex::new(Vec::new()));
    let target_reads = Arc::new(Mutex::new(Vec::new()));
    let source = CommittedInode::new(
        2,
        0o600,
        1_000,
        1_000,
        1,
        7,
        Arc::new(CloneableFile {
            bytes: b"0123456789abcdef".to_vec(),
            reads: Arc::clone(&source_reads),
        }),
    )
    .expect("source inode");
    let target = CommittedInode::new(
        3,
        0o600,
        1_000,
        1_000,
        1,
        11,
        Arc::new(CloneableFile {
            bytes: b"----------------".to_vec(),
            reads: Arc::clone(&target_reads),
        }),
    )
    .expect("target inode");
    let snapshot = CommittedNamespaceSnapshot::new(
        4,
        4_096,
        12,
        vec![source, target],
        vec![
            CommittedEntry::new(1, 2, b"source".to_vec()).expect("source entry"),
            CommittedEntry::new(1, 3, b"target".to_vec()).expect("target entry"),
        ],
    )
    .expect("snapshot");
    let namespace = Namespace::from_committed_writable(NamespaceConfig::default(), snapshot)
        .expect("writable namespace");
    let source_inode = fastdup_posix::InodeId::new(2).expect("source inode id");
    let target_inode = fastdup_posix::InodeId::new(3).expect("target inode id");
    let Reply::Opened(source_handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: source_inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("source open")
    else {
        panic!("ASSERT: source open reply");
    };
    let Reply::Opened(target_handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: target_inode,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        )
        .expect("target open")
    else {
        panic!("ASSERT: target open reply");
    };
    let Reply::Opened(source_write_handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: source_inode,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        )
        .expect("source read-write open")
    else {
        panic!("ASSERT: source read-write open reply");
    };
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::CloneRange {
                source_inode,
                source_handle: source_write_handle,
                source_offset: 0,
                target_inode: source_inode,
                target_handle: source_write_handle,
                target_offset: 1,
                length: 8,
            },
        ),
        Err(PosixError::Unsupported)
    );

    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::CloneRange {
                source_inode,
                source_handle,
                source_offset: 3,
                target_inode,
                target_handle,
                target_offset: 5,
                length: 8,
            },
        ),
        Ok(Reply::Cloned {
            bytes: 8,
            mutation_sequence: 12,
        })
    );
    assert!(
        source_reads
            .lock()
            .expect("ASSERT: source read log lock poisoned")
            .is_empty(),
        "accepted clone must not read source DATA"
    );
    assert!(
        target_reads
            .lock()
            .expect("ASSERT: target read log lock poisoned")
            .is_empty(),
        "accepted clone must not read overwritten target DATA"
    );
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode: target_inode,
                handle: target_handle,
                offset: 0,
                length: 16,
            },
        ),
        Ok(Reply::Data(b"-----3456789a---".to_vec()))
    );
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: target_inode,
                handle: target_handle,
                offset: 7,
                data: b"XX",
            },
        )
        .expect("overwrite cloned target range");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode: source_inode,
                handle: source_handle,
                offset: 0,
                length: 16,
            },
        ),
        Ok(Reply::Data(b"0123456789abcdef".to_vec())),
        "target copy-on-write must not change the immutable source"
    );
}
