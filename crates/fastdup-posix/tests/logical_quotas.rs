use fastdup_posix::{
    FallocateMode, LogicalQuotaRule, Namespace, NamespaceConfig, OpenOptions, Operation,
    PosixError, ROOT_INODE, Reply, RequestContext,
};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 42,
};

#[test]
#[allow(clippy::too_many_lines)]
fn subtree_quota_admits_exact_logical_allocation_and_releases_reaped_files() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Entry(share) = namespace
        .dispatch(
            CALLER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"share-a",
                mode: 0o770,
            },
        )
        .expect("create share root")
    else {
        panic!("ASSERT: mkdir returned an entry");
    };
    namespace
        .replace_logical_quotas(
            "shares-r1".to_owned(),
            [LogicalQuotaRule::new(share.attr.inode, 10).expect("valid quota")],
        )
        .expect("install logical quota");

    let Reply::Created {
        entry,
        handle: writer,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: share.attr.inode,
                name: b"backup.bin",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create quota-owned file")
    else {
        panic!("ASSERT: create returned a handle");
    };
    let inode = entry.attr.inode;

    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: 0,
                data: b"12345678",
            },
        )
        .expect("initial allocation");
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: 0,
                data: b"ABCD",
            },
        )
        .expect("overwrite consumes no extra quota");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: 8,
                data: b"XYZ",
            },
        ),
        Err(PosixError::NoSpace)
    );
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle: writer,
                offset: 8,
                data: b"XY",
            },
        )
        .expect("fill exact quota");
    let status = namespace
        .logical_quota_status(share.attr.inode)
        .expect("quota status");
    assert_eq!((status.limit_bytes, status.used_bytes), (10, 10));

    namespace
        .dispatch(
            CALLER,
            Operation::SetLength {
                inode,
                handle: Some(writer),
                length: 4,
            },
        )
        .expect("truncate releases logical quota");
    namespace
        .dispatch(
            CALLER,
            Operation::Fallocate {
                inode,
                handle: writer,
                offset: 4,
                length: 6,
                mode: FallocateMode::Allocate { keep_size: false },
            },
        )
        .expect("fallocate consumes the released quota");
    assert_eq!(
        namespace
            .logical_quota_status(share.attr.inode)
            .expect("quota after fallocate")
            .used_bytes,
        10
    );
    let Reply::Opened(truncator) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_WRITE,
                truncate: true,
            },
        )
        .expect("open with truncate")
    else {
        panic!("ASSERT: open returned a handle");
    };
    assert_eq!(
        namespace
            .logical_quota_status(share.attr.inode)
            .expect("quota after open truncate")
            .used_bytes,
        0
    );
    namespace
        .dispatch(
            CALLER,
            Operation::Release {
                inode,
                handle: truncator,
            },
        )
        .expect("release truncator");

    namespace
        .dispatch(
            CALLER,
            Operation::Release {
                inode,
                handle: writer,
            },
        )
        .expect("release writer");
    namespace
        .dispatch(
            CALLER,
            Operation::Unlink {
                parent: share.attr.inode,
                name: b"backup.bin",
            },
        )
        .expect("unlink file");
    namespace
        .dispatch(
            CALLER,
            Operation::Forget {
                inode,
                lookup_count: 1,
            },
        )
        .expect("reap file");
    assert_eq!(
        namespace
            .logical_quota_status(share.attr.inode)
            .expect("quota status after delete")
            .used_bytes,
        0
    );
}

#[test]
fn sparse_holes_are_free_but_cross_quota_links_are_rejected() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let mut roots = Vec::new();
    for name in [b"share-a".as_slice(), b"share-b".as_slice()] {
        let Reply::Entry(entry) = namespace
            .dispatch(
                CALLER,
                Operation::Mkdir {
                    parent: ROOT_INODE,
                    name,
                    mode: 0o770,
                },
            )
            .expect("create share root")
        else {
            panic!("ASSERT: mkdir returned an entry");
        };
        roots.push(entry.attr.inode);
    }
    namespace
        .replace_logical_quotas(
            "shares-r1".to_owned(),
            roots
                .iter()
                .copied()
                .map(|root| LogicalQuotaRule::new(root, 4).expect("valid quota")),
        )
        .expect("install logical quotas");
    let Reply::Created {
        entry,
        handle: writer,
    } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: roots[0],
                name: b"sparse.bin",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create sparse file")
    else {
        panic!("ASSERT: create returned a handle");
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle: writer,
                offset: 1_000_000,
                data: b"DATA",
            },
        )
        .expect("only allocated payload counts");
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Link {
                inode: entry.attr.inode,
                new_parent: roots[1],
                new_name: b"foreign-link",
            },
        ),
        Err(PosixError::CrossDevice)
    );
}
