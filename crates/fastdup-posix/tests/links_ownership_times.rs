use fastdup_posix::{
    InodeAttributesUpdate, Namespace, NamespaceConfig, OpenOptions, Operation, PosixError,
    PosixTimestamp, ROOT_INODE, Reply, RequestContext,
};

const OWNER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 7,
};
const ROOT: RequestContext = RequestContext {
    uid: 0,
    gid: 0,
    pid: 1,
};

#[test]
fn hardlinks_share_one_inode_and_unlink_only_removes_one_name() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            OWNER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"original",
                mode: 0o640,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("create reply")
    };
    namespace
        .dispatch(
            OWNER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"shared",
            },
        )
        .unwrap();
    let Reply::Entry(link) = namespace
        .dispatch(
            OWNER,
            Operation::Link {
                inode: entry.attr.inode,
                new_parent: ROOT_INODE,
                new_name: b"alias",
            },
        )
        .unwrap()
    else {
        panic!("link reply")
    };
    assert_eq!(link.attr.inode, entry.attr.inode);
    assert_eq!(link.attr.link_count, 2);
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"original"
            }
        ),
        Ok(Reply::Empty)
    );
    let Reply::Entry(alias) = namespace
        .dispatch(
            OWNER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"alias",
            },
        )
        .unwrap()
    else {
        panic!("lookup reply")
    };
    assert_eq!(alias.attr.link_count, 1);
    assert!(matches!(
        namespace.dispatch(
            OWNER,
            Operation::Read { inode: alias.attr.inode, handle, offset: 0, length: 6 },
        ),
        Ok(Reply::Data(data)) if data == b"shared"
    ));
}

#[test]
fn symlink_targets_are_byte_exact_and_hardlinkable() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let target = b"../opaque/\xff-target";
    let Reply::Entry(entry) = namespace
        .dispatch(
            OWNER,
            Operation::Symlink {
                parent: ROOT_INODE,
                name: b"latest",
                target,
            },
        )
        .unwrap()
    else {
        panic!("symlink reply")
    };
    assert_eq!(entry.attr.size, target.len() as u64);
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::Readlink {
                inode: entry.attr.inode
            }
        ),
        Ok(Reply::LinkTarget(target.to_vec()))
    );
    let Reply::Entry(alias) = namespace
        .dispatch(
            OWNER,
            Operation::Link {
                inode: entry.attr.inode,
                new_parent: ROOT_INODE,
                new_name: b"latest-2",
            },
        )
        .unwrap()
    else {
        panic!("hardlink reply")
    };
    assert_eq!(alias.attr.link_count, 2);
}

#[test]
fn setattr_enforces_chown_rules_and_sets_times_atomically() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            OWNER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"owned",
                mode: 0o6750,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("create reply")
    };
    let inode = entry.attr.inode;
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::SetAttributes {
                inode,
                update: InodeAttributesUpdate {
                    uid: Some(2_000),
                    ..Default::default()
                },
            },
        ),
        Err(PosixError::PermissionDenied)
    );
    let atime = PosixTimestamp::new(123, 456);
    let mtime = PosixTimestamp::new(789, 12);
    let Reply::Attr(attr) = namespace
        .dispatch(
            ROOT,
            Operation::SetAttributes {
                inode,
                update: InodeAttributesUpdate {
                    uid: Some(2_000),
                    gid: Some(3_000),
                    atime: Some(atime),
                    mtime: Some(mtime),
                    ..Default::default()
                },
            },
        )
        .unwrap()
    else {
        panic!("setattr reply")
    };
    assert_eq!((attr.uid, attr.gid), (2_000, 3_000));
    assert_eq!(attr.mode & 0o6000, 0);
    assert_eq!(attr.times.atime, atime);
    assert_eq!(attr.times.mtime, mtime);
    namespace
        .dispatch(
            ROOT,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"new",
            },
        )
        .unwrap();
    let Reply::Attr(after_write) = namespace
        .dispatch(ROOT, Operation::GetAttr { inode })
        .unwrap()
    else {
        panic!("getattr reply")
    };
    assert!(after_write.times.mtime > mtime);
    assert_eq!(after_write.times.mtime, after_write.times.ctime);
    namespace
        .dispatch(
            ROOT,
            Operation::Read {
                inode,
                handle,
                offset: 0,
                length: 3,
            },
        )
        .unwrap();
    let Reply::Attr(after_read) = namespace
        .dispatch(ROOT, Operation::GetAttr { inode })
        .unwrap()
    else {
        panic!("getattr reply")
    };
    assert!(after_read.times.atime > atime);
    assert_eq!(after_read.times.ctime, after_write.times.ctime);
}
