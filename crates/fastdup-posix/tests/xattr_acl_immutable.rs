use fastdup_posix::{
    FS_IMMUTABLE_FL, FallocateMode, HandleId, InodeId, Namespace, NamespaceConfig, OpenOptions,
    Operation, POSIX_ACL_ACCESS_XATTR, POSIX_ACL_DEFAULT_XATTR, PosixError, ROOT_INODE, Reply,
    RequestContext, XattrSetMode,
};

const OWNER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 71,
};
const ROOT: RequestContext = RequestContext {
    uid: 0,
    gid: 0,
    pid: 1,
};

fn create(namespace: &Namespace, name: &[u8]) -> (InodeId, HandleId) {
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            OWNER,
            Operation::Create {
                parent: ROOT_INODE,
                name,
                mode: 0o640,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create fixture")
    else {
        panic!("ASSERT: create returned the wrong reply");
    };
    (entry.attr.inode, handle)
}

fn acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut value = 2_u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in entries {
        value.extend_from_slice(&tag.to_le_bytes());
        value.extend_from_slice(&permissions.to_le_bytes());
        value.extend_from_slice(&id.to_le_bytes());
    }
    value
}

#[test]
#[allow(clippy::too_many_lines)]
fn xattrs_are_byte_exact_sorted_and_access_acl_updates_mode() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let (inode, _) = create(&namespace, b"backup.vbk");
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::SetXattr {
                inode,
                name: b"user.immutable.until",
                value: b"2026-09-08 13:14:15",
                mode: XattrSetMode::Create,
            },
        ),
        Ok(Reply::Empty)
    );
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::SetXattr {
                inode,
                name: b"user.backup-id",
                value: b"\0\xffopaque",
                mode: XattrSetMode::Upsert,
            },
        ),
        Ok(Reply::Empty)
    );
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::GetXattr {
                inode,
                name: b"user.backup-id",
            },
        ),
        Ok(Reply::Xattr(b"\0\xffopaque".to_vec()))
    );
    assert_eq!(
        namespace.dispatch(OWNER, Operation::ListXattrs { inode }),
        Ok(Reply::Xattr(
            b"user.backup-id\0user.immutable.until\0".to_vec()
        ))
    );
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::SetXattr {
                inode,
                name: b"user.backup-id",
                value: b"duplicate",
                mode: XattrSetMode::Create,
            },
        ),
        Err(PosixError::Exists)
    );

    let access_acl = acl(&[
        (0x01, 0o7, u32::MAX),
        (0x02, 0o6, 2_000),
        (0x04, 0o5, u32::MAX),
        (0x10, 0o4, u32::MAX),
        (0x20, 0o1, u32::MAX),
    ]);
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::SetXattr {
                inode,
                name: POSIX_ACL_ACCESS_XATTR,
                value: &access_acl,
                mode: XattrSetMode::Upsert,
            },
        ),
        Ok(Reply::Empty)
    );
    let Reply::Attr(attr) = namespace
        .dispatch(OWNER, Operation::GetAttr { inode })
        .expect("get attributes")
    else {
        panic!("ASSERT: getattr returned the wrong reply");
    };
    assert_eq!(attr.mode, 0o741);
    let Reply::Attr(chmod_attr) = namespace
        .dispatch(OWNER, Operation::SetMode { inode, mode: 0o601 })
        .expect("chmod ACL-bearing inode")
    else {
        panic!("ASSERT: chmod reply");
    };
    assert_eq!(chmod_attr.mode, 0o601);
    let Reply::Xattr(chmod_acl) = namespace
        .dispatch(
            OWNER,
            Operation::GetXattr {
                inode,
                name: POSIX_ACL_ACCESS_XATTR,
            },
        )
        .expect("read chmod-updated ACL")
    else {
        panic!("ASSERT: getxattr reply");
    };
    assert_eq!(u16::from_le_bytes(chmod_acl[6..8].try_into().unwrap()), 0o6);
    assert_eq!(
        u16::from_le_bytes(chmod_acl[30..32].try_into().unwrap()),
        0o0
    );
    assert_eq!(
        u16::from_le_bytes(chmod_acl[38..40].try_into().unwrap()),
        0o1
    );
}

#[test]
fn immutable_flag_blocks_content_names_and_metadata_until_root_clears_it() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let (inode, handle) = create(&namespace, b"sealed.vbk");
    assert!(matches!(
        namespace.dispatch(
            OWNER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"backup",
            },
        ),
        Ok(Reply::Written { .. })
    ));
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::SetFileFlags {
                inode,
                flags: FS_IMMUTABLE_FL,
            },
        ),
        Err(PosixError::PermissionDenied)
    );
    assert_eq!(
        namespace.dispatch(
            ROOT,
            Operation::SetFileFlags {
                inode,
                flags: FS_IMMUTABLE_FL,
            },
        ),
        Ok(Reply::Empty)
    );
    assert_eq!(
        namespace.dispatch(ROOT, Operation::GetFileFlags { inode }),
        Ok(Reply::FileFlags(FS_IMMUTABLE_FL))
    );
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"damage",
            },
        ),
        Err(PosixError::PermissionDenied)
    );
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::Fallocate {
                inode,
                handle,
                offset: 0,
                length: 1,
                mode: FallocateMode::PunchHole,
            },
        ),
        Err(PosixError::PermissionDenied)
    );
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::RemoveXattr {
                inode,
                name: b"user.immutable.until",
            },
        ),
        Err(PosixError::PermissionDenied)
    );
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::Unlink {
                parent: ROOT_INODE,
                name: b"sealed.vbk",
            },
        ),
        Err(PosixError::PermissionDenied)
    );
    assert_eq!(
        namespace.dispatch(ROOT, Operation::SetFileFlags { inode, flags: 0 }),
        Ok(Reply::Empty)
    );
    assert!(matches!(
        namespace.dispatch(
            OWNER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"open",
            },
        ),
        Ok(Reply::Written { .. })
    ));
}

#[test]
fn commit_cut_captures_xattrs_and_flags_as_metadata_only() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let (inode, _) = create(&namespace, b"durable.vbk");
    namespace
        .dispatch(
            OWNER,
            Operation::SetXattr {
                inode,
                name: b"user.immutable.until",
                value: b"2030-01-01 00:00:00",
                mode: XattrSetMode::Upsert,
            },
        )
        .expect("set retention xattr");
    namespace
        .dispatch(
            ROOT,
            Operation::SetFileFlags {
                inode,
                flags: FS_IMMUTABLE_FL,
            },
        )
        .expect("set immutable flag");

    let commit = namespace
        .begin_commit()
        .expect("freeze metadata")
        .expect("metadata is dirty");
    let committed = commit
        .inodes()
        .iter()
        .find(|candidate| candidate.inode() == inode)
        .expect("committed inode exists");
    assert_eq!(committed.metadata().file_flags(), FS_IMMUTABLE_FL);
    let attributes = committed.metadata().xattrs().collect::<Vec<_>>();
    assert_eq!(attributes.len(), 1);
    assert_eq!(attributes[0].name(), b"user.immutable.until");
    assert_eq!(attributes[0].value(), b"2030-01-01 00:00:00");
    assert_eq!(committed.logical_size(), 0);
    assert_eq!(committed.allocated_bytes(), 0);
}

#[test]
fn default_acl_is_inherited_atomically_and_replaces_umask_for_new_children() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let Reply::Entry(directory) = namespace
        .dispatch(
            OWNER,
            Operation::Mkdir {
                parent: ROOT_INODE,
                name: b"repository",
                mode: 0o700,
            },
        )
        .expect("create ACL parent")
    else {
        panic!("ASSERT: mkdir reply");
    };
    let default_acl = acl(&[
        (0x01, 0o7, u32::MAX),
        (0x02, 0o6, 2_000),
        (0x04, 0o5, u32::MAX),
        (0x10, 0o4, u32::MAX),
        (0x20, 0o1, u32::MAX),
    ]);
    namespace
        .dispatch(
            OWNER,
            Operation::SetXattr {
                inode: directory.attr.inode,
                name: POSIX_ACL_DEFAULT_XATTR,
                value: &default_acl,
                mode: XattrSetMode::Create,
            },
        )
        .expect("install default ACL");
    let Reply::Created { entry: file, .. } = namespace
        .dispatch(
            OWNER,
            Operation::CreateWithUmask {
                parent: directory.attr.inode,
                name: b"backup.vbk",
                mode: 0o666,
                umask: 0o077,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create ACL-inheriting file")
    else {
        panic!("ASSERT: create reply");
    };
    assert_eq!(file.attr.mode & 0o777, 0o640);
    assert!(matches!(
        namespace.dispatch(
            OWNER,
            Operation::GetXattr {
                inode: file.attr.inode,
                name: POSIX_ACL_ACCESS_XATTR,
            },
        ),
        Ok(Reply::Xattr(_))
    ));
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::GetXattr {
                inode: file.attr.inode,
                name: POSIX_ACL_DEFAULT_XATTR,
            },
        ),
        Err(PosixError::NoData)
    );
    let Reply::Entry(child_directory) = namespace
        .dispatch(
            OWNER,
            Operation::MkdirWithUmask {
                parent: directory.attr.inode,
                name: b"chain",
                mode: 0o777,
                umask: 0o077,
            },
        )
        .expect("create ACL-inheriting directory")
    else {
        panic!("ASSERT: mkdir reply");
    };
    assert_eq!(child_directory.attr.mode & 0o777, 0o741);
    assert_eq!(
        namespace.dispatch(
            OWNER,
            Operation::GetXattr {
                inode: child_directory.attr.inode,
                name: POSIX_ACL_DEFAULT_XATTR,
            },
        ),
        Ok(Reply::Xattr(default_acl))
    );
}
