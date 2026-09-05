use fastdup_posix::{
    Namespace, NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply,
    RequestContext,
};
const CALLER: RequestContext = RequestContext {
    uid: 1000,
    gid: 1000,
    pid: 1,
};

#[test]
fn share_membership_handles_existing_children_new_children_and_live_default() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let mkdir = |parent, name: &[u8]| {
        let Reply::Entry(e) = namespace
            .dispatch(
                CALLER,
                Operation::Mkdir {
                    parent,
                    name,
                    mode: 0o770,
                },
            )
            .unwrap()
        else {
            panic!("directory")
        };
        e.attr.inode
    };
    let a = mkdir(ROOT_INODE, b"a");
    let b = mkdir(ROOT_INODE, b"b");
    let nested = mkdir(a, b"nested");
    namespace
        .replace_share_reduction(false, vec![(a, true), (b, false)])
        .unwrap();
    assert!(namespace.advanced_reduction_enabled(nested));
    assert!(!namespace.advanced_reduction_enabled(b));
    let Reply::Created { entry, .. } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: nested,
                name: b"data",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("file")
    };
    assert!(namespace.advanced_reduction_enabled(entry.attr.inode));
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Link {
                inode: entry.attr.inode,
                new_parent: b,
                new_name: b"cross-share"
            }
        ),
        Err(PosixError::CrossDevice)
    );
    namespace.set_advanced_reduction_default(true);
    assert!(
        !namespace.advanced_reduction_enabled(b),
        "explicit off wins over global on"
    );
    assert!(namespace.advanced_reduction_enabled(ROOT_INODE));
    assert_eq!(
        namespace.replace_share_reduction(false, vec![(a, true), (nested, false)]),
        Err(PosixError::InvalidArgument)
    );
    assert!(
        namespace.advanced_reduction_enabled(nested),
        "rejected replacement is atomic"
    );
    namespace
        .replace_share_reduction(false, vec![(a, false), (b, true)])
        .unwrap();
    assert!(!namespace.advanced_reduction_enabled(entry.attr.inode));
    assert!(namespace.advanced_reduction_enabled(b));
}
