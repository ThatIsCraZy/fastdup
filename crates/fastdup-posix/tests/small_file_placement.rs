use fastdup_posix::{
    Namespace, NamespaceConfig, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext,
    SMALL_FILE_PLACEMENT_XATTR, SMALL_FILE_SPILL_BYTES_V1, XattrSetMode,
};

const OWNER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 71,
};

fn create(namespace: &Namespace, name: &[u8]) -> (fastdup_posix::InodeId, fastdup_posix::HandleId) {
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            OWNER,
            Operation::Create {
                parent: ROOT_INODE,
                name,
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create placement fixture")
    else {
        panic!("ASSERT: create returns an inode and handle")
    };
    (entry.attr.inode, handle)
}

#[test]
fn v1_policy_matches_byte_names_hints_and_spills_above_eight_mib() {
    let namespace = Namespace::new_volatile(NamespaceConfig::default());
    let (xml, xml_handle) = create(&namespace, b"Inventory.XML");
    assert!(namespace.prefers_small_file_tier(xml));

    namespace
        .dispatch(
            OWNER,
            Operation::SetLength {
                inode: xml,
                handle: Some(xml_handle),
                length: SMALL_FILE_SPILL_BYTES_V1 + 1,
            },
        )
        .expect("grow past placement hysteresis");
    assert!(!namespace.prefers_small_file_tier(xml));

    let (hinted, _) = create(&namespace, b"opaque.bin");
    assert!(!namespace.prefers_small_file_tier(hinted));
    namespace
        .dispatch(
            OWNER,
            Operation::SetXattr {
                inode: hinted,
                name: SMALL_FILE_PLACEMENT_XATTR,
                value: b"metadata",
                mode: XattrSetMode::Create,
            },
        )
        .expect("set explicit Metadata placement hint");
    assert!(namespace.prefers_small_file_tier(hinted));

    let (forced_data, _) = create(&namespace, b"forced.json");
    namespace
        .dispatch(
            OWNER,
            Operation::SetXattr {
                inode: forced_data,
                name: SMALL_FILE_PLACEMENT_XATTR,
                value: b"data",
                mode: XattrSetMode::Create,
            },
        )
        .expect("set explicit DATA placement hint");
    assert!(!namespace.prefers_small_file_tier(forced_data));
}
