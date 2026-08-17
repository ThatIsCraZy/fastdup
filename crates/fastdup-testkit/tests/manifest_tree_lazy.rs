use fastdup_format::{
    DurableInode, ManifestExtent, ManifestInnerNode, ManifestLeaf, MetadataObjectId,
    NamespaceEntry, NamespaceRoot, PolicySetId,
};
use fastdup_store::{ContainerRepository, GenerationRepository, StorageIo};
use fastdup_testkit::MemoryStorageIo;

const ROOT_INODE: u64 = 1;
const FILE_INODE: u64 = 2;
const RESERVATION_END: u64 = 1_024;
const WINDOW_BYTES: u64 = 64 * 1_024 * 1_024;

#[test]
fn tail_read_of_large_manifest_revalidates_only_its_lazy_tree_path() {
    let policy = PolicySetId::new([0xD1; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let repository = GenerationRepository::new(metadata.clone(), policy);
    repository
        .commit_namespace(
            &NamespaceRoot::new(RESERVATION_END, FILE_INODE, 0, Vec::new(), Vec::new())
                .expect("initial reservation root is valid"),
        )
        .expect("reserve inode IDs before visibility");

    let extent_count = 1_025_u64;
    let logical_size = WINDOW_BYTES
        .checked_mul(extent_count)
        .expect("worked logical size fits u64");
    let extents = (0..extent_count)
        .map(|ordinal| ManifestExtent::Fill {
            logical_length: WINDOW_BYTES,
            value: u8::try_from(ordinal % 251).expect("worked fill value fits u8"),
        })
        .collect();
    let manifest = ManifestLeaf::new(logical_size, extents).expect("large sparse recipe is valid");
    let manifest_root = repository
        .publish_manifest(&manifest)
        .expect("publish a multi-level Manifest tree");
    let namespace_root = NamespaceRoot::new(
        RESERVATION_END,
        FILE_INODE + 1,
        1,
        vec![
            DurableInode::new(FILE_INODE, 0o600, 0, 0, 1, 1, logical_size, manifest_root)
                .expect("durable inode is valid"),
        ],
        vec![
            NamespaceEntry::new(ROOT_INODE, FILE_INODE, b"huge".to_vec())
                .expect("directory entry is valid"),
        ],
    )
    .expect("namespace root is valid");
    let committed = repository
        .commit_namespace_with_verified_files(&namespace_root, &containers)
        .expect("commit and verify the complete tree");
    let (_, mut files) = committed.into_parts();
    let file = files.remove(0).into_file();

    let tail_leaf = last_leaf_id(&metadata, manifest_root);
    let tail_name = metadata_name(tail_leaf);
    let mut tail_bytes = metadata.read(&tail_name).expect("tail leaf exists");
    tail_bytes[4_096 + 24] ^= 1;
    metadata
        .write_at(&tail_name, 4_096 + 24, &tail_bytes[4_096 + 24..4_096 + 25])
        .expect("inject tail leaf corruption");

    let baseline = metadata.operation_count();
    let result = file.read_at(logical_size - 1, 1);
    assert!(
        result.is_err(),
        "a demand read must revalidate every touched Manifest path"
    );
    let metadata_operations = metadata.operation_count() - baseline;
    assert!(
        metadata_operations <= 8,
        "one tail read must remain depth-bounded, observed {metadata_operations} metadata operations"
    );
}

#[test]
fn one_random_overwrite_publishes_only_one_leaf_and_its_ancestor_path() {
    let policy = PolicySetId::new([0xD2; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let extent_count = 1_025_u64;
    let logical_size = WINDOW_BYTES
        .checked_mul(extent_count)
        .expect("worked logical size fits u64");
    let original_extents = (0..extent_count)
        .map(|ordinal| ManifestExtent::Fill {
            logical_length: WINDOW_BYTES,
            value: u8::try_from(ordinal % 251).expect("worked fill value fits u8"),
        })
        .collect::<Vec<_>>();
    let original = ManifestLeaf::new(logical_size, original_extents.clone())
        .expect("original Manifest is valid");
    let original_root = repository
        .publish_manifest(&original)
        .expect("publish original tree");
    let original_root_node = read_inner(&metadata, original_root);
    let untouched_tail_subtree = original_root_node
        .children()
        .last()
        .expect("worked tree has a tail subtree")
        .child();

    let mut changed_extents = original_extents;
    changed_extents[500] = ManifestExtent::Fill {
        logical_length: WINDOW_BYTES,
        value: 0xFE,
    };
    let changed =
        ManifestLeaf::new(logical_size, changed_extents).expect("changed Manifest is valid");
    let objects_before = metadata.list_names().expect("list original objects").len();
    let baseline = metadata.operation_count();
    let changed_ordinal = 500_u64;
    let changed_start = changed_ordinal * WINDOW_BYTES;
    let changed_root = repository
        .publish_manifest_replacement(
            original_root,
            logical_size,
            changed_start..changed_start + WINDOW_BYTES,
            &[ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0xFE,
            }],
        )
        .expect("publish one path-local successor");
    let publication_operations = metadata.operation_count() - baseline;

    assert_ne!(changed_root, original_root);
    assert!(
        publication_operations <= 64,
        "one changed leaf must publish O(tree depth) metadata operations, observed {publication_operations}"
    );
    let objects_after = metadata.list_names().expect("list successor objects").len();
    assert_eq!(
        objects_after - objects_before,
        3,
        "one changed leaf in this three-level tree must add only leaf and ancestor path"
    );
    assert_eq!(
        read_inner(&metadata, changed_root)
            .children()
            .last()
            .expect("successor has a tail subtree")
            .child(),
        untouched_tail_subtree,
        "remote subtree identity must be retained exactly"
    );
    assert_eq!(
        repository
            .read_manifest(changed_root)
            .expect("new tree flattens for compatibility inspection"),
        changed
    );
}

#[test]
fn replacement_boundary_inside_data_fails_before_publication() {
    let policy = PolicySetId::new([0xD3; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let data = vec![0x41; 16 * 1_024];
    let data_length = u64::try_from(data.len()).expect("fixture length fits u64");
    let manifest = ManifestLeaf::new(
        data_length,
        vec![ManifestExtent::Data {
            logical_length: data_length,
            chunk_id: fastdup_format::ChunkId::of(&data),
        }],
    )
    .expect("one DATA extent is valid");
    let root = repository
        .publish_manifest(&manifest)
        .expect("publish predecessor DATA Manifest");
    let object_count = metadata
        .list_names()
        .expect("list predecessor objects")
        .len();

    let result = repository.publish_manifest_replacement(
        root,
        data_length,
        1..2,
        &[ManifestExtent::Fill {
            logical_length: 1,
            value: 0x42,
        }],
    );
    assert!(
        matches!(
            result,
            Err(fastdup_store::GenerationError::ManifestTree(
                fastdup_store::ManifestTreeError::BoundaryInsideData
            ))
        ),
        "a Chunk identity cannot be structurally split by a path-local update"
    );
    assert_eq!(
        metadata.list_names().expect("list after rejection").len(),
        object_count,
        "rejected replacements must not publish metadata"
    );
}

fn last_leaf_id(storage: &MemoryStorageIo, root: MetadataObjectId) -> MetadataObjectId {
    let mut current = root;
    loop {
        let bytes = storage
            .read(&metadata_name(current))
            .expect("Manifest tree object exists");
        let Ok(inner) = ManifestInnerNode::decode(&bytes) else {
            return current;
        };
        current = inner
            .children()
            .last()
            .expect("inner node has a child")
            .child();
    }
}

fn read_inner(storage: &MemoryStorageIo, id: MetadataObjectId) -> ManifestInnerNode {
    ManifestInnerNode::decode(
        &storage
            .read(&metadata_name(id))
            .expect("Manifest inner object exists"),
    )
    .expect("worked object is a valid Manifest inner node")
}

fn metadata_name(object_id: MetadataObjectId) -> String {
    let mut name = String::with_capacity(68);
    for byte in object_id.bytes() {
        use std::fmt::Write;
        write!(&mut name, "{byte:02x}").expect("writing to String is infallible");
    }
    name.push_str(".fdm");
    name
}
