use super::graph::verify_generation_transition_pair;
use super::metadata::metadata_name;
use super::{GenerationError, GenerationRepository};
use crate::StorageIo;
use fastdup_format::{
    CommitRecord, CommitRecordHash, ManifestExtent, ManifestLeaf, MetadataObjectId, NamespaceRoot,
    PolicySetId,
};
use std::sync::Arc;

use fastdup_format::{DurableInode, NamespaceEntry};

#[test]
fn metadata_publication_does_not_sync_a_length_head_per_four_kib() {
    let path = std::env::temp_dir().join(format!("metadata-batch-{}", std::process::id()));
    let storage = crate::FsStorageIo::open(&path).unwrap();
    let repo = GenerationRepository::new(storage, PolicySetId::new([1; 32]).unwrap());
    let extents = (0..8000)
        .map(|index| ManifestExtent::Fill {
            logical_length: 4096,
            value: u8::try_from(index % 2).unwrap(),
        })
        .collect();
    let bytes = ManifestLeaf::new(8000 * 4096, extents)
        .unwrap()
        .encode()
        .unwrap();
    let before = crate::direct_io::WRITE_CALLS.with(std::cell::Cell::get);
    let id = repo.stage_metadata(&bytes).unwrap();
    let writes = crate::direct_io::WRITE_CALLS.with(std::cell::Cell::get) - before;
    assert_eq!(
        writes, 4,
        "one immutable object needs one body and three heads"
    );
    repo.storage.sync_root().unwrap();
    let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
    assert_eq!(repo.read_metadata(id).unwrap(), bytes);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn metadata_graph_reads_share_owned_bytes_across_read_paths() {
    let root = std::env::temp_dir().join(format!("metadata-owned-{}", std::process::id()));
    let mut storage = crate::FsStorageIo::open(&root).unwrap();
    let counters = Arc::new(crate::metadata_read_telemetry::MetadataReadCounters::default());
    storage.metadata_reads = Some(Arc::clone(&counters));
    let mut repo = GenerationRepository::new(storage, PolicySetId::new([1; 32]).unwrap());
    repo.metadata_cache = Arc::new(crate::metadata_object_cache::MetadataObjectCache::limited(
        1 << 20,
    ));
    let layout = ManifestLeaf::new(
        4096,
        vec![ManifestExtent::Fill {
            logical_length: 4096,
            value: 7,
        }],
    )
    .unwrap();
    let id = repo.publish_manifest(&layout).unwrap();
    let expected = repo.read_manifest_node(id).unwrap();
    let before: u64 = counters.rows().iter().map(|r| r.operations).sum();
    for _ in 0..8 {
        assert_eq!(repo.read_metadata(id).unwrap(), expected);
        assert_eq!(repo.read_manifest_node(id).unwrap(), expected);
    }
    let after: u64 = counters.rows().iter().map(|r| r.operations).sum();
    assert_eq!(
        after, before,
        "immutable graph reads must not reach storage again"
    );
    // Inject durable corruption after warming the cache. Both independent
    // scrub and publication verification must still reach storage and fail.
    let mut damaged = expected.clone();
    damaged[0] ^= 1;
    repo.storage
        .write_at(&metadata_name(id), 0, &damaged)
        .unwrap();
    repo.storage.sync_file(&metadata_name(id)).unwrap();
    assert!(repo.scrub_manifest_tree_metadata(id).is_err());
    assert!(repo.publish_manifest(&layout).is_err());
    assert_eq!(repo.read_metadata(id).unwrap(), expected);
    repo.metadata_cache.invalidate(id);
    assert!(repo.read_metadata(id).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovery_rechecks_a_warm_namespace_after_durable_corruption() {
    let path = std::env::temp_dir().join(format!("metadata-recovery-cache-{}", std::process::id()));
    let storage = crate::FsStorageIo::open(&path).unwrap();
    let mut repo = GenerationRepository::new(storage, PolicySetId::new([1; 32]).unwrap());
    repo.metadata_cache = Arc::new(crate::metadata_object_cache::MetadataObjectCache::limited(
        1 << 20,
    ));
    let manifest = ManifestLeaf::new(
        4096,
        vec![ManifestExtent::Fill {
            logical_length: 4096,
            value: 3,
        }],
    )
    .unwrap();
    let id = repo.publish_manifest(&manifest).unwrap();
    let namespace = NamespaceRoot::new(
        4096,
        3,
        1,
        vec![DurableInode::new(2, 0o640, 1000, 1001, 1, 1, 4096, id).unwrap()],
        vec![NamespaceEntry::new(1, 2, b"file".to_vec()).unwrap()],
    )
    .unwrap();
    let initial = repo
        .commit_namespace(&NamespaceRoot::new(4096, 2, 0, vec![], vec![]).unwrap())
        .unwrap();
    let record = repo.commit_namespace(&namespace).unwrap();
    let encoded = repo.read_metadata(record.namespace_root()).unwrap();
    assert!(repo.recover_latest().unwrap().is_some());
    let mut damaged = encoded.clone();
    damaged[0] ^= 1;
    repo.storage
        .write_at(&metadata_name(record.namespace_root()), 0, &damaged)
        .unwrap();
    repo.storage
        .sync_file(&metadata_name(record.namespace_root()))
        .unwrap();
    assert_eq!(
        repo.read_metadata(record.namespace_root()).unwrap(),
        encoded
    );
    let recovered = repo.recover_latest().unwrap().unwrap();
    assert_eq!(
        recovered.record(),
        initial,
        "recovery must reject the damaged newer generation despite its warm bytes"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn recovery_and_scrub_reject_damaged_storage_heads_despite_warm_manifest_bytes() {
    use std::os::unix::fs::FileExt;
    let path = std::env::temp_dir().join(format!("metadata-head-fault-{}", std::process::id()));
    let storage = crate::FsStorageIo::open(&path).unwrap();
    let repo = GenerationRepository::new(storage, PolicySetId::new([1; 32]).unwrap());
    repo.commit_namespace(&NamespaceRoot::new(4096, 2, 0, vec![], vec![]).unwrap())
        .unwrap();
    let manifest = ManifestLeaf::new(
        4096,
        vec![ManifestExtent::Fill {
            logical_length: 4096,
            value: 5,
        }],
    )
    .unwrap();
    let id = repo.publish_manifest(&manifest).unwrap();
    repo.commit_namespace(
        &NamespaceRoot::new(
            4096,
            3,
            1,
            vec![DurableInode::new(2, 0o640, 1000, 1000, 1, 1, 4096, id).unwrap()],
            vec![NamespaceEntry::new(1, 2, b"file".to_vec()).unwrap()],
        )
        .unwrap(),
    )
    .unwrap();
    let warm = repo.read_metadata(id).unwrap();
    // Fault injection intentionally bypasses StorageIo's logical translation.
    // Damage both physical length-head checksums without changing the payload.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join(metadata_name(id)))
        .unwrap();
    for offset in [24, 4096 + 24] {
        let mut byte = [0];
        file.read_exact_at(&mut byte, offset).unwrap();
        byte[0] ^= 1;
        file.write_all_at(&byte, offset).unwrap();
    }
    file.sync_all().unwrap();
    assert_eq!(repo.read_metadata(id).unwrap(), warm);
    assert!(repo.scrub_manifest_tree_metadata(id).is_err());
    assert!(repo.publish_manifest(&manifest).is_err());
    assert!(matches!(
        repo.recover_latest(),
        Err(GenerationError::ManifestTree(crate::manifest_tree::ManifestTreeError::Io(error)))
            if error.kind() == std::io::ErrorKind::InvalidData
    ));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn online_gc_reuses_warm_graph_bytes_but_keeps_current_generation_binding() {
    let path = std::env::temp_dir().join(format!("online-gc-cache-{}", std::process::id()));
    let mut storage = crate::FsStorageIo::open(&path).unwrap();
    let counters = Arc::new(crate::metadata_read_telemetry::MetadataReadCounters::default());
    storage.metadata_reads = Some(Arc::clone(&counters));
    let repo = GenerationRepository::new(storage, PolicySetId::new([1; 32]).unwrap());
    let manifest = ManifestLeaf::new(
        4096,
        vec![ManifestExtent::Fill {
            logical_length: 4096,
            value: 7,
        }],
    )
    .unwrap();
    let id = repo.publish_manifest(&manifest).unwrap();
    let namespace = NamespaceRoot::new(
        4096,
        3,
        1,
        vec![DurableInode::new(2, 0o640, 1000, 1000, 1, 1, 4096, id).unwrap()],
        vec![NamespaceEntry::new(1, 2, b"file".to_vec()).unwrap()],
    )
    .unwrap();
    let initial = repo
        .commit_namespace(&NamespaceRoot::new(4096, 2, 0, vec![], vec![]).unwrap())
        .unwrap();
    repo.read_namespace_root(initial.namespace_root()).unwrap();
    let committed = repo.commit_namespace(&namespace).unwrap();
    repo.read_namespace_root(committed.namespace_root())
        .unwrap();
    repo.read_manifest_node(id).unwrap();
    let reads = || {
        counters
            .rows()
            .iter()
            .filter(|row| row.object == "metadataObject")
            .map(|row| row.operations)
            .sum::<u64>()
    };
    let before = reads();
    let proof = repo.scan_online_liveness().unwrap();
    assert_eq!(reads(), before, "Online GC must reuse the admitted graph");
    assert!(repo.gc_proof_is_current(&proof).unwrap());
    repo.commit_namespace(&namespace).unwrap();
    assert!(
        !repo.gc_proof_is_current(&proof).unwrap(),
        "cached content cannot keep an old GC binding current"
    );
    std::fs::remove_dir_all(path).unwrap();
}

fn inode(inode: u64, mutation_sequence: u64) -> DurableInode {
    DurableInode::new(
        inode,
        0o600,
        1_000,
        1_001,
        1,
        mutation_sequence,
        0,
        MetadataObjectId::new([0xA5; 32]).expect("fixture Manifest ID is nonzero"),
    )
    .expect("fixture regular inode is valid")
}

fn root(
    reservation_end: u64,
    allocation_cursor: u64,
    namespace_mutation_sequence: u64,
    inodes: Vec<DurableInode>,
) -> NamespaceRoot {
    let entries = inodes
        .iter()
        .map(|inode| {
            NamespaceEntry::new(
                1,
                inode.inode(),
                format!("inode-{}", inode.inode()).into_bytes(),
            )
            .expect("fixture root entry is valid")
        })
        .collect();
    NamespaceRoot::new(
        reservation_end,
        allocation_cursor,
        namespace_mutation_sequence,
        inodes,
        entries,
    )
    .expect("fixture Namespace Root is valid")
}

fn record_for(root: &NamespaceRoot) -> CommitRecord {
    CommitRecord::new(
        2,
        CommitRecordHash::from_bytes([0xB6; 32]),
        MetadataObjectId::new([0xC7; 32]).expect("fixture Namespace Root ID is nonzero"),
        PolicySetId::new([0xD8; 32]).expect("fixture Policy Set ID is nonzero"),
        root.namespace_mutation_sequence(),
        root.inode_reservation_end(),
        root.inode_allocation_cursor(),
    )
    .expect("fixture previous Commit Record is valid")
}

#[test]
fn transition_pair_rejects_a_decreasing_per_inode_mutation_sequence() {
    let previous_root = root(128, 16, 20, vec![inode(5, 7)]);
    let previous_record = record_for(&previous_root);
    let proposed_root = root(128, 16, 21, vec![inode(5, 6)]);

    assert!(matches!(
        verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
        Err(GenerationError::NonMonotonicInodeMutation {
            inode: 5,
            previous: 7,
            proposed: 6,
        })
    ));
}

#[test]
fn transition_pair_rejects_a_removed_inode_reused_below_the_allocation_cursor() {
    let previous_root = root(128, 16, 20, Vec::new());
    let previous_record = record_for(&previous_root);
    let proposed_root = root(128, 16, 21, vec![inode(5, 8)]);

    assert!(matches!(
        verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
        Err(GenerationError::ReusedInodeId {
            inode: 5,
            previous_allocation_cursor: 16,
        })
    ));
}

#[test]
fn transition_pair_rejects_consuming_a_reservation_first_enlarged_by_the_proposal() {
    let previous_root = root(128, 16, 20, Vec::new());
    let previous_record = record_for(&previous_root);
    let proposed_root = root(256, 129, 21, vec![inode(128, 8)]);

    assert!(matches!(
        verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
        Err(
            GenerationError::AllocationExceededPreviouslyDurableReservation {
                previous_reservation_end: 128,
                proposed_allocation_cursor: 129,
            }
        )
    ));
}
