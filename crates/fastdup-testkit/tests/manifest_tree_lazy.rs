use fastdup_format::{
    DurableInode, ManifestExtent, ManifestInnerNode, ManifestLeaf, MetadataObjectId,
    NamespaceEntry, NamespaceRoot, PolicySetId,
};
use fastdup_store::{ContainerRepository, GenerationRepository, StorageIo};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

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
fn length_changing_middle_splice_reuses_the_shifted_suffix_subtree() {
    let policy = PolicySetId::new([0xD5; 32]).expect("policy identity is nonzero");
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
    let original_summary = repository
        .scrub_manifest_tree_metadata(original_root)
        .expect("fully verify the predecessor capability");
    let original_root_node = read_inner(&metadata, original_root);
    let shifted_suffix = original_root_node
        .children()
        .last()
        .expect("worked tree has a remote suffix subtree")
        .child();

    let splice_ordinal = 500_u64;
    let splice_start = splice_ordinal * WINDOW_BYTES;
    let inserted_length = WINDOW_BYTES + WINDOW_BYTES / 2;
    let expected_size = logical_size + WINDOW_BYTES / 2;
    let objects_before = metadata
        .list_names()
        .expect("list predecessor objects")
        .len();
    let baseline = metadata.operation_count();
    let successor = repository
        .publish_manifest_splice(
            original_summary,
            splice_start..splice_start + WINDOW_BYTES,
            &[ManifestExtent::Fill {
                logical_length: inserted_length,
                value: 0xFE,
            }],
        )
        .expect("publish length-changing middle splice");
    let publication_operations = metadata.operation_count() - baseline;

    assert_eq!(successor.logical_size(), expected_size);
    assert_eq!(successor.allocated_bytes(), expected_size);
    assert_eq!(
        repository
            .scrub_manifest_tree_metadata(successor.root())
            .expect("offline scrub accepts the writer-produced splice tree"),
        successor,
        "writer and offline scrub must derive the same logical and allocation totals"
    );
    assert!(
        publication_operations <= 96,
        "one middle splice must publish O(tree height + edit frontier) metadata operations, observed {publication_operations}"
    );
    assert!(
        metadata.list_names().expect("list successor objects").len() - objects_before <= 8,
        "one middle splice must not republish the remote recipe"
    );
    let successor_root = read_inner(&metadata, successor.root());
    assert_eq!(
        successor_root
            .children()
            .last()
            .expect("successor retains a remote suffix subtree")
            .child(),
        shifted_suffix,
        "node-local coordinates must let the shifted suffix retain its exact object identity"
    );

    let mut expected_extents = original_extents;
    expected_extents[usize::try_from(splice_ordinal).expect("fixture ordinal fits usize")] =
        ManifestExtent::Fill {
            logical_length: inserted_length,
            value: 0xFE,
        };
    let expected =
        ManifestLeaf::new(expected_size, expected_extents).expect("expected Manifest is valid");
    assert_eq!(
        repository
            .read_manifest(successor.root())
            .expect("splice successor flattens for compatibility inspection"),
        expected
    );
}

#[test]
fn concat_at_a_subtree_boundary_reuses_both_existing_sides() {
    let policy = PolicySetId::new([0xD6; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let extent_count = 1_025_u64;
    let logical_size = WINDOW_BYTES * extent_count;
    let manifest = ManifestLeaf::new(
        logical_size,
        (0..extent_count)
            .map(|ordinal| ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: u8::try_from(ordinal % 251).expect("fixture byte fits u8"),
            })
            .collect(),
    )
    .expect("concat predecessor is valid");
    let root = repository
        .publish_manifest(&manifest)
        .expect("publish concat predecessor");
    let summary = repository
        .scrub_manifest_tree_metadata(root)
        .expect("verify concat predecessor");
    let old_root = read_inner(&metadata, root);
    let old_left = old_root.children()[0].child();
    let old_right = old_root.children()[1].child();
    let boundary = WINDOW_BYTES * 1_024;

    let successor = repository
        .publish_manifest_splice(
            summary,
            boundary..boundary,
            &[ManifestExtent::Fill {
                logical_length: 17,
                value: 0xA7,
            }],
        )
        .expect("insert at an exact subtree boundary");
    let new_root = read_inner(&metadata, successor.root());
    assert_eq!(new_root.children().len(), 3);
    assert_eq!(new_root.children()[0].child(), old_left);
    assert_eq!(new_root.children()[2].child(), old_right);
    assert_eq!(successor.logical_size(), logical_size + 17);
    assert_eq!(successor.allocated_bytes(), logical_size + 17);
    assert_eq!(
        repository
            .read_manifest_range(
                successor.root(),
                successor.logical_size(),
                boundary..boundary + 17
            )
            .expect("read inserted concat range")[0]
            .extent(),
        &ManifestExtent::Fill {
            logical_length: 17,
            value: 0xA7,
        }
    );
}

#[test]
fn deletion_across_child_ranges_keeps_the_remote_suffix_and_exact_totals() {
    let policy = PolicySetId::new([0xD7; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let extent_count = 2_049_u64;
    let logical_size = WINDOW_BYTES * extent_count;
    let manifest = ManifestLeaf::new(
        logical_size,
        (0..extent_count)
            .map(|ordinal| ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: u8::try_from(ordinal % 251).expect("fixture byte fits u8"),
            })
            .collect(),
    )
    .expect("delete predecessor is valid");
    let root = repository
        .publish_manifest(&manifest)
        .expect("publish delete predecessor");
    let summary = repository
        .scrub_manifest_tree_metadata(root)
        .expect("verify delete predecessor");
    let old_root = read_inner(&metadata, root);
    let remote_suffix = old_root
        .children()
        .last()
        .expect("worked tree has a remote suffix")
        .child();
    let delete_start = WINDOW_BYTES * 1_023 + WINDOW_BYTES / 2;
    let delete_end = WINDOW_BYTES * 1_025 + WINDOW_BYTES / 2;
    let removed = delete_end - delete_start;

    let successor = repository
        .publish_manifest_splice(summary, delete_start..delete_end, &[])
        .expect("delete across child ranges");
    let new_root = read_inner(&metadata, successor.root());
    assert_eq!(
        new_root
            .children()
            .last()
            .expect("delete successor retains remote suffix")
            .child(),
        remote_suffix
    );
    assert_eq!(successor.logical_size(), logical_size - removed);
    assert_eq!(successor.allocated_bytes(), logical_size - removed);
    let seam = repository
        .read_manifest_range(
            successor.root(),
            successor.logical_size(),
            delete_start - 1..delete_start + 1,
        )
        .expect("read around the delete seam");
    assert_eq!(seam.len(), 2);
    assert_eq!(seam[0].logical_offset(), WINDOW_BYTES * 1_023);
    assert_eq!(
        seam[0].extent(),
        &ManifestExtent::Fill {
            logical_length: WINDOW_BYTES / 2,
            value: u8::try_from(1_023 % 251).expect("fixture byte fits u8"),
        }
    );
    assert_eq!(seam[1].logical_offset(), delete_start);
    assert_eq!(
        seam[1].extent(),
        &ManifestExtent::Fill {
            logical_length: WINDOW_BYTES / 2,
            value: u8::try_from(1_025 % 251).expect("fixture byte fits u8"),
        }
    );
}

#[test]
fn length_changing_splice_preserves_data_identity_as_checked_slices() {
    let policy = PolicySetId::new([0xD8; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let bytes = vec![0x44; 16 * 1_024];
    let length = u64::try_from(bytes.len()).expect("fixture length fits u64");
    let manifest = ManifestLeaf::new(
        length,
        vec![ManifestExtent::Data {
            logical_length: length,
            chunk_id: fastdup_format::ChunkId::of(&bytes),
        }],
    )
    .expect("DATA splice predecessor is valid");
    let root = repository
        .publish_manifest(&manifest)
        .expect("publish DATA splice predecessor");
    let summary = repository
        .scrub_manifest_tree_metadata(root)
        .expect("verify DATA splice predecessor");
    let successor = repository
        .publish_manifest_splice(
            summary,
            1..2,
            &[ManifestExtent::Fill {
                logical_length: 7,
                value: 0x55,
            }],
        )
        .expect("a v2 Manifest represents partial DATA as metadata-only slices");
    assert_eq!(successor.logical_size(), length + 6);
    assert_eq!(successor.allocated_bytes(), length + 6);
    let extents = repository
        .read_manifest_range(
            successor.root(),
            successor.logical_size(),
            0..successor.logical_size(),
        )
        .expect("read the complete splice recipe");
    assert_eq!(
        extents
            .iter()
            .map(|extent| (extent.logical_offset(), extent.extent().clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                0,
                ManifestExtent::DataSlice {
                    logical_length: 1,
                    chunk_id: fastdup_format::ChunkId::of(&bytes),
                    chunk_length: u32::try_from(length).expect("fixture length fits u32"),
                    chunk_offset: 0,
                },
            ),
            (
                1,
                ManifestExtent::Fill {
                    logical_length: 7,
                    value: 0x55,
                },
            ),
            (
                8,
                ManifestExtent::DataSlice {
                    logical_length: length - 2,
                    chunk_id: fastdup_format::ChunkId::of(&bytes),
                    chunk_length: u32::try_from(length).expect("fixture length fits u32"),
                    chunk_offset: 2,
                },
            ),
        ],
        "the replacement must preserve the exact source Chunk identity and byte offsets"
    );
}

#[test]
fn concat_can_raise_an_empty_tree_and_complete_delete_returns_the_canonical_empty_tree() {
    const INSERTED_BYTES: u64 = 68 * 1_024 * 1_024 * 1_024;
    const INSERTED_WINDOWS: usize = 1_088;
    let policy = PolicySetId::new([0xDA; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let empty = ManifestLeaf::new(0, Vec::new()).expect("empty Manifest is valid");
    let empty_root = repository
        .publish_manifest(&empty)
        .expect("publish canonical empty tree");
    let empty_summary = repository
        .scrub_manifest_tree_metadata(empty_root)
        .expect("verify canonical empty tree");

    let raised = repository
        .publish_manifest_splice(
            empty_summary,
            0..0,
            &(0..INSERTED_WINDOWS)
                .map(|ordinal| ManifestExtent::Fill {
                    logical_length: WINDOW_BYTES,
                    value: u8::try_from(ordinal % 251).expect("fixture byte fits u8"),
                })
                .collect::<Vec<_>>(),
        )
        .expect("large concat raises the root to the required level");
    assert!(
        read_inner(&metadata, raised.root()).level() >= 2,
        "68 GiB of 64-MiB leaves requires a multi-level Manifest tree"
    );
    assert_eq!(raised.logical_size(), INSERTED_BYTES);
    assert_eq!(raised.allocated_bytes(), INSERTED_BYTES);
    assert_eq!(
        repository
            .scrub_manifest_tree_metadata(raised.root())
            .expect("scrub the raised concat tree"),
        raised
    );

    let deleted = repository
        .publish_manifest_splice(raised, 0..INSERTED_BYTES, &[])
        .expect("complete delete returns an empty tree");
    assert_eq!(deleted.logical_size(), 0);
    assert_eq!(deleted.allocated_bytes(), 0);
    assert_eq!(
        repository
            .read_manifest(deleted.root())
            .expect("read canonical empty successor"),
        empty
    );
}

#[test]
fn every_middle_splice_fault_recovers_the_previous_or_complete_successor() {
    let policy = PolicySetId::new([0xD9; 32]).expect("policy identity is nonzero");
    let probe = MemoryStorageIo::new();
    let (probe_repository, probe_containers, predecessor, previous_summary) =
        seed_splice_predecessor(&probe, policy);
    let baseline = probe.operation_count();
    let next_record = publish_splice_generation(
        &probe_repository,
        &probe_containers,
        predecessor,
        previous_summary,
    )
    .expect("probe splice generation commits");
    let operations = probe.operations()[baseline..].to_vec();
    assert_eq!(
        operations.last(),
        Some(&StorageOperation::SyncFile),
        "the Commit-WAL file sync must remain the last fallible splice-generation operation"
    );

    for fail_after in [false, true] {
        for relative_position in 0..operations.len() {
            let fail_position = baseline + relative_position;
            let storage = if fail_after {
                MemoryStorageIo::with_fail_after(fail_position)
            } else {
                MemoryStorageIo::with_fail_before(fail_position)
            };
            let (repository, containers, predecessor, previous_summary) =
                seed_splice_predecessor(&storage, policy);
            let _ =
                publish_splice_generation(&repository, &containers, predecessor, previous_summary);
            drop(repository);
            storage.crash();

            let recovered_repository = GenerationRepository::new(storage, policy);
            let recovered = recovered_repository
                .recover_latest()
                .expect("a splice failpoint leaves one recoverable generation")
                .expect("the predecessor generation remains recoverable");
            match recovered.record().generation() {
                2 => {
                    assert_eq!(
                        recovered.namespace_root().inodes()[0].logical_size(),
                        3 * WINDOW_BYTES
                    );
                    assert_eq!(
                        recovered_repository
                            .read_manifest(recovered.namespace_root().inodes()[0].manifest_root())
                            .expect("read recovered predecessor"),
                        splice_predecessor_manifest()
                    );
                }
                3 => {
                    assert_eq!(recovered.record(), next_record);
                    assert_eq!(
                        recovered_repository
                            .read_manifest(recovered.namespace_root().inodes()[0].manifest_root())
                            .expect("read recovered splice successor"),
                        splice_successor_manifest()
                    );
                }
                generation => panic!(
                    "splice fault recovered forbidden mixed generation {generation} at operation {relative_position} (fail_after={fail_after})"
                ),
            }
        }
    }
}

fn seed_splice_predecessor(
    storage: &MemoryStorageIo,
    policy: PolicySetId,
) -> (
    GenerationRepository<MemoryStorageIo>,
    ContainerRepository<MemoryStorageIo>,
    fastdup_store::SuccessorPredecessor,
    fastdup_store::ManifestTreeSummary,
) {
    let repository = GenerationRepository::new(storage.clone(), policy);
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let reservation = repository
        .commit_namespace(
            &NamespaceRoot::new(RESERVATION_END, FILE_INODE, 0, Vec::new(), Vec::new())
                .expect("reservation root is valid"),
        )
        .expect("commit initial inode reservation");
    let predecessor = fastdup_store::SuccessorPredecessor::from_committed_record(reservation);
    let manifest = splice_predecessor_manifest();
    let proof = repository
        .publish_manifest_successor(predecessor, &manifest)
        .expect("publish splice predecessor Manifest");
    let summary = proof.summary();
    let root = NamespaceRoot::new(
        RESERVATION_END,
        FILE_INODE + 1,
        1,
        vec![
            DurableInode::new(
                FILE_INODE,
                0o600,
                0,
                0,
                1,
                1,
                manifest.file_length(),
                summary.root(),
            )
            .expect("splice predecessor inode is valid"),
        ],
        vec![
            NamespaceEntry::new(ROOT_INODE, FILE_INODE, b"splice".to_vec())
                .expect("splice predecessor entry is valid"),
        ],
    )
    .expect("splice predecessor root is valid");
    let committed = repository
        .commit_namespace_with_successor_proofs_using(
            &root,
            &containers,
            predecessor,
            &[proof],
            &containers,
        )
        .expect("commit splice predecessor");
    (
        repository,
        containers,
        fastdup_store::SuccessorPredecessor::from_committed_record(committed.record()),
        summary,
    )
}

#[test]
fn retained_clone_proof_rejects_a_manifest_not_named_by_the_predecessor() {
    let storage = MemoryStorageIo::new();
    let policy = PolicySetId::new([0xa7; 32]).expect("policy ID");
    let (repository, _containers, predecessor, summary) = seed_splice_predecessor(&storage, policy);
    let foreign = ManifestLeaf::new(
        4_096,
        vec![ManifestExtent::Fill {
            logical_length: 4_096,
            value: 0x44,
        }],
    )
    .expect("foreign Manifest");
    let foreign_root = repository
        .publish_manifest(&foreign)
        .expect("publish foreign metadata object");
    assert!(matches!(
        repository.retain_predecessor_manifest_range_successor(
            repository.reuse_manifest_successor(predecessor, summary),
            foreign_root,
            0..4_096,
        ),
        Err(fastdup_store::GenerationError::RetainedManifestNotInPredecessor(root))
            if root == foreign_root
    ));
}

fn publish_splice_generation(
    repository: &GenerationRepository<MemoryStorageIo>,
    containers: &ContainerRepository<MemoryStorageIo>,
    predecessor: fastdup_store::SuccessorPredecessor,
    previous_summary: fastdup_store::ManifestTreeSummary,
) -> Result<fastdup_format::CommitRecord, fastdup_store::GenerationError> {
    let proof = repository.publish_manifest_splice_successor(
        repository.reuse_manifest_successor(predecessor, previous_summary),
        WINDOW_BYTES..2 * WINDOW_BYTES,
        &[
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES / 2,
                value: 0xE1,
            },
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0xE2,
            },
        ],
    )?;
    let root = NamespaceRoot::new(
        RESERVATION_END,
        FILE_INODE + 1,
        2,
        vec![DurableInode::new(
            FILE_INODE,
            0o600,
            0,
            0,
            1,
            2,
            proof.summary().logical_size(),
            proof.summary().root(),
        )?],
        vec![NamespaceEntry::new(
            ROOT_INODE,
            FILE_INODE,
            b"splice".to_vec(),
        )?],
    )?;
    Ok(repository
        .commit_namespace_with_successor_proofs_using(
            &root,
            containers,
            predecessor,
            &[proof],
            containers,
        )?
        .record())
}

fn splice_predecessor_manifest() -> ManifestLeaf {
    ManifestLeaf::new(
        3 * WINDOW_BYTES,
        vec![
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0x11,
            },
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0x22,
            },
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0x33,
            },
        ],
    )
    .expect("splice predecessor Manifest is valid")
}

fn splice_successor_manifest() -> ManifestLeaf {
    ManifestLeaf::new(
        3 * WINDOW_BYTES + WINDOW_BYTES / 2,
        vec![
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0x11,
            },
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES / 2,
                value: 0xE1,
            },
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0xE2,
            },
            ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0x33,
            },
        ],
    )
    .expect("splice successor Manifest is valid")
}

#[test]
fn equal_length_replacement_inside_data_publishes_checked_slices() {
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
    let successor = repository
        .publish_manifest_replacement(
            root,
            data_length,
            1..2,
            &[ManifestExtent::Fill {
                logical_length: 1,
                value: 0x42,
            }],
        )
        .expect("a v2 Manifest can slice DATA without re-ingesting it");
    let extents = repository
        .read_manifest_range(successor, data_length, 0..data_length)
        .expect("read the replacement recipe");
    assert_eq!(
        extents
            .iter()
            .map(|extent| (extent.logical_offset(), extent.extent().clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                0,
                ManifestExtent::DataSlice {
                    logical_length: 1,
                    chunk_id: fastdup_format::ChunkId::of(&data),
                    chunk_length: u32::try_from(data_length).expect("fixture length fits u32"),
                    chunk_offset: 0,
                },
            ),
            (
                1,
                ManifestExtent::Fill {
                    logical_length: 1,
                    value: 0x42,
                },
            ),
            (
                2,
                ManifestExtent::DataSlice {
                    logical_length: data_length - 2,
                    chunk_id: fastdup_format::ChunkId::of(&data),
                    chunk_length: u32::try_from(data_length).expect("fixture length fits u32"),
                    chunk_offset: 2,
                },
            ),
        ],
        "the replacement must retain both untouched byte ranges by Chunk identity"
    );
}

#[test]
fn metadata_scrub_rejects_an_authenticated_wrong_subtree_allocation_summary() {
    let policy = PolicySetId::new([0xD4; 32]).expect("policy identity is nonzero");
    let metadata = MemoryStorageIo::new();
    let repository = GenerationRepository::new(metadata.clone(), policy);
    let extent_count = 1_025_u64;
    let logical_size = WINDOW_BYTES
        .checked_mul(extent_count)
        .expect("worked logical size fits u64");
    let manifest = ManifestLeaf::new(
        logical_size,
        (0..extent_count)
            .map(|_| ManifestExtent::Fill {
                logical_length: WINDOW_BYTES,
                value: 0xA5,
            })
            .collect(),
    )
    .expect("large scrub fixture is valid");
    let root = repository
        .publish_manifest(&manifest)
        .expect("publish v2 scrub fixture");
    assert_eq!(
        repository
            .scrub_manifest_tree_metadata(root)
            .expect("writer output passes metadata scrub")
            .allocated_bytes(),
        logical_size
    );

    let mut corrupted = metadata
        .read(&metadata_name(root))
        .expect("root object exists");
    let allocation_offset = 4_096 + 64 + 48;
    let original = u64::from_le_bytes(
        corrupted[allocation_offset..allocation_offset + 8]
            .try_into()
            .expect("fixed allocation field"),
    );
    corrupted[allocation_offset..allocation_offset + 8]
        .copy_from_slice(&(original - 1).to_le_bytes());
    let corrupt_root = reauthenticate_metadata_object(&mut corrupted);
    publish_exact_metadata(&metadata, corrupt_root, &corrupted);

    assert!(
        matches!(
            repository.scrub_manifest_tree_metadata(corrupt_root),
            Err(fastdup_store::GenerationError::ManifestTree(
                fastdup_store::ManifestTreeError::InvalidTree
            ))
        ),
        "offline metadata scrub must pair the parent summary with the verified child subtree"
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

fn reauthenticate_metadata_object(encoded: &mut [u8]) -> MetadataObjectId {
    let payload_length = usize::try_from(u64::from_le_bytes(
        encoded[32..40].try_into().expect("fixed payload length"),
    ))
    .expect("fixture payload length fits");
    let kind = u16::from_le_bytes(encoded[12..14].try_into().expect("fixed kind"));
    let (payload_crc, id) = {
        let payload = &encoded[4_096..4_096 + payload_length];
        let payload_crc = crc32c::crc32c(payload);
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"fastdup-metadata-object-v1\0");
        hasher.update(&kind.to_le_bytes());
        hasher.update(
            &u64::try_from(payload_length)
                .expect("fixture payload length fits")
                .to_le_bytes(),
        );
        hasher.update(payload);
        let id =
            MetadataObjectId::new(*hasher.finalize().as_bytes()).expect("object ID is nonzero");
        (payload_crc, id)
    };
    encoded[80..84].copy_from_slice(&payload_crc.to_le_bytes());
    encoded[48..80].copy_from_slice(&id.bytes());
    encoded[84..88].fill(0);
    let header_crc = crc32c::crc32c(&encoded[..4_096]);
    encoded[84..88].copy_from_slice(&header_crc.to_le_bytes());
    id
}

fn publish_exact_metadata(storage: &MemoryStorageIo, id: MetadataObjectId, bytes: &[u8]) {
    let final_name = metadata_name(id);
    let temporary = format!(".{final_name}.building");
    storage
        .create_new(&temporary)
        .expect("create corrupt scrub fixture temporary");
    storage
        .write_at(&temporary, 0, bytes)
        .expect("write corrupt scrub fixture");
    storage
        .set_len(
            &temporary,
            u64::try_from(bytes.len()).expect("fixture size fits u64"),
        )
        .expect("set corrupt scrub fixture length");
    storage
        .sync_file(&temporary)
        .expect("sync corrupt scrub fixture bytes");
    storage
        .publish_noreplace(&temporary, &final_name)
        .expect("publish corrupt scrub fixture");
    storage
        .sync_root()
        .expect("sync corrupt scrub fixture name");
}
