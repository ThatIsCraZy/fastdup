use fastdup_appliance::{DurableNamespace, recover_mount};
use fastdup_format::{
    DurableInode, ManifestExtent, ManifestLeaf, NamespaceEntry, NamespaceRoot, PolicySetId,
};
use fastdup_posix::{NamespaceConfig, OpenOptions, Operation, ROOT_INODE, Reply, RequestContext};
use fastdup_store::{ContainerRepository, GenerationRepository, SuccessorPredecessor};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const CALLER: RequestContext = RequestContext {
    uid: 1000,
    gid: 1000,
    pid: 61,
};

#[test]
fn growing_a_large_manifest_while_rewriting_its_header_stays_path_local() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let containers = ContainerRepository::new(data.clone());
    containers.open_generation_allocator(1024).unwrap();
    let policy = PolicySetId::new([0xe9; 32]).unwrap();
    let generations = GenerationRepository::new(metadata.clone(), policy);
    let initial = generations
        .commit_namespace(&NamespaceRoot::new(4096, 2, 0, vec![], vec![]).unwrap())
        .unwrap();
    let half: Vec<_> = (0..131_072)
        .map(|i| ManifestExtent::Fill {
            logical_length: 512,
            value: if i % 2 == 0 { 31 } else { 91 },
        })
        .collect();
    let first = generations
        .publish_manifest_successor(
            SuccessorPredecessor::from_committed_record(initial),
            &ManifestLeaf::new(67_108_864, half.clone()).unwrap(),
        )
        .unwrap();
    let root = |length, manifest, sequence| {
        NamespaceRoot::new(
            4096,
            3,
            sequence,
            vec![DurableInode::new(2, 0o640, 1000, 1000, 1, sequence, length, manifest).unwrap()],
            vec![NamespaceEntry::new(1, 2, b"backup".to_vec()).unwrap()],
        )
        .unwrap()
    };
    let committed = generations
        .commit_namespace_with_successor_proofs_using(
            &root(67_108_864, first.summary().root(), 1),
            &containers,
            SuccessorPredecessor::from_committed_record(initial),
            &[first],
            &containers,
        )
        .unwrap();
    let (record, files) = committed.into_parts();
    let second = generations
        .publish_manifest_append(
            SuccessorPredecessor::from_committed_record(record),
            files[0].manifest_summary().unwrap(),
            &half,
        )
        .unwrap();
    let summary = second.summary();
    generations
        .commit_namespace_with_successor_proofs_using(
            &root(134_217_728, summary.root(), 2),
            &containers,
            SuccessorPredecessor::from_committed_record(record),
            &[second],
            &containers,
        )
        .unwrap();
    assert!(
        generations.read_manifest(summary.root()).is_err(),
        "fixture must exceed the flat Manifest limit"
    );
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        generations.clone(),
        containers.clone(),
        16,
    )
    .unwrap();
    let Reply::Entry(entry) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"backup",
            },
        )
        .unwrap()
    else {
        panic!("lookup")
    };
    let inode = entry.attr.inode;
    let Reply::Opened(handle) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_WRITE,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("open")
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 0,
                data: b"HEAD",
            },
        )
        .unwrap();
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode,
                handle,
                offset: 134_217_728,
                data: b"TAIL",
            },
        )
        .unwrap();
    let metadata_baseline = metadata.operation_count();
    appliance
        .checkpoint()
        .expect("mixed overwrite and growth must not flatten the complete prior Manifest")
        .unwrap();
    let metadata_reads = metadata.operations()[metadata_baseline..]
        .iter()
        .filter(|operation| **operation == StorageOperation::Read)
        .count();
    assert!(
        metadata_reads < 64,
        "mixed growth must read only touched tree paths: {metadata_reads}"
    );
    drop(appliance);
    metadata.crash();
    data.crash();
    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy),
        &ContainerRepository::new(data),
    )
    .unwrap()
    .unwrap();
    let Reply::Opened(handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .unwrap()
    else {
        panic!("reopen")
    };
    assert_eq!(
        recovered
            .dispatch(
                CALLER,
                Operation::Read {
                    inode,
                    handle,
                    offset: 0,
                    length: 8
                }
            )
            .unwrap(),
        Reply::Data(vec![b'H', b'E', b'A', b'D', 31, 31, 31, 31])
    );
    assert_eq!(
        recovered
            .dispatch(
                CALLER,
                Operation::Read {
                    inode,
                    handle,
                    offset: 134_217_724,
                    length: 8
                }
            )
            .unwrap(),
        Reply::Data(vec![91, 91, 91, 91, b'T', b'A', b'I', b'L'])
    );
    generations.scrub_all_with_data(&containers).unwrap();
}

#[test]
fn appended_successor_still_requires_data_introduced_by_earlier_replacement() {
    use fastdup_format::{ChunkId, ContainerId};
    let metadata = MemoryStorageIo::new();
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let generations = GenerationRepository::new(metadata, PolicySetId::new([0xea; 32]).unwrap());
    generations
        .commit_namespace(&NamespaceRoot::new(4096, 2, 0, vec![], vec![]).unwrap())
        .unwrap();
    let manifest = generations
        .publish_manifest(
            &ManifestLeaf::new(
                16,
                vec![ManifestExtent::Fill {
                    logical_length: 16,
                    value: 31,
                }],
            )
            .unwrap(),
        )
        .unwrap();
    let root = |length, manifest, sequence| {
        NamespaceRoot::new(
            4096,
            3,
            sequence,
            vec![DurableInode::new(2, 0o640, 1000, 1000, 1, sequence, length, manifest).unwrap()],
            vec![NamespaceEntry::new(1, 2, b"backup".to_vec()).unwrap()],
        )
        .unwrap()
    };
    let record = generations
        .commit_namespace(&root(16, manifest, 1))
        .unwrap();
    let (_, files) = generations
        .recover_latest_with_verified_files(&containers)
        .unwrap()
        .unwrap()
        .into_parts();
    let predecessor = SuccessorPredecessor::from_committed_record(record);
    let proof =
        generations.reuse_manifest_successor(predecessor, files[0].manifest_summary().unwrap());
    let proof = generations
        .stage_manifest_replacement_successor(
            proof,
            0..4,
            &[ManifestExtent::Data {
                logical_length: 4,
                chunk_id: ChunkId::of(b"HEAD"),
            }],
        )
        .unwrap();
    let proof = generations
        .stage_manifest_append_successor(
            proof,
            &[ManifestExtent::Data {
                logical_length: 4,
                chunk_id: ChunkId::of(b"TAIL"),
            }],
        )
        .unwrap();
    containers
        .publish_raw(
            ContainerId::new([0xeb; 16]).unwrap(),
            1,
            &[b"TAIL".as_slice()],
        )
        .unwrap();
    assert!(
        generations
            .commit_namespace_with_successor_proofs_using(
                &root(20, proof.summary().root(), 2),
                &containers,
                predecessor,
                &[proof],
                &containers
            )
            .is_err(),
        "appending must not discard the missing HEAD dependency from the prior replacement"
    );
    assert_eq!(
        generations.recover_latest().unwrap().unwrap().record(),
        record
    );
}
