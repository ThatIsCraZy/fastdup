use std::collections::BTreeMap;

use fastdup_format::{
    DurableInode, DurableInodeKind, DurableRootMetadata, DurableTimes, DurableTimestamp,
    DurableXattr, MetadataFormatError, MetadataObjectId, NamespaceEntry, NamespaceGraphRoot,
    NamespaceRoot,
};

fn object_id(byte: u8) -> MetadataObjectId {
    MetadataObjectId::new([byte; 32]).expect("fixture object ID is nonzero")
}

fn large_xattr_namespace(changed_inode: Option<u64>) -> NamespaceRoot {
    let mut inodes = Vec::new();
    let mut entries = Vec::new();
    for ordinal in 0_u64..280 {
        let inode = ordinal + 2;
        let fill = if changed_inode == Some(inode) {
            0x5A
        } else {
            0xA5
        };
        inodes.push(
            DurableInode::new_with_metadata(
                inode,
                0o600,
                1_000,
                1_000,
                1,
                ordinal + 1,
                0,
                object_id(0x44),
                0,
                vec![
                    DurableXattr::new(b"user.large".to_vec(), vec![fill; 60 * 1_024])
                        .expect("bounded xattr"),
                ],
            )
            .expect("durable inode"),
        );
        entries.push(
            NamespaceEntry::new(1, inode, format!("file-{ordinal:04}").into_bytes())
                .expect("namespace entry"),
        );
    }
    NamespaceRoot::new(
        1_024,
        282,
        if changed_inode.is_some() { 281 } else { 280 },
        inodes,
        entries,
    )
    .expect("large namespace")
}

#[test]
fn sharded_graph_round_trips_beyond_object_bound_and_rejects_missing_or_corrupt_children() {
    let root = large_xattr_namespace(None);
    assert!(root.encode_canonical_state().unwrap().len() > 16 * 1_024 * 1_024);

    let graph = root.encode_graph().expect("bounded graph encoding");
    assert!(graph.shards().len() > 1);
    let shards = graph
        .shards()
        .iter()
        .map(|shard| (shard.object_id(), shard.bytes().to_vec()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(NamespaceRoot::decode_graph(graph.root(), &shards), Ok(root));

    let mut missing = shards.clone();
    missing.pop_first();
    assert_eq!(
        NamespaceRoot::decode_graph(graph.root(), &missing),
        Err(MetadataFormatError::InvalidPayload)
    );

    let mut corrupt = shards;
    corrupt.first_entry().expect("graph has a shard").get_mut()[0] ^= 1;
    assert!(NamespaceRoot::decode_graph(graph.root(), &corrupt).is_err());
}

#[test]
fn gc_namespace_view_matches_full_graph_reachability_and_transition_inputs() {
    let root = NamespaceRoot::new(
        4,
        4,
        2,
        vec![
            DurableInode::new_directory(2, 0o750, 1_000, 1_000, 2, 1).expect("directory is valid"),
            DurableInode::new(3, 0o600, 1_000, 1_000, 1, 2, 8, object_id(0x33))
                .expect("file is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, b"directory".to_vec()).expect("directory entry"),
            NamespaceEntry::new(2, 3, b"file".to_vec()).expect("file entry"),
        ],
    )
    .expect("mixed namespace is valid");
    let graph = root.encode_graph().expect("graph encodes");
    let shards = graph
        .shards()
        .iter()
        .map(|shard| (shard.object_id(), shard.bytes().to_vec()))
        .collect::<BTreeMap<_, _>>();
    let full = NamespaceRoot::decode_graph(graph.root(), &shards).expect("graph decodes fully");
    let view = NamespaceGraphRoot::decode(graph.root())
        .expect("descriptor decodes")
        .decode_gc_graph_with_shards(&shards)
        .expect("gc view decodes");
    assert_eq!(
        view.inode_transitions(),
        full.inodes()
            .iter()
            .map(|inode| (inode.inode(), inode.mutation_sequence()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        view.manifest_roots(),
        full.file_inodes()
            .map(|inode| inode
                .file_manifest_root()
                .expect("regular inode has a manifest root"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        view.namespace_object_ids(),
        graph
            .shards()
            .iter()
            .map(fastdup_format::EncodedNamespaceShard::object_id)
            .collect::<Vec<_>>()
    );
}

/// Shard boundaries come from the record keys alone, so a commit that changes
/// one inode may only replace the one shard that inode lives in. That is the
/// whole point of ADR 0095: without it a commit re-encodes, re-hashes and
/// republishes the entire Namespace no matter how little changed.
#[test]
fn changing_one_inode_replaces_exactly_one_namespace_shard() {
    let before = large_xattr_namespace(None).encode_graph().unwrap();
    let after = large_xattr_namespace(Some(142)).encode_graph().unwrap();
    let ids = |graph: &fastdup_format::EncodedNamespaceGraph| {
        graph
            .shards()
            .iter()
            .map(fastdup_format::EncodedNamespaceShard::object_id)
            .collect::<std::collections::BTreeSet<_>>()
    };
    let before_ids = ids(&before);
    let after_ids = ids(&after);
    assert!(
        before_ids.len() > 2,
        "the fixture must span several shards: {}",
        before_ids.len()
    );
    assert_eq!(
        before_ids.difference(&after_ids).count(),
        1,
        "one changed inode must retire exactly one shard: {before_ids:?} -> {after_ids:?}"
    );
    assert_eq!(
        after_ids.difference(&before_ids).count(),
        1,
        "one changed inode must publish exactly one new shard"
    );
    assert_eq!(
        before.shards().len(),
        after.shards().len(),
        "a value-only edit must not repartition the Namespace"
    );
}

#[test]
fn version_four_round_trips_timestamps_and_byte_exact_symlinks() {
    let times = DurableTimes {
        atime: DurableTimestamp {
            seconds: -2,
            nanoseconds: 3,
        },
        mtime: DurableTimestamp {
            seconds: 4,
            nanoseconds: 5,
        },
        ctime: DurableTimestamp {
            seconds: 6,
            nanoseconds: 7,
        },
    };
    let root = NamespaceRoot::new_with_root_metadata(
        16,
        3,
        9,
        DurableRootMetadata::default().with_times(times),
        vec![
            DurableInode::new_symlink(2, 1_000, 1_001, 2, 11, b"../target/\xff".to_vec())
                .unwrap()
                .with_times(times),
        ],
        vec![
            NamespaceEntry::new(1, 2, b"one".to_vec()).unwrap(),
            NamespaceEntry::new(1, 2, b"two".to_vec()).unwrap(),
        ],
    )
    .unwrap();
    let encoded = root.encode_canonical_state().unwrap();
    assert_eq!(&encoded[8..10], &4_u16.to_le_bytes());
    assert_eq!(NamespaceRoot::decode_canonical_state(&encoded), Ok(root));
}

#[test]
fn nested_directory_namespace_round_trips_and_rejects_cycles() {
    let root = NamespaceRoot::new(
        32,
        5,
        7,
        vec![
            DurableInode::new_directory(2, 0o750, 1_000, 1_000, 3, 1)
                .expect("parent directory is valid"),
            DurableInode::new_directory(3, 0o700, 1_000, 1_000, 2, 2)
                .expect("child directory is valid"),
            DurableInode::new(4, 0o600, 1_000, 1_000, 1, 3, 8, object_id(4))
                .expect("regular file is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, b"parent".to_vec()).expect("root entry is valid"),
            NamespaceEntry::new(2, 3, b"child".to_vec()).expect("nested entry is valid"),
            NamespaceEntry::new(3, 4, b"file".to_vec()).expect("file entry is valid"),
        ],
    )
    .expect("nested namespace is valid");
    let encoded = root
        .encode_canonical_state()
        .expect("nested namespace encodes");
    let decoded =
        NamespaceRoot::decode_canonical_state(&encoded).expect("nested namespace decodes");
    assert_eq!(decoded, root);
    assert_eq!(decoded.inodes()[0].kind(), DurableInodeKind::Directory);
    assert_eq!(decoded.inodes()[2].kind(), DurableInodeKind::Regular);

    let cycle = NamespaceRoot::new(
        32,
        4,
        8,
        vec![
            DurableInode::new_directory(2, 0o755, 0, 0, 3, 1)
                .expect("cycle fixture directory is locally valid"),
            DurableInode::new_directory(3, 0o755, 0, 0, 3, 1)
                .expect("cycle fixture directory is locally valid"),
        ],
        vec![
            NamespaceEntry::new(2, 3, b"three".to_vec()).expect("component is valid"),
            NamespaceEntry::new(3, 2, b"two".to_vec()).expect("component is valid"),
        ],
    );
    assert_eq!(cycle, Err(MetadataFormatError::InvalidPayload));
}

#[test]
fn namespace_root_has_stable_bytes_and_round_trips_byte_exact_hardlinks() {
    let root = NamespaceRoot::new(
        1_024,
        10,
        17,
        vec![
            DurableInode::new(2, 0o640, 1_000, 1_001, 2, 9, 15, object_id(0x22))
                .expect("regular inode is valid"),
            DurableInode::new(9, 0o600, 2_000, 2_001, 1, 3, 0, object_id(0x99))
                .expect("empty regular inode is valid"),
        ],
        vec![
            NamespaceEntry::new(1, 2, b"backup.raw".to_vec()).expect("valid entry"),
            NamespaceEntry::new(1, 2, vec![b'b', 0xff]).expect("byte-exact entry"),
            NamespaceEntry::new(1, 9, b"empty".to_vec()).expect("valid entry"),
        ],
    )
    .expect("worked namespace root is valid");

    let encoded = root
        .encode_canonical_state()
        .expect("canonical namespace must encode");

    assert_eq!(&encoded[0..8], b"FDNSRT01");
    assert_eq!(&encoded[40..48], &1_024_u64.to_le_bytes());
    assert_eq!(&encoded[48..56], &17_u64.to_le_bytes());
    assert_eq!(&encoded[88..96], &10_u64.to_le_bytes());
    assert_eq!(root.inode_allocation_cursor(), 10);
    let entries_offset = 128 + 2 * 96;
    assert_eq!(
        &encoded[entries_offset..entries_offset + 4],
        &40_u32.to_le_bytes()
    );
    assert_eq!(
        &encoded[entries_offset + 24..entries_offset + 34],
        b"backup.raw"
    );
    assert!(
        encoded[entries_offset + 34..entries_offset + 40]
            .iter()
            .all(|byte| *byte == 0)
    );
    assert_eq!(NamespaceRoot::decode_canonical_state(&encoded), Ok(root));
}

#[test]
fn current_format_round_trips_root_file_flags_xattrs_and_posix_acls() {
    let access_acl = acl(&[
        (0x01, 0o7, u32::MAX),
        (0x02, 0o6, 2_000),
        (0x04, 0o5, u32::MAX),
        (0x10, 0o4, u32::MAX),
        (0x20, 0o1, u32::MAX),
    ]);
    let root_metadata = DurableRootMetadata::new(
        0o750,
        10,
        20,
        0,
        vec![DurableXattr::new(b"user.root".to_vec(), b"metadata".to_vec()).expect("root xattr")],
    )
    .expect("root metadata");
    let inode = DurableInode::new_with_metadata(
        2,
        0o741,
        1_000,
        1_001,
        1,
        9,
        15,
        object_id(0x22),
        0x10,
        vec![
            DurableXattr::new(POSIX_ACL_ACCESS.to_vec(), access_acl).expect("access ACL"),
            DurableXattr::new(
                b"user.immutable.until".to_vec(),
                b"2030-01-01 00:00:00".to_vec(),
            )
            .expect("retention xattr"),
        ],
    )
    .expect("regular inode metadata");
    let root = NamespaceRoot::new_with_root_metadata(
        16,
        3,
        11,
        root_metadata,
        vec![inode],
        vec![NamespaceEntry::new(1, 2, b"backup.vbk".to_vec()).expect("entry")],
    )
    .expect("namespace root");
    let encoded = root
        .encode_canonical_state()
        .expect("current namespace encodes");
    assert_eq!(&encoded[8..10], &4_u16.to_le_bytes());
    assert_eq!(NamespaceRoot::decode_canonical_state(&encoded), Ok(root));
}

const POSIX_ACL_ACCESS: &[u8] = b"system.posix_acl_access";

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
fn canonical_decoder_rejects_noncanonical_inode_order() {
    let root = NamespaceRoot::new(
        100,
        4,
        2,
        vec![
            DurableInode::new(2, 0o600, 10, 20, 1, 1, 3, object_id(2)).expect("valid inode"),
            DurableInode::new(3, 0o600, 10, 20, 1, 1, 3, object_id(3)).expect("valid inode"),
        ],
        vec![
            NamespaceEntry::new(1, 2, b"aa".to_vec()).expect("valid entry"),
            NamespaceEntry::new(1, 3, b"bb".to_vec()).expect("valid entry"),
        ],
    )
    .expect("valid root");
    let mut encoded = root.encode_canonical_state().expect("root encodes");
    let first = 128;
    let second = first + 96;
    for offset in 0..96 {
        encoded.swap(first + offset, second + offset);
    }
    assert_eq!(
        NamespaceRoot::decode_canonical_state(&encoded),
        Err(MetadataFormatError::InvalidPayload)
    );
}

#[test]
fn decoder_rejects_impossible_entry_count_before_allocation() {
    let root = NamespaceRoot::new(2, 2, 0, Vec::new(), Vec::new()).expect("empty root is valid");
    let mut encoded = root.encode_canonical_state().expect("root encodes");
    let count_offset = 60;
    encoded[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());

    let result = std::panic::catch_unwind(|| NamespaceRoot::decode_canonical_state(&encoded));
    assert!(
        matches!(result, Ok(Err(MetadataFormatError::InvalidPayload))),
        "corrupt counts must fail before reserving attacker-selected memory"
    );
}

#[test]
fn writer_rejects_duplicates_dangling_entries_orphans_and_inode_reuse() {
    let inode = || DurableInode::new(2, 0o600, 10, 20, 1, 4, 5, object_id(2)).expect("valid inode");
    let entry = || NamespaceEntry::new(1, 2, b"file".to_vec()).expect("valid entry");

    assert_eq!(
        NamespaceRoot::new(2, 3, 0, vec![inode()], vec![entry()]),
        Err(MetadataFormatError::InvalidPayload),
        "reservation end is exclusive and must be above every inode"
    );
    assert_eq!(
        NamespaceRoot::new(3, 3, 0, vec![inode(), inode()], vec![entry()]),
        Err(MetadataFormatError::InvalidPayload),
        "inode identities must be unique"
    );
    assert_eq!(
        NamespaceRoot::new(3, 3, 0, vec![inode()], Vec::new()),
        Err(MetadataFormatError::InvalidPayload),
        "an unlinked inode is an open orphan and cannot be committed"
    );
    assert_eq!(
        NamespaceRoot::new(
            4,
            3,
            0,
            vec![inode()],
            vec![NamespaceEntry::new(1, 3, b"dangling".to_vec()).expect("valid component")],
        ),
        Err(MetadataFormatError::InvalidPayload),
        "every entry target must have an inode version"
    );
    assert_eq!(
        NamespaceRoot::new(3, 3, 0, vec![inode()], vec![entry(), entry()]),
        Err(MetadataFormatError::InvalidPayload),
        "root names must be unique"
    );
    assert_eq!(
        NamespaceEntry::new(1, 2, vec![b'a', 0]),
        Err(MetadataFormatError::InvalidPayload)
    );
    assert_eq!(
        NamespaceEntry::new(1, 2, b"a/b".to_vec()),
        Err(MetadataFormatError::InvalidPayload)
    );
}

#[test]
fn every_truncated_or_single_byte_corrupt_namespace_graph_object_is_rejected_without_panicking() {
    let root = NamespaceRoot::new(
        10,
        3,
        5,
        vec![DurableInode::new(2, 0o640, 1, 2, 1, 3, 4, object_id(2)).expect("valid inode")],
        vec![NamespaceEntry::new(1, 2, vec![b'n', 0xff]).expect("valid entry")],
    )
    .expect("valid root");
    let graph = root.encode_graph().expect("root graph encodes");
    let shards = graph
        .shards()
        .iter()
        .map(|shard| (shard.object_id(), shard.bytes().to_vec()))
        .collect::<BTreeMap<_, _>>();

    for prefix_length in 0..graph.root().len() {
        let result = std::panic::catch_unwind(|| {
            NamespaceRoot::decode_graph(&graph.root()[..prefix_length], &shards)
        });
        assert!(result.is_ok(), "decoder panicked at prefix {prefix_length}");
        assert!(
            result.expect("checked above").is_err(),
            "decoder accepted truncated prefix {prefix_length}"
        );
    }
    for offset in 0..graph.root().len() {
        let mut corrupted = graph.root().to_vec();
        corrupted[offset] ^= 1;
        assert!(
            NamespaceRoot::decode_graph(&corrupted, &shards).is_err(),
            "decoder accepted root corruption at byte {offset}"
        );
    }
    for (shard_id, shard) in &shards {
        for offset in 0..shard.len() {
            let mut corrupted = shards.clone();
            corrupted.get_mut(shard_id).expect("selected shard exists")[offset] ^= 1;
            assert!(
                NamespaceRoot::decode_graph(graph.root(), &corrupted).is_err(),
                "decoder accepted shard corruption at byte {offset}"
            );
        }
    }
}

/// One directory's entries are a contiguous run in the canonical order, which
/// is what lets the reachability proof walk them without a child map. The tree
/// below interleaves two parents around a third so a run that ended early or
/// started late would leave an inode unreachable.
#[test]
fn interleaved_directory_runs_prove_reachability_and_directory_link_counts() {
    let file = |inode: u64| {
        DurableInode::new(inode, 0o600, 10, 20, 1, 4, 5, object_id(2)).expect("regular inode")
    };
    let directory = |inode: u64, links: u32| {
        DurableInode::new_directory(inode, 0o750, 10, 20, links, 1).expect("directory inode")
    };
    let entry = |parent: u64, target: u64, name: &[u8]| {
        NamespaceEntry::new(parent, target, name.to_vec()).expect("valid entry")
    };
    let inodes = |first_directory_links: u32| {
        vec![
            directory(2, first_directory_links),
            directory(3, 2),
            file(4),
            directory(5, 2),
            file(6),
            file(7),
        ]
    };
    let entries = vec![
        entry(1, 2, b"a"),
        entry(1, 3, b"b"),
        entry(2, 4, b"c"),
        entry(2, 5, b"d"),
        entry(3, 6, b"e"),
        entry(5, 7, b"f"),
    ];

    let root = NamespaceRoot::new(1_024, 8, 0, inodes(3), entries.clone())
        .expect("a reachable tree with matching link counts is valid");
    assert_eq!(root.inodes().len(), 6);
    assert_eq!(
        NamespaceRoot::decode_canonical_state(
            &root.encode_canonical_state().expect("encode the tree")
        ),
        Ok(root),
        "the encoder must round-trip the tree byte-exactly"
    );

    assert_eq!(
        NamespaceRoot::new(1_024, 8, 0, inodes(2), entries.clone()),
        Err(MetadataFormatError::InvalidPayload),
        "a directory's link count must include its child directories"
    );
    let mut parented_by_a_file = entries;
    parented_by_a_file.push(entry(4, 7, b"g"));
    parented_by_a_file.sort_by(|left, right| {
        (left.parent_inode(), left.name()).cmp(&(right.parent_inode(), right.name()))
    });
    assert_eq!(
        NamespaceRoot::new(1_024, 8, 0, inodes(3), parented_by_a_file),
        Err(MetadataFormatError::InvalidPayload),
        "only a directory may parent an entry"
    );
}

/// Reports the per-generation commit cost of one whole Namespace. Every
/// checkpoint pays construction, proof and encoding once, so this is the budget
/// that grows with the pool rather than with the change.
#[test]
#[ignore = "A/B timing benchmark; run explicitly in release mode with --nocapture"]
fn benchmark_whole_namespace_commit_encoding() {
    const ENTRIES: u64 = 120_000;
    let inodes = (2..2 + ENTRIES)
        .map(|inode| {
            DurableInode::new_directory(inode, 0o750, 10, 20, 2, 1).expect("directory inode")
        })
        .collect::<Vec<_>>();
    let entries = (2..2 + ENTRIES)
        .map(|inode| {
            NamespaceEntry::new(1, inode, format!("fixture-{inode:016}").into_bytes())
                .expect("valid entry")
        })
        .collect::<Vec<_>>();

    let construct = std::time::Instant::now();
    let root = NamespaceRoot::new(4_000_000, ENTRIES + 2, ENTRIES, inodes, entries)
        .expect("valid namespace");
    let construct = construct.elapsed();
    let encode = std::time::Instant::now();
    let payload = root
        .encode_canonical_state()
        .expect("encode canonical state");
    let encode = encode.elapsed();
    let graph = std::time::Instant::now();
    let encoded = root.encode_graph().expect("encode graph");
    let graph = graph.elapsed();
    eprintln!(
        "namespace_commit entries={ENTRIES} payload_bytes={} shards={} \
         construct_and_prove={construct:?} encode={encode:?} encode_graph={graph:?}",
        payload.len(),
        encoded.shards().len()
    );
}
