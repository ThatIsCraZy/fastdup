use fastdup_format::{
    CommitRecord, CommitRecordHash, MetadataObjectId, PolicySetId,
    RECOVERY_CHECKPOINT_PAYLOAD_OFFSET, RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES,
    RecoveryCheckpointBody, RecoveryCheckpointChain, RecoveryCheckpointDescriptor,
    RecoveryCheckpointHeadRecord, RecoveryCheckpointLink, RecoveryCheckpointObjectFacts,
    RecoveryCheckpointObjectKind, RecoveryCheckpointTableRecord, decode_recovery_checkpoint_table,
};

fn record(generation: u64) -> CommitRecord {
    CommitRecord::new(
        generation,
        if generation == 1 {
            CommitRecordHash::ZERO
        } else {
            CommitRecordHash::of(b"previous checkpoint fixture")
        },
        MetadataObjectId::new([0x91; 32]).expect("namespace identity is nonzero"),
        PolicySetId::new([0x92; 32]).expect("policy identity is nonzero"),
        7,
        1_024,
        9,
    )
    .expect("fixture Commit is valid")
}

fn body(object_count: u64, chain_object_count: u64, hash: u8) -> RecoveryCheckpointBody {
    RecoveryCheckpointBody {
        object_count,
        file_length: 20_480 + object_count * 80,
        body_hash: [hash; 32],
        table_hash: [hash ^ 0xff; 32],
        chain_object_count,
        chain_payload_bytes: 4_096,
    }
}

fn base(generation: u64) -> RecoveryCheckpointDescriptor {
    RecoveryCheckpointDescriptor::new(
        record(generation),
        body(2, 2, 0x93),
        RecoveryCheckpointChain::Base,
        None,
    )
    .expect("Base descriptor is valid")
}

#[test]
fn chain_descriptors_tables_and_head_links_round_trip() {
    let first = base(7);
    let previous = RecoveryCheckpointLink::new(3, 40_960, [0x97; 32]).expect("link is valid");
    let delta = RecoveryCheckpointDescriptor::new(
        record(8),
        body(1, 3, 0x94),
        RecoveryCheckpointChain::Delta {
            parent: first.link(),
            chain_index: 1,
            base_generation: 7,
        },
        Some(previous),
    )
    .expect("Delta descriptor is valid");
    for descriptor in [first, delta] {
        assert_eq!(
            RecoveryCheckpointDescriptor::decode_header(&descriptor.encode_header())
                .expect("header round trips"),
            descriptor
        );
        assert_eq!(
            RecoveryCheckpointDescriptor::decode_footer(&descriptor.encode_footer())
                .expect("footer round trips"),
            descriptor
        );
    }
    assert_eq!(delta.parent(), Some(first.link()));
    assert_eq!(delta.previous_chain(), Some(previous));
    assert_eq!(delta.base_generation(), 7);

    let first_head =
        RecoveryCheckpointHeadRecord::new(first.link(), None).expect("first head is valid");
    let second_head = RecoveryCheckpointHeadRecord::new(delta.link(), Some(first_head))
        .expect("successor head is valid");
    assert_eq!(
        RecoveryCheckpointHeadRecord::decode(&second_head.encode()).expect("head round trips"),
        second_head
    );
    assert_eq!(second_head.previous_generation(), first_head.generation());
    assert_eq!(second_head.previous_record_hash(), first_head.record_hash());

    let leaf = RecoveryCheckpointTableRecord::new(
        MetadataObjectId::new([0x01; 32]).expect("object identity is nonzero"),
        RECOVERY_CHECKPOINT_PAYLOAD_OFFSET,
        4_097,
        0x1234_5678,
        RecoveryCheckpointObjectFacts::manifest_leaf(65_536, 4_096).expect("leaf facts"),
    )
    .expect("leaf record is valid");
    let inner = RecoveryCheckpointTableRecord::new(
        MetadataObjectId::new([0x02; 32]).expect("object identity is nonzero"),
        leaf.padded_end(),
        100,
        7,
        RecoveryCheckpointObjectFacts::manifest_inner(2, 1 << 30, None).expect("inner facts"),
    )
    .expect("inner record is valid");
    let mut table = leaf.encode().to_vec();
    table.extend_from_slice(&inner.encode());
    assert_eq!(
        decode_recovery_checkpoint_table(&table, inner.padded_end()).expect("table decodes"),
        vec![leaf, inner]
    );
}

#[test]
fn table_layout_and_facts_reject_non_canonical_records() {
    let first = RecoveryCheckpointTableRecord::new(
        MetadataObjectId::new([0x05; 32]).expect("object identity is nonzero"),
        RECOVERY_CHECKPOINT_PAYLOAD_OFFSET,
        64,
        1,
        RecoveryCheckpointObjectFacts::namespace(RecoveryCheckpointObjectKind::NamespaceGraphRoot)
            .expect("namespace facts"),
    )
    .expect("record is valid");
    let second = RecoveryCheckpointTableRecord::new(
        MetadataObjectId::new([0x04; 32]).expect("object identity is nonzero"),
        first.padded_end(),
        64,
        2,
        RecoveryCheckpointObjectFacts::namespace(RecoveryCheckpointObjectKind::NamespaceEntryShard)
            .expect("namespace facts"),
    )
    .expect("record is valid");
    let mut descending = first.encode().to_vec();
    descending.extend_from_slice(&second.encode());
    assert!(decode_recovery_checkpoint_table(&descending, second.padded_end()).is_err());
    assert!(
        decode_recovery_checkpoint_table(&first.encode(), first.padded_end() + 64).is_err(),
        "the table must start exactly after the last padded payload"
    );

    let mut encoded = first.encode();
    encoded[56] = 1;
    assert!(
        RecoveryCheckpointTableRecord::decode(&encoded).is_err(),
        "Namespace objects carry no Manifest length"
    );
    let mut encoded = first.encode();
    encoded[RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES - 1] = 1;
    assert!(RecoveryCheckpointTableRecord::decode(&encoded).is_err());
    assert!(RecoveryCheckpointObjectFacts::manifest_leaf(10, 11).is_err());
    assert!(RecoveryCheckpointObjectFacts::manifest_inner(0, 10, Some(1)).is_err());
}

#[test]
fn chain_positions_and_authenticated_fields_reject_inconsistency() {
    let first = base(7);
    let mut header = first.encode_header();
    header[104] ^= 0x01;
    assert!(RecoveryCheckpointDescriptor::decode_header(&header).is_err());

    let head = RecoveryCheckpointHeadRecord::new(first.link(), None).expect("head is valid");
    let mut encoded_head = head.encode();
    encoded_head[32] ^= 0x01;
    assert!(RecoveryCheckpointHeadRecord::decode(&encoded_head).is_err());

    assert!(
        RecoveryCheckpointDescriptor::new(
            record(8),
            body(0, 2, 0x95),
            RecoveryCheckpointChain::Delta {
                parent: first.link(),
                chain_index: 256,
                base_generation: 7,
            },
            None,
        )
        .is_err(),
        "a chain holds at most 256 files"
    );
    assert!(
        RecoveryCheckpointDescriptor::new(
            record(7),
            body(1, 1, 0x95),
            RecoveryCheckpointChain::Delta {
                parent: first.link(),
                chain_index: 1,
                base_generation: 7,
            },
            None,
        )
        .is_err(),
        "a Delta names a strictly older parent"
    );
    assert!(
        RecoveryCheckpointDescriptor::new(
            record(7),
            body(0, 0, 0x95),
            RecoveryCheckpointChain::Base,
            None,
        )
        .is_err(),
        "a Base embeds its complete graph"
    );
    assert!(
        RecoveryCheckpointDescriptor::new(
            record(7),
            body(2, 2, 0x95),
            RecoveryCheckpointChain::Base,
            Some(first.link()),
        )
        .is_err(),
        "a previous chain is older than the Base"
    );
    let empty_delta = RecoveryCheckpointDescriptor::new(
        record(9),
        body(0, 2, 0x96),
        RecoveryCheckpointChain::Delta {
            parent: first.link(),
            chain_index: 1,
            base_generation: 7,
        },
        None,
    )
    .expect("an unchanged graph publishes an empty Delta");
    assert_eq!(
        RecoveryCheckpointDescriptor::decode_footer(&empty_delta.encode_footer())
            .expect("empty Delta round trips"),
        empty_delta
    );
}
