use fastdup_format::{
    DurableInode, METADATA_HEADER_BYTES, MetadataFormatError, MetadataObjectId, NamespaceEntry,
    NamespaceRoot,
};

fn object_id(byte: u8) -> MetadataObjectId {
    MetadataObjectId::new([byte; 32]).expect("fixture object ID is nonzero")
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

    let encoded = root.encode().expect("bounded namespace must encode");

    assert_eq!(&encoded[0..8], b"FDMDOBJ1");
    assert_eq!(&encoded[12..14], &2_u16.to_le_bytes());
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES..METADATA_HEADER_BYTES + 8],
        b"FDNSRT01"
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 40..METADATA_HEADER_BYTES + 48],
        &1_024_u64.to_le_bytes()
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 48..METADATA_HEADER_BYTES + 56],
        &17_u64.to_le_bytes()
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 88..METADATA_HEADER_BYTES + 96],
        &10_u64.to_le_bytes()
    );
    assert_eq!(root.inode_allocation_cursor(), 10);
    let entries_offset = METADATA_HEADER_BYTES + 128 + 2 * 96;
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
    assert_eq!(NamespaceRoot::decode(&encoded), Ok(root));
}

#[test]
fn decoder_rejects_reauthenticated_noncanonical_inode_order() {
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
    let mut encoded = root.encode().expect("root encodes");
    let first = METADATA_HEADER_BYTES + 128;
    let second = first + 96;
    for offset in 0..96 {
        encoded.swap(first + offset, second + offset);
    }
    reauthenticate_metadata_object(&mut encoded);

    assert_eq!(
        NamespaceRoot::decode(&encoded),
        Err(MetadataFormatError::InvalidPayload)
    );
}

#[test]
fn decoder_rejects_impossible_entry_count_before_allocation() {
    let root = NamespaceRoot::new(2, 2, 0, Vec::new(), Vec::new()).expect("empty root is valid");
    let mut encoded = root.encode().expect("root encodes");
    let count_offset = METADATA_HEADER_BYTES + 60;
    encoded[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    reauthenticate_metadata_object(&mut encoded);

    let result = std::panic::catch_unwind(|| NamespaceRoot::decode(&encoded));
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
fn every_truncated_or_single_byte_corrupt_namespace_root_is_rejected_without_panicking() {
    let root = NamespaceRoot::new(
        10,
        3,
        5,
        vec![DurableInode::new(2, 0o640, 1, 2, 1, 3, 4, object_id(2)).expect("valid inode")],
        vec![NamespaceEntry::new(1, 2, vec![b'n', 0xff]).expect("valid entry")],
    )
    .expect("valid root");
    let encoded = root.encode().expect("root encodes");

    for prefix_length in 0..encoded.len() {
        let result = std::panic::catch_unwind(|| NamespaceRoot::decode(&encoded[..prefix_length]));
        assert!(result.is_ok(), "decoder panicked at prefix {prefix_length}");
        assert!(
            result.expect("checked above").is_err(),
            "decoder accepted truncated prefix {prefix_length}"
        );
    }
    for offset in 0..encoded.len() {
        let mut corrupted = encoded.clone();
        corrupted[offset] ^= 1;
        assert!(
            NamespaceRoot::decode(&corrupted).is_err(),
            "decoder accepted corruption at byte {offset}"
        );
    }
}

fn reauthenticate_metadata_object(encoded: &mut [u8]) {
    let payload_length = usize::try_from(u64::from_le_bytes(
        encoded[32..40].try_into().expect("fixed payload length"),
    ))
    .expect("fixture payload length fits");
    let kind = u16::from_le_bytes(encoded[12..14].try_into().expect("fixed kind"));
    let (payload_crc, object_id) = {
        let payload = &encoded[METADATA_HEADER_BYTES..METADATA_HEADER_BYTES + payload_length];
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
        (payload_crc, *hasher.finalize().as_bytes())
    };
    encoded[80..84].copy_from_slice(&payload_crc.to_le_bytes());
    encoded[48..80].copy_from_slice(&object_id);

    encoded[84..88].fill(0);
    let header_crc = crc32c::crc32c(&encoded[..METADATA_HEADER_BYTES]);
    encoded[84..88].copy_from_slice(&header_crc.to_le_bytes());
}
