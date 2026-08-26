use fastdup_format::{
    METADATA_MARK_CATALOG_HEADER_BYTES, METADATA_MARK_CATALOG_ROW_BYTES,
    MetadataMarkCatalogDescriptor, MetadataMarkCatalogError, MetadataMarkCatalogRunKind,
    MetadataMarkCatalogStreamEncoder, MetadataObjectId,
};

#[test]
fn metadata_mark_catalog_round_trips_sorted_rows_and_rejects_row_corruption() {
    let first = MetadataObjectId::new([0x11; 32]).expect("object ID is nonzero");
    let second = MetadataObjectId::new([0x22; 32]).expect("object ID is nonzero");
    let mut encoder =
        MetadataMarkCatalogStreamEncoder::new(7, [0xA5; 32], 2).expect("catalog layout is valid");
    let (first_offset, first_bytes) = encoder.push(first).expect("first row is valid");
    let (second_offset, second_bytes) = encoder.push(second).expect("second row is valid");
    assert_eq!(first_offset, 4_096);
    assert_eq!(second_offset, 4_096 + 32);
    let (expected, header, footer) = encoder.finish().expect("complete catalog is valid");
    let first_offset = usize::try_from(first_offset).expect("fixture offset fits usize");
    let second_offset = usize::try_from(second_offset).expect("fixture offset fits usize");
    let footer_offset =
        usize::try_from(expected.footer_offset()).expect("fixture footer fits usize");
    let mut catalog_bytes =
        vec![0_u8; usize::try_from(expected.file_length()).expect("fixture length fits usize")];
    catalog_bytes[..METADATA_MARK_CATALOG_HEADER_BYTES].copy_from_slice(&header);
    catalog_bytes[first_offset..first_offset + METADATA_MARK_CATALOG_ROW_BYTES]
        .copy_from_slice(&first_bytes);
    catalog_bytes[second_offset..second_offset + METADATA_MARK_CATALOG_ROW_BYTES]
        .copy_from_slice(&second_bytes);
    catalog_bytes[footer_offset..].copy_from_slice(&footer);

    let decoded = MetadataMarkCatalogDescriptor::decode(
        &catalog_bytes[..METADATA_MARK_CATALOG_HEADER_BYTES],
        &catalog_bytes[footer_offset..],
        u64::try_from(catalog_bytes.len()).expect("fixture length fits u64"),
    )
    .expect("paired envelopes decode");
    assert_eq!(decoded, expected);
    let mut audit = decoded.start_audit();
    assert_eq!(
        audit
            .push(&catalog_bytes[first_offset..first_offset + 32])
            .expect("first row audits"),
        first
    );
    assert_eq!(
        audit
            .push(&catalog_bytes[second_offset..second_offset + 32])
            .expect("second row audits"),
        second
    );
    audit.finish().expect("row stream hash audits");

    catalog_bytes[first_offset + 31] ^= 0x01;
    let mut corrupt = decoded.start_audit();
    corrupt
        .push(&catalog_bytes[first_offset..first_offset + 32])
        .expect("changed row remains structurally decodable");
    corrupt
        .push(&catalog_bytes[second_offset..second_offset + 32])
        .expect("second row remains structurally valid");
    assert_eq!(
        corrupt.finish(),
        Err(MetadataMarkCatalogError::RowsHashMismatch)
    );
}

#[test]
fn metadata_mark_addition_binds_its_immediate_catalog_base() {
    let object_id = MetadataObjectId::new([0x44; 32]).expect("object ID is nonzero");
    let mut encoder = MetadataMarkCatalogStreamEncoder::new_addition(8, 7, [0x55; 32], 1)
        .expect("additive catalog layout is valid");
    encoder.push(object_id).expect("addition row is valid");
    let (expected, header, footer) = encoder.finish().expect("addition run completes");

    let decoded = MetadataMarkCatalogDescriptor::decode(&header, &footer, expected.file_length())
        .expect("paired addition envelopes decode");

    assert_eq!(decoded.run_kind(), MetadataMarkCatalogRunKind::Addition);
    assert_eq!(decoded.generation(), 8);
    assert_eq!(decoded.base_generation(), 7);
    assert_eq!(
        MetadataMarkCatalogStreamEncoder::new_addition(8, 0, [0x55; 32], 1).err(),
        Some(MetadataMarkCatalogError::InvalidEnvelope)
    );
    assert_eq!(
        MetadataMarkCatalogStreamEncoder::new_addition(8, 8, [0x55; 32], 1).err(),
        Some(MetadataMarkCatalogError::InvalidEnvelope)
    );
}

#[test]
fn metadata_mark_catalog_decoder_accepts_legacy_v1_snapshots() {
    fn envelope(magic: [u8; 8]) -> [u8; METADATA_MARK_CATALOG_HEADER_BYTES] {
        let mut bytes = [0_u8; METADATA_MARK_CATALOG_HEADER_BYTES];
        bytes[0..8].copy_from_slice(&magic);
        bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
        bytes[10..12].copy_from_slice(&4_096_u16.to_le_bytes());
        bytes[12..14].copy_from_slice(&32_u16.to_le_bytes());
        bytes[16..24].copy_from_slice(&9_u64.to_le_bytes());
        bytes[24..56].copy_from_slice(&[0x66; 32]);
        bytes[56..64].copy_from_slice(&0_u64.to_le_bytes());
        bytes[64..72].copy_from_slice(&4_096_u64.to_le_bytes());
        bytes[72..80].copy_from_slice(&4_096_u64.to_le_bytes());
        bytes[80..88].copy_from_slice(&8_192_u64.to_le_bytes());
        let mut rows = blake3::Hasher::new();
        rows.update(b"fastdup-metadata-mark-rows-v1\0");
        rows.update(&9_u64.to_le_bytes());
        rows.update(&[0x66; 32]);
        rows.update(&0_u64.to_le_bytes());
        bytes[88..120].copy_from_slice(rows.finalize().as_bytes());
        let mut envelope = blake3::Hasher::new();
        envelope.update(b"fastdup-metadata-mark-envelope-v1\0");
        envelope.update(&bytes[..128]);
        envelope.update(&[0; 32]);
        envelope.update(&bytes[160..]);
        bytes[128..160].copy_from_slice(envelope.finalize().as_bytes());
        bytes
    }

    let header = envelope(*b"FDMMARK1");
    let footer = envelope(*b"FDMMARKF");
    let decoded = MetadataMarkCatalogDescriptor::decode(&header, &footer, 8_192)
        .expect("legacy v1 snapshot remains readable");

    assert_eq!(decoded.run_kind(), MetadataMarkCatalogRunKind::Snapshot);
    assert_eq!(decoded.generation(), 9);
    assert_eq!(decoded.base_generation(), 0);
    decoded
        .start_audit()
        .finish()
        .expect("legacy empty row stream remains auditable");
}

#[test]
fn metadata_mark_catalog_rejects_noncanonical_order_and_envelope_mutation() {
    let first = MetadataObjectId::new([0x11; 32]).expect("object ID is nonzero");
    let second = MetadataObjectId::new([0x22; 32]).expect("object ID is nonzero");
    let mut encoder =
        MetadataMarkCatalogStreamEncoder::new(1, [0x33; 32], 2).expect("catalog layout is valid");
    encoder.push(second).expect("first emitted row is valid");
    assert_eq!(
        encoder.push(first),
        Err(MetadataMarkCatalogError::NonCanonicalOrder)
    );

    let encoder = MetadataMarkCatalogStreamEncoder::new(1, [0x33; 32], 0)
        .expect("empty catalog layout is valid");
    let (descriptor, mut header, footer) = encoder.finish().expect("empty catalog completes");
    header[24] ^= 0x80;
    assert_eq!(
        MetadataMarkCatalogDescriptor::decode(&header, &footer, descriptor.file_length()),
        Err(MetadataMarkCatalogError::EnvelopeHashMismatch)
    );
}
