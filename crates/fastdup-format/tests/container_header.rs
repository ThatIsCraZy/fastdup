use fastdup_format::{
    BuildingContainerHeader, ContainerHeader, ContainerId, ContainerLayout, FormatError,
    HEADER_BYTES,
};

fn example_header() -> ContainerHeader {
    ContainerHeader::sealed(
        ContainerId::new([0x5a; 16]).expect("nonzero container id"),
        42,
        ContainerLayout {
            record_count: 2,
            chunk_entry_count: 2,
            index_offset: 8_192,
            index_length: 320,
            footer_offset: 12_288,
            file_length: 16_384,
        },
    )
    .expect("valid sealed layout")
}

#[test]
fn sealed_header_has_stable_bytes_and_round_trips() {
    let header = example_header();

    let encoded = header.encode();

    assert_eq!(encoded.len(), HEADER_BYTES);
    assert_eq!(&encoded[0..8], b"FDCTNR01");
    assert_eq!(&encoded[8..10], &1_u16.to_le_bytes());
    assert_eq!(&encoded[10..12], &4096_u16.to_le_bytes());
    assert_eq!(&encoded[12..14], &2_u16.to_le_bytes());
    assert_eq!(&encoded[40..56], &[0x5a; 16]);
    assert_eq!(&encoded[56..64], &42_u64.to_le_bytes());
    assert_eq!(ContainerHeader::decode(&encoded), Ok(header));
}

#[test]
fn sealed_header_rejects_a_corrupted_reserved_byte() {
    let mut encoded = example_header().encode();
    encoded[108] = 1;

    assert_eq!(
        ContainerHeader::decode(&encoded),
        Err(FormatError::HeaderChecksumMismatch)
    );
}

#[test]
fn building_header_is_explicitly_not_a_sealed_container() {
    let encoded = BuildingContainerHeader::new(
        ContainerId::new([0x7b; 16]).expect("nonzero container id"),
        43,
    )
    .expect("nonzero generation")
    .encode();

    assert_eq!(&encoded[12..14], &1_u16.to_le_bytes());
    assert_eq!(
        ContainerHeader::decode(&encoded),
        Err(FormatError::ContainerNotSealed)
    );
}

#[test]
fn sealed_header_rejects_more_records_than_chunk_entries() {
    let mut encoded = example_header().encode();
    encoded[64..68].copy_from_slice(&3_u32.to_le_bytes());
    encoded[104..108].fill(0);
    let checksum = crc32c::crc32c(&encoded);
    encoded[104..108].copy_from_slice(&checksum.to_le_bytes());

    assert_eq!(
        ContainerHeader::decode(&encoded),
        Err(FormatError::InvalidContainerLayout)
    );
}
