use fastdup_format::{
    ChunkId, MANIFEST_HEADER_BYTES, METADATA_HEADER_BYTES, ManifestExtent, ManifestLeaf,
    MetadataFormatError, MetadataObjectId,
};

#[test]
fn manifest_leaf_has_stable_bytes_and_round_trips_a_complete_file_partition() {
    let leaf = ManifestLeaf::new(
        15,
        vec![
            ManifestExtent::Data {
                logical_length: 4,
                chunk_id: ChunkId::of(b"abcd"),
            },
            ManifestExtent::Hole { logical_length: 8 },
            ManifestExtent::Fill {
                logical_length: 3,
                value: 0,
            },
        ],
    )
    .expect("worked manifest partition must be valid");

    let encoded = leaf.encode().expect("bounded manifest must encode");

    assert_eq!(&encoded[0..8], b"FDMDOBJ1");
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES..METADATA_HEADER_BYTES + 8],
        b"FDMANL01"
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 8..METADATA_HEADER_BYTES + 10],
        &2_u16.to_le_bytes()
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 10..METADATA_HEADER_BYTES + 12],
        &u16::try_from(MANIFEST_HEADER_BYTES)
            .expect("header size fits")
            .to_le_bytes()
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 24..METADATA_HEADER_BYTES + 32],
        &15_u64.to_le_bytes()
    );
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 32..METADATA_HEADER_BYTES + 36],
        &3_u32.to_le_bytes()
    );
    assert_eq!(
        MetadataObjectId::from_encoded(&encoded)
            .expect("envelope identity must verify")
            .bytes()
            .len(),
        32
    );
    assert_eq!(ManifestLeaf::decode(&encoded), Ok(leaf));
}

#[test]
fn every_truncated_or_single_byte_corrupt_manifest_is_rejected_without_panicking() {
    let leaf = ManifestLeaf::new(
        12,
        vec![
            ManifestExtent::Data {
                logical_length: 4,
                chunk_id: ChunkId::of(b"data"),
            },
            ManifestExtent::Hole { logical_length: 4 },
            ManifestExtent::Fill {
                logical_length: 4,
                value: 0x7f,
            },
        ],
    )
    .expect("fixture must be valid");
    let encoded = leaf.encode().expect("fixture must encode");

    for prefix_length in 0..encoded.len() {
        let result = std::panic::catch_unwind(|| ManifestLeaf::decode(&encoded[..prefix_length]));
        assert!(result.is_ok(), "prefix {prefix_length} panicked");
        assert!(
            result.expect("checked above").is_err(),
            "prefix {prefix_length} was accepted"
        );
    }
    for offset in 0..encoded.len() {
        let mut corrupted = encoded.clone();
        corrupted[offset] ^= 1;
        assert!(
            ManifestLeaf::decode(&corrupted).is_err(),
            "single-byte corruption at {offset} was accepted"
        );
    }
}

#[test]
fn writer_rejects_incomplete_zero_length_and_oversized_data_partitions() {
    assert_eq!(
        ManifestLeaf::new(1, Vec::new()),
        Err(MetadataFormatError::InvalidPartition)
    );
    assert_eq!(
        ManifestLeaf::new(1, vec![ManifestExtent::Hole { logical_length: 0 }]),
        Err(MetadataFormatError::InvalidExtent)
    );
    assert_eq!(
        ManifestLeaf::new(
            256 * 1_024 + 1,
            vec![ManifestExtent::Data {
                logical_length: 256 * 1_024 + 1,
                chunk_id: ChunkId::of(b"oversized"),
            }],
        ),
        Err(MetadataFormatError::InvalidExtent)
    );
}

#[test]
fn manifest_v2_chunk_slice_round_trips_and_rejects_out_of_bounds_ranges() {
    let chunk_id = ChunkId::of(&vec![0x5a; 64 * 1_024]);
    let leaf = ManifestLeaf::new(
        4 * 1_024,
        vec![ManifestExtent::DataSlice {
            logical_length: 4 * 1_024,
            chunk_id,
            chunk_length: 64 * 1_024,
            chunk_offset: 12 * 1_024,
        }],
    )
    .expect("a bounded slice of one verified Chunk is valid");
    let encoded = leaf.encode().expect("v2 slice manifest encodes");
    assert_eq!(
        &encoded[METADATA_HEADER_BYTES + 8..METADATA_HEADER_BYTES + 10],
        &2_u16.to_le_bytes()
    );
    assert_eq!(ManifestLeaf::decode(&encoded), Ok(leaf));

    assert_eq!(
        ManifestLeaf::new(
            8,
            vec![ManifestExtent::DataSlice {
                logical_length: 8,
                chunk_id,
                chunk_length: 16,
                chunk_offset: 12,
            }],
        ),
        Err(MetadataFormatError::InvalidExtent)
    );
}
