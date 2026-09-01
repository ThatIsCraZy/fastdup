use fastdup_format::{
    ContainerId, FormatError, HEADER_BYTES, SealedContainer, SealedContainerDescriptor,
};

const HEADER_SUMMARY_OFFSET: usize = 128;
const FOOTER_SUMMARY_OFFSET: usize = 192;

#[test]
fn envelope_exposes_exact_raw_geometry_without_reading_the_record_region() {
    let image = SealedContainer::encode(
        ContainerId::new([0x91; 16]).expect("fixture ID is nonzero"),
        7,
        &[b"abc", b"xyz"],
    )
    .expect("RAW Container encodes");
    let summary = summary(&image).expect("Header/Footer envelope verifies");

    assert_eq!(summary.raw_record_count(), 2);
    assert_eq!(summary.zstd_record_count(), 0);
    assert_eq!(summary.zstd_prefix_record_count(), 0);
    assert_eq!(summary.sparse_xor_record_count(), 0);
    assert_eq!(summary.independent_chunk_count(), 2);
    assert_eq!(summary.dependent_chunk_count(), 0);
    assert_eq!(summary.raw_encoded_bytes(), 512);
    assert_eq!(summary.raw_decoded_bytes(), 6);
    assert_eq!(summary.single_chunk_record_count(), 2);
    assert_eq!(summary.multi_chunk_record_count(), 0);
    assert_eq!(summary.outgoing_dependency_edges(), 0);
    assert_eq!(summary.unique_outgoing_base_ids(), 0);
}

#[test]
fn envelope_accounts_sparse_xor_as_a_dependent_codec() {
    let base = vec![b'A'; 64 * 1_024];
    let mut first = base.clone();
    first[17] = b'B';
    let mut second = base.clone();
    second[31] = b'C';
    let image = SealedContainer::encode_sparse_xor_pairs(
        ContainerId::new([0x94; 16]).expect("fixture ID is nonzero"),
        10,
        &[
            (base.as_slice(), first.as_slice()),
            (base.as_slice(), second.as_slice()),
        ],
    )
    .expect("Sparse-XOR Container encodes")
    .into_bytes();
    let summary = summary(&image).expect("Header/Footer envelope verifies");

    assert_eq!(summary.raw_record_count(), 0);
    assert_eq!(summary.zstd_record_count(), 0);
    assert_eq!(summary.zstd_prefix_record_count(), 0);
    assert_eq!(summary.sparse_xor_record_count(), 2);
    assert_eq!(summary.independent_chunk_count(), 0);
    assert_eq!(summary.dependent_chunk_count(), 2);
    assert_eq!(summary.sparse_xor_decoded_bytes(), 128 * 1_024);
    assert!(summary.sparse_xor_encoded_bytes() > 0);
    assert_eq!(summary.single_chunk_record_count(), 2);
    assert_eq!(summary.outgoing_dependency_edges(), 2);
    assert_eq!(summary.unique_outgoing_base_ids(), 1);
}

#[test]
fn envelope_counts_unique_prefix_bases_separately_from_dependency_edges() {
    let base = vec![b'A'; 64 * 1_024];
    let mut first = base.clone();
    first[17] = b'B';
    let mut second = base.clone();
    second[31] = b'C';
    let image = SealedContainer::encode_zstd_prefix_pairs(
        ContainerId::new([0x92; 16]).expect("fixture ID is nonzero"),
        8,
        &[
            (base.as_slice(), first.as_slice()),
            (base.as_slice(), second.as_slice()),
        ],
    )
    .expect("Prefix Container encodes")
    .into_bytes();
    let summary = summary(&image).expect("Header/Footer envelope verifies");

    assert_eq!(summary.raw_record_count(), 0);
    assert_eq!(summary.zstd_record_count(), 0);
    assert_eq!(summary.zstd_prefix_record_count(), 2);
    assert_eq!(summary.independent_chunk_count(), 0);
    assert_eq!(summary.dependent_chunk_count(), 2);
    assert_eq!(summary.zstd_prefix_decoded_bytes(), 128 * 1_024);
    assert_eq!(summary.single_chunk_record_count(), 2);
    assert_eq!(summary.outgoing_dependency_edges(), 2);
    assert_eq!(summary.unique_outgoing_base_ids(), 1);
}

#[test]
fn complete_verifier_rejects_a_consistent_envelope_summary_that_disagrees_with_records() {
    let mut image = SealedContainer::encode(
        ContainerId::new([0x93; 16]).expect("fixture ID is nonzero"),
        9,
        &[b"abc", b"xyz"],
    )
    .expect("RAW Container encodes");
    let footer_offset = image.len() - 4_096;

    rewrite_raw_pair_as_raw_plus_zstd(&mut image[..HEADER_BYTES], HEADER_SUMMARY_OFFSET);
    rewrite_crc32c(&mut image[..HEADER_BYTES], 104);
    rewrite_raw_pair_as_raw_plus_zstd(&mut image[footer_offset..], FOOTER_SUMMARY_OFFSET);
    rewrite_crc32c(&mut image[footer_offset..], 128);

    descriptor(&image).expect("paired envelope summary remains structurally valid");
    assert_eq!(
        SealedContainer::decode(&image),
        Err(FormatError::ContainerSummaryMismatch)
    );
}

fn descriptor(image: &[u8]) -> Result<SealedContainerDescriptor, FormatError> {
    SealedContainerDescriptor::decode(
        &image[..HEADER_BYTES],
        &image[image.len() - 4_096..],
        u64::try_from(image.len()).expect("fixture length fits u64"),
    )
}

fn summary(image: &[u8]) -> Result<fastdup_format::ContainerIntrinsicSummary, FormatError> {
    SealedContainerDescriptor::decode_intrinsic_summary(
        &image[..HEADER_BYTES],
        &image[image.len() - 4_096..],
        u64::try_from(image.len()).expect("fixture length fits u64"),
    )
}

fn rewrite_raw_pair_as_raw_plus_zstd(block: &mut [u8], summary_offset: usize) {
    block[summary_offset + 4..summary_offset + 8].copy_from_slice(&1_u32.to_le_bytes());
    block[summary_offset + 8..summary_offset + 12].copy_from_slice(&1_u32.to_le_bytes());
    block[summary_offset + 24..summary_offset + 32].copy_from_slice(&256_u64.to_le_bytes());
    block[summary_offset + 32..summary_offset + 40].copy_from_slice(&256_u64.to_le_bytes());
    block[summary_offset + 48..summary_offset + 56].copy_from_slice(&3_u64.to_le_bytes());
    block[summary_offset + 56..summary_offset + 64].copy_from_slice(&3_u64.to_le_bytes());
}

fn rewrite_crc32c(bytes: &mut [u8], checksum_offset: usize) {
    bytes[checksum_offset..checksum_offset + 4].fill(0);
    let checksum = crc32c::crc32c(bytes);
    bytes[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());
}
