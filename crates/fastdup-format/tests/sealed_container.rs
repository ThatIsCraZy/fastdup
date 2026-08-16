use fastdup_format::{
    ContainerId, ExactIndexEntry, FormatError, SealedContainer, SealedContainerDescriptor,
};

#[test]
fn sealed_container_round_trips_records_through_its_recovery_index() {
    let encoded = SealedContainer::encode(
        ContainerId::new([0x33; 16]).expect("nonzero container id"),
        9,
        &[b"abc", b"xyz"],
    )
    .expect("valid container");

    assert_eq!(encoded.len(), 12_288);
    assert_eq!(&encoded[0..8], b"FDCTNR01");
    assert_eq!(&encoded[4_608..4_616], b"FDINDX01");
    assert_eq!(&encoded[8_192..8_200], b"FDFOOT01");

    let decoded = SealedContainer::decode(&encoded).expect("fully valid container");
    assert_eq!(decoded.header().container_generation(), 9);
    assert_eq!(decoded.chunk_count(), 2);
    assert_eq!(
        decoded
            .chunk(fastdup_format::ChunkId::of(b"abc"))
            .expect("indexed abc"),
        b"abc"
    );
    assert_eq!(
        decoded
            .chunk(fastdup_format::ChunkId::of(b"xyz"))
            .expect("indexed xyz"),
        b"xyz"
    );
}

#[test]
fn bounded_descriptor_pairs_the_envelope_and_fully_verifies_one_candidate_record() {
    let encoded = SealedContainer::encode(
        ContainerId::new([0x34; 16]).expect("nonzero container id"),
        19,
        &[b"first bounded record", b"requested bounded record"],
    )
    .expect("valid container");
    let complete = SealedContainer::decode(&encoded).expect("worked Container is fully valid");
    let candidate = ExactIndexEntry::from_verified_raw(complete.raw_locations()[1])
        .expect("build the candidate from full rebuild evidence");
    let footer_offset = encoded.len() - 4_096;

    let descriptor = SealedContainerDescriptor::decode(
        &encoded[..4_096],
        &encoded[footer_offset..],
        u64::try_from(encoded.len()).expect("worked Container length fits u64"),
    )
    .expect("bounded reader pairs Header, Footer, and physical length");
    let range = descriptor
        .raw_record_range(candidate)
        .expect("candidate lies in the sealed record region");
    let start = usize::try_from(range.offset()).expect("worked record offset fits usize");
    let end = start + range.length();
    let record = descriptor
        .decode_raw_candidate(candidate, &encoded[start..end])
        .expect("bounded reader checks Record CRC and complete Chunk ID");

    assert_eq!(record.payload(), b"requested bounded record");
}

#[test]
fn sealed_container_rejects_recovery_index_corruption() {
    let mut encoded = SealedContainer::encode(
        ContainerId::new([0x44; 16]).expect("nonzero container id"),
        10,
        &[b"abc", b"xyz"],
    )
    .expect("valid container");
    encoded[4_608 + 64 + 7] ^= 1;

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::IndexChecksumMismatch)
    );
}

#[test]
fn sealed_container_rejects_a_valid_header_crc_with_a_footer_identity_mismatch() {
    let mut encoded = SealedContainer::encode(
        ContainerId::new([0x45; 16]).expect("nonzero container id"),
        10,
        &[b"header identity"],
    )
    .expect("valid container");
    encoded[56..64].copy_from_slice(&11_u64.to_le_bytes());
    rewrite_crc32c(&mut encoded[..4_096], 104);

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::HeaderFooterMismatch)
    );
}

#[test]
fn sealed_container_rejects_a_valid_footer_crc_with_a_header_identity_mismatch() {
    let mut encoded = SealedContainer::encode(
        ContainerId::new([0x46; 16]).expect("nonzero container id"),
        12,
        &[b"footer identity"],
    )
    .expect("valid container");
    let footer_offset = encoded.len() - 4_096;
    encoded[footer_offset + 48..footer_offset + 56].copy_from_slice(&13_u64.to_le_bytes());
    rewrite_crc32c(&mut encoded[footer_offset..], 128);

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::HeaderFooterMismatch)
    );
}

#[test]
fn sealed_container_rejects_an_authenticated_nonzero_header_reserved_byte() {
    let mut encoded = SealedContainer::encode(
        ContainerId::new([0x48; 16]).expect("nonzero container id"),
        17,
        &[b"header reserved"],
    )
    .expect("valid container");
    encoded[108] = 1;
    rewrite_crc32c(&mut encoded[..4_096], 104);

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::NonZeroHeaderReserved)
    );
}

#[test]
fn sealed_container_rejects_an_authenticated_nonzero_footer_reserved_byte() {
    let mut encoded = SealedContainer::encode(
        ContainerId::new([0x49; 16]).expect("nonzero container id"),
        18,
        &[b"footer reserved"],
    )
    .expect("valid container");
    let footer_offset = encoded.len() - 4_096;
    encoded[footer_offset + 132] = 1;
    rewrite_crc32c(&mut encoded[footer_offset..], 128);

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::InvalidFooter)
    );
}

#[test]
fn every_truncated_container_prefix_is_rejected_without_panicking() {
    let encoded = SealedContainer::encode(
        ContainerId::new([0x47; 16]).expect("nonzero container id"),
        14,
        &[b"first record", b"second record"],
    )
    .expect("valid container");

    for prefix_length in 0..encoded.len() {
        let outcome =
            std::panic::catch_unwind(|| SealedContainer::decode(&encoded[..prefix_length]));
        assert!(
            outcome.is_ok(),
            "decoder panicked for a {prefix_length}-byte prefix"
        );
        assert!(
            outcome.expect("panic outcome checked above").is_err(),
            "decoder accepted a truncated {prefix_length}-byte prefix"
        );
    }
}

#[test]
fn every_single_byte_corruption_is_rejected() {
    let encoded = SealedContainer::encode(
        ContainerId::new([0x55; 16]).expect("nonzero container id"),
        11,
        &[b"integrity audit"],
    )
    .expect("valid container");

    for offset in 0..encoded.len() {
        let mut corrupted = encoded.clone();
        corrupted[offset] ^= 1;
        assert!(
            SealedContainer::decode(&corrupted).is_err(),
            "single-byte corruption at offset {offset} was accepted"
        );
    }
}

#[test]
fn oversized_container_is_rejected_by_preflight() {
    let chunk = vec![0xa5; 256 * 1_024];
    let chunks = vec![chunk.as_slice(); 256];

    assert_eq!(
        SealedContainer::encode(
            ContainerId::new([0x56; 16]).expect("nonzero container id"),
            12,
            &chunks,
        ),
        Err(FormatError::InvalidContainerLayout)
    );
}

#[test]
fn checksummed_index_redirection_fails_the_record_bijection() {
    let mut encoded = SealedContainer::encode(
        ContainerId::new([0x57; 16]).expect("nonzero container id"),
        13,
        &[b"abc", b"xyz"],
    )
    .expect("valid container");
    let index_offset = usize::try_from(read_u64(&encoded, 72)).expect("index offset fits usize");
    let index_length = usize::try_from(read_u64(&encoded, 80)).expect("index length fits usize");
    let first_record_offset = index_offset + 64 + 40;
    let redirected: u64 = if read_u64(&encoded, first_record_offset) == 4_096 {
        4_352
    } else {
        4_096
    };
    encoded[first_record_offset..first_record_offset + 8]
        .copy_from_slice(&redirected.to_le_bytes());
    reauthenticate_index_and_container(&mut encoded, index_offset, index_length);

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::IndexRecordMismatch)
    );
}

#[test]
fn authenticated_duplicate_recovery_index_entry_is_rejected() {
    let mut encoded = SealedContainer::encode(
        ContainerId::new([0x58; 16]).expect("nonzero container id"),
        15,
        &[b"duplicate source", b"duplicate target"],
    )
    .expect("valid container");
    let index_offset = usize::try_from(read_u64(&encoded, 72)).expect("index offset fits usize");
    let index_length = usize::try_from(read_u64(&encoded, 80)).expect("index length fits usize");
    let first_entry = index_offset + 64;
    let second_entry = first_entry + 128;
    let duplicate = encoded[first_entry..first_entry + 128].to_vec();
    encoded[second_entry..second_entry + 128].copy_from_slice(&duplicate);
    reauthenticate_index_and_container(&mut encoded, index_offset, index_length);

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::InvalidRecoveryIndex)
    );
}

#[test]
fn authenticated_reordered_recovery_index_entries_are_rejected() {
    let mut encoded = SealedContainer::encode(
        ContainerId::new([0x59; 16]).expect("nonzero container id"),
        16,
        &[b"reorder first", b"reorder second"],
    )
    .expect("valid container");
    let index_offset = usize::try_from(read_u64(&encoded, 72)).expect("index offset fits usize");
    let index_length = usize::try_from(read_u64(&encoded, 80)).expect("index length fits usize");
    let first_entry = index_offset + 64;
    let second_entry = first_entry + 128;
    for byte_offset in 0..128 {
        encoded.swap(first_entry + byte_offset, second_entry + byte_offset);
    }
    reauthenticate_index_and_container(&mut encoded, index_offset, index_length);

    assert_eq!(
        SealedContainer::decode(&encoded),
        Err(FormatError::InvalidRecoveryIndex)
    );
}

fn reauthenticate_index_and_container(
    encoded: &mut [u8],
    index_offset: usize,
    index_length: usize,
) {
    encoded[index_offset + 36..index_offset + 40].fill(0);
    let index_checksum = crc32c::crc32c(&encoded[index_offset..index_offset + index_length]);
    encoded[index_offset + 36..index_offset + 40].copy_from_slice(&index_checksum.to_le_bytes());

    let footer_offset = encoded.len() - 4_096;
    encoded[footer_offset + 96..footer_offset + 132].fill(0);
    let container_hash = blake3::hash(encoded);
    encoded[footer_offset + 96..footer_offset + 128].copy_from_slice(container_hash.as_bytes());
    let footer_checksum = crc32c::crc32c(&encoded[footer_offset..]);
    encoded[footer_offset + 128..footer_offset + 132]
        .copy_from_slice(&footer_checksum.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("test offset selects exactly eight bytes"),
    )
}

fn rewrite_crc32c(bytes: &mut [u8], checksum_offset: usize) {
    bytes[checksum_offset..checksum_offset + 4].fill(0);
    let checksum = crc32c::crc32c(bytes);
    bytes[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());
}
