use fastdup_format::{
    ChunkId, ContainerId, EXACT_INDEX_ENTRY_BYTES, EXACT_INDEX_HEADER_BYTES,
    EXACT_INDEX_PAGE_BYTES, ExactIndexEntry, ExactIndexFormatError, ExactIndexLocation,
    ExactIndexPagePosition, ExactIndexProfileId, ExactIndexRun, ExactIndexRunDescriptor,
    MAX_CONTAINER_BYTES, SealedContainer,
};

fn entry(ordinal: u8, logical_length: u32) -> ExactIndexEntry {
    let stored_record_length = logical_length
        .checked_add(255)
        .expect("worked length does not overflow")
        / 64
        * 64;
    let location = ExactIndexLocation::raw(
        ContainerId::new([ordinal + 1; 16]).expect("container identity is nonzero"),
        u64::from(ordinal) + 1,
        4_096 + u64::from(ordinal) * 64,
        stored_record_length,
        0x1020_3000 + u32::from(ordinal),
    )
    .expect("worked RAW location is valid");
    ExactIndexEntry::active(ChunkId::from_bytes([ordinal; 32]), logical_length, location)
        .expect("worked active entry is valid")
}

fn worked_run() -> ExactIndexRun {
    let profile = ExactIndexProfileId::new([0xA5; 32]).expect("profile identity is nonzero");
    let mut entries = (0_u8..32)
        .rev()
        .map(|ordinal| entry(ordinal, 16_384 + u32::from(ordinal)))
        .collect::<Vec<_>>();
    entries.swap(3, 17);
    ExactIndexRun::new(profile, 7, entries).expect("worked run is canonicalizable")
}

#[test]
fn exact_index_run_has_stable_pages_and_canonical_round_trip() {
    let run = worked_run();

    let encoded = run.encode().expect("bounded run must encode");

    assert_eq!(encoded.len(), 4 * EXACT_INDEX_PAGE_BYTES);
    assert_eq!(&encoded[0..8], b"FDXIRN01");
    assert_eq!(&encoded[EXACT_INDEX_HEADER_BYTES..][0..8], b"FDXPG001");
    assert_eq!(
        &encoded[EXACT_INDEX_HEADER_BYTES + 12..EXACT_INDEX_HEADER_BYTES + 14],
        &u16::try_from(EXACT_INDEX_ENTRY_BYTES)
            .expect("entry size fits u16")
            .to_le_bytes()
    );
    assert_eq!(
        &encoded[EXACT_INDEX_HEADER_BYTES + 14..EXACT_INDEX_HEADER_BYTES + 16],
        &31_u16.to_le_bytes()
    );
    assert_eq!(
        &encoded[EXACT_INDEX_HEADER_BYTES + EXACT_INDEX_PAGE_BYTES + 14
            ..EXACT_INDEX_HEADER_BYTES + EXACT_INDEX_PAGE_BYTES + 16],
        &1_u16.to_le_bytes()
    );
    assert_eq!(run.entries()[0].chunk_id(), ChunkId::from_bytes([0; 32]));
    assert_eq!(run.entries()[31].chunk_id(), ChunkId::from_bytes([31; 32]));
    assert_eq!(ExactIndexRun::decode(&encoded), Ok(run));
}

#[test]
fn every_truncated_or_single_byte_corrupt_run_is_rejected_without_panicking() {
    let encoded = worked_run().encode().expect("worked run must encode");

    for prefix_length in 0..encoded.len() {
        let result = std::panic::catch_unwind(|| ExactIndexRun::decode(&encoded[..prefix_length]));
        assert!(result.is_ok(), "decoder panicked at prefix {prefix_length}");
        assert!(
            result.expect("panic checked").is_err(),
            "decoder accepted truncated prefix {prefix_length}"
        );
    }

    for offset in 0..encoded.len() {
        let mut corrupted = encoded.clone();
        corrupted[offset] ^= 1;
        assert!(
            ExactIndexRun::decode(&corrupted).is_err(),
            "decoder accepted corruption at byte {offset}"
        );
    }
}

#[test]
fn writer_rejects_duplicate_locations_and_chunk_length_conflicts() {
    let profile = ExactIndexProfileId::new([0x5A; 32]).expect("profile identity is nonzero");
    let duplicate = entry(9, 32_768);
    assert_eq!(
        ExactIndexRun::new(profile, 1, vec![duplicate, duplicate]),
        Err(ExactIndexFormatError::NonCanonicalOrder)
    );

    let original = entry(10, 32_768);
    let conflicting_length = entry(10, 32_769);
    assert_eq!(
        ExactIndexRun::new(profile, 1, vec![conflicting_length, original]),
        Err(ExactIndexFormatError::ChunkLengthConflict)
    );
}

#[test]
fn raw_location_writer_rejects_impossible_container_coordinates() {
    let container = ContainerId::new([0xC3; 16]).expect("container identity is nonzero");
    let undersized = ExactIndexLocation::raw(container, 1, 4_096, 256, 7)
        .expect("coordinates are structurally aligned until logical length is known");
    assert_eq!(
        ExactIndexEntry::active(ChunkId::from_bytes([7; 32]), 16_384, undersized),
        Err(ExactIndexFormatError::InvalidEntry)
    );

    assert_eq!(
        ExactIndexLocation::raw(container, 1, MAX_CONTAINER_BYTES - 64, 128, 7),
        Err(ExactIndexFormatError::InvalidEntry)
    );
}

#[test]
fn descriptor_and_pages_support_bounded_candidate_lookup() {
    let encoded = worked_run().encode().expect("worked run must encode");
    let footer_offset = encoded.len() - EXACT_INDEX_PAGE_BYTES;
    let descriptor = ExactIndexRunDescriptor::decode(
        &encoded[..EXACT_INDEX_HEADER_BYTES],
        &encoded[footer_offset..],
        u64::try_from(encoded.len()).expect("worked run length fits u64"),
    )
    .expect("header and footer form one valid run envelope");

    assert_eq!(descriptor.entry_count(), 32);
    assert_eq!(descriptor.page_count(), 2);
    assert_eq!(descriptor.page_offset(0), Some(4_096));
    assert_eq!(descriptor.page_offset(1), Some(8_192));
    assert_eq!(descriptor.page_offset(2), None);

    let first_page = descriptor
        .decode_page(0, &encoded[4_096..8_192])
        .expect("first page is independently valid");
    assert_eq!(
        first_page.position(ChunkId::from_bytes([31; 32]), 16_415),
        ExactIndexPagePosition::After
    );
    let candidates = first_page.candidates(ChunkId::from_bytes([7; 32]), 16_391);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].chunk_id(), ChunkId::from_bytes([7; 32]));

    let second_page = descriptor
        .decode_page(1, &encoded[8_192..12_288])
        .expect("last page is independently valid");
    assert_eq!(
        second_page.position(ChunkId::from_bytes([31; 32]), 16_415),
        ExactIndexPagePosition::Within
    );
}

#[test]
fn run_entries_are_derived_from_verified_container_recovery_evidence() {
    let container_id = ContainerId::new([0xD4; 16]).expect("container identity is nonzero");
    let chunks = [
        b"first durable chunk".as_slice(),
        b"second durable chunk".as_slice(),
    ];
    let encoded =
        SealedContainer::encode(container_id, 19, &chunks).expect("worked container must encode");
    let container = SealedContainer::decode(&encoded).expect("container evidence must verify");
    let entries = container
        .raw_locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified_raw)
        .collect::<Result<Vec<_>, _>>()
        .expect("verified RAW locations must remain format-valid");
    let profile = ExactIndexProfileId::new([0x44; 32]).expect("profile identity is nonzero");
    let run = ExactIndexRun::new(profile, 20, entries).expect("evidence must build a run");

    assert_eq!(run.entries().len(), 2);
    assert_eq!(
        ExactIndexRun::decode(&run.encode().expect("run encodes")),
        Ok(run)
    );
}

#[test]
fn bounded_reader_rejects_a_valid_page_from_a_different_run() {
    let encoded = worked_run().encode().expect("worked run must encode");
    let footer_offset = encoded.len() - EXACT_INDEX_PAGE_BYTES;
    let descriptor = ExactIndexRunDescriptor::decode(
        &encoded[..EXACT_INDEX_HEADER_BYTES],
        &encoded[footer_offset..],
        u64::try_from(encoded.len()).expect("worked run length fits u64"),
    )
    .expect("worked envelope must verify");

    let profile = ExactIndexProfileId::new([0xA5; 32]).expect("profile identity is nonzero");
    let foreign_entries = (64_u8..96)
        .map(|ordinal| entry(ordinal, 16_384 + u32::from(ordinal)))
        .collect();
    let foreign = ExactIndexRun::new(profile, 8, foreign_entries)
        .expect("foreign run is independently valid")
        .encode()
        .expect("foreign run encodes");

    assert_eq!(
        descriptor.decode_page(0, &foreign[4_096..8_192]),
        Err(ExactIndexFormatError::InvalidPage)
    );
}
