use fastdup_format::{
    ChunkId, ContainerId, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES, ExactIndexEntry,
    ExactIndexLocation, ExactIndexProfileId, ExactIndexRun, ExactIndexRunDescriptor,
    ExactIndexRunRef, ExactIndexRunSet, ExactIndexRunSetError, ExactIndexRunSetId,
    METADATA_HEADER_BYTES,
};

fn descriptor(
    profile: ExactIndexProfileId,
    generation: u64,
    ordinal: u8,
) -> ExactIndexRunDescriptor {
    let logical_length = 16_384 + u32::from(ordinal);
    let record_length = (logical_length + 255) / 64 * 64;
    let location = ExactIndexLocation::raw(
        ContainerId::new([ordinal + 1; 16]).expect("container identity is nonzero"),
        generation,
        4_096,
        record_length,
        0xEE00_0000 + u32::from(ordinal),
    )
    .expect("worked RAW location is valid");
    let entry =
        ExactIndexEntry::active(ChunkId::from_bytes([ordinal; 32]), logical_length, location)
            .expect("worked entry is valid");
    let encoded = ExactIndexRun::new(profile, generation, vec![entry])
        .expect("worked run is valid")
        .encode()
        .expect("worked run encodes");
    let footer_offset = encoded.len() - EXACT_INDEX_PAGE_BYTES;
    ExactIndexRunDescriptor::decode(
        &encoded[..EXACT_INDEX_HEADER_BYTES],
        &encoded[footer_offset..],
        u64::try_from(encoded.len()).expect("worked run length fits u64"),
    )
    .expect("worked descriptor verifies")
}

#[test]
fn run_set_is_content_identified_canonical_and_byte_exact() {
    let profile = ExactIndexProfileId::new([0xB2; 32]).expect("profile identity is nonzero");
    let newer = ExactIndexRunRef::new(0, descriptor(profile, 9, 9))
        .expect("level-zero run reference is valid");
    let older = ExactIndexRunRef::new(1, descriptor(profile, 4, 4))
        .expect("level-one run reference is valid");
    let run_set = ExactIndexRunSet::new(profile, 3, vec![older, newer])
        .expect("worked run set canonicalizes");

    let encoded = run_set.encode().expect("worked run set encodes");
    let decoded = ExactIndexRunSet::decode(&encoded).expect("worked run set decodes");
    let identity = ExactIndexRunSetId::from_encoded(&encoded)
        .expect("generic envelope and Run Set payload establish one identity");

    assert_eq!(&encoded[0..8], b"FDMDOBJ1");
    assert_eq!(&encoded[12..14], &3_u16.to_le_bytes());
    assert_eq!(&encoded[4_096..4_104], b"FDXRST01");
    assert_eq!(run_set.runs()[0].level(), 0);
    assert_eq!(run_set.runs()[0].generation(), 9);
    assert_eq!(run_set.runs()[1].level(), 1);
    assert_eq!(decoded, run_set);
    assert_eq!(decoded.id().expect("decoded Run Set re-encodes"), identity);
}

#[test]
fn every_truncated_or_single_byte_corrupt_run_set_is_rejected_without_panicking() {
    let profile = ExactIndexProfileId::new([0xC2; 32]).expect("profile identity is nonzero");
    let run_set = ExactIndexRunSet::new(
        profile,
        1,
        vec![
            ExactIndexRunRef::new(0, descriptor(profile, 1, 1))
                .expect("worked run reference is valid"),
            ExactIndexRunRef::new(0, descriptor(profile, 2, 2))
                .expect("worked run reference is valid"),
        ],
    )
    .expect("worked run set is valid");
    let encoded = run_set.encode().expect("worked run set encodes");

    for prefix_length in 0..encoded.len() {
        let result =
            std::panic::catch_unwind(|| ExactIndexRunSet::decode(&encoded[..prefix_length]));
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
            ExactIndexRunSet::decode(&corrupted).is_err(),
            "decoder accepted corruption at byte {offset}"
        );
    }
}

#[test]
fn partitioned_run_family_has_stable_v2_bytes_and_round_trips_as_one_generation() {
    let profile = ExactIndexProfileId::new([0xD2; 32]).expect("profile identity is nonzero");
    let first = ExactIndexRunRef::family_partition(2, 100, 0, 2, descriptor(profile, 100, 10))
        .expect("first family partition is valid");
    let second = ExactIndexRunRef::family_partition(2, 100, 1, 2, descriptor(profile, 101, 20))
        .expect("second family partition is valid");
    let run_set = ExactIndexRunSet::new(profile, 7, vec![second, first])
        .expect("complete partitioned family canonicalizes");

    let encoded = run_set.encode().expect("v2 Run Set encodes");
    assert_eq!(&encoded[4_096 + 8..4_096 + 10], &2_u16.to_le_bytes());
    assert_eq!(&encoded[4_096 + 12..4_096 + 14], &160_u16.to_le_bytes());
    let decoded = ExactIndexRunSet::decode(&encoded).expect("v2 Run Set decodes");
    assert_eq!(decoded, run_set);
    assert_eq!(decoded.family_count(), 1);
    assert_eq!(decoded.runs()[0].family_generation(), 100);
    assert_eq!(decoded.runs()[0].partition_ordinal(), 0);
    assert_eq!(decoded.runs()[0].partition_count(), 2);
    assert_eq!(decoded.runs()[1].generation(), 101);
    assert_eq!(
        decoded.id().expect("decoded v2 re-encodes"),
        run_set.id().expect("v2 identity")
    );
}

#[test]
fn writer_rejects_incomplete_and_overlapping_run_families() {
    let profile = ExactIndexProfileId::new([0xD3; 32]).expect("profile identity is nonzero");
    let incomplete = ExactIndexRunRef::family_partition(2, 100, 0, 2, descriptor(profile, 100, 10))
        .expect("one partition reference is locally valid");
    assert!(matches!(
        ExactIndexRunSet::new(profile, 1, vec![incomplete]),
        Err(ExactIndexRunSetError::InvalidRunFamily)
    ));

    let first = ExactIndexRunRef::family_partition(2, 200, 0, 2, descriptor(profile, 200, 20))
        .expect("first partition reference is locally valid");
    let second = ExactIndexRunRef::family_partition(2, 200, 1, 2, descriptor(profile, 201, 10))
        .expect("second partition reference is locally valid");
    assert!(matches!(
        ExactIndexRunSet::new(profile, 1, vec![first, second]),
        Err(ExactIndexRunSetError::OverlappingRunFamily)
    ));
}

#[test]
fn reader_rejects_reauthenticated_missing_partition_without_panicking() {
    let profile = ExactIndexProfileId::new([0xD4; 32]).expect("profile identity is nonzero");
    let first = ExactIndexRunRef::family_partition(2, 300, 0, 2, descriptor(profile, 300, 10))
        .expect("first family partition is valid");
    let second = ExactIndexRunRef::family_partition(2, 300, 1, 2, descriptor(profile, 301, 20))
        .expect("second family partition is valid");
    let run_set =
        ExactIndexRunSet::new(profile, 1, vec![first, second]).expect("complete family is valid");
    let mut encoded = run_set.encode().expect("v2 fixture encodes");
    let second_entry = METADATA_HEADER_BYTES + 128 + 160;
    encoded[second_entry + 2..second_entry + 4].copy_from_slice(&0_u16.to_le_bytes());
    reauthenticate_metadata_object(&mut encoded);

    let result = std::panic::catch_unwind(|| ExactIndexRunSet::decode(&encoded));
    assert!(
        result.is_ok(),
        "reader must not panic on authenticated corruption"
    );
    assert!(result.expect("panic checked").is_err());
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
