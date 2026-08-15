use fastdup_format::{
    ChunkId, ContainerId, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES, ExactIndexEntry,
    ExactIndexLocation, ExactIndexProfileId, ExactIndexRun, ExactIndexRunDescriptor,
    ExactIndexRunRef, ExactIndexRunSet, ExactIndexRunSetId,
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
