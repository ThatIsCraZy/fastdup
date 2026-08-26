use fastdup_format::{
    ChunkId, ContainerId, ExactIndexEntry, FOOTER_BYTES, FormatError, HEADER_BYTES,
    MAX_LOGICAL_CHUNK_BYTES, SealedContainer, SealedContainerDescriptor, ZstdPrefixRecord,
};

#[test]
fn codec_three_record_round_trips_one_verified_depth_one_dependency() {
    let base = deterministic_bytes(64 * 1_024, 7);
    let mut target = base.clone();
    target.rotate_left(1);

    let encoded = ZstdPrefixRecord::encode(&base, &target).expect("Prefix encode succeeds");
    let dependency =
        ZstdPrefixRecord::dependency(&encoded).expect("Prefix dependency is structurally valid");
    let decoded = ZstdPrefixRecord::decode(&encoded, &base).expect("Prefix decode succeeds");

    assert_eq!(dependency.chunk_id(), ChunkId::of(&base));
    assert_eq!(
        dependency.logical_length(),
        u32::try_from(base.len()).expect("fixture length fits u32")
    );
    assert_eq!(decoded.chunk_id(), ChunkId::of(&target));
    assert_eq!(decoded.payload(), target);
    assert!(encoded.len().is_multiple_of(64));
}

#[test]
fn codec_three_record_is_deterministic_and_rejects_the_wrong_base() {
    let base = deterministic_bytes(32 * 1_024, 11);
    let mut target = base.clone();
    target[4_000..8_000].rotate_left(17);
    let first = ZstdPrefixRecord::encode(&base, &target).expect("first encode succeeds");
    let second = ZstdPrefixRecord::encode(&base, &target).expect("second encode succeeds");
    assert_eq!(first, second);

    let wrong_base = deterministic_bytes(base.len(), 12);
    assert_eq!(
        ZstdPrefixRecord::decode(&first, &wrong_base),
        Err(FormatError::ZstdPrefixBaseMismatch)
    );
}

#[test]
fn dependency_metadata_never_escapes_before_crc_and_shape_validation() {
    let base = deterministic_bytes(16 * 1_024, 21);
    let mut target = base.clone();
    target[99] ^= 0x55;
    let mut encoded = ZstdPrefixRecord::encode(&base, &target).expect("Prefix encode succeeds");
    encoded[64] ^= 1;

    assert_eq!(
        ZstdPrefixRecord::dependency(&encoded),
        Err(FormatError::RecordChecksumMismatch)
    );
}

#[test]
fn codec_three_rejects_empty_unequal_and_oversized_chunks() {
    assert!(ZstdPrefixRecord::encode(&[], &[]).is_err());
    assert_eq!(
        ZstdPrefixRecord::encode(&[1; 16], &[2; 17]),
        Err(FormatError::InvalidZstdPrefixRecord)
    );
    let oversized = vec![1; MAX_LOGICAL_CHUNK_BYTES + 1];
    assert!(ZstdPrefixRecord::encode(&oversized, &oversized).is_err());
}

#[test]
fn container_writer_pairs_dependency_through_recovery_and_exact_evidence() {
    let base = deterministic_bytes(64 * 1_024, 31);
    let mut target = base.clone();
    target.rotate_right(1);
    let encoded = SealedContainer::encode_zstd_prefix_pairs(
        ContainerId::new([0x73; 16]).expect("fixture Container ID is nonzero"),
        9,
        &[(base.as_slice(), target.as_slice())],
    )
    .expect("Prefix Container encode succeeds");
    let (image, publication) = encoded.into_publication_parts();
    assert_eq!(publication.zstd_prefix_record_count(), 1);
    assert_eq!(publication.locations().len(), 1);

    let candidate = ExactIndexEntry::from_verified(publication.locations()[0])
        .expect("dependent Location becomes an Exact candidate");
    assert_eq!(
        candidate.location().dependency_id(),
        ChunkId::of(&base).bytes()
    );

    let footer_bytes = usize::try_from(FOOTER_BYTES).expect("footer length fits memory");
    let descriptor = SealedContainerDescriptor::decode(
        &image[..HEADER_BYTES],
        &image[image.len() - footer_bytes..],
        u64::try_from(image.len()).expect("fixture image length fits u64"),
    )
    .expect("Container envelope verifies");
    let range = descriptor
        .record_range(candidate)
        .expect("Exact candidate pairs with the Container envelope");
    let start = usize::try_from(range.offset()).expect("record offset fits memory");
    let record = &image[start..start + range.length()];
    let decoded = descriptor
        .decode_zstd_prefix_candidate(candidate, record, &base)
        .expect("Exact Prefix candidate verifies with its Base");
    assert_eq!(decoded.payload(), target);
    let mut resolve = |dependency: fastdup_format::ZstdPrefixDependency| {
        assert_eq!(dependency.chunk_id(), ChunkId::of(&base));
        Ok(base.clone())
    };
    let fully_decoded = SealedContainer::decode_with_zstd_prefix_resolver(&image, &mut resolve)
        .expect("full Container decode resolves its verified Base");
    assert_eq!(fully_decoded.zstd_prefix_record_count(), 1);
    assert_eq!(
        fully_decoded
            .chunk(ChunkId::of(&target))
            .expect("target Chunk is present"),
        target
    );
    assert_eq!(
        SealedContainer::decode(&image),
        Err(FormatError::ZstdPrefixBaseRequired)
    );
}

fn deterministic_bytes(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}
