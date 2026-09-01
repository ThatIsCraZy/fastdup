use fastdup_format::{
    ChunkId, ContainerId, DependentDependency, ExactIndexEntry, FOOTER_BYTES, FormatError,
    HEADER_BYTES, MAX_LOGICAL_CHUNK_BYTES, SealedContainer, SealedContainerDescriptor,
    SparseXorRecord, SparseXorRun,
};

#[test]
fn codec_four_record_round_trips_canonical_sparse_changes() {
    let base = deterministic_bytes(64 * 1_024, 7);
    let mut target = base.clone();
    target[31] ^= 0x55;
    target[4_096..4_103]
        .iter_mut()
        .for_each(|byte| *byte ^= 0xa3);
    target[63 * 1_024] ^= 1;

    let first = SparseXorRecord::encode(&base, &target).expect("Sparse-XOR encode succeeds");
    let second = SparseXorRecord::encode(&base, &target).expect("encoding is repeatable");
    let dependency = SparseXorRecord::dependency(&first).expect("dependency is structurally valid");
    let decoded = SparseXorRecord::decode(&first, &base).expect("Sparse-XOR decode succeeds");

    assert_eq!(first, second);
    assert_eq!(dependency.chunk_id(), ChunkId::of(&base));
    assert_eq!(
        dependency.logical_length(),
        u32::try_from(base.len()).expect("fixture length fits u32")
    );
    assert_eq!(decoded.chunk_id(), ChunkId::of(&target));
    assert_eq!(decoded.payload(), target);
    assert!(first.len().is_multiple_of(64));
}

#[test]
fn codec_four_writer_rejects_noncanonical_runs_and_wrong_bases() {
    let base = deterministic_bytes(16 * 1_024, 11);
    let mut target = base.clone();
    target[99] ^= 0x44;
    let encoded = SparseXorRecord::encode(&base, &target).expect("Sparse-XOR encode succeeds");
    let wrong_base = deterministic_bytes(base.len(), 12);
    assert_eq!(
        SparseXorRecord::decode(&encoded, &wrong_base),
        Err(FormatError::DependentBaseMismatch)
    );

    let id = ChunkId::of(&base);
    assert!(
        SparseXorRecord::prepare(
            id,
            u32::try_from(base.len()).expect("fixture length fits u32"),
            ChunkId::of(&target),
            vec![SparseXorRun::new(10, 2), SparseXorRun::new(12, 1)].into_boxed_slice(),
            vec![1, 2, 3].into_boxed_slice(),
        )
        .is_err(),
        "adjacent runs are noncanonical"
    );
    assert!(
        SparseXorRecord::prepare(
            id,
            u32::try_from(base.len()).expect("fixture length fits u32"),
            ChunkId::of(&target),
            vec![SparseXorRun::new(10, 1)].into_boxed_slice(),
            vec![0].into_boxed_slice(),
        )
        .is_err(),
        "zero XOR bytes are noncanonical"
    );
}

#[test]
fn codec_four_reader_rejects_reauthenticated_noncanonical_run_tables() {
    const RECORD_CRC_OFFSET: usize = 60;
    const RUN_TABLE_OFFSET: usize = 192;

    let base = deterministic_bytes(16 * 1_024, 19);
    let mut target = base.clone();
    target[99] ^= 0x44;
    target[1_024] ^= 0x55;
    let mut encoded = SparseXorRecord::encode(&base, &target).expect("fixture encodes");

    let first_offset = u32::from_le_bytes(
        encoded[RUN_TABLE_OFFSET..RUN_TABLE_OFFSET + 4]
            .try_into()
            .expect("fixed-width field"),
    );
    let first_length = u32::from_le_bytes(
        encoded[RUN_TABLE_OFFSET + 4..RUN_TABLE_OFFSET + 8]
            .try_into()
            .expect("fixed-width field"),
    );
    let adjacent_offset = first_offset + first_length;
    encoded[RUN_TABLE_OFFSET + 8..RUN_TABLE_OFFSET + 12]
        .copy_from_slice(&adjacent_offset.to_le_bytes());
    encoded[RECORD_CRC_OFFSET..RECORD_CRC_OFFSET + 4].fill(0);
    let checksum = crc32c::crc32c(&encoded);
    encoded[RECORD_CRC_OFFSET..RECORD_CRC_OFFSET + 4].copy_from_slice(&checksum.to_le_bytes());

    assert_eq!(
        SparseXorRecord::dependency(&encoded),
        Err(FormatError::InvalidSparseXorRecord),
        "a valid CRC must not make a noncanonical run table acceptable"
    );
}

#[test]
fn codec_four_rejects_empty_unchanged_unequal_and_oversized_chunks() {
    assert!(SparseXorRecord::encode(&[], &[]).is_err());
    assert!(SparseXorRecord::encode(&[1; 16], &[2; 17]).is_err());
    assert!(SparseXorRecord::encode(&[1; 16], &[1; 16]).is_err());
    let oversized = vec![1; MAX_LOGICAL_CHUNK_BYTES + 1];
    assert!(SparseXorRecord::encode(&oversized, &oversized).is_err());
}

#[test]
fn codec_four_container_pairs_dependency_through_recovery_and_exact_evidence() {
    let base = deterministic_bytes(64 * 1_024, 31);
    let mut target = base.clone();
    target[7] ^= 0x7f;
    target[32_000..32_008]
        .iter_mut()
        .for_each(|byte| *byte ^= 0x91);
    let encoded = SealedContainer::encode_sparse_xor_pairs(
        ContainerId::new([0x74; 16]).expect("fixture Container ID is nonzero"),
        10,
        &[(base.as_slice(), target.as_slice())],
    )
    .expect("Sparse-XOR Container encode succeeds");
    let (image, publication) = encoded.into_publication_parts();
    assert_eq!(publication.sparse_xor_record_count(), 1);
    assert_eq!(publication.zstd_prefix_record_count(), 0);

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
        image.len() as u64,
    )
    .expect("Container envelope verifies");
    let range = descriptor
        .record_range(candidate)
        .expect("candidate pairs with envelope");
    let start = usize::try_from(range.offset()).expect("record offset fits memory");
    let record = &image[start..start + range.length()];
    let decoded = descriptor
        .decode_dependent_candidate(candidate, record, &base)
        .expect("Exact Sparse-XOR candidate verifies with its Base");
    assert_eq!(decoded.payload(), target);

    let mut resolve = |dependency: DependentDependency| {
        assert_eq!(dependency.chunk_id(), ChunkId::of(&base));
        Ok(base.clone())
    };
    let fully_decoded = SealedContainer::decode_with_dependent_resolver(&image, &mut resolve)
        .expect("full Container decode resolves its verified Base");
    assert_eq!(fully_decoded.sparse_xor_record_count(), 1);
    assert_eq!(
        fully_decoded
            .chunk(ChunkId::of(&target))
            .expect("target Chunk is present"),
        target
    );
    assert_eq!(
        SealedContainer::decode(&image),
        Err(FormatError::DependentBaseRequired)
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
