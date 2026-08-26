use fastdup_format::{
    ChunkId, ExactIndexProfileId, ExactIndexRunSet, SimilarityBucketKey, SimilarityIndexEntry,
    SimilarityIndexPartitionRef, SimilarityIndexRun, SimilarityIndexRunFamily,
};

#[test]
fn partition_family_has_stable_round_trip_and_rejects_corruption() {
    let first = run(41, 0, 8);
    let second = run(41, 8, 16);
    let references = vec![
        SimilarityIndexPartitionRef::new(
            41,
            0,
            2,
            descriptor(&first),
            bucket_key(0, 0),
            bucket_key(1, 99),
        )
        .expect("construct first partition reference"),
        SimilarityIndexPartitionRef::new(
            41,
            1,
            2,
            descriptor(&second),
            bucket_key(2, 0),
            bucket_key(3, 99),
        )
        .expect("construct second partition reference"),
    ];
    let family = SimilarityIndexRunFamily::new(1, 1, 41, 16, references)
        .expect("construct partition family");
    let encoded = family.encode().expect("encode partition family");

    assert_eq!(
        SimilarityIndexRunFamily::decode(&encoded).expect("decode partition family"),
        family
    );
    let mut corrupt = encoded;
    corrupt[4_096 + 50] ^= 1;
    assert!(SimilarityIndexRunFamily::decode(&corrupt).is_err());
}

#[test]
fn partition_family_rejects_overlapping_bucket_ranges() {
    let first = run(42, 0, 4);
    let second = run(42, 4, 8);
    let references = vec![
        SimilarityIndexPartitionRef::new(
            42,
            0,
            2,
            descriptor(&first),
            bucket_key(0, 0),
            bucket_key(2, 9),
        )
        .expect("construct first overlapping reference"),
        SimilarityIndexPartitionRef::new(
            42,
            1,
            2,
            descriptor(&second),
            bucket_key(2, 9),
            bucket_key(3, 99),
        )
        .expect("construct second overlapping reference"),
    ];

    assert!(SimilarityIndexRunFamily::new(1, 1, 42, 8, references).is_err());
}

#[test]
fn bound_empty_family_round_trips_the_exact_run_set_identity() {
    let exact = ExactIndexRunSet::new(
        ExactIndexProfileId::new([0x55; 32]).expect("nonzero Exact profile"),
        7,
        Vec::new(),
    )
    .expect("construct empty Exact Run Set");
    let exact_id = exact.id().expect("identify Exact Run Set");
    let family = SimilarityIndexRunFamily::new_bound(1, 1, 43, 0, exact_id, Vec::new())
        .expect("construct bound empty Similarity family");
    let encoded = family.encode().expect("encode bound empty family");
    let decoded = SimilarityIndexRunFamily::decode(&encoded).expect("decode bound empty family");

    assert_eq!(decoded, family);
    assert_eq!(decoded.source_exact_run_set_id(), Some(exact_id));
    assert!(SimilarityIndexRunFamily::new(1, 1, 43, 0, Vec::new()).is_err());
}

fn run(generation: u64, start: u64, end: u64) -> SimilarityIndexRun {
    SimilarityIndexRun::new(
        1,
        1,
        generation,
        (start..end)
            .map(|ordinal| {
                SimilarityIndexEntry::new(
                    ChunkId::of(&ordinal.to_le_bytes()),
                    64 * 1_024,
                    1,
                    [
                        ordinal * 4,
                        ordinal * 4 + 1,
                        ordinal * 4 + 2,
                        ordinal * 4 + 3,
                    ],
                    [ordinal; 8],
                )
                .expect("construct family fixture entry")
            })
            .collect(),
    )
    .expect("construct family fixture Run")
}

fn descriptor(run: &SimilarityIndexRun) -> fastdup_format::SimilarityIndexRunDescriptor {
    let encoded = run.encode().expect("encode family fixture Run");
    let footer = &encoded[encoded.len() - 4_096..];
    fastdup_format::SimilarityIndexRunDescriptor::decode(
        &encoded[..4_096],
        footer,
        u64::try_from(encoded.len()).expect("fixture length fits u64"),
    )
    .expect("decode family fixture descriptor")
}

fn bucket_key(slot: u8, value: u64) -> SimilarityBucketKey {
    SimilarityBucketKey::new(1, slot, 64 * 1_024, value).expect("construct family BucketKey")
}
