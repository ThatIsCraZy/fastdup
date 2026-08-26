use std::num::NonZeroUsize;

use fastdup_store::{ReductionEngine, ReductionFeatures, ReductionPolicy, ReductionRuntime};

#[test]
fn version_one_policy_exposes_every_independently_benchmarkable_stage() {
    let policy = ReductionPolicy::v1(ReductionFeatures::ALL)
        .expect("the complete bounded v1 feature matrix is valid");

    for feature in [
        ReductionFeatures::RAW,
        ReductionFeatures::CDC,
        ReductionFeatures::EXACT,
        ReductionFeatures::COMPRESSION,
        ReductionFeatures::GROUPING,
        ReductionFeatures::SIMILARITY,
        ReductionFeatures::DELTA,
        ReductionFeatures::REORDER,
        ReductionFeatures::ZSTD_PREFIX,
    ] {
        assert!(policy.features().contains(feature));
    }
    assert_eq!(policy.cdc_min_bytes(), 16 * 1_024);
    assert_eq!(policy.cdc_target_bytes(), 64 * 1_024);
    assert_eq!(policy.cdc_max_bytes(), 256 * 1_024);
    assert_eq!(policy.compression_region_bytes(), 512 * 1_024);
    assert_eq!(policy.placement_window_bytes(), 64 * 1_024 * 1_024);
    assert_eq!(policy.maximum_similarity_candidates(), 16);
    assert_eq!(policy.maximum_trial_encodes(), 4);
    assert_ne!(policy.id(), [0; 32]);
}

#[test]
fn policy_rejects_dependent_encoding_without_candidate_search() {
    let invalid = ReductionFeatures::RAW | ReductionFeatures::DELTA;

    assert!(ReductionPolicy::v1(invalid).is_err());

    let prefix_without_delta =
        ReductionFeatures::RAW | ReductionFeatures::SIMILARITY | ReductionFeatures::ZSTD_PREFIX;
    assert!(ReductionPolicy::v1(prefix_without_delta).is_err());
}

#[test]
fn raw_baseline_round_trips_empty_and_chunk_boundary_inputs() {
    let policy = ReductionPolicy::v1(ReductionFeatures::RAW).expect("RAW baseline is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(3).expect("fixture worker count is nonzero"),
        8 * 1_024 * 1_024,
    )
    .expect("fixture memory budget is valid");
    let mut engine = ReductionEngine::new(policy, runtime);

    for length in [0_usize, 1, 65_535, 65_536, 65_537] {
        let input = (0..length)
            .map(|offset| u8::try_from(offset % 251).expect("fixture byte is bounded"))
            .collect::<Vec<_>>();
        let object = engine.ingest(&input).expect("RAW ingest succeeds");

        assert_eq!(engine.restore(object).expect("RAW restore succeeds"), input);
        let report = engine.report(object).expect("report exists for object");
        assert_eq!(
            report.logical_bytes(),
            u64::try_from(length).expect("fixture length fits u64")
        );
        assert_eq!(report.raw_chunks(), length.div_ceil(64 * 1_024));
        assert_eq!(report.zstd_regions(), 0);
        assert_eq!(report.delta_chunks(), 0);
        assert_eq!(report.exact_hits(), 0);
    }
}

#[test]
fn seqcdc_resynchronizes_after_an_insertion_and_exact_reuses_the_unchanged_suffix() {
    let policy = ReductionPolicy::v1(
        ReductionFeatures::RAW | ReductionFeatures::CDC | ReductionFeatures::EXACT,
    )
    .expect("SeqCDC plus Exact is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
        16 * 1_024 * 1_024,
    )
    .expect("fixture memory budget is valid");
    let mut engine = ReductionEngine::new(policy, runtime);
    let original = deterministic_bytes(4 * 1_024 * 1_024);
    let first = engine.ingest(&original).expect("first ingest succeeds");
    assert_eq!(
        engine.restore(first).expect("first restore succeeds"),
        original
    );

    let insertion_offset = 1_234_567;
    let mut shifted = original.clone();
    shifted.insert(insertion_offset, 0xa7);
    let second = engine.ingest(&shifted).expect("shifted ingest succeeds");
    assert_eq!(
        engine.restore(second).expect("shifted restore succeeds"),
        shifted
    );

    let report = engine.report(second).expect("second report exists");
    assert!(report.logical_chunks() > 16);
    assert!(report.minimum_chunk_bytes() >= 16 * 1_024);
    assert!(report.maximum_chunk_bytes() <= 256 * 1_024);
    assert!(
        report.exact_hit_bytes() >= 3 * 1_024 * 1_024,
        "SeqCDC should resynchronize and reuse most of a 4-MiB shifted stream"
    );
    assert!(report.exact_hits() > 0);
}

#[test]
fn grouped_zstd_regions_are_bounded_parallel_and_byte_exact() {
    let policy = ReductionPolicy::v1(
        ReductionFeatures::RAW | ReductionFeatures::COMPRESSION | ReductionFeatures::GROUPING,
    )
    .expect("grouped Zstd plus RAW fallback is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
        16 * 1_024 * 1_024,
    )
    .expect("fixture memory budget is valid");
    let mut engine = ReductionEngine::new(policy, runtime);
    let line = br#"{"vm":"rocky","generation":42,"path":"/var/lib/backup","state":"clean"}
"#;
    let input = line
        .iter()
        .copied()
        .cycle()
        .take(4 * 1_024 * 1_024)
        .collect::<Vec<_>>();

    let object = engine.ingest(&input).expect("grouped Zstd ingest succeeds");

    assert_eq!(
        engine.restore(object).expect("Zstd restore succeeds"),
        input
    );
    let report = engine.report(object).expect("Zstd report exists");
    assert!(report.zstd_regions() >= 8);
    assert_eq!(report.raw_chunks(), 0);
    assert!(report.physical_payload_bytes() < report.logical_bytes() / 4);
    assert!(report.maximum_region_decoded_bytes() <= 512 * 1_024);
    assert!(report.workers_used() >= 2);
}

#[test]
fn similar_mutated_chunks_use_only_bounded_depth_one_delta_trials() {
    let policy = ReductionPolicy::v1(
        ReductionFeatures::RAW
            | ReductionFeatures::EXACT
            | ReductionFeatures::SIMILARITY
            | ReductionFeatures::DELTA,
    )
    .expect("bounded Depth-1 Delta plus an independent RAW fallback is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
        16 * 1_024 * 1_024,
    )
    .expect("fixture memory budget is valid");
    let mut engine = ReductionEngine::new(policy, runtime);
    let original = deterministic_bytes(4 * 1_024 * 1_024);
    let first = engine.ingest(&original).expect("base ingest succeeds");

    assert_eq!(
        engine.restore(first).expect("base restore succeeds"),
        original
    );

    let mutated_chunk_ordinals = [3_usize, 17, 41, 60];
    let mut mutated = original.clone();
    for (mutation, chunk_ordinal) in mutated_chunk_ordinals.into_iter().enumerate() {
        let chunk_start = chunk_ordinal * 64 * 1_024;
        for byte_offset in 29_001..29_017 {
            mutated[chunk_start + byte_offset] ^=
                u8::try_from(mutation + 1).expect("fixture mutation fits one byte");
        }
    }

    let second = engine.ingest(&mutated).expect("mutated ingest succeeds");

    assert_eq!(
        engine.restore(second).expect("Delta restore succeeds"),
        mutated
    );
    let report = engine.report(second).expect("Delta report exists");
    assert_eq!(report.logical_chunks(), 64);
    assert_eq!(report.exact_hits(), 60);
    assert_eq!(report.delta_chunks(), mutated_chunk_ordinals.len());
    assert_eq!(
        report.delta_logical_bytes(),
        u64::try_from(mutated_chunk_ordinals.len() * 64 * 1_024)
            .expect("fixture Delta bytes fit u64")
    );
    assert!(report.delta_payload_bytes() < report.delta_logical_bytes());
    assert!(report.similarity_candidates() >= mutated_chunk_ordinals.len());
    assert!(
        report.similarity_candidates()
            <= mutated_chunk_ordinals.len() * usize::from(policy.maximum_similarity_candidates())
    );
    assert!(report.delta_trials() >= mutated_chunk_ordinals.len());
    assert!(
        report.delta_trials()
            <= mutated_chunk_ordinals.len() * usize::from(policy.maximum_trial_encodes())
    );
    assert_eq!(report.maximum_delta_depth(), 1);

    let audit = engine.audit().expect("offline reduction AUDIT succeeds");
    assert_eq!(audit.objects_verified(), 2);
    assert_eq!(audit.logical_bytes_verified(), 8 * 1_024 * 1_024);
    assert!(audit.records_verified() >= 64);
    assert!(audit.chunks_verified() >= 64);
}

#[test]
fn shifted_incompressible_chunk_selects_zstd_prefix_over_sparse_xor() {
    let policy = ReductionPolicy::v1(
        ReductionFeatures::RAW
            | ReductionFeatures::EXACT
            | ReductionFeatures::SIMILARITY
            | ReductionFeatures::DELTA
            | ReductionFeatures::ZSTD_PREFIX,
    )
    .expect("Zstd Prefix plus an independent RAW Base is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(2).expect("fixture worker count is nonzero"),
        4 * 1_024 * 1_024,
    )
    .expect("fixture memory budget is valid");
    let mut engine = ReductionEngine::new(policy, runtime);
    let original = deterministic_bytes(64 * 1_024);
    engine.ingest(&original).expect("Base ingest succeeds");

    let mut shifted = original.clone();
    shifted.rotate_left(1);
    let object = engine.ingest(&shifted).expect("Prefix ingest succeeds");

    assert_eq!(
        engine.restore(object).expect("Prefix restore succeeds"),
        shifted
    );
    let report = engine.report(object).expect("Prefix report exists");
    assert_eq!(report.logical_chunks(), 1);
    assert_eq!(report.delta_chunks(), 1);
    assert_eq!(report.zstd_prefix_chunks(), 1);
    assert_eq!(report.maximum_delta_depth(), 1);
    assert!(report.delta_trials() <= usize::from(policy.maximum_trial_encodes()));
    engine.audit().expect("Prefix archive AUDIT succeeds");
}

fn deterministic_bytes(length: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}
