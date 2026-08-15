use std::num::NonZeroUsize;

use fastdup_store::{ReductionEngine, ReductionFeatures, ReductionPolicy, ReductionRuntime};

#[test]
fn every_valid_feature_prefix_restores_deterministic_mixed_data_byte_exactly() {
    let policies = feature_prefixes();
    for seed in 0_u64..8 {
        let original = mixed_fixture(seed);
        let mut changed = original.clone();
        for ordinal in 0..6 {
            let offset = 4_096
                + usize::try_from((seed * 7_919 + ordinal * 65_537) % 900_000)
                    .expect("fixture offset fits usize");
            changed[offset] ^= 0x5a;
        }

        for (name, features) in &policies {
            let policy = ReductionPolicy::v1(*features).expect("fixture feature prefix is valid");
            let runtime = ReductionRuntime::new(
                NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
                16 * 1_024 * 1_024,
            )
            .expect("fixture runtime is valid");
            let mut engine = ReductionEngine::new(policy, runtime);

            for input in [&original, &changed, &changed] {
                let object = engine
                    .ingest(input)
                    .unwrap_or_else(|error| panic!("{name} seed {seed} ingest failed: {error}"));
                let restored = engine
                    .restore(object)
                    .unwrap_or_else(|error| panic!("{name} seed {seed} restore failed: {error}"));
                assert_eq!(restored, *input, "{name} seed {seed} changed bytes");
                let report = engine.report(object).expect("ingested object has a report");
                assert_eq!(report.logical_bytes(), input.len() as u64);
                assert!(report.physical_payload_bytes() <= report.logical_bytes());
            }
        }
    }
}

#[test]
fn worker_count_does_not_change_reduction_decisions() {
    let base = deterministic_bytes(4 * 1_024 * 1_024, 0x5eed);
    let mut changed = base.clone();
    for offset in (32 * 1_024..changed.len()).step_by(64 * 1_024) {
        changed[offset] ^= 0xa5;
    }
    let features = ReductionFeatures::RAW
        | ReductionFeatures::CDC
        | ReductionFeatures::EXACT
        | ReductionFeatures::SIMILARITY
        | ReductionFeatures::DELTA;
    let mut observations = Vec::new();

    for workers in [1, 2, 4, 8] {
        let policy = ReductionPolicy::v1(features).expect("all features are valid");
        let runtime = ReductionRuntime::new(
            NonZeroUsize::new(workers).expect("fixture worker count is nonzero"),
            16 * 1_024 * 1_024,
        )
        .expect("fixture runtime is valid");
        let mut engine = ReductionEngine::new(policy, runtime);
        let base_object = engine.ingest(&base).expect("base ingest succeeds");
        assert_eq!(
            engine.restore(base_object).expect("base restore succeeds"),
            base
        );
        let object = engine.ingest(&changed).expect("changed ingest succeeds");
        assert_eq!(
            engine.restore(object).expect("changed restore succeeds"),
            changed
        );
        let report = engine.report(object).expect("changed report exists");
        assert!(
            report.delta_chunks() > 0,
            "fixture must exercise Delta jobs"
        );
        observations.push((
            [
                report.logical_bytes(),
                report.physical_payload_bytes(),
                report.exact_hit_bytes(),
                report.delta_logical_bytes(),
                report.delta_payload_bytes(),
                report.fill_bytes(),
            ],
            [
                report.logical_chunks(),
                report.raw_chunks(),
                report.zstd_regions(),
                report.zstd_dictionary_regions(),
                report.delta_chunks(),
                report.similarity_candidates(),
                report.delta_trials(),
                report.exact_hits(),
                report.maximum_region_decoded_bytes(),
                report.fill_extents(),
                report.reordered_regions(),
                report.placement_windows(),
            ],
            report.maximum_delta_depth(),
        ));
    }

    assert!(
        observations.windows(2).all(|pair| pair[0] == pair[1]),
        "execution parallelism must not alter policy decisions"
    );
}

fn feature_prefixes() -> [(/* label */ &'static str, ReductionFeatures); 8] {
    let raw = ReductionFeatures::RAW;
    let cdc = raw | ReductionFeatures::CDC;
    [
        ("raw", raw),
        ("cdc", cdc),
        ("exact", cdc | ReductionFeatures::EXACT),
        ("compression", cdc | ReductionFeatures::COMPRESSION),
        (
            "grouping",
            cdc | ReductionFeatures::COMPRESSION | ReductionFeatures::GROUPING,
        ),
        (
            "similarity",
            cdc | ReductionFeatures::EXACT | ReductionFeatures::SIMILARITY,
        ),
        (
            "delta",
            cdc | ReductionFeatures::EXACT
                | ReductionFeatures::SIMILARITY
                | ReductionFeatures::DELTA,
        ),
        ("all", ReductionFeatures::ALL),
    ]
}

fn mixed_fixture(seed: u64) -> Vec<u8> {
    let mut data = deterministic_bytes(384 * 1_024, seed ^ 0x243f_6a88_85a3_08d3);
    let json = format!(
        "{{\"seed\":{seed},\"vm\":\"rocky\",\"path\":\"/var/lib/backup\",\"clean\":true}}\n"
    );
    data.extend(json.as_bytes().iter().copied().cycle().take(512 * 1_024));
    data.extend(std::iter::repeat_n(0_u8, 96 * 1_024));
    data.extend(deterministic_bytes(
        384 * 1_024,
        seed ^ 0x1319_8a2e_0370_7344,
    ));
    data
}

fn deterministic_bytes(length: usize, mut state: u64) -> Vec<u8> {
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}
