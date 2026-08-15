use std::num::NonZeroUsize;

use fastdup_store::{ReductionEngine, ReductionFeatures, ReductionPolicy, ReductionRuntime};

const FILL_THRESHOLD: usize = 64 * 1_024;

#[test]
fn long_constant_runs_become_zero_payload_fill_extents_and_restore_exactly() {
    let mut input = deterministic_bytes(24 * 1_024, 0x18d7_31a9_0042_0001);
    input.extend(std::iter::repeat_n(0x11, FILL_THRESHOLD));
    input.extend(deterministic_bytes(24 * 1_024, 0x18d7_31a9_0042_0002));
    let mut engine = engine();

    let object = engine.ingest(&input).expect("FILL ingest succeeds");

    assert_eq!(
        engine.restore(object).expect("FILL restore succeeds"),
        input
    );
    let report = engine.report(object).expect("FILL report exists");
    assert_eq!(report.fill_extents(), 1);
    assert_eq!(report.fill_bytes(), FILL_THRESHOLD as u64);
    assert_eq!(
        report.physical_payload_bytes(),
        u64::try_from(input.len() - FILL_THRESHOLD).expect("fixture length fits u64")
    );
}

#[test]
fn fill_threshold_is_inclusive_and_shorter_runs_remain_data() {
    for (length, expected_fills) in [
        (FILL_THRESHOLD - 1, 0_usize),
        (FILL_THRESHOLD, 1),
        (FILL_THRESHOLD + 1, 1),
    ] {
        let input = vec![0_u8; length];
        let mut engine = engine();
        let object = engine.ingest(&input).expect("constant ingest succeeds");

        assert_eq!(engine.restore(object).expect("restore succeeds"), input);
        let report = engine.report(object).expect("report exists");
        assert_eq!(report.fill_extents(), expected_fills);
        assert_eq!(
            report.fill_bytes(),
            if expected_fills == 0 {
                0
            } else {
                u64::try_from(length).expect("fixture length fits u64")
            }
        );
    }
}

#[test]
fn adjacent_different_constant_runs_remain_distinct_fill_extents() {
    let mut input = vec![0_u8; FILL_THRESHOLD];
    input.extend(std::iter::repeat_n(0xff, FILL_THRESHOLD));
    let mut engine = engine();

    let object = engine.ingest(&input).expect("two-FILL ingest succeeds");

    assert_eq!(engine.restore(object).expect("restore succeeds"), input);
    let report = engine.report(object).expect("report exists");
    assert_eq!(report.fill_extents(), 2);
    assert_eq!(report.fill_bytes(), (2 * FILL_THRESHOLD) as u64);
    assert_eq!(report.physical_payload_bytes(), 0);
}

fn engine() -> ReductionEngine {
    let policy = ReductionPolicy::v1(ReductionFeatures::RAW | ReductionFeatures::CDC)
        .expect("RAW plus CDC is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(4).expect("fixture workers are nonzero"),
        16 * 1_024 * 1_024,
    )
    .expect("fixture runtime is valid");
    ReductionEngine::new(policy, runtime)
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
