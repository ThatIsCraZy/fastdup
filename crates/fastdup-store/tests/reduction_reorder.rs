use std::num::NonZeroUsize;

use fastdup_store::{ReductionEngine, ReductionFeatures, ReductionPolicy, ReductionRuntime};

#[test]
fn reorder_clusters_only_inside_bounded_placement_windows_and_restores_exactly() {
    let input = deterministic_bytes(65 * 1_024 * 1_024 + 257 * 1_024);
    let policy = ReductionPolicy::v1(ReductionFeatures::RAW | ReductionFeatures::REORDER)
        .expect("RAW bounded Reorder is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(8).expect("fixture worker count is nonzero"),
        32 * 1_024 * 1_024,
    )
    .expect("fixture runtime is valid");
    let mut engine = ReductionEngine::new(policy, runtime);

    let object = engine.ingest(&input).expect("Reorder ingest succeeds");

    assert_eq!(
        engine.restore(object).expect("Reorder restore succeeds"),
        input
    );
    let report = engine.report(object).expect("report exists");
    assert_eq!(report.placement_windows(), 2);
    assert!(report.reordered_regions() > 0);
    assert!(report.reordered_regions() <= report.logical_chunks());
    assert!(report.workers_used() >= 2);
}

#[test]
fn reorder_off_preserves_the_physical_order_baseline() {
    let input = deterministic_bytes(2 * 1_024 * 1_024);
    let policy = ReductionPolicy::v1(ReductionFeatures::RAW).expect("RAW is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
        16 * 1_024 * 1_024,
    )
    .expect("fixture runtime is valid");
    let mut engine = ReductionEngine::new(policy, runtime);

    let object = engine.ingest(&input).expect("RAW ingest succeeds");

    assert_eq!(engine.restore(object).expect("RAW restore succeeds"), input);
    let report = engine.report(object).expect("report exists");
    assert_eq!(report.reordered_regions(), 0);
    assert_eq!(report.placement_windows(), 1);
}

fn deterministic_bytes(length: usize) -> Vec<u8> {
    let mut state = 0x510e_527f_ade6_82d1_u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}
