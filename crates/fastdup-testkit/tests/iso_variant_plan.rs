use std::collections::BTreeSet;

use fastdup_testkit::minimal_variant_plan;

#[test]
fn minimal_variant_plan_is_reproducible_unique_and_in_bounds() {
    let first = minimal_variant_plan(4_096, 10, 8, 0x4d59_5df4_d0f3_3173)
        .expect("valid deterministic mutation plan");
    let second =
        minimal_variant_plan(4_096, 10, 8, 0x4d59_5df4_d0f3_3173).expect("same seed is valid");

    assert_eq!(first, second);
    assert_eq!(first.len(), 10);
    assert!(first.windows(2).all(|pair| pair[0] != pair[1]));
    for variant in first {
        assert_eq!(variant.len(), 8);
        assert_eq!(
            variant
                .iter()
                .map(|mutation| mutation.offset)
                .collect::<BTreeSet<_>>()
                .len(),
            8
        );
        assert!(variant.iter().all(|mutation| mutation.offset < 4_096));
        assert!(variant.iter().all(|mutation| mutation.xor_mask != 0));
    }
}

#[test]
fn minimal_variant_plan_rejects_an_impossible_request() {
    assert!(minimal_variant_plan(1, 1, 2, 7).is_err());
}
