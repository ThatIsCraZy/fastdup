use std::num::NonZeroUsize;

use fastdup_store::{
    ReductionDictionary, ReductionEngine, ReductionFeatures, ReductionPolicy, ReductionRuntime,
};

#[test]
fn trained_dictionary_is_selected_only_when_it_beats_plain_zstd_and_restores_exactly() {
    let samples = (0..64).map(structured_object).collect::<Vec<_>>();
    let dictionary =
        ReductionDictionary::train_v1(&samples, 64 * 1_024).expect("training succeeds");
    let input = structured_object(800);
    assert!(input.len() > 16 * 1_024);

    let policy = ReductionPolicy::v1(
        ReductionFeatures::RAW | ReductionFeatures::COMPRESSION | ReductionFeatures::GROUPING,
    )
    .expect("dictionary compression policy is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(4).expect("fixture worker count is nonzero"),
        16 * 1_024 * 1_024,
    )
    .expect("fixture runtime is valid");
    let mut plain_engine = ReductionEngine::new(policy, runtime);
    let plain_object = plain_engine
        .ingest(&input)
        .expect("plain Zstd ingest succeeds");
    let plain_bytes = plain_engine
        .report(plain_object)
        .expect("plain report exists")
        .physical_payload_bytes();
    let mut engine = ReductionEngine::with_dictionary(policy, runtime, &dictionary)
        .expect("the immutable dictionary prepares");

    let object = engine.ingest(&input).expect("dictionary ingest succeeds");

    assert_eq!(
        engine.restore(object).expect("dictionary restore succeeds"),
        input
    );
    let report = engine.report(object).expect("report exists");
    assert!(
        report.zstd_dictionary_regions() > 0,
        "dictionary was not selected: plain={plain_bytes}, selected={}",
        report.physical_payload_bytes()
    );
    assert!(report.zstd_dictionary_regions() <= report.zstd_regions());
    assert!(report.physical_payload_bytes() < plain_bytes);
    assert!(report.physical_payload_bytes() < report.logical_bytes());
}

#[test]
fn dictionary_configuration_requires_compression() {
    let dictionary = ReductionDictionary::from_bytes(b"immutable dictionary bytes")
        .expect("fixture dictionary is valid");
    let policy = ReductionPolicy::v1(ReductionFeatures::RAW).expect("RAW is valid");
    let runtime = ReductionRuntime::new(
        NonZeroUsize::new(1).expect("one worker is nonzero"),
        1_048_576,
    )
    .expect("fixture runtime is valid");

    assert!(ReductionEngine::with_dictionary(policy, runtime, &dictionary).is_err());
}

fn structured_object(generation: u32) -> Vec<u8> {
    let mut output = b"{\n".to_vec();
    for field in 0..256_u32 {
        let key = hex_bytes(u64::from(field) ^ 0x6a09_e667_f3bc_c909, 24);
        let value = hex_bytes((u64::from(generation) << 32) | u64::from(field), 64);
        output.extend_from_slice(format!("  \"{key}\":\"{value}\",\n").as_bytes());
    }
    output.extend_from_slice(b"  \"state\":\"clean\"\n}\n");
    output
}

fn hex_bytes(mut state: u64, digits: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(digits);
    for _ in 0..digits {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        output.push(char::from(HEX[(state & 0x0f) as usize]));
    }
    output
}
