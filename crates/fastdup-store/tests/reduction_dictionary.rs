use fastdup_store::{ReductionDictionary, ReductionDictionaryError};

#[test]
fn dictionary_identity_changes_with_exact_bytes() {
    let first = ReductionDictionary::from_bytes(b"dictionary version one")
        .expect("first dictionary is valid");
    let second = ReductionDictionary::from_bytes(b"dictionary version two")
        .expect("second dictionary is valid");

    assert_ne!(first.id(), second.id());
    assert_eq!(first.len(), first.bytes().len());
    assert_eq!(first.bytes(), b"dictionary version one");
}

#[test]
fn verified_construction_rejects_the_wrong_identity() {
    let expected = ReductionDictionary::from_bytes(b"expected dictionary")
        .expect("fixture is valid")
        .id();
    let error = ReductionDictionary::from_verified_bytes(expected, b"different dictionary")
        .expect_err("different exact bytes cannot reuse an ID");

    assert!(matches!(
        error,
        ReductionDictionaryError::HashMismatch { .. }
    ));
}

#[test]
fn version_one_training_is_deterministic() {
    let samples = training_samples();
    let first = ReductionDictionary::train_v1(&samples, 4_096)
        .expect("first deterministic training succeeds");
    let second = ReductionDictionary::train_v1(&samples, 4_096)
        .expect("second deterministic training succeeds");

    assert_eq!(first.id(), second.id());
    assert_eq!(first.bytes(), second.bytes());
    assert!(!first.bytes().is_empty());
    assert!(first.len() <= 4_096);
}

#[test]
fn empty_and_unsafe_training_inputs_are_rejected() {
    assert!(matches!(
        ReductionDictionary::from_bytes([]),
        Err(ReductionDictionaryError::EmptyBytes)
    ));
    assert!(matches!(
        ReductionDictionary::train_v1::<Vec<u8>>(&[], 4_096),
        Err(ReductionDictionaryError::EmptyTrainingSet)
    ));
    assert!(matches!(
        ReductionDictionary::train_v1(&[Vec::new()], 4_096),
        Err(ReductionDictionaryError::EmptyTrainingSample(0))
    ));
    assert!(matches!(
        ReductionDictionary::train_v1(&[b"sample"], 0),
        Err(ReductionDictionaryError::InvalidTrainingMaximum { .. })
    ));
    assert!(matches!(
        ReductionDictionary::train_v1(&[b"sample"], usize::MAX),
        Err(ReductionDictionaryError::InvalidTrainingMaximum { .. })
    ));
    assert!(matches!(
        ReductionDictionary::train_v1(&[b"too small"], 256),
        Err(ReductionDictionaryError::Training(_))
    ));
}

fn training_samples() -> Vec<Vec<u8>> {
    (0..256_u16)
        .map(|generation| {
            format!(
                "{{\"vm\":\"rocky-{generation:04}\",\"path\":\"/var/lib/backup/{generation:04}\",\"state\":\"clean\",\"generation\":{generation}}}\n"
            )
            .into_bytes()
        })
        .collect()
}
