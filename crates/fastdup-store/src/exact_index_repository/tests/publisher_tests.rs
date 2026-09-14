use super::*;

fn counted(label: &str) -> ExactIndexRunRepository<ReuseFaultStorage> {
    let initial = reuse_repository(label);
    ExactIndexRunRepository::new(ReuseFaultStorage {
        inner: initial.storage,
        fault: Arc::new(AtomicUsize::new(0)),
        scans: Arc::new(AtomicUsize::new(0)),
    })
}

#[test]
fn allocator_skips_orphans_and_observes_standalone_compaction_across_clones() {
    let repository = counted("allocator");
    let profile = ExactIndexProfileId::new([101; 32]).unwrap();
    let other_profile = ExactIndexProfileId::new([102; 32]).unwrap();
    let mut inputs = Vec::new();
    for generation in [80, 81] {
        let run = ExactIndexRun::new(profile, generation, vec![reuse_fixture(generation)]).unwrap();
        inputs.push(ExactIndexRunRef::new(0, repository.publish(&run).unwrap()).unwrap());
    }
    repository
        .append_level_zero(profile, vec![reuse_fixture(1)])
        .unwrap();
    let clone = repository.clone();
    clone.compact_family(&inputs, 1, 120).unwrap();
    clone
        .publish(&ExactIndexRun::new(other_profile, 200, vec![reuse_fixture(2)]).unwrap())
        .unwrap();
    repository
        .append_level_zero(profile, vec![reuse_fixture(3)])
        .unwrap();
    let current = repository.pin_active_generation().unwrap();
    assert_eq!(
        current
            .run_set()
            .runs()
            .iter()
            .map(|run| run.generation())
            .max(),
        Some(201)
    );
    drop(current);
    for ordinal in 4..20 {
        clone
            .append_level_zero(profile, vec![reuse_fixture(ordinal)])
            .unwrap();
    }
    assert_eq!(repository.storage.scans.load(AtomicOrdering::Relaxed), 1);
    assert!(
        repository
            .publication_timings()
            .iter()
            .any(|row| row.id == "exactCompaction" && row.completed > 0)
    );
    // Reopening reconstructs the allocator independently, including an orphan
    // which is absent from the installed Run Set and belongs to another profile.
    clone
        .publish(&ExactIndexRun::new(other_profile, 1000, vec![reuse_fixture(50)]).unwrap())
        .unwrap();
    let reopened = ExactIndexRunRepository::new(repository.storage.clone());
    reopened.recover_active_generation().unwrap();
    reopened
        .append_level_zero(profile, vec![reuse_fixture(51)])
        .unwrap();
    let latest = reopened.pin_active_generation().unwrap();
    assert!(
        latest
            .run_set()
            .runs()
            .iter()
            .any(|run| run.generation() >= 1001)
    );
    assert_eq!(repository.storage.scans.load(AtomicOrdering::Relaxed), 2);
    assert_eq!(
        reopened.audit_activation_log().unwrap(),
        Some(latest.record())
    );
}

#[test]
fn allocator_does_not_reuse_failed_run_reservations() {
    for mode in [3, 4] {
        let repository = counted(&format!("allocator-failure-{mode}"));
        let profile = ExactIndexProfileId::new([103; 32]).unwrap();
        repository
            .append_level_zero(profile, vec![reuse_fixture(1)])
            .unwrap();
        repository
            .storage
            .fault
            .store(mode, AtomicOrdering::Relaxed);
        assert!(
            repository
                .append_level_zero(profile, vec![reuse_fixture(2)])
                .is_err()
        );
        assert_eq!(repository.storage.fault.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(
            repository
                .recover_active()
                .unwrap()
                .unwrap()
                .run_set()
                .generation(),
            1
        );
        repository
            .clone()
            .append_level_zero(profile, vec![reuse_fixture(3)])
            .unwrap();
        let current = repository.pin_active_generation().unwrap();
        assert_eq!(
            current
                .run_set()
                .runs()
                .iter()
                .map(|run| run.generation())
                .max(),
            Some(3)
        );
        assert_eq!(repository.storage.scans.load(AtomicOrdering::Relaxed), 1);
        assert!(
            current
                .lookup_transitions(reuse_fixture(2).chunk_id(), 32)
                .unwrap()
                .candidates()
                .is_empty()
        );
        assert_eq!(
            repository.audit_activation_log().unwrap(),
            Some(current.record())
        );
    }
}

#[test]
fn allocator_exhaustion_does_not_overwrite_a_run_or_advance_activation() {
    let repository = counted("allocator-exhaustion");
    let profile = ExactIndexProfileId::new([104; 32]).unwrap();
    repository
        .publish(&ExactIndexRun::new(profile, u64::MAX, vec![reuse_fixture(1)]).unwrap())
        .unwrap();
    assert!(matches!(
        repository.append_level_zero(profile, vec![reuse_fixture(2)]),
        Err(ExactIndexStoreError::NonMonotonicRunSetGeneration)
    ));
    assert!(repository.recover_active().unwrap().is_none());
    repository.audit(profile, u64::MAX).unwrap();
}
