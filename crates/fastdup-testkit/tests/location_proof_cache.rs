use fastdup_format::{ContainerId, ExactIndexEntry, ExactIndexProfileId};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, MemoryPressureSnapshot, ReadIntent,
    ReadIntentScope, StorageIo, VerifiedReadCache, VerifiedReadCacheConfig,
};
use fastdup_testkit::MemoryStorageIo;

fn fixture() -> (
    MemoryStorageIo,
    ContainerRepository<MemoryStorageIo>,
    [ExactIndexEntry; 2],
    VerifiedReadCache,
) {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let entries = [0xe1, 0xe2].map(|id| {
        let publication = containers
            .publish_adaptive_regions_verified(
                ContainerId::new([id; 16]).unwrap(),
                u64::from(id),
                &[&[&vec![71; 32_768]]],
            )
            .unwrap();
        ExactIndexEntry::from_verified(publication.locations()[0]).unwrap()
    });
    assert_eq!(entries[0].chunk_id(), entries[1].chunk_id());
    let snapshot = MemoryPressureSnapshot::new(1 << 30, 1 << 29, 0);
    let cache = VerifiedReadCache::new_with_snapshot(
        VerifiedReadCacheConfig::conservative(snapshot),
        snapshot,
    )
    .unwrap();
    (storage, containers, entries, cache)
}

#[test]
fn verified_copies_of_one_chunk_do_not_block_each_others_admission() {
    let (storage, containers, entries, cache) = fixture();
    for entry in entries {
        containers.verify_location_cached(entry, &cache).unwrap();
    }
    let before = storage.operation_count();
    for _ in 0..3 {
        for entry in entries {
            containers.verify_location_cached(entry, &cache).unwrap();
        }
    }
    assert_eq!(
        storage.operation_count() - before,
        0,
        "both independently checked physical copies must stay reusable"
    );
    assert_eq!(cache.status().location_proofs().entries, 2);
    assert_eq!(cache.status().resident_bytes(), 0);
}

#[test]
fn retiring_a_cached_location_does_not_force_repeated_reads_of_its_replacement() {
    let (storage, containers, entries, cache) = fixture();
    let indexes = ExactIndexRunRepository::new(MemoryStorageIo::new());
    let profile = ExactIndexProfileId::new([0xe3; 32]).unwrap();
    indexes
        .append_level_zero(profile, vec![entries[0]])
        .unwrap();
    let resolve = || {
        containers
            .find_verified_location_with_index_cached(
                &indexes.pin_active_generation().unwrap(),
                entries[0].chunk_id(),
                u64::from(entries[0].logical_length()),
                &cache,
            )
            .unwrap()
    };
    assert_eq!(resolve(), Some(entries[0]));
    indexes
        .append_level_zero(
            profile,
            vec![ExactIndexEntry::retiring(entries[0]).unwrap(), entries[1]],
        )
        .unwrap();
    let before = storage.operation_count();
    assert_eq!(resolve(), Some(entries[1]));
    assert!(
        storage.operation_count() > before,
        "the replacement is cold"
    );
    let before = storage.operation_count();
    for _ in 0..3 {
        assert_eq!(resolve(), Some(entries[1]));
    }
    assert_eq!(
        storage.operation_count() - before,
        0,
        "an obsolete proof must not prevent retaining the verified replacement"
    );
    let before = storage.operation_count();
    {
        let _independent = ReadIntentScope::enter(ReadIntent::Independent);
        assert_eq!(resolve(), Some(entries[1]));
    }
    assert!(storage.operation_count() > before);
    indexes
        .append_level_zero(
            profile,
            vec![ExactIndexEntry::retiring(entries[1]).unwrap()],
        )
        .unwrap();
    assert_eq!(resolve(), None, "no cached copy may bypass retirement");
}

fn coverage(containers: &ContainerRepository<MemoryStorageIo>) -> fastdup_store::ScrubCoverage {
    let generations = fastdup_store::GenerationRepository::new(
        MemoryStorageIo::new(),
        fastdup_format::PolicySetId::new([0xe4; 32]).unwrap(),
    );
    generations
        .commit_namespace(&fastdup_format::NamespaceRoot::new(1024, 2, 0, vec![], vec![]).unwrap())
        .unwrap();
    let (_, required) = generations.recover_committed_for_mount(containers).unwrap();
    fastdup_store::ScrubCoverage::new(required)
}

#[test]
fn fresh_scrub_supplies_online_location_evidence_without_retaining_payloads() {
    let (storage, containers, entries, cache) = fixture();
    let mut coverage = coverage(&containers);
    containers
        .scrub_for_progress_with_cache::<MemoryStorageIo>(
            entries[0].location().container_id(),
            None,
            &mut coverage,
            100,
            &cache,
        )
        .unwrap();
    let before = storage.operation_count();
    containers
        .verify_location_cached(entries[0], &cache)
        .unwrap();
    assert_eq!(
        storage.operation_count() - before,
        0,
        "a full scrub already verified the exact physical Location in this process"
    );
    assert_eq!(cache.status().location_proofs().entries, 1);
    assert_eq!(cache.status().resident_bytes(), 0);
    let name = format!("{}.fdc", "e1".repeat(16));
    storage
        .write_at(&name, entries[0].location().record_offset(), &[0xff])
        .unwrap();
    assert!(
        containers
            .scrub_for_progress_with_cache::<MemoryStorageIo>(
                entries[0].location().container_id(),
                None,
                &mut coverage,
                101,
                &cache,
            )
            .is_err(),
        "a warm proof must not satisfy a subsequent physical scrub"
    );
}

#[test]
fn resumed_or_failed_scrub_cannot_admit_current_location_evidence() {
    let (storage, containers, entries, cache) = fixture();
    let mut coverage = coverage(&containers);
    let certificate = containers
        .scrub_for_progress::<MemoryStorageIo>(
            entries[0].location().container_id(),
            None,
            &mut coverage,
            100,
        )
        .unwrap();
    assert!(
        containers
            .resume_scrub(&certificate, &mut coverage)
            .unwrap()
    );
    assert_eq!(cache.status().location_proofs().entries, 0);
    let name = format!("{}.fdc", "e1".repeat(16));
    storage
        .write_at(&name, entries[0].location().record_offset(), &[0xff])
        .unwrap();
    assert!(
        containers
            .scrub_for_progress_with_cache::<MemoryStorageIo>(
                entries[0].location().container_id(),
                None,
                &mut coverage,
                101,
                &cache,
            )
            .is_err()
    );
    assert_eq!(cache.status().location_proofs().entries, 0);
    assert!(
        containers
            .verify_location_cached(entries[0], &cache)
            .is_err()
    );
    assert_eq!(cache.status().location_proofs().entries, 0);
}

#[test]
fn fresh_scrub_evidence_respects_caller_intent_and_common_pressure() {
    for (intent, pressure) in [
        (ReadIntent::Independent, false),
        (ReadIntent::Scan, false),
        (ReadIntent::Demand, true),
    ] {
        let (_, containers, entries, cache) = fixture();
        if pressure {
            cache.update_memory_pressure(MemoryPressureSnapshot::new(1 << 30, 0, 0));
        }
        let mut coverage = coverage(&containers);
        let _intent = ReadIntentScope::enter(intent);
        containers
            .scrub_for_progress_with_cache::<MemoryStorageIo>(
                entries[0].location().container_id(),
                None,
                &mut coverage,
                100,
                &cache,
            )
            .unwrap();
        assert_eq!(cache.status().location_proofs().entries, 0);
        assert_eq!(cache.status().resident_bytes(), 0);
        coverage.finish().unwrap();
    }
}

#[test]
fn cold_graph_verification_uses_one_exact_lookup() {
    use fastdup_store::{IndexedRequiredChunkVerifier, RequiredChunkVerifier};
    let (_, containers, entries, cache) = fixture();
    let metadata = MemoryStorageIo::new();
    let indexes = ExactIndexRunRepository::new(metadata.clone());
    indexes
        .append_level_zero(
            ExactIndexProfileId::new([0xe5; 32]).unwrap(),
            vec![entries[0]],
        )
        .unwrap();
    let active = indexes.pin_active_generation().unwrap();
    let _independent = ReadIntentScope::enter(ReadIntent::Independent);
    let before = metadata.operation_count();
    active
        .lookup_transitions(entries[0].chunk_id(), entries[0].logical_length())
        .unwrap();
    let one_lookup = metadata.operation_count() - before;
    assert!(one_lookup > 0);
    let verifier = IndexedRequiredChunkVerifier::new(containers, active)
        .with_verified_read_cache(std::sync::Arc::new(cache));
    let before = metadata.operation_count();
    verifier
        .verify_required_chunks(&std::collections::BTreeMap::from([(
            entries[0].chunk_id(),
            u64::from(entries[0].logical_length()),
        )]))
        .unwrap();
    assert_eq!(
        metadata.operation_count() - before,
        one_lookup,
        "a cold proof probe must share its Exact lookup with verification"
    );
}
