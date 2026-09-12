use super::*;
use fastdup_store::{ScrubCertificate, ScrubCoverage, ScrubProgress};
const JOURNAL: &str = ".fastdup-scrub-progress-v1";
const BINDING: [u8; 32] = [73; 32];

fn coverage(metadata: &MemoryStorageIo, data: &MemoryStorageIo) -> ScrubCoverage {
    let (_, required) = GenerationRepository::new(metadata.clone(), checkpoint_policy_set())
        .recover_committed_for_mount(&ContainerRepository::new(data.clone()))
        .unwrap();
    ScrubCoverage::new(required)
}
fn certificates(
    metadata: &MemoryStorageIo,
    data: &MemoryStorageIo,
    ids: &[u8],
) -> Vec<ScrubCertificate> {
    let mut work = coverage(metadata, data);
    let repository = ContainerRepository::new(data.clone());
    ids.iter()
        .map(|&n| {
            repository
                .scrub_for_progress::<MemoryStorageIo>(id(n), None, &mut work, 100)
                .unwrap()
        })
        .collect()
}
fn saved(entries: &[ScrubCertificate]) -> MemoryStorageIo {
    let disk = MemoryStorageIo::new();
    let mut journal = ScrubProgress::open(disk.clone(), BINDING, 100).unwrap();
    for entry in entries {
        journal.record(entry).unwrap();
    }
    journal.sync().unwrap();
    disk.crash();
    disk
}

#[test]
fn clean_stop_and_crash_resume_without_reading_payloads() {
    let (metadata, data, _) = fixture(true);
    let entries = certificates(&metadata, &data, &[31, 32]);
    let disk = saved(&entries);
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    let repository = ContainerRepository::new(data.clone());
    let mut work = coverage(&metadata, &data);
    let before = data.operation_count();
    for n in [32, 31] {
        // Dependency before Base must work as well.
        let entry = journal.lookup(id(n), 101).unwrap().unwrap();
        assert!(repository.resume_scrub(&entry, &mut work).unwrap());
    }
    assert_eq!(
        &data.operations()[before..],
        &[
            StorageOperation::ObjectLen,
            StorageOperation::ReadExactAt,
            StorageOperation::ReadExactAt,
            StorageOperation::ObjectLen,
            StorageOperation::ReadExactAt,
            StorageOperation::ReadExactAt,
        ]
    );
    work.finish().unwrap();
}

#[test]
fn missing_base_cannot_be_hidden_by_resumed_dependent_certificate() {
    let (metadata, data, _) = fixture(true);
    let disk = saved(&certificates(&metadata, &data, &[31, 32]));
    data.remove_file(&name(31)).unwrap();
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    let repository = ContainerRepository::new(data.clone());
    let mut work = coverage(&metadata, &data);
    assert!(
        repository
            .resume_scrub(&journal.lookup(id(32), 101).unwrap().unwrap(), &mut work)
            .unwrap()
    );
    assert!(work.finish().is_err());
}

#[test]
fn missing_required_container_cannot_be_hidden_by_a_journal_entry() {
    let (metadata, data, _) = fixture(false);
    let disk = saved(&certificates(&metadata, &data, &[31]));
    data.remove_file(&name(31)).unwrap();
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    let repository = ContainerRepository::new(data.clone());
    let mut work = coverage(&metadata, &data);
    assert!(
        repository
            .resume_scrub(&journal.lookup(id(31), 101).unwrap().unwrap(), &mut work)
            .is_err()
    );
    assert!(work.finish().is_err());
}

#[test]
fn current_graph_requires_new_chunks_even_when_old_containers_resume() {
    let (metadata, data, _) = fixture(false);
    let disk = saved(&certificates(&metadata, &data, &[31]));
    let repository = ContainerRepository::new(data.clone());
    let payload = vec![55; 65536];
    repository.publish_raw(id(33), 2, &[&payload]).unwrap();
    let generations = GenerationRepository::new(metadata.clone(), checkpoint_policy_set());
    let manifest = generations
        .publish_manifest(
            &ManifestLeaf::new(
                payload.len() as u64,
                vec![ManifestExtent::Data {
                    logical_length: payload.len() as u64,
                    chunk_id: ChunkId::of(&payload),
                }],
            )
            .unwrap(),
        )
        .unwrap();
    let root = NamespaceRoot::new(
        1024,
        3,
        1,
        vec![DurableInode::new(2, 0o600, 0, 0, 1, 1, payload.len() as u64, manifest).unwrap()],
        vec![NamespaceEntry::new(1, 2, b"backup".to_vec()).unwrap()],
    )
    .unwrap();
    generations
        .commit_namespace_with_data(&root, &repository)
        .unwrap();
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    let mut work = coverage(&metadata, &data);
    repository
        .resume_scrub(&journal.lookup(id(31), 101).unwrap().unwrap(), &mut work)
        .unwrap();
    assert!(work.finish().is_err());
    let mut work = coverage(&metadata, &data);
    repository
        .resume_scrub(&journal.lookup(id(31), 101).unwrap().unwrap(), &mut work)
        .unwrap();
    repository
        .scrub_for_progress::<MemoryStorageIo>(id(33), None, &mut work, 101)
        .unwrap();
    work.finish().unwrap();
}

#[test]
fn retiring_container_never_supplies_current_coverage() {
    let (metadata, data, _) = fixture(false);
    let disk = saved(&certificates(&metadata, &data, &[31]));
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    let repository = ContainerRepository::new(data.clone());
    let mut work = coverage(&metadata, &data);
    repository.install_retiring_selection_barrier(&std::collections::BTreeMap::from([(
        id(31).bytes(),
        id(31),
    )]));
    assert!(
        repository
            .resume_scrub(&journal.lookup(id(31), 101).unwrap().unwrap(), &mut work)
            .unwrap()
    );
    assert!(work.finish().is_err());
}

#[test]
fn changed_envelope_is_not_resumed_and_damage_still_fails_full_scrub() {
    let (metadata, data, _) = fixture(false);
    let disk = saved(&certificates(&metadata, &data, &[31]));
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    let repository = ContainerRepository::new(data.clone());
    let mut work = coverage(&metadata, &data);
    data.remove_file(&name(31)).unwrap();
    repository
        .publish_raw(id(31), 999, &[&vec![41; 65536]])
        .unwrap();
    assert!(
        !repository
            .resume_scrub(&journal.lookup(id(31), 101).unwrap().unwrap(), &mut work)
            .unwrap()
    );
    assert!(work.finish().is_err());
    data.write_at(&name(31), 10, &[255]).unwrap();
    assert!(
        repository
            .resume_scrub(
                &journal.lookup(id(31), 101).unwrap().unwrap(),
                &mut coverage(&metadata, &data)
            )
            .is_err()
    );
    assert!(
        repository
            .scrub_container::<MemoryStorageIo>(id(31), None)
            .is_err()
    );
}

#[test]
fn old_check_does_not_bypass_demand_or_offline_payload_verification() {
    let (metadata, data, payload) = fixture(false);
    let disk = saved(&certificates(&metadata, &data, &[31]));
    data.write_at(&name(31), 4096 + 192, &[payload[0] ^ 1])
        .unwrap();
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    let repository = ContainerRepository::new(data.clone());
    assert!(
        repository
            .resume_scrub(
                &journal.lookup(id(31), 101).unwrap().unwrap(),
                &mut coverage(&metadata, &data)
            )
            .unwrap()
    );
    assert!(
        repository
            .scrub_container::<MemoryStorageIo>(id(31), None)
            .is_err()
    );
    let (_, mut files) = GenerationRepository::new(metadata, checkpoint_policy_set())
        .recover_latest_with_structural_files(&repository)
        .unwrap()
        .unwrap()
        .into_parts();
    assert!(
        files
            .pop()
            .unwrap()
            .into_file()
            .read_at(0, payload.len() as u32)
            .is_err()
    );
}

#[test]
fn completed_expired_foreign_and_clock_reversed_rounds_start_over() {
    let (metadata, data, _) = fixture(false);
    let entries = certificates(&metadata, &data, &[31]);
    for (binding, now, completed) in [
        (BINDING, 101, true),
        ([74; 32], 101, false),
        (BINDING, 100 + 7 * 86400, false),
        (BINDING, 99, false),
    ] {
        let disk = saved(&entries);
        if completed {
            ScrubProgress::open(disk.clone(), BINDING, 100)
                .unwrap()
                .complete()
                .unwrap();
        }
        disk.crash();
        let journal = ScrubProgress::open(disk, binding, now).unwrap();
        assert!(journal.lookup(id(31), now).unwrap().is_none());
    }
}

#[test]
fn torn_or_corrupt_second_record_preserves_only_the_verified_prefix() {
    let (metadata, data, _) = fixture(true);
    let entries = certificates(&metadata, &data, &[31, 32]);
    let disk = saved(&entries);
    let image = disk.read(JOURNAL).unwrap();
    let first_end = 80 + u32::from_le_bytes(image[80..84].try_into().unwrap()) as usize;
    for end in first_end..image.len() {
        let storage = saved(&entries);
        storage.inject_durable_torn_write(JOURNAL, end).unwrap();
        storage.crash();
        let journal = ScrubProgress::open(storage, BINDING, 101).unwrap();
        assert!(journal.lookup(id(31), 101).unwrap().is_some());
        assert!(journal.lookup(id(32), 101).unwrap().is_none());
    }
    for offset in [0, 8, 40, 48, first_end + 4, first_end + 70, image.len() - 1] {
        let storage = saved(&entries);
        storage
            .write_at(JOURNAL, offset as u64, &[image[offset] ^ 1])
            .unwrap();
        storage.sync_file(JOURNAL).unwrap();
        storage.crash();
        let journal = ScrubProgress::open(storage, BINDING, 101).unwrap();
        assert_eq!(
            journal.lookup(id(31), 101).unwrap().is_some(),
            offset >= first_end
        );
        assert!(journal.lookup(id(32), 101).unwrap().is_none());
    }
}

#[test]
fn crash_before_and_after_every_progress_io_keeps_only_fully_verified_entries() {
    let (metadata, data, _) = fixture(true);
    let entries = certificates(&metadata, &data, &[31, 32]);
    let run = |storage: MemoryStorageIo, complete: bool| -> std::io::Result<()> {
        let mut journal = ScrubProgress::open(storage, BINDING, 100)?;
        journal.record(&entries[0])?;
        journal.sync()?;
        journal.record(&entries[1])?;
        journal.sync()?;
        if complete {
            journal.complete()?;
        }
        Ok(())
    };
    for complete in [false, true] {
        let baseline = MemoryStorageIo::new();
        run(baseline.clone(), complete).unwrap();
        for point in 0..baseline.operation_count() {
            for after in [false, true] {
                let storage = if after {
                    MemoryStorageIo::with_fail_after(point)
                } else {
                    MemoryStorageIo::with_fail_before(point)
                };
                assert!(run(storage.clone(), complete).is_err());
                storage.crash();
                let journal = ScrubProgress::open(storage, BINDING, 101).unwrap();
                let repository = ContainerRepository::new(data.clone());
                let mut work = coverage(&metadata, &data);
                for n in [31, 32] {
                    if let Some(entry) = journal.lookup(id(n), 101).unwrap() {
                        assert!(repository.resume_scrub(&entry, &mut work).unwrap());
                    } else {
                        repository
                            .scrub_for_progress::<MemoryStorageIo>(id(n), None, &mut work, 101)
                            .unwrap();
                    }
                }
                work.finish().unwrap();
            }
        }
    }
}

#[test]
fn fresh_base_and_resumed_dependency_complete_in_either_order() {
    let (metadata, data, _) = fixture(true);
    let disk = saved(&certificates(&metadata, &data, &[32]));
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    let repository = ContainerRepository::new(data.clone());
    for order in [[31, 32], [32, 31]] {
        let mut work = coverage(&metadata, &data);
        for n in order {
            if let Some(entry) = journal.lookup(id(n), 101).unwrap() {
                assert!(repository.resume_scrub(&entry, &mut work).unwrap());
            } else {
                repository
                    .scrub_for_progress::<MemoryStorageIo>(id(n), None, &mut work, 101)
                    .unwrap();
            }
        }
        work.finish().unwrap();
    }
}

#[test]
fn interrupted_full_verification_never_mints_progress() {
    let (metadata, baseline, _) = fixture(false);
    let image = baseline.read(&name(31)).unwrap();
    let repository = ContainerRepository::new(baseline.clone());
    let before = baseline.operation_count();
    repository
        .scrub_for_progress::<MemoryStorageIo>(
            id(31),
            None,
            &mut coverage(&metadata, &baseline),
            100,
        )
        .unwrap();
    // A backend read failure is returned before a certificate can be recorded.
    for after in [false, true] {
        let data = if after {
            MemoryStorageIo::with_fail_after(3)
        } else {
            MemoryStorageIo::with_fail_before(3)
        };
        data.create_new(&name(31)).unwrap();
        data.write_at(&name(31), 0, &image).unwrap();
        data.sync_file(&name(31)).unwrap();
        let mut work = coverage(&metadata, &baseline);
        assert!(
            ContainerRepository::new(data)
                .scrub_for_progress::<MemoryStorageIo>(id(31), None, &mut work, 100)
                .is_err()
        );
        assert!(work.finish().is_err());
    }
    assert!(baseline.operation_count() > before);
}

#[test]
fn offline_repository_scrub_ignores_progress_even_when_progress_is_damaged() {
    let (metadata, data, _) = fixture(false);
    let entries = certificates(&metadata, &data, &[31]);
    let mut journal = ScrubProgress::open(metadata.clone(), BINDING, 100).unwrap();
    journal.record(&entries[0]).unwrap();
    journal.sync().unwrap();
    let maintenance = fastdup_store::MaintenanceRepository::new(
        GenerationRepository::new(metadata.clone(), checkpoint_policy_set()),
        ContainerRepository::new(data.clone()),
        ExactIndexRunRepository::new(metadata.clone()),
        fastdup_appliance::checkpoint_exact_index_profile_v1(),
    );
    maintenance.scrub().unwrap();
    metadata.write_at(JOURNAL, 0, b"broken").unwrap();
    maintenance.scrub().unwrap();
    data.write_at(&name(31), 4096 + 192, &[0xff]).unwrap();
    assert!(maintenance.scrub().is_err());
}

#[test]
fn crash_during_tail_repair_or_completed_round_reset_never_fabricates_work() {
    let (metadata, data, _) = fixture(true);
    let entries = certificates(&metadata, &data, &[31, 32]);
    let run = |storage: MemoryStorageIo| -> std::io::Result<()> {
        let mut journal = ScrubProgress::open(storage.clone(), BINDING, 100)?;
        journal.record(&entries[0])?;
        journal.sync()?;
        let end = storage.object_len(JOURNAL)?;
        storage.write_at(JOURNAL, end, &[250, 255, 255])?;
        storage.sync_file(JOURNAL)?;
        let mut journal = ScrubProgress::open(storage.clone(), BINDING, 101)?;
        journal.record(&entries[1])?;
        journal.sync()?;
        journal.complete()?;
        let fresh = ScrubProgress::open(storage, BINDING, 102)?;
        assert!(fresh.lookup(id(31), 102)?.is_none());
        Ok(())
    };
    let baseline = MemoryStorageIo::new();
    run(baseline.clone()).unwrap();
    for point in 0..baseline.operation_count() {
        for after in [false, true] {
            let storage = if after {
                MemoryStorageIo::with_fail_after(point)
            } else {
                MemoryStorageIo::with_fail_before(point)
            };
            assert!(run(storage.clone()).is_err());
            storage.crash();
            let journal = ScrubProgress::open(storage, BINDING, 103).unwrap();
            let repository = ContainerRepository::new(data.clone());
            let mut work = coverage(&metadata, &data);
            for n in [31, 32] {
                if let Some(entry) = journal.lookup(id(n), 103).unwrap() {
                    assert!(repository.resume_scrub(&entry, &mut work).unwrap());
                } else {
                    repository
                        .scrub_for_progress::<MemoryStorageIo>(id(n), None, &mut work, 103)
                        .unwrap();
                }
            }
            work.finish().unwrap();
        }
    }
}

#[test]
fn resume_pool_overlaps_32_envelope_reads_and_never_reads_payloads() {
    use fastdup_store::{SCRUB_RESUME_MAX_IOS, ScrubResumePool};
    use fastdup_testkit::PausedStorageIo;
    use std::time::Duration;
    let (metadata, data, _) = fixture(false);
    let entries = certificates(&metadata, &data, &[31; SCRUB_RESUME_MAX_IOS]);
    let work = coverage(&metadata, &data);
    let paused = PausedStorageIo::before(data.clone(), StorageOperation::ReadExactAt);
    let repository = ContainerRepository::new(paused.clone());
    let before = data.operation_count();
    let worker = std::thread::spawn(move || {
        let pool = ScrubResumePool::new().unwrap();
        let mut work = work;
        let outcomes = pool.resume(&repository, &entries, &mut work).unwrap();
        assert!(outcomes.iter().all(|matched| *matched));
        work.finish().unwrap();
    });
    let overlapped = paused.wait_until_reached_count(SCRUB_RESUME_MAX_IOS, Duration::from_secs(2));
    paused.resume();
    worker.join().unwrap();
    assert!(
        overlapped,
        "all 32 header requests must overlap rather than run serially"
    );
    let operations = &data.operations()[before..];
    assert_eq!(
        operations
            .iter()
            .filter(|op| **op == StorageOperation::ReadExactAt)
            .count(),
        64
    );
    assert!(!operations.contains(&StorageOperation::Read));
}

#[test]
fn resume_pool_rejects_oversized_batches_and_failed_batches_add_no_coverage() {
    use fastdup_store::{SCRUB_RESUME_MAX_IOS, ScrubResumePool};
    let (metadata, data, _) = fixture(true);
    let entries = certificates(&metadata, &data, &[31; SCRUB_RESUME_MAX_IOS + 1]);
    let mut work = coverage(&metadata, &data);
    let repository = ContainerRepository::new(data.clone());
    let pool = ScrubResumePool::new().unwrap();
    let before = data.operation_count();
    assert!(pool.resume(&repository, &entries, &mut work).is_err());
    assert_eq!(data.operation_count(), before);
    let entries = certificates(&metadata, &data, &[31, 32]);
    data.write_at(&name(31), 0, &[0; 4096]).unwrap();
    assert!(pool.resume(&repository, &entries, &mut work).is_err());
    assert!(
        work.finish().is_err(),
        "partial success cannot complete current graph coverage"
    );
}

#[test]
fn resume_prefetch_limits_certificate_memory_as_well_as_io_count() {
    let (metadata, data, _) = fixture(false);
    let repository = ContainerRepository::new(data.clone());
    let chunks = vec![b"x".as_slice(); 30000];
    repository.publish_raw(id(33), 3, &chunks).unwrap();
    let entries = certificates(&metadata, &data, &[31, 33]);
    let disk = saved(&entries);
    let journal = ScrubProgress::open(disk, BINDING, 101).unwrap();
    assert_eq!(journal.resume_batch_len(&[id(31); 40], 40), 32);
    assert_eq!(journal.resume_batch_len(&[id(31); 40], 1), 1);
    assert_eq!(journal.resume_batch_len(&[id(31), id(33)], 32), 1);
    assert_eq!(journal.resume_batch_len(&[id(33), id(31)], 32), 1);
}
