use super::super::*;

#[test]
fn block_fill_scan_matches_scalar_across_fragment_and_lane_edges() {
    for length in 1..=512 {
        for value in [0, 31, 255] {
            for split in [1, 31, 32, 33, length] {
                let mut bytes = vec![value; length];
                for bad in [None, Some(0), Some(length / 2), Some(length - 1)] {
                    if let Some(index) = bad {
                        bytes[index] ^= 1;
                    }
                    let parts = bytes
                        .chunks(split)
                        .map(|part| MutationPayload::from_owned_bytes(part.to_vec()))
                        .collect();
                    let input = ChunkFragments::new(parts, length);
                    assert_eq!(input.is_fill(), bytes.iter().all(|byte| *byte == bytes[0]));
                    if let Some(index) = bad {
                        bytes[index] ^= 1;
                    }
                }
            }
        }
    }
}

fn publication_fixture(first_sequence: u64, through_sequence: u64) -> DetachedContainerWork {
    let bytes = vec![17; 16_384];
    DetachedContainerWork::new(
        InodeId::new(2).unwrap(),
        through_sequence,
        vec![PendingWriteThroughChunk {
            offset: 0,
            chunk_id: ChunkId::of(&bytes),
            bytes: ChunkFragments::new_through(
                vec![MutationPayload::from_owned_bytes(bytes)],
                16_384,
                first_sequence,
            ),
            placement: ContainerPlacement::Data,
        }],
        16_384,
    )
}

#[test]
fn publication_fence_waits_for_pre_cut_chunks_in_a_later_ending_batch() {
    let queue = Arc::new(PublicationQueue::new());
    let inode = InodeId::new(2).unwrap();
    queue.enqueue(publication_fixture(7, 36));
    let work = queue.next_work().unwrap();
    let waiter = Arc::clone(&queue);
    let (sender, receiver) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        waiter.wait_through(inode, 13);
        sender.send(()).unwrap();
    });
    let escaped = receiver.recv_timeout(Duration::from_millis(100)).is_ok();
    queue.finish(&work);
    thread.join().unwrap();
    assert!(
        !escaped,
        "the pre-cut Chunk recipe is not attached before retirement"
    );
    receiver.recv_timeout(Duration::from_secs(5)).unwrap();
}

#[test]
fn fixed_publication_fence_does_not_wait_for_later_arrivals() {
    let queue = PublicationQueue::new();
    let inode = InodeId::new(2).unwrap();
    queue.enqueue(publication_fixture(7, 36));
    let first = queue.next_work().unwrap();
    let target = queue.retirement_target(inode);
    queue.enqueue(publication_fixture(8, 37));
    let later = queue.next_work().unwrap();
    queue.finish(&first);
    queue.wait_for_retirement(inode, target);
    assert_eq!(queue.buffered_bytes(), 16_384);
    queue.finish(&later);
}

#[test]
fn pending_regions_gate_blocks_staging_until_a_release_path_frees_space() {
    let regions = super::PendingRegions::new();
    regions.reserve_staging_growth(super::INGEST_PENDING_GATE_BYTES_V1);
    regions.settle_staging(
        super::INGEST_PENDING_GATE_BYTES_V1,
        0,
        super::INGEST_PENDING_GATE_BYTES_V1,
    );
    let blocked = {
        let regions = regions.clone();
        std::thread::spawn(move || {
            regions.reserve_staging_growth(4096);
            regions.settle_staging(4096, 0, 0);
        })
    };
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        !blocked.is_finished(),
        "ASSERT: staging admission must block while the pending-region gate is full"
    );
    regions.release_lane_bytes(4096);
    blocked.join().unwrap();
}

#[test]
fn forced_checkpoint_gate_releases_a_saturated_staging_reservation() {
    let regions = super::PendingRegions::new();
    regions.reserve_staging_growth(super::INGEST_PENDING_GATE_BYTES_V1);
    regions.settle_staging(
        super::INGEST_PENDING_GATE_BYTES_V1,
        0,
        super::INGEST_PENDING_GATE_BYTES_V1,
    );
    let blocked = {
        let regions = regions.clone();
        std::thread::spawn(move || {
            regions.reserve_staging_growth(4096);
            regions.settle_staging(4096, 0, 0);
        })
    };
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        !blocked.is_finished(),
        "ASSERT: a transient checkpoint stall must not open the staging gate before the watchdog acts"
    );
    regions.force_checkpoint_staging();
    blocked.join().unwrap();
    assert_eq!(regions.forced_staging_batches(), 1);
    assert!(
        regions
            .checkpoint_staging_open
            .load(std::sync::atomic::Ordering::Acquire),
        "ASSERT: the watchdog release survives until checkpoint absorption clears it"
    );
    regions.clear_checkpoint_staging();
    assert!(
        !regions
            .checkpoint_staging_open
            .load(std::sync::atomic::Ordering::Acquire)
    );
}

#[test]
fn drain_residue_absorption_and_drop_release_their_gate_charge() {
    let regions = super::PendingRegions::new();
    regions.reserve_staging_growth(8192);
    regions.transfer_lane_to_residue(8192);
    assert_eq!(regions.residue_bytes(), 8192);
    let mut residue = super::DrainResidue {
        inode: InodeId::new(2).unwrap(),
        chunks: Vec::new(),
        charged_bytes: 8192,
        regions: regions.clone(),
    };
    residue.absorb_chunk(4096);
    assert_eq!(regions.residue_bytes(), 4096);
    drop(residue);
    assert_eq!(regions.residue_bytes(), 0);
}

fn budget_entry(ordinal: usize) -> ExactIndexEntry {
    let location =
        ExactIndexLocation::raw(ContainerId::new([29; 16]).unwrap(), 1, 4096, 256, 0).unwrap();
    ExactIndexEntry::active(ChunkId::of(&ordinal.to_le_bytes()), 8, location).unwrap()
}

#[test]
#[allow(clippy::too_many_lines)]
fn proof_budget_overflow_keeps_existing_proofs_and_verifies_uncached_dependencies() {
    struct RejectMissing;
    impl RequiredChunkVerifier for RejectMissing {
        fn verify_required_chunks(
            &self,
            required: &BTreeMap<ChunkId, u64>,
        ) -> Result<(), StoreError> {
            assert_eq!(required.len(), 1);
            let (&chunk_id, &logical_length) = required.first_key_value().unwrap();
            Err(StoreError::MissingVerifiedChunk {
                chunk_id,
                logical_length,
            })
        }
    }
    // This test requires a historical hit; supply deterministic capacity
    // instead of competing with concurrently running system-cache tests.
    let snapshot = fastdup_store::MemoryPressureSnapshot::new(1 << 30, 1 << 30, 0);
    let mut owned = OnlineDependencyProofs::new().unwrap();
    owned.historical = HistoricalProofCache::new_with_snapshot(
        crate::historical_proof_cache::HistoricalProofCacheConfig::conservative(snapshot),
        snapshot,
    )
    .unwrap();
    let proofs = Arc::new(owned);
    for ordinal in 0..MAX_ONLINE_DEPENDENCY_PROOFS_V1 / 2 {
        proofs.remember_active(budget_entry(ordinal), OnlineProofAdmission::Published);
    }
    assert!(proofs.freeze_for_commit());
    for ordinal in MAX_ONLINE_DEPENDENCY_PROOFS_V1 / 2..MAX_ONLINE_DEPENDENCY_PROOFS_V1 {
        proofs.remember_active(budget_entry(ordinal), OnlineProofAdmission::Published);
    }
    let overflow = budget_entry(MAX_ONLINE_DEPENDENCY_PROOFS_V1);
    proofs.remember_frozen(overflow, OnlineProofAdmission::ExactReuse);
    proofs.remember_active(overflow, OnlineProofAdmission::Published);
    assert_eq!(
        proofs.generation_status().active_proofs() + proofs.generation_status().frozen_proofs(),
        MAX_ONLINE_DEPENDENCY_PROOFS_V1
    );
    assert_eq!(proofs.verified_entry(overflow.chunk_id(), 8), None);
    let resident = budget_entry(0);
    assert_eq!(
        proofs.verified_entry(resident.chunk_id(), 8),
        Some(resident)
    );
    assert!(matches!(
        proofs.claim_publication(resident.chunk_id(), 8),
        PublicationClaim::Existing(_)
    ));
    assert_eq!(
        proofs.generation_status().active_proofs(),
        MAX_ONLINE_DEPENDENCY_PROOFS_V1 / 2
    );

    let verifier = OnlineSuccessorVerifier {
        proofs: Arc::clone(&proofs),
        fallback: Box::new(RejectMissing),
    };
    let required = BTreeMap::from([(resident.chunk_id(), 8), (overflow.chunk_id(), 8)]);
    assert!(
        matches!(verifier.verify_required_chunks(&required), Err(StoreError::MissingVerifiedChunk { chunk_id, .. }) if chunk_id == overflow.chunk_id())
    );
    // Exercise the real Container verifier, including a corrupt object,
    // while the dependency cannot be admitted into either proof set.
    let io = MemoryStorageIo::new();
    let containers = ContainerRepository::new(io.clone());
    let real_verifier = OnlineSuccessorVerifier {
        proofs: Arc::clone(&proofs),
        fallback: Box::new(containers.clone()),
    };
    assert!(real_verifier.verify_required_chunks(&required).is_err());
    let payload = MAX_ONLINE_DEPENDENCY_PROOFS_V1.to_le_bytes();
    containers
        .publish_raw(ContainerId::new([31; 16]).unwrap(), 1, &[&payload])
        .unwrap();
    real_verifier.verify_required_chunks(&required).unwrap();
    assert_eq!(
        proofs.generation_status().active_proofs() + proofs.generation_status().frozen_proofs(),
        MAX_ONLINE_DEPENDENCY_PROOFS_V1
    );
    let name = io.list_names().unwrap().pop().unwrap();
    io.write_at(&name, 0, b"BAD!").unwrap();
    assert!(real_verifier.verify_required_chunks(&required).is_err());

    // Even historical hits must remain safe when promotion has no capacity.
    proofs
        .historical
        .admit(overflow, HistoricalProofAdmission::ExactReuse);
    assert!(proofs.unproven(&required).is_empty());
    assert_eq!(
        proofs.verified_entry(overflow.chunk_id(), 8),
        Some(overflow)
    );
    assert_eq!(
        proofs.generation_status().active_proofs() + proofs.generation_status().frozen_proofs(),
        MAX_ONLINE_DEPENDENCY_PROOFS_V1
    );
    proofs.complete_frozen();
    proofs.remember_active(overflow, OnlineProofAdmission::Published);
    assert_eq!(
        proofs.generation_status().active_proofs(),
        MAX_ONLINE_DEPENDENCY_PROOFS_V1 / 2 + 1
    );
    assert!(proofs.freeze_for_commit());
    proofs.cancel_new_freeze(true);
    assert_eq!(
        proofs.verified_entry(overflow.chunk_id(), 8),
        Some(overflow)
    );
}

#[test]
fn proof_budget_overflow_releases_completed_publication_claims() {
    let proofs = OnlineDependencyProofs::new().unwrap();
    for ordinal in 0..MAX_ONLINE_DEPENDENCY_PROOFS_V1 {
        proofs.remember_active(budget_entry(ordinal), OnlineProofAdmission::Published);
    }
    assert!(proofs.freeze_for_commit());
    let entry = budget_entry(MAX_ONLINE_DEPENDENCY_PROOFS_V1);
    let key = (entry.chunk_id(), entry.logical_length());
    assert!(matches!(
        proofs.claim_publication(key.0, key.1),
        PublicationClaim::Acquired
    ));
    proofs.finish_publications(&[entry], &[key]);
    assert!(proofs.generation.lock().unwrap().publishing.is_empty());
    assert_eq!(proofs.generation_status().active_proofs(), 0);
    assert_eq!(
        proofs.generation_status().frozen_proofs(),
        MAX_ONLINE_DEPENDENCY_PROOFS_V1
    );
    assert!(matches!(
        proofs.claim_publication(key.0, key.1),
        PublicationClaim::Acquired
    ));
    proofs.abandon_publications(&[key]);
    proofs.complete_frozen();
    proofs.remember_active(entry, OnlineProofAdmission::Published);
    assert_eq!(proofs.verified_entry(key.0, u64::from(key.1)), Some(entry));
}

#[test]
fn idle_checkpoint_releases_reference_admission_from_completed_ingest() {
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(MemoryStorageIo::new(), checkpoint_policy_set()),
        ContainerRepository::new(MemoryStorageIo::new()),
        16,
    )
    .unwrap();
    let proofs = &appliance.online_dependency_proofs;
    let entry = budget_entry(0);
    proofs.remember_active(entry, OnlineProofAdmission::Published);
    proofs
        .reuse_location(
            appliance.manifest_readers.as_ref(),
            &appliance.containers,
            entry.chunk_id(),
            u64::from(entry.logical_length()),
            false,
        )
        .unwrap();
    let guard = Arc::downgrade(
        proofs
            .generation
            .lock()
            .unwrap()
            .active_references
            .as_ref()
            .unwrap(),
    );
    assert!(appliance.checkpoint().unwrap().is_none());
    assert!(
        guard.upgrade().is_none(),
        "idle commits release completed writer admission for GC"
    );
}

#[test]
fn generation_proof_admission_preserves_reuse_across_freeze() {
    let proofs = OnlineDependencyProofs::new().unwrap();
    let location =
        ExactIndexLocation::raw(ContainerId::new([29; 16]).unwrap(), 1, 4096, 256, 0).unwrap();
    let id = ChunkId::of(b"proof");
    let entry = ExactIndexEntry::active(id, 5, location).unwrap();
    proofs.remember_active(entry, OnlineProofAdmission::Published);
    assert_eq!(proofs.verified_entry(id, 5), Some(entry));
    assert!(proofs.freeze_for_commit());
    assert_eq!(proofs.verified_entry(id, 5), Some(entry));
    proofs.remember_active(entry, OnlineProofAdmission::ExactReuse);
    proofs.remember_active(entry, OnlineProofAdmission::Published);
    let state = proofs.generation.lock().unwrap();
    assert_eq!(state.active.len(), 1);
    assert_eq!(state.frozen.as_ref().unwrap().len(), 1);
    assert_eq!(
        state.active.get(&(id, 5)).unwrap().admission,
        HistoricalProofAdmission::ExactReuse
    );
    drop(state);
    assert_eq!(proofs.verified_entry(id, 6), None);
}

#[test]
fn generation_arena_checks_full_keys_and_survives_cancelled_freeze() {
    let proofs = OnlineDependencyProofs::new().unwrap();
    let location =
        ExactIndexLocation::raw(ContainerId::new([29; 16]).unwrap(), 1, 4096, 256, 0).unwrap();
    let mut first = [3_u8; 32];
    first[8] = 7;
    let mut second = first;
    second[8] = 8;
    let a = ExactIndexEntry::active(ChunkId::from_bytes(first), 32, location).unwrap();
    let b = ExactIndexEntry::active(ChunkId::from_bytes(second), 32, location).unwrap();
    assert_eq!(
        GenerationProofMap::hash((a.chunk_id(), 32)),
        GenerationProofMap::hash((b.chunk_id(), 32))
    );
    proofs.remember_active(a, OnlineProofAdmission::Published);
    proofs.remember_active(b, OnlineProofAdmission::Published);
    assert!(proofs.freeze_for_commit());
    assert_eq!(proofs.verified_entry(b.chunk_id(), 32), Some(b));
    proofs.remember_active(b, OnlineProofAdmission::ExactReuse);
    proofs.cancel_new_freeze(true);
    let state = proofs.generation.lock().unwrap();
    assert!(state.frozen.is_none());
    assert_eq!(state.active.len(), 2);
    assert_eq!(state.active.get(&(a.chunk_id(), 32)).unwrap().entry, a);
    assert_eq!(
        state.active.get(&(b.chunk_id(), 32)).unwrap().admission,
        HistoricalProofAdmission::ExactReuse
    );
    assert!(state.active.get(&(a.chunk_id(), 33)).is_none());
    drop(state);
    assert!(proofs.freeze_for_commit());
    let frozen = proofs.generation.lock().unwrap().frozen.take().unwrap();
    let ordered: Vec<_> = frozen
        .into_sorted_values()
        .map(|p| p.entry.chunk_id())
        .collect();
    assert_eq!(ordered, vec![a.chunk_id(), b.chunk_id()]);
    assert_eq!(proofs.generation_status().accounted_bytes(), 0);
}

#[test]
fn hash_granularity_preserves_fragmented_mixed_batches_and_partial_permits() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(10)
        .build()
        .unwrap();
    pool.install(|| {
        let permits = WorkerPermits::new(NonZeroUsize::new(10).unwrap());
        let mut batch = Vec::new();
        for ordinal in 0..128 {
            let mut bytes = vec![19; CDC_MAXIMUM_BYTES];
            if ordinal % 3 != 0 {
                let middle = bytes.len() / 2;
                bytes[middle] = 27;
            }
            let parts = bytes
                .chunks(17003)
                .map(|part| MutationPayload::from_owned_bytes(part.to_vec()))
                .collect();
            batch.push(StableChunk {
                offset: (ordinal * CDC_MAXIMUM_BYTES) as u64,
                bytes: ChunkFragments::new(parts, bytes.len()),
            });
        }
        let expected: Vec<_> = batch
            .iter()
            .map(|c| (!c.bytes.is_fill()).then(|| c.bytes.chunk_id()))
            .collect();
        let held = permits.acquire(NonZeroUsize::new(8).unwrap());
        let (actual, workers) = classify_stable_chunk_batch(
            &batch,
            NonZeroUsize::new(10).unwrap(),
            &permits,
            &CpuPhaseTelemetry::default(),
        )
        .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(workers, 2);
        assert_eq!(permits.available(), 2);
        drop(held);
        let fill: Vec<_> = batch.into_iter().step_by(3).collect();
        let (actual, workers) = classify_stable_chunk_batch(
            &fill,
            NonZeroUsize::new(10).unwrap(),
            &permits,
            &CpuPhaseTelemetry::default(),
        )
        .unwrap();
        assert!(actual.iter().all(Option::is_none));
        assert_eq!(workers, 1);
        assert_eq!(permits.available(), 10);
        assert_eq!(
            stable_hash_workers(32 * 1024 * 1024, 128, NonZeroUsize::new(10).unwrap()).get(),
            10
        );
    });
}

#[test]
#[should_panic(expected = "ordered and disjoint")]
fn pending_append_rejects_overlap_before_staging() {
    let mut pending = PendingWriteThrough::default();
    for offset in [10, 12] {
        pending
            .push(PendingWriteThroughChunk {
                offset,
                chunk_id: ChunkId::of(b"abc"),
                bytes: ChunkFragments::new(
                    vec![MutationPayload::from_owned_bytes(b"abc".to_vec())],
                    3,
                ),
                placement: ContainerPlacement::Data,
            })
            .unwrap();
    }
}

#[test]
#[should_panic(expected = "ordered and disjoint")]
fn detach_independently_rejects_reordered_chunks() {
    let chunks = [10, 5]
        .into_iter()
        .map(|offset| PendingWriteThroughChunk {
            offset,
            chunk_id: ChunkId::of(b"abc"),
            bytes: ChunkFragments::new(vec![MutationPayload::from_owned_bytes(b"abc".to_vec())], 3),
            placement: ContainerPlacement::Data,
        })
        .collect();
    DetachedContainerWork::new(InodeId::new(2).unwrap(), 1, chunks, 6);
}
use super::*;
use fastdup_format::ExactIndexLocation;
use fastdup_testkit::MemoryStorageIo;

#[test]
#[allow(clippy::too_many_lines)] // One fixture follows cold, warm, graph and Independent reuse.
fn ingest_exact_references_are_read_free_without_minting_payload_proofs() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let chunks = [vec![41; 32768], vec![43; 32768]];
    let publication = containers
        .publish_adaptive_regions_verified(
            ContainerId::new([0xc7; 16]).unwrap(),
            1,
            &[&[chunks[0].as_slice(), chunks[1].as_slice()]],
        )
        .unwrap();
    let entries: Vec<_> = publication
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        entries[0].location().record_offset(),
        entries[1].location().record_offset()
    );
    let index_storage = MemoryStorageIo::new();
    let indexes = ExactIndexRunRepository::new(index_storage.clone());
    let profile = checkpoint_exact_index_profile_v1();
    indexes.append_level_zero(profile, entries.clone()).unwrap();
    drop(indexes);
    let snapshot = fastdup_store::MemoryPressureSnapshot::new(1 << 30, 1 << 29, 0);
    let indexes = ExactIndexRunRepository::new_with_memory_snapshot(index_storage, snapshot);
    indexes.recover_active_generation().unwrap().unwrap();
    assert_eq!(indexes.page_cache_status().resident_pages(), 0);
    let core = Arc::new(ExactPublisherCore {
        repository: indexes,
        profile,
        degraded: AtomicBool::new(false),
        recent: RwLock::new(BTreeMap::new()),
        similarity: None,
        failed_reduction_guard: Mutex::new(None),
    });
    let policy = IndexedManifestReaders {
        publisher: ExactPublicationQueue::start(Arc::clone(&core)).unwrap(),
        core,
        read_cache: Arc::new(
            VerifiedReadCache::new_with_snapshot(
                fastdup_store::VerifiedReadCacheConfig::conservative(snapshot),
                snapshot,
            )
            .unwrap(),
        ),
        reduction: None,
    };
    let before = storage.operation_count();
    for entry in &entries {
        assert_eq!(
            <IndexedManifestReaders<_> as ManifestReaderPolicy<MemoryStorageIo>>::exact_location(
                &policy,
                entry.chunk_id(),
                32768,
                None,
            ),
            Some(*entry)
        );
    }
    assert_eq!(
        storage.operation_count(),
        before,
        "cold Exact selection reads no DATA"
    );
    assert_eq!(policy.read_cache.status().resident_bytes(), 0);
    assert_eq!(
        policy.read_cache.status().location_proofs().entries,
        0,
        "an index reference must not become physical verification evidence"
    );
    policy.core.remember_recent(&entries);
    assert_eq!(
        <IndexedManifestReaders<_> as ManifestReaderPolicy<MemoryStorageIo>>::exact_location(
            &policy,
            entries[0].chunk_id(),
            32768,
            None,
        ),
        Some(entries[0])
    );
    let required = entries
        .iter()
        .map(|entry| (entry.chunk_id(), 32768))
        .collect();
    policy
        .online_graph_verifier(containers.clone())
        .verify_required_chunks(&required)
        .unwrap();
    assert_eq!(
        storage.operation_count(),
        before,
        "Commit reference checking reads no DATA"
    );
    // Saturating required workspace must retain the same read-free fallback.
    let proofs = Arc::new(OnlineDependencyProofs::new().unwrap());
    for ordinal in 0..MAX_ONLINE_DEPENDENCY_PROOFS_V1 {
        proofs.remember_active(budget_entry(ordinal), OnlineProofAdmission::Published);
    }
    proofs.freeze_for_commit();
    assert_eq!(
        proofs.reuse_location(&policy, &containers, entries[0].chunk_id(), 32768, true),
        Some(entries[0])
    );
    assert!(
        proofs
            .generation
            .lock()
            .unwrap()
            .frozen_references
            .is_some(),
        "proof overflow must still retain GC admission through Commit"
    );
    let verifier = OnlineSuccessorVerifier {
        proofs: Arc::clone(&proofs),
        fallback: policy.online_graph_verifier(containers.clone()),
    };
    verifier.verify_required_chunks(&required).unwrap();
    assert_eq!(
        storage.operation_count(),
        before,
        "proof-set overflow reads no DATA"
    );
    proofs.cancel_new_freeze(true);
    assert!(
        proofs
            .generation
            .lock()
            .unwrap()
            .active_references
            .is_some(),
        "a cancelled cut returns DATA-reference admission to Active"
    );
    proofs.freeze_for_commit();
    assert!(
        !proofs.freeze_for_commit(),
        "a failed Commit retries its existing Frozen owner"
    );
    assert!(
        proofs
            .generation
            .lock()
            .unwrap()
            .frozen_references
            .is_some()
    );
    proofs.complete_frozen();
    assert!(
        proofs
            .generation
            .lock()
            .unwrap()
            .frozen_references
            .is_none()
    );
    assert!(
        containers
            .verify_location_cached(
                ExactIndexEntry::retiring(entries[0]).unwrap(),
                &policy.read_cache
            )
            .is_err()
    );
    let forged = ExactIndexEntry::active(
        entries[0].chunk_id(),
        entries[0].logical_length(),
        ExactIndexLocation::raw(ContainerId::new([0xc8; 16]).unwrap(), 2, 4096, 32960, 1234)
            .unwrap(),
    )
    .unwrap();
    assert!(
        containers
            .verify_location_cached(forged, &policy.read_cache)
            .is_err(),
        "matching logical content cannot authorize a different physical Location"
    );
    let independent = fastdup_store::ReadIntentScope::enter(fastdup_store::ReadIntent::Independent);
    let before = storage.operation_count();
    containers
        .verify_location_cached(entries[0], &policy.read_cache)
        .unwrap();
    assert!(
        storage.operation_count() > before,
        "independent verification must read storage"
    );
    let single_record_operations = storage.operation_count() - before;
    let before = storage.operation_count();
    policy
        .graph_verifier(containers.clone())
        .verify_required_chunks(
            &entries
                .iter()
                .map(|entry| (entry.chunk_id(), 32768))
                .collect(),
        )
        .unwrap();
    assert!(
        storage.operation_count() > before,
        "independent graph verification bypasses reuse"
    );
    assert_eq!(
        storage.operation_count() - before,
        single_record_operations,
        "independent graph verification must check sibling Chunks in one Record pass"
    );
    drop(independent);
    policy.core.recent.write().unwrap().clear();
    policy
        .core
        .repository
        .append_level_zero(profile, vec![forged])
        .unwrap();
    let before = storage.operation_count();
    assert_eq!(
        <IndexedManifestReaders<_> as ManifestReaderPolicy<MemoryStorageIo>>::exact_location(
            &policy,
            entries[0].chunk_id(),
            32768,
            Some(entries[0]),
        ),
        Some(entries[0])
    );
    assert_eq!(
        storage.operation_count(),
        before,
        "a later eligible warm Location precedes a cold alternative"
    );

    policy
        .read_cache
        .update_memory_pressure(fastdup_store::MemoryPressureSnapshot::new(1 << 30, 0, 0));
    let before = storage.operation_count();
    assert_eq!(
        <IndexedManifestReaders<_> as ManifestReaderPolicy<MemoryStorageIo>>::exact_location(
            &policy,
            entries[1].chunk_id(),
            32768,
            Some(entries[1]),
        ),
        Some(entries[1])
    );
    assert_eq!(
        storage.operation_count(),
        before,
        "cache pressure must not turn Exact references into payload reads"
    );
    policy.read_cache.update_memory_pressure(snapshot);
    let delayed_online_verifier = policy.online_graph_verifier(containers.clone());
    assert_eq!(
        <IndexedManifestReaders<_> as ManifestReaderPolicy<MemoryStorageIo>>::exact_location(
            &policy,
            entries[0].chunk_id(),
            32768,
            Some(entries[0]),
        ),
        Some(entries[0])
    );
    policy
        .core
        .repository
        .append_level_zero(
            profile,
            vec![
                ExactIndexEntry::retiring(entries[0]).unwrap(),
                ExactIndexEntry::retiring(forged).unwrap(),
            ],
        )
        .unwrap();
    assert_eq!(
        <IndexedManifestReaders<_> as ManifestReaderPolicy<MemoryStorageIo>>::exact_location(
            &policy,
            entries[0].chunk_id(),
            32768,
            Some(entries[0]),
        ),
        None,
        "cached evidence cannot override a newer RETIRING transition"
    );
    // Fresh physical validation must still detect damage while compact
    // evidence is resident. External mutation is deliberately injected.
    let name = format!("{}.fdc", "c7".repeat(16));
    let offset = entries[1].location().record_offset();
    let original = storage.read(&name).unwrap()[usize::try_from(offset).unwrap()];
    storage.write_at(&name, offset, &[original ^ 1]).unwrap();
    assert!(
        delayed_online_verifier
            .verify_required_chunks(&BTreeMap::from([(entries[0].chunk_id(), 32768)]))
            .is_err(),
        "Commit must refresh selection after a planned Location retires"
    );
    let _independent =
        fastdup_store::ReadIntentScope::enter(fastdup_store::ReadIntent::Independent);
    assert!(
        containers
            .verify_location_cached(entries[1], &policy.read_cache)
            .is_err()
    );
    assert!(
        policy
            .graph_verifier(containers.clone())
            .verify_required_chunks(&BTreeMap::from([(entries[1].chunk_id(), 32768)]))
            .is_err()
    );
}

#[test]
fn failed_exact_publication_retains_one_gc_guard_until_owner_teardown() {
    let containers = ContainerRepository::new(MemoryStorageIo::new());
    let core = Arc::new(ExactPublisherCore {
        repository: ExactIndexRunRepository::new(MemoryStorageIo::with_fail_before(0)),
        profile: checkpoint_exact_index_profile_v1(),
        degraded: AtomicBool::new(false),
        recent: RwLock::new(BTreeMap::new()),
        similarity: None,
        failed_reduction_guard: Mutex::new(None),
    });
    let queue = ExactPublicationQueue::start(Arc::clone(&core)).unwrap();
    let location =
        ExactIndexLocation::raw(ContainerId::new([7; 16]).unwrap(), 1, 4096, 256, 0).unwrap();
    let entry = ExactIndexEntry::active(ChunkId::of(b"target"), 6, location).unwrap();
    queue.publish(vec![entry], Vec::new(), containers.try_pin_data_reference());
    queue.flush();
    assert!(core.degraded.load(Ordering::Acquire));
    assert!(core.failed_reduction_guard.lock().unwrap().is_some());
    // A later successful Exact write must not release the earlier failed
    // target's protection. One retained guard remains a bounded safe stop.
    queue.publish(vec![entry], Vec::new(), containers.try_pin_data_reference());
    queue.flush();
    assert!(!core.degraded.load(Ordering::Acquire));
    assert!(core.failed_reduction_guard.lock().unwrap().is_some());
}

#[test]
fn completing_one_publication_batch_is_atomic_with_generation_freeze() {
    const ENTRY_COUNT: usize = 16_384;

    let proofs = Arc::new(OnlineDependencyProofs::new().expect("allocate proof sets"));
    let container_id = ContainerId::new([0xA7; 16]).expect("fixture ID is nonzero");
    let mut entries = Vec::with_capacity(ENTRY_COUNT);
    let mut claimed = Vec::with_capacity(ENTRY_COUNT);
    for ordinal in 0..ENTRY_COUNT {
        let mut chunk_bytes = [0_u8; 32];
        chunk_bytes[..8].copy_from_slice(
            &u64::try_from(ordinal + 1)
                .expect("fixture ordinal fits u64")
                .to_le_bytes(),
        );
        let chunk_id = ChunkId::from_bytes(chunk_bytes);
        let logical_length = 1_u32;
        let record_offset =
            4_096_u64 + u64::try_from(ordinal).expect("fixture ordinal fits u64") * 256;
        let location = ExactIndexLocation::raw(
            container_id,
            1,
            record_offset,
            256,
            u32::try_from(ordinal).expect("fixture ordinal fits u32"),
        )
        .expect("construct fixture RAW location");
        let entry = ExactIndexEntry::active(chunk_id, logical_length, location)
            .expect("construct fixture Exact entry");
        assert!(matches!(
            proofs.claim_publication(chunk_id, logical_length),
            PublicationClaim::Acquired
        ));
        entries.push(entry);
        claimed.push((chunk_id, logical_length));
    }

    let publisher_proofs = Arc::clone(&proofs);
    let publisher = std::thread::spawn(move || {
        publisher_proofs.finish_publications(&entries, &claimed);
    });

    let mut observed_partial_batch = false;
    loop {
        let state = proofs
            .generation
            .lock()
            .expect("fixture Generation Proof Set lock remains healthy");
        if state.active.len() != 0 && !state.publishing.is_empty() {
            observed_partial_batch = true;
            drop(state);
            assert!(proofs.freeze_for_commit());
            break;
        }
        if state.publishing.is_empty() {
            break;
        }
        drop(state);
        std::thread::yield_now();
    }

    assert!(
        publisher.join().is_ok(),
        "publication completion must not race"
    );
    assert!(
        !observed_partial_batch,
        "a Generation freeze observed only part of one published Container"
    );
}

#[test]
fn frozen_proof_waits_for_the_owned_publication_claim() {
    let proofs = Arc::new(OnlineDependencyProofs::new().expect("allocate proof sets"));
    let entry = budget_entry(42);
    let key = (entry.chunk_id(), entry.logical_length());
    assert!(matches!(
        proofs.claim_publication(key.0, key.1),
        PublicationClaim::Acquired
    ));
    assert!(proofs.freeze_for_commit());
    proofs.remember_frozen(entry, OnlineProofAdmission::Published);

    let waiting_proofs = Arc::clone(&proofs);
    let started = Arc::new(std::sync::Barrier::new(2));
    let waiting_started = Arc::clone(&started);
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let waiter = std::thread::spawn(move || {
        waiting_started.wait();
        let claim = waiting_proofs.claim_publication(key.0, key.1);
        sender.send(claim).expect("report the competing claim");
    });
    started.wait();
    let escaped = receiver.recv_timeout(Duration::from_millis(100)).ok();

    proofs.finish_publications(&[entry], &[key]);
    let claim = escaped.unwrap_or_else(|| {
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("the competing claim resumes after publication completion")
    });
    waiter
        .join()
        .expect("the competing claimant does not panic");

    assert!(
        escaped.is_none(),
        "a Frozen proof cannot overtake its in-flight publication owner"
    );
    assert!(matches!(claim, PublicationClaim::Existing(existing) if existing == entry));
}

#[test]
fn active_proof_admission_waits_for_an_owned_publication_claim() {
    let proofs = Arc::new(OnlineDependencyProofs::new().expect("allocate proof sets"));
    let entry = budget_entry(43);
    let key = (entry.chunk_id(), entry.logical_length());
    assert!(matches!(
        proofs.claim_publication(key.0, key.1),
        PublicationClaim::Acquired
    ));

    let waiting_proofs = Arc::clone(&proofs);
    let started = Arc::new(std::sync::Barrier::new(2));
    let waiting_started = Arc::clone(&started);
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let waiter = std::thread::spawn(move || {
        waiting_started.wait();
        waiting_proofs.remember_active(entry, OnlineProofAdmission::ExactReuse);
        sender.send(()).expect("report the Active proof admission");
    });
    started.wait();
    let escaped = receiver.recv_timeout(Duration::from_millis(100)).is_ok();

    proofs.abandon_publications(&[key]);
    if !escaped {
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("Active proof admission resumes after claim abandonment");
    }
    waiter
        .join()
        .expect("the Active proof admission does not panic");

    assert!(
        !escaped,
        "an Active proof cannot overtake its in-flight publication owner"
    );
    assert_eq!(proofs.verified_entry(key.0, u64::from(key.1)), Some(entry));
}

#[test]
fn publication_claims_match_grouped_encoder_locations_by_chunk_key() {
    let proofs = OnlineDependencyProofs::new().expect("allocate proof sets");
    let container_id = ContainerId::new([0xB8; 16]).expect("fixture ID is nonzero");
    let low_id = ChunkId::from_bytes([0x11; 32]);
    let high_id = ChunkId::from_bytes([0xEE; 32]);
    let logical_length = 1_u32;
    let low_location = ExactIndexLocation::raw(container_id, 1, 4_096, 256, 0)
        .expect("construct low fixture Location");
    let high_location = ExactIndexLocation::raw(container_id, 1, 8_192, 256, 1)
        .expect("construct high fixture Location");
    let low_entry = ExactIndexEntry::active(low_id, logical_length, low_location)
        .expect("construct low fixture Exact entry");
    let high_entry = ExactIndexEntry::active(high_id, logical_length, high_location)
        .expect("construct high fixture Exact entry");

    let mut claims = PublicationClaims::new(&proofs, 2).expect("allocate claims");
    assert!(matches!(
        claims.claim(low_id, logical_length),
        PublicationClaim::Acquired
    ));
    assert!(matches!(
        claims.claim(high_id, logical_length),
        PublicationClaim::Acquired
    ));

    // Advanced reduction groups ordinary, independent, and dependent
    // records before encoding, so writer Locations need not retain the
    // Chunk-ID order in which publication claims were acquired.
    let mut entries = [high_entry, low_entry];
    claims.finish(&mut entries);

    assert_eq!(
        proofs.verified_entry(low_id, u64::from(logical_length)),
        Some(low_entry)
    );
    assert_eq!(
        proofs.verified_entry(high_id, u64::from(logical_length)),
        Some(high_entry)
    );
}

#[test]
fn exact_externalization_consumes_existing_verification_without_second_container_read() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let container_id = ContainerId::new([0xD4; 16]).expect("fixture ID is nonzero");
    let payload = b"one verified exact Chunk must not be physically verified twice";
    containers
        .publish_raw(container_id, 23, &[payload])
        .expect("publish fixture Container");
    let sealed = containers
        .read(container_id)
        .expect("read rebuild evidence for fixture entry");
    let entry = ExactIndexEntry::from_verified_raw(sealed.raw_locations()[0])
        .expect("construct fixture Exact entry");
    let verified = containers
        .read_verified_location(entry)
        .expect("perform the one physical candidate verification");
    let after_verification = storage.operation_count();
    let external = VerifiedLocationFile {
        source: Arc::new(crate::ManifestCommittedFile::from_verified(
            VerifiedManifestFile::from_published_locations(&[entry], containers).unwrap(),
        )),
        source_offset: 0,
        entry,
    };

    assert!(
        external
            .matches_complete_bytes(&verified)
            .expect("match already verified bytes"),
        "the externalization proof must match the physically verified Chunk"
    );
    assert_eq!(
        storage.operation_count(),
        after_verification,
        "frontend externalization must not repeat Container envelope or Record I/O"
    );
}

#[test]
fn live_prefix_location_reads_before_target_index_activation() {
    let storage = MemoryStorageIo::new();
    let containers = ContainerRepository::new(storage.clone());
    let base = (0_u32..16384)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect::<Vec<_>>();
    let mut target = base.clone();
    target[17] ^= 0x5b;
    let base_id = ContainerId::new([0xe1; 16]).unwrap();
    containers.publish_raw(base_id, 1, &[&base]).unwrap();
    let base_entry =
        ExactIndexEntry::from_verified(containers.read(base_id).unwrap().locations()[0]).unwrap();
    let indexes = ExactIndexRunRepository::new(storage.clone());
    indexes
        .append_level_zero(
            ExactIndexProfileId::new([0xe3; 32]).unwrap(),
            vec![base_entry],
        )
        .unwrap();
    let active = indexes.pin_active_generation().unwrap();
    let publication = containers
        .publish_zstd_prefix_pairs_verified(
            ContainerId::new([0xe2; 16]).unwrap(),
            2,
            &[(&base, &target)],
        )
        .unwrap();
    let entry = ExactIndexEntry::from_verified(publication.locations()[0]).unwrap();
    let before = storage.operation_count();
    let reader = VerifiedManifestFile::from_published_locations(&[entry], containers)
        .unwrap()
        .with_active_index(&active);
    let external = VerifiedLocationFile {
        source: Arc::new(ManifestCommittedFile::from_verified(reader)),
        source_offset: 0,
        entry,
    };
    assert_eq!(
        storage.operation_count(),
        before,
        "live recipe construction cannot reread DATA"
    );
    assert_eq!(external.read_at(0, 16384).unwrap(), target);
    assert_eq!(
        external.read_shared_at(9, 53).unwrap().as_ref(),
        &target[9..62]
    );
}

#[test]
fn segmented_ingest_tail_materializes_only_the_selected_prefix() {
    let mut tail = SegmentedIngestTail::default();
    for (sequence, bytes) in [b"abc".as_slice(), b"defg".as_slice(), b"hijkl".as_slice()]
        .into_iter()
        .enumerate()
    {
        tail.push(
            MutationPayload::try_copy_from_slice(bytes).expect("allocate fixture segment"),
            u64::try_from(sequence + 1).expect("fixture sequence fits u64"),
        );
    }

    let consumed = tail.take_prefix(5).expect("consume compact prefix");
    assert_eq!(consumed.as_bytes(), b"abcde");
    assert_eq!(tail.len(), 7);
    assert_eq!(tail.materialized_bytes(), 5);
    let remaining = tail
        .take_prefix(7)
        .expect("consume remaining fixture bytes");
    assert_eq!(remaining.as_bytes(), b"fghijkl");
    assert_eq!(tail.materialized_bytes(), 12);
}

#[test]
fn inline_and_fragmented_prefixes_keep_owners_sequences_and_error_state() {
    let first = MutationPayload::try_copy_from_slice(b"abcdef").unwrap();
    let first_address = first.as_bytes().as_ptr();
    let mut tail = SegmentedIngestTail::default();
    tail.push(first, 9);
    tail.push(MutationPayload::try_copy_from_slice(b"ghij").unwrap(), 3);
    assert!(tail.take_prefix_fragments(11).is_err());
    assert_eq!(tail.len(), 10);
    tail.assert_valid();
    let prefix = tail.take_prefix_fragments(2).unwrap();
    assert!(matches!(prefix.parts, ChunkParts::Single(_)));
    assert_eq!(prefix.contiguous_bytes().unwrap().as_ptr(), first_address);
    assert_eq!(prefix.materialize_fixture(), b"ab");
    assert_eq!(prefix.through_sequence(), 9);
    let spanning = tail.take_prefix_fragments(5).unwrap();
    assert!(matches!(spanning.parts, ChunkParts::Fragmented(_)));
    assert_eq!(spanning.materialize_fixture(), b"cdefg");
    assert_eq!(spanning.through_sequence(), 9);
    let remaining = tail.take_prefix_fragments(3).unwrap();
    assert_eq!(remaining.materialize_fixture(), b"hij");
    assert_eq!(remaining.through_sequence(), 3);
    assert!(tail.is_empty());
    tail.assert_valid();
    drop(tail);
    assert_eq!(prefix.materialize_fixture(), b"ab");
    assert_eq!(spanning.materialize_fixture(), b"cdefg");
}

#[test]
#[should_panic(expected = "cached Ingest Tail length must match its segments")]
fn tail_boundary_audit_rejects_corrupted_cached_length() {
    let mut tail = SegmentedIngestTail::default();
    tail.push(MutationPayload::try_copy_from_slice(b"data").unwrap(), 1);
    tail.length += 1;
    tail.assert_valid();
}

#[test]
fn pathological_tiny_writes_compact_before_chunk_fragment_metadata_can_grow_unbounded() {
    let mut tail = SegmentedIngestTail::default();
    let length = MAX_CHUNK_FRAGMENTS_V1 + 1;
    for ordinal in 0..length {
        tail.push(
            MutationPayload::from_owned_bytes(vec![
                u8::try_from(ordinal % 251).expect("fixture byte fits u8"),
            ]),
            u64::try_from(ordinal + 1).expect("fixture sequence fits u64"),
        );
    }

    let chunk = tail
        .take_prefix_fragments(length)
        .expect("bounded fragment compaction succeeds");

    assert_eq!(chunk.parts.as_slice().len(), 1);
    assert_eq!(chunk.len(), length);
    assert_eq!(chunk.materialize_fixture().len(), length);
    assert_eq!(chunk.through_sequence(), u64::try_from(length).unwrap());
    assert_eq!(
        chunk.materialize_fixture(),
        (0..length)
            .map(|ordinal| u8::try_from(ordinal % 251).unwrap())
            .collect::<Vec<_>>()
    );
    assert!(tail.is_empty());
    tail.assert_valid();
}

#[test]
fn fragmented_chunks_materialize_directly_into_one_compression_region() {
    let first = ChunkFragments::new(
        vec![
            MutationPayload::try_copy_from_slice(b"fragmented ").expect("allocate fixture"),
            MutationPayload::try_copy_from_slice(b"first chunk").expect("allocate fixture"),
        ],
        22,
    );
    let second = ChunkFragments::new(
        vec![
            MutationPayload::try_copy_from_slice(b" and ").expect("allocate fixture"),
            MutationPayload::try_copy_from_slice(b"second").expect("allocate fixture"),
        ],
        11,
    );
    let chunks = [
        PendingWriteThroughChunk {
            offset: 0,
            chunk_id: first.chunk_id(),
            bytes: first,
            placement: ContainerPlacement::Data,
        },
        PendingWriteThroughChunk {
            offset: 22,
            chunk_id: second.chunk_id(),
            bytes: second,
            placement: ContainerPlacement::Data,
        },
    ];
    let references = chunks.iter().collect::<Vec<_>>();

    let regions = prepare_compression_regions(
        &references,
        NonZeroUsize::new(4).unwrap(),
        &WorkerPermits::new(NonZeroUsize::new(4).unwrap()),
    )
    .expect("prepare fixture regions");

    assert!(regions.borrowed.is_empty());
    assert_eq!(regions.materialized.len(), 1);
    assert_eq!(
        regions.materialized[0].decoded,
        b"fragmented first chunk and second"
    );
    assert_eq!(regions.materialized[0].chunks[0].1, 0..22);
    assert_eq!(regions.materialized[0].chunks[1].1, 22..33);
}

fn materialization_fixture(count: u8) -> Vec<PendingWriteThroughChunk> {
    (0..count)
        .map(|n| {
            let bytes = vec![n; 128 * 1024];
            let parts = if n < 4 {
                vec![MutationPayload::try_copy_from_slice(&bytes).unwrap()]
            } else {
                vec![
                    MutationPayload::try_copy_from_slice(&bytes[..17]).unwrap(),
                    MutationPayload::try_copy_from_slice(&bytes[17..]).unwrap(),
                ]
            };
            PendingWriteThroughChunk {
                offset: u64::from(n) * 128 * 1024,
                chunk_id: ChunkId::of(&bytes),
                bytes: ChunkFragments::new(parts, 128 * 1024),
                placement: ContainerPlacement::Data,
            }
        })
        .collect()
}

#[test]
fn parallel_materialization_preserves_regions_views_and_byte_order() {
    let chunks = materialization_fixture(32);
    let references = chunks.iter().collect::<Vec<_>>();
    let admission = WorkerPermits::new(NonZeroUsize::new(4).unwrap());
    let serial = prepare_compression_regions(&references, NonZeroUsize::MIN, &admission).unwrap();
    let parallel =
        prepare_compression_regions(&references, NonZeroUsize::new(4).unwrap(), &admission)
            .unwrap();
    assert_eq!(serial.borrowed.len(), 1);
    assert_eq!(serial.materialized.len(), 7);
    assert_eq!(serial.order.len(), parallel.order.len());
    for (left, right) in serial.order.iter().zip(&parallel.order) {
        assert_eq!(std::mem::discriminant(left), std::mem::discriminant(right));
    }
    for (left, right) in serial.materialized.iter().zip(&parallel.materialized) {
        assert_eq!(left.decoded, right.decoded);
        assert_eq!(left.chunks, right.chunks);
    }
    assert_eq!(admission.available(), 4);
}

#[test]
#[ignore = "manual release-mode Compression Region materialization A/B"]
fn parallel_materialization_microbenchmark() {
    let chunks = materialization_fixture(255);
    let references = chunks.iter().collect::<Vec<_>>();
    let admission = WorkerPermits::new(NonZeroUsize::new(8).unwrap());
    let mut samples = [Vec::new(), Vec::new()];
    for round in 0..11 {
        for side in 0..2 {
            let side = (side + round) % 2;
            let workers = NonZeroUsize::new(if side == 0 { 1 } else { 8 }).unwrap();
            let start = Instant::now();
            std::hint::black_box(
                prepare_compression_regions(&references, workers, &admission).unwrap(),
            );
            samples[side].push(start.elapsed());
        }
    }
    for samples in &mut samples {
        samples.sort_unstable();
    }
    println!(
        "region_materialization serial_ms={:.3} parallel_ms={:.3} speedup={:.3}",
        samples[0][5].as_secs_f64() * 1000.0,
        samples[1][5].as_secs_f64() * 1000.0,
        samples[0][5].as_secs_f64() / samples[1][5].as_secs_f64()
    );
}

#[test]
fn independent_ingest_lane_state_starts_on_cache_lines() {
    assert_eq!(std::mem::align_of::<WriteThroughStream>(), 64);
}

#[test]
fn segmented_seqcdc_matches_contiguous_v1_boundaries() {
    let mut state = 0x5eed_cafe_1234_5678_u64;
    let mut source = vec![0_u8; 4 * 1_024 * 1_024 + 91_337];
    for byte in &mut source {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }
    let mut expected = Vec::new();
    let mut expected_offset = 0_usize;
    while source.len() - expected_offset > CDC_MAXIMUM_BYTES {
        let length = seqcdc_cut(&source[expected_offset..], SEQCDC_CONFIG_V1);
        if length > source.len() - expected_offset - CDC_MAXIMUM_BYTES {
            break;
        }
        expected.push(source[expected_offset..expected_offset + length].to_vec());
        expected_offset += length;
    }

    let mut tail = SegmentedIngestTail::default();
    let segment_sizes = [1, 4_095, 131_071, 17, 1_048_576, 65_537];
    let mut cursor = 0_usize;
    let mut ordinal = 0_usize;
    while cursor < source.len() {
        let end = cursor
            .saturating_add(segment_sizes[ordinal % segment_sizes.len()])
            .min(source.len());
        tail.push(
            MutationPayload::try_copy_from_slice(&source[cursor..end])
                .expect("allocate segmented fixture"),
            u64::try_from(ordinal + 1).expect("fixture sequence fits u64"),
        );
        cursor = end;
        ordinal += 1;
    }
    let mut observed = Vec::new();
    while let Some(chunk) =
        take_next_stable_seqcdc_chunk(&mut tail).expect("chunk segmented fixture")
    {
        observed.push(chunk.materialize_fixture());
    }

    assert_eq!(observed, expected);
    assert_eq!(tail.materialized_bytes(), 0);
    assert_eq!(
        tail.len(),
        source.len() - expected.iter().map(Vec::len).sum::<usize>()
    );

    let mut fragmented = SegmentedIngestTail::default();
    for (sequence, bytes) in source.chunks(4_096).enumerate() {
        fragmented.push(
            MutationPayload::try_copy_from_slice(bytes).expect("allocate fragmented fixture"),
            u64::try_from(sequence + 1).expect("fixture sequence fits u64"),
        );
    }
    let mut selected_bytes = 0_usize;
    while let Some(chunk) =
        take_next_stable_seqcdc_chunk(&mut fragmented).expect("chunk fragmented fixture")
    {
        selected_bytes = selected_bytes
            .checked_add(chunk.len())
            .expect("fixture byte count cannot overflow");
    }
    assert_ne!(
        selected_bytes, 0,
        "fragmented fixture must expose stable Chunks"
    );
    assert_eq!(
        fragmented.materialized_bytes(),
        0,
        "SeqCDC extraction and hashing retain request fragments without copying"
    );
}

#[test]
fn streaming_seqcdc_matches_contiguous_v1_boundaries() {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut source = vec![0_u8; 4 * 1_024 * 1_024 + 73_019];
    for byte in &mut source {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }

    let mut stream = SeqCdcStream::new(std::io::Cursor::new(source.as_slice()))
        .expect("allocate SeqCDC stream fixture");
    let mut offset = 0_usize;
    while let Some(observed) = stream.next_chunk().expect("scan SeqCDC stream fixture") {
        let expected_length = seqcdc_cut(&source[offset..], SEQCDC_CONFIG_V1);
        assert_eq!(observed, source[offset..offset + expected_length]);
        offset += expected_length;
    }
    assert_eq!(offset, source.len());
    assert_eq!(stream.consumed_bytes(), source.len() as u64);
}

#[test]
fn full_registry_never_evicts_an_in_flight_ingest_lane() {
    let mut registry = WriteThroughRegistry::default();
    let mut held = Vec::new();
    for raw_inode in
        2..u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 + 2).expect("fixture lane bound fits u64")
    {
        let inode = InodeId::new(raw_inode).expect("fixture inode is nonzero");
        held.push(registry.acquire_lane(inode).0);
    }
    assert_eq!(registry.lanes.len(), MAX_ACTIVE_INGEST_LANES_V1);

    let overflow_inode =
        u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 + 2).expect("fixture overflow inode fits u64");
    let overflow = registry
        .acquire_lane(InodeId::new(overflow_inode).expect("fixture overflow inode is nonzero"))
        .0;
    assert!(Arc::ptr_eq(&overflow, &registry.overflow));
    assert_eq!(registry.lanes.len(), MAX_ACTIVE_INGEST_LANES_V1);

    for raw_inode in (overflow_inode + 1)
        ..(overflow_inode
            + u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 * 2 + 2).expect("fixture grace fits u64"))
    {
        let candidate = registry
            .acquire_lane(InodeId::new(raw_inode).expect("fixture inode is nonzero"))
            .0;
        assert!(Arc::ptr_eq(&candidate, &registry.overflow));
    }
    drop(held.remove(0));
    let replacement = registry.acquire_lane(
        InodeId::new(
            overflow_inode
                + u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 * 2 + 2)
                    .expect("fixture grace fits u64"),
        )
        .expect("fixture replacement inode is nonzero"),
    );
    let (replacement, evicted) = replacement;
    assert!(
        evicted.is_some(),
        "replacing a released Lane must report the evicted stream so its bytes can be released"
    );
    assert!(!Arc::ptr_eq(&replacement, &registry.overflow));
    assert_eq!(registry.lanes.len(), MAX_ACTIVE_INGEST_LANES_V1);
    assert!(
        !registry
            .lanes
            .contains_key(&InodeId::new(2).expect("fixture inode is nonzero"))
    );
}

#[test]
fn cpu_admission_uses_available_workers_during_ingest_io() {
    let budget = NonZeroUsize::new(10).unwrap();
    let permits = WorkerPermits::new(budget);
    let active = AtomicUsize::new(0);
    let _io_job = ActiveWriteThrough::enter(&active);
    let cpu = permits.acquire(budget);
    assert_eq!(cpu.workers(), budget);
    assert_eq!(permits.available(), 0);
    drop(cpu);
    assert_eq!(permits.available(), 10);
}

#[test]
fn ingest_mode_transition_drains_older_batch_before_unbatched_write() {
    let queue = IngestQueue::new();
    let a = InodeId::new(2).unwrap();
    let b = InodeId::new(3).unwrap();
    let fragment = |sequence, offset| IngestWriteFragment {
        offset,
        bytes: MutationPayload::from_owned_bytes(vec![u8::try_from(sequence).unwrap(); 4096]),
        mutation_sequence: sequence,
        placement: ContainerPlacement::Data,
    };
    // One inode leaves a partial batch. A second inode changes admission
    // mode before that batch has been consumed.
    queue.enqueue_write_fragment(a, fragment(1, 0));
    queue.enqueue_write_fragment(b, fragment(1, 0));
    queue.enqueue_write_fragment(a, fragment(2, 4096));
    queue.shutdown();
    let mut sequences = Vec::new();
    while let Some(job) = queue.next_job() {
        if job.inode == a {
            sequences.push(job.mutation_sequence);
        }
        queue.finish(&job);
    }
    assert_eq!(sequences, vec![1, 2]);
    assert_eq!(queue.status().buffered_bytes, 0);
    queue.wait_through(a, 2);
}

#[test]
fn ingest_mode_transition_during_backpressure_preserves_handle_order() {
    let queue = Arc::new(IngestQueue::new());
    let a = InodeId::new(2).unwrap();
    let b = InodeId::new(3).unwrap();
    queue.opened_write_handle(a);
    let payload = MutationPayload::from_owned_bytes(vec![7; 1024 * 1024]);
    for sequence in 1..=32 {
        queue.enqueue_write_fragment(
            a,
            IngestWriteFragment {
                offset: (sequence - 1) * 1024 * 1024,
                bytes: payload.clone(),
                mutation_sequence: sequence,
                placement: ContainerPlacement::Data,
            },
        );
    }
    let (entered, waiting) = mpsc::channel();
    *queue.before_fragment_wait.lock().unwrap() = Some(entered);
    let writer_queue = Arc::clone(&queue);
    let writer = std::thread::spawn(move || {
        writer_queue.enqueue_write_fragment(
            a,
            IngestWriteFragment {
                offset: 32 * 1024 * 1024,
                bytes: MutationPayload::from_owned_bytes(vec![8; 4096]),
                mutation_sequence: 33,
                placement: ContainerPlacement::Data,
            },
        );
    });
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    // The waiting writer selected batching with one writable inode. A
    // second handle changes the mode while its admission lock is released.
    queue.opened_write_handle(b);
    let first = queue.next_job().unwrap();
    queue.finish(&first);
    writer.join().unwrap();
    // Hold the batch's age below the expiry threshold deterministically.
    queue
        .state
        .lock()
        .unwrap()
        .inodes
        .get_mut(&a)
        .unwrap()
        .open
        .as_mut()
        .unwrap()
        .opened_at = Instant::now() + Duration::from_mins(1);
    for _ in 0..4 {
        let job = queue.next_job().unwrap();
        queue.finish(&job);
    }
    queue.enqueue_write_fragment(
        a,
        IngestWriteFragment {
            offset: 32 * 1024 * 1024 + 4096,
            bytes: MutationPayload::from_owned_bytes(vec![9; 4096]),
            mutation_sequence: 34,
            placement: ContainerPlacement::Data,
        },
    );
    queue.shutdown();
    let mut sequences = Vec::new();
    while let Some(job) = queue.next_job() {
        sequences.push(job.mutation_sequence);
        queue.finish(&job);
    }
    assert_eq!(sequences, vec![24, 28, 32, 33, 34]);
    queue.wait_through(a, 34);
    assert_eq!(queue.status().buffered_bytes, 0);
    queue.released_write_handle(a);
    queue.released_write_handle(b);
}

#[test]
#[ignore = "manual optimized queue admission A/B benchmark"]
fn ingest_queue_admission_benchmark() {
    let payload = MutationPayload::from_owned_bytes(vec![7; 1024 * 1024]);
    for mode in ["single", "multi"] {
        for round in 0..7 {
            let queue = IngestQueue::new();
            let a = InodeId::new(2).unwrap();
            let b = InodeId::new(3).unwrap();
            queue.opened_write_handle(a);
            if mode == "multi" {
                queue.opened_write_handle(b);
            }
            let started = Instant::now();
            let jobs = 200_000_u64;
            let per_job = if mode == "single" { 4 } else { 1 };
            for job in 0..jobs {
                let inode = if mode == "multi" && job % 2 == 1 {
                    b
                } else {
                    a
                };
                for part in 0..per_job {
                    let sequence = job * per_job + part + 1;
                    queue.enqueue_write_fragment(
                        inode,
                        IngestWriteFragment {
                            offset: sequence * 1024 * 1024,
                            bytes: std::hint::black_box(payload.clone()),
                            mutation_sequence: sequence,
                            placement: ContainerPlacement::Data,
                        },
                    );
                }
                let work = queue.next_job().unwrap();
                queue.finish(&work);
            }
            println!(
                "queue_bench mode={mode} round={round} fragments={} elapsed_ns={}",
                jobs * per_job,
                started.elapsed().as_nanos()
            );
            assert_eq!(queue.status().buffered_bytes, 0);
        }
    }
}

#[test]
fn ingest_queue_preserves_per_inode_sequence_order() {
    let queue = IngestQueue::new();
    let inode = InodeId::new(2).expect("fixture inode is nonzero");
    queue.enqueue_write_fragment(
        inode,
        IngestWriteFragment {
            offset: 0,
            bytes: MutationPayload::try_copy_from_slice(&[1]).expect("allocate fixture payload"),
            mutation_sequence: 7,
            placement: ContainerPlacement::Data,
        },
    );
    queue.enqueue(IngestJob {
        inode,
        mutation_sequence: 8,
        kind: IngestJobKind::Truncate,
    });

    let first = queue.next_job().expect("first queued job exists");
    assert_eq!(first.mutation_sequence, 7);
    queue.finish(&first);
    let second = queue.next_job().expect("second queued job exists");
    assert_eq!(second.mutation_sequence, 8);
    queue.finish(&second);
    queue.wait_through(inode, 8);
}

#[test]
fn ingest_queue_wait_ignores_metadata_only_sequence_gaps() {
    let queue = Arc::new(IngestQueue::new());
    let inode = InodeId::new(2).expect("fixture inode is nonzero");
    queue.enqueue_write_fragment(
        inode,
        IngestWriteFragment {
            offset: 0,
            bytes: MutationPayload::try_copy_from_slice(&[1]).expect("allocate fixture payload"),
            mutation_sequence: 1,
            placement: ContainerPlacement::Data,
        },
    );
    let write = queue.next_job().expect("queued write exists");
    queue.finish(&write);

    let waiter = Arc::clone(&queue);
    let (completed, receive) = mpsc::channel();
    std::thread::spawn(move || {
        waiter.wait_through(inode, 2);
        completed.send(()).expect("test receiver remains live");
    });
    receive
        .recv_timeout(Duration::from_millis(100))
        .expect("Metadata-only sequence gaps have no missing ingest work");
}

#[test]
#[should_panic(expected = "per-inode ingest admission sequence cannot move backwards")]
fn ingest_queue_asserts_on_decreasing_inode_sequence() {
    let queue = IngestQueue::new();
    let inode = InodeId::new(2).expect("fixture inode is nonzero");
    for mutation_sequence in [8, 7] {
        queue.enqueue(IngestJob {
            inode,
            mutation_sequence,
            kind: IngestJobKind::Truncate,
        });
    }
}

#[test]
fn encode_worker_permits_cannot_overbook_the_write_through_budget() {
    let budget = NonZeroUsize::new(10).expect("fixture budget is nonzero");
    let permits = WorkerPermits::new(budget);
    let telemetry = CpuPhaseTelemetry::default();
    let first = permits.acquire(NonZeroUsize::new(6).expect("fixture share is nonzero"));
    telemetry.record_permit(&first);
    let first_phase = telemetry.begin();
    let second = permits.acquire(NonZeroUsize::new(10).expect("fixture share is nonzero"));
    telemetry.record_permit(&second);
    let second_phase = telemetry.begin();
    assert_eq!(first.workers().get(), 6);
    assert_eq!(second.workers().get(), 4);
    assert_eq!(first.requested_workers().get(), 6);
    assert_eq!(second.requested_workers().get(), 10);
    assert!(!first.blocked());
    assert!(!second.blocked());
    let active = telemetry.status();
    assert_eq!(active.phases(), 2);
    assert_eq!(active.active(), 2);
    assert_eq!(active.maximum_active(), 2);
    assert_eq!(active.requested_workers(), 16);
    assert_eq!(active.granted_workers(), 10);
    assert_eq!(active.partial_grants(), 1);
    assert_eq!(active.permit_blocked_phases(), 0);
    assert_eq!(permits.available(), 0);
    drop(second_phase);
    drop(first_phase);
    drop(second);
    drop(first);
    let completed = telemetry.status();
    assert_eq!(completed.active(), 0);
    assert!(completed.runnable_wall_ns() > 0);
    assert!(completed.maximum_permit_wait_ns() <= completed.permit_wait_ns());
    assert_eq!(permits.available(), budget.get());
}

#[test]
#[should_panic(expected = "requested encode workers exceed the write-through worker budget")]
fn encode_worker_permits_assert_on_an_impossible_request() {
    let permits = WorkerPermits::new(NonZeroUsize::new(10).expect("fixture is nonzero"));
    let _lease = permits.acquire(NonZeroUsize::new(11).expect("fixture is nonzero"));
}

#[test]
#[should_panic(expected = "pending write-through byte accounting must be exact")]
fn pending_write_through_accounting_asserts_at_the_planner_boundary() {
    let chunks = vec![PendingWriteThroughChunk {
        offset: 0,
        chunk_id: ChunkId::of(&[1, 2, 3]),
        bytes: ChunkFragments::new(vec![MutationPayload::from_owned_bytes(vec![1, 2, 3])], 3),
        placement: ContainerPlacement::Data,
    }];
    DetachedContainerWork::new(InodeId::new(2).unwrap(), 1, chunks, 2);
}

#[test]
#[should_panic(expected = "one Ingest Lane exceeded one Container plus CDC suffix")]
fn stable_lane_asserts_on_an_impossible_buffer_overshoot() {
    let mut tail = SegmentedIngestTail::default();
    tail.push(
        MutationPayload::try_copy_from_slice(&vec![
            0_u8;
            CONTAINER_PAYLOAD_TARGET_BYTES
                + CDC_MAXIMUM_BYTES
                + 1
        ])
        .expect("allocate oversized fixture payload"),
        1,
    );
    let state = WriteThroughStream {
        tail,
        ..WriteThroughStream::default()
    };
    assert_bounded_write_through_lane(&state);
}
