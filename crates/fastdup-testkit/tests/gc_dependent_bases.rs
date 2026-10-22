//! DATA GC keeps the Base of every stored dependent record (ADR 0105).
//!
//! Every fixture uses codec 5 (`ZSTD_PREFIX_FLEX`): the Base length differs
//! from the dependent target, so a Base protected under the target's length
//! would be indistinguishable from a missing one.
use fastdup_format::{
    ChunkId, ContainerId, DurableInode, ExactIndexProfileId, GcCandidateCatalogRow, ManifestExtent,
    ManifestLeaf, NamespaceEntry, NamespaceRoot, PolicySetId, SealedContainer,
};
use fastdup_store::{
    ContainerGenerationAllocator, ContainerRepository, DataPoolUsage, DependentBasePinCollector,
    ExactIndexRunRepository, GcCandidateCatalogRepository, GcCandidateSelectionMode,
    GenerationRepository, MaintenanceRepository, ScrubCoverage,
};
use fastdup_testkit::MemoryStorageIo;

struct Fixture {
    metadata: MemoryStorageIo,
    generations: GenerationRepository<MemoryStorageIo>,
    containers: ContainerRepository<MemoryStorageIo>,
    indexes: ExactIndexRunRepository<MemoryStorageIo>,
    profile: ExactIndexProfileId,
    allocator: ContainerGenerationAllocator<MemoryStorageIo>,
}

impl Fixture {
    fn new() -> Self {
        let metadata = MemoryStorageIo::new();
        let generations = GenerationRepository::new(
            metadata.clone(),
            PolicySetId::new([0x91; 32]).expect("policy ID is nonzero"),
        );
        generations
            .commit_namespace(
                &NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
                    .expect("reservation root is valid"),
            )
            .expect("reserve inode range before visibility");
        let containers = ContainerRepository::new(MemoryStorageIo::new());
        Self {
            indexes: ExactIndexRunRepository::new(metadata.clone()),
            metadata,
            generations,
            allocator: containers
                .open_generation_allocator(1_024)
                .expect("initialize Container generation state"),
            containers,
            profile: ExactIndexProfileId::new([0x92; 32]).expect("profile ID is nonzero"),
        }
    }

    /// Publishes RAW chunks and returns the Container generation.
    fn publish_raw(&self, id: u8, chunks: &[&[u8]]) -> u64 {
        let generation = self
            .allocator
            .reserve_generation()
            .expect("reserve generation");
        self.containers
            .publish_raw(container(id), generation, chunks)
            .expect("publish RAW fixture Container");
        generation
    }

    /// Publishes dependent records and returns the Container generation.
    fn publish_dependents(&self, id: u8, pairs: &[(&[u8], &[u8])]) -> u64 {
        let generation = self
            .allocator
            .reserve_generation()
            .expect("reserve generation");
        self.containers
            .publish_zstd_prefix_pairs_verified(container(id), generation, pairs)
            .expect("publish dependent fixture Container");
        generation
    }

    /// Commits one regular file per payload, each one DATA extent, and returns
    /// the latest Commit generation.
    fn commit_files(&self, files: &[&[u8]], sequence: u64) -> u64 {
        let mut inodes = Vec::new();
        let mut entries = Vec::new();
        for (ordinal, payload) in files.iter().enumerate() {
            let inode = u64::try_from(ordinal).expect("ordinal fits u64") + 2;
            let length = u64::try_from(payload.len()).expect("length fits u64");
            let manifest = ManifestLeaf::new(
                length,
                vec![ManifestExtent::Data {
                    logical_length: length,
                    chunk_id: ChunkId::of(payload),
                }],
            )
            .expect("Manifest is valid");
            let root = self
                .generations
                .publish_manifest(&manifest)
                .expect("publish Manifest");
            inodes.push(
                DurableInode::new(inode, 0o640, 1_000, 1_000, 1, sequence, length, root)
                    .expect("inode is valid"),
            );
            entries.push(
                NamespaceEntry::new(1, inode, format!("file-{ordinal}.vbk").into_bytes())
                    .expect("entry is valid"),
            );
        }
        let namespace = NamespaceRoot::new(1_024, 8, sequence, inodes, entries)
            .expect("Namespace Root is valid");
        // Two commits drain the previous generation out of online liveness.
        let mut committed = 0;
        for _ in 0..2 {
            committed = self
                .generations
                .commit_namespace_with_data(&namespace, &self.containers)
                .expect("commit DATA generation")
                .generation();
        }
        committed
    }

    fn maintenance(
        &self,
    ) -> MaintenanceRepository<MemoryStorageIo, MemoryStorageIo, MemoryStorageIo> {
        let maintenance = MaintenanceRepository::new(
            self.generations.clone(),
            self.containers.clone(),
            self.indexes.clone(),
            self.profile,
        );
        maintenance
            .rebuild_exact_index()
            .expect("activate Exact coverage of every stored record");
        maintenance
    }

    fn assert_readable(&self, payload: &[u8]) {
        let active = self
            .indexes
            .recover_active()
            .expect("recover active Exact generation")
            .expect("an Exact generation is active");
        assert_eq!(
            self.containers
                .read_verified_chunk_with_index(
                    &active,
                    ChunkId::of(payload),
                    u64::try_from(payload.len()).expect("length fits u64"),
                )
                .expect("dependent target remains readable after GC"),
            payload
        );
    }
}

fn container(id: u8) -> ContainerId {
    ContainerId::new([id; 16]).expect("fixture Container ID is nonzero")
}

/// A 4,107-byte Base and a 4,625-byte target that shares most of it.
fn base_and_target(seed: u8) -> (Vec<u8>, Vec<u8>) {
    let base = (0..4_107_u32)
        .map(|index| u8::try_from((index * 31 + u32::from(seed) * 7) % 251).expect("byte"))
        .collect::<Vec<_>>();
    let mut target = base.clone();
    target[100] ^= 0x5a;
    target.extend(std::iter::repeat_n(seed, 518));
    (base, target)
}

fn candidate_row(id: u8, generation: u64, chunks: &[&[u8]]) -> GcCandidateCatalogRow {
    row_from(
        SealedContainer::encode_with_writer_evidence(container(id), generation, chunks)
            .expect("fixture Container encodes")
            .into_publication_parts(),
        id,
        generation,
    )
}

fn dependent_row(id: u8, generation: u64, pairs: &[(&[u8], &[u8])]) -> GcCandidateCatalogRow {
    row_from(
        SealedContainer::encode_zstd_prefix_pairs(container(id), generation, pairs)
            .expect("fixture dependent Container encodes")
            .into_publication_parts(),
        id,
        generation,
    )
}

fn row_from(
    (image, publication): (Vec<u8>, fastdup_format::VerifiedContainerPublication),
    id: u8,
    generation: u64,
) -> GcCandidateCatalogRow {
    GcCandidateCatalogRow::from_intrinsic_summary(
        container(id),
        generation,
        u64::try_from(image.len()).expect("length fits u64"),
        publication
            .intrinsic_summary()
            .expect("publication reconstructs intrinsic summary"),
    )
    .expect("candidate row is valid")
}

#[test]
fn offline_gc_keeps_a_base_that_only_a_live_dependent_names() {
    let fixture = Fixture::new();
    let (base, target) = base_and_target(1);
    fixture.publish_raw(0x10, &[&base]);
    fixture.publish_dependents(0x11, &[(&base, &target)]);
    fixture.commit_files(&[&target], 1);
    let maintenance = fixture.maintenance();

    let plan = maintenance
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("pool usage is valid"))
        .expect("scrub the committed graph and its Base closure");
    maintenance
        .garbage_collect(plan)
        .expect("collect unreachable Containers");

    fixture.assert_readable(&target);
    maintenance
        .scrub()
        .expect("every stored dependent still resolves its Base");
}

#[test]
fn offline_gc_keeps_the_base_of_a_dead_dependent_that_stays_stored() {
    let fixture = Fixture::new();
    let (live_base, live_target) = base_and_target(2);
    let (dead_base, dead_target) = base_and_target(3);
    fixture.publish_raw(0x20, &[&live_base]);
    fixture.publish_raw(0x21, &[&dead_base]);
    // The live target keeps this Container; its dead neighbour stays stored.
    fixture.publish_dependents(
        0x22,
        &[(&live_base, &live_target), (&dead_base, &dead_target)],
    );
    fixture.commit_files(&[&live_target], 1);
    let maintenance = fixture.maintenance();

    let plan = maintenance
        .scrub_for_gc(DataPoolUsage::new(50, 100).expect("pool usage is valid"))
        .expect("scrub the committed graph and its Base closure");
    maintenance
        .garbage_collect(plan)
        .expect("collect unreachable Containers");

    fixture.assert_readable(&live_target);
    maintenance
        .scrub()
        .expect("a stored dead dependent must not lose its Base");
}

type Maintenance = MaintenanceRepository<MemoryStorageIo, MemoryStorageIo, MemoryStorageIo>;

fn prove(
    fixture: &Fixture,
    committed: u64,
    rows: Vec<GcCandidateCatalogRow>,
) -> (Maintenance, fastdup_store::GcCandidateProof) {
    let maintenance = fixture.maintenance();
    let proof = prove_with(&maintenance, fixture, 1, committed, rows);
    (maintenance, proof)
}

fn prove_with(
    maintenance: &Maintenance,
    fixture: &Fixture,
    catalog_generation: u64,
    committed: u64,
    rows: Vec<GcCandidateCatalogRow>,
) -> fastdup_store::GcCandidateProof {
    let catalog = GcCandidateCatalogRepository::new(fixture.metadata.clone());
    let row_count = u64::try_from(rows.len()).expect("row count fits u64");
    catalog
        .publish_rows(catalog_generation, committed, 0, row_count, rows)
        .expect("publish candidate catalog");
    let snapshot = catalog
        .recover_latest()
        .expect("recover candidate catalog")
        .expect("candidate catalog exists");
    let shortlist = snapshot
        .shortlist(GcCandidateSelectionMode::Urgent, 4, 4)
        .expect("select every candidate row");
    maintenance
        .prove_gc_candidates(
            &shortlist,
            DataPoolUsage::new(50, 100).expect("pool usage is valid"),
        )
        .expect("prove the victim set with its dependent Bases")
}

/// Scrubs every published Container the way the startup scrub does and
/// collects the Bases of dependents absent from the active Exact generation.
fn scrub_pins(fixture: &Fixture) -> DependentBasePinCollector {
    let active = fixture
        .indexes
        .recover_active()
        .expect("recover active Exact generation")
        .expect("an Exact generation is active");
    let (_, required) = fixture
        .generations
        .recover_committed_for_mount(&fixture.containers)
        .expect("recover the committed graph");
    let mut coverage = ScrubCoverage::new(required);
    let mut collector = DependentBasePinCollector::default();
    for id in fixture
        .containers
        .recovery_container_snapshot()
        .expect("snapshot published Containers")
    {
        let certificate = fixture
            .containers
            .scrub_for_progress(id, Some(&active), &mut coverage, 1)
            .expect("scrub one Container");
        collector
            .observe(&certificate, Some(&active))
            .expect("collect dependent Base pins");
    }
    collector
}

fn prove_and_collect(fixture: &Fixture, committed: u64, rows: Vec<GcCandidateCatalogRow>) -> usize {
    let (maintenance, proof) = prove(fixture, committed, rows);
    let replacement_chunks = proof.replacement_chunks();
    maintenance
        .garbage_collect_proved_candidates(proof)
        .expect("relocate required Bases before deleting victims");
    maintenance
        .scrub()
        .expect("every stored dependent still resolves its Base");
    replacement_chunks
}

/// Enough dead DATA beside a Base to make relocating the Base profitable.
fn dead_padding() -> Vec<u8> {
    (0..96 * 1_024_u32)
        .map(|index| u8::try_from(index.wrapping_mul(2_654_435_761) >> 24).expect("byte"))
        .collect()
}

#[test]
fn online_gc_relocates_a_flex_base_of_a_live_dependent() {
    let fixture = Fixture::new();
    let (base, target) = base_and_target(4);
    let padding = b"unrelated-dead-online-gc-padding";
    let base_generation = fixture.publish_raw(0x30, &[&base]);
    let padding_generation = fixture.publish_raw(0x31, &[padding]);
    fixture.publish_dependents(0x32, &[(&base, &target)]);
    let committed = fixture.commit_files(&[&target], 1);

    let relocated = prove_and_collect(
        &fixture,
        committed,
        vec![
            candidate_row(0x30, base_generation, &[&base]),
            candidate_row(0x31, padding_generation, &[padding]),
        ],
    );

    assert_eq!(relocated, 1, "only the live target's Base is relocated");
    fixture.assert_readable(&target);
}

#[test]
fn online_gc_relocates_the_base_of_a_dead_dependent_outside_the_victims() {
    let fixture = Fixture::new();
    let (live_base, live_target) = base_and_target(5);
    let (dead_base, dead_target) = base_and_target(6);
    let padding = dead_padding();
    fixture.publish_raw(0x40, &[&live_base]);
    let victim_generation = fixture.publish_raw(0x41, &[&dead_base, &padding]);
    fixture.publish_dependents(
        0x42,
        &[(&live_base, &live_target), (&dead_base, &dead_target)],
    );
    let committed = fixture.commit_files(&[&live_target], 1);

    let relocated = prove_and_collect(
        &fixture,
        committed,
        vec![candidate_row(
            0x41,
            victim_generation,
            &[&dead_base, &padding],
        )],
    );

    assert_eq!(
        relocated, 1,
        "the dead dependent stays stored, so its Base moves; the padding does not"
    );
    fixture.assert_readable(&live_target);
}

#[test]
fn offline_gc_releases_a_base_once_its_last_dependent_is_collected() {
    let fixture = Fixture::new();
    let (kept, _) = base_and_target(7);
    let (dead_base, dead_target) = base_and_target(8);
    fixture.publish_raw(0x50, &[&kept]);
    fixture.publish_raw(0x51, &[&dead_base]);
    fixture.publish_dependents(0x52, &[(&dead_base, &dead_target)]);
    fixture.commit_files(&[&kept], 1);
    let maintenance = fixture.maintenance();
    let pool = DataPoolUsage::new(50, 100).expect("pool usage is valid");

    // The dead dependent still pins its Base while it is being removed.
    let first = maintenance
        .garbage_collect(maintenance.scrub_for_gc(pool).expect("first scrub"))
        .expect("first collection");
    assert_eq!(
        first.containers_removed(),
        1,
        "only the dependent's Container goes"
    );
    maintenance
        .scrub()
        .expect("the retained Base still verifies");

    let maintenance = fixture.maintenance();
    let second = maintenance
        .garbage_collect(maintenance.scrub_for_gc(pool).expect("second scrub"))
        .expect("second collection");
    assert_eq!(
        second.containers_removed(),
        1,
        "the now unnamed Base is released"
    );
    maintenance.scrub().expect("the remaining graph verifies");
    fixture.assert_readable(&kept);
}

#[test]
fn online_gc_relocates_one_flex_base_for_targets_of_different_lengths() {
    let fixture = Fixture::new();
    let (base, first) = base_and_target(9);
    let mut second = base.clone();
    second[2_000] ^= 0x33;
    second.extend(std::iter::repeat_n(0x44, 300));
    let padding = dead_padding();
    let victim_generation = fixture.publish_raw(0x60, &[&base, &padding]);
    fixture.publish_dependents(0x61, &[(&base, &first), (&base, &second)]);
    let committed = fixture.commit_files(&[&first, &second], 1);

    let relocated = prove_and_collect(
        &fixture,
        committed,
        vec![candidate_row(0x60, victim_generation, &[&base, &padding])],
    );

    assert_eq!(relocated, 1, "both targets share the one relocated Base");
    fixture.assert_readable(&first);
    fixture.assert_readable(&second);
}

#[test]
fn recovered_online_gc_finalizes_a_dead_dependent_victim_after_process_loss() {
    let fixture = Fixture::new();
    let (kept, _) = base_and_target(10);
    let (dead_base, dead_target) = base_and_target(11);
    let padding = dead_padding();
    fixture.publish_raw(0x70, &[&kept]);
    let base_generation = fixture.publish_raw(0x71, &[&dead_base, &padding]);
    let pairs: [(&[u8], &[u8]); 1] = [(&dead_base, &dead_target)];
    let dependent_generation = fixture.publish_dependents(0x72, &pairs);
    let committed = fixture.commit_files(&[&kept], 1);

    // The dead dependent and its only Base are both victims.
    let (maintenance, proof) = prove(
        &fixture,
        committed,
        vec![
            candidate_row(0x71, base_generation, &[&dead_base, &padding]),
            dependent_row(0x72, dependent_generation, &pairs),
        ],
    );
    let retirement = maintenance
        .begin_online_gc_retirement(proof)
        .expect("publish replacements and activate RETIRING");
    // Process loss after the durable RETIRING activation.
    drop(retirement);
    drop(maintenance);

    let restarted = MaintenanceRepository::new(
        GenerationRepository::new(
            fixture.metadata.clone(),
            PolicySetId::new([0x91; 32]).expect("policy ID is nonzero"),
        ),
        ContainerRepository::new(fixture.containers.storage().clone()),
        ExactIndexRunRepository::new(fixture.metadata.clone()),
        fixture.profile,
    );
    let report = restarted
        .finalize_recovered_online_gc()
        .expect("restart resolves every retiring dependent and finalizes");
    assert_eq!(report.retiring_containers(), 2);
    restarted
        .scrub()
        .expect("the finalized pool has no orphaned dependent");
    fixture.assert_readable(&kept);
}

/// A Container published before a process loss and never activated in Exact
/// is invisible to online proofs; the startup scrub pins its dependents' Bases.
#[test]
fn online_gc_relocates_a_base_pinned_by_an_unindexed_dependent() {
    let fixture = Fixture::new();
    let (base, target) = base_and_target(12);
    let (indexed_base, indexed_target) = base_and_target(13);
    let padding = dead_padding();
    let victim_generation = fixture.publish_raw(0x80, &[&base, &padding]);
    fixture.publish_raw(0x81, &[&indexed_base]);
    fixture.publish_dependents(0x82, &[(&indexed_base, &indexed_target)]);
    let committed = fixture.commit_files(&[&indexed_target], 1);
    let maintenance = fixture.maintenance();
    fixture.publish_dependents(0x83, &[(&base, &target)]);

    let collector = scrub_pins(&fixture);
    assert_eq!(
        collector.len(),
        1,
        "only the unindexed dependent pins its Base"
    );
    let pins = maintenance.dependent_base_pins();
    pins.install(collector);
    let proof = prove_with(
        &maintenance,
        &fixture,
        1,
        committed,
        vec![candidate_row(0x80, victim_generation, &[&base, &padding])],
    );
    assert_eq!(
        proof.replacement_chunks(),
        1,
        "the pinned Base is relocated"
    );
    maintenance
        .garbage_collect_proved_candidates(proof)
        .expect("relocate the pinned Base before deleting its Container");

    maintenance
        .scrub()
        .expect("the unindexed dependent still resolves its Base");
    fixture.assert_readable(&indexed_target);
}

#[test]
fn online_gc_releases_a_scrub_pin_once_its_dependent_container_is_collected() {
    let fixture = Fixture::new();
    let (base, target) = base_and_target(14);
    let padding = dead_padding();
    let victim_generation = fixture.publish_raw(0x84, &[&base, &padding]);
    let committed = fixture.commit_files(&[], 1);
    let maintenance = fixture.maintenance();
    let pairs: [(&[u8], &[u8]); 1] = [(&base, &target)];
    let dependent_generation = fixture.publish_dependents(0x85, &pairs);
    let pins = maintenance.dependent_base_pins();
    pins.install(scrub_pins(&fixture));
    assert_eq!(pins.len(), 1);

    let proof = prove_with(
        &maintenance,
        &fixture,
        1,
        committed,
        vec![dependent_row(0x85, dependent_generation, &pairs)],
    );
    maintenance
        .garbage_collect_proved_candidates(proof)
        .expect("collect the dead unindexed dependent");
    assert!(
        pins.is_empty(),
        "removing the pinning Container releases its pin"
    );

    let proof = prove_with(
        &maintenance,
        &fixture,
        2,
        committed,
        vec![candidate_row(0x84, victim_generation, &[&base, &padding])],
    );
    assert_eq!(
        proof.replacement_chunks(),
        0,
        "the unnamed Base is released"
    );
    maintenance
        .garbage_collect_proved_candidates(proof)
        .expect("collect the released Base");
    maintenance
        .scrub()
        .expect("no stored dependent names the collected Base");
}

/// A RETIRING Container still stores its dependents until it is unlinked.
#[test]
fn online_gc_keeps_the_base_of_a_dependent_in_a_retiring_container() {
    let fixture = Fixture::new();
    let (base, target) = base_and_target(15);
    let padding = dead_padding();
    let victim_generation = fixture.publish_raw(0x86, &[&base, &padding]);
    let pairs: [(&[u8], &[u8]); 1] = [(&base, &target)];
    let dependent_generation = fixture.publish_dependents(0x87, &pairs);
    let committed = fixture.commit_files(&[], 1);
    let maintenance = fixture.maintenance();

    let proof = prove_with(
        &maintenance,
        &fixture,
        1,
        committed,
        vec![dependent_row(0x87, dependent_generation, &pairs)],
    );
    let retirement = maintenance
        .begin_online_gc_retirement(proof)
        .expect("activate RETIRING for the dead dependent");
    let base_victim = vec![candidate_row(0x86, victim_generation, &[&base, &padding])];
    let proof = prove_with(&maintenance, &fixture, 2, committed, base_victim.clone());
    assert_eq!(
        proof.replacement_chunks(),
        1,
        "a RETIRING dependent still requires its Base"
    );
    drop(proof);

    maintenance
        .finish_online_gc_retirement(retirement)
        .expect("unlink the retired dependent");
    let proof = prove_with(&maintenance, &fixture, 3, committed, base_victim);
    assert_eq!(
        proof.replacement_chunks(),
        0,
        "the unnamed Base is released"
    );
    maintenance
        .garbage_collect_proved_candidates(proof)
        .expect("collect the released Base");
    maintenance
        .scrub()
        .expect("no stored dependent names the collected Base");
}
