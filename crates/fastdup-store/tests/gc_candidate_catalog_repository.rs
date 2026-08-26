use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_format::{
    ContainerId, ContainerIntrinsicSummary, GcCandidateCatalogRow, GcCandidateLivenessEstimate,
    GcCandidateLocationState, GcDependencyEstimate, GcRecordLivenessEstimate, SealedContainer,
};
use fastdup_store::{
    FsStorageIo, GcCandidateCatalogRepository, GcCandidateCatalogStoreError,
    GcCandidateSelectionMode, StorageIo, gc_candidate_row_from_publication,
};

#[test]
fn verified_publication_becomes_a_seed_without_payload_reread() {
    let id = ContainerId::new([0xF1; 16]).expect("fixture identity is nonzero");
    let (image, publication) =
        SealedContainer::encode_with_writer_evidence(id, 19, &[b"publication seed"])
            .expect("fixture Container encodes")
            .into_publication_parts();
    let row = gc_candidate_row_from_publication(&publication)
        .expect("payload-free publication evidence seeds catalog");

    assert_eq!(row.container_id(), id);
    assert_eq!(row.container_generation(), 19);
    assert_eq!(
        row.physical_bytes(),
        u64::try_from(image.len()).expect("fixture length fits u64")
    );
    assert!(!row.estimate_known());
}

#[test]
fn newest_catalog_recovers_as_an_audited_mapping_and_holds_its_mutation_lease() {
    let root = test_root("mmap-lease");
    let storage = FsStorageIo::open(&root).expect("open catalog root");
    let repository = GcCandidateCatalogRepository::new(storage.clone());
    let summary = fixture_summary();
    let rows = (0_u8..8)
        .map(|ordinal| estimated_row(ordinal + 1, u64::from(ordinal) + 1, summary))
        .collect::<Vec<_>>();
    repository
        .publish_rows(1, 41, 17, 8, rows)
        .expect("publish first catalog");

    let snapshot = repository
        .recover_latest()
        .expect("recover latest catalog")
        .expect("catalog exists");
    assert!(snapshot.mapped());
    assert_eq!(snapshot.descriptor().incorporated_commit_generation(), 41);
    let shortlist = snapshot
        .shortlist(GcCandidateSelectionMode::Urgent, 3, 100)
        .expect("scan mapped catalog");
    assert_eq!(shortlist.rows().len(), 3);
    assert_eq!(shortlist.rows()[0].reachable_target_count(), 0);

    let published = published_name(&storage);
    assert_eq!(
        storage
            .remove_file(&published)
            .expect_err("mapped generation rejects removal")
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    drop(snapshot);
    storage
        .remove_file(&published)
        .expect("last mapping drop releases removal lease");
}

#[test]
fn corrupt_newest_hint_falls_back_without_failing_the_older_catalog() {
    let root = test_root("fallback");
    let storage = FsStorageIo::open(&root).expect("open catalog root");
    let repository = GcCandidateCatalogRepository::new(storage.clone());
    let summary = fixture_summary();
    repository
        .publish_rows(1, 10, 5, 1, [seed_row(1, 1, summary)])
        .expect("publish older catalog");
    repository
        .publish_rows(2, 11, 6, 1, [seed_row(2, 2, summary)])
        .expect("publish newer catalog");

    let newest = root.join("gc-candidate-catalog-0000000000000002.run");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(newest)
        .expect("open newest catalog for fault injection");
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, 4_096 + 40)
        .expect("read row byte");
    byte[0] ^= 1;
    file.write_all_at(&byte, 4_096 + 40)
        .expect("corrupt newest row");

    let recovered = repository
        .recover_latest()
        .expect("corrupt hint generation falls back")
        .expect("older catalog remains");
    assert_eq!(recovered.descriptor().generation(), 1);
}

#[test]
fn empty_newest_generation_prevents_fallback_to_stale_candidates() {
    let root = test_root("empty-tombstone");
    let storage = FsStorageIo::open(&root).expect("open catalog root");
    let repository = GcCandidateCatalogRepository::new(storage);
    repository
        .publish_rows(1, 10, 5, 1, [seed_row(1, 1, fixture_summary())])
        .expect("publish populated predecessor");
    repository
        .publish_rows(2, 11, 6, 0, [])
        .expect("publish empty successor generation");

    let recovered = repository
        .recover_latest()
        .expect("recover empty generation")
        .expect("empty generation is present");
    assert_eq!(recovered.descriptor().generation(), 2);
    assert!(
        recovered
            .shortlist(GcCandidateSelectionMode::Urgent, 1, 2)
            .expect("empty shortlist succeeds")
            .rows()
            .is_empty()
    );
}

#[test]
fn hundred_thousand_rows_publish_and_shortlist_without_a_pool_sized_builder() {
    let root = test_root("large-stream");
    let storage = FsStorageIo::open(&root).expect("open catalog root");
    let repository = GcCandidateCatalogRepository::new(storage);
    let summary = fixture_summary();
    let row_count = 100_000_u64;
    let rows = (1..=row_count).map(|ordinal| {
        let id = ContainerId::new(u128::from(ordinal).to_be_bytes())
            .expect("positive ordinal is a nonzero sorted identity");
        GcCandidateCatalogRow::from_intrinsic_summary(id, ordinal, 12_288, summary)
            .expect("fixture publication row")
    });
    let descriptor = repository
        .publish_rows(7, 100, 50, row_count, rows)
        .expect("stream large catalog");
    assert_eq!(descriptor.row_count(), row_count);
    assert!(descriptor.file_length() < 10 * 1_024 * 1_024);

    let snapshot = repository
        .recover_latest()
        .expect("recover large catalog")
        .expect("large catalog exists");
    let shortlist = snapshot
        .shortlist(GcCandidateSelectionMode::Background, 16, row_count + 1)
        .expect("bounded shortlist scans mapping");
    assert_eq!(shortlist.rows().len(), 16);
    assert!(
        shortlist
            .rows()
            .windows(2)
            .all(|pair| { pair[0].container_generation() < pair[1].container_generation() })
    );
}

#[test]
fn adapters_without_immutable_leases_reaudit_with_bounded_reads() {
    let root = test_root("bounded-fallback");
    let storage = NoLeaseFs(FsStorageIo::open(&root).expect("open fallback root"));
    let repository = GcCandidateCatalogRepository::new(storage);
    let summary = fixture_summary();
    repository
        .publish_rows(
            1,
            4,
            2,
            2,
            [seed_row(1, 1, summary), seed_row(2, 2, summary)],
        )
        .expect("publish fallback catalog");
    let snapshot = repository
        .recover_latest()
        .expect("recover through bounded adapter")
        .expect("catalog exists");
    assert!(!snapshot.mapped());
    assert_eq!(
        snapshot
            .shortlist(GcCandidateSelectionMode::Urgent, 2, 3)
            .expect("bounded path reaudits rows")
            .rows()
            .len(),
        2
    );
}

#[test]
fn publication_and_liveness_updates_merge_into_a_successor_without_pool_materialization() {
    let root = test_root("successor-merge");
    let storage = FsStorageIo::open(&root).expect("open successor root");
    let repository = GcCandidateCatalogRepository::new(storage);
    let summary = fixture_summary();
    repository
        .publish_rows(
            1,
            10,
            5,
            2,
            [seed_row(1, 1, summary), seed_row(3, 3, summary)],
        )
        .expect("publish predecessor");
    let predecessor = repository
        .recover_latest()
        .expect("recover predecessor")
        .expect("predecessor exists");
    let updates = [seed_row(2, 2, summary), estimated_row(3, 3, summary)];
    let successor = repository
        .publish_successor(&predecessor, 2, 11, 6, &updates)
        .expect("stream-merge publication and liveness updates");
    assert_eq!(successor.row_count(), 3);
    assert_eq!(successor.incorporated_commit_generation(), 11);
    drop(predecessor);

    let recovered = repository
        .recover_latest()
        .expect("recover successor")
        .expect("successor exists");
    let mut ids = recovered
        .shortlist(GcCandidateSelectionMode::Urgent, 3, 4)
        .expect("scan successor")
        .rows()
        .iter()
        .map(|row| row.container_id().bytes())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![[1; 16], [2; 16], [3; 16]]);

    let changed_identity = GcCandidateCatalogRow::from_intrinsic_summary(
        ContainerId::new([3; 16]).expect("identity is nonzero"),
        99,
        12_288,
        summary,
    )
    .expect("individually valid but stale identity update");
    assert!(matches!(
        repository.publish_successor(&recovered, 3, 12, 7, &[changed_identity]),
        Err(GcCandidateCatalogStoreError::ImmutableUpdateMismatch)
    ));
    assert!(matches!(
        repository.publish_successor(&recovered, 2, 12, 7, &[]),
        Err(GcCandidateCatalogStoreError::StaleSuccessor)
    ));
    assert!(matches!(
        repository.publish_successor(&recovered, 3, 10, 7, &[]),
        Err(GcCandidateCatalogStoreError::StaleSuccessor)
    ));
}

fn fixture_summary() -> ContainerIntrinsicSummary {
    let id = ContainerId::new([0xA5; 16]).expect("fixture identity is nonzero");
    let (_image, publication) =
        SealedContainer::encode_with_writer_evidence(id, 1, &[b"catalog summary fixture"])
            .expect("fixture Container encodes")
            .into_publication_parts();
    publication
        .intrinsic_summary()
        .expect("publication evidence reconstructs intrinsic summary")
}

fn seed_row(
    identity_byte: u8,
    generation: u64,
    summary: ContainerIntrinsicSummary,
) -> GcCandidateCatalogRow {
    GcCandidateCatalogRow::from_intrinsic_summary(
        ContainerId::new([identity_byte; 16]).expect("identity is nonzero"),
        generation,
        12_288,
        summary,
    )
    .expect("seed row")
}

fn estimated_row(
    identity_byte: u8,
    generation: u64,
    summary: ContainerIntrinsicSummary,
) -> GcCandidateCatalogRow {
    let seed = seed_row(identity_byte, generation, summary);
    let zero = identity_byte.is_multiple_of(2);
    let records = GcRecordLivenessEstimate::new(
        if zero { 256 } else { 0 },
        if zero { 0 } else { 256 },
        0,
        4_096,
    )
    .expect("record estimate fits");
    let estimate = GcCandidateLivenessEstimate::new(
        u32::from(!zero),
        if zero { 0 } else { 256 },
        records,
        Some(GcDependencyEstimate::new(0, 0)),
        4_096,
    )
    .expect("liveness estimate fits");
    seed.with_estimate(GcCandidateLocationState::Active, estimate)
        .expect("estimate applies")
}

fn published_name(storage: &FsStorageIo) -> String {
    storage
        .list_names()
        .expect("list catalog root")
        .into_iter()
        .find(|name| {
            name.starts_with("gc-candidate-catalog-")
                && Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("run"))
        })
        .expect("published catalog exists")
}

#[test]
fn generation_high_water_includes_a_corrupt_ignored_hint_name() {
    let root = test_root("corrupt-high-water");
    let storage = FsStorageIo::open(&root).expect("open catalog root");
    storage
        .create_new("gc-candidate-catalog-0000000000000009.run")
        .expect("create corrupt immutable hint name");
    storage.sync_root().expect("persist corrupt hint name");
    let catalog = GcCandidateCatalogRepository::new(storage);

    assert!(
        catalog
            .recover_latest()
            .expect("corrupt hint is non-authoritative")
            .is_none()
    );
    assert_eq!(
        catalog
            .discover_generation_high_water()
            .expect("name high-water is readable"),
        Some(9)
    );
}

fn test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock follows epoch")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!(
            "gc-candidate-catalog-{name}-{}-{nonce}",
            std::process::id()
        ))
}

#[derive(Clone, Debug)]
struct NoLeaseFs(FsStorageIo);

impl StorageIo for NoLeaseFs {
    fn create_new(&self, name: &str) -> io::Result<()> {
        self.0.create_new(name)
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        self.0.exists(name)
    }

    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.0.write_at(name, offset, bytes)
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        self.0.read(name)
    }

    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.0.object_len(name)
    }

    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.0.read_exact_at(name, offset, length)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        self.0.list_names()
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.0.set_len(name, length)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        self.0.sync_file(name)
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        self.0.publish_noreplace(temporary_name, published_name)
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.0.remove_file(name)
    }

    fn sync_root(&self) -> io::Result<()> {
        self.0.sync_root()
    }
}
