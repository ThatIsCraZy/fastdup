use fastdup_format::{ContainerId, GcCandidateCatalogRow, SealedContainer};
use fastdup_store::{GcCandidateCatalogRepository, GcCandidateSelectionMode};
use fastdup_testkit::MemoryStorageIo;

fn fixture_row() -> GcCandidateCatalogRow {
    let id = ContainerId::new([0xCA; 16]).expect("fixture identity is nonzero");
    let (image, publication) =
        SealedContainer::encode_with_writer_evidence(id, 7, &[b"GC catalog fault fixture"])
            .expect("fixture Container encodes")
            .into_publication_parts();
    GcCandidateCatalogRow::from_intrinsic_summary(
        id,
        7,
        u64::try_from(image.len()).expect("fixture length fits u64"),
        publication
            .intrinsic_summary()
            .expect("publication reconstructs intrinsic summary"),
    )
    .expect("fixture row is valid")
}

fn publish(repository: &GcCandidateCatalogRepository<MemoryStorageIo>) {
    repository
        .publish_rows(1, 9, 4, 1, [fixture_row()])
        .expect("catalog publication succeeds without a fault");
}

fn assert_absent_or_complete(repository: &GcCandidateCatalogRepository<MemoryStorageIo>) {
    let Some(snapshot) = repository
        .recover_latest()
        .expect("recovery treats incomplete catalogs as hints")
    else {
        return;
    };
    assert_eq!(snapshot.descriptor().generation(), 1);
    assert_eq!(snapshot.descriptor().incorporated_commit_generation(), 9);
    assert_eq!(snapshot.descriptor().incorporated_location_generation(), 4);
    let shortlist = snapshot
        .shortlist(GcCandidateSelectionMode::Urgent, 1, 8)
        .expect("recovered catalog reaudits");
    assert_eq!(shortlist.rows(), &[fixture_row()]);
}

#[test]
fn every_catalog_publish_failpoint_recovers_to_absent_or_fully_audited() {
    let probe = MemoryStorageIo::new();
    publish(&GcCandidateCatalogRepository::new(probe.clone()));
    let operation_count = probe.operation_count();
    assert!(operation_count > 0, "publication exercises storage I/O");

    for position in 0..operation_count {
        for fail_after in [false, true] {
            let storage = if fail_after {
                MemoryStorageIo::with_fail_after(position)
            } else {
                MemoryStorageIo::with_fail_before(position)
            };
            let repository = GcCandidateCatalogRepository::new(storage.clone());
            let result = repository.publish_rows(1, 9, 4, 1, [fixture_row()]);
            assert!(
                result.is_err(),
                "configured fault {position}, fail_after={fail_after} was not observed"
            );
            storage.crash();
            assert_absent_or_complete(&repository);
        }
    }
}
