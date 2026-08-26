use fastdup_format::{
    ContainerId, GC_CANDIDATE_CATALOG_HEADER_BYTES, GC_CANDIDATE_CATALOG_ROW_BYTES,
    GcCandidateCatalog, GcCandidateCatalogError, GcCandidateCatalogRow,
    GcCandidateCatalogStreamEncoder, GcCandidateLivenessEstimate, GcCandidateLocationState,
    GcDependencyEstimate, GcRecordLivenessEstimate, SealedContainer,
};

#[test]
fn empty_generation_is_a_complete_tombstone_not_an_absent_catalog() {
    let catalog = GcCandidateCatalog::new(1, 4, 2, Vec::new())
        .expect("an empty pool has a durable catalog generation");
    let encoded = catalog.encode().expect("empty catalog encodes");
    let decoded = GcCandidateCatalog::decode(&encoded).expect("empty catalog audits");

    assert_eq!(decoded, catalog);
    assert_eq!(decoded.descriptor().row_count(), 0);
    assert_eq!(decoded.descriptor().rows_end(), Some(4_096));
    assert_eq!(decoded.descriptor().footer_offset(), 4_096);
    assert_eq!(decoded.descriptor().file_length(), 8_192);
}

#[test]
fn fixed_rows_round_trip_with_generation_freshness_and_unknown_seed_state() {
    let first = seed(0x11, 7, &[b"alpha", b"beta"]);
    let second = seed(0x22, 8, &[b"gamma", b"delta"]);
    let catalog =
        GcCandidateCatalog::new(3, 41, 17, vec![first, second]).expect("ordered catalog is valid");
    let encoded = catalog.encode().expect("catalog encodes");
    let decoded = GcCandidateCatalog::decode(&encoded).expect("catalog independently audits");

    assert_eq!(decoded, catalog);
    assert_eq!(decoded.descriptor().incorporated_commit_generation(), 41);
    assert_eq!(decoded.descriptor().incorporated_location_generation(), 17);
    assert_eq!(decoded.descriptor().row_count(), 2);
    assert!(!decoded.rows()[0].estimate_known());
    assert_eq!(
        decoded.rows()[0].location_state(),
        GcCandidateLocationState::Active
    );
    assert_eq!(decoded.rows()[0].physical_bytes(), 12_288);
    assert!(decoded.rows()[0].raw_replacement_upper_bound() > 9);
}

#[test]
fn metadata_liveness_estimate_remains_explicitly_non_authoritative_row_state() {
    let seed = seed(0x33, 9, &[b"one", b"two"]);
    let records = GcRecordLivenessEstimate::new(256, 256, 0, 4_096)
        .expect("record classes fit immutable record area");
    let estimate = GcCandidateLivenessEstimate::new(
        1,
        256,
        records,
        Some(GcDependencyEstimate::new(1, 4)),
        4_096,
    )
    .expect("estimate fits immutable bounds");
    let row = seed
        .with_estimate(GcCandidateLocationState::Retiring, estimate)
        .expect("newer estimate applies");

    assert!(row.estimate_known());
    assert!(row.dependency_estimate_known());
    assert_eq!(row.reachable_target_count(), 1);
    assert_eq!(row.dead_record_bytes(), 256);
    assert_eq!(row.incoming_base_fanout(), 4);
    assert_eq!(row.location_state(), GcCandidateLocationState::Retiring);
}

#[test]
fn reachability_deltas_never_turn_unknown_underflow_into_zero_live() {
    let unknown = seed(0x34, 10, &[b"delta"]);
    let still_unknown = unknown
        .with_reachable_target_delta(-1)
        .expect("removing from unknown remains conservative");
    assert!(!still_unknown.estimate_known());

    let one = unknown
        .with_reachable_target_delta(1)
        .expect("first addition initializes the estimate");
    assert!(one.estimate_known());
    assert_eq!(one.reachable_target_count(), 1);
    let zero = one
        .with_reachable_target_delta(-1)
        .expect("balanced delta reaches zero");
    assert!(zero.estimate_known());
    assert_eq!(zero.reachable_target_count(), 0);
    let conservative = zero
        .with_reachable_target_delta(-1)
        .expect("underflow clears rather than fabricates an estimate");
    assert!(!conservative.estimate_known());
}

#[test]
fn streaming_writer_is_byte_identical_and_uses_bounded_fixed_rows() {
    let rows = [seed(0x44, 10, &[b"first"]), seed(0x55, 11, &[b"second"])];
    let canonical = GcCandidateCatalog::new(5, 77, 19, rows.to_vec())
        .expect("canonical catalog")
        .encode()
        .expect("canonical encoding");
    let mut encoder =
        GcCandidateCatalogStreamEncoder::new(5, 77, 19, 2).expect("streaming layout starts");
    let mut emitted = Vec::new();
    for row in rows {
        emitted.push(encoder.push(row).expect("ordered row streams"));
    }
    let (descriptor, header, footer) = encoder.finish().expect("exact row count finishes");
    let mut streamed =
        vec![0_u8; usize::try_from(descriptor.file_length()).expect("fixture length fits memory")];
    streamed[..GC_CANDIDATE_CATALOG_HEADER_BYTES].copy_from_slice(&header);
    for (offset, row) in emitted {
        let offset = usize::try_from(offset).expect("fixture offset fits memory");
        streamed[offset..offset + GC_CANDIDATE_CATALOG_ROW_BYTES].copy_from_slice(&row);
    }
    let footer_offset = usize::try_from(descriptor.footer_offset()).expect("offset fits memory");
    streamed[footer_offset..].copy_from_slice(&footer);

    assert_eq!(streamed, canonical);
}

#[test]
fn corruption_order_and_incomplete_streams_never_expose_catalog_rows() {
    let first = seed(0x66, 12, &[b"first"]);
    let second = seed(0x77, 13, &[b"second"]);
    assert_eq!(
        GcCandidateCatalog::new(6, 1, 1, vec![second, first]),
        Err(GcCandidateCatalogError::NonCanonicalOrder)
    );
    let incomplete = GcCandidateCatalogStreamEncoder::new(6, 1, 1, 2)
        .expect("stream starts")
        .finish();
    assert!(matches!(
        incomplete,
        Err(GcCandidateCatalogError::RowCountMismatch)
    ));

    let catalog = GcCandidateCatalog::new(6, 1, 1, vec![first, second]).expect("catalog is valid");
    let encoded = catalog.encode().expect("catalog encodes");
    for offset in [0, 120, 4_096, 4_096 + 36, encoded.len() - 1] {
        let mut corrupt = encoded.clone();
        corrupt[offset] ^= 1;
        assert!(
            GcCandidateCatalog::decode(&corrupt).is_err(),
            "corruption at {offset} escaped the independent audit"
        );
    }
}

fn seed(identity_byte: u8, generation: u64, chunks: &[&[u8]]) -> GcCandidateCatalogRow {
    let id = ContainerId::new([identity_byte; 16]).expect("fixture identity is nonzero");
    let (image, publication) = SealedContainer::encode_with_writer_evidence(id, generation, chunks)
        .expect("fixture Container encodes")
        .into_publication_parts();
    let summary = publication
        .intrinsic_summary()
        .expect("payload-free publication evidence reconstructs the writer summary");
    GcCandidateCatalogRow::from_intrinsic_summary(
        id,
        generation,
        u64::try_from(image.len()).expect("fixture length fits u64"),
        summary,
    )
    .expect("publication facts seed a catalog row")
}
