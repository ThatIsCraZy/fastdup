use fastdup_format::{
    ChunkId, SIMILARITY_INDEX_ENTRY_BYTES, SIMILARITY_INDEX_HEADER_BYTES,
    SIMILARITY_INDEX_PAGE_BYTES, SimilarityIndexEntry, SimilarityIndexFormatError,
    SimilarityIndexRun, SimilarityIndexRunDescriptor, SimilarityIndexRunStreamEncoder,
};

const FINGERPRINT_PROFILE: u16 = 1;
const BUCKET_PROFILE: u16 = 1;

#[test]
fn similarity_run_round_trips_and_streams_without_materializing_the_run() {
    let entries = fixture_entries(53);
    let run = SimilarityIndexRun::new(FINGERPRINT_PROFILE, BUCKET_PROFILE, 17, entries.clone())
        .expect("construct canonical Similarity run");
    let encoded = run.encode().expect("encode Similarity run");
    let decoded = SimilarityIndexRun::decode(&encoded).expect("decode Similarity run");
    assert_eq!(decoded, run);
    assert_eq!(decoded.entries().len(), entries.len());

    let footer_offset = encoded.len() - SIMILARITY_INDEX_HEADER_BYTES;
    let descriptor = SimilarityIndexRunDescriptor::decode(
        &encoded[..SIMILARITY_INDEX_HEADER_BYTES],
        &encoded[footer_offset..],
        u64::try_from(encoded.len()).expect("fixture length fits u64"),
    )
    .expect("verify Similarity descriptor");
    assert_eq!(descriptor.entry_count(), 53);
    assert_eq!(descriptor.page_count(), 3);
    assert_eq!(descriptor.bucket_reference_count(), 212);
    assert_eq!(descriptor.bucket_page_count(), 2);
    assert_eq!(descriptor.fingerprint_profile(), FINGERPRINT_PROFILE);
    assert_eq!(descriptor.bucket_profile(), BUCKET_PROFILE);

    let mut audit = descriptor.start_hash_audit();
    audit
        .update(0, &encoded[..SIMILARITY_INDEX_HEADER_BYTES])
        .expect("hash Header");
    let mut observed = Vec::new();
    for ordinal in 0..descriptor.page_count() {
        let offset = usize::try_from(
            descriptor
                .page_offset(ordinal)
                .expect("page ordinal is in range"),
        )
        .expect("page offset fits usize");
        let bytes = &encoded[offset..offset + SIMILARITY_INDEX_PAGE_BYTES];
        let page = descriptor
            .decode_page(ordinal, bytes)
            .expect("decode independently checked page");
        audit
            .update(u64::try_from(offset).expect("offset fits u64"), bytes)
            .expect("hash page");
        audit.verify_page(&page).expect("verify cross-page order");
        observed.extend_from_slice(page.entries());
    }
    let mut observed_bucket_references = Vec::new();
    for ordinal in 0..descriptor.bucket_page_count() {
        let offset = usize::try_from(
            descriptor
                .bucket_page_offset(ordinal)
                .expect("bucket page ordinal is in range"),
        )
        .expect("bucket page offset fits usize");
        let bytes = &encoded[offset..offset + SIMILARITY_INDEX_PAGE_BYTES];
        let page = descriptor
            .decode_bucket_page(ordinal, bytes)
            .expect("decode independently checked bucket page");
        audit
            .update(u64::try_from(offset).expect("offset fits u64"), bytes)
            .expect("hash bucket page");
        audit
            .verify_bucket_page(&page)
            .expect("verify bucket cross-page order");
        observed_bucket_references.extend_from_slice(page.references());
    }
    audit
        .update(
            descriptor.footer_offset(),
            &encoded[footer_offset..footer_offset + SIMILARITY_INDEX_HEADER_BYTES],
        )
        .expect("hash Footer with durable hash fields zeroed");
    audit.finish().expect("verify complete streamed run hash");
    assert_eq!(observed, decoded.entries());
    assert_eq!(observed_bucket_references, decoded.bucket_references());
}

#[test]
fn similarity_run_rejects_duplicate_identity_and_mixed_profiles() {
    let mut duplicate = fixture_entries(2);
    duplicate[1] = SimilarityIndexEntry::new(
        duplicate[0].chunk_id(),
        duplicate[0].logical_length(),
        FINGERPRINT_PROFILE,
        duplicate[0].superfeatures(),
        duplicate[0].sketch(),
    )
    .expect("duplicate remains field-valid");
    assert_eq!(
        SimilarityIndexRun::new(FINGERPRINT_PROFILE, BUCKET_PROFILE, 1, duplicate),
        Err(SimilarityIndexFormatError::NonCanonicalOrder)
    );

    let mixed = vec![
        SimilarityIndexEntry::new(
            ChunkId::of(b"mixed profile"),
            4_096,
            FINGERPRINT_PROFILE + 1,
            [1, 2, 3, 4],
            [5; 8],
        )
        .expect("alternate profile is field-valid"),
    ];
    assert_eq!(
        SimilarityIndexRun::new(FINGERPRINT_PROFILE, BUCKET_PROFILE, 1, mixed),
        Err(SimilarityIndexFormatError::InvalidEntry)
    );
}

#[test]
fn similarity_run_rejects_page_header_footer_and_hash_corruption() {
    let run = SimilarityIndexRun::new(FINGERPRINT_PROFILE, BUCKET_PROFILE, 9, fixture_entries(26))
        .expect("construct corruption fixture");
    let encoded = run.encode().expect("encode corruption fixture");

    let mut header = encoded.clone();
    header[200] = 1;
    assert!(matches!(
        SimilarityIndexRun::decode(&header),
        Err(SimilarityIndexFormatError::InvalidHeader)
    ));

    let mut page = encoded.clone();
    let first_entry_reserved = SIMILARITY_INDEX_HEADER_BYTES + 96 + 38;
    page[first_entry_reserved] = 1;
    let descriptor = SimilarityIndexRunDescriptor::decode(
        &page[..SIMILARITY_INDEX_HEADER_BYTES],
        &page[page.len() - SIMILARITY_INDEX_HEADER_BYTES..],
        u64::try_from(page.len()).expect("fixture length fits u64"),
    )
    .expect("page corruption does not alter descriptor blocks");
    assert_eq!(
        descriptor.decode_page(
            0,
            &page[SIMILARITY_INDEX_HEADER_BYTES
                ..SIMILARITY_INDEX_HEADER_BYTES + SIMILARITY_INDEX_PAGE_BYTES],
        ),
        Err(SimilarityIndexFormatError::InvalidPage)
    );

    let mut footer = encoded.clone();
    let footer_offset = footer.len() - SIMILARITY_INDEX_HEADER_BYTES;
    footer[footer_offset + 24] ^= 1;
    assert!(matches!(
        SimilarityIndexRun::decode(&footer),
        Err(SimilarityIndexFormatError::InvalidHeader
            | SimilarityIndexFormatError::HeaderFooterMismatch)
    ));

    let mut hash_only = encoded;
    let page_payload = SIMILARITY_INDEX_HEADER_BYTES + 96 + SIMILARITY_INDEX_ENTRY_BYTES;
    hash_only[page_payload + 10] ^= 1;
    let page_start = SIMILARITY_INDEX_HEADER_BYTES;
    let page_end = page_start + SIMILARITY_INDEX_PAGE_BYTES;
    let crc = crc32c_with_zeroed(&hash_only[page_start..page_end], 20);
    hash_only[page_start + 20..page_start + 24].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(
        SimilarityIndexRun::decode(&hash_only),
        Err(SimilarityIndexFormatError::RunHashMismatch)
    );
}

#[test]
fn bucket_pages_retain_only_the_64_smallest_chunk_id_ordinals() {
    let entries = (0_u64..1_000)
        .map(|ordinal| {
            SimilarityIndexEntry::new(
                ChunkId::of(&ordinal.to_le_bytes()),
                64 * 1_024,
                FINGERPRINT_PROFILE,
                [11, 22, 33, 44],
                [ordinal; 8],
            )
            .expect("construct hot-bucket entry")
        })
        .collect();
    let run = SimilarityIndexRun::new(FINGERPRINT_PROFILE, BUCKET_PROFILE, 10, entries)
        .expect("construct hot-bucket run");

    assert_eq!(run.bucket_count(), 4);
    assert_eq!(run.bucket_references().len(), 4 * 64);
    for references in run.bucket_references().chunks(64) {
        assert_eq!(references.len(), 64);
        assert_eq!(
            references
                .iter()
                .map(|reference| reference.entry_ordinal())
                .collect::<Vec<_>>(),
            (0_u32..64).collect::<Vec<_>>()
        );
    }
}

#[test]
fn streaming_encoder_is_byte_identical_and_rejects_incomplete_output() {
    let run = SimilarityIndexRun::new(
        FINGERPRINT_PROFILE,
        BUCKET_PROFILE,
        11,
        fixture_entries(400),
    )
    .expect("construct streaming fixture");
    let canonical = run.encode().expect("encode canonical fixture");

    let incomplete =
        SimilarityIndexRunStreamEncoder::new(run.stream_layout()).expect("start incomplete writer");
    assert!(matches!(
        incomplete.finish(),
        Err(SimilarityIndexFormatError::NonSequentialAudit)
    ));

    let mut encoder =
        SimilarityIndexRunStreamEncoder::new(run.stream_layout()).expect("start streaming writer");
    let mut streamed = Vec::with_capacity(encoder.file_length());
    streamed.extend_from_slice(encoder.header());
    for entries in run.entries().chunks(25) {
        streamed.extend_from_slice(
            &encoder
                .encode_next_entry_page(entries)
                .expect("stream entry page"),
        );
    }
    for references in run.bucket_references().chunks(167) {
        streamed.extend_from_slice(
            &encoder
                .encode_next_bucket_page(references)
                .expect("stream bucket page"),
        );
    }
    let (footer, descriptor) = encoder.finish().expect("finish streaming writer");
    streamed.extend_from_slice(&footer);

    assert_eq!(streamed, canonical);
    assert_eq!(descriptor.file_length(), streamed.len() as u64);
    assert_eq!(
        SimilarityIndexRun::decode(&streamed).expect("decode streamed bytes"),
        run
    );
}

fn fixture_entries(count: usize) -> Vec<SimilarityIndexEntry> {
    (0..count)
        .rev()
        .map(|ordinal| {
            let ordinal = u64::try_from(ordinal).expect("fixture ordinal fits u64");
            let bytes = fixture_bytes(64, ordinal);
            SimilarityIndexEntry::new(
                ChunkId::of(&bytes),
                64 * 1_024,
                FINGERPRINT_PROFILE,
                [ordinal, ordinal + 1, ordinal + 2, ordinal + 3],
                [ordinal.rotate_left(7); 8],
            )
            .expect("fixture entry is valid")
        })
        .collect()
}

fn fixture_bytes(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn crc32c_with_zeroed(bytes: &[u8], offset: usize) -> u32 {
    let mut copy = bytes.to_vec();
    copy[offset..offset + 4].fill(0);
    crc32c::crc32c(&copy)
}
