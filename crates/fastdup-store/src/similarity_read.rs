//! Audited immutable-file leases with bounded Direct-I/O reads.
//! Decoded views and encoded ranges use the common application cache.

use fastdup_format::{
    SIMILARITY_INDEX_HEADER_BYTES, SIMILARITY_INDEX_PAGE_BYTES, SimilarityBucketKey,
    SimilarityBucketPage, SimilarityIndexEntry, SimilarityIndexPage, SimilarityIndexRunDescriptor,
};

use crate::ImmutableFileLease;

/// Pages per audit range read: one 256 KiB Direct-I/O range per 64 pages.
pub(crate) const SIMILARITY_AUDIT_PAGES_PER_IO_V1: usize = 64;
use crate::similarity_index_repository::{SimilarityIndexStoreError, SimilarityPageCache};
use std::sync::Arc;

/// One fully audited immutable Similarity Run backed by a read-only file lease.
pub(crate) struct ImmutableSimilarityRun {
    // The lease protects physical identity until the final reader drops.
    lease: ImmutableFileLease,
    descriptor: SimilarityIndexRunDescriptor,
    minimum_bucket_key: SimilarityBucketKey,
    maximum_bucket_key: SimilarityBucketKey,
}

impl ImmutableSimilarityRun {
    pub(crate) fn open(
        lease: ImmutableFileLease,
        expected: SimilarityIndexRunDescriptor,
        page_cache: &SimilarityPageCache,
        observe_bucket_page: impl FnMut(SimilarityBucketKey),
    ) -> Result<Self, SimilarityIndexStoreError> {
        let _read_reason = crate::MetadataReadScope::enter(crate::MetadataReadReason::IndexAudit);
        let independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let metadata = lease.file().metadata()?;
        if !metadata.is_file() || lease.logical_len()? != expected.file_length() {
            return Err(SimilarityIndexStoreError::IdentityMismatch);
        }

        let header = exact_range(&lease, 0, SIMILARITY_INDEX_HEADER_BYTES)?;
        let footer_offset = usize::try_from(expected.footer_offset())
            .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
        let footer = exact_range(&lease, footer_offset, SIMILARITY_INDEX_HEADER_BYTES)?;
        let descriptor =
            SimilarityIndexRunDescriptor::decode(&header, &footer, expected.file_length())?;
        if descriptor != expected {
            return Err(SimilarityIndexStoreError::IdentityMismatch);
        }

        drop(independent);
        let (minimum_bucket_key, maximum_bucket_key) =
            audit_source(&lease, descriptor, page_cache, observe_bucket_page)?;
        Ok(Self {
            lease,
            descriptor,
            minimum_bucket_key,
            maximum_bucket_key,
        })
    }

    pub(crate) const fn descriptor(&self) -> SimilarityIndexRunDescriptor {
        self.descriptor
    }

    pub(crate) const fn minimum_bucket_key(&self) -> SimilarityBucketKey {
        self.minimum_bucket_key
    }

    pub(crate) const fn maximum_bucket_key(&self) -> SimilarityBucketKey {
        self.maximum_bucket_key
    }

    pub(crate) fn page(&self, offset: u64) -> Result<Vec<u8>, SimilarityIndexStoreError> {
        let offset =
            usize::try_from(offset).map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
        exact_range(&self.lease, offset, SIMILARITY_INDEX_PAGE_BYTES)
    }
}

fn audit_source(
    lease: &ImmutableFileLease,
    descriptor: SimilarityIndexRunDescriptor,
    page_cache: &SimilarityPageCache,
    observe_bucket_page: impl FnMut(SimilarityBucketKey),
) -> Result<(SimilarityBucketKey, SimilarityBucketKey), SimilarityIndexStoreError> {
    let independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
    let mut audit = descriptor.start_hash_audit();
    let header = exact_range(lease, 0, SIMILARITY_INDEX_HEADER_BYTES)?;
    audit.update(0, &header)?;

    for_each_page_span(
        lease,
        |ordinal| descriptor.page_offset(ordinal),
        descriptor.page_count(),
        |ordinal, offset, bytes| {
            let page = descriptor.decode_page(ordinal, bytes)?;
            audit.verify_page(&page)?;
            Ok(audit.update(offset, bytes)?)
        },
    )?;

    // Verify the complete fresh on-disk hash and page ordering first. Cached
    // bytes cannot make a damaged publication pass this gate.
    for_each_page_span(
        lease,
        |ordinal| descriptor.bucket_page_offset(ordinal),
        descriptor.bucket_page_count(),
        |ordinal, offset, bytes| {
            let page = descriptor.decode_bucket_page(ordinal, bytes)?;
            audit.verify_bucket_page(&page)?;
            Ok(audit.update(offset, bytes)?)
        },
    )?;
    let footer_offset = descriptor.footer_offset();
    audit.update(footer_offset, &exact_page(lease, footer_offset)?)?;
    audit.finish()?;
    drop(independent);

    // The Run hash is now bound to the current immutable lease. Retain Entry
    // pages in the shared budget before the nonlocal Bucket semantic walk.
    // Admission may fail/shrink: correctness always has the bounded fallback.
    let run_hash = descriptor.run_hash();
    let mut ordinal = 0;
    while ordinal < descriptor.page_count() {
        if page_cache.get_entry(run_hash, ordinal).is_some() {
            ordinal += 1;
            continue;
        }
        // Retain only the contiguous absent pages: a warm cache still pays
        // nothing, and a cold one pays one range read instead of one per page.
        let mut span = 1;
        while span < SIMILARITY_AUDIT_PAGES_PER_IO_V1
            && ordinal + span < descriptor.page_count()
            && page_cache.get_entry(run_hash, ordinal + span).is_none()
        {
            span += 1;
        }
        let offset = descriptor
            .page_offset(ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let bytes = page_span(lease, offset, span)?;
        for (index, page_bytes) in bytes.chunks_exact(SIMILARITY_INDEX_PAGE_BYTES).enumerate() {
            let page = Arc::new(descriptor.decode_page(ordinal + index, page_bytes)?);
            page_cache.insert_entry(run_hash, ordinal + index, page);
        }
        ordinal += span;
    }
    walk_buckets(lease, descriptor, page_cache, observe_bucket_page)
}

/// Proves every Bucket reference against its Entry and reports the Run's key range.
fn walk_buckets(
    lease: &ImmutableFileLease,
    descriptor: SimilarityIndexRunDescriptor,
    page_cache: &SimilarityPageCache,
    mut observe_bucket_page: impl FnMut(SimilarityBucketKey),
) -> Result<(SimilarityBucketKey, SimilarityBucketKey), SimilarityIndexStoreError> {
    let run_hash = descriptor.run_hash();
    let mut semantic_entry_page = None;
    let mut minimum_bucket_key = None;
    let mut maximum_bucket_key = None;
    // The walk consumes Bucket pages in ascending order, so a miss can refill a
    // whole span at once instead of paying one Direct-I/O read per page.
    let mut bucket_span: Option<(usize, Vec<u8>)> = None;
    for ordinal in 0..descriptor.bucket_page_count() {
        let page = if let Some(page) = page_cache.get_bucket(run_hash, ordinal) {
            page
        } else {
            bucket_page_from_span(lease, descriptor, ordinal, &mut bucket_span)?
        };
        minimum_bucket_key.get_or_insert_with(|| page.first_key());
        maximum_bucket_key = Some(page.last_key());
        observe_bucket_page(page.last_key());
        for reference in page.references() {
            let entry = mapped_entry(
                lease,
                descriptor,
                reference.entry_ordinal(),
                &mut semantic_entry_page,
                page_cache,
            )?;
            let key = reference.key();
            if entry.fingerprint_profile() != key.fingerprint_profile()
                || entry.logical_length() != key.logical_length()
                || entry.superfeatures().get(usize::from(key.slot())) != Some(&key.superfeature())
            {
                return Err(SimilarityIndexStoreError::IndexCorruption);
            }
        }
        page_cache.insert_bucket(run_hash, ordinal, page);
    }
    Ok((
        minimum_bucket_key.ok_or(SimilarityIndexStoreError::IndexCorruption)?,
        maximum_bucket_key.ok_or(SimilarityIndexStoreError::IndexCorruption)?,
    ))
}

/// Decodes one Bucket page from the resident span, refilling the span first.
fn bucket_page_from_span(
    lease: &ImmutableFileLease,
    descriptor: SimilarityIndexRunDescriptor,
    ordinal: usize,
    bucket_span: &mut Option<(usize, Vec<u8>)>,
) -> Result<Arc<SimilarityBucketPage>, SimilarityIndexStoreError> {
    let covered = bucket_span.as_ref().is_some_and(|(first, bytes)| {
        ordinal >= *first && ordinal - *first < bytes.len() / SIMILARITY_INDEX_PAGE_BYTES
    });
    if !covered {
        let span = SIMILARITY_AUDIT_PAGES_PER_IO_V1.min(descriptor.bucket_page_count() - ordinal);
        let offset = descriptor
            .bucket_page_offset(ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        *bucket_span = Some((ordinal, page_span(lease, offset, span)?));
    }
    let (first, bytes) = bucket_span
        .as_ref()
        .expect("ASSERT: a Bucket span covering this ordinal is resident");
    let start = (ordinal - first) * SIMILARITY_INDEX_PAGE_BYTES;
    Ok(Arc::new(descriptor.decode_bucket_page(
        ordinal,
        &bytes[start..start + SIMILARITY_INDEX_PAGE_BYTES],
    )?))
}

fn mapped_entry(
    lease: &ImmutableFileLease,
    descriptor: SimilarityIndexRunDescriptor,
    entry_ordinal: u32,
    cached_page: &mut Option<(usize, Arc<SimilarityIndexPage>)>,
    page_cache: &SimilarityPageCache,
) -> Result<SimilarityIndexEntry, SimilarityIndexStoreError> {
    let entry_ordinal =
        usize::try_from(entry_ordinal).map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
    if entry_ordinal >= descriptor.entry_count() {
        return Err(SimilarityIndexStoreError::IndexCorruption);
    }
    let page_ordinal = entry_ordinal / fastdup_format::SIMILARITY_INDEX_ENTRIES_PER_PAGE;
    if cached_page
        .as_ref()
        .is_none_or(|(cached_ordinal, _)| *cached_ordinal != page_ordinal)
    {
        let offset = descriptor
            .page_offset(page_ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let page = if let Some(page) = page_cache.get_entry(descriptor.run_hash(), page_ordinal) {
            page
        } else {
            let page = Arc::new(descriptor.decode_page(page_ordinal, &exact_page(lease, offset)?)?);
            page_cache.insert_entry(descriptor.run_hash(), page_ordinal, Arc::clone(&page));
            page
        };
        *cached_page = Some((page_ordinal, page));
    }
    cached_page
        .as_ref()
        .and_then(|(_, page)| {
            page.entries()
                .get(entry_ordinal % fastdup_format::SIMILARITY_INDEX_ENTRIES_PER_PAGE)
        })
        .copied()
        .ok_or(SimilarityIndexStoreError::IndexCorruption)
}

fn exact_page(
    lease: &ImmutableFileLease,
    offset: u64,
) -> Result<Vec<u8>, SimilarityIndexStoreError> {
    let offset = usize::try_from(offset).map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
    exact_range(lease, offset, SIMILARITY_INDEX_PAGE_BYTES)
}

/// Reads `pages` consecutive pages from `offset` as one Direct-I/O range.
fn page_span(
    lease: &ImmutableFileLease,
    offset: u64,
    pages: usize,
) -> Result<Vec<u8>, SimilarityIndexStoreError> {
    let offset = usize::try_from(offset).map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
    let length = pages
        .checked_mul(SIMILARITY_INDEX_PAGE_BYTES)
        .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
    let bytes = exact_range(lease, offset, length)?;
    if bytes.len() != length {
        return Err(SimilarityIndexStoreError::IndexCorruption);
    }
    Ok(bytes)
}

/// Walks `count` consecutive pages in ascending order, reading them in bounded
/// spans instead of one range per page.
///
/// The audit sees the same pages, in the same order, decoded from the same
/// bytes; only the I/O granularity differs. Auditing a Run on every mount
/// otherwise costs one 4 KiB Direct-I/O read per page, which is device-bound
/// at a few thousand IOPS no matter how fast the Run can be streamed.
fn for_each_page_span(
    lease: &ImmutableFileLease,
    page_offset: impl Fn(usize) -> Option<u64>,
    count: usize,
    mut observe: impl FnMut(usize, u64, &[u8]) -> Result<(), SimilarityIndexStoreError>,
) -> Result<(), SimilarityIndexStoreError> {
    let mut ordinal = 0;
    while ordinal < count {
        let span = SIMILARITY_AUDIT_PAGES_PER_IO_V1.min(count - ordinal);
        let first = page_offset(ordinal).ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let bytes = page_span(lease, first, span)?;
        for (index, page_bytes) in bytes.chunks_exact(SIMILARITY_INDEX_PAGE_BYTES).enumerate() {
            let offset = page_offset(ordinal + index)
                .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
            observe(ordinal + index, offset, page_bytes)?;
        }
        ordinal += span;
    }
    Ok(())
}

fn exact_range(
    lease: &ImmutableFileLease,
    offset: usize,
    length: usize,
) -> Result<Vec<u8>, SimilarityIndexStoreError> {
    Ok(lease.read_at(offset as u64, length)?)
}
