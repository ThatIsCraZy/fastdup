//! Audited immutable-file leases with bounded Direct-I/O reads.
//! Decoded views and encoded ranges use the common application cache.

use fastdup_format::{
    SIMILARITY_INDEX_HEADER_BYTES, SIMILARITY_INDEX_PAGE_BYTES, SimilarityBucketKey,
    SimilarityIndexEntry, SimilarityIndexPage, SimilarityIndexRunDescriptor,
};

use crate::ImmutableFileLease;
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
    mut observe_bucket_page: impl FnMut(SimilarityBucketKey),
) -> Result<(SimilarityBucketKey, SimilarityBucketKey), SimilarityIndexStoreError> {
    let independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
    let mut audit = descriptor.start_hash_audit();
    let header = exact_range(lease, 0, SIMILARITY_INDEX_HEADER_BYTES)?;
    audit.update(0, &header)?;

    for ordinal in 0..descriptor.page_count() {
        let offset = descriptor
            .page_offset(ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let bytes = exact_page(lease, offset)?;
        let page = descriptor.decode_page(ordinal, &bytes)?;
        audit.verify_page(&page)?;
        audit.update(offset, &bytes)?;
    }

    // Verify the complete fresh on-disk hash and page ordering first. Cached
    // bytes cannot make a damaged publication pass this gate.
    for ordinal in 0..descriptor.bucket_page_count() {
        let offset = descriptor
            .bucket_page_offset(ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let bytes = exact_page(lease, offset)?;
        let page = descriptor.decode_bucket_page(ordinal, &bytes)?;
        audit.verify_bucket_page(&page)?;
        audit.update(offset, &bytes)?;
    }
    let footer_offset = descriptor.footer_offset();
    audit.update(footer_offset, &exact_page(lease, footer_offset)?)?;
    audit.finish()?;
    drop(independent);

    // The Run hash is now bound to the current immutable lease. Retain Entry
    // pages in the shared budget before the nonlocal Bucket semantic walk.
    // Admission may fail/shrink: correctness always has the bounded fallback.
    let run_hash = descriptor.run_hash();
    for ordinal in 0..descriptor.page_count() {
        if page_cache.get_entry(run_hash, ordinal).is_none() {
            let offset = descriptor
                .page_offset(ordinal)
                .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
            let page = Arc::new(descriptor.decode_page(ordinal, &exact_page(lease, offset)?)?);
            page_cache.insert_entry(run_hash, ordinal, page);
        }
    }
    let mut semantic_entry_page = None;
    let mut minimum_bucket_key = None;
    let mut maximum_bucket_key = None;
    for ordinal in 0..descriptor.bucket_page_count() {
        let offset = descriptor
            .bucket_page_offset(ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let page = match page_cache.get_bucket(run_hash, ordinal) {
            Some(page) => page,
            None => Arc::new(descriptor.decode_bucket_page(ordinal, &exact_page(lease, offset)?)?),
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

fn exact_range(
    lease: &ImmutableFileLease,
    offset: usize,
    length: usize,
) -> Result<Vec<u8>, SimilarityIndexStoreError> {
    Ok(lease.read_at(offset as u64, length)?)
}
