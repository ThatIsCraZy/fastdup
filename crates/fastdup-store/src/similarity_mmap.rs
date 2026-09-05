#![allow(unsafe_code)]

//! Audited read-only mappings for immutable Similarity Run generations.
//!
//! Unsafe code is deliberately confined to the one mapping operation. The
//! lease owns the read-only file descriptor and prevents all cooperating
//! filesystem adapters from mutating or unlinking the published name until
//! the mapping is dropped.

use memmap2::{Mmap, MmapOptions};

use fastdup_format::{
    SIMILARITY_INDEX_HEADER_BYTES, SIMILARITY_INDEX_PAGE_BYTES, SimilarityBucketKey,
    SimilarityIndexEntry, SimilarityIndexPage, SimilarityIndexRunDescriptor,
};

use crate::ImmutableFileLease;
use crate::similarity_index_repository::SimilarityIndexStoreError;

/// One fully audited immutable Similarity Run backed by a read-only mapping.
pub(crate) struct ImmutableSimilarityRun {
    // Drop order is significant: unmap before releasing the mutation lease.
    mapping: Mmap,
    _lease: ImmutableFileLease,
    descriptor: SimilarityIndexRunDescriptor,
    minimum_bucket_key: SimilarityBucketKey,
    maximum_bucket_key: SimilarityBucketKey,
}

impl ImmutableSimilarityRun {
    pub(crate) fn open(
        lease: ImmutableFileLease,
        expected: SimilarityIndexRunDescriptor,
        observe_bucket_page: impl FnMut(SimilarityBucketKey),
    ) -> Result<Self, SimilarityIndexStoreError> {
        let metadata = lease.file().metadata()?;
        if !metadata.is_file() || metadata.len() != expected.file_length() {
            return Err(SimilarityIndexStoreError::IdentityMismatch);
        }
        let length = usize::try_from(expected.file_length())
            .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;

        // SAFETY: `lease` owns a read-only descriptor for a no-replace
        // published object. FsStorageIo holds a root-wide generation lease
        // that rejects write, truncate, rename, and remove operations for this
        // name until the mapping is dropped. The appliance owns this directory;
        // out-of-process mutation is outside the storage interface contract.
        // The descriptor remains alive in `_lease` for at least as long as the
        // mapping, and the exact file length was verified above.
        let mapping = unsafe { MmapOptions::new().len(length).map(lease.file())? };
        if mapping.len() != length {
            return Err(SimilarityIndexStoreError::IdentityMismatch);
        }

        let header = exact_range(&mapping, 0, SIMILARITY_INDEX_HEADER_BYTES)?;
        let footer_offset = usize::try_from(expected.footer_offset())
            .map_err(|_| SimilarityIndexStoreError::CounterOverflow)?;
        let footer = exact_range(&mapping, footer_offset, SIMILARITY_INDEX_HEADER_BYTES)?;
        let descriptor =
            SimilarityIndexRunDescriptor::decode(header, footer, expected.file_length())?;
        if descriptor != expected {
            return Err(SimilarityIndexStoreError::IdentityMismatch);
        }

        let (minimum_bucket_key, maximum_bucket_key) =
            audit_mapping(&mapping, descriptor, observe_bucket_page)?;
        Ok(Self {
            mapping,
            _lease: lease,
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

    pub(crate) fn page(&self, offset: u64) -> Result<&[u8], SimilarityIndexStoreError> {
        let offset =
            usize::try_from(offset).map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
        exact_range(&self.mapping, offset, SIMILARITY_INDEX_PAGE_BYTES)
    }
}

fn audit_mapping(
    mapping: &[u8],
    descriptor: SimilarityIndexRunDescriptor,
    mut observe_bucket_page: impl FnMut(SimilarityBucketKey),
) -> Result<(SimilarityBucketKey, SimilarityBucketKey), SimilarityIndexStoreError> {
    let mut audit = descriptor.start_hash_audit();
    let header = exact_range(mapping, 0, SIMILARITY_INDEX_HEADER_BYTES)?;
    audit.update(0, header)?;

    for ordinal in 0..descriptor.page_count() {
        let offset = descriptor
            .page_offset(ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let bytes = exact_page(mapping, offset)?;
        let page = descriptor.decode_page(ordinal, bytes)?;
        audit.verify_page(&page)?;
        audit.update(offset, bytes)?;
    }

    let mut semantic_entry_page = None;
    let mut minimum_bucket_key = None;
    let mut maximum_bucket_key = None;
    for ordinal in 0..descriptor.bucket_page_count() {
        let offset = descriptor
            .bucket_page_offset(ordinal)
            .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
        let bytes = exact_page(mapping, offset)?;
        let page = descriptor.decode_bucket_page(ordinal, bytes)?;
        minimum_bucket_key.get_or_insert_with(|| page.first_key());
        maximum_bucket_key = Some(page.last_key());
        observe_bucket_page(page.last_key());
        for reference in page.references() {
            let entry = mapped_entry(
                mapping,
                descriptor,
                reference.entry_ordinal(),
                &mut semantic_entry_page,
            )?;
            let key = reference.key();
            if entry.fingerprint_profile() != key.fingerprint_profile()
                || entry.logical_length() != key.logical_length()
                || entry.superfeatures().get(usize::from(key.slot())) != Some(&key.superfeature())
            {
                return Err(SimilarityIndexStoreError::IndexCorruption);
            }
        }
        audit.verify_bucket_page(&page)?;
        audit.update(offset, bytes)?;
    }

    let footer_offset = descriptor.footer_offset();
    let footer = exact_page(mapping, footer_offset)?;
    audit.update(footer_offset, footer)?;
    audit.finish()?;
    Ok((
        minimum_bucket_key.ok_or(SimilarityIndexStoreError::IndexCorruption)?,
        maximum_bucket_key.ok_or(SimilarityIndexStoreError::IndexCorruption)?,
    ))
}

fn mapped_entry(
    mapping: &[u8],
    descriptor: SimilarityIndexRunDescriptor,
    entry_ordinal: u32,
    cached_page: &mut Option<(usize, SimilarityIndexPage)>,
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
        let page = descriptor.decode_page(page_ordinal, exact_page(mapping, offset)?)?;
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

fn exact_page(mapping: &[u8], offset: u64) -> Result<&[u8], SimilarityIndexStoreError> {
    let offset = usize::try_from(offset).map_err(|_| SimilarityIndexStoreError::IndexCorruption)?;
    exact_range(mapping, offset, SIMILARITY_INDEX_PAGE_BYTES)
}

fn exact_range(
    mapping: &[u8],
    offset: usize,
    length: usize,
) -> Result<&[u8], SimilarityIndexStoreError> {
    let end = offset
        .checked_add(length)
        .ok_or(SimilarityIndexStoreError::IndexCorruption)?;
    mapping
        .get(offset..end)
        .ok_or(SimilarityIndexStoreError::IndexCorruption)
}
