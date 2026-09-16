//! Audited immutable-file leases with bounded Direct-I/O reads.
//! Decoded views and encoded ranges use the common application cache.

use std::fmt;
use std::mem::size_of;
use std::sync::Arc;

use fastdup_format::{
    ChunkId, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES, ExactIndexEntry,
    ExactIndexPagePosition, ExactIndexRunDescriptor,
};

use crate::ImmutableFileLease;
use crate::MAX_STORAGE_RANGE_BYTES;
use crate::exact_index_repository::ExactIndexStoreError;

/// Full-scan Direct-I/O span: at most one adapter range read (the ADR 0030
/// 1-MiB bound) supplies a contiguous run of independently verified 4-KiB
/// pages. Only the I/O granularity changes; page decoding, checksumming, and
/// AUDIT hashing still consume exact per-page bytes from RAM.
pub(crate) const EXACT_SCAN_PAGES_PER_IO: usize = MAX_STORAGE_RANGE_BYTES / EXACT_INDEX_PAGE_BYTES;

/// Reads `page_count` consecutive pages in ascending Direct-I/O spans and
/// hands each complete span to `visit` with its first page ordinal. `read_span`
/// must return exactly `pages * EXACT_INDEX_PAGE_BYTES` bytes; the caller
/// retains full responsibility for per-page decode, verification, and AUDIT
/// order inside `visit`.
///
/// # Errors
/// Propagates span read failures and the first `visit` failure. A short span
/// fails closed as an identity mismatch.
pub(crate) fn visit_page_spans(
    descriptor: &ExactIndexRunDescriptor,
    page_count: usize,
    mut read_span: impl FnMut(u64, usize) -> Result<Vec<u8>, ExactIndexStoreError>,
    mut visit: impl FnMut(usize, &[u8]) -> Result<(), ExactIndexStoreError>,
) -> Result<(), ExactIndexStoreError> {
    let mut first = 0_usize;
    while first < page_count {
        let pages = (page_count - first).min(EXACT_SCAN_PAGES_PER_IO);
        let offset = descriptor
            .page_offset(first)
            .ok_or(ExactIndexStoreError::IdentityMismatch)?;
        let span = read_span(offset, pages * EXACT_INDEX_PAGE_BYTES)?;
        if span.len() != pages * EXACT_INDEX_PAGE_BYTES {
            return Err(ExactIndexStoreError::IdentityMismatch);
        }
        visit(first, &span)?;
        first += pages;
    }
    Ok(())
}

/// One fully audited immutable Exact Run backed by a read-only file lease.
pub(crate) struct ImmutableExactIndexRun {
    // The lease protects physical identity until the final reader drops.
    lease: ImmutableFileLease,
    descriptor: ExactIndexRunDescriptor,
    bounds_cache: crate::ReadCacheNamespace,
    writer_pages: Option<crate::ReadCacheNamespace>,
}

impl ImmutableExactIndexRun {
    pub(crate) fn open(
        lease: ImmutableFileLease,
        expected: ExactIndexRunDescriptor,
        bounds_parent: &crate::ReadCacheNamespace,
        mut visit: impl FnMut(&ExactIndexEntry),
    ) -> Result<Self, ExactIndexStoreError> {
        let _read_reason = crate::MetadataReadScope::enter(crate::MetadataReadReason::IndexAudit);
        let independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let metadata = lease.file().metadata()?;
        let expected_length = u64::try_from(expected.file_length())
            .map_err(|_| ExactIndexStoreError::CounterOverflow)?;
        if !metadata.is_file() || lease.logical_len()? != expected_length {
            return Err(ExactIndexStoreError::IdentityMismatch);
        }

        let header = exact_range(&lease, 0, EXACT_INDEX_HEADER_BYTES)?;
        let footer_offset = expected
            .file_length()
            .checked_sub(EXACT_INDEX_PAGE_BYTES)
            .ok_or(ExactIndexStoreError::IdentityMismatch)?;
        let footer = exact_range(
            &lease,
            u64::try_from(footer_offset).map_err(|_| ExactIndexStoreError::CounterOverflow)?,
            EXACT_INDEX_PAGE_BYTES,
        )?;
        let descriptor = ExactIndexRunDescriptor::decode(&header, &footer, expected_length)?;
        if descriptor != expected {
            return Err(ExactIndexStoreError::IdentityMismatch);
        }

        let mut audit = descriptor.begin_hash_audit();
        let mut page_bounds = Vec::new();
        page_bounds
            .try_reserve_exact(descriptor.page_count())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        audit.update(0, &header)?;
        visit_page_spans(
            &descriptor,
            descriptor.page_count(),
            |offset, length| exact_range(&lease, offset, length),
            |first, span| {
                for (index, page_bytes) in span.chunks_exact(EXACT_INDEX_PAGE_BYTES).enumerate() {
                    let page_ordinal = first + index;
                    let offset = descriptor
                        .page_offset(page_ordinal)
                        .ok_or(ExactIndexStoreError::IdentityMismatch)?;
                    let page = descriptor.decode_page(page_ordinal, page_bytes)?;
                    audit.verify_page(&page)?;
                    page_bounds.push(ExactPageKeyBounds::from_page(&page));
                    for entry in page.entries() {
                        visit(entry);
                    }
                    audit.update(offset, page_bytes)?;
                }
                Ok(())
            },
        )?;
        audit.update(
            u64::try_from(footer_offset).map_err(|_| ExactIndexStoreError::CounterOverflow)?,
            &footer,
        )?;
        audit.finish()?;

        drop(independent);
        let bounds_cache = bounds_parent.ephemeral_sibling(crate::ReadCacheClass::ExactPageBounds);
        bounds_cache.insert(
            crate::ReadCacheKey {
                identity: descriptor.run_hash(),
                ordinal: 0,
            },
            Arc::new(page_bounds.into_boxed_slice()),
            (descriptor.page_count() * size_of::<ExactPageKeyBounds>()
                + size_of::<Box<[ExactPageKeyBounds]>>()) as u64,
            EXACT_INDEX_PAGE_BYTES as u64,
        );
        Ok(Self {
            lease,
            descriptor,
            bounds_cache,
            writer_pages: None,
        })
    }

    /// The caller owns the checked encoder output and completed publication.
    /// Only optional page/bounds retention enters the common cache. The file
    /// lease, not a cache hit, prevents mutation while this proof is selected.
    pub(crate) fn from_writer(
        lease: ImmutableFileLease,
        descriptor: ExactIndexRunDescriptor,
        bounds: Vec<ExactPageKeyBounds>,
        pages: crate::ReadCacheNamespace,
    ) -> Result<Self, ExactIndexStoreError> {
        if bounds.len() != descriptor.page_count() {
            return Err(ExactIndexStoreError::DependencyMismatch);
        }
        let bounds_cache = pages.ephemeral_sibling(crate::ReadCacheClass::ExactPageBounds);
        let bytes = bounds.capacity() * size_of::<ExactPageKeyBounds>()
            + size_of::<Box<[ExactPageKeyBounds]>>();
        bounds_cache.insert(
            crate::ReadCacheKey {
                identity: descriptor.run_hash(),
                ordinal: 0,
            },
            Arc::new(bounds.into_boxed_slice()),
            bytes as u64,
            EXACT_INDEX_PAGE_BYTES as u64,
        );
        Ok(Self {
            lease,
            descriptor,
            bounds_cache,
            writer_pages: Some(pages),
        })
    }

    pub(crate) fn page_position(
        &self,
        page_ordinal: usize,
        chunk_id: ChunkId,
        logical_length: u32,
    ) -> Result<ExactIndexPagePosition, ExactIndexStoreError> {
        if let Some(bounds) =
            self.bounds_cache
                .get::<Box<[ExactPageKeyBounds]>>(crate::ReadCacheKey {
                    identity: self.descriptor.run_hash(),
                    ordinal: 0,
                })
        {
            return bounds
                .get(page_ordinal)
                .map(|bounds| bounds.position(chunk_id, logical_length))
                .ok_or(ExactIndexStoreError::IdentityMismatch);
        }
        let offset = self
            .descriptor
            .page_offset(page_ordinal)
            .ok_or(ExactIndexStoreError::IdentityMismatch)?;
        let page = self
            .descriptor
            .decode_page(page_ordinal, &self.page(offset)?)?;
        Ok(page.position(chunk_id, logical_length))
    }

    /// Reads one contiguous span of this Run's page area (at most
    /// `MAX_STORAGE_RANGE_BYTES`). Like `page`, this is an audited lease read.
    ///
    /// # Errors
    /// Propagates the same bounded Direct-I/O failures as `page`.
    pub(crate) fn page_span(
        &self,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, ExactIndexStoreError> {
        if let Some(pages) = &self.writer_pages {
            let first_page = offset / EXACT_INDEX_PAGE_BYTES as u64;
            let count = length / EXACT_INDEX_PAGE_BYTES;
            let mut span = Vec::with_capacity(length);
            for ordinal in first_page..first_page + count as u64 {
                let hit = pages.get::<Vec<u8>>(crate::ReadCacheKey {
                    identity: [0; 32],
                    ordinal,
                });
                match hit {
                    Some(bytes) if bytes.len() == EXACT_INDEX_PAGE_BYTES => {
                        span.extend_from_slice(&bytes);
                    }
                    _ => break,
                }
            }
            if span.len() == length {
                return Ok(span);
            }
            let skipped = span.len();
            span.extend(exact_range(
                &self.lease,
                offset + skipped as u64,
                length - skipped,
            )?);
            return Ok(span);
        }
        exact_range(&self.lease, offset, length)
    }

    /// Returns the complete cached page-key bounds array without I/O.
    pub(crate) fn peek_page_bounds(&self) -> Option<Arc<Box<[ExactPageKeyBounds]>>> {
        self.bounds_cache.peek(crate::ReadCacheKey {
            identity: self.descriptor.run_hash(),
            ordinal: 0,
        })
    }

    pub(crate) fn page_bounds_bytes(&self) -> usize {
        self.bounds_cache
            .peek::<Box<[ExactPageKeyBounds]>>(crate::ReadCacheKey {
                identity: self.descriptor.run_hash(),
                ordinal: 0,
            })
            .map_or(0, |bounds| bounds.len() * size_of::<ExactPageKeyBounds>())
    }

    #[must_use]
    pub(crate) const fn page_bounds_charge_bytes(page_count: usize) -> usize {
        page_count * size_of::<ExactPageKeyBounds>() + size_of::<Box<[ExactPageKeyBounds]>>()
    }

    pub(crate) fn insert_page_bounds(&self, bounds: Box<[ExactPageKeyBounds]>) -> bool {
        if bounds.len() != self.descriptor.page_count() {
            return false;
        }
        let bytes = u64::try_from(
            bounds.len() * size_of::<ExactPageKeyBounds>() + size_of::<Box<[ExactPageKeyBounds]>>(),
        )
        .map_or(u64::MAX, |value| value);
        self.bounds_cache.insert(
            crate::ReadCacheKey {
                identity: self.descriptor.run_hash(),
                ordinal: 0,
            },
            Arc::new(bounds),
            bytes,
            EXACT_INDEX_PAGE_BYTES as u64,
        );
        self.page_bounds_bytes() != 0
    }

    pub(crate) fn page(&self, offset: u64) -> Result<Vec<u8>, ExactIndexStoreError> {
        if let Some(bytes) = self.writer_pages.as_ref().and_then(|pages| {
            pages.get::<Vec<u8>>(crate::ReadCacheKey {
                identity: [0; 32],
                ordinal: offset / EXACT_INDEX_PAGE_BYTES as u64,
            })
        }) {
            return Ok((*bytes).clone());
        }
        exact_page(&self.lease, offset)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ExactPageKeyBounds {
    first: (ChunkId, u32),
    last: (ChunkId, u32),
}

impl ExactPageKeyBounds {
    pub(crate) fn from_page(page: &fastdup_format::ExactIndexPage) -> Self {
        Self::from_entries(page.entries())
    }

    pub(crate) fn from_entries(entries: &[ExactIndexEntry]) -> Self {
        let first = entries
            .first()
            .expect("ASSERT: a verified Exact Index page is never empty");
        let last = entries
            .last()
            .expect("ASSERT: a verified Exact Index page is never empty");
        Self {
            first: (first.chunk_id(), first.logical_length()),
            last: (last.chunk_id(), last.logical_length()),
        }
    }

    /// True when the key sorts after this page's last entry, mirroring the
    /// `After` result of `position` for batched boundary searches.
    pub(crate) fn is_after(self, chunk_id: ChunkId, logical_length: u32) -> bool {
        (chunk_id, logical_length) > self.last
    }

    pub(crate) fn position(self, chunk_id: ChunkId, logical_length: u32) -> ExactIndexPagePosition {
        let key = (chunk_id, logical_length);
        if key < self.first {
            ExactIndexPagePosition::Before
        } else if key > self.last {
            ExactIndexPagePosition::After
        } else {
            ExactIndexPagePosition::Within
        }
    }
}

impl fmt::Debug for ImmutableExactIndexRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ImmutableExactIndexRun")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

fn exact_page(lease: &ImmutableFileLease, offset: u64) -> Result<Vec<u8>, ExactIndexStoreError> {
    exact_range(lease, offset, EXACT_INDEX_PAGE_BYTES)
}

fn exact_range(
    lease: &ImmutableFileLease,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, ExactIndexStoreError> {
    if length > MAX_STORAGE_RANGE_BYTES {
        return Err(ExactIndexStoreError::IdentityMismatch);
    }
    Ok(lease.read_at(offset, length)?)
}
