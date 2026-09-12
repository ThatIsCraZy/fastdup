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
use crate::exact_index_repository::ExactIndexStoreError;

/// One fully audited immutable Exact Run backed by a read-only file lease.
pub(crate) struct ImmutableExactIndexRun {
    // The lease protects physical identity until the final reader drops.
    lease: ImmutableFileLease,
    descriptor: ExactIndexRunDescriptor,
    bounds_cache: crate::ReadCacheNamespace,
}

impl ImmutableExactIndexRun {
    pub(crate) fn open(
        lease: ImmutableFileLease,
        expected: ExactIndexRunDescriptor,
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
        let footer = exact_range(&lease, footer_offset, EXACT_INDEX_PAGE_BYTES)?;
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
        for page_ordinal in 0..descriptor.page_count() {
            let offset = descriptor
                .page_offset(page_ordinal)
                .ok_or(ExactIndexStoreError::IdentityMismatch)?;
            let bytes = exact_page(&lease, offset)?;
            let page = descriptor.decode_page(page_ordinal, &bytes)?;
            audit.verify_page(&page)?;
            page_bounds.push(ExactPageKeyBounds::from_page(&page));
            for entry in page.entries() {
                visit(entry);
            }
            audit.update(offset, &bytes)?;
        }
        audit.update(
            u64::try_from(footer_offset).map_err(|_| ExactIndexStoreError::CounterOverflow)?,
            &footer,
        )?;
        audit.finish()?;

        drop(independent);
        let bounds_cache =
            crate::ReadCacheNamespace::system(crate::ReadCacheClass::ExactPageBounds);
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

    pub(crate) fn page_bounds_bytes(&self) -> usize {
        self.bounds_cache
            .peek::<Box<[ExactPageKeyBounds]>>(crate::ReadCacheKey {
                identity: self.descriptor.run_hash(),
                ordinal: 0,
            })
            .map_or(0, |bounds| bounds.len() * size_of::<ExactPageKeyBounds>())
    }

    pub(crate) fn page(&self, offset: u64) -> Result<Vec<u8>, ExactIndexStoreError> {
        exact_page(&self.lease, offset)
    }
}

#[derive(Clone, Copy)]
struct ExactPageKeyBounds {
    first: (ChunkId, u32),
    last: (ChunkId, u32),
}

impl ExactPageKeyBounds {
    fn from_page(page: &fastdup_format::ExactIndexPage) -> Self {
        let first = page
            .entries()
            .first()
            .expect("ASSERT: a verified Exact Index page is never empty");
        let last = page
            .entries()
            .last()
            .expect("ASSERT: a verified Exact Index page is never empty");
        Self {
            first: (first.chunk_id(), first.logical_length()),
            last: (last.chunk_id(), last.logical_length()),
        }
    }

    fn position(self, chunk_id: ChunkId, logical_length: u32) -> ExactIndexPagePosition {
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
    let offset = usize::try_from(offset).map_err(|_| ExactIndexStoreError::CounterOverflow)?;
    exact_range(lease, offset, EXACT_INDEX_PAGE_BYTES)
}

fn exact_range(
    lease: &ImmutableFileLease,
    offset: usize,
    length: usize,
) -> Result<Vec<u8>, ExactIndexStoreError> {
    Ok(lease.read_at(offset as u64, length)?)
}
