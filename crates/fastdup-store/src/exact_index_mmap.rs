#![allow(unsafe_code)]

//! Audited read-only mappings for immutable Exact Index Run generations.
//!
//! Unsafe code is confined to mapping one fully published file. The mapping
//! owns an immutable-file lease, so cooperating filesystem adapters cannot
//! write, truncate, replace, or unlink its name until the mapping is dropped.

use std::fmt;
use std::mem::size_of;

use memmap2::{Mmap, MmapOptions};

use fastdup_format::{
    ChunkId, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES, ExactIndexEntry,
    ExactIndexPagePosition, ExactIndexRunDescriptor,
};

use crate::ImmutableFileLease;
use crate::exact_index_repository::ExactIndexStoreError;

/// One fully audited immutable Exact Run backed by a read-only mapping.
pub(crate) struct ImmutableExactIndexRun {
    // Drop order is significant: unmap before releasing the mutation lease.
    mapping: Mmap,
    _lease: ImmutableFileLease,
    descriptor: ExactIndexRunDescriptor,
    page_bounds: Box<[ExactPageKeyBounds]>,
}

impl ImmutableExactIndexRun {
    pub(crate) fn open(
        lease: ImmutableFileLease,
        expected: ExactIndexRunDescriptor,
        mut visit: impl FnMut(&ExactIndexEntry),
    ) -> Result<Self, ExactIndexStoreError> {
        let metadata = lease.file().metadata()?;
        let expected_length = u64::try_from(expected.file_length())
            .map_err(|_| ExactIndexStoreError::CounterOverflow)?;
        if !metadata.is_file() || metadata.len() != expected_length {
            return Err(ExactIndexStoreError::IdentityMismatch);
        }

        // SAFETY: `lease` owns a read-only descriptor for a no-replace
        // published object. FsStorageIo holds a root-wide generation lease
        // that rejects writes, truncation, replacement, and removal for this
        // name until the mapping is dropped. The descriptor stays alive in
        // `_lease`, and its exact file length was checked above.
        let mapping = unsafe {
            MmapOptions::new()
                .len(expected.file_length())
                .map(lease.file())?
        };
        if mapping.len() != expected.file_length() {
            return Err(ExactIndexStoreError::IdentityMismatch);
        }

        let header = exact_range(&mapping, 0, EXACT_INDEX_HEADER_BYTES)?;
        let footer_offset = expected
            .file_length()
            .checked_sub(EXACT_INDEX_PAGE_BYTES)
            .ok_or(ExactIndexStoreError::IdentityMismatch)?;
        let footer = exact_range(&mapping, footer_offset, EXACT_INDEX_PAGE_BYTES)?;
        let descriptor = ExactIndexRunDescriptor::decode(header, footer, expected_length)?;
        if descriptor != expected {
            return Err(ExactIndexStoreError::IdentityMismatch);
        }

        let mut audit = descriptor.begin_hash_audit();
        let mut page_bounds = Vec::new();
        page_bounds
            .try_reserve_exact(descriptor.page_count())
            .map_err(|_| ExactIndexStoreError::OutOfMemory)?;
        audit.update(0, header)?;
        for page_ordinal in 0..descriptor.page_count() {
            let offset = descriptor
                .page_offset(page_ordinal)
                .ok_or(ExactIndexStoreError::IdentityMismatch)?;
            let bytes = exact_page(&mapping, offset)?;
            let page = descriptor.decode_page(page_ordinal, bytes)?;
            audit.verify_page(&page)?;
            page_bounds.push(ExactPageKeyBounds::from_page(&page));
            for entry in page.entries() {
                visit(entry);
            }
            audit.update(offset, bytes)?;
        }
        audit.update(
            u64::try_from(footer_offset).map_err(|_| ExactIndexStoreError::CounterOverflow)?,
            footer,
        )?;
        audit.finish()?;

        Ok(Self {
            mapping,
            _lease: lease,
            descriptor,
            page_bounds: page_bounds.into_boxed_slice(),
        })
    }

    pub(crate) fn page_position(
        &self,
        page_ordinal: usize,
        chunk_id: ChunkId,
        logical_length: u32,
    ) -> Result<ExactIndexPagePosition, ExactIndexStoreError> {
        self.page_bounds
            .get(page_ordinal)
            .map(|bounds| bounds.position(chunk_id, logical_length))
            .ok_or(ExactIndexStoreError::IdentityMismatch)
    }

    pub(crate) fn page_bounds_bytes(&self) -> usize {
        self.page_bounds
            .len()
            .checked_mul(size_of::<ExactPageKeyBounds>())
            .expect("ASSERT: mapped Exact page-bound bytes fit usize")
    }

    pub(crate) fn page(&self, offset: u64) -> Result<&[u8], ExactIndexStoreError> {
        exact_page(&self.mapping, offset)
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

fn exact_page(mapping: &[u8], offset: u64) -> Result<&[u8], ExactIndexStoreError> {
    let offset = usize::try_from(offset).map_err(|_| ExactIndexStoreError::CounterOverflow)?;
    exact_range(mapping, offset, EXACT_INDEX_PAGE_BYTES)
}

fn exact_range(
    mapping: &[u8],
    offset: usize,
    length: usize,
) -> Result<&[u8], ExactIndexStoreError> {
    let end = offset
        .checked_add(length)
        .ok_or(ExactIndexStoreError::IdentityMismatch)?;
    mapping
        .get(offset..end)
        .ok_or(ExactIndexStoreError::IdentityMismatch)
}
