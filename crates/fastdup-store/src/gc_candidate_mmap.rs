#![allow(unsafe_code)]

//! Audited read-only mappings for immutable GC candidate catalog generations.
//!
//! Unsafe code is confined to the mapping operation. The shared immutable-file
//! lease prevents every cooperating storage adapter from mutating, truncating,
//! replacing, or unlinking the catalog name while this mapping exists.

use memmap2::{Mmap, MmapOptions};

use fastdup_format::{
    GC_CANDIDATE_CATALOG_HEADER_BYTES, GC_CANDIDATE_CATALOG_ROW_BYTES,
    GcCandidateCatalogDescriptor, GcCandidateCatalogRow,
};

use crate::ImmutableFileLease;
use crate::gc_candidate_catalog::GcCandidateCatalogStoreError;

pub(crate) struct ImmutableGcCandidateCatalog {
    // Drop order is significant: unmap before releasing the mutation lease.
    mapping: Mmap,
    _lease: ImmutableFileLease,
    descriptor: GcCandidateCatalogDescriptor,
}

impl ImmutableGcCandidateCatalog {
    pub(crate) fn open(
        lease: ImmutableFileLease,
        expected: GcCandidateCatalogDescriptor,
    ) -> Result<Self, GcCandidateCatalogStoreError> {
        let metadata = lease.file().metadata()?;
        if !metadata.is_file() || metadata.len() != expected.file_length() {
            return Err(GcCandidateCatalogStoreError::IdentityMismatch);
        }
        let length = usize::try_from(expected.file_length())
            .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;

        // SAFETY: `lease` owns a read-only descriptor for one no-replace
        // published object. FsStorageIo shares a root-wide lease registry that
        // rejects write, truncate, replacement, and remove for this exact name
        // until `_lease` drops. The appliance owns the directory; unsupported
        // out-of-process mutation is outside the StorageIo contract. The exact
        // file length and ordinary-file type were checked immediately above.
        let mapping = unsafe { MmapOptions::new().len(length).map(lease.file())? };
        if mapping.len() != length {
            return Err(GcCandidateCatalogStoreError::IdentityMismatch);
        }

        let footer_offset = usize::try_from(expected.footer_offset())
            .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
        let descriptor = GcCandidateCatalogDescriptor::decode(
            exact_range(&mapping, 0, GC_CANDIDATE_CATALOG_HEADER_BYTES)?,
            exact_range(&mapping, footer_offset, GC_CANDIDATE_CATALOG_HEADER_BYTES)?,
            expected.file_length(),
        )?;
        if descriptor != expected {
            return Err(GcCandidateCatalogStoreError::IdentityMismatch);
        }
        audit_mapping(&mapping, descriptor)?;
        Ok(Self {
            mapping,
            _lease: lease,
            descriptor,
        })
    }

    pub(crate) const fn descriptor(&self) -> GcCandidateCatalogDescriptor {
        self.descriptor
    }

    pub(crate) fn row(
        &self,
        ordinal: u64,
    ) -> Result<GcCandidateCatalogRow, GcCandidateCatalogStoreError> {
        let offset = self
            .descriptor
            .row_offset(ordinal)
            .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
        let offset =
            usize::try_from(offset).map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
        Ok(self.descriptor.decode_row(
            ordinal,
            exact_range(&self.mapping, offset, GC_CANDIDATE_CATALOG_ROW_BYTES)?,
        )?)
    }
}

fn audit_mapping(
    mapping: &[u8],
    descriptor: GcCandidateCatalogDescriptor,
) -> Result<(), GcCandidateCatalogStoreError> {
    let mut audit = descriptor.start_audit();
    for ordinal in 0..descriptor.row_count() {
        let offset = descriptor
            .row_offset(ordinal)
            .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
        let offset =
            usize::try_from(offset).map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
        audit.push(exact_range(
            mapping,
            offset,
            GC_CANDIDATE_CATALOG_ROW_BYTES,
        )?)?;
    }
    let rows_end = descriptor
        .rows_end()
        .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
    let rows_end =
        usize::try_from(rows_end).map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
    let footer_offset = usize::try_from(descriptor.footer_offset())
        .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
    if exact_range(mapping, rows_end, footer_offset - rows_end)?
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(GcCandidateCatalogStoreError::IndexCorruption);
    }
    audit.finish()?;
    Ok(())
}

fn exact_range(
    bytes: &[u8],
    offset: usize,
    length: usize,
) -> Result<&[u8], GcCandidateCatalogStoreError> {
    let end = offset
        .checked_add(length)
        .ok_or(GcCandidateCatalogStoreError::CounterOverflow)?;
    bytes
        .get(offset..end)
        .ok_or(GcCandidateCatalogStoreError::IndexCorruption)
}
