//! Audited immutable-file leases with bounded Direct-I/O reads.
//! Decoded views and encoded ranges use the common application cache.

use fastdup_format::{
    GC_CANDIDATE_CATALOG_HEADER_BYTES, GC_CANDIDATE_CATALOG_ROW_BYTES,
    GcCandidateCatalogDescriptor, GcCandidateCatalogRow,
};

use crate::ImmutableFileLease;
use crate::gc_candidate_catalog::GcCandidateCatalogStoreError;

pub(crate) struct ImmutableGcCandidateCatalog {
    // The lease protects physical identity until the final reader drops.
    lease: ImmutableFileLease,
    descriptor: GcCandidateCatalogDescriptor,
}

impl ImmutableGcCandidateCatalog {
    pub(crate) fn open(
        lease: ImmutableFileLease,
        expected: GcCandidateCatalogDescriptor,
    ) -> Result<Self, GcCandidateCatalogStoreError> {
        let _read_reason =
            crate::MetadataReadScope::enter(crate::MetadataReadReason::GarbageCollection);
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let metadata = lease.file().metadata()?;
        if !metadata.is_file() || lease.logical_len()? != expected.file_length() {
            return Err(GcCandidateCatalogStoreError::IdentityMismatch);
        }

        let footer_offset = usize::try_from(expected.footer_offset())
            .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
        let descriptor = GcCandidateCatalogDescriptor::decode(
            &exact_range(&lease, 0, GC_CANDIDATE_CATALOG_HEADER_BYTES)?,
            &exact_range(&lease, footer_offset, GC_CANDIDATE_CATALOG_HEADER_BYTES)?,
            expected.file_length(),
        )?;
        if descriptor != expected {
            return Err(GcCandidateCatalogStoreError::IdentityMismatch);
        }
        audit_source(&lease, descriptor)?;
        Ok(Self { lease, descriptor })
    }

    pub(crate) const fn descriptor(&self) -> GcCandidateCatalogDescriptor {
        self.descriptor
    }

    pub(crate) fn row(
        &self,
        ordinal: u64,
    ) -> Result<GcCandidateCatalogRow, GcCandidateCatalogStoreError> {
        let _read_reason =
            crate::MetadataReadScope::enter(crate::MetadataReadReason::GarbageCollection);
        let offset = self
            .descriptor
            .row_offset(ordinal)
            .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
        let offset =
            usize::try_from(offset).map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
        Ok(self.descriptor.decode_row(
            ordinal,
            &exact_range(&self.lease, offset, GC_CANDIDATE_CATALOG_ROW_BYTES)?,
        )?)
    }

    pub(crate) fn visit_rows(
        &self,
        mut visit: impl FnMut(GcCandidateCatalogRow) -> Result<(), GcCandidateCatalogStoreError>,
    ) -> Result<(), GcCandidateCatalogStoreError> {
        let _read_reason =
            crate::MetadataReadScope::enter(crate::MetadataReadReason::GarbageCollection);
        let mut ordinal = 0;
        while ordinal < self.descriptor.row_count() {
            let rows = (self.descriptor.row_count() - ordinal).min(4096);
            let offset = self
                .descriptor
                .row_offset(ordinal)
                .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
            let length = usize::try_from(rows)
                .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?
                * GC_CANDIDATE_CATALOG_ROW_BYTES;
            let bytes = self.lease.read_at(offset, length)?;
            for row in bytes.chunks_exact(GC_CANDIDATE_CATALOG_ROW_BYTES) {
                visit(self.descriptor.decode_row(ordinal, row)?)?;
                ordinal += 1;
            }
        }
        Ok(())
    }
}

fn audit_source(
    lease: &ImmutableFileLease,
    descriptor: GcCandidateCatalogDescriptor,
) -> Result<(), GcCandidateCatalogStoreError> {
    let mut audit = descriptor.start_audit();
    let mut ordinal = 0;
    while ordinal < descriptor.row_count() {
        let rows = (descriptor.row_count() - ordinal).min(4096);
        let offset = descriptor
            .row_offset(ordinal)
            .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
        let length = usize::try_from(rows)
            .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?
            * GC_CANDIDATE_CATALOG_ROW_BYTES;
        let bytes = lease.read_at(offset, length)?;
        for row in bytes.chunks_exact(GC_CANDIDATE_CATALOG_ROW_BYTES) {
            audit.push(row)?;
        }
        ordinal += rows;
    }

    let rows_end = descriptor
        .rows_end()
        .ok_or(GcCandidateCatalogStoreError::IndexCorruption)?;
    let rows_end =
        usize::try_from(rows_end).map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
    let footer_offset = usize::try_from(descriptor.footer_offset())
        .map_err(|_| GcCandidateCatalogStoreError::CounterOverflow)?;
    if exact_range(lease, rows_end, footer_offset - rows_end)?
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(GcCandidateCatalogStoreError::IndexCorruption);
    }
    audit.finish()?;
    Ok(())
}

fn exact_range(
    lease: &ImmutableFileLease,
    offset: usize,
    length: usize,
) -> Result<Vec<u8>, GcCandidateCatalogStoreError> {
    Ok(lease.read_at(offset as u64, length)?)
}
