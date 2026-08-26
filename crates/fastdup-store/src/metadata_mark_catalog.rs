use std::fmt;
use std::io;

use fastdup_format::{
    METADATA_MARK_CATALOG_HEADER_BYTES, METADATA_MARK_CATALOG_ROW_BYTES,
    MetadataMarkCatalogDescriptor, MetadataMarkCatalogError as MetadataMarkFormatError,
    MetadataMarkCatalogRunKind, MetadataMarkCatalogStreamEncoder, MetadataObjectId,
};

use crate::StorageIo;

pub(crate) use fastdup_format::metadata_mark_commit_binding as commit_binding;

const ROW_WRITE_BATCH_BYTES: usize = 8_192 * METADATA_MARK_CATALOG_ROW_BYTES;
const PREFIX: &str = "metadata-mark-catalog-";
const SUFFIX: &str = ".run";

pub(crate) struct PreparedMetadataMarkCatalog {
    descriptor: MetadataMarkCatalogDescriptor,
    temporary_name: String,
    published_name: String,
}

impl PreparedMetadataMarkCatalog {
    pub(crate) fn publish<I: StorageIo>(
        self,
        storage: &I,
    ) -> Result<MetadataMarkCatalogDescriptor, MetadataMarkCatalogError> {
        match storage.publish_noreplace(&self.temporary_name, &self.published_name) {
            Ok(()) => Ok(self.descriptor),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let observed = audit_named(storage, &self.published_name)?;
                if observed != self.descriptor {
                    return Err(MetadataMarkFormatError::IdentityCollision.into());
                }
                Ok(observed)
            }
            Err(error) => Err(error.into()),
        }
    }
}

pub(crate) fn prepare<I: StorageIo>(
    storage: &I,
    generation: u64,
    commit_binding: [u8; 32],
    rows: impl IntoIterator<Item = MetadataObjectId>,
    row_count: u64,
) -> Result<PreparedMetadataMarkCatalog, MetadataMarkCatalogError> {
    prepare_run(
        storage,
        MetadataMarkCatalogRunKind::Snapshot,
        generation,
        0,
        commit_binding,
        rows,
        row_count,
    )
}

pub(crate) fn prepare_addition<I: StorageIo>(
    storage: &I,
    generation: u64,
    base_generation: u64,
    commit_binding: [u8; 32],
    rows: impl IntoIterator<Item = MetadataObjectId>,
    row_count: u64,
) -> Result<PreparedMetadataMarkCatalog, MetadataMarkCatalogError> {
    prepare_run(
        storage,
        MetadataMarkCatalogRunKind::Addition,
        generation,
        base_generation,
        commit_binding,
        rows,
        row_count,
    )
}

#[allow(clippy::too_many_arguments)]
fn prepare_run<I: StorageIo>(
    storage: &I,
    run_kind: MetadataMarkCatalogRunKind,
    generation: u64,
    base_generation: u64,
    commit_binding: [u8; 32],
    rows: impl IntoIterator<Item = MetadataObjectId>,
    row_count: u64,
) -> Result<PreparedMetadataMarkCatalog, MetadataMarkCatalogError> {
    let temporary_name = temporary_name(generation);
    let published_name = published_name(generation);
    match storage.create_new(&temporary_name) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            storage.set_len(&temporary_name, 0)?;
        }
        Err(error) => return Err(error.into()),
    }

    let mut encoder = match run_kind {
        MetadataMarkCatalogRunKind::Snapshot => {
            MetadataMarkCatalogStreamEncoder::new(generation, commit_binding, row_count)?
        }
        MetadataMarkCatalogRunKind::Addition => MetadataMarkCatalogStreamEncoder::new_addition(
            generation,
            base_generation,
            commit_binding,
            row_count,
        )?,
    };
    let mut batch = Vec::new();
    batch
        .try_reserve_exact(ROW_WRITE_BATCH_BYTES)
        .map_err(|_| MetadataMarkFormatError::OutOfMemory)?;
    let mut batch_offset = u64::try_from(METADATA_MARK_CATALOG_HEADER_BYTES)
        .expect("ASSERT: Metadata mark header size fits u64");
    for object_id in rows {
        let (offset, bytes) = encoder.push(object_id)?;
        let expected_offset = batch_offset
            .checked_add(
                u64::try_from(batch.len())
                    .map_err(|_| MetadataMarkFormatError::ArithmeticOverflow)?,
            )
            .ok_or(MetadataMarkFormatError::ArithmeticOverflow)?;
        if offset != expected_offset {
            return Err(MetadataMarkFormatError::InvalidEnvelope.into());
        }
        batch.extend_from_slice(&bytes);
        if batch.len() == ROW_WRITE_BATCH_BYTES {
            storage.write_at(&temporary_name, batch_offset, &batch)?;
            batch_offset = batch_offset
                .checked_add(
                    u64::try_from(batch.len())
                        .map_err(|_| MetadataMarkFormatError::ArithmeticOverflow)?,
                )
                .ok_or(MetadataMarkFormatError::ArithmeticOverflow)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        storage.write_at(&temporary_name, batch_offset, &batch)?;
    }
    let (descriptor, header, footer) = encoder.finish()?;
    storage.set_len(&temporary_name, descriptor.file_length())?;
    storage.write_at(&temporary_name, 0, &header)?;
    storage.write_at(&temporary_name, descriptor.footer_offset(), &footer)?;
    let observed = audit_named(storage, &temporary_name)?;
    if observed != descriptor {
        return Err(MetadataMarkFormatError::IdentityCollision.into());
    }
    storage.sync_file(&temporary_name)?;
    Ok(PreparedMetadataMarkCatalog {
        descriptor,
        temporary_name,
        published_name,
    })
}

pub(crate) fn audit_named<I: StorageIo>(
    storage: &I,
    name: &str,
) -> Result<MetadataMarkCatalogDescriptor, MetadataMarkCatalogError> {
    let file_length = storage.object_len(name)?;
    if file_length
        < 2 * u64::try_from(METADATA_MARK_CATALOG_HEADER_BYTES)
            .expect("ASSERT: Metadata mark header size fits u64")
    {
        return Err(MetadataMarkFormatError::InvalidEnvelope.into());
    }
    let header = storage.read_exact_at(name, 0, METADATA_MARK_CATALOG_HEADER_BYTES)?;
    let footer_offset = file_length
        .checked_sub(
            u64::try_from(METADATA_MARK_CATALOG_HEADER_BYTES)
                .expect("ASSERT: Metadata mark header size fits u64"),
        )
        .ok_or(MetadataMarkFormatError::InvalidEnvelope)?;
    let footer = storage.read_exact_at(name, footer_offset, METADATA_MARK_CATALOG_HEADER_BYTES)?;
    let descriptor = MetadataMarkCatalogDescriptor::decode(&header, &footer, file_length)?;
    if descriptor.footer_offset() != footer_offset {
        return Err(MetadataMarkFormatError::InvalidEnvelope.into());
    }

    let mut audit = descriptor.start_audit();
    let mut ordinal = 0_u64;
    while ordinal < descriptor.row_count() {
        let remaining = descriptor.row_count() - ordinal;
        let batch_rows = remaining.min(
            u64::try_from(ROW_WRITE_BATCH_BYTES / METADATA_MARK_CATALOG_ROW_BYTES)
                .expect("ASSERT: Metadata mark batch row count fits u64"),
        );
        let length = usize::try_from(
            batch_rows
                .checked_mul(
                    u64::try_from(METADATA_MARK_CATALOG_ROW_BYTES)
                        .expect("ASSERT: Metadata mark row size fits u64"),
                )
                .ok_or(MetadataMarkFormatError::ArithmeticOverflow)?,
        )
        .map_err(|_| MetadataMarkFormatError::ArithmeticOverflow)?;
        let offset = descriptor
            .row_offset(ordinal)
            .ok_or(MetadataMarkFormatError::RowCountMismatch)?;
        let bytes = storage.read_exact_at(name, offset, length)?;
        for row in bytes.chunks_exact(METADATA_MARK_CATALOG_ROW_BYTES) {
            audit.push(row)?;
            ordinal += 1;
        }
    }
    audit.finish()?;
    let rows_end = descriptor
        .rows_end()
        .ok_or(MetadataMarkFormatError::ArithmeticOverflow)?;
    let padding_length = usize::try_from(descriptor.footer_offset() - rows_end)
        .map_err(|_| MetadataMarkFormatError::ArithmeticOverflow)?;
    if padding_length != 0
        && storage
            .read_exact_at(name, rows_end, padding_length)?
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(MetadataMarkFormatError::NonzeroPadding.into());
    }
    Ok(descriptor)
}

pub(crate) fn parse_generation(name: &str) -> Option<u64> {
    let generation = name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
    if generation.len() != 20 || !generation.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    generation.parse().ok().filter(|value| *value != 0)
}

pub(crate) fn is_published_name(name: &str) -> bool {
    name.starts_with(PREFIX) && name.ends_with(SUFFIX)
}

fn published_name(generation: u64) -> String {
    format!("{PREFIX}{generation:020}{SUFFIX}")
}

fn temporary_name(generation: u64) -> String {
    format!(".{PREFIX}{generation:020}.building")
}

#[derive(Debug)]
pub(crate) enum MetadataMarkCatalogError {
    Io(io::Error),
    Format(MetadataMarkFormatError),
}

impl fmt::Display for MetadataMarkCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for MetadataMarkCatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Format(error) => Some(error),
        }
    }
}

impl From<io::Error> for MetadataMarkCatalogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<MetadataMarkFormatError> for MetadataMarkCatalogError {
    fn from(error: MetadataMarkFormatError) -> Self {
        Self::Format(error)
    }
}
