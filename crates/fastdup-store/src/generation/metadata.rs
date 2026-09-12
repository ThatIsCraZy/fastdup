//! Content-identified Metadata object publication, cache-aware reads and canonical names.
use super::metadata_gc::mark_metadata_gc_unclassified;
use super::{
    GenerationError, GenerationRepository, MAX_METADATA_OBJECT_BYTES_U64, METADATA_SUFFIX,
    StagedMetadata, WRITE_BLOCK_BYTES,
};
use crate::StorageIo;
use crate::manifest_tree::ManifestTreeError;
use fastdup_format::{MAX_METADATA_OBJECT_BYTES, MetadataObjectId};
use std::sync::Arc;

impl<I: StorageIo> GenerationRepository<I> {
    pub(super) fn stage_metadata(
        &self,
        encoded: &[u8],
    ) -> Result<MetadataObjectId, GenerationError> {
        Ok(self.stage_metadata_with_status(encoded)?.object_id)
    }

    pub(super) fn stage_metadata_with_status(
        &self,
        encoded: &[u8],
    ) -> Result<StagedMetadata, GenerationError> {
        if encoded.len() > MAX_METADATA_OBJECT_BYTES {
            return Err(GenerationError::MetadataTooLarge);
        }
        let object_id = MetadataObjectId::from_encoded(encoded)?;
        let published_name = metadata_name(object_id);
        if self.storage.exists(&published_name)? {
            let existing = self.storage.read(&published_name)?;
            let existing_id = MetadataObjectId::from_encoded(&existing)?;
            if existing_id != object_id || existing != encoded {
                return Err(GenerationError::MetadataIdentityCollision(object_id));
            }
            return Ok(StagedMetadata {
                object_id,
                published_new: false,
            });
        }

        let temporary_name = format!(".{}.building", encode_object_id(object_id));
        self.storage.create_new(&temporary_name)?;
        for (ordinal, block) in encoded.chunks(WRITE_BLOCK_BYTES).enumerate() {
            let offset = ordinal
                .checked_mul(WRITE_BLOCK_BYTES)
                .ok_or(GenerationError::MetadataTooLarge)?;
            self.storage.write_at(
                &temporary_name,
                u64::try_from(offset).map_err(|_| GenerationError::MetadataTooLarge)?,
                block,
            )?;
        }
        self.storage.set_len(
            &temporary_name,
            u64::try_from(encoded.len()).map_err(|_| GenerationError::MetadataTooLarge)?,
        )?;
        let reread = self.storage.read(&temporary_name)?;
        if reread != encoded || MetadataObjectId::from_encoded(&reread)? != object_id {
            return Err(GenerationError::PublishVerificationMismatch);
        }
        self.storage.sync_file(&temporary_name)?;
        self.storage
            .publish_noreplace(&temporary_name, &published_name)?;
        mark_metadata_gc_unclassified(&self.metadata_gc_epoch, &self.metadata_gc_delta, object_id);
        Ok(StagedMetadata {
            object_id,
            published_new: true,
        })
    }

    pub(super) fn read_metadata(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<Vec<u8>, GenerationError> {
        let _read_reason = crate::MetadataReadScope::enter(crate::MetadataReadReason::Namespace);
        self.read_metadata_bytes(object_id)
            .map_err(|error| match error {
                ManifestTreeError::IdentityMismatch(_) => {
                    GenerationError::MetadataIdentityCollision(object_id)
                }
                other => other.into(),
            })
    }

    fn read_metadata_bytes(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<Vec<u8>, ManifestTreeError> {
        self.check_maintenance()?;
        let bytes = self.metadata_cache.read(object_id, || {
            let name = metadata_name(object_id);
            let length = self.storage.object_len(&name)?;
            if length > MAX_METADATA_OBJECT_BYTES_U64 {
                return Err(ManifestTreeError::IdentityMismatch(object_id));
            }
            let bytes = self.storage.read(&name)?;
            if u64::try_from(bytes.len()) != Ok(length) {
                return Err(ManifestTreeError::IdentityMismatch(object_id));
            }
            Ok(bytes)
        })?;
        Ok(Arc::unwrap_or_clone(bytes))
    }

    pub(super) fn read_manifest_node(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<Vec<u8>, ManifestTreeError> {
        let _read_reason = crate::MetadataReadScope::enter(crate::MetadataReadReason::Manifest);
        self.read_metadata_bytes(object_id)
    }
}

pub(super) fn metadata_name(object_id: MetadataObjectId) -> String {
    format!("{}{}", encode_object_id(object_id), METADATA_SUFFIX)
}

pub(super) fn parse_metadata_name(name: &str) -> Result<Option<MetadataObjectId>, GenerationError> {
    let Some(encoded) = name.strip_suffix(METADATA_SUFFIX) else {
        return Ok(None);
    };
    if encoded.len() != 64 {
        return Err(GenerationError::InvalidMetadataObjectName(name.to_owned()));
    }
    let mut bytes = [0_u8; 32];
    for (output, pair) in bytes.iter_mut().zip(encoded.as_bytes().chunks_exact(2)) {
        let (Some(high), Some(low)) = (decode_hex_nibble(pair[0]), decode_hex_nibble(pair[1]))
        else {
            return Err(GenerationError::InvalidMetadataObjectName(name.to_owned()));
        };
        *output = (high << 4) | low;
    }
    MetadataObjectId::new(bytes)
        .map(Some)
        .ok_or_else(|| GenerationError::InvalidMetadataObjectName(name.to_owned()))
}

const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn encode_object_id(object_id: MetadataObjectId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in object_id.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
