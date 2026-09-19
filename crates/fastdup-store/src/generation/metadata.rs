//! Content-identified Metadata object publication, cache-aware reads and canonical names.
use super::metadata_gc::mark_metadata_gc_unclassified;
use super::{GenerationError, GenerationRepository, METADATA_SUFFIX, StagedMetadata};
use crate::StorageIo;
use crate::manifest_tree::ManifestTreeError;
use fastdup_format::{MAX_METADATA_OBJECT_BYTES, METADATA_HEADER_BYTES, MetadataObjectId};
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
            if !self.published_metadata_header_matches(&published_name, encoded)? {
                return Err(GenerationError::MetadataIdentityCollision(object_id));
            }
            // The durable envelope carries this encoding's identity, so the
            // image is the one a reread would have produced.
            self.metadata_cache.admit_validated(object_id, encoded);
            return Ok(StagedMetadata {
                object_id,
                published_new: false,
            });
        }

        let temporary_name = format!(".{}.building", encode_object_id(object_id));
        self.storage
            .create_new_unpublished_image(&temporary_name, encoded)?;
        self.storage.sync_file(&temporary_name)?;
        self.storage
            .publish_noreplace(&temporary_name, &published_name)?;
        mark_metadata_gc_unclassified(&self.metadata_gc_epoch, &self.metadata_gc_delta, object_id);
        // The complete encoder image was validated above. Successful writes,
        // file sync and no-replace publication carry that image forward without
        // rereading it. Cache residency is not a Commit/root durability proof;
        // callers still owe their directory and activation barriers.
        self.metadata_cache.admit_validated(object_id, encoded);
        Ok(StagedMetadata {
            object_id,
            published_new: true,
        })
    }

    /// Compares the durable header of one published name with this encoding.
    ///
    /// The name is the BLAKE3-256 identity of the encoding, and the publication
    /// protocol links it only after the complete image is durable. Since the
    /// appliance owns its Metadata pool, rereading the payload of a name that is
    /// already present would restage nothing: the aligned header alone commits
    /// to the object identity, the payload length, the total file length and the
    /// payload checksum. Damage below that header is what scrub exists for; it
    /// reads every object instead of the incidental subset that a publication
    /// happens to restage.
    fn published_metadata_header_matches(
        &self,
        published_name: &str,
        encoded: &[u8],
    ) -> Result<bool, GenerationError> {
        // An existing image is not evidence from this publication, so the probe
        // observes durable bytes rather than any reusable representation.
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let header = self
            .storage
            .read_exact_at(published_name, 0, METADATA_HEADER_BYTES)?;
        Ok(header == encoded[..METADATA_HEADER_BYTES])
    }

    pub(super) fn read_metadata(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<Arc<Vec<u8>>, GenerationError> {
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
    ) -> Result<Arc<Vec<u8>>, ManifestTreeError> {
        self.check_maintenance()?;
        self.metadata_cache.read(object_id, || {
            // The bound is checked on what was actually read. Measuring the
            // object first only repeats what the read already reports, because
            // no other writer can resize it between the two operations.
            let bytes = self.storage.read(&metadata_name(object_id))?;
            if bytes.len() > MAX_METADATA_OBJECT_BYTES {
                return Err(ManifestTreeError::IdentityMismatch(object_id));
            }
            Ok(bytes)
        })
    }

    pub(super) fn read_manifest_node(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<Arc<Vec<u8>>, ManifestTreeError> {
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
