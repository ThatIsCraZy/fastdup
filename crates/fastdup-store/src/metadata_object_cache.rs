//! Content verification adapter for Metadata bytes in the unified read cache.
use crate::manifest_tree::ManifestTreeError;
use crate::{ReadCacheClass, ReadCacheKey, ReadCacheNamespace};
use fastdup_format::MetadataObjectId;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct MetadataObjectCache {
    cache: ReadCacheNamespace,
}

impl MetadataObjectCache {
    pub(crate) fn system() -> Self {
        Self {
            cache: ReadCacheNamespace::system(ReadCacheClass::MetadataObject),
        }
    }

    pub(crate) fn read<F>(
        &self,
        id: MetadataObjectId,
        read: F,
    ) -> Result<Arc<Vec<u8>>, ManifestTreeError>
    where
        F: FnOnce() -> Result<Vec<u8>, ManifestTreeError>,
    {
        let key = ReadCacheKey {
            identity: id.bytes(),
            ordinal: 0,
        };
        if let Some(bytes) = self.cache.get(key) {
            return Ok(bytes);
        }
        let bytes = read()?;
        if MetadataObjectId::from_encoded(&bytes)? != id {
            return Err(ManifestTreeError::IdentityMismatch(id));
        }
        let charge = (bytes.capacity() + size_of::<Vec<u8>>()) as u64;
        let bytes = Arc::new(bytes);
        self.cache
            .insert(key, Arc::clone(&bytes), charge, bytes.len() as u64);
        Ok(bytes)
    }

    pub(crate) fn invalidate(&self, id: MetadataObjectId) {
        self.cache.remove(ReadCacheKey {
            identity: id.bytes(),
            ordinal: 0,
        });
    }

    /// Offer a successfully published image whose full encoded identity the
    /// caller has already validated. This conveys bytes, never durability or
    /// liveness authority. Scan/Independent intent and pressure may decline it.
    pub(crate) fn admit_validated(&self, id: MetadataObjectId, encoded: &[u8]) {
        if crate::read_intent::bypass_admission() {
            return;
        }
        let bytes = encoded.to_vec();
        let charge = (bytes.capacity() + size_of::<Vec<u8>>()) as u64;
        self.cache.insert(
            ReadCacheKey {
                identity: id.bytes(),
                ordinal: 0,
            },
            Arc::new(bytes),
            charge,
            encoded.len() as u64,
        );
    }

    #[cfg(test)]
    pub(crate) fn limited(target: u64) -> Self {
        Self {
            cache: ReadCacheNamespace::isolated(ReadCacheClass::MetadataObject, target),
        }
    }
}

/// Independent verification bypasses every reusable representation, including
/// backend ranges. The guard remains synchronous and restores nested scopes.
pub(crate) struct IndependentRead {
    _scope: crate::ReadIntentScope,
}
impl IndependentRead {
    pub(crate) fn enter() -> Self {
        Self {
            _scope: crate::ReadIntentScope::enter(crate::ReadIntent::Independent),
        }
    }
}

#[cfg(test)]
#[path = "metadata_object_cache_tests.rs"]
mod tests;
