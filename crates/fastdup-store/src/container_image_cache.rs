//! Bounded underfilled Container image pool in the unified read cache.
use crate::{
    ReadCacheClass, ReadCacheKey, ReadCacheNamespace, ReadIntent, ReadIntentScope, read_intent,
};
use fastdup_format::ContainerId;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ContainerImageCache {
    cache: ReadCacheNamespace,
}

impl ContainerImageCache {
    pub(crate) fn system() -> Self {
        Self {
            cache: ReadCacheNamespace::system(ReadCacheClass::ContainerImage),
        }
    }

    #[cfg(test)]
    fn limited(target: u64) -> Self {
        Self {
            cache: ReadCacheNamespace::isolated(ReadCacheClass::ContainerImage, target),
        }
    }

    /// Returns resident bytes only for Demand reads. Scan and Independent
    /// reads must observe current durable bytes without consuming this cache.
    pub(crate) fn get(&self, container_id: ContainerId) -> Option<Arc<Vec<u8>>> {
        if ReadIntentScope::current() != ReadIntent::Demand {
            return None;
        }
        self.cache.get(image_key(container_id))
    }

    /// Offers one image that the caller proved belongs to the published ID.
    /// Admission may decline under Scan/Independent intent or memory pressure.
    pub(crate) fn admit_validated(&self, container_id: ContainerId, encoded: Vec<u8>) {
        if read_intent::bypass_admission() {
            return;
        }
        let charge = (encoded.capacity() + size_of::<Vec<u8>>()) as u64;
        let length = encoded.len() as u64;
        self.cache
            .insert(image_key(container_id), Arc::new(encoded), charge, length);
    }

    pub(crate) fn forget(&self, container_id: ContainerId) {
        self.cache.remove(image_key(container_id));
    }
}

fn image_key(container_id: ContainerId) -> ReadCacheKey {
    let mut identity = [0_u8; 32];
    identity[..16].copy_from_slice(&container_id.bytes());
    ReadCacheKey {
        identity,
        ordinal: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReadIntent, ReadIntentScope};

    fn container_id(byte: u8) -> ContainerId {
        ContainerId::new([byte; 16]).expect("test identity is nonzero")
    }

    #[test]
    fn validated_images_admit_get_and_forget() {
        let cache = ContainerImageCache::limited(1024 * 1024);
        let id = container_id(7);
        assert!(cache.get(id).is_none());
        cache.admit_validated(id, b"container image".to_vec());
        let cached = cache.get(id).expect("admitted image is present");
        assert_eq!(cached.as_slice(), b"container image");
        cache.forget(id);
        assert!(cache.get(id).is_none());
    }

    #[test]
    fn scan_intent_does_not_admit_images() {
        let cache = ContainerImageCache::limited(1024 * 1024);
        let id = container_id(8);
        let _scope = ReadIntentScope::enter(ReadIntent::Scan);
        cache.admit_validated(id, b"scan image".to_vec());
        assert!(cache.get(id).is_none());
    }
}
