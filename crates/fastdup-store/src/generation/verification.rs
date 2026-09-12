//! Complete required-Chunk verification through Containers or an independently checked Exact index.
use super::{IndexedRequiredChunkVerifier, RequiredChunkVerifier};
use crate::{ContainerRepository, StorageIo, StoreError};
use std::collections::{BTreeMap, BTreeSet};

impl<I: StorageIo> RequiredChunkVerifier for ContainerRepository<I> {
    fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError> {
        ContainerRepository::verify_required_chunks(self, required)
    }
}

impl<C, X> IndexedRequiredChunkVerifier<C, X> {
    #[must_use]
    pub const fn new(
        containers: ContainerRepository<C>,
        index: crate::ExactIndexGenerationPin<X>,
    ) -> Self {
        Self {
            containers,
            index,
            read_cache: None,
        }
    }

    /// Shares already verified DATA with online readers and ingest. Current
    /// Exact selection and full Location matching remain mandatory; Independent
    /// intent bypasses reuse for recovery and scrub.
    #[must_use]
    pub fn with_verified_read_cache(
        mut self,
        cache: std::sync::Arc<crate::VerifiedReadCache>,
    ) -> Self {
        self.read_cache = Some(cache);
        self
    }
}

impl<C: StorageIo, X: StorageIo> RequiredChunkVerifier for IndexedRequiredChunkVerifier<C, X> {
    fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError> {
        // A missing hint must not restart already verified Records or bypass
        // later valid hints. Retain failed identities, not another full graph.
        let mut missing = BTreeMap::new();
        let mut co_verified = BTreeSet::new();
        for (chunk_id, logical_length) in required {
            if co_verified.remove(chunk_id) {
                continue;
            }
            let Some((_, read)) = self.containers.find_verified_candidate_payload_cached(
                &self.index,
                *chunk_id,
                *logical_length,
                self.read_cache.as_deref(),
            ) else {
                missing.insert(*chunk_id, *logical_length);
                continue;
            };
            let (_, groups) = read.into_parts();
            for payload in groups.iter().flatten() {
                let id = payload.chunk_id();
                if required.get(&id).copied() == u64::try_from(payload.len()).ok() {
                    missing.remove(&id);
                    if id > *chunk_id {
                        co_verified.insert(id);
                    }
                }
            }
            if let Some(cache) = &self.read_cache {
                for group in groups {
                    cache.admit_decoded_group(group);
                }
            }
        }
        self.containers.verify_required_chunks(&missing)
    }
}
