//! Bounded content verification when persistent Exact hints are unavailable.
use std::collections::BTreeMap;

use fastdup_format::{ChunkId, VerifiedChunkPayload};

use crate::{
    ContainerBaseResolver, ContainerRepository, StorageIo, StoreError, parse_published_name,
};

impl<I: StorageIo> ContainerRepository<I> {
    /// One namespace pass for all missing identities. Each local Index is read
    /// once; each selected Record discharges all verified required siblings.
    pub(crate) fn scan_required_records(
        &self,
        required: &BTreeMap<ChunkId, u64>,
        mut consume: impl FnMut(Vec<VerifiedChunkPayload>),
    ) -> Result<(), StoreError> {
        if required.is_empty() {
            return Ok(());
        }
        let mut missing = required.clone();
        let mut names = self.storage.list_names()?;
        names.sort_unstable();
        for name in names {
            let Some(id) = parse_published_name(&name)? else {
                continue;
            };
            if !self.selectable_container(id) {
                continue;
            }
            let envelope = self.read_recovery_envelope(&name, id)?;
            let range = envelope.recovery_index_range()?;
            let index_bytes =
                self.read_storage_range_chunked(&name, range.offset(), range.length())?;
            let index = envelope.verify_recovery_index(&index_bytes)?;
            let mut bases = ContainerBaseResolver::new(self);
            for candidate in index.candidates() {
                if missing.get(&candidate.chunk_id()).copied()
                    != Some(u64::from(candidate.logical_length()))
                {
                    continue;
                }
                let range = candidate.record_range()?;
                let bytes = self
                    .storage
                    .read_exact_at(&name, range.offset(), range.length())?;
                let mut resolver_error = None;
                let decoded =
                    index.decode_candidate_with_resolver(candidate, &bytes, &mut |dependency| {
                        bases.resolve(dependency).map_err(|error| {
                            resolver_error = Some(error);
                            fastdup_format::FormatError::DependentBaseRequired
                        })
                    });
                if let Some(error) = resolver_error {
                    return Err(error);
                }
                let payloads = decoded?;
                for payload in &payloads {
                    if missing.get(&payload.chunk_id()).copied()
                        == u64::try_from(payload.len()).ok()
                    {
                        missing.remove(&payload.chunk_id());
                    }
                }
                consume(payloads);
                if missing.is_empty() {
                    return Ok(());
                }
            }
        }
        let (&chunk_id, &logical_length) = missing
            .first_key_value()
            .expect("ASSERT: unresolved scan retains a missing identity");
        Err(StoreError::MissingVerifiedChunk {
            chunk_id,
            logical_length,
        })
    }
}
