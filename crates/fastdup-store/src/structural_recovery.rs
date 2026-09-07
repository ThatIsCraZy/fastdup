//! Startup-only structure checks. Never implements `RequiredChunkVerifier` and
//! never manufactures a verified payload or deletion capability.
use crate::{ContainerRepository, StorageIo, StoreError, parse_published_name};
use fastdup_format::{
    ChunkId, ContainerId, ContainerStructure, FOOTER_BYTES, HEADER_BYTES, MAX_CONTAINER_BYTES,
};
use std::collections::{BTreeMap, BTreeSet};

/// Outstanding DATA identities from one selected committed Metadata graph.
/// This is scrub work, never a content proof or deletion capability. Only a
/// completely verified selectable Container can discharge an identity.
#[derive(Debug)]
#[must_use = "the initial scrub must check the selected commit's DATA requirements"]
pub struct PendingDataVerification {
    required: BTreeMap<ChunkId, u64>,
}

impl PendingDataVerification {
    pub(crate) fn new(required: BTreeMap<ChunkId, u64>) -> Self {
        Self { required }
    }

    #[must_use]
    pub fn remaining_chunks(&self) -> usize {
        self.required.len()
    }

    /// Confirms that the scrub found all required identities, including those
    /// whose Container files are missing from the directory snapshot entirely.
    ///
    /// # Errors
    /// Returns the first required Chunk not verified in a selectable Container.
    pub fn finish(self) -> Result<(), StoreError> {
        if let Some((&chunk_id, &logical_length)) = self.required.first_key_value() {
            return Err(StoreError::MissingVerifiedChunk {
                chunk_id,
                logical_length,
            });
        }
        Ok(())
    }

    fn observe(&mut self, structure: &ContainerStructure) {
        for chunk in structure.chunks() {
            if self.required.get(&chunk.chunk_id).copied() == Some(u64::from(chunk.logical_length))
            {
                self.required.remove(&chunk.chunk_id);
            }
        }
    }
}

impl<I: StorageIo> ContainerRepository<I> {
    /// Independently scrubs one complete Container without retaining its decoded
    /// corpus. Index hints accelerate Base lookup; unavailable hints fall back
    /// to the ordinary independently verified Base discovery path.
    ///
    /// # Errors
    /// Returns payload, dependency, format, identity, or storage failures.
    pub fn scrub_container<X: StorageIo>(
        &self,
        id: ContainerId,
        index: Option<&crate::ActivatedExactIndex<X>>,
    ) -> Result<u64, StoreError> {
        self.scrub_container_using(id, index, None)
    }

    /// Fully scrubs a Container and discharges matching startup requirements.
    /// Dependent Bases must independently verify before any identity is counted.
    ///
    /// # Errors
    /// Returns the same failures as `scrub_container`.
    pub fn scrub_container_for_recovery<X: StorageIo>(
        &self,
        id: ContainerId,
        index: Option<&crate::ActivatedExactIndex<X>>,
        required: &mut PendingDataVerification,
    ) -> Result<u64, StoreError> {
        self.scrub_container_using(id, index, Some(required))
    }

    fn scrub_container_using<X: StorageIo>(
        &self,
        id: ContainerId,
        index: Option<&crate::ActivatedExactIndex<X>>,
        required: Option<&mut PendingDataVerification>,
    ) -> Result<u64, StoreError> {
        let bytes = self.storage.read(&crate::published_name(id))?;
        let mut fallback = crate::ContainerBaseResolver::new(self);
        let mut resolver_error = None;
        let mut resolve = |dependency: fastdup_format::DependentDependency| {
            if let Some(index) = index
                && let Some(base) = self.find_verified_independent_base_with_index(
                    index,
                    dependency.chunk_id(),
                    dependency.logical_length(),
                )
            {
                return Ok(base);
            }
            fallback.resolve(dependency).map_err(|error| {
                resolver_error = Some(error);
                fastdup_format::FormatError::DependentBaseRequired
            })
        };
        let verified = fastdup_format::SealedContainer::verify_publication_with_dependent_resolver(
            &bytes,
            &mut resolve,
        );
        if let Some(error) = resolver_error {
            return Err(error);
        }
        if verified?.header().container_id() != id {
            return Err(StoreError::PublishVerificationMismatch);
        }
        if let Some(required) = required
            && self.selectable_container(id)
        {
            // All payloads and Bases have already verified. Extract identities
            // from that same immutable image, with no extra disk reads.
            let footer_bytes = usize::try_from(FOOTER_BYTES)
                .map_err(|_| fastdup_format::FormatError::ArithmeticOverflow)?;
            let structure = ContainerStructure::read(
                &bytes[..HEADER_BYTES],
                &bytes[bytes.len() - footer_bytes..],
                bytes.len() as u64,
                |offset, length| {
                    let start = usize::try_from(offset)
                        .map_err(|_| fastdup_format::FormatError::ArithmeticOverflow)?;
                    let end = start
                        .checked_add(length)
                        .ok_or(fastdup_format::FormatError::ArithmeticOverflow)?;
                    bytes
                        .get(start..end)
                        .map(<[u8]>::to_vec)
                        .ok_or(fastdup_format::FormatError::InvalidContainerLayout)
                },
            )?;
            required.observe(&structure);
        }
        Ok(bytes.len() as u64)
    }

    /// Validates all metadata of one immutable Container, without payload reads.
    ///
    /// # Errors
    /// Returns naming, length, seal, Record metadata, Index or commitment errors.
    pub fn read_structure(&self, id: ContainerId) -> Result<ContainerStructure, StoreError> {
        let name = crate::published_name(id);
        let length = self.storage.object_len(&name)?;
        if !(HEADER_BYTES as u64 + FOOTER_BYTES..=MAX_CONTAINER_BYTES).contains(&length) {
            return Err(fastdup_format::FormatError::InvalidContainerLength(
                usize::try_from(length).unwrap_or(usize::MAX),
            )
            .into());
        }
        let header = self.storage.read_structure_at(&name, 0, HEADER_BYTES)?;
        let footer = self
            .storage
            .read_structure_at(&name, length - FOOTER_BYTES, HEADER_BYTES)?;
        let structure = ContainerStructure::read(&header, &footer, length, |offset, count| {
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(count)
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
            while bytes.len() < count {
                let length = (count - bytes.len()).min(crate::MAX_STORAGE_RANGE_BYTES);
                let part =
                    self.storage
                        .read_structure_at(&name, offset + bytes.len() as u64, length)?;
                if part.len() != length {
                    return Err(StoreError::from(std::io::Error::from(
                        std::io::ErrorKind::UnexpectedEof,
                    )));
                }
                bytes.extend_from_slice(&part);
            }
            Ok::<_, StoreError>(bytes)
        })?;
        if structure.descriptor().container_id() != id {
            return Err(StoreError::PublishedIdentityMismatch {
                name: id,
                header: structure.descriptor().container_id(),
            });
        }
        Ok(structure)
    }

    /// Returns one immutable name snapshot for a startup/background verification
    /// pass. It grants no content or GC authority; concurrent deletion must be
    /// excluded by the owning runtime until the pass completes.
    ///
    /// # Errors
    /// Returns directory or canonical-name errors.
    pub fn recovery_container_snapshot(&self) -> Result<Vec<ContainerId>, StoreError> {
        self.published_container_names()?
            .iter()
            .map(|name| parse_published_name(name)?.ok_or(StoreError::PublishVerificationMismatch))
            .collect()
    }

    pub(crate) fn verify_required_chunk_structure(
        &self,
        required: &BTreeMap<ChunkId, u64>,
    ) -> Result<(), StoreError> {
        let names = self.recovery_container_snapshot()?;
        let mut missing = required.clone();
        // Only the selected graph's identities and candidate Bases are retained;
        // never an inventory of every Chunk in the physical pool.
        let mut dependent = BTreeMap::<ChunkId, BTreeSet<(ChunkId, u32)>>::new();
        for id in &names {
            if !self.selectable_container(*id) {
                continue;
            }
            let structure = self.read_structure(*id)?;
            for chunk in structure.chunks() {
                if missing.get(&chunk.chunk_id).copied() != Some(u64::from(chunk.logical_length)) {
                    continue;
                }
                if let Some(base) = chunk.dependency {
                    dependent.entry(chunk.chunk_id).or_default().insert(base);
                } else {
                    missing.remove(&chunk.chunk_id);
                    dependent.remove(&chunk.chunk_id);
                }
            }
        }
        let mut bases: BTreeSet<_> = dependent.values().flatten().copied().collect();
        let mut available = BTreeSet::new();
        if !bases.is_empty() {
            for id in names {
                if !self.selectable_container(id) {
                    continue;
                }
                let structure = self.read_structure(id)?;
                for chunk in structure.chunks() {
                    let key = (chunk.chunk_id, chunk.logical_length);
                    if chunk.dependency.is_none() && bases.remove(&key) {
                        available.insert(key);
                    }
                }
                if bases.is_empty() {
                    break;
                }
            }
        }
        missing.retain(|id, _| {
            !dependent
                .get(id)
                .is_some_and(|choices| choices.iter().any(|base| available.contains(base)))
        });
        if let Some((&chunk_id, &logical_length)) = missing.first_key_value() {
            return Err(StoreError::MissingVerifiedChunk {
                chunk_id,
                logical_length,
            });
        }
        Ok(())
    }
}
