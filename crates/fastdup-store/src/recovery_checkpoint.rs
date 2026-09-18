use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use fastdup_format::{
    COMMIT_RECORD_BYTES, ChunkId, CommitFormatError, CommitRecord, ManifestExtent,
    MetadataFormatError, MetadataObjectId, NamespaceGraphRoot, NamespaceRoot,
    RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES, RECOVERY_CHECKPOINT_FOOTER_BYTES,
    RECOVERY_CHECKPOINT_HEAD_BYTES, RECOVERY_CHECKPOINT_HEADER_BYTES, RecoveryCheckpointDescriptor,
    RecoveryCheckpointEntryHeader, RecoveryCheckpointFormatError, RecoveryCheckpointHeadRecord,
};

use crate::generation::{
    GenerationError, GenerationRepository, RecoveredGeneration, RequiredChunkVerifier,
};
use crate::immutable_write::ImmutableWriteBuffer;
use crate::manifest_tree::{ManifestTreeError, scan_manifest_tree};
use crate::{MAX_STORAGE_RANGE_BYTES, StorageIo, StoreError};

const CHECKPOINT_PREFIX: &str = "recovery-checkpoint.";
const CHECKPOINT_SUFFIX: &str = ".fdrc";
const HEAD_NAMES: [&str; 2] = ["recovery-checkpoint.0.head", "recovery-checkpoint.1.head"];
const CHECKPOINT_PROTECTED_CHUNK_CACHE_LIMIT: usize = 8_388_608;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct CheckpointProtectedIdentity {
    generation: u64,
    file_length: u64,
    body_hash: [u8; 32],
}

struct ProtectedChunkEntry {
    chunk_id: ChunkId,
    logical_length: u32,
}

struct CheckpointProtectedChunkSet {
    entries: Vec<ProtectedChunkEntry>,
}

impl CheckpointProtectedChunkSet {
    fn matched(&self, selected: &BTreeSet<ChunkId>) -> BTreeMap<ChunkId, u64> {
        let mut matched = BTreeMap::new();
        for chunk_id in selected {
            if let Ok(index) = self
                .entries
                .binary_search_by(|entry| entry.chunk_id.cmp(chunk_id))
            {
                matched.insert(*chunk_id, u64::from(self.entries[index].logical_length));
            }
        }
        matched
    }
}

#[derive(Default)]
struct CheckpointProtectedChunkCacheState {
    entries: HashMap<CheckpointProtectedIdentity, Arc<CheckpointProtectedChunkSet>>,
    retained_chunks: usize,
}

#[derive(Default)]
pub(crate) struct CheckpointProtectedChunkCache {
    state: Mutex<CheckpointProtectedChunkCacheState>,
}

impl CheckpointProtectedChunkCache {
    fn retain_current(&self, active: &HashSet<CheckpointProtectedIdentity>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state
            .entries
            .retain(|identity, _| active.contains(identity));
        state.retained_chunks = state
            .entries
            .values()
            .map(|entry| entry.entries.len())
            .sum::<usize>();
    }

    fn get(
        &self,
        identity: &CheckpointProtectedIdentity,
    ) -> Option<Arc<CheckpointProtectedChunkSet>> {
        self.state.lock().ok()?.entries.get(identity).cloned()
    }

    fn invalidate(&self, identity: &CheckpointProtectedIdentity) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(entry) = state.entries.remove(identity) {
            state.retained_chunks = state.retained_chunks.saturating_sub(entry.entries.len());
        }
    }

    fn insert(
        &self,
        identity: CheckpointProtectedIdentity,
        entries: Vec<ProtectedChunkEntry>,
    ) -> Option<Arc<CheckpointProtectedChunkSet>> {
        if entries.len() > CHECKPOINT_PROTECTED_CHUNK_CACHE_LIMIT {
            return None;
        }
        let inserted = Arc::new(CheckpointProtectedChunkSet { entries });
        let Ok(mut state) = self.state.lock() else {
            return Some(inserted.clone());
        };
        if let Some(previous) = state.entries.remove(&identity) {
            state.retained_chunks = state.retained_chunks.saturating_sub(previous.entries.len());
        }
        let retained = state.retained_chunks.saturating_add(inserted.entries.len());
        if retained > CHECKPOINT_PROTECTED_CHUNK_CACHE_LIMIT {
            return Some(inserted);
        }
        state.retained_chunks = retained;
        state.entries.insert(identity, inserted.clone());
        Some(inserted)
    }

    #[cfg(test)]
    fn cached_checkpoint_count_for_test(&self) -> usize {
        self.state.lock().map_or(0, |state| state.entries.len())
    }
}

impl fmt::Debug for CheckpointProtectedChunkCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointProtectedChunkCache")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct RecoveryCheckpointRepository<I> {
    storage: I,
    // One successful publication receipt, shared by this destination owner.
    publication: Arc<Mutex<Option<(CommitRecord, RecoveryCheckpointSummary)>>>,
}

impl<I: StorageIo> RecoveryCheckpointRepository<I> {
    #[must_use]
    pub fn new(storage: I) -> Self {
        Self {
            storage,
            publication: Arc::new(Mutex::new(None)),
        }
    }

    /// Publishes the newest wholly verified Commit graph as one immutable,
    /// self-contained DATA-tier Recovery Checkpoint.
    ///
    /// The source repository briefly serializes candidate selection and root
    /// pinning with Commit and Metadata GC. Graph enumeration, DATA verification,
    /// and checkpoint I/O run after those locks are released while the selected
    /// root remains pinned. This operation is never called from the Commit hot
    /// loop.
    ///
    /// # Errors
    ///
    /// Returns a source-graph, DATA-verification, format, identity, storage, or
    /// durability error without selecting a partial checkpoint.
    ///
    /// # Panics
    ///
    /// Panics if the process-local publication lock is poisoned.
    pub fn publish<M: StorageIo>(
        &self,
        source: &GenerationRepository<M>,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<Option<RecoveryCheckpointSummary>, RecoveryCheckpointError> {
        let mut publication = self
            .publication
            .lock()
            .expect("ASSERT: checkpoint publication lock poisoned");
        *publication = None;
        source.publish_latest_recovery_checkpoint_to(self, Some(verifier), &mut publication)
    }

    /// Copies a committed graph whose DATA durability was established before
    /// its Commit WAL record. Validates the pinned source graph and hash-binds
    /// every copied object while writing, without rereading the new image.
    /// Recovery and offline
    /// scrub still independently verify DATA before restoring a lost Metadata tier.
    ///
    /// # Errors
    /// Returns source/copy graph, identity, format, or durability failures.
    ///
    /// # Panics
    ///
    /// Panics if the process-local publication lock is poisoned.
    pub fn publish_committed<M: StorageIo>(
        &self,
        source: &GenerationRepository<M>,
    ) -> Result<Option<RecoveryCheckpointSummary>, RecoveryCheckpointError> {
        let mut publication = self
            .publication
            .lock()
            .expect("ASSERT: checkpoint publication lock poisoned");
        if crate::read_intent::independent() {
            *publication = None;
        }
        source.publish_latest_recovery_checkpoint_to(self, None, &mut publication)
    }

    /// Selects the greatest wholly valid checkpoint and installs its exact
    /// Metadata objects plus Commit anchor into an empty Metadata repository.
    ///
    /// Corrupt, torn, or transitively incomplete newer candidates are ignored
    /// as whole generations. Transient storage I/O is returned instead of
    /// being mistaken for durable corruption.
    ///
    /// # Errors
    ///
    /// Returns an error when no selected candidate is complete, transient I/O
    /// prevents verification, or the target cannot accept the exact anchor.
    ///
    /// # Panics
    ///
    /// Panics if the process-local publication lock is poisoned.
    pub fn recover_latest<M: StorageIo>(
        &self,
        target: &GenerationRepository<M>,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<Option<RecoveredGeneration>, RecoveryCheckpointError> {
        let mut publication = self
            .publication
            .lock()
            .expect("ASSERT: checkpoint publication lock poisoned");
        // Independent recovery establishes fresh stored-byte evidence. A
        // previous online publication receipt must not survive that boundary,
        // including when recovery later returns a candidate or I/O error.
        *publication = None;
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let candidates = self.head_candidates(false)?;
        let had_candidates = !candidates.is_empty();
        for (head, name) in candidates {
            let audited = match self.audit_head_candidate(head, &name) {
                Ok(audited) => audited,
                Err(error) if error.is_candidate_corruption() => continue,
                Err(error) => return Err(error),
            };
            if let Err(error) = self.verify_graph(&audited, verifier) {
                if error.is_candidate_corruption() {
                    continue;
                }
                return Err(error);
            }
            let recovered = target.install_recovery_checkpoint(
                audited.record,
                &audited.objects.keys().copied().collect(),
                |object_id| self.read_object(&audited, object_id),
                verifier,
            )?;
            return Ok(Some(recovered));
        }
        if had_candidates {
            Err(RecoveryCheckpointError::NoCompleteCheckpoint)
        } else {
            Ok(None)
        }
    }

    /// Exhaustively verifies every published Recovery Checkpoint.
    ///
    /// Unlike recovery selection, scrub never hides a corrupt retained
    /// generation by falling back to an older one.
    ///
    /// # Errors
    ///
    /// Returns the first selector, format, graph, DATA-verification, identity,
    /// storage, or arithmetic failure.
    pub fn scrub(
        &self,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<RecoveryCheckpointScrubSummary, RecoveryCheckpointError> {
        self.scrub_with_protected_chunks(verifier)
            .map(|(summary, _)| summary)
    }

    pub(crate) fn scrub_with_protected_chunks(
        &self,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<(RecoveryCheckpointScrubSummary, BTreeMap<ChunkId, u64>), RecoveryCheckpointError>
    {
        let mut publication = self
            .publication
            .lock()
            .expect("ASSERT: checkpoint publication lock poisoned");
        *publication = None;
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let mut candidates = self.head_candidates(true)?;
        candidates.reverse();
        let mut summary = RecoveryCheckpointScrubSummary::default();
        let mut protected = BTreeMap::new();
        for (head, name) in candidates {
            let generation = head.generation();
            let audited = self.audit_head_candidate(head, &name)?;
            let (checkpoint, required) = self.verify_graph_with_chunks(&audited, verifier)?;
            for (chunk_id, logical_length) in required {
                if let Some(previous) = protected.insert(chunk_id, logical_length)
                    && previous != logical_length
                {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
            }
            summary.checkpoint_count = summary
                .checkpoint_count
                .checked_add(1)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            summary.first_generation.get_or_insert(generation);
            summary.latest_generation = Some(generation);
            summary.metadata_object_count = summary
                .metadata_object_count
                .checked_add(checkpoint.metadata_object_count)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            summary.metadata_payload_bytes = summary
                .metadata_payload_bytes
                .checked_add(checkpoint.metadata_payload_bytes)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            summary.file_bytes = summary
                .file_bytes
                .checked_add(checkpoint.file_length)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        }
        Ok((summary, protected))
    }

    pub(crate) fn protected_chunks_matching(
        &self,
        selected_chunks: &BTreeSet<ChunkId>,
        cache: Option<&CheckpointProtectedChunkCache>,
    ) -> Result<BTreeMap<ChunkId, u64>, RecoveryCheckpointError> {
        let candidates = self.head_candidates(false)?;
        if let Some(cache) = cache {
            cache.retain_current(
                &candidates
                    .iter()
                    .map(|(head, _)| Self::protected_identity(*head))
                    .collect(),
            );
        }
        let mut protected = BTreeMap::new();
        let mut complete = 0_usize;
        for (head, name) in candidates {
            let required = match cache {
                Some(cache) => {
                    self.matched_from_cached_head(head, &name, selected_chunks, cache)?
                }
                None => self.matched_from_audited_head(head, &name, selected_chunks)?,
            };
            let Some(required) = required else {
                continue;
            };
            for (chunk_id, logical_length) in required {
                if let Some(previous) = protected.insert(chunk_id, logical_length)
                    && previous != logical_length
                {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
            }
            complete += 1;
            if complete == 2 {
                break;
            }
        }
        Ok(protected)
    }

    fn protected_identity(head: RecoveryCheckpointHeadRecord) -> CheckpointProtectedIdentity {
        CheckpointProtectedIdentity {
            generation: head.generation(),
            file_length: head.file_length(),
            body_hash: head.checkpoint_body_hash(),
        }
    }

    fn matched_from_cached_head(
        &self,
        head: RecoveryCheckpointHeadRecord,
        name: &str,
        selected_chunks: &BTreeSet<ChunkId>,
        cache: &CheckpointProtectedChunkCache,
    ) -> Result<Option<BTreeMap<ChunkId, u64>>, RecoveryCheckpointError> {
        let identity = Self::protected_identity(head);
        if let Some(cached) = cache.get(&identity).filter(|_| {
            self.storage
                .object_len(name)
                .is_ok_and(|length| length == head.file_length())
        }) {
            return Ok(Some(cached.matched(selected_chunks)));
        }
        cache.invalidate(&identity);
        let audited = match self.audit_head_candidate(head, name) {
            Ok(audited) => audited,
            Err(error) if error.is_candidate_corruption() => return Ok(None),
            Err(error) => return Err(error),
        };
        let all = match self.scan_graph_matching(&audited, None) {
            Ok((_, all)) => all,
            Err(RecoveryCheckpointError::Manifest(ManifestTreeError::InvalidReplacement)) => {
                return Ok(Some(
                    self.scan_graph_matching(&audited, Some(selected_chunks))?.1,
                ));
            }
            Err(error) if error.is_candidate_corruption() => return Ok(None),
            Err(error) => return Err(error),
        };
        let required = all
            .iter()
            .filter(|(chunk_id, _)| selected_chunks.contains(chunk_id))
            .map(|(chunk_id, logical_length)| (*chunk_id, *logical_length))
            .collect();
        if let Some(entries) = Self::cached_protected_entries(all) {
            cache.insert(identity, entries);
        }
        Ok(Some(required))
    }

    fn matched_from_audited_head(
        &self,
        head: RecoveryCheckpointHeadRecord,
        name: &str,
        selected_chunks: &BTreeSet<ChunkId>,
    ) -> Result<Option<BTreeMap<ChunkId, u64>>, RecoveryCheckpointError> {
        let audited = match self.audit_head_candidate(head, name) {
            Ok(audited) => audited,
            Err(error) if error.is_candidate_corruption() => return Ok(None),
            Err(error) => return Err(error),
        };
        match self.scan_graph_matching(&audited, Some(selected_chunks)) {
            Ok((_, required)) => Ok(Some(required)),
            Err(error) if error.is_candidate_corruption() => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn cached_protected_entries(
        required: BTreeMap<ChunkId, u64>,
    ) -> Option<Vec<ProtectedChunkEntry>> {
        if required.len() > CHECKPOINT_PROTECTED_CHUNK_CACHE_LIMIT {
            return None;
        }
        required
            .into_iter()
            .map(|(chunk_id, logical_length)| {
                u32::try_from(logical_length)
                    .ok()
                    .map(|logical_length| ProtectedChunkEntry {
                        chunk_id,
                        logical_length,
                    })
            })
            .collect()
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn publish_source<F>(
        &self,
        record: CommitRecord,
        object_ids: &BTreeSet<MetadataObjectId>,
        required_chunk_count: usize,
        verifier: Option<&dyn RequiredChunkVerifier>,
        mut read_object: F,
    ) -> Result<RecoveryCheckpointSummary, RecoveryCheckpointError>
    where
        F: FnMut(MetadataObjectId) -> Result<Arc<Vec<u8>>, RecoveryCheckpointError>,
    {
        if object_ids.is_empty() || !object_ids.contains(&record.namespace_root()) {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        self.ensure_head_slots()?;
        let published_name = checkpoint_name(record.generation());
        if self.storage.exists(&published_name)? {
            let audited = self.audit_named(&published_name)?;
            if audited.record != record || !audited.objects.keys().eq(object_ids.iter()) {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            self.storage.sync_root()?;
            let summary = self.verify_publication_graph(&audited, verifier)?;
            let obsolete = self.publish_head(audited.descriptor)?;
            self.prune_obsolete(&obsolete)?;
            return Ok(summary);
        }

        let temporary_name = format!(".{published_name}.building");
        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        self.storage.set_len(&temporary_name, 0)?;
        let encoded_record = record.encode();
        let commit_offset = u64::try_from(RECOVERY_CHECKPOINT_HEADER_BYTES)
            .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
        let mut output = ImmutableWriteBuffer::new()?;
        output.append(
            &self.storage,
            &temporary_name,
            &[0; RECOVERY_CHECKPOINT_HEADER_BYTES],
        )?;
        output.append(&self.storage, &temporary_name, &encoded_record)?;
        let mut body_hasher = blake3::Hasher::new();
        body_hasher.update(&encoded_record);
        let mut cursor = commit_offset
            .checked_add(
                u64::try_from(COMMIT_RECORD_BYTES)
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            )
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        let mut metadata_payload_bytes = 0_u64;
        for object_id in object_ids.iter().copied() {
            let encoded = read_object(object_id)?;
            if MetadataObjectId::from_encoded(&encoded)? != object_id {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            let header = RecoveryCheckpointEntryHeader::new(
                object_id,
                encoded.len(),
                crc32c::crc32c(&encoded),
            )?;
            let encoded_header = header.encode();
            output.append(&self.storage, &temporary_name, &encoded_header)?;
            body_hasher.update(&encoded_header);
            output.append(&self.storage, &temporary_name, &encoded)?;
            body_hasher.update(&encoded);
            let padded_length = header.padded_length()?;
            let unpadded_length = u64::try_from(encoded_header.len())
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?
                .checked_add(
                    u64::try_from(encoded.len())
                        .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
                )
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            let padding_length = usize::try_from(padded_length - unpadded_length)
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
            if padding_length != 0 {
                let padding = [0_u8; 63];
                let padding = &padding[..padding_length];
                output.append(&self.storage, &temporary_name, padding)?;
                body_hasher.update(padding);
            }
            cursor = cursor
                .checked_add(padded_length)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            metadata_payload_bytes = metadata_payload_bytes
                .checked_add(
                    u64::try_from(encoded.len())
                        .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
                )
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        }
        let file_length = cursor
            .checked_add(
                u64::try_from(RECOVERY_CHECKPOINT_FOOTER_BYTES)
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            )
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        let descriptor = RecoveryCheckpointDescriptor::new(
            record,
            u64::try_from(object_ids.len())
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            file_length,
            *body_hasher.finalize().as_bytes(),
        )?;
        output.append(&self.storage, &temporary_name, &descriptor.encode_footer())?;
        output.finish(&self.storage, &temporary_name)?;
        // The body hash is now known. Patch only the fixed header; all entry
        // fields were batched across boundaries without partial-page appends.
        self.storage
            .write_at(&temporary_name, 0, &descriptor.encode_header())?;
        self.storage.set_len(&temporary_name, file_length)?;
        // The pinned source graph was validated by the sole caller before
        // copying. Every copied object is hash-bound to that graph above;
        // lengths, entry CRCs and the body hash come from the emitted bytes.
        // Do not read our fresh file back as a substitute for durability.
        // Recovery/scrub and existing-file collisions still audit stored bytes.
        let summary = RecoveryCheckpointSummary {
            generation: record.generation(),
            metadata_object_count: u64::try_from(object_ids.len())
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            metadata_payload_bytes,
            required_chunk_count: u64::try_from(required_chunk_count)
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            file_length,
        };
        self.storage.sync_file(&temporary_name)?;
        match self
            .storage
            .publish_noreplace(&temporary_name, &published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let raced = self.audit_named(&published_name)?;
                if raced.record != record
                    || !raced.objects.keys().eq(object_ids.iter())
                    || raced.descriptor != descriptor
                {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        let obsolete = self.publish_head(descriptor)?;
        self.prune_obsolete(&obsolete)?;
        Ok(summary)
    }

    fn ensure_head_slots(&self) -> Result<(), RecoveryCheckpointError> {
        let mut created = false;
        for name in HEAD_NAMES {
            if self.storage.exists(name)? {
                continue;
            }
            self.storage.create_new(name)?;
            self.storage.set_len(name, 0)?;
            self.storage.sync_file(name)?;
            created = true;
        }
        if created {
            self.storage.sync_root()?;
        }
        Ok(())
    }

    fn publish_head(
        &self,
        descriptor: RecoveryCheckpointDescriptor,
    ) -> Result<Vec<String>, RecoveryCheckpointError> {
        let old = self.head_candidates(false)?;
        if let Some((current, _)) = old.first()
            && current.generation() == descriptor.generation()
        {
            if current.file_length() != descriptor.file_length()
                || current.checkpoint_body_hash() != descriptor.body_hash()
            {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            return Ok(Vec::new());
        }
        if old
            .first()
            .is_some_and(|(current, _)| current.generation() > descriptor.generation())
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let selected_slot = old
            .first()
            .map(|(record, _)| self.head_slot(*record))
            .transpose()?
            .flatten();
        let target_slot = selected_slot.map_or(0, |slot| 1 - slot);
        let record =
            RecoveryCheckpointHeadRecord::new(descriptor, old.first().map(|(record, _)| *record))?;
        let encoded = record.encode();
        self.storage.set_len(HEAD_NAMES[target_slot], 0)?;
        self.storage
            .write_at(HEAD_NAMES[target_slot], 0, &encoded)?;
        self.storage.set_len(
            HEAD_NAMES[target_slot],
            u64::try_from(encoded.len())
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
        )?;
        let reread = self.storage.read(HEAD_NAMES[target_slot])?;
        if reread != encoded || RecoveryCheckpointHeadRecord::decode(&reread)? != record {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        self.storage.sync_file(HEAD_NAMES[target_slot])?;
        let retained = self
            .head_candidates(false)?
            .into_iter()
            .map(|(_, name)| name)
            .collect::<BTreeSet<_>>();
        Ok(old
            .into_iter()
            .map(|(_, name)| name)
            .filter(|name| !retained.contains(name))
            .collect())
    }

    fn prune_obsolete(&self, obsolete: &[String]) -> Result<(), RecoveryCheckpointError> {
        if obsolete.is_empty() {
            return Ok(());
        }
        for name in obsolete {
            self.storage.remove_file(name)?;
        }
        self.storage.sync_root()?;
        Ok(())
    }

    fn head_slot(
        &self,
        selected: RecoveryCheckpointHeadRecord,
    ) -> Result<Option<usize>, RecoveryCheckpointError> {
        for (slot, name) in HEAD_NAMES.into_iter().enumerate() {
            let length = self.storage.object_len(name)?;
            if length
                != u64::try_from(RECOVERY_CHECKPOINT_HEAD_BYTES)
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?
            {
                continue;
            }
            let bytes = self.storage.read(name)?;
            let Ok(candidate) = RecoveryCheckpointHeadRecord::decode(&bytes) else {
                continue;
            };
            if candidate == selected {
                return Ok(Some(slot));
            }
        }
        Ok(None)
    }

    fn head_candidates(
        &self,
        strict: bool,
    ) -> Result<Vec<(RecoveryCheckpointHeadRecord, String)>, RecoveryCheckpointError> {
        let mut valid = Vec::new();
        let mut invalid = false;
        for (slot, name) in HEAD_NAMES.into_iter().enumerate() {
            let length = match self.storage.object_len(name) {
                Ok(length) => length,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if length == 0 {
                continue;
            }
            if length
                != u64::try_from(RECOVERY_CHECKPOINT_HEAD_BYTES)
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?
            {
                invalid = true;
                continue;
            }
            match self.storage.read(name).and_then(|bytes| {
                RecoveryCheckpointHeadRecord::decode(&bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
            }) {
                Ok(record) => valid.push((slot, record)),
                Err(error) if error.kind() == io::ErrorKind::InvalidData => invalid = true,
                Err(error) => return Err(error.into()),
            }
        }
        valid.sort_unstable_by_key(|candidate| Reverse(candidate.1.generation()));
        if valid.len() == 2 {
            let newer = valid[0].1;
            let older = valid[1].1;
            if newer.previous_generation() != older.generation()
                || newer.previous_record_hash() != older.record_hash()
            {
                if strict {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
                valid.remove(0);
            }
        } else if strict && invalid {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok(valid
            .into_iter()
            .map(|(_, record)| (record, checkpoint_name(record.generation())))
            .collect())
    }

    fn audit_head_candidate(
        &self,
        head: RecoveryCheckpointHeadRecord,
        name: &str,
    ) -> Result<AuditedCheckpoint, RecoveryCheckpointError> {
        let audited = self.audit_named(name)?;
        if audited.descriptor.generation() != head.generation()
            || audited.descriptor.file_length() != head.file_length()
            || audited.descriptor.body_hash() != head.checkpoint_body_hash()
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok(audited)
    }

    #[allow(clippy::too_many_lines)]
    fn audit_named(&self, name: &str) -> Result<AuditedCheckpoint, RecoveryCheckpointError> {
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let actual_length = self.storage.object_len(name)?;
        let minimum_length = RECOVERY_CHECKPOINT_HEADER_BYTES
            .checked_add(COMMIT_RECORD_BYTES)
            .and_then(|length| length.checked_add(RECOVERY_CHECKPOINT_FOOTER_BYTES))
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        if actual_length
            < u64::try_from(minimum_length)
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let header = self
            .storage
            .read_exact_at(name, 0, RECOVERY_CHECKPOINT_HEADER_BYTES)?;
        let descriptor = RecoveryCheckpointDescriptor::decode_header(&header)?;
        let footer_offset = actual_length
            .checked_sub(
                u64::try_from(RECOVERY_CHECKPOINT_FOOTER_BYTES)
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            )
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        let footer =
            self.storage
                .read_exact_at(name, footer_offset, RECOVERY_CHECKPOINT_FOOTER_BYTES)?;
        if RecoveryCheckpointDescriptor::decode_footer(&footer)? != descriptor
            || descriptor.file_length() != actual_length
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let commit_offset = u64::try_from(RECOVERY_CHECKPOINT_HEADER_BYTES)
            .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
        let encoded_record =
            self.storage
                .read_exact_at(name, commit_offset, COMMIT_RECORD_BYTES)?;
        let record = CommitRecord::decode(&encoded_record)?;
        if record.generation() != descriptor.generation()
            || record.namespace_root() != descriptor.namespace_root()
            || record.policy_set() != descriptor.policy_set()
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let mut body_hasher = blake3::Hasher::new();
        body_hasher.update(&encoded_record);
        let mut cursor = commit_offset
            .checked_add(
                u64::try_from(COMMIT_RECORD_BYTES)
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            )
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        let maximum_entries = footer_offset.saturating_sub(cursor)
            / u64::try_from(RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES)
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
        if descriptor.object_count() > maximum_entries {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let capacity = usize::try_from(descriptor.object_count())
            .map_err(|_| RecoveryCheckpointError::OutOfMemory)?;
        let mut objects = BTreeMap::new();
        let mut previous_object_id = None;
        let mut metadata_payload_bytes = 0_u64;
        for _ in 0..capacity {
            let encoded_header =
                self.storage
                    .read_exact_at(name, cursor, RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES)?;
            body_hasher.update(&encoded_header);
            let entry = RecoveryCheckpointEntryHeader::decode(&encoded_header)?;
            if previous_object_id.is_some_and(|previous| previous >= entry.object_id()) {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            let payload_offset = cursor
                .checked_add(
                    u64::try_from(RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES)
                        .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
                )
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            let payload = read_bounded(
                &self.storage,
                name,
                payload_offset,
                usize::try_from(entry.encoded_length())
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            )?;
            body_hasher.update(&payload);
            if crc32c::crc32c(&payload) != entry.payload_crc32c()
                || MetadataObjectId::from_encoded(&payload)? != entry.object_id()
            {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            let padded_length = entry.padded_length()?;
            let unpadded_length = u64::try_from(RECOVERY_CHECKPOINT_ENTRY_HEADER_BYTES)
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?
                .checked_add(u64::from(entry.encoded_length()))
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            let padding_length = usize::try_from(padded_length - unpadded_length)
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
            if padding_length != 0 {
                let padding = self.storage.read_exact_at(
                    name,
                    cursor
                        .checked_add(unpadded_length)
                        .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?,
                    padding_length,
                )?;
                if padding.iter().any(|byte| *byte != 0) {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
                body_hasher.update(&padding);
            }
            if cursor
                .checked_add(padded_length)
                .is_none_or(|end| end > footer_offset)
            {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            objects.insert(
                entry.object_id(),
                ObjectSpan {
                    offset: payload_offset,
                    length: entry.encoded_length(),
                },
            );
            previous_object_id = Some(entry.object_id());
            cursor = cursor
                .checked_add(padded_length)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            metadata_payload_bytes = metadata_payload_bytes
                .checked_add(u64::from(entry.encoded_length()))
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        }
        if cursor != footer_offset
            || *body_hasher.finalize().as_bytes() != descriptor.body_hash()
            || !objects.contains_key(&record.namespace_root())
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok(AuditedCheckpoint {
            name: name.to_owned(),
            descriptor,
            record,
            objects,
            metadata_payload_bytes,
        })
    }

    fn verify_graph(
        &self,
        checkpoint: &AuditedCheckpoint,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<RecoveryCheckpointSummary, RecoveryCheckpointError> {
        self.verify_graph_with_chunks(checkpoint, verifier)
            .map(|(summary, _)| summary)
    }

    fn verify_publication_graph(
        &self,
        checkpoint: &AuditedCheckpoint,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<RecoveryCheckpointSummary, RecoveryCheckpointError> {
        self.verify_graph_policy(checkpoint, verifier)
            .map(|(summary, _)| summary)
    }

    fn verify_graph_with_chunks(
        &self,
        checkpoint: &AuditedCheckpoint,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<(RecoveryCheckpointSummary, BTreeMap<ChunkId, u64>), RecoveryCheckpointError> {
        self.verify_graph_policy(checkpoint, Some(verifier))
    }

    fn verify_graph_policy(
        &self,
        checkpoint: &AuditedCheckpoint,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<(RecoveryCheckpointSummary, BTreeMap<ChunkId, u64>), RecoveryCheckpointError> {
        let (_root, required) = self.scan_graph(checkpoint)?;
        if let Some(verifier) = verifier {
            verifier.verify_required_chunks(&required)?;
        }
        Ok((
            RecoveryCheckpointSummary {
                generation: checkpoint.record.generation(),
                metadata_object_count: u64::try_from(checkpoint.objects.len())
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
                metadata_payload_bytes: checkpoint.metadata_payload_bytes,
                required_chunk_count: u64::try_from(required.len())
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
                file_length: checkpoint.descriptor.file_length(),
            },
            required,
        ))
    }

    fn scan_graph(
        &self,
        checkpoint: &AuditedCheckpoint,
    ) -> Result<(NamespaceRoot, BTreeMap<ChunkId, u64>), RecoveryCheckpointError> {
        let encoded_root = self.read_object(checkpoint, checkpoint.record.namespace_root())?;
        let descriptor = NamespaceGraphRoot::decode(&encoded_root)?;
        let mut encoded_shards = BTreeMap::new();
        for reference in descriptor.shards() {
            let shard_id = reference.object_id();
            if let std::collections::btree_map::Entry::Vacant(entry) =
                encoded_shards.entry(shard_id)
            {
                entry.insert(self.read_object(checkpoint, shard_id)?);
            }
        }
        let root = NamespaceRoot::decode_graph(&encoded_root, &encoded_shards)?;
        if root.namespace_mutation_sequence() != checkpoint.record.namespace_mutation_cutoff()
            || root.inode_reservation_end() != checkpoint.record.inode_reservation_end()
            || root.inode_allocation_cursor() != checkpoint.record.inode_allocation_cursor()
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let mut reachable = BTreeSet::new();
        reachable.insert(checkpoint.record.namespace_root());
        reachable.extend(encoded_shards.keys().copied());
        let mut required = BTreeMap::new();
        let mut length_conflict = None;
        for inode in root.file_inodes() {
            let summary = scan_manifest_tree(
                inode.manifest_root(),
                |object_id| {
                    reachable.insert(object_id);
                    self.read_object(checkpoint, object_id)
                        .map_err(map_checkpoint_manifest_error)
                },
                |_logical_offset, extent| {
                    let (chunk_id, logical_length) = match *extent {
                        ManifestExtent::Data {
                            logical_length,
                            chunk_id,
                        } => (chunk_id, logical_length),
                        ManifestExtent::DataSlice {
                            chunk_id,
                            chunk_length,
                            ..
                        } => (chunk_id, u64::from(chunk_length)),
                        ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => return Ok(()),
                    };
                    if let Some(previous) = required.insert(chunk_id, logical_length)
                        && previous != logical_length
                    {
                        length_conflict = Some((chunk_id, previous, logical_length));
                    }
                    Ok(())
                },
            )?;
            if summary.logical_size() != inode.logical_size() {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
        }
        if length_conflict.is_some() || !reachable.iter().eq(checkpoint.objects.keys()) {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok((root, required))
    }

    fn scan_graph_matching(
        &self,
        checkpoint: &AuditedCheckpoint,
        selected_chunks: Option<&BTreeSet<ChunkId>>,
    ) -> Result<(NamespaceRoot, BTreeMap<ChunkId, u64>), RecoveryCheckpointError> {
        let mut encoded_shards = BTreeMap::new();
        let encoded_root = self.read_object(checkpoint, checkpoint.record.namespace_root())?;
        let descriptor = NamespaceGraphRoot::decode(&encoded_root)?;
        for reference in descriptor.shards() {
            let shard_id = reference.object_id();
            if let std::collections::btree_map::Entry::Vacant(entry) =
                encoded_shards.entry(shard_id)
            {
                entry.insert(self.read_object(checkpoint, shard_id)?);
            }
        }
        let root = NamespaceRoot::decode_graph(&encoded_root, &encoded_shards)?;
        if root.namespace_mutation_sequence() != checkpoint.record.namespace_mutation_cutoff()
            || root.inode_reservation_end() != checkpoint.record.inode_reservation_end()
            || root.inode_allocation_cursor() != checkpoint.record.inode_allocation_cursor()
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let mut reachable = BTreeSet::new();
        reachable.insert(checkpoint.record.namespace_root());
        reachable.extend(encoded_shards.keys().copied());
        let mut required = BTreeMap::new();
        let mut length_conflict = None;
        let collect_all = selected_chunks.is_none();
        for inode in root.file_inodes() {
            let summary = scan_manifest_tree(
                inode.manifest_root(),
                |object_id| {
                    reachable.insert(object_id);
                    self.read_object(checkpoint, object_id)
                        .map_err(map_checkpoint_manifest_error)
                },
                |_logical_offset, extent| {
                    let (chunk_id, logical_length) = match *extent {
                        ManifestExtent::Data {
                            logical_length,
                            chunk_id,
                        } => (chunk_id, logical_length),
                        ManifestExtent::DataSlice {
                            chunk_id,
                            chunk_length,
                            ..
                        } => (chunk_id, u64::from(chunk_length)),
                        ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => return Ok(()),
                    };
                    if let Some(selected_chunks) = selected_chunks {
                        if !selected_chunks.contains(&chunk_id) {
                            return Ok(());
                        }
                    } else if collect_all
                        && required.len() >= CHECKPOINT_PROTECTED_CHUNK_CACHE_LIMIT
                    {
                        return Err(ManifestTreeError::InvalidReplacement);
                    }
                    if let Some(previous) = required.insert(chunk_id, logical_length)
                        && previous != logical_length
                    {
                        length_conflict = Some((chunk_id, previous, logical_length));
                    }
                    Ok(())
                },
            )?;
            if summary.logical_size() != inode.logical_size() {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
        }
        if length_conflict.is_some() || !reachable.iter().eq(checkpoint.objects.keys()) {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok((root, required))
    }

    fn read_object(
        &self,
        checkpoint: &AuditedCheckpoint,
        object_id: MetadataObjectId,
    ) -> Result<Arc<Vec<u8>>, RecoveryCheckpointError> {
        let span = checkpoint
            .objects
            .get(&object_id)
            .ok_or(RecoveryCheckpointError::IdentityMismatch)?;
        let encoded = read_bounded(
            &self.storage,
            &checkpoint.name,
            span.offset,
            usize::try_from(span.length)
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
        )?;
        if MetadataObjectId::from_encoded(&encoded)? != object_id {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok(Arc::new(encoded))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryCheckpointSummary {
    generation: u64,
    metadata_object_count: u64,
    metadata_payload_bytes: u64,
    required_chunk_count: u64,
    file_length: u64,
}

impl RecoveryCheckpointSummary {
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn metadata_object_count(self) -> u64 {
        self.metadata_object_count
    }

    #[must_use]
    pub const fn metadata_payload_bytes(self) -> u64 {
        self.metadata_payload_bytes
    }

    #[must_use]
    pub const fn required_chunk_count(self) -> u64 {
        self.required_chunk_count
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecoveryCheckpointScrubSummary {
    checkpoint_count: u64,
    first_generation: Option<u64>,
    latest_generation: Option<u64>,
    metadata_object_count: u64,
    metadata_payload_bytes: u64,
    file_bytes: u64,
}

impl RecoveryCheckpointScrubSummary {
    #[must_use]
    pub const fn checkpoint_count(self) -> u64 {
        self.checkpoint_count
    }

    #[must_use]
    pub const fn first_generation(self) -> Option<u64> {
        self.first_generation
    }

    #[must_use]
    pub const fn latest_generation(self) -> Option<u64> {
        self.latest_generation
    }

    #[must_use]
    pub const fn metadata_object_count(self) -> u64 {
        self.metadata_object_count
    }

    #[must_use]
    pub const fn metadata_payload_bytes(self) -> u64 {
        self.metadata_payload_bytes
    }

    #[must_use]
    pub const fn file_bytes(self) -> u64 {
        self.file_bytes
    }
}

#[derive(Debug)]
pub enum RecoveryCheckpointError {
    Io(io::Error),
    Format(RecoveryCheckpointFormatError),
    Commit(CommitFormatError),
    Metadata(MetadataFormatError),
    Manifest(ManifestTreeError),
    Store(StoreError),
    Generation(Box<GenerationError>),
    IdentityMismatch,
    NoCompleteCheckpoint,
    ArithmeticOverflow,
    OutOfMemory,
}

impl RecoveryCheckpointError {
    fn is_candidate_corruption(&self) -> bool {
        match self {
            Self::Io(error)
            | Self::Store(StoreError::Io(error))
            | Self::Manifest(ManifestTreeError::Io(error)) => {
                error.kind() == io::ErrorKind::NotFound
            }
            Self::Format(_)
            | Self::Commit(_)
            | Self::Metadata(_)
            | Self::Manifest(
                ManifestTreeError::Metadata(_)
                | ManifestTreeError::Inner(_)
                | ManifestTreeError::IdentityMismatch(_)
                | ManifestTreeError::InvalidTree
                | ManifestTreeError::TreeTooDeep
                | ManifestTreeError::TreeTooLarge
                | ManifestTreeError::InvalidReplacement
                | ManifestTreeError::MissingSubtreeAllocation
                | ManifestTreeError::ArithmeticOverflow
                | ManifestTreeError::OutOfMemory,
            )
            | Self::Store(
                StoreError::Format(_)
                | StoreError::InvalidPublishedName(_)
                | StoreError::PublishedIdentityMismatch { .. }
                | StoreError::MissingVerifiedChunk { .. }
                | StoreError::ExactLocationMismatch,
            )
            | Self::IdentityMismatch => true,
            Self::Generation(_)
            | Self::Store(_)
            | Self::NoCompleteCheckpoint
            | Self::ArithmeticOverflow
            | Self::OutOfMemory => false,
        }
    }
}

impl fmt::Display for RecoveryCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for RecoveryCheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Format(error) => Some(error),
            Self::Commit(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Manifest(error) => Some(error),
            Self::Store(error) => Some(error),
            Self::Generation(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for RecoveryCheckpointError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<RecoveryCheckpointFormatError> for RecoveryCheckpointError {
    fn from(error: RecoveryCheckpointFormatError) -> Self {
        Self::Format(error)
    }
}

impl From<CommitFormatError> for RecoveryCheckpointError {
    fn from(error: CommitFormatError) -> Self {
        Self::Commit(error)
    }
}

impl From<MetadataFormatError> for RecoveryCheckpointError {
    fn from(error: MetadataFormatError) -> Self {
        Self::Metadata(error)
    }
}

impl From<ManifestTreeError> for RecoveryCheckpointError {
    fn from(error: ManifestTreeError) -> Self {
        Self::Manifest(error)
    }
}

impl From<StoreError> for RecoveryCheckpointError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<GenerationError> for RecoveryCheckpointError {
    fn from(error: GenerationError) -> Self {
        Self::Generation(Box::new(error))
    }
}

#[derive(Clone, Copy, Debug)]
struct ObjectSpan {
    offset: u64,
    length: u32,
}

#[derive(Debug)]
struct AuditedCheckpoint {
    name: String,
    descriptor: RecoveryCheckpointDescriptor,
    record: CommitRecord,
    objects: BTreeMap<MetadataObjectId, ObjectSpan>,
    metadata_payload_bytes: u64,
}

fn checkpoint_name(generation: u64) -> String {
    format!("{CHECKPOINT_PREFIX}{generation:016x}{CHECKPOINT_SUFFIX}")
}

fn read_bounded<I: StorageIo>(
    storage: &I,
    name: &str,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, RecoveryCheckpointError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| RecoveryCheckpointError::OutOfMemory)?;
    let mut completed = 0_usize;
    while completed < length {
        let current = (length - completed).min(MAX_STORAGE_RANGE_BYTES);
        let current_offset = offset
            .checked_add(
                u64::try_from(completed)
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            )
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        bytes.extend_from_slice(&storage.read_exact_at(name, current_offset, current)?);
        completed = completed
            .checked_add(current)
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
    }
    Ok(bytes)
}

fn map_checkpoint_manifest_error(error: RecoveryCheckpointError) -> ManifestTreeError {
    match error {
        RecoveryCheckpointError::Io(error) => ManifestTreeError::Io(error),
        _ => ManifestTreeError::InvalidTree,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastdup_format::{DurableInode, ManifestLeaf, NamespaceEntry, PolicySetId};

    #[test]
    fn checkpoint_copy_batches_entries_without_per_page_read_modify_write() {
        let root = std::env::temp_dir().join(format!("checkpoint-batch-{}", std::process::id()));
        let metadata = crate::FsStorageIo::open(root.join("metadata")).unwrap();
        let data = crate::FsStorageIo::open(root.join("data")).unwrap();
        let source = GenerationRepository::new(metadata, PolicySetId::new([1; 32]).unwrap());
        source
            .commit_namespace(&NamespaceRoot::new(1024, 2, 0, vec![], vec![]).unwrap())
            .unwrap();
        let length = (0..32_000_u64).map(|i| 4096 + i).sum();
        let manifest = ManifestLeaf::new(
            length,
            (0..32_000_u64)
                .map(|i| ManifestExtent::Fill {
                    logical_length: 4096 + i,
                    value: (i % 2) as u8,
                })
                .collect(),
        )
        .unwrap();
        let id = source.publish_manifest(&manifest).unwrap();
        let namespace = NamespaceRoot::new(
            1024,
            3,
            1,
            vec![DurableInode::new(2, 0o640, 1000, 1000, 1, 1, length, id).unwrap()],
            vec![NamespaceEntry::new(1, 2, b"backup".to_vec()).unwrap()],
        )
        .unwrap();
        let committed = source.commit_namespace(&namespace).unwrap();
        let checkpoints = RecoveryCheckpointRepository::new(data);
        let before = crate::direct_io::WRITE_CALLS.with(std::cell::Cell::get);
        let edges_before = crate::direct_io::WRITE_EDGE_READS.with(std::cell::Cell::get);
        let summary = checkpoints.publish_committed(&source).unwrap().unwrap();
        let writes = crate::direct_io::WRITE_CALLS.with(std::cell::Cell::get) - before;
        let edge_reads =
            crate::direct_io::WRITE_EDGE_READS.with(std::cell::Cell::get) - edges_before;
        eprintln!(
            "checkpoint bytes={}, writes={writes}, write-edge reads={edge_reads}",
            summary.file_length()
        );
        assert_eq!(
            edge_reads, 0,
            "the aligned checkpoint stream must not read sectors to preserve write edges"
        );
        assert!(
            writes <= 24,
            "checkpoint body must batch across object boundaries: {writes} physical writes"
        );
        assert!(summary.file_length() > 1024 * 1024);
        let audited = checkpoints
            .audit_named(&checkpoint_name(committed.generation()))
            .unwrap();
        assert_eq!(audited.record, committed);
        assert_eq!(
            audited.objects.len() as u64,
            summary.metadata_object_count()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn checkpoint_protection_cache_answers_later_proofs_without_rereading_the_graph() {
        let root = std::env::temp_dir().join(format!(
            "checkpoint-protection-cache-{}",
            std::process::id()
        ));
        let metadata = crate::FsStorageIo::open(root.join("metadata")).unwrap();
        let data = crate::FsStorageIo::open(root.join("data")).unwrap();
        let source = GenerationRepository::new(metadata, PolicySetId::new([1; 32]).unwrap());
        source
            .commit_namespace(&NamespaceRoot::new(1024, 2, 0, vec![], vec![]).unwrap())
            .unwrap();
        let manifest = ManifestLeaf::new(
            8192,
            vec![
                ManifestExtent::Fill {
                    logical_length: 4096,
                    value: 0,
                },
                ManifestExtent::Fill {
                    logical_length: 4096,
                    value: 1,
                },
            ],
        )
        .unwrap();
        let manifest_id = source.publish_manifest(&manifest).unwrap();
        let namespace = NamespaceRoot::new(
            1024,
            3,
            1,
            vec![DurableInode::new(2, 0o640, 1000, 1000, 1, 1, 8192, manifest_id).unwrap()],
            vec![NamespaceEntry::new(1, 2, b"backup".to_vec()).unwrap()],
        )
        .unwrap();
        source.commit_namespace(&namespace).unwrap();

        let checkpoints = RecoveryCheckpointRepository::new(data);
        checkpoints.publish_committed(&source).unwrap().unwrap();
        let selected = BTreeSet::from([ChunkId::from_bytes([7; 32])]);
        let cache = CheckpointProtectedChunkCache::default();

        let first_before = crate::direct_io::READ_BYTES.with(std::cell::Cell::get);
        let first = checkpoints
            .protected_chunks_matching(&selected, Some(&cache))
            .unwrap();
        let first_reads = crate::direct_io::READ_BYTES.with(std::cell::Cell::get) - first_before;
        assert!(first.is_empty());
        assert_eq!(cache.cached_checkpoint_count_for_test(), 1);

        let second_before = crate::direct_io::READ_BYTES.with(std::cell::Cell::get);
        let second = checkpoints
            .protected_chunks_matching(&selected, Some(&cache))
            .unwrap();
        let second_reads = crate::direct_io::READ_BYTES.with(std::cell::Cell::get) - second_before;
        assert_eq!(second, first);
        assert!(
            second_reads < first_reads,
            "cached protection must avoid a second checkpoint graph read: \
             first={first_reads} second={second_reads}"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
