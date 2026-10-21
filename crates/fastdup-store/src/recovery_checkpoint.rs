//! DATA-tier Recovery Checkpoint chains (ADR 0104, `recovery-checkpoint-v2`).
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use fastdup_format::{
    COMMIT_RECORD_BYTES, ChunkId, CommitFormatError, CommitRecord, ManifestExtent,
    ManifestInnerNode, ManifestLeaf, MetadataFormatError, MetadataObjectId, MetadataObjectKind,
    NamespaceGraphRoot, NamespaceObjectReferences, NamespaceRoot, RECOVERY_CHECKPOINT_FOOTER_BYTES,
    RECOVERY_CHECKPOINT_HEAD_BYTES, RECOVERY_CHECKPOINT_HEADER_BYTES,
    RECOVERY_CHECKPOINT_MAX_CHAIN_FILES, RECOVERY_CHECKPOINT_PAYLOAD_OFFSET,
    RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES, RecoveryCheckpointBody, RecoveryCheckpointChain,
    RecoveryCheckpointDescriptor, RecoveryCheckpointFormatError, RecoveryCheckpointHeadRecord,
    RecoveryCheckpointLink, RecoveryCheckpointObjectFacts, RecoveryCheckpointObjectKind,
    RecoveryCheckpointTableRecord, decode_namespace_object_references,
    decode_recovery_checkpoint_table, metadata_object_kind, recovery_checkpoint_padded_length,
};

use crate::generation::{
    CheckpointIncrement, GenerationError, GenerationRepository, RecoveredGeneration,
    RequiredChunkVerifier,
};
use crate::immutable_write::ImmutableWriteBuffer;
use crate::manifest_tree::{ManifestTreeError, scan_manifest_tree};
use crate::{MAX_STORAGE_RANGE_BYTES, StorageIo, StoreError};

const CHECKPOINT_PREFIX: &str = "recovery-checkpoint.";
const CHECKPOINT_SUFFIX: &str = ".fdrc";
const HEAD_NAMES: [&str; 2] = ["recovery-checkpoint.0.head", "recovery-checkpoint.1.head"];
/// Child and Chunk references the Online-GC protection cache may retain.
const CHECKPOINT_PROTECTED_REFERENCE_CACHE_LIMIT: usize = 8_388_608;

/// Outgoing references of one object embedded in a verified checkpoint file.
#[derive(Debug, Default)]
struct NodeReferences {
    children: Vec<MetadataObjectId>,
    chunks: Vec<(ChunkId, u64)>,
}

/// References of every object embedded in one verified checkpoint file.
#[derive(Debug, Default)]
struct FileReferences {
    nodes: HashMap<MetadataObjectId, NodeReferences>,
    reference_count: usize,
}

#[derive(Default)]
struct CheckpointProtectedChunkCacheState {
    files: HashMap<RecoveryCheckpointLink, Arc<FileReferences>>,
    retained_references: usize,
}

/// Online-GC cache of per-file references, keyed by immutable file identity.
/// A new Delta costs one read of that Delta; protection itself is an exact
/// in-memory traversal of every retained recovery point.
#[derive(Default)]
pub(crate) struct CheckpointProtectedChunkCache {
    state: Mutex<CheckpointProtectedChunkCacheState>,
}

impl CheckpointProtectedChunkCache {
    fn retain_current(&self, active: &HashSet<RecoveryCheckpointLink>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.files.retain(|identity, _| active.contains(identity));
        state.retained_references = state
            .files
            .values()
            .map(|file| file.reference_count)
            .sum::<usize>();
    }

    fn get(&self, identity: &RecoveryCheckpointLink) -> Option<Arc<FileReferences>> {
        self.state.lock().ok()?.files.get(identity).cloned()
    }

    fn insert(&self, identity: RecoveryCheckpointLink, references: &Arc<FileReferences>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(previous) = state.files.remove(&identity) {
            state.retained_references = state
                .retained_references
                .saturating_sub(previous.reference_count);
        }
        let retained = state
            .retained_references
            .saturating_add(references.reference_count);
        if retained <= CHECKPOINT_PROTECTED_REFERENCE_CACHE_LIMIT {
            state.retained_references = retained;
            state.files.insert(identity, Arc::clone(references));
        }
    }

    #[cfg(test)]
    fn cached_file_count_for_test(&self) -> usize {
        self.state.lock().map_or(0, |state| state.files.len())
    }
}

impl fmt::Debug for CheckpointProtectedChunkCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointProtectedChunkCache")
            .finish_non_exhaustive()
    }
}

/// Descriptor, Commit and authenticated table of one immutable checkpoint file.
#[derive(Clone, Debug)]
struct CheckpointFile {
    name: String,
    descriptor: RecoveryCheckpointDescriptor,
    record: CommitRecord,
    table: Arc<[RecoveryCheckpointTableRecord]>,
    payload_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
struct ObjectLocation {
    file: usize,
    record: RecoveryCheckpointTableRecord,
}

/// One structurally linked chain, Base first.
#[derive(Clone, Debug)]
struct CheckpointChain {
    files: Vec<CheckpointFile>,
    objects: HashMap<MetadataObjectId, ObjectLocation>,
}

impl CheckpointChain {
    fn tip(&self) -> &CheckpointFile {
        self.files
            .last()
            .expect("ASSERT: a loaded checkpoint chain has a Base")
    }

    fn contains_link(&self, link: RecoveryCheckpointLink) -> bool {
        self.files.iter().any(|file| file.descriptor.link() == link)
    }

    fn names(&self) -> impl Iterator<Item = String> + '_ {
        self.files.iter().map(|file| file.name.clone())
    }
}

/// Active writer state of the selected chain: the facts of every object it
/// holds. It is owner state behind the publication lock, not a content cache.
#[derive(Debug)]
pub(crate) struct CheckpointWriterState {
    chain: CheckpointChain,
    previous_chain_names: BTreeSet<String>,
    summary: RecoveryCheckpointSummary,
}

impl CheckpointWriterState {
    pub(crate) fn tip_record(&self) -> CommitRecord {
        self.chain.tip().record
    }

    pub(crate) const fn summary(&self) -> RecoveryCheckpointSummary {
        self.summary
    }

    pub(crate) fn facts(
        &self,
        object_id: MetadataObjectId,
    ) -> Option<RecoveryCheckpointObjectFacts> {
        self.chain
            .objects
            .get(&object_id)
            .map(|location| location.record.facts())
    }

    /// A new file may extend this chain unless it is full or its Deltas
    /// already carry as much payload as its Base.
    pub(crate) fn accepts_delta(&self) -> bool {
        let base_payload = self.chain.files[0].payload_bytes;
        let delta_payload = self
            .chain
            .tip()
            .descriptor
            .chain_payload_bytes()
            .saturating_sub(base_payload);
        u64::try_from(self.chain.files.len()).is_ok_and(|files| {
            files < RECOVERY_CHECKPOINT_MAX_CHAIN_FILES && delta_payload < base_payload
        })
    }

    fn retained_names(&self) -> BTreeSet<String> {
        let mut names = self.previous_chain_names.clone();
        names.extend(self.chain.names());
        names
    }
}

#[derive(Clone, Debug)]
pub struct RecoveryCheckpointRepository<I> {
    storage: I,
    // One active chain writer, shared by clones of this destination owner.
    writer: Arc<Mutex<Option<CheckpointWriterState>>>,
}

impl<I: StorageIo> RecoveryCheckpointRepository<I> {
    #[must_use]
    pub fn new(storage: I) -> Self {
        Self {
            storage,
            writer: Arc::new(Mutex::new(None)),
        }
    }

    /// Publishes the newest wholly verified Commit graph as a new chain Base.
    ///
    /// The source repository briefly serializes candidate selection and root
    /// pinning with Commit and Metadata GC. Graph enumeration, DATA verification,
    /// and checkpoint I/O run after those locks are released while the selected
    /// root remains pinned. This operation is never called from the Commit hot
    /// loop. If that Commit is already selected, its complete chain and DATA
    /// are verified instead.
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
        let mut writer = self
            .writer
            .lock()
            .expect("ASSERT: checkpoint publication lock poisoned");
        *writer = None;
        source.publish_latest_recovery_checkpoint_to(self, Some(verifier), &mut writer)
    }

    /// Copies a committed graph whose DATA durability was established before
    /// its Commit WAL record, as a Delta of the selected chain when the chain
    /// accepts one. Only objects absent from the chain are read, hash-bound and
    /// written; the new file is not read back. Recovery and offline scrub still
    /// independently verify DATA before restoring a lost Metadata tier.
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
        let mut writer = self
            .writer
            .lock()
            .expect("ASSERT: checkpoint publication lock poisoned");
        if crate::read_intent::independent() {
            *writer = None;
        }
        source.publish_latest_recovery_checkpoint_to(self, None, &mut writer)
    }

    /// Selects the greatest wholly valid recovery point and installs its exact
    /// reachable Metadata objects plus Commit anchor into an empty Metadata
    /// repository.
    ///
    /// Corrupt, torn, or transitively incomplete candidates are ignored as
    /// whole chains. Transient storage I/O is returned instead of being
    /// mistaken for durable corruption.
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
        let mut writer = self
            .writer
            .lock()
            .expect("ASSERT: checkpoint publication lock poisoned");
        // Independent recovery establishes fresh stored-byte evidence. Writer
        // state must not survive that boundary, including on error.
        *writer = None;
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let candidates = self.recovery_candidates(false)?;
        for link in &candidates {
            let outcome = self.load_chain(*link).and_then(|chain| {
                self.audit_chain_files(&chain)?;
                let (reachable, required) = self.verify_candidate_graph(&chain)?;
                verifier.verify_required_chunks(&required)?;
                Ok((chain, reachable))
            });
            let (chain, reachable) = match outcome {
                Ok(complete) => complete,
                Err(error) if error.is_candidate_corruption() => continue,
                Err(error) => return Err(error),
            };
            let recovered = target.install_recovery_checkpoint(
                chain.tip().record,
                &reachable,
                |object_id| self.read_object(&chain, object_id),
                verifier,
            )?;
            return Ok(Some(recovered));
        }
        if candidates.is_empty() {
            Ok(None)
        } else {
            Err(RecoveryCheckpointError::NoCompleteCheckpoint)
        }
    }

    /// Exhaustively verifies every retained recovery point and its chain.
    ///
    /// Unlike recovery selection, scrub never hides a corrupt retained
    /// candidate by falling back to an older one.
    ///
    /// # Errors
    ///
    /// Returns the first selector, format, chain, graph, DATA-verification,
    /// identity, storage, or arithmetic failure.
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
        let mut writer = self
            .writer
            .lock()
            .expect("ASSERT: checkpoint publication lock poisoned");
        *writer = None;
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let mut summary = RecoveryCheckpointScrubSummary::default();
        let mut protected = BTreeMap::new();
        let mut audited = HashSet::new();
        for link in self.recovery_candidates(true)? {
            let chain = self.load_chain(link)?;
            for (index, file) in chain.files.iter().enumerate() {
                if audited.insert(file.descriptor.link()) {
                    self.audit_file(&chain, index, false)?;
                    summary.metadata_object_count = summary
                        .metadata_object_count
                        .checked_add(file.descriptor.object_count())
                        .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
                    summary.metadata_payload_bytes = summary
                        .metadata_payload_bytes
                        .checked_add(file.payload_bytes)
                        .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
                    summary.file_bytes = summary
                        .file_bytes
                        .checked_add(file.descriptor.file_length())
                        .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
                }
            }
            let (_, required) = self.verify_candidate_graph(&chain)?;
            verifier.verify_required_chunks(&required)?;
            merge_protected(&mut protected, required)?;
            summary.checkpoint_count = summary
                .checkpoint_count
                .checked_add(1)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            let generation = link.generation();
            summary.first_generation = Some(
                summary
                    .first_generation
                    .map_or(generation, |first| first.min(generation)),
            );
            summary.latest_generation = Some(
                summary
                    .latest_generation
                    .map_or(generation, |latest| latest.max(generation)),
            );
        }
        Ok((summary, protected))
    }

    /// Returns the selected Chunks reachable from any retained recovery point.
    pub(crate) fn protected_chunks_matching(
        &self,
        selected_chunks: &BTreeSet<ChunkId>,
        cache: Option<&CheckpointProtectedChunkCache>,
    ) -> Result<BTreeMap<ChunkId, u64>, RecoveryCheckpointError> {
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let mut active = HashSet::new();
        let mut protected = BTreeMap::new();
        'candidates: for link in self.recovery_candidates(false)? {
            let chain = match self.load_chain(link) {
                Ok(chain) => chain,
                Err(error) if error.is_candidate_corruption() => continue,
                Err(error) => return Err(error),
            };
            let mut references = Vec::with_capacity(chain.files.len());
            for (index, file) in chain.files.iter().enumerate() {
                let identity = file.descriptor.link();
                active.insert(identity);
                if let Some(cached) = cache.and_then(|cache| cache.get(&identity)) {
                    references.push(cached);
                    continue;
                }
                let audited = match self.audit_file(&chain, index, true) {
                    Ok(audited) => Arc::new(audited),
                    Err(error) if error.is_candidate_corruption() => continue 'candidates,
                    Err(error) => return Err(error),
                };
                if let Some(cache) = cache {
                    cache.insert(identity, &audited);
                }
                references.push(audited);
            }
            collect_protected(&chain, &references, selected_chunks, &mut protected)?;
        }
        if let Some(cache) = cache {
            cache.retain_current(&active);
        }
        Ok(protected)
    }

    /// Adopts the newest head's chain as writer state from descriptors and
    /// authenticated tables only. A corrupt chain yields `None`: the next
    /// publication then starts a new Base.
    pub(crate) fn adopt_selected_chain(
        &self,
    ) -> Result<Option<CheckpointWriterState>, RecoveryCheckpointError> {
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        self.ensure_head_slots()?;
        let Some(newest) = self.head_candidates(false)?.first().copied() else {
            return Ok(None);
        };
        let chain = match self.load_chain(newest.tip()) {
            Ok(chain) => chain,
            Err(error) if error.is_candidate_corruption() => return Ok(None),
            Err(error) => return Err(error),
        };
        let previous_chain_names = match chain.tip().descriptor.previous_chain() {
            Some(link) => self.chain_names(link)?.unwrap_or_default(),
            None => BTreeSet::new(),
        };
        let summary = RecoveryCheckpointSummary::for_tip(&chain, 0)?;
        Ok(Some(CheckpointWriterState {
            chain,
            previous_chain_names,
            summary,
        }))
    }

    /// Fully verifies the stored chain named by writer state, plus its DATA
    /// when a verifier is supplied.
    pub(crate) fn verify_writer_chain(
        &self,
        state: &CheckpointWriterState,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<(), RecoveryCheckpointError> {
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let chain = self.load_chain(state.chain.tip().descriptor.link())?;
        self.audit_chain_files(&chain)?;
        let (_, required) = self.verify_candidate_graph(&chain)?;
        if let Some(verifier) = verifier {
            verifier.verify_required_chunks(&required)?;
        }
        Ok(())
    }

    /// Writes one Base, or one Delta of `current`, selects it, and prunes
    /// every checkpoint name outside the retained chains.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn publish_chain_file<F>(
        &self,
        record: CommitRecord,
        increment: &CheckpointIncrement,
        current: Option<CheckpointWriterState>,
        delta: bool,
        mut read_object: F,
    ) -> Result<CheckpointWriterState, RecoveryCheckpointError>
    where
        F: FnMut(MetadataObjectId) -> Result<Arc<Vec<u8>>, RecoveryCheckpointError>,
    {
        let (parent, replaced) = if delta {
            (
                Some(current.ok_or(RecoveryCheckpointError::IdentityMismatch)?),
                None,
            )
        } else {
            (None, current)
        };
        let root_held = increment.objects.contains_key(&record.namespace_root())
            || parent
                .as_ref()
                .is_some_and(|state| state.facts(record.namespace_root()).is_some());
        if !root_held
            || (parent.is_none() && increment.objects.is_empty())
            || parent.as_ref().is_some_and(|state| !state.accepts_delta())
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        self.ensure_head_slots()?;
        let published_name = checkpoint_name(record.generation());
        if self.storage.exists(&published_name)? {
            if let Some(link) = self
                .head_candidates(false)?
                .into_iter()
                .map(RecoveryCheckpointHeadRecord::tip)
                .find(|link| link.generation() == record.generation())
            {
                // A selected file of this generation must audit completely.
                return self.reuse_selected(link, record);
            }
            // An unselected name carries no authority; rewrite it.
            self.storage.remove_file(&published_name)?;
            self.storage.sync_root()?;
        }
        // A Delta inherits its chain's previous-chain link; a Base names the
        // chain it replaces and keeps that chain recoverable.
        let (previous_chain, previous_chain_names) = match (&parent, &replaced) {
            (Some(state), _) => (
                state.chain.tip().descriptor.previous_chain(),
                state.previous_chain_names.clone(),
            ),
            (None, Some(state)) => (
                Some(state.chain.tip().descriptor.link()),
                state.chain.names().collect(),
            ),
            (None, None) => (None, BTreeSet::new()),
        };

        let temporary_name = format!(".{published_name}.building");
        match self.storage.create_new(&temporary_name) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        self.storage.set_len_unpublished(&temporary_name, 0)?;
        let encoded_record = record.encode();
        let mut output = ImmutableWriteBuffer::new_unpublished()?;
        output.append(
            &self.storage,
            &temporary_name,
            &[0; RECOVERY_CHECKPOINT_HEADER_BYTES],
        )?;
        output.append(&self.storage, &temporary_name, &encoded_record)?;
        let mut body_hasher = blake3::Hasher::new();
        body_hasher.update(&encoded_record);
        let mut cursor = RECOVERY_CHECKPOINT_PAYLOAD_OFFSET;
        let mut table = Vec::new();
        table
            .try_reserve_exact(increment.objects.len())
            .map_err(|_| RecoveryCheckpointError::OutOfMemory)?;
        let mut payload_bytes = 0_u64;
        for (object_id, facts) in &increment.objects {
            let encoded = read_object(*object_id)?;
            if MetadataObjectId::from_encoded(&encoded)? != *object_id {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            let entry = RecoveryCheckpointTableRecord::new(
                *object_id,
                cursor,
                encoded.len(),
                crc32c::crc32c(&encoded),
                *facts,
            )?;
            output.append(&self.storage, &temporary_name, &encoded)?;
            body_hasher.update(&encoded);
            let padding_length =
                usize::try_from(entry.padded_end() - cursor - u64::from(entry.payload_length()))
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
            if padding_length != 0 {
                let padding = &[0_u8; 63][..padding_length];
                output.append(&self.storage, &temporary_name, padding)?;
                body_hasher.update(padding);
            }
            cursor = entry.padded_end();
            payload_bytes = payload_bytes
                .checked_add(u64::from(entry.payload_length()))
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            table.push(entry);
        }
        let table_bytes = encode_table(&table)?;
        output.append(&self.storage, &temporary_name, &table_bytes)?;
        body_hasher.update(&table_bytes);
        let object_count =
            u64::try_from(table.len()).map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
        let file_length = cursor
            .checked_add(
                u64::try_from(table_bytes.len())
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            )
            .and_then(|length| length.checked_add(RECOVERY_CHECKPOINT_FOOTER_BYTES as u64))
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        let (chain, chain_object_count, chain_payload_bytes) = match &parent {
            Some(state) => {
                let tip = state.chain.tip().descriptor;
                (
                    RecoveryCheckpointChain::Delta {
                        parent: tip.link(),
                        chain_index: tip
                            .chain_index()
                            .checked_add(1)
                            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?,
                        base_generation: tip.base_generation(),
                    },
                    tip.chain_object_count()
                        .checked_add(object_count)
                        .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?,
                    tip.chain_payload_bytes()
                        .checked_add(payload_bytes)
                        .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?,
                )
            }
            None => (RecoveryCheckpointChain::Base, object_count, payload_bytes),
        };
        let descriptor = RecoveryCheckpointDescriptor::new(
            record,
            RecoveryCheckpointBody {
                object_count,
                file_length,
                body_hash: *body_hasher.finalize().as_bytes(),
                table_hash: *blake3::hash(&table_bytes).as_bytes(),
                chain_object_count,
                chain_payload_bytes,
            },
            chain,
            previous_chain,
        )?;
        output.append(&self.storage, &temporary_name, &descriptor.encode_footer())?;
        output.finish(&self.storage, &temporary_name)?;
        // The body hash is now known. Patch only the fixed header; all other
        // fields were batched across boundaries without partial-page appends.
        self.storage
            .write_unpublished_at(&temporary_name, 0, &descriptor.encode_header())?;
        self.storage
            .set_len_unpublished(&temporary_name, file_length)?;
        // Every copied object is hash-bound to the pinned source graph above;
        // table, lengths, checksums and hashes come from the emitted bytes.
        // Do not read the fresh file back as a substitute for durability.
        self.storage.sync_file(&temporary_name)?;
        self.storage
            .publish_noreplace(&temporary_name, &published_name)?;
        self.storage.sync_root()?;
        self.publish_head(&descriptor)?;

        let file = CheckpointFile {
            name: published_name,
            descriptor,
            record,
            table: table.into(),
            payload_bytes,
        };
        let mut chain = parent.map_or_else(
            || CheckpointChain {
                files: Vec::new(),
                objects: HashMap::new(),
            },
            |state| state.chain,
        );
        let index = chain.files.len();
        for entry in file.table.iter() {
            chain.objects.insert(
                entry.object_id(),
                ObjectLocation {
                    file: index,
                    record: *entry,
                },
            );
        }
        chain.files.push(file);
        let summary = RecoveryCheckpointSummary::for_tip(
            &chain,
            u64::try_from(increment.required.len())
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
        )?;
        let state = CheckpointWriterState {
            chain,
            previous_chain_names,
            summary,
        };
        self.prune_unretained(&state)?;
        Ok(state)
    }

    fn reuse_selected(
        &self,
        link: RecoveryCheckpointLink,
        record: CommitRecord,
    ) -> Result<CheckpointWriterState, RecoveryCheckpointError> {
        let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
        let chain = self.load_chain(link)?;
        if chain.tip().record != record {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        self.audit_chain_files(&chain)?;
        self.verify_candidate_graph(&chain)?;
        let previous_chain_names = match chain.tip().descriptor.previous_chain() {
            Some(link) => self.chain_names(link)?.unwrap_or_default(),
            None => BTreeSet::new(),
        };
        let summary = RecoveryCheckpointSummary::for_tip(&chain, 0)?;
        Ok(CheckpointWriterState {
            chain,
            previous_chain_names,
            summary,
        })
    }

    /// Removes checkpoint names outside every retained chain. Listing is
    /// cleanup only; selection authority remains the heads.
    fn prune_unretained(
        &self,
        state: &CheckpointWriterState,
    ) -> Result<(), RecoveryCheckpointError> {
        let mut retained = state.retained_names();
        for head in self.head_candidates(false)? {
            if state.chain.contains_link(head.tip())
                || retained.contains(&checkpoint_name(head.generation()))
            {
                continue;
            }
            match self.chain_names(head.tip())? {
                Some(names) => retained.extend(names),
                // Keep everything that cannot be attributed to a chain.
                None => return Ok(()),
            }
        }
        let obsolete = self
            .storage
            .list_names()?
            .into_iter()
            .filter(|name| {
                name.starts_with(CHECKPOINT_PREFIX)
                    && name.ends_with(CHECKPOINT_SUFFIX)
                    && !retained.contains(name)
            })
            .collect::<Vec<_>>();
        for name in &obsolete {
            self.storage.remove_file(name)?;
        }
        if !obsolete.is_empty() {
            self.storage.sync_root()?;
        }
        Ok(())
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
        descriptor: &RecoveryCheckpointDescriptor,
    ) -> Result<(), RecoveryCheckpointError> {
        let old = self.head_candidates(false)?;
        if let Some(current) = old.first()
            && current.generation() >= descriptor.generation()
        {
            return if current.tip() == descriptor.link() {
                Ok(())
            } else {
                Err(RecoveryCheckpointError::IdentityMismatch)
            };
        }
        let selected_slot = old
            .first()
            .map(|record| self.head_slot(*record))
            .transpose()?
            .flatten();
        let target_slot = selected_slot.map_or(0, |slot| 1 - slot);
        let record = RecoveryCheckpointHeadRecord::new(descriptor.link(), old.first().copied())?;
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

    /// Valid heads, newest first. With two heads the newer must link exactly
    /// to the older; strict mode rejects any malformed retained head.
    fn head_candidates(
        &self,
        strict: bool,
    ) -> Result<Vec<RecoveryCheckpointHeadRecord>, RecoveryCheckpointError> {
        let mut valid = Vec::new();
        let mut invalid = false;
        for name in HEAD_NAMES {
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
                Ok(record) => valid.push(record),
                Err(error) if error.kind() == io::ErrorKind::InvalidData => invalid = true,
                Err(error) => return Err(error.into()),
            }
        }
        valid.sort_unstable_by_key(|record| Reverse(record.generation()));
        if valid.len() == 2 {
            let newer = valid[0];
            let older = valid[1];
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
        Ok(valid)
    }

    /// Retained recovery points in fallback order: newest head, older head,
    /// then the newest head's previous-chain link.
    fn recovery_candidates(
        &self,
        strict: bool,
    ) -> Result<Vec<RecoveryCheckpointLink>, RecoveryCheckpointError> {
        let heads = self.head_candidates(strict)?;
        let mut links = heads.iter().map(|head| head.tip()).collect::<Vec<_>>();
        if let Some(newest) = heads.first() {
            match self.discover_previous_chain(newest.tip()) {
                Ok(Some(link)) if !links.contains(&link) => links.push(link),
                Ok(_) => {}
                Err(error) if !strict && error.is_candidate_corruption() => {}
                Err(error) => return Err(error),
            }
        }
        Ok(links)
    }

    /// Reads the previous-chain link from the tip header, or from its footer
    /// when the header is damaged.
    fn discover_previous_chain(
        &self,
        tip: RecoveryCheckpointLink,
    ) -> Result<Option<RecoveryCheckpointLink>, RecoveryCheckpointError> {
        let name = checkpoint_name(tip.generation());
        let footer_offset = tip
            .file_length()
            .checked_sub(RECOVERY_CHECKPOINT_FOOTER_BYTES as u64)
            .ok_or(RecoveryCheckpointError::IdentityMismatch)?;
        for (offset, footer) in [(0, false), (footer_offset, true)] {
            let bytes = match self.storage.read_exact_at(&name, offset, 4_096) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => continue,
                Err(error) => return Err(error.into()),
            };
            let decoded = if footer {
                RecoveryCheckpointDescriptor::decode_footer(&bytes)
            } else {
                RecoveryCheckpointDescriptor::decode_header(&bytes)
            };
            if let Ok(descriptor) = decoded
                && descriptor.link() == tip
            {
                return Ok(descriptor.previous_chain());
            }
        }
        Err(RecoveryCheckpointError::IdentityMismatch)
    }

    /// Names of every file in the chain ending at `tip`, or `None` when the
    /// chain cannot be read as a structurally linked whole.
    fn chain_names(
        &self,
        tip: RecoveryCheckpointLink,
    ) -> Result<Option<BTreeSet<String>>, RecoveryCheckpointError> {
        let mut names = BTreeSet::new();
        let mut link = tip;
        loop {
            let name = checkpoint_name(link.generation());
            let descriptor = match self.read_descriptor(&name) {
                Ok((descriptor, _)) if descriptor.link() == link => descriptor,
                Ok(_) => return Ok(None),
                Err(error) if error.is_candidate_corruption() => return Ok(None),
                Err(error) => return Err(error),
            };
            names.insert(name);
            match descriptor.parent() {
                Some(parent)
                    if u64::try_from(names.len())
                        .is_ok_and(|files| files < RECOVERY_CHECKPOINT_MAX_CHAIN_FILES) =>
                {
                    link = parent;
                }
                Some(_) => return Ok(None),
                None => return Ok(Some(names)),
            }
        }
    }

    /// Reads the descriptors, Commits and authenticated tables of the chain
    /// ending at `tip` and validates every link, position, total and identity
    /// constraint that needs no payload bytes.
    fn load_chain(
        &self,
        tip: RecoveryCheckpointLink,
    ) -> Result<CheckpointChain, RecoveryCheckpointError> {
        let mut files = Vec::new();
        let mut link = tip;
        loop {
            if u64::try_from(files.len())
                .map_or(true, |files| files >= RECOVERY_CHECKPOINT_MAX_CHAIN_FILES)
            {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            let file = self.read_file(link)?;
            let parent = file.descriptor.parent();
            files.push(file);
            match parent {
                Some(parent) => link = parent,
                None => break,
            }
        }
        files.reverse();
        let base_generation = files[0].descriptor.generation();
        let previous_chain = files[0].descriptor.previous_chain();
        let capacity = usize::try_from(
            files
                .last()
                .map_or(0, |file| file.descriptor.chain_object_count()),
        )
        .map_err(|_| RecoveryCheckpointError::OutOfMemory)?;
        let mut objects = HashMap::new();
        objects
            .try_reserve(capacity)
            .map_err(|_| RecoveryCheckpointError::OutOfMemory)?;
        let mut chain_objects = 0_u64;
        let mut chain_payload = 0_u64;
        for (index, file) in files.iter().enumerate() {
            let descriptor = file.descriptor;
            chain_objects = chain_objects
                .checked_add(descriptor.object_count())
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            chain_payload = chain_payload
                .checked_add(file.payload_bytes)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            if u64::try_from(index) != Ok(descriptor.chain_index())
                || descriptor.base_generation() != base_generation
                || descriptor.previous_chain() != previous_chain
                || descriptor.chain_object_count() != chain_objects
                || descriptor.chain_payload_bytes() != chain_payload
            {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            for entry in file.table.iter() {
                if objects
                    .insert(
                        entry.object_id(),
                        ObjectLocation {
                            file: index,
                            record: *entry,
                        },
                    )
                    .is_some()
                {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
            }
            if !objects
                .get(&file.record.namespace_root())
                .is_some_and(|location| {
                    location.record.facts().kind()
                        == RecoveryCheckpointObjectKind::NamespaceGraphRoot
                })
            {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
        }
        Ok(CheckpointChain { files, objects })
    }

    fn read_file(
        &self,
        link: RecoveryCheckpointLink,
    ) -> Result<CheckpointFile, RecoveryCheckpointError> {
        let name = checkpoint_name(link.generation());
        let (descriptor, record) = self.read_descriptor(&name)?;
        if descriptor.link() != link {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let table_length = usize::try_from(descriptor.object_count())
            .ok()
            .and_then(|count| count.checked_mul(RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES))
            .ok_or(RecoveryCheckpointError::OutOfMemory)?;
        let table_bytes = read_bounded(
            &self.storage,
            &name,
            descriptor.table_offset(),
            table_length,
        )?;
        if *blake3::hash(&table_bytes).as_bytes() != descriptor.table_hash() {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let table = decode_recovery_checkpoint_table(&table_bytes, descriptor.table_offset())?;
        let payload_bytes = table.iter().try_fold(0_u64, |total, entry| {
            total
                .checked_add(u64::from(entry.payload_length()))
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)
        })?;
        Ok(CheckpointFile {
            name,
            descriptor,
            record,
            table: table.into(),
            payload_bytes,
        })
    }

    fn read_descriptor(
        &self,
        name: &str,
    ) -> Result<(RecoveryCheckpointDescriptor, CommitRecord), RecoveryCheckpointError> {
        let actual_length = self.storage.object_len(name)?;
        let minimum_length = RECOVERY_CHECKPOINT_PAYLOAD_OFFSET
            .checked_add(RECOVERY_CHECKPOINT_FOOTER_BYTES as u64)
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        if actual_length < minimum_length {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let header = self
            .storage
            .read_exact_at(name, 0, RECOVERY_CHECKPOINT_HEADER_BYTES)?;
        let descriptor = RecoveryCheckpointDescriptor::decode_header(&header)?;
        let footer = self.storage.read_exact_at(
            name,
            actual_length - RECOVERY_CHECKPOINT_FOOTER_BYTES as u64,
            RECOVERY_CHECKPOINT_FOOTER_BYTES,
        )?;
        if RecoveryCheckpointDescriptor::decode_footer(&footer)? != descriptor
            || descriptor.file_length() != actual_length
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let encoded_record = self.storage.read_exact_at(
            name,
            RECOVERY_CHECKPOINT_HEADER_BYTES as u64,
            COMMIT_RECORD_BYTES,
        )?;
        let record = CommitRecord::decode(&encoded_record)?;
        if record.generation() != descriptor.generation()
            || record.namespace_root() != descriptor.namespace_root()
            || record.policy_set() != descriptor.policy_set()
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok((descriptor, record))
    }

    fn audit_chain_files(&self, chain: &CheckpointChain) -> Result<(), RecoveryCheckpointError> {
        for index in 0..chain.files.len() {
            self.audit_file(chain, index, false)?;
        }
        Ok(())
    }

    /// Streams one file's payload region and proves its body hash, every
    /// payload identity and checksum, its recorded facts, and prefix closure:
    /// each reference resolves to this file or an earlier one and matches the
    /// referenced object's facts.
    fn audit_file(
        &self,
        chain: &CheckpointChain,
        index: usize,
        collect: bool,
    ) -> Result<FileReferences, RecoveryCheckpointError> {
        let file = &chain.files[index];
        let encoded_record = self.storage.read_exact_at(
            &file.name,
            RECOVERY_CHECKPOINT_HEADER_BYTES as u64,
            COMMIT_RECORD_BYTES,
        )?;
        if CommitRecord::decode(&encoded_record)? != file.record {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let mut body_hasher = blake3::Hasher::new();
        body_hasher.update(&encoded_record);
        let mut reader =
            SequentialReader::new(&self.storage, &file.name, file.descriptor.table_offset());
        let mut references = FileReferences::default();
        for entry in file.table.iter() {
            let padded = usize::try_from(recovery_checkpoint_padded_length(entry.payload_length()))
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
            let bytes = reader.read(entry.payload_offset(), padded)?;
            body_hasher.update(&bytes);
            let (payload, padding) = bytes.split_at(
                usize::try_from(entry.payload_length())
                    .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            );
            if padding.iter().any(|byte| *byte != 0)
                || crc32c::crc32c(payload) != entry.payload_crc32c()
                || MetadataObjectId::from_encoded(payload)? != entry.object_id()
            {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            let decoded = decode_checkpoint_object(payload)?;
            if decoded.facts != entry.facts() {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
            for (child, expectation) in &decoded.references {
                if !chain.objects.get(child).is_some_and(|location| {
                    location.file <= index && expectation.admits(location.record.facts())
                }) {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
            }
            if collect {
                references.reference_count = references
                    .reference_count
                    .saturating_add(decoded.references.len())
                    .saturating_add(decoded.chunks.len());
                references.nodes.insert(
                    entry.object_id(),
                    NodeReferences {
                        children: decoded.references.iter().map(|(child, _)| *child).collect(),
                        chunks: decoded.chunks,
                    },
                );
            }
        }
        body_hasher.update(&encode_table(&file.table)?);
        if *body_hasher.finalize().as_bytes() != file.descriptor.body_hash() {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok(references)
    }

    /// Verifies the tip Commit's graph through the chain and returns its exact
    /// reachable objects and required Chunks.
    fn verify_candidate_graph(
        &self,
        chain: &CheckpointChain,
    ) -> Result<(BTreeSet<MetadataObjectId>, BTreeMap<ChunkId, u64>), RecoveryCheckpointError> {
        let record = chain.tip().record;
        let encoded_root = self.read_object(chain, record.namespace_root())?;
        let descriptor = NamespaceGraphRoot::decode(&encoded_root)?;
        let mut encoded_shards = BTreeMap::new();
        for reference in descriptor.shards() {
            let shard_id = reference.object_id();
            if let std::collections::btree_map::Entry::Vacant(entry) =
                encoded_shards.entry(shard_id)
            {
                entry.insert(self.read_object(chain, shard_id)?);
            }
        }
        let root = NamespaceRoot::decode_graph(&encoded_root, &encoded_shards)?;
        if root.namespace_mutation_sequence() != record.namespace_mutation_cutoff()
            || root.inode_reservation_end() != record.inode_reservation_end()
            || root.inode_allocation_cursor() != record.inode_allocation_cursor()
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let mut reachable = BTreeSet::new();
        reachable.insert(record.namespace_root());
        reachable.extend(encoded_shards.keys().copied());
        let mut required = BTreeMap::new();
        let mut length_conflict = false;
        for inode in root.file_inodes() {
            if reachable.contains(&inode.manifest_root()) {
                // A shared tree was walked for an earlier inode; its length
                // must still match this inode.
                if chain
                    .objects
                    .get(&inode.manifest_root())
                    .is_none_or(|location| {
                        location.record.facts().logical_length() != inode.logical_size()
                    })
                {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
                continue;
            }
            let summary = scan_manifest_tree(
                inode.manifest_root(),
                |object_id| {
                    reachable.insert(object_id);
                    self.read_object(chain, object_id)
                        .map_err(map_checkpoint_manifest_error)
                },
                |_logical_offset, extent| {
                    if let Some((chunk_id, logical_length)) = extent_chunk(extent)
                        && let Some(previous) = required.insert(chunk_id, logical_length)
                        && previous != logical_length
                    {
                        length_conflict = true;
                    }
                    Ok(())
                },
            )?;
            if summary.logical_size() != inode.logical_size() {
                return Err(RecoveryCheckpointError::IdentityMismatch);
            }
        }
        if length_conflict {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        Ok((reachable, required))
    }

    fn read_object(
        &self,
        chain: &CheckpointChain,
        object_id: MetadataObjectId,
    ) -> Result<Arc<Vec<u8>>, RecoveryCheckpointError> {
        let location = chain
            .objects
            .get(&object_id)
            .ok_or(RecoveryCheckpointError::IdentityMismatch)?;
        let encoded = read_bounded(
            &self.storage,
            &chain.files[location.file].name,
            location.record.payload_offset(),
            usize::try_from(location.record.payload_length())
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
    chain_file_count: u64,
    chain_metadata_payload_bytes: u64,
    base: bool,
}

impl RecoveryCheckpointSummary {
    fn for_tip(
        chain: &CheckpointChain,
        required_chunk_count: u64,
    ) -> Result<Self, RecoveryCheckpointError> {
        let tip = chain.tip();
        Ok(Self {
            generation: tip.descriptor.generation(),
            metadata_object_count: tip.descriptor.object_count(),
            metadata_payload_bytes: tip.payload_bytes,
            required_chunk_count,
            file_length: tip.descriptor.file_length(),
            chain_file_count: u64::try_from(chain.files.len())
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            chain_metadata_payload_bytes: tip.descriptor.chain_payload_bytes(),
            base: tip.descriptor.is_base(),
        })
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Metadata objects embedded in the published file.
    #[must_use]
    pub const fn metadata_object_count(self) -> u64 {
        self.metadata_object_count
    }

    /// Payload bytes of the objects embedded in the published file.
    #[must_use]
    pub const fn metadata_payload_bytes(self) -> u64 {
        self.metadata_payload_bytes
    }

    /// Distinct Chunks named by the leaves embedded in the published file;
    /// zero when an existing file was adopted rather than written.
    #[must_use]
    pub const fn required_chunk_count(self) -> u64 {
        self.required_chunk_count
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length
    }

    #[must_use]
    pub const fn chain_file_count(self) -> u64 {
        self.chain_file_count
    }

    #[must_use]
    pub const fn chain_metadata_payload_bytes(self) -> u64 {
        self.chain_metadata_payload_bytes
    }

    #[must_use]
    pub const fn is_base(self) -> bool {
        self.base
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
    /// Verified recovery points.
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

    /// Objects across every distinct verified file.
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
                matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof
                )
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

/// What a reference requires of the object it names.
#[derive(Clone, Copy, Debug)]
enum ReferenceExpectation {
    NamespaceShard,
    ManifestRoot {
        logical_length: u64,
    },
    ManifestChild {
        level: u16,
        logical_length: u64,
        allocated_bytes: Option<u64>,
    },
}

impl ReferenceExpectation {
    fn admits(self, facts: RecoveryCheckpointObjectFacts) -> bool {
        match self {
            Self::NamespaceShard => matches!(
                facts.kind(),
                RecoveryCheckpointObjectKind::NamespaceInodeShard
                    | RecoveryCheckpointObjectKind::NamespaceEntryShard
            ),
            Self::ManifestRoot { logical_length } => {
                facts.is_manifest() && facts.logical_length() == logical_length
            }
            Self::ManifestChild {
                level,
                logical_length,
                allocated_bytes,
            } => {
                facts.is_manifest()
                    && facts.level() == level
                    && facts.logical_length() == logical_length
                    && allocated_bytes
                        .is_none_or(|expected| facts.allocated_bytes() == Some(expected))
            }
        }
    }
}

struct DecodedCheckpointObject {
    facts: RecoveryCheckpointObjectFacts,
    references: Vec<(MetadataObjectId, ReferenceExpectation)>,
    chunks: Vec<(ChunkId, u64)>,
}

/// Fully decodes one embedded object into its facts and outgoing references.
fn decode_checkpoint_object(
    encoded: &[u8],
) -> Result<DecodedCheckpointObject, RecoveryCheckpointError> {
    match metadata_object_kind(encoded)? {
        MetadataObjectKind::ManifestLeaf => {
            let leaf = ManifestLeaf::decode(encoded)?;
            let mut allocated = 0_u64;
            let mut chunks = Vec::new();
            for extent in leaf.extents() {
                if let Some(chunk) = extent_chunk(extent) {
                    chunks.push(chunk);
                }
                if let ManifestExtent::Data { logical_length, .. }
                | ManifestExtent::DataSlice { logical_length, .. }
                | ManifestExtent::Fill { logical_length, .. } = *extent
                {
                    allocated = allocated
                        .checked_add(logical_length)
                        .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
                }
            }
            chunks.sort_unstable();
            chunks.dedup();
            Ok(DecodedCheckpointObject {
                facts: RecoveryCheckpointObjectFacts::manifest_leaf(leaf.file_length(), allocated)?,
                references: Vec::new(),
                chunks,
            })
        }
        MetadataObjectKind::ManifestInnerNode => {
            let node = ManifestInnerNode::decode(encoded).map_err(ManifestTreeError::from)?;
            let child_level = node
                .level()
                .checked_sub(1)
                .ok_or(RecoveryCheckpointError::IdentityMismatch)?;
            Ok(DecodedCheckpointObject {
                facts: RecoveryCheckpointObjectFacts::manifest_inner(
                    node.level(),
                    node.file_length(),
                    node.allocated_bytes().map_err(ManifestTreeError::from)?,
                )?,
                references: node
                    .children()
                    .iter()
                    .map(|child| {
                        (
                            child.child(),
                            ReferenceExpectation::ManifestChild {
                                level: child_level,
                                logical_length: child.logical_length(),
                                allocated_bytes: child.allocated_bytes(),
                            },
                        )
                    })
                    .collect(),
                chunks: Vec::new(),
            })
        }
        MetadataObjectKind::NamespaceRoot => {
            let (kind, references) = match decode_namespace_object_references(encoded)? {
                NamespaceObjectReferences::GraphRoot(shards) => (
                    RecoveryCheckpointObjectKind::NamespaceGraphRoot,
                    shards
                        .into_iter()
                        .map(|shard| (shard, ReferenceExpectation::NamespaceShard))
                        .collect(),
                ),
                NamespaceObjectReferences::InodeShard(files) => (
                    RecoveryCheckpointObjectKind::NamespaceInodeShard,
                    files
                        .into_iter()
                        .map(|(root, logical_length)| {
                            (root, ReferenceExpectation::ManifestRoot { logical_length })
                        })
                        .collect(),
                ),
                NamespaceObjectReferences::EntryShard => (
                    RecoveryCheckpointObjectKind::NamespaceEntryShard,
                    Vec::new(),
                ),
            };
            Ok(DecodedCheckpointObject {
                facts: RecoveryCheckpointObjectFacts::namespace(kind)?,
                references,
                chunks: Vec::new(),
            })
        }
        MetadataObjectKind::ExactIndexRunSet
        | MetadataObjectKind::NamespaceShard
        | MetadataObjectKind::Unknown(_) => Err(RecoveryCheckpointError::IdentityMismatch),
    }
}

const fn extent_chunk(extent: &ManifestExtent) -> Option<(ChunkId, u64)> {
    match *extent {
        ManifestExtent::Data {
            logical_length,
            chunk_id,
        } => Some((chunk_id, logical_length)),
        ManifestExtent::DataSlice {
            chunk_id,
            chunk_length,
            ..
        } => Some((chunk_id, chunk_length as u64)),
        ManifestExtent::Hole { .. } | ManifestExtent::Fill { .. } => None,
    }
}

/// Exact in-memory traversal of one recovery point over verified references.
fn collect_protected(
    chain: &CheckpointChain,
    references: &[Arc<FileReferences>],
    selected_chunks: &BTreeSet<ChunkId>,
    protected: &mut BTreeMap<ChunkId, u64>,
) -> Result<(), RecoveryCheckpointError> {
    let mut pending = vec![chain.tip().record.namespace_root()];
    let mut visited = HashSet::new();
    while let Some(object_id) = pending.pop() {
        if !visited.insert(object_id) {
            continue;
        }
        let node = chain
            .objects
            .get(&object_id)
            .and_then(|location| references.get(location.file))
            .and_then(|file| file.nodes.get(&object_id))
            .ok_or(RecoveryCheckpointError::IdentityMismatch)?;
        pending.extend(node.children.iter().copied());
        for (chunk_id, logical_length) in &node.chunks {
            if selected_chunks.contains(chunk_id) {
                merge_protected(protected, [(*chunk_id, *logical_length)])?;
            }
        }
    }
    Ok(())
}

fn merge_protected(
    protected: &mut BTreeMap<ChunkId, u64>,
    chunks: impl IntoIterator<Item = (ChunkId, u64)>,
) -> Result<(), RecoveryCheckpointError> {
    for (chunk_id, logical_length) in chunks {
        if let Some(previous) = protected.insert(chunk_id, logical_length)
            && previous != logical_length
        {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
    }
    Ok(())
}

fn encode_table(
    table: &[RecoveryCheckpointTableRecord],
) -> Result<Vec<u8>, RecoveryCheckpointError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(
            table
                .len()
                .checked_mul(RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?,
        )
        .map_err(|_| RecoveryCheckpointError::OutOfMemory)?;
    for entry in table {
        bytes.extend_from_slice(&entry.encode());
    }
    Ok(bytes)
}

/// Forward reader over one payload region using bounded windows.
struct SequentialReader<'a, I> {
    storage: &'a I,
    name: &'a str,
    limit: u64,
    start: u64,
    window: Vec<u8>,
}

impl<'a, I: StorageIo> SequentialReader<'a, I> {
    const fn new(storage: &'a I, name: &'a str, limit: u64) -> Self {
        Self {
            storage,
            name,
            limit,
            start: 0,
            window: Vec::new(),
        }
    }

    fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, RecoveryCheckpointError> {
        let end = offset
            .checked_add(
                u64::try_from(length).map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
            )
            .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
        if end > self.limit {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let window_end = self.start + self.window.len() as u64;
        if offset < self.start || end > window_end {
            if length >= MAX_STORAGE_RANGE_BYTES {
                self.window.clear();
                return read_bounded(self.storage, self.name, offset, length);
            }
            let fill = usize::try_from((self.limit - offset).min(MAX_STORAGE_RANGE_BYTES as u64))
                .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
            self.window = self.storage.read_exact_at(self.name, offset, fill)?;
            self.start = offset;
        }
        let from = usize::try_from(offset - self.start)
            .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
        Ok(self.window[from..from + length].to_vec())
    }
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
    use crate::ContainerRepository;
    use fastdup_format::{DurableInode, NamespaceEntry, PolicySetId};

    struct AcceptAll;

    impl RequiredChunkVerifier for AcceptAll {
        fn verify_required_chunks(&self, _: &BTreeMap<ChunkId, u64>) -> Result<(), StoreError> {
            Ok(())
        }
    }

    struct Fixture {
        root: std::path::PathBuf,
        metadata: crate::FsStorageIo,
        source: GenerationRepository<crate::FsStorageIo>,
        containers: ContainerRepository<crate::FsStorageIo>,
        checkpoints: RecoveryCheckpointRepository<crate::FsStorageIo>,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            let metadata = crate::FsStorageIo::open(root.join("metadata")).unwrap();
            let data = crate::FsStorageIo::open(root.join("data")).unwrap();
            let source =
                GenerationRepository::new(metadata.clone(), PolicySetId::new([1; 32]).unwrap());
            source
                .commit_namespace(&NamespaceRoot::new(1024, 2, 0, vec![], vec![]).unwrap())
                .unwrap();
            Self {
                root,
                metadata,
                source,
                containers: ContainerRepository::new(data.clone()),
                checkpoints: RecoveryCheckpointRepository::new(data),
            }
        }

        /// Commits one regular file per Chunk, each backed by a one-extent leaf.
        fn commit_files(&self, chunks: &[ChunkId], sequence: u64) -> CommitRecord {
            let mut inodes = Vec::new();
            let mut entries = Vec::new();
            for (ordinal, chunk_id) in chunks.iter().enumerate() {
                let inode = u64::try_from(ordinal).unwrap() + 2;
                let manifest = ManifestLeaf::new(
                    4096,
                    vec![ManifestExtent::Data {
                        logical_length: 4096,
                        chunk_id: *chunk_id,
                    }],
                )
                .unwrap();
                let manifest_root = self.source.publish_manifest(&manifest).unwrap();
                inodes.push(
                    DurableInode::new(inode, 0o640, 1000, 1000, 1, sequence, 4096, manifest_root)
                        .unwrap(),
                );
                entries.push(
                    NamespaceEntry::new(1, inode, format!("file-{ordinal}").into_bytes()).unwrap(),
                );
            }
            let namespace = NamespaceRoot::new(1024, 8, sequence, inodes, entries).unwrap();
            self.source
                .commit_namespace_with_verified_files_using(
                    &namespace,
                    &self.containers,
                    &AcceptAll,
                )
                .unwrap()
                .record()
        }

        fn read_source_object(&self, object_id: MetadataObjectId) -> Arc<Vec<u8>> {
            use std::fmt::Write as _;
            let mut name = String::new();
            for byte in object_id.bytes() {
                write!(&mut name, "{byte:02x}").unwrap();
            }
            name.push_str(".fdm");
            Arc::new(self.metadata.read(&name).unwrap())
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn online_protection_is_exact_per_retained_point_and_reads_only_new_files() {
        let fixture = Fixture::new("checkpoint-chain-protection");
        let deleted = ChunkId::from_bytes([7; 32]);
        let kept = ChunkId::from_bytes([8; 32]);
        fixture.commit_files(&[deleted, kept], 1);
        assert!(
            fixture
                .checkpoints
                .publish_committed(&fixture.source)
                .unwrap()
                .unwrap()
                .is_base()
        );
        fixture.commit_files(&[kept], 2);
        assert!(
            !fixture
                .checkpoints
                .publish_committed(&fixture.source)
                .unwrap()
                .unwrap()
                .is_base()
        );

        let selected = BTreeSet::from([deleted, kept]);
        let cache = CheckpointProtectedChunkCache::default();
        let protected = fixture
            .checkpoints
            .protected_chunks_matching(&selected, Some(&cache))
            .unwrap();
        assert_eq!(
            protected.keys().copied().collect::<BTreeSet<_>>(),
            selected,
            "the older head still names the deleted file"
        );
        assert_eq!(cache.cached_file_count_for_test(), 2);

        fixture.commit_files(&[kept], 3);
        assert!(
            !fixture
                .checkpoints
                .publish_committed(&fixture.source)
                .unwrap()
                .unwrap()
                .is_base()
        );
        let full_before = crate::direct_io::READ_BYTES.with(std::cell::Cell::get);
        let uncached = fixture
            .checkpoints
            .protected_chunks_matching(&selected, None)
            .unwrap();
        let full_reads = crate::direct_io::READ_BYTES.with(std::cell::Cell::get) - full_before;
        let cached_before = crate::direct_io::READ_BYTES.with(std::cell::Cell::get);
        let cached = fixture
            .checkpoints
            .protected_chunks_matching(&selected, Some(&cache))
            .unwrap();
        let cached_reads = crate::direct_io::READ_BYTES.with(std::cell::Cell::get) - cached_before;
        assert_eq!(cached, uncached);
        assert_eq!(
            cached.keys().copied().collect::<Vec<_>>(),
            vec![kept],
            "superseded chain objects protect nothing once no retained point reaches them"
        );
        assert_eq!(cache.cached_file_count_for_test(), 3);
        assert!(
            cached_reads < full_reads,
            "cached protection rereads only the new Delta: cached={cached_reads} full={full_reads}"
        );
    }

    #[test]
    fn scrub_and_recovery_reject_a_delta_without_prefix_closure() {
        let fixture = Fixture::new("checkpoint-chain-closure");
        let first = fixture.commit_files(&[ChunkId::from_bytes([3; 32])], 1);
        fixture
            .checkpoints
            .publish_committed(&fixture.source)
            .unwrap();
        let second = fixture.commit_files(&[ChunkId::from_bytes([4; 32])], 2);

        let state = fixture.checkpoints.adopt_selected_chain().unwrap().unwrap();
        let mut increment = fixture
            .source
            .scan_recovery_checkpoint_increment(second, |object_id| state.facts(object_id))
            .unwrap();
        let leaf = increment
            .objects
            .iter()
            .find(|(_, facts)| facts.kind() == RecoveryCheckpointObjectKind::ManifestLeaf)
            .map(|(object_id, _)| *object_id)
            .unwrap();
        // Simulate a writer that forgets one new child of an embedded parent.
        increment.objects.remove(&leaf);
        fixture
            .checkpoints
            .publish_chain_file(second, &increment, Some(state), true, |object_id| {
                Ok(fixture.read_source_object(object_id))
            })
            .unwrap();

        assert!(fixture.checkpoints.scrub(&AcceptAll).is_err());
        let replacement = crate::FsStorageIo::open(fixture.root.join("replacement")).unwrap();
        let target = GenerationRepository::new(replacement, PolicySetId::new([1; 32]).unwrap());
        let recovered = fixture
            .checkpoints
            .recover_latest(&target, &AcceptAll)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.record(), first);
    }
}
