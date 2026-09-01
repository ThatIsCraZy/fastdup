use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;

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
use crate::manifest_tree::{ManifestTreeError, scan_manifest_tree};
use crate::{MAX_STORAGE_RANGE_BYTES, StorageIo, StoreError};

const CHECKPOINT_PREFIX: &str = "recovery-checkpoint.";
const CHECKPOINT_SUFFIX: &str = ".fdrc";
const HEAD_NAMES: [&str; 2] = ["recovery-checkpoint.0.head", "recovery-checkpoint.1.head"];
const WRITE_BLOCK_BYTES: usize = 4_096;

#[derive(Clone, Debug)]
pub struct RecoveryCheckpointRepository<I> {
    storage: I,
}

impl<I: StorageIo> RecoveryCheckpointRepository<I> {
    #[must_use]
    pub const fn new(storage: I) -> Self {
        Self { storage }
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
    pub fn publish<M: StorageIo>(
        &self,
        source: &GenerationRepository<M>,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<Option<RecoveryCheckpointSummary>, RecoveryCheckpointError> {
        source.publish_latest_recovery_checkpoint_to(self, verifier)
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
    pub fn recover_latest<M: StorageIo>(
        &self,
        target: &GenerationRepository<M>,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<Option<RecoveredGeneration>, RecoveryCheckpointError> {
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

    pub(crate) fn protected_chunks(
        &self,
    ) -> Result<BTreeMap<ChunkId, u64>, RecoveryCheckpointError> {
        let mut protected = BTreeMap::new();
        let mut complete = 0_usize;
        for (head, name) in self.head_candidates(false)? {
            let audited = match self.audit_head_candidate(head, &name) {
                Ok(audited) => audited,
                Err(error) if error.is_candidate_corruption() => continue,
                Err(error) => return Err(error),
            };
            let (_, required) = match self.scan_graph(&audited) {
                Ok(scanned) => scanned,
                Err(error) if error.is_candidate_corruption() => continue,
                Err(error) => return Err(error),
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

    #[allow(clippy::too_many_lines)]
    pub(crate) fn publish_source<F>(
        &self,
        record: CommitRecord,
        object_ids: &BTreeSet<MetadataObjectId>,
        verifier: &dyn RequiredChunkVerifier,
        mut read_object: F,
    ) -> Result<RecoveryCheckpointSummary, RecoveryCheckpointError>
    where
        F: FnMut(MetadataObjectId) -> Result<Vec<u8>, RecoveryCheckpointError>,
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
            let summary = self.verify_graph(&audited, verifier)?;
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
        self.storage
            .write_at(&temporary_name, commit_offset, &encoded_record)?;
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
            self.storage
                .write_at(&temporary_name, cursor, &encoded_header)?;
            body_hasher.update(&encoded_header);
            let payload_offset = cursor
                .checked_add(
                    u64::try_from(encoded_header.len())
                        .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?,
                )
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?;
            write_blocks(&self.storage, &temporary_name, payload_offset, &encoded)?;
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
                self.storage.write_at(
                    &temporary_name,
                    cursor
                        .checked_add(unpadded_length)
                        .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?,
                    padding,
                )?;
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
        self.storage
            .write_at(&temporary_name, 0, &descriptor.encode_header())?;
        self.storage
            .write_at(&temporary_name, cursor, &descriptor.encode_footer())?;
        self.storage.set_len(&temporary_name, file_length)?;
        let audited = self.audit_named(&temporary_name)?;
        if audited.record != record || !audited.objects.keys().eq(object_ids.iter()) {
            return Err(RecoveryCheckpointError::IdentityMismatch);
        }
        let summary = self.verify_graph(&audited, verifier)?;
        self.storage.sync_file(&temporary_name)?;
        match self
            .storage
            .publish_noreplace(&temporary_name, &published_name)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let raced = self.audit_named(&published_name)?;
                if raced.record != record || !raced.objects.keys().eq(object_ids.iter()) {
                    return Err(RecoveryCheckpointError::IdentityMismatch);
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.storage.sync_root()?;
        let obsolete = self.publish_head(audited.descriptor)?;
        self.prune_obsolete(&obsolete)?;
        debug_assert_eq!(summary.metadata_payload_bytes, metadata_payload_bytes);
        debug_assert_eq!(summary.file_length, file_length);
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

    fn verify_graph_with_chunks(
        &self,
        checkpoint: &AuditedCheckpoint,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<(RecoveryCheckpointSummary, BTreeMap<ChunkId, u64>), RecoveryCheckpointError> {
        let (_root, required) = self.scan_graph(checkpoint)?;
        verifier.verify_required_chunks(&required)?;
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

    fn read_object(
        &self,
        checkpoint: &AuditedCheckpoint,
        object_id: MetadataObjectId,
    ) -> Result<Vec<u8>, RecoveryCheckpointError> {
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
        Ok(encoded)
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

fn write_blocks<I: StorageIo>(
    storage: &I,
    name: &str,
    offset: u64,
    bytes: &[u8],
) -> Result<(), RecoveryCheckpointError> {
    for (ordinal, block) in bytes.chunks(WRITE_BLOCK_BYTES).enumerate() {
        let block_offset = u64::try_from(
            ordinal
                .checked_mul(WRITE_BLOCK_BYTES)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?,
        )
        .map_err(|_| RecoveryCheckpointError::ArithmeticOverflow)?;
        storage.write_at(
            name,
            offset
                .checked_add(block_offset)
                .ok_or(RecoveryCheckpointError::ArithmeticOverflow)?,
            block,
        )?;
    }
    Ok(())
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
