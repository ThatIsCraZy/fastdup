use crate::{ContainerRepository, StorageIo, StoreError};
use fastdup_format::{
    COMMIT_RECORD_BYTES, CommitFormatError, CommitRecord, CommitRecordHash,
    MAX_METADATA_OBJECT_BYTES, ManifestExtent, ManifestLeaf, MetadataFormatError, MetadataObjectId,
    NamespaceRoot, PolicySetId,
};
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

const COMMIT_WAL_NAME: &str = "commit.wal";
const METADATA_SUFFIX: &str = ".fdm";
const MAX_COMMIT_WAL_BYTES: usize = 64 * 1_024 * 1_024;
const WRITE_BLOCK_BYTES: usize = 4_096;

#[derive(Clone, Debug)]
pub struct GenerationRepository<I> {
    storage: I,
    supported_policy: PolicySetId,
    commit_lock: Arc<Mutex<()>>,
}

impl<I: StorageIo> GenerationRepository<I> {
    #[must_use]
    pub fn new(storage: I, supported_policy: PolicySetId) -> Self {
        Self {
            storage,
            supported_policy,
            commit_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Publishes one verified immutable Manifest metadata object.
    ///
    /// Identical content-addressed objects are reused. A same-name object with
    /// different or invalid bytes fails closed.
    ///
    /// # Errors
    ///
    /// Returns format, bounded-size, identity, or durable-publication errors.
    pub fn publish_manifest(
        &self,
        manifest: &ManifestLeaf,
    ) -> Result<MetadataObjectId, GenerationError> {
        let encoded = manifest.encode()?;
        self.publish_metadata(&encoded)
    }

    /// Loads and fully verifies one immutable Manifest by Metadata Object ID.
    ///
    /// # Errors
    ///
    /// Returns bounded I/O, envelope-identity, or Manifest-format errors.
    pub fn read_manifest(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<ManifestLeaf, GenerationError> {
        let bytes = self.read_metadata(object_id)?;
        ManifestLeaf::decode(&bytes).map_err(Into::into)
    }

    /// Publishes a complete Namespace Root and appends its Commit Record last.
    ///
    /// This checkpoint accepts only manifests made entirely of HOLE/FILL
    /// extents. DATA references are refused until verified Container locations
    /// are connected to generation recovery.
    ///
    /// # Errors
    ///
    /// Returns graph verification, publication, WAL-chain, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn commit_namespace(&self, root: &NamespaceRoot) -> Result<CommitRecord, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        self.verify_manifest_graph::<I>(root, None)?;
        self.commit_verified_namespace(root)
    }

    /// Publishes a Namespace Root after verifying every reachable DATA Chunk
    /// against one independently supplied durable Container Repository.
    ///
    /// # Errors
    ///
    /// Returns graph, container, publication, WAL-chain, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn commit_namespace_with_data<J: StorageIo>(
        &self,
        root: &NamespaceRoot,
        containers: &ContainerRepository<J>,
    ) -> Result<CommitRecord, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        self.verify_manifest_graph(root, Some(containers))?;
        self.commit_verified_namespace(root)
    }

    fn commit_verified_namespace(
        &self,
        root: &NamespaceRoot,
    ) -> Result<CommitRecord, GenerationError> {
        let encoded_root = root.encode()?;
        let root_id = self.publish_metadata(&encoded_root)?;
        self.ensure_wal_exists()?;
        let wal = self.read_wal()?;
        let prefix = decode_wal_prefix(&wal);
        if prefix.tail != WalTail::Clean {
            return Err(GenerationError::WalNeedsRepair(prefix.tail));
        }
        if let Some(previous) = prefix.records.last() {
            self.verify_generation_transition(*previous, root)?;
        } else if root.inode_allocation_cursor() != 2 || !root.inodes().is_empty() {
            return Err(GenerationError::InitialInodeReservationRequired);
        }
        let (generation, previous_hash) = match prefix.records.last() {
            Some(previous) => (
                previous
                    .generation()
                    .checked_add(1)
                    .ok_or(GenerationError::GenerationExhausted)?,
                CommitRecordHash::of(&wal[wal.len() - COMMIT_RECORD_BYTES..wal.len()]),
            ),
            None => (1, CommitRecordHash::ZERO),
        };
        let record = CommitRecord::new(
            generation,
            previous_hash,
            root_id,
            self.supported_policy,
            root.namespace_mutation_sequence(),
            root.inode_reservation_end(),
            root.inode_allocation_cursor(),
        )?;
        let encoded_record = record.encode();
        let offset = u64::try_from(wal.len()).map_err(|_| GenerationError::WalTooLarge)?;
        let new_length = wal
            .len()
            .checked_add(COMMIT_RECORD_BYTES)
            .ok_or(GenerationError::WalTooLarge)?;
        if new_length > MAX_COMMIT_WAL_BYTES {
            return Err(GenerationError::WalTooLarge);
        }
        self.storage
            .write_at(COMMIT_WAL_NAME, offset, &encoded_record)?;
        self.storage.set_len(
            COMMIT_WAL_NAME,
            u64::try_from(new_length).map_err(|_| GenerationError::WalTooLarge)?,
        )?;
        let reread = self.read_wal()?;
        if reread.len() != new_length
            || reread[..wal.len()] != wal
            || reread[wal.len()..] != encoded_record
            || decode_wal_prefix(&reread).tail != WalTail::Clean
        {
            return Err(GenerationError::PublishVerificationMismatch);
        }
        self.storage.sync_file(COMMIT_WAL_NAME)?;
        Ok(record)
    }

    /// Recovers the newest wholly verified generation supported by this writer.
    ///
    /// A torn or invalid WAL tail and an invalid newest metadata graph fall back
    /// to an earlier complete generation. No object fragments are merged.
    ///
    /// # Errors
    ///
    /// Returns I/O errors or `NoRecoverableGeneration` when a WAL exists but no
    /// supported record has a complete reachable graph.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn recover_latest(&self) -> Result<Option<RecoveredGeneration>, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        self.recover_latest_using::<I>(None)
    }

    /// Recovers the newest generation whose metadata graph and reachable DATA
    /// chunks are all independently verified.
    ///
    /// # Errors
    ///
    /// Returns I/O, format, container-integrity, or graph-completeness errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn recover_latest_with_data<J: StorageIo>(
        &self,
        containers: &ContainerRepository<J>,
    ) -> Result<Option<RecoveredGeneration>, GenerationError> {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        self.recover_latest_using(Some(containers))
    }

    fn recover_latest_using<J: StorageIo>(
        &self,
        containers: Option<&ContainerRepository<J>>,
    ) -> Result<Option<RecoveredGeneration>, GenerationError> {
        let wal = match self.storage.read(COMMIT_WAL_NAME) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if wal.len() > MAX_COMMIT_WAL_BYTES {
            return Err(GenerationError::WalTooLarge);
        }
        let prefix = decode_wal_prefix(&wal);
        if prefix.records.is_empty() {
            return if wal.is_empty() {
                Ok(None)
            } else {
                Err(GenerationError::NoRecoverableGeneration)
            };
        }
        let latest_generation = match prefix.records.last() {
            Some(record) => record.generation(),
            None => return Err(GenerationError::NoRecoverableGeneration),
        };
        let inode_reservation_end_high_water = prefix
            .records
            .iter()
            .map(|record| record.inode_reservation_end())
            .max()
            .ok_or(GenerationError::NoRecoverableGeneration)?;
        let mut previous: Option<(CommitRecord, NamespaceRoot)> = None;
        let mut selected: Option<(CommitRecord, NamespaceRoot)> = None;
        for record in &prefix.records {
            if record.policy_set() != self.supported_policy {
                return Err(GenerationError::UnsupportedPolicySet {
                    generation: record.generation(),
                    policy_set: record.policy_set(),
                });
            }
            let root = match self.read_namespace_root(record.namespace_root()) {
                Ok(root) => root,
                Err(error) if error.allows_generation_fallback() => break,
                Err(error) => return Err(error),
            };
            if root.namespace_mutation_sequence() != record.namespace_mutation_cutoff()
                || root.inode_reservation_end() != record.inode_reservation_end()
                || root.inode_allocation_cursor() != record.inode_allocation_cursor()
            {
                break;
            }
            match self.verify_manifest_graph(&root, containers) {
                Ok(()) => {}
                Err(error) if error.allows_generation_fallback() => break,
                Err(error) => return Err(error),
            }
            match &previous {
                Some((previous_record, previous_root)) => {
                    if verify_generation_transition_pair(*previous_record, previous_root, &root)
                        .is_err()
                    {
                        break;
                    }
                }
                None if root.inode_allocation_cursor() != 2 || !root.inodes().is_empty() => break,
                None => {}
            }
            previous = Some((*record, root.clone()));
            selected = Some((*record, root));
        }
        let Some((record, namespace_root)) = selected else {
            return Err(GenerationError::NoRecoverableGeneration);
        };
        Ok(Some(RecoveredGeneration {
            record,
            namespace_root,
            wal_tail: prefix.tail,
            rejected_newer_generations: latest_generation - record.generation(),
            inode_reservation_end_high_water,
        }))
    }

    fn publish_metadata(&self, encoded: &[u8]) -> Result<MetadataObjectId, GenerationError> {
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
            self.storage.sync_root()?;
            return Ok(object_id);
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
        self.storage.sync_root()?;
        Ok(object_id)
    }

    fn ensure_wal_exists(&self) -> Result<(), GenerationError> {
        if self.storage.exists(COMMIT_WAL_NAME)? {
            let bytes = self.storage.read(COMMIT_WAL_NAME)?;
            if bytes.len() > MAX_COMMIT_WAL_BYTES {
                return Err(GenerationError::WalTooLarge);
            }
            self.storage.sync_root()?;
            return Ok(());
        }
        self.storage.create_new(COMMIT_WAL_NAME)?;
        self.storage.set_len(COMMIT_WAL_NAME, 0)?;
        self.storage.sync_file(COMMIT_WAL_NAME)?;
        self.storage.sync_root()?;
        Ok(())
    }

    fn read_wal(&self) -> Result<Vec<u8>, GenerationError> {
        let bytes = self.storage.read(COMMIT_WAL_NAME)?;
        if bytes.len() > MAX_COMMIT_WAL_BYTES {
            return Err(GenerationError::WalTooLarge);
        }
        Ok(bytes)
    }

    fn read_namespace_root(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<NamespaceRoot, GenerationError> {
        let bytes = self.read_metadata(object_id)?;
        NamespaceRoot::decode(&bytes).map_err(Into::into)
    }

    fn read_metadata(&self, object_id: MetadataObjectId) -> Result<Vec<u8>, GenerationError> {
        let bytes = self.storage.read(&metadata_name(object_id))?;
        if bytes.len() > MAX_METADATA_OBJECT_BYTES
            || MetadataObjectId::from_encoded(&bytes)? != object_id
        {
            return Err(GenerationError::MetadataIdentityCollision(object_id));
        }
        Ok(bytes)
    }

    fn verify_manifest_graph<J: StorageIo>(
        &self,
        root: &NamespaceRoot,
        containers: Option<&ContainerRepository<J>>,
    ) -> Result<(), GenerationError> {
        let mut required_chunks = BTreeMap::new();
        for inode in root.inodes() {
            let bytes = self.read_metadata(inode.manifest_root())?;
            let manifest = ManifestLeaf::decode(&bytes)?;
            if manifest.file_length() != inode.logical_size() {
                return Err(GenerationError::ManifestLengthMismatch {
                    inode: inode.inode(),
                    inode_length: inode.logical_size(),
                    manifest_length: manifest.file_length(),
                });
            }
            for extent in manifest.extents() {
                let ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                } = *extent
                else {
                    continue;
                };
                if let Some(previous_length) = required_chunks.insert(chunk_id, logical_length)
                    && previous_length != logical_length
                {
                    return Err(GenerationError::ManifestChunkLengthConflict {
                        chunk_id,
                        first_length: previous_length,
                        second_length: logical_length,
                    });
                }
            }
        }
        if required_chunks.is_empty() {
            return Ok(());
        }
        let Some(containers) = containers else {
            return Err(GenerationError::DataLocationsNotConnected);
        };
        containers.verify_required_chunks(&required_chunks)?;
        Ok(())
    }

    fn verify_generation_transition(
        &self,
        previous_record: CommitRecord,
        proposed_root: &NamespaceRoot,
    ) -> Result<(), GenerationError> {
        let previous_root = self.read_namespace_root(previous_record.namespace_root())?;
        if previous_root.namespace_mutation_sequence()
            != previous_record.namespace_mutation_cutoff()
            || previous_root.inode_reservation_end() != previous_record.inode_reservation_end()
            || previous_root.inode_allocation_cursor() != previous_record.inode_allocation_cursor()
        {
            return Err(GenerationError::PreviousGenerationRecordMismatch);
        }
        verify_generation_transition_pair(previous_record, &previous_root, proposed_root)
    }
}

fn verify_generation_transition_pair(
    previous_record: CommitRecord,
    previous_root: &NamespaceRoot,
    proposed_root: &NamespaceRoot,
) -> Result<(), GenerationError> {
    if proposed_root.namespace_mutation_sequence() < previous_record.namespace_mutation_cutoff() {
        return Err(GenerationError::NonMonotonicNamespaceMutation {
            previous: previous_record.namespace_mutation_cutoff(),
            proposed: proposed_root.namespace_mutation_sequence(),
        });
    }
    if proposed_root.inode_reservation_end() < previous_record.inode_reservation_end() {
        return Err(GenerationError::NonMonotonicInodeReservation {
            previous: previous_record.inode_reservation_end(),
            proposed: proposed_root.inode_reservation_end(),
        });
    }
    if proposed_root.inode_allocation_cursor() < previous_record.inode_allocation_cursor() {
        return Err(GenerationError::NonMonotonicInodeAllocation {
            previous: previous_record.inode_allocation_cursor(),
            proposed: proposed_root.inode_allocation_cursor(),
        });
    }
    if proposed_root.inode_allocation_cursor() > previous_record.inode_reservation_end() {
        return Err(
            GenerationError::AllocationExceededPreviouslyDurableReservation {
                previous_reservation_end: previous_record.inode_reservation_end(),
                proposed_allocation_cursor: proposed_root.inode_allocation_cursor(),
            },
        );
    }
    for proposed_inode in proposed_root.inodes() {
        match previous_root
            .inodes()
            .binary_search_by_key(&proposed_inode.inode(), fastdup_format::DurableInode::inode)
        {
            Ok(previous_index) => {
                let previous_inode = &previous_root.inodes()[previous_index];
                if proposed_inode.mutation_sequence() < previous_inode.mutation_sequence() {
                    return Err(GenerationError::NonMonotonicInodeMutation {
                        inode: proposed_inode.inode(),
                        previous: previous_inode.mutation_sequence(),
                        proposed: proposed_inode.mutation_sequence(),
                    });
                }
            }
            Err(_) if proposed_inode.inode() < previous_record.inode_allocation_cursor() => {
                return Err(GenerationError::ReusedInodeId {
                    inode: proposed_inode.inode(),
                    previous_allocation_cursor: previous_record.inode_allocation_cursor(),
                });
            }
            Err(_) => {}
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalTail {
    Clean,
    Torn {
        valid_bytes: usize,
        tail_bytes: usize,
    },
    InvalidRecord {
        offset: usize,
    },
    BrokenChain {
        offset: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredGeneration {
    record: CommitRecord,
    namespace_root: NamespaceRoot,
    wal_tail: WalTail,
    rejected_newer_generations: u64,
    inode_reservation_end_high_water: u64,
}

impl RecoveredGeneration {
    #[must_use]
    pub const fn record(&self) -> CommitRecord {
        self.record
    }

    #[must_use]
    pub const fn namespace_root(&self) -> &NamespaceRoot {
        &self.namespace_root
    }

    #[must_use]
    pub const fn wal_tail(&self) -> &WalTail {
        &self.wal_tail
    }

    #[must_use]
    pub const fn rejected_newer_generations(&self) -> u64 {
        self.rejected_newer_generations
    }

    /// Returns the newest reservation carried by the valid WAL prefix, even
    /// when recovery selected an older Namespace Root after corruption.
    #[must_use]
    pub const fn inode_reservation_end_high_water(&self) -> u64 {
        self.inode_reservation_end_high_water
    }
}

#[derive(Debug)]
pub enum GenerationError {
    Io(io::Error),
    MetadataFormat(MetadataFormatError),
    CommitFormat(CommitFormatError),
    Store(StoreError),
    MetadataTooLarge,
    WalTooLarge,
    GenerationExhausted,
    UnsupportedPolicySet {
        generation: u64,
        policy_set: PolicySetId,
    },
    NonMonotonicNamespaceMutation {
        previous: u64,
        proposed: u64,
    },
    NonMonotonicInodeReservation {
        previous: u64,
        proposed: u64,
    },
    NonMonotonicInodeAllocation {
        previous: u64,
        proposed: u64,
    },
    NonMonotonicInodeMutation {
        inode: u64,
        previous: u64,
        proposed: u64,
    },
    PreviousGenerationRecordMismatch,
    InitialInodeReservationRequired,
    AllocationExceededPreviouslyDurableReservation {
        previous_reservation_end: u64,
        proposed_allocation_cursor: u64,
    },
    ReusedInodeId {
        inode: u64,
        previous_allocation_cursor: u64,
    },
    PublishVerificationMismatch,
    MetadataIdentityCollision(MetadataObjectId),
    WalNeedsRepair(WalTail),
    NoRecoverableGeneration,
    DataLocationsNotConnected,
    ManifestLengthMismatch {
        inode: u64,
        inode_length: u64,
        manifest_length: u64,
    },
    ManifestChunkLengthConflict {
        chunk_id: fastdup_format::ChunkId,
        first_length: u64,
        second_length: u64,
    },
}

impl GenerationError {
    fn allows_generation_fallback(&self) -> bool {
        match self {
            Self::Io(error) | Self::Store(StoreError::Io(error)) => {
                error.kind() == io::ErrorKind::NotFound
            }
            Self::MetadataFormat(_)
            | Self::MetadataIdentityCollision(_)
            | Self::ManifestLengthMismatch { .. }
            | Self::ManifestChunkLengthConflict { .. }
            | Self::Store(
                StoreError::Format(_)
                | StoreError::InvalidPublishedName(_)
                | StoreError::PublishedIdentityMismatch { .. }
                | StoreError::MissingVerifiedChunk { .. },
            ) => true,
            Self::CommitFormat(_)
            | Self::Store(StoreError::PublishVerificationMismatch)
            | Self::MetadataTooLarge
            | Self::WalTooLarge
            | Self::GenerationExhausted
            | Self::UnsupportedPolicySet { .. }
            | Self::NonMonotonicNamespaceMutation { .. }
            | Self::NonMonotonicInodeReservation { .. }
            | Self::NonMonotonicInodeAllocation { .. }
            | Self::NonMonotonicInodeMutation { .. }
            | Self::PreviousGenerationRecordMismatch
            | Self::InitialInodeReservationRequired
            | Self::AllocationExceededPreviouslyDurableReservation { .. }
            | Self::ReusedInodeId { .. }
            | Self::PublishVerificationMismatch
            | Self::WalNeedsRepair(_)
            | Self::NoRecoverableGeneration
            | Self::DataLocationsNotConnected => false,
        }
    }
}

impl fmt::Display for GenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for GenerationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::MetadataFormat(error) => Some(error),
            Self::CommitFormat(error) => Some(error),
            Self::Store(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for GenerationError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<MetadataFormatError> for GenerationError {
    fn from(error: MetadataFormatError) -> Self {
        Self::MetadataFormat(error)
    }
}

impl From<CommitFormatError> for GenerationError {
    fn from(error: CommitFormatError) -> Self {
        Self::CommitFormat(error)
    }
}

impl From<StoreError> for GenerationError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

struct WalPrefix {
    records: Vec<CommitRecord>,
    tail: WalTail,
}

fn decode_wal_prefix(bytes: &[u8]) -> WalPrefix {
    let complete_bytes = bytes.len() / COMMIT_RECORD_BYTES * COMMIT_RECORD_BYTES;
    let mut records = Vec::new();
    let mut previous_encoded: Option<&[u8]> = None;
    let mut previous_record: Option<CommitRecord> = None;
    for offset in (0..complete_bytes).step_by(COMMIT_RECORD_BYTES) {
        let encoded = &bytes[offset..offset + COMMIT_RECORD_BYTES];
        let Ok(record) = CommitRecord::decode(encoded) else {
            return WalPrefix {
                records,
                tail: WalTail::InvalidRecord { offset },
            };
        };
        let expected_generation =
            u64::try_from(records.len()).expect("ASSERT: bounded WAL record count fits u64") + 1;
        let expected_hash = previous_encoded.map_or(CommitRecordHash::ZERO, CommitRecordHash::of);
        if record.generation() != expected_generation
            || record.previous_record_hash() != expected_hash
            || previous_record.is_some_and(|previous| {
                record.namespace_mutation_cutoff() < previous.namespace_mutation_cutoff()
                    || record.inode_reservation_end() < previous.inode_reservation_end()
                    || record.inode_allocation_cursor() < previous.inode_allocation_cursor()
            })
        {
            return WalPrefix {
                records,
                tail: WalTail::BrokenChain { offset },
            };
        }
        records.push(record);
        previous_encoded = Some(encoded);
        previous_record = Some(record);
    }
    let tail_bytes = bytes.len() - complete_bytes;
    let tail = if tail_bytes == 0 {
        WalTail::Clean
    } else {
        WalTail::Torn {
            valid_bytes: complete_bytes,
            tail_bytes,
        }
    };
    WalPrefix { records, tail }
}

fn metadata_name(object_id: MetadataObjectId) -> String {
    format!("{}{}", encode_object_id(object_id), METADATA_SUFFIX)
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

#[cfg(test)]
mod tests {
    use super::*;
    use fastdup_format::{DurableInode, NamespaceEntry};

    fn inode(inode: u64, mutation_sequence: u64) -> DurableInode {
        DurableInode::new(
            inode,
            0o600,
            1_000,
            1_001,
            1,
            mutation_sequence,
            0,
            MetadataObjectId::new([0xA5; 32]).expect("fixture Manifest ID is nonzero"),
        )
        .expect("fixture regular inode is valid")
    }

    fn root(
        reservation_end: u64,
        allocation_cursor: u64,
        namespace_mutation_sequence: u64,
        inodes: Vec<DurableInode>,
    ) -> NamespaceRoot {
        let entries = inodes
            .iter()
            .map(|inode| {
                NamespaceEntry::new(
                    1,
                    inode.inode(),
                    format!("inode-{}", inode.inode()).into_bytes(),
                )
                .expect("fixture root entry is valid")
            })
            .collect();
        NamespaceRoot::new(
            reservation_end,
            allocation_cursor,
            namespace_mutation_sequence,
            inodes,
            entries,
        )
        .expect("fixture Namespace Root is valid")
    }

    fn record_for(root: &NamespaceRoot) -> CommitRecord {
        CommitRecord::new(
            2,
            CommitRecordHash::from_bytes([0xB6; 32]),
            MetadataObjectId::new([0xC7; 32]).expect("fixture Namespace Root ID is nonzero"),
            PolicySetId::new([0xD8; 32]).expect("fixture Policy Set ID is nonzero"),
            root.namespace_mutation_sequence(),
            root.inode_reservation_end(),
            root.inode_allocation_cursor(),
        )
        .expect("fixture previous Commit Record is valid")
    }

    #[test]
    fn transition_pair_rejects_a_decreasing_per_inode_mutation_sequence() {
        let previous_root = root(128, 16, 20, vec![inode(5, 7)]);
        let previous_record = record_for(&previous_root);
        let proposed_root = root(128, 16, 21, vec![inode(5, 6)]);

        assert!(matches!(
            verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
            Err(GenerationError::NonMonotonicInodeMutation {
                inode: 5,
                previous: 7,
                proposed: 6,
            })
        ));
    }

    #[test]
    fn transition_pair_rejects_a_removed_inode_reused_below_the_allocation_cursor() {
        let previous_root = root(128, 16, 20, Vec::new());
        let previous_record = record_for(&previous_root);
        let proposed_root = root(128, 16, 21, vec![inode(5, 8)]);

        assert!(matches!(
            verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
            Err(GenerationError::ReusedInodeId {
                inode: 5,
                previous_allocation_cursor: 16,
            })
        ));
    }

    #[test]
    fn transition_pair_rejects_consuming_a_reservation_first_enlarged_by_the_proposal() {
        let previous_root = root(128, 16, 20, Vec::new());
        let previous_record = record_for(&previous_root);
        let proposed_root = root(256, 129, 21, vec![inode(128, 8)]);

        assert!(matches!(
            verify_generation_transition_pair(previous_record, &previous_root, &proposed_root),
            Err(
                GenerationError::AllocationExceededPreviouslyDurableReservation {
                    previous_reservation_end: 128,
                    proposed_allocation_cursor: 129,
                }
            )
        ));
    }
}
