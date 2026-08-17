use crate::generation_log::{GenerationLog, GenerationLogError};
use crate::manifest_tree::{
    ManifestRangeExtent, ManifestTreeError, ManifestTreeSummary, encode_manifest_tree,
    flatten_manifest_tree, read_manifest_tree_range, rewrite_manifest_tree_range,
    scan_manifest_tree,
};
use crate::{
    ActivatedExactIndex, ContainerRepository, StorageIo, StoreError, VerifiedManifestFile,
};
use fastdup_format::{
    CommitFormatError, CommitRecord, CommitRecordHash, MAX_METADATA_OBJECT_BYTES, ManifestExtent,
    ManifestLeaf, MetadataFormatError, MetadataObjectId, NamespaceRoot, PolicySetId,
};
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};

const METADATA_SUFFIX: &str = ".fdm";
const WRITE_BLOCK_BYTES: usize = 4_096;
const MAX_METADATA_OBJECT_BYTES_U64: u64 = 16 * 1_024 * 1_024;
type VerifiedManifests = Vec<(u64, ManifestTreeSummary)>;

struct RecoveredGraph {
    generation: RecoveredGeneration,
    manifests: VerifiedManifests,
}

struct SelectedGraph {
    record: CommitRecord,
    root: NamespaceRoot,
    manifests: VerifiedManifests,
}

#[derive(Clone, Debug)]
pub struct GenerationRepository<I> {
    storage: I,
    supported_policy: PolicySetId,
    commit_lock: Arc<Mutex<()>>,
}

/// Verifies that every logical Chunk required by one metadata graph has at
/// least one durable, byte-exact physical Location.
///
/// Implementations may use rebuild scans or nonauthoritative acceleration, but
/// success is a complete graph proof. A negative index hint alone can never
/// satisfy this interface.
pub trait RequiredChunkVerifier {
    /// Verifies every unique `(Chunk ID, logical length)` dependency.
    ///
    /// # Errors
    ///
    /// Returns the first missing, conflicting, corrupt, unsupported, or I/O
    /// failure without exposing a partial proof.
    fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError>;
}

impl<I: StorageIo> RequiredChunkVerifier for ContainerRepository<I> {
    fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError> {
        ContainerRepository::verify_required_chunks(self, required)
    }
}

/// Complete graph verifier using one pinned Exact-Index generation with the
/// authoritative verified Container scan as a single fallback.
#[derive(Clone, Debug)]
pub struct IndexedRequiredChunkVerifier<C, X> {
    containers: ContainerRepository<C>,
    index: Arc<ActivatedExactIndex<X>>,
}

impl<C, X> IndexedRequiredChunkVerifier<C, X> {
    #[must_use]
    pub const fn new(
        containers: ContainerRepository<C>,
        index: Arc<ActivatedExactIndex<X>>,
    ) -> Self {
        Self { containers, index }
    }
}

impl<C: StorageIo, X: StorageIo> RequiredChunkVerifier for IndexedRequiredChunkVerifier<C, X> {
    fn verify_required_chunks(
        &self,
        required: &BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), StoreError> {
        for (chunk_id, logical_length) in required {
            if self
                .containers
                .find_verified_chunk_with_index(&self.index, *chunk_id, *logical_length)?
                .is_none()
            {
                return self.containers.verify_required_chunks(required);
            }
        }
        Ok(())
    }
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
    ///
    /// # Panics
    ///
    /// Panics only if a previously content-identified tree plan disagrees with
    /// the generic Metadata Object writer, an impossible internal invariant.
    pub fn publish_manifest(
        &self,
        manifest: &ManifestLeaf,
    ) -> Result<MetadataObjectId, GenerationError> {
        let tree = encode_manifest_tree(manifest)?;
        for (expected_id, encoded) in tree.objects() {
            let published_id = self.stage_metadata(encoded)?;
            assert_eq!(
                published_id, *expected_id,
                "ASSERT: Manifest tree plan identity must equal published object identity"
            );
        }
        self.storage.sync_root()?;
        Ok(tree.root())
    }

    /// Publishes an equal-length immutable successor by replacing one logical
    /// range and rewriting only the intersecting leaves and their ancestors.
    /// Unchanged subtree object IDs are retained exactly.
    ///
    /// # Errors
    ///
    /// Returns format, predecessor-tree, replacement-boundary, identity, or
    /// durable-publication errors. A replacement boundary inside DATA is
    /// rejected because one DATA extent is the indivisible Chunk identity.
    ///
    /// # Panics
    ///
    /// Panics only if the path-local tree plan disagrees with the generic
    /// content-addressed Metadata Object writer.
    pub fn publish_manifest_replacement(
        &self,
        previous_root: MetadataObjectId,
        expected_logical_size: u64,
        replaced: Range<u64>,
        replacement: &[ManifestExtent],
    ) -> Result<MetadataObjectId, GenerationError> {
        let tree = rewrite_manifest_tree_range(
            previous_root,
            expected_logical_size,
            replaced,
            replacement,
            |node_id| self.read_manifest_node(node_id),
        )?;
        for (expected_id, encoded) in tree.objects() {
            let published_id = self.stage_metadata(encoded)?;
            assert_eq!(
                published_id, *expected_id,
                "ASSERT: path-local Manifest plan identity must equal published object identity"
            );
        }
        self.storage.sync_root()?;
        Ok(tree.root())
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
        flatten_manifest_tree(object_id, |node_id| {
            let name = metadata_name(node_id);
            let length = self.storage.object_len(&name)?;
            if length > MAX_METADATA_OBJECT_BYTES_U64 {
                return Err(ManifestTreeError::IdentityMismatch(node_id));
            }
            let bytes = self.storage.read(&name)?;
            if u64::try_from(bytes.len()) != Ok(length)
                || MetadataObjectId::from_encoded(&bytes)? != node_id
            {
                return Err(ManifestTreeError::IdentityMismatch(node_id));
            }
            Ok(bytes)
        })
        .map_err(Into::into)
    }

    /// Reads and verifies only Manifest tree paths intersecting one range.
    /// Returned extents retain their absolute logical offsets.
    ///
    /// # Errors
    ///
    /// Returns bounded I/O, identity, tree-partition, or arithmetic errors.
    pub fn read_manifest_range(
        &self,
        object_id: MetadataObjectId,
        expected_logical_size: u64,
        range: Range<u64>,
    ) -> Result<Vec<ManifestRangeExtent>, GenerationError> {
        let length = range
            .end
            .checked_sub(range.start)
            .ok_or(ManifestTreeError::InvalidReplacement)?;
        read_manifest_tree_range(
            object_id,
            expected_logical_size,
            range.start,
            length,
            |node_id| self.read_manifest_node(node_id),
        )
        .map_err(Into::into)
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
        self.verify_manifest_graph(root, None)?;
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

    /// Commits a DATA-bearing Namespace Root and returns the Manifest readers
    /// proven by the same complete graph verification.
    ///
    /// The returned readers do not repeat dependency discovery. Demand reads
    /// still re-verify the selected immutable Container before returning data.
    /// Callers cannot construct this proof or attach an unrelated Manifest.
    ///
    /// # Errors
    ///
    /// Returns graph, container, bounded-allocation, publication, WAL-chain,
    /// or durability errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn commit_namespace_with_verified_files<J>(
        &self,
        root: &NamespaceRoot,
        containers: &ContainerRepository<J>,
    ) -> Result<CommittedDataGeneration<J>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let manifests = self.verify_manifest_graph(root, Some(containers))?;
        let files = verified_files(manifests, &self.storage, containers)?;
        let record = self.commit_verified_namespace(root)?;
        Ok(CommittedDataGeneration { record, files })
    }

    /// Commits a DATA-bearing Namespace Root using an independently supplied
    /// complete dependency verifier and returns readers backed by `containers`.
    ///
    /// This is the indexed counterpart of
    /// [`Self::commit_namespace_with_verified_files`]. The verifier may use
    /// bounded acceleration, but success must cover every required Chunk and a
    /// miss must fall back or fail closed.
    ///
    /// # Errors
    ///
    /// Returns graph, dependency-integrity, bounded-allocation, publication,
    /// WAL-chain, or durability errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn commit_namespace_with_verified_files_using<J>(
        &self,
        root: &NamespaceRoot,
        containers: &ContainerRepository<J>,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<CommittedDataGeneration<J>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let manifests = self.verify_manifest_graph(root, Some(verifier))?;
        let files = verified_files(manifests, &self.storage, containers)?;
        let record = self.commit_verified_namespace(root)?;
        Ok(CommittedDataGeneration { record, files })
    }

    fn commit_verified_namespace(
        &self,
        root: &NamespaceRoot,
    ) -> Result<CommitRecord, GenerationError> {
        let encoded_root = root.encode()?;
        let root_id = self.publish_metadata(&encoded_root)?;
        let log = GenerationLog::new(&self.storage);
        let snapshot = log.load_for_append().map_err(map_log_error)?;
        if snapshot.tail() != &WalTail::Clean {
            return Err(GenerationError::WalNeedsRepair(snapshot.tail().clone()));
        }
        if let Some(previous) = snapshot.last_record() {
            self.verify_generation_transition(previous, root)?;
        } else if root.inode_allocation_cursor() != 2 || !root.inodes().is_empty() {
            return Err(GenerationError::InitialInodeReservationRequired);
        }
        let (generation, previous_hash) = match snapshot.last_record() {
            Some(previous) => (
                previous
                    .generation()
                    .checked_add(1)
                    .ok_or(GenerationError::GenerationExhausted)?,
                snapshot
                    .last_hash()
                    .expect("ASSERT: a last Commit Record has encoded bytes"),
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
        log.append(&snapshot, record).map_err(map_log_error)?;
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
        self.recover_latest_using(None)
            .map(|recovered| recovered.map(|graph| graph.generation))
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
            .map(|recovered| recovered.map(|graph| graph.generation))
    }

    /// Recovers the newest complete DATA generation together with Manifest
    /// readers proven for that selected recovery candidate.
    ///
    /// # Errors
    ///
    /// Returns I/O, format, container-integrity, graph-completeness, or
    /// bounded-allocation errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn recover_latest_with_verified_files<J>(
        &self,
        containers: &ContainerRepository<J>,
    ) -> Result<Option<RecoveredDataGeneration<J>>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let Some(graph) = self.recover_latest_using(Some(containers))? else {
            return Ok(None);
        };
        let files = verified_files(graph.manifests, &self.storage, containers)?;
        Ok(Some(RecoveredDataGeneration {
            generation: graph.generation,
            files,
        }))
    }

    /// Recovers the newest complete DATA generation using an independently
    /// supplied complete dependency verifier.
    ///
    /// The verifier may use a pinned Exact Index, but it must fall back or fail
    /// closed when acceleration cannot prove one required Location. Returned
    /// Manifest readers retain the supplied Container Repository for demand
    /// verification.
    ///
    /// # Errors
    ///
    /// Returns I/O, format, dependency-integrity, graph-completeness, or
    /// bounded-allocation errors.
    ///
    /// # Panics
    ///
    /// Panics when a prior internal invariant panic poisoned the single-writer
    /// commit lock.
    pub fn recover_latest_with_verified_files_using<J>(
        &self,
        containers: &ContainerRepository<J>,
        verifier: &dyn RequiredChunkVerifier,
    ) -> Result<Option<RecoveredDataGeneration<J>>, GenerationError>
    where
        I: Clone + Send + Sync + 'static,
        J: Clone + StorageIo,
    {
        let _guard = self
            .commit_lock
            .lock()
            .expect("ASSERT: generation commit lock poisoned");
        let Some(graph) = self.recover_latest_using(Some(verifier))? else {
            return Ok(None);
        };
        let files = verified_files(graph.manifests, &self.storage, containers)?;
        Ok(Some(RecoveredDataGeneration {
            generation: graph.generation,
            files,
        }))
    }

    fn recover_latest_using(
        &self,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<Option<RecoveredGraph>, GenerationError> {
        let Some(snapshot) = GenerationLog::new(&self.storage)
            .load_for_recovery()
            .map_err(map_log_error)?
        else {
            return Ok(None);
        };
        let latest_generation = match snapshot.records().last() {
            Some(record) => record.generation(),
            None => return Err(GenerationError::NoRecoverableGeneration),
        };
        let inode_reservation_end_high_water = snapshot
            .records()
            .iter()
            .map(|record| record.inode_reservation_end())
            .max()
            .ok_or(GenerationError::NoRecoverableGeneration)?;
        let structurally_valid_records =
            self.validate_recovery_transition_prefix(snapshot.records())?;
        let oldest_online_generation = latest_generation.saturating_sub(1);
        let selected = self
            .select_live_recovery_graph(
                &structurally_valid_records,
                oldest_online_generation,
                verifier,
            )?
            .ok_or(GenerationError::NoRecoverableGeneration)?;
        Ok(Some(RecoveredGraph {
            generation: RecoveredGeneration {
                record: selected.record,
                namespace_root: selected.root,
                wal_tail: snapshot.tail().clone(),
                rejected_newer_generations: latest_generation - selected.record.generation(),
                inode_reservation_end_high_water,
            },
            manifests: selected.manifests,
        }))
    }

    fn validate_recovery_transition_prefix(
        &self,
        records: &[CommitRecord],
    ) -> Result<Vec<CommitRecord>, GenerationError> {
        for record in records {
            if record.policy_set() != self.supported_policy {
                return Err(GenerationError::UnsupportedPolicySet {
                    generation: record.generation(),
                    policy_set: record.policy_set(),
                });
            }
        }
        let mut structurally_valid_records = Vec::new();
        structurally_valid_records
            .try_reserve_exact(records.len())
            .map_err(|_| GenerationError::OutOfMemory)?;
        let mut previous: Option<(CommitRecord, NamespaceRoot)> = None;
        for record in records {
            let root = match self.read_namespace_root(record.namespace_root()) {
                Ok(root) => root,
                Err(error) if error.allows_generation_fallback() => break,
                Err(error) => return Err(error),
            };
            if !record_matches_namespace_root(*record, &root) {
                break;
            }
            match &previous {
                Some((previous_record, previous_root)) => {
                    if verify_generation_transition_pair(*previous_record, previous_root, &root)
                        .is_err()
                    {
                        break;
                    }
                }
                None if record.generation() == 1
                    && (root.inode_allocation_cursor() != 2 || !root.inodes().is_empty()) =>
                {
                    break;
                }
                None => {}
            }
            structurally_valid_records.push(*record);
            previous = Some((*record, root));
        }
        Ok(structurally_valid_records)
    }

    fn select_live_recovery_graph(
        &self,
        structurally_valid_records: &[CommitRecord],
        oldest_online_generation: u64,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<Option<SelectedGraph>, GenerationError> {
        for record in structurally_valid_records
            .iter()
            .rev()
            .take_while(|record| record.generation() >= oldest_online_generation)
        {
            let root = match self.read_namespace_root(record.namespace_root()) {
                Ok(root) => root,
                Err(error) if error.allows_generation_fallback() => continue,
                Err(error) => return Err(error),
            };
            if !record_matches_namespace_root(*record, &root) {
                continue;
            }
            let manifests = match self.verify_manifest_graph(&root, verifier) {
                Ok(manifests) => manifests,
                Err(error) if error.allows_generation_fallback() => continue,
                Err(error) => return Err(error),
            };
            return Ok(Some(SelectedGraph {
                record: *record,
                root,
                manifests,
            }));
        }
        Ok(None)
    }

    fn publish_metadata(&self, encoded: &[u8]) -> Result<MetadataObjectId, GenerationError> {
        let object_id = self.stage_metadata(encoded)?;
        self.storage.sync_root()?;
        Ok(object_id)
    }

    fn stage_metadata(&self, encoded: &[u8]) -> Result<MetadataObjectId, GenerationError> {
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
        Ok(object_id)
    }

    fn read_namespace_root(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<NamespaceRoot, GenerationError> {
        let bytes = self.read_metadata(object_id)?;
        NamespaceRoot::decode(&bytes).map_err(Into::into)
    }

    fn read_metadata(&self, object_id: MetadataObjectId) -> Result<Vec<u8>, GenerationError> {
        let name = metadata_name(object_id);
        let length = self.storage.object_len(&name)?;
        if length > MAX_METADATA_OBJECT_BYTES_U64 {
            return Err(GenerationError::MetadataIdentityCollision(object_id));
        }
        let bytes = self.storage.read(&name)?;
        if u64::try_from(bytes.len()) != Ok(length)
            || MetadataObjectId::from_encoded(&bytes)? != object_id
        {
            return Err(GenerationError::MetadataIdentityCollision(object_id));
        }
        Ok(bytes)
    }

    fn verify_manifest_graph(
        &self,
        root: &NamespaceRoot,
        verifier: Option<&dyn RequiredChunkVerifier>,
    ) -> Result<VerifiedManifests, GenerationError> {
        let mut required_chunks = BTreeMap::new();
        let mut chunk_length_conflict = None;
        let mut manifests = Vec::new();
        manifests
            .try_reserve_exact(root.inodes().len())
            .map_err(|_| GenerationError::OutOfMemory)?;
        for inode in root.inodes() {
            let summary = scan_manifest_tree(
                inode.manifest_root(),
                |node_id| self.read_manifest_node(node_id),
                |_logical_offset, extent| {
                    let ManifestExtent::Data {
                        logical_length,
                        chunk_id,
                    } = *extent
                    else {
                        return Ok(());
                    };
                    if let Some(previous_length) = required_chunks.get(&chunk_id).copied() {
                        if previous_length != logical_length {
                            chunk_length_conflict =
                                Some((chunk_id, previous_length, logical_length));
                        }
                    } else {
                        required_chunks.insert(chunk_id, logical_length);
                    }
                    Ok(())
                },
            )?;
            if let Some((chunk_id, first_length, second_length)) = chunk_length_conflict.take() {
                return Err(GenerationError::ManifestChunkLengthConflict {
                    chunk_id,
                    first_length,
                    second_length,
                });
            }
            if summary.logical_size() != inode.logical_size() {
                return Err(GenerationError::ManifestLengthMismatch {
                    inode: inode.inode(),
                    inode_length: inode.logical_size(),
                    manifest_length: summary.logical_size(),
                });
            }
            manifests.push((inode.inode(), summary));
        }
        if required_chunks.is_empty() {
            return Ok(manifests);
        }
        let Some(verifier) = verifier else {
            return Err(GenerationError::DataLocationsNotConnected);
        };
        verifier.verify_required_chunks(&required_chunks)?;
        Ok(manifests)
    }

    fn read_manifest_node(
        &self,
        object_id: MetadataObjectId,
    ) -> Result<Vec<u8>, ManifestTreeError> {
        let name = metadata_name(object_id);
        let length = self.storage.object_len(&name)?;
        if length > MAX_METADATA_OBJECT_BYTES_U64 {
            return Err(ManifestTreeError::IdentityMismatch(object_id));
        }
        let bytes = self.storage.read(&name)?;
        if u64::try_from(bytes.len()) != Ok(length)
            || MetadataObjectId::from_encoded(&bytes)? != object_id
        {
            return Err(ManifestTreeError::IdentityMismatch(object_id));
        }
        Ok(bytes)
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

fn record_matches_namespace_root(record: CommitRecord, root: &NamespaceRoot) -> bool {
    root.namespace_mutation_sequence() == record.namespace_mutation_cutoff()
        && root.inode_reservation_end() == record.inode_reservation_end()
        && root.inode_allocation_cursor() == record.inode_allocation_cursor()
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

pub use crate::generation_log::LogTail as WalTail;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredGeneration {
    record: CommitRecord,
    namespace_root: NamespaceRoot,
    wal_tail: WalTail,
    rejected_newer_generations: u64,
    inode_reservation_end_high_water: u64,
}

/// One committed DATA generation and the Manifest readers proven by its graph
/// verification.
#[derive(Debug)]
pub struct CommittedDataGeneration<I> {
    record: CommitRecord,
    files: Vec<VerifiedCommittedFile<I>>,
}

/// One recovered DATA generation and the Manifest readers proven for that same
/// selected recovery candidate.
#[derive(Debug)]
pub struct RecoveredDataGeneration<I> {
    generation: RecoveredGeneration,
    files: Vec<VerifiedCommittedFile<I>>,
}

impl<I> RecoveredDataGeneration<I> {
    #[must_use]
    pub const fn generation(&self) -> &RecoveredGeneration {
        &self.generation
    }

    #[must_use]
    pub fn into_parts(self) -> (RecoveredGeneration, Vec<VerifiedCommittedFile<I>>) {
        (self.generation, self.files)
    }
}

impl<I> CommittedDataGeneration<I> {
    #[must_use]
    pub const fn record(&self) -> CommitRecord {
        self.record
    }

    #[must_use]
    pub fn into_parts(self) -> (CommitRecord, Vec<VerifiedCommittedFile<I>>) {
        (self.record, self.files)
    }
}

/// One inode-associated Manifest reader that can only originate from a
/// complete committed DATA-graph verification.
#[derive(Debug)]
pub struct VerifiedCommittedFile<I> {
    inode: u64,
    file: VerifiedManifestFile<I>,
}

impl<I: StorageIo> VerifiedCommittedFile<I> {
    #[must_use]
    pub const fn inode(&self) -> u64 {
        self.inode
    }

    #[must_use]
    pub fn manifest_root(&self) -> Option<MetadataObjectId> {
        self.file.manifest_root()
    }

    #[must_use]
    pub fn logical_size(&self) -> u64 {
        self.file.logical_size()
    }

    #[must_use]
    pub fn allocated_bytes(&self) -> u64 {
        self.file.allocated_bytes()
    }

    #[must_use]
    pub fn into_file(self) -> VerifiedManifestFile<I> {
        self.file
    }
}

fn verified_files<M, I>(
    manifests: Vec<(u64, ManifestTreeSummary)>,
    metadata: &M,
    containers: &ContainerRepository<I>,
) -> Result<Vec<VerifiedCommittedFile<I>>, GenerationError>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    I: Clone + StorageIo,
{
    let mut files = Vec::new();
    files
        .try_reserve_exact(manifests.len())
        .map_err(|_| GenerationError::OutOfMemory)?;
    for (inode, summary) in manifests {
        files.push(VerifiedCommittedFile {
            inode,
            file: VerifiedManifestFile::from_verified_tree(
                summary,
                metadata.clone(),
                containers.clone(),
            ),
        });
    }
    Ok(files)
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
    ManifestTree(ManifestTreeError),
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
    OutOfMemory,
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
            Self::Io(error)
            | Self::Store(StoreError::Io(error))
            | Self::ManifestTree(ManifestTreeError::Io(error)) => {
                error.kind() == io::ErrorKind::NotFound
            }
            Self::MetadataFormat(_)
            | Self::ManifestTree(
                ManifestTreeError::Metadata(_)
                | ManifestTreeError::Inner(
                    fastdup_format::ManifestInnerNodeError::Metadata(_)
                    | fastdup_format::ManifestInnerNodeError::InvalidLevel
                    | fastdup_format::ManifestInnerNodeError::InvalidChildRange
                    | fastdup_format::ManifestInnerNodeError::InvalidPartition
                    | fastdup_format::ManifestInnerNodeError::InvalidPayload
                    | fastdup_format::ManifestInnerNodeError::ArithmeticOverflow,
                )
                | ManifestTreeError::IdentityMismatch(_)
                | ManifestTreeError::InvalidTree
                | ManifestTreeError::ArithmeticOverflow,
            )
            | Self::MetadataIdentityCollision(_)
            | Self::ManifestLengthMismatch { .. }
            | Self::ManifestChunkLengthConflict { .. }
            | Self::Store(
                StoreError::Format(_)
                | StoreError::InvalidPublishedName(_)
                | StoreError::PublishedIdentityMismatch { .. }
                | StoreError::MissingVerifiedChunk { .. }
                | StoreError::ExactLocationMismatch,
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
            | Self::DataLocationsNotConnected
            | Self::ManifestTree(
                ManifestTreeError::TreeTooDeep
                | ManifestTreeError::TreeTooLarge
                | ManifestTreeError::InvalidReplacement
                | ManifestTreeError::BoundaryInsideData
                | ManifestTreeError::OutOfMemory
                | ManifestTreeError::Inner(fastdup_format::ManifestInnerNodeError::OutOfMemory),
            )
            | Self::OutOfMemory => false,
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
            Self::ManifestTree(error) => Some(error),
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

impl From<ManifestTreeError> for GenerationError {
    fn from(error: ManifestTreeError) -> Self {
        Self::ManifestTree(error)
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

fn map_log_error(error: GenerationLogError) -> GenerationError {
    match error {
        GenerationLogError::Io(error) => GenerationError::Io(error),
        GenerationLogError::SegmentTooLarge => GenerationError::WalTooLarge,
        GenerationLogError::NeedsRepair(tail) => GenerationError::WalNeedsRepair(tail),
        GenerationLogError::PublishVerificationMismatch => {
            GenerationError::PublishVerificationMismatch
        }
        GenerationLogError::OutOfMemory => GenerationError::OutOfMemory,
        GenerationLogError::BrokenGenerationChain
        | GenerationLogError::DivergentSlots
        | GenerationLogError::EmptyAfterInitialization => GenerationError::NoRecoverableGeneration,
    }
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
