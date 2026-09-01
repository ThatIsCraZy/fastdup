#![forbid(unsafe_code)]

//! Durable repository-to-POSIX mount orchestration.

mod appliance_lease;
mod appliance_recovery_latch;
mod checkpoint;
mod checkpoint_trigger;
mod commit_capacity;
mod historical_proof_cache;
mod online_gc;
mod pool_binding;
mod pool_isolation;
mod proof_cache_trace;
mod statfs;

pub use appliance_lease::{APPLIANCE_LEASE_FILE_NAME, ApplianceLease, ApplianceLeaseOwner};
pub use appliance_recovery_latch::{
    APPLIANCE_RECOVERY_LATCH_FILE_NAME, ApplianceRecoveryLatch, ApplianceRecoveryState,
};
pub use pool_binding::{AppliancePoolBinding, AppliancePoolBindingError, POOL_IDENTITY_FILE_NAME};
pub use pool_isolation::{
    POOL_ISOLATION_POLICY_ENV, PhysicalPoolIsolation, PoolIsolationError, PoolIsolationObservation,
    PoolIsolationPolicy, PoolIsolationPolicyError,
};

pub use checkpoint::{
    CHECKPOINT_DIRTY_PAYLOAD_BYTES_V1, CheckpointMetrics, CheckpointPhaseMetrics, CpuPhaseStatus,
    DurableNamespace, DurableNamespaceError, GenerationProofSetStatus, INODE_RESERVATION_SPAN_V1,
    ProfiledCheckpoint, WriteThroughStatus, checkpoint_exact_index_profile_v1,
    checkpoint_policy_set,
};
pub use checkpoint_trigger::{
    CONTAINER_COMMIT_COALESCE, CheckpointAction, CheckpointPressure, CheckpointProgressAction,
    CheckpointTrigger, DurabilityObservation, DurabilitySupervisor, MUTATION_ADMISSION_GUARD,
    MUTATION_COMMIT_TARGET, SEALED_CONTAINER_COMMIT_LIMIT, checkpoint_action,
};
pub use commit_capacity::{
    COMMIT_METADATA_FLOOR_BYTES_V1, CommitCapacityConfigurationError, CommitCapacityGovernor,
    CommitCapacitySnapshot, CommitCapacityStatus,
};
pub use historical_proof_cache::HistoricalProofCacheStatus;
pub use online_gc::{
    DailyGcWindow, ONLINE_GC_ACTIVE_INTERVAL_SECONDS_ENV, ONLINE_GC_CONTROL_REQUEST,
    ONLINE_GC_CONTROL_SOCKET_NAME, ONLINE_GC_DAILY_WINDOW_UTC_ENV,
    ONLINE_GC_IDLE_AFTER_SECONDS_ENV, ONLINE_GC_IDLE_INTERVAL_SECONDS_ENV,
    ONLINE_GC_MAX_RELOCATION_WORKERS_ENV, ONLINE_GC_PRESSURE_HIGH_BASIS_POINTS_ENV,
    ONLINE_GC_PRESSURE_LOW_BASIS_POINTS_ENV, ONLINE_GC_URGENT_INTERVAL_SECONDS_ENV,
    ONLINE_GC_WINDOW_INTERVAL_SECONDS_ENV, OnlineGcPolicy, OnlineGcPolicyConfigurationError,
    OnlineGcPolicyError, OnlineGcScheduler, OnlineGcSchedulerStatus, bind_online_gc_control_socket,
    online_gc_control_path, remove_stale_online_gc_socket, request_online_gc_now,
};
pub use proof_cache_trace::{
    ProofCacheEvent, ProofCachePolicy, ProofCacheReplayError, ProofCacheReplayReport,
    ProofCacheTrace, ProofKey, replay_proof_cache_trace,
};
pub use statfs::{
    STATFS_RESERVE_BASIS_POINTS, StatFsOverride, StatFsOverrideError, TieredStatFsSource,
};

use std::fmt;
use std::sync::Arc;

use fastdup_format::{DurableInode, DurableInodeKind, DurableXattr, ManifestExtent, NamespaceRoot};
use fastdup_posix::{
    CommittedDirectory, CommittedEntry, CommittedFile, CommittedInode, CommittedNamespaceSnapshot,
    CommittedSymlink, ExtendedAttribute, FileKind, InodeMetadata, Namespace, NamespaceConfig,
    PosixError, PosixTimes, PosixTimestamp, PreparedCommitExtent, PreparedDataRecipe,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, GenerationError, GenerationRepository,
    IndexedRequiredChunkVerifier, ManifestReadError, RecoveredDataGeneration, StorageIo,
    VerifiedCommittedFile, VerifiedManifestFile,
};

/// Recovers and mounts the newest wholly verified namespace generation.
///
/// This Adapter is the single Seam between durable format/store types and the
/// POSIX namespace. It retains immutable Manifest recipes and verified
/// container access behind [`CommittedFile`] without materializing complete
/// file bytes. This recovery-only helper deliberately returns a read-only
/// namespace; [`DurableNamespace`] owns the separately gated writable mount.
///
/// # Errors
///
/// Returns generation recovery, Manifest dependency, or POSIX snapshot
/// validation failures. A missing Commit WAL or empty repository returns
/// `Ok(None)`.
///
/// # Panics
///
/// Panics only if the Store returns an internally inconsistent opaque graph
/// proof whose inode order or lengths disagree with its verified Namespace
/// Root.
pub fn recover_mount<M, C>(
    config: NamespaceConfig,
    generations: &GenerationRepository<M>,
    containers: &ContainerRepository<C>,
) -> Result<Option<Namespace>, MountError>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let Some(recovered) = generations.recover_latest_with_verified_files(containers)? else {
        return Ok(None);
    };
    mount_recovered(config, recovered, |file| file).map(Some)
}

/// Recovers a namespace and pins the currently activated Exact Index into its
/// immutable Manifest readers.
///
/// The Exact Index is non-authoritative acceleration state. An absent or
/// unreadable activation therefore mounts the verified namespace through its
/// Container-scan fallback instead of rolling metadata back or making content
/// unavailable. Once recovered, one immutable Run Set is pinned for the
/// lifetime of every returned committed file reader.
///
/// # Errors
///
/// Returns only Namespace generation, Manifest dependency, or POSIX snapshot
/// validation failures. Exact Index recovery failures deliberately disable the
/// accelerator for this mount.
///
/// # Panics
///
/// Panics only if the Store returns an internally inconsistent opaque graph
/// proof whose inode order or lengths disagree with its verified Namespace
/// Root.
pub fn recover_mount_with_index<M, C, X>(
    config: NamespaceConfig,
    generations: &GenerationRepository<M>,
    containers: &ContainerRepository<C>,
    indexes: &ExactIndexRunRepository<X>,
) -> Result<Option<Namespace>, MountError>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
    X: Clone + Send + Sync + StorageIo + 'static,
{
    let active = indexes
        .recover_active_generation()
        .and_then(|active| {
            if let Some(index) = &active {
                let retiring = indexes.retiring_containers(index)?;
                containers.install_retiring_selection_barrier(&retiring);
            }
            Ok(active)
        })
        .ok()
        .flatten();
    let recovered = match &active {
        Some(index) => {
            let verifier = IndexedRequiredChunkVerifier::new(containers.clone(), index.clone());
            generations.recover_latest_with_verified_files_using(containers, &verifier)?
        }
        None => generations.recover_latest_with_verified_files(containers)?,
    };
    let Some(recovered) = recovered else {
        return Ok(None);
    };
    mount_recovered(config, recovered, |file| match &active {
        Some(index) => file.with_active_index(index),
        None => file,
    })
    .map(Some)
}

fn mount_recovered<C, F>(
    config: NamespaceConfig,
    recovered: RecoveredDataGeneration<C>,
    prepare_file: F,
) -> Result<Namespace, MountError>
where
    C: Send + Sync + StorageIo + 'static,
    F: FnMut(VerifiedManifestFile<C>) -> VerifiedManifestFile<C>,
{
    let (generation, verified_files) = recovered.into_parts();
    let high_water = generation.inode_reservation_end_high_water();
    let root = generation.namespace_root();
    namespace_from_verified_files_using(
        config,
        root,
        high_water,
        high_water,
        verified_files,
        false,
        prepare_file,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn namespace_from_verified_files_using<C, F>(
    config: NamespaceConfig,
    root: &NamespaceRoot,
    next_inode: u64,
    inode_reservation_end: u64,
    verified_files: Vec<VerifiedCommittedFile<C>>,
    writable: bool,
    mut prepare_file: F,
) -> Result<Namespace, MountError>
where
    C: Send + Sync + StorageIo + 'static,
    F: FnMut(VerifiedManifestFile<C>) -> VerifiedManifestFile<C>,
{
    assert_eq!(
        verified_files.len(),
        root.file_inode_count(),
        "ASSERT: opaque DATA graph proof count must match the Namespace Root"
    );
    let mut files = Vec::new();
    files
        .try_reserve_exact(verified_files.len())
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for (inode, verified) in root.file_inodes().zip(verified_files) {
        assert_eq!(
            verified.inode(),
            inode.inode(),
            "ASSERT: recovered DATA proof order must match the Namespace Root"
        );
        assert_eq!(
            verified.logical_size(),
            inode.logical_size(),
            "ASSERT: recovered DATA proof length must match the durable inode"
        );
        files.push(Arc::new(ManifestCommittedFile::from_verified(prepare_file(
            verified.into_file(),
        ))) as Arc<dyn CommittedFile>);
    }
    namespace_from_files(
        config,
        root,
        next_inode,
        inode_reservation_end,
        files,
        writable,
    )
}

fn namespace_from_files(
    config: NamespaceConfig,
    root: &NamespaceRoot,
    next_inode: u64,
    inode_reservation_end: u64,
    files: Vec<Arc<dyn CommittedFile>>,
    writable: bool,
) -> Result<Namespace, MountError> {
    if files.len() != root.file_inode_count() {
        return Err(MountError::Posix(PosixError::Io));
    }
    let mut inodes = Vec::new();
    inodes
        .try_reserve_exact(root.file_inode_count())
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for (inode, file) in root.file_inodes().zip(files) {
        inodes.push(
            CommittedInode::new_with_metadata(
                inode.inode(),
                inode.mode(),
                inode.uid(),
                inode.gid(),
                inode.link_count(),
                inode.mutation_sequence(),
                metadata_from_durable(inode)?,
                file,
            )?
            .with_times(posix_times(inode.times())),
        );
    }

    let mut directories = Vec::new();
    directories
        .try_reserve_exact(root.inodes().len().saturating_sub(root.file_inode_count()))
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for inode in root.directory_inodes() {
        directories.push(
            CommittedDirectory::new_with_metadata(
                inode.inode(),
                inode.mode(),
                inode.uid(),
                inode.gid(),
                inode.link_count(),
                inode.mutation_sequence(),
                metadata_from_durable(inode)?,
            )?
            .with_times(posix_times(inode.times())),
        );
    }

    let mut symlinks = Vec::new();
    symlinks
        .try_reserve_exact(root.symlink_inodes().count())
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for inode in root.symlink_inodes() {
        symlinks.push(CommittedSymlink::new(
            inode.inode(),
            inode.uid(),
            inode.gid(),
            inode.link_count(),
            inode.mutation_sequence(),
            posix_times(inode.times()),
            inode
                .symlink_target()
                .ok_or(MountError::Posix(PosixError::Io))?
                .to_vec(),
        )?);
    }

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(root.entries().len())
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for entry in root.entries() {
        entries.push(CommittedEntry::new(
            entry.parent_inode(),
            entry.target_inode(),
            entry.name().to_vec(),
        )?);
    }

    let root_metadata = root.root_metadata();
    let snapshot = CommittedNamespaceSnapshot::new_with_directories(
        next_inode,
        inode_reservation_end,
        root.namespace_mutation_sequence(),
        inodes,
        directories,
        entries,
    )?
    .with_root_metadata(
        root_metadata.mode(),
        root_metadata.uid(),
        root_metadata.gid(),
        InodeMetadata::new(
            FileKind::Directory,
            root_metadata.file_flags(),
            extended_attributes(root_metadata.xattrs(), FileKind::Directory)?,
        )?,
    )
    .with_posix_state(posix_times(root_metadata.times()), symlinks);
    if writable {
        Namespace::from_committed_writable(config, snapshot).map_err(Into::into)
    } else {
        Namespace::from_committed(config, snapshot).map_err(Into::into)
    }
}

fn posix_times(times: fastdup_format::DurableTimes) -> PosixTimes {
    fn timestamp(value: fastdup_format::DurableTimestamp) -> PosixTimestamp {
        PosixTimestamp::new(value.seconds, value.nanoseconds)
    }
    PosixTimes {
        atime: timestamp(times.atime),
        mtime: timestamp(times.mtime),
        ctime: timestamp(times.ctime),
    }
}

fn metadata_from_durable(inode: &DurableInode) -> Result<InodeMetadata, MountError> {
    let kind = match inode.kind() {
        DurableInodeKind::Regular => FileKind::Regular,
        DurableInodeKind::Directory => FileKind::Directory,
        DurableInodeKind::Symlink => FileKind::Symlink,
    };
    InodeMetadata::new(
        kind,
        inode.file_flags(),
        extended_attributes(inode.xattrs(), kind)?,
    )
    .map_err(Into::into)
}

fn extended_attributes(
    durable: &[DurableXattr],
    kind: FileKind,
) -> Result<Vec<ExtendedAttribute>, MountError> {
    let mut xattrs = Vec::new();
    xattrs
        .try_reserve_exact(durable.len())
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for xattr in durable {
        xattrs.push(ExtendedAttribute::new(
            kind,
            xattr.name().to_vec(),
            xattr.value().to_vec(),
        )?);
    }
    Ok(xattrs)
}

pub(crate) struct ManifestCommittedFile<I> {
    file: VerifiedManifestFile<I>,
    logical_size: u64,
    allocated_bytes: u64,
}

impl<I: StorageIo> ManifestCommittedFile<I> {
    pub(crate) fn from_verified(file: VerifiedManifestFile<I>) -> Self {
        let logical_size = file.logical_size();
        let allocated_bytes = file.allocated_bytes();
        Self {
            file,
            logical_size,
            allocated_bytes,
        }
    }
}

impl<I> fmt::Debug for ManifestCommittedFile<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManifestCommittedFile")
            .field("logical_size", &self.logical_size)
            .field("allocated_bytes", &self.allocated_bytes)
            .finish_non_exhaustive()
    }
}

impl<I> CommittedFile for ManifestCommittedFile<I>
where
    I: Send + Sync + StorageIo,
{
    fn logical_size(&self) -> u64 {
        self.logical_size
    }

    fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        if offset == 0 && length >= self.logical_size {
            return Ok(self.allocated_bytes);
        }
        self.file
            .allocated_bytes_in_range(offset, length)
            .map_err(|_| PosixError::Io)
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.file
            .read_at(offset, length)
            .map_err(|_| PosixError::Io)
    }

    fn prepared_clone_extents(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Option<Vec<PreparedCommitExtent>>, PosixError> {
        let end = offset.checked_add(length).ok_or(PosixError::FileTooLarge)?;
        if length == 0 || end > self.logical_size {
            return Err(PosixError::InvalidArgument);
        }
        if self.allocated_bytes_in_range(offset, length)? != length {
            return Ok(None);
        }
        let located = self
            .file
            .manifest_extents_in_range(offset, length)
            .map_err(|_| PosixError::Io)?;
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(located.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        let mut cursor = offset;
        for located_extent in located {
            let extent = located_extent.extent();
            let extent_length = match *extent {
                ManifestExtent::Data { logical_length, .. }
                | ManifestExtent::DataSlice { logical_length, .. }
                | ManifestExtent::Hole { logical_length }
                | ManifestExtent::Fill { logical_length, .. } => logical_length,
            };
            let extent_end = located_extent
                .logical_offset()
                .checked_add(extent_length)
                .ok_or(PosixError::Io)?;
            let selected_start = located_extent.logical_offset().max(offset);
            let selected_end = extent_end.min(end);
            if selected_start != cursor || selected_start >= selected_end {
                return Err(PosixError::Io);
            }
            let selected_length = selected_end - selected_start;
            let recipe = match *extent {
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                } => {
                    if selected_start == located_extent.logical_offset()
                        && selected_length == logical_length
                    {
                        PreparedDataRecipe::Chunk {
                            chunk_id: chunk_id.bytes(),
                        }
                    } else {
                        PreparedDataRecipe::ChunkSlice {
                            chunk_id: chunk_id.bytes(),
                            chunk_length: u32::try_from(logical_length)
                                .map_err(|_| PosixError::Io)?,
                            chunk_offset: u32::try_from(
                                selected_start - located_extent.logical_offset(),
                            )
                            .map_err(|_| PosixError::Io)?,
                        }
                    }
                }
                ManifestExtent::DataSlice {
                    chunk_id,
                    chunk_length,
                    chunk_offset,
                    ..
                } => PreparedDataRecipe::ChunkSlice {
                    chunk_id: chunk_id.bytes(),
                    chunk_length,
                    chunk_offset: chunk_offset
                        .checked_add(
                            u32::try_from(selected_start - located_extent.logical_offset())
                                .map_err(|_| PosixError::Io)?,
                        )
                        .ok_or(PosixError::Io)?,
                },
                ManifestExtent::Fill { value, .. } => PreparedDataRecipe::Fill { value },
                ManifestExtent::Hole { .. } => return Ok(None),
            };
            let prepared_extent = match self.file.manifest_root() {
                Some(root) => PreparedCommitExtent::try_new_retained(
                    selected_start,
                    selected_length,
                    recipe,
                    root.bytes(),
                    selected_start,
                )?,
                None => PreparedCommitExtent::try_new(selected_start, selected_length, recipe)?,
            };
            prepared.push(prepared_extent);
            cursor = selected_end;
        }
        if cursor != end {
            return Err(PosixError::Io);
        }
        Ok(Some(prepared))
    }
}

/// Failure while translating one recovered durable generation into POSIX state.
#[derive(Debug)]
pub enum MountError {
    Generation(GenerationError),
    Manifest(ManifestReadError),
    Posix(PosixError),
}

impl fmt::Display for MountError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for MountError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Generation(error) => Some(error),
            Self::Manifest(error) => Some(error),
            Self::Posix(_) => None,
        }
    }
}

impl From<GenerationError> for MountError {
    fn from(error: GenerationError) -> Self {
        Self::Generation(error)
    }
}

impl From<ManifestReadError> for MountError {
    fn from(error: ManifestReadError) -> Self {
        Self::Manifest(error)
    }
}

impl From<PosixError> for MountError {
    fn from(error: PosixError) -> Self {
        Self::Posix(error)
    }
}
