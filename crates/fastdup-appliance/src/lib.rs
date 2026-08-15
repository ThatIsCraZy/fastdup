#![forbid(unsafe_code)]

//! Durable repository-to-POSIX mount orchestration.

mod checkpoint;

pub use checkpoint::{DurableNamespace, DurableNamespaceError};

use std::fmt;
use std::sync::Arc;

use fastdup_format::{ManifestExtent, ManifestLeaf, NamespaceRoot};
use fastdup_posix::{
    CommittedEntry, CommittedFile, CommittedInode, CommittedNamespaceSnapshot, Namespace,
    NamespaceConfig, PosixError,
};
use fastdup_store::{
    ContainerRepository, GenerationError, GenerationRepository, ManifestReadError, StorageIo,
    VerifiedManifestFile,
};

/// Recovers and mounts the newest wholly verified namespace generation.
///
/// This Adapter is the single Seam between durable format/store types and the
/// POSIX namespace. It retains immutable Manifest recipes and verified
/// container access behind [`CommittedFile`] without materializing complete
/// file bytes. All mutation admission deliberately remains read-only until a
/// later module wires the commit scheduler and durable Inode reservation.
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
    M: StorageIo,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let Some(recovered) = generations.recover_latest_with_verified_files(containers)? else {
        return Ok(None);
    };
    let (generation, verified_files) = recovered.into_parts();
    let high_water = generation.inode_reservation_end_high_water();
    let root = generation.namespace_root();
    let mut files = Vec::new();
    files
        .try_reserve_exact(verified_files.len())
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for (inode, verified) in root.inodes().iter().zip(verified_files) {
        assert_eq!(
            verified.inode(),
            inode.inode(),
            "ASSERT: recovered DATA proof order must match the Namespace Root"
        );
        assert_eq!(
            verified.manifest().file_length(),
            inode.logical_size(),
            "ASSERT: recovered DATA proof length must match the durable inode"
        );
        files.push(
            Arc::new(ManifestCommittedFile::from_verified(verified.into_file()))
                as Arc<dyn CommittedFile>,
        );
    }

    namespace_from_files(config, root, high_water, high_water, files, false).map(Some)
}

#[allow(clippy::too_many_arguments)]
fn namespace_from_root<M, C>(
    config: NamespaceConfig,
    root: &NamespaceRoot,
    next_inode: u64,
    inode_reservation_end: u64,
    generations: &GenerationRepository<M>,
    containers: &ContainerRepository<C>,
    writable: bool,
) -> Result<Namespace, MountError>
where
    M: StorageIo,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let mut files = Vec::new();
    files
        .try_reserve_exact(root.inodes().len())
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for inode in root.inodes() {
        let manifest = generations.read_manifest(inode.manifest_root())?;
        if manifest.file_length() != inode.logical_size() {
            return Err(MountError::Posix(PosixError::Io));
        }
        let file = Arc::new(ManifestCommittedFile::new(manifest, containers.clone())?)
            as Arc<dyn CommittedFile>;
        files.push(file);
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
    if files.len() != root.inodes().len() {
        return Err(MountError::Posix(PosixError::Io));
    }
    let mut inodes = Vec::new();
    inodes
        .try_reserve_exact(root.inodes().len())
        .map_err(|_| MountError::Posix(PosixError::OutOfMemory))?;
    for (inode, file) in root.inodes().iter().zip(files) {
        inodes.push(CommittedInode::new(
            inode.inode(),
            inode.mode(),
            inode.uid(),
            inode.gid(),
            inode.link_count(),
            inode.mutation_sequence(),
            file,
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

    let snapshot = CommittedNamespaceSnapshot::new(
        next_inode,
        inode_reservation_end,
        root.namespace_mutation_sequence(),
        inodes,
        entries,
    )?;
    if writable {
        Namespace::from_committed_writable(config, snapshot).map_err(Into::into)
    } else {
        Namespace::from_committed(config, snapshot).map_err(Into::into)
    }
}

#[derive(Clone, Copy)]
struct AllocatedRange {
    start: u64,
    end: u64,
}

pub(crate) struct ManifestCommittedFile<I> {
    file: VerifiedManifestFile<I>,
    logical_size: u64,
    allocated_bytes: u64,
    allocated_ranges: Vec<AllocatedRange>,
}

impl<I: StorageIo> ManifestCommittedFile<I> {
    pub(crate) fn new(
        manifest: ManifestLeaf,
        containers: ContainerRepository<I>,
    ) -> Result<Self, ManifestReadError> {
        let file = VerifiedManifestFile::new(manifest, containers)?;
        Ok(Self::from_verified(file))
    }

    pub(crate) fn from_verified(file: VerifiedManifestFile<I>) -> Self {
        let logical_size = file.manifest().file_length();
        let mut allocated_ranges = Vec::<AllocatedRange>::new();
        let mut extent_start = 0_u64;
        for extent in file.manifest().extents() {
            let length = extent_length(extent);
            let extent_end = extent_start
                .checked_add(length)
                .expect("ASSERT: verified Manifest extent end must not overflow");
            if !matches!(extent, ManifestExtent::Hole { .. }) {
                if let Some(previous) = allocated_ranges.last_mut()
                    && previous.end == extent_start
                {
                    previous.end = extent_end;
                } else {
                    allocated_ranges.push(AllocatedRange {
                        start: extent_start,
                        end: extent_end,
                    });
                }
            }
            extent_start = extent_end;
        }
        assert_eq!(
            extent_start, logical_size,
            "ASSERT: verified Manifest extents must partition the file"
        );
        let allocated_bytes = allocated_ranges.iter().fold(0_u64, |total, range| {
            total
                .checked_add(range.end - range.start)
                .expect("ASSERT: allocated Manifest subset cannot exceed file length")
        });
        Self {
            file,
            logical_size,
            allocated_bytes,
            allocated_ranges,
        }
    }
}

impl<I> fmt::Debug for ManifestCommittedFile<I> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManifestCommittedFile")
            .field("logical_size", &self.logical_size)
            .field("allocated_bytes", &self.allocated_bytes)
            .field("allocated_range_count", &self.allocated_ranges.len())
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
        let end = offset.checked_add(length).ok_or(PosixError::Io)?;
        let end = end.min(self.logical_size);
        if offset >= end {
            return Ok(0);
        }
        let first = self
            .allocated_ranges
            .partition_point(|range| range.end <= offset);
        let mut allocated = 0_u64;
        for range in &self.allocated_ranges[first..] {
            if range.start >= end {
                break;
            }
            let intersection_start = range.start.max(offset);
            let intersection_end = range.end.min(end);
            allocated = allocated
                .checked_add(intersection_end - intersection_start)
                .ok_or(PosixError::Io)?;
        }
        Ok(allocated)
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.file
            .read_at(offset, length)
            .map_err(|_| PosixError::Io)
    }
}

const fn extent_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
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
