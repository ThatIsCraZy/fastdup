//! Translate durable namespace metadata and verified file proofs into POSIX state.

use std::sync::Arc;

use fastdup_format::{DurableInode, DurableInodeKind, DurableXattr, NamespaceRoot};
use fastdup_posix::{
    CommittedDirectory, CommittedEntry, CommittedFile, CommittedInode, CommittedNamespaceSnapshot,
    CommittedSymlink, ExtendedAttribute, FileKind, InodeMetadata, Namespace, NamespaceConfig,
    PosixError, PosixTimes, PosixTimestamp,
};
use fastdup_store::{StorageIo, VerifiedCommittedFile, VerifiedManifestFile};

use crate::MountError;
use crate::manifest_file::ManifestCommittedFile;

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
