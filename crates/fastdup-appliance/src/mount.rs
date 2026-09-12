//! Recover verified generations and mount them as read-only POSIX namespaces.

use std::fmt;

use fastdup_posix::{Namespace, NamespaceConfig, PosixError};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, GenerationError, GenerationRepository,
    IndexedRequiredChunkVerifier, ManifestReadError, RecoveredDataGeneration, StorageIo,
    VerifiedManifestFile,
};

use crate::namespace_restore::namespace_from_verified_files_using;

/// Recovers and mounts the newest wholly verified namespace generation.
///
/// This Adapter is the single Seam between durable format/store types and the
/// POSIX namespace. It retains immutable Manifest recipes and verified
/// container access behind [`fastdup_posix::CommittedFile`] without materializing complete
/// file bytes. This recovery-only helper deliberately returns a read-only
/// namespace; [`crate::DurableNamespace`] owns the separately gated writable mount.
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
/// unavailable. Each bounded read pins the current immutable Run Set;
/// dormant committed file readers do not retain operation pins.
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
        Some(_) => file.with_index_repository(indexes),
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
