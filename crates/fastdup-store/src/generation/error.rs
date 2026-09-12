//! Generation error classification, conversion and recovery fallback policy.
use super::WalTail;
use crate::StoreError;
use crate::generation_log::GenerationLogError;
use crate::manifest_tree::ManifestTreeError;
use crate::metadata_mark_catalog::MetadataMarkCatalogError;
use fastdup_format::{CommitFormatError, MetadataFormatError, MetadataObjectId, PolicySetId};
use std::{fmt, io};

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
    UnsupportedFormatEpoch {
        generation: u64,
        format_epoch: u16,
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
    ManifestCountMismatch {
        namespace_inodes: usize,
        manifests: usize,
    },
    StaleSuccessorPredecessor {
        proof_generation: u64,
        installed_generation: Option<u64>,
    },
    MixedSuccessorPredecessors {
        expected_generation: u64,
        observed_generation: u64,
    },
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
    LivenessDeltaBaseUnavailable {
        requested: Option<u64>,
        latest: Option<u64>,
    },
    RetainedManifestNotInPredecessor(MetadataObjectId),
    RetainedManifestRangeInvalid {
        root: MetadataObjectId,
        start: u64,
        end: u64,
        logical_size: u64,
    },
    InvalidMetadataObjectName(String),
    MetadataMarkCatalogCorruption,
    RecoveryTargetNotEmpty,
}

impl GenerationError {
    pub(super) fn allows_generation_fallback(&self) -> bool {
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
                | ManifestTreeError::MissingSubtreeAllocation
                | ManifestTreeError::ArithmeticOverflow,
            )
            | Self::MetadataIdentityCollision(_)
            | Self::ManifestLengthMismatch { .. }
            | Self::ManifestChunkLengthConflict { .. }
            | Self::LivenessDeltaBaseUnavailable { .. }
            | Self::Store(
                StoreError::Format(_)
                | StoreError::InvalidPublishedName(_)
                | StoreError::PublishedIdentityMismatch { .. }
                | StoreError::MissingVerifiedChunk { .. }
                | StoreError::ExactLocationMismatch,
            ) => true,
            Self::CommitFormat(_)
            | Self::Store(
                StoreError::PublishVerificationMismatch
                | StoreError::InvalidContainerGenerationReservationSpan
                | StoreError::ContainerGenerationExhausted
                | StoreError::ContainerGenerationHighWaterFormat(_)
                | StoreError::ContainerGenerationHighWaterMissing
                | StoreError::ContainerGenerationHighWaterChain
                | StoreError::ContainerGenerationHighWaterBehind { .. },
            )
            | Self::MetadataTooLarge
            | Self::WalTooLarge
            | Self::GenerationExhausted
            | Self::UnsupportedPolicySet { .. }
            | Self::UnsupportedFormatEpoch { .. }
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
            | Self::ManifestCountMismatch { .. }
            | Self::StaleSuccessorPredecessor { .. }
            | Self::MixedSuccessorPredecessors { .. }
            | Self::RetainedManifestNotInPredecessor(_)
            | Self::RetainedManifestRangeInvalid { .. }
            | Self::ManifestTree(
                ManifestTreeError::TreeTooDeep
                | ManifestTreeError::TreeTooLarge
                | ManifestTreeError::InvalidReplacement
                | ManifestTreeError::OutOfMemory
                | ManifestTreeError::Inner(fastdup_format::ManifestInnerNodeError::OutOfMemory),
            )
            | Self::OutOfMemory
            | Self::InvalidMetadataObjectName(_)
            | Self::MetadataMarkCatalogCorruption
            | Self::RecoveryTargetNotEmpty => false,
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

impl From<MetadataMarkCatalogError> for GenerationError {
    fn from(error: MetadataMarkCatalogError) -> Self {
        match error {
            MetadataMarkCatalogError::Io(error) => Self::Io(error),
            MetadataMarkCatalogError::Format(
                fastdup_format::MetadataMarkCatalogError::OutOfMemory,
            ) => Self::OutOfMemory,
            MetadataMarkCatalogError::Format(
                fastdup_format::MetadataMarkCatalogError::ArithmeticOverflow,
            ) => Self::MetadataTooLarge,
            MetadataMarkCatalogError::Format(_) => Self::MetadataMarkCatalogCorruption,
        }
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

pub(super) fn map_log_error(error: GenerationLogError) -> GenerationError {
    match error {
        GenerationLogError::Io(error) => GenerationError::Io(error),
        GenerationLogError::SegmentTooLarge => GenerationError::WalTooLarge,
        GenerationLogError::NeedsRepair(tail) => GenerationError::WalNeedsRepair(tail),
        GenerationLogError::PublishVerificationMismatch => {
            GenerationError::PublishVerificationMismatch
        }
        GenerationLogError::OutOfMemory => GenerationError::OutOfMemory,
        GenerationLogError::AlreadyInitialized => GenerationError::RecoveryTargetNotEmpty,
        GenerationLogError::BrokenGenerationChain
        | GenerationLogError::DivergentSlots
        | GenerationLogError::EmptyAfterInitialization => GenerationError::NoRecoverableGeneration,
    }
}
