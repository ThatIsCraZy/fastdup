//! Opaque generation outcomes, liveness evidence and telemetry result accessors.
use super::{GenerationError, GenerationRepository};
use crate::manifest_tree::ManifestTreeSummary;
use crate::{ContainerRepository, StorageIo, VerifiedManifestFile};
use fastdup_format::{CommitRecord, MetadataObjectId, NamespaceRoot};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

/// Payload-free evidence from an exhaustive bounded Generation-Log scrub.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GenerationScrubSummary {
    pub(super) generations: usize,
    pub(super) first_generation: Option<u64>,
    pub(super) latest_generation: Option<u64>,
    pub(super) latest_namespace_inodes: usize,
    pub(super) latest_manifest_files: usize,
}

/// How one Metadata-GC quantum established its retained-object catalog view.
///
/// Only `ExactSnapshot` has deletion authority. Reuse and additive deltas are
/// acceleration states and cannot authorize an unlink.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataGcMarkMode {
    #[default]
    Reused,
    AdditionDelta,
    ExactSnapshot,
}

/// Why Metadata GC had to rebuild exact deletion authority instead of reusing
/// or extending the process-local clean catalog state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataGcExactReason {
    ProcessStart,
    UnclassifiedPublication,
    MetadataRootPinDrain,
    WalRotation,
    UncertainWalDurability,
    DeltaChainLimit,
    RecoveryCheckpointPinChange,
}

impl MetadataGcExactReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProcessStart => "process_start",
            Self::UnclassifiedPublication => "unclassified_publication",
            Self::MetadataRootPinDrain => "metadata_root_pin_drain",
            Self::WalRotation => "wal_rotation",
            Self::UncertainWalDurability => "uncertain_wal_durability",
            Self::DeltaChainLimit => "delta_chain_limit",
            Self::RecoveryCheckpointPinChange => "recovery_checkpoint_pin_change",
        }
    }
}

/// Per-quantum Metadata-GC work visible at the maintenance seam.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataGcMetrics {
    pub(super) wall: Duration,
    pub(super) barrier_wait: Duration,
    pub(super) object_graph_read_bytes: u64,
    pub(super) candidate_read_bytes: u64,
    pub(super) catalog_read_bytes: u64,
    pub(super) catalog_write_bytes: u64,
    pub(super) unlinked_bytes: u64,
    pub(super) root_syncs: u64,
    pub(super) catalog_chain_runs: u32,
}

impl MetadataGcMetrics {
    #[must_use]
    pub const fn wall(self) -> Duration {
        self.wall
    }

    #[must_use]
    pub const fn barrier_wait(self) -> Duration {
        self.barrier_wait
    }

    #[must_use]
    pub const fn object_graph_read_bytes(self) -> u64 {
        self.object_graph_read_bytes
    }

    #[must_use]
    pub const fn candidate_read_bytes(self) -> u64 {
        self.candidate_read_bytes
    }

    #[must_use]
    pub const fn catalog_read_bytes(self) -> u64 {
        self.catalog_read_bytes
    }

    #[must_use]
    pub const fn catalog_write_bytes(self) -> u64 {
        self.catalog_write_bytes
    }

    #[must_use]
    pub const fn unlinked_bytes(self) -> u64 {
        self.unlinked_bytes
    }

    #[must_use]
    pub const fn root_syncs(self) -> u64 {
        self.root_syncs
    }

    #[must_use]
    pub const fn catalog_chain_runs(self) -> u32 {
        self.catalog_chain_runs
    }
}

impl MetadataGcMarkMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reused => "reused",
            Self::AdditionDelta => "addition_delta",
            Self::ExactSnapshot => "exact_snapshot",
        }
    }

    #[must_use]
    pub const fn has_deletion_authority(self) -> bool {
        matches!(self, Self::ExactSnapshot)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct GenerationMetadataGcSummary {
    pub(super) objects_removed: u64,
    pub(super) bytes_removed: u64,
    pub(super) objects_retained: u64,
    pub(super) mark_mode: MetadataGcMarkMode,
    pub(super) exact_reason: Option<MetadataGcExactReason>,
    pub(super) catalog_generation: Option<u64>,
    pub(super) metrics: MetadataGcMetrics,
}

impl GenerationMetadataGcSummary {
    pub(crate) const fn objects_removed(self) -> u64 {
        self.objects_removed
    }

    pub(crate) const fn bytes_removed(self) -> u64 {
        self.bytes_removed
    }

    pub(crate) const fn objects_retained(self) -> u64 {
        self.objects_retained
    }

    pub(crate) const fn mark_mode(self) -> MetadataGcMarkMode {
        self.mark_mode
    }

    pub(crate) const fn exact_reason(self) -> Option<MetadataGcExactReason> {
        self.exact_reason
    }

    pub(crate) const fn catalog_generation(self) -> Option<u64> {
        self.catalog_generation
    }

    pub(crate) const fn metrics(self) -> MetadataGcMetrics {
        self.metrics
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GenerationLivenessProof {
    pub(super) summary: GenerationScrubSummary,
    pub(super) online_records: Vec<CommitRecord>,
    pub(super) online_chunks: BTreeMap<fastdup_format::ChunkId, u64>,
    pub(super) pinned_roots: BTreeSet<MetadataObjectId>,
    pub(super) recovery_checkpoint_roots: BTreeSet<MetadataObjectId>,
}

/// Metadata-only reachability changes for the current and previous protected
/// Commit generations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenerationLivenessDelta {
    pub(super) base_generation: Option<u64>,
    pub(super) latest_generation: Option<u64>,
    pub(super) added: BTreeMap<fastdup_format::ChunkId, u64>,
    pub(super) removed: BTreeMap<fastdup_format::ChunkId, u64>,
    pub(super) protected_chunk_count: usize,
}

impl GenerationLivenessDelta {
    #[must_use]
    pub const fn base_generation(&self) -> Option<u64> {
        self.base_generation
    }

    #[must_use]
    pub const fn latest_generation(&self) -> Option<u64> {
        self.latest_generation
    }

    #[must_use]
    pub fn added(&self) -> &BTreeMap<fastdup_format::ChunkId, u64> {
        &self.added
    }

    #[must_use]
    pub fn removed(&self) -> &BTreeMap<fastdup_format::ChunkId, u64> {
        &self.removed
    }

    #[must_use]
    pub const fn protected_chunk_count(&self) -> usize {
        self.protected_chunk_count
    }
}

impl GenerationLivenessProof {
    pub(crate) const fn summary(&self) -> GenerationScrubSummary {
        self.summary
    }

    pub(crate) fn online_chunks(&self) -> &BTreeMap<fastdup_format::ChunkId, u64> {
        &self.online_chunks
    }

    pub(crate) fn extend_protected_chunks(
        &mut self,
        additional: BTreeMap<fastdup_format::ChunkId, u64>,
    ) -> Result<(), GenerationError> {
        for (chunk_id, logical_length) in additional {
            if let Some(previous) = self.online_chunks.insert(chunk_id, logical_length)
                && previous != logical_length
            {
                return Err(GenerationError::ManifestChunkLengthConflict {
                    chunk_id,
                    first_length: previous,
                    second_length: logical_length,
                });
            }
        }
        Ok(())
    }
}

impl GenerationScrubSummary {
    #[must_use]
    pub const fn generations(self) -> usize {
        self.generations
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
    pub const fn latest_namespace_inodes(self) -> usize {
        self.latest_namespace_inodes
    }

    #[must_use]
    pub const fn latest_manifest_files(self) -> usize {
        self.latest_manifest_files
    }
}

pub use crate::generation_log::LogTail as WalTail;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredGeneration {
    pub(super) record: CommitRecord,
    pub(super) namespace_root: NamespaceRoot,
    pub(super) wal_tail: WalTail,
    pub(super) rejected_newer_generations: u64,
    pub(super) inode_reservation_end_high_water: u64,
}

/// One committed DATA generation and the Manifest readers proven by its graph
/// verification.
#[derive(Debug)]
pub struct CommittedDataGeneration<I> {
    pub(super) record: CommitRecord,
    pub(super) files: Vec<VerifiedCommittedFile<I>>,
}

/// One recovered generation and demand-verifying Manifest readers bound to that
/// selected candidate. The recovery entry point determines whether payloads were
/// preverified or are awaiting background scrub.
#[derive(Debug)]
pub struct RecoveredDataGeneration<I> {
    pub(super) generation: RecoveredGeneration,
    pub(super) files: Vec<VerifiedCommittedFile<I>>,
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
/// complete committed Metadata-graph validation. Returned bytes always undergo
/// full DATA verification, including after a structurally verified startup.
#[derive(Debug)]
pub struct VerifiedCommittedFile<I> {
    pub(super) inode: u64,
    pub(super) file: VerifiedManifestFile<I>,
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

    /// Returns the opaque graph summary established by complete verification
    /// or a verified successor transition.
    #[must_use]
    pub fn manifest_summary(&self) -> Option<ManifestTreeSummary> {
        self.file.manifest_root().map(|root| {
            ManifestTreeSummary::new(root, self.file.logical_size(), self.file.allocated_bytes())
        })
    }

    #[must_use]
    pub fn into_file(self) -> VerifiedManifestFile<I> {
        self.file
    }
}

pub(super) fn verified_files<M, I>(
    manifests: Vec<(u64, ManifestTreeSummary)>,
    generations: &GenerationRepository<M>,
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
                generations.storage.clone(),
                containers.clone(),
                generations.pin_metadata_root(summary.root()),
                Arc::clone(&generations.manifest_cache),
                Arc::clone(&generations.metadata_cache),
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
