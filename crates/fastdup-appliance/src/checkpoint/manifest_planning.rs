//! Manifest planning and publication for one frozen Namespace generation.
//!
//! This module owns the translation from a frozen POSIX view to immutable
//! Manifest objects and the final namespace-generation publication. Durable
//! serialization and successor-proof ordering stay local to this module.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};

use fastdup_format::{
    ChunkId, CommitRecord, ContainerId, DurableInode, DurableRootMetadata, DurableTimes,
    DurableTimestamp, DurableXattr, ExactIndexEntry, ExactIndexProfileId, MAX_LOGICAL_CHUNK_BYTES,
    ManifestExtent, ManifestLayout, MetadataObjectId, NamespaceEntry, NamespaceRoot,
    PrehashedAdaptiveRegion, PrehashedChunk,
};
use fastdup_posix::{
    CommitInode, CommitRange, CommittedFile, CommittedFileInstall, InodeId, NamespaceCommit,
    PosixError, PreparedCommitExtent, PreparedDataRecipe,
};
use fastdup_store::{
    AdaptiveContainerPublishMetrics, ContainerGenerationAllocator, ContainerPlacement,
    ContainerRepository, GenerationRepository, ManifestSuccessorProof, ManifestTreeSummary,
    PersistentChunkPlan, ReadIntent, ReadIntentScope, StorageIo, SuccessorPredecessor,
    VerifiedCommittedFile, seqcdc_cut, seqcdc_cut_scalar,
};

use crate::ManifestCommittedFile;

use super::metrics::{CheckpointReductionMetrics, CheckpointStage, PhaseStarted};
use super::write_through::DrainResidue;
use super::{
    CDC_MAXIMUM_BYTES, COMPRESSION_REGION_TARGET_BYTES, CONTAINER_PAYLOAD_TARGET_BYTES,
    DurableNamespace, DurableNamespaceError, InstalledManifest, ManifestReaderPolicy,
    OnlineDependencyProofs, OnlineProofAdmission, OnlineSuccessorVerifier, SEQCDC_CONFIG_V1,
    seqcdc_force_scalar,
};

type RetainedManifestRanges = BTreeMap<u64, BTreeMap<MetadataObjectId, Vec<Range<u64>>>>;
type AdaptiveCommitFinish = (
    Vec<ExactIndexEntry>,
    CheckpointReductionMetrics,
    RetainedManifestRanges,
);

impl<M, C> DurableNamespace<M, C>
where
    M: Clone + Send + Sync + StorageIo + 'static,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    #[allow(clippy::too_many_lines)]
    fn publish_manifest_plans(
        &self,
        commit: &NamespaceCommit,
        manifests: Vec<ManifestPublication>,
        predecessor: SuccessorPredecessor,
        retained_ranges: &RetainedManifestRanges,
    ) -> Result<(Vec<DurableInode>, Vec<ManifestSuccessorProof>), DurableNamespaceError> {
        if manifests.len() != commit.inodes().len() {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let mut durable_inodes = Vec::new();
        let mut successor_proofs = Vec::new();
        durable_inodes
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        successor_proofs
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (inode, publication) in commit.inodes().iter().zip(manifests) {
            let (manifest_root, mut proof) = match &publication {
                ManifestPublication::Reuse { summary } => {
                    let proof = self
                        .generations
                        .reuse_manifest_successor(predecessor, *summary);
                    (summary.root(), proof)
                }
                ManifestPublication::Append { previous, extents } => {
                    let proof =
                        self.generations
                            .stage_manifest_append(predecessor, *previous, extents)?;
                    (proof.summary().root(), proof)
                }
                ManifestPublication::Replace {
                    previous,
                    replacements,
                    appended,
                } => {
                    let mut proof = self
                        .generations
                        .reuse_manifest_successor(predecessor, *previous);
                    let edits: Vec<_> = replacements
                        .iter()
                        .map(|replacement| {
                            (replacement.replaced.clone(), replacement.extents.as_slice())
                        })
                        .collect();
                    proof = self
                        .generations
                        .stage_manifest_replacements_successor(proof, &edits)?;
                    if !appended.is_empty() {
                        proof = self
                            .generations
                            .stage_manifest_append_successor(proof, appended)?;
                    }
                    (proof.summary().root(), proof)
                }
                ManifestPublication::Truncate {
                    previous,
                    replacements,
                    logical_size,
                    allocated_bytes,
                } => {
                    let mut proof = self
                        .generations
                        .reuse_manifest_successor(predecessor, *previous);
                    let edits: Vec<_> = replacements
                        .iter()
                        .map(|replacement| {
                            (replacement.replaced.clone(), replacement.extents.as_slice())
                        })
                        .collect();
                    proof = self
                        .generations
                        .stage_manifest_replacements_successor(proof, &edits)?;
                    proof = self
                        .generations
                        .stage_manifest_truncate_successor(proof, *logical_size)?;
                    if proof.summary().allocated_bytes() != *allocated_bytes {
                        return Err(DurableNamespaceError::FrozenViewMismatch);
                    }
                    (proof.summary().root(), proof)
                }
                ManifestPublication::Complete { manifest } => {
                    let proof = self
                        .generations
                        .stage_manifest_layout_successor(predecessor, manifest)?;
                    (proof.summary().root(), proof)
                }
            };
            if let Some(by_root) = retained_ranges.get(&inode.inode().get()) {
                for (source_root, ranges) in by_root {
                    for source_range in coalesced_ranges(ranges)? {
                        proof = self
                            .generations
                            .retain_predecessor_manifest_range_successor(
                                proof,
                                *source_root,
                                source_range,
                            )?;
                    }
                }
            }
            successor_proofs.push(proof);
            durable_inodes.push(
                DurableInode::new_with_metadata(
                    inode.inode().get(),
                    inode.mode(),
                    inode.uid(),
                    inode.gid(),
                    inode.link_count(),
                    inode.mutation_sequence(),
                    inode.logical_size(),
                    manifest_root,
                    inode.metadata().file_flags(),
                    durable_xattrs(inode.metadata())?,
                )?
                .with_times(durable_times(inode.times())),
            );
        }
        Ok((durable_inodes, successor_proofs))
    }

    pub(super) fn publish_generation(
        &self,
        commit: &NamespaceCommit,
        manifests: Vec<ManifestPublication>,
        retained_ranges: &RetainedManifestRanges,
    ) -> Result<CommitRecord, DurableNamespaceError> {
        let predecessor = *self
            .installed_predecessor
            .lock()
            .expect("ASSERT: installed predecessor lock poisoned");
        let phase = self
            .checkpoint_timings
            .begin(CheckpointStage::MetadataManifests);
        let (durable_inodes, successor_proofs) =
            self.publish_manifest_plans(commit, manifests, predecessor, retained_ranges)?;
        drop(phase);
        let mut installs = Vec::new();
        installs
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let phase = self
            .checkpoint_timings
            .begin(CheckpointStage::NamespaceRoot);
        let root = namespace_root_for_commit(commit, durable_inodes)?;
        drop(phase);
        let fallback = self
            .manifest_readers
            .online_graph_verifier(self.containers.clone());
        let graph_verifier = OnlineSuccessorVerifier {
            proofs: Arc::clone(&self.online_dependency_proofs),
            fallback,
        };
        let phase = self
            .checkpoint_timings
            .begin(CheckpointStage::NamespaceCommit);
        let committed = self
            .generations
            .commit_namespace_with_successor_proofs_using(
                &root,
                &self.containers,
                predecessor,
                &successor_proofs,
                &graph_verifier,
            )?;
        drop(phase);
        let _phase = self
            .checkpoint_timings
            .begin(CheckpointStage::NamespaceInstall);
        let (record, verified_files) = committed.into_parts();
        assert_eq!(
            verified_files.len(),
            commit.inodes().len(),
            "ASSERT: committed DATA proof must cover every frozen inode"
        );
        let mut next_manifests = Vec::new();
        next_manifests
            .try_reserve_exact(verified_files.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (inode, verified) in commit.inodes().iter().zip(verified_files) {
            assert_eq!(
                verified.inode(),
                inode.inode().get(),
                "ASSERT: committed DATA proof order must match the Namespace Root"
            );
            assert_eq!(
                verified.logical_size(),
                inode.logical_size(),
                "ASSERT: published Manifest reread length must equal the planned Manifest"
            );
            assert_eq!(
                verified.manifest_root(),
                root.inodes()
                    .binary_search_by_key(&inode.inode().get(), DurableInode::inode)
                    .ok()
                    .and_then(|ordinal| root.inodes()[ordinal].file_manifest_root()),
                "ASSERT: committed DATA proof must retain the published Manifest Root"
            );
            assert_eq!(
                verified.allocated_bytes(),
                inode.allocated_bytes(),
                "ASSERT: committed Manifest allocation must match the Frozen inode"
            );
            let summary = verified
                .manifest_summary()
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            next_manifests.push(InstalledManifest {
                inode: inode.inode().get(),
                root: summary.root(),
                logical_size: summary.logical_size(),
                allocated_bytes: summary.allocated_bytes(),
                summary,
            });
            let installed = Arc::new(ManifestCommittedFile::from_verified(
                self.manifest_readers.prepare(verified.into_file()),
            )) as Arc<dyn CommittedFile>;
            installs.push(CommittedFileInstall::new(
                inode.inode(),
                inode.mutation_sequence(),
                installed,
            ));
        }
        self.namespace.complete_commit(commit, installs)?;
        *self
            .manifests
            .lock()
            .expect("ASSERT: installed Manifest cache lock poisoned") = next_manifests;
        *self
            .installed_predecessor
            .lock()
            .expect("ASSERT: installed predecessor lock poisoned") =
            SuccessorPredecessor::from_committed_record(record);
        Ok(record)
    }
}

fn namespace_root_for_commit(
    commit: &NamespaceCommit,
    mut durable_inodes: Vec<DurableInode>,
) -> Result<NamespaceRoot, DurableNamespaceError> {
    durable_inodes
        .try_reserve_exact(commit.directories().len() + commit.symlinks().len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for directory in commit.directories() {
        durable_inodes.push(
            DurableInode::new_directory_with_metadata(
                directory.inode().get(),
                directory.mode(),
                directory.uid(),
                directory.gid(),
                directory.link_count(),
                directory.mutation_sequence(),
                directory.metadata().file_flags(),
                durable_xattrs(directory.metadata())?,
            )?
            .with_times(durable_times(directory.times())),
        );
    }
    for symlink in commit.symlinks() {
        durable_inodes.push(
            DurableInode::new_symlink(
                symlink.inode().get(),
                symlink.uid(),
                symlink.gid(),
                symlink.link_count(),
                symlink.mutation_sequence(),
                symlink.target().to_vec(),
            )?
            .with_times(durable_times(symlink.times())),
        );
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(commit.entries().len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for entry in commit.entries() {
        entries.push(NamespaceEntry::new(
            entry.parent().get(),
            entry.target().get(),
            entry.name().to_vec(),
        )?);
    }
    NamespaceRoot::new_with_root_metadata(
        commit.inode_reservation_end(),
        commit.inode_allocation_cursor(),
        commit.namespace_mutation_sequence(),
        DurableRootMetadata::new(
            commit.root().mode(),
            commit.root().uid(),
            commit.root().gid(),
            commit.root().metadata().file_flags(),
            durable_xattrs(commit.root().metadata())?,
        )?
        .with_times(durable_times(commit.root().times())),
        durable_inodes,
        entries,
    )
    .map_err(Into::into)
}

fn durable_times(times: fastdup_posix::PosixTimes) -> DurableTimes {
    fn timestamp(value: fastdup_posix::PosixTimestamp) -> DurableTimestamp {
        DurableTimestamp {
            seconds: value.seconds,
            nanoseconds: value.nanoseconds,
        }
    }
    DurableTimes {
        atime: timestamp(times.atime),
        mtime: timestamp(times.mtime),
        ctime: timestamp(times.ctime),
    }
}

fn durable_xattrs(
    metadata: &fastdup_posix::InodeMetadata,
) -> Result<Vec<DurableXattr>, DurableNamespaceError> {
    let attributes = metadata.xattrs();
    let mut durable = Vec::new();
    durable
        .try_reserve_exact(attributes.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for xattr in attributes {
        durable.push(DurableXattr::new(
            xattr.name().to_vec(),
            xattr.value().to_vec(),
        )?);
    }
    Ok(durable)
}

pub(super) enum ManifestPublication {
    Reuse {
        summary: ManifestTreeSummary,
    },
    Append {
        previous: ManifestTreeSummary,
        extents: Vec<ManifestExtent>,
    },
    Replace {
        previous: ManifestTreeSummary,
        replacements: Vec<ManifestReplacement>,
        appended: Vec<ManifestExtent>,
    },
    Truncate {
        previous: ManifestTreeSummary,
        replacements: Vec<ManifestReplacement>,
        logical_size: u64,
        allocated_bytes: u64,
    },
    Complete {
        manifest: ManifestLayout,
    },
}

pub(super) struct ManifestReplacement {
    replaced: Range<u64>,
    extents: Vec<ManifestExtent>,
}

pub(super) fn plan_checkpoint_manifest<M: StorageIo, C: StorageIo>(
    inode: &CommitInode,
    previous: Option<InstalledManifest>,
    generations: &GenerationRepository<M>,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestPublication, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    let changed = inode.changed_ranges()?;
    if let Some(previous) = previous
        && previous.logical_size == logical_size
    {
        if changed.is_empty() {
            if previous.allocated_bytes != inode.allocated_bytes() {
                return Err(DurableNamespaceError::FrozenViewMismatch);
            }
            return Ok(ManifestPublication::Reuse {
                summary: previous.summary,
            });
        }
        return plan_path_local_manifest(inode, previous, &changed, generations, writer);
    }

    if let Some(previous) = previous
        && previous.logical_size < logical_size
        && changed
            .iter()
            .all(|range| range.offset() >= previous.logical_size)
    {
        return plan_append_manifest(inode, previous, writer);
    }

    if let Some(previous) = previous {
        return plan_path_local_manifest(inode, previous, &changed, generations, writer);
    }
    let manifest = plan_full_manifest(inode, writer)?;
    Ok(ManifestPublication::Complete { manifest })
}

fn plan_append_manifest<C: StorageIo>(
    inode: &CommitInode,
    previous: InstalledManifest,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestPublication, DurableNamespaceError> {
    let append_length = inode
        .logical_size()
        .checked_sub(previous.logical_size)
        .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
    assert!(append_length > 0, "ASSERT: append plan must grow the file");
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut extents = Vec::new();
    plan_manifest_range_with_prepared(
        inode,
        previous.logical_size,
        append_length,
        writer,
        &mut stack,
        &mut extents,
    )?;
    let appended_allocated = extents.iter().try_fold(0_u64, |total, extent| {
        if matches!(extent, ManifestExtent::Hole { .. }) {
            Ok(total)
        } else {
            total
                .checked_add(extent_length(extent))
                .ok_or(DurableNamespaceError::ArithmeticOverflow)
        }
    })?;
    if previous
        .allocated_bytes
        .checked_add(appended_allocated)
        .ok_or(DurableNamespaceError::ArithmeticOverflow)?
        != inode.allocated_bytes()
    {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(ManifestPublication::Append {
        previous: previous.summary,
        extents,
    })
}

fn plan_path_local_manifest<M: StorageIo, C: StorageIo>(
    inode: &CommitInode,
    previous: InstalledManifest,
    changed: &[CommitRange],
    generations: &GenerationRepository<M>,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestPublication, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    let shrinking = logical_size < previous.logical_size;
    let rewrites = manifest_rewrites(logical_size, previous, changed)?;
    let mut replacements = Vec::new();
    replacements
        .try_reserve_exact(rewrites.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut allocated_bytes = previous.allocated_bytes;
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for rewrite in rewrites {
        let previous_extents = generations.read_manifest_range(
            previous.root,
            previous.logical_size,
            rewrite.start..rewrite.end,
        )?;
        let removed = allocated_bytes_in_range(&previous_extents, rewrite.start..rewrite.end)?;
        let mut extents = Vec::new();
        plan_manifest_range_with_prepared(
            inode,
            rewrite.start,
            rewrite.end.min(logical_size) - rewrite.start,
            writer,
            &mut stack,
            &mut extents,
        )?;
        assert!(
            stack.is_empty(),
            "ASSERT: path-local range planner must consume its complete work stack"
        );
        if rewrite.end > logical_size {
            extents.push(ManifestExtent::Hole {
                logical_length: rewrite.end - logical_size,
            });
        }
        ManifestLayout::validate(rewrite.end - rewrite.start, &extents)?;
        let added = manifest_extent_allocation(&extents)?;
        allocated_bytes = allocated_bytes
            .checked_sub(removed)
            .and_then(|remaining| remaining.checked_add(added))
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        replacements.push(ManifestReplacement {
            replaced: rewrite.start..rewrite.end,
            extents,
        });
    }
    let mut appended = Vec::new();
    if inode.logical_size() > previous.logical_size {
        plan_manifest_range_with_prepared(
            inode,
            previous.logical_size,
            inode.logical_size() - previous.logical_size,
            writer,
            &mut stack,
            &mut appended,
        )?;
        allocated_bytes = allocated_bytes
            .checked_add(manifest_extent_allocation(&appended)?)
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
    }
    if shrinking {
        return Ok(ManifestPublication::Truncate {
            previous: previous.summary,
            replacements,
            logical_size,
            allocated_bytes: inode.allocated_bytes(),
        });
    }
    if allocated_bytes != inode.allocated_bytes() {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(ManifestPublication::Replace {
        previous: previous.summary,
        replacements,
        appended,
    })
}

fn manifest_rewrites(
    logical_size: u64,
    previous: InstalledManifest,
    changed: &[CommitRange],
) -> Result<Vec<RewriteRange>, DurableNamespaceError> {
    let mut rewrites = rewrite_ranges_before(
        changed,
        logical_size,
        previous.logical_size.min(logical_size),
    )?;
    // The persistent tree preserves untouched edges as DATA_SLICE recipes.
    // Expanding to the old DATA extent would materialize and rechunk bytes
    // outside the actual mutation, including on a cold one-byte overwrite.
    coalesce_rewrites(&mut rewrites);

    Ok(rewrites)
}

fn manifest_extent_allocation(extents: &[ManifestExtent]) -> Result<u64, DurableNamespaceError> {
    extents.iter().try_fold(0_u64, |total, extent| {
        if matches!(extent, ManifestExtent::Hole { .. }) {
            Ok(total)
        } else {
            total
                .checked_add(extent_length(extent))
                .ok_or(DurableNamespaceError::ArithmeticOverflow)
        }
    })
}

fn allocated_bytes_in_range(
    extents: &[fastdup_store::ManifestRangeExtent],
    range: Range<u64>,
) -> Result<u64, DurableNamespaceError> {
    let mut allocated = 0_u64;
    for extent in extents {
        if matches!(extent.extent(), ManifestExtent::Hole { .. }) {
            continue;
        }
        let extent_end = extent
            .logical_offset()
            .checked_add(extent_length(extent.extent()))
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        let start = extent.logical_offset().max(range.start);
        let end = extent_end.min(range.end);
        if start < end {
            allocated = allocated
                .checked_add(end - start)
                .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        }
    }
    Ok(allocated)
}

/// Exact-Index profile paired with the durable checkpoint's SeqCDC-v1 rules.
///
/// # Panics
///
/// Panics only if BLAKE3 maps the fixed canonical profile bytes to the
/// reserved all-zero identity, an impossible production `ASSERT` for this
/// pinned input.
#[must_use]
pub fn checkpoint_exact_index_profile_v1() -> ExactIndexProfileId {
    ExactIndexProfileId::new(
        ChunkId::of(
            b"fastdup/SeqCDC-v1/mode=increasing,sequence=6,skip-trigger=50,skip=1024,min=16384,max=262144",
        )
        .bytes(),
    )
    .expect("ASSERT: the SeqCDC-v1 Exact-Index profile hash is nonzero")
}

pub(super) fn load_manifest_cache<I: StorageIo>(
    root: &NamespaceRoot,
    files: &[VerifiedCommittedFile<I>],
) -> Result<Vec<InstalledManifest>, DurableNamespaceError> {
    if root.file_inode_count() != files.len() {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    let mut manifests: Vec<InstalledManifest> = Vec::new();
    manifests
        .try_reserve_exact(root.file_inode_count())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for (inode, file) in root.file_inodes().zip(files) {
        if file.inode() != inode.inode()
            || file.manifest_root() != Some(inode.manifest_root())
            || file.logical_size() != inode.logical_size()
        {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if let Some(previous) = manifests.last()
            && previous.inode >= inode.inode()
        {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let summary = file
            .manifest_summary()
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        manifests.push(InstalledManifest {
            inode: inode.inode(),
            root: summary.root(),
            logical_size: summary.logical_size(),
            allocated_bytes: summary.allocated_bytes(),
            summary,
        });
    }
    Ok(manifests)
}

fn plan_full_manifest<C: StorageIo>(
    inode: &CommitInode,
    writer: &mut AdaptiveCommitWriter<'_, C>,
) -> Result<ManifestLayout, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut extents = Vec::new();
    if logical_size != 0 {
        plan_manifest_range_with_prepared(
            inode,
            0,
            logical_size,
            writer,
            &mut stack,
            &mut extents,
        )?;
    }
    let manifest = ManifestLayout::new(logical_size, extents)?;
    verify_manifest_allocation(&manifest, inode)?;
    Ok(manifest)
}

#[derive(Clone, Copy, Debug)]
struct RewriteRange {
    start: u64,
    end: u64,
}

fn rewrite_ranges_before(
    changed: &[CommitRange],
    logical_size: u64,
    end_limit: u64,
) -> Result<Vec<RewriteRange>, DurableNamespaceError> {
    if end_limit > logical_size {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    let mut rewrites = Vec::new();
    rewrites
        .try_reserve_exact(changed.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut previous_end = 0_u64;
    for range in changed {
        let raw_end = range
            .offset()
            .checked_add(range.length())
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        if range.length() == 0 || raw_end > logical_size || range.offset() < previous_end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let clipped_end = raw_end.min(end_limit);
        if range.offset() >= clipped_end {
            previous_end = raw_end;
            continue;
        }
        // Manifest tree rewriting can split predecessor DATA extents into
        // authenticated DATA_SLICE recipes. Rechunk only the bytes that the
        // frozen epoch says changed; rounding to a CDC cell would reread the
        // untouched predecessor edges from DATA.
        let start = range.offset();
        let end = clipped_end;
        if start >= end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        rewrites.push(RewriteRange { start, end });
        previous_end = raw_end;
    }
    coalesce_rewrites(&mut rewrites);
    Ok(rewrites)
}

fn coalesce_rewrites(rewrites: &mut Vec<RewriteRange>) {
    let mut output = 0_usize;
    for input in 0..rewrites.len() {
        let candidate = rewrites[input];
        if output > 0 && rewrites[output - 1].end >= candidate.start {
            rewrites[output - 1].end = rewrites[output - 1].end.max(candidate.end);
        } else {
            rewrites[output] = candidate;
            output += 1;
        }
    }
    rewrites.truncate(output);
}

fn coalesced_ranges(ranges: &[Range<u64>]) -> Result<Vec<Range<u64>>, DurableNamespaceError> {
    let mut sorted = Vec::new();
    sorted
        .try_reserve_exact(ranges.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    sorted.extend_from_slice(ranges);
    sorted.sort_unstable_by_key(|range| range.start);
    let mut output = Vec::<Range<u64>>::new();
    output
        .try_reserve_exact(sorted.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for range in sorted {
        if range.start >= range.end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        match output.last_mut() {
            Some(previous) if range.start <= previous.end => {
                previous.end = previous.end.max(range.end);
            }
            _ => output.push(range),
        }
    }
    Ok(output)
}

const fn extent_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::DataSlice { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
    }
}

/// One unit of the Manifest planner work stack.
///
/// `Range` items cover logical bytes that still require hole classification
/// and `SeqCDC` re-chunking. `Residue` items carry one complete commit-cut
/// Drain Residue Chunk that must become a DATA extent at its staged offset
/// without any reread or re-cut.
#[derive(Clone, Copy, Debug)]
enum PlanItem {
    Range {
        offset: u64,
        length: u64,
    },
    Residue {
        length: u64,
        chunk_id: ChunkId,
        chunk_length: u32,
        chunk_offset: u32,
    },
}

fn plan_manifest_range_with_prepared<C: StorageIo>(
    inode: &CommitInode,
    offset: u64,
    length: u64,
    writer: &mut AdaptiveCommitWriter<'_, C>,
    stack: &mut Vec<PlanItem>,
    extents: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    assert!(length > 0, "ASSERT: manifest range must be nonempty");
    assert!(
        stack.is_empty(),
        "ASSERT: prepared range planning requires an empty work stack"
    );
    let end = offset
        .checked_add(length)
        .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
    let prepared = inode.prepared_extents_in_range(offset, length)?;
    let mut cursor = offset;
    for extent in prepared {
        let prepared_end = extent
            .offset()
            .checked_add(extent.length())
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        if extent.offset() < cursor || prepared_end > end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if cursor < extent.offset() {
            stack.push(PlanItem::Range {
                offset: cursor,
                length: extent.offset() - cursor,
            });
            plan_manifest_ranges(inode, writer, stack, extents)?;
            assert!(
                stack.is_empty(),
                "ASSERT: range planner must consume a prepared-recipe gap"
            );
        }
        let manifest_extent = match extent.recipe() {
            PreparedDataRecipe::Chunk { chunk_id } => {
                if extent.length()
                    > u64::try_from(MAX_LOGICAL_CHUNK_BYTES)
                        .expect("ASSERT: maximum logical Chunk bytes fit u64")
                {
                    return Err(DurableNamespaceError::FrozenViewMismatch);
                }
                let chunk_id = ChunkId::from_bytes(chunk_id);
                writer.record_prepared_chunk(chunk_id, extent.length(), extent)?;
                ManifestExtent::Data {
                    logical_length: extent.length(),
                    chunk_id,
                }
            }
            PreparedDataRecipe::ChunkSlice {
                chunk_id,
                chunk_length,
                chunk_offset,
            } => {
                let slice_end = u64::from(chunk_offset)
                    .checked_add(extent.length())
                    .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
                if chunk_length == 0
                    || usize::try_from(chunk_length).is_err()
                    || usize::try_from(chunk_length)
                        .is_ok_and(|length| length > MAX_LOGICAL_CHUNK_BYTES)
                    || slice_end > u64::from(chunk_length)
                {
                    return Err(DurableNamespaceError::FrozenViewMismatch);
                }
                let chunk_id = ChunkId::from_bytes(chunk_id);
                writer.record_prepared_chunk(chunk_id, u64::from(chunk_length), extent)?;
                ManifestExtent::DataSlice {
                    logical_length: extent.length(),
                    chunk_id,
                    chunk_length,
                    chunk_offset,
                }
            }
            PreparedDataRecipe::Fill { value } => {
                writer.record_prepared_fill(extent.length());
                ManifestExtent::Fill {
                    logical_length: extent.length(),
                    value,
                }
            }
        };
        push_extent(extents, manifest_extent)?;
        cursor = prepared_end;
    }
    if cursor < end {
        stack.push(PlanItem::Range {
            offset: cursor,
            length: end - cursor,
        });
        plan_manifest_ranges(inode, writer, stack, extents)?;
    }
    assert!(
        stack.is_empty(),
        "ASSERT: prepared range planner must consume its complete work stack"
    );
    Ok(())
}

/// Interleaves taken Drain Residue emissions with the gap Ranges they do not
/// cover, pushing them onto the work stack in pop order.
fn push_residue_overlay(
    stack: &mut Vec<PlanItem>,
    offset: u64,
    end: u64,
    residues: Vec<ResidueEmission>,
) -> Result<(), DurableNamespaceError> {
    let mut sub = Vec::new();
    sub.try_reserve(residues.len() * 2 + 1)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut cursor = offset;
    for emission in residues {
        if emission.logical_offset > cursor {
            sub.push(PlanItem::Range {
                offset: cursor,
                length: emission.logical_offset - cursor,
            });
        }
        sub.push(PlanItem::Residue {
            length: emission.logical_length,
            chunk_id: emission.chunk_id,
            chunk_length: emission.chunk_length,
            chunk_offset: emission.chunk_offset,
        });
        cursor = emission
            .logical_offset
            .checked_add(emission.logical_length)
            .expect("ASSERT: consumed emission ends inside its Range");
    }
    if cursor < end {
        sub.push(PlanItem::Range {
            offset: cursor,
            length: end - cursor,
        });
    }
    for item in sub.into_iter().rev() {
        stack.push(item);
    }
    Ok(())
}

fn plan_manifest_ranges<C: StorageIo>(
    inode: &CommitInode,
    writer: &mut AdaptiveCommitWriter<'_, C>,
    stack: &mut Vec<PlanItem>,
    extents: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    while let Some(item) = stack.pop() {
        let (offset, length) = match item {
            PlanItem::Residue {
                length,
                chunk_id,
                chunk_length,
                chunk_offset,
            } => {
                writer.record_residue_extent(chunk_id, u64::from(chunk_length));
                let extent = if chunk_offset == 0 && u64::from(chunk_length) == length {
                    ManifestExtent::Data {
                        logical_length: length,
                        chunk_id,
                    }
                } else {
                    ManifestExtent::DataSlice {
                        logical_length: length,
                        chunk_id,
                        chunk_length,
                        chunk_offset,
                    }
                };
                push_extent(extents, extent)?;
                continue;
            }
            PlanItem::Range { offset, length } => (offset, length),
        };
        assert!(
            length > 0,
            "ASSERT: manifest planner range must be nonempty"
        );
        let end = offset
            .checked_add(length)
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        // Drain Residue Chunks first: complete commit-cut Chunks become DATA
        // extents at their staged Offsets without reread or re-cut. Only the
        // gaps between them fall through to hole classification and SeqCDC.
        let residues = writer.take_residue_spans(inode.inode(), offset, end);
        if !residues.is_empty() {
            push_residue_overlay(stack, offset, end, residues)?;
            continue;
        }
        let allocated = inode.allocated_bytes_in_range(offset, length)?;
        if allocated > length {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if allocated == 0 {
            push_extent(
                extents,
                ManifestExtent::Hole {
                    logical_length: length,
                },
            )?;
            continue;
        }
        if allocated == length {
            plan_allocated_range(inode, offset, length, writer, extents)?;
            continue;
        }

        let left_length = length / 2;
        if left_length == 0 || left_length == length {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let right_offset = offset
            .checked_add(left_length)
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        stack.push(PlanItem::Range {
            offset: right_offset,
            length: length - left_length,
        });
        stack.push(PlanItem::Range {
            offset,
            length: left_length,
        });
    }
    Ok(())
}

fn plan_allocated_range<C: StorageIo>(
    inode: &CommitInode,
    offset: u64,
    length: u64,
    writer: &mut AdaptiveCommitWriter<'_, C>,
    extents: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    assert!(length > 0, "ASSERT: a DATA range must be nonempty");
    assert_eq!(
        CDC_MAXIMUM_BYTES, MAX_LOGICAL_CHUNK_BYTES,
        "ASSERT: SeqCDC-v1 maximum must equal the durable format bound"
    );
    writer.metrics.checkpoint_rechunk_bytes = writer
        .metrics
        .checkpoint_rechunk_bytes
        .checked_add(length)
        .expect("ASSERT: checkpoint rechunk bytes cannot overflow u64");
    let reader = CommitRangeReader::new(inode, offset, length);
    let mut chunks = SeqCdcStream::new(reader)?;
    let mut expected_offset = 0_u64;
    loop {
        let cdc_started = PhaseStarted::now();
        let chunk = read_next_rechunk_chunk(&mut chunks)?;
        cdc_started.finish_into(&mut writer.metrics.cdc);
        let Some(chunk) = chunk else {
            break;
        };
        assert_eq!(
            expected_offset,
            chunks.consumed_bytes() - u64::try_from(chunk.len()).expect("bounded Chunk length"),
            "ASSERT: SeqCDC-v1 chunks must be contiguous"
        );
        assert!(
            !chunk.is_empty() && chunk.len() <= CDC_MAXIMUM_BYTES,
            "ASSERT: SeqCDC-v1 returned an invalid logical Chunk length"
        );
        let logical_length =
            u64::try_from(chunk.len()).expect("ASSERT: a bounded SeqCDC Chunk length fits u64");
        writer.metrics.logical_chunks = writer
            .metrics
            .logical_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint logical Chunk count cannot overflow u64");
        writer.metrics.logical_chunk_bytes = writer
            .metrics
            .logical_chunk_bytes
            .checked_add(logical_length)
            .expect("ASSERT: checkpoint logical Chunk bytes cannot overflow u64");
        expected_offset = expected_offset
            .checked_add(logical_length)
            .expect("ASSERT: a SeqCDC DATA range cursor cannot overflow");
        let hash_started = PhaseStarted::now();
        let fill = chunk.iter().all(|byte| *byte == chunk[0]);
        let chunk_id = (!fill).then(|| ChunkId::of(&chunk));
        hash_started.finish_into(&mut writer.metrics.hash_and_fill);
        if fill {
            writer.metrics.fill_chunks = writer
                .metrics
                .fill_chunks
                .checked_add(1)
                .expect("ASSERT: checkpoint FILL Chunk count cannot overflow u64");
            writer.metrics.fill_bytes = writer
                .metrics
                .fill_bytes
                .checked_add(logical_length)
                .expect("ASSERT: checkpoint FILL bytes cannot overflow u64");
            push_extent(
                extents,
                ManifestExtent::Fill {
                    logical_length,
                    value: chunk[0],
                },
            )?;
        } else {
            let chunk_id = chunk_id.expect("ASSERT: non-FILL data must have one Chunk ID");
            writer.push(chunk_id, chunk)?;
            push_extent(
                extents,
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                },
            )?;
        }
    }
    if expected_offset != length {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(())
}

pub(super) struct SeqCdcStream<R> {
    reader: R,
    buffer: Vec<u8>,
    start: usize,
    eof: bool,
    consumed_bytes: u64,
}

impl<R: Read> SeqCdcStream<R> {
    pub(super) fn new(reader: R) -> Result<Self, DurableNamespaceError> {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(2 * CDC_MAXIMUM_BYTES)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        Ok(Self {
            reader,
            buffer,
            start: 0,
            eof: false,
            consumed_bytes: 0,
        })
    }

    pub(super) fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, DurableNamespaceError> {
        if self.start >= CDC_MAXIMUM_BYTES {
            self.buffer.copy_within(self.start.., 0);
            self.buffer.truncate(self.buffer.len() - self.start);
            self.start = 0;
        }
        while self.buffer.len() - self.start < CDC_MAXIMUM_BYTES && !self.eof {
            let available = self.buffer.len() - self.start;
            let requested = CDC_MAXIMUM_BYTES - available;
            let old_length = self.buffer.len();
            let new_length = old_length
                .checked_add(requested)
                .expect("ASSERT: bounded SeqCDC stream buffer cannot overflow");
            assert!(
                new_length <= self.buffer.capacity(),
                "ASSERT: SeqCDC stream buffer exceeds its fixed reservation"
            );
            self.buffer.resize(new_length, 0);
            let read = self.reader.read(&mut self.buffer[old_length..])?;
            self.buffer.truncate(old_length + read);
            self.eof = read == 0;
        }
        let remaining = &self.buffer[self.start..];
        if remaining.is_empty() {
            return Ok(None);
        }
        let length = if seqcdc_force_scalar() {
            seqcdc_cut_scalar(remaining, SEQCDC_CONFIG_V1)
        } else {
            seqcdc_cut(remaining, SEQCDC_CONFIG_V1)
        };
        assert!(
            length != 0 && length <= remaining.len() && length <= CDC_MAXIMUM_BYTES,
            "ASSERT: SeqCDC selected an invalid stream Chunk length"
        );
        let mut chunk = Vec::new();
        chunk
            .try_reserve_exact(length)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        chunk.extend_from_slice(&remaining[..length]);
        self.start += length;
        self.consumed_bytes = self
            .consumed_bytes
            .checked_add(u64::try_from(length).expect("ASSERT: bounded Chunk length fits u64"))
            .expect("ASSERT: SeqCDC stream position cannot overflow");
        Ok(Some(chunk))
    }

    pub(super) const fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes
    }
}

trait FrozenCommitRangeSource {
    fn read_frozen_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError>;
}

impl FrozenCommitRangeSource for CommitInode {
    fn read_frozen_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.read_at(offset, length)
    }
}

fn read_next_rechunk_chunk<R: Read>(
    stream: &mut SeqCdcStream<R>,
) -> Result<Option<Vec<u8>>, DurableNamespaceError> {
    let _scan = ReadIntentScope::enter(ReadIntent::Scan);
    stream.next_chunk()
}

struct CommitRangeReader<'a> {
    source: &'a dyn FrozenCommitRangeSource,
    start: u64,
    consumed: u64,
    length: u64,
}

impl<'a> CommitRangeReader<'a> {
    fn new(source: &'a dyn FrozenCommitRangeSource, start: u64, length: u64) -> Self {
        Self {
            source,
            start,
            consumed: 0,
            length,
        }
    }
}

impl Read for CommitRangeReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.consumed == self.length || output.is_empty() {
            return Ok(0);
        }
        let remaining = self
            .length
            .checked_sub(self.consumed)
            .expect("ASSERT: a range reader cursor cannot exceed its length");
        let requested = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(output.len())
            .min(CDC_MAXIMUM_BYTES);
        let requested_u32 =
            u32::try_from(requested).expect("ASSERT: SeqCDC-v1 reads never exceed 256 KiB");
        let read_offset = self
            .start
            .checked_add(self.consumed)
            .ok_or_else(|| io::Error::other("commit range read offset overflow"))?;
        let bytes = self
            .source
            .read_frozen_at(read_offset, requested_u32)
            .map_err(|error| io::Error::other(format!("commit range read failed: {error:?}")))?;
        if bytes.len() != requested {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "commit range returned fewer bytes than requested",
            ));
        }
        output[..requested].copy_from_slice(&bytes);
        self.consumed = self
            .consumed
            .checked_add(
                u64::try_from(requested).expect("ASSERT: a bounded range read length fits u64"),
            )
            .expect("ASSERT: a bounded range reader cursor cannot overflow");
        Ok(requested)
    }
}

fn verify_manifest_allocation(
    manifest: &ManifestLayout,
    inode: &CommitInode,
) -> Result<(), DurableNamespaceError> {
    let planned_allocated = manifest.extents().iter().try_fold(0_u64, |total, extent| {
        let length = match extent {
            ManifestExtent::Data { logical_length, .. }
            | ManifestExtent::DataSlice { logical_length, .. }
            | ManifestExtent::Fill { logical_length, .. } => *logical_length,
            ManifestExtent::Hole { .. } => 0,
        };
        total.checked_add(length)
    });
    if planned_allocated != Some(inode.allocated_bytes()) {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(())
}

fn push_extent(
    extents: &mut Vec<ManifestExtent>,
    extent: ManifestExtent,
) -> Result<(), DurableNamespaceError> {
    match (extents.last_mut(), &extent) {
        (
            Some(ManifestExtent::Hole { logical_length }),
            ManifestExtent::Hole {
                logical_length: added,
            },
        ) => {
            *logical_length = logical_length
                .checked_add(*added)
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            return Ok(());
        }
        (
            Some(ManifestExtent::Fill {
                logical_length,
                value,
            }),
            ManifestExtent::Fill {
                logical_length: added,
                value: added_value,
            },
        ) if value == added_value => {
            *logical_length = logical_length
                .checked_add(*added)
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            return Ok(());
        }
        _ => {}
    }
    extents
        .try_reserve(1)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    extents.push(extent);
    Ok(())
}

/// One complete commit-cut Drain Residue Chunk pending Manifest extent
/// emission at its staged file Offset.
#[derive(Clone, Copy, Debug)]
struct ResidueSpan {
    offset: u64,
    length: u64,
    chunk_id: ChunkId,
    consumed: bool,
}

/// One clipped emission of a seeded Drain Residue Chunk inside one planned
/// Range. A Chunk staged from the retained `SeqCDC` suffix may begin below the
/// Range's start: bytes outside the Range are already committed at these
/// Offsets by an earlier Manifest, so the emission clips to the intersection
/// and references the staged Chunk as a slice.
#[derive(Clone, Copy)]
struct ResidueEmission {
    logical_offset: u64,
    logical_length: u64,
    chunk_id: ChunkId,
    chunk_length: u32,
    chunk_offset: u32,
}

pub(super) struct AdaptiveCommitWriter<'a, C> {
    containers: &'a ContainerRepository<C>,
    container_generations: &'a ContainerGenerationAllocator<C>,
    index: &'a dyn ManifestReaderPolicy<C>,
    seen: BTreeMap<ChunkId, u64>,
    chunks: Vec<Vec<u8>>,
    chunk_ids: Vec<ChunkId>,
    advanced: bool,
    level_zero_entries: Vec<ExactIndexEntry>,
    payload_bytes: usize,
    workers: NonZeroUsize,
    metrics: CheckpointReductionMetrics,
    current_inode: Option<u64>,
    placement: ContainerPlacement,
    retained_ranges: RetainedManifestRanges,
    online_dependency_proofs: Arc<OnlineDependencyProofs>,
    residues: BTreeMap<u64, DrainResidue>,
    residue_spans: Vec<ResidueSpan>,
}

impl<'a, C: StorageIo> AdaptiveCommitWriter<'a, C> {
    pub(super) fn new(
        containers: &'a ContainerRepository<C>,
        container_generations: &'a ContainerGenerationAllocator<C>,
        index: &'a dyn ManifestReaderPolicy<C>,
        workers: NonZeroUsize,
        online_dependency_proofs: Arc<OnlineDependencyProofs>,
        residues: Vec<DrainResidue>,
    ) -> Self {
        let mut residue_map = BTreeMap::new();
        for residue in residues {
            assert!(
                residue_map.insert(residue.inode.get(), residue).is_none(),
                "ASSERT: one Ingest Lane yields one Drain Residue per Inode"
            );
        }
        Self {
            containers,
            container_generations,
            index,
            seen: BTreeMap::new(),
            chunks: Vec::new(),
            chunk_ids: Vec::new(),
            advanced: false,
            level_zero_entries: Vec::new(),
            payload_bytes: 0,
            workers,
            metrics: CheckpointReductionMetrics::default(),
            current_inode: None,
            placement: ContainerPlacement::Data,
            retained_ranges: BTreeMap::new(),
            online_dependency_proofs,
            residues: residue_map,
            residue_spans: Vec::new(),
        }
    }

    pub(super) fn begin_inode(
        &mut self,
        inode: InodeId,
        placement: ContainerPlacement,
        advanced: bool,
    ) -> Result<(), DurableNamespaceError> {
        // Spans the previous Inode's plan did not emit cover bytes already
        // committed at their Offsets (suffix-anchored Chunks below the first
        // changed Range). Their buffer entries deduplicate against the active
        // Exact Index, so the clear below just drops redundant lookups.
        if self.placement != placement || self.advanced != advanced {
            self.flush()?;
            self.placement = placement;
            self.advanced = advanced;
        }
        self.residue_spans.clear();
        if let Some(mut residue) = self.residues.remove(&inode.get()) {
            self.residue_spans
                .try_reserve(residue.chunks.len())
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            for chunk in std::mem::take(&mut residue.chunks) {
                let bytes_len = chunk.bytes.len();
                let length = u64::try_from(bytes_len)
                    .expect("ASSERT: a bounded Drain Residue Chunk length fits u64");
                // Seed the adaptive buffer with the already-hashed staged
                // Chunk. The planner then emits a DATA extent at the staged
                // Offset instead of re-reading and re-cutting those bytes.
                self.push(chunk.chunk_id, chunk.bytes)?;
                self.residue_spans.push(ResidueSpan {
                    offset: chunk.offset,
                    length,
                    chunk_id: chunk.chunk_id,
                    consumed: false,
                });
                residue.absorb_chunk(bytes_len);
            }
        }
        self.current_inode = Some(inode.get());
        Ok(())
    }

    /// Drains the Drain Residue Spans overlapping `[offset, end)`, marking
    /// them consumed. A Span that reaches past a Range edge is clipped to the
    /// Range: the emission keeps its Chunk slice instead of failing, because a
    /// suffix-anchored Chunk legitimately extends below a changed Range's
    /// start (bytes outside the Range are committed by an earlier Manifest).
    fn take_residue_spans(
        &mut self,
        inode: InodeId,
        offset: u64,
        end: u64,
    ) -> Vec<ResidueEmission> {
        if self.residue_spans.is_empty() {
            return Vec::new();
        }
        assert_eq!(
            self.current_inode,
            Some(inode.get()),
            "ASSERT: Drain Residue planning only serves its seeded Inode"
        );
        let mut taken = Vec::new();
        for span in &mut self.residue_spans {
            if span.consumed {
                continue;
            }
            let span_end = span
                .offset
                .checked_add(span.length)
                .expect("ASSERT: bounded Drain Residue Span end");
            if span_end <= offset || span.offset >= end {
                continue;
            }
            // The Chunk may reach outside this Range (it was staged from the
            // retained SeqCDC suffix or a coalesced rewrite). Clip it: bytes
            // outside the Range are already committed at their Offsets by an
            // earlier Manifest or belong to a later Range that re-reads them.
            span.consumed = true;
            let slice_start = span.offset.max(offset);
            let slice_end = span_end.min(end);
            taken.push(ResidueEmission {
                logical_offset: slice_start,
                logical_length: slice_end
                    .checked_sub(slice_start)
                    .expect("ASSERT: an overlapping Span leaves a nonempty slice"),
                chunk_id: span.chunk_id,
                chunk_length: u32::try_from(span.length)
                    .expect("ASSERT: a bounded Chunk length fits u32"),
                chunk_offset: u32::try_from(slice_start - span.offset)
                    .expect("ASSERT: a bounded Chunk offset fits u32"),
            });
        }
        taken
    }

    fn record_residue_extent(&mut self, chunk_id: ChunkId, length: u64) {
        assert_eq!(
            self.seen.get(&chunk_id),
            Some(&length),
            "ASSERT: a Drain Residue Chunk is buffered and deduplicated once per checkpoint"
        );
        self.metrics.logical_chunks = self
            .metrics
            .logical_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint logical Chunk count cannot overflow");
        self.metrics.logical_chunk_bytes = self
            .metrics
            .logical_chunk_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint logical Chunk bytes cannot overflow");
        self.metrics.drain_merged_chunks = self
            .metrics
            .drain_merged_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint Drain Residue count cannot overflow");
        self.metrics.drain_merged_bytes = self
            .metrics
            .drain_merged_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint Drain Residue bytes cannot overflow");
    }

    fn record_prepared_chunk(
        &mut self,
        chunk_id: ChunkId,
        length: u64,
        prepared: PreparedCommitExtent,
    ) -> Result<(), DurableNamespaceError> {
        assert!(length > 0, "ASSERT: a prepared Chunk is nonempty");
        if let Some(previous) = self.seen.insert(chunk_id, length)
            && previous != length
        {
            return Err(DurableNamespaceError::ChunkLengthConflict {
                chunk_id,
                first_length: previous,
                second_length: length,
            });
        }
        match (
            prepared.retained_manifest_root(),
            prepared.retained_source_offset(),
        ) {
            (Some(root), Some(source_offset)) => {
                let root =
                    MetadataObjectId::new(root).ok_or(DurableNamespaceError::FrozenViewMismatch)?;
                let end = source_offset
                    .checked_add(prepared.length())
                    .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
                let inode = self
                    .current_inode
                    .expect("ASSERT: prepared Chunk planning requires an active inode");
                self.retained_ranges
                    .entry(inode)
                    .or_default()
                    .entry(root)
                    .or_default()
                    .push(source_offset..end);
            }
            (None, None) => {}
            _ => return Err(DurableNamespaceError::FrozenViewMismatch),
        }
        self.record_prepared_recipe(length);
        Ok(())
    }

    fn record_prepared_fill(&mut self, length: u64) {
        assert!(length > 0, "ASSERT: a prepared FILL is nonempty");
        self.metrics.fill_chunks = self
            .metrics
            .fill_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint FILL Chunk count cannot overflow u64");
        self.metrics.fill_bytes = self
            .metrics
            .fill_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint FILL bytes cannot overflow u64");
        self.record_prepared_recipe(length);
    }

    fn record_prepared_recipe(&mut self, length: u64) {
        self.metrics.logical_chunks = self
            .metrics
            .logical_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint logical Chunk count cannot overflow u64");
        self.metrics.logical_chunk_bytes = self
            .metrics
            .logical_chunk_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint logical Chunk bytes cannot overflow u64");
        self.metrics.recipe_reuse_chunks = self
            .metrics
            .recipe_reuse_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint recipe-reuse count cannot overflow u64");
        self.metrics.recipe_reuse_bytes = self
            .metrics
            .recipe_reuse_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint recipe-reuse bytes cannot overflow u64");
    }

    fn push(&mut self, chunk_id: ChunkId, bytes: Vec<u8>) -> Result<(), DurableNamespaceError> {
        let length =
            u64::try_from(bytes.len()).map_err(|_| DurableNamespaceError::FrozenViewMismatch)?;
        let exact_started = PhaseStarted::now();
        if let Some(previous) = self.seen.insert(chunk_id, length) {
            if previous != length {
                return Err(DurableNamespaceError::ChunkLengthConflict {
                    chunk_id,
                    first_length: previous,
                    second_length: length,
                });
            }
            self.record_exact_hit(length);
            exact_started.finish_into(&mut self.metrics.exact_lookup);
            return Ok(());
        }
        if self
            .online_dependency_proofs
            .reuse_location(self.index, self.containers, chunk_id, length, true)
            .is_some()
        {
            if self.advanced {
                self.index
                    .read_cache()
                    .admit_writer_chunk(chunk_id, &[&bytes]);
            }
            self.record_exact_hit(length);
            exact_started.finish_into(&mut self.metrics.exact_lookup);
            return Ok(());
        }
        exact_started.finish_into(&mut self.metrics.exact_lookup);
        self.metrics.new_chunks = self
            .metrics
            .new_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint new Chunk count cannot overflow u64");
        self.metrics.new_chunk_bytes = self
            .metrics
            .new_chunk_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint new Chunk bytes cannot overflow u64");
        let next_payload = self
            .payload_bytes
            .checked_add(bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
        if !self.chunks.is_empty() && next_payload > CONTAINER_PAYLOAD_TARGET_BYTES {
            self.flush()?;
        }
        self.payload_bytes = self
            .payload_bytes
            .checked_add(bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
        self.chunks
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        self.chunks.push(bytes);
        self.chunk_ids.push(chunk_id);
        self.metrics.peak_buffered_chunk_bytes = self.metrics.peak_buffered_chunk_bytes.max(
            u64::try_from(self.payload_bytes)
                .expect("ASSERT: a bounded checkpoint Container payload byte count fits u64"),
        );
        self.metrics.peak_buffered_chunks = self.metrics.peak_buffered_chunks.max(
            u64::try_from(self.chunks.len())
                .expect("ASSERT: a bounded checkpoint Chunk count fits u64"),
        );
        Ok(())
    }

    fn record_exact_hit(&mut self, length: u64) {
        self.metrics.exact_hit_chunks = self
            .metrics
            .exact_hit_chunks
            .checked_add(1)
            .expect("ASSERT: checkpoint Exact Hit count cannot overflow u64");
        self.metrics.exact_hit_bytes = self
            .metrics
            .exact_hit_bytes
            .checked_add(length)
            .expect("ASSERT: checkpoint Exact Hit bytes cannot overflow u64");
    }

    pub(super) fn finish(mut self) -> Result<AdaptiveCommitFinish, DurableNamespaceError> {
        // Unconsumed Residue Spans cover bytes this commit never planned: a
        // Chunk anchored in the retained SeqCDC suffix can lie entirely below
        // the first changed Range, and those bytes are already committed at
        // their Offsets. Their seeded buffer entries are deduplicated against
        // the active Exact Index by push, so dropping the Spans is safe.
        self.residue_spans.clear();
        self.flush()?;
        Ok((self.level_zero_entries, self.metrics, self.retained_ranges))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the bounded writer flush keeps encoding, publication, and cache handoff ordered"
    )]
    fn flush(&mut self) -> Result<(), DurableNamespaceError> {
        if self.chunks.is_empty() {
            return Ok(());
        }
        let id = random_container_id()?;
        let publication_guard = (self.advanced && self.index.advanced_reduction_available())
            .then(|| self.containers.try_pin_data_reference())
            .flatten();
        let advanced = publication_guard.is_some();
        let mut regions = Vec::<Vec<PrehashedChunk<'_>>>::new();
        let mut independent = Vec::new();
        let mut dependents = Vec::new();
        let mut similarities = Vec::new();
        let mut region_bytes = 0_usize;
        for (chunk, &chunk_id) in self.chunks.iter().zip(&self.chunk_ids) {
            if advanced {
                let (plan, hint) =
                    self.index
                        .plan_similarity_chunk(self.containers, chunk_id, chunk);
                if let Some(entry) = hint {
                    similarities.push(entry);
                }
                match plan {
                    PersistentChunkPlan::NoCandidates => (),
                    PersistentChunkPlan::Independent(record) => {
                        independent.push(record);
                        region_bytes = 0;
                        continue;
                    }
                    PersistentChunkPlan::Dependent(record) => {
                        dependents.push(record);
                        region_bytes = 0;
                        continue;
                    }
                }
            }
            let next_region_bytes = region_bytes
                .checked_add(chunk.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
            if !regions.last().is_none_or(Vec::is_empty)
                && next_region_bytes > COMPRESSION_REGION_TARGET_BYTES
            {
                region_bytes = 0;
            }
            if region_bytes == 0 {
                regions
                    .try_reserve(1)
                    .map_err(|_| DurableNamespaceError::OutOfMemory)?;
                regions.push(Vec::new());
            }
            let region = regions
                .last_mut()
                .expect("ASSERT: a zero region cursor must create one region");
            region
                .try_reserve(1)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            region.push(PrehashedChunk::new(chunk_id, chunk));
            region_bytes = region_bytes
                .checked_add(chunk.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
            assert!(
                region_bytes <= COMPRESSION_REGION_TARGET_BYTES,
                "ASSERT: no logical Chunk may exceed a Compression Region"
            );
        }
        let region_refs = regions
            .iter()
            .map(|r| PrehashedAdaptiveRegion::Borrowed(r))
            .collect::<Vec<_>>();
        let generation = self.container_generations.reserve_generation()?;
        let prepared = ContainerRepository::<C>::prepare_mixed_prehashed_reduction_parallel(
            id,
            generation,
            &region_refs,
            independent,
            dependents,
            self.workers,
            Some(&self.chunk_ids),
        )?;
        let (verified, publish_metrics) = self
            .containers
            .publish_prepared_adaptive_profiled_with_placement(prepared, self.placement)?;
        self.record_container_metrics(publish_metrics);
        let mut published_entries = Vec::new();
        published_entries
            .try_reserve(verified.locations().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for location in verified.locations().iter().copied() {
            let entry = ExactIndexEntry::from_verified(location).expect(
                "ASSERT: fully verified Container evidence must form a valid Exact-Index Location",
            );
            self.online_dependency_proofs
                .remember_frozen(entry, OnlineProofAdmission::Published);
            published_entries.push(entry);
        }
        self.index
            .publish_reduction_batch(published_entries, similarities, publication_guard);
        if self.advanced {
            for (&chunk_id, bytes) in self.chunk_ids.iter().zip(&self.chunks) {
                self.index
                    .read_cache()
                    .admit_writer_chunk(chunk_id, &[bytes]);
            }
        }
        self.chunks.clear();
        self.chunk_ids.clear();
        self.payload_bytes = 0;
        Ok(())
    }

    fn record_container_metrics(&mut self, published: AdaptiveContainerPublishMetrics) {
        self.metrics
            .compression_encode
            .add(published.encode_wall(), published.encode_process_cpu());
        self.metrics
            .container_publish
            .add(published.publish_wall(), published.publish_process_cpu());
        self.metrics.container_file_bytes = self
            .metrics
            .container_file_bytes
            .checked_add(published.file_bytes())
            .expect("ASSERT: checkpoint Container file bytes cannot overflow u64");
        self.metrics.raw_records = self
            .metrics
            .raw_records
            .checked_add(
                u64::try_from(published.raw_records())
                    .expect("ASSERT: bounded RAW Record count fits u64"),
            )
            .expect("ASSERT: checkpoint RAW Record count cannot overflow u64");
        self.metrics.zstd_records = self
            .metrics
            .zstd_records
            .checked_add(
                u64::try_from(published.zstd_records())
                    .expect("ASSERT: bounded Zstd Record count fits u64"),
            )
            .expect("ASSERT: checkpoint Zstd Record count cannot overflow u64");
        self.metrics
            .incompressibility_gate
            .checked_merge(published.incompressibility_gate())
            .expect("ASSERT: bounded gate metrics cannot overflow within one checkpoint");
        self.metrics.containers = self
            .metrics
            .containers
            .checked_add(1)
            .expect("ASSERT: checkpoint Container count cannot overflow u64");
        assert_eq!(
            published.logical_bytes(),
            self.chunks.iter().fold(0_u64, |total, chunk| {
                total
                    .checked_add(
                        u64::try_from(chunk.len()).expect("ASSERT: bounded Chunk length fits u64"),
                    )
                    .expect("ASSERT: bounded Container logical bytes cannot overflow")
            }),
            "ASSERT: profiled Container logical bytes must equal buffered Chunks"
        );
    }
}

/// Shared kernel entropy source for Container identities.
///
/// Sealing a Container is a hot-path event, so the source is opened once for the
/// process instead of once per identity. A lost race merely opens the device
/// twice and keeps the first handle.
fn container_entropy() -> Result<&'static Mutex<File>, DurableNamespaceError> {
    static ENTROPY: OnceLock<Mutex<File>> = OnceLock::new();
    if let Some(entropy) = ENTROPY.get() {
        return Ok(entropy);
    }
    let opened = File::open("/dev/urandom")?;
    Ok(ENTROPY.get_or_init(|| Mutex::new(opened)))
}

pub(super) fn random_container_id() -> Result<ContainerId, DurableNamespaceError> {
    let mut random = container_entropy()?
        .lock()
        .expect("ASSERT: Container entropy lock poisoned");
    loop {
        let mut bytes = [0_u8; 16];
        random.read_exact(&mut bytes)?;
        if bytes != [0; 16] {
            return ContainerId::new(bytes).map_err(|_| DurableNamespaceError::FrozenViewMismatch);
        }
    }
}

#[cfg(test)]
mod rechunk_intent_tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct RecordingSource {
        data: Vec<u8>,
        intent: Mutex<Option<ReadIntent>>,
    }

    impl FrozenCommitRangeSource for RecordingSource {
        fn read_frozen_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
            *self
                .intent
                .lock()
                .expect("ASSERT: recording source intent lock poisoned") =
                Some(ReadIntentScope::current());
            let start = usize::try_from(offset).expect("ASSERT: test source offset fits usize");
            let length = usize::try_from(length).expect("ASSERT: test source length fits usize");
            Ok(self.data[start..start + length].to_vec())
        }
    }

    #[test]
    fn checkpoint_rechunk_reads_enter_scan_intent() {
        let source = RecordingSource {
            data: vec![0xA5; CDC_MAXIMUM_BYTES + 1],
            intent: Mutex::new(None),
        };
        let reader = CommitRangeReader::new(&source, 0, (CDC_MAXIMUM_BYTES + 1) as u64);
        let mut stream = SeqCdcStream::new(reader).expect("construct SeqCDC test stream");

        let chunk = read_next_rechunk_chunk(&mut stream)
            .expect("read first SeqCDC Chunk")
            .expect("nonempty test range must yield a Chunk");

        assert!(!chunk.is_empty());
        assert_eq!(
            *source
                .intent
                .lock()
                .expect("ASSERT: recording source intent lock poisoned"),
            Some(ReadIntent::Scan)
        );
        assert_eq!(ReadIntentScope::current(), ReadIntent::Demand);
    }
}
