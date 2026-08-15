use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

use fastdup_format::{
    ChunkId, CommitRecord, ContainerId, DurableInode, MAX_LOGICAL_CHUNK_BYTES, ManifestExtent,
    ManifestLeaf, MetadataFormatError, NamespaceEntry, NamespaceRoot,
};
use fastdup_posix::{
    CommitInode, CommitRange, CommittedFile, CommittedFileInstall, Namespace, NamespaceCommit,
    NamespaceConfig, PosixError,
};
use fastdup_store::{
    ContainerRepository, GenerationError, GenerationRepository, ManifestReadError, StorageIo,
    StoreError,
};

use crate::{ManifestCommittedFile, MountError, namespace_from_root};

const FIRST_REGULAR_INODE: u64 = 2;
const CONTAINER_PAYLOAD_TARGET_BYTES: usize = 32 * 1_024 * 1_024;

/// Writable namespace plus the durable generation machinery behind it.
///
/// The module owns the only checkpoint serialization lock. POSIX callers use
/// [`Self::namespace`] and do not need to know about manifests, containers, or
/// Commit Records.
#[derive(Debug)]
pub struct DurableNamespace<M, C> {
    namespace: Arc<Namespace>,
    generations: GenerationRepository<M>,
    containers: ContainerRepository<C>,
    checkpoint_lock: Mutex<()>,
    manifests: Mutex<Vec<(u64, ManifestLeaf)>>,
    next_container_generation: Mutex<u64>,
}

impl<M, C> DurableNamespace<M, C>
where
    M: StorageIo,
    C: Clone + Send + Sync + StorageIo + 'static,
{
    /// Recovers the newest generation, durably reserves a fresh Inode ID
    /// range, and only then enables mutation admission.
    ///
    /// A new repository first publishes its initial reservation generation.
    /// Reopening an existing repository deliberately skips every unused ID in
    /// the prior reservation before publishing a new range.
    ///
    /// # Errors
    ///
    /// Returns recovery, reservation, graph verification, durability, or
    /// namespace-construction failures. A zero or overflowing reservation span
    /// is rejected before mutation admission.
    pub fn open(
        config: NamespaceConfig,
        generations: GenerationRepository<M>,
        containers: ContainerRepository<C>,
        inode_reservation_span: u64,
    ) -> Result<Self, DurableNamespaceError> {
        if inode_reservation_span == 0 {
            return Err(DurableNamespaceError::InvalidReservationSpan);
        }
        let recovered = generations.recover_latest_with_data(&containers)?;
        let next_container_generation = discover_next_container_generation(&containers)?;
        let (root, next_inode, reservation_end) = match recovered {
            None => {
                let reservation_end = FIRST_REGULAR_INODE
                    .checked_add(inode_reservation_span)
                    .ok_or(DurableNamespaceError::InodeReservationExhausted)?;
                let root = NamespaceRoot::new(
                    reservation_end,
                    FIRST_REGULAR_INODE,
                    0,
                    Vec::new(),
                    Vec::new(),
                )?;
                generations.commit_namespace(&root)?;
                (root, FIRST_REGULAR_INODE, reservation_end)
            }
            Some(recovered) => {
                let previous = recovered.namespace_root();
                let next_inode = recovered.inode_reservation_end_high_water();
                let reservation_end = next_inode
                    .checked_add(inode_reservation_span)
                    .ok_or(DurableNamespaceError::InodeReservationExhausted)?;
                let root = NamespaceRoot::new(
                    reservation_end,
                    next_inode,
                    previous.namespace_mutation_sequence(),
                    previous.inodes().to_vec(),
                    previous.entries().to_vec(),
                )?;
                generations.commit_namespace_with_data(&root, &containers)?;
                (root, next_inode, reservation_end)
            }
        };
        let manifests = load_manifest_cache(&root, &generations)?;
        let namespace = namespace_from_root(
            config,
            &root,
            next_inode,
            reservation_end,
            &generations,
            &containers,
            true,
        )?;
        Ok(Self {
            namespace: Arc::new(namespace),
            generations,
            containers,
            checkpoint_lock: Mutex::new(()),
            manifests: Mutex::new(manifests),
            next_container_generation: Mutex::new(next_container_generation),
        })
    }

    #[must_use]
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    #[must_use]
    pub fn namespace_arc(&self) -> Arc<Namespace> {
        Arc::clone(&self.namespace)
    }

    /// Durably commits one complete prefix of accepted mutations.
    ///
    /// DATA containers are sealed and synchronized first, immutable manifests
    /// and the Namespace Root follow, and the Commit WAL is synchronized last.
    /// A failed call leaves the same frozen cut available for retry while later
    /// writes remain live in the next epoch.
    ///
    /// # Errors
    ///
    /// Returns frozen-view, format, container, metadata, graph, or durability
    /// failures. `Ok(None)` means no accepted mutation is waiting for a commit.
    ///
    /// # Panics
    ///
    /// Panics when a prior impossible invariant poisoned the single checkpoint
    /// lock or when a verified installed view disagrees with its own cut.
    pub fn checkpoint(&self) -> Result<Option<CommitRecord>, DurableNamespaceError> {
        let _guard = self
            .checkpoint_lock
            .lock()
            .expect("ASSERT: durable namespace checkpoint lock poisoned");
        let Some(commit) = self.namespace.begin_commit()? else {
            return Ok(None);
        };
        let mut next_container_generation = self
            .next_container_generation
            .lock()
            .expect("ASSERT: Container generation allocator lock poisoned");
        let mut writer = RawCommitWriter::new(&self.containers, &mut next_container_generation);
        let mut manifests = Vec::new();
        manifests
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let installed_manifests = self
            .manifests
            .lock()
            .expect("ASSERT: installed Manifest cache lock poisoned");
        for inode in commit.inodes() {
            let previous = installed_manifests
                .binary_search_by_key(&inode.inode().get(), |(inode, _)| *inode)
                .ok()
                .map(|index| &installed_manifests[index].1);
            manifests.push(plan_manifest(inode, previous, &mut writer)?);
        }
        drop(installed_manifests);
        writer.finish()?;
        drop(next_container_generation);
        self.publish_generation(&commit, manifests).map(Some)
    }

    fn publish_generation(
        &self,
        commit: &NamespaceCommit,
        manifests: Vec<ManifestLeaf>,
    ) -> Result<CommitRecord, DurableNamespaceError> {
        if manifests.len() != commit.inodes().len() {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let mut durable_inodes = Vec::new();
        let mut installs = Vec::new();
        let mut next_manifests = Vec::new();
        durable_inodes
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        installs
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        next_manifests
            .try_reserve_exact(commit.inodes().len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (inode, manifest) in commit.inodes().iter().zip(manifests) {
            let manifest_root = self.generations.publish_manifest(&manifest)?;
            durable_inodes.push(DurableInode::new(
                inode.inode().get(),
                inode.mode(),
                inode.uid(),
                inode.gid(),
                inode.link_count(),
                inode.mutation_sequence(),
                inode.logical_size(),
                manifest_root,
            )?);
            next_manifests.push((inode.inode().get(), manifest));
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
        let root = NamespaceRoot::new(
            commit.inode_reservation_end(),
            commit.inode_allocation_cursor(),
            commit.namespace_mutation_sequence(),
            durable_inodes,
            entries,
        )?;
        let committed = self
            .generations
            .commit_namespace_with_verified_files(&root, &self.containers)?;
        let (record, verified_files) = committed.into_parts();
        assert_eq!(
            verified_files.len(),
            commit.inodes().len(),
            "ASSERT: committed DATA proof must cover every frozen inode"
        );
        for ((inode, verified), (_, planned_manifest)) in commit
            .inodes()
            .iter()
            .zip(verified_files)
            .zip(&next_manifests)
        {
            assert_eq!(
                verified.inode(),
                inode.inode().get(),
                "ASSERT: committed DATA proof order must match the Namespace Root"
            );
            assert_eq!(
                verified.manifest(),
                planned_manifest,
                "ASSERT: published Manifest reread must equal the planned Manifest"
            );
            let installed = Arc::new(ManifestCommittedFile::from_verified(verified.into_file()))
                as Arc<dyn CommittedFile>;
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
        Ok(record)
    }
}

fn discover_next_container_generation<I: StorageIo>(
    containers: &ContainerRepository<I>,
) -> Result<u64, DurableNamespaceError> {
    containers
        .verify_published()?
        .into_iter()
        .map(fastdup_store::PublishedContainerSummary::container_generation)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(DurableNamespaceError::ContainerGenerationExhausted)
}

fn load_manifest_cache<I: StorageIo>(
    root: &NamespaceRoot,
    generations: &GenerationRepository<I>,
) -> Result<Vec<(u64, ManifestLeaf)>, DurableNamespaceError> {
    let mut manifests = Vec::new();
    manifests
        .try_reserve_exact(root.inodes().len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for inode in root.inodes() {
        let manifest = generations.read_manifest(inode.manifest_root())?;
        if manifest.file_length() != inode.logical_size() {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if let Some((previous, _)) = manifests.last()
            && *previous >= inode.inode()
        {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        manifests.push((inode.inode(), manifest));
    }
    Ok(manifests)
}

fn plan_manifest<C: StorageIo>(
    inode: &CommitInode,
    previous: Option<&ManifestLeaf>,
    writer: &mut RawCommitWriter<'_, C>,
) -> Result<ManifestLeaf, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    if logical_size == 0 {
        if inode.allocated_bytes() != 0 {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        return ManifestLeaf::new(0, Vec::new()).map_err(Into::into);
    }
    let changed = inode.changed_ranges()?;
    if let Some(previous) = previous
        && previous.file_length() == logical_size
    {
        if changed.is_empty() {
            verify_manifest_allocation(previous, inode)?;
            return Ok(previous.clone());
        }
        return plan_incremental_manifest(inode, previous, &changed, writer);
    }
    plan_full_manifest(inode, writer)
}

fn plan_full_manifest<C: StorageIo>(
    inode: &CommitInode,
    writer: &mut RawCommitWriter<'_, C>,
) -> Result<ManifestLeaf, DurableNamespaceError> {
    let logical_size = inode.logical_size();
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    stack.push((0_u64, logical_size));
    let mut extents = Vec::new();
    plan_manifest_ranges(inode, writer, &mut stack, &mut extents)?;
    let manifest = ManifestLeaf::new(logical_size, extents)?;
    verify_manifest_allocation(&manifest, inode)?;
    Ok(manifest)
}

#[derive(Clone, Copy, Debug)]
struct RewriteRange {
    start: u64,
    end: u64,
}

#[derive(Clone, Copy)]
struct LocatedExtent<'a> {
    start: u64,
    end: u64,
    extent: &'a ManifestExtent,
}

fn plan_incremental_manifest<C: StorageIo>(
    inode: &CommitInode,
    previous: &ManifestLeaf,
    changed: &[CommitRange],
    writer: &mut RawCommitWriter<'_, C>,
) -> Result<ManifestLeaf, DurableNamespaceError> {
    assert_eq!(
        previous.file_length(),
        inode.logical_size(),
        "ASSERT: incremental planning requires a size-stable file"
    );
    assert!(
        !changed.is_empty(),
        "ASSERT: incremental planning requires one changed range"
    );
    let located = locate_extents(previous)?;
    let mut rewrites = rewrite_ranges(changed, inode.logical_size())?;
    expand_rewrites_over_data(&mut rewrites, &located)?;
    coalesce_rewrites(&mut rewrites);

    let capacity = previous
        .extents()
        .len()
        .checked_add(rewrites.len())
        .ok_or(DurableNamespaceError::OutOfMemory)?;
    let mut extents = Vec::new();
    extents
        .try_reserve_exact(capacity)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut cursor = 0_u64;
    let mut stack = Vec::new();
    stack
        .try_reserve_exact(128)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for rewrite in rewrites {
        if rewrite.start < cursor || rewrite.end > inode.logical_size() {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        preserve_range(&located, cursor, rewrite.start, &mut extents)?;
        stack.push((rewrite.start, rewrite.end - rewrite.start));
        plan_manifest_ranges(inode, writer, &mut stack, &mut extents)?;
        assert!(
            stack.is_empty(),
            "ASSERT: range planner must consume its complete work stack"
        );
        cursor = rewrite.end;
    }
    preserve_range(&located, cursor, inode.logical_size(), &mut extents)?;
    let manifest = ManifestLeaf::new(inode.logical_size(), extents)?;
    verify_manifest_allocation(&manifest, inode)?;
    Ok(manifest)
}

fn locate_extents(
    manifest: &ManifestLeaf,
) -> Result<Vec<LocatedExtent<'_>>, DurableNamespaceError> {
    let mut located = Vec::new();
    located
        .try_reserve_exact(manifest.extents().len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut start = 0_u64;
    for extent in manifest.extents() {
        let end = start
            .checked_add(extent_length(extent))
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        located.push(LocatedExtent { start, end, extent });
        start = end;
    }
    if start != manifest.file_length() {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(located)
}

fn rewrite_ranges(
    changed: &[CommitRange],
    logical_size: u64,
) -> Result<Vec<RewriteRange>, DurableNamespaceError> {
    let cell = MAX_LOGICAL_CHUNK_BYTES as u64;
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
        let start = range.offset() / cell * cell;
        let remainder = raw_end % cell;
        let end = if remainder == 0 {
            raw_end
        } else {
            raw_end
                .checked_add(cell - remainder)
                .unwrap_or(logical_size)
        }
        .min(logical_size);
        if start >= end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        rewrites.push(RewriteRange { start, end });
        previous_end = raw_end;
    }
    coalesce_rewrites(&mut rewrites);
    Ok(rewrites)
}

fn expand_rewrites_over_data(
    rewrites: &mut [RewriteRange],
    located: &[LocatedExtent<'_>],
) -> Result<(), DurableNamespaceError> {
    for rewrite in rewrites {
        let first = located.partition_point(|extent| extent.end <= rewrite.start);
        for extent in &located[first..] {
            if extent.start >= rewrite.end {
                break;
            }
            if extent.end <= rewrite.start {
                continue;
            }
            if matches!(extent.extent, ManifestExtent::Data { .. }) {
                rewrite.start = rewrite.start.min(extent.start);
                rewrite.end = rewrite.end.max(extent.end);
            }
        }
        if rewrite.start >= rewrite.end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
    }
    Ok(())
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

fn preserve_range(
    located: &[LocatedExtent<'_>],
    start: u64,
    end: u64,
    output: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    if start == end {
        return Ok(());
    }
    if start > end {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    let first = located.partition_point(|extent| extent.end <= start);
    let mut cursor = start;
    for located_extent in &located[first..] {
        if located_extent.start >= end {
            break;
        }
        let overlap_start = located_extent.start.max(start);
        let overlap_end = located_extent.end.min(end);
        if overlap_start != cursor || overlap_start >= overlap_end {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let logical_length = overlap_end - overlap_start;
        let extent = match located_extent.extent {
            ManifestExtent::Data {
                logical_length: original_length,
                chunk_id,
            } => {
                if overlap_start != located_extent.start
                    || overlap_end != located_extent.end
                    || logical_length != *original_length
                {
                    return Err(DurableNamespaceError::FrozenViewMismatch);
                }
                ManifestExtent::Data {
                    logical_length,
                    chunk_id: *chunk_id,
                }
            }
            ManifestExtent::Hole { .. } => ManifestExtent::Hole { logical_length },
            ManifestExtent::Fill { value, .. } => ManifestExtent::Fill {
                logical_length,
                value: *value,
            },
        };
        push_extent(output, extent)?;
        cursor = overlap_end;
    }
    if cursor != end {
        return Err(DurableNamespaceError::FrozenViewMismatch);
    }
    Ok(())
}

const fn extent_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
    }
}

fn plan_manifest_ranges<C: StorageIo>(
    inode: &CommitInode,
    writer: &mut RawCommitWriter<'_, C>,
    stack: &mut Vec<(u64, u64)>,
    extents: &mut Vec<ManifestExtent>,
) -> Result<(), DurableNamespaceError> {
    while let Some((offset, length)) = stack.pop() {
        assert!(
            length > 0,
            "ASSERT: manifest planner range must be nonempty"
        );
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
        if allocated == length && length <= MAX_LOGICAL_CHUNK_BYTES as u64 {
            let read_length =
                u32::try_from(length).expect("ASSERT: format-v1 logical chunk length fits u32");
            let bytes = inode.read_at(offset, read_length)?;
            if u64::try_from(bytes.len()) != Ok(length) {
                return Err(DurableNamespaceError::FrozenViewMismatch);
            }
            if bytes.iter().all(|byte| *byte == bytes[0]) {
                push_extent(
                    extents,
                    ManifestExtent::Fill {
                        logical_length: length,
                        value: bytes[0],
                    },
                )?;
            } else {
                let chunk_id = ChunkId::of(&bytes);
                writer.push(chunk_id, bytes)?;
                push_extent(
                    extents,
                    ManifestExtent::Data {
                        logical_length: length,
                        chunk_id,
                    },
                )?;
            }
            continue;
        }

        let left_length = if allocated == length {
            (MAX_LOGICAL_CHUNK_BYTES as u64).min(length)
        } else {
            length / 2
        };
        if left_length == 0 || left_length == length {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        let right_offset = offset
            .checked_add(left_length)
            .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
        stack.push((right_offset, length - left_length));
        stack.push((offset, left_length));
    }
    Ok(())
}

fn verify_manifest_allocation(
    manifest: &ManifestLeaf,
    inode: &CommitInode,
) -> Result<(), DurableNamespaceError> {
    let planned_allocated = manifest.extents().iter().try_fold(0_u64, |total, extent| {
        let length = match extent {
            ManifestExtent::Data { logical_length, .. }
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

struct RawCommitWriter<'a, C> {
    containers: &'a ContainerRepository<C>,
    next_generation: &'a mut u64,
    seen: BTreeMap<ChunkId, u64>,
    chunks: Vec<Vec<u8>>,
    payload_bytes: usize,
}

impl<'a, C: StorageIo> RawCommitWriter<'a, C> {
    fn new(containers: &'a ContainerRepository<C>, next_generation: &'a mut u64) -> Self {
        Self {
            containers,
            next_generation,
            seen: BTreeMap::new(),
            chunks: Vec::new(),
            payload_bytes: 0,
        }
    }

    fn push(&mut self, chunk_id: ChunkId, bytes: Vec<u8>) -> Result<(), DurableNamespaceError> {
        let length =
            u64::try_from(bytes.len()).map_err(|_| DurableNamespaceError::FrozenViewMismatch)?;
        if let Some(previous) = self.seen.insert(chunk_id, length) {
            if previous != length {
                return Err(DurableNamespaceError::ChunkLengthConflict {
                    chunk_id,
                    first_length: previous,
                    second_length: length,
                });
            }
            return Ok(());
        }
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
        Ok(())
    }

    fn finish(mut self) -> Result<(), DurableNamespaceError> {
        self.flush()
    }

    fn flush(&mut self) -> Result<(), DurableNamespaceError> {
        if self.chunks.is_empty() {
            return Ok(());
        }
        let id = random_container_id()?;
        let chunks = self.chunks.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let generation = *self.next_generation;
        *self.next_generation = generation
            .checked_add(1)
            .ok_or(DurableNamespaceError::ContainerGenerationExhausted)?;
        self.containers.publish_raw(id, generation, &chunks)?;
        self.chunks.clear();
        self.payload_bytes = 0;
        Ok(())
    }
}

fn random_container_id() -> Result<ContainerId, DurableNamespaceError> {
    let mut random = File::open("/dev/urandom")?;
    loop {
        let mut bytes = [0_u8; 16];
        random.read_exact(&mut bytes)?;
        if bytes != [0; 16] {
            return ContainerId::new(bytes).map_err(|_| DurableNamespaceError::FrozenViewMismatch);
        }
    }
}

#[derive(Debug)]
pub enum DurableNamespaceError {
    Io(io::Error),
    Posix(PosixError),
    Metadata(MetadataFormatError),
    Store(StoreError),
    Generation(GenerationError),
    Manifest(ManifestReadError),
    Mount(MountError),
    InvalidReservationSpan,
    InodeReservationExhausted,
    ContainerGenerationExhausted,
    OutOfMemory,
    FrozenViewMismatch,
    ChunkLengthConflict {
        chunk_id: ChunkId,
        first_length: u64,
        second_length: u64,
    },
}

impl fmt::Display for DurableNamespaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DurableNamespaceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Store(error) => Some(error),
            Self::Generation(error) => Some(error),
            Self::Manifest(error) => Some(error),
            Self::Mount(error) => Some(error),
            Self::Posix(_)
            | Self::InvalidReservationSpan
            | Self::InodeReservationExhausted
            | Self::ContainerGenerationExhausted
            | Self::OutOfMemory
            | Self::FrozenViewMismatch
            | Self::ChunkLengthConflict { .. } => None,
        }
    }
}

impl From<io::Error> for DurableNamespaceError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<PosixError> for DurableNamespaceError {
    fn from(error: PosixError) -> Self {
        Self::Posix(error)
    }
}

impl From<MetadataFormatError> for DurableNamespaceError {
    fn from(error: MetadataFormatError) -> Self {
        Self::Metadata(error)
    }
}

impl From<StoreError> for DurableNamespaceError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<GenerationError> for DurableNamespaceError {
    fn from(error: GenerationError) -> Self {
        Self::Generation(error)
    }
}

impl From<ManifestReadError> for DurableNamespaceError {
    fn from(error: ManifestReadError) -> Self {
        Self::Manifest(error)
    }
}

impl From<MountError> for DurableNamespaceError {
    fn from(error: MountError) -> Self {
        Self::Mount(error)
    }
}
