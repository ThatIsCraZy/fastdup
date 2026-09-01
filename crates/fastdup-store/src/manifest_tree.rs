use fastdup_format::{
    MANIFEST_HEADER_BYTES, MAX_METADATA_OBJECT_BYTES, ManifestChildRange, ManifestExtent,
    ManifestInnerNode, ManifestInnerNodeError, ManifestLeaf, MetadataFormatError, MetadataObjectId,
    MetadataObjectKind, metadata_object_kind,
};
use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::ops::Range;

const LEAF_TARGET_LOGICAL_BYTES: u64 = 64 * 1_024 * 1_024;
const MAX_LEAF_EXTENTS: usize = 1_024;
const MAX_INNER_CHILDREN: usize = 1_024;
const MAX_TREE_LEVEL: u16 = 16;
const MAX_FLATTENED_EXTENTS: usize =
    (MAX_METADATA_OBJECT_BYTES - fastdup_format::METADATA_HEADER_BYTES - MANIFEST_HEADER_BYTES)
        / 64;
const MAX_FLATTENED_NODES: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Opaque identity and verified scalar totals for one immutable Manifest tree.
///
/// Callers can retain and present this capability, but only the store can
/// construct it after publishing or completely verifying the referenced tree.
pub struct ManifestTreeSummary {
    root: MetadataObjectId,
    logical_size: u64,
    allocated_bytes: u64,
}

impl ManifestTreeSummary {
    pub(crate) const fn new(
        root: MetadataObjectId,
        logical_size: u64,
        allocated_bytes: u64,
    ) -> Self {
        Self {
            root,
            logical_size,
            allocated_bytes,
        }
    }

    #[must_use]
    pub const fn root(self) -> MetadataObjectId {
        self.root
    }

    #[must_use]
    pub const fn logical_size(self) -> u64 {
        self.logical_size
    }

    #[must_use]
    pub const fn allocated_bytes(self) -> u64 {
        self.allocated_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestRangeExtent {
    logical_offset: u64,
    extent: ManifestExtent,
}

impl ManifestRangeExtent {
    pub(crate) const fn new(logical_offset: u64, extent: ManifestExtent) -> Self {
        Self {
            logical_offset,
            extent,
        }
    }

    #[must_use]
    pub const fn logical_offset(&self) -> u64 {
        self.logical_offset
    }

    #[must_use]
    pub const fn extent(&self) -> &ManifestExtent {
        &self.extent
    }
}

#[derive(Debug)]
pub(crate) struct EncodedManifestTree {
    root: MetadataObjectId,
    objects: Vec<(MetadataObjectId, Vec<u8>)>,
}

impl EncodedManifestTree {
    pub(crate) fn root(&self) -> MetadataObjectId {
        self.root
    }

    pub(crate) fn objects(&self) -> &[(MetadataObjectId, Vec<u8>)] {
        &self.objects
    }
}

#[derive(Clone, Copy)]
struct NodeRef {
    logical_length: u64,
    allocated_bytes: u64,
    object_id: MetadataObjectId,
}

pub(crate) fn encode_manifest_tree(
    manifest: &ManifestLeaf,
) -> Result<EncodedManifestTree, ManifestTreeError> {
    let mut objects = Vec::new();
    let mut seen = BTreeSet::new();
    let mut current = encode_leaves(manifest, &mut objects, &mut seen)?;
    let mut level = 1_u16;
    while current.len() > 1 {
        if level > MAX_TREE_LEVEL {
            return Err(ManifestTreeError::TreeTooDeep);
        }
        let parent_count = current.len().div_ceil(MAX_INNER_CHILDREN);
        let mut parents = Vec::new();
        parents
            .try_reserve_exact(parent_count)
            .map_err(|_| ManifestTreeError::OutOfMemory)?;
        for children in current.chunks(MAX_INNER_CHILDREN) {
            let mut child_ranges = Vec::new();
            child_ranges
                .try_reserve_exact(children.len())
                .map_err(|_| ManifestTreeError::OutOfMemory)?;
            let mut offset = 0_u64;
            for child in children {
                child_ranges.push(ManifestChildRange::new_with_allocated_bytes(
                    offset,
                    child.logical_length,
                    child.allocated_bytes,
                    child.object_id,
                )?);
                offset = offset
                    .checked_add(child.logical_length)
                    .ok_or(ManifestTreeError::ArithmeticOverflow)?;
            }
            let encoded = ManifestInnerNode::new(offset, level, child_ranges)?.encode()?;
            let object_id = MetadataObjectId::from_encoded(&encoded)?;
            remember_object(object_id, encoded, &mut objects, &mut seen)?;
            parents.push(NodeRef {
                logical_length: offset,
                allocated_bytes: children.iter().try_fold(0_u64, |total, child| {
                    total
                        .checked_add(child.allocated_bytes)
                        .ok_or(ManifestTreeError::ArithmeticOverflow)
                })?,
                object_id,
            });
        }
        current = parents;
        level = level.checked_add(1).ok_or(ManifestTreeError::TreeTooDeep)?;
    }
    let root = current
        .first()
        .ok_or(ManifestTreeError::InvalidTree)?
        .object_id;
    Ok(EncodedManifestTree { root, objects })
}

pub(crate) fn rewrite_manifest_tree_range<F>(
    root: MetadataObjectId,
    expected_logical_size: u64,
    replaced: Range<u64>,
    replacement: &[ManifestExtent],
    mut read: F,
) -> Result<EncodedManifestTree, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    if replaced.start > replaced.end || replaced.end > expected_logical_size {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    let replaced_length = replaced.end - replaced.start;
    let replacement_length = replacement.iter().try_fold(0_u64, |total, extent| {
        total
            .checked_add(extent_length(extent))
            .ok_or(ManifestTreeError::ArithmeticOverflow)
    })?;
    if replaced_length != replacement_length {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    let mut verified_replacement = Vec::new();
    verified_replacement
        .try_reserve_exact(replacement.len())
        .map_err(|_| ManifestTreeError::OutOfMemory)?;
    verified_replacement.extend_from_slice(replacement);
    ManifestLeaf::new(replacement_length, verified_replacement)?;
    if replaced.is_empty() {
        return Ok(EncodedManifestTree {
            root,
            objects: Vec::new(),
        });
    }

    let mut objects = Vec::new();
    let mut seen = BTreeSet::new();
    let rewritten = rewrite_node(
        PendingNode {
            object_id: root,
            absolute_offset: 0,
            expected_length: Some(expected_logical_size),
            expected_level: None,
            expected_allocated_bytes: None,
        },
        &replaced,
        replacement,
        &mut read,
        &mut objects,
        &mut seen,
    )?;
    if rewritten.logical_length != expected_logical_size {
        return Err(ManifestTreeError::InvalidTree);
    }
    Ok(EncodedManifestTree {
        root: rewritten.object_id,
        objects,
    })
}

pub(crate) fn rewrite_manifest_tree_range_successor<F>(
    previous: ManifestTreeSummary,
    replaced: Range<u64>,
    replacement: &[ManifestExtent],
    mut read: F,
) -> Result<(EncodedManifestTree, ManifestTreeSummary), ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    if replaced.start > replaced.end || replaced.end > previous.logical_size() {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    let touched = read_manifest_tree_range(
        previous.root(),
        previous.logical_size(),
        replaced.start,
        replaced.end - replaced.start,
        &mut read,
    )?;
    let removed_allocated = allocated_bytes_in_range(&touched, &replaced)?;
    let added_allocated = replacement.iter().try_fold(0_u64, |total, extent| {
        if matches!(extent, ManifestExtent::Hole { .. }) {
            Ok(total)
        } else {
            total
                .checked_add(extent_length(extent))
                .ok_or(ManifestTreeError::ArithmeticOverflow)
        }
    })?;
    let tree = rewrite_manifest_tree_range(
        previous.root(),
        previous.logical_size(),
        replaced,
        replacement,
        read,
    )?;
    let allocated_bytes = previous
        .allocated_bytes()
        .checked_sub(removed_allocated)
        .and_then(|remaining| remaining.checked_add(added_allocated))
        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
    let summary = ManifestTreeSummary::new(tree.root(), previous.logical_size(), allocated_bytes);
    Ok((tree, summary))
}

fn allocated_bytes_in_range(
    extents: &[ManifestRangeExtent],
    requested: &Range<u64>,
) -> Result<u64, ManifestTreeError> {
    extents.iter().try_fold(0_u64, |total, located| {
        if matches!(located.extent(), ManifestExtent::Hole { .. }) {
            return Ok(total);
        }
        let extent_end = located
            .logical_offset()
            .checked_add(extent_length(located.extent()))
            .ok_or(ManifestTreeError::ArithmeticOverflow)?;
        let overlap_start = located.logical_offset().max(requested.start);
        let overlap_end = extent_end.min(requested.end);
        let overlap = overlap_end.saturating_sub(overlap_start);
        total
            .checked_add(overlap)
            .ok_or(ManifestTreeError::ArithmeticOverflow)
    })
}

pub(crate) fn append_manifest_tree<F>(
    previous: ManifestTreeSummary,
    appended: &[ManifestExtent],
    mut read: F,
) -> Result<(EncodedManifestTree, ManifestTreeSummary), ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    let appended_length = appended.iter().try_fold(0_u64, |total, extent| {
        total
            .checked_add(extent_length(extent))
            .ok_or(ManifestTreeError::ArithmeticOverflow)
    })?;
    if appended_length == 0 {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    let appended_allocated = appended.iter().try_fold(0_u64, |total, extent| {
        let length = if matches!(extent, ManifestExtent::Hole { .. }) {
            0
        } else {
            extent_length(extent)
        };
        total
            .checked_add(length)
            .ok_or(ManifestTreeError::ArithmeticOverflow)
    })?;
    let logical_size = previous
        .logical_size()
        .checked_add(appended_length)
        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
    let allocated_bytes = previous
        .allocated_bytes()
        .checked_add(appended_allocated)
        .ok_or(ManifestTreeError::ArithmeticOverflow)?;

    let mut verified_appended = Vec::new();
    verified_appended
        .try_reserve_exact(appended.len())
        .map_err(|_| ManifestTreeError::OutOfMemory)?;
    verified_appended.extend_from_slice(appended);
    let appended_manifest = ManifestLeaf::new(appended_length, verified_appended)?;
    let mut objects = Vec::new();
    let mut seen = BTreeSet::new();
    let appended_leaves = encode_leaves(&appended_manifest, &mut objects, &mut seen)?;
    let (mut level, mut forest) = append_leaves_to_right_spine(
        PendingNode {
            object_id: previous.root(),
            absolute_offset: 0,
            expected_length: Some(previous.logical_size()),
            expected_level: None,
            expected_allocated_bytes: Some(previous.allocated_bytes()),
        },
        &appended_leaves,
        &mut read,
        &mut objects,
        &mut seen,
    )?;
    while forest.len() > 1 {
        level = level.checked_add(1).ok_or(ManifestTreeError::TreeTooDeep)?;
        if level > MAX_TREE_LEVEL {
            return Err(ManifestTreeError::TreeTooDeep);
        }
        forest = encode_parent_forest(level, &forest, &mut objects, &mut seen)?;
    }
    let root = forest
        .first()
        .ok_or(ManifestTreeError::InvalidTree)?
        .object_id;
    Ok((
        EncodedManifestTree { root, objects },
        ManifestTreeSummary::new(root, logical_size, allocated_bytes),
    ))
}

pub(crate) fn truncate_manifest_tree<F>(
    previous: ManifestTreeSummary,
    logical_size: u64,
    mut read: F,
) -> Result<(EncodedManifestTree, ManifestTreeSummary), ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    if logical_size >= previous.logical_size() {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    if logical_size == 0 {
        let empty = ManifestLeaf::new(0, Vec::new())?;
        let tree = encode_manifest_tree(&empty)?;
        return Ok((
            tree,
            ManifestTreeSummary::new(MetadataObjectId::from_encoded(&empty.encode()?)?, 0, 0),
        ));
    }

    let mut objects = Vec::new();
    let mut seen = BTreeSet::new();
    let root = truncate_node(
        PendingNode {
            object_id: previous.root(),
            absolute_offset: 0,
            expected_length: Some(previous.logical_size()),
            expected_level: None,
            expected_allocated_bytes: Some(previous.allocated_bytes()),
        },
        logical_size,
        &mut read,
        &mut objects,
        &mut seen,
    )?;
    if root.logical_length != logical_size {
        return Err(ManifestTreeError::InvalidTree);
    }
    Ok((
        EncodedManifestTree {
            root: root.object_id,
            objects,
        },
        ManifestTreeSummary::new(root.object_id, logical_size, root.allocated_bytes),
    ))
}

pub(crate) fn splice_manifest_tree<F>(
    previous: ManifestTreeSummary,
    replaced: Range<u64>,
    replacement: &[ManifestExtent],
    mut read: F,
) -> Result<(EncodedManifestTree, ManifestTreeSummary), ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    if replaced.start > replaced.end
        || replaced.end > previous.logical_size()
        || (replaced.is_empty() && replacement.is_empty())
    {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    let replacement_length = replacement.iter().try_fold(0_u64, |total, extent| {
        total
            .checked_add(extent_length(extent))
            .ok_or(ManifestTreeError::ArithmeticOverflow)
    })?;
    let replacement_allocated = manifest_allocated_bytes(replacement)?;
    let replacement_copy = copy_extents(replacement)?;
    ManifestLeaf::new(replacement_length, replacement_copy)?;
    let logical_size = previous
        .logical_size()
        .checked_sub(replaced.end - replaced.start)
        .and_then(|retained| retained.checked_add(replacement_length))
        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
    let removed_allocated = allocated_bytes_in_manifest_tree_range(
        previous.root(),
        previous.logical_size(),
        replaced.start,
        replaced.end - replaced.start,
        &mut read,
    )?;
    let allocated_bytes = previous
        .allocated_bytes()
        .checked_sub(removed_allocated)
        .and_then(|retained| retained.checked_add(replacement_allocated))
        .ok_or(ManifestTreeError::ArithmeticOverflow)?;

    if logical_size == 0 {
        let empty = ManifestLeaf::new(0, Vec::new())?;
        let tree = encode_manifest_tree(&empty)?;
        let root = tree.root();
        return Ok((tree, ManifestTreeSummary::new(root, 0, 0)));
    }

    let mut objects = Vec::new();
    let mut seen = BTreeSet::new();
    let (mut level, mut forest) = splice_node(
        PendingNode {
            object_id: previous.root(),
            absolute_offset: 0,
            expected_length: Some(previous.logical_size()),
            expected_level: None,
            expected_allocated_bytes: Some(previous.allocated_bytes()),
        },
        replaced,
        replacement,
        &mut read,
        &mut objects,
        &mut seen,
    )?;
    while forest.len() > 1 {
        level = level.checked_add(1).ok_or(ManifestTreeError::TreeTooDeep)?;
        if level > MAX_TREE_LEVEL {
            return Err(ManifestTreeError::TreeTooDeep);
        }
        forest = encode_parent_forest(level, &forest, &mut objects, &mut seen)?;
    }
    let root = *forest.first().ok_or(ManifestTreeError::InvalidTree)?;
    if root.logical_length != logical_size || root.allocated_bytes != allocated_bytes {
        return Err(ManifestTreeError::InvalidTree);
    }
    Ok((
        EncodedManifestTree {
            root: root.object_id,
            objects,
        },
        ManifestTreeSummary::new(root.object_id, logical_size, allocated_bytes),
    ))
}

#[allow(
    clippy::too_many_lines,
    reason = "keeping split, replacement insertion, and concat in one recursive COW operation makes suffix-reuse auditable"
)]
fn splice_node<F>(
    candidate: PendingNode,
    replaced: Range<u64>,
    replacement: &[ManifestExtent],
    read: &mut F,
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<(u16, Vec<NodeRef>), ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    match decode_manifest_node(candidate.object_id, read)? {
        DecodedManifestNode::Leaf(leaf) => {
            let allocated_bytes = manifest_allocated_bytes(leaf.extents())?;
            if candidate.expected_level.is_some_and(|level| level != 0)
                || candidate
                    .expected_length
                    .is_some_and(|length| length != leaf.file_length())
                || candidate
                    .expected_allocated_bytes
                    .is_some_and(|expected| expected != allocated_bytes)
                || replaced.start > replaced.end
                || replaced.end > leaf.file_length()
            {
                return Err(ManifestTreeError::InvalidTree);
            }
            let mut extents = slice_extents(leaf.extents(), 0..replaced.start)?;
            append_extents(&mut extents, copy_extents(replacement)?)?;
            append_extents(
                &mut extents,
                slice_extents(leaf.extents(), replaced.end..leaf.file_length())?,
            )?;
            let logical_length = extents.iter().try_fold(0_u64, |total, extent| {
                total
                    .checked_add(extent_length(extent))
                    .ok_or(ManifestTreeError::ArithmeticOverflow)
            })?;
            if logical_length == 0 {
                return Ok((0, Vec::new()));
            }
            let manifest = ManifestLeaf::new(logical_length, extents)?;
            Ok((0, encode_leaves(&manifest, objects, seen)?))
        }
        DecodedManifestNode::Inner(node) => {
            let node_allocated = node.allocated_bytes()?;
            if candidate
                .expected_length
                .is_some_and(|length| length != node.file_length())
                || candidate
                    .expected_level
                    .is_some_and(|level| level != node.level())
                || candidate.expected_allocated_bytes.is_some_and(|expected| {
                    node_allocated.is_none_or(|observed| observed != expected)
                })
                || node.level() == 0
                || node.level() > MAX_TREE_LEVEL
                || replaced.start > replaced.end
                || replaced.end > node.file_length()
            {
                return Err(ManifestTreeError::InvalidTree);
            }
            let child_level = node.level() - 1;
            let mut containing_child = None;
            for (ordinal, child) in node.children().iter().enumerate() {
                let child_end = child
                    .logical_offset()
                    .checked_add(child.logical_length())
                    .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                let contains = if replaced.is_empty() {
                    child.logical_offset() < replaced.start && replaced.start < child_end
                } else {
                    child.logical_offset() <= replaced.start && replaced.end <= child_end
                };
                if contains {
                    containing_child = Some(ordinal);
                    break;
                }
            }
            let mut children = Vec::new();
            children
                .try_reserve(node.children().len().saturating_add(4))
                .map_err(|_| ManifestTreeError::OutOfMemory)?;
            if let Some(target) = containing_child {
                for (ordinal, child) in node.children().iter().enumerate() {
                    if ordinal == target {
                        let local = replaced.start - child.logical_offset()
                            ..replaced.end - child.logical_offset();
                        let (observed_level, rewritten) = splice_node(
                            pending_child(candidate, child, child_level)?,
                            local,
                            replacement,
                            read,
                            objects,
                            seen,
                        )?;
                        if observed_level != child_level {
                            return Err(ManifestTreeError::InvalidTree);
                        }
                        append_node_refs(&mut children, &rewritten)?;
                    } else {
                        children.push(node_ref_from_child(child)?);
                    }
                }
            } else {
                for child in node.children() {
                    let child_end = child
                        .logical_offset()
                        .checked_add(child.logical_length())
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    if child_end <= replaced.start {
                        children.push(node_ref_from_child(child)?);
                    } else if child.logical_offset() < replaced.start {
                        let (observed_level, prefix) = splice_node(
                            pending_child(candidate, child, child_level)?,
                            replaced.start - child.logical_offset()..child.logical_length(),
                            &[],
                            read,
                            objects,
                            seen,
                        )?;
                        if observed_level != child_level {
                            return Err(ManifestTreeError::InvalidTree);
                        }
                        append_node_refs(&mut children, &prefix)?;
                    }
                }
                let replacement_forest =
                    encode_extents_at_level(replacement, child_level, objects, seen)?;
                append_node_refs(&mut children, &replacement_forest)?;
                for child in node.children() {
                    let child_end = child
                        .logical_offset()
                        .checked_add(child.logical_length())
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    if child.logical_offset() >= replaced.end {
                        children.push(node_ref_from_child(child)?);
                    } else if child_end > replaced.end {
                        let (observed_level, suffix) = splice_node(
                            pending_child(candidate, child, child_level)?,
                            0..replaced.end - child.logical_offset(),
                            &[],
                            read,
                            objects,
                            seen,
                        )?;
                        if observed_level != child_level {
                            return Err(ManifestTreeError::InvalidTree);
                        }
                        append_node_refs(&mut children, &suffix)?;
                    }
                }
            }
            if children.is_empty() {
                return Ok((node.level(), Vec::new()));
            }
            Ok((
                node.level(),
                encode_parent_forest(node.level(), &children, objects, seen)?,
            ))
        }
    }
}

fn pending_child(
    parent: PendingNode,
    child: &ManifestChildRange,
    level: u16,
) -> Result<PendingNode, ManifestTreeError> {
    Ok(PendingNode {
        object_id: child.child(),
        absolute_offset: parent
            .absolute_offset
            .checked_add(child.logical_offset())
            .ok_or(ManifestTreeError::ArithmeticOverflow)?,
        expected_length: Some(child.logical_length()),
        expected_level: Some(level),
        expected_allocated_bytes: child.allocated_bytes(),
    })
}

fn node_ref_from_child(child: &ManifestChildRange) -> Result<NodeRef, ManifestTreeError> {
    Ok(NodeRef {
        logical_length: child.logical_length(),
        allocated_bytes: child
            .allocated_bytes()
            .ok_or(ManifestTreeError::MissingSubtreeAllocation)?,
        object_id: child.child(),
    })
}

fn append_node_refs(
    target: &mut Vec<NodeRef>,
    source: &[NodeRef],
) -> Result<(), ManifestTreeError> {
    target
        .try_reserve(source.len())
        .map_err(|_| ManifestTreeError::OutOfMemory)?;
    target.extend_from_slice(source);
    Ok(())
}

fn encode_extents_at_level(
    extents: &[ManifestExtent],
    target_level: u16,
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<Vec<NodeRef>, ManifestTreeError> {
    if extents.is_empty() {
        return Ok(Vec::new());
    }
    let logical_length = extents.iter().try_fold(0_u64, |total, extent| {
        total
            .checked_add(extent_length(extent))
            .ok_or(ManifestTreeError::ArithmeticOverflow)
    })?;
    let manifest = ManifestLeaf::new(logical_length, copy_extents(extents)?)?;
    let mut forest = encode_leaves(&manifest, objects, seen)?;
    let mut level = 0_u16;
    while level < target_level {
        level = level.checked_add(1).ok_or(ManifestTreeError::TreeTooDeep)?;
        forest = encode_parent_forest(level, &forest, objects, seen)?;
    }
    Ok(forest)
}

fn copy_extents(extents: &[ManifestExtent]) -> Result<Vec<ManifestExtent>, ManifestTreeError> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(extents.len())
        .map_err(|_| ManifestTreeError::OutOfMemory)?;
    copied.extend_from_slice(extents);
    Ok(copied)
}

#[allow(
    clippy::too_many_lines,
    reason = "keeping leaf and inner cutoff handling together makes the recursive COW invariant auditable"
)]
fn truncate_node<F>(
    candidate: PendingNode,
    retained_length: u64,
    read: &mut F,
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<NodeRef, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    if retained_length == 0
        || candidate
            .expected_length
            .is_some_and(|length| retained_length >= length)
    {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    match decode_manifest_node(candidate.object_id, read)? {
        DecodedManifestNode::Leaf(leaf) => {
            let predecessor_allocated = manifest_allocated_bytes(leaf.extents())?;
            if candidate.expected_level.is_some_and(|level| level != 0)
                || candidate
                    .expected_length
                    .is_some_and(|length| length != leaf.file_length())
                || candidate
                    .expected_allocated_bytes
                    .is_some_and(|allocated| allocated != predecessor_allocated)
            {
                return Err(ManifestTreeError::InvalidTree);
            }
            let extents = slice_extents(leaf.extents(), 0..retained_length)?;
            let truncated = ManifestLeaf::new(retained_length, extents)?;
            let allocated_bytes = manifest_allocated_bytes(truncated.extents())?;
            let encoded = truncated.encode()?;
            let object_id = MetadataObjectId::from_encoded(&encoded)?;
            remember_object(object_id, encoded, objects, seen)?;
            Ok(NodeRef {
                logical_length: retained_length,
                allocated_bytes,
                object_id,
            })
        }
        DecodedManifestNode::Inner(node) => {
            let node_allocated = node.allocated_bytes()?;
            if candidate
                .expected_length
                .is_some_and(|length| length != node.file_length())
                || candidate
                    .expected_level
                    .is_some_and(|level| level != node.level())
                || candidate.expected_allocated_bytes.is_some_and(|expected| {
                    node_allocated.is_none_or(|observed| observed != expected)
                })
                || node.level() > MAX_TREE_LEVEL
            {
                return Err(ManifestTreeError::InvalidTree);
            }
            let child_level = node
                .level()
                .checked_sub(1)
                .ok_or(ManifestTreeError::InvalidTree)?;
            let mut retained = Vec::new();
            retained
                .try_reserve_exact(node.children().len())
                .map_err(|_| ManifestTreeError::OutOfMemory)?;
            for child in node.children() {
                let child_end = child
                    .logical_offset()
                    .checked_add(child.logical_length())
                    .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                if retained_length >= child_end {
                    retained.push(NodeRef {
                        logical_length: child.logical_length(),
                        allocated_bytes: child
                            .allocated_bytes()
                            .ok_or(ManifestTreeError::MissingSubtreeAllocation)?,
                        object_id: child.child(),
                    });
                    if retained_length == child_end {
                        break;
                    }
                    continue;
                }
                if retained_length > child.logical_offset() {
                    retained.push(truncate_node(
                        PendingNode {
                            object_id: child.child(),
                            absolute_offset: candidate
                                .absolute_offset
                                .checked_add(child.logical_offset())
                                .ok_or(ManifestTreeError::ArithmeticOverflow)?,
                            expected_length: Some(child.logical_length()),
                            expected_level: Some(child_level),
                            expected_allocated_bytes: child.allocated_bytes(),
                        },
                        retained_length - child.logical_offset(),
                        read,
                        objects,
                        seen,
                    )?);
                }
                break;
            }
            if retained.is_empty() {
                return Err(ManifestTreeError::InvalidTree);
            }
            let parents = encode_parent_forest(node.level(), &retained, objects, seen)?;
            if parents.len() != 1 {
                return Err(ManifestTreeError::InvalidTree);
            }
            let root = parents[0];
            if root.logical_length != retained_length {
                return Err(ManifestTreeError::InvalidTree);
            }
            Ok(root)
        }
    }
}

fn append_leaves_to_right_spine<F>(
    candidate: PendingNode,
    appended_leaves: &[NodeRef],
    read: &mut F,
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<(u16, Vec<NodeRef>), ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    match decode_manifest_node(candidate.object_id, read)? {
        DecodedManifestNode::Leaf(leaf) => {
            let leaf_allocated_bytes = manifest_allocated_bytes(leaf.extents())?;
            if candidate.expected_level.is_some_and(|level| level != 0)
                || candidate
                    .expected_length
                    .is_some_and(|length| length != leaf.file_length())
                || candidate
                    .expected_allocated_bytes
                    .is_some_and(|allocated| allocated != leaf_allocated_bytes)
            {
                return Err(ManifestTreeError::InvalidTree);
            }
            let mut leaves = Vec::new();
            leaves
                .try_reserve_exact(appended_leaves.len().saturating_add(1))
                .map_err(|_| ManifestTreeError::OutOfMemory)?;
            if leaf.file_length() != 0 {
                leaves.push(NodeRef {
                    logical_length: leaf.file_length(),
                    allocated_bytes: leaf_allocated_bytes,
                    object_id: candidate.object_id,
                });
            }
            leaves.extend_from_slice(appended_leaves);
            Ok((0, leaves))
        }
        DecodedManifestNode::Inner(node) => {
            let node_allocated_bytes = node.allocated_bytes()?;
            if candidate
                .expected_length
                .is_some_and(|length| length != node.file_length())
                || candidate
                    .expected_level
                    .is_some_and(|level| level != node.level())
                || candidate.expected_allocated_bytes.is_some_and(|expected| {
                    node_allocated_bytes.is_none_or(|observed| observed != expected)
                })
                || node.level() == 0
                || node.level() > MAX_TREE_LEVEL
            {
                return Err(ManifestTreeError::InvalidTree);
            }
            let child_level = node.level() - 1;
            let last = node
                .children()
                .last()
                .ok_or(ManifestTreeError::InvalidTree)?;
            let last_absolute_offset = candidate
                .absolute_offset
                .checked_add(last.logical_offset())
                .ok_or(ManifestTreeError::ArithmeticOverflow)?;
            let (observed_child_level, appended_children) = append_leaves_to_right_spine(
                PendingNode {
                    object_id: last.child(),
                    absolute_offset: last_absolute_offset,
                    expected_length: Some(last.logical_length()),
                    expected_level: Some(child_level),
                    expected_allocated_bytes: last.allocated_bytes(),
                },
                appended_leaves,
                read,
                objects,
                seen,
            )?;
            if observed_child_level != child_level {
                return Err(ManifestTreeError::InvalidTree);
            }
            let mut children = Vec::new();
            children
                .try_reserve_exact(
                    node.children()
                        .len()
                        .saturating_sub(1)
                        .saturating_add(appended_children.len()),
                )
                .map_err(|_| ManifestTreeError::OutOfMemory)?;
            for child in &node.children()[..node.children().len() - 1] {
                children.push(NodeRef {
                    logical_length: child.logical_length(),
                    allocated_bytes: child
                        .allocated_bytes()
                        .ok_or(ManifestTreeError::MissingSubtreeAllocation)?,
                    object_id: child.child(),
                });
            }
            children.extend_from_slice(&appended_children);
            Ok((
                node.level(),
                encode_parent_forest(node.level(), &children, objects, seen)?,
            ))
        }
    }
}

fn encode_parent_forest(
    level: u16,
    children: &[NodeRef],
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<Vec<NodeRef>, ManifestTreeError> {
    if level == 0 || level > MAX_TREE_LEVEL || children.is_empty() {
        return Err(ManifestTreeError::InvalidTree);
    }
    let mut parents = Vec::new();
    parents
        .try_reserve_exact(children.len().div_ceil(MAX_INNER_CHILDREN))
        .map_err(|_| ManifestTreeError::OutOfMemory)?;
    for group in children.chunks(MAX_INNER_CHILDREN) {
        let mut ranges = Vec::new();
        ranges
            .try_reserve_exact(group.len())
            .map_err(|_| ManifestTreeError::OutOfMemory)?;
        let mut logical_offset = 0_u64;
        for child in group {
            ranges.push(ManifestChildRange::new_with_allocated_bytes(
                logical_offset,
                child.logical_length,
                child.allocated_bytes,
                child.object_id,
            )?);
            logical_offset = logical_offset
                .checked_add(child.logical_length)
                .ok_or(ManifestTreeError::ArithmeticOverflow)?;
        }
        let encoded = ManifestInnerNode::new(logical_offset, level, ranges)?.encode()?;
        let object_id = MetadataObjectId::from_encoded(&encoded)?;
        remember_object(object_id, encoded, objects, seen)?;
        parents.push(NodeRef {
            logical_length: logical_offset,
            allocated_bytes: group.iter().try_fold(0_u64, |total, child| {
                total
                    .checked_add(child.allocated_bytes)
                    .ok_or(ManifestTreeError::ArithmeticOverflow)
            })?,
            object_id,
        });
    }
    Ok(parents)
}

#[allow(
    clippy::too_many_lines,
    reason = "keeping leaf and inner rewrites together makes the recursive COW invariant auditable"
)]
fn rewrite_node<F>(
    candidate: PendingNode,
    replaced: &Range<u64>,
    replacement: &[ManifestExtent],
    read: &mut F,
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<NodeRef, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    match decode_manifest_node(candidate.object_id, read)? {
        DecodedManifestNode::Leaf(leaf) => {
            let leaf_allocated_bytes = manifest_allocated_bytes(leaf.extents())?;
            if candidate.expected_level.is_some_and(|level| level != 0)
                || candidate
                    .expected_length
                    .is_some_and(|length| length != leaf.file_length())
                || candidate
                    .expected_allocated_bytes
                    .is_some_and(|allocated| allocated != leaf_allocated_bytes)
            {
                return Err(ManifestTreeError::InvalidTree);
            }
            let leaf_end = candidate
                .absolute_offset
                .checked_add(leaf.file_length())
                .ok_or(ManifestTreeError::ArithmeticOverflow)?;
            let overlap_start = replaced.start.max(candidate.absolute_offset);
            let overlap_end = replaced.end.min(leaf_end);
            if overlap_start >= overlap_end {
                return Ok(NodeRef {
                    logical_length: leaf.file_length(),
                    allocated_bytes: leaf_allocated_bytes,
                    object_id: candidate.object_id,
                });
            }
            let local_start = overlap_start - candidate.absolute_offset;
            let local_end = overlap_end - candidate.absolute_offset;
            let replacement_start = overlap_start - replaced.start;
            let replacement_end = overlap_end - replaced.start;
            let mut extents = slice_extents(leaf.extents(), 0..local_start)?;
            append_extents(
                &mut extents,
                slice_extents(replacement, replacement_start..replacement_end)?,
            )?;
            append_extents(
                &mut extents,
                slice_extents(leaf.extents(), local_end..leaf.file_length())?,
            )?;
            let rewritten_leaf = ManifestLeaf::new(leaf.file_length(), extents)?;
            let allocated_bytes = manifest_allocated_bytes(rewritten_leaf.extents())?;
            let encoded = rewritten_leaf.encode()?;
            let object_id = MetadataObjectId::from_encoded(&encoded)?;
            remember_object(object_id, encoded, objects, seen)?;
            Ok(NodeRef {
                logical_length: leaf.file_length(),
                allocated_bytes,
                object_id,
            })
        }
        DecodedManifestNode::Inner(node) => {
            let node_allocated_bytes = node.allocated_bytes()?;
            if candidate
                .expected_length
                .is_some_and(|length| length != node.file_length())
                || candidate
                    .expected_level
                    .is_some_and(|level| level != node.level())
                || candidate.expected_allocated_bytes.is_some_and(|expected| {
                    node_allocated_bytes.is_none_or(|observed| observed != expected)
                })
                || node.level() > MAX_TREE_LEVEL
            {
                return Err(ManifestTreeError::InvalidTree);
            }
            let child_level = node
                .level()
                .checked_sub(1)
                .ok_or(ManifestTreeError::InvalidTree)?;
            let mut children = Vec::new();
            children
                .try_reserve_exact(node.children().len())
                .map_err(|_| ManifestTreeError::OutOfMemory)?;
            for child in node.children() {
                let absolute_offset = candidate
                    .absolute_offset
                    .checked_add(child.logical_offset())
                    .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                let absolute_end = absolute_offset
                    .checked_add(child.logical_length())
                    .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                let child_ref = if absolute_end > replaced.start && absolute_offset < replaced.end {
                    rewrite_node(
                        PendingNode {
                            object_id: child.child(),
                            absolute_offset,
                            expected_length: Some(child.logical_length()),
                            expected_level: Some(child_level),
                            expected_allocated_bytes: child.allocated_bytes(),
                        },
                        replaced,
                        replacement,
                        read,
                        objects,
                        seen,
                    )?
                } else {
                    NodeRef {
                        logical_length: child.logical_length(),
                        allocated_bytes: child
                            .allocated_bytes()
                            .ok_or(ManifestTreeError::MissingSubtreeAllocation)?,
                        object_id: child.child(),
                    }
                };
                children.push(ManifestChildRange::new_with_allocated_bytes(
                    child.logical_offset(),
                    child_ref.logical_length,
                    child_ref.allocated_bytes,
                    child_ref.object_id,
                )?);
            }
            let rewritten_node =
                ManifestInnerNode::new(node.file_length(), node.level(), children)?;
            let allocated_bytes = rewritten_node
                .allocated_bytes()?
                .ok_or(ManifestTreeError::MissingSubtreeAllocation)?;
            let encoded = rewritten_node.encode()?;
            let object_id = MetadataObjectId::from_encoded(&encoded)?;
            remember_object(object_id, encoded, objects, seen)?;
            Ok(NodeRef {
                logical_length: node.file_length(),
                allocated_bytes,
                object_id,
            })
        }
    }
}

fn append_extents(
    target: &mut Vec<ManifestExtent>,
    source: Vec<ManifestExtent>,
) -> Result<(), ManifestTreeError> {
    target
        .try_reserve(source.len())
        .map_err(|_| ManifestTreeError::OutOfMemory)?;
    target.extend(source);
    Ok(())
}

fn slice_extents(
    extents: &[ManifestExtent],
    requested: Range<u64>,
) -> Result<Vec<ManifestExtent>, ManifestTreeError> {
    if requested.start > requested.end {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    let mut sliced = Vec::new();
    let mut cursor = 0_u64;
    for extent in extents {
        let end = cursor
            .checked_add(extent_length(extent))
            .ok_or(ManifestTreeError::ArithmeticOverflow)?;
        let start = cursor.max(requested.start);
        let selected_end = end.min(requested.end);
        if start < selected_end {
            let selected_length = selected_end - start;
            let selected = match *extent {
                ManifestExtent::Data {
                    logical_length,
                    chunk_id,
                } => {
                    if selected_length == logical_length {
                        ManifestExtent::Data {
                            logical_length,
                            chunk_id,
                        }
                    } else {
                        ManifestExtent::DataSlice {
                            logical_length: selected_length,
                            chunk_id,
                            chunk_length: u32::try_from(logical_length)
                                .map_err(|_| ManifestTreeError::ArithmeticOverflow)?,
                            chunk_offset: u32::try_from(start - cursor)
                                .map_err(|_| ManifestTreeError::ArithmeticOverflow)?,
                        }
                    }
                }
                ManifestExtent::DataSlice {
                    chunk_id,
                    chunk_length,
                    chunk_offset,
                    ..
                } => ManifestExtent::DataSlice {
                    logical_length: selected_length,
                    chunk_id,
                    chunk_length,
                    chunk_offset: chunk_offset
                        .checked_add(
                            u32::try_from(start - cursor)
                                .map_err(|_| ManifestTreeError::ArithmeticOverflow)?,
                        )
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?,
                },
                ManifestExtent::Hole { .. } => ManifestExtent::Hole {
                    logical_length: selected_length,
                },
                ManifestExtent::Fill { value, .. } => ManifestExtent::Fill {
                    logical_length: selected_length,
                    value,
                },
            };
            sliced
                .try_reserve(1)
                .map_err(|_| ManifestTreeError::OutOfMemory)?;
            sliced.push(selected);
        }
        cursor = end;
        if cursor >= requested.end {
            break;
        }
    }
    if cursor < requested.end {
        return Err(ManifestTreeError::InvalidReplacement);
    }
    Ok(sliced)
}

fn encode_leaves(
    manifest: &ManifestLeaf,
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<Vec<NodeRef>, ManifestTreeError> {
    if manifest.extents().is_empty() {
        let encoded = manifest.encode()?;
        let object_id = MetadataObjectId::from_encoded(&encoded)?;
        remember_object(object_id, encoded, objects, seen)?;
        return Ok(vec![NodeRef {
            logical_length: 0,
            allocated_bytes: 0,
            object_id,
        }]);
    }

    let mut leaves = Vec::new();
    let estimated = manifest.extents().len().div_ceil(MAX_LEAF_EXTENTS);
    leaves
        .try_reserve_exact(estimated)
        .map_err(|_| ManifestTreeError::OutOfMemory)?;
    let mut extent_start = 0_usize;
    let mut leaf_start = 0_u64;
    let mut cursor = 0_u64;
    let mut window_end = next_window_end(leaf_start);
    for (ordinal, extent) in manifest.extents().iter().enumerate() {
        cursor = cursor
            .checked_add(extent_length(extent))
            .ok_or(ManifestTreeError::ArithmeticOverflow)?;
        let extent_count = ordinal - extent_start + 1;
        if extent_count == MAX_LEAF_EXTENTS || cursor >= window_end {
            push_leaf(
                &manifest.extents()[extent_start..=ordinal],
                cursor - leaf_start,
                &mut leaves,
                objects,
                seen,
            )?;
            extent_start = ordinal + 1;
            leaf_start = cursor;
            window_end = next_window_end(leaf_start);
        }
    }
    if extent_start < manifest.extents().len() {
        push_leaf(
            &manifest.extents()[extent_start..],
            cursor - leaf_start,
            &mut leaves,
            objects,
            seen,
        )?;
    }
    if cursor != manifest.file_length() {
        return Err(ManifestTreeError::InvalidTree);
    }
    Ok(leaves)
}

fn push_leaf(
    extents: &[ManifestExtent],
    logical_length: u64,
    leaves: &mut Vec<NodeRef>,
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<(), ManifestTreeError> {
    let mut copied = Vec::new();
    copied
        .try_reserve_exact(extents.len())
        .map_err(|_| ManifestTreeError::OutOfMemory)?;
    copied.extend_from_slice(extents);
    let leaf = ManifestLeaf::new(logical_length, copied)?;
    let allocated_bytes = manifest_allocated_bytes(leaf.extents())?;
    let encoded = leaf.encode()?;
    let object_id = MetadataObjectId::from_encoded(&encoded)?;
    remember_object(object_id, encoded, objects, seen)?;
    leaves.push(NodeRef {
        logical_length,
        allocated_bytes,
        object_id,
    });
    Ok(())
}

fn remember_object(
    object_id: MetadataObjectId,
    encoded: Vec<u8>,
    objects: &mut Vec<(MetadataObjectId, Vec<u8>)>,
    seen: &mut BTreeSet<MetadataObjectId>,
) -> Result<(), ManifestTreeError> {
    if seen.insert(object_id) {
        objects
            .try_reserve(1)
            .map_err(|_| ManifestTreeError::OutOfMemory)?;
        objects.push((object_id, encoded));
    }
    Ok(())
}

fn next_window_end(offset: u64) -> u64 {
    offset
        .checked_div(LEAF_TARGET_LOGICAL_BYTES)
        .and_then(|window| window.checked_add(1))
        .and_then(|window| window.checked_mul(LEAF_TARGET_LOGICAL_BYTES))
        .unwrap_or(u64::MAX)
}

pub(crate) fn flatten_manifest_tree<F>(
    root: MetadataObjectId,
    mut read: F,
) -> Result<ManifestLeaf, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    let mut pending = vec![(root, None, None, None)];
    let mut extents = Vec::new();
    let mut root_length = None;
    let mut visited_nodes = 0_usize;
    while let Some((object_id, expected_length, expected_level, expected_allocated)) = pending.pop()
    {
        visited_nodes = visited_nodes
            .checked_add(1)
            .ok_or(ManifestTreeError::ArithmeticOverflow)?;
        if visited_nodes > MAX_FLATTENED_NODES {
            return Err(ManifestTreeError::TreeTooLarge);
        }
        let encoded = read(object_id)?;
        match metadata_object_kind(&encoded)? {
            MetadataObjectKind::ManifestLeaf => {
                if expected_level.is_some_and(|level| level != 0) {
                    return Err(ManifestTreeError::InvalidTree);
                }
                let leaf = ManifestLeaf::decode(&encoded)?;
                let leaf_allocated = manifest_allocated_bytes(leaf.extents())?;
                if expected_length.is_some_and(|length| length != leaf.file_length())
                    || expected_allocated.is_some_and(|allocated| allocated != leaf_allocated)
                {
                    return Err(ManifestTreeError::InvalidTree);
                }
                root_length.get_or_insert(leaf.file_length());
                let new_count = extents
                    .len()
                    .checked_add(leaf.extents().len())
                    .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                if new_count > MAX_FLATTENED_EXTENTS {
                    return Err(ManifestTreeError::TreeTooLarge);
                }
                extents
                    .try_reserve(leaf.extents().len())
                    .map_err(|_| ManifestTreeError::OutOfMemory)?;
                extents.extend_from_slice(leaf.extents());
            }
            MetadataObjectKind::ManifestInnerNode => {
                let node = ManifestInnerNode::decode(&encoded)?;
                let node_allocated = node.allocated_bytes()?;
                if expected_length.is_some_and(|length| length != node.file_length())
                    || expected_level.is_some_and(|level| level != node.level())
                    || expected_allocated.is_some_and(|allocated| {
                        node_allocated.is_none_or(|observed| observed != allocated)
                    })
                    || node.level() > MAX_TREE_LEVEL
                {
                    return Err(ManifestTreeError::InvalidTree);
                }
                root_length.get_or_insert(node.file_length());
                pending
                    .try_reserve(node.children().len())
                    .map_err(|_| ManifestTreeError::OutOfMemory)?;
                let child_level = node.level() - 1;
                for child in node.children().iter().rev() {
                    pending.push((
                        child.child(),
                        Some(child.logical_length()),
                        Some(child_level),
                        child.allocated_bytes(),
                    ));
                }
            }
            MetadataObjectKind::NamespaceRoot
            | MetadataObjectKind::NamespaceShard
            | MetadataObjectKind::ExactIndexRunSet
            | MetadataObjectKind::Unknown(_) => return Err(ManifestTreeError::InvalidTree),
        }
    }
    ManifestLeaf::new(root_length.ok_or(ManifestTreeError::InvalidTree)?, extents)
        .map_err(Into::into)
}

pub(crate) fn scan_manifest_tree<F, V>(
    root: MetadataObjectId,
    mut read: F,
    mut visit: V,
) -> Result<ManifestTreeSummary, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
    V: FnMut(u64, &ManifestExtent) -> Result<(), ManifestTreeError>,
{
    let mut allocated_bytes = 0_u64;
    let logical_size = walk_manifest_tree(root, None, None, &mut read, |offset, extent| {
        if !matches!(extent, ManifestExtent::Hole { .. }) {
            allocated_bytes = allocated_bytes
                .checked_add(extent_length(extent))
                .ok_or(ManifestTreeError::ArithmeticOverflow)?;
        }
        visit(offset, extent)
    })?;
    Ok(ManifestTreeSummary::new(
        root,
        logical_size,
        allocated_bytes,
    ))
}

pub(crate) fn read_manifest_tree_range<F>(
    root: MetadataObjectId,
    expected_logical_size: u64,
    offset: u64,
    length: u64,
    mut read: F,
) -> Result<Vec<ManifestRangeExtent>, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    if length == 0 || offset >= expected_logical_size {
        return Ok(Vec::new());
    }
    let end = offset.saturating_add(length).min(expected_logical_size);
    let mut extents = Vec::new();
    let logical_size = walk_manifest_tree(
        root,
        Some((offset, end)),
        Some(expected_logical_size),
        &mut read,
        |logical_offset, extent| {
            extents
                .try_reserve(1)
                .map_err(|_| ManifestTreeError::OutOfMemory)?;
            extents.push(ManifestRangeExtent {
                logical_offset,
                extent: extent.clone(),
            });
            Ok(())
        },
    )?;
    if logical_size != expected_logical_size {
        return Err(ManifestTreeError::InvalidTree);
    }
    Ok(extents)
}

#[allow(
    clippy::too_many_lines,
    reason = "one traversal keeps v1 fallback, v2 summary consumption, and touched-leaf accounting paired"
)]
pub(crate) fn allocated_bytes_in_manifest_tree_range<F>(
    root: MetadataObjectId,
    expected_logical_size: u64,
    offset: u64,
    length: u64,
    mut read: F,
) -> Result<u64, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    if length == 0 || offset >= expected_logical_size {
        return Ok(0);
    }
    let end = offset.saturating_add(length).min(expected_logical_size);
    let mut pending = vec![PendingNode {
        object_id: root,
        absolute_offset: 0,
        expected_length: Some(expected_logical_size),
        expected_level: None,
        expected_allocated_bytes: None,
    }];
    let mut allocated_bytes = 0_u64;
    while let Some(candidate) = pending.pop() {
        match decode_manifest_node(candidate.object_id, &mut read)? {
            DecodedManifestNode::Leaf(leaf) => {
                let leaf_allocated = manifest_allocated_bytes(leaf.extents())?;
                if candidate.expected_level.is_some_and(|level| level != 0)
                    || candidate
                        .expected_length
                        .is_some_and(|expected| expected != leaf.file_length())
                    || candidate
                        .expected_allocated_bytes
                        .is_some_and(|expected| expected != leaf_allocated)
                {
                    return Err(ManifestTreeError::InvalidTree);
                }
                let mut local_offset = 0_u64;
                for extent in leaf.extents() {
                    let absolute_offset = candidate
                        .absolute_offset
                        .checked_add(local_offset)
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    let extent_end = absolute_offset
                        .checked_add(extent_length(extent))
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    if !matches!(extent, ManifestExtent::Hole { .. }) {
                        allocated_bytes = allocated_bytes
                            .checked_add(
                                extent_end
                                    .min(end)
                                    .saturating_sub(absolute_offset.max(offset)),
                            )
                            .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    }
                    local_offset = local_offset
                        .checked_add(extent_length(extent))
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                }
            }
            DecodedManifestNode::Inner(node) => {
                let node_allocated = node.allocated_bytes()?;
                if candidate
                    .expected_length
                    .is_some_and(|expected| expected != node.file_length())
                    || candidate
                        .expected_level
                        .is_some_and(|expected| expected != node.level())
                    || candidate.expected_allocated_bytes.is_some_and(|expected| {
                        node_allocated.is_none_or(|observed| observed != expected)
                    })
                    || node.level() > MAX_TREE_LEVEL
                {
                    return Err(ManifestTreeError::InvalidTree);
                }
                let child_level = node
                    .level()
                    .checked_sub(1)
                    .ok_or(ManifestTreeError::InvalidTree)?;
                for child in node.children().iter().rev() {
                    let absolute_offset = candidate
                        .absolute_offset
                        .checked_add(child.logical_offset())
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    let child_end = absolute_offset
                        .checked_add(child.logical_length())
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    if child_end <= offset || absolute_offset >= end {
                        continue;
                    }
                    if offset <= absolute_offset
                        && child_end <= end
                        && let Some(child_allocated) = child.allocated_bytes()
                    {
                        allocated_bytes = allocated_bytes
                            .checked_add(child_allocated)
                            .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                        continue;
                    }
                    pending
                        .try_reserve(1)
                        .map_err(|_| ManifestTreeError::OutOfMemory)?;
                    pending.push(PendingNode {
                        object_id: child.child(),
                        absolute_offset,
                        expected_length: Some(child.logical_length()),
                        expected_level: Some(child_level),
                        expected_allocated_bytes: child.allocated_bytes(),
                    });
                }
            }
        }
    }
    Ok(allocated_bytes)
}

#[derive(Debug)]
enum DecodedManifestNode {
    Leaf(ManifestLeaf),
    Inner(ManifestInnerNode),
}

#[derive(Clone, Copy)]
struct PendingNode {
    object_id: MetadataObjectId,
    absolute_offset: u64,
    expected_length: Option<u64>,
    expected_level: Option<u16>,
    expected_allocated_bytes: Option<u64>,
}

fn walk_manifest_tree<F, V>(
    root: MetadataObjectId,
    requested: Option<(u64, u64)>,
    expected_root_length: Option<u64>,
    read: &mut F,
    mut visit: V,
) -> Result<u64, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
    V: FnMut(u64, &ManifestExtent) -> Result<(), ManifestTreeError>,
{
    let mut pending = vec![PendingNode {
        object_id: root,
        absolute_offset: 0,
        expected_length: expected_root_length,
        expected_level: None,
        expected_allocated_bytes: None,
    }];
    let mut root_length = None;
    while let Some(candidate) = pending.pop() {
        let node = decode_manifest_node(candidate.object_id, read)?;
        match node {
            DecodedManifestNode::Leaf(leaf) => {
                let leaf_allocated_bytes = manifest_allocated_bytes(leaf.extents())?;
                if candidate.expected_level.is_some_and(|level| level != 0)
                    || candidate
                        .expected_length
                        .is_some_and(|length| length != leaf.file_length())
                    || candidate
                        .expected_allocated_bytes
                        .is_some_and(|allocated| allocated != leaf_allocated_bytes)
                {
                    return Err(ManifestTreeError::InvalidTree);
                }
                root_length.get_or_insert(leaf.file_length());
                let mut local_offset = 0_u64;
                for extent in leaf.extents() {
                    let absolute_offset = candidate
                        .absolute_offset
                        .checked_add(local_offset)
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    let extent_end = absolute_offset
                        .checked_add(extent_length(extent))
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    if requested
                        .is_none_or(|(start, end)| extent_end > start && absolute_offset < end)
                    {
                        visit(absolute_offset, extent)?;
                    }
                    local_offset = local_offset
                        .checked_add(extent_length(extent))
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                }
                if local_offset != leaf.file_length() {
                    return Err(ManifestTreeError::InvalidTree);
                }
            }
            DecodedManifestNode::Inner(node) => {
                let node_allocated_bytes = node.allocated_bytes()?;
                if candidate
                    .expected_length
                    .is_some_and(|length| length != node.file_length())
                    || candidate
                        .expected_level
                        .is_some_and(|level| level != node.level())
                    || candidate.expected_allocated_bytes.is_some_and(|expected| {
                        node_allocated_bytes.is_none_or(|observed| observed != expected)
                    })
                    || node.level() > MAX_TREE_LEVEL
                {
                    return Err(ManifestTreeError::InvalidTree);
                }
                root_length.get_or_insert(node.file_length());
                let child_level = node
                    .level()
                    .checked_sub(1)
                    .ok_or(ManifestTreeError::InvalidTree)?;
                pending
                    .try_reserve(node.children().len())
                    .map_err(|_| ManifestTreeError::OutOfMemory)?;
                for child in node.children().iter().rev() {
                    let absolute_offset = candidate
                        .absolute_offset
                        .checked_add(child.logical_offset())
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    let child_end = absolute_offset
                        .checked_add(child.logical_length())
                        .ok_or(ManifestTreeError::ArithmeticOverflow)?;
                    if requested
                        .is_some_and(|(start, end)| child_end <= start || absolute_offset >= end)
                    {
                        continue;
                    }
                    pending.push(PendingNode {
                        object_id: child.child(),
                        absolute_offset,
                        expected_length: Some(child.logical_length()),
                        expected_level: Some(child_level),
                        expected_allocated_bytes: child.allocated_bytes(),
                    });
                }
            }
        }
    }
    root_length.ok_or(ManifestTreeError::InvalidTree)
}

fn decode_manifest_node<F>(
    object_id: MetadataObjectId,
    read: &mut F,
) -> Result<DecodedManifestNode, ManifestTreeError>
where
    F: FnMut(MetadataObjectId) -> Result<Vec<u8>, ManifestTreeError>,
{
    let encoded = read(object_id)?;
    match metadata_object_kind(&encoded)? {
        MetadataObjectKind::ManifestLeaf => {
            Ok(DecodedManifestNode::Leaf(ManifestLeaf::decode(&encoded)?))
        }
        MetadataObjectKind::ManifestInnerNode => Ok(DecodedManifestNode::Inner(
            ManifestInnerNode::decode(&encoded)?,
        )),
        MetadataObjectKind::NamespaceRoot
        | MetadataObjectKind::NamespaceShard
        | MetadataObjectKind::ExactIndexRunSet
        | MetadataObjectKind::Unknown(_) => Err(ManifestTreeError::InvalidTree),
    }
}

const fn extent_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::DataSlice { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
    }
}

fn manifest_allocated_bytes(extents: &[ManifestExtent]) -> Result<u64, ManifestTreeError> {
    extents.iter().try_fold(0_u64, |total, extent| {
        if matches!(extent, ManifestExtent::Hole { .. }) {
            Ok(total)
        } else {
            total
                .checked_add(extent_length(extent))
                .ok_or(ManifestTreeError::ArithmeticOverflow)
        }
    })
}

/// Failure to build or traverse one bounded immutable Manifest tree.
#[derive(Debug)]
pub enum ManifestTreeError {
    Io(io::Error),
    Metadata(MetadataFormatError),
    Inner(ManifestInnerNodeError),
    IdentityMismatch(MetadataObjectId),
    InvalidTree,
    InvalidReplacement,
    MissingSubtreeAllocation,
    TreeTooDeep,
    TreeTooLarge,
    ArithmeticOverflow,
    OutOfMemory,
}

impl fmt::Display for ManifestTreeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ManifestTreeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Inner(error) => Some(error),
            Self::IdentityMismatch(_)
            | Self::InvalidTree
            | Self::InvalidReplacement
            | Self::MissingSubtreeAllocation
            | Self::TreeTooDeep
            | Self::TreeTooLarge
            | Self::ArithmeticOverflow
            | Self::OutOfMemory => None,
        }
    }
}

impl From<io::Error> for ManifestTreeError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<MetadataFormatError> for ManifestTreeError {
    fn from(error: MetadataFormatError) -> Self {
        Self::Metadata(error)
    }
}

impl From<ManifestInnerNodeError> for ManifestTreeError {
    fn from(error: ManifestInnerNodeError) -> Self {
        Self::Inner(error)
    }
}
