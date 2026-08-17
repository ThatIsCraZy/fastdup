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
pub(crate) struct ManifestTreeSummary {
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

    pub(crate) const fn root(self) -> MetadataObjectId {
        self.root
    }

    pub(crate) const fn logical_size(self) -> u64 {
        self.logical_size
    }

    pub(crate) const fn allocated_bytes(self) -> u64 {
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
                child_ranges.push(ManifestChildRange::new(
                    offset,
                    child.logical_length,
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
            if candidate.expected_level.is_some_and(|level| level != 0)
                || candidate
                    .expected_length
                    .is_some_and(|length| length != leaf.file_length())
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
            let encoded = ManifestLeaf::new(leaf.file_length(), extents)?.encode()?;
            let object_id = MetadataObjectId::from_encoded(&encoded)?;
            remember_object(object_id, encoded, objects, seen)?;
            Ok(NodeRef {
                logical_length: leaf.file_length(),
                object_id,
            })
        }
        DecodedManifestNode::Inner(node) => {
            if candidate
                .expected_length
                .is_some_and(|length| length != node.file_length())
                || candidate
                    .expected_level
                    .is_some_and(|level| level != node.level())
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
                let child_id = if absolute_end > replaced.start && absolute_offset < replaced.end {
                    rewrite_node(
                        PendingNode {
                            object_id: child.child(),
                            absolute_offset,
                            expected_length: Some(child.logical_length()),
                            expected_level: Some(child_level),
                        },
                        replaced,
                        replacement,
                        read,
                        objects,
                        seen,
                    )?
                    .object_id
                } else {
                    child.child()
                };
                children.push(ManifestChildRange::new(
                    child.logical_offset(),
                    child.logical_length(),
                    child_id,
                )?);
            }
            let encoded =
                ManifestInnerNode::new(node.file_length(), node.level(), children)?.encode()?;
            let object_id = MetadataObjectId::from_encoded(&encoded)?;
            remember_object(object_id, encoded, objects, seen)?;
            Ok(NodeRef {
                logical_length: node.file_length(),
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
                    if selected_length != logical_length {
                        return Err(ManifestTreeError::BoundaryInsideData);
                    }
                    ManifestExtent::Data {
                        logical_length,
                        chunk_id,
                    }
                }
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
    let encoded = ManifestLeaf::new(logical_length, copied)?.encode()?;
    let object_id = MetadataObjectId::from_encoded(&encoded)?;
    remember_object(object_id, encoded, objects, seen)?;
    leaves.push(NodeRef {
        logical_length,
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
    let mut pending = vec![(root, None, None)];
    let mut extents = Vec::new();
    let mut root_length = None;
    let mut visited_nodes = 0_usize;
    while let Some((object_id, expected_length, expected_level)) = pending.pop() {
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
                if expected_length.is_some_and(|length| length != leaf.file_length()) {
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
                if expected_length.is_some_and(|length| length != node.file_length())
                    || expected_level.is_some_and(|level| level != node.level())
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
                    ));
                }
            }
            MetadataObjectKind::NamespaceRoot
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
    }];
    let mut root_length = None;
    while let Some(candidate) = pending.pop() {
        let node = decode_manifest_node(candidate.object_id, read)?;
        match node {
            DecodedManifestNode::Leaf(leaf) => {
                if candidate.expected_level.is_some_and(|level| level != 0)
                    || candidate
                        .expected_length
                        .is_some_and(|length| length != leaf.file_length())
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
                if candidate
                    .expected_length
                    .is_some_and(|length| length != node.file_length())
                    || candidate
                        .expected_level
                        .is_some_and(|level| level != node.level())
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
        | MetadataObjectKind::ExactIndexRunSet
        | MetadataObjectKind::Unknown(_) => Err(ManifestTreeError::InvalidTree),
    }
}

const fn extent_length(extent: &ManifestExtent) -> u64 {
    match *extent {
        ManifestExtent::Data { logical_length, .. }
        | ManifestExtent::Hole { logical_length }
        | ManifestExtent::Fill { logical_length, .. } => logical_length,
    }
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
    BoundaryInsideData,
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
            | Self::BoundaryInsideData
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
