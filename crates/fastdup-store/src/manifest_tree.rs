use fastdup_format::{
    MANIFEST_HEADER_BYTES, MAX_METADATA_OBJECT_BYTES, ManifestChildRange, ManifestExtent,
    ManifestInnerNode, ManifestInnerNodeError, ManifestLeaf, MetadataFormatError, MetadataObjectId,
    MetadataObjectKind, metadata_object_kind,
};
use std::collections::BTreeSet;
use std::fmt;
use std::io;

const LEAF_TARGET_LOGICAL_BYTES: u64 = 64 * 1_024 * 1_024;
const MAX_LEAF_EXTENTS: usize = 1_024;
const MAX_INNER_CHILDREN: usize = 1_024;
const MAX_TREE_LEVEL: u16 = 16;
const MAX_FLATTENED_EXTENTS: usize =
    (MAX_METADATA_OBJECT_BYTES - fastdup_format::METADATA_HEADER_BYTES - MANIFEST_HEADER_BYTES)
        / 64;
const MAX_FLATTENED_NODES: usize = 4_096;

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
