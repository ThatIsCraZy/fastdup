use rayon::prelude::*;
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use crate::metadata::{NAMESPACE_ROOT_KIND, decode_metadata_object, encode_metadata_object};
use crate::{MetadataFormatError, MetadataObjectId};

pub const NAMESPACE_ROOT_HEADER_BYTES: usize = 128;
const DURABLE_INODE_BYTES: usize = 96;
const NAMESPACE_ENTRY_HEADER_BYTES: usize = 24;
const NAMESPACE_ENTRY_ALIGNMENT: usize = 8;
const MAX_NAME_BYTES: usize = 255;
const NAMESPACE_ROOT_MAGIC: &[u8; 8] = b"FDNSRT01";
const FORMAT_VERSION: u16 = 4;
const ROOT_INODE: u64 = 1;
const XATTR_RECORD_HEADER_BYTES: usize = 24;
const XATTR_RECORD_ALIGNMENT: usize = 8;
const POSIX_METADATA_RECORD_HEADER_BYTES: usize = 64;
const MAXIMUM_XATTR_NAME_BYTES: usize = 255;
const MAXIMUM_XATTR_VALUE_BYTES: usize = 65_536;
const MAXIMUM_XATTRS_PER_INODE: usize = 1_024;
const MAXIMUM_XATTR_BYTES_PER_INODE: usize = 1_048_576;
const FS_IMMUTABLE_FL: u32 = 0x0000_0010;
const POSIX_ACL_ACCESS_XATTR: &[u8] = b"system.posix_acl_access";
const POSIX_ACL_DEFAULT_XATTR: &[u8] = b"system.posix_acl_default";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedNamespaceShard {
    object_id: MetadataObjectId,
    bytes: Vec<u8>,
}

impl EncodedNamespaceShard {
    #[must_use]
    pub const fn object_id(&self) -> MetadataObjectId {
        self.object_id
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedNamespaceGraph {
    root: Vec<u8>,
    shards: Vec<EncodedNamespaceShard>,
}

impl EncodedNamespaceGraph {
    #[must_use]
    pub fn root(&self) -> &[u8] {
        &self.root
    }

    #[must_use]
    pub fn shards(&self) -> &[EncodedNamespaceShard] {
        &self.shards
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceShardRef {
    kind: u8,
    record_count: u32,
    first_key: u64,
    first_name_length: u32,
    object_id: MetadataObjectId,
}

impl NamespaceShardRef {
    #[must_use]
    pub const fn object_id(self) -> MetadataObjectId {
        self.object_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceGraphRoot {
    inode_reservation_end: u64,
    inode_allocation_cursor: u64,
    namespace_mutation_sequence: u64,
    inode_count: u64,
    entry_count: u64,
    root_metadata: DurableRootMetadata,
    shards: Vec<NamespaceShardRef>,
}

/// Metadata reachability view of one authenticated Namespace graph.
///
/// It carries only durable graph object identities and regular-file Manifest
/// roots. Directory entries, extended attributes, and POSIX metadata remain
/// independently validated by full namespace decoding and offline scrub.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceGcGraph {
    inode_reservation_end: u64,
    inode_allocation_cursor: u64,
    namespace_mutation_sequence: u64,
    namespace_object_ids: Vec<MetadataObjectId>,
    inode_transitions: Vec<(u64, u64)>,
    manifest_roots: Vec<MetadataObjectId>,
}

impl NamespaceGcGraph {
    #[must_use]
    pub const fn inode_reservation_end(&self) -> u64 {
        self.inode_reservation_end
    }

    #[must_use]
    pub const fn inode_allocation_cursor(&self) -> u64 {
        self.inode_allocation_cursor
    }

    #[must_use]
    pub const fn namespace_mutation_sequence(&self) -> u64 {
        self.namespace_mutation_sequence
    }

    #[must_use]
    pub fn namespace_object_ids(&self) -> &[MetadataObjectId] {
        &self.namespace_object_ids
    }

    /// Returns every inode ID and mutation sequence in durable inode order.
    ///
    /// These values are sufficient to verify Commit-Record graph transitions
    /// without materializing directory entries or extended attributes.
    #[must_use]
    pub fn inode_transitions(&self) -> &[(u64, u64)] {
        &self.inode_transitions
    }

    #[must_use]
    pub fn manifest_roots(&self) -> &[MetadataObjectId] {
        &self.manifest_roots
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableInodeKind {
    Regular,
    Directory,
    Symlink,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DurableTimestamp {
    pub seconds: i64,
    pub nanoseconds: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DurableTimes {
    pub atime: DurableTimestamp,
    pub mtime: DurableTimestamp,
    pub ctime: DurableTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableXattr {
    name: Vec<u8>,
    value: Vec<u8>,
}

impl DurableXattr {
    /// Constructs one bounded byte-exact durable extended attribute.
    ///
    /// # Errors
    ///
    /// Rejects invalid names, unsupported namespaces, oversized values, and
    /// malformed POSIX ACL wire values.
    pub fn new(name: Vec<u8>, value: Vec<u8>) -> Result<Self, MetadataFormatError> {
        validate_xattr_name(&name)?;
        if value.len() > MAXIMUM_XATTR_VALUE_BYTES {
            return Err(MetadataFormatError::InvalidPayload);
        }
        if name == POSIX_ACL_ACCESS_XATTR || name == POSIX_ACL_DEFAULT_XATTR {
            validate_acl(&value)?;
        }
        Ok(Self { name, value })
    }

    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRootMetadata {
    mode: u16,
    uid: u32,
    gid: u32,
    file_flags: u32,
    xattrs: Vec<DurableXattr>,
    times: DurableTimes,
}

impl Default for DurableRootMetadata {
    fn default() -> Self {
        Self {
            mode: 0o755,
            uid: 0,
            gid: 0,
            file_flags: 0,
            xattrs: Vec::new(),
            times: DurableTimes::default(),
        }
    }
}

impl DurableRootMetadata {
    /// Constructs the explicit metadata of the otherwise implicit root inode.
    ///
    /// # Errors
    ///
    /// Rejects unsupported flags or invalid directory attributes.
    pub fn new(
        mode: u16,
        uid: u32,
        gid: u32,
        file_flags: u32,
        xattrs: Vec<DurableXattr>,
    ) -> Result<Self, MetadataFormatError> {
        let xattrs = canonical_xattrs(DurableInodeKind::Directory, xattrs)?;
        validate_file_flags(file_flags)?;
        Ok(Self {
            mode: mode & 0o7777,
            uid,
            gid,
            file_flags,
            xattrs,
            times: DurableTimes::default(),
        })
    }

    #[must_use]
    pub const fn mode(&self) -> u16 {
        self.mode
    }

    #[must_use]
    pub const fn uid(&self) -> u32 {
        self.uid
    }

    #[must_use]
    pub const fn gid(&self) -> u32 {
        self.gid
    }

    #[must_use]
    pub const fn file_flags(&self) -> u32 {
        self.file_flags
    }

    #[must_use]
    pub fn xattrs(&self) -> &[DurableXattr] {
        &self.xattrs
    }

    #[must_use]
    pub const fn times(&self) -> DurableTimes {
        self.times
    }

    #[must_use]
    pub fn with_times(mut self, times: DurableTimes) -> Self {
        self.times = times;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableInode {
    inode: u64,
    kind: DurableInodeKind,
    mode: u16,
    uid: u32,
    gid: u32,
    link_count: u32,
    mutation_sequence: u64,
    logical_size: u64,
    manifest_root: Option<MetadataObjectId>,
    file_flags: u32,
    xattrs: Vec<DurableXattr>,
    times: DurableTimes,
    symlink_target: Option<Vec<u8>>,
}

impl DurableInode {
    /// Constructs one immutable regular-file inode version.
    ///
    /// # Errors
    ///
    /// Rejects the implicit root inode and zero-link orphan records.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inode: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
        logical_size: u64,
        manifest_root: MetadataObjectId,
    ) -> Result<Self, MetadataFormatError> {
        if inode <= ROOT_INODE || link_count == 0 {
            return Err(MetadataFormatError::InvalidPayload);
        }
        Ok(Self {
            inode,
            kind: DurableInodeKind::Regular,
            mode,
            uid,
            gid,
            link_count,
            mutation_sequence,
            logical_size,
            manifest_root: Some(manifest_root),
            file_flags: 0,
            xattrs: Vec::new(),
            times: DurableTimes::default(),
            symlink_target: None,
        })
    }

    /// Constructs one immutable regular-file inode version with extended metadata.
    ///
    /// # Errors
    ///
    /// Rejects the same malformed inode fields as [`Self::new`] plus invalid
    /// file flags or attributes.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_metadata(
        inode: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
        logical_size: u64,
        manifest_root: MetadataObjectId,
        file_flags: u32,
        xattrs: Vec<DurableXattr>,
    ) -> Result<Self, MetadataFormatError> {
        let mut durable = Self::new(
            inode,
            mode,
            uid,
            gid,
            link_count,
            mutation_sequence,
            logical_size,
            manifest_root,
        )?;
        validate_file_flags(file_flags)?;
        durable.file_flags = file_flags;
        durable.xattrs = canonical_xattrs(DurableInodeKind::Regular, xattrs)?;
        Ok(durable)
    }

    /// Constructs one immutable directory inode version.
    ///
    /// # Errors
    ///
    /// Rejects the implicit root inode and directory link counts below two.
    pub fn new_directory(
        inode: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
    ) -> Result<Self, MetadataFormatError> {
        if inode <= ROOT_INODE || link_count < 2 {
            return Err(MetadataFormatError::InvalidPayload);
        }
        Ok(Self {
            inode,
            kind: DurableInodeKind::Directory,
            mode,
            uid,
            gid,
            link_count,
            mutation_sequence,
            logical_size: 0,
            manifest_root: None,
            file_flags: 0,
            xattrs: Vec::new(),
            times: DurableTimes::default(),
            symlink_target: None,
        })
    }

    /// Constructs one directory inode version with extended metadata.
    ///
    /// # Errors
    ///
    /// Rejects the same malformed inode fields as [`Self::new_directory`]
    /// plus invalid file flags or attributes.
    #[allow(clippy::too_many_arguments)]
    pub fn new_directory_with_metadata(
        inode: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
        file_flags: u32,
        xattrs: Vec<DurableXattr>,
    ) -> Result<Self, MetadataFormatError> {
        let mut durable =
            Self::new_directory(inode, mode, uid, gid, link_count, mutation_sequence)?;
        validate_file_flags(file_flags)?;
        durable.file_flags = file_flags;
        durable.xattrs = canonical_xattrs(DurableInodeKind::Directory, xattrs)?;
        Ok(durable)
    }

    /// Constructs one byte-exact symbolic-link inode.
    ///
    /// # Errors
    ///
    /// Rejects invalid inode identities, link counts, or target lengths.
    pub fn new_symlink(
        inode: u64,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
        target: Vec<u8>,
    ) -> Result<Self, MetadataFormatError> {
        if inode <= ROOT_INODE || link_count == 0 || target.is_empty() || target.len() > 4_096 {
            return Err(MetadataFormatError::InvalidPayload);
        }
        Ok(Self {
            inode,
            kind: DurableInodeKind::Symlink,
            mode: 0o777,
            uid,
            gid,
            link_count,
            mutation_sequence,
            logical_size: u64::try_from(target.len())
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
            manifest_root: None,
            file_flags: 0,
            xattrs: Vec::new(),
            times: DurableTimes::default(),
            symlink_target: Some(target),
        })
    }

    #[must_use]
    pub fn with_times(mut self, times: DurableTimes) -> Self {
        self.times = times;
        self
    }

    #[must_use]
    pub const fn inode(&self) -> u64 {
        self.inode
    }

    #[must_use]
    pub const fn kind(&self) -> DurableInodeKind {
        self.kind
    }

    #[must_use]
    pub const fn mode(&self) -> u16 {
        self.mode
    }

    #[must_use]
    pub const fn uid(&self) -> u32 {
        self.uid
    }

    #[must_use]
    pub const fn gid(&self) -> u32 {
        self.gid
    }

    #[must_use]
    pub const fn link_count(&self) -> u32 {
        self.link_count
    }

    #[must_use]
    pub const fn mutation_sequence(&self) -> u64 {
        self.mutation_sequence
    }

    #[must_use]
    pub const fn logical_size(&self) -> u64 {
        self.logical_size
    }

    #[must_use]
    /// Returns the Manifest Root of a regular-file inode.
    ///
    /// # Panics
    ///
    /// Panics when called for a directory. Callers traversing a mixed
    /// Namespace Root must use [`Self::file_manifest_root`] or `file_inodes`.
    pub fn manifest_root(&self) -> MetadataObjectId {
        self.manifest_root
            .expect("ASSERT: only regular durable inodes have Manifest Roots")
    }

    #[must_use]
    pub const fn file_manifest_root(&self) -> Option<MetadataObjectId> {
        self.manifest_root
    }

    #[must_use]
    pub const fn file_flags(&self) -> u32 {
        self.file_flags
    }

    #[must_use]
    pub fn xattrs(&self) -> &[DurableXattr] {
        &self.xattrs
    }

    #[must_use]
    pub const fn times(&self) -> DurableTimes {
        self.times
    }

    #[must_use]
    pub fn symlink_target(&self) -> Option<&[u8]> {
        self.symlink_target.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceEntry {
    parent_inode: u64,
    target_inode: u64,
    name: Vec<u8>,
}

impl NamespaceEntry {
    /// Constructs one byte-exact directory entry.
    ///
    /// # Errors
    ///
    /// Rejects a zero parent, an invalid target, or a component that POSIX
    /// cannot represent as one byte-exact name. The complete Namespace Root
    /// later proves that the parent is a reachable directory.
    pub fn new(
        parent_inode: u64,
        target_inode: u64,
        name: Vec<u8>,
    ) -> Result<Self, MetadataFormatError> {
        validate_entry_fields(parent_inode, target_inode, &name)?;
        Ok(Self {
            parent_inode,
            target_inode,
            name,
        })
    }

    #[must_use]
    pub const fn parent_inode(&self) -> u64 {
        self.parent_inode
    }

    #[must_use]
    pub const fn target_inode(&self) -> u64 {
        self.target_inode
    }

    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceRoot {
    inode_reservation_end: u64,
    inode_allocation_cursor: u64,
    namespace_mutation_sequence: u64,
    root_metadata: DurableRootMetadata,
    inodes: Vec<DurableInode>,
    entries: Vec<NamespaceEntry>,
}

impl NamespaceRoot {
    /// Constructs one canonical, bounded Namespace Root.
    ///
    /// The root inode is implicit. Input vectors are canonicalized by durable
    /// key before uniqueness, reachability, and exact link counts are checked.
    ///
    /// # Errors
    ///
    /// Rejects duplicate IDs or names, dangling entries, open orphans, link
    /// count disagreement, and an inode reservation that could permit reuse.
    pub fn new(
        inode_reservation_end: u64,
        inode_allocation_cursor: u64,
        namespace_mutation_sequence: u64,
        inodes: Vec<DurableInode>,
        entries: Vec<NamespaceEntry>,
    ) -> Result<Self, MetadataFormatError> {
        Self::new_with_root_metadata(
            inode_reservation_end,
            inode_allocation_cursor,
            namespace_mutation_sequence,
            DurableRootMetadata::default(),
            inodes,
            entries,
        )
    }

    /// Constructs a canonical Namespace Root with explicit root-inode metadata.
    ///
    /// # Errors
    ///
    /// Rejects the same namespace and size invariants as [`Self::new`].
    pub fn new_with_root_metadata(
        inode_reservation_end: u64,
        inode_allocation_cursor: u64,
        namespace_mutation_sequence: u64,
        root_metadata: DurableRootMetadata,
        mut inodes: Vec<DurableInode>,
        mut entries: Vec<NamespaceEntry>,
    ) -> Result<Self, MetadataFormatError> {
        inodes.sort_unstable_by_key(DurableInode::inode);
        entries.sort_unstable_by(|left, right| {
            (left.parent_inode, left.name.as_slice())
                .cmp(&(right.parent_inode, right.name.as_slice()))
        });
        validate_namespace(
            inode_reservation_end,
            inode_allocation_cursor,
            &inodes,
            &entries,
        )?;
        payload_length(&root_metadata, &entries, &inodes)?;
        Ok(Self {
            inode_reservation_end,
            inode_allocation_cursor,
            namespace_mutation_sequence,
            root_metadata,
            inodes,
            entries,
        })
    }

    #[must_use]
    pub const fn inode_reservation_end(&self) -> u64 {
        self.inode_reservation_end
    }

    #[must_use]
    pub const fn inode_allocation_cursor(&self) -> u64 {
        self.inode_allocation_cursor
    }

    #[must_use]
    pub const fn namespace_mutation_sequence(&self) -> u64 {
        self.namespace_mutation_sequence
    }

    #[must_use]
    pub const fn root_metadata(&self) -> &DurableRootMetadata {
        &self.root_metadata
    }

    #[must_use]
    pub fn inodes(&self) -> &[DurableInode] {
        &self.inodes
    }

    pub fn file_inodes(&self) -> impl Iterator<Item = &DurableInode> {
        self.inodes
            .iter()
            .filter(|inode| inode.kind == DurableInodeKind::Regular)
    }

    #[must_use]
    pub fn file_inode_count(&self) -> usize {
        self.file_inodes().count()
    }

    pub fn directory_inodes(&self) -> impl Iterator<Item = &DurableInode> {
        self.inodes
            .iter()
            .filter(|inode| inode.kind == DurableInodeKind::Directory)
    }

    pub fn symlink_inodes(&self) -> impl Iterator<Item = &DurableInode> {
        self.inodes
            .iter()
            .filter(|inode| inode.kind == DurableInodeKind::Symlink)
    }

    #[must_use]
    pub fn entries(&self) -> &[NamespaceEntry] {
        &self.entries
    }

    /// Encodes the canonical logical Namespace state before graph sharding.
    ///
    /// # Errors
    ///
    /// Returns an invariant or arithmetic failure.
    ///
    /// # Panics
    ///
    /// Panics only if the checked payload preflight disagrees with the encoder
    /// cursor, which is an impossible internal writer state.
    #[allow(clippy::too_many_lines)]
    pub fn encode_canonical_state(&self) -> Result<Vec<u8>, MetadataFormatError> {
        // Every constructor validates, the fields are private and no method
        // takes `&mut self`, so a live value cannot have become invalid. The
        // commit path encodes once per generation and must not repeat a
        // whole-Namespace proof that construction already carried.
        let payload_length = payload_length(&self.root_metadata, &self.entries, &self.inodes)?;
        let inode_bytes = self
            .inodes
            .len()
            .checked_mul(DURABLE_INODE_BYTES)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let entries_offset = NAMESPACE_ROOT_HEADER_BYTES
            .checked_add(inode_bytes)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let xattrs_offset = self
            .entries
            .iter()
            .try_fold(entries_offset, |offset, entry| {
                offset
                    .checked_add(entry_record_length(entry.name.len())?)
                    .ok_or(MetadataFormatError::ArithmeticOverflow)
            })?;
        let xattr_count =
            self.inodes
                .iter()
                .try_fold(self.root_metadata.xattrs.len(), |count, inode| {
                    count
                        .checked_add(inode.xattrs.len())
                        .ok_or(MetadataFormatError::ArithmeticOverflow)
                })?;
        let posix_metadata_offset =
            self.inodes
                .iter()
                .try_fold(xattrs_offset, |offset, inode| {
                    inode.xattrs.iter().try_fold(offset, |offset, xattr| {
                        offset
                            .checked_add(xattr_record_length(xattr.name.len(), xattr.value.len())?)
                            .ok_or(MetadataFormatError::ArithmeticOverflow)
                    })
                })?;
        let posix_metadata_offset =
            self.root_metadata
                .xattrs
                .iter()
                .try_fold(posix_metadata_offset, |offset, xattr| {
                    offset
                        .checked_add(xattr_record_length(xattr.name.len(), xattr.value.len())?)
                        .ok_or(MetadataFormatError::ArithmeticOverflow)
                })?;
        let mut payload = vec![0_u8; payload_length];
        payload[0..8].copy_from_slice(NAMESPACE_ROOT_MAGIC);
        put_u16(&mut payload, 8, FORMAT_VERSION);
        put_u16(&mut payload, 10, 128);
        put_u16(&mut payload, 12, 96);
        put_u16(&mut payload, 14, 24);
        put_u16(&mut payload, 16, self.root_metadata.mode);
        put_u32(&mut payload, 20, self.root_metadata.uid);
        put_u32(&mut payload, 24, self.root_metadata.gid);
        put_u32(&mut payload, 28, self.root_metadata.file_flags);
        put_u64(&mut payload, 32, ROOT_INODE);
        put_u64(&mut payload, 40, self.inode_reservation_end);
        put_u64(&mut payload, 48, self.namespace_mutation_sequence);
        put_u32(
            &mut payload,
            56,
            u32::try_from(self.inodes.len())
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u32(
            &mut payload,
            60,
            u32::try_from(self.entries.len())
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u64(&mut payload, 64, 128);
        put_u64(
            &mut payload,
            72,
            u64::try_from(entries_offset).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u64(
            &mut payload,
            80,
            u64::try_from(payload_length).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u64(&mut payload, 88, self.inode_allocation_cursor);
        put_u32(
            &mut payload,
            96,
            u32::try_from(xattr_count).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u16(&mut payload, 100, 24);
        put_u64(
            &mut payload,
            104,
            u64::try_from(xattrs_offset).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u32(
            &mut payload,
            112,
            u32::try_from(self.inodes.len() + 1)
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u16(
            &mut payload,
            116,
            u16::try_from(POSIX_METADATA_RECORD_HEADER_BYTES)
                .expect("ASSERT: fixed metadata header length fits u16"),
        );
        put_u64(
            &mut payload,
            120,
            u64::try_from(posix_metadata_offset)
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );

        for (ordinal, inode) in self.inodes.iter().enumerate() {
            let start = NAMESPACE_ROOT_HEADER_BYTES + ordinal * DURABLE_INODE_BYTES;
            let record = &mut payload[start..start + DURABLE_INODE_BYTES];
            put_u64(record, 0, inode.inode);
            put_u16(record, 8, inode.mode);
            put_u16(
                record,
                10,
                match inode.kind {
                    DurableInodeKind::Regular => 1,
                    DurableInodeKind::Directory => 2,
                    DurableInodeKind::Symlink => 3,
                },
            );
            put_u32(record, 12, inode.uid);
            put_u32(record, 16, inode.gid);
            put_u32(record, 20, inode.link_count);
            put_u64(record, 24, inode.mutation_sequence);
            put_u64(record, 32, inode.logical_size);
            if let Some(manifest_root) = inode.manifest_root {
                record[40..72].copy_from_slice(&manifest_root.bytes());
            }
            put_u32(record, 72, inode.file_flags);
        }

        let mut cursor = entries_offset;
        for entry in &self.entries {
            let record_length = entry_record_length(entry.name.len())?;
            let end = cursor
                .checked_add(record_length)
                .ok_or(MetadataFormatError::ArithmeticOverflow)?;
            let record = &mut payload[cursor..end];
            put_u32(
                record,
                0,
                u32::try_from(record_length)
                    .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
            );
            put_u16(
                record,
                4,
                u16::try_from(entry.name.len())
                    .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
            );
            put_u64(record, 8, entry.parent_inode);
            put_u64(record, 16, entry.target_inode);
            record[24..24 + entry.name.len()].copy_from_slice(&entry.name);
            cursor = end;
        }
        assert_eq!(
            cursor, xattrs_offset,
            "ASSERT: xattr offset matches entries"
        );
        for xattr in &self.root_metadata.xattrs {
            cursor = encode_xattr_record(&mut payload, cursor, ROOT_INODE, xattr)?;
        }
        for inode in &self.inodes {
            for xattr in &inode.xattrs {
                cursor = encode_xattr_record(&mut payload, cursor, inode.inode, xattr)?;
            }
        }
        assert_eq!(cursor, posix_metadata_offset);
        cursor = encode_posix_metadata_record(
            &mut payload,
            cursor,
            ROOT_INODE,
            self.root_metadata.times,
            None,
        )?;
        for inode in &self.inodes {
            cursor = encode_posix_metadata_record(
                &mut payload,
                cursor,
                inode.inode,
                inode.times,
                inode.symlink_target.as_deref(),
            )?;
        }
        assert_eq!(
            cursor, payload_length,
            "ASSERT: namespace payload preflight must match encoder cursor"
        );
        Ok(payload)
    }

    /// Encodes one child-first, bounded durable Namespace graph.
    ///
    /// # Errors
    ///
    /// Returns an invariant, arithmetic, or bounded-envelope failure.
    pub fn encode_graph(&self) -> Result<EncodedNamespaceGraph, MetadataFormatError> {
        encode_namespace_graph(self)
    }

    /// Fully validates and decodes one reconstructed canonical Namespace state.
    ///
    /// # Errors
    ///
    /// Returns an envelope, layout, reserved-field, name, reference, or link
    /// invariant failure without exposing partial namespace state.
    #[allow(clippy::too_many_lines)]
    pub fn decode_canonical_state(payload: &[u8]) -> Result<Self, MetadataFormatError> {
        if payload.len() < NAMESPACE_ROOT_HEADER_BYTES {
            return Err(MetadataFormatError::InvalidPayload);
        }
        if &payload[0..8] != NAMESPACE_ROOT_MAGIC
            || get_u16(payload, 8) != FORMAT_VERSION
            || usize::from(get_u16(payload, 10)) != NAMESPACE_ROOT_HEADER_BYTES
            || usize::from(get_u16(payload, 12)) != DURABLE_INODE_BYTES
            || usize::from(get_u16(payload, 14)) != NAMESPACE_ENTRY_HEADER_BYTES
            || get_u64(payload, 32) != ROOT_INODE
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        if get_u16(payload, 18) != 0 || get_u16(payload, 102) != 0 || get_u16(payload, 118) != 0 {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let root_metadata = DurableRootMetadata::new(
            get_u16(payload, 16),
            get_u32(payload, 20),
            get_u32(payload, 24),
            get_u32(payload, 28),
            Vec::new(),
        )?;
        let inode_count = usize::try_from(get_u32(payload, 56))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let entry_count = usize::try_from(get_u32(payload, 60))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let inode_bytes = inode_count
            .checked_mul(DURABLE_INODE_BYTES)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let entries_offset = NAMESPACE_ROOT_HEADER_BYTES
            .checked_add(inode_bytes)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if usize::try_from(get_u64(payload, 64)) != Ok(NAMESPACE_ROOT_HEADER_BYTES)
            || usize::try_from(get_u64(payload, 72)) != Ok(entries_offset)
            || usize::try_from(get_u64(payload, 80)) != Ok(payload.len())
            || entries_offset > payload.len()
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        if usize::from(get_u16(payload, 100)) != XATTR_RECORD_HEADER_BYTES {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let xattrs_offset = usize::try_from(get_u64(payload, 104))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        if xattrs_offset < entries_offset || xattrs_offset > payload.len() {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let remaining_entry_bytes = xattrs_offset - entries_offset;
        let minimum_entry_bytes = entry_record_length(1)?;
        if entry_count > remaining_entry_bytes / minimum_entry_bytes {
            return Err(MetadataFormatError::InvalidPayload);
        }

        let mut inodes = decode_inodes(payload, inode_count)?;
        let entries = decode_entries(payload, entries_offset, xattrs_offset, entry_count)?;
        let mut root_metadata = root_metadata;
        if usize::from(get_u16(payload, 116)) != POSIX_METADATA_RECORD_HEADER_BYTES {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let posix_metadata_offset = usize::try_from(get_u64(payload, 120))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        if posix_metadata_offset < xattrs_offset || posix_metadata_offset > payload.len() {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let xattr_count = usize::try_from(get_u32(payload, 96))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let decoded = decode_xattrs(payload, xattrs_offset, posix_metadata_offset, xattr_count)?;
        install_decoded_xattrs(&mut root_metadata, &mut inodes, decoded)?;
        let count = usize::try_from(get_u32(payload, 112))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        decode_posix_metadata(
            payload,
            posix_metadata_offset,
            count,
            &mut root_metadata,
            &mut inodes,
        )?;
        if inodes.windows(2).any(|pair| pair[0].inode >= pair[1].inode)
            || entries.windows(2).any(|pair| {
                (pair[0].parent_inode, pair[0].name.as_slice())
                    >= (pair[1].parent_inode, pair[1].name.as_slice())
            })
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        Self::new_with_root_metadata(
            get_u64(payload, 40),
            get_u64(payload, 88),
            get_u64(payload, 48),
            root_metadata,
            inodes,
            entries,
        )
    }

    /// Reconstructs and fully validates one bounded Namespace graph.
    ///
    /// # Errors
    ///
    /// Rejects a missing, extra, reordered, substituted, corrupt, or
    /// non-contiguous shard and any invalid reconstructed namespace state.
    pub fn decode_graph<B: Borrow<Vec<u8>>>(
        encoded_root: &[u8],
        encoded_shards: &BTreeMap<MetadataObjectId, B>,
    ) -> Result<Self, MetadataFormatError> {
        let root = NamespaceGraphRoot::decode(encoded_root)?;
        let expected_ids = root
            .shards
            .iter()
            .map(|reference| reference.object_id)
            .collect::<BTreeSet<_>>();
        if encoded_shards.keys().copied().collect::<BTreeSet<_>>() != expected_ids {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let mut inodes = Vec::new();
        let mut entries = Vec::new();
        for reference in &root.shards {
            let encoded = encoded_shards
                .get(&reference.object_id)
                .ok_or(MetadataFormatError::InvalidPayload)?;
            if reference.kind == SHARD_KIND_INODE {
                let shard = decode_inode_shard(encoded.borrow())?;
                if u32::try_from(shard.len()) != Ok(reference.record_count)
                    || shard[0].inode != reference.first_key
                {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                inodes.extend(shard);
            } else {
                let shard = decode_entry_shard(encoded.borrow())?;
                if u32::try_from(shard.len()) != Ok(reference.record_count)
                    || shard[0].parent_inode != reference.first_key
                    || u32::try_from(shard[0].name.len()) != Ok(reference.first_name_length)
                {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                entries.extend(shard);
            }
        }
        if u64::try_from(inodes.len()) != Ok(root.inode_count)
            || u64::try_from(entries.len()) != Ok(root.entry_count)
            || inodes.windows(2).any(|pair| pair[0].inode >= pair[1].inode)
            || entries.windows(2).any(|pair| {
                (pair[0].parent_inode, pair[0].name.as_slice())
                    >= (pair[1].parent_inode, pair[1].name.as_slice())
            })
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        Self::new_with_root_metadata(
            root.inode_reservation_end,
            root.inode_allocation_cursor,
            root.namespace_mutation_sequence,
            root.root_metadata,
            inodes,
            entries,
        )
    }
}

impl NamespaceGraphRoot {
    /// Decodes and authenticates one compact Namespace graph descriptor.
    ///
    /// # Errors
    ///
    /// Rejects any envelope, field, partition, or bound violation.
    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataFormatError> {
        let object = decode_metadata_object(Some(NAMESPACE_ROOT_KIND), bytes)?;
        let payload = object.payload;
        if payload.len() < NAMESPACE_GRAPH_HEADER_BYTES_V2
            || &payload[0..8] != NAMESPACE_GRAPH_MAGIC_V2
            || get_u16(payload, 8) != NAMESPACE_GRAPH_VERSION_V2
            || usize::from(get_u16(payload, 10)) != NAMESPACE_GRAPH_HEADER_BYTES_V2
            || usize::from(get_u16(payload, 12)) != NAMESPACE_GRAPH_SHARD_REF_BYTES_V2
            || get_u16(payload, 14) != 0
            || usize::try_from(get_u64(payload, 64)) != Ok(NAMESPACE_GRAPH_HEADER_BYTES_V2)
            || usize::try_from(get_u64(payload, 88)) != Ok(payload.len())
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let metadata_length = usize::try_from(get_u64(payload, 72))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let shard_refs_offset = usize::try_from(get_u64(payload, 80))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        if shard_refs_offset
            != NAMESPACE_GRAPH_HEADER_BYTES_V2
                .checked_add(metadata_length)
                .ok_or(MetadataFormatError::ArithmeticOverflow)?
            || shard_refs_offset > payload.len()
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let root_metadata = decode_root_metadata_blob(
            &payload[NAMESPACE_GRAPH_HEADER_BYTES_V2..shard_refs_offset],
        )?;
        let inode_shard_count = usize::try_from(get_u32(payload, 56))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let entry_shard_count = usize::try_from(get_u32(payload, 60))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let shard_count = inode_shard_count
            .checked_add(entry_shard_count)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if shard_count
            .checked_mul(NAMESPACE_GRAPH_SHARD_REF_BYTES_V2)
            .and_then(|length| length.checked_add(shard_refs_offset))
            != Some(payload.len())
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let inode_count = get_u64(payload, 40);
        let entry_count = get_u64(payload, 48);
        let mut shards = Vec::with_capacity(shard_count);
        let mut inode_records = 0_u64;
        let mut entry_records = 0_u64;
        for index in 0..shard_count {
            let start = shard_refs_offset + index * NAMESPACE_GRAPH_SHARD_REF_BYTES_V2;
            let record = &payload[start..start + NAMESPACE_GRAPH_SHARD_REF_BYTES_V2];
            let kind = record[0];
            let record_count = get_u32(record, 4);
            let expected_kind = if index < inode_shard_count {
                SHARD_KIND_INODE
            } else {
                SHARD_KIND_ENTRY
            };
            if kind != expected_kind
                || record_count == 0
                || usize::try_from(record_count)
                    .map_or(true, |count| count > NAMESPACE_SHARD_MAX_RECORDS)
                || record[1..4].iter().any(|byte| *byte != 0)
                || get_u32(record, 20) != 0
                || (kind == SHARD_KIND_INODE && get_u32(record, 16) != 0)
            {
                return Err(MetadataFormatError::InvalidPayload);
            }
            if kind == SHARD_KIND_INODE {
                inode_records = inode_records
                    .checked_add(u64::from(record_count))
                    .ok_or(MetadataFormatError::ArithmeticOverflow)?;
            } else {
                entry_records = entry_records
                    .checked_add(u64::from(record_count))
                    .ok_or(MetadataFormatError::ArithmeticOverflow)?;
            }
            let mut object_id = [0_u8; 32];
            object_id.copy_from_slice(&record[24..56]);
            shards.push(NamespaceShardRef {
                kind,
                record_count,
                first_key: get_u64(record, 8),
                first_name_length: get_u32(record, 16),
                object_id: MetadataObjectId::new(object_id)
                    .ok_or(MetadataFormatError::InvalidPayload)?,
            });
        }
        if inode_records != inode_count || entry_records != entry_count {
            return Err(MetadataFormatError::InvalidPayload);
        }
        Ok(Self {
            inode_reservation_end: get_u64(payload, 16),
            inode_allocation_cursor: get_u64(payload, 24),
            namespace_mutation_sequence: get_u64(payload, 32),
            inode_count,
            entry_count,
            root_metadata,
            shards,
        })
    }

    #[must_use]
    pub fn shards(&self) -> &[NamespaceShardRef] {
        &self.shards
    }

    #[must_use]
    pub const fn inode_reservation_end(&self) -> u64 {
        self.inode_reservation_end
    }

    #[must_use]
    pub const fn inode_allocation_cursor(&self) -> u64 {
        self.inode_allocation_cursor
    }

    #[must_use]
    pub const fn namespace_mutation_sequence(&self) -> u64 {
        self.namespace_mutation_sequence
    }

    /// Reconstructs the bounded graph payload and extracts only the child
    /// Metadata identities needed by exact Metadata marking.
    ///
    /// # Errors
    ///
    /// Rejects a missing, extra, reordered, substituted, corrupt, or
    /// non-contiguous shard, plus malformed inode records. This intentionally
    /// does not decode directory, xattr, or POSIX records because they cannot
    /// contain Metadata Object references.
    ///
    /// # Panics
    ///
    /// This method never panics.
    pub fn decode_gc_graph_with_shards<B: Borrow<Vec<u8>>>(
        &self,
        encoded_shards: &BTreeMap<MetadataObjectId, B>,
    ) -> Result<NamespaceGcGraph, MetadataFormatError> {
        let expected_ids = self
            .shards
            .iter()
            .map(|reference| reference.object_id)
            .collect::<BTreeSet<_>>();
        if encoded_shards.keys().copied().collect::<BTreeSet<_>>() != expected_ids {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let mut inode_transitions = Vec::new();
        let mut manifest_roots = Vec::new();
        let mut previous = 0_u64;
        for reference in self.shards.iter().filter(|r| r.kind == SHARD_KIND_INODE) {
            let encoded = encoded_shards
                .get(&reference.object_id)
                .ok_or(MetadataFormatError::InvalidPayload)?;
            let shard = decode_inode_shard(encoded.borrow())?;
            if u32::try_from(shard.len()) != Ok(reference.record_count)
                || shard[0].inode != reference.first_key
            {
                return Err(MetadataFormatError::InvalidPayload);
            }
            for inode in shard {
                if inode.inode <= previous {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                previous = inode.inode;
                inode_transitions.push((inode.inode, inode.mutation_sequence));
                if let Some(manifest_root) = inode.manifest_root {
                    manifest_roots.push(manifest_root);
                }
            }
        }
        if u64::try_from(inode_transitions.len()) != Ok(self.inode_count) {
            return Err(MetadataFormatError::InvalidPayload);
        }
        manifest_roots.sort_unstable();
        manifest_roots.dedup();
        Ok(NamespaceGcGraph {
            inode_reservation_end: self.inode_reservation_end,
            inode_allocation_cursor: self.inode_allocation_cursor,
            namespace_mutation_sequence: self.namespace_mutation_sequence,
            namespace_object_ids: self
                .shards
                .iter()
                .map(|reference| reference.object_id)
                .collect(),
            inode_transitions,
            manifest_roots,
        })
    }
}

/// Encodes the Namespace as record-range shards plus one graph root.
///
/// Shard boundaries come from the record keys alone, so two generations that
/// share a key range publish the identical shard object. That is what makes a
/// commit cost the change rather than the Namespace: every untouched shard
/// resolves to an already published Metadata Object.
fn encode_namespace_graph(
    root: &NamespaceRoot,
) -> Result<EncodedNamespaceGraph, MetadataFormatError> {
    let inode_ranges = shard_ranges(
        root.inodes.len(),
        |ordinal| inode_shard_key_hash(root.inodes[ordinal].inode),
        |ordinal| inode_shard_record_bytes(&root.inodes[ordinal]),
    );
    let entry_ranges = shard_ranges(
        root.entries.len(),
        |ordinal| {
            entry_shard_key_hash(
                root.entries[ordinal].parent_inode,
                &root.entries[ordinal].name,
            )
        },
        |ordinal| {
            entry_record_length(root.entries[ordinal].name.len())
                .unwrap_or(NAMESPACE_ENTRY_HEADER_BYTES)
        },
    );

    // Each shard is an independent envelope over a disjoint record range, so
    // its checksum and BLAKE3 identity are computed on the shared worker pool.
    let encoded_inodes = inode_ranges
        .par_iter()
        .map(|range| {
            let bytes = encode_inode_shard(&root.inodes[range.clone()])?;
            let object_id = MetadataObjectId::from_encoded(&bytes)?;
            Ok((object_id, bytes))
        })
        .collect::<Result<Vec<_>, MetadataFormatError>>()?;
    let encoded_entries = entry_ranges
        .par_iter()
        .map(|range| {
            let bytes = encode_entry_shard(&root.entries[range.clone()])?;
            let object_id = MetadataObjectId::from_encoded(&bytes)?;
            Ok((object_id, bytes))
        })
        .collect::<Result<Vec<_>, MetadataFormatError>>()?;

    let mut refs = Vec::with_capacity(inode_ranges.len() + entry_ranges.len());
    // Shards are emitted in graph order, not by identity. Publication is
    // sequential, so ordering by a content hash would let one shard that
    // changes every generation reorder itself around one that never does, and
    // the durable operation sequence would stop being a function of the work.
    let mut shards = Vec::with_capacity(inode_ranges.len() + entry_ranges.len());
    let mut published = BTreeSet::new();
    for (range, (object_id, bytes)) in inode_ranges.iter().zip(encoded_inodes) {
        refs.push(NamespaceShardRef {
            kind: SHARD_KIND_INODE,
            record_count: u32::try_from(range.len())
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
            first_key: root.inodes[range.start].inode,
            first_name_length: 0,
            object_id,
        });
        if published.insert(object_id) {
            shards.push(EncodedNamespaceShard { object_id, bytes });
        }
    }
    for (range, (object_id, bytes)) in entry_ranges.iter().zip(encoded_entries) {
        let first = &root.entries[range.start];
        refs.push(NamespaceShardRef {
            kind: SHARD_KIND_ENTRY,
            record_count: u32::try_from(range.len())
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
            first_key: first.parent_inode,
            first_name_length: u32::try_from(first.name.len())
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
            object_id,
        });
        if published.insert(object_id) {
            shards.push(EncodedNamespaceShard { object_id, bytes });
        }
    }
    let encoded_root = encode_namespace_graph_root(root, &refs)?;
    Ok(EncodedNamespaceGraph {
        root: encoded_root,
        shards,
    })
}

fn decode_inodes(
    payload: &[u8],
    inode_count: usize,
) -> Result<Vec<DurableInode>, MetadataFormatError> {
    decode_inode_records(payload, NAMESPACE_ROOT_HEADER_BYTES, inode_count)
}

fn decode_inode_records(
    payload: &[u8],
    base_offset: usize,
    inode_count: usize,
) -> Result<Vec<DurableInode>, MetadataFormatError> {
    let mut inodes = Vec::with_capacity(inode_count);
    for ordinal in 0..inode_count {
        let start = base_offset
            .checked_add(
                ordinal
                    .checked_mul(DURABLE_INODE_BYTES)
                    .ok_or(MetadataFormatError::ArithmeticOverflow)?,
            )
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let record = &payload[start..start + DURABLE_INODE_BYTES];
        let kind = match get_u16(record, 10) {
            1 => DurableInodeKind::Regular,
            2 => DurableInodeKind::Directory,
            3 => DurableInodeKind::Symlink,
            _ => return Err(MetadataFormatError::InvalidPayload),
        };
        if record[76..].iter().any(|byte| *byte != 0) {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let file_flags = get_u32(record, 72);
        validate_file_flags(file_flags)?;
        let mut manifest_root = [0_u8; 32];
        manifest_root.copy_from_slice(&record[40..72]);
        let inode = match kind {
            DurableInodeKind::Regular => DurableInode::new_with_metadata(
                get_u64(record, 0),
                get_u16(record, 8),
                get_u32(record, 12),
                get_u32(record, 16),
                get_u32(record, 20),
                get_u64(record, 24),
                get_u64(record, 32),
                MetadataObjectId::new(manifest_root).ok_or(MetadataFormatError::InvalidPayload)?,
                file_flags,
                Vec::new(),
            )?,
            DurableInodeKind::Directory => {
                if manifest_root != [0; 32] || get_u64(record, 32) != 0 {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                DurableInode::new_directory_with_metadata(
                    get_u64(record, 0),
                    get_u16(record, 8),
                    get_u32(record, 12),
                    get_u32(record, 16),
                    get_u32(record, 20),
                    get_u64(record, 24),
                    file_flags,
                    Vec::new(),
                )?
            }
            DurableInodeKind::Symlink => {
                if manifest_root != [0; 32] || get_u64(record, 32) == 0 || file_flags != 0 {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                DurableInode {
                    inode: get_u64(record, 0),
                    kind,
                    mode: get_u16(record, 8),
                    uid: get_u32(record, 12),
                    gid: get_u32(record, 16),
                    link_count: get_u32(record, 20),
                    mutation_sequence: get_u64(record, 24),
                    logical_size: get_u64(record, 32),
                    manifest_root: None,
                    file_flags: 0,
                    xattrs: Vec::new(),
                    times: DurableTimes::default(),
                    symlink_target: None,
                }
            }
        };
        inodes.push(inode);
    }
    Ok(inodes)
}

fn decode_entries(
    payload: &[u8],
    entries_offset: usize,
    entries_end: usize,
    entry_count: usize,
) -> Result<Vec<NamespaceEntry>, MetadataFormatError> {
    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = entries_offset;
    for _ in 0..entry_count {
        let header_end = cursor
            .checked_add(NAMESPACE_ENTRY_HEADER_BYTES)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if header_end > entries_end {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let header = &payload[cursor..header_end];
        let record_length = usize::try_from(get_u32(header, 0))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let name_length = usize::from(get_u16(header, 4));
        let expected_length = entry_record_length(name_length)?;
        let end = cursor
            .checked_add(record_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let name_end = header_end
            .checked_add(name_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if record_length != expected_length
            || end > entries_end
            || name_end > end
            || get_u16(header, 6) != 0
            || payload[name_end..end].iter().any(|byte| *byte != 0)
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        entries.push(NamespaceEntry::new(
            get_u64(header, 8),
            get_u64(header, 16),
            payload[header_end..name_end].to_vec(),
        )?);
        cursor = end;
    }
    if cursor != entries_end {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(entries)
}

fn validate_namespace(
    inode_reservation_end: u64,
    inode_allocation_cursor: u64,
    inodes: &[DurableInode],
    entries: &[NamespaceEntry],
) -> Result<(), MetadataFormatError> {
    if inode_reservation_end < 2
        || inode_allocation_cursor < 2
        || inode_allocation_cursor > inode_reservation_end
        || inodes
            .last()
            .is_some_and(|inode| inode.inode >= inode_allocation_cursor)
        || inodes.windows(2).any(|pair| pair[0].inode >= pair[1].inode)
        || entries.windows(2).any(|pair| {
            (pair[0].parent_inode, pair[0].name.as_slice())
                >= (pair[1].parent_inode, pair[1].name.as_slice())
        })
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    // Both inputs were just proven strictly ordered, so every lookup below is a
    // binary search over them and every per-inode tally is one slot in a vector
    // indexed by that search. This runs once per published generation over the
    // whole Namespace; building maps and cloning names here would allocate once
    // per entry for evidence that is discarded at the end of the proof.
    let mut observed_links = vec![0_u32; inodes.len()];
    let mut directory_children = vec![0_u32; inodes.len()];
    for entry in entries {
        validate_entry_fields(entry.parent_inode, entry.target_inode, &entry.name)?;
        let parent_index = if entry.parent_inode == ROOT_INODE {
            None
        } else {
            let index = inode_index(inodes, entry.parent_inode)
                .ok_or(MetadataFormatError::InvalidPayload)?;
            if inodes[index].kind != DurableInodeKind::Directory {
                return Err(MetadataFormatError::InvalidPayload);
            }
            Some(index)
        };
        let target_index =
            inode_index(inodes, entry.target_inode).ok_or(MetadataFormatError::InvalidPayload)?;
        observed_links[target_index] = observed_links[target_index]
            .checked_add(1)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        // The Namespace root is never one of `inodes`, so its own child tally
        // has no reader and is not kept.
        if inodes[target_index].kind == DurableInodeKind::Directory
            && let Some(parent_index) = parent_index
        {
            directory_children[parent_index] = directory_children[parent_index]
                .checked_add(1)
                .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        }
    }
    for (index, inode) in inodes.iter().enumerate() {
        let incoming = observed_links[index];
        match inode.kind {
            DurableInodeKind::Regular | DurableInodeKind::Symlink
                if incoming != inode.link_count =>
            {
                return Err(MetadataFormatError::InvalidPayload);
            }
            DurableInodeKind::Directory => {
                let expected_links = 2_u32
                    .checked_add(directory_children[index])
                    .ok_or(MetadataFormatError::ArithmeticOverflow)?;
                if incoming != 1 || inode.link_count != expected_links {
                    return Err(MetadataFormatError::InvalidPayload);
                }
            }
            DurableInodeKind::Regular | DurableInodeKind::Symlink => {}
        }
    }

    // Entries are ordered by parent first, so one directory's children are a
    // contiguous run that the walk locates without a child map.
    let mut reachable = vec![false; inodes.len()];
    let mut reachable_count = 0_usize;
    let mut pending = vec![ROOT_INODE];
    while let Some(parent) = pending.pop() {
        let first = entries.partition_point(|entry| entry.parent_inode < parent);
        for entry in entries[first..]
            .iter()
            .take_while(|entry| entry.parent_inode == parent)
        {
            let index = inode_index(inodes, entry.target_inode)
                .ok_or(MetadataFormatError::InvalidPayload)?;
            let is_directory = inodes[index].kind == DurableInodeKind::Directory;
            if std::mem::replace(&mut reachable[index], true) {
                if is_directory {
                    return Err(MetadataFormatError::InvalidPayload);
                }
            } else {
                reachable_count += 1;
            }
            if is_directory {
                pending.push(entry.target_inode);
            }
        }
    }
    if reachable_count != inodes.len() {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(())
}

fn inode_index(inodes: &[DurableInode], inode: u64) -> Option<usize> {
    inodes
        .binary_search_by_key(&inode, |candidate| candidate.inode)
        .ok()
}

fn validate_entry_fields(
    parent_inode: u64,
    target_inode: u64,
    name: &[u8],
) -> Result<(), MetadataFormatError> {
    if parent_inode == 0
        || target_inode <= ROOT_INODE
        || name.is_empty()
        || name.len() > MAX_NAME_BYTES
        || name == b"."
        || name == b".."
        || name.contains(&0)
        || name.contains(&b'/')
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(())
}

fn payload_length(
    root_metadata: &DurableRootMetadata,
    entries: &[NamespaceEntry],
    inodes: &[DurableInode],
) -> Result<usize, MetadataFormatError> {
    let inode_bytes = inodes
        .len()
        .checked_mul(DURABLE_INODE_BYTES)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let entries_end = entries.iter().try_fold(
        NAMESPACE_ROOT_HEADER_BYTES
            .checked_add(inode_bytes)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?,
        |length, entry| {
            length
                .checked_add(entry_record_length(entry.name.len())?)
                .ok_or(MetadataFormatError::ArithmeticOverflow)
        },
    )?;
    let mut length = root_metadata
        .xattrs
        .iter()
        .try_fold(entries_end, |length, xattr| {
            length
                .checked_add(xattr_record_length(xattr.name.len(), xattr.value.len())?)
                .ok_or(MetadataFormatError::ArithmeticOverflow)
        })?;
    for inode in inodes {
        length = inode.xattrs.iter().try_fold(length, |length, xattr| {
            length
                .checked_add(xattr_record_length(xattr.name.len(), xattr.value.len())?)
                .ok_or(MetadataFormatError::ArithmeticOverflow)
        })?;
    }
    length = length
        .checked_add(posix_metadata_record_length(0)?)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    for inode in inodes {
        length = length
            .checked_add(posix_metadata_record_length(
                inode.symlink_target.as_ref().map_or(0, Vec::len),
            )?)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    }
    Ok(length)
}

fn xattr_record_length(
    name_length: usize,
    value_length: usize,
) -> Result<usize, MetadataFormatError> {
    let unaligned = XATTR_RECORD_HEADER_BYTES
        .checked_add(name_length)
        .and_then(|length| length.checked_add(value_length))
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let mask = XATTR_RECORD_ALIGNMENT - 1;
    unaligned
        .checked_add(mask)
        .map(|candidate| candidate & !mask)
        .ok_or(MetadataFormatError::ArithmeticOverflow)
}

fn posix_metadata_record_length(target_length: usize) -> Result<usize, MetadataFormatError> {
    let unaligned = POSIX_METADATA_RECORD_HEADER_BYTES
        .checked_add(target_length)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    unaligned
        .checked_add(7)
        .map(|length| length & !7)
        .ok_or(MetadataFormatError::ArithmeticOverflow)
}

fn encode_posix_metadata_record(
    payload: &mut [u8],
    cursor: usize,
    inode: u64,
    times: DurableTimes,
    target: Option<&[u8]>,
) -> Result<usize, MetadataFormatError> {
    let target = target.unwrap_or_default();
    let record_length = posix_metadata_record_length(target.len())?;
    let end = cursor
        .checked_add(record_length)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let record = &mut payload[cursor..end];
    put_u32(
        record,
        0,
        u32::try_from(record_length).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u16(
        record,
        4,
        u16::try_from(target.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u16(record, 6, u16::from(!target.is_empty()));
    put_u64(record, 8, inode);
    encode_timestamp(record, 16, times.atime);
    encode_timestamp(record, 32, times.mtime);
    encode_timestamp(record, 48, times.ctime);
    record[64..64 + target.len()].copy_from_slice(target);
    Ok(end)
}

fn encode_timestamp(record: &mut [u8], offset: usize, time: DurableTimestamp) {
    record[offset..offset + 8].copy_from_slice(&time.seconds.to_le_bytes());
    put_u32(record, offset + 8, time.nanoseconds);
}

fn encode_xattr_record(
    payload: &mut [u8],
    cursor: usize,
    inode: u64,
    xattr: &DurableXattr,
) -> Result<usize, MetadataFormatError> {
    let record_length = xattr_record_length(xattr.name.len(), xattr.value.len())?;
    let end = cursor
        .checked_add(record_length)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let record = &mut payload[cursor..end];
    put_u32(
        record,
        0,
        u32::try_from(record_length).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u16(
        record,
        4,
        u16::try_from(xattr.name.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(record, 8, inode);
    put_u32(
        record,
        16,
        u32::try_from(xattr.value.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    let name_start = XATTR_RECORD_HEADER_BYTES;
    let value_start = name_start + xattr.name.len();
    record[name_start..value_start].copy_from_slice(&xattr.name);
    record[value_start..value_start + xattr.value.len()].copy_from_slice(&xattr.value);
    Ok(end)
}

fn decode_posix_metadata(
    payload: &[u8],
    offset: usize,
    count: usize,
    root: &mut DurableRootMetadata,
    inodes: &mut [DurableInode],
) -> Result<(), MetadataFormatError> {
    if count != inodes.len() + 1 {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let mut cursor = offset;
    let mut previous = 0_u64;
    for ordinal in 0..count {
        let header_end = cursor
            .checked_add(POSIX_METADATA_RECORD_HEADER_BYTES)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if header_end > payload.len() {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let header = &payload[cursor..header_end];
        let record_length = usize::try_from(get_u32(header, 0))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let target_length = usize::from(get_u16(header, 4));
        let flags = get_u16(header, 6);
        let inode = get_u64(header, 8);
        let expected = posix_metadata_record_length(target_length)?;
        let end = cursor
            .checked_add(record_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let target_end = header_end
            .checked_add(target_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if record_length != expected
            || end > payload.len()
            || target_end > end
            || flags != u16::from(target_length != 0)
            || header[28..32]
                .iter()
                .chain(&header[44..48])
                .chain(&header[60..64])
                .any(|byte| *byte != 0)
            || payload[target_end..end].iter().any(|byte| *byte != 0)
            || (ordinal == 0 && inode != ROOT_INODE)
            || (ordinal != 0 && inode <= previous)
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let times = DurableTimes {
            atime: decode_timestamp(header, 16)?,
            mtime: decode_timestamp(header, 32)?,
            ctime: decode_timestamp(header, 48)?,
        };
        if inode == ROOT_INODE {
            if target_length != 0 {
                return Err(MetadataFormatError::InvalidPayload);
            }
            root.times = times;
        } else {
            let item = inodes
                .binary_search_by_key(&inode, DurableInode::inode)
                .map_err(|_| MetadataFormatError::InvalidPayload)?;
            let durable = &mut inodes[item];
            if durable.kind == DurableInodeKind::Symlink {
                let target = payload[header_end..target_end].to_vec();
                if target.is_empty()
                    || target.len() > 4_096
                    || durable.logical_size != u64::try_from(target.len()).unwrap_or(u64::MAX)
                    || durable.mode != 0o777
                    || durable.link_count == 0
                {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                durable.symlink_target = Some(target);
            } else if target_length != 0 {
                return Err(MetadataFormatError::InvalidPayload);
            }
            durable.times = times;
        }
        previous = inode;
        cursor = end;
    }
    if cursor != payload.len() {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(())
}

fn decode_timestamp(record: &[u8], offset: usize) -> Result<DurableTimestamp, MetadataFormatError> {
    let nanoseconds = get_u32(record, offset + 8);
    if nanoseconds >= 1_000_000_000 {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(DurableTimestamp {
        seconds: i64::from_le_bytes(
            record[offset..offset + 8]
                .try_into()
                .expect("fixed i64 field"),
        ),
        nanoseconds,
    })
}

fn decode_xattrs(
    payload: &[u8],
    xattrs_offset: usize,
    xattrs_end: usize,
    xattr_count: usize,
) -> Result<Vec<(u64, DurableXattr)>, MetadataFormatError> {
    let remaining = xattrs_end.saturating_sub(xattrs_offset);
    if xattr_count > remaining / XATTR_RECORD_HEADER_BYTES {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let mut decoded = Vec::with_capacity(xattr_count);
    let mut cursor = xattrs_offset;
    for _ in 0..xattr_count {
        let header_end = cursor
            .checked_add(XATTR_RECORD_HEADER_BYTES)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if header_end > xattrs_end {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let header = &payload[cursor..header_end];
        let record_length = usize::try_from(get_u32(header, 0))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let name_length = usize::from(get_u16(header, 4));
        let value_length = usize::try_from(get_u32(header, 16))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let expected = xattr_record_length(name_length, value_length)?;
        let end = cursor
            .checked_add(record_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let name_start = header_end;
        let value_start = name_start
            .checked_add(name_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let value_end = value_start
            .checked_add(value_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if record_length != expected
            || end > xattrs_end
            || value_end > end
            || get_u16(header, 6) != 0
            || get_u32(header, 20) != 0
            || payload[value_end..end].iter().any(|byte| *byte != 0)
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let inode = get_u64(header, 8);
        decoded.push((
            inode,
            DurableXattr::new(
                payload[name_start..value_start].to_vec(),
                payload[value_start..value_end].to_vec(),
            )?,
        ));
        cursor = end;
    }
    if cursor != xattrs_end
        || decoded.windows(2).any(|pair| {
            (pair[0].0, pair[0].1.name.as_slice()) >= (pair[1].0, pair[1].1.name.as_slice())
        })
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(decoded)
}

fn install_decoded_xattrs(
    root_metadata: &mut DurableRootMetadata,
    inodes: &mut [DurableInode],
    decoded: Vec<(u64, DurableXattr)>,
) -> Result<(), MetadataFormatError> {
    for (inode, xattr) in decoded {
        if inode == ROOT_INODE {
            root_metadata.xattrs.push(xattr);
            continue;
        }
        let ordinal = inodes
            .binary_search_by_key(&inode, DurableInode::inode)
            .map_err(|_| MetadataFormatError::InvalidPayload)?;
        inodes[ordinal].xattrs.push(xattr);
    }
    root_metadata.xattrs = canonical_xattrs(
        DurableInodeKind::Directory,
        std::mem::take(&mut root_metadata.xattrs),
    )?;
    for inode in inodes {
        inode.xattrs = canonical_xattrs(inode.kind, std::mem::take(&mut inode.xattrs))?;
    }
    Ok(())
}

fn canonical_xattrs(
    kind: DurableInodeKind,
    mut xattrs: Vec<DurableXattr>,
) -> Result<Vec<DurableXattr>, MetadataFormatError> {
    xattrs.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    if xattrs.len() > MAXIMUM_XATTRS_PER_INODE
        || xattrs.windows(2).any(|pair| pair[0].name == pair[1].name)
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let mut bytes = 0_usize;
    for xattr in &xattrs {
        validate_xattr_name(&xattr.name)?;
        validate_xattr_value(kind, &xattr.name, &xattr.value)?;
        bytes = bytes
            .checked_add(xattr.name.len())
            .and_then(|total| total.checked_add(xattr.value.len()))
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if bytes > MAXIMUM_XATTR_BYTES_PER_INODE {
            return Err(MetadataFormatError::InvalidPayload);
        }
    }
    Ok(xattrs)
}

fn validate_file_flags(flags: u32) -> Result<(), MetadataFormatError> {
    if flags & !FS_IMMUTABLE_FL != 0 {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(())
}

fn validate_xattr_name(name: &[u8]) -> Result<(), MetadataFormatError> {
    if name.is_empty()
        || name.len() > MAXIMUM_XATTR_NAME_BYTES
        || name.contains(&0)
        || !(name.starts_with(b"user.")
            || name.starts_with(b"trusted.")
            || name.starts_with(b"security.")
            || name == POSIX_ACL_ACCESS_XATTR
            || name == POSIX_ACL_DEFAULT_XATTR)
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(())
}

fn validate_xattr_value(
    kind: DurableInodeKind,
    name: &[u8],
    value: &[u8],
) -> Result<(), MetadataFormatError> {
    if value.len() > MAXIMUM_XATTR_VALUE_BYTES
        || (name == POSIX_ACL_DEFAULT_XATTR && kind != DurableInodeKind::Directory)
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    if name == POSIX_ACL_ACCESS_XATTR || name == POSIX_ACL_DEFAULT_XATTR {
        validate_acl(value)?;
    }
    Ok(())
}

fn validate_acl(value: &[u8]) -> Result<(), MetadataFormatError> {
    const ACL_VERSION: u32 = 2;
    const ACL_USER_OBJ: u16 = 0x01;
    const ACL_USER: u16 = 0x02;
    const ACL_GROUP_OBJ: u16 = 0x04;
    const ACL_GROUP: u16 = 0x08;
    const ACL_MASK: u16 = 0x10;
    const ACL_OTHER: u16 = 0x20;
    if value.len() < 4 || !(value.len() - 4).is_multiple_of(8) || get_u32(value, 0) != ACL_VERSION {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let mut singleton_tags = BTreeSet::new();
    let mut named = BTreeSet::new();
    let mut has_named = false;
    let mut previous_order = None;
    for entry in value[4..].chunks_exact(8) {
        let tag = get_u16(entry, 0);
        let permissions = get_u16(entry, 2);
        let id = get_u32(entry, 4);
        if permissions & !0o7 != 0 {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let order = match tag {
            ACL_USER_OBJ | ACL_GROUP_OBJ | ACL_MASK | ACL_OTHER if id == u32::MAX => {
                if !singleton_tags.insert(tag) {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                match tag {
                    ACL_USER_OBJ => (0, 0),
                    ACL_GROUP_OBJ => (2, 0),
                    ACL_MASK => (4, 0),
                    ACL_OTHER => (5, 0),
                    _ => unreachable!(),
                }
            }
            ACL_USER | ACL_GROUP if id != u32::MAX => {
                has_named = true;
                if !named.insert((tag, id)) {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                if tag == ACL_USER { (1, id) } else { (3, id) }
            }
            _ => return Err(MetadataFormatError::InvalidPayload),
        };
        if previous_order.is_some_and(|previous| previous >= order) {
            return Err(MetadataFormatError::InvalidPayload);
        }
        previous_order = Some(order);
    }
    if !singleton_tags.contains(&ACL_USER_OBJ)
        || !singleton_tags.contains(&ACL_GROUP_OBJ)
        || !singleton_tags.contains(&ACL_OTHER)
        || (has_named && !singleton_tags.contains(&ACL_MASK))
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(())
}

fn entry_record_length(name_length: usize) -> Result<usize, MetadataFormatError> {
    let unaligned = NAMESPACE_ENTRY_HEADER_BYTES
        .checked_add(name_length)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let mask = NAMESPACE_ENTRY_ALIGNMENT - 1;
    unaligned
        .checked_add(mask)
        .map(|candidate| candidate & !mask)
        .ok_or(MetadataFormatError::ArithmeticOverflow)
}

// ---------------------------------------------------------------------------
// Record-range Namespace shards (ADR 0095)
//
// The graph used to be one canonical payload cut by FastCDC. Producing that
// payload is O(Namespace) on every commit no matter how little changed, which
// is the cost ADR 0095 set out to remove. Shards now carry records, not byte
// ranges, and their boundaries are a deterministic function of the record keys
// alone. A commit re-encodes only the shards whose key range contains a change
// and republishes every other shard by identity.
// ---------------------------------------------------------------------------

const INODE_SHARD_MAGIC: &[u8; 8] = b"FDNSIS02";
const ENTRY_SHARD_MAGIC: &[u8; 8] = b"FDNSES02";
const INODE_SHARD_HEADER_BYTES: usize = 48;
const ENTRY_SHARD_HEADER_BYTES: usize = 32;
const NAMESPACE_SHARD_VERSION_V2: u16 = 2;

/// Expected records per shard. The boundary predicate accepts one key in this
/// many, so shards average this size and a single changed record re-encodes
/// about this much work.
const NAMESPACE_SHARD_TARGET_RECORDS: u64 = 1_024;
/// Hard bound on one shard, so a boundary-free run cannot produce an object
/// that breaks the Metadata Object size limit.
const NAMESPACE_SHARD_MAX_RECORDS: usize = 8_192;
/// Byte bound on one shard. Records are not uniform — one inode may carry a
/// mebibyte of extended attributes — so the record count alone cannot keep a
/// shard inside the Metadata Object limit.
const NAMESPACE_SHARD_MAX_PAYLOAD_BYTES: usize = 4 * 1_024 * 1_024;

/// Mixes one 64-bit key into the value the boundary predicate reads.
///
/// This is `splitmix64`. It is not a security primitive: shard identity and
/// authentication come from the Metadata Object id. It only has to be stable
/// across releases and well distributed, because the partition it induces is
/// part of the durable format.
const fn namespace_key_mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

fn inode_shard_key_hash(inode: u64) -> u64 {
    namespace_key_mix(inode)
}

fn entry_shard_key_hash(parent: u64, name: &[u8]) -> u64 {
    let mut folded = namespace_key_mix(parent);
    for byte in name {
        folded = namespace_key_mix(folded ^ u64::from(*byte));
    }
    folded
}

/// Reports whether a record with this key hash starts a new shard.
fn starts_shard(key_hash: u64) -> bool {
    key_hash < u64::MAX / NAMESPACE_SHARD_TARGET_RECORDS
}

/// Splits `count` sorted records into shard ranges.
///
/// A record starts a new shard when its own key says so, so the partition
/// depends on the record set and never on history or position. Inserting one
/// record therefore disturbs exactly the shard it lands in. The record cap is
/// the only positional rule and only engages on a run with no boundary key.
fn shard_ranges(
    count: usize,
    key_hash: impl Fn(usize) -> u64,
    record_bytes: impl Fn(usize) -> usize,
) -> Vec<Range<usize>> {
    if count == 0 {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut bytes = record_bytes(0);
    for ordinal in 1..count {
        let record = record_bytes(ordinal);
        let full = ordinal - start >= NAMESPACE_SHARD_MAX_RECORDS
            || bytes.saturating_add(record) > NAMESPACE_SHARD_MAX_PAYLOAD_BYTES;
        if full || starts_shard(key_hash(ordinal)) {
            ranges.push(start..ordinal);
            start = ordinal;
            bytes = record;
        } else {
            bytes = bytes.saturating_add(record);
        }
    }
    ranges.push(start..count);
    ranges
}

/// Encoded size of one inode inside a shard, used only to bound shard bytes.
fn inode_shard_record_bytes(inode: &DurableInode) -> usize {
    let mut bytes = DURABLE_INODE_BYTES
        + posix_metadata_record_length(inode.symlink_target.as_ref().map_or(0, Vec::len))
            .unwrap_or(POSIX_METADATA_RECORD_HEADER_BYTES);
    for xattr in &inode.xattrs {
        bytes = bytes.saturating_add(
            xattr_record_length(xattr.name.len(), xattr.value.len())
                .unwrap_or(XATTR_RECORD_HEADER_BYTES),
        );
    }
    bytes
}

fn encode_inode_shard(inodes: &[DurableInode]) -> Result<Vec<u8>, MetadataFormatError> {
    if inodes.is_empty() {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let fixed_bytes = inodes
        .len()
        .checked_mul(DURABLE_INODE_BYTES)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let xattrs_offset = INODE_SHARD_HEADER_BYTES
        .checked_add(fixed_bytes)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let mut xattr_count = 0_usize;
    let mut posix_offset = xattrs_offset;
    for inode in inodes {
        xattr_count = xattr_count
            .checked_add(inode.xattrs.len())
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        for xattr in &inode.xattrs {
            posix_offset = posix_offset
                .checked_add(xattr_record_length(xattr.name.len(), xattr.value.len())?)
                .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        }
    }
    let mut payload_length = posix_offset;
    for inode in inodes {
        payload_length = payload_length
            .checked_add(posix_metadata_record_length(
                inode.symlink_target.as_ref().map_or(0, Vec::len),
            )?)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    }

    let mut payload = vec![0_u8; payload_length];
    payload[0..8].copy_from_slice(INODE_SHARD_MAGIC);
    put_u16(&mut payload, 8, NAMESPACE_SHARD_VERSION_V2);
    put_u16(
        &mut payload,
        10,
        u16::try_from(INODE_SHARD_HEADER_BYTES).expect("ASSERT: shard header size fits u16"),
    );
    put_u16(
        &mut payload,
        12,
        u16::try_from(DURABLE_INODE_BYTES).expect("ASSERT: inode record size fits u16"),
    );
    put_u16(
        &mut payload,
        14,
        u16::try_from(XATTR_RECORD_HEADER_BYTES).expect("ASSERT: xattr header size fits u16"),
    );
    put_u32(
        &mut payload,
        16,
        u32::try_from(inodes.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u32(
        &mut payload,
        20,
        u32::try_from(xattr_count).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u32(
        &mut payload,
        24,
        u32::try_from(inodes.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        &mut payload,
        32,
        u64::try_from(xattrs_offset).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        &mut payload,
        40,
        u64::try_from(posix_offset).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    for (ordinal, inode) in inodes.iter().enumerate() {
        let start = INODE_SHARD_HEADER_BYTES + ordinal * DURABLE_INODE_BYTES;
        encode_inode_fixed_record(&mut payload[start..start + DURABLE_INODE_BYTES], inode);
    }
    let mut cursor = xattrs_offset;
    for inode in inodes {
        for xattr in &inode.xattrs {
            cursor = encode_xattr_record(&mut payload, cursor, inode.inode, xattr)?;
        }
    }
    assert_eq!(cursor, posix_offset, "ASSERT: shard xattr section is exact");
    for inode in inodes {
        cursor = encode_posix_metadata_record(
            &mut payload,
            cursor,
            inode.inode,
            inode.times,
            inode.symlink_target.as_deref(),
        )?;
    }
    assert_eq!(
        cursor, payload_length,
        "ASSERT: shard preflight matches the encoder cursor"
    );
    encode_metadata_object(NAMESPACE_ROOT_KIND, &payload)
}

fn encode_inode_fixed_record(record: &mut [u8], inode: &DurableInode) {
    put_u64(record, 0, inode.inode);
    put_u16(record, 8, inode.mode);
    put_u16(
        record,
        10,
        match inode.kind {
            DurableInodeKind::Regular => 1,
            DurableInodeKind::Directory => 2,
            DurableInodeKind::Symlink => 3,
        },
    );
    put_u32(record, 12, inode.uid);
    put_u32(record, 16, inode.gid);
    put_u32(record, 20, inode.link_count);
    put_u64(record, 24, inode.mutation_sequence);
    put_u64(record, 32, inode.logical_size);
    if let Some(manifest_root) = inode.manifest_root {
        record[40..72].copy_from_slice(&manifest_root.bytes());
    }
    put_u32(record, 72, inode.file_flags);
}

fn encode_entry_shard(entries: &[NamespaceEntry]) -> Result<Vec<u8>, MetadataFormatError> {
    if entries.is_empty() {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let mut payload_length = ENTRY_SHARD_HEADER_BYTES;
    for entry in entries {
        payload_length = payload_length
            .checked_add(entry_record_length(entry.name.len())?)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    }
    let mut payload = vec![0_u8; payload_length];
    payload[0..8].copy_from_slice(ENTRY_SHARD_MAGIC);
    put_u16(&mut payload, 8, NAMESPACE_SHARD_VERSION_V2);
    put_u16(
        &mut payload,
        10,
        u16::try_from(ENTRY_SHARD_HEADER_BYTES).expect("ASSERT: shard header size fits u16"),
    );
    put_u16(
        &mut payload,
        12,
        u16::try_from(NAMESPACE_ENTRY_HEADER_BYTES).expect("ASSERT: entry header size fits u16"),
    );
    put_u32(
        &mut payload,
        16,
        u32::try_from(entries.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        &mut payload,
        24,
        u64::try_from(payload_length).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    let mut cursor = ENTRY_SHARD_HEADER_BYTES;
    for entry in entries {
        let record_length = entry_record_length(entry.name.len())?;
        let end = cursor
            .checked_add(record_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let record = &mut payload[cursor..end];
        put_u32(
            record,
            0,
            u32::try_from(record_length).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u16(
            record,
            4,
            u16::try_from(entry.name.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
        );
        put_u64(record, 8, entry.parent_inode);
        put_u64(record, 16, entry.target_inode);
        record[24..24 + entry.name.len()].copy_from_slice(&entry.name);
        cursor = end;
    }
    assert_eq!(
        cursor, payload_length,
        "ASSERT: entry shard preflight matches the encoder cursor"
    );
    encode_metadata_object(NAMESPACE_ROOT_KIND, &payload)
}

fn decode_inode_shard(encoded: &[u8]) -> Result<Vec<DurableInode>, MetadataFormatError> {
    let object = decode_metadata_object(Some(NAMESPACE_ROOT_KIND), encoded)?;
    let payload = object.payload;
    if payload.len() < INODE_SHARD_HEADER_BYTES
        || &payload[0..8] != INODE_SHARD_MAGIC
        || get_u16(payload, 8) != NAMESPACE_SHARD_VERSION_V2
        || usize::from(get_u16(payload, 10)) != INODE_SHARD_HEADER_BYTES
        || usize::from(get_u16(payload, 12)) != DURABLE_INODE_BYTES
        || usize::from(get_u16(payload, 14)) != XATTR_RECORD_HEADER_BYTES
        || get_u32(payload, 28) != 0
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let inode_count = usize::try_from(get_u32(payload, 16))
        .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
    let xattr_count = usize::try_from(get_u32(payload, 20))
        .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
    if inode_count == 0
        || inode_count > NAMESPACE_SHARD_MAX_RECORDS
        || usize::try_from(get_u32(payload, 24)) != Ok(inode_count)
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let fixed_bytes = inode_count
        .checked_mul(DURABLE_INODE_BYTES)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let expected_xattrs_offset = INODE_SHARD_HEADER_BYTES
        .checked_add(fixed_bytes)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let xattrs_offset = usize::try_from(get_u64(payload, 32))
        .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
    let posix_offset = usize::try_from(get_u64(payload, 40))
        .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
    if xattrs_offset != expected_xattrs_offset
        || posix_offset < xattrs_offset
        || posix_offset > payload.len()
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let mut inodes = decode_inode_records(payload, INODE_SHARD_HEADER_BYTES, inode_count)?;
    if inodes.windows(2).any(|pair| pair[0].inode >= pair[1].inode) {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let decoded = decode_xattrs(payload, xattrs_offset, posix_offset, xattr_count)?;
    install_shard_xattrs(&mut inodes, decoded)?;
    decode_shard_posix_metadata(payload, posix_offset, &mut inodes)?;
    Ok(inodes)
}

/// Installs decoded xattrs into the shard's inodes, which carry no root record.
fn install_shard_xattrs(
    inodes: &mut [DurableInode],
    decoded: Vec<(u64, DurableXattr)>,
) -> Result<(), MetadataFormatError> {
    for (inode_id, xattr) in decoded {
        let index = inode_index(inodes, inode_id).ok_or(MetadataFormatError::InvalidPayload)?;
        inodes[index].xattrs.push(xattr);
    }
    for inode in inodes.iter_mut() {
        let canonical = canonical_xattrs(inode.kind, std::mem::take(&mut inode.xattrs))?;
        inode.xattrs = canonical;
    }
    Ok(())
}

fn decode_shard_posix_metadata(
    payload: &[u8],
    offset: usize,
    inodes: &mut [DurableInode],
) -> Result<(), MetadataFormatError> {
    let mut cursor = offset;
    for inode in inodes.iter_mut() {
        let header_end = cursor
            .checked_add(POSIX_METADATA_RECORD_HEADER_BYTES)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if header_end > payload.len() {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let header = &payload[cursor..header_end];
        let record_length = usize::try_from(get_u32(header, 0))
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
        let target_length = usize::from(get_u16(header, 4));
        let flags = get_u16(header, 6);
        let record_end = cursor
            .checked_add(record_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let target_end = header_end
            .checked_add(target_length)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if record_length != posix_metadata_record_length(target_length)?
            || record_end > payload.len()
            || target_end > record_end
            || flags != u16::from(target_length != 0)
            || get_u64(header, 8) != inode.inode
            || header[28..32]
                .iter()
                .chain(&header[44..48])
                .chain(&header[60..64])
                .any(|byte| *byte != 0)
            || payload[target_end..record_end]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let record = &payload[cursor..record_end];
        let times = DurableTimes {
            atime: decode_timestamp(record, 16)?,
            mtime: decode_timestamp(record, 32)?,
            ctime: decode_timestamp(record, 48)?,
        };
        let target = payload[header_end..target_end].to_vec();
        inode.times = times;
        match inode.kind {
            DurableInodeKind::Symlink => {
                if target_length == 0 || usize::try_from(inode.logical_size) != Ok(target_length) {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                inode.symlink_target = Some(target);
            }
            _ => {
                if target_length != 0 {
                    return Err(MetadataFormatError::InvalidPayload);
                }
            }
        }
        cursor = record_end;
    }
    if cursor != payload.len() {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(())
}

fn decode_entry_shard(encoded: &[u8]) -> Result<Vec<NamespaceEntry>, MetadataFormatError> {
    let object = decode_metadata_object(Some(NAMESPACE_ROOT_KIND), encoded)?;
    let payload = object.payload;
    if payload.len() < ENTRY_SHARD_HEADER_BYTES
        || &payload[0..8] != ENTRY_SHARD_MAGIC
        || get_u16(payload, 8) != NAMESPACE_SHARD_VERSION_V2
        || usize::from(get_u16(payload, 10)) != ENTRY_SHARD_HEADER_BYTES
        || usize::from(get_u16(payload, 12)) != NAMESPACE_ENTRY_HEADER_BYTES
        || get_u32(payload, 20) != 0
        || usize::try_from(get_u64(payload, 24)) != Ok(payload.len())
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let entry_count = usize::try_from(get_u32(payload, 16))
        .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
    if entry_count == 0 || entry_count > NAMESPACE_SHARD_MAX_RECORDS {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let entries = decode_entries(
        payload,
        ENTRY_SHARD_HEADER_BYTES,
        payload.len(),
        entry_count,
    )?;
    if entries.windows(2).any(|pair| {
        (pair[0].parent_inode, pair[0].name.as_slice())
            >= (pair[1].parent_inode, pair[1].name.as_slice())
    }) {
        return Err(MetadataFormatError::InvalidPayload);
    }
    Ok(entries)
}

const NAMESPACE_GRAPH_MAGIC_V2: &[u8; 8] = b"FDNSGR02";
const NAMESPACE_GRAPH_VERSION_V2: u16 = 2;
const NAMESPACE_GRAPH_HEADER_BYTES_V2: usize = 96;
const NAMESPACE_GRAPH_SHARD_REF_BYTES_V2: usize = 56;
const NAMESPACE_ROOT_METADATA_HEADER_BYTES: usize = 24;
const SHARD_KIND_INODE: u8 = 0;
const SHARD_KIND_ENTRY: u8 = 1;

fn encode_root_metadata_blob(
    metadata: &DurableRootMetadata,
) -> Result<Vec<u8>, MetadataFormatError> {
    let posix_length = posix_metadata_record_length(0)?;
    let mut length = NAMESPACE_ROOT_METADATA_HEADER_BYTES
        .checked_add(posix_length)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    for xattr in &metadata.xattrs {
        length = length
            .checked_add(xattr_record_length(xattr.name.len(), xattr.value.len())?)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    }
    let mut blob = vec![0_u8; length];
    put_u16(&mut blob, 0, metadata.mode);
    put_u32(&mut blob, 4, metadata.uid);
    put_u32(&mut blob, 8, metadata.gid);
    put_u32(&mut blob, 12, metadata.file_flags);
    put_u32(
        &mut blob,
        16,
        u32::try_from(metadata.xattrs.len())
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    let mut cursor = encode_posix_metadata_record(
        &mut blob,
        NAMESPACE_ROOT_METADATA_HEADER_BYTES,
        ROOT_INODE,
        metadata.times,
        None,
    )?;
    for xattr in &metadata.xattrs {
        cursor = encode_xattr_record(&mut blob, cursor, ROOT_INODE, xattr)?;
    }
    assert_eq!(cursor, length, "ASSERT: root metadata preflight is exact");
    Ok(blob)
}

fn decode_root_metadata_blob(blob: &[u8]) -> Result<DurableRootMetadata, MetadataFormatError> {
    let posix_length = posix_metadata_record_length(0)?;
    let xattrs_offset = NAMESPACE_ROOT_METADATA_HEADER_BYTES
        .checked_add(posix_length)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    if blob.len() < xattrs_offset || get_u16(blob, 2) != 0 || get_u32(blob, 20) != 0 {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let xattr_count =
        usize::try_from(get_u32(blob, 16)).map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
    let header = &blob[NAMESPACE_ROOT_METADATA_HEADER_BYTES..xattrs_offset];
    if usize::try_from(get_u32(header, 0)) != Ok(posix_length)
        || get_u16(header, 4) != 0
        || get_u16(header, 6) != 0
        || get_u64(header, 8) != ROOT_INODE
    {
        return Err(MetadataFormatError::InvalidPayload);
    }
    let times = DurableTimes {
        atime: decode_timestamp(header, 16)?,
        mtime: decode_timestamp(header, 32)?,
        ctime: decode_timestamp(header, 48)?,
    };
    let decoded = decode_xattrs(blob, xattrs_offset, blob.len(), xattr_count)?;
    let mut xattrs = Vec::with_capacity(decoded.len());
    for (inode, xattr) in decoded {
        if inode != ROOT_INODE {
            return Err(MetadataFormatError::InvalidPayload);
        }
        xattrs.push(xattr);
    }
    Ok(DurableRootMetadata::new(
        get_u16(blob, 0),
        get_u32(blob, 4),
        get_u32(blob, 8),
        get_u32(blob, 12),
        xattrs,
    )?
    .with_times(times))
}

fn encode_namespace_graph_root(
    root: &NamespaceRoot,
    refs: &[NamespaceShardRef],
) -> Result<Vec<u8>, MetadataFormatError> {
    let metadata = encode_root_metadata_blob(&root.root_metadata)?;
    let shard_refs_offset = NAMESPACE_GRAPH_HEADER_BYTES_V2
        .checked_add(metadata.len())
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let total_length = refs
        .len()
        .checked_mul(NAMESPACE_GRAPH_SHARD_REF_BYTES_V2)
        .and_then(|length| length.checked_add(shard_refs_offset))
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let mut payload = vec![0_u8; total_length];
    payload[0..8].copy_from_slice(NAMESPACE_GRAPH_MAGIC_V2);
    put_u16(&mut payload, 8, NAMESPACE_GRAPH_VERSION_V2);
    put_u16(
        &mut payload,
        10,
        u16::try_from(NAMESPACE_GRAPH_HEADER_BYTES_V2).expect("ASSERT: header size fits u16"),
    );
    put_u16(
        &mut payload,
        12,
        u16::try_from(NAMESPACE_GRAPH_SHARD_REF_BYTES_V2).expect("ASSERT: ref size fits u16"),
    );
    put_u64(&mut payload, 16, root.inode_reservation_end);
    put_u64(&mut payload, 24, root.inode_allocation_cursor);
    put_u64(&mut payload, 32, root.namespace_mutation_sequence);
    put_u64(
        &mut payload,
        40,
        u64::try_from(root.inodes.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        &mut payload,
        48,
        u64::try_from(root.entries.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    let inode_shards = refs.iter().filter(|r| r.kind == SHARD_KIND_INODE).count();
    put_u32(
        &mut payload,
        56,
        u32::try_from(inode_shards).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u32(
        &mut payload,
        60,
        u32::try_from(refs.len() - inode_shards)
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        &mut payload,
        64,
        u64::try_from(NAMESPACE_GRAPH_HEADER_BYTES_V2)
            .map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        &mut payload,
        72,
        u64::try_from(metadata.len()).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        &mut payload,
        80,
        u64::try_from(shard_refs_offset).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        &mut payload,
        88,
        u64::try_from(total_length).map_err(|_| MetadataFormatError::ArithmeticOverflow)?,
    );
    payload[NAMESPACE_GRAPH_HEADER_BYTES_V2..shard_refs_offset].copy_from_slice(&metadata);
    for (index, reference) in refs.iter().enumerate() {
        let start = shard_refs_offset + index * NAMESPACE_GRAPH_SHARD_REF_BYTES_V2;
        let record = &mut payload[start..start + NAMESPACE_GRAPH_SHARD_REF_BYTES_V2];
        record[0] = reference.kind;
        put_u32(record, 4, reference.record_count);
        put_u64(record, 8, reference.first_key);
        put_u32(record, 16, reference.first_name_length);
        record[24..56].copy_from_slice(&reference.object_id.bytes());
    }
    encode_metadata_object(NAMESPACE_ROOT_KIND, &payload)
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed u16 field"),
    )
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed u32 field"),
    )
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 field"),
    )
}
