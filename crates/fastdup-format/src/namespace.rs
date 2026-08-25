use std::collections::{BTreeMap, BTreeSet};

use crate::metadata::{NAMESPACE_ROOT_KIND, decode_metadata_object, encode_metadata_object};
use crate::{
    MAX_METADATA_OBJECT_BYTES, METADATA_HEADER_BYTES, MetadataFormatError, MetadataObjectId,
};

pub const NAMESPACE_ROOT_HEADER_BYTES: usize = 128;
const DURABLE_INODE_BYTES: usize = 96;
const NAMESPACE_ENTRY_HEADER_BYTES: usize = 24;
const NAMESPACE_ENTRY_ALIGNMENT: usize = 8;
const MAX_NAME_BYTES: usize = 255;
const NAMESPACE_ROOT_MAGIC: &[u8; 8] = b"FDNSRT01";
const FORMAT_VERSION_V1: u16 = 1;
const FORMAT_VERSION_V2: u16 = 2;
const FORMAT_VERSION_V3: u16 = 3;
const FORMAT_VERSION_V4: u16 = 4;
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

    /// Encodes this Namespace Root inside the generic metadata envelope.
    ///
    /// # Errors
    ///
    /// Returns an invariant, arithmetic, or bounded-envelope failure.
    ///
    /// # Panics
    ///
    /// Panics only if the checked payload preflight disagrees with the encoder
    /// cursor, which is an impossible internal writer state.
    #[allow(clippy::too_many_lines)]
    pub fn encode(&self) -> Result<Vec<u8>, MetadataFormatError> {
        validate_namespace(
            self.inode_reservation_end,
            self.inode_allocation_cursor,
            &self.inodes,
            &self.entries,
        )?;
        let payload_length = payload_length(&self.root_metadata, &self.entries, &self.inodes)?;
        let has_posix_metadata = self.root_metadata.times != DurableTimes::default()
            || self.inodes.iter().any(|inode| {
                inode.times != DurableTimes::default() || inode.symlink_target.is_some()
            });
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
        put_u16(
            &mut payload,
            8,
            if has_posix_metadata {
                FORMAT_VERSION_V4
            } else {
                FORMAT_VERSION_V3
            },
        );
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
        if has_posix_metadata {
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
        }

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
        if has_posix_metadata {
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
        }
        assert_eq!(
            cursor, payload_length,
            "ASSERT: namespace payload preflight must match encoder cursor"
        );
        encode_metadata_object(NAMESPACE_ROOT_KIND, &payload)
    }

    /// Fully validates and decodes one Namespace Root Metadata Object.
    ///
    /// # Errors
    ///
    /// Returns an envelope, layout, reserved-field, name, reference, or link
    /// invariant failure without exposing partial namespace state.
    #[allow(clippy::too_many_lines)]
    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataFormatError> {
        let object = decode_metadata_object(Some(NAMESPACE_ROOT_KIND), bytes)?;
        let payload = object.payload;
        if payload.len() < NAMESPACE_ROOT_HEADER_BYTES {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let format_version = get_u16(payload, 8);
        if &payload[0..8] != NAMESPACE_ROOT_MAGIC
            || !matches!(
                format_version,
                FORMAT_VERSION_V1 | FORMAT_VERSION_V2 | FORMAT_VERSION_V3 | FORMAT_VERSION_V4
            )
            || usize::from(get_u16(payload, 10)) != NAMESPACE_ROOT_HEADER_BYTES
            || usize::from(get_u16(payload, 12)) != DURABLE_INODE_BYTES
            || usize::from(get_u16(payload, 14)) != NAMESPACE_ENTRY_HEADER_BYTES
            || get_u64(payload, 32) != ROOT_INODE
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let root_metadata = if matches!(format_version, FORMAT_VERSION_V3 | FORMAT_VERSION_V4) {
            if get_u16(payload, 18) != 0
                || get_u16(payload, 102) != 0
                || (format_version == FORMAT_VERSION_V3
                    && payload[112..NAMESPACE_ROOT_HEADER_BYTES]
                        .iter()
                        .any(|byte| *byte != 0))
                || (format_version == FORMAT_VERSION_V4 && get_u16(payload, 118) != 0)
            {
                return Err(MetadataFormatError::InvalidPayload);
            }
            DurableRootMetadata::new(
                get_u16(payload, 16),
                get_u32(payload, 20),
                get_u32(payload, 24),
                get_u32(payload, 28),
                Vec::new(),
            )?
        } else {
            if get_u64(payload, 16) != 0
                || get_u64(payload, 24) != 0
                || payload[96..NAMESPACE_ROOT_HEADER_BYTES]
                    .iter()
                    .any(|byte| *byte != 0)
            {
                return Err(MetadataFormatError::InvalidPayload);
            }
            DurableRootMetadata::default()
        };
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
        let xattrs_offset = if matches!(format_version, FORMAT_VERSION_V3 | FORMAT_VERSION_V4) {
            if usize::from(get_u16(payload, 100)) != XATTR_RECORD_HEADER_BYTES {
                return Err(MetadataFormatError::InvalidPayload);
            }
            usize::try_from(get_u64(payload, 104))
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?
        } else {
            payload.len()
        };
        if xattrs_offset < entries_offset || xattrs_offset > payload.len() {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let remaining_entry_bytes = xattrs_offset - entries_offset;
        let minimum_entry_bytes = entry_record_length(1)?;
        if entry_count > remaining_entry_bytes / minimum_entry_bytes {
            return Err(MetadataFormatError::InvalidPayload);
        }

        let mut inodes = decode_inodes(payload, inode_count, format_version)?;
        let entries = decode_entries(payload, entries_offset, xattrs_offset, entry_count)?;
        let mut root_metadata = root_metadata;
        let posix_metadata_offset = if format_version == FORMAT_VERSION_V4 {
            if usize::from(get_u16(payload, 116)) != POSIX_METADATA_RECORD_HEADER_BYTES {
                return Err(MetadataFormatError::InvalidPayload);
            }
            usize::try_from(get_u64(payload, 120))
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?
        } else {
            payload.len()
        };
        if posix_metadata_offset < xattrs_offset || posix_metadata_offset > payload.len() {
            return Err(MetadataFormatError::InvalidPayload);
        }
        if matches!(format_version, FORMAT_VERSION_V3 | FORMAT_VERSION_V4) {
            let xattr_count = usize::try_from(get_u32(payload, 96))
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
            let decoded =
                decode_xattrs(payload, xattrs_offset, posix_metadata_offset, xattr_count)?;
            install_decoded_xattrs(&mut root_metadata, &mut inodes, decoded)?;
        } else if xattrs_offset != payload.len() {
            return Err(MetadataFormatError::InvalidPayload);
        }
        if format_version == FORMAT_VERSION_V4 {
            let count = usize::try_from(get_u32(payload, 112))
                .map_err(|_| MetadataFormatError::ArithmeticOverflow)?;
            decode_posix_metadata(
                payload,
                posix_metadata_offset,
                count,
                &mut root_metadata,
                &mut inodes,
            )?;
        }
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
}

fn decode_inodes(
    payload: &[u8],
    inode_count: usize,
    format_version: u16,
) -> Result<Vec<DurableInode>, MetadataFormatError> {
    let mut inodes = Vec::with_capacity(inode_count);
    for ordinal in 0..inode_count {
        let start = NAMESPACE_ROOT_HEADER_BYTES
            .checked_add(
                ordinal
                    .checked_mul(DURABLE_INODE_BYTES)
                    .ok_or(MetadataFormatError::ArithmeticOverflow)?,
            )
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let record = &payload[start..start + DURABLE_INODE_BYTES];
        let kind = match (format_version, get_u16(record, 10)) {
            (FORMAT_VERSION_V1, 0)
            | (FORMAT_VERSION_V2 | FORMAT_VERSION_V3 | FORMAT_VERSION_V4, 1) => {
                DurableInodeKind::Regular
            }
            (FORMAT_VERSION_V2 | FORMAT_VERSION_V3 | FORMAT_VERSION_V4, 2) => {
                DurableInodeKind::Directory
            }
            (FORMAT_VERSION_V4, 3) => DurableInodeKind::Symlink,
            _ => return Err(MetadataFormatError::InvalidPayload),
        };
        let file_flags = if matches!(format_version, FORMAT_VERSION_V3 | FORMAT_VERSION_V4) {
            if record[76..].iter().any(|byte| *byte != 0) {
                return Err(MetadataFormatError::InvalidPayload);
            }
            let flags = get_u32(record, 72);
            validate_file_flags(flags)?;
            flags
        } else {
            if record[72..].iter().any(|byte| *byte != 0) {
                return Err(MetadataFormatError::InvalidPayload);
            }
            0
        };
        if format_version == FORMAT_VERSION_V1 && kind != DurableInodeKind::Regular {
            return Err(MetadataFormatError::InvalidPayload);
        }
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
    let by_inode = inodes
        .iter()
        .map(|inode| (inode.inode, inode))
        .collect::<BTreeMap<_, _>>();
    let mut observed_links = BTreeMap::<u64, u32>::new();
    let mut directory_children = BTreeMap::<u64, u32>::new();
    let mut children = BTreeMap::<u64, Vec<u64>>::new();
    for entry in entries {
        NamespaceEntry::new(entry.parent_inode, entry.target_inode, entry.name.clone())?;
        if entry.parent_inode != ROOT_INODE
            && by_inode
                .get(&entry.parent_inode)
                .is_none_or(|parent| parent.kind != DurableInodeKind::Directory)
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let target = by_inode
            .get(&entry.target_inode)
            .ok_or(MetadataFormatError::InvalidPayload)?;
        let count = observed_links.entry(entry.target_inode).or_default();
        *count = count
            .checked_add(1)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if target.kind == DurableInodeKind::Directory {
            let count = directory_children.entry(entry.parent_inode).or_default();
            *count = count
                .checked_add(1)
                .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        }
        children
            .entry(entry.parent_inode)
            .or_default()
            .push(entry.target_inode);
    }
    for inode in inodes {
        let incoming = observed_links.get(&inode.inode).copied().unwrap_or(0);
        match inode.kind {
            DurableInodeKind::Regular | DurableInodeKind::Symlink
                if incoming != inode.link_count =>
            {
                return Err(MetadataFormatError::InvalidPayload);
            }
            DurableInodeKind::Directory => {
                let child_directories = directory_children.get(&inode.inode).copied().unwrap_or(0);
                let expected_links = 2_u32
                    .checked_add(child_directories)
                    .ok_or(MetadataFormatError::ArithmeticOverflow)?;
                if incoming != 1 || inode.link_count != expected_links {
                    return Err(MetadataFormatError::InvalidPayload);
                }
            }
            DurableInodeKind::Regular | DurableInodeKind::Symlink => {}
        }
    }

    let mut reachable = BTreeSet::new();
    let mut pending = vec![ROOT_INODE];
    while let Some(parent) = pending.pop() {
        if let Some(targets) = children.get(&parent) {
            for &target in targets {
                let is_directory = by_inode
                    .get(&target)
                    .is_some_and(|inode| inode.kind == DurableInodeKind::Directory);
                if !reachable.insert(target) && is_directory {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                if is_directory {
                    pending.push(target);
                }
            }
        }
    }
    if reachable.len() != inodes.len() {
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
    let has_posix_metadata = root_metadata.times != DurableTimes::default()
        || inodes
            .iter()
            .any(|inode| inode.times != DurableTimes::default() || inode.symlink_target.is_some());
    if has_posix_metadata {
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
    }
    if length > MAX_METADATA_OBJECT_BYTES - METADATA_HEADER_BYTES {
        return Err(MetadataFormatError::InvalidObjectLength(length));
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
