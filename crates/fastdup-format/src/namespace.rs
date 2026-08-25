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
const ROOT_INODE: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableInodeKind {
    Regular,
    Directory,
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
        })
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
        })
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
        payload_length(&entries, inodes.len())?;
        Ok(Self {
            inode_reservation_end,
            inode_allocation_cursor,
            namespace_mutation_sequence,
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
    pub fn encode(&self) -> Result<Vec<u8>, MetadataFormatError> {
        validate_namespace(
            self.inode_reservation_end,
            self.inode_allocation_cursor,
            &self.inodes,
            &self.entries,
        )?;
        let payload_length = payload_length(&self.entries, self.inodes.len())?;
        let inode_bytes = self
            .inodes
            .len()
            .checked_mul(DURABLE_INODE_BYTES)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let entries_offset = NAMESPACE_ROOT_HEADER_BYTES
            .checked_add(inode_bytes)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        let mut payload = vec![0_u8; payload_length];
        payload[0..8].copy_from_slice(NAMESPACE_ROOT_MAGIC);
        put_u16(&mut payload, 8, FORMAT_VERSION_V2);
        put_u16(&mut payload, 10, 128);
        put_u16(&mut payload, 12, 96);
        put_u16(&mut payload, 14, 24);
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
    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataFormatError> {
        let object = decode_metadata_object(Some(NAMESPACE_ROOT_KIND), bytes)?;
        let payload = object.payload;
        if payload.len() < NAMESPACE_ROOT_HEADER_BYTES
            || &payload[0..8] != NAMESPACE_ROOT_MAGIC
            || !matches!(get_u16(payload, 8), FORMAT_VERSION_V1 | FORMAT_VERSION_V2)
            || usize::from(get_u16(payload, 10)) != NAMESPACE_ROOT_HEADER_BYTES
            || usize::from(get_u16(payload, 12)) != DURABLE_INODE_BYTES
            || usize::from(get_u16(payload, 14)) != NAMESPACE_ENTRY_HEADER_BYTES
            || get_u64(payload, 16) != 0
            || get_u64(payload, 24) != 0
            || get_u64(payload, 32) != ROOT_INODE
            || payload[96..NAMESPACE_ROOT_HEADER_BYTES]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
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
        let remaining_entry_bytes = payload.len() - entries_offset;
        let minimum_entry_bytes = entry_record_length(1)?;
        if entry_count > remaining_entry_bytes / minimum_entry_bytes {
            return Err(MetadataFormatError::InvalidPayload);
        }

        let format_version = get_u16(payload, 8);
        let inodes = decode_inodes(payload, inode_count, format_version)?;
        let entries = decode_entries(payload, entries_offset, entry_count)?;
        if inodes.windows(2).any(|pair| pair[0].inode >= pair[1].inode)
            || entries.windows(2).any(|pair| {
                (pair[0].parent_inode, pair[0].name.as_slice())
                    >= (pair[1].parent_inode, pair[1].name.as_slice())
            })
        {
            return Err(MetadataFormatError::InvalidPayload);
        }
        Self::new(
            get_u64(payload, 40),
            get_u64(payload, 88),
            get_u64(payload, 48),
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
            (FORMAT_VERSION_V1, 0) | (FORMAT_VERSION_V2, 1) => DurableInodeKind::Regular,
            (FORMAT_VERSION_V2, 2) => DurableInodeKind::Directory,
            _ => return Err(MetadataFormatError::InvalidPayload),
        };
        if record[72..].iter().any(|byte| *byte != 0) {
            return Err(MetadataFormatError::InvalidPayload);
        }
        let mut manifest_root = [0_u8; 32];
        manifest_root.copy_from_slice(&record[40..72]);
        let inode = match kind {
            DurableInodeKind::Regular => DurableInode::new(
                get_u64(record, 0),
                get_u16(record, 8),
                get_u32(record, 12),
                get_u32(record, 16),
                get_u32(record, 20),
                get_u64(record, 24),
                get_u64(record, 32),
                MetadataObjectId::new(manifest_root).ok_or(MetadataFormatError::InvalidPayload)?,
            )?,
            DurableInodeKind::Directory => {
                if manifest_root != [0; 32] || get_u64(record, 32) != 0 {
                    return Err(MetadataFormatError::InvalidPayload);
                }
                DurableInode::new_directory(
                    get_u64(record, 0),
                    get_u16(record, 8),
                    get_u32(record, 12),
                    get_u32(record, 16),
                    get_u32(record, 20),
                    get_u64(record, 24),
                )?
            }
        };
        inodes.push(inode);
    }
    Ok(inodes)
}

fn decode_entries(
    payload: &[u8],
    entries_offset: usize,
    entry_count: usize,
) -> Result<Vec<NamespaceEntry>, MetadataFormatError> {
    let mut entries = Vec::with_capacity(entry_count);
    let mut cursor = entries_offset;
    for _ in 0..entry_count {
        let header_end = cursor
            .checked_add(NAMESPACE_ENTRY_HEADER_BYTES)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?;
        if header_end > payload.len() {
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
            || end > payload.len()
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
    if cursor != payload.len() {
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
            DurableInodeKind::Regular if incoming != inode.link_count => {
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
            DurableInodeKind::Regular => {}
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
    entries: &[NamespaceEntry],
    inode_count: usize,
) -> Result<usize, MetadataFormatError> {
    let inode_bytes = inode_count
        .checked_mul(DURABLE_INODE_BYTES)
        .ok_or(MetadataFormatError::ArithmeticOverflow)?;
    let length = entries.iter().try_fold(
        NAMESPACE_ROOT_HEADER_BYTES
            .checked_add(inode_bytes)
            .ok_or(MetadataFormatError::ArithmeticOverflow)?,
        |length, entry| {
            length
                .checked_add(entry_record_length(entry.name.len())?)
                .ok_or(MetadataFormatError::ArithmeticOverflow)
        },
    )?;
    if length > MAX_METADATA_OBJECT_BYTES - METADATA_HEADER_BYTES {
        return Err(MetadataFormatError::InvalidObjectLength(length));
    }
    Ok(length)
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
