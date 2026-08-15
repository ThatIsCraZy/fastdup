//! Byte-exact POSIX namespace semantics and the low-level FUSE adapter.
//!
//! The first implementation checkpoint is deliberately volatile. It proves
//! live POSIX semantics but does not claim crash durability.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::ops::Bound::Excluded;
use std::sync::{Arc, RwLock};

mod fuse_adapter;
mod versioned_file;

use versioned_file::VersionedFile;

pub use fuse_adapter::{FuseFilesystem, volatile_mount_options};
pub use versioned_file::CommittedFile;

pub const ROOT_INODE: InodeId = InodeId(NonZeroU64::MIN);
const MAX_COALESCED_EXTENT_BYTES: usize = 16 * 1_024 * 1_024;
const MAX_DIRECTORY_ENTRIES_PER_REPLY: usize = 256;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InodeId(NonZeroU64);

impl InodeId {
    #[must_use]
    pub const fn new(raw: u64) -> Option<Self> {
        match NonZeroU64::new(raw) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HandleId(NonZeroU64);

impl HandleId {
    #[must_use]
    pub const fn new(raw: u64) -> Option<Self> {
        match NonZeroU64::new(raw) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestContext {
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessMode {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenOptions {
    pub access: AccessMode,
    pub append: bool,
}

impl OpenOptions {
    pub const READ_ONLY: Self = Self {
        access: AccessMode::ReadOnly,
        append: false,
    };
    pub const READ_WRITE: Self = Self {
        access: AccessMode::ReadWrite,
        append: false,
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileKind {
    Directory,
    Regular,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileAttr {
    pub inode: InodeId,
    pub size: u64,
    pub allocated_bytes: u64,
    pub kind: FileKind,
    pub mode: u16,
    pub uid: u32,
    pub gid: u32,
    pub link_count: u32,
    pub mutation_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    pub attr: FileAttr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub inode: InodeId,
    pub kind: FileKind,
    pub attr: FileAttr,
    pub name: Vec<u8>,
    pub next_offset: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Operation<'a> {
    Lookup {
        parent: InodeId,
        name: &'a [u8],
    },
    GetAttr {
        inode: InodeId,
    },
    Create {
        parent: InodeId,
        name: &'a [u8],
        mode: u16,
        options: OpenOptions,
        exclusive: bool,
        truncate: bool,
    },
    Open {
        inode: InodeId,
        options: OpenOptions,
        truncate: bool,
    },
    Read {
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        length: u32,
    },
    Write {
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        data: &'a [u8],
    },
    SetLength {
        inode: InodeId,
        handle: Option<HandleId>,
        length: u64,
    },
    Sync {
        inode: InodeId,
        handle: HandleId,
        data_only: bool,
    },
    Release {
        inode: InodeId,
        handle: HandleId,
    },
    Unlink {
        parent: InodeId,
        name: &'a [u8],
    },
    ReadDirectory {
        inode: InodeId,
        offset: i64,
        acquire_lookup: bool,
    },
    Forget {
        inode: InodeId,
        lookup_count: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reply {
    Entry(Entry),
    Attr(FileAttr),
    Created { entry: Entry, handle: HandleId },
    Opened(HandleId),
    Data(Vec<u8>),
    Written { bytes: u32, mutation_sequence: u64 },
    Directory(Vec<DirectoryEntry>),
    Empty,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PosixError {
    NoEntry,
    Exists,
    NotDirectory,
    IsDirectory,
    InvalidName,
    NameTooLong,
    InvalidArgument,
    BadHandle,
    FileTooLarge,
    NoSpace,
    OutOfMemory,
    Unsupported,
    Io,
    ReadOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NamespaceConfig {
    pub maximum_name_bytes: usize,
    pub maximum_file_bytes: u64,
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            maximum_name_bytes: 255,
            maximum_file_bytes: u64::MAX,
        }
    }
}

#[derive(Debug)]
pub struct CommittedInode {
    inode: InodeId,
    mode: u16,
    uid: u32,
    gid: u32,
    link_count: u32,
    mutation_sequence: u64,
    file: Arc<dyn CommittedFile>,
}

impl CommittedInode {
    /// Describes one verified committed regular inode without loading its bytes.
    ///
    /// # Errors
    ///
    /// Rejects the implicit root inode, a zero-link orphan, or a committed
    /// reader whose allocated byte count exceeds its logical size.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inode: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
        file: Arc<dyn CommittedFile>,
    ) -> Result<Self, PosixError> {
        let inode = InodeId::new(inode).ok_or(PosixError::InvalidArgument)?;
        if inode <= ROOT_INODE || link_count == 0 || file.allocated_bytes() > file.logical_size() {
            return Err(PosixError::InvalidArgument);
        }
        Ok(Self {
            inode,
            mode,
            uid,
            gid,
            link_count,
            mutation_sequence,
            file,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedEntry {
    parent: InodeId,
    target: InodeId,
    name: Vec<u8>,
}

impl CommittedEntry {
    /// Describes one byte-exact committed directory entry.
    ///
    /// # Errors
    ///
    /// Rejects zero inode identities. Parent, name, and reachability are
    /// verified together when the complete snapshot is mounted.
    pub fn new(parent: u64, target: u64, name: Vec<u8>) -> Result<Self, PosixError> {
        Ok(Self {
            parent: InodeId::new(parent).ok_or(PosixError::InvalidArgument)?,
            target: InodeId::new(target).ok_or(PosixError::InvalidArgument)?,
            name,
        })
    }
}

#[derive(Debug)]
pub struct CommittedNamespaceSnapshot {
    next_inode: u64,
    inode_reservation_end: u64,
    namespace_mutation_sequence: u64,
    inodes: Vec<CommittedInode>,
    entries: Vec<CommittedEntry>,
}

impl CommittedNamespaceSnapshot {
    /// Bundles one already verified committed namespace for a POSIX mount.
    ///
    /// The current recover adapter mounts committed snapshots read-only until
    /// the checkpoint scheduler and reservation publisher are connected. No
    /// identifier is guessed from visible inode records.
    ///
    /// # Errors
    ///
    /// Rejects an empty, reversed, or root-overlapping allocation interval.
    pub fn new(
        next_inode: u64,
        inode_reservation_end: u64,
        namespace_mutation_sequence: u64,
        inodes: Vec<CommittedInode>,
        entries: Vec<CommittedEntry>,
    ) -> Result<Self, PosixError> {
        if next_inode <= ROOT_INODE.get() || next_inode > inode_reservation_end {
            return Err(PosixError::InvalidArgument);
        }
        Ok(Self {
            next_inode,
            inode_reservation_end,
            namespace_mutation_sequence,
            inodes,
            entries,
        })
    }
}

#[derive(Debug)]
struct InodeState {
    kind: FileKind,
    mode: u16,
    uid: u32,
    gid: u32,
    link_count: u32,
    mutation_sequence: u64,
    data: VersionedFile,
}

impl InodeState {
    fn attributes(&self, inode: InodeId) -> FileAttr {
        FileAttr {
            inode,
            size: self.data.logical_size(),
            allocated_bytes: self.data.allocated_bytes(),
            kind: self.kind,
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            link_count: self.link_count,
            mutation_sequence: self.mutation_sequence,
        }
    }
}

#[derive(Debug, Default)]
struct SparseData {
    logical_size: u64,
    allocated_bytes: u64,
    extents: BTreeMap<u64, Vec<u8>>,
}

impl SparseData {
    #[cfg(test)]
    fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        if length == 0 || offset >= self.logical_size {
            return Ok(Vec::new());
        }
        let end = offset
            .saturating_add(u64::from(length))
            .min(self.logical_size);
        let output_length = usize::try_from(end - offset).map_err(|_| PosixError::FileTooLarge)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(output_length)
            .map_err(|_| PosixError::OutOfMemory)?;
        output.resize(output_length, 0);

        if let Some((&extent_start, bytes)) = self.extents.range(..=offset).next_back() {
            overlay_extent(&mut output, offset, end, extent_start, bytes);
        }
        for (&extent_start, bytes) in self.extents.range((Excluded(offset), Excluded(end))) {
            overlay_extent(&mut output, offset, end, extent_start, bytes);
        }
        Ok(output)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), PosixError> {
        assert!(
            !data.is_empty(),
            "ASSERT: empty writes are handled by caller"
        );
        let data_length = u64::try_from(data.len()).expect("ASSERT: usize must fit in u64");
        let end = offset
            .checked_add(data_length)
            .ok_or(PosixError::FileTooLarge)?;
        if self.try_append_to_previous(offset, end, data)? {
            return Ok(());
        }

        let mut payload = Vec::new();
        payload
            .try_reserve_exact(data.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        payload.extend_from_slice(data);

        let mut overlapping = Vec::new();
        let mut fragments = Vec::new();
        if let Some((&extent_start, bytes)) = self.extents.range(..=offset).next_back() {
            plan_overlap(
                &mut overlapping,
                &mut fragments,
                extent_start,
                bytes,
                offset,
                end,
            )?;
        }
        for (&extent_start, bytes) in self.extents.range((Excluded(offset), Excluded(end))) {
            plan_overlap(
                &mut overlapping,
                &mut fragments,
                extent_start,
                bytes,
                offset,
                end,
            )?;
        }

        for start in overlapping {
            let removed = self
                .extents
                .remove(&start)
                .expect("ASSERT: planned overlapping extent vanished");
            self.allocated_bytes = self
                .allocated_bytes
                .checked_sub(u64::try_from(removed.len()).expect("ASSERT: usize must fit in u64"))
                .expect("ASSERT: removed extent bytes must be accounted");
        }
        for (start, fragment) in fragments {
            self.allocated_bytes = self
                .allocated_bytes
                .checked_add(u64::try_from(fragment.len()).expect("ASSERT: usize must fit in u64"))
                .expect("ASSERT: allocated extent bytes must not overflow");
            assert!(
                self.extents.insert(start, fragment).is_none(),
                "ASSERT: split extent must not overlap a surviving extent"
            );
        }
        self.allocated_bytes = self
            .allocated_bytes
            .checked_add(data_length)
            .expect("ASSERT: allocated extent bytes must not overflow");
        assert!(
            self.extents.insert(offset, payload).is_none(),
            "ASSERT: new write extent must replace every overlap"
        );
        self.logical_size = self.logical_size.max(end);
        self.assert_valid_around(offset);
        self.assert_valid_around(end);
        #[cfg(test)]
        self.audit_valid();
        Ok(())
    }

    fn try_append_to_previous(
        &mut self,
        offset: u64,
        end: u64,
        data: &[u8],
    ) -> Result<bool, PosixError> {
        if self.extents.range(offset..end).next().is_some() {
            return Ok(false);
        }
        let Some((&previous_start, previous)) = self.extents.range_mut(..offset).next_back() else {
            return Ok(false);
        };
        let previous_length = u64::try_from(previous.len()).expect("ASSERT: usize must fit in u64");
        let previous_end = previous_start
            .checked_add(previous_length)
            .expect("ASSERT: validated extent end must not overflow");
        let combined = previous
            .len()
            .checked_add(data.len())
            .ok_or(PosixError::OutOfMemory)?;
        if previous_end != offset || combined > MAX_COALESCED_EXTENT_BYTES {
            return Ok(false);
        }
        previous
            .try_reserve(data.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        previous.extend_from_slice(data);
        self.allocated_bytes = self
            .allocated_bytes
            .checked_add(u64::try_from(data.len()).expect("ASSERT: usize must fit in u64"))
            .expect("ASSERT: allocated extent bytes must not overflow");
        self.logical_size = self.logical_size.max(end);
        self.assert_valid_around(previous_start);
        #[cfg(test)]
        self.audit_valid();
        Ok(true)
    }

    fn truncate(&mut self, length: u64) -> Result<(), PosixError> {
        if length >= self.logical_size {
            self.logical_size = length;
            return Ok(());
        }

        let crossing = self
            .extents
            .range(..length)
            .next_back()
            .and_then(|(&start, bytes)| {
                let extent_length =
                    u64::try_from(bytes.len()).expect("ASSERT: usize must fit in u64");
                let end = start
                    .checked_add(extent_length)
                    .expect("ASSERT: validated extent end must not overflow");
                if end > length {
                    Some((start, bytes.as_slice()))
                } else {
                    None
                }
            });
        let crossing = if let Some((start, bytes)) = crossing {
            let keep = usize::try_from(length - start)
                .expect("ASSERT: truncated extent must fit in usize");
            Some((start, copy_bytes(&bytes[..keep])?))
        } else {
            None
        };
        let mut remove = self
            .extents
            .range(length..)
            .map(|(&start, _)| start)
            .collect::<Vec<_>>();
        if let Some((start, _)) = &crossing {
            remove.push(*start);
        }
        for start in remove {
            let removed = self
                .extents
                .remove(&start)
                .expect("ASSERT: planned truncated extent vanished");
            self.allocated_bytes = self
                .allocated_bytes
                .checked_sub(u64::try_from(removed.len()).expect("ASSERT: usize must fit in u64"))
                .expect("ASSERT: removed extent bytes must be accounted");
        }
        if let Some((start, bytes)) = crossing {
            self.allocated_bytes = self
                .allocated_bytes
                .checked_add(u64::try_from(bytes.len()).expect("ASSERT: usize must fit in u64"))
                .expect("ASSERT: allocated extent bytes must not overflow");
            assert!(
                self.extents.insert(start, bytes).is_none(),
                "ASSERT: truncated extent must not overlap a survivor"
            );
        }
        self.logical_size = length;
        self.assert_valid_around(length);
        #[cfg(test)]
        self.audit_valid();
        Ok(())
    }

    #[cfg(test)]
    const fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes
    }

    fn assert_valid_around(&self, position: u64) {
        if let Some((&start, bytes)) = self.extents.range(..=position).next_back() {
            self.assert_extent_valid(start, bytes);
        }
        if let Some((&start, bytes)) = self.extents.range(position..).next() {
            self.assert_extent_valid(start, bytes);
        }
    }

    fn assert_extent_valid(&self, start: u64, bytes: &[u8]) {
        assert!(
            !bytes.is_empty(),
            "ASSERT: sparse DATA extent must be nonempty"
        );
        let length = u64::try_from(bytes.len()).expect("ASSERT: usize must fit in u64");
        let end = start
            .checked_add(length)
            .expect("ASSERT: sparse extent end must not overflow");
        assert!(
            end <= self.logical_size,
            "ASSERT: sparse extent must stay inside logical size"
        );
        if let Some((&previous_start, previous)) = self.extents.range(..start).next_back() {
            let previous_length =
                u64::try_from(previous.len()).expect("ASSERT: usize must fit in u64");
            let previous_end = previous_start
                .checked_add(previous_length)
                .expect("ASSERT: sparse extent end must not overflow");
            assert!(
                previous_end <= start,
                "ASSERT: sparse DATA extents must not overlap"
            );
        }
        if let Some((&next_start, _)) = self
            .extents
            .range((Excluded(start), std::ops::Bound::Unbounded))
            .next()
        {
            assert!(
                end <= next_start,
                "ASSERT: sparse DATA extents must not overlap"
            );
        }
    }

    #[cfg(test)]
    fn audit_valid(&self) {
        let mut previous_end = 0_u64;
        let mut allocated_bytes = 0_u64;
        for (&start, bytes) in &self.extents {
            assert!(
                !bytes.is_empty(),
                "ASSERT: sparse DATA extent must be nonempty"
            );
            assert!(
                start >= previous_end,
                "ASSERT: sparse DATA extents must not overlap"
            );
            let length = u64::try_from(bytes.len()).expect("ASSERT: usize must fit in u64");
            previous_end = start
                .checked_add(length)
                .expect("ASSERT: sparse extent end must not overflow");
            assert!(
                previous_end <= self.logical_size,
                "ASSERT: sparse extent must stay inside logical size"
            );
            allocated_bytes = allocated_bytes
                .checked_add(length)
                .expect("ASSERT: allocated extent bytes must not overflow");
        }
        assert_eq!(
            allocated_bytes, self.allocated_bytes,
            "ASSERT: cached allocated extent bytes must match the extent map"
        );
    }
}

#[cfg(test)]
fn overlay_extent(
    output: &mut [u8],
    read_start: u64,
    read_end: u64,
    extent_start: u64,
    bytes: &[u8],
) {
    let extent_length = u64::try_from(bytes.len()).expect("ASSERT: usize must fit in u64");
    let extent_end = extent_start
        .checked_add(extent_length)
        .expect("ASSERT: validated extent end must not overflow");
    if extent_end <= read_start || extent_start >= read_end {
        return;
    }
    let copy_start = extent_start.max(read_start);
    let copy_end = extent_end.min(read_end);
    let source_start = usize::try_from(copy_start - extent_start)
        .expect("ASSERT: source offset must fit in usize");
    let source_end =
        usize::try_from(copy_end - extent_start).expect("ASSERT: source end must fit in usize");
    let target_start =
        usize::try_from(copy_start - read_start).expect("ASSERT: target offset must fit in usize");
    let target_end =
        usize::try_from(copy_end - read_start).expect("ASSERT: target end must fit in usize");
    output[target_start..target_end].copy_from_slice(&bytes[source_start..source_end]);
}

fn plan_overlap(
    overlapping: &mut Vec<u64>,
    fragments: &mut Vec<(u64, Vec<u8>)>,
    extent_start: u64,
    bytes: &[u8],
    write_start: u64,
    write_end: u64,
) -> Result<(), PosixError> {
    let extent_length = u64::try_from(bytes.len()).expect("ASSERT: usize must fit in u64");
    let extent_end = extent_start
        .checked_add(extent_length)
        .expect("ASSERT: validated extent end must not overflow");
    if extent_end <= write_start || extent_start >= write_end {
        return Ok(());
    }
    overlapping.push(extent_start);
    if extent_start < write_start {
        let keep = usize::try_from(write_start - extent_start)
            .expect("ASSERT: left fragment must fit in usize");
        fragments.push((extent_start, copy_bytes(&bytes[..keep])?));
    }
    if extent_end > write_end {
        let skip = usize::try_from(write_end - extent_start)
            .expect("ASSERT: right fragment must fit in usize");
        fragments.push((write_end, copy_bytes(&bytes[skip..])?));
    }
    Ok(())
}

fn copy_bytes(source: &[u8]) -> Result<Vec<u8>, PosixError> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(source.len())
        .map_err(|_| PosixError::OutOfMemory)?;
    copy.extend_from_slice(source);
    Ok(copy)
}

#[derive(Debug)]
#[repr(align(64))]
struct Inode {
    state: RwLock<InodeState>,
}

#[derive(Clone, Copy, Debug)]
struct OpenHandle {
    inode: InodeId,
    options: OpenOptions,
}

#[derive(Clone, Copy)]
struct CreateRequest<'a> {
    context: RequestContext,
    parent: InodeId,
    name: &'a [u8],
    mode: u16,
    options: OpenOptions,
    exclusive: bool,
    truncate: bool,
}

#[derive(Debug)]
struct Catalog {
    next_inode: u64,
    inode_reservation_end: u64,
    next_handle: u64,
    inodes: BTreeMap<InodeId, Arc<Inode>>,
    entries: BTreeMap<(InodeId, Vec<u8>), InodeId>,
    handles: BTreeMap<HandleId, OpenHandle>,
    lookup_counts: BTreeMap<InodeId, u64>,
}

#[derive(Debug)]
pub struct Namespace {
    config: NamespaceConfig,
    mutations_enabled: bool,
    catalog: RwLock<Catalog>,
}

impl Namespace {
    /// Creates the explicitly non-durable namespace checkpoint.
    ///
    /// # Panics
    ///
    /// Panics when the internal configuration permits no valid component name.
    #[must_use]
    pub fn new_volatile(config: NamespaceConfig) -> Self {
        assert!(
            config.maximum_name_bytes > 0,
            "ASSERT: maximum name length must be nonzero"
        );

        let root = Arc::new(Inode {
            state: RwLock::new(InodeState {
                kind: FileKind::Directory,
                mode: 0o755,
                uid: 0,
                gid: 0,
                link_count: 2,
                mutation_sequence: 0,
                data: VersionedFile::new_empty(),
            }),
        });
        let mut inodes = BTreeMap::new();
        let replaced = inodes.insert(ROOT_INODE, root);
        assert!(replaced.is_none(), "ASSERT: root inode inserted twice");

        Self {
            config,
            mutations_enabled: true,
            catalog: RwLock::new(Catalog {
                next_inode: ROOT_INODE.get() + 1,
                inode_reservation_end: u64::MAX,
                next_handle: 1,
                inodes,
                entries: BTreeMap::new(),
                handles: BTreeMap::new(),
                lookup_counts: BTreeMap::new(),
            }),
        }
    }

    /// Mounts one verified committed snapshot without materializing file data.
    ///
    /// The returned namespace uses the exact same [`Self::dispatch`] seam as
    /// the volatile model and FUSE adapter. Committed read failures map to
    /// [`PosixError::Io`]; no partial bytes are returned.
    ///
    /// # Errors
    ///
    /// Rejects invalid names, duplicate or dangling entries, inconsistent link
    /// counts, inode IDs outside the durable allocation cursor, and resource
    /// exhaustion while constructing the bounded in-memory catalog.
    ///
    /// # Panics
    ///
    /// Panics when the internal configuration permits no valid component name.
    pub fn from_committed(
        config: NamespaceConfig,
        snapshot: CommittedNamespaceSnapshot,
    ) -> Result<Self, PosixError> {
        assert!(
            config.maximum_name_bytes > 0,
            "ASSERT: maximum name length must be nonzero"
        );
        let root = Arc::new(Inode {
            state: RwLock::new(InodeState {
                kind: FileKind::Directory,
                mode: 0o755,
                uid: 0,
                gid: 0,
                link_count: 2,
                mutation_sequence: snapshot.namespace_mutation_sequence,
                data: VersionedFile::new_empty(),
            }),
        });
        let mut inodes = BTreeMap::new();
        assert!(
            inodes.insert(ROOT_INODE, root).is_none(),
            "ASSERT: root inode inserted twice"
        );
        for committed in snapshot.inodes {
            if committed.inode.get() >= snapshot.next_inode {
                return Err(PosixError::InvalidArgument);
            }
            let inode = committed.inode;
            let object = Arc::new(Inode {
                state: RwLock::new(InodeState {
                    kind: FileKind::Regular,
                    mode: committed.mode,
                    uid: committed.uid,
                    gid: committed.gid,
                    link_count: committed.link_count,
                    mutation_sequence: committed.mutation_sequence,
                    data: VersionedFile::from_committed(
                        committed.file,
                        committed.mutation_sequence,
                    ),
                }),
            });
            if inodes.insert(inode, object).is_some() {
                return Err(PosixError::InvalidArgument);
            }
        }

        let mut entries = BTreeMap::new();
        let mut observed_links = BTreeMap::<InodeId, u32>::new();
        for entry in snapshot.entries {
            validate_component(&config, &entry.name)?;
            if entry.parent != ROOT_INODE
                || entry.target == ROOT_INODE
                || !inodes.contains_key(&entry.target)
            {
                return Err(PosixError::InvalidArgument);
            }
            let key = (entry.parent, entry.name);
            if entries.insert(key, entry.target).is_some() {
                return Err(PosixError::InvalidArgument);
            }
            let count = observed_links.entry(entry.target).or_default();
            *count = count.checked_add(1).ok_or(PosixError::NoSpace)?;
        }
        for (&inode, object) in &inodes {
            if inode == ROOT_INODE {
                continue;
            }
            let expected = object
                .state
                .read()
                .expect("ASSERT: new committed inode lock poisoned")
                .link_count;
            if observed_links.get(&inode).copied() != Some(expected) {
                return Err(PosixError::InvalidArgument);
            }
        }

        Ok(Self {
            config,
            mutations_enabled: false,
            catalog: RwLock::new(Catalog {
                next_inode: snapshot.next_inode,
                inode_reservation_end: snapshot.inode_reservation_end,
                next_handle: 1,
                inodes,
                entries,
                handles: BTreeMap::new(),
                lookup_counts: BTreeMap::new(),
            }),
        })
    }

    /// Executes one byte-exact POSIX semantic operation.
    ///
    /// # Errors
    ///
    /// Returns a stable semantic error for invalid user input, missing objects,
    /// invalid handles, or exhausted configured resources.
    #[allow(clippy::needless_pass_by_value)]
    pub fn dispatch(
        &self,
        context: RequestContext,
        operation: Operation<'_>,
    ) -> Result<Reply, PosixError> {
        match operation {
            Operation::Lookup { parent, name } => self.lookup(parent, name),
            Operation::GetAttr { inode } => self.getattr(inode),
            Operation::Create {
                parent,
                name,
                mode,
                options,
                exclusive,
                truncate,
            } => self.create(CreateRequest {
                context,
                parent,
                name,
                mode,
                options,
                exclusive,
                truncate,
            }),
            Operation::Open {
                inode,
                options,
                truncate,
            } => self.open(inode, options, truncate),
            Operation::Read {
                inode,
                handle,
                offset,
                length,
            } => self.read(inode, handle, offset, length),
            Operation::Write {
                inode,
                handle,
                offset,
                data,
            } => self.write(inode, handle, offset, data),
            Operation::SetLength {
                inode,
                handle,
                length,
            } => self.set_length(inode, handle, length),
            Operation::Sync {
                inode,
                handle,
                data_only: _,
            } => self.sync(inode, handle),
            Operation::Release { inode, handle } => self.release(inode, handle),
            Operation::Unlink { parent, name } => self.unlink(parent, name),
            Operation::ReadDirectory {
                inode,
                offset,
                acquire_lookup,
            } => self.read_directory(inode, offset, acquire_lookup),
            Operation::Forget {
                inode,
                lookup_count,
            } => Ok(self.forget(inode, lookup_count)),
        }
    }

    fn lookup(&self, parent: InodeId, name: &[u8]) -> Result<Reply, PosixError> {
        self.validate_name(name)?;
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, parent)?;
        let key = (parent, name.to_vec());
        let inode = *catalog.entries.get(&key).ok_or(PosixError::NoEntry)?;
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .expect("ASSERT: directory entry must reference a live inode");
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        assert!(
            state.link_count > 0,
            "ASSERT: a named inode must have a positive link count"
        );
        let attr = state.attributes(inode);
        drop(state);
        acquire_lookup(&mut catalog, inode, 1)?;
        Ok(Reply::Entry(Entry { attr }))
    }

    fn getattr(&self, inode: InodeId) -> Result<Reply, PosixError> {
        let object = self.resolve_inode(inode)?;
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        Ok(Reply::Attr(state.attributes(inode)))
    }

    fn create(&self, request: CreateRequest<'_>) -> Result<Reply, PosixError> {
        self.validate_name(request.name)?;
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, request.parent)?;
        let key = (request.parent, request.name.to_vec());
        if let Some(&inode) = catalog.entries.get(&key) {
            if request.exclusive {
                return Err(PosixError::Exists);
            }
            if !self.mutations_enabled && request.options.access != AccessMode::ReadOnly {
                return Err(PosixError::ReadOnly);
            }
            return open_existing_for_create(&mut catalog, inode, request);
        }
        if !self.mutations_enabled {
            return Err(PosixError::ReadOnly);
        }
        create_new_file(&mut catalog, key, request)
    }

    fn open(
        &self,
        inode: InodeId,
        options: OpenOptions,
        truncate: bool,
    ) -> Result<Reply, PosixError> {
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .ok_or(PosixError::NoEntry)?;
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.kind == FileKind::Directory {
            return Err(PosixError::IsDirectory);
        }
        if !self.mutations_enabled && options.access != AccessMode::ReadOnly {
            return Err(PosixError::ReadOnly);
        }
        if truncate && options.access == AccessMode::ReadOnly {
            return Err(PosixError::BadHandle);
        }
        let next_sequence = if truncate && state.data.logical_size() != 0 {
            Some(
                state
                    .mutation_sequence
                    .checked_add(1)
                    .ok_or(PosixError::NoSpace)?,
            )
        } else {
            None
        };

        let handle = allocate_handle(&mut catalog)?;
        if let Some(sequence) = next_sequence {
            state.data.truncate(0, sequence)?;
            state.mutation_sequence = sequence;
        }
        drop(state);
        assert!(
            catalog
                .handles
                .insert(handle, OpenHandle { inode, options })
                .is_none(),
            "ASSERT: monotonic handle allocator returned a live ID"
        );
        Ok(Reply::Opened(handle))
    }

    fn read(
        &self,
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        length: u32,
    ) -> Result<Reply, PosixError> {
        let (object, open) = self.resolve_open_file(inode, handle)?;
        if open.options.access == AccessMode::WriteOnly {
            return Err(PosixError::BadHandle);
        }
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        assert_eq!(
            state.kind,
            FileKind::Regular,
            "ASSERT: a file handle must reference a regular inode"
        );
        let plan = state.data.plan_read(offset, length)?;
        drop(state);
        Ok(Reply::Data(plan.execute()?))
    }

    fn write(
        &self,
        inode: InodeId,
        handle: HandleId,
        requested_offset: u64,
        data: &[u8],
    ) -> Result<Reply, PosixError> {
        if !self.mutations_enabled {
            return Err(PosixError::ReadOnly);
        }
        let written = u32::try_from(data.len()).map_err(|_| PosixError::FileTooLarge)?;
        let (object, open) = self.resolve_open_file(inode, handle)?;
        if open.options.access == AccessMode::ReadOnly {
            return Err(PosixError::BadHandle);
        }
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        assert_eq!(
            state.kind,
            FileKind::Regular,
            "ASSERT: a file handle must reference a regular inode"
        );
        if data.is_empty() {
            return Ok(Reply::Written {
                bytes: 0,
                mutation_sequence: state.mutation_sequence,
            });
        }
        let offset = if open.options.append {
            state.data.logical_size()
        } else {
            requested_offset
        };
        let data_length = u64::try_from(data.len()).expect("ASSERT: usize must fit in u64");
        let end = offset
            .checked_add(data_length)
            .ok_or(PosixError::FileTooLarge)?;
        if end > self.config.maximum_file_bytes {
            return Err(PosixError::FileTooLarge);
        }
        let next_sequence = state
            .mutation_sequence
            .checked_add(u64::from(!data.is_empty()))
            .ok_or(PosixError::NoSpace)?;

        state.data.write(offset, data, next_sequence)?;
        state.mutation_sequence = next_sequence;

        Ok(Reply::Written {
            bytes: written,
            mutation_sequence: state.mutation_sequence,
        })
    }

    fn set_length(
        &self,
        inode: InodeId,
        handle: Option<HandleId>,
        length: u64,
    ) -> Result<Reply, PosixError> {
        if length > self.config.maximum_file_bytes {
            return Err(PosixError::FileTooLarge);
        }
        let object = match handle {
            Some(handle) => {
                let (object, open) = self.resolve_open_file(inode, handle)?;
                if open.options.access == AccessMode::ReadOnly {
                    return Err(PosixError::BadHandle);
                }
                object
            }
            None => self.resolve_inode(inode)?,
        };
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.kind == FileKind::Directory {
            return Err(PosixError::IsDirectory);
        }
        if length == state.data.logical_size() {
            return Ok(Reply::Attr(state.attributes(inode)));
        }
        if !self.mutations_enabled {
            return Err(PosixError::ReadOnly);
        }
        let next_sequence = state
            .mutation_sequence
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        state.data.truncate(length, next_sequence)?;
        state.mutation_sequence = next_sequence;
        Ok(Reply::Attr(state.attributes(inode)))
    }

    fn sync(&self, inode: InodeId, handle: HandleId) -> Result<Reply, PosixError> {
        let _ = self.resolve_open_file(inode, handle)?;
        Ok(Reply::Empty)
    }

    fn release(&self, inode: InodeId, handle: HandleId) -> Result<Reply, PosixError> {
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let open = *catalog.handles.get(&handle).ok_or(PosixError::BadHandle)?;
        if open.inode != inode {
            return Err(PosixError::BadHandle);
        }
        let removed = catalog.handles.remove(&handle);
        assert!(removed.is_some(), "ASSERT: validated handle disappeared");

        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .expect("ASSERT: an open handle must pin its inode");
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        let is_orphan = state.link_count == 0;
        drop(state);
        let has_lookup = catalog.lookup_counts.get(&inode).copied().unwrap_or(0) != 0;
        if is_orphan
            && !has_lookup
            && !catalog
                .handles
                .values()
                .any(|candidate| candidate.inode == inode)
        {
            let removed = catalog.inodes.remove(&inode);
            assert!(removed.is_some(), "ASSERT: orphan inode disappeared early");
        }
        Ok(Reply::Empty)
    }

    fn unlink(&self, parent: InodeId, name: &[u8]) -> Result<Reply, PosixError> {
        self.validate_name(name)?;
        if !self.mutations_enabled {
            return Err(PosixError::ReadOnly);
        }
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, parent)?;
        let key = (parent, name.to_vec());
        let inode = *catalog.entries.get(&key).ok_or(PosixError::NoEntry)?;
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .expect("ASSERT: directory entry must reference a live inode");
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.kind == FileKind::Directory {
            return Err(PosixError::IsDirectory);
        }
        assert_eq!(
            state.link_count, 1,
            "ASSERT: first slice has exactly one name per regular inode"
        );
        let next_sequence = state
            .mutation_sequence
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        state.link_count = 0;
        state.mutation_sequence = next_sequence;
        drop(state);
        let removed = catalog.entries.remove(&key);
        assert_eq!(removed, Some(inode), "ASSERT: validated name disappeared");

        let has_lookup = catalog.lookup_counts.get(&inode).copied().unwrap_or(0) != 0;
        if !has_lookup
            && !catalog
                .handles
                .values()
                .any(|candidate| candidate.inode == inode)
        {
            let removed = catalog.inodes.remove(&inode);
            assert!(
                removed.is_some(),
                "ASSERT: unlinked inode disappeared early"
            );
        }
        Ok(Reply::Empty)
    }

    fn read_directory(
        &self,
        inode: InodeId,
        offset: i64,
        acquire_lookup_references: bool,
    ) -> Result<Reply, PosixError> {
        if offset < 0 {
            return Err(PosixError::InvalidArgument);
        }
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, inode)?;
        let directory = catalog
            .inodes
            .get(&inode)
            .expect("ASSERT: validated directory must remain in catalog");
        let directory_attr = directory
            .state
            .read()
            .expect("ASSERT: inode lock poisoned")
            .attributes(inode);
        let start = usize::try_from(offset).map_err(|_| PosixError::InvalidArgument)?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(MAX_DIRECTORY_ENTRIES_PER_REPLY)
            .map_err(|_| PosixError::OutOfMemory)?;
        if start == 0 {
            result.push(DirectoryEntry {
                inode,
                kind: FileKind::Directory,
                attr: directory_attr,
                name: b".".to_vec(),
                next_offset: 1,
            });
        }
        if start <= 1 {
            result.push(DirectoryEntry {
                inode: ROOT_INODE,
                kind: FileKind::Directory,
                attr: directory_attr,
                name: b"..".to_vec(),
                next_offset: 2,
            });
        }
        let child_skip = start.saturating_sub(2);
        let remaining = MAX_DIRECTORY_ENTRIES_PER_REPLY - result.len();
        for (child_index, ((_, name), child)) in catalog
            .entries
            .iter()
            .filter(|((parent, _), _)| *parent == inode)
            .enumerate()
            .skip(child_skip)
            .take(remaining)
        {
            let object = catalog
                .inodes
                .get(child)
                .expect("ASSERT: directory entry must reference a live inode");
            let state = object.state.read().expect("ASSERT: inode lock poisoned");
            let next_offset = child_index.checked_add(3).ok_or(PosixError::NoSpace)?;
            result.push(DirectoryEntry {
                inode: *child,
                kind: state.kind,
                attr: state.attributes(*child),
                name: name.clone(),
                next_offset: i64::try_from(next_offset).map_err(|_| PosixError::NoSpace)?,
            });
        }
        if acquire_lookup_references {
            let mut additions = BTreeMap::<InodeId, u64>::new();
            for entry in &result {
                let count = additions.entry(entry.inode).or_default();
                *count = count.checked_add(1).ok_or(PosixError::NoSpace)?;
            }
            for (&entry_inode, &count) in &additions {
                let current = catalog
                    .lookup_counts
                    .get(&entry_inode)
                    .copied()
                    .unwrap_or(0);
                current.checked_add(count).ok_or(PosixError::NoSpace)?;
            }
            for (entry_inode, count) in additions {
                acquire_lookup(&mut catalog, entry_inode, count)?;
            }
        }
        Ok(Reply::Directory(result))
    }

    fn forget(&self, inode: InodeId, lookup_count: u64) -> Reply {
        if lookup_count == 0 {
            return Reply::Empty;
        }
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let Some(current) = catalog.lookup_counts.get(&inode).copied() else {
            return Reply::Empty;
        };
        if lookup_count >= current {
            catalog.lookup_counts.remove(&inode);
        } else {
            catalog.lookup_counts.insert(inode, current - lookup_count);
        }
        let Some(object) = catalog.inodes.get(&inode).cloned() else {
            return Reply::Empty;
        };
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        let is_orphan = state.link_count == 0;
        drop(state);
        let has_lookup = catalog.lookup_counts.get(&inode).copied().unwrap_or(0) != 0;
        let has_handle = catalog
            .handles
            .values()
            .any(|candidate| candidate.inode == inode);
        if is_orphan && !has_lookup && !has_handle {
            let removed = catalog.inodes.remove(&inode);
            assert!(
                removed.is_some(),
                "ASSERT: forgotten orphan must remain live"
            );
        }
        Reply::Empty
    }

    fn resolve_inode(&self, inode: InodeId) -> Result<Arc<Inode>, PosixError> {
        self.catalog
            .read()
            .expect("ASSERT: catalog lock poisoned")
            .inodes
            .get(&inode)
            .cloned()
            .ok_or(PosixError::NoEntry)
    }

    fn resolve_open_file(
        &self,
        inode: InodeId,
        handle: HandleId,
    ) -> Result<(Arc<Inode>, OpenHandle), PosixError> {
        let catalog = self.catalog.read().expect("ASSERT: catalog lock poisoned");
        let open = *catalog.handles.get(&handle).ok_or(PosixError::BadHandle)?;
        if open.inode != inode {
            return Err(PosixError::BadHandle);
        }
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .expect("ASSERT: open handle must pin its inode");
        Ok((object, open))
    }

    fn validate_name(&self, name: &[u8]) -> Result<(), PosixError> {
        validate_component(&self.config, name)
    }
}

fn validate_component(config: &NamespaceConfig, name: &[u8]) -> Result<(), PosixError> {
    if name.len() > config.maximum_name_bytes {
        return Err(PosixError::NameTooLong);
    }
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') || name.contains(&0)
    {
        return Err(PosixError::InvalidName);
    }
    Ok(())
}

fn open_existing_for_create(
    catalog: &mut Catalog,
    inode: InodeId,
    request: CreateRequest<'_>,
) -> Result<Reply, PosixError> {
    let object = catalog
        .inodes
        .get(&inode)
        .cloned()
        .expect("ASSERT: directory entry must reference a live inode");
    let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
    if state.kind == FileKind::Directory {
        return Err(PosixError::IsDirectory);
    }
    if request.truncate && request.options.access == AccessMode::ReadOnly {
        return Err(PosixError::BadHandle);
    }
    let next_sequence = if request.truncate && state.data.logical_size() != 0 {
        Some(
            state
                .mutation_sequence
                .checked_add(1)
                .ok_or(PosixError::NoSpace)?,
        )
    } else {
        None
    };
    let next_lookup = catalog
        .lookup_counts
        .get(&inode)
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(PosixError::NoSpace)?;
    let handle = allocate_handle(catalog)?;
    if let Some(sequence) = next_sequence {
        state.data.truncate(0, sequence)?;
        state.mutation_sequence = sequence;
    }
    let attr = state.attributes(inode);
    drop(state);
    assert!(
        catalog
            .handles
            .insert(
                handle,
                OpenHandle {
                    inode,
                    options: request.options,
                },
            )
            .is_none(),
        "ASSERT: monotonic handle allocator returned a live ID"
    );
    catalog.lookup_counts.insert(inode, next_lookup);
    Ok(Reply::Created {
        entry: Entry { attr },
        handle,
    })
}

fn create_new_file(
    catalog: &mut Catalog,
    key: (InodeId, Vec<u8>),
    request: CreateRequest<'_>,
) -> Result<Reply, PosixError> {
    let inode = allocate_inode(catalog)?;
    let handle = allocate_handle(catalog)?;
    let object = Arc::new(Inode {
        state: RwLock::new(InodeState {
            kind: FileKind::Regular,
            mode: request.mode & 0o7777,
            uid: request.context.uid,
            gid: request.context.gid,
            link_count: 1,
            mutation_sequence: 0,
            data: VersionedFile::new_empty(),
        }),
    });
    let attr = object
        .state
        .read()
        .expect("ASSERT: new inode lock poisoned")
        .attributes(inode);

    assert!(
        catalog.inodes.insert(inode, object).is_none(),
        "ASSERT: monotonic inode allocator returned a live ID"
    );
    assert!(
        catalog.lookup_counts.insert(inode, 1).is_none(),
        "ASSERT: new inode must not have lookup references"
    );
    assert!(
        catalog.entries.insert(key, inode).is_none(),
        "ASSERT: create replaced an existing directory entry"
    );
    assert!(
        catalog
            .handles
            .insert(
                handle,
                OpenHandle {
                    inode,
                    options: request.options,
                },
            )
            .is_none(),
        "ASSERT: monotonic handle allocator returned a live ID"
    );

    Ok(Reply::Created {
        entry: Entry { attr },
        handle,
    })
}

fn validate_directory(catalog: &Catalog, inode: InodeId) -> Result<(), PosixError> {
    let object = catalog.inodes.get(&inode).ok_or(PosixError::NoEntry)?;
    let state = object.state.read().expect("ASSERT: inode lock poisoned");
    if state.kind != FileKind::Directory {
        return Err(PosixError::NotDirectory);
    }
    Ok(())
}

fn allocate_inode(catalog: &mut Catalog) -> Result<InodeId, PosixError> {
    if catalog.next_inode >= catalog.inode_reservation_end {
        return Err(PosixError::NoSpace);
    }
    let inode = InodeId::new(catalog.next_inode).ok_or(PosixError::NoSpace)?;
    catalog.next_inode = catalog
        .next_inode
        .checked_add(1)
        .ok_or(PosixError::NoSpace)?;
    Ok(inode)
}

fn allocate_handle(catalog: &mut Catalog) -> Result<HandleId, PosixError> {
    let handle = HandleId::new(catalog.next_handle).ok_or(PosixError::NoSpace)?;
    catalog.next_handle = catalog
        .next_handle
        .checked_add(1)
        .ok_or(PosixError::NoSpace)?;
    Ok(handle)
}

fn acquire_lookup(catalog: &mut Catalog, inode: InodeId, count: u64) -> Result<(), PosixError> {
    assert!(count > 0, "ASSERT: lookup acquisition must be positive");
    assert!(
        catalog.inodes.contains_key(&inode),
        "ASSERT: lookup reference must target a live inode"
    );
    let next = catalog
        .lookup_counts
        .get(&inode)
        .copied()
        .unwrap_or(0)
        .checked_add(count)
        .ok_or(PosixError::NoSpace)?;
    catalog.lookup_counts.insert(inode, next);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Inode, MAX_COALESCED_EXTENT_BYTES, SparseData};

    #[test]
    fn inode_sequencers_start_on_separate_cache_lines() {
        assert_eq!(std::mem::align_of::<Inode>(), 64);
    }

    #[test]
    fn sequential_writes_coalesce_into_bounded_extents() {
        let mut data = SparseData::default();
        let block = vec![0x5a; 1_024 * 1_024];

        for index in 0_u64..20 {
            data.write(index * 1_024 * 1_024, &block)
                .expect("sequential write must succeed");
        }

        assert_eq!(data.extents.len(), 2);
        assert_eq!(data.extents[&0].len(), MAX_COALESCED_EXTENT_BYTES);
        assert_eq!(data.extents[&(16 * 1_024 * 1_024)].len(), 4 * 1_024 * 1_024);
        assert_eq!(data.allocated_bytes(), 20 * 1_024 * 1_024);
        assert_eq!(
            data.read(1, 0).expect("zero-length read must succeed"),
            Vec::<u8>::new()
        );
        assert_eq!(
            data.read(16 * 1_024 * 1_024 - 8, 16)
                .expect("cross-extent read must succeed"),
            vec![0x5a; 16]
        );
        data.audit_valid();
    }
}
