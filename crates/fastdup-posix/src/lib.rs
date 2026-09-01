//! Byte-exact POSIX namespace semantics and the low-level FUSE adapter.
//!
//! The first implementation checkpoint is deliberately volatile. It proves
//! live POSIX semantics but does not claim crash durability.

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::ops::Bound::Excluded;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

mod fuse_adapter;
mod inode_metadata;
mod logical_quota;
mod versioned_file;

use versioned_file::VersionedFile;

pub use fuse_adapter::{
    FrontendTelemetry, FrontendTelemetrySnapshot, FuseFilesystem, StatFsSnapshot,
    StatFsSnapshotError, StatFsSource, volatile_mount_options,
};
pub use inode_metadata::{
    ExtendedAttribute, FS_IMMUTABLE_FL, InodeMetadata, POSIX_ACL_ACCESS_XATTR,
    POSIX_ACL_DEFAULT_XATTR, XattrSetMode,
};
use logical_quota::LogicalQuotaTable;
pub use logical_quota::{LogicalQuotaRule, LogicalQuotaStatus};
pub use versioned_file::CommittedFile;

pub const SMALL_FILE_SPILL_BYTES_V1: u64 = 8 * 1_024 * 1_024;
pub const SMALL_FILE_PLACEMENT_XATTR: &[u8] = b"user.fastdup.placement";

/// Format-independent reduction recipe retained by verified write-through DATA.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedDataRecipe {
    /// One complete immutable content-addressed Chunk.
    Chunk { chunk_id: [u8; 32] },
    /// One logical byte range inside a complete immutable Chunk.
    ChunkSlice {
        chunk_id: [u8; 32],
        chunk_length: u32,
        chunk_offset: u32,
    },
    /// One byte repeated for the complete logical extent.
    Fill { value: u8 },
}

/// One range-local recipe available to a generation checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreparedCommitExtent {
    offset: u64,
    length: u64,
    recipe: PreparedDataRecipe,
    retained_manifest_root: Option<[u8; 32]>,
    retained_source_offset: u64,
}

impl PreparedCommitExtent {
    const fn new(offset: u64, length: u64, recipe: PreparedDataRecipe) -> Self {
        assert!(length > 0, "ASSERT: a prepared commit extent is nonempty");
        assert!(
            offset.checked_add(length).is_some(),
            "ASSERT: a prepared commit extent cannot overflow"
        );
        Self {
            offset,
            length,
            recipe,
            retained_manifest_root: None,
            retained_source_offset: 0,
        }
    }

    /// Constructs one externally supplied immutable recipe range.
    ///
    /// # Errors
    ///
    /// Rejects empty or overflowing ranges before they cross the checkpoint
    /// integrity boundary.
    pub fn try_new(
        offset: u64,
        length: u64,
        recipe: PreparedDataRecipe,
    ) -> Result<Self, PosixError> {
        if length == 0 || offset.checked_add(length).is_none() {
            return Err(PosixError::InvalidArgument);
        }
        Ok(Self::new(offset, length, recipe))
    }

    /// Constructs a recipe proven to originate from one installed immutable
    /// Manifest range. The durable Store must still validate this claim
    /// against the exact predecessor Namespace Root before reusing its DATA
    /// proof.
    ///
    /// # Errors
    ///
    /// Rejects a zero Manifest identity and empty, destination-overflowing, or
    /// source-overflowing ranges.
    pub fn try_new_retained(
        offset: u64,
        length: u64,
        recipe: PreparedDataRecipe,
        manifest_root: [u8; 32],
        source_offset: u64,
    ) -> Result<Self, PosixError> {
        let mut extent = Self::try_new(offset, length, recipe)?;
        if manifest_root == [0; 32] || source_offset.checked_add(length).is_none() {
            return Err(PosixError::InvalidArgument);
        }
        extent.retained_manifest_root = Some(manifest_root);
        extent.retained_source_offset = source_offset;
        Ok(extent)
    }

    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    #[must_use]
    pub const fn length(self) -> u64 {
        self.length
    }

    #[must_use]
    pub const fn recipe(self) -> PreparedDataRecipe {
        self.recipe
    }

    #[must_use]
    pub const fn retained_manifest_root(self) -> Option<[u8; 32]> {
        self.retained_manifest_root
    }

    #[must_use]
    pub const fn retained_source_offset(self) -> Option<u64> {
        if self.retained_manifest_root.is_some() {
            Some(self.retained_source_offset)
        } else {
            None
        }
    }
}

pub const ROOT_INODE: InodeId = InodeId(NonZeroU64::MIN);
const MAX_DIRECTORY_ENTRIES_PER_REPLY: usize = 256;
const MAX_RETAINED_PAYLOAD_AMPLIFICATION: usize = 4;
const MAXIMUM_RECORD_LOCKS: usize = 65_536;

/// One immutable, owned write payload shared by the POSIX Dirty Extent Map and
/// asynchronous mutation observers.
///
/// Clones and checked slices retain the same allocation. The Namespace creates
/// exactly one backing allocation when adapting the borrowed FUSE request;
/// observers may therefore retain or split this value after the write reply
/// without copying its bytes.
#[derive(Clone, Debug)]
pub struct MutationPayload {
    bytes: Bytes,
    backing_bytes: usize,
}

impl PartialEq for MutationPayload {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for MutationPayload {}

impl MutationPayload {
    /// Adopts one already-owned immutable request buffer without copying it.
    #[must_use]
    pub fn from_owned_bytes(bytes: Vec<u8>) -> Self {
        let backing_bytes = bytes.capacity().max(bytes.len());
        Self::from_shared_bytes(Bytes::from(bytes), backing_bytes)
    }

    /// Adopts a shared immutable view into an already-owned request buffer.
    ///
    /// `backing_bytes` accounts for the complete retained allocation, which
    /// may be larger than this payload view.
    ///
    /// # Panics
    ///
    /// Panics if `backing_bytes` is smaller than the retained byte view.
    #[must_use]
    pub fn from_shared_bytes(bytes: Bytes, backing_bytes: usize) -> Self {
        assert!(
            backing_bytes >= bytes.len(),
            "ASSERT: retained backing covers its immutable byte view"
        );
        Self {
            bytes,
            backing_bytes,
        }
    }

    /// Copies one borrowed request buffer into its single retained backing.
    ///
    /// # Errors
    ///
    /// Returns [`PosixError::OutOfMemory`] when the owned allocation cannot be
    /// reserved.
    pub fn try_copy_from_slice(source: &[u8]) -> Result<Self, PosixError> {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(source.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        owned.extend_from_slice(source);
        Ok(Self::from_owned_bytes(owned))
    }

    /// Returns the complete immutable byte view.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the logical byte length of this view.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Reports whether this view is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Creates a zero-copy sub-view when `start..end` lies inside this payload.
    #[must_use]
    pub fn checked_slice(&self, start: usize, end: usize) -> Option<Self> {
        if start > end || end > self.bytes.len() {
            return None;
        }
        Some(Self {
            bytes: self.bytes.slice(start..end),
            backing_bytes: self.backing_bytes,
        })
    }

    fn retained_fragment(&self, start: usize, end: usize) -> Result<Self, PosixError> {
        let fragment_bytes = end
            .checked_sub(start)
            .expect("ASSERT: retained fragment bounds are ordered");
        // Share large tails while they move through write-through ingestion.
        // Compact a small long-lived survivor once so it cannot pin an
        // arbitrarily larger FUSE request allocation.
        let minimum_shared_bytes = self
            .backing_bytes
            .div_ceil(MAX_RETAINED_PAYLOAD_AMPLIFICATION);
        if fragment_bytes >= minimum_shared_bytes {
            return Ok(self
                .checked_slice(start, end)
                .expect("ASSERT: retained fragment lies inside its payload"));
        }
        Self::try_copy_from_slice(
            self.as_bytes()
                .get(start..end)
                .expect("ASSERT: compact fragment lies inside its payload"),
        )
    }

    #[cfg(test)]
    fn starts_at_same_address(&self, other: &Self) -> bool {
        self.bytes.as_ptr() == other.bytes.as_ptr()
    }

    #[cfg(test)]
    fn is_uniquely_owned(&self) -> bool {
        self.bytes.is_unique()
    }
}

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

/// Opaque identity of one in-flight atomic generation cut.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommitToken(NonZeroU64);

impl CommitToken {
    const fn new(raw: u64) -> Option<Self> {
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
    Symlink,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct PosixTimestamp {
    pub seconds: i64,
    pub nanoseconds: u32,
}

impl PosixTimestamp {
    #[must_use]
    pub const fn new(seconds: i64, nanoseconds: u32) -> Self {
        Self {
            seconds,
            nanoseconds,
        }
    }

    fn now() -> Self {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(value) => Self {
                seconds: i64::try_from(value.as_secs()).unwrap_or(i64::MAX),
                nanoseconds: value.subsec_nanos(),
            },
            Err(value) => {
                let duration = value.duration();
                Self {
                    seconds: -i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
                    nanoseconds: duration.subsec_nanos(),
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PosixTimes {
    pub atime: PosixTimestamp,
    pub mtime: PosixTimestamp,
    pub ctime: PosixTimestamp,
}

impl PosixTimes {
    fn now() -> Self {
        let now = PosixTimestamp::now();
        Self {
            atime: now,
            mtime: now,
            ctime: now,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InodeAttributesUpdate {
    pub mode: Option<u16>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub atime: Option<PosixTimestamp>,
    pub mtime: Option<PosixTimestamp>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockKind {
    Read,
    Write,
    Unlock,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FallocateMode {
    Allocate { keep_size: bool },
    PunchHole,
    ZeroRange { keep_size: bool },
    CollapseRange,
    InsertRange,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeekKind {
    Data,
    Hole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileLock {
    pub start: u64,
    pub end: u64,
    pub kind: LockKind,
    pub pid: u32,
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
    pub times: PosixTimes,
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
    GetXattr {
        inode: InodeId,
        name: &'a [u8],
    },
    ListXattrs {
        inode: InodeId,
    },
    SetXattr {
        inode: InodeId,
        name: &'a [u8],
        value: &'a [u8],
        mode: XattrSetMode,
    },
    RemoveXattr {
        inode: InodeId,
        name: &'a [u8],
    },
    GetFileFlags {
        inode: InodeId,
    },
    SetFileFlags {
        inode: InodeId,
        flags: u32,
    },
    SetMode {
        inode: InodeId,
        mode: u16,
    },
    SetAttributes {
        inode: InodeId,
        update: InodeAttributesUpdate,
    },
    Link {
        inode: InodeId,
        new_parent: InodeId,
        new_name: &'a [u8],
    },
    Symlink {
        parent: InodeId,
        name: &'a [u8],
        target: &'a [u8],
    },
    Readlink {
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
    CreateWithUmask {
        parent: InodeId,
        name: &'a [u8],
        mode: u16,
        umask: u16,
        options: OpenOptions,
        exclusive: bool,
        truncate: bool,
    },
    Mkdir {
        parent: InodeId,
        name: &'a [u8],
        mode: u16,
    },
    MkdirWithUmask {
        parent: InodeId,
        name: &'a [u8],
        mode: u16,
        umask: u16,
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
    CloneRange {
        source_inode: InodeId,
        source_handle: HandleId,
        source_offset: u64,
        target_inode: InodeId,
        target_handle: HandleId,
        target_offset: u64,
        length: u64,
    },
    Fallocate {
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        length: u64,
        mode: FallocateMode,
    },
    Seek {
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        kind: SeekKind,
    },
    Sync {
        inode: InodeId,
        handle: HandleId,
        data_only: bool,
    },
    GetLock {
        inode: InodeId,
        handle: HandleId,
        owner: u64,
        lock: FileLock,
    },
    SetLock {
        inode: InodeId,
        handle: HandleId,
        owner: u64,
        lock: FileLock,
    },
    UnlockOwner {
        inode: InodeId,
        handle: HandleId,
        owner: u64,
    },
    Release {
        inode: InodeId,
        handle: HandleId,
    },
    Unlink {
        parent: InodeId,
        name: &'a [u8],
    },
    Rmdir {
        parent: InodeId,
        name: &'a [u8],
    },
    Rename {
        parent: InodeId,
        name: &'a [u8],
        new_parent: InodeId,
        new_name: &'a [u8],
        no_replace: bool,
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

const MUTATION_METADATA_INCREMENT_BYTES_V1: u64 = 2 * 1_024 * 1_024;
const DATA_RECORD_SAFETY_BYTES_V1: u64 = 4 * 1_024;
const DATA_RECHUNK_MINIMUM_BYTES_V1: u64 = 256 * 1_024;

impl Operation<'_> {
    fn is_durable_mutation(&self) -> bool {
        matches!(
            self,
            Self::SetXattr { .. }
                | Self::RemoveXattr { .. }
                | Self::SetFileFlags { .. }
                | Self::SetMode { .. }
                | Self::SetAttributes { .. }
                | Self::Link { .. }
                | Self::Symlink { .. }
                | Self::Create { .. }
                | Self::CreateWithUmask { .. }
                | Self::Mkdir { .. }
                | Self::MkdirWithUmask { .. }
                | Self::Open { truncate: true, .. }
                | Self::Write { .. }
                | Self::SetLength { .. }
                | Self::CloneRange { .. }
                | Self::Fallocate { .. }
                | Self::Unlink { .. }
                | Self::Rmdir { .. }
                | Self::Rename { .. }
        )
    }

    fn commit_capacity_claim(&self) -> CommitCapacityClaim {
        let metadata = CommitCapacityClaim::new(MUTATION_METADATA_INCREMENT_BYTES_V1, 0);
        match self {
            Self::SetXattr { .. }
            | Self::SetMode { .. }
            | Self::Link { .. }
            | Self::Fallocate {
                mode:
                    FallocateMode::Allocate { .. }
                    | FallocateMode::ZeroRange { .. }
                    | FallocateMode::CollapseRange
                    | FallocateMode::InsertRange,
                ..
            } => metadata,
            Self::CloneRange { length, .. } if *length != 0 => metadata,
            _ => CommitCapacityClaim::default(),
        }
    }

    fn requires_mutation_admission(&self) -> bool {
        self.is_durable_mutation()
            || matches!(
                self,
                Self::Open {
                    options,
                    truncate,
                    ..
                } if options.access != AccessMode::ReadOnly || *truncate
            )
    }
}

fn write_capacity_claim(
    payload_bytes: usize,
    metadata_bytes: u64,
    small_file: bool,
) -> Result<CommitCapacityClaim, PosixError> {
    if payload_bytes == 0 {
        return Ok(CommitCapacityClaim::default());
    }
    let payload_bytes = u64::try_from(payload_bytes).map_err(|_| PosixError::NoSpace)?;
    let physical_bytes = payload_bytes
        .max(DATA_RECHUNK_MINIMUM_BYTES_V1)
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(DATA_RECORD_SAFETY_BYTES_V1))
        .ok_or(PosixError::NoSpace)?;
    if small_file {
        Ok(CommitCapacityClaim::with_small_file_bytes(
            metadata_bytes
                .checked_add(physical_bytes)
                .ok_or(PosixError::NoSpace)?,
            physical_bytes,
        ))
    } else {
        Ok(CommitCapacityClaim::new(metadata_bytes, physical_bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reply {
    Entry(Entry),
    Attr(FileAttr),
    Created {
        entry: Entry,
        handle: HandleId,
    },
    Opened(HandleId),
    Data(Vec<u8>),
    LinkTarget(Vec<u8>),
    Xattr(Vec<u8>),
    FileFlags(u32),
    Written {
        bytes: u32,
        mutation_sequence: u64,
        offset: u64,
    },
    Cloned {
        bytes: u64,
        mutation_sequence: u64,
    },
    Offset(u64),
    Lock(FileLock),
    Directory(Vec<DirectoryEntry>),
    Empty,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PosixError {
    NoEntry,
    Exists,
    NotDirectory,
    IsDirectory,
    NotEmpty,
    InvalidName,
    NameTooLong,
    InvalidArgument,
    BadHandle,
    FileTooLarge,
    NoSpace,
    OutOfMemory,
    NoLocks,
    NoSuchAddress,
    NoData,
    TooBig,
    PermissionDenied,
    CrossDevice,
    Unsupported,
    Io,
    ReadOnly,
    Again,
}

/// Optional appliance-owned sink for successfully admitted content mutations.
///
/// Notifications happen after the live view was updated but before the POSIX
/// write reply is returned. The sink is acceleration only: it must retain its
/// own failure state and must never make an accepted mutation disappear from
/// the Namespace dirty overlay.
#[derive(Clone, Debug)]
pub struct ExternalizedExtent {
    inode: InodeId,
    offset: u64,
    through_sequence: u64,
    data: Arc<dyn CommittedFile>,
}

impl ExternalizedExtent {
    /// Constructs one verified DATA range that may replace matching resident
    /// dirty bytes. The source uses range-local coordinates starting at zero.
    ///
    /// # Errors
    ///
    /// Rejects an empty, sparse, or arithmetically overflowing source.
    pub fn new(
        inode: InodeId,
        offset: u64,
        through_sequence: u64,
        data: Arc<dyn CommittedFile>,
    ) -> Result<Self, PosixError> {
        let length = data.logical_size();
        if length == 0 || data.allocated_bytes() != length || offset.checked_add(length).is_none() {
            return Err(PosixError::InvalidArgument);
        }
        Ok(Self {
            inode,
            offset,
            through_sequence,
            data,
        })
    }
}

pub trait MutationObserver: std::fmt::Debug + Send + Sync {
    /// Records one writable handle for adaptive ingest scheduling.
    fn opened_write_handle(&self, _inode: InodeId) {}

    /// Releases one previously recorded writable handle.
    fn released_write_handle(&self, _inode: InodeId) {}

    fn accepted_write(
        &self,
        inode: InodeId,
        offset: u64,
        mutation_sequence: u64,
        small_file: bool,
        bytes: MutationPayload,
    ) -> Vec<ExternalizedExtent>;

    fn accepted_truncate(&self, inode: InodeId, mutation_sequence: u64, length: u64);

    /// Waits until every accepted mutation through `mutation_sequence` has
    /// left the observer's asynchronous processing queue.
    fn wait_through(&self, _inode: InodeId, _mutation_sequence: u64) {}
}

/// Pessimistic physical capacity required by one acknowledged mutation.
///
/// The claim describes additional durable footprint, not logical file size.
/// Cleanup operations therefore use a zero claim and consume the separately
/// protected Metadata floor when they eventually checkpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct CommitCapacityClaim {
    metadata_bytes: u64,
    data_bytes: u64,
    small_file_bytes: u64,
}

impl CommitCapacityClaim {
    #[must_use]
    pub const fn new(metadata_bytes: u64, data_bytes: u64) -> Self {
        Self {
            metadata_bytes,
            data_bytes,
            small_file_bytes: 0,
        }
    }

    #[must_use]
    pub const fn with_small_file_bytes(metadata_bytes: u64, small_file_bytes: u64) -> Self {
        Self {
            metadata_bytes,
            data_bytes: 0,
            small_file_bytes,
        }
    }

    #[must_use]
    pub const fn metadata_bytes(self) -> u64 {
        self.metadata_bytes
    }

    #[must_use]
    pub const fn data_bytes(self) -> u64 {
        self.data_bytes
    }

    #[must_use]
    pub const fn small_file_bytes(self) -> u64 {
        self.small_file_bytes
    }

    const fn is_empty(self) -> bool {
        self.metadata_bytes == 0 && self.data_bytes == 0 && self.small_file_bytes == 0
    }
}

/// Appliance-owned physical commit-capacity admission.
///
/// Implementations keep the request path syscall-free. Namespace invokes the
/// lifecycle callbacks while holding its mutation fence, so accepted claims
/// move into exactly the Commit Cut that contains their mutation.
pub trait CommitCapacityAdmission: std::fmt::Debug + Send + Sync {
    /// Claims physical capacity without changing live namespace state.
    ///
    /// # Errors
    ///
    /// Returns [`PosixError::NoSpace`] when either tier lacks cached headroom.
    fn try_reserve(&self, claim: CommitCapacityClaim) -> Result<(), PosixError>;
    fn cancel(&self, claim: CommitCapacityClaim);
    fn accept(&self, claim: CommitCapacityClaim);
    /// Releases accepted Metadata whose mutation was completely reversed
    /// before it entered a Frozen Commit Cut. DATA publication is irreversible
    /// at this boundary and deliberately has no matching callback.
    fn release_active_metadata(&self, bytes: u64);
    fn freeze(&self, token: CommitToken);
    fn complete(&self, token: CommitToken);
    /// Finishes Active claims when no recoverable Namespace mutation exists.
    /// Metadata may be released immediately; irreversible write-through DATA
    /// remains charged until a later physical observation includes it.
    fn finish_uncheckpointed_active(&self);
}

struct CommitCapacityReservation<'a> {
    admission: Option<&'a dyn CommitCapacityAdmission>,
    claim: CommitCapacityClaim,
    accepted: bool,
}

impl CommitCapacityReservation<'_> {
    fn accept(mut self) {
        if let Some(admission) = self.admission {
            admission.accept(self.claim);
        }
        self.accepted = true;
    }
}

impl Drop for CommitCapacityReservation<'_> {
    fn drop(&mut self) {
        if !self.accepted
            && let Some(admission) = self.admission
        {
            admission.cancel(self.claim);
        }
    }
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
    metadata: Arc<InodeMetadata>,
    times: PosixTimes,
    file: Arc<dyn CommittedFile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedDirectory {
    inode: InodeId,
    mode: u16,
    uid: u32,
    gid: u32,
    link_count: u32,
    mutation_sequence: u64,
    metadata: Arc<InodeMetadata>,
    times: PosixTimes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedSymlink {
    inode: InodeId,
    uid: u32,
    gid: u32,
    link_count: u32,
    mutation_sequence: u64,
    times: PosixTimes,
    target: Arc<[u8]>,
}

impl CommittedDirectory {
    /// Describes one verified committed directory inode.
    ///
    /// # Errors
    ///
    /// Rejects the implicit root inode and link counts below two.
    pub fn new(
        inode: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
    ) -> Result<Self, PosixError> {
        let inode = InodeId::new(inode).ok_or(PosixError::InvalidArgument)?;
        if inode <= ROOT_INODE || link_count < 2 {
            return Err(PosixError::InvalidArgument);
        }
        Ok(Self {
            inode,
            mode,
            uid,
            gid,
            link_count,
            mutation_sequence,
            metadata: Arc::new(InodeMetadata::default()),
            times: PosixTimes::default(),
        })
    }

    /// Describes one verified committed directory with extended metadata.
    ///
    /// # Errors
    ///
    /// Rejects the same malformed directory identity as [`Self::new`].
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_metadata(
        inode: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
        metadata: InodeMetadata,
    ) -> Result<Self, PosixError> {
        let mut committed = Self::new(inode, mode, uid, gid, link_count, mutation_sequence)?;
        committed.metadata = Arc::new(metadata);
        Ok(committed)
    }

    #[must_use]
    pub fn with_times(mut self, times: PosixTimes) -> Self {
        self.times = times;
        self
    }
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
            metadata: Arc::new(InodeMetadata::default()),
            times: PosixTimes::default(),
            file,
        })
    }

    /// Describes one verified committed regular inode with extended metadata.
    ///
    /// # Errors
    ///
    /// Rejects the same malformed inode state as [`Self::new`].
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_metadata(
        inode: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
        metadata: InodeMetadata,
        file: Arc<dyn CommittedFile>,
    ) -> Result<Self, PosixError> {
        let mut committed = Self::new(inode, mode, uid, gid, link_count, mutation_sequence, file)?;
        committed.metadata = Arc::new(metadata);
        Ok(committed)
    }

    #[must_use]
    pub fn with_times(mut self, times: PosixTimes) -> Self {
        self.times = times;
        self
    }
}

impl CommittedSymlink {
    /// Describes one verified committed symbolic link.
    ///
    /// # Errors
    ///
    /// Rejects invalid inode identities, link counts, and target lengths.
    pub fn new(
        inode: u64,
        uid: u32,
        gid: u32,
        link_count: u32,
        mutation_sequence: u64,
        times: PosixTimes,
        target: Vec<u8>,
    ) -> Result<Self, PosixError> {
        let inode = InodeId::new(inode).ok_or(PosixError::InvalidArgument)?;
        if inode <= ROOT_INODE || link_count == 0 || target.is_empty() || target.len() > 4_096 {
            return Err(PosixError::InvalidArgument);
        }
        Ok(Self {
            inode,
            uid,
            gid,
            link_count,
            mutation_sequence,
            times,
            target: Arc::from(target),
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

/// One coalesced changed range captured by an atomic namespace cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitRange {
    offset: u64,
    length: u64,
}

impl CommitRange {
    const fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    #[must_use]
    pub const fn length(self) -> u64 {
        self.length
    }
}

/// One immutable regular-file view captured by an atomic namespace cut.
#[derive(Clone, Debug)]
pub struct CommitInode {
    inode: InodeId,
    mode: u16,
    uid: u32,
    gid: u32,
    link_count: u32,
    mutation_sequence: u64,
    metadata: Arc<InodeMetadata>,
    times: PosixTimes,
    file: Arc<dyn CommittedFile>,
    frozen_epoch: Option<Arc<versioned_file::FrozenEpoch>>,
}

/// One immutable directory view captured by an atomic namespace cut.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitDirectory {
    inode: InodeId,
    mode: u16,
    uid: u32,
    gid: u32,
    link_count: u32,
    mutation_sequence: u64,
    metadata: Arc<InodeMetadata>,
    times: PosixTimes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitSymlink {
    inode: InodeId,
    uid: u32,
    gid: u32,
    link_count: u32,
    mutation_sequence: u64,
    times: PosixTimes,
    target: Arc<[u8]>,
}

impl CommitDirectory {
    #[must_use]
    pub const fn inode(&self) -> InodeId {
        self.inode
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
    pub fn metadata(&self) -> &InodeMetadata {
        &self.metadata
    }

    #[must_use]
    pub const fn times(&self) -> PosixTimes {
        self.times
    }
}

impl CommitSymlink {
    #[must_use]
    pub const fn inode(&self) -> InodeId {
        self.inode
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
    pub const fn times(&self) -> PosixTimes {
        self.times
    }
    #[must_use]
    pub fn target(&self) -> &[u8] {
        &self.target
    }
}

impl CommitInode {
    #[must_use]
    pub const fn inode(&self) -> InodeId {
        self.inode
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
    pub fn metadata(&self) -> &InodeMetadata {
        &self.metadata
    }

    #[must_use]
    pub const fn times(&self) -> PosixTimes {
        self.times
    }

    #[must_use]
    pub fn logical_size(&self) -> u64 {
        self.file.logical_size()
    }

    #[must_use]
    pub fn allocated_bytes(&self) -> u64 {
        self.file.allocated_bytes()
    }

    /// Evaluates Small-File placement against the exact frozen namespace cut.
    #[must_use]
    pub fn prefers_small_file_tier(&self, entries: &[CommitEntry]) -> bool {
        small_file_policy(
            self.logical_size(),
            &self.metadata,
            entries
                .iter()
                .filter(|entry| entry.target == self.inode)
                .map(|entry| entry.name.as_slice()),
        )
    }

    /// Returns the coalesced DATA/HOLE ranges changed since the immediately
    /// preceding installed version.
    ///
    /// File-size changes are represented by [`Self::logical_size`] and the
    /// preceding durable Manifest length; truncation may therefore return an
    /// empty range list. The ranges are a planning hint from the frozen epoch,
    /// never an authorization to skip byte or Manifest verification.
    ///
    /// # Errors
    ///
    /// Returns a bounded allocation or integrity error while materializing the
    /// immutable range summary.
    pub fn changed_ranges(&self) -> Result<Vec<CommitRange>, PosixError> {
        self.frozen_epoch
            .as_ref()
            .map_or_else(|| Ok(Vec::new()), |epoch| epoch.changed_ranges())
    }

    /// Returns verified write-through recipes wholly reusable in one range.
    ///
    /// Partial content-addressed Chunks are omitted because their identity no
    /// longer describes the requested bytes. FILL recipes may be clipped.
    /// Missing ranges remain readable through [`Self::read_at`] and must be
    /// planned normally by the checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an integrity, arithmetic, or bounded-allocation error while
    /// examining the immutable frozen epoch.
    pub fn prepared_extents_in_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Vec<PreparedCommitExtent>, PosixError> {
        self.frozen_epoch.as_ref().map_or_else(
            || Ok(Vec::new()),
            |epoch| epoch.prepared_extents_in_range(offset, length),
        )
    }

    /// Counts allocated bytes in the frozen version without reading content.
    ///
    /// # Errors
    ///
    /// Returns an integrity or bounded-range error from the frozen view.
    pub fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        self.file.allocated_bytes_in_range(offset, length)
    }

    /// Reads exact bytes from the frozen version, excluding later mutations.
    ///
    /// # Errors
    ///
    /// Returns a committed dependency, integrity, or bounded-resource error.
    pub fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.file.read_at(offset, length)
    }
}

/// One byte-exact directory entry captured by an atomic namespace cut.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitEntry {
    parent: InodeId,
    target: InodeId,
    name: Vec<u8>,
}

impl CommitEntry {
    #[must_use]
    pub const fn parent(&self) -> InodeId {
        self.parent
    }

    #[must_use]
    pub const fn target(&self) -> InodeId {
        self.target
    }

    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }
}

/// Retryable immutable view of one complete generation candidate.
#[derive(Clone, Debug)]
pub struct NamespaceCommit {
    token: CommitToken,
    inode_reservation_end: u64,
    inode_allocation_cursor: u64,
    namespace_mutation_sequence: u64,
    root: CommitDirectory,
    inodes: Vec<CommitInode>,
    directories: Vec<CommitDirectory>,
    symlinks: Vec<CommitSymlink>,
    entries: Vec<CommitEntry>,
}

impl NamespaceCommit {
    #[must_use]
    pub const fn token(&self) -> CommitToken {
        self.token
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
    pub const fn root(&self) -> &CommitDirectory {
        &self.root
    }

    #[must_use]
    pub fn inodes(&self) -> &[CommitInode] {
        &self.inodes
    }

    #[must_use]
    pub fn directories(&self) -> &[CommitDirectory] {
        &self.directories
    }

    #[must_use]
    pub fn symlinks(&self) -> &[CommitSymlink] {
        &self.symlinks
    }

    #[must_use]
    pub fn entries(&self) -> &[CommitEntry] {
        &self.entries
    }
}

/// Verified immutable content to install after a generation becomes durable.
#[derive(Debug)]
pub struct CommittedFileInstall {
    inode: InodeId,
    mutation_sequence: u64,
    file: Arc<dyn CommittedFile>,
}

impl CommittedFileInstall {
    #[must_use]
    pub fn new(inode: InodeId, mutation_sequence: u64, file: Arc<dyn CommittedFile>) -> Self {
        Self {
            inode,
            mutation_sequence,
            file,
        }
    }
}

#[derive(Debug)]
pub struct CommittedNamespaceSnapshot {
    next_inode: u64,
    inode_reservation_end: u64,
    namespace_mutation_sequence: u64,
    root_mode: u16,
    root_uid: u32,
    root_gid: u32,
    root_metadata: Arc<InodeMetadata>,
    root_times: PosixTimes,
    inodes: Vec<CommittedInode>,
    directories: Vec<CommittedDirectory>,
    symlinks: Vec<CommittedSymlink>,
    entries: Vec<CommittedEntry>,
}

impl CommittedNamespaceSnapshot {
    /// Bundles one already verified committed namespace for a POSIX mount.
    ///
    /// The recovery-only adapter mounts this form read-only. The durable
    /// appliance uses [`Namespace::from_committed_writable`] only after it has
    /// published a fresh Inode reservation. No identifier is guessed from
    /// visible inode records.
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
            root_mode: 0o755,
            root_uid: 0,
            root_gid: 0,
            root_metadata: Arc::new(InodeMetadata::default()),
            root_times: PosixTimes::default(),
            inodes,
            directories: Vec::new(),
            symlinks: Vec::new(),
            entries,
        })
    }

    /// Bundles one verified committed namespace containing directories.
    ///
    /// # Errors
    ///
    /// Rejects an empty, reversed, or root-overlapping allocation interval.
    pub fn new_with_directories(
        next_inode: u64,
        inode_reservation_end: u64,
        namespace_mutation_sequence: u64,
        inodes: Vec<CommittedInode>,
        directories: Vec<CommittedDirectory>,
        entries: Vec<CommittedEntry>,
    ) -> Result<Self, PosixError> {
        let mut snapshot = Self::new(
            next_inode,
            inode_reservation_end,
            namespace_mutation_sequence,
            inodes,
            entries,
        )?;
        snapshot.directories = directories;
        Ok(snapshot)
    }

    /// Installs the verified metadata of the implicit root directory.
    #[must_use]
    pub fn with_root_metadata(
        mut self,
        mode: u16,
        uid: u32,
        gid: u32,
        metadata: InodeMetadata,
    ) -> Self {
        self.root_mode = mode & 0o7777;
        self.root_uid = uid;
        self.root_gid = gid;
        self.root_metadata = Arc::new(metadata);
        self
    }

    #[must_use]
    pub fn with_posix_state(
        mut self,
        root_times: PosixTimes,
        symlinks: Vec<CommittedSymlink>,
    ) -> Self {
        self.root_times = root_times;
        self.symlinks = symlinks;
        self
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
    metadata: Arc<InodeMetadata>,
    times: PosixTimes,
    symlink_target: Option<Arc<[u8]>>,
    data: VersionedFile,
}

impl InodeState {
    fn attributes(&self, inode: InodeId) -> FileAttr {
        let (size, allocated_bytes) = match &self.symlink_target {
            Some(target) => (u64::try_from(target.len()).unwrap_or(u64::MAX), 0),
            None => (self.data.logical_size(), self.data.allocated_bytes()),
        };
        FileAttr {
            inode,
            size,
            allocated_bytes,
            kind: self.kind,
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            link_count: self.link_count,
            mutation_sequence: self.mutation_sequence,
            times: self.times,
        }
    }
}

#[derive(Clone, Debug)]
struct ExternalDirtyData {
    source: Arc<dyn CommittedFile>,
    source_offset: u64,
    length: u64,
    through_sequence: u64,
}

#[derive(Clone, Debug)]
struct ResidentDirtyData {
    bytes: MutationPayload,
    mutation_sequence: u64,
}

impl ResidentDirtyData {
    fn new(bytes: MutationPayload, mutation_sequence: u64) -> Self {
        Self {
            bytes,
            mutation_sequence,
        }
    }

    fn retained_fragment(&self, start: usize, end: usize) -> Result<Self, PosixError> {
        Ok(Self::new(
            self.bytes.retained_fragment(start, end)?,
            self.mutation_sequence,
        ))
    }

    fn as_bytes(&self) -> &[u8] {
        self.bytes.as_bytes()
    }

    const fn len(&self) -> usize {
        self.bytes.len()
    }

    const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[derive(Debug, Default)]
struct SparseData {
    logical_size: u64,
    allocated_bytes: u64,
    extents: BTreeMap<u64, ResidentDirtyData>,
    external_extents: BTreeMap<u64, ExternalDirtyData>,
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
            overlay_extent(&mut output, offset, end, extent_start, bytes.as_bytes());
        }
        for (&extent_start, bytes) in self.extents.range((Excluded(offset), Excluded(end))) {
            overlay_extent(&mut output, offset, end, extent_start, bytes.as_bytes());
        }
        if let Some((&extent_start, external)) = self.external_extents.range(..=offset).next_back()
        {
            overlay_external(&mut output, offset, end, extent_start, external)?;
        }
        for (&extent_start, external) in self
            .external_extents
            .range((Excluded(offset), Excluded(end)))
        {
            overlay_external(&mut output, offset, end, extent_start, external)?;
        }
        Ok(output)
    }

    fn write(
        &mut self,
        offset: u64,
        data: MutationPayload,
        mutation_sequence: u64,
    ) -> Result<(), PosixError> {
        assert!(
            !data.is_empty(),
            "ASSERT: empty writes are handled by caller"
        );
        let data_length = u64::try_from(data.len()).expect("ASSERT: usize must fit in u64");
        let end = offset
            .checked_add(data_length)
            .ok_or(PosixError::FileTooLarge)?;
        self.remove_external_overlaps(offset, end)?;
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
            self.extents
                .insert(offset, ResidentDirtyData::new(data, mutation_sequence))
                .is_none(),
            "ASSERT: new write extent must replace every overlap"
        );
        self.logical_size = self.logical_size.max(end);
        self.assert_valid_around(offset);
        self.assert_valid_around(end);
        #[cfg(test)]
        self.audit_valid();
        Ok(())
    }

    fn write_external(
        &mut self,
        offset: u64,
        source: Arc<dyn CommittedFile>,
        source_offset: u64,
        length: u64,
        through_sequence: u64,
    ) -> Result<(), PosixError> {
        assert!(length > 0, "ASSERT: external writes are nonempty");
        let end = offset.checked_add(length).ok_or(PosixError::FileTooLarge)?;
        let source_end = source_offset
            .checked_add(length)
            .ok_or(PosixError::FileTooLarge)?;
        if source_end > source.logical_size()
            || source.allocated_bytes_in_range(source_offset, length)? != length
        {
            return Err(PosixError::Unsupported);
        }
        self.remove_resident_overlaps(offset, end)?;
        self.remove_external_overlaps(offset, end)?;
        assert!(
            self.external_extents
                .insert(
                    offset,
                    ExternalDirtyData {
                        source,
                        source_offset,
                        length,
                        through_sequence,
                    },
                )
                .is_none(),
            "ASSERT: external write must replace every overlap"
        );
        self.allocated_bytes = self
            .allocated_bytes
            .checked_add(length)
            .expect("ASSERT: external allocation cannot overflow");
        self.logical_size = self.logical_size.max(end);
        self.assert_valid_around(offset);
        self.assert_valid_around(end);
        #[cfg(test)]
        self.audit_valid();
        Ok(())
    }

    fn truncate(&mut self, length: u64) -> Result<(), PosixError> {
        if length >= self.logical_size {
            self.logical_size = length;
            return Ok(());
        }

        self.truncate_external(length)?;

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
                    Some((start, bytes))
                } else {
                    None
                }
            });
        let crossing = if let Some((start, bytes)) = crossing {
            let keep = usize::try_from(length - start)
                .expect("ASSERT: truncated extent must fit in usize");
            Some((start, bytes.retained_fragment(0, keep)?))
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

    fn punch(&mut self, start: u64, end: u64) -> Result<(), PosixError> {
        assert!(start <= end, "ASSERT: punched range must be ordered");
        if start == end {
            return Ok(());
        }
        self.remove_resident_overlaps(start, end)?;
        self.remove_external_overlaps(start, end)?;
        self.assert_valid_around(start);
        self.assert_valid_around(end);
        #[cfg(test)]
        self.audit_valid();
        Ok(())
    }

    #[cfg(test)]
    const fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes
    }

    fn resident_bytes(&self) -> u64 {
        self.extents.values().fold(0_u64, |total, bytes| {
            total
                .checked_add(u64::try_from(bytes.len()).expect("ASSERT: usize fits u64"))
                .expect("ASSERT: resident Dirty DATA cannot overflow")
        })
    }

    fn range_unchanged_through(&self, offset: u64, end: u64, through_sequence: u64) -> bool {
        let mut cursor = offset;
        while cursor < end {
            let resident =
                self.extents
                    .range(..=cursor)
                    .next_back()
                    .and_then(|(&extent_start, extent)| {
                        let extent_end = extent_start.checked_add(
                            u64::try_from(extent.len()).expect("ASSERT: usize fits u64"),
                        )?;
                        (extent_end > cursor).then_some((extent_end, extent.mutation_sequence))
                    });
            let external = self.external_extents.range(..=cursor).next_back().and_then(
                |(&extent_start, extent)| {
                    let extent_end = extent_start.checked_add(extent.length)?;
                    (extent_end > cursor).then_some((extent_end, extent.through_sequence))
                },
            );
            let Some((extent_end, mutation_sequence)) = resident.or(external) else {
                return false;
            };
            if mutation_sequence > through_sequence {
                return false;
            }
            cursor = extent_end.min(end);
        }
        true
    }

    fn externalize_many(
        &mut self,
        mut candidates: Vec<(u64, u64, Arc<dyn CommittedFile>)>,
    ) -> Result<(), PosixError> {
        if candidates.is_empty() {
            return Ok(());
        }
        candidates.sort_unstable_by_key(|(offset, _, _)| *offset);
        let mut runs = Vec::<(u64, u64)>::new();
        runs.try_reserve(candidates.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        let mut previous_end = None;
        for (offset, through_sequence, source) in &candidates {
            let length = source.logical_size();
            let end = offset.checked_add(length).ok_or(PosixError::Io)?;
            if length == 0
                || end > self.logical_size
                || source.allocated_bytes() != length
                || previous_end.is_some_and(|previous| *offset < previous)
                || !self.range_unchanged_through(*offset, end, *through_sequence)
            {
                return Err(PosixError::Io);
            }
            match runs.last_mut() {
                Some((_, run_end)) if *run_end == *offset => *run_end = end,
                _ => runs.push((*offset, end)),
            }
            previous_end = Some(end);
        }
        for &(start, end) in &runs {
            self.remove_resident_overlaps(start, end)?;
            self.remove_external_overlaps(start, end)?;
        }
        for (offset, through_sequence, source) in candidates {
            let length = source.logical_size();
            assert!(
                self.external_extents
                    .insert(
                        offset,
                        ExternalDirtyData {
                            source,
                            source_offset: 0,
                            length,
                            through_sequence,
                        },
                    )
                    .is_none(),
                "ASSERT: externalized range must replace every overlap"
            );
            self.allocated_bytes = self
                .allocated_bytes
                .checked_add(length)
                .expect("ASSERT: external allocation cannot overflow");
        }
        #[cfg(test)]
        self.audit_valid();
        Ok(())
    }

    fn remove_resident_overlaps(&mut self, start: u64, end: u64) -> Result<(), PosixError> {
        let mut overlapping = Vec::new();
        let mut fragments = Vec::new();
        if let Some((&extent_start, bytes)) = self.extents.range(..=start).next_back() {
            plan_overlap(
                &mut overlapping,
                &mut fragments,
                extent_start,
                bytes,
                start,
                end,
            )?;
        }
        for (&extent_start, bytes) in self.extents.range((Excluded(start), Excluded(end))) {
            plan_overlap(
                &mut overlapping,
                &mut fragments,
                extent_start,
                bytes,
                start,
                end,
            )?;
        }
        for extent_start in overlapping {
            let removed = self
                .extents
                .remove(&extent_start)
                .expect("ASSERT: resident overlap vanished");
            self.allocated_bytes -= u64::try_from(removed.len()).expect("ASSERT: usize fits u64");
        }
        for (fragment_start, fragment) in fragments {
            self.allocated_bytes = self
                .allocated_bytes
                .checked_add(u64::try_from(fragment.len()).expect("ASSERT: usize fits u64"))
                .expect("ASSERT: fragment allocation cannot overflow");
            assert!(self.extents.insert(fragment_start, fragment).is_none());
        }
        Ok(())
    }

    fn remove_external_overlaps(&mut self, start: u64, end: u64) -> Result<(), PosixError> {
        let starts = external_overlapping_starts(&self.external_extents, start, end);
        let mut fragments = Vec::new();
        for extent_start in &starts {
            let external = &self.external_extents[extent_start];
            let extent_end = extent_start
                .checked_add(external.length)
                .ok_or(PosixError::Io)?;
            if *extent_start < start {
                fragments.push((
                    *extent_start,
                    ExternalDirtyData {
                        source: Arc::clone(&external.source),
                        source_offset: external.source_offset,
                        length: start - extent_start,
                        through_sequence: external.through_sequence,
                    },
                ));
            }
            if extent_end > end {
                fragments.push((
                    end,
                    ExternalDirtyData {
                        source: Arc::clone(&external.source),
                        source_offset: external
                            .source_offset
                            .checked_add(end - extent_start)
                            .ok_or(PosixError::Io)?,
                        length: extent_end - end,
                        through_sequence: external.through_sequence,
                    },
                ));
            }
        }
        for extent_start in starts {
            let removed = self
                .external_extents
                .remove(&extent_start)
                .expect("ASSERT: external overlap vanished");
            self.allocated_bytes -= removed.length;
        }
        for (fragment_start, fragment) in fragments {
            self.allocated_bytes = self
                .allocated_bytes
                .checked_add(fragment.length)
                .expect("ASSERT: external fragment allocation cannot overflow");
            assert!(
                self.external_extents
                    .insert(fragment_start, fragment)
                    .is_none()
            );
        }
        Ok(())
    }

    fn truncate_external(&mut self, length: u64) -> Result<(), PosixError> {
        self.remove_external_overlaps(length, self.logical_size)
    }

    fn assert_valid_around(&self, position: u64) {
        if let Some((&start, bytes)) = self.extents.range(..=position).next_back() {
            self.assert_extent_valid(start, bytes.as_bytes());
        }
        if let Some((&start, bytes)) = self.extents.range(position..).next() {
            self.assert_extent_valid(start, bytes.as_bytes());
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

    fn audit_valid(&self) {
        let mut ranges = Vec::with_capacity(self.extents.len() + self.external_extents.len());
        let mut allocated_bytes = 0_u64;
        for (&start, bytes) in &self.extents {
            assert!(
                !bytes.is_empty(),
                "AUDIT: sparse DATA extent must be nonempty"
            );
            let length = u64::try_from(bytes.len()).expect("ASSERT: usize must fit in u64");
            let end = start
                .checked_add(length)
                .expect("AUDIT: sparse extent end must not overflow");
            assert!(
                end <= self.logical_size,
                "AUDIT: sparse extent must stay inside logical size"
            );
            ranges.push((start, end));
            allocated_bytes = allocated_bytes
                .checked_add(length)
                .expect("AUDIT: allocated extent bytes must not overflow");
        }
        for (&start, external) in &self.external_extents {
            assert!(
                external.length != 0,
                "AUDIT: external dirty DATA extent must be nonempty"
            );
            let end = start
                .checked_add(external.length)
                .expect("AUDIT: external dirty extent end must not overflow");
            assert!(
                end <= self.logical_size,
                "AUDIT: external dirty extent must stay inside logical size"
            );
            assert!(
                external
                    .source_offset
                    .checked_add(external.length)
                    .is_some_and(|source_end| source_end <= external.source.logical_size()),
                "AUDIT: external dirty extent must stay inside its verified source"
            );
            ranges.push((start, end));
            allocated_bytes = allocated_bytes
                .checked_add(external.length)
                .expect("AUDIT: allocated extent bytes must not overflow");
        }
        ranges.sort_unstable();
        for pair in ranges.windows(2) {
            assert!(
                pair[0].1 <= pair[1].0,
                "AUDIT: resident and external dirty DATA extents must not overlap"
            );
        }
        assert_eq!(
            allocated_bytes, self.allocated_bytes,
            "AUDIT: cached allocated extent bytes must match the extent map"
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

#[cfg(test)]
fn overlay_external(
    output: &mut [u8],
    read_start: u64,
    read_end: u64,
    extent_start: u64,
    external: &ExternalDirtyData,
) -> Result<(), PosixError> {
    let extent_end = extent_start
        .checked_add(external.length)
        .ok_or(PosixError::Io)?;
    let copy_start = extent_start.max(read_start);
    let copy_end = extent_end.min(read_end);
    if copy_start >= copy_end {
        return Ok(());
    }
    let source_offset = external
        .source_offset
        .checked_add(copy_start - extent_start)
        .ok_or(PosixError::Io)?;
    let length = u32::try_from(copy_end - copy_start).map_err(|_| PosixError::FileTooLarge)?;
    let bytes = external.source.read_at(source_offset, length)?;
    if bytes.len() != usize::try_from(length).expect("ASSERT: u32 fits usize") {
        return Err(PosixError::Io);
    }
    let target_start =
        usize::try_from(copy_start - read_start).expect("ASSERT: target offset fits usize");
    output[target_start..target_start + bytes.len()].copy_from_slice(&bytes);
    Ok(())
}

fn external_overlapping_starts(
    extents: &BTreeMap<u64, ExternalDirtyData>,
    start: u64,
    end: u64,
) -> Vec<u64> {
    let mut starts = Vec::new();
    if let Some((&candidate, extent)) = extents.range(..=start).next_back()
        && candidate.saturating_add(extent.length) > start
    {
        starts.push(candidate);
    }
    starts.extend(
        extents
            .range((Excluded(start), Excluded(end)))
            .map(|(&candidate, _)| candidate),
    );
    starts
}

fn plan_overlap(
    overlapping: &mut Vec<u64>,
    fragments: &mut Vec<(u64, ResidentDirtyData)>,
    extent_start: u64,
    bytes: &ResidentDirtyData,
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
        fragments.push((extent_start, bytes.retained_fragment(0, keep)?));
    }
    if extent_end > write_end {
        let skip = usize::try_from(write_end - extent_start)
            .expect("ASSERT: right fragment must fit in usize");
        fragments.push((write_end, bytes.retained_fragment(skip, bytes.len())?));
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
    observer_order: Mutex<()>,
    kernel_data_cache_exposed: AtomicBool,
    state: RwLock<InodeState>,
}

#[derive(Debug)]
struct WriteResult {
    reply: Reply,
    kernel_data_cache_exposed: bool,
}

#[derive(Clone, Copy, Debug)]
struct OpenHandle {
    inode: InodeId,
    options: OpenOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecordLock {
    owner: u64,
    start: u64,
    end: u64,
    kind: LockKind,
    pid: u32,
}

impl RecordLock {
    const fn from_request(owner: u64, lock: FileLock) -> Self {
        Self {
            owner,
            start: lock.start,
            end: lock.end,
            kind: lock.kind,
            pid: lock.pid,
        }
    }

    const fn as_reply(self) -> FileLock {
        FileLock {
            start: self.start,
            end: self.end,
            kind: self.kind,
            pid: self.pid,
        }
    }
}

#[derive(Debug, Default)]
struct LockTable {
    by_inode: BTreeMap<InodeId, Vec<RecordLock>>,
    record_count: usize,
}

#[derive(Clone, Copy)]
struct CreateRequest<'a> {
    context: RequestContext,
    parent: InodeId,
    name: &'a [u8],
    mode: u16,
    umask: u16,
    options: OpenOptions,
    exclusive: bool,
    truncate: bool,
}

#[derive(Debug)]
struct Catalog {
    next_inode: u64,
    inode_reservation_end: u64,
    next_handle: u64,
    next_commit_token: u64,
    committed_namespace_mutation_sequence: u64,
    inflight_commit: Option<NamespaceCommit>,
    inodes: BTreeMap<InodeId, Arc<Inode>>,
    entries: BTreeMap<(InodeId, Vec<u8>), InodeId>,
    handles: BTreeMap<HandleId, OpenHandle>,
    lookup_counts: BTreeMap<InodeId, u64>,
    active_create_metadata_bytes: BTreeMap<InodeId, u64>,
}

#[derive(Debug)]
#[repr(align(64))]
struct DirtyPayloadTracker {
    checkpointable_active_bytes: AtomicU64,
    wake_at_bytes: AtomicU64,
    changed: Notify,
}

impl Default for DirtyPayloadTracker {
    fn default() -> Self {
        Self {
            checkpointable_active_bytes: AtomicU64::new(0),
            wake_at_bytes: AtomicU64::new(u64::MAX),
            changed: Notify::new(),
        }
    }
}

impl DirtyPayloadTracker {
    fn replace(&self, before: u64, after: u64) {
        match after.cmp(&before) {
            std::cmp::Ordering::Greater => {
                let added = after - before;
                let update = self.checkpointable_active_bytes.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |current| current.checked_add(added),
                );
                assert!(
                    update.is_ok(),
                    "ASSERT: checkpointable dirty payload counter must not overflow"
                );
                let current = update
                    .expect("ASSERT: successful dirty payload update must expose its prior value")
                    .checked_add(added)
                    .expect("ASSERT: preflighted dirty payload addition cannot overflow");
                self.notify_if_watermark_reached(current);
            }
            std::cmp::Ordering::Less => {
                let removed = before - after;
                let update = self.checkpointable_active_bytes.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |current| current.checked_sub(removed),
                );
                assert!(
                    update.is_ok(),
                    "ASSERT: checkpointable dirty payload removal must be accounted"
                );
            }
            std::cmp::Ordering::Equal => {}
        }
    }

    fn load(&self) -> u64 {
        self.checkpointable_active_bytes.load(Ordering::Acquire)
    }

    fn notify_if_watermark_reached(&self, current: u64) {
        loop {
            let wake_at = self.wake_at_bytes.load(Ordering::Acquire);
            if current < wake_at {
                return;
            }
            if self
                .wake_at_bytes
                .compare_exchange(wake_at, u64::MAX, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.changed.notify_waiters();
                return;
            }
        }
    }
}

#[derive(Debug)]
pub struct Namespace {
    config: NamespaceConfig,
    mutations_supported: bool,
    mutations_admitted: RwLock<bool>,
    admission_changed: Notify,
    dirty_payload: DirtyPayloadTracker,
    mutation_observer: RwLock<Option<Arc<dyn MutationObserver>>>,
    commit_capacity_admission: OnceLock<Arc<dyn CommitCapacityAdmission>>,
    logical_quotas: LogicalQuotaTable,
    catalog: RwLock<Catalog>,
    locks: Mutex<LockTable>,
    lock_change_sequence: AtomicU64,
    lock_changed: Notify,
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
            observer_order: Mutex::new(()),
            kernel_data_cache_exposed: AtomicBool::new(false),
            state: RwLock::new(InodeState {
                kind: FileKind::Directory,
                mode: 0o755,
                uid: 0,
                gid: 0,
                link_count: 2,
                mutation_sequence: 0,
                metadata: Arc::new(InodeMetadata::default()),
                times: PosixTimes::now(),
                symlink_target: None,
                data: VersionedFile::new_empty(),
            }),
        });
        let mut inodes = BTreeMap::new();
        let replaced = inodes.insert(ROOT_INODE, root);
        assert!(replaced.is_none(), "ASSERT: root inode inserted twice");

        Self {
            config,
            mutations_supported: true,
            mutations_admitted: RwLock::new(true),
            admission_changed: Notify::new(),
            dirty_payload: DirtyPayloadTracker::default(),
            mutation_observer: RwLock::new(None),
            commit_capacity_admission: OnceLock::new(),
            logical_quotas: LogicalQuotaTable::default(),
            catalog: RwLock::new(Catalog {
                next_inode: ROOT_INODE.get() + 1,
                inode_reservation_end: u64::MAX,
                next_handle: 1,
                next_commit_token: 1,
                committed_namespace_mutation_sequence: 0,
                inflight_commit: None,
                inodes,
                entries: BTreeMap::new(),
                handles: BTreeMap::new(),
                lookup_counts: BTreeMap::new(),
                active_create_metadata_bytes: BTreeMap::new(),
            }),
            locks: Mutex::new(LockTable::default()),
            lock_change_sequence: AtomicU64::new(0),
            lock_changed: Notify::new(),
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
        Self::from_committed_mode(config, snapshot, false)
    }

    /// Mounts a verified snapshot with mutation admission enabled.
    ///
    /// The caller must have durably reserved `[next_inode,
    /// inode_reservation_end)` before constructing the snapshot. This
    /// constructor performs no I/O and never guesses a reservation from live
    /// inode records.
    ///
    /// # Errors
    ///
    /// Rejects the same malformed or inconsistent snapshot state as
    /// [`Self::from_committed`].
    ///
    /// # Panics
    ///
    /// Panics when the internal configuration permits no valid component name.
    pub fn from_committed_writable(
        config: NamespaceConfig,
        snapshot: CommittedNamespaceSnapshot,
    ) -> Result<Self, PosixError> {
        Self::from_committed_mode(config, snapshot, true)
    }

    #[allow(clippy::too_many_lines)]
    fn from_committed_mode(
        config: NamespaceConfig,
        snapshot: CommittedNamespaceSnapshot,
        mutations_enabled: bool,
    ) -> Result<Self, PosixError> {
        assert!(
            config.maximum_name_bytes > 0,
            "ASSERT: maximum name length must be nonzero"
        );
        let root_times = snapshot.root_times;
        let committed_symlinks = snapshot.symlinks;
        let root = Arc::new(Inode {
            observer_order: Mutex::new(()),
            kernel_data_cache_exposed: AtomicBool::new(false),
            state: RwLock::new(InodeState {
                kind: FileKind::Directory,
                mode: snapshot.root_mode,
                uid: snapshot.root_uid,
                gid: snapshot.root_gid,
                link_count: 2,
                mutation_sequence: snapshot.namespace_mutation_sequence,
                metadata: snapshot.root_metadata,
                times: root_times,
                symlink_target: None,
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
                observer_order: Mutex::new(()),
                kernel_data_cache_exposed: AtomicBool::new(false),
                state: RwLock::new(InodeState {
                    kind: FileKind::Regular,
                    mode: committed.mode,
                    uid: committed.uid,
                    gid: committed.gid,
                    link_count: committed.link_count,
                    mutation_sequence: committed.mutation_sequence,
                    metadata: committed.metadata,
                    times: committed.times,
                    symlink_target: None,
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
        for committed in snapshot.directories {
            if committed.inode.get() >= snapshot.next_inode {
                return Err(PosixError::InvalidArgument);
            }
            let inode = committed.inode;
            let object = Arc::new(Inode {
                observer_order: Mutex::new(()),
                kernel_data_cache_exposed: AtomicBool::new(false),
                state: RwLock::new(InodeState {
                    kind: FileKind::Directory,
                    mode: committed.mode,
                    uid: committed.uid,
                    gid: committed.gid,
                    link_count: committed.link_count,
                    mutation_sequence: committed.mutation_sequence,
                    metadata: committed.metadata,
                    times: committed.times,
                    symlink_target: None,
                    data: VersionedFile::new_empty(),
                }),
            });
            if inodes.insert(inode, object).is_some() {
                return Err(PosixError::InvalidArgument);
            }
        }
        for committed in committed_symlinks {
            if committed.inode.get() >= snapshot.next_inode {
                return Err(PosixError::InvalidArgument);
            }
            let inode = committed.inode;
            let object = Arc::new(Inode {
                observer_order: Mutex::new(()),
                kernel_data_cache_exposed: AtomicBool::new(false),
                state: RwLock::new(InodeState {
                    kind: FileKind::Symlink,
                    mode: 0o777,
                    uid: committed.uid,
                    gid: committed.gid,
                    link_count: committed.link_count,
                    mutation_sequence: committed.mutation_sequence,
                    metadata: Arc::new(InodeMetadata::default()),
                    times: committed.times,
                    symlink_target: Some(committed.target),
                    data: VersionedFile::new_empty(),
                }),
            });
            if inodes.insert(inode, object).is_some() {
                return Err(PosixError::InvalidArgument);
            }
        }

        let mut entries = BTreeMap::new();
        let mut observed_links = BTreeMap::<InodeId, u32>::new();
        let mut directory_children = BTreeMap::<InodeId, u32>::new();
        for entry in snapshot.entries {
            validate_component(&config, &entry.name)?;
            let parent_is_directory = inodes.get(&entry.parent).is_some_and(|parent| {
                parent
                    .state
                    .read()
                    .expect("ASSERT: new committed parent lock poisoned")
                    .kind
                    == FileKind::Directory
            });
            if !parent_is_directory
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
            let target_is_directory = inodes
                .get(&entry.target)
                .expect("ASSERT: validated committed target exists")
                .state
                .read()
                .expect("ASSERT: new committed target lock poisoned")
                .kind
                == FileKind::Directory;
            if target_is_directory {
                let count = directory_children.entry(entry.parent).or_default();
                *count = count.checked_add(1).ok_or(PosixError::NoSpace)?;
            }
        }
        for (&inode, object) in &inodes {
            if inode == ROOT_INODE {
                continue;
            }
            let state = object
                .state
                .read()
                .expect("ASSERT: new committed inode lock poisoned");
            match state.kind {
                FileKind::Regular | FileKind::Symlink => {
                    if observed_links.get(&inode).copied() != Some(state.link_count) {
                        return Err(PosixError::InvalidArgument);
                    }
                }
                FileKind::Directory => {
                    let expected = 2_u32
                        .checked_add(directory_children.get(&inode).copied().unwrap_or(0))
                        .ok_or(PosixError::NoSpace)?;
                    if observed_links.get(&inode).copied() != Some(1)
                        || state.link_count != expected
                    {
                        return Err(PosixError::InvalidArgument);
                    }
                }
            }
        }
        let root_links = 2_u32
            .checked_add(directory_children.get(&ROOT_INODE).copied().unwrap_or(0))
            .ok_or(PosixError::NoSpace)?;
        inodes
            .get(&ROOT_INODE)
            .expect("ASSERT: committed root exists")
            .state
            .write()
            .expect("ASSERT: committed root lock poisoned")
            .link_count = root_links;
        let reachable = reachable_inodes(&entries, &inodes)?;
        if reachable.len() != inodes.len().saturating_sub(1) {
            return Err(PosixError::InvalidArgument);
        }

        Ok(Self {
            config,
            mutations_supported: mutations_enabled,
            mutations_admitted: RwLock::new(mutations_enabled),
            admission_changed: Notify::new(),
            dirty_payload: DirtyPayloadTracker::default(),
            mutation_observer: RwLock::new(None),
            commit_capacity_admission: OnceLock::new(),
            logical_quotas: LogicalQuotaTable::default(),
            catalog: RwLock::new(Catalog {
                next_inode: snapshot.next_inode,
                inode_reservation_end: snapshot.inode_reservation_end,
                next_handle: 1,
                next_commit_token: 1,
                committed_namespace_mutation_sequence: snapshot.namespace_mutation_sequence,
                inflight_commit: None,
                inodes,
                entries,
                handles: BTreeMap::new(),
                lookup_counts: BTreeMap::new(),
                active_create_metadata_bytes: BTreeMap::new(),
            }),
            locks: Mutex::new(LockTable::default()),
            lock_change_sequence: AtomicU64::new(0),
            lock_changed: Notify::new(),
        })
    }

    /// Stops acknowledging new mutations while preserving reads and already
    /// admitted dirty epochs.
    ///
    /// # Panics
    ///
    /// Panics when a prior impossible invariant poisoned the admission lock.
    pub fn pause_mutation_admission(&self) {
        *self
            .mutations_admitted
            .write()
            .expect("ASSERT: mutation admission lock poisoned") = false;
    }

    /// Reopens mutation admission after durable progress catches up.
    ///
    /// # Panics
    ///
    /// Panics if called for a namespace constructed as permanently read-only.
    pub fn resume_mutation_admission(&self) {
        assert!(
            self.mutations_supported,
            "ASSERT: a read-only namespace cannot resume mutation admission"
        );
        *self
            .mutations_admitted
            .write()
            .expect("ASSERT: mutation admission lock poisoned") = true;
        self.admission_changed.notify_waiters();
    }

    /// Waits until transient checkpoint backpressure permits a mutation.
    ///
    /// The synchronous model seam deliberately continues to return
    /// [`PosixError::Again`] while admission is closed. The kernel-FUSE edge
    /// uses this notification to turn that internal state into transparent
    /// POSIX backpressure instead of leaking `EAGAIN` to ordinary file-copy
    /// programs.
    pub(crate) async fn wait_for_mutation_admission(&self) -> Result<(), PosixError> {
        if !self.mutations_supported {
            return Err(PosixError::ReadOnly);
        }
        loop {
            let changed = self.admission_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.mutation_admission_open() {
                return Ok(());
            }
            changed.await;
        }
    }

    #[must_use]
    /// Reports whether this namespace currently accepts new mutations.
    ///
    /// # Panics
    ///
    /// Panics when a prior impossible invariant poisoned the admission lock.
    pub fn mutation_admission_open(&self) -> bool {
        self.mutations_supported
            && *self
                .mutations_admitted
                .read()
                .expect("ASSERT: mutation admission lock poisoned")
    }

    /// Returns unique resident DATA bytes retained by active dirty extents.
    ///
    /// Overwrites of an already dirty range are counted once, sparse holes are
    /// not counted, and freezing a commit cut transfers its bytes out of this
    /// active-pressure counter. A byte-exact dirty range externalized to a
    /// verified immutable Container is also excluded because the live view can
    /// reread it without retaining the original payload. The value deliberately
    /// excludes range recipes, encoder buffers, and an already frozen epoch.
    #[must_use]
    pub fn checkpointable_dirty_payload_bytes(&self) -> u64 {
        self.dirty_payload.load()
    }

    /// Installs the one appliance-owned write-through observer before serving
    /// requests. Replacing an installed observer would split one mutation
    /// stream across two reduction states and is therefore an impossible use.
    ///
    /// # Panics
    ///
    /// Panics if the observer lock was poisoned or an observer was already
    /// installed for this Namespace.
    pub fn install_mutation_observer(&self, observer: Arc<dyn MutationObserver>) {
        let mut installed = self
            .mutation_observer
            .write()
            .expect("ASSERT: mutation observer lock poisoned");
        assert!(
            installed.is_none(),
            "ASSERT: a Namespace may install only one mutation observer"
        );
        *installed = Some(observer);
    }

    /// Installs the appliance's one physical commit-capacity governor.
    ///
    /// Installation is intentionally one-shot and must happen before serving
    /// requests. The request path then reads the immutable trait pointer and
    /// performs only atomic accounting in the governor.
    ///
    /// # Panics
    ///
    /// Panics when a governor was already installed.
    pub fn install_commit_capacity_admission(&self, admission: Arc<dyn CommitCapacityAdmission>) {
        assert!(
            self.commit_capacity_admission.set(admission).is_ok(),
            "ASSERT: a Namespace may install only one commit-capacity governor"
        );
    }

    /// Atomically replaces the hard logical quotas attached to managed
    /// directory subtrees.
    ///
    /// Usage is reconstructed from the live namespace while mutation admission
    /// is fenced. Allocated DATA/FILL/clone extents count in full, sparse holes
    /// do not, and a hard-linked inode is counted once. Cross-quota hard links
    /// and nested quota roots are rejected because they have no single owner.
    ///
    /// # Errors
    ///
    /// Rejects invalid revisions, missing or non-directory roots, overlapping
    /// quota trees, usage already above the requested limit, or inconsistent
    /// namespace allocation metadata.
    ///
    /// # Panics
    ///
    /// Panics when an internal admission, catalog, inode, or quota lock was
    /// poisoned by an earlier invariant violation.
    pub fn replace_logical_quotas(
        &self,
        revision: String,
        rules: impl IntoIterator<Item = LogicalQuotaRule>,
    ) -> Result<(), PosixError> {
        let _mutation_fence = self
            .mutations_admitted
            .write()
            .expect("ASSERT: mutation admission lock poisoned");
        let catalog = self.catalog.read().expect("ASSERT: catalog lock poisoned");
        let mut limits = BTreeMap::new();
        for rule in rules {
            if limits
                .insert(rule.root_inode(), rule.limit_bytes())
                .is_some()
            {
                return Err(PosixError::InvalidArgument);
            }
            let root = catalog
                .inodes
                .get(&rule.root_inode())
                .ok_or(PosixError::NoEntry)?;
            if root
                .state
                .read()
                .expect("ASSERT: quota root inode lock poisoned")
                .kind
                != FileKind::Directory
            {
                return Err(PosixError::NotDirectory);
            }
        }

        let mut membership = BTreeMap::<InodeId, InodeId>::new();
        let mut usage = BTreeMap::<InodeId, u64>::new();
        for &root_inode in limits.keys() {
            let mut pending = vec![root_inode];
            while let Some(inode) = pending.pop() {
                if inode != root_inode && limits.contains_key(&inode) {
                    return Err(PosixError::InvalidArgument);
                }
                match membership.insert(inode, root_inode) {
                    Some(existing) if existing != root_inode => {
                        return Err(PosixError::InvalidArgument);
                    }
                    Some(_) => continue,
                    None => {}
                }
                let object = catalog
                    .inodes
                    .get(&inode)
                    .ok_or(PosixError::InvalidArgument)?;
                let state = object.state.read().expect("ASSERT: inode lock poisoned");
                if state.kind == FileKind::Regular {
                    let root_usage = usage.entry(root_inode).or_default();
                    *root_usage = root_usage
                        .checked_add(state.data.allocated_bytes())
                        .ok_or(PosixError::NoSpace)?;
                }
                let is_directory = state.kind == FileKind::Directory;
                drop(state);
                if is_directory {
                    for ((parent, _), &child) in catalog.entries.range((inode, Vec::new())..) {
                        if *parent != inode {
                            break;
                        }
                        pending.push(child);
                    }
                }
            }
        }
        for ((parent, _), target) in &catalog.entries {
            if let Some(&target_root) = membership.get(target)
                && *target != target_root
                && membership.get(parent).copied() != Some(target_root)
            {
                return Err(PosixError::InvalidArgument);
            }
        }
        for (&inode, object) in &catalog.inodes {
            if membership.contains_key(&inode) {
                continue;
            }
            let state = object.state.read().expect("ASSERT: inode lock poisoned");
            if state.link_count != 0 || state.kind != FileKind::Regular {
                continue;
            }
            let Some(root_inode) = self.logical_quotas.root_for(inode) else {
                continue;
            };
            if !limits.contains_key(&root_inode) {
                continue;
            }
            membership.insert(inode, root_inode);
            let root_usage = usage.entry(root_inode).or_default();
            *root_usage = root_usage
                .checked_add(state.data.allocated_bytes())
                .ok_or(PosixError::NoSpace)?;
        }
        drop(catalog);
        self.logical_quotas
            .replace(revision, &limits, membership, &usage)
    }

    #[must_use]
    pub fn logical_quota_revision(&self) -> String {
        self.logical_quotas.revision()
    }

    #[must_use]
    pub fn logical_quota_status(&self, root_inode: InodeId) -> Option<LogicalQuotaStatus> {
        self.logical_quotas.status(root_inode)
    }

    #[must_use]
    pub fn logical_quota_status_for_inode(&self, inode: InodeId) -> Option<LogicalQuotaStatus> {
        self.logical_quotas.status_for_inode(inode)
    }

    /// Applies the v1 Small-File placement policy to one live regular inode.
    ///
    /// A `user.fastdup.placement` value of `metadata` or `data` is an explicit
    /// hint. Without a hint, any current hardlink name ending in `.xml` or
    /// `.json` (ASCII case-insensitive) selects the Small-File tier. New
    /// records spill to DATA once the live logical size exceeds 8 MiB.
    ///
    /// # Panics
    ///
    /// Panics if an earlier invariant violation poisoned a namespace lock.
    #[must_use]
    pub fn prefers_small_file_tier(&self, inode: InodeId) -> bool {
        let catalog = self
            .catalog
            .read()
            .expect("ASSERT: namespace catalog lock poisoned");
        let Some(object) = catalog.inodes.get(&inode) else {
            return false;
        };
        let state = object
            .state
            .read()
            .expect("ASSERT: inode state lock poisoned");
        if state.kind != FileKind::Regular {
            return false;
        }
        small_file_policy(
            state.data.logical_size(),
            &state.metadata,
            catalog
                .entries
                .iter()
                .filter(|(_, target)| **target == inode)
                .map(|((_, name), _)| name.as_slice()),
        )
    }

    fn notify_write_handle_opened(&self, inode: InodeId) {
        if let Some(observer) = self
            .mutation_observer
            .read()
            .expect("ASSERT: mutation observer lock poisoned")
            .as_ref()
            .cloned()
        {
            observer.opened_write_handle(inode);
        }
    }

    fn notify_write_handle_released(&self, inode: InodeId) {
        if let Some(observer) = self
            .mutation_observer
            .read()
            .expect("ASSERT: mutation observer lock poisoned")
            .as_ref()
            .cloned()
        {
            observer.released_write_handle(inode);
        }
    }

    /// Waits until active checkpointable dirty DATA reaches `threshold`.
    ///
    /// The notification is edge-independent: a caller arriving after the
    /// threshold was crossed returns immediately. This is the scheduler seam
    /// used to start an early durability checkpoint without polling.
    ///
    /// # Panics
    ///
    /// Panics when `threshold` is zero because every namespace would satisfy
    /// such a policy continuously.
    pub async fn wait_for_checkpointable_dirty_payload(&self, threshold: u64) -> u64 {
        assert!(
            threshold > 0,
            "ASSERT: dirty checkpoint threshold must be nonzero"
        );
        loop {
            let changed = self.dirty_payload.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let current = self.dirty_payload.load();
            if current >= threshold {
                return current;
            }
            self.dirty_payload
                .wake_at_bytes
                .fetch_min(threshold, Ordering::AcqRel);
            let current = self.dirty_payload.load();
            if current >= threshold {
                return current;
            }
            changed.await;
        }
    }

    /// Freezes one retryable atomic generation candidate.
    ///
    /// Later mutations remain admitted and immediately readable through
    /// [`Self::dispatch`], but are excluded from the returned view. Repeating
    /// this call before [`Self::complete_commit`] returns the same token and
    /// bytes, so a failed durable publication can retry without opening a
    /// second in-flight epoch.
    ///
    /// Forming the cut briefly takes the mutation-admission write fence. This
    /// waits for every already admitted mutation, including its acceleration
    /// observer, without closing admission for the duration of persistence.
    /// Consequently a Frozen Commit Cut can never overtake the corresponding
    /// write-through staging operation.
    ///
    /// # Errors
    ///
    /// Returns a bounded allocation or counter-exhaustion error. `Ok(None)`
    /// means the live namespace already equals its installed generation.
    ///
    /// # Panics
    ///
    /// Panics when internal namespace reachability, link counts, or lock order
    /// disagree while the catalog is exclusively locked.
    #[allow(clippy::too_many_lines)]
    pub fn begin_commit(&self) -> Result<Option<NamespaceCommit>, PosixError> {
        let _mutation_fence = self
            .mutations_admitted
            .write()
            .expect("ASSERT: mutation admission lock poisoned");
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        if let Some(inflight) = &catalog.inflight_commit {
            return Ok(Some(inflight.clone()));
        }

        let namespace_mutation_sequence = root_mutation_sequence(&catalog);
        let namespace_dirty =
            namespace_mutation_sequence != catalog.committed_namespace_mutation_sequence;
        let mut content_dirty = false;
        for (&inode, object) in &catalog.inodes {
            if inode == ROOT_INODE {
                continue;
            }
            let state = object.state.read().expect("ASSERT: inode lock poisoned");
            if state.link_count > 0 && state.data.has_active_mutations() {
                content_dirty = true;
                break;
            }
        }
        if !namespace_dirty && !content_dirty {
            if let Some(admission) = self.commit_capacity_admission.get() {
                admission.finish_uncheckpointed_active();
            }
            return Ok(None);
        }

        let token = CommitToken::new(catalog.next_commit_token).ok_or(PosixError::NoSpace)?;
        let next_commit_token = catalog
            .next_commit_token
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;

        let mut entries = Vec::new();
        entries
            .try_reserve_exact(catalog.entries.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        for ((parent, name), target) in &catalog.entries {
            let mut copied_name = Vec::new();
            copied_name
                .try_reserve_exact(name.len())
                .map_err(|_| PosixError::OutOfMemory)?;
            copied_name.extend_from_slice(name);
            entries.push(CommitEntry {
                parent: *parent,
                target: *target,
                name: copied_name,
            });
        }

        let mut inodes = Vec::new();
        inodes
            .try_reserve_exact(catalog.inodes.len().saturating_sub(1))
            .map_err(|_| PosixError::OutOfMemory)?;
        let mut directories = Vec::new();
        directories
            .try_reserve_exact(catalog.inodes.len().saturating_sub(1))
            .map_err(|_| PosixError::OutOfMemory)?;
        let mut symlinks = Vec::new();
        symlinks
            .try_reserve_exact(catalog.inodes.len().saturating_sub(1))
            .map_err(|_| PosixError::OutOfMemory)?;
        let root = {
            let state = catalog
                .inodes
                .get(&ROOT_INODE)
                .expect("ASSERT: root inode exists")
                .state
                .read()
                .expect("ASSERT: root inode lock poisoned");
            CommitDirectory {
                inode: ROOT_INODE,
                mode: state.mode,
                uid: state.uid,
                gid: state.gid,
                link_count: state.link_count,
                mutation_sequence: state.mutation_sequence,
                metadata: Arc::clone(&state.metadata),
                times: state.times,
            }
        };
        for (&inode, object) in &catalog.inodes {
            if inode == ROOT_INODE {
                continue;
            }
            let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
            if state.link_count == 0 {
                continue;
            }
            if state.kind == FileKind::Directory {
                directories.push(CommitDirectory {
                    inode,
                    mode: state.mode,
                    uid: state.uid,
                    gid: state.gid,
                    link_count: state.link_count,
                    mutation_sequence: state.mutation_sequence,
                    metadata: Arc::clone(&state.metadata),
                    times: state.times,
                });
                continue;
            }
            if state.kind == FileKind::Symlink {
                symlinks.push(CommitSymlink {
                    inode,
                    uid: state.uid,
                    gid: state.gid,
                    link_count: state.link_count,
                    mutation_sequence: state.mutation_sequence,
                    times: state.times,
                    target: Arc::clone(
                        state
                            .symlink_target
                            .as_ref()
                            .expect("ASSERT: symlink has target"),
                    ),
                });
                continue;
            }
            let dirty_before = state.data.active_resident_payload_bytes();
            let (file, frozen_epoch) = state.data.freeze_for_commit(token);
            let dirty_after = state.data.active_resident_payload_bytes();
            self.dirty_payload.replace(dirty_before, dirty_after);
            inodes.push(CommitInode {
                inode,
                mode: state.mode,
                uid: state.uid,
                gid: state.gid,
                link_count: state.link_count,
                mutation_sequence: state.mutation_sequence,
                metadata: Arc::clone(&state.metadata),
                times: state.times,
                file,
                frozen_epoch,
            });
        }
        assert_commit_reachability(&inodes, &directories, &symlinks, &entries);

        let commit = NamespaceCommit {
            token,
            inode_reservation_end: catalog.inode_reservation_end,
            inode_allocation_cursor: catalog.next_inode,
            namespace_mutation_sequence,
            root,
            inodes,
            directories,
            symlinks,
            entries,
        };
        catalog.next_commit_token = next_commit_token;
        assert!(
            catalog.inflight_commit.replace(commit.clone()).is_none(),
            "ASSERT: begin commit replaced an in-flight generation"
        );
        catalog.active_create_metadata_bytes.clear();
        if let Some(admission) = self.commit_capacity_admission.get() {
            admission.freeze(token);
        }
        Ok(Some(commit))
    }

    /// Installs fully verified immutable readers after the cut's Commit Record
    /// is durable.
    ///
    /// Active mutations accepted after the cut remain layered over these new
    /// bases. A file unlinked and reclaimed after the cut is intentionally not
    /// resurrected.
    ///
    /// # Errors
    ///
    /// Returns [`PosixError::Io`] when the installed inode set, sequence, size,
    /// or allocation metadata disagrees with the frozen cut.
    ///
    /// # Panics
    ///
    /// Panics when the coordinator supplies a stale token or an internal
    /// frozen epoch does not match the accepted commit cut.
    pub fn complete_commit(
        &self,
        commit: &NamespaceCommit,
        installed: Vec<CommittedFileInstall>,
    ) -> Result<(), PosixError> {
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let inflight = catalog
            .inflight_commit
            .as_ref()
            .expect("ASSERT: complete requires one in-flight commit");
        assert_eq!(
            inflight.token, commit.token,
            "ASSERT: complete commit token must match the in-flight cut"
        );
        if installed.len() != commit.inodes.len() {
            return Err(PosixError::Io);
        }
        let mut by_inode = BTreeMap::new();
        for install in installed {
            if by_inode.insert(install.inode, install).is_some() {
                return Err(PosixError::Io);
            }
        }
        for frozen in &commit.inodes {
            let install = by_inode.get(&frozen.inode).ok_or(PosixError::Io)?;
            if install.mutation_sequence != frozen.mutation_sequence
                || install.file.logical_size() != frozen.logical_size()
                || install.file.allocated_bytes() != frozen.allocated_bytes()
            {
                return Err(PosixError::Io);
            }
        }

        for frozen in &commit.inodes {
            let install = by_inode
                .remove(&frozen.inode)
                .expect("ASSERT: preflighted install disappeared");
            let Some(object) = catalog.inodes.get(&frozen.inode).cloned() else {
                continue;
            };
            let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
            assert!(
                state.mutation_sequence >= frozen.mutation_sequence,
                "ASSERT: live inode sequence cannot precede a frozen prefix"
            );
            state.data.install_commit_view(
                commit.token,
                install.file,
                install.mutation_sequence,
                frozen.frozen_epoch.is_some(),
            );
        }
        assert!(
            by_inode.is_empty(),
            "ASSERT: exact install preflight left an unexpected inode"
        );
        assert!(
            root_mutation_sequence(&catalog) >= commit.namespace_mutation_sequence,
            "ASSERT: live namespace cannot precede a frozen prefix"
        );
        catalog.committed_namespace_mutation_sequence = commit.namespace_mutation_sequence;
        let removed = catalog.inflight_commit.take();
        assert!(removed.is_some(), "ASSERT: completed commit disappeared");
        if let Some(admission) = self.commit_capacity_admission.get() {
            admission.complete(commit.token);
        }
        Ok(())
    }

    /// Executes one byte-exact POSIX semantic operation.
    ///
    /// # Errors
    ///
    /// Returns a stable semantic error for invalid user input, missing objects,
    /// invalid handles, or exhausted configured resources.
    ///
    /// # Panics
    ///
    /// Panics when internal namespace, lock, or capacity-attribution invariants
    /// are violated.
    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    pub fn dispatch(
        &self,
        context: RequestContext,
        operation: Operation<'_>,
    ) -> Result<Reply, PosixError> {
        let durable_mutation = operation.is_durable_mutation();
        let requires_mutation_admission = operation.requires_mutation_admission();
        let claim = operation.commit_capacity_claim();
        let _mutation_fence = requires_mutation_admission
            .then(|| self.require_mutation_admission())
            .transpose()?;
        let reservation = self.reserve_commit_capacity(claim)?;
        let result = match operation {
            Operation::Lookup { parent, name } => self.lookup(parent, name),
            Operation::GetAttr { inode } => self.getattr(inode),
            Operation::GetXattr { inode, name } => self.get_xattr(inode, name),
            Operation::ListXattrs { inode } => self.list_xattrs(inode),
            Operation::SetXattr {
                inode,
                name,
                value,
                mode,
            } => self.set_xattr(context, inode, name, value, mode),
            Operation::RemoveXattr { inode, name } => self.remove_xattr(context, inode, name),
            Operation::GetFileFlags { inode } => self.get_file_flags(inode),
            Operation::SetFileFlags { inode, flags } => self.set_file_flags(context, inode, flags),
            Operation::SetMode { inode, mode } => self.set_mode(context, inode, mode),
            Operation::SetAttributes { inode, update } => {
                self.set_attributes(context, inode, update)
            }
            Operation::Link {
                inode,
                new_parent,
                new_name,
            } => self.link(context, inode, new_parent, new_name),
            Operation::Symlink {
                parent,
                name,
                target,
            } => self.symlink(context, parent, name, target),
            Operation::Readlink { inode } => self.readlink(inode),
            Operation::Create {
                parent,
                name,
                mode,
                options,
                exclusive,
                truncate,
            } => self.create_and_track_writer(CreateRequest {
                context,
                parent,
                name,
                mode,
                umask: 0,
                options,
                exclusive,
                truncate,
            }),
            Operation::CreateWithUmask {
                parent,
                name,
                mode,
                umask,
                options,
                exclusive,
                truncate,
            } => self.create_and_track_writer(CreateRequest {
                context,
                parent,
                name,
                mode,
                umask,
                options,
                exclusive,
                truncate,
            }),
            Operation::Mkdir { parent, name, mode } => self.mkdir(context, parent, name, mode, 0),
            Operation::MkdirWithUmask {
                parent,
                name,
                mode,
                umask,
            } => self.mkdir(context, parent, name, mode, umask),
            Operation::Open {
                inode,
                options,
                truncate,
            } => self.open_and_track_writer(inode, options, truncate),
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
            Operation::CloneRange {
                source_inode,
                source_handle,
                source_offset,
                target_inode,
                target_handle,
                target_offset,
                length,
            } => self.clone_range(
                source_inode,
                source_handle,
                source_offset,
                target_inode,
                target_handle,
                target_offset,
                length,
            ),
            Operation::Fallocate {
                inode,
                handle,
                offset,
                length,
                mode,
            } => self.fallocate(inode, handle, offset, length, mode),
            Operation::Seek {
                inode,
                handle,
                offset,
                kind,
            } => self.seek(inode, handle, offset, kind),
            Operation::Sync {
                inode,
                handle,
                data_only: _,
            } => self.sync(inode, handle),
            Operation::GetLock {
                inode,
                handle,
                owner,
                lock,
            } => self.get_lock(inode, handle, owner, lock),
            Operation::SetLock {
                inode,
                handle,
                owner,
                lock,
            } => self.set_lock(inode, handle, owner, lock),
            Operation::UnlockOwner {
                inode,
                handle,
                owner,
            } => self.unlock_owner(inode, handle, owner),
            Operation::Release { inode, handle } => self.release_and_track_writer(inode, handle),
            Operation::Unlink { parent, name } => self.unlink(parent, name),
            Operation::Rmdir { parent, name } => self.rmdir(parent, name),
            Operation::Rename {
                parent,
                name,
                new_parent,
                new_name,
                no_replace,
            } => self.rename(parent, name, new_parent, new_name, no_replace),
            Operation::ReadDirectory {
                inode,
                offset,
                acquire_lookup,
            } => self.read_directory(inode, offset, acquire_lookup),
            Operation::Forget {
                inode,
                lookup_count,
            } => Ok(self.forget(inode, lookup_count)),
        };
        if durable_mutation && result.is_ok() {
            reservation.accept();
        }
        result
    }

    fn create_and_track_writer(&self, request: CreateRequest<'_>) -> Result<Reply, PosixError> {
        let writable = request.options.access != AccessMode::ReadOnly;
        let result = self.create(request);
        if writable && let Ok(Reply::Created { entry, .. }) = &result {
            self.notify_write_handle_opened(entry.attr.inode);
        }
        result
    }

    fn open_and_track_writer(
        &self,
        inode: InodeId,
        options: OpenOptions,
        truncate: bool,
    ) -> Result<Reply, PosixError> {
        let result = self.open(inode, options, truncate);
        if options.access != AccessMode::ReadOnly && result.is_ok() {
            self.notify_write_handle_opened(inode);
        }
        result
    }

    fn release_and_track_writer(
        &self,
        inode: InodeId,
        handle: HandleId,
    ) -> Result<Reply, PosixError> {
        let writable = self
            .resolve_open_file(inode, handle)
            .is_ok_and(|(_, open)| open.options.access != AccessMode::ReadOnly);
        let result = self.release(inode, handle);
        if writable && result.is_ok() {
            self.notify_write_handle_released(inode);
        }
        result
    }

    /// Executes one write from an already owned immutable payload.
    ///
    /// FUSE uses this seam after moving potentially blocking observer-queue
    /// admission onto its bounded blocking executor. The Dirty Extent Map and
    /// mutation observer receive views of the same backing allocation.
    ///
    /// # Errors
    ///
    /// Returns the same handle, range, admission, allocation, and capacity
    /// errors as [`Operation::Write`].
    pub fn dispatch_owned_write(
        &self,
        _context: RequestContext,
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        payload: MutationPayload,
    ) -> Result<Reply, PosixError> {
        let result = self.dispatch_owned_write_inner(inode, handle, offset, payload)?;
        Ok(result.reply)
    }

    pub(crate) fn dispatch_owned_write_for_fuse(
        &self,
        _context: RequestContext,
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        payload: MutationPayload,
    ) -> Result<(Reply, bool), PosixError> {
        let result = self.dispatch_owned_write_inner(inode, handle, offset, payload)?;
        Ok((result.reply, result.kernel_data_cache_exposed))
    }

    fn dispatch_owned_write_inner(
        &self,
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        payload: MutationPayload,
    ) -> Result<WriteResult, PosixError> {
        let _mutation_fence = self.require_mutation_admission()?;
        self.write_payload(inode, handle, offset, payload)
    }

    pub(crate) fn expose_kernel_data_cache(&self, inode: InodeId) -> Result<(), PosixError> {
        let object = self.resolve_inode(inode)?;
        object
            .kernel_data_cache_exposed
            .store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn kernel_data_cache_exposed(&self, inode: InodeId) -> bool {
        self.resolve_inode(inode)
            .is_ok_and(|object| object.kernel_data_cache_exposed.load(Ordering::Acquire))
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

    fn get_xattr(&self, inode: InodeId, name: &[u8]) -> Result<Reply, PosixError> {
        let object = self.resolve_inode(inode)?;
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        Ok(Reply::Xattr(state.metadata.get_xattr(name)?))
    }

    fn list_xattrs(&self, inode: InodeId) -> Result<Reply, PosixError> {
        let object = self.resolve_inode(inode)?;
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        Ok(Reply::Xattr(state.metadata.list_xattrs()?))
    }

    fn set_xattr(
        &self,
        context: RequestContext,
        inode: InodeId,
        name: &[u8],
        value: &[u8],
        mode: XattrSetMode,
    ) -> Result<Reply, PosixError> {
        self.mutate_metadata(context, inode, name, |state| {
            let metadata = Arc::make_mut(&mut state.metadata);
            if let Some(access_mode) = metadata.set_xattr(state.kind, name, value, mode)? {
                state.mode = state.mode & !0o777 | access_mode;
            }
            Ok(())
        })
    }

    fn remove_xattr(
        &self,
        context: RequestContext,
        inode: InodeId,
        name: &[u8],
    ) -> Result<Reply, PosixError> {
        self.mutate_metadata(context, inode, name, |state| {
            Arc::make_mut(&mut state.metadata).remove_xattr(name)
        })
    }

    fn get_file_flags(&self, inode: InodeId) -> Result<Reply, PosixError> {
        let object = self.resolve_inode(inode)?;
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        Ok(Reply::FileFlags(state.metadata.file_flags()))
    }

    fn set_file_flags(
        &self,
        context: RequestContext,
        inode: InodeId,
        flags: u32,
    ) -> Result<Reply, PosixError> {
        if context.uid != 0 {
            return Err(PosixError::PermissionDenied);
        }
        let catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .ok_or(PosixError::NoEntry)?;
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.metadata.file_flags() == flags {
            return Ok(Reply::Empty);
        }
        let reservation = self.reserve_commit_capacity(CommitCapacityClaim::new(
            MUTATION_METADATA_INCREMENT_BYTES_V1,
            0,
        ))?;
        Arc::make_mut(&mut state.metadata).set_file_flags(flags)?;
        state.times.ctime = PosixTimestamp::now();
        let (next_namespace_sequence, next_inode_sequence) =
            next_namespace_and_inode_mutation_sequences(&catalog, inode, state.mutation_sequence)?;
        state.mutation_sequence = next_inode_sequence;
        if state.kind == FileKind::Regular {
            let mutation_sequence = state.mutation_sequence;
            state.data.advance_mutation_sequence(mutation_sequence);
        }
        drop(state);
        if inode != ROOT_INODE {
            install_root_mutation_sequence(&catalog, next_namespace_sequence);
        }
        reservation.accept();
        Ok(Reply::Empty)
    }

    fn set_mode(
        &self,
        context: RequestContext,
        inode: InodeId,
        mode: u16,
    ) -> Result<Reply, PosixError> {
        let catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .ok_or(PosixError::NoEntry)?;
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if context.uid != 0 && context.uid != state.uid {
            return Err(PosixError::PermissionDenied);
        }
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        let (next_namespace_sequence, next_inode_sequence) =
            next_namespace_and_inode_mutation_sequences(&catalog, inode, state.mutation_sequence)?;
        state.mode = Arc::make_mut(&mut state.metadata).chmod(mode)?;
        state.times.ctime = PosixTimestamp::now();
        if state.kind == FileKind::Regular {
            state.data.advance_mutation_sequence(next_inode_sequence);
        }
        state.mutation_sequence = next_inode_sequence;
        let attr = state.attributes(inode);
        drop(state);
        if inode != ROOT_INODE {
            install_root_mutation_sequence(&catalog, next_namespace_sequence);
        }
        Ok(Reply::Attr(attr))
    }

    fn set_attributes(
        &self,
        context: RequestContext,
        inode: InodeId,
        update: InodeAttributesUpdate,
    ) -> Result<Reply, PosixError> {
        if update.mode.is_none()
            && update.uid.is_none()
            && update.gid.is_none()
            && update.atime.is_none()
            && update.mtime.is_none()
        {
            return self.getattr(inode);
        }
        if update
            .atime
            .is_some_and(|time| time.nanoseconds >= 1_000_000_000)
            || update
                .mtime
                .is_some_and(|time| time.nanoseconds >= 1_000_000_000)
        {
            return Err(PosixError::InvalidArgument);
        }
        let catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .ok_or(PosixError::NoEntry)?;
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        if context.uid != 0 {
            if context.uid != state.uid || update.uid.is_some() {
                return Err(PosixError::PermissionDenied);
            }
            if update.gid.is_some_and(|gid| gid != context.gid) {
                return Err(PosixError::PermissionDenied);
            }
        }
        let reservation = self.reserve_commit_capacity(CommitCapacityClaim::new(
            MUTATION_METADATA_INCREMENT_BYTES_V1,
            0,
        ))?;
        let (next_namespace_sequence, next_inode_sequence) =
            next_namespace_and_inode_mutation_sequences(&catalog, inode, state.mutation_sequence)?;
        if let Some(mode) = update.mode {
            state.mode = Arc::make_mut(&mut state.metadata).chmod(mode)?;
        }
        if let Some(uid) = update.uid {
            state.uid = uid;
        }
        if let Some(gid) = update.gid {
            state.gid = gid;
        }
        if update.uid.is_some() || update.gid.is_some() {
            state.mode &= !0o6000;
        }
        if let Some(atime) = update.atime {
            state.times.atime = atime;
        }
        if let Some(mtime) = update.mtime {
            state.times.mtime = mtime;
        }
        state.times.ctime = PosixTimestamp::now();
        if state.kind == FileKind::Regular {
            state.data.advance_mutation_sequence(next_inode_sequence);
        }
        state.mutation_sequence = next_inode_sequence;
        let attr = state.attributes(inode);
        drop(state);
        if inode != ROOT_INODE {
            install_root_mutation_sequence(&catalog, next_namespace_sequence);
        }
        reservation.accept();
        Ok(Reply::Attr(attr))
    }

    fn link(
        &self,
        context: RequestContext,
        inode: InodeId,
        new_parent: InodeId,
        new_name: &[u8],
    ) -> Result<Reply, PosixError> {
        self.validate_name(new_name)?;
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, new_parent)?;
        validate_mutable_directory(&catalog, new_parent)?;
        let key = (new_parent, new_name.to_vec());
        if catalog.entries.contains_key(&key) {
            return Err(PosixError::Exists);
        }
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .ok_or(PosixError::NoEntry)?;
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.kind == FileKind::Directory {
            return Err(PosixError::PermissionDenied);
        }
        if !self.logical_quotas.same_domain(inode, new_parent) {
            return Err(PosixError::CrossDevice);
        }
        if context.uid != 0 && context.uid != state.uid {
            return Err(PosixError::PermissionDenied);
        }
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        let next_namespace_sequence = next_root_mutation_sequence(&catalog)?;
        let next_inode_sequence = state
            .mutation_sequence
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        state.link_count = state.link_count.checked_add(1).ok_or(PosixError::NoSpace)?;
        state.mutation_sequence = next_inode_sequence;
        state.times.ctime = PosixTimestamp::now();
        if state.kind == FileKind::Regular {
            state.data.advance_mutation_sequence(next_inode_sequence);
        }
        let attr = state.attributes(inode);
        drop(state);
        assert!(catalog.entries.insert(key, inode).is_none());
        install_root_mutation_sequence(&catalog, next_namespace_sequence);
        acquire_lookup(&mut catalog, inode, 1)?;
        Ok(Reply::Entry(Entry { attr }))
    }

    fn symlink(
        &self,
        context: RequestContext,
        parent: InodeId,
        name: &[u8],
        target: &[u8],
    ) -> Result<Reply, PosixError> {
        self.validate_name(name)?;
        if target.is_empty() || target.len() > 4_096 {
            return Err(PosixError::InvalidArgument);
        }
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, parent)?;
        validate_mutable_directory(&catalog, parent)?;
        let key = (parent, name.to_vec());
        if catalog.entries.contains_key(&key) {
            return Err(PosixError::Exists);
        }
        let mut copied = Vec::new();
        copied
            .try_reserve_exact(target.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        copied.extend_from_slice(target);
        let next_namespace_sequence = next_root_mutation_sequence(&catalog)?;
        let reservation = self.reserve_commit_capacity(CommitCapacityClaim::new(
            MUTATION_METADATA_INCREMENT_BYTES_V1,
            0,
        ))?;
        let inode = allocate_inode(&mut catalog)?;
        let object = Arc::new(Inode {
            observer_order: Mutex::new(()),
            kernel_data_cache_exposed: AtomicBool::new(false),
            state: RwLock::new(InodeState {
                kind: FileKind::Symlink,
                mode: 0o777,
                uid: context.uid,
                gid: context.gid,
                link_count: 1,
                mutation_sequence: 0,
                metadata: Arc::new(InodeMetadata::default()),
                times: PosixTimes::now(),
                symlink_target: Some(Arc::from(copied)),
                data: VersionedFile::new_empty(),
            }),
        });
        let attr = object
            .state
            .read()
            .expect("ASSERT: symlink lock poisoned")
            .attributes(inode);
        assert!(catalog.inodes.insert(inode, object).is_none());
        assert!(catalog.lookup_counts.insert(inode, 1).is_none());
        assert!(catalog.entries.insert(key, inode).is_none());
        self.logical_quotas.associate_child(parent, inode);
        install_root_mutation_sequence(&catalog, next_namespace_sequence);
        if self.commit_capacity_admission.get().is_some() {
            assert!(
                catalog
                    .active_create_metadata_bytes
                    .insert(inode, MUTATION_METADATA_INCREMENT_BYTES_V1)
                    .is_none(),
                "ASSERT: a new inode receives one Active create claim"
            );
            reservation.accept();
        }
        Ok(Reply::Entry(Entry { attr }))
    }

    fn readlink(&self, inode: InodeId) -> Result<Reply, PosixError> {
        let object = self.resolve_inode(inode)?;
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        let target = state
            .symlink_target
            .as_ref()
            .ok_or(PosixError::InvalidArgument)?;
        let target = target.to_vec();
        drop(state);
        self.update_relatime(inode, &object);
        Ok(Reply::LinkTarget(target))
    }

    fn update_relatime(&self, inode: InodeId, object: &Arc<Inode>) {
        if !self.mutations_supported {
            return;
        }
        let now = PosixTimestamp::now();
        let should_update = {
            let state = object.state.read().expect("ASSERT: inode lock poisoned");
            relatime_due(state.times, now)
        };
        if !should_update {
            return;
        }
        let Ok(_admission) = self.require_mutation_admission() else {
            return;
        };
        let catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let Some(live) = catalog.inodes.get(&inode) else {
            return;
        };
        if !Arc::ptr_eq(live, object) {
            return;
        }
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if !relatime_due(state.times, now) {
            return;
        }
        let Ok((next_namespace_sequence, next_inode_sequence)) =
            next_namespace_and_inode_mutation_sequences(&catalog, inode, state.mutation_sequence)
        else {
            return;
        };
        let Ok(reservation) = self.reserve_commit_capacity(CommitCapacityClaim::new(
            MUTATION_METADATA_INCREMENT_BYTES_V1,
            0,
        )) else {
            return;
        };
        state.times.atime = now;
        if state.kind == FileKind::Regular {
            state.data.advance_mutation_sequence(next_inode_sequence);
        }
        state.mutation_sequence = next_inode_sequence;
        drop(state);
        if inode != ROOT_INODE {
            install_root_mutation_sequence(&catalog, next_namespace_sequence);
        }
        reservation.accept();
    }

    fn mutate_metadata(
        &self,
        context: RequestContext,
        inode: InodeId,
        name: &[u8],
        mutation: impl FnOnce(&mut InodeState) -> Result<(), PosixError>,
    ) -> Result<Reply, PosixError> {
        let catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .ok_or(PosixError::NoEntry)?;
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        authorize_xattr_mutation(context, &state, name)?;
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        let (next_namespace_sequence, next_inode_sequence) =
            next_namespace_and_inode_mutation_sequences(&catalog, inode, state.mutation_sequence)?;
        mutation(&mut state)?;
        state.times.ctime = PosixTimestamp::now();
        if state.kind == FileKind::Regular {
            state.data.advance_mutation_sequence(next_inode_sequence);
        }
        state.mutation_sequence = next_inode_sequence;
        drop(state);
        if inode != ROOT_INODE {
            install_root_mutation_sequence(&catalog, next_namespace_sequence);
        }
        Ok(Reply::Empty)
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
            let observer = self
                .mutation_observer
                .read()
                .expect("ASSERT: mutation observer lock poisoned")
                .clone();
            return open_existing_for_create(
                &mut catalog,
                inode,
                request,
                &self.dirty_payload,
                &self.logical_quotas,
                observer.as_deref(),
            );
        }
        validate_mutable_directory(&catalog, request.parent)?;
        let reservation = self.reserve_commit_capacity(CommitCapacityClaim::new(
            MUTATION_METADATA_INCREMENT_BYTES_V1,
            0,
        ))?;
        let result = create_new_file(&mut catalog, key, request);
        if let Ok(Reply::Created { entry, .. }) = &result {
            self.logical_quotas
                .associate_child(request.parent, entry.attr.inode);
        }
        if let Ok(Reply::Created { entry, .. }) = &result
            && self.commit_capacity_admission.get().is_some()
        {
            assert!(
                catalog
                    .active_create_metadata_bytes
                    .insert(entry.attr.inode, MUTATION_METADATA_INCREMENT_BYTES_V1,)
                    .is_none(),
                "ASSERT: a new inode receives one Active create claim"
            );
            reservation.accept();
        }
        result
    }

    fn mkdir(
        &self,
        context: RequestContext,
        parent: InodeId,
        name: &[u8],
        mode: u16,
        umask: u16,
    ) -> Result<Reply, PosixError> {
        self.validate_name(name)?;
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, parent)?;
        let key = (parent, name.to_vec());
        if catalog.entries.contains_key(&key) {
            return Err(PosixError::Exists);
        }
        validate_mutable_directory(&catalog, parent)?;
        let next_namespace_sequence = next_root_mutation_sequence(&catalog)?;
        let parent_object = catalog
            .inodes
            .get(&parent)
            .cloned()
            .expect("ASSERT: validated parent directory exists");
        let mut parent_state = parent_object
            .state
            .write()
            .expect("ASSERT: parent directory lock poisoned");
        let next_parent_links = parent_state
            .link_count
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        let next_parent_sequence = if parent == ROOT_INODE {
            next_namespace_sequence
        } else {
            parent_state
                .mutation_sequence
                .checked_add(1)
                .ok_or(PosixError::NoSpace)?
        };
        let (mode, metadata) = parent_state
            .metadata
            .for_child(FileKind::Directory, mode, umask)?;
        let reservation = self.reserve_commit_capacity(CommitCapacityClaim::new(
            MUTATION_METADATA_INCREMENT_BYTES_V1,
            0,
        ))?;
        let inode = allocate_inode(&mut catalog)?;
        let object = Arc::new(Inode {
            observer_order: Mutex::new(()),
            kernel_data_cache_exposed: AtomicBool::new(false),
            state: RwLock::new(InodeState {
                kind: FileKind::Directory,
                mode,
                uid: context.uid,
                gid: context.gid,
                link_count: 2,
                mutation_sequence: 0,
                metadata: Arc::new(metadata),
                times: PosixTimes::now(),
                symlink_target: None,
                data: VersionedFile::new_empty(),
            }),
        });
        let attr = object
            .state
            .read()
            .expect("ASSERT: new directory lock poisoned")
            .attributes(inode);
        assert!(
            catalog.inodes.insert(inode, object).is_none(),
            "ASSERT: monotonic inode allocator returned a live ID"
        );
        assert!(
            catalog.lookup_counts.insert(inode, 1).is_none(),
            "ASSERT: new directory must not have lookup references"
        );
        assert!(
            catalog.entries.insert(key, inode).is_none(),
            "ASSERT: mkdir replaced an existing directory entry"
        );
        self.logical_quotas.associate_child(parent, inode);
        parent_state.link_count = next_parent_links;
        parent_state.mutation_sequence = next_parent_sequence;
        drop(parent_state);
        if parent != ROOT_INODE {
            install_root_mutation_sequence(&catalog, next_namespace_sequence);
        }
        if self.commit_capacity_admission.get().is_some() {
            assert!(
                catalog
                    .active_create_metadata_bytes
                    .insert(inode, MUTATION_METADATA_INCREMENT_BYTES_V1)
                    .is_none(),
                "ASSERT: a new inode receives one Active create claim"
            );
            reservation.accept();
        }
        Ok(Reply::Entry(Entry { attr }))
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
        if state.kind != FileKind::Regular {
            return Err(PosixError::InvalidArgument);
        }
        if (options.access != AccessMode::ReadOnly || truncate) && state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
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
        let logical_quota = next_sequence
            .map(|_| {
                self.logical_quotas
                    .reserve_change(inode, state.data.allocated_bytes(), 0)
            })
            .transpose()?;

        let handle = allocate_handle(&mut catalog)?;
        if let Some(sequence) = next_sequence {
            let dirty_before = state.data.active_resident_payload_bytes();
            state.data.truncate(0, sequence)?;
            let dirty_after = state.data.active_resident_payload_bytes();
            if state.link_count > 0 {
                self.dirty_payload.replace(dirty_before, dirty_after);
            }
            state.mutation_sequence = sequence;
            let now = PosixTimestamp::now();
            state.times.mtime = now;
            state.times.ctime = now;
        }
        drop(state);
        if let Some(logical_quota) = logical_quota {
            logical_quota.accept();
        }
        assert!(
            catalog
                .handles
                .insert(handle, OpenHandle { inode, options })
                .is_none(),
            "ASSERT: monotonic handle allocator returned a live ID"
        );
        drop(catalog);
        if let Some(sequence) = next_sequence
            && let Some(observer) = self
                .mutation_observer
                .read()
                .expect("ASSERT: mutation observer lock poisoned")
                .as_ref()
                .cloned()
        {
            observer.accepted_truncate(inode, sequence, 0);
        }
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
        let bytes = plan.execute()?;
        self.update_relatime(inode, &object);
        Ok(Reply::Data(bytes))
    }

    fn write(
        &self,
        inode: InodeId,
        handle: HandleId,
        requested_offset: u64,
        data: &[u8],
    ) -> Result<Reply, PosixError> {
        let payload = MutationPayload::try_copy_from_slice(data)?;
        self.write_payload(inode, handle, requested_offset, payload)
            .map(|result| result.reply)
    }

    #[allow(clippy::too_many_lines)]
    fn write_payload(
        &self,
        inode: InodeId,
        handle: HandleId,
        requested_offset: u64,
        payload: MutationPayload,
    ) -> Result<WriteResult, PosixError> {
        let written = u32::try_from(payload.len()).map_err(|_| PosixError::FileTooLarge)?;
        let (object, open) = self.resolve_open_file(inode, handle)?;
        let policy_name_matches = self
            .catalog
            .read()
            .expect("ASSERT: namespace catalog lock poisoned")
            .entries
            .iter()
            .any(|((_, name), target)| {
                *target == inode
                    && (ascii_suffix_eq(name, b".xml") || ascii_suffix_eq(name, b".json"))
            });
        if open.options.access == AccessMode::ReadOnly {
            return Err(PosixError::BadHandle);
        }
        let observer_order = object
            .observer_order
            .lock()
            .expect("ASSERT: inode observer-order lock poisoned");
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        assert_eq!(
            state.kind,
            FileKind::Regular,
            "ASSERT: a file handle must reference a regular inode"
        );
        let offset = if open.options.append {
            state.data.logical_size()
        } else {
            requested_offset
        };
        if payload.is_empty() {
            return Ok(WriteResult {
                reply: Reply::Written {
                    bytes: 0,
                    mutation_sequence: state.mutation_sequence,
                    offset,
                },
                kernel_data_cache_exposed: object.kernel_data_cache_exposed.load(Ordering::Acquire),
            });
        }
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        let data_length = u64::try_from(payload.len()).expect("ASSERT: usize must fit in u64");
        let end = offset
            .checked_add(data_length)
            .ok_or(PosixError::FileTooLarge)?;
        if end > self.config.maximum_file_bytes {
            return Err(PosixError::FileTooLarge);
        }
        let capacity = state.data.plan_write_capacity(offset, data_length)?;
        let small_file = small_file_policy_with_name_match(
            state.data.logical_size().max(end),
            &state.metadata,
            policy_name_matches,
        );
        let reservation = self.reserve_commit_capacity(write_capacity_claim(
            payload.len(),
            capacity.metadata_bytes(),
            small_file,
        )?)?;
        let logical_before = state.data.allocated_bytes();
        let overwritten_end = end.min(state.data.logical_size());
        let overwritten = if offset < overwritten_end {
            state
                .data
                .allocated_bytes_in_live_range(offset, overwritten_end)?
        } else {
            0
        };
        let logical_after = logical_before
            .checked_sub(overwritten)
            .and_then(|remaining| remaining.checked_add(data_length))
            .ok_or(PosixError::NoSpace)?;
        let logical_quota =
            self.logical_quotas
                .reserve_change(inode, logical_before, logical_after)?;
        let next_sequence = state
            .mutation_sequence
            .checked_add(u64::from(!payload.is_empty()))
            .ok_or(PosixError::NoSpace)?;

        let dirty_before = state.data.active_resident_payload_bytes();
        state
            .data
            .write_payload(offset, payload.clone(), next_sequence)?;
        assert_eq!(
            state.data.allocated_bytes(),
            logical_after,
            "ASSERT: admitted write allocation must match its logical quota claim"
        );
        state.data.accept_write_capacity(capacity);
        let dirty_after = state.data.active_resident_payload_bytes();
        if state.link_count > 0 {
            self.dirty_payload.replace(dirty_before, dirty_after);
        }
        state.mutation_sequence = next_sequence;
        let now = PosixTimestamp::now();
        state.times.mtime = now;
        state.times.ctime = now;
        drop(state);
        logical_quota.accept();
        let externalized = self
            .mutation_observer
            .read()
            .expect("ASSERT: mutation observer lock poisoned")
            .as_ref()
            .cloned()
            .map_or_else(Vec::new, |observer| {
                observer.accepted_write(inode, offset, next_sequence, small_file, payload)
            });
        drop(observer_order);
        self.externalize_verified_extents(externalized);
        reservation.accept();

        Ok(WriteResult {
            reply: Reply::Written {
                bytes: written,
                mutation_sequence: next_sequence,
                offset,
            },
            kernel_data_cache_exposed: object.kernel_data_cache_exposed.load(Ordering::Acquire),
        })
    }

    /// Replaces matching resident dirty ranges with independently verified
    /// immutable sources while retaining the byte-exact fallback on rejection.
    ///
    /// This acceleration interface does not change mutation order, commit
    /// membership, or visibility. Sources that do not match either the Active
    /// Dirty Epoch or the one Frozen Commit Cut are ignored.
    ///
    /// # Panics
    ///
    /// Panics if an inode lock is poisoned, which marks an impossible internal
    /// synchronization failure.
    pub fn externalize_verified_extents(&self, extents: Vec<ExternalizedExtent>) {
        let mut by_inode = BTreeMap::<InodeId, Vec<(u64, u64, Arc<dyn CommittedFile>)>>::new();
        for extent in extents {
            by_inode.entry(extent.inode).or_default().push((
                extent.offset,
                extent.through_sequence,
                extent.data,
            ));
        }
        for (inode, candidates) in by_inode {
            let Ok(object) = self.resolve_inode(inode) else {
                continue;
            };
            let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
            let resident_before = state.data.active_resident_payload_bytes();
            if state.data.externalize_many(candidates).is_err() {
                continue;
            }
            let resident_after = state.data.active_resident_payload_bytes();
            if state.link_count > 0 {
                self.dirty_payload.replace(resident_before, resident_after);
            }
        }
    }

    fn observe_truncate(&self, inode: InodeId, mutation_sequence: u64, length: u64) {
        if let Some(observer) = self
            .mutation_observer
            .read()
            .expect("ASSERT: mutation observer lock poisoned")
            .as_ref()
            .cloned()
        {
            observer.accepted_truncate(inode, mutation_sequence, length);
        }
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
        let observer_order = object
            .observer_order
            .lock()
            .expect("ASSERT: inode observer-order lock poisoned");
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.kind == FileKind::Directory {
            return Err(PosixError::IsDirectory);
        }
        if state.kind != FileKind::Regular {
            return Err(PosixError::InvalidArgument);
        }
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        let current_size = state.data.logical_size();
        if length == current_size {
            return Ok(Reply::Attr(state.attributes(inode)));
        }
        let capacity = if length > current_size {
            CommitCapacityClaim::new(MUTATION_METADATA_INCREMENT_BYTES_V1, 0)
        } else {
            CommitCapacityClaim::default()
        };
        let reservation = self.reserve_commit_capacity(capacity)?;
        let logical_before = state.data.allocated_bytes();
        let logical_after = if length < current_size {
            logical_before
                .checked_sub(
                    state
                        .data
                        .allocated_bytes_in_live_range(length, current_size)?,
                )
                .ok_or(PosixError::Io)?
        } else {
            logical_before
        };
        let logical_quota =
            self.logical_quotas
                .reserve_change(inode, logical_before, logical_after)?;
        let next_sequence = state
            .mutation_sequence
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        let dirty_before = state.data.active_resident_payload_bytes();
        state.data.truncate(length, next_sequence)?;
        assert_eq!(
            state.data.allocated_bytes(),
            logical_after,
            "ASSERT: admitted truncate allocation must match its logical quota claim"
        );
        let dirty_after = state.data.active_resident_payload_bytes();
        if state.link_count > 0 {
            self.dirty_payload.replace(dirty_before, dirty_after);
        }
        state.mutation_sequence = next_sequence;
        let now = PosixTimestamp::now();
        state.times.mtime = now;
        state.times.ctime = now;
        let attr = state.attributes(inode);
        drop(state);
        logical_quota.accept();
        self.observe_truncate(inode, next_sequence, length);
        drop(observer_order);
        reservation.accept();
        Ok(Reply::Attr(attr))
    }

    #[allow(clippy::too_many_lines)]
    fn fallocate(
        &self,
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        length: u64,
        mode: FallocateMode,
    ) -> Result<Reply, PosixError> {
        if length == 0 {
            return Err(PosixError::InvalidArgument);
        }
        let end = offset.checked_add(length).ok_or(PosixError::FileTooLarge)?;
        let (object, open) = self.resolve_open_file(inode, handle)?;
        if open.options.access == AccessMode::ReadOnly {
            return Err(PosixError::BadHandle);
        }
        let observer_order = object
            .observer_order
            .lock()
            .expect("ASSERT: inode observer-order lock poisoned");
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.kind != FileKind::Regular {
            return Err(PosixError::IsDirectory);
        }
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        let current_size = state.data.logical_size();
        let result_size = match mode {
            FallocateMode::Allocate { keep_size: true }
            | FallocateMode::ZeroRange { keep_size: true }
            | FallocateMode::PunchHole => current_size,
            FallocateMode::Allocate { keep_size: false }
            | FallocateMode::ZeroRange { keep_size: false } => current_size.max(end),
            FallocateMode::CollapseRange => {
                if end >= current_size {
                    return Err(PosixError::InvalidArgument);
                }
                current_size - length
            }
            FallocateMode::InsertRange => {
                if offset >= current_size {
                    return Err(PosixError::InvalidArgument);
                }
                current_size
                    .checked_add(length)
                    .ok_or(PosixError::FileTooLarge)?
            }
        };
        if result_size > self.config.maximum_file_bytes {
            return Err(PosixError::FileTooLarge);
        }
        let logical_before = state.data.allocated_bytes();
        let effective_end = end.min(result_size);
        let logical_after = match mode {
            FallocateMode::Allocate { .. } | FallocateMode::ZeroRange { .. } => {
                let allocated = if offset < effective_end {
                    state
                        .data
                        .allocated_bytes_in_live_range(offset, effective_end)?
                } else {
                    0
                };
                logical_before
                    .checked_add(
                        effective_end
                            .saturating_sub(offset)
                            .saturating_sub(allocated),
                    )
                    .ok_or(PosixError::NoSpace)?
            }
            FallocateMode::PunchHole | FallocateMode::CollapseRange => logical_before
                .checked_sub(state.data.allocated_bytes_in_live_range(
                    offset.min(current_size),
                    end.min(current_size),
                )?)
                .ok_or(PosixError::Io)?,
            FallocateMode::InsertRange => logical_before,
        };
        let logical_quota =
            self.logical_quotas
                .reserve_change(inode, logical_before, logical_after)?;
        let next_sequence = state
            .mutation_sequence
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        let dirty_before = state.data.active_resident_payload_bytes();
        match mode {
            FallocateMode::Allocate { .. } => {
                let effective_end = end.min(result_size);
                state.data.allocate_zero(
                    offset.min(effective_end),
                    effective_end,
                    result_size,
                    next_sequence,
                )?;
            }
            FallocateMode::PunchHole => {
                state.data.punch_hole(offset, end, next_sequence)?;
            }
            FallocateMode::ZeroRange { .. } => {
                let effective_end = end.min(result_size);
                if offset < effective_end {
                    state
                        .data
                        .zero_range(offset, effective_end, result_size, next_sequence)?;
                } else {
                    state.data.allocate_zero(
                        effective_end,
                        effective_end,
                        result_size,
                        next_sequence,
                    )?;
                }
            }
            FallocateMode::CollapseRange => {
                state.data.collapse_range(offset, end, next_sequence)?;
            }
            FallocateMode::InsertRange => {
                state.data.insert_range(offset, length, next_sequence)?;
            }
        }
        assert_eq!(
            state.data.allocated_bytes(),
            logical_after,
            "ASSERT: admitted fallocate allocation must match its logical quota claim"
        );
        let dirty_after = state.data.active_resident_payload_bytes();
        if state.link_count > 0 {
            self.dirty_payload.replace(dirty_before, dirty_after);
        }
        state.mutation_sequence = next_sequence;
        let now = PosixTimestamp::now();
        state.times.mtime = now;
        state.times.ctime = now;
        let attr = state.attributes(inode);
        drop(state);
        logical_quota.accept();
        self.observe_truncate(inode, next_sequence, result_size);
        drop(observer_order);
        Ok(Reply::Attr(attr))
    }

    fn seek(
        &self,
        inode: InodeId,
        handle: HandleId,
        offset: u64,
        kind: SeekKind,
    ) -> Result<Reply, PosixError> {
        let (object, _) = self.resolve_open_file(inode, handle)?;
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        if state.kind != FileKind::Regular {
            return Err(PosixError::IsDirectory);
        }
        let found = match kind {
            SeekKind::Data => state.data.seek_data(offset)?,
            SeekKind::Hole => state.data.seek_hole(offset)?,
        }
        .ok_or(PosixError::NoSuchAddress)?;
        Ok(Reply::Offset(found))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn clone_range(
        &self,
        source_inode: InodeId,
        source_handle: HandleId,
        source_offset: u64,
        target_inode: InodeId,
        target_handle: HandleId,
        target_offset: u64,
        length: u64,
    ) -> Result<Reply, PosixError> {
        let (source_object, source_open) = self.resolve_open_file(source_inode, source_handle)?;
        let (target_object, target_open) = self.resolve_open_file(target_inode, target_handle)?;
        if source_open.options.access == AccessMode::WriteOnly
            || target_open.options.access == AccessMode::ReadOnly
        {
            return Err(PosixError::BadHandle);
        }
        let source_end = source_offset
            .checked_add(length)
            .ok_or(PosixError::FileTooLarge)?;
        let target_end = target_offset
            .checked_add(length)
            .ok_or(PosixError::FileTooLarge)?;
        if target_end > self.config.maximum_file_bytes {
            return Err(PosixError::FileTooLarge);
        }
        if source_inode == target_inode
            && source_offset < target_end
            && target_offset < source_end
            && length != 0
        {
            return Err(PosixError::Unsupported);
        }
        if length == 0 {
            let target = target_object
                .state
                .read()
                .expect("ASSERT: target inode lock poisoned");
            return Ok(Reply::Cloned {
                bytes: 0,
                mutation_sequence: target.mutation_sequence,
            });
        }

        let source = {
            let state = source_object
                .state
                .read()
                .expect("ASSERT: source inode lock poisoned");
            if state.kind != FileKind::Regular || source_end > state.data.logical_size() {
                return Err(PosixError::InvalidArgument);
            }
            let source = state.data.stable_clone_source()?;
            let Some(prepared) = source.prepared_clone_extents(source_offset, length)? else {
                return Err(PosixError::Unsupported);
            };
            verify_prepared_clone_partition(&prepared, source_offset, length)?;
            source
        };

        let observer_order = target_object
            .observer_order
            .lock()
            .expect("ASSERT: target observer-order lock poisoned");
        let mut target = target_object
            .state
            .write()
            .expect("ASSERT: target inode lock poisoned");
        assert_eq!(
            target.kind,
            FileKind::Regular,
            "ASSERT: a file handle must reference a regular inode"
        );
        if target.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        let logical_before = target.data.allocated_bytes();
        let overwritten_end = target_end.min(target.data.logical_size());
        let overwritten = if target_offset < overwritten_end {
            target
                .data
                .allocated_bytes_in_live_range(target_offset, overwritten_end)?
        } else {
            0
        };
        let logical_after = logical_before
            .checked_sub(overwritten)
            .and_then(|remaining| remaining.checked_add(length))
            .ok_or(PosixError::NoSpace)?;
        let logical_quota =
            self.logical_quotas
                .reserve_change(target_inode, logical_before, logical_after)?;
        let next_sequence = target
            .mutation_sequence
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        let dirty_before = target.data.active_resident_payload_bytes();
        target
            .data
            .clone_range(target_offset, source, source_offset, length, next_sequence)?;
        assert_eq!(
            target.data.allocated_bytes(),
            logical_after,
            "ASSERT: admitted clone allocation must match its logical quota claim"
        );
        let dirty_after = target.data.active_resident_payload_bytes();
        assert_eq!(
            dirty_before, dirty_after,
            "ASSERT: a metadata clone must not allocate resident dirty payload"
        );
        target.mutation_sequence = next_sequence;
        let now = PosixTimestamp::now();
        target.times.mtime = now;
        target.times.ctime = now;
        let target_size = target.data.logical_size();
        drop(target);
        logical_quota.accept();
        self.observe_truncate(target_inode, next_sequence, target_size);
        drop(observer_order);
        Ok(Reply::Cloned {
            bytes: length,
            mutation_sequence: next_sequence,
        })
    }

    fn sync(&self, inode: InodeId, handle: HandleId) -> Result<Reply, PosixError> {
        let (object, _) = self.resolve_open_file(inode, handle)?;
        let observer_order = object
            .observer_order
            .lock()
            .expect("ASSERT: inode observer-order lock poisoned");
        let mutation_sequence = object
            .state
            .read()
            .expect("ASSERT: inode lock poisoned")
            .mutation_sequence;
        if let Some(observer) = self
            .mutation_observer
            .read()
            .expect("ASSERT: mutation observer lock poisoned")
            .as_ref()
            .cloned()
        {
            observer.wait_through(inode, mutation_sequence);
        }
        drop(observer_order);
        Ok(Reply::Empty)
    }

    fn get_lock(
        &self,
        inode: InodeId,
        handle: HandleId,
        owner: u64,
        requested: FileLock,
    ) -> Result<Reply, PosixError> {
        self.validate_lock_handle(inode, handle)?;
        validate_lock_request(requested, false)?;
        let requested = RecordLock::from_request(owner, requested);
        let locks = self
            .locks
            .lock()
            .expect("ASSERT: record lock table poisoned");
        let conflict = locks
            .by_inode
            .get(&inode)
            .and_then(|held| first_lock_conflict(held, requested));
        Ok(Reply::Lock(conflict.map_or(
            FileLock {
                start: requested.start,
                end: requested.end,
                kind: LockKind::Unlock,
                pid: 0,
            },
            RecordLock::as_reply,
        )))
    }

    fn set_lock(
        &self,
        inode: InodeId,
        handle: HandleId,
        owner: u64,
        requested: FileLock,
    ) -> Result<Reply, PosixError> {
        let open = self.validate_lock_handle(inode, handle)?;
        validate_lock_request(requested, true)?;
        match (requested.kind, open.options.access) {
            (LockKind::Read, AccessMode::WriteOnly) | (LockKind::Write, AccessMode::ReadOnly) => {
                return Err(PosixError::BadHandle);
            }
            (LockKind::Read | LockKind::Write | LockKind::Unlock, _) => {}
        }
        let requested = RecordLock::from_request(owner, requested);
        let mut locks = self
            .locks
            .lock()
            .expect("ASSERT: record lock table poisoned");
        let held = locks.by_inode.get(&inode).map_or(&[][..], Vec::as_slice);
        if requested.kind != LockKind::Unlock && first_lock_conflict(held, requested).is_some() {
            return Err(PosixError::Again);
        }
        let updated = replace_owner_lock_range(held, requested)?;
        if updated == held {
            return Ok(Reply::Empty);
        }
        let new_count = locks
            .record_count
            .checked_sub(held.len())
            .and_then(|count| count.checked_add(updated.len()))
            .ok_or(PosixError::NoLocks)?;
        if new_count > MAXIMUM_RECORD_LOCKS {
            return Err(PosixError::NoLocks);
        }
        if updated.is_empty() {
            locks.by_inode.remove(&inode);
        } else {
            locks.by_inode.insert(inode, updated);
        }
        locks.record_count = new_count;
        drop(locks);
        self.announce_lock_change();
        Ok(Reply::Empty)
    }

    fn unlock_owner(
        &self,
        inode: InodeId,
        handle: HandleId,
        owner: u64,
    ) -> Result<Reply, PosixError> {
        self.validate_lock_handle(inode, handle)?;
        let mut locks = self
            .locks
            .lock()
            .expect("ASSERT: record lock table poisoned");
        let Some(held) = locks.by_inode.get_mut(&inode) else {
            return Ok(Reply::Empty);
        };
        let before = held.len();
        held.retain(|lock| lock.owner != owner);
        let removed = before - held.len();
        if removed == 0 {
            return Ok(Reply::Empty);
        }
        let inode_is_unlocked = held.is_empty();
        locks.record_count = locks
            .record_count
            .checked_sub(removed)
            .expect("ASSERT: lock table count covers every inode lock");
        if inode_is_unlocked {
            locks.by_inode.remove(&inode);
        }
        drop(locks);
        self.announce_lock_change();
        Ok(Reply::Empty)
    }

    fn validate_lock_handle(
        &self,
        inode: InodeId,
        handle: HandleId,
    ) -> Result<OpenHandle, PosixError> {
        let (object, open) = self.resolve_open_file(inode, handle)?;
        if object
            .state
            .read()
            .expect("ASSERT: inode lock poisoned")
            .kind
            != FileKind::Regular
        {
            return Err(PosixError::IsDirectory);
        }
        Ok(open)
    }

    fn announce_lock_change(&self) {
        self.lock_change_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .expect("ASSERT: record lock change sequence must not overflow");
        self.lock_changed.notify_waiters();
    }

    fn lock_sequence(&self) -> u64 {
        self.lock_change_sequence.load(Ordering::Acquire)
    }

    async fn wait_for_lock_change(&self, observed: u64) {
        loop {
            let notified = self.lock_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.lock_sequence() != observed {
                return;
            }
            notified.await;
        }
    }

    fn release(&self, inode: InodeId, handle: HandleId) -> Result<Reply, PosixError> {
        let (object, _) = self.resolve_open_file(inode, handle)?;
        let observer_order = object
            .observer_order
            .lock()
            .expect("ASSERT: inode observer-order lock poisoned");
        let mutation_sequence = object
            .state
            .read()
            .expect("ASSERT: inode lock poisoned")
            .mutation_sequence;
        if let Some(observer) = self
            .mutation_observer
            .read()
            .expect("ASSERT: mutation observer lock poisoned")
            .as_ref()
            .cloned()
        {
            observer.wait_through(inode, mutation_sequence);
        }
        drop(observer_order);
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
            self.release_removed_inode_quota(inode, &object);
        }
        Ok(Reply::Empty)
    }

    fn unlink(&self, parent: InodeId, name: &[u8]) -> Result<Reply, PosixError> {
        self.validate_name(name)?;
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, parent)?;
        validate_mutable_directory(&catalog, parent)?;
        let next_namespace_sequence = next_root_mutation_sequence(&catalog)?;
        let key = (parent, name.to_vec());
        let inode = *catalog.entries.get(&key).ok_or(PosixError::NoEntry)?;
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .expect("ASSERT: directory entry must reference a live inode");
        let observer_order = object
            .observer_order
            .lock()
            .expect("ASSERT: inode observer-order lock poisoned");
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.kind == FileKind::Directory {
            return Err(PosixError::IsDirectory);
        }
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        let next_sequence = state
            .mutation_sequence
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        let final_link = state.link_count == 1;
        let dirty_before = state.data.active_resident_payload_bytes();
        if state.kind == FileKind::Regular {
            state.data.advance_mutation_sequence(next_sequence);
        }
        state.link_count -= 1;
        state.mutation_sequence = next_sequence;
        state.times.ctime = PosixTimestamp::now();
        if final_link && state.kind == FileKind::Regular {
            self.dirty_payload.replace(dirty_before, 0);
        }
        drop(state);
        let removed = catalog.entries.remove(&key);
        assert_eq!(removed, Some(inode), "ASSERT: validated name disappeared");
        install_root_mutation_sequence(&catalog, next_namespace_sequence);
        let reversed_create_claim = final_link
            .then(|| catalog.active_create_metadata_bytes.remove(&inode))
            .flatten();

        let has_lookup = catalog.lookup_counts.get(&inode).copied().unwrap_or(0) != 0;
        if final_link
            && !has_lookup
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
            self.release_removed_inode_quota(inode, &object);
        }
        drop(catalog);
        if let Some(bytes) = reversed_create_claim
            && let Some(admission) = self.commit_capacity_admission.get()
        {
            admission.release_active_metadata(bytes);
        }
        if final_link
            && object
                .state
                .read()
                .expect("ASSERT: inode lock poisoned")
                .kind
                == FileKind::Regular
        {
            self.observe_truncate(inode, next_sequence, 0);
        }
        drop(observer_order);
        Ok(Reply::Empty)
    }

    fn rmdir(&self, parent: InodeId, name: &[u8]) -> Result<Reply, PosixError> {
        self.validate_name(name)?;
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, parent)?;
        validate_mutable_directory(&catalog, parent)?;
        let key = (parent, name.to_vec());
        let inode = *catalog.entries.get(&key).ok_or(PosixError::NoEntry)?;
        let object = catalog
            .inodes
            .get(&inode)
            .cloned()
            .expect("ASSERT: directory entry must reference a live inode");
        let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
        if state.kind != FileKind::Directory {
            return Err(PosixError::NotDirectory);
        }
        if state.metadata.is_immutable() {
            return Err(PosixError::PermissionDenied);
        }
        if catalog
            .entries
            .range((inode, Vec::new())..)
            .next()
            .is_some_and(|((entry_parent, _), _)| *entry_parent == inode)
        {
            return Err(PosixError::NotEmpty);
        }
        assert_eq!(
            state.link_count, 2,
            "ASSERT: an empty linked directory has exactly dot and parent links"
        );
        let next_sequence = state
            .mutation_sequence
            .checked_add(1)
            .ok_or(PosixError::NoSpace)?;
        let next_namespace_sequence = next_root_mutation_sequence(&catalog)?;
        let parent_object = catalog
            .inodes
            .get(&parent)
            .cloned()
            .expect("ASSERT: validated parent directory exists");
        let mut parent_state = parent_object
            .state
            .write()
            .expect("ASSERT: parent directory lock poisoned");
        let next_parent_links = parent_state
            .link_count
            .checked_sub(1)
            .expect("ASSERT: linked child directory contributes one parent link");
        let next_parent_sequence = if parent == ROOT_INODE {
            next_namespace_sequence
        } else {
            parent_state
                .mutation_sequence
                .checked_add(1)
                .ok_or(PosixError::NoSpace)?
        };
        state.link_count = 0;
        state.mutation_sequence = next_sequence;
        parent_state.link_count = next_parent_links;
        parent_state.mutation_sequence = next_parent_sequence;
        drop(parent_state);
        drop(state);
        let removed = catalog.entries.remove(&key);
        assert_eq!(
            removed,
            Some(inode),
            "ASSERT: validated rmdir name disappeared"
        );
        if parent != ROOT_INODE {
            install_root_mutation_sequence(&catalog, next_namespace_sequence);
        }
        let has_lookup = catalog.lookup_counts.get(&inode).copied().unwrap_or(0) != 0;
        if !has_lookup {
            let removed = catalog.inodes.remove(&inode);
            assert!(
                removed.is_some(),
                "ASSERT: unpinned removed directory disappeared"
            );
            self.release_removed_inode_quota(inode, &object);
        }
        let reversed_create_claim = catalog.active_create_metadata_bytes.remove(&inode);
        drop(catalog);
        if let Some(bytes) = reversed_create_claim
            && let Some(admission) = self.commit_capacity_admission.get()
        {
            admission.release_active_metadata(bytes);
        }
        Ok(Reply::Empty)
    }

    #[allow(clippy::too_many_lines)]
    fn rename(
        &self,
        parent: InodeId,
        name: &[u8],
        new_parent: InodeId,
        new_name: &[u8],
        no_replace: bool,
    ) -> Result<Reply, PosixError> {
        self.validate_name(name)?;
        self.validate_name(new_name)?;
        let mut catalog = self.catalog.write().expect("ASSERT: catalog lock poisoned");
        validate_directory(&catalog, parent)?;
        validate_directory(&catalog, new_parent)?;
        validate_mutable_directory(&catalog, parent)?;
        if parent != new_parent {
            validate_mutable_directory(&catalog, new_parent)?;
        }
        let old_key = (parent, name.to_vec());
        let new_key = (new_parent, new_name.to_vec());
        let source_inode = *catalog.entries.get(&old_key).ok_or(PosixError::NoEntry)?;
        if old_key == new_key {
            return Ok(Reply::Empty);
        }
        let replaced_inode = catalog.entries.get(&new_key).copied();
        if no_replace && replaced_inode.is_some() {
            return Err(PosixError::Exists);
        }
        if replaced_inode == Some(source_inode) {
            return Ok(Reply::Empty);
        }
        if !self.logical_quotas.same_domain(source_inode, new_parent) {
            return Err(PosixError::CrossDevice);
        }
        let source_kind = catalog
            .inodes
            .get(&source_inode)
            .expect("ASSERT: rename source entry references a live inode")
            .state
            .read()
            .expect("ASSERT: rename source inode lock poisoned")
            .kind;
        if catalog
            .inodes
            .get(&source_inode)
            .expect("ASSERT: rename source entry references a live inode")
            .state
            .read()
            .expect("ASSERT: rename source inode lock poisoned")
            .metadata
            .is_immutable()
        {
            return Err(PosixError::PermissionDenied);
        }
        if let Some(replaced_inode) = replaced_inode
            && catalog
                .inodes
                .get(&replaced_inode)
                .expect("ASSERT: rename target entry references a live inode")
                .state
                .read()
                .expect("ASSERT: rename target inode lock poisoned")
                .metadata
                .is_immutable()
        {
            return Err(PosixError::PermissionDenied);
        }
        let reservation = self.reserve_commit_capacity(CommitCapacityClaim::new(
            MUTATION_METADATA_INCREMENT_BYTES_V1,
            0,
        ))?;
        if source_kind == FileKind::Directory {
            let result = rename_directory(
                &mut catalog,
                parent,
                &old_key,
                new_parent,
                new_key,
                source_inode,
                replaced_inode,
            );
            let reversed_create_claim = result
                .is_ok()
                .then(|| {
                    replaced_inode.and_then(|target_inode| {
                        catalog.active_create_metadata_bytes.remove(&target_inode)
                    })
                })
                .flatten();
            drop(catalog);
            if let Some(bytes) = reversed_create_claim
                && let Some(admission) = self.commit_capacity_admission.get()
            {
                admission.release_active_metadata(bytes);
            }
            if result.is_ok() {
                reservation.accept();
            }
            return result;
        }
        let next_namespace_sequence = next_root_mutation_sequence(&catalog)?;
        let target_object = replaced_inode.map(|target_inode| {
            catalog
                .inodes
                .get(&target_inode)
                .cloned()
                .expect("ASSERT: target name must reference a live inode")
        });
        let target_observer_order = target_object.as_ref().map(|target_object| {
            target_object
                .observer_order
                .lock()
                .expect("ASSERT: target observer-order lock poisoned")
        });
        let mut target_sequence = None;
        if let (Some(target_inode), Some(target_object)) = (replaced_inode, &target_object) {
            let mut target = target_object
                .state
                .write()
                .expect("ASSERT: target inode lock poisoned");
            if target.kind == FileKind::Directory {
                return Err(PosixError::IsDirectory);
            }
            let next_sequence = target
                .mutation_sequence
                .checked_add(1)
                .ok_or(PosixError::NoSpace)?;
            let final_link = target.link_count == 1;
            let dirty_before = target.data.active_resident_payload_bytes();
            if target.kind == FileKind::Regular {
                target.data.advance_mutation_sequence(next_sequence);
            }
            target.link_count -= 1;
            target.mutation_sequence = next_sequence;
            target.times.ctime = PosixTimestamp::now();
            if final_link && target.kind == FileKind::Regular {
                self.dirty_payload.replace(dirty_before, 0);
                target_sequence = Some((target_inode, next_sequence));
            }
        }
        let source_object = catalog
            .inodes
            .get(&source_inode)
            .cloned()
            .expect("ASSERT: rename source inode remains live");
        let mut source = source_object
            .state
            .write()
            .expect("ASSERT: source inode lock poisoned");
        source.times.ctime = PosixTimestamp::now();
        drop(source);
        let removed = catalog.entries.remove(&old_key);
        assert_eq!(
            removed,
            Some(source_inode),
            "ASSERT: validated rename source disappeared"
        );
        let previous = catalog.entries.insert(new_key, source_inode);
        assert_eq!(
            previous, replaced_inode,
            "ASSERT: rename target changed under the catalog write lock"
        );
        install_root_mutation_sequence(&catalog, next_namespace_sequence);
        let mut reversed_create_claim = None;
        if let Some(target_inode) = replaced_inode {
            let has_lookup = catalog
                .lookup_counts
                .get(&target_inode)
                .copied()
                .unwrap_or(0)
                != 0;
            let has_handle = catalog
                .handles
                .values()
                .any(|candidate| candidate.inode == target_inode);
            let is_orphan = target_object
                .as_ref()
                .expect("ASSERT: replaced target object exists")
                .state
                .read()
                .expect("ASSERT: target inode lock poisoned")
                .link_count
                == 0;
            if is_orphan {
                reversed_create_claim = catalog.active_create_metadata_bytes.remove(&target_inode);
            }
            if is_orphan && !has_lookup && !has_handle {
                let removed = catalog.inodes.remove(&target_inode);
                assert!(
                    removed.is_some(),
                    "ASSERT: replaced unpinned inode must remain live until rename"
                );
                self.release_removed_inode_quota(
                    target_inode,
                    target_object
                        .as_ref()
                        .expect("ASSERT: replaced target object exists"),
                );
            }
        }
        drop(catalog);
        if let Some(bytes) = reversed_create_claim
            && let Some(admission) = self.commit_capacity_admission.get()
        {
            admission.release_active_metadata(bytes);
        }
        reservation.accept();
        if let Some((target_inode, next_sequence)) = target_sequence {
            self.observe_truncate(target_inode, next_sequence, 0);
        }
        drop(target_observer_order);
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
            let parent_inode = if inode == ROOT_INODE {
                ROOT_INODE
            } else {
                catalog
                    .entries
                    .iter()
                    .find_map(|((parent, _), target)| (*target == inode).then_some(*parent))
                    .expect("ASSERT: every linked directory has one parent entry")
            };
            let parent_attr = catalog
                .inodes
                .get(&parent_inode)
                .expect("ASSERT: directory parent remains live")
                .state
                .read()
                .expect("ASSERT: directory parent lock poisoned")
                .attributes(parent_inode);
            result.push(DirectoryEntry {
                inode: parent_inode,
                kind: FileKind::Directory,
                attr: parent_attr,
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
            self.release_removed_inode_quota(inode, &object);
        }
        Reply::Empty
    }

    fn release_removed_inode_quota(&self, inode: InodeId, object: &Arc<Inode>) {
        let state = object.state.read().expect("ASSERT: inode lock poisoned");
        let allocated_bytes = if state.kind == FileKind::Regular {
            state.data.allocated_bytes()
        } else {
            0
        };
        drop(state);
        self.logical_quotas.remove_inode(inode, allocated_bytes);
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

    fn require_mutation_admission(&self) -> Result<RwLockReadGuard<'_, bool>, PosixError> {
        if !self.mutations_supported {
            return Err(PosixError::ReadOnly);
        }
        let admitted = self
            .mutations_admitted
            .read()
            .expect("ASSERT: mutation admission lock poisoned");
        if !*admitted {
            return Err(PosixError::Again);
        }
        Ok(admitted)
    }

    fn reserve_commit_capacity(
        &self,
        claim: CommitCapacityClaim,
    ) -> Result<CommitCapacityReservation<'_>, PosixError> {
        let admission = (!claim.is_empty())
            .then(|| self.commit_capacity_admission.get().map(AsRef::as_ref))
            .flatten();
        if let Some(admission) = admission {
            admission.try_reserve(claim)?;
        }
        Ok(CommitCapacityReservation {
            admission,
            claim,
            accepted: false,
        })
    }
}

fn validate_lock_request(lock: FileLock, allow_unlock: bool) -> Result<(), PosixError> {
    if lock.start > lock.end || (!allow_unlock && lock.kind == LockKind::Unlock) {
        return Err(PosixError::InvalidArgument);
    }
    Ok(())
}

fn first_lock_conflict(held: &[RecordLock], requested: RecordLock) -> Option<RecordLock> {
    held.iter().copied().find(|candidate| {
        candidate.owner != requested.owner
            && candidate.start <= requested.end
            && requested.start <= candidate.end
            && (candidate.kind == LockKind::Write || requested.kind == LockKind::Write)
    })
}

fn replace_owner_lock_range(
    held: &[RecordLock],
    requested: RecordLock,
) -> Result<Vec<RecordLock>, PosixError> {
    let capacity = held.len().checked_add(3).ok_or(PosixError::NoLocks)?;
    let mut updated = Vec::new();
    updated
        .try_reserve_exact(capacity)
        .map_err(|_| PosixError::OutOfMemory)?;
    for current in held.iter().copied() {
        let overlaps = current.start <= requested.end && requested.start <= current.end;
        if current.owner != requested.owner || !overlaps {
            updated.push(current);
            continue;
        }
        if current.start < requested.start {
            updated.push(RecordLock {
                end: requested.start - 1,
                ..current
            });
        }
        if current.end > requested.end {
            updated.push(RecordLock {
                start: requested.end + 1,
                ..current
            });
        }
    }
    if requested.kind != LockKind::Unlock {
        updated.push(requested);
    }
    canonicalize_record_locks(&mut updated);
    Ok(updated)
}

fn canonicalize_record_locks(locks: &mut Vec<RecordLock>) {
    locks.sort_unstable_by_key(|lock| {
        (
            lock.owner,
            lock_kind_order(lock.kind),
            lock.pid,
            lock.start,
            lock.end,
        )
    });
    let mut write = 0;
    for read in 0..locks.len() {
        let current = locks[read];
        if write != 0 {
            let previous = &mut locks[write - 1];
            if previous.owner == current.owner
                && previous.kind == current.kind
                && previous.pid == current.pid
                && current.start <= previous.end.saturating_add(1)
            {
                previous.end = previous.end.max(current.end);
                continue;
            }
        }
        locks[write] = current;
        write += 1;
    }
    locks.truncate(write);
    locks.sort_unstable_by_key(|lock| (lock.start, lock.end, lock.owner));
}

const fn lock_kind_order(kind: LockKind) -> u8 {
    match kind {
        LockKind::Read => 0,
        LockKind::Write => 1,
        LockKind::Unlock => 2,
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

fn ascii_suffix_eq(value: &[u8], suffix: &[u8]) -> bool {
    value
        .get(value.len().saturating_sub(suffix.len())..)
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(suffix))
}

fn small_file_policy<'a>(
    logical_size: u64,
    metadata: &InodeMetadata,
    mut names: impl Iterator<Item = &'a [u8]>,
) -> bool {
    small_file_policy_with_name_match(
        logical_size,
        metadata,
        names.any(|name| ascii_suffix_eq(name, b".xml") || ascii_suffix_eq(name, b".json")),
    )
}

fn small_file_policy_with_name_match(
    logical_size: u64,
    metadata: &InodeMetadata,
    policy_name_matches: bool,
) -> bool {
    if logical_size > SMALL_FILE_SPILL_BYTES_V1 {
        return false;
    }
    match metadata.get_xattr(SMALL_FILE_PLACEMENT_XATTR) {
        Ok(value) if value == b"metadata" => return true,
        Ok(value) if value == b"data" => return false,
        Ok(_) | Err(PosixError::NoData) => {}
        Err(_) => return false,
    }
    policy_name_matches
}

fn authorize_xattr_mutation(
    context: RequestContext,
    state: &InodeState,
    name: &[u8],
) -> Result<(), PosixError> {
    if (name.starts_with(b"trusted.") || name.starts_with(b"security.")) && context.uid != 0 {
        return Err(PosixError::PermissionDenied);
    }
    if context.uid != 0 && context.uid != state.uid {
        return Err(PosixError::PermissionDenied);
    }
    Ok(())
}

fn root_mutation_sequence(catalog: &Catalog) -> u64 {
    catalog
        .inodes
        .get(&ROOT_INODE)
        .expect("ASSERT: namespace root inode must remain live")
        .state
        .read()
        .expect("ASSERT: root inode lock poisoned")
        .mutation_sequence
}

fn next_root_mutation_sequence(catalog: &Catalog) -> Result<u64, PosixError> {
    root_mutation_sequence(catalog)
        .checked_add(1)
        .ok_or(PosixError::NoSpace)
}

fn next_namespace_and_inode_mutation_sequences(
    catalog: &Catalog,
    inode: InodeId,
    inode_sequence: u64,
) -> Result<(u64, u64), PosixError> {
    let next_inode_sequence = inode_sequence.checked_add(1).ok_or(PosixError::NoSpace)?;
    if inode == ROOT_INODE {
        return Ok((next_inode_sequence, next_inode_sequence));
    }
    Ok((next_root_mutation_sequence(catalog)?, next_inode_sequence))
}

fn install_root_mutation_sequence(catalog: &Catalog, sequence: u64) {
    let root = catalog
        .inodes
        .get(&ROOT_INODE)
        .expect("ASSERT: namespace root inode must remain live");
    let mut state = root
        .state
        .write()
        .expect("ASSERT: root inode lock poisoned");
    assert_eq!(
        state
            .mutation_sequence
            .checked_add(1)
            .expect("ASSERT: preflighted namespace sequence cannot overflow"),
        sequence,
        "ASSERT: namespace mutations must remain contiguous"
    );
    state.mutation_sequence = sequence;
}

fn verify_prepared_clone_partition(
    extents: &[PreparedCommitExtent],
    offset: u64,
    length: u64,
) -> Result<(), PosixError> {
    let expected_end = offset.checked_add(length).ok_or(PosixError::FileTooLarge)?;
    let mut cursor = offset;
    for extent in extents {
        if extent.offset() != cursor || extent.length() == 0 {
            return Err(PosixError::Io);
        }
        cursor = cursor.checked_add(extent.length()).ok_or(PosixError::Io)?;
        if cursor > expected_end {
            return Err(PosixError::Io);
        }
    }
    if cursor != expected_end {
        return Err(PosixError::Unsupported);
    }
    Ok(())
}

fn assert_commit_reachability(
    inodes: &[CommitInode],
    directories: &[CommitDirectory],
    symlinks: &[CommitSymlink],
    entries: &[CommitEntry],
) {
    let mut observed_links = BTreeMap::<InodeId, u32>::new();
    let directory_ids = directories
        .iter()
        .map(|directory| directory.inode)
        .collect::<BTreeSet<_>>();
    let file_ids = inodes
        .iter()
        .map(|inode| inode.inode)
        .collect::<BTreeSet<_>>();
    let symlink_ids = symlinks
        .iter()
        .map(|symlink| symlink.inode)
        .collect::<BTreeSet<_>>();
    let mut directory_children = BTreeMap::<InodeId, u32>::new();
    for entry in entries {
        assert!(
            entry.parent == ROOT_INODE || directory_ids.contains(&entry.parent),
            "ASSERT: committed entry parent must be a directory"
        );
        assert!(
            file_ids.contains(&entry.target)
                || directory_ids.contains(&entry.target)
                || symlink_ids.contains(&entry.target),
            "ASSERT: committed entry target must be reachable"
        );
        let count = observed_links.entry(entry.target).or_default();
        *count = count
            .checked_add(1)
            .expect("ASSERT: bounded link count cannot overflow");
        if directory_ids.contains(&entry.target) {
            let count = directory_children.entry(entry.parent).or_default();
            *count = count
                .checked_add(1)
                .expect("ASSERT: bounded directory link count cannot overflow");
        }
    }
    assert_eq!(
        observed_links.len(),
        inodes.len() + directories.len() + symlinks.len(),
        "ASSERT: commit inode and reachable target counts must agree"
    );
    for inode in inodes {
        assert_eq!(
            observed_links.get(&inode.inode).copied(),
            Some(inode.link_count),
            "ASSERT: commit link count must equal exact namespace reachability"
        );
    }
    for symlink in symlinks {
        assert_eq!(
            observed_links.get(&symlink.inode).copied(),
            Some(symlink.link_count),
            "ASSERT: symlink link count must equal exact namespace reachability"
        );
    }
    for directory in directories {
        assert_eq!(
            observed_links.get(&directory.inode).copied(),
            Some(1),
            "ASSERT: each committed directory has one parent entry"
        );
        assert_eq!(
            directory.link_count,
            2 + directory_children
                .get(&directory.inode)
                .copied()
                .unwrap_or(0),
            "ASSERT: committed directory link count includes child directories"
        );
    }
}

#[allow(clippy::too_many_lines)]
fn rename_directory(
    catalog: &mut Catalog,
    old_parent: InodeId,
    old_key: &(InodeId, Vec<u8>),
    new_parent: InodeId,
    new_key: (InodeId, Vec<u8>),
    source_inode: InodeId,
    replaced_inode: Option<InodeId>,
) -> Result<Reply, PosixError> {
    if directory_is_ancestor_or_self(catalog, source_inode, new_parent)? {
        return Err(PosixError::InvalidArgument);
    }
    let target_object = if let Some(target_inode) = replaced_inode {
        let object = catalog
            .inodes
            .get(&target_inode)
            .cloned()
            .expect("ASSERT: rename target entry references a live inode");
        let state = object
            .state
            .read()
            .expect("ASSERT: rename target lock poisoned");
        if state.kind != FileKind::Directory {
            return Err(PosixError::NotDirectory);
        }
        if !directory_is_empty(catalog, target_inode) {
            return Err(PosixError::NotEmpty);
        }
        assert_eq!(
            state.link_count, 2,
            "ASSERT: an empty linked directory has exactly dot and parent links"
        );
        drop(state);
        Some((target_inode, object))
    } else {
        None
    };

    let next_namespace_sequence = next_root_mutation_sequence(catalog)?;
    let mut link_deltas = BTreeMap::<InodeId, i32>::new();
    if old_parent != new_parent {
        *link_deltas.entry(old_parent).or_default() -= 1;
        *link_deltas.entry(new_parent).or_default() += 1;
    }
    if target_object.is_some() {
        *link_deltas.entry(new_parent).or_default() -= 1;
    }
    let mut touched = BTreeSet::from([old_parent, new_parent]);
    touched.insert(ROOT_INODE);
    let mut parent_updates = Vec::new();
    parent_updates
        .try_reserve_exact(touched.len())
        .map_err(|_| PosixError::OutOfMemory)?;
    for parent in touched.iter().copied() {
        let object = catalog
            .inodes
            .get(&parent)
            .cloned()
            .expect("ASSERT: validated rename parent exists");
        let state = object
            .state
            .read()
            .expect("ASSERT: rename parent lock poisoned");
        let links = state
            .link_count
            .checked_add_signed(link_deltas.get(&parent).copied().unwrap_or(0))
            .ok_or(PosixError::NoSpace)?;
        let sequence = if parent == ROOT_INODE {
            next_namespace_sequence
        } else {
            state
                .mutation_sequence
                .checked_add(1)
                .ok_or(PosixError::NoSpace)?
        };
        drop(state);
        parent_updates.push((object, links, sequence));
    }

    let target_sequence = target_object
        .as_ref()
        .map(|(_, object)| {
            object
                .state
                .read()
                .expect("ASSERT: rename target lock poisoned")
                .mutation_sequence
                .checked_add(1)
                .ok_or(PosixError::NoSpace)
        })
        .transpose()?;
    let removed = catalog.entries.remove(old_key);
    assert_eq!(
        removed,
        Some(source_inode),
        "ASSERT: validated directory rename source disappeared"
    );
    let previous = catalog.entries.insert(new_key, source_inode);
    assert_eq!(
        previous, replaced_inode,
        "ASSERT: directory rename target changed under catalog lock"
    );
    for (object, links, sequence) in parent_updates {
        let mut state = object
            .state
            .write()
            .expect("ASSERT: rename parent lock poisoned");
        state.link_count = links;
        state.mutation_sequence = sequence;
    }
    if let (Some((target_inode, object)), Some(sequence)) = (target_object, target_sequence) {
        let mut state = object
            .state
            .write()
            .expect("ASSERT: rename target lock poisoned");
        state.link_count = 0;
        state.mutation_sequence = sequence;
        drop(state);
        let has_lookup = catalog
            .lookup_counts
            .get(&target_inode)
            .copied()
            .unwrap_or(0)
            != 0;
        if !has_lookup {
            let removed = catalog.inodes.remove(&target_inode);
            assert!(removed.is_some(), "ASSERT: replaced directory disappeared");
        }
    }
    Ok(Reply::Empty)
}

fn directory_is_empty(catalog: &Catalog, inode: InodeId) -> bool {
    catalog
        .entries
        .range((inode, Vec::new())..)
        .next()
        .is_none_or(|((parent, _), _)| *parent != inode)
}

fn directory_is_ancestor_or_self(
    catalog: &Catalog,
    ancestor: InodeId,
    mut candidate: InodeId,
) -> Result<bool, PosixError> {
    loop {
        if candidate == ancestor {
            return Ok(true);
        }
        if candidate == ROOT_INODE {
            return Ok(false);
        }
        candidate = catalog
            .entries
            .iter()
            .find_map(|((parent, _), target)| (*target == candidate).then_some(*parent))
            .ok_or(PosixError::Io)?;
    }
}

fn reachable_inodes(
    entries: &BTreeMap<(InodeId, Vec<u8>), InodeId>,
    inodes: &BTreeMap<InodeId, Arc<Inode>>,
) -> Result<BTreeSet<InodeId>, PosixError> {
    let mut reachable = BTreeSet::new();
    let mut pending = vec![ROOT_INODE];
    while let Some(parent) = pending.pop() {
        for ((entry_parent, _), target) in entries {
            if *entry_parent != parent {
                continue;
            }
            let object = inodes.get(target).ok_or(PosixError::InvalidArgument)?;
            let is_directory = object
                .state
                .read()
                .expect("ASSERT: committed reachability inode lock poisoned")
                .kind
                == FileKind::Directory;
            if !reachable.insert(*target) && is_directory {
                return Err(PosixError::InvalidArgument);
            }
            if is_directory {
                pending.push(*target);
            }
        }
    }
    Ok(reachable)
}

fn open_existing_for_create(
    catalog: &mut Catalog,
    inode: InodeId,
    request: CreateRequest<'_>,
    dirty_payload: &DirtyPayloadTracker,
    logical_quotas: &LogicalQuotaTable,
    observer: Option<&dyn MutationObserver>,
) -> Result<Reply, PosixError> {
    let object = catalog
        .inodes
        .get(&inode)
        .cloned()
        .expect("ASSERT: directory entry must reference a live inode");
    let observer_order = object
        .observer_order
        .lock()
        .expect("ASSERT: inode observer-order lock poisoned");
    let mut state = object.state.write().expect("ASSERT: inode lock poisoned");
    if state.kind == FileKind::Directory {
        return Err(PosixError::IsDirectory);
    }
    if state.kind != FileKind::Regular {
        return Err(PosixError::InvalidArgument);
    }
    if (request.options.access != AccessMode::ReadOnly || request.truncate)
        && state.metadata.is_immutable()
    {
        return Err(PosixError::PermissionDenied);
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
    let logical_quota = next_sequence
        .map(|_| logical_quotas.reserve_change(inode, state.data.allocated_bytes(), 0))
        .transpose()?;
    let next_lookup = catalog
        .lookup_counts
        .get(&inode)
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(PosixError::NoSpace)?;
    let handle = allocate_handle(catalog)?;
    if let Some(sequence) = next_sequence {
        let dirty_before = state.data.active_resident_payload_bytes();
        state.data.truncate(0, sequence)?;
        let dirty_after = state.data.active_resident_payload_bytes();
        assert!(
            state.link_count > 0,
            "ASSERT: create-existing target must remain linked"
        );
        dirty_payload.replace(dirty_before, dirty_after);
        state.mutation_sequence = sequence;
        let now = PosixTimestamp::now();
        state.times.mtime = now;
        state.times.ctime = now;
    }
    let attr = state.attributes(inode);
    drop(state);
    if let Some(logical_quota) = logical_quota {
        logical_quota.accept();
    }
    if let Some(sequence) = next_sequence
        && let Some(observer) = observer
    {
        observer.accepted_truncate(inode, sequence, 0);
    }
    drop(observer_order);
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
    let next_namespace_sequence = next_root_mutation_sequence(catalog)?;
    let parent = catalog
        .inodes
        .get(&request.parent)
        .expect("ASSERT: validated parent directory exists")
        .state
        .read()
        .expect("ASSERT: parent directory lock poisoned");
    let (mode, metadata) =
        parent
            .metadata
            .for_child(FileKind::Regular, request.mode, request.umask)?;
    drop(parent);
    let inode = allocate_inode(catalog)?;
    let handle = allocate_handle(catalog)?;
    let object = Arc::new(Inode {
        observer_order: Mutex::new(()),
        kernel_data_cache_exposed: AtomicBool::new(false),
        state: RwLock::new(InodeState {
            kind: FileKind::Regular,
            mode,
            uid: request.context.uid,
            gid: request.context.gid,
            link_count: 1,
            mutation_sequence: 0,
            metadata: Arc::new(metadata),
            times: PosixTimes::now(),
            symlink_target: None,
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
    install_root_mutation_sequence(catalog, next_namespace_sequence);

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

fn relatime_due(times: PosixTimes, now: PosixTimestamp) -> bool {
    const RELATIME_INTERVAL_SECONDS: i64 = 24 * 60 * 60;
    times.atime <= times.mtime
        || times.atime <= times.ctime
        || now
            .seconds
            .checked_sub(RELATIME_INTERVAL_SECONDS)
            .is_some_and(|cutoff| times.atime.seconds <= cutoff)
}

fn validate_mutable_directory(catalog: &Catalog, inode: InodeId) -> Result<(), PosixError> {
    let object = catalog.inodes.get(&inode).ok_or(PosixError::NoEntry)?;
    let state = object.state.read().expect("ASSERT: inode lock poisoned");
    if state.kind != FileKind::Directory {
        return Err(PosixError::NotDirectory);
    }
    if state.metadata.is_immutable() {
        return Err(PosixError::PermissionDenied);
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
    use super::{Inode, MutationPayload, SparseData};

    #[test]
    fn inode_sequencers_start_on_separate_cache_lines() {
        assert_eq!(std::mem::align_of::<Inode>(), 64);
    }

    #[test]
    fn sequential_shared_payload_extents_remain_byte_exact() {
        let mut data = SparseData::default();
        let block = vec![0x5a; 1_024 * 1_024];

        for index in 0_u64..20 {
            data.write(
                index * 1_024 * 1_024,
                MutationPayload::try_copy_from_slice(&block).expect("allocate fixture payload"),
                index + 1,
            )
            .expect("sequential write must succeed");
        }

        assert_eq!(data.extents.len(), 20);
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

    #[test]
    fn mutation_payload_clones_and_prefix_slices_share_the_owned_bytes() {
        let payload = MutationPayload::try_copy_from_slice(b"shared-payload")
            .expect("allocate fixture payload");
        let clone = payload.clone();
        let prefix = payload
            .checked_slice(0, 6)
            .expect("prefix lies inside fixture payload");

        assert!(payload.starts_at_same_address(&clone));
        assert!(payload.starts_at_same_address(&prefix));
        assert_eq!(prefix.as_bytes(), b"shared");
    }

    #[test]
    fn large_overlap_survivor_reuses_the_original_backing() {
        let mut data = SparseData::default();
        let original = MutationPayload::try_copy_from_slice(&vec![0x41; 1_024 * 1_024])
            .expect("allocate original fixture payload");
        data.write(0, original.clone(), 1)
            .expect("install original fixture payload");
        data.write(
            512 * 1_024,
            MutationPayload::try_copy_from_slice(&vec![0x42; 512 * 1_024])
                .expect("allocate overwrite fixture payload"),
            2,
        )
        .expect("overwrite right half of fixture payload");

        let survivor = &data.extents[&0];
        assert_eq!(survivor.as_bytes(), vec![0x41; 512 * 1_024]);
        assert!(original.starts_at_same_address(&survivor.bytes));
    }

    #[test]
    fn partial_overwrite_does_not_retain_the_large_obsolete_backing() {
        let mut data = SparseData::default();
        let original = MutationPayload::try_copy_from_slice(&vec![0x41; 1_024 * 1_024])
            .expect("allocate original fixture payload");
        data.write(0, original.clone(), 1)
            .expect("install original fixture payload");
        data.write(
            1,
            MutationPayload::try_copy_from_slice(&vec![0x42; 1_024 * 1_024 - 2])
                .expect("allocate overwrite fixture payload"),
            2,
        )
        .expect("partially overwrite fixture payload");

        assert_eq!(data.extents[&0].as_bytes(), b"A");
        assert_eq!(data.extents[&(1_024 * 1_024 - 1)].as_bytes(), b"A");
        assert!(original.is_uniquely_owned());
    }
}
