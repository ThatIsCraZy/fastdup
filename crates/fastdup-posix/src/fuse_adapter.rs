use crate::{
    AccessMode, DirectoryEntry as NamespaceDirectoryEntry, Entry, FallocateMode, FileAttr,
    FileKind, FileLock, HandleId, InodeId, LockKind, Namespace, OpenOptions, Operation, PosixError,
    Reply, RequestContext, SeekKind,
};
use bytes::Bytes;
use fastdup_copy_metrics::{CopyClass, record_copy};
use fuse3::raw::reply::{
    DirectoryEntry, DirectoryEntryPlus, FileAttr as FuseFileAttr, ReplyAttr, ReplyCopyFileRange,
    ReplyCreated, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit, ReplyLSeek,
    ReplyLock as FuseReplyLock, ReplyOpen, ReplyStatFs, ReplyWrite,
};
use fuse3::raw::{Filesystem, OwnedRequestPayload, Request};
use fuse3::{Errno, FileType, MountOptions, SetAttr, Timestamp};
use futures_util::stream::{self, Stream};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::num::NonZeroU32;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Semaphore;

const MAXIMUM_WRITE_BYTES: u32 = 1_024 * 1_024;
const FOPEN_DIRECT_IO: u32 = 1;
const ZERO_TTL: Duration = Duration::ZERO;
const INTERNAL_CONTEXT: RequestContext = RequestContext {
    uid: 0,
    gid: 0,
    pid: 0,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatFsSnapshot {
    capacity_bytes: u64,
    free_bytes: u64,
    available_bytes: u64,
    files: u64,
    free_files: u64,
    block_size: u32,
    maximum_name_bytes: u32,
}

impl StatFsSnapshot {
    /// Creates one internally consistent filesystem-capacity observation.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero block size, free space above capacity,
    /// available space above free space, or free inode count above the total.
    pub const fn new(
        capacity_bytes: u64,
        free_bytes: u64,
        available_bytes: u64,
        files: u64,
        free_files: u64,
        block_size: u32,
        maximum_name_bytes: u32,
    ) -> Result<Self, StatFsSnapshotError> {
        if block_size == 0
            || free_bytes > capacity_bytes
            || available_bytes > free_bytes
            || free_files > files
        {
            return Err(StatFsSnapshotError);
        }
        Ok(Self {
            capacity_bytes,
            free_bytes,
            available_bytes,
            files,
            free_files,
            block_size,
            maximum_name_bytes,
        })
    }

    #[must_use]
    pub const fn capacity_bytes(self) -> u64 {
        self.capacity_bytes
    }

    #[must_use]
    pub const fn free_bytes(self) -> u64 {
        self.free_bytes
    }

    #[must_use]
    pub const fn available_bytes(self) -> u64 {
        self.available_bytes
    }

    #[must_use]
    pub const fn files(self) -> u64 {
        self.files
    }

    #[must_use]
    pub const fn free_files(self) -> u64 {
        self.free_files
    }

    #[must_use]
    pub const fn block_size(self) -> u32 {
        self.block_size
    }

    #[must_use]
    pub const fn maximum_name_bytes(self) -> u32 {
        self.maximum_name_bytes
    }

    fn reply(self) -> ReplyStatFs {
        let block_bytes = u64::from(self.block_size);
        ReplyStatFs {
            blocks: self.capacity_bytes / block_bytes,
            bfree: self.free_bytes / block_bytes,
            bavail: self.available_bytes / block_bytes,
            files: self.files,
            ffree: self.free_files,
            bsize: self.block_size,
            namelen: self.maximum_name_bytes,
            frsize: self.block_size,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatFsSnapshotError;

impl fmt::Display for StatFsSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("statfs values must describe one bounded filesystem")
    }
}

impl std::error::Error for StatFsSnapshotError {}

pub trait StatFsSource: fmt::Debug + Send + Sync {
    /// Returns the current cached capacity presented by the mounted filesystem.
    ///
    /// Implementations must not perform blocking I/O. A FUSE client may query
    /// capacity in its write path, so backing storage refresh belongs outside
    /// this method.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the backing capacity cannot be observed.
    fn snapshot(&self) -> std::io::Result<StatFsSnapshot>;
}

#[derive(Debug)]
struct TrackedDirectoryEntryPlus {
    inode: InodeId,
    reply: DirectoryEntryPlus,
}

#[derive(Debug)]
struct LookupTrackingStream {
    namespace: Arc<Namespace>,
    entries: std::vec::IntoIter<TrackedDirectoryEntryPlus>,
    pending: Option<InodeId>,
}

impl LookupTrackingStream {
    fn new(namespace: Arc<Namespace>, entries: Vec<NamespaceDirectoryEntry>) -> Self {
        let entries = entries
            .into_iter()
            .map(|entry| TrackedDirectoryEntryPlus {
                inode: entry.inode,
                reply: directory_entry_plus(entry),
            })
            .collect::<Vec<_>>()
            .into_iter();
        Self {
            namespace,
            entries,
            pending: None,
        }
    }
}

impl Stream for LookupTrackingStream {
    type Item = fuse3::Result<DirectoryEntryPlus>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // fuse3 asks for the next item only after it serialized the previous
        // one into the kernel reply. Until then the previous lookup pin is
        // pending and Drop must roll it back if the reply buffer was full.
        this.pending = None;
        let Some(entry) = this.entries.next() else {
            return Poll::Ready(None);
        };
        this.pending = Some(entry.inode);
        Poll::Ready(Some(Ok(entry.reply)))
    }
}

impl Drop for LookupTrackingStream {
    fn drop(&mut self) {
        if let Some(inode) = self.pending.take() {
            release_lookup_reference(&self.namespace, inode);
        }
        for entry in self.entries.by_ref() {
            release_lookup_reference(&self.namespace, entry.inode);
        }
    }
}

#[derive(Clone, Debug)]
pub struct FuseFilesystem {
    namespace: Arc<Namespace>,
    blocking_permits: Arc<Semaphore>,
    statfs_source: Option<Arc<dyn StatFsSource>>,
}

impl FuseFilesystem {
    #[must_use]
    pub fn new(namespace: Arc<Namespace>) -> Self {
        let workers = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        Self {
            namespace,
            blocking_permits: Arc::new(Semaphore::new(workers)),
            statfs_source: None,
        }
    }

    #[must_use]
    pub fn with_statfs_source(mut self, source: Arc<dyn StatFsSource>) -> Self {
        self.statfs_source = Some(source);
        self
    }

    async fn run_blocking<R, F>(&self, work: F) -> R
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let permit = Arc::clone(&self.blocking_permits)
            .acquire_owned()
            .await
            .expect("ASSERT: the FUSE blocking executor is never closed while mounted");
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .expect("ASSERT: bounded FUSE blocking work must not panic")
    }

    async fn write_payload(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        offset: u64,
        payload: crate::MutationPayload,
        write_flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        if write_flags & fuse3::raw::flags::FUSE_WRITE_CACHE != 0 {
            return Err(libc::EIO.into());
        }
        let inode = inode_from_raw(inode)?;
        let handle = handle_from_raw(handle)?;
        let request = context(request);
        let reply = loop {
            self.namespace
                .wait_for_mutation_admission()
                .await
                .map_err(errno)?;
            let namespace = Arc::clone(&self.namespace);
            let payload = payload.clone();
            match self
                .run_blocking(move || {
                    namespace.dispatch_owned_write(request, inode, handle, offset, payload)
                })
                .await
            {
                Err(PosixError::Again) => {}
                result => break result.map_err(errno)?,
            }
        };
        let (bytes, _) = expect_written(&reply);
        Ok(ReplyWrite { written: bytes })
    }
}

#[must_use]
pub fn volatile_mount_options() -> MountOptions {
    let mut options = MountOptions::default();
    options
        .fs_name("fastdup")
        .default_permissions(true)
        .write_back(false);
    #[cfg(target_os = "linux")]
    // fuse3 serializes this integer as the textual octal mount option.
    options.rootmode(40_755);
    options
}

async fn dispatch_mutation_with_backpressure<R>(
    namespace: &Namespace,
    mut mutation: impl FnMut() -> Result<R, PosixError>,
) -> Result<R, PosixError> {
    loop {
        namespace.wait_for_mutation_admission().await?;
        match mutation() {
            Err(PosixError::Again) => {}
            result => return result,
        }
    }
}

impl Filesystem for FuseFilesystem {
    async fn init(&self, _request: Request) -> fuse3::Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(MAXIMUM_WRITE_BYTES)
                .expect("ASSERT: maximum FUSE write size must be nonzero"),
        })
    }

    async fn destroy(&self, _request: Request) {}

    async fn lookup(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
    ) -> fuse3::Result<ReplyEntry> {
        let parent = inode_from_raw(parent)?;
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::Lookup {
                    parent,
                    name: name.as_bytes(),
                },
            )
            .map_err(errno)?;
        Ok(reply_entry(expect_entry(reply).attr))
    }

    async fn forget(&self, request: Request, inode: u64, lookup_count: u64) {
        let Some(inode) = InodeId::new(inode) else {
            return;
        };
        let reply = self.namespace.dispatch(
            context(request),
            Operation::Forget {
                inode,
                lookup_count,
            },
        );
        assert_eq!(
            reply,
            Ok(Reply::Empty),
            "ASSERT: forget must be an infallible liveness release"
        );
    }

    async fn getattr(
        &self,
        request: Request,
        inode: u64,
        _handle: Option<u64>,
        _flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        let inode = inode_from_raw(inode)?;
        let reply = self
            .namespace
            .dispatch(context(request), Operation::GetAttr { inode })
            .map_err(errno)?;
        Ok(ReplyAttr {
            ttl: ZERO_TTL,
            attr: fuse_attr(expect_attr(&reply)),
        })
    }

    async fn setattr(
        &self,
        request: Request,
        inode: u64,
        handle: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        if set_attr.mode.is_some()
            || set_attr.uid.is_some()
            || set_attr.gid.is_some()
            || set_attr.atime.is_some()
            || set_attr.mtime.is_some()
            || set_attr.ctime.is_some()
        {
            return Err(libc::EOPNOTSUPP.into());
        }
        let inode = inode_from_raw(inode)?;
        let Some(length) = set_attr.size else {
            if set_attr.lock_owner.is_some() {
                return Err(libc::EOPNOTSUPP.into());
            }
            let reply = self
                .namespace
                .dispatch(context(request), Operation::GetAttr { inode })
                .map_err(errno)?;
            return Ok(ReplyAttr {
                ttl: ZERO_TTL,
                attr: fuse_attr(expect_attr(&reply)),
            });
        };
        let handle = handle.map(handle_from_raw).transpose()?;
        let request = context(request);
        let reply = dispatch_mutation_with_backpressure(&self.namespace, || {
            self.namespace.dispatch(
                request,
                Operation::SetLength {
                    inode,
                    handle,
                    length,
                },
            )
        })
        .await
        .map_err(errno)?;
        Ok(ReplyAttr {
            ttl: ZERO_TTL,
            attr: fuse_attr(expect_attr(&reply)),
        })
    }

    async fn mkdir(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        let parent = inode_from_raw(parent)?;
        let mode = u16::try_from((mode & !umask) & 0o7777)
            .expect("ASSERT: masked directory mode must fit in u16");
        let request = context(request);
        let reply = dispatch_mutation_with_backpressure(&self.namespace, || {
            self.namespace.dispatch(
                request,
                Operation::Mkdir {
                    parent,
                    name: name.as_bytes(),
                    mode,
                },
            )
        })
        .await
        .map_err(errno)?;
        Ok(reply_entry(expect_entry(reply).attr))
    }

    async fn unlink(&self, request: Request, parent: u64, name: &OsStr) -> fuse3::Result<()> {
        let parent = inode_from_raw(parent)?;
        let request = context(request);
        let reply = dispatch_mutation_with_backpressure(&self.namespace, || {
            self.namespace.dispatch(
                request,
                Operation::Unlink {
                    parent,
                    name: name.as_bytes(),
                },
            )
        })
        .await
        .map_err(errno)?;
        expect_empty(&reply);
        Ok(())
    }

    async fn rmdir(&self, request: Request, parent: u64, name: &OsStr) -> fuse3::Result<()> {
        let parent = inode_from_raw(parent)?;
        let request = context(request);
        let reply = dispatch_mutation_with_backpressure(&self.namespace, || {
            self.namespace.dispatch(
                request,
                Operation::Rmdir {
                    parent,
                    name: name.as_bytes(),
                },
            )
        })
        .await
        .map_err(errno)?;
        expect_empty(&reply);
        Ok(())
    }

    async fn rename(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> fuse3::Result<()> {
        self.rename_with_flags(request, parent, name, new_parent, new_name, 0)
            .await
    }

    async fn open(&self, request: Request, inode: u64, flags: u32) -> fuse3::Result<ReplyOpen> {
        let inode = inode_from_raw(inode)?;
        let options = open_options(flags)?;
        let truncate =
            flags & u32::try_from(libc::O_TRUNC).expect("ASSERT: O_TRUNC must be nonnegative") != 0;
        let request = context(request);
        let operation = || {
            self.namespace.dispatch(
                request,
                Operation::Open {
                    inode,
                    options,
                    truncate,
                },
            )
        };
        let reply = if options.access == AccessMode::ReadOnly && !truncate {
            operation()
        } else {
            dispatch_mutation_with_backpressure(&self.namespace, operation).await
        }
        .map_err(errno)?;
        let handle = expect_opened(&reply);
        Ok(ReplyOpen {
            fh: handle.get(),
            flags: FOPEN_DIRECT_IO,
        })
    }

    async fn read(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        let namespace = Arc::clone(&self.namespace);
        let request = context(request);
        let inode = inode_from_raw(inode)?;
        let handle = handle_from_raw(handle)?;
        let reply = self
            .run_blocking(move || {
                namespace.dispatch(
                    request,
                    Operation::Read {
                        inode,
                        handle,
                        offset,
                        length: size,
                    },
                )
            })
            .await
            .map_err(errno)?;
        Ok(ReplyData {
            data: Bytes::from(expect_data(reply)),
        })
    }

    async fn write(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        _flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        record_copy(CopyClass::FuseRequestAdaptation, data.len());
        let payload = crate::MutationPayload::try_copy_from_slice(data).map_err(errno)?;
        self.write_payload(request, inode, handle, offset, payload, write_flags)
            .await
    }

    async fn write_owned(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        offset: u64,
        data: Vec<u8>,
        write_flags: u32,
        _flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        let payload = crate::MutationPayload::from_owned_bytes(data);
        self.write_payload(request, inode, handle, offset, payload, write_flags)
            .await
    }

    async fn write_owned_request(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        offset: u64,
        data: OwnedRequestPayload,
        write_flags: u32,
        _flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        let (bytes, backing_bytes) = data.into_parts();
        let payload = crate::MutationPayload::from_shared_bytes(bytes, backing_bytes);
        self.write_payload(request, inode, handle, offset, payload, write_flags)
            .await
    }

    async fn statfs(&self, _request: Request, _inode: u64) -> fuse3::Result<ReplyStatFs> {
        let source = self
            .statfs_source
            .as_ref()
            .ok_or_else(|| Errno::from(libc::EOPNOTSUPP))?;
        let snapshot = source.snapshot().map_err(Errno::from)?;
        Ok(snapshot.reply())
    }

    async fn release(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        _flags: u32,
        lock_owner: u64,
        _flush: bool,
    ) -> fuse3::Result<()> {
        let namespace = Arc::clone(&self.namespace);
        let request = context(request);
        let inode = inode_from_raw(inode)?;
        let handle = handle_from_raw(handle)?;
        let reply = self
            .run_blocking(move || {
                namespace.dispatch(
                    request,
                    Operation::UnlockOwner {
                        inode,
                        handle,
                        owner: lock_owner,
                    },
                )?;
                namespace.dispatch(request, Operation::Release { inode, handle })
            })
            .await
            .map_err(errno)?;
        expect_empty(&reply);
        Ok(())
    }

    async fn fsync(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        data_only: bool,
    ) -> fuse3::Result<()> {
        self.sync(request, inode, handle, data_only).await
    }

    async fn flush(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        lock_owner: u64,
    ) -> fuse3::Result<()> {
        let namespace = Arc::clone(&self.namespace);
        let request = context(request);
        let inode = inode_from_raw(inode)?;
        let handle = handle_from_raw(handle)?;
        self.run_blocking(move || {
            namespace.dispatch(
                request,
                Operation::UnlockOwner {
                    inode,
                    handle,
                    owner: lock_owner,
                },
            )?;
            namespace.dispatch(
                request,
                Operation::Sync {
                    inode,
                    handle,
                    data_only: false,
                },
            )
        })
        .await
        .map_err(errno)
        .map(|reply| expect_empty(&reply))
    }

    async fn getlk(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
    ) -> fuse3::Result<FuseReplyLock> {
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::GetLock {
                    inode: inode_from_raw(inode)?,
                    handle: handle_from_raw(handle)?,
                    owner: lock_owner,
                    lock: FileLock {
                        start,
                        end,
                        kind: lock_kind(r#type, false)?,
                        pid,
                    },
                },
            )
            .map_err(errno)?;
        let Reply::Lock(lock) = reply else {
            panic!("ASSERT: namespace get-lock returned a non-lock reply");
        };
        Ok(FuseReplyLock {
            start: lock.start,
            end: lock.end,
            r#type: fuse_lock_kind(lock.kind),
            pid: lock.pid,
        })
    }

    async fn setlk(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
        block: bool,
    ) -> fuse3::Result<()> {
        let request = context(request);
        let inode = inode_from_raw(inode)?;
        let handle = handle_from_raw(handle)?;
        let lock = FileLock {
            start,
            end,
            kind: lock_kind(r#type, true)?,
            pid,
        };
        loop {
            let observed = self.namespace.lock_sequence();
            match self.namespace.dispatch(
                request,
                Operation::SetLock {
                    inode,
                    handle,
                    owner: lock_owner,
                    lock,
                },
            ) {
                Err(PosixError::Again) if block => {
                    self.namespace.wait_for_lock_change(observed).await;
                }
                Err(error) => return Err(errno(error)),
                Ok(reply) => {
                    expect_empty(&reply);
                    return Ok(());
                }
            }
        }
    }

    async fn opendir(&self, request: Request, inode: u64, _flags: u32) -> fuse3::Result<ReplyOpen> {
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::GetAttr {
                    inode: inode_from_raw(inode)?,
                },
            )
            .map_err(errno)?;
        if expect_attr(&reply).kind != FileKind::Directory {
            return Err(libc::ENOTDIR.into());
        }
        Ok(ReplyOpen { fh: 0, flags: 0 })
    }

    async fn readdir(
        &self,
        request: Request,
        inode: u64,
        _handle: u64,
        offset: i64,
    ) -> fuse3::Result<ReplyDirectory<impl Stream<Item = fuse3::Result<DirectoryEntry>> + Send + '_>>
    {
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::ReadDirectory {
                    inode: inode_from_raw(inode)?,
                    offset,
                    acquire_lookup: false,
                },
            )
            .map_err(errno)?;
        let entries = expect_directory(reply)
            .into_iter()
            .map(|entry| Ok(directory_entry(entry)))
            .collect::<Vec<_>>();
        Ok(ReplyDirectory {
            entries: stream::iter(entries),
        })
    }

    async fn readdirplus(
        &self,
        request: Request,
        inode: u64,
        _handle: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> fuse3::Result<
        ReplyDirectoryPlus<impl Stream<Item = fuse3::Result<DirectoryEntryPlus>> + Send + '_>,
    > {
        let offset = i64::try_from(offset).map_err(|_| Errno::from(libc::EINVAL))?;
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::ReadDirectory {
                    inode: inode_from_raw(inode)?,
                    offset,
                    acquire_lookup: true,
                },
            )
            .map_err(errno)?;
        Ok(ReplyDirectoryPlus {
            entries: LookupTrackingStream::new(self.namespace.clone(), expect_directory(reply)),
        })
    }

    async fn rename2(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
    ) -> fuse3::Result<()> {
        self.rename_with_flags(request, parent, name, new_parent, new_name, flags)
            .await
    }

    async fn releasedir(
        &self,
        _request: Request,
        _inode: u64,
        _handle: u64,
        _flags: u32,
    ) -> fuse3::Result<()> {
        Ok(())
    }

    async fn fsyncdir(
        &self,
        request: Request,
        inode: u64,
        _handle: u64,
        _data_only: bool,
    ) -> fuse3::Result<()> {
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::GetAttr {
                    inode: inode_from_raw(inode)?,
                },
            )
            .map_err(errno)?;
        if expect_attr(&reply).kind != FileKind::Directory {
            return Err(libc::ENOTDIR.into());
        }
        Ok(())
    }

    async fn access(&self, request: Request, inode: u64, _mask: u32) -> fuse3::Result<()> {
        self.namespace
            .dispatch(
                context(request),
                Operation::GetAttr {
                    inode: inode_from_raw(inode)?,
                },
            )
            .map_err(errno)?;
        Ok(())
    }

    async fn create(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        let parent = inode_from_raw(parent)?;
        let options = open_options(flags)?;
        let mode = u16::try_from(mode & 0o7777).expect("ASSERT: masked mode must fit in u16");
        let exclusive =
            flags & u32::try_from(libc::O_EXCL).expect("ASSERT: O_EXCL is nonnegative") != 0;
        let truncate =
            flags & u32::try_from(libc::O_TRUNC).expect("ASSERT: O_TRUNC must be nonnegative") != 0;
        let request = context(request);
        let reply = dispatch_mutation_with_backpressure(&self.namespace, || {
            self.namespace.dispatch(
                request,
                Operation::Create {
                    parent,
                    name: name.as_bytes(),
                    mode,
                    options,
                    exclusive,
                    truncate,
                },
            )
        })
        .await
        .map_err(errno)?;
        let (entry, handle) = expect_created(reply);

        Ok(ReplyCreated {
            ttl: ZERO_TTL,
            attr: fuse_attr(entry.attr),
            generation: 1,
            fh: handle.get(),
            flags: FOPEN_DIRECT_IO,
        })
    }

    async fn fallocate(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> fuse3::Result<()> {
        let inode = inode_from_raw(inode)?;
        let handle = handle_from_raw(handle)?;
        let mode = fallocate_mode(mode)?;
        let request = context(request);
        loop {
            self.namespace
                .wait_for_mutation_admission()
                .await
                .map_err(errno)?;
            let namespace = Arc::clone(&self.namespace);
            match self
                .run_blocking(move || {
                    namespace.dispatch(
                        request,
                        Operation::Fallocate {
                            inode,
                            handle,
                            offset,
                            length,
                            mode,
                        },
                    )
                })
                .await
            {
                Err(PosixError::Again) => {}
                result => {
                    let reply = result.map_err(errno)?;
                    let Reply::Attr(_) = reply else {
                        panic!("ASSERT: namespace fallocate returned a non-attr reply");
                    };
                    return Ok(());
                }
            }
        }
    }

    async fn lseek(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        offset: u64,
        whence: u32,
    ) -> fuse3::Result<ReplyLSeek> {
        let kind = if whence
            == u32::try_from(libc::SEEK_DATA).expect("ASSERT: SEEK_DATA is nonnegative")
        {
            SeekKind::Data
        } else if whence
            == u32::try_from(libc::SEEK_HOLE).expect("ASSERT: SEEK_HOLE is nonnegative")
        {
            SeekKind::Hole
        } else {
            return Err(libc::EINVAL.into());
        };
        let namespace = Arc::clone(&self.namespace);
        let reply = self
            .run_blocking(move || {
                namespace.dispatch(
                    context(request),
                    Operation::Seek {
                        inode: inode_from_raw(inode).map_err(|_| PosixError::InvalidArgument)?,
                        handle: handle_from_raw(handle).map_err(|_| PosixError::BadHandle)?,
                        offset,
                        kind,
                    },
                )
            })
            .await
            .map_err(errno)?;
        let Reply::Offset(offset) = reply else {
            panic!("ASSERT: namespace seek returned a non-offset reply");
        };
        Ok(ReplyLSeek { offset })
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_file_range(
        &self,
        request: Request,
        inode: u64,
        source_handle: u64,
        source_offset: u64,
        target_inode: u64,
        target_handle: u64,
        target_offset: u64,
        length: u64,
        flags: u64,
    ) -> fuse3::Result<ReplyCopyFileRange> {
        if flags != 0 {
            return Err(libc::EINVAL.into());
        }
        let source_inode = inode_from_raw(inode)?;
        let source_handle = handle_from_raw(source_handle)?;
        let target_inode = inode_from_raw(target_inode)?;
        let target_handle = handle_from_raw(target_handle)?;
        let request = context(request);
        let reply = loop {
            self.namespace
                .wait_for_mutation_admission()
                .await
                .map_err(errno)?;
            let namespace = Arc::clone(&self.namespace);
            match self
                .run_blocking(move || {
                    namespace.dispatch(
                        request,
                        Operation::CloneRange {
                            source_inode,
                            source_handle,
                            source_offset,
                            target_inode,
                            target_handle,
                            target_offset,
                            length,
                        },
                    )
                })
                .await
            {
                Err(PosixError::Again) => {}
                result => break result.map_err(errno)?,
            }
        };
        let Reply::Cloned { bytes, .. } = reply else {
            panic!("ASSERT: namespace clone returned a non-cloned reply");
        };
        Ok(ReplyCopyFileRange { copied: bytes })
    }
}

impl FuseFilesystem {
    async fn rename_with_flags(
        &self,
        request: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        flags: u32,
    ) -> fuse3::Result<()> {
        let no_replace = flags == libc::RENAME_NOREPLACE;
        if flags != 0 && !no_replace {
            return Err(libc::EOPNOTSUPP.into());
        }
        let request = context(request);
        let reply = dispatch_mutation_with_backpressure(&self.namespace, || {
            self.namespace.dispatch(
                request,
                Operation::Rename {
                    parent: inode_from_raw(parent).map_err(|_| PosixError::InvalidArgument)?,
                    name: name.as_bytes(),
                    new_parent: inode_from_raw(new_parent)
                        .map_err(|_| PosixError::InvalidArgument)?,
                    new_name: new_name.as_bytes(),
                    no_replace,
                },
            )
        })
        .await
        .map_err(errno)?;
        expect_empty(&reply);
        Ok(())
    }

    async fn sync(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        data_only: bool,
    ) -> fuse3::Result<()> {
        let namespace = Arc::clone(&self.namespace);
        let request = context(request);
        let inode = inode_from_raw(inode)?;
        let handle = handle_from_raw(handle)?;
        let reply = self
            .run_blocking(move || {
                namespace.dispatch(
                    request,
                    Operation::Sync {
                        inode,
                        handle,
                        data_only,
                    },
                )
            })
            .await
            .map_err(errno)?;
        expect_empty(&reply);
        Ok(())
    }
}

const fn context(request: Request) -> RequestContext {
    RequestContext {
        uid: request.uid,
        gid: request.gid,
        pid: request.pid,
    }
}

fn inode_from_raw(raw: u64) -> fuse3::Result<InodeId> {
    InodeId::new(raw).ok_or_else(|| libc::EINVAL.into())
}

fn handle_from_raw(raw: u64) -> fuse3::Result<HandleId> {
    HandleId::new(raw).ok_or_else(|| libc::EBADF.into())
}

fn open_options(flags: u32) -> fuse3::Result<OpenOptions> {
    let access_mask =
        u32::try_from(libc::O_ACCMODE).expect("ASSERT: O_ACCMODE must be nonnegative");
    let access = match flags & access_mask {
        value
            if value == u32::try_from(libc::O_RDONLY).expect("ASSERT: O_RDONLY is nonnegative") =>
        {
            AccessMode::ReadOnly
        }
        value
            if value == u32::try_from(libc::O_WRONLY).expect("ASSERT: O_WRONLY is nonnegative") =>
        {
            AccessMode::WriteOnly
        }
        value if value == u32::try_from(libc::O_RDWR).expect("ASSERT: O_RDWR is nonnegative") => {
            AccessMode::ReadWrite
        }
        _ => return Err(libc::EINVAL.into()),
    };
    Ok(OpenOptions {
        access,
        append: flags
            & u32::try_from(libc::O_APPEND).expect("ASSERT: O_APPEND must be nonnegative")
            != 0,
    })
}

fn lock_kind(value: u32, allow_unlock: bool) -> fuse3::Result<LockKind> {
    if value == u32::try_from(libc::F_RDLCK).expect("ASSERT: F_RDLCK is nonnegative") {
        return Ok(LockKind::Read);
    }
    if value == u32::try_from(libc::F_WRLCK).expect("ASSERT: F_WRLCK is nonnegative") {
        return Ok(LockKind::Write);
    }
    if allow_unlock
        && value == u32::try_from(libc::F_UNLCK).expect("ASSERT: F_UNLCK is nonnegative")
    {
        return Ok(LockKind::Unlock);
    }
    Err(libc::EINVAL.into())
}

fn fallocate_mode(mode: u32) -> fuse3::Result<FallocateMode> {
    let keep = u32::try_from(libc::FALLOC_FL_KEEP_SIZE)
        .expect("ASSERT: FALLOC_FL_KEEP_SIZE is nonnegative");
    let punch = u32::try_from(libc::FALLOC_FL_PUNCH_HOLE)
        .expect("ASSERT: FALLOC_FL_PUNCH_HOLE is nonnegative");
    let zero = u32::try_from(libc::FALLOC_FL_ZERO_RANGE)
        .expect("ASSERT: FALLOC_FL_ZERO_RANGE is nonnegative");
    let collapse = u32::try_from(libc::FALLOC_FL_COLLAPSE_RANGE)
        .expect("ASSERT: FALLOC_FL_COLLAPSE_RANGE is nonnegative");
    let insert = u32::try_from(libc::FALLOC_FL_INSERT_RANGE)
        .expect("ASSERT: FALLOC_FL_INSERT_RANGE is nonnegative");
    match mode {
        0 => Ok(FallocateMode::Allocate { keep_size: false }),
        value if value == keep => Ok(FallocateMode::Allocate { keep_size: true }),
        value if value == punch | keep => Ok(FallocateMode::PunchHole),
        value if value == zero => Ok(FallocateMode::ZeroRange { keep_size: false }),
        value if value == zero | keep => Ok(FallocateMode::ZeroRange { keep_size: true }),
        value if value == collapse => Ok(FallocateMode::CollapseRange),
        value if value == insert => Ok(FallocateMode::InsertRange),
        value if value & (punch | zero | collapse | insert) != 0 => Err(libc::EINVAL.into()),
        _ => Err(libc::EOPNOTSUPP.into()),
    }
}

fn fuse_lock_kind(kind: LockKind) -> u32 {
    match kind {
        LockKind::Read => u32::try_from(libc::F_RDLCK).expect("ASSERT: F_RDLCK is nonnegative"),
        LockKind::Write => u32::try_from(libc::F_WRLCK).expect("ASSERT: F_WRLCK is nonnegative"),
        LockKind::Unlock => u32::try_from(libc::F_UNLCK).expect("ASSERT: F_UNLCK is nonnegative"),
    }
}

fn errno(error: PosixError) -> Errno {
    match error {
        PosixError::NoEntry => Errno::new_not_exist(),
        PosixError::Exists => Errno::new_exist(),
        PosixError::NotDirectory => Errno::new_is_not_dir(),
        PosixError::IsDirectory => Errno::new_is_dir(),
        PosixError::NotEmpty => libc::ENOTEMPTY.into(),
        PosixError::InvalidName | PosixError::InvalidArgument => libc::EINVAL.into(),
        PosixError::NameTooLong => libc::ENAMETOOLONG.into(),
        PosixError::BadHandle => libc::EBADF.into(),
        PosixError::FileTooLarge => libc::EFBIG.into(),
        PosixError::NoSpace => libc::ENOSPC.into(),
        PosixError::OutOfMemory => libc::ENOMEM.into(),
        PosixError::NoLocks => libc::ENOLCK.into(),
        PosixError::NoSuchAddress => libc::ENXIO.into(),
        PosixError::Unsupported => libc::EOPNOTSUPP.into(),
        PosixError::Io => libc::EIO.into(),
        PosixError::ReadOnly => libc::EROFS.into(),
        PosixError::Again => libc::EAGAIN.into(),
    }
}

fn reply_entry(attr: FileAttr) -> ReplyEntry {
    ReplyEntry {
        ttl: ZERO_TTL,
        attr: fuse_attr(attr),
        generation: 1,
    }
}

fn fuse_attr(attr: FileAttr) -> FuseFileAttr {
    FuseFileAttr {
        ino: attr.inode.get(),
        size: attr.size,
        blocks: attr.allocated_bytes.saturating_add(511) / 512,
        atime: Timestamp::new(0, 0),
        mtime: Timestamp::new(0, 0),
        ctime: Timestamp::new(0, 0),
        kind: match attr.kind {
            FileKind::Directory => FileType::Directory,
            FileKind::Regular => FileType::RegularFile,
        },
        perm: attr.mode,
        nlink: attr.link_count,
        uid: attr.uid,
        gid: attr.gid,
        rdev: 0,
        blksize: 4_096,
    }
}

fn directory_entry(entry: NamespaceDirectoryEntry) -> DirectoryEntry {
    DirectoryEntry {
        inode: entry.inode.get(),
        kind: match entry.kind {
            FileKind::Directory => FileType::Directory,
            FileKind::Regular => FileType::RegularFile,
        },
        name: OsString::from_vec(entry.name),
        offset: entry.next_offset,
    }
}

fn directory_entry_plus(entry: NamespaceDirectoryEntry) -> DirectoryEntryPlus {
    DirectoryEntryPlus {
        inode: entry.inode.get(),
        generation: 1,
        kind: match entry.kind {
            FileKind::Directory => FileType::Directory,
            FileKind::Regular => FileType::RegularFile,
        },
        name: OsString::from_vec(entry.name),
        offset: entry.next_offset,
        attr: fuse_attr(entry.attr),
        entry_ttl: ZERO_TTL,
        attr_ttl: ZERO_TTL,
    }
}

fn expect_entry(reply: Reply) -> Entry {
    let Reply::Entry(entry) = reply else {
        panic!("ASSERT: namespace lookup returned a non-entry reply");
    };
    entry
}

fn expect_attr(reply: &Reply) -> FileAttr {
    let Reply::Attr(attr) = *reply else {
        panic!("ASSERT: namespace getattr returned a non-attr reply");
    };
    attr
}

fn expect_created(reply: Reply) -> (Entry, HandleId) {
    let Reply::Created { entry, handle } = reply else {
        panic!("ASSERT: namespace create returned a non-created reply");
    };
    (entry, handle)
}

fn expect_opened(reply: &Reply) -> HandleId {
    let Reply::Opened(handle) = *reply else {
        panic!("ASSERT: namespace open returned a non-opened reply");
    };
    handle
}

fn expect_data(reply: Reply) -> Vec<u8> {
    let Reply::Data(data) = reply else {
        panic!("ASSERT: namespace read returned a non-data reply");
    };
    data
}

fn expect_written(reply: &Reply) -> (u32, u64) {
    let Reply::Written {
        bytes,
        mutation_sequence,
    } = *reply
    else {
        panic!("ASSERT: namespace write returned a non-written reply");
    };
    (bytes, mutation_sequence)
}

fn expect_directory(reply: Reply) -> Vec<NamespaceDirectoryEntry> {
    let Reply::Directory(entries) = reply else {
        panic!("ASSERT: namespace readdir returned a non-directory reply");
    };
    entries
}

fn expect_empty(reply: &Reply) {
    assert_eq!(
        *reply,
        Reply::Empty,
        "ASSERT: namespace operation returned a non-empty reply"
    );
}

fn release_lookup_reference(namespace: &Namespace, inode: InodeId) {
    let reply = namespace.dispatch(
        INTERNAL_CONTEXT,
        Operation::Forget {
            inode,
            lookup_count: 1,
        },
    );
    assert_eq!(
        reply,
        Ok(Reply::Empty),
        "ASSERT: rollback of an un-emitted readdirplus lookup pin must be infallible"
    );
}

#[cfg(test)]
mod tests {
    use super::{
        INTERNAL_CONTEXT, LookupTrackingStream, StatFsSnapshot,
        dispatch_mutation_with_backpressure, fallocate_mode,
    };
    use crate::{
        FallocateMode, Namespace, NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE,
        Reply,
    };
    use futures_util::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn dropped_readdirplus_item_rolls_back_its_lookup_pin() {
        let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
        let Reply::Created { entry, handle } = namespace
            .dispatch(
                INTERNAL_CONTEXT,
                Operation::Create {
                    parent: ROOT_INODE,
                    name: b"pending",
                    mode: 0o600,
                    options: OpenOptions::READ_WRITE,
                    exclusive: true,
                    truncate: false,
                },
            )
            .expect("create must succeed")
        else {
            panic!("ASSERT: create returned the wrong reply variant");
        };
        assert_eq!(
            namespace.dispatch(
                INTERNAL_CONTEXT,
                Operation::Release {
                    inode: entry.attr.inode,
                    handle,
                },
            ),
            Ok(Reply::Empty)
        );

        let Reply::Directory(entries) = namespace
            .dispatch(
                INTERNAL_CONTEXT,
                Operation::ReadDirectory {
                    inode: ROOT_INODE,
                    offset: 2,
                    acquire_lookup: true,
                },
            )
            .expect("readdirplus snapshot must succeed")
        else {
            panic!("ASSERT: readdir returned the wrong reply variant");
        };
        let mut stream = LookupTrackingStream::new(namespace.clone(), entries);
        assert!(stream.next().await.is_some());
        drop(stream);

        assert_eq!(
            namespace.dispatch(
                INTERNAL_CONTEXT,
                Operation::Unlink {
                    parent: ROOT_INODE,
                    name: b"pending",
                },
            ),
            Ok(Reply::Empty)
        );
        assert_eq!(
            namespace.dispatch(
                INTERNAL_CONTEXT,
                Operation::Forget {
                    inode: entry.attr.inode,
                    lookup_count: 1,
                },
            ),
            Ok(Reply::Empty)
        );
        assert_eq!(
            namespace.dispatch(
                INTERNAL_CONTEXT,
                Operation::GetAttr {
                    inode: entry.attr.inode,
                },
            ),
            Err(PosixError::NoEntry)
        );
    }

    #[tokio::test]
    async fn fuse_mutation_waits_for_transient_backpressure_instead_of_returning_again() {
        let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
        namespace.pause_mutation_admission();
        let attempts = Arc::new(AtomicUsize::new(0));
        let task_namespace = Arc::clone(&namespace);
        let task_attempts = Arc::clone(&attempts);
        let mutation = tokio::spawn(async move {
            dispatch_mutation_with_backpressure(&task_namespace, || {
                task_attempts.fetch_add(1, Ordering::SeqCst);
                Ok(Reply::Empty)
            })
            .await
        });

        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert!(!mutation.is_finished());

        namespace.resume_mutation_admission();
        assert_eq!(
            mutation.await.expect("mutation task must not panic"),
            Ok(Reply::Empty)
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn fallocate_flags_accept_only_the_supported_exact_combinations() {
        let flag = |value: i32| u32::try_from(value).expect("Linux fallocate flag is nonnegative");
        assert_eq!(
            fallocate_mode(0).expect("default allocation"),
            FallocateMode::Allocate { keep_size: false }
        );
        assert_eq!(
            fallocate_mode(flag(libc::FALLOC_FL_KEEP_SIZE)).expect("keep-size allocation"),
            FallocateMode::Allocate { keep_size: true }
        );
        assert_eq!(
            fallocate_mode(flag(libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE))
                .expect("hole punch"),
            FallocateMode::PunchHole
        );
        assert_eq!(
            fallocate_mode(flag(libc::FALLOC_FL_ZERO_RANGE | libc::FALLOC_FL_KEEP_SIZE))
                .expect("keep-size zero range"),
            FallocateMode::ZeroRange { keep_size: true }
        );
        assert_eq!(
            fallocate_mode(flag(libc::FALLOC_FL_COLLAPSE_RANGE)).expect("collapse range"),
            FallocateMode::CollapseRange
        );
        assert_eq!(
            fallocate_mode(flag(libc::FALLOC_FL_INSERT_RANGE)).expect("insert range"),
            FallocateMode::InsertRange
        );
        assert!(fallocate_mode(flag(libc::FALLOC_FL_PUNCH_HOLE)).is_err());
        assert!(
            fallocate_mode(flag(
                libc::FALLOC_FL_COLLAPSE_RANGE | libc::FALLOC_FL_KEEP_SIZE
            ))
            .is_err()
        );
        assert!(fallocate_mode(flag(libc::FALLOC_FL_UNSHARE_RANGE)).is_err());
    }

    #[test]
    fn statfs_snapshot_rejects_inconsistent_values_and_rounds_down_to_blocks() {
        assert!(StatFsSnapshot::new(0, 0, 0, 0, 0, 0, 255).is_err());
        assert!(StatFsSnapshot::new(100, 101, 100, 10, 10, 4, 255).is_err());
        assert!(StatFsSnapshot::new(100, 90, 91, 10, 10, 4, 255).is_err());
        assert!(StatFsSnapshot::new(100, 90, 80, 10, 11, 4, 255).is_err());

        let reply = StatFsSnapshot::new(4_099, 3_003, 2_007, 10, 7, 1_000, 255)
            .expect("valid snapshot")
            .reply();
        assert_eq!((reply.blocks, reply.bfree, reply.bavail), (4, 3, 2));
        assert_eq!((reply.files, reply.ffree), (10, 7));
        assert_eq!(
            (reply.bsize, reply.frsize, reply.namelen),
            (1_000, 1_000, 255)
        );
    }
}
