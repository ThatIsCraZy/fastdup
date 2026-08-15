use crate::{
    AccessMode, DirectoryEntry as NamespaceDirectoryEntry, Entry, FileAttr, FileKind, HandleId,
    InodeId, Namespace, OpenOptions, Operation, PosixError, Reply, RequestContext,
};
use bytes::Bytes;
use fuse3::raw::Filesystem;
use fuse3::raw::Request;
use fuse3::raw::reply::{
    DirectoryEntry, DirectoryEntryPlus, FileAttr as FuseFileAttr, ReplyAttr, ReplyCreated,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit, ReplyOpen, ReplyStatFs,
    ReplyWrite,
};
use fuse3::{Errno, FileType, MountOptions, SetAttr, Timestamp};
use futures_util::stream::{self, Stream};
use std::ffi::{OsStr, OsString};
use std::num::NonZeroU32;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

const MAXIMUM_WRITE_BYTES: u32 = 1_024 * 1_024;
const FOPEN_DIRECT_IO: u32 = 1;
const ZERO_TTL: Duration = Duration::ZERO;
const INTERNAL_CONTEXT: RequestContext = RequestContext {
    uid: 0,
    gid: 0,
    pid: 0,
};

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
}

impl FuseFilesystem {
    #[must_use]
    pub const fn new(namespace: Arc<Namespace>) -> Self {
        Self { namespace }
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
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::SetLength {
                    inode,
                    handle,
                    length,
                },
            )
            .map_err(errno)?;
        Ok(ReplyAttr {
            ttl: ZERO_TTL,
            attr: fuse_attr(expect_attr(&reply)),
        })
    }

    async fn unlink(&self, request: Request, parent: u64, name: &OsStr) -> fuse3::Result<()> {
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::Unlink {
                    parent: inode_from_raw(parent)?,
                    name: name.as_bytes(),
                },
            )
            .map_err(errno)?;
        expect_empty(&reply);
        Ok(())
    }

    async fn open(&self, request: Request, inode: u64, flags: u32) -> fuse3::Result<ReplyOpen> {
        let inode = inode_from_raw(inode)?;
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::Open {
                    inode,
                    options: open_options(flags)?,
                    truncate: flags
                        & u32::try_from(libc::O_TRUNC)
                            .expect("ASSERT: O_TRUNC must be nonnegative")
                        != 0,
                },
            )
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
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::Read {
                    inode: inode_from_raw(inode)?,
                    handle: handle_from_raw(handle)?,
                    offset,
                    length: size,
                },
            )
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
        if write_flags & fuse3::raw::flags::FUSE_WRITE_CACHE != 0 {
            return Err(libc::EIO.into());
        }
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::Write {
                    inode: inode_from_raw(inode)?,
                    handle: handle_from_raw(handle)?,
                    offset,
                    data,
                },
            )
            .map_err(errno)?;
        let (bytes, _) = expect_written(&reply);
        Ok(ReplyWrite { written: bytes })
    }

    async fn statfs(&self, _request: Request, _inode: u64) -> fuse3::Result<ReplyStatFs> {
        Err(libc::EOPNOTSUPP.into())
    }

    async fn release(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> fuse3::Result<()> {
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::Release {
                    inode: inode_from_raw(inode)?,
                    handle: handle_from_raw(handle)?,
                },
            )
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
        self.sync(request, inode, handle, data_only)
    }

    async fn flush(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        _lock_owner: u64,
    ) -> fuse3::Result<()> {
        self.sync(request, inode, handle, false)
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
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::Create {
                    parent,
                    name: name.as_bytes(),
                    mode: u16::try_from(mode & 0o7777)
                        .expect("ASSERT: masked mode must fit in u16"),
                    options,
                    exclusive: flags
                        & u32::try_from(libc::O_EXCL).expect("ASSERT: O_EXCL is nonnegative")
                        != 0,
                    truncate: flags
                        & u32::try_from(libc::O_TRUNC)
                            .expect("ASSERT: O_TRUNC must be nonnegative")
                        != 0,
                },
            )
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
}

impl FuseFilesystem {
    fn sync(
        &self,
        request: Request,
        inode: u64,
        handle: u64,
        data_only: bool,
    ) -> fuse3::Result<()> {
        let reply = self
            .namespace
            .dispatch(
                context(request),
                Operation::Sync {
                    inode: inode_from_raw(inode)?,
                    handle: handle_from_raw(handle)?,
                    data_only,
                },
            )
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

fn errno(error: PosixError) -> Errno {
    match error {
        PosixError::NoEntry => Errno::new_not_exist(),
        PosixError::Exists => Errno::new_exist(),
        PosixError::NotDirectory => Errno::new_is_not_dir(),
        PosixError::IsDirectory => Errno::new_is_dir(),
        PosixError::InvalidName | PosixError::InvalidArgument => libc::EINVAL.into(),
        PosixError::NameTooLong => libc::ENAMETOOLONG.into(),
        PosixError::BadHandle => libc::EBADF.into(),
        PosixError::FileTooLarge => libc::EFBIG.into(),
        PosixError::NoSpace => libc::ENOSPC.into(),
        PosixError::OutOfMemory => libc::ENOMEM.into(),
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
    use super::{INTERNAL_CONTEXT, LookupTrackingStream};
    use crate::{
        Namespace, NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply,
    };
    use futures_util::StreamExt;
    use std::sync::Arc;

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
}
