//! Linux `io_uring` adapter for fastdup's synchronous durable storage seam.
//!
//! One shared worker owns the ring. Callers keep the existing blocking
//! [`StorageIo`] contract, while operations from independent Container
//! publishers can overlap in the kernel. Buffer ownership and the only unsafe
//! submission call are confined to this platform crate.

mod read_buffer;
use read_buffer::ReadBuffer;

use std::collections::{HashMap, VecDeque};
use std::ffi::CString;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::num::{NonZeroU32, NonZeroU64};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use fastdup_format::{
    AlignedContainerBytes, HEADER_BYTES, MAX_CONTAINER_BYTES, VerifiedContainerPublication,
};
use fastdup_store::{
    FsStorageIo, MAX_STORAGE_RANGE_BYTES, OwnedContainerPublication, PublicationSampleRange,
    StorageIo, StoreError, publication_sample_ranges, verify_publication_sample,
};
use io_uring::{IoUring, Probe, opcode, squeue, types};

const DEFAULT_RING_ENTRIES: u32 = 256;
const DEFAULT_INFLIGHT_BYTES: u64 = 256 * 1_024 * 1_024;
const WAKE_USER_DATA: u64 = u64::MAX;
/// Smallest sealed Container for which the XFS A/B benchmark showed no
/// publication-throughput or tail-latency regression from Direct I/O.
pub const DIRECT_PUBLICATION_MIN_BYTES: usize = 4 * 1_024 * 1_024;

/// Cache policy for the short-lived DATA Container publication descriptor.
///
/// This does not affect ordinary reads after rename; they remain buffered and
/// retain kernel readahead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationIoMode {
    Buffered,
    Direct,
    /// Selects Direct I/O only for Containers at least 4 MiB long.
    Adaptive,
}

/// Bounded shared-ring configuration for the data-tier adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringStorageConfig {
    ring_entries: NonZeroU32,
    max_inflight_bytes: NonZeroU64,
    publication_io_mode: PublicationIoMode,
}

impl IoUringStorageConfig {
    #[must_use]
    pub fn new(ring_entries: NonZeroU32, max_inflight_bytes: NonZeroU64) -> Self {
        Self {
            ring_entries,
            max_inflight_bytes,
            publication_io_mode: PublicationIoMode::Adaptive,
        }
    }

    #[must_use]
    pub const fn ring_entries(self) -> NonZeroU32 {
        self.ring_entries
    }

    #[must_use]
    pub const fn max_inflight_bytes(self) -> NonZeroU64 {
        self.max_inflight_bytes
    }

    #[must_use]
    pub const fn with_publication_io_mode(mut self, mode: PublicationIoMode) -> Self {
        self.publication_io_mode = mode;
        self
    }

    #[must_use]
    pub const fn publication_io_mode(self) -> PublicationIoMode {
        self.publication_io_mode
    }
}

impl Default for IoUringStorageConfig {
    fn default() -> Self {
        Self::new(
            NonZeroU32::new(DEFAULT_RING_ENTRIES).expect("ASSERT: default ring size is nonzero"),
            NonZeroU64::new(DEFAULT_INFLIGHT_BYTES)
                .expect("ASSERT: default in-flight byte limit is nonzero"),
        )
    }
}

/// Point-in-time boundedness and batching telemetry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoUringStorageStatus {
    ring_entries: u32,
    max_inflight_bytes: u64,
    inflight_bytes: u64,
    peak_inflight_bytes: u64,
    submitted_operations: u64,
    completed_operations: u64,
    root_sync_callers: u64,
    root_sync_submissions: u64,
    owned_publications_started: u64,
    owned_publications_completed: u64,
    borrowed_write_copy_bytes: u64,
    publication_io_mode: PublicationIoMode,
    direct_publication_write_bytes: u64,
    direct_publication_sample_bytes: u64,
}

impl IoUringStorageStatus {
    #[must_use]
    pub const fn ring_entries(&self) -> u32 {
        self.ring_entries
    }

    #[must_use]
    pub const fn max_inflight_bytes(&self) -> u64 {
        self.max_inflight_bytes
    }

    #[must_use]
    pub const fn inflight_bytes(&self) -> u64 {
        self.inflight_bytes
    }

    #[must_use]
    pub const fn peak_inflight_bytes(&self) -> u64 {
        self.peak_inflight_bytes
    }

    #[must_use]
    pub const fn submitted_operations(&self) -> u64 {
        self.submitted_operations
    }

    #[must_use]
    pub const fn completed_operations(&self) -> u64 {
        self.completed_operations
    }

    #[must_use]
    pub const fn root_sync_callers(&self) -> u64 {
        self.root_sync_callers
    }

    #[must_use]
    pub const fn root_sync_submissions(&self) -> u64 {
        self.root_sync_submissions
    }

    #[must_use]
    pub const fn owned_publications_started(&self) -> u64 {
        self.owned_publications_started
    }

    #[must_use]
    pub const fn owned_publications_completed(&self) -> u64 {
        self.owned_publications_completed
    }

    #[must_use]
    pub const fn borrowed_write_copy_bytes(&self) -> u64 {
        self.borrowed_write_copy_bytes
    }

    #[must_use]
    pub const fn publication_io_mode(&self) -> PublicationIoMode {
        self.publication_io_mode
    }

    #[must_use]
    pub const fn direct_publication_write_bytes(&self) -> u64 {
        self.direct_publication_write_bytes
    }

    #[must_use]
    pub const fn direct_publication_sample_bytes(&self) -> u64 {
        self.direct_publication_sample_bytes
    }
}

/// Cloneable data-tier storage adapter backed by one shared bounded ring.
pub struct IoUringStorageIo {
    filesystem: FsStorageIo,
    backend: Arc<ActiveBackend>,
    config: IoUringStorageConfig,
}

impl Clone for IoUringStorageIo {
    fn clone(&self) -> Self {
        Self {
            filesystem: self.filesystem.clone(),
            backend: Arc::clone(&self.backend),
            config: self.config,
        }
    }
}

impl fmt::Debug for IoUringStorageIo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IoUringStorageIo")
            .field("root", &self.filesystem.root())
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl IoUringStorageIo {
    /// Opens the root and starts its required shared ring.
    ///
    /// # Errors
    ///
    /// Returns root initialization, ring setup, or worker spawn errors.
    pub fn open(root: impl AsRef<Path>, config: IoUringStorageConfig) -> io::Result<Self> {
        let filesystem = FsStorageIo::open(root)?;
        let backend = Arc::new(ActiveBackend::start(config)?);
        Ok(Self {
            filesystem,
            backend,
            config,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.filesystem.root()
    }

    #[must_use]
    pub fn status(&self) -> IoUringStorageStatus {
        self.backend.status(self.config)
    }

    fn path(&self, name: &str) -> io::Result<PathBuf> {
        validate_name(name)?;
        Ok(self.filesystem.root().join(name))
    }
}

impl StorageIo for IoUringStorageIo {
    fn create_new(&self, name: &str) -> io::Result<()> {
        self.filesystem.create_new(name)
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        self.filesystem.exists(name)
    }

    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let byte_length = u64::try_from(bytes.len())
            .map_err(|_| invalid_input("write length does not fit u64"))?;
        offset
            .checked_add(byte_length)
            .ok_or_else(|| invalid_input("write range overflows"))?;
        let lease = self.backend.budget.acquire(byte_length)?;
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        owned.extend_from_slice(bytes);
        self.backend
            .counters
            .callers
            .borrowed_write_copy_bytes
            .fetch_add(byte_length, Ordering::Relaxed);
        self.filesystem.with_file_mutation(name, || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.path(name)?)?;
            self.backend.write(file, offset, owned, lease)
        })
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let file = File::open(self.path(name)?)?;
        let length = file.metadata()?.len();
        if length > MAX_CONTAINER_BYTES {
            return Err(invalid_data("container exceeds the format-v1 hard limit"));
        }
        self.backend
            .read(Arc::new(file), 0, usize_from_u64(length)?)
    }

    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.filesystem.object_len(name)
    }

    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        if length > MAX_STORAGE_RANGE_BYTES {
            return Err(invalid_input(
                "bounded storage read exceeds the hard allocation limit",
            ));
        }
        let file = self.filesystem.open_read_range(name, offset, length)?;
        self.backend.read(file, offset, length)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        self.filesystem.list_names()
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.filesystem.set_len(name, length)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        self.backend.fsync(File::open(self.path(name)?)?)
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        let old_name = c_name(temporary_name)?;
        let new_name = c_name(published_name)?;
        let directory = File::open(self.filesystem.root())?;
        self.filesystem
            .with_file_rename(temporary_name, published_name, || {
                self.backend.rename_noreplace(directory, old_name, new_name)
            })
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.filesystem.remove_file(name)
    }

    fn sync_root(&self) -> io::Result<()> {
        self.backend.sync_root(File::open(self.filesystem.root())?)
    }

    fn publish_owned_container(
        &self,
        publication: OwnedContainerPublication,
    ) -> Result<VerifiedContainerPublication, StoreError> {
        let lease = self.backend.acquire_publication(&publication)?;
        let temporary_name = publication.temporary_name().to_owned();
        let published_name = publication.published_name().to_owned();
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        let resolved_io_mode = match self.config.publication_io_mode {
            PublicationIoMode::Buffered => PublicationIoMode::Buffered,
            PublicationIoMode::Direct => PublicationIoMode::Direct,
            PublicationIoMode::Adaptive => {
                if publication.sealed_len() >= DIRECT_PUBLICATION_MIN_BYTES {
                    PublicationIoMode::Direct
                } else {
                    PublicationIoMode::Buffered
                }
            }
        };
        if resolved_io_mode == PublicationIoMode::Direct {
            options.custom_flags(libc::O_DIRECT);
        }
        let file = options.open(self.path(&temporary_name)?)?;
        let directory = File::open(self.filesystem.root())?;
        self.backend.publish_owned(
            PublicationTarget {
                file,
                directory,
                old_name: c_name(&temporary_name)?,
                new_name: c_name(&published_name)?,
                io_mode: resolved_io_mode,
            },
            publication,
            lease,
        )
    }
}

struct WakeSignal {
    file: File,
}

impl WakeSignal {
    fn new() -> io::Result<Self> {
        // SAFETY: `eventfd` returns a new owned descriptor on success. `File`
        // takes that ownership exactly once and closes it on drop.
        let descriptor = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful `eventfd` call returned one valid owned fd.
        let file = unsafe { File::from_raw_fd(descriptor) };
        Ok(Self { file })
    }

    fn notify(&self) -> io::Result<()> {
        let value = 1_u64.to_ne_bytes();
        // SAFETY: the eventfd is live for this call and `value` supplies the
        // exact eight bytes required by eventfd(2).
        let written =
            unsafe { libc::write(self.file.as_raw_fd(), value.as_ptr().cast(), value.len()) };
        if written == isize::try_from(value.len()).expect("ASSERT: eventfd write length fits isize")
        {
            Ok(())
        } else if written < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                // A saturated counter already guarantees a pending wakeup.
                Ok(())
            } else if error.kind() == io::ErrorKind::Interrupted {
                self.notify()
            } else {
                Err(error)
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short eventfd wake write",
            ))
        }
    }

    fn notify_best_effort(&self) {
        let _ = self.notify();
    }

    fn drain(&self) -> io::Result<()> {
        loop {
            let mut value = [0_u8; std::mem::size_of::<u64>()];
            // SAFETY: the eventfd is live and `value` is one writable u64.
            let read = unsafe {
                libc::read(
                    self.file.as_raw_fd(),
                    value.as_mut_ptr().cast(),
                    value.len(),
                )
            };
            if read == isize::try_from(value.len()).expect("ASSERT: eventfd read length fits isize")
            {
                continue;
            }
            if read < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short eventfd wake read",
            ));
        }
    }
}

struct ActiveBackend {
    sender: mpsc::SyncSender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    budget: Arc<InflightBudget>,
    counters: Arc<Counters>,
    wake: Arc<WakeSignal>,
}

impl ActiveBackend {
    fn start(config: IoUringStorageConfig) -> io::Result<Self> {
        if config.ring_entries.get() < 2 {
            return Err(invalid_input(
                "io_uring requires one operation entry plus one command wake entry",
            ));
        }
        let queue_capacity = usize::try_from(config.ring_entries.get())
            .expect("ASSERT: u32 ring entry count fits usize")
            .checked_mul(2)
            .expect("ASSERT: bounded ring queue capacity cannot overflow");
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let budget = Arc::new(InflightBudget::new(config.max_inflight_bytes.get()));
        let counters = Arc::new(Counters::default());
        let worker_counters = Arc::clone(&counters);
        let wake = Arc::new(WakeSignal::new()?);
        let worker_wake = Arc::clone(&wake);
        let entries = usize::try_from(config.ring_entries.get())
            .expect("ASSERT: u32 ring entry count fits usize");
        let (initialized, initialization) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("fastdup-io-uring".to_owned())
            .spawn(move || {
                let ring = IoUring::builder()
                    .setup_single_issuer()
                    .build(config.ring_entries.get());
                let ring = match ring {
                    Ok(ring) => ring,
                    Err(error) => {
                        let _ = initialized.send(Err(error));
                        return;
                    }
                };
                if let Err(error) = require_publication_opcodes(&ring) {
                    let _ = initialized.send(Err(error));
                    return;
                }
                if initialized.send(Ok(())).is_err() {
                    return;
                }
                worker_loop(ring, &receiver, &worker_counters, entries, &worker_wake);
            })?;
        match initialization.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                worker
                    .join()
                    .expect("ASSERT: failed io_uring worker initialization does not panic");
                return Err(error);
            }
            Err(_) => {
                worker
                    .join()
                    .expect("ASSERT: failed io_uring worker initialization does not panic");
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "io_uring worker stopped during initialization",
                ));
            }
        }
        Ok(Self {
            sender,
            worker: Mutex::new(Some(worker)),
            budget,
            counters,
            wake,
        })
    }

    fn status(&self, config: IoUringStorageConfig) -> IoUringStorageStatus {
        IoUringStorageStatus {
            ring_entries: config.ring_entries.get(),
            max_inflight_bytes: config.max_inflight_bytes.get(),
            inflight_bytes: self.budget.used.load(Ordering::Relaxed),
            peak_inflight_bytes: self.budget.peak.load(Ordering::Relaxed),
            submitted_operations: self.counters.worker.submitted.load(Ordering::Relaxed),
            completed_operations: self.counters.worker.completed.load(Ordering::Relaxed),
            root_sync_callers: self.counters.callers.root_sync.load(Ordering::Relaxed),
            root_sync_submissions: self
                .counters
                .worker
                .root_sync_submissions
                .load(Ordering::Relaxed),
            owned_publications_started: self
                .counters
                .callers
                .owned_publications_started
                .load(Ordering::Relaxed),
            owned_publications_completed: self
                .counters
                .worker
                .owned_publications_completed
                .load(Ordering::Relaxed),
            borrowed_write_copy_bytes: self
                .counters
                .callers
                .borrowed_write_copy_bytes
                .load(Ordering::Relaxed),
            publication_io_mode: config.publication_io_mode,
            direct_publication_write_bytes: self
                .counters
                .worker
                .direct_publication_write_bytes
                .load(Ordering::Relaxed),
            direct_publication_sample_bytes: self
                .counters
                .worker
                .direct_publication_sample_bytes
                .load(Ordering::Relaxed),
        }
    }

    fn write(&self, file: File, offset: u64, bytes: Vec<u8>, lease: BudgetLease) -> io::Result<()> {
        let (reply, receive) = mpsc::sync_channel(1);
        self.send(Command::Write {
            file,
            offset,
            bytes,
            lease,
            reply,
        })?;
        receive_reply(&receive)
    }

    fn read(&self, file: Arc<File>, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let length_u64 =
            u64::try_from(length).map_err(|_| invalid_input("read length does not fit u64"))?;
        let lease = self.budget.acquire(length_u64)?;
        let bytes = ReadBuffer::new(length)?;
        let (reply, receive) = mpsc::sync_channel(1);
        self.send(Command::Read {
            file,
            offset,
            bytes,
            lease,
            reply,
        })?;
        receive_reply(&receive)
    }

    fn fsync(&self, file: File) -> io::Result<()> {
        let (reply, receive) = mpsc::sync_channel(1);
        self.send(Command::Fsync { file, reply })?;
        receive_reply(&receive)
    }

    fn rename_noreplace(
        &self,
        directory: File,
        old_name: CString,
        new_name: CString,
    ) -> io::Result<()> {
        let (reply, receive) = mpsc::sync_channel(1);
        self.send(Command::Rename {
            directory,
            old_name,
            new_name,
            reply,
        })?;
        receive_reply(&receive)
    }

    fn sync_root(&self, directory: File) -> io::Result<()> {
        self.counters
            .callers
            .root_sync
            .fetch_add(1, Ordering::Relaxed);
        let (reply, receive) = mpsc::sync_channel(1);
        self.send(Command::SyncRoot { directory, reply })?;
        receive_reply(&receive)
    }

    fn publish_owned(
        &self,
        target: PublicationTarget,
        publication: OwnedContainerPublication,
        lease: BudgetLease,
    ) -> Result<VerifiedContainerPublication, StoreError> {
        assert_eq!(
            lease.bytes,
            u64::try_from(publication.sealed_len())
                .expect("ASSERT: bounded Container image length fits u64"),
            "ASSERT: publication lease is paired with its owned image"
        );
        self.counters
            .callers
            .owned_publications_started
            .fetch_add(1, Ordering::Relaxed);
        let (reply, receive) = mpsc::sync_channel(1);
        self.send(Command::PublishOwned {
            target,
            publication: Box::new(publication),
            lease,
            reply,
        })?;
        receive_store_reply(&receive)
    }

    fn acquire_publication(
        &self,
        publication: &OwnedContainerPublication,
    ) -> Result<BudgetLease, StoreError> {
        let image_bytes = u64::try_from(publication.sealed_len()).map_err(|_| {
            StoreError::Io(invalid_input("Container image length does not fit u64"))
        })?;
        self.budget.acquire(image_bytes).map_err(StoreError::Io)
    }

    fn send(&self, command: Command) -> io::Result<()> {
        self.sender
            .send(command)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring worker stopped"))?;
        self.wake
            .notify()
            .expect("ASSERT: a live io_uring command channel has a live eventfd wakeup");
        Ok(())
    }
}

impl Drop for ActiveBackend {
    fn drop(&mut self) {
        let _ = self.sender.send(Command::Shutdown);
        self.wake.notify_best_effort();
        if let Some(worker) = self
            .worker
            .lock()
            .expect("ASSERT: io_uring worker lock poisoned")
            .take()
        {
            let result = worker.join();
            assert!(result.is_ok(), "ASSERT: io_uring worker panicked");
        }
    }
}

fn require_publication_opcodes(ring: &IoUring) -> io::Result<()> {
    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe)?;
    for (name, code) in [
        ("READ", opcode::Read::CODE),
        ("WRITE", opcode::Write::CODE),
        ("FSYNC", opcode::Fsync::CODE),
        ("POLL_ADD", opcode::PollAdd::CODE),
        ("RENAMEAT", opcode::RenameAt::CODE),
        ("FTRUNCATE", opcode::Ftruncate::CODE),
    ] {
        if !probe.is_supported(code) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("required io_uring opcode {name} is unavailable"),
            ));
        }
    }
    Ok(())
}

#[derive(Default)]
struct Counters {
    worker: WorkerCounters,
    callers: CallerCounters,
}

#[derive(Default)]
#[repr(C, align(64))]
struct WorkerCounters {
    submitted: AtomicU64,
    completed: AtomicU64,
    root_sync_submissions: AtomicU64,
    owned_publications_completed: AtomicU64,
    direct_publication_write_bytes: AtomicU64,
    direct_publication_sample_bytes: AtomicU64,
}

#[derive(Default)]
#[repr(C, align(64))]
struct CallerCounters {
    root_sync: AtomicU64,
    owned_publications_started: AtomicU64,
    borrowed_write_copy_bytes: AtomicU64,
}

struct InflightBudget {
    maximum: u64,
    state: Mutex<u64>,
    available: Condvar,
    used: AtomicU64,
    peak: AtomicU64,
}

impl InflightBudget {
    fn new(maximum: u64) -> Self {
        Self {
            maximum,
            state: Mutex::new(0),
            available: Condvar::new(),
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        }
    }

    fn acquire(self: &Arc<Self>, bytes: u64) -> io::Result<BudgetLease> {
        if bytes > self.maximum {
            return Err(invalid_input(
                "one io_uring buffer exceeds the in-flight byte limit",
            ));
        }
        let mut used = self
            .state
            .lock()
            .expect("ASSERT: io_uring byte-budget lock poisoned");
        while used
            .checked_add(bytes)
            .is_none_or(|candidate| candidate > self.maximum)
        {
            used = self
                .available
                .wait(used)
                .expect("ASSERT: io_uring byte-budget lock poisoned while waiting");
        }
        *used = used
            .checked_add(bytes)
            .expect("ASSERT: admitted io_uring bytes cannot overflow");
        self.used.store(*used, Ordering::Relaxed);
        self.peak.fetch_max(*used, Ordering::Relaxed);
        drop(used);
        Ok(BudgetLease {
            budget: Arc::clone(self),
            bytes,
        })
    }

    fn release(&self, bytes: u64) {
        let mut used = self
            .state
            .lock()
            .expect("ASSERT: io_uring byte-budget lock poisoned");
        *used = used
            .checked_sub(bytes)
            .expect("ASSERT: io_uring byte-budget release is paired");
        self.used.store(*used, Ordering::Relaxed);
        drop(used);
        self.available.notify_all();
    }
}

struct BudgetLease {
    budget: Arc<InflightBudget>,
    bytes: u64,
}

impl Drop for BudgetLease {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

enum Command {
    PublishOwned {
        target: PublicationTarget,
        publication: Box<OwnedContainerPublication>,
        lease: BudgetLease,
        reply: mpsc::SyncSender<Result<VerifiedContainerPublication, StoreError>>,
    },
    Write {
        file: File,
        offset: u64,
        bytes: Vec<u8>,
        lease: BudgetLease,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
    Read {
        file: Arc<File>,
        offset: u64,
        bytes: ReadBuffer,
        lease: BudgetLease,
        reply: mpsc::SyncSender<io::Result<Vec<u8>>>,
    },
    Fsync {
        file: File,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
    Rename {
        directory: File,
        old_name: CString,
        new_name: CString,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
    SyncRoot {
        directory: File,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
    Shutdown,
}

struct PublicationTarget {
    file: File,
    directory: File,
    old_name: CString,
    new_name: CString,
    io_mode: PublicationIoMode,
}

enum PublishPhase {
    Building {
        progress: usize,
    },
    Body {
        progress: usize,
    },
    SealedHeader {
        progress: usize,
    },
    SetLength,
    SampleRead {
        ordinal: usize,
        range: PublicationSampleRange,
        bytes: AlignedContainerBytes,
        progress: usize,
    },
    FileSync,
    Rename,
}

struct PublishOperation {
    file: File,
    directory: Option<File>,
    old_name: CString,
    new_name: CString,
    sealed_length: usize,
    building_header: AlignedContainerBytes,
    sealed: AlignedContainerBytes,
    publication_io_mode: PublicationIoMode,
    phase: PublishPhase,
    verified: Option<VerifiedContainerPublication>,
    lease: Option<BudgetLease>,
    reply: Option<mpsc::SyncSender<Result<VerifiedContainerPublication, StoreError>>>,
}

impl PublishOperation {
    fn new(
        target: PublicationTarget,
        publication: OwnedContainerPublication,
        lease: BudgetLease,
        reply: mpsc::SyncSender<Result<VerifiedContainerPublication, StoreError>>,
    ) -> Self {
        let PublicationTarget {
            file,
            directory,
            old_name,
            new_name,
            io_mode: publication_io_mode,
        } = target;
        let (
            container_id,
            container_generation,
            building_header,
            sealed,
            verified,
            temporary_name,
            published_name,
        ) = publication.into_parts();
        assert_eq!(
            old_name.as_bytes(),
            temporary_name.as_bytes(),
            "ASSERT: owned publisher temporary name is paired"
        );
        assert_eq!(
            new_name.as_bytes(),
            published_name.as_bytes(),
            "ASSERT: owned publisher final name is paired"
        );
        assert!(
            sealed.len() > HEADER_BYTES,
            "ASSERT: owned publisher receives a complete sealed Container"
        );
        assert_eq!(
            verified.header().container_id(),
            container_id,
            "ASSERT: writer evidence keeps the owned Container identity"
        );
        assert_eq!(
            verified.header().container_generation(),
            container_generation,
            "ASSERT: writer evidence keeps the owned Container generation"
        );
        let sealed_length = sealed.len();
        Self {
            file,
            directory: Some(directory),
            old_name,
            new_name,
            sealed_length,
            building_header,
            sealed,
            publication_io_mode,
            phase: PublishPhase::Building { progress: 0 },
            verified: Some(verified),
            lease: Some(lease),
            reply: Some(reply),
        }
    }

    fn entry(&mut self) -> io::Result<squeue::Entry> {
        match &mut self.phase {
            PublishPhase::Building { progress } => {
                write_entry(&self.file, &self.building_header[*progress..], *progress)
            }
            PublishPhase::Body { progress } => {
                let body = &self.sealed[HEADER_BYTES + *progress..];
                write_entry(&self.file, body, HEADER_BYTES + *progress)
            }
            PublishPhase::SealedHeader { progress } => {
                write_entry(&self.file, &self.sealed[*progress..HEADER_BYTES], *progress)
            }
            PublishPhase::SetLength => Ok(opcode::Ftruncate::new(
                types::Fd(self.file.as_raw_fd()),
                u64::try_from(self.sealed_length)
                    .expect("ASSERT: bounded Container length fits u64"),
            )
            .build()),
            PublishPhase::SampleRead {
                range,
                bytes,
                progress,
                ..
            } => {
                let offset = usize::try_from(range.offset())
                    .map_err(|_| invalid_input("publication sample offset does not fit usize"))?
                    .checked_add(*progress)
                    .ok_or_else(|| invalid_input("publication sample offset overflow"))?;
                read_entry(&self.file, &mut bytes[*progress..], offset)
            }
            PublishPhase::FileSync => {
                Ok(opcode::Fsync::new(types::Fd(self.file.as_raw_fd())).build())
            }
            PublishPhase::Rename => {
                let directory = self
                    .directory
                    .as_ref()
                    .expect("ASSERT: rename retains its root-directory descriptor");
                Ok(opcode::RenameAt::new(
                    types::Fd(directory.as_raw_fd()),
                    self.old_name.as_ptr(),
                    types::Fd(directory.as_raw_fd()),
                    self.new_name.as_ptr(),
                )
                .flags(libc::RENAME_NOREPLACE)
                .build())
            }
        }
    }

    fn complete(mut self, result: i32, counters: &Counters) -> OperationCompletion {
        if result < 0 {
            self.fail(StoreError::Io(io::Error::from_raw_os_error(-result)));
            return OperationCompletion::Done;
        }
        let transferred = usize::try_from(result)
            .expect("ASSERT: nonnegative owned-publisher CQE result fits usize");
        if self.publication_io_mode == PublicationIoMode::Direct {
            match &self.phase {
                PublishPhase::Building { .. }
                | PublishPhase::Body { .. }
                | PublishPhase::SealedHeader { .. } => {
                    counters.worker.direct_publication_write_bytes.fetch_add(
                        u64::try_from(transferred)
                            .expect("ASSERT: Direct-I/O write count fits u64"),
                        Ordering::Relaxed,
                    );
                }
                PublishPhase::SampleRead { .. } => {
                    counters.worker.direct_publication_sample_bytes.fetch_add(
                        u64::try_from(transferred)
                            .expect("ASSERT: Direct-I/O sample count fits u64"),
                        Ordering::Relaxed,
                    );
                }
                PublishPhase::SetLength | PublishPhase::FileSync | PublishPhase::Rename => {}
            }
        }
        match std::mem::replace(&mut self.phase, PublishPhase::FileSync) {
            PublishPhase::Building { progress } => self.complete_building(progress, transferred),
            PublishPhase::Body { progress } => self.complete_body(progress, transferred),
            PublishPhase::SealedHeader { progress } => {
                self.complete_sealed_header(progress, transferred)
            }
            PublishPhase::SetLength => {
                assert_eq!(
                    result, 0,
                    "ASSERT: owned publisher truncate succeeds with zero"
                );
                self.begin_sample_reads()
            }
            PublishPhase::SampleRead {
                ordinal,
                range,
                bytes,
                progress,
            } => self.complete_sample_read(ordinal, range, bytes, progress, transferred),
            PublishPhase::FileSync => {
                assert_eq!(
                    result, 0,
                    "ASSERT: owned publisher fsync succeeds with zero"
                );
                self.phase = PublishPhase::Rename;
                self.pending()
            }
            PublishPhase::Rename => {
                assert_eq!(
                    result, 0,
                    "ASSERT: owned publisher rename succeeds with zero"
                );
                OperationCompletion::ReadyRoot(PublishReady {
                    directory: self
                        .directory
                        .take()
                        .expect("ASSERT: renamed publication retains its root descriptor"),
                    verified: self
                        .verified
                        .take()
                        .expect("ASSERT: rename retains writer publication evidence"),
                    reply: self
                        .reply
                        .take()
                        .expect("ASSERT: publication reply is completed exactly once"),
                })
            }
        }
    }

    fn complete_building(mut self, mut progress: usize, transferred: usize) -> OperationCompletion {
        if !advance_write(
            &mut progress,
            self.building_header.len(),
            transferred,
            &mut self.reply,
        ) {
            return OperationCompletion::Done;
        }
        self.phase = if progress == self.building_header.len() {
            PublishPhase::Body { progress: 0 }
        } else {
            PublishPhase::Building { progress }
        };
        self.pending()
    }

    fn complete_body(mut self, mut progress: usize, transferred: usize) -> OperationCompletion {
        let body_length = self.sealed.len() - HEADER_BYTES;
        if !advance_write(&mut progress, body_length, transferred, &mut self.reply) {
            return OperationCompletion::Done;
        }
        self.phase = if progress == body_length {
            PublishPhase::SealedHeader { progress: 0 }
        } else {
            PublishPhase::Body { progress }
        };
        self.pending()
    }

    fn complete_sealed_header(
        mut self,
        mut progress: usize,
        transferred: usize,
    ) -> OperationCompletion {
        if !advance_write(&mut progress, HEADER_BYTES, transferred, &mut self.reply) {
            return OperationCompletion::Done;
        }
        if progress != HEADER_BYTES {
            self.phase = PublishPhase::SealedHeader { progress };
            return self.pending();
        }
        self.phase = PublishPhase::SetLength;
        self.pending()
    }

    fn begin_sample_reads(mut self) -> OperationCompletion {
        let ranges = match publication_sample_ranges(self.sealed_length) {
            Ok(ranges) => ranges,
            Err(error) => {
                self.fail(error);
                return OperationCompletion::Done;
            }
        };
        let bytes = match allocate_sample_buffer(ranges[0].length()) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.fail(error);
                return OperationCompletion::Done;
            }
        };
        self.phase = PublishPhase::SampleRead {
            ordinal: 0,
            range: ranges[0],
            bytes,
            progress: 0,
        };
        self.pending()
    }

    fn complete_sample_read(
        mut self,
        ordinal: usize,
        range: PublicationSampleRange,
        bytes: AlignedContainerBytes,
        mut progress: usize,
        transferred: usize,
    ) -> OperationCompletion {
        let remaining = bytes.len() - progress;
        assert!(
            transferred <= remaining,
            "ASSERT: owned publisher read CQE cannot exceed its request"
        );
        if transferred == 0 && remaining != 0 {
            self.fail(StoreError::Io(io::Error::from(
                io::ErrorKind::UnexpectedEof,
            )));
            return OperationCompletion::Done;
        }
        progress += transferred;
        if progress != bytes.len() {
            self.phase = PublishPhase::SampleRead {
                ordinal,
                range,
                bytes,
                progress,
            };
            return self.pending();
        }
        if let Err(error) = verify_publication_sample(&self.sealed, range, &bytes) {
            self.fail(error);
            return OperationCompletion::Done;
        }
        let ranges = match publication_sample_ranges(self.sealed_length) {
            Ok(ranges) => ranges,
            Err(error) => {
                self.fail(error);
                return OperationCompletion::Done;
            }
        };
        let next = ordinal + 1;
        if let Some(range) = ranges.get(next).copied() {
            let bytes = match allocate_sample_buffer(range.length()) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.fail(error);
                    return OperationCompletion::Done;
                }
            };
            self.phase = PublishPhase::SampleRead {
                ordinal: next,
                range,
                bytes,
                progress: 0,
            };
            return self.pending();
        }
        self.sealed = AlignedContainerBytes::empty();
        self.lease = None;
        self.phase = PublishPhase::FileSync;
        self.pending()
    }

    fn pending(self) -> OperationCompletion {
        OperationCompletion::Pending(Operation::PublishOwned(Box::new(self)))
    }

    fn fail(&mut self, error: StoreError) {
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Err(error));
        }
    }
}

fn allocate_sample_buffer(length: usize) -> Result<AlignedContainerBytes, StoreError> {
    if length == 0 || !length.is_multiple_of(HEADER_BYTES) {
        return Err(StoreError::Io(invalid_input(
            "publication sample is not Direct-I/O aligned",
        )));
    }
    Ok(AlignedContainerBytes::zeroed(length))
}

struct PublishReady {
    directory: File,
    verified: VerifiedContainerPublication,
    reply: mpsc::SyncSender<Result<VerifiedContainerPublication, StoreError>>,
}

struct RootPublication {
    verified: VerifiedContainerPublication,
    reply: mpsc::SyncSender<Result<VerifiedContainerPublication, StoreError>>,
}

enum OperationCompletion {
    Pending(Operation),
    ReadyRoot(PublishReady),
    Done,
}

enum Operation {
    PublishOwned(Box<PublishOperation>),
    Write {
        file: File,
        offset: u64,
        bytes: Vec<u8>,
        progress: usize,
        _lease: BudgetLease,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
    Read {
        file: Arc<File>,
        offset: u64,
        bytes: ReadBuffer,
        progress: usize,
        _lease: BudgetLease,
        reply: mpsc::SyncSender<io::Result<Vec<u8>>>,
    },
    Fsync {
        file: File,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
    Rename {
        directory: File,
        old_name: CString,
        new_name: CString,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
    RootSync {
        directory: File,
        replies: Vec<mpsc::SyncSender<io::Result<()>>>,
        publications: Vec<RootPublication>,
    },
}

impl Operation {
    fn entry(&mut self, user_data: u64) -> io::Result<squeue::Entry> {
        let entry = match self {
            Self::PublishOwned(operation) => operation.entry()?,
            Self::Write {
                file,
                offset,
                bytes,
                progress,
                ..
            } => {
                let remaining = &bytes[*progress..];
                let length = u32::try_from(remaining.len())
                    .map_err(|_| invalid_input("one io_uring write exceeds u32"))?;
                let operation_offset = offset
                    .checked_add(u64::try_from(*progress).expect("ASSERT: usize fits u64"))
                    .ok_or_else(|| invalid_input("write progress overflows offset"))?;
                opcode::Write::new(types::Fd(file.as_raw_fd()), remaining.as_ptr(), length)
                    .offset(operation_offset)
                    .build()
            }
            Self::Read {
                file,
                offset,
                bytes,
                progress,
                ..
            } => {
                let length = u32::try_from(bytes.len() - *progress)
                    .map_err(|_| invalid_input("one io_uring read exceeds u32"))?;
                let operation_offset = offset
                    .checked_add(u64::try_from(*progress).expect("ASSERT: usize fits u64"))
                    .ok_or_else(|| invalid_input("read progress overflows offset"))?;
                opcode::Read::new(
                    types::Fd(file.as_raw_fd()),
                    bytes.remaining_ptr(*progress),
                    length,
                )
                .offset(operation_offset)
                .build()
            }
            Self::Fsync { file, .. }
            | Self::RootSync {
                directory: file, ..
            } => opcode::Fsync::new(types::Fd(file.as_raw_fd())).build(),
            Self::Rename {
                directory,
                old_name,
                new_name,
                ..
            } => opcode::RenameAt::new(
                types::Fd(directory.as_raw_fd()),
                old_name.as_ptr(),
                types::Fd(directory.as_raw_fd()),
                new_name.as_ptr(),
            )
            .flags(libc::RENAME_NOREPLACE)
            .build(),
        };
        Ok(entry.user_data(user_data))
    }

    fn complete(self, result: i32, counters: &Counters) -> OperationCompletion {
        match self {
            Self::PublishOwned(operation) => (*operation).complete(result, counters),
            operation => complete_storage_operation(operation, result, counters),
        }
    }

    fn reply_error(&mut self, error: &io::Error) {
        let kind = error.kind();
        let message = error.to_string();
        match self {
            Self::PublishOwned(operation) => {
                operation.fail(StoreError::Io(io::Error::new(kind, message)));
            }
            Self::Write { reply, .. } | Self::Fsync { reply, .. } | Self::Rename { reply, .. } => {
                let _ = reply.send(Err(io::Error::new(kind, message)));
            }
            Self::Read { reply, .. } => {
                let _ = reply.send(Err(io::Error::new(kind, message)));
            }
            Self::RootSync {
                replies,
                publications,
                ..
            } => {
                for reply in replies.drain(..) {
                    let _ = reply.send(Err(io::Error::new(kind, message.clone())));
                }
                for publication in publications.drain(..) {
                    let _ = publication
                        .reply
                        .send(Err(StoreError::Io(io::Error::new(kind, message.clone()))));
                }
            }
        }
    }
}

fn complete_storage_operation(
    mut operation: Operation,
    result: i32,
    counters: &Counters,
) -> OperationCompletion {
    if result < 0 {
        operation.reply_error(&io::Error::from_raw_os_error(-result));
        return OperationCompletion::Done;
    }
    match &mut operation {
        Operation::Write {
            bytes,
            progress,
            reply,
            ..
        } => {
            let transferred =
                usize::try_from(result).expect("ASSERT: nonnegative io_uring result fits usize");
            let remaining = bytes.len() - *progress;
            assert!(
                transferred <= remaining,
                "ASSERT: kernel write completion exceeds submitted length"
            );
            if transferred == 0 && remaining != 0 {
                let _ = reply.send(Err(io::Error::from(io::ErrorKind::WriteZero)));
                return OperationCompletion::Done;
            }
            *progress += transferred;
            if *progress == bytes.len() {
                let _ = reply.send(Ok(()));
                OperationCompletion::Done
            } else {
                OperationCompletion::Pending(operation)
            }
        }
        Operation::Read {
            bytes,
            progress,
            reply,
            ..
        } => {
            let transferred =
                usize::try_from(result).expect("ASSERT: nonnegative io_uring result fits usize");
            let remaining = bytes.len() - *progress;
            assert!(
                transferred <= remaining,
                "ASSERT: kernel read completion exceeds submitted length"
            );
            if transferred == 0 && remaining != 0 {
                let _ = reply.send(Err(io::Error::from(io::ErrorKind::UnexpectedEof)));
                return OperationCompletion::Done;
            }
            *progress += transferred;
            if *progress == bytes.len() {
                let finished =
                    std::mem::replace(bytes, ReadBuffer::new(0).expect("empty read capacity"));
                // SAFETY: cumulative positive CQEs cover exactly 0..len. Each
                // SQE wrote the next disjoint spare range. This CQE completes
                // the last access; failures and short EOF never reach finish.
                let finished = unsafe { finished.finish() };
                let _ = reply.send(Ok(finished));
                OperationCompletion::Done
            } else {
                OperationCompletion::Pending(operation)
            }
        }
        Operation::Fsync { reply, .. } | Operation::Rename { reply, .. } => {
            assert_eq!(result, 0, "ASSERT: metadata CQE returns zero on success");
            let _ = reply.send(Ok(()));
            OperationCompletion::Done
        }
        Operation::RootSync {
            replies,
            publications,
            ..
        } => {
            assert_eq!(result, 0, "ASSERT: root fsync CQE returns zero on success");
            for reply in replies.drain(..) {
                let _ = reply.send(Ok(()));
            }
            let completed = publications.len();
            for publication in publications.drain(..) {
                let _ = publication.reply.send(Ok(publication.verified));
            }
            counters.worker.owned_publications_completed.fetch_add(
                u64::try_from(completed)
                    .expect("ASSERT: completed publication cohort count fits u64"),
                Ordering::Relaxed,
            );
            OperationCompletion::Done
        }
        Operation::PublishOwned(_) => unreachable!("ASSERT: publish branch returned above"),
    }
}

fn worker_loop(
    ring: IoUring,
    receiver: &mpsc::Receiver<Command>,
    counters: &Counters,
    ring_entries: usize,
    wake: &WakeSignal,
) {
    RingWorker {
        ring,
        receiver,
        counters,
        ring_entries,
        wake,
        ready: VecDeque::with_capacity(ring_entries),
        submitted: HashMap::with_capacity(ring_entries),
        completion_scratch: Vec::with_capacity(ring_entries),
        roots: RootCohort::default(),
        next_user_data: 1,
        wake_submitted: false,
        shutdown: false,
    }
    .run();
}

struct RingWorker<'a> {
    ring: IoUring,
    receiver: &'a mpsc::Receiver<Command>,
    counters: &'a Counters,
    ring_entries: usize,
    wake: &'a WakeSignal,
    ready: VecDeque<Operation>,
    submitted: HashMap<u64, Operation>,
    completion_scratch: Vec<(u64, i32)>,
    roots: RootCohort,
    next_user_data: u64,
    wake_submitted: bool,
    shutdown: bool,
}

impl RingWorker<'_> {
    fn run(mut self) {
        loop {
            if let Err(error) = self.reap_completions() {
                self.fail(&error);
                return;
            }
            self.admit_commands();
            if let Some(root_sync) = self.roots.take_operation(self.counters) {
                self.ready.push_back(root_sync);
            }
            if self.finished() {
                return;
            }
            self.arm_wakeup();
            self.submit_ready();
            if let Err(error) = self.ring.submit_and_wait(1)
                && error.kind() != io::ErrorKind::Interrupted
            {
                self.fail(&error);
                return;
            }
        }
    }

    fn reap_completions(&mut self) -> io::Result<()> {
        self.completion_scratch.clear();
        {
            let mut completion = self.ring.completion();
            self.completion_scratch.extend(
                completion
                    .by_ref()
                    .map(|entry| (entry.user_data(), entry.result())),
            );
        }
        assert!(
            self.completion_scratch.capacity() >= self.ring_entries,
            "ASSERT: CQE scratch retains its ring-sized allocation"
        );
        for index in 0..self.completion_scratch.len() {
            let (user_data, result) = self.completion_scratch[index];
            if user_data == WAKE_USER_DATA {
                self.wake_submitted = false;
                self.wake.drain()?;
                continue;
            }
            let operation = self
                .submitted
                .remove(&user_data)
                .expect("ASSERT: every operation CQE names one submitted operation");
            self.counters
                .worker
                .completed
                .fetch_add(1, Ordering::Relaxed);
            handle_operation_completion(
                operation.complete(result, self.counters),
                &mut self.ready,
                &mut self.roots,
            );
        }
        Ok(())
    }

    fn admit_commands(&mut self) {
        while !self.shutdown {
            match self.receiver.try_recv() {
                Ok(Command::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => {
                    self.shutdown = true;
                }
                Ok(command) => admit_command(command, &mut self.ready, &mut self.roots),
                Err(mpsc::TryRecvError::Empty) => break,
            }
        }
    }

    fn finished(&self) -> bool {
        self.shutdown && self.ready.is_empty() && self.submitted.is_empty()
    }

    fn arm_wakeup(&mut self) {
        if !self.shutdown && !self.wake_submitted && self.submitted.len() < self.ring_entries {
            let entry = opcode::PollAdd::new(
                types::Fd(self.wake.file.as_raw_fd()),
                u32::try_from(libc::POLLIN).expect("ASSERT: POLLIN fits u32"),
            )
            .build()
            .user_data(WAKE_USER_DATA);
            let mut submission = self.ring.submission();
            // SAFETY: PollAdd stores only the eventfd number. `wake` owns that
            // descriptor until the worker exits and this CQE has completed.
            unsafe {
                submission
                    .push(&entry)
                    .expect("ASSERT: reserved wake entry fits the ring");
            }
            self.wake_submitted = true;
        }
    }

    fn submit_ready(&mut self) {
        while self.submitted.len() + usize::from(self.wake_submitted) < self.ring_entries {
            let Some(mut operation) = self.ready.pop_front() else {
                break;
            };
            let user_data = next_operation_token(&mut self.next_user_data, &self.submitted);
            let entry = match operation.entry(user_data) {
                Ok(entry) => entry,
                Err(error) => {
                    operation.reply_error(&error);
                    continue;
                }
            };
            {
                let mut submission = self.ring.submission();
                // SAFETY: every pointer in the SQE refers to an allocation
                // owned by `operation`. Moving the enum into `submitted` does
                // not move Vec or CString allocations, and the map retains the
                // operation unchanged until its matching CQE is consumed.
                unsafe {
                    submission
                        .push(&entry)
                        .expect("ASSERT: preflighted operation fits the ring");
                }
            }
            let replaced = self.submitted.insert(user_data, operation);
            assert!(replaced.is_none(), "ASSERT: operation token is unique");
            self.counters
                .worker
                .submitted
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn fail(&mut self, error: &io::Error) {
        fail_worker(
            error,
            self.receiver,
            &mut self.ready,
            &mut self.submitted,
            &mut self.roots,
        );
    }
}

#[derive(Default)]
struct RootCohort {
    directory: Option<File>,
    replies: Vec<mpsc::SyncSender<io::Result<()>>>,
    publications: Vec<RootPublication>,
}

impl RootCohort {
    fn add_sync(&mut self, directory: File, reply: mpsc::SyncSender<io::Result<()>>) {
        self.directory.get_or_insert(directory);
        self.replies.push(reply);
    }

    fn add_publication(&mut self, publication: PublishReady) {
        self.directory.get_or_insert(publication.directory);
        self.publications.push(RootPublication {
            verified: publication.verified,
            reply: publication.reply,
        });
    }

    fn take_operation(&mut self, counters: &Counters) -> Option<Operation> {
        let directory = self.directory.take()?;
        assert!(
            !self.replies.is_empty() || !self.publications.is_empty(),
            "ASSERT: a root-sync cohort has at least one caller"
        );
        counters
            .worker
            .root_sync_submissions
            .fetch_add(1, Ordering::Relaxed);
        Some(Operation::RootSync {
            directory,
            replies: std::mem::take(&mut self.replies),
            publications: std::mem::take(&mut self.publications),
        })
    }

    fn fail(&mut self, error: &io::Error) {
        let kind = error.kind();
        let message = error.to_string();
        for reply in self.replies.drain(..) {
            let _ = reply.send(Err(io::Error::new(kind, message.clone())));
        }
        for publication in self.publications.drain(..) {
            let _ = publication
                .reply
                .send(Err(StoreError::Io(io::Error::new(kind, message.clone()))));
        }
        self.directory = None;
    }
}

fn admit_command(command: Command, ready: &mut VecDeque<Operation>, roots: &mut RootCohort) {
    match command {
        Command::PublishOwned {
            target,
            publication,
            lease,
            reply,
        } => ready.push_back(Operation::PublishOwned(Box::new(PublishOperation::new(
            target,
            *publication,
            lease,
            reply,
        )))),
        Command::Write {
            file,
            offset,
            bytes,
            lease,
            reply,
        } => ready.push_back(Operation::Write {
            file,
            offset,
            bytes,
            progress: 0,
            _lease: lease,
            reply,
        }),
        Command::Read {
            file,
            offset,
            bytes,
            lease,
            reply,
        } => ready.push_back(Operation::Read {
            file,
            offset,
            bytes,
            progress: 0,
            _lease: lease,
            reply,
        }),
        Command::Fsync { file, reply } => ready.push_back(Operation::Fsync { file, reply }),
        Command::Rename {
            directory,
            old_name,
            new_name,
            reply,
        } => ready.push_back(Operation::Rename {
            directory,
            old_name,
            new_name,
            reply,
        }),
        Command::SyncRoot { directory, reply } => roots.add_sync(directory, reply),
        Command::Shutdown => unreachable!("ASSERT: shutdown is handled by the worker loop"),
    }
}

fn handle_operation_completion(
    completion: OperationCompletion,
    ready: &mut VecDeque<Operation>,
    roots: &mut RootCohort,
) {
    match completion {
        OperationCompletion::Pending(operation) => ready.push_back(operation),
        OperationCompletion::ReadyRoot(publication) => roots.add_publication(publication),
        OperationCompletion::Done => {}
    }
}

fn next_operation_token(next: &mut u64, submitted: &HashMap<u64, Operation>) -> u64 {
    loop {
        let candidate = *next;
        *next = if candidate == WAKE_USER_DATA - 1 {
            1
        } else {
            candidate + 1
        };
        if !submitted.contains_key(&candidate) {
            return candidate;
        }
    }
}

fn fail_worker(
    error: &io::Error,
    receiver: &mpsc::Receiver<Command>,
    ready: &mut VecDeque<Operation>,
    submitted: &mut HashMap<u64, Operation>,
    roots: &mut RootCohort,
) {
    for operation in ready {
        operation.reply_error(error);
    }
    for operation in submitted.values_mut() {
        operation.reply_error(error);
    }
    roots.fail(error);
    while let Ok(command) = receiver.try_recv() {
        if matches!(command, Command::Shutdown) {
            continue;
        }
        let mut queued = VecDeque::new();
        let mut queued_roots = RootCohort::default();
        admit_command(command, &mut queued, &mut queued_roots);
        for operation in &mut queued {
            operation.reply_error(error);
        }
        queued_roots.fail(error);
    }
}

fn write_entry(file: &File, bytes: &[u8], offset: usize) -> io::Result<squeue::Entry> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| invalid_input("one owned-publisher write exceeds u32"))?;
    let offset = u64::try_from(offset)
        .map_err(|_| invalid_input("owned-publisher write offset does not fit u64"))?;
    Ok(
        opcode::Write::new(types::Fd(file.as_raw_fd()), bytes.as_ptr(), length)
            .offset(offset)
            .build(),
    )
}

fn read_entry(file: &File, bytes: &mut [u8], offset: usize) -> io::Result<squeue::Entry> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| invalid_input("one owned-publisher read exceeds u32"))?;
    let offset = u64::try_from(offset)
        .map_err(|_| invalid_input("owned-publisher read offset does not fit u64"))?;
    Ok(
        opcode::Read::new(types::Fd(file.as_raw_fd()), bytes.as_mut_ptr(), length)
            .offset(offset)
            .build(),
    )
}

fn advance_write(
    progress: &mut usize,
    total: usize,
    transferred: usize,
    reply: &mut Option<mpsc::SyncSender<Result<VerifiedContainerPublication, StoreError>>>,
) -> bool {
    let remaining = total
        .checked_sub(*progress)
        .expect("ASSERT: owned-publisher write progress remains in range");
    assert!(
        transferred <= remaining,
        "ASSERT: owned-publisher write CQE cannot exceed its request"
    );
    if transferred == 0 && remaining != 0 {
        if let Some(reply) = reply.take() {
            let _ = reply.send(Err(StoreError::Io(io::Error::from(
                io::ErrorKind::WriteZero,
            ))));
        }
        return false;
    }
    *progress = progress
        .checked_add(transferred)
        .expect("ASSERT: bounded write progress cannot overflow");
    true
}

fn receive_reply<T>(receiver: &mpsc::Receiver<io::Result<T>>) -> io::Result<T> {
    receiver.recv().map_err(|_| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "io_uring worker stopped before replying",
        )
    })?
}

fn receive_store_reply(
    receiver: &mpsc::Receiver<Result<VerifiedContainerPublication, StoreError>>,
) -> Result<VerifiedContainerPublication, StoreError> {
    receiver.recv().map_err(|_| {
        StoreError::Io(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "io_uring worker stopped before completing owned publication",
        ))
    })?
}

fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        return Err(invalid_input("storage name is not one path component"));
    }
    Ok(())
}

fn c_name(name: &str) -> io::Result<CString> {
    validate_name(name)?;
    CString::new(name).map_err(|_| invalid_input("storage name contains a NUL byte"))
}

fn usize_from_u64(value: u64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid_input("object length does not fit usize"))
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod read_completion_tests {
    use super::*;

    #[test]
    fn partial_read_cqes_expose_bytes_only_after_full_success_and_release_errors() {
        let budget = Arc::new(InflightBudget::new(8));
        let counters = Counters::default();
        for failure in [None, Some(0), Some(-libc::EIO)] {
            let (reply, receive) = mpsc::sync_channel(1);
            let mut bytes = ReadBuffer::new(8).unwrap();
            bytes.write_fixture(0, b"abc");
            let operation = Operation::Read {
                file: Arc::new(File::open("/dev/null").unwrap()),
                offset: 0,
                bytes,
                progress: 0,
                _lease: budget.acquire(8).unwrap(),
                reply,
            };
            let OperationCompletion::Pending(mut operation) =
                complete_storage_operation(operation, 3, &counters)
            else {
                panic!("partial CQE must retain buffer ownership");
            };
            assert!(matches!(receive.try_recv(), Err(mpsc::TryRecvError::Empty)));
            assert_eq!(budget.used.load(Ordering::Relaxed), 8);
            let completion = failure.unwrap_or(5);
            if failure.is_none() {
                let Operation::Read { bytes, .. } = &mut operation else {
                    unreachable!()
                };
                bytes.write_fixture(3, b"defgh");
            }
            assert!(matches!(
                complete_storage_operation(operation, completion, &counters),
                OperationCompletion::Done
            ));
            let result = receive.recv().unwrap();
            match failure {
                None => assert_eq!(result.unwrap(), b"abcdefgh"),
                Some(0) => assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof),
                Some(_) => assert_eq!(
                    result.unwrap_err().kind(),
                    io::Error::from_raw_os_error(libc::EIO).kind()
                ),
            }
            assert_eq!(budget.used.load(Ordering::Relaxed), 0);
        }
    }
}
