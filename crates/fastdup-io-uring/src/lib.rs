//! Linux `io_uring` adapter for fastdup's synchronous durable storage seam.
//!
//! One shared worker owns the ring. Callers keep the existing blocking
//! [`StorageIo`] contract, while operations from independent Container
//! publishers can overlap in the kernel. Buffer ownership and the only unsafe
//! submission call are confined to this platform crate.

use std::ffi::CString;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use fastdup_format::{
    FOOTER_BYTES, HEADER_BYTES, MAX_CONTAINER_BYTES, SealedContainer, SealedContainerDescriptor,
};
use fastdup_store::{
    FsStorageIo, MAX_STORAGE_RANGE_BYTES, OwnedContainerPublication, StorageIo, StoreError,
};
use io_uring::{IoUring, opcode, squeue, types};

mod verification_pool;

use verification_pool::{VerificationPool, VerificationRequest};

const DEFAULT_RING_ENTRIES: u32 = 256;
const DEFAULT_INFLIGHT_BYTES: u64 = 256 * 1_024 * 1_024;
const MIN_POOLED_VERIFICATION_BYTES: usize = 1_024 * 1_024;
const ROOT_SYNC_COHORT_DELAY: Duration = Duration::from_micros(200);
const OWNED_PUBLISH_COHORT_DELAY: Duration = Duration::from_micros(100);

/// Bounded shared-ring configuration for the data-tier adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringStorageConfig {
    ring_entries: NonZeroU32,
    max_inflight_bytes: NonZeroU64,
    verifier_workers: NonZeroUsize,
}

impl IoUringStorageConfig {
    #[must_use]
    pub fn new(ring_entries: NonZeroU32, max_inflight_bytes: NonZeroU64) -> Self {
        Self {
            ring_entries,
            max_inflight_bytes,
            verifier_workers: default_verifier_workers(),
        }
    }

    #[must_use]
    pub const fn with_verifier_workers(mut self, verifier_workers: NonZeroUsize) -> Self {
        self.verifier_workers = verifier_workers;
        self
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
    pub const fn verifier_workers(self) -> NonZeroUsize {
        self.verifier_workers
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

fn default_verifier_workers() -> NonZeroUsize {
    thread::available_parallelism().unwrap_or(NonZeroUsize::MIN)
}

/// Kernel-I/O mode selected when the adapter was opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoUringStorageMode {
    Active,
    Synchronous,
    SyncFallback,
}

/// Point-in-time boundedness and batching telemetry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoUringStorageStatus {
    mode: IoUringStorageMode,
    fallback_reason: Option<String>,
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
    verifier_workers: usize,
    verification_jobs_started: u64,
    verification_jobs_completed: u64,
    verification_jobs_failed: u64,
    active_verifications: u64,
    peak_active_verifications: u64,
}

impl IoUringStorageStatus {
    #[must_use]
    pub const fn mode(&self) -> IoUringStorageMode {
        self.mode
    }

    #[must_use]
    pub fn fallback_reason(&self) -> Option<&str> {
        self.fallback_reason.as_deref()
    }

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
    pub const fn verifier_workers(&self) -> usize {
        self.verifier_workers
    }

    #[must_use]
    pub const fn verification_jobs_started(&self) -> u64 {
        self.verification_jobs_started
    }

    #[must_use]
    pub const fn verification_jobs_completed(&self) -> u64 {
        self.verification_jobs_completed
    }

    #[must_use]
    pub const fn verification_jobs_failed(&self) -> u64 {
        self.verification_jobs_failed
    }

    #[must_use]
    pub const fn active_verifications(&self) -> u64 {
        self.active_verifications
    }

    #[must_use]
    pub const fn peak_active_verifications(&self) -> u64 {
        self.peak_active_verifications
    }
}

/// Cloneable data-tier storage adapter backed by one shared bounded ring.
pub struct IoUringStorageIo {
    filesystem: FsStorageIo,
    backend: Arc<Backend>,
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
    /// Opens the root and requires a functioning ring.
    ///
    /// # Errors
    ///
    /// Returns root initialization, ring setup, or worker spawn errors.
    pub fn open_required(root: impl AsRef<Path>, config: IoUringStorageConfig) -> io::Result<Self> {
        let filesystem = FsStorageIo::open(root)?;
        let active = ActiveBackend::start(config)?;
        Ok(Self {
            filesystem,
            backend: Arc::new(Backend::Active(active)),
            config,
        })
    }

    /// Opens the root and falls back to the proven synchronous adapter when
    /// ring creation is unavailable.
    ///
    /// # Errors
    ///
    /// Returns only root initialization errors. Ring setup failure is exposed
    /// through [`Self::status`].
    pub fn open_or_fallback(
        root: impl AsRef<Path>,
        config: IoUringStorageConfig,
    ) -> io::Result<Self> {
        let filesystem = FsStorageIo::open(root)?;
        let backend = match ActiveBackend::start(config) {
            Ok(active) => Backend::Active(active),
            Err(error) => Backend::Fallback(error.to_string()),
        };
        Ok(Self {
            filesystem,
            backend: Arc::new(backend),
            config,
        })
    }

    /// Opens the root with the proven synchronous adapter selected by policy.
    ///
    /// This is distinct from fallback after a failed ring setup, allowing
    /// telemetry to distinguish an intentional benchmark decision from a host
    /// capability failure.
    ///
    /// # Errors
    ///
    /// Returns root initialization errors.
    pub fn open_synchronous(
        root: impl AsRef<Path>,
        config: IoUringStorageConfig,
    ) -> io::Result<Self> {
        Ok(Self {
            filesystem: FsStorageIo::open(root)?,
            backend: Arc::new(Backend::Synchronous),
            config,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.filesystem.root()
    }

    #[must_use]
    pub fn status(&self) -> IoUringStorageStatus {
        match self.backend.as_ref() {
            Backend::Active(active) => active.status(self.config),
            Backend::Synchronous | Backend::Fallback(_) => IoUringStorageStatus {
                mode: if matches!(self.backend.as_ref(), Backend::Synchronous) {
                    IoUringStorageMode::Synchronous
                } else {
                    IoUringStorageMode::SyncFallback
                },
                fallback_reason: match self.backend.as_ref() {
                    Backend::Fallback(reason) => Some(reason.clone()),
                    Backend::Active(_) | Backend::Synchronous => None,
                },
                ring_entries: self.config.ring_entries.get(),
                max_inflight_bytes: self.config.max_inflight_bytes.get(),
                inflight_bytes: 0,
                peak_inflight_bytes: 0,
                submitted_operations: 0,
                completed_operations: 0,
                root_sync_callers: 0,
                root_sync_submissions: 0,
                owned_publications_started: 0,
                owned_publications_completed: 0,
                borrowed_write_copy_bytes: 0,
                verifier_workers: self.config.verifier_workers.get(),
                verification_jobs_started: 0,
                verification_jobs_completed: 0,
                verification_jobs_failed: 0,
                active_verifications: 0,
                peak_active_verifications: 0,
            },
        }
    }

    fn path(&self, name: &str) -> io::Result<PathBuf> {
        validate_name(name)?;
        Ok(self.filesystem.root().join(name))
    }

    fn active(&self) -> Option<&ActiveBackend> {
        match self.backend.as_ref() {
            Backend::Active(active) => Some(active),
            Backend::Synchronous | Backend::Fallback(_) => None,
        }
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
        let Some(active) = self.active() else {
            return self.filesystem.write_at(name, offset, bytes);
        };
        let byte_length = u64::try_from(bytes.len())
            .map_err(|_| invalid_input("write length does not fit u64"))?;
        offset
            .checked_add(byte_length)
            .ok_or_else(|| invalid_input("write range overflows"))?;
        let lease = active.budget.acquire(byte_length)?;
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        owned.extend_from_slice(bytes);
        active
            .counters
            .callers
            .borrowed_write_copy_bytes
            .fetch_add(byte_length, Ordering::Relaxed);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.path(name)?)?;
        active.write(file, offset, owned, lease)
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let Some(active) = self.active() else {
            return self.filesystem.read(name);
        };
        let file = File::open(self.path(name)?)?;
        let length = file.metadata()?.len();
        if length > MAX_CONTAINER_BYTES {
            return Err(invalid_data("container exceeds the format-v1 hard limit"));
        }
        active.read(file, 0, usize_from_u64(length)?)
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
        let length_u64 =
            u64::try_from(length).map_err(|_| invalid_input("read length does not fit u64"))?;
        let end = offset
            .checked_add(length_u64)
            .ok_or_else(|| invalid_input("read range overflows"))?;
        let Some(active) = self.active() else {
            return self.filesystem.read_exact_at(name, offset, length);
        };
        let file = File::open(self.path(name)?)?;
        if end > file.metadata()?.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "bounded storage read exceeds the current object length",
            ));
        }
        active.read(file, offset, length)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        self.filesystem.list_names()
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.filesystem.set_len(name, length)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        let Some(active) = self.active() else {
            return self.filesystem.sync_file(name);
        };
        active.fsync(File::open(self.path(name)?)?)
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        let Some(active) = self.active() else {
            return self
                .filesystem
                .publish_noreplace(temporary_name, published_name);
        };
        let old_name = c_name(temporary_name)?;
        let new_name = c_name(published_name)?;
        let directory = File::open(self.filesystem.root())?;
        active.rename_noreplace(directory, old_name, new_name)
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.filesystem.remove_file(name)
    }

    fn sync_root(&self) -> io::Result<()> {
        let Some(active) = self.active() else {
            return self.filesystem.sync_root();
        };
        active.sync_root(File::open(self.filesystem.root())?)
    }

    fn publish_owned_container(
        &self,
        publication: OwnedContainerPublication,
    ) -> Result<SealedContainer, StoreError> {
        let Some(active) = self.active() else {
            return self.filesystem.publish_owned_container(publication);
        };
        let lease = active.acquire_publication(&publication)?;
        let temporary_name = publication.temporary_name().to_owned();
        let published_name = publication.published_name().to_owned();
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(self.path(&temporary_name)?)?;
        let directory = File::open(self.filesystem.root())?;
        active.publish_owned(
            file,
            directory,
            c_name(&temporary_name)?,
            c_name(&published_name)?,
            publication,
            lease,
        )
    }
}

enum Backend {
    Active(ActiveBackend),
    Synchronous,
    Fallback(String),
}

struct ActiveBackend {
    sender: mpsc::SyncSender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    budget: Arc<InflightBudget>,
    counters: Arc<Counters>,
}

impl ActiveBackend {
    fn start(config: IoUringStorageConfig) -> io::Result<Self> {
        let ring = IoUring::new(config.ring_entries.get())?;
        let queue_capacity = usize::try_from(config.ring_entries.get())
            .expect("ASSERT: u32 ring entry count fits usize")
            .checked_mul(2)
            .expect("ASSERT: bounded ring queue capacity cannot overflow");
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let budget = Arc::new(InflightBudget::new(config.max_inflight_bytes.get()));
        let counters = Arc::new(Counters::default());
        let worker_counters = Arc::clone(&counters);
        let entries = usize::try_from(config.ring_entries.get())
            .expect("ASSERT: u32 ring entry count fits usize");
        let verifier_pool =
            VerificationPool::start(config.verifier_workers, entries, &worker_counters)?;
        let worker = thread::Builder::new()
            .name("fastdup-io-uring".to_owned())
            .spawn(move || {
                worker_loop(ring, &receiver, &worker_counters, entries, verifier_pool);
            })?;
        Ok(Self {
            sender,
            worker: Mutex::new(Some(worker)),
            budget,
            counters,
        })
    }

    fn status(&self, config: IoUringStorageConfig) -> IoUringStorageStatus {
        IoUringStorageStatus {
            mode: IoUringStorageMode::Active,
            fallback_reason: None,
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
            verifier_workers: config.verifier_workers.get(),
            verification_jobs_started: self.counters.verifier.jobs_started.load(Ordering::Relaxed),
            verification_jobs_completed: self
                .counters
                .verifier
                .jobs_completed
                .load(Ordering::Relaxed),
            verification_jobs_failed: self.counters.verifier.jobs_failed.load(Ordering::Relaxed),
            active_verifications: self.counters.verifier.active.load(Ordering::Relaxed),
            peak_active_verifications: self.counters.verifier.peak_active.load(Ordering::Relaxed),
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

    fn read(&self, file: File, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let length_u64 =
            u64::try_from(length).map_err(|_| invalid_input("read length does not fit u64"))?;
        let lease = self.budget.acquire(length_u64)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        bytes.resize(length, 0);
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
        file: File,
        directory: File,
        old_name: CString,
        new_name: CString,
        publication: OwnedContainerPublication,
        lease: BudgetLease,
    ) -> Result<SealedContainer, StoreError> {
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
            file,
            directory,
            old_name,
            new_name,
            publication,
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
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring worker stopped"))
    }
}

impl Drop for ActiveBackend {
    fn drop(&mut self) {
        let _ = self.sender.send(Command::Shutdown);
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

#[derive(Default)]
struct Counters {
    worker: WorkerCounters,
    callers: CallerCounters,
    verifier: VerifierCounters,
}

#[derive(Default)]
#[repr(C, align(64))]
struct WorkerCounters {
    submitted: AtomicU64,
    completed: AtomicU64,
    root_sync_submissions: AtomicU64,
    owned_publications_completed: AtomicU64,
}

#[derive(Default)]
#[repr(C, align(64))]
struct CallerCounters {
    root_sync: AtomicU64,
    owned_publications_started: AtomicU64,
    borrowed_write_copy_bytes: AtomicU64,
}

#[derive(Default)]
#[repr(C, align(64))]
struct VerifierCounters {
    jobs_started: AtomicU64,
    jobs_completed: AtomicU64,
    jobs_failed: AtomicU64,
    active: AtomicU64,
    peak_active: AtomicU64,
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
        file: File,
        directory: File,
        old_name: CString,
        new_name: CString,
        publication: OwnedContainerPublication,
        lease: BudgetLease,
        reply: mpsc::SyncSender<Result<SealedContainer, StoreError>>,
    },
    Write {
        file: File,
        offset: u64,
        bytes: Vec<u8>,
        lease: BudgetLease,
        reply: mpsc::SyncSender<io::Result<()>>,
    },
    Read {
        file: File,
        offset: u64,
        bytes: Vec<u8>,
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

enum PublishPhase {
    Building { progress: usize },
    Body { progress: usize },
    SealedHeader { progress: usize },
    Reread { bytes: Vec<u8>, progress: usize },
    AwaitVerification,
    FileSync,
    Rename,
}

struct PublishOperation {
    file: File,
    directory: Option<File>,
    old_name: CString,
    new_name: CString,
    container_id: fastdup_format::ContainerId,
    container_generation: u64,
    expected_container_hash: [u8; 32],
    sealed_length: usize,
    building_header: Box<[u8; HEADER_BYTES]>,
    sealed: Vec<u8>,
    phase: PublishPhase,
    verified: Option<SealedContainer>,
    lease: Option<BudgetLease>,
    reply: Option<mpsc::SyncSender<Result<SealedContainer, StoreError>>>,
}

impl PublishOperation {
    fn new(
        file: File,
        directory: File,
        old_name: CString,
        new_name: CString,
        publication: OwnedContainerPublication,
        lease: BudgetLease,
        reply: mpsc::SyncSender<Result<SealedContainer, StoreError>>,
    ) -> Self {
        let (
            container_id,
            container_generation,
            building_header,
            sealed,
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
        let footer_bytes =
            usize::try_from(FOOTER_BYTES).expect("ASSERT: format-v1 Footer size fits usize");
        let footer_offset = sealed
            .len()
            .checked_sub(footer_bytes)
            .expect("ASSERT: a sealed Container contains its Footer");
        let sealed_length = sealed.len();
        let expected_container_hash = SealedContainerDescriptor::decode(
            &sealed[..HEADER_BYTES],
            &sealed[footer_offset..],
            u64::try_from(sealed_length).expect("ASSERT: bounded Container length fits u64"),
        )
        .expect("ASSERT: the format writer produced a valid Container envelope")
        .container_hash();
        Self {
            file,
            directory: Some(directory),
            old_name,
            new_name,
            container_id,
            container_generation,
            expected_container_hash,
            sealed_length,
            building_header,
            sealed,
            phase: PublishPhase::Building { progress: 0 },
            verified: None,
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
            PublishPhase::Reread { bytes, progress } => {
                read_entry(&self.file, &mut bytes[*progress..], *progress)
            }
            PublishPhase::AwaitVerification => {
                unreachable!("ASSERT: verification owns no kernel operation")
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

    fn complete(mut self, result: i32) -> OperationCompletion {
        if result < 0 {
            self.fail(StoreError::Io(io::Error::from_raw_os_error(-result)));
            return OperationCompletion::Done;
        }
        let transferred = usize::try_from(result)
            .expect("ASSERT: nonnegative owned-publisher CQE result fits usize");
        match std::mem::replace(&mut self.phase, PublishPhase::FileSync) {
            PublishPhase::Building { progress } => self.complete_building(progress, transferred),
            PublishPhase::Body { progress } => self.complete_body(progress, transferred),
            PublishPhase::SealedHeader { progress } => {
                self.complete_sealed_header(progress, transferred)
            }
            PublishPhase::Reread { bytes, progress } => {
                self.complete_reread(bytes, progress, transferred)
            }
            PublishPhase::AwaitVerification => {
                unreachable!("ASSERT: verification owns no CQE")
            }
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
                        .expect("ASSERT: rename follows complete writer verification"),
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
        let sealed_length =
            u64::try_from(self.sealed_length).expect("ASSERT: bounded Container length fits u64");
        if let Err(error) = self.file.set_len(sealed_length) {
            self.fail(StoreError::Io(error));
            return OperationCompletion::Done;
        }
        self.sealed = Vec::new();
        let bytes = match allocate_reread_buffer(self.sealed_length) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.fail(error);
                return OperationCompletion::Done;
            }
        };
        self.phase = PublishPhase::Reread { bytes, progress: 0 };
        self.pending()
    }

    fn complete_reread(
        mut self,
        bytes: Vec<u8>,
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
            self.phase = PublishPhase::Reread { bytes, progress };
            return self.pending();
        }
        self.phase = PublishPhase::AwaitVerification;
        OperationCompletion::NeedsVerification(PendingVerification {
            operation: Box::new(self),
            bytes,
        })
    }

    fn finish_verification(
        mut self,
        verified: Result<SealedContainer, StoreError>,
    ) -> OperationCompletion {
        assert!(
            matches!(self.phase, PublishPhase::AwaitVerification),
            "ASSERT: only a completed writer reread may enter CPU verification"
        );
        match verified {
            Ok(verified) => {
                self.lease = None;
                self.verified = Some(verified);
                self.phase = PublishPhase::FileSync;
                self.pending()
            }
            Err(error) => {
                self.fail(error);
                OperationCompletion::Done
            }
        }
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

fn allocate_reread_buffer(length: usize) -> Result<Vec<u8>, StoreError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| StoreError::Io(io::Error::from(io::ErrorKind::OutOfMemory)))?;
    bytes.resize(length, 0);
    Ok(bytes)
}

fn verify_owned_reread(
    reread: &[u8],
    expected_container_hash: [u8; 32],
    container_id: fastdup_format::ContainerId,
    container_generation: u64,
) -> Result<SealedContainer, StoreError> {
    let verified = SealedContainer::decode(reread)?;
    let footer_bytes =
        usize::try_from(FOOTER_BYTES).expect("ASSERT: format-v1 Footer size fits usize");
    let footer_offset = reread
        .len()
        .checked_sub(footer_bytes)
        .ok_or(StoreError::PublishVerificationMismatch)?;
    let descriptor = SealedContainerDescriptor::decode(
        &reread[..HEADER_BYTES],
        &reread[footer_offset..],
        u64::try_from(reread.len()).map_err(|_| StoreError::PublishVerificationMismatch)?,
    )?;
    if descriptor.container_hash() != expected_container_hash
        || verified.header().container_id() != container_id
        || verified.header().container_generation() != container_generation
    {
        return Err(StoreError::PublishVerificationMismatch);
    }
    Ok(verified)
}

struct PublishReady {
    directory: File,
    verified: SealedContainer,
    reply: mpsc::SyncSender<Result<SealedContainer, StoreError>>,
}

struct RootPublication {
    verified: SealedContainer,
    reply: mpsc::SyncSender<Result<SealedContainer, StoreError>>,
}

struct PendingVerification {
    operation: Box<PublishOperation>,
    bytes: Vec<u8>,
}

enum OperationCompletion {
    Pending(Operation),
    NeedsVerification(PendingVerification),
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
        file: File,
        offset: u64,
        bytes: Vec<u8>,
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
                let remaining = &mut bytes[*progress..];
                let length = u32::try_from(remaining.len())
                    .map_err(|_| invalid_input("one io_uring read exceeds u32"))?;
                let operation_offset = offset
                    .checked_add(u64::try_from(*progress).expect("ASSERT: usize fits u64"))
                    .ok_or_else(|| invalid_input("read progress overflows offset"))?;
                opcode::Read::new(types::Fd(file.as_raw_fd()), remaining.as_mut_ptr(), length)
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
            Self::PublishOwned(operation) => (*operation).complete(result),
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
                let finished = std::mem::take(bytes);
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
    mut ring: IoUring,
    receiver: &mpsc::Receiver<Command>,
    counters: &Counters,
    ring_entries: usize,
    mut verifier_pool: VerificationPool,
) {
    while let Ok(first) = receiver.recv() {
        if matches!(first, Command::Shutdown) {
            break;
        }
        let first_is_root = matches!(first, Command::SyncRoot { .. });
        let first_is_owned_publish = matches!(first, Command::PublishOwned { .. });
        let mut commands = Vec::with_capacity(ring_entries);
        commands.push(first);
        if first_is_root || first_is_owned_publish {
            let delay = if first_is_root {
                ROOT_SYNC_COHORT_DELAY
            } else {
                OWNED_PUBLISH_COHORT_DELAY
            };
            let deadline = Instant::now() + delay;
            while commands.len() < ring_entries {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                match receiver.recv_timeout(deadline - now) {
                    Ok(Command::Shutdown) => break,
                    Ok(command) => commands.push(command),
                    Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
                        break;
                    }
                }
            }
        } else {
            while commands.len() < ring_entries {
                match receiver.try_recv() {
                    Ok(Command::Shutdown)
                    | Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
                    Ok(command) => commands.push(command),
                }
            }
        }
        let operations = coalesce_commands(commands, counters);
        execute_operations(&mut ring, operations, counters, &mut verifier_pool);
    }
}

fn coalesce_commands(commands: Vec<Command>, counters: &Counters) -> Vec<Operation> {
    let mut operations = Vec::with_capacity(commands.len());
    let mut root_directory = None;
    let mut root_replies = Vec::new();
    for command in commands {
        match command {
            Command::PublishOwned {
                file,
                directory,
                old_name,
                new_name,
                publication,
                lease,
                reply,
            } => operations.push(Operation::PublishOwned(Box::new(PublishOperation::new(
                file,
                directory,
                old_name,
                new_name,
                publication,
                lease,
                reply,
            )))),
            Command::Write {
                file,
                offset,
                bytes,
                lease,
                reply,
            } => operations.push(Operation::Write {
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
            } => operations.push(Operation::Read {
                file,
                offset,
                bytes,
                progress: 0,
                _lease: lease,
                reply,
            }),
            Command::Fsync { file, reply } => operations.push(Operation::Fsync { file, reply }),
            Command::Rename {
                directory,
                old_name,
                new_name,
                reply,
            } => operations.push(Operation::Rename {
                directory,
                old_name,
                new_name,
                reply,
            }),
            Command::SyncRoot { directory, reply } => {
                root_directory.get_or_insert(directory);
                root_replies.push(reply);
            }
            Command::Shutdown => {}
        }
    }
    if let Some(directory) = root_directory {
        assert!(
            !root_replies.is_empty(),
            "ASSERT: a root-sync cohort has at least one caller"
        );
        counters
            .worker
            .root_sync_submissions
            .fetch_add(1, Ordering::Relaxed);
        operations.push(Operation::RootSync {
            directory,
            replies: root_replies,
            publications: Vec::new(),
        });
    }
    operations
}

fn execute_operations(
    ring: &mut IoUring,
    mut operations: Vec<Operation>,
    counters: &Counters,
    verifier_pool: &mut VerificationPool,
) {
    while !operations.is_empty() {
        let mut entries = Vec::with_capacity(operations.len());
        for (index, operation) in operations.iter_mut().enumerate() {
            match operation.entry(u64::try_from(index).expect("ASSERT: operation index fits u64")) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    operation.reply_error(&error);
                    return;
                }
            }
        }
        {
            let mut submission = ring.submission();
            assert!(
                submission.capacity() >= entries.len(),
                "ASSERT: bounded worker batch fits the configured ring"
            );
            // SAFETY: every SQE points only into its matching `Operation`.
            // `operations`, its Files, Vec allocations, and CStrings remain alive
            // and are not mutated or reallocated until every CQE is consumed.
            unsafe {
                submission
                    .push_multiple(&entries)
                    .expect("ASSERT: preflighted ring capacity accepts the whole batch");
            }
        }
        counters.worker.submitted.fetch_add(
            u64::try_from(entries.len()).expect("ASSERT: batch length fits u64"),
            Ordering::Relaxed,
        );
        if let Err(error) = ring.submit_and_wait(entries.len()) {
            for operation in &mut operations {
                operation.reply_error(&io::Error::new(error.kind(), error.to_string()));
            }
            return;
        }
        let mut results = vec![None; operations.len()];
        let mut completion = ring.completion();
        for entry in &mut completion {
            let index =
                usize::try_from(entry.user_data()).expect("ASSERT: submitted user_data fits usize");
            assert!(
                index < results.len(),
                "ASSERT: CQE index belongs to current batch"
            );
            assert!(
                results[index].is_none(),
                "ASSERT: one CQE exists per submitted SQE"
            );
            results[index] = Some(entry.result());
        }
        drop(completion);
        assert!(
            results.iter().all(Option::is_some),
            "ASSERT: submit_and_wait returned every requested CQE"
        );
        counters.worker.completed.fetch_add(
            u64::try_from(results.len()).expect("ASSERT: completion count fits u64"),
            Ordering::Relaxed,
        );
        let mut pending = Vec::new();
        let mut needs_verification = Vec::new();
        let mut ready_publications = Vec::new();
        for (operation, result) in operations.into_iter().zip(results) {
            match operation.complete(result.expect("ASSERT: CQE exists"), counters) {
                OperationCompletion::Pending(operation) => pending.push(operation),
                OperationCompletion::NeedsVerification(verification) => {
                    needs_verification.push(verification);
                }
                OperationCompletion::ReadyRoot(publication) => {
                    ready_publications.push(publication);
                }
                OperationCompletion::Done => {}
            }
        }
        if !ready_publications.is_empty() {
            pending.push(publication_root_sync(ready_publications, counters));
        }
        if !needs_verification.is_empty() {
            pending.extend(verify_publications(verifier_pool, needs_verification));
        }
        operations = pending;
    }
}

fn verify_publications(
    verifier_pool: &VerificationPool,
    pending: Vec<PendingVerification>,
) -> Vec<Operation> {
    let mut ready = Vec::with_capacity(pending.len());
    let mut pooled = Vec::with_capacity(pending.len());
    for pending in pending {
        if pending.bytes.len() < MIN_POOLED_VERIFICATION_BYTES {
            let verified = verify_owned_reread(
                &pending.bytes,
                pending.operation.expected_container_hash,
                pending.operation.container_id,
                pending.operation.container_generation,
            );
            if let Some(operation) = finish_verified_operation(pending.operation, verified) {
                ready.push(operation);
            }
        } else {
            pooled.push(pending);
        }
    }
    let mut operations = Vec::with_capacity(pooled.len());
    let mut requests = Vec::with_capacity(pooled.len());
    for (ordinal, pending) in pooled.into_iter().enumerate() {
        requests.push(VerificationRequest::new(
            ordinal,
            pending.bytes,
            pending.operation.expected_container_hash,
            pending.operation.container_id,
            pending.operation.container_generation,
        ));
        operations.push(Some(pending.operation));
    }
    for result in verifier_pool.verify_batch(requests) {
        let ordinal = result.ordinal();
        let operation = operations
            .get_mut(ordinal)
            .and_then(Option::take)
            .expect("ASSERT: every verifier result names one unique submitted publication");
        if let Some(operation) = finish_verified_operation(operation, result.into_verified()) {
            ready.push(operation);
        }
    }
    assert!(
        operations.into_iter().all(|operation| operation.is_none()),
        "ASSERT: every submitted publication returned one verifier result"
    );
    ready
}

fn finish_verified_operation(
    operation: Box<PublishOperation>,
    verified: Result<SealedContainer, StoreError>,
) -> Option<Operation> {
    match operation.finish_verification(verified) {
        OperationCompletion::Pending(operation) => Some(operation),
        OperationCompletion::Done => None,
        OperationCompletion::NeedsVerification(_) | OperationCompletion::ReadyRoot(_) => {
            unreachable!("ASSERT: CPU verification transitions only to file sync or failure")
        }
    }
}

fn publication_root_sync(mut ready: Vec<PublishReady>, counters: &Counters) -> Operation {
    let first = ready
        .pop()
        .expect("ASSERT: publication root-sync cohort is nonempty");
    let directory = first.directory;
    let mut publications = Vec::with_capacity(ready.len() + 1);
    publications.push(RootPublication {
        verified: first.verified,
        reply: first.reply,
    });
    publications.extend(ready.into_iter().map(|publication| RootPublication {
        verified: publication.verified,
        reply: publication.reply,
    }));
    counters
        .worker
        .root_sync_submissions
        .fetch_add(1, Ordering::Relaxed);
    Operation::RootSync {
        directory,
        replies: Vec::new(),
        publications,
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
    reply: &mut Option<mpsc::SyncSender<Result<SealedContainer, StoreError>>>,
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
    receiver: &mpsc::Receiver<Result<SealedContainer, StoreError>>,
) -> Result<SealedContainer, StoreError> {
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
