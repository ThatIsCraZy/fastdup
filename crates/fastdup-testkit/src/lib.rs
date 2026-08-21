#![forbid(unsafe_code)]

//! Deterministic corpora and fault-injection adapters for public store seams.

mod corpus;
mod sigkill_harness;

pub use corpus::{ByteMutation, generate_structured_corpus, minimal_variant_plan};
pub use sigkill_harness::{
    SigkillCaseReport, SigkillHarnessError, SigkillRemountConfig, SigkillRemountReport,
};

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use fastdup_store::{MAX_STORAGE_RANGE_BYTES, StorageIo};

/// The operation attempted at one deterministic fault-injection position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageOperation {
    CreateNew,
    Exists,
    WriteAt,
    SetLen,
    Read,
    ObjectLen,
    ReadExactAt,
    ListNames,
    SyncFile,
    PublishNoreplace,
    RemoveFile,
    SyncRoot,
}

/// In-memory storage with independently modeled live and durable state.
///
/// Operation positions are zero based. A configured fault is returned either
/// before that operation changes state or after its complete effect. `crash`
/// discards live file contents and directory entries that were not made durable.
#[derive(Clone, Debug)]
pub struct MemoryStorageIo {
    state: Arc<Mutex<StorageState>>,
}

/// Storage adapter that pauses before one selected operation until resumed.
///
/// This is a scheduling test adapter, not a durability model. All storage
/// semantics remain delegated to the wrapped [`MemoryStorageIo`].
#[derive(Clone, Debug)]
pub struct PausedStorageIo {
    inner: MemoryStorageIo,
    control: Arc<PauseControl>,
}

#[derive(Debug)]
struct PauseControl {
    operation: StorageOperation,
    state: Mutex<PauseState>,
    changed: Condvar,
}

#[derive(Clone, Copy, Debug, Default)]
struct PauseState {
    reached: usize,
    resumed: bool,
}

impl PausedStorageIo {
    #[must_use]
    pub fn before(inner: MemoryStorageIo, operation: StorageOperation) -> Self {
        Self {
            inner,
            control: Arc::new(PauseControl {
                operation,
                state: Mutex::new(PauseState::default()),
                changed: Condvar::new(),
            }),
        }
    }

    #[must_use]
    /// Waits until the selected operation reaches the pause point.
    ///
    /// # Panics
    ///
    /// Panics if an earlier test-thread panic poisoned the pause control.
    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        let state = self
            .control
            .state
            .lock()
            .expect("ASSERT: pause control lock poisoned");
        let (state, _) = self
            .control
            .changed
            .wait_timeout_while(state, timeout, |state| state.reached == 0)
            .expect("ASSERT: pause control lock poisoned while waiting");
        state.reached != 0
    }

    /// Waits until at least `count` callers concurrently reach the pause.
    ///
    /// # Panics
    ///
    /// Panics if an earlier test-thread panic poisoned the pause control.
    #[must_use]
    pub fn wait_until_reached_count(&self, count: usize, timeout: Duration) -> bool {
        let state = self
            .control
            .state
            .lock()
            .expect("ASSERT: pause control lock poisoned");
        let (state, _) = self
            .control
            .changed
            .wait_timeout_while(state, timeout, |state| state.reached < count)
            .expect("ASSERT: pause control lock poisoned while waiting");
        state.reached >= count
    }

    /// Permanently releases the selected operation and all later callers.
    ///
    /// # Panics
    ///
    /// Panics if an earlier test-thread panic poisoned the pause control.
    pub fn resume(&self) {
        let mut state = self
            .control
            .state
            .lock()
            .expect("ASSERT: pause control lock poisoned");
        state.resumed = true;
        self.control.changed.notify_all();
    }

    fn pause_before(&self, operation: StorageOperation) {
        if operation != self.control.operation {
            return;
        }
        let mut state = self
            .control
            .state
            .lock()
            .expect("ASSERT: pause control lock poisoned");
        state.reached = state
            .reached
            .checked_add(1)
            .expect("ASSERT: paused caller count cannot overflow");
        self.control.changed.notify_all();
        while !state.resumed {
            state = self
                .control
                .changed
                .wait(state)
                .expect("ASSERT: pause control lock poisoned while paused");
        }
    }
}

impl MemoryStorageIo {
    #[must_use]
    pub fn new() -> Self {
        Self::configured(None, None, None)
    }

    #[must_use]
    pub fn with_fail_before(operation_position: usize) -> Self {
        Self::configured(Some(operation_position), None, None)
    }

    /// Creates a backend that applies the selected operation completely and
    /// then reports an injected error to its caller.
    #[must_use]
    pub fn with_fail_after(operation_position: usize) -> Self {
        Self::configured(None, Some(operation_position), None)
    }

    /// Creates a backend whose reads return another independently supplied byte
    /// sequence, modeling a wrong-object or misdirected-I/O integrity fault.
    #[must_use]
    pub fn with_read_substitution(bytes: Vec<u8>) -> Self {
        Self::configured(None, None, Some(bytes))
    }

    fn configured(
        fail_before: Option<usize>,
        fail_after: Option<usize>,
        read_substitution: Option<Vec<u8>>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(StorageState {
                fail_before,
                fail_after,
                read_substitution,
                ..StorageState::default()
            })),
        }
    }

    /// Discard every mutation not represented by durable file and directory
    /// state, as a process or appliance crash would.
    pub fn crash(&self) {
        let mut state = self.lock();
        state.live_directory = state.durable_directory.clone();
        let durable_files = state
            .durable_directory
            .values()
            .copied()
            .collect::<BTreeSet<_>>();
        state.files.retain(|id, _| durable_files.contains(id));
        for file in state.files.values_mut() {
            file.live.clone_from(&file.durable);
        }
    }

    #[must_use]
    pub fn operation_count(&self) -> usize {
        self.lock().operations.len()
    }

    #[must_use]
    pub fn operations(&self) -> Vec<StorageOperation> {
        self.lock().operations.clone()
    }

    fn lock(&self) -> MutexGuard<'_, StorageState> {
        self.state
            .lock()
            .expect("ASSERT: fault-model state was poisoned by an earlier invariant failure")
    }
}

impl Default for MemoryStorageIo {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageIo for MemoryStorageIo {
    fn create_new(&self, name: &str) -> io::Result<()> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::CreateNew)?;
        validate_name(name)?;
        if state.live_directory.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "file already exists",
            ));
        }
        let file_id = state.allocate_file_id()?;
        state.files.insert(file_id, MemoryFile::default());
        state.live_directory.insert(name.to_owned(), file_id);
        state.finish(position)
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::Exists)?;
        validate_name(name)?;
        let exists = state.live_directory.contains_key(name);
        state.finish(position)?;
        Ok(exists)
    }

    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::WriteAt)?;
        validate_name(name)?;
        let offset = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset is too large"))?;
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write range overflows"))?;
        let file_id = state.file_id(name)?;
        let file = state
            .files
            .get_mut(&file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file inode does not exist"))?;
        if end > file.live.len() {
            file.live.resize(end, 0);
        }
        file.live[offset..end].copy_from_slice(bytes);
        state.finish(position)
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::Read)?;
        validate_name(name)?;
        let file_id = state.file_id(name)?;
        let bytes = if let Some(substitution) = &state.read_substitution {
            substitution.clone()
        } else {
            state
                .files
                .get(&file_id)
                .map(|file| file.live.clone())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "file inode does not exist")
                })?
        };
        state.finish(position)?;
        Ok(bytes)
    }

    fn object_len(&self, name: &str) -> io::Result<u64> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::ObjectLen)?;
        validate_name(name)?;
        let file_id = state.file_id(name)?;
        let length = state
            .files
            .get(&file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file inode does not exist"))?
            .live
            .len();
        state.finish(position)?;
        u64::try_from(length).map_err(|_| io::Error::other("in-memory file length exceeds u64"))
    }

    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::ReadExactAt)?;
        validate_name(name)?;
        if length > MAX_STORAGE_RANGE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bounded storage read exceeds the hard allocation limit",
            ));
        }
        let offset = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset is too large"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read range overflows"))?;
        let file_id = state.file_id(name)?;
        let source = if let Some(substitution) = &state.read_substitution {
            substitution
        } else {
            &state
                .files
                .get(&file_id)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "file inode does not exist")
                })?
                .live
        };
        let bytes = source.get(offset..end).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "bounded storage read exceeds the current object length",
            )
        })?;
        let bytes = bytes.to_vec();
        state.finish(position)?;
        Ok(bytes)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::ListNames)?;
        let names = state.live_directory.keys().cloned().collect();
        state.finish(position)?;
        Ok(names)
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::SetLen)?;
        validate_name(name)?;
        let length = usize::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "length is too large"))?;
        let file_id = state.file_id(name)?;
        let file = state
            .files
            .get_mut(&file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file inode does not exist"))?;
        file.live.resize(length, 0);
        state.finish(position)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::SyncFile)?;
        validate_name(name)?;
        let file_id = state.file_id(name)?;
        let file = state
            .files
            .get_mut(&file_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file inode does not exist"))?;
        file.durable.clone_from(&file.live);
        state.finish(position)
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::PublishNoreplace)?;
        validate_name(temporary_name)?;
        validate_name(published_name)?;
        if state.live_directory.contains_key(published_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "published file already exists",
            ));
        }
        let file_id = state.file_id(temporary_name)?;
        state.live_directory.remove(temporary_name);
        state
            .live_directory
            .insert(published_name.to_owned(), file_id);
        state.finish(position)
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::RemoveFile)?;
        validate_name(name)?;
        if state.live_directory.remove(name).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file does not exist",
            ));
        }
        state.finish(position)
    }

    fn sync_root(&self) -> io::Result<()> {
        let mut state = self.lock();
        let position = state.begin(StorageOperation::SyncRoot)?;
        state.durable_directory = state.live_directory.clone();
        state.finish(position)
    }
}

impl StorageIo for PausedStorageIo {
    fn create_new(&self, name: &str) -> io::Result<()> {
        self.pause_before(StorageOperation::CreateNew);
        self.inner.create_new(name)
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        self.pause_before(StorageOperation::Exists);
        self.inner.exists(name)
    }

    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.pause_before(StorageOperation::WriteAt);
        self.inner.write_at(name, offset, bytes)
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        self.pause_before(StorageOperation::Read);
        self.inner.read(name)
    }

    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.pause_before(StorageOperation::ObjectLen);
        self.inner.object_len(name)
    }

    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.pause_before(StorageOperation::ReadExactAt);
        self.inner.read_exact_at(name, offset, length)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        self.pause_before(StorageOperation::ListNames);
        self.inner.list_names()
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.pause_before(StorageOperation::SetLen);
        self.inner.set_len(name, length)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        self.pause_before(StorageOperation::SyncFile);
        self.inner.sync_file(name)
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        self.pause_before(StorageOperation::PublishNoreplace);
        self.inner.publish_noreplace(temporary_name, published_name)
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.pause_before(StorageOperation::RemoveFile);
        self.inner.remove_file(name)
    }

    fn sync_root(&self) -> io::Result<()> {
        self.pause_before(StorageOperation::SyncRoot);
        self.inner.sync_root()
    }
}

#[derive(Debug, Default)]
struct StorageState {
    next_file_id: u64,
    live_directory: BTreeMap<String, u64>,
    durable_directory: BTreeMap<String, u64>,
    files: BTreeMap<u64, MemoryFile>,
    operations: Vec<StorageOperation>,
    fail_before: Option<usize>,
    fail_after: Option<usize>,
    read_substitution: Option<Vec<u8>>,
}

impl StorageState {
    fn begin(&mut self, operation: StorageOperation) -> io::Result<usize> {
        let position = self.operations.len();
        self.operations.push(operation);
        if self.fail_before == Some(position) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("injected failure before storage operation {position}"),
            ));
        }
        Ok(position)
    }

    fn finish(&self, position: usize) -> io::Result<()> {
        if self.fail_after == Some(position) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("injected failure after storage operation {position}"),
            ));
        }
        Ok(())
    }

    fn allocate_file_id(&mut self) -> io::Result<u64> {
        self.next_file_id = self
            .next_file_id
            .checked_add(1)
            .ok_or_else(|| io::Error::other("in-memory file ID space exhausted"))?;
        Ok(self.next_file_id)
    }

    fn file_id(&self, name: &str) -> io::Result<u64> {
        self.live_directory.get(name).copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("file {name:?} does not exist"),
            )
        })
    }
}

#[derive(Debug, Default)]
struct MemoryFile {
    live: Vec<u8>,
    durable: Vec<u8>,
}

fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file name is not one path component",
        ));
    }
    Ok(())
}
