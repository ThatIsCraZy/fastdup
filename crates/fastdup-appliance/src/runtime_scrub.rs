//! One cancellable, read-only startup scrub. Its gate is independent of telemetry.
use super::{MaintenanceContainerStorage, TelemetryStorageIo, runtime_telemetry};
use fastdup_posix::Namespace;
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, ScrubCoverage, ScrubProgress,
    StorageIo,
};
use std::fmt::Write as _;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const READ_QUANTUM: usize = 256 * 1024;

#[derive(Clone)]
pub struct ScrubGate(Arc<Control>);

struct Control {
    cancelled: AtomicBool,
    complete: AtomicBool,
    read_bytes: AtomicU64,
    progress: Mutex<Progress>,
    activity: Box<dyn Fn() -> u64 + Send + Sync>,
    pace: Mutex<Pace>,
}

struct Pace {
    operations: u64,
    last_busy: Option<Instant>,
    last_report: Instant,
    structure_bytes: usize,
    structure_elapsed: Duration,
}

struct Progress {
    total: usize,
    verified: usize,
    resumed: usize,
    verified_bytes: u64,
    current: Option<String>,
}

impl ScrubGate {
    pub fn permits_gc(&self) -> bool {
        self.0.complete.load(Ordering::Acquire)
    }
}

pub struct ScrubHandle {
    pub gate: ScrubGate,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ScrubHandle {
    fn drop(&mut self) {
        self.gate.0.cancelled.store(true, Ordering::Release);
    }
}

impl ScrubHandle {
    pub async fn stop(mut self) -> Result<(), String> {
        self.gate.0.cancelled.store(true, Ordering::Release);
        let worker = self.worker.take().expect("scrub worker joined once");
        tokio::task::spawn_blocking(move || worker.join())
            .await
            .map_err(|error| error.to_string())?
            .map_err(|_| "background scrub worker panicked".to_owned())
    }
}

pub fn start(
    required: fastdup_store::PendingDataVerification,
    containers: ContainerRepository<MaintenanceContainerStorage>,
    indexes: ExactIndexRunRepository<FsStorageIo>,
    frontend: TelemetryStorageIo,
    namespace: Arc<Namespace>,
    progress_storage: (FsStorageIo, [u8; 32]),
) -> io::Result<ScrubHandle> {
    let control = Arc::new(Control::new(frontend));
    control.report("running", None);
    let gate = ScrubGate(Arc::clone(&control));
    let worker = std::thread::Builder::new()
        .name("recovery-scrub".to_owned())
        .spawn(move || {
            let mut journal = None;
            let result = (|| {
                fastdup_store::set_background_io_priority().map_err(io::Error::other)?;
                rustix::process::nice(10).map_err(io::Error::from)?;
                journal = match ScrubProgress::open(
                    progress_storage.0,
                    progress_storage.1,
                    unix_seconds(),
                ) {
                    Ok(progress) => Some(progress),
                    Err(error) => {
                        progress_warning(&error);
                        None
                    }
                };
                let mut coverage = ScrubCoverage::new(required);
                let mut last_sync = Instant::now();
                let mut unsynced = 0;
                let names = containers
                    .recovery_container_snapshot()
                    .map_err(io::Error::other)?;
                control.progress.lock().expect("scrub progress lock").total = names.len();
                control.report("running", None);
                let paced = PacedStorage {
                    inner: containers.storage().clone(),
                    control: Arc::clone(&control),
                };
                let repository = containers.with_maintenance_storage(paced);
                let index = indexes.pin_active_generation();
                for id in names {
                    control.check_cancelled()?;
                    control
                        .progress
                        .lock()
                        .expect("scrub progress lock")
                        .current = Some(id.bytes().iter().fold(
                        String::with_capacity(32),
                        |mut output, byte| {
                            write!(output, "{byte:02x}").expect("String formatting");
                            output
                        },
                    ));
                    let (bytes, resumed) = verify_next(
                        &repository,
                        index.as_deref(),
                        id,
                        &mut coverage,
                        &mut journal,
                    )?;
                    unsynced += usize::from(!resumed);
                    if unsynced >= 64 || last_sync.elapsed() >= Duration::from_secs(5) {
                        sync_progress(&mut journal);
                        unsynced = 0;
                        last_sync = Instant::now();
                    }
                    let mut progress = control.progress.lock().expect("scrub progress lock");
                    progress.verified += 1;
                    progress.resumed += usize::from(resumed);
                    progress.verified_bytes += bytes;
                    drop(progress);
                    control.report("running", None);
                }
                coverage.finish().map_err(io::Error::other)?;
                if let Some(progress) = &mut journal
                    && let Err(error) = progress.complete()
                {
                    progress_warning(&error);
                    journal = None;
                }
                Ok::<_, io::Error>(())
            })();
            sync_progress(&mut journal);
            control.finish(result, &namespace);
        })?;
    Ok(ScrubHandle {
        gate,
        worker: Some(worker),
    })
}

fn verify_next(
    repository: &ContainerRepository<PacedStorage<MaintenanceContainerStorage>>,
    index: Option<&fastdup_store::ActivatedExactIndex<FsStorageIo>>,
    id: fastdup_format::ContainerId,
    coverage: &mut ScrubCoverage,
    journal: &mut Option<ScrubProgress<FsStorageIo>>,
) -> io::Result<(u64, bool)> {
    let cached = match journal
        .as_ref()
        .map(|j| j.lookup(id, unix_seconds()))
        .transpose()
    {
        Ok(entry) => entry.flatten(),
        Err(error) => {
            progress_warning(&error);
            *journal = None;
            None
        }
    };
    let resumed = match cached.as_ref() {
        Some(entry) => repository
            .resume_scrub(entry, coverage)
            .map_err(io::Error::other)?,
        None => false,
    };
    let bytes = if resumed {
        cached.expect("resumed entry exists").bytes()
    } else {
        let entry = repository
            .scrub_for_progress(id, index, coverage, unix_seconds())
            .map_err(io::Error::other)?;
        if let Some(progress) = journal
            && let Err(error) = progress.record(&entry)
        {
            progress_warning(&error);
            *journal = None;
        }
        entry.bytes()
    };
    Ok((bytes, resumed))
}

fn unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn progress_warning(error: &io::Error) {
    eprintln!(
        "WARNING: scrub_progress_unavailable=true full_verification_continues=true error={error}"
    );
}

fn sync_progress(journal: &mut Option<ScrubProgress<FsStorageIo>>) {
    if let Some(progress) = journal
        && let Err(error) = progress.sync()
    {
        progress_warning(&error);
        *journal = None;
    }
}

impl Control {
    fn new(frontend: TelemetryStorageIo) -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            complete: AtomicBool::new(false),
            read_bytes: AtomicU64::new(0),
            progress: Mutex::new(Progress {
                total: 0,
                verified: 0,
                resumed: 0,
                verified_bytes: 0,
                current: None,
            }),
            activity: Box::new(move || frontend.inner.status().submitted_operations()),
            pace: Mutex::new(Pace {
                operations: 0,
                last_busy: None,
                last_report: Instant::now(),
                structure_bytes: 0,
                structure_elapsed: Duration::ZERO,
            }),
        }
    }

    fn finish(&self, result: io::Result<()>, namespace: &Namespace) {
        if self.cancelled.load(Ordering::Acquire) {
            self.report("cancelled", None);
        } else if let Err(error) = result {
            namespace.fail_integrity();
            self.report("failed", Some(&error.to_string()));
            eprintln!(
                "CRITICAL: background_scrub_failed=true mutation_admission=integrity_failed error={error}"
            );
        } else {
            self.complete.store(true, Ordering::Release);
            self.report("complete", None);
            eprintln!(
                "background_scrub_ok=true containers={} read_bytes={}",
                self.progress.lock().expect("scrub progress lock").verified,
                self.read_bytes.load(Ordering::Relaxed)
            );
        }
    }

    fn check_cancelled(&self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "scrub cancelled",
            ))
        } else {
            Ok(())
        }
    }

    fn report(&self, state: &str, error: Option<&str>) {
        let p = self.progress.lock().expect("scrub progress lock");
        runtime_telemetry::record_scrub(serde_json::json!({
            "state":state, "totalContainers":p.total, "verifiedContainers":p.verified,
            "resumedContainers":p.resumed, "newlyVerifiedContainers":p.verified.saturating_sub(p.resumed),
            "remainingContainers":p.total.saturating_sub(p.verified),
            "verifiedBytes":p.verified_bytes, "readBytes":self.read_bytes.load(Ordering::Relaxed),
            "currentContainer":p.current, "error":error,
        }));
    }

    fn after_read(&self, bytes: usize, elapsed: Duration) -> io::Result<()> {
        self.read_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        let now = Instant::now();
        let operations = (self.activity)();
        let mut pace = self.pace.lock().expect("scrub pace lock");
        if operations != pace.operations {
            pace.operations = operations;
            pace.last_busy = Some(now);
        }
        let busy = pace
            .last_busy
            .is_some_and(|last| now.duration_since(last) < Duration::from_secs(5));
        let report = now.duration_since(pace.last_report) >= Duration::from_secs(1);
        if report {
            pace.last_report = now;
        }
        drop(pace);
        if report {
            self.report("running", None);
        }
        // At most one 256-KiB operation at a time. Under foreground load spend
        // at most roughly one tenth of the measured read+sleep cycle doing I/O;
        // retain a 50% duty limit while idle. Linux idle I/O priority is separate.
        let delay = elapsed
            .saturating_mul(if busy { 9 } else { 1 })
            .max(Duration::from_millis(if busy { 10 } else { 1 }));
        let end = Instant::now() + delay;
        while Instant::now() < end {
            self.check_cancelled()?;
            std::thread::sleep(
                end.saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(20)),
            );
        }
        self.check_cancelled()
    }
}

#[derive(Clone)]
struct PacedStorage<I> {
    inner: I,
    control: Arc<Control>,
}

fn read_only() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "scrub storage is read-only",
    )
}

impl<I: StorageIo> StorageIo for PacedStorage<I> {
    fn create_new(&self, _: &str) -> io::Result<()> {
        Err(read_only())
    }
    fn exists(&self, name: &str) -> io::Result<bool> {
        self.inner.exists(name)
    }
    fn write_at(&self, _: &str, _: u64, _: &[u8]) -> io::Result<()> {
        Err(read_only())
    }
    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.inner.object_len(name)
    }
    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let length = self.inner.object_len(name)?;
        if length > fastdup_format::MAX_CONTAINER_BYTES {
            return Err(io::Error::other("scrub object exceeds Container limit"));
        }
        self.read_exact_at(name, 0, usize::try_from(length).map_err(io::Error::other)?)
    }
    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        if length as u64 > fastdup_format::MAX_CONTAINER_BYTES {
            return Err(io::Error::other("scrub range exceeds Container limit"));
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(io::Error::other)?;
        while bytes.len() < length {
            self.control.check_cancelled()?;
            let start = Instant::now();
            let count = (length - bytes.len()).min(READ_QUANTUM);
            let position = offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| io::Error::other("scrub range overflow"))?;
            let part = self.inner.read_exact_at(name, position, count)?;
            if part.len() != count {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            bytes.extend_from_slice(&part);
            self.control.after_read(count, start.elapsed())?;
        }
        Ok(bytes)
    }
    fn read_structure_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.control.check_cancelled()?;
        let start = Instant::now();
        let bytes = self.inner.read_structure_at(name, offset, length)?;
        self.control
            .read_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let mut pace = self.control.pace.lock().expect("scrub pace lock");
        pace.structure_bytes += bytes.len();
        pace.structure_elapsed += start.elapsed();
        let batch = if pace.structure_bytes >= READ_QUANTUM {
            pace.structure_bytes = 0;
            Some(std::mem::take(&mut pace.structure_elapsed))
        } else {
            None
        };
        drop(pace);
        if let Some(elapsed) = batch {
            self.control.after_read(0, elapsed)?;
        }
        self.control.check_cancelled()?;
        Ok(bytes)
    }
    fn list_names(&self) -> io::Result<Vec<String>> {
        self.inner.list_names()
    }
    fn set_len(&self, _: &str, _: u64) -> io::Result<()> {
        Err(read_only())
    }
    fn sync_file(&self, _: &str) -> io::Result<()> {
        Err(read_only())
    }
    fn publish_noreplace(&self, _: &str, _: &str) -> io::Result<()> {
        Err(read_only())
    }
    fn remove_file(&self, _: &str) -> io::Result<()> {
        Err(read_only())
    }
    fn sync_root(&self) -> io::Result<()> {
        Err(read_only())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastdup_testkit::{MemoryStorageIo, StorageOperation};

    fn control(cancel_on_first_read: bool) -> Arc<Control> {
        Arc::new_cyclic(|weak: &std::sync::Weak<Control>| {
            let owner = weak.clone();
            Control {
                cancelled: AtomicBool::new(false),
                complete: AtomicBool::new(false),
                read_bytes: AtomicU64::new(0),
                progress: Mutex::new(Progress {
                    total: 1,
                    verified: 0,
                    resumed: 0,
                    verified_bytes: 0,
                    current: None,
                }),
                activity: Box::new(move || {
                    if cancel_on_first_read {
                        owner
                            .upgrade()
                            .unwrap()
                            .cancelled
                            .store(true, Ordering::Release);
                    }
                    0
                }),
                pace: Mutex::new(Pace {
                    operations: 0,
                    last_busy: None,
                    last_report: Instant::now(),
                    structure_bytes: 0,
                    structure_elapsed: Duration::ZERO,
                }),
            }
        })
    }

    #[test]
    fn actual_scrub_failure_blocks_writes_and_never_opens_gc_gate() {
        for damaged in [false, true] {
            let storage = MemoryStorageIo::new();
            let id = fastdup_format::ContainerId::new([41; 16]).unwrap();
            let name = format!("{}.fdc", "29".repeat(16));
            ContainerRepository::new(storage.clone())
                .publish_raw(id, 1, &[&vec![3; 256 * 1024]])
                .unwrap();
            if damaged {
                storage.write_at(&name, 4096 + 192, &[4]).unwrap();
            }
            let before = storage.operation_count();
            let control = control(false);
            let repository = ContainerRepository::new(PacedStorage {
                inner: storage.clone(),
                control: Arc::clone(&control),
            });
            let result = repository
                .scrub_container::<MemoryStorageIo>(id, None)
                .map(|_| ())
                .map_err(io::Error::other);
            assert_eq!(result.is_err(), damaged);
            let namespace = Namespace::new_volatile(fastdup_posix::NamespaceConfig::default());
            control.finish(result, &namespace);
            namespace.resume_mutation_admission();
            assert_eq!(ScrubGate(Arc::clone(&control)).permits_gc(), !damaged);
            assert_eq!(namespace.integrity_failed(), damaged);
            assert_eq!(namespace.mutation_admission_open(), !damaged);
            assert!(!storage.operations()[before..].contains(&StorageOperation::Read));
        }
    }

    #[test]
    fn cancellation_between_read_portions_does_not_report_corruption() {
        let storage = MemoryStorageIo::new();
        let id = fastdup_format::ContainerId::new([42; 16]).unwrap();
        ContainerRepository::new(storage.clone())
            .publish_raw(id, 1, &[&vec![3; 256 * 1024], &vec![4; 256 * 1024]])
            .unwrap();
        let control = control(true);
        let repository = ContainerRepository::new(PacedStorage {
            inner: storage,
            control: Arc::clone(&control),
        });
        let result = repository
            .scrub_container::<MemoryStorageIo>(id, None)
            .map(|_| ())
            .map_err(io::Error::other);
        assert!(result.is_err());
        assert_eq!(
            control.read_bytes.load(Ordering::Relaxed),
            READ_QUANTUM as u64
        );
        let namespace = Namespace::new_volatile(fastdup_posix::NamespaceConfig::default());
        control.finish(result, &namespace);
        assert!(!ScrubGate(control).permits_gc());
        assert!(!namespace.integrity_failed());
        assert!(namespace.mutation_admission_open());
    }
}

#[cfg(test)]
mod resume_tests {
    use super::*;
    use fastdup_format::{ContainerId, NamespaceRoot};
    use fastdup_store::{GenerationRepository, TieredStorageIo};

    #[tokio::test]
    async fn orderly_worker_stop_flushes_and_the_next_worker_reuses_only_that_round() {
        let root = std::env::temp_dir().join(format!(
            "scrub-resume-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        for name in ["metadata", "data", "small"] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        let metadata = FsStorageIo::open(root.join("metadata")).unwrap();
        let data = FsStorageIo::open(root.join("data")).unwrap();
        let small = FsStorageIo::open(root.join("small")).unwrap();
        let repository = ContainerRepository::new(TieredStorageIo::new(data.clone(), small));
        let chunk = vec![17; 65536];
        for n in 1..=3 {
            let chunks = vec![chunk.as_slice(); if n == 1 { 1 } else { 64 }];
            repository
                .publish_raw(ContainerId::new([n; 16]).unwrap(), u64::from(n), &chunks)
                .unwrap();
        }
        let generation =
            GenerationRepository::new(metadata.clone(), fastdup_appliance::checkpoint_policy_set());
        generation
            .commit_namespace(&NamespaceRoot::new(1024, 2, 0, vec![], vec![]).unwrap())
            .unwrap();
        let frontend = super::super::open_data_storage(&root.join("data"), false).unwrap();
        let launch = || {
            let (_, required) = generation.recover_committed_for_mount(&repository).unwrap();
            start(
                required,
                repository.clone(),
                ExactIndexRunRepository::new(metadata.clone()),
                frontend.clone(),
                Arc::new(Namespace::new_volatile(
                    fastdup_posix::NamespaceConfig::default(),
                )),
                (metadata.clone(), [19; 32]),
            )
            .unwrap()
        };
        let first = launch();
        let first_gate = first.gate.clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            while first_gate.0.progress.lock().unwrap().verified == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        first.stop().await.unwrap();
        let saved = first_gate.0.progress.lock().unwrap().verified;
        assert!((1..3).contains(&saved));
        assert!(!first_gate.permits_gc());
        let second = launch();
        let second_gate = second.gate.clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !second_gate.permits_gc() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        second.stop().await.unwrap();
        assert_eq!(second_gate.0.progress.lock().unwrap().resumed, saved);
        assert_eq!(second_gate.0.progress.lock().unwrap().verified, 3);
        let third = launch();
        let third_gate = third.gate.clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !third_gate.permits_gc() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        third.stop().await.unwrap();
        assert_eq!(third_gate.0.progress.lock().unwrap().resumed, 0);
        assert!(
            second_gate.0.read_bytes.load(Ordering::Relaxed)
                < third_gate.0.read_bytes.load(Ordering::Relaxed)
        );
        drop(frontend);
        drop(generation);
        drop(repository);
        drop(metadata);
        drop(data);
        std::fs::remove_dir_all(root).unwrap();
    }
}
