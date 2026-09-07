//! One cancellable, read-only startup scrub. Its gate is independent of telemetry.
use super::{MaintenanceContainerStorage, TelemetryStorageIo, runtime_telemetry};
use fastdup_posix::Namespace;
use fastdup_store::{ContainerRepository, ExactIndexRunRepository, FsStorageIo, StorageIo};
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
}

struct Progress {
    total: usize,
    verified: usize,
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
    mut required: fastdup_store::PendingDataVerification,
    containers: ContainerRepository<MaintenanceContainerStorage>,
    indexes: ExactIndexRunRepository<FsStorageIo>,
    frontend: TelemetryStorageIo,
    namespace: Arc<Namespace>,
) -> io::Result<ScrubHandle> {
    let control = Arc::new(Control {
        cancelled: AtomicBool::new(false),
        complete: AtomicBool::new(false),
        read_bytes: AtomicU64::new(0),
        progress: Mutex::new(Progress {
            total: 0,
            verified: 0,
            verified_bytes: 0,
            current: None,
        }),
        activity: Box::new(move || frontend.inner.status().submitted_operations()),
        pace: Mutex::new(Pace {
            operations: 0,
            last_busy: None,
            last_report: Instant::now(),
        }),
    });
    control.report("running", None);
    let gate = ScrubGate(Arc::clone(&control));
    let worker = std::thread::Builder::new()
        .name("recovery-scrub".to_owned())
        .spawn(move || {
            let result = (|| {
                fastdup_store::set_background_io_priority().map_err(io::Error::other)?;
                rustix::process::nice(10).map_err(io::Error::from)?;
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
                    let bytes = repository
                        .scrub_container_for_recovery(id, index.as_deref(), &mut required)
                        .map_err(io::Error::other)?;
                    let mut progress = control.progress.lock().expect("scrub progress lock");
                    progress.verified += 1;
                    progress.verified_bytes += bytes;
                    drop(progress);
                    control.report("running", None);
                }
                required.finish().map_err(io::Error::other)?;
                Ok::<_, io::Error>(())
            })();
            control.finish(result, &namespace);
        })?;
    Ok(ScrubHandle {
        gate,
        worker: Some(worker),
    })
}

impl Control {
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
