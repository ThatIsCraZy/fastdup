//! Bounded, process-local Metadata read attribution. Never storage authority.
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub enum MetadataReadReason {
    Other,
    IndexLookup,
    IndexCompaction,
    IndexAudit,
    IndexEnvelope,
    Manifest,
    Namespace,
    RecoveryScrub,
    GarbageCollection,
}
const REASONS: [&str; 9] = [
    "other",
    "indexLookup",
    "indexCompaction",
    "indexAudit",
    "indexEnvelope",
    "manifest",
    "namespace",
    "recoveryScrub",
    "garbageCollection",
];
const OBJECTS: [&str; 6] = [
    "exactIndex",
    "similarityIndex",
    "metadataObject",
    "smallFile",
    "control",
    "other",
];
const MODES: [&str; 4] = [
    "directRange",
    "directFile",
    "directStructure",
    "directLease",
];
const ROWS: usize = REASONS.len() * OBJECTS.len() * MODES.len();

thread_local! {
    static REASON: Cell<MetadataReadReason> = const { Cell::new(MetadataReadReason::Other) };
}

/// Synchronous attribution scope; deliberately cannot cross threads or awaits.
pub struct MetadataReadScope(MetadataReadReason, PhantomData<Rc<()>>);
impl MetadataReadScope {
    #[must_use]
    pub fn enter(reason: MetadataReadReason) -> Self {
        Self(REASON.with(|current| current.replace(reason)), PhantomData)
    }
}
impl Drop for MetadataReadScope {
    fn drop(&mut self) {
        REASON.with(|current| current.set(self.0));
    }
}

#[derive(Debug, Default)]
struct Counters {
    operations: AtomicU64,
    requested: AtomicU64,
    returned: AtomicU64,
    errors: AtomicU64,
    micros: AtomicU64,
    max_micros: AtomicU64,
    active: AtomicU64,
}
#[derive(Debug)]
pub(crate) struct MetadataReadCounters([Counters; ROWS]);
impl Default for MetadataReadCounters {
    fn default() -> Self {
        Self(std::array::from_fn(|_| Counters::default()))
    }
}

pub(crate) fn system_counters() -> &'static Arc<MetadataReadCounters> {
    static COUNTERS: OnceLock<Arc<MetadataReadCounters>> = OnceLock::new();
    COUNTERS.get_or_init(|| Arc::new(MetadataReadCounters::default()))
}

// Durable object suffixes are canonical lowercase, not user file extensions.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub(crate) fn object_class(name: &str) -> usize {
    if name.ends_with(".fdx") || name.ends_with(".fdxset") {
        0
    } else if name.ends_with(".fds") || name.ends_with(".fdsf") {
        1
    } else if name.ends_with(".fdm") {
        2
    } else if name.ends_with(".fdc") {
        3
    } else if name.starts_with('.')
        || name.contains("commit")
        || name.contains("catalog")
        || name.contains("checkpoint")
    {
        4
    } else {
        5
    }
}
fn index(object: usize, mode: usize) -> usize {
    REASON.with(|reason| (reason.get() as usize * OBJECTS.len() + object) * MODES.len() + mode)
}

pub(crate) struct ReadSpan<'a> {
    counter: Option<&'a Counters>,
    started: Option<Instant>,
    requested: Option<usize>,
}
impl<'a> ReadSpan<'a> {
    pub(crate) fn start(
        counters: Option<&'a MetadataReadCounters>,
        name: &str,
        mode: usize,
        requested: Option<usize>,
    ) -> Self {
        let counter = counters.map(|counters| &counters.0[index(object_class(name), mode)]);
        if let Some(counter) = counter {
            counter.active.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            counter,
            started: counter.map(|_| Instant::now()),
            requested,
        }
    }
    pub(crate) fn finish(self, result: &std::io::Result<Vec<u8>>) {
        if let Some(counter) = self.counter {
            let returned = result.as_ref().map_or(0, Vec::len) as u64;
            let micros = u64::try_from(
                self.started
                    .map_or(0, |started| started.elapsed().as_micros()),
            )
            .unwrap_or(u64::MAX);
            record(
                counter,
                self.requested.map_or(returned, |bytes| bytes as u64),
                returned,
                result.is_err(),
                micros,
            );
        }
    }
}
impl Drop for ReadSpan<'_> {
    fn drop(&mut self) {
        if let Some(counter) = self.counter {
            counter.active.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
fn record(counter: &Counters, requested: u64, returned: u64, error: bool, micros: u64) {
    counter.requested.fetch_add(requested, Ordering::Relaxed);
    counter.returned.fetch_add(returned, Ordering::Relaxed);
    counter
        .errors
        .fetch_add(u64::from(error), Ordering::Relaxed);
    counter.micros.fetch_add(micros, Ordering::Relaxed);
    counter.max_micros.fetch_max(micros, Ordering::Relaxed);
    counter.operations.fetch_add(1, Ordering::Relaxed);
}

/// Logical direct backend requests and their observed elapsed time.
#[derive(Clone, Debug)]
pub struct MetadataReadRow {
    pub reason: &'static str,
    pub object: &'static str,
    pub mode: &'static str,
    pub operations: u64,
    pub requested_bytes: u64,
    pub returned_bytes: u64,
    pub errors: u64,
    pub elapsed_micros: u64,
    pub max_micros: u64,
    pub in_flight: u64,
    pub operations_per_second: f64,
    pub requested_mbps: f64,
}
#[derive(Clone, Debug)]
pub struct MetadataReadStatus {
    pub interval_seconds: f64,
    pub rows: Vec<MetadataReadRow>,
}

/// Samples cumulative counters and rates since the preceding management sample.
/// First samples have no elapsed interval. No I/O or allocator walk is performed.
/// # Panics
/// Panics if the telemetry sampler lock was poisoned.
#[must_use]
#[allow(clippy::cast_precision_loss)] // Approximate display rates; integer lifetime counters remain exact.
pub fn metadata_read_status() -> MetadataReadStatus {
    static PREVIOUS: Mutex<Option<(Instant, Vec<MetadataReadRow>)>> = Mutex::new(None);
    let mut previous = PREVIOUS
        .lock()
        .expect("ASSERT: metadata telemetry lock poisoned");
    let now = Instant::now();
    let interval = previous
        .as_ref()
        .map_or(0.0, |(at, _)| now.duration_since(*at).as_secs_f64());
    let mut rows = system_counters().rows();
    if let Some((_, old)) = previous.as_ref() {
        for row in &mut rows {
            let old = old.iter().find(|old| {
                old.reason == row.reason && old.object == row.object && old.mode == row.mode
            });
            if interval > 0.0 {
                row.operations_per_second = row
                    .operations
                    .saturating_sub(old.map_or(0, |old| old.operations))
                    as f64
                    / interval;
                row.requested_mbps = row
                    .requested_bytes
                    .saturating_sub(old.map_or(0, |old| old.requested_bytes))
                    as f64
                    / interval
                    / 1e6;
            }
        }
    }
    *previous = Some((now, rows.clone()));
    MetadataReadStatus {
        interval_seconds: interval,
        rows,
    }
}
impl MetadataReadCounters {
    pub(crate) fn rows(&self) -> Vec<MetadataReadRow> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(index, counter)| {
                let operations = counter.operations.load(Ordering::Relaxed);
                let in_flight = counter.active.load(Ordering::Relaxed);
                (operations != 0 || in_flight != 0).then(|| MetadataReadRow {
                    reason: REASONS[index / MODES.len() / OBJECTS.len()],
                    object: OBJECTS[index / MODES.len() % OBJECTS.len()],
                    mode: MODES[index % MODES.len()],
                    operations,
                    requested_bytes: counter.requested.load(Ordering::Relaxed),
                    returned_bytes: counter.returned.load(Ordering::Relaxed),
                    errors: counter.errors.load(Ordering::Relaxed),
                    elapsed_micros: counter.micros.load(Ordering::Relaxed),
                    max_micros: counter.max_micros.load(Ordering::Relaxed),
                    in_flight,
                    operations_per_second: 0.0,
                    requested_mbps: 0.0,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FsStorageIo, StorageIo};

    #[test]
    fn filesystem_reads_report_ranges_files_errors_and_restore_reason() {
        let root = std::path::PathBuf::from(std::env::var_os("TMPDIR").unwrap())
            .join(format!("metadata-reads-{}", std::process::id()));
        let counters = Arc::new(MetadataReadCounters::default());
        let mut storage = FsStorageIo::open(&root).unwrap();
        storage.metadata_reads = Some(Arc::clone(&counters));
        storage.create_new("sample.fdx").unwrap();
        storage.write_at("sample.fdx", 0, &vec![91; 8192]).unwrap();
        storage.sync_file("sample.fdx").unwrap();
        {
            let _lookup = MetadataReadScope::enter(MetadataReadReason::IndexLookup);
            assert_eq!(
                storage
                    .read_exact_at("sample.fdx", 4096, 4096)
                    .unwrap()
                    .len(),
                4096
            );
            {
                let _audit = MetadataReadScope::enter(MetadataReadReason::IndexAudit);
                assert_eq!(storage.read("sample.fdx").unwrap().len(), 8192);
            }
            assert!(storage.read_exact_at("sample.fdx", 8192, 4096).is_err());
        }
        {
            let _independent = crate::ReadIntentScope::enter(crate::ReadIntent::Independent);
            storage.read_structure_at("sample.fdx", 0, 4096).unwrap();
        }
        let rows = counters.rows();
        let lookup = rows.iter().find(|row| row.reason == "indexLookup").unwrap();
        assert_eq!(
            (
                lookup.operations,
                lookup.requested_bytes,
                lookup.returned_bytes,
                lookup.errors
            ),
            (2, 8192, 4096, 1)
        );
        assert_eq!(lookup.in_flight, 0);
        let audit = rows.iter().find(|row| row.reason == "indexAudit").unwrap();
        assert_eq!(
            (audit.mode, audit.operations, audit.returned_bytes),
            ("directFile", 1, 8192)
        );
        assert!(
            rows.iter()
                .any(|row| row.reason == "other" && row.mode == "directStructure")
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn active_backend_work_is_visible_before_a_read_finishes() {
        let counters = MetadataReadCounters::default();
        let read = ReadSpan::start(Some(&counters), "sample.fdm", 0, Some(4096));
        let rows = counters.rows();
        assert_eq!((rows[0].operations, rows[0].in_flight), (0, 1));
        read.finish(&Ok(vec![0; 4096]));
        let rows = counters.rows();
        assert_eq!((rows[0].operations, rows[0].in_flight), (1, 0));
    }

    #[test]
    fn real_mapped_index_audits_and_cache_misses_are_counted_but_hits_are_not() {
        use fastdup_format::{
            ChunkId, ContainerId, ExactIndexEntry, ExactIndexLocation, ExactIndexProfileId,
        };
        let root = std::path::PathBuf::from(std::env::var_os("TMPDIR").unwrap())
            .join(format!("metadata-mapping-{}", std::process::id()));
        let counters = Arc::new(MetadataReadCounters::default());
        {
            let mut storage = FsStorageIo::open(&root).unwrap();
            storage.metadata_reads = Some(Arc::clone(&counters));
            let repository = crate::ExactIndexRunRepository::new(storage);
            let location = ExactIndexLocation::raw(
                ContainerId::new([9; 16]).unwrap(),
                9,
                4608,
                16640,
                0xAB00_0008,
            )
            .unwrap();
            let entry =
                ExactIndexEntry::active(ChunkId::from_bytes([8; 32]), 16392, location).unwrap();
            let transition = repository
                .append_level_zero(ExactIndexProfileId::new([0xD8; 32]).unwrap(), vec![entry])
                .unwrap();
            let active = transition.current();
            assert!(counters.rows().iter().any(|row| row.reason == "indexAudit"
                && row.mode == "directLease"
                && row.returned_bytes > 0));
            active
                .lookup_transitions(entry.chunk_id(), entry.logical_length())
                .unwrap();
            let before: u64 = counters.rows().iter().map(|row| row.operations).sum();
            assert!(counters.rows().iter().any(|row| row.reason == "indexLookup"
                && row.mode == "directLease"
                && row.returned_bytes > 0));
            active
                .lookup_transitions(entry.chunk_id(), entry.logical_length())
                .unwrap();
            assert_eq!(
                before,
                counters
                    .rows()
                    .iter()
                    .map(|row| row.operations)
                    .sum::<u64>()
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
