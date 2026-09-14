//! DATA-tier storage adapter with optional access-pattern telemetry.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fastdup_copy_metrics::copy_telemetry;
use fastdup_format::{HEADER_BYTES, VerifiedContainerPublication};
use fastdup_io_uring::{IoUringStorageConfig, IoUringStorageIo};
use fastdup_store::{OwnedContainerPublication, StorageIo, StoreError, publication_sample_ranges};

#[derive(Clone, Debug)]
pub(super) struct TelemetryStorageIo {
    pub(super) inner: IoUringStorageIo,
    enabled: bool,
    telemetry: Arc<DataIoTelemetry>,
}

impl TelemetryStorageIo {
    pub(super) fn open(root: &Path, enabled: bool) -> io::Result<Self> {
        let inner = IoUringStorageIo::open(root, IoUringStorageConfig::default())?;
        Ok(Self::new(inner, enabled))
    }

    fn new(inner: IoUringStorageIo, enabled: bool) -> Self {
        Self {
            inner,
            enabled,
            telemetry: Arc::new(DataIoTelemetry::default()),
        }
    }

    pub(super) fn emit(&self) {
        if !self.enabled {
            return;
        }
        eprintln!(
            concat!(
                "data_io_metrics whole_reads={} whole_read_bytes={} range_reads={} ",
                "range_read_bytes={} random_range_reads={} writes={} write_bytes={} ",
                "nonsequential_writes={}"
            ),
            self.telemetry.whole_reads.load(Ordering::Relaxed),
            self.telemetry.whole_read_bytes.load(Ordering::Relaxed),
            self.telemetry.range_reads.load(Ordering::Relaxed),
            self.telemetry.range_read_bytes.load(Ordering::Relaxed),
            self.telemetry.random_range_reads.load(Ordering::Relaxed),
            self.telemetry.writes.load(Ordering::Relaxed),
            self.telemetry.write_bytes.load(Ordering::Relaxed),
            self.telemetry.nonsequential_writes.load(Ordering::Relaxed),
        );
    }

    pub(super) fn emit_backend_state(&self) {
        let status = self.inner.status();
        eprintln!(
            concat!(
                "data_io_uring ring_entries={} max_inflight_bytes={} ",
                "inflight_bytes={} peak_inflight_bytes={} submitted_operations={} ",
                "completed_operations={} root_sync_callers={} root_sync_submissions={} ",
                "owned_publications_started={} owned_publications_completed={} ",
                "borrowed_write_copy_bytes={}"
            ),
            status.ring_entries(),
            status.max_inflight_bytes(),
            status.inflight_bytes(),
            status.peak_inflight_bytes(),
            status.submitted_operations(),
            status.completed_operations(),
            status.root_sync_callers(),
            status.root_sync_submissions(),
            status.owned_publications_started(),
            status.owned_publications_completed(),
            status.borrowed_write_copy_bytes(),
        );
        let copies = copy_telemetry();
        eprintln!(
            concat!(
                "copy_bytes checksum_scratch_bytes={} publication_verify_materialization_bytes={} ",
                "fuse_request_adaptation_bytes={} container_assembly_bytes={} ",
                "chunk_fragment_coalescing_bytes={} compression_region_materialization_bytes={} ",
                "compression_region_concatenation_bytes={}"
            ),
            copies.checksum_scratch_bytes,
            copies.publication_verify_materialization_bytes,
            copies.fuse_request_adaptation_bytes,
            copies.container_assembly_bytes,
            copies.chunk_fragment_coalescing_bytes,
            copies.compression_region_materialization_bytes,
            copies.compression_region_concatenation_bytes,
        );
    }
}

#[derive(Debug, Default)]
struct DataIoTelemetry {
    whole_reads: AtomicU64,
    whole_read_bytes: AtomicU64,
    range_reads: AtomicU64,
    range_read_bytes: AtomicU64,
    random_range_reads: AtomicU64,
    writes: AtomicU64,
    write_bytes: AtomicU64,
    nonsequential_writes: AtomicU64,
    last_read_end: Mutex<BTreeMap<String, u64>>,
    last_write_end: Mutex<BTreeMap<String, u64>>,
}

impl DataIoTelemetry {
    fn classify_range_read(&self, name: &str, offset: u64, length: usize) {
        let mut ends = self
            .last_read_end
            .lock()
            .expect("ASSERT: data-tier read telemetry lock poisoned");
        let random = ends.get(name).map_or(offset != 0, |end| *end != offset);
        let length = u64::try_from(length).expect("ASSERT: range-read length fits u64");
        let end = offset.saturating_add(length);
        ends.insert(name.to_owned(), end);
        if random {
            self.random_range_reads.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn classify_write(&self, name: &str, offset: u64, length: usize) {
        let mut ends = self
            .last_write_end
            .lock()
            .expect("ASSERT: data-tier write telemetry lock poisoned");
        let nonsequential = ends.get(name).map_or(offset != 0, |end| *end != offset);
        let length = u64::try_from(length).expect("ASSERT: write length fits u64");
        let end = offset.saturating_add(length);
        ends.insert(name.to_owned(), end);
        if nonsequential {
            self.nonsequential_writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_owned_publication(&self, temporary_name: &str, sealed_bytes: usize) {
        let ranges = publication_sample_ranges(sealed_bytes)
            .expect("ASSERT: owned publication has valid format-v1 sample ranges");
        let sealed_bytes =
            u64::try_from(sealed_bytes).expect("ASSERT: format-v1 Container length fits u64");
        let durable_write_bytes = sealed_bytes
            .checked_add(
                u64::try_from(HEADER_BYTES).expect("ASSERT: format Header length fits u64"),
            )
            .expect("ASSERT: bounded Container publication bytes cannot overflow");
        for range in ranges {
            self.classify_range_read(temporary_name, range.offset(), range.length());
            self.range_reads.fetch_add(1, Ordering::Relaxed);
            self.range_read_bytes.fetch_add(
                u64::try_from(range.length()).expect("ASSERT: sample length fits u64"),
                Ordering::Relaxed,
            );
        }
        self.writes.fetch_add(3, Ordering::Relaxed);
        self.write_bytes
            .fetch_add(durable_write_bytes, Ordering::Relaxed);
        self.nonsequential_writes.fetch_add(1, Ordering::Relaxed);
    }
}

impl StorageIo for TelemetryStorageIo {
    fn read_structure_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.inner.read_structure_at(name, offset, length)
    }

    fn create_new(&self, name: &str) -> io::Result<()> {
        self.inner.create_new(name)?;
        if self.enabled {
            self.telemetry
                .last_write_end
                .lock()
                .expect("ASSERT: data-tier write telemetry lock poisoned")
                .insert(name.to_owned(), 0);
        }
        Ok(())
    }

    fn exists(&self, name: &str) -> io::Result<bool> {
        self.inner.exists(name)
    }

    fn write_at(&self, name: &str, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_at(name, offset, bytes)?;
        if !self.enabled {
            return Ok(());
        }
        self.telemetry.classify_write(name, offset, bytes.len());
        self.telemetry.writes.fetch_add(1, Ordering::Relaxed);
        self.telemetry.write_bytes.fetch_add(
            u64::try_from(bytes.len()).expect("ASSERT: write length fits u64"),
            Ordering::Relaxed,
        );
        Ok(())
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        let bytes = self.inner.read(name)?;
        if !self.enabled {
            return Ok(bytes);
        }
        self.telemetry.whole_reads.fetch_add(1, Ordering::Relaxed);
        self.telemetry.whole_read_bytes.fetch_add(
            u64::try_from(bytes.len()).expect("ASSERT: whole-object read length fits u64"),
            Ordering::Relaxed,
        );
        Ok(bytes)
    }

    fn object_len(&self, name: &str) -> io::Result<u64> {
        self.inner.object_len(name)
    }

    fn read_exact_at(&self, name: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let bytes = self.inner.read_exact_at(name, offset, length)?;
        if !self.enabled {
            return Ok(bytes);
        }
        self.telemetry.classify_range_read(name, offset, length);
        self.telemetry.range_reads.fetch_add(1, Ordering::Relaxed);
        self.telemetry.range_read_bytes.fetch_add(
            u64::try_from(length).expect("ASSERT: range-read length fits u64"),
            Ordering::Relaxed,
        );
        Ok(bytes)
    }

    fn list_names(&self) -> io::Result<Vec<String>> {
        self.inner.list_names()
    }

    fn set_len(&self, name: &str, length: u64) -> io::Result<()> {
        self.inner.set_len(name, length)
    }

    fn sync_file(&self, name: &str) -> io::Result<()> {
        self.inner.sync_file(name)
    }

    fn publish_noreplace(&self, temporary_name: &str, published_name: &str) -> io::Result<()> {
        self.inner.publish_noreplace(temporary_name, published_name)
    }

    fn remove_file(&self, name: &str) -> io::Result<()> {
        self.inner.remove_file(name)
    }

    fn sync_root(&self) -> io::Result<()> {
        self.inner.sync_root()
    }

    fn publish_owned_container(
        &self,
        publication: OwnedContainerPublication,
    ) -> Result<VerifiedContainerPublication, StoreError> {
        let sealed_bytes = publication.sealed_len();
        let temporary_name = publication.temporary_name().to_owned();
        let verified = self.inner.publish_owned_container(publication)?;
        if self.enabled {
            self.telemetry
                .record_owned_publication(&temporary_name, sealed_bytes);
        }
        Ok(verified)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use fastdup_format::ContainerId;
    use fastdup_store::ContainerRepository;

    use super::*;

    #[test]
    fn production_storage_requires_io_uring() {
        let root =
            std::env::temp_dir().join(format!("fastdup-default-io-uring-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("create unique test root");

        let storage = TelemetryStorageIo::open(&root, false).expect("open production data storage");

        assert!(storage.inner.status().ring_entries() > 0);
        drop(storage);
        std::fs::remove_dir_all(root).expect("remove test root");
    }

    #[test]
    fn adapter_records_sampled_owned_container_publication() {
        let root =
            std::env::temp_dir().join(format!("fastdup-telemetry-owned-{}", std::process::id()));
        std::fs::create_dir(&root).expect("create unique test root");
        let storage = TelemetryStorageIo::open(&root, true).expect("open production data storage");
        let repository = ContainerRepository::new(storage.clone());
        let chunk = b"telemetry-owned-publication".repeat(8_192);
        let region = [chunk.as_slice()];
        let regions = [region.as_slice()];
        let prepared =
            ContainerRepository::<TelemetryStorageIo>::prepare_adaptive_regions_parallel(
                ContainerId::new([0xA7; 16]).expect("fixture Container ID is nonzero"),
                1,
                &regions,
                NonZeroUsize::MIN,
            )
            .expect("prepare fixture Container");

        let (_, metrics) = repository
            .publish_prepared_adaptive_profiled(prepared)
            .expect("publish fixture Container through telemetry adapter");

        let status = storage.inner.status();
        assert_eq!(status.owned_publications_started(), 1);
        assert_eq!(status.owned_publications_completed(), 1);
        assert_eq!(status.borrowed_write_copy_bytes(), 0);
        assert_eq!(storage.telemetry.whole_reads.load(Ordering::Relaxed), 0);
        assert_eq!(
            storage.telemetry.whole_read_bytes.load(Ordering::Relaxed),
            0
        );
        assert_eq!(storage.telemetry.range_reads.load(Ordering::Relaxed), 3);
        assert_eq!(
            storage.telemetry.range_read_bytes.load(Ordering::Relaxed),
            u64::try_from(HEADER_BYTES * 3).expect("sample bytes fit u64")
        );
        assert_eq!(storage.telemetry.writes.load(Ordering::Relaxed), 3);
        assert_eq!(
            storage.telemetry.write_bytes.load(Ordering::Relaxed),
            metrics.file_bytes()
                + u64::try_from(HEADER_BYTES).expect("format Header length fits u64")
        );
        assert_eq!(
            storage
                .telemetry
                .nonsequential_writes
                .load(Ordering::Relaxed),
            1
        );

        drop(repository);
        drop(storage);
        std::fs::remove_dir_all(root).expect("remove test root");
    }
}
