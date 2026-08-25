#![forbid(unsafe_code)]

//! Process-local accounting for the large-copy classes in the ingest path.

use std::sync::atomic::{AtomicU64, Ordering};

/// One avoidable-copy class measured in copied payload bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyClass {
    ChecksumScratch,
    PublicationVerifyMaterialization,
    FuseRequestAdaptation,
    ContainerAssembly,
    ChunkFragmentCoalescing,
    CompressionRegionMaterialization,
}

/// One point-in-time view of all avoidable-copy counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CopyTelemetrySnapshot {
    pub checksum_scratch_bytes: u64,
    pub publication_verify_materialization_bytes: u64,
    pub fuse_request_adaptation_bytes: u64,
    pub container_assembly_bytes: u64,
    pub chunk_fragment_coalescing_bytes: u64,
    pub compression_region_materialization_bytes: u64,
}

#[repr(align(64))]
struct CacheLineCounter(AtomicU64);

impl CacheLineCounter {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    fn add(&self, bytes: u64) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(bytes))
            });
    }

    fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

struct CopyCounters {
    checksum_scratch: CacheLineCounter,
    publication_verify_materialization: CacheLineCounter,
    fuse_request_adaptation: CacheLineCounter,
    container_assembly: CacheLineCounter,
    chunk_fragment_coalescing: CacheLineCounter,
    compression_region_materialization: CacheLineCounter,
}

static COUNTERS: CopyCounters = CopyCounters {
    checksum_scratch: CacheLineCounter::new(),
    publication_verify_materialization: CacheLineCounter::new(),
    fuse_request_adaptation: CacheLineCounter::new(),
    container_assembly: CacheLineCounter::new(),
    chunk_fragment_coalescing: CacheLineCounter::new(),
    compression_region_materialization: CacheLineCounter::new(),
};

/// Records bytes copied by one named ingest copy class.
pub fn record_copy(class: CopyClass, bytes: usize) {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    match class {
        CopyClass::ChecksumScratch => COUNTERS.checksum_scratch.add(bytes),
        CopyClass::PublicationVerifyMaterialization => {
            COUNTERS.publication_verify_materialization.add(bytes);
        }
        CopyClass::FuseRequestAdaptation => COUNTERS.fuse_request_adaptation.add(bytes),
        CopyClass::ContainerAssembly => COUNTERS.container_assembly.add(bytes),
        CopyClass::ChunkFragmentCoalescing => COUNTERS.chunk_fragment_coalescing.add(bytes),
        CopyClass::CompressionRegionMaterialization => {
            COUNTERS.compression_region_materialization.add(bytes);
        }
    }
}

/// Returns all process-local counters without resetting them.
#[must_use]
pub fn copy_telemetry() -> CopyTelemetrySnapshot {
    CopyTelemetrySnapshot {
        checksum_scratch_bytes: COUNTERS.checksum_scratch.load(),
        publication_verify_materialization_bytes: COUNTERS
            .publication_verify_materialization
            .load(),
        fuse_request_adaptation_bytes: COUNTERS.fuse_request_adaptation.load(),
        container_assembly_bytes: COUNTERS.container_assembly.load(),
        chunk_fragment_coalescing_bytes: COUNTERS.chunk_fragment_coalescing.load(),
        compression_region_materialization_bytes: COUNTERS
            .compression_region_materialization
            .load(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_occupy_independent_cache_lines_and_account_by_class() {
        assert_eq!(std::mem::size_of::<CacheLineCounter>(), 64);
        assert_eq!(std::mem::align_of::<CacheLineCounter>(), 64);

        let before = copy_telemetry();
        record_copy(CopyClass::ChecksumScratch, 1);
        record_copy(CopyClass::PublicationVerifyMaterialization, 2);
        record_copy(CopyClass::FuseRequestAdaptation, 3);
        record_copy(CopyClass::ContainerAssembly, 4);
        record_copy(CopyClass::ChunkFragmentCoalescing, 5);
        record_copy(CopyClass::CompressionRegionMaterialization, 6);
        let after = copy_telemetry();

        assert_eq!(
            after.checksum_scratch_bytes - before.checksum_scratch_bytes,
            1
        );
        assert_eq!(
            after.publication_verify_materialization_bytes
                - before.publication_verify_materialization_bytes,
            2
        );
        assert_eq!(
            after.fuse_request_adaptation_bytes - before.fuse_request_adaptation_bytes,
            3
        );
        assert_eq!(
            after.container_assembly_bytes - before.container_assembly_bytes,
            4
        );
        assert_eq!(
            after.chunk_fragment_coalescing_bytes - before.chunk_fragment_coalescing_bytes,
            5
        );
        assert_eq!(
            after.compression_region_materialization_bytes
                - before.compression_region_materialization_bytes,
            6
        );
    }
}
