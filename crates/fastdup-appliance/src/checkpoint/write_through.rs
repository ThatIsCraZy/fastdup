//! Bounded pre-commit reduction pipeline.
//!
//! The parent checkpoint module sees a small lifecycle interface. Queueing,
//! per-inode ordering, `SeqCDC` state, CPU admission, Container publication and
//! externalization remain coupled here because they enforce one shared memory
//! and durability contract.

use super::metrics::{CheckpointMetrics, CheckpointStage, CheckpointTimings};
use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use fastdup_copy_metrics::{CopyClass, record_copy};
use fastdup_format::{
    ChunkId, ExactIndexEntry, PrehashedAdaptiveRegion, PrehashedChunk, PrehashedContiguousRegion,
};
use fastdup_posix::{
    CommittedFile, ExternalizedExtent, InodeId, MutationObserver, MutationPayload, Namespace,
    NamespaceCommit,
};
use fastdup_store::{
    ContainerGenerationAllocator, ContainerPlacement, ContainerRepository, PersistentChunkPlan,
    StorageIo, VerifiedManifestFile, WorkerPermitLease, WorkerPermits, seqcdc_cut,
    seqcdc_cut_scalar, seqcdc_cut_segmented, seqcdc_cut_segmented_scalar,
};
use rayon::prelude::*;

use super::manifest_planning::random_container_id;
use super::{
    CDC_MAXIMUM_BYTES, COMPRESSION_REGION_TARGET_BYTES, CONTAINER_PAYLOAD_TARGET_BYTES,
    DurableNamespaceError, FillCommittedFile, ManifestReaderPolicy, OnlineDependencyProofs,
    PublicationClaim, PublicationClaims, SEQCDC_CONFIG_V1, VerifiedLocationFile,
    seqcdc_force_scalar,
};
use super::{CpuPhaseStatus, WriteThroughStatus};

const CONTAINER_PAYLOAD_FLUSH_BYTES: usize = CONTAINER_PAYLOAD_TARGET_BYTES - CDC_MAXIMUM_BYTES;
const PARTIAL_BATCH_BUDGET_BYTES_V1: usize = CONTAINER_PAYLOAD_TARGET_BYTES;
const MAX_CHUNK_FRAGMENTS_V1: usize = 1_024;
const WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1: usize = 400 * 1_024 * 1_024;
const WRITE_THROUGH_QUEUE_BUDGET_BYTES_V1: usize = 32 * 1_024 * 1_024;
const MULTI_STREAM_QUEUE_BUDGET_BYTES_V1: usize = 16 * 1_024 * 1_024;
const DETACHED_CONTAINER_BUDGET_BYTES_V1: usize = 2 * CONTAINER_PAYLOAD_TARGET_BYTES;
const PUBLICATION_WAIT_DIAGNOSTIC_INTERVAL_V1: Duration = Duration::from_secs(5);
const SINGLE_STREAM_PUBLICATION_WINDOW_V1: usize = 2;
const WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1: usize = 1_024 * 1_024;
// Owned request fragments remain separate allocations. The Ingest Ring groups
// only their immutable views, so sealing a batch never copies payload bytes.
const SINGLE_STREAM_INGEST_BATCH_BYTES_V1: usize = 4 * 1_024 * 1_024;
const SINGLE_STREAM_INGEST_RING_SLOTS_V1: usize = 8;
const INGEST_BATCH_MAXIMUM_AGE_V1: Duration = Duration::from_millis(10);
const MAX_ACTIVE_INGEST_LANES_V1: usize = (WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1
    - WRITE_THROUGH_QUEUE_BUDGET_BYTES_V1
    - DETACHED_CONTAINER_BUDGET_BYTES_V1)
    / (CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES)
    - 1;
const _: () = assert!(
    WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1
        >= WRITE_THROUGH_QUEUE_BUDGET_BYTES_V1
            + DETACHED_CONTAINER_BUDGET_BYTES_V1
            + 2 * (CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES)
);

#[derive(Debug)]
struct PendingWriteThroughChunk {
    offset: u64,
    chunk_id: ChunkId,
    bytes: ChunkFragments,
    placement: ContainerPlacement,
}

struct CompressionRegionPlan<'a> {
    chunks: Vec<&'a PendingWriteThroughChunk>,
    decoded_length: usize,
    materialized: bool,
}

struct MaterializedCompressionRegion {
    decoded: Vec<u8>,
    chunks: Vec<(ChunkId, Range<usize>)>,
}

#[derive(Clone, Copy)]
enum CompressionRegionOrder {
    Borrowed(usize),
    Materialized(usize),
}

struct PreparedCompressionRegions<'a> {
    borrowed: Vec<Vec<PrehashedChunk<'a>>>,
    materialized: Vec<MaterializedCompressionRegion>,
    order: Vec<CompressionRegionOrder>,
}

#[derive(Debug)]
enum ChunkParts {
    Single(MutationPayload),
    Fragmented(Vec<MutationPayload>),
}

impl ChunkParts {
    fn as_slice(&self) -> &[MutationPayload] {
        match self {
            Self::Single(part) => std::slice::from_ref(part),
            Self::Fragmented(parts) => parts,
        }
    }
}

#[derive(Debug)]
struct ChunkFragments {
    parts: ChunkParts,
    length: usize,
    through_sequence: u64,
}

impl ChunkFragments {
    #[cfg(test)]
    fn new(parts: Vec<MutationPayload>, length: usize) -> Self {
        Self::new_through(parts, length, 0)
    }

    #[cfg(test)]
    fn new_through(mut parts: Vec<MutationPayload>, length: usize, through_sequence: u64) -> Self {
        let parts = if parts.len() == 1 {
            ChunkParts::Single(parts.pop().expect("ASSERT: one fixture fragment"))
        } else {
            ChunkParts::Fragmented(parts)
        };
        Self::from_parts(parts, length, through_sequence)
    }

    fn from_parts(parts: ChunkParts, length: usize, through_sequence: u64) -> Self {
        assert!(length != 0, "ASSERT: a SeqCDC Chunk is nonempty");
        let actual = parts.as_slice().iter().fold(0_usize, |total, part| {
            assert!(!part.is_empty(), "ASSERT: Chunk fragments are nonempty");
            total
                .checked_add(part.len())
                .expect("ASSERT: bounded Chunk fragment sum cannot overflow")
        });
        assert_eq!(actual, length, "ASSERT: Chunk fragment length is exact");
        Self {
            parts,
            length,
            through_sequence,
        }
    }

    const fn len(&self) -> usize {
        self.length
    }

    const fn is_empty(&self) -> bool {
        self.length == 0
    }

    const fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    fn first_byte(&self) -> u8 {
        self.parts.as_slice()[0].as_bytes()[0]
    }

    fn is_fill(&self) -> bool {
        let first = self.first_byte();
        let repeated = [first; 32];
        self.parts.as_slice().iter().all(|part| {
            let bytes = part.as_bytes();
            // Keep immediate rejection cheap; fixed-size comparisons let the
            // compiler vectorize long FILL scans without alignment or Unsafe.
            let head = bytes.len().min(8);
            if !bytes[..head].iter().all(|&byte| byte == first) {
                return false;
            }
            let mut blocks = bytes[head..].chunks_exact(repeated.len());
            blocks.all(|block| block == repeated)
                && blocks.remainder().iter().all(|&byte| byte == first)
        })
    }

    fn chunk_id(&self) -> ChunkId {
        let mut hasher = blake3::Hasher::new();
        for part in self.parts.as_slice() {
            hasher.update(part.as_bytes());
        }
        ChunkId::from_bytes(*hasher.finalize().as_bytes())
    }

    #[cfg(test)]
    fn materialize_new_chunk(&self) -> Result<Cow<'_, [u8]>, DurableNamespaceError> {
        if self.parts.as_slice().len() == 1 {
            return Ok(Cow::Borrowed(self.parts.as_slice()[0].as_bytes()));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.length)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for part in self.parts.as_slice() {
            bytes.extend_from_slice(part.as_bytes());
        }
        assert_eq!(
            bytes.len(),
            self.length,
            "ASSERT: new-Chunk coalescing preserves its exact length"
        );
        record_copy(CopyClass::ChunkFragmentCoalescing, bytes.len());
        Ok(Cow::Owned(bytes))
    }

    fn append_to_compression_region(&self, decoded: &mut Vec<u8>) {
        let start = decoded.len();
        for part in self.parts.as_slice() {
            decoded.extend_from_slice(part.as_bytes());
        }
        assert_eq!(
            decoded.len() - start,
            self.length,
            "ASSERT: Compression Region materialization preserves the Chunk length"
        );
        record_copy(CopyClass::CompressionRegionMaterialization, self.length);
    }

    fn contiguous_bytes(&self) -> Option<&[u8]> {
        (self.parts.as_slice().len() == 1).then(|| self.parts.as_slice()[0].as_bytes())
    }

    #[cfg(test)]
    fn materialize_fixture(&self) -> Vec<u8> {
        self.parts
            .as_slice()
            .iter()
            .flat_map(|part| part.as_bytes().iter().copied())
            .collect()
    }
}

fn prepare_compression_regions<'a>(
    new_chunks: &[&'a PendingWriteThroughChunk],
    workers: NonZeroUsize,
    admission: &WorkerPermits,
) -> Result<PreparedCompressionRegions<'a>, DurableNamespaceError> {
    let mut plans = Vec::<CompressionRegionPlan<'_>>::new();
    for chunk in new_chunks {
        let materialized = chunk.bytes.contiguous_bytes().is_none();
        let needs_region = plans.last().is_none_or(|region| {
            region.chunks.last().is_some_and(|previous| {
                previous.offset + previous.bytes.len() as u64 != chunk.offset
            }) || region
                .decoded_length
                .checked_add(chunk.bytes.len())
                .is_none_or(|length| length > COMPRESSION_REGION_TARGET_BYTES)
        });
        if needs_region {
            plans
                .try_reserve(1)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            plans.push(CompressionRegionPlan {
                chunks: Vec::new(),
                decoded_length: 0,
                materialized,
            });
        }
        let region = plans
            .last_mut()
            .expect("ASSERT: a new Chunk owns one Compression Region");
        region
            .chunks
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        region.materialized |= materialized;
        region.chunks.push(chunk);
        region.decoded_length = region
            .decoded_length
            .checked_add(chunk.bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
    }

    let mut prepared = PreparedCompressionRegions {
        borrowed: Vec::new(),
        materialized: Vec::new(),
        order: Vec::new(),
    };
    prepared
        .order
        .try_reserve_exact(plans.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut materializing = Vec::new();
    for plan in plans {
        if !plan.materialized {
            let mut chunks = Vec::new();
            chunks
                .try_reserve_exact(plan.chunks.len())
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            for chunk in plan.chunks {
                chunks.push(PrehashedChunk::new(
                    chunk.chunk_id,
                    chunk
                        .bytes
                        .contiguous_bytes()
                        .expect("ASSERT: a borrowed region contains contiguous Chunks"),
                ));
            }
            let ordinal = prepared.borrowed.len();
            prepared.borrowed.push(chunks);
            prepared
                .order
                .push(CompressionRegionOrder::Borrowed(ordinal));
            continue;
        }
        let ordinal = materializing.len();
        materializing.push(plan);
        prepared
            .order
            .push(CompressionRegionOrder::Materialized(ordinal));
    }
    let materializing_bytes = materializing.iter().try_fold(0_usize, |sum, plan| {
        sum.checked_add(plan.decoded_length)
            .ok_or(DurableNamespaceError::OutOfMemory)
    })?;
    // Copies need substantially more bytes per worker than fingerprint/codec
    // jobs. Small batches still own one permit; large batches keep parallelism.
    let copy_workers = NonZeroUsize::new(
        workers
            .get()
            .min((materializing_bytes / (512 * 1024)).max(1)),
    )
    .expect("ASSERT: materialization requests at least one worker");
    prepared.materialized = admission
        .map(materializing, copy_workers, materialize_compression_region)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(prepared)
}

fn materialize_compression_region(
    plan: CompressionRegionPlan<'_>,
) -> Result<MaterializedCompressionRegion, DurableNamespaceError> {
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(plan.decoded_length)
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut chunks = Vec::new();
    chunks
        .try_reserve_exact(plan.chunks.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    for chunk in plan.chunks {
        let start = decoded.len();
        chunk.bytes.append_to_compression_region(&mut decoded);
        chunks.push((chunk.chunk_id, start..decoded.len()));
    }
    assert_eq!(
        decoded.len(),
        plan.decoded_length,
        "ASSERT: one Compression Region is materialized exactly once"
    );
    Ok(MaterializedCompressionRegion { decoded, chunks })
}

/// Retains prepared region owners and splits only at a selected codec boundary.
/// `NoCandidate` uses identical grouping regardless of receive fragmentation.
fn ordinary_region_slices<'a>(
    regions: &[PrehashedAdaptiveRegion<'a>],
    ordinary: &[bool],
) -> Result<Vec<PrehashedAdaptiveRegion<'a>>, DurableNamespaceError> {
    let mut result = Vec::new();
    let mut ordinal = 0;
    for region in regions {
        let (chunks, decoded) = match *region {
            PrehashedAdaptiveRegion::Borrowed(chunks) => (chunks, None),
            PrehashedAdaptiveRegion::Contiguous(region) => {
                (region.chunks(), Some(region.decoded()))
            }
        };
        let mut position = 0;
        let mut byte_offset = 0;
        while position < chunks.len() {
            let start = position;
            let start_byte = byte_offset;
            let include = ordinary[ordinal + position];
            while position < chunks.len() && ordinary[ordinal + position] == include {
                byte_offset += chunks[position].bytes().len();
                position += 1;
            }
            if include {
                let chunks = &chunks[start..position];
                result.push(match decoded {
                    Some(decoded) => PrehashedAdaptiveRegion::Contiguous(
                        PrehashedContiguousRegion::new(chunks, &decoded[start_byte..byte_offset])
                            .map_err(|_| DurableNamespaceError::FrozenViewMismatch)?,
                    ),
                    None => PrehashedAdaptiveRegion::Borrowed(chunks),
                });
            }
        }
        ordinal += chunks.len();
    }
    assert_eq!(
        ordinal,
        ordinary.len(),
        "ASSERT: one encoding plan per prepared target"
    );
    Ok(result)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StableExtraction {
    FillContainer,
    DrainStable,
}

#[derive(Debug)]
struct IngestSegment {
    payload: MutationPayload,
    mutation_sequence: u64,
}

#[derive(Debug, Default)]
struct SegmentedIngestTail {
    segments: VecDeque<IngestSegment>,
    length: usize,
    #[cfg(test)]
    materialized_bytes: usize,
}

impl SegmentedIngestTail {
    fn len(&self) -> usize {
        self.length
    }

    fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn clear(&mut self) {
        self.segments.clear();
        self.length = 0;
        #[cfg(test)]
        {
            self.materialized_bytes = 0;
        }
    }

    fn push(&mut self, payload: MutationPayload, mutation_sequence: u64) {
        assert!(
            !payload.is_empty(),
            "ASSERT: an Ingest Tail segment is nonempty"
        );
        self.length = self
            .length
            .checked_add(payload.len())
            .expect("ASSERT: bounded Ingest Tail length cannot overflow");
        self.segments.push_back(IngestSegment {
            payload,
            mutation_sequence,
        });
    }

    fn front_bytes(&self) -> &[u8] {
        self.segments
            .front()
            .map_or(&[], |segment| segment.payload.as_bytes())
    }

    #[cfg(test)]
    fn take_prefix(&mut self, length: usize) -> Result<MutationPayload, DurableNamespaceError> {
        let fragments = self.take_prefix_fragments(length)?;
        let bytes = fragments.materialize_new_chunk()?.into_owned();
        self.materialized_bytes = self
            .materialized_bytes
            .checked_add(length)
            .expect("ASSERT: bounded materialized-byte counter cannot overflow");
        self.assert_valid();
        Ok(MutationPayload::from_owned_bytes(bytes))
    }

    fn take_prefix_fragments(
        &mut self,
        length: usize,
    ) -> Result<ChunkFragments, DurableNamespaceError> {
        assert!(length != 0, "ASSERT: consumed Ingest prefix is nonempty");
        if length > self.length {
            return Err(DurableNamespaceError::FrozenViewMismatch);
        }
        if self.front_bytes().len() >= length {
            let (part, sequence) = self.consume_front(length);
            return Ok(ChunkFragments::from_parts(
                ChunkParts::Single(part),
                length,
                sequence,
            ));
        }

        // Preflight only the consumed prefix. All fallible reservations precede
        // mutation, including the pathological fragment-coalescing fallback.
        let mut covered = 0_usize;
        let mut count = 0_usize;
        for segment in &self.segments {
            covered = covered
                .checked_add(segment.payload.len())
                .expect("ASSERT: bounded Ingest prefix length cannot overflow");
            count += 1;
            if covered >= length {
                break;
            }
        }
        assert!(
            covered >= length,
            "ASSERT: accounted Tail covers its consumed prefix"
        );
        let mut parts = Vec::new();
        let mut compact = Vec::new();
        let coalesce = count > MAX_CHUNK_FRAGMENTS_V1;
        if coalesce {
            compact
                .try_reserve_exact(length)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        } else {
            parts
                .try_reserve_exact(count)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        }
        let mut through_sequence = 0;
        let mut remaining = length;
        while remaining != 0 {
            let consumed = remaining.min(self.front_bytes().len());
            let (part, sequence) = self.consume_front(consumed);
            through_sequence = through_sequence.max(sequence);
            if coalesce {
                compact.extend_from_slice(part.as_bytes());
            } else {
                parts.push(part);
            }
            remaining -= consumed;
        }
        let parts = if coalesce {
            assert_eq!(
                compact.len(),
                length,
                "ASSERT: compaction preserves the complete Chunk"
            );
            record_copy(CopyClass::ChunkFragmentCoalescing, compact.len());
            ChunkParts::Single(MutationPayload::from_owned_bytes(compact))
        } else {
            ChunkParts::Fragmented(parts)
        };
        Ok(ChunkFragments::from_parts(parts, length, through_sequence))
    }

    fn consume_front(&mut self, length: usize) -> (MutationPayload, u64) {
        let front = self
            .segments
            .front_mut()
            .expect("ASSERT: accounted Ingest Tail owns a front segment");
        assert!(
            length != 0 && length <= front.payload.len(),
            "ASSERT: consumed prefix lies in front segment"
        );
        let sequence = front.mutation_sequence;
        let payload = if length == front.payload.len() {
            self.segments
                .pop_front()
                .expect("ASSERT: front was established")
                .payload
        } else {
            front
                .payload
                .checked_split_to(length)
                .expect("ASSERT: consumed fragment lies inside front segment")
        };
        self.length = self
            .length
            .checked_sub(length)
            .expect("ASSERT: Tail byte accounting covers the consumed prefix");
        assert_eq!(
            self.segments.is_empty(),
            self.is_empty(),
            "ASSERT: Tail emptiness matches its byte count"
        );
        (payload, sequence)
    }

    #[cfg(test)]
    fn materialized_bytes(&self) -> usize {
        self.materialized_bytes
    }

    // Independently audit the remaining Tail at a stable-batch boundary, rather
    // than rescanning the entire retained suffix after each individual Chunk.
    fn assert_valid(&self) {
        let actual = self.segments.iter().fold(0_usize, |total, segment| {
            assert!(
                !segment.payload.is_empty(),
                "ASSERT: Ingest Tail cannot retain empty segments"
            );
            total
                .checked_add(segment.payload.len())
                .expect("ASSERT: bounded Ingest Tail segment sum cannot overflow")
        });
        assert_eq!(
            actual, self.length,
            "ASSERT: cached Ingest Tail length must match its segments"
        );
        assert_eq!(
            self.segments.is_empty(),
            self.is_empty(),
            "ASSERT: Ingest Tail emptiness must match its byte count"
        );
    }
}

fn segmented_seqcdc_cut(tail: &SegmentedIngestTail) -> usize {
    assert!(
        tail.len() > CDC_MAXIMUM_BYTES,
        "ASSERT: stable SeqCDC scan owns more than one maximum Chunk"
    );
    let segments = || {
        tail.segments
            .iter()
            .map(|segment| segment.payload.as_bytes())
    };
    if seqcdc_force_scalar() {
        seqcdc_cut_segmented_scalar(segments(), tail.len(), SEQCDC_CONFIG_V1)
    } else {
        seqcdc_cut_segmented(segments(), tail.len(), SEQCDC_CONFIG_V1)
    }
}

fn take_next_stable_seqcdc_chunk(
    tail: &mut SegmentedIngestTail,
) -> Result<Option<ChunkFragments>, DurableNamespaceError> {
    if tail.len() <= CDC_MAXIMUM_BYTES {
        return Ok(None);
    }
    let stable_before = tail.len() - CDC_MAXIMUM_BYTES;
    let length = if tail.front_bytes().len() >= CDC_MAXIMUM_BYTES {
        if seqcdc_force_scalar() {
            seqcdc_cut_scalar(tail.front_bytes(), SEQCDC_CONFIG_V1)
        } else {
            seqcdc_cut(tail.front_bytes(), SEQCDC_CONFIG_V1)
        }
    } else {
        segmented_seqcdc_cut(tail)
    };
    if length > stable_before {
        return Ok(None);
    }
    tail.take_prefix_fragments(length).map(Some)
}

#[derive(Debug)]
struct StableChunk {
    offset: u64,
    bytes: ChunkFragments,
}

fn take_stable_chunk_batch(
    state: &mut WriteThroughStream,
    maximum_bytes: usize,
) -> Result<Vec<StableChunk>, DurableNamespaceError> {
    assert!(maximum_bytes != 0, "ASSERT: stable batch budget is nonzero");
    let mut batch = Vec::new();
    let mut batch_bytes = 0_usize;
    while let Some(bytes) = take_next_stable_seqcdc_chunk(&mut state.tail)? {
        let offset = state.tail_offset;
        state.tail_offset = state
            .tail_offset
            .checked_add(
                u64::try_from(bytes.len()).expect("ASSERT: bounded SeqCDC Chunk length fits u64"),
            )
            .ok_or(DurableNamespaceError::ArithmeticOverflow)?;
        batch_bytes = batch_bytes
            .checked_add(bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
        batch
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        batch.push(StableChunk { offset, bytes });
        if batch_bytes >= maximum_bytes {
            break;
        }
    }
    state.tail.assert_valid();
    Ok(batch)
}

/// A serial FILL pass carries its result into hashing without repeating FILL classification.
fn classify_stable_chunk_batch(
    batch: &[StableChunk],
    budget: NonZeroUsize,
    permits: &WorkerPermits,
    telemetry: &CpuPhaseTelemetry,
) -> Result<(Vec<Option<ChunkId>>, usize), DurableNamespaceError> {
    assert!(
        !batch.is_empty(),
        "ASSERT: classification batch is nonempty"
    );
    let mut lease = permits.acquire(NonZeroUsize::MIN);
    telemetry.record_permit(&lease);
    let mut phase = telemetry.begin();
    let mut ordinals = Vec::new();
    ordinals
        .try_reserve_exact(batch.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    let mut classified = Vec::new();
    classified
        .try_reserve_exact(batch.len())
        .map_err(|_| DurableNamespaceError::OutOfMemory)?;
    classified.resize(batch.len(), None);
    let mut hash_bytes = 0_usize;
    for (ordinal, chunk) in batch.iter().enumerate() {
        assert!(!chunk.bytes.is_empty(), "ASSERT: SeqCDC Chunk is nonempty");
        if !chunk.bytes.is_fill() {
            ordinals.push(ordinal);
            hash_bytes = hash_bytes
                .checked_add(chunk.bytes.len())
                .ok_or(DurableNamespaceError::OutOfMemory)?;
        }
    }
    let desired = stable_hash_workers(hash_bytes, ordinals.len(), budget);
    if desired.get() > 1 {
        // Never wait for another admission while holding the serial-pass permit.
        drop(phase);
        drop(lease);
        lease = permits.acquire(desired);
        telemetry.record_permit(&lease);
        phase = telemetry.begin();
    }
    let workers = lease.workers().get();
    if workers == 1 {
        for ordinal in ordinals {
            classified[ordinal] = Some(batch[ordinal].bytes.chunk_id());
        }
    } else {
        let next = AtomicUsize::new(0);
        let results = (0..workers)
            .into_par_iter()
            .map(|worker| {
                struct Retire<'a, 'b>(&'a WorkerPermitLease<'b>, usize);
                impl Drop for Retire<'_, '_> {
                    fn drop(&mut self) {
                        if self.1 != 0 {
                            self.0.retire_worker();
                        }
                    }
                }
                let _worker = Retire(&lease, worker);
                let mut completed = Vec::new();
                loop {
                    let start = next.fetch_add(4, Ordering::Relaxed);
                    if start >= ordinals.len() {
                        break;
                    }
                    for &ordinal in &ordinals[start..(start + 4).min(ordinals.len())] {
                        completed.push((ordinal, batch[ordinal].bytes.chunk_id()));
                    }
                }
                completed
            })
            .collect::<Vec<_>>();
        let mut completed_count = 0;
        for (ordinal, id) in results.into_iter().flatten() {
            classified[ordinal] = Some(id);
            completed_count += 1;
        }
        assert_eq!(
            completed_count,
            ordinals.len(),
            "ASSERT: every non-FILL Chunk has one hash owner"
        );
    }
    drop(phase);
    drop(lease);
    Ok((classified, workers))
}

fn stable_hash_workers(bytes: usize, chunks: usize, budget: NonZeroUsize) -> NonZeroUsize {
    // Measured crossover: 1 MiB -> 2 workers, 4 MiB -> 4; large batches may
    // use the entire pool. Balance coordination growing with worker count
    // against hash time decreasing with it, rather than imposing a CPU cap.
    let by_bytes = bytes.div_ceil(256 * 1024).isqrt().max(1);
    NonZeroUsize::new(budget.get().min(chunks.div_ceil(4).max(1)).min(by_bytes))
        .expect("ASSERT: hash classification always has one worker")
}

/// Append-only staging until the whole owner is detached or discarded.
/// Each append proves its new range and preserves the cached byte sum.
#[derive(Debug, Default)]
struct PendingWriteThrough {
    chunks: Vec<PendingWriteThroughChunk>,
    bytes: usize,
}

impl PendingWriteThrough {
    fn validate_append(previous_end: Option<u64>, chunk: &PendingWriteThroughChunk) -> u64 {
        assert!(
            !chunk.bytes.is_empty() && chunk.bytes.len() <= CDC_MAXIMUM_BYTES,
            "ASSERT: pending write-through Chunk violates SeqCDC length bounds"
        );
        assert!(
            previous_end.is_none_or(|end| end <= chunk.offset),
            "ASSERT: pending write-through Chunks must be ordered and disjoint"
        );
        chunk
            .offset
            .checked_add(u64::try_from(chunk.bytes.len()).expect("bounded Chunk fits u64"))
            .expect("ASSERT: pending write-through Chunk range cannot overflow")
    }

    fn push(&mut self, chunk: PendingWriteThroughChunk) -> Result<(), DurableNamespaceError> {
        let previous_end = self.chunks.last().map(|previous| {
            previous.offset + u64::try_from(previous.bytes.len()).expect("bounded Chunk fits u64")
        });
        Self::validate_append(previous_end, &chunk);
        let bytes = self
            .bytes
            .checked_add(chunk.bytes.len())
            .ok_or(DurableNamespaceError::OutOfMemory)?;
        assert!(
            bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
            "ASSERT: pending write-through payload exceeds its Container bound"
        );
        self.chunks
            .try_reserve(1)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        self.chunks.push(chunk);
        self.bytes = bytes;
        Ok(())
    }

    fn clear(&mut self) {
        self.chunks.clear();
        self.bytes = 0;
    }

    fn assert_bounded(&self) {
        assert_eq!(
            self.chunks.is_empty(),
            self.bytes == 0,
            "ASSERT: pending write-through count and bytes must agree"
        );
        assert!(
            self.bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
            "ASSERT: pending write-through payload exceeds its Container bound"
        );
    }
}

#[derive(Debug, Default)]
#[repr(align(64))]
pub(super) struct WriteThroughStream {
    inode: Option<InodeId>,
    placement: Option<ContainerPlacement>,
    last_mutation_sequence: Option<u64>,
    next_offset: u64,
    tail_offset: u64,
    tail: SegmentedIngestTail,
    pending: PendingWriteThrough,
}

#[derive(Debug, Default)]
struct WriteThroughRegistry {
    lanes: BTreeMap<InodeId, WriteThroughLane>,
    overflow: Arc<Mutex<WriteThroughStream>>,
    sealed: VecDeque<Instant>,
    degraded: bool,
    next_touch: u64,
}

struct WriteThroughStatusSnapshot {
    lanes: Vec<Arc<Mutex<WriteThroughStream>>>,
    overflow: Arc<Mutex<WriteThroughStream>>,
    sealed_uncommitted_containers: usize,
    oldest_sealed_age: Option<Duration>,
    degraded: bool,
}

#[derive(Debug)]
struct WriteThroughLane {
    stream: Arc<Mutex<WriteThroughStream>>,
    last_touch: u64,
}

impl WriteThroughRegistry {
    fn status_snapshot(&self) -> WriteThroughStatusSnapshot {
        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(self.lanes.len())
            .expect("ASSERT: bounded status Lane snapshot allocation succeeds");
        lanes.extend(self.lanes.values().map(|lane| Arc::clone(&lane.stream)));
        WriteThroughStatusSnapshot {
            lanes,
            overflow: Arc::clone(&self.overflow),
            sealed_uncommitted_containers: self.sealed.len(),
            oldest_sealed_age: self.sealed.front().map(Instant::elapsed),
            degraded: self.degraded,
        }
    }

    fn acquire_lane(&mut self, inode: InodeId) -> Arc<Mutex<WriteThroughStream>> {
        let touch = self.next_touch;
        self.next_touch = self
            .next_touch
            .checked_add(1)
            .expect("ASSERT: Ingest Lane touch sequence cannot overflow");
        if let Some(lane) = self.lanes.get_mut(&inode) {
            lane.last_touch = touch;
            return Arc::clone(&lane.stream);
        }
        if self
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned")
            .inode
            == Some(inode)
        {
            return Arc::clone(&self.overflow);
        }
        if self.lanes.len() >= MAX_ACTIVE_INGEST_LANES_V1 {
            let eviction_grace = u64::try_from(MAX_ACTIVE_INGEST_LANES_V1 * 2)
                .expect("ASSERT: bounded Ingest Lane grace fits u64");
            let evicted = self
                .lanes
                .iter()
                .filter(|(_, lane)| Arc::strong_count(&lane.stream) == 1)
                .filter(|(_, lane)| touch.saturating_sub(lane.last_touch) > eviction_grace)
                .min_by_key(|(candidate_inode, lane)| (lane.last_touch, **candidate_inode))
                .map(|(candidate_inode, _)| *candidate_inode);
            if let Some(evicted) = evicted {
                let removed = self.lanes.remove(&evicted);
                assert!(removed.is_some(), "ASSERT: selected Ingest Lane vanished");
            } else {
                return Arc::clone(&self.overflow);
            }
        }
        let lane = Arc::new(Mutex::new(WriteThroughStream::default()));
        assert!(
            self.lanes
                .insert(
                    inode,
                    WriteThroughLane {
                        stream: Arc::clone(&lane),
                        last_touch: touch,
                    },
                )
                .is_none(),
            "ASSERT: a new Ingest Lane cannot replace an existing inode lane"
        );
        assert!(
            self.lanes.len() <= MAX_ACTIVE_INGEST_LANES_V1,
            "ASSERT: registered Ingest Lanes exceed the process memory budget"
        );
        lane
    }
}

pub(super) struct WriteThroughIngest<C> {
    containers: ContainerRepository<C>,
    container_generations: ContainerGenerationAllocator<C>,
    index: Arc<dyn ManifestReaderPolicy<C>>,
    worker_budget: NonZeroUsize,
    worker_permits: Arc<WorkerPermits>,
    active_writers: AtomicUsize,
    shared_batch_bytes: Arc<AtomicUsize>,
    hash_batches: AtomicUsize,
    maximum_hash_workers: AtomicUsize,
    hash_cpu: CpuPhaseTelemetry,
    encode_cpu: CpuPhaseTelemetry,
    planning_cpu: CpuPhaseTelemetry,
    materialization_wall_ns: AtomicU64,
    registry: Mutex<WriteThroughRegistry>,
    queue: Arc<IngestQueue>,
    publication_queue: Arc<PublicationQueue>,
    namespace: OnceLock<Weak<Namespace>>,
    #[cfg(test)]
    pub(super) after_inline_stage: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    online_dependency_proofs: Arc<OnlineDependencyProofs>,
}

#[derive(Debug)]
enum IngestJobKind {
    WriteFragment(IngestWriteFragment),
    WriteBatch { fragments: Vec<IngestWriteFragment> },
    Truncate,
}

#[derive(Debug)]
struct IngestWriteFragment {
    offset: u64,
    bytes: MutationPayload,
    mutation_sequence: u64,
    placement: ContainerPlacement,
}

#[derive(Debug)]
struct IngestJob {
    inode: InodeId,
    mutation_sequence: u64,
    kind: IngestJobKind,
}

impl IngestJob {
    fn buffered_bytes(&self) -> usize {
        match &self.kind {
            IngestJobKind::WriteFragment(fragment) => fragment.bytes.len(),
            IngestJobKind::WriteBatch { fragments } => {
                fragments.iter().fold(0_usize, |total, fragment| {
                    total
                        .checked_add(fragment.bytes.len())
                        .expect("ASSERT: bounded Ingest Batch bytes cannot overflow")
                })
            }
            IngestJobKind::Truncate => 0,
        }
    }
}

#[derive(Debug)]
struct OpenIngestBatch {
    opened_at: Instant,
    fragments: Vec<IngestWriteFragment>,
    buffered_bytes: usize,
    last_mutation_sequence: u64,
}

impl OpenIngestBatch {
    fn into_job(self, inode: InodeId) -> IngestJob {
        assert!(
            !self.fragments.is_empty() && self.buffered_bytes != 0,
            "ASSERT: only a nonempty Ingest Batch may be sealed"
        );
        IngestJob {
            inode,
            mutation_sequence: self.last_mutation_sequence,
            kind: IngestJobKind::WriteBatch {
                fragments: self.fragments,
            },
        }
    }
}

#[derive(Debug, Default)]
struct InodeJobQueue {
    pending: VecDeque<IngestJob>,
    open: Option<OpenIngestBatch>,
    in_flight: bool,
    last_enqueued_sequence: u64,
    completed_sequence: u64,
}

#[derive(Debug, Default)]
struct IngestQueueState {
    inodes: BTreeMap<InodeId, InodeJobQueue>,
    writable_handles: BTreeMap<InodeId, usize>,
    ready: VecDeque<InodeId>,
    buffered_bytes: usize,
    ingest_batches: u64,
    ingest_fragments: u64,
    maximum_ingest_batch_bytes: usize,
    minimum_ingest_batch_target_bytes: usize,
    maximum_ingest_batch_target_bytes: usize,
    maximum_ingest_ring_slots: usize,
    ingest_ring_wait_ns: u64,
    shutdown: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct IngestQueueStatus {
    buffered_bytes: usize,
    ingest_batches: u64,
    ingest_fragments: u64,
    maximum_ingest_batch_bytes: usize,
    minimum_ingest_batch_target_bytes: usize,
    maximum_ingest_batch_target_bytes: usize,
    maximum_ingest_ring_slots: usize,
    ingest_ring_wait_ns: u64,
}

#[derive(Debug)]
struct IngestQueue {
    #[cfg(test)]
    before_fragment_wait: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    state: Mutex<IngestQueueState>,
    work_available: Condvar,
    space_available: Condvar,
    completed: Condvar,
}

type SharedPublicationError = Arc<DurableNamespaceError>;
type PublicationResult = Result<(), SharedPublicationError>;
type PublicationCompletions = Vec<std::sync::mpsc::Receiver<PublicationResult>>;

struct PublicationFences {
    retirement_targets: BTreeMap<InodeId, u64>,
    barriers: BTreeMap<InodeId, u64>,
    receivers: PublicationCompletions,
}

fn unwrap_shared_publication_error(error: SharedPublicationError) -> DurableNamespaceError {
    Arc::try_unwrap(error).unwrap_or_else(|error| {
        DurableNamespaceError::Io(std::io::Error::other(format!(
            "shared publication failure: {error:?}"
        )))
    })
}

fn recv_publication_completion(
    completion: &std::sync::mpsc::Receiver<PublicationResult>,
) -> Result<(), DurableNamespaceError> {
    completion
        .recv()
        .map_err(|_| DurableNamespaceError::FrozenViewMismatch)?
        .map_err(unwrap_shared_publication_error)
}

#[derive(Debug)]
struct DetachedContainerWork {
    inode: InodeId,
    first_chunk_sequence: u64,
    completion: Option<std::sync::mpsc::SyncSender<PublicationResult>>,
    through_sequence: u64,
    publication_ordinal: u64,
    chunks: Vec<PendingWriteThroughChunk>,
    payload_bytes: usize,
}

impl DetachedContainerWork {
    fn new(
        inode: InodeId,
        through_sequence: u64,
        chunks: Vec<PendingWriteThroughChunk>,
        payload_bytes: usize,
    ) -> Self {
        let (actual, first_chunk_sequence) = validate_pending_chunks(&chunks);
        assert_eq!(
            actual, payload_bytes,
            "ASSERT: pending write-through byte accounting must be exact"
        );
        assert!(
            !chunks.is_empty() && payload_bytes != 0,
            "ASSERT: detached Container work must contain payload"
        );
        assert!(
            payload_bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
            "ASSERT: detached Container exceeds its pre-format payload bound"
        );
        Self {
            inode,
            through_sequence,
            publication_ordinal: 0,
            first_chunk_sequence: first_chunk_sequence
                .expect("ASSERT: detached work contains Chunks"),
            completion: None,
            chunks,
            payload_bytes,
        }
    }
}

#[derive(Debug)]
struct PublicationMember {
    inode: InodeId,
    through_sequence: u64,
    first_chunk_sequence: u64,
    completion: Option<std::sync::mpsc::SyncSender<PublicationResult>>,
    chunks: Vec<PendingWriteThroughChunk>,
    payload_bytes: usize,
    advanced: bool,
}

#[derive(Debug)]
struct PublicationGroup {
    id: u64,
    members: Vec<PublicationMember>,
    payload_bytes: usize,
}

#[derive(Debug)]
enum PublicationPendingItem {
    Single(DetachedContainerWork),
    GroupMember {
        group_id: u64,
        first_chunk_sequence: u64,
    },
}

impl PublicationPendingItem {
    fn as_single(&self) -> Option<&DetachedContainerWork> {
        match self {
            Self::Single(work) => Some(work),
            Self::GroupMember { .. } => None,
        }
    }

    fn is_single(&self) -> bool {
        matches!(self, Self::Single(_))
    }
}

#[derive(Debug)]
enum PublicationUnit {
    Single(DetachedContainerWork),
    Group(PublicationGroup),
}

#[derive(Debug)]
struct PartialCandidate {
    inode: InodeId,
    through_sequence: u64,
    first_chunk_sequence: u64,
    chunks: Vec<PendingWriteThroughChunk>,
    payload_bytes: usize,
    placement: ContainerPlacement,
    advanced: bool,
    sender: Option<std::sync::mpsc::SyncSender<PublicationResult>>,
    receiver: Option<std::sync::mpsc::Receiver<PublicationResult>>,
}

fn partial_placement_bucket(placement: ContainerPlacement) -> u8 {
    match placement {
        ContainerPlacement::Data => 0,
        ContainerPlacement::SmallFile => 1,
    }
}

impl PartialCandidate {
    fn new(
        inode: InodeId,
        through_sequence: u64,
        chunks: Vec<PendingWriteThroughChunk>,
        payload_bytes: usize,
        placement: ContainerPlacement,
        advanced: bool,
    ) -> Self {
        let (actual, first_chunk_sequence) = validate_pending_chunks(&chunks);
        assert_eq!(
            actual, payload_bytes,
            "ASSERT: partial publication byte accounting must be exact"
        );
        assert!(
            payload_bytes <= PARTIAL_BATCH_BUDGET_BYTES_V1,
            "ASSERT: one partial publication candidate fits its local batch budget"
        );
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        Self {
            inode,
            through_sequence,
            first_chunk_sequence: first_chunk_sequence
                .expect("ASSERT: partial publication candidate contains Chunks"),
            chunks,
            payload_bytes,
            placement,
            advanced,
            sender: Some(sender),
            receiver: Some(receiver),
        }
    }

    fn into_member(self) -> PublicationMember {
        PublicationMember {
            inode: self.inode,
            through_sequence: self.through_sequence,
            first_chunk_sequence: self.first_chunk_sequence,
            completion: self.sender,
            chunks: self.chunks,
            payload_bytes: self.payload_bytes,
            advanced: self.advanced,
        }
    }

    fn into_single(self) -> DetachedContainerWork {
        let mut work = DetachedContainerWork::new(
            self.inode,
            self.through_sequence,
            self.chunks,
            self.payload_bytes,
        );
        work.completion = self.sender;
        work
    }
}

#[derive(Debug, Default)]
struct InodePublicationQueue {
    pending: VecDeque<PublicationPendingItem>,
    in_flight: BTreeMap<u64, u64>,
    barrier: Option<u64>,
    next_publication_ordinal: u64,
    next_retirement_ordinal: u64,
    last_enqueued_sequence: u64,
    drain_candidates: usize,
    ready: bool,
}

#[derive(Debug, Default)]
struct PublicationQueueState {
    inodes: BTreeMap<InodeId, InodePublicationQueue>,
    ready_inodes: VecDeque<InodeId>,
    groups: BTreeMap<u64, PublicationGroup>,
    active_groups: BTreeMap<u64, u64>,
    ready_groups: VecDeque<u64>,
    next_group_id: u64,
    buffered_bytes: usize,
    direct_drain_waiters: usize,
    shutdown: bool,
}

#[derive(Debug)]
struct PublicationQueue {
    shared_batch_bytes: Arc<AtomicUsize>,
    state: Mutex<PublicationQueueState>,
    work_available: Condvar,
    space_available: Condvar,
    completed: Condvar,
    drain_available: Condvar,
}

impl PublicationQueue {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_shared_batch_bytes(Arc::new(AtomicUsize::new(0)))
    }

    fn with_shared_batch_bytes(shared_batch_bytes: Arc<AtomicUsize>) -> Self {
        Self {
            shared_batch_bytes,
            state: Mutex::new(PublicationQueueState::default()),
            work_available: Condvar::new(),
            space_available: Condvar::new(),
            completed: Condvar::new(),
            drain_available: Condvar::new(),
        }
    }

    fn charged_bytes(state: &PublicationQueueState, shared_batch_bytes: usize) -> usize {
        state
            .buffered_bytes
            .checked_add(shared_batch_bytes)
            .expect("ASSERT: charged detached publication bytes cannot overflow")
    }

    fn release_local_bytes(&self, bytes: usize) {
        let previous = self.shared_batch_bytes.fetch_sub(bytes, Ordering::Relaxed);
        assert!(
            previous >= bytes,
            "ASSERT: shared batch release cannot underflow its reservation"
        );
        self.space_available.notify_all();
    }

    fn shared_batch_bytes(&self) -> usize {
        self.shared_batch_bytes.load(Ordering::Relaxed)
    }

    fn reserve_local_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: publication queue lock poisoned while reserving shared bytes");
        loop {
            let charged = Self::charged_bytes(&state, self.shared_batch_bytes());
            if charged
                .checked_add(bytes)
                .is_some_and(|total| total <= DETACHED_CONTAINER_BUDGET_BYTES_V1)
            {
                break;
            }
            state = self.space_available.wait(state).expect(
                "ASSERT: publication queue lock poisoned while applying shared backpressure",
            );
        }
        assert!(
            !state.shutdown,
            "ASSERT: cannot reserve shared publication bytes after scheduler shutdown"
        );
        self.shared_batch_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn take_reserved_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let previous = self.shared_batch_bytes.fetch_sub(bytes, Ordering::Relaxed);
        assert!(
            previous >= bytes,
            "ASSERT: publication handoff cannot consume more reserved bytes than were held"
        );
    }

    fn begin_drain_candidate(&self, inode: InodeId) {
        let mut state = self.state.lock().expect(
            "ASSERT: detached publication queue lock poisoned while reserving a drain slot",
        );
        let inode_queue = state.inodes.entry(inode).or_default();
        inode_queue.drain_candidates = inode_queue
            .drain_candidates
            .checked_add(1)
            .expect("ASSERT: commit-drain publication markers cannot overflow");
    }

    fn clear_drain_candidate(&self, inode: InodeId) {
        let mut state = self.state.lock().expect(
            "ASSERT: detached publication queue lock poisoned while releasing a drain slot",
        );
        let inode_queue = state
            .inodes
            .get_mut(&inode)
            .expect("ASSERT: commit-drain publication marker must exist");
        assert!(
            inode_queue.drain_candidates > 0,
            "ASSERT: commit-drain publication marker cannot be cleared without a candidate"
        );
        inode_queue.drain_candidates -= 1;
        self.drain_available.notify_all();
    }

    fn enqueue(&self, work: DetachedContainerWork) {
        let _ = self.enqueue_work(work, 0, false);
    }

    #[cfg(test)]
    fn enqueue_with_reservation(&self, work: DetachedContainerWork, reserved_bytes: usize) -> u64 {
        self.enqueue_work(work, reserved_bytes, false)
    }

    fn enqueue_drain_candidate(&self, work: DetachedContainerWork, reserved_bytes: usize) -> u64 {
        self.enqueue_work(work, reserved_bytes, true)
    }

    fn enqueue_work(
        &self,
        mut work: DetachedContainerWork,
        reserved_bytes: usize,
        drain_candidate: bool,
    ) -> u64 {
        let inode = work.inode;
        let work_bytes = work.payload_bytes;
        assert!(
            reserved_bytes <= work_bytes,
            "ASSERT: enqueue cannot consume more reserved bytes than the handed-off work"
        );
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned");
        loop {
            if !drain_candidate
                && !state.shutdown
                && state
                    .inodes
                    .get(&inode)
                    .is_some_and(|queue| queue.drain_candidates > 0)
            {
                state.direct_drain_waiters = state
                    .direct_drain_waiters
                    .checked_add(1)
                    .expect("ASSERT: direct publication drain waiters cannot overflow");
                state = self.drain_available.wait(state).expect(
                    "ASSERT: detached publication queue lock poisoned while waiting for a drain slot",
                );
                state.direct_drain_waiters = state
                    .direct_drain_waiters
                    .checked_sub(1)
                    .expect("ASSERT: direct publication drain waiters cannot underflow");
                continue;
            }
            let shared_batch_bytes = self.shared_batch_bytes();
            assert!(
                shared_batch_bytes >= reserved_bytes,
                "ASSERT: shared publication reservation vanished before handoff"
            );
            let other_shared_batch_bytes = shared_batch_bytes
                .checked_sub(reserved_bytes)
                .expect("ASSERT: shared publication reservation cannot underflow");
            if Self::charged_bytes(&state, other_shared_batch_bytes)
                .checked_add(work_bytes)
                .is_some_and(|total| total <= DETACHED_CONTAINER_BUDGET_BYTES_V1)
            {
                break;
            }
            state = self.space_available.wait(state).expect(
                "ASSERT: detached publication queue lock poisoned while applying backpressure",
            );
        }
        assert!(
            !state.shutdown,
            "ASSERT: cannot enqueue detached Container work after scheduler shutdown"
        );
        self.take_reserved_bytes(reserved_bytes);
        state.buffered_bytes = state
            .buffered_bytes
            .checked_add(work_bytes)
            .expect("ASSERT: detached publication bytes cannot overflow");
        let inode_queue = state.inodes.entry(inode).or_default();
        assert!(
            work.through_sequence >= inode_queue.last_enqueued_sequence,
            "ASSERT: detached per-inode publication sequence cannot move backwards"
        );
        inode_queue.last_enqueued_sequence = work.through_sequence;
        work.publication_ordinal = inode_queue.next_publication_ordinal;
        inode_queue.next_publication_ordinal = inode_queue
            .next_publication_ordinal
            .checked_add(1)
            .expect("ASSERT: detached publication ordinal cannot overflow");
        inode_queue
            .pending
            .push_back(PublicationPendingItem::Single(work));
        let retirement_target = inode_queue.next_publication_ordinal;
        let drain_slot_released = if drain_candidate {
            assert!(
                inode_queue.drain_candidates > 0,
                "ASSERT: commit-drain publication must consume its own marker"
            );
            inode_queue.drain_candidates -= 1;
            inode_queue.drain_candidates == 0
        } else {
            false
        };
        if schedule_publication_inodes(&mut state) {
            self.work_available.notify_one();
        }
        if drain_slot_released {
            self.drain_available.notify_all();
        }
        retirement_target
    }

    fn try_enqueue_group(
        &self,
        mut group: PublicationGroup,
        reserved_bytes: usize,
    ) -> Result<(u64, BTreeMap<InodeId, u64>), PublicationGroup> {
        let group_bytes = group.payload_bytes;
        assert!(
            group.members.len() > 1,
            "ASSERT: grouped publication requires multiple members"
        );
        assert!(
            reserved_bytes <= group_bytes,
            "ASSERT: grouped publication cannot consume more reserved bytes than its members"
        );
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: grouped publication queue lock poisoned");
        loop {
            let shared_batch_bytes = self.shared_batch_bytes();
            assert!(
                shared_batch_bytes >= reserved_bytes,
                "ASSERT: grouped publication reservation vanished before handoff"
            );
            let other_shared_batch_bytes = shared_batch_bytes
                .checked_sub(reserved_bytes)
                .expect("ASSERT: grouped publication reservation cannot underflow");
            if Self::charged_bytes(&state, other_shared_batch_bytes)
                .checked_add(group_bytes)
                .is_some_and(|total| total <= DETACHED_CONTAINER_BUDGET_BYTES_V1)
            {
                break;
            }
            state = self.space_available.wait(state).expect(
                "ASSERT: grouped publication queue lock poisoned while applying backpressure",
            );
        }
        assert!(
            !state.shutdown,
            "ASSERT: cannot enqueue grouped Container work after scheduler shutdown"
        );
        if group.members.iter().any(|member| {
            state.inodes.get(&member.inode).is_some_and(|inode_queue| {
                !inode_queue.pending.is_empty()
                    || !inode_queue.in_flight.is_empty()
                    || inode_queue.barrier.is_some()
                    || member.through_sequence < inode_queue.last_enqueued_sequence
            })
        }) {
            return Err(group);
        }
        let group_id = state.next_group_id;
        state.next_group_id = state
            .next_group_id
            .checked_add(1)
            .expect("ASSERT: publication group id cannot overflow");
        group.id = group_id;
        self.take_reserved_bytes(reserved_bytes);
        state.buffered_bytes = state
            .buffered_bytes
            .checked_add(group_bytes)
            .expect("ASSERT: grouped publication bytes cannot overflow");
        let mut drain_slot_released = false;
        let mut retirement_targets = BTreeMap::new();
        for member in &group.members {
            let inode_queue = state.inodes.entry(member.inode).or_default();
            inode_queue.last_enqueued_sequence = member.through_sequence;
            inode_queue.barrier = Some(group_id);
            inode_queue
                .pending
                .push_back(PublicationPendingItem::GroupMember {
                    group_id,
                    first_chunk_sequence: member.first_chunk_sequence,
                });
            retirement_targets.insert(member.inode, inode_queue.next_publication_ordinal);
            if inode_queue.drain_candidates > 0 {
                inode_queue.drain_candidates -= 1;
                drain_slot_released |= inode_queue.drain_candidates == 0;
            }
        }
        let first_chunk_sequence = group
            .members
            .iter()
            .map(|member| member.first_chunk_sequence)
            .min()
            .expect("ASSERT: grouped publication contains members");
        state.active_groups.insert(group_id, first_chunk_sequence);
        state.groups.insert(group_id, group);
        state.ready_groups.push_back(group_id);
        self.work_available.notify_one();
        if drain_slot_released {
            self.drain_available.notify_all();
        }
        Ok((group_id, retirement_targets))
    }

    fn next_unit(&self) -> Option<PublicationUnit> {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned");
        loop {
            if let Some(group_id) = state.ready_groups.pop_front() {
                let group = state
                    .groups
                    .remove(&group_id)
                    .expect("ASSERT: ready grouped publication remains pending");
                assert!(
                    state.active_groups.contains_key(&group_id),
                    "ASSERT: grouped publication has an active barrier"
                );
                return Some(PublicationUnit::Group(group));
            }
            if let Some(inode) = state.ready_inodes.pop_front() {
                let per_inode_limit = publication_window(&state);
                let inode_queue = state
                    .inodes
                    .get_mut(&inode)
                    .expect("ASSERT: ready publication inode must own a queue");
                inode_queue.ready = false;
                if inode_queue.barrier.is_some()
                    || inode_queue.pending.is_empty()
                    || inode_queue.in_flight.len() >= per_inode_limit
                {
                    continue;
                }
                match inode_queue.pending.pop_front() {
                    Some(PublicationPendingItem::Single(work)) => {
                        assert!(
                            inode_queue
                                .in_flight
                                .insert(work.publication_ordinal, work.first_chunk_sequence)
                                .is_none(),
                            "ASSERT: detached publication ordinal is unique per inode"
                        );
                        if schedule_publication_inodes(&mut state) {
                            self.work_available.notify_one();
                        }
                        return Some(PublicationUnit::Single(work));
                    }
                    Some(group_member) => {
                        if let PublicationPendingItem::GroupMember { group_id, .. } = group_member
                            && state.groups.contains_key(&group_id)
                            && !state.ready_groups.contains(&group_id)
                        {
                            state.ready_groups.push_back(group_id);
                        }
                        continue;
                    }
                    None => continue,
                }
            }
            if state.shutdown {
                return None;
            }
            state = self
                .work_available
                .wait(state)
                .expect("ASSERT: detached publication queue lock poisoned while waiting for work");
        }
    }

    #[cfg(test)]
    fn next_work(&self) -> Option<DetachedContainerWork> {
        match self.next_unit()? {
            PublicationUnit::Single(work) => Some(work),
            PublicationUnit::Group(_) => {
                panic!("ASSERT: next_work is only used by single-publication tests")
            }
        }
    }

    fn wait_for_retirement_turn(&self, work: &DetachedContainerWork) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned");
        loop {
            let inode_queue = state
                .inodes
                .get(&work.inode)
                .expect("ASSERT: retiring publication inode retains queue state");
            if inode_queue.next_retirement_ordinal == work.publication_ordinal {
                return;
            }
            state = self.completed.wait(state).expect(
                "ASSERT: detached publication queue lock poisoned while ordering completion",
            );
        }
    }

    fn finish(&self, work: &DetachedContainerWork) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned");
        state.buffered_bytes = state
            .buffered_bytes
            .checked_sub(work.payload_bytes)
            .expect("ASSERT: completed detached bytes must have been admitted");
        let inode_queue = state
            .inodes
            .get_mut(&work.inode)
            .expect("ASSERT: completed publication inode must retain queue state");
        assert_eq!(
            inode_queue.in_flight.remove(&work.publication_ordinal),
            Some(work.first_chunk_sequence),
            "ASSERT: completed publication must match the active inode sequence"
        );
        assert_eq!(
            inode_queue.next_retirement_ordinal, work.publication_ordinal,
            "ASSERT: detached publications retire in per-inode order"
        );
        inode_queue.next_retirement_ordinal = inode_queue
            .next_retirement_ordinal
            .checked_add(1)
            .expect("ASSERT: detached retirement ordinal cannot overflow");
        if schedule_publication_inodes(&mut state) {
            self.work_available.notify_one();
        }
        self.space_available.notify_all();
        self.completed.notify_all();
    }

    fn finish_group(&self, group: &PublicationGroup) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: grouped publication queue lock poisoned");
        state.buffered_bytes = state
            .buffered_bytes
            .checked_sub(group.payload_bytes)
            .expect("ASSERT: completed grouped publication bytes must have been admitted");
        assert!(
            state.active_groups.remove(&group.id).is_some(),
            "ASSERT: grouped publication retirement has an active barrier"
        );
        for member in &group.members {
            let inode_queue = state
                .inodes
                .get_mut(&member.inode)
                .expect("ASSERT: grouped publication inode retains queue state");
            assert_eq!(inode_queue.barrier, Some(group.id));
            inode_queue.barrier = None;
            match inode_queue.pending.pop_front() {
                Some(PublicationPendingItem::GroupMember {
                    group_id,
                    first_chunk_sequence,
                }) => {
                    assert_eq!(group_id, group.id);
                    assert_eq!(first_chunk_sequence, member.first_chunk_sequence);
                }
                other => {
                    panic!("ASSERT: grouped publication retirement must pop its member: {other:?}")
                }
            }
        }
        if schedule_publication_inodes(&mut state) {
            self.work_available.notify_one();
        }
        self.space_available.notify_all();
        self.completed.notify_all();
    }

    fn publication_fence(&self, inode: InodeId, through_sequence: u64) -> (Option<u64>, u64) {
        let state = self
            .state
            .lock()
            .expect("ASSERT: publication queue lock poisoned");
        let inode_queue = state.inodes.get(&inode);
        let barrier = inode_queue
            .and_then(|queue| queue.barrier)
            .filter(|group_id| {
                state
                    .active_groups
                    .get(group_id)
                    .is_some_and(|first| through_sequence >= *first)
            });
        let target = inode_queue
            .and_then(|queue| {
                // A later-ending batch may contain complete pre-cut Chunks.
                // Snapshot the required ordinal once: later arrivals cannot
                // extend a Sync/Release/checkpoint fence indefinitely.
                queue
                    .in_flight
                    .iter()
                    .filter_map(|(&ordinal, &first)| (first <= through_sequence).then_some(ordinal))
                    .chain(queue.pending.iter().filter_map(|item| {
                        item.as_single()
                            .filter(|work| work.first_chunk_sequence <= through_sequence)
                            .map(|work| work.publication_ordinal)
                    }))
                    .max()
            })
            .map_or(0, |ordinal| ordinal + 1);
        (barrier, target)
    }

    fn wait_through(&self, inode: InodeId, through_sequence: u64) {
        let (barrier, target) = self.publication_fence(inode, through_sequence);
        if let Some(group_id) = barrier {
            self.wait_for_group(group_id);
        }
        self.wait_for_retirement(inode, target);
    }

    #[cfg(test)]
    fn retirement_target(&self, inode: InodeId) -> u64 {
        self.state
            .lock()
            .expect("ASSERT: publication queue lock poisoned")
            .inodes
            .get(&inode)
            .map_or(0, |queue| queue.next_publication_ordinal)
    }

    fn wait_for_retirement(&self, inode: InodeId, target: u64) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: publication queue lock poisoned");
        while state
            .inodes
            .get(&inode)
            .is_some_and(|queue| queue.next_retirement_ordinal < target)
        {
            let (waited, timeout) = self
                .completed
                .wait_timeout(state, PUBLICATION_WAIT_DIAGNOSTIC_INTERVAL_V1)
                .expect("ASSERT: publication queue lock poisoned while waiting for retirement");
            state = waited;
            if timeout.timed_out() {
                self.diagnose_publication_wait_locked(&state, "retirement", Some((inode, target)));
            }
        }
    }

    fn wait_for_group(&self, group_id: u64) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: grouped publication queue lock poisoned");
        while state.active_groups.contains_key(&group_id) && !state.shutdown {
            let (waited, timeout) = self
                .completed
                .wait_timeout(state, PUBLICATION_WAIT_DIAGNOSTIC_INTERVAL_V1)
                .expect("ASSERT: publication queue lock poisoned while waiting");
            state = waited;
            if timeout.timed_out() {
                self.diagnose_publication_wait_locked(&state, "group", None);
            }
        }
    }

    fn diagnose_publication_wait_locked(
        &self,
        state: &PublicationQueueState,
        context: &str,
        retirement_wait: Option<(InodeId, u64)>,
    ) {
        let (waiting_inode, retirement_target) =
            retirement_wait.map_or((None, None), |(inode, target)| (Some(inode), Some(target)));
        eprintln!(
            concat!(
                "publication_wait_stall context={} waiting_inode={:?} retirement_target={:?} ",
                "ready_inodes={} ready_groups={} groups={} active_groups={} next_group_id={} ",
                "buffered_bytes={} shared_batch_bytes={} direct_drain_waiters={}"
            ),
            context,
            waiting_inode,
            retirement_target,
            state.ready_inodes.len(),
            state.ready_groups.len(),
            state.groups.len(),
            state.active_groups.len(),
            state.next_group_id,
            state.buffered_bytes,
            self.shared_batch_bytes(),
            state.direct_drain_waiters,
        );
        for (index, (inode, queue)) in state.inodes.iter().enumerate().take(8) {
            eprintln!(
                concat!(
                    "publication_wait_inode context={} index={} inode={:?} pending={} ",
                    "in_flight={} barrier={:?} next_publication_ordinal={} ",
                    "next_retirement_ordinal={} last_sequence={} drain_candidates={} ready={}"
                ),
                context,
                index,
                inode,
                queue.pending.len(),
                queue.in_flight.len(),
                queue.barrier,
                queue.next_publication_ordinal,
                queue.next_retirement_ordinal,
                queue.last_enqueued_sequence,
                queue.drain_candidates,
                queue.ready,
            );
        }
        if state.inodes.len() > 8 {
            eprintln!(
                "publication_wait_inode context={} omitted_inodes={}",
                context,
                state.inodes.len() - 8
            );
        }
    }

    fn buffered_bytes(&self) -> usize {
        self.state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned")
            .buffered_bytes
    }

    #[cfg(test)]
    fn direct_drain_waiters(&self) -> usize {
        self.state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned in test")
            .direct_drain_waiters
    }

    fn shutdown(&self) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: detached publication queue lock poisoned during shutdown");
        state.shutdown = true;
        self.work_available.notify_all();
        self.space_available.notify_all();
        self.completed.notify_all();
        self.drain_available.notify_all();
    }
}

fn publication_window(state: &PublicationQueueState) -> usize {
    let active_inodes = state
        .inodes
        .values()
        .filter(|queue| !queue.pending.is_empty() || !queue.in_flight.is_empty())
        .count();
    if active_inodes <= 1 {
        SINGLE_STREAM_PUBLICATION_WINDOW_V1
    } else {
        1
    }
}

fn schedule_publication_inodes(state: &mut PublicationQueueState) -> bool {
    let per_inode_limit = publication_window(state);
    let mut scheduled = false;
    let inodes = state.inodes.keys().copied().collect::<Vec<_>>();
    for inode in inodes {
        let queue = state
            .inodes
            .get_mut(&inode)
            .expect("ASSERT: enumerated publication inode remains present");
        if !queue.ready
            && queue.barrier.is_none()
            && queue
                .pending
                .front()
                .is_some_and(PublicationPendingItem::is_single)
            && queue.in_flight.len() < per_inode_limit
        {
            queue.ready = true;
            state.ready_inodes.push_back(inode);
            scheduled = true;
        }
    }
    scheduled
}

impl IngestQueue {
    fn opened_write_handle(&self, inode: InodeId) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        let handles = state.writable_handles.entry(inode).or_default();
        *handles = handles
            .checked_add(1)
            .expect("ASSERT: writable handle count cannot overflow");
        if state.writable_handles.len() >= 2 {
            let open_inodes = state
                .inodes
                .iter()
                .filter_map(|(open_inode, queue)| queue.open.is_some().then_some(*open_inode))
                .collect::<Vec<_>>();
            for open_inode in open_inodes {
                seal_open_ingest_batch(&mut state, open_inode);
            }
            self.work_available.notify_all();
        }
    }

    fn released_write_handle(&self, inode: InodeId) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        let handles = state
            .writable_handles
            .get_mut(&inode)
            .expect("ASSERT: released writable handle must be tracked");
        *handles = handles
            .checked_sub(1)
            .expect("ASSERT: writable handle count cannot underflow");
        if *handles == 0 {
            state.writable_handles.remove(&inode);
        }
    }

    fn new() -> Self {
        Self {
            #[cfg(test)]
            before_fragment_wait: Mutex::new(None),
            state: Mutex::new(IngestQueueState::default()),
            work_available: Condvar::new(),
            space_available: Condvar::new(),
            completed: Condvar::new(),
        }
    }

    fn enqueue_write_fragment(&self, inode: InodeId, fragment: IngestWriteFragment) {
        let fragment_bytes = fragment.bytes.len();
        assert!(
            fragment_bytes <= WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1,
            "ASSERT: one queued ingest fragment exceeds its byte bound"
        );
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        state.inodes.entry(inode).or_default();
        if active_ingest_inodes_with_candidate(&state, inode) >= 2 {
            self.enqueue_unbatched_fragment(state, inode, fragment);
            return;
        }
        state = self.wait_for_fragment_admission(state, inode, &fragment);
        assert!(
            !state.shutdown,
            "ASSERT: cannot enqueue after scheduler shutdown"
        );
        state.buffered_bytes = state
            .buffered_bytes
            .checked_add(fragment_bytes)
            .expect("ASSERT: bounded ingest queue bytes cannot overflow");
        let batch_target =
            ingest_batch_target_bytes(active_ingest_inodes_with_candidate(&state, inode));
        let inode_queue = state.inodes.entry(inode).or_default();
        inode_queue.last_enqueued_sequence = fragment.mutation_sequence;
        let open = inode_queue.open.get_or_insert_with(|| OpenIngestBatch {
            opened_at: Instant::now(),
            fragments: Vec::with_capacity(4),
            buffered_bytes: 0,
            last_mutation_sequence: fragment.mutation_sequence,
        });
        open.buffered_bytes = open
            .buffered_bytes
            .checked_add(fragment_bytes)
            .expect("ASSERT: bounded open Ingest Batch bytes cannot overflow");
        open.last_mutation_sequence = fragment.mutation_sequence;
        open.fragments.push(fragment);
        if open.buffered_bytes >= batch_target {
            seal_open_ingest_batch(&mut state, inode);
        }
        self.work_available.notify_one();
    }

    fn enqueue_unbatched_fragment(
        &self,
        mut state: std::sync::MutexGuard<'_, IngestQueueState>,
        inode: InodeId,
        fragment: IngestWriteFragment,
    ) {
        // A mode change may leave a partial single-stream batch for this
        // inode. Queue it before admitting any newer unbatched fragment,
        // including when admission must release the lock for backpressure.
        if seal_open_ingest_batch(&mut state, inode) {
            self.work_available.notify_all();
        }
        let wait_started = Instant::now();
        let mut waited = false;
        while state
            .buffered_bytes
            .checked_add(fragment.bytes.len())
            .is_none_or(|total| total > MULTI_STREAM_QUEUE_BUDGET_BYTES_V1)
        {
            waited = true;
            state = self
                .space_available
                .wait(state)
                .expect("ASSERT: ingest queue lock poisoned while applying backpressure");
        }
        if waited {
            state.ingest_ring_wait_ns = state.ingest_ring_wait_ns.saturating_add(
                u64::try_from(wait_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
        }
        record_ingest_batch_target(&mut state, WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1);
        state.buffered_bytes = state
            .buffered_bytes
            .checked_add(fragment.bytes.len())
            .expect("ASSERT: bounded ingest queue bytes cannot overflow");
        let fragment_bytes = fragment.bytes.len();
        let mutation_sequence = fragment.mutation_sequence;
        let inode_queue = state
            .inodes
            .get_mut(&inode)
            .expect("ASSERT: admitted inode retains queue state");
        assert!(
            mutation_sequence >= inode_queue.last_enqueued_sequence,
            "ASSERT: per-inode ingest admission sequence cannot move backwards"
        );
        inode_queue.last_enqueued_sequence = mutation_sequence;
        let schedule = !inode_queue.in_flight && inode_queue.pending.is_empty();
        inode_queue.pending.push_back(IngestJob {
            inode,
            mutation_sequence,
            kind: IngestJobKind::WriteFragment(fragment),
        });
        let queue_slots = ingest_ring_slots(inode_queue);
        state.ingest_batches = state.ingest_batches.saturating_add(1);
        state.ingest_fragments = state.ingest_fragments.saturating_add(1);
        state.maximum_ingest_batch_bytes = state.maximum_ingest_batch_bytes.max(fragment_bytes);
        state.maximum_ingest_ring_slots = state.maximum_ingest_ring_slots.max(queue_slots);
        if schedule {
            state.ready.push_back(inode);
        }
        self.work_available.notify_one();
    }

    fn wait_for_fragment_admission<'a>(
        &self,
        mut state: std::sync::MutexGuard<'a, IngestQueueState>,
        inode: InodeId,
        fragment: &IngestWriteFragment,
    ) -> std::sync::MutexGuard<'a, IngestQueueState> {
        let wait_started = Instant::now();
        let mut waited = false;
        loop {
            let active_inodes = active_ingest_inodes_with_candidate(&state, inode);
            let batch_target = ingest_batch_target_bytes(active_inodes);
            record_ingest_batch_target(&mut state, batch_target);
            if seal_open_batches_at_or_above(&mut state, batch_target) {
                self.work_available.notify_all();
            }
            let inode_queue = state
                .inodes
                .get(&inode)
                .expect("ASSERT: admitted inode retains queue state");
            assert!(
                fragment.mutation_sequence >= inode_queue.last_enqueued_sequence,
                "ASSERT: per-inode ingest admission sequence cannot move backwards"
            );
            if !ingest_fragment_extends_batch(inode_queue, fragment, batch_target) {
                seal_open_ingest_batch(&mut state, inode);
                self.work_available.notify_one();
                continue;
            }
            let ring_slots = SINGLE_STREAM_INGEST_RING_SLOTS_V1;
            let has_slot =
                inode_queue.open.is_some() || ingest_ring_slots(inode_queue) < ring_slots;
            let has_bytes = state
                .buffered_bytes
                .checked_add(fragment.bytes.len())
                .is_some_and(|total| total <= WRITE_THROUGH_QUEUE_BUDGET_BYTES_V1);
            if has_slot && has_bytes {
                if waited {
                    state.ingest_ring_wait_ns = state.ingest_ring_wait_ns.saturating_add(
                        u64::try_from(wait_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    );
                }
                return state;
            }
            waited = true;
            #[cfg(test)]
            if let Some(sender) = self.before_fragment_wait.lock().unwrap().take() {
                let _ = sender.send(());
            }
            state = self
                .space_available
                .wait(state)
                .expect("ASSERT: ingest queue lock poisoned while applying backpressure");
        }
    }

    fn enqueue(&self, job: IngestJob) {
        let inode = job.inode;
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        state.inodes.entry(inode).or_default();
        seal_open_ingest_batch(&mut state, inode);
        let inode_queue = state
            .inodes
            .get_mut(&inode)
            .expect("ASSERT: admitted inode retains queue state");
        assert!(
            job.mutation_sequence >= inode_queue.last_enqueued_sequence,
            "ASSERT: per-inode ingest admission sequence cannot move backwards"
        );
        inode_queue.last_enqueued_sequence = job.mutation_sequence;
        let schedule = !inode_queue.in_flight && inode_queue.pending.is_empty();
        inode_queue.pending.push_back(job);
        if schedule {
            state.ready.push_back(inode);
        }
        self.work_available.notify_one();
    }

    fn next_job(&self) -> Option<IngestJob> {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        loop {
            seal_expired_ingest_batches(&mut state, Instant::now());
            if let Some(inode) = state.ready.pop_front() {
                let inode_queue = state
                    .inodes
                    .get_mut(&inode)
                    .expect("ASSERT: ready inode must own a queue");
                assert!(
                    !inode_queue.in_flight,
                    "ASSERT: one inode cannot have two active ingest jobs"
                );
                let job = inode_queue
                    .pending
                    .pop_front()
                    .expect("ASSERT: ready inode must own pending work");
                inode_queue.in_flight = true;
                return Some(job);
            }
            if state.shutdown {
                return None;
            }
            if let Some(wait) = next_ingest_batch_expiry(&state, Instant::now()) {
                let (next, _) = self
                    .work_available
                    .wait_timeout(state, wait)
                    .expect("ASSERT: ingest queue lock poisoned while waiting for batch age");
                state = next;
            } else {
                state = self
                    .work_available
                    .wait(state)
                    .expect("ASSERT: ingest queue lock poisoned while waiting for work");
            }
        }
    }

    fn finish(&self, job: &IngestJob) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        state.buffered_bytes = state
            .buffered_bytes
            .checked_sub(job.buffered_bytes())
            .expect("ASSERT: completed ingest bytes must have been admitted");
        let inode_queue = state
            .inodes
            .get_mut(&job.inode)
            .expect("ASSERT: completed inode must retain queue state");
        assert!(
            inode_queue.in_flight,
            "ASSERT: completed ingest job must have been active"
        );
        inode_queue.in_flight = false;
        assert!(
            job.mutation_sequence >= inode_queue.completed_sequence,
            "ASSERT: completed inode sequence cannot move backwards"
        );
        inode_queue.completed_sequence = job.mutation_sequence;
        if !inode_queue.pending.is_empty() {
            state.ready.push_back(job.inode);
            self.work_available.notify_one();
        }
        self.space_available.notify_all();
        self.completed.notify_all();
    }

    fn wait_through(&self, inode: InodeId, mutation_sequence: u64) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        if seal_open_ingest_batch(&mut state, inode) {
            self.work_available.notify_one();
        }
        // Inode versions include Metadata-only mutations that intentionally
        // have no ingest job. Flush waits through the newest DATA job at or
        // before the requested version instead of waiting for every sequence
        // number to appear in this queue.
        let Some(target_sequence) = state
            .inodes
            .get(&inode)
            .map(|queue| queue.last_enqueued_sequence.min(mutation_sequence))
        else {
            return;
        };
        loop {
            let complete = state
                .inodes
                .get(&inode)
                .is_none_or(|queue| queue.completed_sequence >= target_sequence);
            if complete {
                return;
            }
            state = self
                .completed
                .wait(state)
                .expect("ASSERT: ingest queue lock poisoned while waiting for sequence fence");
        }
    }

    fn status(&self) -> IngestQueueStatus {
        let state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned");
        IngestQueueStatus {
            buffered_bytes: state.buffered_bytes,
            ingest_batches: state.ingest_batches,
            ingest_fragments: state.ingest_fragments,
            maximum_ingest_batch_bytes: state.maximum_ingest_batch_bytes,
            minimum_ingest_batch_target_bytes: state.minimum_ingest_batch_target_bytes,
            maximum_ingest_batch_target_bytes: state.maximum_ingest_batch_target_bytes,
            maximum_ingest_ring_slots: state.maximum_ingest_ring_slots,
            ingest_ring_wait_ns: state.ingest_ring_wait_ns,
        }
    }

    fn shutdown(&self) {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: ingest queue lock poisoned during shutdown");
        let open_inodes = state
            .inodes
            .iter()
            .filter_map(|(inode, queue)| queue.open.as_ref().map(|_| *inode))
            .collect::<Vec<_>>();
        for inode in open_inodes {
            seal_open_ingest_batch(&mut state, inode);
        }
        state.shutdown = true;
        self.work_available.notify_all();
        self.space_available.notify_all();
        self.completed.notify_all();
    }
}

fn ingest_ring_slots(queue: &InodeJobQueue) -> usize {
    queue.pending.len() + usize::from(queue.in_flight) + usize::from(queue.open.is_some())
}

fn active_ingest_inodes_with_candidate(state: &IngestQueueState, candidate: InodeId) -> usize {
    if !state.writable_handles.is_empty() {
        return state.writable_handles.len()
            + usize::from(!state.writable_handles.contains_key(&candidate));
    }
    let active = state
        .inodes
        .iter()
        .filter(|(_, queue)| ingest_ring_slots(queue) != 0)
        .count();
    let candidate_active = state
        .inodes
        .get(&candidate)
        .is_some_and(|queue| ingest_ring_slots(queue) != 0);
    active + usize::from(!candidate_active)
}

fn ingest_batch_target_bytes(active_inodes: usize) -> usize {
    match active_inodes {
        0..=1 => SINGLE_STREAM_INGEST_BATCH_BYTES_V1,
        _ => WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1,
    }
}

fn record_ingest_batch_target(state: &mut IngestQueueState, target: usize) {
    if state.minimum_ingest_batch_target_bytes == 0 {
        state.minimum_ingest_batch_target_bytes = target;
    } else {
        state.minimum_ingest_batch_target_bytes =
            state.minimum_ingest_batch_target_bytes.min(target);
    }
    state.maximum_ingest_batch_target_bytes = state.maximum_ingest_batch_target_bytes.max(target);
}

fn ingest_fragment_extends_batch(
    queue: &InodeJobQueue,
    fragment: &IngestWriteFragment,
    target: usize,
) -> bool {
    queue.open.as_ref().is_none_or(|open| {
        let contiguous = open.fragments.last().is_none_or(|last| {
            last.offset
                .checked_add(
                    u64::try_from(last.bytes.len()).expect("ASSERT: fragment length fits u64"),
                )
                .is_some_and(|next| next == fragment.offset)
                && fragment.mutation_sequence >= last.mutation_sequence
                && fragment.placement == last.placement
        });
        let fits = open
            .buffered_bytes
            .checked_add(fragment.bytes.len())
            .is_some_and(|bytes| bytes <= target);
        contiguous && fits
    })
}

fn seal_open_batches_at_or_above(state: &mut IngestQueueState, target: usize) -> bool {
    let ready = state
        .inodes
        .iter()
        .filter_map(|(inode, queue)| {
            queue
                .open
                .as_ref()
                .is_some_and(|open| open.buffered_bytes >= target)
                .then_some(*inode)
        })
        .collect::<Vec<_>>();
    let mut sealed = false;
    for inode in ready {
        sealed |= seal_open_ingest_batch(state, inode);
    }
    sealed
}

fn seal_open_ingest_batch(state: &mut IngestQueueState, inode: InodeId) -> bool {
    let Some(queue) = state.inodes.get_mut(&inode) else {
        return false;
    };
    let Some(open) = queue.open.take() else {
        return false;
    };
    let fragment_count = open.fragments.len();
    let batch_bytes = open.buffered_bytes;
    let schedule = !queue.in_flight && queue.pending.is_empty();
    queue.pending.push_back(open.into_job(inode));
    let ring_slots = ingest_ring_slots(queue);
    state.ingest_batches = state.ingest_batches.saturating_add(1);
    state.ingest_fragments = state
        .ingest_fragments
        .saturating_add(u64::try_from(fragment_count).unwrap_or(u64::MAX));
    state.maximum_ingest_batch_bytes = state.maximum_ingest_batch_bytes.max(batch_bytes);
    state.maximum_ingest_ring_slots = state.maximum_ingest_ring_slots.max(ring_slots);
    if schedule {
        state.ready.push_back(inode);
    }
    true
}

fn seal_expired_ingest_batches(state: &mut IngestQueueState, now: Instant) -> bool {
    let expired = state
        .inodes
        .iter()
        .filter_map(|(inode, queue)| {
            queue.open.as_ref().and_then(|open| {
                (now.saturating_duration_since(open.opened_at) >= INGEST_BATCH_MAXIMUM_AGE_V1)
                    .then_some(*inode)
            })
        })
        .collect::<Vec<_>>();
    let mut sealed = false;
    for inode in expired {
        sealed |= seal_open_ingest_batch(state, inode);
    }
    sealed
}

fn next_ingest_batch_expiry(state: &IngestQueueState, now: Instant) -> Option<Duration> {
    state
        .inodes
        .values()
        .filter_map(|queue| queue.open.as_ref())
        .map(|open| {
            INGEST_BATCH_MAXIMUM_AGE_V1
                .saturating_sub(now.saturating_duration_since(open.opened_at))
        })
        .min()
}

#[derive(Debug, Default)]
#[repr(align(64))]
struct CpuPhaseTelemetry {
    phases: AtomicU64,
    active: AtomicU64,
    maximum_active: AtomicU64,
    runnable_wall_ns: AtomicU64,
    permit_blocked_phases: AtomicU64,
    permit_wait_ns: AtomicU64,
    maximum_permit_wait_ns: AtomicU64,
    requested_workers: AtomicU64,
    granted_workers: AtomicU64,
    partial_grants: AtomicU64,
}

impl CpuPhaseTelemetry {
    fn begin(&self) -> CpuPhaseGuard<'_> {
        atomic_saturating_add(&self.phases, 1);
        let active = self
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
                active.checked_add(1)
            })
            .expect("ASSERT: active CPU-phase telemetry cannot overflow")
            .checked_add(1)
            .expect("ASSERT: active CPU-phase telemetry cannot overflow");
        self.maximum_active.fetch_max(active, Ordering::Relaxed);
        CpuPhaseGuard {
            telemetry: self,
            started: Instant::now(),
        }
    }

    fn record_permit(&self, lease: &WorkerPermitLease<'_>) {
        let requested = u64::try_from(lease.requested_workers().get())
            .expect("ASSERT: worker count fits telemetry");
        let granted =
            u64::try_from(lease.workers().get()).expect("ASSERT: worker count fits telemetry");
        atomic_saturating_add(&self.requested_workers, requested);
        atomic_saturating_add(&self.granted_workers, granted);
        atomic_saturating_add(&self.permit_wait_ns, lease.wait_ns());
        self.maximum_permit_wait_ns
            .fetch_max(lease.wait_ns(), Ordering::Relaxed);
        if lease.blocked() {
            atomic_saturating_add(&self.permit_blocked_phases, 1);
        }
        if granted < requested {
            atomic_saturating_add(&self.partial_grants, 1);
        }
    }

    fn status(&self) -> CpuPhaseStatus {
        CpuPhaseStatus {
            phases: self.phases.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            maximum_active: self.maximum_active.load(Ordering::Relaxed),
            runnable_wall_ns: self.runnable_wall_ns.load(Ordering::Relaxed),
            permit_blocked_phases: self.permit_blocked_phases.load(Ordering::Relaxed),
            permit_wait_ns: self.permit_wait_ns.load(Ordering::Relaxed),
            maximum_permit_wait_ns: self.maximum_permit_wait_ns.load(Ordering::Relaxed),
            requested_workers: self.requested_workers.load(Ordering::Relaxed),
            granted_workers: self.granted_workers.load(Ordering::Relaxed),
            partial_grants: self.partial_grants.load(Ordering::Relaxed),
        }
    }
}

struct CpuPhaseGuard<'a> {
    telemetry: &'a CpuPhaseTelemetry,
    started: Instant,
}

impl Drop for CpuPhaseGuard<'_> {
    fn drop(&mut self) {
        atomic_saturating_add(
            &self.telemetry.runnable_wall_ns,
            duration_ns_saturating(self.started.elapsed()),
        );
        let previous = self.telemetry.active.fetch_sub(1, Ordering::Relaxed);
        assert!(
            previous != 0,
            "ASSERT: active CPU-phase telemetry underflow"
        );
    }
}

fn atomic_saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn duration_ns_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

struct ActiveWriteThrough<'a> {
    active_writers: &'a AtomicUsize,
}

impl<'a> ActiveWriteThrough<'a> {
    fn enter(active_writers: &'a AtomicUsize) -> Self {
        active_writers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .expect("ASSERT: active write-through job count cannot overflow");
        Self { active_writers }
    }
}

impl Drop for ActiveWriteThrough<'_> {
    fn drop(&mut self) {
        let previous = self.active_writers.fetch_sub(1, Ordering::AcqRel);
        assert!(
            previous != 0,
            "ASSERT: write-through job retirement requires one active job"
        );
    }
}

fn validate_pending_chunks(chunks: &[PendingWriteThroughChunk]) -> (usize, Option<u64>) {
    let mut summed_bytes = 0_usize;
    let mut previous_end = None;
    let mut first_sequence: Option<u64> = None;
    for pending in chunks {
        previous_end = Some(PendingWriteThrough::validate_append(previous_end, pending));
        summed_bytes = summed_bytes
            .checked_add(pending.bytes.len())
            .expect("ASSERT: bounded pending write-through bytes cannot overflow");
        let sequence = pending.bytes.through_sequence();
        first_sequence = Some(first_sequence.map_or(sequence, |previous| previous.min(sequence)));
    }
    (summed_bytes, first_sequence)
}

fn assert_pending_write_through_state(state: &WriteThroughStream) {
    state.pending.assert_bounded();
}

fn assert_bounded_write_through_lane(state: &WriteThroughStream) {
    assert_pending_write_through_state(state);
    let buffered = state
        .tail
        .len()
        .checked_add(state.pending.bytes)
        .expect("ASSERT: bounded Ingest Lane bytes cannot overflow");
    assert!(
        buffered <= CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES,
        "ASSERT: one Ingest Lane exceeded one Container plus CDC suffix: tail={} pending={} buffered={} bound={}",
        state.tail.len(),
        state.pending.bytes,
        buffered,
        CONTAINER_PAYLOAD_TARGET_BYTES + CDC_MAXIMUM_BYTES,
    );
}

impl<C> fmt::Debug for WriteThroughIngest<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let snapshot = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .status_snapshot();
        let buffered_bytes = snapshot.lanes.iter().fold(0_usize, |total, lane| {
            let lane = lane
                .lock()
                .expect("ASSERT: write-through lane lock poisoned");
            total
                .checked_add(lane.tail.len())
                .and_then(|sum| sum.checked_add(lane.pending.bytes))
                .expect("ASSERT: bounded write-through lane bytes cannot overflow")
        });
        let overflow = snapshot
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned");
        let ingest = self.queue.status();
        let buffered_bytes = buffered_bytes
            .checked_add(self.shared_batch_bytes.load(Ordering::Relaxed))
            .and_then(|sum| sum.checked_add(overflow.tail.len()))
            .and_then(|sum| sum.checked_add(overflow.pending.bytes))
            .and_then(|sum| sum.checked_add(ingest.buffered_bytes))
            .and_then(|sum| sum.checked_add(self.publication_queue.buffered_bytes()))
            .expect("ASSERT: bounded write-through bytes cannot overflow");
        assert!(
            buffered_bytes <= WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1,
            "ASSERT: write-through registry exceeded its process memory budget"
        );
        formatter
            .debug_struct("WriteThroughIngest")
            .field("active_lanes", &snapshot.lanes.len())
            .field("buffered_bytes", &buffered_bytes)
            .field(
                "sealed_uncommitted",
                &snapshot.sealed_uncommitted_containers,
            )
            .field(
                "active_writers",
                &self.active_writers.load(Ordering::Relaxed),
            )
            .field("degraded", &snapshot.degraded)
            .finish_non_exhaustive()
    }
}

impl<C> WriteThroughIngest<C>
where
    C: Clone + Send + Sync + StorageIo + 'static,
{
    fn start_workers(self: &Arc<Self>, namespace: &Arc<Namespace>) {
        self.namespace
            .set(Arc::downgrade(namespace))
            .expect("ASSERT: write-through Namespace is attached exactly once");
        let worker_count = self
            .worker_budget
            .get()
            .min(MAX_ACTIVE_INGEST_LANES_V1.saturating_add(1));
        let mut workers = self
            .workers
            .lock()
            .expect("ASSERT: ingest worker handle lock poisoned");
        assert!(
            workers.is_empty(),
            "ASSERT: ingest workers start exactly once"
        );
        let publication_worker_count = worker_count.min(2);
        workers
            .try_reserve_exact(worker_count.saturating_add(publication_worker_count))
            .expect("ASSERT: bounded ingest worker handle allocation succeeds");
        for ordinal in 0..worker_count {
            let owner = Arc::downgrade(self);
            let queue = Arc::clone(&self.queue);
            let worker = std::thread::Builder::new()
                .name(format!("fastdup-ingest-{ordinal}"))
                .spawn(move || {
                    while let Some(job) = queue.next_job() {
                        if let Some(owner) = owner.upgrade() {
                            owner.process_job(&job);
                        }
                        queue.finish(&job);
                    }
                })
                .expect("ASSERT: bounded permanent ingest worker creation succeeds");
            workers.push(worker);
        }
        for ordinal in 0..publication_worker_count {
            let owner = Arc::downgrade(self);
            let queue = Arc::clone(&self.publication_queue);
            let worker = std::thread::Builder::new()
                .name(format!("fastdup-publish-{ordinal}"))
                .spawn(move || {
                    while let Some(unit) = queue.next_unit() {
                        match unit {
                            PublicationUnit::Single(work) => {
                                let result = if let Some(owner) = owner.upgrade() {
                                    let result = owner.publish_detached_container(&work);
                                    queue.wait_for_retirement_turn(&work);
                                    owner.retire_detached_container(&work, result)
                                } else {
                                    queue.wait_for_retirement_turn(&work);
                                    Err(DurableNamespaceError::FrozenViewMismatch)
                                };
                                queue.finish(&work);
                                if let Some(completion) = &work.completion {
                                    // One bounded reply; a canceled checkpoint may have
                                    // dropped its receiver, but retirement still finishes.
                                    let _ = completion.send(result.map_err(Arc::new));
                                }
                            }
                            PublicationUnit::Group(group) => {
                                let result = if let Some(owner) = owner.upgrade() {
                                    let result = owner.publish_shared_group(&group);
                                    owner.retire_shared_group(&group, result)
                                } else {
                                    Err(DurableNamespaceError::FrozenViewMismatch)
                                };
                                queue.finish_group(&group);
                                let completion = result.map_err(SharedPublicationError::from);
                                for member in &group.members {
                                    if let Some(sender) = &member.completion {
                                        let _ = sender.send(completion.clone());
                                    }
                                }
                            }
                        }
                    }
                })
                .expect("ASSERT: bounded permanent publication worker creation succeeds");
            workers.push(worker);
        }
    }

    fn enqueue_write(
        &self,
        inode: InodeId,
        offset: u64,
        mutation_sequence: u64,
        placement: ContainerPlacement,
        bytes: &MutationPayload,
    ) {
        let mut consumed = 0_usize;
        while consumed < bytes.len() {
            let chunk_end = consumed
                .saturating_add(WRITE_THROUGH_FRAGMENT_MAX_BYTES_V1)
                .min(bytes.len());
            let job_offset = offset
                .checked_add(u64::try_from(consumed).expect("ASSERT: queued offset fits u64"))
                .expect("ASSERT: accepted write range was already checked");
            let chunk = bytes
                .checked_slice(consumed, chunk_end)
                .expect("ASSERT: queued write slice lies inside accepted payload");
            consumed = chunk_end;
            self.queue.enqueue_write_fragment(
                inode,
                IngestWriteFragment {
                    offset: job_offset,
                    bytes: chunk,
                    mutation_sequence,
                    placement,
                },
            );
        }
    }

    fn process_job(&self, job: &IngestJob) {
        let result = match &job.kind {
            IngestJobKind::WriteFragment(fragment) => {
                self.stage_write_batch(job.inode, std::slice::from_ref(fragment))
            }
            IngestJobKind::WriteBatch { fragments } => self.stage_write_batch(job.inode, fragments),
            IngestJobKind::Truncate => {
                self.reset_lane(job.inode);
                Ok(())
            }
        };
        if let Err(error) = result {
            self.degrade_job(job, &error);
        }
    }

    fn publish_detached_container(
        &self,
        work: &DetachedContainerWork,
    ) -> Result<(Vec<ExternalizedExtent>, bool), DurableNamespaceError> {
        let _active_writer = ActiveWriteThrough::enter(&self.active_writers);
        self.publish_chunks(&work.chunks, work.inode, work.through_sequence)
    }

    fn retire_detached_container(
        &self,
        work: &DetachedContainerWork,
        result: Result<(Vec<ExternalizedExtent>, bool), DurableNamespaceError>,
    ) -> Result<(), DurableNamespaceError> {
        match result {
            Ok((externalized, sealed)) => {
                if let Some(namespace) = self.namespace.get().and_then(Weak::upgrade) {
                    namespace.externalize_verified_extents(externalized);
                }
                if sealed {
                    let mut registry = self
                        .registry
                        .lock()
                        .expect("ASSERT: write-through registry lock poisoned");
                    registry.sealed.push_back(Instant::now());
                }
                Ok(())
            }
            Err(error) => {
                // Partial drains return the error to their checkpoint, which
                // retains the Frozen view for retry. Only unobserved background
                // failures use ordinary lane degradation.
                if work.completion.is_none() {
                    self.degrade_inode(work.inode, work.through_sequence, &error);
                }
                Err(error)
            }
        }
    }

    fn retire_shared_group(
        &self,
        group: &PublicationGroup,
        result: Result<(Vec<Vec<ExternalizedExtent>>, bool), DurableNamespaceError>,
    ) -> Result<(), DurableNamespaceError> {
        match result {
            Ok((externalized, sealed)) => {
                assert_eq!(
                    externalized.len(),
                    group.members.len(),
                    "ASSERT: shared publication has one extent batch per member"
                );
                if let Some(namespace) = self.namespace.get().and_then(Weak::upgrade) {
                    for extents in externalized {
                        namespace.externalize_verified_extents(extents);
                    }
                }
                if sealed {
                    let mut registry = self
                        .registry
                        .lock()
                        .expect("ASSERT: write-through registry lock poisoned");
                    registry.sealed.push_back(Instant::now());
                }
                Ok(())
            }
            Err(error) => {
                for member in &group.members {
                    if member.completion.is_none() {
                        self.degrade_inode(member.inode, member.through_sequence, &error);
                    }
                }
                Err(error)
            }
        }
    }

    fn degrade_job(&self, job: &IngestJob, error: &DurableNamespaceError) {
        let (offset, length) = match &job.kind {
            IngestJobKind::WriteFragment(fragment) => (fragment.offset, fragment.bytes.len()),
            IngestJobKind::WriteBatch { fragments } => (
                fragments.first().map_or(0, |fragment| fragment.offset),
                job.buffered_bytes(),
            ),
            IngestJobKind::Truncate => (0, 0),
        };
        eprintln!(
            "write-through staging degraded; resident fallback retained: inode={} offset={offset} length={length} sequence={} error={error:?}",
            job.inode.get(),
            job.mutation_sequence,
        );
        self.mark_degraded();
        self.reset_lane(job.inode);
    }

    fn degrade_inode(&self, inode: InodeId, mutation_sequence: u64, error: &DurableNamespaceError) {
        eprintln!(
            "detached Container publication degraded; resident fallback retained: inode={} sequence={mutation_sequence} error={error:?}",
            inode.get(),
        );
        // The failed work was already detached. Later lane contents remain
        // valid and resident; the publisher must not invalidate them or wait
        // on a lane whose producer may need this publication to retire.
        self.mark_degraded();
    }

    fn mark_degraded(&self) {
        self.registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .degraded = true;
    }

    fn reset_lane(&self, inode: InodeId) {
        let lane = {
            let registry = self
                .registry
                .lock()
                .expect("ASSERT: write-through registry lock poisoned");
            registry.lanes.get(&inode).map_or_else(
                || Arc::clone(&registry.overflow),
                |lane| Arc::clone(&lane.stream),
            )
        };
        // A checkpoint may have captured this Arc before the truncate barrier.
        // Reset in place: replacing it would let that checkpoint drain stale
        // work after a new lane has published later mutation sequences.
        // Drop the Registry before waiting for a lane/publication backpressure.
        let mut lane = lane
            .lock()
            .expect("ASSERT: write-through lane lock poisoned");
        if lane.inode == Some(inode) {
            *lane = WriteThroughStream::default();
        }
    }

    pub(super) fn status(&self) -> WriteThroughStatus {
        // Never wait for a Lane while holding the Registry. A Lane may apply
        // publication-queue backpressure while the completing publisher needs
        // the Registry to retire its work and release that queue space.
        let snapshot = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .status_snapshot();
        let buffered = snapshot.lanes.iter().fold(0_usize, |total, lane| {
            let lane = lane
                .lock()
                .expect("ASSERT: write-through lane lock poisoned");
            total
                .checked_add(lane.tail.len())
                .and_then(|sum| sum.checked_add(lane.pending.bytes))
                .expect("ASSERT: bounded write-through lane bytes cannot overflow")
        });
        let overflow = snapshot
            .overflow
            .lock()
            .expect("ASSERT: write-through overflow lane lock poisoned");
        let buffered = buffered
            .checked_add(overflow.tail.len())
            .and_then(|sum| sum.checked_add(overflow.pending.bytes))
            .expect("ASSERT: bounded write-through bytes cannot overflow");
        let ingest = self.queue.status();
        let queued_bytes = ingest
            .buffered_bytes
            .checked_add(self.publication_queue.buffered_bytes())
            .expect("ASSERT: bounded scheduler queue bytes cannot overflow");
        let buffered = buffered
            .checked_add(queued_bytes)
            .expect("ASSERT: bounded write-through plus queue bytes cannot overflow");
        assert!(
            buffered <= WRITE_THROUGH_BUFFER_BUDGET_BYTES_V1,
            "ASSERT: write-through registry exceeded its process memory budget"
        );
        WriteThroughStatus {
            buffered_bytes: u64::try_from(buffered).expect("ASSERT: process buffers fit in u64"),
            queued_bytes: u64::try_from(queued_bytes)
                .expect("ASSERT: bounded queued bytes fit u64"),
            active_lanes: u64::try_from(snapshot.lanes.len())
                .expect("ASSERT: bounded Ingest Lane count fits u64"),
            sealed_uncommitted_containers: u64::try_from(snapshot.sealed_uncommitted_containers)
                .expect("ASSERT: process Container count fits in u64"),
            oldest_sealed_age: snapshot.oldest_sealed_age,
            hash_batches: u64::try_from(self.hash_batches.load(Ordering::Relaxed))
                .expect("ASSERT: process hash-batch count fits u64"),
            maximum_hash_workers: u64::try_from(self.maximum_hash_workers.load(Ordering::Relaxed))
                .expect("ASSERT: process hash-worker count fits u64"),
            ingest_batches: ingest.ingest_batches,
            ingest_fragments: ingest.ingest_fragments,
            maximum_ingest_batch_bytes: u64::try_from(ingest.maximum_ingest_batch_bytes)
                .expect("ASSERT: bounded Ingest Batch bytes fit u64"),
            minimum_ingest_batch_target_bytes: u64::try_from(
                ingest.minimum_ingest_batch_target_bytes,
            )
            .expect("ASSERT: bounded Ingest Batch target fits u64"),
            maximum_ingest_batch_target_bytes: u64::try_from(
                ingest.maximum_ingest_batch_target_bytes,
            )
            .expect("ASSERT: bounded Ingest Batch target fits u64"),
            maximum_ingest_ring_slots: u64::try_from(ingest.maximum_ingest_ring_slots)
                .expect("ASSERT: bounded Ingest Ring slots fit u64"),
            ingest_ring_wait_ns: ingest.ingest_ring_wait_ns,
            hash_cpu: self.hash_cpu.status(),
            encode_cpu: self.encode_cpu.status(),
            planning_cpu: self.planning_cpu.status(),
            materialization_wall_ns: self.materialization_wall_ns.load(Ordering::Relaxed),
            advanced_reduction: self.index.advanced_reduction_status(),
            degraded: snapshot.degraded,
        }
    }

    pub(super) fn capture_cut(&self) -> usize {
        self.registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned")
            .sealed
            .len()
    }

    pub(super) fn wait_for_commit_cut(
        &self,
        commit: &NamespaceCommit,
        timings: &CheckpointTimings,
        metrics: &mut CheckpointMetrics,
    ) {
        for inode in commit.inodes() {
            let ingest = timings.begin(CheckpointStage::IngestWait);
            self.queue
                .wait_through(inode.inode(), inode.mutation_sequence());
            ingest.finish_into(&mut metrics.ingest_wait);
            let publication = timings.begin(CheckpointStage::PublicationWait);
            self.publication_queue
                .wait_through(inode.inode(), inode.mutation_sequence());
            publication.finish_into(&mut metrics.publication_wait);
        }
    }

    pub(super) fn complete_cut(&self, sealed_at_cut: usize) {
        let mut registry = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned");
        assert!(
            sealed_at_cut <= registry.sealed.len(),
            "ASSERT: completed cut cannot retire future Containers"
        );
        registry.sealed.drain(..sealed_at_cut);
        // Preserve the incomplete SeqCDC suffix across generations. It is the
        // exact content anchor required for a later append to make the same
        // cuts as the checkpoint planner. Chunks the checkpoint published from
        // this suffix are filtered through the newly active Exact Index before
        // the write-through path publishes its next Container.
    }

    fn stage_write_batch(
        &self,
        inode: InodeId,
        fragments: &[IngestWriteFragment],
    ) -> Result<(), DurableNamespaceError> {
        assert!(
            !fragments.is_empty(),
            "ASSERT: an Ingest Batch contains at least one fragment"
        );
        let _active_writer = ActiveWriteThrough::enter(&self.active_writers);
        let lane = self.lane_for(inode);
        let mut lane = lane
            .lock()
            .expect("ASSERT: write-through lane lock poisoned");
        let mut externalized = Vec::new();
        for fragment in fragments {
            let discontinuous = lane.inode != Some(inode)
                || lane.placement != Some(fragment.placement)
                || lane.next_offset != fragment.offset
                || lane
                    .last_mutation_sequence
                    .is_some_and(|previous| fragment.mutation_sequence <= previous);
            if discontinuous {
                externalized.extend(self.drain_before_lane_reset(&mut lane)?);
                lane.inode = Some(inode);
                lane.placement = Some(fragment.placement);
                lane.tail_offset = fragment.offset;
                lane.tail.clear();
                lane.pending.clear();
            }
            assert_bounded_write_through_lane(&lane);
            lane.last_mutation_sequence = Some(fragment.mutation_sequence);
            lane.next_offset = fragment
                .offset
                .checked_add(u64::try_from(fragment.bytes.len()).expect("ASSERT: usize fits u64"))
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            lane.tail
                .push(fragment.bytes.clone(), fragment.mutation_sequence);
            loop {
                externalized.extend(self.extract_stable_chunks(
                    &mut lane,
                    inode,
                    fragment.mutation_sequence,
                    StableExtraction::FillContainer,
                )?);
                if lane.pending.bytes < CONTAINER_PAYLOAD_FLUSH_BYTES {
                    break;
                }
                assert!(
                    lane.pending.bytes <= CONTAINER_PAYLOAD_TARGET_BYTES,
                    "ASSERT: write-through payload exceeded its pre-format Container bound"
                );
                let pending = std::mem::take(&mut lane.pending);
                let work = DetachedContainerWork::new(
                    inode,
                    fragment.mutation_sequence,
                    pending.chunks,
                    pending.bytes,
                );
                assert_pending_write_through_state(&lane);
                self.publication_queue.enqueue(work);
            }
            assert_bounded_write_through_lane(&lane);
        }
        lane.tail.assert_valid();
        #[cfg(test)]
        let had_inline = !externalized.is_empty();
        if !externalized.is_empty()
            && let Some(namespace) = self.namespace.get().and_then(Weak::upgrade)
        {
            // A post-cut job can consume pre-cut Tail bytes. Publish its inline
            // recipes before the Lane becomes available to the commit drain:
            // the cut's Ingest fence need not wait for this later job, and the
            // detached-publication fence cannot see inline Exact/FILL reuse.
            // This only updates Namespace memory. Mutation observers enqueue
            // after releasing inode state, so no inode holder waits on this Lane.
            namespace.externalize_verified_extents(externalized);
        }
        drop(lane);
        #[cfg(test)]
        if had_inline {
            let hook = self.after_inline_stage.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        Ok(())
    }

    fn drain_before_lane_reset(
        &self,
        lane: &mut WriteThroughStream,
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        let (Some(inode), Some(through_sequence)) = (lane.inode, lane.last_mutation_sequence)
        else {
            return Ok(Vec::new());
        };
        // A discontinuity ends CDC continuity, not the validity of the entire
        // buffered prefix. Preserve complete Chunks under their original
        // sequences; Namespace still rejects any subsequently overwritten range.
        // The ordinary bounded publication queue owns pending payload before
        // the lane starts the next range. No storage wait is added to admission.
        let mut externalized = Vec::new();
        loop {
            let previous_tail = lane.tail.len();
            externalized.extend(self.extract_stable_chunks(
                lane,
                inode,
                through_sequence,
                StableExtraction::DrainStable,
            )?);
            let had_pending = !lane.pending.chunks.is_empty();
            if had_pending {
                let pending = std::mem::take(&mut lane.pending);
                self.publication_queue.enqueue(DetachedContainerWork::new(
                    inode,
                    through_sequence,
                    pending.chunks,
                    pending.bytes,
                ));
            }
            if lane.tail.len() == previous_tail || !had_pending {
                break;
            }
        }
        assert!(
            lane.pending.chunks.is_empty() && lane.pending.bytes == 0,
            "ASSERT: a lane reset preserves every complete staged Chunk"
        );
        assert!(
            lane.tail.len() <= CDC_MAXIMUM_BYTES * 2,
            "ASSERT: a lane reset discards only the bounded incomplete CDC suffix"
        );
        Ok(externalized)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "partial drain owns batch formation, ordered enqueue, and shared reservations"
    )]
    fn enqueue_shared_partial_batch(
        &self,
        batch: &mut Vec<Option<PartialCandidate>>,
    ) -> Result<PublicationFences, DurableNamespaceError> {
        let mut receivers = Vec::new();
        receivers
            .try_reserve_exact(batch.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for candidate in batch.iter_mut().flatten() {
            if let Some(receiver) = candidate.receiver.take() {
                receivers.push(receiver);
            }
        }
        let mut placements = BTreeMap::<(u8, bool), Vec<usize>>::new();
        for (index, candidate) in batch.iter().enumerate() {
            let Some(candidate) = candidate else {
                continue;
            };
            placements
                .entry((
                    partial_placement_bucket(candidate.placement),
                    candidate.advanced,
                ))
                .or_default()
                .push(index);
        }
        let mut retirement_targets: BTreeMap<InodeId, u64> = BTreeMap::new();
        let mut barriers: BTreeMap<InodeId, u64> = BTreeMap::new();
        for group_candidates in placements.into_values() {
            let mut start = 0;
            while start < group_candidates.len() {
                let first = group_candidates[start];
                start += 1;
                if batch[first].is_none() {
                    continue;
                }
                let mut selected = vec![first];
                let mut payload = batch[first]
                    .as_ref()
                    .expect("ASSERT: selected candidate exists")
                    .payload_bytes;
                let placement = batch[first]
                    .as_ref()
                    .expect("ASSERT: selected candidate exists")
                    .placement;
                for &other in &group_candidates[start..] {
                    let Some(candidate) = batch[other].as_ref() else {
                        continue;
                    };
                    let Some(total) = payload.checked_add(candidate.payload_bytes) else {
                        continue;
                    };
                    if total > PARTIAL_BATCH_BUDGET_BYTES_V1 || candidate.placement != placement {
                        continue;
                    }
                    if selected.iter().any(|&selected| {
                        batch[selected]
                            .as_ref()
                            .is_some_and(|selected| selected.inode == candidate.inode)
                    }) {
                        continue;
                    }
                    selected.push(other);
                    payload = total;
                }
                if selected.len() == 1 {
                    let Some(candidate) = batch[first].take() else {
                        continue;
                    };
                    let inode = candidate.inode;
                    let target = self
                        .publication_queue
                        .enqueue_drain_candidate(candidate.into_single(), payload);
                    retirement_targets
                        .entry(inode)
                        .and_modify(|existing| *existing = (*existing).max(target))
                        .or_insert(target);
                    continue;
                }
                let members = selected
                    .iter()
                    .map(|&index| {
                        batch[index]
                            .take()
                            .expect("ASSERT: grouped candidate exists")
                            .into_member()
                    })
                    .collect::<Vec<_>>();
                let member_inodes = members
                    .iter()
                    .map(|member| member.inode)
                    .collect::<Vec<_>>();
                let group = PublicationGroup {
                    id: 0,
                    members,
                    payload_bytes: payload,
                };
                match self.publication_queue.try_enqueue_group(group, payload) {
                    Ok((group_id, member_targets)) => {
                        for inode in member_inodes {
                            barriers.insert(inode, group_id);
                        }
                        Self::merge_publication_targets(&mut retirement_targets, member_targets);
                    }
                    Err(group) => {
                        for member in group.members {
                            let inode = member.inode;
                            let reserved = member.payload_bytes;
                            let mut work = DetachedContainerWork::new(
                                member.inode,
                                member.through_sequence,
                                member.chunks,
                                member.payload_bytes,
                            );
                            work.completion = member.completion;
                            let target = self
                                .publication_queue
                                .enqueue_drain_candidate(work, reserved);
                            retirement_targets
                                .entry(inode)
                                .and_modify(|existing| *existing = (*existing).max(target))
                                .or_insert(target);
                        }
                    }
                }
            }
        }
        batch.clear();
        Ok(PublicationFences {
            retirement_targets,
            barriers,
            receivers,
        })
    }

    fn release_shared_drain_batch(&self, batch: &mut Vec<Option<PartialCandidate>>) {
        let mut drain_candidates = BTreeMap::new();
        for candidate in batch.drain(..).flatten() {
            self.publication_queue
                .release_local_bytes(candidate.payload_bytes);
            *drain_candidates
                .entry(candidate.inode)
                .or_insert_with(|| 0_usize) += 1;
        }
        for (inode, count) in drain_candidates {
            for _ in 0..count {
                self.publication_queue.clear_drain_candidate(inode);
            }
        }
    }

    fn flush_shared_partial_batch(
        &self,
        batch: &mut Vec<Option<PartialCandidate>>,
    ) -> Result<PublicationFences, DurableNamespaceError> {
        let result = self.enqueue_shared_partial_batch(batch);
        if result.is_err() {
            self.release_shared_drain_batch(batch);
        }
        result
    }

    fn merge_publication_targets(
        targets: &mut BTreeMap<InodeId, u64>,
        additions: BTreeMap<InodeId, u64>,
    ) {
        for (inode, target) in additions {
            targets
                .entry(inode)
                .and_modify(|existing| *existing = (*existing).max(target))
                .or_insert(target);
        }
    }

    fn wait_for_shared_publication_fences(
        &self,
        retirement_targets: BTreeMap<InodeId, u64>,
        barriers: &BTreeMap<InodeId, u64>,
        receivers: &[std::sync::mpsc::Receiver<PublicationResult>],
    ) -> Result<(), DurableNamespaceError> {
        for (inode, target) in retirement_targets {
            if let Some(group_id) = barriers.get(&inode) {
                self.publication_queue.wait_for_group(*group_id);
            }
            self.publication_queue.wait_for_retirement(inode, target);
        }
        for receiver in receivers {
            recv_publication_completion(receiver)?;
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "commit-cut drain owns lane extraction, bounded grouping, and cut fences"
    )]
    pub(super) fn flush_stable_for_commit_cut(
        &self,
        timings: &CheckpointTimings,
        metrics: &mut CheckpointMetrics,
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        let _active_writer = ActiveWriteThrough::enter(&self.active_writers);
        let registry_started = timings.begin(CheckpointStage::LaneLock);
        let lanes = {
            let registry = self
                .registry
                .lock()
                .expect("ASSERT: write-through registry lock poisoned");
            let mut lanes = Vec::new();
            lanes
                .try_reserve_exact(registry.lanes.len().saturating_add(1))
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            lanes.extend(registry.lanes.values().map(|lane| Arc::clone(&lane.stream)));
            lanes.push(Arc::clone(&registry.overflow));
            lanes
        };
        registry_started.finish_into(&mut metrics.lane_lock);
        let mut externalized = Vec::new();
        let mut batch = Vec::new();
        let mut batch_bytes = 0_usize;
        let mut retirement_targets = BTreeMap::new();
        let mut barriers = BTreeMap::new();
        let mut receivers = Vec::new();
        for lane in lanes {
            let lane_started = timings.begin(CheckpointStage::LaneLock);
            let mut lane = lane
                .lock()
                .expect("ASSERT: write-through lane lock poisoned");
            lane_started.finish_into(&mut metrics.lane_lock);
            let (Some(inode), Some(through_sequence)) = (lane.inode, lane.last_mutation_sequence)
            else {
                assert_pending_write_through_state(&lane);
                continue;
            };
            let advanced = self.advanced_reduction_enabled_for(inode);
            loop {
                let extract_started = timings.begin(CheckpointStage::StableExtract);
                let previous_tail = lane.tail.len();
                let extracted = match self.extract_stable_chunks(
                    &mut lane,
                    inode,
                    through_sequence,
                    StableExtraction::DrainStable,
                ) {
                    Ok(extracted) => extracted,
                    Err(error) => {
                        self.release_shared_drain_batch(&mut batch);
                        return Err(error);
                    }
                };
                if let Err(error) = externalized
                    .try_reserve(extracted.len())
                    .map_err(|_| DurableNamespaceError::OutOfMemory)
                {
                    self.release_shared_drain_batch(&mut batch);
                    return Err(error);
                }
                externalized.extend(extracted);
                extract_started.finish_into(&mut metrics.stable_extract);
                let had_pending = !lane.pending.chunks.is_empty();
                if had_pending {
                    let pending = std::mem::take(&mut lane.pending);
                    let placement = pending
                        .chunks
                        .first()
                        .expect("ASSERT: pending work has one placement")
                        .placement;
                    let candidate = PartialCandidate::new(
                        inode,
                        through_sequence,
                        pending.chunks,
                        pending.bytes,
                        placement,
                        advanced,
                    );
                    self.publication_queue
                        .reserve_local_bytes(candidate.payload_bytes);
                    self.publication_queue.begin_drain_candidate(inode);
                    batch_bytes = batch_bytes
                        .checked_add(candidate.payload_bytes)
                        .expect("ASSERT: bounded partial batch bytes cannot overflow");
                    batch.push(Some(candidate));
                    if batch_bytes >= PARTIAL_BATCH_BUDGET_BYTES_V1 {
                        let enqueue_started = timings.begin(CheckpointStage::PublicationEnqueue);
                        let fences = self.flush_shared_partial_batch(&mut batch)?;
                        enqueue_started.finish_into(&mut metrics.publication_enqueue);
                        Self::merge_publication_targets(
                            &mut retirement_targets,
                            fences.retirement_targets,
                        );
                        barriers.extend(fences.barriers);
                        receivers.extend(fences.receivers);
                        batch_bytes = 0;
                    }
                }
                if lane.tail.len() == previous_tail || !had_pending {
                    break;
                }
            }
            assert!(
                lane.pending.chunks.is_empty() && lane.pending.bytes == 0,
                "ASSERT: commit-cut drain detaches every complete staged Chunk"
            );
            assert!(
                lane.tail.len() <= CDC_MAXIMUM_BYTES * 2,
                "ASSERT: commit-cut drain retains only a boundary Chunk and CDC suffix"
            );
            assert_bounded_write_through_lane(&lane);
            let (base_barrier, base_target) = self
                .publication_queue
                .publication_fence(inode, through_sequence);
            retirement_targets
                .entry(inode)
                .and_modify(|existing| *existing = (*existing).max(base_target))
                .or_insert(base_target);
            if let Some(group_id) = base_barrier {
                barriers.insert(inode, group_id);
            }
        }
        let enqueue_started = timings.begin(CheckpointStage::PublicationEnqueue);
        let fences = self.flush_shared_partial_batch(&mut batch)?;
        enqueue_started.finish_into(&mut metrics.publication_enqueue);
        Self::merge_publication_targets(&mut retirement_targets, fences.retirement_targets);
        barriers.extend(fences.barriers);
        receivers.extend(fences.receivers);
        let retire_started = timings.begin(CheckpointStage::PublicationRetire);
        self.wait_for_shared_publication_fences(retirement_targets, &barriers, &receivers)?;
        retire_started.finish_into(&mut metrics.publication_retire);
        Ok(externalized)
    }

    pub(super) fn lane_for(&self, inode: InodeId) -> Arc<Mutex<WriteThroughStream>> {
        let mut registry = self
            .registry
            .lock()
            .expect("ASSERT: write-through registry lock poisoned");
        registry.acquire_lane(inode)
    }

    #[allow(clippy::too_many_lines)]
    fn extract_stable_chunks(
        &self,
        state: &mut WriteThroughStream,
        inode: InodeId,
        through_sequence: u64,
        extraction: StableExtraction,
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        assert_pending_write_through_state(state);
        let stable_before = state.tail.len().saturating_sub(CDC_MAXIMUM_BYTES);
        let stable_required = CONTAINER_PAYLOAD_FLUSH_BYTES
            .checked_sub(state.pending.bytes)
            .expect("ASSERT: pending bytes remain below the flush threshold");
        if extraction == StableExtraction::FillContainer && stable_before < stable_required {
            return Ok(Vec::new());
        }
        let mut externalized = Vec::new();
        while state.pending.bytes < CONTAINER_PAYLOAD_FLUSH_BYTES {
            let maximum_batch_bytes = CONTAINER_PAYLOAD_FLUSH_BYTES
                .checked_sub(state.pending.bytes)
                .expect("ASSERT: pending bytes remain below the flush threshold");
            let batch = take_stable_chunk_batch(state, maximum_batch_bytes)?;
            if batch.is_empty() {
                break;
            }
            let (chunk_ids, workers) = classify_stable_chunk_batch(
                &batch,
                self.worker_budget,
                &self.worker_permits,
                &self.hash_cpu,
            )?;
            self.hash_batches
                .fetch_add(1, Ordering::Relaxed)
                .checked_add(1)
                .expect("ASSERT: hash-batch telemetry cannot overflow");
            self.maximum_hash_workers
                .fetch_max(workers, Ordering::Relaxed);
            assert_eq!(
                batch.len(),
                chunk_ids.len(),
                "ASSERT: every stable Chunk has one classification"
            );
            for (chunk, chunk_id) in batch.into_iter().zip(chunk_ids) {
                let chunk_through_sequence = chunk.bytes.through_sequence();
                assert!(
                    chunk_through_sequence <= through_sequence,
                    "ASSERT: one stable Chunk cannot depend on a future mutation"
                );
                let logical_length = u64::try_from(chunk.bytes.len())
                    .expect("ASSERT: bounded SeqCDC Chunk length fits u64");
                let Some(chunk_id) = chunk_id else {
                    externalized.push(ExternalizedExtent::new(
                        inode,
                        chunk.offset,
                        chunk_through_sequence,
                        Arc::new(FillCommittedFile {
                            value: chunk.bytes.first_byte(),
                            length: logical_length,
                        }),
                    )?);
                    continue;
                };
                if let Some(entry) = self.online_dependency_proofs.reuse_location(
                    self.index.as_ref(),
                    &self.containers,
                    chunk_id,
                    logical_length,
                    false,
                ) {
                    let segments: Vec<&[u8]> = chunk
                        .bytes
                        .parts
                        .as_slice()
                        .iter()
                        .map(MutationPayload::as_bytes)
                        .collect();
                    self.index
                        .read_cache()
                        .admit_writer_chunk(chunk_id, &segments);
                    externalized.push(self.externalized_proven_location(
                        inode,
                        chunk.offset,
                        chunk_through_sequence,
                        entry,
                    )?);
                    continue;
                }
                state.pending.push(PendingWriteThroughChunk {
                    offset: chunk.offset,
                    chunk_id,
                    bytes: chunk.bytes,
                    placement: state
                        .placement
                        .expect("ASSERT: active Write-Through stream has a placement"),
                })?;
            }
        }
        assert_pending_write_through_state(state);
        Ok(externalized)
    }

    fn advanced_reduction_enabled_for(&self, inode: InodeId) -> bool {
        self.index.advanced_reduction_available()
            && self
                .namespace
                .get()
                .and_then(Weak::upgrade)
                .is_some_and(|namespace| namespace.advanced_reduction_enabled(inode))
    }

    fn publish_chunks(
        &self,
        chunks: &[PendingWriteThroughChunk],
        inode: InodeId,
        through_sequence: u64,
    ) -> Result<(Vec<ExternalizedExtent>, bool), DurableNamespaceError> {
        assert!(
            !chunks.is_empty(),
            "ASSERT: Container publication requires at least one pending Chunk"
        );
        let mut locations = Vec::<ExactIndexEntry>::new();
        let mut candidates = Vec::<(ChunkId, u32, &PendingWriteThroughChunk)>::new();
        candidates
            .try_reserve_exact(chunks.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let mut new_chunks = Vec::new();
        new_chunks
            .try_reserve(chunks.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for chunk in chunks {
            let chunk_id = chunk.chunk_id;
            let logical_length = u32::try_from(chunk.bytes.len())
                .map_err(|_| DurableNamespaceError::FrozenViewMismatch)?;
            candidates.push((chunk_id, logical_length, chunk));
        }
        candidates.sort_unstable_by_key(|(chunk_id, _, _)| *chunk_id);
        let mut unique_candidates = Vec::new();
        unique_candidates
            .try_reserve_exact(candidates.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (chunk_id, logical_length, chunk) in candidates {
            if let Some((previous_id, previous_length, _)) = unique_candidates.last().copied()
                && previous_id == chunk_id
            {
                if previous_length != logical_length {
                    return Err(DurableNamespaceError::ChunkLengthConflict {
                        chunk_id,
                        first_length: u64::from(previous_length),
                        second_length: u64::from(logical_length),
                    });
                }
                continue;
            }
            unique_candidates.push((chunk_id, logical_length, chunk));
        }
        let mut claims =
            PublicationClaims::new(&self.online_dependency_proofs, unique_candidates.len())?;
        for (chunk_id, logical_length, chunk) in unique_candidates {
            match claims.claim(chunk_id, logical_length) {
                PublicationClaim::Existing(entry) => {
                    locations.push(entry);
                }
                PublicationClaim::Acquired => new_chunks.push(chunk),
            }
        }
        // Claims are acquired in key order; physical output follows file order.
        new_chunks.sort_unstable_by_key(|chunk| chunk.offset);
        let advanced = self.advanced_reduction_enabled_for(inode);
        let publication_guard = advanced
            .then(|| self.containers.try_pin_data_reference())
            .flatten();
        let advanced = publication_guard.is_some();
        let (mut entries, similarity_entries) = if new_chunks.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            self.publish_new_chunks(&new_chunks, advanced)?
        };
        locations.extend(entries.iter().copied());
        locations.sort_unstable_by_key(ExactIndexEntry::chunk_id);
        assert!(
            locations
                .windows(2)
                .all(|pair| pair[0].chunk_id() < pair[1].chunk_id()),
            "ASSERT: one unique candidate Chunk has exactly one publication result"
        );
        claims.finish(&mut entries);
        let sealed = !entries.is_empty();
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.bytes.through_sequence() <= through_sequence),
            "ASSERT: detached work sequence covers every published Chunk"
        );
        let externalized = self.externalize_chunks(chunks, inode, &locations)?;
        self.index
            .publish_reduction_batch(entries, similarity_entries, publication_guard);
        Ok((externalized, sealed))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one shared Container publication must keep per-member ordering and externalization"
    )]
    fn publish_shared_group(
        &self,
        group: &PublicationGroup,
    ) -> Result<(Vec<Vec<ExternalizedExtent>>, bool), DurableNamespaceError> {
        assert!(
            group.members.len() > 1,
            "ASSERT: shared Container publication requires multiple members"
        );
        let total_chunks = group.members.iter().fold(0_usize, |total, member| {
            total
                .checked_add(member.chunks.len())
                .expect("ASSERT: shared Container chunk count cannot overflow")
        });
        let mut candidates = Vec::with_capacity(total_chunks);
        candidates
            .try_reserve_exact(total_chunks)
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for member in &group.members {
            for chunk in &member.chunks {
                let logical_length = u32::try_from(chunk.bytes.len())
                    .map_err(|_| DurableNamespaceError::FrozenViewMismatch)?;
                candidates.push((chunk.chunk_id, logical_length, chunk, member.inode));
            }
        }
        candidates
            .sort_unstable_by_key(|(chunk_id, _, chunk, owner)| (*chunk_id, *owner, chunk.offset));
        let mut unique_candidates = Vec::with_capacity(candidates.len());
        unique_candidates
            .try_reserve_exact(candidates.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        for (chunk_id, logical_length, chunk, owner) in candidates {
            if let Some((previous_id, previous_length, _, _)) = unique_candidates.last().copied()
                && previous_id == chunk_id
            {
                if previous_length != logical_length {
                    return Err(DurableNamespaceError::ChunkLengthConflict {
                        chunk_id,
                        first_length: u64::from(previous_length),
                        second_length: u64::from(logical_length),
                    });
                }
                continue;
            }
            unique_candidates.push((chunk_id, logical_length, chunk, owner));
        }
        let mut locations = Vec::new();
        locations
            .try_reserve(unique_candidates.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let mut new_chunk_items = Vec::new();
        new_chunk_items
            .try_reserve(unique_candidates.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let mut claims =
            PublicationClaims::new(&self.online_dependency_proofs, unique_candidates.len())?;
        for (chunk_id, logical_length, chunk, owner) in unique_candidates {
            match claims.claim(chunk_id, logical_length) {
                PublicationClaim::Existing(entry) => locations.push(entry),
                PublicationClaim::Acquired => new_chunk_items.push((owner, chunk)),
            }
        }
        new_chunk_items.sort_unstable_by_key(|(owner, chunk)| (*owner, chunk.offset));
        let new_chunks = new_chunk_items
            .iter()
            .map(|(_, chunk)| *chunk)
            .collect::<Vec<_>>();
        let advanced = group
            .members
            .iter()
            .all(|member| member.advanced && self.advanced_reduction_enabled_for(member.inode));
        let publication_guard = advanced
            .then(|| self.containers.try_pin_data_reference())
            .flatten();
        let advanced = publication_guard.is_some();
        let (mut entries, similarity_entries) = if new_chunks.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            self.publish_new_chunks(&new_chunks, advanced)?
        };
        locations.extend(entries.iter().copied());
        locations.sort_unstable_by_key(ExactIndexEntry::chunk_id);
        assert!(
            locations
                .windows(2)
                .all(|pair| pair[0].chunk_id() < pair[1].chunk_id()),
            "ASSERT: one unique candidate Chunk has exactly one publication result"
        );
        claims.finish(&mut entries);
        let sealed = !entries.is_empty();
        let mut externalized = Vec::with_capacity(group.members.len());
        for member in &group.members {
            assert!(
                member
                    .chunks
                    .iter()
                    .all(|chunk| chunk.bytes.through_sequence() <= member.through_sequence),
                "ASSERT: shared publication member sequence covers every published Chunk"
            );
            externalized
                .try_reserve(1)
                .map_err(|_| DurableNamespaceError::OutOfMemory)?;
            externalized.push(self.externalize_chunks(&member.chunks, member.inode, &locations)?);
        }
        self.index
            .publish_reduction_batch(entries, similarity_entries, publication_guard);
        Ok((externalized, sealed))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "single owner for materialized input, admitted plans and ordered publication"
    )]
    fn publish_new_chunks(
        &self,
        new_chunks: &[&PendingWriteThroughChunk],
        advanced: bool,
    ) -> Result<
        (
            Vec<ExactIndexEntry>,
            Vec<fastdup_format::SimilarityIndexEntry>,
        ),
        DurableNamespaceError,
    > {
        assert!(
            !new_chunks.is_empty(),
            "ASSERT: a new-Chunk Container must contain at least one Chunk"
        );
        assert!(
            new_chunks
                .iter()
                .all(|chunk| chunk.placement == new_chunks[0].placement),
            "ASSERT: one Container cannot cross physical placement tiers"
        );
        let desired_workers = self.worker_budget;
        let materialization_started = Instant::now();
        let prepared_regions =
            prepare_compression_regions(new_chunks, desired_workers, &self.worker_permits);
        self.materialization_wall_ns.fetch_add(
            u64::try_from(materialization_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let prepared_regions = prepared_regions?;
        let materialized_chunks = prepared_regions
            .materialized
            .iter()
            .map(|region| {
                region
                    .chunks
                    .iter()
                    .map(|(chunk_id, range)| {
                        PrehashedChunk::new(*chunk_id, &region.decoded[range.clone()])
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let contiguous_regions = materialized_chunks
            .iter()
            .zip(&prepared_regions.materialized)
            .map(|(chunks, region)| {
                PrehashedContiguousRegion::new(chunks, &region.decoded)
                    .expect("ASSERT: constructed Chunk views exactly partition their region")
            })
            .collect::<Vec<_>>();
        let regions = prepared_regions
            .order
            .iter()
            .map(|region| match *region {
                CompressionRegionOrder::Borrowed(ordinal) => {
                    PrehashedAdaptiveRegion::Borrowed(&prepared_regions.borrowed[ordinal])
                }
                CompressionRegionOrder::Materialized(ordinal) => {
                    PrehashedAdaptiveRegion::Contiguous(contiguous_regions[ordinal])
                }
            })
            .collect::<Vec<_>>();
        let generation = self.container_generations.reserve_generation()?;
        let mut independent = Vec::new();
        let mut dependents = Vec::new();
        let mut similarity_entries = Vec::new();
        let ordinary_regions = if advanced {
            let targets = regions
                .iter()
                .flat_map(|region| match region {
                    PrehashedAdaptiveRegion::Borrowed(chunks) => chunks.iter().copied(),
                    PrehashedAdaptiveRegion::Contiguous(region) => region.chunks().iter().copied(),
                })
                .collect::<Vec<_>>();
            let mut ordinary = Vec::with_capacity(targets.len());
            let phase = self.planning_cpu.begin();
            let plans = self.index.plan_similarity_batch(
                &self.containers,
                &targets,
                desired_workers,
                &self.worker_permits,
            );
            drop(phase);
            for (plan, hint) in plans {
                if let Some(hint) = hint {
                    similarity_entries.push(hint);
                }
                ordinary.push(matches!(plan, PersistentChunkPlan::NoCandidates));
                match plan {
                    PersistentChunkPlan::NoCandidates => {}
                    PersistentChunkPlan::Independent(record) => independent.push(record),
                    PersistentChunkPlan::Dependent(record) => dependents.push(record),
                }
            }
            Cow::Owned(ordinary_region_slices(&regions, &ordinary)?)
        } else {
            Cow::Borrowed(regions.as_slice())
        };
        let chunk_order = regions
            .iter()
            .flat_map(|region| match region {
                PrehashedAdaptiveRegion::Borrowed(chunks) => chunks.iter(),
                PrehashedAdaptiveRegion::Contiguous(region) => region.chunks().iter(),
            })
            .map(|target| target.chunk_id())
            .collect::<Vec<_>>();
        let desired_workers =
            NonZeroUsize::new(desired_workers.get().min(ordinary_regions.len().max(1)))
                .expect("ASSERT: serial assembly needs one worker");
        let worker_lease = self.worker_permits.acquire(desired_workers);
        let workers = worker_lease.workers();
        self.encode_cpu.record_permit(&worker_lease);
        let cpu_phase = self.encode_cpu.begin();
        assert!(
            workers.get() <= self.worker_budget.get(),
            "ASSERT: one encode job cannot exceed the write-through worker budget"
        );
        let prepared =
            ContainerRepository::<C>::prepare_mixed_prehashed_reduction_with_worker_retirement(
                random_container_id()?,
                generation,
                &ordinary_regions,
                independent,
                dependents,
                workers,
                Some(&chunk_order),
                &|worker| {
                    if worker != 0 {
                        worker_lease.retire_worker();
                    }
                },
            )?;
        drop(cpu_phase);
        drop(worker_lease);
        let (verified, _) = self
            .containers
            .publish_prepared_adaptive_profiled_with_placement(prepared, new_chunks[0].placement)?;
        let entries = verified
            .locations()
            .iter()
            .copied()
            .map(|location| {
                ExactIndexEntry::from_verified(location)
                    .expect("ASSERT: verified write-through Location forms an Exact Index entry")
            })
            .collect();
        Ok((entries, similarity_entries))
    }

    fn externalize_chunks(
        &self,
        chunks: &[PendingWriteThroughChunk],
        inode: InodeId,
        locations: &[ExactIndexEntry],
    ) -> Result<Vec<ExternalizedExtent>, DurableNamespaceError> {
        let mut externalized = Vec::new();
        externalized
            .try_reserve(chunks.len())
            .map_err(|_| DurableNamespaceError::OutOfMemory)?;
        let mut entries = Vec::with_capacity(chunks.len());
        for pending in chunks {
            let chunk_id = pending.chunk_id;
            let entry = locations
                .binary_search_by_key(&chunk_id, ExactIndexEntry::chunk_id)
                .ok()
                .and_then(|ordinal| locations.get(ordinal))
                .copied()
                .ok_or(DurableNamespaceError::FrozenViewMismatch)?;
            let expected_length = u32::try_from(pending.bytes.len())
                .map_err(|_| DurableNamespaceError::FrozenViewMismatch)?;
            if entry.logical_length() != expected_length {
                return Err(DurableNamespaceError::ChunkLengthConflict {
                    chunk_id,
                    first_length: u64::from(entry.logical_length()),
                    second_length: u64::from(expected_length),
                });
            }
            // Publication/Exact claims have completed. Before resident dirty
            // backing is retired, offer logical bytes to the same reader cache.
            // This is content reuse only, never a physical verification proof.
            let segments: Vec<&[u8]> = pending
                .bytes
                .parts
                .as_slice()
                .iter()
                .map(MutationPayload::as_bytes)
                .collect();
            self.index
                .read_cache()
                .admit_writer_chunk(chunk_id, &segments);
            entries.push(entry);
        }
        // One shared virtual file per bounded publication lets live reads
        // batch adjacent chunk views, including before Exact activation.
        let source: Arc<dyn CommittedFile> = Arc::new(crate::ManifestCommittedFile::from_verified(
            self.index
                .prepare(VerifiedManifestFile::from_published_locations(
                    &entries,
                    self.containers.clone(),
                )?),
        ));
        let mut source_offset = 0;
        for (pending, entry) in chunks.iter().zip(entries) {
            externalized.push(ExternalizedExtent::new(
                inode,
                pending.offset,
                pending.bytes.through_sequence(),
                Arc::new(VerifiedLocationFile {
                    source: Arc::clone(&source),
                    source_offset,
                    entry,
                }),
            )?);
            source_offset += u64::from(entry.logical_length());
        }
        Ok(externalized)
    }

    fn externalized_proven_location(
        &self,
        inode: InodeId,
        offset: u64,
        through_sequence: u64,
        entry: ExactIndexEntry,
    ) -> Result<ExternalizedExtent, DurableNamespaceError> {
        ExternalizedExtent::new(
            inode,
            offset,
            through_sequence,
            Arc::new(VerifiedLocationFile {
                source: Arc::new(crate::ManifestCommittedFile::from_verified(
                    self.index
                        .prepare(VerifiedManifestFile::from_published_locations(
                            &[entry],
                            self.containers.clone(),
                        )?),
                )),
                source_offset: 0,
                entry,
            }),
        )
        .map_err(Into::into)
    }
}

impl<C> MutationObserver for WriteThroughIngest<C>
where
    C: Clone + Send + Sync + StorageIo + 'static,
{
    fn opened_write_handle(&self, inode: InodeId) {
        self.queue.opened_write_handle(inode);
    }

    fn released_write_handle(&self, inode: InodeId) {
        self.queue.released_write_handle(inode);
    }

    fn accepted_write(
        &self,
        inode: InodeId,
        offset: u64,
        mutation_sequence: u64,
        small_file: bool,
        bytes: MutationPayload,
    ) -> Vec<ExternalizedExtent> {
        self.enqueue_write(
            inode,
            offset,
            mutation_sequence,
            if small_file {
                ContainerPlacement::SmallFile
            } else {
                ContainerPlacement::Data
            },
            &bytes,
        );
        Vec::new()
    }

    fn accepted_truncate(&self, inode: InodeId, mutation_sequence: u64, _length: u64) {
        self.queue.enqueue(IngestJob {
            inode,
            mutation_sequence,
            kind: IngestJobKind::Truncate,
        });
    }

    fn wait_through(&self, inode: InodeId, mutation_sequence: u64) {
        self.queue.wait_through(inode, mutation_sequence);
        self.publication_queue
            .wait_through(inode, mutation_sequence);
        self.index.flush_level_zero();
    }
}

impl<C> Drop for WriteThroughIngest<C> {
    fn drop(&mut self) {
        self.queue.shutdown();
        self.publication_queue.shutdown();
        let current = std::thread::current().id();
        let workers = self
            .workers
            .get_mut()
            .expect("ASSERT: ingest worker handle lock poisoned during shutdown");
        for worker in workers.drain(..) {
            if worker.thread().id() != current {
                worker
                    .join()
                    .expect("ASSERT: permanent ingest worker must not panic");
            }
        }
    }
}

pub(super) fn install_write_through<C>(
    namespace: &Arc<Namespace>,
    containers: ContainerRepository<C>,
    container_generations: ContainerGenerationAllocator<C>,
    index: Arc<dyn ManifestReaderPolicy<C>>,
    worker_budget: NonZeroUsize,
    online_dependency_proofs: Arc<OnlineDependencyProofs>,
) -> Arc<WriteThroughIngest<C>>
where
    C: Clone + Send + Sync + StorageIo + 'static,
{
    let worker_permits = Arc::new(WorkerPermits::new(worker_budget));
    containers.install_cpu_admission(Arc::clone(&worker_permits));
    let shared_batch_bytes = Arc::new(AtomicUsize::new(0));
    let write_through = Arc::new(WriteThroughIngest {
        containers,
        container_generations,
        index,
        worker_budget,
        worker_permits,
        active_writers: AtomicUsize::new(0),
        shared_batch_bytes: Arc::clone(&shared_batch_bytes),
        hash_batches: AtomicUsize::new(0),
        maximum_hash_workers: AtomicUsize::new(0),
        hash_cpu: CpuPhaseTelemetry::default(),
        encode_cpu: CpuPhaseTelemetry::default(),
        planning_cpu: CpuPhaseTelemetry::default(),
        materialization_wall_ns: AtomicU64::new(0),
        registry: Mutex::new(WriteThroughRegistry::default()),
        queue: Arc::new(IngestQueue::new()),
        publication_queue: Arc::new(PublicationQueue::with_shared_batch_bytes(Arc::clone(
            &shared_batch_bytes,
        ))),
        namespace: OnceLock::new(),
        #[cfg(test)]
        after_inline_stage: Mutex::new(None),
        workers: Mutex::new(Vec::new()),
        online_dependency_proofs,
    });
    write_through.start_workers(namespace);
    namespace.install_mutation_observer(write_through.clone());
    write_through
}

#[cfg(test)]
#[path = "write_through_tests.rs"]
mod tests;
