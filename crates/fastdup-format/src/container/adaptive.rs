//! Parallel adaptive encoding, prehashed inputs and bounded incompressibility trials.
use super::aligned::{AlignedContainerBuilder, AlignedContainerBytes};
use super::compression::{
    IncompressibilityGateMetrics, IncompressibilityGatePolicy, with_adaptive_encoder_v1,
};
use super::dependent::{PreparedDependentRecord, SparseXorRecord, ZstdPrefixRecord};
use super::image::{SealedContainer, VerifiedContainerPublication};
use super::records::{
    RawRecord, collect_prehashed_decoded, encode_prehashed_raw_record_into,
    encode_prehashed_zstd_record_into, encode_zstd_record, prehashed_decoded_length,
    raw_record_length, validate_logical_chunk_length, write_prehashed_raw_metadata,
    write_prehashed_zstd_metadata, zstd_record_length,
};
use super::summary::IntrinsicSummaryAccumulator;
use super::writer::{encode_container_from_adaptive_plans, encode_container_from_records};
use super::{
    CHUNK_TABLE_ENTRY_BYTES, ChunkId, ContainerId, FormatError,
    INCOMPRESSIBILITY_GATE_MIN_BYTES_V1, MAX_DECODED_RECORD_BYTES, RAW_CODEC, RAW_PAYLOAD_OFFSET,
    RECORD_ALIGNMENT, RECORD_HEADER_BYTES, ZSTD_CODEC, ZSTD_LEVEL_V1,
    ZSTD_MINIMUM_SAVINGS_BYTES_V1, ZSTD_MINIMUM_SAVINGS_PERCENT_V1, ZSTD_RESCUE_LEVEL_V1, get_u32,
};
use rayon::prelude::*;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// One encoded Container image paired with writer-produced publication
/// evidence and non-authoritative gate metrics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdaptiveContainerEncoding {
    pub(super) bytes: AlignedContainerBytes,
    pub(super) publication: VerifiedContainerPublication,
    pub(super) metrics: IncompressibilityGateMetrics,
}

impl AdaptiveContainerEncoding {
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub const fn metrics(&self) -> IncompressibilityGateMetrics {
        self.metrics
    }

    /// Consumes the writer result into the immutable image and the Location
    /// evidence derived while that image was encoded.
    #[must_use]
    pub fn into_publication_parts(self) -> (Vec<u8>, VerifiedContainerPublication) {
        (self.bytes.into_vec(), self.publication)
    }

    /// Consumes the writer result without discarding the image's page
    /// alignment required by a Direct-I/O publication adapter.
    #[must_use]
    pub fn into_aligned_publication_parts(
        self,
    ) -> (AlignedContainerBytes, VerifiedContainerPublication) {
        (self.bytes, self.publication)
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes.into_vec()
    }
}

impl SealedContainer {
    /// Encodes nonempty RAW chunks into one fully sealed container image.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid chunk sizes, layout overflow, or a container
    /// exceeding the v1 bounds.
    ///
    /// # Panics
    ///
    /// Panics if the preflight layout and the independently encoded record or
    /// index lengths disagree, which is an impossible internal writer state.
    pub fn encode(
        container_id: ContainerId,
        container_generation: u64,
        chunks: &[&[u8]],
    ) -> Result<Vec<u8>, FormatError> {
        Self::encode_with_writer_evidence(container_id, container_generation, chunks)
            .map(AdaptiveContainerEncoding::into_bytes)
    }

    /// Encodes RAW Chunks and retains the Location evidence established by
    /// the writer's existing Chunk hashes and serialized layout.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::encode`].
    pub fn encode_with_writer_evidence(
        container_id: ContainerId,
        container_generation: u64,
        chunks: &[&[u8]],
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let mut encoded_records = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            encoded_records.push(RawRecord::encode(chunk)?);
        }
        encode_container_from_records(
            container_id,
            container_generation,
            encoded_records,
            NonZeroUsize::MIN,
        )
    }

    /// Encodes bounded multi-Chunk Compression Regions as independent Zstd
    /// records inside one fully sealed Container.
    ///
    /// The caller chooses complete region boundaries. This writer fixes the
    /// durable codec to Zstd level 3, verifies every chunk partition and
    /// identity, and emits a complete Recovery Index entry per logical Chunk.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty Container or region, an invalid Chunk,
    /// decoded regions above 512 KiB, Zstd failure, layout overflow, or a
    /// Container above the v1 bound.
    pub fn encode_zstd_regions(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
    ) -> Result<Vec<u8>, FormatError> {
        if regions.is_empty() {
            return Err(FormatError::InvalidContainerLayout);
        }
        let mut encoded_records = Vec::new();
        encoded_records
            .try_reserve_exact(regions.len())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        for region in regions {
            encoded_records.push(encode_zstd_record(region, ZSTD_LEVEL_V1)?);
        }
        encode_container_from_records(
            container_id,
            container_generation,
            encoded_records,
            NonZeroUsize::MIN,
        )
        .map(AdaptiveContainerEncoding::into_bytes)
    }

    /// Encodes one codec-3 record per `(Base, target)` pair.
    ///
    /// Every pair must have equal nonzero logical lengths. Bases are named by
    /// BLAKE3 identity but are not copied into this Container. The caller must
    /// ensure each Base has an independently decodable durable Location before
    /// publishing the returned image.
    ///
    /// # Errors
    ///
    /// Returns a Prefix codec, length, allocation, layout, or Container error.
    pub fn encode_zstd_prefix_pairs(
        container_id: ContainerId,
        container_generation: u64,
        pairs: &[(&[u8], &[u8])],
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        if pairs.is_empty() {
            return Err(FormatError::InvalidContainerLayout);
        }
        let mut encoded_records = Vec::new();
        encoded_records
            .try_reserve_exact(pairs.len())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        for &(base, target) in pairs {
            encoded_records.push(ZstdPrefixRecord::encode(base, target)?);
        }
        encode_container_from_records(
            container_id,
            container_generation,
            encoded_records,
            NonZeroUsize::MIN,
        )
    }

    /// Encodes one codec-4 record per same-length `(Base, target)` pair.
    ///
    /// # Errors
    ///
    /// Returns a Sparse-XOR codec, length, allocation, layout, or Container error.
    pub fn encode_sparse_xor_pairs(
        container_id: ContainerId,
        container_generation: u64,
        pairs: &[(&[u8], &[u8])],
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        if pairs.is_empty() {
            return Err(FormatError::InvalidContainerLayout);
        }
        let mut encoded_records = Vec::new();
        encoded_records
            .try_reserve_exact(pairs.len())
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        for &(base, target) in pairs {
            encoded_records.push(SparseXorRecord::encode(base, target)?);
        }
        encode_container_from_records(
            container_id,
            container_generation,
            encoded_records,
            NonZeroUsize::MIN,
        )
    }

    /// Encodes bounded regions using Zstd only when the complete encoded
    /// record saves at least 4 KiB and 3% versus independent RAW records.
    ///
    /// The comparison includes record headers, Chunk Tables, and record
    /// alignment. Recovery Index cost is identical per logical Chunk in both
    /// alternatives. Incompressible regions remain independently decodable
    /// RAW records.
    ///
    /// # Errors
    ///
    /// Returns the same region, codec, arithmetic, and Container layout errors
    /// as [`Self::encode_zstd_regions`].
    pub fn encode_adaptive_regions(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
    ) -> Result<Vec<u8>, FormatError> {
        Self::encode_adaptive_regions_parallel(
            container_id,
            container_generation,
            regions,
            NonZeroUsize::MIN,
        )
    }

    /// Encodes independent Compression Regions on a bounded number of scoped
    /// workers, then merges their results in original region order.
    ///
    /// Workers own disjoint input ordinals and private output vectors, avoiding
    /// shared hot counters and cache-line contention. Runtime scheduling never
    /// changes logical or physical ordering, so one and many workers emit
    /// byte-identical Container images.
    ///
    /// # Errors
    ///
    /// Returns the same region, codec, arithmetic, allocation, and Container
    /// layout errors as [`Self::encode_adaptive_regions`].
    ///
    /// # Panics
    ///
    /// Panics if an encoding worker panics or returns a duplicate/missing
    /// ordinal. Both are impossible internal writer failures after preflight.
    pub fn encode_adaptive_regions_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
    ) -> Result<Vec<u8>, FormatError> {
        Self::encode_adaptive_regions_parallel_profiled(
            container_id,
            container_generation,
            regions,
            workers,
        )
        .map(AdaptiveContainerEncoding::into_bytes)
    }

    /// Encodes adaptive regions and returns runtime-only gate evidence.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::encode_adaptive_regions_parallel`].
    pub fn encode_adaptive_regions_parallel_profiled(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        Self::encode_adaptive_regions_parallel_profiled_with_gate(
            container_id,
            container_generation,
            regions,
            workers,
            IncompressibilityGatePolicy::V1,
        )
    }

    /// Encodes adaptive regions with an explicit benchmarkable gate policy.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::encode_adaptive_regions_parallel`].
    pub fn encode_adaptive_regions_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[&[u8]]],
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let prehashed = regions
            .iter()
            .map(|region| {
                region
                    .iter()
                    .map(|bytes| PrehashedChunk::new(ChunkId::of(bytes), bytes))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let prehashed_regions = prehashed.iter().map(Vec::as_slice).collect::<Vec<_>>();
        Self::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &prehashed_regions,
            workers,
            gate,
        )
    }

    /// Encodes adaptive Compression Regions from Chunk identities already
    /// computed by the ingest writer.
    ///
    /// The supplied identities are trusted writer evidence. Publication
    /// carries them forward without hashing the same resident bytes again.
    /// Every ordinary reader, recovery pass, and scrub recomputes the identity
    /// from independently read decoded bytes.
    ///
    /// # Errors
    ///
    /// Returns the same bounded region, codec, allocation, and layout errors as
    /// [`Self::encode_adaptive_regions_parallel`].
    ///
    /// # Panics
    ///
    /// Panics if worker result ownership or ordering violates an internal
    /// writer invariant.
    pub fn encode_prehashed_adaptive_regions_parallel(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        workers: NonZeroUsize,
    ) -> Result<Vec<u8>, FormatError> {
        Self::encode_prehashed_adaptive_regions_parallel_profiled(
            container_id,
            container_generation,
            regions,
            workers,
        )
        .map(AdaptiveContainerEncoding::into_bytes)
    }

    /// Encodes prehashed adaptive regions and returns writer publication plus
    /// runtime-only gate evidence.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`Self::encode_prehashed_adaptive_regions_parallel`].
    ///
    /// # Panics
    ///
    /// Panics if worker ownership, deterministic ordering, or final gate
    /// accounting violates an internal writer invariant.
    pub fn encode_prehashed_adaptive_regions_parallel_profiled(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        workers: NonZeroUsize,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        Self::encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
            container_id,
            container_generation,
            regions,
            workers,
            IncompressibilityGatePolicy::V1,
        )
    }

    /// Encodes prehashed regions with an explicit benchmarkable gate policy.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`Self::encode_prehashed_adaptive_regions_parallel`].
    ///
    /// # Panics
    ///
    /// Panics if worker ownership, deterministic ordering, or final gate
    /// accounting violates an internal writer invariant.
    pub fn encode_prehashed_adaptive_regions_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|chunks| AdaptiveRegionInput {
                chunks,
                decoded: None,
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            workers,
            gate,
            None,
            &|_| {},
        )
    }

    /// Encodes already contiguous prehashed regions without joining their
    /// decoded bytes into another temporary allocation.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`Self::encode_prehashed_adaptive_regions_parallel`].
    pub fn encode_prehashed_contiguous_regions_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedContiguousRegion<'_>],
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|region| AdaptiveRegionInput {
                chunks: region.chunks,
                decoded: Some(region.decoded),
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            workers,
            gate,
            None,
            &|_| {},
        )
    }

    /// Encodes an ordered mixture of borrowed and already-materialized
    /// prehashed regions. Fragmented Chunks can avoid a second join while
    /// ordinary contiguous Chunks retain their low-memory borrowed path.
    ///
    /// # Errors
    ///
    /// Returns the same errors as
    /// [`Self::encode_prehashed_adaptive_regions_parallel`].
    pub fn encode_mixed_prehashed_adaptive_regions_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedAdaptiveRegion<'_>],
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|region| match *region {
                PrehashedAdaptiveRegion::Borrowed(chunks) => AdaptiveRegionInput {
                    chunks,
                    decoded: None,
                },
                PrehashedAdaptiveRegion::Contiguous(region) => AdaptiveRegionInput {
                    chunks: region.chunks,
                    decoded: Some(region.decoded),
                },
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            workers,
            gate,
            None,
            &|_| {},
        )
    }

    /// Encodes prehashed partial Records together with byte-for-byte copied
    /// independent Records from verified Container images.
    ///
    /// The copied Record CRC and codec parameters are retained. The enclosing
    /// Container metadata and commitment are rebuilt for the new identity.
    ///
    /// # Errors
    ///
    /// Returns bounded format, allocation, compression, or worker errors.
    pub fn encode_prehashed_adaptive_regions_with_transplants_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[&[PrehashedChunk<'_>]],
        transplanted: Vec<PreparedEncodedRecord>,
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|chunks| AdaptiveRegionInput {
                chunks,
                decoded: None,
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            transplanted,
            Vec::new(),
            Vec::new(),
            workers,
            gate,
            None,
            &|_| {},
        )
    }

    /// Prepares the best independent RAW/Zstd record for one prehashed Chunk.
    ///
    /// This is used only when the same Chunk will enter bounded Prefix trials:
    /// the winning independent fallback is retained, so a rejected dependent
    /// trial never causes a second Zstd encode.
    ///
    /// # Errors
    ///
    /// Returns bounded Chunk, codec, allocation, or record-layout failures.
    pub fn prepare_prehashed_independent_record(
        chunk: PrehashedChunk<'_>,
        gate: IncompressibilityGatePolicy,
    ) -> Result<PreparedIndependentRecord, FormatError> {
        let chunks = [chunk];
        let mut encoded = encode_adaptive_region(&chunks, gate)?;
        if encoded.records.len() != 1 {
            return Err(FormatError::InvalidContainerLayout);
        }
        let Some(record) = encoded.records.pop() else {
            return Err(FormatError::InvalidContainerLayout);
        };
        let record_length = record.record_length()?;
        let mut bytes = vec![0_u8; record_length];
        record.encode_into(&mut bytes)?;
        Ok(PreparedIndependentRecord { bytes })
    }

    /// Encodes independent adaptive regions together with prepared Depth-1
    /// dependent records in one Container image.
    ///
    /// Prefix frames are consumed and copied only once, directly into the
    /// final Container image. Their target identities are prior writer
    /// evidence; ordinary reads, recovery, and scrub independently decode and
    /// rehash every target.
    ///
    /// # Errors
    ///
    /// Returns the same bounded layout, codec, allocation, and worker errors
    /// as [`Self::encode_mixed_prehashed_adaptive_regions_parallel_profiled_with_gate`].
    #[allow(clippy::too_many_arguments)]
    pub fn encode_mixed_prehashed_reduction_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedAdaptiveRegion<'_>],
        independent: Vec<PreparedIndependentRecord>,
        dependents: Vec<PreparedDependentRecord>,
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
        chunk_order: Option<&[ChunkId]>,
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        Self::encode_mixed_prehashed_reduction_with_worker_retirement(
            container_id,
            container_generation,
            regions,
            independent,
            dependents,
            workers,
            gate,
            chunk_order,
            &|_| {},
        )
    }

    /// Encodes regions and retires each worker at its CPU boundary, before
    /// serial Container assembly. The callback also runs on worker failure.
    ///
    /// # Errors
    /// Returns the same format and allocation failures as the ordinary encoder.
    #[allow(clippy::too_many_arguments)]
    pub fn encode_mixed_prehashed_reduction_with_worker_retirement(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[PrehashedAdaptiveRegion<'_>],
        independent: Vec<PreparedIndependentRecord>,
        dependents: Vec<PreparedDependentRecord>,
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
        chunk_order: Option<&[ChunkId]>,
        retire: &(dyn Fn(usize) + Sync),
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        let inputs = regions
            .iter()
            .map(|region| match *region {
                PrehashedAdaptiveRegion::Borrowed(chunks) => AdaptiveRegionInput {
                    chunks,
                    decoded: None,
                },
                PrehashedAdaptiveRegion::Contiguous(region) => AdaptiveRegionInput {
                    chunks: region.chunks,
                    decoded: Some(region.decoded),
                },
            })
            .collect::<Vec<_>>();
        Self::encode_adaptive_region_inputs_parallel_profiled_with_gate(
            container_id,
            container_generation,
            &inputs,
            Vec::new(),
            independent,
            dependents,
            workers,
            gate,
            chunk_order,
            retire,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_adaptive_region_inputs_parallel_profiled_with_gate(
        container_id: ContainerId,
        container_generation: u64,
        regions: &[AdaptiveRegionInput<'_>],
        transplanted: Vec<PreparedEncodedRecord>,
        independent: Vec<PreparedIndependentRecord>,
        dependents: Vec<PreparedDependentRecord>,
        workers: NonZeroUsize,
        gate: IncompressibilityGatePolicy,
        chunk_order: Option<&[ChunkId]>,
        retire: &(dyn Fn(usize) + Sync),
    ) -> Result<AdaptiveContainerEncoding, FormatError> {
        if regions.is_empty()
            && transplanted.is_empty()
            && independent.is_empty()
            && dependents.is_empty()
        {
            return Err(FormatError::InvalidContainerLayout);
        }
        let worker_count = workers.get().min(regions.len().max(1));
        let next_region = AtomicUsize::new(0);
        let encoded_by_region = (0..worker_count)
            .into_par_iter()
            .map(|worker| {
                struct Retire<'a>(&'a (dyn Fn(usize) + Sync), usize);
                impl Drop for Retire<'_> {
                    fn drop(&mut self) {
                        (self.0)(self.1);
                    }
                }
                let _retire = Retire(retire, worker);
                let mut completed = Vec::new();
                loop {
                    // Exactly worker_count jobs own permits; work stealing
                    // happens between regions without bypassing admission.
                    let ordinal = next_region.fetch_add(1, Ordering::Relaxed);
                    if ordinal >= regions.len() {
                        break;
                    }
                    let input = regions[ordinal];
                    let encoded = if let Some(decoded) = input.decoded {
                        encode_adaptive_region_from_decoded(input.chunks, decoded, gate)?
                    } else {
                        encode_adaptive_region(input.chunks, gate)?
                    };
                    completed.push((ordinal, encoded));
                }
                Ok::<_, FormatError>(completed)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let encoded_by_region = {
            let mut ordered = Vec::new();
            ordered
                .try_reserve_exact(regions.len())
                .map_err(|_| FormatError::ArithmeticOverflow)?;
            ordered.resize_with(regions.len(), || None);
            for completed in encoded_by_region {
                for (ordinal, encoded) in completed {
                    assert!(
                        ordered[ordinal].replace(encoded).is_none(),
                        "ASSERT: each Compression Region has exactly one worker owner"
                    );
                }
            }
            Ok::<_, FormatError>(ordered)
        }?;
        let mut encoded_records = Vec::new();
        let mut gate_metrics = IncompressibilityGateMetrics::default();
        for region in encoded_by_region {
            let encoded =
                region.expect("ASSERT: every Compression Region worker must return its output");
            gate_metrics.checked_merge(encoded.metrics)?;
            encoded_records.extend(encoded.records);
        }
        encoded_records.extend(
            transplanted
                .into_iter()
                .map(AdaptiveRecordPlan::PreparedEncoded),
        );
        encoded_records.extend(
            independent
                .into_iter()
                .map(AdaptiveRecordPlan::PreparedIndependent),
        );
        encoded_records.extend(dependents.into_iter().map(AdaptiveRecordPlan::Dependent));
        order_adaptive_records(&mut encoded_records, chunk_order)?;
        let encoding = encode_container_from_adaptive_plans(
            container_id,
            container_generation,
            encoded_records,
            workers,
        )?;
        assert_eq!(
            gate_metrics
                .disabled_regions
                .checked_add(gate_metrics.eligible_regions)
                .and_then(|total| total.checked_add(gate_metrics.size_bypassed_regions)),
            Some(regions.len()),
            "ASSERT: every adaptive region has exactly one gate disposition"
        );
        assert_eq!(
            gate_metrics
                .target_zstd_accepted
                .checked_add(gate_metrics.target_zstd_rejected)
                .and_then(|total| total.checked_add(gate_metrics.raw_regions_after_gate)),
            Some(regions.len()),
            "ASSERT: every adaptive region has exactly one final encoding disposition"
        );
        Ok(AdaptiveContainerEncoding {
            metrics: gate_metrics,
            ..encoding
        })
    }
}

/// One immutable Chunk payload paired with identity evidence computed earlier
/// in the writer pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrehashedChunk<'a> {
    pub(super) chunk_id: ChunkId,
    pub(super) bytes: &'a [u8],
}

/// One Compression Region whose Chunk views partition one existing contiguous
/// decoded buffer.
///
/// This form lets an ingest writer materialize fragmented request data exactly
/// once. Adaptive compression consumes `decoded` directly instead of joining
/// the same Chunks into another temporary vector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrehashedContiguousRegion<'a> {
    chunks: &'a [PrehashedChunk<'a>],
    decoded: &'a [u8],
}

/// One adaptive region supplied either as existing Chunk views or as a buffer
/// the caller already had to materialize from fragments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrehashedAdaptiveRegion<'a> {
    Borrowed(&'a [PrehashedChunk<'a>]),
    Contiguous(PrehashedContiguousRegion<'a>),
}

/// One independently decodable RAW/Zstd record prepared exactly once for a
/// Chunk that also entered bounded dependent-codec trials.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedIndependentRecord {
    bytes: Vec<u8>,
}

/// One already verified, position-independent RAW/Zstd Encoding Record.
///
/// Only [`super::VerifiedContainerImage`] can produce this capability. The Container
/// builder copies the serialized fields byte-for-byte and constructs a new
/// Header, Recovery Index, structural commitment, and Footer around it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedEncodedRecord {
    pub(super) bytes: Vec<u8>,
    pub(super) chunk_count: usize,
}

impl PreparedIndependentRecord {
    #[must_use]
    pub fn encoded_payload_bytes(&self) -> usize {
        get_u32(&self.bytes, 44) as usize
    }

    #[must_use]
    pub fn target_id(&self) -> ChunkId {
        let mut id = [0_u8; 32];
        id.copy_from_slice(&self.bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32]);
        ChunkId::from_bytes(id)
    }
}

impl PreparedEncodedRecord {
    #[must_use]
    pub fn encoded_bytes(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub const fn chunk_count(&self) -> usize {
        self.chunk_count
    }
}

impl<'a> PrehashedContiguousRegion<'a> {
    /// Proves that `chunks` are consecutive, complete views of `decoded`.
    ///
    /// # Errors
    ///
    /// Rejects an empty region, invalid Chunk lengths, a region above the
    /// durable decoded-size bound, or views that do not exactly partition the
    /// supplied buffer in order.
    pub fn new(chunks: &'a [PrehashedChunk<'a>], decoded: &'a [u8]) -> Result<Self, FormatError> {
        if chunks.is_empty() {
            return Err(FormatError::InvalidZstdRecord);
        }
        let mut offset = 0_usize;
        for chunk in chunks {
            validate_logical_chunk_length(chunk.bytes.len())?;
            let end = offset
                .checked_add(chunk.bytes.len())
                .ok_or(FormatError::ArithmeticOverflow)?;
            let expected = decoded
                .get(offset..end)
                .ok_or(FormatError::InvalidZstdRecord)?;
            if expected.as_ptr() != chunk.bytes.as_ptr() {
                return Err(FormatError::InvalidZstdRecord);
            }
            offset = end;
        }
        if offset != decoded.len() || offset > MAX_DECODED_RECORD_BYTES {
            return Err(FormatError::InvalidZstdRecord);
        }
        Ok(Self { chunks, decoded })
    }

    #[must_use]
    pub const fn chunks(self) -> &'a [PrehashedChunk<'a>] {
        self.chunks
    }

    #[must_use]
    pub const fn decoded(self) -> &'a [u8] {
        self.decoded
    }
}

impl<'a> PrehashedChunk<'a> {
    #[must_use]
    pub const fn new(chunk_id: ChunkId, bytes: &'a [u8]) -> Self {
        Self { chunk_id, bytes }
    }

    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.bytes
    }
}

struct EncodedAdaptiveRegion<'a> {
    records: Vec<AdaptiveRecordPlan<'a>>,
    metrics: IncompressibilityGateMetrics,
}

#[derive(Clone, Copy)]
struct AdaptiveRegionInput<'a> {
    chunks: &'a [PrehashedChunk<'a>],
    decoded: Option<&'a [u8]>,
}

#[cfg_attr(test, derive(Clone))]
pub(super) enum AdaptiveRecordPlan<'a> {
    Raw(PrehashedChunk<'a>),
    Zstd {
        chunks: &'a [PrehashedChunk<'a>],
        decoded_length: usize,
        payload: Vec<u8>,
        level: i32,
    },
    PreparedEncoded(PreparedEncodedRecord),
    PreparedIndependent(PreparedIndependentRecord),
    Dependent(PreparedDependentRecord),
}

fn order_adaptive_records(
    records: &mut [AdaptiveRecordPlan<'_>],
    chunk_order: Option<&[ChunkId]>,
) -> Result<(), FormatError> {
    if let Some(order) = chunk_order {
        let mut ordinals = hashbrown::HashTable::<usize>::with_capacity(order.len());
        for (ordinal, &id) in order.iter().enumerate() {
            let hash = chunk_order_hash(id);
            if ordinals.find(hash, |&other| order[other] == id).is_some() {
                return Err(FormatError::InvalidContainerLayout);
            }
            ordinals.insert_unique(hash, ordinal, |&other| chunk_order_hash(order[other]));
        }
        // Keep full identities in the caller's existing order. Cache each
        // Record's ordinal once instead of repeating a tree lookup per comparison.
        records.sort_by_cached_key(|record| {
            let id = record.chunk_id_at(0);
            ordinals
                .find(chunk_order_hash(id), |&other| order[other] == id)
                .copied()
        });
        let mut expected = order.iter();
        for record in records.iter() {
            for ordinal in 0..record.chunk_count() {
                if expected.next() != Some(&record.chunk_id_at(ordinal)) {
                    return Err(FormatError::InvalidContainerLayout);
                }
            }
        }
        if expected.next().is_some() {
            return Err(FormatError::InvalidContainerLayout);
        }
    }

    Ok(())
}

fn chunk_order_hash(id: ChunkId) -> u64 {
    u64::from_le_bytes(
        id.bytes()[..8]
            .try_into()
            .expect("ASSERT: fixed Chunk ID prefix"),
    )
}

impl AdaptiveRecordPlan<'_> {
    fn chunk_id_at(&self, ordinal: usize) -> ChunkId {
        match self {
            Self::Raw(chunk) => chunk.chunk_id,
            Self::Zstd { chunks, .. } => chunks[ordinal].chunk_id,
            Self::PreparedIndependent(record) => record.target_id(),
            Self::Dependent(record) => record.target_id(),
            Self::PreparedEncoded(record) => {
                let start = RECORD_HEADER_BYTES + ordinal * 64;
                ChunkId::from_bytes(
                    record.bytes[start..start + 32]
                        .try_into()
                        .expect("ASSERT: verified encoded record contains its chunk table"),
                )
            }
        }
    }

    pub(super) fn record_length(&self) -> Result<usize, FormatError> {
        match self {
            Self::Raw(chunk) => raw_record_length(chunk.bytes.len()),
            Self::Zstd {
                chunks, payload, ..
            } => zstd_record_length(chunks.len(), payload.len()),
            Self::PreparedEncoded(record) => Ok(record.bytes.len()),
            Self::PreparedIndependent(record) => Ok(record.bytes.len()),
            Self::Dependent(record) => record.record_length(),
        }
    }

    pub(super) fn chunk_count(&self) -> usize {
        match self {
            Self::Zstd { chunks, .. } => chunks.len(),
            Self::PreparedEncoded(record) => record.chunk_count,
            Self::Raw(_) | Self::PreparedIndependent(_) | Self::Dependent(_) => 1,
        }
    }

    pub(super) fn observe_intrinsic_summary(
        &self,
        summary: &mut IntrinsicSummaryAccumulator,
    ) -> Result<(), FormatError> {
        match self {
            Self::Raw(chunk) => {
                summary.observe(RAW_CODEC, self.record_length()?, chunk.bytes.len(), 1, None)
            }
            Self::Zstd {
                chunks,
                decoded_length,
                ..
            } => summary.observe(
                ZSTD_CODEC,
                self.record_length()?,
                *decoded_length,
                chunks.len(),
                None,
            ),
            Self::PreparedEncoded(record) => summary.observe_encoded_record(&record.bytes),
            Self::PreparedIndependent(record) => summary.observe_encoded_record(&record.bytes),
            Self::Dependent(record) => summary.observe(
                record.codec_id(),
                self.record_length()?,
                usize::try_from(record.logical_length())
                    .map_err(|_| FormatError::ArithmeticOverflow)?,
                1,
                Some(record.dependency().chunk_id.bytes()),
            ),
        }
    }

    pub(super) fn encode_into(&self, destination: &mut [u8]) -> Result<(), FormatError> {
        match self {
            Self::Raw(chunk) => encode_prehashed_raw_record_into(*chunk, destination),
            Self::Zstd {
                chunks,
                decoded_length,
                payload,
                level,
            } => encode_prehashed_zstd_record_into(
                chunks,
                *decoded_length,
                payload,
                *level,
                destination,
            ),
            Self::PreparedEncoded(record) => {
                if destination.len() != record.bytes.len() {
                    return Err(FormatError::InvalidRecordLength(destination.len()));
                }
                destination.copy_from_slice(&record.bytes);
                Ok(())
            }
            Self::PreparedIndependent(record) => {
                if destination.len() != record.bytes.len() {
                    return Err(FormatError::InvalidRecordLength(destination.len()));
                }
                destination.copy_from_slice(&record.bytes);
                Ok(())
            }
            Self::Dependent(record) => record.encode_into(destination),
        }
    }

    pub(super) fn append_to(
        &self,
        builder: &mut AlignedContainerBuilder,
    ) -> Result<(), FormatError> {
        let length = self.record_length()?;
        match self {
            Self::Raw(chunk) => {
                builder.append_record(length, RAW_PAYLOAD_OFFSET, chunk.bytes, |bytes| {
                    write_prehashed_raw_metadata(*chunk, bytes)
                })
            }
            Self::Zstd {
                chunks,
                decoded_length,
                payload,
                level,
            } => {
                let metadata_length = RECORD_HEADER_BYTES + chunks.len() * CHUNK_TABLE_ENTRY_BYTES;
                builder.append_record(length, metadata_length, payload, |bytes| {
                    write_prehashed_zstd_metadata(chunks, *decoded_length, payload, *level, bytes)
                })
            }
            Self::PreparedEncoded(record) => {
                builder.append_slice(&record.bytes);
                Ok(())
            }
            Self::PreparedIndependent(record) => {
                builder.append_slice(&record.bytes);
                Ok(())
            }
            Self::Dependent(record) => record.append_to(builder, length),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn encode_adaptive_region<'a>(
    region: &'a [PrehashedChunk<'a>],
    gate: IncompressibilityGatePolicy,
) -> Result<EncodedAdaptiveRegion<'a>, FormatError> {
    if region.is_empty() {
        return Err(FormatError::InvalidZstdRecord);
    }
    if gate == IncompressibilityGatePolicy::Off {
        let decoded_length = prehashed_decoded_length(region)?;
        return encode_adaptive_region_from_input(region, None, decoded_length, gate);
    }
    let decoded = collect_prehashed_decoded(region)?;
    encode_adaptive_region_from_decoded(region, &decoded, gate)
}

#[allow(clippy::too_many_lines)]
fn encode_adaptive_region_from_decoded<'a>(
    region: &'a [PrehashedChunk<'a>],
    decoded: &[u8],
    gate: IncompressibilityGatePolicy,
) -> Result<EncodedAdaptiveRegion<'a>, FormatError> {
    let decoded_length = prehashed_decoded_length(region)?;
    if decoded.len() != decoded_length {
        return Err(FormatError::InvalidZstdRecord);
    }
    encode_adaptive_region_from_input(region, Some(decoded), decoded_length, gate)
}

#[allow(clippy::too_many_lines)]
fn encode_adaptive_region_from_input<'a>(
    region: &'a [PrehashedChunk<'a>],
    decoded: Option<&[u8]>,
    decoded_length: usize,
    gate: IncompressibilityGatePolicy,
) -> Result<EncodedAdaptiveRegion<'a>, FormatError> {
    let raw_bytes = region.iter().try_fold(0_usize, |total, chunk| {
        total
            .checked_add(raw_record_length(chunk.bytes.len())?)
            .ok_or(FormatError::ArithmeticOverflow)
    })?;
    let payload_cap = useful_zstd_payload_cap(raw_bytes, region.len())?;
    let mut metrics = IncompressibilityGateMetrics {
        // The worker-local encoder owns one fixed-size destination buffer.
        // Report resident scratch, not merely the smaller cap passed to a
        // particular codec invocation.
        scratch_high_water_bytes: MAX_DECODED_RECORD_BYTES,
        ..IncompressibilityGateMetrics::default()
    };

    let should_try_target = if gate == IncompressibilityGatePolicy::Off {
        metrics.disabled_regions = 1;
        true
    } else if decoded_length < INCOMPRESSIBILITY_GATE_MIN_BYTES_V1 {
        metrics.size_bypassed_regions = 1;
        true
    } else {
        let decoded = decoded.ok_or(FormatError::InvalidZstdRecord)?;
        metrics.eligible_regions = 1;
        with_adaptive_encoder_v1(|encoder| {
            if encoder.lz4_fits(decoded, payload_cap)? {
                metrics.lz4_allowed_regions = 1;
                return Ok(true);
            }
            metrics.lz4_rejected_regions = 1;
            if gate == IncompressibilityGatePolicy::Lz4Only {
                return Ok(false);
            }
            if encoder.zstd_fits(decoded, ZSTD_RESCUE_LEVEL_V1, payload_cap)? {
                metrics.zstd1_allowed_regions = 1;
                Ok(true)
            } else {
                metrics.zstd1_rejected_regions = 1;
                Ok(false)
            }
        })?
    };

    if should_try_target {
        metrics.target_zstd_trials = 1;
        let payload = with_adaptive_encoder_v1(|encoder| match decoded {
            Some(decoded) => encoder.zstd_owned_payload(decoded, ZSTD_LEVEL_V1, payload_cap),
            None => encoder.zstd_fragmented_owned_payload(
                region,
                decoded_length,
                ZSTD_LEVEL_V1,
                payload_cap,
            ),
        })?;
        if let Some(payload) = payload {
            let record_length = zstd_record_length(region.len(), payload.len())?;
            assert!(
                zstd_record_wins(raw_bytes, record_length)?,
                "ASSERT: a payload bounded by the v1 useful cap must beat RAW"
            );
            metrics.target_zstd_accepted = 1;
            return Ok(EncodedAdaptiveRegion {
                records: vec![AdaptiveRecordPlan::Zstd {
                    chunks: region,
                    decoded_length,
                    payload,
                    level: ZSTD_LEVEL_V1,
                }],
                metrics,
            });
        }
        metrics.target_zstd_rejected = 1;
    } else {
        metrics.raw_regions_after_gate = 1;
    }

    let records = region
        .iter()
        .copied()
        .map(AdaptiveRecordPlan::Raw)
        .collect();
    Ok(EncodedAdaptiveRegion { records, metrics })
}

fn useful_zstd_payload_cap(raw_bytes: usize, chunks: usize) -> Result<usize, FormatError> {
    let percent_numerator = raw_bytes
        .checked_mul(
            usize::try_from(ZSTD_MINIMUM_SAVINGS_PERCENT_V1)
                .map_err(|_| FormatError::ArithmeticOverflow)?,
        )
        .ok_or(FormatError::ArithmeticOverflow)?;
    let percentage_saving = percent_numerator
        .checked_add(99)
        .ok_or(FormatError::ArithmeticOverflow)?
        / 100;
    let required_saving = ZSTD_MINIMUM_SAVINGS_BYTES_V1.max(percentage_saving);
    let complete_cap = raw_bytes.saturating_sub(required_saving);
    let aligned_complete_cap = complete_cap - complete_cap % usize::from(RECORD_ALIGNMENT);
    let payload_offset = RECORD_HEADER_BYTES
        .checked_add(
            chunks
                .checked_mul(CHUNK_TABLE_ENTRY_BYTES)
                .ok_or(FormatError::ArithmeticOverflow)?,
        )
        .ok_or(FormatError::ArithmeticOverflow)?;
    Ok(aligned_complete_cap.saturating_sub(payload_offset))
}

fn zstd_record_wins(raw_bytes: usize, zstd_bytes: usize) -> Result<bool, FormatError> {
    let Some(savings) = raw_bytes.checked_sub(zstd_bytes) else {
        return Ok(false);
    };
    if savings < ZSTD_MINIMUM_SAVINGS_BYTES_V1 {
        return Ok(false);
    }
    let raw = u128::try_from(raw_bytes).map_err(|_| FormatError::ArithmeticOverflow)?;
    let savings = u128::try_from(savings).map_err(|_| FormatError::ArithmeticOverflow)?;
    Ok(savings * 100 >= raw * ZSTD_MINIMUM_SAVINGS_PERCENT_V1)
}

#[cfg(test)]
#[path = "adaptive_tests.rs"]
mod hotpath_tests;
