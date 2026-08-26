use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::BitOr;

use fastdup_format::ChunkId;
use rayon::prelude::*;

use crate::reduction_codec::{
    IndependentEncoding, PreparedDictionary, WorkerCodec, accept_zstd_v1,
};
use crate::reduction_filter::{BlockedBloomHint, BloomLookupHint};
use crate::reduction_prefix::{VerifiedBaseChunk, ZstdPrefixCodec, ZstdPrefixEncoding};
use crate::reduction_similarity::{
    IndependentBaseRef, SimilarityCandidate, SimilarityError, SimilarityFingerprint,
    SimilarityIndex, SparseXorDelta,
};
use crate::{ReductionDictionary, SeqCdcConfig, seqcdc_cut};

const FIXED_CHUNK_BYTES: usize = 64 * 1_024;
const CDC_MIN_BYTES: u32 = 16 * 1_024;
const CDC_TARGET_BYTES: u32 = 64 * 1_024;
const CDC_MAX_BYTES: u32 = 256 * 1_024;
const SEQCDC_SEQUENCE_LENGTH: u16 = 6;
const SEQCDC_SKIP_TRIGGER: u16 = 50;
const SEQCDC_SKIP_BYTES: u32 = 1_024;
const COMPRESSION_REGION_BYTES: u32 = 512 * 1_024;
const PLACEMENT_WINDOW_BYTES: u32 = 64 * 1_024 * 1_024;
const MAXIMUM_SIMILARITY_CANDIDATES: u8 = 16;
const MAXIMUM_TRIAL_ENCODINGS: u8 = 4;
const ZSTD_LEVEL_V1: i32 = 3;
const FILL_MINIMUM_BYTES: usize = 64 * 1_024;
const DICTIONARY_DEPENDENCY_BYTES: u64 = 32;
const EXACT_BLOOM_EXPECTED_KEYS: usize = 1 << 20;
const EXACT_BLOOM_MAXIMUM_BYTES: usize = 4 * 1_024 * 1_024;
const DELTA_MINIMUM_SAVINGS_BYTES: u64 = 4_096;
const DELTA_MINIMUM_SAVINGS_PERCENT: u128 = 5;
const PERCENT_DENOMINATOR: u128 = 100;

thread_local! {
    /// One mutable Zstd encode/decode context per permanent Rayon worker.
    ///
    /// Reduction tasks never share the context and therefore need no codec
    /// lock. Keeping it in TLS avoids rebuilding Zstd state at every
    /// Container, Similarity, or Delta phase boundary.
    static REDUCTION_CODEC_V1: RefCell<Option<WorkerCodec>> = const { RefCell::new(None) };
}

fn with_worker_codec<T>(
    operation: impl FnOnce(&mut WorkerCodec) -> Result<T, ReductionError>,
) -> Result<T, ReductionError> {
    REDUCTION_CODEC_V1.with(|codec| {
        let mut codec = codec.borrow_mut();
        if codec.is_none() {
            *codec = Some(WorkerCodec::new().map_err(|error| codec_error(&error))?);
        }
        operation(
            codec
                .as_mut()
                .expect("ASSERT: worker-local Reduction Codec was initialized"),
        )
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReductionFeatures(u16);

impl ReductionFeatures {
    pub const RAW: Self = Self(1 << 0);
    pub const CDC: Self = Self(1 << 1);
    pub const EXACT: Self = Self(1 << 2);
    pub const COMPRESSION: Self = Self(1 << 3);
    pub const GROUPING: Self = Self(1 << 4);
    pub const SIMILARITY: Self = Self(1 << 5);
    pub const DELTA: Self = Self(1 << 6);
    pub const REORDER: Self = Self(1 << 7);
    pub const ZSTD_PREFIX: Self = Self(1 << 8);
    pub const ALL: Self = Self(0x1ff);

    #[must_use]
    pub const fn contains(self, feature: Self) -> bool {
        self.0 & feature.0 == feature.0
    }
}

impl BitOr for ReductionFeatures {
    type Output = Self;

    fn bitor(self, right: Self) -> Self::Output {
        Self(self.0 | right.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReductionPolicy {
    features: ReductionFeatures,
    id: [u8; 32],
}

impl ReductionPolicy {
    /// Constructs the bounded, versioned v1 writer policy.
    ///
    /// # Errors
    ///
    /// Returns an error for a feature dependency that cannot produce a
    /// byte-exact independently decodable fallback.
    pub fn v1(features: ReductionFeatures) -> Result<Self, ReductionError> {
        if features.contains(ReductionFeatures::DELTA)
            && !features.contains(ReductionFeatures::SIMILARITY)
        {
            return Err(ReductionError::InvalidPolicy(
                "DELTA requires SIMILARITY candidate search",
            ));
        }
        if features.contains(ReductionFeatures::ZSTD_PREFIX)
            && !features.contains(ReductionFeatures::DELTA)
        {
            return Err(ReductionError::InvalidPolicy("ZSTD_PREFIX requires DELTA"));
        }
        if features.contains(ReductionFeatures::GROUPING)
            && !features.contains(ReductionFeatures::COMPRESSION)
        {
            return Err(ReductionError::InvalidPolicy(
                "GROUPING requires COMPRESSION",
            ));
        }
        if !features.contains(ReductionFeatures::RAW)
            && !features.contains(ReductionFeatures::COMPRESSION)
        {
            return Err(ReductionError::InvalidPolicy(
                "RAW or COMPRESSION is required for independent encoding",
            ));
        }

        let mut canonical = [0_u8; 40];
        canonical[0..8].copy_from_slice(b"FDRSEQ01");
        canonical[8..10].copy_from_slice(&1_u16.to_le_bytes());
        canonical[10..12].copy_from_slice(&features.0.to_le_bytes());
        canonical[12..16].copy_from_slice(&CDC_MIN_BYTES.to_le_bytes());
        canonical[16..20].copy_from_slice(&CDC_TARGET_BYTES.to_le_bytes());
        canonical[20..24].copy_from_slice(&CDC_MAX_BYTES.to_le_bytes());
        canonical[24..28].copy_from_slice(&COMPRESSION_REGION_BYTES.to_le_bytes());
        canonical[28] = MAXIMUM_SIMILARITY_CANDIDATES;
        canonical[29] = MAXIMUM_TRIAL_ENCODINGS;
        canonical[30..32].copy_from_slice(&SEQCDC_SEQUENCE_LENGTH.to_le_bytes());
        canonical[32..34].copy_from_slice(&SEQCDC_SKIP_TRIGGER.to_le_bytes());
        canonical[34..38].copy_from_slice(&SEQCDC_SKIP_BYTES.to_le_bytes());
        canonical[38] = 1;
        Ok(Self {
            features,
            id: ChunkId::of(&canonical).bytes(),
        })
    }

    #[must_use]
    pub const fn features(self) -> ReductionFeatures {
        self.features
    }

    #[must_use]
    pub const fn id(self) -> [u8; 32] {
        self.id
    }

    #[must_use]
    pub const fn cdc_min_bytes(self) -> u32 {
        CDC_MIN_BYTES
    }

    #[must_use]
    pub const fn cdc_target_bytes(self) -> u32 {
        CDC_TARGET_BYTES
    }

    #[must_use]
    pub const fn cdc_max_bytes(self) -> u32 {
        CDC_MAX_BYTES
    }

    #[must_use]
    pub const fn compression_region_bytes(self) -> u32 {
        COMPRESSION_REGION_BYTES
    }

    #[must_use]
    pub const fn placement_window_bytes(self) -> u32 {
        PLACEMENT_WINDOW_BYTES
    }

    #[must_use]
    pub const fn maximum_similarity_candidates(self) -> u8 {
        MAXIMUM_SIMILARITY_CANDIDATES
    }

    #[must_use]
    pub const fn maximum_trial_encodes(self) -> u8 {
        MAXIMUM_TRIAL_ENCODINGS
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReductionRuntime {
    workers: NonZeroUsize,
    maximum_inflight_bytes: usize,
}

impl ReductionRuntime {
    /// Creates execution-only scheduling limits that do not affect policy or
    /// logical output.
    ///
    /// `maximum_inflight_bytes` limits the number and decoded size of work
    /// units scheduled concurrently. It is not a process peak-RSS promise:
    /// source buffers, completed archive records, indexes, codec contexts, and
    /// writer self-check buffers are deliberately outside this reference
    /// engine's scheduling budget.
    ///
    /// # Errors
    ///
    /// Returns an error if less than one maximum-size chunk can be in flight.
    pub const fn new(
        workers: NonZeroUsize,
        maximum_inflight_bytes: usize,
    ) -> Result<Self, ReductionError> {
        if maximum_inflight_bytes < CDC_MAX_BYTES as usize {
            return Err(ReductionError::InvalidRuntime(
                "inflight budget is smaller than one maximum-size chunk",
            ));
        }
        Ok(Self {
            workers,
            maximum_inflight_bytes,
        })
    }

    #[must_use]
    pub const fn workers(self) -> NonZeroUsize {
        self.workers
    }

    #[must_use]
    pub const fn maximum_inflight_bytes(self) -> usize {
        self.maximum_inflight_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReducedObject(u64);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReductionReport {
    logical_bytes: u64,
    physical_payload_bytes: u64,
    exact_hit_bytes: u64,
    logical_chunks: usize,
    minimum_chunk_bytes: usize,
    maximum_chunk_bytes: usize,
    raw_chunks: usize,
    zstd_regions: usize,
    zstd_dictionary_regions: usize,
    delta_chunks: usize,
    zstd_prefix_chunks: usize,
    similarity_candidates: usize,
    delta_trials: usize,
    delta_logical_bytes: u64,
    delta_payload_bytes: u64,
    maximum_delta_depth: u8,
    exact_hits: usize,
    maximum_region_decoded_bytes: usize,
    workers_used: usize,
    fill_extents: usize,
    fill_bytes: u64,
    reordered_regions: usize,
    placement_windows: usize,
}

impl ReductionReport {
    #[must_use]
    pub const fn logical_bytes(self) -> u64 {
        self.logical_bytes
    }

    #[must_use]
    pub const fn physical_payload_bytes(self) -> u64 {
        self.physical_payload_bytes
    }

    #[must_use]
    pub const fn exact_hit_bytes(self) -> u64 {
        self.exact_hit_bytes
    }

    #[must_use]
    pub const fn logical_chunks(self) -> usize {
        self.logical_chunks
    }

    #[must_use]
    pub const fn minimum_chunk_bytes(self) -> usize {
        self.minimum_chunk_bytes
    }

    #[must_use]
    pub const fn maximum_chunk_bytes(self) -> usize {
        self.maximum_chunk_bytes
    }

    #[must_use]
    pub const fn raw_chunks(self) -> usize {
        self.raw_chunks
    }

    #[must_use]
    pub const fn zstd_regions(self) -> usize {
        self.zstd_regions
    }

    #[must_use]
    pub const fn zstd_dictionary_regions(self) -> usize {
        self.zstd_dictionary_regions
    }

    #[must_use]
    pub const fn delta_chunks(self) -> usize {
        self.delta_chunks
    }

    #[must_use]
    pub const fn zstd_prefix_chunks(self) -> usize {
        self.zstd_prefix_chunks
    }

    #[must_use]
    pub const fn similarity_candidates(self) -> usize {
        self.similarity_candidates
    }

    #[must_use]
    pub const fn delta_trials(self) -> usize {
        self.delta_trials
    }

    #[must_use]
    pub const fn delta_logical_bytes(self) -> u64 {
        self.delta_logical_bytes
    }

    #[must_use]
    pub const fn delta_payload_bytes(self) -> u64 {
        self.delta_payload_bytes
    }

    #[must_use]
    pub const fn maximum_delta_depth(self) -> u8 {
        self.maximum_delta_depth
    }

    #[must_use]
    pub const fn exact_hits(self) -> usize {
        self.exact_hits
    }

    #[must_use]
    pub const fn maximum_region_decoded_bytes(self) -> usize {
        self.maximum_region_decoded_bytes
    }

    #[must_use]
    pub const fn workers_used(self) -> usize {
        self.workers_used
    }

    #[must_use]
    pub const fn fill_extents(self) -> usize {
        self.fill_extents
    }

    #[must_use]
    pub const fn fill_bytes(self) -> u64 {
        self.fill_bytes
    }

    #[must_use]
    pub const fn reordered_regions(self) -> usize {
        self.reordered_regions
    }

    #[must_use]
    pub const fn placement_windows(self) -> usize {
        self.placement_windows
    }
}

/// Compact result of an expensive full reference-archive `AUDIT`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReductionAuditReport {
    objects: usize,
    records: usize,
    chunks: usize,
    logical_bytes: u64,
}

impl ReductionAuditReport {
    #[must_use]
    pub const fn objects_verified(self) -> usize {
        self.objects
    }

    #[must_use]
    pub const fn records_verified(self) -> usize {
        self.records
    }

    #[must_use]
    pub const fn chunks_verified(self) -> usize {
        self.chunks
    }

    #[must_use]
    pub const fn logical_bytes_verified(self) -> u64 {
        self.logical_bytes
    }
}

pub struct ReductionEngine {
    policy: ReductionPolicy,
    runtime: ReductionRuntime,
    records: Vec<EncodingRecord>,
    objects: Vec<ArchiveObject>,
    exact_index: ExactIndex,
    independent_index: IndependentIndex,
    similarity_index: SimilarityIndex,
    restore_codec: RefCell<Option<WorkerCodec>>,
    dictionary: Option<PreparedDictionary>,
}

impl ReductionEngine {
    #[must_use]
    pub fn new(policy: ReductionPolicy, runtime: ReductionRuntime) -> Self {
        Self {
            policy,
            runtime,
            records: Vec::new(),
            objects: Vec::new(),
            exact_index: ExactIndex::new(),
            independent_index: IndependentIndex::new(),
            similarity_index: SimilarityIndex::new(),
            restore_codec: RefCell::new(None),
            dictionary: None,
        }
    }

    /// Creates a reference engine that may select one immutable Zstd
    /// dictionary when it beats both RAW and ordinary Zstd.
    ///
    /// # Errors
    ///
    /// Returns an invalid-policy error when compression is disabled, or a
    /// codec error if the immutable dictionary cannot be re-verified.
    pub fn with_dictionary(
        policy: ReductionPolicy,
        runtime: ReductionRuntime,
        dictionary: &ReductionDictionary,
    ) -> Result<Self, ReductionError> {
        if !policy.features.contains(ReductionFeatures::COMPRESSION) {
            return Err(ReductionError::InvalidPolicy(
                "a Zstd dictionary requires COMPRESSION",
            ));
        }
        let prepared =
            PreparedDictionary::try_from(dictionary).map_err(|error| codec_error(&error))?;
        let mut engine = Self::new(policy, runtime);
        engine.dictionary = Some(prepared);
        Ok(engine)
    }

    /// Reduces one immutable byte snapshot into the experimental in-memory
    /// reference archive.
    ///
    /// # Errors
    ///
    /// Returns a policy, size, or archive-capacity error.
    ///
    /// # Panics
    ///
    /// Panics only if the number of in-memory objects cannot fit a `u64`, an
    /// impossible production `ASSERT` on supported Rust targets.
    #[expect(
        clippy::too_many_lines,
        reason = "ordered ingest keeps publication, index insertion, and recipe assembly local"
    )]
    pub fn ingest(&mut self, input: &[u8]) -> Result<ReducedObject, ReductionError> {
        if !self.policy.features.contains(ReductionFeatures::RAW)
            && !self
                .policy
                .features
                .contains(ReductionFeatures::COMPRESSION)
        {
            return Err(ReductionError::Unsupported(
                "no independent encoding is enabled",
            ));
        }
        let logical_bytes = u64::try_from(input.len())
            .map_err(|_| ReductionError::InvalidInput("logical length does not fit u64"))?;
        let chunked = self.logical_chunks(input);
        let chunks = &chunked.chunks;
        let mut minimum_chunk_bytes = usize::MAX;
        let mut maximum_chunk_bytes = 0_usize;
        for chunk in chunks {
            minimum_chunk_bytes = minimum_chunk_bytes.min(chunk.length);
            maximum_chunk_bytes = maximum_chunk_bytes.max(chunk.length);
        }
        if chunks.is_empty() {
            minimum_chunk_bytes = 0;
        }
        let plan = self.plan_regions(chunks)?;
        let mut encoded = self.encode_regions(input, &plan.regions)?;
        let fingerprints = self.fingerprint_regions(input, chunks, &encoded.regions)?;
        let similarity_stats = self.apply_similarity_delta(
            input,
            chunks,
            &plan.recipe_sources,
            &fingerprints.values,
            &mut encoded.regions,
        )?;
        let reorder_stats =
            self.reorder_regions(chunks, &fingerprints.values, &mut encoded.regions);
        encoded.stats.workers_used = encoded
            .stats
            .workers_used
            .max(chunked.workers_used)
            .max(fingerprints.workers_used)
            .max(reorder_stats.workers_used);
        let encoded_stats = summarize_encoded(&encoded.regions, encoded.stats.workers_used)?;
        let mut new_locations = vec![None; chunks.len()];

        for encoded_region in encoded.regions {
            let record_index = self.records.len();
            assert_eq!(
                encoded_region.logical_ordinals.len(),
                encoded_region.record.chunks.len(),
                "ASSERT: every encoded record chunk has one logical owner"
            );
            self.records.push(encoded_region.record);
            let record = self
                .records
                .get(record_index)
                .expect("ASSERT: the record just appended must exist");
            for (&logical_ordinal, chunk) in encoded_region
                .logical_ordinals
                .iter()
                .zip(record.chunks.iter())
            {
                let location = ChunkLocation {
                    record: record_index,
                    decoded_offset: chunk.decoded_offset,
                    length: chunk.length,
                };
                let slot = new_locations
                    .get_mut(logical_ordinal)
                    .expect("ASSERT: a planned logical chunk ordinal is in bounds");
                assert!(
                    slot.replace(location).is_none(),
                    "ASSERT: a new logical chunk receives exactly one Location"
                );
                if self.policy.features.contains(ReductionFeatures::EXACT) {
                    self.exact_index.insert(chunk.id, chunk.length, location)?;
                }
                if record.encoding.is_independent() {
                    self.independent_index
                        .insert(chunk.id, chunk.length, location)?;
                    if self.policy.features.contains(ReductionFeatures::SIMILARITY) {
                        let fingerprint = fingerprints
                            .values
                            .get(logical_ordinal)
                            .copied()
                            .flatten()
                            .expect("ASSERT: every new Similarity Chunk has a fingerprint");
                        self.similarity_index
                            .insert(
                                chunk.id,
                                u32::try_from(chunk.length).map_err(|_| {
                                    ReductionError::InvalidInput(
                                        "logical chunk length does not fit u32",
                                    )
                                })?,
                                fingerprint,
                            )
                            .map_err(similarity_writer_error)?;
                    }
                }
            }
        }

        let mut data_recipe = Vec::with_capacity(chunks.len());
        for (logical_ordinal, source) in plan.recipe_sources.into_iter().enumerate() {
            let location = match source {
                RecipeSource::Existing(location) => location,
                RecipeSource::New => new_locations[logical_ordinal]
                    .expect("ASSERT: every planned new chunk has a Location"),
                RecipeSource::PendingExact(owner) => {
                    new_locations[owner].expect("ASSERT: an in-run Exact Hit owner has a Location")
                }
            };
            data_recipe.push(DataRecipeEntry {
                id: chunks[logical_ordinal].id,
                location,
            });
        }
        assert_eq!(
            data_recipe.len(),
            chunks.len(),
            "ASSERT: every DATA chunk has one recipe entry"
        );
        let mut recipe = Vec::with_capacity(chunked.layout.len());
        let mut fill_extents = 0_usize;
        let mut fill_bytes = 0_u64;
        for extent in chunked.layout {
            match extent {
                InputExtent::Data(ordinal) => {
                    let entry = data_recipe
                        .get(ordinal)
                        .copied()
                        .expect("ASSERT: a DATA layout ordinal is in bounds");
                    recipe.push(RecipeEntry::Data(entry));
                }
                InputExtent::Fill { byte, length } => {
                    assert!(
                        length >= FILL_MINIMUM_BYTES,
                        "ASSERT: a writer FILL meets the versioned threshold"
                    );
                    fill_extents = fill_extents
                        .checked_add(1)
                        .expect("ASSERT: FILL extents cannot exceed input bytes");
                    fill_bytes = fill_bytes
                        .checked_add(
                            u64::try_from(length)
                                .expect("ASSERT: an in-memory FILL length fits u64"),
                        )
                        .expect("ASSERT: FILL bytes cannot exceed logical bytes");
                    recipe.push(RecipeEntry::Fill { byte, length });
                }
            }
        }

        assert!(
            plan.exact_hit_bytes <= logical_bytes,
            "ASSERT: Exact Hit bytes cannot exceed the input"
        );
        assert!(
            encoded_stats.physical_payload_bytes <= logical_bytes,
            "ASSERT: physical payload cannot exceed the input"
        );
        let report = ReductionReport {
            logical_bytes,
            physical_payload_bytes: encoded_stats.physical_payload_bytes,
            exact_hit_bytes: plan.exact_hit_bytes,
            logical_chunks: chunks.len(),
            minimum_chunk_bytes,
            maximum_chunk_bytes,
            raw_chunks: encoded_stats.raw_chunks,
            zstd_regions: encoded_stats.zstd_regions,
            zstd_dictionary_regions: encoded_stats.zstd_dictionary_regions,
            delta_chunks: encoded_stats.delta_chunks,
            zstd_prefix_chunks: encoded_stats.zstd_prefix_chunks,
            similarity_candidates: similarity_stats.candidates,
            delta_trials: similarity_stats.trials,
            delta_logical_bytes: encoded_stats.delta_logical_bytes,
            delta_payload_bytes: encoded_stats.delta_payload_bytes,
            maximum_delta_depth: u8::from(encoded_stats.delta_chunks != 0),
            exact_hits: plan.exact_hits,
            maximum_region_decoded_bytes: encoded_stats.maximum_region_decoded_bytes,
            workers_used: encoded_stats.workers_used,
            fill_extents,
            fill_bytes,
            reordered_regions: reorder_stats.reordered_regions,
            placement_windows: reorder_stats.placement_windows,
        };
        self.objects.push(ArchiveObject { recipe, report });
        let ordinal = u64::try_from(self.objects.len())
            .expect("ASSERT: an in-memory object ordinal always fits u64");
        Ok(ReducedObject(ordinal))
    }

    /// Restores and verifies one previously ingested object.
    ///
    /// # Errors
    ///
    /// Returns an unknown-object error or corruption if a stored logical chunk
    /// no longer matches its BLAKE3 identity.
    pub fn restore(&self, object: ReducedObject) -> Result<Vec<u8>, ReductionError> {
        let archived = self.object(object)?;
        let capacity = usize::try_from(archived.report.logical_bytes)
            .map_err(|_| ReductionError::Corruption("logical length does not fit memory"))?;
        let mut restored = Vec::with_capacity(capacity);
        let mut codec_slot = self
            .restore_codec
            .try_borrow_mut()
            .map_err(|_| ReductionError::Codec("restore codec is already in use".to_owned()))?;
        if codec_slot.is_none() {
            *codec_slot = Some(WorkerCodec::new().map_err(|error| codec_error(&error))?);
        }
        let codec = codec_slot
            .as_mut()
            .ok_or_else(|| ReductionError::Codec("restore codec is unavailable".to_owned()))?;
        let mut cached_record = None;
        let mut cached_decoded = Vec::new();

        for recipe_entry in &archived.recipe {
            let RecipeEntry::Data(recipe_entry) = recipe_entry else {
                let RecipeEntry::Fill { byte, length } = *recipe_entry else {
                    unreachable!("ASSERT: every recipe entry is DATA or FILL")
                };
                if length < FILL_MINIMUM_BYTES {
                    return Err(ReductionError::Corruption(
                        "FILL extent is shorter than the versioned threshold",
                    ));
                }
                let end = restored
                    .len()
                    .checked_add(length)
                    .ok_or(ReductionError::Corruption("FILL extent length overflows"))?;
                if end > capacity {
                    return Err(ReductionError::Corruption(
                        "FILL extent exceeds the object logical length",
                    ));
                }
                restored.resize(end, byte);
                cached_record = None;
                cached_decoded.clear();
                continue;
            };
            let location = recipe_entry.location;
            let record = self
                .records
                .get(location.record)
                .ok_or(ReductionError::Corruption("recipe record is absent"))?;
            if cached_record != Some(location.record) {
                cached_decoded = self.decode_record(codec, record)?;
                verify_decoded_record(record, &cached_decoded)?;
                cached_record = Some(location.record);
            }
            let table_entry = record
                .chunks
                .iter()
                .find(|chunk| {
                    chunk.decoded_offset == location.decoded_offset
                        && chunk.length == location.length
                })
                .ok_or(ReductionError::Corruption(
                    "recipe Location is absent from the record chunk table",
                ))?;
            if table_entry.id != recipe_entry.id {
                return Err(ReductionError::Corruption(
                    "recipe Chunk ID disagrees with its record chunk table",
                ));
            }
            let end = location
                .decoded_offset
                .checked_add(location.length)
                .ok_or(ReductionError::Corruption("recipe decoded slice overflows"))?;
            let bytes = cached_decoded.get(location.decoded_offset..end).ok_or(
                ReductionError::Corruption("recipe decoded slice lies outside the record"),
            )?;
            if ChunkId::of(bytes) != recipe_entry.id {
                return Err(ReductionError::Corruption(
                    "restored logical Chunk ID mismatch",
                ));
            }
            restored.extend_from_slice(bytes);
        }
        if restored.len() != capacity {
            return Err(ReductionError::Corruption(
                "restored object length disagrees with recipe",
            ));
        }
        Ok(restored)
    }

    /// Performs an expensive full `AUDIT` of every record, recipe, and
    /// rebuildable in-memory index without retaining complete object restores.
    ///
    /// Every Encoding Record is decoded and every Chunk ID is rehashed. Recipe
    /// Locations and logical-length sums are then paired against the verified
    /// records. Exact, independent-Base, Bloom, and Similarity acceleration
    /// state is checked against the same source objects.
    ///
    /// # Errors
    ///
    /// Returns a defined corruption, codec, or arithmetic error. This path is
    /// deliberately expensive and is not part of normal ingest or restore.
    pub fn audit(&self) -> Result<ReductionAuditReport, ReductionError> {
        let mut codec = WorkerCodec::new().map_err(|error| codec_error(&error))?;
        let mut chunks_verified = 0_usize;
        for record in &self.records {
            let decoded = self.decode_record(&mut codec, record)?;
            verify_decoded_record(record, &decoded)?;
            chunks_verified = chunks_verified.checked_add(record.chunks.len()).ok_or(
                ReductionError::Corruption("AUDIT verified Chunk count overflows usize"),
            )?;
        }

        self.exact_index.audit(&self.records)?;
        self.independent_index.audit(&self.records)?;
        self.similarity_index
            .audit()
            .map_err(|_| ReductionError::Corruption("Similarity Index AUDIT failed"))?;

        let mut logical_bytes_verified = 0_u64;
        for object in &self.objects {
            let mut object_bytes = 0_u64;
            for entry in &object.recipe {
                let length = match *entry {
                    RecipeEntry::Data(data) => {
                        verify_index_location(
                            &self.records,
                            data.id,
                            data.location.length,
                            data.location,
                            false,
                        )?;
                        data.location.length
                    }
                    RecipeEntry::Fill { length, .. } => {
                        if length < FILL_MINIMUM_BYTES {
                            return Err(ReductionError::Corruption(
                                "AUDIT found a sub-threshold FILL extent",
                            ));
                        }
                        length
                    }
                };
                object_bytes = object_bytes
                    .checked_add(u64::try_from(length).map_err(|_| {
                        ReductionError::Corruption("AUDIT recipe length does not fit u64")
                    })?)
                    .ok_or(ReductionError::Corruption(
                        "AUDIT object logical length overflows u64",
                    ))?;
            }
            if object_bytes != object.report.logical_bytes
                || object.report.maximum_delta_depth > 1
                || object.report.physical_payload_bytes > object.report.logical_bytes
            {
                return Err(ReductionError::Corruption(
                    "AUDIT object report disagrees with its verified recipe",
                ));
            }
            logical_bytes_verified = logical_bytes_verified.checked_add(object_bytes).ok_or(
                ReductionError::Corruption("AUDIT aggregate logical length overflows u64"),
            )?;
        }

        Ok(ReductionAuditReport {
            objects: self.objects.len(),
            records: self.records.len(),
            chunks: chunks_verified,
            logical_bytes: logical_bytes_verified,
        })
    }

    fn decode_record(
        &self,
        codec: &mut WorkerCodec,
        record: &EncodingRecord,
    ) -> Result<Vec<u8>, ReductionError> {
        match &record.encoding {
            RecordEncoding::Independent(encoding) => {
                let dictionary = dictionary_for_encoding(encoding, self.dictionary.as_ref())?;
                codec
                    .decode(encoding, encoding.decoded_length(), dictionary)
                    .map_err(|_| ReductionError::Corruption("record codec decode failed"))
            }
            RecordEncoding::Delta { encoding, .. } => {
                let base_id = encoding.base_chunk_id();
                let base_length =
                    usize::try_from(encoding.base_logical_length()).map_err(|_| {
                        ReductionError::Corruption("Delta Base length does not fit memory")
                    })?;
                let location = self.independent_index.lookup(base_id, base_length)?.ok_or(
                    ReductionError::Corruption(
                        "Delta Base has no independently decodable Location",
                    ),
                )?;
                let base = decode_independent_location(
                    codec,
                    &self.records,
                    base_id,
                    base_length,
                    location,
                    self.dictionary.as_ref(),
                )?;
                encoding.decode(&base.bytes)
            }
        }
    }

    /// Returns immutable accounting for one ingested object.
    ///
    /// # Errors
    ///
    /// Returns an error for an object identity not created by this engine.
    pub fn report(&self, object: ReducedObject) -> Result<ReductionReport, ReductionError> {
        Ok(self.object(object)?.report)
    }

    fn logical_chunks(&self, input: &[u8]) -> ChunkedInput {
        let segments = if self.policy.features.contains(ReductionFeatures::CDC) {
            split_fill_segments(input)
        } else if input.is_empty() {
            Vec::new()
        } else {
            vec![InputSegment::Data {
                input_offset: 0,
                length: input.len(),
            }]
        };
        let mut ranges = Vec::new();
        let mut layout = Vec::new();
        for segment in segments {
            match segment {
                InputSegment::Fill { byte, length } => {
                    layout.push(InputExtent::Fill { byte, length });
                }
                InputSegment::Data {
                    input_offset,
                    length,
                } => {
                    let input_end = input_offset
                        .checked_add(length)
                        .expect("ASSERT: a DATA segment range cannot overflow");
                    let bytes = input
                        .get(input_offset..input_end)
                        .expect("ASSERT: a DATA segment lies within the input");
                    for (relative_offset, chunk_length) in self.chunk_ranges(bytes) {
                        let chunk_offset = input_offset
                            .checked_add(relative_offset)
                            .expect("ASSERT: a DATA chunk offset cannot overflow");
                        let ordinal = ranges.len();
                        ranges.push((chunk_offset, chunk_length));
                        layout.push(InputExtent::Data(ordinal));
                    }
                }
            }
        }
        let budget_workers = self
            .runtime
            .maximum_inflight_bytes
            .checked_div(CDC_MAX_BYTES as usize)
            .unwrap_or(0)
            .max(1);
        let worker_count = self
            .runtime
            .workers
            .get()
            .min(budget_workers)
            .min(ranges.len());
        let ids = parallel_chunk_ids(input, &ranges, worker_count);
        assert_eq!(
            ids.len(),
            ranges.len(),
            "ASSERT: every logical Chunk range has one BLAKE3 identity"
        );
        let chunks = ranges
            .into_iter()
            .zip(ids)
            .map(|((input_offset, length), id)| LogicalChunk {
                id,
                input_offset,
                length,
            })
            .collect();
        ChunkedInput {
            chunks,
            layout,
            workers_used: worker_count,
        }
    }

    fn chunk_ranges(&self, input: &[u8]) -> Vec<(usize, usize)> {
        if !self.policy.features.contains(ReductionFeatures::CDC) {
            return input
                .chunks(FIXED_CHUNK_BYTES)
                .scan(0_usize, |offset, chunk| {
                    let range = (*offset, chunk.len());
                    *offset = offset
                        .checked_add(chunk.len())
                        .expect("ASSERT: fixed chunk offsets cannot exceed the input");
                    Some(range)
                })
                .collect();
        }

        let config = SeqCdcConfig {
            sequence_length: SEQCDC_SEQUENCE_LENGTH,
            skip_trigger: SEQCDC_SKIP_TRIGGER,
            skip_bytes: SEQCDC_SKIP_BYTES as usize,
            minimum_bytes: CDC_MIN_BYTES as usize,
            maximum_bytes: CDC_MAX_BYTES as usize,
        };
        let mut expected_offset = 0_usize;
        let mut ranges = Vec::new();
        while expected_offset < input.len() {
            let length = seqcdc_cut(&input[expected_offset..], config);
            assert!(length > 0, "ASSERT: SeqCDC Chunks are nonempty");
            assert!(
                length <= CDC_MAX_BYTES as usize,
                "ASSERT: SeqCDC exceeded the configured maximum"
            );
            let offset = expected_offset;
            expected_offset = expected_offset
                .checked_add(length)
                .expect("ASSERT: SeqCDC Chunk end cannot overflow");
            assert!(
                expected_offset <= input.len(),
                "ASSERT: SeqCDC Chunk lies outside the input"
            );
            ranges.push((offset, length));
        }
        assert_eq!(
            expected_offset,
            input.len(),
            "ASSERT: SeqCDC must partition the complete input"
        );
        ranges
    }

    fn plan_regions(&self, chunks: &[LogicalChunk]) -> Result<IngestPlan, ReductionError> {
        let exact_enabled = self.policy.features.contains(ReductionFeatures::EXACT);
        let grouping_enabled = self.policy.features.contains(ReductionFeatures::GROUPING);
        let region_limit =
            (COMPRESSION_REGION_BYTES as usize).min(self.runtime.maximum_inflight_bytes);
        let mut pending_new = BTreeMap::new();
        let mut recipe_sources = Vec::with_capacity(chunks.len());
        let mut regions = Vec::new();
        let mut current_region = None;
        let mut exact_hits = 0_usize;
        let mut exact_hit_bytes = 0_u64;

        for (logical_ordinal, chunk) in chunks.iter().enumerate() {
            let key = (chunk.id, chunk.length);
            let exact_source = if exact_enabled {
                if let Some(location) = self.exact_index.lookup(chunk.id, chunk.length)? {
                    self.verify_exact_location(chunk.id, chunk.length, location)?;
                    Some(RecipeSource::Existing(location))
                } else {
                    pending_new
                        .get(&key)
                        .copied()
                        .map(RecipeSource::PendingExact)
                }
            } else {
                None
            };

            if let Some(source) = exact_source {
                flush_region(&mut current_region, &mut regions);
                recipe_sources.push(source);
                exact_hits = exact_hits
                    .checked_add(1)
                    .expect("ASSERT: Exact Hits cannot exceed logical chunks");
                exact_hit_bytes = exact_hit_bytes
                    .checked_add(
                        u64::try_from(chunk.length)
                            .expect("ASSERT: a bounded chunk length fits u64"),
                    )
                    .expect("ASSERT: Exact Hit bytes cannot exceed logical bytes");
                continue;
            }

            if exact_enabled {
                let previous = pending_new.insert(key, logical_ordinal);
                assert!(
                    previous.is_none(),
                    "ASSERT: an in-run duplicate must have been classified as an Exact Hit"
                );
            }
            recipe_sources.push(RecipeSource::New);

            let can_append = current_region.as_ref().is_some_and(|region: &RegionPlan| {
                grouping_enabled
                    && region
                        .decoded_length
                        .checked_add(chunk.length)
                        .is_some_and(|length| length <= region_limit)
                    && region
                        .input_offset
                        .checked_add(region.decoded_length)
                        .is_some_and(|offset| offset == chunk.input_offset)
                    && region.input_offset / PLACEMENT_WINDOW_BYTES as usize
                        == chunk.input_offset / PLACEMENT_WINDOW_BYTES as usize
            });
            if !can_append {
                flush_region(&mut current_region, &mut regions);
                current_region = Some(RegionPlan {
                    ordinal: regions.len(),
                    input_offset: chunk.input_offset,
                    decoded_length: 0,
                    members: Vec::new(),
                });
            }
            let region = current_region
                .as_mut()
                .expect("ASSERT: a new chunk always has a current region");
            let decoded_offset = region.decoded_length;
            region.decoded_length = decoded_offset
                .checked_add(chunk.length)
                .expect("ASSERT: a bounded Compression Region cannot overflow");
            assert!(
                region.decoded_length <= region_limit,
                "ASSERT: a Compression Region exceeds the policy maximum"
            );
            region.members.push(RegionMember {
                logical_ordinal,
                id: chunk.id,
                decoded_offset,
                length: chunk.length,
            });
        }
        flush_region(&mut current_region, &mut regions);
        assert_eq!(
            recipe_sources.len(),
            chunks.len(),
            "ASSERT: every logical chunk has one recipe source"
        );
        Ok(IngestPlan {
            recipe_sources,
            regions,
            exact_hits,
            exact_hit_bytes,
        })
    }

    fn encode_regions(
        &self,
        input: &[u8],
        regions: &[RegionPlan],
    ) -> Result<EncodedBatch, ReductionError> {
        if regions.is_empty() {
            return Ok(EncodedBatch::default());
        }
        let budget_workers = self
            .runtime
            .maximum_inflight_bytes
            .checked_div(COMPRESSION_REGION_BYTES as usize)
            .unwrap_or(0)
            .max(1);
        let worker_count = self
            .runtime
            .workers
            .get()
            .min(budget_workers)
            .min(regions.len());
        let compression_enabled = self
            .policy
            .features
            .contains(ReductionFeatures::COMPRESSION);
        let dictionary = self.dictionary.as_ref();

        let worker_results = (0..worker_count)
            .into_par_iter()
            .map(|worker_ordinal| {
                with_worker_codec(|codec| {
                    let mut stats = WorkerStats::default();
                    let mut encoded = Vec::new();
                    for region_index in (worker_ordinal..regions.len()).step_by(worker_count) {
                        let region = regions
                            .get(region_index)
                            .expect("ASSERT: a scheduled region ordinal is in bounds");
                        let encoded_region =
                            encode_region(codec, input, region, compression_enabled, dictionary)?;
                        stats.observe(&encoded_region.record)?;
                        encoded.push(encoded_region);
                    }
                    Ok(WorkerResult { encoded, stats })
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut ordered = (0..regions.len()).map(|_| None).collect::<Vec<_>>();
        let mut aggregate = AggregateStats::default();
        for worker in worker_results {
            aggregate.merge(&worker.stats);
            for region in worker.encoded {
                let ordinal = region.ordinal;
                let slot = ordered
                    .get_mut(ordinal)
                    .expect("ASSERT: an encoded region ordinal is in bounds");
                assert!(
                    slot.replace(region).is_none(),
                    "ASSERT: every region is encoded exactly once"
                );
            }
        }
        let regions = ordered
            .into_iter()
            .map(|region| region.expect("ASSERT: every planned region completed"))
            .collect();
        Ok(EncodedBatch {
            regions,
            stats: aggregate,
        })
    }

    fn fingerprint_regions(
        &self,
        input: &[u8],
        chunks: &[LogicalChunk],
        regions: &[EncodedRegion],
    ) -> Result<FingerprintBatch, ReductionError> {
        let enabled = self.policy.features.contains(ReductionFeatures::SIMILARITY)
            || self.policy.features.contains(ReductionFeatures::REORDER);
        if !enabled || regions.is_empty() {
            return Ok(FingerprintBatch {
                values: vec![None; chunks.len()],
                workers_used: 0,
            });
        }
        let jobs = regions
            .iter()
            .map(|region| region.logical_ordinals.len())
            .sum::<usize>();
        let budget_workers = self
            .runtime
            .maximum_inflight_bytes
            .checked_div(CDC_MAX_BYTES as usize)
            .unwrap_or(0)
            .max(1);
        let worker_count = self.runtime.workers.get().min(budget_workers).min(jobs);
        let values = parallel_region_fingerprints(input, chunks, regions, worker_count)?;
        Ok(FingerprintBatch {
            values,
            workers_used: worker_count,
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the bounded reference slice keeps candidate planning, worker trials, and ordered reduction together"
    )]
    fn apply_similarity_delta(
        &self,
        input: &[u8],
        chunks: &[LogicalChunk],
        recipe_sources: &[RecipeSource],
        fingerprints: &[Option<SimilarityFingerprint>],
        regions: &mut [EncodedRegion],
    ) -> Result<SimilarityRunStats, ReductionError> {
        if !self.policy.features.contains(ReductionFeatures::SIMILARITY) {
            return Ok(SimilarityRunStats::default());
        }
        if self.similarity_index.is_empty() {
            return Ok(SimilarityRunStats::default());
        }

        let delta_enabled = self.policy.features.contains(ReductionFeatures::DELTA);
        let mut region_by_logical = vec![None; chunks.len()];
        for region in regions.iter() {
            for &logical_ordinal in &region.logical_ordinals {
                let slot = region_by_logical
                    .get_mut(logical_ordinal)
                    .expect("ASSERT: an encoded logical ordinal is in bounds");
                assert!(
                    slot.replace(region.ordinal).is_none(),
                    "ASSERT: a new logical chunk belongs to exactly one region"
                );
            }
        }

        let mut stats = SimilarityRunStats::default();
        let mut jobs = Vec::new();
        for (logical_ordinal, chunk) in chunks.iter().copied().enumerate() {
            if !matches!(recipe_sources.get(logical_ordinal), Some(RecipeSource::New)) {
                continue;
            }
            let fingerprint = fingerprints
                .get(logical_ordinal)
                .copied()
                .flatten()
                .expect("ASSERT: every new Similarity target has a fingerprint");
            let candidates = self
                .similarity_index
                .candidates(
                    chunk.id,
                    u32::try_from(chunk.length).map_err(|_| {
                        ReductionError::InvalidInput("logical chunk length does not fit u32")
                    })?,
                    fingerprint,
                    usize::from(MAXIMUM_SIMILARITY_CANDIDATES),
                )
                .map_err(similarity_writer_error)?;
            stats.candidates = stats
                .candidates
                .checked_add(candidates.len())
                .expect("ASSERT: Similarity Candidates cannot overflow for one input");
            if !delta_enabled || candidates.is_empty() {
                continue;
            }

            let region_ordinal = region_by_logical
                .get(logical_ordinal)
                .copied()
                .flatten()
                .expect("ASSERT: every new logical chunk has an encoded region");
            let region = regions
                .get(region_ordinal)
                .expect("ASSERT: a mapped encoded region ordinal is in bounds");
            // Grouping remains active when DELTA is requested. The v1 Delta
            // record can replace only a one-Chunk independent region; a
            // multi-Chunk region remains independently encoded rather than
            // silently disabling GROUPING or inventing a multi-target Delta.
            if region.logical_ordinals.as_ref() != [logical_ordinal] {
                continue;
            }
            let independent_payload_bytes = u64::try_from(region.record.encoding.payload_bytes())
                .map_err(|_| {
                ReductionError::InvalidInput("independent payload length does not fit u64")
            })?;
            let mut bases = Vec::with_capacity(candidates.len());
            for candidate in candidates {
                let candidate_length =
                    usize::try_from(candidate.logical_length()).map_err(|_| {
                        ReductionError::Corruption(
                            "Similarity Candidate length does not fit memory",
                        )
                    })?;
                let location = self
                    .independent_index
                    .lookup(candidate.chunk_id(), candidate_length)?
                    .ok_or(ReductionError::Corruption(
                        "Similarity Candidate has no independent Location",
                    ))?;
                bases.push(BaseCandidate {
                    candidate,
                    location,
                });
            }
            jobs.push(DeltaJob {
                region_ordinal,
                target_input_offset: chunk.input_offset,
                target_length: chunk.length,
                independent_payload_bytes,
                bases,
            });
        }
        if !delta_enabled || jobs.is_empty() {
            return Ok(stats);
        }

        let budget_workers = self
            .runtime
            .maximum_inflight_bytes
            .checked_div(2 * CDC_MAX_BYTES as usize)
            .unwrap_or(0)
            .max(1);
        let worker_count = self
            .runtime
            .workers
            .get()
            .min(budget_workers)
            .min(jobs.len());
        let records = &self.records;
        let dictionary = self.dictionary.as_ref();
        let zstd_prefix_enabled = self
            .policy
            .features
            .contains(ReductionFeatures::ZSTD_PREFIX);
        let worker_results = (0..worker_count)
            .into_par_iter()
            .map(|worker_ordinal| {
                with_worker_codec(|codec| {
                    let mut worker_stats = DeltaWorkerStats::default();
                    let mut decisions = Vec::new();
                    for job_index in (worker_ordinal..jobs.len()).step_by(worker_count) {
                        let job = jobs
                            .get(job_index)
                            .expect("ASSERT: a scheduled Delta job is in bounds");
                        let target_end = job
                            .target_input_offset
                            .checked_add(job.target_length)
                            .expect("ASSERT: a bounded Delta target cannot overflow");
                        let target = input
                            .get(job.target_input_offset..target_end)
                            .expect("ASSERT: a scheduled Delta target lies inside the input");
                        let target_id = ChunkId::of(target);
                        let mut best: Option<(u32, DependentEncoding)> = None;
                        let mut remaining_trials = usize::from(MAXIMUM_TRIAL_ENCODINGS);
                        for base in &job.bases {
                            if remaining_trials == 0 {
                                break;
                            }
                            worker_stats.trials = worker_stats
                                .trials
                                .checked_add(1)
                                .expect("ASSERT: Delta Trials cannot overflow for one input");
                            remaining_trials -= 1;
                            let decoded_base = decode_independent_location(
                                codec,
                                records,
                                base.candidate.chunk_id(),
                                usize::try_from(base.candidate.logical_length()).map_err(|_| {
                                    ReductionError::Corruption(
                                        "Similarity Candidate length does not fit memory",
                                    )
                                })?,
                                base.location,
                                dictionary,
                            )?;
                            let base_reference =
                                IndependentBaseRef::from_verified_bytes(&decoded_base.bytes)
                                    .map_err(similarity_writer_error)?;
                            if base_reference.chunk_id() != base.candidate.chunk_id()
                                || base_reference.logical_length()
                                    != base.candidate.logical_length()
                            {
                                return Err(ReductionError::Corruption(
                                    "verified Base disagrees with its Similarity Candidate",
                                ));
                            }
                            let trial = SparseXorDelta::encode_trial(
                                base_reference,
                                &decoded_base.bytes,
                                target,
                            )
                            .map_err(similarity_writer_error)?;
                            let trial_cost = trial.cost();
                            if trial.encoding().target_id() != target_id
                                || usize::try_from(trial_cost.target_bytes()).map_err(|_| {
                                    ReductionError::InvalidInput(
                                        "Delta Trial target length does not fit memory",
                                    )
                                })? != target.len()
                                || trial_cost.run_count() > trial_cost.xor_bytes()
                            {
                                return Err(ReductionError::Corruption(
                                    "Delta Trial cost disagrees with its byte-exact encoding",
                                ));
                            }
                            let trial_bytes = trial_cost.encoded_payload_bytes();
                            if best
                                .as_ref()
                                .is_none_or(|(best_bytes, _)| trial_bytes < *best_bytes)
                            {
                                best = Some((
                                    trial_bytes,
                                    DependentEncoding::SparseXor(trial.into_encoding()),
                                ));
                            }

                            if zstd_prefix_enabled && remaining_trials != 0 {
                                worker_stats.trials = worker_stats
                                    .trials
                                    .checked_add(1)
                                    .expect("ASSERT: Delta Trials cannot overflow for one input");
                                remaining_trials -= 1;
                                let verified_base =
                                    VerifiedBaseChunk::from_bytes(&decoded_base.bytes)
                                        .map_err(zstd_prefix_writer_error)?;
                                if verified_base.reference().chunk_id()
                                    != base.candidate.chunk_id()
                                    || verified_base.reference().logical_length()
                                        != base.candidate.logical_length()
                                {
                                    return Err(ReductionError::Corruption(
                                        "verified Prefix Base disagrees with its Similarity Candidate",
                                    ));
                                }
                                let maximum_payload_bytes = usize::try_from(
                                    job.independent_payload_bytes,
                                )
                                .map_err(|_| {
                                    ReductionError::InvalidInput(
                                        "independent payload length does not fit memory",
                                    )
                                })?;
                                if let Some(prefix) = ZstdPrefixCodec::encode_trial(
                                    verified_base,
                                    target,
                                    maximum_payload_bytes,
                                )
                                .map_err(zstd_prefix_writer_error)?
                                {
                                    if prefix.encoding().target_id() != target_id
                                        || prefix.encoding().logical_length()
                                            != u32::try_from(target.len()).map_err(|_| {
                                                ReductionError::InvalidInput(
                                                    "Prefix target length does not fit u32",
                                                )
                                            })?
                                    {
                                        return Err(ReductionError::Corruption(
                                            "Zstd Prefix Trial disagrees with its target",
                                        ));
                                    }
                                    let prefix_bytes = prefix.encoded_payload_bytes();
                                    if best.as_ref().is_none_or(|(best_bytes, _)| {
                                        prefix_bytes < *best_bytes
                                    }) {
                                        best = Some((
                                            prefix_bytes,
                                            DependentEncoding::ZstdPrefix(
                                                prefix.into_encoding(),
                                            ),
                                        ));
                                    }
                                }
                            }
                        }
                        let accepted = best.and_then(|(payload_bytes, encoding)| {
                            if accept_delta_v1(
                                job.independent_payload_bytes,
                                u64::from(payload_bytes),
                            ) {
                                Some((encoding, payload_bytes))
                            } else {
                                None
                            }
                        });
                        decisions.push(DeltaDecision {
                            region_ordinal: job.region_ordinal,
                            accepted,
                        });
                    }
                    Ok(DeltaWorkerResult {
                        decisions,
                        stats: worker_stats,
                    })
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut ordered = (0..regions.len()).map(|_| None).collect::<Vec<_>>();
        for worker in worker_results {
            stats.trials = stats
                .trials
                .checked_add(worker.stats.trials)
                .expect("ASSERT: Delta Trials cannot overflow for one input");
            for decision in worker.decisions {
                let ordinal = decision.region_ordinal;
                let slot = ordered
                    .get_mut(ordinal)
                    .expect("ASSERT: a completed Delta region ordinal is in bounds");
                assert!(
                    slot.replace(decision).is_none(),
                    "ASSERT: every Delta job completes exactly once"
                );
            }
        }
        for decision in ordered.into_iter().flatten() {
            let Some((encoding, payload_bytes)) = decision.accepted else {
                continue;
            };
            let region = regions
                .get_mut(decision.region_ordinal)
                .expect("ASSERT: an accepted Delta region ordinal is in bounds");
            let table = region.record.chunks.as_ref();
            if table.len() != 1
                || table[0].id != encoding.target_id()
                || table[0].decoded_offset != 0
                || table[0].length
                    != usize::try_from(encoding.logical_length()).map_err(|_| {
                        ReductionError::Corruption("Delta target length does not fit memory")
                    })?
            {
                return Err(ReductionError::Corruption(
                    "accepted Delta disagrees with its target record table",
                ));
            }
            region.record.encoding = RecordEncoding::Delta {
                encoding,
                payload_bytes: usize::try_from(payload_bytes).map_err(|_| {
                    ReductionError::InvalidInput("Delta payload length does not fit memory")
                })?,
            };
        }
        Ok(stats)
    }

    fn reorder_regions(
        &self,
        chunks: &[LogicalChunk],
        fingerprints: &[Option<SimilarityFingerprint>],
        regions: &mut [EncodedRegion],
    ) -> ReorderStats {
        if regions.is_empty() {
            return ReorderStats::default();
        }
        let placement_windows = placement_window_count(chunks, regions);
        if !self.policy.features.contains(ReductionFeatures::REORDER) {
            return ReorderStats {
                reordered_regions: 0,
                placement_windows,
                workers_used: 0,
            };
        }

        let budget_workers = self
            .runtime
            .maximum_inflight_bytes
            .checked_div(CDC_MAX_BYTES as usize)
            .unwrap_or(0)
            .max(1);
        let worker_count = self
            .runtime
            .workers
            .get()
            .min(budget_workers)
            .min(regions.len());
        let keys = parallel_reorder_keys(chunks, fingerprints, regions, worker_count);
        let original = regions
            .iter()
            .map(|region| region.ordinal)
            .collect::<Vec<_>>();
        regions.sort_by_key(|region| {
            keys.get(region.ordinal)
                .copied()
                .flatten()
                .expect("ASSERT: every Reorder region completed")
        });
        let reordered_regions = regions
            .iter()
            .zip(original)
            .filter(|(region, original_ordinal)| region.ordinal != *original_ordinal)
            .count();
        verify_bounded_reorder(regions, &keys);
        ReorderStats {
            reordered_regions,
            placement_windows,
            workers_used: worker_count,
        }
    }

    fn verify_exact_location(
        &self,
        id: ChunkId,
        logical_length: usize,
        location: ChunkLocation,
    ) -> Result<(), ReductionError> {
        let record = self
            .records
            .get(location.record)
            .ok_or(ReductionError::Corruption("Exact Index location is absent"))?;
        let matches = record.chunks.iter().any(|chunk| {
            chunk.id == id
                && chunk.decoded_offset == location.decoded_offset
                && chunk.length == logical_length
                && chunk.length == location.length
        });
        let end_in_bounds = location
            .decoded_offset
            .checked_add(location.length)
            .is_some_and(|end| end <= record.encoding.decoded_length());
        if !matches || !end_in_bounds {
            return Err(ReductionError::Corruption(
                "Exact Index location identity mismatch",
            ));
        }
        Ok(())
    }

    fn object(&self, object: ReducedObject) -> Result<&ArchiveObject, ReductionError> {
        let index = object
            .0
            .checked_sub(1)
            .and_then(|ordinal| usize::try_from(ordinal).ok())
            .ok_or(ReductionError::UnknownObject)?;
        self.objects.get(index).ok_or(ReductionError::UnknownObject)
    }
}

#[derive(Debug)]
struct EncodingRecord {
    encoding: RecordEncoding,
    chunks: Box<[RecordChunk]>,
}

#[derive(Debug)]
enum RecordEncoding {
    Independent(IndependentEncoding),
    Delta {
        encoding: DependentEncoding,
        payload_bytes: usize,
    },
}

#[derive(Debug)]
enum DependentEncoding {
    SparseXor(SparseXorDelta),
    ZstdPrefix(ZstdPrefixEncoding),
}

impl DependentEncoding {
    const fn base_chunk_id(&self) -> ChunkId {
        match self {
            Self::SparseXor(encoding) => encoding.base().chunk_id(),
            Self::ZstdPrefix(encoding) => encoding.base().chunk_id(),
        }
    }

    const fn base_logical_length(&self) -> u32 {
        match self {
            Self::SparseXor(encoding) => encoding.base().logical_length(),
            Self::ZstdPrefix(encoding) => encoding.base().logical_length(),
        }
    }

    const fn target_id(&self) -> ChunkId {
        match self {
            Self::SparseXor(encoding) => encoding.target_id(),
            Self::ZstdPrefix(encoding) => encoding.target_id(),
        }
    }

    const fn logical_length(&self) -> u32 {
        match self {
            Self::SparseXor(encoding) => encoding.logical_length(),
            Self::ZstdPrefix(encoding) => encoding.logical_length(),
        }
    }

    fn decode(&self, base_bytes: &[u8]) -> Result<Vec<u8>, ReductionError> {
        match self {
            Self::SparseXor(encoding) => encoding
                .decode(base_bytes)
                .map_err(|_| ReductionError::Corruption("Sparse-XOR reconstruction failed")),
            Self::ZstdPrefix(encoding) => {
                let base = VerifiedBaseChunk::from_expected(encoding.base(), base_bytes)
                    .map_err(|_| ReductionError::Corruption("Zstd Prefix Base proof failed"))?;
                encoding
                    .decode(base)
                    .map_err(|_| ReductionError::Corruption("Zstd Prefix reconstruction failed"))
            }
        }
    }

    const fn is_zstd_prefix(&self) -> bool {
        matches!(self, Self::ZstdPrefix(_))
    }
}

impl RecordEncoding {
    #[must_use]
    const fn decoded_length(&self) -> usize {
        match self {
            Self::Independent(encoding) => encoding.decoded_length(),
            Self::Delta { encoding, .. } => encoding.logical_length() as usize,
        }
    }

    #[must_use]
    fn payload_bytes(&self) -> usize {
        match self {
            Self::Independent(encoding) => encoding.payload().len(),
            Self::Delta { payload_bytes, .. } => *payload_bytes,
        }
    }

    #[must_use]
    const fn is_independent(&self) -> bool {
        matches!(self, Self::Independent(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecordChunk {
    id: ChunkId,
    decoded_offset: usize,
    length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ChunkLocation {
    record: usize,
    decoded_offset: usize,
    length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DataRecipeEntry {
    id: ChunkId,
    location: ChunkLocation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecipeEntry {
    Data(DataRecipeEntry),
    Fill { byte: u8, length: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LogicalChunk {
    id: ChunkId,
    input_offset: usize,
    length: usize,
}

#[derive(Debug)]
struct ChunkedInput {
    chunks: Vec<LogicalChunk>,
    layout: Vec<InputExtent>,
    workers_used: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputExtent {
    Data(usize),
    Fill { byte: u8, length: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputSegment {
    Data { input_offset: usize, length: usize },
    Fill { byte: u8, length: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecipeSource {
    Existing(ChunkLocation),
    New,
    PendingExact(usize),
}

#[derive(Debug)]
struct IngestPlan {
    recipe_sources: Vec<RecipeSource>,
    regions: Vec<RegionPlan>,
    exact_hits: usize,
    exact_hit_bytes: u64,
}

#[derive(Debug)]
struct RegionPlan {
    ordinal: usize,
    input_offset: usize,
    decoded_length: usize,
    members: Vec<RegionMember>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegionMember {
    logical_ordinal: usize,
    id: ChunkId,
    decoded_offset: usize,
    length: usize,
}

#[derive(Debug)]
struct EncodedRegion {
    ordinal: usize,
    record: EncodingRecord,
    logical_ordinals: Box<[usize]>,
}

#[derive(Debug)]
struct DeltaJob {
    region_ordinal: usize,
    target_input_offset: usize,
    target_length: usize,
    independent_payload_bytes: u64,
    bases: Vec<BaseCandidate>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BaseCandidate {
    candidate: SimilarityCandidate,
    location: ChunkLocation,
}

#[derive(Debug)]
struct DeltaDecision {
    region_ordinal: usize,
    accepted: Option<(DependentEncoding, u32)>,
}

#[derive(Debug)]
struct DeltaWorkerResult {
    decisions: Vec<DeltaDecision>,
    stats: DeltaWorkerStats,
}

#[repr(align(64))]
#[derive(Debug, Default)]
struct DeltaWorkerStats {
    trials: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SimilarityRunStats {
    candidates: usize,
    trials: usize,
}

#[derive(Debug)]
struct FingerprintBatch {
    values: Vec<Option<SimilarityFingerprint>>,
    workers_used: usize,
}

fn parallel_chunk_ids(
    input: &[u8],
    ranges: &[(usize, usize)],
    worker_count: usize,
) -> Vec<ChunkId> {
    if ranges.is_empty() {
        assert_eq!(
            worker_count, 0,
            "ASSERT: an empty Chunk batch schedules no workers"
        );
        return Vec::new();
    }
    assert!(
        (1..=ranges.len()).contains(&worker_count),
        "ASSERT: Chunk hashing schedules one or more nonempty workers"
    );
    if worker_count == 1 {
        return hash_chunk_shard(input, ranges, 0, ranges.len())
            .into_iter()
            .map(|(_, id)| id)
            .collect();
    }

    let worker_results = (0..worker_count)
        .into_par_iter()
        .map(|worker_ordinal| {
            let (start, end) = contiguous_shard(ranges.len(), worker_count, worker_ordinal);
            hash_chunk_shard(input, ranges, start, end)
        })
        .collect::<Vec<_>>();

    let mut ordered = vec![None; ranges.len()];
    for worker in worker_results {
        for (ordinal, id) in worker {
            let slot = ordered
                .get_mut(ordinal)
                .expect("ASSERT: a hashed Chunk ordinal is in bounds");
            assert!(
                slot.replace(id).is_none(),
                "ASSERT: every logical Chunk is hashed exactly once"
            );
        }
    }
    ordered
        .into_iter()
        .map(|id| id.expect("ASSERT: every logical Chunk hash completed"))
        .collect()
}

fn hash_chunk_shard(
    input: &[u8],
    ranges: &[(usize, usize)],
    start: usize,
    end: usize,
) -> Vec<(usize, ChunkId)> {
    assert!(
        start < end && end <= ranges.len(),
        "ASSERT: every Chunk hash shard is nonempty and in bounds"
    );
    let mut hashed = Vec::with_capacity(end - start);
    for ordinal in start..end {
        let &(input_offset, length) = ranges
            .get(ordinal)
            .expect("ASSERT: a scheduled Chunk hash range is in bounds");
        let input_end = input_offset
            .checked_add(length)
            .expect("ASSERT: a bounded Chunk hash range cannot overflow");
        let bytes = input
            .get(input_offset..input_end)
            .expect("ASSERT: a scheduled Chunk hash range lies within the input");
        hashed.push((ordinal, ChunkId::of(bytes)));
    }
    hashed
}

fn contiguous_shard(jobs: usize, workers: usize, worker: usize) -> (usize, usize) {
    assert!(
        jobs >= workers && worker < workers,
        "ASSERT: contiguous sharding assigns at least one job per worker"
    );
    let base = jobs / workers;
    let extra = jobs % workers;
    let start = worker
        .checked_mul(base)
        .and_then(|offset| offset.checked_add(worker.min(extra)))
        .expect("ASSERT: a bounded contiguous shard start cannot overflow");
    let length = base + usize::from(worker < extra);
    let end = start
        .checked_add(length)
        .expect("ASSERT: a bounded contiguous shard end cannot overflow");
    assert!(
        start < end && end <= jobs,
        "ASSERT: a contiguous shard is nonempty and in bounds"
    );
    (start, end)
}

fn parallel_region_fingerprints(
    input: &[u8],
    chunks: &[LogicalChunk],
    regions: &[EncodedRegion],
    worker_count: usize,
) -> Result<Vec<Option<SimilarityFingerprint>>, ReductionError> {
    assert!(
        worker_count > 0,
        "ASSERT: Similarity fingerprinting has at least one worker"
    );
    let jobs = regions
        .iter()
        .flat_map(|region| region.logical_ordinals.iter().copied())
        .collect::<Vec<_>>();
    let worker_results = (0..worker_count)
        .into_par_iter()
        .map(|worker_ordinal| {
            let mut fingerprints = Vec::new();
            for job_index in (worker_ordinal..jobs.len()).step_by(worker_count) {
                let logical_ordinal = *jobs
                    .get(job_index)
                    .expect("ASSERT: a scheduled fingerprint job is in bounds");
                let chunk = chunks
                    .get(logical_ordinal)
                    .expect("ASSERT: a fingerprint Chunk ordinal is in bounds");
                let end = chunk
                    .input_offset
                    .checked_add(chunk.length)
                    .expect("ASSERT: a bounded fingerprint range cannot overflow");
                let bytes = input
                    .get(chunk.input_offset..end)
                    .expect("ASSERT: a fingerprint Chunk lies within the input");
                let fingerprint =
                    SimilarityFingerprint::v1(bytes).map_err(similarity_writer_error)?;
                fingerprints.push((logical_ordinal, fingerprint));
            }
            Ok::<_, ReductionError>(fingerprints)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut fingerprints = vec![None; chunks.len()];
    for worker in worker_results {
        for (logical_ordinal, fingerprint) in worker {
            let slot = fingerprints
                .get_mut(logical_ordinal)
                .expect("ASSERT: a completed fingerprint ordinal is in bounds");
            assert!(
                slot.replace(fingerprint).is_none(),
                "ASSERT: every new logical Chunk is fingerprinted exactly once"
            );
        }
    }
    Ok(fingerprints)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReorderStats {
    reordered_regions: usize,
    placement_windows: usize,
    workers_used: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReorderKey {
    placement_window: usize,
    superfeatures: [u64; 4],
    sketch: [u64; 8],
    original_ordinal: usize,
}

fn placement_window_count(chunks: &[LogicalChunk], regions: &[EncodedRegion]) -> usize {
    regions
        .iter()
        .map(|region| {
            let ordinal = *region
                .logical_ordinals
                .first()
                .expect("ASSERT: every Encoding Region has a logical Chunk");
            chunks
                .get(ordinal)
                .expect("ASSERT: a Placement Window Chunk ordinal is in bounds")
                .input_offset
                / PLACEMENT_WINDOW_BYTES as usize
        })
        .collect::<BTreeSet<_>>()
        .len()
}

fn parallel_reorder_keys(
    chunks: &[LogicalChunk],
    fingerprints: &[Option<SimilarityFingerprint>],
    regions: &[EncodedRegion],
    worker_count: usize,
) -> Vec<Option<ReorderKey>> {
    assert!(worker_count > 0, "ASSERT: Reorder has at least one worker");
    let worker_keys = (0..worker_count)
        .into_par_iter()
        .map(|worker_ordinal| {
            let mut keys = Vec::new();
            for region_index in (worker_ordinal..regions.len()).step_by(worker_count) {
                let region = regions
                    .get(region_index)
                    .expect("ASSERT: a scheduled Reorder region is in bounds");
                keys.push((region.ordinal, reorder_key(chunks, fingerprints, region)));
            }
            keys
        })
        .collect::<Vec<_>>();

    let mut keys = vec![None; regions.len()];
    for worker in worker_keys {
        for (ordinal, key) in worker {
            let slot = keys
                .get_mut(ordinal)
                .expect("ASSERT: a Reorder key ordinal is in bounds");
            assert!(
                slot.replace(key).is_none(),
                "ASSERT: every Reorder region has exactly one key"
            );
        }
    }
    keys
}

fn reorder_key(
    chunks: &[LogicalChunk],
    fingerprints: &[Option<SimilarityFingerprint>],
    region: &EncodedRegion,
) -> ReorderKey {
    let logical_ordinal = *region
        .logical_ordinals
        .first()
        .expect("ASSERT: every Encoding Region has a logical Chunk");
    let chunk = chunks
        .get(logical_ordinal)
        .expect("ASSERT: a Reorder logical Chunk ordinal is in bounds");
    let fingerprint = fingerprints
        .get(logical_ordinal)
        .copied()
        .flatten()
        .expect("ASSERT: every Reorder target has a fingerprint");
    let (superfeatures, sketch) = fingerprint.placement_key();
    let placement_window = chunk.input_offset / PLACEMENT_WINDOW_BYTES as usize;
    for &member in &region.logical_ordinals {
        let member_chunk = chunks
            .get(member)
            .expect("ASSERT: a Reorder region member ordinal is in bounds");
        assert_eq!(
            member_chunk.input_offset / PLACEMENT_WINDOW_BYTES as usize,
            placement_window,
            "ASSERT: one Encoding Region cannot cross Placement Windows"
        );
    }
    ReorderKey {
        placement_window,
        superfeatures,
        sketch,
        original_ordinal: region.ordinal,
    }
}

fn verify_bounded_reorder(regions: &[EncodedRegion], keys: &[Option<ReorderKey>]) {
    let mut previous_window = None;
    for region in regions {
        let window = keys[region.ordinal]
            .expect("ASSERT: a sorted Reorder region retains its key")
            .placement_window;
        assert!(
            previous_window.is_none_or(|previous| previous <= window),
            "ASSERT: Reorder never moves a record across Placement Windows"
        );
        previous_window = Some(window);
    }
}

#[derive(Debug)]
struct DecodedIndependent {
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct WorkerResult {
    encoded: Vec<EncodedRegion>,
    stats: WorkerStats,
}

#[repr(align(64))]
#[derive(Debug, Default)]
struct WorkerStats {
    regions: usize,
    raw_chunks: usize,
    zstd_regions: usize,
    zstd_dictionary_regions: usize,
    delta_chunks: usize,
    zstd_prefix_chunks: usize,
    delta_logical_bytes: u64,
    delta_payload_bytes: u64,
    physical_payload_bytes: u64,
    maximum_region_decoded_bytes: usize,
}

impl WorkerStats {
    fn observe(&mut self, record: &EncodingRecord) -> Result<(), ReductionError> {
        self.regions = self
            .regions
            .checked_add(1)
            .expect("ASSERT: worker region count cannot exceed planned regions");
        self.maximum_region_decoded_bytes = self
            .maximum_region_decoded_bytes
            .max(record.encoding.decoded_length());
        self.physical_payload_bytes =
            self.physical_payload_bytes
                .checked_add(u64::try_from(record.encoding.payload_bytes()).map_err(|_| {
                    ReductionError::InvalidInput("encoded payload does not fit u64")
                })?)
                .expect("ASSERT: encoded payload bytes cannot overflow for one input");
        match &record.encoding {
            RecordEncoding::Independent(IndependentEncoding::Raw { .. }) => {
                self.raw_chunks = self
                    .raw_chunks
                    .checked_add(record.chunks.len())
                    .expect("ASSERT: RAW chunk count cannot exceed logical chunks");
            }
            RecordEncoding::Independent(IndependentEncoding::Zstd { dictionary_id, .. }) => {
                self.zstd_regions = self
                    .zstd_regions
                    .checked_add(1)
                    .expect("ASSERT: Zstd region count cannot exceed regions");
                if dictionary_id.is_some() {
                    self.zstd_dictionary_regions = self
                        .zstd_dictionary_regions
                        .checked_add(1)
                        .expect("ASSERT: dictionary regions cannot exceed Zstd regions");
                }
            }
            RecordEncoding::Delta {
                encoding,
                payload_bytes,
            } => {
                assert_eq!(
                    record.chunks.len(),
                    1,
                    "ASSERT: a v1 Delta record has exactly one target Chunk"
                );
                self.delta_chunks = self
                    .delta_chunks
                    .checked_add(1)
                    .expect("ASSERT: Delta chunks cannot exceed logical chunks");
                if encoding.is_zstd_prefix() {
                    self.zstd_prefix_chunks = self
                        .zstd_prefix_chunks
                        .checked_add(1)
                        .expect("ASSERT: Prefix chunks cannot exceed Delta chunks");
                }
                self.delta_logical_bytes = self
                    .delta_logical_bytes
                    .checked_add(
                        u64::try_from(record.encoding.decoded_length())
                            .expect("ASSERT: a bounded Delta length fits u64"),
                    )
                    .expect("ASSERT: Delta logical bytes cannot exceed logical bytes");
                self.delta_payload_bytes = self
                    .delta_payload_bytes
                    .checked_add(
                        u64::try_from(*payload_bytes)
                            .expect("ASSERT: a bounded Delta payload fits u64"),
                    )
                    .expect("ASSERT: Delta payload bytes cannot overflow for one input");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct AggregateStats {
    raw_chunks: usize,
    zstd_regions: usize,
    zstd_dictionary_regions: usize,
    delta_chunks: usize,
    zstd_prefix_chunks: usize,
    delta_logical_bytes: u64,
    delta_payload_bytes: u64,
    physical_payload_bytes: u64,
    maximum_region_decoded_bytes: usize,
    workers_used: usize,
}

impl AggregateStats {
    fn merge(&mut self, worker: &WorkerStats) {
        self.raw_chunks = self
            .raw_chunks
            .checked_add(worker.raw_chunks)
            .expect("ASSERT: RAW chunks cannot exceed logical chunks");
        self.zstd_regions = self
            .zstd_regions
            .checked_add(worker.zstd_regions)
            .expect("ASSERT: Zstd regions cannot exceed planned regions");
        self.zstd_dictionary_regions = self
            .zstd_dictionary_regions
            .checked_add(worker.zstd_dictionary_regions)
            .expect("ASSERT: dictionary regions cannot exceed Zstd regions");
        self.delta_chunks = self
            .delta_chunks
            .checked_add(worker.delta_chunks)
            .expect("ASSERT: Delta chunks cannot exceed logical chunks");
        self.zstd_prefix_chunks = self
            .zstd_prefix_chunks
            .checked_add(worker.zstd_prefix_chunks)
            .expect("ASSERT: Prefix chunks cannot exceed Delta chunks");
        self.delta_logical_bytes = self
            .delta_logical_bytes
            .checked_add(worker.delta_logical_bytes)
            .expect("ASSERT: Delta logical bytes cannot exceed logical bytes");
        self.delta_payload_bytes = self
            .delta_payload_bytes
            .checked_add(worker.delta_payload_bytes)
            .expect("ASSERT: Delta payload bytes cannot overflow for one input");
        self.physical_payload_bytes = self
            .physical_payload_bytes
            .checked_add(worker.physical_payload_bytes)
            .expect("ASSERT: physical payload bytes cannot overflow for one input");
        self.maximum_region_decoded_bytes = self
            .maximum_region_decoded_bytes
            .max(worker.maximum_region_decoded_bytes);
        if worker.regions > 0 {
            self.workers_used = self
                .workers_used
                .checked_add(1)
                .expect("ASSERT: workers used cannot exceed configured workers");
        }
    }
}

#[derive(Debug, Default)]
struct EncodedBatch {
    regions: Vec<EncodedRegion>,
    stats: AggregateStats,
}

fn split_fill_segments(input: &[u8]) -> Vec<InputSegment> {
    if input.is_empty() {
        return Vec::new();
    }

    let mut segments = Vec::new();
    let mut data_start = 0_usize;
    let mut run_start = 0_usize;
    for cursor in 1..=input.len() {
        let run_ended = cursor == input.len() || input[cursor] != input[run_start];
        if !run_ended {
            continue;
        }
        let run_length = cursor
            .checked_sub(run_start)
            .expect("ASSERT: a forward scan run length cannot underflow");
        if run_length >= FILL_MINIMUM_BYTES {
            if data_start < run_start {
                segments.push(InputSegment::Data {
                    input_offset: data_start,
                    length: run_start
                        .checked_sub(data_start)
                        .expect("ASSERT: a forward DATA segment cannot underflow"),
                });
            }
            segments.push(InputSegment::Fill {
                byte: input[run_start],
                length: run_length,
            });
            data_start = cursor;
        }
        run_start = cursor;
    }
    if data_start < input.len() {
        segments.push(InputSegment::Data {
            input_offset: data_start,
            length: input
                .len()
                .checked_sub(data_start)
                .expect("ASSERT: a trailing DATA segment cannot underflow"),
        });
    }

    let covered = segments.iter().fold(0_usize, |covered, segment| {
        let length = match segment {
            InputSegment::Data { length, .. } | InputSegment::Fill { length, .. } => *length,
        };
        covered
            .checked_add(length)
            .expect("ASSERT: segment coverage cannot exceed the input")
    });
    assert_eq!(
        covered,
        input.len(),
        "ASSERT: DATA and FILL segments partition the complete input"
    );
    segments
}

fn summarize_encoded(
    regions: &[EncodedRegion],
    workers_used: usize,
) -> Result<AggregateStats, ReductionError> {
    let mut local = WorkerStats::default();
    for region in regions {
        local.observe(&region.record)?;
    }
    let mut aggregate = AggregateStats::default();
    aggregate.merge(&local);
    aggregate.workers_used = workers_used;
    Ok(aggregate)
}

fn flush_region(current: &mut Option<RegionPlan>, regions: &mut Vec<RegionPlan>) {
    if let Some(region) = current.take() {
        assert!(!region.members.is_empty(), "ASSERT: a region is nonempty");
        assert_eq!(
            region.ordinal,
            regions.len(),
            "ASSERT: region ordinals are contiguous"
        );
        regions.push(region);
    }
}

fn encode_independent(
    codec: &mut WorkerCodec,
    decoded: &[u8],
    decoded_length: usize,
    compression_enabled: bool,
    dictionary: Option<&PreparedDictionary>,
) -> Result<IndependentEncoding, ReductionError> {
    if !compression_enabled {
        return Ok(IndependentEncoding::Raw {
            payload: decoded.to_vec().into_boxed_slice(),
            decoded_length,
        });
    }

    let plain_decision = codec
        .encode_v1(decoded, decoded_length, ZSTD_LEVEL_V1, None)
        .map_err(|error| codec_error(&error))?;
    let plain = plain_decision.into_encoding();
    let Some(dictionary) = dictionary else {
        return Ok(plain);
    };

    let dictionary_decision = codec
        .encode_v1(decoded, decoded_length, ZSTD_LEVEL_V1, Some(dictionary))
        .map_err(|error| codec_error(&error))?;
    let complete_costs = dictionary_decision
        .payload_costs()
        .with_metadata(0, DICTIONARY_DEPENDENCY_BYTES);
    let raw_cost = complete_costs.raw();
    let zstd_cost = complete_costs.zstd();
    let raw_complete = raw_cost
        .payload_bytes()
        .checked_add(raw_cost.metadata_bytes())
        .expect("ASSERT: a bounded RAW encoding cost cannot overflow");
    let zstd_complete = zstd_cost
        .payload_bytes()
        .checked_add(zstd_cost.metadata_bytes())
        .expect("ASSERT: a bounded dictionary encoding cost cannot overflow");
    assert!(
        raw_complete
            >= u64::try_from(decoded_length).expect("ASSERT: a bounded decoded length fits u64"),
        "ASSERT: complete RAW cost covers its payload"
    );
    let dictionary_is_zstd = matches!(
        dictionary_decision.encoding(),
        IndependentEncoding::Zstd {
            dictionary_id: Some(_),
            ..
        }
    );
    if !dictionary_is_zstd
        || !accept_zstd_v1(complete_costs).map_err(|error| codec_error(&error))?
    {
        return Ok(plain);
    }
    let dictionary_encoding = dictionary_decision.into_encoding();
    let plain_bytes = complete_independent_bytes(&plain)?;
    if zstd_complete < plain_bytes {
        Ok(dictionary_encoding)
    } else {
        Ok(plain)
    }
}

fn complete_independent_bytes(encoding: &IndependentEncoding) -> Result<u64, ReductionError> {
    let payload = u64::try_from(encoding.payload().len())
        .map_err(|_| ReductionError::InvalidInput("encoded payload length does not fit u64"))?;
    if encoding.dictionary_id().is_some() {
        payload
            .checked_add(DICTIONARY_DEPENDENCY_BYTES)
            .ok_or(ReductionError::InvalidInput(
                "complete dictionary encoding cost overflows u64",
            ))
    } else {
        Ok(payload)
    }
}

fn dictionary_for_encoding<'a>(
    encoding: &IndependentEncoding,
    available: Option<&'a PreparedDictionary>,
) -> Result<Option<&'a PreparedDictionary>, ReductionError> {
    let Some(expected) = encoding.dictionary_id() else {
        return Ok(None);
    };
    let provided = available.ok_or(ReductionError::Corruption(
        "record requires an unavailable dictionary",
    ))?;
    if expected.bytes() != provided.id().bytes() {
        return Err(ReductionError::Corruption(
            "record dictionary identity mismatch",
        ));
    }
    Ok(Some(provided))
}

fn encode_region(
    codec: &mut WorkerCodec,
    input: &[u8],
    region: &RegionPlan,
    compression_enabled: bool,
    dictionary: Option<&PreparedDictionary>,
) -> Result<EncodedRegion, ReductionError> {
    let input_end = region
        .input_offset
        .checked_add(region.decoded_length)
        .expect("ASSERT: a bounded region range cannot overflow");
    let decoded = input
        .get(region.input_offset..input_end)
        .expect("ASSERT: a planned region lies within the input");
    let encoding = encode_independent(
        codec,
        decoded,
        region.decoded_length,
        compression_enabled,
        dictionary,
    )?;
    let chunks = region
        .members
        .iter()
        .map(|member| RecordChunk {
            id: member.id,
            decoded_offset: member.decoded_offset,
            length: member.length,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let record = EncodingRecord {
        encoding: RecordEncoding::Independent(encoding),
        chunks,
    };

    let RecordEncoding::Independent(independent) = &record.encoding else {
        unreachable!("ASSERT: encode_region creates an independent Encoding Record")
    };
    let decode_dictionary = dictionary_for_encoding(independent, dictionary)?;
    let self_checked = codec
        .decode(independent, region.decoded_length, decode_dictionary)
        .map_err(|error| codec_error(&error))?;
    if self_checked != decoded {
        return Err(ReductionError::Corruption(
            "writer codec self-check did not reproduce the region",
        ));
    }
    verify_decoded_record(&record, &self_checked)?;
    Ok(EncodedRegion {
        ordinal: region.ordinal,
        record,
        logical_ordinals: region
            .members
            .iter()
            .map(|member| member.logical_ordinal)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    })
}

fn verify_decoded_record(record: &EncodingRecord, decoded: &[u8]) -> Result<(), ReductionError> {
    if decoded.len() != record.encoding.decoded_length() {
        return Err(ReductionError::Corruption(
            "record decoded length disagrees with its encoding",
        ));
    }
    let mut expected_offset = 0_usize;
    for chunk in &record.chunks {
        if chunk.length == 0 || chunk.decoded_offset != expected_offset {
            return Err(ReductionError::Corruption(
                "record chunk table is not a contiguous nonempty partition",
            ));
        }
        let end = chunk
            .decoded_offset
            .checked_add(chunk.length)
            .ok_or(ReductionError::Corruption("record chunk slice overflows"))?;
        let bytes = decoded
            .get(chunk.decoded_offset..end)
            .ok_or(ReductionError::Corruption(
                "record chunk slice lies outside decoded bytes",
            ))?;
        if ChunkId::of(bytes) != chunk.id {
            return Err(ReductionError::Corruption(
                "record chunk table Chunk ID mismatch",
            ));
        }
        expected_offset = end;
    }
    if expected_offset != decoded.len() {
        return Err(ReductionError::Corruption(
            "record chunk table does not cover all decoded bytes",
        ));
    }
    Ok(())
}

fn verify_index_location(
    records: &[EncodingRecord],
    expected_id: ChunkId,
    expected_length: usize,
    location: ChunkLocation,
    require_independent: bool,
) -> Result<(), ReductionError> {
    if expected_length == 0 || location.length != expected_length {
        return Err(ReductionError::Corruption(
            "Index Location has an invalid logical length",
        ));
    }
    let record = records
        .get(location.record)
        .ok_or(ReductionError::Corruption(
            "Index Location record is absent",
        ))?;
    if require_independent && !record.encoding.is_independent() {
        return Err(ReductionError::Corruption(
            "independent Base Index names a dependent record",
        ));
    }
    if !record.chunks.iter().any(|chunk| {
        chunk.id == expected_id
            && chunk.decoded_offset == location.decoded_offset
            && chunk.length == expected_length
    }) {
        return Err(ReductionError::Corruption(
            "Index Location disagrees with its record Chunk table",
        ));
    }
    Ok(())
}

fn decode_independent_location(
    codec: &mut WorkerCodec,
    records: &[EncodingRecord],
    expected_id: ChunkId,
    expected_length: usize,
    location: ChunkLocation,
    dictionary: Option<&PreparedDictionary>,
) -> Result<DecodedIndependent, ReductionError> {
    let record = records
        .get(location.record)
        .ok_or(ReductionError::Corruption(
            "independent Base Location record is absent",
        ))?;
    let RecordEncoding::Independent(encoding) = &record.encoding else {
        return Err(ReductionError::Corruption(
            "a Delta Base Location names a dependent record",
        ));
    };
    let decode_dictionary = dictionary_for_encoding(encoding, dictionary)?;
    let decoded = codec
        .decode(encoding, encoding.decoded_length(), decode_dictionary)
        .map_err(|_| ReductionError::Corruption("independent Base codec decode failed"))?;
    verify_decoded_record(record, &decoded)?;
    let table_entry = record
        .chunks
        .iter()
        .find(|chunk| {
            chunk.id == expected_id
                && chunk.decoded_offset == location.decoded_offset
                && chunk.length == expected_length
                && chunk.length == location.length
        })
        .ok_or(ReductionError::Corruption(
            "independent Base Location identity mismatch",
        ))?;
    let end = table_entry
        .decoded_offset
        .checked_add(table_entry.length)
        .ok_or(ReductionError::Corruption(
            "independent Base decoded slice overflows",
        ))?;
    let bytes = decoded
        .get(table_entry.decoded_offset..end)
        .ok_or(ReductionError::Corruption(
            "independent Base decoded slice lies outside its record",
        ))?
        .to_vec();
    if ChunkId::of(&bytes) != expected_id {
        return Err(ReductionError::Corruption(
            "independent Base Chunk ID mismatch",
        ));
    }
    Ok(DecodedIndependent { bytes })
}

fn accept_delta_v1(independent_bytes: u64, delta_bytes: u64) -> bool {
    let Some(savings) = independent_bytes.checked_sub(delta_bytes) else {
        return false;
    };
    if savings < DELTA_MINIMUM_SAVINGS_BYTES {
        return false;
    }
    u128::from(savings) * PERCENT_DENOMINATOR
        >= u128::from(independent_bytes) * DELTA_MINIMUM_SAVINGS_PERCENT
}

fn similarity_writer_error(error: SimilarityError) -> ReductionError {
    if matches!(
        error,
        SimilarityError::ChunkIdentityLengthMismatch
            | SimilarityError::FingerprintMismatch
            | SimilarityError::IndexCorruption
            | SimilarityError::BaseLengthMismatch
            | SimilarityError::BaseIdentityMismatch
    ) {
        return ReductionError::Corruption("Similarity Index or Base identity mismatch");
    }
    ReductionError::Similarity(error.to_string())
}

fn zstd_prefix_writer_error(error: crate::ZstdPrefixError) -> ReductionError {
    if matches!(
        error,
        crate::ZstdPrefixError::BaseLengthMismatch
            | crate::ZstdPrefixError::BaseIdentityMismatch
            | crate::ZstdPrefixError::TargetIdentityMismatch
    ) {
        return ReductionError::Corruption("Zstd Prefix identity mismatch");
    }
    ReductionError::Codec(error.to_string())
}

fn codec_error(error: &crate::reduction_codec::CodecError) -> ReductionError {
    ReductionError::Codec(error.to_string())
}

#[derive(Debug)]
struct IndependentIndex {
    locations: BTreeMap<(ChunkId, usize), ChunkLocation>,
    logical_lengths: BTreeMap<ChunkId, usize>,
}

impl IndependentIndex {
    const fn new() -> Self {
        Self {
            locations: BTreeMap::new(),
            logical_lengths: BTreeMap::new(),
        }
    }

    fn lookup(
        &self,
        id: ChunkId,
        logical_length: usize,
    ) -> Result<Option<ChunkLocation>, ReductionError> {
        if self
            .logical_lengths
            .get(&id)
            .is_some_and(|stored_length| *stored_length != logical_length)
        {
            return Err(ReductionError::Corruption(
                "one independent Base Chunk ID has conflicting logical lengths",
            ));
        }
        Ok(self.locations.get(&(id, logical_length)).copied())
    }

    fn insert(
        &mut self,
        id: ChunkId,
        logical_length: usize,
        location: ChunkLocation,
    ) -> Result<(), ReductionError> {
        if self.lookup(id, logical_length)?.is_some() {
            return Ok(());
        }
        let previous_length = self.logical_lengths.insert(id, logical_length);
        assert!(
            previous_length.is_none(),
            "ASSERT: a new independent Base Chunk ID has no prior length"
        );
        let previous_location = self.locations.insert((id, logical_length), location);
        assert!(
            previous_location.is_none(),
            "ASSERT: a new independent Base key has no prior Location"
        );
        Ok(())
    }

    fn audit(&self, records: &[EncodingRecord]) -> Result<(), ReductionError> {
        if self.locations.len() != self.logical_lengths.len() {
            return Err(ReductionError::Corruption(
                "independent Base Index key maps disagree",
            ));
        }
        for (&(id, logical_length), &location) in &self.locations {
            if self.logical_lengths.get(&id) != Some(&logical_length) {
                return Err(ReductionError::Corruption(
                    "independent Base Index length map disagrees",
                ));
            }
            verify_index_location(records, id, logical_length, location, true)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ExactIndex {
    locations: BTreeMap<(ChunkId, usize), ChunkLocation>,
    logical_lengths: BTreeMap<ChunkId, usize>,
    bloom: Option<BlockedBloomHint>,
}

impl ExactIndex {
    const fn new() -> Self {
        Self {
            locations: BTreeMap::new(),
            logical_lengths: BTreeMap::new(),
            bloom: None,
        }
    }

    fn lookup(
        &self,
        id: ChunkId,
        logical_length: usize,
    ) -> Result<Option<ChunkLocation>, ReductionError> {
        if self
            .logical_lengths
            .get(&id)
            .is_some_and(|stored_length| *stored_length != logical_length)
        {
            return Err(ReductionError::Corruption(
                "one Chunk ID has conflicting logical lengths",
            ));
        }
        if self.bloom.as_ref().is_some_and(|bloom| {
            bloom.probe_for_exact_lookup(id, logical_length) == BloomLookupHint::DefinitelyAbsent
        }) {
            return Ok(None);
        }
        Ok(self.locations.get(&(id, logical_length)).copied())
    }

    fn insert(
        &mut self,
        id: ChunkId,
        logical_length: usize,
        location: ChunkLocation,
    ) -> Result<(), ReductionError> {
        if self.lookup(id, logical_length)?.is_some() {
            return Err(ReductionError::Corruption(
                "Exact Index attempted to replace an existing location",
            ));
        }
        if self.bloom.is_none() {
            self.bloom = Some(
                BlockedBloomHint::new(EXACT_BLOOM_EXPECTED_KEYS, EXACT_BLOOM_MAXIMUM_BYTES)
                    .map_err(|error| ReductionError::Resource(error.to_string()))?,
            );
        }
        let previous_length = self.logical_lengths.insert(id, logical_length);
        assert!(
            previous_length.is_none(),
            "ASSERT: a verified new Chunk ID has no prior length"
        );
        let previous_location = self.locations.insert((id, logical_length), location);
        assert!(
            previous_location.is_none(),
            "ASSERT: a verified new Exact key has no prior location"
        );
        self.bloom
            .as_mut()
            .expect("ASSERT: the Exact Bloom is allocated before index mutation")
            .insert_hint(id, logical_length);
        Ok(())
    }

    fn audit(&self, records: &[EncodingRecord]) -> Result<(), ReductionError> {
        if self.locations.len() != self.logical_lengths.len() {
            return Err(ReductionError::Corruption("Exact Index key maps disagree"));
        }
        for (&(id, logical_length), &location) in &self.locations {
            if self.logical_lengths.get(&id) != Some(&logical_length) {
                return Err(ReductionError::Corruption(
                    "Exact Index length map disagrees",
                ));
            }
            let bloom = self.bloom.as_ref().ok_or(ReductionError::Corruption(
                "nonempty Exact Index has no Bloom acceleration",
            ))?;
            if bloom.probe_for_exact_lookup(id, logical_length) == BloomLookupHint::DefinitelyAbsent
            {
                return Err(ReductionError::Corruption(
                    "Exact Bloom has a false negative for an indexed Chunk",
                ));
            }
            verify_index_location(records, id, logical_length, location, false)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ArchiveObject {
    recipe: Vec<RecipeEntry>,
    report: ReductionReport,
}

#[derive(Debug)]
pub enum ReductionError {
    InvalidPolicy(&'static str),
    InvalidRuntime(&'static str),
    InvalidInput(&'static str),
    Unsupported(&'static str),
    UnknownObject,
    Corruption(&'static str),
    Codec(String),
    Similarity(String),
    Resource(String),
}

impl fmt::Display for ReductionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy(message) => {
                write!(formatter, "invalid reduction policy: {message}")
            }
            Self::InvalidRuntime(message) => {
                write!(formatter, "invalid reduction runtime: {message}")
            }
            Self::InvalidInput(message) => write!(formatter, "invalid reduction input: {message}"),
            Self::Unsupported(message) => {
                write!(formatter, "unsupported reduction path: {message}")
            }
            Self::UnknownObject => formatter.write_str("unknown reduced object"),
            Self::Corruption(message) => {
                write!(formatter, "reduction archive corruption: {message}")
            }
            Self::Codec(message) => write!(formatter, "reduction codec failed: {message}"),
            Self::Similarity(message) => {
                write!(formatter, "reduction similarity failed: {message}")
            }
            Self::Resource(message) => {
                write!(formatter, "reduction resource allocation failed: {message}")
            }
        }
    }
}

impl std::error::Error for ReductionError {}
