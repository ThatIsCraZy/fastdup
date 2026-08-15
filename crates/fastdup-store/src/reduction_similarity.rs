use std::collections::BTreeMap;
use std::fmt;

use fastdup_format::ChunkId;

pub(crate) const SIMILARITY_PROFILE_V1: u16 = 1;
pub(crate) const MAX_SIMILARITY_CANDIDATES: usize = 16;
const SIMILARITY_BUCKET_PROFILE_V1: u16 = 1;
const MAX_BUCKET_REPRESENTATIVES_V1: usize = 64;
const SUPERFEATURE_SLOTS_V1: usize = 4;
const MAX_QUERY_REPRESENTATIVES_EXAMINED_V1: usize =
    SUPERFEATURE_SLOTS_V1 * MAX_BUCKET_REPRESENTATIVES_V1;

const MAX_LOGICAL_CHUNK_BYTES: usize = 256 * 1_024;
const MAX_LOGICAL_CHUNK_BYTES_U32: u32 = 256 * 1_024;
const SHINGLE_BYTES: usize = 32;
const SHINGLE_ROTATION: u32 = 32;
const MINIMIZER_SPAN: usize = 64;
const DELTA_DEPENDENCY_BYTES: u32 = 32;
const DELTA_RUN_COUNT_BYTES: u32 = 4;
const DELTA_RUN_ENTRY_BYTES: u32 = 8;
const BYTE_HASH_SEED: u64 = 0x6a09_e667_f3bc_c909;
const SUPERFEATURE_SEEDS: [u64; 4] = [
    0xbb67_ae85_84ca_a73b,
    0x3c6e_f372_fe94_f82b,
    0xa54f_f53a_5f1d_36f1,
    0x510e_527f_ade6_82d1,
];
const SKETCH_SEEDS: [u64; 8] = [
    0x9b05_688c_2b3e_6c1f,
    0x1f83_d9ab_fb41_bd6b,
    0x5be0_cd19_137e_2179,
    0xcbbb_9d5d_c105_9ed8,
    0x629a_292a_367c_d507,
    0x9159_015a_3070_dd17,
    0x152f_ecd8_f70e_5939,
    0x6733_2667_ffc0_0b31,
];

/// A versioned, similarity-preserving description of one logical chunk.
///
/// It is derived acceleration only. Neither a matching Superfeature nor an
/// identical Sketch establishes content identity or integrity; only the full
/// `ChunkId` can do that. The scalar v1 implementation is the semantic oracle
/// for any later SIMD implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SimilarityFingerprint {
    profile: u16,
    superfeatures: [u64; 4],
    sketch: [u64; 8],
}

impl SimilarityFingerprint {
    /// Computes the deterministic v1 fingerprint for one nonempty logical
    /// chunk.
    ///
    /// Local edits affect the rolling shingles and bounded minimizer spans
    /// that cover them rather than mixing every later byte into one state.
    pub(crate) fn v1(bytes: &[u8]) -> Result<Self, SimilarityError> {
        validate_chunk_length(bytes.len())?;

        let mut accumulator = FingerprintAccumulator::new();
        if bytes.len() < SHINGLE_BYTES {
            let mut shingle = 0_u64;
            for &byte in bytes {
                shingle = shingle.rotate_left(1) ^ byte_hash(byte);
            }
            accumulator.observe_shingle(shingle);
        } else {
            let mut rolling = 0_u64;
            for &byte in &bytes[..SHINGLE_BYTES] {
                rolling = rolling.rotate_left(1) ^ byte_hash(byte);
            }
            accumulator.observe_shingle(rolling);

            for offset in SHINGLE_BYTES..bytes.len() {
                let outgoing = byte_hash(bytes[offset - SHINGLE_BYTES]);
                let incoming = byte_hash(bytes[offset]);
                rolling =
                    rolling.rotate_left(1) ^ outgoing.rotate_left(SHINGLE_ROTATION) ^ incoming;
                accumulator.observe_shingle(rolling);
            }
        }

        Ok(accumulator.finish())
    }

    /// Returns the stable scalar ordering material used only for bounded
    /// physical placement. It remains derived acceleration, never identity.
    #[must_use]
    pub(crate) const fn placement_key(self) -> ([u64; 4], [u64; 8]) {
        (self.superfeatures, self.sketch)
    }

    /// Returns the scalar XOR plus POPCNT distance in the inclusive range
    /// `0..=512`.
    fn distance(self, other: Self) -> Result<u16, SimilarityError> {
        if self.profile != other.profile {
            return Err(SimilarityError::ProfileMismatch);
        }
        let distance = self
            .sketch
            .iter()
            .zip(other.sketch)
            .map(|(left, right)| (left ^ right).count_ones())
            .sum::<u32>();
        u16::try_from(distance).map_err(|_| SimilarityError::ArithmeticOverflow)
    }
}

struct FingerprintAccumulator {
    superfeatures: [u64; 4],
    sketch_votes: [i32; 512],
    span_minimum: u64,
    span_length: usize,
    minimizer_count: usize,
}

impl FingerprintAccumulator {
    const fn new() -> Self {
        Self {
            superfeatures: [u64::MAX; 4],
            sketch_votes: [0; 512],
            span_minimum: u64::MAX,
            span_length: 0,
            minimizer_count: 0,
        }
    }

    fn observe_shingle(&mut self, shingle: u64) {
        self.span_minimum = self.span_minimum.min(shingle);
        self.span_length += 1;
        if self.span_length == MINIMIZER_SPAN {
            self.commit_minimizer();
        }
    }

    fn commit_minimizer(&mut self) {
        assert_ne!(
            self.span_length, 0,
            "ASSERT: a minimizer commit always has an observed shingle"
        );
        let minimizer = self.span_minimum;
        for (slot, seed) in self.superfeatures.iter_mut().zip(SUPERFEATURE_SEEDS) {
            *slot = (*slot).min(mix64(minimizer ^ seed));
        }
        for (word_index, seed) in SKETCH_SEEDS.into_iter().enumerate() {
            let word = mix64(minimizer ^ seed);
            let votes = &mut self.sketch_votes[word_index * 64..(word_index + 1) * 64];
            for (bit, vote) in votes.iter_mut().enumerate() {
                if word & (1_u64 << bit) == 0 {
                    *vote -= 1;
                } else {
                    *vote += 1;
                }
            }
        }
        self.minimizer_count += 1;
        self.span_minimum = u64::MAX;
        self.span_length = 0;
    }

    fn finish(mut self) -> SimilarityFingerprint {
        if self.span_length != 0 {
            self.commit_minimizer();
        }
        assert_ne!(
            self.minimizer_count, 0,
            "ASSERT: every nonempty chunk produces at least one minimizer"
        );

        let mut sketch = [0_u64; 8];
        for (word_index, output) in sketch.iter_mut().enumerate() {
            let votes = &self.sketch_votes[word_index * 64..(word_index + 1) * 64];
            for (bit, vote) in votes.iter().enumerate() {
                if *vote > 0 {
                    *output |= 1_u64 << bit;
                }
            }
        }
        SimilarityFingerprint {
            profile: SIMILARITY_PROFILE_V1,
            superfeatures: self.superfeatures,
            sketch,
        }
    }
}

/// One bounded similarity query. A result remains only a compression-base
/// candidate and must never be interpreted as an Exact Hit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SimilarityCandidate {
    chunk_id: ChunkId,
    logical_length: u32,
    sketch_distance: u16,
}

impl SimilarityCandidate {
    #[must_use]
    pub(crate) const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub(crate) const fn logical_length(self) -> u32 {
        self.logical_length
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SimilarityEntry {
    logical_length: u32,
    fingerprint: SimilarityFingerprint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct BucketKey {
    profile: u16,
    slot: u8,
    logical_length: u32,
    superfeature: u64,
}

/// The v1 representative policy retains the 64 smallest full Chunk IDs.
///
/// BLAKE3 Chunk IDs make this a deterministic min-hash sample rather than an
/// insertion-order sample. The sorted contiguous representation is bounded,
/// cache-local during lookup, and independent of map iteration order.
#[derive(Debug, Default, Eq, PartialEq)]
struct SimilarityBucketV1 {
    representatives: Vec<ChunkId>,
}

impl SimilarityBucketV1 {
    fn insert(&mut self, chunk_id: ChunkId) {
        let Err(position) = self.representatives.binary_search(&chunk_id) else {
            return;
        };
        if position >= MAX_BUCKET_REPRESENTATIVES_V1 {
            return;
        }

        self.representatives.insert(position, chunk_id);
        if self.representatives.len() > MAX_BUCKET_REPRESENTATIVES_V1 {
            let removed = self.representatives.pop();
            assert!(
                removed.is_some(),
                "ASSERT: overflowing a nonempty bounded similarity bucket removes one tail"
            );
        }
        assert!(
            self.representatives.len() <= MAX_BUCKET_REPRESENTATIVES_V1,
            "ASSERT: v1 similarity bucket exceeds its representative bound"
        );
    }

    fn representatives(&self) -> &[ChunkId] {
        assert!(
            self.representatives.len() <= MAX_BUCKET_REPRESENTATIVES_V1,
            "ASSERT: queried v1 similarity bucket exceeds its representative bound"
        );
        &self.representatives
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SimilarityQueryStats {
    bucket_profile: u16,
    buckets_read: usize,
    representatives_examined: usize,
    distinct_representatives_visited: usize,
    temporary_representative_ids_buffered: usize,
}

#[derive(Debug, Eq, PartialEq)]
struct SimilarityQuery {
    candidates: Vec<SimilarityCandidate>,
    stats: SimilarityQueryStats,
}

struct SimilarityBucketSelection<'a> {
    buckets: [Option<&'a SimilarityBucketV1>; SUPERFEATURE_SLOTS_V1],
    buckets_read: usize,
    representatives_examined: usize,
}

/// A deterministic bounded in-memory reference index.
///
/// `BTreeMap` plus sorted bounded representative vectors intentionally avoid
/// hash-map iteration order. Query results are ordered by Sketch distance and
/// then full Chunk ID, so insertion order and worker scheduling cannot change
/// the selected prefix.
#[derive(Debug, Default)]
pub(crate) struct SimilarityIndex {
    entries: BTreeMap<ChunkId, SimilarityEntry>,
    buckets: BTreeMap<BucketKey, SimilarityBucketV1>,
}

impl SimilarityIndex {
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            buckets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Rebuilds the bounded representative map from complete entries and
    /// verifies byte-for-byte equality with the live acceleration state.
    ///
    /// This is an expensive offline `AUDIT`, not a query-path check.
    pub(crate) fn audit(&self) -> Result<(), SimilarityError> {
        let mut rebuilt = BTreeMap::<BucketKey, SimilarityBucketV1>::new();
        for (&chunk_id, entry) in &self.entries {
            validate_logical_length(entry.logical_length)?;
            if entry.fingerprint.profile != SIMILARITY_PROFILE_V1 {
                return Err(SimilarityError::IndexCorruption);
            }
            for (slot, superfeature) in entry.fingerprint.superfeatures.into_iter().enumerate() {
                let slot = u8::try_from(slot).map_err(|_| SimilarityError::ArithmeticOverflow)?;
                rebuilt
                    .entry(BucketKey {
                        profile: entry.fingerprint.profile,
                        slot,
                        logical_length: entry.logical_length,
                        superfeature,
                    })
                    .or_default()
                    .insert(chunk_id);
            }
        }
        if rebuilt != self.buckets {
            return Err(SimilarityError::IndexCorruption);
        }
        Ok(())
    }

    /// Inserts derived similarity state without storing content bytes.
    ///
    /// Re-inserting the same logical identity is idempotent only when length
    /// and fingerprint agree. A Chunk ID length disagreement is Corruption,
    /// consistent with the Exact Index invariant.
    pub(crate) fn insert(
        &mut self,
        chunk_id: ChunkId,
        logical_length: u32,
        fingerprint: SimilarityFingerprint,
    ) -> Result<(), SimilarityError> {
        validate_logical_length(logical_length)?;
        if fingerprint.profile != SIMILARITY_PROFILE_V1 {
            return Err(SimilarityError::ProfileMismatch);
        }
        if let Some(existing) = self.entries.get(&chunk_id) {
            if existing.logical_length != logical_length {
                return Err(SimilarityError::ChunkIdentityLengthMismatch);
            }
            if existing.fingerprint != fingerprint {
                return Err(SimilarityError::FingerprintMismatch);
            }
            return Ok(());
        }

        self.entries.insert(
            chunk_id,
            SimilarityEntry {
                logical_length,
                fingerprint,
            },
        );
        for (slot, superfeature) in fingerprint.superfeatures.into_iter().enumerate() {
            let slot = u8::try_from(slot).map_err(|_| SimilarityError::ArithmeticOverflow)?;
            self.buckets
                .entry(BucketKey {
                    profile: fingerprint.profile,
                    slot,
                    logical_length,
                    superfeature,
                })
                .or_default()
                .insert(chunk_id);
        }
        Ok(())
    }

    /// Returns at most `limit` distinct equal-length candidates sharing at
    /// least one Superfeature with `target`.
    ///
    /// Ranking is scalar XOR plus POPCNT over the complete 512-bit Sketch.
    /// Exact content identity must be checked elsewhere with BLAKE3-256.
    pub(crate) fn candidates(
        &self,
        target_id: ChunkId,
        logical_length: u32,
        target: SimilarityFingerprint,
        limit: usize,
    ) -> Result<Vec<SimilarityCandidate>, SimilarityError> {
        let query = self.candidates_with_stats(target_id, logical_length, target, limit)?;
        assert_eq!(
            query.stats.bucket_profile, SIMILARITY_BUCKET_PROFILE_V1,
            "ASSERT: similarity query reports a different bucket profile"
        );
        assert!(
            query.stats.buckets_read <= SUPERFEATURE_SLOTS_V1,
            "ASSERT: similarity query reads more buckets than v1 has Superfeature slots"
        );
        assert!(
            query.stats.representatives_examined <= MAX_QUERY_REPRESENTATIVES_EXAMINED_V1,
            "ASSERT: similarity query examines more than its v1 hard bound"
        );
        assert!(
            query.stats.distinct_representatives_visited <= query.stats.representatives_examined,
            "ASSERT: query cannot visit more distinct IDs than it examined"
        );
        assert_eq!(
            query.stats.temporary_representative_ids_buffered, 0,
            "ASSERT: v1 query streams sorted buckets without an ID buffer"
        );
        Ok(query.candidates)
    }

    fn candidates_with_stats(
        &self,
        target_id: ChunkId,
        logical_length: u32,
        target: SimilarityFingerprint,
        limit: usize,
    ) -> Result<SimilarityQuery, SimilarityError> {
        validate_logical_length(logical_length)?;
        if target.profile != SIMILARITY_PROFILE_V1 {
            return Err(SimilarityError::ProfileMismatch);
        }
        if limit > MAX_SIMILARITY_CANDIDATES {
            return Err(SimilarityError::CandidateLimitExceeded);
        }
        if limit == 0 {
            return Ok(SimilarityQuery {
                candidates: Vec::new(),
                stats: SimilarityQueryStats {
                    bucket_profile: SIMILARITY_BUCKET_PROFILE_V1,
                    buckets_read: 0,
                    representatives_examined: 0,
                    distinct_representatives_visited: 0,
                    temporary_representative_ids_buffered: 0,
                },
            });
        }

        let selection = self.select_buckets(logical_length, target)?;

        // Merge four sorted representative slices. This visits every stored
        // representative once, deduplicates equal IDs across buckets, and
        // requires only four cursors rather than a per-query ID collection.
        let mut cursors = [0_usize; SUPERFEATURE_SLOTS_V1];
        let mut distinct_representatives_visited = 0_usize;
        let mut candidates: Vec<SimilarityCandidate> = Vec::with_capacity(limit);
        loop {
            let next_id = selection
                .buckets
                .iter()
                .zip(cursors)
                .filter_map(|(bucket, cursor)| {
                    (*bucket)
                        .and_then(|bucket| bucket.representatives().get(cursor))
                        .copied()
                })
                .min();
            let Some(chunk_id) = next_id else {
                break;
            };

            for (bucket, cursor) in selection.buckets.iter().zip(&mut cursors) {
                let Some(bucket) = bucket else {
                    continue;
                };
                let representative = bucket.representatives().get(*cursor);
                if representative == Some(&chunk_id) {
                    *cursor += 1;
                } else {
                    assert!(
                        representative.is_none_or(|candidate| *candidate > chunk_id),
                        "ASSERT: v1 bucket representatives are not strictly sorted"
                    );
                }
            }
            distinct_representatives_visited = distinct_representatives_visited
                .checked_add(1)
                .ok_or(SimilarityError::ArithmeticOverflow)?;
            if distinct_representatives_visited > MAX_QUERY_REPRESENTATIVES_EXAMINED_V1 {
                return Err(SimilarityError::IndexCorruption);
            }

            if chunk_id == target_id {
                continue;
            }
            let entry = self
                .entries
                .get(&chunk_id)
                .ok_or(SimilarityError::IndexCorruption)?;
            if entry.logical_length != logical_length {
                continue;
            }
            let candidate = SimilarityCandidate {
                chunk_id,
                logical_length: entry.logical_length,
                sketch_distance: target.distance(entry.fingerprint)?,
            };
            insert_ranked_candidate(&mut candidates, candidate, limit);
        }
        Ok(SimilarityQuery {
            candidates,
            stats: SimilarityQueryStats {
                bucket_profile: SIMILARITY_BUCKET_PROFILE_V1,
                buckets_read: selection.buckets_read,
                representatives_examined: selection.representatives_examined,
                distinct_representatives_visited,
                temporary_representative_ids_buffered: 0,
            },
        })
    }

    fn select_buckets(
        &self,
        logical_length: u32,
        target: SimilarityFingerprint,
    ) -> Result<SimilarityBucketSelection<'_>, SimilarityError> {
        let mut buckets = [None; SUPERFEATURE_SLOTS_V1];
        let mut buckets_read = 0_usize;
        let mut representatives_examined = 0_usize;
        for (slot, superfeature) in target.superfeatures.into_iter().enumerate() {
            let slot_u8 = u8::try_from(slot).map_err(|_| SimilarityError::ArithmeticOverflow)?;
            if let Some(bucket) = self.buckets.get(&BucketKey {
                profile: target.profile,
                slot: slot_u8,
                logical_length,
                superfeature,
            }) {
                buckets_read += 1;
                representatives_examined = representatives_examined
                    .checked_add(bucket.representatives().len())
                    .ok_or(SimilarityError::ArithmeticOverflow)?;
                if representatives_examined > MAX_QUERY_REPRESENTATIVES_EXAMINED_V1 {
                    return Err(SimilarityError::IndexCorruption);
                }
                buckets[slot] = Some(bucket);
            }
        }
        Ok(SimilarityBucketSelection {
            buckets,
            buckets_read,
            representatives_examined,
        })
    }
}

fn insert_ranked_candidate(
    candidates: &mut Vec<SimilarityCandidate>,
    candidate: SimilarityCandidate,
    limit: usize,
) {
    let candidate_key = (candidate.sketch_distance, candidate.chunk_id);
    let position = candidates
        .partition_point(|existing| (existing.sketch_distance, existing.chunk_id) < candidate_key);
    if candidates.len() < limit {
        candidates.insert(position, candidate);
    } else if position < limit {
        let removed = candidates.pop();
        assert!(
            removed.is_some(),
            "ASSERT: a full nonempty candidate prefix has a removable tail"
        );
        candidates.insert(position, candidate);
    }
}

/// Exactly one verified, independently decodable Base Chunk reference.
///
/// Physical codec and Location are intentionally absent: relocation or
/// re-encoding of an independently decodable Base must not invalidate a
/// logical Depth-1 dependency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndependentBaseRef {
    chunk_id: ChunkId,
    logical_length: u32,
}

impl IndependentBaseRef {
    /// Constructs a logical Base reference from verified bytes. The caller is
    /// responsible for selecting an independently decodable Location.
    pub(crate) fn from_verified_bytes(bytes: &[u8]) -> Result<Self, SimilarityError> {
        validate_chunk_length(bytes.len())?;
        let logical_length =
            u32::try_from(bytes.len()).map_err(|_| SimilarityError::ArithmeticOverflow)?;
        Ok(Self {
            chunk_id: ChunkId::of(bytes),
            logical_length,
        })
    }

    #[must_use]
    pub(crate) const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub(crate) const fn logical_length(self) -> u32 {
        self.logical_length
    }

    fn verify(self, bytes: &[u8]) -> Result<(), SimilarityError> {
        let length = u32::try_from(bytes.len()).map_err(|_| SimilarityError::BaseLengthMismatch)?;
        if length != self.logical_length {
            return Err(SimilarityError::BaseLengthMismatch);
        }
        if ChunkId::of(bytes) != self.chunk_id {
            return Err(SimilarityError::BaseIdentityMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeltaRun {
    logical_offset: u32,
    payload_offset: u32,
    length: u32,
}

/// The measured payload cost of a sparse-XOR trial.
///
/// `encoded_payload_bytes` includes the Base Chunk ID, run count, run table,
/// and XOR bytes. It deliberately excludes the enclosing Encoding Record,
/// Recovery Index, and alignment: the Reduction Engine acceptance gate must
/// add those exact costs before applying the 5% and 4 KiB thresholds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeltaTrialCost {
    target_bytes: u32,
    run_count: u32,
    xor_bytes: u32,
    encoded_payload_bytes: u32,
}

impl DeltaTrialCost {
    #[must_use]
    pub(crate) const fn target_bytes(self) -> u32 {
        self.target_bytes
    }

    #[must_use]
    pub(crate) const fn run_count(self) -> u32 {
        self.run_count
    }

    #[must_use]
    pub(crate) const fn xor_bytes(self) -> u32 {
        self.xor_bytes
    }

    #[must_use]
    pub(crate) const fn encoded_payload_bytes(self) -> u32 {
        self.encoded_payload_bytes
    }
}

/// One Depth-1 sparse-XOR trial. Creating it does not accept it for storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeltaTrial {
    encoding: SparseXorDelta,
    cost: DeltaTrialCost,
}

impl DeltaTrial {
    #[must_use]
    pub(crate) const fn encoding(&self) -> &SparseXorDelta {
        &self.encoding
    }

    #[must_use]
    pub(crate) const fn cost(&self) -> DeltaTrialCost {
        self.cost
    }

    /// Consumes the trial after the caller's complete physical-cost gate has
    /// accepted it. This module intentionally contains no acceptance policy.
    #[must_use]
    pub(crate) fn into_encoding(self) -> SparseXorDelta {
        self.encoding
    }
}

/// A byte-exact sparse-XOR encoding for one same-length target Chunk.
///
/// Its single `IndependentBaseRef` is the only Chunk dependency. The run table
/// and XOR payload are checked independently during encode and decode, and the
/// fully reconstructed target must match `target_id` before it can be returned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SparseXorDelta {
    base: IndependentBaseRef,
    target_id: ChunkId,
    logical_length: u32,
    runs: Box<[DeltaRun]>,
    xor_bytes: Box<[u8]>,
}

impl SparseXorDelta {
    pub(crate) fn encode_trial(
        base: IndependentBaseRef,
        base_bytes: &[u8],
        target_bytes: &[u8],
    ) -> Result<DeltaTrial, SimilarityError> {
        base.verify(base_bytes)?;
        validate_chunk_length(target_bytes.len())?;
        if base_bytes.len() != target_bytes.len() {
            return Err(SimilarityError::DeltaLengthMismatch);
        }

        let logical_length =
            u32::try_from(target_bytes.len()).map_err(|_| SimilarityError::ArithmeticOverflow)?;
        let mut runs = Vec::new();
        let mut xor_bytes = Vec::new();
        let mut cursor = 0_usize;
        while cursor < target_bytes.len() {
            if base_bytes[cursor] == target_bytes[cursor] {
                cursor += 1;
                continue;
            }

            let run_start = cursor;
            let payload_start = xor_bytes.len();
            while cursor < target_bytes.len() && base_bytes[cursor] != target_bytes[cursor] {
                xor_bytes.push(base_bytes[cursor] ^ target_bytes[cursor]);
                cursor += 1;
            }
            let run_length = cursor
                .checked_sub(run_start)
                .ok_or(SimilarityError::ArithmeticOverflow)?;
            runs.push(DeltaRun {
                logical_offset: u32::try_from(run_start)
                    .map_err(|_| SimilarityError::ArithmeticOverflow)?,
                payload_offset: u32::try_from(payload_start)
                    .map_err(|_| SimilarityError::ArithmeticOverflow)?,
                length: u32::try_from(run_length)
                    .map_err(|_| SimilarityError::ArithmeticOverflow)?,
            });
        }

        let run_count =
            u32::try_from(runs.len()).map_err(|_| SimilarityError::ArithmeticOverflow)?;
        let xor_length =
            u32::try_from(xor_bytes.len()).map_err(|_| SimilarityError::ArithmeticOverflow)?;
        let run_table_bytes = run_count
            .checked_mul(DELTA_RUN_ENTRY_BYTES)
            .ok_or(SimilarityError::ArithmeticOverflow)?;
        let encoded_payload_bytes = DELTA_DEPENDENCY_BYTES
            .checked_add(DELTA_RUN_COUNT_BYTES)
            .and_then(|bytes| bytes.checked_add(run_table_bytes))
            .and_then(|bytes| bytes.checked_add(xor_length))
            .ok_or(SimilarityError::ArithmeticOverflow)?;
        let encoding = Self {
            base,
            target_id: ChunkId::of(target_bytes),
            logical_length,
            runs: runs.into_boxed_slice(),
            xor_bytes: xor_bytes.into_boxed_slice(),
        };
        // Exercise the reader-side invariant before presenting a writer trial.
        if encoding.decode(base_bytes)? != target_bytes {
            return Err(SimilarityError::DeltaReconstructionMismatch);
        }
        Ok(DeltaTrial {
            encoding,
            cost: DeltaTrialCost {
                target_bytes: logical_length,
                run_count,
                xor_bytes: xor_length,
                encoded_payload_bytes,
            },
        })
    }

    #[must_use]
    pub(crate) const fn base(&self) -> IndependentBaseRef {
        self.base
    }

    #[must_use]
    pub(crate) const fn target_id(&self) -> ChunkId {
        self.target_id
    }

    #[must_use]
    pub(crate) const fn logical_length(&self) -> u32 {
        self.logical_length
    }

    /// Reconstructs and fully verifies the target bytes.
    pub(crate) fn decode(&self, base_bytes: &[u8]) -> Result<Vec<u8>, SimilarityError> {
        self.base.verify(base_bytes)?;
        if self.base.logical_length != self.logical_length {
            return Err(SimilarityError::DeltaLengthMismatch);
        }
        let length = usize::try_from(self.logical_length)
            .map_err(|_| SimilarityError::ArithmeticOverflow)?;
        if length > MAX_LOGICAL_CHUNK_BYTES {
            return Err(SimilarityError::ChunkTooLarge);
        }
        let mut restored = base_bytes.to_vec();
        let mut logical_end = 0_usize;
        let mut payload_end = 0_usize;
        for (ordinal, run) in self.runs.iter().enumerate() {
            let logical_offset = usize::try_from(run.logical_offset)
                .map_err(|_| SimilarityError::DeltaRunOutOfBounds)?;
            let payload_offset = usize::try_from(run.payload_offset)
                .map_err(|_| SimilarityError::DeltaPayloadMismatch)?;
            let run_length =
                usize::try_from(run.length).map_err(|_| SimilarityError::DeltaRunOutOfBounds)?;
            if run_length == 0 || payload_offset != payload_end {
                return Err(SimilarityError::DeltaPayloadMismatch);
            }
            if ordinal != 0 && logical_offset <= logical_end {
                return Err(SimilarityError::DeltaRunOrder);
            }
            let next_logical_end = logical_offset
                .checked_add(run_length)
                .ok_or(SimilarityError::ArithmeticOverflow)?;
            let next_payload_end = payload_offset
                .checked_add(run_length)
                .ok_or(SimilarityError::ArithmeticOverflow)?;
            if next_logical_end > length || next_payload_end > self.xor_bytes.len() {
                return Err(SimilarityError::DeltaRunOutOfBounds);
            }

            let xor = &self.xor_bytes[payload_offset..next_payload_end];
            if xor.contains(&0) {
                return Err(SimilarityError::DeltaPayloadMismatch);
            }
            for (target, difference) in restored[logical_offset..next_logical_end]
                .iter_mut()
                .zip(xor)
            {
                *target ^= difference;
            }
            logical_end = next_logical_end;
            payload_end = next_payload_end;
        }
        if payload_end != self.xor_bytes.len() {
            return Err(SimilarityError::DeltaPayloadMismatch);
        }
        if ChunkId::of(&restored) != self.target_id {
            return Err(SimilarityError::DeltaReconstructionMismatch);
        }
        Ok(restored)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SimilarityError {
    EmptyChunk,
    ChunkTooLarge,
    InvalidLogicalLength,
    ProfileMismatch,
    CandidateLimitExceeded,
    ChunkIdentityLengthMismatch,
    FingerprintMismatch,
    IndexCorruption,
    BaseLengthMismatch,
    BaseIdentityMismatch,
    DeltaLengthMismatch,
    DeltaRunOrder,
    DeltaRunOutOfBounds,
    DeltaPayloadMismatch,
    DeltaReconstructionMismatch,
    ArithmeticOverflow,
}

impl fmt::Display for SimilarityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyChunk => "logical chunks must be nonempty",
            Self::ChunkTooLarge => "logical chunk exceeds the v1 maximum",
            Self::InvalidLogicalLength => "logical chunk length is outside v1 bounds",
            Self::ProfileMismatch => "similarity fingerprint profile mismatch",
            Self::CandidateLimitExceeded => "similarity candidate limit exceeds 16",
            Self::ChunkIdentityLengthMismatch => "one Chunk ID has two logical lengths",
            Self::FingerprintMismatch => "one Chunk ID has conflicting fingerprints",
            Self::IndexCorruption => "similarity bucket references an absent entry",
            Self::BaseLengthMismatch => "Base Chunk length mismatch",
            Self::BaseIdentityMismatch => "Base Chunk identity mismatch",
            Self::DeltaLengthMismatch => "sparse-XOR requires equal nonempty chunk lengths",
            Self::DeltaRunOrder => "delta runs overlap or are not in canonical order",
            Self::DeltaRunOutOfBounds => "delta run escapes the logical chunk or payload",
            Self::DeltaPayloadMismatch => "delta run table does not partition its XOR payload",
            Self::DeltaReconstructionMismatch => "reconstructed target Chunk ID mismatch",
            Self::ArithmeticOverflow => "similarity or delta length arithmetic overflow",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for SimilarityError {}

fn validate_chunk_length(length: usize) -> Result<(), SimilarityError> {
    if length == 0 {
        return Err(SimilarityError::EmptyChunk);
    }
    if length > MAX_LOGICAL_CHUNK_BYTES {
        return Err(SimilarityError::ChunkTooLarge);
    }
    Ok(())
}

fn validate_logical_length(length: u32) -> Result<(), SimilarityError> {
    if length == 0 || length > MAX_LOGICAL_CHUNK_BYTES_U32 {
        return Err(SimilarityError::InvalidLogicalLength);
    }
    Ok(())
}

fn byte_hash(byte: u8) -> u64 {
    mix64(BYTE_HASH_SEED ^ u64::from(byte))
}

const fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_profile_v1_has_a_stable_golden_fingerprint() {
        let fingerprint = SimilarityFingerprint::v1(b"fastdup-similarity-profile-v1")
            .expect("golden chunk is valid");

        assert_eq!(fingerprint.profile, SIMILARITY_PROFILE_V1);
        assert_eq!(
            fingerprint.superfeatures,
            [
                3_732_405_128_740_378_522,
                8_196_857_781_856_605_258,
                13_259_868_137_371_181_506,
                7_156_318_887_736_273_063,
            ]
        );
        assert_eq!(
            fingerprint.sketch,
            [
                13_497_657_219_009_815_527,
                5_669_547_852_489_421_255,
                5_545_660_712_991_071_033,
                11_075_370_911_771_268_383,
                16_619_688_691_950_237_572,
                16_131_934_278_318_304_641,
                2_938_352_127_724_265_571,
                8_645_401_422_114_912_897,
            ]
        );
        assert_eq!(fingerprint.distance(fingerprint), Ok(0));
    }

    #[test]
    fn scalar_profile_is_deterministic_across_boundaries_and_local_edits_stay_local() {
        for length in [1, 31, 32, 33, 95, 96, 97, 16 * 1_024, 64 * 1_024] {
            let bytes = fixture_bytes(length, 0x1234_5678_9abc_def0);
            let first = SimilarityFingerprint::v1(&bytes).expect("fixture chunk is valid");
            let second = SimilarityFingerprint::v1(&bytes).expect("fixture chunk is valid");
            assert_eq!(first, second);
            assert_eq!(first.distance(second), Ok(0));
        }

        let original = fixture_bytes(64 * 1_024, 11);
        let mut locally_edited = original.clone();
        locally_edited[32 * 1_024] ^= 0x5a;
        let unrelated = fixture_bytes(64 * 1_024, 12);
        let original_fingerprint = SimilarityFingerprint::v1(&original).expect("original is valid");
        let local_fingerprint =
            SimilarityFingerprint::v1(&locally_edited).expect("local edit is valid");
        let unrelated_fingerprint =
            SimilarityFingerprint::v1(&unrelated).expect("unrelated chunk is valid");
        let local_distance = original_fingerprint
            .distance(local_fingerprint)
            .expect("profiles agree");
        let unrelated_distance = original_fingerprint
            .distance(unrelated_fingerprint)
            .expect("profiles agree");

        assert!(local_distance < unrelated_distance);
        assert!(local_distance <= 32);
        assert!(
            original_fingerprint
                .superfeatures
                .iter()
                .zip(local_fingerprint.superfeatures)
                .any(|(left, right)| *left == right)
        );
    }

    #[test]
    fn fingerprint_rejects_empty_oversized_and_mixed_profiles() {
        assert_eq!(
            SimilarityFingerprint::v1(&[]),
            Err(SimilarityError::EmptyChunk)
        );
        assert_eq!(
            SimilarityFingerprint::v1(&vec![0; MAX_LOGICAL_CHUNK_BYTES + 1]),
            Err(SimilarityError::ChunkTooLarge)
        );
        let valid = SimilarityFingerprint::v1(b"valid").expect("fixture is valid");
        let mut other_profile = valid;
        other_profile.profile += 1;
        assert_eq!(
            valid.distance(other_profile),
            Err(SimilarityError::ProfileMismatch)
        );
    }

    #[test]
    fn candidates_are_ordered_deduplicated_limited_and_length_filtered() {
        let target_id = fixture_id(9_000);
        let target = controlled_fingerprint([10, 20, 30, 40], [0; 8]);
        let same_fingerprint_id = fixture_id(1);
        let multi_bucket_id = fixture_id(2);
        let farther_id = fixture_id(3);
        let wrong_length_id = fixture_id(4);
        let mut index = SimilarityIndex::new();

        index
            .insert(target_id, 64 * 1_024, target)
            .expect("target insertion succeeds");
        index
            .insert(same_fingerprint_id, 64 * 1_024, target)
            .expect("same derived fingerprint is not content identity");
        index
            .insert(
                multi_bucket_id,
                64 * 1_024,
                controlled_fingerprint([10, 20, 300, 400], [1; 8]),
            )
            .expect("multi-bucket candidate insertion succeeds");
        index
            .insert(
                farther_id,
                64 * 1_024,
                controlled_fingerprint([10, 200, 300, 400], [u64::MAX; 8]),
            )
            .expect("far candidate insertion succeeds");
        index
            .insert(wrong_length_id, 32 * 1_024, target)
            .expect("different-length derived entry is valid");

        let candidates = index
            .candidates(target_id, 64 * 1_024, target, 16)
            .expect("bounded query succeeds");
        assert_eq!(candidates.len(), 3);
        assert_eq!(candidates[0].chunk_id(), same_fingerprint_id);
        assert_eq!(candidates[0].sketch_distance, 0);
        assert_ne!(candidates[0].chunk_id(), target_id);
        assert_eq!(candidates[1].chunk_id(), multi_bucket_id);
        assert_eq!(candidates[1].sketch_distance, 8);
        assert_eq!(candidates[2].chunk_id(), farther_id);
        assert_eq!(candidates[2].sketch_distance, 512);
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.logical_length() == 64 * 1_024)
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.chunk_id() != wrong_length_id)
        );
        assert_eq!(
            index.candidates(target_id, 64 * 1_024, target, 0),
            Ok(Vec::new())
        );
        assert_eq!(
            index.candidates(target_id, 64 * 1_024, target, 17),
            Err(SimilarityError::CandidateLimitExceeded)
        );
    }

    #[test]
    fn candidate_limit_returns_the_deterministic_best_prefix() {
        let target_id = fixture_id(10_000);
        let target = controlled_fingerprint([7, 8, 9, 10], [0; 8]);
        let mut index = SimilarityIndex::new();
        let mut expected = Vec::new();
        for ordinal in 0_u64..32 {
            let chunk_id = fixture_id(ordinal);
            let distance_word = 1_u64 << u32::try_from(ordinal % 64).expect("bit is bounded");
            let fingerprint =
                controlled_fingerprint([7, 80 + ordinal, 90, 100], [distance_word; 8]);
            index
                .insert(chunk_id, 64 * 1_024, fingerprint)
                .expect("fixture candidate insertion succeeds");
            expected.push((8_u16, chunk_id));
        }
        expected.sort_unstable();
        expected.truncate(MAX_SIMILARITY_CANDIDATES);

        let actual = index
            .candidates(target_id, 64 * 1_024, target, MAX_SIMILARITY_CANDIDATES)
            .expect("maximum bounded query succeeds")
            .into_iter()
            .map(|candidate| (candidate.sketch_distance, candidate.chunk_id))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn hot_buckets_have_versioned_hard_storage_and_query_bounds() {
        let target_id = fixture_id(20_000);
        let target = controlled_fingerprint([7, 8, 9, 10], [0; 8]);
        let mut ids = (0_u64..10_000).map(fixture_id).collect::<Vec<_>>();
        let mut expected = ids.clone();
        expected.sort_unstable();
        expected.truncate(MAX_SIMILARITY_CANDIDATES);

        let mut forward = SimilarityIndex::new();
        for chunk_id in &ids {
            forward
                .insert(*chunk_id, 64 * 1_024, target)
                .expect("hot-bucket insertion succeeds");
        }
        ids.reverse();
        let mut reverse = SimilarityIndex::new();
        for chunk_id in &ids {
            reverse
                .insert(*chunk_id, 64 * 1_024, target)
                .expect("reverse hot-bucket insertion succeeds");
        }

        let forward_query = forward
            .candidates_with_stats(target_id, 64 * 1_024, target, MAX_SIMILARITY_CANDIDATES)
            .expect("bounded hot-bucket query succeeds");
        let reverse_query = reverse
            .candidates_with_stats(target_id, 64 * 1_024, target, MAX_SIMILARITY_CANDIDATES)
            .expect("reverse bounded hot-bucket query succeeds");

        assert_eq!(forward_query.candidates, reverse_query.candidates);
        assert_eq!(
            forward_query
                .candidates
                .iter()
                .map(|candidate| candidate.chunk_id())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            forward_query.stats.bucket_profile,
            SIMILARITY_BUCKET_PROFILE_V1
        );
        assert_eq!(forward_query.stats.buckets_read, 4);
        assert_eq!(
            forward_query.stats.representatives_examined,
            4 * MAX_BUCKET_REPRESENTATIVES_V1
        );
        assert_eq!(
            forward_query.stats.distinct_representatives_visited,
            MAX_BUCKET_REPRESENTATIVES_V1
        );
        assert_eq!(forward_query.stats.temporary_representative_ids_buffered, 0);
        assert_eq!(forward_query.stats, reverse_query.stats);
        assert!(
            forward_query.stats.representatives_examined <= MAX_QUERY_REPRESENTATIVES_EXAMINED_V1
        );
        assert!(
            forward_query.stats.distinct_representatives_visited
                <= MAX_QUERY_REPRESENTATIVES_EXAMINED_V1
        );
    }

    #[test]
    fn hot_buckets_of_other_lengths_cannot_evict_equal_length_bases() {
        let target_id = fixture_id(30_000);
        let target = controlled_fingerprint([71, 81, 91, 101], [0; 8]);
        let mut ids = (0_u64..96).map(fixture_id).collect::<Vec<_>>();
        ids.sort_unstable();
        let (small_wrong_length_ids, large_equal_length_ids) = ids.split_at(64);
        let mut index = SimilarityIndex::new();
        for chunk_id in small_wrong_length_ids {
            index
                .insert(*chunk_id, 32 * 1_024, target)
                .expect("different-length hot-bucket insertion succeeds");
        }
        for chunk_id in large_equal_length_ids {
            index
                .insert(*chunk_id, 64 * 1_024, target)
                .expect("equal-length hot-bucket insertion succeeds");
        }

        let actual = index
            .candidates(target_id, 64 * 1_024, target, MAX_SIMILARITY_CANDIDATES)
            .expect("length-isolated bounded query succeeds")
            .into_iter()
            .map(SimilarityCandidate::chunk_id)
            .collect::<Vec<_>>();
        assert_eq!(actual, large_equal_length_ids[..MAX_SIMILARITY_CANDIDATES]);
    }

    #[test]
    fn index_reinsertion_pairs_chunk_identity_length_and_fingerprint() {
        let id = fixture_id(77);
        let fingerprint = controlled_fingerprint([1, 2, 3, 4], [5; 8]);
        let mut index = SimilarityIndex::new();
        assert_eq!(index.insert(id, 1024, fingerprint), Ok(()));
        assert_eq!(index.insert(id, 1024, fingerprint), Ok(()));
        assert_eq!(
            index.insert(id, 1025, fingerprint),
            Err(SimilarityError::ChunkIdentityLengthMismatch)
        );
        assert_eq!(
            index.insert(id, 1024, controlled_fingerprint([1, 2, 3, 4], [6; 8])),
            Err(SimilarityError::FingerprintMismatch)
        );
        assert_eq!(
            index.insert(id, 0, fingerprint),
            Err(SimilarityError::InvalidLogicalLength)
        );
    }

    #[test]
    fn sparse_xor_handles_zero_one_and_multiple_runs_byte_exactly() {
        let base_bytes = fixture_bytes(4 * 1_024, 100);
        let base = IndependentBaseRef::from_verified_bytes(&base_bytes).expect("base is valid");

        let zero = SparseXorDelta::encode_trial(base, &base_bytes, &base_bytes)
            .expect("zero-run trial is valid");
        assert_eq!(zero.cost().run_count(), 0);
        assert_eq!(zero.cost().xor_bytes(), 0);
        assert_eq!(zero.cost().encoded_payload_bytes(), 36);
        assert_eq!(zero.encoding().decode(&base_bytes), Ok(base_bytes.clone()));

        let mut one_target = base_bytes.clone();
        for (ordinal, byte) in one_target[100..107].iter_mut().enumerate() {
            *byte ^= u8::try_from(ordinal + 1).expect("fixture XOR is bounded");
        }
        let one = SparseXorDelta::encode_trial(base, &base_bytes, &one_target)
            .expect("one-run trial is valid");
        assert_eq!(one.cost().run_count(), 1);
        assert_eq!(one.cost().xor_bytes(), 7);
        assert_eq!(one.cost().encoded_payload_bytes(), 36 + 8 + 7);
        assert_eq!(one.encoding().decode(&base_bytes), Ok(one_target));

        let mut multiple_target = base_bytes.clone();
        multiple_target[1] ^= 1;
        multiple_target[10] ^= 2;
        multiple_target[11] ^= 3;
        multiple_target[12] ^= 4;
        let last = multiple_target.len() - 1;
        multiple_target[last] ^= 5;
        let multiple = SparseXorDelta::encode_trial(base, &base_bytes, &multiple_target)
            .expect("multi-run trial is valid");
        assert_eq!(multiple.cost().run_count(), 3);
        assert_eq!(multiple.cost().xor_bytes(), 5);
        assert_eq!(multiple.encoding().decode(&base_bytes), Ok(multiple_target));
    }

    #[test]
    fn sparse_xor_rejects_empty_oversized_and_length_mismatched_chunks() {
        assert_eq!(
            IndependentBaseRef::from_verified_bytes(&[]),
            Err(SimilarityError::EmptyChunk)
        );
        assert_eq!(
            IndependentBaseRef::from_verified_bytes(&vec![0; MAX_LOGICAL_CHUNK_BYTES + 1]),
            Err(SimilarityError::ChunkTooLarge)
        );
        let base_bytes = vec![1; 16];
        let base = IndependentBaseRef::from_verified_bytes(&base_bytes).expect("base is valid");
        assert_eq!(
            SparseXorDelta::encode_trial(base, &base_bytes, &[]),
            Err(SimilarityError::EmptyChunk)
        );
        assert_eq!(
            SparseXorDelta::encode_trial(base, &base_bytes, &[2; 17]),
            Err(SimilarityError::DeltaLengthMismatch)
        );
        assert_eq!(
            SparseXorDelta::encode_trial(base, &base_bytes, &vec![2; MAX_LOGICAL_CHUNK_BYTES + 1],),
            Err(SimilarityError::ChunkTooLarge)
        );
    }

    #[test]
    fn sparse_xor_writer_reader_sweep_is_byte_exact() {
        for case in 0_usize..256 {
            let length = 1 + (case * 997) % (32 * 1_024);
            let base_bytes = fixture_bytes(
                length,
                0x8000 + u64::try_from(case).expect("fixture case fits u64"),
            );
            let mut target = base_bytes.clone();
            let edit_count = 1 + case % 17;
            for edit in 0..edit_count {
                let offset = (edit * 7_919 + case * 101) % length;
                target[offset] ^= 1 + u8::try_from(edit % 251).expect("fixture XOR is bounded");
            }
            let base =
                IndependentBaseRef::from_verified_bytes(&base_bytes).expect("sweep base is valid");
            let trial = SparseXorDelta::encode_trial(base, &base_bytes, &target)
                .expect("sweep trial is valid");
            assert_eq!(trial.cost().target_bytes(), u32::try_from(length).unwrap());
            assert_eq!(trial.encoding().target_id(), ChunkId::of(&target));
            assert_eq!(
                trial.encoding().logical_length(),
                u32::try_from(length).unwrap()
            );
            assert_eq!(trial.encoding().decode(&base_bytes), Ok(target));
        }
    }

    #[test]
    fn sparse_xor_rejects_wrong_base_identity_and_length() {
        let (base_bytes, target, encoding) = valid_multi_run_delta();
        let mut wrong_identity = base_bytes.clone();
        wrong_identity[0] ^= 1;
        assert_eq!(
            encoding.decode(&wrong_identity),
            Err(SimilarityError::BaseIdentityMismatch)
        );
        assert_eq!(
            encoding.decode(&base_bytes[..base_bytes.len() - 1]),
            Err(SimilarityError::BaseLengthMismatch)
        );
        assert_eq!(encoding.decode(&base_bytes), Ok(target));
    }

    #[test]
    fn sparse_xor_rejects_every_malformed_run_and_payload_shape() {
        let (base_bytes, _, valid) = valid_multi_run_delta();

        let mut overlap = valid.clone();
        overlap.runs[1].logical_offset = overlap.runs[0].logical_offset;
        assert_eq!(
            overlap.decode(&base_bytes),
            Err(SimilarityError::DeltaRunOrder)
        );

        let mut reversed = valid.clone();
        reversed.runs[0].logical_offset = 20;
        reversed.runs[1].logical_offset = 2;
        assert_eq!(
            reversed.decode(&base_bytes),
            Err(SimilarityError::DeltaRunOrder)
        );

        let mut out_of_bounds = valid.clone();
        out_of_bounds.runs[1].logical_offset = out_of_bounds.logical_length - 1;
        out_of_bounds.runs[1].length = 2;
        assert_eq!(
            out_of_bounds.decode(&base_bytes),
            Err(SimilarityError::DeltaRunOutOfBounds)
        );

        let mut bad_payload_offset = valid.clone();
        bad_payload_offset.runs[1].payload_offset = 0;
        assert_eq!(
            bad_payload_offset.decode(&base_bytes),
            Err(SimilarityError::DeltaPayloadMismatch)
        );

        let mut payload_out_of_bounds = valid.clone();
        payload_out_of_bounds.runs[0].length = 4;
        assert_eq!(
            payload_out_of_bounds.decode(&base_bytes),
            Err(SimilarityError::DeltaRunOutOfBounds)
        );

        let mut zero_length = valid.clone();
        zero_length.runs[0].length = 0;
        assert_eq!(
            zero_length.decode(&base_bytes),
            Err(SimilarityError::DeltaPayloadMismatch)
        );

        let mut zero_xor = valid.clone();
        zero_xor.xor_bytes[0] = 0;
        assert_eq!(
            zero_xor.decode(&base_bytes),
            Err(SimilarityError::DeltaPayloadMismatch)
        );

        let mut trailing_payload = valid.clone();
        let mut payload = trailing_payload.xor_bytes.to_vec();
        payload.push(1);
        trailing_payload.xor_bytes = payload.into_boxed_slice();
        assert_eq!(
            trailing_payload.decode(&base_bytes),
            Err(SimilarityError::DeltaPayloadMismatch)
        );

        let mut wrong_target = valid;
        wrong_target.target_id = fixture_id(0xdead_beef);
        assert_eq!(
            wrong_target.decode(&base_bytes),
            Err(SimilarityError::DeltaReconstructionMismatch)
        );
    }

    #[test]
    fn base_reference_is_logical_and_has_no_physical_codec_dependency() {
        let bytes = b"independently decodable base";
        let base =
            IndependentBaseRef::from_verified_bytes(bytes).expect("independent base is valid");
        assert_eq!(base.chunk_id(), ChunkId::of(bytes));
        assert_eq!(base.logical_length(), u32::try_from(bytes.len()).unwrap());
        assert_eq!(std::mem::size_of::<IndependentBaseRef>(), 36);
    }

    fn controlled_fingerprint(superfeatures: [u64; 4], sketch: [u64; 8]) -> SimilarityFingerprint {
        SimilarityFingerprint {
            profile: SIMILARITY_PROFILE_V1,
            superfeatures,
            sketch,
        }
    }

    fn valid_multi_run_delta() -> (Vec<u8>, Vec<u8>, SparseXorDelta) {
        let base_bytes = fixture_bytes(64, 0x1234);
        let mut target = base_bytes.clone();
        target[2] ^= 0x11;
        target[20] ^= 0x22;
        target[21] ^= 0x33;
        let base =
            IndependentBaseRef::from_verified_bytes(&base_bytes).expect("fixture base is valid");
        let encoding = SparseXorDelta::encode_trial(base, &base_bytes, &target)
            .expect("fixture delta is valid")
            .into_encoding();
        (base_bytes, target, encoding)
    }

    fn fixture_id(seed: u64) -> ChunkId {
        ChunkId::of(&fixture_bytes(64, seed))
    }

    fn fixture_bytes(length: usize, seed: u64) -> Vec<u8> {
        let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
        (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect()
    }
}
