use std::fmt;

use crate::{ChunkId, MAX_LOGICAL_CHUNK_BYTES, crc32c_with_zeroed_u32};

pub const SIMILARITY_INDEX_HEADER_BYTES: usize = 4_096;
pub const SIMILARITY_INDEX_PAGE_BYTES: usize = 4_096;
pub const SIMILARITY_INDEX_ENTRY_BYTES: usize = 160;
pub const SIMILARITY_BUCKET_REFERENCE_BYTES: usize = 24;
pub const SIMILARITY_INDEX_ENTRIES_PER_PAGE: usize = 25;
pub const SIMILARITY_BUCKET_REFERENCES_PER_PAGE: usize = 167;
const PAGE_HEADER_BYTES: usize = 96;
const ENTRIES_PER_PAGE: usize = SIMILARITY_INDEX_ENTRIES_PER_PAGE;
const BUCKET_PAGE_HEADER_BYTES: usize = 80;
const BUCKET_REFERENCES_PER_PAGE: usize = SIMILARITY_BUCKET_REFERENCES_PER_PAGE;
const MAX_BUCKET_REPRESENTATIVES: usize = 64;
const FOOTER_BYTES: usize = 4_096;
const MAX_RUN_BYTES: usize = 1 << 30;
const FORMAT_VERSION: u16 = 2;
const HEADER_MAGIC: [u8; 8] = *b"FDSIRN02";
const PAGE_MAGIC: [u8; 8] = *b"FDSIPG02";
const BUCKET_PAGE_MAGIC: [u8; 8] = *b"FDSIBK02";
const FOOTER_MAGIC: [u8; 8] = *b"FDSIFT02";
const HEADER_CRC_OFFSET: usize = 192;
const RUN_HASH_OFFSET: usize = 192;
const FOOTER_CRC_OFFSET: usize = 224;
const PAGE_CRC_OFFSET: usize = 20;

/// One complete derived Similarity Fingerprint entry.
///
/// The entry proposes Base Chunks only. It is neither content identity nor
/// physical Location evidence; callers must pair its `ChunkId` with the Exact
/// Index before using a candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimilarityIndexEntry {
    chunk_id: ChunkId,
    logical_length: u32,
    fingerprint_profile: u16,
    superfeatures: [u64; 4],
    sketch: [u64; 8],
}

impl SimilarityIndexEntry {
    /// Constructs one field-validated derived entry.
    ///
    /// # Errors
    ///
    /// Rejects zero or oversized logical chunks and profile zero.
    pub fn new(
        chunk_id: ChunkId,
        logical_length: u32,
        fingerprint_profile: u16,
        superfeatures: [u64; 4],
        sketch: [u64; 8],
    ) -> Result<Self, SimilarityIndexFormatError> {
        if !valid_logical_length(logical_length) || fingerprint_profile == 0 {
            return Err(SimilarityIndexFormatError::InvalidEntry);
        }
        Ok(Self {
            chunk_id,
            logical_length,
            fingerprint_profile,
            superfeatures,
            sketch,
        })
    }

    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }

    #[must_use]
    pub const fn fingerprint_profile(self) -> u16 {
        self.fingerprint_profile
    }

    #[must_use]
    pub const fn superfeatures(self) -> [u64; 4] {
        self.superfeatures
    }

    #[must_use]
    pub const fn sketch(self) -> [u64; 8] {
        self.sketch
    }

    fn encode(self, output: &mut [u8]) {
        assert_eq!(
            output.len(),
            SIMILARITY_INDEX_ENTRY_BYTES,
            "ASSERT: Similarity entry encoder receives one fixed entry"
        );
        output[0..32].copy_from_slice(&self.chunk_id.bytes());
        put_u32(output, 32, self.logical_length);
        put_u16(output, 36, self.fingerprint_profile);
        for (ordinal, value) in self.superfeatures.into_iter().enumerate() {
            put_u64(output, 40 + ordinal * 8, value);
        }
        for (ordinal, value) in self.sketch.into_iter().enumerate() {
            put_u64(output, 72 + ordinal * 8, value);
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, SimilarityIndexFormatError> {
        if bytes.len() != SIMILARITY_INDEX_ENTRY_BYTES
            || bytes[38..40].iter().any(|byte| *byte != 0)
            || bytes[136..].iter().any(|byte| *byte != 0)
        {
            return Err(SimilarityIndexFormatError::InvalidEntry);
        }
        let mut chunk_id = [0_u8; 32];
        chunk_id.copy_from_slice(&bytes[0..32]);
        let mut superfeatures = [0_u64; 4];
        for (ordinal, value) in superfeatures.iter_mut().enumerate() {
            *value = get_u64(bytes, 40 + ordinal * 8);
        }
        let mut sketch = [0_u64; 8];
        for (ordinal, value) in sketch.iter_mut().enumerate() {
            *value = get_u64(bytes, 72 + ordinal * 8);
        }
        Self::new(
            ChunkId::from_bytes(chunk_id),
            get_u32(bytes, 32),
            get_u16(bytes, 36),
            superfeatures,
            sketch,
        )
    }
}

/// One versioned pool-wide Similarity bucket address.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SimilarityBucketKey {
    fingerprint_profile: u16,
    slot: u8,
    logical_length: u32,
    superfeature: u64,
}

impl SimilarityBucketKey {
    /// Constructs one field-validated bucket key.
    ///
    /// # Errors
    ///
    /// Rejects profile zero, slots outside `0..4`, and invalid lengths.
    pub fn new(
        fingerprint_profile: u16,
        slot: u8,
        logical_length: u32,
        superfeature: u64,
    ) -> Result<Self, SimilarityIndexFormatError> {
        if fingerprint_profile == 0 || slot >= 4 || !valid_logical_length(logical_length) {
            return Err(SimilarityIndexFormatError::InvalidBucketReference);
        }
        Ok(Self {
            fingerprint_profile,
            slot,
            logical_length,
            superfeature,
        })
    }

    #[must_use]
    pub const fn fingerprint_profile(self) -> u16 {
        self.fingerprint_profile
    }

    #[must_use]
    pub const fn slot(self) -> u8 {
        self.slot
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }

    #[must_use]
    pub const fn superfeature(self) -> u64 {
        self.superfeature
    }

    fn encode(self, output: &mut [u8]) {
        assert_eq!(
            output.len(),
            16,
            "ASSERT: Similarity bucket-key encoder receives one fixed key"
        );
        put_u16(output, 0, self.fingerprint_profile);
        output[2] = self.slot;
        output[3] = 0;
        put_u32(output, 4, self.logical_length);
        put_u64(output, 8, self.superfeature);
    }

    fn decode(bytes: &[u8]) -> Result<Self, SimilarityIndexFormatError> {
        if bytes.len() != 16 || bytes[3] != 0 {
            return Err(SimilarityIndexFormatError::InvalidBucketReference);
        }
        Self::new(
            get_u16(bytes, 0),
            bytes[2],
            get_u32(bytes, 4),
            get_u64(bytes, 8),
        )
    }
}

/// One compact reference from a sorted bucket to the Chunk-ID-sorted entry
/// arena. Entry ordinals preserve Chunk-ID ordering without duplicating the
/// 32-byte identity in every bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimilarityBucketReference {
    key: SimilarityBucketKey,
    entry_ordinal: u32,
}

impl SimilarityBucketReference {
    /// Constructs one reference from a validated bucket key and entry ordinal.
    #[must_use]
    pub const fn new(key: SimilarityBucketKey, entry_ordinal: u32) -> Self {
        Self { key, entry_ordinal }
    }

    #[must_use]
    pub const fn key(self) -> SimilarityBucketKey {
        self.key
    }

    #[must_use]
    pub const fn entry_ordinal(self) -> u32 {
        self.entry_ordinal
    }

    fn sort_key(self) -> (SimilarityBucketKey, u32) {
        (self.key, self.entry_ordinal)
    }

    fn encode(self, output: &mut [u8]) {
        assert_eq!(
            output.len(),
            SIMILARITY_BUCKET_REFERENCE_BYTES,
            "ASSERT: Similarity bucket-reference encoder receives one fixed reference"
        );
        self.key.encode(&mut output[..16]);
        put_u32(output, 16, self.entry_ordinal);
        output[20..24].fill(0);
    }

    fn decode(bytes: &[u8], entry_count: usize) -> Result<Self, SimilarityIndexFormatError> {
        if bytes.len() != SIMILARITY_BUCKET_REFERENCE_BYTES
            || bytes[20..24].iter().any(|byte| *byte != 0)
        {
            return Err(SimilarityIndexFormatError::InvalidBucketReference);
        }
        let entry_ordinal = get_u32(bytes, 16);
        if usize::try_from(entry_ordinal).map_or(true, |ordinal| ordinal >= entry_count) {
            return Err(SimilarityIndexFormatError::InvalidBucketReference);
        }
        Ok(Self {
            key: SimilarityBucketKey::decode(&bytes[..16])?,
            entry_ordinal,
        })
    }
}

/// One canonical immutable snapshot of the complete rebuildable Similarity
/// Index entry stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimilarityIndexRun {
    fingerprint_profile: u16,
    bucket_profile: u16,
    generation: u64,
    entries: Vec<SimilarityIndexEntry>,
    bucket_references: Vec<SimilarityBucketReference>,
    bucket_count: usize,
}

impl SimilarityIndexRun {
    /// Canonicalizes a complete Similarity snapshot by full `ChunkId`.
    ///
    /// # Errors
    ///
    /// Rejects empty snapshots, profile zero, generation zero, duplicate Chunk
    /// IDs, mixed fingerprint profiles, and objects above the format bound.
    pub fn new(
        fingerprint_profile: u16,
        bucket_profile: u16,
        generation: u64,
        mut entries: Vec<SimilarityIndexEntry>,
    ) -> Result<Self, SimilarityIndexFormatError> {
        if fingerprint_profile == 0 || bucket_profile == 0 {
            return Err(SimilarityIndexFormatError::UnsupportedProfile);
        }
        if generation == 0 {
            return Err(SimilarityIndexFormatError::InvalidGeneration);
        }
        entries.sort_unstable_by_key(|entry| entry.chunk_id);
        validate_entries(&entries, fingerprint_profile)?;
        let (bucket_references, bucket_count) = build_bucket_references(&entries)?;
        encoded_length(entries.len(), bucket_references.len())?;
        Ok(Self {
            fingerprint_profile,
            bucket_profile,
            generation,
            entries,
            bucket_references,
            bucket_count,
        })
    }

    #[must_use]
    pub const fn fingerprint_profile(&self) -> u16 {
        self.fingerprint_profile
    }

    #[must_use]
    pub const fn bucket_profile(&self) -> u16 {
        self.bucket_profile
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn entries(&self) -> &[SimilarityIndexEntry] {
        &self.entries
    }

    #[must_use]
    pub fn bucket_references(&self) -> &[SimilarityBucketReference] {
        &self.bucket_references
    }

    #[must_use]
    pub const fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    /// Returns exact geometry for an allocation-bounded second-pass writer.
    #[must_use]
    pub fn stream_layout(&self) -> SimilarityIndexRunLayout {
        SimilarityIndexRunLayout {
            identity: self.identity(),
        }
    }

    fn identity(&self) -> SimilarityRunIdentity {
        SimilarityRunIdentity {
            fingerprint_profile: self.fingerprint_profile,
            bucket_profile: self.bucket_profile,
            generation: self.generation,
            entry_count: self.entries.len(),
            bucket_count: self.bucket_count,
            bucket_reference_count: self.bucket_references.len(),
            key_bounds: key_bounds(&self.entries),
        }
    }

    /// Serializes the run field by field into 4 KiB independently checksummed
    /// pages and a complete-file BLAKE3 hash.
    ///
    /// # Errors
    ///
    /// Returns a geometry, canonical-order, allocation, or arithmetic error.
    ///
    /// # Panics
    ///
    /// Panics only if the streaming encoder's final descriptor disagrees with
    /// the exact byte count it emitted, which is an internal invariant.
    pub fn encode(&self) -> Result<Vec<u8>, SimilarityIndexFormatError> {
        validate_entries(&self.entries, self.fingerprint_profile)?;
        validate_bucket_references(
            &self.bucket_references,
            &self.entries,
            self.fingerprint_profile,
        )?;
        let mut encoder = SimilarityIndexRunStreamEncoder::new(self.stream_layout())?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(encoder.file_length())
            .map_err(|_| SimilarityIndexFormatError::OutOfMemory)?;
        bytes.extend_from_slice(encoder.header());
        for entries in self.entries.chunks(ENTRIES_PER_PAGE) {
            bytes.extend_from_slice(&encoder.encode_next_entry_page(entries)?);
        }
        for references in self.bucket_references.chunks(BUCKET_REFERENCES_PER_PAGE) {
            bytes.extend_from_slice(&encoder.encode_next_bucket_page(references)?);
        }
        let (footer, descriptor) = encoder.finish()?;
        bytes.extend_from_slice(&footer);
        assert_eq!(
            u64::try_from(bytes.len()).ok(),
            Some(descriptor.file_length()),
            "ASSERT: streamed Similarity image matches its encoded geometry"
        );
        Ok(bytes)
    }

    /// Fully validates and materializes one complete run.
    ///
    /// Normal pool rebuild should use [`SimilarityIndexRunDescriptor`] and
    /// decode one page at a time instead.
    ///
    /// # Errors
    ///
    /// Rejects invalid geometry, checksums, hashes, profiles, ordering,
    /// reserved bytes, or allocation beyond the format bound.
    ///
    /// # Panics
    ///
    /// Panics only if a previously verified descriptor produces an impossible
    /// in-range page offset. That condition is an internal format `ASSERT`.
    pub fn decode(bytes: &[u8]) -> Result<Self, SimilarityIndexFormatError> {
        if bytes.len() < SIMILARITY_INDEX_HEADER_BYTES + FOOTER_BYTES || bytes.len() > MAX_RUN_BYTES
        {
            return Err(SimilarityIndexFormatError::InvalidObjectLength(bytes.len()));
        }
        let footer_offset = bytes.len() - FOOTER_BYTES;
        let descriptor = SimilarityIndexRunDescriptor::decode(
            &bytes[..SIMILARITY_INDEX_HEADER_BYTES],
            &bytes[footer_offset..],
            u64_from_usize(bytes.len())?,
        )?;
        if calculate_run_hash(bytes, footer_offset) != descriptor.run_hash {
            return Err(SimilarityIndexFormatError::RunHashMismatch);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(descriptor.entry_count)
            .map_err(|_| SimilarityIndexFormatError::OutOfMemory)?;
        for ordinal in 0..descriptor.page_count {
            let offset = usize::try_from(
                descriptor
                    .page_offset(ordinal)
                    .expect("ASSERT: a validated Similarity page has an offset"),
            )
            .expect("ASSERT: format-v2 Similarity offsets fit usize");
            let page = descriptor.decode_page(
                ordinal,
                &bytes[offset..offset + SIMILARITY_INDEX_PAGE_BYTES],
            )?;
            entries.extend_from_slice(page.entries());
        }
        let mut bucket_references = Vec::new();
        bucket_references
            .try_reserve_exact(descriptor.bucket_reference_count)
            .map_err(|_| SimilarityIndexFormatError::OutOfMemory)?;
        for ordinal in 0..descriptor.bucket_page_count {
            let offset = usize::try_from(
                descriptor
                    .bucket_page_offset(ordinal)
                    .expect("ASSERT: a validated Similarity bucket page has an offset"),
            )
            .expect("ASSERT: format-v2 Similarity offsets fit usize");
            let page = descriptor.decode_bucket_page(
                ordinal,
                &bytes[offset..offset + SIMILARITY_INDEX_PAGE_BYTES],
            )?;
            bucket_references.extend_from_slice(page.references());
        }
        validate_entries(&entries, descriptor.fingerprint_profile)?;
        validate_bucket_references(&bucket_references, &entries, descriptor.fingerprint_profile)?;
        if entries.len() != descriptor.entry_count || key_bounds(&entries) != descriptor.key_bounds
        {
            return Err(SimilarityIndexFormatError::InvalidHeader);
        }
        let bucket_count = distinct_bucket_count(&bucket_references);
        if bucket_references.len() != descriptor.bucket_reference_count
            || bucket_count != descriptor.bucket_count
        {
            return Err(SimilarityIndexFormatError::InvalidHeader);
        }
        Ok(Self {
            fingerprint_profile: descriptor.fingerprint_profile,
            bucket_profile: descriptor.bucket_profile,
            generation: descriptor.generation,
            entries,
            bucket_references,
            bucket_count,
        })
    }
}

/// Exact geometry established before streaming one canonical Similarity Run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimilarityIndexRunLayout {
    identity: SimilarityRunIdentity,
}

impl SimilarityIndexRunLayout {
    /// Constructs validated geometry for a two-pass streaming writer.
    ///
    /// # Errors
    ///
    /// Rejects zero profiles or generation, empty cardinalities, reversed key
    /// bounds, impossible bucket counts, arithmetic overflow, or a run above
    /// the format object bound.
    pub fn new(
        fingerprint_profile: u16,
        bucket_profile: u16,
        generation: u64,
        entry_count: usize,
        bucket_count: usize,
        bucket_reference_count: usize,
        key_bounds: [ChunkId; 2],
    ) -> Result<Self, SimilarityIndexFormatError> {
        if fingerprint_profile == 0 || bucket_profile == 0 {
            return Err(SimilarityIndexFormatError::UnsupportedProfile);
        }
        if generation == 0 {
            return Err(SimilarityIndexFormatError::InvalidGeneration);
        }
        if entry_count == 0
            || entry_count > u32::MAX as usize
            || bucket_count == 0
            || bucket_count > bucket_reference_count
            || key_bounds[0] > key_bounds[1]
        {
            return Err(SimilarityIndexFormatError::InvalidHeader);
        }
        encoded_length(entry_count, bucket_reference_count)?;
        let mut encoded_bounds = [0_u8; 64];
        encoded_bounds[..32].copy_from_slice(&key_bounds[0].bytes());
        encoded_bounds[32..].copy_from_slice(&key_bounds[1].bytes());
        Ok(Self {
            identity: SimilarityRunIdentity {
                fingerprint_profile,
                bucket_profile,
                generation,
                entry_count,
                bucket_count,
                bucket_reference_count,
                key_bounds: encoded_bounds,
            },
        })
    }

    #[must_use]
    pub const fn entry_count(self) -> usize {
        self.identity.entry_count
    }

    #[must_use]
    pub const fn bucket_reference_count(self) -> usize {
        self.identity.bucket_reference_count
    }
}

/// Allocation-bounded encoder for one immutable Similarity Run.
///
/// A verified first pass supplies [`SimilarityIndexRunLayout`]. The second
/// pass emits complete Chunk-ID-sorted entry pages followed by complete
/// BucketKey/ordinal-sorted reference pages. Only one page and hash state are
/// retained by the encoder.
pub struct SimilarityIndexRunStreamEncoder {
    layout: SimilarityIndexRunLayout,
    geometry: RunGeometry,
    header: [u8; SIMILARITY_INDEX_HEADER_BYTES],
    hasher: blake3::Hasher,
    emitted_entries: usize,
    emitted_entry_pages: usize,
    previous_entry: Option<SimilarityIndexEntry>,
    emitted_bucket_references: usize,
    emitted_bucket_pages: usize,
    previous_bucket_reference: Option<SimilarityBucketReference>,
    observed_bucket_count: usize,
    current_bucket_key: Option<SimilarityBucketKey>,
    current_bucket_references: usize,
}

impl SimilarityIndexRunStreamEncoder {
    /// Starts a streaming writer from exact first-pass geometry.
    ///
    /// # Errors
    ///
    /// Returns an arithmetic or identity-block encoding failure.
    pub fn new(layout: SimilarityIndexRunLayout) -> Result<Self, SimilarityIndexFormatError> {
        let identity = layout.identity;
        let entry_pages = page_count(identity.entry_count);
        let bucket_pages = bucket_page_count(identity.bucket_reference_count);
        let bucket_offset = SIMILARITY_INDEX_HEADER_BYTES
            .checked_add(
                entry_pages
                    .checked_mul(SIMILARITY_INDEX_PAGE_BYTES)
                    .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
            )
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        let footer_offset = bucket_offset
            .checked_add(
                bucket_pages
                    .checked_mul(SIMILARITY_INDEX_PAGE_BYTES)
                    .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
            )
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        let geometry = RunGeometry {
            entry_pages,
            bucket_pages,
            bucket_offset,
            footer_offset,
            file_length: encoded_length(identity.entry_count, identity.bucket_reference_count)?,
        };
        let mut header = [0_u8; SIMILARITY_INDEX_HEADER_BYTES];
        encode_identity_block(&mut header, HEADER_MAGIC, identity, geometry)?;
        let header_crc = crc32c_with_zeroed_u32(&header, HEADER_CRC_OFFSET);
        put_u32(&mut header, HEADER_CRC_OFFSET, header_crc);
        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        Ok(Self {
            layout,
            geometry,
            header,
            hasher,
            emitted_entries: 0,
            emitted_entry_pages: 0,
            previous_entry: None,
            emitted_bucket_references: 0,
            emitted_bucket_pages: 0,
            previous_bucket_reference: None,
            observed_bucket_count: 0,
            current_bucket_key: None,
            current_bucket_references: 0,
        })
    }

    #[must_use]
    pub const fn header(&self) -> &[u8; SIMILARITY_INDEX_HEADER_BYTES] {
        &self.header
    }

    #[must_use]
    pub const fn file_length(&self) -> usize {
        self.geometry.file_length
    }

    /// Encodes the next complete entry page.
    ///
    /// # Errors
    ///
    /// Rejects a partial nonfinal page, excess output, mixed profiles,
    /// duplicate or reordered Chunk IDs, or key-bound disagreement.
    pub fn encode_next_entry_page(
        &mut self,
        entries: &[SimilarityIndexEntry],
    ) -> Result<[u8; SIMILARITY_INDEX_PAGE_BYTES], SimilarityIndexFormatError> {
        let identity = self.layout.identity;
        let remaining = identity
            .entry_count
            .checked_sub(self.emitted_entries)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        let expected = remaining.min(ENTRIES_PER_PAGE);
        if self.emitted_entry_pages >= self.geometry.entry_pages
            || entries.len() != expected
            || expected == 0
            || self.emitted_bucket_pages != 0
        {
            return Err(SimilarityIndexFormatError::InvalidPage);
        }
        validate_entries(entries, identity.fingerprint_profile)?;
        if self
            .previous_entry
            .is_some_and(|previous| previous.chunk_id >= entries[0].chunk_id)
        {
            return Err(SimilarityIndexFormatError::NonCanonicalOrder);
        }
        let mut minimum = [0_u8; 32];
        minimum.copy_from_slice(&identity.key_bounds[..32]);
        let minimum = ChunkId::from_bytes(minimum);
        let mut maximum = [0_u8; 32];
        maximum.copy_from_slice(&identity.key_bounds[32..]);
        let maximum = ChunkId::from_bytes(maximum);
        if (self.emitted_entry_pages == 0 && entries[0].chunk_id != minimum)
            || (self.emitted_entry_pages + 1 == self.geometry.entry_pages
                && entries.last().is_none_or(|entry| entry.chunk_id != maximum))
        {
            return Err(SimilarityIndexFormatError::InvalidPage);
        }
        let mut page = [0_u8; SIMILARITY_INDEX_PAGE_BYTES];
        encode_page(&mut page, self.emitted_entry_pages, entries)?;
        self.hasher.update(&page);
        self.previous_entry = entries.last().copied();
        self.emitted_entries = self
            .emitted_entries
            .checked_add(entries.len())
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        self.emitted_entry_pages = self
            .emitted_entry_pages
            .checked_add(1)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        Ok(page)
    }

    /// Encodes the next complete bucket-reference page.
    ///
    /// # Errors
    ///
    /// Rejects output before all entries, partial nonfinal pages, invalid
    /// ordinals, reordered references, mixed profiles, or oversized buckets.
    pub fn encode_next_bucket_page(
        &mut self,
        references: &[SimilarityBucketReference],
    ) -> Result<[u8; SIMILARITY_INDEX_PAGE_BYTES], SimilarityIndexFormatError> {
        let identity = self.layout.identity;
        let remaining = identity
            .bucket_reference_count
            .checked_sub(self.emitted_bucket_references)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        let expected = remaining.min(BUCKET_REFERENCES_PER_PAGE);
        if self.emitted_entries != identity.entry_count
            || self.emitted_bucket_pages >= self.geometry.bucket_pages
            || references.len() != expected
            || expected == 0
        {
            return Err(SimilarityIndexFormatError::InvalidBucketPage);
        }
        for reference in references {
            if reference.key.fingerprint_profile != identity.fingerprint_profile
                || usize::try_from(reference.entry_ordinal)
                    .map_or(true, |ordinal| ordinal >= identity.entry_count)
                || self
                    .previous_bucket_reference
                    .is_some_and(|previous| previous.sort_key() >= reference.sort_key())
            {
                return Err(SimilarityIndexFormatError::InvalidBucketReference);
            }
            if self.current_bucket_key != Some(reference.key) {
                self.current_bucket_key = Some(reference.key);
                self.current_bucket_references = 0;
                self.observed_bucket_count = self
                    .observed_bucket_count
                    .checked_add(1)
                    .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
            }
            self.current_bucket_references = self
                .current_bucket_references
                .checked_add(1)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
            if self.current_bucket_references > MAX_BUCKET_REPRESENTATIVES {
                return Err(SimilarityIndexFormatError::InvalidBucketReference);
            }
            self.previous_bucket_reference = Some(*reference);
        }
        let mut page = [0_u8; SIMILARITY_INDEX_PAGE_BYTES];
        encode_bucket_page(&mut page, self.emitted_bucket_pages, references)?;
        self.hasher.update(&page);
        self.emitted_bucket_references = self
            .emitted_bucket_references
            .checked_add(references.len())
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        self.emitted_bucket_pages = self
            .emitted_bucket_pages
            .checked_add(1)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        Ok(page)
    }

    /// Finishes the Footer and returns the verified descriptor.
    ///
    /// # Errors
    ///
    /// Rejects incomplete output, cardinality disagreement, or an invalid
    /// writer-generated Header/Footer pair.
    pub fn finish(
        mut self,
    ) -> Result<
        (
            [u8; SIMILARITY_INDEX_PAGE_BYTES],
            SimilarityIndexRunDescriptor,
        ),
        SimilarityIndexFormatError,
    > {
        let identity = self.layout.identity;
        if self.emitted_entries != identity.entry_count
            || self.emitted_entry_pages != self.geometry.entry_pages
            || self.emitted_bucket_references != identity.bucket_reference_count
            || self.emitted_bucket_pages != self.geometry.bucket_pages
            || self.observed_bucket_count != identity.bucket_count
        {
            return Err(SimilarityIndexFormatError::NonSequentialAudit);
        }
        let mut footer = [0_u8; SIMILARITY_INDEX_PAGE_BYTES];
        encode_identity_block(&mut footer, FOOTER_MAGIC, identity, self.geometry)?;
        self.hasher.update(&footer);
        let run_hash = *self.hasher.finalize().as_bytes();
        footer[RUN_HASH_OFFSET..RUN_HASH_OFFSET + 32].copy_from_slice(&run_hash);
        let footer_crc = crc32c_with_zeroed_u32(&footer, FOOTER_CRC_OFFSET);
        put_u32(&mut footer, FOOTER_CRC_OFFSET, footer_crc);
        let descriptor = SimilarityIndexRunDescriptor::decode(
            &self.header,
            &footer,
            u64_from_usize(self.geometry.file_length)?,
        )?;
        Ok((footer, descriptor))
    }
}

/// Header and Footer proof used to stream fixed 4 KiB Similarity pages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimilarityIndexRunDescriptor {
    fingerprint_profile: u16,
    bucket_profile: u16,
    generation: u64,
    entry_count: usize,
    page_count: usize,
    bucket_count: usize,
    bucket_reference_count: usize,
    bucket_page_count: usize,
    bucket_offset: usize,
    footer_offset: usize,
    file_length: usize,
    key_bounds: [u8; 64],
    run_hash: [u8; 32],
}

impl SimilarityIndexRunDescriptor {
    /// Verifies independently read Header and Footer blocks.
    ///
    /// # Errors
    ///
    /// Rejects invalid checksums, profiles, geometry, identity disagreement,
    /// reserved bytes, or an object outside the format bound.
    pub fn decode(
        header: &[u8],
        footer: &[u8],
        physical_length: u64,
    ) -> Result<Self, SimilarityIndexFormatError> {
        validate_identity_block(header, HEADER_MAGIC, HEADER_CRC_OFFSET)?;
        validate_identity_block(footer, FOOTER_MAGIC, FOOTER_CRC_OFFSET)?;
        if footer[8..176] != header[8..176] || footer[228..].iter().any(|byte| *byte != 0) {
            return Err(SimilarityIndexFormatError::HeaderFooterMismatch);
        }
        let fingerprint_profile = get_u16(header, 16);
        let bucket_profile = get_u16(header, 18);
        let generation = get_u64(header, 24);
        if fingerprint_profile == 0 || bucket_profile == 0 {
            return Err(SimilarityIndexFormatError::UnsupportedProfile);
        }
        if generation == 0 {
            return Err(SimilarityIndexFormatError::InvalidGeneration);
        }
        let entry_count = usize_from_u64(get_u64(header, 32))?;
        let expected_pages = page_count(entry_count);
        let bucket_count = usize_from_u64(get_u64(header, 136))?;
        let bucket_reference_count = usize_from_u64(get_u64(header, 144))?;
        let expected_bucket_pages = bucket_page_count(bucket_reference_count);
        let bucket_offset = SIMILARITY_INDEX_HEADER_BYTES
            .checked_add(
                expected_pages
                    .checked_mul(SIMILARITY_INDEX_PAGE_BYTES)
                    .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
            )
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        let footer_offset = bucket_offset
            .checked_add(
                expected_bucket_pages
                    .checked_mul(SIMILARITY_INDEX_PAGE_BYTES)
                    .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
            )
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        let expected_length = encoded_length(entry_count, bucket_reference_count)?;
        let physical_length = usize_from_u64(physical_length)?;
        if physical_length != expected_length
            || usize_from_u64(get_u64(header, 40))? != expected_pages
            || get_u64(header, 112) != u64_from_usize(SIMILARITY_INDEX_HEADER_BYTES)?
            || get_u64(header, 120) != u64_from_usize(footer_offset)?
            || get_u64(header, 128) != u64_from_usize(expected_length)?
            || usize_from_u64(get_u64(header, 152))? != expected_bucket_pages
            || get_u64(header, 160) != u64_from_usize(bucket_offset)?
            || bucket_count == 0
            || bucket_count > bucket_reference_count
        {
            return Err(SimilarityIndexFormatError::InvalidHeader);
        }
        let mut key_bounds = [0_u8; 64];
        key_bounds.copy_from_slice(&header[48..112]);
        if key_bounds[..32] > key_bounds[32..] {
            return Err(SimilarityIndexFormatError::InvalidHeader);
        }
        let mut run_hash = [0_u8; 32];
        run_hash.copy_from_slice(&footer[RUN_HASH_OFFSET..RUN_HASH_OFFSET + 32]);
        Ok(Self {
            fingerprint_profile,
            bucket_profile,
            generation,
            entry_count,
            page_count: expected_pages,
            bucket_count,
            bucket_reference_count,
            bucket_page_count: expected_bucket_pages,
            bucket_offset,
            footer_offset,
            file_length: expected_length,
            key_bounds,
            run_hash,
        })
    }

    #[must_use]
    pub const fn fingerprint_profile(self) -> u16 {
        self.fingerprint_profile
    }

    #[must_use]
    pub const fn bucket_profile(self) -> u16 {
        self.bucket_profile
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn entry_count(self) -> usize {
        self.entry_count
    }

    #[must_use]
    pub const fn page_count(self) -> usize {
        self.page_count
    }

    #[must_use]
    pub const fn bucket_count(self) -> usize {
        self.bucket_count
    }

    #[must_use]
    pub const fn bucket_reference_count(self) -> usize {
        self.bucket_reference_count
    }

    #[must_use]
    pub const fn bucket_page_count(self) -> usize {
        self.bucket_page_count
    }

    #[must_use]
    pub const fn footer_offset(self) -> u64 {
        self.footer_offset as u64
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length as u64
    }

    #[must_use]
    pub const fn run_hash(self) -> [u8; 32] {
        self.run_hash
    }

    #[must_use]
    pub fn minimum_chunk_id(self) -> ChunkId {
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(&self.key_bounds[..32]);
        ChunkId::from_bytes(bytes)
    }

    #[must_use]
    pub fn maximum_chunk_id(self) -> ChunkId {
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(&self.key_bounds[32..]);
        ChunkId::from_bytes(bytes)
    }

    #[must_use]
    pub fn page_offset(self, ordinal: usize) -> Option<u64> {
        if ordinal >= self.page_count {
            return None;
        }
        SIMILARITY_INDEX_HEADER_BYTES
            .checked_add(ordinal.checked_mul(SIMILARITY_INDEX_PAGE_BYTES)?)
            .and_then(|offset| u64::try_from(offset).ok())
    }

    #[must_use]
    pub fn bucket_page_offset(self, ordinal: usize) -> Option<u64> {
        if ordinal >= self.bucket_page_count {
            return None;
        }
        self.bucket_offset
            .checked_add(ordinal.checked_mul(SIMILARITY_INDEX_PAGE_BYTES)?)
            .and_then(|offset| u64::try_from(offset).ok())
    }

    /// Decodes one independently checksummed page without retaining the run.
    ///
    /// # Errors
    ///
    /// Rejects an out-of-range ordinal, page corruption, mixed profiles,
    /// noncanonical entries, or run key-bound disagreement.
    pub fn decode_page(
        self,
        ordinal: usize,
        bytes: &[u8],
    ) -> Result<SimilarityIndexPage, SimilarityIndexFormatError> {
        if ordinal >= self.page_count {
            return Err(SimilarityIndexFormatError::InvalidPage);
        }
        let entries = decode_page(bytes, ordinal, self.entry_count, self.fingerprint_profile)?;
        let first = entries
            .first()
            .ok_or(SimilarityIndexFormatError::InvalidPage)?;
        let last = entries
            .last()
            .ok_or(SimilarityIndexFormatError::InvalidPage)?;
        let mut minimum = [0_u8; 32];
        minimum.copy_from_slice(&self.key_bounds[..32]);
        let mut maximum = [0_u8; 32];
        maximum.copy_from_slice(&self.key_bounds[32..]);
        if (ordinal == 0 && first.chunk_id != ChunkId::from_bytes(minimum))
            || (ordinal + 1 == self.page_count && last.chunk_id != ChunkId::from_bytes(maximum))
        {
            return Err(SimilarityIndexFormatError::InvalidPage);
        }
        Ok(SimilarityIndexPage { ordinal, entries })
    }

    /// Decodes one independently checksummed, key-sorted bucket page.
    ///
    /// # Errors
    ///
    /// Rejects an out-of-range ordinal, invalid page geometry, checksum
    /// failure, noncanonical references, or a profile mismatch.
    pub fn decode_bucket_page(
        self,
        ordinal: usize,
        bytes: &[u8],
    ) -> Result<SimilarityBucketPage, SimilarityIndexFormatError> {
        if ordinal >= self.bucket_page_count {
            return Err(SimilarityIndexFormatError::InvalidBucketPage);
        }
        let references = decode_bucket_page(
            bytes,
            ordinal,
            self.bucket_reference_count,
            self.entry_count,
            self.fingerprint_profile,
        )?;
        Ok(SimilarityBucketPage {
            ordinal,
            references,
        })
    }

    #[must_use]
    pub fn start_hash_audit(self) -> SimilarityIndexRunHashAudit {
        SimilarityIndexRunHashAudit {
            descriptor: self,
            hasher: blake3::Hasher::new(),
            next_offset: 0,
            pages_verified: 0,
            previous_entry: None,
            bucket_pages_verified: 0,
            previous_bucket_reference: None,
            bucket_count_verified: 0,
            bucket_references_verified: 0,
            current_bucket_key: None,
            current_bucket_references: 0,
        }
    }
}

/// One independently checksummed Similarity page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimilarityIndexPage {
    ordinal: usize,
    entries: Vec<SimilarityIndexEntry>,
}

impl SimilarityIndexPage {
    #[must_use]
    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }

    #[must_use]
    pub fn entries(&self) -> &[SimilarityIndexEntry] {
        &self.entries
    }
}

/// One independently checksummed sorted bucket-reference page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimilarityBucketPage {
    ordinal: usize,
    references: Vec<SimilarityBucketReference>,
}

impl SimilarityBucketPage {
    #[must_use]
    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }

    #[must_use]
    pub fn references(&self) -> &[SimilarityBucketReference] {
        &self.references
    }

    #[must_use]
    /// # Panics
    ///
    /// Panics only if an internal caller constructs an empty verified page.
    pub fn first_key(&self) -> SimilarityBucketKey {
        self.references
            .first()
            .expect("ASSERT: verified Similarity bucket pages are nonempty")
            .key
    }

    #[must_use]
    /// # Panics
    ///
    /// Panics only if an internal caller constructs an empty verified page.
    pub fn last_key(&self) -> SimilarityBucketKey {
        self.references
            .last()
            .expect("ASSERT: verified Similarity bucket pages are nonempty")
            .key
    }
}

/// Sequential complete-file hash and cross-page order verifier.
pub struct SimilarityIndexRunHashAudit {
    descriptor: SimilarityIndexRunDescriptor,
    hasher: blake3::Hasher,
    next_offset: usize,
    pages_verified: usize,
    previous_entry: Option<SimilarityIndexEntry>,
    bucket_pages_verified: usize,
    previous_bucket_reference: Option<SimilarityBucketReference>,
    bucket_count_verified: usize,
    bucket_references_verified: usize,
    current_bucket_key: Option<SimilarityBucketKey>,
    current_bucket_references: usize,
}

impl SimilarityIndexRunHashAudit {
    /// Adds the next exact physical byte range to the complete-file hash.
    ///
    /// # Errors
    ///
    /// Rejects nonsequential, overlapping, overflowing, or out-of-file input.
    pub fn update(&mut self, offset: u64, bytes: &[u8]) -> Result<(), SimilarityIndexFormatError> {
        let offset =
            usize::try_from(offset).map_err(|_| SimilarityIndexFormatError::NonSequentialAudit)?;
        if offset != self.next_offset {
            return Err(SimilarityIndexFormatError::NonSequentialAudit);
        }
        let end = offset
            .checked_add(bytes.len())
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        if end > self.descriptor.file_length {
            return Err(SimilarityIndexFormatError::NonSequentialAudit);
        }
        let zero_start = self
            .descriptor
            .footer_offset
            .checked_add(RUN_HASH_OFFSET)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        let zero_end = self
            .descriptor
            .footer_offset
            .checked_add(FOOTER_CRC_OFFSET + 4)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        let overlap_start = offset.max(zero_start).min(end);
        let overlap_end = end.min(zero_end).max(offset);
        if overlap_start < overlap_end {
            let prefix = overlap_start - offset;
            let suffix = overlap_end - offset;
            self.hasher.update(&bytes[..prefix]);
            self.hasher
                .update(&[0_u8; 36][..overlap_end - overlap_start]);
            self.hasher.update(&bytes[suffix..]);
        } else {
            self.hasher.update(bytes);
        }
        self.next_offset = end;
        Ok(())
    }

    /// Pairs one decoded page with the prior page's final entry.
    ///
    /// # Errors
    ///
    /// Rejects skipped pages and cross-page key reversal or duplication.
    ///
    /// # Panics
    ///
    /// Panics only if a caller forges an empty `SimilarityIndexPage`; the
    /// public page decoder rejects that impossible internal state.
    pub fn verify_page(
        &mut self,
        page: &SimilarityIndexPage,
    ) -> Result<(), SimilarityIndexFormatError> {
        if page.ordinal != self.pages_verified {
            return Err(SimilarityIndexFormatError::InvalidPage);
        }
        let first = page
            .entries
            .first()
            .expect("ASSERT: verified Similarity pages are nonempty");
        if self
            .previous_entry
            .is_some_and(|previous| previous.chunk_id >= first.chunk_id)
        {
            return Err(SimilarityIndexFormatError::NonCanonicalOrder);
        }
        self.previous_entry = page.entries.last().copied();
        self.pages_verified = self
            .pages_verified
            .checked_add(1)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        Ok(())
    }

    /// Pairs one bucket page with the prior page's final sort key.
    ///
    /// # Errors
    ///
    /// Rejects skipped pages, cross-page ordering failures, overflow, or a
    /// bucket exceeding its versioned representative bound.
    ///
    /// # Panics
    ///
    /// Panics only if an internal caller forges an empty verified page.
    pub fn verify_bucket_page(
        &mut self,
        page: &SimilarityBucketPage,
    ) -> Result<(), SimilarityIndexFormatError> {
        if page.ordinal != self.bucket_pages_verified {
            return Err(SimilarityIndexFormatError::InvalidBucketPage);
        }
        let first = page
            .references
            .first()
            .expect("ASSERT: verified Similarity bucket pages are nonempty");
        if self
            .previous_bucket_reference
            .is_some_and(|previous| previous.sort_key() >= first.sort_key())
        {
            return Err(SimilarityIndexFormatError::NonCanonicalOrder);
        }
        for reference in &page.references {
            if self.current_bucket_key != Some(reference.key) {
                self.current_bucket_key = Some(reference.key);
                self.current_bucket_references = 0;
                self.bucket_count_verified = self
                    .bucket_count_verified
                    .checked_add(1)
                    .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
            }
            self.current_bucket_references = self
                .current_bucket_references
                .checked_add(1)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
            self.bucket_references_verified = self
                .bucket_references_verified
                .checked_add(1)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
            if self.current_bucket_references > MAX_BUCKET_REPRESENTATIVES {
                return Err(SimilarityIndexFormatError::InvalidBucketReference);
            }
        }
        self.previous_bucket_reference = page.references.last().copied();
        self.bucket_pages_verified = self
            .bucket_pages_verified
            .checked_add(1)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        Ok(())
    }

    /// Completes the audit after every physical byte and page was observed.
    ///
    /// # Errors
    ///
    /// Rejects incomplete input, a missing page, or a run-hash mismatch.
    pub fn finish(self) -> Result<(), SimilarityIndexFormatError> {
        if self.next_offset != self.descriptor.file_length
            || self.pages_verified != self.descriptor.page_count
            || self.bucket_pages_verified != self.descriptor.bucket_page_count
            || self.bucket_count_verified != self.descriptor.bucket_count
            || self.bucket_references_verified != self.descriptor.bucket_reference_count
        {
            return Err(SimilarityIndexFormatError::NonSequentialAudit);
        }
        if self.hasher.finalize().as_bytes() != &self.descriptor.run_hash {
            return Err(SimilarityIndexFormatError::RunHashMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct RunGeometry {
    entry_pages: usize,
    bucket_pages: usize,
    bucket_offset: usize,
    footer_offset: usize,
    file_length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SimilarityRunIdentity {
    fingerprint_profile: u16,
    bucket_profile: u16,
    generation: u64,
    entry_count: usize,
    bucket_count: usize,
    bucket_reference_count: usize,
    key_bounds: [u8; 64],
}

fn encode_identity_block(
    block: &mut [u8],
    magic: [u8; 8],
    identity: SimilarityRunIdentity,
    geometry: RunGeometry,
) -> Result<(), SimilarityIndexFormatError> {
    block[0..8].copy_from_slice(&magic);
    put_u16(block, 8, FORMAT_VERSION);
    put_u16(block, 10, 4_096);
    put_u16(block, 12, 4_096);
    put_u16(block, 14, 160);
    put_u16(block, 16, identity.fingerprint_profile);
    put_u16(block, 18, identity.bucket_profile);
    put_u32(block, 20, 25);
    put_u64(block, 24, identity.generation);
    put_u64(block, 32, u64_from_usize(identity.entry_count)?);
    put_u64(block, 40, u64_from_usize(geometry.entry_pages)?);
    block[48..112].copy_from_slice(&identity.key_bounds);
    put_u64(block, 112, u64_from_usize(SIMILARITY_INDEX_HEADER_BYTES)?);
    put_u64(block, 120, u64_from_usize(geometry.footer_offset)?);
    put_u64(block, 128, u64_from_usize(geometry.file_length)?);
    put_u64(block, 136, u64_from_usize(identity.bucket_count)?);
    put_u64(block, 144, u64_from_usize(identity.bucket_reference_count)?);
    put_u64(block, 152, u64_from_usize(geometry.bucket_pages)?);
    put_u64(block, 160, u64_from_usize(geometry.bucket_offset)?);
    Ok(())
}

fn validate_identity_block(
    block: &[u8],
    magic: [u8; 8],
    crc_offset: usize,
) -> Result<(), SimilarityIndexFormatError> {
    if block.len() != SIMILARITY_INDEX_HEADER_BYTES
        || block[0..8] != magic
        || get_u16(block, 8) != FORMAT_VERSION
        || usize::from(get_u16(block, 10)) != SIMILARITY_INDEX_HEADER_BYTES
        || usize::from(get_u16(block, 12)) != SIMILARITY_INDEX_PAGE_BYTES
        || usize::from(get_u16(block, 14)) != SIMILARITY_INDEX_ENTRY_BYTES
        || usize::try_from(get_u32(block, 20)).ok() != Some(ENTRIES_PER_PAGE)
        || block[168..192].iter().any(|byte| *byte != 0)
        || crc32c_with_zeroed_u32(block, crc_offset) != get_u32(block, crc_offset)
    {
        return Err(SimilarityIndexFormatError::InvalidHeader);
    }
    if magic == HEADER_MAGIC && block[196..].iter().any(|byte| *byte != 0) {
        return Err(SimilarityIndexFormatError::InvalidHeader);
    }
    Ok(())
}

fn encode_page(
    page: &mut [u8],
    ordinal: usize,
    entries: &[SimilarityIndexEntry],
) -> Result<(), SimilarityIndexFormatError> {
    let first = entries
        .first()
        .ok_or(SimilarityIndexFormatError::InvalidPage)?;
    let last = entries
        .last()
        .ok_or(SimilarityIndexFormatError::InvalidPage)?;
    page[0..8].copy_from_slice(&PAGE_MAGIC);
    put_u16(page, 8, FORMAT_VERSION);
    put_u16(page, 10, 96);
    put_u16(page, 12, 160);
    put_u16(
        page,
        14,
        u16::try_from(entries.len()).map_err(|_| SimilarityIndexFormatError::ArithmeticOverflow)?,
    );
    put_u32(
        page,
        16,
        u32::try_from(ordinal).map_err(|_| SimilarityIndexFormatError::ArithmeticOverflow)?,
    );
    put_u64(
        page,
        24,
        u64_from_usize(
            ordinal
                .checked_mul(ENTRIES_PER_PAGE)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
        )?,
    );
    page[32..64].copy_from_slice(&first.chunk_id.bytes());
    page[64..96].copy_from_slice(&last.chunk_id.bytes());
    for (index, entry) in entries.iter().copied().enumerate() {
        let start = PAGE_HEADER_BYTES + index * SIMILARITY_INDEX_ENTRY_BYTES;
        entry.encode(&mut page[start..start + SIMILARITY_INDEX_ENTRY_BYTES]);
    }
    let crc = crc32c_with_zeroed_u32(page, PAGE_CRC_OFFSET);
    put_u32(page, PAGE_CRC_OFFSET, crc);
    Ok(())
}

fn decode_page(
    page: &[u8],
    ordinal: usize,
    total_entries: usize,
    fingerprint_profile: u16,
) -> Result<Vec<SimilarityIndexEntry>, SimilarityIndexFormatError> {
    let remaining = total_entries.saturating_sub(
        ordinal
            .checked_mul(ENTRIES_PER_PAGE)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
    );
    let expected = remaining.min(ENTRIES_PER_PAGE);
    if page.len() != SIMILARITY_INDEX_PAGE_BYTES
        || page[0..8] != PAGE_MAGIC
        || get_u16(page, 8) != FORMAT_VERSION
        || usize::from(get_u16(page, 10)) != PAGE_HEADER_BYTES
        || usize::from(get_u16(page, 12)) != SIMILARITY_INDEX_ENTRY_BYTES
        || usize::from(get_u16(page, 14)) != expected
        || usize::try_from(get_u32(page, 16)).ok() != Some(ordinal)
        || usize_from_u64(get_u64(page, 24))?
            != ordinal
                .checked_mul(ENTRIES_PER_PAGE)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?
        || crc32c_with_zeroed_u32(page, PAGE_CRC_OFFSET) != get_u32(page, PAGE_CRC_OFFSET)
    {
        return Err(SimilarityIndexFormatError::InvalidPage);
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(expected)
        .map_err(|_| SimilarityIndexFormatError::OutOfMemory)?;
    for index in 0..expected {
        let start = PAGE_HEADER_BYTES + index * SIMILARITY_INDEX_ENTRY_BYTES;
        let entry =
            SimilarityIndexEntry::decode(&page[start..start + SIMILARITY_INDEX_ENTRY_BYTES])?;
        if entry.fingerprint_profile != fingerprint_profile {
            return Err(SimilarityIndexFormatError::UnsupportedProfile);
        }
        entries.push(entry);
    }
    if page[PAGE_HEADER_BYTES + expected * SIMILARITY_INDEX_ENTRY_BYTES..]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(SimilarityIndexFormatError::InvalidPage);
    }
    validate_entries(&entries, fingerprint_profile)?;
    let first = entries
        .first()
        .ok_or(SimilarityIndexFormatError::InvalidPage)?;
    let last = entries
        .last()
        .ok_or(SimilarityIndexFormatError::InvalidPage)?;
    if page[32..64] != first.chunk_id.bytes() || page[64..96] != last.chunk_id.bytes() {
        return Err(SimilarityIndexFormatError::InvalidPage);
    }
    Ok(entries)
}

fn encode_bucket_page(
    page: &mut [u8],
    ordinal: usize,
    references: &[SimilarityBucketReference],
) -> Result<(), SimilarityIndexFormatError> {
    let first = references
        .first()
        .ok_or(SimilarityIndexFormatError::InvalidBucketPage)?;
    let last = references
        .last()
        .ok_or(SimilarityIndexFormatError::InvalidBucketPage)?;
    page[0..8].copy_from_slice(&BUCKET_PAGE_MAGIC);
    put_u16(page, 8, FORMAT_VERSION);
    put_u16(page, 10, 80);
    put_u16(page, 12, 24);
    put_u16(
        page,
        14,
        u16::try_from(references.len())
            .map_err(|_| SimilarityIndexFormatError::ArithmeticOverflow)?,
    );
    put_u32(
        page,
        16,
        u32::try_from(ordinal).map_err(|_| SimilarityIndexFormatError::ArithmeticOverflow)?,
    );
    first.key.encode(&mut page[24..40]);
    put_u32(page, 40, first.entry_ordinal);
    last.key.encode(&mut page[48..64]);
    put_u32(page, 64, last.entry_ordinal);
    put_u64(
        page,
        68,
        u64_from_usize(
            ordinal
                .checked_mul(BUCKET_REFERENCES_PER_PAGE)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
        )?,
    );
    for (index, reference) in references.iter().copied().enumerate() {
        let start = BUCKET_PAGE_HEADER_BYTES + index * SIMILARITY_BUCKET_REFERENCE_BYTES;
        reference.encode(&mut page[start..start + SIMILARITY_BUCKET_REFERENCE_BYTES]);
    }
    let crc = crc32c_with_zeroed_u32(page, PAGE_CRC_OFFSET);
    put_u32(page, PAGE_CRC_OFFSET, crc);
    Ok(())
}

fn decode_bucket_page(
    page: &[u8],
    ordinal: usize,
    total_references: usize,
    entry_count: usize,
    fingerprint_profile: u16,
) -> Result<Vec<SimilarityBucketReference>, SimilarityIndexFormatError> {
    let remaining = total_references.saturating_sub(
        ordinal
            .checked_mul(BUCKET_REFERENCES_PER_PAGE)
            .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
    );
    let expected = remaining.min(BUCKET_REFERENCES_PER_PAGE);
    if page.len() != SIMILARITY_INDEX_PAGE_BYTES
        || page[0..8] != BUCKET_PAGE_MAGIC
        || get_u16(page, 8) != FORMAT_VERSION
        || usize::from(get_u16(page, 10)) != BUCKET_PAGE_HEADER_BYTES
        || usize::from(get_u16(page, 12)) != SIMILARITY_BUCKET_REFERENCE_BYTES
        || usize::from(get_u16(page, 14)) != expected
        || usize::try_from(get_u32(page, 16)).ok() != Some(ordinal)
        || page[44..48].iter().any(|byte| *byte != 0)
        || page[76..80].iter().any(|byte| *byte != 0)
        || usize_from_u64(get_u64(page, 68))?
            != ordinal
                .checked_mul(BUCKET_REFERENCES_PER_PAGE)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?
        || crc32c_with_zeroed_u32(page, PAGE_CRC_OFFSET) != get_u32(page, PAGE_CRC_OFFSET)
    {
        return Err(SimilarityIndexFormatError::InvalidBucketPage);
    }
    let mut references = Vec::new();
    references
        .try_reserve_exact(expected)
        .map_err(|_| SimilarityIndexFormatError::OutOfMemory)?;
    for index in 0..expected {
        let start = BUCKET_PAGE_HEADER_BYTES + index * SIMILARITY_BUCKET_REFERENCE_BYTES;
        let reference = SimilarityBucketReference::decode(
            &page[start..start + SIMILARITY_BUCKET_REFERENCE_BYTES],
            entry_count,
        )?;
        if reference.key.fingerprint_profile != fingerprint_profile {
            return Err(SimilarityIndexFormatError::UnsupportedProfile);
        }
        references.push(reference);
    }
    if page[BUCKET_PAGE_HEADER_BYTES + expected * SIMILARITY_BUCKET_REFERENCE_BYTES..]
        .iter()
        .any(|byte| *byte != 0)
        || !references
            .windows(2)
            .all(|pair| pair[0].sort_key() < pair[1].sort_key())
    {
        return Err(SimilarityIndexFormatError::InvalidBucketPage);
    }
    let first = references
        .first()
        .ok_or(SimilarityIndexFormatError::InvalidBucketPage)?;
    let last = references
        .last()
        .ok_or(SimilarityIndexFormatError::InvalidBucketPage)?;
    if page[24..40] != encoded_bucket_key(first.key)
        || get_u32(page, 40) != first.entry_ordinal
        || page[48..64] != encoded_bucket_key(last.key)
        || get_u32(page, 64) != last.entry_ordinal
    {
        return Err(SimilarityIndexFormatError::InvalidBucketPage);
    }
    Ok(references)
}

fn encoded_bucket_key(key: SimilarityBucketKey) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    key.encode(&mut bytes);
    bytes
}

fn validate_entries(
    entries: &[SimilarityIndexEntry],
    fingerprint_profile: u16,
) -> Result<(), SimilarityIndexFormatError> {
    if entries.is_empty() {
        return Err(SimilarityIndexFormatError::InvalidEntry);
    }
    for entry in entries {
        if !valid_logical_length(entry.logical_length)
            || entry.fingerprint_profile != fingerprint_profile
        {
            return Err(SimilarityIndexFormatError::InvalidEntry);
        }
    }
    if !entries
        .windows(2)
        .all(|pair| pair[0].chunk_id < pair[1].chunk_id)
    {
        return Err(SimilarityIndexFormatError::NonCanonicalOrder);
    }
    Ok(())
}

fn build_bucket_references(
    entries: &[SimilarityIndexEntry],
) -> Result<(Vec<SimilarityBucketReference>, usize), SimilarityIndexFormatError> {
    let capacity = entries
        .len()
        .checked_mul(4)
        .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
    let mut all = Vec::new();
    all.try_reserve_exact(capacity)
        .map_err(|_| SimilarityIndexFormatError::OutOfMemory)?;
    for (entry_ordinal, entry) in entries.iter().copied().enumerate() {
        let entry_ordinal = u32::try_from(entry_ordinal)
            .map_err(|_| SimilarityIndexFormatError::ArithmeticOverflow)?;
        for (slot, superfeature) in entry.superfeatures.into_iter().enumerate() {
            all.push(SimilarityBucketReference {
                key: SimilarityBucketKey::new(
                    entry.fingerprint_profile,
                    u8::try_from(slot)
                        .map_err(|_| SimilarityIndexFormatError::ArithmeticOverflow)?,
                    entry.logical_length,
                    superfeature,
                )?,
                entry_ordinal,
            });
        }
    }
    all.sort_unstable_by_key(|reference| reference.sort_key());

    let mut retained = Vec::new();
    retained
        .try_reserve_exact(all.len())
        .map_err(|_| SimilarityIndexFormatError::OutOfMemory)?;
    let mut bucket_count = 0_usize;
    let mut current_key = None;
    let mut retained_in_bucket = 0_usize;
    for reference in all {
        if current_key != Some(reference.key) {
            current_key = Some(reference.key);
            retained_in_bucket = 0;
            bucket_count = bucket_count
                .checked_add(1)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
        }
        if retained_in_bucket < MAX_BUCKET_REPRESENTATIVES {
            retained.push(reference);
            retained_in_bucket += 1;
        }
    }
    validate_bucket_references(retained.as_slice(), entries, entries[0].fingerprint_profile)?;
    Ok((retained, bucket_count))
}

fn validate_bucket_references(
    references: &[SimilarityBucketReference],
    entries: &[SimilarityIndexEntry],
    fingerprint_profile: u16,
) -> Result<(), SimilarityIndexFormatError> {
    if references.is_empty()
        || !references
            .windows(2)
            .all(|pair| pair[0].sort_key() < pair[1].sort_key())
    {
        return Err(SimilarityIndexFormatError::NonCanonicalOrder);
    }
    let mut previous_key = None;
    let mut references_in_bucket = 0_usize;
    for reference in references {
        if previous_key != Some(reference.key) {
            previous_key = Some(reference.key);
            references_in_bucket = 0;
        }
        references_in_bucket += 1;
        if references_in_bucket > MAX_BUCKET_REPRESENTATIVES
            || reference.key.fingerprint_profile != fingerprint_profile
        {
            return Err(SimilarityIndexFormatError::InvalidBucketReference);
        }
        let entry = entries
            .get(
                usize::try_from(reference.entry_ordinal)
                    .map_err(|_| SimilarityIndexFormatError::InvalidBucketReference)?,
            )
            .ok_or(SimilarityIndexFormatError::InvalidBucketReference)?;
        let slot = usize::from(reference.key.slot);
        if entry.fingerprint_profile != reference.key.fingerprint_profile
            || entry.logical_length != reference.key.logical_length
            || entry.superfeatures.get(slot) != Some(&reference.key.superfeature)
        {
            return Err(SimilarityIndexFormatError::InvalidBucketReference);
        }
    }
    Ok(())
}

fn distinct_bucket_count(references: &[SimilarityBucketReference]) -> usize {
    references
        .iter()
        .map(|reference| reference.key)
        .fold((None, 0_usize), |(previous, count), key| {
            (Some(key), count + usize::from(previous != Some(key)))
        })
        .1
}

fn valid_logical_length(logical_length: u32) -> bool {
    logical_length != 0
        && usize::try_from(logical_length).is_ok_and(|length| length <= MAX_LOGICAL_CHUNK_BYTES)
}

fn page_count(entry_count: usize) -> usize {
    entry_count.div_ceil(ENTRIES_PER_PAGE)
}

fn bucket_page_count(reference_count: usize) -> usize {
    reference_count.div_ceil(BUCKET_REFERENCES_PER_PAGE)
}

fn encoded_length(
    entry_count: usize,
    bucket_reference_count: usize,
) -> Result<usize, SimilarityIndexFormatError> {
    if entry_count == 0 || bucket_reference_count == 0 {
        return Err(SimilarityIndexFormatError::InvalidEntry);
    }
    let length = SIMILARITY_INDEX_HEADER_BYTES
        .checked_add(
            page_count(entry_count)
                .checked_add(bucket_page_count(bucket_reference_count))
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?
                .checked_mul(SIMILARITY_INDEX_PAGE_BYTES)
                .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?,
        )
        .and_then(|value| value.checked_add(FOOTER_BYTES))
        .ok_or(SimilarityIndexFormatError::ArithmeticOverflow)?;
    if length > MAX_RUN_BYTES {
        return Err(SimilarityIndexFormatError::InvalidObjectLength(length));
    }
    Ok(length)
}

fn key_bounds(entries: &[SimilarityIndexEntry]) -> [u8; 64] {
    let mut bounds = [0_u8; 64];
    if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
        bounds[..32].copy_from_slice(&first.chunk_id.bytes());
        bounds[32..].copy_from_slice(&last.chunk_id.bytes());
    }
    bounds
}

fn calculate_run_hash(bytes: &[u8], footer_offset: usize) -> [u8; 32] {
    let hash_offset = footer_offset + RUN_HASH_OFFSET;
    let crc_offset = footer_offset + FOOTER_CRC_OFFSET;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes[..hash_offset]);
    hasher.update(&[0_u8; 32]);
    hasher.update(&bytes[hash_offset + 32..crc_offset]);
    hasher.update(&[0_u8; 4]);
    hasher.update(&bytes[crc_offset + 4..]);
    *hasher.finalize().as_bytes()
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("ASSERT: fixed u16 field has two bytes"),
    )
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("ASSERT: fixed u32 field has four bytes"),
    )
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("ASSERT: fixed u64 field has eight bytes"),
    )
}

fn u64_from_usize(value: usize) -> Result<u64, SimilarityIndexFormatError> {
    u64::try_from(value).map_err(|_| SimilarityIndexFormatError::ArithmeticOverflow)
}

fn usize_from_u64(value: u64) -> Result<usize, SimilarityIndexFormatError> {
    usize::try_from(value).map_err(|_| SimilarityIndexFormatError::ArithmeticOverflow)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimilarityIndexFormatError {
    InvalidEntry,
    UnsupportedProfile,
    InvalidGeneration,
    InvalidObjectLength(usize),
    InvalidHeader,
    HeaderFooterMismatch,
    InvalidPage,
    InvalidBucketPage,
    InvalidBucketReference,
    NonCanonicalOrder,
    RunHashMismatch,
    NonSequentialAudit,
    ArithmeticOverflow,
    OutOfMemory,
}

impl fmt::Display for SimilarityIndexFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEntry => formatter.write_str("invalid Similarity Index entry"),
            Self::UnsupportedProfile => formatter.write_str("unsupported Similarity profile"),
            Self::InvalidGeneration => formatter.write_str("invalid Similarity generation"),
            Self::InvalidObjectLength(length) => {
                write!(formatter, "invalid Similarity Index object length {length}")
            }
            Self::InvalidHeader => formatter.write_str("invalid Similarity Index header"),
            Self::HeaderFooterMismatch => {
                formatter.write_str("Similarity Index header and footer disagree")
            }
            Self::InvalidPage => formatter.write_str("invalid Similarity Index page"),
            Self::InvalidBucketPage => formatter.write_str("invalid Similarity Index bucket page"),
            Self::InvalidBucketReference => {
                formatter.write_str("invalid Similarity Index bucket reference")
            }
            Self::NonCanonicalOrder => {
                formatter.write_str("noncanonical Similarity Index entry order")
            }
            Self::RunHashMismatch => formatter.write_str("Similarity Index run hash mismatch"),
            Self::NonSequentialAudit => formatter.write_str("nonsequential Similarity Index audit"),
            Self::ArithmeticOverflow => formatter.write_str("Similarity Index arithmetic overflow"),
            Self::OutOfMemory => formatter.write_str("Similarity Index allocation failed"),
        }
    }
}

impl std::error::Error for SimilarityIndexFormatError {}
