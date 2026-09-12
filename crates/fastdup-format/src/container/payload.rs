//! Verified payload ownership, provenance and process-local cache representations.
use super::{ChunkId, ContainerId, FormatError, RAW_CODEC, ZSTD_CODEC};
use crate::exact_index::ExactIndexEntry;
use crate::exact_index::ExactIndexLocation;
use crate::exact_index::ExactLocationTransition;
use core::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

#[path = "payload_backing.rs"]
mod payload_backing;
use payload_backing::{PayloadBacking, WeakBacking};
#[path = "cache_payload.rs"]
mod cache_payload;
pub use cache_payload::CompressedVerifiedChunkPayload;

#[derive(Clone, Copy)]
pub(super) struct VerifiedIndependentRecord {
    container_id: ContainerId,
    container_generation: u64,
    record_offset: u64,
    record_length: NonZeroU32,
    record_crc32c: u32,
    record_decoded_length: u32,
    record_payload_length: u32,
    codec_id: u16,
}

impl VerifiedIndependentRecord {
    // Called only after independent Record decoding and full Chunk verification.
    // Dependency and per-Chunk coordinates are not duplicated in this RAM proof.
    // The positive Record length also supplies the Option niche without packing.
    pub(super) fn new(location: ExactIndexLocation) -> Result<Self, FormatError> {
        if location.dependency_id() != [0; 32]
            || !matches!(location.codec_id(), RAW_CODEC | ZSTD_CODEC)
        {
            return Err(FormatError::ExactLocationMismatch);
        }
        Ok(Self {
            container_id: location.container_id(),
            container_generation: location.container_generation(),
            record_offset: location.record_offset(),
            record_length: NonZeroU32::new(location.record_length())
                .ok_or(FormatError::ExactLocationMismatch)?,
            record_crc32c: location.record_crc32c(),
            record_decoded_length: location.record_decoded_length(),
            record_payload_length: location.record_payload_length(),
            codec_id: location.codec_id(),
        })
    }
}

/// One decoded logical Chunk whose complete stored Encoding Record and BLAKE3
/// identity were independently verified.
///
/// Multi-Chunk records share one backing allocation. The private constructors
/// keep this type as verification evidence rather than a caller-supplied claim.
#[derive(Clone)]
pub struct VerifiedChunkPayload {
    chunk_id: ChunkId,
    backing: PayloadBacking,
    offset: usize,
    length: usize,
    pub(super) decoded_offset: usize,
    pub(super) chunk_ordinal: u32,
    pub(super) source: Option<VerifiedIndependentRecord>,
}

/// Ephemeral identity of a retained payload allocation, never durable evidence.
/// It may be compared only while an owning payload or Read View remains alive.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VerifiedChunkBackingId(usize);

/// A contiguous file-response view assembled from already verified Chunk ranges.
/// Unlike a Chunk payload, this view has no content identity of its own.
#[derive(Clone, Debug)]
pub struct VerifiedReadView {
    backing: PayloadBacking,
    range: std::ops::Range<usize>,
}

impl AsRef<[u8]> for VerifiedReadView {
    fn as_ref(&self) -> &[u8] {
        &self.backing[self.range.clone()]
    }
}

impl VerifiedReadView {
    /// Extends only across adjacent verified bytes in this same allocation.
    /// A different owner, a gap, reversed order or an invalid range returns false.
    pub fn try_append(
        &mut self,
        payload: &VerifiedChunkPayload,
        range: std::ops::Range<usize>,
    ) -> bool {
        if range.start > range.end
            || range.end > payload.length
            || !self.backing.shares(&payload.backing)
            || self.range.end != payload.offset + range.start
        {
            return false;
        }
        self.range.end = payload.offset + range.end;
        true
    }
}

impl AsRef<[u8]> for VerifiedChunkPayload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl VerifiedChunkPayload {
    /// Consumes this verified payload into a checked response owner, moving
    /// the backing reference without retaining identity or Location metadata.
    #[must_use]
    pub fn into_read_view(self, range: std::ops::Range<usize>) -> Option<VerifiedReadView> {
        (range.start <= range.end && range.end <= self.length).then(|| VerifiedReadView {
            backing: self.backing,
            range: self.offset + range.start..self.offset + range.end,
        })
    }

    /// Returns a checked response owner for part of this verified Chunk.
    #[must_use]
    pub fn read_view(&self, range: std::ops::Range<usize>) -> Option<VerifiedReadView> {
        (range.start <= range.end && range.end <= self.length).then(|| VerifiedReadView {
            backing: self.backing.clone(),
            range: self.offset + range.start..self.offset + range.end,
        })
    }

    /// Returns a process-local grouping key while this allocation is retained.
    #[must_use]
    pub fn backing_id(&self) -> VerifiedChunkBackingId {
        VerifiedChunkBackingId(self.backing.id())
    }
    pub(super) fn from_owned(chunk_id: ChunkId, bytes: Vec<u8>) -> Self {
        let length = bytes.len();
        Self {
            chunk_id,
            backing: PayloadBacking::Owned(Arc::new(bytes)),
            offset: 0,
            length,
            decoded_offset: 0,
            chunk_ordinal: 0,
            source: None,
        }
    }

    pub(super) fn from_shared(
        chunk_id: ChunkId,
        backing: Arc<Vec<u8>>,
        offset: usize,
        length: usize,
    ) -> Result<Self, FormatError> {
        let end = offset
            .checked_add(length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if end > backing.len() {
            return Err(FormatError::InvalidZstdRecord);
        }
        Ok(Self {
            chunk_id,
            backing: PayloadBacking::Owned(backing),
            offset,
            length,
            decoded_offset: offset,
            chunk_ordinal: 0,
            source: None,
        })
    }

    #[must_use]
    pub const fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.length
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Logical offset inside the decoded Record, independent of the physical
    /// backing offset (RAW views retain their encoded header). Waiters use it
    /// to pair a shared Singleflight
    /// result with the requested Chunk-table coordinate in O(1).
    #[must_use]
    pub const fn decoded_offset(&self) -> usize {
        self.decoded_offset
    }

    /// Carries independently checked physical coordinates without retaining
    /// the decoded allocation. This evidence is process-local, not liveness
    /// authority, and is absent when decoding did not establish a Location.
    #[must_use]
    pub fn verified_location(&self) -> Option<super::VerifiedChunkLocation> {
        let source = self.source?;
        Some(super::VerifiedChunkLocation {
            chunk_id: self.chunk_id,
            logical_length: self.length.try_into().ok()?,
            container_id: source.container_id,
            container_generation: source.container_generation,
            record_offset: source.record_offset,
            record_length: source.record_length.get(),
            chunk_ordinal: self.chunk_ordinal,
            decoded_offset: self.decoded_offset.try_into().ok()?,
            codec_id: source.codec_id,
            dependency_id: [0; 32],
            record_crc32c: source.record_crc32c,
            record_decoded_length: source.record_decoded_length,
            record_payload_length: source.record_payload_length,
        })
    }

    /// Matches a current independent Exact candidate against the physical
    /// Record and Chunk coordinates independently verified at decode time.
    #[must_use]
    pub fn matches_independent_candidate(&self, candidate: ExactIndexEntry) -> bool {
        let Some(source) = &self.source else {
            return false;
        };
        let location = candidate.location();
        candidate.transition() == ExactLocationTransition::Active
            && self.chunk_id == candidate.chunk_id()
            && u64::try_from(self.length) == Ok(u64::from(candidate.logical_length()))
            && self.chunk_ordinal == location.chunk_ordinal()
            && u64::try_from(self.decoded_offset) == Ok(u64::from(location.decoded_offset()))
            && location.dependency_id() == [0; 32]
            && source.container_id == location.container_id()
            && source.container_generation == location.container_generation()
            && source.record_offset == location.record_offset()
            && source.record_length.get() == location.record_length()
            && source.record_crc32c == location.record_crc32c()
            && source.record_decoded_length == location.record_decoded_length()
            && source.record_payload_length == location.record_payload_length()
            && source.codec_id == location.codec_id()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.backing[self.offset..self.offset + self.length]
    }

    /// Returns the one allocation retained by this payload and every sibling
    /// view from the same decoded Record or coalesced encoded RAW batch.
    #[must_use]
    pub fn backing_allocation_bytes(&self) -> usize {
        self.backing.capacity()
    }

    #[must_use]
    pub fn shares_backing_with(&self, other: &Self) -> bool {
        self.backing.shares(&other.backing)
    }

    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        if self.offset == 0 && self.length == self.backing.len() {
            self.backing.into_vec()
        } else {
            self.as_slice().to_vec()
        }
    }
}

impl fmt::Debug for VerifiedChunkPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedChunkPayload")
            .field("chunk_id", &self.chunk_id)
            .field("length", &self.length)
            .field("backing_bytes", &self.backing.capacity())
            .finish_non_exhaustive()
    }
}

impl PartialEq for VerifiedChunkPayload {
    fn eq(&self, other: &Self) -> bool {
        self.chunk_id == other.chunk_id && self.as_slice() == other.as_slice()
    }
}

impl Eq for VerifiedChunkPayload {}

#[cfg(test)]
#[path = "payload_tests.rs"]
mod tests;
