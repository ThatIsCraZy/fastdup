use core::fmt;

use crate::metadata::{
    EXACT_INDEX_RUN_SET_KIND, MAX_METADATA_OBJECT_BYTES, MetadataFormatError, MetadataObjectId,
    decode_metadata_object, encode_metadata_object,
};
use crate::{ChunkId, ExactIndexProfileId, ExactIndexRunDescriptor};

const PAYLOAD_MAGIC: [u8; 8] = *b"FDXRST01";
const FORMAT_VERSION: u16 = 2;
const PAYLOAD_HEADER_BYTES: usize = 128;
const RUN_ENTRY_BYTES: usize = 160;
const PAYLOAD_HEADER_BYTES_U16: u16 = 128;
const MAX_RUN_FILE_BYTES: u64 = 1 << 30;
const MIN_RUN_FILE_BYTES: u64 = 8_192;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExactIndexRunSetId(MetadataObjectId);

impl ExactIndexRunSetId {
    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Option<Self> {
        MetadataObjectId::new(bytes).map(Self)
    }

    /// Verifies a complete Run Set object and returns its content identity.
    ///
    /// # Errors
    ///
    /// Returns envelope, payload, canonical-order, or identity errors.
    pub fn from_encoded(bytes: &[u8]) -> Result<Self, ExactIndexRunSetError> {
        decode_with_id(bytes).map(|(_, identity)| identity)
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0.bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactIndexRunRef {
    level: u16,
    partition_ordinal: u16,
    partition_count: u16,
    profile: ExactIndexProfileId,
    family_generation: u64,
    generation: u64,
    run_hash: [u8; 32],
    file_length: u64,
    entry_count: u64,
    minimum_chunk_id: ChunkId,
    maximum_chunk_id: ChunkId,
}

impl ExactIndexRunRef {
    /// Pins one fully verified immutable run in a Run Set.
    ///
    /// # Errors
    ///
    /// Empty runs are rebuild machinery and cannot be activated for lookup.
    pub fn new(
        level: u16,
        descriptor: ExactIndexRunDescriptor,
    ) -> Result<Self, ExactIndexRunSetError> {
        if descriptor.entry_count() == 0 {
            return Err(ExactIndexRunSetError::InvalidRunReference);
        }
        Ok(Self {
            level,
            partition_ordinal: 0,
            partition_count: 1,
            profile: descriptor.profile(),
            family_generation: descriptor.generation(),
            generation: descriptor.generation(),
            run_hash: descriptor.run_hash(),
            file_length: u64::try_from(descriptor.file_length())
                .map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?,
            entry_count: u64::try_from(descriptor.entry_count())
                .map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?,
            minimum_chunk_id: descriptor.minimum_chunk_id(),
            maximum_chunk_id: descriptor.maximum_chunk_id(),
        })
    }

    /// Pins one physical partition of a complete logical Run family.
    ///
    /// # Errors
    ///
    /// Rejects empty Runs, zero/inconsistent family geometry, or a Run
    /// generation other than `family_generation + partition_ordinal`.
    pub fn family_partition(
        level: u16,
        family_generation: u64,
        partition_ordinal: u16,
        partition_count: u16,
        descriptor: ExactIndexRunDescriptor,
    ) -> Result<Self, ExactIndexRunSetError> {
        if descriptor.entry_count() == 0
            || family_generation == 0
            || partition_count == 0
            || partition_ordinal >= partition_count
            || family_generation.checked_add(u64::from(partition_ordinal))
                != Some(descriptor.generation())
        {
            return Err(ExactIndexRunSetError::InvalidRunFamily);
        }
        Ok(Self {
            level,
            partition_ordinal,
            partition_count,
            profile: descriptor.profile(),
            family_generation,
            generation: descriptor.generation(),
            run_hash: descriptor.run_hash(),
            file_length: u64::try_from(descriptor.file_length())
                .map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?,
            entry_count: u64::try_from(descriptor.entry_count())
                .map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?,
            minimum_chunk_id: descriptor.minimum_chunk_id(),
            maximum_chunk_id: descriptor.maximum_chunk_id(),
        })
    }

    #[must_use]
    pub const fn level(self) -> u16 {
        self.level
    }

    #[must_use]
    pub const fn profile(self) -> ExactIndexProfileId {
        self.profile
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn family_generation(self) -> u64 {
        self.family_generation
    }

    #[must_use]
    pub const fn partition_ordinal(self) -> u16 {
        self.partition_ordinal
    }

    #[must_use]
    pub const fn partition_count(self) -> u16 {
        self.partition_count
    }

    #[must_use]
    pub const fn run_hash(self) -> [u8; 32] {
        self.run_hash
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length
    }

    #[must_use]
    pub const fn entry_count(self) -> u64 {
        self.entry_count
    }

    #[must_use]
    pub const fn minimum_chunk_id(self) -> ChunkId {
        self.minimum_chunk_id
    }

    #[must_use]
    pub const fn maximum_chunk_id(self) -> ChunkId {
        self.maximum_chunk_id
    }

    const fn canonical_key(self) -> (u16, u64, u16, u64) {
        (
            self.level,
            self.family_generation,
            self.partition_ordinal,
            self.generation,
        )
    }

    fn encode(self, bytes: &mut [u8]) {
        put_u16(bytes, 0, self.level);
        put_u16(bytes, 2, self.partition_ordinal);
        put_u16(bytes, 4, self.partition_count);
        put_u64(bytes, 8, self.generation);
        bytes[16..48].copy_from_slice(&self.run_hash);
        put_u64(bytes, 48, self.file_length);
        put_u64(bytes, 56, self.entry_count);
        bytes[64..96].copy_from_slice(&self.minimum_chunk_id.bytes());
        bytes[96..128].copy_from_slice(&self.maximum_chunk_id.bytes());
        put_u64(bytes, 128, self.family_generation);
    }

    fn decode(profile: ExactIndexProfileId, bytes: &[u8]) -> Result<Self, ExactIndexRunSetError> {
        if bytes.len() != RUN_ENTRY_BYTES
            || bytes[6..8].iter().any(|byte| *byte != 0)
            || bytes[136..].iter().any(|byte| *byte != 0)
            || get_u64(bytes, 8) == 0
            || get_u64(bytes, 48) < MIN_RUN_FILE_BYTES
            || get_u64(bytes, 48) > MAX_RUN_FILE_BYTES
            || !get_u64(bytes, 48).is_multiple_of(4_096)
            || get_u64(bytes, 56) == 0
            || get_u64(bytes, 128) == 0
            || get_u16(bytes, 4) == 0
            || get_u16(bytes, 2) >= get_u16(bytes, 4)
            || get_u64(bytes, 128).checked_add(u64::from(get_u16(bytes, 2)))
                != Some(get_u64(bytes, 8))
        {
            return Err(ExactIndexRunSetError::InvalidRunFamily);
        }
        let mut run_hash = [0_u8; 32];
        run_hash.copy_from_slice(&bytes[16..48]);
        let mut minimum = [0_u8; 32];
        minimum.copy_from_slice(&bytes[64..96]);
        let mut maximum = [0_u8; 32];
        maximum.copy_from_slice(&bytes[96..128]);
        let minimum_chunk_id = ChunkId::from_bytes(minimum);
        let maximum_chunk_id = ChunkId::from_bytes(maximum);
        if minimum_chunk_id > maximum_chunk_id {
            return Err(ExactIndexRunSetError::InvalidRunReference);
        }
        Ok(Self {
            level: get_u16(bytes, 0),
            partition_ordinal: get_u16(bytes, 2),
            partition_count: get_u16(bytes, 4),
            profile,
            family_generation: get_u64(bytes, 128),
            generation: get_u64(bytes, 8),
            run_hash,
            file_length: get_u64(bytes, 48),
            entry_count: get_u64(bytes, 56),
            minimum_chunk_id,
            maximum_chunk_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactIndexRunSet {
    profile: ExactIndexProfileId,
    generation: u64,
    runs: Vec<ExactIndexRunRef>,
}

impl ExactIndexRunSet {
    /// Canonicalizes one immutable set of already durable Exact Index runs.
    ///
    /// # Errors
    ///
    /// Rejects zero generations, profile mismatch, duplicate run generations,
    /// an oversized payload, or allocation failure.
    pub fn new(
        profile: ExactIndexProfileId,
        generation: u64,
        mut runs: Vec<ExactIndexRunRef>,
    ) -> Result<Self, ExactIndexRunSetError> {
        if generation == 0 {
            return Err(ExactIndexRunSetError::InvalidGeneration);
        }
        payload_length(runs.len(), RUN_ENTRY_BYTES)?;
        runs.sort_unstable_by_key(|run| run.canonical_key());
        validate_runs(profile, &runs)?;
        Ok(Self {
            profile,
            generation,
            runs,
        })
    }

    #[must_use]
    pub const fn profile(&self) -> ExactIndexProfileId {
        self.profile
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn runs(&self) -> &[ExactIndexRunRef] {
        &self.runs
    }

    #[must_use]
    pub fn family_count(&self) -> usize {
        self.runs
            .iter()
            .enumerate()
            .filter(|(ordinal, run)| {
                *ordinal == 0
                    || self.runs[*ordinal - 1].family_generation != run.family_generation
                    || self.runs[*ordinal - 1].level != run.level
            })
            .count()
    }

    /// Encodes the canonical payload in the generic content-addressed Metadata
    /// Object envelope as object kind 3.
    ///
    /// # Errors
    ///
    /// Returns canonical, arithmetic, size, allocation, or envelope errors.
    pub fn encode(&self) -> Result<Vec<u8>, ExactIndexRunSetError> {
        validate_runs(self.profile, &self.runs)?;
        let payload_length = payload_length(self.runs.len(), RUN_ENTRY_BYTES)?;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(payload_length)
            .map_err(|_| ExactIndexRunSetError::OutOfMemory)?;
        payload.resize(payload_length, 0);
        payload[0..8].copy_from_slice(&PAYLOAD_MAGIC);
        put_u16(&mut payload, 8, FORMAT_VERSION);
        put_u16(&mut payload, 10, PAYLOAD_HEADER_BYTES_U16);
        put_u16(
            &mut payload,
            12,
            u16::try_from(RUN_ENTRY_BYTES)
                .map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?,
        );
        put_u64(&mut payload, 24, self.generation);
        payload[32..64].copy_from_slice(&self.profile.bytes());
        put_u32(
            &mut payload,
            64,
            u32::try_from(self.runs.len())
                .map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?,
        );
        put_u32(
            &mut payload,
            68,
            u32::try_from(payload_length).map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?,
        );
        for (ordinal, run) in self.runs.iter().copied().enumerate() {
            let start = PAYLOAD_HEADER_BYTES + ordinal * RUN_ENTRY_BYTES;
            run.encode(&mut payload[start..start + RUN_ENTRY_BYTES]);
        }
        Ok(encode_metadata_object(EXACT_INDEX_RUN_SET_KIND, &payload)?)
    }

    /// Decodes and fully validates one content-addressed Run Set.
    ///
    /// # Errors
    ///
    /// Returns envelope, payload, allocation, profile, duplicate, or canonical
    /// ordering failures.
    pub fn decode(bytes: &[u8]) -> Result<Self, ExactIndexRunSetError> {
        decode_with_id(bytes).map(|(run_set, _)| run_set)
    }

    /// Returns the content identity of this canonical Run Set.
    ///
    /// # Errors
    ///
    /// Returns any encoding failure.
    pub fn id(&self) -> Result<ExactIndexRunSetId, ExactIndexRunSetError> {
        ExactIndexRunSetId::from_encoded(&self.encode()?)
    }
}

fn decode_with_id(
    bytes: &[u8],
) -> Result<(ExactIndexRunSet, ExactIndexRunSetId), ExactIndexRunSetError> {
    let object = decode_metadata_object(Some(EXACT_INDEX_RUN_SET_KIND), bytes)?;
    let payload = object.payload;
    if payload.len() < PAYLOAD_HEADER_BYTES {
        return Err(ExactIndexRunSetError::InvalidPayload);
    }
    if payload[0..8] != PAYLOAD_MAGIC
        || get_u16(payload, 8) != FORMAT_VERSION
        || usize::from(get_u16(payload, 10)) != PAYLOAD_HEADER_BYTES
        || usize::from(get_u16(payload, 12)) != RUN_ENTRY_BYTES
        || get_u16(payload, 14) != 0
        || get_u64(payload, 16) != 0
        || payload[72..PAYLOAD_HEADER_BYTES]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(ExactIndexRunSetError::InvalidPayload);
    }
    let generation = get_u64(payload, 24);
    if generation == 0 {
        return Err(ExactIndexRunSetError::InvalidGeneration);
    }
    let run_count = usize::try_from(get_u32(payload, 64))
        .map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?;
    if payload_length(run_count, RUN_ENTRY_BYTES)? != payload.len()
        || usize::try_from(get_u32(payload, 68)) != Ok(payload.len())
    {
        return Err(ExactIndexRunSetError::InvalidPayload);
    }
    let mut profile = [0_u8; 32];
    profile.copy_from_slice(&payload[32..64]);
    let profile = ExactIndexProfileId::new(profile).ok_or(ExactIndexRunSetError::InvalidProfile)?;
    let mut runs = Vec::new();
    runs.try_reserve_exact(run_count)
        .map_err(|_| ExactIndexRunSetError::OutOfMemory)?;
    for ordinal in 0..run_count {
        let start = PAYLOAD_HEADER_BYTES + ordinal * RUN_ENTRY_BYTES;
        let entry = &payload[start..start + RUN_ENTRY_BYTES];
        runs.push(ExactIndexRunRef::decode(profile, entry)?);
    }
    validate_runs(profile, &runs)?;
    Ok((
        ExactIndexRunSet {
            profile,
            generation,
            runs,
        },
        ExactIndexRunSetId(object.id),
    ))
}

fn validate_runs(
    profile: ExactIndexProfileId,
    runs: &[ExactIndexRunRef],
) -> Result<(), ExactIndexRunSetError> {
    if runs.iter().any(|run| run.profile != profile) {
        return Err(ExactIndexRunSetError::InvalidProfile);
    }
    let noncanonical = runs
        .windows(2)
        .any(|pair| pair[0].canonical_key() >= pair[1].canonical_key());
    if noncanonical {
        return Err(ExactIndexRunSetError::NonCanonicalOrder);
    }
    validate_families(runs)?;
    let mut generations = Vec::new();
    generations
        .try_reserve_exact(runs.len())
        .map_err(|_| ExactIndexRunSetError::OutOfMemory)?;
    generations.extend(runs.iter().map(|run| run.generation));
    generations.sort_unstable();
    if generations.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ExactIndexRunSetError::DuplicateRunGeneration);
    }
    Ok(())
}

fn validate_families(runs: &[ExactIndexRunRef]) -> Result<(), ExactIndexRunSetError> {
    let mut cursor = 0;
    while cursor < runs.len() {
        let first = runs[cursor];
        let expected_count = usize::from(first.partition_count);
        let end = cursor
            .checked_add(expected_count)
            .ok_or(ExactIndexRunSetError::ArithmeticOverflow)?;
        if end > runs.len() {
            return Err(ExactIndexRunSetError::InvalidRunFamily);
        }
        let family = &runs[cursor..end];
        for (ordinal, run) in family.iter().copied().enumerate() {
            if run.level != first.level
                || run.family_generation != first.family_generation
                || run.partition_count != first.partition_count
                || usize::from(run.partition_ordinal) != ordinal
                || first.family_generation.checked_add(
                    u64::try_from(ordinal)
                        .map_err(|_| ExactIndexRunSetError::ArithmeticOverflow)?,
                ) != Some(run.generation)
            {
                return Err(ExactIndexRunSetError::InvalidRunFamily);
            }
        }
        if family
            .windows(2)
            .any(|pair| pair[0].maximum_chunk_id >= pair[1].minimum_chunk_id)
        {
            return Err(ExactIndexRunSetError::OverlappingRunFamily);
        }
        cursor = end;
    }
    Ok(())
}

fn payload_length(run_count: usize, entry_bytes: usize) -> Result<usize, ExactIndexRunSetError> {
    let length = PAYLOAD_HEADER_BYTES
        .checked_add(
            run_count
                .checked_mul(entry_bytes)
                .ok_or(ExactIndexRunSetError::ArithmeticOverflow)?,
        )
        .ok_or(ExactIndexRunSetError::ArithmeticOverflow)?;
    if length > MAX_METADATA_OBJECT_BYTES - 4_096 {
        return Err(ExactIndexRunSetError::InvalidPayload);
    }
    Ok(length)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactIndexRunSetError {
    Metadata(MetadataFormatError),
    InvalidGeneration,
    InvalidProfile,
    InvalidRunReference,
    InvalidRunFamily,
    OverlappingRunFamily,
    InvalidPayload,
    DuplicateRunGeneration,
    NonCanonicalOrder,
    ArithmeticOverflow,
    OutOfMemory,
}

impl fmt::Display for ExactIndexRunSetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ExactIndexRunSetError {}

impl From<MetadataFormatError> for ExactIndexRunSetError {
    fn from(error: MetadataFormatError) -> Self {
        Self::Metadata(error)
    }
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
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}
