use std::fmt;

use crate::{
    ChunkId, ExactIndexRunSetId, SimilarityBucketKey, SimilarityIndexRunDescriptor,
    crc32c_with_zeroed_u32,
};

pub const SIMILARITY_FAMILY_HEADER_BYTES: usize = 4_096;
pub const SIMILARITY_FAMILY_PARTITION_BYTES: usize = 192;
const SIMILARITY_FAMILY_FOOTER_BYTES: usize = 4_096;
const SIMILARITY_FAMILY_MAX_BYTES: usize = 16 * 1_024 * 1_024;
const FAMILY_MAGIC: [u8; 8] = *b"FDSIFM02";
const FOOTER_MAGIC: [u8; 8] = *b"FDSIFF02";
const FORMAT_VERSION: u16 = 2;
const HEADER_CRC_OFFSET: usize = 80;
const FOOTER_HASH_OFFSET: usize = 80;
const FOOTER_CRC_OFFSET: usize = 112;

/// One authenticated physical Run dependency of a Similarity family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimilarityIndexPartitionRef {
    partition_ordinal: u16,
    partition_count: u16,
    run_hash: [u8; 32],
    file_length: u64,
    entry_count: u64,
    bucket_count: u64,
    bucket_reference_count: u64,
    minimum_chunk_id: ChunkId,
    maximum_chunk_id: ChunkId,
    minimum_bucket_key: SimilarityBucketKey,
    maximum_bucket_key: SimilarityBucketKey,
}

impl SimilarityIndexPartitionRef {
    /// Pins one fully verified physical Run to a family partition.
    ///
    /// # Errors
    ///
    /// Rejects ordinal/count disagreement, an empty Run, a reversed `BucketKey`
    /// range, or a descriptor from another logical generation.
    pub fn new(
        family_generation: u64,
        partition_ordinal: u16,
        partition_count: u16,
        descriptor: SimilarityIndexRunDescriptor,
        minimum_bucket_key: SimilarityBucketKey,
        maximum_bucket_key: SimilarityBucketKey,
    ) -> Result<Self, SimilarityIndexFamilyError> {
        if family_generation == 0
            || descriptor.generation() != family_generation
            || partition_count == 0
            || partition_ordinal >= partition_count
            || descriptor.entry_count() == 0
            || descriptor.bucket_count() == 0
            || descriptor.bucket_reference_count() == 0
            || minimum_bucket_key > maximum_bucket_key
        {
            return Err(SimilarityIndexFamilyError::InvalidPartition);
        }
        Ok(Self {
            partition_ordinal,
            partition_count,
            run_hash: descriptor.run_hash(),
            file_length: descriptor.file_length(),
            entry_count: u64::try_from(descriptor.entry_count())
                .map_err(|_| SimilarityIndexFamilyError::ArithmeticOverflow)?,
            bucket_count: u64::try_from(descriptor.bucket_count())
                .map_err(|_| SimilarityIndexFamilyError::ArithmeticOverflow)?,
            bucket_reference_count: u64::try_from(descriptor.bucket_reference_count())
                .map_err(|_| SimilarityIndexFamilyError::ArithmeticOverflow)?,
            minimum_chunk_id: descriptor.minimum_chunk_id(),
            maximum_chunk_id: descriptor.maximum_chunk_id(),
            minimum_bucket_key,
            maximum_bucket_key,
        })
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
    pub const fn bucket_count(self) -> u64 {
        self.bucket_count
    }

    #[must_use]
    pub const fn bucket_reference_count(self) -> u64 {
        self.bucket_reference_count
    }

    #[must_use]
    pub const fn minimum_chunk_id(self) -> ChunkId {
        self.minimum_chunk_id
    }

    #[must_use]
    pub const fn maximum_chunk_id(self) -> ChunkId {
        self.maximum_chunk_id
    }

    #[must_use]
    pub const fn minimum_bucket_key(self) -> SimilarityBucketKey {
        self.minimum_bucket_key
    }

    #[must_use]
    pub const fn maximum_bucket_key(self) -> SimilarityBucketKey {
        self.maximum_bucket_key
    }

    fn encode(self, output: &mut [u8]) {
        assert_eq!(
            output.len(),
            SIMILARITY_FAMILY_PARTITION_BYTES,
            "ASSERT: fixed Similarity family partition record"
        );
        output.fill(0);
        put_u16(output, 0, self.partition_ordinal);
        put_u16(output, 2, self.partition_count);
        put_u64(output, 8, self.file_length);
        put_u64(output, 16, self.entry_count);
        put_u64(output, 24, self.bucket_count);
        put_u64(output, 32, self.bucket_reference_count);
        output[40..72].copy_from_slice(&self.run_hash);
        output[72..104].copy_from_slice(&self.minimum_chunk_id.bytes());
        output[104..136].copy_from_slice(&self.maximum_chunk_id.bytes());
        encode_bucket_key(self.minimum_bucket_key, &mut output[136..152]);
        encode_bucket_key(self.maximum_bucket_key, &mut output[152..168]);
    }

    fn decode(input: &[u8]) -> Result<Self, SimilarityIndexFamilyError> {
        if input.len() != SIMILARITY_FAMILY_PARTITION_BYTES
            || input[4..8].iter().any(|byte| *byte != 0)
            || input[168..].iter().any(|byte| *byte != 0)
        {
            return Err(SimilarityIndexFamilyError::InvalidPartition);
        }
        let mut run_hash = [0_u8; 32];
        run_hash.copy_from_slice(&input[40..72]);
        let mut minimum_chunk_id = [0_u8; 32];
        minimum_chunk_id.copy_from_slice(&input[72..104]);
        let mut maximum_chunk_id = [0_u8; 32];
        maximum_chunk_id.copy_from_slice(&input[104..136]);
        let reference = Self {
            partition_ordinal: get_u16(input, 0),
            partition_count: get_u16(input, 2),
            run_hash,
            file_length: get_u64(input, 8),
            entry_count: get_u64(input, 16),
            bucket_count: get_u64(input, 24),
            bucket_reference_count: get_u64(input, 32),
            minimum_chunk_id: ChunkId::from_bytes(minimum_chunk_id),
            maximum_chunk_id: ChunkId::from_bytes(maximum_chunk_id),
            minimum_bucket_key: decode_bucket_key(&input[136..152])?,
            maximum_bucket_key: decode_bucket_key(&input[152..168])?,
        };
        if reference.partition_count == 0
            || reference.partition_ordinal >= reference.partition_count
            || reference.file_length < 3 * 4_096
            || reference.entry_count == 0
            || reference.bucket_count == 0
            || reference.bucket_reference_count == 0
            || reference.bucket_count > reference.bucket_reference_count
            || reference.minimum_chunk_id > reference.maximum_chunk_id
            || reference.minimum_bucket_key > reference.maximum_bucket_key
        {
            return Err(SimilarityIndexFamilyError::InvalidPartition);
        }
        Ok(reference)
    }
}

/// Atomic selection manifest for one logical partitioned Similarity snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimilarityIndexRunFamily {
    fingerprint_profile: u16,
    bucket_profile: u16,
    generation: u64,
    logical_entry_count: u64,
    source_exact_run_set_id: Option<ExactIndexRunSetId>,
    partitions: Vec<SimilarityIndexPartitionRef>,
}

impl SimilarityIndexRunFamily {
    /// Constructs one canonical complete partition family.
    ///
    /// # Errors
    ///
    /// Rejects empty, mixed, incomplete, reordered, or overlapping families.
    pub fn new(
        fingerprint_profile: u16,
        bucket_profile: u16,
        generation: u64,
        logical_entry_count: u64,
        partitions: Vec<SimilarityIndexPartitionRef>,
    ) -> Result<Self, SimilarityIndexFamilyError> {
        Self::new_inner(
            fingerprint_profile,
            bucket_profile,
            generation,
            logical_entry_count,
            None,
            partitions,
        )
    }

    /// Constructs a complete family tied to the Exact Run Set built from the
    /// same verified pool scan.
    ///
    /// Empty families are valid tombstones: they supersede an older Similarity
    /// snapshot after rebuilding an empty pool.
    ///
    /// # Errors
    ///
    /// Rejects invalid profiles, generations, cardinality, partition geometry,
    /// ordering, or encoded-size bounds.
    pub fn new_bound(
        fingerprint_profile: u16,
        bucket_profile: u16,
        generation: u64,
        logical_entry_count: u64,
        source_exact_run_set_id: ExactIndexRunSetId,
        partitions: Vec<SimilarityIndexPartitionRef>,
    ) -> Result<Self, SimilarityIndexFamilyError> {
        Self::new_inner(
            fingerprint_profile,
            bucket_profile,
            generation,
            logical_entry_count,
            Some(source_exact_run_set_id),
            partitions,
        )
    }

    fn new_inner(
        fingerprint_profile: u16,
        bucket_profile: u16,
        generation: u64,
        logical_entry_count: u64,
        source_exact_run_set_id: Option<ExactIndexRunSetId>,
        partitions: Vec<SimilarityIndexPartitionRef>,
    ) -> Result<Self, SimilarityIndexFamilyError> {
        let empty = logical_entry_count == 0 && partitions.is_empty();
        if fingerprint_profile == 0
            || bucket_profile == 0
            || generation == 0
            || (empty && source_exact_run_set_id.is_none())
            || (!empty && (logical_entry_count == 0 || partitions.is_empty()))
            || partitions.len() > usize::from(u16::MAX)
        {
            return Err(SimilarityIndexFamilyError::InvalidFamily);
        }
        validate_partitions(&partitions)?;
        encoded_length(partitions.len())?;
        Ok(Self {
            fingerprint_profile,
            bucket_profile,
            generation,
            logical_entry_count,
            source_exact_run_set_id,
            partitions,
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
    pub const fn logical_entry_count(&self) -> u64 {
        self.logical_entry_count
    }

    #[must_use]
    pub const fn source_exact_run_set_id(&self) -> Option<ExactIndexRunSetId> {
        self.source_exact_run_set_id
    }

    #[must_use]
    pub fn partitions(&self) -> &[SimilarityIndexPartitionRef] {
        &self.partitions
    }

    /// Encodes the complete family manifest field by field.
    ///
    /// # Errors
    ///
    /// Returns an invalid-family or arithmetic failure.
    pub fn encode(&self) -> Result<Vec<u8>, SimilarityIndexFamilyError> {
        validate_partitions(&self.partitions)?;
        let length = encoded_length(self.partitions.len())?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| SimilarityIndexFamilyError::OutOfMemory)?;
        bytes.resize(length, 0);
        encode_identity(
            &mut bytes[..SIMILARITY_FAMILY_HEADER_BYTES],
            FAMILY_MAGIC,
            self,
        )?;
        let header_crc =
            crc32c_with_zeroed_u32(&bytes[..SIMILARITY_FAMILY_HEADER_BYTES], HEADER_CRC_OFFSET);
        put_u32(
            &mut bytes[..SIMILARITY_FAMILY_HEADER_BYTES],
            HEADER_CRC_OFFSET,
            header_crc,
        );
        for (ordinal, partition) in self.partitions.iter().copied().enumerate() {
            let start =
                SIMILARITY_FAMILY_HEADER_BYTES + ordinal * SIMILARITY_FAMILY_PARTITION_BYTES;
            partition.encode(&mut bytes[start..start + SIMILARITY_FAMILY_PARTITION_BYTES]);
        }
        let footer_offset = length - SIMILARITY_FAMILY_FOOTER_BYTES;
        let hash = blake3::hash(&bytes[..footer_offset]);
        encode_identity(&mut bytes[footer_offset..], FOOTER_MAGIC, self)?;
        bytes[footer_offset + FOOTER_HASH_OFFSET..footer_offset + FOOTER_HASH_OFFSET + 32]
            .copy_from_slice(hash.as_bytes());
        let footer_crc = crc32c_with_zeroed_u32(&bytes[footer_offset..], FOOTER_CRC_OFFSET);
        put_u32(&mut bytes[footer_offset..], FOOTER_CRC_OFFSET, footer_crc);
        Ok(bytes)
    }

    /// Decodes and authenticates one complete family manifest.
    ///
    /// # Errors
    ///
    /// Rejects malformed geometry, reserved fields, checksums, hashes,
    /// incomplete ordinals, or overlapping `BucketKey` ranges.
    pub fn decode(bytes: &[u8]) -> Result<Self, SimilarityIndexFamilyError> {
        if bytes.len() < SIMILARITY_FAMILY_HEADER_BYTES + SIMILARITY_FAMILY_FOOTER_BYTES
            || bytes.len() > SIMILARITY_FAMILY_MAX_BYTES
        {
            return Err(SimilarityIndexFamilyError::InvalidLength);
        }
        let footer_offset = bytes.len() - SIMILARITY_FAMILY_FOOTER_BYTES;
        let header = &bytes[..SIMILARITY_FAMILY_HEADER_BYTES];
        let footer = &bytes[footer_offset..];
        verify_identity(header, FAMILY_MAGIC, bytes.len(), false)?;
        verify_identity(footer, FOOTER_MAGIC, bytes.len(), true)?;
        if header[8..80] != footer[8..80]
            || get_u32(header, HEADER_CRC_OFFSET)
                != crc32c_with_zeroed_u32(header, HEADER_CRC_OFFSET)
            || get_u32(footer, FOOTER_CRC_OFFSET)
                != crc32c_with_zeroed_u32(footer, FOOTER_CRC_OFFSET)
        {
            return Err(SimilarityIndexFamilyError::ChecksumMismatch);
        }
        let partition_count = usize::try_from(get_u32(header, 32))
            .map_err(|_| SimilarityIndexFamilyError::ArithmeticOverflow)?;
        if encoded_length(partition_count)? != bytes.len() {
            return Err(SimilarityIndexFamilyError::InvalidLength);
        }
        let mut expected_hash = [0_u8; 32];
        expected_hash.copy_from_slice(&footer[FOOTER_HASH_OFFSET..FOOTER_HASH_OFFSET + 32]);
        if blake3::hash(&bytes[..footer_offset]).as_bytes() != &expected_hash {
            return Err(SimilarityIndexFamilyError::HashMismatch);
        }
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(partition_count)
            .map_err(|_| SimilarityIndexFamilyError::OutOfMemory)?;
        for ordinal in 0..partition_count {
            let start =
                SIMILARITY_FAMILY_HEADER_BYTES + ordinal * SIMILARITY_FAMILY_PARTITION_BYTES;
            partitions.push(SimilarityIndexPartitionRef::decode(
                &bytes[start..start + SIMILARITY_FAMILY_PARTITION_BYTES],
            )?);
        }
        let mut source_exact_run_set_id = [0_u8; 32];
        source_exact_run_set_id.copy_from_slice(&header[48..80]);
        let source_exact_run_set_id = if source_exact_run_set_id == [0; 32] {
            None
        } else {
            Some(
                ExactIndexRunSetId::from_bytes(source_exact_run_set_id)
                    .ok_or(SimilarityIndexFamilyError::InvalidHeader)?,
            )
        };
        Self::new_inner(
            get_u16(header, 12),
            get_u16(header, 14),
            get_u64(header, 16),
            get_u64(header, 24),
            source_exact_run_set_id,
            partitions,
        )
    }
}

fn validate_partitions(
    partitions: &[SimilarityIndexPartitionRef],
) -> Result<(), SimilarityIndexFamilyError> {
    let partition_count = u16::try_from(partitions.len())
        .map_err(|_| SimilarityIndexFamilyError::TooManyPartitions)?;
    for (ordinal, partition) in partitions.iter().copied().enumerate() {
        if partition.partition_count != partition_count
            || usize::from(partition.partition_ordinal) != ordinal
        {
            return Err(SimilarityIndexFamilyError::InvalidPartition);
        }
    }
    if partitions
        .windows(2)
        .any(|pair| pair[0].maximum_bucket_key >= pair[1].minimum_bucket_key)
    {
        return Err(SimilarityIndexFamilyError::OverlappingPartitions);
    }
    Ok(())
}

fn encode_identity(
    output: &mut [u8],
    magic: [u8; 8],
    family: &SimilarityIndexRunFamily,
) -> Result<(), SimilarityIndexFamilyError> {
    output.fill(0);
    output[..8].copy_from_slice(&magic);
    put_u16(output, 8, FORMAT_VERSION);
    put_u16(
        output,
        10,
        u16::try_from(SIMILARITY_FAMILY_HEADER_BYTES)
            .expect("ASSERT: Similarity family Header length fits u16"),
    );
    put_u16(output, 12, family.fingerprint_profile);
    put_u16(output, 14, family.bucket_profile);
    put_u64(output, 16, family.generation);
    put_u64(output, 24, family.logical_entry_count);
    put_u32(
        output,
        32,
        u32::try_from(family.partitions.len())
            .map_err(|_| SimilarityIndexFamilyError::TooManyPartitions)?,
    );
    put_u16(
        output,
        36,
        u16::try_from(SIMILARITY_FAMILY_PARTITION_BYTES)
            .map_err(|_| SimilarityIndexFamilyError::ArithmeticOverflow)?,
    );
    put_u64(
        output,
        40,
        u64::try_from(encoded_length(family.partitions.len())?)
            .map_err(|_| SimilarityIndexFamilyError::ArithmeticOverflow)?,
    );
    if let Some(source_exact_run_set_id) = family.source_exact_run_set_id {
        output[48..80].copy_from_slice(&source_exact_run_set_id.bytes());
    }
    Ok(())
}

fn verify_identity(
    bytes: &[u8],
    magic: [u8; 8],
    physical_length: usize,
    footer: bool,
) -> Result<(), SimilarityIndexFamilyError> {
    if bytes.len() != SIMILARITY_FAMILY_HEADER_BYTES
        || bytes[..8] != magic
        || get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != SIMILARITY_FAMILY_HEADER_BYTES
        || get_u16(bytes, 12) == 0
        || get_u16(bytes, 14) == 0
        || get_u64(bytes, 16) == 0
        || usize::from(get_u16(bytes, 36)) != SIMILARITY_FAMILY_PARTITION_BYTES
        || usize::try_from(get_u64(bytes, 40)).ok() != Some(physical_length)
    {
        return Err(SimilarityIndexFamilyError::InvalidHeader);
    }
    let reserved_start = if footer { 116 } else { 84 };
    if bytes[reserved_start..].iter().any(|byte| *byte != 0) {
        return Err(SimilarityIndexFamilyError::InvalidHeader);
    }
    Ok(())
}

fn encoded_length(partition_count: usize) -> Result<usize, SimilarityIndexFamilyError> {
    let length = SIMILARITY_FAMILY_HEADER_BYTES
        .checked_add(
            partition_count
                .checked_mul(SIMILARITY_FAMILY_PARTITION_BYTES)
                .ok_or(SimilarityIndexFamilyError::ArithmeticOverflow)?,
        )
        .and_then(|value| value.checked_add(SIMILARITY_FAMILY_FOOTER_BYTES))
        .ok_or(SimilarityIndexFamilyError::ArithmeticOverflow)?;
    if length > SIMILARITY_FAMILY_MAX_BYTES {
        return Err(SimilarityIndexFamilyError::TooManyPartitions);
    }
    Ok(length)
}

fn encode_bucket_key(key: SimilarityBucketKey, output: &mut [u8]) {
    put_u16(output, 0, key.fingerprint_profile());
    output[2] = key.slot();
    output[3] = 0;
    put_u32(output, 4, key.logical_length());
    put_u64(output, 8, key.superfeature());
}

fn decode_bucket_key(input: &[u8]) -> Result<SimilarityBucketKey, SimilarityIndexFamilyError> {
    if input.len() != 16 || input[3] != 0 {
        return Err(SimilarityIndexFamilyError::InvalidPartition);
    }
    SimilarityBucketKey::new(
        get_u16(input, 0),
        input[2],
        get_u32(input, 4),
        get_u64(input, 8),
    )
    .map_err(|_| SimilarityIndexFamilyError::InvalidPartition)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimilarityIndexFamilyError {
    InvalidLength,
    InvalidHeader,
    InvalidFamily,
    InvalidPartition,
    OverlappingPartitions,
    TooManyPartitions,
    ChecksumMismatch,
    HashMismatch,
    ArithmeticOverflow,
    OutOfMemory,
}

impl fmt::Display for SimilarityIndexFamilyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for SimilarityIndexFamilyError {}
