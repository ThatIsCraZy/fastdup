//! Depth-one Zstd-prefix and Sparse-XOR codecs with verified base/target identities.
use super::aligned::AlignedContainerBuilder;
use super::payload::VerifiedChunkPayload;
use super::records::{RawRecord, validate_logical_chunk_length};
use super::{
    CHUNK_TABLE_ENTRY_BYTES, CHUNK_TABLE_ENTRY_BYTES_U16, ChunkId, FORMAT_VERSION, FormatError,
    MAX_RECORD_BYTES, MIN_RAW_RECORD_BYTES, RAW_PAYLOAD_OFFSET, RAW_PAYLOAD_OFFSET_U32,
    RECORD_ALIGNMENT, RECORD_CRC_OFFSET, RECORD_HEADER_BYTES, RECORD_HEADER_BYTES_U16,
    RECORD_HEADER_BYTES_U32, RECORD_MAGIC, SPARSE_XOR_CODEC, ZSTD_PREFIX_CODEC,
    ZSTD_PREFIX_LEVEL_V1, align_up_usize, crc32c_with_zeroed_field, get_u16, get_u32, get_u64,
    put_u16, put_u32,
};

/// One verified logical dependency named by a Depth-1 dependent record.
///
/// The reference contains no physical Location. A reader must resolve it to
/// an independently decodable Chunk before calling [`ZstdPrefixRecord::decode`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependentDependency {
    pub(super) chunk_id: ChunkId,
    logical_length: u32,
}

impl DependentDependency {
    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }
}

/// Compatibility name for callers that only handle codec-3 records.
pub type ZstdPrefixDependency = DependentDependency;

/// One already-compressed codec-3 record awaiting Container assembly.
///
/// The opaque value carries prior writer evidence and owns its Zstd frame.
/// Moving it into a Container therefore avoids a second compression and a
/// temporary encoded-record copy in the ingest hot loop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedZstdPrefixRecord {
    dependency: ZstdPrefixDependency,
    target_id: ChunkId,
    logical_length: u32,
    frame: Box<[u8]>,
}

impl PreparedZstdPrefixRecord {
    fn record_length(&self) -> Result<usize, FormatError> {
        zstd_prefix_record_length(self.logical_length, self.frame.len())
    }

    fn encode_into(&self, destination: &mut [u8]) -> Result<(), FormatError> {
        encode_zstd_prefix_record_from_frame_into(
            self.dependency,
            self.target_id,
            self.logical_length,
            &self.frame,
            destination,
        )
    }

    /// Returns the dependency-plus-frame bytes used by the v1 cost policy.
    #[must_use]
    pub fn encoded_payload_bytes(&self) -> usize {
        32 + self.frame.len()
    }

    #[must_use]
    pub const fn target_id(&self) -> ChunkId {
        self.target_id
    }
}

/// The durable codec selected for one prepared dependent record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependentCodec {
    ZstdPrefix,
    SparseXor,
}

/// One canonical changed-byte run in a codec-4 Sparse-XOR record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SparseXorRun {
    logical_offset: u32,
    length: u32,
}

impl SparseXorRun {
    /// Creates one run. Full ordering and payload validation happens when the
    /// prepared record is constructed.
    #[must_use]
    pub const fn new(logical_offset: u32, length: u32) -> Self {
        Self {
            logical_offset,
            length,
        }
    }

    #[must_use]
    pub const fn logical_offset(self) -> u32 {
        self.logical_offset
    }

    #[must_use]
    pub const fn length(self) -> u32 {
        self.length
    }
}

/// One codec-4 record awaiting Container assembly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSparseXorRecord {
    dependency: DependentDependency,
    target_id: ChunkId,
    logical_length: u32,
    runs: Box<[SparseXorRun]>,
    xor_bytes: Box<[u8]>,
}

impl PreparedSparseXorRecord {
    fn record_length(&self) -> Result<usize, FormatError> {
        sparse_xor_record_length(self.logical_length, self.runs.len(), self.xor_bytes.len())
    }

    fn encode_into(&self, destination: &mut [u8]) -> Result<(), FormatError> {
        encode_sparse_xor_record_into(
            self.dependency,
            self.target_id,
            self.logical_length,
            &self.runs,
            &self.xor_bytes,
            destination,
        )
    }

    /// Returns the dependency, run table and XOR bytes charged by the v1 cost
    /// policy.
    #[must_use]
    pub fn encoded_payload_bytes(&self) -> usize {
        32_usize
            .saturating_add(4)
            .saturating_add(self.runs.len().saturating_mul(8))
            .saturating_add(self.xor_bytes.len())
    }

    #[must_use]
    pub const fn target_id(&self) -> ChunkId {
        self.target_id
    }
}

/// One prepared Depth-1 record behind the Container writer's dependent-codec
/// seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedDependentRecord {
    ZstdPrefix(PreparedZstdPrefixRecord),
    SparseXor(PreparedSparseXorRecord),
}

impl PreparedDependentRecord {
    /// Appends initialized payload bytes using this codec's own metadata layout.
    /// `length` is the checked result of `record_length` from the assembly plan.
    pub(super) fn append_to(
        &self,
        builder: &mut AlignedContainerBuilder,
        length: usize,
    ) -> Result<(), FormatError> {
        match self {
            Self::ZstdPrefix(record) => {
                builder.append_record(length, RAW_PAYLOAD_OFFSET, &record.frame, |bytes| {
                    write_zstd_prefix_metadata(
                        record.dependency,
                        record.target_id,
                        record.logical_length,
                        &record.frame,
                        bytes,
                    )
                })
            }
            Self::SparseXor(record) => {
                let metadata_length = RAW_PAYLOAD_OFFSET + record.runs.len() * 8;
                builder.append_record(length, metadata_length, &record.xor_bytes, |bytes| {
                    write_sparse_xor_metadata(
                        record.dependency,
                        record.target_id,
                        record.logical_length,
                        &record.runs,
                        &record.xor_bytes,
                        bytes,
                    )
                })
            }
        }
    }

    pub(super) fn record_length(&self) -> Result<usize, FormatError> {
        match self {
            Self::ZstdPrefix(record) => record.record_length(),
            Self::SparseXor(record) => record.record_length(),
        }
    }

    pub(super) fn encode_into(&self, destination: &mut [u8]) -> Result<(), FormatError> {
        match self {
            Self::ZstdPrefix(record) => record.encode_into(destination),
            Self::SparseXor(record) => record.encode_into(destination),
        }
    }

    #[must_use]
    pub const fn codec(&self) -> DependentCodec {
        match self {
            Self::ZstdPrefix(_) => DependentCodec::ZstdPrefix,
            Self::SparseXor(_) => DependentCodec::SparseXor,
        }
    }

    pub(super) const fn codec_id(&self) -> u16 {
        match self {
            Self::ZstdPrefix(_) => ZSTD_PREFIX_CODEC,
            Self::SparseXor(_) => SPARSE_XOR_CODEC,
        }
    }

    #[must_use]
    pub const fn dependency(&self) -> DependentDependency {
        match self {
            Self::ZstdPrefix(record) => record.dependency,
            Self::SparseXor(record) => record.dependency,
        }
    }

    #[must_use]
    pub const fn target_id(&self) -> ChunkId {
        match self {
            Self::ZstdPrefix(record) => record.target_id,
            Self::SparseXor(record) => record.target_id,
        }
    }

    #[must_use]
    pub const fn logical_length(&self) -> u32 {
        match self {
            Self::ZstdPrefix(record) => record.logical_length,
            Self::SparseXor(record) => record.logical_length,
        }
    }

    #[must_use]
    pub fn encoded_payload_bytes(&self) -> usize {
        match self {
            Self::ZstdPrefix(record) => record.encoded_payload_bytes(),
            Self::SparseXor(record) => record.encoded_payload_bytes(),
        }
    }
}

impl From<PreparedZstdPrefixRecord> for PreparedDependentRecord {
    fn from(record: PreparedZstdPrefixRecord) -> Self {
        Self::ZstdPrefix(record)
    }
}

impl From<PreparedSparseXorRecord> for PreparedDependentRecord {
    fn from(record: PreparedSparseXorRecord) -> Self {
        Self::SparseXor(record)
    }
}

/// Field-by-field codec-3 Encoding Record for one Depth-1 Zstd Prefix target.
///
/// The fixed header stores the Base Chunk ID and length. The one-entry Chunk
/// Table stores the target identity and length. The payload is one Zstd frame
/// encoded against the exact Base bytes. Neither Rust memory layout nor a
/// physical Base Location is serialized.
#[derive(Clone, Copy, Debug, Default)]
pub struct ZstdPrefixRecord;

impl ZstdPrefixRecord {
    /// Wraps a Prefix frame produced by an earlier bounded trial without
    /// compressing or hashing the target again.
    ///
    /// # Errors
    ///
    /// Rejects invalid lengths, empty frames, or impossible record geometry.
    pub fn prepare_precompressed(
        base_id: ChunkId,
        logical_length: u32,
        target_id: ChunkId,
        frame: Box<[u8]>,
    ) -> Result<PreparedZstdPrefixRecord, FormatError> {
        if base_id == ChunkId::from_bytes([0; 32]) {
            return Err(FormatError::InvalidZstdPrefixRecord);
        }
        let dependency = ZstdPrefixDependency {
            chunk_id: base_id,
            logical_length,
        };
        zstd_prefix_record_length(logical_length, frame.len())?;
        Ok(PreparedZstdPrefixRecord {
            dependency,
            target_id,
            logical_length,
            frame,
        })
    }

    /// Encodes one same-length target against a verified Base byte slice.
    ///
    /// This function emits a record but does not decide whether Prefix beats
    /// RAW, independent Zstd, or another Delta codec. The caller owns that
    /// versioned physical-cost decision.
    ///
    /// # Errors
    ///
    /// Returns a length, allocation, arithmetic, or Zstd error.
    ///
    /// # Panics
    ///
    /// Panics only if Zstd reports writing beyond the destination supplied to
    /// it, an internal codec-contract violation.
    pub fn encode(base: &[u8], target: &[u8]) -> Result<Vec<u8>, FormatError> {
        validate_logical_chunk_length(base.len())?;
        validate_logical_chunk_length(target.len())?;
        if base.len() != target.len() {
            return Err(FormatError::InvalidZstdPrefixRecord);
        }

        let mut frame = Vec::new();
        frame
            .try_reserve_exact(zstd::zstd_safe::compress_bound(target.len()))
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        frame.resize(zstd::zstd_safe::compress_bound(target.len()), 0);
        let mut context = zstd::zstd_safe::CCtx::try_create().ok_or(FormatError::ZstdFailure)?;
        context
            .set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(
                ZSTD_PREFIX_LEVEL_V1,
            ))
            .map_err(|_| FormatError::ZstdFailure)?;
        context
            .set_parameter(zstd::zstd_safe::CParameter::NbWorkers(0))
            .map_err(|_| FormatError::ZstdFailure)?;
        context
            .set_pledged_src_size(Some(
                u64::try_from(target.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
            ))
            .map_err(|_| FormatError::ZstdFailure)?;
        context
            .ref_prefix(base)
            .map_err(|_| FormatError::ZstdFailure)?;
        let written = context
            .compress2(frame.as_mut_slice(), target)
            .map_err(|_| FormatError::ZstdFailure)?;
        assert!(
            written <= frame.len(),
            "ASSERT: Zstd Prefix cannot report bytes beyond its destination"
        );
        frame.truncate(written);
        if frame.is_empty() {
            return Err(FormatError::ZstdFailure);
        }

        encode_zstd_prefix_record_from_frame(
            ZstdPrefixDependency {
                chunk_id: ChunkId::of(base),
                logical_length: u32::try_from(base.len())
                    .map_err(|_| FormatError::ArithmeticOverflow)?,
            },
            ChunkId::of(target),
            u32::try_from(target.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
            &frame,
        )
    }

    /// Validates record structure and CRC before returning its logical Base.
    ///
    /// This method does not decode the target. It is safe to use for planning
    /// a bounded Base lookup because malformed dependency metadata is rejected
    /// before the ID escapes.
    ///
    /// # Errors
    ///
    /// Returns a structural, checksum, length, or codec error.
    pub fn dependency(bytes: &[u8]) -> Result<ZstdPrefixDependency, FormatError> {
        validate_zstd_prefix_record(bytes)?;
        let mut chunk_id = [0_u8; 32];
        chunk_id.copy_from_slice(&bytes[64..96]);
        Ok(ZstdPrefixDependency {
            chunk_id: ChunkId::from_bytes(chunk_id),
            logical_length: get_u32(bytes, 100),
        })
    }

    /// Decodes and verifies the exact target against resolved Base bytes.
    ///
    /// The reader checks Base length and BLAKE3 identity before asking Zstd to
    /// decode. It then checks decoded length and target BLAKE3 before returning
    /// any logical bytes.
    ///
    /// # Errors
    ///
    /// Returns a record, Base, Zstd, decoded-length, or target-integrity error.
    pub fn decode(bytes: &[u8], base: &[u8]) -> Result<RawRecord, FormatError> {
        let dependency = Self::dependency(bytes)?;
        if usize::try_from(dependency.logical_length) != Ok(base.len())
            || ChunkId::of(base) != dependency.chunk_id
        {
            return Err(FormatError::ZstdPrefixBaseMismatch);
        }
        Self::decode_after_base_verification(bytes, base)
    }

    /// Decodes one target using a Base whose length and BLAKE3 identity were
    /// already established by the independent record verifier.
    ///
    /// # Errors
    ///
    /// Returns a Base pairing, record, Zstd, decoded-length, or target-integrity
    /// error. The target identity is always recomputed.
    pub fn decode_with_verified_base(
        bytes: &[u8],
        base: &VerifiedChunkPayload,
    ) -> Result<RawRecord, FormatError> {
        let dependency = Self::dependency(bytes)?;
        if usize::try_from(dependency.logical_length) != Ok(base.len())
            || dependency.chunk_id != base.chunk_id()
        {
            return Err(FormatError::ZstdPrefixBaseMismatch);
        }
        Self::decode_after_base_verification(bytes, base.as_slice())
    }

    fn decode_after_base_verification(bytes: &[u8], base: &[u8]) -> Result<RawRecord, FormatError> {
        let decoded_length =
            usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let payload_offset =
            usize::try_from(get_u32(bytes, 40)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let payload_length =
            usize::try_from(get_u32(bytes, 44)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let payload_end = payload_offset
            .checked_add(payload_length)
            .ok_or(FormatError::ArithmeticOverflow)?;

        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(decoded_length)
            .map_err(|_| FormatError::ArithmeticOverflow)?;
        let mut context = zstd::zstd_safe::DCtx::try_create().ok_or(FormatError::ZstdFailure)?;
        context
            .ref_prefix(base)
            .map_err(|_| FormatError::ZstdFailure)?;
        let written = context
            .decompress(&mut decoded, &bytes[payload_offset..payload_end])
            .map_err(|_| FormatError::ZstdFailure)?;
        if written != decoded_length {
            return Err(FormatError::InvalidZstdPrefixRecord);
        }

        let mut target_id = [0_u8; 32];
        target_id.copy_from_slice(&bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32]);
        let target_id = ChunkId::from_bytes(target_id);
        if ChunkId::of(&decoded) != target_id {
            return Err(FormatError::ChunkHashMismatch);
        }
        Ok(RawRecord {
            payload: VerifiedChunkPayload::from_owned(target_id, decoded),
        })
    }
}

fn encode_zstd_prefix_record_from_frame(
    dependency: ZstdPrefixDependency,
    target_id: ChunkId,
    logical_length: u32,
    frame: &[u8],
) -> Result<Vec<u8>, FormatError> {
    let record_length = zstd_prefix_record_length(logical_length, frame.len())?;
    let mut bytes = vec![0_u8; record_length];
    encode_zstd_prefix_record_from_frame_into(
        dependency,
        target_id,
        logical_length,
        frame,
        &mut bytes,
    )?;
    Ok(bytes)
}

fn zstd_prefix_record_length(
    logical_length: u32,
    frame_length: usize,
) -> Result<usize, FormatError> {
    validate_logical_chunk_length(
        usize::try_from(logical_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    )?;
    if frame_length == 0 {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    let payload_offset = RAW_PAYLOAD_OFFSET;
    let payload_end = payload_offset
        .checked_add(frame_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if record_length > MAX_RECORD_BYTES {
        return Err(FormatError::InvalidRecordLength(record_length));
    }
    Ok(record_length)
}

fn encode_zstd_prefix_record_from_frame_into(
    dependency: ZstdPrefixDependency,
    target_id: ChunkId,
    logical_length: u32,
    frame: &[u8],
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    let record_length = zstd_prefix_record_length(logical_length, frame.len())?;
    if bytes.len() != record_length {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    let metadata_length = RAW_PAYLOAD_OFFSET;
    bytes[..metadata_length].fill(0);
    write_zstd_prefix_metadata(
        dependency,
        target_id,
        logical_length,
        frame,
        &mut bytes[..metadata_length],
    )?;
    let payload_end = metadata_length + frame.len();
    bytes[metadata_length..payload_end].copy_from_slice(frame);
    bytes[payload_end..].fill(0);
    let checksum = crc32c::crc32c(bytes);
    put_u32(bytes, RECORD_CRC_OFFSET, checksum);
    Ok(())
}

fn write_zstd_prefix_metadata(
    dependency: ZstdPrefixDependency,
    target_id: ChunkId,
    logical_length: u32,
    frame: &[u8],
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    assert_eq!(bytes.len(), RAW_PAYLOAD_OFFSET);
    if dependency.logical_length != logical_length {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    let record_length = zstd_prefix_record_length(logical_length, frame.len())?;

    bytes[0..8].copy_from_slice(RECORD_MAGIC);
    put_u16(bytes, 8, FORMAT_VERSION);
    put_u16(bytes, 10, RECORD_HEADER_BYTES_U16);
    put_u16(bytes, 12, ZSTD_PREFIX_CODEC);
    put_u32(
        bytes,
        32,
        u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(bytes, 36, logical_length);
    put_u32(bytes, 40, RAW_PAYLOAD_OFFSET_U32);
    put_u32(
        bytes,
        44,
        u32::try_from(frame.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(bytes, 48, RECORD_HEADER_BYTES_U32);
    put_u16(bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
    put_u32(bytes, 56, 1);
    bytes[64..96].copy_from_slice(&dependency.chunk_id.0);
    bytes[96..100].copy_from_slice(&ZSTD_PREFIX_LEVEL_V1.to_le_bytes());
    put_u32(bytes, 100, dependency.logical_length);
    bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32].copy_from_slice(&target_id.0);
    put_u32(bytes, RECORD_HEADER_BYTES + 36, logical_length);
    Ok(())
}

pub(super) fn validate_zstd_prefix_record(bytes: &[u8]) -> Result<(), FormatError> {
    if bytes.len() < MIN_RAW_RECORD_BYTES
        || bytes.len() > MAX_RECORD_BYTES
        || !bytes.len().is_multiple_of(usize::from(RECORD_ALIGNMENT))
        || &bytes[0..8] != RECORD_MAGIC
        || get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECORD_HEADER_BYTES
        || get_u16(bytes, 12) != ZSTD_PREFIX_CODEC
        || get_u16(bytes, 14) != 0
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || usize::try_from(get_u32(bytes, 32)) != Ok(bytes.len())
        || usize::try_from(get_u32(bytes, 40)) != Ok(RAW_PAYLOAD_OFFSET)
        || usize::try_from(get_u32(bytes, 48)) != Ok(RECORD_HEADER_BYTES)
        || usize::from(get_u16(bytes, 52)) != CHUNK_TABLE_ENTRY_BYTES
        || get_u16(bytes, 54) != 0
        || get_u32(bytes, 56) != 1
        || i32::from_le_bytes(
            bytes[96..100]
                .try_into()
                .expect("ASSERT: fixed Prefix level field is four bytes"),
        ) != ZSTD_PREFIX_LEVEL_V1
        || bytes[104..128].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    if crc32c_with_zeroed_field(bytes, RECORD_CRC_OFFSET) != get_u32(bytes, RECORD_CRC_OFFSET) {
        return Err(FormatError::RecordChecksumMismatch);
    }
    let logical_length =
        usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
    validate_logical_chunk_length(logical_length)?;
    if get_u32(bytes, 100) != get_u32(bytes, 36)
        || bytes[64..96].iter().all(|byte| *byte == 0)
        || get_u32(bytes, RECORD_HEADER_BYTES + 32) != 0
        || get_u32(bytes, RECORD_HEADER_BYTES + 36) != get_u32(bytes, 36)
        || bytes[RECORD_HEADER_BYTES + 40..RAW_PAYLOAD_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    let payload_length =
        usize::try_from(get_u32(bytes, 44)).map_err(|_| FormatError::ArithmeticOverflow)?;
    if payload_length == 0 {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    let payload_end = RAW_PAYLOAD_OFFSET
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    if payload_end > bytes.len()
        || align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))? != bytes.len()
        || bytes[payload_end..].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidZstdPrefixRecord);
    }
    Ok(())
}

/// Field-by-field codec-4 record for one Depth-1 Sparse-XOR target.
#[derive(Clone, Copy, Debug, Default)]
pub struct SparseXorRecord;

impl SparseXorRecord {
    /// Wraps canonical runs and XOR bytes produced by a bounded writer trial.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid lengths, noncanonical runs, zero XOR bytes,
    /// or a record that exceeds the format bounds.
    pub fn prepare(
        base_id: ChunkId,
        logical_length: u32,
        target_id: ChunkId,
        runs: Box<[SparseXorRun]>,
        xor_bytes: Box<[u8]>,
    ) -> Result<PreparedSparseXorRecord, FormatError> {
        let dependency = DependentDependency {
            chunk_id: base_id,
            logical_length,
        };
        validate_sparse_xor_parts(logical_length, &runs, &xor_bytes)?;
        sparse_xor_record_length(logical_length, runs.len(), xor_bytes.len())?;
        Ok(PreparedSparseXorRecord {
            dependency,
            target_id,
            logical_length,
            runs,
            xor_bytes,
        })
    }

    /// Encodes a target against a same-length Base using the canonical scalar
    /// oracle. Production trials use the SIMD-equivalent store implementation.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, unequal, unchanged, or oversized chunks, or
    /// when the canonical record cannot be represented.
    pub fn encode(base: &[u8], target: &[u8]) -> Result<Vec<u8>, FormatError> {
        validate_logical_chunk_length(base.len())?;
        validate_logical_chunk_length(target.len())?;
        if base.len() != target.len() {
            return Err(FormatError::InvalidSparseXorRecord);
        }
        let mut runs = Vec::new();
        let mut xor_bytes = Vec::new();
        let mut cursor = 0_usize;
        while cursor < target.len() {
            if base[cursor] == target[cursor] {
                cursor += 1;
                continue;
            }
            let start = cursor;
            while cursor < target.len() && base[cursor] != target[cursor] {
                xor_bytes.push(base[cursor] ^ target[cursor]);
                cursor += 1;
            }
            runs.push(SparseXorRun::new(
                u32::try_from(start).map_err(|_| FormatError::ArithmeticOverflow)?,
                u32::try_from(cursor - start).map_err(|_| FormatError::ArithmeticOverflow)?,
            ));
        }
        let prepared = Self::prepare(
            ChunkId::of(base),
            u32::try_from(base.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
            ChunkId::of(target),
            runs.into_boxed_slice(),
            xor_bytes.into_boxed_slice(),
        )?;
        let mut bytes = vec![0_u8; prepared.record_length()?];
        prepared.encode_into(&mut bytes)?;
        Ok(bytes)
    }

    /// Returns the authenticated Base dependency from a codec-4 record.
    ///
    /// # Errors
    ///
    /// Returns a record, checksum, geometry, or bounds error.
    pub fn dependency(bytes: &[u8]) -> Result<DependentDependency, FormatError> {
        validate_sparse_xor_record(bytes)?;
        let mut chunk_id = [0_u8; 32];
        chunk_id.copy_from_slice(&bytes[64..96]);
        Ok(DependentDependency {
            chunk_id: ChunkId::from_bytes(chunk_id),
            logical_length: get_u32(bytes, 100),
        })
    }

    /// Reconstructs and verifies a codec-4 target from its Base bytes.
    ///
    /// # Errors
    ///
    /// Returns a record, Base, bounds, or target-integrity error.
    pub fn decode(bytes: &[u8], base: &[u8]) -> Result<RawRecord, FormatError> {
        let dependency = Self::dependency(bytes)?;
        if usize::try_from(dependency.logical_length) != Ok(base.len())
            || ChunkId::of(base) != dependency.chunk_id
        {
            return Err(FormatError::DependentBaseMismatch);
        }
        Self::decode_after_base_verification(bytes, base)
    }

    /// Reconstructs a codec-4 target while reusing verified Base identity.
    ///
    /// # Errors
    ///
    /// Returns a record, Base, bounds, or target-integrity error.
    pub fn decode_with_verified_base(
        bytes: &[u8],
        base: &VerifiedChunkPayload,
    ) -> Result<RawRecord, FormatError> {
        let dependency = Self::dependency(bytes)?;
        if usize::try_from(dependency.logical_length) != Ok(base.len())
            || dependency.chunk_id != base.chunk_id()
        {
            return Err(FormatError::DependentBaseMismatch);
        }
        Self::decode_after_base_verification(bytes, base.as_slice())
    }

    fn decode_after_base_verification(bytes: &[u8], base: &[u8]) -> Result<RawRecord, FormatError> {
        let logical_length =
            usize::try_from(get_u32(bytes, 36)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let run_count =
            usize::try_from(get_u32(bytes, 104)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let xor_length =
            usize::try_from(get_u32(bytes, 108)).map_err(|_| FormatError::ArithmeticOverflow)?;
        let run_table_bytes = run_count
            .checked_mul(8)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let run_table_end = RAW_PAYLOAD_OFFSET
            .checked_add(run_table_bytes)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let xor_end = run_table_end
            .checked_add(xor_length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        let xor = &bytes[run_table_end..xor_end];
        let mut decoded = base.to_vec();
        let mut xor_cursor = 0_usize;
        for ordinal in 0..run_count {
            let entry = RAW_PAYLOAD_OFFSET + ordinal * 8;
            let logical_offset = usize::try_from(get_u32(bytes, entry))
                .map_err(|_| FormatError::ArithmeticOverflow)?;
            let run_length = usize::try_from(get_u32(bytes, entry + 4))
                .map_err(|_| FormatError::ArithmeticOverflow)?;
            let logical_end = logical_offset
                .checked_add(run_length)
                .ok_or(FormatError::ArithmeticOverflow)?;
            let next_xor = xor_cursor
                .checked_add(run_length)
                .ok_or(FormatError::ArithmeticOverflow)?;
            if logical_end > logical_length || next_xor > xor.len() {
                return Err(FormatError::InvalidSparseXorRecord);
            }
            for (target, difference) in decoded[logical_offset..logical_end]
                .iter_mut()
                .zip(&xor[xor_cursor..next_xor])
            {
                *target ^= difference;
            }
            xor_cursor = next_xor;
        }
        if xor_cursor != xor.len() {
            return Err(FormatError::InvalidSparseXorRecord);
        }
        let mut target_id = [0_u8; 32];
        target_id.copy_from_slice(&bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32]);
        let target_id = ChunkId::from_bytes(target_id);
        if ChunkId::of(&decoded) != target_id {
            return Err(FormatError::ChunkHashMismatch);
        }
        Ok(RawRecord {
            payload: VerifiedChunkPayload::from_owned(target_id, decoded),
        })
    }
}

/// Validated bytes and dependency cannot be separated or forged by callers.
/// Decode reuses their CRC/run-geometry proof while checking Base and target.
pub(super) struct ValidatedDependentRecord<'a> {
    bytes: &'a [u8],
    pub(super) dependency: DependentDependency,
}

impl<'a> ValidatedDependentRecord<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Result<Self, FormatError> {
        Ok(Self {
            bytes,
            dependency: DependentRecord::dependency(bytes)?,
        })
    }

    fn base_mismatch(&self) -> FormatError {
        if get_u16(self.bytes, 12) == ZSTD_PREFIX_CODEC {
            FormatError::ZstdPrefixBaseMismatch
        } else {
            FormatError::DependentBaseMismatch
        }
    }

    pub(super) fn decode(self, base: &[u8]) -> Result<RawRecord, FormatError> {
        if usize::try_from(self.dependency.logical_length()) != Ok(base.len())
            || ChunkId::of(base) != self.dependency.chunk_id()
        {
            return Err(self.base_mismatch());
        }
        self.decode_after_base_verification(base)
    }

    pub(super) fn decode_with_verified_base(
        self,
        base: &VerifiedChunkPayload,
    ) -> Result<RawRecord, FormatError> {
        if usize::try_from(self.dependency.logical_length()) != Ok(base.len())
            || base.chunk_id() != self.dependency.chunk_id()
        {
            return Err(self.base_mismatch());
        }
        self.decode_after_base_verification(base.as_slice())
    }

    fn decode_after_base_verification(self, base: &[u8]) -> Result<RawRecord, FormatError> {
        match get_u16(self.bytes, 12) {
            ZSTD_PREFIX_CODEC => ZstdPrefixRecord::decode_after_base_verification(self.bytes, base),
            SPARSE_XOR_CODEC => SparseXorRecord::decode_after_base_verification(self.bytes, base),
            _ => unreachable!("validated dependent codec"),
        }
    }
}

/// Codec-independent dispatcher for every durable Depth-1 record.
#[derive(Clone, Copy, Debug, Default)]
pub struct DependentRecord;

impl DependentRecord {
    /// Returns the authenticated Base dependency for any known dependent codec.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown, malformed, or corrupt dependent record.
    pub fn dependency(bytes: &[u8]) -> Result<DependentDependency, FormatError> {
        match record_codec(bytes)? {
            ZSTD_PREFIX_CODEC => ZstdPrefixRecord::dependency(bytes),
            SPARSE_XOR_CODEC => SparseXorRecord::dependency(bytes),
            _ => Err(FormatError::InvalidDependentRecord),
        }
    }

    /// Reconstructs and verifies any known dependent codec from Base bytes.
    ///
    /// # Errors
    ///
    /// Returns a record, Base, codec, bounds, or target-integrity error.
    pub fn decode(bytes: &[u8], base: &[u8]) -> Result<RawRecord, FormatError> {
        match record_codec(bytes)? {
            ZSTD_PREFIX_CODEC => ZstdPrefixRecord::decode(bytes, base),
            SPARSE_XOR_CODEC => SparseXorRecord::decode(bytes, base),
            _ => Err(FormatError::InvalidDependentRecord),
        }
    }

    /// Reconstructs any known dependent codec using verified Base identity.
    ///
    /// # Errors
    ///
    /// Returns a record, Base, codec, bounds, or target-integrity error.
    pub fn decode_with_verified_base(
        bytes: &[u8],
        base: &VerifiedChunkPayload,
    ) -> Result<RawRecord, FormatError> {
        match record_codec(bytes)? {
            ZSTD_PREFIX_CODEC => ZstdPrefixRecord::decode_with_verified_base(bytes, base),
            SPARSE_XOR_CODEC => SparseXorRecord::decode_with_verified_base(bytes, base),
            _ => Err(FormatError::InvalidDependentRecord),
        }
    }
}

fn record_codec(bytes: &[u8]) -> Result<u16, FormatError> {
    if bytes.len() < RECORD_HEADER_BYTES || &bytes[0..8] != RECORD_MAGIC {
        return Err(FormatError::InvalidDependentRecord);
    }
    Ok(get_u16(bytes, 12))
}

pub(super) fn is_dependent_codec(codec_id: u16) -> bool {
    matches!(codec_id, ZSTD_PREFIX_CODEC | SPARSE_XOR_CODEC)
}

fn sparse_xor_record_length(
    logical_length: u32,
    run_count: usize,
    xor_length: usize,
) -> Result<usize, FormatError> {
    validate_logical_chunk_length(
        usize::try_from(logical_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    )?;
    if run_count == 0 || xor_length == 0 {
        return Err(FormatError::InvalidSparseXorRecord);
    }
    let payload_length = run_count
        .checked_mul(8)
        .and_then(|length| length.checked_add(xor_length))
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_end = RAW_PAYLOAD_OFFSET
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let record_length = align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))?;
    if record_length > MAX_RECORD_BYTES {
        return Err(FormatError::InvalidRecordLength(record_length));
    }
    Ok(record_length)
}

fn validate_sparse_xor_parts(
    logical_length: u32,
    runs: &[SparseXorRun],
    xor_bytes: &[u8],
) -> Result<(), FormatError> {
    validate_sparse_xor_runs(logical_length, runs.iter().copied(), xor_bytes)
}

fn validate_sparse_xor_runs(
    logical_length: u32,
    runs: impl ExactSizeIterator<Item = SparseXorRun>,
    xor_bytes: &[u8],
) -> Result<(), FormatError> {
    let logical_length =
        usize::try_from(logical_length).map_err(|_| FormatError::ArithmeticOverflow)?;
    validate_logical_chunk_length(logical_length)?;
    if runs.len() == 0 || xor_bytes.is_empty() || xor_bytes.contains(&0) {
        return Err(FormatError::InvalidSparseXorRecord);
    }
    let mut previous_end = 0_usize;
    let mut payload_bytes = 0_usize;
    for (ordinal, run) in runs.enumerate() {
        let start =
            usize::try_from(run.logical_offset).map_err(|_| FormatError::ArithmeticOverflow)?;
        let length = usize::try_from(run.length).map_err(|_| FormatError::ArithmeticOverflow)?;
        let end = start
            .checked_add(length)
            .ok_or(FormatError::ArithmeticOverflow)?;
        if length == 0 || end > logical_length || (ordinal != 0 && start <= previous_end) {
            return Err(FormatError::InvalidSparseXorRecord);
        }
        previous_end = end;
        payload_bytes = payload_bytes
            .checked_add(length)
            .ok_or(FormatError::ArithmeticOverflow)?;
    }
    if payload_bytes != xor_bytes.len() {
        return Err(FormatError::InvalidSparseXorRecord);
    }
    Ok(())
}

fn encode_sparse_xor_record_into(
    dependency: DependentDependency,
    target_id: ChunkId,
    logical_length: u32,
    runs: &[SparseXorRun],
    xor_bytes: &[u8],
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    let record_length = sparse_xor_record_length(logical_length, runs.len(), xor_bytes.len())?;
    if bytes.len() != record_length {
        return Err(FormatError::InvalidRecordLength(bytes.len()));
    }
    let metadata_length = RAW_PAYLOAD_OFFSET + runs.len() * 8;
    bytes[..metadata_length].fill(0);
    write_sparse_xor_metadata(
        dependency,
        target_id,
        logical_length,
        runs,
        xor_bytes,
        &mut bytes[..metadata_length],
    )?;
    let payload_end = metadata_length + xor_bytes.len();
    bytes[metadata_length..payload_end].copy_from_slice(xor_bytes);
    bytes[payload_end..].fill(0);
    let checksum = crc32c::crc32c(bytes);
    put_u32(bytes, RECORD_CRC_OFFSET, checksum);
    Ok(())
}

fn write_sparse_xor_metadata(
    dependency: DependentDependency,
    target_id: ChunkId,
    logical_length: u32,
    runs: &[SparseXorRun],
    xor_bytes: &[u8],
    bytes: &mut [u8],
) -> Result<(), FormatError> {
    assert_eq!(bytes.len(), RAW_PAYLOAD_OFFSET + runs.len() * 8);
    if dependency.logical_length != logical_length
        || dependency.chunk_id == ChunkId::from_bytes([0; 32])
    {
        return Err(FormatError::InvalidSparseXorRecord);
    }
    validate_sparse_xor_parts(logical_length, runs, xor_bytes)?;
    let record_length = sparse_xor_record_length(logical_length, runs.len(), xor_bytes.len())?;

    let run_table_bytes = runs
        .len()
        .checked_mul(8)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_length = run_table_bytes
        .checked_add(xor_bytes.len())
        .ok_or(FormatError::ArithmeticOverflow)?;
    bytes[0..8].copy_from_slice(RECORD_MAGIC);
    put_u16(bytes, 8, FORMAT_VERSION);
    put_u16(bytes, 10, RECORD_HEADER_BYTES_U16);
    put_u16(bytes, 12, SPARSE_XOR_CODEC);
    put_u32(
        bytes,
        32,
        u32::try_from(record_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(bytes, 36, logical_length);
    put_u32(bytes, 40, RAW_PAYLOAD_OFFSET_U32);
    put_u32(
        bytes,
        44,
        u32::try_from(payload_length).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(bytes, 48, RECORD_HEADER_BYTES_U32);
    put_u16(bytes, 52, CHUNK_TABLE_ENTRY_BYTES_U16);
    put_u32(bytes, 56, 1);
    bytes[64..96].copy_from_slice(&dependency.chunk_id.0);
    put_u32(bytes, 96, 1);
    put_u32(bytes, 100, dependency.logical_length);
    put_u32(
        bytes,
        104,
        u32::try_from(runs.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    put_u32(
        bytes,
        108,
        u32::try_from(xor_bytes.len()).map_err(|_| FormatError::ArithmeticOverflow)?,
    );
    bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + 32].copy_from_slice(&target_id.0);
    put_u32(bytes, RECORD_HEADER_BYTES + 36, logical_length);
    for (ordinal, run) in runs.iter().copied().enumerate() {
        let offset = RAW_PAYLOAD_OFFSET + ordinal * 8;
        put_u32(bytes, offset, run.logical_offset);
        put_u32(bytes, offset + 4, run.length);
    }
    Ok(())
}

pub(super) fn validate_sparse_xor_record(bytes: &[u8]) -> Result<(), FormatError> {
    if bytes.len() < MIN_RAW_RECORD_BYTES
        || bytes.len() > MAX_RECORD_BYTES
        || !bytes.len().is_multiple_of(usize::from(RECORD_ALIGNMENT))
        || &bytes[0..8] != RECORD_MAGIC
        || get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECORD_HEADER_BYTES
        || get_u16(bytes, 12) != SPARSE_XOR_CODEC
        || get_u16(bytes, 14) != 0
        || get_u64(bytes, 16) != 0
        || get_u64(bytes, 24) != 0
        || usize::try_from(get_u32(bytes, 32)) != Ok(bytes.len())
        || usize::try_from(get_u32(bytes, 40)) != Ok(RAW_PAYLOAD_OFFSET)
        || usize::try_from(get_u32(bytes, 48)) != Ok(RECORD_HEADER_BYTES)
        || usize::from(get_u16(bytes, 52)) != CHUNK_TABLE_ENTRY_BYTES
        || get_u16(bytes, 54) != 0
        || get_u32(bytes, 56) != 1
        || get_u32(bytes, 96) != 1
        || bytes[112..128].iter().any(|byte| *byte != 0)
        || get_u32(bytes, 100) != get_u32(bytes, 36)
        || bytes[64..96].iter().all(|byte| *byte == 0)
        || get_u32(bytes, RECORD_HEADER_BYTES + 32) != 0
        || get_u32(bytes, RECORD_HEADER_BYTES + 36) != get_u32(bytes, 36)
        || bytes[RECORD_HEADER_BYTES + 40..RAW_PAYLOAD_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidSparseXorRecord);
    }
    if crc32c_with_zeroed_field(bytes, RECORD_CRC_OFFSET) != get_u32(bytes, RECORD_CRC_OFFSET) {
        return Err(FormatError::RecordChecksumMismatch);
    }
    let logical_length = get_u32(bytes, 36);
    let run_count =
        usize::try_from(get_u32(bytes, 104)).map_err(|_| FormatError::ArithmeticOverflow)?;
    let xor_length =
        usize::try_from(get_u32(bytes, 108)).map_err(|_| FormatError::ArithmeticOverflow)?;
    let run_table_bytes = run_count
        .checked_mul(8)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let xor_offset = RAW_PAYLOAD_OFFSET
        .checked_add(run_table_bytes)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_length = run_table_bytes
        .checked_add(xor_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    let payload_end = RAW_PAYLOAD_OFFSET
        .checked_add(payload_length)
        .ok_or(FormatError::ArithmeticOverflow)?;
    if run_count == 0
        || xor_length == 0
        || usize::try_from(get_u32(bytes, 44)) != Ok(payload_length)
        || payload_end > bytes.len()
        || align_up_usize(payload_end, usize::from(RECORD_ALIGNMENT))? != bytes.len()
        || bytes[payload_end..].iter().any(|byte| *byte != 0)
    {
        return Err(FormatError::InvalidSparseXorRecord);
    }
    let runs = bytes[RAW_PAYLOAD_OFFSET..xor_offset]
        .chunks_exact(8)
        .map(|entry| SparseXorRun::new(get_u32(entry, 0), get_u32(entry, 4)));
    validate_sparse_xor_runs(logical_length, runs, &bytes[xor_offset..payload_end])
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_vec_output_checks_actual_decoded_length_in_both_directions() {
        let base = vec![19; 64 * 1024];
        let mut target = base.clone();
        target[123] = 7;
        let encoded = ZstdPrefixRecord::encode(&base, &target).unwrap();
        let verified = VerifiedChunkPayload::from_owned(ChunkId::of(&base), base.clone());
        assert_eq!(
            ZstdPrefixRecord::decode_with_verified_base(&encoded, &verified)
                .unwrap()
                .payload(),
            target
        );
        for declared_length in [65535, 65537] {
            let mut corrupt = encoded.clone();
            put_u32(&mut corrupt, 36, declared_length);
            // Exercise the output boundary independently of earlier CRC/shape
            // rejection: successful Zstd output must still match the exact length.
            assert!(ZstdPrefixRecord::decode_after_base_verification(&corrupt, &base).is_err());
        }
        let mut corrupt = encoded;
        corrupt[RECORD_HEADER_BYTES + CHUNK_TABLE_ENTRY_BYTES] ^= 3;
        assert!(ZstdPrefixRecord::decode_with_verified_base(&corrupt, &verified).is_err());
    }
}
