use core::fmt;

use crate::{CommitRecord, MAX_METADATA_OBJECT_BYTES, MetadataObjectId, PolicySetId};

pub const RECOVERY_CHECKPOINT_HEADER_BYTES: usize = 4_096;
pub const RECOVERY_CHECKPOINT_FOOTER_BYTES: usize = 4_096;
/// Descriptor header plus embedded Commit Record.
pub const RECOVERY_CHECKPOINT_PAYLOAD_OFFSET: u64 = 8_192;
pub const RECOVERY_CHECKPOINT_PAYLOAD_ALIGNMENT: usize = 64;
pub const RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES: usize = 80;
pub const RECOVERY_CHECKPOINT_HEAD_BYTES: usize = 4_096;
/// One Base plus at most 255 Deltas.
pub const RECOVERY_CHECKPOINT_MAX_CHAIN_FILES: u64 = 256;

const HEADER_MAGIC: [u8; 8] = *b"FDRCV002";
const FOOTER_MAGIC: [u8; 8] = *b"FDRCF002";
const HEAD_MAGIC: [u8; 8] = *b"FDRCH002";
const FORMAT_VERSION: u16 = 2;
const BLAKE3_256_ALGORITHM: u16 = 1;
const DESCRIPTOR_CRC_OFFSET: usize = 296;
const DESCRIPTOR_USED_BYTES: usize = 300;
const HEAD_CRC_OFFSET: usize = 104;
const FLAG_ALLOCATED_BYTES: u8 = 1;

const _: () = assert!(
    RECOVERY_CHECKPOINT_PAYLOAD_OFFSET
        == (RECOVERY_CHECKPOINT_HEADER_BYTES + crate::COMMIT_RECORD_BYTES) as u64
);

/// Identity of one immutable checkpoint file named by a head or another file.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RecoveryCheckpointLink {
    generation: u64,
    file_length: u64,
    body_hash: [u8; 32],
}

impl RecoveryCheckpointLink {
    /// # Errors
    ///
    /// Rejects a zero generation, length, or body hash.
    pub fn new(
        generation: u64,
        file_length: u64,
        body_hash: [u8; 32],
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        if generation == 0 || file_length == 0 || body_hash == [0; 32] {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        Ok(Self {
            generation,
            file_length,
            body_hash,
        })
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.file_length
    }

    #[must_use]
    pub const fn body_hash(self) -> [u8; 32] {
        self.body_hash
    }
}

/// Position of one checkpoint file in its chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryCheckpointChain {
    /// A complete graph.
    Base,
    /// Only objects absent from the parent's chain prefix.
    Delta {
        parent: RecoveryCheckpointLink,
        chain_index: u64,
        base_generation: u64,
    },
}

/// Totals a writer computes while emitting one checkpoint file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryCheckpointBody {
    pub object_count: u64,
    pub file_length: u64,
    pub body_hash: [u8; 32],
    pub table_hash: [u8; 32],
    pub chain_object_count: u64,
    pub chain_payload_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryCheckpointDescriptor {
    generation: u64,
    namespace_root: MetadataObjectId,
    policy_set: PolicySetId,
    body: RecoveryCheckpointBody,
    chain: RecoveryCheckpointChain,
    previous_chain: Option<RecoveryCheckpointLink>,
}

impl RecoveryCheckpointDescriptor {
    /// Constructs one authenticated descriptor from the selected Commit.
    ///
    /// # Errors
    ///
    /// Returns an error for an impossible length, count, link, or chain
    /// position, or a zero hash. `previous_chain` names the tip of the chain
    /// this file's Base replaced; every file of a chain repeats it.
    pub fn new(
        record: CommitRecord,
        body: RecoveryCheckpointBody,
        chain: RecoveryCheckpointChain,
        previous_chain: Option<RecoveryCheckpointLink>,
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        let descriptor = Self {
            generation: record.generation(),
            namespace_root: record.namespace_root(),
            policy_set: record.policy_set(),
            body,
            chain,
            previous_chain,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<(), RecoveryCheckpointFormatError> {
        let table_bytes = self
            .body
            .object_count
            .checked_mul(RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES as u64)
            .ok_or(RecoveryCheckpointFormatError::ArithmeticOverflow)?;
        let minimum = RECOVERY_CHECKPOINT_PAYLOAD_OFFSET
            .checked_add(table_bytes)
            .and_then(|length| length.checked_add(RECOVERY_CHECKPOINT_FOOTER_BYTES as u64))
            .ok_or(RecoveryCheckpointFormatError::ArithmeticOverflow)?;
        if self.generation == 0
            || self.body.file_length < minimum
            || self.body.body_hash == [0; 32]
            || self.body.table_hash == [0; 32]
            || self.body.chain_object_count < self.body.object_count
        {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        if self
            .previous_chain
            .is_some_and(|link| link.generation >= self.base_generation())
        {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        let valid = match self.chain {
            RecoveryCheckpointChain::Base => {
                self.body.object_count != 0
                    && self.body.chain_object_count == self.body.object_count
            }
            RecoveryCheckpointChain::Delta {
                parent,
                chain_index,
                base_generation,
            } => {
                (1..RECOVERY_CHECKPOINT_MAX_CHAIN_FILES).contains(&chain_index)
                    && parent.generation < self.generation
                    && base_generation != 0
                    && base_generation <= parent.generation
            }
        };
        if valid {
            Ok(())
        } else {
            Err(RecoveryCheckpointFormatError::InvalidDescriptor)
        }
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn namespace_root(self) -> MetadataObjectId {
        self.namespace_root
    }

    #[must_use]
    pub const fn policy_set(self) -> PolicySetId {
        self.policy_set
    }

    #[must_use]
    pub const fn object_count(self) -> u64 {
        self.body.object_count
    }

    #[must_use]
    pub const fn file_length(self) -> u64 {
        self.body.file_length
    }

    #[must_use]
    pub const fn body_hash(self) -> [u8; 32] {
        self.body.body_hash
    }

    #[must_use]
    pub const fn table_hash(self) -> [u8; 32] {
        self.body.table_hash
    }

    #[must_use]
    pub const fn chain_object_count(self) -> u64 {
        self.body.chain_object_count
    }

    #[must_use]
    pub const fn chain_payload_bytes(self) -> u64 {
        self.body.chain_payload_bytes
    }

    #[must_use]
    pub const fn chain(self) -> RecoveryCheckpointChain {
        self.chain
    }

    #[must_use]
    pub const fn chain_index(self) -> u64 {
        match self.chain {
            RecoveryCheckpointChain::Base => 0,
            RecoveryCheckpointChain::Delta { chain_index, .. } => chain_index,
        }
    }

    #[must_use]
    pub const fn base_generation(self) -> u64 {
        match self.chain {
            RecoveryCheckpointChain::Base => self.generation,
            RecoveryCheckpointChain::Delta {
                base_generation, ..
            } => base_generation,
        }
    }

    #[must_use]
    pub const fn parent(self) -> Option<RecoveryCheckpointLink> {
        match self.chain {
            RecoveryCheckpointChain::Base => None,
            RecoveryCheckpointChain::Delta { parent, .. } => Some(parent),
        }
    }

    #[must_use]
    pub const fn previous_chain(self) -> Option<RecoveryCheckpointLink> {
        self.previous_chain
    }

    #[must_use]
    pub const fn is_base(self) -> bool {
        matches!(self.chain, RecoveryCheckpointChain::Base)
    }

    /// The link a head, child Delta, or later Base uses to name this file.
    #[must_use]
    pub const fn link(self) -> RecoveryCheckpointLink {
        RecoveryCheckpointLink {
            generation: self.generation,
            file_length: self.body.file_length,
            body_hash: self.body.body_hash,
        }
    }

    /// Offset of the object table; the footer follows it directly.
    #[must_use]
    pub const fn table_offset(self) -> u64 {
        self.body.file_length
            - RECOVERY_CHECKPOINT_FOOTER_BYTES as u64
            - self.body.object_count * RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES as u64
    }

    #[must_use]
    pub fn encode_header(self) -> [u8; RECOVERY_CHECKPOINT_HEADER_BYTES] {
        encode_descriptor(HEADER_MAGIC, &self)
    }

    #[must_use]
    pub fn encode_footer(self) -> [u8; RECOVERY_CHECKPOINT_FOOTER_BYTES] {
        encode_descriptor(FOOTER_MAGIC, &self)
    }

    /// Decodes and authenticates a descriptor header.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid field, checksum, bound, or reserved byte.
    pub fn decode_header(bytes: &[u8]) -> Result<Self, RecoveryCheckpointFormatError> {
        decode_descriptor(bytes, HEADER_MAGIC)
    }

    /// Decodes and authenticates a descriptor footer.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid field, checksum, bound, or reserved byte.
    pub fn decode_footer(bytes: &[u8]) -> Result<Self, RecoveryCheckpointFormatError> {
        decode_descriptor(bytes, FOOTER_MAGIC)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RecoveryCheckpointObjectKind {
    NamespaceGraphRoot,
    NamespaceInodeShard,
    NamespaceEntryShard,
    ManifestLeaf,
    ManifestInnerNode,
}

impl RecoveryCheckpointObjectKind {
    const fn code(self) -> u8 {
        match self {
            Self::NamespaceGraphRoot => 1,
            Self::NamespaceInodeShard => 2,
            Self::NamespaceEntryShard => 3,
            Self::ManifestLeaf => 4,
            Self::ManifestInnerNode => 5,
        }
    }

    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::NamespaceGraphRoot),
            2 => Some(Self::NamespaceInodeShard),
            3 => Some(Self::NamespaceEntryShard),
            4 => Some(Self::ManifestLeaf),
            5 => Some(Self::ManifestInnerNode),
            _ => None,
        }
    }
}

/// Structural facts that let a parent reference be validated without the child.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RecoveryCheckpointObjectFacts {
    kind: RecoveryCheckpointObjectKind,
    level: u16,
    logical_length: u64,
    allocated_bytes: Option<u64>,
}

impl RecoveryCheckpointObjectFacts {
    /// # Errors
    ///
    /// Rejects a Manifest kind.
    pub const fn namespace(
        kind: RecoveryCheckpointObjectKind,
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        match kind {
            RecoveryCheckpointObjectKind::NamespaceGraphRoot
            | RecoveryCheckpointObjectKind::NamespaceInodeShard
            | RecoveryCheckpointObjectKind::NamespaceEntryShard => Ok(Self {
                kind,
                level: 0,
                logical_length: 0,
                allocated_bytes: None,
            }),
            RecoveryCheckpointObjectKind::ManifestLeaf
            | RecoveryCheckpointObjectKind::ManifestInnerNode => {
                Err(RecoveryCheckpointFormatError::InvalidEntry)
            }
        }
    }

    /// # Errors
    ///
    /// Rejects allocation above the logical length.
    pub const fn manifest_leaf(
        logical_length: u64,
        allocated_bytes: u64,
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        if allocated_bytes > logical_length {
            return Err(RecoveryCheckpointFormatError::InvalidEntry);
        }
        Ok(Self {
            kind: RecoveryCheckpointObjectKind::ManifestLeaf,
            level: 0,
            logical_length,
            allocated_bytes: Some(allocated_bytes),
        })
    }

    /// # Errors
    ///
    /// Rejects level zero or allocation above the logical length.
    pub const fn manifest_inner(
        level: u16,
        logical_length: u64,
        allocated_bytes: Option<u64>,
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        if level == 0 {
            return Err(RecoveryCheckpointFormatError::InvalidEntry);
        }
        if let Some(allocated) = allocated_bytes
            && allocated > logical_length
        {
            return Err(RecoveryCheckpointFormatError::InvalidEntry);
        }
        Ok(Self {
            kind: RecoveryCheckpointObjectKind::ManifestInnerNode,
            level,
            logical_length,
            allocated_bytes,
        })
    }

    #[must_use]
    pub const fn kind(self) -> RecoveryCheckpointObjectKind {
        self.kind
    }

    #[must_use]
    pub const fn level(self) -> u16 {
        self.level
    }

    #[must_use]
    pub const fn logical_length(self) -> u64 {
        self.logical_length
    }

    #[must_use]
    pub const fn allocated_bytes(self) -> Option<u64> {
        self.allocated_bytes
    }

    #[must_use]
    pub const fn is_manifest(self) -> bool {
        matches!(
            self.kind,
            RecoveryCheckpointObjectKind::ManifestLeaf
                | RecoveryCheckpointObjectKind::ManifestInnerNode
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryCheckpointTableRecord {
    object_id: MetadataObjectId,
    payload_offset: u64,
    payload_length: u32,
    payload_crc32c: u32,
    facts: RecoveryCheckpointObjectFacts,
}

impl RecoveryCheckpointTableRecord {
    /// # Errors
    ///
    /// Rejects an empty or oversized payload, or a misaligned offset inside
    /// the fixed descriptor and Commit prefix.
    pub fn new(
        object_id: MetadataObjectId,
        payload_offset: u64,
        payload_length: usize,
        payload_crc32c: u32,
        facts: RecoveryCheckpointObjectFacts,
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        if payload_length == 0
            || payload_length > MAX_METADATA_OBJECT_BYTES
            || payload_offset < RECOVERY_CHECKPOINT_PAYLOAD_OFFSET
            || !payload_offset.is_multiple_of(RECOVERY_CHECKPOINT_PAYLOAD_ALIGNMENT as u64)
        {
            return Err(RecoveryCheckpointFormatError::InvalidEntry);
        }
        Ok(Self {
            object_id,
            payload_offset,
            payload_length: u32::try_from(payload_length)
                .map_err(|_| RecoveryCheckpointFormatError::InvalidEntry)?,
            payload_crc32c,
            facts,
        })
    }

    #[must_use]
    pub const fn object_id(self) -> MetadataObjectId {
        self.object_id
    }

    #[must_use]
    pub const fn payload_offset(self) -> u64 {
        self.payload_offset
    }

    #[must_use]
    pub const fn payload_length(self) -> u32 {
        self.payload_length
    }

    #[must_use]
    pub const fn payload_crc32c(self) -> u32 {
        self.payload_crc32c
    }

    #[must_use]
    pub const fn facts(self) -> RecoveryCheckpointObjectFacts {
        self.facts
    }

    /// End of this payload after zero padding to the next 64-byte boundary.
    #[must_use]
    pub const fn padded_end(self) -> u64 {
        self.payload_offset + recovery_checkpoint_padded_length(self.payload_length)
    }

    #[must_use]
    pub fn encode(self) -> [u8; RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES] {
        let mut bytes = [0_u8; RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES];
        bytes[..32].copy_from_slice(&self.object_id.bytes());
        put_u64(&mut bytes, 32, self.payload_offset);
        put_u32(&mut bytes, 40, self.payload_length);
        put_u32(&mut bytes, 44, self.payload_crc32c);
        bytes[48] = self.facts.kind.code();
        bytes[49] = if self.facts.allocated_bytes.is_some() {
            FLAG_ALLOCATED_BYTES
        } else {
            0
        };
        put_u16(&mut bytes, 50, self.facts.level);
        put_u64(&mut bytes, 56, self.facts.logical_length);
        put_u64(&mut bytes, 64, self.facts.allocated_bytes.unwrap_or(0));
        bytes
    }

    /// Decodes one table record and validates its self-contained fields.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid kind, flag, fact, span, or reserved byte.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryCheckpointFormatError> {
        if bytes.len() != RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES
            || bytes[52..56].iter().any(|byte| *byte != 0)
            || bytes[72..].iter().any(|byte| *byte != 0)
            || bytes[49] & !FLAG_ALLOCATED_BYTES != 0
        {
            return Err(RecoveryCheckpointFormatError::InvalidEntry);
        }
        let mut object_id = [0_u8; 32];
        object_id.copy_from_slice(&bytes[..32]);
        let kind = RecoveryCheckpointObjectKind::from_code(bytes[48])
            .ok_or(RecoveryCheckpointFormatError::InvalidEntry)?;
        let level = get_u16(bytes, 50);
        let logical_length = get_u64(bytes, 56);
        let allocated = get_u64(bytes, 64);
        let has_allocated = bytes[49] & FLAG_ALLOCATED_BYTES != 0;
        let facts = match kind {
            RecoveryCheckpointObjectKind::ManifestLeaf if has_allocated && level == 0 => {
                RecoveryCheckpointObjectFacts::manifest_leaf(logical_length, allocated)?
            }
            RecoveryCheckpointObjectKind::ManifestInnerNode if has_allocated || allocated == 0 => {
                RecoveryCheckpointObjectFacts::manifest_inner(
                    level,
                    logical_length,
                    has_allocated.then_some(allocated),
                )?
            }
            RecoveryCheckpointObjectKind::NamespaceGraphRoot
            | RecoveryCheckpointObjectKind::NamespaceInodeShard
            | RecoveryCheckpointObjectKind::NamespaceEntryShard
                if !has_allocated && level == 0 && logical_length == 0 && allocated == 0 =>
            {
                RecoveryCheckpointObjectFacts::namespace(kind)?
            }
            _ => return Err(RecoveryCheckpointFormatError::InvalidEntry),
        };
        Self::new(
            MetadataObjectId::new(object_id).ok_or(RecoveryCheckpointFormatError::InvalidEntry)?,
            get_u64(bytes, 32),
            usize::try_from(get_u32(bytes, 40))
                .map_err(|_| RecoveryCheckpointFormatError::InvalidEntry)?,
            get_u32(bytes, 44),
            facts,
        )
    }
}

/// Payload length rounded up to the 64-byte payload alignment.
#[must_use]
pub const fn recovery_checkpoint_padded_length(payload_length: u32) -> u64 {
    let alignment = RECOVERY_CHECKPOINT_PAYLOAD_ALIGNMENT as u64;
    (payload_length as u64).div_ceil(alignment) * alignment
}

/// Decodes a complete object table and proves the exact packed layout: IDs
/// strictly ascend, the first payload starts after the Commit, each payload
/// starts at its predecessor's padded end, and the last one ends at the table.
///
/// # Errors
///
/// Returns an error for a length, record, order, or span violation.
pub fn decode_recovery_checkpoint_table(
    bytes: &[u8],
    table_offset: u64,
) -> Result<Vec<RecoveryCheckpointTableRecord>, RecoveryCheckpointFormatError> {
    if !bytes
        .len()
        .is_multiple_of(RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES)
    {
        return Err(RecoveryCheckpointFormatError::InvalidEntry);
    }
    let mut records = Vec::new();
    records
        .try_reserve_exact(bytes.len() / RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES)
        .map_err(|_| RecoveryCheckpointFormatError::ArithmeticOverflow)?;
    let mut cursor = RECOVERY_CHECKPOINT_PAYLOAD_OFFSET;
    let mut previous: Option<MetadataObjectId> = None;
    for encoded in bytes.chunks_exact(RECOVERY_CHECKPOINT_TABLE_RECORD_BYTES) {
        let record = RecoveryCheckpointTableRecord::decode(encoded)?;
        if previous.is_some_and(|previous| previous >= record.object_id)
            || record.payload_offset != cursor
        {
            return Err(RecoveryCheckpointFormatError::InvalidEntry);
        }
        cursor = record
            .payload_offset
            .checked_add(recovery_checkpoint_padded_length(record.payload_length))
            .ok_or(RecoveryCheckpointFormatError::ArithmeticOverflow)?;
        previous = Some(record.object_id);
        records.push(record);
    }
    if cursor != table_offset {
        return Err(RecoveryCheckpointFormatError::InvalidEntry);
    }
    Ok(records)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryCheckpointHeadRecord {
    tip: RecoveryCheckpointLink,
    previous_generation: u64,
    previous_record_hash: [u8; 32],
}

impl RecoveryCheckpointHeadRecord {
    /// Constructs the next selector record and links it to the preceding head.
    ///
    /// # Errors
    ///
    /// Returns an error if the predecessor generation is not strictly older.
    pub fn new(
        tip: RecoveryCheckpointLink,
        previous: Option<Self>,
    ) -> Result<Self, RecoveryCheckpointFormatError> {
        let (previous_generation, previous_record_hash) = previous.map_or((0, [0; 32]), |record| {
            (record.generation(), record.record_hash())
        });
        if previous_generation >= tip.generation {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        Ok(Self {
            tip,
            previous_generation,
            previous_record_hash,
        })
    }

    #[must_use]
    pub const fn tip(self) -> RecoveryCheckpointLink {
        self.tip
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.tip.generation
    }

    #[must_use]
    pub const fn previous_generation(self) -> u64 {
        self.previous_generation
    }

    #[must_use]
    pub const fn previous_record_hash(self) -> [u8; 32] {
        self.previous_record_hash
    }

    #[must_use]
    pub fn record_hash(self) -> [u8; 32] {
        *blake3::hash(&self.encode()).as_bytes()
    }

    #[must_use]
    pub fn encode(self) -> [u8; RECOVERY_CHECKPOINT_HEAD_BYTES] {
        let mut bytes = [0_u8; RECOVERY_CHECKPOINT_HEAD_BYTES];
        bytes[..8].copy_from_slice(&HEAD_MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, 4_096);
        put_u16(&mut bytes, 12, BLAKE3_256_ALGORITHM);
        put_u64(&mut bytes, 16, self.tip.generation);
        put_u64(&mut bytes, 24, self.tip.file_length);
        bytes[32..64].copy_from_slice(&self.tip.body_hash);
        put_u64(&mut bytes, 64, self.previous_generation);
        bytes[72..104].copy_from_slice(&self.previous_record_hash);
        let crc = crc32c::crc32c(&bytes);
        put_u32(&mut bytes, HEAD_CRC_OFFSET, crc);
        bytes
    }

    /// Decodes and authenticates one selector record.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid field, checksum, link, or reserved byte.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryCheckpointFormatError> {
        if bytes.len() != RECOVERY_CHECKPOINT_HEAD_BYTES
            || bytes[..8] != HEAD_MAGIC
            || get_u16(bytes, 8) != FORMAT_VERSION
            || usize::from(get_u16(bytes, 10)) != RECOVERY_CHECKPOINT_HEAD_BYTES
            || get_u16(bytes, 12) != BLAKE3_256_ALGORITHM
            || get_u16(bytes, 14) != 0
            || bytes[108..].iter().any(|byte| *byte != 0)
        {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        verify_crc(bytes, HEAD_CRC_OFFSET)?;
        let tip =
            decode_link(bytes, 16)?.ok_or(RecoveryCheckpointFormatError::InvalidDescriptor)?;
        let previous_generation = get_u64(bytes, 64);
        let mut previous_record_hash = [0_u8; 32];
        previous_record_hash.copy_from_slice(&bytes[72..104]);
        if previous_generation >= tip.generation
            || (previous_generation == 0) != (previous_record_hash == [0; 32])
        {
            return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
        }
        Ok(Self {
            tip,
            previous_generation,
            previous_record_hash,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryCheckpointFormatError {
    InvalidDescriptor,
    InvalidEntry,
    InvalidChecksum,
    ArithmeticOverflow,
}

impl fmt::Display for RecoveryCheckpointFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for RecoveryCheckpointFormatError {}

fn encode_descriptor(
    magic: [u8; 8],
    descriptor: &RecoveryCheckpointDescriptor,
) -> [u8; RECOVERY_CHECKPOINT_HEADER_BYTES] {
    let mut bytes = [0_u8; RECOVERY_CHECKPOINT_HEADER_BYTES];
    bytes[..8].copy_from_slice(&magic);
    put_u16(&mut bytes, 8, FORMAT_VERSION);
    put_u16(&mut bytes, 10, 4_096);
    put_u16(&mut bytes, 12, BLAKE3_256_ALGORITHM);
    put_u64(&mut bytes, 16, descriptor.body.file_length);
    put_u64(&mut bytes, 24, descriptor.generation);
    put_u64(&mut bytes, 32, descriptor.body.object_count);
    bytes[40..72].copy_from_slice(&descriptor.namespace_root.bytes());
    bytes[72..104].copy_from_slice(&descriptor.policy_set.bytes());
    bytes[104..136].copy_from_slice(&descriptor.body.body_hash);
    bytes[136..168].copy_from_slice(&descriptor.body.table_hash);
    put_u64(&mut bytes, 168, descriptor.chain_index());
    put_u64(&mut bytes, 176, descriptor.base_generation());
    put_u64(&mut bytes, 184, descriptor.body.chain_object_count);
    put_u64(&mut bytes, 192, descriptor.body.chain_payload_bytes);
    encode_link(&mut bytes, 200, descriptor.parent());
    encode_link(&mut bytes, 248, descriptor.previous_chain());
    let crc = crc32c::crc32c(&bytes);
    put_u32(&mut bytes, DESCRIPTOR_CRC_OFFSET, crc);
    bytes
}

fn decode_descriptor(
    bytes: &[u8],
    magic: [u8; 8],
) -> Result<RecoveryCheckpointDescriptor, RecoveryCheckpointFormatError> {
    if bytes.len() != RECOVERY_CHECKPOINT_HEADER_BYTES
        || bytes[..8] != magic
        || get_u16(bytes, 8) != FORMAT_VERSION
        || usize::from(get_u16(bytes, 10)) != RECOVERY_CHECKPOINT_HEADER_BYTES
        || get_u16(bytes, 12) != BLAKE3_256_ALGORITHM
        || get_u16(bytes, 14) != 0
        || bytes[DESCRIPTOR_USED_BYTES..].iter().any(|byte| *byte != 0)
    {
        return Err(RecoveryCheckpointFormatError::InvalidDescriptor);
    }
    verify_crc(bytes, DESCRIPTOR_CRC_OFFSET)?;
    let mut namespace_root = [0_u8; 32];
    namespace_root.copy_from_slice(&bytes[40..72]);
    let mut policy_set = [0_u8; 32];
    policy_set.copy_from_slice(&bytes[72..104]);
    let mut body_hash = [0_u8; 32];
    body_hash.copy_from_slice(&bytes[104..136]);
    let mut table_hash = [0_u8; 32];
    table_hash.copy_from_slice(&bytes[136..168]);
    let generation = get_u64(bytes, 24);
    let chain_index = get_u64(bytes, 168);
    let base_generation = get_u64(bytes, 176);
    let parent = decode_link(bytes, 200)?;
    let previous_chain = decode_link(bytes, 248)?;
    let chain = match (chain_index, parent) {
        (0, None) if base_generation == generation => RecoveryCheckpointChain::Base,
        (1.., Some(parent)) => RecoveryCheckpointChain::Delta {
            parent,
            chain_index,
            base_generation,
        },
        _ => return Err(RecoveryCheckpointFormatError::InvalidDescriptor),
    };
    let descriptor = RecoveryCheckpointDescriptor {
        generation,
        namespace_root: MetadataObjectId::new(namespace_root)
            .ok_or(RecoveryCheckpointFormatError::InvalidDescriptor)?,
        policy_set: PolicySetId::new(policy_set)
            .ok_or(RecoveryCheckpointFormatError::InvalidDescriptor)?,
        body: RecoveryCheckpointBody {
            object_count: get_u64(bytes, 32),
            file_length: get_u64(bytes, 16),
            body_hash,
            table_hash,
            chain_object_count: get_u64(bytes, 184),
            chain_payload_bytes: get_u64(bytes, 192),
        },
        chain,
        previous_chain,
    };
    descriptor.validate()?;
    Ok(descriptor)
}

fn encode_link(bytes: &mut [u8], offset: usize, link: Option<RecoveryCheckpointLink>) {
    if let Some(link) = link {
        put_u64(bytes, offset, link.generation);
        put_u64(bytes, offset + 8, link.file_length);
        bytes[offset + 16..offset + 48].copy_from_slice(&link.body_hash);
    }
}

/// An all-zero link is absent; a partially zero link is invalid.
fn decode_link(
    bytes: &[u8],
    offset: usize,
) -> Result<Option<RecoveryCheckpointLink>, RecoveryCheckpointFormatError> {
    let generation = get_u64(bytes, offset);
    let file_length = get_u64(bytes, offset + 8);
    let mut body_hash = [0_u8; 32];
    body_hash.copy_from_slice(&bytes[offset + 16..offset + 48]);
    if generation == 0 && file_length == 0 && body_hash == [0; 32] {
        return Ok(None);
    }
    RecoveryCheckpointLink::new(generation, file_length, body_hash).map(Some)
}

fn verify_crc(bytes: &[u8], offset: usize) -> Result<(), RecoveryCheckpointFormatError> {
    let computed = crc32c::crc32c_append(
        crc32c::crc32c_append(crc32c::crc32c(&bytes[..offset]), &[0; 4]),
        &bytes[offset + 4..],
    );
    if computed == get_u32(bytes, offset) {
        Ok(())
    } else {
        Err(RecoveryCheckpointFormatError::InvalidChecksum)
    }
}

fn get_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("fixed field"))
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("fixed field"))
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed field"))
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
