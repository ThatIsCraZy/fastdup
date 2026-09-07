//! Resumable scrub work, never a current payload proof or GC deletion authority.
use crate::{ContainerRepository, PendingDataVerification, StorageIo, StoreError};
use fastdup_format::{
    ChunkId, ContainerId, ContainerStructure, FOOTER_BYTES, HEADER_BYTES, MAX_CONTAINER_BYTES,
    SealedContainerDescriptor, StructuralChunk,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io;

const NAME: &str = ".fastdup-scrub-progress-v1";
const MAGIC: &[u8; 8] = b"FDSCRB01";
const HEADER_LEN: usize = 80;
// A stopped pass cannot postpone latent-corruption checks indefinitely.
const MAX_ROUND_AGE: u64 = 7 * 24 * 60 * 60;

/// A past complete verification within one scrub round. Opaque and deliberately
/// incompatible with payload/publication proofs. Entries also retain the Chunk
/// map so a new committed graph can be checked without trusting index hints.
#[derive(Debug)]
pub struct ScrubCertificate {
    id: ContainerId,
    generation: u64,
    length: u64,
    fingerprint: [u8; 32],
    checked_at: u64,
    chunks: Vec<StructuralChunk>,
}

impl ScrubCertificate {
    fn new(structure: &ContainerStructure, checked_at: u64) -> Self {
        let descriptor = structure.descriptor();
        Self {
            id: descriptor.container_id(),
            generation: descriptor.container_generation(),
            length: descriptor.layout().file_length,
            fingerprint: descriptor.container_hash(),
            checked_at,
            chunks: structure.chunks().to_vec(),
        }
    }

    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.length
    }
}

/// Coverage of today's startup graph and all resumed dependent Bases. Independent
/// locations are retained only for this pass; RETIRING locations never count.
#[must_use = "finish must establish current graph and Base coverage before opening the GC gate"]
pub struct ScrubCoverage {
    required: PendingDataVerification,
    independent: BTreeSet<(ChunkId, u32)>,
    bases: BTreeSet<(ChunkId, u32)>,
}

impl ScrubCoverage {
    pub fn new(required: PendingDataVerification) -> Self {
        Self {
            required,
            independent: BTreeSet::new(),
            bases: BTreeSet::new(),
        }
    }

    fn observe(&mut self, certificate: &ScrubCertificate) {
        self.required.observe_chunks(&certificate.chunks);
        for chunk in &certificate.chunks {
            if let Some(base) = chunk.dependency {
                if !self.independent.contains(&base) {
                    self.bases.insert(base);
                }
            } else {
                let key = (chunk.chunk_id, chunk.logical_length);
                self.independent.insert(key);
                self.bases.remove(&key);
            }
        }
    }

    /// Finishes only when current Chunk requirements and independent Bases exist.
    /// # Errors
    /// Returns missing DATA even when every existing directory entry passed.
    pub fn finish(self) -> Result<(), StoreError> {
        self.required.finish()?;
        if let Some((chunk_id, logical_length)) = self.bases.first().copied() {
            return Err(StoreError::MissingVerifiedChunk {
                chunk_id,
                logical_length: u64::from(logical_length),
            });
        }
        Ok(())
    }
}

impl<I: StorageIo> ContainerRepository<I> {
    /// Fully checks bytes and dependencies before minting a progress entry.
    /// # Errors
    /// Returns full scrub integrity, dependency, or I/O failures.
    pub fn scrub_for_progress<X: StorageIo>(
        &self,
        id: ContainerId,
        index: Option<&crate::ActivatedExactIndex<X>>,
        coverage: &mut ScrubCoverage,
        checked_at: u64,
    ) -> Result<ScrubCertificate, StoreError> {
        let entry = ScrubCertificate::new(&self.scrub_structure(id, index)?, checked_at);
        if self.selectable_container(id) {
            coverage.observe(&entry);
        }
        Ok(entry)
    }

    /// Reconciles one past check with today's actual immutable envelope. This
    /// skips only scrub payload work; demand reads and offline scrub still verify.
    /// # Errors
    /// Returns missing, damaged, or unreadable current Container envelopes.
    pub fn resume_scrub(
        &self,
        entry: &ScrubCertificate,
        coverage: &mut ScrubCoverage,
    ) -> Result<bool, StoreError> {
        let name = crate::published_name(entry.id);
        let length = self.storage.object_len(&name)?;
        if length != entry.length || !(8192..=MAX_CONTAINER_BYTES).contains(&length) {
            return Ok(false);
        }
        let header = self.storage.read_structure_at(&name, 0, HEADER_BYTES)?;
        let footer = self
            .storage
            .read_structure_at(&name, length - FOOTER_BYTES, HEADER_BYTES)?;
        let descriptor = SealedContainerDescriptor::decode(&header, &footer, length)?;
        if descriptor.container_id() != entry.id
            || descriptor.container_generation() != entry.generation
            || descriptor.container_hash() != entry.fingerprint
            || descriptor.layout().chunk_entry_count as usize != entry.chunks.len()
        {
            return Ok(false);
        }
        if self.selectable_container(entry.id) {
            coverage.observe(entry);
        }
        Ok(true)
    }
}

/// Single-writer, checksummed append journal in the Metadata pool. The caller
/// holds the appliance lease. Only an incomplete, recent round with the exact
/// pool binding is resumable. The in-memory index stores offsets, not Chunk maps.
pub struct ScrubProgress<I> {
    storage: I,
    header_hash: [u8; 32],
    started: u64,
    end: u64,
    entries: BTreeMap<[u8; 16], (u64, usize)>,
}

impl<I: StorageIo> ScrubProgress<I> {
    /// Opens the accepted prefix; a torn suffix is discarded before appending.
    /// Invalid/foreign/expired headers and completed rounds start a new pass.
    /// # Errors
    /// Returns storage failures. Callers may fall back to a full scrub.
    pub fn open(storage: I, binding: [u8; 32], now: u64) -> io::Result<Self> {
        let mut this = Self {
            storage,
            header_hash: [0; 32],
            started: now,
            end: 0,
            entries: BTreeMap::new(),
        };
        if this.storage.exists(NAME)? && this.load(binding, now)? {
            return Ok(this);
        }
        this.reset(binding, now)?;
        Ok(this)
    }

    fn load(&mut self, binding: [u8; 32], now: u64) -> io::Result<bool> {
        let length = self.storage.object_len(NAME)?;
        if length < HEADER_LEN as u64 {
            return Ok(false);
        }
        let header = self.storage.read_exact_at(NAME, 0, HEADER_LEN)?;
        if &header[..8] != MAGIC
            || header[8..40] != binding
            || header[48..] != *blake3::hash(&header[..48]).as_bytes()
        {
            return Ok(false);
        }
        self.started = u64::from_le_bytes(header[40..48].try_into().expect("fixed header"));
        if now < self.started || now - self.started >= MAX_ROUND_AGE {
            return Ok(false);
        }
        self.header_hash.copy_from_slice(&header[48..]);
        self.end = HEADER_LEN as u64;
        while self.end < length {
            if length - self.end < 4 {
                break;
            }
            let prefix = self.storage.read_exact_at(NAME, self.end, 4)?;
            let size = u32::from_le_bytes(prefix.try_into().expect("frame length")) as usize;
            if !(37..=MAX_CONTAINER_BYTES).contains(&(size as u64))
                || size as u64 > length - self.end
            {
                break;
            }
            let bytes = self.read_frame(self.end, size)?;
            if !valid_frame(&bytes, &self.header_hash) {
                break;
            }
            if bytes[4] == 2 && bytes.len() == 37 {
                return Ok(false);
            }
            let Some(entry) = decode_entry(&bytes, self.started, now) else {
                break;
            };
            self.entries.insert(entry.id.bytes(), (self.end, size));
            self.end += size as u64;
        }
        // Only auxiliary work is lost on a crash here; prior accepted entries
        // are immutable and checksum-bound to this round's header.
        if self.end != length {
            self.storage.set_len(NAME, self.end)?;
            self.storage.sync_file(NAME)?;
        }
        Ok(true)
    }

    fn reset(&mut self, binding: [u8; 32], now: u64) -> io::Result<()> {
        if self.storage.exists(NAME)? {
            self.storage.set_len(NAME, 0)?;
            self.storage.sync_file(NAME)?;
        } else {
            self.storage.create_new(NAME)?;
        }
        let mut header = Vec::from(MAGIC.as_slice());
        header.extend_from_slice(&binding);
        header.extend_from_slice(&now.to_le_bytes());
        self.header_hash = *blake3::hash(&header).as_bytes();
        header.extend_from_slice(&self.header_hash);
        self.storage.write_at(NAME, 0, &header)?;
        if self.storage.read_exact_at(NAME, 0, HEADER_LEN)? != header {
            return Err(invalid());
        }
        self.storage.sync_file(NAME)?;
        self.storage.sync_root()?;
        self.entries.clear();
        self.started = now;
        self.end = HEADER_LEN as u64;
        Ok(())
    }

    /// Loads only the requested entry's Chunk map from Metadata storage.
    /// # Errors
    /// Returns I/O or checksum failures; never returns an unchecked entry.
    pub fn lookup(&self, id: ContainerId, now: u64) -> io::Result<Option<ScrubCertificate>> {
        let Some(&(offset, size)) = self.entries.get(&id.bytes()) else {
            return Ok(None);
        };
        let bytes = self.read_frame(offset, size)?;
        if !valid_frame(&bytes, &self.header_hash) {
            return Err(invalid());
        }
        let entry = decode_entry(&bytes, self.started, now).ok_or_else(invalid)?;
        if entry.id != id {
            return Err(invalid());
        }
        Ok(Some(entry))
    }

    fn read_frame(&self, offset: u64, size: usize) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size)?;
        while bytes.len() < size {
            let count = (size - bytes.len()).min(crate::MAX_STORAGE_RANGE_BYTES);
            bytes.extend_from_slice(&self.storage.read_exact_at(
                NAME,
                offset + bytes.len() as u64,
                count,
            )?);
        }
        Ok(bytes)
    }

    /// Appends only an entry minted by full verification. Call `sync` in bounded
    /// batches and at clean cancellation; unflushed suffixes may be repeated.
    /// # Errors
    /// Returns storage/size errors. Stop using this writer after any failure.
    pub fn record(&mut self, entry: &ScrubCertificate) -> io::Result<()> {
        if entry.checked_at < self.started {
            return Err(invalid());
        }
        let mut payload = vec![1];
        payload.extend_from_slice(&entry.id.bytes());
        payload.extend_from_slice(&entry.generation.to_le_bytes());
        payload.extend_from_slice(&entry.length.to_le_bytes());
        payload.extend_from_slice(&entry.fingerprint);
        payload.extend_from_slice(&entry.checked_at.to_le_bytes());
        payload.extend_from_slice(
            &u32::try_from(entry.chunks.len())
                .map_err(|_| invalid())?
                .to_le_bytes(),
        );
        for chunk in &entry.chunks {
            payload.extend_from_slice(&chunk.chunk_id.bytes());
            payload.extend_from_slice(&chunk.logical_length.to_le_bytes());
            if let Some((base, length)) = chunk.dependency {
                payload.push(1);
                payload.extend_from_slice(&base.bytes());
                payload.extend_from_slice(&length.to_le_bytes());
            } else {
                payload.push(0);
            }
        }
        let offset = self.end;
        let size = self.append(&payload)?;
        self.entries.insert(entry.id.bytes(), (offset, size));
        Ok(())
    }

    fn append(&mut self, payload: &[u8]) -> io::Result<usize> {
        let size = payload
            .len()
            .checked_add(36)
            .filter(|n| *n as u64 <= MAX_CONTAINER_BYTES)
            .ok_or_else(invalid)?;
        let mut bytes = u32::try_from(size)
            .map_err(|_| invalid())?
            .to_le_bytes()
            .to_vec();
        bytes.extend_from_slice(payload);
        let hash = frame_hash(&bytes, &self.header_hash);
        bytes.extend_from_slice(&hash);
        self.storage.write_at(NAME, self.end, &bytes)?;
        if self.read_frame(self.end, size)? != bytes {
            return Err(invalid());
        }
        self.end += size as u64;
        Ok(size)
    }

    /// Persists the accepted work prefix, including after clean cancellation.
    /// # Errors
    /// Returns the Metadata storage sync error.
    pub fn sync(&self) -> io::Result<()> {
        self.storage.sync_file(NAME)
    }

    /// Closes the round durably. The next start performs a new full pass.
    /// # Errors
    /// Returns storage errors; a lost completion repeats only scheduling work.
    pub fn complete(&mut self) -> io::Result<()> {
        self.append(&[2])?;
        self.sync()
    }
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid scrub progress")
}
fn frame_hash(bytes: &[u8], header: &[u8; 32]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(header);
    hash.update(bytes);
    *hash.finalize().as_bytes()
}
fn valid_frame(bytes: &[u8], header: &[u8; 32]) -> bool {
    bytes.len() >= 37 && bytes[bytes.len() - 32..] == frame_hash(&bytes[..bytes.len() - 32], header)
}
fn decode_entry(bytes: &[u8], started: u64, now: u64) -> Option<ScrubCertificate> {
    if bytes.len() < 121 || bytes[4] != 1 {
        return None;
    }
    let mut reader = Reader(&bytes[5..bytes.len() - 32]);
    let id = ContainerId::new(reader.take()?).ok()?;
    let generation = u64::from_le_bytes(reader.take()?);
    let length = u64::from_le_bytes(reader.take()?);
    let fingerprint = reader.take()?;
    let checked_at = u64::from_le_bytes(reader.take()?);
    let count = u32::from_le_bytes(reader.take()?) as usize;
    if generation == 0
        || !(8192..=MAX_CONTAINER_BYTES).contains(&length)
        || checked_at < started
        || checked_at > now
        || count > reader.0.len() / 37
    {
        return None;
    }
    let mut chunks = Vec::new();
    chunks.try_reserve_exact(count).ok()?;
    for _ in 0..count {
        let chunk_id = ChunkId::from_bytes(reader.take()?);
        let logical_length = u32::from_le_bytes(reader.take()?);
        if logical_length == 0 {
            return None;
        }
        let dependency = match reader.take::<1>()?[0] {
            0 => None,
            1 => {
                let base = ChunkId::from_bytes(reader.take()?);
                let length = u32::from_le_bytes(reader.take()?);
                if length != logical_length {
                    return None;
                }
                Some((base, length))
            }
            _ => return None,
        };
        chunks.push(StructuralChunk {
            chunk_id,
            logical_length,
            dependency,
        });
    }
    if !reader.0.is_empty() {
        return None;
    }
    Some(ScrubCertificate {
        id,
        generation,
        length,
        fingerprint,
        checked_at,
        chunks,
    })
}
struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let value = self.0.get(..N)?.try_into().ok()?;
        self.0 = &self.0[N..];
        Some(value)
    }
}
