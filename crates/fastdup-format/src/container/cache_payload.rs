//! Process-local, independently decodable RAM representations. No disk format.
use super::super::MAX_LOGICAL_CHUNK_BYTES;
use super::{ChunkId, VerifiedChunkPayload, VerifiedIndependentRecord};
use super::{PayloadBacking, WeakBacking};
use crate::BufferPool;
use std::fmt;
use std::sync::{Arc, Mutex};

/// An independently compressed copy made exclusively from verified Chunk bytes.
/// It retains original provenance, never a pointer to a Base or backend reader.
#[derive(Clone)]
pub struct CompressedVerifiedChunkPayload(Arc<CompressedChunk>);

struct CompressedChunk {
    bytes: Box<[u8]>,
    chunk_id: ChunkId,
    length: usize,
    decoded_offset: usize,
    chunk_ordinal: u32,
    source: Option<VerifiedIndependentRecord>,
    // Readers can share a temporary decode; this never retains payload RAM.
    decoded: Mutex<(WeakBacking, usize)>,
}

impl fmt::Debug for CompressedVerifiedChunkPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompressedVerifiedChunkPayload")
            .field("chunk_id", &self.0.chunk_id)
            .field("length", &self.0.length)
            .field("compressed_bytes", &self.0.bytes.len())
            .finish_non_exhaustive()
    }
}

impl VerifiedChunkPayload {
    /// Creates a bounded, self-contained cache representation if it saves RAM
    /// after allocation/owner overhead. Failure leaves the original untouched.
    #[must_use]
    pub fn compress_for_cache(&self) -> Option<CompressedVerifiedChunkPayload> {
        if self.is_empty() || self.len() > MAX_LOGICAL_CHUNK_BYTES {
            return None;
        }
        let bytes = lz4::block::compress(self.as_slice(), None, false).ok()?;
        self.compressed_copy(bytes.into_boxed_slice())
    }

    /// Compresses into reusable initialized scratch; only the compact result is retained.
    #[must_use]
    pub fn compress_for_cache_with_pool(
        &self,
        pool: &BufferPool,
    ) -> Option<CompressedVerifiedChunkPayload> {
        if self.is_empty() || self.len() > MAX_LOGICAL_CHUNK_BYTES {
            return None;
        }
        let mut scratch = pool.take(lz4::block::compress_bound(self.len()).ok()?);
        let length =
            lz4::block::compress_to_buffer(self.as_slice(), None, false, &mut scratch).ok()?;
        if length.checked_add(CompressedVerifiedChunkPayload::overhead())? >= self.len() {
            return None;
        }
        self.compressed_copy(scratch[..length].into())
    }

    fn compressed_copy(&self, bytes: Box<[u8]>) -> Option<CompressedVerifiedChunkPayload> {
        if bytes
            .len()
            .checked_add(CompressedVerifiedChunkPayload::overhead())?
            >= self.len()
        {
            return None;
        }
        Some(CompressedVerifiedChunkPayload(Arc::new(CompressedChunk {
            bytes,
            chunk_id: self.chunk_id,
            length: self.length,
            decoded_offset: self.decoded_offset,
            chunk_ordinal: self.chunk_ordinal,
            source: self.source,
            decoded: Mutex::new((self.backing.downgrade(), self.offset)),
        })))
    }
}

impl CompressedVerifiedChunkPayload {
    const fn overhead() -> usize {
        // Owner, Arc counters and conservative allocator bookkeeping.
        std::mem::size_of::<CompressedChunk>() + 64
    }

    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        self.0.bytes.len() + Self::overhead()
    }

    #[must_use]
    pub fn chunk_id(&self) -> ChunkId {
        self.0.chunk_id
    }

    #[must_use]
    pub fn logical_length(&self) -> usize {
        self.0.length
    }

    /// Compares process-local ownership while both representations are alive.
    #[must_use]
    pub fn shares_backing_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Reuses an already live verified owner without allocating codec buffers.
    #[must_use]
    pub fn live_view(&self) -> Option<VerifiedChunkPayload> {
        let decoded = self.0.decoded.try_lock().ok()?;
        Some(self.view(decoded.0.upgrade()?, decoded.1))
    }

    fn view(&self, backing: PayloadBacking, offset: usize) -> VerifiedChunkPayload {
        VerifiedChunkPayload {
            chunk_id: self.0.chunk_id,
            backing,
            offset,
            length: self.0.length,
            decoded_offset: self.0.decoded_offset,
            chunk_ordinal: self.0.chunk_ordinal,
            source: self.0.source,
        }
    }

    /// Restores a verified view and reports whether a decode was required.
    ///
    /// Full logical identity is checked after decompression, before creating
    /// verification evidence. Concurrent readers share one decode while a
    /// returned payload is alive. An invalid cache copy is a miss, not DATA.
    #[must_use]
    pub fn decompress(&self) -> Option<(VerifiedChunkPayload, bool)> {
        self.decompress_inner(None)
    }

    /// Recycles decoded storage after the last immutable reader releases it.
    #[must_use]
    pub fn decompress_with_pool(&self, pool: &BufferPool) -> Option<(VerifiedChunkPayload, bool)> {
        self.decompress_inner(Some(pool))
    }

    fn decompress_inner(&self, pool: Option<&BufferPool>) -> Option<(VerifiedChunkPayload, bool)> {
        let mut decoded = self.0.decoded.lock().ok()?;
        let (backing, offset, did_decode) = if let Some(backing) = decoded.0.upgrade() {
            (backing, decoded.1, false)
        } else {
            let expected = i32::try_from(self.0.length).ok()?;
            let backing = if let Some(pool) = pool {
                let mut bytes = pool.take(self.0.length);
                let length =
                    lz4::block::decompress_to_buffer(&self.0.bytes, Some(expected), &mut bytes)
                        .ok()?;
                if length != self.0.length {
                    return None;
                }
                PayloadBacking::Pooled(Arc::new(bytes))
            } else {
                PayloadBacking::Owned(Arc::new(
                    lz4::block::decompress(&self.0.bytes, Some(expected)).ok()?,
                ))
            };
            if backing.len() != self.0.length || ChunkId::of(&backing) != self.0.chunk_id {
                return None;
            }
            *decoded = (backing.downgrade(), 0);
            (backing, 0, true)
        };
        Some((self.view(backing, offset), did_decode))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RawRecord;

    fn payload(bytes: &[u8]) -> VerifiedChunkPayload {
        RawRecord::decode(&RawRecord::encode(bytes).unwrap())
            .unwrap()
            .into_verified_payload()
    }

    #[test]
    fn compressed_cache_roundtrip_shares_only_live_decodes() {
        let original = payload(&vec![42; 65536]);
        let cache = original.compress_for_cache().unwrap();
        assert!(cache.resident_bytes() < original.len());
        let (source_view, decoded) = cache.decompress().unwrap();
        assert!(!decoded);
        assert!(source_view.shares_backing_with(&original));
        drop((source_view, original));
        let (first, decoded) = cache.decompress().unwrap();
        assert!(decoded);
        assert_eq!(first.as_slice(), &vec![42; 65536]);
        let (second, decoded) = cache.decompress().unwrap();
        assert!(!decoded);
        assert!(first.shares_backing_with(&second));
        drop((first, second));
        assert!(cache.decompress().unwrap().1);
    }

    #[test]
    fn damaged_cache_bytes_or_identity_never_make_a_verified_payload() {
        let mut cache = payload(&vec![42; 65536]).compress_for_cache().unwrap();
        Arc::get_mut(&mut cache.0).unwrap().chunk_id = ChunkId::of(b"different");
        assert!(cache.decompress().is_none());
        let mut cache = payload(&vec![42; 65536]).compress_for_cache().unwrap();
        Arc::get_mut(&mut cache.0).unwrap().bytes[0] ^= 0xff;
        assert!(cache.decompress().is_none());
        let mut cache = payload(&vec![42; 65536]).compress_for_cache().unwrap();
        Arc::get_mut(&mut cache.0).unwrap().length -= 1;
        assert!(cache.decompress().is_none());
    }

    #[test]
    fn pooled_decodes_recycle_only_after_the_last_response_and_never_reuse_evidence() {
        let pool = BufferPool::new();
        pool.set_limit(1 << 20);
        let compressed = payload(&vec![42; 65536])
            .compress_for_cache_with_pool(&pool)
            .unwrap();
        let (first, did_decode) = compressed.decompress_with_pool(&pool).unwrap();
        assert!(did_decode);
        let response = first.read_view(0..first.len()).unwrap();
        drop(first);
        let (second, did_decode) = compressed.decompress_with_pool(&pool).unwrap();
        assert!(!did_decode);
        pool.set_limit(0);
        assert_eq!(response.as_ref(), &vec![42; 65_536]);
        drop((response, second));
        assert_eq!(pool.status().active_bytes, 0);
        assert_eq!(pool.status().retained_bytes, 0);
        pool.set_limit(1 << 20);
        for _ in 0..20 {
            let (value, did_decode) = compressed.decompress_with_pool(&pool).unwrap();
            assert!(did_decode, "pool must not retain a live verification owner");
            assert_eq!(value.as_slice(), &vec![42; 65_536]);
        }
        assert!(pool.status().hits >= 19);
        let before = pool.status().misses;
        // Stale weak references cannot upgrade after the buffer was overwritten.
        let mut recycled = pool.take(65536);
        recycled.fill(99);
        drop(recycled);
        assert!(compressed.live_view().is_none());
        assert_eq!(
            compressed.decompress_with_pool(&pool).unwrap().0.as_slice(),
            &vec![42; 65_536]
        );
        assert_eq!(pool.status().misses, before);
    }

    #[test]
    fn pooled_corruption_returns_workspace_without_publishing_stale_bytes() {
        let pool = BufferPool::new();
        pool.set_limit(1 << 20);
        let mut value = payload(&vec![42; 65536])
            .compress_for_cache_with_pool(&pool)
            .unwrap();
        Arc::get_mut(&mut value.0).unwrap().chunk_id = ChunkId::of(b"wrong");
        for _ in 0..4 {
            assert!(value.decompress_with_pool(&pool).is_none());
        }
        assert_eq!(pool.status().active_bytes, 0);
        assert!(pool.status().hits >= 3);
    }

    #[test]
    fn pooled_owned_transfer_preserves_the_allocation_and_cleans_accounting() {
        let pool = BufferPool::new();
        pool.set_limit(1 << 20);
        let compressed = payload(&vec![42; 65536]).compress_for_cache().unwrap();
        let value = compressed.decompress_with_pool(&pool).unwrap().0;
        let address = value.as_slice().as_ptr();
        let bytes = value.into_payload();
        assert_eq!(bytes.as_ptr(), address);
        assert_eq!(bytes, vec![42; 65536]);
        assert_eq!(pool.status().active_bytes, 0);
        assert_eq!(pool.status().retained_bytes, 0);
    }
}

#[cfg(test)]
#[test]
#[ignore = "manual codec allocation-reuse A/B"]
fn buffer_pool_benchmark() {
    use std::hint::black_box;
    use std::time::Instant;
    let original = crate::RawRecord::decode(&crate::RawRecord::encode(&vec![42; 262_144]).unwrap())
        .unwrap()
        .into_verified_payload();
    let compressed = original.compress_for_cache().unwrap();
    drop(original);
    for round in 0..6 {
        for pooled in [round % 2 == 0, round % 2 != 0] {
            let pool = BufferPool::new();
            pool.set_limit(1 << 20);
            let start = Instant::now();
            for _ in 0..2000 {
                let (value, decoded) = if pooled {
                    compressed.decompress_with_pool(&pool)
                } else {
                    compressed.decompress()
                }
                .unwrap();
                assert!(decoded);
                black_box(value.as_slice());
            }
            println!(
                "buffer_pool round={round} pooled={pooled} elapsed_us={} stats={:?}",
                start.elapsed().as_micros(),
                pool.status()
            );
        }
    }
}

#[cfg(test)]
#[test]
#[ignore = "manual compression scratch reuse A/B"]
fn buffer_pool_compression_benchmark() {
    use std::hint::black_box;
    use std::time::Instant;
    let original = crate::RawRecord::decode(&crate::RawRecord::encode(&vec![42; 262_144]).unwrap())
        .unwrap()
        .into_verified_payload();
    for round in 0..6 {
        for pooled in [round % 2 == 0, round % 2 != 0] {
            let pool = BufferPool::new();
            pool.set_limit(1 << 20);
            let start = Instant::now();
            for _ in 0..2000 {
                black_box(
                    if pooled {
                        original.compress_for_cache_with_pool(&pool)
                    } else {
                        original.compress_for_cache()
                    }
                    .unwrap(),
                );
            }
            println!(
                "buffer_pool_compress round={round} pooled={pooled} elapsed_us={} stats={:?}",
                start.elapsed().as_micros(),
                pool.status()
            );
        }
    }
}

#[cfg(test)]
#[test]
#[ignore = "manual release-mode LZ4/Zstd cache codec comparison"]
fn cache_codec_benchmark() {
    use std::hint::black_box;
    use std::io::{Read, Seek, SeekFrom};
    use std::time::Instant;
    let pattern: Vec<_> = (0_u32..8192)
        .map(|n| n.wrapping_mul(7).to_le_bytes()[0])
        .collect();
    let mut noise = vec![0_u8; 65536];
    let mut random = 719_u64;
    for byte in &mut noise {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        *byte = random.to_le_bytes()[0];
    }
    let mut corpora = vec![("pattern", vec![pattern.repeat(8)]), ("noise", vec![noise])];
    if let Some(path) = std::env::var_os("FASTDUP_CACHE_BENCH_CORPUS") {
        let mut file = std::fs::File::open(path).unwrap();
        let length = file.metadata().unwrap().len();
        let mut samples = Vec::new();
        for n in 0..64_u64 {
            file.seek(SeekFrom::Start((length - 65536) * n / 64))
                .unwrap();
            let mut bytes = vec![0_u8; 65536];
            file.read_exact(&mut bytes).unwrap();
            samples.push(bytes);
        }
        corpora.push(("iso", samples));
    }
    for (label, corpus) in corpora {
        for round in 0..5 {
            for lz4 in [round % 2 == 0, round % 2 != 0] {
                let started = Instant::now();
                let encoded: Vec<_> = (0..32)
                    .flat_map(|_| corpus.iter())
                    .map(|bytes| {
                        if lz4 {
                            lz4::block::compress(bytes, None, false).unwrap()
                        } else {
                            zstd::bulk::compress(bytes, -1).unwrap()
                        }
                    })
                    .collect();
                let compress_ns = started.elapsed().as_nanos();
                let encoded_bytes: usize = encoded.iter().map(Vec::len).sum();
                let started = Instant::now();
                for (n, bytes) in encoded.iter().enumerate() {
                    let expected = &corpus[n % corpus.len()];
                    let decoded = if lz4 {
                        lz4::block::decompress(bytes, Some(65536)).unwrap()
                    } else {
                        zstd::bulk::decompress(bytes, 65536).unwrap()
                    };
                    black_box(ChunkId::of(&decoded));
                    assert_eq!(&decoded, expected);
                }
                println!(
                    "cache_codec corpus={label} round={round} lz4={lz4} raw_bytes={} encoded_bytes={encoded_bytes} compress_ns={compress_ns} decode_verify_ns={}",
                    encoded.len() * 65536,
                    started.elapsed().as_nanos()
                );
            }
        }
    }
}
