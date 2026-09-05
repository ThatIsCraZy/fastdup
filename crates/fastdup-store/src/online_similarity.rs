//! Incremental, immutable replacement-bucket Similarity generations.
//!
//! The two small heads select complete run sets. A batch is never a queryable
//! overlay. Reads retain an Arc generation and examine only the newest value
//! of each of four buckets. Compaction streams four chronological families.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use crate::StorageIo;
use crate::exact_index_repository::{ExactIndexGenerationPin, ExactIndexRunRepository};
use crate::reduction_similarity::SimilarityFingerprint;
use crate::similarity_index_repository::{
    RecoveredSimilarityIndex, SimilarityBaseCandidate, SimilarityIndexRepository,
    SimilarityIndexStoreError,
};
use fastdup_format::{ChunkId, SimilarityBucketKey, SimilarityIndexEntry};

const HEAD_BYTES: usize = 4096;
const MAX_FAMILIES: usize = 24;
/// Admission is bounded independently of DATA publication and pool size.
pub const ONLINE_SIMILARITY_BATCH_ENTRIES: usize = 4096;
const HEAD_NAMES: [&str; 2] = ["reduction-head.0.fds", "reduction-head.1.fds"];

type Error = SimilarityIndexStoreError;
type Bucket = (SimilarityBucketKey, Vec<SimilarityIndexEntry>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RunRef {
    generation: u64,
    first: u64,
    last: u64,
    level: u8,
    hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Head {
    sequence: u64,
    exact: [u8; 32],
    runs: Vec<RunRef>,
}

impl Head {
    fn encode(&self) -> Result<[u8; HEAD_BYTES], Error> {
        if self.sequence == 0 || self.exact == [0; 32] || self.runs.len() > MAX_FAMILIES {
            return Err(Error::IndexCorruption);
        }
        let mut bytes = [0; HEAD_BYTES];
        bytes[..8].copy_from_slice(b"FDRED001");
        bytes[8..16].copy_from_slice(&self.sequence.to_le_bytes());
        bytes[16..48].copy_from_slice(&self.exact);
        bytes[48..56].copy_from_slice(&(self.runs.len() as u64).to_le_bytes());
        let mut previous = None;
        for (i, run) in self.runs.iter().enumerate() {
            if run.generation == 0
                || run.hash == [0; 32]
                || self.runs[..i]
                    .iter()
                    .any(|prior| prior.generation == run.generation)
                || run.first > run.last
                || run.last > self.sequence
                || run.level > 15
                || previous.is_some_and(|last| last >= run.first)
            {
                return Err(Error::IndexCorruption);
            }
            previous = Some(run.last);
            let offset = 64 + i * 64;
            bytes[offset..offset + 8].copy_from_slice(&run.generation.to_le_bytes());
            bytes[offset + 8..offset + 16].copy_from_slice(&run.first.to_le_bytes());
            bytes[offset + 16..offset + 24].copy_from_slice(&run.last.to_le_bytes());
            bytes[offset + 24] = run.level;
            bytes[offset + 32..offset + 64].copy_from_slice(&run.hash);
        }
        let hash = blake3::hash(&bytes[..HEAD_BYTES - 32]);
        bytes[HEAD_BYTES - 32..].copy_from_slice(hash.as_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != HEAD_BYTES
            || &bytes[..8] != b"FDRED001"
            || blake3::hash(&bytes[..HEAD_BYTES - 32]).as_bytes() != &bytes[HEAD_BYTES - 32..]
        {
            return Err(Error::IndexCorruption);
        }
        let number =
            |offset| u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed field"));
        let count = usize::try_from(number(48)).map_err(|_| Error::IndexCorruption)?;
        if count > MAX_FAMILIES {
            return Err(Error::IndexCorruption);
        }
        let mut runs = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 64 + i * 64;
            runs.push(RunRef {
                generation: number(offset),
                first: number(offset + 8),
                last: number(offset + 16),
                level: bytes[offset + 24],
                hash: bytes[offset + 32..offset + 64]
                    .try_into()
                    .expect("fixed hash"),
            });
        }
        let head = Self {
            sequence: number(8),
            exact: bytes[16..48].try_into().expect("fixed identity"),
            runs,
        };
        if head.encode()?.as_slice() != bytes {
            return Err(Error::IndexCorruption);
        }
        Ok(head)
    }
}

/// Immutable candidate state; only a bounded query holds a current Exact pin.
pub(crate) struct OnlineReductionGeneration<I> {
    exact: ExactIndexRunRepository<I>,
    runs: Vec<Arc<RecoveredSimilarityIndex<I>>>,
}

impl<I: Clone + StorageIo> OnlineReductionGeneration<I> {
    pub(crate) fn pin_exact(&self) -> Option<ExactIndexGenerationPin<I>> {
        self.exact.pin_active_generation()
    }

    fn bucket(&self, key: SimilarityBucketKey) -> Result<Vec<SimilarityIndexEntry>, Error> {
        for run in self.runs.iter().rev() {
            let entries = run.bucket_entries(key)?;
            if !entries.is_empty() {
                return Ok(entries);
            }
        }
        Ok(Vec::new())
    }

    pub(crate) fn candidates(
        &self,
        target_id: ChunkId,
        fingerprint: &SimilarityFingerprint,
        length: u32,
    ) -> Result<Vec<SimilarityBaseCandidate>, Error> {
        let mut buckets = Vec::with_capacity(4);
        for (slot, value) in fingerprint.superfeatures().into_iter().enumerate() {
            buckets.push(self.bucket(SimilarityBucketKey::new(
                fingerprint.profile(),
                u8::try_from(slot).map_err(|_| Error::IndexCorruption)?,
                length,
                value,
            )?)?);
        }
        let mut positions = [0; 4];
        let mut candidates: Vec<SimilarityBaseCandidate> = Vec::with_capacity(16);
        loop {
            let next = (0..4)
                .filter_map(|i| buckets[i].get(positions[i]))
                .min_by_key(|e| e.chunk_id())
                .copied();
            let Some(entry) = next else {
                break;
            };
            for i in 0..4 {
                if let Some(other) = buckets[i].get(positions[i])
                    && other.chunk_id() == entry.chunk_id()
                {
                    if *other != entry {
                        return Err(Error::IndexCorruption);
                    }
                    positions[i] += 1;
                }
            }
            if entry.chunk_id() == target_id {
                continue;
            }
            let candidate = SimilarityBaseCandidate::from_entry(entry, fingerprint)?;
            let key = (candidate.sketch_distance(), candidate.chunk_id());
            let at = candidates.partition_point(|c| (c.sketch_distance(), c.chunk_id()) < key);
            if at < 16 {
                candidates.insert(at, candidate);
                candidates.truncate(16);
            }
        }
        Ok(candidates)
    }
}

struct Publisher<I> {
    head: Option<Head>,
    heads: [Option<Head>; 2],
    high_water: u64,
    retired: Vec<(u64, Weak<RecoveredSimilarityIndex<I>>)>,
}

/// One regular Similarity index with online immutable-run publication.
/// It owns no DATA and may safely miss an uncommitted publication tail.
pub struct OnlineSimilarityRepository<I> {
    repository: SimilarityIndexRepository<I>,
    exact_repository: ExactIndexRunRepository<I>,
    current: RwLock<Option<Arc<OnlineReductionGeneration<I>>>>,
    publisher: Mutex<Publisher<I>>,
    published_batches: AtomicU64,
    skipped_entries: AtomicU64,
    compactions: AtomicU64,
    errors: AtomicU64,
}

/// Process-lifetime publication counters and current bounded topology.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OnlineSimilarityStatus {
    pub published_batches: u64,
    pub skipped_entries: u64,
    pub compactions: u64,
    pub errors: u64,
    pub active_families: usize,
}

impl<I: Clone + StorageIo> OnlineSimilarityRepository<I> {
    /// Opens and audits the selected run set and records current Exact provenance.
    /// An offline family is an optional initial level, never required.
    ///
    /// # Errors
    /// Returns corrupt heads, missing/corrupt selected runs, or publication I/O.
    ///
    /// # Panics
    /// Panics if an internal generation or publication lock is poisoned.
    #[allow(
        clippy::too_many_lines,
        reason = "one audited recovery and activation transaction"
    )]
    pub fn open(
        repository: SimilarityIndexRepository<I>,
        exact_repository: &ExactIndexRunRepository<I>,
    ) -> Result<Self, Error> {
        let exact_pin = exact_repository.pin_active_generation();
        let exact = exact_pin.as_ref();
        let mut heads: [Option<Head>; 2] = [None, None];
        let mut existing = false;
        for (i, name) in HEAD_NAMES.iter().enumerate() {
            if repository.storage().exists(name)? {
                existing = true;
                if repository.storage().object_len(name)? == HEAD_BYTES as u64 {
                    heads[i] =
                        Head::decode(&repository.storage().read_exact_at(name, 0, HEAD_BYTES)?)
                            .ok();
                }
            }
        }
        let mut head = heads.iter().flatten().max_by_key(|h| h.sequence).cloned();
        if existing && head.is_none() {
            return Err(Error::IndexCorruption);
        }
        if let (Some(a), Some(b)) = (&heads[0], &heads[1])
            && a.sequence == b.sequence
            && a != b
        {
            return Err(Error::IndexCorruption);
        }
        let high_water = repository.discover_generation_high_water()?.unwrap_or(0);
        if let Some(exact) = exact {
            let exact_id = exact.run_set().id().map_err(|_| Error::IdentityMismatch)?;
            if let Some(generation) = repository.latest_bound_generation(exact_id)?
                && head
                    .as_ref()
                    .is_none_or(|h| h.runs.iter().all(|r| r.generation < generation))
            {
                // A newer explicit offline rebuild replaces the old candidate
                // universe, including an intentionally empty rebuilt family.
                head = Some(Head {
                    sequence: head.as_ref().map_or(1, |h| h.sequence),
                    exact: exact_id.bytes(),
                    runs: vec![RunRef {
                        generation,
                        first: 0,
                        last: 0,
                        level: 15,
                        hash: repository.family_hash(generation)?,
                    }],
                });
            }
        }
        let mut runs = Vec::new();
        if let Some(selected) = &head {
            for run in &selected.runs {
                if repository.family_hash(run.generation)? != run.hash {
                    return Err(Error::IdentityMismatch);
                }
                runs.push(Arc::new(repository.recover_generation(run.generation)?));
            }
        }
        // Recover retirement work protected by the previous activation slot.
        // Unselected artifacts from an interrupted publication remain offline
        // maintenance garbage; never guess ownership from a filename alone.
        let retired = heads
            .iter()
            .flatten()
            .flat_map(|h| &h.runs)
            .filter(|r| {
                head.as_ref()
                    .is_none_or(|h| !h.runs.iter().any(|live| live.generation == r.generation))
            })
            .map(|r| (r.generation, Weak::new()))
            .collect();
        let instance = Self {
            repository,
            exact_repository: exact_repository.clone(),
            current: RwLock::new(Some(Arc::new(OnlineReductionGeneration {
                exact: exact_repository.clone(),
                runs: runs.clone(),
            }))),
            publisher: Mutex::new(Publisher {
                head,
                heads,
                high_water,
                retired,
            }),
            published_batches: AtomicU64::new(0),
            skipped_entries: AtomicU64::new(0),
            compactions: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        };
        if let Some(exact) = exact {
            let mut publisher = instance
                .publisher
                .lock()
                .expect("Similarity publisher lock");
            let exact_id = exact.run_set().id().map_err(|_| Error::IdentityMismatch)?;
            let sequence = publisher
                .head
                .as_ref()
                .map_or(Some(1), |h| h.sequence.checked_add(1))
                .ok_or(Error::CounterOverflow)?;
            let next = Head {
                sequence,
                exact: exact_id.bytes(),
                runs: publisher
                    .head
                    .as_ref()
                    .map_or_else(Vec::new, |h| h.runs.clone()),
            };
            instance.activate(&mut publisher, next, runs)?;
        }
        Ok(instance)
    }

    pub(crate) fn pin(&self) -> Option<Arc<OnlineReductionGeneration<I>>> {
        self.current
            .read()
            .expect("Similarity generation lock")
            .clone()
    }

    /// Queries only activated immutable bucket states, without DATA reads.
    ///
    /// # Errors
    /// Returns invalid targets or corrupt touched index pages.
    pub fn candidates_prehashed(
        &self,
        id: ChunkId,
        target: &[u8],
    ) -> Result<Vec<SimilarityBaseCandidate>, Error> {
        let fingerprint = SimilarityFingerprint::v1(target).map_err(|_| Error::InvalidTarget)?;
        let length = u32::try_from(target.len()).map_err(|_| Error::InvalidTarget)?;
        self.pin().map_or_else(
            || Ok(Vec::new()),
            |g| g.candidates(id, &fingerprint, length),
        )
    }

    #[must_use]
    pub fn status(&self) -> OnlineSimilarityStatus {
        OnlineSimilarityStatus {
            published_batches: self.published_batches.load(Ordering::Relaxed),
            skipped_entries: self.skipped_entries.load(Ordering::Relaxed),
            compactions: self.compactions.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            active_families: self.pin().map_or(0, |g| g.runs.len()),
        }
    }

    pub(crate) fn page_cache_status(&self) -> crate::SimilarityIndexPageCacheStatus {
        self.repository.page_cache_status()
    }

    pub fn skip(&self, count: usize) {
        self.skipped_entries
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    /// Publishes one bounded batch after its Exact activation. On failure the
    /// previous generation remains visible and DATA publication is unaffected.
    ///
    /// # Errors
    /// Returns invalid entries, bounded-admission or storage errors.
    pub fn append(
        &self,
        exact: &ExactIndexGenerationPin<I>,
        entries: &[SimilarityIndexEntry],
    ) -> Result<(), Error> {
        let exact_id = exact
            .run_set()
            .id()
            .map_err(|_| Error::IdentityMismatch)?
            .bytes();
        self.append_identified(exact_id, entries)
    }

    /// Publishes already durable independent hints without retaining an Exact
    /// generation pin during metadata compaction. Queries resolve every hint
    /// against their own current Exact pin, so stale hints are safe misses.
    ///
    /// # Errors
    /// Returns absent Exact state, invalid entries or publication I/O errors.
    pub fn append_current(&self, entries: &[SimilarityIndexEntry]) -> Result<(), Error> {
        let exact_id = {
            let exact = self
                .exact_repository
                .pin_active_generation()
                .ok_or(Error::IdentityMismatch)?;
            exact
                .run_set()
                .id()
                .map_err(|_| Error::IdentityMismatch)?
                .bytes()
        };
        self.append_identified(exact_id, entries)
    }

    fn append_identified(
        &self,
        exact_id: [u8; 32],
        entries: &[SimilarityIndexEntry],
    ) -> Result<(), Error> {
        let result = self.append_inner(exact_id, entries);
        if result.is_err() {
            self.errors.fetch_add(1, Ordering::Relaxed);
            self.skip(entries.len());
        }
        result
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one serialized publish/compact/activate transaction"
    )]
    fn append_inner(
        &self,
        exact_id: [u8; 32],
        entries: &[SimilarityIndexEntry],
    ) -> Result<(), Error> {
        if entries.len() > ONLINE_SIMILARITY_BATCH_ENTRIES {
            return Err(Error::OutOfMemory);
        }
        let mut publisher = self.publisher.lock().expect("Similarity publisher lock");
        let previous = self.pin();
        let sequence = publisher
            .head
            .as_ref()
            .map_or(Some(1), |h| h.sequence.checked_add(1))
            .ok_or(Error::CounterOverflow)?;
        let mut refs = publisher
            .head
            .as_ref()
            .map_or_else(Vec::new, |h| h.runs.clone());
        let mut runs = previous.as_ref().map_or_else(Vec::new, |g| g.runs.clone());
        // Reject exhausted topology before creating immutable artifacts.
        let mut levels: Vec<_> = refs.iter().map(|r| r.level).collect();
        levels.push(0);
        while let Some(start) = levels
            .windows(4)
            .position(|w| w[0] < 15 && w.iter().all(|level| *level == w[0]))
        {
            let next = levels[start] + 1;
            levels.splice(start..start + 4, [next]);
        }
        if levels.len() > MAX_FAMILIES {
            return Err(Error::OutOfMemory);
        }
        let mut mutations: BTreeMap<SimilarityBucketKey, Vec<SimilarityIndexEntry>> =
            BTreeMap::new();
        for &entry in entries {
            if entry.fingerprint_profile() != crate::SIMILARITY_FINGERPRINT_PROFILE_V1 {
                return Err(Error::UnsupportedProfile);
            }
            for (slot, value) in entry.superfeatures().into_iter().enumerate() {
                let key = SimilarityBucketKey::new(
                    entry.fingerprint_profile(),
                    u8::try_from(slot).map_err(|_| Error::IndexCorruption)?,
                    entry.logical_length(),
                    value,
                )?;
                mutations.entry(key).or_default().push(entry);
            }
        }
        // The builder loads one old value at a time. It does not retain 64
        // historical representatives for every changed key simultaneously.
        if !mutations.is_empty() {
            let generation = next_generation(&mut publisher)?;
            let mut buckets = mutations
                .into_iter()
                .filter_map(|(key, values)| {
                    let result = (|| {
                        let mut merged = previous
                            .as_ref()
                            .map_or_else(|| Ok(Vec::new()), |g| g.bucket(key))?;
                        let old = merged.clone();
                        merge_entries(&mut merged, &values)?;
                        Ok((old != merged).then_some((key, merged)))
                    })();
                    match result {
                        Ok(value) => value.map(Ok),
                        Err(error) => Some(Err(error)),
                    }
                })
                .peekable();
            if buckets.peek().is_some() {
                let run = Arc::new(self.repository.publish_buckets(generation, buckets)?);
                refs.push(RunRef {
                    generation,
                    first: sequence,
                    last: sequence,
                    level: 0,
                    hash: self.repository.family_hash(generation)?,
                });
                runs.push(run);
            }
        }
        while let Some(start) = refs
            .windows(4)
            .position(|w| w[0].level < 15 && w.iter().all(|r| r.level == w[0].level))
        {
            let generation = next_generation(&mut publisher)?;
            let mut cursors: Vec<_> = runs[start..start + 4]
                .iter()
                .map(|run| run.buckets())
                .collect();
            let mut pending: Vec<_> = cursors.iter_mut().map(Iterator::next).collect();
            let mut failed = false;
            let merge = std::iter::from_fn(|| {
                if failed {
                    return None;
                }
                if let Some(i) = pending.iter().position(|v| matches!(v, Some(Err(_)))) {
                    failed = true;
                    return pending[i].take();
                }
                let key = pending
                    .iter()
                    .filter_map(|v| v.as_ref().and_then(|r| r.as_ref().ok()).map(|b| b.0))
                    .min()?;
                let mut newest: Option<Bucket> = None;
                for i in 0..4 {
                    if pending[i]
                        .as_ref()
                        .and_then(|r| r.as_ref().ok())
                        .is_some_and(|b| b.0 == key)
                    {
                        newest = pending[i].take().and_then(Result::ok);
                        pending[i] = cursors[i].next();
                    }
                }
                newest.map(Ok)
            });
            let run = Arc::new(self.repository.publish_buckets(generation, merge)?);
            drop(cursors);
            let reference = RunRef {
                generation,
                first: refs[start].first,
                last: refs[start + 3].last,
                level: refs[start].level + 1,
                hash: self.repository.family_hash(generation)?,
            };
            for (reference, run) in refs[start..start + 4].iter().zip(&runs[start..start + 4]) {
                publisher
                    .retired
                    .push((reference.generation, Arc::downgrade(run)));
            }
            refs.splice(start..start + 4, [reference]);
            runs.splice(start..start + 4, [run]);
            self.compactions.fetch_add(1, Ordering::Relaxed);
        }
        if refs.len() > MAX_FAMILIES {
            return Err(Error::OutOfMemory);
        }
        let head = Head {
            sequence,
            exact: exact_id,
            runs: refs,
        };
        self.activate(&mut publisher, head, runs)?;
        self.published_batches.fetch_add(1, Ordering::Relaxed);
        drop(previous);
        let keep: Vec<_> = publisher
            .heads
            .iter()
            .flatten()
            .flat_map(|h| h.runs.iter().map(|r| r.generation))
            .collect();
        publisher.retired.retain(|(generation, weak)| {
            keep.contains(generation)
                || weak.strong_count() != 0
                || self.repository.remove_family(*generation).is_err()
        });
        Ok(())
    }

    fn activate(
        &self,
        publisher: &mut Publisher<I>,
        head: Head,
        runs: Vec<Arc<RecoveredSimilarityIndex<I>>>,
    ) -> Result<(), Error> {
        let slot = (head.sequence % 2) as usize;
        let storage = self.repository.storage();
        let name = HEAD_NAMES[slot];
        let bytes = head.encode()?;
        if !storage.exists(name)? {
            storage.create_new(name)?;
        }
        storage.write_at(name, 0, &bytes)?;
        storage.set_len(name, HEAD_BYTES as u64)?;
        let observed = storage.read_exact_at(name, 0, HEAD_BYTES)?;
        if Head::decode(&observed)? != head {
            return Err(Error::IdentityMismatch);
        }
        storage.sync_file(name)?;
        storage.sync_root()?;
        publisher.heads[slot] = Some(head.clone());
        publisher.head = Some(head);
        *self.current.write().expect("Similarity generation lock") =
            Some(Arc::new(OnlineReductionGeneration {
                exact: self.exact_repository.clone(),
                runs,
            }));
        Ok(())
    }

    /// Independently audits the newest complete selection and every referenced
    /// physical partition; it never scans DATA or mutates the index.
    ///
    /// # Errors
    /// Returns invalid heads, bindings, missing families or corrupt partitions.
    pub fn audit(repository: &SimilarityIndexRepository<I>) -> Result<usize, Error> {
        let mut heads = Vec::new();
        for name in HEAD_NAMES {
            if repository.storage().exists(name)?
                && repository.storage().object_len(name)? == HEAD_BYTES as u64
                && let Ok(head) =
                    Head::decode(&repository.storage().read_exact_at(name, 0, HEAD_BYTES)?)
            {
                heads.push(head);
            }
        }
        if heads.len() == 2 && heads[0].sequence == heads[1].sequence && heads[0] != heads[1] {
            return Err(Error::IndexCorruption);
        }
        let Some(head) = heads.into_iter().max_by_key(|h| h.sequence) else {
            return Err(Error::IndexCorruption);
        };
        for run in &head.runs {
            if repository.family_hash(run.generation)? != run.hash {
                return Err(Error::IdentityMismatch);
            }
            repository.audit_generation(run.generation)?;
        }
        Ok(head.runs.len())
    }

    /// Whether this repository has ever selected an online generation.
    ///
    /// # Errors
    /// Returns storage errors.
    pub fn exists(repository: &SimilarityIndexRepository<I>) -> Result<bool, Error> {
        Ok(repository.storage().exists(HEAD_NAMES[0])?
            || repository.storage().exists(HEAD_NAMES[1])?)
    }
}

fn next_generation<I>(publisher: &mut Publisher<I>) -> Result<u64, Error> {
    publisher.high_water = publisher
        .high_water
        .checked_add(1)
        .ok_or(Error::CounterOverflow)?;
    Ok(publisher.high_water)
}

fn merge_entries(
    entries: &mut Vec<SimilarityIndexEntry>,
    additions: &[SimilarityIndexEntry],
) -> Result<(), Error> {
    for &entry in additions {
        match entries.binary_search_by_key(&entry.chunk_id(), |e| e.chunk_id()) {
            Ok(i) => {
                if entries[i] != entry {
                    return Err(Error::IndexCorruption);
                }
            }
            Err(i) if i < 64 => {
                entries.insert(i, entry);
                entries.truncate(64);
            }
            Err(_) => (),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head() -> Head {
        Head {
            sequence: 2,
            exact: [1; 32],
            runs: vec![RunRef {
                generation: 1,
                first: 1,
                last: 1,
                level: 0,
                hash: [2; 32],
            }],
        }
    }

    #[test]
    fn head_format_rejects_noncanonical_reserved_bytes_even_with_valid_hash() {
        let canonical = head().encode().unwrap();
        assert_eq!(Head::decode(&canonical).unwrap(), head());
        for offset in [56, 89, 95, 128, HEAD_BYTES - 33] {
            let mut bytes = canonical;
            bytes[offset] = 1;
            let hash = blake3::hash(&bytes[..HEAD_BYTES - 32]);
            bytes[HEAD_BYTES - 32..].copy_from_slice(hash.as_bytes());
            assert!(Head::decode(&bytes).is_err(), "reserved byte {offset}");
        }
    }

    #[test]
    fn head_format_rejects_overlapping_chronology_and_duplicate_identity() {
        let mut value = head();
        let mut newer = value.runs[0];
        newer.generation = 2;
        value.runs.push(newer);
        assert!(value.encode().is_err());
        value.runs[1].first = 2;
        value.runs[1].last = 2;
        assert!(value.encode().is_ok());
        value.runs[1].generation = 1;
        assert!(value.encode().is_err());
        value.runs.truncate(1);
        value.runs[0].last = 3;
        assert!(value.encode().is_err());
    }
}
