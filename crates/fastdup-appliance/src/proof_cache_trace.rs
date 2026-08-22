use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use fastdup_format::ChunkId;

const TRACE_MAGIC: [u8; 8] = *b"FDPTRC01";
const TRACE_VERSION: u16 = 1;
const TRACE_HEADER_BYTES: usize = 64;
const TRACE_RECORD_BYTES: usize = 56;
const MAX_TRACE_EVENTS: usize = 4_194_304;
const BUDGET_BYTES_PER_RESIDENT_PROOF: u64 = 192;
const MAX_EVICTION_STEPS_PER_ADMISSION: usize = 256;

/// One byte-exact Chunk identity used by the proof-cache trace and replay.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProofKey {
    chunk_id: ChunkId,
    logical_length: u32,
}

impl ProofKey {
    #[must_use]
    pub const fn new(chunk_id: ChunkId, logical_length: u32) -> Self {
        Self {
            chunk_id,
            logical_length,
        }
    }

    #[must_use]
    pub const fn chunk_id(self) -> ChunkId {
        self.chunk_id
    }

    #[must_use]
    pub const fn logical_length(self) -> u32 {
        self.logical_length
    }
}

/// One payload-free event observed at the online proof-cache seam.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofCacheEvent {
    Lookup { key: ProofKey },
    AdmitPublished { key: ProofKey, verify_bytes: u32 },
    AdmitExactReuse { key: ProofKey, verify_bytes: u32 },
}

impl ProofCacheEvent {
    #[must_use]
    pub const fn lookup(key: ProofKey) -> Self {
        Self::Lookup { key }
    }

    #[must_use]
    pub const fn admit_published(key: ProofKey, verify_bytes: u32) -> Self {
        Self::AdmitPublished { key, verify_bytes }
    }

    #[must_use]
    pub const fn admit_exact_reuse(key: ProofKey, verify_bytes: u32) -> Self {
        Self::AdmitExactReuse { key, verify_bytes }
    }

    const fn key(self) -> ProofKey {
        match self {
            Self::Lookup { key }
            | Self::AdmitPublished { key, .. }
            | Self::AdmitExactReuse { key, .. } => key,
        }
    }

    const fn verify_bytes(self) -> u32 {
        match self {
            Self::Lookup { .. } => 0,
            Self::AdmitPublished { verify_bytes, .. }
            | Self::AdmitExactReuse { verify_bytes, .. } => verify_bytes,
        }
    }

    const fn kind(self) -> u8 {
        match self {
            Self::Lookup { .. } => 1,
            Self::AdmitPublished { .. } => 2,
            Self::AdmitExactReuse { .. } => 3,
        }
    }
}

/// Versioned sequence of real online proof-cache events without file payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProofCacheTrace {
    events: Vec<ProofCacheEvent>,
}

impl ProofCacheTrace {
    /// Constructs a bounded trace.
    ///
    /// # Errors
    ///
    /// Rejects empty Chunk lengths, zero verification spans, or too many events.
    pub fn new(events: Vec<ProofCacheEvent>) -> Result<Self, ProofCacheReplayError> {
        if events.len() > MAX_TRACE_EVENTS {
            return Err(ProofCacheReplayError::TraceTooLarge);
        }
        for event in &events {
            if event.key().logical_length == 0
                || !matches!(event, ProofCacheEvent::Lookup { .. }) && event.verify_bytes() == 0
            {
                return Err(ProofCacheReplayError::InvalidEvent);
            }
        }
        Ok(Self { events })
    }

    #[must_use]
    pub fn events(&self) -> &[ProofCacheEvent] {
        &self.events
    }

    /// Encodes Trace v1 field by field and authenticates every record together.
    ///
    /// # Errors
    ///
    /// Returns an overflow error if the bounded encoded length is not representable.
    pub fn encode(&self) -> Result<Vec<u8>, ProofCacheReplayError> {
        let payload_bytes = self
            .events
            .len()
            .checked_mul(TRACE_RECORD_BYTES)
            .ok_or(ProofCacheReplayError::ArithmeticOverflow)?;
        let total_bytes = TRACE_HEADER_BYTES
            .checked_add(payload_bytes)
            .ok_or(ProofCacheReplayError::ArithmeticOverflow)?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(total_bytes)
            .map_err(|_| ProofCacheReplayError::OutOfMemory)?;
        encoded.resize(total_bytes, 0);
        encoded[0..8].copy_from_slice(&TRACE_MAGIC);
        put_u16(&mut encoded, 8, TRACE_VERSION);
        put_u16(
            &mut encoded,
            10,
            u16::try_from(TRACE_HEADER_BYTES)
                .map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?,
        );
        put_u16(
            &mut encoded,
            12,
            u16::try_from(TRACE_RECORD_BYTES)
                .map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?,
        );
        put_u64(
            &mut encoded,
            16,
            u64::try_from(self.events.len())
                .map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?,
        );
        put_u64(
            &mut encoded,
            24,
            u64::try_from(payload_bytes).map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?,
        );
        for (index, event) in self.events.iter().copied().enumerate() {
            let offset = TRACE_HEADER_BYTES + index * TRACE_RECORD_BYTES;
            encoded[offset] = event.kind();
            put_u32(&mut encoded, offset + 4, event.key().logical_length);
            put_u32(&mut encoded, offset + 8, event.verify_bytes());
            encoded[offset + 16..offset + 48].copy_from_slice(&event.key().chunk_id.bytes());
            put_u64(
                &mut encoded,
                offset + 48,
                u64::try_from(index + 1).map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?,
            );
        }
        let payload_hash = blake3::hash(&encoded[TRACE_HEADER_BYTES..]);
        encoded[32..64].copy_from_slice(payload_hash.as_bytes());
        Ok(encoded)
    }

    /// Decodes and verifies a complete Trace v1 image.
    ///
    /// # Errors
    ///
    /// Rejects unknown versions or kinds, nonzero reserved bytes, inconsistent
    /// bounds or sequence numbers, and any payload hash mismatch.
    pub fn decode(encoded: &[u8]) -> Result<Self, ProofCacheReplayError> {
        if encoded.len() < TRACE_HEADER_BYTES || encoded[0..8] != TRACE_MAGIC {
            return Err(ProofCacheReplayError::InvalidTraceHeader);
        }
        if get_u16(encoded, 8) != TRACE_VERSION
            || usize::from(get_u16(encoded, 10)) != TRACE_HEADER_BYTES
            || usize::from(get_u16(encoded, 12)) != TRACE_RECORD_BYTES
            || encoded[14..16] != [0; 2]
        {
            return Err(ProofCacheReplayError::InvalidTraceHeader);
        }
        let event_count = usize::try_from(get_u64(encoded, 16))
            .map_err(|_| ProofCacheReplayError::TraceTooLarge)?;
        if event_count > MAX_TRACE_EVENTS {
            return Err(ProofCacheReplayError::TraceTooLarge);
        }
        let payload_bytes = event_count
            .checked_mul(TRACE_RECORD_BYTES)
            .ok_or(ProofCacheReplayError::ArithmeticOverflow)?;
        if get_u64(encoded, 24)
            != u64::try_from(payload_bytes)
                .map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?
            || encoded.len() != TRACE_HEADER_BYTES + payload_bytes
        {
            return Err(ProofCacheReplayError::InvalidTraceBounds);
        }
        if encoded[32..64] != *blake3::hash(&encoded[TRACE_HEADER_BYTES..]).as_bytes() {
            return Err(ProofCacheReplayError::TraceHashMismatch);
        }
        let mut events = Vec::new();
        events
            .try_reserve_exact(event_count)
            .map_err(|_| ProofCacheReplayError::OutOfMemory)?;
        for index in 0..event_count {
            let offset = TRACE_HEADER_BYTES + index * TRACE_RECORD_BYTES;
            if encoded[offset + 1..offset + 4] != [0; 3]
                || encoded[offset + 12..offset + 16] != [0; 4]
                || get_u64(encoded, offset + 48)
                    != u64::try_from(index + 1)
                        .map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?
            {
                return Err(ProofCacheReplayError::InvalidEvent);
            }
            let logical_length = get_u32(encoded, offset + 4);
            let verify_bytes = get_u32(encoded, offset + 8);
            let mut chunk_id = [0_u8; 32];
            chunk_id.copy_from_slice(&encoded[offset + 16..offset + 48]);
            let key = ProofKey::new(ChunkId::from_bytes(chunk_id), logical_length);
            let event = match encoded[offset] {
                1 if verify_bytes == 0 => ProofCacheEvent::lookup(key),
                2 => ProofCacheEvent::admit_published(key, verify_bytes),
                3 => ProofCacheEvent::admit_exact_reuse(key, verify_bytes),
                _ => return Err(ProofCacheReplayError::InvalidEvent),
            };
            events.push(event);
        }
        Self::new(events)
    }
}

#[derive(Debug, Default)]
pub(crate) struct ProofCacheTraceRecorder {
    enabled: AtomicBool,
    state: Mutex<Option<ActiveTrace>>,
}

#[derive(Debug)]
struct ActiveTrace {
    max_events: usize,
    events: Vec<ProofCacheEvent>,
    overflowed: bool,
}

impl ProofCacheTraceRecorder {
    pub(crate) fn start(&self, max_events: usize) -> Result<(), ProofCacheReplayError> {
        if max_events == 0 || max_events > MAX_TRACE_EVENTS {
            return Err(ProofCacheReplayError::TraceTooLarge);
        }
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: online proof trace lock poisoned");
        if state.is_some() {
            return Err(ProofCacheReplayError::TraceAlreadyActive);
        }
        *state = Some(ActiveTrace {
            max_events,
            events: Vec::new(),
            overflowed: false,
        });
        self.enabled.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn record(&self, event: ProofCacheEvent) {
        if !self.enabled.load(Ordering::Acquire) {
            return;
        }
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: online proof trace lock poisoned");
        let Some(active) = state.as_mut() else {
            return;
        };
        if active.events.len() == active.max_events {
            active.overflowed = true;
            return;
        }
        active.events.push(event);
    }

    pub(crate) fn finish(&self) -> Result<ProofCacheTrace, ProofCacheReplayError> {
        if !self.enabled.swap(false, Ordering::AcqRel) {
            return Err(ProofCacheReplayError::TraceNotActive);
        }
        let active = self
            .state
            .lock()
            .expect("ASSERT: online proof trace lock poisoned")
            .take()
            .ok_or(ProofCacheReplayError::TraceNotActive)?;
        if active.overflowed {
            return Err(ProofCacheReplayError::TraceCaptureOverflow);
        }
        ProofCacheTrace::new(active.events)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofCachePolicy {
    S3Fifo,
    Sieve,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProofCacheReplayReport {
    policy: ProofCachePolicy,
    byte_budget: u64,
    capacity: usize,
    lookups: u64,
    hits: u64,
    misses: u64,
    admissions: u64,
    admission_rejections: u64,
    evictions: u64,
    avoided_verify_bytes: u64,
    required_verify_bytes: u64,
    maximum_eviction_steps: usize,
}

impl ProofCacheReplayReport {
    #[must_use]
    pub const fn policy(self) -> ProofCachePolicy {
        self.policy
    }
    #[must_use]
    pub const fn byte_budget(self) -> u64 {
        self.byte_budget
    }
    #[must_use]
    pub const fn capacity(self) -> usize {
        self.capacity
    }
    #[must_use]
    pub const fn lookups(self) -> u64 {
        self.lookups
    }
    #[must_use]
    pub const fn hits(self) -> u64 {
        self.hits
    }
    #[must_use]
    pub const fn misses(self) -> u64 {
        self.misses
    }
    #[must_use]
    pub const fn admissions(self) -> u64 {
        self.admissions
    }
    #[must_use]
    pub const fn admission_rejections(self) -> u64 {
        self.admission_rejections
    }
    #[must_use]
    pub const fn evictions(self) -> u64 {
        self.evictions
    }
    #[must_use]
    pub const fn avoided_verify_bytes(self) -> u64 {
        self.avoided_verify_bytes
    }
    #[must_use]
    pub const fn required_verify_bytes(self) -> u64 {
        self.required_verify_bytes
    }
    #[must_use]
    pub const fn maximum_eviction_steps(self) -> usize {
        self.maximum_eviction_steps
    }
}

/// Replays one trace under a conservative common 192-byte resident-proof charge.
///
/// # Errors
///
/// Rejects a budget smaller than one resident proof, conflicting verification
/// spans for one byte-exact key, arithmetic overflow, or an impossible policy state.
pub fn replay_proof_cache_trace(
    trace: &ProofCacheTrace,
    policy: ProofCachePolicy,
    byte_budget: u64,
) -> Result<ProofCacheReplayReport, ProofCacheReplayError> {
    let capacity = usize::try_from(byte_budget / BUDGET_BYTES_PER_RESIDENT_PROOF)
        .map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?;
    if capacity == 0 {
        return Err(ProofCacheReplayError::BudgetTooSmall);
    }
    // Only already-published entries have a physical location that a miss must
    // verify. Looking up a new chunk before its publication is a cold lookup,
    // not container read amplification. Keeping this catalog temporal also
    // prevents future trace events from changing earlier accounting.
    let mut verify_bytes = BTreeMap::new();
    let mut state: Box<dyn ReplayPolicy> = match policy {
        ProofCachePolicy::S3Fifo => Box::new(S3Fifo::new(capacity)),
        ProofCachePolicy::Sieve => Box::new(Sieve::new(capacity)),
    };
    let mut report = ProofCacheReplayReport {
        policy,
        byte_budget,
        capacity,
        lookups: 0,
        hits: 0,
        misses: 0,
        admissions: 0,
        admission_rejections: 0,
        evictions: 0,
        avoided_verify_bytes: 0,
        required_verify_bytes: 0,
        maximum_eviction_steps: 0,
    };
    for event in trace.events().iter().copied() {
        match event {
            ProofCacheEvent::Lookup { key } => {
                report.lookups = checked_add(report.lookups, 1)?;
                let bytes = u64::from(verify_bytes.get(&key).copied().unwrap_or(0));
                if state.access(key) {
                    report.hits = checked_add(report.hits, 1)?;
                    report.avoided_verify_bytes = checked_add(report.avoided_verify_bytes, bytes)?;
                } else {
                    report.misses = checked_add(report.misses, 1)?;
                    report.required_verify_bytes =
                        checked_add(report.required_verify_bytes, bytes)?;
                }
            }
            ProofCacheEvent::AdmitPublished {
                key,
                verify_bytes: bytes,
            } => {
                remember_verification_span(&mut verify_bytes, key, bytes)?;
                report.admissions = checked_add(report.admissions, 1)?;
                let outcome = state.admit(key, Admission::Published)?;
                if outcome.rejected {
                    report.admission_rejections = checked_add(report.admission_rejections, 1)?;
                }
                report.evictions = checked_add(
                    report.evictions,
                    u64::try_from(outcome.evictions)
                        .map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?,
                )?;
                report.maximum_eviction_steps = report.maximum_eviction_steps.max(outcome.steps);
            }
            ProofCacheEvent::AdmitExactReuse {
                key,
                verify_bytes: bytes,
            } => {
                remember_verification_span(&mut verify_bytes, key, bytes)?;
                report.admissions = checked_add(report.admissions, 1)?;
                let outcome = state.admit(key, Admission::ExactReuse)?;
                if outcome.rejected {
                    report.admission_rejections = checked_add(report.admission_rejections, 1)?;
                }
                report.evictions = checked_add(
                    report.evictions,
                    u64::try_from(outcome.evictions)
                        .map_err(|_| ProofCacheReplayError::ArithmeticOverflow)?,
                )?;
                report.maximum_eviction_steps = report.maximum_eviction_steps.max(outcome.steps);
            }
        }
    }
    Ok(report)
}

fn remember_verification_span(
    spans: &mut BTreeMap<ProofKey, u32>,
    key: ProofKey,
    bytes: u32,
) -> Result<(), ProofCacheReplayError> {
    if let Some(previous) = spans.insert(key, bytes)
        && previous != bytes
    {
        return Err(ProofCacheReplayError::ConflictingVerificationSpan);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Admission {
    Published,
    ExactReuse,
}

#[derive(Default)]
struct AdmissionOutcome {
    evictions: usize,
    steps: usize,
    rejected: bool,
}

trait ReplayPolicy {
    fn access(&mut self, key: ProofKey) -> bool;
    fn admit(
        &mut self,
        key: ProofKey,
        admission: Admission,
    ) -> Result<AdmissionOutcome, ProofCacheReplayError>;
}

#[derive(Clone, Copy)]
enum S3Queue {
    Small,
    Main,
}

#[derive(Clone, Copy)]
struct S3Entry {
    queue: S3Queue,
    frequency: u8,
}

struct S3Fifo {
    capacity: usize,
    small_target: usize,
    ghost_target: usize,
    entries: BTreeMap<ProofKey, S3Entry>,
    small: VecDeque<ProofKey>,
    main: VecDeque<ProofKey>,
    ghost: VecDeque<ProofKey>,
}

impl S3Fifo {
    fn new(capacity: usize) -> Self {
        let small_target = (capacity / 10).max(1);
        Self {
            capacity,
            small_target,
            ghost_target: capacity.saturating_sub(small_target).max(1),
            entries: BTreeMap::new(),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
        }
    }

    fn evict_one(&mut self, outcome: &mut AdmissionOutcome) -> Result<bool, ProofCacheReplayError> {
        outcome.steps = outcome
            .steps
            .checked_add(1)
            .ok_or(ProofCacheReplayError::ArithmeticOverflow)?;
        let step_limit = self
            .capacity
            .saturating_mul(4)
            .min(MAX_EVICTION_STEPS_PER_ADMISSION);
        if outcome.steps > step_limit {
            outcome.steps = step_limit;
            return Ok(false);
        }
        if self.small.len() >= self.small_target {
            let key = self
                .small
                .pop_front()
                .ok_or(ProofCacheReplayError::InvalidPolicyState)?;
            let entry = self
                .entries
                .get_mut(&key)
                .ok_or(ProofCacheReplayError::InvalidPolicyState)?;
            if entry.frequency > 1 {
                entry.queue = S3Queue::Main;
                entry.frequency = 0;
                self.main.push_back(key);
            } else {
                self.entries.remove(&key);
                self.ghost.push_back(key);
                while self.ghost.len() > self.ghost_target {
                    self.ghost.pop_front();
                }
                outcome.evictions += 1;
            }
        } else {
            let key = self
                .main
                .pop_front()
                .ok_or(ProofCacheReplayError::InvalidPolicyState)?;
            let entry = self
                .entries
                .get_mut(&key)
                .ok_or(ProofCacheReplayError::InvalidPolicyState)?;
            if entry.frequency > 0 {
                entry.frequency -= 1;
                self.main.push_back(key);
            } else {
                self.entries.remove(&key);
                outcome.evictions += 1;
            }
        }
        Ok(true)
    }
}

impl ReplayPolicy for S3Fifo {
    fn access(&mut self, key: ProofKey) -> bool {
        let Some(entry) = self.entries.get_mut(&key) else {
            return false;
        };
        entry.frequency = entry.frequency.saturating_add(1).min(3);
        true
    }

    fn admit(
        &mut self,
        key: ProofKey,
        admission: Admission,
    ) -> Result<AdmissionOutcome, ProofCacheReplayError> {
        if self.entries.contains_key(&key) {
            return Ok(AdmissionOutcome::default());
        }
        let mut outcome = AdmissionOutcome::default();
        while self.entries.len() >= self.capacity {
            if !self.evict_one(&mut outcome)? {
                outcome.rejected = true;
                return Ok(outcome);
            }
        }
        let ghost_hit = self.ghost.iter().position(|candidate| *candidate == key);
        if let Some(position) = ghost_hit {
            self.ghost.remove(position);
        }
        let to_main = matches!(admission, Admission::ExactReuse) || ghost_hit.is_some();
        let queue = if to_main {
            S3Queue::Main
        } else {
            S3Queue::Small
        };
        self.entries.insert(
            key,
            S3Entry {
                queue,
                frequency: 0,
            },
        );
        if to_main {
            self.main.push_back(key);
        } else {
            self.small.push_back(key);
        }
        Ok(outcome)
    }
}

#[derive(Clone, Copy)]
struct SieveNode {
    key: ProofKey,
    visited: bool,
    previous: Option<usize>,
    next: Option<usize>,
}

struct Sieve {
    capacity: usize,
    index: BTreeMap<ProofKey, usize>,
    nodes: Vec<Option<SieveNode>>,
    free: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    hand: Option<usize>,
}

impl Sieve {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            index: BTreeMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
            hand: None,
        }
    }

    fn node(&self, slot: usize) -> Result<SieveNode, ProofCacheReplayError> {
        self.nodes
            .get(slot)
            .and_then(|node| *node)
            .ok_or(ProofCacheReplayError::InvalidPolicyState)
    }

    fn allocate(&mut self, node: SieveNode) -> usize {
        if let Some(slot) = self.free.pop() {
            self.nodes[slot] = Some(node);
            slot
        } else {
            self.nodes.push(Some(node));
            self.nodes.len() - 1
        }
    }

    fn remove(&mut self, slot: usize) -> Result<ProofKey, ProofCacheReplayError> {
        let node = self.node(slot)?;
        if let Some(previous) = node.previous {
            self.nodes[previous]
                .as_mut()
                .ok_or(ProofCacheReplayError::InvalidPolicyState)?
                .next = node.next;
        } else {
            self.head = node.next;
        }
        if let Some(next) = node.next {
            self.nodes[next]
                .as_mut()
                .ok_or(ProofCacheReplayError::InvalidPolicyState)?
                .previous = node.previous;
        } else {
            self.tail = node.previous;
        }
        self.nodes[slot] = None;
        self.free.push(slot);
        self.index.remove(&node.key);
        Ok(node.key)
    }
}

impl ReplayPolicy for Sieve {
    fn access(&mut self, key: ProofKey) -> bool {
        let Some(slot) = self.index.get(&key).copied() else {
            return false;
        };
        self.nodes[slot]
            .as_mut()
            .expect("ASSERT: SIEVE index owns a live node")
            .visited = true;
        true
    }

    fn admit(
        &mut self,
        key: ProofKey,
        _admission: Admission,
    ) -> Result<AdmissionOutcome, ProofCacheReplayError> {
        if self.index.contains_key(&key) {
            return Ok(AdmissionOutcome::default());
        }
        let mut outcome = AdmissionOutcome::default();
        if self.index.len() == self.capacity {
            let mut candidate = self
                .hand
                .or(self.tail)
                .ok_or(ProofCacheReplayError::InvalidPolicyState)?;
            loop {
                outcome.steps += 1;
                let step_limit = self.capacity.min(MAX_EVICTION_STEPS_PER_ADMISSION);
                if outcome.steps > step_limit {
                    outcome.steps = step_limit;
                    outcome.rejected = true;
                    return Ok(outcome);
                }
                let node = self.node(candidate)?;
                if node.visited {
                    self.nodes[candidate]
                        .as_mut()
                        .ok_or(ProofCacheReplayError::InvalidPolicyState)?
                        .visited = false;
                    candidate = node
                        .previous
                        .or(self.tail)
                        .ok_or(ProofCacheReplayError::InvalidPolicyState)?;
                } else {
                    self.hand = node.previous;
                    self.remove(candidate)?;
                    outcome.evictions = 1;
                    break;
                }
            }
        }
        let previous_head = self.head;
        let slot = self.allocate(SieveNode {
            key,
            visited: false,
            previous: None,
            next: previous_head,
        });
        if let Some(head) = previous_head {
            self.nodes[head]
                .as_mut()
                .ok_or(ProofCacheReplayError::InvalidPolicyState)?
                .previous = Some(slot);
        } else {
            self.tail = Some(slot);
        }
        self.head = Some(slot);
        self.index.insert(key, slot);
        Ok(outcome)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofCacheReplayError {
    ArithmeticOverflow,
    BudgetTooSmall,
    ConflictingVerificationSpan,
    EvictionStepLimit,
    InvalidEvent,
    InvalidPolicyState,
    InvalidTraceBounds,
    InvalidTraceHeader,
    OutOfMemory,
    TraceHashMismatch,
    TraceAlreadyActive,
    TraceCaptureOverflow,
    TraceNotActive,
    TraceTooLarge,
}

impl fmt::Display for ProofCacheReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for ProofCacheReplayError {}

fn checked_add(left: u64, right: u64) -> Result<u64, ProofCacheReplayError> {
    left.checked_add(right)
        .ok_or(ProofCacheReplayError::ArithmeticOverflow)
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
            .expect("ASSERT: verified trace field bounds"),
    )
}
fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("ASSERT: verified trace field bounds"),
    )
}
fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("ASSERT: verified trace field bounds"),
    )
}
