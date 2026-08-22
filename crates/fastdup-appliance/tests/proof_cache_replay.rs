use fastdup_appliance::{
    ProofCacheEvent, ProofCachePolicy, ProofCacheReplayError, ProofCacheTrace, ProofKey,
    replay_proof_cache_trace,
};
use fastdup_format::ChunkId;

const BYTES_PER_PROOF: u64 = 192;

fn key(value: u64) -> ProofKey {
    let mut bytes = [0_u8; 32];
    bytes[..8].copy_from_slice(&value.to_le_bytes());
    ProofKey::new(ChunkId::from_bytes(bytes), 64 * 1_024)
}

#[test]
fn identical_byte_budget_exposes_scan_retention_difference() {
    let hot = key(1);
    let mut events = vec![ProofCacheEvent::admit_exact_reuse(hot, 68 * 1_024)];
    for value in 2..=40 {
        events.push(ProofCacheEvent::admit_published(key(value), 68 * 1_024));
    }
    events.push(ProofCacheEvent::lookup(hot));
    let trace = ProofCacheTrace::new(events).expect("bounded trace");

    let encoded = trace.encode().expect("encode trace");
    let decoded = ProofCacheTrace::decode(&encoded).expect("decode trace");
    assert_eq!(decoded, trace);

    let budget = 10 * BYTES_PER_PROOF;
    let s3 = replay_proof_cache_trace(&decoded, ProofCachePolicy::S3Fifo, budget)
        .expect("replay S3-FIFO");
    let sieve =
        replay_proof_cache_trace(&decoded, ProofCachePolicy::Sieve, budget).expect("replay SIEVE");

    assert_eq!(s3.byte_budget(), budget);
    assert_eq!(sieve.byte_budget(), budget);
    assert_eq!(s3.capacity(), 10);
    assert_eq!(sieve.capacity(), 10);
    assert_eq!(s3.lookups(), 1);
    assert_eq!(s3.hits(), 1);
    assert_eq!(s3.misses(), 0);
    assert_eq!(sieve.lookups(), 1);
    assert_eq!(sieve.hits(), 0);
    assert_eq!(sieve.misses(), 1);
    assert_eq!(s3.avoided_verify_bytes(), 68 * 1_024);
    assert_eq!(sieve.avoided_verify_bytes(), 0);
    assert!(s3.maximum_eviction_steps() <= 10);
    assert!(sieve.maximum_eviction_steps() <= 10);
}

#[test]
fn sieve_rejects_one_admission_after_bounded_full_ring_scan() {
    let first = key(100);
    let second = key(101);
    let trace = ProofCacheTrace::new(vec![
        ProofCacheEvent::admit_published(first, 68 * 1_024),
        ProofCacheEvent::admit_published(second, 68 * 1_024),
        ProofCacheEvent::lookup(first),
        ProofCacheEvent::lookup(second),
        ProofCacheEvent::admit_published(key(102), 68 * 1_024),
    ])
    .expect("bounded full-ring trace");

    let report = replay_proof_cache_trace(&trace, ProofCachePolicy::Sieve, 2 * BYTES_PER_PROOF)
        .expect("bounded SIEVE rejection is not a replay failure");

    assert_eq!(report.admission_rejections(), 1);
    assert_eq!(report.evictions(), 0);
    assert_eq!(report.maximum_eviction_steps(), 2);
}

#[test]
fn trace_reader_rejects_authenticated_payload_corruption() {
    let trace = ProofCacheTrace::new(vec![ProofCacheEvent::admit_published(key(200), 68 * 1_024)])
        .expect("one-event trace");
    let mut encoded = trace.encode().expect("encode trace");
    encoded[80] ^= 0x80;

    assert_eq!(
        ProofCacheTrace::decode(&encoded),
        Err(ProofCacheReplayError::TraceHashMismatch)
    );
}

#[test]
fn cold_lookup_does_not_claim_a_physical_verification() {
    let cold = key(300);
    let trace = ProofCacheTrace::new(vec![
        ProofCacheEvent::lookup(cold),
        ProofCacheEvent::admit_published(cold, 68 * 1_024),
        ProofCacheEvent::lookup(cold),
    ])
    .expect("cold-then-published trace");

    let report = replay_proof_cache_trace(&trace, ProofCachePolicy::Sieve, 2 * BYTES_PER_PROOF)
        .expect("replay cold publication");

    assert_eq!(report.lookups(), 2);
    assert_eq!(report.hits(), 1);
    assert_eq!(report.misses(), 1);
    assert_eq!(report.required_verify_bytes(), 0);
    assert_eq!(report.avoided_verify_bytes(), 68 * 1_024);
}
