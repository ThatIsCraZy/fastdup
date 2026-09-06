use super::*;

#[test]
fn consuming_read_view_preserves_nonzero_backing_offset_and_checked_bounds() {
    let mut payload =
        VerifiedChunkPayload::from_owned(ChunkId::of(b"0123456789"), b"0123456789".to_vec());
    payload.offset = 3;
    payload.length = 4;
    let pointer = payload.as_slice()[1..].as_ptr();
    let view = payload.clone().into_read_view(1..4).unwrap();
    assert_eq!(view.as_ref(), b"456");
    assert_eq!(view.as_ref().as_ptr(), pointer);
    assert!(
        payload
            .clone()
            .into_read_view(4..4)
            .unwrap()
            .as_ref()
            .is_empty()
    );
    assert!(
        payload
            .clone()
            .into_read_view(std::ops::Range { start: 2, end: 1 })
            .is_none()
    );
    assert!(payload.clone().into_read_view(0..5).is_none());
    drop(payload);
    assert_eq!(view.as_ref(), b"456");
}

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

fn fixture(length: usize) -> Vec<u8> {
    let mut state = 0x1234_5678_9012_abcd_u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

#[test]
fn append_writer_matches_zeroed_image_for_all_record_plans_and_verifies() {
    let base = fixture(65_536);
    let first = vec![0x36; 16_384];
    let second = vec![0x47; 16_384];
    let chunks = [
        PrehashedChunk::new(ChunkId::of(&first), &first),
        PrehashedChunk::new(ChunkId::of(&second), &second),
    ];
    let decoded = [first.as_slice(), second.as_slice()].concat();
    let compressed = compress_zstd_v1(&decoded, ZSTD_LEVEL_V1).unwrap();
    let mut prefix_target = base.clone();
    prefix_target[0] ^= 1;
    let prefix_encoded = ZstdPrefixRecord::encode(&base, &prefix_target).unwrap();
    let frame_length = usize::try_from(get_u32(&prefix_encoded, 44)).unwrap();
    let prefix = ZstdPrefixRecord::prepare_precompressed(
        ChunkId::of(&base),
        65_536,
        ChunkId::of(&prefix_target),
        prefix_encoded[RAW_PAYLOAD_OFFSET..RAW_PAYLOAD_OFFSET + frame_length].into(),
    )
    .unwrap();
    let mut sparse_target = base.clone();
    sparse_target[11] ^= 3;
    let sparse = PreparedSparseXorRecord {
        dependency: DependentDependency {
            chunk_id: ChunkId::of(&base),
            logical_length: 65_536,
        },
        target_id: ChunkId::of(&sparse_target),
        logical_length: 65_536,
        runs: vec![SparseXorRun::new(11, 1)].into_boxed_slice(),
        xor_bytes: vec![3].into_boxed_slice(),
    };
    let independent_bytes = vec![0x89; 32_768];
    let independent = SealedContainer::prepare_prehashed_independent_record(
        PrehashedChunk::new(ChunkId::of(&independent_bytes), &independent_bytes),
        IncompressibilityGatePolicy::Off,
    )
    .unwrap();
    let transplant = vec![0x9a; 8192];
    let plans = vec![
        AdaptiveRecordPlan::Raw(PrehashedChunk::new(ChunkId::of(&base), &base)),
        AdaptiveRecordPlan::Zstd {
            chunks: &chunks,
            decoded_length: decoded.len(),
            payload: compressed,
            level: ZSTD_LEVEL_V1,
        },
        AdaptiveRecordPlan::PreparedIndependent(independent),
        AdaptiveRecordPlan::PreparedEncoded(PreparedEncodedRecord {
            bytes: RawRecord::encode(&transplant).unwrap(),
            chunk_count: 1,
        }),
        AdaptiveRecordPlan::Dependent(prefix.into()),
        AdaptiveRecordPlan::Dependent(sparse.into()),
    ];
    let id = ContainerId::new([0x71; 16]).unwrap();
    let expected =
        encode_container_from_adaptive_plans_zeroed(id, 1, plans.clone(), NonZeroUsize::MIN)
            .unwrap();
    assert_reordered_plans_match(&plans, id, &expected.bytes);
    let actual = encode_container_from_adaptive_plans(id, 1, plans, NonZeroUsize::MIN).unwrap();
    assert_eq!(actual.bytes, expected.bytes);
    assert_eq!(actual.bytes.as_ptr().addr() % HEADER_BYTES, 0);
    let verified =
        SealedContainer::decode_with_dependent_resolver(&actual.bytes, &mut |dependency| {
            assert_eq!(dependency.chunk_id(), ChunkId::of(&base));
            Ok(base.clone())
        })
        .unwrap();
    assert_eq!(verified.locations().len(), 7);
    let mut corrupted = actual.bytes.clone();
    corrupted[HEADER_BYTES + RAW_PAYLOAD_OFFSET] ^= 1;
    assert!(
        SealedContainer::decode_with_dependent_resolver(&corrupted, &mut |_| Ok(base.clone()))
            .is_err()
    );
}

fn assert_reordered_plans_match(
    plans: &[AdaptiveRecordPlan<'_>],
    id: ContainerId,
    expected: &[u8],
) {
    let order = plans
        .iter()
        .flat_map(|plan| (0..plan.chunk_count()).map(|ordinal| plan.chunk_id_at(ordinal)))
        .collect::<Vec<_>>();
    let mut reversed = plans.to_vec();
    reversed.reverse();
    order_adaptive_records(&mut reversed, Some(&order)).unwrap();
    let encoded = encode_container_from_adaptive_plans(id, 1, reversed, NonZeroUsize::MIN).unwrap();
    assert_eq!(encoded.bytes.as_ref(), expected);
    let mut within_record = order;
    within_record.swap(1, 2);
    assert!(order_adaptive_records(&mut plans.to_vec(), Some(&within_record)).is_err());
}

#[test]
fn record_order_preserves_full_identity_and_rejects_invalid_permutations() {
    let a = ChunkId::from_bytes([0; 32]);
    let mut bytes = [0; 32];
    bytes[31] = 1;
    let b = ChunkId::from_bytes(bytes);
    bytes[31] = 2;
    let unknown = ChunkId::from_bytes(bytes);
    assert_eq!(chunk_order_hash(a), chunk_order_hash(b));
    let payload = b"only the order is under test";
    let mut records = vec![
        AdaptiveRecordPlan::Raw(PrehashedChunk::new(b, payload)),
        AdaptiveRecordPlan::Raw(PrehashedChunk::new(a, payload)),
    ];
    order_adaptive_records(&mut records, Some(&[a, b])).unwrap();
    assert_eq!(records[0].chunk_id_at(0), a);
    assert_eq!(records[1].chunk_id_at(0), b);
    for invalid in [vec![a, a], vec![a], vec![a, unknown], vec![a, b, unknown]] {
        assert!(order_adaptive_records(&mut records.clone(), Some(&invalid)).is_err());
    }
}

#[test]
fn read_views_join_only_adjacent_verified_ranges_of_one_backing() {
    let backing = Arc::new(b"0123456789abcdef".to_vec());
    let first =
        VerifiedChunkPayload::from_shared(ChunkId::of(&backing[..8]), Arc::clone(&backing), 0, 8)
            .unwrap();
    let second =
        VerifiedChunkPayload::from_shared(ChunkId::of(&backing[8..]), Arc::clone(&backing), 8, 8)
            .unwrap();
    let separate = RawRecord::decode(&RawRecord::encode(b"89abcdef").unwrap())
        .unwrap()
        .into_verified_payload();
    assert_eq!(first.backing_id(), second.backing_id());
    assert_ne!(first.backing_id(), separate.backing_id());
    let mut view = first.read_view(3..8).unwrap();
    assert!(!view.try_append(&second, 1..8));
    assert!(!view.try_append(&separate, 0..8));
    assert!(!view.try_append(&second, 0..9));
    assert!(view.try_append(&second, 0..6));
    assert_eq!(view.as_ref(), b"3456789abcd");
    assert_eq!(view.as_ref().as_ptr(), backing[3..].as_ptr());
    assert!(!view.try_append(&first, 0..3));
    assert!(first.read_view(7..9).is_none());
    drop(first);
    drop(second);
    drop(backing);
    assert_eq!(view.as_ref(), b"3456789abcd");
}

#[test]
#[ignore = "manual release-mode adaptive Container assembly A/B"]
fn adaptive_append_and_zeroed_assembly_microbenchmark() {
    use std::hint::black_box;
    use std::time::Instant;
    let bytes = fixture(32 * 1024 * 1024);
    let plans = bytes
        .chunks(65_536)
        .map(|bytes| AdaptiveRecordPlan::Raw(PrehashedChunk::new(ChunkId::of(bytes), bytes)))
        .collect::<Vec<_>>();
    let id = ContainerId::new([0x73; 16]).unwrap();
    let mut samples = [Vec::new(), Vec::new()];
    for round in 0..11 {
        for side in 0..2 {
            let side = (side + round) % 2;
            let plans = plans.clone();
            let start = Instant::now();
            let image = if side == 0 {
                encode_container_from_adaptive_plans_zeroed(id, 1, plans, NonZeroUsize::MIN)
            } else {
                encode_container_from_adaptive_plans(id, 1, plans, NonZeroUsize::MIN)
            }
            .unwrap();
            black_box(&image);
            samples[side].push(start.elapsed());
        }
    }
    for samples in &mut samples {
        samples.sort_unstable();
    }
    println!(
        "adaptive_raw_assembly bytes={} zeroed_ms={:.3} append_ms={:.3} speedup={:.3}",
        bytes.len(),
        samples[0][5].as_secs_f64() * 1000.0,
        samples[1][5].as_secs_f64() * 1000.0,
        samples[0][5].as_secs_f64() / samples[1][5].as_secs_f64()
    );
}

#[test]
fn compact_record_provenance_keeps_every_candidate_coordinate() {
    let data = [vec![71; 16_384], vec![83; 16_384]];
    let refs = data.iter().map(Vec::as_slice).collect::<Vec<_>>();
    for compressed in [false, true] {
        let id = ContainerId::new([0x76; 16]).unwrap();
        let encoded = Arc::new(if compressed {
            SealedContainer::encode_zstd_regions(id, 1, &[&refs]).unwrap()
        } else {
            SealedContainer::encode(id, 1, &refs).unwrap()
        });
        let decoded = SealedContainer::decode(&encoded).unwrap();
        let descriptor = SealedContainerDescriptor::decode(
            &encoded[..HEADER_BYTES],
            &encoded[encoded.len() - 4096..],
            u64::try_from(encoded.len()).unwrap(),
        )
        .unwrap();
        let entries = decoded
            .locations()
            .iter()
            .copied()
            .map(|location| ExactIndexEntry::from_verified(location).unwrap())
            .collect::<Vec<_>>();
        for (ordinal, &candidate) in entries.iter().enumerate() {
            let range = descriptor.record_range(candidate).unwrap();
            let start = usize::try_from(range.offset()).unwrap();
            let read = descriptor
                .decode_owned_candidate_payloads(
                    &[candidate],
                    &encoded,
                    start..start + range.length(),
                )
                .unwrap();
            let payload = &read.requested()[0];
            assert_eq!(payload.as_slice(), data[ordinal]);
            assert!(payload.matches_independent_candidate(candidate));
            assert!(!payload.matches_independent_candidate(entries[1 - ordinal]));
            assert_changed_provenance_rejected(payload, candidate);
        }
    }
}

fn assert_changed_provenance_rejected(payload: &VerifiedChunkPayload, candidate: ExactIndexEntry) {
    for field in 0..8 {
        let mut wrong = payload.clone();
        let source = wrong.source.as_mut().unwrap();
        match field {
            0 => source.container_id = ContainerId::new([0x77; 16]).unwrap(),
            1 => source.container_generation += 1,
            2 => source.record_offset += 64,
            3 => source.record_length = NonZeroU32::new(source.record_length.get() + 64).unwrap(),
            4 => source.record_crc32c ^= 1,
            5 => source.record_decoded_length += 1,
            6 => source.record_payload_length += 1,
            7 => source.codec_id ^= 1,
            _ => unreachable!(),
        }
        assert!(!wrong.matches_independent_candidate(candidate));
    }
    let mut missing = payload.clone();
    missing.source = None;
    assert!(!missing.matches_independent_candidate(candidate));
    let mut wrong_ordinal = payload.clone();
    wrong_ordinal.chunk_ordinal += 1;
    assert!(!wrong_ordinal.matches_independent_candidate(candidate));
    let mut wrong_offset = payload.clone();
    wrong_offset.decoded_offset += 1;
    assert!(!wrong_offset.matches_independent_candidate(candidate));
}
