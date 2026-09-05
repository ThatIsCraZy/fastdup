use super::*;

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
