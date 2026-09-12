use super::super::{HEADER_BYTES, RawRecord, SealedContainer, SealedContainerDescriptor};
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
            assert_eq!(
                ExactIndexEntry::from_verified(payload.verified_location().unwrap()).unwrap(),
                candidate
            );
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
