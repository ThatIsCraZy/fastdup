use fastdup_format::{
    ContainerId, ExactIndexEntry, HEADER_BYTES, SealedContainer, SealedContainerDescriptor,
};
use std::sync::Arc;

#[test]
fn owned_raw_views_preserve_buffer_ownership_and_verified_location_coordinates() {
    let chunks = [vec![31; 16384], vec![79; 16384]];
    let image = Arc::new(
        SealedContainer::encode(
            ContainerId::new([25; 16]).unwrap(),
            1,
            &[&chunks[0], &chunks[1]],
        )
        .unwrap(),
    );
    let decoded = SealedContainer::decode(&image).unwrap();
    let descriptor = SealedContainerDescriptor::decode(
        &image[..HEADER_BYTES],
        &image[image.len() - 4096..],
        u64::try_from(image.len()).unwrap(),
    )
    .unwrap();
    let entries = decoded
        .locations()
        .iter()
        .copied()
        .map(ExactIndexEntry::from_verified)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut views = Vec::new();
    for (ordinal, entry) in entries.iter().copied().enumerate() {
        let range = descriptor.record_range(entry).unwrap();
        let start = usize::try_from(range.offset()).unwrap();
        let result = descriptor
            .decode_owned_candidate_payloads(&[entry], &image, start..start + range.length())
            .unwrap();
        let view = result.requested()[0].clone();
        assert_eq!(view.as_slice().as_ptr(), image[start + 192..].as_ptr());
        assert_eq!(view.as_slice(), chunks[ordinal]);
        assert_eq!(view.decoded_offset(), 0);
        assert!(view.matches_independent_candidate(entry));
        assert!(!view.matches_independent_candidate(entries[1 - ordinal]));
        let mut corrupt = image.as_ref().clone();
        corrupt[start + 192] ^= 1;
        assert!(
            descriptor
                .decode_owned_candidate_payloads(
                    &[entry],
                    &Arc::new(corrupt),
                    start..start + range.length()
                )
                .is_err()
        );
        views.push(view);
    }
    // The same Chunk ID at a different physical Location is not cache proof
    // for a newly selected independent Base.
    let other_image =
        SealedContainer::encode(ContainerId::new([26; 16]).unwrap(), 2, &[&chunks[0]]).unwrap();
    let other = SealedContainer::decode(&other_image).unwrap();
    let relocated = ExactIndexEntry::from_verified(other.locations()[0]).unwrap();
    assert_eq!(relocated.chunk_id(), entries[0].chunk_id());
    assert!(!views[0].matches_independent_candidate(relocated));
    assert!(views[0].shares_backing_with(&views[1]));
    assert_eq!(views[0].backing_allocation_bytes(), image.capacity());
    drop(image);
    assert_eq!(views[1].as_slice(), chunks[1]);
}
