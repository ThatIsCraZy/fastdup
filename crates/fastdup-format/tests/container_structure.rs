use fastdup_format::{ContainerId, ContainerStructure, FormatError, HEADER_BYTES, SealedContainer};

fn fixture() -> Vec<u8> {
    let payload = vec![0x73; 256 * 1024];
    SealedContainer::encode(ContainerId::new([19; 16]).unwrap(), 7, &[&payload]).unwrap()
}

fn structure(image: &[u8]) -> Result<ContainerStructure, FormatError> {
    ContainerStructure::read(
        &image[..HEADER_BYTES],
        &image[image.len() - 4096..],
        image.len() as u64,
        |offset, length| Ok(image[offset as usize..offset as usize + length].to_vec()),
    )
}

#[test]
fn complete_structure_requires_no_payload_read() {
    let image = fixture();
    let payload_start = HEADER_BYTES + 192;
    let payload_end = payload_start + 256 * 1024;
    let mut read_bytes = 8192;
    let checked = ContainerStructure::read(
        &image[..HEADER_BYTES],
        &image[image.len() - 4096..],
        image.len() as u64,
        |offset, length| {
            let start = offset as usize;
            assert!(
                start + length <= payload_start || start >= payload_end,
                "payload read during startup"
            );
            read_bytes += length;
            Ok::<_, FormatError>(image[start..start + length].to_vec())
        },
    )
    .unwrap();
    assert_eq!(checked.chunks().len(), 1);
    assert!(read_bytes < 16 * 1024);
}

#[test]
fn payload_corruption_is_deferred_but_full_verification_rejects_it() {
    let mut image = fixture();
    image[HEADER_BYTES + 192 + 1024] ^= 1;
    assert!(structure(&image).is_ok());
    assert!(SealedContainer::decode(&image).is_err());
}

#[test]
fn corruption_at_each_structural_boundary_fails_closed() {
    let image = fixture();
    let index = structure(&image)
        .unwrap()
        .descriptor()
        .layout()
        .index_offset as usize;
    for offset in [
        40,
        HEADER_BYTES + 12,
        HEADER_BYTES + 128,
        index + 80,
        image.len() - 4096 + 96,
    ] {
        let mut corrupted = image.clone();
        corrupted[offset] ^= 1;
        assert!(
            structure(&corrupted).is_err(),
            "accepted corruption at {offset}"
        );
    }
}

#[test]
fn short_backend_reads_are_errors_not_panics() {
    let image = fixture();
    assert!(
        ContainerStructure::read(
            &image[..HEADER_BYTES],
            &image[image.len() - 4096..],
            image.len() as u64,
            |_, _| Ok::<_, FormatError>(vec![0; 3])
        )
        .is_err()
    );
}

#[test]
fn structure_and_full_decoders_agree_for_all_compressed_codecs() {
    let base = vec![b'A'; 65536];
    let mut target = base.clone();
    target[177] = b'B';
    let id = ContainerId::new([21; 16]).unwrap();
    let region = [base.as_slice(), target.as_slice()];
    let images = [
        SealedContainer::encode_zstd_regions(id, 1, &[&region]).unwrap(),
        SealedContainer::encode_zstd_prefix_pairs(id, 2, &[(&base, &target)])
            .unwrap()
            .into_bytes(),
        SealedContainer::encode_sparse_xor_pairs(id, 3, &[(&base, &target)])
            .unwrap()
            .into_bytes(),
    ];
    for image in images {
        let checked = structure(&image).unwrap();
        let full =
            SealedContainer::decode_with_dependent_resolver(&image, &mut |_| Ok(base.clone()))
                .unwrap();
        assert_eq!(checked.chunks().len(), full.records().len());
        for chunk in checked.chunks() {
            assert_eq!(
                full.chunk(chunk.chunk_id).unwrap().len(),
                chunk.logical_length as usize
            );
        }
    }
}
