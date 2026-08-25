#![forbid(unsafe_code)]

//! Explicit, versioned serialization for fastdup durable objects.

mod commit;
mod container;
mod exact_index;
mod exact_index_activation;
mod exact_index_run_set;
mod manifest;
mod manifest_inner;
mod metadata;
mod namespace;

fn crc32c_with_zeroed_u32(bytes: &[u8], field_offset: usize) -> u32 {
    let field_end = field_offset
        .checked_add(4)
        .expect("ASSERT: a four-byte checksum offset cannot overflow");
    assert!(
        field_end <= bytes.len(),
        "ASSERT: checksum field lies inside the encoded object"
    );
    let before = crc32c::crc32c(&bytes[..field_offset]);
    let with_zero = crc32c::crc32c_append(before, &[0_u8; 4]);
    crc32c::crc32c_append(with_zero, &bytes[field_end..])
}

pub use commit::{
    COMMIT_RECORD_BYTES, CommitFormatError, CommitRecord, CommitRecordHash, PolicySetId,
};
pub use container::{
    AdaptiveContainerEncoding, BuildingContainerHeader, ChunkId, ContainerHeader, ContainerId,
    ContainerLayout, ContainerRecordRange, FOOTER_BYTES, FormatError, HEADER_BYTES,
    IncompressibilityGateMetrics, IncompressibilityGatePolicy, MAX_CONTAINER_BYTES,
    MAX_LOGICAL_CHUNK_BYTES, PrehashedAdaptiveRegion, PrehashedChunk, PrehashedContiguousRegion,
    RECORD_HEADER_BYTES, RawRecord, SealedContainer, SealedContainerDescriptor,
    VerifiedChunkLocation, VerifiedContainerPublication, VerifiedRawLocation,
};
pub use exact_index::{
    EXACT_INDEX_ENTRY_BYTES, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES, ExactIndexEntry,
    ExactIndexFormatError, ExactIndexLocation, ExactIndexPage, ExactIndexPagePosition,
    ExactIndexProfileId, ExactIndexRun, ExactIndexRunDescriptor, ExactIndexRunHashAudit,
    ExactIndexRunStreamEncoder, ExactLocationTransition,
};
pub use exact_index_activation::{
    EXACT_INDEX_ACTIVATION_RECORD_BYTES, ExactIndexActivationError, ExactIndexActivationHash,
    ExactIndexActivationRecord,
};
pub use exact_index_run_set::{
    ExactIndexRunRef, ExactIndexRunSet, ExactIndexRunSetError, ExactIndexRunSetId,
};
pub use manifest::{MANIFEST_HEADER_BYTES, ManifestExtent, ManifestLeaf};
pub use manifest_inner::{
    MANIFEST_CHILD_RANGE_BYTES, MANIFEST_INNER_HEADER_BYTES, ManifestChildRange, ManifestInnerNode,
    ManifestInnerNodeError,
};
pub use metadata::{
    MAX_METADATA_OBJECT_BYTES, METADATA_HEADER_BYTES, MetadataFormatError, MetadataObjectId,
    MetadataObjectKind, metadata_object_kind,
};
pub use namespace::{
    DurableInode, DurableInodeKind, DurableRootMetadata, DurableTimes, DurableTimestamp,
    DurableXattr, NAMESPACE_ROOT_HEADER_BYTES, NamespaceEntry, NamespaceRoot,
};

#[cfg(test)]
mod checksum_tests {
    use super::crc32c_with_zeroed_u32;

    #[test]
    fn segmented_zero_field_checksum_matches_independent_copy_oracle() {
        for length in [4_usize, 5, 64, 4_096, 65_537] {
            let bytes = (0..length)
                .map(|offset| u8::try_from(offset % 251).expect("fixture byte fits u8"))
                .collect::<Vec<_>>();
            for offset in [0, (length - 4) / 2, length - 4] {
                let mut oracle = bytes.clone();
                oracle[offset..offset + 4].fill(0);
                assert_eq!(
                    crc32c_with_zeroed_u32(&bytes, offset),
                    crc32c::crc32c(&oracle)
                );
            }
        }
    }
}
