#![forbid(unsafe_code)]

//! Explicit, versioned serialization for fastdup durable objects.

#[cfg(not(target_arch = "x86_64"))]
compile_error!("fastdup supports only 64-bit x86 (x86-64) targets; see ADR 0078");

mod commit;
mod container;
mod container_generation_high_water;
mod exact_index;
mod exact_index_activation;
mod exact_index_run_set;
mod gc_candidate_catalog;
mod manifest;
mod manifest_inner;
mod metadata;
mod metadata_mark_catalog;
mod namespace;
mod similarity_index;
mod similarity_index_family;

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
    ContainerIntrinsicSummary, ContainerLayout, ContainerRecordRange, ContainerRecoveryEnvelope,
    FOOTER_BYTES, FormatError, HEADER_BYTES, IncompressibilityGateMetrics,
    IncompressibilityGatePolicy, MAX_CONTAINER_BYTES, MAX_LOGICAL_CHUNK_BYTES,
    PrehashedAdaptiveRegion, PrehashedChunk, PrehashedContiguousRegion, PreparedIndependentRecord,
    PreparedZstdPrefixRecord, RECORD_HEADER_BYTES, RawRecord, RecoveryIndexCandidate,
    SealedContainer, SealedContainerDescriptor, VerifiedChunkLocation,
    VerifiedContainerPublication, VerifiedRawLocation, VerifiedRecoveryIndex, ZstdPrefixDependency,
    ZstdPrefixRecord,
};
pub use container_generation_high_water::{
    CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES, ContainerGenerationHighWaterFormatError,
    ContainerGenerationHighWaterHash, ContainerGenerationHighWaterRecord,
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
pub use gc_candidate_catalog::{
    GC_CANDIDATE_CATALOG_HEADER_BYTES, GC_CANDIDATE_CATALOG_ROW_BYTES, GcCandidateCatalog,
    GcCandidateCatalogAudit, GcCandidateCatalogDescriptor, GcCandidateCatalogError,
    GcCandidateCatalogRow, GcCandidateCatalogStreamEncoder, GcCandidateLivenessEstimate,
    GcCandidateLocationState, GcDependencyEstimate, GcRecordLivenessEstimate,
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
pub use metadata_mark_catalog::{
    METADATA_MARK_CATALOG_HEADER_BYTES, METADATA_MARK_CATALOG_ROW_BYTES, MetadataMarkCatalogAudit,
    MetadataMarkCatalogDescriptor, MetadataMarkCatalogError, MetadataMarkCatalogRunKind,
    MetadataMarkCatalogStreamEncoder, metadata_mark_commit_binding,
};
pub use namespace::{
    DurableInode, DurableInodeKind, DurableRootMetadata, DurableTimes, DurableTimestamp,
    DurableXattr, NAMESPACE_ROOT_HEADER_BYTES, NamespaceEntry, NamespaceRoot,
};
pub use similarity_index::{
    SIMILARITY_BUCKET_REFERENCE_BYTES, SIMILARITY_BUCKET_REFERENCES_PER_PAGE,
    SIMILARITY_INDEX_ENTRIES_PER_PAGE, SIMILARITY_INDEX_ENTRY_BYTES, SIMILARITY_INDEX_HEADER_BYTES,
    SIMILARITY_INDEX_PAGE_BYTES, SimilarityBucketKey, SimilarityBucketPage,
    SimilarityBucketReference, SimilarityIndexEntry, SimilarityIndexFormatError,
    SimilarityIndexPage, SimilarityIndexRun, SimilarityIndexRunDescriptor,
    SimilarityIndexRunHashAudit, SimilarityIndexRunLayout, SimilarityIndexRunStreamEncoder,
};
pub use similarity_index_family::{
    SIMILARITY_FAMILY_HEADER_BYTES, SIMILARITY_FAMILY_PARTITION_BYTES, SimilarityIndexFamilyError,
    SimilarityIndexPartitionRef, SimilarityIndexRunFamily,
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
