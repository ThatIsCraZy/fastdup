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

pub use commit::{
    COMMIT_RECORD_BYTES, CommitFormatError, CommitRecord, CommitRecordHash, PolicySetId,
};
pub use container::{
    BuildingContainerHeader, ChunkId, ContainerHeader, ContainerId, ContainerLayout,
    ContainerRecordRange, FOOTER_BYTES, FormatError, HEADER_BYTES, MAX_CONTAINER_BYTES,
    MAX_LOGICAL_CHUNK_BYTES, PrehashedChunk, RECORD_HEADER_BYTES, RawRecord, SealedContainer,
    SealedContainerDescriptor, VerifiedChunkLocation, VerifiedRawLocation,
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
pub use namespace::{DurableInode, NAMESPACE_ROOT_HEADER_BYTES, NamespaceEntry, NamespaceRoot};
