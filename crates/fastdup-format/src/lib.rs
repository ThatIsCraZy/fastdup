#![forbid(unsafe_code)]

//! Explicit, versioned serialization for fastdup durable objects.

mod commit;
mod container;
mod exact_index;
mod exact_index_activation;
mod exact_index_run_set;
mod manifest;
mod metadata;
mod namespace;

pub use commit::{
    COMMIT_RECORD_BYTES, CommitFormatError, CommitRecord, CommitRecordHash, PolicySetId,
};
pub use container::{
    BuildingContainerHeader, ChunkId, ContainerHeader, ContainerId, ContainerLayout, FormatError,
    HEADER_BYTES, MAX_CONTAINER_BYTES, MAX_LOGICAL_CHUNK_BYTES, RECORD_HEADER_BYTES, RawRecord,
    SealedContainer, VerifiedRawLocation,
};
pub use exact_index::{
    EXACT_INDEX_ENTRY_BYTES, EXACT_INDEX_HEADER_BYTES, EXACT_INDEX_PAGE_BYTES, ExactIndexEntry,
    ExactIndexFormatError, ExactIndexLocation, ExactIndexPage, ExactIndexPagePosition,
    ExactIndexProfileId, ExactIndexRun, ExactIndexRunDescriptor, ExactIndexRunHashAudit,
    ExactLocationTransition,
};
pub use exact_index_activation::{
    EXACT_INDEX_ACTIVATION_RECORD_BYTES, ExactIndexActivationError, ExactIndexActivationHash,
    ExactIndexActivationRecord,
};
pub use exact_index_run_set::{
    ExactIndexRunRef, ExactIndexRunSet, ExactIndexRunSetError, ExactIndexRunSetId,
};
pub use manifest::{MANIFEST_HEADER_BYTES, ManifestExtent, ManifestLeaf};
pub use metadata::{
    MAX_METADATA_OBJECT_BYTES, METADATA_HEADER_BYTES, MetadataFormatError, MetadataObjectId,
};
pub use namespace::{DurableInode, NAMESPACE_ROOT_HEADER_BYTES, NamespaceEntry, NamespaceRoot};
