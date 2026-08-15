#![forbid(unsafe_code)]

//! Explicit, versioned serialization for fastdup durable objects.

mod commit;
mod container;
mod manifest;
mod metadata;
mod namespace;

pub use commit::{
    COMMIT_RECORD_BYTES, CommitFormatError, CommitRecord, CommitRecordHash, PolicySetId,
};
pub use container::{
    BuildingContainerHeader, ChunkId, ContainerHeader, ContainerId, ContainerLayout, FormatError,
    HEADER_BYTES, MAX_CONTAINER_BYTES, MAX_LOGICAL_CHUNK_BYTES, RECORD_HEADER_BYTES, RawRecord,
    SealedContainer,
};
pub use manifest::{MANIFEST_HEADER_BYTES, ManifestExtent, ManifestLeaf};
pub use metadata::{
    MAX_METADATA_OBJECT_BYTES, METADATA_HEADER_BYTES, MetadataFormatError, MetadataObjectId,
};
pub use namespace::{DurableInode, NAMESPACE_ROOT_HEADER_BYTES, NamespaceEntry, NamespaceRoot};
