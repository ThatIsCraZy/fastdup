//! Cross-boundary fault oracles adapted from the `OpenZFS` checksum, fault, and
//! recovery suites. Durable-format offsets below only construct authenticated
//! corrupt fixtures; every behavioral assertion uses public mount/read/scrub seams.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use fastdup_appliance::{DurableNamespace, recover_mount};
use fastdup_format::{
    ChunkId, ContainerId, DurableInode, FormatError, HEADER_BYTES, ManifestExtent, ManifestLeaf,
    NamespaceEntry, NamespaceRoot, PolicySetId, SealedContainer,
};
use fastdup_posix::{
    NamespaceConfig, OpenOptions, Operation, PosixError, ROOT_INODE, Reply, RequestContext,
};
use fastdup_store::{ContainerRepository, GenerationRepository, StorageIo};
use fastdup_testkit::{MemoryStorageIo, PausedStorageIo, StorageOperation};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 42,
};

const RECORD_CRC_OFFSET: usize = 60;
const INDEX_HEADER_BYTES: usize = 64;
const INDEX_ENTRY_CRC_OFFSET: usize = 60;
const INDEX_CRC_OFFSET: usize = 36;
const FOOTER_HASH_OFFSET: usize = 96;
const FOOTER_CRC_OFFSET: usize = 128;
const CONTAINER_COMMITMENT_DOMAIN_V1: &[u8] = b"fastdup-container-structural-v1\0";

#[derive(Clone, Copy, Debug)]
enum MetadataFault {
    Corrupt,
    Truncate,
    Remove,
}

#[test]
#[allow(clippy::too_many_lines)]
fn authenticated_zstd_decode_failure_returns_no_posix_bytes_and_fails_offline_scrub() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let policy = PolicySetId::new([0xD0; 32]).expect("policy identity is nonzero");
    let container_id = ContainerId::new([0xD1; 16]).expect("Container identity is nonzero");
    let container_name = format!("{}.fdc", "d1".repeat(16));
    let payload = b"OpenZFS decompression fault oracle ".repeat(4_096);

    let containers = ContainerRepository::new(data.clone());
    let container_generation = containers
        .open_generation_allocator(1_024)
        .expect("initialize the durable Container generation allocator")
        .reserve_generation()
        .expect("reserve the first Container generation");
    let region = [payload.as_slice()];
    containers
        .publish_adaptive_regions(container_id, container_generation, &[&region])
        .expect("publish one dependency-free compressed record");
    let published = containers
        .read(container_id)
        .expect("writer output independently verifies before fault injection");
    assert_eq!(published.zstd_record_count(), 1);
    assert_eq!(published.raw_record_count(), 0);

    let generations = GenerationRepository::new(metadata.clone(), policy);
    generations
        .commit_namespace(&reservation_root())
        .expect("reserve Inode identities before visibility");
    let logical_size = u64::try_from(payload.len()).expect("fixture length fits u64");
    let manifest = ManifestLeaf::new(
        logical_size,
        vec![ManifestExtent::Data {
            logical_length: logical_size,
            chunk_id: ChunkId::of(&payload),
        }],
    )
    .expect("construct the DATA Manifest");
    let manifest_root = generations
        .publish_manifest(&manifest)
        .expect("publish the immutable Manifest");
    generations
        .commit_namespace_with_data(&visible_root(manifest_root, logical_size), &containers)
        .expect("commit the complete DATA-bearing generation");

    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        generations.clone(),
        containers.clone(),
        1_024,
    )
    .expect("mount the healthy committed generation before injecting corruption");
    let Reply::Entry(entry) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"compressed",
            },
        )
        .expect("look up the committed file")
    else {
        panic!("ASSERT: lookup returned the wrong reply variant");
    };
    let Reply::Opened(handle) = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open the committed file")
    else {
        panic!("ASSERT: open returned the wrong reply variant");
    };

    let mut corrupted = data
        .read(&container_name)
        .expect("read the published Container image for physical fault injection");
    corrupt_zstd_frame_and_reauthenticate_structure(&mut corrupted);
    assert_eq!(
        SealedContainer::decode(&corrupted),
        Err(FormatError::ZstdFailure),
        "the injected image must reach the decoder rather than fail an earlier checksum"
    );
    data.write_at(&container_name, 0, &corrupted)
        .expect("inject the authenticated compressed-frame fault");
    data.sync_file(&container_name)
        .expect("make the injected physical fault durable");
    data.crash();

    let cache_before = appliance.verified_read_cache_status();
    assert_eq!(
        appliance.namespace().dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: u32::try_from(payload.len()).expect("fixture read fits u32"),
            },
        ),
        Err(PosixError::Io),
        "a compressed DATA fault must return EIO instead of partial or unchecked bytes"
    );
    let cache_after = appliance.verified_read_cache_status();
    assert_eq!(cache_after.admissions(), cache_before.admissions());
    assert_eq!(cache_after.entry_count(), cache_before.entry_count());
    assert!(
        generations.scrub_all_with_data(&containers).is_err(),
        "offline scrub must report the same corrupt physical object"
    );
}

#[test]
fn newest_namespace_metadata_fault_recovers_only_the_complete_previous_generation() {
    for fault in [
        MetadataFault::Corrupt,
        MetadataFault::Truncate,
        MetadataFault::Remove,
    ] {
        let metadata = MemoryStorageIo::new();
        let data = MemoryStorageIo::new();
        let policy = PolicySetId::new([0xE0; 32]).expect("policy identity is nonzero");
        let generations = GenerationRepository::new(metadata.clone(), policy);
        let containers = ContainerRepository::new(data);
        generations
            .commit_namespace(&reservation_root())
            .expect("reserve Inode identities before visibility");

        let first_size = 4_096;
        let first_manifest = generations
            .publish_manifest(
                &ManifestLeaf::new(
                    first_size,
                    vec![ManifestExtent::Fill {
                        logical_length: first_size,
                        value: 0x11,
                    }],
                )
                .expect("construct the previous complete Manifest"),
            )
            .expect("publish the previous complete Manifest");
        generations
            .commit_namespace(&versioned_root(first_manifest, first_size, 1))
            .expect("commit the previous complete Namespace generation");

        let newest_size = 8_192;
        let newest_manifest = generations
            .publish_manifest(
                &ManifestLeaf::new(
                    newest_size,
                    vec![ManifestExtent::Fill {
                        logical_length: newest_size,
                        value: 0x22,
                    }],
                )
                .expect("construct the newest complete Manifest"),
            )
            .expect("publish the newest complete Manifest");
        let newest_record = generations
            .commit_namespace(&versioned_root(newest_manifest, newest_size, 2))
            .expect("commit the newest complete Namespace generation");

        let newest_name = metadata_name(newest_record.namespace_root());
        match fault {
            MetadataFault::Corrupt => {
                let mut bytes = metadata
                    .read(&newest_name)
                    .expect("read the newest Namespace Root for fault injection");
                bytes[104] ^= 0x80;
                metadata
                    .write_at(&newest_name, 0, &bytes)
                    .expect("inject Namespace Root corruption");
                metadata
                    .sync_file(&newest_name)
                    .expect("make Namespace Root corruption durable");
            }
            MetadataFault::Truncate => {
                let length = metadata
                    .object_len(&newest_name)
                    .expect("read the newest Namespace Root length");
                metadata
                    .set_len(&newest_name, length / 2)
                    .expect("inject a torn Namespace Root");
                metadata
                    .sync_file(&newest_name)
                    .expect("make the torn Namespace Root durable");
            }
            MetadataFault::Remove => {
                metadata
                    .remove_file(&newest_name)
                    .expect("remove the newest Namespace Root");
                metadata
                    .sync_root()
                    .expect("make the missing Namespace Root durable");
            }
        }
        metadata.crash();

        let recovered = recover_mount(
            NamespaceConfig::default(),
            &GenerationRepository::new(metadata.clone(), policy),
            &containers,
        )
        .unwrap_or_else(|error| panic!("{fault:?}: recovery failed: {error}"))
        .unwrap_or_else(|| panic!("{fault:?}: previous generation was not recovered"));
        assert_recovered_fill(&recovered, first_size, 0x11, fault);
        assert!(
            generations.scrub_all_with_data(&containers).is_err(),
            "{fault:?}: scrub must not hide the corrupt newest retained generation"
        );
    }
}

#[test]
fn modeled_power_cut_with_torn_newest_container_recovers_previous_complete_generation() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let policy = PolicySetId::new([0xE7; 32]).expect("policy identity is nonzero");
    let appliance = DurableNamespace::open(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), policy),
        ContainerRepository::new(data.clone()),
        1_024,
    )
    .expect("open modeled power-cut fixture");
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"power-cut.bin",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create power-cut fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant")
    };
    // A Fill-backed previous generation has no Container dependency. The
    // fault target can therefore only invalidate the newer DATA-backed view,
    // making the fallback oracle independent of asynchronous Container
    // generation allocation order.
    let first = vec![0x41; 256 * 1_024];
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: &first,
            },
        )
        .expect("write previous complete generation");
    appliance
        .checkpoint()
        .expect("checkpoint previous generation")
        .expect("previous generation is nonempty");
    let previous_containers = data
        .list_names()
        .expect("list previous-generation DATA objects")
        .into_iter()
        .filter(|name| std::path::Path::new(name).extension() == Some(std::ffi::OsStr::new("fdc")))
        .collect::<BTreeSet<_>>();

    let second = pseudo_random_payload(0x92, first.len());
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: &second,
            },
        )
        .expect("write newest generation");
    appliance
        .checkpoint()
        .expect("checkpoint newest generation")
        .expect("newest generation is nonempty");
    drop(appliance);

    let newest = data
        .list_names()
        .expect("list DATA objects")
        .into_iter()
        .filter(|name| std::path::Path::new(name).extension() == Some(std::ffi::OsStr::new("fdc")))
        .filter(|name| !previous_containers.contains(name))
        .map(|name| {
            let bytes = data.read(&name).expect("read Container fixture");
            let generation = SealedContainer::decode(&bytes)
                .expect("healthy Container before power cut")
                .header()
                .container_generation();
            (generation, name, bytes.len())
        })
        .max_by_key(|(generation, _, _)| *generation)
        .expect("newest generation publishes a new DATA Container");
    data.inject_durable_torn_write(&newest.1, newest.2 / 2)
        .expect("inject half-written durable Container image");
    metadata.crash();
    data.crash();

    let generations = GenerationRepository::new(metadata.clone(), policy);
    let containers = ContainerRepository::new(data.clone());
    let recovered = recover_mount(NamespaceConfig::default(), &generations, &containers)
        .expect("power-cut recovery selects a complete generation")
        .expect("previous complete generation remains available");
    assert_recovered_bytes(&recovered, b"power-cut.bin", &first);
    assert!(
        generations.scrub_all_with_data(&containers).is_err(),
        "scrub must retain evidence of the torn newest Container"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn resumed_data_sync_recovers_one_byte_exact_generation_after_remount() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let paused_data =
        PausedStorageIo::disarmed_before_name_prefix(data.clone(), StorageOperation::SyncFile, ".");
    let policy = PolicySetId::new([0xF0; 32]).expect("policy identity is nonzero");
    let appliance = Arc::new(
        DurableNamespace::open(
            NamespaceConfig::default(),
            GenerationRepository::new(metadata.clone(), policy),
            ContainerRepository::new(paused_data.clone()),
            1_024,
        )
        .expect("open the DATA-resume fixture"),
    );
    let payload = b"resumed DATA survives remount";
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"resumed",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create the DATA-resume fixture")
    else {
        panic!("ASSERT: create returned the wrong reply variant");
    };
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: payload,
            },
        )
        .expect("acknowledge the DATA-resume payload");

    paused_data.arm();
    let checkpointing = Arc::clone(&appliance);
    let checkpoint = std::thread::spawn(move || checkpointing.checkpoint());
    assert!(
        paused_data.wait_until_reached(Duration::from_secs(5)),
        "checkpoint must reach the selected DATA sync"
    );
    assert_eq!(
        appliance.namespace().dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: u32::try_from(payload.len()).expect("fixture read fits u32"),
            },
        ),
        Ok(Reply::Data(payload.to_vec())),
        "accepted bytes remain live while durable DATA is suspended"
    );
    paused_data.resume();
    let committed = checkpoint
        .join()
        .expect("checkpoint worker remains healthy")
        .expect("resumed storage completes the checkpoint")
        .expect("the dirty generation commits exactly once");
    assert_eq!(committed.generation(), 2);

    drop(appliance);
    metadata.crash();
    data.crash();
    let recovered = recover_mount(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, policy),
        &ContainerRepository::new(data),
    )
    .expect("reopen after the resumed DATA sync")
    .expect("the resumed generation is recoverable");
    let Reply::Entry(recovered_entry) = recovered
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"resumed",
            },
        )
        .expect("look up the resumed file after remount")
    else {
        panic!("ASSERT: recovered lookup returned the wrong reply variant");
    };
    let Reply::Opened(recovered_handle) = recovered
        .dispatch(
            CALLER,
            Operation::Open {
                inode: recovered_entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open the resumed file after remount")
    else {
        panic!("ASSERT: recovered open returned the wrong reply variant");
    };
    assert_eq!(
        recovered.dispatch(
            CALLER,
            Operation::Read {
                inode: recovered_entry.attr.inode,
                handle: recovered_handle,
                offset: 0,
                length: u32::try_from(payload.len()).expect("fixture read fits u32"),
            },
        ),
        Ok(Reply::Data(payload.to_vec()))
    );
}

fn reservation_root() -> NamespaceRoot {
    NamespaceRoot::new(1_024, 2, 0, Vec::new(), Vec::new())
        .expect("empty reservation root is valid")
}

fn visible_root(
    manifest_root: fastdup_format::MetadataObjectId,
    logical_size: u64,
) -> NamespaceRoot {
    NamespaceRoot::new(
        1_024,
        3,
        1,
        vec![
            DurableInode::new(
                2,
                0o640,
                CALLER.uid,
                CALLER.gid,
                1,
                1,
                logical_size,
                manifest_root,
            )
            .expect("durable file inode is valid"),
        ],
        vec![
            NamespaceEntry::new(ROOT_INODE.get(), 2, b"compressed".to_vec())
                .expect("durable file name is valid"),
        ],
    )
    .expect("visible Namespace Root is valid")
}

fn versioned_root(
    manifest_root: fastdup_format::MetadataObjectId,
    logical_size: u64,
    mutation_sequence: u64,
) -> NamespaceRoot {
    NamespaceRoot::new(
        1_024,
        3,
        mutation_sequence,
        vec![
            DurableInode::new(
                2,
                0o640,
                CALLER.uid,
                CALLER.gid,
                1,
                mutation_sequence,
                logical_size,
                manifest_root,
            )
            .expect("durable versioned inode is valid"),
        ],
        vec![
            NamespaceEntry::new(ROOT_INODE.get(), 2, b"state".to_vec())
                .expect("durable versioned name is valid"),
        ],
    )
    .expect("versioned Namespace Root is valid")
}

fn assert_recovered_fill(
    namespace: &fastdup_posix::Namespace,
    logical_size: u64,
    value: u8,
    fault: MetadataFault,
) {
    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name: b"state",
            },
        )
        .unwrap_or_else(|error| panic!("{fault:?}: lookup failed: {error:?}"))
    else {
        panic!("{fault:?}: lookup returned the wrong reply variant");
    };
    assert_eq!(entry.attr.size, logical_size, "{fault:?}");
    assert_eq!(entry.attr.mutation_sequence, 1, "{fault:?}");
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .unwrap_or_else(|error| panic!("{fault:?}: open failed: {error:?}"))
    else {
        panic!("{fault:?}: open returned the wrong reply variant");
    };
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: u32::try_from(logical_size).expect("fixture read fits u32"),
            },
        ),
        Ok(Reply::Data(vec![
            value;
            usize::try_from(logical_size)
                .expect("fixture length fits usize")
        ])),
        "{fault:?}: recovery exposed a mixed or newest generation"
    );
}

fn pseudo_random_payload(seed: u8, length: usize) -> Vec<u8> {
    let mut state = u64::from(seed) | 1;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn assert_recovered_bytes(namespace: &fastdup_posix::Namespace, name: &[u8], expected: &[u8]) {
    let Reply::Entry(entry) = namespace
        .dispatch(
            CALLER,
            Operation::Lookup {
                parent: ROOT_INODE,
                name,
            },
        )
        .expect("look up recovered power-cut file")
    else {
        panic!("ASSERT: recovered lookup returns an entry")
    };
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: entry.attr.inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open recovered power-cut file")
    else {
        panic!("ASSERT: recovered open returns a handle")
    };
    assert_eq!(
        namespace.dispatch(
            CALLER,
            Operation::Read {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                length: u32::try_from(expected.len()).expect("fixture read fits u32"),
            },
        ),
        Ok(Reply::Data(expected.to_vec()))
    );
}

fn metadata_name(object_id: fastdup_format::MetadataObjectId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(68);
    for byte in object_id.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded.push_str(".fdm");
    encoded
}

fn corrupt_zstd_frame_and_reauthenticate_structure(image: &mut [u8]) {
    let record_start = HEADER_BYTES;
    let record_length = get_u32(image, record_start + 32) as usize;
    let record_end = record_start + record_length;
    let payload_offset = get_u32(image, record_start + 40) as usize;
    let payload_length = get_u32(image, record_start + 44) as usize;
    let payload_start = record_start + payload_offset;
    let payload_end = payload_start + payload_length;
    assert!(payload_start < payload_end && payload_end <= record_end);
    image[payload_start..payload_end].fill(0);

    let record_crc = reauthenticate_crc(&mut image[record_start..record_end], RECORD_CRC_OFFSET);
    let index_offset =
        usize::try_from(get_u64(image, 72)).expect("fixture Index offset fits usize");
    let index_length =
        usize::try_from(get_u64(image, 80)).expect("fixture Index length fits usize");
    let index_end = index_offset + index_length;
    let index_entry_crc = index_offset + INDEX_HEADER_BYTES + INDEX_ENTRY_CRC_OFFSET;
    image[index_entry_crc..index_entry_crc + 4].copy_from_slice(&record_crc.to_le_bytes());
    reauthenticate_crc(&mut image[index_offset..index_end], INDEX_CRC_OFFSET);

    let footer_offset =
        usize::try_from(get_u64(image, 88)).expect("fixture Footer offset fits usize");
    image[footer_offset + FOOTER_HASH_OFFSET..footer_offset + FOOTER_CRC_OFFSET].fill(0);
    let commitment = structural_commitment(image, index_offset, index_end, footer_offset);
    image[footer_offset + FOOTER_HASH_OFFSET..footer_offset + FOOTER_HASH_OFFSET + 32]
        .copy_from_slice(commitment.as_bytes());
    reauthenticate_crc(&mut image[footer_offset..], FOOTER_CRC_OFFSET);
}

fn structural_commitment(
    image: &[u8],
    index_offset: usize,
    index_end: usize,
    footer_offset: usize,
) -> blake3::Hash {
    let record_start = HEADER_BYTES;
    let chunk_count = get_u32(image, record_start + 56) as usize;
    let chunk_table_end = record_start + 128 + chunk_count * 64;
    let mut hasher = blake3::Hasher::new();
    hasher.update(CONTAINER_COMMITMENT_DOMAIN_V1);
    hasher.update(&image[..HEADER_BYTES]);
    hasher.update(&image[record_start..chunk_table_end]);
    hasher.update(&image[index_offset..index_end]);
    hasher.update(&image[footer_offset..footer_offset + FOOTER_HASH_OFFSET]);
    hasher.update(&[0; 36]);
    hasher.update(&image[footer_offset + FOOTER_CRC_OFFSET + 4..]);
    hasher.finalize()
}

fn reauthenticate_crc(bytes: &mut [u8], field_offset: usize) -> u32 {
    bytes[field_offset..field_offset + 4].fill(0);
    let checksum = crc32c::crc32c(bytes);
    bytes[field_offset..field_offset + 4].copy_from_slice(&checksum.to_le_bytes());
    checksum
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("worked fixture field is four bytes"),
    )
}

fn get_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("worked fixture field is eight bytes"),
    )
}
