//! Acceptance sweep for the SMB SingleStream workload.
//!
//! The shape follows `docs/benchmarks/smb-v0.6.1-2026-09-06.md`: three serial
//! uploads of the same image into a fresh repository, each one a single handle
//! written strictly sequentially in SMB-sized blocks and closed with a flush and
//! a checkpoint. What the benchmark measures as throughput and reduction is
//! asserted here as durable behaviour instead of timing.
use fastdup_appliance::{DurableNamespace, checkpoint_policy_set, recover_mount_with_index};
use fastdup_posix::{
    HandleId, InodeId, Namespace, NamespaceConfig, OpenOptions, Operation, ROOT_INODE, Reply,
    RequestContext,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, GenerationRepository, StorageIo,
};
use fastdup_testkit::{MemoryStorageIo, StorageOperation};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 7,
};
/// One SMB write as `smbclient put` issues it.
const SMB_WRITE_BYTES: usize = 1_048_576;
/// Image size per upload. Large enough to seal several Containers and to cross
/// one bounded Manifest leaf window.
const IMAGE_BLOCKS: u64 = 80;
const UPLOADS: usize = 3;

type Appliance = DurableNamespace<MemoryStorageIo, MemoryStorageIo>;

/// Incompressible payload. Three identical uploads then isolate Exact Dedup,
/// which is what the benchmark's three-copy run predominantly measures.
fn image() -> Vec<u8> {
    let mut bytes = vec![0_u8; SMB_WRITE_BYTES * usize::try_from(IMAGE_BLOCKS).expect("bounded")];
    let mut state = 0x5bd1_e995_d09c_3f17_u64;
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }
    bytes
}

fn create(appliance: &Appliance, name: &[u8]) -> (InodeId, HandleId) {
    let Reply::Created { entry, handle } = appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name,
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("create the upload target")
    else {
        panic!("ASSERT: create returns one new handle");
    };
    (entry.attr.inode, handle)
}

/// Uploads one image the way a single SMB stream does: one handle, strictly
/// sequential writes, a closing flush, then the durability checkpoint.
///
/// Returns the new inode and the DATA bytes that were already published when the
/// stream closed, before its checkpoint ran.
fn upload(
    appliance: &Appliance,
    data: &MemoryStorageIo,
    name: &[u8],
    image: &[u8],
) -> (InodeId, u64) {
    let (inode, handle) = create(appliance, name);
    for (ordinal, block) in image.chunks(SMB_WRITE_BYTES).enumerate() {
        appliance
            .namespace()
            .dispatch(
                CALLER,
                Operation::Write {
                    inode,
                    handle,
                    offset: u64::try_from(ordinal * SMB_WRITE_BYTES).expect("bounded offset"),
                    data: block,
                },
            )
            .expect("accept one sequential SMB write");
    }
    appliance
        .namespace()
        .dispatch(
            CALLER,
            Operation::Sync {
                inode,
                handle,
                data_only: false,
            },
        )
        .expect("close flushes the accepted stream");
    let streamed = container_bytes(data);
    appliance
        .checkpoint()
        .expect("checkpoint the completed upload")
        .expect("one completed upload needs one generation");
    (inode, streamed)
}

fn container_bytes(data: &MemoryStorageIo) -> u64 {
    data.list_names()
        .expect("list the DATA pool")
        .iter()
        .filter(|name| name.ends_with(".fdc"))
        .map(|name| data.object_len(name).expect("published Container length"))
        .sum()
}

fn assert_image(namespace: &Namespace, inode: InodeId, image: &[u8]) {
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode,
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("open the recovered upload")
    else {
        panic!("ASSERT: open returns one read handle");
    };
    for (ordinal, block) in image.chunks(SMB_WRITE_BYTES).enumerate() {
        let Reply::Data(bytes) = namespace
            .dispatch(
                CALLER,
                Operation::Read {
                    inode,
                    handle,
                    offset: u64::try_from(ordinal * SMB_WRITE_BYTES).expect("bounded offset"),
                    length: u32::try_from(block.len()).expect("bounded SMB read"),
                },
            )
            .expect("read one recovered block")
        else {
            panic!("ASSERT: read returns payload bytes");
        };
        assert_eq!(bytes, block, "block {ordinal} differs after recovery");
    }
}

#[test]
fn three_serial_single_stream_uploads_deduplicate_and_survive_a_crash() {
    let metadata = MemoryStorageIo::new();
    let data = MemoryStorageIo::new();
    let indexes = MemoryStorageIo::new();
    let appliance = DurableNamespace::open_with_index(
        NamespaceConfig::default(),
        GenerationRepository::new(metadata.clone(), checkpoint_policy_set()),
        ContainerRepository::new(data.clone()),
        &ExactIndexRunRepository::new(indexes.clone()),
        32,
    )
    .expect("open a fresh repository");
    let image = image();
    let logical = u64::try_from(image.len()).expect("bounded image") * UPLOADS as u64;

    let mut inodes = Vec::new();
    let mut physical_after_first = 0_u64;
    let mut metadata_baseline = 0;
    for upload_ordinal in 0..UPLOADS {
        let name = format!("backup-{upload_ordinal}.iso");
        let (inode, streamed) = upload(&appliance, &data, name.as_bytes(), &image);
        inodes.push(inode);
        if upload_ordinal == 0 {
            assert!(
                streamed > 0,
                "write-through must publish Containers while the stream runs, \
                 not only at its checkpoint"
            );
            physical_after_first = container_bytes(&data);
            metadata_baseline = metadata.operation_count();
        }
    }

    // A repeated upload is answered from the Exact Index, so the DATA pool must
    // not grow with it beyond one partially filled Container.
    let physical = container_bytes(&data);
    assert!(
        physical <= physical_after_first + 32 * 1_024 * 1_024,
        "two repeated uploads added {} bytes over the first upload's {physical_after_first}",
        physical - physical_after_first
    );
    assert!(
        logical / physical >= 2,
        "three identical uploads must reduce by at least a factor of two: \
         logical {logical}, physical {physical}"
    );

    // Republishing the identical stream restages Metadata that is already
    // published. Those objects are content addressed, so their payload must
    // never be read back.
    let republication = &metadata.operations()[metadata_baseline..];
    let restaged = republication
        .iter()
        .filter(|operation| **operation == StorageOperation::ReadExactAt)
        .count();
    assert!(
        restaged > 0,
        "the repeated uploads must restage already published Metadata"
    );
    assert_eq!(
        republication
            .iter()
            .filter(|operation| **operation == StorageOperation::Read)
            .count(),
        0,
        "restaging {restaged} published Metadata Objects must not reread their payload"
    );

    metadata.crash();
    data.crash();
    indexes.crash();
    let recovered = recover_mount_with_index(
        NamespaceConfig::default(),
        &GenerationRepository::new(metadata, checkpoint_policy_set()),
        &ContainerRepository::new(data),
        &ExactIndexRunRepository::new(indexes),
    )
    .expect("recover the crashed repository")
    .expect("three committed uploads exist");
    for inode in inodes {
        assert_image(&recovered, inode, &image);
    }
}
