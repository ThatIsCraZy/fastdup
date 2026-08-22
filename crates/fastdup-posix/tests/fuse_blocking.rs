use std::sync::Arc;
use std::time::{Duration, Instant};

use fastdup_posix::{
    CommittedEntry, CommittedFile, CommittedInode, CommittedNamespaceSnapshot, FuseFilesystem,
    InodeId, MutationObserver, MutationPayload, Namespace, NamespaceConfig, OpenOptions, Operation,
    PosixError, ROOT_INODE, Reply, RequestContext,
};
use fuse3::raw::{Filesystem, Request};

const CALLER: RequestContext = RequestContext {
    uid: 1_000,
    gid: 1_000,
    pid: 77,
};

#[derive(Debug)]
struct SlowFile;

#[derive(Debug)]
struct SlowFenceObserver;

#[derive(Debug)]
struct SlowWriteObserver;

#[derive(Debug)]
struct PayloadPointerObserver(std::sync::mpsc::Sender<usize>);

impl MutationObserver for PayloadPointerObserver {
    fn accepted_write(
        &self,
        _inode: InodeId,
        _offset: u64,
        _mutation_sequence: u64,
        bytes: MutationPayload,
    ) -> Vec<fastdup_posix::ExternalizedExtent> {
        self.0
            .send(bytes.as_bytes().as_ptr() as usize)
            .expect("fixture receiver remains alive");
        Vec::new()
    }

    fn accepted_truncate(&self, _inode: InodeId, _mutation_sequence: u64, _length: u64) {}
}

impl MutationObserver for SlowWriteObserver {
    fn accepted_write(
        &self,
        _inode: InodeId,
        _offset: u64,
        _mutation_sequence: u64,
        _bytes: MutationPayload,
    ) -> Vec<fastdup_posix::ExternalizedExtent> {
        std::thread::sleep(Duration::from_millis(300));
        Vec::new()
    }

    fn accepted_truncate(&self, _inode: InodeId, _mutation_sequence: u64, _length: u64) {}
}

impl MutationObserver for SlowFenceObserver {
    fn accepted_write(
        &self,
        _inode: InodeId,
        _offset: u64,
        _mutation_sequence: u64,
        _bytes: MutationPayload,
    ) -> Vec<fastdup_posix::ExternalizedExtent> {
        Vec::new()
    }

    fn accepted_truncate(&self, _inode: InodeId, _mutation_sequence: u64, _length: u64) {}

    fn wait_through(&self, _inode: InodeId, _mutation_sequence: u64) {
        std::thread::sleep(Duration::from_millis(300));
    }
}

impl CommittedFile for SlowFile {
    fn logical_size(&self) -> u64 {
        1
    }

    fn allocated_bytes(&self) -> u64 {
        1
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        Ok(u64::from(offset == 0 && length != 0))
    }

    fn read_at(&self, _offset: u64, _length: u32) -> Result<Vec<u8>, PosixError> {
        std::thread::sleep(Duration::from_millis(300));
        Ok(vec![b'X'])
    }
}

#[tokio::test(flavor = "current_thread")]
async fn slow_committed_read_does_not_block_the_fuse_runtime_thread() {
    let inode = CommittedInode::new(2, 0o600, 1_000, 1_000, 1, 1, Arc::new(SlowFile))
        .expect("fixture inode is valid");
    let snapshot = CommittedNamespaceSnapshot::new(
        4_096,
        4_096,
        1,
        vec![inode],
        vec![
            CommittedEntry::new(ROOT_INODE.get(), 2, b"slow".to_vec())
                .expect("fixture entry is valid"),
        ],
    )
    .expect("fixture snapshot is valid");
    let namespace = Arc::new(
        Namespace::from_committed(NamespaceConfig::default(), snapshot)
            .expect("fixture namespace mounts"),
    );
    let Reply::Opened(handle) = namespace
        .dispatch(
            CALLER,
            Operation::Open {
                inode: fastdup_posix::InodeId::new(2).expect("fixture inode is nonzero"),
                options: OpenOptions::READ_ONLY,
                truncate: false,
            },
        )
        .expect("fixture file opens")
    else {
        panic!("open returned the wrong reply");
    };
    let filesystem = FuseFilesystem::new(namespace);
    let started = Instant::now();
    let read = tokio::spawn(async move {
        Filesystem::read(&filesystem, Request::default(), 2, handle.get(), 0, 1)
            .await
            .expect("slow FUSE read succeeds")
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "blocking committed storage I/O must leave the async FUSE runtime responsive"
    );
    let reply = read.await.expect("FUSE read task does not panic");
    assert_eq!(reply.data.as_ref(), b"X");
}

#[tokio::test(flavor = "current_thread")]
async fn release_sequence_fence_does_not_block_the_fuse_runtime_thread() {
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    namespace.install_mutation_observer(Arc::new(SlowFenceObserver));
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"slow-release",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("fixture file is created")
    else {
        panic!("create returned the wrong reply");
    };
    namespace
        .dispatch(
            CALLER,
            Operation::Write {
                inode: entry.attr.inode,
                handle,
                offset: 0,
                data: b"X",
            },
        )
        .expect("fixture write is accepted");
    let filesystem = FuseFilesystem::new(namespace);
    let started = Instant::now();
    let release = tokio::spawn(async move {
        Filesystem::release(
            &filesystem,
            Request::default(),
            entry.attr.inode.get(),
            handle.get(),
            0,
            0,
            false,
        )
        .await
        .expect("slow FUSE release succeeds");
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "a sequence fence must leave the async FUSE runtime responsive"
    );
    release.await.expect("FUSE release task does not panic");
}

#[tokio::test(flavor = "current_thread")]
async fn write_queue_admission_does_not_block_the_fuse_runtime_thread() {
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"slow-write",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("fixture file is created")
    else {
        panic!("create returned the wrong reply");
    };
    namespace.install_mutation_observer(Arc::new(SlowWriteObserver));
    let filesystem = FuseFilesystem::new(namespace);
    let started = Instant::now();
    let write = tokio::spawn(async move {
        Filesystem::write(
            &filesystem,
            Request::default(),
            entry.attr.inode.get(),
            handle.get(),
            0,
            b"X",
            0,
            0,
        )
        .await
        .expect("slow FUSE write succeeds")
    });

    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "write-through queue admission must leave the async FUSE runtime responsive"
    );
    assert_eq!(
        write.await.expect("FUSE write task does not panic").written,
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn owned_fuse_write_reaches_the_namespace_without_a_second_payload_copy() {
    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let Reply::Created { entry, handle } = namespace
        .dispatch(
            CALLER,
            Operation::Create {
                parent: ROOT_INODE,
                name: b"owned-write",
                mode: 0o600,
                options: OpenOptions::READ_WRITE,
                exclusive: true,
                truncate: false,
            },
        )
        .expect("fixture file is created")
    else {
        panic!("create returned the wrong reply");
    };
    let (pointer_sender, pointer_receiver) = std::sync::mpsc::channel();
    namespace.install_mutation_observer(Arc::new(PayloadPointerObserver(pointer_sender)));
    let filesystem = FuseFilesystem::new(namespace);
    let payload = vec![0x5a; 128 * 1_024];
    let original_pointer = payload.as_ptr() as usize;

    let reply = Filesystem::write_owned(
        &filesystem,
        Request::default(),
        entry.attr.inode.get(),
        handle.get(),
        0,
        payload,
        0,
        0,
    )
    .await
    .expect("owned FUSE write succeeds");

    assert_eq!(reply.written, 128 * 1_024);
    assert_eq!(
        pointer_receiver
            .recv()
            .expect("observer reports the retained payload allocation"),
        original_pointer,
        "the owned FUSE write must preserve its request allocation"
    );
}
