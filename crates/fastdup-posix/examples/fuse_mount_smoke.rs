use fastdup_posix::{FuseFilesystem, Namespace, NamespaceConfig, volatile_mount_options};
use fuse3::raw::Session;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mount_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: fuse_mount_smoke MOUNT_PATH")?;
    std::fs::create_dir_all(&mount_path)?;
    if std::fs::read_dir(&mount_path)?.next().is_some() {
        return Err("mount directory must be empty".into());
    }

    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let filesystem = FuseFilesystem::new(namespace);
    let mut mount_options = volatile_mount_options();
    mount_options.force_readdir_plus(true);
    let session = Session::new(mount_options);
    let mount = session.mount(filesystem, &mount_path).await?;

    let syscall_path = mount_path.clone();
    let result = tokio::task::spawn_blocking(move || exercise_syscalls(&syscall_path)).await?;
    let unmount_result = mount.unmount().await;
    result?;
    unmount_result?;
    Ok(())
}

fn exercise_syscalls(mount_path: &std::path::Path) -> io::Result<()> {
    const SPARSE_OFFSET: u64 = 1_024 * 1_024 * 1_024 * 1_024;
    let raw_name = std::ffi::OsString::from_vec(b"vm-\xff".to_vec());
    let path = mount_path.join(&raw_name);
    let writer = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let first_inode = writer.metadata()?.ino();

    assert_eq!(writer.write_at(b"abcdef", 0)?, 6);
    writer.sync_all()?;
    let reader = OpenOptions::new().read(true).write(true).open(&path)?;
    let mut bytes = [0_u8; 6];
    reader.read_exact_at(&mut bytes, 0)?;
    assert_eq!(&bytes, b"abcdef");

    assert_eq!(reader.write_at(b"ZZ", 2)?, 2);
    writer.read_exact_at(&mut bytes, 0)?;
    assert_eq!(&bytes, b"abZZef");

    writer.set_len(SPARSE_OFFSET)?;
    assert_eq!(writer.write_at(b"X", SPARSE_OFFSET)?, 1);
    let mut boundary = [1_u8; 5];
    reader.read_exact_at(&mut boundary, SPARSE_OFFSET - 4)?;
    assert_eq!(boundary, [0, 0, 0, 0, b'X']);
    let sparse_metadata = writer.metadata()?;
    assert_eq!(sparse_metadata.len(), SPARSE_OFFSET + 1);
    assert!(
        sparse_metadata.blocks() <= 8,
        "sparse logical holes must not be reported as allocated blocks"
    );

    std::fs::remove_file(&path)?;
    assert_eq!(
        std::fs::metadata(&path).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    writer.read_exact_at(&mut bytes, 0)?;
    assert_eq!(&bytes, b"abZZef");
    drop(reader);
    drop(writer);

    let recreated = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    assert!(recreated.metadata()?.ino() > first_inode);
    let names = std::fs::read_dir(mount_path)?
        .map(|entry| entry.map(|value| value.file_name().as_bytes().to_vec()))
        .collect::<io::Result<Vec<_>>>()?;
    assert_eq!(names, vec![raw_name.as_bytes().to_vec()]);
    drop(recreated);
    std::fs::remove_file(path)?;

    exercise_concurrent_append(mount_path)?;
    exercise_sequential_extent_coalescing(mount_path)?;
    exercise_readdirplus_pagination(mount_path)?;
    Ok(())
}

fn exercise_concurrent_append(mount_path: &std::path::Path) -> io::Result<()> {
    const WRITERS: u32 = 8;
    const RECORDS: u32 = 64;
    const FRAME_BYTES: usize = 16;

    let path = mount_path.join("append.log");
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    let mut threads = Vec::new();
    for writer in 0..WRITERS {
        let path = path.clone();
        threads.push(std::thread::spawn(move || -> io::Result<()> {
            let mut file = OpenOptions::new().append(true).open(path)?;
            for record in 0..RECORDS {
                let mut frame = [0_u8; FRAME_BYTES];
                frame[0..4].copy_from_slice(&writer.to_le_bytes());
                frame[4..8].copy_from_slice(&record.to_le_bytes());
                frame[8..].copy_from_slice(b"FASTDUP!");
                file.write_all(&frame)?;
            }
            Ok(())
        }));
    }
    for thread in threads {
        thread
            .join()
            .map_err(|_| io::Error::other("append worker panicked"))??;
    }

    let mut bytes = Vec::new();
    std::fs::File::open(&path)?.read_to_end(&mut bytes)?;
    assert_eq!(
        bytes.len(),
        usize::try_from(WRITERS * RECORDS).expect("record count fits") * FRAME_BYTES
    );
    let mut observed = std::collections::BTreeSet::new();
    for frame in bytes.chunks_exact(FRAME_BYTES) {
        assert_eq!(&frame[8..], b"FASTDUP!");
        let writer = u32::from_le_bytes(frame[0..4].try_into().expect("four-byte writer"));
        let record = u32::from_le_bytes(frame[4..8].try_into().expect("four-byte record"));
        assert!(observed.insert((writer, record)), "duplicate append frame");
    }
    let expected = (0..WRITERS)
        .flat_map(|writer| (0..RECORDS).map(move |record| (writer, record)))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(observed, expected);
    std::fs::remove_file(path)?;
    Ok(())
}

fn exercise_sequential_extent_coalescing(mount_path: &std::path::Path) -> io::Result<()> {
    const MIB: usize = 1_024 * 1_024;
    const BLOCKS: u64 = 20;

    let path = mount_path.join("sequential.raw");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let block = vec![0x5a; MIB];
    for index in 0..BLOCKS {
        file.write_all_at(&block, index * u64::try_from(MIB).expect("MiB fits in u64"))?;
    }
    let metadata = file.metadata()?;
    assert_eq!(metadata.len(), BLOCKS * 1_024 * 1_024);
    assert_eq!(metadata.blocks(), BLOCKS * 1_024 * 1_024 / 512);
    let mut boundary = [0_u8; 16];
    file.read_exact_at(&mut boundary, 16 * 1_024 * 1_024 - 8)?;
    assert_eq!(boundary, [0x5a; 16]);
    drop(file);
    std::fs::remove_file(path)?;
    Ok(())
}

fn exercise_readdirplus_pagination(mount_path: &std::path::Path) -> io::Result<()> {
    const ENTRIES: u32 = 512;

    let mut expected = Vec::new();
    for index in 0..ENTRIES {
        let name = format!("entry-{index:04}-{}", "x".repeat(160));
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(mount_path.join(&name))?;
        expected.push(name.into_bytes());
    }
    expected.sort_unstable();

    let mut observed = std::fs::read_dir(mount_path)?
        .map(|entry| entry.map(|value| value.file_name().as_bytes().to_vec()))
        .collect::<io::Result<Vec<_>>>()?;
    observed.sort_unstable();
    assert_eq!(observed, expected);

    for name in expected {
        std::fs::remove_file(mount_path.join(std::ffi::OsStr::from_bytes(&name)))?;
    }
    Ok(())
}
