use fastdup_posix::{
    FuseFilesystem, Namespace, NamespaceConfig, StatFsSnapshot, StatFsSource,
    volatile_mount_options,
};
use fuse3::raw::Session;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

const SMOKE_CAPACITY_BYTES: u64 = 16 * 1_024 * 1_024 * 1_024;
const SMOKE_AVAILABLE_BYTES: u64 = 12 * 1_024 * 1_024 * 1_024;

#[derive(Debug)]
struct SmokeStatFs;

impl StatFsSource for SmokeStatFs {
    fn snapshot(&self) -> io::Result<StatFsSnapshot> {
        StatFsSnapshot::new(
            SMOKE_CAPACITY_BYTES,
            SMOKE_AVAILABLE_BYTES,
            SMOKE_AVAILABLE_BYTES,
            1_000_000,
            900_000,
            4_096,
            255,
        )
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let first = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: fuse_mount_smoke MOUNT_PATH")?;
    if first.as_os_str() == "--lock-child" {
        let path = arguments
            .next()
            .map(PathBuf::from)
            .ok_or("missing lock path")?;
        return exercise_lock_child(&path).map_err(Into::into);
    }
    let mount_path = first;
    std::fs::create_dir_all(&mount_path)?;
    if std::fs::read_dir(&mount_path)?.next().is_some() {
        return Err("mount directory must be empty".into());
    }

    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let filesystem = FuseFilesystem::new(namespace).with_statfs_source(Arc::new(SmokeStatFs));
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
    exercise_statfs(mount_path)?;
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

    exercise_xattrs_acl_and_immutable(&path, &writer)?;

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
    exercise_process_record_lock(mount_path)?;
    exercise_sparse_allocation_syscalls(mount_path)?;
    exercise_links_ownership_and_times(mount_path)?;
    exercise_sequential_extent_coalescing(mount_path)?;
    exercise_readdirplus_pagination(mount_path)?;
    Ok(())
}

fn exercise_links_ownership_and_times(mount_path: &std::path::Path) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, chown, symlink};

    let original = mount_path.join("posix-original");
    let hardlink = mount_path.join("posix-hardlink");
    let symlink_path = mount_path.join("posix-symlink");
    std::fs::write(&original, b"one inode")?;
    std::fs::hard_link(&original, &hardlink)?;
    let original_metadata = std::fs::metadata(&original)?;
    let hardlink_metadata = std::fs::metadata(&hardlink)?;
    assert_eq!(original_metadata.ino(), hardlink_metadata.ino());
    assert_eq!(hardlink_metadata.nlink(), 2);
    symlink("posix-hardlink", &symlink_path)?;
    assert_eq!(
        std::fs::read_link(&symlink_path)?,
        std::path::Path::new("posix-hardlink")
    );

    let atime = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(123);
    let mtime = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(456);
    let file = OpenOptions::new().read(true).write(true).open(&original)?;
    file.set_times(
        std::fs::FileTimes::new()
            .set_accessed(atime)
            .set_modified(mtime),
    )?;
    let metadata = file.metadata()?;
    assert_eq!(metadata.atime(), 123);
    assert_eq!(metadata.mtime(), 456);
    chown(&original, Some(1_234), Some(2_345))?;
    let metadata = file.metadata()?;
    assert_eq!(metadata.uid(), 1_234);
    assert_eq!(metadata.gid(), 2_345);

    std::fs::remove_file(&original)?;
    assert_eq!(std::fs::read(&hardlink)?, b"one inode");
    std::fs::remove_file(&symlink_path)?;
    std::fs::remove_file(&hardlink)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn exercise_xattrs_acl_and_immutable(
    path: &std::path::Path,
    file: &std::fs::File,
) -> io::Result<()> {
    use rustix::fs::{
        IFlags, XattrFlags, fgetxattr, flistxattr, fremovexattr, fsetxattr, ioctl_getflags,
    };

    fsetxattr(
        file,
        "user.immutable.until",
        b"2030-01-01 00:00:00",
        XattrFlags::CREATE,
    )
    .map_err(io::Error::from)?;
    let mut retention_buffer = vec![0_u8; 64];
    let retention_length =
        fgetxattr(file, "user.immutable.until", &mut retention_buffer).map_err(io::Error::from)?;
    assert_eq!(
        &retention_buffer[..retention_length],
        b"2030-01-01 00:00:00"
    );
    let mut list_buffer = vec![0_u8; 256];
    let list_length = flistxattr(file, &mut list_buffer).map_err(io::Error::from)?;
    assert!(
        list_buffer[..list_length]
            .split(|byte| *byte == 0)
            .any(|name| name == b"user.immutable.until")
    );

    let access_acl = acl(&[
        (0x01, 0o7, u32::MAX),
        (0x02, 0o6, 2_000),
        (0x04, 0o5, u32::MAX),
        (0x10, 0o4, u32::MAX),
        (0x20, 0o1, u32::MAX),
    ]);
    fsetxattr(
        file,
        "system.posix_acl_access",
        &access_acl,
        XattrFlags::empty(),
    )
    .map_err(io::Error::from)?;
    assert_eq!(file.metadata()?.mode() & 0o777, 0o741);
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o601))?;
    assert_eq!(file.metadata()?.mode() & 0o777, 0o601);

    assert!(
        Command::new("chattr")
            .arg("+i")
            .arg(path)
            .status()?
            .success()
    );
    assert!(
        ioctl_getflags(file)
            .map_err(io::Error::from)?
            .contains(IFlags::IMMUTABLE)
    );
    let listed_flags = Command::new("lsattr").arg("-d").arg(path).output()?;
    assert!(listed_flags.status.success());
    assert!(
        listed_flags
            .stdout
            .iter()
            .take(24)
            .any(|byte| *byte == b'i')
    );
    assert_eq!(
        file.write_at(b"blocked", 0)
            .expect_err("immutable write fails")
            .raw_os_error(),
        Some(libc::EPERM)
    );
    assert_eq!(
        std::fs::remove_file(path)
            .expect_err("immutable unlink fails")
            .raw_os_error(),
        Some(libc::EPERM)
    );
    assert!(
        Command::new("chattr")
            .arg("-i")
            .arg(path)
            .status()?
            .success()
    );
    fremovexattr(file, "user.immutable.until").map_err(io::Error::from)?;

    let acl_directory = path
        .parent()
        .expect("ASSERT: smoke path has a parent")
        .join("acl-default");
    std::fs::create_dir(&acl_directory)?;
    let directory = std::fs::File::open(&acl_directory)?;
    let default_acl = acl(&[
        (0x01, 0o7, u32::MAX),
        (0x02, 0o6, 2_000),
        (0x04, 0o5, u32::MAX),
        (0x10, 0o4, u32::MAX),
        (0x20, 0o1, u32::MAX),
    ]);
    fsetxattr(
        &directory,
        "system.posix_acl_default",
        &default_acl,
        XattrFlags::CREATE,
    )
    .map_err(io::Error::from)?;
    let inherited_path = acl_directory.join("inherited.vbk");
    let inherited = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&inherited_path)?;
    assert_eq!(inherited.metadata()?.mode() & 0o777, 0o640);
    let mut inherited_acl = vec![0_u8; 256];
    let inherited_length = fgetxattr(&inherited, "system.posix_acl_access", &mut inherited_acl)
        .map_err(io::Error::from)?;
    assert!(inherited_length > 4);
    drop(inherited);
    std::fs::remove_file(inherited_path)?;
    std::fs::remove_dir(acl_directory)?;
    Ok(())
}

fn acl(entries: &[(u16, u16, u32)]) -> Vec<u8> {
    let mut value = 2_u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in entries {
        value.extend_from_slice(&tag.to_le_bytes());
        value.extend_from_slice(&permissions.to_le_bytes());
        value.extend_from_slice(&id.to_le_bytes());
    }
    value
}

fn exercise_statfs(mount_path: &std::path::Path) -> io::Result<()> {
    let statistics = rustix::fs::statvfs(mount_path).map_err(io::Error::from)?;
    assert_eq!(
        statistics.f_blocks * statistics.f_frsize,
        SMOKE_CAPACITY_BYTES
    );
    assert_eq!(
        statistics.f_bavail * statistics.f_frsize,
        SMOKE_AVAILABLE_BYTES
    );
    assert_eq!(statistics.f_files, 1_000_000);
    assert_eq!(statistics.f_ffree, 900_000);
    assert_eq!(statistics.f_namemax, 255);
    Ok(())
}

fn exercise_sparse_allocation_syscalls(mount_path: &std::path::Path) -> io::Result<()> {
    use rustix::fs::{FallocateFlags, SeekFrom, fallocate, seek};

    let path = mount_path.join("sparse-allocation.dat");
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    file.write_all_at(b"abc", 0)?;
    file.write_all_at(b"XYZ", 8)?;
    assert_eq!(seek(&file, SeekFrom::Hole(0)).map_err(io::Error::from)?, 3);
    assert_eq!(seek(&file, SeekFrom::Data(3)).map_err(io::Error::from)?, 8);

    fallocate(
        &file,
        FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
        1,
        1,
    )
    .map_err(io::Error::from)?;
    assert_eq!(seek(&file, SeekFrom::Hole(1)).map_err(io::Error::from)?, 1);
    fallocate(&file, FallocateFlags::ZERO_RANGE, 1, 1).map_err(io::Error::from)?;
    assert_eq!(seek(&file, SeekFrom::Data(1)).map_err(io::Error::from)?, 1);
    fallocate(&file, FallocateFlags::empty(), 2, 6).map_err(io::Error::from)?;
    assert_eq!(seek(&file, SeekFrom::Hole(0)).map_err(io::Error::from)?, 11);
    assert_eq!(file.metadata()?.blocks(), 11_u64.div_ceil(512));

    for mode in [FallocateFlags::COLLAPSE_RANGE, FallocateFlags::INSERT_RANGE] {
        let error = fallocate(&file, mode, 1, 1)
            .expect_err("the Linux FUSE kernel path rejects structural fallocate modes");
        assert_eq!(error, rustix::io::Errno::OPNOTSUPP);
    }

    drop(file);
    std::fs::remove_file(path)
}

fn exercise_process_record_lock(mount_path: &std::path::Path) -> io::Result<()> {
    use rustix::fs::{FlockOperation, fcntl_lock};

    let path = mount_path.join("record-lock.dat");
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)?;
    let mut child = Command::new(std::env::current_exe()?)
        .arg("--lock-child")
        .arg(&path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut ready = String::new();
    BufReader::new(
        child
            .stdout
            .take()
            .expect("ASSERT: lock child stdout is piped"),
    )
    .read_line(&mut ready)?;
    assert_eq!(ready, "LOCKED\n");

    let contender = OpenOptions::new().read(true).write(true).open(&path)?;
    let conflict = fcntl_lock(&contender, FlockOperation::NonBlockingLockExclusive)
        .expect_err("independent process holds a conflicting record lock");
    assert!(
        matches!(
            conflict,
            rustix::io::Errno::AGAIN | rustix::io::Errno::ACCESS
        ),
        "conflicting record lock returned {conflict}"
    );
    child
        .stdin
        .take()
        .expect("ASSERT: lock child stdin is piped")
        .write_all(b"\n")?;
    assert!(child.wait()?.success());

    fcntl_lock(&contender, FlockOperation::LockExclusive).map_err(io::Error::from)?;
    fcntl_lock(&contender, FlockOperation::Unlock).map_err(io::Error::from)?;
    drop(contender);
    std::fs::remove_file(path)?;
    Ok(())
}

fn exercise_lock_child(path: &std::path::Path) -> io::Result<()> {
    use rustix::fs::{FlockOperation, fcntl_lock};

    let file = OpenOptions::new().read(true).write(true).open(path)?;
    fcntl_lock(&file, FlockOperation::LockExclusive).map_err(io::Error::from)?;
    println!("LOCKED");
    io::stdout().flush()?;
    let mut release = String::new();
    io::stdin().read_line(&mut release)?;
    fcntl_lock(&file, FlockOperation::Unlock).map_err(io::Error::from)
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
