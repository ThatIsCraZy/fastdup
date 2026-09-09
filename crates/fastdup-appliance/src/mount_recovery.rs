//! Reclaim only a FUSE endpoint whose userspace server is already disconnected.
use std::{io, path::Path, process::Command};

pub fn ensure_mount_directory(path: &Path) -> io::Result<()> {
    check_directory(
        || std::fs::metadata(path).map(|metadata| metadata.is_dir()),
        || {
            let output = Command::new("fusermount3")
                .args(["-uz", "--"])
                .arg(path)
                .output()?;
            if !output.status.success() {
                return Err(io::Error::other(format!(
                    "cannot detach disconnected FUSE mount: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )));
            }
            eprintln!("detached_disconnected_fuse_mount={}", path.display());
            Ok(())
        },
    )
}

fn check_directory(
    mut inspect: impl FnMut() -> io::Result<bool>,
    detach: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let directory = match inspect() {
        // ENOTCONN is returned by a dead FUSE connection. A healthy mount,
        // EACCES, ENOENT or ordinary directory must never trigger unmount.
        Err(error) if error.kind() == io::ErrorKind::NotConnected => {
            detach()?;
            inspect()?
        }
        result => result?,
    };
    if directory {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "mount path is not a directory"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_disconnected_mount_is_detached_and_rechecked() {
        let mut inspections = 0;
        let mut detached = false;
        check_directory(|| {
            inspections += 1;
            if inspections == 1 { Err(io::Error::from_raw_os_error(107)) } else { Ok(true) }
        }, || { detached = true; Ok(()) }).unwrap();
        assert!(detached);
        assert_eq!(inspections, 2);
        for initial in [Ok(true), Ok(false), Err(io::Error::from(io::ErrorKind::PermissionDenied)), Err(io::Error::from(io::ErrorKind::NotFound))] {
            let expected_success = matches!(initial, Ok(true));
            let mut initial = Some(initial);
            let result = check_directory(|| initial.take().unwrap(), || panic!("must not detach a live mount or unrelated path"));
            assert_eq!(result.is_ok(), expected_success);
        }
    }

    #[test]
    fn failed_detach_or_still_disconnected_mount_cannot_start() {
        assert!(check_directory(
            || Err(io::Error::from(io::ErrorKind::NotConnected)),
            || Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        ).is_err());
        assert!(check_directory(
            || Err(io::Error::from(io::ErrorKind::NotConnected)), || Ok(()),
        ).is_err());
    }
}
