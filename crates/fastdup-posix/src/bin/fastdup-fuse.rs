use fastdup_posix::{FuseFilesystem, Namespace, NamespaceConfig, volatile_mount_options};
use fuse3::raw::Session;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mount_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: fastdup-fuse MOUNT_PATH")?;
    if !mount_path.is_dir() {
        return Err(format!("mount path is not a directory: {}", mount_path.display()).into());
    }

    let namespace = Arc::new(Namespace::new_volatile(NamespaceConfig::default()));
    let filesystem = FuseFilesystem::new(namespace);
    let session = Session::new(volatile_mount_options());
    let mount = session.mount(filesystem, &mount_path).await?;

    eprintln!(
        "fastdup volatile FUSE checkpoint mounted at {}; data is lost on daemon exit",
        mount_path.display()
    );
    tokio::signal::ctrl_c().await?;
    mount.unmount().await?;
    Ok(())
}
