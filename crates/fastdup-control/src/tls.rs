use std::fmt::Write as _;
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use rcgen::generate_simple_self_signed;
use sha2::{Digest as _, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum TlsIdentityError {
    #[error("TLS identity I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS identity generation failed: {0}")]
    Rcgen(#[from] rcgen::Error),
}

#[derive(Clone, Debug)]
pub struct TlsIdentity {
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
    pub fingerprint: String,
}

impl TlsIdentity {
    pub fn load_or_generate(
        directory: &Path,
        hostnames: &[String],
    ) -> Result<Self, TlsIdentityError> {
        std::fs::create_dir_all(directory)?;
        let certificate_path = directory.join("control-plane.crt");
        let private_key_path = directory.join("control-plane.key");
        if !certificate_path.exists() || !private_key_path.exists() {
            generate(&certificate_path, &private_key_path, hostnames)?;
        }
        ensure_private_mode(&certificate_path)?;
        ensure_private_mode(&private_key_path)?;
        let certificate = std::fs::read(&certificate_path)?;
        Ok(Self {
            certificate_path,
            private_key_path,
            fingerprint: fingerprint(&certificate),
        })
    }

    pub fn regenerate(directory: &Path, hostnames: &[String]) -> Result<Self, TlsIdentityError> {
        std::fs::create_dir_all(directory)?;
        let certificate_path = directory.join("control-plane.crt");
        let private_key_path = directory.join("control-plane.key");
        generate(&certificate_path, &private_key_path, hostnames)?;
        let certificate = std::fs::read(&certificate_path)?;
        Ok(Self {
            certificate_path,
            private_key_path,
            fingerprint: fingerprint(&certificate),
        })
    }
}

fn generate(
    certificate_path: &Path,
    private_key_path: &Path,
    hostnames: &[String],
) -> Result<(), TlsIdentityError> {
    let mut names = hostnames.to_vec();
    if !names.iter().any(|name| name == "localhost") {
        names.push("localhost".to_owned());
    }
    let generated = generate_simple_self_signed(names)?;
    publish_private(
        private_key_path,
        generated.signing_key.serialize_pem().as_bytes(),
    )?;
    publish_private(certificate_path, generated.cert.pem().as_bytes())?;
    Ok(())
}

fn publish_private(path: &Path, contents: &[u8]) -> Result<(), std::io::Error> {
    let stage = path.with_extension("staged");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o640)
        .open(&stage)?;
    file.write_all(contents)?;
    file.sync_all()?;
    std::fs::rename(stage, path)?;
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn ensure_private_mode(path: &Path) -> Result<(), std::io::Error> {
    let metadata = std::fs::metadata(path)?;
    if metadata.permissions().mode() & 0o777 != 0o640 {
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o640);
        std::fs::set_permissions(path, permissions)?;
    }
    Ok(())
}

fn fingerprint(certificate: &[u8]) -> String {
    let digest = Sha256::digest(certificate);
    let mut result = String::with_capacity(digest.len() * 3 - 1);
    for (index, byte) in digest.iter().enumerate() {
        if index > 0 {
            result.push(':');
        }
        write!(&mut result, "{byte:02X}").expect("String write cannot fail");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_stable_after_first_generation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let first = TlsIdentity::load_or_generate(directory.path(), &["fastdup.test".to_owned()])
            .expect("generate identity");
        let second = TlsIdentity::load_or_generate(directory.path(), &["changed.test".to_owned()])
            .expect("load identity");
        assert_eq!(first.fingerprint, second.fingerprint);
        assert_eq!(
            std::fs::metadata(&first.private_key_path)
                .expect("key metadata")
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
        let replacement = TlsIdentity::regenerate(directory.path(), &["fastdup.test".to_owned()])
            .expect("regenerate identity");
        assert_ne!(first.fingerprint, replacement.fingerprint);
    }
}
