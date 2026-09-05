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
        let active = directory.join("active");
        let selected = if std::fs::symlink_metadata(&active).is_ok() {
            active
        } else {
            directory.to_path_buf()
        };
        let certificate_path = selected.join("control-plane.crt");
        let private_key_path = selected.join("control-plane.key");
        if selected == directory && (!certificate_path.exists() || !private_key_path.exists()) {
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
        let (certificate, key) = Self::self_signed_pem(hostnames)?;
        Self::publish_pem(directory, &certificate, &key)
    }

    pub fn self_signed_pem(hostnames: &[String]) -> Result<(Vec<u8>, Vec<u8>), TlsIdentityError> {
        let mut names = hostnames.to_vec();
        if !names.iter().any(|name| name == "localhost") {
            names.push("localhost".to_owned());
        }
        let generated = generate_simple_self_signed(names)?;
        Ok((
            generated.cert.pem().into_bytes(),
            generated.signing_key.serialize_pem().into_bytes(),
        ))
    }

    /// Publishes a complete pair through one atomic pointer; old identities remain recoverable.
    pub fn publish_pem(
        directory: &Path,
        certificate: &[u8],
        key: &[u8],
    ) -> Result<Self, TlsIdentityError> {
        std::fs::create_dir_all(directory)?;
        let name = format!("identity-{}", uuid::Uuid::new_v4());
        let generation = directory.join(&name);
        std::fs::create_dir(&generation)?;
        std::fs::set_permissions(&generation, std::fs::Permissions::from_mode(0o750))?;
        let stage = directory.join(format!(".active-{}", uuid::Uuid::new_v4()));
        let result = (|| -> Result<(), std::io::Error> {
            publish_private(&generation.join("control-plane.crt"), certificate)?;
            publish_private(&generation.join("control-plane.key"), key)?;
            std::os::unix::fs::symlink(&name, &stage)?;
            std::fs::File::open(directory)?.sync_all()?;
            std::fs::rename(&stage, directory.join("active"))?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&stage);
            let _ = std::fs::remove_dir_all(&generation);
        }
        result?;
        std::fs::File::open(directory)?.sync_all()?;
        Ok(Self {
            certificate_path: generation.join("control-plane.crt"),
            private_key_path: generation.join("control-plane.key"),
            fingerprint: fingerprint(certificate),
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
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(certificate))
        .next()
        .and_then(Result::ok);
    let digest = Sha256::digest(der.as_ref().map_or(certificate, |der| der.as_ref()));
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
