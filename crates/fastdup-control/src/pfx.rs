//! Password-protected PKCS#12 import via the appliance's OpenSSL executable.
//! Secrets travel through stdin / a child-only environment, never command arguments.
use std::io::Write as _;
use std::process::{Command, Stdio};

pub fn decode_pfx(archive: &[u8], password: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
    if archive.is_empty()
        || archive.len() > 1_048_576
        || password.len() > 1024
        || password.contains('\0')
    {
        return Err("Invalid PFX size or password (maximum 1 MiB)".into());
    }
    let extract = |args: &[&str]| -> Result<Vec<u8>, String> {
        let mut child = Command::new("openssl")
            .args(["pkcs12", "-passin", "env:FASTDUP_PFX_PASSWORD"])
            .args(args)
            .env("FASTDUP_PFX_PASSWORD", password)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| "OpenSSL could not be started")?;
        let mut input = child.stdin.take().ok_or("PFX input unavailable")?;
        let written = input.write_all(archive);
        drop(input);
        let output = child.wait_with_output().map_err(|_| "PFX import failed")?;
        if written.is_err() || !output.status.success() {
            return Err("PFX could not be decrypted; check the password and archive format".into());
        }
        Ok(output.stdout)
    };
    let leaf = extract(&["-clcerts", "-nokeys"])?;
    let certificates = rustls_pemfile::certs(&mut std::io::Cursor::new(&leaf))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "Invalid certificate")?;
    if certificates.len() != 1 {
        return Err("PFX must contain exactly one server certificate".into());
    }
    let (_, certificate) = x509_parser::parse_x509_certificate(certificates[0].as_ref())
        .map_err(|_| "Invalid X.509 certificate")?;
    if !certificate.validity().is_valid() {
        return Err("Certificate is expired or not yet valid".into());
    }
    let key = extract(&["-nocerts", "-nodes"])?;
    if rustls_pemfile::private_key(&mut std::io::Cursor::new(&key))
        .map_err(|_| "Invalid private key")?
        .is_none()
    {
        return Err("PFX does not contain a private key".into());
    }
    let ca = extract(&["-cacerts", "-nokeys"])?;
    // OpenSSL emits the leaf separately, followed by the bundled CA chain.
    let mut chain = leaf;
    chain.extend(ca);
    Ok((chain, key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TlsIdentity;

    #[tokio::test]
    async fn pfx_roundtrip_keeps_key_and_rejects_wrong_password_without_replacing_identity() {
        let directory = tempfile::tempdir().unwrap();
        let original =
            TlsIdentity::load_or_generate(directory.path(), &["localhost".into()]).unwrap();
        let output = Command::new("openssl")
            .args([
                "pkcs12",
                "-export",
                "-passout",
                "pass:fixture-password",
                "-inkey",
            ])
            .arg(&original.private_key_path)
            .arg("-in")
            .arg(&original.certificate_path)
            .output()
            .unwrap();
        assert!(output.status.success());
        let (cert, key) = decode_pfx(&output.stdout, "fixture-password").unwrap();
        assert!(decode_pfx(&output.stdout, "wrong-password").is_err());
        assert!(decode_pfx(b"not a pfx", "").is_err());
        let unchanged =
            TlsIdentity::load_or_generate(directory.path(), &["localhost".into()]).unwrap();
        assert_eq!(original.fingerprint, unchanged.fingerprint);
        axum_server::tls_rustls::RustlsConfig::from_pem(cert.clone(), key.clone())
            .await
            .unwrap();
        let installed = TlsIdentity::publish_pem(directory.path(), &cert, &key).unwrap();
        let restarted =
            TlsIdentity::load_or_generate(directory.path(), &["localhost".into()]).unwrap();
        assert_eq!(installed.fingerprint, restarted.fingerprint);
        assert_eq!(original.fingerprint, installed.fingerprint);
        assert_eq!(std::fs::read(restarted.private_key_path).unwrap(), key);
        let (_, mismatched_key) = TlsIdentity::self_signed_pem(&["localhost".into()]).unwrap();
        assert!(
            axum_server::tls_rustls::RustlsConfig::from_pem(cert, mismatched_key)
                .await
                .is_err()
        );
    }
}
