use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub const POOL_ISOLATION_POLICY_ENV: &str = "FASTDUP_POOL_ISOLATION";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolIsolationPolicy {
    Required,
    LabAllowShared,
}

impl PoolIsolationPolicy {
    /// Parses the fail-closed production policy before storage is opened.
    ///
    /// # Errors
    ///
    /// Rejects every value except `required` and `lab-allow-shared`.
    pub fn from_environment() -> Result<Self, PoolIsolationPolicyError> {
        match std::env::var_os(POOL_ISOLATION_POLICY_ENV) {
            None => Ok(Self::Required),
            Some(value) if value == OsStr::new("required") => Ok(Self::Required),
            Some(value) if value == OsStr::new("lab-allow-shared") => Ok(Self::LabAllowShared),
            Some(value) => Err(PoolIsolationPolicyError { value }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolIsolationPolicyError {
    value: OsString,
}

impl fmt::Display for PoolIsolationPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{POOL_ISOLATION_POLICY_ENV} must be required or lab-allow-shared, got {}",
            self.value.to_string_lossy()
        )
    }
}

impl std::error::Error for PoolIsolationPolicyError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolIsolationObservation {
    metadata_device: u64,
    metadata_filesystem: String,
    data_device: u64,
    data_filesystem: String,
}

impl PoolIsolationObservation {
    #[must_use]
    pub fn new(
        metadata_device: u64,
        metadata_filesystem: impl Into<String>,
        data_device: u64,
        data_filesystem: impl Into<String>,
    ) -> Self {
        Self {
            metadata_device,
            metadata_filesystem: metadata_filesystem.into(),
            data_device,
            data_filesystem: data_filesystem.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalPoolIsolation {
    Enforced,
    LabBypass,
}

impl PhysicalPoolIsolation {
    /// Observes canonical device identities and mounted filesystem formats.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if either root or Linux mount topology cannot be
    /// inspected unambiguously.
    pub fn observe_paths(
        metadata_root: &Path,
        data_root: &Path,
    ) -> io::Result<PoolIsolationObservation> {
        let metadata_root = fs::canonicalize(metadata_root)?;
        let data_root = fs::canonicalize(data_root)?;
        let mountinfo = fs::read("/proc/self/mountinfo")?;
        Ok(PoolIsolationObservation::new(
            fs::metadata(&metadata_root)?.dev(),
            filesystem_type(&mountinfo, &metadata_root)?,
            fs::metadata(&data_root)?.dev(),
            filesystem_type(&mountinfo, &data_root)?,
        ))
    }

    /// Verifies the v1 production boundary: Metadata and DATA are distinct XFS
    /// filesystems. The lab policy is deliberately reported as a bypass, never
    /// as enforced isolation.
    ///
    /// # Errors
    ///
    /// Rejects shared devices and non-XFS filesystems under the required
    /// production policy.
    pub fn audit(
        observation: &PoolIsolationObservation,
        policy: PoolIsolationPolicy,
    ) -> Result<Self, PoolIsolationError> {
        if policy == PoolIsolationPolicy::LabAllowShared {
            return Ok(Self::LabBypass);
        }
        if observation.metadata_device == observation.data_device {
            return Err(PoolIsolationError::SharedFilesystem);
        }
        if observation.metadata_filesystem != "xfs" || observation.data_filesystem != "xfs" {
            return Err(PoolIsolationError::UnsupportedFilesystem);
        }
        Ok(Self::Enforced)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolIsolationError {
    SharedFilesystem,
    UnsupportedFilesystem,
}

impl fmt::Display for PoolIsolationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SharedFilesystem => formatter
                .write_str("Metadata and DATA must be mounted on distinct physical filesystems"),
            Self::UnsupportedFilesystem => {
                formatter.write_str("production Metadata and DATA filesystems must both use XFS")
            }
        }
    }
}

impl std::error::Error for PoolIsolationError {}

fn filesystem_type(mountinfo: &[u8], path: &Path) -> io::Result<String> {
    let path = path.as_os_str().as_bytes();
    let mut best: Option<(usize, &[u8])> = None;
    for line in mountinfo.split(|byte| *byte == b'\n') {
        let fields = line
            .split(u8::is_ascii_whitespace)
            .filter(|field| !field.is_empty())
            .collect::<Vec<_>>();
        let Some(separator) = fields.iter().position(|field| *field == b"-") else {
            continue;
        };
        if separator < 5 || separator + 1 >= fields.len() {
            continue;
        }
        let mount_point = decode_mountinfo_field(fields[4])?;
        if path_is_below(path, &mount_point)
            && best
                .as_ref()
                .is_none_or(|(length, _)| mount_point.len() > *length)
        {
            best = Some((mount_point.len(), fields[separator + 1]));
        }
    }
    let (_, filesystem) = best.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "no mountinfo entry covers {}",
                PathBuf::from(path_as_os(path)).display()
            ),
        )
    })?;
    String::from_utf8(filesystem.to_vec())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "filesystem type is not UTF-8"))
}

fn path_as_os(path: &[u8]) -> OsString {
    OsString::from_vec(path.to_vec())
}

fn path_is_below(path: &[u8], mount_point: &[u8]) -> bool {
    path == mount_point
        || (path.starts_with(mount_point)
            && (mount_point == b"/" || path.get(mount_point.len()) == Some(&b'/')))
}

fn decode_mountinfo_field(encoded: &[u8]) -> io::Result<Vec<u8>> {
    let mut decoded = Vec::with_capacity(encoded.len());
    let mut cursor = 0;
    while cursor < encoded.len() {
        if encoded[cursor] == b'\\' {
            let octal = encoded.get(cursor + 1..cursor + 4).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated mountinfo escape")
            })?;
            if octal.iter().all(u8::is_ascii_digit) && octal.iter().all(|digit| *digit < b'8') {
                decoded.push((octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + octal[2] - b'0');
                cursor += 4;
                continue;
            }
        }
        decoded.push(encoded[cursor]);
        cursor += 1;
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_mount_and_escaped_path_are_selected() {
        let mountinfo =
            b"1 0 0:1 / / rw - overlay overlay rw\n2 1 0:2 / /pool\\040meta rw - xfs /dev/a rw\n";
        assert_eq!(
            filesystem_type(mountinfo, Path::new("/pool meta/subdir")).expect("mount"),
            "xfs"
        );
    }
}
