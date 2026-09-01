use std::collections::BTreeMap;
use std::fmt;

pub const MAX_SMALL_FILE_EXTENSIONS: usize = 64;
pub const MAX_SMALL_FILE_EXTENSION_BYTES: usize = 32;
pub const MAX_SMALL_FILE_POLICY_REVISION_BYTES: usize = 128;
pub const DEFAULT_SMALL_FILE_EXTENSIONS: [&str; 2] = [".json", ".xml"];

const NO_TRANSITION: u16 = u16::MAX;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SmallFilePolicySnapshot {
    pub revision: String,
    pub extensions: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SmallFilePolicyError {
    EmptyRevision,
    RevisionTooLong,
    TooManyExtensions,
    InvalidExtension(String),
}

impl fmt::Display for SmallFilePolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyRevision => formatter.write_str("small-file policy revision is empty"),
            Self::RevisionTooLong => formatter.write_str("small-file policy revision is too long"),
            Self::TooManyExtensions => formatter.write_str("too many small-file extensions"),
            Self::InvalidExtension(extension) => {
                write!(formatter, "invalid small-file extension {extension:?}")
            }
        }
    }
}

impl std::error::Error for SmallFilePolicyError {}

#[derive(Clone, Debug)]
pub(crate) struct SmallFileExtensionPolicy {
    revision: String,
    extensions: Vec<String>,
    matcher: ReverseSuffixMatcher,
}

impl Default for SmallFileExtensionPolicy {
    fn default() -> Self {
        Self::compile(
            "default-v1".to_owned(),
            DEFAULT_SMALL_FILE_EXTENSIONS.map(str::to_owned).to_vec(),
        )
        .expect("ASSERT: built-in Small-File extensions are valid")
    }
}

impl SmallFileExtensionPolicy {
    pub(crate) fn compile(
        revision: String,
        extensions: Vec<String>,
    ) -> Result<Self, SmallFilePolicyError> {
        if revision.is_empty() {
            return Err(SmallFilePolicyError::EmptyRevision);
        }
        if revision.len() > MAX_SMALL_FILE_POLICY_REVISION_BYTES {
            return Err(SmallFilePolicyError::RevisionTooLong);
        }
        if extensions.len() > MAX_SMALL_FILE_EXTENSIONS {
            return Err(SmallFilePolicyError::TooManyExtensions);
        }

        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(extensions.len())
            .map_err(|_| SmallFilePolicyError::TooManyExtensions)?;
        for mut extension in extensions {
            if !valid_extension(&extension) {
                return Err(SmallFilePolicyError::InvalidExtension(extension));
            }
            extension.make_ascii_lowercase();
            canonical.push(extension);
        }
        canonical.sort_unstable();
        canonical.dedup();

        Ok(Self {
            revision,
            matcher: ReverseSuffixMatcher::compile(&canonical),
            extensions: canonical,
        })
    }

    pub(crate) fn matches_name(&self, name: &[u8]) -> bool {
        self.matcher.matches(name)
    }

    pub(crate) fn snapshot(&self) -> SmallFilePolicySnapshot {
        SmallFilePolicySnapshot {
            revision: self.revision.clone(),
            extensions: self.extensions.clone(),
        }
    }
}

/// Validates and canonicalizes a candidate extension list without installing it.
///
/// # Errors
///
/// Returns [`SmallFilePolicyError`] when the list is unbounded or contains an
/// invalid suffix.
pub fn validate_small_file_extensions(
    extensions: &[String],
) -> Result<Vec<String>, SmallFilePolicyError> {
    SmallFileExtensionPolicy::compile("validation".to_owned(), extensions.to_vec())
        .map(|policy| policy.extensions)
}

fn valid_extension(extension: &str) -> bool {
    let bytes = extension.as_bytes();
    (2..=MAX_SMALL_FILE_EXTENSION_BYTES).contains(&bytes.len())
        && bytes[0] == b'.'
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-'))
}

#[derive(Clone, Debug)]
struct ReverseSuffixMatcher {
    transitions: Box<[u16]>,
    terminal: Box<[bool]>,
    maximum_length: usize,
}

impl ReverseSuffixMatcher {
    fn compile(extensions: &[String]) -> Self {
        let mut edges = vec![BTreeMap::<u8, usize>::new()];
        let mut terminal = vec![false];
        let mut maximum_length = 0;
        for extension in extensions {
            maximum_length = maximum_length.max(extension.len());
            let mut node = 0;
            for byte in extension.bytes().rev() {
                let next = if let Some(&next) = edges[node].get(&byte) {
                    next
                } else {
                    let next = edges.len();
                    edges.push(BTreeMap::new());
                    terminal.push(false);
                    edges[node].insert(byte, next);
                    next
                };
                node = next;
            }
            terminal[node] = true;
        }

        let mut transitions = vec![NO_TRANSITION; edges.len() * 128];
        for (node, edges) in edges.into_iter().enumerate() {
            for (byte, target) in edges {
                transitions[node * 128 + usize::from(byte)] =
                    u16::try_from(target).expect("ASSERT: bounded suffix trie fits u16");
            }
        }
        Self {
            transitions: transitions.into_boxed_slice(),
            terminal: terminal.into_boxed_slice(),
            maximum_length,
        }
    }

    fn matches(&self, name: &[u8]) -> bool {
        if self.maximum_length == 0 {
            return false;
        }
        let mut node = 0_usize;
        for &byte in name.iter().rev().take(self.maximum_length) {
            if !byte.is_ascii() {
                return false;
            }
            let next = self.transitions[node * 128 + usize::from(byte.to_ascii_lowercase())];
            if next == NO_TRANSITION {
                return false;
            }
            node = usize::from(next);
            if self.terminal[node] {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::hint::black_box;
    use std::time::Instant;

    use super::*;

    #[test]
    fn canonicalizes_and_matches_case_insensitive_multi_dot_suffixes() {
        let policy = SmallFileExtensionPolicy::compile(
            "settings-2".to_owned(),
            vec![".XML".to_owned(), ".tar.gz".to_owned(), ".xml".to_owned()],
        )
        .expect("compile policy");

        assert_eq!(policy.extensions, [".tar.gz", ".xml"]);
        assert!(policy.matches_name(b"inventory.Xml"));
        assert!(policy.matches_name(b"archive.TAR.GZ"));
        assert!(!policy.matches_name(b"archive.gz"));
        assert!(!policy.matches_name(b"xml"));
    }

    #[test]
    fn rejects_unbounded_or_path_like_extensions() {
        assert!(validate_small_file_extensions(&["xml".to_owned()]).is_err());
        assert!(validate_small_file_extensions(&["../xml".to_owned()]).is_err());
        assert!(validate_small_file_extensions(&[format!(".{}", "x".repeat(32))]).is_err());
        assert!(
            validate_small_file_extensions(
                &(0..=MAX_SMALL_FILE_EXTENSIONS)
                    .map(|index| format!(".x{index}"))
                    .collect::<Vec<_>>()
            )
            .is_err()
        );
    }

    #[test]
    #[ignore = "release-mode microbenchmark; run explicitly for placement-policy changes"]
    fn dynamic_matcher_hot_path_benchmark() {
        const ITERATIONS: u32 = 20_000_000;
        let names: [&[u8]; 4] = [
            b"inventory-000042.XML",
            b"payload-000042.bin",
            b"catalog-000042.json",
            b"archive-000042.tar.gz",
        ];
        let policy = SmallFileExtensionPolicy::default();
        let baseline = |name: &[u8]| {
            name.get(name.len().saturating_sub(4)..)
                .is_some_and(|suffix| suffix.eq_ignore_ascii_case(b".xml"))
                || name
                    .get(name.len().saturating_sub(5)..)
                    .is_some_and(|suffix| suffix.eq_ignore_ascii_case(b".json"))
        };

        let started = Instant::now();
        let mut baseline_matches = 0_usize;
        for index in 0..ITERATIONS {
            let index = usize::try_from(index & 3).expect("bounded fixture index");
            baseline_matches += usize::from(black_box(baseline(black_box(names[index]))));
        }
        let baseline_elapsed = started.elapsed();

        let started = Instant::now();
        let mut dynamic_matches = 0_usize;
        for index in 0..ITERATIONS {
            let index = usize::try_from(index & 3).expect("bounded fixture index");
            dynamic_matches += usize::from(black_box(policy.matches_name(black_box(names[index]))));
        }
        let dynamic_elapsed = started.elapsed();
        assert_eq!(dynamic_matches, baseline_matches);
        eprintln!(
            "small_file_suffix_match baseline_ps_per_op={} dynamic_ps_per_op={} ratio_milli={}",
            baseline_elapsed.as_nanos() * 1_000 / u128::from(ITERATIONS),
            dynamic_elapsed.as_nanos() * 1_000 / u128::from(ITERATIONS),
            dynamic_elapsed.as_nanos() * 1_000 / baseline_elapsed.as_nanos(),
        );
    }
}
