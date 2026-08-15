use std::path::{Path, PathBuf};

use fastdup_testkit::generate_structured_corpus;

const EXPECTED_FILES: [&str; 6] = [
    "inventory-v1.json",
    "inventory-v1.xml",
    "inventory-v2.json",
    "inventory-v2.xml",
    "inventory-v3.json",
    "inventory-v3.xml",
];

fn test_root(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}", std::process::id()))
}

#[test]
fn structured_corpus_is_deterministic_validly_framed_and_bounded() {
    let first = test_root("structured-corpus-a");
    let second = test_root("structured-corpus-b");
    for root in [&first, &second] {
        if root.exists() {
            std::fs::remove_dir_all(root).expect("remove only this test's prior artifact");
        }
        generate_structured_corpus(root).expect("generate deterministic structured fixtures");
    }

    let mut names = std::fs::read_dir(&first)
        .expect("list first corpus")
        .map(|entry| {
            entry
                .expect("read fixture entry")
                .file_name()
                .into_string()
                .expect("fixture name is UTF-8")
        })
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(names, EXPECTED_FILES);

    for name in EXPECTED_FILES {
        let first_bytes = std::fs::read(first.join(name)).expect("read first fixture");
        let second_bytes = std::fs::read(second.join(name)).expect("read second fixture");
        assert_eq!(
            first_bytes, second_bytes,
            "fixture {name} is not deterministic"
        );
        assert!(!first_bytes.is_empty());
        assert!(
            first_bytes.len() <= 800 * 1_024,
            "fixture {name} is too large"
        );
        if Path::new(name)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            assert!(first_bytes.starts_with(b"[\n"));
            assert!(first_bytes.ends_with(b"\n]\n"));
        } else {
            assert!(first_bytes.starts_with(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"));
            assert!(first_bytes.ends_with(b"</inventory>\n"));
        }
    }
}
