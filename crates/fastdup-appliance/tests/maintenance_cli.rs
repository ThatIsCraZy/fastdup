use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use fastdup_appliance::{
    APPLIANCE_RECOVERY_LATCH_FILE_NAME, AppliancePoolBinding, checkpoint_policy_set,
};
use fastdup_format::{ManifestExtent, ManifestLeaf, MetadataObjectId, NamespaceRoot};
use fastdup_store::{FsStorageIo, GenerationRepository};

fn unique_test_root(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after the Unix epoch")
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}-{nonce}", std::process::id()))
}

fn metadata_path(root: &Path, object_id: MetadataObjectId) -> PathBuf {
    let mut name = String::with_capacity(64 + ".fdm".len());
    for byte in object_id.bytes() {
        write!(&mut name, "{byte:02x}").expect("writing to a String cannot fail");
    }
    name.push_str(".fdm");
    root.join(name)
}

fn initialize_pool_binding(metadata_root: &Path, data_root: &Path) {
    let metadata = FsStorageIo::open(metadata_root).expect("open Metadata Pool");
    let data = FsStorageIo::open(data_root).expect("open Data Pool");
    AppliancePoolBinding::initialize_or_open(&metadata, &data)
        .expect("initialize current Pool identities");
}

#[test]
fn offline_metadata_gc_cli_removes_an_orphan_and_scrubs_the_retained_graph() {
    let root = unique_test_root("metadata-gc-cli");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    initialize_pool_binding(&metadata_root, &container_root);

    let generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("create workspace-local metadata repository"),
        checkpoint_policy_set(),
    );
    generations
        .commit_namespace(
            &NamespaceRoot::new(4_096, 2, 0, Vec::new(), Vec::new())
                .expect("empty namespace root is valid"),
        )
        .expect("commit the retained namespace graph");
    let orphan = generations
        .publish_manifest(
            &ManifestLeaf::new(
                4_096,
                vec![ManifestExtent::Fill {
                    logical_length: 4_096,
                    value: 0xA5,
                }],
            )
            .expect("fill-only orphan manifest is valid"),
        )
        .expect("publish a valid but uncommitted Metadata object");
    let orphan_path = metadata_path(&metadata_root, orphan);
    assert!(
        orphan_path.is_file(),
        "ASSERT: published orphan must exist before Metadata GC"
    );
    drop(generations);

    let output = Command::new(env!("CARGO_BIN_EXE_fastdup-maintenance"))
        .args([
            "--offline",
            "metadata-gc",
            metadata_root
                .to_str()
                .expect("workspace-local metadata path is UTF-8"),
            container_root
                .to_str()
                .expect("workspace-local container path is UTF-8"),
        ])
        .output()
        .expect("execute the real maintenance CLI");
    assert!(
        output.status.success(),
        "ASSERT: Metadata GC CLI failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("CLI output is UTF-8");
    assert!(
        stdout.contains("metadata_gc_ok=true objects_removed=1"),
        "ASSERT: CLI must report exactly the injected orphan: {stdout}"
    );
    assert!(
        stdout.contains("mark_mode=exact_snapshot"),
        "ASSERT: CLI must distinguish the exact Metadata mark mode: {stdout}"
    );
    assert!(
        stdout.contains("exact_reason=process_start")
            && stdout.contains("object_graph_read_bytes=")
            && stdout.contains("catalog_write_bytes=")
            && stdout.contains("wall_us="),
        "ASSERT: CLI must expose Metadata-GC reason, work, and latency: {stdout}"
    );
    assert!(
        stdout.contains("scrub_ok=true"),
        "ASSERT: CLI must scrub the retained graph after collection: {stdout}"
    );
    assert!(
        !orphan_path.exists(),
        "ASSERT: Metadata GC must unlink the injected orphan"
    );
}

#[test]
fn offline_pool_rebuild_cli_publishes_a_coherent_exact_similarity_pair() {
    let root = unique_test_root("pool-rebuild-cli");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    initialize_pool_binding(&metadata_root, &container_root);
    let generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("create metadata repository"),
        checkpoint_policy_set(),
    );
    generations
        .commit_namespace(
            &NamespaceRoot::new(4_096, 2, 0, Vec::new(), Vec::new())
                .expect("empty namespace root is valid"),
        )
        .expect("commit a rebuildable empty pool");
    drop(generations);

    let output = Command::new(env!("CARGO_BIN_EXE_fastdup-maintenance"))
        .args([
            "--offline",
            "rebuild-pool-indexes",
            metadata_root
                .to_str()
                .expect("workspace-local metadata path is UTF-8"),
            container_root
                .to_str()
                .expect("workspace-local container path is UTF-8"),
        ])
        .output()
        .expect("execute the real paired-rebuild CLI");
    assert!(
        output.status.success(),
        "ASSERT: paired rebuild CLI failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("CLI output is UTF-8");
    assert!(
        stdout.contains("pool_rebuild_ok=true")
            && stdout.contains("similarity_generation=")
            && stdout.contains("similarity_entries=0")
            && stdout.contains("source_exact_run_set_id="),
        "ASSERT: paired rebuild identity and work are observable: {stdout}"
    );
    assert!(stdout.contains("scrub_ok=true"));
}

#[test]
fn recovery_required_repository_allows_scrub_before_other_offline_mutation() {
    let root = unique_test_root("recovery-required-cli");
    let metadata_root = root.join("metadata");
    let container_root = root.join("containers");
    initialize_pool_binding(&metadata_root, &container_root);
    let generations = GenerationRepository::new(
        FsStorageIo::open(&metadata_root).expect("create metadata repository"),
        checkpoint_policy_set(),
    );
    generations
        .commit_namespace(
            &NamespaceRoot::new(4_096, 2, 0, Vec::new(), Vec::new())
                .expect("empty namespace root is valid"),
        )
        .expect("commit one scrub-valid generation");
    drop(generations);
    let latch = metadata_root.join(APPLIANCE_RECOVERY_LATCH_FILE_NAME);
    std::fs::write(&latch, []).expect("arm empty recovery latch");
    std::fs::File::open(&metadata_root)
        .expect("open metadata directory")
        .sync_all()
        .expect("sync recovery latch name");

    let run = |command: &str| {
        Command::new(env!("CARGO_BIN_EXE_fastdup-maintenance"))
            .args([
                "--offline",
                command,
                metadata_root
                    .to_str()
                    .expect("workspace-local metadata path is UTF-8"),
                container_root
                    .to_str()
                    .expect("workspace-local container path is UTF-8"),
            ])
            .output()
            .expect("execute real maintenance CLI")
    };

    let blocked = run("rebuild-exact");
    assert!(!blocked.status.success());
    assert!(
        String::from_utf8_lossy(&blocked.stderr)
            .contains("recovery-required repository needs a successful offline scrub"),
        "ASSERT: unsafe offline mutation reports the required proof: {}",
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        latch.exists(),
        "ASSERT: rejected mutation retains the latch"
    );

    let scrubbed = run("scrub");
    assert!(
        scrubbed.status.success(),
        "ASSERT: scrub clears recovery requirement: {}",
        String::from_utf8_lossy(&scrubbed.stderr)
    );
    assert!(!latch.exists(), "ASSERT: successful scrub clears the latch");

    let rebuilt = run("rebuild-exact");
    assert!(
        rebuilt.status.success(),
        "ASSERT: offline mutation proceeds after scrub: {}",
        String::from_utf8_lossy(&rebuilt.stderr)
    );
}
