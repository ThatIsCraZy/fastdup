use std::env;
use std::io;
use std::path::PathBuf;

use fastdup_store::ContainerStore;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let root = arguments.next().map_or_else(default_store, PathBuf::from);
    if arguments.next().is_some() {
        return Err("usage: audit_container_store [store-directory]".into());
    }

    let store = ContainerStore::open(&root)?;
    let summaries = store.verify_published()?;
    let chunks = summaries.iter().try_fold(0_usize, |total, summary| {
        total
            .checked_add(summary.chunk_count())
            .ok_or_else(|| io::Error::other("chunk counter overflow"))
    })?;
    let file_bytes = summaries.iter().try_fold(0_u64, |total, summary| {
        total
            .checked_add(summary.file_length())
            .ok_or_else(|| io::Error::other("file-byte counter overflow"))
    })?;

    println!("result=PASS");
    println!("verification=FULL_CONTAINER_AUDIT");
    println!("containers={}", summaries.len());
    println!("chunks={chunks}");
    println!("file_bytes={file_bytes}");
    println!("retained_payload_bytes=0");
    println!("store={}", root.display());
    Ok(())
}

fn default_store() -> PathBuf {
    PathBuf::from("/source/fastdup/.artifacts/tier-data/iso-raw-ingest-v1")
}
