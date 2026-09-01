use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use fastdup_format::ContainerId;
use fastdup_store::{
    ContainerPlacement, ContainerRepository, FsStorageIo, StorageIo, TieredStorageIo,
};

fn test_root(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".artifacts/tests")
        .join(format!("{name}-{}", std::process::id()))
}

#[test]
fn placement_is_hidden_behind_one_recovery_and_read_seam() {
    let root = test_root("tiered-container-placement");
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("remove only this test's prior artifact");
    }
    let data = FsStorageIo::open(root.join("data")).expect("open DATA fixture");
    let small = FsStorageIo::open(root.join("small")).expect("open Small-File fixture");
    let storage = TieredStorageIo::new(data.clone(), small.clone());
    let repository = ContainerRepository::new(storage.clone());

    let data_payload = vec![0x41; 32 * 1_024];
    repository
        .publish_adaptive_regions(
            ContainerId::new([0x31; 16]).expect("nonzero DATA Container ID"),
            1,
            &[&[data_payload.as_slice()]],
        )
        .expect("publish ordinary DATA Container");

    let small_payload = vec![0x42; 24 * 1_024];
    let prepared = ContainerRepository::<TieredStorageIo<FsStorageIo, FsStorageIo>>::
        prepare_adaptive_regions_parallel(
            ContainerId::new([0x32; 16]).expect("nonzero Small-File Container ID"),
            2,
            &[&[small_payload.as_slice()]],
            NonZeroUsize::new(1).expect("one worker"),
        )
        .expect("prepare Small-File Container");
    repository
        .publish_prepared_adaptive_profiled_with_placement(prepared, ContainerPlacement::SmallFile)
        .expect("publish Small-File Container");

    assert_eq!(
        data.list_names()
            .expect("list DATA")
            .iter()
            .filter(|name| {
                std::path::Path::new(name).extension() == Some(std::ffi::OsStr::new("fdc"))
            })
            .count(),
        1
    );
    assert_eq!(
        small
            .list_names()
            .expect("list Small-File")
            .iter()
            .filter(|name| {
                std::path::Path::new(name).extension() == Some(std::ffi::OsStr::new("fdc"))
            })
            .count(),
        1
    );

    assert_eq!(
        repository
            .verify_published()
            .expect("audit both tiers")
            .len(),
        2
    );
    assert_eq!(
        repository
            .read(ContainerId::new([0x32; 16]).expect("Small-File identity"))
            .expect("read through tier-neutral Container repository")
            .records()[0]
            .payload(),
        small_payload
    );

    std::fs::remove_dir_all(root).expect("remove only this test repository");
}
