use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;

use fastdup_format::{
    DurableInode, ExactIndexProfileId, ManifestExtent, ManifestLeaf, NamespaceEntry, NamespaceRoot,
    PolicySetId,
};
use fastdup_store::{
    ContainerRepository, ExactIndexRunRepository, FsStorageIo, GenerationRepository,
    MaintenanceRepository,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let mode = arguments.next().expect("mode");
    let mode = mode.to_str().expect("ASCII mode").to_owned();
    let root = PathBuf::from(arguments.next().expect("root"));
    let policy = PolicySetId::new([0x11; 32]).expect("fixture policy");
    let profile = ExactIndexProfileId::new([0x22; 32]).expect("fixture profile");
    let storage = FsStorageIo::open(&root)?;
    let generations = GenerationRepository::new(storage.clone(), policy);
    if mode == "create" {
        let files: u64 = arguments
            .next()
            .expect("files")
            .to_str()
            .expect("ASCII")
            .parse()?;
        let commits: usize = arguments
            .next()
            .expect("commits")
            .to_str()
            .expect("ASCII")
            .parse()?;
        let _ = std::fs::remove_dir_all(&root);
        let storage = FsStorageIo::open(&root)?;
        let generations = GenerationRepository::new(storage.clone(), policy);
        let reservation = NamespaceRoot::new(files + 16, 2, 0, Vec::new(), Vec::new())?;
        generations.commit_namespace(&reservation)?;
        let manifest = ManifestLeaf::new(
            4_096,
            vec![ManifestExtent::Fill {
                logical_length: 4_096,
                value: 0x5a,
            }],
        )?;
        let manifest_root = generations.publish_manifest(&manifest)?;
        let inodes = (2..=files + 1)
            .map(|inode| DurableInode::new(inode, 0o644, 0, 0, 1, 1, 4_096, manifest_root))
            .collect::<Result<Vec<_>, _>>()?;
        let entries = (2..=files + 1)
            .map(|inode| NamespaceEntry::new(1, inode, format!("file-{inode:08}").into_bytes()))
            .collect::<Result<Vec<_>, _>>()?;
        let mut previous_sequence = 0_u64;
        for sequence in 1..=u64::try_from(commits)? {
            let namespace = NamespaceRoot::new(
                files + 16,
                files + 2,
                sequence,
                inodes.clone(),
                entries.clone(),
            )?;
            generations.commit_namespace(&namespace)?;
            previous_sequence = sequence;
        }
        assert_eq!(usize::try_from(previous_sequence)?, commits);
        return Ok(());
    }
    if mode == "gc" {
        let maintenance = MaintenanceRepository::new(
            generations.clone(),
            ContainerRepository::new(storage.clone()),
            ExactIndexRunRepository::new(storage.clone()),
            profile,
        );
        let started = Instant::now();
        let first = maintenance.garbage_collect_metadata()?;
        let first_wall = started.elapsed();
        let started = Instant::now();
        let second = maintenance.garbage_collect_metadata()?;
        let second_wall = started.elapsed();
        println!(
            "first_wall_us={} first_exact={} first_retained={} first_graph_bytes={} second_wall_us={} second_exact={} second_mark_mode={:?}",
            first_wall.as_micros(),
            first.exact_mark_performed(),
            first.objects_retained(),
            first.metrics().object_graph_read_bytes(),
            second_wall.as_micros(),
            second.exact_mark_performed(),
            second.mark_mode()
        );
        return Ok(());
    }
    Err("mode must be create or gc".into())
}
