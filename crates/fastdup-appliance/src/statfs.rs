use std::collections::BTreeMap;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Weak};
use std::thread;
use std::time::Duration;

use fastdup_posix::{InodeId, Namespace, StatFsSnapshot, StatFsSource};

use crate::{CommitCapacityGovernor, CommitCapacitySnapshot};

pub const STATFS_RESERVE_BASIS_POINTS: u64 = 1_000;
const BASIS_POINTS: u64 = 10_000;
const STATFS_BLOCK_BYTES: u32 = 4_096;
const MAXIMUM_NAME_BYTES: u32 = 255;
const STATFS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatFsOverride {
    capacity_bytes: u64,
    available_bytes: u64,
}

impl StatFsOverride {
    /// Creates an explicit client-visible capacity override.
    ///
    /// The override affects reporting only. It does not reserve storage or
    /// bypass the appliance's physical mutation admission.
    ///
    /// # Errors
    ///
    /// Returns an error when capacity is zero or availability exceeds it.
    pub const fn new(
        capacity_bytes: u64,
        available_bytes: u64,
    ) -> Result<Self, StatFsOverrideError> {
        if capacity_bytes == 0 || available_bytes > capacity_bytes {
            return Err(StatFsOverrideError);
        }
        Ok(Self {
            capacity_bytes,
            available_bytes,
        })
    }

    #[must_use]
    pub const fn capacity_bytes(self) -> u64 {
        self.capacity_bytes
    }

    #[must_use]
    pub const fn available_bytes(self) -> u64 {
        self.available_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatFsOverrideError;

impl std::fmt::Display for StatFsOverrideError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("statfs override requires 0 <= available <= nonzero capacity")
    }
}

impl std::error::Error for StatFsOverrideError {}

#[derive(Clone, Debug)]
pub struct TieredStatFsSource {
    snapshot: Arc<RwLock<StatFsSnapshot>>,
    governor: Arc<CommitCapacityGovernor>,
    presented_capacities: Arc<RwLock<PresentedCapacities>>,
    logical_quota_namespace: Arc<RwLock<Option<Weak<Namespace>>>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PresentedCapacities {
    revision: String,
    by_inode: BTreeMap<u64, u64>,
}

#[derive(Clone, Debug)]
struct SmallFileCapacitySource {
    root: PathBuf,
    hard_limit_bytes: u64,
}

impl TieredStatFsSource {
    /// Opens a cached tier-capacity source.
    ///
    /// Physical capacity is sampled before this function returns and refreshed
    /// every five seconds by one dedicated thread. An explicit reporting
    /// override keeps the client snapshot fixed while physical observations
    /// continue feeding mutation admission.
    ///
    /// # Errors
    ///
    /// Returns an error when either initial backing-filesystem observation or
    /// refresher-thread creation fails.
    pub fn open(
        data_root: impl Into<PathBuf>,
        metadata_root: impl Into<PathBuf>,
        capacity_override: Option<StatFsOverride>,
    ) -> io::Result<Self> {
        Self::open_inner(data_root, metadata_root, capacity_override, None)
    }

    /// Opens the physical capacity source with a separately bounded Small-File
    /// project. Its cached headroom participates in synchronous mutation
    /// admission while the client-visible `statfs` view remains DATA-based.
    ///
    /// # Errors
    ///
    /// Returns invalid limits, filesystem sampling failures, a Metadata tier
    /// without the protected commit floor, or refresher-thread spawn failures.
    pub fn open_with_small_file_tier(
        data_root: impl Into<PathBuf>,
        metadata_root: impl Into<PathBuf>,
        small_file_root: impl Into<PathBuf>,
        small_file_hard_limit_bytes: u64,
        capacity_override: Option<StatFsOverride>,
    ) -> io::Result<Self> {
        if small_file_hard_limit_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Small-File hard limit must be nonzero",
            ));
        }
        Self::open_inner(
            data_root,
            metadata_root,
            capacity_override,
            Some(SmallFileCapacitySource {
                root: small_file_root.into(),
                hard_limit_bytes: small_file_hard_limit_bytes,
            }),
        )
    }

    fn open_inner(
        data_root: impl Into<PathBuf>,
        metadata_root: impl Into<PathBuf>,
        capacity_override: Option<StatFsOverride>,
        small_file: Option<SmallFileCapacitySource>,
    ) -> io::Result<Self> {
        let data_root = data_root.into();
        let metadata_root = metadata_root.into();
        let (initial, capacity) = sample_paths(
            &data_root,
            &metadata_root,
            capacity_override,
            small_file.as_ref(),
        )?;
        let governor = Arc::new(
            CommitCapacityGovernor::new(capacity)
                .map_err(|error| io::Error::new(io::ErrorKind::StorageFull, error))?,
        );
        let snapshot = Arc::new(RwLock::new(initial));
        spawn_refresher(
            &snapshot,
            &governor,
            data_root,
            metadata_root,
            capacity_override,
            small_file,
        )?;
        Ok(Self {
            snapshot,
            governor,
            presented_capacities: Arc::new(RwLock::new(PresentedCapacities::default())),
            logical_quota_namespace: Arc::new(RwLock::new(None)),
        })
    }

    /// Connects Share `statfs` reporting to the synchronous logical quota
    /// ledger owned by the POSIX namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when an earlier invariant violation poisoned the
    /// attachment lock.
    pub fn attach_logical_quota_namespace(&self, namespace: &Arc<Namespace>) -> io::Result<()> {
        *self
            .logical_quota_namespace
            .write()
            .map_err(|_| io::Error::other("logical-quota namespace lock is poisoned"))? =
            Some(Arc::downgrade(namespace));
        Ok(())
    }

    /// Returns the admission governor fed by the same physical observations as
    /// client-visible `statfs` reporting.
    #[must_use]
    pub fn commit_capacity_governor(&self) -> Arc<CommitCapacityGovernor> {
        Arc::clone(&self.governor)
    }

    /// Atomically replaces reporting-only capacities for managed Share roots.
    ///
    /// Physical observations and mutation admission remain unchanged.
    ///
    /// # Errors
    ///
    /// Rejects an invalid revision, a zero inode or capacity, duplicate inode
    /// rules, or a poisoned policy lock.
    pub fn replace_presented_capacities(
        &self,
        revision: String,
        rules: impl IntoIterator<Item = (u64, u64)>,
    ) -> io::Result<()> {
        if revision.is_empty() || revision.len() > 128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "presented-capacity revision is invalid",
            ));
        }
        let mut by_inode = BTreeMap::new();
        for (inode, capacity_bytes) in rules {
            if inode == 0 || capacity_bytes == 0 || by_inode.insert(inode, capacity_bytes).is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "presented-capacity rules require unique nonzero inodes and capacities",
                ));
            }
        }
        let mut current = self
            .presented_capacities
            .write()
            .map_err(|_| io::Error::other("presented-capacity lock is poisoned"))?;
        *current = PresentedCapacities { revision, by_inode };
        Ok(())
    }

    /// Returns the revision of the currently active Share presentation rules.
    ///
    /// # Errors
    ///
    /// Returns an error when the policy lock is poisoned.
    pub fn presented_capacity_revision(&self) -> io::Result<String> {
        self.presented_capacities
            .read()
            .map(|capacities| capacities.revision.clone())
            .map_err(|_| io::Error::other("presented-capacity lock is poisoned"))
    }
}

fn sample_paths(
    data_root: &Path,
    metadata_root: &Path,
    capacity_override: Option<StatFsOverride>,
    small_file: Option<&SmallFileCapacitySource>,
) -> io::Result<(StatFsSnapshot, CommitCapacitySnapshot)> {
    let metadata = TierCapacity::read(metadata_root)?;
    let data = TierCapacity::read(data_root)?;
    Ok((
        snapshot_from_tiers(Some(data), metadata, capacity_override)?,
        commit_capacity_from_tiers(data, metadata)?.with_small_file_available_bytes(
            small_file.map_or(Ok(u64::MAX), small_file_available_bytes)?,
        ),
    ))
}

fn spawn_refresher(
    snapshot: &Arc<RwLock<StatFsSnapshot>>,
    governor: &Arc<CommitCapacityGovernor>,
    data_root: PathBuf,
    metadata_root: PathBuf,
    capacity_override: Option<StatFsOverride>,
    small_file: Option<SmallFileCapacitySource>,
) -> io::Result<()> {
    let weak_snapshot = Arc::downgrade(snapshot);
    let weak_governor = Arc::downgrade(governor);
    thread::Builder::new()
        .name("fastdup-statfs".to_owned())
        .spawn(move || {
            refresh_loop(
                &weak_snapshot,
                &weak_governor,
                &data_root,
                &metadata_root,
                capacity_override,
                small_file.as_ref(),
            );
        })?;
    Ok(())
}

fn refresh_loop(
    weak_snapshot: &Weak<RwLock<StatFsSnapshot>>,
    weak_governor: &Weak<CommitCapacityGovernor>,
    data_root: &Path,
    metadata_root: &Path,
    capacity_override: Option<StatFsOverride>,
    small_file: Option<&SmallFileCapacitySource>,
) {
    loop {
        thread::sleep(STATFS_REFRESH_INTERVAL);
        let Some(snapshot) = weak_snapshot.upgrade() else {
            return;
        };
        let Some(governor) = weak_governor.upgrade() else {
            return;
        };
        let observation_epoch = governor.begin_observation();
        let sampled = sample_paths(data_root, metadata_root, capacity_override, small_file);
        let Ok(mut cached) = snapshot.write() else {
            return;
        };
        *cached = if let Ok((sampled, capacity)) = sampled {
            governor.finish_observation(observation_epoch, capacity);
            sampled
        } else {
            governor.observation_failed(observation_epoch);
            unavailable_snapshot(*cached)
        };
    }
}

fn unavailable_snapshot(snapshot: StatFsSnapshot) -> StatFsSnapshot {
    StatFsSnapshot::new(
        snapshot.capacity_bytes(),
        snapshot.free_bytes(),
        0,
        snapshot.files(),
        snapshot.free_files(),
        snapshot.block_size(),
        snapshot.maximum_name_bytes(),
    )
    .expect("ASSERT: reducing a valid statfs snapshot to zero availability remains valid")
}

fn snapshot_from_tiers(
    data: Option<TierCapacity>,
    metadata: TierCapacity,
    capacity_override: Option<StatFsOverride>,
) -> io::Result<StatFsSnapshot> {
    let (capacity_bytes, free_bytes, available_bytes) =
        if let Some(capacity_override) = capacity_override {
            (
                capacity_override.capacity_bytes(),
                capacity_override.available_bytes(),
                capacity_override.available_bytes(),
            )
        } else {
            let data = data.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "physical statfs reporting requires a data-tier observation",
                )
            })?;
            let data_reserve = reserve_bytes(data.capacity_bytes)?;
            let metadata_reserve = reserve_bytes(metadata.capacity_bytes)?;
            let capacity_bytes = data.capacity_bytes.saturating_sub(data_reserve);
            let free_bytes = data.free_bytes.saturating_sub(data_reserve);
            let available_bytes = if metadata.available_bytes > metadata_reserve {
                data.available_bytes.saturating_sub(data_reserve)
            } else {
                0
            };
            (capacity_bytes, free_bytes, available_bytes)
        };
    StatFsSnapshot::new(
        capacity_bytes,
        free_bytes.min(capacity_bytes),
        available_bytes.min(free_bytes).min(capacity_bytes),
        metadata.files,
        metadata.free_files.min(metadata.files),
        STATFS_BLOCK_BYTES,
        MAXIMUM_NAME_BYTES,
    )
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn commit_capacity_from_tiers(
    data: TierCapacity,
    metadata: TierCapacity,
) -> io::Result<CommitCapacitySnapshot> {
    let data_reserve = reserve_bytes(data.capacity_bytes)?;
    let metadata_reserve = reserve_bytes(metadata.capacity_bytes)?;
    Ok(CommitCapacitySnapshot::new(
        metadata.available_bytes.saturating_sub(metadata_reserve),
        data.available_bytes.saturating_sub(data_reserve),
    ))
}

fn small_file_available_bytes(source: &SmallFileCapacitySource) -> io::Result<u64> {
    let reserve = reserve_bytes(source.hard_limit_bytes)?;
    let usable = source.hard_limit_bytes.saturating_sub(reserve);
    let mut allocated = source
        .root
        .metadata()?
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| io::Error::other("Small-File allocated bytes overflow u64"))?;
    for entry in std::fs::read_dir(&source.root)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Small-File Container root contains a non-file entry",
            ));
        }
        allocated = allocated
            .checked_add(
                metadata
                    .blocks()
                    .checked_mul(512)
                    .ok_or_else(|| io::Error::other("Small-File file blocks overflow u64"))?,
            )
            .ok_or_else(|| io::Error::other("Small-File allocated bytes overflow u64"))?;
    }
    Ok(usable.saturating_sub(allocated))
}

impl StatFsSource for TieredStatFsSource {
    fn snapshot(&self, inode: u64) -> io::Result<StatFsSnapshot> {
        let snapshot = self
            .snapshot
            .read()
            .map(|snapshot| *snapshot)
            .map_err(|_| io::Error::other("cached statfs capacity lock is poisoned"))?;
        let configured_capacity = self
            .presented_capacities
            .read()
            .map_err(|_| io::Error::other("presented-capacity lock is poisoned"))?
            .by_inode
            .get(&inode)
            .copied();
        let quota_status = InodeId::new(inode).and_then(|inode| {
            self.logical_quota_namespace
                .read()
                .ok()
                .and_then(|namespace| namespace.as_ref().and_then(Weak::upgrade))
                .and_then(|namespace| namespace.logical_quota_status_for_inode(inode))
        });
        let capacity = quota_status
            .as_ref()
            .map(|status| status.limit_bytes)
            .or(configured_capacity);
        capacity.map_or(Ok(snapshot), |capacity| {
            let quota_available =
                quota_status.map(|status| status.limit_bytes.saturating_sub(status.used_bytes));
            presented_snapshot(snapshot, capacity, quota_available)
        })
    }
}

fn presented_snapshot(
    snapshot: StatFsSnapshot,
    capacity_bytes: u64,
    quota_available_bytes: Option<u64>,
) -> io::Result<StatFsSnapshot> {
    let quota_available_bytes = quota_available_bytes.unwrap_or(capacity_bytes);
    StatFsSnapshot::new(
        capacity_bytes,
        snapshot
            .free_bytes()
            .min(capacity_bytes)
            .min(quota_available_bytes),
        snapshot
            .available_bytes()
            .min(capacity_bytes)
            .min(quota_available_bytes),
        snapshot.files(),
        snapshot.free_files(),
        snapshot.block_size(),
        snapshot.maximum_name_bytes(),
    )
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TierCapacity {
    capacity_bytes: u64,
    free_bytes: u64,
    available_bytes: u64,
    files: u64,
    free_files: u64,
}

impl TierCapacity {
    fn read(path: &Path) -> io::Result<Self> {
        let statistics = rustix::fs::statvfs(path)?;
        let fragment_bytes = statistics.f_frsize.max(1);
        Ok(Self {
            capacity_bytes: checked_scale(statistics.f_blocks, fragment_bytes)?,
            free_bytes: checked_scale(statistics.f_bfree, fragment_bytes)?,
            available_bytes: checked_scale(statistics.f_bavail, fragment_bytes)?,
            files: statistics.f_files,
            free_files: statistics.f_ffree,
        })
    }
}

fn checked_scale(units: u64, unit_bytes: u64) -> io::Result<u64> {
    units.checked_mul(unit_bytes).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "statvfs byte count overflows u64",
        )
    })
}

fn reserve_bytes(capacity_bytes: u64) -> io::Result<u64> {
    let scaled = u128::from(capacity_bytes) * u128::from(STATFS_RESERVE_BASIS_POINTS);
    let rounded = scaled.div_ceil(u128::from(BASIS_POINTS));
    u64::try_from(rounded)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "statfs reserve overflows u64"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_rejects_zero_or_unbounded_values() {
        assert!(StatFsOverride::new(0, 0).is_err());
        assert!(StatFsOverride::new(1, 2).is_err());
        assert_eq!(
            StatFsOverride::new(10, 9),
            Ok(StatFsOverride {
                capacity_bytes: 10,
                available_bytes: 9,
            })
        );
    }

    #[test]
    fn reserve_rounds_up_without_overflow() {
        assert_eq!(reserve_bytes(1).expect("one-byte capacity"), 1);
        assert_eq!(reserve_bytes(10_000).expect("ten-thousand bytes"), 1_000);
        assert_eq!(
            reserve_bytes(u64::MAX).expect("u64 capacity"),
            1_844_674_407_370_955_162
        );
    }

    #[test]
    fn physical_reporting_deducts_reserve_and_uses_metadata_as_a_gate() {
        let data = TierCapacity {
            capacity_bytes: 100_000,
            free_bytes: 80_000,
            available_bytes: 70_000,
            files: 0,
            free_files: 0,
        };
        let healthy_metadata = TierCapacity {
            capacity_bytes: 10_000,
            free_bytes: 9_000,
            available_bytes: 2_000,
            files: 100,
            free_files: 90,
        };
        let snapshot = snapshot_from_tiers(Some(data), healthy_metadata, None)
            .expect("healthy physical capacity");
        assert_eq!(snapshot.capacity_bytes(), 90_000);
        assert_eq!(snapshot.free_bytes(), 70_000);
        assert_eq!(snapshot.available_bytes(), 60_000);
        assert_eq!((snapshot.files(), snapshot.free_files()), (100, 90));

        let exhausted_metadata = TierCapacity {
            available_bytes: 1_000,
            ..healthy_metadata
        };
        let snapshot = snapshot_from_tiers(Some(data), exhausted_metadata, None)
            .expect("metadata reserve gate");
        assert_eq!(snapshot.free_bytes(), 70_000);
        assert_eq!(snapshot.available_bytes(), 0);
    }

    #[test]
    fn explicit_override_controls_only_the_reported_capacity() {
        let metadata = TierCapacity {
            capacity_bytes: 10_000,
            free_bytes: 0,
            available_bytes: 0,
            files: 100,
            free_files: 20,
        };
        let capacity_override = StatFsOverride::new(1_000_000, 750_000).expect("valid override");
        let snapshot = snapshot_from_tiers(None, metadata, Some(capacity_override))
            .expect("explicit capacity override");
        assert_eq!(snapshot.capacity_bytes(), 1_000_000);
        assert_eq!(snapshot.free_bytes(), 750_000);
        assert_eq!(snapshot.available_bytes(), 750_000);
        assert_eq!((snapshot.files(), snapshot.free_files()), (100, 20));
    }

    #[test]
    fn failed_refresh_keeps_geometry_but_closes_available_capacity() {
        let snapshot = StatFsSnapshot::new(10_000, 8_000, 7_000, 100, 90, 4_096, 255)
            .expect("valid fixture snapshot");
        let unavailable = unavailable_snapshot(snapshot);
        assert_eq!(unavailable.capacity_bytes(), 10_000);
        assert_eq!(unavailable.free_bytes(), 8_000);
        assert_eq!(unavailable.available_bytes(), 0);
        assert_eq!((unavailable.files(), unavailable.free_files()), (100, 90));
    }

    #[test]
    fn presented_capacity_changes_geometry_without_inventing_availability() {
        let physical = StatFsSnapshot::new(100_000, 80_000, 70_000, 100, 90, 4_096, 255)
            .expect("valid physical snapshot");
        let smaller = presented_snapshot(physical, 25_000, None).expect("smaller presentation");
        assert_eq!(smaller.capacity_bytes(), 25_000);
        assert_eq!(smaller.free_bytes(), 25_000);
        assert_eq!(smaller.available_bytes(), 25_000);

        let larger = presented_snapshot(physical, 1_000_000, None).expect("larger presentation");
        assert_eq!(larger.capacity_bytes(), 1_000_000);
        assert_eq!(larger.free_bytes(), 80_000);
        assert_eq!(larger.available_bytes(), 70_000);
    }

    #[test]
    fn share_root_inode_selects_one_hot_replaceable_presentation() {
        let snapshot = StatFsSnapshot::new(100_000, 80_000, 70_000, 100, 90, 4_096, 255)
            .expect("valid physical snapshot");
        let source = TieredStatFsSource {
            snapshot: Arc::new(RwLock::new(snapshot)),
            governor: Arc::new(
                CommitCapacityGovernor::new(CommitCapacitySnapshot::new(
                    128 * 1_024 * 1_024,
                    1_000_000,
                ))
                .expect("valid test governor"),
            ),
            presented_capacities: Arc::new(RwLock::new(PresentedCapacities::default())),
            logical_quota_namespace: Arc::new(RwLock::new(None)),
        };
        source
            .replace_presented_capacities("shares-r1".to_owned(), [(42, 25_000)])
            .expect("install presented capacity");
        assert_eq!(
            StatFsSource::snapshot(&source, 42)
                .expect("share-root snapshot")
                .capacity_bytes(),
            25_000
        );
        assert_eq!(
            StatFsSource::snapshot(&source, 7)
                .expect("repository snapshot")
                .capacity_bytes(),
            100_000
        );
        assert_eq!(
            source
                .presented_capacity_revision()
                .expect("presented-capacity revision"),
            "shares-r1"
        );
    }
}
