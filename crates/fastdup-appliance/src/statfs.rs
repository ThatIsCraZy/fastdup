use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Weak};
use std::thread;
use std::time::Duration;

use fastdup_posix::{StatFsSnapshot, StatFsSource};

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
}

impl TieredStatFsSource {
    /// Opens a cached tier-capacity source.
    ///
    /// Physical capacity is sampled before this function returns and refreshed
    /// every five seconds by one dedicated thread. An explicit override keeps
    /// one fixed snapshot and starts no refresher.
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
        let data_root = data_root.into();
        let metadata_root = metadata_root.into();
        let initial = sample_paths(&data_root, &metadata_root, capacity_override)?;
        let snapshot = Arc::new(RwLock::new(initial));
        if capacity_override.is_none() {
            spawn_refresher(&snapshot, data_root, metadata_root)?;
        }
        Ok(Self { snapshot })
    }
}

fn sample_paths(
    data_root: &Path,
    metadata_root: &Path,
    capacity_override: Option<StatFsOverride>,
) -> io::Result<StatFsSnapshot> {
    let metadata = TierCapacity::read(metadata_root)?;
    let data = if capacity_override.is_some() {
        None
    } else {
        Some(TierCapacity::read(data_root)?)
    };
    snapshot_from_tiers(data, metadata, capacity_override)
}

fn spawn_refresher(
    snapshot: &Arc<RwLock<StatFsSnapshot>>,
    data_root: PathBuf,
    metadata_root: PathBuf,
) -> io::Result<()> {
    let weak_snapshot = Arc::downgrade(snapshot);
    thread::Builder::new()
        .name("fastdup-statfs".to_owned())
        .spawn(move || refresh_loop(&weak_snapshot, &data_root, &metadata_root))?;
    Ok(())
}

fn refresh_loop(
    weak_snapshot: &Weak<RwLock<StatFsSnapshot>>,
    data_root: &Path,
    metadata_root: &Path,
) {
    loop {
        thread::sleep(STATFS_REFRESH_INTERVAL);
        let Some(snapshot) = weak_snapshot.upgrade() else {
            return;
        };
        let sampled = sample_paths(data_root, metadata_root, None);
        let Ok(mut cached) = snapshot.write() else {
            return;
        };
        *cached = match sampled {
            Ok(sampled) => sampled,
            Err(_) => unavailable_snapshot(*cached),
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

impl StatFsSource for TieredStatFsSource {
    fn snapshot(&self) -> io::Result<StatFsSnapshot> {
        self.snapshot
            .read()
            .map(|snapshot| *snapshot)
            .map_err(|_| io::Error::other("cached statfs capacity lock is poisoned"))
    }
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
    fn explicit_override_controls_capacity_without_sampling_the_data_tier() {
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
}
