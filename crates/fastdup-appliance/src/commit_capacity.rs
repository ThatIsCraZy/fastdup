use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use fastdup_posix::{CommitCapacityAdmission, CommitCapacityClaim, CommitToken, PosixError};

/// Capacity permanently withheld for one bounded Metadata commit and cleanup.
pub const COMMIT_METADATA_FLOOR_BYTES_V1: u64 = 64 * 1_024 * 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitCapacitySnapshot {
    metadata_available_bytes: u64,
    data_available_bytes: u64,
}

impl CommitCapacitySnapshot {
    #[must_use]
    pub const fn new(metadata_available_bytes: u64, data_available_bytes: u64) -> Self {
        Self {
            metadata_available_bytes,
            data_available_bytes,
        }
    }

    #[must_use]
    pub const fn metadata_available_bytes(self) -> u64 {
        self.metadata_available_bytes
    }

    #[must_use]
    pub const fn data_available_bytes(self) -> u64 {
        self.data_available_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitCapacityConfigurationError;

impl fmt::Display for CommitCapacityConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Metadata capacity cannot protect one bounded commit floor")
    }
}

impl std::error::Error for CommitCapacityConfigurationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FrozenClaim {
    metadata_bytes: u64,
    data_bytes: u64,
    completed_after_observation: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct UncheckpointedDataClaim {
    data_bytes: u64,
    completed_after_observation: Option<u64>,
}

/// Lock-free request-path admission against cached physical tier headroom.
///
/// Only Commit Cut rotation and capacity observation take the generation
/// mutex. A write performs bounded compare/exchange accounting and no syscall.
#[derive(Debug)]
pub struct CommitCapacityGovernor {
    metadata_limit: AtomicU64,
    data_limit: AtomicU64,
    metadata_reserved: AtomicU64,
    data_reserved: AtomicU64,
    active_metadata: AtomicU64,
    active_data: AtomicU64,
    observation_epoch: AtomicU64,
    frozen: Mutex<BTreeMap<CommitToken, FrozenClaim>>,
    uncheckpointed_data: Mutex<UncheckpointedDataClaim>,
}

impl CommitCapacityGovernor {
    /// Creates admission state from one physical, post-operating-reserve
    /// observation.
    ///
    /// # Errors
    ///
    /// Rejects a Metadata tier that cannot retain the permanent bounded-commit
    /// floor. DATA may start exhausted; reads and cleanup still remain useful.
    pub fn new(snapshot: CommitCapacitySnapshot) -> Result<Self, CommitCapacityConfigurationError> {
        if snapshot.metadata_available_bytes < COMMIT_METADATA_FLOOR_BYTES_V1 {
            return Err(CommitCapacityConfigurationError);
        }
        Ok(Self {
            metadata_limit: AtomicU64::new(snapshot.metadata_available_bytes),
            data_limit: AtomicU64::new(snapshot.data_available_bytes),
            metadata_reserved: AtomicU64::new(COMMIT_METADATA_FLOOR_BYTES_V1),
            data_reserved: AtomicU64::new(0),
            active_metadata: AtomicU64::new(0),
            active_data: AtomicU64::new(0),
            observation_epoch: AtomicU64::new(0),
            frozen: Mutex::new(BTreeMap::new()),
            uncheckpointed_data: Mutex::new(UncheckpointedDataClaim::default()),
        })
    }

    /// Starts one physical observation before its filesystem calls execute.
    ///
    /// # Panics
    ///
    /// Panics only after exhausting every `u64` observation epoch.
    #[must_use]
    pub fn begin_observation(&self) -> u64 {
        self.observation_epoch
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .expect("ASSERT: physical observation epoch cannot wrap")
    }

    /// Publishes a fresh physical observation and retires only commits that
    /// completed before this observation began.
    ///
    /// # Panics
    ///
    /// Panics when observations overlap, arrive out of order, or an internal
    /// reservation invariant was previously violated.
    pub fn finish_observation(&self, epoch: u64, snapshot: CommitCapacitySnapshot) {
        assert_eq!(
            self.observation_epoch.load(Ordering::Acquire),
            epoch,
            "ASSERT: physical capacity observations are serialized"
        );
        self.metadata_limit
            .store(snapshot.metadata_available_bytes, Ordering::Release);
        self.data_limit
            .store(snapshot.data_available_bytes, Ordering::Release);

        let mut frozen = self
            .frozen
            .lock()
            .expect("ASSERT: commit-capacity generation lock poisoned");
        let mut released_metadata = 0_u64;
        let mut released_data = 0_u64;
        frozen.retain(|_, claim| {
            if claim
                .completed_after_observation
                .is_some_and(|completed_epoch| completed_epoch < epoch)
            {
                released_metadata = released_metadata
                    .checked_add(claim.metadata_bytes)
                    .expect("ASSERT: frozen Metadata claims fit reserved total");
                released_data = released_data
                    .checked_add(claim.data_bytes)
                    .expect("ASSERT: frozen DATA claims fit reserved total");
                false
            } else {
                true
            }
        });
        subtract_reserved(&self.metadata_reserved, released_metadata);
        subtract_reserved(&self.data_reserved, released_data);

        let mut uncheckpointed = self
            .uncheckpointed_data
            .lock()
            .expect("ASSERT: uncheckpointed DATA claim lock poisoned");
        let released_uncheckpointed_data = if uncheckpointed
            .completed_after_observation
            .is_some_and(|completed_epoch| completed_epoch < epoch)
        {
            let released = uncheckpointed.data_bytes;
            *uncheckpointed = UncheckpointedDataClaim::default();
            released
        } else {
            0
        };
        subtract_reserved(&self.data_reserved, released_uncheckpointed_data);
    }

    /// Closes new capacity admission after an observation failure while
    /// retaining every outstanding claim.
    ///
    /// # Panics
    ///
    /// Panics when observations overlap or arrive out of order.
    pub fn observation_failed(&self, epoch: u64) {
        assert_eq!(
            self.observation_epoch.load(Ordering::Acquire),
            epoch,
            "ASSERT: physical capacity observations are serialized"
        );
        self.metadata_limit.store(0, Ordering::Release);
        self.data_limit.store(0, Ordering::Release);
    }

    #[must_use]
    /// Returns one diagnostic snapshot.
    ///
    /// # Panics
    ///
    /// Panics when an impossible prior failure poisoned the generation lock.
    pub fn status(&self) -> CommitCapacityStatus {
        CommitCapacityStatus {
            metadata_limit_bytes: self.metadata_limit.load(Ordering::Acquire),
            data_limit_bytes: self.data_limit.load(Ordering::Acquire),
            reserved_metadata_bytes: self.metadata_reserved.load(Ordering::Acquire),
            reserved_data_bytes: self.data_reserved.load(Ordering::Acquire),
            active_metadata_bytes: self.active_metadata.load(Ordering::Acquire),
            active_data_bytes: self.active_data.load(Ordering::Acquire),
            frozen_generations: self
                .frozen
                .lock()
                .expect("ASSERT: commit-capacity generation lock poisoned")
                .len(),
        }
    }
}

impl CommitCapacityAdmission for CommitCapacityGovernor {
    fn try_reserve(&self, claim: CommitCapacityClaim) -> Result<(), PosixError> {
        reserve_bounded(
            &self.metadata_reserved,
            &self.metadata_limit,
            claim.metadata_bytes(),
        )?;
        if let Err(error) =
            reserve_bounded(&self.data_reserved, &self.data_limit, claim.data_bytes())
        {
            subtract_reserved(&self.metadata_reserved, claim.metadata_bytes());
            return Err(error);
        }
        Ok(())
    }

    fn cancel(&self, claim: CommitCapacityClaim) {
        subtract_reserved(&self.metadata_reserved, claim.metadata_bytes());
        subtract_reserved(&self.data_reserved, claim.data_bytes());
    }

    fn accept(&self, claim: CommitCapacityClaim) {
        self.active_metadata
            .fetch_add(claim.metadata_bytes(), Ordering::AcqRel);
        self.active_data
            .fetch_add(claim.data_bytes(), Ordering::AcqRel);
    }

    fn release_active_metadata(&self, bytes: u64) {
        subtract_reserved(&self.active_metadata, bytes);
        subtract_reserved(&self.metadata_reserved, bytes);
    }

    fn freeze(&self, token: CommitToken) {
        let claim = FrozenClaim {
            metadata_bytes: self.active_metadata.swap(0, Ordering::AcqRel),
            data_bytes: self.active_data.swap(0, Ordering::AcqRel),
            completed_after_observation: None,
        };
        let replaced = self
            .frozen
            .lock()
            .expect("ASSERT: commit-capacity generation lock poisoned")
            .insert(token, claim);
        assert!(
            replaced.is_none(),
            "ASSERT: a Commit token freezes capacity exactly once"
        );
    }

    fn complete(&self, token: CommitToken) {
        let mut frozen = self
            .frozen
            .lock()
            .expect("ASSERT: commit-capacity generation lock poisoned");
        let claim = frozen
            .get_mut(&token)
            .expect("ASSERT: completed Commit capacity must be frozen");
        assert!(
            claim.completed_after_observation.is_none(),
            "ASSERT: Commit capacity completes once"
        );
        claim.completed_after_observation = Some(self.observation_epoch.load(Ordering::Acquire));
    }

    fn finish_uncheckpointed_active(&self) {
        let metadata = self.active_metadata.swap(0, Ordering::AcqRel);
        let data = self.active_data.swap(0, Ordering::AcqRel);
        subtract_reserved(&self.metadata_reserved, metadata);
        if data == 0 {
            return;
        }
        let completed_epoch = self.observation_epoch.load(Ordering::Acquire);
        let mut uncheckpointed = self
            .uncheckpointed_data
            .lock()
            .expect("ASSERT: uncheckpointed DATA claim lock poisoned");
        uncheckpointed.data_bytes = uncheckpointed
            .data_bytes
            .checked_add(data)
            .expect("ASSERT: uncheckpointed DATA claims fit reserved total");
        uncheckpointed.completed_after_observation = Some(
            uncheckpointed
                .completed_after_observation
                .map_or(completed_epoch, |previous| previous.max(completed_epoch)),
        );
    }
}

fn reserve_bounded(reserved: &AtomicU64, limit: &AtomicU64, bytes: u64) -> Result<(), PosixError> {
    if bytes == 0 {
        return Ok(());
    }
    let mut current = reserved.load(Ordering::Acquire);
    loop {
        let next = current.checked_add(bytes).ok_or(PosixError::NoSpace)?;
        if next > limit.load(Ordering::Acquire) {
            return Err(PosixError::NoSpace);
        }
        match reserved.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

fn subtract_reserved(reserved: &AtomicU64, bytes: u64) {
    if bytes == 0 {
        return;
    }
    let previous = reserved.fetch_sub(bytes, Ordering::AcqRel);
    assert!(
        previous >= bytes,
        "ASSERT: released capacity must have been reserved"
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitCapacityStatus {
    metadata_limit_bytes: u64,
    data_limit_bytes: u64,
    reserved_metadata_bytes: u64,
    reserved_data_bytes: u64,
    active_metadata_bytes: u64,
    active_data_bytes: u64,
    frozen_generations: usize,
}

impl CommitCapacityStatus {
    #[must_use]
    pub const fn metadata_limit_bytes(self) -> u64 {
        self.metadata_limit_bytes
    }
    #[must_use]
    pub const fn data_limit_bytes(self) -> u64 {
        self.data_limit_bytes
    }
    #[must_use]
    pub const fn reserved_metadata_bytes(self) -> u64 {
        self.reserved_metadata_bytes
    }
    #[must_use]
    pub const fn reserved_data_bytes(self) -> u64 {
        self.reserved_data_bytes
    }
    #[must_use]
    pub const fn active_metadata_bytes(self) -> u64 {
        self.active_metadata_bytes
    }
    #[must_use]
    pub const fn active_data_bytes(self) -> u64 {
        self.active_data_bytes
    }
    #[must_use]
    pub const fn frozen_generations(self) -> usize {
        self.frozen_generations
    }
}
