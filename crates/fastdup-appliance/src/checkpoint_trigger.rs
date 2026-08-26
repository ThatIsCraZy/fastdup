use std::time::Duration;

/// Maximum delay used to combine adjacent full-Container publications into
/// one Namespace generation.
pub const CONTAINER_COMMIT_COALESCE: Duration = Duration::from_millis(500);
/// Normal maximum age of the oldest admitted mutation before a commit starts.
pub const MUTATION_COMMIT_TARGET: Duration = Duration::from_secs(2);
/// Admission closes at this age until durable progress catches up.
pub const MUTATION_ADMISSION_GUARD: Duration = Duration::from_secs(5);
/// Persisted but uncommitted Containers that force an immediate commit.
pub const SEALED_CONTAINER_COMMIT_LIMIT: u64 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointTrigger {
    ContainerCoalesce,
    SealedContainerLimit,
    MutationAge,
    AdmissionGuard,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointAction {
    Wait(Duration),
    Commit(CheckpointTrigger),
    PauseAndCommit(CheckpointTrigger),
}

/// Admission decision while one checkpoint is already performing durable I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointProgressAction {
    Continue,
    CloseAdmission,
}

/// One immutable scheduler observation. Ages are measured from process-local
/// monotonic time and never enter a durable format.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CheckpointPressure {
    pub oldest_mutation_age: Option<Duration>,
    pub oldest_sealed_container_age: Option<Duration>,
    pub sealed_uncommitted_containers: u64,
}

/// One control-path observation used to derive checkpoint age without reading
/// a clock inside the policy module.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DurabilityObservation {
    pub has_checkpointable_dirty_payload: bool,
    pub oldest_sealed_container_age: Option<Duration>,
    pub sealed_uncommitted_containers: u64,
}

/// Process-local monotonic checkpoint and admission policy state.
///
/// Callers supply elapsed monotonic time explicitly. This keeps production and
/// fake-clock adapters at the daemon control seam and out of mutation/Ingest
/// hot loops.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurabilitySupervisor {
    oldest_dirty_since: Option<Duration>,
    last_checkpoint_attempt: Duration,
}

impl DurabilitySupervisor {
    #[must_use]
    pub const fn new(now: Duration) -> Self {
        Self {
            oldest_dirty_since: None,
            last_checkpoint_attempt: now,
        }
    }

    #[must_use]
    pub fn checkpoint_progress(elapsed: Duration) -> CheckpointProgressAction {
        if elapsed >= MUTATION_ADMISSION_GUARD {
            CheckpointProgressAction::CloseAdmission
        } else {
            CheckpointProgressAction::Continue
        }
    }

    #[must_use]
    pub fn observe(
        &mut self,
        now: Duration,
        observation: DurabilityObservation,
    ) -> CheckpointAction {
        if observation.has_checkpointable_dirty_payload {
            self.oldest_dirty_since.get_or_insert(now);
        } else {
            self.oldest_dirty_since = None;
        }
        let action = checkpoint_action(CheckpointPressure {
            oldest_mutation_age: self
                .oldest_dirty_since
                .map(|started| now.saturating_sub(started)),
            oldest_sealed_container_age: observation.oldest_sealed_container_age,
            sealed_uncommitted_containers: observation.sealed_uncommitted_containers,
        });
        if matches!(action, CheckpointAction::Wait(_))
            && now.saturating_sub(self.last_checkpoint_attempt) >= MUTATION_COMMIT_TARGET
        {
            CheckpointAction::Commit(CheckpointTrigger::MutationAge)
        } else {
            action
        }
    }

    pub fn record_checkpoint_attempt(&mut self, now: Duration, dirty_remains: bool) {
        self.last_checkpoint_attempt = now;
        self.oldest_dirty_since = dirty_remains.then_some(now);
    }
}

/// Decides the next action without performing I/O or reading wall-clock time.
///
/// The ordering is intentional: the hard admission guard dominates every
/// batching preference, then the bounded Container backlog, then the ordinary
/// mutation deadline, and finally the short Container debounce.
#[must_use]
pub fn checkpoint_action(pressure: CheckpointPressure) -> CheckpointAction {
    if pressure
        .oldest_mutation_age
        .is_some_and(|age| age >= MUTATION_ADMISSION_GUARD)
    {
        return CheckpointAction::PauseAndCommit(CheckpointTrigger::AdmissionGuard);
    }
    if pressure.sealed_uncommitted_containers >= SEALED_CONTAINER_COMMIT_LIMIT {
        return CheckpointAction::Commit(CheckpointTrigger::SealedContainerLimit);
    }
    if pressure
        .oldest_mutation_age
        .is_some_and(|age| age >= MUTATION_COMMIT_TARGET)
    {
        return CheckpointAction::Commit(CheckpointTrigger::MutationAge);
    }
    if pressure
        .oldest_sealed_container_age
        .is_some_and(|age| age >= CONTAINER_COMMIT_COALESCE)
    {
        return CheckpointAction::Commit(CheckpointTrigger::ContainerCoalesce);
    }

    let mutation_wait = pressure
        .oldest_mutation_age
        .map_or(MUTATION_COMMIT_TARGET, |age| {
            MUTATION_COMMIT_TARGET.saturating_sub(age)
        });
    let container_wait = pressure
        .oldest_sealed_container_age
        .map_or(MUTATION_COMMIT_TARGET, |age| {
            CONTAINER_COMMIT_COALESCE.saturating_sub(age)
        });
    CheckpointAction::Wait(mutation_wait.min(container_wait))
}
