use std::sync::{Arc, Mutex};

use fastdup_format::{
    CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES, ContainerGenerationHighWaterHash,
    ContainerGenerationHighWaterRecord,
};

use crate::{ContainerRepository, StorageIo, StoreError};

pub const CONTAINER_GENERATION_HIGH_WATER_SLOT_0: &str = "container-generation.wal";
pub const CONTAINER_GENERATION_HIGH_WATER_SLOT_1: &str = "container-generation.1.wal";
pub const CONTAINER_GENERATION_RESERVATION_SPAN_V1: u64 = 1_024;

#[derive(Debug, Default)]
pub(crate) struct ContainerGenerationAllocatorRegistry {
    state: Mutex<Option<Arc<Mutex<AllocatorState>>>>,
}

#[derive(Clone, Debug)]
pub struct ContainerGenerationAllocator<I> {
    repository: ContainerRepository<I>,
    reservation_span: u64,
    state: Arc<Mutex<AllocatorState>>,
}

#[derive(Clone, Copy, Debug)]
struct AllocatorState {
    selected: Option<ContainerGenerationHighWaterRecord>,
    next: u64,
    reserved_through: u64,
}

#[derive(Clone, Copy)]
struct DecodedSlot {
    record: ContainerGenerationHighWaterRecord,
    hash: ContainerGenerationHighWaterHash,
}

impl<I: Clone + StorageIo> ContainerGenerationAllocator<I> {
    pub(crate) fn open(
        repository: ContainerRepository<I>,
        reservation_span: u64,
    ) -> Result<Self, StoreError> {
        if reservation_span == 0 {
            return Err(StoreError::InvalidContainerGenerationReservationSpan);
        }
        let registry = Arc::clone(&repository.generation_allocator_registry);
        let mut registered = registry.state.lock().map_err(|_| {
            std::io::Error::other("Container generation allocator registry is poisoned")
        })?;
        if let Some(state) = registered.as_ref() {
            return Ok(Self {
                repository,
                reservation_span,
                state: Arc::clone(state),
            });
        }
        let barrier = Arc::clone(&repository.generation_allocator_barrier);
        let _guard = barrier.lock().map_err(|_| {
            std::io::Error::other("Container generation allocator barrier is poisoned")
        })?;
        ensure_slots(repository.storage())?;
        let selected = load_selected(repository.storage())?;
        let selected = if let Some(selected) = selected {
            Some(selected.record)
        } else {
            let discovered = repository.discover_container_generation_high_water()?;
            match discovered {
                Some(high_water) => {
                    Some(publish_successor(repository.storage(), None, high_water)?)
                }
                None => None,
            }
        };
        let reserved_through =
            selected.map_or(0, ContainerGenerationHighWaterRecord::reserved_through);
        let next = reserved_through
            .checked_add(1)
            .ok_or(StoreError::ContainerGenerationExhausted)?;
        let state = Arc::new(Mutex::new(AllocatorState {
            selected,
            next,
            reserved_through,
        }));
        *registered = Some(Arc::clone(&state));
        Ok(Self {
            repository,
            reservation_span,
            state,
        })
    }
}

impl<I: StorageIo> ContainerGenerationAllocator<I> {
    /// Returns one generation from a range made durable before this call.
    ///
    /// # Errors
    ///
    /// Returns allocator corruption, exhaustion, or storage durability errors.
    /// A failed range extension returns no generation to the caller.
    ///
    /// # Panics
    ///
    /// Panics if another allocator operation poisoned the shared state lock.
    pub fn reserve_generation(&self) -> Result<u64, StoreError> {
        let mut state = self
            .state
            .lock()
            .expect("ASSERT: Container generation allocator lock poisoned");
        if state.next > state.reserved_through {
            let reserved_through = state
                .reserved_through
                .checked_add(self.reservation_span)
                .ok_or(StoreError::ContainerGenerationExhausted)?;
            let _guard = self
                .repository
                .generation_allocator_barrier
                .lock()
                .map_err(|_| {
                    std::io::Error::other("Container generation allocator barrier is poisoned")
                })?;
            let record =
                publish_successor(self.repository.storage(), state.selected, reserved_through)?;
            state.selected = Some(record);
            state.reserved_through = reserved_through;
        }
        let generation = state.next;
        state.next = state
            .next
            .checked_add(1)
            .ok_or(StoreError::ContainerGenerationExhausted)?;
        Ok(generation)
    }

    #[must_use]
    /// Returns the upper bound of the range durably reserved by this process.
    ///
    /// # Panics
    ///
    /// Panics if another allocator operation poisoned the shared state lock.
    pub fn durable_reserved_through(&self) -> u64 {
        self.state
            .lock()
            .expect("ASSERT: Container generation allocator lock poisoned")
            .reserved_through
    }
}

pub(crate) fn audit_generation_high_water<I: StorageIo>(
    storage: &I,
    observed_generation: Option<u64>,
) -> Result<Option<u64>, StoreError> {
    let first_exists = storage.exists(CONTAINER_GENERATION_HIGH_WATER_SLOT_0)?;
    let second_exists = storage.exists(CONTAINER_GENERATION_HIGH_WATER_SLOT_1)?;
    match (first_exists, second_exists) {
        (false, false) => return Ok(None),
        (true, true) => {}
        (true, false) => {
            return if read_slot(storage, CONTAINER_GENERATION_HIGH_WATER_SLOT_0)?.is_none() {
                Ok(None)
            } else {
                Err(StoreError::ContainerGenerationHighWaterChain)
            };
        }
        (false, true) => {
            return if read_slot(storage, CONTAINER_GENERATION_HIGH_WATER_SLOT_1)?.is_none() {
                Ok(None)
            } else {
                Err(StoreError::ContainerGenerationHighWaterChain)
            };
        }
    }
    let selected = load_selected(storage)?;
    let Some(selected) = selected else {
        return Ok(None);
    };
    let reserved_through = selected.record.reserved_through();
    if let Some(observed) = observed_generation
        && observed > reserved_through
    {
        return Err(StoreError::ContainerGenerationHighWaterBehind {
            reserved_through,
            observed,
        });
    }
    Ok(Some(reserved_through))
}

fn ensure_slots<I: StorageIo>(storage: &I) -> Result<(), StoreError> {
    for name in [
        CONTAINER_GENERATION_HIGH_WATER_SLOT_0,
        CONTAINER_GENERATION_HIGH_WATER_SLOT_1,
    ] {
        if !storage.exists(name)? {
            storage.create_new(name)?;
            storage.sync_file(name)?;
        }
    }
    storage.sync_root()?;
    Ok(())
}

fn load_selected<I: StorageIo>(storage: &I) -> Result<Option<DecodedSlot>, StoreError> {
    let first = read_slot(storage, CONTAINER_GENERATION_HIGH_WATER_SLOT_0)?;
    let second = read_slot(storage, CONTAINER_GENERATION_HIGH_WATER_SLOT_1)?;
    match (first, second) {
        (None, None) => Ok(None),
        (Some(slot), None) | (None, Some(slot)) if slot.record.sequence() == 1 => Ok(Some(slot)),
        (Some(_), None) | (None, Some(_)) => Err(StoreError::ContainerGenerationHighWaterChain),
        (Some(first), Some(second)) => {
            let (older, newer) = if first.record.sequence() < second.record.sequence() {
                (first, second)
            } else {
                (second, first)
            };
            if newer.record.sequence() != older.record.sequence().saturating_add(1)
                || newer.record.previous_record_hash() != older.hash
                || newer.record.reserved_through() < older.record.reserved_through()
            {
                return Err(StoreError::ContainerGenerationHighWaterChain);
            }
            Ok(Some(newer))
        }
    }
}

fn read_slot<I: StorageIo>(storage: &I, name: &str) -> Result<Option<DecodedSlot>, StoreError> {
    let length = storage.object_len(name)?;
    if length == 0 {
        return Ok(None);
    }
    if usize::try_from(length) != Ok(CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES) {
        return Err(StoreError::ContainerGenerationHighWaterFormat(
            fastdup_format::ContainerGenerationHighWaterFormatError::InvalidLength(
                usize::try_from(length).unwrap_or(usize::MAX),
            ),
        ));
    }
    let bytes = storage.read_exact_at(name, 0, CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES)?;
    let record = ContainerGenerationHighWaterRecord::decode(&bytes)
        .map_err(StoreError::ContainerGenerationHighWaterFormat)?;
    Ok(Some(DecodedSlot {
        record,
        hash: ContainerGenerationHighWaterHash::of(&bytes),
    }))
}

fn publish_successor<I: StorageIo>(
    storage: &I,
    selected: Option<ContainerGenerationHighWaterRecord>,
    reserved_through: u64,
) -> Result<ContainerGenerationHighWaterRecord, StoreError> {
    let (sequence, previous_hash) = match selected {
        Some(record) => {
            let bytes = record.encode();
            (
                record
                    .sequence()
                    .checked_add(1)
                    .ok_or(StoreError::ContainerGenerationExhausted)?,
                ContainerGenerationHighWaterHash::of(&bytes),
            )
        }
        None => (1, ContainerGenerationHighWaterHash::ZERO),
    };
    let record = ContainerGenerationHighWaterRecord::new(sequence, previous_hash, reserved_through)
        .map_err(StoreError::ContainerGenerationHighWaterFormat)?;
    let bytes = record.encode();
    let target = if sequence % 2 == 1 {
        CONTAINER_GENERATION_HIGH_WATER_SLOT_0
    } else {
        CONTAINER_GENERATION_HIGH_WATER_SLOT_1
    };
    storage.set_len(target, 0)?;
    storage.write_at(target, 0, &bytes)?;
    storage.set_len(
        target,
        u64::try_from(CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES)
            .expect("ASSERT: fixed allocator record length fits u64"),
    )?;
    let reread = storage.read_exact_at(target, 0, CONTAINER_GENERATION_HIGH_WATER_RECORD_BYTES)?;
    if reread != bytes
        || ContainerGenerationHighWaterRecord::decode(&reread)
            .map_err(StoreError::ContainerGenerationHighWaterFormat)?
            != record
    {
        return Err(StoreError::PublishVerificationMismatch);
    }
    storage.sync_file(target)?;
    Ok(record)
}
