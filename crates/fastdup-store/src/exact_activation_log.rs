use std::fmt;
use std::io;

use fastdup_format::{
    EXACT_INDEX_ACTIVATION_RECORD_BYTES, ExactIndexActivationHash, ExactIndexActivationRecord,
};

use crate::StorageIo;

const SLOT_NAMES: [&str; 2] = ["exact-index.activation.wal", "exact-index.activation.1.wal"];
const MAX_SLOT_RECORDS: usize = 64;
const MAX_SLOT_BYTES: usize = MAX_SLOT_RECORDS * EXACT_INDEX_ACTIVATION_RECORD_BYTES;
const MAX_LEGACY_WAL_BYTES: usize = 64 * 1_024 * 1_024;

/// Paired-slot selection and publication for Exact-Index activations.
///
/// The immutable Runs and Run Set remain outside this module. Its interface
/// owns only lifetime-bounded activation ordering and keeps slot topology out
/// of the repository's public surface.
#[derive(Debug)]
pub(crate) struct ExactActivationLog<'a, I> {
    storage: &'a I,
}

impl<'a, I: StorageIo> ExactActivationLog<'a, I> {
    pub(crate) const fn new(storage: &'a I) -> Self {
        Self { storage }
    }

    pub(crate) fn load_for_recovery(
        &self,
    ) -> Result<Option<ActivationLogSnapshot>, ExactActivationLogError> {
        self.load()
    }

    pub(crate) fn load_for_append(&self) -> Result<ActivationLogSnapshot, ExactActivationLogError> {
        self.ensure_slots_exist()?;
        match self.load()? {
            Some(snapshot) => Ok(snapshot),
            None => Ok(ActivationLogSnapshot::empty(0)),
        }
    }

    pub(crate) fn append(
        &self,
        snapshot: &ActivationLogSnapshot,
        record: ExactIndexActivationRecord,
    ) -> Result<(), ExactActivationLogError> {
        if snapshot.tail != ActivationLogTail::Clean {
            return Err(ExactActivationLogError::NeedsRepair);
        }
        verify_successor(snapshot, record)?;

        let encoded_record = record.encode();
        let (target_slot, expected) = if snapshot.record_count() >= MAX_SLOT_RECORDS {
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(2 * EXACT_INDEX_ACTIVATION_RECORD_BYTES)
                .map_err(|_| ExactActivationLogError::OutOfMemory)?;
            bytes.extend_from_slice(
                snapshot
                    .last_encoded()
                    .ok_or(ExactActivationLogError::EmptyAfterInitialization)?,
            );
            bytes.extend_from_slice(&encoded_record);
            (1 - snapshot.active_slot, bytes)
        } else {
            let mut bytes = snapshot.bytes.clone();
            bytes
                .try_reserve_exact(EXACT_INDEX_ACTIVATION_RECORD_BYTES)
                .map_err(|_| ExactActivationLogError::OutOfMemory)?;
            bytes.extend_from_slice(&encoded_record);
            (snapshot.active_slot, bytes)
        };

        let target_name = SLOT_NAMES[target_slot];
        if target_slot == snapshot.active_slot {
            let offset = u64::try_from(snapshot.bytes.len())
                .map_err(|_| ExactActivationLogError::SlotTooLarge)?;
            self.storage
                .write_at(target_name, offset, &encoded_record)?;
        } else {
            self.storage.set_len(target_name, 0)?;
            self.storage.write_at(
                target_name,
                0,
                &expected[..EXACT_INDEX_ACTIVATION_RECORD_BYTES],
            )?;
            self.storage.write_at(
                target_name,
                u64::try_from(EXACT_INDEX_ACTIVATION_RECORD_BYTES)
                    .expect("ASSERT: activation record byte count fits u64"),
                &expected[EXACT_INDEX_ACTIVATION_RECORD_BYTES..],
            )?;
        }
        self.storage.set_len(
            target_name,
            u64::try_from(expected.len()).map_err(|_| ExactActivationLogError::SlotTooLarge)?,
        )?;

        let reread = self.storage.read(target_name)?;
        let verified = decode_slot(target_slot, reread)?;
        if verified.tail != ActivationLogTail::Clean || verified.bytes != expected {
            return Err(ExactActivationLogError::PublishVerificationMismatch);
        }

        // Both fixed names are directory-durable before the first append. The
        // selected slot sync is the only activation/rotation commit point.
        self.storage.sync_file(target_name)?;
        Ok(())
    }

    pub(crate) fn sync_selected(
        &self,
        snapshot: &ActivationLogSnapshot,
    ) -> Result<(), ExactActivationLogError> {
        if snapshot.tail != ActivationLogTail::Clean || snapshot.last_record().is_none() {
            return Err(ExactActivationLogError::NeedsRepair);
        }
        self.storage.sync_file(SLOT_NAMES[snapshot.active_slot])?;
        Ok(())
    }

    fn ensure_slots_exist(&self) -> Result<(), ExactActivationLogError> {
        for (slot, name) in SLOT_NAMES.into_iter().enumerate() {
            if self.storage.exists(name)? {
                let length = self.storage.object_len(name)?;
                if length > maximum_slot_bytes(slot) {
                    return Err(ExactActivationLogError::SlotTooLarge);
                }
            } else {
                self.storage.create_new(name)?;
                self.storage.set_len(name, 0)?;
                self.storage.sync_file(name)?;
            }
        }
        self.storage.sync_root()?;
        Ok(())
    }

    fn load(&self) -> Result<Option<ActivationLogSnapshot>, ExactActivationLogError> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(SLOT_NAMES.len())
            .map_err(|_| ExactActivationLogError::OutOfMemory)?;
        for (slot, name) in SLOT_NAMES.into_iter().enumerate() {
            let length = match self.storage.object_len(name) {
                Ok(length) => length,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if length > maximum_slot_bytes(slot) {
                return Err(ExactActivationLogError::SlotTooLarge);
            }
            let bytes = self.storage.read(name)?;
            if u64::try_from(bytes.len()) != Ok(length) {
                return Err(ExactActivationLogError::PublishVerificationMismatch);
            }
            slots.push(decode_slot(slot, bytes)?);
        }
        select_current(slots)
    }
}

fn maximum_slot_bytes(slot: usize) -> u64 {
    let maximum = if slot == 0 {
        MAX_LEGACY_WAL_BYTES
    } else {
        MAX_SLOT_BYTES
    };
    u64::try_from(maximum).expect("ASSERT: Exact Activation Log size limit fits u64")
}

#[derive(Clone, Debug)]
pub(crate) struct ActivationLogSnapshot {
    active_slot: usize,
    bytes: Vec<u8>,
    records: Vec<ExactIndexActivationRecord>,
    tail: ActivationLogTail,
}

impl ActivationLogSnapshot {
    fn empty(active_slot: usize) -> Self {
        Self {
            active_slot,
            bytes: Vec::new(),
            records: Vec::new(),
            tail: ActivationLogTail::Clean,
        }
    }

    pub(crate) fn last_record(&self) -> Option<ExactIndexActivationRecord> {
        self.records.last().copied()
    }

    pub(crate) fn last_hash(&self) -> Option<ExactIndexActivationHash> {
        self.last_encoded().map(ExactIndexActivationHash::of)
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }

    fn first_encoded(&self) -> Option<&[u8]> {
        (!self.records.is_empty()).then(|| &self.bytes[..EXACT_INDEX_ACTIVATION_RECORD_BYTES])
    }

    fn last_encoded(&self) -> Option<&[u8]> {
        (!self.records.is_empty())
            .then(|| &self.bytes[self.bytes.len() - EXACT_INDEX_ACTIVATION_RECORD_BYTES..])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivationLogTail {
    Clean,
    Torn,
}

fn decode_slot(
    slot: usize,
    mut bytes: Vec<u8>,
) -> Result<ActivationLogSnapshot, ExactActivationLogError> {
    let complete_bytes =
        bytes.len() / EXACT_INDEX_ACTIVATION_RECORD_BYTES * EXACT_INDEX_ACTIVATION_RECORD_BYTES;
    let mut records = Vec::new();
    records
        .try_reserve_exact(complete_bytes / EXACT_INDEX_ACTIVATION_RECORD_BYTES)
        .map_err(|_| ExactActivationLogError::OutOfMemory)?;
    let mut previous_encoded_hash = None;
    let mut previous_record: Option<ExactIndexActivationRecord> = None;
    for offset in (0..complete_bytes).step_by(EXACT_INDEX_ACTIVATION_RECORD_BYTES) {
        let encoded = &bytes[offset..offset + EXACT_INDEX_ACTIVATION_RECORD_BYTES];
        let record = ExactIndexActivationRecord::decode(encoded)
            .map_err(|_| ExactActivationLogError::BrokenChain)?;
        if let Some(previous) = previous_record {
            let expected_generation = previous
                .generation()
                .checked_add(1)
                .ok_or(ExactActivationLogError::BrokenChain)?;
            let expected_hash = previous_encoded_hash
                .expect("ASSERT: prior activation record has a prior encoded hash");
            if record.generation() != expected_generation
                || record.previous_record_hash() != expected_hash
                || record.run_set_generation() <= previous.run_set_generation()
            {
                return Err(ExactActivationLogError::BrokenChain);
            }
        }
        records.push(record);
        previous_encoded_hash = Some(ExactIndexActivationHash::of(encoded));
        previous_record = Some(record);
    }
    let tail = if bytes.len() == complete_bytes {
        ActivationLogTail::Clean
    } else {
        ActivationLogTail::Torn
    };
    bytes.truncate(complete_bytes);
    Ok(ActivationLogSnapshot {
        active_slot: slot,
        bytes,
        records,
        tail,
    })
}

fn select_current(
    mut slots: Vec<ActivationLogSnapshot>,
) -> Result<Option<ActivationLogSnapshot>, ExactActivationLogError> {
    if slots
        .iter()
        .any(|slot| slot.records.is_empty() && slot.tail != ActivationLogTail::Clean)
    {
        return Err(ExactActivationLogError::BrokenChain);
    }
    slots.retain(|slot| !slot.records.is_empty());
    match slots.len() {
        0 => Ok(None),
        1 => Ok(slots.pop()),
        2 => {
            let right = slots.pop().expect("ASSERT: exactly two activation slots");
            let left = slots.pop().expect("ASSERT: exactly two activation slots");
            let left_last = left
                .last_record()
                .expect("ASSERT: retained activation slot is nonempty");
            let right_last = right
                .last_record()
                .expect("ASSERT: retained activation slot is nonempty");
            if left_last.generation() == right_last.generation() {
                if left.last_encoded() != right.last_encoded() {
                    return Err(ExactActivationLogError::DivergentSlots);
                }
                if left.record_count() != right.record_count() {
                    return if left.record_count() > right.record_count() {
                        Ok(Some(left))
                    } else {
                        Ok(Some(right))
                    };
                }
                if left.bytes != right.bytes {
                    return Err(ExactActivationLogError::DivergentSlots);
                }
                return if left.tail == ActivationLogTail::Clean {
                    Ok(Some(left))
                } else {
                    Ok(Some(right))
                };
            }
            let (older, newer) = if left_last.generation() < right_last.generation() {
                (left, right)
            } else {
                (right, left)
            };
            if newer.first_encoded() != older.last_encoded() {
                return Err(ExactActivationLogError::DivergentSlots);
            }
            Ok(Some(newer))
        }
        _ => unreachable!("ASSERT: exactly two Exact Activation Log slot names exist"),
    }
}

fn verify_successor(
    snapshot: &ActivationLogSnapshot,
    record: ExactIndexActivationRecord,
) -> Result<(), ExactActivationLogError> {
    let Some(previous) = snapshot.last_record() else {
        return if record.generation() == 1
            && record.previous_record_hash() == ExactIndexActivationHash::ZERO
        {
            Ok(())
        } else {
            Err(ExactActivationLogError::BrokenChain)
        };
    };
    if previous.generation().checked_add(1) != Some(record.generation())
        || snapshot.last_hash() != Some(record.previous_record_hash())
        || record.run_set_generation() <= previous.run_set_generation()
    {
        return Err(ExactActivationLogError::BrokenChain);
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum ExactActivationLogError {
    Io(io::Error),
    SlotTooLarge,
    BrokenChain,
    DivergentSlots,
    NeedsRepair,
    EmptyAfterInitialization,
    PublishVerificationMismatch,
    OutOfMemory,
}

impl fmt::Display for ExactActivationLogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ExactActivationLogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ExactActivationLogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
