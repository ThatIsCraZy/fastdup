use crate::StorageIo;
use fastdup_format::{COMMIT_RECORD_BYTES, CommitRecord, CommitRecordHash};
use std::fmt;
use std::io;

const SLOT_NAMES: [&str; 2] = ["commit.wal", "commit.1.wal"];
const MAX_SEGMENT_RECORDS: usize = 64;
const MAX_SEGMENT_BYTES: usize = MAX_SEGMENT_RECORDS * COMMIT_RECORD_BYTES;
const MAX_LEGACY_WAL_BYTES: usize = 64 * 1_024 * 1_024;

#[derive(Debug)]
pub(crate) struct GenerationLog<'a, I> {
    storage: &'a I,
}

impl<'a, I: StorageIo> GenerationLog<'a, I> {
    pub(crate) const fn new(storage: &'a I) -> Self {
        Self { storage }
    }

    pub(crate) fn load_for_recovery(&self) -> Result<Option<LogSnapshot>, GenerationLogError> {
        self.load()
    }

    pub(crate) fn load_for_append(&self) -> Result<LogSnapshot, GenerationLogError> {
        self.ensure_slots_exist()?;
        match self.load()? {
            Some(snapshot) => Ok(snapshot),
            None => Ok(LogSnapshot::empty(0)),
        }
    }

    pub(crate) fn append(
        &self,
        snapshot: &LogSnapshot,
        record: CommitRecord,
    ) -> Result<(), GenerationLogError> {
        if snapshot.tail != LogTail::Clean {
            return Err(GenerationLogError::NeedsRepair(snapshot.tail.clone()));
        }
        verify_successor(snapshot, record)?;

        let encoded_record = record.encode();
        let (target_slot, expected) = if snapshot.record_count() >= MAX_SEGMENT_RECORDS {
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(2 * COMMIT_RECORD_BYTES)
                .map_err(|_| GenerationLogError::OutOfMemory)?;
            bytes.extend_from_slice(
                snapshot
                    .last_encoded()
                    .ok_or(GenerationLogError::EmptyAfterInitialization)?,
            );
            bytes.extend_from_slice(&encoded_record);
            (1 - snapshot.active_slot, bytes)
        } else {
            let mut bytes = snapshot.bytes.clone();
            bytes
                .try_reserve_exact(COMMIT_RECORD_BYTES)
                .map_err(|_| GenerationLogError::OutOfMemory)?;
            bytes.extend_from_slice(&encoded_record);
            (snapshot.active_slot, bytes)
        };

        let target_name = SLOT_NAMES[target_slot];
        if target_slot == snapshot.active_slot {
            let offset = u64::try_from(snapshot.bytes.len())
                .map_err(|_| GenerationLogError::SegmentTooLarge)?;
            self.storage
                .write_at(target_name, offset, &encoded_record)?;
        } else {
            self.storage.set_len(target_name, 0)?;
            self.storage
                .write_at(target_name, 0, &expected[..COMMIT_RECORD_BYTES])?;
            self.storage.write_at(
                target_name,
                u64::try_from(COMMIT_RECORD_BYTES)
                    .expect("ASSERT: Commit Record byte count fits u64"),
                &expected[COMMIT_RECORD_BYTES..],
            )?;
        }
        self.storage.set_len(
            target_name,
            u64::try_from(expected.len()).map_err(|_| GenerationLogError::SegmentTooLarge)?,
        )?;

        let reread = self.storage.read(target_name)?;
        let verified = decode_segment(target_slot, reread)?;
        if verified.tail != LogTail::Clean || verified.bytes != expected {
            return Err(GenerationLogError::PublishVerificationMismatch);
        }

        // The synchronized slot is the only commit point. Both fixed slot
        // names were made directory-durable before any record write.
        self.storage.sync_file(target_name)?;
        Ok(())
    }

    fn ensure_slots_exist(&self) -> Result<(), GenerationLogError> {
        for (slot, name) in SLOT_NAMES.into_iter().enumerate() {
            if self.storage.exists(name)? {
                let length = self.storage.object_len(name)?;
                if length > maximum_slot_bytes(slot) {
                    return Err(GenerationLogError::SegmentTooLarge);
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

    fn load(&self) -> Result<Option<LogSnapshot>, GenerationLogError> {
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(SLOT_NAMES.len())
            .map_err(|_| GenerationLogError::OutOfMemory)?;
        for (slot, name) in SLOT_NAMES.into_iter().enumerate() {
            let length = match self.storage.object_len(name) {
                Ok(length) => length,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if length > maximum_slot_bytes(slot) {
                return Err(GenerationLogError::SegmentTooLarge);
            }
            let bytes = self.storage.read(name)?;
            if u64::try_from(bytes.len()) != Ok(length) {
                return Err(GenerationLogError::PublishVerificationMismatch);
            }
            segments.push(decode_segment(slot, bytes)?);
        }
        select_current(segments)
    }
}

fn maximum_slot_bytes(slot: usize) -> u64 {
    let maximum = if slot == 0 {
        MAX_LEGACY_WAL_BYTES
    } else {
        MAX_SEGMENT_BYTES
    };
    u64::try_from(maximum).expect("ASSERT: Generation Log size limit fits u64")
}

#[derive(Clone, Debug)]
pub(crate) struct LogSnapshot {
    active_slot: usize,
    bytes: Vec<u8>,
    records: Vec<CommitRecord>,
    tail: LogTail,
}

impl LogSnapshot {
    fn empty(active_slot: usize) -> Self {
        Self {
            active_slot,
            bytes: Vec::new(),
            records: Vec::new(),
            tail: LogTail::Clean,
        }
    }

    pub(crate) fn records(&self) -> &[CommitRecord] {
        &self.records
    }

    pub(crate) fn tail(&self) -> &LogTail {
        &self.tail
    }

    pub(crate) fn last_record(&self) -> Option<CommitRecord> {
        self.records.last().copied()
    }

    pub(crate) fn last_hash(&self) -> Option<CommitRecordHash> {
        self.last_encoded().map(CommitRecordHash::of)
    }

    pub(crate) fn will_rotate(&self) -> bool {
        self.record_count() >= MAX_SEGMENT_RECORDS
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }

    fn first_encoded(&self) -> Option<&[u8]> {
        (!self.records.is_empty()).then(|| &self.bytes[..COMMIT_RECORD_BYTES])
    }

    fn last_encoded(&self) -> Option<&[u8]> {
        (!self.records.is_empty()).then(|| &self.bytes[self.bytes.len() - COMMIT_RECORD_BYTES..])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogTail {
    Clean,
    Torn {
        valid_bytes: usize,
        tail_bytes: usize,
    },
    InvalidRecord {
        offset: usize,
    },
    BrokenChain {
        offset: usize,
    },
}

fn decode_segment(slot: usize, mut bytes: Vec<u8>) -> Result<LogSnapshot, GenerationLogError> {
    let complete_bytes = bytes.len() / COMMIT_RECORD_BYTES * COMMIT_RECORD_BYTES;
    let mut records = Vec::new();
    records
        .try_reserve_exact(complete_bytes / COMMIT_RECORD_BYTES)
        .map_err(|_| GenerationLogError::OutOfMemory)?;
    let mut previous_encoded_hash: Option<CommitRecordHash> = None;
    let mut previous_record: Option<CommitRecord> = None;
    for offset in (0..complete_bytes).step_by(COMMIT_RECORD_BYTES) {
        let encoded = &bytes[offset..offset + COMMIT_RECORD_BYTES];
        let Ok(record) = CommitRecord::decode(encoded) else {
            bytes.truncate(offset);
            return Ok(LogSnapshot {
                active_slot: slot,
                bytes,
                records,
                tail: LogTail::InvalidRecord { offset },
            });
        };
        if let Some(previous) = previous_record {
            let expected_generation = previous
                .generation()
                .checked_add(1)
                .ok_or(GenerationLogError::BrokenGenerationChain)?;
            let expected_hash =
                previous_encoded_hash.expect("ASSERT: prior record has prior encoded hash");
            if record.generation() != expected_generation
                || record.previous_record_hash() != expected_hash
                || record.namespace_mutation_cutoff() < previous.namespace_mutation_cutoff()
                || record.inode_reservation_end() < previous.inode_reservation_end()
                || record.inode_allocation_cursor() < previous.inode_allocation_cursor()
            {
                bytes.truncate(offset);
                return Ok(LogSnapshot {
                    active_slot: slot,
                    bytes,
                    records,
                    tail: LogTail::BrokenChain { offset },
                });
            }
        }
        records.push(record);
        previous_encoded_hash = Some(CommitRecordHash::of(encoded));
        previous_record = Some(record);
    }
    let tail_bytes = bytes.len() - complete_bytes;
    let tail = if tail_bytes == 0 {
        LogTail::Clean
    } else {
        LogTail::Torn {
            valid_bytes: complete_bytes,
            tail_bytes,
        }
    };
    bytes.truncate(complete_bytes);
    Ok(LogSnapshot {
        active_slot: slot,
        bytes,
        records,
        tail,
    })
}

fn select_current(
    mut segments: Vec<LogSnapshot>,
) -> Result<Option<LogSnapshot>, GenerationLogError> {
    let has_invalid_nonempty_segment = segments
        .iter()
        .any(|segment| segment.records.is_empty() && segment.tail != LogTail::Clean);
    if has_invalid_nonempty_segment {
        return Err(GenerationLogError::BrokenGenerationChain);
    }
    segments.retain(|segment| !segment.records.is_empty());
    match segments.len() {
        0 => Ok(None),
        1 => Ok(segments.pop()),
        2 => {
            let right = segments.pop().expect("ASSERT: exactly two segments");
            let left = segments.pop().expect("ASSERT: exactly two segments");
            let left_last = left
                .last_record()
                .expect("ASSERT: retained segment is nonempty");
            let right_last = right
                .last_record()
                .expect("ASSERT: retained segment is nonempty");
            if left_last.generation() == right_last.generation() {
                if left.last_encoded() != right.last_encoded() {
                    return Err(GenerationLogError::DivergentSlots);
                }
                if left.record_count() != right.record_count() {
                    return if left.record_count() > right.record_count() {
                        Ok(Some(left))
                    } else {
                        Ok(Some(right))
                    };
                }
                if left.bytes != right.bytes {
                    return Err(GenerationLogError::DivergentSlots);
                }
                return if left.tail == LogTail::Clean {
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
                return Err(GenerationLogError::DivergentSlots);
            }
            Ok(Some(newer))
        }
        _ => unreachable!("ASSERT: exactly two Generation Log slot names exist"),
    }
}

fn verify_successor(
    snapshot: &LogSnapshot,
    record: CommitRecord,
) -> Result<(), GenerationLogError> {
    let Some(previous) = snapshot.last_record() else {
        return if record.generation() == 1
            && record.previous_record_hash() == CommitRecordHash::ZERO
        {
            Ok(())
        } else {
            Err(GenerationLogError::BrokenGenerationChain)
        };
    };
    if previous.generation().checked_add(1) != Some(record.generation())
        || snapshot.last_hash() != Some(record.previous_record_hash())
        || record.namespace_mutation_cutoff() < previous.namespace_mutation_cutoff()
        || record.inode_reservation_end() < previous.inode_reservation_end()
        || record.inode_allocation_cursor() < previous.inode_allocation_cursor()
    {
        return Err(GenerationLogError::BrokenGenerationChain);
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum GenerationLogError {
    Io(io::Error),
    SegmentTooLarge,
    BrokenGenerationChain,
    DivergentSlots,
    NeedsRepair(LogTail),
    EmptyAfterInitialization,
    PublishVerificationMismatch,
    OutOfMemory,
}

impl fmt::Display for GenerationLogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for GenerationLogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for GenerationLogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
