use crate::{CommitRange, ExternalDirtyData, PosixError, SparseData, copy_bytes};
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Bound::Excluded;
use std::sync::Arc;

use crate::CommitToken;

/// Type-erased, independently verified immutable content behind a POSIX inode.
pub trait CommittedFile: fmt::Debug + Send + Sync {
    /// Returns the exact logical file length represented by this version.
    fn logical_size(&self) -> u64;
    /// Returns POSIX-allocated bytes for this immutable version.
    fn allocated_bytes(&self) -> u64;
    /// Counts allocated bytes intersecting one logical range using metadata only.
    ///
    /// This method must not perform container I/O. It is used while admitting
    /// a mutation under an inode lock so `st_blocks` remains exact.
    ///
    /// # Errors
    ///
    /// Returns [`PosixError::Io`] when allocation metadata is inconsistent.
    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError>;
    /// Reads one range after verifying every durable dependency it touches.
    ///
    /// # Errors
    ///
    /// Returns [`PosixError::Io`] for I/O or integrity failures and a bounded
    /// resource error when the requested output cannot be represented.
    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError>;

    /// Verifies that one complete candidate payload is exactly this source.
    ///
    /// The default is intentionally conservative and performs a verified read.
    /// Content-addressed implementations may override it with an equivalent
    /// identity check to avoid a second physical read immediately after the
    /// source was verified. Returning `true` authorizes the Namespace to drop
    /// its resident fallback, so this is an integrity boundary rather than a
    /// probabilistic membership hint.
    ///
    /// # Errors
    ///
    /// Returns a read, integrity, or bounded-resource error. The caller must
    /// retain the resident bytes on every error.
    fn matches_complete_bytes(&self, candidate: &[u8]) -> Result<bool, PosixError> {
        if u64::try_from(candidate.len()).expect("ASSERT: usize fits u64") != self.logical_size() {
            return Ok(false);
        }
        let length = u32::try_from(candidate.len()).map_err(|_| PosixError::FileTooLarge)?;
        Ok(self.read_at(0, length)? == candidate)
    }
}

#[derive(Debug)]
struct EmptyCommittedFile;

impl CommittedFile for EmptyCommittedFile {
    fn logical_size(&self) -> u64 {
        0
    }

    fn allocated_bytes(&self) -> u64 {
        0
    }

    fn allocated_bytes_in_range(&self, _offset: u64, _length: u64) -> Result<u64, PosixError> {
        Ok(0)
    }

    fn read_at(&self, _offset: u64, _length: u32) -> Result<Vec<u8>, PosixError> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Default)]
struct RangeSet {
    ranges: BTreeMap<u64, u64>,
}

impl RangeSet {
    fn insert(&mut self, start: u64, end: u64) {
        assert!(start <= end, "ASSERT: range start must not exceed end");
        if start == end {
            return;
        }
        let mut merged_start = start;
        let mut merged_end = end;
        let mut remove = Vec::new();
        if let Some((&previous_start, &previous_end)) = self.ranges.range(..=start).next_back()
            && previous_end >= start
        {
            merged_start = previous_start;
            merged_end = merged_end.max(previous_end);
            remove.push(previous_start);
        }
        while let Some((&candidate_start, &candidate_end)) = self
            .ranges
            .range(merged_start..)
            .find(|(candidate, _)| !remove.contains(candidate))
        {
            if candidate_start > merged_end {
                break;
            }
            merged_end = merged_end.max(candidate_end);
            remove.push(candidate_start);
        }
        for key in remove {
            let removed = self.ranges.remove(&key);
            assert!(removed.is_some(), "ASSERT: planned hole range vanished");
        }
        assert!(
            self.ranges.insert(merged_start, merged_end).is_none(),
            "ASSERT: merged hole range replaced a survivor"
        );
        self.assert_valid();
    }

    fn remove(&mut self, start: u64, end: u64) {
        assert!(start <= end, "ASSERT: range start must not exceed end");
        if start == end {
            return;
        }
        let overlaps = self.overlapping_starts(start, end);
        let mut fragments = Vec::new();
        for key in &overlaps {
            let range_end = self.ranges[key];
            if *key < start {
                fragments.push((*key, start));
            }
            if range_end > end {
                fragments.push((end, range_end));
            }
        }
        for key in overlaps {
            let removed = self.ranges.remove(&key);
            assert!(removed.is_some(), "ASSERT: planned hole range vanished");
        }
        for (fragment_start, fragment_end) in fragments {
            assert!(
                self.ranges.insert(fragment_start, fragment_end).is_none(),
                "ASSERT: hole fragment overlaps a survivor"
            );
        }
        self.assert_valid();
    }

    fn truncate(&mut self, length: u64) {
        let crossing = self
            .ranges
            .range(..length)
            .next_back()
            .and_then(|(&start, &end)| (end > length).then_some((start, length)));
        let remove = self
            .ranges
            .range(length..)
            .map(|(&start, _)| start)
            .chain(crossing.map(|(start, _)| start))
            .collect::<Vec<_>>();
        for key in remove {
            let removed = self.ranges.remove(&key);
            assert!(removed.is_some(), "ASSERT: planned hole range vanished");
        }
        if let Some((start, end)) = crossing {
            assert!(
                self.ranges.insert(start, end).is_none(),
                "ASSERT: truncated hole overlaps a survivor"
            );
        }
        self.assert_valid();
    }

    fn overlapping_starts(&self, start: u64, end: u64) -> Vec<u64> {
        let mut keys = Vec::new();
        if let Some((&candidate_start, &candidate_end)) = self.ranges.range(..=start).next_back()
            && candidate_end > start
        {
            keys.push(candidate_start);
        }
        keys.extend(
            self.ranges
                .range((Excluded(start), Excluded(end)))
                .map(|(&candidate_start, _)| candidate_start),
        );
        keys
    }

    fn assert_valid(&self) {
        let mut previous_end = 0_u64;
        for (&start, &end) in &self.ranges {
            assert!(start < end, "ASSERT: hole range must be nonempty");
            assert!(
                start > previous_end || previous_end == 0,
                "ASSERT: hole ranges must be disjoint and non-adjacent"
            );
            previous_end = end;
        }
    }
}

#[derive(Debug)]
struct DirtyEpoch {
    base_size: u64,
    result_size: u64,
    base_sequence: u64,
    through_sequence: u64,
    first_sequence: Option<u64>,
    data: SparseData,
    holes: RangeSet,
}

impl DirtyEpoch {
    fn new(base_size: u64, base_sequence: u64) -> Self {
        Self {
            base_size,
            result_size: base_size,
            base_sequence,
            through_sequence: base_sequence,
            first_sequence: None,
            data: SparseData {
                logical_size: base_size,
                ..SparseData::default()
            },
            holes: RangeSet::default(),
        }
    }

    fn write(&mut self, offset: u64, bytes: &[u8], sequence: u64) -> Result<(), PosixError> {
        assert!(!bytes.is_empty(), "ASSERT: empty write reached dirty epoch");
        self.assert_next_sequence(sequence);
        let previous_size = self.result_size;
        let length = u64::try_from(bytes.len()).expect("ASSERT: usize must fit u64");
        let end = offset.checked_add(length).ok_or(PosixError::FileTooLarge)?;
        self.data.write(offset, bytes)?;
        if offset > previous_size {
            self.holes.insert(previous_size, offset);
        }
        self.holes.remove(offset, end);
        self.result_size = previous_size.max(end);
        self.record_sequence(sequence);
        self.assert_valid();
        Ok(())
    }

    fn truncate(&mut self, length: u64, sequence: u64) -> Result<(), PosixError> {
        self.assert_next_sequence(sequence);
        let previous_size = self.result_size;
        self.data.truncate(length)?;
        if length > previous_size {
            self.holes.insert(previous_size, length);
        } else {
            self.holes.truncate(length);
        }
        self.result_size = length;
        self.record_sequence(sequence);
        self.assert_valid();
        Ok(())
    }

    fn assert_next_sequence(&self, sequence: u64) {
        assert_eq!(
            sequence,
            self.through_sequence
                .checked_add(1)
                .expect("ASSERT: mutation sequence must not overflow"),
            "ASSERT: dirty epoch must receive a contiguous inode sequence"
        );
    }

    fn record_sequence(&mut self, sequence: u64) {
        self.first_sequence.get_or_insert(sequence);
        self.through_sequence = sequence;
    }

    fn assert_valid(&self) {
        assert_eq!(
            self.data.logical_size, self.result_size,
            "ASSERT: dirty DATA view and epoch size must agree"
        );
        assert!(
            self.through_sequence >= self.base_sequence,
            "ASSERT: epoch sequence must not precede its base"
        );
        assert_eq!(
            self.first_sequence.is_some(),
            self.through_sequence != self.base_sequence,
            "ASSERT: dirty epoch sequence bounds must agree"
        );
        let mut accounted_data = 0_u64;
        for (&data_start, bytes) in &self.data.extents {
            let data_length = u64::try_from(bytes.len()).expect("ASSERT: usize must fit u64");
            let data_end = data_start
                .checked_add(data_length)
                .expect("ASSERT: validated DATA extent must not overflow");
            assert!(
                self.holes
                    .overlapping_starts(data_start, data_end)
                    .is_empty(),
                "ASSERT: dirty DATA and HOLE extents must not overlap"
            );
            accounted_data = accounted_data
                .checked_add(data_length)
                .expect("ASSERT: dirty DATA accounting cannot overflow");
        }
        let mut previous_external_end = 0_u64;
        for (&data_start, external) in &self.data.external_extents {
            let data_end = data_start
                .checked_add(external.length)
                .expect("ASSERT: external dirty extent must not overflow");
            assert!(
                self.holes
                    .overlapping_starts(data_start, data_end)
                    .is_empty(),
                "ASSERT: external dirty DATA and HOLE extents must not overlap"
            );
            assert!(
                data_start >= previous_external_end,
                "ASSERT: external dirty DATA extents must not overlap"
            );
            assert!(
                external.through_sequence > self.base_sequence
                    && external.through_sequence <= self.through_sequence,
                "ASSERT: external dirty DATA must belong to this mutation epoch"
            );
            assert!(
                external
                    .source_offset
                    .checked_add(external.length)
                    .is_some_and(|end| end <= external.source.logical_size()),
                "ASSERT: external dirty DATA must stay inside its verified source"
            );
            assert!(
                self.data.extents.iter().all(|(&resident_start, bytes)| {
                    let resident_end = resident_start
                        .checked_add(u64::try_from(bytes.len()).expect("ASSERT: usize fits u64"))
                        .expect("ASSERT: resident dirty DATA must not overflow");
                    resident_end <= data_start || resident_start >= data_end
                }),
                "ASSERT: resident and external dirty DATA must not overlap"
            );
            previous_external_end = data_end;
            accounted_data = accounted_data
                .checked_add(external.length)
                .expect("ASSERT: dirty DATA accounting cannot overflow");
        }
        assert_eq!(
            accounted_data, self.data.allocated_bytes,
            "ASSERT: resident plus external dirty DATA must match cached allocation"
        );
    }
}

#[derive(Debug)]
pub(super) struct FrozenEpoch {
    token: CommitToken,
    dirty: DirtyEpoch,
}

impl FrozenEpoch {
    pub(super) fn changed_ranges(&self) -> Result<Vec<CommitRange>, PosixError> {
        let dirty = &self.dirty;
        let capacity = dirty
            .data
            .extents
            .len()
            .checked_add(dirty.data.external_extents.len())
            .ok_or(PosixError::OutOfMemory)?
            .checked_add(dirty.holes.ranges.len())
            .ok_or(PosixError::OutOfMemory)?;
        let mut ranges = Vec::<CommitRange>::new();
        ranges
            .try_reserve_exact(capacity)
            .map_err(|_| PosixError::OutOfMemory)?;
        for (&start, bytes) in &dirty.data.extents {
            ranges.push(CommitRange::new(
                start,
                u64::try_from(bytes.len()).expect("ASSERT: usize fits u64"),
            ));
        }
        for (&start, external) in &dirty.data.external_extents {
            ranges.push(CommitRange::new(start, external.length));
        }
        for (&start, &end) in &dirty.holes.ranges {
            ranges.push(CommitRange::new(start, end - start));
        }
        ranges.sort_unstable_by_key(|range| range.offset());
        let mut output = Vec::<CommitRange>::new();
        output
            .try_reserve_exact(ranges.len())
            .map_err(|_| PosixError::OutOfMemory)?;
        for range in ranges {
            let start = range.offset();
            let end = start.checked_add(range.length()).ok_or(PosixError::Io)?;
            if end > dirty.result_size || start >= end {
                return Err(PosixError::Io);
            }
            if let Some(previous) = output.last_mut()
                && previous
                    .offset()
                    .checked_add(previous.length())
                    .ok_or(PosixError::Io)?
                    >= start
            {
                let previous_end = previous
                    .offset()
                    .checked_add(previous.length())
                    .ok_or(PosixError::Io)?;
                previous.length = previous_end.max(end) - previous.offset();
            } else {
                output.push(CommitRange::new(start, end - start));
            }
        }
        Ok(output)
    }
}

#[derive(Clone, Debug)]
struct FrozenCommit {
    committed: Arc<dyn CommittedFile>,
    epoch: Arc<FrozenEpoch>,
    allocated_bytes: u64,
}

impl FrozenCommit {
    #[cfg(test)]
    fn token(&self) -> CommitToken {
        self.epoch.token
    }

    #[cfg(test)]
    fn through_sequence(&self) -> u64 {
        self.epoch.dirty.through_sequence
    }

    #[cfg(test)]
    const fn epoch(&self) -> &Arc<FrozenEpoch> {
        &self.epoch
    }

    fn plan_read(&self, offset: u64, length: u32) -> Result<ReadPlan, PosixError> {
        ReadPlan::new(
            Arc::clone(&self.committed),
            &[&self.epoch.dirty],
            self.epoch.dirty.result_size,
            offset,
            length,
        )
    }
}

impl CommittedFile for FrozenCommit {
    fn logical_size(&self) -> u64 {
        self.epoch.dirty.result_size
    }

    fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes
    }

    fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
        let end = offset.checked_add(length).ok_or(PosixError::Io)?;
        allocated_bytes_through(
            &self.committed,
            &[&self.epoch.dirty],
            offset,
            end.min(self.logical_size()),
        )
    }

    fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
        self.plan_read(offset, length)?.execute()
    }
}

#[derive(Debug)]
pub(super) struct VersionedFile {
    committed: Arc<dyn CommittedFile>,
    committed_sequence: u64,
    inflight: Option<Arc<FrozenEpoch>>,
    active: DirtyEpoch,
    live_allocated_bytes: u64,
}

impl VersionedFile {
    pub(super) fn new_empty() -> Self {
        Self::from_committed(Arc::new(EmptyCommittedFile), 0)
    }

    pub(super) fn from_committed(
        committed: Arc<dyn CommittedFile>,
        committed_sequence: u64,
    ) -> Self {
        let logical_size = committed.logical_size();
        let live_allocated_bytes = committed.allocated_bytes();
        Self {
            committed,
            committed_sequence,
            inflight: None,
            active: DirtyEpoch::new(logical_size, committed_sequence),
            live_allocated_bytes,
        }
    }

    pub(super) const fn logical_size(&self) -> u64 {
        self.active.result_size
    }

    pub(super) fn allocated_bytes(&self) -> u64 {
        self.live_allocated_bytes
    }

    pub(super) fn active_resident_payload_bytes(&self) -> u64 {
        self.active.data.resident_bytes()
    }

    pub(super) fn externalize_many(
        &mut self,
        candidates: Vec<(u64, u64, Arc<dyn CommittedFile>)>,
    ) -> Result<(), PosixError> {
        if candidates.is_empty() {
            return Ok(());
        }
        let mut accepted = false;
        let mut first_error = None;
        for (offset, through_sequence, source) in candidates {
            let candidate = (|| {
                if through_sequence <= self.active.base_sequence
                    || through_sequence > self.active.through_sequence
                {
                    return Err(PosixError::Again);
                }
                let end = offset
                    .checked_add(source.logical_size())
                    .ok_or(PosixError::Io)?;
                if !self.active.holes.overlapping_starts(offset, end).is_empty() {
                    return Err(PosixError::Again);
                }
                self.active
                    .data
                    .externalize_many(vec![(offset, through_sequence, source)])
            })();
            match candidate {
                Ok(()) => accepted = true,
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        self.active.assert_valid();
        if accepted {
            Ok(())
        } else {
            Err(first_error.expect("ASSERT: a nonempty candidate batch produced one result"))
        }
    }

    pub(super) fn write(
        &mut self,
        offset: u64,
        bytes: &[u8],
        sequence: u64,
    ) -> Result<(), PosixError> {
        let length = u64::try_from(bytes.len()).expect("ASSERT: usize must fit u64");
        let end = offset.checked_add(length).ok_or(PosixError::FileTooLarge)?;
        let overwritten_end = end.min(self.logical_size());
        let overwritten = if offset < overwritten_end {
            self.allocated_bytes_in_range(offset, overwritten_end)?
        } else {
            0
        };
        self.active.write(offset, bytes, sequence)?;
        self.live_allocated_bytes = self
            .live_allocated_bytes
            .checked_sub(overwritten)
            .and_then(|remaining| remaining.checked_add(length))
            .expect("ASSERT: allocated-byte replacement must remain bounded by logical size");
        assert!(
            self.live_allocated_bytes <= self.logical_size(),
            "ASSERT: allocated bytes must not exceed logical size"
        );
        Ok(())
    }

    pub(super) fn truncate(&mut self, length: u64, sequence: u64) -> Result<(), PosixError> {
        let removed = if length < self.logical_size() {
            self.allocated_bytes_in_range(length, self.logical_size())?
        } else {
            0
        };
        self.active.truncate(length, sequence)?;
        self.live_allocated_bytes = self
            .live_allocated_bytes
            .checked_sub(removed)
            .expect("ASSERT: truncated allocation must have been accounted");
        assert!(
            self.live_allocated_bytes <= self.logical_size(),
            "ASSERT: allocated bytes must not exceed logical size"
        );
        Ok(())
    }

    pub(super) fn advance_mutation_sequence(&mut self, sequence: u64) {
        self.active.assert_next_sequence(sequence);
        self.active.record_sequence(sequence);
        self.active.assert_valid();
    }

    fn allocated_bytes_in_range(&self, start: u64, end: u64) -> Result<u64, PosixError> {
        assert!(start <= end, "ASSERT: allocation range must be ordered");
        let mut layers = Vec::with_capacity(2);
        if let Some(inflight) = &self.inflight {
            layers.push(&inflight.dirty);
        }
        layers.push(&self.active);
        allocated_bytes_through(&self.committed, &layers, start, end)
    }

    pub(super) fn plan_read(&self, offset: u64, length: u32) -> Result<ReadPlan, PosixError> {
        let mut layers = Vec::with_capacity(2);
        if let Some(inflight) = &self.inflight {
            layers.push(&inflight.dirty);
        }
        layers.push(&self.active);
        ReadPlan::new(
            Arc::clone(&self.committed),
            &layers,
            self.active.result_size,
            offset,
            length,
        )
    }

    pub(super) const fn has_active_mutations(&self) -> bool {
        self.active.first_sequence.is_some()
    }

    pub(super) fn freeze_for_commit(
        &mut self,
        token: CommitToken,
    ) -> (Arc<dyn CommittedFile>, Option<Arc<FrozenEpoch>>) {
        match self.freeze_active(token) {
            Some(frozen) => {
                let epoch = Arc::clone(&frozen.epoch);
                (Arc::new(frozen), Some(epoch))
            }
            None => (Arc::clone(&self.committed), None),
        }
    }

    fn freeze_active(&mut self, token: CommitToken) -> Option<FrozenCommit> {
        assert!(
            self.inflight.is_none(),
            "ASSERT: only one commit epoch may be in flight"
        );
        self.active.first_sequence?;
        let result_size = self.active.result_size;
        let through_sequence = self.active.through_sequence;
        let dirty = std::mem::replace(
            &mut self.active,
            DirtyEpoch::new(result_size, through_sequence),
        );
        let epoch = Arc::new(FrozenEpoch { token, dirty });
        self.inflight = Some(Arc::clone(&epoch));
        Some(FrozenCommit {
            committed: Arc::clone(&self.committed),
            epoch,
            allocated_bytes: self.live_allocated_bytes,
        })
    }

    #[cfg(test)]
    fn install_committed(
        &mut self,
        token: CommitToken,
        committed: Arc<dyn CommittedFile>,
        committed_sequence: u64,
    ) {
        self.install_commit_view(token, committed, committed_sequence, true);
    }

    pub(super) fn install_commit_view(
        &mut self,
        token: CommitToken,
        committed: Arc<dyn CommittedFile>,
        committed_sequence: u64,
        had_frozen_epoch: bool,
    ) {
        let committed_size = committed.logical_size();
        if had_frozen_epoch {
            let inflight = self
                .inflight
                .take()
                .expect("ASSERT: install requires one frozen commit epoch");
            assert_eq!(inflight.token, token, "ASSERT: commit token must match");
            assert_eq!(
                inflight.dirty.result_size, committed_size,
                "ASSERT: installed committed size must match the frozen result"
            );
            assert_eq!(
                inflight.dirty.through_sequence, committed_sequence,
                "ASSERT: installed committed sequence must match the frozen prefix"
            );
        } else {
            assert!(
                self.inflight.is_none(),
                "ASSERT: unchanged commit inode cannot own a frozen epoch"
            );
            assert_eq!(
                self.committed_sequence, committed_sequence,
                "ASSERT: unchanged commit inode sequence must match its base"
            );
            assert_eq!(
                self.committed.logical_size(),
                committed_size,
                "ASSERT: unchanged commit inode size must match its base"
            );
        }
        assert_eq!(
            self.active.base_size, committed_size,
            "ASSERT: later active epoch must inherit the committed result"
        );
        assert_eq!(
            self.active.base_sequence, committed_sequence,
            "ASSERT: later active epoch must begin after the committed prefix"
        );
        if let Some(first_sequence) = self.active.first_sequence {
            assert!(
                first_sequence > committed_sequence,
                "ASSERT: active mutations after a cut must not be retired"
            );
        }
        self.committed = committed;
        self.committed_sequence = committed_sequence;
        let recomputed = self
            .allocated_bytes_in_range(0, self.logical_size())
            .expect("ASSERT: installed verified allocation metadata must remain readable");
        assert_eq!(
            recomputed, self.live_allocated_bytes,
            "ASSERT: installing a frozen prefix must preserve live allocated bytes"
        );
    }
}

fn allocated_bytes_through(
    committed: &Arc<dyn CommittedFile>,
    epochs: &[&DirtyEpoch],
    start: u64,
    end: u64,
) -> Result<u64, PosixError> {
    if start >= end {
        return Ok(0);
    }
    let Some((epoch, lower_epochs)) = epochs.split_last() else {
        let base_end = end.min(committed.logical_size());
        if start >= base_end {
            return Ok(0);
        }
        let length = base_end - start;
        let allocated = committed.allocated_bytes_in_range(start, length)?;
        return if allocated <= length {
            Ok(allocated)
        } else {
            Err(PosixError::Io)
        };
    };
    let effective_end = end.min(epoch.result_size);
    if start >= effective_end {
        return Ok(0);
    }
    let mut allocated = allocated_bytes_through(committed, lower_epochs, start, effective_end)?;
    for hole_start in epoch.holes.overlapping_starts(start, effective_end) {
        let overlap_start = hole_start.max(start);
        let overlap_end = epoch.holes.ranges[&hole_start].min(effective_end);
        allocated = allocated
            .checked_sub(allocated_bytes_through(
                committed,
                lower_epochs,
                overlap_start,
                overlap_end,
            )?)
            .ok_or(PosixError::Io)?;
    }
    let mut apply_data = |extent_start: u64, extent_length: u64| -> Result<(), PosixError> {
        let extent_end = extent_start
            .checked_add(extent_length)
            .expect("ASSERT: validated DATA extent must not overflow");
        let overlap_start = extent_start.max(start);
        let overlap_end = extent_end.min(effective_end);
        if overlap_start >= overlap_end {
            return Ok(());
        }
        let lower_allocated =
            allocated_bytes_through(committed, lower_epochs, overlap_start, overlap_end)?;
        allocated = allocated
            .checked_sub(lower_allocated)
            .and_then(|remaining| remaining.checked_add(overlap_end - overlap_start))
            .ok_or(PosixError::Io)?;
        Ok(())
    };
    if let Some((&extent_start, bytes)) = epoch.data.extents.range(..=start).next_back() {
        apply_data(
            extent_start,
            u64::try_from(bytes.len()).expect("ASSERT: usize fits u64"),
        )?;
    }
    for (&extent_start, bytes) in epoch
        .data
        .extents
        .range((Excluded(start), Excluded(effective_end)))
    {
        apply_data(
            extent_start,
            u64::try_from(bytes.len()).expect("ASSERT: usize fits u64"),
        )?;
    }
    if let Some((&extent_start, external)) = epoch.data.external_extents.range(..=start).next_back()
    {
        apply_data(extent_start, external.length)?;
    }
    for (&extent_start, external) in epoch
        .data
        .external_extents
        .range((Excluded(start), Excluded(effective_end)))
    {
        apply_data(extent_start, external.length)?;
    }
    if allocated <= effective_end - start {
        Ok(allocated)
    } else {
        Err(PosixError::Io)
    }
}

#[derive(Debug)]
struct PlannedData {
    start: u64,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct PlannedEpoch {
    result_size: u64,
    data: Vec<PlannedData>,
    holes: Vec<(u64, u64)>,
}

impl PlannedEpoch {
    fn new(epoch: &DirtyEpoch, read_start: u64, read_end: u64) -> Result<Self, PosixError> {
        if read_start >= read_end {
            return Ok(Self {
                result_size: epoch.result_size,
                data: Vec::new(),
                holes: Vec::new(),
            });
        }
        let mut data = Vec::new();
        if let Some((&extent_start, bytes)) = epoch.data.extents.range(..=read_start).next_back() {
            plan_data(&mut data, read_start, read_end, extent_start, bytes)?;
        }
        for (&extent_start, bytes) in epoch
            .data
            .extents
            .range((Excluded(read_start), Excluded(read_end)))
        {
            plan_data(&mut data, read_start, read_end, extent_start, bytes)?;
        }
        if let Some((&extent_start, external)) =
            epoch.data.external_extents.range(..=read_start).next_back()
        {
            plan_external(&mut data, read_start, read_end, extent_start, external)?;
        }
        for (&extent_start, external) in epoch
            .data
            .external_extents
            .range((Excluded(read_start), Excluded(read_end)))
        {
            plan_external(&mut data, read_start, read_end, extent_start, external)?;
        }
        let holes = epoch
            .holes
            .overlapping_starts(read_start, read_end)
            .into_iter()
            .map(|start| {
                (
                    start.max(read_start),
                    epoch.holes.ranges[&start].min(read_end),
                )
            })
            .collect();
        Ok(Self {
            result_size: epoch.result_size,
            data,
            holes,
        })
    }

    fn apply(&self, output: &mut [u8], read_start: u64, read_end: u64) {
        if self.result_size < read_end {
            zero_range(
                output,
                read_start,
                read_end,
                self.result_size.max(read_start),
                read_end,
            );
        }
        for &(hole_start, hole_end) in &self.holes {
            zero_range(output, read_start, read_end, hole_start, hole_end);
        }
        for extent in &self.data {
            overlay_bytes(output, read_start, read_end, extent.start, &extent.bytes);
        }
    }
}

#[derive(Debug)]
pub(super) struct ReadPlan {
    committed: Arc<dyn CommittedFile>,
    read_start: u64,
    read_end: u64,
    epochs: Vec<PlannedEpoch>,
}

impl ReadPlan {
    fn new(
        committed: Arc<dyn CommittedFile>,
        epochs: &[&DirtyEpoch],
        logical_size: u64,
        offset: u64,
        length: u32,
    ) -> Result<Self, PosixError> {
        let read_end = offset.saturating_add(u64::from(length)).min(logical_size);
        let epochs = epochs
            .iter()
            .map(|epoch| PlannedEpoch::new(epoch, offset, read_end))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            committed,
            read_start: offset,
            read_end,
            epochs,
        })
    }

    pub(super) fn execute(self) -> Result<Vec<u8>, PosixError> {
        if self.read_start >= self.read_end {
            return Ok(Vec::new());
        }
        let output_length = usize::try_from(self.read_end - self.read_start)
            .map_err(|_| PosixError::FileTooLarge)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(output_length)
            .map_err(|_| PosixError::OutOfMemory)?;
        output.resize(output_length, 0);

        let committed_end = self.read_end.min(self.committed.logical_size());
        if self.read_start < committed_end {
            let committed_length = u32::try_from(committed_end - self.read_start)
                .expect("ASSERT: a planned read length originated from u32");
            let committed = self.committed.read_at(self.read_start, committed_length)?;
            if committed.len()
                != usize::try_from(committed_length)
                    .expect("ASSERT: u32 read length must fit usize")
            {
                return Err(PosixError::Io);
            }
            output[..committed.len()].copy_from_slice(&committed);
        }
        for epoch in &self.epochs {
            epoch.apply(&mut output, self.read_start, self.read_end);
        }
        Ok(output)
    }
}

fn plan_data(
    planned: &mut Vec<PlannedData>,
    read_start: u64,
    read_end: u64,
    extent_start: u64,
    bytes: &[u8],
) -> Result<(), PosixError> {
    let extent_end = extent_start
        .checked_add(u64::try_from(bytes.len()).expect("ASSERT: usize must fit u64"))
        .expect("ASSERT: validated DATA extent must not overflow");
    let copy_start = extent_start.max(read_start);
    let copy_end = extent_end.min(read_end);
    if copy_start >= copy_end {
        return Ok(());
    }
    let source_start =
        usize::try_from(copy_start - extent_start).expect("ASSERT: source offset must fit usize");
    let source_end =
        usize::try_from(copy_end - extent_start).expect("ASSERT: source end must fit usize");
    planned.push(PlannedData {
        start: copy_start,
        bytes: copy_bytes(&bytes[source_start..source_end])?,
    });
    Ok(())
}

fn plan_external(
    planned: &mut Vec<PlannedData>,
    read_start: u64,
    read_end: u64,
    extent_start: u64,
    external: &ExternalDirtyData,
) -> Result<(), PosixError> {
    let extent_end = extent_start
        .checked_add(external.length)
        .ok_or(PosixError::Io)?;
    let copy_start = extent_start.max(read_start);
    let copy_end = extent_end.min(read_end);
    if copy_start >= copy_end {
        return Ok(());
    }
    let source_offset = external
        .source_offset
        .checked_add(copy_start - extent_start)
        .ok_or(PosixError::Io)?;
    let length = u32::try_from(copy_end - copy_start).map_err(|_| PosixError::FileTooLarge)?;
    let bytes = external.source.read_at(source_offset, length)?;
    if bytes.len() != usize::try_from(length).expect("ASSERT: u32 fits usize") {
        return Err(PosixError::Io);
    }
    planned.push(PlannedData {
        start: copy_start,
        bytes,
    });
    Ok(())
}

fn overlay_bytes(
    output: &mut [u8],
    read_start: u64,
    read_end: u64,
    extent_start: u64,
    bytes: &[u8],
) {
    let extent_end = extent_start
        .checked_add(u64::try_from(bytes.len()).expect("ASSERT: usize must fit u64"))
        .expect("ASSERT: planned DATA extent must not overflow");
    let copy_start = extent_start.max(read_start);
    let copy_end = extent_end.min(read_end);
    if copy_start >= copy_end {
        return;
    }
    let source_start =
        usize::try_from(copy_start - extent_start).expect("ASSERT: source offset must fit usize");
    let source_end =
        usize::try_from(copy_end - extent_start).expect("ASSERT: source end must fit usize");
    let target_start =
        usize::try_from(copy_start - read_start).expect("ASSERT: target offset must fit usize");
    let target_end =
        usize::try_from(copy_end - read_start).expect("ASSERT: target end must fit usize");
    output[target_start..target_end].copy_from_slice(&bytes[source_start..source_end]);
}

fn zero_range(output: &mut [u8], read_start: u64, read_end: u64, zero_start: u64, zero_end: u64) {
    let start = zero_start.max(read_start);
    let end = zero_end.min(read_end);
    if start >= end {
        return;
    }
    let target_start =
        usize::try_from(start - read_start).expect("ASSERT: target offset must fit usize");
    let target_end = usize::try_from(end - read_start).expect("ASSERT: target end must fit usize");
    output[target_start..target_end].fill(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct FillReader {
        length: u64,
        fill: u8,
        patches: Vec<(u64, Vec<u8>)>,
        reads: Arc<Mutex<Vec<(u64, u32)>>>,
    }

    impl CommittedFile for FillReader {
        fn logical_size(&self) -> u64 {
            self.length
        }

        fn allocated_bytes(&self) -> u64 {
            self.length
        }

        fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
            Ok(offset.saturating_add(length).min(self.length) - offset.min(self.length))
        }

        fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
            self.reads
                .lock()
                .expect("ASSERT: test read log lock poisoned")
                .push((offset, length));
            let end = offset.saturating_add(u64::from(length)).min(self.length);
            let mut bytes = vec![
                self.fill;
                usize::try_from(end - offset)
                    .expect("ASSERT: bounded test read must fit usize")
            ];
            for (patch_start, patch) in &self.patches {
                overlay_bytes(&mut bytes, offset, end, *patch_start, patch);
            }
            Ok(bytes)
        }
    }

    fn fill_reader(length: u64, patches: Vec<(u64, Vec<u8>)>) -> Arc<dyn CommittedFile> {
        Arc::new(FillReader {
            length,
            fill: b'a',
            patches,
            reads: Arc::new(Mutex::new(Vec::new())),
        })
    }

    #[derive(Debug)]
    struct BytesReader {
        bytes: Vec<u8>,
        allocated: Vec<bool>,
    }

    impl CommittedFile for BytesReader {
        fn logical_size(&self) -> u64 {
            u64::try_from(self.bytes.len()).expect("ASSERT: usize must fit u64")
        }

        fn allocated_bytes(&self) -> u64 {
            u64::try_from(
                self.allocated
                    .iter()
                    .filter(|allocated| **allocated)
                    .count(),
            )
            .expect("ASSERT: usize must fit u64")
        }

        fn allocated_bytes_in_range(&self, offset: u64, length: u64) -> Result<u64, PosixError> {
            let start = usize::try_from(offset.min(self.logical_size()))
                .expect("ASSERT: bounded offset must fit usize");
            let end = usize::try_from(offset.saturating_add(length).min(self.logical_size()))
                .expect("ASSERT: bounded end must fit usize");
            Ok(u64::try_from(
                self.allocated[start..end]
                    .iter()
                    .filter(|allocated| **allocated)
                    .count(),
            )
            .expect("ASSERT: usize must fit u64"))
        }

        fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, PosixError> {
            let start = usize::try_from(offset).map_err(|_| PosixError::FileTooLarge)?;
            let end = start
                .saturating_add(usize::try_from(length).expect("ASSERT: u32 must fit usize"))
                .min(self.bytes.len());
            Ok(self.bytes[start.min(end)..end].to_vec())
        }
    }

    fn bytes_reader(bytes: Vec<u8>) -> Arc<dyn CommittedFile> {
        let allocated = vec![true; bytes.len()];
        sparse_bytes_reader(bytes, allocated)
    }

    fn sparse_bytes_reader(bytes: Vec<u8>, allocated: Vec<bool>) -> Arc<dyn CommittedFile> {
        assert_eq!(
            bytes.len(),
            allocated.len(),
            "ASSERT: test content and allocation maps must agree"
        );
        Arc::new(BytesReader { bytes, allocated })
    }

    #[test]
    fn verified_external_dirty_extent_releases_resident_bytes_and_survives_updates() {
        let mut file = VersionedFile::new_empty();
        file.write(0, b"abcdefgh", 1).expect("initial write");
        assert_eq!(file.active_resident_payload_bytes(), 8);

        file.externalize_many(vec![(2, 1, bytes_reader(b"cdef".to_vec()))])
            .expect("matching verified source must externalize");
        assert_eq!(file.active_resident_payload_bytes(), 4);
        assert_eq!(
            file.plan_read(0, 8)
                .expect("externalized read plan")
                .execute()
                .expect("externalized read"),
            b"abcdefgh"
        );

        file.write(3, b"XY", 2)
            .expect("later write splits external source");
        assert_eq!(
            file.plan_read(0, 8)
                .expect("updated read plan")
                .execute()
                .expect("updated read"),
            b"abcXYfgh"
        );
        file.truncate(6, 3)
            .expect("truncate clips an external fragment");
        assert_eq!(
            file.plan_read(0, 8)
                .expect("truncated read plan")
                .execute()
                .expect("truncated read"),
            b"abcXYf"
        );
    }

    #[test]
    fn mismatched_external_source_preserves_the_resident_fallback() {
        let mut file = VersionedFile::new_empty();
        file.write(0, b"abcdefgh", 1).expect("initial write");

        assert_eq!(
            file.externalize_many(vec![(2, 1, bytes_reader(b"WRNG".to_vec()))]),
            Err(PosixError::Io)
        );
        assert_eq!(file.active_resident_payload_bytes(), 8);
        assert_eq!(
            file.plan_read(0, 8)
                .expect("fallback read plan")
                .execute()
                .expect("fallback read"),
            b"abcdefgh"
        );
    }

    #[test]
    fn stale_cross_cut_candidate_does_not_reject_a_valid_active_candidate() {
        let mut file = VersionedFile::new_empty();
        file.write(0, b"abcd", 1).expect("write frozen prefix");
        let cut = file
            .freeze_active(CommitToken::new(1).expect("token is nonzero"))
            .expect("prefix creates a frozen cut");
        file.write(4, b"efgh", 2).expect("write active suffix");

        file.externalize_many(vec![
            (0, 2, bytes_reader(b"abcd".to_vec())),
            (4, 2, bytes_reader(b"efgh".to_vec())),
        ])
        .expect("one stale candidate must not reject its valid sibling");
        assert_eq!(file.active_resident_payload_bytes(), 0);
        assert_eq!(
            file.plan_read(0, 8)
                .expect("cross-cut read plan")
                .execute()
                .expect("cross-cut read"),
            b"abcdefgh"
        );
        drop(cut);
    }

    #[test]
    fn commit_cut_install_and_retirement_preserve_a_later_write() {
        let length = 1_u64 << 40;
        let mut file = VersionedFile::from_committed(fill_reader(length, Vec::new()), 0);

        file.write(2, b"bb", 1).expect("W1 must be admitted");
        let cut = file
            .freeze_active(CommitToken::new(1).expect("token is nonzero"))
            .expect("W1 creates a commit cut");
        let retired = Arc::downgrade(cut.epoch());

        file.write(3, b"c", 2).expect("W2 must remain active");
        assert_eq!(
            file.plan_read(0, 5)
                .expect("live read plan")
                .execute()
                .expect("live read"),
            b"aabca"
        );
        assert_eq!(
            cut.plan_read(0, 5)
                .expect("cut read plan")
                .execute()
                .expect("cut read"),
            b"aabba"
        );

        file.install_committed(
            cut.token(),
            fill_reader(length, vec![(2, b"bb".to_vec())]),
            cut.through_sequence(),
        );
        assert_eq!(
            file.plan_read(0, 5)
                .expect("post-install live read plan")
                .execute()
                .expect("post-install live read"),
            b"aabca"
        );
        drop(cut);
        assert!(
            retired.upgrade().is_none(),
            "the installed W1 epoch must retire after the publisher drops its cut"
        );
    }

    #[test]
    fn shrink_then_grow_records_a_hole_instead_of_resurrecting_base_bytes() {
        let mut file =
            VersionedFile::from_committed(fill_reader(8, vec![(0, b"abcdefgh".to_vec())]), 0);
        file.truncate(3, 1).expect("shrink must succeed");
        file.truncate(7, 2).expect("growth must succeed");
        assert_eq!(
            file.plan_read(0, 8)
                .expect("read plan")
                .execute()
                .expect("read"),
            b"abc\0\0\0\0"
        );
    }

    #[test]
    fn layered_writes_truncates_and_commit_cuts_match_a_dense_oracle() {
        let mut oracle = (0_u8..=250).collect::<Vec<_>>();
        let mut oracle_allocated = vec![true; oracle.len()];
        let mut file = VersionedFile::from_committed(bytes_reader(oracle.clone()), 0);
        let mut sequence = 0_u64;
        let mut token = 0_u64;
        let mut random = 0x4d59_5df4_d0f3_3173_u64;

        for operation in 0..512 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            sequence += 1;
            if random.is_multiple_of(4) {
                let length = usize::try_from((random >> 11) % 384)
                    .expect("ASSERT: bounded test length fits usize");
                oracle.resize(length, 0);
                oracle_allocated.resize(length, false);
                file.truncate(
                    u64::try_from(length).expect("ASSERT: usize must fit u64"),
                    sequence,
                )
                .expect("differential truncate must succeed");
            } else {
                let offset = usize::try_from((random >> 13) % 384)
                    .expect("ASSERT: bounded offset fits usize");
                let length = usize::try_from(((random >> 29) % 23) + 1)
                    .expect("ASSERT: bounded length fits usize");
                let value = random.to_le_bytes();
                let bytes = (0..length)
                    .map(|index| value[index % value.len()])
                    .collect::<Vec<_>>();
                let end = offset + length;
                if oracle.len() < end {
                    oracle.resize(end, 0);
                    oracle_allocated.resize(end, false);
                }
                oracle[offset..end].copy_from_slice(&bytes);
                oracle_allocated[offset..end].fill(true);
                file.write(
                    u64::try_from(offset).expect("ASSERT: usize must fit u64"),
                    &bytes,
                    sequence,
                )
                .expect("differential write must succeed");
            }

            let read_offset = (random >> 37) % 448;
            let read_length =
                u32::try_from((random >> 53) % 97).expect("ASSERT: bounded read length fits u32");
            let expected_start = usize::try_from(read_offset)
                .expect("ASSERT: bounded read offset fits usize")
                .min(oracle.len());
            let expected_end = expected_start
                .saturating_add(usize::try_from(read_length).expect("ASSERT: u32 must fit usize"))
                .min(oracle.len());
            assert_eq!(
                file.plan_read(read_offset, read_length)
                    .expect("differential read plan")
                    .execute()
                    .expect("differential read"),
                oracle[expected_start..expected_end],
                "operation {operation}"
            );

            if operation % 19 == 18 {
                token += 1;
                let cut = file
                    .freeze_active(CommitToken::new(token).expect("token is nonzero"))
                    .expect("each interval contains dirty mutations");
                let committed = oracle.clone();
                let committed_allocated = oracle_allocated.clone();

                sequence += 1;
                let later_offset = operation % 31;
                let later_value =
                    [u8::try_from(operation % 251).expect("ASSERT: bounded value fits u8")];
                if oracle.len() <= later_offset {
                    oracle.resize(later_offset + 1, 0);
                    oracle_allocated.resize(later_offset + 1, false);
                }
                oracle[later_offset] = later_value[0];
                oracle_allocated[later_offset] = true;
                file.write(
                    u64::try_from(later_offset).expect("ASSERT: usize must fit u64"),
                    &later_value,
                    sequence,
                )
                .expect("post-cut write must succeed");

                file.install_committed(
                    cut.token(),
                    sparse_bytes_reader(committed, committed_allocated),
                    cut.through_sequence(),
                );
                assert_eq!(
                    file.plan_read(0, u32::try_from(oracle.len()).expect("test file fits u32"),)
                        .expect("post-install read plan")
                        .execute()
                        .expect("post-install read"),
                    oracle,
                    "operation {operation} after installing the frozen prefix"
                );
            }
        }
    }

    #[test]
    fn a_short_committed_read_is_an_io_error_not_partial_data() {
        #[derive(Debug)]
        struct ShortReader;

        impl CommittedFile for ShortReader {
            fn logical_size(&self) -> u64 {
                8
            }

            fn allocated_bytes(&self) -> u64 {
                8
            }

            fn allocated_bytes_in_range(
                &self,
                offset: u64,
                length: u64,
            ) -> Result<u64, PosixError> {
                Ok(offset.saturating_add(length).min(8) - offset.min(8))
            }

            fn read_at(&self, _offset: u64, _length: u32) -> Result<Vec<u8>, PosixError> {
                Ok(b"short".to_vec())
            }
        }

        let file = VersionedFile::from_committed(Arc::new(ShortReader), 0);
        assert_eq!(
            file.plan_read(0, 8).expect("read plan").execute(),
            Err(PosixError::Io)
        );
    }
}
