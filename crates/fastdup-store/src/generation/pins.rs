//! Process-local Metadata root ownership and conservative invalidation when pins drain.
use super::metadata_gc::mark_metadata_gc_exact_required;
use super::{
    GenerationRepository, MetadataGcExactReason, MetadataRootPin, MetadataRootPinInner,
    RecoveryCheckpointRootPin, RecoveryCheckpointRootPinInner,
};
use crate::StorageIo;
use fastdup_format::MetadataObjectId;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

impl fmt::Debug for MetadataRootPin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataRootPin")
            .field("root", &self.inner.root)
            .finish_non_exhaustive()
    }
}

impl Drop for MetadataRootPinInner {
    fn drop(&mut self) {
        let mut pins = self
            .pins
            .lock()
            .expect("ASSERT: Metadata root pin registry poisoned during release");
        let remove = match pins.get_mut(&self.root) {
            Some(count) => {
                assert_ne!(
                    *count, 0,
                    "ASSERT: registered Metadata root pin count is nonzero"
                );
                if *count == 1 {
                    true
                } else {
                    *count -= 1;
                    false
                }
            }
            None => panic!("ASSERT: Metadata root pin release has an acquisition"),
        };
        if remove {
            pins.remove(&self.root);
        }
        drop(pins);
        if self.release_requires_exact.load(Ordering::Acquire) {
            mark_metadata_gc_exact_required(
                &self.metadata_gc_epoch,
                &self.metadata_gc_delta,
                MetadataGcExactReason::MetadataRootPinDrain,
            );
        }
    }
}

impl Drop for RecoveryCheckpointRootPin {
    fn drop(&mut self) {
        let inner = &self.inner;
        let mut pins = inner
            .pins
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pin registry poisoned during release");
        let remove = match pins.get_mut(&inner.root) {
            Some(1) => true,
            Some(count) => {
                *count -= 1;
                false
            }
            None => panic!("ASSERT: Recovery Checkpoint root pin release has an acquisition"),
        };
        if remove {
            pins.remove(&inner.root);
        }
        drop(pins);
        if remove && inner.release_requires_exact.load(Ordering::Acquire) {
            mark_metadata_gc_exact_required(
                &inner.metadata_gc_epoch,
                &inner.metadata_gc_delta,
                MetadataGcExactReason::RecoveryCheckpointPinChange,
            );
        }
    }
}

impl<I: StorageIo> GenerationRepository<I> {
    /// Recovery-Checkpoint invalidation tracks the protected root set, not the
    /// transient refcount used to overlap one candidate publication with its
    /// predecessor. Duplicate acquisition and non-final release leave that set
    /// unchanged, so the clean Metadata mark remains valid.
    ///
    /// When the selected root is already covered by the retained Commit WAL,
    /// acquisition is liveness-preserving rather than a root-set expansion that
    /// requires a fresh exact mark. A later WAL rotation re-arms such pins before
    /// it can evict the covering Commit Record.
    pub(super) fn pin_recovery_checkpoint_root(
        &self,
        root: MetadataObjectId,
        covered_by_commit: bool,
    ) -> RecoveryCheckpointRootPin {
        let mut pins = self
            .recovery_checkpoint_root_pins
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pin registry poisoned");
        let newly_protected = !pins.contains_key(&root);
        let count = pins.entry(root).or_insert(0);
        *count = count
            .checked_add(1)
            .expect("ASSERT: Recovery Checkpoint root pin count cannot overflow");
        drop(pins);
        if newly_protected && !covered_by_commit {
            mark_metadata_gc_exact_required(
                &self.metadata_gc_epoch,
                &self.metadata_gc_delta,
                MetadataGcExactReason::RecoveryCheckpointPinChange,
            );
        }
        let inner = Arc::new(RecoveryCheckpointRootPinInner {
            root,
            pins: Arc::clone(&self.recovery_checkpoint_root_pins),
            metadata_gc_epoch: Arc::clone(&self.metadata_gc_epoch),
            metadata_gc_delta: Arc::clone(&self.metadata_gc_delta),
            release_requires_exact: AtomicBool::new(!covered_by_commit),
        });
        self.recovery_checkpoint_root_pin_handles
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pin handle registry poisoned")
            .push(Arc::downgrade(&inner));
        RecoveryCheckpointRootPin { inner }
    }

    pub(super) fn pin_metadata_root(&self, root: MetadataObjectId) -> MetadataRootPin {
        let mut pins = self
            .metadata_root_pins
            .lock()
            .expect("ASSERT: Metadata root pin registry poisoned during acquisition");
        let count = pins.entry(root).or_insert(0);
        *count = count
            .checked_add(1)
            .expect("ASSERT: Metadata root pin count cannot overflow");
        drop(pins);
        let inner = Arc::new(MetadataRootPinInner {
            root,
            pins: Arc::clone(&self.metadata_root_pins),
            metadata_gc_epoch: Arc::clone(&self.metadata_gc_epoch),
            release_requires_exact: AtomicBool::new(true),
            metadata_gc_delta: Arc::clone(&self.metadata_gc_delta),
        });
        self.metadata_root_pin_handles
            .lock()
            .expect("ASSERT: Metadata root pin handle registry poisoned")
            .push(Arc::downgrade(&inner));
        MetadataRootPin { inner }
    }

    pub(super) fn mark_metadata_root_releases_covered_by_commit(
        &self,
        roots: &BTreeSet<MetadataObjectId>,
    ) {
        let mut handles = self
            .metadata_root_pin_handles
            .lock()
            .expect("ASSERT: Metadata root pin handle registry poisoned");
        handles.retain(|handle| {
            let Some(inner) = handle.upgrade() else {
                return false;
            };
            if roots.contains(&inner.root) {
                inner.release_requires_exact.store(false, Ordering::Release);
            }
            true
        });
    }

    pub(super) fn mark_all_metadata_root_pin_releases_exact(&self) {
        let mut handles = self
            .metadata_root_pin_handles
            .lock()
            .expect("ASSERT: Metadata root pin handle registry poisoned");
        handles.retain(|handle| {
            let Some(inner) = handle.upgrade() else {
                return false;
            };
            inner.release_requires_exact.store(true, Ordering::Release);
            true
        });
    }

    pub(super) fn mark_all_recovery_checkpoint_root_releases_exact(&self) {
        let mut handles = self
            .recovery_checkpoint_root_pin_handles
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pin handle registry poisoned");
        handles.retain(|handle| {
            let Some(inner) = handle.upgrade() else {
                return false;
            };
            inner.release_requires_exact.store(true, Ordering::Release);
            true
        });
    }

    pub(super) fn cleanup_recovery_checkpoint_root_pin_handles(&self) {
        let mut handles = self
            .recovery_checkpoint_root_pin_handles
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pin handle registry poisoned");
        handles.retain(|handle| handle.upgrade().is_some());
    }
}

#[cfg(test)]
mod tests {
    use super::{GenerationRepository, MetadataGcExactReason};
    use fastdup_format::{MetadataObjectId, PolicySetId};

    #[test]
    fn recovery_checkpoint_pins_invalidate_only_at_protected_root_set_edges() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock follows epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "fastdup-recovery-checkpoint-pin-{}-{nonce}",
            std::process::id()
        ));
        let storage = crate::FsStorageIo::open(&root).expect("open pin test root");
        let repository =
            GenerationRepository::new(storage, PolicySetId::new([1; 32]).expect("fixture policy"));
        let epoch = Arc::clone(&repository.metadata_gc_epoch);
        let root_id = MetadataObjectId::new([7; 32]).expect("fixture root is nonzero");

        let before = epoch.load(Ordering::Acquire);
        let first = repository.pin_recovery_checkpoint_root(root_id, false);
        let after_first_pin = epoch.load(Ordering::Acquire);
        assert_ne!(
            after_first_pin, before,
            "a newly protected checkpoint root requires an exact refresh"
        );

        let second = repository.pin_recovery_checkpoint_root(root_id, false);
        assert_eq!(
            epoch.load(Ordering::Acquire),
            after_first_pin,
            "a duplicate pin does not change the protected root set"
        );
        drop(second);
        assert_eq!(
            epoch.load(Ordering::Acquire),
            after_first_pin,
            "a non-final release does not change the protected root set"
        );

        drop(first);
        assert_ne!(
            epoch.load(Ordering::Acquire),
            after_first_pin,
            "removing the last pin changes the protected root set"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn wal_rotation_rearms_wal_covered_recovery_checkpoint_root_releases() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock follows epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "fastdup-recovery-checkpoint-pin-covered-{}-{nonce}",
            std::process::id()
        ));
        let storage = crate::FsStorageIo::open(&root).expect("open pin test root");
        let repository =
            GenerationRepository::new(storage, PolicySetId::new([1; 32]).expect("fixture policy"));
        let epoch = Arc::clone(&repository.metadata_gc_epoch);
        let root_id = MetadataObjectId::new([8; 32]).expect("fixture root is nonzero");

        let before = epoch.load(Ordering::Acquire);
        let pin = repository.pin_recovery_checkpoint_root(root_id, true);
        assert_eq!(
            epoch.load(Ordering::Acquire),
            before,
            "a WAL-covered checkpoint root pin does not invalidate a clean Metadata mark"
        );
        assert!(
            !repository
                .metadata_gc_delta
                .lock()
                .expect("journal lock")
                .exact_required,
            "a WAL-covered checkpoint root pin does not require an exact Metadata mark"
        );

        repository.mark_all_recovery_checkpoint_root_releases_exact();
        assert_eq!(
            epoch.load(Ordering::Acquire),
            before,
            "re-arming releases does not itself invalidate the current mark"
        );

        drop(pin);
        assert_ne!(
            epoch.load(Ordering::Acquire),
            before,
            "a re-armed checkpoint root release invalidates the clean Metadata mark"
        );
        let journal = repository.metadata_gc_delta.lock().expect("journal lock");
        assert!(journal.exact_required);
        assert_eq!(
            journal.exact_reason,
            Some(MetadataGcExactReason::RecoveryCheckpointPinChange)
        );

        let _ = std::fs::remove_dir_all(root);
    }

    use std::sync::Arc;
    use std::sync::atomic::Ordering;
}
