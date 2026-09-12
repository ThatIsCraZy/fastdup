//! Process-local Metadata root ownership and conservative invalidation when pins drain.
use super::metadata_gc::mark_metadata_gc_exact_required;
use super::{
    GenerationRepository, MetadataGcExactReason, MetadataRootPin, MetadataRootPinInner,
    RecoveryCheckpointRootPin,
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
        let mut pins = self
            .pins
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pin registry poisoned during release");
        let remove = match pins.get_mut(&self.root) {
            Some(1) => true,
            Some(count) => {
                *count -= 1;
                false
            }
            None => panic!("ASSERT: Recovery Checkpoint root pin release has an acquisition"),
        };
        if remove {
            pins.remove(&self.root);
        }
        drop(pins);
        mark_metadata_gc_exact_required(
            &self.metadata_gc_epoch,
            &self.metadata_gc_delta,
            MetadataGcExactReason::RecoveryCheckpointPinChange,
        );
    }
}

impl<I: StorageIo> GenerationRepository<I> {
    pub(super) fn pin_recovery_checkpoint_root(
        &self,
        root: MetadataObjectId,
    ) -> RecoveryCheckpointRootPin {
        let mut pins = self
            .recovery_checkpoint_root_pins
            .lock()
            .expect("ASSERT: Recovery Checkpoint root pin registry poisoned");
        let count = pins.entry(root).or_insert(0);
        *count = count
            .checked_add(1)
            .expect("ASSERT: Recovery Checkpoint root pin count cannot overflow");
        drop(pins);
        mark_metadata_gc_exact_required(
            &self.metadata_gc_epoch,
            &self.metadata_gc_delta,
            MetadataGcExactReason::RecoveryCheckpointPinChange,
        );
        RecoveryCheckpointRootPin {
            root,
            pins: Arc::clone(&self.recovery_checkpoint_root_pins),
            metadata_gc_epoch: Arc::clone(&self.metadata_gc_epoch),
            metadata_gc_delta: Arc::clone(&self.metadata_gc_delta),
        }
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
}
