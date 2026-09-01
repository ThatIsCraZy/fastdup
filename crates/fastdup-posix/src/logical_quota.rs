use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::{InodeId, PosixError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalQuotaRule {
    root_inode: InodeId,
    limit_bytes: u64,
}

impl LogicalQuotaRule {
    /// Creates one subtree quota rooted at a managed directory.
    ///
    /// # Errors
    ///
    /// Rejects a zero-byte limit.
    pub const fn new(root_inode: InodeId, limit_bytes: u64) -> Result<Self, PosixError> {
        if limit_bytes == 0 {
            return Err(PosixError::InvalidArgument);
        }
        Ok(Self {
            root_inode,
            limit_bytes,
        })
    }

    #[must_use]
    pub const fn root_inode(self) -> InodeId {
        self.root_inode
    }

    #[must_use]
    pub const fn limit_bytes(self) -> u64 {
        self.limit_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalQuotaStatus {
    pub revision: String,
    pub root_inode: InodeId,
    pub limit_bytes: u64,
    pub used_bytes: u64,
}

#[derive(Debug)]
struct QuotaBucket {
    root_inode: InodeId,
    limit_bytes: u64,
    used_bytes: AtomicU64,
}

impl QuotaBucket {
    fn new(root_inode: InodeId, limit_bytes: u64, used_bytes: u64) -> Result<Self, PosixError> {
        if limit_bytes == 0 || used_bytes > limit_bytes {
            return Err(PosixError::NoSpace);
        }
        Ok(Self {
            root_inode,
            limit_bytes,
            used_bytes: AtomicU64::new(used_bytes),
        })
    }

    fn try_reserve(self: &Arc<Self>, bytes: u64) -> Result<QuotaReservation, PosixError> {
        if bytes == 0 {
            return Ok(QuotaReservation::empty());
        }
        loop {
            let used = self.used_bytes.load(Ordering::Acquire);
            let next = used.checked_add(bytes).ok_or(PosixError::NoSpace)?;
            if next > self.limit_bytes {
                return Err(PosixError::NoSpace);
            }
            if self
                .used_bytes
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(QuotaReservation {
                    bucket: Some(Arc::clone(self)),
                    reserved_bytes: bytes,
                    accepted: false,
                });
            }
        }
    }

    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(bytes)
            })
            .expect("ASSERT: logical quota release must have been accounted");
    }
}

#[derive(Debug, Default)]
struct QuotaIndex {
    revision: String,
    by_root: BTreeMap<InodeId, Arc<QuotaBucket>>,
    by_inode: BTreeMap<InodeId, Arc<QuotaBucket>>,
}

#[derive(Debug, Default)]
pub(crate) struct LogicalQuotaTable {
    index: RwLock<QuotaIndex>,
}

impl LogicalQuotaTable {
    pub(crate) fn replace(
        &self,
        revision: String,
        rules: &BTreeMap<InodeId, u64>,
        membership: BTreeMap<InodeId, InodeId>,
        usage: &BTreeMap<InodeId, u64>,
    ) -> Result<(), PosixError> {
        if revision.is_empty() || revision.len() > 128 {
            return Err(PosixError::InvalidArgument);
        }
        let mut by_root = BTreeMap::new();
        for (&root_inode, &limit_bytes) in rules {
            let bucket = Arc::new(QuotaBucket::new(
                root_inode,
                limit_bytes,
                usage.get(&root_inode).copied().unwrap_or(0),
            )?);
            by_root.insert(root_inode, bucket);
        }
        let mut by_inode = BTreeMap::new();
        for (inode, root_inode) in membership {
            let bucket = by_root
                .get(&root_inode)
                .cloned()
                .ok_or(PosixError::InvalidArgument)?;
            by_inode.insert(inode, bucket);
        }
        let mut index = self
            .index
            .write()
            .expect("ASSERT: logical quota index lock poisoned");
        *index = QuotaIndex {
            revision,
            by_root,
            by_inode,
        };
        Ok(())
    }

    pub(crate) fn revision(&self) -> String {
        self.index
            .read()
            .expect("ASSERT: logical quota index lock poisoned")
            .revision
            .clone()
    }

    pub(crate) fn status(&self, root_inode: InodeId) -> Option<LogicalQuotaStatus> {
        let index = self
            .index
            .read()
            .expect("ASSERT: logical quota index lock poisoned");
        let bucket = index.by_root.get(&root_inode)?;
        Some(LogicalQuotaStatus {
            revision: index.revision.clone(),
            root_inode: bucket.root_inode,
            limit_bytes: bucket.limit_bytes,
            used_bytes: bucket.used_bytes.load(Ordering::Acquire),
        })
    }

    pub(crate) fn status_for_inode(&self, inode: InodeId) -> Option<LogicalQuotaStatus> {
        let index = self
            .index
            .read()
            .expect("ASSERT: logical quota index lock poisoned");
        let bucket = index.by_inode.get(&inode)?;
        Some(LogicalQuotaStatus {
            revision: index.revision.clone(),
            root_inode: bucket.root_inode,
            limit_bytes: bucket.limit_bytes,
            used_bytes: bucket.used_bytes.load(Ordering::Acquire),
        })
    }

    pub(crate) fn root_for(&self, inode: InodeId) -> Option<InodeId> {
        self.index
            .read()
            .expect("ASSERT: logical quota index lock poisoned")
            .by_inode
            .get(&inode)
            .map(|bucket| bucket.root_inode)
    }

    pub(crate) fn reserve_change(
        &self,
        inode: InodeId,
        before: u64,
        after: u64,
    ) -> Result<QuotaChange, PosixError> {
        let bucket = self
            .index
            .read()
            .expect("ASSERT: logical quota index lock poisoned")
            .by_inode
            .get(&inode)
            .cloned();
        let growth = after.saturating_sub(before);
        let shrink = before.saturating_sub(after);
        let reservation = bucket.as_ref().map_or_else(
            || Ok(QuotaReservation::empty()),
            |bucket| bucket.try_reserve(growth),
        )?;
        Ok(QuotaChange {
            bucket,
            shrink_bytes: shrink,
            reservation,
        })
    }

    pub(crate) fn associate_child(&self, parent: InodeId, child: InodeId) {
        let mut index = self
            .index
            .write()
            .expect("ASSERT: logical quota index lock poisoned");
        if let Some(bucket) = index.by_inode.get(&parent).cloned() {
            index.by_inode.insert(child, bucket);
        }
    }

    pub(crate) fn same_domain(&self, inode: InodeId, parent: InodeId) -> bool {
        let index = self
            .index
            .read()
            .expect("ASSERT: logical quota index lock poisoned");
        match (index.by_inode.get(&inode), index.by_inode.get(&parent)) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    pub(crate) fn remove_inode(&self, inode: InodeId, allocated_bytes: u64) {
        let bucket = self
            .index
            .write()
            .expect("ASSERT: logical quota index lock poisoned")
            .by_inode
            .remove(&inode);
        if let Some(bucket) = bucket {
            bucket.release(allocated_bytes);
        }
    }
}

#[derive(Debug)]
struct QuotaReservation {
    bucket: Option<Arc<QuotaBucket>>,
    reserved_bytes: u64,
    accepted: bool,
}

impl QuotaReservation {
    const fn empty() -> Self {
        Self {
            bucket: None,
            reserved_bytes: 0,
            accepted: false,
        }
    }

    fn accept(&mut self) {
        self.accepted = true;
    }
}

impl Drop for QuotaReservation {
    fn drop(&mut self) {
        if !self.accepted
            && let Some(bucket) = &self.bucket
        {
            bucket.release(self.reserved_bytes);
        }
    }
}

#[derive(Debug)]
pub(crate) struct QuotaChange {
    bucket: Option<Arc<QuotaBucket>>,
    shrink_bytes: u64,
    reservation: QuotaReservation,
}

impl QuotaChange {
    pub(crate) fn accept(mut self) {
        self.reservation.accept();
        if let Some(bucket) = &self.bucket {
            bucket.release(self.shrink_bytes);
        }
    }
}
