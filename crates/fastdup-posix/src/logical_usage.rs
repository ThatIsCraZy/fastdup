//! Bounded, nonblocking observation of cached inode allocation counters.
use super::{FileKind, InodeId, Namespace};
use std::ops::Bound::{Excluded, Included, Unbounded};

#[derive(Debug, Default)]
pub(super) struct LogicalUsageSampler {
    cursor: Option<InodeId>,
    end: Option<InodeId>,
    total: u64,
    latest: Option<(u64, u64)>,
}

impl Namespace {
    /// Samples at most 4096 in-memory inode counters without waiting for writers
    /// or reading DATA. Returns bytes and completion time of the last whole pass.
    /// The observation is approximate across concurrent mutations; hard links
    /// count once, sparse holes never count, and pinned unlinked files still count.
    #[must_use]
    pub fn sample_logical_usage(&self) -> Option<(u64, u64)> {
        let mut sample = self.logical_usage.try_lock().ok()?;
        let Ok(catalog) = self.catalog.try_read() else {
            return sample.latest;
        };
        if sample.cursor.is_none() {
            sample.end = catalog.inodes.last_key_value().map(|(&id, _)| id);
        }
        let upper = sample.end.map_or(Unbounded, Included);
        let lower = sample.cursor.map_or(Unbounded, Excluded);
        let batch: Vec<_> = catalog
            .inodes
            .range((lower, upper))
            .take(4096)
            .map(|(&id, inode)| (id, std::sync::Arc::clone(inode)))
            .collect();
        drop(catalog);
        for (id, inode) in &batch {
            let Ok(state) = inode.state.try_read() else {
                return sample.latest;
            };
            if state.kind == FileKind::Regular {
                sample.total = sample.total.checked_add(state.data.allocated_bytes())?;
            }
            sample.cursor = Some(*id);
        }
        if batch.len() < 4096 {
            let observed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_secs();
            sample.latest = Some((sample.total, observed));
            sample.total = 0;
            sample.cursor = None;
        }
        sample.latest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn an_empty_namespace_reports_zero_and_busy_catalog_does_not_block() {
        let namespace = Namespace::new_volatile(super::super::NamespaceConfig::default());
        assert_eq!(namespace.sample_logical_usage().unwrap().0, 0);
        let _catalog = namespace.catalog.write().unwrap();
        assert_eq!(namespace.sample_logical_usage().unwrap().0, 0);
    }
}
