//! Process-local pins for Bases of dependents that online GC cannot see.
//!
//! Online GC proofs learn stored dependents from effective Exact Index
//! Locations (ADR 0105). A Container published before a process loss but never
//! activated in Exact, or left behind by a degraded activation, is invisible to
//! them. The startup scrub reads every such Container before it opens the GC
//! gate, so it pins the Bases of their dependents for the life of the process.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, RwLock};

use fastdup_format::{ChunkId, ContainerId, ExactLocationTransition};

use crate::{
    ActivatedExactIndex, ExactIndexLookup, ExactIndexStoreError, ScrubCertificate, StorageIo,
};

/// Upper bound on distinct pinned Bases.
///
/// Only dependents absent from the scrub's Exact generation are pinned: the
/// in-flight work of one crash window or one degraded activation. A larger set
/// keeps online GC closed for the process instead of being truncated.
pub const MAX_DEPENDENT_BASE_PINS: usize = 262_144;

#[derive(Debug)]
pub enum DependentBasePinError {
    Exact(ExactIndexStoreError),
    LimitExceeded { limit: usize },
}

impl fmt::Display for DependentBasePinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(error) => write!(formatter, "dependent Base pin lookup failed: {error}"),
            Self::LimitExceeded { limit } => write!(
                formatter,
                "unindexed dependent records name more than {limit} Bases"
            ),
        }
    }
}

impl std::error::Error for DependentBasePinError {}

impl From<ExactIndexStoreError> for DependentBasePinError {
    fn from(error: ExactIndexStoreError) -> Self {
        Self::Exact(error)
    }
}

/// Collects pins from scrub certificates, fresh or resumed, before install.
#[derive(Debug, Default)]
pub struct DependentBasePinCollector {
    bases: BTreeMap<ChunkId, BTreeSet<[u8; 16]>>,
}

impl DependentBasePinCollector {
    /// Pins the Base of every dependent in `certificate` whose own Location in
    /// that Container is not effective in `index`. Without an Exact generation
    /// every dependent is unindexed.
    ///
    /// Each dependent is looked up on its own rather than letting one stand
    /// for its Container: a missed pin becomes a sticky integrity failure, so
    /// the check does not rely on Exact activating a Container all at once.
    ///
    /// # Errors
    ///
    /// Returns Exact page failures or [`DependentBasePinError::LimitExceeded`].
    pub fn observe<X: StorageIo>(
        &mut self,
        certificate: &ScrubCertificate,
        index: Option<&ActivatedExactIndex<X>>,
    ) -> Result<(), DependentBasePinError> {
        let container = certificate.container_id();
        let mut dependents = certificate.dependents().collect::<Vec<_>>();
        if dependents.is_empty() {
            return Ok(());
        }
        dependents.sort_unstable();
        dependents.dedup();
        let mut keys = dependents
            .iter()
            .map(|(chunk_id, logical_length, _)| (*chunk_id, *logical_length))
            .collect::<Vec<_>>();
        keys.dedup();
        let visible = match index {
            None => Vec::new(),
            Some(index) => keys
                .iter()
                .zip(index.lookup_transitions_batch(&keys)?)
                .filter(|(_, lookup)| stored_in(lookup, container))
                .map(|(key, _)| *key)
                .collect(),
        };
        for (chunk_id, logical_length, base) in dependents {
            if visible.binary_search(&(chunk_id, logical_length)).is_ok() {
                continue;
            }
            self.bases
                .entry(base)
                .or_default()
                .insert(container.bytes());
            if self.bases.len() > MAX_DEPENDENT_BASE_PINS {
                return Err(DependentBasePinError::LimitExceeded {
                    limit: MAX_DEPENDENT_BASE_PINS,
                });
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bases.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bases.is_empty()
    }
}

/// Candidates arrive newest Run first, so the first transition naming this
/// Container is its effective one. RETIRING and QUARANTINED records still sit
/// in their Container and stay visible to online proofs.
fn stored_in(lookup: &ExactIndexLookup, container: ContainerId) -> bool {
    lookup
        .candidates()
        .iter()
        .find(|entry| entry.location().container_id() == container)
        .is_some_and(|entry| entry.transition() != ExactLocationTransition::Removed)
}

/// Shared pin set: written once by the startup scrub before the GC gate
/// opens, read by online proofs, released as pinning Containers are removed.
#[derive(Clone, Debug, Default)]
pub struct DependentBasePins(Arc<RwLock<BTreeMap<ChunkId, BTreeSet<[u8; 16]>>>>);

impl DependentBasePins {
    /// Replaces the pin set with one completed scrub's collection. It runs
    /// once per process, before the startup scrub opens the GC gate.
    ///
    /// # Panics
    ///
    /// Panics if another thread poisoned the pin lock.
    pub fn install(&self, collector: DependentBasePinCollector) {
        *self
            .0
            .write()
            .expect("ASSERT: dependent Base pin lock poisoned") = collector.bases;
    }

    /// # Panics
    ///
    /// Panics if another thread poisoned the pin lock.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0
            .read()
            .expect("ASSERT: dependent Base pin lock poisoned")
            .len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn matching(&self, candidates: &BTreeSet<ChunkId>) -> Vec<ChunkId> {
        let pins = self
            .0
            .read()
            .expect("ASSERT: dependent Base pin lock poisoned");
        if pins.len() <= candidates.len() {
            pins.keys()
                .filter(|base| candidates.contains(base))
                .copied()
                .collect()
        } else {
            candidates
                .iter()
                .filter(|base| pins.contains_key(base))
                .copied()
                .collect()
        }
    }

    /// Drops the pins held by Containers that were unlinked and synced.
    pub(crate) fn release(&self, removed: &BTreeMap<[u8; 16], ContainerId>) {
        let mut pins = self
            .0
            .write()
            .expect("ASSERT: dependent Base pin lock poisoned");
        if pins.is_empty() {
            return;
        }
        pins.retain(|_, containers| {
            containers.retain(|container| !removed.contains_key(container));
            !containers.is_empty()
        });
    }
}
