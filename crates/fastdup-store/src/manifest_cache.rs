//! Manifest decoding adapter; all residency and eviction belong to the unified cache.
use crate::manifest_tree::{DecodedManifestNode, ManifestTreeError, decode_manifest_node};
use crate::{ReadCacheClass, ReadCacheKey, ReadCacheNamespace};
use fastdup_format::MetadataObjectId;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ManifestNodeCache {
    cache: ReadCacheNamespace,
}

impl ManifestNodeCache {
    pub(crate) fn system() -> Self {
        Self {
            cache: ReadCacheNamespace::system(ReadCacheClass::ManifestNode),
        }
    }

    pub(crate) fn read<F>(
        &self,
        id: MetadataObjectId,
        read: F,
    ) -> Result<Arc<DecodedManifestNode>, ManifestTreeError>
    where
        F: FnOnce() -> Result<Vec<u8>, ManifestTreeError>,
    {
        let key = ReadCacheKey {
            identity: id.bytes(),
            ordinal: 0,
        };
        if let Some(node) = self.cache.get(key) {
            return Ok(node);
        }
        let encoded = read()?;
        if MetadataObjectId::from_encoded(&encoded)? != id {
            return Err(ManifestTreeError::IdentityMismatch(id));
        }
        let encoded_bytes = encoded.len() as u64;
        let mut bytes = Some(encoded);
        let node = Arc::new(decode_manifest_node(id, &mut |_| {
            Ok(bytes.take().expect("ASSERT: node decoder reads one object"))
        })?);
        self.cache.insert(
            key,
            Arc::clone(&node),
            encoded_bytes.saturating_mul(2),
            encoded_bytes,
        );
        Ok(node)
    }

    #[cfg(test)]
    fn limited(target: u64) -> Self {
        Self {
            cache: ReadCacheNamespace::isolated(ReadCacheClass::ManifestNode, target),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest_tree::{
        allocated_bytes_in_manifest_tree_range_decoded, encode_manifest_tree,
        read_manifest_tree_range_decoded,
    };
    use fastdup_format::ManifestExtent;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    fn tree() -> (MetadataObjectId, u64, BTreeMap<MetadataObjectId, Vec<u8>>) {
        let extents = (0..4096)
            .map(|i| ManifestExtent::Fill {
                logical_length: 65536,
                value: u8::try_from(i % 251).unwrap(),
            })
            .collect::<Vec<_>>();
        let size = 4096 * 65536;
        let tree = encode_manifest_tree(size, &extents).unwrap();
        (tree.root(), size, tree.objects().iter().cloned().collect())
    }
    #[test]
    fn adjacent_clone_ranges_share_verified_decoded_nodes_and_preserve_validation() {
        let (root, size, objects) = tree();
        assert!(
            objects.len() > 2,
            "must exercise inner nodes and multiple leaves"
        );
        let cache = ManifestNodeCache::limited(8 * 1024 * 1024);
        let reads = AtomicUsize::new(0);
        let mut load = |id| {
            cache.read(id, || {
                reads.fetch_add(1, Ordering::Relaxed);
                Ok(objects[&id].clone())
            })
        };
        let first = read_manifest_tree_range_decoded(root, size, 65536, 4 * 1024 * 1024, &mut load)
            .unwrap();
        let cold = reads.load(Ordering::Relaxed);
        assert!(cold >= 2);
        assert_eq!(
            allocated_bytes_in_manifest_tree_range_decoded(
                root,
                size,
                65536,
                4 * 1024 * 1024,
                &mut load
            )
            .unwrap(),
            4 * 1024 * 1024
        );
        let again = read_manifest_tree_range_decoded(root, size, 65536, 4 * 1024 * 1024, &mut load)
            .unwrap();
        assert_eq!(first, again);
        assert_eq!(
            reads.load(Ordering::Relaxed),
            cold,
            "warm clone must perform zero metadata reads"
        );
        assert!(
            read_manifest_tree_range_decoded(root, size + 1, 0, 1, &mut load).is_err(),
            "a cache hit still checks the caller's expected tree length"
        );
        let held = load(root).unwrap();
        cache.cache.set_capacity(0);
        assert_eq!(cache.cache.stats().resident_bytes, 0);
        assert!(matches!(held.as_ref(), DecodedManifestNode::Inner(_)));
        let mut corrupt = objects[&root].clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(
            cache.read(root, || Ok(corrupt)).is_err(),
            "evicted nodes must be independently verified again"
        );
        let isolated = ManifestNodeCache::limited(8 * 1024 * 1024);
        assert!(
            isolated
                .read(root, || Err(ManifestTreeError::InvalidTree))
                .is_err()
        );
    }
    #[test]
    fn concurrent_misses_charge_one_entry_and_pressure_keeps_owned_views_valid() {
        let (root, _, objects) = tree();
        let cache = ManifestNodeCache::limited(8 * 1024 * 1024);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..50 {
                        cache.read(root, || Ok(objects[&root].clone())).unwrap();
                    }
                });
            }
        });
        assert_eq!(cache.cache.stats().entries, 1);
        assert!(cache.cache.stats().resident_bytes > 0);
    }
    #[test]
    #[ignore = "A/B timing benchmark; run explicitly in release mode with --nocapture"]
    fn benchmark_manifest_clone_ranges() {
        let (root, size, objects) = tree();
        let mut results = Vec::new();
        for budget in [0, 8 * 1024 * 1024] {
            let cache = ManifestNodeCache::limited(budget);
            let reads = AtomicUsize::new(0);
            let start = Instant::now();
            for i in 0..2000 {
                let offset = (i % 48) * 4 * 1024 * 1024;
                let result =
                    read_manifest_tree_range_decoded(root, size, offset, 4 * 1024 * 1024, |id| {
                        cache.read(id, || {
                            reads.fetch_add(1, Ordering::Relaxed);
                            Ok(objects[&id].clone())
                        })
                    })
                    .unwrap();
                std::hint::black_box(result);
            }
            results.push((start.elapsed(), reads.load(Ordering::Relaxed)));
        }
        eprintln!(
            "manifest_range_ab uncached={:?} cached={:?}",
            results[0], results[1]
        );
        assert!(results[1].1 < results[0].1 / 100);
    }
}
