use super::*;
use fastdup_format::{ManifestExtent, ManifestLeaf};

fn object(value: u8) -> (MetadataObjectId, Vec<u8>) {
    let bytes = ManifestLeaf::new(
        4096,
        vec![ManifestExtent::Fill {
            logical_length: 4096,
            value,
        }],
    )
    .unwrap()
    .encode()
    .unwrap();
    (MetadataObjectId::from_encoded(&bytes).unwrap(), bytes)
}

#[test]
fn shrink_releases_cache_ownership_but_keeps_reader_views_valid() {
    let (id, bytes) = object(1);
    let cache = MetadataObjectCache::limited(1 << 20);
    let held = cache.read(id, || Ok(bytes.clone())).unwrap();
    assert!(cache.admission.lock().unwrap().resident >= bytes.len() as u64);
    {
        let mut admission = cache.admission.lock().unwrap();
        admission.target = 0;
        cache.trim(&mut admission, 0);
        assert_eq!(admission.resident, 0);
    }
    assert_eq!(*held, bytes);
    let reads = std::cell::Cell::new(0);
    for _ in 0..2 {
        assert_eq!(
            *cache
                .read(id, || {
                    reads.set(reads.get() + 1);
                    Ok(bytes.clone())
                })
                .unwrap(),
            bytes
        );
    }
    assert_eq!(reads.get(), 2);
    assert_eq!(cache.admission.lock().unwrap().resident, 0);
}

#[test]
fn failed_miss_is_not_admitted_and_independent_scopes_restore_after_errors() {
    let (id, bytes) = object(1);
    let (_, other) = object(2);
    let cache = MetadataObjectCache::limited(1 << 20);
    assert!(cache.read(id, || Ok(other.clone())).is_err());
    assert_eq!(cache.admission.lock().unwrap().resident, 0);
    cache.read(id, || Ok(bytes.clone())).unwrap();
    {
        let _outer = IndependentRead::enter();
        {
            let _inner = IndependentRead::enter();
            assert!(cache.read(id, || Ok(other.clone())).is_err());
        }
        assert!(
            cache
                .read(id, || Err(
                    std::io::Error::other("injected read failure").into()
                ))
                .is_err()
        );
    }
    assert_eq!(
        *cache
            .read(id, || panic!("warm read reached backend"))
            .unwrap(),
        bytes
    );
    cache.invalidate(id);
    assert_eq!(cache.admission.lock().unwrap().resident, 0);
    assert!(
        cache
            .read(id, || Err(
                std::io::Error::other("missing after unlink").into()
            ))
            .is_err()
    );
}

#[test]
fn concurrent_admission_charges_one_identity_and_survives_invalidation() {
    let cache = Arc::new(MetadataObjectCache::limited(1 << 20));
    let (id, bytes) = object(3);
    let barrier = Arc::new(std::sync::Barrier::new(8));
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            let bytes = &bytes;
            handles.push(scope.spawn(move || {
                cache
                    .read(id, || {
                        barrier.wait();
                        Ok(bytes.clone())
                    })
                    .unwrap()
            }));
        }
        for handle in handles {
            assert_eq!(*handle.join().unwrap(), bytes);
        }
    });
    assert_eq!(
        cache.admission.lock().unwrap().resident,
        bytes.len() as u64 + ENTRY_OVERHEAD
    );
    cache.invalidate(id);
    assert_eq!(cache.admission.lock().unwrap().resident, 0);
}
