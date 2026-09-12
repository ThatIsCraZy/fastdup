//! Preserve existing RAW owners and recycle codec output after its final view.
use crate::PooledBuffer;
use std::ops::Deref;
use std::sync::{Arc, Weak};

#[derive(Clone, Debug)]
pub(super) enum PayloadBacking {
    Owned(Arc<Vec<u8>>),
    Pooled(Arc<PooledBuffer>),
}
pub(super) enum WeakBacking {
    Owned(Weak<Vec<u8>>),
    Pooled(Weak<PooledBuffer>),
}
impl PayloadBacking {
    pub(super) fn id(&self) -> usize {
        match self {
            Self::Owned(value) => Arc::as_ptr(value).addr(),
            Self::Pooled(value) => Arc::as_ptr(value).addr(),
        }
    }
    pub(super) fn shares(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Owned(a), Self::Owned(b)) => Arc::ptr_eq(a, b),
            (Self::Pooled(a), Self::Pooled(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
    pub(super) fn capacity(&self) -> usize {
        match self {
            Self::Owned(value) => value.capacity(),
            Self::Pooled(value) => value.capacity(),
        }
    }
    pub(super) fn downgrade(&self) -> WeakBacking {
        match self {
            Self::Owned(value) => WeakBacking::Owned(Arc::downgrade(value)),
            Self::Pooled(value) => WeakBacking::Pooled(Arc::downgrade(value)),
        }
    }
    pub(super) fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Owned(value) => {
                Arc::try_unwrap(value).unwrap_or_else(|value| value.as_slice().to_vec())
            }
            Self::Pooled(value) => {
                Arc::try_unwrap(value).map_or_else(|value| value.to_vec(), PooledBuffer::into_vec)
            }
        }
    }
}
impl Deref for PayloadBacking {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Owned(value) => value,
            Self::Pooled(value) => value,
        }
    }
}
impl WeakBacking {
    pub(super) fn upgrade(&self) -> Option<PayloadBacking> {
        match self {
            Self::Owned(value) => value.upgrade().map(PayloadBacking::Owned),
            Self::Pooled(value) => value.upgrade().map(PayloadBacking::Pooled),
        }
    }
}
