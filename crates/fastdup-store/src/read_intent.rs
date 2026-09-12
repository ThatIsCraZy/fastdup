//! Explicit synchronous read policy, captured before dispatching backend work.
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;

/// Reuse policy never changes the validation a caller owes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadIntent {
    Demand,
    /// May reuse immutable bytes but cannot displace useful resident entries.
    Scan,
    /// Read current durable bytes, independently of every application cache.
    Independent,
}

thread_local! {
    static INTENT: Cell<ReadIntent> = const { Cell::new(ReadIntent::Demand) };
}

/// Nestable synchronous scope. It is not Send and must never be held across
/// an async await, including on a single-thread executor.
pub struct ReadIntentScope(ReadIntent, PhantomData<Rc<()>>);

impl ReadIntentScope {
    /// Captures policy for work dispatched to another synchronous worker.
    #[must_use]
    pub fn current() -> ReadIntent {
        INTENT.with(Cell::get)
    }

    #[must_use]
    pub fn enter(intent: ReadIntent) -> Self {
        let previous = INTENT.with(|current| {
            let previous = current.get();
            if previous != ReadIntent::Independent {
                current.set(intent);
            }
            previous
        });
        Self(previous, PhantomData)
    }
}

impl Drop for ReadIntentScope {
    fn drop(&mut self) {
        INTENT.with(|current| current.set(self.0));
    }
}

pub(crate) fn independent() -> bool {
    INTENT.with(|current| current.get() == ReadIntent::Independent)
}
pub(crate) fn bypass_admission() -> bool {
    INTENT.with(|current| current.get() != ReadIntent::Demand)
}
