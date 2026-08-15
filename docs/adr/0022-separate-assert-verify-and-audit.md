---
status: accepted
---

# Separate ASSERT, VERIFY, and AUDIT

`ASSERT` marks an impossible internal state and remains active in production;
`VERIFY` handles persistent or I/O integrity failure by failing closed and
quarantining the smallest provably isolated object; `AUDIT` adds expensive
redundant checks that are sampled in production and exhaustive in tests and
offline scrub. Expected resource, permission, and device errors never become
assertions.

## Consequences

Every durable invariant is paired at writer, reader/recovery, and offline-scrub
boundaries. A bad location or isolated inode returns `EIO` without returning
unchecked bytes. An untrusted Namespace Root falls back to a previous wholly
valid generation or prevents read-write mount. Repair always creates and
verifies a new immutable Location before an atomic switch; it never overwrites
the damaged evidence.
