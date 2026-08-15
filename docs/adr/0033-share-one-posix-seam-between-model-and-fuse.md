---
status: accepted
---

# Share one POSIX semantic seam between the model and FUSE

The deterministic model and the low-level FUSE adapter both call one concrete,
byte-oriented `Namespace::dispatch` seam. The namespace owns inode, FUSE lookup,
and handle liveness, mutation ordering, the live view, open-orphan semantics,
sparse logical layout, and expected POSIX errors; the adapter only converts FUSE
request bytes, flags, attributes, and errno values. This prevents the test
harness and kernel adapter from growing two subtly different POSIX
implementations.

## Consequences

The seam is synchronous and independent of Tokio, FUSE, libc flags, manifests,
and physical locations. Internally it uses a short catalog lock and per-inode
locks, so independent file I/O does not share a content lock. Impossible
request/reply pairings and broken catalog relationships are production
assertions; invalid names, handles, offsets, and resource exhaustion are normal
errors. For `readdirplus`, the adapter explicitly owns the gap between a
namespace snapshot and the bounded kernel reply: accepted entries retain their
FUSE lookup pins, while a pending or unpolled entry is released when the stream
is dropped.

The first mounted checkpoint is explicitly volatile. It disables FUSE
writeback, sets `DIRECT_IO` and zero attribute/entry TTLs, and implements only
the operations listed in the POSIX checkpoint report. This safe cache policy is
not the final performance policy, and the checkpoint does not satisfy the
ten-second durability contract until immutable manifests, Namespace Roots, and
the Commit WAL sit behind the same seam.

## Considered options

Separate typed methods would catch request/reply mismatches at compile time but
make the public surface grow with every POSIX opcode. An actor/callback port
would make backpressure explicit but add an asynchronous boundary to local
lookups and deterministic tests. The command seam keeps one small boundary;
private typed handlers retain operation locality.
