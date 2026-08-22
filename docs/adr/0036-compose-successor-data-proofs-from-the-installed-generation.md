---
status: accepted
---

# Compose successor DATA proofs from the installed generation

Normal in-process commits form a Successor Graph Proof: unchanged Manifest
extents retain the complete DATA proof of the immediately preceding verified
and installed generation, while every newly introduced Chunk dependency is
fully verified through the ordinary nonauthoritative Exact-Index/Container
path. This removes graph verification proportional to a growing file without
making the Exact Index authoritative or retaining a complete Chunk map in RAM.

## Consequences

Proof reuse is valid only across the single serialized `DurableNamespace`
successor transition. Common Manifest prefixes and suffixes identify preserved
dependencies; the changed middle is reread from the published Manifest and
verified completely before the Commit WAL append. A failed commit retains the
same installed predecessor proof for retry. The rule is part of the versioned
writer Policy Set.

Every online proof carries a `SuccessorPredecessor` naming the exact complete
Commit Record from which it was derived. All per-inode proofs in one Namespace
successor must carry that same record. Under the generation commit lock, the
repository loads the current clean Commit-Log head and compares the complete
record before dependency verification, Manifest-reader construction, Namespace
Root publication, or WAL append. A stale or mixed predecessor fails closed and
does not silently fall back to a complete scan. This is a process-local
generation fence, not the still-deferred cross-process Appliance Lease.

Process restart, recovery, offline scrub, a missing installed predecessor, or
an unusable Exact candidate performs a fresh complete proof or fails closed.
Immutable Container corruption discovered later still fails demand reads and
is handled by scrub/quarantine; repeatedly rereading every historical Chunk at
five-second commit cadence is not a substitute for scrub.

The online writer may also retain a bounded process-local proof for a Chunk
Location that it has already obtained from a successful Exact candidate
verification or the mandatory reread of a newly published Container. The
proof is an opaque appliance capability, not a serialized flag and not an
Exact-Index assertion. A later write in the same process may reuse it during
externalization, and an online successor commit may consume it for a matching
Chunk ID and logical length. Eviction only causes another full
verification. Restart, recovery, scrub, and index rebuild never consume this
cache.

V1 retains at most 65,536 such capabilities, enough for two simultaneous
512-MiB generations at FastCDC-v1's 16-KiB minimum. The cache uses bounded LRU
replacement and remains separate from payload and Exact-page caches. A
successful online proof does not suppress demand-read verification or later
scrub: immutable corruption discovered after the proof still fails the reader
and enters the normal corruption path.

## Implementation boundary

The installed online state is an opaque Manifest Root, logical length, and
verified allocated-byte scalar; it is not a flattened file recipe. Equal-size
dirty updates read only intersecting tree paths, expand boundaries across whole
DATA extents, publish replacement leaves child-first, and retain every remote
subtree ID exactly.

Sequential length-increasing updates now use an append-native persistent tree
operation. The store verifies the installed predecessor Root while descending
only its right spine, encodes the new suffix as local-coordinate leaves, and
rewrites that spine child-first. Remote subtree IDs remain unchanged even
though the file grew. The commit consumes an opaque `ManifestSuccessorProof`
containing a store-constructed tree summary and the independently derived set
of newly introduced DATA dependencies. Neither the summary nor the proof has a
public constructor, so a caller cannot turn a raw Root ID or asserted scalar
into proof reuse. Equal-length replacement calls verify the touched predecessor
paths, derive removed and replacement allocation totals, publish rewritten
paths, and extend the same opaque proof with replacement DATA identities.

Every append begins a new Manifest leaf sequence at the preceding committed
EOF. This deliberately avoids rewriting the predecessor's last leaf and makes
that commit boundary a stable structural seam; partially filled leaves at
successive checkpoint boundaries are accepted. Inner nodes retain the normal
1,024-child maximum and a root is raised only when right-spine overflow
requires it. The commit verifies only newly introduced Chunk dependencies;
new metadata objects are reread by the content-addressed publisher before its
directory sync, and the Commit WAL remains the sole visibility point.

The writer's structural pairing is the append/replacement descent plus verified
publication of every new node and the opaque predecessor capability. Process
restart, recovery, and offline scrub continue to traverse the complete
immutable tree.

Length-decreasing updates use authenticated subtree allocation summaries from
Manifest Inner Node v2. Truncate drops right-hand child capabilities and
rewrites only the cutoff path. A cutoff inside DATA first re-encodes the exact
retained Chunk prefix and turns the discarded suffix into HOLE before the
structural cut; HOLE and FILL split directly. Arbitrary length-changing middle
splice/concat uses the same persistent-tree capability: one predecessor range
`[start, end)` is replaced by a canonical extent sequence of any length. An
empty range is insertion/concat and an empty replacement is deletion.

The store splits only the two touched paths, raises newly encoded replacement
leaves to the surrounding child level, concatenates the resulting prefix,
replacement, and suffix forests, and rewrites their ancestors child-first.
Complete remote prefix and suffix subtree IDs remain exact even when a length
change moves the suffix to a new absolute file offset, because all durable
child coordinates are node-local. The result length is checked as
`old_length - removed_length + replacement_length`; allocation is derived from
the installed predecessor scalar, authenticated summaries for the removed
range, and the replacement extents. Only replacement DATA identities extend
the Successor Graph Proof.

A splice boundary may split HOLE or FILL but never DATA. A caller that needs a
cut inside DATA must first reconstruct and reduce the complete retained byte
fragments into new independently verified DATA extents. The Metadata objects
are durable before the Namespace Root and Commit WAL; recovery and scrub still
verify the complete selected tree rather than trusting the online shortcut.
