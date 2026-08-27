---
status: accepted
---

# Resolve Bases through bounded Recovery Index reads

Index-free Prefix decoding owns one pass-local Base resolver. It enumerates the
published namespace at most once, reads only each Container's paired envelope
and compact Recovery Index, and retains every successfully verified Base for
the remainder of that dependent Container decode. This replaces the former
resolver that listed the namespace and fully read Provider Containers once per
Prefix record.

The existing Container format version 2 already contains the required durable
evidence. Header and Footer pair Container identity, generation, layout, and
intrinsic summary. The complete Recovery Index has a CRC and canonical entry
geometry. A selected independent Encoding Record must additionally match every
stored coordinate and CRC and decode to the requested Chunk ID and logical
length before bytes escape. Complete Container verification and scrub continue
to establish the Index-to-Record bijection and structural commitment.

No Header or Footer field and no new Container format version are introduced.
The appliance has no released v2 data requiring migration, but keeping the
only writer and reader on v2 avoids manufacturing an obsolete v2-to-v3
transition, downgrade fence, dual reader, or migration utility.

The immutable dependent Container stores no physical Base Container ID or
offset because GC may relocate an equivalent independent Location without
changing the logical dependency. The Exact Index remains rebuildable location
acceleration rather than durable content authority.

## Consequences

The recovery cost changes from repeated complete Provider-Container scans to
one namespace enumeration, paired envelope reads, compact Recovery Index reads,
and one bounded selected-record read. Index ranges larger than the storage
adapter's one-megabyte limit are assembled from bounded chunks.

The ingest hot loop performs no additional hash, filter probe, payload scan,
I/O, allocation, or lock acquisition. A tested Header/Footer digest and 3-KiB
membership filter remain excluded. The initial SingleStream result against
those fields was invalidated after a shared-cgroup Swap charge was found to
close every rebuildable cache. Repeating the evolving-family GC experiment
with the corrected governor and this bounded v2 resolver reduced the
Prefix/Off GC ratio from 3.424x to 1.306x. An isolated provider probe found that
the filter could avoid at most 1.47% of one complete verification pass's bytes.
That acceleration does not justify an incompatible durable format; see
`docs/benchmarks/container-format-v3-gc-reevaluation-2026-08-27.md`.

Tests cover envelope/index/record verification, corrupt Index CRCs, one
namespace enumeration per dependent Container, and repeated-Base caching.
Failures close the selected path without returning unchecked bytes or weakening
GC proof requirements.
