---
status: accepted
---

# Journal Metadata additions in RAM and persist them in maintenance

Proof-bearing Namespace commits classify newly published Manifest nodes and the
new Namespace Root as additive Metadata liveness changes while the Commit WAL
retains every prior root. The frontend commit path records only bounded
process-local identities and state transitions; it never writes, maps, or syncs
a Metadata Mark Catalog. The maintenance worker later publishes a v2 immutable
addition run chained to the prior catalog generation. Addition runs may suppress
a complete mark, but cannot authorize unlink.

Complete, append, equal-length replacement, truncate, and splice successor
proofs all carry the identities of nodes they newly published. When a path edit
replaces the Manifest root still named by its durable predecessor, releasing
that temporary pin is covered by the retained Commit graph and remains
additive. Replacing an unpublished intermediate root still forces an exact mark.

Delta publication is serialized only against other Metadata-GC runs. It does
not hold the Metadata publication barrier or Commit lock while writing or
syncing the run, so maintenance-file latency cannot stall a frontend checkpoint.
Concurrent publication, pin drain, or WAL rotation merely leaves a newer journal
revision that forces another delta or an exact pass.

Any unclassified publication, unpublished-root-pin drain, uncertain WAL
durability result, legacy commit path, Commit-WAL rotation, broken delta chain,
or 32-run chain limit requires a new exact mark and snapshot. Rotation marks
existing root-pin releases as potentially reclaiming before the WAL write, so a
reader that outlives the displaced Commit graph cannot become invisible to a
later collection. Process restart still requires one exact refresh; persistent
deltas do not reconstruct uncommitted process-local pins or become recovery
roots.

Catalog format v2 uses the former envelope-reserved fields for a run kind and
base generation. Snapshots have base zero; additions name the immediately prior
catalog generation. Readers continue to accept v1 snapshots. Scrub audits every
run and its chain, while exact collection may replace any older snapshot/delta
set from Commit and live-pin authority.
