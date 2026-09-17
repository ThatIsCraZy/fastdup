---
status: accepted
---

# Journal Metadata additions in RAM and persist them in maintenance

Proof-bearing Namespace commits classify newly published Manifest nodes and the
new Namespace Shards plus their Namespace Root descriptor as additive Metadata
liveness changes while the Commit WAL retains every prior root. The frontend
commit path records only bounded
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
durability result, Commit-WAL rotation, broken delta chain,
or 32-run chain limit requires a new exact mark and snapshot. Rotation marks
existing root-pin releases as potentially reclaiming before the WAL write, so a
reader that outlives the displaced Commit graph cannot become invisible to a
later collection. Process restart still requires one exact refresh; persistent
deltas do not reconstruct uncommitted process-local pins or become recovery
roots.

Catalog format v2 uses the former envelope-reserved fields for a run kind and
base generation. Snapshots have base zero; additions name the immediately prior
catalog generation. Readers accept only v2. Scrub audits every run and its
chain, while exact collection may replace any older snapshot/delta set from
Commit and live-pin authority.

An exact pass retires the prior in-memory addition/unclassified sets and clean
catalog tail while holding the publication barrier, before any fallible catalog
publication or object removal. It leaves an exact-required journal until the
complete namespace transition is durable. Installing a new clean state checks
both epoch and journal revision under the journal lock, so a concurrent live-pin
or Recovery Checkpoint pin release cannot be lost. A content-identified object
removed by GC can then be published again without inheriting its former
publication's journal membership. A failed exact pass also forces an exact retry;
it cannot extend a catalog tail already removed by the interrupted pass.

The scheduling regression drains a pin during catalog I/O, rotates old roots out
of the WAL, republishes collected content, and commits it through the normal
successor-proof path. Fault cases fail before and after the final directory sync;
all cases must subsequently collect, scrub, and recover the new committed graph.

Cooperative shutdown may interrupt an exact Metadata pass between graph-object
reads, candidate verification, or individual unlinks. The exact-required journal
and cleared clean-catalog tail remain in force on cancellation, just as on an
I/O failure. If any unlink may have occurred, the collector synchronizes the
Metadata directory before reporting cancellation; a sync failure remains an
I/O failure. It never installs a partial mark as a clean state. The next owner
or uncancelled pass derives a fresh exact mark from Commit and live-pin authority.
The fault regression stops inside both candidate reading and deletion, simulates
process loss, checks the committed graph with Scrub, and requires an exact retry.

## WAL-covered Recovery Checkpoint root pins are release-exempt (17 September 2026)

Recovery Checkpoint publication selects candidates only from the retained Commit
segment and pins each selected Namespace Root before graph copying. Acquisition
of such a WAL-covered root therefore protects graph bytes whose marking authority
is already the retained Commit Record; it does not dirty a clean mark or force an
exact Metadata pass. Its release is likewise release-exempt while that covering
Commit Record remains in the segment.

Before WAL rotation can evict a covering Record, the Commit writer re-arms every
live WAL-covered Recovery Checkpoint root pin. A subsequent final release, or any
release of an unarmored pin, changes protected-set authority and forces the same
complete exact mark required by any other potential root removal. Duplicate
acquisition and non-final release remain no-ops, and process start still performs
the mandatory exact Commit/Pin refresh. Cleanup of dead weak pin handles is
bookkeeping only and never changes armament or liveness.

This refines the every-root-removal sentence of ADR 0067 without weakening exact
deletion authority. Catalog reuse still never trusts the temporary Recovery
Checkpoint pin registry as a graph scan; every exact pass re-scans live roots, and
candidate verification remains byte-identical before unlink.

## Chain-length compaction preserves authoritative marks (17 September 2026)

Reaching the 32-run addition-chain limit alone no longer re-reads the durable
Namespace and Manifest graphs. If the existing catalog chain is internally valid
and the journal contains only classified additions, maintenance may acquire the
same publication barrier and Commit lock used by an exact pass, reload the clean
mark and journal, then materialize a new Snapshot run whose row set is the chain
union plus those additions, replace the old catalog names with one directory sync,
and continue from that clean tail. Compaction inherits deletion authority only from
the earlier exact Snapshot that seeded the chain; it cannot authorize an unlink in
the compacting quantum itself and grants no new reachability inference.

Corruption, a noncontiguous chain, exact-required root/WAL events, unclassified
publication, or any compaction race leaves the next pass exact-required. The new
Snapshot still binds the current Commit segment and uses the existing v2 format;
Scrub audits its chain and rows independently, and recovery never uses a catalog
as authority.
