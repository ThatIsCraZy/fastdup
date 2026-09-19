---
status: accepted
---

# Prove one Namespace Root once per commit

`NamespaceRoot` construction is the sole whole-graph proof. Its fields are
private and it has no mutable interface, so encoding is a pure projection and
must not revalidate an already proven value.

The constructor proves sorted unique records, valid names and parents, exact
regular-file/symlink link counts, directory link count `2 + child directories`,
one incoming directory link, and reachability from the root. It uses ordered
slices, binary search, and bounded tally/reachability vectors rather than
per-entry maps or cloned names.

This removed one redundant whole-Namespace walk without changing durable bytes
or accepted invariants. The 120,000-entry benchmark reduced construction from
40.5 ms to 17.6 ms and canonical encoding from 42.2 ms to 9.4 ms. ADR 0095 owns
the remaining O(Namespace) commit cost and incremental-mirror work.
