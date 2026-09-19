---
status: accepted
---

# Lease immutable Similarity Runs across generation reads

ADR 0046 replaced file-backed mappings with bounded Direct-I/O readers, but the
lease decision remains: every selected immutable Similarity Run is held by a
generation-owned file lease. Cooperating write, truncate, replace, and remove
operations fail while any lease exists; reclamation retries after the final
reader drops. Adapters on the same canonical root share this lifetime state.

Before a Run becomes queryable, activation or recovery verifies its expected
length, envelope identity, page checksums, ordering, Bucket/Entry relationships,
and complete Run hash. Online publication may carry that audited owner through
family activation; restart and offline scrub audit independently. A partial or
mixed source family fails activation.

Optional page fences and decoded pages live in the Unified Read Cache. A bounded
query may retain its current page per cursor as transient working memory; these
views neither extend file lifetime beyond the generation lease nor establish
content, liveness, or Location authority.
