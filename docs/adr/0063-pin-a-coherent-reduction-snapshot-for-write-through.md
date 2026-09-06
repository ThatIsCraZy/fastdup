---
status: accepted
---

# Pin a coherent reduction snapshot for write-through

Current-state note (2026-09-05): ADR 0089 replaces the mount-lifetime Exact/Similarity
pair described below in the online writer. Each planning batch retains an
immutable Similarity view and then pins current Exact for candidate resolution;
the Reduction Head records Exact provenance, not a historical Exact lease.
A dependent-publication guard protects Bases through target Exact activation.
ADR 0088 adds Sparse-XOR beside Prefix within the same four-trial budget.
Fragmented targets are materialized once and can use both dependent codecs.
The original frozen-pair seam remains available for offline/experimental use;
missing acceleration still falls back to independent encoding. ADR 0089 is
implemented but remains proposed pending its stated performance gates.

Advanced write-through pins one immutable Similarity family together with the
exact Exact Run Set named by that family. Later Exact L0 activations do not
replace this pair during the mount: the candidate universe remains the
Similarity snapshot, while its pinned Exact snapshot continues to resolve
every candidate coherently. A missing or mismatched pair disables Prefix
selection without weakening Exact reuse or write availability.

For one prehashed contiguous target, the hot path fingerprints without
repeating BLAKE3, prepares the best independent RAW/Zstd fallback once, reads
and verifies at most four independent Bases, and retains the smallest Prefix
frame. Prefix is selected only when it beats the prepared independent payload
by at least 5 percent and 4 KiB. The selected frame moves directly into one
mixed Container alongside ordinary adaptive regions; neither the target nor
the accepted Prefix is compressed twice. Fragmented Chunks keep the existing
single-materialization independent path until segmented fingerprinting shares
the scalar v1 oracle.

Dependent records remain valid Exact Index targets but are excluded from every
Similarity rebuild, so a codec-3 target can never become a Base and dependency
depth stays one. Writer publication trusts prehashed identities and prepared
frames under ADR 0059; ordinary reads, recovery, rebuild, and scrub still
resolve the independent Base, decode the Prefix, and recompute the target
identity.
