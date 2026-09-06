---
status: accepted
---

# Persist Sparse-XOR as a depth-one dependent codec

Sparse-XOR is durable codec 4 and competes with codec-3 ZSTD_PREFIX behind one
dependent-encoding interface. Both name exactly one independently decodable,
same-length Base Chunk; a shared versioned cost policy retains at most four
total trials per target and accepts a dependent record only when it beats the
best independent encoding by at least 5 percent and 4 KiB.

The codec-4 payload is canonical: an ordered table of nonempty, nonoverlapping
changed-byte runs followed by their contiguous nonzero XOR bytes. Writer,
ordinary reader, recovery, rebuild, and offline scrub validate the same run
geometry, Base identity and length, reconstructed target length, and BLAKE3
target identity. GC treats codecs 3 and 4 identically for dependency closure;
neither dependent target may become a Base, so dependency depth remains one.

## Consequences

The Container envelope and intrinsic summary advance together to version 3
instead of preserving compatibility with pre-production images. Record-local
formats retain their own version fields; envelope version 3 does not mean
every nested structure is version 3. The structural-commitment algorithm
remains identifier 2 under ADR 0060. Writers and readers accept only the
current envelope, so the earlier Container-v2 wording in ADR 0075 is historical. Sparse-XOR is selected for
sparse in-place changes while ZSTD_PREFIX remains available for shifts or dense
changes. Unknown codecs, malformed runs, missing or incorrect Bases, truncated
payloads, and target-identity mismatches fail closed before bytes reach a
caller.
