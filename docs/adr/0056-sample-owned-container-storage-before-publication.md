---
status: superseded by ADR-0059
---

# Sample owned Container storage before publication

Owned ingest publication fully validates the retained writer image, but reads
back only the 4 KiB Header, one 4 KiB block aligned at the physical midpoint,
and the 4 KiB Footer before file sync, no-replace rename, and root sync. Each
sample must equal the writer image and the stored file length must match. This
replaces the complete storage reread required by ADRs 0053 and 0055 because the
prototype prioritizes SingleStream ingest throughput.

The retained writer image remains the authority for publication Locations and
still undergoes Record, Chunk-ID, Recovery-Index, padding, and envelope
validation. Publication trusts the whole-Container hash produced by the
encoder instead of recomputing it. The storage samples detect wrong length,
gross misdirection, and corruption in the sampled blocks only. Corruption
elsewhere may therefore become committed and is detected later by ordinary
reads, recovery, rebuild, or scrub. This is an explicit prototype trade-off,
not a complete proof that every stored payload byte matches the writer image.
