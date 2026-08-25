---
status: superseded by ADR-0056
---

# Prove owned publications by the exact writer image

The Container encoder computes its Container commitment once while sealing
its immutable image. Owned publication retains that exact image until the
mandatory storage reread has completed. Publication compares the reread
byte-for-byte with the retained image and then independently validates the
Header/Footer envelope, every Record CRC, codec completion, every decoded Chunk
ID, zero padding, and the Recovery Index bijection. Exact equality proves that
the stored bytes contain the commitment already calculated by the encoder, so
the writer does not recompute it during publication.

A changed reread remains a publication VERIFY failure even when the changed
image is independently well formed. A wrong prehashed Chunk identity still
fails because publication rehashes every decoded Chunk. The synchronous and
io_uring owned-publication adapters use the same proof; the io_uring verifier
moves the no-longer-needed writer image into its bounded verification job.

This proof is available only at the writer boundary while the unmodified
encoder output is retained. Recovery, rebuild, offline scrub, and any verifier
without the exact writer image continue to recompute and compare the durable
Container commitment. ADR 0060 later changed that commitment from a whole-image
hash to a structural BLAKE3 and intentionally revised the prototype byte
format. This decision otherwise narrows the publication-hash requirement in
ADRs 0008 and 0053 without changing recovery rules or durability order.
