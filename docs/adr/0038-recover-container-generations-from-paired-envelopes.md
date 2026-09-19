---
status: superseded by ADR 0072
---

# Recover Container generations from paired envelopes

ADR 0072 supersedes the proposed writable-start migration: a nonempty DATA Pool
without allocator state fails closed. Envelope scanning remains an offline
diagnostic that reads each canonical Container's length and paired Header/Footer
to discover the greatest durable generation without reading payloads.

The scan is structural evidence only. It cannot initialize missing writable
allocator state, authorize a Location, rebuild Exact, or prove payload content.
A malformed name, checksum, identity, layout, generation, or length fails the
diagnostic rather than being skipped.
