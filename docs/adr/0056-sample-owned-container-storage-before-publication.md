---
status: superseded by ADR 0059
---

# Sample owned Container storage before publication

This record replaced a complete storage reread with Header, midpoint, and Footer
sampling while retaining full validation of the writer image. ADR 0059 now
defines the current rule: ingest may trust exact writer evidence until an
independent read, while recovery and scrub verify stored bytes independently.
ADR 0060 defines the current structural commitment.
