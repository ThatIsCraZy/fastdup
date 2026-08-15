---
status: accepted
---

# Recover committed prefixes of interrupted ingests

An appliance crash during a long sequential ingest recovers the newest wholly
committed file prefix rather than hiding or discarding the incomplete file.
fastdup cannot know whether a partial application file is useful, and the
ten-second guarantee applies to every successful write rather than only to
closed files. Cleanup or format-specific salvage therefore belongs to the
application.
