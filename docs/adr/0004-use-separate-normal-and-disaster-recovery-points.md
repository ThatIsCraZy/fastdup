---
status: accepted
---

# Use separate normal and disaster recovery points

Normal process crashes and power loss retain every committed mutation outside
the ten-second durability window, while complete loss of the NVMe metadata tier may
fall back to a data-tier recovery checkpoint up to 90 seconds old. The latter is
a disaster-recovery path, not an online failover path; separating its RPO avoids
forcing high-frequency HDD metadata checkpoints into the ingest hot path.

## Consequences

A metadata-tier rebuild restores the last wholly valid checkpoint and may expose
later, independently verifiable files only under `lost+found`. It never merges
those files into the canonical recovered namespace automatically.
