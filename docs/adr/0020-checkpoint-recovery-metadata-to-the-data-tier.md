---
status: accepted
---

# Checkpoint recovery metadata to the data tier

At least once per 90 seconds, fastdup publishes a self-contained immutable
Recovery Checkpoint to the redundantly protected Data Tier. It names a generation
and Namespace Root and makes the complete transitive metadata needed by that
root discoverable outside NVMe. This deliberately slower disaster-recovery RPO
keeps HDD checkpoint work out of the normal five-second commit hot path.

## Consequences

After complete metadata-tier loss, rebuild scans container Recovery Indexes and
selects the highest wholly valid checkpoint. Later independently verifiable
objects may be offered only through `lost+found`. Missing, torn, or transitively
incomplete checkpoints are ignored rather than partially merged.
