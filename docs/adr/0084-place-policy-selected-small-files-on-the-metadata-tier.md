---
status: accepted
---

# Place policy-selected Small Files on the Metadata Tier

Policy-known Small Files begin in their protected Metadata-Tier quota and spill
new records to the Data Tier above an initial 8 MiB hysteresis threshold;
unknown families begin on Data unless an allowed hint says otherwise. Existing
immutable records do not move synchronously. Small-File Locations are ordinary
durable coverage, while Cache Locations remain removable extras and may never
be the sole coverage of a live Logical Chunk.
