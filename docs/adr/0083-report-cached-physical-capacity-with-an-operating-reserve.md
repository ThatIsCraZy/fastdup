---
status: accepted
---

# Report cached physical capacity with an operating reserve

FUSE `statfs` reports physical Data-Tier capacity without multiplying it by a
reduction ratio and removes a ten-percent operating reserve from total and
available blocks. Crossing the Metadata reserve reports zero available blocks
even when Data has space. A dedicated control-path sampler refreshes both
backing-filesystem observations every five seconds, so `statfs` never performs
filesystem I/O or enters the write worker pool.

An explicit, fully validated reporting override may replace total and available
bytes for qualification or administration. It changes reporting only; physical
mutation admission and `ENOSPC` remain authoritative.

The reporting primitive remains separate from admission. ADR 0087 couples it
to each managed Share's Logical Share quota: total bytes equal that quota, while
free and available bytes equal quota minus POSIX-allocated logical bytes in
that Share, saturated at zero. The physical snapshot does not clamp a Share
with a logical quota. Shares without a quota retain physical reporting. The physical reserve and physical mutation
admission in this record remain authoritative and independent.
