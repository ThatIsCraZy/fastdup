---
status: accepted
---

# Version formats before stabilizing them

Every durable structure is explicitly versioned and strictly validated from the
first prototype, but compatibility is not promised until an explicit
`format-v1-stable` milestone. Before that marker, test data may require a
documented offline rebuild; after it, unknown versions are rejected and format
changes require a crash-safe offline migration or generation transition. This
allows early correction of format mistakes without normalizing silent or unsafe
in-place upgrades.
