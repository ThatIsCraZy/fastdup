---
status: superseded by ADR 0046
---

# Use S3-FIFO for the historical proof cache

This record selected a private S3-FIFO for historical dependency proofs. ADR
0046 replaced all private read caches with one unified owner and common weighted
replacement. Active and Frozen generation proofs remain pinned required state;
historical proof admission remains optional acceleration. The S3-FIFO code is
retained only for replay comparison, not production policy.
