# Preserve inode order when ingest admission changes mode

The Veeam retry on RPM 0.6.4-5 aborted at 2026-09-06 20:51:58 CEST in
`IngestQueue::finish`: `completed inode sequence cannot move backwards`.
This is a different failure from the preceding proof-cache admission limit.
The process was PID 28321, and its last logged completed generation was 1014.

A single-stream admission can retain a partial open batch. The unbatched
multi-stream path previously queued a newer fragment without first sealing that
batch. Later expiry, shutdown, or a fence appended the older batch behind the
newer fragment. Completion then hit the sequence assertion. Writable-handle
changes can occur while admission releases the queue lock for backpressure:
a writer that already selected batching can resume with a short fragment and
leave an open batch even though subsequent writers select unbatched admission.

Two deterministic tests reproduce the exact fatal assertion. One switches the
queue's active-inode admission mode with a partial batch. The other fills the
32-MiB queue, pauses one short write at its admission wait, opens another writable
inode, frees queue space, and admits a newer fragment after the waiting write
has returned. A queue-local, test-only channel observes the wait; modeled batch
age prevents expiry from hiding the ordering bug. Both tests failed before the
fix and pass afterward, including completion fences and byte-accounting checks.
The VM log identifies the violated invariant; it does not contain a complete
trace of that job's individual queue admissions.

The production change seals an existing partial batch for the same inode before
unbatched admission, including before its backpressure wait. This moves existing
payload views into the existing FIFO. It adds no payload copy, storage operation,
worker serialization, or additional production mutex. Existing per-inode sequence
assertions and the global byte budgets remain in force. There is no durable
format change. The library, durable fault, recovery, and write-through suites
passed 96 tests with three intentional ignored tests, including the manual
benchmark. Appliance library/test Clippy passed with warnings denied.

## Queue performance A/B

Optimized test executables were built before and after the production change.
Both contain the same ignored `ingest_queue_admission_benchmark`. Each round
completes 200,000 queue jobs with shared one-MiB payload views; single-stream
mode batches four fragments per job, while multi-stream mode uses two writable
inodes and one fragment per job. Payload encoding and storage I/O are deliberately
outside this microbenchmark.

The executables ran alternately on the same pinned CPU (CPU 0), in three paired
runs. Each run used seven rounds per mode; the first was discarded. Medians of
18 measured rounds per mode were:

| Mode | Before, ns/fragment | After, ns/fragment | Change |
| --- | ---: | ---: | ---: |
| Single stream | 252.855 | 248.810 | -1.60% |
| Multiple writable inodes | 432.658 | 436.572 | +0.90% |

These measurements bound the queue overhead of the fix; they are not an SMB
throughput measurement or evidence of a general throughput improvement.
Artifacts and the comparison runner are under `.artifacts/ingest-order/`.
The manual benchmark can be rerun with workspace-local Cargo directories:

```sh
CARGO_TARGET_DIR=/source/fastdup/.artifacts/target \
TMPDIR=/source/fastdup/.artifacts/tmp \
cargo test --release -p fastdup-appliance --lib \
  ingest_queue_admission_benchmark -- --ignored --nocapture
```
