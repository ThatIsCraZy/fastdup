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

## Installed SMB validation

RPM `fastdup-0.6.4-6.el10.x86_64` was installed on the test VM. The disconnected
mount left by the aborted process was detached after stopping that service.
Normal recovery completed with PID 31250, which started at 21:05:32 CEST and
mounted the repository at 21:09:03. The same process remained active with zero
restarts after the test.

The encrypted SMB 3.1.1 loopback test wrote 10 GiB of random, repeated, locally
modified, and zero-filled content through irregular request fragments. The
initial 2 GiB used one connection and took 53.62 seconds. Then a second
connection repeatedly opened a small file, wrote and read-verified 4 KiB, and
closed it: 10,000 complete cycles finished while the main write continued.
All SMB connections and payload processing ran on the VM; SSH over the VPN
only orchestrated the test.

The complete main write took 309.696 seconds, including the additional file
activity. The maximum measured one-MiB write batch, spanning several SMB
requests, was 0.046 seconds. Full readback of all 10 GiB took 186.971 seconds
and matched SHA-256
`0d1b877dc0a0976263d3ae3fba8c175da37ac83b598463d71ee0ac076dcff34a`.
A sample of 516 completed checkpoints had a maximum wall time of 0.171 seconds
and no critical errors. Temporary files, the test share, and its Samba account
were removed successfully. This mixed-activity workload is not an end-to-end
A/B against the earlier single-connection workload and does not measure the
Veeam client's LAN throughput. An actual Veeam retry is still required.

RPM SHA-256: `2bf257efb8b90c0f4dc1d6e02b12e8d3dcc34b3a686ce7bbf177c35cfc6af22d`.
