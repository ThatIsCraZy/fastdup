# Adaptive Base-read admission — 9 September 2026

The writer uses the existing Similarity Sketch before speculative cold Base
reads, with bounded volatile feedback from observed additional payload savings
and Base-resolution/codec time. See ADR 0018 for the policy and trade-offs.
No durable format or demand-read integrity rule changes.

Validation on the implementation worktree:

- 89 Store library tests, including the new bounded learner and a real
  filesystem I/O-counting test with deliberately misleading Similarity hints.
- 107 appliance/control library and binary tests.
- 17 manifest-reader, prefix-recovery and differential reduction integration
  tests, including byte-exact restoration and verified-cache behavior.
- 39 WebUI tests, including new evidence counters and absent historical fields.
- Control-plane parsing/serialization checks preserve the new optional counters.
- Production Clippy with warnings denied; TypeScript/Vite production build.

Total: 252 distinct passing tests; existing manual benchmark cases remain ignored.
Logs are in `.artifacts/candidate-gate/` in the build workspace.

The deterministic misleading-hint case produces no compression gain despite a
perfect Sketch match. After eight cold learning attempts, 31 consecutive
candidates incur no additional storage reads; the next attempt probes again.
A verified warm Base bypasses rejection without reading storage. Ordinary cold
Base resolution still rejects a damaged encoding record. The policy tests also
show that measured cost can favor a distant Sketch over an expensive near
match, and that changed workloads can recover through exploration.

This is a bounded I/O regression test, not a Veeam throughput benchmark. It does
not establish a production savings percentage or optimal tuning constants.
False rejections can reduce compression ratio; learned state resets on runtime
restart and uses nonblocking fail-open admission during contention. No VM
installation or repository service restart was performed for this change.
