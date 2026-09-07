# Resume an incomplete background scrub

ADR 0092 persists checked Container work across clean stops and crashes. The
runtime uses a Metadata journal with bounded per-entry reads and an offset index.
Saved entries retain Chunk identities and Base requirements so the next mount's
selected graph can be reconciled, including foreground commits since the prior
start. No DATA format changes, new SQLite dependency, unsafe code, or extra
payload reads at mount were introduced.

## Validation

- 27 structural/startup integration tests, including 14 new progress cases:
  crash before/after every journal creation, append, sync, completion, torn-tail
  repair and completed-round reset operation; every truncation point of a
  dependent entry; damaged header/entry checksums; wrong pool binding; expired,
  completed and clock-reversed rounds; changed/missing Containers; missing Bases;
  RETIRING exclusion; newer graph requirements; fresh/resumed Base ordering;
  interrupted verification; and demand/offline verification despite prior checks.
- 189 store/appliance/control library and binary tests passed (14 existing manual
  benchmarks ignored). The actual filesystem-backed worker test stops during its
  first pass, joins and flushes, starts a new worker, confirms exactly the saved
  count is reused, completes with less DATA read traffic, then confirms a third
  worker starts a full new round. Existing integrity-failure and cancellation
  tests retain their GC/write-admission checks.
- 56 end-to-end maintenance tests verify the unchanged scrub and GC contracts.
- Five UI tests include carried-forward/new/remaining counters in historical
  samples. Control serialization preserves new counters while older samples
  without them still round-trip unchanged. TypeScript/Vite production build and
  production Clippy with warnings denied pass.

Total: 277 passing tests. Command logs are workspace-local under
`.artifacts/scrub-resume/`. Generated UI distribution assets are refreshed for
source builds that serve the checked-in bundle.

## I/O evidence and limits

The deterministic dependent/Base resume case asserts exactly one length lookup
and two 4-KiB range reads per saved Container; no payload or full-structure scan
occurs. Coverage succeeds even when the dependent is encountered first. A full
repository offline scrub is also exercised with both valid and damaged progress
journals and still detects DATA corruption independently.

Header/Footer reads retain random-access advice and share one pacing decision
per 256 KiB rather than sleeping after each 4-KiB read. Journal syncs occur after
64 new checks or five seconds at a Container boundary, plus orderly worker stop.
These are algorithmic and local functional results, not a VM/Veeam throughput
measurement. No service restart or test-VM deployment was performed for this
change. Progress recorded by an older runtime cannot be recovered retroactively.

A saved round is historical checking work. Payload bitrot that leaves the
Container envelope intact is deliberately deferred to demand/offline verification
or a later full round. Completed rounds are not reused at the next mount;
incomplete rounds expire after seven days. No new periodic scrub scheduler was
added. The journal stores Chunk maps as well as Container summaries; the format
uses 121 bytes per Container frame plus 37 bytes per independent Chunk or 73 per
dependent Chunk, with one 80-byte round header and a 37-byte completion marker.
