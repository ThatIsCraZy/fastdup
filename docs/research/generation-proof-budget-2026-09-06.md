# Generation proof admission at capacity

On the Rocky test VM running 0.6.4-4, the repository process aborted at
2026-09-06 20:25:09 CEST with `combined Active and Frozen Generation Proof
Sets exceeded their budget`. Veeam reported failed asynchronous writes at
20:25:23, starting at offset 7,863,088,128. A disconnected FUSE mount remained;
automatic restarts failed because the mount path could not be inspected as a
directory. Missing runtime telemetry explained the empty detail panel.

The 65,536-entry cap was treated as an impossible invariant derived from two
512-MiB resident generations divided by a 16-KiB CDC minimum. Externalized DATA
no longer occupies that resident budget, and boundary Chunks can be shorter
than the CDC minimum. The cap is therefore a cache admission policy, not a
valid bound on incoming verified dependencies. The truncated VM core did not
retain the crashing stack; the exact caller and workload contribution cannot
be reconstructed from it.

Two deterministic tests reproduced fatal admission at the actual cap, including
combined Active/Frozen occupancy and publication completion against a full
Frozen set. Both failed before the fix in 0.06 seconds. Admission now checks the
combined cap under the generation lock before inserting. Existing proofs remain
pinned and updatable. New entries may remain uncached; successor verification
still requires a complete storage proof for every uncached dependency. Frozen
completion releases capacity, and publication claims retire even when caching
the result is rejected. No durable format or recovery rule changes.

Regression tests cover Frozen and Active admission, promotion from Frozen and
history, canceled freeze, successful completion, and publication claim reuse.
The real Container verifier rejects a missing uncached dependency, accepts its
published bytes, and rejects a subsequently corrupted Container while the
cache remains full. Final focused tests passed in 0.27 seconds. The appliance
library and durable namespace fault, recovery, and write-through suites passed
94 tests with two existing ignored cases. Library Clippy passed with warnings
denied.

The UI now puts the reduced Small-File limit in the shared topbar with a
keyboard-accessible explanation. A missing online telemetry sample retains the
last quota for that mount; an explicit new quota or non-online state updates or
clears it. Missing detail telemetry no longer tells the operator to mount the
repository. All 31 UI tests and the production build passed; browser checks at
1440 and 390 pixels verified the notice, keyboard explanation, and absence of
horizontal overflow.

Build, VM evidence, and browser artifacts are local under
`.artifacts/ui-quota-notice/` and are excluded from source control.
