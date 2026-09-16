---
status: accepted
---

# Isolate adaptive GC I/O and require offline full speed

Adaptive maintenance executes each Scrub and GC phase on a short-lived worker
in Linux's work-conserving idle I/O class. Space pressure may promote CPU
scheduling from Unix nice +10 to normal priority, but it does not promote
adaptive maintenance into the frontend I/O class. This is deliberately
asymmetric: frontend work can prevent admission of new maintenance I/O, while
maintenance never asks the write hot loop to measure, lock, signal, sleep, or
reserve capacity on its behalf.

The explicit `gc-now` execution mode skips CPU and I/O demotion. The appliance
CLI admits it only after the operator supplies `--offline` and acquires the
exclusive Appliance Lease from ADR 0069. Full speed changes resource scheduling
only. It does not weaken the
generation proof, replacement-before-deletion, Exact activation, identity
reread, or directory-sync invariants.

Linux I/O priority is applied to a fresh phase worker because an unprivileged
thread cannot be assumed able to promote itself after entering the idle class.
The unsafe `ioprio_set` syscall is confined to one small platform adapter with
an integer-only safe interface. It does not touch file buffers or durable
formats.

Online candidate discovery, proof, relocation, RETIRING activation, pin drain,
and unlink remain bounded resumable quanta. Their DATA operations use a
maintenance-specific adapter or request class and never share frontend
admission capacity. Scheduled windows may raise maintenance concurrency, but
only explicit exclusive full-speed mode may remove the frontend I/O priority
separation.

The writable appliance implements three adaptive paces. Defaults admit a small
background quantum every fifteen minutes under continuing frontend submissions,
a larger idle quantum once per minute after thirty quiet seconds, and an urgent
large quantum every thirty seconds at the inclusive 90% high watermark. Space
pressure stays latched until occupancy reaches the inclusive 85% low watermark,
preventing admission oscillation. Operators may configure all intervals, both
watermarks, one wrapping daily UTC window, and the maximum replacement-encoder
worker count before startup. Invalid configuration fails before storage opens.
Scheduled work uses Idle mode but retains idle-class I/O. Background relocation
uses one encoder worker; Idle/Urgent work uses at most the configured count and
available CPUs. Whole-Container candidate proof verification reads may fan out
to reader threads of that same class and count, while every accounting
decision, transition activation, pin drain, and unlink remains serialized on
the maintenance worker. The scheduler samples the frontend
io_uring submission counter that already exists; write and read paths gain no
GC counter, lock, notification, or branch.

Online DATA maintenance uses a synchronous `FsStorageIo` view on the
idle-prioritized maintenance worker. That view shares the process-local
Container descriptor cache and RETIRING selection state with the frontend
repository, but it does not share the frontend io_uring ring or inflight-byte
budget. Exact-generation pins remain shared through the same Exact repository.

The local online `gc-now` control request starts one urgent quantum immediately
with normal CPU priority but retains idle I/O priority. Only the existing
explicitly offline `gc-now` command means unrestricted CPU and ordinary I/O
priority. The control path is a daemon-owned mode-0600 Unix socket inside the
Metadata root; a CLI request never opens DATA or Metadata repositories and
therefore cannot become a competing storage owner. Bind and connect resolve
that filesystem socket through a short directory-file-descriptor path, so the
bounded `sockaddr_un.sun_path` does not impose a shorter-than-POSIX limit on the
Metadata root while filesystem ownership and mode remain the authorization
boundary.

An adaptive runtime quantum accepts a cooperative maintenance cancellation
request. Stop checks occur between Metadata graph objects, candidate identity
reads, reverse-dependency targets, Container hint rows, and major phases. The
cancellable Generation repository is a maintenance-only clone sharing the same
locks and pins; cancelling it cannot cancel frontend commits or final catch-up.
No storage worker is detached on shutdown.

Cancellation before a durable DATA retirement may leave unused immutable
replacement objects, which remain ordinary future GC candidates. Once RETIRING
is activated, the bounded retirement finishes its predecessor-pin drain,
verified unlink, DATA directory sync, and REMOVED activation. Cancellation must
not turn an incomplete transition into success. The caller then skips further
catalog rebuilding and background phases. Existing storage errors and assertion
failures remain failures, even when a stop was requested at the same time.

## One verification read and parallel item-local work (2026-09-14)

Before this amendment, one Online-GC quantum read and fully verified every
victim Container three times: once in candidate proof (`read_with_index`), once
during replacement publication (`read_verified_image`), and once again before
unlink (`remove_verified_published`). The third read also resolved dependent
Bases, so deleting a victim could read unrelated still-live Base Containers
and fail with `DependentBaseRequired` when a Base retired in the same quantum.
The deletion was still considered "safe" because it re-verified bytes it was
about to destroy.

Now one quantum performs exactly one independent whole-Container verification
read per victim, in candidate proof, using the same full verification the
replacement publication previously paid for. From that one verified image the
proof phase collects the complete replacement plan: independently verified
Chunk payloads, byte-exact encoded Record transplants for fully live Records,
and the `RETIRING` entries for every victim Location. Replacement publication
consumes those collected Items and never reads victim DATA again; a plan that
went stale before activation discards its already-published replacement
Objects, which remain harmless ordinary future GC candidates under the
paragraph above.

Profitability now compares the victim bytes against a compressed relocation
estimate derived from the proven Record geometry of that same verified read:
independent Records apportion their measured encoded length across the decoded
Chunks of the Record, and dependent Records use the conservative RAW upper
bound because retirement forces independent re-encoding away from their Base.
The previous RAW-only rule rejected almost every partially live compressed
Container, so reclaimed bytes after mass deletion were a near-zero trickle;
the RAW bound remains in the proof API but no longer gates collection. The
estimate never promises more reclaim than the RAW bound and remains a
performance-only decision — never a correctness boundary.

Verified unlink is replaced by envelope-identity removal. Before any unlink,
each victim name is re-read through the Independent intent as exactly its
sealed Header and Footer and must still pair to one sealed Container carrying
the expected identity, generation, and immutable physical length. No record
payloads are re-decoded and no dependent Bases are resolved. Consistency is
unchanged: content truth was established by the one independent verification
read, replacement-before-deletion and the atomic `RETIRING` activation with
pin drain still guarantee that no live Location references the victim, and
Scrub remains the independent content auditor. Verifying bytes only to destroy
them added cost without adding a safety property; byte-level corruption of a
dying victim must not block reclamation. Recovery's `finalize_online_gc` keeps
its stronger cold-path re-verification because after a restart there is no
in-process proof to rely on.

Item-local whole-Container verification and identity reads may run on reader
threads that each place themselves in the same idle I/O class (and `nice` only
for Background) before their first read, exactly as a fresh phase worker does.
Reader threads perform only the item-local read and verification under a
scoped `ReadIntent` (`Scan` for proof images, `Independent` for identity
re-reads); dispatch is bounded by job count, a one-reader-ahead window, and a
128 MiB whole-image inflight byte cap that consumption releases. All
victim-order-sensitive accounting — acceptance, Chunk claiming, the RAW proof
budget, and profitability — executes sequentially in canonical shortlist order
on the maintenance worker, so the result is independent of the reader count.
Frontend protection is unchanged: while frontend I/O continues the scheduler's
Background pace still uses one reader/encoder worker, and only quiet or
pressure paces raise the count; maintenance I/O keeps idle-class priority in
every adaptive mode, and no frontend thread ever waits on GC work.

Deletion must be reflected in process caches. A completed retirement removes
the victim identities from the Container descriptor cache and drops verified
Location evidence for the retired Locations from the shared LocationProof
namespace; file layout, handle, and range caches were already invalidated by
the remove-file mutation path. Demand paths never trusted a removed Location
in the first place — reuse requires a currently eligible Location and
`REMOVED` entries shadow retired evidence — so the purge is memory hygiene and
honest deletion reflection, not a new correctness gate. Legacy isolated test
caches retain entries until CLOCK eviction.

Catalog recovery now decodes and pairs a generation's Header/Footer envelope to
choose the newest valid name, then takes the immutable lease that freezes the
inode against every repository mutation and performs exactly one complete row
audit of the leased bytes. Refresh operates on the caller's already-opened
snapshot. One quantum therefore performs two complete content audits per
published catalog generation (publication re-read and final open) instead of
one full audit per recovery call repeated through every phase.

Paired invariants and fault-injection coverage: the envelope-identity gate at
removal fails closed when a victim name no longer pairs to the proven sealed
image after proof (identity change is rejected and the `RETIRING` barrier
stays durable and recoverable), and a replacement-plan/commit race is rejected
only by the unchanged atomic activation and proof-CAS checks. Tests
`online_gc_identity_change_after_proof_blocks_removal`,
`proof_fanout_workers_do_not_change_the_candidate_proof`, and
`compressed_estimate_collects_where_raw_bound_rejected` pair these boundaries.
