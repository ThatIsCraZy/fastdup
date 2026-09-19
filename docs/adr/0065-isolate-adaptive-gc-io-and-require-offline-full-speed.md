---
status: accepted
---

# Isolate adaptive GC I/O and require offline full speed

Adaptive Scrub and GC phases run on short-lived workers in Linux's
work-conserving idle I/O class. Space pressure may promote CPU scheduling from
nice +10 to normal, but adaptive maintenance never enters the frontend I/O
class or adds measurement, locks, waits or reservations to the write hot path.

Only CLI `gc-now --offline`, under the exclusive Appliance Lease from ADR 0069,
uses ordinary I/O priority and unrestricted CPU scheduling. Full speed changes
resource scheduling, not proof, replacement-before-deletion, Exact activation,
identity or sync invariants. The local daemon control request named `gc-now`
starts an urgent online quantum with normal CPU but idle I/O.

The small Linux `ioprio_set` adapter exposes an integer-only safe interface.
Fresh workers set priority before I/O because an unprivileged idle-class thread
cannot be assumed able to promote itself later.

## Scheduling and isolation

Defaults admit:

- a small Background quantum every 15 minutes during frontend activity;
- a larger Idle quantum every minute after 30 quiet seconds; and
- an Urgent quantum every 30 seconds at the inclusive 90% high watermark.

Urgency stays latched to the inclusive 85% low watermark. Startup validates
intervals, watermarks, one wrapping UTC window and the maximum replacement
encoder count. Background uses one reader/encoder; Idle and Urgent use at most
the configured count and available CPUs. The scheduler samples the existing
frontend io_uring submission counter.

Online DATA maintenance uses synchronous `FsStorageIo` on its own prioritized
workers. It shares descriptor caches, RETIRING selection and Exact-generation
pins with the repository, but not the frontend io_uring ring or inflight-byte
budget. The daemon control socket is mode 0600 below the Metadata root; CLI
clients never open the repositories. Directory-fd path resolution avoids the
`sockaddr_un.sun_path` path-length limit.

Cancellation is cooperative between objects and phases. Maintenance-only
repository clones share locks and pins without being able to cancel frontend
work. No worker detaches at shutdown. Before durable RETIRING, cancellation may
leave harmless immutable replacements. After RETIRING, pin drain, verified
unlink, DATA directory sync and REMOVED activation must finish. Storage errors
and assertion failures remain errors even when cancellation races them.

## Bounded proof and retirement work

One quantum performs exactly one independent whole-Container verification read
per victim. That read creates the complete replacement plan: verified Chunk
payloads, byte-exact transplants for fully live Records and all RETIRING
Locations. Publication consumes the plan without rereading victim DATA. A stale
plan discards its already-published replacements as ordinary future candidates.

Profitability uses the proven Record geometry: independent Records apportion
encoded length across decoded Chunks; dependent Records use the conservative RAW
upper bound because relocation removes the Base dependency. The estimate affects
selection only, never correctness.

Before unlink, an Independent Header/Footer read must match the sealed identity,
generation and physical length. Payloads and Bases are not reverified: content
truth came from the in-process proof, while replacement activation and pin drain
establish liveness. Restart recovery keeps full cold-path reverification because
that proof no longer exists.

Item-local proof and identity reads may run in parallel workers of the same I/O
class. Dispatch is bounded by job count, one-reader-ahead and 128 MiB of whole
images. Accounting, Chunk claiming, RAW bounds, profitability, activation, drain
and unlink remain serialized in canonical shortlist order. Background retains
one worker during frontend activity.

Retirement purges victim descriptors and verified Location evidence; normal
remove-file invalidation handles layout/range state. Catalog recovery pairs its
Header/Footer, leases the immutable generation and performs one complete row
audit; refresh reuses the caller's open snapshot.

Fault tests cover identity changes after proof, activation races, cancellation,
worker-count-independent selection and compressed candidates rejected by the
RAW-only heuristic. Maintenance remains bounded and resumable in every online
mode.
