# Metadata activity after background scrub (2026-09-13)

Read-only diagnosis on VM 10.1.1.161, running fastdup 0.7.4-16, PID 268873.
The screenshot at 14:32:19 shows 92 MB/s Metadata reads. Journal confirms
Container scrub completion at 14:30:46 (74,576 Containers, 5,861,806,080 read bytes).

The subsequent 39.019-second live counter window did not capture that spike:
Metadata disk reads averaged 0.0021 MB/s; DATA reads averaged 8.963 MB/s.
Metadata cause counters increased only by 32,768 small-file bytes. Point syscall
samples captured 142 DATA Container preads on the Online GC worker and five
Recovery Checkpoint preads on a Tokio worker. GC state was running; scrub complete.
These samples cannot retrospectively assign the screenshot's 92 MB/s.

Code explains the post-scrub phase boundary: run_online_gc_runtime gates GC on
scrub_gate.permits_gc(). Every run_online_gc_cycle begins with
finalize_recovered_online_gc, then Metadata GC and candidate catalog work.
finalize_recovered_online_gc calls recover_active_generation, which invokes
recover_active_locked under Independent intent, rereading and verifying the
Exact activation and dependencies even before checking whether its generation
is already installed. This is a concrete redundant recovery path in live GC.
Metadata GC uses Scan intent, which allows hits but declines admission.

A separate periodic task calls publish_committed every 90 seconds; the source
Manifest graph is scanned before publish_source can reuse an existing checkpoint.
Journal repeatedly reports the unchanged generation 2190, 1,481 Metadata objects,
230,633,472 Metadata bytes, at 14:27:44, 14:29:10, 14:30:39, 14:33:24 and 14:33:38.
These summaries do not prove those bytes were rewritten each time. Sampled fdrc
reads do confirm checkpoint activity. It is independent of the Container scrub.

Next optimization boundary: use the installed owned Exact generation during
ordinary GC; retain true recovery and final deletion verification. Avoid repeated
checkpoint graph/image work for an unchanged successfully published Commit.
Capture another spike with per-phase telemetry to quantify attribution; neither
the cumulative 6.0 GB Manifest reads nor the screenshot provides that split.
No production code or VM configuration changed in this diagnosis.

Evidence: .artifacts/tmp/post-scrub-metadata/{sample.json,details.json,events.log,journal.log}.
