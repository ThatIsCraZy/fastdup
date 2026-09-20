# Veeam bottleneck detection and what it means for fastdup

Research date: 2026-09-20

## Scope and source quality

This note uses Veeam's own documentation. The principal description applies to
Veeam Backup & Replication build **13.1.1.18**. Veeam does not publish the
implementation or a complete mathematical formula for these counters. Claims
below therefore distinguish documented behavior from deductions based on the
documented pipeline and our measured job.

## What the four percentages measure

Veeam models backup data transfer as a repeating pipeline: read from source,
process on proxy, transport, then write to target. The displayed percentage is
the share of the measured job time for which that pipeline component was busy;
it is **not** CPU utilization, disk utilization, NIC utilization, or percentage
of a component's rated capacity. Veeam selects the component with the greatest
busy-time percentage as the primary bottleneck.

| Counter | Veeam's pipeline component | Practical interpretation |
| --- | --- | --- |
| Source | Source disk reader | Time spent obtaining source blocks. |
| Proxy | Backup proxy | Time spent processing source data, including work performed before transfer. |
| Network | Network queue writer between the source-side and target-side Data Movers | Time spent handing processed data to the target side. This is not a generic measurement of every physical network hop. |
| Target | Target disk writer; for gateway-based designs Veeam also describes this as the gateway/target component | Time spent accepting, processing, and storing data on the target side. |

Source: [Performance Bottlenecks, VBR 13.1.1.18](https://helpcenter.veeam.com/docs/vbr/userguide/detecting_bottlenecks.html).

Consequences:

- `Target 94%` does not mean that a target disk or CPU is 94% utilized. It means
  the target pipeline stage was active for 94% of the measurement interval.
- The individual percentages are time fractions and thus meaningful on their
  own. The **choice of primary bottleneck** is relative: Veeam picks the largest
  fraction.
- The stages are measured separately and can be busy concurrently. Their
  percentages are not shares of one 100% total and must not be added or
  normalized to 100%.
- A primary bottleneck is not automatically a fault. Veeam explicitly says the
  statistics identify the weakest stage and need not indicate a problem when
  the achieved rate and backup window are acceptable.
- Near-equal values can make the primary label less informative because a small
  change can move the maximum to a neighboring stage. The percentages should
  be retained, rather than reducing the result to the label alone.

### No documented threshold

The v13.1 documentation specifies the **maximum workload/busy time** rule. It
does not document a minimum percentage, minimum lead over the second-highest
stage, sampling interval, warm-up rule, rounding rule, or tie-breaking rule for
`Primary bottleneck`. Therefore a 90% or 95% cutoff must not be assumed. Nor is
there an official basis for treating only a value near 100% as a bottleneck.

Veeam separately reports `Throttling` when repository read/write rate limits or
network throttling rules constrain a job. That is distinct from an organically
busy `Network` or `Target` stage.

Veeam's officially maintained R&D FAQ, currently scoped to v12.3, adds useful
operational detail: the stage values are independently measured busy-versus-wait
time; the Network counter concerns writes into the network-stack queue; and
terminal per-VM averages are more reliable than transient real-time values,
which can be distorted by effects such as initial cache population. This is
consistent with the current v13 pipeline description, but it is older-version
guidance, not a published v13 implementation contract.

Source: [Veeam R&D FAQ, updated for v12.3](https://forums.veeam.com/viewtopic.php?f=2&t=17633).

## What `Network` means for an SMB repository

An SMB share cannot host a Veeam Data Mover. Veeam runs the target-side Data
Mover on a gateway server; that mover communicates with the source-side Data
Mover and accesses the SMB share. Veeam recommends placing the gateway close
to the share.

Sources:

- [SMB (CIFS) Share, VBR 13.1.1.18](https://helpcenter.veeam.com/docs/vbr/userguide/smb_share.html)
- [Gateway Servers, VBR 13.1.1.18](https://helpcenter.veeam.com/docs/vbr/userguide/gateway_server.html)
- [Shared Folder Settings, VBR 13.1.1.18](https://helpcenter.veeam.com/docs/vbr/userguide/smb_repository_server.html)

The important boundary is therefore:

```text
source reader -> source Data Mover/proxy -> Veeam transport -> target Data Mover/gateway -> SMB client -> Samba/FUSE/fastdup
     Source              Proxy                    Network                    Target
```

This boundary is a deduction from Veeam's component definitions and SMB
architecture. In particular, a low Veeam `Network` value does **not** clear the
gateway-to-SMB network hop or the SMB protocol path. Backpressure while the
gateway waits for SMB writes, flushes, file extension, or other target I/O is
expected to surface primarily as `Target`, because the SMB share is downstream
of the target-side Data Mover.

Likewise, `Network` becoming primary would identify the Data-Mover transport
queue as the busiest stage; it would not by itself prove that a physical NIC is
at line rate. The official documentation names the software queue writer, not a
link-utilization counter.

## Job versus task statistics

Veeam exposes overall job/session statistics and details for each processed
object. Its v13 workload documentation explicitly distinguishes the aggregated
job `Load` from the `Load` shown after selecting a specific VM. One backup task
normally corresponds to one VM. Veeam does not document how the job-level busy
percentages are aggregated across overlapping tasks; an arithmetic mean,
duration-weighted mean, maximum, or another formula must not be assumed.

Sources:

- [Analyzing Performance Bottlenecks](https://helpcenter.veeam.com/docs/vbr/userguide/ahv_bottlenecks.html?ver=13)
- [Viewing Real-Time Statistics, VBR 13.1.1.18](https://helpcenter.veeam.com/docs/vbr/userguide/realtime_statistics.html)

For diagnosis, use both levels:

1. The terminal job-level `Load` answers which shared pipeline stage dominated
   the whole run.
2. Per-VM terminal bottlenecks show whether the conclusion is broad or caused
   by one unusual task.
3. Intermediate labels should not be treated as final; the busy-time fractions
   continue to accumulate while the task runs.
4. Prefer the final per-VM averages for attribution. A short real-time change
   from Source to Target can reflect changing pipeline/cache state, while the
   terminal value summarizes the transfer interval.

## Rate counters are not interchangeable

Veeam defines `Transferred` as bytes sent from the source-side Data Mover to the
target-side Data Mover after compression and source-side deduplication. It also
warns that this value is not necessarily the size finally written to the backup
file. `Processing rate` is based on data read and data-transfer time, not total
wall-clock job duration. These distinctions matter when comparing Veeam's UI,
wire rate, SMB bytes, and fastdup's logical ingest rate.

Source: [Viewing Job Session Results, VBR 13.1.1.18](https://helpcenter.veeam.com/docs/vbr/userguide/session_results.html).

## Deduction for the measured fastdup run

The clean control run recorded:

- terminal job load `Source 45% > Proxy 14% > Network 16% > Target 94%`;
- `Target` for all eight VM tasks;
- 304,358,750,992 transferred bytes in a 403.381-second task window, or about
  754.5 MB/s (6.04 Gbit/s of transferred payload);
- no admission closure and no multi-second checkpoint outlier;
- a 10-Gbit/s physical ceiling.

Local evidence:

- `.artifacts/veeam-e2e/20260919T214751Z-2280548/final-session-logs.json`
- `.artifacts/veeam-e2e/20260919T214751Z-2280548/final-task-sessions.json`
- `.artifacts/smb-tail-fix/REPORT.md`

The result is not a marginal maximum: Target leads Source by 49 percentage
points and Network by 78 points, and every task independently ends with Target.
Under Veeam's documented model, the source and source-to-target Data-Mover
transport had substantial waiting time while the target stage remained busy.
The remaining throughput limit is therefore very likely **after the target
Data Mover has received data**, within one or more of:

1. target Data Mover/gateway processing and scheduling;
2. the gateway SMB client, its TCP path to the share, and SMB credit/request
   concurrency;
3. Samba request dispatch and synchronous SMB command completion;
4. FUSE request concurrency and scheduling;
5. fastdup ingest, index/metadata work, durability, or backing-device service
   time.

The fixed multi-second checkpoint stalls were a real tail-latency defect, but
their absence did not move Veeam's target stage away from 94% or establish a
throughput gain. This means those stalls were not the steady-state throughput
limit.

### Localization inside the target stage

The instrumented warm run provides a stronger discriminator at the SMB/FUSE
boundary. Samba and fastdup reported the same 304,674,821,969 bytes during the
run, so the counters cover the same payload:

| Observation | Measured value | Interpretation |
| --- | ---: | --- |
| SMB2 writes | 74,723 | Veeam issued large writes rather than a small-I/O workload. |
| Mean SMB2 write size | 3.889 MiB | This agrees with the target Data Mover's 4 MiB transfer block size. |
| Mean SMB2 write time | 6.796 ms | This is steady target service latency, not a rare tail event. |
| Time in Samba's asynchronous `pwrite` | 505.740 s of 507.790 s SMB2-write time | 99.60% of SMB write service time is below Samba's SMB dispatcher. |
| Residual Samba write overhead | about 0.027 ms per request | SMB command parsing/dispatch is not the material cost. |
| Average writes in service | about 1.139 | Derived as cumulative SMB2-write service time divided by the 445.948 s task window. |
| Fastdup FUSE writes | 364,991, averaging 0.796 MiB | Each SMB2 write became about 4.885 FUSE writes. |
| FUSE configuration | 1 MiB `max_write`; `max_background=12`; observed queue maximum 6 | The 4 MiB Veeam requests are fragmented, but the kernel queue did not hit its configured cap. |

The service-time arithmetic explains the observed rate: one active write at
the measured size and latency services about 602 MB/s, and the inferred 1.139
average writes in service corresponds to about 686 MB/s. That agrees with the
measured 682.5 MB/s instrumented ingest rate. Reaching 10-Gbit line rate with
the same write latency would require roughly twice the effective outstanding
write concurrency; alternatively, keeping the present concurrency would
require reducing target write latency to roughly 3.7 ms or less (and somewhat
less after protocol overhead).

This is an application of Little's law to Samba's cumulative service timer, so
it identifies the capacity boundary but not whether the missing concurrency is
chosen by the gateway SMB client or caused by request serialization farther
downstream. It does establish that the current rate is accounted for by
sustained target-write service, rather than by occasional checkpoint pauses.

Additional evidence narrows the likely implementation weaknesses:

- Fastdup advertises a 1 MiB maximum FUSE write, causing almost 4.9 FUSE
  operations per Veeam/Samba write. The CPU profile attributes about 6% to the
  kernel's FUSE copy and 5% to receive-buffer clearing. This request and memory
  traffic is avoidable amplification on the ingest boundary.
- Samba performed 79,027 `chdir` calls and 158,845 `stat` calls during the run,
  with a combined cumulative time of 120.7 seconds, or approximately 1.52 ms
  per SMB request. These operations compete for the same zero-TTL FUSE
  namespace even though the workload is predominantly sequential data ingest.
- The fastdup profile's largest compute hotspot was
  `BTreeMap<ChunkId, u64>::insert` plus its comparisons (about 17% together),
  used in checkpoint manifest planning. Checkpoint metadata commit accumulated
  93.5 seconds and manifest planning 54.6 seconds over the run. This is a
  plausible shared software-path cost, but the profile does not prove it is on
  every synchronous write's critical path.
- Whole-machine resource ceilings are contradicted by the measurements: CPU
  was about 37.8% busy, data and metadata devices were around 16% utilized with
  approximately 2 ms write await, FUSE's observed queue did not reach its
  congestion threshold, and mutation admission never closed.

The best-supported diagnosis is therefore **steady target-side SMB-to-FUSE
service latency combined with insufficient effective write concurrency**. The
first concrete fastdup weakness is 4 MiB-to-1 MiB request fragmentation and its
copy/allocation/dispatch cost. Namespace metadata chatter and checkpoint
manifest/index work are secondary shared-path candidates. Neither backing-disk
bandwidth, aggregate CPU, rare latency outliers, nor Samba command dispatch is
supported as the primary bottleneck by this run.

Local evidence:

- `.artifacts/smb-tail-fix/run-02-warm/smb-profile-final.txt`
- `.artifacts/smb-tail-fix/run-02-warm/runtime.jsonl`
- `.artifacts/smb-tail-fix/run-02-warm/kernel.jsonl`
- `.artifacts/smb-tail-fix/run-02-warm/cpu-report.txt`
- `crates/fastdup-posix/src/fuse_adapter.rs`

The next discriminator should measure the same warm E2E run at the target
boundary, without high-frequency profiling overhead:

- On the gateway, correlate Data Mover input rate with SMB client writes and
  SMB request latency/concurrency.
- On fastdup, correlate accepted SMB bytes with outstanding FUSE requests,
  ingest queue occupancy, commit/publication wait, metadata/data-device latency,
  and completed writes.
- If Data Mover input is near 10 Gbit/s but SMB output is lower, the gateway/SMB
  client is limiting. If SMB requests remain continuously outstanding and
  complete only at roughly 755 MB/s, Samba/FUSE/fastdup is limiting. If fastdup
  accepts writes faster than durable completions, the internal ingest/durability
  pipeline is limiting.
- Repeat at controlled task counts. Throughput that scales with tasks until the
  NIC saturates points to insufficient per-request/per-file concurrency;
  throughput that plateaus with growing internal queues points to a shared
  serialized resource or device ceiling.

## Limits of this conclusion

- Veeam's public documentation gives stage semantics, not the internal counter
  implementation. It does not reveal queue-depth calculations, sampling
  cadence, aggregation weights, or tie behavior.
- `Target` localizes the problem to a stage, not a single process or device.
- A 10-Gbit link has protocol overhead, and Veeam's transferred bytes are not
  guaranteed to equal Ethernet bytes or final backup-file bytes. The gap from
  6.04 Gbit/s payload to line rate cannot be assigned solely from these counters.
- The conclusion applies to the warm qualification-job Active Full and its SMB
  topology. Cold-cache behavior and synthetic/merge operations have different
  I/O patterns and require separate qualification.
