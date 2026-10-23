# Historical diagnostics

The telemetry page has a shared historical selection. Clicking either chart,
moving the keyboard-accessible sample slider, or selecting an event updates
throughput, drive state, caches, GC, checkpoint and pipeline details together.
The displayed timestamp is the actual retained observation. Event selection
uses the nearest retained observation; it does not manufacture an exact match.

Detail zoom requests a fixed interval around the selection. Explicit local
date/time inputs support revisiting an incident without moving the window to
the present. Older short intervals select retained 10-second or 60-second
data rather than incorrectly looking only in the raw table. The server and UI
bound requests to 90 days; chart responses remain bounded to 1,500 buckets.
Late responses cannot replace a newer selection or the Live view.

## Aggregation and evidence

Retention remains 24 hours raw, seven days at 10 seconds and 90 days at 60
seconds. An additive, explicitly versioned JSON history summary records source
interval, resolution, source count, and weighted mean/minimum/maximum for
frontend Read/Write throughput and CPU/RAM. Query aggregation merges those
summaries instead of choosing one arbitrary sample and discarding the rest.
Long queries use existing coarse buckets, with finer observations for the tail
that may have advanced since maintenance. Readers use a separate SQLite
connection and do not hold the sampler mutex during history scans.

One original incident snapshot per bucket preserves contemporaneous engine
details and workload context. Selection prioritizes closed admission, then its
current closure duration, then the highest occupied interval-latency bucket,
then the lowest combined frontend throughput. The original snapshot contains
no nested history or repeated live chart window. The UI explicitly separates
aggregate curves from the last snapshot and the retained incident snapshot.
Extrema of different metrics need not have occurred together.

Frontend telemetry now exposes cumulative, disjoint latency-bucket counts.
The sampler computes differences within one runtime and wall-clock interval.
Restart, counter reset, unavailable runtime, clock reversal, or a gap longer
than 30 seconds establishes a new baseline. Duplicate observations within one
second do not advance the baseline. Window histograms merge bucket counts,
never percentiles. Their coverage excludes gaps. Percentiles and the peak
bucket are upper bounds; the open-ended bucket is shown as greater than 60
seconds. Both successful and failed completed requests are included.

Legacy aggregates remain readable, but report unknown counts and incomplete
extrema. Old mount percentiles cannot be converted into interval percentiles.
Previously discarded details cannot be recovered. The incident policy is
bounded: it does not retain every five-second state change, every separate
metric's extremum snapshot, or complete per-request tracing. A boundary bucket
may extend outside a requested interval; its actual bounds remain visible and
are included in the export.

## Workloads, events and diagnosis

Every ten seconds, a separate sampler reads
[`smbstatus --json --shares`](https://www.samba.org/samba/docs/current/man-html/smbstatus.1.html).
It records client, Share, SMB session ID and connection start time alongside
configured Share paths, repository identity and the native Veeam storage area.
The child process has a two-second deadline and a 1 MiB output bound. At most
256 connections are kept, with explicit truncation. Missing observations and
context more than 20 seconds from the selected snapshot are marked. Connection
search navigates to retained observations with the chosen client or Share.

These are connection associations, not per-client or per-Share I/O accounting.
Engine metrics remain appliance-wide. The current native Veeam service does
not expose backup job/session IDs; they remain unavailable rather than being
inferred from an SMB session, filename, or configuration. Connections shorter
than the workload sampling cadence can be missed.

Authenticated, range-filtered events include Audit records and the last stored
update of each management Job. They are bounded to 1,000 entries with a visible
truncation flag. Intermediate Job progress transitions are not reconstructed.
The selected interval highlights nearby events with a 30-second margin.

Versioned diagnostic rules carry timestamp, runtime identity, field names,
values and units. They identify closed admission, simultaneous Durability-Lag
and active Publication-Waits, interval request errors and high latency buckets,
and sampled running GC. Lifetime maxima never establish a current wait. The
wording distinguishes observation and correlation from proven cause; absence
of a rule match does not establish healthy operation.

The JSON export contains the requested and observed windows, selected sample
identity, retained samples including incident evidence, events, diagnostic
findings, coverage and truncation information. Missing event data is explicit.
The exported metrics retain their scope and source timestamps.

## Verification

Rust regressions cover weighted aggregation, both retention tiers, narrow old
windows, query point limits, a between-point spike, coherent incident details,
late samples after maintenance, retention boundaries, legacy and unknown
format versions, histogram resets and gaps, same-second observations, bounded
SMB parsing, authentication, event range filtering and truncation. A SQLite
trigger injects a rollup write failure: raw sources survive and reopening and
retrying produces the complete aggregate. Repository recovery and offline
scrub do not consume the telemetry database; storage formats and authority
are unchanged.

UI regressions cover chart and slider selection, fixed zoom windows, stale
responses, export, percentile/peak calculation, evidence rules and missing
legacy extrema. Browser checks use mocked API data and real ECharts events
at 1,440 px and 390 px, including original incident selection and zoom.
TypeScript checking and a production Vite build are included. Build output,
test logs, the manual RSS probe and browser evidence are under
`.artifacts/history/`. These checks preceded the appliance deployment recorded
below.

Recorded results: 75 Control Plane tests, the focused POSIX telemetry test and
78 UI tests passed. TypeScript, production build, formatting and whitespace
checks passed. The existing Vite chart-chunk size warning remains. Scoped
library Clippy passed with the existing `similar_names` findings suppressed;
the unrestricted all-target run also reports pre-existing fixture literal and
floating-point comparison lints, so it is not a clean full-repository lint run.

The manual 3,600-sample legacy-series probe used 15,800 KiB peak RSS, including
fixture creation. Rollup took 110 ms; querying 1,200 display buckets took 91 ms.
This is a bounded-memory regression probe, not a fully populated 90-day
production-load qualification. Retaining an incident adds up to one extra
original snapshot per aggregate, plus small summary/histogram fields.

## Appliance deployment

On 2026-10-23, RPM `fastdup-0.8.1-49.el10.x86_64` replaced RPM 48 on
`10.1.1.161`. The update ran from 04:18:05 to 04:19:50 CEST, after all FastDup
backup jobs reported `Stopped`. The latest qualification session,
`95aa052f-60bc-4b44-9629-a5dfdc2836a2`, had completed successfully in 7:46
with Veeam reporting 766 MB/s. It ran on RPM 48; it is not an end-to-end test
of RPM 49.

The package was built from clean isolated checkout
`1ab5ed53f8dc9b2672a2fd38e6601ecbd1c4a9bc`, including feature commit
`e2c1e1ea1744b751dacf77d67de237a40ba793d8`. The packaging script performed
a fresh UI production build before compiling the Control Plane, which embeds
the UI assets. The generated files were byte-identical to the previously
tested UI build. The RPM SHA256 is
`87e9d008fc5a25fd52ebc2069d1cbfe2cbca705089b175d1c8dbe68d4d470c7b`.

Post-deployment checks passed:

- Installed RPM identity and header match the selected package. Runtime PID
  changed from `3945699` to `3978184`; hashes of the running Runtime, Agent
  and Control Plane executables match their packaged files. The installed
  maintenance executable also matches the package.
- Repository, Agent, Control Plane, Samba and Veeam container services are
  active. The FUSE mount exists; Agent health reports `online` without a
  runtime issue. All four Veeam transport/deployment/environment/immutability
  services inside the container are active.
- The container sees the same repository mount as the host. Two read-only
  64-KiB VBM prefix checks returned identical host/container hashes.
- HTTPS requests returned status 200 for the index and all four referenced
  assets. All five SHA256 values match the fresh build. The index references
  `index-pUdG5Ok9.js` and `index-a1EKpQBA.css`; its SHA256 is
  `dd73a21bc315bc0cf04e30bf187da975a8794402969096c130ea2abb5646f493`.
  The index is served with `no-cache`, while named assets are immutable.
- Runtime inspection exposes the new 16-bucket read/write histograms. Fresh
  persisted telemetry identifies runtime `3978184`, contains one-second
  interval histograms and sampled workload context with SMB status
  `available`. Read/write errors are zero and integrity failure is false.

UI delivery was verified against the live appliance; authenticated chart
interaction remains covered by the pre-deployment browser tests described
above. No new backup was launched for this deployment verification.

The update job is
`.artifacts/test-vm-update/20261023T021711Z-3228e919cd/job.json`.
Build, package provenance, service/process checks, delivered UI hashes,
runtime inspection and container-read evidence are under
`.artifacts/deploy-history-20261023/`. The RPM is under
`.artifacts/wt-history-49/rpmbuild/RPMS/x86_64/`.
