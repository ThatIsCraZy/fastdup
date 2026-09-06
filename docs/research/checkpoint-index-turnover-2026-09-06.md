# Checkpoint reads after Exact Index activation

The Veeam failure at 19:26:33 on the test appliance coincided with Samba writes
blocked in FUSE for over 122 seconds. Seven offsets in the kernel hung-task
report match the client's failed 1 MiB writes exactly. Checkpoint generation 44
finished at 19:31:10 after 472.620 seconds, with 470.924 seconds attributed to the
CDC stage. That stage includes reads of existing file ranges. Neither Samba nor
the FUSE daemon restarted during the failure. The actual Windows disconnect
reason was not captured; target-induced timeout is the supported explanation.

Profiling showed repeated Zstd decoding and Chunk verification with almost no
physical disk activity. The file reader retained the Exact generation present
when it was installed. Ordinary activation closed admission on that generation,
so later cold reads fell back to scanning Containers despite a healthy current
index. This could repeat for each Chunk reached during checkpoint rechunking.

## Reproduction and correction

`cargo test --locked -p fastdup-store --test manifest_reader
manifest_reads_follow_index_turnover_without_container_scans -- --nocapture`
was red before the correction: the same 4 KiB read performed zero full Container
reads before activation and nine after activation. With repository-bound readers
it performs zero before and zero after, including subsequent activations. The
fixture bypasses the verified read cache so warm reads cannot mask the defect.
A corrupted target Record remains an error after the transitions.

Appliance readers now atomically pin the repository's current Exact generation
for each bounded read. The explicit fixed-generation API retains its previous
behavior. No durable bytes or authority boundaries change. Dormant files do not
retain operation pins; a paused DATA-read regression proves a retired generation
cannot drain until the in-flight read finishes. The recovered POSIX-reader test
also activates a successor before its first read and rejects any full DATA scan.

Validation includes Manifest reads, Exact activation/repository tests, recovery,
checkpoint failure injection, write-through ingest, and library Clippy with
warnings denied. Local command logs and live profiling evidence are retained in
`.artifacts/veeam-live-performance/`. Package revision is 0.6.4-3.

The upgrade also reproduced a startup-lifecycle defect: the management agent
allowed only five seconds between starting the Type=simple repository unit and
activating shares. It stopped the daemon while recovery was still verifying the
existing pool. Repository startup now has a separate five-minute lifecycle
budget, matching the service's stop budget. Running SMB request and checkpoint
limits are unchanged.

## Installed SMB verification

A temporary encrypted SMB 3.1.1 share on the test VM accepted 5 GiB of random
content in 1 MiB write calls, followed by flush, close/reopen, and complete
SHA-256-checked readback. The client ran on the VM through Samba over loopback;
the operator connection is a VPN and is unsuitable for interpreting LAN
throughput. Write time was 82.726 seconds, readback 103.504 seconds, and the
slowest write call 0.025 seconds. The observed checkpoint sample contained 135
commits with maximum total wall time 0.516 seconds and maximum CDC-stage wall
time 0.000898 seconds. These are a different workload from the failing Veeam
job, not a direct throughput A/B. The complete Veeam job still needs a client
retry. The temporary account, share, and file were removed.

This run's readback SHA-256 was
`4b724d8a04f9b75869a88a65f65b8f58879d4a6c9bb22cbbf1055bd7c7356fdb`.
