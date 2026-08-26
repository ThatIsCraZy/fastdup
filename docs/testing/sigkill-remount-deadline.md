# SIGKILL, remount, and durability-deadline harness

Status: real-process K-level tracer implemented and green on 2026-08-21.

This harness tests the accepted ten-second durability contract through the
public process and POSIX boundary. It launches `fastdup-durable-fuse`, writes
four individually framed 64-KiB records, verifies live visibility through an
independent read, sends the daemon a real `SIGKILL`, detaches the dead FUSE
mount, and launches a new daemon over the same metadata and data roots.

The recovery oracle accepts only a byte-exact prefix ending at a complete
record boundary. It rejects mixed records, unacknowledged suffixes, and any
non-prefix state. For each record it measures the interval from the successful
`write(2)` reply to `SIGKILL`; every record whose interval reached ten seconds
must be present after remount. `fsync(2)` is issued before the live read but is
not treated as a stronger crash boundary, as required by
[ADR 0003](../adr/0003-fsync-does-not-strengthen-durability.md).

The standard v1 offsets are 0, 750, 2,250, 4,750, 5,250, 9,500, and 11,000
milliseconds after the final acknowledgement. These offsets straddle the
externally observable Container coalescing, commit-target, admission-guard,
and durability-window boundaries without reaching into checkpoint internals.
Every case uses a fresh repository.

## Reproduction

All output roots must be new directories below `.artifacts`; the harness keeps
them and both daemon logs as diagnostic evidence.

```bash
export RUSTUP_HOME=/source/fastdup/.artifacts/rustup
export CARGO_HOME=/source/fastdup/.artifacts/cargo
export CARGO_TARGET_DIR=/source/fastdup/.artifacts/target
export TMPDIR=/source/fastdup/.artifacts/tmp
export PATH="$CARGO_HOME/bin:$PATH"

cargo build -p fastdup-appliance --bin fastdup-durable-fuse
cargo run -p fastdup-testkit --example sigkill_remount_deadline -- \
  /source/fastdup/.artifacts/target/debug/fastdup-durable-fuse \
  /source/fastdup/.artifacts/sigkill-manual-UNIQUE
```

The ignored integration tracer uses the same public harness:

```bash
FASTDUP_DAEMON_BIN=/source/fastdup/.artifacts/target/debug/fastdup-durable-fuse \
FASTDUP_SIGKILL_RUN_ROOT=/source/fastdup/.artifacts/sigkill-test-UNIQUE \
cargo test -p fastdup-testkit --test sigkill_remount_deadline -- \
  --ignored --exact --nocapture
```

The run root must not already exist; this prevents accidental reuse or
overwriting of evidence.

## Observed result

The 2026-08-21 run used
`.artifacts/sigkill-harness.buAXlp/run`. All four writes were acknowledged in
every case. Times below are measured acknowledgement-to-kill intervals, not
requested sleep durations.

| Case | Kill interval | Recovered records | Required by deadline | Result |
| ---: | ---: | ---: | ---: | --- |
| 0 | 5 ms | 0 | 0 | valid old prefix |
| 1 | 750 ms | 0 | 0 | valid old prefix |
| 2 | 2,250 ms | 4 | 0 | valid new prefix |
| 3 | 4,750 ms | 4 | 0 | valid new prefix |
| 4 | 5,250 ms | 4 | 0 | valid new prefix |
| 5 | 9,500 ms | 4 | 0 | valid new prefix |
| 6 | 11,000 ms | 4 | 4 | deadline satisfied |

The standalone run and the independently activated integration test both
passed. No daemon or FUSE mount remained after either run.

## Scope and remaining gates

This is real daemon-crash and remount evidence, not a block-device power-cut
model. It does not inject torn writes, lost device caches, transient I/O stalls,
or single-device loss. It also does not prove deadline behavior while the
system is under sustained memory, CPU, or storage pressure. Lazy unmount only
removes the dead daemon's mount attachment; it cannot flush daemon state.

The next deadline gates are deterministic fake-clock tests with deliberately
stalled storage, persisted unhealthy/admission state, randomized kill offsets
during long ingest, and block-device power-cut fault injection. The exclusive
Appliance Lease is implemented; a stable downgrade/format-epoch fence remains
required before production deployment.
