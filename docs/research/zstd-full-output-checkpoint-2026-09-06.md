# Zstd trial rejection at a Chunk boundary

The Veeam retry on package 0.6.4-3 reached checkpoint generation 315 normally:
its final three completed checkpoints took 0.790, 0.512, and 0.817 seconds.
At 20:00:49 the next checkpoint began repeatedly returning
`Store(Format(ZstdFailure))`. Mutation admission correctly stayed closed, so
SMB writes stopped progressing. This differs from the earlier retired-index
Container-scan stall corrected in package revision 3.

## Live evidence and deterministic reproduction

A bounded debugger capture of the failing streaming-encoder branch found:

- current Chunk input length: 2,813 bytes;
- current Chunk input position: zero;
- destination position and capacity: both 257,088 bytes;
- the codec call returned no error, but made no progress.

The preceding Chunk had consumed its complete input while filling the output.
The next Chunk then entered Zstd with no destination room. Our no-progress
check wrongly classified this expected bounded-trial rejection as a fatal
codec failure. Index selection, stored-data verification, and the VPN were not
the cause of this failure.

The public adaptive writer reproduces the defect using deterministic xorshift
bytes: a 264,957-byte region split into 8,192-byte Chunks ends with a 2,813-byte
tail. The following test failed with `ZstdFailure` before the fix in 0.02 seconds:

```sh
cargo test --locked -p fastdup-format --test fragmented_zstd_rejection -- --nocapture
```

The regression also exercises several other region/Chunk sizes, verifies every
recovered Chunk against its original bytes, and follows the rejected trials
with a successful compressible frame on the same worker.

## Correction and validation

Check output capacity before each streaming call, including the first call for
a new Chunk. A full output selects the existing RAW fallback; genuine codec
errors and no progress with output room remain errors. The existing context
reset precedes the next trial. No durable format, reduction threshold, or
verification policy changes.

Format tests cover bounded rejection, gate behavior, borrowed fragmented input,
byte-exact Container decoding, and corruption detection. Appliance tests cover
checkpoint failures, write-through ingest, and recovery. Library Clippy uses
warnings-as-errors. Command logs and the debugger capture remain under
`.artifacts/zstd-checkpoint/`; no captured backup content is included here.
Package revision is 0.6.4-4.

## Installed verification

The test VM was upgraded to 0.6.4-4 and successfully verified/mounted its last
durable repository after the failed checkpoint. A loopback SMB 3.1.1 test then
wrote 2 GiB using mixed random bytes, repeated content, localized changes,
zero ranges, and irregular write sizes including 264,957-byte and 2,813-byte
fragments. Periodic flushes exercised checkpoint publication. Complete readback
matched SHA-256
`bc695a2e0001e2ab8c46e497ee4008617d527dd2496bce689116e96138251061`.
Advanced reduction exercised both Prefix and Sparse-XOR. The captured sample
of 87 completed checkpoints had maximum wall time 0.114 seconds and no critical
error. The temporary share, account, and file were removed. This validates the
installed SMB path; the complete Veeam job still requires another client run.
