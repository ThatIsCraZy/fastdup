# Aligned repository storage v1

ADR 0046 introduces Repository Format Epoch two. This is a pre-stable format
break: rebuild existing repositories. The filesystem storage seam exposes the
same logical object bytes to field-by-field Container, Metadata, Index, WAL and
checkpoint decoders. The physical filesystem representation below that seam is
versioned separately and never uses Rust structure layout.

Generic files have 4096-byte blocks, two length heads at physical offsets 0 and
4096, and logical content starting at offset 8192. Physical EOF is always a
multiple of 4096. Supported Direct-I/O memory and offset alignments must be
nonzero powers of two no greater than 4096, reported by `statx(STATX_DIOALIGN)`;
the supported filesystem is XFS with 4096-byte blocks. Unsupported geometry fails closed.

Each head encodes:

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 8 | ASCII `FDIO0001` |
| 8 | 8 | Nonzero sequence, little-endian u64 |
| 16 | 8 | Logical length, little-endian u64 |
| 24 | 4 | CRC32C of the complete block with these four bytes zeroed |
| 28 | 4068 | Zero reserved bytes |

The reader validates both heads independently, including the CRC, reserved
bytes, sequence and `logical_length <= physical_length - 8192`. It selects the
higher valid sequence; equal sequences with unequal lengths are corruption.
One torn/invalid head leaves the valid predecessor usable. Two invalid heads
fail closed. Physical space beyond the selected logical end may contain a
partly submitted extension; it does not extend the logical object.

A separately recognizable native aligned Container is allowed without the
storage envelope. The owned io_uring publisher writes its pre-existing native
4096-byte Header/Footer format. If neither storage head decodes, the reader
requires a valid native Container Header and exact embedded physical file
length; normal Container verification still checks its Footer and records.
Native published Containers cannot be modified through generic logical writes.
An arbitrary unframed Metadata, WAL or control file is never a legacy fallback.

## Writer and synchronization

Creation writes two identical sequence-one, zero-length heads through Direct
I/O. Higher layers still synchronize the newly created file and its directory
before making it durable authority.

A bounded aligned writer uses initialized overallocated safe Rust byte buffers.
Partial edge blocks are independently read, preserved and rewritten; interior
blocks need no read-modify-write. Reads round their physical request outwards
and return only the requested logical range. Short I/O is retried only while
both the remaining buffer and offset remain aligned. Zero progress, unsupported
alignment and incomplete requested data are errors. There is no buffered retry.

Extending a logical file zeroes any hole, writes the payload, writes the alternate
length head with sequence plus one and synchronizes the body/head pair before
another update may reuse the predecessor's slot. This extra synchronization
prevents two unsynchronized updates from overwriting both previously durable
heads. It may strengthen when bytes become durable; it does not acknowledge a
Namespace Commit or replace upper-layer readback and file/directory sync.

Shrinking zeroes the partial logical tail through Direct I/O, publishes and
synchronizes the smaller logical length, then truncates physical EOF to
`8192 + round_up(logical_length, 4096)`. Thus XFS never receives an unaligned
physical truncate. A crash before truncation leaves harmless excess physical
space. Later extension zeroes previously hidden bytes. Equal-length overwrite
retains the upper layer's ordinary synchronization protocol and corruption
checks; the length heads do not promise atomic payload overwrites.

Mutation is serialized per name and invalidates retained handles. Immutable
leases reject writes, truncation and rename/unlink while their owners remain
active. Direct-I/O file identity and length-head cache entries are keyed by the
current file change stamp and a process-owned mutation revision. The revision
advances before and after each mutation, even when it fails or timestamps do
not change. Resident range/head views retain that revision's synchronization
identity. Independent reads bypass those entries too; mutable Reduction Heads
always bypass range/head reuse.

## Reader, recovery and scrub gates

All filesystem storage reads, object lengths and immutable leases use this
same envelope decoder. Recovery and offline/background scrub enter Independent
intent and therefore reread the physical heads and payload. Existing logical
format checks and corruption classification remain required after unwrapping.
Epoch validation rejects unsupported Commit epochs before graph fallback.
Two invalid physical length heads produce a storage `InvalidData` error;
ordinary recovery fails closed rather than treating that object as absent or
reinterpreting arbitrary bytes as a legacy file. A single valid head can still
expose its preceding durable logical prefix.

Fault qualification covers a payload extension before its head, a torn new
head, both invalid heads, equal-sequence forks, lengths beyond physical EOF,
nonzero reserved bytes even with a valid CRC, sequence zero, shrink/re-extend,
unaligned ranges and EOF. Warm-cache tests corrupt logical content and require
recovery/scrub to detect it. A separate `fincore` gate checks zero resident file
pages after tiny writes, shortening and reads without any eviction syscall.
The in-memory StorageIo fault model remains a logical storage model; it does
not model these physical envelope sectors, so physical tests are mandatory.
