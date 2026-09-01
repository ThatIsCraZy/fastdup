# Pool identity v1

Each Metadata and Data Pool contains one immutable canonical
`.fastdup-pool.identity` object. The two records bind distinct physical Pools
to one Appliance without assigning identity to a mount path, device name, or
argument position.

## Record layout

The record is exactly 4096 bytes and all integers are little-endian.

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `FDPOOL01` |
| 8 | 2 | format version `1` |
| 10 | 2 | header bytes `64` |
| 12 | 2 | record type `1` |
| 14 | 2 | Pool Role: Metadata `1`, Data `2` |
| 16 | 4 | record bytes `4096` |
| 20 | 2 | CRC32C algorithm `1` |
| 22 | 2 | reserved zero |
| 24 | 16 | nonzero Appliance ID |
| 40 | 16 | nonzero Pool ID |
| 56 | 4 | CRC32C of the complete record with this field zeroed |
| 60 | 4036 | reserved zero |

Readers reject unknown versions, lengths, roles, algorithms, nonzero reserved
bytes, zero IDs, and checksum mismatch. There is no legacy decoder or migration
path.

## Pair invariant

The Metadata and Data records must have the same Appliance ID, different Pool
IDs, and exactly the roles required by the opening boundary. Writable startup
and offline Scrub validate this pair before repository recovery or maintenance.
A canonical identity entry must be a regular filesystem object; symlinks and
other object types fail closed.

## First initialization

Initialization is allowed only while an unidentified Pool contains bootstrap
objects: the Metadata Pool may contain the Appliance Lease, Recovery Latch, and
identity temporary; the Data Pool may contain only the identity temporary. Any
other object makes a missing identity an unsupported prototype repository.

Each record is encoded completely into `.fastdup-pool.identity.tmp`, fixed to
4096 bytes, file-synchronized, published without replacement, and followed by
a root-directory synchronization. The published bytes are read and verified
before initialization succeeds. Metadata is published first. If interruption
leaves only that record, restart generates a new distinct Data Pool ID but
copies the already durable Appliance ID. The symmetric partial state is also
accepted so recovery never relies on publication order.
