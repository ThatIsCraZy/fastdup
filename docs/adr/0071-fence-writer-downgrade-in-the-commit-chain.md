---
status: accepted
---

# Fence writer downgrade in the authoritative Commit chain

Every new Commit Record carries a monotonic Repository Format Epoch. Epoch zero
is the legacy v1 record; any nonzero epoch uses Commit Record format v2 and
stores the epoch in the former reserved field at offset 22. The current writer
reads epochs zero and one and writes only epoch one. Append, recovery, and
offline Scrub validate every retained record before graph fallback, reject an
unsupported or decreasing epoch, and never use an older Namespace generation
to hide a newer writer epoch.

The Commit chain is the downgrade fence because it is already the authoritative
mutation boundary and older binaries reject the v2 record structurally. A
separate marker file was rejected: binaries predating the marker would ignore
it and could continue writing after an upgrade. A new writer may read an
epoch-zero repository, but it must durably append an epoch-one Commit before it
can publish any feature that depends on epoch one. A crash during that append
therefore exposes either the complete legacy head or the complete fenced head;
only the latter authorizes newer writer behavior.

Policy Set and Repository Format Epoch remain distinct. A Policy Set identifies
chunking, encoding, placement, and maintenance choices for one generation; the
epoch fences repository-wide reader/writer compatibility. Object-local versions
still determine how individual immutable bytes decode. Under ADR 0074 the
pre-production writer accepts only the one current Policy Set from the first
Commit; this does not remove the separate epoch-zero reader needed by the
Format-Epoch fault and downgrade boundary.
