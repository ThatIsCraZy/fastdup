---
status: accepted
---

# Mark Metadata from durable graphs and live root pins

Metadata GC marks every Namespace Root and Manifest node reachable from the
selected bounded Commit-Log segment plus every process-local Metadata Root Pin,
then removes only canonical `.fdm` objects outside that union. It deliberately
does not persist mutable reference counts: Metadata Objects are immutable and
content-addressed, Commit-Log rotation defines durable historical liveness, and
long-lived readers or unpublished successor proofs supply the additional online
roots that durable history cannot describe.

A Metadata publication holds a shared barrier until its complete child-first
object batch has a root pin. Collection holds the exclusive side of that
barrier and the Generation commit lock across mark, candidate verification,
unlink, and Metadata-directory sync. Consequently GC cannot observe a partial
Manifest publication, race a Namespace commit, or invalidate an open orphan.
Every candidate's canonical name, length, envelope, checksum, and Object ID are
verified before the first unlink. A crash before the final directory sync may
retain garbage; a crash after it observes the complete removal batch. Neither
case changes the selected recovery graph.

The first collector performed an exact mark and materialized directory
inventory for each admitted maintenance quantum. ADR 0067 adds immutable mark
catalog generations, incrementally invalidated clean-state reuse, and streaming
directory enumeration. These mechanisms may suppress or defer work but never
become deletion authority.
