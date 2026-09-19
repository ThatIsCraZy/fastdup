---
status: accepted
---

# Charge the pending-region gate only for resident Lane payload

The pending-region gate is an ownership ledger: its Lane charge must equal bytes
currently retained by live Ingest Lanes, while transferred Drain Residue is
charged to the residue owner. Two detach paths violated that invariant:
evicting a Lane dropped its buffers without releasing their charge, and the
commit-cut drain published Lane bytes outside normal batch settlement.

Lane acquisition now returns any evicted stream so its retained bytes are
released outside the Registry lock. Commit-cut draining measures the Lane region
before and after extraction, releases bytes that left, and excludes bytes whose
charge transferred to Drain Residue. Absorption or Drop later settles the
residue charge.

The invariant test exercises both publication drain and Registry eviction and
fails when either release is removed. Runtime status exposes Lane charge,
residue charge, and gate size so ownership drift is observable. On the test
appliance the fix reduced a leaked 2,260 MiB charge to roughly 1.4 MiB and
stopped throughput from degrading across identical copies.
