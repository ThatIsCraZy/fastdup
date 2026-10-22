---
status: accepted
---

# Length-band buckets and flexible Prefix codec

Measured Veeam ingest left most Similarity queries without any candidate:
bucket keys and both dependent codecs required the Base Chunk to have the
exact target length, while changed SeqCDC regions are precisely where
chunk boundaries and lengths diverge. Two versioned changes widen the
candidate space without extra DATA reads.

Bucket profile v2 replaces the exact logical length in the bucket key with
the minimum of a geometric length band (relative width below one eighth;
key ordering stays monotonic in length). Entries keep their exact logical
length; validation at writer, recovery, and query boundaries checks band
membership under the Run's own profile. Profile-v1 families remain readable
and queryable: online generations union the newest complete bucket value
per profile, and compaction never merges across profiles. Object names
keep the v1 profile fields as a frozen naming scheme; the family manifest
and Run headers carry the authoritative bucket profile. A full pool-index
rebuild converts the candidate universe to v2 bands.

Codec 5 (`ZSTD_PREFIX_FLEX`) shares the codec-3 record layout, including
separate Base-length and target-length fields, but permits the Base logical
length to differ from the target. Writers emit codec 3 whenever lengths are
equal, so codec 5 records always carry differing lengths; structural
validation enforces this canonical selection at writer, reader/recovery,
and offline-scrub boundaries. Sparse-XOR stays equal-length. Depth remains
one, the 5%/4 KiB acceptance rule and the four-trial budget are unchanged,
and codec-3/4 records decode exactly as before. Binaries predating codec 5
reject the unknown codec and fail closed.

ADR 0106 replaces the length band for new families with bucket profile v3,
which gives every length one band. Codec 5 is unchanged.
