# Large public corpora for Exact and Similarity reduction

Date: 2026-09-02

## Conclusion

A useful Similarity benchmark is a version family, not one isolated large
file. The family must be ingested oldest to newest into one initially empty
repository. Compressed and unpacked representations should be reported
separately because compression can hide byte-level similarity.

## Recommended families

### Linux kernel patch releases: best small first experiment

Download adjacent 6.12 patch releases, for example
[`linux-6.12.20.tar.xz`](https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.12.20.tar.xz)
and
[`linux-6.12.21.tar.xz`](https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.12.21.tar.xz).
The official v6.x index reports 148,029,712 and 148,042,304 bytes. Run the
pair once as downloaded and once after expanding each file to its `.tar` form.
The uncompressed TAR pair combines mostly stable source content with local
edits and archive-header changes, making it useful for SeqCDC, Sparse-XOR, and
ZSTD_PREFIX. The `.xz` pair is the matching precompression negative control.

Kernel.org publishes a
[`sha256sums.asc`](https://cdn.kernel.org/pub/linux/kernel/v6.x/sha256sums.asc)
file and detached release signatures. Its
[signature documentation](https://www.kernel.org/signature.html) explains
that the release signature covers the uncompressed TAR stream.

### Ubuntu Cloud daily images: realistic VM-version family

The official Ubuntu Noble daily archive retains dated builds. For example,
the AMD64 QCOW2 image is 596 MB in both the
[`20260814`](https://cloud-images.ubuntu.com/noble/20260814/) and
[`20260826`](https://cloud-images.ubuntu.com/noble/20260826/) directories.
Both directories publish `SHA256SUMS` and its GPG signature.

Test the downloaded QCOW2 files as application-visible backup objects, then
convert each to RAW and repeat. RAW exposes fixed-offset guest filesystem
changes that are favorable to Sparse-XOR; QCOW2 tests cluster allocation and
metadata movement. Report sparse holes, FILL bytes, and dependent-codec bytes
separately so zero ranges do not masquerade as Similarity gain.

### Wikimedia monthly XML dumps: growing structured stream

For a bounded smoke test, use the same English-Wikipedia page-ID partition in
two monthly snapshots:

- [`20260701`, part 1](https://dumps.wikimedia.org/enwiki/20260701/enwiki-20260701-pages-articles-multistream1.xml-p1p41242.bz2), 298.4 MB;
- [`20260801`, part 1](https://dumps.wikimedia.org/enwiki/20260801/enwiki-20260801-pages-articles-multistream1.xml-p1p41242.bz2), 299.1 MB.

The [July status page](https://dumps.wikimedia.org/enwiki/20260701/) and
[August status page](https://dumps.wikimedia.org/enwiki/20260801/) publish the
complete file lists and checksum manifests. For a larger run, the combined
multistream files are 26.56 GB and 26.67 GB respectively. Test the Bzip2 files
as a compressed control and decompressed XML as the main CDC/Similarity
workload. Wikimedia's
[dump copyright page](https://dumps.wikimedia.org/legal.html) describes the
applicable project licenses and exceptions.

### Rocky release media: large conservative real-world pair

Rocky Linux 9.6 and 9.7 DVD images are 12.85 GB and 13.36 GB in the official
[`9.6` vault index](https://download.rockylinux.org/vault/rocky/9.6/isos/x86_64/)
and
[`9.7` vault index](https://download.rockylinux.org/vault/rocky/9.7/isos/x86_64/).
The images contain many unchanged package blobs but rebuilt ISO metadata and
changed package sets. That makes the pair a large, conservative test of exact
reuse and CDC resynchronization. Each index provides checksum files.

For a smaller VM-image variant, use the official Rocky 9.6 and 9.7
GenericCloud QCOW2 images from their
[`9.6` image index](https://download.rockylinux.org/vault/rocky/9.6/images/x86_64/)
and
[`9.7` image index](https://download.rockylinux.org/vault/rocky/9.7/images/x86_64/).
The listed images are about 628 MB and 649 MB.

## Controls and interpretation

- Exact positive control: ingest the same pinned file twice under different
  names. The second copy should contribute almost no new payload bytes.
- Similarity positive control: retain fastdup's existing deterministic
  minimally changed Rocky-ISO variants. They isolate sparse in-place edits but
  are synthetic and should not be the only evidence.
- Negative control: ingest unrelated Rocky, Wikimedia, and kernel objects in
  one run. Candidates may be proposed, but dependent encodings should not be
  accepted unless they beat the independent representation under the normal
  policy.
- Precompression control: compare `.tar.xz` with `.tar`, `.xml.bz2` with XML,
  and QCOW2 with RAW. Do not combine those results into one ratio.
- Pin versioned URLs, exact byte lengths, and cryptographic hashes in the
  corpus manifest. Never benchmark a moving `latest` URL without recording the
  resolved content identity.
- Ingest versions in chronological order. Similarity requires an earlier
  independently decodable Base Chunk; reversing or randomizing the order
  changes the workload.

The decision metric is the additional allocated DATA plus Metadata reduction
relative to the same ingest with Advanced Reduction disabled. Candidate counts
alone are not evidence. Also record accepted Sparse-XOR and ZSTD_PREFIX logical
and payload bytes, restore throughput and latency, maximum dependency depth,
and byte-exact restore/scrub results.

For multi-gigabyte inputs use the streaming appliance/FUSE path. The
`reduction_matrix` reference harness reads each complete input and restored
object into memory and reports payload-only bytes, so it is suitable for
bounded slices and codec attribution, not whole-repository capacity claims.
