moss design ideas & findings
============================

A running log of performance / design ideas explored for moss, and what
measurement actually showed.  The intent is measure-first: prototype in
a self-contained benchmark spike, quantify the win (or lack of one), and
record the result here before committing to an invasive change.  New
ideas and findings get appended over time.

Commit hashes below refer to the spike-2026-refresh line; the adaptive
prototype itself lives on the separate spike-2026-adaptive branch (it
was backtracked off spike-2026-refresh pending a decision).


Segment point-lookup search performance
=======================================

Background
----------

A CPU profile of a point Get on a large (1M-entry) segment shows ~84% of
the time in the binary search (segment.findKeyPos), and it is
memory-bound: each probe reads the entry array (kvs) AND chases into the
scattered key bytes (buf), so latency grows super-linearly with segment
size as the data outgrows CPU cache (measured ~11 ns/probe at 1k entries
vs ~42 ns/probe at 1M).

Sparse in-memory key index -- SHIPPED (commit 1b53556)
------------------------------------------------------

moss already had a sampled/sparse key index (segment_index.go,
segmentKeysIndex): every hop'th key copied into a small contiguous array
that is binary-searched first to narrow the main search to a small
window.  It was only built for large PERSISTED segments (> 10 MB of
keys).  Now it is also auto-built for large IN-MEMORY segments at their
sort/merge finalization points, with the budget sized proportionally to
the segment (target hop ~32, capped at 8 MB) so the window stays small
as the segment grows.  Measured ~13-14% faster point Get at 1M entries,
no format change, no-op for small segments.

Search/layout spike -- measured alternatives (commit 2051709)
-------------------------------------------------------------

See spike_search_bench_test.go.  Over 1M x 24B keys (M2 Pro, go1.25),
point-lookup ns/op:

    layout                         seq   rand
    baseline (offset+key buf)      606    657
    contiguous fixed keys          580    448   (kills the indirection)
    blocked + front-coded          602    607   (~neutral)
    eytzinger (BFS probe order)    521    476

Takeaways: the offset->buf double indirection is real (contig is -32% on
random keys); eytzinger is fastest for point lookups but breaks in-order
iteration and front-coding (would need a separate side index).

Blocked + front-coded segment -- SPIKED, NOT adopted (commit 405e605)
---------------------------------------------------------------------

See spike_bfc_bench_test.go.  RocksDB-style data blocks: front-coded keys
(shared-prefix-length + suffix) with a block index and intra-block linear
scan.  Measured: it is a SPACE optimization, not a speed one -- ~tie to
slower on Get and ~2x slower on full scan, because the front-code
decode/reconstruction CPU is paid on every in-memory/mmap access.  The
big space win (up to 5x) shows only for key-heavy + prefix-heavy data;
with 100B values it is 10-23%.  Why it does not port cleanly: RocksDB's
block format wins by shrinking DISK and decoding once into a block
cache, whereas moss accesses segments directly (mmap), so there is no
compression/IO benefit to amortize the decode against.  Do NOT adopt as
the default format.

Adaptive per-segment entry width -- PROTOTYPED (branch spike-2026-adaptive)
--------------------------------------------------------------------------

The most promising format idea.  At finalization a segment knows its
max keyLen / valLen / buf offset, so it packs each entry at the minimal
FIXED byte width for that segment: [opCode:1][keyLen:kw][valLen:vw]
[bufOffset:ow], e.g. ~7 bytes vs the basic 16 bytes for typical small
keys/vals.  Fixed-width-per-segment preserves O(1) random access and
mmap zero-copy -- the property front-coding sacrifices.

Prototype (commits edfc148, 4f7353e, 3a5182a on branch spike-2026-adaptive):
an opt-in in-memory adaptiveSegment (CollectionOptions.UseAdaptiveSegments)
produced at merge finalization, implementing the full Segment interface
plus a branch-free decoder specialized for the common (1,1,4) width combo
(a generic per-probe width switch erodes the win), and persisting back as
the normal basic on-disk format (file format / compaction / mmap
untouched).

Findings (1M x 24B keys, M2 Pro):
  * In-memory Get: adaptive (no index) 660/679 ns (seq/rand) ~= moss WITH
    the sparse index (676/654) and beats moss without it (877/717).  So
    adaptive delivers the index's speed at a SMALLER footprint (slimmer
    entry array AND no separate index).  End-to-end collection Get was
    neutral-to-slightly-faster.
  * On-disk (projected, reopened mmap store): key-heavy (val=0) .moss
    38MB -> ~29MB (~22% smaller) and store search 705 -> 569 ns (-19%);
    value-heavy (val=200) ~4% smaller, ~0 read speedup (per-probe buf
    cache-misses dominate the read and values dominate the file).
  * Rough break-even: worthwhile when value size <= key + entry metadata.

Status / next steps:
  * Kept opt-in and OFF this branch pending a decision.
  * Combine adaptive width WITH the sparse index to stack both wins
    (parity -> a clear speed win), rather than either/or.
  * An on-disk adaptive SegmentKind is worth it for KEY-HEAVY / small-value
    stores only; it needs a new SegmentKind, an adaptive compactWriter,
    and width fields in SegmentLoc (SegmentLoc.TotOps hardcodes
    KvsBytes/8/2).  Back-compat is not required for a major version.

Other noted options
-------------------

  * Offset-array slimming (uint32 buf offset -> 12 B/entry): subsumed by
    adaptive width, which is more optimal and graceful.
  * Interpolation search: rejected -- O(N) worst case on arbitrary byte
    keys is too risky for a general store.


Read-path allocations
=====================

From the benchmark suite (benchmark_moss_test.go): in-memory point Get
(hit and miss) is 0 allocs/op (the read path returns value views, not
copies).  Store (mmap) Get is 1 alloc/op -- the value copy in
Footer.Get -- unless ReadOptions.NoCopyValue is set (which drops it to
0 allocs, ~16% faster).  Read-heavy disk callers that can honor
NoCopyValue should.  The merge-chain Get allocation is dominated by the
sample MergeOperatorStringAppend's O(n^2) string concatenation, not moss
core.


Future ideas (unmeasured)
=========================

  * Adaptive entry width + sparse index combined (see above).
  * On-disk adaptive segment format for key-heavy workloads (see above).
  * Lazy merge operator PartialMerge folding: the read path now resolves
    a merge chain with a single FullMerge over all collected operands
    (commit 00beacf); folding via PartialMerge could shrink long chains
    further, but changes results for operators where partial != full.
  * Thread context.Context all the way into the lower-level (disk) read
    during a Get, not just at method entry.
  * See also IDEAS.md for older, longer-horizon ideas (incremental
    compaction, checksums, columnar side-structures, compression, etc.).
