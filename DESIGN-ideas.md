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


Child collection bugs (found 2026)
==================================

An adversarial test pass over the child-collections feature (see
child_collection_more_test.go, child_collection_store_test.go) plus two
code reviews found the following.  The in-memory and persisted
delete/recreate-across-batches paths, isolation, nesting, untouched-child
survival, iteration, and compaction survival were all verified CORRECT
and are covered by passing tests.  The bugs below are pre-existing (not
introduced by the spike-2026 work).

FIXED (commit 961eafb, "ref-count fix cluster") -- items 3, 4, and 8
below shared the asymmetric child-Footer ref-count model and were fixed
together in a dedicated pass: Footer.DecRef now recurses into ChildFooters
(item 4 leak), ScanFooter initChildRefs()'s reopened child footers to
refs=1 (item 3 read-after-reopen data loss), and revertToSnapshot
preserves child incarNum (item 8). Because DecRef now writes
f.ChildFooters at end-of-life, ChildCollectionSnapshot / ChildCollectionNames
were changed to take f.m -- the formerly lockless map reads were only
safe while the field was immutable, and -race flagged the write/read data
race. Regression tests: TestChildFooterCloseReleasesChildren,
TestChildFooterReadTwice.  Items 3/4/8 are kept below (tagged [FIXED])
for the historical record and the numbering the write-up references.

CONFIRMED (have failing/pending tests):

  1. [FIXED e072ae0] Same-batch delete+recreate collides (in-memory).
     DelChildCollection(name) and NewChildCollectionBatch(name) both write
     b.childBatches[name] (one slot per name), so within ONE batch only
     the last wins: Del-then-New leaks the prior incarnation's keys
     (delete lost, new batch merges onto old data, incarNum not bumped);
     New-then-Del silently loses the new data. Cross-batch delete+recreate
     is fine. Fix idea: represent "deleted-then-recreated" distinctly
     (e.g. mark the recreated child batch as replacing, so buildStackDirtyTop
     bumps incarNum and starts fresh). Test: TestChildSameBatchDelRecreatePending.

  2. [FIXED e15a8c5] Child-collection ops not counted in dirty accounting.
     segmentStack.Stats() ignored childSegStacks, so a child-only write
     left CurDirtyOps/CurDirtyBytes at 0 (broke waitForPersistence + the
     MaxDirtyOps/MaxDirtyKeyValBytes back-pressure). Stats() now recurses.
     The naive one-line fix HUNG because two other work-detection sites
     used len(stack.a) instead of the recursive isEmpty() (mergerWaitForWork
     parked the merger; the persister's idle nudge skipped a child-only
     stackDirtyMid) -- both now use isEmpty(). That also surfaced a
     pre-existing child-only-STORE persist bug: startOrReuseFile located the
     file via top-level SegmentLocs[0] (empty for a child-only store), so it
     started a new file each persist -> "doLoadSegments fref mismatch"
     retried forever; new Footer.anyFileRef searches the footer tree. Tests:
     TestChildDirtyAccounting, TestChildStoreDirtyAccountingPersists.

  3. [FIXED 961eafb] Raw Footer child read-after-reopen loses data.
     Child Footers loaded from disk are JSON-unmarshaled with refs==0 (vs
     refs=1 on the fresh-persist path), and Footer.DecRef/AddRef never
     recurse into ChildFooters. Via the raw *Store/*Footer API, the first
     ChildCollectionSnapshot+Close drives a reloaded child footer to 0 and
     frees it (ss=nil, mmap unmapped); a second read returns empty (silent
     data loss, potential use-after-unmap). The Collection API path is
     unaffected (verified). Test: TestChildFooterReadTwicePending.

FLAGGED BY REVIEW (not yet independently reproduced with a test):

  4. [FIXED 961eafb] Leak: because Footer.AddRef/DecRef don't recurse into ChildFooters
     (and doLoadSegments AddRef's each child sloc), child mmaps/FileRefs
     are never released and superseded data files are never deleted after
     compaction (disk grows unbounded); accumulates per persist. Same root
     cause as #3.
  5. [FIXED 3874bed] store.Persist(nil, CompactionForce) (idle/full
     compaction with higher==nil) rebuilt a footer from footer.ss (which
     carries no child segStacks) -> dropped all child collections. Fixed
     via Footer.ssWithChildren(). Reachable via the exported API; not hit
     by the normal collection persister (always passes a non-nil higher).
  6. [FIXED 3874bed] Partial/leveled compaction applied the top-level
     splicePoint index to child footers (which have independent, usually
     smaller segment counts) -> out-of-range panic or mis-split
     (MB-29664-adjacent). Fixed: children are always fully compacted
     (splicePoint 0) in mergeSegStacks/spliceFooter. Also fixed an
     entangled incarNum defect -- mergeSegStacks compared the child
     footer's incarNum to the PARENT stack's (always unequal), dropping
     persisted child segments on EVERY compaction; now child-to-child, and
     writeSegments preserves incarNum so later compactions don't re-drop.
     Tests: TestChildStorePersistNilKeepsChildren,
     TestChildStoreCompactionKeepsAllData, TestChildStorePartialCompaction.
  7. [FIXED 4fe153e] segmentStack.decRef/Close didn't recurse into
     childSegStacks -> leaked child lower-level (mmap/File) handles once a
     store is attached (superseded data files never deleted); benign
     (GC-reclaimed) for pure in-memory. decRef now releases the parent's
     owned ref on each child recursively (same shape as Footer.DecRef).
     Test: TestChildSnapshotCloseReleasesChildLowerLevels.
  8. [FIXED 961eafb] store_revert.go builds reverted child footers with incarNum==0, so a
     later buildNewFooter/mergeSegStacks incarNum comparison spuriously
     drops the reverted child's segments; plus an error-path child leak.

Common theme: the child-Footer ref-count model is asymmetric (parent
AddRef/DecRef ignore ChildFooters; the two child-footer creation paths
disagree on initial refs). #3+#4+#8 share that root cause and should be
fixed together, carefully, as a dedicated pass.

STATUS (2026 spike-2026-refresh): ALL of the above are now fixed --
#1 (0cf49b5), #2 (e15a8c5), #3/#4/#8 (961eafb), #5/#6 + an entangled
compaction incarNum defect (3874bed), #7 (4fe153e).  Recurring root cause
across #3/#4/#7/#8: parent ref-count / release operations (Footer and
segmentStack) did not recurse into children; the child + Footer levels now
both release owned child refs recursively.
