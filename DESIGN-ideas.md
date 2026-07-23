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
  * See "Longer-horizon ideas" below for older ideas (incremental
    compaction, checksums, columnar side-structures, compression, etc.),
    merged in from the former IDEAS.md.


Secondary indexing use case (unmeasured proposals)
==================================================

One target use for moss is storing & maintaining secondary indexes (e.g.
bleve/scorch). A secondary index is almost always COMPOSITE keys mapping
to empty/tiny values -- (fieldValue, docId) -> nil -- and the workload is
range/prefix scans (equality = prefix scan, BETWEEN = range scan), high
churn (every doc update = delete-old-entry + insert-new-entry), whole-index
churn (reindex a field, drop an index), planner cardinality questions, and
a hard consistency requirement (the index must move atomically with the
base data).

Moss already covers the two hardest parts: atomic child-collection batches
(NewChildCollectionBatch, api.go) let a single ExecuteBatch update a doc and
all of its index entries atomically -- keep the doc store and its N indexes
as child collections under one top-level collection -- and MergeOperator +
LowerLevelUpdate give blind-write updates and disk chaining. The remaining
gaps are scan ergonomics, bulk mutation, and planner support. None of the
below is measured yet; they are recorded here as candidate spikes.

Highest-leverage (fit moss's LSM grain):

  * Range delete (DeleteRange tombstone). Reindexing a field or dropping a
    term's postings today means scan-then-Del of every key -- O(n) writes.
    A first-class range tombstone that is O(1) to write and resolved during
    merge/read (a'la RocksDB) is the biggest index-maintenance win. Fits as
    a new operation type alongside OperationSet/Del/Merge (api.go). One
    immutable tombstone entry instead of thousands of point deletes.
  * Approximate range count from the segment index. Planners need "roughly
    how many rows match x BETWEEN a AND b" WITHOUT scanning. The existing
    sampled segmentKeysIndex (segment_index.go) can yield a cheap cardinality
    estimate: Snapshot.CountRange(start, end) -> approx uint64. High value,
    low storage cost; enables cost-based scan selection.
  * Built-in merge operators for index maintenance. Ship reusable
    MergeOperators so callers stop doing read-compute-write: integer counters
    (per-term doc counts, facet counts) and posting-list set add/remove
    (roaring-bitmap merge). Turns "read posting list, add docId, write back"
    into a blind Merge, much cheaper under churn, composes with the atomic
    child batch.
  * Key-only / no-copy iteration. Index scans usually want just the key (the
    docId is IN the key), never the value. ReadOptions.NoCopyValue exists for
    Get (api.go) but there is no iterator equivalent. An IteratorOptions.KeyOnly
    that skips materializing/copying values would speed the dominant index
    operation and cut allocations.
  * Reverse iteration (Prev). Iterators are forward-only today (Next/SeekTo
    forward, api.go). Ranking queries constantly want ORDER BY x DESC /
    "top-N newest"; descending iteration avoids inverted-key encodings.

Ergonomic / library-level:

  * Composite-key + prefix helpers. Moss is deliberately just bytes; an
    OPTIONAL companion package for order-preserving tuple encoding (so
    (int, string, docId) sorts correctly) and a StartIterator(prefix)
    convenience that computes endKeyExclusive as the prefix successor.
    Keep out of core; removes the #1 footgun for index builders.
  * Value-less segment format. Empty-value entries are the common case for
    indexes; a segment format storing a single bit instead of a zero-length
    value region saves space and speeds scans. Dovetails with the adaptive
    per-segment entry-width work above (per-segment minimal encodings).
  * Snapshot-driven backfill pattern. Building an index over existing data
    while writes continue: LowerLevelUpdate (api.go) already gives the
    primitive; a documented "build index from Snapshot S, then catch up the
    delta" recipe (or thin API) would make online index builds a supported
    path.

Suggested first spikes: range-delete tombstones (biggest maintenance win,
clean LSM fit) and approximate range counts (unlocks a planner, cheap on the
existing segment index). Merge operators and key-only iteration are close
behind and comparatively easy.


Longer-horizon ideas (merged from IDEAS.md)
===========================================

Older, longer-horizon todo / future ideas, preserved from the former
IDEAS.md file:

  * plugin / extension APIs for adding "side data structures" like perfectly
    balanced b-trees to speed lookups
  * or, postings lists and columnar re-layouts of data
  * reductions for LSM storage
    (https://docs.google.com/document/d/1X9JtIud9an23d4VTxWLpIe4HxgD4NwAFprKQOQX6aDU/edit#heading=h.jj90qi7qbon1)
  * more stats
  * performance optimizations for handling time-series data, where top-level
    should be able to binary-search through non-overlapping, ordered segments,
    if each segment knows its start/end keys
  * hard crash testing (a'la sqlite or foundationdb)
  * related, use fake file interface implementation to test file corruptions
  * checksums
  * moss as general purpose k/v store...
    * API for synchronous storage
    * non-batch API
  * benchmarks against other KV stores
  * incremental compaction, as opposed to the existing full compaction
    * hole punching / punch-line algorithm?
    * plasma inspired multiple-files approach for logical "hole punching"
    * block reuse algorithm?
  * more concurrent writer goroutines to utilize more I/O bandwidth
  * faster Get()'s by explicitly caching top-level binary-search positions
  * optimization where each segment tracks min/max key (skip binary search if
    outside of range)
  * optimization where segmentStack knows if min/max keys of segments are
    non-overlapping and linear
    * support binary searching of segmentStack
    * as an optimization for time-series patterns
  * compression (key-prefix?)
  * callback API so apps can hook into compaction (e.g., for TTL expirations)
  * C-based version of moss?
    * might be named "mossc" (pronounced like "mossy" or "mosque")? or,
      perhaps cmoss ("sea moss" / "CMOS")?
  * Optimizations using posix_fadvise()
  * Optimizations using sync_file_range()
    * http://stackoverflow.com/questions/3755765/what-posix-fadvise-args-for-sequential-file-write
    * http://yoshinorimatsunobu.blogspot.com/2014/03/how-syncfilerange-really-works.html

Incremental compaction (handwave design)
-----------------------------------------

Some handwave thoughts about incremental compaction...

The mossStore append-only file approach is simple, robust, allows for fast
recovery, allows for partial rollbacks, and is relatively performant for
writes. Its main downsides are:

  * A: the mossStore file (data-*.moss) continually grows.
  * B: write amplification.
  * C: doesn't support concurrent mutations, but currently has just a single
    persister that performs either file mutation appends or full file
    compaction.

On Issue C, an application can shard data across multiple mossStore instances
to try to achieve higher I/O concurrency.

On Issue A, to avoid a forever growing file size, a full compaction can be
performed, which copies any live data to a brand new file. However, during a
full compaction, incoming mutations are not persisted and are instead queued,
and worst case up to 2x the disk space might be used to perform a full
compaction.

Incremental compaction might help solve issue (A) of file growth and stalled
mutation persistence while also perhaps helping with write amplification (B)
and with concurrency (C).

mossStore tracks a stack of immutable segments, where each segment is a
sorted array of key-val entries. So it's O(1) to retrieve the smallest key
and largest key of each segment. We represent the smallest and largest keys
of a segment like "[smallest, largest]".

A stack of 5 segments might look like...

  Diagram: 1

    Level | Key Range
        4 | [B, C] <-- most recent segment.
        3 | [D, I]
        2 | [F, H]
        1 | [E, G]
        0 | [A, J] <-- oldest segment.

Switching to a representation where the key ranges take horizontal space, the
range overlaps amongst those 5 segments becomes more apparent...

  Diagram: 2

    Level | Key Range
        4 |   [B-C]               <-- most recent segment.
        3 |       [D---------I]
        2 |           [F---H]
        1 |         [E---G]
        0 | [A-----------------J] <-- oldest segment.

Next, we can flatten the diagrammatic representation into a single row (e.g.,
sorted by key), and also incorporate the level information next to each key...

  Diagram: 3

    [A0   [B4   C4]   [D3   [E1   [F2   G1]   H2]   I3]   J0]

For lisp folks, that might look like a bunch of nested parens.

Next, we can calculate the depth (or nesting level) of sub-ranges between keys
by increasing a running depth counter when we see a '[' and decreasing the
depth counter when we see a ']'.

  Diagram: 4

             [A0   [B4   C4]   [D3   [E1   [F2   G1]   H2]   I3]   J0]
    depth: 0     1     2     1     2     3     4     3     2     1     0

We can easily find the sub-range with the largest depth, in this case, F to G
which has depth 4. That sub-range makes a promising candidate to incrementally
compact, based on the theory that higher depth not only slows down reads more,
but also has the most opportunity for compaction win (higher depth likely
means more potential for de-duplications of older mutations and removals of
deletion tombstones).

The compaction of range F to G means we'd have to split any intersecting
ranges (like range A0 to J0) into potentially 2 smaller ranges (sub-range A to
E and sub-range H to J), such as...

  Diagram: 5

    Level | Key Range
        7 |           [F-G]       <-- most recent segment.
        6 |   [B-C]
        5 |               [H-I]
        4 |       [D-E]
        3 |               [H]
        2 |         [E]
        1 |               [H---J]
        0 | [A-------E]           <-- oldest segment.

As you can see, [F-G] indeed got shorter depth, but the splitting introduced
even more levels to the left and right of [F-G].

So, as a next step, we need to consider heuristics in the algorithm that might
greedily expand the incremental compaction range, so that instead of
incrementally compacting just the range of F to G, perhaps the incremental
compaction should also take care of (for example) E and H at the same time, so
we end up instead with something like...

  Diagram: 6

    Level | Key Range
        5 |         [E-----H]
        4 |   [B-C]
        3 |                 [I]
        2 |       [D]
        1 |                 [I-J]
        0 | [A-----D]

In addition, other sub-ranges that don't overlap with E to H might be also
concurrently, incrementally compacted. For example, B to C...

  Diagram: 7

    Level | Key Range
        6 |   [B-C]
        5 |         [E-----H]
        4 |                 [I]
        3 |       [D]
        2 |                 [I-J]
        1 |       [D]
        0 | [A]

Next, imagine that sub-ranges D and I-to-J are concurrently, incrementally
compacted, leaving us with...

  Diagram: 8

    Level | Key Range
        4 |                 [I-J]
        3 |       [D]
        2 |   [B-C]
        1 |         [E-----H]
        0 | [A]

In this case, some "adjacent range merger" should notice that [B-C] and [D]
are adjacent and can be trivially merged, leaving us with....

  Diagram: 9

    Level | Key Range
        3 |   [B---D]
        2 |                 [I-J]
        1 |         [E-----H]
        0 | [A]


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
