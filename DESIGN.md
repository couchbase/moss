moss design overview
--------------------

moss as a in-memory KV collection and write-back cache
======================================================

moss originally began as a 100% go, in-memory, ordered, key-value
collection library with optional write-back caching.  Users can
optionally provide their own lower-level storage to which moss can
asynchronously persist batched-up mutations.

    +---------------------+
    | moss                |
    +---------------------+
    | lower-level storage |
    +---------------------+

    Figure 1.

That is, users can use moss as a write-back cache in-front of
forestdb, rocksdb, boltdb, etc.

Later on, the project added its own, optional, built-in lower-level
storage implementation, called "mossStore", which is an append-only
design that will be described in a later section.

To implement the cache, moss follows an LSM-array (Log Structured
Merge array) design, which allows mutations in the cache to be
readable, iteratable and snapshot'able.

When the user atomically commits a batch of key-val mutations into a
moss "collection", the batch of mutations is sorted by key, becoming
an immutable "segment" of key-val mutations.  The segment is then
pushed onto the end of a logical sequence of segments.  See:
collection.go / collection.ExecuteBatch().

The logical sequence of segments that is maintained by a collection
has different sections...

    +---------------------+
    | collection          |
    |                     |
    | * top               |
    | * mid               |
    | * base              |
    | * clean             |
    |                     |
    +---------------------+

    Figure 2.

The top, mid, base sections are considered dirty, containing segments
with mutations that haven't yet been written back to the optional
lower-level storage.  The clean section is considered non-dirty,
having already been persisted to the optional lower-level store.

A section is implemented as a "segmentStack".  See: segment_stack.go.

The top section is where incoming batches of mutations are pushed.

    +---------------------+
    | collection          |
    |                     |
    | * top               |
    |     segment-0       |
    | * mid               |
    | * base              |
    | * clean             |
    |                     |
    +---------------------+

    Figure 3.

Eventually, following the LSM based approach, the logical sequence of
segments becomes too long or too tall...

    +---------------------+
    | collection          |
    |                     |
    | * top               |
    |     segment-4       |
    |     segment-3       |
    |     segment-2       |
    |     segment-1       |
    |     segment-0       |
    | * mid               |
    | * base              |
    | * clean             |
    |                     |
    +---------------------+

    Figure 4.

So, the segments need to be merged together.  A background "merger"
goroutine handles this in a loop, by atomically moving segments from
the top section to the mid section.

    +---------------------+
    | collection          |
    |                     |
    | * top               |
    | * mid               |
    |     segment-4       |
    |     segment-3       |
    |     segment-2       |
    |     segment-1       |
    |     segment-0       |
    | * base              |
    | * clean             |
    |                     |
    +---------------------+

    Figure 5.

This opens up more logical, configuration-controlled space in the top
section for more incoming batches, such as segment-5...

    +---------------------+
    | collection          |
    |                     |
    | * top               |
    |     segment-5       |
    | * mid               |
    |     segment-4       |
    |     segment-3       |
    |     segment-2       |
    |     segment-1       |
    |     segment-0       |
    | * base              |
    | * clean             |
    |                     |
    +---------------------+

    Figure 6.

The merger then asynchronously merges the segments in the mid section
and atomically swaps in the shorter mid section when done, and
repeats.  See: segment_stack_merge.go.

    +---------------------+
    | collection          |
    |                     |
    | * top               |
    |     segment-5       |
    | * mid               |
    |     segment-0...4   |
    | * base              |
    | * clean             |
    |                     |
    +---------------------+

    Figure 7.

To support optional write-back persistence, a background "persister"
goroutine handles this in a loop, by atomically moving the mid section
into the base section....

    +---------------------+
    | collection          |
    |                     |
    | * top               |
    |     segment-5       |
    | * base              |
    | * mid               |
    |     segment-0...4   |
    | * clean             |
    |                     |
    +---------------------+

    Figure 8.

The persister then writes out all the mutations in the base section to
the lower-level storage.  When done with persistence, the persister
atomically clears out the base section or optionally moves it into the
clean section, and repeats.  See: persister.go.

    +---------------------+
    | collection          |
    |                     |
    | * top               |
    |     segment-5       |
    | * base              |
    | * mid               |
    | * clean             |
    |                     |
    +---------------------+
    | lower-level-storage |
    |                     |
    | mutations from...   |
    |     segment-0...4   |
    |                     |
    +---------------------+

    Figure 9.

A snapshot is implemented by copying all the segment pointers from all
the sections and returning a brand new, immutable segmentStack.  See:
collection.go / collection.Snapshot().

For example, if the snapshot was taken when the collection was in the
state represented by Figure 6, then the snapshot would look like...

    +---------------------+
    | segmentStack        |
    |                     |
    |     segment-5       |
    |     segment-4       |
    |     segment-3       |
    |     segment-2       |
    |     segment-1       |
    |     segment-0       |
    |                     |
    +---------------------+

    Figure 10.

The lower-level storage must also provide snapshot semantics for
snapshotting to work.

Per the LSM design approach, the key-val mutations from a more recent
or higher segment in a segmentStack shadow the key-val mutations from
an older or lower segment in a segmentStack.  See: segment_stack.go /
segmentStack.Get().

An iterator is implemented by performing a heap-merge from multiple
segments.  See: iterator.go.

mossStore
=========

One option for lower-level storage is the built-in storage
implementation called mossStore.  It follows an append-only design,
where mossStore appends new segments and a "footer" to the end of a
file, with page-aligned start offsets.  The footer contains metadata
such as the offsets and lengths of the segments written to the file.
See: store.go / SegmentLoc struct and pageAlign().

Once the file footer is appended, mossStore performs an mmap() to read
the just-appended segments.  See: store_footer.go /
Footer.loadSegments().

The already-written parts of the file are immutable and any mmap()'ed
regions are also read-only.

On a restart or reopen of a previous mossStore file, mossStore scans
the file backwards looking for the last good footer.  See:
store_footer.go / ScanFooter().

mossStore supports navigating to previous, read-only snapshots of the
database, by scanning the file for older footers.  See:
store_previous.go.

mossStore can revert to a previous snapshot of the database, by
copying an older footer and just appending it to the end of the file
as a brand-new footer.  See: store_revert.go.

Full compaction is supported by mossStore to handle the situation of
an ever-growing append-only file, where active data from file X is
copied to a file X+1, and then file X is deleted.  See:
store_compact.go.

To track file-related handle resources, mossStore uses ref-counting.
For file handles, see file.go.  For mmap()'ed regions, see mmap.go.

One implication of this mossStore design is that a file written on one
machine endian'ness cannot be opened on a machine with different
endian'ness, especially since mossStore has an optimization where
large arrays of uint64's are read/written from/to storage as large,
contiguous byte arrays.  See: slice_util.go.
