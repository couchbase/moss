moss
----

moss provides a simple, ordered key-value collection data structure as
a 100% golang library.

moss stands for "memory-oriented sorted segments"

Features
========

* ordered key-val collection API
* range iterators
* batched mutations
* snapshots for stability and isolation across multiple reads
* merge operations allow for support for write-heavy counters, bags, etc.
* concurrent readers and writers don't block each other

License
=======

Apache 2.0

Example
=======

    import github.com/couchbaselabs/moss

    c, err := moss.NewCollection(CollectionOptions{})

    batch, c := c.NewBatch(0, 0)
    batch.Set([]byte("car-0"), []byte("tesla"))
    batch.Set([]byte("car-1"), []byte("honda"))
    err = c.ExecuteBatch(batch)
    batch.Close()

    ss, err := c.Snapshot()
    val0, err := ss.Get([]byte("car-0") // val0 == []byte("tesla").
    valX, err := ss.Get([]byte("car-not-there") // valX == nil.
    ss.Close()

Design
======

The design is similar to a (much) simplified LSM tree, with a stack of
sorted key-val arrays or "segments".

To incorporate the next Batch of key-val mutations, the incoming
key-val entries are first sorted into a "segment", which is then
atomically pushed onto the top of the stack of segments.

For readers, a higher segment in the stack will shadow entries of the
same key from lower segments.

Separately, an asynchronous goroutine (the "merger") will continuously
merge N sorted segments to keep stack height low.

In the best case, a remaining, single, large sorted segment will be
efficient in memory usage and efficient for binary search and range
iteration.

Another asynchronous goroutine (the "persister") can optionally
persist the most recent work of the merger to outside storage.

Iterations when the stack height is > 1 are implementing using a
simple N-way heap merge.

In this design, the stack of segments is treated as immutable via a
copy-on-write approach whenever the stack needs to be "modified".  So,
multiple readers and writers won't block each other, and taking a
Snapshot is also a similarly cheap operation by cloning a stack.

Limitations
===========

Max key length is 2^24 (24 bits used to track key length).

Max val length is 2^28 (28 bits used to track val length).

Read performance is roughly O(log N) for key-value retrieval.

Write performance is roughly O(M log M), where M is the number of
mutations in a batch when invoking ExecuteBatch().

Those performance characterizations, however, don't account for
background, asynchronous processing for merging of segments and data
structure maintenance.
