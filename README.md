moss stands for "memory-oriented sorted segments", and provides a pure
golang data structure that manages an ordered Collection of key-val
entries.

The design is similar to a (much) simplified LSM tree, in that there
is a stack of sorted key-val arrays or "segments".  To incorporate the
next Batch (see: ExecuteBatch()), we sort the incoming Batch of
key-val mutations into a "segment" and atomically push the new segment
onto the stack.  A higher segment in the stack will shadow entries of
the same key from lower segments.

Separately, an asynchronous goroutine (the "merger") will continuously
merge N sorted segments into a single sorted segment to keep stack
height low.  After you stop mutations, that is, the stack will
eventually be merged down into a stack of height 1.

The remaining, single, large sorted segment will be efficient in
memory usage and efficient for binary search and range iteration.

Another asynchronous goroutine (the "persister") can optionally
persist the most recent work of the merger to outside storage.

Iterations when the stack height is > 1 are implementing using a
simple N-way heap merge.

In this design, stacks are treated as immutable via a copy-on-write
approach whenever a stack is "modified".  So, readers and writers
essentially don't block each other, and taking a Snapshot is also a
similarly cheap operation by cloning a stack.

LICENSE: Apache 2.0
