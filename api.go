//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// Package moss stands for "memory-oriented sorted segments", and
// provides a data structure that manages an ordered Collection of
// key-val entries.
//
// The design is similar to a (much) simplified LSM tree, in that
// there is a stack of sorted key-val arrays or "segments".  To
// incorporate the next Batch (see: ExecuteBatch()), we sort the
// incoming Batch of key-val mutations into a "segment" and atomically
// push the new segment onto the stack.  A higher segment in the stack
// will shadow entries of the same key from lower segments.
//
// Separately, an asynchronous goroutine (the "merger") will
// continuously merge N sorted segments to keep stack height low.
//
// In the best case, a remaining, single, large sorted segment will be
// efficient in memory usage and efficient for binary search and range
// iteration.
//
// Iterations when the stack height is > 1 are implementing using a
// N-way heap merge.
//
// In this design, stacks are treated as immutable via a copy-on-write
// approach whenever a stack is "modified".  So, readers and writers
// essentially don't block each other, and taking a Snapshot is also a
// similarly cheap operation by cloning a stack.

package moss

import (
	"errors"
	"sync"
)

var ErrAllocTooLarge = errors.New("alloc-too-large")
var ErrIteratorDone = errors.New("iterator-done")
var ErrMergeOperatorNil = errors.New("merge-operator-nil")
var ErrMergeOperatorFullMergeFailed = errors.New("merge-operator-full-merge-failed")
var ErrUnimplemented = errors.New("unimplemented")

// A Collection represents an ordered mapping of key-val entries,
// where a Collection is snapshot'able and atomically updatable.
type Collection interface {
	// Start kicks off required background tasks.
	Start() error

	// Close synchronously stops background tasks and releases
	// resources.
	Close() error

	// Options returns the options currently being used.
	Options() CollectionOptions

	// Snapshot returns a stable Snapshot of the key-value entries.
	Snapshot() (Snapshot, error)

	// NewBatch returns a new Batch instance with preallocated
	// resources.  See the Batch.Alloc() method.
	NewBatch(totalOps, totalKeyValBytes int) (Batch, error)

	// ExecuteBatch atomically incorporates the provided Batch into
	// the Collection.  The Batch instance should be Close()'ed and
	// not reused after ExecuteBatch() returns.
	ExecuteBatch(b Batch, writeOptions WriteOptions) error
}

// CollectionOptions allows applications to specify config settings.
type CollectionOptions struct {
	// MergeOperator is an optional func provided by an application
	// that wants to use Batch.Merge()'ing.
	MergeOperator MergeOperator

	// DeferredSort allows ExecuteBatch() to operate more quickly by
	// deferring the sorting of an incoming batch until it is needed
	// by a reader.  The tradeoff is that later read operations can
	// take longer as the sorting is finally required.
	DeferredSort bool

	// MinMergePercentage allows the merger to avoid premature merging
	// of segments that are too small, where a segment X has to reach
	// a certain size percentage compared to the next lower segment
	// before segment X (and all segments above X) will be merged.
	MinMergePercentage float64

	// MaxStackDirtyTopHeight is the max height of the stack of
	// to-be-merged segments before blocking mutations to allow the
	// merger to catch up.
	MaxStackDirtyTopHeight int

	// LowerLevelInit is an optional Snapshot implementation that
	// initializes the lower-level storage of a Collection.  This
	// might be used, for example, for having a Collection be a
	// write-back cache in front of a persistent implementation.
	LowerLevelInit Snapshot

	// LowerLevelUpdate is an optional func that is invoked when the
	// lower-level storage should be updated.
	LowerLevelUpdate LowerLevelUpdate

	Debug int // Higher means more logging, when Log != nil.

	// Log is a callback invoked when the Collection needs to log a
	// debug message.  Optional, may be nil.
	Log func(format string, a ...interface{})

	// OnError is a callback invoked when the Collection encounters a
	// background error.  Optional, may be nil.
	OnError func(error)
}

// DefaultCollectionOptions are the default configuration options.
var DefaultCollectionOptions = CollectionOptions{
	MergeOperator:          nil,
	MinMergePercentage:     0.8,
	MaxStackDirtyTopHeight: 10,
	Debug: 0,
	Log:   nil,
}

// A Batch is a set of mutations that will be incorporated atomically
// into a Collection.
type Batch interface {
	// Close must be invoked to release resources.
	Close() error

	// Set creates or updates an key-val entry in the Collection.  The
	// key must be unique (not repeated) within the Batch.  Set()
	// copies the key and val bytes into the Batch, so the memory
	// bytes of the key and val may be reused by the caller.
	Set(key, val []byte) error

	// Del deletes a key-val entry from the Collection.  The key must
	// be unique (not repeated) within the Batch.  Del copies the key
	// bytes into the Batch, so the memory bytes of the key may be
	// reused by the caller.  Del() on a non-existent key results in a
	// nil error.
	Del(key []byte) error

	// Merge creates or updates a key-val entry in the Collection via
	// the MergeOperator defined in the CollectionOptions.  The key
	// must be unique (not repeated) within the Batch.  Merge() copies
	// the key and val bytes into the Batch, so the memory bytes of
	// the key and val may be reused by the caller.
	Merge(key, val []byte) error

	// ----------------------------------------------------

	// Alloc provides a slice of bytes "owned" by the Batch, to reduce
	// extra copying of memory.  See the Collection.NewBatch() method.
	Alloc(numBytes int) ([]byte, error)

	// AllocSet is like Set(), but the caller must provide []byte
	// parameters that came from Alloc().
	AllocSet(keyFromAlloc, valFromAlloc []byte) error

	// AllocDel is like Del(), but the caller must provide []byte
	// parameters that came from Alloc().
	AllocDel(keyFromAlloc []byte) error

	// AllocMerge is like Merge(), but the caller must provide []byte
	// parameters that came from Alloc().
	AllocMerge(keyFromAlloc, valFromAlloc []byte) error
}

// A Snapshot is a stable view of a Collection for readers, isolated
// from concurrent mutation activity.
type Snapshot interface {
	// Close must be invoked to release resources.
	Close() error

	// Get retrieves a val from the Snapshot, and will return nil val
	// if the entry does not exist in the Snapshot.
	Get(key []byte, readOptions ReadOptions) ([]byte, error)

	// StartIterator returns a new Iterator instance on this Snapshot.
	//
	// On success, the returned Iterator will be positioned so that
	// Iterator.Current() will either provide the first entry in the
	// range or ErrIteratorDone.
	//
	// A startKeyInclusive of nil means the logical "bottom-most"
	// possible key and an endKeyExclusive of nil means the logical
	// "top-most" possible key.
	StartIterator(startKeyInclusive, endKeyExclusive []byte,
		iteratorOptions IteratorOptions) (Iterator, error)
}

// An Iterator allows enumeration of key-val entries.
type Iterator interface {
	// Close must be invoked to release resources.
	Close() error

	// Next moves the Iterator to the next key-val entry and will
	// return ErrIteratorDone if the Iterator is done.
	Next() error

	// Current returns ErrIteratorDone if the iterator is done.
	// Otherwise, Current() returns the current key and val, which
	// should be treated as immutable or read-only.  The key and val
	// bytes will remain available until the next call to Next() or
	// Close().
	Current() (key, val []byte, err error)

	// CurrentEx is a more advanced form of Current() that returns
	// more metadata for each entry.  It is more useful when used with
	// IteratorOptions.IncludeDeletions of true.  It returns
	// ErrIteratorDone if the iterator is done.  Otherwise, the
	// current EntryEx, key, val are returned, which should be treated
	// as immutable or read-only.
	CurrentEx() (entryEx EntryEx, key, val []byte, err error)
}

// WriteOptions are provided to Collection.ExecuteBatch().
type WriteOptions struct {
}

// ReadOptions are provided to Snapshot.Get().
type ReadOptions struct {
}

// IteratorOptions are provided to StartIterator().
type IteratorOptions struct {
	// IncludeDeletions is an advanced flag that specifies that an
	// Iterator should include deletion operations in its enuemration.
	// See also the Iterator.CurrentEx() method.
	IncludeDeletions bool

	// SkipLowerLevel is an advanced flag that specifies that an
	// Iterator should not enumerate key-val entries from the
	// optional, chained, lower-level iterator.  See
	// CollectionOptions.LowerLevelInit/LowerLevelUpdate.
	SkipLowerLevel bool

	// MinSegmentLevel is an advanced parameter that specifies that an
	// Iterator should skip segments at a level less than
	// MinSegmentLevel.  MinSegmentLevel is 0-based level, like an
	// array index.
	MinSegmentLevel int

	// MaxSegmentHeight is an advanced parameter that specifies that
	// an Iterator should skip segments at a level >= than
	// MaxSegmentHeight.  MaxSegmentHeight is 1-based height, like an
	// array length.
	MaxSegmentHeight int
}

// EntryEx provides extra, advanced information about an entry from
// the Iterator.CurrentEx() method.
type EntryEx struct {
	// Operation is an OperationXxx const.
	Operation uint64
}

const OperationSet = uint64(0x0100000000000000)
const OperationDel = uint64(0x0200000000000000)
const OperationMerge = uint64(0x0300000000000000)

// A MergeOperator may be implemented by applications that wish to
// optimize their read-compute-write use cases.  Write-heavy counters,
// for example, could be implemented efficiently by using the
// MergeOperator functionality.
type MergeOperator interface {
	// Name returns an identifier for this merge operator, which might
	// be used for logging / debugging.
	Name() string

	// FullMerge the full sequence of operands on top of an
	// existingValue and returns the merged value.  The existingValue
	// may be nil if no value currently exists.  If full merge cannot
	// be done, return (nil, false).
	FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool)

	// Partially merge two operands.  If partial merge cannot be done,
	// return (nil, false), which will defer processing until a later
	// FullMerge().
	PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool)
}

// LowerLevelUpdate is the func callback signature used when a
// Collection wants to update its optional, lower-level storage.
type LowerLevelUpdate func(higher Snapshot) (lower Snapshot, err error)

// ------------------------------------------------------------

// NewCollection returns a new, unstarted Collection instance.
func NewCollection(options CollectionOptions) (
	Collection, error) {
	c := &collection{
		options:            options,
		stopCh:             make(chan struct{}),
		pingMergerCh:       make(chan ping, 10),
		doneMergerCh:       make(chan struct{}),
		donePersisterCh:    make(chan struct{}),
		lowerLevelSnapshot: newSnapshotWrapper(options.LowerLevelInit),
	}

	c.stackDirtyTopCond = sync.NewCond(&c.m)
	c.stackDirtyBaseCond = sync.NewCond(&c.m)

	return c, nil
}
