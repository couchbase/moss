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

package moss

import (
	"sync"
)

// A collection implements the Collection interface.
type collection struct {
	options CollectionOptions

	stopCh          chan struct{}
	pingMergerCh    chan ping
	doneMergerCh    chan struct{}
	donePersisterCh chan struct{}

	// When a newly merged stackDirtyMid is ready, the merger will
	// notify the persister via the awakePersisterCh.
	awakePersisterCh chan *segmentStack

	m sync.Mutex // Protects the fields that follow.

	// When ExecuteBatch() has pushed a new segment onto
	// stackDirtyTop, it notifies the merger via awakeMergerCh (if
	// non-nil).
	awakeMergerCh chan struct{}

	// stackDirtyTopCond is used to wait for space in stackDirtyTop.
	stackDirtyTopCond *sync.Cond

	// stackDirtyBaseCond is used to wait for non-nil stackDirtyBase.
	stackDirtyBaseCond *sync.Cond

	// ExecuteBatch() will push new segments onto stackDirtyTop.
	stackDirtyTop *segmentStack

	// The merger asynchronously grabs all segments from stackDirtyTop
	// and atomically moves them into stackDirtyMid.
	stackDirtyMid *segmentStack

	// stackDirtyBase represents the segments currently being
	// optionally persisted.  Will be nil when persistence is not
	// being used.
	stackDirtyBase *segmentStack

	// stackClean represents the segments that have been persisted,
	// and can be safely evicted, as the lowerLevelSnapshot will have
	// those entries.  Will be nil when persistence is not being used.
	stackClean *segmentStack

	// lowerLevelSnapshot provides an optional, lower-level storage
	// implementation, when using the Collection as a cache.
	lowerLevelSnapshot *snapshotWrapper
}

// A segmentStack is a stack of segments, where higher (later) entries
// in the stack have higher precedence, and should "shadow" any
// entries of the same key from lower in the stack.  A segmentStack
// implements the Snapshot interface.
type segmentStack struct {
	collection *collection

	a []*segment

	m sync.Mutex // Protects the fields the follow.

	refs int

	lowerLevelSnapshot *snapshotWrapper
}

// A segment is a sequence of key-val entries or operations.  A
// segment's kvs will be sorted by key when the segment is pushed into
// the collection.  A segment implements the Batch interface.
type segment struct {
	// Each key-val operation is encoded as 2 uint64's...
	// - operation (see: maskOperation) |
	//       key length (see: maskKeyLength) |
	//       val length (see: maskValLength).
	// - start index into buf for key-val bytes.
	kvs []uint64

	// Contiguous backing memory for the keys and vals of the segment.
	buf []byte

	totOperationSet   uint64
	totOperationDel   uint64
	totOperationMerge uint64
	totKeyByte        uint64
	totValByte        uint64
}

// See the OperationXxx consts.
const maskOperation = uint64(0x0F00000000000000)

// Max key length is 2^24, from 24 bits key length.
const maskKeyLength = uint64(0x00FFFFFF00000000)

// Max val length is 2^28, from 28 bits val length.
const maskValLength = uint64(0x000000000FFFFFFF)

// TODO: Consider using some bits from reserved, perhaps for LRU,
// perhaps to track whether an item was persisted?

const maskRESERVED = uint64(0xF0000000F0000000)

// An iterator tracks a min-heap "scan-line" of cursors through a
// segmentStack.  Iterator also implements the sort.Interface and
// heap.Interface on its cursors.
type iterator struct {
	ss *segmentStack

	cursors []cursor // The len(cursors) <= len(ss.a).

	startKeyInclusive []byte
	endKeyExclusive   []byte

	iteratorOptions IteratorOptions

	lowerLevelIter Iterator // May be nil.
}

// A cursor rerpresents a logical entry position inside a segment in a
// segmentStack.
type cursor struct {
	ssIndex int // Index into Iterator.ss.a.
	pos     int // Logical entry position into Iterator.ss.a[ssIndex].kvs.

	op uint64
	k  []byte
	v  []byte
}

// A ping message is used to notify and wait for asynchronous tasks.
type ping struct {
	kind string // The kind of ping.

	// When non-nil, the pongCh will be closed when task is done.
	pongCh chan struct{}
}
