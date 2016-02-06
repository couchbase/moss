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
// provides a data structure that manages an ordered collection of
// key-val entries.
//
// The design is similar to a (much) simplified LSM tree, in that
// there is a stack of sorted key-val arrays or "segments".  To
// incorporate the next batch (see: ExecuteBatch()), we sort the
// incoming batch of key-val mutations into a "segment" and atomically
// push the new segment onto the stack.  A higher segment in the stack
// will shadow entries of the same key from lower segments.
//
// Separately, an asynchronous goroutine (the "merger") will
// continuously merge N sorted segments into a single sorted segment
// to keep stack height low.  After you stop mutations, that is, the
// stack will eventually be merged down into a stack of height 1.
//
// The remaining, single, large sorted segment will be efficient in
// memory usage and efficient for binary search and range iteration.
//
// Another asynchronous goroutine (the "persister") can optionally
// persist the most recent work of the merger to outside storage.
//
// Iterations when the stack height is > 1 are implementing using a
// simple N-way heap merge.
//
// In this design, stacks are treated as immutable via a copy-on-write
// approach whenever a stack is "modified".  So, readers and writers
// essentially don't block each other, and taking a snapshot is also a
// similarly cheap operation by cloning a stack.

package moss

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"sort"
	"sync"
)

var ErrIteratorDone = errors.New("iterator-done")
var ErrAllocTooLarge = errors.New("alloc-too-large")

// A Collection represents an ordered mapping of key-val entries,
// where a Collection is snapshot'able and atomically updatable.
type Collection interface {
	// Start kicks off required background tasks.
	Start() error

	// Close synchronously stops background tasks and releases resources.
	Close() error

	// Snapshot returns a stable snapshot of the key-value entries.
	Snapshot() (Snapshot, error)

	// NewBatch returns a new Batch instance with preallocated
	// resources.  See the Batch.Alloc() method.
	NewBatch(totalOps, totalKeyValBytes int) (Batch, error)

	// ExecuteBatch atomically incorporates the provided Batch into
	// the Collection.  The Batch instance should not be reused after
	// ExecuteBatch() returns.
	ExecuteBatch(b Batch) error
}

// CollectionOptions allows applications to specify config settings.
type CollectionOptions struct {
	// MinMergePercentage allows the merger to avoid premature merging
	// of segments that are too small, where a segment X has to reach
	// a certain size percentage compared to the next lower segment
	// before segment X (and all segments above X) will be N-way
	// merged downards.
	MinMergePercentage float64

	Debug int // Higher means more logging, when Log != nil.

	Log func(format string, a ...interface{}) // Optional, may be nil.
}

// DefaultCollectionOptions are the default config settings.
var DefaultCollectionOptions = CollectionOptions{
	MinMergePercentage: 0.8,
	Debug:              0,
	Log:                nil,
}

// A Batch is a set of mutations that will be incorporated atomically
// into a Collection.
type Batch interface {
	// Close must be invoked to release resources.
	Close() error

	// Set creates or updates an key-val entry in the collection.  The
	// key must be unique (not repeated) within the Batch.  Set copies
	// the key and val bytes into the Batch, so the key-val memory may
	// be reused by the caller.
	Set(key, val []byte) error

	// Del deletes a key-val entry from the collection.  The key must
	// be unique (not repeated) within the Batch.  Del copies the key
	// bytes into the Batch, so the key bytes may be memory by the
	// caller.  Del() on a non-existent key results in a nil error.
	Del(key []byte) error

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
}

// A Snapshot is a stable view of a Collection for readers, isolated
// from concurrent mutation activity.
type Snapshot interface {
	// Close must be invoked to release resources.
	Close() error

	// Get retrieves a val from the Snapshot, and will return nil val
	// if the entry does not exist in the Snapshot.
	Get(key []byte) ([]byte, error)

	// StartIterator returns a new Iterator instance on this Snapshot.
	//
	// On success, the returned Iterator will be positioned so that
	// Iterator.Current() will either provide the first entry in the
	// range or ErrIteratorDone.
	//
	// A startKeyInclusive of nil means the logical "bottom-most"
	// possible key and an endKeyExclusive of nil means the logical
	// "top-most" possible key.
	StartIterator(startKeyInclusive, endKeyExclusive []byte) (Iterator, error)
}

// An Iterator allows enumeration of key-val entries from a Snapshot.
type Iterator interface {
	// Close must be invoked to release resources.
	Close() error

	// Next moves the Iterator to the next key-val entry and will
	// return ErrIteratorDone if the iterator is done.
	Next() error

	// Current returns ErrIteratorDone when the iterator is done.
	// Otherwise, Current() returns the current key and val, which
	// should be treated as immutable and as "owned" by the Iterator.
	// The key and val bytes will remain available until the next call
	// to Next() or Close().
	Current() (key, val []byte, err error)
}

// ------------------------------------------------------

// A collection implements the Collection interface.
type collection struct {
	options CollectionOptions

	stopCh          chan struct{}
	pingMergerCh    chan chan struct{}
	doneMergerCh    chan struct{}
	donePersisterCh chan struct{}

	// When a newly merged stackBase is ready, the merger will notify
	// the persister via the awakePersisterCh.
	awakePersisterCh chan *segmentStack

	m sync.Mutex // Protects the fields that follow.

	// New segments will be pushed onto stackOpen by ExecuteBatch().
	stackOpen *segmentStack

	// The merger asynchronously incorporates stackOpen entries into
	// the stackBase and also merges stackBase down to height of 1.
	//
	// The segments from both stackOpen + stackBase together represent
	// the entries of the collection.
	stackBase *segmentStack

	// When ExecuteBatch() has pushed a new segment onto stackOpen, it
	// notifies the merger via awakeMergerCh (if non-nil).
	awakeMergerCh chan struct{}
}

// A segmentStack is a stack of segments, where higher (later) entries
// in the stack have higher precedence, and should "shadow" any
// entries of the same key from lower in the stack.  A segmentStack
// implements the Snapshot interface.
type segmentStack struct {
	a []*segment
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

	totOperationSet uint64
	totOperationDel uint64
	totKeyByte      uint64
	totValByte      uint64
}

// TODO: Consider using some bytes from val length, perhaps for LRU?

const maskOperation = uint64(0xFF00000000000000)
const maskKeyLength = uint64(0x00FFFFFF00000000)
const maskValLength = uint64(0x00000000FFFFFFFF)

const operationSet = uint64(0x0100000000000000)
const operationDel = uint64(0x0200000000000000)

// An iterator tracks a min-heap "scan-line" of cursors through a
// segmentStack.  Iterator also implements the sort.Interface and
// heap.Interface on its cursors.
type iterator struct {
	bs *segmentStack

	cursors []cursor // The len(cursors) <= len(bs.a).

	startKeyInclusive []byte
	endKeyExclusive   []byte
	includeDeletions  bool
}

// A cursor rerpresents a logical entry position inside a segment in a
// segmentStack.
type cursor struct {
	bsIndex int // Index into Iterator.bs.a.
	pos     int // Logical entry position into Iterator.bs.a[bsIndex].kvs.

	op uint64
	k  []byte
	v  []byte
}

// ------------------------------------------------------

// NewCollection returns a new, unstarted Collection instance.
func NewCollection(options CollectionOptions) (
	Collection, error) {
	return &collection{
		options:         options,
		stopCh:          make(chan struct{}),
		pingMergerCh:    make(chan chan struct{}, 10),
		doneMergerCh:    make(chan struct{}),
		donePersisterCh: make(chan struct{}),

		awakePersisterCh: make(chan *segmentStack, 10),
	}, nil
}

// Start kicks off required background gouroutines.
func (m *collection) Start() error {
	go m.runMerger()
	go m.runPersister()
	return nil
}

// Close synchronously stops background goroutines.
func (m *collection) Close() error {
	close(m.stopCh)
	<-m.doneMergerCh
	<-m.donePersisterCh
	return nil
}

// Snapshot returns a stable snapshot of the key-value entries.
func (m *collection) Snapshot() (Snapshot, error) {
	rv := &segmentStack{}

	m.m.Lock()

	rv.a = make([]*segment, 0, m.stackOpenBaseLenLOCKED())

	if m.stackBase != nil {
		rv.a = append(rv.a, m.stackBase.a...)
	}
	if m.stackOpen != nil {
		rv.a = append(rv.a, m.stackOpen.a...)
	}

	m.m.Unlock()

	return rv, nil
}

// NewBatch returns a new Batch instance with hinted amount of
// resources expected to be required.
func (m *collection) NewBatch(totalOps, totalKeyValBytes int) (
	Batch, error) {
	return newSegment(totalOps, totalKeyValBytes)
}

// ExecuteBatch atomically incorporates the provided Batch into the
// collection.  The Batch instance should not be reused after
// ExecuteBatch() returns.
func (m *collection) ExecuteBatch(bIn Batch) error {
	b, ok := bIn.(*segment)
	if !ok {
		return fmt.Errorf("wrong Batch implementation type")
	}

	if b == nil || len(b.kvs) <= 0 {
		return nil
	}

	bsorted := b.sort()

	stackOpen := &segmentStack{}

	m.m.Lock()

	if m.stackOpen != nil {
		stackOpen.a = append(stackOpen.a, m.stackOpen.a...)
	}

	stackOpen.a = append(stackOpen.a, bsorted)

	m.stackOpen = stackOpen

	awakeMergerCh := m.awakeMergerCh
	m.awakeMergerCh = nil

	m.m.Unlock()

	if awakeMergerCh != nil {
		close(awakeMergerCh)
	}

	return b.Close()
}

// ------------------------------------------------------

// WaitForMerger blocks until the merger has run another cycle.
func (m *collection) WaitForMerger() error {
	pongCh := make(chan struct{})
	m.pingMergerCh <- pongCh
	<-pongCh
	return nil
}

// ------------------------------------------------------

func (m *collection) Log(format string, a ...interface{}) {
	if m.options.Log == nil {
		return
	}

	if m.options.Debug > 0 {
		m.options.Log(format, a...)
	}
}

// ------------------------------------------------------

func (m *collection) stackOpenBaseLenLOCKED() int {
	rv := 0
	if m.stackOpen != nil {
		rv += len(m.stackOpen.a)
	}
	if m.stackBase != nil {
		rv += len(m.stackBase.a)
	}
	return rv
}

// ------------------------------------------------------

func (m *collection) runMerger() {
	defer close(m.doneMergerCh)

	minMergePercentage := m.options.MinMergePercentage
	if minMergePercentage <= 0 {
		minMergePercentage = DefaultCollectionOptions.MinMergePercentage
	}

	var pongChs []chan struct{}

	notifyPongChs := func(pongChsIn []chan struct{}) {
		for _, pongCh := range pongChsIn {
			close(pongCh)
		}
	}

	defer func() {
		notifyPongChs(pongChs)
		pongChs = nil
	}()

	// ---------------------------------------------------

OUTER:
	for {
		// ---------------------------------------------
		// Notify pongChs from last loop.

		notifyPongChs(pongChs)
		pongChs = nil

		// ---------------------------------------------
		// Wait for new stackOpen entries.

		var awakeMergerCh chan struct{}

		m.m.Lock()

		if m.stackOpen == nil || len(m.stackOpen.a) <= 0 {
			m.awakeMergerCh = make(chan struct{})
			awakeMergerCh = m.awakeMergerCh
		}

		m.m.Unlock()

		if awakeMergerCh != nil {
			select {
			case <-m.stopCh:
				return
			case pongCh := <-m.pingMergerCh:
				pongChs = append(pongChs, pongCh)
			case <-awakeMergerCh:
				// NO-OP.
			}
		}

		// ---------------------------------------------

	COLLECT_MORE_PONGS:
		for {
			select {
			case pongCh := <-m.pingMergerCh:
				pongChs = append(pongChs, pongCh)
			default:
				break COLLECT_MORE_PONGS
			}
		}

		// ---------------------------------------------
		// Atomically ingest stackOpen into stackBase.

		dirty := false

		stackBase := &segmentStack{}

		m.m.Lock()

		stackBase.a = make([]*segment, 0, m.stackOpenBaseLenLOCKED())

		if m.stackBase != nil {
			stackBase.a = append(stackBase.a, m.stackBase.a...)
		}

		if m.stackOpen != nil {
			stackBase.a = append(stackBase.a, m.stackOpen.a...)
			m.stackOpen = nil

			dirty = true
		}

		m.stackBase = stackBase

		m.m.Unlock()

		// ---------------------------------------------
		// Merge multiple stackBase layers.

		if len(stackBase.a) > 1 {
			maxTopLevel := len(stackBase.a) - 2
			newTopLevel := 0
			for newTopLevel < maxTopLevel {
				numX0 := len(stackBase.a[newTopLevel].kvs)
				numX1 := len(stackBase.a[newTopLevel+1].kvs)
				if (float64(numX1) / float64(numX0)) > minMergePercentage {
					break
				}

				newTopLevel += 1
			}

			nextStackBase, err := stackBase.merge(newTopLevel)
			if err != nil {
				// TODO: err handling.
				continue OUTER
			}

			m.m.Lock()

			m.stackBase = nextStackBase

			soHeight := 0
			if m.stackOpen != nil {
				soHeight = len(m.stackOpen.a)
			}

			m.m.Unlock()

			stackBase = nextStackBase

			m.Log("stackBase height: %d,"+
				" top level size: %d,"+
				" stackOpen height: %d\n",
				len(nextStackBase.a),
				len(nextStackBase.a[len(nextStackBase.a)-1].kvs)/2,
				soHeight)
		}

		// ---------------------------------------------
		// Notify persister.

		if m.awakePersisterCh != nil && dirty {
			m.awakePersisterCh <- stackBase
		}
	}

	// TODO: Delay merger until lots of deletion tombstones?
	//
	// TODO: The base layer is likely the largest, so instead of heap
	// merging the base layer entries, treat the base layer with
	// special case to binary search to find better start points?
}

// merge returns a new segmentStack, merging all the segments at
// newTopLevel and higher.
func (bs *segmentStack) merge(newTopLevel int) (*segmentStack, error) {
	totOps := len(bs.a[newTopLevel].kvs) / 2
	totBytes := len(bs.a[newTopLevel].buf)

	iterPrealloc, err :=
		bs.startIterator(nil, nil, true, newTopLevel+1)
	if err != nil {
		return nil, err
	}

	defer iterPrealloc.Close()

	for {
		_, key, val, err := iterPrealloc.current()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			return nil, err
		}

		totOps += 1
		totBytes += len(key) + len(val)

		err = iterPrealloc.Next()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	// ----------------------------------------------------

	mergedSegment, err := newSegment(totOps, totBytes)
	if err != nil {
		return nil, err
	}

	iter, err := bs.startIterator(nil, nil, true, newTopLevel)
	if err != nil {
		return nil, err
	}

	defer iter.Close()

	for {
		op, key, val, err := iter.current()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			return nil, err
		}

		err = mergedSegment.mutate(op, key, val)
		if err != nil {
			return nil, err
		}

		err = iter.Next()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	var a []*segment

	a = append(a, bs.a[0:newTopLevel]...)
	a = append(a, mergedSegment)

	return &segmentStack{a: a}, nil
}

// ------------------------------------------------------

func (m *collection) runPersister() {
	defer close(m.donePersisterCh)

	for {
		var persistableSS *segmentStack

		// Consume until we have the last, persistable segment stack.
	CONSUME_LOOP:
		for {
			select {
			case <-m.stopCh:
				return
			case persistableSS = <-m.awakePersisterCh:
				// NO-OP.
			default:
				break CONSUME_LOOP
			}
		}

		if persistableSS == nil { // Need to wait.
			select {
			case <-m.stopCh:
				return
			case persistableSS = <-m.awakePersisterCh:
				// NO-OP.
			}
		}

		// TODO: actually persist the persistableSS.
	}
}

// ------------------------------------------------------

// Close releases associated resources.
func (bs *segmentStack) Close() error {
	return nil
}

// ------------------------------------------------------

// newSegment allocates a segment with hinted amount of resources.
func newSegment(totalOps, totalKeyValBytes int) (
	*segment, error) {
	return &segment{
		kvs: make([]uint64, 0, totalOps*2),
		buf: make([]byte, 0, totalKeyValBytes),
	}, nil
}

// Close releases resources associated with the segment.
func (a *segment) Close() error {
	a.kvs = nil
	a.buf = nil
	return nil
}

// Set copies the key and val bytes into the segment as a "set"
// mutation.  The key must be unique (not repeated) within the
// segment.
func (a *segment) Set(key, val []byte) error {
	return a.mutate(operationSet, key, val)
}

// Del copies the key bytes into the segment as a "deletion" mutation.
// The key must be unique (not repeated) within the segment.
func (a *segment) Del(key []byte) error {
	return a.mutate(operationDel, key, []byte(nil))
}

func (a *segment) mutate(operation uint64, key, val []byte) error {
	keyStart := len(a.buf)
	a.buf = append(a.buf, key...)
	keyLength := len(a.buf) - keyStart

	valStart := len(a.buf)
	a.buf = append(a.buf, val...)
	valLength := len(a.buf) - valStart

	return a.mutateEx(operation, keyStart, keyLength, valLength)
}

func (a *segment) mutateEx(operation uint64,
	keyStart, keyLength, valLength int) error {
	if keyLength <= 0 && valLength <= 0 {
		keyStart = 0
	}

	opKlVl := (maskOperation & operation) |
		(maskKeyLength & (uint64(keyLength) << 32)) |
		(maskValLength & (uint64(valLength)))

	a.kvs = append(a.kvs, opKlVl, uint64(keyStart))

	switch operation {
	case operationSet:
		a.totOperationSet += 1
	case operationDel:
		a.totOperationDel += 1
	default:
	}

	a.totKeyByte += uint64(keyLength)
	a.totValByte += uint64(valLength)

	return nil
}

// Alloc provides a slice of bytes "owned" by the segment, to reduce
// extra copying of memory.  See the Collection.NewBatch() method.
func (a *segment) Alloc(numBytes int) ([]byte, error) {
	bufLen := len(a.buf)
	bufCap := cap(a.buf)

	if numBytes > bufCap-bufLen {
		return nil, ErrAllocTooLarge
	}

	rv := a.buf[bufLen : bufLen+numBytes]

	a.buf = a.buf[0 : bufLen+numBytes]

	return rv, nil
}

// AllocSet is like Set(), but the caller must provide []byte
// parameters that came from Alloc(), for less buffer copying.
func (a *segment) AllocSet(keyFromAlloc, valFromAlloc []byte) error {
	bufCap := cap(a.buf)

	keyStart := bufCap - cap(keyFromAlloc)

	return a.mutateEx(operationSet,
		keyStart, len(keyFromAlloc), len(valFromAlloc))
}

// AllocDel is like Del(), but the caller must provide []byte
// parameters that came from Alloc(), for less buffer copying.
func (a *segment) AllocDel(keyFromAlloc []byte) error {
	bufCap := cap(a.buf)

	keyStart := bufCap - cap(keyFromAlloc)

	return a.mutateEx(operationDel,
		keyStart, len(keyFromAlloc), 0)
}

// ------------------------------------------------------

// sort returns a new, sorted segment from a given segment, where the
// underlying buf bytes might be shared.
func (b *segment) sort() *segment {
	rv := *b // Copy fields.

	rv.kvs = append([]uint64(nil), b.kvs...)

	sort.Sort(&rv)

	return &rv
}

func (a *segment) Len() int {
	return len(a.kvs) / 2
}

func (a *segment) Swap(i, j int) {
	x := i * 2
	y := j * 2

	// Operation + key length + val length.
	a.kvs[x], a.kvs[y] = a.kvs[y], a.kvs[x]

	x++
	y++

	a.kvs[x], a.kvs[y] = a.kvs[y], a.kvs[x] // Buf index.
}

func (a *segment) Less(i, j int) bool {
	x := i * 2
	y := j * 2

	kxLength := int((maskKeyLength & a.kvs[x]) >> 32)
	kxStart := int(a.kvs[x+1])
	kx := a.buf[kxStart : kxStart+kxLength]

	kyLength := int((maskKeyLength & a.kvs[y]) >> 32)
	kyStart := int(a.kvs[y+1])
	ky := a.buf[kyStart : kyStart+kyLength]

	return bytes.Compare(kx, ky) < 0
}

// ------------------------------------------------------

// Get retrieves a val from the segmentStack.
func (bs *segmentStack) Get(key []byte) ([]byte, error) {
	for j := len(bs.a) - 1; j >= 0; j-- {
		b := bs.a[j]

		operation, k, v :=
			b.getOperationKeyVal(b.findStartKeyInclusivePos(key))
		if k != nil && bytes.Equal(k, key) {
			if operation == operationDel {
				return nil, nil
			}

			return v, nil
		}
	}

	return nil, nil
}

// ------------------------------------------------------

// findStartKeyInclusivePos returns the logical entry position for the
// given (inclusive) start key.  With segment keys of [b, d, f],
// looking for 'c' will return 1.  Looking for 'd' will return 1.
// Looking for 'g' will return 3.  Looking for 'a' will return 0.
func (b *segment) findStartKeyInclusivePos(startKeyInclusive []byte) int {
	n := len(b.kvs) / 2

	return sort.Search(n, func(pos int) bool {
		x := pos * 2

		kLength := int((maskKeyLength & b.kvs[x]) >> 32)
		kStart := int(b.kvs[x+1])
		k := b.buf[kStart : kStart+kLength]

		return bytes.Compare(k, startKeyInclusive) >= 0
	})
}

// getOperationKeyVal returns the operation, key, val for a given
// logical entry position in the segment.
func (b *segment) getOperationKeyVal(pos int) (
	uint64, []byte, []byte) {
	x := pos * 2
	if x < 0 || x >= len(b.kvs) {
		return 0, nil, nil
	}

	opklvl := b.kvs[x]

	kLength := int((maskKeyLength & opklvl) >> 32)
	vLength := int(maskValLength & opklvl)

	kStart := int(b.kvs[x+1])
	vStart := kStart + kLength

	k := b.buf[kStart : kStart+kLength]
	v := b.buf[vStart : vStart+vLength]

	operation := maskOperation & opklvl

	return operation, k, v
}

// ------------------------------------------------------

// StartIterator returns a new iterator instance based on the
// segmentStack.
//
// On success, the returned Iterator will be positioned so that
// Iterator.Current() will either provide the first entry in the
// iteration range or ErrIteratorDone.
//
// A startKeyInclusive of nil means the logical "bottom-most" possible
// key and an endKeyExclusive of nil means the logical "top-most"
// possible key.
func (bs *segmentStack) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
) (Iterator, error) {
	return bs.startIterator(startKeyInclusive, endKeyExclusive,
		false, 0)
}

// startIterator returns a new iterator instance on the segmentStack.
//
// On success, the returned Iterator will be positioned so that
// Iterator.Current() will either provide the first entry in the
// iteration range or ErrIteratorDone.
//
// A startKeyInclusive of nil means the logical "bottom-most" possible
// key and an endKeyExclusive of nil means the logical "top-most"
// possible key.
//
// startIterator can optionally include deletion operations in the
// enumeration via the includeDeletions flag.
//
// startIterator can ignore lower segments, via the minLevel
// parameter.  For example, to ignore the lowest, 0th segment, use
// minLevel of 1.
func (bs *segmentStack) startIterator(
	startKeyInclusive, endKeyExclusive []byte,
	includeDeletions bool,
	minLevel int,
) (*iterator, error) {
	iter := &iterator{
		bs:      bs,
		cursors: make([]cursor, 0, len(bs.a)),

		startKeyInclusive: startKeyInclusive,
		endKeyExclusive:   endKeyExclusive,
		includeDeletions:  includeDeletions,
	}

	for bsIndex := minLevel; bsIndex < len(bs.a); bsIndex++ {
		b := bs.a[bsIndex]

		pos := b.findStartKeyInclusivePos(startKeyInclusive)

		op, k, v := b.getOperationKeyVal(pos)
		if op == 0 && k == nil && v == nil {
			continue
		}

		if iter.endKeyExclusive != nil &&
			bytes.Compare(k, iter.endKeyExclusive) >= 0 {
			continue
		}

		iter.cursors = append(iter.cursors, cursor{
			bsIndex: bsIndex,
			pos:     pos,
			op:      op,
			k:       k,
			v:       v,
		})
	}

	heap.Init(iter)

	if !iter.includeDeletions {
		op, _, _, _ := iter.current()
		if op == operationDel {
			iter.Next()
		}
	}

	return iter, nil
}

// Close must be invoked to release resources.
func (iter *iterator) Close() error {
	return nil
}

// Next returns ErrIteratorDone if the iterator is done.
func (iter *iterator) Next() error {
	if len(iter.cursors) <= 0 {
		return ErrIteratorDone
	}

	// ---------------------------------------------
	// Special case when only have 1 cursor left.

	if len(iter.cursors) == 1 {
		for {
			next := &iter.cursors[0]
			next.pos += 1
			next.op, next.k, next.v =
				iter.bs.a[next.bsIndex].getOperationKeyVal(next.pos)
			if (next.op == 0 && next.k == nil && next.v == nil) ||
				(iter.endKeyExclusive != nil &&
					bytes.Compare(next.k, iter.endKeyExclusive) >= 0) {
				heap.Pop(iter)

				return ErrIteratorDone
			}

			if !iter.includeDeletions &&
				iter.cursors[0].op == operationDel {
				continue
			}

			return nil
		}
	}

	// ---------------------------------------------
	// Otherwise use min-heap.

	lastK := iter.cursors[0].k

	for {
		next := &iter.cursors[0]
		next.pos += 1
		next.op, next.k, next.v =
			iter.bs.a[next.bsIndex].getOperationKeyVal(next.pos)
		if (next.op == 0 && next.k == nil && next.v == nil) ||
			(iter.endKeyExclusive != nil &&
				bytes.Compare(next.k, iter.endKeyExclusive) >= 0) {
			heap.Pop(iter)
		} else {
			heap.Fix(iter, 0)
		}

		if len(iter.cursors) <= 0 {
			return ErrIteratorDone
		}

		if !bytes.Equal(iter.cursors[0].k, lastK) {
			if !iter.includeDeletions &&
				iter.cursors[0].op == operationDel {
				return iter.Next()
			}

			return nil
		}
	}
}

// Current returns ErrIteratorDone if the iterator is done.
// Otherwise, Current() returns the current key and val, which should
// be treated as immutable or read-only.  The key and val bytes will
// remain available until the next call to Next() or Close().
func (iter *iterator) Current() (key, val []byte, err error) {
	_, key, val, err = iter.current()

	return key, val, err
}

func (iter *iterator) current() (op uint64, key, val []byte, err error) {
	if len(iter.cursors) <= 0 {
		return 0, nil, nil, ErrIteratorDone
	}

	cursor := &iter.cursors[0]

	return cursor.op, cursor.k, cursor.v, nil
}

func (iter *iterator) Len() int {
	return len(iter.cursors)
}

func (iter *iterator) Less(i, j int) bool {
	c := bytes.Compare(iter.cursors[i].k, iter.cursors[j].k)
	if c < 0 {
		return true
	}
	if c > 0 {
		return false
	}

	return iter.cursors[i].bsIndex > iter.cursors[j].bsIndex
}

func (iter *iterator) Swap(i, j int) {
	iter.cursors[i], iter.cursors[j] = iter.cursors[j], iter.cursors[i]
}

func (iter *iterator) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	iter.cursors = append(iter.cursors, x.(cursor))
}

func (iter *iterator) Pop() interface{} {
	n := len(iter.cursors)
	x := iter.cursors[n-1]
	iter.cursors = iter.cursors[0 : n-1]
	return x
}
