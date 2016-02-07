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
	"bytes"
	"container/heap"
	"sort"
)

// Close releases associated resources.
func (ss *segmentStack) Close() error {
	return nil
}

// Get retrieves a val from a segmentStack.
func (ss *segmentStack) Get(key []byte) ([]byte, error) {
	for j := len(ss.a) - 1; j >= 0; j-- {
		b := ss.a[j]

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
func (ss *segmentStack) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
) (Iterator, error) {
	return ss.startIterator(startKeyInclusive, endKeyExclusive,
		false, 0)
}

// ------------------------------------------------------

// merge returns a new segmentStack, merging all the segments at
// newTopLevel and higher.
func (ss *segmentStack) merge(newTopLevel int) (*segmentStack, error) {
	totOps := len(ss.a[newTopLevel].kvs) / 2
	totBytes := len(ss.a[newTopLevel].buf)

	iterPrealloc, err :=
		ss.startIterator(nil, nil, true, newTopLevel+1)
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

	iter, err := ss.startIterator(nil, nil, true, newTopLevel)
	if err != nil {
		return nil, err
	}

	defer iter.Close()

OUTER:
	for {
		op, key, val, err := iter.current()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(iter.cursors) == 1 {
			// When only 1 cursor remains, copy the remains of the
			// last segment more directly instead of Next()'ing
			// through the iterator.
			cursor := &iter.cursors[0]

			segment := iter.ss.a[cursor.ssIndex]
			segmentOps := len(segment.kvs) / 2

			for pos := cursor.pos; pos < segmentOps; pos++ {
				op, k, v := segment.getOperationKeyVal(pos)

				err = mergedSegment.mutate(op, k, v)
				if err != nil {
					return nil, err
				}
			}

			break OUTER
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

	a = append(a, ss.a[0:newTopLevel]...)
	a = append(a, mergedSegment)

	return &segmentStack{a: a}, nil
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

// Merge creates or updates a key-val entry in the Collection via the
// MergeOperator defined in the CollectionOptions.  The key must be
// unique (not repeated) within the segment.
func (a *segment) Merge(key, val []byte) error {
	return ErrUnimplemented // TODO.
}

// ------------------------------------------------------

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
func (ss *segmentStack) startIterator(
	startKeyInclusive, endKeyExclusive []byte,
	includeDeletions bool,
	minLevel int,
) (*iterator, error) {
	iter := &iterator{
		ss:      ss,
		cursors: make([]cursor, 0, len(ss.a)),

		startKeyInclusive: startKeyInclusive,
		endKeyExclusive:   endKeyExclusive,
		includeDeletions:  includeDeletions,
	}

	for ssIndex := minLevel; ssIndex < len(ss.a); ssIndex++ {
		b := ss.a[ssIndex]

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
			ssIndex: ssIndex,
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
				iter.ss.a[next.ssIndex].getOperationKeyVal(next.pos)
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
			iter.ss.a[next.ssIndex].getOperationKeyVal(next.pos)
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

	return iter.cursors[i].ssIndex > iter.cursors[j].ssIndex
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
