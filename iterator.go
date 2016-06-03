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
)

// An iterator tracks a min-heap "scan-line" of cursors through a
// segmentStack.  Iterator implements the sort.Interface and
// heap.Interface on its cursors.
type iterator struct {
	ss *segmentStack

	cursors []*cursor // The len(cursors) <= len(ss.a).

	startKeyInclusive []byte
	endKeyExclusive   []byte

	iteratorOptions IteratorOptions

	lowerLevelIter Iterator // May be nil.
}

// A cursor rerpresents a logical entry position inside a segment in a
// segmentStack.  An ssIndex < 0 and pos < 0 mean that the op/k/v came
// from the lowerLevelIter.
type cursor struct {
	ssIndex int // Index into Iterator.ss.a.
	posEnd  int // Found via endKeyExclusive, or the segment length.
	pos     int // Logical entry position into Iterator.ss.a[ssIndex].

	op uint64
	k  []byte
	v  []byte
}

// StartIterator returns a new iterator on the given segmentStack.
//
// On success, the returned Iterator will be positioned so that
// Iterator.Current() will either provide the first entry in the
// iteration range or ErrIteratorDone.
//
// A startKeyInclusive of nil means the logical "bottom-most" possible
// key and an endKeyExclusive of nil means the logical "top-most"
// possible key.
//
// StartIterator can optionally include deletion operations in the
// enumeration via the IteratorOptions.IncludeDeletions flag.
//
// StartIterator can skip lower segments, via the
// IteratorOptions.MinSegmentLevel parameter.  For example, to ignore
// the lowest, 0th segment, use MinSegmentLevel of 1.
func (ss *segmentStack) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	if iteratorOptions.MaxSegmentHeight <= 0 {
		iteratorOptions.MaxSegmentHeight = len(ss.a)
	}

	return ss.startIterator(startKeyInclusive, endKeyExclusive, iteratorOptions)
}

// startIterator() returns a new iterator on the given segmentStack.
//
// On success, the returned Iterator will be positioned so that
// Iterator.Current() will either provide the first entry in the
// iteration range or ErrIteratorDone.
//
// A startKeyInclusive of nil means the logical "bottom-most" possible
// key and an endKeyExclusive of nil means the logical "top-most"
// possible key.
//
// startIterator() can optionally include deletion operations in the
// enumeration via the IteratorOptions.IncludeDeletions flag.
//
// startIterator() can skip lower segments, via the
// IteratorOptions.MinSegmentLevel parameter.  For example, to ignore
// the lowest, 0th segment, use MinSegmentLevel of 1.
func (ss *segmentStack) startIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (*iterator, error) {
	iter := &iterator{
		ss:      ss,
		cursors: make([]*cursor, 0, len(ss.a)+1),

		startKeyInclusive: startKeyInclusive,
		endKeyExclusive:   endKeyExclusive,

		iteratorOptions: iteratorOptions,
	}

	// ----------------------------------------------
	// Add cursors for our allowed segments.

	minSegmentLevel := iteratorOptions.MinSegmentLevel
	maxSegmentLevel := iteratorOptions.MaxSegmentHeight - 1

	ss.ensureSorted(minSegmentLevel, maxSegmentLevel)

	for ssIndex := minSegmentLevel; ssIndex <= maxSegmentLevel; ssIndex++ {
		b := ss.a[ssIndex]

		posEnd := b.Len()
		if endKeyExclusive != nil {
			posEnd = b.FindStartKeyInclusivePos(endKeyExclusive)
		}

		pos := b.FindStartKeyInclusivePos(startKeyInclusive)
		if pos >= posEnd {
			continue
		}

		op, k, v := b.GetOperationKeyVal(pos)
		if op == 0 && k == nil && v == nil {
			continue
		}

		iter.cursors = append(iter.cursors, &cursor{
			ssIndex: ssIndex,
			posEnd:  posEnd,
			pos:     pos,
			op:      op,
			k:       k,
			v:       v,
		})
	}

	// ----------------------------------------------
	// Add cursor for the lower level, if wanted.

	if !iteratorOptions.SkipLowerLevel &&
		ss.lowerLevelSnapshot != nil {
		llss := ss.lowerLevelSnapshot.addRef()
		if llss != nil {
			lowerLevelIter, err := llss.StartIterator(
				startKeyInclusive, endKeyExclusive, IteratorOptions{})

			llss.decRef()

			if err != nil {
				return nil, err
			}

			k, v, err := lowerLevelIter.Current()
			if err != nil && err != ErrIteratorDone {
				return nil, err
			}
			if err == ErrIteratorDone {
				lowerLevelIter.Close()
			}
			if err == nil {
				iter.cursors = append(iter.cursors, &cursor{
					ssIndex: -1,
					pos:     -1,
					op:      OperationSet,
					k:       k,
					v:       v,
				})

				iter.lowerLevelIter = lowerLevelIter
			}
		}
	}

	// ----------------------------------------------
	// Heap-ify the cursors.

	heap.Init(iter)

	if !iteratorOptions.IncludeDeletions {
		entryEx, _, _, _ := iter.CurrentEx()
		if entryEx.Operation == OperationDel {
			iter.Next()
		}
	}

	return iter, nil
}

// Close must be invoked to release resources.
func (iter *iterator) Close() error {
	if iter.lowerLevelIter != nil {
		iter.lowerLevelIter.Close()
		iter.lowerLevelIter = nil
	}

	return nil
}

// Next returns ErrIteratorDone if the iterator is done.
func (iter *iterator) Next() error {
	if len(iter.cursors) <= 0 {
		return ErrIteratorDone
	}

	lastK := iter.cursors[0].k

	for len(iter.cursors) > 0 {
		next := iter.cursors[0]

		if next.ssIndex < 0 && next.pos < 0 {
			err := iter.lowerLevelIter.Next()
			if err == nil {
				next.k, next.v, err = iter.lowerLevelIter.Current()
				if err == nil {
					heap.Fix(iter, 0)
				}
			}

			if err != nil {
				iter.lowerLevelIter.Close()
				iter.lowerLevelIter = nil

				heap.Pop(iter)
			}
		} else {
			next.pos++
			if next.pos >= next.posEnd {
				heap.Pop(iter)
			} else {
				next.op, next.k, next.v =
					iter.ss.a[next.ssIndex].GetOperationKeyVal(next.pos)
				if next.op == 0 {
					heap.Pop(iter)
				} else {
					heap.Fix(iter, 0)
				}
			}
		}

		if len(iter.cursors) <= 0 {
			return ErrIteratorDone
		}

		if !bytes.Equal(iter.cursors[0].k, lastK) {
			if !iter.iteratorOptions.IncludeDeletions &&
				iter.cursors[0].op == OperationDel {
				return iter.Next()
			}

			return nil
		}
	}

	return ErrIteratorDone
}

// Current returns ErrIteratorDone if the iterator is done.
// Otherwise, Current() returns the current key and val, which should
// be treated as immutable or read-only.  The key and val bytes will
// remain available until the next call to Next() or Close().
func (iter *iterator) Current() ([]byte, []byte, error) {
	entryEx, key, val, err := iter.CurrentEx()
	if err != nil {
		return nil, nil, err
	}

	op := entryEx.Operation
	if op == OperationDel {
		return nil, nil, nil
	}

	if op == OperationMerge {
		valMerged, err := iter.ss.getMerged(key, val, iter.cursors[0].ssIndex-1,
			iter.iteratorOptions.base)
		if err != nil {
			return nil, nil, err
		}

		return key, valMerged, nil
	}

	return key, val, err
}

// CurrentEx is a more advanced form of Current() that returns more
// metadata.  It is used when IteratorOptions.IncludeDeletions is
// true.  It returns ErrIteratorDone if the iterator is done.
// Otherwise, the current operation, key, val are returned.
func (iter *iterator) CurrentEx() (
	entryEx EntryEx, key, val []byte, err error) {
	if len(iter.cursors) <= 0 {
		return EntryEx{}, nil, nil, ErrIteratorDone
	}

	cursor := iter.cursors[0]

	return EntryEx{Operation: cursor.op}, cursor.k, cursor.v, nil
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
	iter.cursors = append(iter.cursors, x.(*cursor))
}

func (iter *iterator) Pop() interface{} {
	n := len(iter.cursors)
	x := iter.cursors[n-1]
	iter.cursors = iter.cursors[0 : n-1]
	return x
}
