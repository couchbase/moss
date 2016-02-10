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

// Close releases associated resources.
func (ss *segmentStack) Close() error {
	if ss.lowerLevelSnapshot != nil {
		ss.lowerLevelSnapshot.Close()
		ss.lowerLevelSnapshot = nil
	}

	return nil
}

// Get retrieves a val from a segmentStack.
func (ss *segmentStack) Get(key []byte,
	readOptions ReadOptions) ([]byte, error) {
	return ss.get(key, len(ss.a)-1)
}

// get() retreives a val from a segmentStack, but only considers
// segments at or below the segStart level.
func (ss *segmentStack) get(key []byte, segStart int) ([]byte, error) {
	if segStart >= 0 {
		for seg := segStart; seg >= 0; seg-- {
			b := ss.a[seg]

			operation, k, v :=
				b.getOperationKeyVal(b.findStartKeyInclusivePos(key))
			if k != nil && bytes.Equal(k, key) {
				if operation == OperationDel {
					return nil, nil
				}

				if operation == OperationMerge {
					return ss.getMerged(k, v, seg-1)
				}

				return v, nil
			}
		}
	}

	if ss.lowerLevelSnapshot != nil {
		return ss.lowerLevelSnapshot.Get(key, ReadOptions{})
	}

	return nil, nil
}

// ------------------------------------------------------

// getMerged() retrieves a lower level val for a given key and returns
// a merged val, based on the configured merge operator.
func (ss *segmentStack) getMerged(key, val []byte, segStart int) (
	[]byte, error) {
	mo := ss.collection.options.MergeOperator
	if mo == nil {
		return nil, ErrMergeOperatorNil
	}

	vLower, err := ss.get(key, segStart)
	if err != nil {
		return nil, err
	}

	vMerged, ok := mo.FullMerge(key, vLower, [][]byte{val})
	if !ok {
		return nil, ErrMergeOperatorFullMergeFailed
	}

	return vMerged, nil
}

// ------------------------------------------------------

// calcTargetTopLevel() heuristically computes a new top level that
// the segmentStack should be merged to.
func (ss *segmentStack) calcTargetTopLevel() int {
	minMergePercentage := ss.collection.options.MinMergePercentage
	if minMergePercentage <= 0 {
		minMergePercentage = DefaultCollectionOptions.MinMergePercentage
	}

	newTopLevel := 0
	maxTopLevel := len(ss.a) - 2

	for newTopLevel < maxTopLevel {
		numX0 := len(ss.a[newTopLevel].kvs)
		numX1 := len(ss.a[newTopLevel+1].kvs)
		if (float64(numX1) / float64(numX0)) > minMergePercentage {
			break
		}

		newTopLevel += 1
	}

	return newTopLevel
}

// ------------------------------------------------------

// merge() returns a new segmentStack, merging all the segments that
// are at the given newTopLevel and higher.
func (ss *segmentStack) merge(newTopLevel int) (*segmentStack, error) {
	// ----------------------------------------------------
	// First, rough estimate the bytes neeeded.

	totOps := len(ss.a[newTopLevel].kvs) / 2
	totBytes := len(ss.a[newTopLevel].buf)

	iterPrealloc, err := ss.StartIterator(nil, nil, IteratorOptions{
		IncludeDeletions: true,
		SkipLowerLevel:   true,
		MinSegmentLevel:  newTopLevel + 1,
	})
	if err != nil {
		return nil, err
	}

	defer iterPrealloc.Close()

	for {
		_, key, val, err := iterPrealloc.CurrentEx()
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
	// Next, use an iterator for the actual merge.

	mergedSegment, err := newSegment(totOps, totBytes)
	if err != nil {
		return nil, err
	}

	iter, err := ss.startIterator(nil, nil, IteratorOptions{
		IncludeDeletions: true,
		SkipLowerLevel:   true,
		MinSegmentLevel:  newTopLevel,
	})
	if err != nil {
		return nil, err
	}

	defer iter.Close()

	var readOptions ReadOptions

OUTER:
	for {
		op, key, val, err := iter.CurrentEx()
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

		if op == OperationMerge {
			// TODO: the merge operator implementation is currently
			// inefficient and not lazy enough right now.
			val, err = iter.ss.Get(key, readOptions)
			if err != nil {
				return nil, err
			}

			if val == nil {
				op = OperationDel
			} else {
				op = OperationSet
			}
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

	return &segmentStack{collection: ss.collection, a: a}, nil
}

// ------------------------------------------------------

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
		cursors: make([]cursor, 0, len(ss.a)+1),

		startKeyInclusive: startKeyInclusive,
		endKeyExclusive:   endKeyExclusive,

		iteratorOptions: iteratorOptions,
	}

	for ssIndex := iteratorOptions.MinSegmentLevel; ssIndex < len(ss.a); ssIndex++ {
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

	if !iteratorOptions.SkipLowerLevel {
		llss := ss.lowerLevelSnapshot.addRef()
		if llss != nil {
			lowerLevelIter, err := llss.StartIterator(
				startKeyInclusive, endKeyExclusive, iteratorOptions)

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
				iter.cursors = append(iter.cursors, cursor{
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

	heap.Init(iter)

	if !iteratorOptions.IncludeDeletions {
		op, _, _, _ := iter.CurrentEx()
		if op == OperationDel {
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

	// ---------------------------------------------
	// Special case when only have 1 cursor left.

	if len(iter.cursors) == 1 {
		for {
			next := &iter.cursors[0]

			if next.ssIndex < 0 && next.pos < 0 {
				err := iter.lowerLevelIter.Next()
				if err != nil {
					iter.lowerLevelIter.Close()
					iter.lowerLevelIter = nil

					heap.Pop(iter)

					return err
				}

				return nil
			}

			next.pos += 1
			next.op, next.k, next.v =
				iter.ss.a[next.ssIndex].getOperationKeyVal(next.pos)
			if (next.op == 0 && next.k == nil && next.v == nil) ||
				(iter.endKeyExclusive != nil &&
					bytes.Compare(next.k, iter.endKeyExclusive) >= 0) {
				heap.Pop(iter)

				return ErrIteratorDone
			}

			if !iter.iteratorOptions.IncludeDeletions &&
				iter.cursors[0].op == OperationDel {
				continue
			}

			return nil
		}
	}

	// ---------------------------------------------
	// Otherwise use heap to find the next entry.

	lastK := iter.cursors[0].k

	for {
		next := &iter.cursors[0]

		if next.ssIndex < 0 && next.pos < 0 {
			var err error

			next.k, next.v, err = iter.lowerLevelIter.Current()
			if err != nil {
				iter.lowerLevelIter.Close()
				iter.lowerLevelIter = nil

				heap.Pop(iter)
			} else {
				heap.Fix(iter, 0)
			}
		} else {
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
}

// Current returns ErrIteratorDone if the iterator is done.
// Otherwise, Current() returns the current key and val, which should
// be treated as immutable or read-only.  The key and val bytes will
// remain available until the next call to Next() or Close().
func (iter *iterator) Current() ([]byte, []byte, error) {
	operation, key, val, err := iter.CurrentEx()
	if err != nil {
		return nil, nil, err
	}

	if operation == OperationDel {
		return nil, nil, nil
	}

	if operation == OperationMerge {
		valMerged, err :=
			iter.ss.getMerged(key, val, iter.cursors[0].ssIndex-1)
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
	op uint64, key, val []byte, err error) {
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
