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
	"sync"
)

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

func (ss *segmentStack) addRef() {
	ss.m.Lock()
	ss.refs++
	ss.m.Unlock()
}

func (ss *segmentStack) decRef() {
	ss.m.Lock()
	ss.refs--
	if ss.refs <= 0 {
		if ss.lowerLevelSnapshot != nil {
			ss.lowerLevelSnapshot.Close()
			ss.lowerLevelSnapshot = nil
		}
	}
	ss.m.Unlock()
}

// ------------------------------------------------------

// Close releases associated resources.
func (ss *segmentStack) Close() error {
	if ss != nil {
		ss.decRef()
	}

	return nil
}

// Get retrieves a val from a segmentStack.
func (ss *segmentStack) Get(key []byte, readOptions ReadOptions) ([]byte, error) {
	return ss.get(key, len(ss.a)-1, nil)
}

// get() retrieves a val from a segmentStack, but only considers
// segments at or below the segStart level.  The optional base
// segmentStack, when non-nil, is used instead of the
// lowerLevelSnapshot, as a form of controllable chaining.
func (ss *segmentStack) get(key []byte, segStart int, base *segmentStack) (
	[]byte, error) {
	if segStart >= 0 {
		ss.ensureSorted(0, segStart)

		for seg := segStart; seg >= 0; seg-- {
			b := ss.a[seg]

			operation, k, v :=
				b.getOperationKeyVal(b.findStartKeyInclusivePos(key))
			if k != nil && bytes.Equal(k, key) {
				if operation == OperationDel {
					return nil, nil
				}

				if operation == OperationMerge {
					return ss.getMerged(k, v, seg-1, base)
				}

				return v, nil
			}
		}
	}

	if base != nil {
		return base.Get(key, ReadOptions{})
	}

	if ss.lowerLevelSnapshot != nil {
		return ss.lowerLevelSnapshot.Get(key, ReadOptions{})
	}

	return nil, nil
}

// ------------------------------------------------------

// getMerged() retrieves a lower level val for a given key and returns
// a merged val, based on the configured merge operator.
func (ss *segmentStack) getMerged(key, val []byte, segStart int,
	base *segmentStack) ([]byte, error) {
	mo := ss.collection.options.MergeOperator
	if mo == nil {
		return nil, ErrMergeOperatorNil
	}

	vLower, err := ss.get(key, segStart, base)
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
		numX0 := ss.a[newTopLevel].Len()
		numX1 := ss.a[newTopLevel+1].Len()
		if (float64(numX1) / float64(numX0)) > minMergePercentage {
			break
		}

		newTopLevel++
	}

	return newTopLevel
}

// ------------------------------------------------------

// merge() returns a new segmentStack, merging all the segments that
// are at the given newTopLevel and higher.
func (ss *segmentStack) merge(newTopLevel int, base *segmentStack) (
	*segmentStack, error) {
	// ----------------------------------------------------
	// First, rough estimate the bytes neeeded.

	totOps := ss.a[newTopLevel].Len()
	totBytes := ss.a[newTopLevel].NumKeyValBytes()

	iterPrealloc, err := ss.StartIterator(nil, nil, IteratorOptions{
		IncludeDeletions: true,
		SkipLowerLevel:   true,
		MinSegmentLevel:  newTopLevel + 1,
		MaxSegmentHeight: len(ss.a),
		base:             base,
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

		totOps++
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
		MaxSegmentHeight: len(ss.a),
		base:             base,
	})
	if err != nil {
		return nil, err
	}

	defer iter.Close()

OUTER:
	for {
		entryEx, key, val, err := iter.CurrentEx()
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
			segmentOps := segment.Len()

			for pos := cursor.pos; pos < segmentOps; pos++ {
				op, k, v := segment.getOperationKeyVal(pos)

				err = mergedSegment.mutate(op, k, v)
				if err != nil {
					return nil, err
				}
			}

			break OUTER
		}

		op := entryEx.Operation
		if op == OperationMerge {
			// TODO: the merge operator implementation is currently
			// inefficient and not lazy enough right now.
			val, err = ss.get(key, len(ss.a)-1, base)
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

	return &segmentStack{
		collection:         ss.collection,
		a:                  a,
		refs:               1,
		lowerLevelSnapshot: ss.lowerLevelSnapshot.addRef(),
	}, nil
}

// ------------------------------------------------------

func (ss *segmentStack) ensureSorted(minSeg, maxSeg int) {
	if !ss.collection.options.DeferredSort {
		return
	}

	sorted := true // Two phases allows for more concurrent sorting.
	for seg := maxSeg; seg >= minSeg; seg-- {
		sorted = sorted && ss.a[seg].requestSort(false)
	}

	if !sorted {
		for seg := maxSeg; seg >= minSeg; seg-- {
			ss.a[seg].requestSort(true)
		}
	}
}

// ------------------------------------------------------

// SegmentStackStats represents the stats for a segmentStack.
type SegmentStackStats struct {
	CurOps      uint64
	CurBytes    uint64 // Counts key-val bytes only, not metadata.
	CurSegments uint64
}

// AddTo adds the values from this SegmentStackStats to the dest
// SegmentStackStats.
func (sss *SegmentStackStats) AddTo(dest *SegmentStackStats) {
	if sss == nil {
		return
	}

	dest.CurOps += sss.CurOps
	dest.CurBytes += sss.CurBytes
	dest.CurSegments += sss.CurSegments
}

// Stats returns the stats for this segment stack.
func (ss *segmentStack) Stats() *SegmentStackStats {
	rv := &SegmentStackStats{CurSegments: uint64(len(ss.a))}
	for _, seg := range ss.a {
		rv.CurOps += uint64(seg.Len())
		rv.CurBytes += uint64(seg.NumKeyValBytes())
	}
	return rv
}
