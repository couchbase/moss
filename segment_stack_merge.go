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

// calcTargetTopLevel() heuristically computes a new top level that
// the segmentStack should be merged to.
func (ss *segmentStack) calcTargetTopLevel() int {
	var minMergePercentage float64
	if ss.options != nil {
		minMergePercentage = ss.options.MinMergePercentage
	}
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

	var totOps int
	var totKeyBytes, totValBytes uint64
	for i := newTopLevel; i < len(ss.a); i++ {
		totOps += ss.a[i].Len()
		nk, nv := ss.a[i].NumKeyValBytes()
		totKeyBytes += nk
		totValBytes += nv
	}

	// ----------------------------------------------------
	// Next, use an iterator for the actual merge.

	mergedSegment, err := newSegment(totOps, int(totKeyBytes+totValBytes))
	if err != nil {
		return nil, err
	}

	err = ss.mergeInto(newTopLevel, len(ss.a), mergedSegment, base, true, nil)
	if err != nil {
		return nil, err
	}

	a := make([]Segment, 0, newTopLevel+1)
	a = append(a, ss.a[0:newTopLevel]...)
	a = append(a, mergedSegment)

	return &segmentStack{
		options:            ss.options,
		a:                  a,
		refs:               1,
		lowerLevelSnapshot: ss.lowerLevelSnapshot.addRef(),
	}, nil
}

func (ss *segmentStack) mergeInto(minSegmentLevel, maxSegmentHeight int,
	dest SegmentMutator, base *segmentStack, optimizeTail bool,
	cancelCh chan error) error {
	cancelCheckEvery := ss.options.MergerCancelCheckEvery
	if cancelCheckEvery <= 0 {
		cancelCheckEvery = DefaultCollectionOptions.MergerCancelCheckEvery
	}

	iter, err := ss.startIterator(nil, nil, IteratorOptions{
		IncludeDeletions: true,
		SkipLowerLevel:   true,
		MinSegmentLevel:  minSegmentLevel,
		MaxSegmentHeight: maxSegmentHeight,
		base:             base,
	})
	if err != nil {
		return err
	}

	defer iter.Close()

OUTER:
	for i := 0; true; i++ {
		if cancelCh != nil && i%cancelCheckEvery == 0 {
			select {
			case cancelErr := <-cancelCh:
				return cancelErr
			default:
				// NO-OP.
			}
		}

		entryEx, key, val, err := iter.CurrentEx()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			return err
		}

		if optimizeTail && len(iter.cursors) == 1 {
			// When only 1 cursor remains, copy the remains of the
			// last segment more directly instead of Next()'ing
			// through the iterator.
			cursor := iter.cursors[0]

			segment := iter.ss.a[cursor.ssIndex]
			segmentOps := segment.Len()

			for pos := cursor.pos; pos < segmentOps; pos++ {
				op, k, v := segment.GetOperationKeyVal(pos)

				err = dest.Mutate(op, k, v)
				if err != nil {
					return err
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
				return err
			}

			if val == nil {
				op = OperationDel
			} else {
				op = OperationSet
			}
		}

		err = dest.Mutate(op, key, val)
		if err != nil {
			return err
		}

		err = iter.Next()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}
