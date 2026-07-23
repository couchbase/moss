//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"context"
	"sync"
	"sync/atomic"
)

// A segmentStack is a stack of segments, where higher (later) entries
// in the stack have higher precedence, and should "shadow" any
// entries of the same key from lower in the stack.  A segmentStack
// implements the Snapshot interface.
type segmentStack struct {
	options *CollectionOptions
	stats   *CollectionStats

	a []Segment

	m sync.Mutex // Protects the fields the follow.

	refs int

	lowerLevelSnapshot *SnapshotWrapper

	// incarNum represents this segmentStack's unique incarnation number assigned
	// when the child collection was created. 0 for top-level collection.
	incarNum uint64

	// childSegStacks recursively store child collection segmentStacks.
	childSegStacks map[string]*segmentStack
}

func (ss *segmentStack) addRef() {
	ss.m.Lock()
	ss.refs++
	ss.m.Unlock()
}

func (ss *segmentStack) decRef() {
	ss.m.Lock()
	ss.refs--
	var childSegStacks map[string]*segmentStack
	if ss.refs <= 0 {
		if ss.stats != nil { // Only update stats if snapshot is on collection.
			atomic.AddUint64(&ss.stats.TotSnapshotInternalClose, 1)
		}
		if ss.lowerLevelSnapshot != nil {
			ss.lowerLevelSnapshot.Close()
			ss.lowerLevelSnapshot = nil
		}
		// A segmentStack owns one ref on each of its child segStacks
		// (created with refs==1), so release that ownership recursively
		// when finally freed -- otherwise the children's lowerLevelSnapshots
		// (mmap/FileRef handles once a store is attached) are never closed
		// and superseded data files are never deleted.
		childSegStacks = ss.childSegStacks
		ss.childSegStacks = nil
	}
	ss.m.Unlock()

	// Outside the lock (each child's decRef takes the child's own lock
	// and recurses into grandchildren).
	for _, childSegStack := range childSegStacks {
		childSegStack.decRef()
	}
}

// ------------------------------------------------------

// Close releases associated resources.
func (ss *segmentStack) Close() error {
	if ss != nil {
		ss.decRef()
	}
	return nil
}

// ------------------------------------------------------

// childStacks returns a shallow copy of ss's child segment stacks, taken
// under ss.m.  decRef() nils ss.childSegStacks at end-of-life under the same
// lock, so every reader must go through this (or childStack) rather than
// ranging the map directly -- a lockless range would data-race that write.
// Returns nil (no allocation) for the common case of no child collections.
// The returned map is a private copy the caller may range without the lock;
// the child stacks it references stay valid as long as the caller holds the
// parent alive (a parent owns a ref on each child).
func (ss *segmentStack) childStacks() map[string]*segmentStack {
	ss.m.Lock()
	defer ss.m.Unlock()
	if len(ss.childSegStacks) == 0 {
		return nil
	}
	rv := make(map[string]*segmentStack, len(ss.childSegStacks))
	for name, childSegStack := range ss.childSegStacks {
		rv[name] = childSegStack
	}
	return rv
}

// childStack returns the child segment stack for the given name (nil if
// none), reading ss.childSegStacks under ss.m -- see childStacks.
func (ss *segmentStack) childStack(name string) *segmentStack {
	ss.m.Lock()
	defer ss.m.Unlock()
	return ss.childSegStacks[name]
}

// ------------------------------------------------------

// Get retrieves a val from a segmentStack.
func (ss *segmentStack) Get(key []byte, readOptions ReadOptions) ([]byte, error) {
	return ss.GetWithContext(context.Background(), key, readOptions)
}

// GetWithContext is like Get, but returns early with ctx.Err() if ctx
// is already canceled or past its deadline.
func (ss *segmentStack) GetWithContext(ctx context.Context, key []byte,
	readOptions ReadOptions) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ss.get(key, len(ss.a)-1, nil, readOptions)
}

// GetEx is like Get but also reports whether the key exists.
func (ss *segmentStack) GetEx(key []byte, readOptions ReadOptions) (
	[]byte, bool, error) {
	return ss.GetExWithContext(context.Background(), key, readOptions)
}

// GetExWithContext is like GetEx, but returns early with ctx.Err() if
// ctx is already canceled or past its deadline.
func (ss *segmentStack) GetExWithContext(ctx context.Context, key []byte,
	readOptions ReadOptions) ([]byte, bool, error) {
	return getExVal(ss.GetWithContext(ctx, key, readOptions))
}

// get() retrieves a val from a segmentStack, but only considers
// segments at or below the segStart level.  The optional base
// segmentStack, when non-nil, is used instead of the
// lowerLevelSnapshot, as a form of controllable chaining.
func (ss *segmentStack) get(key []byte, segStart int, base *segmentStack,
	readOptions ReadOptions) ([]byte, error) {
	return ss.resolveMerge(key, segStart, base, readOptions, nil)
}

// getMerged() continues resolving a key for which newer merge
// operand(s) have already been collected (newest-first): it descends
// from segStart to find the base value and applies the operands.  It's
// retained for the iterator Current() fast path (see iterator.go).
func (ss *segmentStack) getMerged(key, val []byte, segStart int,
	base *segmentStack, readOptions ReadOptions) ([]byte, error) {
	return ss.resolveMerge(key, segStart, base, readOptions, [][]byte{val})
}

// ------------------------------------------------------

// resolveMerge descends the segmentStack from segStart (newest to
// oldest), collecting OperationMerge operands (newest-first, on top of
// any already-collected mergeOperands) until it reaches a base value:
// an OperationSet, an OperationDel (a nil base), or the lower level.
// It then applies all collected operands to that base with a single
// MergeOperator.FullMerge() call.
//
// This is the "lazy" merge resolution: one top-to-bottom walk and one
// FullMerge() with all operands, rather than a recursive FullMerge()
// per operand that re-walked the stack each time.  It is semantically
// equivalent to that recursion for any spec-compliant FullMerge, whose
// contract is to apply a sequence of operands, in order, onto an
// existing value.
func (ss *segmentStack) resolveMerge(key []byte, segStart int,
	base *segmentStack, readOptions ReadOptions,
	mergeOperands [][]byte) ([]byte, error) {
	if segStart >= 0 {
		ss.ensureSorted(0, segStart)

		for seg := segStart; seg >= 0; seg-- {
			op, val, err := ss.a[seg].Get(key)
			if err != nil {
				return nil, err
			}
			if val == nil {
				// No point op at this level.  A range tombstone covering
				// key at this level deletes it (a nil base) -- but only
				// after ruling out a point op here, so a same-level point
				// op wins.  covers() short-circuits when the segment has
				// no range tombstones.
				if rd, ok := ss.a[seg].(rangeDeleter); ok && rd.covers(key) {
					if len(mergeOperands) == 0 {
						return nil, nil
					}
					return applyMergeOperands(ss.mergeOperator(), key, nil,
						mergeOperands)
				}
				continue
			}
			if op == OperationMerge {
				mergeOperands = append(mergeOperands, val) // Newest-first.
				continue
			}

			// op is OperationSet or OperationDel: the base value for
			// any collected merge operands (Del is a nil base).
			var baseVal []byte
			if op != OperationDel {
				baseVal = val
			}
			if len(mergeOperands) == 0 {
				return baseVal, nil
			}
			return applyMergeOperands(ss.mergeOperator(), key, baseVal,
				mergeOperands)
		}
	}

	// Reached the bottom of this stack; the base value (if any) comes
	// from the level below (base stack or lowerLevelSnapshot).
	lowerVal, err := ss.getLowerLevel(key, base, readOptions)
	if err != nil {
		return nil, err
	}
	if len(mergeOperands) == 0 {
		return lowerVal, nil
	}
	return applyMergeOperands(ss.mergeOperator(), key, lowerVal, mergeOperands)
}

func (ss *segmentStack) mergeOperator() MergeOperator {
	if ss.options != nil {
		return ss.options.MergeOperator
	}
	return nil
}

// getLowerLevel retrieves a val from the level below this segmentStack:
// the given base stack when non-nil, otherwise the lowerLevelSnapshot
// (unless the read opts out via SkipLowerLevel).
func (ss *segmentStack) getLowerLevel(key []byte, base *segmentStack,
	readOptions ReadOptions) ([]byte, error) {
	if base != nil {
		return base.Get(key, readOptions)
	}

	if !readOptions.SkipLowerLevel && ss.lowerLevelSnapshot != nil {
		return ss.lowerLevelSnapshot.Get(key, readOptions)
	} // TODO: else add a special return error indicating cache-miss!

	return nil, nil
}

// applyMergeOperands applies merge operands (given newest-first, as
// collected during a top-to-bottom descent) on top of baseVal via a
// single FullMerge(), after reversing them into the oldest-first order
// that FullMerge expects.
func applyMergeOperands(mo MergeOperator, key, baseVal []byte,
	operandsNewestFirst [][]byte) ([]byte, error) {
	if mo == nil {
		return nil, ErrMergeOperatorNil
	}

	n := len(operandsNewestFirst)
	operands := make([][]byte, n)
	for i := 0; i < n; i++ {
		operands[i] = operandsNewestFirst[n-1-i] // Reverse to oldest-first.
	}

	vMerged, ok := mo.FullMerge(key, baseVal, operands)
	if !ok {
		return nil, ErrMergeOperatorFullMergeFailed
	}

	return vMerged, nil
}

// ------------------------------------------------------

func (ss *segmentStack) ensureSorted(minSeg, maxSeg int) {
	if ss.options == nil || !ss.options.DeferredSort {
		return
	}

	sorted := true // Two phases allows for more concurrent sorting.
	for seg := maxSeg; seg >= minSeg; seg-- {
		sorted = sorted && ss.a[seg].RequestSort(false)
	}

	if !sorted {
		for seg := maxSeg; seg >= minSeg; seg-- {
			ss.a[seg].RequestSort(true)
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

// Stats returns the stats for this segment stack, including the stats of
// all of its (recursive) child collection segment stacks -- so that a
// child-only write registers as dirty in CurDirtyOps/CurDirtyBytes (which
// drives waitForPersistence and the MaxDirtyOps/MaxDirtyKeyValBytes
// back-pressure).  This mirrors isEmpty(), which likewise recurses.
func (ss *segmentStack) Stats() *SegmentStackStats {
	rv := &SegmentStackStats{}
	ss.statsTo(rv)
	return rv
}

// statsTo accumulates this segment stack's stats (and its child
// collections' stats, recursively) into rv.
func (ss *segmentStack) statsTo(rv *SegmentStackStats) {
	rv.CurSegments += uint64(len(ss.a))
	for _, seg := range ss.a {
		rv.CurOps += uint64(seg.Len())
		nk, nv := seg.NumKeyValBytes()
		rv.CurBytes += nk + nv
	}
	for _, childSegStack := range ss.childStacks() {
		childSegStack.statsTo(rv)
	}
}

// ChildCollectionNames returns an array of child collection name strings.
func (ss *segmentStack) ChildCollectionNames() ([]string, error) {
	childStacks := ss.childStacks()
	childCollections := make([]string, 0, len(childStacks))
	for name := range childStacks {
		childCollections = append(childCollections, name)
	}
	return childCollections, nil
}

// ChildCollectionSnapshot returns a Snapshot on a given child
// collection by its name.
func (ss *segmentStack) ChildCollectionSnapshot(childCollectionName string) (
	Snapshot, error) {
	childSegStack := ss.childStack(childCollectionName)
	if childSegStack == nil {
		return nil, nil
	}
	childSegStack.addRef()
	return childSegStack, nil
}

// ensureFullySorted recursively ensures that all child segmentStacks
// are sorted from 0 to end.
func (ss *segmentStack) ensureFullySorted() {
	ss.ensureSorted(0, len(ss.a)-1)
	for _, childSnapshot := range ss.childStacks() {
		childSnapshot.ensureFullySorted()
	}
}

func (ss *segmentStack) isEmpty() bool {
	if len(ss.a) > 0 {
		return false
	}
	for _, childSegStack := range ss.childStacks() {
		if !childSegStack.isEmpty() {
			return false
		}
	}
	return true
}
