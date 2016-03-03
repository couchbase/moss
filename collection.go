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
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
)

// A collection implements the Collection interface.
type collection struct {
	options CollectionOptions

	stopCh          chan struct{}
	pingMergerCh    chan ping
	doneMergerCh    chan struct{}
	donePersisterCh chan struct{}

	m sync.Mutex // Protects the fields that follow.

	// When ExecuteBatch() has pushed a new segment onto
	// stackDirtyTop, it notifies the merger via awakeMergerCh (if
	// non-nil).
	awakeMergerCh chan struct{}

	// stackDirtyTopCond is used to wait for space in stackDirtyTop.
	stackDirtyTopCond *sync.Cond

	// stackDirtyBaseCond is used to wait for non-nil stackDirtyBase.
	stackDirtyBaseCond *sync.Cond

	// ExecuteBatch() will push new segments onto stackDirtyTop if
	// there is space.
	stackDirtyTop *segmentStack

	// The merger goroutine asynchronously, atomically grabs all
	// segments from stackDirtyTop and atomically moves them into
	// stackDirtyMid.  The merger will also merge segments in
	// stackDirtyMid to keep its height low.
	stackDirtyMid *segmentStack

	// stackDirtyBase represents the segments currently being
	// persisted.  It is optionally populated by the merger when there
	// are merged segments ready for persistence.  Will be nil when
	// persistence is not being used.
	stackDirtyBase *segmentStack

	// stackClean represents the segments that have been optionally
	// persisted by the persister, and can now be safely evicted, as
	// the lowerLevelSnapshot will contain the entries from
	// stackClean.  Will be nil when persistence is not being used.
	stackClean *segmentStack

	// lowerLevelSnapshot provides an optional, lower-level storage
	// implementation, when using the Collection as a cache.
	lowerLevelSnapshot *snapshotWrapper

	// stats leverage sync/atomic counters.
	stats *CollectionStats
}

// ------------------------------------------------------

// Start kicks off required background gouroutines.
func (m *collection) Start() error {
	go m.runMerger()
	go m.runPersister()
	return nil
}

// Close synchronously stops background goroutines.
func (m *collection) Close() error {
	atomic.AddUint64(&m.stats.TotCloseBeg, 1)

	close(m.stopCh)

	m.stackDirtyBaseCond.Signal() // Awake persister.

	<-m.doneMergerCh
	atomic.AddUint64(&m.stats.TotCloseMergerDone, 1)

	<-m.donePersisterCh
	atomic.AddUint64(&m.stats.TotClosePersisterDone, 1)

	m.m.Lock()
	if m.lowerLevelSnapshot != nil {
		atomic.AddUint64(&m.stats.TotCloseLowerLevelBeg, 1)
		m.lowerLevelSnapshot.Close()
		m.lowerLevelSnapshot = nil
		atomic.AddUint64(&m.stats.TotCloseLowerLevelEnd, 1)
	}
	m.m.Unlock()

	atomic.AddUint64(&m.stats.TotCloseEnd, 1)

	return nil
}

// Options returns the current options.
func (m *collection) Options() CollectionOptions {
	return m.options
}

// Snapshot returns a stable snapshot of the key-value entries.
func (m *collection) Snapshot() (Snapshot, error) {
	atomic.AddUint64(&m.stats.TotSnapshotBeg, 1)

	rv, _, _, _, _ := m.snapshot(0, nil)

	atomic.AddUint64(&m.stats.TotSnapshotEnd, 1)

	return rv, nil
}

// NewBatch returns a new Batch instance with hinted amount of
// resources expected to be required.
func (m *collection) NewBatch(totalOps, totalKeyValBytes int) (
	Batch, error) {
	atomic.AddUint64(&m.stats.TotNewBatch, 1)
	atomic.AddUint64(&m.stats.TotNewBatchTotalOps, uint64(totalOps))
	atomic.AddUint64(&m.stats.TotNewBatchTotalKeyValBytes, uint64(totalKeyValBytes))

	return newSegment(totalOps, totalKeyValBytes)
}

// ExecuteBatch atomically incorporates the provided Batch into the
// collection.  The Batch instance should not be reused after
// ExecuteBatch() returns.
func (m *collection) ExecuteBatch(bIn Batch,
	writeOptions WriteOptions) error {
	atomic.AddUint64(&m.stats.TotExecuteBatchBeg, 1)

	b, ok := bIn.(*segment)
	if !ok {
		atomic.AddUint64(&m.stats.TotExecuteBatchErr, 1)

		return fmt.Errorf("wrong Batch implementation type")
	}

	if b == nil || b.Len() <= 0 {
		atomic.AddUint64(&m.stats.TotExecuteBatchEmpty, 1)
		return nil
	}

	maxPreMergerBatches := m.options.MaxPreMergerBatches
	if maxPreMergerBatches <= 0 {
		maxPreMergerBatches =
			DefaultCollectionOptions.MaxPreMergerBatches
	}

	if m.options.DeferredSort {
		b.needSorterCh = make(chan bool, 1)
		b.needSorterCh <- true // A ticket for the future sorter.
		close(b.needSorterCh)

		b.waitSortedCh = make(chan struct{})
	} else {
		sort.Sort(b)
	}

	stackDirtyTop := &segmentStack{collection: m, refs: 1}

	m.m.Lock()

	for m.stackDirtyTop != nil &&
		len(m.stackDirtyTop.a) >= maxPreMergerBatches {
		if m.options.DeferredSort {
			go b.requestSort(false) // While waiting, might as well sort.
		}

		atomic.AddUint64(&m.stats.TotExecuteBatchWaitBeg, 1)
		m.stackDirtyTopCond.Wait()
		atomic.AddUint64(&m.stats.TotExecuteBatchWaitEnd, 1)
	}

	numDirtyTop := 0
	if m.stackDirtyTop != nil {
		numDirtyTop = len(m.stackDirtyTop.a)
	}

	stackDirtyTop.a = make([]*segment, 0, numDirtyTop+1)

	if m.stackDirtyTop != nil {
		stackDirtyTop.a = append(stackDirtyTop.a, m.stackDirtyTop.a...)
	}

	stackDirtyTop.a = append(stackDirtyTop.a, b)

	prevStackDirtyTop := m.stackDirtyTop
	m.stackDirtyTop = stackDirtyTop

	awakeMergerCh := m.awakeMergerCh
	m.awakeMergerCh = nil

	m.m.Unlock()

	prevStackDirtyTop.Close()

	if awakeMergerCh != nil {
		atomic.AddUint64(&m.stats.TotExecuteBatchAwakeMergerBeg, 1)
		close(awakeMergerCh)
		atomic.AddUint64(&m.stats.TotExecuteBatchAwakeMergerEnd, 1)
	}

	atomic.AddUint64(&m.stats.TotExecuteBatchEnd, 1)

	return nil
}

// ------------------------------------------------------

// WaitForMerger blocks until the merger has run another cycle.
// Providing a kind of "mergeAll" forces a full merge and can be
// useful for applications that are no longer performing mutations and
// that want to optimize for retrievals.
func (m *collection) WaitForMerger(kind string) error {
	atomic.AddUint64(&m.stats.TotWaitForMergerBeg, 1)

	pongCh := make(chan struct{})
	m.pingMergerCh <- ping{
		kind:   kind,
		pongCh: pongCh,
	}
	<-pongCh

	atomic.AddUint64(&m.stats.TotWaitForMergerEnd, 1)
	return nil
}

// ------------------------------------------------------

// Log invokes the user's configured Log callback, if any, if the
// debug levels are met.
func (m *collection) Logf(format string, a ...interface{}) {
	if m.options.Debug > 0 &&
		m.options.Log != nil {
		m.options.Log(format, a...)
	}
}

// OnError invokes the user's configured OnError callback, in which
// the application might take further action, for example, such as
// Close()'ing the Collection in order to fix underlying
// storage/resource issues.
func (m *collection) OnError(err error) {
	atomic.AddUint64(&m.stats.TotOnError, 1)

	if m.options.OnError != nil {
		m.options.OnError(err)
	}
}

// ------------------------------------------------------

const snapshotSkipDirtyTop = uint32(0x00000001)
const snapshotSkipDirtyMid = uint32(0x00000002)
const snapshotSkipDirtyBase = uint32(0x00000004)
const snapshotSkipClean = uint32(0x00000008)

// snapshot() atomically clones the various stacks into a new, single
// segmentStack, controllable by skip flags, and also invokes the
// optional callback while holding the collection lock.
func (m *collection) snapshot(skip uint32, cb func(*segmentStack)) (
	*segmentStack, int, int, int, int) {
	atomic.AddUint64(&m.stats.TotSnapshotInternalBeg, 1)

	rv := &segmentStack{collection: m, refs: 1}

	heightDirtyTop := 0
	heightDirtyMid := 0
	heightDirtyBase := 0
	heightClean := 0

	m.m.Lock()

	rv.lowerLevelSnapshot = m.lowerLevelSnapshot.addRef()

	if m.stackDirtyTop != nil && (skip&snapshotSkipDirtyTop == 0) {
		heightDirtyTop = len(m.stackDirtyTop.a)
	}

	if m.stackDirtyMid != nil && (skip&snapshotSkipDirtyMid == 0) {
		heightDirtyMid = len(m.stackDirtyMid.a)
	}

	if m.stackDirtyBase != nil && (skip&snapshotSkipDirtyBase == 0) {
		heightDirtyBase = len(m.stackDirtyBase.a)
	}

	if m.stackClean != nil && (skip&snapshotSkipClean == 0) {
		heightClean = len(m.stackClean.a)
	}

	rv.a = make([]*segment, 0,
		heightDirtyTop+heightDirtyMid+heightDirtyBase+heightClean)

	if m.stackClean != nil && (skip&snapshotSkipClean == 0) {
		rv.a = append(rv.a, m.stackClean.a...)
	}

	if m.stackDirtyBase != nil && (skip&snapshotSkipDirtyBase == 0) {
		rv.a = append(rv.a, m.stackDirtyBase.a...)
	}

	if m.stackDirtyMid != nil && (skip&snapshotSkipDirtyMid == 0) {
		rv.a = append(rv.a, m.stackDirtyMid.a...)
	}

	if m.stackDirtyTop != nil && (skip&snapshotSkipDirtyTop == 0) {
		rv.a = append(rv.a, m.stackDirtyTop.a...)
	}

	if cb != nil {
		cb(rv)
	}

	m.m.Unlock()

	atomic.AddUint64(&m.stats.TotSnapshotInternalEnd, 1)

	return rv, heightClean, heightDirtyBase, heightDirtyMid, heightDirtyTop
}

// ------------------------------------------------------

// runMerger() implements the background merger task.
func (m *collection) runMerger() {
	defer func() {
		close(m.doneMergerCh)

		atomic.AddUint64(&m.stats.TotMergerEnd, 1)
	}()

	pings := []ping{}

	defer func() {
		replyToPings(pings)
		pings = pings[0:0]
	}()

OUTER:
	for {
		atomic.AddUint64(&m.stats.TotMergerLoop, 1)

		// ---------------------------------------------
		// Notify ping'ers from the previous loop.

		replyToPings(pings)
		pings = pings[0:0]

		// ---------------------------------------------
		// Wait for new stackDirtyTop entries and/or pings.

		var awakeMergerCh chan struct{}

		m.m.Lock()

		if m.stackDirtyTop == nil || len(m.stackDirtyTop.a) <= 0 {
			m.awakeMergerCh = make(chan struct{})
			awakeMergerCh = m.awakeMergerCh
		}

		m.m.Unlock()

		mergeAll := false

		if awakeMergerCh != nil {
			atomic.AddUint64(&m.stats.TotMergerWaitBeg, 1)

			select {
			case <-m.stopCh:
				return

			case ping := <-m.pingMergerCh:
				pings = append(pings, ping)
				if ping.kind == "mergeAll" {
					mergeAll = true
				}

			case <-awakeMergerCh:
				// NO-OP.
			}

			atomic.AddUint64(&m.stats.TotMergerWaitEnd, 1)
		}

		pings, mergeAll =
			receivePings(m.pingMergerCh, pings, "mergeAll", mergeAll)

		// ---------------------------------------------
		// Atomically ingest stackDirtyTop into stackDirtyMid.

		var stackDirtyTopPrev *segmentStack
		var stackDirtyMidPrev *segmentStack

		stackDirtyMid, _, _, prevLenDirtyMid, prevLenDirtyTop :=
			m.snapshot(snapshotSkipClean|snapshotSkipDirtyBase,
				func(ss *segmentStack) {
					// m.stackDirtyMid takes 1 refs, and
					// stackDirtyMid takes 1 refs.
					ss.refs++

					stackDirtyTopPrev = m.stackDirtyTop
					m.stackDirtyTop = nil

					stackDirtyMidPrev = m.stackDirtyMid
					m.stackDirtyMid = ss

					// Awake any writers that are waiting for more space
					// in stackDirtyTop.
					m.stackDirtyTopCond.Signal()
				})

		stackDirtyTopPrev.Close()
		stackDirtyMidPrev.Close()

		// ---------------------------------------------
		// Merge multiple stackDirtyMid layers.

		if len(stackDirtyMid.a) > 1 {
			newTopLevel := 0

			if !mergeAll {
				// If we have not been asked to merge all segments,
				// then heuristically calc a newTopLevel.
				newTopLevel = stackDirtyMid.calcTargetTopLevel()
			}

			if newTopLevel <= 0 {
				atomic.AddUint64(&m.stats.TotMergerAll, 1)
			}

			atomic.AddUint64(&m.stats.TotMergerInternalBeg, 1)

			mergedStackDirtyMid, err := stackDirtyMid.merge(newTopLevel)
			if err != nil {
				atomic.AddUint64(&m.stats.TotMergerInternalErr, 1)

				m.Logf("collection: runMerger stackDirtyMid.merge,"+
					" newTopLevel: %d, err: %v", newTopLevel, err)

				m.OnError(err)

				continue OUTER
			}

			atomic.AddUint64(&m.stats.TotMergerInternalEnd, 1)

			stackDirtyMid.Close()

			mergedStackDirtyMid.addRef()
			stackDirtyMid = mergedStackDirtyMid

			m.m.Lock()
			stackDirtyMidPrev = m.stackDirtyMid
			m.stackDirtyMid = mergedStackDirtyMid
			m.m.Unlock()

			stackDirtyMidPrev.Close()
		} else {
			atomic.AddUint64(&m.stats.TotMergerInternalSkip, 1)
		}

		lenDirtyMid := len(stackDirtyMid.a)
		if lenDirtyMid > 0 {
			topDirtyMid := stackDirtyMid.a[lenDirtyMid-1]

			m.Logf("collection: runMerger,"+
				" dirtyTop prev height: %2d,"+
				" dirtyMid height: %2d (%2d),"+
				" dirtyMid top (%0.2f kvs cap, %0.2f buf cap) # entries: %d",
				prevLenDirtyTop, lenDirtyMid, lenDirtyMid-prevLenDirtyMid,
				float64(len(topDirtyMid.kvs))/float64(cap(topDirtyMid.kvs)),
				float64(len(topDirtyMid.buf))/float64(cap(topDirtyMid.buf)),
				topDirtyMid.Len())
		}

		stackDirtyMid.Close()

		// ---------------------------------------------
		// Notify persister.

		if m.options.LowerLevelUpdate != nil {
			m.m.Lock()
			if m.stackDirtyBase == nil &&
				m.stackDirtyMid != nil && len(m.stackDirtyMid.a) > 0 {
				atomic.AddUint64(&m.stats.TotMergerLowerLevelNotify, 1)

				m.stackDirtyBase = m.stackDirtyMid
				m.stackDirtyMid = nil

				m.stackDirtyBaseCond.Signal()
			} else {
				atomic.AddUint64(&m.stats.TotMergerLowerLevelNotifySkip, 1)
			}
			m.m.Unlock()
		}

		atomic.AddUint64(&m.stats.TotMergerLoopRepeat, 1)
	}

	// TODO: Concurrent merging of disjoint slices of stackDirtyMid
	// instead of the current, single-threaded merger?
	//
	// TODO: A busy merger means no feeding of the persister?
	//
	// TODO: Delay merger until lots of deletion tombstones?
	//
	// TODO: The base layer is likely the largest, so instead of heap
	// merging the base layer entries, treat the base layer with
	// special case to binary search to find better start points?
	//
	// TODO: Dynamically calc'ed soft max dirty top height, for
	// read-heavy (favor lower) versus write-heavy (favor higher)
	// situations?
}

// ------------------------------------------------------

// Stats returns stats for this collection.
func (m *collection) Stats() (*CollectionStats, error) {
	rv := &CollectionStats{}
	m.stats.AtomicCopyTo(rv)

	var sssDirtyTop *SegmentStackStats
	var sssDirtyMid *SegmentStackStats
	var sssDirtyBase *SegmentStackStats
	var sssClean *SegmentStackStats

	m.m.Lock()

	if m.stackDirtyTop != nil {
		sssDirtyTop = m.stackDirtyTop.Stats()
	}

	if m.stackDirtyMid != nil {
		sssDirtyMid = m.stackDirtyMid.Stats()
	}

	if m.stackDirtyBase != nil {
		sssDirtyBase = m.stackDirtyBase.Stats()
	}

	if m.stackClean != nil {
		sssClean = m.stackClean.Stats()
	}

	m.m.Unlock()

	sssDirty := &SegmentStackStats{}
	sssDirtyTop.AddTo(sssDirty)
	sssDirtyMid.AddTo(sssDirty)
	sssDirtyBase.AddTo(sssDirty)

	rv.CurDirtyOps = sssDirty.CurOps
	rv.CurDirtyBytes = sssDirty.CurBytes
	rv.CurDirtySegments = sssDirty.CurSegments

	if sssDirtyTop != nil {
		rv.CurDirtyTopOps = sssDirtyTop.CurOps
		rv.CurDirtyTopBytes = sssDirtyTop.CurBytes
		rv.CurDirtyTopSegments = sssDirtyTop.CurSegments
	}

	if sssDirtyMid != nil {
		rv.CurDirtyMidOps = sssDirtyMid.CurOps
		rv.CurDirtyMidBytes = sssDirtyMid.CurBytes
		rv.CurDirtyMidSegments = sssDirtyMid.CurSegments
	}

	if sssDirtyBase != nil {
		rv.CurDirtyBaseOps = sssDirtyBase.CurOps
		rv.CurDirtyBaseBytes = sssDirtyBase.CurBytes
		rv.CurDirtyBaseSegments = sssDirtyBase.CurSegments
	}

	if sssClean != nil {
		rv.CurCleanOps = sssClean.CurOps
		rv.CurCleanBytes = sssClean.CurBytes
		rv.CurCleanSegments = sssClean.CurSegments
	}

	return rv, nil
}

// AtomicCopyTo copies stats from s to r (from source to result).
func (s *CollectionStats) AtomicCopyTo(r *CollectionStats) {
	rve := reflect.ValueOf(r).Elem()
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		rvef := rve.Field(i)
		svef := sve.Field(i)
		if rvef.CanAddr() && svef.CanAddr() {
			rvefp := rvef.Addr().Interface()
			svefp := svef.Addr().Interface()
			atomic.StoreUint64(rvefp.(*uint64),
				atomic.LoadUint64(svefp.(*uint64)))
		}
	}
}
