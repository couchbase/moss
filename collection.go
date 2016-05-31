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
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
	// stackDirtyTop, it can notify waiters like the merger via
	// waitDirtyIncomingCh (if non-nil).
	waitDirtyIncomingCh chan struct{}

	// When the persister has finished a persistence cycle, it can
	// notify waiters like the merger via waitDirtyOutgoingCh (if
	// non-nil).
	waitDirtyOutgoingCh chan struct{}

	// ----------------------------------------

	// stackDirtyTopCond is used to wait for space in stackDirtyTop.
	stackDirtyTopCond *sync.Cond

	// stackDirtyBaseCond is used to wait for non-nil stackDirtyBase.
	stackDirtyBaseCond *sync.Cond

	// ----------------------------------------

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
	m.fireEvent(EventKindCloseStart, 0)
	startTime := time.Now()
	defer func() {
		m.fireEvent(EventKindClose, time.Now().Sub(startTime))
	}()

	atomic.AddUint64(&m.stats.TotCloseBeg, 1)

	m.m.Lock()

	close(m.stopCh)

	m.stackDirtyTopCond.Broadcast()  // Awake all ExecuteBatch()'ers.
	m.stackDirtyBaseCond.Broadcast() // Awake persister.

	m.m.Unlock()

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

	stackDirtyTopPrev := m.stackDirtyTop
	m.stackDirtyTop = nil

	stackDirtyMidPrev := m.stackDirtyMid
	m.stackDirtyMid = nil

	stackDirtyBasePrev := m.stackDirtyBase
	m.stackDirtyBase = nil

	stackCleanPrev := m.stackClean
	m.stackClean = nil

	m.m.Unlock()

	stackDirtyTopPrev.Close()
	stackDirtyMidPrev.Close()
	stackDirtyBasePrev.Close()
	stackCleanPrev.Close()

	atomic.AddUint64(&m.stats.TotCloseEnd, 1)

	return nil
}

func (m *collection) isClosed() bool {
	select {
	case <-m.stopCh:
		return true
	default:
		return false
	}
}

// Options returns the current options.
func (m *collection) Options() CollectionOptions {
	return m.options
}

// Snapshot returns a stable snapshot of the key-value entries.
func (m *collection) Snapshot() (Snapshot, error) {
	if m.isClosed() {
		return nil, ErrClosed
	}

	atomic.AddUint64(&m.stats.TotSnapshotBeg, 1)

	rv, _, _, _, _ := m.snapshot(0, nil)

	atomic.AddUint64(&m.stats.TotSnapshotEnd, 1)

	return rv, nil
}

// NewBatch returns a new Batch instance with hinted amount of
// resources expected to be required.
func (m *collection) NewBatch(totalOps, totalKeyValBytes int) (
	Batch, error) {
	if m.isClosed() {
		return nil, ErrClosed
	}

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
	startTime := time.Now()
	defer func() {
		m.fireEvent(EventKindBatchExecute, time.Now().Sub(startTime))
	}()

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

	stackDirtyTop := &segmentStack{options: &m.options, refs: 1}

	// notify interested handlers that we are about to execute this batch
	m.fireEvent(EventKindBatchExecuteStart, 0)

	m.m.Lock()

	for m.stackDirtyTop != nil &&
		len(m.stackDirtyTop.a) >= maxPreMergerBatches {
		if m.isClosed() {
			m.m.Unlock()
			return ErrClosed
		}

		if m.options.DeferredSort {
			go b.RequestSort(false) // While waiting, might as well sort.
		}

		atomic.AddUint64(&m.stats.TotExecuteBatchWaitBeg, 1)
		m.stackDirtyTopCond.Wait()
		atomic.AddUint64(&m.stats.TotExecuteBatchWaitEnd, 1)
	}

	// check again, could have been closed while waiting
	if m.isClosed() {
		m.m.Unlock()
		return ErrClosed
	}

	numDirtyTop := 0
	if m.stackDirtyTop != nil {
		numDirtyTop = len(m.stackDirtyTop.a)
	}

	stackDirtyTop.a = make([]Segment, 0, numDirtyTop+1)

	if m.stackDirtyTop != nil {
		stackDirtyTop.a = append(stackDirtyTop.a, m.stackDirtyTop.a...)
	}

	stackDirtyTop.a = append(stackDirtyTop.a, b)

	prevStackDirtyTop := m.stackDirtyTop
	m.stackDirtyTop = stackDirtyTop

	waitDirtyIncomingCh := m.waitDirtyIncomingCh
	m.waitDirtyIncomingCh = nil

	m.m.Unlock()

	prevStackDirtyTop.Close()

	if waitDirtyIncomingCh != nil {
		atomic.AddUint64(&m.stats.TotExecuteBatchAwakeMergerBeg, 1)
		close(waitDirtyIncomingCh)
		atomic.AddUint64(&m.stats.TotExecuteBatchAwakeMergerEnd, 1)
	}

	atomic.AddUint64(&m.stats.TotExecuteBatchEnd, 1)

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

func (m *collection) fireEvent(kind EventKind, dur time.Duration) {
	if m.options.OnEvent != nil {
		m.options.OnEvent(Event{Kind: kind, Collection: m, Duration: dur})
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

	rv := &segmentStack{options: &m.options, refs: 1}

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

	rv.a = make([]Segment, 0,
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
