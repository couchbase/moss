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

// ------------------------------------------------------

// Start kicks off required background gouroutines.
func (m *collection) Start() error {
	go m.runMerger()
	go m.runPersister()
	return nil
}

// Close synchronously stops background goroutines.
func (m *collection) Close() error {
	close(m.stopCh)

	m.stackDirtyBaseCond.Signal() // Awake persister.

	<-m.doneMergerCh
	<-m.donePersisterCh

	m.m.Lock()
	if m.lowerLevelSnapshot != nil {
		m.lowerLevelSnapshot.Close()
		m.lowerLevelSnapshot = nil
	}
	m.m.Unlock()

	return nil
}

// Options returns the current options.
func (m *collection) Options() CollectionOptions {
	return m.options
}

// Snapshot returns a stable snapshot of the key-value entries.
func (m *collection) Snapshot() (Snapshot, error) {
	rv, _, _, _, _ := m.snapshot(0, nil)

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
func (m *collection) ExecuteBatch(bIn Batch,
	writeOptions WriteOptions) error {
	b, ok := bIn.(*segment)
	if !ok {
		return fmt.Errorf("wrong Batch implementation type")
	}

	if b == nil || len(b.kvs) <= 0 {
		return nil
	}

	maxStackDirtyTopHeight := m.options.MaxStackDirtyTopHeight
	if maxStackDirtyTopHeight <= 0 {
		maxStackDirtyTopHeight =
			DefaultCollectionOptions.MaxStackDirtyTopHeight
	}

	sort.Sort(b)

	stackDirtyTop := &segmentStack{collection: m, refs: 1}

	m.m.Lock()

	for m.stackDirtyTop != nil &&
		len(m.stackDirtyTop.a) >= maxStackDirtyTopHeight {
		m.stackDirtyTopCond.Wait()
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
		close(awakeMergerCh)
	}

	return nil
}

// ------------------------------------------------------

// WaitForMerger blocks until the merger has run another cycle.
// Providing a kind of "mergeAll" forces a full merge and can be
// useful for applications that are no longer performing mutations and
// that want to optimize for retrievals.
func (m *collection) WaitForMerger(kind string) error {
	pongCh := make(chan struct{})
	m.pingMergerCh <- ping{
		kind:   kind,
		pongCh: pongCh,
	}
	<-pongCh
	return nil
}

// ------------------------------------------------------

// Log invokes the user's configured Log callback, if any, if the
// debug levels are met.
func (m *collection) Log(format string, a ...interface{}) {
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

	return rv, heightClean, heightDirtyBase, heightDirtyMid, heightDirtyTop
}

// ------------------------------------------------------

// runMerger() implements the background merger task.
func (m *collection) runMerger() {
	defer close(m.doneMergerCh)

	pings := []ping{}

	defer func() {
		replyToPings(pings)
		pings = pings[0:0]
	}()

OUTER:
	for {
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
		}

		pings, mergeAll =
			receivePings(m.pingMergerCh, pings, "mergeAll", mergeAll)

		// ---------------------------------------------
		// Atomically ingest stackDirtyTop into stackDirtyMid.

		var stackDirtyTopPrev *segmentStack
		var stackDirtyMidPrev *segmentStack

		stackDirtyMid, _, _, htWasDirtyMid, htWasDirtyTop :=
			m.snapshot(snapshotSkipClean|snapshotSkipDirtyBase,
				func(ss *segmentStack) {
					// m.stackDirtyMid takes 1 refs, and
					// stackDirtyMid takes 1 refs.
					ss.refs += 1

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

			mergedStackDirtyMid, err := stackDirtyMid.merge(newTopLevel)
			if err != nil {
				m.Log("collection: runMerger stackDirtyMid.merge,"+
					" newTopLevel: %d, err: %v", newTopLevel, err)

				m.OnError(err)

				continue OUTER
			}

			stackDirtyMid.Close()

			mergedStackDirtyMid.addRef()
			stackDirtyMid = mergedStackDirtyMid

			m.m.Lock()
			stackDirtyMidPrev = m.stackDirtyMid
			m.stackDirtyMid = mergedStackDirtyMid
			m.m.Unlock()

			stackDirtyMidPrev.Close()
		}

		m.Log("collection: runMerger,"+
			" prev dirtyMid height: %d,"+
			" prev dirtyTop height: %d,"+
			" dirtyMid height: %d,"+
			" dirtyMid top size: %d",
			htWasDirtyMid,
			htWasDirtyTop,
			len(stackDirtyMid.a),
			len(stackDirtyMid.a[len(stackDirtyMid.a)-1].kvs)/2)

		stackDirtyMid.Close()

		// ---------------------------------------------
		// Notify persister.

		if m.options.LowerLevelUpdate != nil {
			m.m.Lock()
			if m.stackDirtyBase == nil &&
				m.stackDirtyMid != nil && len(m.stackDirtyMid.a) > 0 {
				m.stackDirtyBase = m.stackDirtyMid
				m.stackDirtyMid = nil

				m.stackDirtyBaseCond.Signal()
			}
			m.m.Unlock()
		}
	}

	// TODO: Concurrent merging goroutines of disjoint slices of the
	// stackDirtyMid instead of the current, single-threaded merger.
	//
	// TODO: Delay merger until lots of deletion tombstones?
	//
	// TODO: The base layer is likely the largest, so instead of heap
	// merging the base layer entries, treat the base layer with
	// special case to binary search to find better start points?
}
