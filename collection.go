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
)

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

// Options returns the current options.
func (m *collection) Options() CollectionOptions {
	return m.options
}

// Snapshot returns a stable snapshot of the key-value entries.
func (m *collection) Snapshot() (Snapshot, error) {
	rv, _, _ := m.snapshot(nil)

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

	maxStackOpenHeight := m.options.MaxStackOpenHeight
	if maxStackOpenHeight <= 0 {
		maxStackOpenHeight = DefaultCollectionOptions.MaxStackOpenHeight
	}

	bsorted := b.sort()

	stackOpen := &segmentStack{collection: m}

	m.m.Lock()

	for m.stackOpen != nil && len(m.stackOpen.a) >= maxStackOpenHeight {
		m.stackOpenCond.Wait()
	}

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
	if m.options.Log == nil {
		return
	}

	if m.options.Debug > 0 {
		m.options.Log(format, a...)
	}
}

// ------------------------------------------------------

// snapshot() atomically clones the stackBase and stackOpen into a new
// segmentStack, and also invokes the optional callback while holding
// the collection lock.
func (m *collection) snapshot(cb func(*segmentStack)) (
	*segmentStack, int, int) {
	rv := &segmentStack{collection: m}

	numBase := 0
	numOpen := 0

	m.m.Lock()

	if m.stackBase != nil {
		numBase = len(m.stackBase.a)
	}

	if m.stackOpen != nil {
		numOpen = len(m.stackOpen.a)
	}

	rv.a = make([]*segment, 0, numBase+numOpen)

	if m.stackBase != nil {
		rv.a = append(rv.a, m.stackBase.a...)
	}

	if m.stackOpen != nil {
		rv.a = append(rv.a, m.stackOpen.a...)
	}

	if cb != nil {
		cb(rv)
	}

	m.m.Unlock()

	return rv, numBase, numOpen
}

// ------------------------------------------------------

// runMerger() is the merger goroutine.
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
		// Wait for new stackOpen entries and/or pings.

		var awakeMergerCh chan struct{}

		m.m.Lock()

		if m.stackOpen == nil || len(m.stackOpen.a) <= 0 {
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
		// Atomically ingest stackOpen into m.stackBase.

		stackBase, numWasBase, numWasOpen :=
			m.snapshot(func(ss *segmentStack) {
				m.stackOpen = nil
				m.stackBase = ss

				// Awake any writers waiting for more stackOpen space.
				m.stackOpenCond.Signal()
			})

		// ---------------------------------------------
		// Merge multiple stackBase layers.

		if len(stackBase.a) > 1 {
			newTopLevel := 0

			if !mergeAll {
				// If we have not been asked to merge all segments,
				// then heuristically calc a newTopLevel.
				newTopLevel = stackBase.calcTargetTopLevel()
			}

			mergedStackBase, err := stackBase.merge(newTopLevel)
			if err != nil {
				// TODO: err handling.

				m.Log("collection: runMerger stackBase.merge,"+
					" newTopLevel: %d, err: %v",
					newTopLevel, err)

				continue OUTER
			}

			m.m.Lock()
			m.stackBase = mergedStackBase
			m.m.Unlock()

			stackBase = mergedStackBase
		}

		m.Log("prev stackBase height: %d,"+
			" prev stackOpen height: %d"+
			" stackBase height: %d,"+
			" stackBase top size: %d",
			numWasBase, numWasOpen,
			len(stackBase.a),
			len(stackBase.a[len(stackBase.a)-1].kvs)/2)

		// ---------------------------------------------
		// Optionally notify persister.

		if m.awakePersisterCh != nil && numWasOpen > 0 {
			m.awakePersisterCh <- stackBase
		}
	}

	// TODO: Concurrent merging goroutines of disjoint slices of the
	// stackBase instead of the current, single-threaded merger.
	//
	// TODO: Delay merger until lots of deletion tombstones?
	//
	// TODO: The base layer is likely the largest, so instead of heap
	// merging the base layer entries, treat the base layer with
	// special case to binary search to find better start points?
}

// ------------------------------------------------------

// runPersister() is the persister goroutine.
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

// replyToPings() is a helper funciton to respond to ping requests.
func replyToPings(pings []ping) {
	for _, ping := range pings {
		if ping.pongCh != nil {
			close(ping.pongCh)
			ping.pongCh = nil
		}
	}
}

// receivePings() collects any available ping requests, but will not
// block if there are no incoming ping requests.
func receivePings(pingCh chan ping, pings []ping,
	kindMatch string, kindSeen bool) ([]ping, bool) {
	for {
		select {
		case ping := <-pingCh:
			pings = append(pings, ping)
			if ping.kind == kindMatch {
				kindSeen = true
			}

		default:
			return pings, kindSeen
		}
	}
}
