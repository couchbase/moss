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

// Snapshot returns a stable snapshot of the key-value entries.
func (m *collection) Snapshot() (Snapshot, error) {
	rv := &segmentStack{}

	m.m.Lock()

	rv.a = make([]*segment, 0, m.stackOpenBaseLenLOCKED())

	if m.stackBase != nil {
		rv.a = append(rv.a, m.stackBase.a...)
	}
	if m.stackOpen != nil {
		rv.a = append(rv.a, m.stackOpen.a...)
	}

	m.m.Unlock()

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

	bsorted := b.sort()

	stackOpen := &segmentStack{}

	m.m.Lock()

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

func (m *collection) Log(format string, a ...interface{}) {
	if m.options.Log == nil {
		return
	}

	if m.options.Debug > 0 {
		m.options.Log(format, a...)
	}
}

// ------------------------------------------------------

func (m *collection) stackOpenBaseLenLOCKED() int {
	rv := 0
	if m.stackOpen != nil {
		rv += len(m.stackOpen.a)
	}
	if m.stackBase != nil {
		rv += len(m.stackBase.a)
	}
	return rv
}

// ------------------------------------------------------

func (m *collection) runMerger() {
	defer close(m.doneMergerCh)

	minMergePercentage := m.options.MinMergePercentage
	if minMergePercentage <= 0 {
		minMergePercentage = DefaultCollectionOptions.MinMergePercentage
	}

	pings := []ping{}

	replyToPings := func(pings []ping) {
		for _, ping := range pings {
			if ping.pongCh != nil {
				close(ping.pongCh)
			}
		}
	}

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
		// Wait for new stackOpen entries.

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

	COLLECT_MORE_PINGS:
		for {
			select {
			case ping := <-m.pingMergerCh:
				pings = append(pings, ping)
				if ping.kind == "mergeAll" {
					mergeAll = true
				}

			default:
				break COLLECT_MORE_PINGS
			}
		}

		// ---------------------------------------------
		// Atomically ingest stackOpen into stackBase.

		dirty := false

		stackBase := &segmentStack{}

		m.m.Lock()

		stackBase.a = make([]*segment, 0, m.stackOpenBaseLenLOCKED())

		if m.stackBase != nil {
			stackBase.a = append(stackBase.a, m.stackBase.a...)
		}

		if m.stackOpen != nil {
			stackBase.a = append(stackBase.a, m.stackOpen.a...)
			m.stackOpen = nil

			dirty = true
		}

		m.stackBase = stackBase

		m.m.Unlock()

		// ---------------------------------------------
		// Merge multiple stackBase layers.

		if len(stackBase.a) > 1 {
			newTopLevel := 0

			// If we have not been asked to merge all segments, then
			// compute a newTopLevel based on minMergePercentage's.
			if !mergeAll {
				maxTopLevel := len(stackBase.a) - 2
				for newTopLevel < maxTopLevel {
					numX0 := len(stackBase.a[newTopLevel].kvs)
					numX1 := len(stackBase.a[newTopLevel+1].kvs)
					if (float64(numX1) / float64(numX0)) > minMergePercentage {
						break
					}

					newTopLevel += 1
				}
			}

			nextStackBase, err := stackBase.merge(newTopLevel)
			if err != nil {
				// TODO: err handling.
				continue OUTER
			}

			m.m.Lock()

			m.stackBase = nextStackBase

			soHeight := 0
			if m.stackOpen != nil {
				soHeight = len(m.stackOpen.a)
			}

			m.m.Unlock()

			stackBase = nextStackBase

			m.Log("stackBase height: %d,"+
				" stackBase top size: %d,"+
				" stackOpen height: %d\n",
				len(nextStackBase.a),
				len(nextStackBase.a[len(nextStackBase.a)-1].kvs)/2,
				soHeight)
		}

		// ---------------------------------------------
		// Optionally notify persister.

		if m.awakePersisterCh != nil && dirty {
			m.awakePersisterCh <- stackBase
		}
	}

	// TODO: Delay merger until lots of deletion tombstones?
	//
	// TODO: The base layer is likely the largest, so instead of heap
	// merging the base layer entries, treat the base layer with
	// special case to binary search to find better start points?
}

// ------------------------------------------------------

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
