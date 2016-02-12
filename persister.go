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

// runPersister() implements the persister task.
func (m *collection) runPersister() {
	defer close(m.donePersisterCh)

	if m.options.LowerLevelUpdate == nil {
		return
	}

	checkStop := func() bool {
		select {
		case <-m.stopCh:
			return true

		default:
			// NO-OP.
		}

		return false
	}

OUTER:
	for {
		m.m.Lock()

		for m.stackDirtyBase == nil && !checkStop() {
			m.stackDirtyBaseCond.Wait()
		}

		stackDirtyBase := m.stackDirtyBase

		m.m.Unlock()

		if checkStop() {
			return
		}

		llssNext, err := m.options.LowerLevelUpdate(stackDirtyBase)
		if err != nil {
			m.Log("collection: runPersister, LowerLevelUpdate, err: %v", err)

			m.OnError(err)

			continue OUTER
		}

		m.m.Lock()

		stackCleanPrev := m.stackClean
		m.stackClean, m.stackDirtyBase = m.stackDirtyBase, nil

		llssPrev := m.lowerLevelSnapshot
		m.lowerLevelSnapshot = newSnapshotWrapper(llssNext)

		m.m.Unlock()

		stackCleanPrev.Close()

		if llssPrev != nil {
			llssPrev.Close()
		}
	}

	// TODO: More advanced eviction of stackClean.
	// TODO: Timer based eviction of stackClean?
	// TODO: Randomized eviction?
	// TODO: Merging of stackClean to 1 level?
	// TODO: WaitForMerger() also considers stackClean?
	// TODO: Track popular Get() keys?
	// TODO: Track shadowing during merges for writes.
	// TODO: Consider our own simple storage format?
}
