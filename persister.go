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

OUTER:
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

		if m.options.LowerLevelUpdate != nil {
			llssNext, err := m.options.LowerLevelUpdate(persistableSS)
			if err != nil {
				persistableSS.Close()

				m.Log("collection: runPersister, err: %v", err)

				continue OUTER
			}

			m.m.Lock()
			llssPrev := m.lowerLevelSnapshot
			m.lowerLevelSnapshot = newSnapshotWrapper(llssNext)
			m.m.Unlock()

			if llssPrev != nil {
				llssPrev.Close()
			}
		}

		persistableSS.Close()
	}
}
