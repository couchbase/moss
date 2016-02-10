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
	ssCh := make(chan *segmentStack)

	defer close(ssCh)

	go m.runPersisterInner(ssCh)

	var ssLast *segmentStack

	// Keep only the last segmentStack, which will happen when the
	// runPersisterInner is busy.
	for {
		select {
		case <-m.stopCh:
			return

		case ssIn := <-m.awakePersisterCh:
			if ssLast != nil {
				ssLast.Close()
			}
			ssLast = ssIn

		case ssCh <- ssLast:
			ssLast = nil
		}
	}
}

func (m *collection) runPersisterInner(ssCh chan *segmentStack) {
	defer close(m.donePersisterCh)

LOOP:
	for ss := range ssCh {
		if m.options.LowerLevelUpdate != nil {
			llssNext, err := m.options.LowerLevelUpdate(ss)
			if err != nil {
				ss.Close()

				m.Log("collection: runPersisterInner, err: %v", err)

				continue LOOP
			}

			m.m.Lock()
			llssPrev := m.lowerLevelSnapshot
			m.lowerLevelSnapshot = newSnapshotWrapper(llssNext)
			m.m.Unlock()

			if llssPrev != nil {
				llssPrev.Close()
			}
		}

		ss.Close()
	}
}
