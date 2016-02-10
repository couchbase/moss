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
	"sync"
)

type snapshotWrapper struct {
	m        sync.Mutex
	refCount uint64
	obj      Snapshot
}

func newSnapshotWrapper(obj Snapshot) *snapshotWrapper {
	if obj == nil {
		return nil
	}

	return &snapshotWrapper{
		refCount: 1,
		obj:      obj,
	}
}

func (w *snapshotWrapper) addRef() *snapshotWrapper {
	if w != nil {
		w.m.Lock()
		w.refCount += 1
		w.m.Unlock()
	}

	return w
}

func (w *snapshotWrapper) decRef() (err error) {
	w.m.Lock()
	w.refCount -= 1
	if w.refCount <= 0 {
		if w.obj != nil {
			err = w.obj.Close()
			w.obj = nil
		}
	}
	w.m.Unlock()
	return err
}

func (w *snapshotWrapper) Close() (err error) {
	return w.decRef()
}

func (w *snapshotWrapper) Get(key []byte, readOptions ReadOptions) (
	[]byte, error) {
	return w.obj.Get(key, readOptions)
}

func (w *snapshotWrapper) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions,
) (Iterator, error) {
	return w.obj.StartIterator(startKeyInclusive, endKeyExclusive,
		iteratorOptions)
}
