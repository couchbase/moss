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
	"io"
	"sync"
)

type snapshotWrapper struct {
	m        sync.Mutex
	refCount uint64
	ss       Snapshot
	closer   io.Closer // Optional, may be nil.
}

// NewSnapshotWrapper creates a wrapper which provides ref-counting
// around a snapshot.  The snapshot (and an optional io.Closer) will
// be closed when the ref-count reaches zero.
func NewSnapshotWrapper(ss Snapshot, closer io.Closer) *snapshotWrapper {
	if ss == nil {
		return nil
	}

	return &snapshotWrapper{refCount: 1, ss: ss, closer: closer}
}

func (w *snapshotWrapper) addRef() *snapshotWrapper {
	if w != nil {
		w.m.Lock()
		w.refCount++
		w.m.Unlock()
	}

	return w
}

func (w *snapshotWrapper) decRef() (err error) {
	w.m.Lock()
	w.refCount--
	if w.refCount <= 0 {
		if w.ss != nil {
			err = w.ss.Close()
			w.ss = nil
		}
		if w.closer != nil {
			w.closer.Close()
			w.closer = nil
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
	return w.ss.Get(key, readOptions)
}

func (w *snapshotWrapper) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions,
) (Iterator, error) {
	return w.ss.StartIterator(startKeyInclusive, endKeyExclusive,
		iteratorOptions)
}

// --------------------------------------------------------

type iteratorWrapper struct {
	iter   Iterator
	closer io.Closer
}

func (w *iteratorWrapper) Close() error {
	w.iter.Close()
	w.closer.Close()
	return nil
}

func (w *iteratorWrapper) Next() error {
	return w.iter.Next()
}

func (w *iteratorWrapper) Current() (key, val []byte, err error) {
	return w.iter.Current()
}

func (w *iteratorWrapper) CurrentEx() (entryEx EntryEx, key, val []byte, err error) {
	return w.iter.CurrentEx()
}
