//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"io"
	"sync"
)

// SnapshotWrapper implements the moss.Snapshot interface.
type SnapshotWrapper struct {
	m        sync.Mutex
	refCount uint64
	ss       Snapshot
	closer   io.Closer // Optional, may be nil.
}

// NewSnapshotWrapper creates a wrapper which provides ref-counting
// around a snapshot.  The snapshot (and an optional io.Closer) will
// be closed when the ref-count reaches zero.
func NewSnapshotWrapper(ss Snapshot, closer io.Closer) *SnapshotWrapper {
	if ss == nil {
		return nil
	}

	return &SnapshotWrapper{refCount: 1, ss: ss, closer: closer}
}

func (w *SnapshotWrapper) addRef() *SnapshotWrapper {
	if w != nil {
		w.m.Lock()
		w.refCount++
		w.m.Unlock()
	}

	return w
}

func (w *SnapshotWrapper) decRef() (err error) {
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

// ChildCollectionNames returns an array of child collection name strings.
func (w *SnapshotWrapper) ChildCollectionNames() ([]string, error) {
	w.m.Lock()
	defer w.m.Unlock()
	if w.ss != nil {
		return w.ss.ChildCollectionNames()
	}
	return nil, nil
}

// ChildCollectionSnapshot returns a Snapshot on a given child
// collection by its name.
func (w *SnapshotWrapper) ChildCollectionSnapshot(childCollectionName string) (
	Snapshot, error) {
	w.m.Lock()
	defer w.m.Unlock()
	if w.ss != nil {
		return w.ss.ChildCollectionSnapshot(childCollectionName)
	}
	return nil, nil
}

// Close will decRef the underlying snapshot.
func (w *SnapshotWrapper) Close() (err error) {
	return w.decRef()
}

// Get returns the key from the underlying snapshot.
func (w *SnapshotWrapper) Get(key []byte, readOptions ReadOptions) (
	[]byte, error) {
	return w.ss.Get(key, readOptions)
}

// StartIterator initiates a start iterator over the underlying snapshot.
func (w *SnapshotWrapper) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions,
) (Iterator, error) {
	return w.ss.StartIterator(startKeyInclusive, endKeyExclusive,
		iteratorOptions)
}
