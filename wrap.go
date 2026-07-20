//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"context"
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
	return w.GetWithContext(context.Background(), key, readOptions)
}

// GetWithContext returns the key from the underlying snapshot, honoring
// the provided context.
func (w *SnapshotWrapper) GetWithContext(ctx context.Context, key []byte,
	readOptions ReadOptions) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Take our own ref under the lock so w.ss can't be closed/niled by
	// a concurrent decRef() while we're reading it, then release the
	// lock before the (potentially slow) underlying Get.  This matches
	// the mutex discipline of ChildCollectionNames/Snapshot below.
	ss, err := w.acquire()
	if err != nil {
		return nil, err
	}
	defer w.decRef()
	return ss.GetWithContext(ctx, key, readOptions)
}

// GetEx is like Get but also reports whether the key exists.
func (w *SnapshotWrapper) GetEx(key []byte, readOptions ReadOptions) (
	[]byte, bool, error) {
	return w.GetExWithContext(context.Background(), key, readOptions)
}

// GetExWithContext is like GetEx, honoring the provided context.
func (w *SnapshotWrapper) GetExWithContext(ctx context.Context, key []byte,
	readOptions ReadOptions) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	ss, err := w.acquire()
	if err != nil {
		return nil, false, err
	}
	defer w.decRef()
	return ss.GetExWithContext(ctx, key, readOptions)
}

// StartIterator initiates a start iterator over the underlying snapshot.
func (w *SnapshotWrapper) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions,
) (Iterator, error) {
	return w.StartIteratorWithContext(context.Background(),
		startKeyInclusive, endKeyExclusive, iteratorOptions)
}

// StartIteratorWithContext initiates an iterator over the underlying
// snapshot, honoring the provided context.
func (w *SnapshotWrapper) StartIteratorWithContext(ctx context.Context,
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions,
) (Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ss, err := w.acquire()
	if err != nil {
		return nil, err
	}
	defer w.decRef()
	return ss.StartIteratorWithContext(ctx, startKeyInclusive, endKeyExclusive,
		iteratorOptions)
}

// acquire returns the underlying snapshot with an extra ref-count
// held, or ErrClosed if the wrapper has already been closed.  The
// caller must balance a successful acquire() with a decRef().
func (w *SnapshotWrapper) acquire() (Snapshot, error) {
	w.m.Lock()
	defer w.m.Unlock()
	if w.ss == nil {
		return nil, ErrClosed
	}
	w.refCount++
	return w.ss, nil
}
