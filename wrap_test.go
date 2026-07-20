//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"sync"
	"sync/atomic"
	"testing"
)

// fakeSnapshot is a minimal Snapshot implementation for exercising
// SnapshotWrapper in isolation.  It records how many times it was
// Close()'d so tests can assert ref-count discipline.
type fakeSnapshot struct {
	closes int32
}

func (f *fakeSnapshot) Close() error {
	atomic.AddInt32(&f.closes, 1)
	return nil
}

func (f *fakeSnapshot) Get(key []byte, readOptions ReadOptions) ([]byte, error) {
	return []byte("v"), nil
}

func (f *fakeSnapshot) StartIterator(startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	return &iteratorSingle{op: 0}, nil
}

func (f *fakeSnapshot) ChildCollectionNames() ([]string, error) { return nil, nil }

func (f *fakeSnapshot) ChildCollectionSnapshot(childCollectionName string) (
	Snapshot, error) {
	return nil, nil
}

// TestSnapshotWrapperConcurrentGet drives Get/StartIterator/addRef/
// decRef concurrently.  Run with -race it proves the w.ss access in
// Get/StartIterator is properly synchronized against the decRef() that
// nils w.ss (previously those two methods read w.ss with no lock).
func TestSnapshotWrapperConcurrentGet(t *testing.T) {
	fake := &fakeSnapshot{}
	w := NewSnapshotWrapper(fake, nil)
	if w == nil {
		t.Fatal("expected non-nil wrapper")
	}

	const workers = 16
	const iters = 500

	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				// Hold an outer ref while reading, mirroring real callers.
				w.addRef()
				if v, err := w.Get([]byte("k"), ReadOptions{}); err != nil || string(v) != "v" {
					t.Errorf("Get = %q, %v; want \"v\", nil", v, err)
				}
				if _, err := w.StartIterator(nil, nil, IteratorOptions{}); err != nil {
					t.Errorf("StartIterator err: %v", err)
				}
				if err := w.decRef(); err != nil {
					t.Errorf("decRef err: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	// The original creation ref is still held, so the snapshot must not
	// have been closed yet.
	if c := atomic.LoadInt32(&fake.closes); c != 0 {
		t.Fatalf("snapshot closed prematurely: closes=%d", c)
	}

	// Drop the final ref; now it should close exactly once.
	if err := w.Close(); err != nil {
		t.Fatalf("Close err: %v", err)
	}
	if c := atomic.LoadInt32(&fake.closes); c != 1 {
		t.Fatalf("expected exactly 1 close, got %d", c)
	}
}

// TestSnapshotWrapperClosedReturnsErrClosed verifies that reads on a
// fully-closed wrapper return ErrClosed instead of panicking with a
// nil-pointer dereference on w.ss.
func TestSnapshotWrapperClosedReturnsErrClosed(t *testing.T) {
	w := NewSnapshotWrapper(&fakeSnapshot{}, nil)
	if err := w.Close(); err != nil {
		t.Fatalf("Close err: %v", err)
	}

	if _, err := w.Get([]byte("k"), ReadOptions{}); err != ErrClosed {
		t.Fatalf("Get on closed wrapper err = %v; want ErrClosed", err)
	}
	if _, err := w.StartIterator(nil, nil, IteratorOptions{}); err != ErrClosed {
		t.Fatalf("StartIterator on closed wrapper err = %v; want ErrClosed", err)
	}
}
