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
	"errors"
	"testing"
	"time"
)

// execOneBatch is a small helper that Sets a single key/val and
// executes it against the collection.
func execOneBatch(t *testing.T, m Collection, key, val []byte) {
	t.Helper()
	b, err := m.NewBatch(1, len(key)+len(val))
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if err := b.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := m.ExecuteBatch(b, WriteOptions{}); err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}
	b.Close()
}

// TestGetExExistsSemantics verifies GetEx disambiguates missing keys,
// deleted keys, present-normal, and present-empty-value.
func TestGetExExistsSemantics(t *testing.T) {
	m, err := NewCollection(DefaultCollectionOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Populate: normal value, empty value, and a delete tombstone.
	b, _ := m.NewBatch(3, 64)
	if err := b.Set([]byte("normal"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("empty"), []byte{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Del([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if err := m.ExecuteBatch(b, WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	b.Close()

	cases := []struct {
		key        string
		wantExists bool
		wantVal    string
	}{
		{"normal", true, "v"},
		{"empty", true, ""},    // present with empty value
		{"gone", false, ""},    // deletion tombstone
		{"missing", false, ""}, // never inserted
	}
	for _, tc := range cases {
		val, exists, err := m.GetEx([]byte(tc.key), ReadOptions{})
		if err != nil {
			t.Errorf("GetEx(%q) err: %v", tc.key, err)
			continue
		}
		if exists != tc.wantExists {
			t.Errorf("GetEx(%q) exists = %v, want %v (val=%q)",
				tc.key, exists, tc.wantExists, val)
		}
		if string(val) != tc.wantVal {
			t.Errorf("GetEx(%q) val = %q, want %q", tc.key, val, tc.wantVal)
		}
		// Get must stay consistent with GetEx's value.
		gv, _ := m.Get([]byte(tc.key), ReadOptions{})
		if string(gv) != string(val) {
			t.Errorf("Get(%q)=%q disagrees with GetEx val=%q", tc.key, gv, val)
		}
	}
}

// TestGetWithContextCanceled verifies a canceled context short-circuits
// the read.
func TestGetWithContextCanceled(t *testing.T) {
	m, err := NewCollection(DefaultCollectionOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	execOneBatch(t, m, []byte("k"), []byte("v"))

	// A live context returns the value.
	if v, err := m.GetWithContext(context.Background(), []byte("k"), ReadOptions{}); err != nil || string(v) != "v" {
		t.Fatalf("GetWithContext live = %q, %v; want \"v\", nil", v, err)
	}

	// A canceled context returns ctx.Err() without touching the store.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.GetWithContext(ctx, []byte("k"), ReadOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetWithContext canceled err = %v; want context.Canceled", err)
	}
	if _, _, err := m.GetExWithContext(ctx, []byte("k"), ReadOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetExWithContext canceled err = %v; want context.Canceled", err)
	}
}

// TestExecuteBatchWithContextCancelDuringWait proves that a batch
// blocked waiting for the merger (MaxPreMergerBatches reached) aborts
// with ctx.Err() when the context is canceled, rather than blocking
// forever.  The collection is intentionally NOT Start()'ed so no
// merger drains stackDirtyTop.
func TestExecuteBatchWithContextCancelDuringWait(t *testing.T) {
	opts := DefaultCollectionOptions
	opts.MaxPreMergerBatches = 1
	m, err := NewCollection(opts)
	if err != nil {
		t.Fatal(err)
	}
	// Note: not Start()'ed, so no background goroutines are launched
	// and there is nothing to Close(); stackDirtyTop never drains.

	// First batch fills stackDirtyTop to the MaxPreMergerBatches limit.
	execOneBatch(t, m, []byte("k1"), []byte("v1"))

	// Second batch would block waiting for the (absent) merger; cancel
	// the context shortly after starting and confirm it returns.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	b, _ := m.NewBatch(1, 16)
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	done := make(chan error, 1)
	go func() {
		done <- m.ExecuteBatchWithContext(ctx, b, WriteOptions{})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ExecuteBatchWithContext err = %v; want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ExecuteBatchWithContext did not return after context cancel (blocked)")
	}
}

// TestSnapshotContextAndGetEx exercises the new context-aware and
// not-found methods on the Snapshot interface (here backed by a
// collection's segmentStack snapshot).
func TestSnapshotContextAndGetEx(t *testing.T) {
	m, err := NewCollection(DefaultCollectionOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	b, _ := m.NewBatch(2, 32)
	_ = b.Set([]byte("normal"), []byte("v"))
	_ = b.Del([]byte("gone"))
	if err := m.ExecuteBatch(b, WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	b.Close()

	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// GetEx exists semantics at the Snapshot level.
	if v, exists, err := ss.GetEx([]byte("normal"), ReadOptions{}); err != nil || !exists || string(v) != "v" {
		t.Fatalf("ss.GetEx(normal) = %q,%v,%v; want \"v\",true,nil", v, exists, err)
	}
	if _, exists, err := ss.GetEx([]byte("gone"), ReadOptions{}); err != nil || exists {
		t.Fatalf("ss.GetEx(gone) exists = %v; want false", exists)
	}
	if _, exists, err := ss.GetEx([]byte("missing"), ReadOptions{}); err != nil || exists {
		t.Fatalf("ss.GetEx(missing) exists = %v; want false", exists)
	}

	// Canceled context short-circuits Snapshot reads/iterators.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ss.GetWithContext(ctx, []byte("normal"), ReadOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ss.GetWithContext canceled err = %v; want context.Canceled", err)
	}
	if _, _, err := ss.GetExWithContext(ctx, []byte("normal"), ReadOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ss.GetExWithContext canceled err = %v; want context.Canceled", err)
	}
	if _, err := ss.StartIteratorWithContext(ctx, nil, nil, IteratorOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ss.StartIteratorWithContext canceled err = %v; want context.Canceled", err)
	}

	// A live context still produces a working iterator.
	it, err := ss.StartIteratorWithContext(context.Background(), nil, nil, IteratorOptions{})
	if err != nil {
		t.Fatalf("ss.StartIteratorWithContext live err: %v", err)
	}
	k, v, err := it.Current()
	if err != nil || string(k) != "normal" || string(v) != "v" {
		t.Fatalf("iterator Current = %q,%q,%v; want normal,v,nil", k, v, err)
	}
	it.Close()
}

// TestExecuteBatchWithContextPreCanceled verifies an already-canceled
// context is rejected up front.
func TestExecuteBatchWithContextPreCanceled(t *testing.T) {
	m, err := NewCollection(DefaultCollectionOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b, _ := m.NewBatch(1, 16)
	_ = b.Set([]byte("k"), []byte("v"))
	defer b.Close()

	if err := m.ExecuteBatchWithContext(ctx, b, WriteOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled ExecuteBatchWithContext err = %v; want context.Canceled", err)
	}
}
