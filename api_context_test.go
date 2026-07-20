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
