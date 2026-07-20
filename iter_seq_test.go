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
	"sync/atomic"
	"testing"
)

func newSeqTestSnapshot(t *testing.T) (Collection, Snapshot) {
	t.Helper()
	m, err := NewCollection(DefaultCollectionOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	b, _ := m.NewBatch(4, 64)
	_ = b.Set([]byte("a"), []byte("1"))
	_ = b.Set([]byte("b"), []byte("2"))
	_ = b.Set([]byte("c"), []byte("3"))
	_ = b.Set([]byte("d"), []byte("4"))
	if err := m.ExecuteBatch(b, WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	b.Close()
	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return m, ss
}

func TestAllRangeOverFunc(t *testing.T) {
	m, ss := newSeqTestSnapshot(t)
	defer m.Close()
	defer ss.Close()

	// Full range, in sorted key order.
	seq, errFn := All(ss, nil, nil, IteratorOptions{})
	var got []string
	for k, v := range seq {
		got = append(got, string(k)+"="+string(v))
	}
	if err := errFn(); err != nil {
		t.Fatalf("errFn: %v", err)
	}
	want := []string{"a=1", "b=2", "c=3", "d=4"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestAllRangeBoundedAndBreak(t *testing.T) {
	m, ss := newSeqTestSnapshot(t)
	defer m.Close()
	defer ss.Close()

	// Bounded range [b, d): yields b, c only (d is exclusive).
	seq, errFn := All(ss, []byte("b"), []byte("d"), IteratorOptions{})
	var got []string
	for k := range seq {
		got = append(got, string(k))
	}
	if err := errFn(); err != nil {
		t.Fatalf("errFn: %v", err)
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("bounded range got %v, want [b c]", got)
	}

	// Breaking out of the loop early is not an error.
	seq2, errFn2 := All(ss, nil, nil, IteratorOptions{})
	n := 0
	for range seq2 {
		n++
		if n == 2 {
			break
		}
	}
	if err := errFn2(); err != nil {
		t.Fatalf("break-out should not be an error, got: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected to stop after 2, got %d", n)
	}
}

func TestAllWithContextCanceled(t *testing.T) {
	m, ss := newSeqTestSnapshot(t)
	defer m.Close()
	defer ss.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	seq, errFn := AllWithContext(ctx, ss, nil, nil, IteratorOptions{})
	n := 0
	for range seq {
		n++
	}
	if n != 0 {
		t.Fatalf("canceled context should yield nothing, got %d entries", n)
	}
	if err := errFn(); !errors.Is(err, context.Canceled) {
		t.Fatalf("errFn = %v; want context.Canceled", err)
	}
}

// TestAtomicCopyToCopiesCounters confirms the Kind-guarded AtomicCopyTo
// still copies the uint64 counters correctly.
func TestAtomicCopyToCopiesCounters(t *testing.T) {
	s := &CollectionStats{}
	atomic.AddUint64(&s.TotGet, 7)
	atomic.AddUint64(&s.TotExecuteBatchEnd, 3)
	atomic.AddUint64(&s.CurDirtyOps, 11)

	r := &CollectionStats{}
	s.AtomicCopyTo(r)

	if r.TotGet != 7 || r.TotExecuteBatchEnd != 3 || r.CurDirtyOps != 11 {
		t.Fatalf("AtomicCopyTo mismatch: TotGet=%d TotExecuteBatchEnd=%d CurDirtyOps=%d",
			r.TotGet, r.TotExecuteBatchEnd, r.CurDirtyOps)
	}
}
