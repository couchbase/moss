//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
)

// Tests for the DelRange range-tombstone feature.  Keys are zero-padded so
// lexicographic order matches numeric order, letting DelRange bounds map to
// contiguous integer ranges.

func drKey(i int) []byte { return []byte(fmt.Sprintf("%05d", i)) }
func drVal(i int) []byte { return []byte(fmt.Sprintf("v%d", i)) }

// drExec runs fn against a fresh batch and executes it.
func drExec(t *testing.T, m Collection, fn func(b Batch)) {
	t.Helper()
	b, err := m.NewBatch(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn(b)
	if err := m.ExecuteBatch(b, WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	b.Close()
}

// drCheckRange asserts, over a snapshot, that keys in [delLo, delHi) are
// absent and all other keys in [0, n) are present with their expected value,
// via both point Get and a full forward iteration.
func drCheckRange(t *testing.T, ss Snapshot, n, delLo, delHi int) {
	t.Helper()

	deleted := func(i int) bool { return i >= delLo && i < delHi }

	// Point Get.
	for i := 0; i < n; i++ {
		v, err := ss.Get(drKey(i), ReadOptions{})
		if err != nil {
			t.Fatalf("Get(%d) err: %v", i, err)
		}
		if deleted(i) {
			if v != nil {
				t.Fatalf("key %d should be range-deleted, got %q", i, v)
			}
		} else {
			if string(v) != string(drVal(i)) {
				t.Fatalf("key %d = %q, want %q", i, v, drVal(i))
			}
		}
	}

	// Full iteration.
	iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	want := 0
	for i := 0; i < n; i++ {
		if !deleted(i) {
			want++
		}
	}

	got := 0
	for {
		k, v, err := iter.Current()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got++
		if len(v) == 0 {
			t.Fatalf("iterated empty val at key %q", k)
		}
		if err := iter.Next(); err == ErrIteratorDone {
			break
		}
	}
	if got != want {
		t.Fatalf("iterated %d live keys, want %d", got, want)
	}
}

func drNewInMem(t *testing.T, co CollectionOptions) Collection {
	t.Helper()
	m, err := NewCollection(co)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	return m
}

// ---- in-memory ----

func TestDelRangeBasicInMemory(t *testing.T) {
	m := drNewInMem(t, CollectionOptions{})
	defer m.Close()

	const n, lo, hi = 100, 30, 60
	drExec(t, m, func(b Batch) {
		for i := 0; i < n; i++ {
			b.Set(drKey(i), drVal(i))
		}
	})
	drExec(t, m, func(b Batch) {
		if err := b.DelRange(drKey(lo), drKey(hi)); err != nil {
			t.Fatal(err)
		}
	})

	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	drCheckRange(t, ss, n, lo, hi)
}

func TestDelRangePrecedence(t *testing.T) {
	m := drNewInMem(t, CollectionOptions{})
	defer m.Close()

	const n, lo, hi = 100, 30, 60
	drExec(t, m, func(b Batch) {
		for i := 0; i < n; i++ {
			b.Set(drKey(i), drVal(i))
		}
	})
	drExec(t, m, func(b Batch) {
		b.DelRange(drKey(lo), drKey(hi))
	})
	// A newer Set inside the deleted range must survive the tombstone.
	drExec(t, m, func(b Batch) {
		b.Set(drKey(45), []byte("revived"))
	})

	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	if v, _ := ss.Get(drKey(45), ReadOptions{}); string(v) != "revived" {
		t.Fatalf("key 45 = %q, want revived (newer Set beats range delete)", v)
	}
	// A sibling still inside the range stays deleted.
	if v, _ := ss.Get(drKey(50), ReadOptions{}); v != nil {
		t.Fatalf("key 50 = %q, want deleted", v)
	}
	// A key set only BEFORE the range delete (older) stays deleted.
	if v, _ := ss.Get(drKey(31), ReadOptions{}); v != nil {
		t.Fatalf("key 31 = %q, want deleted (older Set shadowed)", v)
	}
}

func TestDelRangeInMemoryMerged(t *testing.T) {
	// Force a full in-memory merge, then verify the merged segment resolves
	// range-delete coverage (buildRangeDels after merge + merge-time shadow).
	m := drNewInMem(t, CollectionOptions{})
	defer m.Close()

	const n, lo, hi = 200, 50, 150
	for j := 0; j < 4; j++ { // Several batches -> several segments to merge.
		drExec(t, m, func(b Batch) {
			for i := j * 50; i < (j+1)*50; i++ {
				b.Set(drKey(i), drVal(i))
			}
		})
	}
	drExec(t, m, func(b Batch) {
		b.DelRange(drKey(lo), drKey(hi))
	})

	if err := m.(*collection).NotifyMerger("mergeAll", true); err != nil {
		t.Fatal(err)
	}

	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	drCheckRange(t, ss, n, lo, hi)
}

func TestDelRangeMergeOperator(t *testing.T) {
	m := drNewInMem(t, CollectionOptions{
		MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
	})
	defer m.Close()

	drExec(t, m, func(b Batch) {
		b.Set(drKey(1), []byte("a"))
	})
	drExec(t, m, func(b Batch) {
		b.DelRange(drKey(0), drKey(10)) // Covers key 1.
	})
	drExec(t, m, func(b Batch) {
		b.Merge(drKey(1), []byte("b")) // Merges onto the range-deleted (nil) base.
	})

	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	// The merge must apply onto the range-deleted (nil) base, so StringAppend
	// yields Sep+"b" == ":b".  If the older Set("a") had leaked through the
	// range tombstone, the result would instead be "a:b".
	if v, _ := ss.Get(drKey(1), ReadOptions{}); string(v) != ":b" {
		t.Fatalf("key 1 = %q, want %q (merge onto range-deleted nil base)", v, ":b")
	}
}

func TestDelRangeBadRange(t *testing.T) {
	m := drNewInMem(t, CollectionOptions{})
	defer m.Close()

	b, _ := m.NewBatch(0, 0)
	defer b.Close()

	if err := b.DelRange(drKey(10), drKey(10)); err != ErrBadRange {
		t.Fatalf("equal bounds: got %v, want ErrBadRange", err)
	}
	if err := b.DelRange(drKey(10), drKey(5)); err != ErrBadRange {
		t.Fatalf("inverted bounds: got %v, want ErrBadRange", err)
	}
	if err := b.DelRange(drKey(5), drKey(10)); err != nil {
		t.Fatalf("valid bounds: got %v, want nil", err)
	}
}

func TestDelRangeDeferredSort(t *testing.T) {
	m := drNewInMem(t, CollectionOptions{DeferredSort: true})
	defer m.Close()

	const n, lo, hi = 100, 20, 80
	drExec(t, m, func(b Batch) {
		for i := 0; i < n; i++ {
			b.Set(drKey(i), drVal(i))
		}
	})
	drExec(t, m, func(b Batch) {
		b.DelRange(drKey(lo), drKey(hi))
	})

	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	drCheckRange(t, ss, n, lo, hi)
}

// ---- segment-level merge semantics (preserve vs drop + reclaim) ----

func drSortSeg(t *testing.T, seg *segment) {
	t.Helper()
	prev := SkipStats
	SkipStats = true
	sort.Sort(seg)
	seg.buildInMemIndex()
	seg.buildRangeDels()
	SkipStats = prev
}

// drMergedEntries merges a two-segment stack and returns the resulting
// (op, key) entries in order.
func drMergedEntries(t *testing.T, lower, higher *segment,
	includeDeletions bool) []string {
	t.Helper()

	ss := &segmentStack{options: &CollectionOptions{}, a: []Segment{lower, higher}}
	merged, err := newSegment(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ss.mergeInto(0, 2, merged, nil, includeDeletions, false, nil); err != nil {
		t.Fatal(err)
	}

	var out []string
	for pos := 0; pos < merged.Len(); pos++ {
		op, k, _ := merged.getOperationKeyVal(pos)
		name := "?"
		switch op {
		case OperationSet:
			name = "set"
		case OperationDel:
			name = "del"
		case OperationDelRange:
			name = "delrange"
		}
		out = append(out, fmt.Sprintf("%s:%s", name, k))
	}
	return out
}

func TestDelRangeMergePreserveAndDrop(t *testing.T) {
	// lower: Set a,b,c,d,e ; higher: DelRange [b, d) covering b,c.
	lower, _ := newSegment(0, 0)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		lower.Set([]byte(k), []byte("V"+k))
	}
	drSortSeg(t, lower)

	higher, _ := newSegment(0, 0)
	higher.DelRange([]byte("b"), []byte("d"))
	drSortSeg(t, higher)

	// Partial-compaction style (includeDeletions=true): tombstone PRESERVED,
	// covered data (b, c) reclaimed.
	got := drMergedEntries(t, lower, higher, true)
	want := []string{"set:a", "delrange:b", "set:d", "set:e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("includeDeletions=true: got %v, want %v", got, want)
	}

	// Full-compaction style (includeDeletions=false): tombstone DROPPED,
	// covered data reclaimed.
	got = drMergedEntries(t, lower, higher, false)
	want = []string{"set:a", "set:d", "set:e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("includeDeletions=false: got %v, want %v", got, want)
	}
}

// ---- persistence + compaction (through a Store) ----

func drWaitClean(t *testing.T, m Collection) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		stats, err := m.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.CurDirtyOps <= 0 && stats.CurDirtyBytes <= 0 &&
			stats.CurDirtySegments <= 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for persistence")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDelRangePersistReopen(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "mossDelRange")
	defer os.RemoveAll(tmpDir)

	const n, lo, hi = 500, 100, 300

	store, m, err := OpenStoreCollection(tmpDir, StoreOptions{},
		StorePersistOptions{})
	if err != nil {
		t.Fatal(err)
	}

	drExec(t, m, func(b Batch) {
		for i := 0; i < n; i++ {
			b.Set(drKey(i), drVal(i))
		}
	})
	drExec(t, m, func(b Batch) {
		b.DelRange(drKey(lo), drKey(hi))
	})
	drWaitClean(t, m)

	m.Close()
	store.Close()

	// Reopen: range tombstones must be rebuilt from the persisted (mmap'd)
	// segment via loadBasicSegment -> buildRangeDels.
	store2, m2, err := OpenStoreCollection(tmpDir, StoreOptions{},
		StorePersistOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	defer m2.Close()

	ss, err := m2.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	drCheckRange(t, ss, n, lo, hi)
}

func TestDelRangeFullCompactionReclaims(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "mossDelRange")
	defer os.RemoveAll(tmpDir)

	const n, lo, hi = 1000, 300, 600
	live := n - (hi - lo)

	store, m, err := OpenStoreCollection(tmpDir, StoreOptions{},
		StorePersistOptions{CompactionConcern: CompactionForce})
	if err != nil {
		t.Fatal(err)
	}

	drExec(t, m, func(b Batch) {
		for i := 0; i < n; i++ {
			b.Set(drKey(i), drVal(i))
		}
	})
	drExec(t, m, func(b Batch) {
		b.DelRange(drKey(lo), drKey(hi))
	})
	drWaitClean(t, m)

	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	drCheckRange(t, ss, n, lo, hi)
	ss.Close()

	// After a full compaction, the tombstone and every covered entry must be
	// physically gone from the persisted segment (space reclaimed).
	snap, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	footer, ok := snap.(*Footer)
	if !ok {
		t.Fatalf("store snapshot is %T, want *Footer", snap)
	}
	var totOps, totDelRange uint64
	for _, sloc := range footer.SegmentLocs {
		totOps += uint64(sloc.TotOps())
		totDelRange += sloc.TotOpsDelRange
	}
	if totDelRange != 0 {
		t.Fatalf("full compaction left %d range tombstones, want 0", totDelRange)
	}
	if totOps != uint64(live) {
		t.Fatalf("full compaction left %d ops, want %d live (covered entries not reclaimed)",
			totOps, live)
	}
	snap.Close()

	m.Close()
	store.Close()
}

func TestDelRangeChildCollection(t *testing.T) {
	m := drNewInMem(t, CollectionOptions{})
	defer m.Close()

	const n, lo, hi = 100, 40, 70
	const child = "idx"

	drExec(t, m, func(b Batch) {
		cb, err := b.NewChildCollectionBatch(child, BatchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			cb.Set(drKey(i), drVal(i))
		}
	})
	drExec(t, m, func(b Batch) {
		cb, err := b.NewChildCollectionBatch(child, BatchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cb.DelRange(drKey(lo), drKey(hi))
	})

	ss, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	childSS, err := ss.ChildCollectionSnapshot(child)
	if err != nil {
		t.Fatal(err)
	}
	if childSS == nil {
		t.Fatal("expected child snapshot")
	}
	defer childSS.Close()
	drCheckRange(t, childSS, n, lo, hi)
}
