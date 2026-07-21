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
	"testing"
)

// TestChildStorePersistNilKeepsChildren regresses bug #5: an idle/full
// compaction with higher==nil (reachable via the exported
// store.Persist(nil, CompactionForce)) rebuilt the footer from footer.ss,
// which carries no child segStacks -> all child collections dropped.
func TestChildStorePersistNilKeepsChildren(t *testing.T) {
	dir := t.TempDir()
	store, m := ccOpenStore(t, dir, CompactionAllow)
	defer store.Close()
	defer m.Close()

	// Two persists so the store footer has >1 top-level segment (else
	// compact() returns ErrNothingToCompact); child c added in the first.
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("t1"), []byte("v1"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("ck"), []byte("cv"))
		gb, _ := cb.NewChildCollectionBatch("g", BatchOptions{})
		_ = gb.Set([]byte("gk"), []byte("gv"))
	})
	waitForPersistence(m)
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("t2"), []byte("v2"))
	})
	waitForPersistence(m)

	// Idle full compaction with higher==nil.
	ss, err := store.Persist(nil, StorePersistOptions{
		CompactionConcern: CompactionForce})
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	cs, _ := ss.ChildCollectionSnapshot("c")
	if cs == nil {
		t.Fatal("Persist(nil, CompactionForce) dropped child collection c")
	}
	defer cs.Close()
	if v, _ := cs.Get([]byte("ck"), ReadOptions{}); string(v) != "cv" {
		t.Fatalf("child c ck = %q, want cv", v)
	}
	gs, _ := cs.ChildCollectionSnapshot("g")
	if gs == nil {
		t.Fatal("Persist(nil, CompactionForce) dropped grandchild g")
	}
	defer gs.Close()
	if v, _ := gs.Get([]byte("gk"), ReadOptions{}); string(v) != "gv" {
		t.Fatalf("grandchild g gk = %q, want gv", v)
	}
}

// TestChildStoreCompactionKeepsAllData regresses the incarNum half of
// bug #6: mergeSegStacks compared a child footer's incarNum to the PARENT
// stack's incarNum (always unequal), so every full compaction dropped the
// child's already-persisted segments -- only data re-written in the same
// batch survived.  Here each child key is distinct and written once, so a
// dropped persisted segment is detected as a missing key after compaction.
func TestChildStoreCompactionKeepsAllData(t *testing.T) {
	dir := t.TempDir()
	store, coll, err := OpenStoreCollection(dir,
		StoreOptions{
			CompactionPercentage:       100, // never fall back to append-only
			CompactionLevelMaxSegments: 2,
			CompactionLevelMultiplier:  100000,
		},
		StorePersistOptions{CompactionConcern: CompactionAllow})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer coll.Close()

	numBatches := 40
	for bi := 0; bi < numBatches; bi++ {
		ccExec(t, coll, func(b Batch) {
			_ = b.Set([]byte(fmt.Sprintf("t%d", bi)), []byte("v"))
			cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
			_ = cb.Set([]byte(fmt.Sprintf("ck%d", bi)), []byte("cv"))
		})
		waitForPersistence(coll)
	}

	sstats, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := sstats["total_compactions"].(uint64); c == 0 {
		t.Fatalf("expected >0 compactions, got %d (test not exercising the bug)", c)
	}

	ss, _ := coll.Snapshot()
	defer ss.Close()
	for bi := 0; bi < numBatches; bi++ {
		if v, has := ccChildGet(t, ss, "c", fmt.Sprintf("ck%d", bi)); !has || string(v) != "cv" {
			t.Fatalf("child c ck%d = %q (has=%v) after compaction; persisted "+
				"child segment was dropped", bi, v, has)
		}
	}
}

// TestChildStorePartialCompaction regresses the splicePoint half of bug
// #6: partial/leveled compaction applied the top-level splicePoint to
// child footers, which have an independent (usually smaller) segment
// count -> slice-out-of-range panic (or mis-split) when a child has fewer
// segments than the splicePoint.  Driven directly (like
// TestStorePartialCompactionWithMergeOperator) for a deterministic
// partial compaction: 3 top-level segments, child c with only 1.
func TestChildStorePartialCompaction(t *testing.T) {
	dir := t.TempDir()
	spo := StorePersistOptions{CompactionConcern: CompactionDisable}
	store, coll, err := OpenStoreCollection(dir, DefaultStoreOptions, spo)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer coll.Close()
	m := coll.(*collection)

	// 3 persisted top-level segments; child c only in the first batch, so
	// the store footer's child c has 1 segment (< the splicePoint below).
	for i := 0; i < 3; i++ {
		bi := i
		ccExec(t, coll, func(b Batch) {
			_ = b.Set([]byte(fmt.Sprintf("t%d", bi)), []byte("v"))
			if bi == 0 {
				cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
				_ = cb.Set([]byte("ck"), []byte("cv"))
			}
		})
		waitForPersistence(coll)
	}

	// Final (unpersisted) batch touching top-level AND child c, so the
	// "higher" snapshot carries child c into the compaction.
	b, _ := coll.NewBatch(0, 0)
	_ = b.Set([]byte("t3"), []byte("v"))
	cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
	_ = cb.Set([]byte("ck2"), []byte("cv2"))
	bx := b.(*batch)

	m.m.Lock()
	ssNew := m.buildStackDirtyTop(bx, m.stackDirtyTop)
	m.m.Unlock()

	footer, err := store.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer footer.DecRef()

	// splicePoint=2 exceeds child c's single segment: pre-fix this panics
	// (slice bounds out of range) in the child recursion of mergeSegStacks.
	if err = store.compact(footer, 2, ssNew, spo); err != nil {
		t.Fatal(err)
	}

	ss, _ := store.Snapshot()
	defer ss.Close()
	// Both the old persisted key and the newly-merged key must survive.
	if v, has := ccChildGet(t, ss, "c", "ck"); !has || string(v) != "cv" {
		t.Fatalf("child c ck = %q (has=%v), want cv (prior segment lost)", v, has)
	}
	if v, has := ccChildGet(t, ss, "c", "ck2"); !has || string(v) != "cv2" {
		t.Fatalf("child c ck2 = %q (has=%v), want cv2", v, has)
	}
	// And a top-level key from before and after the splicePoint.
	if v, _ := ss.Get([]byte("t0"), ReadOptions{}); string(v) != "v" {
		t.Fatalf("top t0 = %q, want v", v)
	}
	if v, _ := ss.Get([]byte("t3"), ReadOptions{}); string(v) != "v" {
		t.Fatalf("top t3 = %q, want v", v)
	}
}
