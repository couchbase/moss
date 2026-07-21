//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
)

// ----------------------------------------------------
func loadItems(b Batch, startIdx, stopIdx int) {
	// put numItems in the batch
	for i := startIdx; i > stopIdx; i-- {
		k := fmt.Sprintf("%04d", i)
		b.Set([]byte(k), []byte(k))
	}
}

func verifySnapshot(msg string, ss Snapshot, expectedNum int,
	t *testing.T) {
	defer ss.Close()
	for i := 0; i < expectedNum; i++ {
		k := fmt.Sprintf("%04d", i)
		v, err := ss.Get([]byte(k), ReadOptions{})
		if err != nil {
			t.Errorf("error %s getting key: %s, %v", msg, k, err)
			return
		}
		if string(v) != k {
			t.Errorf("expected %s value for key: %s to be %s,got %s",
				msg, k, k, v)
			return
		}
	}

	iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
	if err != nil {
		t.Errorf("error %s verifySnapshot iter, err: %v", msg, err)
		return
	}

	defer iter.Close()
	n := 0
	var lastKey []byte
	for {
		ex, key, val, err := iter.CurrentEx()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			t.Errorf("error %s iter currentEx, err: %v", msg, err)
			return
		}

		n++

		if ex.Operation != OperationSet {
			t.Errorf("error %s iter op, ex: %v, err: %v", msg, ex, err)
			return
		}

		cmp := bytes.Compare(lastKey, key)
		if cmp >= 0 {
			t.Errorf("error %s iter cmp: %v, err: %v", msg, cmp, err)
			return
		}

		if bytes.Compare(key, val) != 0 {
			t.Errorf("error %s iter key != val: %v, %v", msg, key, val)
			return
		}

		lastKey = key

		err = iter.Next()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			t.Errorf("error %s iter next, err: %v", msg, err)
			return
		}
	}

	if n != expectedNum {
		t.Errorf("error %s iter expectedNum: %d, got: %d",
			msg, expectedNum, n)
	}

}

func childCollectionLoader(m *collection, childName string,
	args *collTestParams, t *testing.T) {
	batchSize := args.batchSize
	for i := args.numIterations - 1; i >= 0; i-- {
		for j := args.numItems - 1; j >= 0; j = j - batchSize {
			// create new batch to set some keys
			b, err := m.NewBatch(0, 0)
			if err != nil {
				args.doneCh <- false
				t.Errorf("error creating new batch: %v", err)
				return
			}

			// also create a child batch
			childB, err := b.NewChildCollectionBatch(childName,
				BatchOptions{0, 0})
			if err != nil {
				args.doneCh <- false
				t.Errorf("error creating new child batch: %v", err)
				return
			}

			loadItems(b, j, j-batchSize)
			loadItems(childB, j, j-batchSize)

			err = m.ExecuteBatch(b, WriteOptions{})
			if err != nil {
				args.doneCh <- false
				t.Errorf("error executing batch: %v", err)
				return
			}

			// cleanup that batch
			err = b.Close()
			if err != nil {
				args.doneCh <- false
				t.Errorf("error closing batch: %v", err)
				return
			}
		}

		topSnap, err := m.Snapshot()
		if err != nil || topSnap == nil {
			args.doneCh <- false
			t.Errorf("error snapshoting: %v", err)
			return
		}
		childSnap, err := topSnap.ChildCollectionSnapshot(childName)
		if err != nil || childSnap == nil {
			args.doneCh <- false
			t.Errorf("error getting child snapshot: %v", err)
			return
		}
		go verifySnapshot(childName, childSnap, args.numItems, t)
		err = topSnap.Close()
		if err != nil {
			args.doneCh <- false
			t.Errorf("error closing snapshot: %v", err)
			return
		}
	}
	args.doneCh <- true
}

type collTestParams struct {
	numItems      int
	batchSize     int
	numIterations int
	numChildren   int
	doneCh        chan bool
}

func testChildCollections(t *testing.T, args *collTestParams) {
	tmpDir, _ := os.MkdirTemp("", "mossStore")
	defer os.RemoveAll(tmpDir)
	store, err := OpenStore(tmpDir, DefaultStoreOptions)
	if err != nil || store == nil {
		t.Fatalf("error opening store :%v", tmpDir)
	}

	coll, _ := NewCollection(DefaultCollectionOptions)
	if coll == nil {
		t.Fatalf("Unable to open new collection in store")
	}
	coll.Start()

	// Create a new child batch to validate basic child collection operations.
	// Same key inserted in different child collections with different values.
	theKey := []byte("sameKey")
	valOfBase := []byte("valOfBase")
	valOfChild := []byte("valOfChild")
	valOfChildb21 := []byte("valOfChildb21")
	valOfChildb22 := []byte("valOfChildb22")

	b, _ := coll.NewBatch(0, 0)
	b.Set(theKey, valOfBase)
	b2, _ := b.NewChildCollectionBatch("child", BatchOptions{0, 0})
	b2.Set(theKey, valOfChild)
	b21, _ := b2.NewChildCollectionBatch("b21", BatchOptions{0, 0})
	b21.Set(theKey, valOfChildb21)
	b22, _ := b2.NewChildCollectionBatch("b22", BatchOptions{0, 0})
	b22.Set(theKey, valOfChildb22)
	err = coll.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Fatalf("Unable to ExecuteBatch on collection %v", err)
	}

	ss, _ := coll.Snapshot()

	llss, err := store.Persist(ss, StorePersistOptions{})
	if err != nil || llss == nil {
		t.Fatalf("expected persist to work")
	}

	ss.Close()
	llss.Close()

	if store.Close() != nil {
		t.Fatalf("expected store close to work")
	}

	if coll.Close() != nil {
		t.Fatalf("Error closing child collection")
	}

	// Now reopen the persisted store & test restore child collections.
	store, coll, err = OpenStoreCollection(tmpDir, DefaultStoreOptions,
		StorePersistOptions{})
	if err != nil {
		t.Fatalf("error re-opening store :%v", tmpDir)
	}

	ss, _ = coll.Snapshot()
	v, err := ss.Get(theKey, ReadOptions{})
	if err != nil || !bytes.Equal(v, valOfBase) {
		t.Fatalf("Expected key %v : got %v, %v, but got err %v",
			string(theKey), string(v), string(valOfBase), err)
	}
	childSS, _ := ss.ChildCollectionSnapshot("child")
	if childSS == nil {
		t.Fatalf("child snapshot not restored after persist")
	}
	v, err = childSS.Get(theKey, ReadOptions{})
	if err != nil || !bytes.Equal(v, valOfChild) {
		t.Fatalf("Expected key %v : got %v, %v, but got err %v",
			string(theKey), string(v), string(valOfChild), err)
	}
	b21SS, _ := childSS.ChildCollectionSnapshot("b21")
	if b21SS == nil {
		t.Fatalf("child snapshot b21 not restored after persist")
	}
	v, err = b21SS.Get(theKey, ReadOptions{})
	if err != nil || !bytes.Equal(v, valOfChildb21) {
		t.Fatalf("Expected key %v : got %v, %v, but got err %v",
			string(theKey), string(v), string(valOfChildb21), err)
	}
	b21SS.Close()   // close grand child snapshot.
	childSS.Close() // each snapshot must be closed properly.
	ss.Close()      // top level close will not auto-close child snapshots.

	// ----------------------------------------------------
	// Now begin parallel collection load with child collections.

	args.doneCh = make(chan bool, args.numChildren)
	for i := 0; i < args.numChildren; i++ {
		name := fmt.Sprintf("child%v", i)
		m := coll.(*collection)
		go childCollectionLoader(m, name, args, t)
	}
	for i := 0; i < args.numChildren; i++ {
		<-args.doneCh
	}
	coll.Close()
}

func Test1Collection100items(t *testing.T) {
	args := &collTestParams{
		numItems:      100,
		batchSize:     10,
		numIterations: 1,
		numChildren:   1,
	}
	testChildCollections(t, args)
}

func Test2Collection1000items(t *testing.T) {
	args := &collTestParams{
		numItems:      1000,
		batchSize:     10,
		numIterations: 2,
		numChildren:   2,
	}
	testChildCollections(t, args)
}

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

func ccNewColl(t *testing.T) Collection {
	t.Helper()
	m, err := NewCollection(CollectionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	return m
}

// ccExec builds a batch via fn and executes it.
func ccExec(t *testing.T, m Collection, fn func(b Batch)) {
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

// ccChildGet reads key from a snapshot's named child; hasChild is false
// if the child collection isn't present in the snapshot.
func ccChildGet(t *testing.T, ss Snapshot, child, key string) (val []byte, hasChild bool) {
	t.Helper()
	cs, err := ss.ChildCollectionSnapshot(child)
	if err != nil {
		t.Fatalf("ChildCollectionSnapshot(%q): %v", child, err)
	}
	if cs == nil {
		return nil, false
	}
	defer cs.Close()
	v, err := cs.Get([]byte(key), ReadOptions{})
	if err != nil {
		t.Fatalf("child %q Get(%q): %v", child, key, err)
	}
	return v, true
}

func ccChildNames(t *testing.T, ss Snapshot) []string {
	t.Helper()
	names, err := ss.ChildCollectionNames()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	return names
}

// TestChildKeyIsolation: same key in top-level and a child are independent.
func TestChildKeyIsolation(t *testing.T) {
	m := ccNewColl(t)
	defer m.Close()

	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("k"), []byte("top"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("k"), []byte("child"))
	})

	ss, _ := m.Snapshot()
	defer ss.Close()

	if v, _ := ss.Get([]byte("k"), ReadOptions{}); string(v) != "top" {
		t.Fatalf("top k = %q, want top", v)
	}
	if v, has := ccChildGet(t, ss, "c", "k"); !has || string(v) != "child" {
		t.Fatalf("child k = %q (has=%v), want child", v, has)
	}
}

// TestChildDeleteRemovesData: deleting a child removes it and its keys.
func TestChildDeleteRemovesData(t *testing.T) {
	m := ccNewColl(t)
	defer m.Close()

	ccExec(t, m, func(b Batch) {
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("k1"), []byte("v1"))
		_ = cb.Set([]byte("k2"), []byte("v2"))
	})
	ccExec(t, m, func(b Batch) {
		_ = b.DelChildCollection("c")
	})

	ss, _ := m.Snapshot()
	defer ss.Close()

	if _, has := ccChildGet(t, ss, "c", "k1"); has {
		t.Fatalf("child c should be gone after delete")
	}
	if names := ccChildNames(t, ss); len(names) != 0 {
		t.Fatalf("child names after delete = %v, want none", names)
	}
}

// TestChildDeleteRecreateNoStale: recreating a deleted child (separate
// batches) must NOT leak keys from the prior incarnation.
func TestChildDeleteRecreateNoStale(t *testing.T) {
	for _, mergeBetween := range []bool{false, true} {
		name := "noMerge"
		if mergeBetween {
			name = "mergeAll"
		}
		t.Run(name, func(t *testing.T) {
			m := ccNewColl(t)
			defer m.Close()
			mc := m.(*collection)

			step := func(fn func(b Batch)) {
				ccExec(t, m, fn)
				if mergeBetween {
					mc.NotifyMerger("mergeAll", true)
				}
			}

			step(func(b Batch) {
				cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
				_ = cb.Set([]byte("old"), []byte("1"))
			})
			step(func(b Batch) { _ = b.DelChildCollection("c") })
			step(func(b Batch) {
				cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
				_ = cb.Set([]byte("new"), []byte("2"))
			})

			ss, _ := m.Snapshot()
			defer ss.Close()

			if v, has := ccChildGet(t, ss, "c", "new"); !has || string(v) != "2" {
				t.Fatalf("recreated child new = %q (has=%v), want 2", v, has)
			}
			if v, _ := ccChildGet(t, ss, "c", "old"); v != nil {
				t.Fatalf("recreated child leaked stale key old = %q, want nil", v)
			}
		})
	}
}

// TestChildUntouchedSurvives: a child not mentioned in a later batch must
// survive the copy-on-write rebuild with its data intact.
func TestChildUntouchedSurvives(t *testing.T) {
	m := ccNewColl(t)
	defer m.Close()

	ccExec(t, m, func(b Batch) {
		ca, _ := b.NewChildCollectionBatch("a", BatchOptions{})
		_ = ca.Set([]byte("ka"), []byte("1"))
		cb, _ := b.NewChildCollectionBatch("b", BatchOptions{})
		_ = cb.Set([]byte("kb"), []byte("1"))
	})
	// Second batch touches only child "a" and the top level.
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("top"), []byte("x"))
		ca, _ := b.NewChildCollectionBatch("a", BatchOptions{})
		_ = ca.Set([]byte("ka"), []byte("2"))
	})

	ss, _ := m.Snapshot()
	defer ss.Close()

	if v, has := ccChildGet(t, ss, "a", "ka"); !has || string(v) != "2" {
		t.Fatalf("child a ka = %q (has=%v), want 2", v, has)
	}
	if v, has := ccChildGet(t, ss, "b", "kb"); !has || string(v) != "1" {
		t.Fatalf("untouched child b kb = %q (has=%v), want 1", v, has)
	}
	if names := ccChildNames(t, ss); len(names) != 2 {
		t.Fatalf("child names = %v, want [a b]", names)
	}
}

// TestChildNestedGrandchild: three levels of nesting are independently
// readable, and deleting the parent drops the whole subtree.
func TestChildNestedGrandchild(t *testing.T) {
	m := ccNewColl(t)
	defer m.Close()

	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("k"), []byte("top"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("k"), []byte("child"))
		gb, _ := cb.NewChildCollectionBatch("g", BatchOptions{})
		_ = gb.Set([]byte("k"), []byte("grand"))
	})

	ss, _ := m.Snapshot()
	if v, _ := ss.Get([]byte("k"), ReadOptions{}); string(v) != "top" {
		t.Fatalf("top = %q", v)
	}
	cs, _ := ss.ChildCollectionSnapshot("c")
	if cs == nil {
		t.Fatal("missing child c")
	}
	if v, _ := cs.Get([]byte("k"), ReadOptions{}); string(v) != "child" {
		t.Fatalf("child = %q", v)
	}
	gs, _ := cs.ChildCollectionSnapshot("g")
	if gs == nil {
		t.Fatal("missing grandchild g")
	}
	if v, _ := gs.Get([]byte("k"), ReadOptions{}); string(v) != "grand" {
		t.Fatalf("grand = %q", v)
	}
	gs.Close()
	cs.Close()
	ss.Close()

	// Deleting c should drop c and its grandchild g.
	ccExec(t, m, func(b Batch) { _ = b.DelChildCollection("c") })

	ss2, _ := m.Snapshot()
	defer ss2.Close()
	if _, has := ccChildGet(t, ss2, "c", "k"); has {
		t.Fatal("child c (and grandchild) should be gone")
	}
}

// TestChildIterationIsolation: iterating a child yields only its keys.
func TestChildIterationIsolation(t *testing.T) {
	m := ccNewColl(t)
	defer m.Close()

	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("t1"), []byte("x"))
		_ = b.Set([]byte("t2"), []byte("x"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("c1"), []byte("x"))
		_ = cb.Set([]byte("c2"), []byte("x"))
		_ = cb.Set([]byte("c3"), []byte("x"))
	})

	ss, _ := m.Snapshot()
	defer ss.Close()
	cs, _ := ss.ChildCollectionSnapshot("c")
	if cs == nil {
		t.Fatal("missing child c")
	}
	defer cs.Close()

	iter, _ := cs.StartIterator(nil, nil, IteratorOptions{})
	defer iter.Close()
	var got []string
	for {
		k, _, err := iter.Current()
		if err == ErrIteratorDone {
			break
		}
		got = append(got, string(k))
		if iter.Next() == ErrIteratorDone {
			break
		}
	}
	if len(got) != 3 || got[0] != "c1" || got[1] != "c2" || got[2] != "c3" {
		t.Fatalf("child iteration = %v, want [c1 c2 c3]", got)
	}
}

// TestChildDelNonexistentNoCrash: deleting a child that never existed is
// a harmless no-op.
func TestChildDelNonexistentNoCrash(t *testing.T) {
	m := ccNewColl(t)
	defer m.Close()

	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("k"), []byte("v"))
		_ = b.DelChildCollection("never-existed")
	})

	ss, _ := m.Snapshot()
	defer ss.Close()
	if v, _ := ss.Get([]byte("k"), ReadOptions{}); string(v) != "v" {
		t.Fatalf("top k = %q, want v", v)
	}
	if names := ccChildNames(t, ss); len(names) != 0 {
		t.Fatalf("child names = %v, want none", names)
	}
}

// ---- Pending tests for confirmed, not-yet-fixed child-collection bugs.
// They assert the CORRECT behavior and are t.Skip'd; remove the Skip
// once the bug is fixed.  See DESIGN-ideas.md "Child collection bugs".

// TestChildSameBatchDelRecreate regresses a fixed bug: a delete +
// recreate of the same child in ONE batch used to collide in the batch's
// childBatches map (a delete sentinel and the new batch fighting over one
// slot), losing the delete so the recreated child merged onto the prior
// incarnation's data (stale leak).  Deletes are now tracked in their own
// set (childCollectionsDeleted), so buildStackDirtyTop applies the delete
// and the recreate mints a fresh incarNum, exactly as a cross-batch
// delete+recreate does.
func TestChildSameBatchDelRecreate(t *testing.T) {
	for _, mergeBetween := range []bool{false, true} {
		name := "noMerge"
		if mergeBetween {
			name = "mergeAll"
		}
		t.Run(name, func(t *testing.T) {
			m := ccNewColl(t)
			defer m.Close()
			mc := m.(*collection)

			// Populate A in its own batch first.
			ccExec(t, m, func(b Batch) {
				cb, _ := b.NewChildCollectionBatch("A", BatchOptions{})
				_ = cb.Set([]byte("old"), []byte("1"))
			})
			if mergeBetween {
				mc.NotifyMerger("mergeAll", true)
			}

			// Same batch: wipe A, then repopulate it.
			ccExec(t, m, func(b Batch) {
				_ = b.DelChildCollection("A") // intent: wipe A ...
				cb, _ := b.NewChildCollectionBatch("A", BatchOptions{})
				_ = cb.Set([]byte("new"), []byte("2")) // ... then repopulate.
			})
			if mergeBetween {
				mc.NotifyMerger("mergeAll", true)
			}

			ss, _ := m.Snapshot()
			defer ss.Close()
			if v, has := ccChildGet(t, ss, "A", "new"); !has || string(v) != "2" {
				t.Fatalf("same-batch wipe+recreate new = %q (has=%v), want 2", v, has)
			}
			if v, _ := ccChildGet(t, ss, "A", "old"); v != nil {
				t.Fatalf("same-batch wipe+recreate leaked stale old = %q, want nil", v)
			}
		})
	}
}

// TestChildSameBatchDelRecreateNewThenDel: within one batch, creating a
// child and then deleting it leaves the child gone (delete wins).
func TestChildSameBatchDelRecreateNewThenDel(t *testing.T) {
	m := ccNewColl(t)
	defer m.Close()

	// Pre-existing A.
	ccExec(t, m, func(b Batch) {
		cb, _ := b.NewChildCollectionBatch("A", BatchOptions{})
		_ = cb.Set([]byte("old"), []byte("1"))
	})
	// Same batch: recreate A (with new data) then delete it -> gone.
	ccExec(t, m, func(b Batch) {
		cb, _ := b.NewChildCollectionBatch("A", BatchOptions{})
		_ = cb.Set([]byte("new"), []byte("2"))
		_ = b.DelChildCollection("A")
	})

	ss, _ := m.Snapshot()
	defer ss.Close()
	if _, has := ccChildGet(t, ss, "A", "new"); has {
		t.Fatal("same-batch new-then-del: child A should be gone")
	}
}

// TestChildDirtyAccounting regresses a fixed bug: child-collection
// mutations were not counted in CurDirtyOps/CurDirtyBytes (segmentStack.Stats
// ignored childSegStacks), which broke waitForPersistence and the
// MaxDirtyOps/MaxDirtyKeyValBytes back-pressure for child-only writes.
// segmentStack.Stats now recurses into childSegStacks.
func TestChildDirtyAccounting(t *testing.T) {
	m := ccNewColl(t)
	defer m.Close()

	ccExec(t, m, func(b Batch) {
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("k"), []byte("v"))
	})

	st, err := m.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.CurDirtyOps == 0 {
		t.Fatalf("child-only write left CurDirtyOps=0; child ops must be counted")
	}
	if st.CurDirtyBytes == 0 {
		t.Fatalf("child-only write left CurDirtyBytes=0; child bytes must be counted")
	}
}

// waitForPersistenceBounded is waitForPersistence with a deadline, so a
// regression that stalls the dirty->clean transition fails the test fast
// instead of hanging until the whole test binary times out.
func waitForPersistenceBounded(t *testing.T, coll Collection, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		stats, err := coll.Stats()
		if err == nil && stats.CurDirtyOps <= 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("persistence did not drain in %v (CurDirtyOps=%d "+
				"top=%d mid=%d base=%d clean=%d); child-only dirty state "+
				"not reconciled with clean", d, stats.CurDirtyOps,
				stats.CurDirtyTopOps, stats.CurDirtyMidOps,
				stats.CurDirtyBaseOps, stats.CurCleanOps)
		}
		time.Sleep(time.Millisecond)
	}
}

func ccOpenStore(t *testing.T, dir string, concern CompactionConcern) (*Store, Collection) {
	t.Helper()
	store, coll, err := OpenStoreCollection(dir, DefaultStoreOptions,
		StorePersistOptions{CompactionConcern: concern})
	if err != nil {
		t.Fatal(err)
	}
	return store, coll
}

// TestChildStoreRoundTrip: top/child/grandchild survive persist+reopen.
func TestChildStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()

	store, m := ccOpenStore(t, dir, CompactionAllow)
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("k"), []byte("top"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("k"), []byte("child"))
		gb, _ := cb.NewChildCollectionBatch("g", BatchOptions{})
		_ = gb.Set([]byte("k"), []byte("grand"))
	})
	waitForPersistence(m)
	m.Close()
	store.Close()

	store2, m2 := ccOpenStore(t, dir, CompactionAllow)
	defer store2.Close()
	defer m2.Close()

	ss, _ := m2.Snapshot()
	defer ss.Close()

	if v, _ := ss.Get([]byte("k"), ReadOptions{}); string(v) != "top" {
		t.Fatalf("after reopen top = %q, want top", v)
	}
	cs, _ := ss.ChildCollectionSnapshot("c")
	if cs == nil {
		t.Fatal("after reopen child c missing")
	}
	defer cs.Close()
	if v, _ := cs.Get([]byte("k"), ReadOptions{}); string(v) != "child" {
		t.Fatalf("after reopen child = %q, want child", v)
	}
	gs, _ := cs.ChildCollectionSnapshot("g")
	if gs == nil {
		t.Fatal("after reopen grandchild g missing")
	}
	defer gs.Close()
	if v, _ := gs.Get([]byte("k"), ReadOptions{}); string(v) != "grand" {
		t.Fatalf("after reopen grand = %q, want grand", v)
	}
}

// TestChildStoreDeletePersisted: a child persisted, then deleted and
// re-persisted, is gone after reopen.
func TestChildStoreDeletePersisted(t *testing.T) {
	dir := t.TempDir()

	store, m := ccOpenStore(t, dir, CompactionAllow)
	// Top-level markers force CurDirtyOps > 0 so waitForPersistence is
	// reliable (child-only writes don't register as dirty -- see
	// TestChildDirtyAccountingPending).
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("1"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("k"), []byte("v"))
	})
	waitForPersistence(m)
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("2"))
		_ = b.DelChildCollection("c")
	})
	waitForPersistence(m)
	m.Close()
	store.Close()

	store2, m2 := ccOpenStore(t, dir, CompactionAllow)
	defer store2.Close()
	defer m2.Close()

	ss, _ := m2.Snapshot()
	defer ss.Close()
	if _, has := ccChildGet(t, ss, "c", "k"); has {
		t.Fatal("deleted+persisted child c should be gone after reopen")
	}
}

// TestChildStoreDeleteRecreateNoStale: delete+recreate a child across
// persists must not resurrect the prior incarnation's keys after reopen.
func TestChildStoreDeleteRecreateNoStale(t *testing.T) {
	dir := t.TempDir()

	store, m := ccOpenStore(t, dir, CompactionAllow)
	// Each batch also sets a top-level "marker" key: child-only writes do
	// not currently register in CurDirtyOps (see the KNOWN BUG test
	// TestChildDirtyAccountingPending), so waitForPersistence would return
	// early on a child-only write.  The marker forces a real persist so
	// this test reliably exercises the delete/recreate PERSIST logic.
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("1"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("old"), []byte("1"))
	})
	waitForPersistence(m)
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("2"))
		_ = b.DelChildCollection("c")
	})
	waitForPersistence(m)
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("3"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("new"), []byte("2"))
	})
	waitForPersistence(m)
	m.Close()
	store.Close()

	store2, m2 := ccOpenStore(t, dir, CompactionAllow)
	defer store2.Close()
	defer m2.Close()

	ss, _ := m2.Snapshot()
	defer ss.Close()
	if v, has := ccChildGet(t, ss, "c", "new"); !has || string(v) != "2" {
		t.Fatalf("after reopen recreated child new = %q (has=%v), want 2", v, has)
	}
	if v, _ := ccChildGet(t, ss, "c", "old"); v != nil {
		t.Fatalf("after reopen recreated child leaked stale old = %q, want nil", v)
	}
}

// TestChildStoreSameBatchDelRecreate: a same-batch delete+recreate of a
// child (the fixed collision bug) must not resurrect the prior
// incarnation's keys after persist+reopen.
func TestChildStoreSameBatchDelRecreate(t *testing.T) {
	dir := t.TempDir()

	store, m := ccOpenStore(t, dir, CompactionAllow)
	// Top-level markers keep waitForPersistence reliable (see
	// TestChildDirtyAccountingPending).
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("1"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("old"), []byte("1"))
	})
	waitForPersistence(m)
	// Same batch: wipe c then repopulate it.
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("2"))
		_ = b.DelChildCollection("c")
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("new"), []byte("2"))
	})
	waitForPersistence(m)
	m.Close()
	store.Close()

	store2, m2 := ccOpenStore(t, dir, CompactionAllow)
	defer store2.Close()
	defer m2.Close()

	ss, _ := m2.Snapshot()
	defer ss.Close()
	if v, has := ccChildGet(t, ss, "c", "new"); !has || string(v) != "2" {
		t.Fatalf("after reopen same-batch recreate new = %q (has=%v), want 2", v, has)
	}
	if v, _ := ccChildGet(t, ss, "c", "old"); v != nil {
		t.Fatalf("after reopen same-batch recreate leaked stale old = %q, want nil", v)
	}
}

// TestChildSnapshotCloseReleasesChildLowerLevels regresses bug #7:
// segmentStack.decRef did not recurse into childSegStacks, so a snapshot's
// child (and grandchild) segStacks were never released -- their
// lowerLevelSnapshots (mmap/FileRef handles once a store is attached)
// stayed open, pinning superseded data files.  decRef now recurses.
func TestChildSnapshotCloseReleasesChildLowerLevels(t *testing.T) {
	dir := t.TempDir()
	store, m := ccOpenStore(t, dir, CompactionAllow)
	defer store.Close()
	defer m.Close()

	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("t"), []byte("v"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("ck"), []byte("cv"))
		gb, _ := cb.NewChildCollectionBatch("g", BatchOptions{})
		_ = gb.Set([]byte("gk"), []byte("gv"))
	})
	waitForPersistence(m)

	// Use the internal snapshot() (refs==1, not the ref-2 cached
	// Collection.Snapshot()) so a single Close() actually frees it and we
	// observe the recursive child release.
	mc := m.(*collection)
	sss, _, _, _, _ := mc.snapshot(0, nil, false)

	childSS := sss.childSegStacks["c"]
	if childSS == nil {
		t.Fatal("missing child c segStack in snapshot")
	}
	grandSS := childSS.childSegStacks["g"]
	if grandSS == nil {
		t.Fatal("missing grandchild g segStack in snapshot")
	}
	if childSS.lowerLevelSnapshot == nil || grandSS.lowerLevelSnapshot == nil {
		t.Fatal("expected store-backed child/grandchild lowerLevelSnapshots")
	}

	sss.Close()

	// The snapshot's final decRef must have recursed into its child
	// segStacks, closing their lowerLevelSnapshots.
	if childSS.lowerLevelSnapshot != nil {
		t.Error("child segStack lowerLevelSnapshot not closed on snapshot close (leak)")
	}
	if grandSS.lowerLevelSnapshot != nil {
		t.Error("grandchild segStack lowerLevelSnapshot not closed on snapshot close (leak)")
	}
}

// TestChildStoreDirtyAccountingPersists regresses the dirty-accounting
// fix end-to-end: a CHILD-ONLY write (no top-level key) must register as
// dirty, then actually drain to clean via the merger+persister -- i.e.
// waitForPersistence must complete and the data must survive reopen.
// Before the fix, child ops weren't counted (waitForPersistence returned
// early / data could be dropped), and naively counting them without
// waking the merger for child-only work hung the drain.
func TestChildStoreDirtyAccountingPersists(t *testing.T) {
	dir := t.TempDir()

	store, m := ccOpenStore(t, dir, CompactionAllow)

	// Child-only writes across several batches, NO top-level marker.
	for i := 0; i < 5; i++ {
		ccExec(t, m, func(b Batch) {
			cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
			_ = cb.Set([]byte("k"), []byte("v"))
			gb, _ := cb.NewChildCollectionBatch("g", BatchOptions{})
			_ = gb.Set([]byte("gk"), []byte("gv"))
		})
	}

	// A child-only write must be visible as dirty.
	if st, _ := m.Stats(); st.CurDirtyOps == 0 {
		t.Fatal("child-only writes left CurDirtyOps=0")
	}

	// ... and must drain to clean (no top-level marker needed anymore).
	waitForPersistenceBounded(t, m, 10*time.Second)

	m.Close()
	store.Close()

	store2, m2 := ccOpenStore(t, dir, CompactionAllow)
	defer store2.Close()
	defer m2.Close()

	ss, _ := m2.Snapshot()
	defer ss.Close()
	if v, has := ccChildGet(t, ss, "c", "k"); !has || string(v) != "v" {
		t.Fatalf("after reopen child c k = %q (has=%v), want v", v, has)
	}
}

// TestChildStoreCompaction: children survive a forced full compaction.
func TestChildStoreCompaction(t *testing.T) {
	dir := t.TempDir()

	store, m := ccOpenStore(t, dir, CompactionForce)
	for i := 0; i < 5; i++ {
		ccExec(t, m, func(b Batch) {
			// Top-level marker so CurDirtyOps > 0 and waitForPersistence
			// is reliable (child-only writes don't register as dirty --
			// see TestChildDirtyAccountingPending).
			_ = b.Set([]byte("marker"), []byte("m"))
			cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
			_ = cb.Set([]byte("k"), []byte("v"))
			gb, _ := cb.NewChildCollectionBatch("g", BatchOptions{})
			_ = gb.Set([]byte("gk"), []byte("gv"))
		})
		waitForPersistence(m)
	}
	m.Close()
	store.Close()

	store2, m2 := ccOpenStore(t, dir, CompactionForce)
	defer store2.Close()
	defer m2.Close()

	ss, _ := m2.Snapshot()
	defer ss.Close()
	cs, _ := ss.ChildCollectionSnapshot("c")
	if cs == nil {
		t.Fatal("after compaction+reopen child c missing")
	}
	defer cs.Close()
	if v, _ := cs.Get([]byte("k"), ReadOptions{}); string(v) != "v" {
		t.Fatalf("child c k = %q, want v", v)
	}
	gs, _ := cs.ChildCollectionSnapshot("g")
	if gs == nil {
		t.Fatal("after compaction+reopen grandchild g missing")
	}
	defer gs.Close()
	if v, _ := gs.Get([]byte("gk"), ReadOptions{}); string(v) != "gv" {
		t.Fatalf("grandchild g gk = %q, want gv", v)
	}
}

// TestChildFooterReadTwice regresses a fixed bug: child Footers loaded
// from disk used to be JSON-unmarshaled with refs==0, so via the raw
// *Store/*Footer API the first ChildCollectionSnapshot+Close drove a
// reloaded child footer to 0 and freed it, and a second read returned
// empty (silent data loss). ScanFooter now initChildRefs()'s them to 1.
func TestChildFooterReadTwice(t *testing.T) {
	dir := t.TempDir()
	store, m := ccOpenStore(t, dir, CompactionAllow)
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("m"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("k"), []byte("v"))
	})
	waitForPersistence(m)
	m.Close()
	store.Close()

	store2, err := OpenStore(dir, DefaultStoreOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	foot, _ := store2.Snapshot()
	defer foot.Close()

	c1, _ := foot.ChildCollectionSnapshot("c")
	v1, _ := c1.Get([]byte("k"), ReadOptions{})
	c1.Close()
	c2, _ := foot.ChildCollectionSnapshot("c")
	v2, _ := c2.Get([]byte("k"), ReadOptions{})
	c2.Close()
	if string(v1) != "v" || string(v2) != "v" {
		t.Fatalf("raw Footer child reads = %q, %q; want v, v (2nd read lost data)", v1, v2)
	}
}

// TestChildFooterCloseReleasesChildren regresses the child-footer leak:
// Footer.DecRef must recurse into ChildFooters, so that fully releasing
// a footer releases its (and its grandchildren's) segments -- otherwise
// child mmaps/FileRefs are pinned forever and superseded files are never
// deleted.
func TestChildFooterCloseReleasesChildren(t *testing.T) {
	dir := t.TempDir()
	store, m := ccOpenStore(t, dir, CompactionAllow)
	ccExec(t, m, func(b Batch) {
		_ = b.Set([]byte("marker"), []byte("m"))
		cb, _ := b.NewChildCollectionBatch("c", BatchOptions{})
		_ = cb.Set([]byte("k"), []byte("v"))
		gb, _ := cb.NewChildCollectionBatch("g", BatchOptions{})
		_ = gb.Set([]byte("gk"), []byte("gv"))
	})
	waitForPersistence(m)
	m.Close()
	store.Close()

	store2, err := OpenStore(dir, DefaultStoreOptions)
	if err != nil {
		t.Fatal(err)
	}
	footSnap, _ := store2.Snapshot()
	foot := footSnap.(*Footer)

	childF := foot.ChildFooters["c"]
	if childF == nil || childF.SegmentLocs == nil {
		t.Fatal("missing/unloaded child footer c")
	}
	grandF := childF.ChildFooters["g"]
	if grandF == nil || grandF.SegmentLocs == nil {
		t.Fatal("missing/unloaded grandchild footer g")
	}

	// Release both the snapshot ref and the store's own footer ref.
	footSnap.Close()
	store2.Close()

	// The parent's final DecRef must have recursed and released the
	// child and grandchild footers' segments.
	if childF.SegmentLocs != nil {
		t.Errorf("child footer segments not released on parent close (leak)")
	}
	if grandF.SegmentLocs != nil {
		t.Errorf("grandchild footer segments not released on parent close (leak)")
	}
}
