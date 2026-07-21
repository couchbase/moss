//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"sort"
	"testing"
)

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
