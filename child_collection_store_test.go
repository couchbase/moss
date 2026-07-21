//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"testing"
	"time"
)

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
