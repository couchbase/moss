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
	"math/rand"
	"os"
	"testing"
)

// mergeModel is a straightforward reference implementation of the
// MergeOperatorStringAppend semantics for a single key, used to check
// moss's (now lazy) merge resolution.
type mergeModel struct {
	mo       *MergeOperatorStringAppend
	baseSet  bool   // whether the current base is a live Set value
	base     string // the current base value (valid when baseSet)
	operands [][]byte
}

func (mm *mergeModel) set(v string) {
	mm.baseSet = true
	mm.base = v
	mm.operands = nil
}

func (mm *mergeModel) del() {
	mm.baseSet = false
	mm.base = ""
	mm.operands = nil
}

func (mm *mergeModel) merge(v string) {
	mm.operands = append(mm.operands, []byte(v))
}

// expected returns (absent, value): absent==true means Get should
// return nil.
func (mm *mergeModel) expected(key string) (bool, string) {
	if len(mm.operands) == 0 {
		if !mm.baseSet {
			return true, ""
		}
		return false, mm.base
	}
	var existing []byte
	if mm.baseSet {
		existing = []byte(mm.base)
	}
	v, ok := mm.mo.FullMerge([]byte(key), existing, mm.operands)
	if !ok {
		panic("reference FullMerge failed")
	}
	return false, string(v)
}

// checkAll asserts every modeled key matches what the collection
// returns, both via Get and via a snapshot iterator.
func checkAll(t *testing.T, m Collection, models map[string]*mergeModel, phase string) {
	t.Helper()
	// Check both read paths: the direct collection.Get() and
	// Snapshot().Get().  Both must resolve merge chains that span the
	// dirty stacks (operands in a newer stack, base value in an older
	// stack / the lower level) consistently with the reference model.
	ss, err := m.Snapshot()
	if err != nil {
		t.Fatalf("[%s] Snapshot err: %v", phase, err)
	}
	defer ss.Close()

	check := func(via string, got []byte, getErr error, key string, absent bool, want string) {
		if getErr != nil {
			t.Fatalf("[%s/%s] Get(%q) err: %v", phase, via, key, getErr)
		}
		if absent {
			if got != nil {
				t.Fatalf("[%s/%s] key %q: expected absent, got %q", phase, via, key, got)
			}
		} else if got == nil || string(got) != want {
			t.Fatalf("[%s/%s] key %q: got %q, want %q", phase, via, key, got, want)
		}
	}

	for key, mm := range models {
		absent, want := mm.expected(key)

		gotC, errC := m.Get([]byte(key), ReadOptions{})
		check("collection", gotC, errC, key, absent, want)

		gotS, errS := ss.Get([]byte(key), ReadOptions{})
		check("snapshot", gotS, errS, key, absent, want)
	}
}

func runMergeDifferential(t *testing.T, m Collection, mc *collection,
	seed int64, iterations int) {
	mo := m.Options().MergeOperator.(*MergeOperatorStringAppend)
	r := rand.New(rand.NewSource(seed))

	const nKeys = 12
	models := map[string]*mergeModel{}
	for i := 0; i < nKeys; i++ {
		models[fmt.Sprintf("k%02d", i)] = &mergeModel{mo: mo}
	}

	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("k%02d", r.Intn(nKeys))
		b, err := m.NewBatch(1, 32)
		if err != nil {
			t.Fatal(err)
		}
		switch r.Intn(10) {
		case 0, 1: // Set (incl. occasional empty value)
			v := fmt.Sprintf("s%d", i)
			if r.Intn(5) == 0 {
				v = ""
			}
			_ = b.Set([]byte(key), []byte(v))
			models[key].set(v)
		case 2: // Del
			_ = b.Del([]byte(key))
			models[key].del()
		default: // Merge (the common case)
			v := fmt.Sprintf("m%d", i)
			_ = b.Merge([]byte(key), []byte(v))
			models[key].merge(v)
		}
		if err := m.ExecuteBatch(b, WriteOptions{}); err != nil {
			t.Fatal(err)
		}
		b.Close()

		// Periodically check the live (possibly multi-segment) read path.
		if i%97 == 0 {
			checkAll(t, m, models, "read")
		}
		// Periodically force a full merge, then check the post-merge path.
		if i%223 == 0 {
			mc.NotifyMerger("mergeAll", true)
			checkAll(t, m, models, "post-merge")
		}
	}

	mc.NotifyMerger("mergeAll", true)
	checkAll(t, m, models, "final")
}

// TestMergeLazyDifferentialInMemory stresses the lazy merge resolution
// against a reference model, purely in-memory.
func TestMergeLazyDifferentialInMemory(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		m, err := NewCollection(CollectionOptions{
			MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Start(); err != nil {
			t.Fatal(err)
		}
		runMergeDifferential(t, m, m.(*collection), seed, 1500)
		m.Close()
	}
}

// TestMergeLazyDifferentialStore stresses the lazy merge resolution
// through a store, so that merge chains resolve their base value from
// the persisted lower level (exercising resolveMerge's getLowerLevel
// path during both reads and compaction).
func TestMergeLazyDifferentialStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mossMergeLazy")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	so := DefaultStoreOptions
	so.CollectionOptions = CollectionOptions{
		MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
	}
	spo := StorePersistOptions{CompactionConcern: CompactionAllow}

	store, m, err := OpenStoreCollection(tmpDir, so, spo)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer m.Close()

	runMergeDifferential(t, m, m.(*collection), 42, 1500)
}
