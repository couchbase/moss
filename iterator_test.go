//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package moss

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"
)

func TestIteratorMergeOps(t *testing.T) {
	var mlock sync.Mutex

	events := map[EventKind]int{}

	mo := &MergeOperatorStringAppend{Sep: ":"}

	m, err := NewCollection(CollectionOptions{
		MergeOperator: mo,
		OnEvent: func(e Event) {
			mlock.Lock()
			events[e.Kind]++
			mlock.Unlock()
		},
	})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}
	mc := m.(*collection)

	err = m.Start()
	if err != nil {
		t.Errorf("expected Start ok")
	}

	mergeVal := func(v string) {
		b, err := m.NewBatch(0, 0)
		if err != nil || b == nil {
			t.Errorf("expected b ok")
		}
		b.Merge([]byte("a"), []byte(v))
		err = m.ExecuteBatch(b, WriteOptions{})
		if err != nil {
			t.Errorf("expected execute batch ok")
		}
	}

	mergeVal("A")
	mergeVal("B")

	mc.NotifyMerger("mergeAll", true)

	checkVal := func(expected string) {
		ss, err := m.Snapshot()
		if err != nil {
			t.Errorf("expected ss ok")
		}
		iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil || iter == nil {
			t.Errorf("expected iter")
		}
		k, v, err := iter.Current()
		if err != nil {
			t.Errorf("expected current")
		}
		if string(k) != "a" {
			t.Errorf("expected current key a")
		}
		if string(v) != expected {
			t.Errorf("expected current val expected: %v, got: %v", expected, v)
		}
		if iter.Next() != ErrIteratorDone {
			t.Errorf("expected only 1 value in iterator")
		}
		v, err = ss.Get([]byte("a"), ReadOptions{})
		if err != nil || string(v) != expected {
			t.Errorf("expected %s, got: %s, err: %v", expected, v, err)
		}
	}

	checkVal(":A:B")

	mc.NotifyMerger("mergeAll", true)

	checkVal(":A:B")

	// -----------------------------------

	mergeVal("C")

	mc.NotifyMerger("mergeAll", true)

	checkVal(":A:B:C")

	mo.m.Lock()
	if mo.numFull+mo.numPartial <= 0 {
		t.Errorf("expected some merges")
	}
	mlock.Lock()
	if len(events) <= 0 {
		t.Errorf("expected some events")
	}
	mlock.Unlock()
	mo.m.Unlock()
}

func TestIteratorMergeOps_MB19667(t *testing.T) {
	// Need to arrange that...
	// - stack dirty top = empty
	// - stack dirty mid = [ various merge ops ]
	// - stack dirty base = [ more merge ops ]
	//
	var mlock sync.Mutex

	events := map[EventKind]int{}

	var eventCh chan EventKind

	mo := &MergeOperatorStringAppend{Sep: ":"}

	m, err := NewCollection(CollectionOptions{
		MergeOperator: mo,
		OnEvent: func(e Event) {
			mlock.Lock()
			events[e.Kind]++
			if eventCh != nil {
				eventCh <- e.Kind
			}
			mlock.Unlock()
		},
	})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}
	mc := m.(*collection)

	// Note that we haven't Start()'ed the collection yet, so it
	// doesn't have any background goroutines runnning yet.

	mergeVal := func(v string) {
		b, err := m.NewBatch(0, 0)
		if err != nil || b == nil {
			t.Errorf("expected b ok")
		}
		b.Merge([]byte("k"), []byte(v))
		err = m.ExecuteBatch(b, WriteOptions{})
		if err != nil {
			t.Errorf("expected execute batch ok")
		}
		b.Close()
	}

	mergeVal("A")
	mergeVal("B")

	// Pretend to be the merger, moving stack dirty top into base.
	mc.m.Lock()
	mc.stackDirtyBase = mc.stackDirtyTop
	mc.stackDirtyTop = nil
	mc.m.Unlock()

	mergeVal("C")
	mergeVal("D")

	// Pretend to be the merger, moving stack dirty top into mid.
	mc.m.Lock()
	mc.stackDirtyMid = mc.stackDirtyTop
	mc.stackDirtyTop = nil
	mc.m.Unlock()

	checkVal := func(expected string) {
		ss, err := m.Snapshot()
		if err != nil {
			t.Errorf("expected ss ok")
		}
		getv, err := ss.Get([]byte("k"), ReadOptions{})
		if err != nil || string(getv) != expected {
			t.Errorf("expected %s, got: %s, err: %v", expected, getv, err)
		}
		iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil || iter == nil {
			t.Errorf("expected iter")
		}
		k, v, err := iter.Current()
		if err != nil {
			t.Errorf("expected iter current no err")
		}
		if string(k) != "k" {
			t.Errorf("expected iter current key k")
		}
		if string(v) != expected {
			t.Errorf("expected iter current val expected: %v, got: %s", expected, v)
		}
		if iter.Next() != ErrIteratorDone {
			t.Errorf("expected only 1 value in iterator")
		}
		ss.Close()
	}

	checkVal(":A:B:C:D")

	// Now, start the merger goroutine and force it to run once.
	mc.Start()
	mc.NotifyMerger("mergeAll", true)

	// After the merging, the value should be correct.  Before the fix
	// to MB-19667, it used to ignore the information in the
	// stackDirtyBase and incorrectly return only the latest mergings
	// of ":C:D".
	checkVal(":A:B:C:D")
}

func TestIteratorSingleMergeOp(t *testing.T) {
	testIteratorSingleMergeOp(t, []string{"A"}, ":A")
}

func TestIteratorSingleMergeOp2(t *testing.T) {
	testIteratorSingleMergeOp(t, []string{"A", "B"}, ":A:B")
}

func testIteratorSingleMergeOp(t *testing.T,
	mergeVals []string, expectVal string) {
	var mlock sync.Mutex

	events := map[EventKind]int{}

	mo := &MergeOperatorStringAppend{Sep: ":"}

	m, err := NewCollection(CollectionOptions{
		MergeOperator: mo,
		OnEvent: func(e Event) {
			mlock.Lock()
			events[e.Kind]++
			mlock.Unlock()
		},
	})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}
	mc := m.(*collection)

	err = m.Start()
	if err != nil {
		t.Errorf("expected Start ok")
	}

	mergeVal := func(v string) {
		b, err := m.NewBatch(0, 0)
		if err != nil || b == nil {
			t.Errorf("expected b ok")
		}
		b.Merge([]byte("a"), []byte(v))
		err = m.ExecuteBatch(b, WriteOptions{})
		if err != nil {
			t.Errorf("expected execute batch ok")
		}
	}

	for _, mv := range mergeVals {
		mergeVal(mv)
	}

	mc.NotifyMerger("mergeAll", true)

	checkVal := func(expected string) {
		ss, err := m.Snapshot()
		if err != nil {
			t.Errorf("expected ss ok")
		}
		iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil || iter == nil {
			t.Errorf("expected iter")
		}
		_, ok := iter.(*iteratorSingle)
		if !ok {
			t.Errorf("expected iteratorSingle")
		}
		k, v, err := iter.Current()
		if err != nil {
			t.Errorf("expected current")
		}
		if string(k) != "a" {
			t.Errorf("expected current key a")
		}
		if string(v) != expected {
			t.Errorf("expected current val expected: %v, got: %v", expected, v)
		}
		if iter.Next() != ErrIteratorDone {
			t.Errorf("expected only 1 value in iterator")
		}
		v, err = ss.Get([]byte("a"), ReadOptions{})
		if err != nil || string(v) != expected {
			t.Errorf("expected %s, got: %s, err: %v", expected, v, err)
		}
	}

	checkVal(expectVal)
}

func TestIteratorSeekTo(t *testing.T) {
	testIteratorSeekTo(t, true, true, nil, nil)
	testIteratorSeekTo(t, true, true, []byte("1"), nil)
	testIteratorSeekTo(t, true, true, []byte("1"), []byte("900"))
	testIteratorSeekTo(t, true, true, nil, []byte("900"))

	testIteratorSeekTo(t, true, false, nil, nil)
	testIteratorSeekTo(t, true, false, []byte("1"), nil)
	testIteratorSeekTo(t, true, false, []byte("1"), []byte("900"))
	testIteratorSeekTo(t, true, false, nil, []byte("900"))

	testIteratorSeekTo(t, false, true, nil, nil)
	testIteratorSeekTo(t, false, true, []byte("1"), nil)
	testIteratorSeekTo(t, false, true, []byte("1"), []byte("900"))
	testIteratorSeekTo(t, false, true, nil, []byte("900"))

	testIteratorSeekTo(t, false, false, nil, nil)
	testIteratorSeekTo(t, false, false, []byte("1"), nil)
	testIteratorSeekTo(t, false, false, []byte("1"), []byte("900"))
	testIteratorSeekTo(t, false, false, nil, []byte("900"))
}

func testIteratorSeekTo(t *testing.T, startEarly, oneBatch bool,
	startKey, endKey []byte) {
	m, _ := NewCollection(CollectionOptions{})

	if startEarly {
		m.Start()
	}

	if oneBatch {
		// Insert 1, 3, 5, 7, 9
		batch, _ := m.NewBatch(0, 0)
		for i := 1; i <= 9; i += 2 {
			x := []byte(fmt.Sprintf("%d", i))
			batch.Set(x, x)
		}
		m.ExecuteBatch(batch, WriteOptions{})
		batch.Close()
	} else {
		// Insert 1, 3, 5
		batch, _ := m.NewBatch(0, 0)
		for i := 1; i <= 5; i += 2 {
			x := []byte(fmt.Sprintf("%d", i))
			batch.Set(x, x)
		}
		m.ExecuteBatch(batch, WriteOptions{})
		batch.Close()

		// Insert 7, 9
		batch, _ = m.NewBatch(0, 0)
		for i := 7; i <= 9; i += 2 {
			x := []byte(fmt.Sprintf("%d", i))
			batch.Set(x, x)
		}
		m.ExecuteBatch(batch, WriteOptions{})
		batch.Close()
	}

	ss, err := m.Snapshot()
	if err != nil {
		t.Errorf("expected no snapshot err, got: %v", err)
	}

	itr, err := ss.StartIterator(nil, nil, IteratorOptions{})
	if err != nil {
		t.Errorf("expected no itr err, got: %v", err)
	}

	err = itr.SeekTo([]byte("0"))
	if err == ErrIteratorDone {
		t.Errorf("expected done, got: %v", err)
	}

	gotk, _, err := itr.Current()
	if err != nil {
		t.Errorf("expected no Current err, got: %v", err)
	}

	expectedk := "1"
	if expectedk != string(gotk) {
		t.Errorf("expected: %s, gotk: %s", expectedk, string(gotk))
	}

	for i := 0; i < 2; i++ {
		err = itr.SeekTo([]byte("1"))
		if err == ErrIteratorDone {
			t.Errorf("expected done, got: %v", err)
		}

		gotk, _, err = itr.Current()
		if err != nil {
			t.Errorf("expected no Current err, got: %v", err)
		}

		expectedk := "1"
		if expectedk != string(gotk) {
			t.Errorf("expected: %s, gotk: %s", expectedk, string(gotk))
		}
	}

	for i := 1; i < 10; i++ {
		k := []byte(fmt.Sprintf("%d", i))

		err = itr.SeekTo(k)
		if err != nil {
			t.Errorf("expected no SeekTo err, got: %v", err)
		}

		gotk, _, err = itr.Current()
		if err != nil {
			t.Errorf("expected no Current err, got: %v", err)
		}

		expected := i
		if i%2 == 0 {
			expected = i + 1
		}

		expectedk := fmt.Sprintf("%d", expected)
		if expectedk != string(gotk) {
			t.Errorf("i: %d, expected: %s, gotk: %s", i, expectedk, string(gotk))
		}
	}

	err = itr.SeekTo([]byte("999"))
	if err != ErrIteratorDone {
		t.Errorf("expected done, got: %v", err)
	}

	err = itr.SeekTo([]byte("999"))
	if err != ErrIteratorDone {
		t.Errorf("expected done, got: %v", err)
	}

	if !startEarly {
		m.Start()
	}

	itr.Close()
	ss.Close()
	m.Close()
}

func TestSharedPrefixLen(t *testing.T) {
	if sharedPrefixLen(nil, []byte("hi")) != 0 {
		t.Errorf("not matched")
	}
	if sharedPrefixLen([]byte("hi"), nil) != 0 {
		t.Errorf("not matched")
	}
	if sharedPrefixLen(nil, nil) != 0 {
		t.Errorf("not matched")
	}

	tests := []struct {
		a, b string
		exp  int
	}{
		{"x", "", 0},
		{"", "x", 0},
		{"x", "x", 1},
		{"x", "xy", 1},
		{"xy", "x", 1},
		{"x", "xx", 1},
		{"xx", "x", 1},
		{"xx", "xx", 2},
		{"xx", "xxy", 2},
		{"xxy", "xx", 2},
	}

	for _, test := range tests {
		if sharedPrefixLen([]byte(test.a), []byte(test.b)) != test.exp {
			t.Errorf("didn't match on test: %#v", test)
		}
	}
}

func TestIteratorSingleDone(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	store, m, err := OpenStoreCollection(tmpDir,
		StoreOptions{}, StorePersistOptions{CompactionConcern: CompactionForce})
	if err != nil || m == nil || store == nil {
		t.Errorf("expected open empty store collection to work")
	}
	defer m.Close()
	defer store.Close()

	mc := m.(*collection)

	setVal := func(s string) {
		b, err2 := m.NewBatch(0, 0)
		if err2 != nil || b == nil {
			t.Errorf("expected b ok")
		}
		b.Set([]byte(s), []byte(s))
		err2 = m.ExecuteBatch(b, WriteOptions{})
		if err2 != nil {
			t.Errorf("expected execute batch ok")
		}
	}

	for _, mv := range []string{"a", "b"} {
		setVal(mv)
	}

	mc.NotifyMerger("mergeAll", true)

	waitUntilClean := func() error {
		for {
			stats, err2 := m.Stats()
			if err2 != nil {
				return err2
			}

			if stats.CurDirtyOps <= 0 &&
				stats.CurDirtyBytes <= 0 &&
				stats.CurDirtySegments <= 0 {
				break
			}

			time.Sleep(200 * time.Millisecond)
		}

		return nil
	}

	waitUntilClean()

	ss, err := m.Snapshot()
	if err != nil {
		t.Errorf("expected ss ok")
	}
	iter, err := ss.StartIterator([]byte("b"), nil, IteratorOptions{})
	if err != nil || iter == nil {
		t.Errorf("expected iter")
	}
	_, ok := iter.(*iteratorSingle)
	if !ok {
		t.Errorf("expected iteratorSingle")
	}

	k, v, err := iter.Current()
	if err != nil {
		t.Errorf("expected nil err")
	}
	if string(k) != "b" {
		t.Errorf("expected b key")
	}
	if string(v) != "b" {
		t.Errorf("expected b val")
	}
	err = iter.Next()
	if err != ErrIteratorDone {
		t.Errorf("expected Next() ErrIteratorDone, got: %v", err)
	}

	for i := 0; i < 10; i++ {
		k, v, err := iter.Current()
		if err != ErrIteratorDone {
			t.Errorf("loop - expected Current() ErrIteratorDone, got: %v", err)
		}
		if k != nil {
			t.Errorf("loop - expected nil key, got: %v", k)
		}
		if v != nil {
			t.Errorf("loop - expected nil val, got: %v", v)
		}
		err = iter.Next()
		if err != ErrIteratorDone {
			t.Errorf("loop - expected Next() ErrIteratorDone, got: %v", err)
		}
	}
}
