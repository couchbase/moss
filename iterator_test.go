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
	"sync"
	"testing"
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
	if len(events) <= 0 {
		t.Errorf("expected some events")
	}
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
