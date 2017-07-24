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
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestNewCollection(t *testing.T) {
	m, err := NewCollection(CollectionOptions{})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	err = m.Start()
	if err != nil {
		t.Errorf("expected Start ok")
	}

	err = m.Close()
	if err != nil {
		t.Errorf("expected Close ok")
	}
}

func TestNewCollectionCloseEvents(t *testing.T) {
	events := map[EventKind]int{}
	durations := map[EventKind]time.Duration{}

	co := CollectionOptions{
		OnEvent: func(e Event) {
			events[e.Kind]++
			durations[e.Kind] += e.Duration
		},
	}

	m, err := NewCollection(co)
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	co2 := m.Options()
	if co2.OnEvent == nil {
		t.Errorf("expected non-nil on-event")
	}

	err = m.Start()
	if err != nil {
		t.Errorf("expected Start ok")
	}

	b, err := m.NewBatch(0, 0)
	if err != nil || b == nil {
		t.Errorf("expected b ok")
	}

	b.Set([]byte("a"), []byte("A"))

	err = m.Close()
	if err != nil {
		t.Errorf("expected Close ok")
	}

	if len(events) != 2 ||
		events[EventKindCloseStart] != 1 ||
		events[EventKindClose] != 1 {
		t.Errorf("expected 2 close events")
	}

	// On platforms where timing granularity is too coarse, the close operation
	// cannot be timed within that granularity (1ms) reliably.
	if !IsTimingCoarse &&
		durations[EventKindClose] <= 0 {
		t.Errorf("expected close to take some time")
	}

	b1, err := m.NewBatch(0, 0)
	if err != ErrClosed || b1 != nil {
		t.Errorf("expected err-close")
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != ErrClosed {
		t.Errorf("expected err-close")
	}

	ss, err := m.Snapshot()
	if err != ErrClosed || ss != nil {
		t.Errorf("expected err-close")
	}
}

func TestEmpty(t *testing.T) {
	m, err := NewCollection(CollectionOptions{})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	err = m.Start()
	if err != nil {
		t.Errorf("expected Start ok")
	}

	ss, err := m.Snapshot()
	if err != nil || ss == nil {
		t.Errorf("expected ss")
	}

	v, err := ss.Get([]byte("a"), ReadOptions{})
	if err != nil || v != nil {
		t.Errorf("expected no a")
	}

	iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
	if err != nil || iter == nil {
		t.Errorf("expected iter")
	}

	err = iter.Next()
	if err != ErrIteratorDone {
		t.Errorf("expected done")
	}

	k, v, err := iter.Current()
	if err != ErrIteratorDone || k != nil || v != nil {
		t.Errorf("expected done")
	}

	err = iter.Close()
	if err != nil {
		t.Errorf("expected ok")
	}

	err = ss.Close()
	if err != nil {
		t.Errorf("expected ok")
	}

	b, err := m.NewBatch(0, 0)
	if err != nil || b == nil {
		t.Errorf("expected b")
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected ok")
	}

	err = b.Close()
	if err != nil {
		t.Errorf("expected ok")
	}

	ss, err = m.Snapshot()
	if err != nil || ss == nil {
		t.Errorf("expected ss")
	}

	v, err = ss.Get([]byte("a"), ReadOptions{})
	if err != nil || v != nil {
		t.Errorf("expected no a")
	}

	iter, err = ss.StartIterator(nil, nil, IteratorOptions{})
	if err != nil || iter == nil {
		t.Errorf("expected iter")
	}

	err = iter.Next()
	if err != ErrIteratorDone {
		t.Errorf("expected done")
	}

	k, v, err = iter.Current()
	if err != ErrIteratorDone || k != nil || v != nil {
		t.Errorf("expected done")
	}

	err = iter.Close()
	if err != nil {
		t.Errorf("expected ok")
	}

	err = ss.Close()
	if err != nil {
		t.Errorf("expected ok")
	}

	err = m.Close()
	if err != nil {
		t.Errorf("expected Close ok")
	}
}

func TestBatchSort(t *testing.T) {
	m, _ := NewCollection(CollectionOptions{})

	segbatch, err := m.NewBatch(0, 0)
	if err != nil {
		t.Errorf("expected ok")
	}

	b := segbatch.(*batch)

	b.Set([]byte("f"), []byte("F"))
	b.Set([]byte("d"), []byte("D"))
	b.Set([]byte("b"), []byte("B"))

	o, k, v := b.getOperationKeyVal(0)
	if o != OperationSet || string(k) != "f" || string(v) != "F" {
		t.Errorf("wrong okv")
	}
	o, k, v = b.getOperationKeyVal(1)
	if o != OperationSet || string(k) != "d" || string(v) != "D" {
		t.Errorf("wrong okv")
	}
	o, k, v = b.getOperationKeyVal(2)
	if o != OperationSet || string(k) != "b" || string(v) != "B" {
		t.Errorf("wrong okv")
	}

	sort.Sort(b)

	b2 := b

	o, k, v = b2.getOperationKeyVal(0)
	if o != OperationSet || string(k) != "b" || string(v) != "B" {
		t.Errorf("wrong okv")
	}
	o, k, v = b2.getOperationKeyVal(1)
	if o != OperationSet || string(k) != "d" || string(v) != "D" {
		t.Errorf("wrong okv")
	}
	o, k, v = b2.getOperationKeyVal(2)
	if o != OperationSet || string(k) != "f" || string(v) != "F" {
		t.Errorf("wrong okv")
	}
	o, k, v = b2.getOperationKeyVal(3)
	if o != 0 || k != nil || v != nil {
		t.Errorf("wrong okv")
	}

	i := b2.findStartKeyInclusivePos(nil)
	if i != 0 {
		t.Errorf("wrong i")
	}
	i = b2.findStartKeyInclusivePos([]byte(""))
	if i != 0 {
		t.Errorf("wrong i")
	}
	i = b2.findStartKeyInclusivePos([]byte("a"))
	if i != 0 {
		t.Errorf("wrong i")
	}
	i = b2.findStartKeyInclusivePos([]byte("b"))
	if i != 0 {
		t.Errorf("wrong i")
	}
	i = b2.findStartKeyInclusivePos([]byte("c"))
	if i != 1 {
		t.Errorf("wrong i")
	}
	i = b2.findStartKeyInclusivePos([]byte("d"))
	if i != 1 {
		t.Errorf("wrong i")
	}
	i = b2.findStartKeyInclusivePos([]byte("e"))
	if i != 2 {
		t.Errorf("wrong i")
	}
	i = b2.findStartKeyInclusivePos([]byte("f"))
	if i != 2 {
		t.Errorf("wrong i")
	}
	i = b2.findStartKeyInclusivePos([]byte("g"))
	if i != 3 {
		t.Errorf("wrong i")
	}
}

func TestOpsAsyncMergeBatchSize1(t *testing.T) {
	m, err := NewCollection(CollectionOptions{})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	m.Start()
	testOpsBatchSize1(t, m)
	m.Close()
}

func testOpsBatchSize1(t *testing.T, m Collection) {
	tests := []struct {
		op string
		k  string
		v  string

		expErr error
	}{
		{"get", "a", "_", nil},
		{"get", "b", "_", nil},
		{"itr", "_:_", "", nil},
		{"itr", "a:_", "", nil},
		{"itr", "_:b", "", nil},
		{"itr", "a:b", "", nil},
		{"itr", "b:a", "", nil},

		{"set", "a", "A", nil},
		{"get", "a", "A", nil},
		{"get", "b", "_", nil},
		{"itr", "_:_", "+a=A", nil},
		{"itr", "a:_", "+a=A", nil},
		{"itr", "_:b", "+a=A", nil},
		{"itr", "a:a", "", nil},
		{"itr", "b:b", "", nil},
		{"itr", "b:a", "", nil},
		{"itr", "b:_", "", nil},

		{"del", "a", "_", nil},
		{"get", "a", "_", nil},
		{"get", "b", "_", nil},
		{"itr", "_:_", "", nil},
		{"itr", "a:_", "", nil},
		{"itr", "_:b", "", nil},
		{"itr", "a:a", "", nil},
		{"itr", "b:b", "", nil},
		{"itr", "b:a", "", nil},
		{"itr", "b:_", "", nil},

		{"set", "f", "F", nil},
		{"set", "d", "D", nil},
		{"set", "b", "B", nil},
		{"get", "a", "_", nil},
		{"get", "b", "B", nil},
		{"get", "c", "_", nil},
		{"get", "d", "D", nil},
		{"get", "e", "_", nil},
		{"get", "f", "F", nil},
		{"get", "g", "_", nil},
		{"itr", "_:_", "+b=B,+d=D,+f=F", nil},
		{"itr", "a:_", "+b=B,+d=D,+f=F", nil},
		{"itr", "b:_", "+b=B,+d=D,+f=F", nil},
		{"itr", "c:_", "+d=D,+f=F", nil},
		{"itr", "d:_", "+d=D,+f=F", nil},
		{"itr", "e:_", "+f=F", nil},
		{"itr", "f:_", "+f=F", nil},
		{"itr", "g:_", "", nil},

		{"set", "d", "DD", nil},
		{"set", "b", "BBB", nil},
		{"get", "a", "_", nil},
		{"get", "b", "BBB", nil},
		{"get", "c", "_", nil},
		{"get", "d", "DD", nil},
		{"get", "e", "_", nil},
		{"get", "f", "F", nil},
		{"get", "g", "_", nil},
		{"itr", "_:_", "+b=BBB,+d=DD,+f=F", nil},
		{"itr", "a:_", "+b=BBB,+d=DD,+f=F", nil},
		{"itr", "b:_", "+b=BBB,+d=DD,+f=F", nil},
		{"itr", "c:_", "+d=DD,+f=F", nil},
		{"itr", "d:_", "+d=DD,+f=F", nil},
		{"itr", "e:_", "+f=F", nil},
		{"itr", "f:_", "+f=F", nil},
		{"itr", "g:_", "", nil},

		{"itr", "_:g", "+b=BBB,+d=DD,+f=F", nil},
		{"itr", "_:f", "+b=BBB,+d=DD", nil},
		{"itr", "_:e", "+b=BBB,+d=DD", nil},
		{"itr", "_:d", "+b=BBB", nil},
		{"itr", "_:c", "+b=BBB", nil},
		{"itr", "_:b", "", nil},
		{"itr", "_:a", "", nil},
		{"itr", "f:a", "", nil},
		{"itr", "e:b", "", nil},
		{"itr", "d:c", "", nil},
		{"itr", "d:d", "", nil},

		{"del", "d", "_", nil},
		{"get", "a", "_", nil},
		{"get", "b", "BBB", nil},
		{"get", "c", "_", nil},
		{"get", "d", "_", nil},
		{"get", "e", "_", nil},
		{"get", "f", "F", nil},
		{"get", "g", "_", nil},
		{"itr", "_:_", "+b=BBB,+f=F", nil},
		{"itr", "a:_", "+b=BBB,+f=F", nil},
		{"itr", "b:_", "+b=BBB,+f=F", nil},
		{"itr", "c:_", "+f=F", nil},
		{"itr", "d:_", "+f=F", nil},
		{"itr", "e:_", "+f=F", nil},
		{"itr", "f:_", "+f=F", nil},
		{"itr", "g:_", "", nil},

		{"del", "b", "_", nil},
		{"del", "f", "_", nil},
		{"get", "a", "_", nil},
		{"get", "b", "_", nil},
		{"get", "c", "_", nil},
		{"get", "d", "_", nil},
		{"get", "e", "_", nil},
		{"get", "f", "_", nil},
		{"get", "g", "_", nil},
		{"itr", "_:_", "", nil},
		{"itr", "a:_", "", nil},
		{"itr", "b:_", "", nil},
		{"itr", "c:_", "", nil},
		{"itr", "d:_", "", nil},
		{"itr", "e:_", "", nil},
		{"itr", "f:_", "", nil},
		{"itr", "g:_", "", nil},
	}

	toBytes := func(s string) []byte {
		if s == "_" {
			return nil
		}
		return []byte(s)
	}

	for testi, test := range tests {
		if test.op == "get" {
			ss, err := m.Snapshot()
			if err != nil || ss == nil {
				t.Errorf("get, testi: %d, test: %#v, expected ss ok",
					testi, test)
			}

			vGot, err := ss.Get(toBytes(test.k), ReadOptions{})
			if err != test.expErr {
				t.Errorf("get, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}

			vExp := toBytes(test.v)
			if (vExp == nil && vGot != nil) ||
				(vExp != nil && vGot == nil) ||
				string(vExp) != string(vGot) {
				t.Errorf("get, testi: %d, test: %v, vExp: %s, vGot: %s",
					testi, test, vExp, vGot)
			}

			err = ss.Close()
			if err != nil {
				t.Errorf("get, testi: %d, test: %#v, expected ss close ok",
					testi, test)
			}
		}

		if test.op == "set" {
			b, err := m.NewBatch(0, 0)
			if err != nil || b == nil {
				t.Errorf("set, testi: %d, test: %#v, err: %v,"+
					" expected b ok",
					testi, test, err)
			}

			err = b.Set(toBytes(test.k), toBytes(test.v))
			if err != test.expErr {
				t.Errorf("set, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}

			err = m.ExecuteBatch(b, WriteOptions{})
			if err != nil {
				t.Errorf("set, testi: %d, test: %#v, err: %v,"+
					" expected execute batch ok",
					testi, test, err)
			}

			err = b.Close()
			if err != nil {
				t.Errorf("set, testi: %d, test: %#v, err: %v,"+
					" expected b close ok",
					testi, test, err)
			}
		}

		if test.op == "del" {
			b, err := m.NewBatch(0, 0)
			if err != nil || b == nil {
				t.Errorf("del, testi: %d, test: %#v, expected b ok",
					testi, test)
			}

			err = b.Del(toBytes(test.k))
			if err != test.expErr {
				t.Errorf("del, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}

			err = m.ExecuteBatch(b, WriteOptions{})
			if err != nil {
				t.Errorf("det, testi: %d, test: %#v, err: %v,"+
					" expected execute batch ok",
					testi, test, err)
			}

			err = b.Close()
			if err != nil {
				t.Errorf("det, testi: %d, test: %#v, err: %v,"+
					" expected b close ok",
					testi, test, err)
			}
		}
		if test.op == "itr" {
			ss, err := m.Snapshot()
			if err != nil || ss == nil {
				t.Errorf("itr, testi: %d, test: %#v, expected ss ok",
					testi, test)
			}

			startEndKeys := strings.Split(test.k, ":")
			startKey := toBytes(startEndKeys[0])
			endKey := toBytes(startEndKeys[1])

			itrObj, err := ss.StartIterator(startKey, endKey, IteratorOptions{})
			if err != test.expErr {
				t.Errorf("itr, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}

			itr, ok := itrObj.(*iterator)
			if false && ok {
				fmt.Printf("  itr: %#v, %s, %s\n", itr, startKey, endKey)
				for i, b := range itr.ss.a {
					fmt.Printf("    batch: %d %#v\n", i, b)
				}
				for i, c := range itr.cursors {
					fmt.Printf("    cursor: %d %#v\n", i, c)
				}
			}

			var expEntries []string
			if len(test.v) > 0 {
				expEntries = strings.Split(test.v, ",")
			}

			var gotEntries []string

			for {
				entryEx, gotK, gotV, gotErr := itrObj.CurrentEx()

				gotOp := entryEx.Operation

				// fmt.Printf("    curr: %x %s %s %v\n",
				//     gotOp, gotK, gotV, gotErr)

				if gotErr == ErrIteratorDone {
					break
				}
				if gotErr != nil {
					t.Errorf("itr, testi: %d, test: %#v, curr gotErr: %v",
						testi, test, gotErr)
				}

				s := ""
				if gotOp == OperationSet {
					s = "+" + string(gotK) + "=" + string(gotV)
				}
				if gotOp == OperationDel {
					s = "-" + string(gotK) + "=" + string(gotV)
				}

				gotEntries = append(gotEntries, s)

				gotErr = itrObj.Next()
				if gotErr == ErrIteratorDone {
					break
				}
				if gotErr != nil {
					t.Errorf("itr, testi: %d, test: %#v, next gotErr: %v",
						testi, test, gotErr)
				}
			}

			if !reflect.DeepEqual(expEntries, gotEntries) {
				t.Fatalf("itr, testi: %d, test: %#v,"+
					" expEntries: %v, gotEntries: %v",
					testi, test, expEntries, gotEntries)
			}

			err = itrObj.Close()
			if err != nil {
				t.Errorf("expected Close ok")
			}
		}
	}
}

func TestOpsAsyncMerge(t *testing.T) {
	m, err := NewCollection(CollectionOptions{
		MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
	})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	m.Start()
	testOps(t, m)
	m.Close()
}

func TestOpsDeferredSort(t *testing.T) {
	m, err := NewCollection(CollectionOptions{
		MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
		DeferredSort:  true,
	})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	m.Start()
	testOps(t, m)
	m.Close()
}

type opTest struct {
	op string
	sb string // Snapshot or batch name.
	k  string
	v  string

	expErr error
}

func testOps(t *testing.T, m Collection) {
	tests := []opTest{
		{"ss+", "S", "", "", nil},
		{"get", "S", "a", "_", nil},
		{"get", "S", "b", "_", nil},
		{"itr", "S", "_:_", "", nil},
		{"itr", "S", "a:_", "", nil},
		{"itr", "S", "_:b", "", nil},
		{"itr", "S", "a:b", "", nil},
		{"itr", "S", "b:a", "", nil},
		{"ss-", "S", "", "", nil},

		// ---------------------------------

		{"bb+", "1", "", "", nil},
		{"set", "1", "a", "A", nil},
		{"bb!", "1", "", "", nil},
		{"ss+", "S", "", "", nil},
		{"get", "S", "a", "A", nil},
		{"get", "S", "b", "_", nil},
		{"itr", "S", "_:_", "+a=A", nil},
		{"itr", "S", "a:_", "+a=A", nil},
		{"itr", "S", "_:b", "+a=A", nil},
		{"itr", "S", "a:a", "", nil},
		{"itr", "S", "b:b", "", nil},
		{"itr", "S", "b:a", "", nil},
		{"itr", "S", "b:_", "", nil},
		{"ss-", "S", "", "", nil},

		// ---------------------------------

		{"bb+", "2", "", "", nil},
		{"del", "2", "a", "_", nil},
		{"bb!", "2", "", "", nil},
		{"ss+", "S", "", "", nil},
		{"get", "S", "a", "_", nil},
		{"get", "S", "b", "_", nil},
		{"itr", "S", "_:_", "", nil},
		{"itr", "S", "a:_", "", nil},
		{"itr", "S", "_:b", "", nil},
		{"itr", "S", "a:a", "", nil},
		{"itr", "S", "b:b", "", nil},
		{"itr", "S", "b:a", "", nil},
		{"itr", "S", "b:_", "", nil},
		{"ss-", "S", "", "", nil},

		// ---------------------------------

		{"bb+", "3", "", "", nil},
		{"set", "3", "f", "F", nil},
		{"set", "3", "d", "D", nil},
		{"set", "3", "b", "B", nil},
		{"bb!", "3", "", "", nil},
		{"ss+", "S", "", "", nil},
		{"get", "S", "a", "_", nil},
		{"get", "S", "b", "B", nil},
		{"get", "S", "c", "_", nil},
		{"get", "S", "d", "D", nil},
		{"get", "S", "e", "_", nil},
		{"get", "S", "f", "F", nil},
		{"get", "S", "g", "_", nil},
		{"itr", "S", "_:_", "+b=B,+d=D,+f=F", nil},
		{"itr", "S", "a:_", "+b=B,+d=D,+f=F", nil},
		{"itr", "S", "b:_", "+b=B,+d=D,+f=F", nil},
		{"itr", "S", "c:_", "+d=D,+f=F", nil},
		{"itr", "S", "d:_", "+d=D,+f=F", nil},
		{"itr", "S", "e:_", "+f=F", nil},
		{"itr", "S", "f:_", "+f=F", nil},
		{"itr", "S", "g:_", "", nil},
		{"ss-", "S", "", "", nil},

		// ---------------------------------

		{"bb+", "4", "", "", nil},
		{"set", "4", "d", "DD", nil},
		{"set", "4", "b", "BBB", nil},
		{"bb!", "4", "", "", nil},
		{"ss+", "S", "", "", nil},
		{"get", "S", "a", "_", nil},
		{"get", "S", "b", "BBB", nil},
		{"get", "S", "c", "_", nil},
		{"get", "S", "d", "DD", nil},
		{"get", "S", "e", "_", nil},
		{"get", "S", "f", "F", nil},
		{"get", "S", "g", "_", nil},
		{"itr", "S", "_:_", "+b=BBB,+d=DD,+f=F", nil},
		{"itr", "S", "a:_", "+b=BBB,+d=DD,+f=F", nil},
		{"itr", "S", "b:_", "+b=BBB,+d=DD,+f=F", nil},
		{"itr", "S", "c:_", "+d=DD,+f=F", nil},
		{"itr", "S", "d:_", "+d=DD,+f=F", nil},
		{"itr", "S", "e:_", "+f=F", nil},
		{"itr", "S", "f:_", "+f=F", nil},
		{"itr", "S", "g:_", "", nil},

		{"itr", "S", "_:g", "+b=BBB,+d=DD,+f=F", nil},
		{"itr", "S", "_:f", "+b=BBB,+d=DD", nil},
		{"itr", "S", "_:e", "+b=BBB,+d=DD", nil},
		{"itr", "S", "_:d", "+b=BBB", nil},
		{"itr", "S", "_:c", "+b=BBB", nil},
		{"itr", "S", "_:b", "", nil},
		{"itr", "S", "_:a", "", nil},
		{"itr", "S", "f:a", "", nil},
		{"itr", "S", "e:b", "", nil},
		{"itr", "S", "d:c", "", nil},
		{"itr", "S", "d:d", "", nil},
		{"ss-", "S", "", "", nil},

		// ---------------------------------

		{"bb+", "5", "", "", nil},
		{"del", "5", "d", "_", nil},
		{"bb!", "5", "", "", nil},
		{"ss+", "S", "", "", nil},
		{"get", "S", "a", "_", nil},
		{"get", "S", "b", "BBB", nil},
		{"get", "S", "c", "_", nil},
		{"get", "S", "d", "_", nil},
		{"get", "S", "e", "_", nil},
		{"get", "S", "f", "F", nil},
		{"get", "S", "g", "_", nil},
		{"itr", "S", "_:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "a:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "b:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "c:_", "+f=F", nil},
		{"itr", "S", "d:_", "+f=F", nil},
		{"itr", "S", "e:_", "+f=F", nil},
		{"itr", "S", "f:_", "+f=F", nil},
		{"itr", "S", "g:_", "", nil},
		// Keep snapshot S open.

		// ---------------------------------

		{"bb+", "6", "", "", nil},
		{"del", "6", "b", "_", nil},
		{"del", "6", "f", "_", nil},
		{"del", "6", "f", "_", nil},
		{"bb!", "6", "", "", nil},
		{"ss+", "S2", "", "", nil},
		{"get", "S2", "a", "_", nil},
		{"get", "S2", "b", "_", nil},
		{"get", "S2", "c", "_", nil},
		{"get", "S2", "d", "_", nil},
		{"get", "S2", "e", "_", nil},
		{"get", "S2", "f", "_", nil},
		{"get", "S2", "g", "_", nil},
		{"itr", "S2", "_:_", "", nil},
		{"itr", "S2", "a:_", "", nil},
		{"itr", "S2", "b:_", "", nil},
		{"itr", "S2", "c:_", "", nil},
		{"itr", "S2", "d:_", "", nil},
		{"itr", "S2", "e:_", "", nil},
		{"itr", "S2", "f:_", "", nil},
		{"itr", "S2", "g:_", "", nil},
		{"ss-", "S2", "", "", nil},

		// ---------------------------------

		// Test snapshot S was stable.
		{"get", "S", "a", "_", nil},
		{"get", "S", "b", "BBB", nil},
		{"get", "S", "c", "_", nil},
		{"get", "S", "d", "_", nil},
		{"get", "S", "e", "_", nil},
		{"get", "S", "f", "F", nil},
		{"get", "S", "g", "_", nil},
		{"itr", "S", "_:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "a:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "b:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "c:_", "+f=F", nil},
		{"itr", "S", "d:_", "+f=F", nil},
		{"itr", "S", "e:_", "+f=F", nil},
		{"itr", "S", "f:_", "+f=F", nil},
		{"itr", "S", "g:_", "", nil},

		// ---------------------------------

		{"bb+", "7", "", "", nil},
		{"set", "7", "b", "B7", nil},
		{"set", "7", "d", "D7", nil},
		{"set", "7", "f", "F7", nil},
		{"bb!", "7", "", "", nil},

		// ---------------------------------

		// Test snapshot S was stable.
		{"get", "S", "a", "_", nil},
		{"get", "S", "b", "BBB", nil},
		{"get", "S", "c", "_", nil},
		{"get", "S", "d", "_", nil},
		{"get", "S", "e", "_", nil},
		{"get", "S", "f", "F", nil},
		{"get", "S", "g", "_", nil},
		{"itr", "S", "_:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "a:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "b:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "c:_", "+f=F", nil},
		{"itr", "S", "d:_", "+f=F", nil},
		{"itr", "S", "e:_", "+f=F", nil},
		{"itr", "S", "f:_", "+f=F", nil},
		{"itr", "S", "g:_", "", nil},

		// ---------------------------------

		// Test 2 batches.
		{"bb+", "8", "", "", nil},
		{"bb+", "9", "", "", nil},
		{"set", "8", "b", "B8", nil},
		{"set", "9", "d", "D9", nil},
		{"set", "8", "f", "F8", nil},
		{"bb!", "8", "", "", nil},
		{"bb!", "9", "", "", nil},

		{"ss+", "S2", "", "", nil},
		{"get", "S2", "a", "_", nil},
		{"get", "S2", "b", "B8", nil},
		{"get", "S2", "c", "_", nil},
		{"get", "S2", "d", "D9", nil},
		{"get", "S2", "e", "_", nil},
		{"get", "S2", "f", "F8", nil},
		{"get", "S2", "g", "_", nil},
		{"itr", "S2", "_:_", "+b=B8,+d=D9,+f=F8", nil},
		{"itr", "S2", "a:_", "+b=B8,+d=D9,+f=F8", nil},
		{"itr", "S2", "b:_", "+b=B8,+d=D9,+f=F8", nil},
		{"itr", "S2", "c:_", "+d=D9,+f=F8", nil},
		{"itr", "S2", "d:_", "+d=D9,+f=F8", nil},
		{"itr", "S2", "e:_", "+f=F8", nil},
		{"itr", "S2", "f:_", "+f=F8", nil},
		{"itr", "S2", "g:_", "", nil},
		{"ss-", "S2", "", "", nil},

		// ---------------------------------

		// Test snapshot S was stable.
		{"get", "S", "a", "_", nil},
		{"get", "S", "b", "BBB", nil},
		{"get", "S", "c", "_", nil},
		{"get", "S", "d", "_", nil},
		{"get", "S", "e", "_", nil},
		{"get", "S", "f", "F", nil},
		{"get", "S", "g", "_", nil},
		{"itr", "S", "_:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "a:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "b:_", "+b=BBB,+f=F", nil},
		{"itr", "S", "c:_", "+f=F", nil},
		{"itr", "S", "d:_", "+f=F", nil},
		{"itr", "S", "e:_", "+f=F", nil},
		{"itr", "S", "f:_", "+f=F", nil},
		{"itr", "S", "g:_", "", nil},

		// ---------------------------------

		// Test merge oeprator.
		{"bb+", "10", "", "", nil},
		{"merge", "10", "m", "M", nil},
		{"bb!", "10", "", "", nil},

		{"ss+", "S3", "", "", nil},
		{"get", "S3", "m", ":M", nil},
		{"Itr", "S3", "_:_", "+b=B8,+d=D9,+f=F8,+m=:M", nil},
		{"ss-", "S3", "", "", nil},

		{"bb+", "11", "", "", nil},
		{"merge", "11", "m", "N", nil},
		{"bb!", "11", "", "", nil},

		{"ss+", "S3", "", "", nil},
		{"get", "S3", "m", ":M:N", nil},
		{"Itr", "S3", "_:_", "+b=B8,+d=D9,+f=F8,+m=:M:N", nil},
		// Keep snapshot S3 open for little.

		{"bb+", "12", "", "", nil},
		{"merge", "12", "m", "O", nil},
		{"bb!", "12", "", "", nil},

		{"ss+", "S4", "", "", nil},
		{"Itr", "S4", "_:_", "+b=B8,+d=D9,+f=F8,+m=:M:N:O", nil},
		{"ss-", "S4", "", "", nil},

		{"Itr", "S3", "_:_", "+b=B8,+d=D9,+f=F8,+m=:M:N", nil},
		{"ss-", "S3", "", "", nil},
	}

	runOpTests(t, m, tests)
}

func runOpTests(t *testing.T, m Collection, tests []opTest) {
	toBytes := func(s string) []byte {
		if s == "_" {
			return nil
		}
		return []byte(s)
	}

	batches := map[string]Batch{}
	snapshots := map[string]Snapshot{}

	for testi, test := range tests {
		if test.op == "ss+" {
			ss, err := m.Snapshot()
			if err != nil || ss == nil {
				t.Errorf("ss+, testi: %d, test: %#v, expected ss ok",
					testi, test)
			}
			if snapshots[test.sb] != nil {
				t.Errorf("ss+, snapshot %s exists", test.sb)
			}
			snapshots[test.sb] = ss
		}

		if test.op == "ss-" {
			ss := snapshots[test.sb]
			if ss == nil {
				t.Errorf("ss-, snapshot %s missing", test.sb)
			}
			err := ss.Close()
			if err != nil {
				t.Errorf("ss-, testi: %d, test: %#v, expected ss close ok",
					testi, test)
			}
			snapshots[test.sb] = nil
		}

		if test.op == "bb+" {
			b, err := m.NewBatch(0, 0)
			if err != nil || b == nil {
				t.Errorf("bb+, testi: %d, test: %#v, err: %v,"+
					" expected b ok",
					testi, test, err)
			}
			if batches[test.sb] != nil {
				t.Errorf("bb+, batch %s exists", test.sb)
			}
			batches[test.sb] = b
		}

		if test.op == "bb!" {
			b := batches[test.sb]
			if b == nil {
				t.Errorf("bb!, batch %s missing", test.sb)
			}
			err := m.ExecuteBatch(b, WriteOptions{})
			if err != nil {
				t.Errorf("bb!, testi: %d, test: %#v, err: %v,"+
					" expected execute batch ok",
					testi, test, err)
			}
			err = b.Close()
			if err != nil {
				t.Errorf("bb!, testi: %d, test: %#v, expected b close ok",
					testi, test)
			}
			batches[test.sb] = nil
		}

		if test.op == "get" {
			ss := snapshots[test.sb]
			if ss == nil {
				t.Errorf("get, testi: %d, test: %#v, expected ss ok",
					testi, test)
			}

			vGot, err := ss.Get(toBytes(test.k), ReadOptions{})
			if err != test.expErr {
				t.Errorf("get, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}

			vExp := toBytes(test.v)
			if (vExp == nil && vGot != nil) ||
				(vExp != nil && vGot == nil) ||
				string(vExp) != string(vGot) {
				t.Errorf("get, testi: %d, test: %v, vExp: %s, vGot: %s",
					testi, test, vExp, vGot)
			}
		}

		if test.op == "set" {
			b := batches[test.sb]
			if b == nil {
				t.Errorf("set, testi: %d, test: %#v,"+
					" expected b ok", testi, test)
			}

			err := b.Set(toBytes(test.k), toBytes(test.v))
			if err != test.expErr {
				t.Errorf("set, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}
		}

		if test.op == "merge" {
			b := batches[test.sb]
			if b == nil {
				t.Errorf("merge, testi: %d, test: %#v,"+
					" expected b ok", testi, test)
			}

			err := b.Merge(toBytes(test.k), toBytes(test.v))
			if err != test.expErr {
				t.Errorf("merge, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}
		}

		if test.op == "del" {
			b := batches[test.sb]
			if b == nil {
				t.Errorf("del, testi: %d, test: %#v,"+
					" expected b ok", testi, test)
			}

			err := b.Del(toBytes(test.k))
			if err != test.expErr {
				t.Errorf("del, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}
		}

		if test.op == "itr" || test.op == "Itr" {
			ss := snapshots[test.sb]
			if ss == nil {
				t.Errorf("itr, testi: %d, test: %#v, expected ss ok",
					testi, test)
			}

			startEndKeys := strings.Split(test.k, ":")
			startKey := toBytes(startEndKeys[0])
			endKey := toBytes(startEndKeys[1])

			itrObj, err := ss.StartIterator(startKey, endKey, IteratorOptions{})
			if err != test.expErr {
				t.Errorf("itr, testi: %d, test: %#v, expErr: %s, err: %s",
					testi, test, test.expErr, err)
			}

			itr, ok := itrObj.(*iterator)
			if false && ok {
				fmt.Printf("  itr: %#v, %s, %s\n", itr, startKey, endKey)
				for i, b := range itr.ss.a {
					fmt.Printf("    batch: %d %#v\n", i, b)
				}
				for i, c := range itr.cursors {
					fmt.Printf("    cursor: %d %#v\n", i, c)
				}
			}

			var expEntries []string
			if len(test.v) > 0 {
				expEntries = strings.Split(test.v, ",")
			}

			var gotEntries []string

			if test.op == "itr" {
				for {
					entryEx, gotK, gotV, gotErr := itrObj.CurrentEx()

					gotOp := entryEx.Operation

					// fmt.Printf("    curr: %x %s %s %v\n",
					//     gotOp, gotK, gotV, gotErr)

					if gotErr == ErrIteratorDone {
						break
					}
					if gotErr != nil {
						t.Errorf("itr, testi: %d, test: %#v, curr gotErr: %v",
							testi, test, gotErr)
					}

					s := ""
					if gotOp == OperationSet {
						s = "+" + string(gotK) + "=" + string(gotV)
					}
					if gotOp == OperationDel {
						s = "-" + string(gotK) + "=" + string(gotV)
					}
					if gotOp == OperationMerge {
						s = "^" + string(gotK) + "=" + string(gotV)
					}

					gotEntries = append(gotEntries, s)

					gotErr = itrObj.Next()
					if gotErr == ErrIteratorDone {
						break
					}
					if gotErr != nil {
						t.Errorf("itr, testi: %d, test: %#v, next gotErr: %v",
							testi, test, gotErr)
					}
				}
			} else { // test.op == "Itr (uses public Current() API)
				for {
					gotK, gotV, gotErr := itrObj.Current()

					// fmt.Printf("    curr: %s %s %v\n",
					//     gotK, gotV, gotErr)

					if gotErr == ErrIteratorDone {
						break
					}
					if gotErr != nil {
						t.Errorf("itr, testi: %d, test: %#v, curr gotErr: %v",
							testi, test, gotErr)
					}

					s := "+" + string(gotK) + "=" + string(gotV)

					gotEntries = append(gotEntries, s)

					gotErr = itrObj.Next()
					if gotErr == ErrIteratorDone {
						break
					}
					if gotErr != nil {
						t.Errorf("itr, testi: %d, test: %#v, next gotErr: %v",
							testi, test, gotErr)
					}
				}
			}

			if !reflect.DeepEqual(expEntries, gotEntries) {
				t.Fatalf("itr, testi: %d, test: %#v,"+
					" expEntries: %v, gotEntries: %v",
					testi, test, expEntries, gotEntries)
			}

			err = itrObj.Close()
			if err != nil {
				t.Errorf("expected Close ok")
			}
		}
	}
}

func TestAllocCollection(t *testing.T) {
	m, err := NewCollection(CollectionOptions{})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	err = m.Start()
	if err != nil {
		t.Errorf("expected Start ok")
	}

	b, err := m.NewBatch(4, 5)
	if err != nil {
		t.Errorf("expected NewBatch with preallocs ok")
	}

	a, err := b.Alloc(100)
	if err == nil || a != nil {
		t.Errorf("expected over-Alloc() to fail")
	}

	a, err = b.Alloc(2)
	if err != nil || len(a) != 2 {
		t.Errorf("expected Alloc to work")
	}
	a[0] = 'a'
	a[1] = 'A'

	err = b.AllocSet(a[0:1], a[1:2])
	if err != nil {
		t.Errorf("expected AllocSet to work")
	}

	a, err = b.Alloc(1)
	if err != nil || len(a) != 1 {
		t.Errorf("expected Alloc to work")
	}
	a[0] = 'b'

	err = b.AllocSet(a[0:1], nil)
	if err != nil {
		t.Errorf("expected AllocSet to work")
	}

	a, err = b.Alloc(1)
	if err != nil || len(a) != 1 {
		t.Errorf("expected Alloc to work")
	}
	a[0] = 'c'

	err = b.AllocDel(a[0:1])
	if err != nil {
		t.Errorf("expected AllocDel to work")
	}

	a, err = b.Alloc(1)
	if err != nil || len(a) != 1 {
		t.Errorf("expected Alloc to work")
	}
	a[0] = 'd'

	err = b.AllocMerge(a[0:1], nil)
	if err != nil {
		t.Errorf("expected AllocMerge to work")
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected ExecuteBatch to work")
	}

	err = m.Close()
	if err != nil {
		t.Errorf("expected Close ok")
	}
}

func TestCollectionStats(t *testing.T) {
	cs0 := &CollectionStats{}
	cs1 := &CollectionStats{}
	cs0.AtomicCopyTo(cs1)
	if !reflect.DeepEqual(cs0, cs1) {
		t.Errorf("expect empty compare to equal")
	}
	cs0.TotOnError = 1234
	cs0.AtomicCopyTo(cs1)
	if !reflect.DeepEqual(cs0, cs1) {
		t.Errorf("expect empty compare to equal")
	}

	m, _ := NewCollection(CollectionOptions{})
	m.Start()
	m.Close()

	s, err := m.Stats()
	if err != nil || s == nil {
		t.Errorf("expect some stats")
	}

	if reflect.DeepEqual(s, &CollectionStats{}) {
		t.Errorf("expect nonzero stats")
	}

	if s.TotCloseBeg != 1 || s.TotCloseEnd != 1 {
		t.Errorf("expect close stat == 1")
	}
}

func TestCollectionStatsClose(t *testing.T) {
	m, _ := NewCollection(CollectionOptions{})
	m.Start()

	b, err := m.NewBatch(0, 0)
	if err != nil {
		t.Errorf("expected ok")
	}

	b.Set([]byte("a"), []byte("A"))

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected exec batch ok")
	}

	s, err := m.Stats()
	if err != nil || s == nil {
		t.Errorf("expect some stats")
	}

	if s.CurDirtyBytes <= 0 {
		t.Errorf("expected dirty bytes, %#v", s)
	}

	if s.CurDirtySegments <= 0 {
		t.Errorf("expected dirty segments, %#v", s)
	}

	m.Close()

	s, err = m.Stats()
	if err != nil || s == nil {
		t.Errorf("expect some stats")
	}

	if s.CurDirtyBytes > 0 {
		t.Errorf("expected no dirty bytes after close, %#v", s)
	}

	if s.CurDirtySegments > 0 {
		t.Errorf("expected no dirty segments after close, %#v", s)
	}
}

func TestCollectionGet(t *testing.T) {
	m, _ := NewCollection(CollectionOptions{})
	m.Start()

	b, err := m.NewBatch(5, 5*4)
	if err != nil {
		t.Errorf("Expected NewBatch() to succeed!")
	}

	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte(fmt.Sprintf("v%d", i))
		b.Set(k, v)
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("Expected ExecuteBatch() to succeed!")
	}

	b, err = m.NewBatch(3, 3*4)
	if err != nil {
		t.Errorf("Expected NewBatch() to succeed!")
	}

	for i := 0; i < 3; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte(fmt.Sprintf("n%d", i))
		b.Set(k, v)
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("Expected ExecuteBatch() to succeed!")
	}

	val, err := m.Get([]byte("k1"), ReadOptions{})
	if err != nil || val == nil {
		t.Errorf("Expected Get() to succeed!")
	}
	if string(val[:]) != "n1" {
		t.Errorf("Unexpected value for k1")
	}

	val, err = m.Get([]byte("k2"), ReadOptions{})
	if err != nil || val == nil {
		t.Errorf("Expected Get() to succeed!")
	}
	if string(val[:]) != "n2" {
		t.Errorf("Unexpected value for k2")
	}

	val, err = m.Get([]byte("k4"), ReadOptions{})
	if err != nil || val == nil {
		t.Errorf("Expected Get() to succeed!")
	}
	if string(val[:]) != "v4" {
		t.Errorf("Unexpected value for k1")
	}

	val, err = m.Get([]byte("k6"), ReadOptions{})
	if err != nil {
		t.Errorf("Expected Get() to succeed!")
	}
	if val != nil {
		t.Errorf("Expected Get() to not return value!")
	}

	m.Close()

	s, err := m.Stats()
	if err != nil || s == nil {
		t.Errorf("Expect some stats")
	}

	if s.TotGet != 4 {
		t.Errorf("Unexpected number of Gets attempted!")
	}

	if s.TotGetErr != 0 {
		t.Errorf("Unexpected number of Get errors!")
	}
}
