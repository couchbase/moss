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
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPersister attempts to test that the persister gets invoked as
// expected.  It currently has some unsightly time.Sleep() calls
// since there doesn't seem to be a good way to know when the
// persister has run.
func Test1Persister(t *testing.T) {
	runTestPersister(t, 1)
}

func Test10Persister(t *testing.T) {
	runTestPersister(t, 10)
}

func Test1000Persister(t *testing.T) {
	runTestPersister(t, 1000)
}

func runTestPersister(t *testing.T, numItems int) {
	// create a new instance of our mock lower-level persister
	lowerLevelPersister := newTestPersister()
	lowerLevelUpdater := func(higher Snapshot) (Snapshot, error) {
		p, err := lowerLevelPersister.Update(higher)
		if err != nil {
			return nil, err
		}
		lowerLevelPersister = p
		return p, nil
	}

	// create new collection configured to use lower level persister
	m, err := NewCollection(
		CollectionOptions{
			LowerLevelInit:   lowerLevelPersister,
			LowerLevelUpdate: lowerLevelUpdater,
		})
	if err != nil || m == nil {
		t.Fatalf("expected moss")
	}

	// FIXME possibly replace start with manual persister invocations?
	// this would require some refactoring
	err = m.Start()
	if err != nil {
		t.Fatalf("error starting moss: %v", err)
	}

	// create new batch to set some keys
	b, err := m.NewBatch(0, 0)
	if err != nil {
		t.Fatalf("error creating new batch: %v", err)
	}

	// put numItems in
	for i := 0; i < numItems; i++ {
		k := fmt.Sprintf("%d", i)
		b.Set([]byte(k), []byte(k))
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Fatalf("error executing batch: %v", err)
	}

	ss0, err := m.Snapshot()
	if err != nil || ss0 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	// cleanup that batch
	err = b.Close()
	if err != nil {
		t.Fatalf("error closing batch: %v", err)
	}

	ss1, err := m.Snapshot()
	if err != nil || ss1 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	// wait for persister to run
	time.Sleep(1 * time.Second)

	ss2, err := m.Snapshot()
	if err != nil || ss2 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	checkSnapshot := func(msg string, ss Snapshot, expectedNum int) {
		for i := 0; i < numItems; i++ {
			k := fmt.Sprintf("%d", i)
			v, err := ss.Get([]byte(k), ReadOptions{})
			if err != nil {
				t.Fatalf("error %s getting key: %s, %v", msg, k, err)
			}
			if string(v) != k {
				t.Errorf("expected %s value for key: %s to be %s, got %s", msg, k, k, v)
			}
		}

		iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			t.Fatalf("error %s checkSnapshot iter, err: %v", msg, err)
		}

		n := 0
		var lastKey []byte
		for {
			ex, key, val, err := iter.CurrentEx()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				t.Fatalf("error %s iter currentEx, err: %v", msg, err)
			}

			n++

			if ex.Operation != OperationSet {
				t.Fatalf("error %s iter op, ex: %v, err: %v", msg, ex, err)
			}

			cmp := bytes.Compare(lastKey, key)
			if cmp >= 0 {
				t.Fatalf("error %s iter cmp: %v, err: %v", msg, cmp, err)
			}

			if bytes.Compare(key, val) != 0 {
				t.Fatalf("error %s iter key != val: %v, %v", msg, key, val)
			}

			lastKey = key

			err = iter.Next()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				t.Fatalf("error %s iter next, err: %v", msg, err)
			}
		}

		if n != expectedNum {
			t.Fatalf("error %s iter expectedNum: %d, got: %d", msg, expectedNum, n)
		}

		iter.Close()
	}

	checkSnapshot("lowerLevelPersister", lowerLevelPersister, numItems)
	checkSnapshot("ss0", ss0, numItems)
	checkSnapshot("ss1", ss1, numItems)
	checkSnapshot("ss2", ss2, numItems)

	// cleanup that batch
	err = b.Close()
	if err != nil {
		t.Fatalf("error closing batch: %v", err)
	}

	// open new batch
	b, err = m.NewBatch(0, 0)
	if err != nil {
		t.Fatalf("error creating new batch: %v", err)
	}

	// delete the values we just set
	for i := 0; i < numItems; i++ {
		k := fmt.Sprintf("%d", i)
		b.Del([]byte(k))
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Fatalf("error executing batch: %v", err)
	}

	ssd0, err := m.Snapshot()
	if err != nil || ssd0 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	// cleanup that batch
	err = b.Close()
	if err != nil {
		t.Fatalf("error closing batch: %v", err)
	}

	ssd1, err := m.Snapshot()
	if err != nil || ssd1 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	time.Sleep(1 * time.Second)

	ssd2, err := m.Snapshot()
	if err != nil || ssd2 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	// check that values are now gone
	checkGetsGone := func(ss Snapshot) {
		for i := 0; i < numItems; i++ {
			k := fmt.Sprintf("%d", i)
			v, err := ss.Get([]byte(k), ReadOptions{})
			if err != nil {
				t.Fatalf("error getting key: %s, %v", k, err)
			}
			if v != nil {
				t.Errorf("expected no value for key: %s, got %s", k, v)
			}
		}
	}

	checkGetsGone(lowerLevelPersister)
	checkGetsGone(ssd0)
	checkGetsGone(ssd1)
	checkGetsGone(ssd2)

	// Check that our old snapshots are still stable.
	checkSnapshot("ss0", ss0, numItems)
	checkSnapshot("ss1", ss1, numItems)
	checkSnapshot("ss2", ss2, numItems)

	// cleanup moss
	err = m.Close()
	if err != nil {
		t.Fatalf("error closing moss: %v", err)
	}
}

// TestPersisterError ensures that if the provided LowerLevelUpdate
// method returns an error, the configured OnError callback is
// invoked
func TestPersisterError(t *testing.T) {
	errCallbackInvoked := false
	customOnError := func(err error) {
		errCallbackInvoked = true
	}

	// create a new instance of our mock lower-level persister
	lowerLevelPersister := newTestPersister()
	lowerLevelUpdater := func(higher Snapshot) (Snapshot, error) {
		return nil, fmt.Errorf("test error")
	}

	// create new collection configured to use lower level persister
	m, err := NewCollection(
		CollectionOptions{
			LowerLevelInit:   lowerLevelPersister,
			LowerLevelUpdate: lowerLevelUpdater,
			OnError:          customOnError,
		})
	if err != nil || m == nil {
		t.Fatalf("expected moss")
	}

	// FIXME possibly replace start with manual persister invocations?
	// this would require some refactoring
	err = m.Start()
	if err != nil {
		t.Fatalf("error starting moss: %v", err)
	}

	// create new batch to set some keys
	b, err := m.NewBatch(0, 0)
	if err != nil {
		t.Fatalf("error creating new batch: %v", err)
	}

	// put 100 values in
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%d", i)
		b.Set([]byte(k), []byte(k))
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Fatalf("error executing batch: %v", err)
	}

	// wait for persister to run
	time.Sleep(1 * time.Second)

	if !errCallbackInvoked {
		t.Errorf("expected error callback to be invoked")
	}
}

// -----------------------------------------------------------------------------
// implementation of mock lower-level test persister and iterator,
// with COW, using map that's cloned on updates and with key sorting
// whenever an iterator is needed.

type testPersisterIterator struct {
	pos     int
	kvpairs map[string][]byte // immutable.
	keys    []string          // immutable, sorted.
	endkey  string
}

func newTestPersisterIterator(kvpairs map[string][]byte,
	startkey, endkey string) *testPersisterIterator {
	rv := &testPersisterIterator{
		kvpairs: kvpairs,
		endkey:  endkey,
	}
	for k := range rv.kvpairs {
		rv.keys = append(rv.keys, k)
	}
	sort.Strings(rv.keys)
	rv.pos = sort.SearchStrings(rv.keys, string(startkey))
	return rv
}

func (i *testPersisterIterator) Close() error {
	i.kvpairs = nil
	i.keys = nil
	return nil
}

func (i *testPersisterIterator) Next() error {
	i.pos++
	if i.pos >= len(i.keys) {
		return ErrIteratorDone
	}
	return nil
}

func (i *testPersisterIterator) Current() ([]byte, []byte, error) {
	if i.pos >= len(i.keys) {
		return nil, nil, ErrIteratorDone
	}
	k := i.keys[i.pos]
	if i.endkey != "" && strings.Compare(k, i.endkey) >= 0 {
		return nil, nil, ErrIteratorDone
	}
	return []byte(k), i.kvpairs[k], nil
}

func (i *testPersisterIterator) CurrentEx() (entryEx EntryEx,
	key, val []byte, err error) {
	k, v, err := i.Current()
	if err != nil {
		return EntryEx{OperationSet}, nil, nil, err
	}
	return EntryEx{OperationSet}, k, v, err
}

type testPersister struct {
	// stable snapshots through writes blocking reads
	mutex sync.RWMutex

	kvpairs map[string][]byte
}

func newTestPersister() *testPersister {
	return &testPersister{
		kvpairs: map[string][]byte{},
	}
}

func (p *testPersister) cloneLOCKED() *testPersister {
	c := newTestPersister()
	for k, v := range p.kvpairs {
		c.kvpairs[k] = v
	}
	return c
}

func (p *testPersister) Close() error {
	// ensure any writes in progress finish
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.kvpairs = nil
	return nil
}

func (p *testPersister) Get(key []byte,
	readOptions ReadOptions) ([]byte, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.kvpairs[string(key)], nil
}

func (p *testPersister) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	p.mutex.RLock() // closing iterator unlocks
	defer p.mutex.RUnlock()
	return newTestPersisterIterator(p.cloneLOCKED().kvpairs,
		string(startKeyInclusive), string(endKeyExclusive)), nil
}

func (p *testPersister) Update(higher Snapshot) (*testPersister, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	c := p.cloneLOCKED()

	if higher != nil {
		iter, err := higher.StartIterator(nil, nil, IteratorOptions{
			IncludeDeletions: true,
			SkipLowerLevel:   true,
		})
		if err != nil {
			return nil, err
		}

		defer iter.Close()

		var readOptions ReadOptions

		for {
			ex, key, val, err := iter.CurrentEx()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				return nil, err
			}

			switch ex.Operation {
			case OperationSet:
				c.kvpairs[string(key)] = val

			case OperationDel:
				delete(c.kvpairs, string(key))

			case OperationMerge:
				val, err = higher.Get(key, readOptions)
				if err != nil {
					return nil, err
				}

				if val != nil {
					c.kvpairs[string(key)] = val
				} else {
					delete(c.kvpairs, string(key))
				}

			default:
				return nil, fmt.Errorf("moss testPersister, update,"+
					" unexpected operation, ex: %v", ex)
			}

			err = iter.Next()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				return nil, err
			}
		}
	}

	return c, nil
}
