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
func TestPersister(t *testing.T) {

	// create a new instance of our mock lower-level persister
	lowerLevelPersister := newOrderedMapPersister()
	lowerLevelUpdater := func(higher Snapshot) (Snapshot, error) {
		err := lowerLevelPersister.Update(higher)
		if err != nil {
			return nil, err
		}
		return lowerLevelPersister, nil
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

	// check that keys are in our lower-level persister
	for i := 1; i < 1000; i++ {
		k := fmt.Sprintf("%d", i)
		v, err := lowerLevelPersister.Get([]byte(k), ReadOptions{})
		if err != nil {
			t.Fatalf("error getting key: %s, %v", k, err)
		}
		if string(v) != k {
			t.Errorf("expected value for key: %s to be %s, got %s", k, k, v)
		}
	}

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
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%d", i)
		b.Del([]byte(k))
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Fatalf("error executing batch: %v", err)
	}

	time.Sleep(1 * time.Second)

	// check that values are now gone
	for i := 1; i < 1000; i++ {
		k := fmt.Sprintf("%d", i)
		v, err := lowerLevelPersister.Get([]byte(k), ReadOptions{})
		if err != nil {
			t.Fatalf("error getting key: %s, %v", k, err)
		}
		if v != nil {
			t.Errorf("expected no value for key: %s, got %s", k, v)
		}
	}

	// cleanup that batch
	err = b.Close()
	if err != nil {
		t.Fatalf("error closing batch: %v", err)
	}

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
	lowerLevelPersister := newOrderedMapPersister()
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
// implementation of mock lower-level persister
// does not attempt COW, reads block writes and writes block reads
// uses a map and sorted string slice to back data structure

// this iterator implementation was added for completeness
// but is not actually used in current tests
// future tests could verify that iteration correctly passes
// through down to this lowest layer, at which point correct
// functionality would be required
type orderedMapPersisterIterator struct {
	pos    int
	parent *orderedMapPersister
	endkey string
}

func newOrderedMapPersisterIterator(p *orderedMapPersister,
	startkey, endkey string) *orderedMapPersisterIterator {
	rv := orderedMapPersisterIterator{
		parent: p,
		endkey: endkey,
	}
	rv.pos = sort.SearchStrings(p.keys, string(startkey))
	return &rv
}

func (i *orderedMapPersisterIterator) Close() error {
	i.parent.mutex.RUnlock()
	return nil
}

func (i *orderedMapPersisterIterator) Next() error {
	i.pos++
	return nil
}

func (i *orderedMapPersisterIterator) Current() ([]byte, []byte, error) {
	if i.pos < len(i.parent.keys) {
		k := i.parent.keys[i.pos]
		if strings.Compare(k, i.endkey) > 1 {
			return nil, nil, ErrIteratorDone
		}
		return []byte(k), i.parent.kvpairs[k], nil
	}
	return nil, nil, ErrIteratorDone
}

func (i *orderedMapPersisterIterator) CurrentEx() (entryEx EntryEx,
	key, val []byte, err error) {
	k, v, err := i.Current()
	if err == ErrIteratorDone {
		return EntryEx{OperationSet}, k, v, err
	}
	return EntryEx{OperationSet}, k, v, nil
}

type orderedMapPersister struct {
	// stable snapshots through writes blocking reads
	mutex sync.RWMutex

	kvpairs map[string][]byte
	keys    sort.StringSlice
}

func newOrderedMapPersister() *orderedMapPersister {
	return &orderedMapPersister{
		kvpairs: make(map[string][]byte),
		keys:    make([]string, 0),
	}
}

func (p *orderedMapPersister) Close() error {
	// ensure any writes in progress finish
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return nil
}

func (p *orderedMapPersister) Get(key []byte,
	readOptions ReadOptions) ([]byte, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.kvpairs[string(key)], nil
}

func (p *orderedMapPersister) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	p.mutex.RLock() // closing iterator unlocks
	return newOrderedMapPersisterIterator(p,
		string(startKeyInclusive), string(endKeyExclusive)), nil
}

// must already be locked
func (p *orderedMapPersister) delete(key []byte) {
	i := sort.SearchStrings(p.keys, string(key))
	if i < len(p.keys) && p.keys[i] == string(key) {
		// found it, delete it
		p.keys = append(p.keys[:i], p.keys[i+1:]...)
		delete(p.kvpairs, string(key))
	}
}

// must already be locked
func (p *orderedMapPersister) set(key, val []byte) {
	i := sort.SearchStrings(p.keys, string(key))
	if i < len(p.keys) && p.keys[i] == string(key) {
		// key exists, overwrite it
		p.kvpairs[string(key)] = val
	} else {
		// new, need to insert
		p.keys = append(p.keys, "")
		copy(p.keys[i+1:], p.keys[i:])
		p.keys[i] = string(key)
		p.kvpairs[string(key)] = val
	}
}

func (p *orderedMapPersister) Update(higher Snapshot) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if higher != nil {
		iter, err := higher.StartIterator(nil, nil, IteratorOptions{
			IncludeDeletions: true,
			SkipLowerLevel:   true,
		})
		if err != nil {
			return err
		}

		defer iter.Close()

		var readOptions ReadOptions

		for {
			err = iter.Next()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				return err
			}

			ex, key, val, err := iter.CurrentEx()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				return err
			}

			switch ex.Operation {
			case OperationSet:
				p.set(key, val)

			case OperationDel:
				p.delete(key)

			case OperationMerge:
				val, err = higher.Get(key, readOptions)
				if err != nil {
					return err
				}

				if val != nil {
					p.set(key, val)
				} else {
					p.delete(key)
				}

			default:
				return fmt.Errorf("moss store, update,"+
					" unexpected operation, ex: %v", ex)
			}
		}
	}

	return nil
}
