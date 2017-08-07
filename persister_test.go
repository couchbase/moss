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
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Implementation of mock lower-level iterator, using map that's
// cloned and sorted on creation.
type TestPersisterIterator struct {
	pos     int
	kvpairs map[string][]byte // immutable.
	keys    []string          // immutable, sorted.
	endkey  string
}

// NewTestPersisterIterator returns an iterator, cloning the provided
// kvpairs.
func NewTestPersisterIterator(kvpairs map[string][]byte,
	startkey, endkey string) *TestPersisterIterator {
	rv := &TestPersisterIterator{
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

func (i *TestPersisterIterator) Close() error {
	i.kvpairs = nil
	i.keys = nil
	return nil
}

func (i *TestPersisterIterator) Next() error {
	i.pos++
	if i.pos >= len(i.keys) {
		return ErrIteratorDone
	}
	return nil
}

func (i *TestPersisterIterator) SeekTo(seekToKey []byte) error {
	return naiveSeekTo(i, seekToKey, 0)
}

func (i *TestPersisterIterator) Current() ([]byte, []byte, error) {
	if i.pos >= len(i.keys) {
		return nil, nil, ErrIteratorDone
	}
	k := i.keys[i.pos]
	if i.endkey != "" && strings.Compare(k, i.endkey) >= 0 {
		return nil, nil, ErrIteratorDone
	}
	return []byte(k), i.kvpairs[k], nil
}

func (i *TestPersisterIterator) CurrentEx() (entryEx EntryEx,
	key, val []byte, err error) {
	k, v, err := i.Current()
	if err != nil {
		return EntryEx{OperationSet}, nil, nil, err
	}
	return EntryEx{OperationSet}, k, v, err
}

// Implementation of mock lower-level test persister, using a map
// that's cloned on updates and with key sorting whenever an iterator
// is needed.
type TestPersister struct {
	// stable snapshots through writes blocking reads
	mutex sync.RWMutex

	kvpairs map[string][]byte
}

// NewTestPersister returns a TestPersister instance that can be used
// to test lower-level persistence features.
func NewTestPersister() *TestPersister {
	return &TestPersister{
		kvpairs: map[string][]byte{},
	}
}

func (p *TestPersister) cloneLOCKED() *TestPersister {
	c := NewTestPersister()
	for k, v := range p.kvpairs {
		c.kvpairs[k] = v
	}
	return c
}

func (p *TestPersister) Close() error {
	// ensure any writes in progress finish
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.kvpairs = nil
	return nil
}

func (p *TestPersister) Get(key []byte,
	readOptions ReadOptions) ([]byte, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.kvpairs[string(key)], nil
}

func (p *TestPersister) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	p.mutex.RLock() // closing iterator unlocks
	defer p.mutex.RUnlock()
	return NewTestPersisterIterator(p.cloneLOCKED().kvpairs,
		string(startKeyInclusive), string(endKeyExclusive)), nil
}

func (p *TestPersister) Update(higher Snapshot) (*TestPersister, error) {
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
				return nil, fmt.Errorf("moss TestPersister, update,"+
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

// ----------------------------------------------------

// TestPersister tests that the persister is invoked as expected.
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

	persisterCh := make(chan string)

	onEvent := func(event Event) {
		if event.Kind == EventKindPersisterProgress {
			persisterCh <- "persisterProgress"
		}
	}

	// create new collection configured to use lower level persister
	m, err := NewCollection(
		CollectionOptions{
			LowerLevelInit:   lowerLevelPersister,
			LowerLevelUpdate: lowerLevelUpdater,
			OnEvent:          onEvent,
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

	// also create a child batch
	childB, err := b.NewChildCollectionBatch("child1", BatchOptions{0, 0})
	if err != nil {
		t.Fatalf("error creating new child batch: %v", err)
	}

	itemLoader := func(b Batch, numItems int) {
		// put numItems in
		for i := 0; i < numItems; i++ {
			k := fmt.Sprintf("%d", i)
			b.Set([]byte(k), []byte(k))
		}
	}
	itemLoader(b, numItems)
	itemLoader(childB, numItems)

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Fatalf("error executing batch: %v", err)
	}

	ss0, err := m.Snapshot()
	if err != nil || ss0 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	childNames, err := ss0.ChildCollectionNames()
	if err != nil {
		t.Fatalf("error getting child collection names: %v", err)
	}
	if len(childNames) != 1 {
		t.Fatalf("Unable to retrieve child snapshot")
	}
	childSnap, err := ss0.ChildCollectionSnapshot("child1")
	if err != nil || ss0 == nil {
		t.Fatalf("error getting child snapshot: %v", err)
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
	<-persisterCh

	ss2, err := m.Snapshot()
	if err != nil || ss2 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	checkSnapshot := func(msg string, ss Snapshot, expectedNum int) {
		for i := 0; i < numItems; i++ {
			k := fmt.Sprintf("%d", i)
			var v []byte
			v, err = ss.Get([]byte(k), ReadOptions{})
			if err != nil {
				t.Fatalf("error %s getting key: %s, %v", msg, k, err)
			}
			if string(v) != k {
				t.Errorf("expected %s value for key: %s to be %s, got %s", msg, k, k, v)
			}
		}

		var iter Iterator
		iter, err = ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			t.Fatalf("error %s checkSnapshot iter, err: %v", msg, err)
		}

		n := 0
		var lastKey []byte
		for {
			var ex EntryEx
			var key, val []byte
			ex, key, val, err = iter.CurrentEx()
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

	checkSnapshot("ss0:child1", childSnap, numItems)

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

	err = b.DelChildCollection("child1")
	if err != nil {
		t.Fatalf("error deleting child collection: %v", err)
	}

	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Fatalf("error executing batch: %v", err)
	}

	ssd0, err := m.Snapshot()
	if err != nil || ssd0 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}
	childNames, err = ssd0.ChildCollectionNames()
	if len(childNames) > 0 {
		t.Fatalf("error child snapshot not deleted: %v", err)
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

	<-persisterCh
	go func() {
		for range persisterCh { /* EAT */
		}
	}()

	ssd2, err := m.Snapshot()
	if err != nil || ssd2 == nil {
		t.Fatalf("error snapshoting: %v", err)
	}

	// check that values are now gone
	checkGetsGone := func(ss Snapshot) {
		for i := 0; i < numItems; i++ {
			k := fmt.Sprintf("%d", i)
			var v []byte
			v, err = ss.Get([]byte(k), ReadOptions{})
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
	onErrorCh := make(chan string)
	customOnError := func(err error) {
		onErrorCh <- "error expected!"
	}

	// create a new instance of our mock lower-level persister
	lowerLevelPersister := newTestPersister()
	lowerLevelUpdater := func(higher Snapshot) (Snapshot, error) {
		return nil, fmt.Errorf("test error")
	}

	gotPersistence := false
	onEvent := func(event Event) {
		if event.Kind == EventKindPersisterProgress {
			gotPersistence = true
		}
	}

	// create new collection configured to use lower level persister
	m, err := NewCollection(
		CollectionOptions{
			LowerLevelInit:   lowerLevelPersister,
			LowerLevelUpdate: lowerLevelUpdater,
			OnError:          customOnError,
			OnEvent:          onEvent,
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
	msg := <-onErrorCh
	if msg != "error expected!" {
		t.Errorf("expected error callback")
	}

	if gotPersistence {
		t.Errorf("expected no persistence due to error")
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

func (i *testPersisterIterator) SeekTo(seekToKey []byte) error {
	return naiveSeekTo(i, seekToKey, 0)
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

// Implements the moss.Snapshot interface
type testPersister struct {
	// stable snapshots through writes blocking reads
	mutex sync.RWMutex

	kvpairs map[string][]byte

	childSnapshots map[string]*testPersister
}

func newTestPersister() *testPersister {
	return &testPersister{
		kvpairs:        map[string][]byte{},
		childSnapshots: make(map[string]*testPersister),
	}
}

func (p *testPersister) cloneLOCKED() *testPersister {
	c := newTestPersister()
	for k, v := range p.kvpairs {
		c.kvpairs[k] = v
	}
	return c
}

// ChildCollectionNames returns an array of child collection name strings.
func (p *testPersister) ChildCollectionNames() ([]string, error) {
	var childCollections = make([]string, len(p.childSnapshots))
	idx := 0
	for name := range p.childSnapshots {
		childCollections[idx] = name
		idx++
	}
	return childCollections, nil
}

// ChildCollectionSnapshot returns a Snapshot on a given child
// collection by its name.
func (p *testPersister) ChildCollectionSnapshot(childCollectionName string) (
	Snapshot, error) {
	childSnapshot, exists := p.childSnapshots[childCollectionName]
	if !exists {
		return nil, ErrNoSuchCollection
	}
	return childSnapshot, nil
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

func TestPersistMergeOps_MB19667(t *testing.T) {
	// Need to arrange that...
	// - stack dirty top = empty
	// - stack dirty mid = [ various merge ops Z ]
	// - stack dirty base = [ more merge ops Y ]
	// - lower-level has stuff (X)
	//
	// Then persister runs and...
	// - stack dirty base = [ (empty) ]
	// - lower-level has more stuff (X + Y)
	//
	// But, stack dirty mid was (incorrectly) pointing at old lower
	// level snapshot (X), which doesn't have anything from Y.  Then,
	// when persister runs again, you'd end up incorrectly with X + Z
	// when you wanted X + Y + Z.
	//
	var mlock sync.Mutex

	events := map[EventKind]int{}

	var eventCh chan EventKind
	var onPersistCh chan bool

	mo := &MergeOperatorStringAppend{Sep: ":"}

	lowerLevelPersister := newTestPersister()
	lowerLevelUpdater := func(higher Snapshot) (Snapshot, error) {
		if onPersistCh != nil {
			<-onPersistCh
		}
		p, err := lowerLevelPersister.Update(higher)
		if err != nil {
			return nil, err
		}
		lowerLevelPersister = p
		p.mutex.RLock()
		defer p.mutex.RUnlock()
		return p.cloneLOCKED(), nil
	}

	m, err := NewCollection(CollectionOptions{
		MergeOperator:    mo,
		LowerLevelInit:   lowerLevelPersister,
		LowerLevelUpdate: lowerLevelUpdater,
		OnEvent: func(e Event) {
			mlock.Lock()
			events[e.Kind]++
			eventCh2 := eventCh
			mlock.Unlock()

			if eventCh2 != nil {
				eventCh2 <- e.Kind
			}
		},
	})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}
	mc := m.(*collection)

	// Note that we don't Start()'ed the collection, so it doesn't
	// have the merger background goroutines runnning.  But we do
	// kickoff the background persister goroutine...
	go mc.runPersister()

	mergeVal := func(v string) {
		var b Batch
		b, err = m.NewBatch(0, 0)
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

	mergeVal("X")

	// Pretend to be the merger, moving stack dirty top into base, and
	// notify and wait for the persister.
	mc.m.Lock()
	mc.stackDirtyBase = mc.stackDirtyTop
	mc.stackDirtyTop = nil

	waitDirtyOutgoingCh := make(chan struct{})
	mc.waitDirtyOutgoingCh = waitDirtyOutgoingCh

	mc.stackDirtyBaseCond.Broadcast()
	mc.m.Unlock()

	<-waitDirtyOutgoingCh

	// At this point...
	// - stackDirtyTop  : empty
	// - stackDirtyMid  : empty
	// - stackDirtyBase : empty
	// - lowerLevel     : X

	mc.m.Lock()
	if mc.stackDirtyTop != nil || mc.stackDirtyMid != nil || mc.stackDirtyBase != nil {
		t.Errorf("expected X state")
	}
	if mc.lowerLevelSnapshot == nil {
		t.Errorf("unexpected llss X state")
	}
	v, err := mc.lowerLevelSnapshot.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X" {
		t.Errorf("expected :X, got: %s", v)
	}
	mc.m.Unlock()

	// --------------------------------------------

	mergeVal("Y")

	// Pretend to be the merger, moving stack dirty top into base,
	// but don't notify the persister.
	stackDirtyMid, _, _, _, _ :=
		mc.snapshot(snapshotSkipClean|snapshotSkipDirtyBase, nil, false)

	mc.m.Lock()
	mc.stackDirtyBase = stackDirtyMid
	mc.stackDirtyTop = nil
	mc.m.Unlock()

	// At this point...
	// - stackDirtyTop  : empty
	// - stackDirtyMid  : empty
	// - stackDirtyBase : Y (and points to lowerLevel X)
	// - lowerLevel     : X

	mc.m.Lock()
	if mc.stackDirtyTop != nil || mc.stackDirtyMid != nil || mc.stackDirtyBase == nil {
		t.Errorf("expected X/Y state")
	}
	if mc.lowerLevelSnapshot == nil {
		t.Errorf("unexpected llss X/Y state")
	}
	v, err = mc.lowerLevelSnapshot.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X" {
		t.Errorf("expected :X, got: %s", v)
	}
	mc.m.Unlock()

	// --------------------------------------------

	mergeVal("Z")

	// Pretend to be the merger, moving stack dirty top into mid,
	// but don't notify the persister.
	stackDirtyMid, _, _, _, _ =
		mc.snapshot(snapshotSkipClean|snapshotSkipDirtyBase, nil, false)

	mc.m.Lock()
	mc.stackDirtyMid = stackDirtyMid
	mc.stackDirtyTop = nil
	mc.m.Unlock()

	// At this point...
	// - stackDirtyTop  : empty
	// - stackDirtyMid  : Z (and points to lowerLevel X)
	// - stackDirtyBase : Y (and points to lowerLevel X)
	// - lowerLevel     : X

	mc.m.Lock()
	if mc.stackDirtyTop != nil || mc.stackDirtyMid == nil || mc.stackDirtyBase == nil {
		t.Errorf("expected X/Y/Z state")
	}
	if mc.lowerLevelSnapshot == nil {
		t.Errorf("unexpected llss X/Y/Z state")
	}
	v, err = mc.lowerLevelSnapshot.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X" {
		t.Errorf("expected :X, got: %s", v)
	}
	if len(mc.stackDirtyMid.a) != 1 {
		t.Errorf("expected stackDirtyMid len of 1")
	}
	if mc.stackDirtyMid.lowerLevelSnapshot == nil {
		t.Errorf("expected stackDirtyMid.lowerLevelSnapshot")
	}
	v, err = mc.stackDirtyMid.lowerLevelSnapshot.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X" {
		t.Errorf("expected :X, got: %s", v)
	}
	if mc.stackDirtyBase.lowerLevelSnapshot == nil {
		t.Errorf("expected stackDirtyBase.lowerLevelSnapshot")
	}
	v, err = mc.stackDirtyBase.lowerLevelSnapshot.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X" {
		t.Errorf("expected :X, got: %s", v)
	}
	if mc.stackDirtyBase.lowerLevelSnapshot != mc.stackDirtyMid.lowerLevelSnapshot {
		t.Errorf("expected same snapshots")
	}
	if mc.stackDirtyBase.lowerLevelSnapshot != mc.lowerLevelSnapshot {
		t.Errorf("expected same snapshots")
	}
	mc.m.Unlock()

	// --------------------------------------------

	checkVal := func(msg, expected string) {
		var ss Snapshot
		ss, err = m.Snapshot()
		if err != nil {
			t.Errorf("%s - expected ss ok", msg)
		}
		var getv []byte
		getv, err = ss.Get([]byte("k"), ReadOptions{})
		if err != nil || string(getv) != expected {
			t.Errorf("%s - expected Get %s, got: %s, err: %v", msg, expected, getv, err)
		}
		var iter Iterator
		iter, err = ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil || iter == nil {
			t.Errorf("%s - expected iter", msg)
		}
		var k []byte
		k, v, err = iter.Current()
		if err != nil {
			t.Errorf("%s - expected iter current no err", msg)
		}
		if string(k) != "k" {
			t.Errorf("%s - expected iter current key k", msg)
		}
		if string(v) != expected {
			t.Errorf("%s - expected iter current val expected: %v, got: %s", msg, expected, v)
		}
		if iter.Next() != ErrIteratorDone {
			t.Errorf("%s - expected only 1 value in iterator", msg)
		}
		ss.Close()
	}

	checkVal("before", ":X:Y:Z")

	// -------------------------------------------

	// Register a fake log func to hold up the merger.
	logCh := make(chan string)
	logBlockCh := make(chan string)
	mc.options.Debug = 1
	mc.options.Log = func(format string, a ...interface{}) {
		if logCh != nil {
			logCh <- format
		}
		if logBlockCh != nil {
			<-logBlockCh
		}
	}

	go mc.runMerger() // Finally start the real merger goroutine.

	notifyDoneCh := make(chan error)
	go func() {
		notifyDoneCh <- mc.NotifyMerger("wake up merger", true)
	}()

	fmtStr := <-logCh
	if !strings.HasPrefix(fmtStr, "collection: mergerMain,") {
		t.Errorf("expected a fmt str, got: %s", fmtStr)
	}

	// At this point the merger is now blocked in a Log() callback.

	// Next, kick the persister goroutine to force it to run concurrently once.
	mc.m.Lock()
	waitDirtyOutgoingCh = make(chan struct{})
	mc.waitDirtyOutgoingCh = waitDirtyOutgoingCh
	mc.stackDirtyBaseCond.Broadcast()
	mc.m.Unlock()

	<-waitDirtyOutgoingCh

	// At this point...
	// - stackDirtyTop  : empty
	// - stackDirtyMid  : Z (and points to lowerLevel X)
	// - stackDirtyBase : empty
	// - lowerLevel     : X+Y

	mc.m.Lock()
	if mc.stackDirtyTop != nil || mc.stackDirtyMid == nil || mc.stackDirtyBase != nil {
		t.Errorf("expected X+Y/Z middle state")
	}
	if mc.lowerLevelSnapshot == nil {
		t.Errorf("unexpected llss X+Y/Z middle state")
	}
	v, err = mc.lowerLevelSnapshot.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X:Y" {
		t.Errorf("expected :X:Y, got: %s", v)
	}
	if mc.stackDirtyMid.lowerLevelSnapshot == nil {
		t.Errorf("expected stackDirtyMid.lowerLevelSnapshot")
	}
	v, err = mc.stackDirtyMid.lowerLevelSnapshot.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X" {
		t.Errorf("expected :X, got: %s", v)
	}
	if mc.lowerLevelSnapshot == mc.stackDirtyMid.lowerLevelSnapshot {
		t.Errorf("expected different snapshots")
	}
	v, err = mc.stackDirtyMid.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X:Z" {
		t.Errorf("expected :X:Z, got: %s", v)
	}
	if len(mc.stackDirtyMid.a) != 1 {
		t.Errorf("expected stackDirtyMid to have len 1")
	}
	if mc.options.LowerLevelUpdate == nil {
		t.Errorf("expected options.LowerLevelUpdate")
	}
	mc.m.Unlock()

	checkVal("mid", ":X:Y:Z")

	// -------------------------------------------

	// Next let the merger proceed.

	mc.options.Debug = 0
	mc.options.Log = nil

	mlock.Lock()
	eventCh = make(chan EventKind)
	mlock.Unlock()

	logBlockCh <- "let the merger proceed"

	<-waitDirtyOutgoingCh

	kind := <-eventCh
	if kind != EventKindMergerProgress {
		t.Errorf("expected EventKindMergerProgress")
	}
	mlock.Lock()
	eventCh = nil
	mlock.Unlock()

	<-notifyDoneCh

	mc.m.Lock()
	if mc.waitDirtyOutgoingCh != nil {
		waitDirtyOutgoingCh = mc.waitDirtyOutgoingCh
		mc.m.Unlock()
		<-waitDirtyOutgoingCh
		mc.m.Lock()
	}
	waitDirtyOutgoingCh = make(chan struct{})
	mc.waitDirtyOutgoingCh = waitDirtyOutgoingCh
	mc.stackDirtyBaseCond.Broadcast()
	mc.m.Unlock()

	// At this point...
	// - stackDirtyTop  : empty
	// - stackDirtyMid  : empty
	// - stackDirtyBase : empty
	// - lowerLevel     : X+Z (incorrect)

	mc.m.Lock()
	if mc.stackDirtyTop != nil {
		t.Errorf("expected X/Y/Z last state top nil")
	}
	if mc.stackDirtyMid != nil && len(mc.stackDirtyMid.a) > 0 {
		t.Errorf("expected X/Y/Z last state mid nil, got: %#v", mc.stackDirtyMid)
	}
	if mc.stackDirtyBase != nil && len(mc.stackDirtyBase.a) > 0 {
		t.Errorf("expected X/Y/Z last state base nil")
	}
	if mc.lowerLevelSnapshot == nil {
		t.Errorf("unexpected llss X/Y/Z last state")
	}
	v, err = mc.lowerLevelSnapshot.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Errorf("expected get ok")
	}
	if string(v) != ":X:Y:Z" { // Before fix, this incorrectly returned :X:Z.
		t.Errorf("expected :X:Y:Z, got: %s", v)
	}
	mc.m.Unlock()

	// Before the fix, we used to incorrectly get :X:Z.
	checkVal("after", ":X:Y:Z")
}

func Test_JustLoad1Mitems(b *testing.T) {
	numItems := 100000
	batchSize := 100
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	so := DefaultStoreOptions
	so.CollectionOptions.MinMergePercentage = 0.0
	so.CompactionPercentage = 0.0
	so.CompactionSync = true
	spo := StorePersistOptions{CompactionConcern: CompactionAllow}

	store, coll, err := OpenStoreCollection(tmpDir, so, spo)
	if err != nil || store == nil || coll == nil {
		b.Fatalf("error opening store collection:%v", tmpDir)
	}

	for i := numItems; i >= 0; i = i - batchSize {
		// create new batch to set some keys
		ba, err := coll.NewBatch(0, 0)
		if err != nil {
			b.Fatalf("error creating new batch: %v", err)
			return
		}

		loadItems(ba, i, i-batchSize)
		err = coll.ExecuteBatch(ba, WriteOptions{})
		if err != nil {
			b.Fatalf("error executing batch: %v", err)
			return
		}

		// cleanup that batch
		err = ba.Close()
		if err != nil {
			b.Fatalf("error closing batch: %v", err)
			return
		}
		val, erro := coll.Get([]byte(fmt.Sprintf("%04d", numItems)), ReadOptions{})
		if erro != nil || val == nil {
			b.Fatalf("Unable to fetch the key written: %v", err)
		}
	}

	if store.Close() != nil {
		b.Fatalf("expected store close to work")
	}

	if coll.Close() != nil {
		b.Fatalf("Error closing child collection")
	}
}

func Test_LevelCompactDeletes(t *testing.T) {
	numItems := 100000
	batchSize := 100
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	so := DefaultStoreOptions
	so.CollectionOptions.MinMergePercentage = 0.0
	so.CompactionPercentage = 0.0
	so.CompactionSync = true
	spo := StorePersistOptions{CompactionConcern: CompactionAllow}

	store, coll, err := OpenStoreCollection(tmpDir, so, spo)
	if err != nil || store == nil || coll == nil {
		t.Fatalf("error opening store collection:%v", tmpDir)
	}

	for i := numItems; i > 0; i = i - batchSize {
		// create new batch to set some keys
		ba, erro := coll.NewBatch(0, 0)
		if erro != nil {
			t.Fatalf("error creating new batch: %v", err)
			return
		}
		for j := i - 1; j >= i-batchSize; j-- {
			k := fmt.Sprintf("%08d", j)
			ba.Set([]byte(k), []byte(k))
		}
		err = coll.ExecuteBatch(ba, WriteOptions{})
		if err != nil {
			t.Fatalf("error executing batch: %v", err)
			return
		}

		// cleanup that batch
		err = ba.Close()
		if err != nil {
			t.Fatalf("error closing batch: %v", err)
			return
		}
		val, er := coll.Get([]byte(fmt.Sprintf("%08d", i-1)), ReadOptions{})
		if er != nil || val == nil {
			t.Fatalf("Unable to fetch the key written: %v", er)
		}
	}

	waitForPersistence(coll)

	if store.Close() != nil {
		t.Fatalf("expected store close to work")
	}

	if coll.Close() != nil {
		t.Fatalf("Error closing child collection")
	}
	// Now reopen the store in level compaction mode and Only do deletes.
	so.CompactionPercentage = 100.0
	so.CompactionLevelMaxSegments = 2
	so.CompactionLevelMultiplier = 3
	store, coll, err = OpenStoreCollection(tmpDir, so, spo)
	if err != nil || store == nil || coll == nil {
		t.Fatalf("error opening store collection:%v", tmpDir)
	}

	// Now load data such that a new batch comprising of only deleted items
	// is appended to the end of the file.
	// On the last attempt to append, level compaction should kick in
	// and only partially compact those last segments comprising of deletes.
	level0segments := 3 // level compact on the third level0 segment.
	for i := 0; i < numItems && level0segments > 0; i = i + batchSize {
		ba, erro := coll.NewBatch(0, 0)
		if erro != nil {
			t.Fatalf("error creating new batch: %v", erro)
			return
		}
		key := fmt.Sprintf("%08d", i)
		erro = ba.Del([]byte(key))
		if erro != nil {
			t.Fatalf("unable to delete key")
		}
		erro = coll.ExecuteBatch(ba, WriteOptions{})
		if erro != nil {
			t.Fatalf("error executing batch: %v", erro)
			return
		}

		// cleanup that batch
		erro = ba.Close()
		if erro != nil {
			t.Fatalf("error closing batch: %v", erro)
			return
		}

		waitForPersistence(coll)
		level0segments--
	}
	val, erro := coll.Get([]byte(fmt.Sprintf("%08d", 0)), ReadOptions{})
	if erro == nil && val != nil {
		t.Fatalf("Should have deleted key 0: %v", err)
	}

	coll.Close()
	store.Close()
}

func Test_IdleCompactionThrottle(t *testing.T) {
	numItems := 1000
	batchSize := 100
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	so := DefaultStoreOptions
	so.CollectionOptions.MergerIdleRunTimeoutMS = 10
	so.CompactionSync = true
	spo := StorePersistOptions{CompactionConcern: CompactionAllow}

	store, coll, err := OpenStoreCollection(tmpDir, so, spo)
	if err != nil || store == nil || coll == nil {
		t.Fatalf("error opening store collection:%v", tmpDir)
	}

	for i := numItems; i > 0; i = i - batchSize {
		// Get the number of idle merger runs before insertion.
		collStats, _ := coll.Stats()
		idleRunsBefore := collStats.TotMergerIdleRuns

		// create new batch to set some keys
		ba, erro := coll.NewBatch(0, 0)
		if erro != nil {
			t.Fatalf("error creating new batch: %v", err)
			return
		}
		for j := i - 1; j >= i-batchSize; j-- {
			k := fmt.Sprintf("%08d", j)
			ba.Set([]byte(k), []byte(k))
		}
		err = coll.ExecuteBatch(ba, WriteOptions{})
		if err != nil {
			t.Fatalf("error executing batch: %v", err)
			return
		}

		// cleanup that batch
		err = ba.Close()
		if err != nil {
			t.Fatalf("error closing batch: %v", err)
			return
		}
		waitForPersistence(coll)

		// wait longer than 3X the idle compaction timeout to verify throttle.
		time.Sleep(40 * time.Millisecond)

		collStats, _ = coll.Stats()
		idleRunsAfter := collStats.TotMergerIdleRuns
		if idleRunsAfter <= idleRunsBefore {
			t.Errorf("Idle compactions not run %v", idleRunsAfter)
		}
	}

	waitForPersistence(coll)

	if store.Close() != nil {
		t.Fatalf("expected store close to work")
	}

	if coll.Close() != nil {
		t.Fatalf("Error closing child collection")
	}
}
