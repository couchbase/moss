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
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/edsrzf/mmap-go"
)

func TestPageAlign(t *testing.T) {
	if pageAlign(0) != 0 {
		t.Errorf("expect 0")
	}
	if pageAlign(1) != int64(STORE_PAGE_SIZE) {
		t.Errorf("expect sps")
	}
	if pageAlign(int64(STORE_PAGE_SIZE-1)) != int64(STORE_PAGE_SIZE) {
		t.Errorf("expect sps")
	}
	if pageAlign(int64(STORE_PAGE_SIZE)) != int64(STORE_PAGE_SIZE) {
		t.Errorf("expect sps")
	}
	if pageAlign(int64(STORE_PAGE_SIZE+1)) != int64(2*STORE_PAGE_SIZE) {
		t.Errorf("expect sps")
	}
}

func TestParseFNameSeq(t *testing.T) {
	s, err := ParseFNameSeq("data-00000123.moss")
	if err != nil || s != int64(123) {
		t.Errorf("expected 123")
	}

	s, err = ParseFNameSeq("data-00000.moss")
	if err != nil || s != int64(0) {
		t.Errorf("expected 0")
	}

	if FormatFName(0) != "data-0000000000000000.moss" {
		t.Errorf("expected lots of 0's")
	}

	if FormatFName(123) != "data-0000000000000123.moss" {
		t.Errorf("expected lots of 0's and 123")
	}
}

func TestOpenEmptyStore(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	s, err := OpenStore(tmpDir, StoreOptions{})
	if err != nil {
		t.Errorf("expected open empty store to work")
	}

	if s.Dir() != tmpDir {
		t.Errorf("expected same dir")
	}

	s.Options() // It should not panic.

	ss, err := s.Snapshot()
	if err != nil || ss == nil {
		t.Errorf("expected snapshot, no err")
	}
	ss2, err := s.Persist(ss, StorePersistOptions{})
	if err == nil || ss2 != nil {
		t.Errorf("expected persist of non-segmentStack to err")
	}

	v, err := ss.Get([]byte("a"), ReadOptions{})
	if err != nil || v != nil {
		t.Errorf("expected no a")
	}

	iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
	if err != nil || iter == nil {
		t.Errorf("expected ss iter to start")
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

	if ss.Close() != nil {
		t.Errorf("expected ss close to work")
	}
	if s.Close() != nil {
		t.Errorf("expected s close to work")
	}

	fileInfos, _ := ioutil.ReadDir(tmpDir)
	if len(fileInfos) != 0 {
		t.Errorf("expected no files")
	}
}

func TestMMap(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var testData = []byte("0123456789ABCDEF")

	f, _ := os.OpenFile(path.Join(tmpDir, "test.mmap"),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	f.Write(testData)

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	if mm[0] != '0' {
		t.Errorf("expected 0, got: %v", mm[0])
	}
	if len(mm) != len(testData) {
		t.Errorf("expected same lengths")
	}

	f.Write(testData) // Write more.
	if len(mm) != len(testData) {
		t.Errorf("expected same lengths")
	}

	mm2, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
	}
	if mm2[len(testData)] != '0' {
		t.Errorf("expected 0, got: %v", mm[0])
	}
	if mm2[len(testData)+1] != '1' {
		t.Errorf("expected 0, got: %v", mm[0])
	}
	if len(mm2) != 2*len(testData) {
		t.Errorf("expected double lengths")
	}

	mm2[1] = 'X'
	if mm[1] != 'X' {
		t.Errorf("expected X the same")
	}

	mm2.Unmap()
	mm.Unmap()

	f.Close()
}

func TestSimpleStore(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	store, err := OpenStore(tmpDir, StoreOptions{})
	if err != nil || store == nil {
		t.Errorf("expected open empty store to work")
	}

	coll, _ := NewCollection(CollectionOptions{})
	coll.Start()

	b, _ := coll.NewBatch(0, 0)
	b.Set([]byte("a"), []byte("A"))
	coll.ExecuteBatch(b, WriteOptions{})

	ss, _ := coll.Snapshot()

	// ------------------------------------------------------

	llss, err := store.Persist(ss, StorePersistOptions{})
	if err != nil || llss == nil {
		t.Errorf("expected persist to work")
	}

	if store.nextFNameSeq != 2 {
		t.Errorf("expected store nextFNameSeq to be 2")
	}

	v, err := llss.Get([]byte("a"), ReadOptions{})
	if err != nil || string(v) != "A" {
		t.Errorf("expected llss get to work, err: %v, v: %v", err, v)
	}

	iter, err := llss.StartIterator(nil, nil, IteratorOptions{})
	if err != nil || iter == nil {
		t.Errorf("expected llss iter to start")
	}

	iterk, iterv, err := iter.Current()
	if err != nil || string(iterk) != "a" || string(iterv) != "A" {
		t.Errorf("expected iterk/v")
	}

	if iter.Next() != ErrIteratorDone {
		t.Errorf("expected 1 iter")
	}

	if iter.Close() != nil {
		t.Errorf("expected iter close to work")
	}

	if llss.Close() != nil {
		t.Errorf("expected llss close to work")
	}

	if store.Close() != nil {
		t.Errorf("expected store close to work")
	}

	// ------------------------------------------------------

	store2, err := OpenStore(tmpDir, StoreOptions{})
	if err != nil || store2 == nil {
		t.Errorf("expected open store2 to work")
	}

	if store2.nextFNameSeq != 2 {
		t.Errorf("expected store2 nextFNameSeq to be 2")
	}

	llss2, err := store2.Snapshot()
	if err != nil || llss2 == nil {
		t.Errorf("expected llss2 to work")
	}

	v, err = llss2.Get([]byte("a"), ReadOptions{})
	if err != nil || string(v) != "A" {
		t.Errorf("expected llss get to work, err: %v, v: %v", err, v)
	}

	iter2, err := llss2.StartIterator(nil, nil, IteratorOptions{})
	if err != nil || iter2 == nil {
		t.Errorf("expected llss iter to start")
	}

	iter2k, iter2v, err := iter2.Current()
	if err != nil || string(iter2k) != "a" || string(iter2v) != "A" {
		t.Errorf("expected iter2k/v")
	}

	if iter2.Next() != ErrIteratorDone {
		t.Errorf("expected 1 iter2")
	}

	if iter2.Close() != nil {
		t.Errorf("expected iter2 close to work")
	}

	if llss2.Close() != nil {
		t.Errorf("expected llss2 close to work")
	}

	if store2.Close() != nil {
		t.Errorf("expected store2 close to work")
	}
}

func TestStoreOps(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	store, err := OpenStore(tmpDir, StoreOptions{})
	if err != nil || store == nil {
		t.Errorf("expected open empty store to work")
	}

	ssInit, err := store.Snapshot()
	if err != nil || ssInit == nil {
		t.Errorf("expected ssInit")
	}

	var mu sync.Mutex
	counts := map[EventKind]int{}

	m, err := NewCollection(CollectionOptions{
		MergeOperator:  &testMergeOperatorAppend{},
		LowerLevelInit: ssInit,
		LowerLevelUpdate: func(higher Snapshot) (Snapshot, error) {
			return store.Persist(higher, StorePersistOptions{})
		},
		OnEvent: func(event Event) {
			mu.Lock()
			counts[event.Kind]++
			mu.Unlock()
		},
	})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	m.Start()

	testOps(t, m)

	err = m.(*collection).NotifyMerger("mergeAll", true)
	if err != nil {
		t.Errorf("mergeAll err")
	}

	err = m.(*collection).NotifyMerger("mergeAll", true)
	if err != nil {
		t.Errorf("mergeAll err")
	}

	m.Close()

	mu.Lock()
	if counts[EventKindPersisterProgress] <= 0 {
		t.Errorf("expected persistence progress")
	}
	mu.Unlock()

	stats, err := m.Stats()
	if err != nil || stats.TotPersisterLowerLevelUpdateEnd <= 0 {
		t.Errorf("expected some persistence")
	}
}
