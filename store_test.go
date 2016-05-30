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
	"path"
	"strconv"
	"strings"
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
	s, err := ParseFNameSeq("data-00000fe.moss")
	if err != nil || s != int64(254) {
		t.Errorf("expected 254, got: %v", s)
	}

	s, err = ParseFNameSeq("data-00000.moss")
	if err != nil || s != int64(0) {
		t.Errorf("expected 0")
	}

	ss := FormatFName(0)
	if ss != "data-0000000000000000.moss" {
		t.Errorf("expected lots of 0's, got: %s", ss)
	}

	ss = FormatFName(256)
	if ss != "data-0000000000000100.moss" {
		t.Errorf("expected lots of 0's and 100, got: %s", ss)
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
	testStoreOps(t, StorePersistOptions{})
}

func TestStoreOpsCompactionForce(t *testing.T) {
	testStoreOps(t, StorePersistOptions{CompactionConcern: CompactionForce})
}

func testStoreOps(t *testing.T, spo StorePersistOptions) {
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
	eventWaiters := map[EventKind]chan bool{}

	m, err := NewCollection(CollectionOptions{
		MergeOperator:  &MergeOperatorStringAppend{Sep: ":"},
		LowerLevelInit: ssInit,
		LowerLevelUpdate: func(higher Snapshot) (Snapshot, error) {
			return store.Persist(higher, spo)
		},
		OnEvent: func(event Event) {
			mu.Lock()
			counts[event.Kind]++
			eventWaiter := eventWaiters[event.Kind]
			mu.Unlock()
			if eventWaiter != nil {
				eventWaiter <- true
			}
		},
	})
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	m.Start()

	persistWaiterCh := make(chan bool, 100)

	mu.Lock()
	eventWaiters[EventKindPersisterProgress] = persistWaiterCh
	mu.Unlock()

	testOps(t, m)

	err = m.(*collection).NotifyMerger("mergeAll", true)
	if err != nil {
		t.Errorf("mergeAll err")
	}

	err = m.(*collection).NotifyMerger("mergeAll", true)
	if err != nil {
		t.Errorf("mergeAll err")
	}

	<-persistWaiterCh

	mu.Lock()
	eventWaiters[EventKindPersisterProgress] = nil
	mu.Unlock()

	go func() {
		for range persistWaiterCh {
			/* eat any more events to keep persister unblocked */
		}
	}()

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

func TestStoreCompaction(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var mu sync.Mutex
	counts := map[EventKind]int{}

	co := CollectionOptions{
		MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
		OnEvent: func(event Event) {
			mu.Lock()
			counts[event.Kind]++
			mu.Unlock()
		},
	}

	store, err := OpenStore(tmpDir, StoreOptions{
		CollectionOptions: co,
	})
	if err != nil || store == nil {
		t.Errorf("expected open empty store to work")
	}

	ssInit, err := store.Snapshot()
	if err != nil || ssInit == nil {
		t.Errorf("expected ssInit")
	}

	persistCh := make(chan struct{})

	spo := StorePersistOptions{CompactionConcern: CompactionForce}

	co2 := co
	co2.LowerLevelInit = ssInit
	co2.LowerLevelUpdate = func(higher Snapshot) (Snapshot, error) {
		ss, err := store.Persist(higher, spo)
		persistCh <- struct{}{}
		return ss, err
	}

	m, err := NewCollection(co2)
	if err != nil || m == nil {
		t.Errorf("expected moss")
	}

	m.Start()

	mirror := map[string]string{}

	set1000 := true
	delTens := true
	updateOdds := true

	numBatches := 0
	for j := 0; j < 10; j++ {
		if set1000 {
			b, _ := m.NewBatch(0, 0)
			for i := 0; i < 1000; i++ {
				xs := fmt.Sprintf("%d", i)
				x := []byte(xs)
				b.Set(x, x)
				mirror[xs] = xs
			}
			err = m.ExecuteBatch(b, WriteOptions{})
			if err != nil {
				t.Errorf("expected exec batch to work")
			}
			b.Close()

			<-persistCh
			numBatches++
		}

		if delTens {
			b, _ := m.NewBatch(0, 0)
			for i := 0; i < 1000; i += 10 {
				xs := fmt.Sprintf("%d", i)
				x := []byte(xs)
				b.Del(x)
				delete(mirror, xs)
			}
			err = m.ExecuteBatch(b, WriteOptions{})
			if err != nil {
				t.Errorf("expected exec batch to work")
			}
			b.Close()

			<-persistCh
			numBatches++
		}

		if updateOdds {
			b, _ := m.NewBatch(0, 0)
			for i := 1; i < 1000; i += 2 {
				xs := fmt.Sprintf("%d", i)
				x := []byte(xs)
				vs := fmt.Sprintf("odd-%d", i)
				b.Set(x, []byte(vs))
				mirror[xs] = vs
			}
			err = m.ExecuteBatch(b, WriteOptions{})
			if err != nil {
				t.Errorf("expected exec batch to work")
			}
			b.Close()

			<-persistCh
			numBatches++
		}
	}

	m.Close()

	store.Close()

	// --------------------

	fileInfos, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Errorf("expected read dir to work")
	}
	if len(fileInfos) != 1 {
		t.Errorf("expected only 1 file")
	}

	seq, err := ParseFNameSeq(fileInfos[0].Name())
	if err != nil {
		t.Errorf("expected parse seq to work")
	}
	if seq <= 0 {
		t.Errorf("expected larger seq")
	}
	if seq != int64(numBatches) {
		t.Errorf("expected seq: %s to equal numBatches: %s", seq, numBatches)
	}

	// --------------------

	store2, err := OpenStore(tmpDir, StoreOptions{
		CollectionOptions: co,
	})
	if err != nil || store2 == nil {
		t.Errorf("expected reopen store to work")
	}

	// --------------------

	store2.m.Lock()

	if store2.nextFNameSeq != seq+1 {
		t.Errorf("expected nextFNameseq to be seq+1")
	}
	if store2.footer == nil {
		t.Errorf("expected nextFNameseq to be seq+1")
	}
	if store2.footer.fref.refs != 1 {
		t.Errorf("expected store2.footer.fref.refs == 1")
	}
	if len(store2.footer.SegmentLocs) != 1 {
		t.Errorf("expected store2.footer.SegmentLocs == 1, got: %d",
			len(store2.footer.SegmentLocs))
	}
	if len(store2.footer.ss.a) != 1 {
		t.Errorf("expected store2.footer.ss.a ==1, got: %d",
			len(store2.footer.ss.a))
	}
	if store2.footer.ss.lowerLevelSnapshot != nil {
		t.Errorf("expected segStack2.lowerLevelSnapshot nil")
	}

	seg, ok := store2.footer.ss.a[0].(*segment)
	if !ok {
		t.Errorf("expected segment")
	}
	if seg.kvs == nil || len(seg.kvs) <= 0 {
		t.Errorf("expected seg.kvs")
	}
	if seg.buf == nil || len(seg.buf) <= 0 {
		t.Errorf("expected seg.buf")
	}
	for pos := 0; pos < seg.Len(); pos++ {
		x := pos * 2
		if x < 0 || x >= len(seg.kvs) {
			t.Errorf("pos to x error")
		}

		opklvl := seg.kvs[x]

		operation, keyLen, valLen := decodeOpKeyLenValLen(opklvl)
		if operation == 0 {
			t.Errorf("should have some nonzero op")
		}

		kstart := int(seg.kvs[x+1])
		vstart := kstart + keyLen

		if kstart+keyLen > len(seg.buf) {
			t.Errorf("key larger than buf, pos: %d, kstart: %d, keyLen: %d, len(buf): %d, op: %x",
				pos, kstart, keyLen, len(seg.buf), operation)
		}
		if vstart+valLen > len(seg.buf) {
			t.Errorf("val larger than buf, pos: %d, vstart: %d, valLen: %d, len(buf): %d, op: %x",
				pos, vstart, valLen, len(seg.buf), operation)
		}
	}

	store2.m.Unlock()

	// --------------------

	ss2, err := store2.Snapshot()
	if err != nil || ss2 == nil {
		t.Errorf("expected snapshot 2, no err")
	}

	iter2, err := ss2.StartIterator(nil, nil, IteratorOptions{})
	if err != nil || iter2 == nil {
		t.Errorf("expected ss2 iter to start")
	}

	numKVs := 0
	for {
		k, v, err := iter2.Current()
		if err == ErrIteratorDone {
			break
		}

		numKVs++

		ks := string(k)
		vs := string(v)

		i, err := strconv.Atoi(ks)
		if err != nil {
			t.Errorf("expected all int keys, got: %s", ks)
		}
		if i <= 0 || i >= 1000 {
			t.Errorf("expected i within range")
		}
		if delTens && i%10 == 0 {
			t.Errorf("expected no multiples of 10")
		}
		if updateOdds && i%2 == 1 {
			if !strings.HasPrefix(vs, "odd-") {
				t.Errorf("expected odds to have odd prefix")
			}
		}

		if mirror[ks] != vs {
			t.Errorf("mirror[ks] (%s) != vs (%s)", mirror[ks], vs)
		}

		err = iter2.Next()
		if err == ErrIteratorDone {
			break
		}
	}

	if numKVs != len(mirror) {
		t.Errorf("numKVs: %d, not matching len(mirror): %d", numKVs, len(mirror))
	}
}
