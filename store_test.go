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
	"time"

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

func TestPageOffset(t *testing.T) {
	p := int64(STORE_PAGE_SIZE)

	tests := []struct {
		pos, exp int64
	}{
		{0, 0},
		{1, 0},
		{p - 1, 0},
		{p, p},
		{p + 1, p},
		{2*p - 1, p},
		{2 * p, 2 * p},
		{2*p + 1, 2 * p},
	}

	for testi, test := range tests {
		got := pageOffset(test.pos, p)
		if test.exp != got {
			t.Errorf("expected pageOffset testi: %d, test: %+v, got: %d",
				testi, test, got)
		}
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

	sstats, err := s.Stats()
	if err != nil {
		t.Errorf("expected no stats err")
	}
	if sstats == nil {
		t.Errorf("expected non-nil stats")
	}
	if sstats["num_bytes_used_disk"].(uint64) != 0 {
		t.Errorf("expected 0 stats to start")
	}
	if sstats["total_persists"].(uint64) != 0 {
		t.Errorf("expected 0 stats to start")
	}
	if sstats["total_compactions"].(uint64) != 0 {
		t.Errorf("expected 0 stats to start")
	}

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
	if s.refs != 0 {
		t.Errorf("expected 0 refs")
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
	testSimpleStore(t, false)
}

func TestSimpleStoreSync(t *testing.T) {
	testSimpleStore(t, true)
}

func testSimpleStore(t *testing.T, sync bool) {
	testSimpleStoreEx(t, "basic", nil,
		StoreOptions{},
		StorePersistOptions{NoSync: !sync},
		CollectionOptions{}, 2)
}

func TestSimpleStoreCleanupBadFiles(t *testing.T) {
	testSimpleStoreCleanupBadFiles(t, "cleanupBadFiles", true)
}

func TestSimpleStoreKeepBadFiles(t *testing.T) {
	testSimpleStoreCleanupBadFiles(t, "cleanupBadFiles(false)", false)
}

func testSimpleStoreCleanupBadFiles(t *testing.T,
	label string,
	cleanupBadFiles bool) {
	testSimpleStoreEx(t, label, map[string]func(string){
		"beforeReopen": func(tmpDir string) {
			// Create some junk files that should be removed automatically.
			s := &Store{
				dir:          tmpDir,
				nextFNameSeq: 100,
				options: &StoreOptions{
					OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
						return os.OpenFile(name, flag, perm)
					},
				},
			}

			fref, file, err := s.startFileLOCKED()
			if err != nil || file == nil {
				t.Errorf("expected startFileLocked to work, err: %v", err)
			}
			fref.DecRef()

			fref, file, err = s.startFileLOCKED()
			if err != nil || file == nil {
				t.Errorf("expected startFileLocked to work, err: %v", err)
			}
			fref.DecRef()
		},
		"done": func(tmpDir string) {
			fileInfos, err := ioutil.ReadDir(tmpDir)
			if err != nil {
				t.Errorf("expected readdir to work, err: %v", err)
			}

			if cleanupBadFiles {
				// All the junk files should have been removed automatically already.
				if len(fileInfos) != 1 {
					t.Errorf("expected only 1 file, saw: %v", fileInfos)
				}
			} else {
				if len(fileInfos) != 3 {
					t.Errorf("expected 3 files, saw: %v", fileInfos)
				}
			}
		},
	},
		StoreOptions{KeepFiles: !cleanupBadFiles},
		StorePersistOptions{},
		CollectionOptions{},
		102)
}

func testSimpleStoreEx(t *testing.T,
	label string,
	callbacks map[string]func(string),
	storeOptions StoreOptions,
	storePersistOptions StorePersistOptions,
	collectionOptions CollectionOptions,
	expectedNextFNameSeq int64) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	store, err := OpenStore(tmpDir, storeOptions)
	if err != nil || store == nil {
		t.Errorf("expected open empty store to work")
	}

	coll, _ := NewCollection(collectionOptions)
	coll.Start()

	b, _ := coll.NewBatch(0, 0)
	b.Set([]byte("a"), []byte("A"))
	b2, _ := b.NewChildCollectionBatch("child", BatchOptions{0, 0})
	b2.Set([]byte("a"), []byte("A2"))
	coll.ExecuteBatch(b, WriteOptions{})

	ss, _ := coll.Snapshot()

	// ------------------------------------------------------

	llss, err := store.Persist(ss, storePersistOptions)
	if err != nil || llss == nil {
		t.Errorf("expected persist to work")
	}

	if store.nextFNameSeq != 2 {
		t.Errorf("expected store nextFNameSeq to be 2")
	}

	cs, err := llss.ChildCollectionSnapshot("child")
	if err != nil {
		t.Errorf("expected store to have child collection")
	}
	v, err := cs.Get([]byte("a"), ReadOptions{})
	if err != nil || string(v) != "A2" {
		t.Errorf("expected llss child get to work, err: %v, v: %v", err, v)
	}

	v, err = llss.Get([]byte("a"), ReadOptions{})
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

	sstats, err := store.Stats()
	if err != nil {
		t.Errorf("expected no stats err")
	}
	if sstats == nil {
		t.Errorf("expected non-nil stats")
	}
	if sstats["num_bytes_used_disk"].(uint64) <= 0 {
		t.Errorf("expected >0 num_bytes_used_disk")
	}
	if sstats["total_persists"].(uint64) <= 0 {
		t.Errorf("expected >0 total_persists")
	}

	if store.Close() != nil {
		t.Errorf("expected store close to work")
	}

	if store.refs != 0 {
		t.Errorf("expected 0 refs")
	}

	// ------------------------------------------------------

	if callbacks != nil {
		cb := callbacks["beforeReopen"]
		if cb != nil {
			cb(tmpDir)
		}
	}

	// ------------------------------------------------------

	store2, err := OpenStore(tmpDir, storeOptions)
	if err != nil || store2 == nil {
		t.Errorf("expected open store2 to work, err: %v", err)
	}

	if store2.nextFNameSeq != expectedNextFNameSeq {
		t.Errorf("expected store nextFNameSeq to be %d, got: %d, label: %s",
			expectedNextFNameSeq, store2.nextFNameSeq, label)
	}

	llss2, err := store2.Snapshot()
	if err != nil || llss2 == nil {
		t.Errorf("expected llss2 to work")
	}

	cllss2, err := llss2.ChildCollectionSnapshot("child")
	if err != nil || cllss2 == nil {
		t.Errorf("expected child snapshot to be durable")
	}

	v, err = cllss2.Get([]byte("a"), ReadOptions{})
	if err != nil || string(v) != "A2" {
		t.Errorf("expected llss get to work, err: %v, v: %v", err, v)
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

	if store2.refs != 0 {
		t.Errorf("expected 0 refs")
	}

	// ------------------------------------------------------

	if callbacks != nil {
		cb := callbacks["done"]
		if cb != nil {
			cb(tmpDir)
		}
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

	mo := &MergeOperatorStringAppend{Sep: ":"}

	var mu sync.Mutex
	counts := map[EventKind]int{}
	durations := map[EventKind]time.Duration{}
	eventWaiters := map[EventKind]chan bool{}

	co := CollectionOptions{
		MergeOperator: mo,
		OnEvent: func(event Event) {
			mu.Lock()
			counts[event.Kind]++
			durations[event.Kind] += event.Duration
			eventWaiter := eventWaiters[event.Kind]
			mu.Unlock()
			if eventWaiter != nil {
				eventWaiter <- true
			}
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

	co.LowerLevelInit = ssInit
	co.LowerLevelUpdate = func(higher Snapshot) (Snapshot, error) {
		return store.Persist(higher, spo)
	}

	m, err := NewCollection(co)
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
	if durations[EventKindPersisterProgress] <= 0 {
		t.Errorf("expected persistence to take some time")
	}
	mu.Unlock()

	stats, err := m.Stats()
	if err != nil || stats.TotPersisterLowerLevelUpdateEnd <= 0 {
		t.Errorf("expected some persistence")
	}
}

func TestStoreCompaction(t *testing.T) {
	testStoreCompaction(t, CollectionOptions{},
		StorePersistOptions{CompactionConcern: CompactionForce})
}

func TestStoreCompactionDeferredSort(t *testing.T) {
	testStoreCompaction(t, CollectionOptions{DeferredSort: true},
		StorePersistOptions{CompactionConcern: CompactionForce})
}

func testStoreCompaction(t *testing.T, co CollectionOptions,
	spo StorePersistOptions) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	co.MergeOperator = &MergeOperatorStringAppend{Sep: ":"}

	var mu sync.Mutex
	counts := map[EventKind]int{}

	co.OnEvent = func(event Event) {
		mu.Lock()
		counts[event.Kind]++
		mu.Unlock()
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

	if store.refs != 0 {
		t.Errorf("expected 0 refs")
	}

	// --------------------

	fileInfos, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Errorf("expected read dir to work")
	}
	if len(fileInfos) != 1 {
		fileNames := []string{}
		for _, fileInfo := range fileInfos {
			fileNames = append(fileNames,
				fmt.Sprintf("%s (%d)", fileInfo.Name(), fileInfo.Size()))
		}
		t.Errorf("expected only 1 file, got: %d, fileNames; %v",
			len(fileInfos), fileNames)
	}

	seq, err := ParseFNameSeq(fileInfos[0].Name())
	if err != nil {
		t.Errorf("expected parse seq to work")
	}
	if seq <= 0 {
		t.Errorf("expected larger seq")
	}
	if seq != int64(numBatches) {
		t.Errorf("expected seq: %d to equal numBatches: %d", seq, numBatches)
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

	if store2.refs != 1 {
		t.Errorf("expected 1 refs")
	}
	if store2.nextFNameSeq != seq+1 {
		t.Errorf("expected nextFNameseq to be seq+1")
	}
	if store2.footer == nil {
		t.Errorf("expected nextFNameseq to be seq+1")
	}
	if store2.footer.SegmentLocs[0].mref.refs != 1 {
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

	if seg, ok := store2.footer.ss.a[0].(SegmentValidater); ok {
		err = seg.Valid()
		if err != nil {
			t.Fatalf("expected valid segment, got: %v", err)
		}
	}

	store2.m.Unlock()

	// --------------------

	store2.AddRef()

	store2.m.Lock()
	if store2.refs != 2 {
		t.Errorf("expected 2 refs")
	}
	store2.m.Unlock()

	err = store2.Close()
	if err != nil {
		t.Errorf("expected close to work")
	}

	store2.m.Lock()
	if store2.refs != 1 {
		t.Errorf("expected 1 refs after close")
	}
	if store2.footer == nil {
		t.Errorf("expected footer after non-final close")
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

func TestOpenStoreCollection(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var mu sync.Mutex
	counts := map[EventKind]int{}
	eventWaiters := map[EventKind]chan bool{}

	co := CollectionOptions{
		MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
		OnEvent: func(event Event) {
			mu.Lock()
			counts[event.Kind]++
			eventWaiter := eventWaiters[event.Kind]
			mu.Unlock()
			if eventWaiter != nil {
				eventWaiter <- true
			}
		},
	}

	store, m, err := OpenStoreCollection(tmpDir, StoreOptions{
		CollectionOptions: co,
	}, StorePersistOptions{})
	if err != nil || m == nil || store == nil {
		t.Errorf("expected open empty store collection to work")
	}

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

			numBatches++
		}
	}

	waitUntilClean := func() error {
		for {
			stats, err := m.Stats()
			if err != nil {
				return err
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

	m.Close()

	store.Close()

	if store.refs != 0 {
		t.Errorf("expected store refs to be 0, got: %d", store.refs)
	}

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
		t.Errorf("expected nonzero seq")
	}

	// --------------------

	store2, m2, err := OpenStoreCollection(tmpDir, StoreOptions{
		CollectionOptions: co,
	}, StorePersistOptions{})
	if err != nil || m2 == nil || store2 == nil {
		t.Errorf("expected reopen store to work")
	}

	ss2, err := m2.Snapshot()
	if err != nil {
		t.Errorf("expected ss2 to work")
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

	iter2.Close()

	if numKVs != len(mirror) {
		t.Errorf("numKVs: %d, not matching len(mirror): %d", numKVs, len(mirror))
	}

	ss2.Close()

	m2.Close()

	store2.Close()

	if store2.refs != 0 {
		t.Errorf("expected store2 refs to be 0, got: %d", store2.refs)
	}
}

func TestStoreCompactionDeletions(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var mu sync.Mutex
	counts := map[EventKind]int{}

	persistedCh := make(chan struct{})
	doneCh := make(chan struct{}, 1)

	storeOptions := StoreOptions{
		CollectionOptions: CollectionOptions{
			MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
			OnEvent: func(event Event) {
				mu.Lock()
				counts[event.Kind]++
				mu.Unlock()

				if event.Kind == EventKindPersisterProgress {
					<-persistedCh
				}

				if event.Kind == EventKindClose {
					doneCh <- struct{}{}
				}
			},
		},
	}

	spo := StorePersistOptions{CompactionConcern: CompactionForce}

	store, m, err := OpenStoreCollection(tmpDir, storeOptions, spo)
	if err != nil {
		t.Errorf("OpenStoreCollection err: %v", err)
	}

	for j := 0; j < 10; j++ {
		b, _ := m.NewBatch(0, 0)
		for i := 0; i < 100; i++ {
			xs := fmt.Sprintf("%d", i)
			x := []byte(xs)
			if j%2 == 0 {
				b.Set(x, x)
			} else {
				b.Del(x)
			}
		}
		err = m.ExecuteBatch(b, WriteOptions{})
		if err != nil {
			t.Errorf("expected exec batch to work")
		}
		b.Close()

		persistedCh <- struct{}{}
	}

	close(persistedCh)

	// Helper function to test that a collection is logically empty
	// from Get()'s and iteration.
	testEmpty := func(m Collection, includeDeletions bool) {
		ss, err := m.Snapshot()
		if ss == nil || err != nil {
			t.Errorf("expected no err")
		}

		for i := 0; i < 100; i++ {
			xs := fmt.Sprintf("%d", i)
			v, err := ss.Get([]byte(xs), ReadOptions{})
			if err != nil || v != nil {
				t.Errorf("expected no keys")
			}
		}

		itr, err := ss.StartIterator(nil, nil, IteratorOptions{
			IncludeDeletions: includeDeletions,
		})
		if err != nil || itr == nil {
			t.Errorf("expected no err iterator")
		}
		entryEx, k, v, err := itr.CurrentEx()
		if err != ErrIteratorDone {
			t.Errorf("expected empty iterator")
		}
		if entryEx.Operation != 0 || k != nil || v != nil {
			t.Errorf("expected empty iterator currentEx")
		}

		ss.Close()
	}

	testEmpty(m, false)

	go m.Close()
	<-doneCh

	store.Close()

	store2, m2, err := OpenStoreCollection(tmpDir, storeOptions, spo)
	if err != nil || store2 == nil || m2 == nil {
		t.Errorf("expected reopen store to work")
	}

	testEmpty(m2, true)
	testEmpty(m2, false)

	go m2.Close()
	<-doneCh

	// Check from stats that the store is physically empty.
	snapshot2, _ := store2.Snapshot()
	stats2 := snapshot2.(*Footer).ss.Stats()
	if stats2.CurOps != 0 {
		t.Errorf("expected no ops, got: %#v", stats2)
	}
	snapshot2.Close()

	store2.Close()
}

func TestStoreNilValue(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var mu sync.Mutex
	counts := map[EventKind]int{}

	persistedCh := make(chan struct{})
	doneCh := make(chan struct{}, 1)

	storeOptions := StoreOptions{
		CollectionOptions: CollectionOptions{
			MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
			OnEvent: func(event Event) {
				mu.Lock()
				counts[event.Kind]++
				mu.Unlock()

				if event.Kind == EventKindPersisterProgress {
					<-persistedCh
				}

				if event.Kind == EventKindClose {
					doneCh <- struct{}{}
				}
			},
		},
	}

	spo := StorePersistOptions{CompactionConcern: CompactionForce}

	store, m, err := OpenStoreCollection(tmpDir, storeOptions, spo)
	if err != nil {
		t.Errorf("OpenStoreCollection err: %v", err)
	}

	b, _ := m.NewBatch(0, 0)
	for i := 0; i < 100; i++ {
		b.Set([]byte(fmt.Sprintf("%d", i)), []byte{})
	}
	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected exec batch for sets to work")
	}
	b.Close()

	persistedCh <- struct{}{}

	b, _ = m.NewBatch(0, 0)
	for i := 0; i < 100; i += 2 {
		b.Del([]byte(fmt.Sprintf("%d", i)))
	}
	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected exec batch for dels to work")
	}
	b.Close()

	persistedCh <- struct{}{}

	close(persistedCh)

	checkCollection := func(m Collection) {
		ss, err := m.Snapshot()
		if ss == nil || err != nil {
			t.Errorf("expected no err")
		}

		for i := 0; i < 100; i++ {
			xs := fmt.Sprintf("%d", i)
			v, err := ss.Get([]byte(xs), ReadOptions{})
			if err != nil {
				t.Errorf("expected no get err, got err: %v", err)
			}
			if i%2 == 0 {
				if v != nil {
					t.Errorf("expected get of deleted item to be nil")
				}
			} else {
				if v == nil || len(v) != 0 {
					t.Errorf("expected get not nil")
				}
			}

			v, err = ss.Get([]byte(xs), ReadOptions{NoCopyValue: true})
			if err != nil {
				t.Errorf("expected no get err, got err: %v", err)
			}
			if i%2 == 0 {
				if v != nil {
					t.Errorf("expected get of deleted item to be nil")
				}
			} else {
				if v == nil || len(v) != 0 {
					t.Errorf("expected get not nil")
				}
			}
		}

		v, err := ss.Get([]byte("not-there"), ReadOptions{})
		if err != nil {
			t.Errorf("expected no get err for not-there, got err: %v", err)
		}
		if v != nil {
			t.Errorf("expected get of not-there item to be nil")
		}

		ss.Close()
	}

	checkCollection(m)

	go m.Close()
	<-doneCh

	store.Close()

	store2, m2, err := OpenStoreCollection(tmpDir, storeOptions, spo)
	if err != nil || store2 == nil || m2 == nil {
		t.Errorf("expected reopen store to work")
	}

	checkCollection(m2)

	go m2.Close()
	<-doneCh

	store2.Close()
}

func TestStoreSnapshotPrevious(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var mu sync.Mutex
	counts := map[EventKind]int{}
	eventWaiters := map[EventKind]chan bool{}

	co := CollectionOptions{
		MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
		OnEvent: func(event Event) {
			mu.Lock()
			counts[event.Kind]++
			eventWaiter := eventWaiters[event.Kind]
			mu.Unlock()
			if eventWaiter != nil {
				eventWaiter <- true
			}
		},
	}

	store, m, err := OpenStoreCollection(tmpDir, StoreOptions{
		CollectionOptions: co,
	}, StorePersistOptions{
		CompactionConcern: CompactionDisable,
	})
	if err != nil || m == nil || store == nil {
		t.Errorf("expected open empty store collection to work")
	}

	// Insert 0, 2, 4, 6, 8.
	b, _ := m.NewBatch(0, 0)
	for i := 0; i < 10; i += 2 {
		xs := fmt.Sprintf("%d", i)
		x := []byte(xs)
		b.Set(x, x)
	}
	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected exec batch to work")
	}
	b.Close()

	checkIterator := func(ss Snapshot, start, delta int) {
		iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			t.Errorf("expected nil iter err, got: %v", err)
		}

		var lastNextErr error

		for i := start; i < 10; i += delta {
			if lastNextErr != nil {
				t.Errorf("expected nil lastNextErr, got: %v", lastNextErr)
			}

			k, v, err := iter.Current()
			if err != nil {
				xs := fmt.Sprintf("%d", i)
				if string(k) != xs || string(v) != xs {
					t.Errorf("expected %d, got %s = %s", i, k, v)
				}
			}

			lastNextErr = iter.Next()
		}

		if lastNextErr != ErrIteratorDone {
			t.Errorf("expected iterator done, got: %v", lastNextErr)
		}

		iter.Close()
	}

	waitUntilClean := func() error {
		for {
			stats, err := m.Stats()
			if err != nil {
				return err
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

	ss, err := store.Snapshot()
	if err != nil {
		t.Errorf("expected no snapshot err, got: %v", err)
	}

	checkIterator(ss, 0, 2)

	ss.Close()

	// Insert 1, 3, 5, 7, 9.
	b, _ = m.NewBatch(0, 0)
	for i := 1; i < 10; i += 2 {
		xs := fmt.Sprintf("%d", i)
		x := []byte(xs)
		b.Set(x, x)
	}
	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected exec batch to work")
	}
	b.Close()

	waitUntilClean()

	ss, err = store.Snapshot()
	if err != nil {
		t.Errorf("expected no snapshot err, got: %v", err)
	}

	checkIterator(ss, 0, 1)

	// ------------------------------------------

	ssPrev, err := store.SnapshotPrevious(ss)
	if err != nil {
		t.Errorf("expected SnapshotPrevious nil err, got: %v", err)
	}
	if ssPrev == nil {
		t.Errorf("expected ssPrev, got nil")
	}

	ss.Close()

	checkIterator(ssPrev, 0, 2)

	// ------------------------------------------

	ssPrevPrev, err := store.SnapshotPrevious(ssPrev)
	if err != nil {
		t.Errorf("expected SnapshotPrevious.PrevSnapshot nil err, got: %v", err)
	}
	if ssPrevPrev != nil {
		t.Errorf("expected ssPrevPrev nil, got: %+v", ssPrevPrev)
	}

	ssPrev.Close()

	// ------------------------------------------

	m.Close()

	store.Close()
}

func TestStoreSnapshotRevert(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var mu sync.Mutex
	counts := map[EventKind]int{}
	eventWaiters := map[EventKind]chan bool{}

	co := CollectionOptions{
		OnEvent: func(event Event) {
			mu.Lock()
			counts[event.Kind]++
			eventWaiter := eventWaiters[event.Kind]
			mu.Unlock()
			if eventWaiter != nil {
				eventWaiter <- true
			}
		},
	}

	store, m, err := OpenStoreCollection(tmpDir, StoreOptions{
		CollectionOptions: co,
	}, StorePersistOptions{
		CompactionConcern: CompactionDisable,
	})
	if err != nil || m == nil || store == nil {
		t.Errorf("expected open empty store collection to work")
	}

	// Insert 0, 2, 4, 6, 8.
	b, _ := m.NewBatch(0, 0)
	for i := 0; i < 10; i += 2 {
		xs := fmt.Sprintf("%d", i)
		x := []byte(xs)
		b.Set(x, x)
	}
	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected exec batch to work")
	}
	b.Close()

	checkIterator := func(ss Snapshot, start, delta int) {
		iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			t.Errorf("expected nil iter err, got: %v", err)
		}

		var lastNextErr error

		for i := start; i < 10; i += delta {
			if lastNextErr != nil {
				t.Errorf("expected nil lastNextErr, got: %v", lastNextErr)
			}

			k, v, err := iter.Current()
			if err != nil {
				xs := fmt.Sprintf("%d", i)
				if string(k) != xs || string(v) != xs {
					t.Errorf("expected %d, got %s = %s", i, k, v)
				}
			}

			lastNextErr = iter.Next()
		}

		if lastNextErr != ErrIteratorDone {
			t.Errorf("expected iterator done, got: %v", lastNextErr)
		}

		iter.Close()
	}

	waitUntilClean := func() error {
		for {
			stats, err := m.Stats()
			if err != nil {
				return err
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

	store.m.Lock()
	nextFNameSeqOrig := store.nextFNameSeq
	store.m.Unlock()

	ss, err := store.Snapshot()
	if err != nil {
		t.Errorf("expected no snapshot err, got: %v", err)
	}

	checkIterator(ss, 0, 2)

	ss.Close()

	// Insert 1, 3, 5, 7, 9.
	b, _ = m.NewBatch(0, 0)
	for i := 1; i < 10; i += 2 {
		xs := fmt.Sprintf("%d", i)
		x := []byte(xs)
		b.Set(x, x)
	}
	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected exec batch to work")
	}
	b.Close()

	waitUntilClean()

	store.m.Lock()
	nextFNameSeqCurr := store.nextFNameSeq
	store.m.Unlock()

	if nextFNameSeqCurr != nextFNameSeqOrig {
		t.Errorf("expected same fname seq")
	}

	ss, err = store.Snapshot()
	if err != nil {
		t.Errorf("expected no snapshot err, got: %v", err)
	}

	checkIterator(ss, 0, 1)

	// ------------------------------------------

	ssPrev, err := store.SnapshotPrevious(ss)
	if err != nil {
		t.Errorf("expected SnapshotPrevious nil err, got: %v", err)
	}
	if ssPrev == nil {
		t.Errorf("expected ssPrev, got nil")
	}

	checkIterator(ssPrev, 0, 2)
	checkIterator(ss, 0, 1)

	err = ss.Close()
	if err != nil {
		t.Errorf("expected ss Close nil err, got: %v", err)
	}

	// ------------------------------------------

	err = m.Close() // To shut down persistence goroutines.
	if err != nil {
		t.Errorf("expected coll Close nil err, got: %v", err)
	}

	// ------------------------------------------

	err = store.SnapshotRevert(ssPrev) // The star of the show.
	if err != nil {
		t.Errorf("expected store SnapshotRevert nil err, got: %v", err)
	}

	store.m.Lock()
	nextFNameSeqCurr = store.nextFNameSeq
	store.m.Unlock()

	if nextFNameSeqCurr != nextFNameSeqOrig {
		t.Errorf("expected same fname seq after revert")
	}

	err = ssPrev.Close()
	if err != nil {
		t.Errorf("expected ssPrev Close nil err, got: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Errorf("expected store Close nil err, got: %v", err)
	}

	// ------------------------------------------

	storeReverted, collReverted, err := // Reopen.
		OpenStoreCollection(tmpDir, StoreOptions{
			CollectionOptions: co,
		}, StorePersistOptions{
			CompactionConcern: CompactionDisable,
		})
	if err != nil || collReverted == nil || storeReverted == nil {
		t.Errorf("expected re-open store collection to work")
	}

	ssReverted, err := storeReverted.Snapshot()
	if err != nil {
		t.Errorf("expected no storeReverted snapshot err, got: %v", err)
	}

	checkIterator(ssReverted, 0, 2)

	ssReverted.Close()

	collReverted.Close()

	store.m.Lock()
	nextFNameSeqCurr = storeReverted.nextFNameSeq
	store.m.Unlock()

	if nextFNameSeqCurr != nextFNameSeqOrig {
		t.Errorf("expected same fname seq after reverted store reopened")
	}

	storeReverted.Close()
}

func openStoreAndWriteNItems(t *testing.T, tmpDir string,
	n int, numBatches int, readOnly bool) (s *Store, c Collection) {
	var store *Store
	var coll Collection
	var err error

	var m sync.Mutex
	var waitingForCleanCh chan struct{}

	var co CollectionOptions

	co = CollectionOptions{
		OnEvent: func(event Event) {
			if event.Kind == EventKindPersisterProgress {
				stats, err := coll.Stats()
				if err == nil && stats.CurDirtyOps <= 0 &&
					stats.CurDirtyBytes <= 0 && stats.CurDirtySegments <= 0 {
					m.Lock()
					if waitingForCleanCh != nil {
						waitingForCleanCh <- struct{}{}
						waitingForCleanCh = nil
					}
					m.Unlock()
				}
			}
		},
		ReadOnly: readOnly,
	}

	ch := make(chan struct{}, 1)

	store, coll, err = OpenStoreCollection(tmpDir,
		StoreOptions{CollectionOptions: co},
		StorePersistOptions{})

	if err != nil || store == nil {
		t.Errorf("Moss-OpenStoreCollection failed, err: %v", err)
	}

	if n <= numBatches {
		numBatches = 1
	}

	itemsPerBatch := n / numBatches
	itemCount := 0

	for bi := 0; bi < numBatches; bi++ {
		if itemsPerBatch > n-itemCount {
			itemsPerBatch = n - itemCount
		}

		if itemsPerBatch <= 0 {
			break
		}

		batch, err := coll.NewBatch(itemsPerBatch, itemsPerBatch*15)
		if err != nil {
			t.Errorf("Expected NewBatch() to succeed!")
		}

		for i := 0; i < itemsPerBatch; i++ {
			k := []byte(fmt.Sprintf("key%d", i))
			v := []byte(fmt.Sprintf("val%d", i))
			itemCount++

			batch.Set(k, v)
		}

		m.Lock()
		waitingForCleanCh = ch
		m.Unlock()

		err = coll.ExecuteBatch(batch, WriteOptions{})
		if err != nil {
			t.Errorf("Expected ExecuteBatch() to work!")
		}
	}

	if readOnly {
		// In the readOnly mode, the persister will not run and therefore
		// nothing gets put on the channel that we need to block on.
		// However to ensure that the persister does not persist anything
		// in this scenario, test it by manually invoking the Persist API
		ss, _ := coll.Snapshot()
		llss, err := store.Persist(ss, StorePersistOptions{})
		if err != nil || llss == nil {
			t.Errorf("Expected Store;Persist() to succeed!")
		}
		ss.Close()
	} else {
		<-ch
	}

	return store, coll
}

func fetchOpsSetFromFooter(store *Store) uint64 {
	if store == nil {
		return 0
	}

	curr_snap, err := store.Snapshot()
	if err != nil || curr_snap == nil {
		return 0
	}

	var ops_set uint64

	footer := curr_snap.(*Footer)
	for i := range footer.SegmentLocs {
		sloc := &footer.SegmentLocs[i]
		ops_set += sloc.TotOpsSet
	}

	curr_snap.Close()

	return ops_set
}

func TestStoreReadOnlyOption(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	// Open store, coll in Regular mode, and write 10 items
	store, coll := openStoreAndWriteNItems(t, tmpDir, 10, 1, false)

	if fetchOpsSetFromFooter(store) != 10 {
		t.Errorf("Unexpected number of sets!")
	}

	coll.Close()
	store.Close()

	// Reopen store, coll in ReadOnly mode, and write 10 more items
	store, coll = openStoreAndWriteNItems(t, tmpDir, 10, 1, true)

	// Expect no additional sets since the first batch
	if fetchOpsSetFromFooter(store) != 10 {
		t.Errorf("Extra number of sets detected!")
	}

	coll.Close()
	store.Close()

	// Reopen store, coll in regular mode, and write 10 more items
	store, coll = openStoreAndWriteNItems(t, tmpDir, 10, 1, false)

	// Expect 10 more sets since the first batch
	if fetchOpsSetFromFooter(store) != 20 {
		t.Errorf("Unexpected number of sets!")
	}

	coll.Close()
	store.Close()
}

func TestStoreCollHistograms(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	itemCount := 10000
	batchCount := 100

	// Open store, coll and write about 10000 items
	store, coll := openStoreAndWriteNItems(
		t, tmpDir, itemCount, batchCount, false)
	coll.Close()
	store.Close()

	shistograms := store.Histograms()
	num_histograms := len(shistograms)

	if shistograms["PersistUsecs"].TotCount == 0 {
		t.Errorf("Expected a few entries for PersistUsecs!")
	}

	if shistograms["CompactUsecs"].TotCount != 0 {
		t.Errorf("Expected no entries for CompactUsecs!")
	}

	if shistograms["PersistFooterUsecs"].TotCount !=
		shistograms["PersistUsecs"].TotCount {
		t.Errorf("Expected PersisterFooterUsecs to match PersistUsecs!")
	}

	chistograms := coll.Histograms()

	if chistograms["ExecuteBatchUsecs"].TotCount != uint64(batchCount) {
		t.Errorf("Unexpected number of ExecuteBatchUsecs samples!")
	}

	if chistograms["MergerUsecs"].TotCount == 0 {
		t.Errorf("Expected a few entries for MergerUsecs!")
	}

	if shistograms.AddAll(chistograms) != nil {
		t.Errorf("Expected AddAll to succeed")
	}

	if len(shistograms) != num_histograms+len(chistograms) {
		t.Errorf("shistograms carries unexpected number of histograms")
	}

	if shistograms["ExecuteBatchUsecs"] == nil ||
		shistograms["ExecuteBatchUsecs"].TotCount !=
			chistograms["ExecuteBatchUsecs"].TotCount {
		t.Errorf("Expected ExecutedBatchUsecs to match chistograms'")
	}

	if shistograms["MergerUsecs"] == nil ||
		shistograms["MergerUsecs"].TotCount !=
			chistograms["MergerUsecs"].TotCount {
		t.Errorf("Expected MergerUsecs to match chistograms'")
	}

	if chistograms["MutationKeyBytes"].TotCount != uint64(itemCount) {
		t.Errorf("Unexpected number of MutationKeyBytes samples!")
	}

	if chistograms["MutationValBytes"].TotCount != uint64(itemCount) {
		t.Errorf("Unexpected number of MutationValBytes samples!")
	}

	if chistograms["ExecuteBatchBytes"].TotCount != uint64(batchCount) {
		t.Errorf("Unexpected number of ExecutedBatchBytes samples!")
	}

	if chistograms["ExecuteBatchOpsCount"].TotCount !=
		uint64(itemCount/batchCount) {
		t.Errorf("Unexpected number of ExecuteBatchOpsCount samples!")
	}
}
