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
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/edsrzf/mmap-go"
)

func TestPageAlign(t *testing.T) {
	if pageAlignCeil(0) != 0 {
		t.Errorf("expect 0")
	}
	if pageAlignCeil(1) != int64(StorePageSize) {
		t.Errorf("expect sps")
	}
	if pageAlignCeil(int64(StorePageSize-1)) != int64(StorePageSize) {
		t.Errorf("expect sps")
	}
	if pageAlignCeil(int64(StorePageSize)) != int64(StorePageSize) {
		t.Errorf("expect sps")
	}
	if pageAlignCeil(int64(StorePageSize+1)) != int64(2*StorePageSize) {
		t.Errorf("expect sps")
	}

	if pageAlignFloor(0) != 0 {
		t.Errorf("expect 0")
	}
	if pageAlignFloor(1) != 0 {
		t.Errorf("expect 0")
	}
	if pageAlignFloor(int64(StorePageSize)+1) != int64(StorePageSize) {
		t.Errorf("expect sps")
	}
	if pageAlignFloor(int64(StorePageSize)) != int64(StorePageSize) {
		t.Errorf("expect sps")
	}
	if pageAlignFloor(int64(2*StorePageSize)-1) != int64(StorePageSize) {
		t.Errorf("expect sps")
	}
}

func TestPageOffset(t *testing.T) {
	p := int64(StorePageSize)

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
				fileRefMap: make(map[string]*FileRef),
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
	// On some platforms (windows) there is a delay between the time data
	// is synced into a file and the time it reflects in the directory stats.
	// As a result this stat can temporarily be zero. To avoid false failures,
	// disable checking for this stat on these platforms.
	if !IsTimingCoarse &&
		sstats["num_bytes_used_disk"].(uint64) <= 0 {
		t.Errorf("expected >0 num_bytes_used_disk")
	}
	if sstats["total_persists"].(uint64) <= 0 {
		t.Errorf("expected >0 total_persists")
	}
	if sstats["num_files"].(int) != 1 {
		t.Errorf("expected 1 file on disk")
	}
	if sstats["num_files_open"].(int) != 1 {
		t.Errorf("expected 1 tracked open file")
	}

	files := sstats["files"].(map[string]interface{})
	data := files["data-0000000000000001.moss"].(map[string]interface{})
	if data["ref_count"].(int) < 1 {
		t.Errorf("expected ref count of file to be >= 1")
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
		ss, errx := store.Persist(higher, spo)
		persistCh <- struct{}{}
		return ss, errx
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

	fileInfos, erro := waitForCompactionCleanup(tmpDir, 10)
	if erro != nil {
		t.Fatalf("Error waiting for compaction cleanup :%v", erro)
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
			var stats *CollectionStats
			stats, err = m.Stats()
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
		var ss Snapshot
		ss, err = m.Snapshot()
		if ss == nil || err != nil {
			t.Errorf("expected no err")
		}

		for i := 0; i < 100; i++ {
			xs := fmt.Sprintf("%d", i)
			var v []byte
			v, err = ss.Get([]byte(xs), ReadOptions{})
			if err != nil || v != nil {
				t.Errorf("expected no keys")
			}
		}

		var itr Iterator
		itr, err = ss.StartIterator(nil, nil, IteratorOptions{
			IncludeDeletions: includeDeletions,
		})
		if err != nil || itr == nil {
			t.Errorf("expected no err iterator")
		}
		var entryEx EntryEx
		var k, v []byte
		entryEx, k, v, err = itr.CurrentEx()
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
		var ss Snapshot
		ss, err = m.Snapshot()
		if ss == nil || err != nil {
			t.Errorf("expected no err")
		}

		for i := 0; i < 100; i++ {
			xs := fmt.Sprintf("%d", i)
			var v []byte
			v, err = ss.Get([]byte(xs), ReadOptions{})
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

		var v []byte
		v, err = ss.Get([]byte("not-there"), ReadOptions{})
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
		var iter Iterator
		iter, err = ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			t.Errorf("expected nil iter err, got: %v", err)
		}

		var lastNextErr error

		for i := start; i < 10; i += delta {
			if lastNextErr != nil {
				t.Errorf("expected nil lastNextErr, got: %v", lastNextErr)
			}

			var k, v []byte
			k, v, err = iter.Current()
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
			var stats *CollectionStats
			stats, err = m.Stats()
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
		var iter Iterator
		iter, err = ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			t.Errorf("expected nil iter err, got: %v", err)
		}

		var lastNextErr error

		for i := start; i < 10; i += delta {
			if lastNextErr != nil {
				t.Errorf("expected nil lastNextErr, got: %v", lastNextErr)
			}

			var k, v []byte
			k, v, err = iter.Current()
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
			var stats *CollectionStats
			stats, err = m.Stats()
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
				var stats *CollectionStats
				stats, err = coll.Stats()
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

	currSnap, err := store.Snapshot()
	if err != nil || currSnap == nil {
		return 0
	}

	var opsSet uint64

	footer := currSnap.(*Footer)
	for i := range footer.SegmentLocs {
		sloc := &footer.SegmentLocs[i]
		opsSet += sloc.TotOpsSet
	}

	currSnap.Close()

	return opsSet
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
	numHistograms := len(shistograms)

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

	if len(shistograms) != numHistograms+len(chistograms) {
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

func TestStoreCompactMaxSegments(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var store *Store
	var coll Collection

	var err error

	store, coll, err = OpenStoreCollection(tmpDir,
		StoreOptions{
			CompactionPercentage: 100, CompactionLevelMaxSegments: 5,
			CompactionLevelMultiplier: 100000},
		StorePersistOptions{CompactionConcern: CompactionAllow})

	if err != nil || store == nil {
		t.Errorf("Moss-OpenStoreCollection failed, err: %v", err)
	}

	numBatches := 200
	itemsPerBatch := 100
	itemCount := 0

	for bi := 0; bi < numBatches; bi++ {
		batch, err1 := coll.NewBatch(itemsPerBatch, itemsPerBatch*15)
		if err1 != nil {
			t.Errorf("Expected NewBatch() to succeed!")
		}

		for i := 0; i < itemsPerBatch; i++ {
			k := []byte(fmt.Sprintf("key%d", i))
			v := []byte(fmt.Sprintf("val%d", i))
			itemCount++

			batch.Set(k, v)
		}

		err1 = coll.ExecuteBatch(batch, WriteOptions{})
		if err1 != nil {
			t.Errorf("Expected ExecuteBatch() to work!")
		}
		waitForPersistence(coll)
	}

	sstats, err := store.Stats()
	if err != nil {
		t.Errorf("expected no stats err")
	}
	if sstats == nil {
		t.Errorf("expected non-nil stats")
	}
	if sstats["total_persists"].(uint64) <= 0 {
		t.Errorf("expected >0 total_persists")
	}
	if sstats["total_compactions"].(uint64) <= 0 {
		t.Errorf("expected >0 total_compactions")
	}
}

func TestStoreCrashRecovery(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	var store *Store
	var coll Collection

	var m sync.Mutex
	var waitingForCleanCh chan struct{}

	co := CollectionOptions{
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
	}

	ch := make(chan struct{}, 1)

	var err error

	store, coll, err = OpenStoreCollection(tmpDir,
		StoreOptions{CollectionOptions: co},
		StorePersistOptions{CompactionConcern: CompactionDisable})

	if err != nil || store == nil {
		t.Errorf("Moss-OpenStoreCollection failed, err: %v", err)
	}

	numBatches := 200
	itemsPerBatch := 100
	itemCount := 0

	for bi := 0; bi < numBatches; bi++ {
		batch, err1 := coll.NewBatch(itemsPerBatch, itemsPerBatch*15)
		if err1 != nil {
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

		err1 = coll.ExecuteBatch(batch, WriteOptions{})
		if err1 != nil {
			t.Errorf("Expected ExecuteBatch() to work!")
		}
	}

	<-ch

	file := store.footer.SegmentLocs[0].mref.fref.file
	finfo, err := file.Stat()
	if err != nil {
		t.Errorf("Expected file stat to work %v", err)
	}
	sizeRem := finfo.Size() % int64(StorePageSize)
	if sizeRem != 0 { // Round up the file to 4K size with garbage
		fillUpBuf := make([]byte, int64(StorePageSize)-sizeRem)
		n, _ := file.WriteAt(fillUpBuf, finfo.Size())
		if n != len(fillUpBuf) {
			t.Errorf("Unable to pad up file: error %v", err)
		}
		err = file.Sync()
		if err != nil {
			t.Errorf("Expected file sync to work %v", err)
		}
	}
	coll.Close() // Properly wait for background tasks.
	store.Close()

	store2, _, err2 := OpenStoreCollection(tmpDir,
		StoreOptions{CollectionOptions: co},
		StorePersistOptions{CompactionConcern: CompactionDisable})

	if err2 != nil || store2 == nil {
		t.Errorf("Moss-OpenStoreCollection failed, err: %v", err2)
	}

}

func TestStoreLargeDeletions(t *testing.T) {
	numItems := 1000000
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)

	so := DefaultStoreOptions
	spo := StorePersistOptions{CompactionConcern: CompactionDisable}

	store, coll, err := OpenStoreCollection(tmpDir, so, spo)
	if err != nil || store == nil || coll == nil {
		t.Fatalf("error opening store collection:%v", tmpDir)
	}
	ba, erro := coll.NewBatch(0, 0)
	if erro != nil {
		t.Fatalf("error creating new batch: %v", err)
		return
	}

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("%08d", i)
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

	// Now create a Huuuugggeee segment that has just 10 sets and
	// like a million deletes.
	ba, erro = coll.NewBatch(numItems, numItems*16)
	if erro != nil {
		t.Fatalf("error creating new batch: %v", err)
		return
	}
	for i := 0; i < numItems; i++ {
		k := fmt.Sprintf("%08d", i)
		if i < 10 {
			ba.Set([]byte(k), []byte(k))
		} else {
			ba.Del([]byte(k))
		}
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

	// Now attempting to iterate over these keys should have to skip over
	// the huuggeee number of deletes. Any use of O(N) recursion will likely
	// result in stack frame explosion (and fail bigly).
	ss, _ := coll.Snapshot()
	iter, erro := ss.StartIterator(nil, nil, IteratorOptions{})
	if erro != nil {
		t.Fatalf("error opening collection iterator:%v", erro)
	}
	i := 0
	for ; err == nil; err, i = iter.Next(), i+1 {
		key := fmt.Sprintf("%08d", i)
		k, _, erro := iter.Current()
		if erro != nil {
			break
		}
		if !bytes.Equal(k, []byte(key)) {
			fmt.Println("Data loss for item ", key, " vs ", string(k))
			t.Fatalf("Data loss for item detected!")
		}
	}
	if i != 10 {
		t.Errorf("Data loss: Expected %d items, but iterated only %d", numItems, i)
	}
	iter.Close()
	ss.Close()
}

// Since file deletions happen asynchronously, sometimes it is possible
// that the deleted file still lingers around.
// To ensure this does not cause test failures, retry a few times
// before just returning the contents of the directory.
func waitForCompactionCleanup(tmpDir string, secsToWait int) (
	fileInfos []os.FileInfo, err error) {
	fileInfos, err = ioutil.ReadDir(tmpDir)
	if err != nil {
		return
	}
	for retry := secsToWait; retry > 0 && len(fileInfos) != 1; retry-- {
		file, erro := os.Open(tmpDir)
		if erro != nil {
			return fileInfos, erro
		}
		file.Sync()
		file.Close()

		time.Sleep(1 * time.Second)
		fileInfos, err = ioutil.ReadDir(tmpDir)
	}
	return
}

func TestCompactionWithAndWithoutRegularSync(t *testing.T) {
	numItems := 1000000

	ch := make(chan *testResults)

	runTest := func(name string, batchSize, syncAfterBytes int) {
		tmpDir, _ := ioutil.TempDir("", "mossStore")
		defer os.RemoveAll(tmpDir)

		so := DefaultStoreOptions
		so.CompactionSync = true
		so.CompactionSyncAfterBytes = syncAfterBytes
		spo := StorePersistOptions{CompactionConcern: CompactionAllow}

		store, coll, er := OpenStoreCollection(tmpDir, so, spo)
		if er != nil || store == nil || coll == nil {
			t.Fatalf("error opening store collection: %v", tmpDir)
		}

		loadComplete := make(chan bool)
		fetchComplete := make(chan bool)

		load := func() {
			x := numItems
			for x > 0 {
				ba, err := coll.NewBatch(batchSize, batchSize*512)
				if err != nil {
					t.Fatalf("error creating new batch: %v", err)
				}

				for i := 0; i < batchSize; i++ {
					k := fmt.Sprintf("%08d", x)
					v := fmt.Sprintf("%128d", x)
					ba.Set([]byte(k), []byte(v))
					x--
				}

				err = coll.ExecuteBatch(ba, WriteOptions{})
				if err != nil {
					t.Fatalf("error executing batch: %v", err)
				}

				err = ba.Close()
				if err != nil {
					t.Fatalf("error closing batch: %v", err)
				}
			}

			loadComplete <- true
		}

		var readtime time.Duration
		fetchtimes := make([]time.Duration, numItems)
		var aggregate int64

		fetch := func() {
			start := time.Now()
			for i := 0; i < numItems; i++ {
				key := fmt.Sprintf("%08d", i+1)
				gstart := time.Now()
				val, err := coll.Get([]byte(key), ReadOptions{})
				fetchtimes[i] = time.Since(gstart)
				expect := fmt.Sprintf("%128d", i+1)
				if err != nil || string(val) != expect {
					t.Fatalf("Unexpected error for key '%v':"+
						" %v / Vals mismatch: '%v' != '%v'",
						key, err, string(val), expect)
				}
				aggregate += fetchtimes[i].Nanoseconds()
			}
			readtime = time.Since(start)

			fetchComplete <- true
		}

		// Initial load
		go load()
		<-loadComplete

		// Concurrent load & fetch
		go load()
		go fetch()

		<-loadComplete
		<-fetchComplete

		sort.Sort(byNS(fetchtimes))

		mean := time.Duration(aggregate/int64(len(fetchtimes))) * time.Nanosecond

		ch <- &testResults{
			name:       name,
			batchsize:  batchSize,
			readtime:   readtime,
			fetchtimes: fetchtimes,
			mean:       mean,
		}
	}

	go runTest("WITHOUT REGULAR SYNC", 100, 0)
	go runTest("WITH REGULAR SYNC", 100, 1000000)
	go runTest("WITHOUT REGULAR SYNC", 10000, 0)
	go runTest("WITH REGULAR SYNC", 10000, 1000000)

	var results []*testResults
	for i := 0; i < 4; i++ {
		results = append(results, <-ch)
	}

	fmt.Printf("%8v (numItems: %10v) %19v %v\n",
		" ", numItems,
		" ", "<-------------------- Fetch times -------------------->")
	fmt.Printf("%20v  %9v  %14v  %10v  %10v  %10v  %10v  %10v\n",
		" ", "BatchSize", "Readtime(s)",
		"Mean", "Median", "90th", "95th", "99th")
	for _, result := range results {
		fmt.Printf("%20v  %9v  %14.3v  %10v  %10v  %10v  %10v  %10v\n",
			result.name,
			result.batchsize,
			result.readtime.Seconds(),
			result.mean,
			result.fetchtimes[len(result.fetchtimes)/2],
			result.fetchtimes[90*len(result.fetchtimes)/100],
			result.fetchtimes[95*len(result.fetchtimes)/100],
			result.fetchtimes[99*len(result.fetchtimes)/100])
	}
}
