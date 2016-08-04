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

	"github.com/edsrzf/mmap-go"
)

func TestMultipleMMapsOnSameFile(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossMMap")
	defer os.RemoveAll(tmpDir)

	f, err := os.Create(tmpDir + string(os.PathSeparator) + "test.file")
	if err != nil {
		t.Errorf("expected open file to work, err: %v", err)
	}

	offset := 1024 * 1024 * 1024 // 1 GB.

	f.WriteAt([]byte("hello"), int64(offset))

	var mms []mmap.MMap

	for i := 0; i < 100; i++ { // Re-mmap the file.
		mm, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			t.Errorf("expected mmap to work, err: %v", err)
		}

		if string(mm[offset:offset+5]) != "hello" {
			t.Errorf("expected hello")
		}

		mms = append(mms, mm)
	}

	for _, mm := range mms {
		if string(mm[offset:offset+5]) != "hello" {
			t.Errorf("expected hello")
		}

		for j := 0; j < offset; j += 1024 * 1024 {
			if mm[j] != 0 {
				t.Errorf("expected 0")
			}
		}
	}

	for _, mm := range mms {
		mm.Unmap()
	}

	f.Close()
}

func TestMMapRef(t *testing.T) {
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

	b, _ := m.NewBatch(0, 0)
	for i := 0; i < 1000; i++ {
		xs := fmt.Sprintf("%d", i)
		x := []byte(xs)
		b.Set(x, x)
	}
	err = m.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Errorf("expected exec batch to work")
	}
	b.Close()

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

	if store.refs != 1 {
		t.Errorf("expected 1 store ref, got: %d", store.refs)
	}

	footer := store.footer
	if footer == nil {
		t.Errorf("expected footer")
	}

	footer.m.Lock()
	if footer.refs != 2 {
		t.Errorf("expected 2 footer ref, : got: %d", footer.refs)
	}

	mref := footer.mref
	if mref == nil {
		t.Errorf("expected mref")
	}

	mrefsCheck := func(expected int) {
		mref.m.Lock()
		if mref.refs != expected {
			t.Errorf("expected mref.refs to be %d, got: %d", expected, mref.refs)
		}
		mref.m.Unlock()
	}

	mrefsCheck(1)

	rv := mref.AddRef()

	mrefsCheck(2)

	if rv != mref {
		t.Errorf("expected rv == mref")
	}

	if mref.DecRef() != nil {
		t.Errorf("expected mref.DecRef to be nil")
	}

	mrefsCheck(1)

	footer.m.Unlock()

	store.m.Unlock()

	m.Close()

	store.Close()

	footer.m.Lock()
	if footer.refs != 0 {
		t.Errorf("expected footer refs to be 0, got: %d", footer.refs)
	}
	footer.m.Unlock()

	store.m.Lock()
	if store.refs != 0 {
		t.Errorf("expected store refs to be 0, got: %d", footer.refs)
	}
	store.m.Unlock()

	mrefsCheck(0)
}
