// Copyright © 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/couchbase/moss"
)

var ITEMS = 5

func initStore(t *testing.T, createDir bool, batches int) (d string,
	s *moss.Store,
	c moss.Collection) {

	dir := "testStatsStore"
	if createDir {
		os.Mkdir(dir, 0777)
	}

	var m sync.Mutex
	var waitingForCleanCh chan struct{}

	var err error
	var store *moss.Store
	var coll moss.Collection

	co := moss.CollectionOptions{
		OnEvent: func(event moss.Event) {
			if event.Kind == moss.EventKindPersisterProgress {
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

	store, coll, err = moss.OpenStoreCollection(dir,
		moss.StoreOptions{CollectionOptions: co},
		moss.StorePersistOptions{})
	if err != nil || store == nil {
		t.Errorf("Moss-OpenStoreCollection failed, err: %v\n", err)
	}

	ch := make(chan struct{}, 1)

	itemsPerBatch := 1
	if batches < ITEMS {
		itemsPerBatch = int(math.Ceil(float64(ITEMS) / float64(batches)))
	}

	itemsWrittenInBatch := 0
	var batch moss.Batch

	for i := 0; i < ITEMS; i++ {
		if itemsWrittenInBatch == 0 {
			batch, err = coll.NewBatch(itemsPerBatch, itemsPerBatch*8)
			if err != nil {
				t.Errorf("Expected NewBatch() to succeed!")
			}
		}

		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%d", i))

		batch.Set(k, v)
		itemsWrittenInBatch++

		if itemsWrittenInBatch == itemsPerBatch || i == ITEMS-1 {
			m.Lock()
			waitingForCleanCh = ch
			m.Unlock()

			err = coll.ExecuteBatch(batch, moss.WriteOptions{})
			if err != nil {
				t.Errorf("Expected ExecuteBatch() to work!")
			}

			itemsWrittenInBatch = 0
		}
	}

	<-ch

	return dir, store, coll
}

func cleanupStore(dir string, store *moss.Store, coll moss.Collection) {
	if dir != "" {
		defer os.RemoveAll(dir)
	}

	if store != nil {
		defer store.Close()
	}

	if coll != nil {
		defer coll.Close()
	}
}

const (
	FOOTERSTATS        = 1
	FRAGMENTATIONSTATS = 2
)

func init2FootersAndInterceptStdout(t *testing.T, batches int,
	command int) (ret string) {
	// Footer 1 (1 segment)
	_, store, coll := initStore(t, true, batches)
	cleanupStore("", store, coll)

	// Footer 2 (2 segments)
	dir, store, coll := initStore(t, false, batches)

	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	var err error

	jsonFormat = true
	dirs := []string{dir}
	if command == FOOTERSTATS {
		err = invokeFooterStats(dirs)
	} else if command == FRAGMENTATIONSTATS {
		err = invokeFragStats(dirs)
	} else {
		t.Errorf("Unknown CMD: %d", command)
	}

	if err != nil {
		t.Error(err)
	}

	outC := make(chan string)
	// copy the output in a separate goroutine so dump wouldn't
	// block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC

	cleanupStore(dir, store, coll)

	return out
}

func TestLatestFooterStats(t *testing.T) {
	out := init2FootersAndInterceptStdout(t, 2, FOOTERSTATS)

	var m []interface{}
	json.Unmarshal([]byte(out), &m)
	if len(m) != 1 {
		t.Errorf("Expected one directory, but count: %d!", len(m))
	}

	store_data := m[0].(map[string]interface{})

	if store_data["testStatsStore"] == nil {
		t.Errorf("Expected directory not found!")
	}

	if len(store_data) != 1 {
		t.Errorf("Expected 1 footer only!")
	}

	footer_data := store_data["testStatsStore"].(map[string]interface{})

	if footer_data["Footer_1"] == nil {
		t.Errorf("Expected Footer_1 to be the latest footer!")
	}

	stats := footer_data["Footer_1"].(map[string]interface{})

	if stats["total_ops_set"] != float64(2*ITEMS) {
		t.Errorf("Unexpected total_ops_set: %v!",
			stats["total_ops_set"])
	}

	if stats["total_key_bytes"] != float64(4*(2*ITEMS)) {
		t.Errorf("Unexpected key bytes: %v!",
			stats["total_key_bytes"])
	}

	if stats["total_val_bytes"] != float64(4*(2*ITEMS)) {
		t.Errorf("Unexpected val bytes: %v!",
			stats["total_val_bytes"])
	}
}

func TestFragmentationStats(t *testing.T) {
	out := init2FootersAndInterceptStdout(t, 2, FRAGMENTATIONSTATS)

	var m []interface{}
	json.Unmarshal([]byte(out), &m)
	if len(m) != 1 {
		t.Errorf("Expected one directory, but count: %d!", len(m))
	}

	store_data := m[0].(map[string]interface{})

	if store_data["testStatsStore"] == nil {
		t.Errorf("Expected directory not found!")
	}

	stats := store_data["testStatsStore"].(map[string]interface{})

	if stats["data_bytes"] == nil {
		t.Errorf("Expected an entry for data_bytes!")
	}

	if stats["dir_size"] == nil {
		t.Errorf("Expected an entry for dir_size")
	}

	if stats["fragmentation_bytes"] == nil {
		t.Errorf("Expected an entry for fragmentation_bytes!")
	}

	if stats["fragmentation_percent"] == nil {
		t.Errorf("Expected an entry for fragmentation_percent!")
	}
}
