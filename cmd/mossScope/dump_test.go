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

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"encoding/json"
	"reflect"

	"github.com/couchbase/moss"
	"github.com/couchbase/moss/cmd/mossScope/cmd"
)

var ITEM_COUNT = 10

func setup(t *testing.T, createDir bool) (d string, s *moss.Store, c moss.Collection) {
	dir := "./testStore"
	if createDir {
		os.Mkdir(dir, 0777)
	}

	store, err := moss.OpenStore(dir, moss.StoreOptions{})
	if err != nil || store == nil {
		t.Errorf("Expected OpenStore() to work!")
	}

	coll, _ := moss.NewCollection(moss.CollectionOptions{})
	coll.Start()

	// Creates
	batch, err := coll.NewBatch(ITEM_COUNT, ITEM_COUNT * 8)
	if err != nil {
		t.Errorf("Expected NewBatch() to succeed!")
	}

	for i := 0; i < ITEM_COUNT; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("val%d", i))
		batch.Set(k, v)
	}

	err = coll.ExecuteBatch(batch, moss.WriteOptions{})
	if err != nil {
		t.Errorf("Expected ExecuteBatch() to work!")
	}

	ss, _ := coll.Snapshot()

	llss, err := store.Persist(ss, moss.StorePersistOptions{})
	if err != nil || llss == nil {
		t.Errorf("Expected Persist() to succeed!")
	}

	ss.Close()

	return dir, store, coll
}

func cleanup(dir string, store *moss.Store, coll moss.Collection) {
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

func dumpHelper(t *testing.T, keysOnly bool) (output string) {
	dir, store, coll := setup(t, true)

	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd.Dump(dir, keysOnly)

	outC := make(chan string)
	// copy the output in a separate goroutine so dump wouldn't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC

	cleanup(dir, store, coll)

	return out
}

func TestDump(t *testing.T) {
	out := dumpHelper(t, false)

	var m []interface{}
	json.Unmarshal([]byte(out), &m)
	if len(m) != ITEM_COUNT {
		t.Errorf("Incorrect number of entries: %d!", len(m))
	}

	for i := 0; i < ITEM_COUNT; i++ {
		entry := m[i].(map[string]interface{})
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("val%d", i)
		if strings.Compare(k, entry["K"].(string)) != 0 {
			t.Errorf("Mismatch in key [%s != %s]!", k, entry["K"].(string))
		}
		if strings.Compare(v, entry["V"].(string)) != 0 {
			t.Errorf("Mismatch in value [%s != %s]!", v, entry["V"].(string))
		}
	}
}

func TestDumpKeysOnly(t *testing.T) {
	out := dumpHelper(t, true)

	var m []interface{}
	json.Unmarshal([]byte(out), &m)
	if (len(m) != ITEM_COUNT) {
		t.Errorf("Incorrect number of entries: %d!", len(m))
	}

	for i := 0; i < ITEM_COUNT; i++ {
		entry := m[i].(map[string]interface{})
		k := fmt.Sprintf("key%d", i)
		if strings.Compare(k, entry["K"].(string)) != 0 {
			t.Errorf("Mismatch in key [%s != %s]!", k, entry["K"].(string))
		}
	}
}

func TestDumpKey(t *testing.T) {
	dir, store, coll := setup(t, true)

	for i := 0; i < ITEM_COUNT; i++ {
		old := os.Stdout // keep backup of the real stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		key := fmt.Sprintf("key%d", i)
		cmd.Key(key, dir, false)

		outC := make(chan string)
		// copy the output in a separate goroutine so dump wouldn't block indefinitely
		go func() {
			var buf bytes.Buffer
			io.Copy(&buf, r)
			outC <- buf.String()
		}()

		// back to normal state
		w.Close()
		os.Stdout = old // restoring the real stdout
		out := <-outC

		var m []interface{}
		json.Unmarshal([]byte(out), &m)
		if len(m) != 1 {
			t.Errorf("Incorrect number of entries: %d!", len(m))
		}

		entry := m[0].(map[string]interface{})
		val := fmt.Sprintf("val%d", i)
		if strings.Compare(key, entry["K"].(string)) != 0 {
			t.Errorf("Mismatch in key [%s != %s]!", key, entry["K"].(string))
		}
		if strings.Compare(val, entry["V"].(string)) != 0 {
			t.Errorf("Mismatch in value [%s != %s]!", val, entry["V"].(string))
		}
	}

	cleanup(dir, store, coll)

}

func TestDumpKeyAllVersions(t *testing.T) {
	// Creates
	_, store, coll := setup(t, true)
	cleanup("", store, coll)

	// Updates
	dir, store, coll := setup(t, false)

	for i := 0; i < ITEM_COUNT; i++ {
		old := os.Stdout // keep backup of the real stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		key := fmt.Sprintf("key%d", i)
		cmd.Key(key, dir, true)

		outC := make(chan string)
		// copy the output in a separate goroutine so dump wouldn't block indefinitely
		go func() {
			var buf bytes.Buffer
			io.Copy(&buf, r)
			outC <- buf.String()
		}()

		// back to normal state
		w.Close()
		os.Stdout = old // restoring the real stdout
		out := <-outC

		var m []interface{}
		json.Unmarshal([]byte(out), &m)
		if len(m) != 2 {
			t.Errorf("Incorrect number of entries!")
		}

		val := fmt.Sprintf("val%d", i)
		for j := 0; j < len(m); j++ {
			entry := m[j].(map[string]interface{})
			if strings.Compare(key, entry["K"].(string)) != 0 {
				t.Errorf("Mismatch in key [%s != %s]!", key, entry["K"].(string))
			}
			if strings.Compare(val, entry["V"].(string)) != 0 {
				t.Errorf("Mismatch in value [%s != %s]!", val, entry["V"].(string))
			}
		}
	}

	cleanup(dir, store, coll)
}

func TestDumpAllFooters(t *testing.T) {
	// Footer 1 (1 segment)
	_, store, coll := setup(t, true)
	cleanup("", store, coll)

	// Footer 2 (2 segments)
	dir, store, coll := setup(t, false)

	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd.Footer(dir, true)

	outC := make(chan string)
	// copy the output in a separate goroutine so dump wouldn't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// back to normal state
	w.Close()
	os.Stdout = old // restoring the real stdout
	out := <-outC

	var m []interface{}
	json.Unmarshal([]byte(out), &m)
	if len(m) != 2 {
		t.Errorf("Incorrect number of entries: %d!", len(m))
	}

	entry := m[0].(map[string]interface{})
	// Expect 2 segment locs on latest footer (footer 2)
	records := reflect.ValueOf(entry["SegmentLocs"])
	if records.Len() != 2 {
		t.Errorf("Unexpected number of segment locs in latest footer: %d", records.Len())
	}

	for i := 0; i < records.Len(); i++ {
		stats := (records.Index(i).Interface()).(map[string]interface{})
		if stats["TotOpsSet"].(float64) != float64(ITEM_COUNT) {
			t.Errorf("[Footer2] Unexpected value for TotOpsSet stat: %d!", stats["TotOpsSet"])
		}
		if stats["TotOpsDel"].(float64) != 0 {
			t.Errorf("[Footer2] Unexpected value for TotOpsDel stat: %d!", stats["TotOpsDel"])
		}
	}

	entry = m[1].(map[string]interface{})
	// Expect 1 segment loc on the older footer (footer 1)
	records = reflect.ValueOf(entry["SegmentLocs"])
	if records.Len() != 1 {
		t.Errorf("Unexpected number of segment locs in older footer: %d", records.Len())
	}

	stats := (records.Index(0).Interface()).(map[string]interface{})
	if stats["TotOpsSet"].(float64) != float64(ITEM_COUNT) {
		t.Errorf("[Footer1] Unexpected value for TotOpsSet stat: %d!", stats["TotOpsSet"])
	}
	if stats["TotOpsDel"].(float64) != 0 {
		t.Errorf("[Footer1] Unexpected value for TotOpsDel stat: %d!", stats["TotOpsDel"])
	}

	cleanup(dir, store, coll)
}
