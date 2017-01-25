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
	"io/ioutil"
	"os"
	"testing"

	"github.com/couchbase/moss"
	"github.com/couchbase/moss/cmd/mossScope/cmd"
)

func importHelper(t *testing.T, batchsize int) {
	// Create a JSON file with some sample content
	json_text := "[\n  { \"K\" : \"key0\", \"V\" : \"val0\" },\n" +
			"  { \"K\" : \"key1\", \"V\" : \"val1\" },\n" +
			"  { \"K\" : \"key2\", \"V\" : \"val2\" },\n" +
			"  { \"K\" : \"key3\", \"V\" : \"val3\" },\n" +
			"  { \"K\" : \"key4\", \"V\" : \"val4\" },\n" +
			"  { \"K\" : \"key5\", \"V\" : \"val5\" },\n" +
			"  { \"K\" : \"key6\", \"V\" : \"val6\" },\n" +
			"  { \"K\" : \"key7\", \"V\" : \"val7\" },\n" +
			"  { \"K\" : \"key8\", \"V\" : \"val8\" },\n" +
			"  { \"K\" : \"key9\", \"V\" : \"val9\" }\n]";

	temp_file := "import_test_temp.json"
	err := ioutil.WriteFile(temp_file, []byte(json_text), 0777)
	if err != nil {
		fmt.Printf("Failed to create file %s, err: %v\n", temp_file, err)
		os.Exit(-1)
	}
	defer os.Remove(temp_file)

	temp_dir := "import_test_dir"

	// Prevent the command from writing anything to stdout
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmd.ImportDocs(temp_file, temp_dir, batchsize)

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
	<-outC

	defer os.RemoveAll(temp_dir)

	store, err := moss.OpenStore(temp_dir, moss.StoreOptions{})
	if err != nil || store == nil {
		t.Errorf("Expected OpenStore() to work!")
	}
	defer store.Close()

	snapshot, _ := store.Snapshot()
	defer snapshot.Close()

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("val%d", i)
		val, err := snapshot.Get([]byte(k), moss.ReadOptions{})
		if err != nil {
			t.Errorf("Expected Snapshot-Get() to succeed!")
		}

		if len(val) == len(v) {
			for j := range v {
				if val[j] != v[j] {
					t.Errorf("Value mismatch!")
				}
			}
		} else {
			t.Errorf("Value length mismatch!")
		}
	}
}

func TestImport(t *testing.T) {
	importHelper(t, 0)
}

func TestImportWithBatchSize(t *testing.T) {
	importHelper(t, 3)
}
