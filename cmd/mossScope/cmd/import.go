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
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sync"
	"encoding/json"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Imports the docs from the JSON file into the store",
	Long: `Imports the key-values from the specified file (required to be in
JSON format - array of maps), taking into account the batch size
specified by a flag, into the store. For example:
	./mossScope import <path_to_json_file> <path_to_store> [flag]
Expected JSON file format:
	[ {"K" : "key0", "V" : "val0"}, {"K" : "key1", "V" : "val1"} ]`,
	Run: func(cmd *cobra.Command, args []string) {
        if len(args) != 2 {
            fmt.Println("USAGE: mossScope import <path_to_json_file> <path_to_store> [flag], more details with --help");
            return
        }

        importDocs(args[0], args[1])
	},
}

var batchSize int

type KV struct {
	KEY		string	`json:"K"`
	VAL		string	`json:"V"`
}

func importDocs(file string, dir string) {
	input, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Printf("File read error: %v\n", err)
		os.Exit(-1)
	}

	var data []KV
	err = json.Unmarshal([]byte(input), &data)
	if err != nil {
		fmt.Printf("Invalid JSON format, err: %v\n", err)
		fmt.Println("Expected format:")
		fmt.Println("[\n {\"K\" : \"key0\", \"V\" : \"val0\"},\n {\"K\" : \"key1\", \"V\" : \"val1\"}\n]");
		os.Exit(-1)
	}

	if len(data) == 0 {
		fmt.Println("Empty JSON file, no key-values to load")
		return
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// Create the directory (specified) if it does not already exist
		os.Mkdir(dir, 0777)
	}

	var m sync.Mutex
	var waitingForCleanCh chan struct {}

	var store *moss.Store
	var coll moss.Collection

	co := moss.CollectionOptions{
		OnEvent: func(event moss.Event) {
			if event.Kind == moss.EventKindPersisterProgress {
				stats, err := coll.Stats()
				if err == nil &&
				stats.CurDirtyOps <= 0 && stats.CurDirtyBytes <= 0 && stats.CurDirtySegments <= 0 {
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
						moss.StoreOptions{CollectionOptions: co,},
						moss.StorePersistOptions{})
	if err != nil || store == nil {
		fmt.Printf("Moss-OpenStoreCollection failed, err: %v\n", err)
		os.Exit(-1)
	}

	defer store.Close()
	defer coll.Close()

	ch := make(chan struct{}, 1)

	numBatches := 1

	if batchSize <= 0 {
		// All key-values in a single batch

		sizeOfBatch := 0
		for i := 0; i < len(data); i++ {
			// Get the size of the batch
			sizeOfBatch += len(data[i].KEY) + len(data[i].VAL)
		}

		if sizeOfBatch == 0 {
			return
		}

		batch, err := coll.NewBatch(len(data), sizeOfBatch)
		if err != nil {
			fmt.Printf("Collection-NewBatch() failed, err: %v\n", err)
			os.Exit(-1)
		}

		for i := 0; i < len(data); i++ {
			kbuf, err := batch.Alloc(len(data[i].KEY))
			if err != nil {
				fmt.Printf("Batch-Alloc() failed, err: %v\n", err)
				os.Exit(-1)
			}
			vbuf, err := batch.Alloc(len(data[i].VAL))
			if err != nil {
				fmt.Printf("Batch-Alloc() failed, err: %v\n", err)
				os.Exit(-1)
			}

			copy(kbuf, data[i].KEY)
			copy(vbuf, data[i].VAL)

			err = batch.AllocSet(kbuf, vbuf)
			if err != nil {
				fmt.Printf("Batch-AllocSet() failed, err: %v\n", err)
				os.Exit(-1)
			}
		}

		m.Lock()
		waitingForCleanCh = ch
		m.Unlock()

		err = coll.ExecuteBatch(batch, moss.WriteOptions{})
		if err != nil {
			fmt.Printf("Collection-ExecuteBatch() failed, err: %v\n", err)
			os.Exit(-1)
		}

	} else {

		numBatches = int(math.Ceil(float64(len(data)) / float64(batchSize)))
		cursor := 0

		for i := 0; i < numBatches; i++ {
			sizeOfBatch := 0
			numItemsInBatch := 0
			for j := cursor; j < cursor + batchSize; j++ {
				if j >= len(data) {
					break
				}
				sizeOfBatch += len(data[j].KEY) + len(data[j].VAL)
				numItemsInBatch++
			}
			if sizeOfBatch == 0 {
				continue
			}

			batch, err := coll.NewBatch(numItemsInBatch, sizeOfBatch)
			if err != nil {
				fmt.Printf("Collection-NewBatch() failed, err: %v\n", err)
				os.Exit(-1)
			}

			for j := 0 ; j < numItemsInBatch; j++ {
				kbuf, err := batch.Alloc(len(data[cursor].KEY))
				if err != nil {
					fmt.Printf("Batch-Alloc() failed, err: %v\n", err)
					os.Exit(-1)
				}
				vbuf, err := batch.Alloc(len(data[cursor].VAL))
				if err != nil {
					fmt.Printf("Batch-Alloc() failed, err: %v\n", err)
					os.Exit(-1)
				}

				copy(kbuf, data[cursor].KEY)
				copy(vbuf, data[cursor].VAL)

				err = batch.AllocSet(kbuf, vbuf)
				if err != nil {
					fmt.Printf("Batch-AllocSet() failed, err: %v\n", err)
					os.Exit(-1)
				}
				cursor++
			}

			m.Lock()
			waitingForCleanCh = ch
			m.Unlock()

			err = coll.ExecuteBatch(batch, moss.WriteOptions{})
			if err != nil {
				fmt.Printf("Collection-ExecuteBatch() failed, err: %v\n", err)
				os.Exit(-1)
			}
		}
	}

	<-ch

	fmt.Printf("DONE! .. Wrote %d key-values, in %d batch(es)\n", len(data), numBatches)
}

// The following wrapper (public) is for test purposes
func ImportDocs(file string, dir string, batch int) {
	batchSize = batch
	importDocs(file, dir)
}

func init() {
	RootCmd.AddCommand(importCmd)

    // Local flag that is intended to work as a flag over import
    importCmd.Flags().IntVar(&batchSize, "batchsize", 0, "Batch-size for the set operations (default: all docs in one batch)")
}
