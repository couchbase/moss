//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkCollectionSnapshotGets(b *testing.B) {
	tmpDir, _ := ioutil.TempDir("", "benchStore")
	defer os.RemoveAll(tmpDir)

	store, coll, keys := createStoreAndWriteNItems(tmpDir, 10000, 100)
	defer store.Close()
	defer coll.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ss, err := coll.Snapshot()
		if err != nil || ss == nil {
			panic("Snapshot() failed!")
		}

		_, err = ss.Get(keys[i%len(keys)], ReadOptions{})
		if err != nil {
			panic("Snapshot-Get() failed!")
		}

		ss.Close()
	}
}

func BenchmarkCollectionGets(b *testing.B) {
	tmpDir, _ := ioutil.TempDir("", "benchStore")
	defer os.RemoveAll(tmpDir)

	store, coll, keys := createStoreAndWriteNItems(tmpDir, 10000, 100)
	defer store.Close()
	defer coll.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := coll.Get(keys[i%len(keys)], ReadOptions{})
		if err != nil {
			panic("Collection-Get() failed!")
		}
	}
}

// ---------------------------------------------------------------

func createStoreAndWriteNItems(tmpDir string, items int,
	batches int) (s *Store, c Collection, ks [][]byte) {

	store, coll, err := OpenStoreCollection(tmpDir,
		StoreOptions{},
		StorePersistOptions{})

	if err != nil || store == nil {
		panic("OpenStoreCollection() failed!")
	}

	keys := make([][]byte, items)

	if batches > items {
		batches = 1
	}
	itemsPerBatch := items / batches
	itemCount := 0

	for i := 0; i < batches; i++ {
		if itemsPerBatch > items-itemCount {
			itemsPerBatch = items - itemCount
		}

		if itemsPerBatch <= 0 {
			break
		}

		batch, err := coll.NewBatch(itemsPerBatch, itemsPerBatch*20)
		if err != nil {
			panic("NewBatch() failed!")
		}

		for j := 0; j < itemsPerBatch; j++ {
			k := []byte(fmt.Sprintf("key%d", i))
			v := []byte(fmt.Sprintf("val%d", i))
			itemCount++

			batch.Set(k, v)
			keys[j] = k
		}

		err = coll.ExecuteBatch(batch, WriteOptions{})
		if err != nil {
			panic("ExecuteBatch() failed!")
		}
	}

	return store, coll, keys
}
