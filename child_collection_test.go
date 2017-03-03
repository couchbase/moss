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
	"testing"
)

// ----------------------------------------------------
func loadItems(b Batch, startIdx, stopIdx int) {
	// put numItems in the batch
	for i := startIdx; i > stopIdx; i-- {
		k := fmt.Sprintf("%04d", i)
		b.Set([]byte(k), []byte(k))
	}
}

func verifySnapshot(msg string, ss Snapshot, expectedNum int,
	t *testing.T) {
	defer ss.Close()
	for i := 0; i < expectedNum; i++ {
		k := fmt.Sprintf("%04d", i)
		v, err := ss.Get([]byte(k), ReadOptions{})
		if err != nil {
			t.Errorf("error %s getting key: %s, %v", msg, k, err)
			return
		}
		if string(v) != k {
			t.Errorf("expected %s value for key: %s to be %s,got %s",
				msg, k, k, v)
			return
		}
	}

	iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
	if err != nil {
		t.Errorf("error %s verifySnapshot iter, err: %v", msg, err)
		return
	}

	defer iter.Close()
	n := 0
	var lastKey []byte
	for {
		ex, key, val, err := iter.CurrentEx()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			t.Errorf("error %s iter currentEx, err: %v", msg, err)
			return
		}

		n++

		if ex.Operation != OperationSet {
			t.Errorf("error %s iter op, ex: %v, err: %v", msg, ex, err)
			return
		}

		cmp := bytes.Compare(lastKey, key)
		if cmp >= 0 {
			t.Errorf("error %s iter cmp: %v, err: %v", msg, cmp, err)
			return
		}

		if bytes.Compare(key, val) != 0 {
			t.Errorf("error %s iter key != val: %v, %v", msg, key, val)
			return
		}

		lastKey = key

		err = iter.Next()
		if err == ErrIteratorDone {
			break
		}
		if err != nil {
			t.Errorf("error %s iter next, err: %v", msg, err)
			return
		}
	}

	if n != expectedNum {
		t.Errorf("error %s iter expectedNum: %d, got: %d",
			msg, expectedNum, n)
	}

}

func childCollectionLoader(m *collection, childName string,
	args *collTestParams, t *testing.T) {
	batchSize := args.batchSize
	for i := args.numIterations - 1; i >= 0; i-- {
		for j := args.numItems - 1; j >= 0; j = j - batchSize {
			// create new batch to set some keys
			b, err := m.NewBatch(0, 0)
			if err != nil {
				args.doneCh <- false
				t.Errorf("error creating new batch: %v", err)
				return
			}

			// also create a child batch
			childB, err := b.NewChildCollectionBatch(childName,
				BatchOptions{0, 0})
			if err != nil {
				args.doneCh <- false
				t.Errorf("error creating new child batch: %v", err)
				return
			}

			loadItems(b, j, j-batchSize)
			loadItems(childB, j, j-batchSize)

			err = m.ExecuteBatch(b, WriteOptions{})
			if err != nil {
				args.doneCh <- false
				t.Errorf("error executing batch: %v", err)
				return
			}

			// cleanup that batch
			err = b.Close()
			if err != nil {
				args.doneCh <- false
				t.Errorf("error closing batch: %v", err)
				return
			}
		}

		topSnap, err := m.Snapshot()
		if err != nil || topSnap == nil {
			args.doneCh <- false
			t.Errorf("error snapshoting: %v", err)
			return
		}
		childSnap, err := topSnap.ChildCollectionSnapshot(childName)
		if err != nil || childSnap == nil {
			args.doneCh <- false
			t.Errorf("error getting child snapshot: %v", err)
			return
		}
		go verifySnapshot(childName, childSnap, args.numItems, t)
		err = topSnap.Close()
		if err != nil {
			args.doneCh <- false
			t.Errorf("error closing snapshot: %v", err)
			return
		}
	}
	args.doneCh <- true
}

type collTestParams struct {
	numItems      int
	batchSize     int
	numIterations int
	numChildren   int
	doneCh        chan bool
}

func testChildCollections(t *testing.T, args *collTestParams) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
	defer os.RemoveAll(tmpDir)
	store, err := OpenStore(tmpDir, DefaultStoreOptions)
	if err != nil || store == nil {
		t.Fatalf("error opening store :%v", tmpDir)
	}

	coll, _ := NewCollection(DefaultCollectionOptions)
	if coll == nil {
		t.Fatalf("Unable to open new collection in store")
	}
	coll.Start()

	// Create a new child batch to validate basic child collection operations.
	// Same key inserted in different child collections with different values.
	theKey := []byte("sameKey")
	valOfBase := []byte("valOfBase")
	valOfChild := []byte("valOfChild")
	valOfChildb21 := []byte("valOfChildb21")
	valOfChildb22 := []byte("valOfChildb22")

	b, _ := coll.NewBatch(0, 0)
	b.Set(theKey, valOfBase)
	b2, _ := b.NewChildCollectionBatch("child", BatchOptions{0, 0})
	b2.Set(theKey, valOfChild)
	b21, _ := b2.NewChildCollectionBatch("b21", BatchOptions{0, 0})
	b21.Set(theKey, valOfChildb21)
	b22, _ := b2.NewChildCollectionBatch("b22", BatchOptions{0, 0})
	b22.Set(theKey, valOfChildb22)
	err = coll.ExecuteBatch(b, WriteOptions{})
	if err != nil {
		t.Fatalf("Unable to ExecuteBatch on collection %v", err)
	}

	ss, _ := coll.Snapshot()

	llss, err := store.Persist(ss, StorePersistOptions{})
	if err != nil || llss == nil {
		t.Fatalf("expected persist to work")
	}

	ss.Close()
	llss.Close()

	if store.Close() != nil {
		t.Fatalf("expected store close to work")
	}

	if coll.Close() != nil {
		t.Fatalf("Error closing child collection")
	}

	// Now reopen the persisted store & test restore child collections.
	store, coll, err = OpenStoreCollection(tmpDir, DefaultStoreOptions,
		StorePersistOptions{})
	if err != nil {
		t.Fatalf("error re-opening store :%v", tmpDir)
	}

	ss, _ = coll.Snapshot()
	v, err := ss.Get(theKey, ReadOptions{})
	if err != nil || !bytes.Equal(v, valOfBase) {
		t.Fatalf("Expected key %v : got %v, %v, but got err %v",
			string(theKey), string(v), string(valOfBase), err)
	}
	childSS, _ := ss.ChildCollectionSnapshot("child")
	if childSS == nil {
		t.Fatalf("child snapshot not restored after persist")
	}
	v, err = childSS.Get(theKey, ReadOptions{})
	if err != nil || !bytes.Equal(v, valOfChild) {
		t.Fatalf("Expected key %v : got %v, %v, but got err %v",
			string(theKey), string(v), string(valOfChild), err)
	}
	b21SS, _ := childSS.ChildCollectionSnapshot("b21")
	if b21SS == nil {
		t.Fatalf("child snapshot b21 not restored after persist")
	}
	v, err = b21SS.Get(theKey, ReadOptions{})
	if err != nil || !bytes.Equal(v, valOfChildb21) {
		t.Fatalf("Expected key %v : got %v, %v, but got err %v",
			string(theKey), string(v), string(valOfChildb21), err)
	}
	b21SS.Close()   // close grand child snapshot.
	childSS.Close() // each snapshot must be closed properly.
	ss.Close()      // top level close will not auto-close child snapshots.

	// ----------------------------------------------------
	// Now begin parallel collection load with child collections.

	args.doneCh = make(chan bool, args.numChildren)
	for i := 0; i < args.numChildren; i++ {
		name := fmt.Sprintf("child%v", i)
		m := coll.(*collection)
		go childCollectionLoader(m, name, args, t)
	}
	for i := 0; i < args.numChildren; i++ {
		<-args.doneCh
	}
	coll.Close()
}

func Test1Collection100items(t *testing.T) {
	args := &collTestParams{
		numItems:      100,
		batchSize:     10,
		numIterations: 1,
		numChildren:   1,
	}
	testChildCollections(t, args)
}

func Test2Collection1000items(t *testing.T) {
	args := &collTestParams{
		numItems:      1000,
		batchSize:     10,
		numIterations: 2,
		numChildren:   2,
	}
	testChildCollections(t, args)
}
