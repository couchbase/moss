//  Copyright (c) 2017 Couchbase, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
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
	"sort"
	"testing"
	"time"
)

func TestSegmentKeysIndex(t *testing.T) {
	// Consider a moss segment with this data:
	// Buf: |key1|val1|key10|val10|key100|val100|key1000|val1000
	//      |key250|val250|key4000|val4000|key500|val500
	// Kvs: |4|4|0    |5|5|8      |6|6|18       |7|7|30
	//      |7|7|44       |7|7|56         |6|6|70

	keys := []string{
		"key1",
		"key10",
		"key100",
		"key1000",
		"key250",
		"key4000",
		"key500",
	}

	s := newSegmentKeysIndex(
		30, // quota
		7,  // number of keys
		6,  // average key size
	)

	for i, k := range keys {
		ret := s.add(i, []byte(k))
		if !ret {
			t.Errorf("Unexpected Add failure!")
		}
	}

	ret := s.add(0, []byte("key"))
	if ret {
		t.Errorf("Space shouldn't have been available!")
	}

	if s.numIndexableKeys != 3 {
		t.Errorf("Unexpected number of keys (%v) indexed!", s.numIndexableKeys)
	}

	data := []byte("key1key1000key500")

	if !bytes.Contains(s.data, data) {
		t.Errorf("Unexpected data in index array: %v", string(s.data))
	}

	if s.numKeys != 3 ||
		s.offsets[0] != 0 || s.offsets[1] != 4 || s.offsets[2] != 11 {
		t.Errorf("Unexpected content in offsets array!")
	}

	if s.hop != 3 {
		t.Errorf("Unexpected hop: %v!", s.hop)
	}

	var left, right int

	left, right = s.lookup([]byte("key1000"))
	if left != 3 || right != 4 {
		t.Errorf("Unexpected results for key1000")
	}

	left, right = s.lookup([]byte("key1"))
	if left != 0 || right != 1 {
		t.Errorf("Unexpected results for key1")
	}

	left, right = s.lookup([]byte("key500"))
	if left != 6 || right != 7 {
		t.Errorf("Unexpected results for key500")
	}

	left, right = s.lookup([]byte("key400"))
	if left != 3 || right != 6 {
		t.Errorf("Unexpected results for key4000")
	}

	left, right = s.lookup([]byte("key100"))
	if left != 0 || right != 3 {
		t.Errorf("Unexpected results for key100")
	}

	left, right = s.lookup([]byte("key0"))
	if left != 0 || right != 0 {
		t.Errorf("Unexpected results for key0")
	}

	left, right = s.lookup([]byte("key6"))
	if left != 6 || right != 7 {
		t.Errorf("Unexpected results for key6")
	}
}

type byNS []time.Duration

func (x byNS) Len() int           { return len(x) }
func (x byNS) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
func (x byNS) Less(i, j int) bool { return x[i].Nanoseconds() < x[j].Nanoseconds() }

type testResults struct {
	name       string
	batchsize  int
	writetime  time.Duration
	readtime   time.Duration
	fetchtimes []time.Duration
	mean       time.Duration
}

func TestSegmentKindBasicWithAndWithoutIndex(t *testing.T) {
	numItems := 1000000

	ch := make(chan *testResults)

	runTest := func(name string, batchSize, quota, minSize int) {
		tmpDir, _ := ioutil.TempDir("", "mossStore")
		defer os.RemoveAll(tmpDir)

		start := time.Now()

		so := DefaultStoreOptions
		so.CompactionSync = true
		so.SegmentKeysIndexMaxBytes = quota
		so.SegmentKeysIndexMinKeyBytes = minSize
		spo := StorePersistOptions{CompactionConcern: CompactionAllow}

		store, coll, er := OpenStoreCollection(tmpDir, so, spo)
		if er != nil || store == nil || coll == nil {
			t.Fatalf("error opening store collection: %v", tmpDir)
		}

		x := 0
		for i := 0; i < numItems; i = i + batchSize {
			ba, err := coll.NewBatch(batchSize, batchSize*512)
			if err != nil {
				t.Fatalf("error creating new batch: %v", err)
			}

			for j := i + batchSize - 1; j >= i; j-- {
				k := fmt.Sprintf("%08d", x)
				v := fmt.Sprintf("%128d", x)
				ba.Set([]byte(k), []byte(v))
				x++
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

		writetime := time.Since(start)

		fetchtimes := make([]time.Duration, numItems/10)
		var aggregate int64

		start = time.Now()

		for i := 0; i < numItems/10; i++ {
			gstart := time.Now()
			val, err := coll.Get([]byte(fmt.Sprintf("%08d", i*10)), ReadOptions{})
			fetchtimes[i] = time.Since(gstart)
			expect := fmt.Sprintf("%128d", i*10)
			if err != nil || string(val) != expect {
				t.Fatalf("Unexpected error: %v / Vals mismatch: '%v' != '%v'",
					err, string(val), expect)
			}
			aggregate += fetchtimes[i].Nanoseconds()
		}

		readtime := time.Since(start)

		sort.Sort(byNS(fetchtimes))

		mean := time.Duration(aggregate/int64(len(fetchtimes))) * time.Nanosecond

		ch <- &testResults{
			name:       name,
			batchsize:  batchSize,
			writetime:  writetime,
			readtime:   readtime,
			fetchtimes: fetchtimes,
			mean:       mean,
		}
	}

	go runTest("WITHOUT SEG INDEX", 100, 0, 0)
	go runTest("WITH SEG INDEX", 100, 100000, 1000000)
	go runTest("WITHOUT SEG INDEX", 10000, 0, 0)
	go runTest("WITH SEG INDEX", 10000, 100000, 1000000)

	var results []*testResults
	for i := 0; i < 4; i++ {
		results = append(results, <-ch)
	}

	fmt.Printf("%17v (numItems: %10v) %24v %v\n",
		" ", numItems,
		" ", "<-------------------- Fetch times -------------------->")
	fmt.Printf("%17v  %9v  %15v  %14v  %10v  %10v  %10v  %10v  %10v\n",
		" ", "BatchSize", "Writetime(s)", "Readtime(s)",
		"Mean", "Median", "90th", "95th", "99th")
	for _, result := range results {
		fmt.Printf("%17v  %9v  %15.3v  %14.3v  %10v  %10v  %10v  %10v  %10v\n",
			result.name,
			result.batchsize,
			result.writetime.Seconds(),
			result.readtime.Seconds(),
			result.mean,
			result.fetchtimes[len(result.fetchtimes)/2],
			result.fetchtimes[90*len(result.fetchtimes)/100],
			result.fetchtimes[95*len(result.fetchtimes)/100],
			result.fetchtimes[99*len(result.fetchtimes)/100])
	}
}
