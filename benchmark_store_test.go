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
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type benchStoreSpec struct {
	numItems, keySize, valSize, batchSize int

	accesses []benchStoreSpecAccess
}

type benchStoreSpecAccess struct {
	domain    int
	ops       int
	batchSize int
}

func BenchmarkStore_numItems1M_keySize20_valSize100_batchSize100(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000, keySize: 20, valSize: 100, batchSize: 100,
	})
}

func BenchmarkStore_numItems1M_keySize20_valSize100_batchSize100_ACCESSES_domain100K_ops200K_batchSize100(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000, keySize: 20, valSize: 100, batchSize: 100,
		accesses: []benchStoreSpecAccess{
			benchStoreSpecAccess{domain: 100000, ops: 200000, batchSize: 100},
		},
	})
}

func BenchmarkStore_numItems1M_keySize20_valSize100_batchSize10000(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000, keySize: 20, valSize: 100, batchSize: 10000,
	})
}

func BenchmarkStore_numItems10M_keySize20_valSize100_batchSize1000(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 10000000, keySize: 20, valSize: 100, batchSize: 1000,
	})
}

func BenchmarkStore_numItems10M_keySize20_valSize100_batchSize10000(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 10000000, keySize: 20, valSize: 100, batchSize: 10000,
	})
}

func BenchmarkStore_numItems50M_keySize20_valSize100_batchSize10000(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 50000000, keySize: 20, valSize: 100, batchSize: 10000,
	})
}

func BenchmarkStore_numItems50M_keySize20_valSize100_batchSize10000_ACCESSES_domain100K_ops1M_batchSize10K(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 50000000, keySize: 20, valSize: 100, batchSize: 10000,
		accesses: []benchStoreSpecAccess{
			benchStoreSpecAccess{domain: 100000, ops: 1000000, batchSize: 10000},
		},
	})
}

func BenchmarkStore_numItems100M_keySize20_valSize100_batchSize10000(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 100000000, keySize: 20, valSize: 100, batchSize: 10000,
	})
}

func benchmarkStore(b *testing.B, spec benchStoreSpec) {
	bufSize := spec.valSize
	if bufSize < spec.keySize {
		bufSize = spec.keySize
	}
	buf := make([]byte, bufSize)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchmarkStoreDo(b, spec, buf)
	}
}

func benchmarkStoreDo(b *testing.B, spec benchStoreSpec, buf []byte) {
	tmpDir, _ := ioutil.TempDir("", "mossStoreBenchmark")
	defer os.RemoveAll(tmpDir)

	fmt.Printf("\n  spec: %+v\n", spec)

	var mu sync.Mutex
	counts := map[EventKind]int{}
	eventWaiters := map[EventKind]chan struct{}{}

	co := CollectionOptions{
		OnEvent: func(event Event) {
			mu.Lock()
			counts[event.Kind]++
			eventWaiter := eventWaiters[event.Kind]
			eventWaiters[event.Kind] = nil
			mu.Unlock()

			if eventWaiter != nil {
				close(eventWaiter)
			}
		},
	}

	so := StoreOptions{CollectionOptions: co}

	spo := StorePersistOptions{CompactionConcern: CompactionAllow}

	var store *Store

	var coll Collection

	var err error

	var cumMSecs int64
	var cumWMSecs int64
	var cumRMSecs int64

	var cumWOps int64
	var cumROps int64

	var cumWBytes int64
	var cumRBytes int64

	var phaseWOps int64
	var phaseROps int64

	var phaseRBytes int64
	var phaseWBytes int64

	phase := func(phaseName, phaseKind string, f func()) {
		phaseWOps = 0
		phaseROps = 0

		phaseWBytes = 0
		phaseRBytes = 0

		phaseBegTime := time.Now()
		f()
		phaseEndTime := time.Now()

		phaseMSecs := phaseEndTime.Sub(phaseBegTime).Nanoseconds() / 1000000.0

		cumMSecs += phaseMSecs
		if strings.Index(phaseKind, "w") >= 0 {
			cumWMSecs += phaseMSecs
		}
		if strings.Index(phaseKind, "r") >= 0 {
			cumRMSecs += phaseMSecs
		}

		cumWOps += phaseWOps
		cumROps += phaseROps

		cumWBytes += phaseWBytes
		cumRBytes += phaseRBytes

		var phaseWOpsPerSec int64
		var phaseROpsPerSec int64
		var phaseWKBPerSec int64
		var phaseRKBPerSec int64

		if phaseMSecs > 0 {
			phaseWOpsPerSec = 1000 * phaseWOps / phaseMSecs
			phaseROpsPerSec = 1000 * phaseROps / phaseMSecs
			phaseWKBPerSec = 1000 * phaseWBytes / phaseMSecs / 1024
			phaseRKBPerSec = 1000 * phaseRBytes / phaseMSecs / 1024
		}

		var cumWOpsPerSec int64
		var cumROpsPerSec int64

		var cumWKBPerSec int64
		var cumRKBPerSec int64

		if cumWMSecs > 0 {
			cumWOpsPerSec = 1000 * cumWOps / cumWMSecs
			cumWKBPerSec = 1000 * cumWBytes / cumWMSecs / 1024
		}
		if cumRMSecs > 0 {
			cumROpsPerSec = 1000 * cumROps / cumRMSecs
			cumRKBPerSec = 1000 * cumRBytes / cumRMSecs / 1024
		}

		fmt.Printf("   %s time: %8d (ms), phase %7d wop/s, %6d wkb/s, %7d rop/s, %6d rkb/s, cum %7d wop/s, %6d wkb/s, %7d rop/s, %6d rkb/s\n",
			phaseName,
			phaseMSecs,
			phaseWOpsPerSec,
			phaseWKBPerSec,
			phaseROpsPerSec,
			phaseRKBPerSec,
			cumWOpsPerSec,
			cumWKBPerSec,
			cumROpsPerSec,
			cumRKBPerSec)
	}

	// ------------------------------------------------

	phase("  open", "", func() {
		store, coll, err = OpenStoreCollection(tmpDir, so, spo)
		if err != nil {
			b.Fatal(err)
		}
	})

	// ------------------------------------------------

	phase("  load", "w", func() {
		batch, err := coll.NewBatch(spec.batchSize, spec.batchSize*(spec.keySize+spec.valSize))
		if err != nil {
			b.Fatal(err)
		}

		for i := 0; i < spec.numItems; i++ {
			binary.PutVarint(buf, int64(i))

			err = batch.Set(buf[0:spec.keySize], buf[0:spec.valSize])
			if err != nil {
				b.Fatal(err)
			}

			phaseWOps++
			phaseWBytes += int64(spec.keySize + spec.valSize)

			if i%spec.batchSize == 0 {
				err = coll.ExecuteBatch(batch, WriteOptions{})
				if err != nil {
					b.Fatal(err)
				}

				batch.Close()

				batch, err = coll.NewBatch(spec.batchSize, spec.batchSize*(spec.keySize+spec.valSize))
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	// ------------------------------------------------

	for _, access := range spec.accesses {
		phase("access", "w", func() {
			batch, err := coll.NewBatch(access.batchSize, access.batchSize*(spec.keySize+spec.valSize))
			if err != nil {
				b.Fatal(err)
			}

			for i, k := 0, 0; i < access.ops; i++ {
				if k > access.domain {
					k = 0
				}

				binary.PutVarint(buf, int64(k))

				err = batch.Set(buf[0:spec.keySize], buf[0:spec.valSize])
				if err != nil {
					b.Fatal(err)
				}

				phaseWOps++
				phaseWBytes += int64(spec.keySize + spec.valSize)

				if i%access.batchSize == 0 {
					err = coll.ExecuteBatch(batch, WriteOptions{})
					if err != nil {
						b.Fatal(err)
					}

					batch.Close()

					batch, err = coll.NewBatch(access.batchSize, access.batchSize*(spec.keySize+spec.valSize))
					if err != nil {
						b.Fatal(err)
					}
				}

				k++
			}
		})
	}

	// ------------------------------------------------

	phase(" drain", "w", func() {
		for {
			stats, err := coll.Stats()
			if err != nil {
				b.Fatal(b)
			}

			if stats.CurDirtyOps <= 0 &&
				stats.CurDirtyBytes <= 0 &&
				stats.CurDirtySegments <= 0 {
				return
			}

			persistenceProgressCh := make(chan struct{})

			mu.Lock()
			eventWaiters[EventKindPersisterProgress] = persistenceProgressCh
			mu.Unlock()

			select {
			case <-persistenceProgressCh:
				// NO-OP.
			case <-time.After(200 * time.Millisecond):
				// NO-OP.
			}
		}
	})

	// ------------------------------------------------

	phase(" close", "", func() {
		coll.Close()
		store.Close()
	})

	// ------------------------------------------------

	phase("reopen", "", func() {
		store, coll, err = OpenStoreCollection(tmpDir, so, spo)
		if err != nil {
			b.Fatal(err)
		}
	})

	// ------------------------------------------------

	phase("  iter", "r", func() {
		ss, err := coll.Snapshot()
		if err != nil {
			b.Fatal(err)
		}

		iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			b.Fatal(err)
		}

		for {
			k, v, err := iter.Current()
			if err == ErrIteratorDone {
				break
			}

			phaseROps++
			phaseRBytes += int64(len(k) + len(v))

			if len(k) <= len(v) {
				if !bytes.HasPrefix(v, k) {
					b.Fatalf("wrong iter bytes")
				}
			} else {
				if !bytes.HasPrefix(k, v) {
					b.Fatalf("wrong bytes iter")
				}
			}

			err = iter.Next()
			if err == ErrIteratorDone {
				break
			}
		}

		iter.Close()
		ss.Close()
	})

	// ------------------------------------------------

	phase(" close", "", func() {
		coll.Close()
		store.Close()
	})

	// ------------------------------------------------

	fmt.Printf("    total time: %8d (ms)\n", cumMSecs)

	fileInfos, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	if len(fileInfos) != 1 {
		b.Fatalf("expected just 1 file")
	}

	fileInfo := fileInfos[0]

	fmt.Printf("  file size: %d (mb), amplification: %.3f\n",
		fileInfo.Size()/1000000.0,
		float64(fileInfo.Size())/float64(int64(spec.numItems)*int64((spec.keySize+spec.valSize))))
}
