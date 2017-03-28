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

const LargePrime = int64(9576890767)

type benchStoreSpec struct {
	numItems, keySize, valSize, batchSize int

	randomLoad bool // When true, use repeatably random-like keys; otherwise, sequential load.

	noCopyValue bool

	accesses []benchStoreSpecAccess

	compactionPercentage float64 // when set to a non-zero value triggers compactions
}

type benchStoreSpecAccess struct {
	after      string // Run this access test after this given phase.
	kind       string // The kind of access: "w" (writes), "r" (reads), "" (neither).
	domainFrom int    // The domain of the test, from keys numbered domainFrom to domainTo.
	domainTo   int
	ops        int     // The number of ops for the access test.
	random     bool    // Whether to use repeatably random-like keys for the ops.
	pctGet     float32 // The pecentage of ops that should be GET's.
	batchSize  int
}

func BenchmarkStore_numItems1M_keySize20_valSize100_batchSize100(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000, keySize: 20, valSize: 100, batchSize: 100,
	})
}

func BenchmarkStore_numItems1M_keySize20_valSize100_batchSize100_randomLoad(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000, keySize: 20, valSize: 100, batchSize: 100, randomLoad: true,
	})
}

func BenchmarkStore_numItems1M_keySize20_valSize1000_batchSize100_randomLoad(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000, keySize: 20, valSize: 1000, batchSize: 100, randomLoad: true,
	})
}

func BenchmarkStore_numItems1M_keySize20_valSize100_batchSize100_ACCESSES_afterLoad_domainTo100K_ops200K_batchSize100(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000, keySize: 20, valSize: 100, batchSize: 100,
		noCopyValue: true,
		accesses: []benchStoreSpecAccess{
			{after: "load", kind: "w", domainTo: 100000, ops: 200000, random: true, batchSize: 100},
			{after: "iter", kind: "r", domainTo: 100000, ops: 200000, random: true, pctGet: 1.0},
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

func BenchmarkStore_numItems20M_keySize16_valSize0_batchSize10000_randomLoad(b *testing.B) { // Similar to Nitro VLDB test.
	benchmarkStore(b, benchStoreSpec{
		numItems: 20000000, keySize: 16, valSize: 0, batchSize: 10000, randomLoad: true,
		accesses: []benchStoreSpecAccess{
			{after: "iter", kind: "r", domainTo: 20000000, ops: 1000000, random: true, pctGet: 1.0},
		},
	})
}

func BenchmarkStore_numItems20M_keySize32_valSize0_batchSize10000_randomLoad(b *testing.B) { // Similar to Nitro VLDB test.
	benchmarkStore(b, benchStoreSpec{
		numItems: 20000000, keySize: 32, valSize: 0, batchSize: 10000, randomLoad: true,
		accesses: []benchStoreSpecAccess{
			{after: "iter", kind: "r", domainTo: 20000000, ops: 1000000, random: true, pctGet: 1.0},
		},
	})
}

func BenchmarkStore_numItems20M_keySize64_valSize0_batchSize10000_randomLoad(b *testing.B) { // Similar to Nitro VLDB test.
	benchmarkStore(b, benchStoreSpec{
		numItems: 20000000, keySize: 64, valSize: 0, batchSize: 10000, randomLoad: true,
		accesses: []benchStoreSpecAccess{
			{after: "iter", kind: "r", domainTo: 20000000, ops: 1000000, random: true, pctGet: 1.0},
		},
	})
}

func BenchmarkStore_numItems20M_keySize128_valSize0_batchSize10000_randomLoad(b *testing.B) { // Similar to Nitro VLDB test.
	benchmarkStore(b, benchStoreSpec{
		numItems: 20000000, keySize: 128, valSize: 0, batchSize: 10000, randomLoad: true,
		accesses: []benchStoreSpecAccess{
			{after: "iter", kind: "r", domainTo: 20000000, ops: 1000000, random: true, pctGet: 1.0},
		},
	})
}

func BenchmarkStore_numItems50M_keySize20_valSize100_batchSize10000(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 50000000, keySize: 20, valSize: 100, batchSize: 10000,
	})
}

func BenchmarkStore_numItems50M_keySize20_valSize100_batchSize10000_ACCESSES_domainTo100K_ops1M_batchSize10K(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 50000000, keySize: 20, valSize: 100, batchSize: 10000,
		accesses: []benchStoreSpecAccess{
			{after: "load", kind: "w", domainTo: 100000, ops: 1000000, batchSize: 10000},
		},
	})
}

func BenchmarkStore_numItems100M_keySize20_valSize100_batchSize10000(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 100000000, keySize: 20, valSize: 100, batchSize: 10000,
		accesses: []benchStoreSpecAccess{
			{after: "iter", kind: "r", domainTo: 20000000, ops: 1000000, random: true, pctGet: 1.0},
		},
	})
}

func BenchmarkStore_numItems1M_keySize20_valSize100_batchSize10_compact(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000, keySize: 20, valSize: 100, batchSize: 100,
		compactionPercentage: 0.2,
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

	fmt.Printf("\n")
	for i := 0; i < 180; i++ {
		fmt.Printf("-")
	}
	fmt.Printf("\nspec: %+v\n", spec)

	readOptions := ReadOptions{NoCopyValue: spec.noCopyValue}

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

	if spec.compactionPercentage > 0 {
		so.CompactionPercentage = spec.compactionPercentage
		so.CompactionSync = true
		spo.CompactionConcern = CompactionAllow
	}

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

	phaseDo := func(phaseName, phaseKind string, addToCumulative bool, f func()) {
		phaseWOps = 0
		phaseROps = 0

		phaseWBytes = 0
		phaseRBytes = 0

		phaseBegTime := time.Now()
		f()
		phaseEndTime := time.Now()

		phaseMSecs := phaseEndTime.Sub(phaseBegTime).Nanoseconds() / 1000000.0

		if addToCumulative {
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
		}

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

		fmt.Printf("   %6s || time: %5d (ms) | %8d wop/s | %8d wkb/s | %8d rop/s | %8d rkb/s"+
			" || cumulative: %8d wop/s | %8d wkb/s | %8d rop/s | %8d rkb/s\n",
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

	phase := func(phaseName, phaseKind string, f func()) {
		phaseDo(phaseName, phaseKind, true, f)

		for accessi, access := range spec.accesses {
			if access.after == phaseName {
				phaseDo("access", access.kind, false, func() {
					fmt.Printf("  <<access %d: %+v>>\n", accessi, access)

					var batch Batch
					batch, err = coll.NewBatch(access.batchSize, access.batchSize*(spec.keySize+spec.valSize))
					if err != nil {
						b.Fatal(err)
					}

					var ss Snapshot
					ss, err = coll.Snapshot()
					if err != nil {
						b.Fatal(err)
					}

					pos := int64(0)
					domainSize64 := int64(access.domainTo - access.domainFrom)

					for i := 0; i < access.ops; i++ {
						clearBuf(buf)
						binary.PutVarint(buf, pos+int64(access.domainFrom))

						if access.random {
							pos = pos + LargePrime
						} else {
							pos++
						}
						pos = pos % domainSize64

						if float32(i%100)/100.0 < access.pctGet {
							var v []byte
							v, err = ss.Get(buf[0:spec.keySize], readOptions)
							if err != nil {
								b.Fatal(err)
							}

							phaseROps++
							phaseRBytes += int64(spec.keySize + len(v))
						} else {
							err = batch.Set(buf[0:spec.keySize], buf[0:spec.valSize])
							if err != nil {
								b.Fatal(err)
							}

							phaseWOps++
							phaseWBytes += int64(spec.keySize + spec.valSize)

							if (i != 0) && (i%access.batchSize == 0) {
								err = coll.ExecuteBatch(batch, WriteOptions{})
								if err != nil {
									b.Fatal(err)
								}

								batch.Close()

								batch, err = coll.NewBatch(access.batchSize, access.batchSize*(spec.keySize+spec.valSize))
								if err != nil {
									b.Fatal(err)
								}

								ss.Close()

								ss, err = coll.Snapshot()
								if err != nil {
									b.Fatal(err)
								}
							}
						}
					}

					batch.Close()
					ss.Close()
				})
			}
		}
	}

	// ------------------------------------------------

	phase("open", "", func() {
		store, coll, err = OpenStoreCollection(tmpDir, so, spo)
		if err != nil {
			b.Fatal(err)
		}
	})

	// ------------------------------------------------

	phase("load", "w", func() {
		var batch Batch
		batch, err = coll.NewBatch(spec.batchSize, spec.batchSize*(spec.keySize+spec.valSize))
		if err != nil {
			b.Fatal(err)
		}

		pos := int64(0)
		numItems64 := int64(spec.numItems)

		for i := 0; i < spec.numItems; i++ {
			clearBuf(buf)
			binary.PutVarint(buf, pos)

			if spec.randomLoad {
				pos = (pos + LargePrime) % numItems64
			} else {
				pos++
			}

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

	phase("drain", "w", func() {
		for {
			var stats *CollectionStats
			stats, err = coll.Stats()
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

	phase("close", "", func() {
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

	phase("iter", "r", func() {
		var ss Snapshot
		ss, err = coll.Snapshot()
		if err != nil {
			b.Fatal(err)
		}

		var iter Iterator
		iter, err = ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			b.Fatal(err)
		}

		for {
			var k, v []byte
			k, v, err = iter.Current()
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

	phase("close", "", func() {
		coll.Close()
		store.Close()
	})

	// ------------------------------------------------

	fmt.Printf("total time: %d (ms)\n", cumMSecs)

	fileInfos, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	if len(fileInfos) != 1 {
		b.Fatalf("expected just 1 file")
	}

	fileInfo := fileInfos[0]

	fmt.Printf("file size: %d (MB), amplification: %.3f\n",
		fileInfo.Size()/1000000.0,
		float64(fileInfo.Size())/float64(int64(spec.numItems)*int64((spec.keySize+spec.valSize))))
}

func clearBuf(buf []byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
}
