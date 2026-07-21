//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func BenchmarkSetsEmptyBatchSize100Asc(b *testing.B) {
	benchmarkSets(b, "empty", 100, "asc")
}

func BenchmarkSetsEmptyBatchSize1000Asc(b *testing.B) {
	benchmarkSets(b, "empty", 1000, "asc")
}

func BenchmarkSetsEmptyBatchSize10000Asc(b *testing.B) {
	benchmarkSets(b, "empty", 10000, "asc")
}

func BenchmarkSetsEmptyBatchSize100000Asc(b *testing.B) {
	benchmarkSets(b, "empty", 100000, "asc")
}

func BenchmarkSetsEmptyBatchSize100Dsc(b *testing.B) {
	benchmarkSets(b, "empty", 100, "desc")
}

func BenchmarkSetsEmptyBatchSize1000Dsc(b *testing.B) {
	benchmarkSets(b, "empty", 1000, "desc")
}

func BenchmarkSetsEmptyBatchSize10000Dsc(b *testing.B) {
	benchmarkSets(b, "empty", 10000, "desc")
}

func BenchmarkSetsEmptyBatchSize100000Dsc(b *testing.B) {
	benchmarkSets(b, "empty", 100000, "desc")
}

// ---------------------------------------------------------------

func BenchmarkSetsCumulativeBatchSize100Asc(b *testing.B) {
	benchmarkSets(b, "cumulative", 100, "asc")
}

func BenchmarkSetsCumulativeBatchSize1000Asc(b *testing.B) {
	benchmarkSets(b, "cumulative", 1000, "asc")
}

func BenchmarkSetsCumulativeBatchSize10000Asc(b *testing.B) {
	benchmarkSets(b, "cumulative", 10000, "asc")
}

func BenchmarkSetsCumulativeBatchSize100000Asc(b *testing.B) {
	benchmarkSets(b, "cumulative", 100000, "asc")
}

func BenchmarkSetsCumulativeBatchSize100Dsc(b *testing.B) {
	benchmarkSets(b, "cumulative", 100, "desc")
}

func BenchmarkSetsCumulativeBatchSize1000Dsc(b *testing.B) {
	benchmarkSets(b, "cumulative", 1000, "desc")
}

func BenchmarkSetsCumulativeBatchSize10000Dsc(b *testing.B) {
	benchmarkSets(b, "cumulative", 10000, "desc")
}

func BenchmarkSetsCumulativeBatchSize100000Dsc(b *testing.B) {
	benchmarkSets(b, "cumulative", 100000, "desc")
}

// ---------------------------------------------------------------

func BenchmarkSetsParallelBatchSize100Asc(b *testing.B) {
	benchmarkSetsParallel(b, 100, "asc")
}

func BenchmarkSetsParallelBatchSize1000Asc(b *testing.B) {
	benchmarkSetsParallel(b, 1000, "asc")
}

func BenchmarkSetsParallelBatchSize10000Asc(b *testing.B) {
	benchmarkSetsParallel(b, 10000, "asc")
}

func BenchmarkSetsParallelBatchSize100000Asc(b *testing.B) {
	benchmarkSetsParallel(b, 100000, "asc")
}

func BenchmarkSetsParallelBatchSize100Dsc(b *testing.B) {
	benchmarkSetsParallel(b, 100, "desc")
}

func BenchmarkSetsParallelBatchSize1000Dsc(b *testing.B) {
	benchmarkSetsParallel(b, 1000, "desc")
}

func BenchmarkSetsParallelBatchSize10000Dsc(b *testing.B) {
	benchmarkSetsParallel(b, 10000, "desc")
}

func BenchmarkSetsParallelBatchSize100000Dsc(b *testing.B) {
	benchmarkSetsParallel(b, 100000, "desc")
}

// ---------------------------------------------------------------

func makeArr(n int, kind string) (arr [][]byte, arrTotBytes int) {
	arr = make([][]byte, 0, n)

	if kind == "asc" {
		for i := 0; i < n; i++ {
			buf := []byte(fmt.Sprintf("%d", i))
			arr = append(arr, buf)
			arrTotBytes += len(buf)
		}
	} else if kind == "desc" {
		for i := n; i > 0; i-- {
			buf := []byte(fmt.Sprintf("%d", i))
			arr = append(arr, buf)
			arrTotBytes += len(buf)
		}
	} else {
		panic("unknown kind")
	}

	return arr, arrTotBytes
}

func benchmarkSets(b *testing.B, fillKind string, batchSize int, batchKind string) {
	arr, arrTotBytes := makeArr(batchSize, batchKind)

	writeOptions := WriteOptions{}

	b.ResetTimer()

	if fillKind == "empty" {
		for i := 0; i < b.N; i++ {
			m, _ := NewCollection(CollectionOptions{})
			m.Start()

			batch, _ := m.NewBatch(len(arr), arrTotBytes+arrTotBytes)
			for _, buf := range arr {
				batch.Set(buf, buf)
			}
			m.ExecuteBatch(batch, writeOptions)
			batch.Close()

			m.Close()
		}
	} else if fillKind == "cumulative" {
		m, _ := NewCollection(CollectionOptions{})
		m.Start()

		for i := 0; i < b.N; i++ {
			batch, _ := m.NewBatch(len(arr), arrTotBytes+arrTotBytes)
			for _, buf := range arr {
				batch.Set(buf, buf)
			}
			m.ExecuteBatch(batch, writeOptions)
			batch.Close()
		}

		m.Close()
	} else {
		panic("unknown fillKind")
	}
}

func benchmarkSetsParallel(b *testing.B, batchSize int, batchKind string) {
	arr, arrTotBytes := makeArr(batchSize, batchKind)

	writeOptions := WriteOptions{}

	m, _ := NewCollection(CollectionOptions{})
	m.Start()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			batch, _ := m.NewBatch(len(arr), arrTotBytes+arrTotBytes)
			for _, buf := range arr {
				batch.Set(buf, buf)
			}
			m.ExecuteBatch(batch, writeOptions)
			batch.Close()
		}
	})
}

// ---------------------------------------------------------------

func BenchmarkGetOperationKeyVal(b *testing.B) {
	s, _ := newBatch(nil, BatchOptions{100, 200})
	key := []byte("a")
	s.Set(key, []byte("A"))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get(key)
	}
}

func BenchmarkCollectionSnapshotGets(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "benchStore")
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
	tmpDir, _ := os.MkdirTemp("", "benchStore")
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

// This file holds a self-contained, quick-to-run benchmark suite that
// exercises moss's common workloads -- heavy reads (point + range),
// heavy writes, mixed read/write, and merge resolution -- across the
// in-memory, multi-segment, and persisted-store paths.  Every
// benchmark reports allocs (b.ReportAllocs) and throughput
// (b.SetBytes) so `go test -bench . -benchmem` surfaces per-op cost and
// hot-path allocations directly.
//
// Defaults are modest so the suite runs quickly and repeatably; scale
// via the consts below when doing dedicated perf work.

const (
	benchKeySize = 24
	benchValSize = 100
	benchN       = 50000 // Distinct items loaded per read/iterate bench.
)

// benchSink defeats dead-code elimination for read results.
var benchSink []byte

// benchKey returns a distinct, zero-padded (hence lexicographically
// ordered) key of keySize bytes for item i.
func benchKey(i, keySize int) []byte {
	s := strconv.Itoa(i)
	if len(s) >= keySize {
		return []byte(s)
	}
	k := make([]byte, keySize)
	pad := keySize - len(s)
	for j := 0; j < pad; j++ {
		k[j] = '0'
	}
	copy(k[pad:], s)
	return k
}

func benchValue(valSize int) []byte {
	v := make([]byte, valSize)
	for i := range v {
		v[i] = 'v'
	}
	return v
}

// benchLoadKeys writes n distinct items (unique keys) into m, in
// batches of batchSize, and returns the keys in index order.
func benchLoadKeys(tb testing.TB, m Collection, n, keySize, valSize, batchSize int) [][]byte {
	tb.Helper()
	keys := make([][]byte, n)
	val := benchValue(valSize)

	for start := 0; start < n; start += batchSize {
		end := min(start+batchSize, n)
		b, err := m.NewBatch(end-start, (end-start)*(keySize+valSize))
		if err != nil {
			tb.Fatalf("NewBatch: %v", err)
		}
		for i := start; i < end; i++ {
			k := benchKey(i, keySize)
			keys[i] = k
			if err := b.Set(k, val); err != nil {
				tb.Fatalf("Set: %v", err)
			}
		}
		if err := m.ExecuteBatch(b, WriteOptions{}); err != nil {
			tb.Fatalf("ExecuteBatch: %v", err)
		}
		b.Close()
	}
	return keys
}

// benchStartedInMemory returns a started in-memory collection.
func benchStartedInMemory(tb testing.TB, opts CollectionOptions) Collection {
	tb.Helper()
	m, err := NewCollection(opts)
	if err != nil {
		tb.Fatal(err)
	}
	if err := m.Start(); err != nil {
		tb.Fatal(err)
	}
	return m
}

// benchStridedIndex returns pseudo-random-but-repeatable indices in
// [0, n) so reads don't just hit a monotonic pattern.
func benchStridedIndex(i, n int) int {
	return int((int64(i) * LargePrime) % int64(n))
}

// ---------------------------------------------------------------
// Reads: point Get across in-memory (single & multi-segment) and store.

func BenchmarkReadPointHit(b *testing.B) {
	b.Run("inmem_single_segment", func(b *testing.B) {
		m := benchStartedInMemory(b, CollectionOptions{})
		defer m.Close()
		keys := benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN) // 1 batch.
		benchPointGets(b, m, keys, true, ReadOptions{})
	})

	b.Run("inmem_multi_segment", func(b *testing.B) {
		// Unstarted + high MaxPreMergerBatches keeps every batch as its
		// own segment (no merger coalescing), so reads heap-merge across
		// a tall stack -- the worst-case read path.  Not Close()'d: with
		// no merger goroutine running there's nothing to stop.
		const batches = 32
		m, err := NewCollection(CollectionOptions{MaxPreMergerBatches: batches + 8})
		if err != nil {
			b.Fatal(err)
		}
		keys := benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN/batches)
		benchPointGets(b, m, keys, true, ReadOptions{})
	})

	b.Run("store_mmap", func(b *testing.B) {
		m := benchReopenedStore(b) // All data persisted; reads hit mmap.
		keys := storeKeys()
		benchPointGets(b, m, keys, true, ReadOptions{})
	})

	// Same as store_mmap but NoCopyValue, to quantify the value-copy
	// allocation that Footer.Get() otherwise does per read.
	b.Run("store_mmap_nocopy", func(b *testing.B) {
		m := benchReopenedStore(b)
		keys := storeKeys()
		benchPointGets(b, m, keys, true, ReadOptions{NoCopyValue: true})
	})
}

func storeKeys() [][]byte {
	keys := make([][]byte, benchN)
	for i := range keys {
		keys[i] = benchKey(i, benchKeySize)
	}
	return keys
}

func BenchmarkReadPointMiss(b *testing.B) {
	m := benchStartedInMemory(b, CollectionOptions{})
	defer m.Close()
	benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN)

	// Keys outside the loaded domain -> always a miss.
	misses := make([][]byte, 4096)
	for i := range misses {
		misses[i] = benchKey(benchN+i, benchKeySize)
	}
	benchPointGets(b, m, misses, false, ReadOptions{})
}

func BenchmarkReadPointHitParallel(b *testing.B) {
	m := benchStartedInMemory(b, CollectionOptions{})
	defer m.Close()
	keys := benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN)

	b.ReportAllocs()
	b.SetBytes(int64(benchKeySize + benchValSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		var sink []byte
		for pb.Next() {
			v, err := m.Get(keys[benchStridedIndex(i, len(keys))], ReadOptions{})
			if err != nil {
				b.Error(err)
				return
			}
			sink = v
			i++
		}
		benchSink = sink
	})
}

// BenchmarkReadSnapshotGet isolates the Snapshot()+Get()+Close() cost
// (e.g. the reuse-cached-snapshot path) versus the direct Get above.
func BenchmarkReadSnapshotGet(b *testing.B) {
	m := benchStartedInMemory(b, CollectionOptions{})
	defer m.Close()
	keys := benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN)

	b.ReportAllocs()
	b.SetBytes(int64(benchKeySize + benchValSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ss, err := m.Snapshot()
		if err != nil {
			b.Fatal(err)
		}
		v, err := ss.Get(keys[benchStridedIndex(i, len(keys))], ReadOptions{})
		if err != nil {
			b.Fatal(err)
		}
		benchSink = v
		ss.Close()
	}
}

func benchPointGets(b *testing.B, m Collection, keys [][]byte, wantHit bool,
	readOptions ReadOptions) {
	b.ReportAllocs()
	b.SetBytes(int64(benchKeySize + benchValSize))
	b.ResetTimer()

	var sink []byte
	for i := 0; i < b.N; i++ {
		v, err := m.Get(keys[benchStridedIndex(i, len(keys))], readOptions)
		if err != nil {
			b.Fatal(err)
		}
		if wantHit && v == nil {
			b.Fatal("expected hit")
		}
		sink = v
	}
	benchSink = sink
}

// ---------------------------------------------------------------
// Iteration: full scans and range scans.

func BenchmarkIterateFullScan(b *testing.B) {
	b.Run("inmem_single_segment", func(b *testing.B) {
		m := benchStartedInMemory(b, CollectionOptions{})
		defer m.Close()
		benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN)
		benchFullScan(b, m)
	})

	b.Run("inmem_multi_segment", func(b *testing.B) {
		const batches = 32
		m, err := NewCollection(CollectionOptions{MaxPreMergerBatches: batches + 8})
		if err != nil {
			b.Fatal(err)
		}
		benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN/batches)
		benchFullScan(b, m)
	})

	b.Run("store_mmap", func(b *testing.B) {
		m := benchReopenedStore(b)
		benchFullScan(b, m)
	})
}

func benchFullScan(b *testing.B, m Collection) {
	b.ReportAllocs()
	b.SetBytes(int64(benchN * (benchKeySize + benchValSize)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ss, err := m.Snapshot()
		if err != nil {
			b.Fatal(err)
		}
		iter, err := ss.StartIterator(nil, nil, IteratorOptions{})
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for {
			k, v, err := iter.Current()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			benchSink = k
			benchSink = v
			n++
			if err := iter.Next(); err == ErrIteratorDone {
				break
			}
		}
		iter.Close()
		ss.Close()
		if n != benchN {
			b.Fatalf("scanned %d, want %d", n, benchN)
		}
	}
}

func BenchmarkIterateRangeScan(b *testing.B) {
	m := benchStartedInMemory(b, CollectionOptions{})
	defer m.Close()
	benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN)

	const rangeLen = 100
	b.ReportAllocs()
	b.SetBytes(int64(rangeLen * (benchKeySize + benchValSize)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		startIdx := benchStridedIndex(i, benchN-rangeLen)
		start := benchKey(startIdx, benchKeySize)
		end := benchKey(startIdx+rangeLen, benchKeySize)

		ss, err := m.Snapshot()
		if err != nil {
			b.Fatal(err)
		}
		iter, err := ss.StartIterator(start, end, IteratorOptions{})
		if err != nil {
			b.Fatal(err)
		}
		for {
			k, _, err := iter.Current()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			benchSink = k
			if err := iter.Next(); err == ErrIteratorDone {
				break
			}
		}
		iter.Close()
		ss.Close()
	}
}

// ---------------------------------------------------------------
// Writes: steady-state batch execution at varying batch sizes, and the
// Alloc()-API copy-avoidance path.

func BenchmarkWriteBatch(b *testing.B) {
	for _, batchSize := range []int{1, 100, 1000, 10000} {
		b.Run("batchSize="+strconv.Itoa(batchSize), func(b *testing.B) {
			m := benchStartedInMemory(b, CollectionOptions{})
			defer m.Close()
			val := benchValue(benchValSize)

			b.ReportAllocs()
			b.SetBytes(int64(batchSize * (benchKeySize + benchValSize)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				batch, err := m.NewBatch(batchSize, batchSize*(benchKeySize+benchValSize))
				if err != nil {
					b.Fatal(err)
				}
				base := i * batchSize
				for j := 0; j < batchSize; j++ {
					if err := batch.Set(benchKey(base+j, benchKeySize), val); err != nil {
						b.Fatal(err)
					}
				}
				if err := m.ExecuteBatch(batch, WriteOptions{}); err != nil {
					b.Fatal(err)
				}
				batch.Close()
			}
		})
	}
}

// BenchmarkWriteBatchAllocAPI exercises the pre-allocation path
// (Batch.Alloc + AllocSet), which lets callers serialize keys/vals
// directly into batch-owned memory to avoid an extra copy.
func BenchmarkWriteBatchAllocAPI(b *testing.B) {
	const batchSize = 1000
	m := benchStartedInMemory(b, CollectionOptions{})
	defer m.Close()

	b.ReportAllocs()
	b.SetBytes(int64(batchSize * (benchKeySize + benchValSize)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		batch, err := m.NewBatch(batchSize, batchSize*(benchKeySize+benchValSize))
		if err != nil {
			b.Fatal(err)
		}
		base := i * batchSize
		for j := 0; j < batchSize; j++ {
			buf, err := batch.Alloc(benchKeySize + benchValSize)
			if err != nil {
				b.Fatal(err)
			}
			k := benchKey(base+j, benchKeySize)
			copy(buf[:benchKeySize], k)
			for x := benchKeySize; x < len(buf); x++ {
				buf[x] = 'v'
			}
			if err := batch.AllocSet(buf[:benchKeySize], buf[benchKeySize:]); err != nil {
				b.Fatal(err)
			}
		}
		if err := m.ExecuteBatch(batch, WriteOptions{}); err != nil {
			b.Fatal(err)
		}
		batch.Close()
	}
}

// ---------------------------------------------------------------
// Mixed read/write concurrent workload.

func BenchmarkMixedReadWrite(b *testing.B) {
	// pctWrite out of 100 operations are writes; the rest are reads.
	for _, pctWrite := range []int{10, 50} {
		b.Run("pctWrite="+strconv.Itoa(pctWrite), func(b *testing.B) {
			m := benchStartedInMemory(b, CollectionOptions{})
			defer m.Close()
			keys := benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN/16)
			val := benchValue(benchValSize)

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				i := 0
				var sink []byte
				for pb.Next() {
					idx := benchStridedIndex(i, len(keys))
					if i%100 < pctWrite {
						batch, err := m.NewBatch(1, benchKeySize+benchValSize)
						if err != nil {
							b.Error(err)
							return
						}
						_ = batch.Set(keys[idx], val)
						if err := m.ExecuteBatch(batch, WriteOptions{}); err != nil {
							b.Error(err)
							return
						}
						batch.Close()
					} else {
						v, err := m.Get(keys[idx], ReadOptions{})
						if err != nil {
							b.Error(err)
							return
						}
						sink = v
					}
					i++
				}
				benchSink = sink
			})
		})
	}
}

// ---------------------------------------------------------------
// Merge resolution: Get on a key with a deep merge-operand chain,
// exercising the (lazy) resolveMerge path at varying chain depths.

func BenchmarkMergeResolveChain(b *testing.B) {
	for _, depth := range []int{8, 64, 256} {
		b.Run("depth="+strconv.Itoa(depth), func(b *testing.B) {
			// Unstarted + high MaxPreMergerBatches so each Merge stays in
			// its own segment (uncoalesced), forcing Get to walk and fold
			// the whole operand chain.
			m, err := NewCollection(CollectionOptions{
				MergeOperator:       &MergeOperatorStringAppend{Sep: ":"},
				MaxPreMergerBatches: depth + 8,
			})
			if err != nil {
				b.Fatal(err)
			}
			key := []byte("mergekey")

			// A base Set, then `depth` Merge operands, each its own batch.
			base, _ := m.NewBatch(1, 64)
			_ = base.Set(key, []byte("base"))
			if err := m.ExecuteBatch(base, WriteOptions{}); err != nil {
				b.Fatal(err)
			}
			base.Close()
			for d := 0; d < depth; d++ {
				bat, _ := m.NewBatch(1, 64)
				_ = bat.Merge(key, []byte("op"))
				if err := m.ExecuteBatch(bat, WriteOptions{}); err != nil {
					b.Fatal(err)
				}
				bat.Close()
			}

			b.ReportAllocs()
			b.ResetTimer()

			var sink []byte
			for i := 0; i < b.N; i++ {
				v, err := m.Get(key, ReadOptions{})
				if err != nil {
					b.Fatal(err)
				}
				sink = v
			}
			benchSink = sink
		})
	}
}

// ---------------------------------------------------------------
// Hot-path micro-benchmarks (allocation-focused).

// BenchmarkHotSegmentGet isolates a single-segment binary-search Get,
// the innermost read primitive.
func BenchmarkHotSegmentGet(b *testing.B) {
	m := benchStartedInMemory(b, CollectionOptions{})
	defer m.Close()
	keys := benchLoadKeys(b, m, benchN, benchKeySize, benchValSize, benchN)
	ss, err := m.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer ss.Close()
	seg := ss.(*segmentStack)

	b.ReportAllocs()
	b.ResetTimer()

	var sink []byte
	for i := 0; i < b.N; i++ {
		_, v, err := seg.a[0].Get(keys[benchStridedIndex(i, len(keys))])
		if err != nil {
			b.Fatal(err)
		}
		sink = v
	}
	benchSink = sink
}

// ---------------------------------------------------------------
// Shared store setup.

// benchReopenedStore loads benchN items into a store-backed collection,
// then closes and reopens it so all data lives in the persisted,
// mmap'd store and reads exercise the lower-level (Footer) path.
func benchReopenedStore(tb testing.TB) Collection {
	tb.Helper()
	tmpDir, err := os.MkdirTemp("", "benchMossStore")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { os.RemoveAll(tmpDir) })

	store, coll, err := OpenStoreCollection(tmpDir, DefaultStoreOptions,
		StorePersistOptions{CompactionConcern: CompactionAllow})
	if err != nil {
		tb.Fatal(err)
	}
	benchLoadKeys(tb, coll, benchN, benchKeySize, benchValSize, benchN/16)
	waitForPersistence(coll) // Ensure all items reach the store before reopen.
	coll.Close()
	store.Close()

	store2, coll2, err := OpenStoreCollection(tmpDir, DefaultStoreOptions,
		StorePersistOptions{CompactionConcern: CompactionAllow})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		coll2.Close()
		store2.Close()
	})
	return coll2
}

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
	tmpDir, _ := os.MkdirTemp("", "mossStoreBenchmark")
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

	fileInfos, err := os.ReadDir(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	if len(fileInfos) != 1 {
		b.Fatalf("expected just 1 file")
	}

	fileInfo, err := fileInfos[0].Info()
	if err != nil {
		b.Fatal(err)
	}

	fmt.Printf("file size: %d (MB), amplification: %.3f\n",
		fileInfo.Size()/1000000.0,
		float64(fileInfo.Size())/float64(int64(spec.numItems)*int64((spec.keySize+spec.valSize))))
}

func clearBuf(buf []byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
}
