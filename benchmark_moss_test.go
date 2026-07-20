//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"os"
	"strconv"
	"testing"
)

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
