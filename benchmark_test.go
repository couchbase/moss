//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"fmt"
	"testing"
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
