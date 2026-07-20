//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

// Perf SPIKE (not used by moss core): a complete, correct prototype of a
// blocked + front-coded segment (RocksDB data-block style) measured
// head-to-head against moss's real *segment (with the in-memory sparse
// key index) on point Get, full-scan iteration, build cost, and space,
// across key distributions and value sizes.
//
// Format: entries are sorted and grouped into fixed-count blocks.  Each
// block starts a fresh front-coding "restart" (shared=0) and stores per
// entry: uvarint(sharedLen) uvarint(suffixLen) uvarint(valLen) suffix
// val.  A contiguous block index (first key + data offset per block) is
// binary-searched to find the block, then the block is linear-scanned,
// reconstructing keys from the shared prefixes.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

const bfcBlockEntries = 16

type bfcSegment struct {
	idxKeys   []byte   // Block first-keys, concatenated.
	idxKeyOff []uint32 // len nBlocks+1; block b's first key = idxKeys[off[b]:off[b+1]].
	idxData   []uint32 // len nBlocks; byte offset of block b in data.
	data      []byte   // Front-coded blocks, concatenated.
	n         int
	nBlocks   int
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func buildBFC(keys, vals [][]byte) *bfcSegment {
	s := &bfcSegment{n: len(keys)}
	var tmp [binary.MaxVarintLen64]byte
	putU := func(x int) {
		n := binary.PutUvarint(tmp[:], uint64(x))
		s.data = append(s.data, tmp[:n]...)
	}
	var prev []byte
	for i := range keys {
		if i%bfcBlockEntries == 0 {
			s.idxKeyOff = append(s.idxKeyOff, uint32(len(s.idxKeys)))
			s.idxKeys = append(s.idxKeys, keys[i]...)
			s.idxData = append(s.idxData, uint32(len(s.data)))
			prev = nil // Restart front-coding at each block start.
		}
		k, v := keys[i], vals[i]
		shared := 0
		if prev != nil {
			shared = commonPrefixLen(prev, k)
		}
		putU(shared)
		putU(len(k) - shared)
		putU(len(v))
		s.data = append(s.data, k[shared:]...)
		s.data = append(s.data, v...)
		prev = k
	}
	s.idxKeyOff = append(s.idxKeyOff, uint32(len(s.idxKeys)))
	s.nBlocks = len(s.idxData)
	return s
}

func (s *bfcSegment) size() int {
	return len(s.idxKeys) + 4*len(s.idxKeyOff) + 4*len(s.idxData) + len(s.data)
}

func (s *bfcSegment) blockFirstKey(b int) []byte {
	return s.idxKeys[s.idxKeyOff[b]:s.idxKeyOff[b+1]]
}

func (s *bfcSegment) blockEnd(b int) int {
	if b+1 < s.nBlocks {
		return int(s.idxData[b+1])
	}
	return len(s.data)
}

func (s *bfcSegment) Get(key []byte) ([]byte, bool) {
	// Block whose first key is the largest <= key.
	lo, hi := 0, s.nBlocks
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if bytes.Compare(s.blockFirstKey(m), key) <= 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	b := lo - 1
	if b < 0 {
		return nil, false
	}

	data := s.data
	pos := int(s.idxData[b])
	end := s.blockEnd(b)
	var scratch [256]byte
	for pos < end {
		shared, n1 := binary.Uvarint(data[pos:])
		pos += n1
		slen, n2 := binary.Uvarint(data[pos:])
		pos += n2
		vlen, n3 := binary.Uvarint(data[pos:])
		pos += n3
		copy(scratch[shared:], data[pos:pos+int(slen)])
		pos += int(slen)
		curLen := int(shared) + int(slen)
		valStart := pos
		pos += int(vlen)
		c := bytes.Compare(scratch[:curLen], key)
		if c == 0 {
			return data[valStart : valStart+int(vlen)], true
		}
		if c > 0 {
			return nil, false // Passed where key would be.
		}
	}
	return nil, false
}

// scan calls fn for every entry in key order (full-range iteration).
func (s *bfcSegment) scan(fn func(k, v []byte)) {
	data := s.data
	var scratch [256]byte
	for b := 0; b < s.nBlocks; b++ {
		pos := int(s.idxData[b])
		end := s.blockEnd(b)
		for pos < end {
			shared, n1 := binary.Uvarint(data[pos:])
			pos += n1
			slen, n2 := binary.Uvarint(data[pos:])
			pos += n2
			vlen, n3 := binary.Uvarint(data[pos:])
			pos += n3
			copy(scratch[shared:], data[pos:pos+int(slen)])
			pos += int(slen)
			curLen := int(shared) + int(slen)
			fn(scratch[:curLen], data[pos:pos+int(vlen)])
			pos += int(vlen)
		}
	}
}

// ---- fair moss baseline: a real *segment (with in-mem index) ----

func buildMossSeg(t testing.TB, keys, vals [][]byte) *segment {
	var tot int
	for i := range keys {
		tot += len(keys[i]) + len(vals[i])
	}
	b, err := newBatch(nil, BatchOptions{len(keys), tot})
	if err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		if err := b.Set(keys[i], vals[i]); err != nil {
			t.Fatal(err)
		}
	}
	prev := SkipStats
	SkipStats = true // b has a nil rootCollection; skip the stats goroutine.
	b.doSort()
	SkipStats = prev
	return b.segment
}

func spikeVals(n, valSize int) [][]byte {
	vals := make([][]byte, n)
	v := benchValue(valSize)
	for i := range vals {
		vals[i] = v // Shared backing is fine; segments copy on Set.
	}
	return vals
}

// ---- correctness ----

func TestSpikeBFCCorrect(t *testing.T) {
	for _, dist := range []string{"seq", "rand"} {
		keys := spikeKeys(3000, dist)
		vals := make([][]byte, len(keys))
		for i := range keys {
			vals[i] = []byte(fmt.Sprintf("v%d", i))
		}
		bfc := buildBFC(keys, vals)

		for i, k := range keys {
			v, ok := bfc.Get(k)
			if !ok || string(v) != string(vals[i]) {
				t.Fatalf("[%s] bfc.Get(%q)=%q,%v want %q", dist, k, v, ok, vals[i])
			}
		}
		if _, ok := bfc.Get([]byte("!!!before")); ok {
			t.Fatalf("[%s] expected miss (before)", dist)
		}
		if _, ok := bfc.Get([]byte("~~~after~~~~~~~~~~~~~~~~~")); ok {
			t.Fatalf("[%s] expected miss (after)", dist)
		}

		// Scan yields all entries in order.
		n := 0
		var last []byte
		bfc.scan(func(k, v []byte) {
			if last != nil && bytes.Compare(last, k) >= 0 {
				t.Fatalf("[%s] scan out of order at %d", dist, n)
			}
			last = append(last[:0], k...)
			n++
		})
		if n != len(keys) {
			t.Fatalf("[%s] scan saw %d want %d", dist, n, len(keys))
		}
	}
}

// TestSpikeBFCSizes reports space vs moss for both distributions and
// value sizes (run with -v).
func TestSpikeBFCSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy 1M-entry size report in -short")
	}
	const n = 1000000
	for _, valSize := range []int{0, 100} {
		for _, dist := range []string{"seq", "rand"} {
			keys := spikeKeys(n, dist)
			vals := spikeVals(n, valSize)
			bfc := buildBFC(keys, vals)
			mseg := buildMossSeg(t, keys, vals)
			mossBytes := len(mseg.kvs)*8 + len(mseg.buf)
			t.Logf("dist=%s valSize=%d  moss=%dMB  blockFC=%dMB  (%.1f%% of moss)",
				dist, valSize, mossBytes>>20, bfc.size()>>20,
				100*float64(bfc.size())/float64(mossBytes))
		}
	}
}

// ---- benchmarks ----

func BenchmarkSpikeBFC(b *testing.B) {
	const n = 1000000
	for _, dist := range []string{"seq", "rand"} {
		keys := spikeKeys(n, dist)
		vals := spikeVals(n, 100)
		bfc := buildBFC(keys, vals)
		mseg := buildMossSeg(b, keys, vals)

		b.Run(dist+"/get/moss", func(b *testing.B) {
			b.ReportAllocs()
			var sink []byte
			for i := 0; i < b.N; i++ {
				_, v, _ := mseg.Get(keys[benchStridedIndex(i, n)])
				sink = v
			}
			benchSink = sink
		})
		b.Run(dist+"/get/blockFC", func(b *testing.B) {
			b.ReportAllocs()
			var sink []byte
			for i := 0; i < b.N; i++ {
				v, _ := bfc.Get(keys[benchStridedIndex(i, n)])
				sink = v
			}
			benchSink = sink
		})

		b.Run(dist+"/scan/moss", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c, _ := mseg.Cursor(nil, nil)
				for {
					op, k, v := c.Current()
					if op == 0 && k == nil && v == nil {
						break
					}
					benchSink = k
					benchSink = v
					if c.Next() != nil {
						break
					}
				}
			}
		})
		b.Run(dist+"/scan/blockFC", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bfc.scan(func(k, v []byte) { benchSink = k; benchSink = v })
			}
		})
	}
}
