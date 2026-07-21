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
	"math/rand"
	"sort"
	"testing"
)

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

// This file is a self-contained perf SPIKE (not used by moss core) that
// compares in-memory key-search/layout strategies over identical sorted
// data, to quantify -- before touching moss's segment format -- how much
// the current "offset-array -> scattered key-buf" double indirection
// costs, and what a contiguous layout and a blocked + front-coded (a la
// RocksDB data blocks) layout would buy on speed AND space.
//
// It searches keys to an entry index (values are omitted); value
// retrieval is identical across designs, so this isolates the search.

const spikeKeySize = 24

// spikeKeys returns n sorted, distinct spikeKeySize-byte keys.
//   - "seq":  zero-padded counters -> long shared prefixes (front-coding
//     friendly, prefix-fingerprint hostile: leading bytes are identical).
//   - "rand": random letters -> little prefix sharing (the opposite).
func spikeKeys(n int, dist string) [][]byte {
	keys := make([][]byte, n)
	switch dist {
	case "seq":
		for i := 0; i < n; i++ {
			keys[i] = []byte(fmt.Sprintf("%0*d", spikeKeySize, i))
		}
	case "rand":
		r := rand.New(rand.NewSource(1))
		seen := make(map[string]struct{}, n)
		for i := 0; i < n; {
			b := make([]byte, spikeKeySize)
			for j := range b {
				b[j] = byte('a' + r.Intn(26))
			}
			if _, ok := seen[string(b)]; ok {
				continue
			}
			seen[string(b)] = struct{}{}
			keys[i] = b
			i++
		}
		sort.Slice(keys, func(a, b int) bool { return bytes.Compare(keys[a], keys[b]) < 0 })
	default:
		panic("unknown dist")
	}
	return keys
}

// ---- Variant 1: baseline (moss-like offset array + key buf) ----

type segBaseline struct {
	off []uint32 // len n+1; key i is buf[off[i]:off[i+1]].
	buf []byte
	n   int
}

func buildBaseline(keys [][]byte) *segBaseline {
	s := &segBaseline{n: len(keys), off: make([]uint32, len(keys)+1)}
	for i, k := range keys {
		s.off[i] = uint32(len(s.buf))
		s.buf = append(s.buf, k...)
	}
	s.off[len(keys)] = uint32(len(s.buf))
	return s
}

func (s *segBaseline) size() int { return len(s.buf) + 4*len(s.off) }

func (s *segBaseline) Get(key []byte) int {
	i, j := 0, s.n
	for i < j {
		m := int(uint(i+j) >> 1)
		c := bytes.Compare(s.buf[s.off[m]:s.off[m+1]], key)
		if c == 0 {
			return m
		} else if c < 0 {
			i = m + 1
		} else {
			j = m
		}
	}
	return -1
}

// ---- Variant 2: contiguous fixed-width keys (no indirection) ----

type segContig struct {
	buf  []byte // n * klen, keys back-to-back in sorted order.
	n    int
	klen int
}

func buildContig(keys [][]byte) *segContig {
	klen := len(keys[0])
	s := &segContig{n: len(keys), klen: klen, buf: make([]byte, 0, len(keys)*klen)}
	for _, k := range keys {
		s.buf = append(s.buf, k...)
	}
	return s
}

func (s *segContig) size() int { return len(s.buf) }

func (s *segContig) Get(key []byte) int {
	i, j := 0, s.n
	for i < j {
		m := int(uint(i+j) >> 1)
		c := bytes.Compare(s.buf[m*s.klen:(m+1)*s.klen], key)
		if c == 0 {
			return m
		} else if c < 0 {
			i = m + 1
		} else {
			j = m
		}
	}
	return -1
}

// ---- Variant 3: blocked + front-coded + intra-block linear scan ----

const spikeBlockSize = 16

type segBlockFC struct {
	firstKeys []byte // nBlocks * klen: first key of each block (block index).
	klen      int
	blockData [][]byte // Per-block front-coded entries.
	nBlocks   int
	n         int
}

func buildBlockFC(keys [][]byte) *segBlockFC {
	klen := len(keys[0])
	s := &segBlockFC{klen: klen, n: len(keys)}
	var tmp [64]byte
	for start := 0; start < len(keys); start += spikeBlockSize {
		end := start + spikeBlockSize
		if end > len(keys) {
			end = len(keys)
		}
		s.firstKeys = append(s.firstKeys, keys[start]...)
		var data []byte
		var prev []byte
		for i := start; i < end; i++ {
			k := keys[i]
			shared := 0
			if prev != nil {
				for shared < len(prev) && shared < len(k) && prev[shared] == k[shared] {
					shared++
				}
			}
			suffix := k[shared:]
			n := binary.PutUvarint(tmp[:], uint64(shared))
			data = append(data, tmp[:n]...)
			n = binary.PutUvarint(tmp[:], uint64(len(suffix)))
			data = append(data, tmp[:n]...)
			data = append(data, suffix...)
			prev = k
		}
		s.blockData = append(s.blockData, data)
	}
	s.nBlocks = len(s.blockData)
	return s
}

func (s *segBlockFC) size() int {
	total := len(s.firstKeys)
	for _, d := range s.blockData {
		total += len(d)
	}
	return total
}

func (s *segBlockFC) Get(key []byte) int {
	// Binary search the block index for the last block whose first key
	// is <= key.
	lo, hi := 0, s.nBlocks
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if bytes.Compare(s.firstKeys[m*s.klen:(m+1)*s.klen], key) <= 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	block := lo - 1
	if block < 0 {
		return -1 // key precedes the first key.
	}

	// Linear scan the block, reconstructing front-coded keys.
	data := s.blockData[block]
	var scratch [64]byte
	curLen := 0
	pos := 0
	idx := 0
	for pos < len(data) {
		shared, n1 := binary.Uvarint(data[pos:])
		pos += n1
		slen, n2 := binary.Uvarint(data[pos:])
		pos += n2
		copy(scratch[shared:], data[pos:pos+int(slen)])
		pos += int(slen)
		curLen = int(shared) + int(slen)
		cur := scratch[:curLen]
		c := bytes.Compare(cur, key)
		if c == 0 {
			return block*spikeBlockSize + idx
		}
		if c > 0 {
			return -1 // Passed where key would be; not present.
		}
		idx++
	}
	return -1
}

// ---- Variant 4: eytzinger (BFS) layout of contiguous fixed keys ----
// Tests whether a cache-friendly / prefetchable *probe order* (not just
// layout) helps: the sorted keys are stored in implicit-heap (BFS)
// order, so early probes cluster near the front of the array.

type segEyt struct {
	buf      []byte // 1-indexed heap: entry i (1..n) at buf[(i-1)*klen:i*klen].
	toSorted []int  // toSorted[i] = original sorted rank of heap entry i.
	n        int
	klen     int
}

func buildEyt(keys [][]byte) *segEyt {
	klen := len(keys[0])
	n := len(keys)
	s := &segEyt{n: n, klen: klen, buf: make([]byte, n*klen), toSorted: make([]int, n+1)}
	idx := 0
	var rec func(i int)
	rec = func(i int) {
		if i > n {
			return
		}
		rec(2 * i)
		copy(s.buf[(i-1)*klen:], keys[idx])
		s.toSorted[i] = idx
		idx++
		rec(2*i + 1)
	}
	rec(1)
	return s
}

func (s *segEyt) size() int { return len(s.buf) + 8*len(s.toSorted) }

func (s *segEyt) Get(key []byte) int {
	i := 1
	for i <= s.n {
		c := bytes.Compare(s.buf[(i-1)*s.klen:i*s.klen], key)
		if c == 0 {
			return s.toSorted[i]
		} else if c < 0 {
			i = 2*i + 1
		} else {
			i = 2 * i
		}
	}
	return -1
}

// ---- correctness (guards the hand-rolled searches) ----

func TestSpikeSearchVariantsCorrect(t *testing.T) {
	for _, dist := range []string{"seq", "rand"} {
		keys := spikeKeys(2000, dist)
		base := buildBaseline(keys)
		cont := buildContig(keys)
		blk := buildBlockFC(keys)
		eyt := buildEyt(keys)

		for i, k := range keys {
			if got := base.Get(k); got != i {
				t.Fatalf("[%s] baseline Get(%q)=%d want %d", dist, k, got, i)
			}
			if got := cont.Get(k); got != i {
				t.Fatalf("[%s] contig Get(%q)=%d want %d", dist, k, got, i)
			}
			if got := blk.Get(k); got != i {
				t.Fatalf("[%s] blockFC Get(%q)=%d want %d", dist, k, got, i)
			}
			if got := eyt.Get(k); got != i {
				t.Fatalf("[%s] eyt Get(%q)=%d want %d", dist, k, got, i)
			}
		}
		// Misses.
		for _, miss := range [][]byte{
			[]byte("!!!!!!!!!!!!!!!!!!!!!!!!"), // before all
			[]byte("~~~~~~~~~~~~~~~~~~~~~~~~"), // after all
		} {
			if got := base.Get(miss); got != -1 {
				t.Fatalf("[%s] baseline miss got %d", dist, got)
			}
			if got := blk.Get(miss); got != -1 {
				t.Fatalf("[%s] blockFC miss got %d", dist, got)
			}
		}
	}
}

// TestSpikeSearchSizes reports the space each layout uses (run with -v).
func TestSpikeSearchSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy 1M-entry size report in -short")
	}
	const n = 1000000
	for _, dist := range []string{"seq", "rand"} {
		keys := spikeKeys(n, dist)
		base := buildBaseline(keys)
		cont := buildContig(keys)
		blk := buildBlockFC(keys)
		t.Logf("dist=%s n=%d  baseline=%dMB  contig=%dMB  blockFC=%dMB (%.1f bytes/key)",
			dist, n,
			base.size()>>20, cont.size()>>20, blk.size()>>20,
			float64(blk.size())/float64(n))
	}
}

// ---- benchmarks ----

func benchSpikeGet(b *testing.B, dist string, get func([]byte) int, keys [][]byte) {
	b.ReportAllocs()
	b.ResetTimer()
	idx := 0
	for i := 0; i < b.N; i++ {
		k := keys[benchStridedIndex(i, len(keys))]
		if get(k) < 0 {
			b.Fatal("unexpected miss")
		}
		idx++
	}
	_ = idx
}

func BenchmarkSpikeSearch(b *testing.B) {
	const n = 1000000
	for _, dist := range []string{"seq", "rand"} {
		keys := spikeKeys(n, dist)
		base := buildBaseline(keys)
		cont := buildContig(keys)
		blk := buildBlockFC(keys)

		eyt := buildEyt(keys)

		b.Run(dist+"/baseline", func(b *testing.B) { benchSpikeGet(b, dist, base.Get, keys) })
		b.Run(dist+"/contig", func(b *testing.B) { benchSpikeGet(b, dist, cont.Get, keys) })
		b.Run(dist+"/blockFC", func(b *testing.B) { benchSpikeGet(b, dist, blk.Get, keys) })
		b.Run(dist+"/eytzinger", func(b *testing.B) { benchSpikeGet(b, dist, eyt.Get, keys) })
	}
}
