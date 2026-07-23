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

// ============================================================================
// DeleteRange tombstone SPIKE (not used by moss core).
//
// Motivation (secondary-index use case, see DESIGN-ideas.md): dropping an
// index or reindexing a field means deleting a whole contiguous key range,
// e.g. every (fieldValue, docId) posting under one prefix.  Today that costs
// one point Del per key: a full range scan to enumerate the keys, then M
// tombstones that bloat the newest segment and are re-visited on every read
// and compaction until they finally shadow their M targets away.
//
// A DeleteRange tombstone encodes the whole [lo, hi) interval as ONE entry.
// It fits moss's grain with no change to the common read path:
//
//   * Encoding: maskOperation is a 4-bit nibble (segment.go); only
//     Set/Del/Merge (0x01/0x02/0x03) are used, so OperationDelRange = 0x04
//     is free.  A tombstone stores key=lo, val=hi (hi exclusive).
//   * Precedence: resolveMerge (segment_stack.go) already walks the stack
//     newest->oldest and treats a point Del as a nil merge base.  A range
//     tombstone behaves identically -- it just matches an INTERVAL rather
//     than one key -- so a covering tombstone at a higher level shadows a
//     lower Set, while a newer Set above the tombstone survives.
//   * Storage: kept as a small per-segment sorted+coalesced []keyRange
//     alongside the kvs array, so a segment with zero range tombstones pays
//     one len==0 branch per Get and the point-lookup binary search is
//     untouched.  Coverage is an O(log R) search over the (tiny) range list.
//   * Iteration (iterator.go): a cursor entering a covered span Seek()s
//     straight to hi instead of visiting -- and discarding -- every deleted
//     entry, turning an O(M) scan tax into O(log).
//
// This spike models exactly that over moss's REAL *segment for the data
// plane, and measures the three claimed wins (maintenance write cost, read
// cost, scan cost) head-to-head against the point-Del status quo on the
// same data.  The point-Del scan baseline below is if anything generous: it
// checks one del-segment per base key, fewer ops than moss's real heap
// merge, which visits both the base key AND its tombstone.

// (keyRange is now defined in segment.go, the graduated implementation.)

// rdSegment is a segment augmented with a DeleteRange tombstone list.
// rangeDels is kept sorted by lo and coalesced (non-overlapping), which is
// what a real merge/compaction would maintain.
type rdSegment struct {
	s         *segment
	rangeDels []keyRange
}

// covers reports whether any range tombstone in this segment covers key,
// via an O(log R) search over the sorted, coalesced range list.
func (rs *rdSegment) covers(key []byte) bool {
	rd := rs.rangeDels
	if len(rd) == 0 { // The common case: one cheap branch, no search.
		return false
	}
	// Rightmost range whose lo <= key.
	i := sort.Search(len(rd), func(i int) bool {
		return bytes.Compare(rd[i].lo, key) > 0
	})
	if i == 0 {
		return false
	}
	return bytes.Compare(key, rd[i-1].hi) < 0
}

// coveringRange returns the tombstone covering key (for the scan skip).
func coveringRange(rd []keyRange, key []byte) (keyRange, bool) {
	if len(rd) == 0 {
		return keyRange{}, false
	}
	i := sort.Search(len(rd), func(i int) bool {
		return bytes.Compare(rd[i].lo, key) > 0
	})
	if i == 0 {
		return keyRange{}, false
	}
	c := rd[i-1]
	if bytes.Compare(key, c.hi) < 0 {
		return c, true
	}
	return keyRange{}, false
}

// rdGet resolves a point Get over a stack (newest segment last), applying
// range-tombstone precedence: at each level a point op wins immediately;
// otherwise a covering range tombstone deletes the key (a nil base), exactly
// as a point Del would in resolveMerge.  (Merge operands are out of scope
// for this spike.)  Returns op==0 when the key is absent.
func rdGet(stack []*rdSegment, key []byte) (op uint64, val []byte) {
	for i := len(stack) - 1; i >= 0; i-- {
		seg := stack[i]
		o, v, _ := seg.s.Get(key)
		if o != 0 { // A point Set/Del/Merge at this level shadows all below.
			return o, v
		}
		if seg.covers(key) {
			return OperationDel, nil // Range tombstone: deleted base.
		}
	}
	return 0, nil
}

// ---- builders over real moss *segments ----

// buildMossDelSeg builds a sorted segment of point Del tombstones (the
// status-quo way to delete a set of keys).
func buildMossDelSeg(t testing.TB, keys [][]byte) *segment {
	var tot int
	for i := range keys {
		tot += len(keys[i])
	}
	b, err := newBatch(nil, BatchOptions{len(keys), tot})
	if err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		if err := b.Del(keys[i]); err != nil {
			t.Fatal(err)
		}
	}
	prev := SkipStats
	SkipStats = true
	b.doSort()
	SkipStats = prev
	return b.segment
}

// segBytes estimates a segment's in-memory/on-disk footprint: 16 bytes of
// kvs metadata per entry (2 uint64) plus the key-val buf.
func segBytes(s *segment) int { return 16*s.Len() + len(s.buf) }

// ---- correctness (guards the precedence rule) ----

func TestSpikeDelRangeCorrect(t *testing.T) {
	const n = 1000
	keys := spikeKeys(n, "seq")
	vals := make([][]byte, n) // Index-style: empty values.

	base := buildMossSeg(t, keys, vals)

	const dropLo, dropHi = 400, 600 // Drop [keys[400], keys[600]).
	drop := keyRange{keys[dropLo], keys[dropHi]}

	// Two representations of the same drop.
	empty := buildMossSeg(t, nil, nil)
	stackRange := []*rdSegment{{base, nil}, {empty, []keyRange{drop}}}

	delPt := buildMossDelSeg(t, keys[dropLo:dropHi])
	stackPoint := []*rdSegment{{base, nil}, {delPt, nil}}

	for i, k := range keys {
		wantDeleted := i >= dropLo && i < dropHi
		for name, stack := range map[string][]*rdSegment{
			"range": stackRange, "point": stackPoint,
		} {
			op, _ := rdGet(stack, k)
			deleted := op == 0 || op == OperationDel
			if deleted != wantDeleted {
				t.Fatalf("%s: key[%d] deleted=%v want %v", name, i, deleted, wantDeleted)
			}
		}
	}

	// Precedence: a newer Set ABOVE the range tombstone must survive it.
	revive := buildMossSeg(t, [][]byte{keys[500]}, [][]byte{[]byte("revived")})
	stackRevive := []*rdSegment{
		{base, nil}, {empty, []keyRange{drop}}, {revive, nil},
	}
	if op, v := rdGet(stackRevive, keys[500]); op != OperationSet || string(v) != "revived" {
		t.Fatalf("revive: op=%x val=%q, want Set/revived", op, v)
	}
	// A sibling still inside the range (no override) stays deleted.
	if op, _ := rdGet(stackRevive, keys[550]); op != OperationDel {
		t.Fatalf("revive: key[550] op=%x, want Del", op)
	}
}

func TestSpikeDelRangeSizes(t *testing.T) {
	const n = 1000000
	keys := spikeKeys(n, "seq")
	const dropLo, dropHi = 400000, 600000 // Drop 200k keys (20%).

	delPt := buildMossDelSeg(t, keys[dropLo:dropHi])
	ptBytes := segBytes(delPt)

	// A range tombstone stores 2 keys (lo, hi) + one entry's metadata.
	rangeBytes := 16 + len(keys[dropLo]) + len(keys[dropHi])

	t.Logf("drop of %d keys: point-Del segment=%d bytes (%d tombstones), "+
		"range tombstone=%d bytes -> %.0fx smaller",
		dropHi-dropLo, ptBytes, delPt.Len(), rangeBytes,
		float64(ptBytes)/float64(rangeBytes))
}

// ---- benchmarks ----

func BenchmarkSpikeDelRange(b *testing.B) {
	const n = 1000000
	keys := spikeKeys(n, "seq")
	vals := make([][]byte, n) // Index-style: empty values.
	base := buildMossSeg(b, keys, vals)

	const dropLo, dropHi = 400000, 600000 // Drop 200k keys (20%).
	drop := keyRange{keys[dropLo], keys[dropHi]}
	rd := []keyRange{drop}
	delKeys := keys[dropLo:dropHi]
	empty := buildMossSeg(b, nil, nil)
	delPt := buildMossDelSeg(b, delKeys)

	stackRange := []*rdSegment{{base, nil}, {empty, rd}}
	stackPoint := []*rdSegment{{base, nil}, {delPt, nil}}

	// (1) Maintenance: cost to record the drop.  Point-Del must enumerate
	// the range (scan base) then build M tombstones; range builds one entry.
	b.Run("maintain/point", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Enumerate keys in [lo, hi) as a real reindex/drop would.
			c, _ := base.Cursor(drop.lo, drop.hi)
			var found [][]byte
			for {
				op, k, _ := c.Current()
				if op == 0 {
					break
				}
				found = append(found, k)
				if c.Next() != nil {
					break
				}
			}
			_ = buildMossDelSeg(b, found)
		}
	})
	b.Run("maintain/range", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s := buildMossSeg(b, nil, nil)
			_ = &rdSegment{s, []keyRange{{drop.lo, drop.hi}}}
		}
	})

	// (2) Point Get on keys inside the dropped range (both must report gone).
	b.Run("get-in-range/point", func(b *testing.B) {
		benchRdGet(b, stackPoint, delKeys, true)
	})
	b.Run("get-in-range/range", func(b *testing.B) {
		benchRdGet(b, stackRange, delKeys, true)
	})

	// (3) Point Get on surviving keys (outside the range).
	live := append(append([][]byte{}, keys[:dropLo]...), keys[dropHi:]...)
	b.Run("get-live/point", func(b *testing.B) {
		benchRdGet(b, stackPoint, live, false)
	})
	b.Run("get-live/range", func(b *testing.B) {
		benchRdGet(b, stackRange, live, false)
	})

	// (4) Full scan of surviving keys after the drop.
	wantLive := n - (dropHi - dropLo)
	b.Run("scan/point", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if got := scanPoint(base, delPt); got != wantLive {
				b.Fatalf("scan point got %d want %d", got, wantLive)
			}
		}
	})
	b.Run("scan/range", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if got := scanRange(base, rd); got != wantLive {
				b.Fatalf("scan range got %d want %d", got, wantLive)
			}
		}
	})
}

// benchRdGet times rdGet over the given keys; wantDeleted asserts each key's
// resolved state so a broken model can't post a fast bogus number.
func benchRdGet(b *testing.B, stack []*rdSegment, keys [][]byte, wantDeleted bool) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[benchStridedIndex(i, len(keys))]
		op, _ := rdGet(stack, k)
		deleted := op == 0 || op == OperationDel
		if deleted != wantDeleted {
			b.Fatalf("key %q deleted=%v want %v", k, deleted, wantDeleted)
		}
	}
}

// scanPoint counts live keys by walking base and rejecting any key present
// in the point-Del segment (one del-Get per base key).
func scanPoint(base, del *segment) int {
	c, _ := base.Cursor(nil, nil)
	n := 0
	for {
		op, k, _ := c.Current()
		if op == 0 {
			break
		}
		if dop, _, _ := del.Get(k); dop == 0 {
			n++
		}
		if c.Next() != nil {
			break
		}
	}
	return n
}

// scanRange counts live keys by walking base and Seek()ing past each covered
// span in one jump -- the O(log) scan skip a range tombstone enables.
func scanRange(base *segment, rd []keyRange) int {
	c, _ := base.Cursor(nil, nil)
	n := 0
	for {
		op, k, _ := c.Current()
		if op == 0 {
			break
		}
		if cr, ok := coveringRange(rd, k); ok {
			if c.Seek(cr.hi) != nil { // Jump straight past the deleted span.
				break
			}
			continue
		}
		n++
		if c.Next() != nil {
			break
		}
	}
	return n
}
