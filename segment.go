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
	"fmt"
	"sort"
)

// SegmentKindBasic is the code for a basic, persistable segment
// implementation, which represents a segment as two arrays: an array
// of contiguous key-val bytes [key0, val0, key1, val1, ... keyN,
// valN], and an array of offsets plus lengths into the first array.
var SegmentKindBasic = "a"

func init() {
	SegmentLoaders[SegmentKindBasic] = loadBasicSegment
	SegmentPersisters[SegmentKindBasic] = persistBasicSegment
}

// A SegmentCursor represents a handle for iterating through consecutive
// op/key/value tuples.
type SegmentCursor interface {
	// Current returns the operation/key/value pointed to by the cursor.
	Current() (operation uint64, key []byte, val []byte)

	// Seek advances current to point to specified key.
	// If the seek key is less than the original startKeyInclusive
	// used to create this cursor, it will seek to that startKeyInclusive
	// instead.
	// If the cursor is not pointing at a valid entry ErrIteratorDone
	// is returned.
	Seek(startKeyInclusive []byte) error

	// Next moves the cursor to the next entry.  If there is no Next
	// entry, ErrIteratorDone is returned.
	Next() error
}

// A Segment represents the read-oriented interface for a segment.
type Segment interface {
	// Returns the kind of segment, used for persistence.
	Kind() string

	// Len returns the number of ops in the segment.
	Len() int

	// NumKeyValBytes returns the number of bytes used for key-val data.
	NumKeyValBytes() (uint64, uint64)

	// Get returns the operation and value associated with the given key.
	// If the key does not exist, the operation is 0, and the val is nil.
	// If an error occurs it is returned instead of the operation and value.
	Get(key []byte) (operation uint64, val []byte, err error)

	// Cursor returns an SegmentCursor that will iterate over entries
	// from the given (inclusive) start key, through the given (exclusive)
	// end key.
	Cursor(startKeyInclusive []byte, endKeyExclusive []byte) (SegmentCursor,
		error)

	// Returns true if the segment is already sorted, and returns
	// false if the sorting is only asynchronously scheduled.
	RequestSort(synchronous bool) bool
}

// SegmentValidater is an optional interface that can be implemented by
// any Segment to allow additional validation in test cases.  The
// method of this interface is NOT invoked during the normal
// runtime usage of a Segment.
type SegmentValidater interface {

	// Valid examines the state of the segment, any problem is returned
	// as an error.
	Valid() error
}

// A SegmentMutator represents the mutation methods of a segment.
type SegmentMutator interface {
	Mutate(operation uint64, key, val []byte) error
}

// A SegmentPersister represents a segment that can be persisted.
type SegmentPersister interface {
	Persist(file File, options *StoreOptions) (SegmentLoc, error)
}

// A segment is a basic implementation of the segment related
// interfaces and represents a sequence of key-val entries or
// operations.  A segment's kvs will be sorted by key when the segment
// is pushed into the collection.  A segment implements the Batch
// interface.
type segment struct {
	// Each key-val operation is encoded as 2 uint64's...
	// - operation (see: maskOperation) |
	//       key length (see: maskKeyLength) |
	//       val length (see: maskValLength).
	// - start index into buf for key-val bytes.
	kvs []uint64

	// Contiguous backing memory for the keys and vals of the segment.
	buf []byte

	// If this segment needs sorting, then needSorterCh will be
	// non-nil and also the first goroutine that reads successfully
	// from needSorterCh becomes the sorter of this segment.  All
	// other goroutines must instead wait on the waitSortedCh.
	needSorterCh chan bool

	// Once the sorter of this segment is done sorting the kvs, it
	// close()'s the waitSortedCh, treating waitSortedCh like a
	// one-way latch.  The needSorterCh and waitSortedCh will either
	// be nil or non-nil together.  A segment that was "born
	// sorted" will have needSorterCh and waitSortedCh as both nil.
	waitSortedCh chan struct{}

	totOperationSet      uint64
	totOperationDel      uint64
	totOperationMerge    uint64
	totOperationDelRange uint64
	totKeyByte           uint64
	totValByte           uint64

	rootCollection *collection // Non-nil when segment is from a batch.

	// In-memory index, immutable after segment initialization.
	index *segmentKeysIndex

	// rangeDels holds this segment's OperationDelRange tombstones as a
	// sorted (by lo), coalesced list of half-open [lo, hi) intervals,
	// built at sort/load finalization from the inline DelRange entries.
	// It is the lookup structure for range-delete coverage; nil/empty for
	// the common case of a segment with no range tombstones, so point
	// reads pay only a len==0 branch.  Immutable after finalization.
	rangeDels []keyRange
}

// keyRange is a half-open [lo, hi) key interval; a DelRange tombstone.
type keyRange struct {
	lo []byte
	hi []byte
}

// See the OperationXxx consts.
const maskOperation = uint64(0x0F00000000000000)

// Max key length is 2^24, from 24 bits key length.
const maskKeyLength = uint64(0x00FFFFFF00000000)

const maxKeyLength = 1<<24 - 1

// Max val length is 2^28, from 28 bits val length.
const maskValLength = uint64(0x000000000FFFFFFF)

const maxValLength = 1<<28 - 1

const maskRESERVED = uint64(0xF0000000F0000000)

// newSegment() allocates a segment with hinted amount of resources.
func newSegment(totalOps, totalKeyValBytes int) (*segment, error) {
	return &segment{
		kvs: make([]uint64, 0, totalOps*2),
		buf: make([]byte, 0, totalKeyValBytes),
	}, nil
}

func (a *segment) Kind() string { return SegmentKindBasic }

// Close releases resources associated with the segment.
func (a *segment) Close() error {
	return nil
}

// Set copies the key and val bytes into the segment as a "set"
// mutation.  The key must be unique (not repeated) within the
// segment.
func (a *segment) Set(key, val []byte) error {
	return a.mutate(OperationSet, key, val)
}

// Del copies the key bytes into the segment as a "deletion" mutation.
// The key must be unique (not repeated) within the segment.
func (a *segment) Del(key []byte) error {
	return a.mutate(OperationDel, key, nil)
}

// Merge creates or updates a key-val entry in the Collection via the
// MergeOperator defined in the CollectionOptions.  The key must be
// unique (not repeated) within the segment.
func (a *segment) Merge(key, val []byte) error {
	return a.mutate(OperationMerge, key, val)
}

// DelRange records a range-delete tombstone covering
// [startKeyInclusive, endKeyExclusive) as a single entry whose key is the
// startKeyInclusive and whose value is the endKeyExclusive.
func (a *segment) DelRange(startKeyInclusive, endKeyExclusive []byte) error {
	if bytes.Compare(startKeyInclusive, endKeyExclusive) >= 0 {
		return ErrBadRange
	}
	return a.mutate(OperationDelRange, startKeyInclusive, endKeyExclusive)
}

// ------------------------------------------------------

// Alloc provides a slice of bytes "owned" by the segment, to reduce
// extra copying of memory.  See the Collection.NewBatch() method.
func (a *segment) Alloc(numBytes int) ([]byte, error) {
	bufLen := len(a.buf)
	bufCap := cap(a.buf)

	if numBytes > bufCap-bufLen {
		return nil, ErrAllocTooLarge
	}

	rv := a.buf[bufLen : bufLen+numBytes]

	a.buf = a.buf[0 : bufLen+numBytes]

	return rv, nil
}

// AllocSet is like Set(), but the caller must provide []byte
// parameters that came from Alloc(), for less buffer copying.
func (a *segment) AllocSet(keyFromAlloc, valFromAlloc []byte) error {
	bufCap := cap(a.buf)

	keyStart := bufCap - cap(keyFromAlloc)

	return a.mutateEx(OperationSet,
		keyStart, len(keyFromAlloc), len(valFromAlloc))
}

// AllocDel is like Del(), but the caller must provide []byte
// parameters that came from Alloc(), for less buffer copying.
func (a *segment) AllocDel(keyFromAlloc []byte) error {
	bufCap := cap(a.buf)

	keyStart := bufCap - cap(keyFromAlloc)

	return a.mutateEx(OperationDel,
		keyStart, len(keyFromAlloc), 0)
}

// AllocMerge is like Merge(), but the caller must provide []byte
// parameters that came from Alloc(), for less buffer copying.
func (a *segment) AllocMerge(keyFromAlloc, valFromAlloc []byte) error {
	bufCap := cap(a.buf)

	keyStart := bufCap - cap(keyFromAlloc)

	return a.mutateEx(OperationMerge,
		keyStart, len(keyFromAlloc), len(valFromAlloc))
}

// ------------------------------------------------------

func (a *segment) Mutate(operation uint64, key, val []byte) error {
	return a.mutate(operation, key, val)
}

func (a *segment) mutate(operation uint64, key, val []byte) error {
	keyStart := len(a.buf)
	a.buf = append(a.buf, key...)
	keyLength := len(a.buf) - keyStart

	valStart := len(a.buf)
	a.buf = append(a.buf, val...)
	valLength := len(a.buf) - valStart

	return a.mutateEx(operation, keyStart, keyLength, valLength)
}

func (a *segment) mutateEx(operation uint64,
	keyStart, keyLength, valLength int) error {
	if keyLength > maxKeyLength {
		return ErrKeyTooLarge
	}
	if valLength > maxValLength {
		return ErrValueTooLarge
	}

	if keyLength <= 0 && valLength <= 0 {
		keyStart = 0
	}

	opKlVl := encodeOpKeyLenValLen(operation, keyLength, valLength)

	a.kvs = append(a.kvs, opKlVl, uint64(keyStart))

	switch operation {
	case OperationSet:
		a.totOperationSet++
	case OperationDel:
		a.totOperationDel++
	case OperationMerge:
		a.totOperationMerge++
	case OperationDelRange:
		a.totOperationDelRange++
	default:
	}

	a.totKeyByte += uint64(keyLength)
	a.totValByte += uint64(valLength)

	return nil
}

// ------------------------------------------------------

// NumKeyValBytes returns the number of bytes used for key-val data.
func (a *segment) NumKeyValBytes() (uint64, uint64) {
	return a.totKeyByte, a.totValByte
}

// ------------------------------------------------------

// Len returns the number of ops in the segment.
func (a *segment) Len() int {
	return len(a.kvs) / 2
}

func (a *segment) Swap(i, j int) {
	x := i * 2
	y := j * 2

	// Operation + key length + val length.
	a.kvs[x], a.kvs[y] = a.kvs[y], a.kvs[x]

	x++
	y++

	a.kvs[x], a.kvs[y] = a.kvs[y], a.kvs[x] // Buf index.
}

func (a *segment) Less(i, j int) bool {
	x := i * 2
	y := j * 2

	kxLength := int((maskKeyLength & a.kvs[x]) >> 32)
	kxStart := int(a.kvs[x+1])
	kx := a.buf[kxStart : kxStart+kxLength]

	kyLength := int((maskKeyLength & a.kvs[y]) >> 32)
	kyStart := int(a.kvs[y+1])
	ky := a.buf[kyStart : kyStart+kyLength]

	return bytes.Compare(kx, ky) < 0
}

// ------------------------------------------------------

type segmentCursor struct {
	s     *segment
	start int
	end   int
	curr  int
}

func (c *segmentCursor) Current() (operation uint64, key []byte, val []byte) {
	if c.curr >= c.start && c.curr < c.end {
		operation, key, val = c.s.getOperationKeyVal(c.curr)
	}
	return
}

func (c *segmentCursor) Seek(startKeyInclusive []byte) error {
	pos, err := c.s.findStartKeyInclusivePos(startKeyInclusive)
	if err != nil {
		return err
	}
	c.curr = pos
	if c.curr < c.start {
		c.curr = c.start
	}
	if c.curr >= c.end {
		return ErrIteratorDone
	}
	return nil
}

func (c *segmentCursor) Next() error {
	c.curr++
	if c.curr >= c.end {
		return ErrIteratorDone
	}
	return nil
}

// nextDelta advances the cursor position by 'delta' steps.
func (c *segmentCursor) nextDelta(delta int) error {
	c.curr += delta
	if c.curr >= c.end {
		return ErrIteratorDone
	}
	return nil
}

// currentKey returns the array position and the key pointed to by the cursor.
func (c *segmentCursor) currentKey() (idx int, key []byte) {
	if c.curr >= c.start && c.curr < c.end {
		idx = c.curr
		_, key, _ = c.s.getOperationKeyVal(c.curr)
	}
	return
}

func (a *segment) Cursor(startKeyInclusive []byte, endKeyExclusive []byte) (
	SegmentCursor, error) {
	rv := &segmentCursor{
		s:   a,
		end: a.Len(),
	}
	start, err := a.findStartKeyInclusivePos(startKeyInclusive)
	if err != nil {
		return nil, err
	}
	rv.start = start
	if endKeyExclusive != nil {
		end, err := a.findStartKeyInclusivePos(endKeyExclusive)
		if err != nil {
			return nil, err
		}
		rv.end = end
	}
	rv.curr = rv.start
	return rv, nil
}

func (a *segment) Get(key []byte) (operation uint64, val []byte, err error) {
	var pos int
	pos, err = a.findKeyPos(key)
	if err != nil {
		return
	}

	if pos >= 0 {
		operation, _, val = a.getOperationKeyVal(pos)
		if operation == OperationDelRange {
			// A range tombstone is not a point entry: it happens to be
			// keyed by its lo bound but must not answer a point lookup.
			// Range-delete coverage is resolved via the rangeDels
			// side-list (see covers), not the point-lookup binary search.
			return 0, nil, nil
		}
	}
	return
}

// rangeDeleter is implemented by segments that can carry range-delete
// tombstones.  The read path and iterator resolve coverage through this
// unexported interface, so the public Segment interface need not widen and
// external Segment implementers remain compatible (they simply carry no
// range tombstones).
type rangeDeleter interface {
	covers(key []byte) bool
	coveringRange(key []byte) (keyRange, bool)
	hasRangeDels() bool
}

// hasRangeDels reports whether this segment carries any range tombstones.
func (a *segment) hasRangeDels() bool { return len(a.rangeDels) > 0 }

// covers reports whether any of this segment's range tombstones covers
// key.  It short-circuits (one branch) for the common case of a segment
// with no range tombstones.
func (a *segment) covers(key []byte) bool {
	_, ok := a.coveringRange(key)
	return ok
}

// coveringRange returns the range tombstone covering key, if any, via an
// O(log R) search over the sorted, coalesced rangeDels list.
func (a *segment) coveringRange(key []byte) (keyRange, bool) {
	rd := a.rangeDels
	if len(rd) == 0 {
		return keyRange{}, false
	}
	// Rightmost range whose lo <= key (ranges are coalesced, so at most
	// one can contain key).
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

// buildRangeDels collects this segment's inline OperationDelRange entries
// into the rangeDels side-list: sorted by lo and coalesced into disjoint
// half-open intervals.  Called at sort/load finalization; a no-op (and no
// scan) when the segment has no range tombstones.
func (a *segment) buildRangeDels() {
	if a.totOperationDelRange == 0 {
		a.rangeDels = nil
		return
	}

	var rds []keyRange
	n := a.Len()
	for pos := 0; pos < n; pos++ {
		op, key, val := a.getOperationKeyVal(pos)
		if op == OperationDelRange {
			rds = append(rds, keyRange{lo: key, hi: val})
		}
	}
	if len(rds) == 0 {
		a.rangeDels = nil
		return
	}

	sort.Slice(rds, func(i, j int) bool {
		return bytes.Compare(rds[i].lo, rds[j].lo) < 0
	})

	// Coalesce overlapping or adjacent ranges so coveringRange's binary
	// search sees disjoint intervals.
	coalesced := make([]keyRange, 0, len(rds))
	for _, r := range rds {
		if len(coalesced) > 0 {
			last := &coalesced[len(coalesced)-1]
			if bytes.Compare(r.lo, last.hi) <= 0 { // Overlap or adjacency.
				if bytes.Compare(r.hi, last.hi) > 0 {
					last.hi = r.hi
				}
				continue
			}
		}
		coalesced = append(coalesced, r)
	}

	a.rangeDels = coalesced
}

// Searches for the key within the in-memory index of the segment
// if available. Returns left and right positions between which
// the key likely exists.
func (a *segment) searchIndex(key []byte) (int, int) {
	if a.index != nil {
		// Check the in-memory index for a more accurate window.
		return a.index.lookup(key)
	}

	return 0, a.Len()
}

// keyAt returns the key bytes for the entry at the given logical
// position (0-based).  It bounds-checks both the kvs index array and
// the buf backing array before slicing, so that a corrupt or
// truncated segment (for example, an mmap'd file that is shorter than
// its footer claims) yields ErrSegmentCorrupted instead of a SIGBUS
// or an out-of-range panic.  All segment read paths that decode a key
// out of a possibly-mmap'd buf should route through keyAt.
func (a *segment) keyAt(pos int) ([]byte, error) {
	x := pos * 2
	if x < 0 || x+1 >= len(a.kvs) {
		return nil, ErrSegmentCorrupted
	}
	keyLen := int((maskKeyLength & a.kvs[x]) >> 32)
	kbeg := int(a.kvs[x+1])
	if kbeg < 0 || keyLen < 0 || kbeg+keyLen > len(a.buf) {
		return nil, ErrSegmentCorrupted
	}
	return a.buf[kbeg : kbeg+keyLen], nil
}

func (a *segment) findKeyPos(key []byte) (int, error) {
	if len(a.kvs) < 2 {
		return -1, nil
	}

	// If key smaller than smallest key, return early.
	startKey, err := a.keyAt(0)
	if err != nil {
		return -1, err
	}
	if bytes.Compare(key, startKey) < 0 {
		return -1, nil
	}

	i, j := a.searchIndex(key)
	if i == j {
		return -1, nil
	}

	// Best-effort guard against an mmap'd buf that's shorter than the
	// footer claims: validate the right-most candidate before looping.
	if _, err := a.keyAt(j - 1); err != nil {
		return -1, err
	}

	for i < j {
		h := i + (j-i)/2 // Keep i <= h < j.
		hKey, err := a.keyAt(h)
		if err != nil {
			return -1, err
		}

		cmp := bytes.Compare(hKey, key)
		if cmp == 0 {
			return h, nil
		} else if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}

	return -1, nil
}

// FindStartKeyInclusivePos() returns the logical entry position for
// the given (inclusive) start key.  With segment keys of [b, d, f],
// looking for 'c' will return 1.  Looking for 'd' will return 1.
// Looking for 'g' will return 3.  Looking for 'a' will return 0.
func (a *segment) findStartKeyInclusivePos(startKeyInclusive []byte) (int, error) {
	i, j := a.searchIndex(startKeyInclusive)
	if i == j {
		return i, nil
	}

	startKey, err := a.keyAt(0)
	if err != nil {
		return i, err
	}
	if bytes.Compare(startKeyInclusive, startKey) < 0 {
		// If key smaller than smallest key, return early.
		return i, nil
	}

	for i < j {
		h := i + (j-i)/2 // Keep i <= h < j.
		hKey, err := a.keyAt(h)
		if err != nil {
			return i, err
		}

		cmp := bytes.Compare(hKey, startKeyInclusive)
		if cmp == 0 {
			return h, nil
		} else if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}

	return i, nil
}

// getOperationKeyVal() returns the operation, key, val for a given
// logical entry position in the segment.
func (a *segment) getOperationKeyVal(pos int) (uint64, []byte, []byte) {
	x := pos * 2
	if x < 0 || x+1 >= len(a.kvs) {
		return 0, nil, nil
	}

	opklvl := a.kvs[x]
	kstart := int(a.kvs[x+1])
	operation, keyLen, valLen := decodeOpKeyLenValLen(opklvl)
	vstart := kstart + keyLen
	vend := vstart + valLen

	// Bounds-check against buf before slicing, so a corrupt or
	// truncated (e.g. mmap'd) segment can't SIGBUS or panic here.
	if kstart < 0 || keyLen < 0 || valLen < 0 || vend > len(a.buf) {
		return 0, nil, nil
	}

	return operation, a.buf[kstart:vstart], a.buf[vstart:vend]
}

// ------------------------------------------------------

func encodeOpKeyLenValLen(operation uint64, keyLen, valLen int) uint64 {
	return (maskOperation & operation) |
		(maskKeyLength & (uint64(keyLen) << 32)) |
		(maskValLength & (uint64(valLen)))
}

func decodeOpKeyLenValLen(opklvl uint64) (uint64, int, int) {
	operation := maskOperation & opklvl
	keyLen := int((maskKeyLength & opklvl) >> 32)
	valLen := int(maskValLength & opklvl)
	return operation, keyLen, valLen
}

// ------------------------------------------------------
// readyDeferredSort() will create a ticket for the future sorter and
// a channel to wait for its completion
func (a *segment) readyDeferredSort() {
	a.needSorterCh = make(chan bool, 1)
	a.needSorterCh <- true // A ticket for the future sorter.
	close(a.needSorterCh)

	a.waitSortedCh = make(chan struct{})
}

// RequestSort() will either perform the previously deferred sorting,
// if the goroutine can acquire the 1 ticket from the needSorterCh.
// Or, requestSort() will ensure that a sorter is working on this
// segment.  Returns true if the segment is sorted, and returns false
// if the sorting is only asynchronously scheduled.
func (a *segment) RequestSort(synchronous bool) bool {
	if a.needSorterCh == nil {
		return true
	}

	iAmTheSorter := <-a.needSorterCh
	if iAmTheSorter {
		a.doSort()
		close(a.waitSortedCh) // Signal any waiters.
		return true
	}

	if synchronous {
		<-a.waitSortedCh // Wait for the sorter to be done.
		return true
	}

	return false
}

// doSort() will immediately sort this segment.
func (a *segment) doSort() {
	// After sorting, the segment is immutable and then safe for
	// concurrent reads.
	sort.Sort(a)

	// Build the sparse key index for large segments now that keys are
	// in sorted order; a no-op for small segments.
	a.buildInMemIndex()

	// Collect any range-delete tombstones into the coverage side-list; a
	// no-op (no scan) when the segment has no range tombstones.
	a.buildRangeDels()

	if !SkipStats {
		go a.rootCollection.updateStats(a)
	}
}

// SkipStats allows advanced applications that don't care about
// correct stats to avoid some stats maintenance overhead.  Defaults
// to false (stats are correctly maintained).
var SkipStats bool

// ------------------------------------------------------

// Persist persists a basic segment, and allows a segment to meet the
// SegmentPersister interface.
func (a *segment) Persist(file File, options *StoreOptions) (rv SegmentLoc, err error) {
	finfo, err := file.Stat()
	if err != nil {
		return rv, err
	}

	persistKind := DefaultPersistKind
	if options.PersistKind != "" {
		persistKind = options.PersistKind
	}

	segmentPersister, exists := SegmentPersisters[persistKind]
	if !exists || segmentPersister == nil {
		return rv, fmt.Errorf("store: unknown PersistKind: %+v", persistKind)
	}

	return segmentPersister(a, file, finfo.Size(), nil)
}

// ------------------------------------------------------

// loadBasicSegment loads a basic segment.
func loadBasicSegment(sloc *SegmentLoc) (Segment, error) {
	var kvs []uint64
	var buf []byte
	var err error

	if sloc.KvsBytes > 0 {
		if sloc.KvsBytes > uint64(len(sloc.mref.buf)) {
			return nil, fmt.Errorf("store: load basic segment KvsOffset/KvsBytes too big,"+
				" len(mref.buf): %d, sloc: %+v", len(sloc.mref.buf), sloc)
		}

		kvsBytes := sloc.mref.buf[0:sloc.KvsBytes]
		kvs, err = ByteSliceToUint64Slice(kvsBytes)
		if err != nil {
			return nil, err
		}
	}

	if sloc.BufBytes > 0 {
		bufStart := sloc.BufOffset - sloc.KvsOffset
		if bufStart+sloc.BufBytes > uint64(len(sloc.mref.buf)) {
			return nil, fmt.Errorf("store: load basic segment BufOffset/BufBytes too big,"+
				" len(mref.buf): %d, sloc: %+v", len(sloc.mref.buf), sloc)
		}

		buf = sloc.mref.buf[bufStart : bufStart+sloc.BufBytes]
	}

	seg := &segment{
		kvs:                  kvs,
		buf:                  buf,
		totOperationSet:      sloc.TotOpsSet,
		totOperationDel:      sloc.TotOpsDel,
		totKeyByte:           sloc.TotKeyByte,
		totValByte:           sloc.TotValByte,
		totOperationDelRange: sloc.TotOpsDelRange,
	}

	// Rebuild the range-delete coverage side-list only when the segment
	// actually has range tombstones (a one-time scan); zero-cost otherwise,
	// so stores that never use DelRange pay nothing on load.
	seg.buildRangeDels()

	return seg, nil
}

// ------------------------------------------------------

func persistBasicSegment(
	s Segment, file File, pos int64, options *StoreOptions) (rv SegmentLoc, err error) {

	seg, ok := s.(*segment)
	if !ok {
		return rv, fmt.Errorf("wrong segment type")
	}

	kvsBuf, err := Uint64SliceToByteSlice(seg.kvs)
	if err != nil {
		return rv, err
	}

	kvsPos := pageAlignCeil(pos)
	bufPos := pageAlignCeil(kvsPos + int64(len(kvsBuf)))

	ioCh := make(chan ioResult)

	go func() {
		kvsWritten, err := file.WriteAt(kvsBuf, kvsPos)
		ioCh <- ioResult{kind: "kvs", want: len(kvsBuf), got: kvsWritten, err: err}
	}()

	go func() {
		bufWritten, err := file.WriteAt(seg.buf, bufPos)
		ioCh <- ioResult{kind: "buf", want: len(seg.buf), got: bufWritten, err: err}
	}()

	resMap := map[string]ioResult{}
	for len(resMap) < 2 {
		res := <-ioCh
		if res.err != nil {
			return rv, res.err
		}
		if res.want != res.got {
			return rv, fmt.Errorf("store: persistSegment error writing,"+
				" res: %+v, err: %v", res, res.err)
		}
		resMap[res.kind] = res
	}

	close(ioCh)

	return SegmentLoc{
		Kind:           seg.Kind(),
		KvsOffset:      uint64(kvsPos),
		KvsBytes:       uint64(resMap["kvs"].got),
		BufOffset:      uint64(bufPos),
		BufBytes:       uint64(resMap["buf"].got),
		TotOpsSet:      seg.totOperationSet,
		TotOpsDel:      seg.totOperationDel,
		TotKeyByte:     seg.totKeyByte,
		TotValByte:     seg.totValByte,
		TotOpsDelRange: seg.totOperationDelRange,
	}, nil
}

func (a *segment) Valid() error {
	if a.kvs == nil || len(a.kvs) <= 0 {
		return fmt.Errorf("expected kvs")
	}
	if a.buf == nil || len(a.buf) <= 0 {
		return fmt.Errorf("expected buf")
	}
	for pos := 0; pos < a.Len(); pos++ {
		x := pos * 2
		if x < 0 || x >= len(a.kvs) {
			return fmt.Errorf("pos to x error")
		}

		opklvl := a.kvs[x]

		operation, keyLen, valLen := decodeOpKeyLenValLen(opklvl)
		if operation == 0 {
			return fmt.Errorf("should have some nonzero op")
		}

		kstart := int(a.kvs[x+1])
		vstart := kstart + keyLen

		if kstart+keyLen > len(a.buf) {
			return fmt.Errorf("key larger than buf, pos: %d, kstart: %d, keyLen: %d, len(buf): %d, op: %x",
				pos, kstart, keyLen, len(a.buf), operation)
		}
		if vstart+valLen > len(a.buf) {
			return fmt.Errorf("val larger than buf, pos: %d, vstart: %d, valLen: %d, len(buf): %d, op: %x",
				pos, vstart, valLen, len(a.buf), operation)
		}
	}

	return nil
}

// ------------------------------------------------------

// Thresholds for auto-building the sparse segment key index (see
// segmentKeysIndex) on large IN-MEMORY segments -- the point-lookup
// binary search is memory-bound on large segments, and the index
// narrows the search window with a small, contiguous, cache-friendly
// sampled-key array.  buildIndex is a cheap no-op below the threshold,
// so small batch segments pay only a size check.  These are distinct
// from the store's SegmentKeysIndex* options (persisted segments).
const inMemSegmentKeysIndexMinKeyBytes = 1 << 20 // Index >= ~1MB of keys.

// inMemSegmentKeysIndexTargetHop is the desired number of source-segment
// keys between adjacent sampled index keys.  A small hop keeps the
// post-index search window small, so the speedup doesn't decay as the
// segment grows; the index budget is sized from it (proportional to
// segment size, ~1/targetHop of the key bytes), capped below.
const inMemSegmentKeysIndexTargetHop = 32

// inMemSegmentKeysIndexMaxBytes caps the per-segment in-memory index
// size, bounding memory for pathologically large segments (at the cost
// of a coarser hop above the cap).
const inMemSegmentKeysIndexMaxBytes = 8 << 20 // 8MB.

// buildInMemIndex builds the sparse key index for an in-memory segment
// (batch-sorted or merge-produced) if it's large enough to benefit.
// Measured ~13-14% faster point Gets on a 1M-entry segment, for both
// short- and long-shared-prefix keys, for ~1/targetHop of the key bytes
// in memory; a no-op below the threshold.
func (a *segment) buildInMemIndex() {
	if int(a.totKeyByte) < inMemSegmentKeysIndexMinKeyBytes {
		return
	}
	keyCount := a.Len()
	if keyCount == 0 {
		return
	}
	keyAvgSize := int(a.totKeyByte) / keyCount

	// Budget the index to sample ~every targetHop'th key (+4 bytes per
	// sample for its offset), so a large segment gets a proportionally
	// larger, still-small index rather than an ever-coarsening one.
	quota := (keyCount / inMemSegmentKeysIndexTargetHop) * (keyAvgSize + 4)
	if quota > inMemSegmentKeysIndexMaxBytes {
		quota = inMemSegmentKeysIndexMaxBytes
	}
	a.buildIndex(quota, inMemSegmentKeysIndexMinKeyBytes)
}

// Builds and initializes the in-memory index for the segment.
func (a *segment) buildIndex(quota int, minKeyBytes int) {
	if int(a.totKeyByte) < minKeyBytes {
		// Build the index only if the total key bytes is greater
		// than or equal to the SegmentKeysIndexMinKeyBytes.
		return
	}

	keyCount := a.Len()
	if keyCount == 0 {
		return // No keys to index.
	}

	keyAvgSize := int(a.totKeyByte) / keyCount

	sindex := newSegmentKeysIndex(quota, keyCount, keyAvgSize)
	if sindex == nil {
		return
	}

	scursor := &segmentCursor{
		s:   a,
		end: a.Len(),
	}

	for {
		keyIdx, key := scursor.currentKey()
		if key == nil {
			break
		}

		if !sindex.add(keyIdx, key) {
			break // Out of space.
		}

		err := scursor.nextDelta(sindex.hop)
		if err != nil {
			break
		}
	}

	a.index = sindex
}

// ------------------------------------------------------

type batch struct {
	// A batch is a type of segment with childCollections.
	*segment

	// childBatches track the (created/updated) segments of child
	// collections indexed by their unique collection names.
	childBatches map[string]*batch

	// childCollectionsDeleted records, as first-class immutable batch
	// content, which child collections were deleted in this batch (a set
	// of names).  Deletes are tracked separately from childBatches -- not
	// as a sentinel stuffed into that same map -- so that a same-batch
	// DelChildCollection(name) + NewChildCollectionBatch(name) do not
	// collide on one slot.  At execute time buildStackDirtyTop applies
	// these deletes first (dropping the prior incarnation); a name present
	// in BOTH sets is a delete+recreate, so the recreate mints a fresh
	// incarNum, exactly as a cross-batch delete+recreate does.
	childCollectionsDeleted map[string]bool
}

// newBatch() allocates a segment with hinted amount of resources.
func newBatch(rootCollection *collection, options BatchOptions) (
	*batch, error) {
	return &batch{
		segment: &segment{
			kvs:            make([]uint64, 0, options.TotalOps*2),
			buf:            make([]byte, 0, options.TotalKeyValBytes),
			rootCollection: rootCollection,
		},
		childBatches: nil, // Created later on demand.
	}, nil
}

func (b *batch) NewChildCollectionBatch(collectionName string,
	options BatchOptions) (Batch, error) {
	if len(collectionName) == 0 {
		return nil, ErrBadCollectionName
	}

	childBatch, err := newBatch(b.rootCollection, options)

	if b.childBatches == nil { // First creation of child batch.
		b.childBatches = make(map[string]*batch)
	}
	b.childBatches[collectionName] = childBatch

	// A same-batch Del(name) then New(name) is a delete+recreate: the
	// name stays in childCollectionsDeleted so buildStackDirtyTop drops
	// the prior incarnation and the recreate starts fresh.  (Do NOT clear
	// the deleted mark here.)

	return childBatch, err
}

func (b *batch) DelChildCollection(collectionName string) error {
	if len(collectionName) == 0 {
		return ErrNoSuchCollection
	}

	if b.childCollectionsDeleted == nil {
		b.childCollectionsDeleted = make(map[string]bool)
	}
	b.childCollectionsDeleted[collectionName] = true

	// A same-batch New(name) then Del(name) cancels the create: drop any
	// child batch so only the delete remains.
	delete(b.childBatches, collectionName)

	return nil
}

func (b *batch) readyDeferredSort() {
	for _, childBatch := range b.childBatches {
		childBatch.readyDeferredSort()
	}

	b.segment.readyDeferredSort()
}

// RequestSort() returns true if all child batches are sorted and
// false if sorting has been asynchronously scheduled.
func (b *batch) RequestSort() bool {
	// false because we must never wait for sorter else it can deadlock.
	sorted := b.segment.RequestSort(false)

	for _, childBatch := range b.childBatches {
		sorted = childBatch.RequestSort() && sorted
	}

	return sorted
}

func (b *batch) doSort() {
	b.segment.doSort()

	for _, childBatch := range b.childBatches {
		childBatch.doSort()
	}
}

func (b *batch) isEmpty() bool {
	if len(b.childBatches) != 0 || len(b.childCollectionsDeleted) != 0 {
		// Presence of child batches or child-collection deletes indicates
		// a non-empty batch even if the child batches themselves are
		// empty. This is so that collection creation/deletions will work.
		return false
	}

	return b.Len() <= 0
}
