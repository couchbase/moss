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
	"fmt"
	"reflect"
	"sort"
	"unsafe"
)

// BASIC_SEGMENT_KIND is persisted.
var BASIC_SEGMENT_KIND = "a"

func init() {
	SegmentLoaders[BASIC_SEGMENT_KIND] = loadBasicSegment
}

// A Segment represents the read-oriented interface for a segment.
type Segment interface {
	// Returns the kind of segment, used for persistence.
	Kind() string

	// Len returns the number of ops in the segment.
	Len() int

	// NumKeyValBytes returns the number of bytes used for key-val data.
	NumKeyValBytes() (uint64, uint64)

	// FindStartKeyInclusivePos() returns the logical entry position for
	// the given (inclusive) start key.  With segment keys of [b, d, f],
	// looking for 'c' will return 1.  Looking for 'd' will return 1.
	// Looking for 'g' will return 3.  Looking for 'a' will return 0.
	FindStartKeyInclusivePos(startKeyInclusive []byte) int

	// GetOperationKeyVal() returns the operation, key, val for a given
	// logical entry position in the segment.
	GetOperationKeyVal(pos int) (operation uint64, key []byte, val []byte)

	// Returns true if the segment is already sorted, and returns
	// false if the sorting is only asynchronously scheduled.
	RequestSort(synchronous bool) bool
}

// A SegmentMutator represents the mutation methods of a segment.
type SegmentMutator interface {
	Mutate(operation uint64, key, val []byte) error
}

// A SegmentPersister represents a segment that can be persisted.
type SegmentPersister interface {
	Persist(file File) (SegmentLoc, error)
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

	totOperationSet   uint64
	totOperationDel   uint64
	totOperationMerge uint64
	totKeyByte        uint64
	totValByte        uint64
}

// See the OperationXxx consts.
const maskOperation = uint64(0x0F00000000000000)

// Max key length is 2^24, from 24 bits key length.
const maskKeyLength = uint64(0x00FFFFFF00000000)

// Max val length is 2^28, from 28 bits val length.
const maskValLength = uint64(0x000000000FFFFFFF)

const maskRESERVED = uint64(0xF0000000F0000000)

// newSegment() allocates a segment with hinted amount of resources.
func newSegment(totalOps, totalKeyValBytes int) (
	*segment, error) {
	return &segment{
		kvs: make([]uint64, 0, totalOps*2),
		buf: make([]byte, 0, totalKeyValBytes),
	}, nil
}

func (a *segment) Kind() string { return BASIC_SEGMENT_KIND }

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

// FindStartKeyInclusivePos() returns the logical entry position for
// the given (inclusive) start key.  With segment keys of [b, d, f],
// looking for 'c' will return 1.  Looking for 'd' will return 1.
// Looking for 'g' will return 3.  Looking for 'a' will return 0.
func (a *segment) FindStartKeyInclusivePos(startKeyInclusive []byte) int {
	kvs := a.kvs
	buf := a.buf

	i, j := 0, a.Len()
	for i < j {
		h := i + (j-i)/2 // Keep i <= h < j.
		x := h * 2
		klen := int((maskKeyLength & kvs[x]) >> 32)
		kbeg := int(kvs[x+1])
		if bytes.Compare(buf[kbeg:kbeg+klen], startKeyInclusive) < 0 {
			i = h + 1
		} else {
			j = h
		}
	}

	return i

	// TODO: Do better than binary search?
	// TODO: Consider a perfectly balanced btree?
}

// GetOperationKeyVal() returns the operation, key, val for a given
// logical entry position in the segment.
func (a *segment) GetOperationKeyVal(pos int) (uint64, []byte, []byte) {
	x := pos * 2
	if x >= len(a.kvs) {
		return 0, nil, nil
	}

	opklvl := a.kvs[x]
	kstart := int(a.kvs[x+1])
	operation, keyLen, valLen := decodeOpKeyLenValLen(opklvl)
	vstart := kstart + keyLen

	return operation, a.buf[kstart:vstart], a.buf[vstart : vstart+valLen]
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
		sort.Sort(a)
		close(a.waitSortedCh) // Signal any waiters.
		return true
	}

	if synchronous {
		<-a.waitSortedCh // Wait for the sorter to be done.
		return true
	}

	return false
}

// ------------------------------------------------------

// Persist persists a basic segment, and allows a segment to meet the
// SegmentPersister interface.
func (seg *segment) Persist(file File) (rv SegmentLoc, err error) {
	finfo, err := file.Stat()
	if err != nil {
		return rv, err
	}
	pos := finfo.Size()

	kvsSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&seg.kvs))

	var kvsBuf []byte
	kvsBufSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&kvsBuf))
	kvsBufSliceHeader.Data = kvsSliceHeader.Data
	kvsBufSliceHeader.Len = kvsSliceHeader.Len * 8
	kvsBufSliceHeader.Cap = kvsSliceHeader.Cap * 8

	kvsPos := pageAlign(pos)
	bufPos := pageAlign(kvsPos + int64(len(kvsBuf)))

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
		Kind:       seg.Kind(),
		KvsOffset:  uint64(kvsPos),
		KvsBytes:   uint64(resMap["kvs"].got),
		BufOffset:  uint64(bufPos),
		BufBytes:   uint64(resMap["buf"].got),
		TotOpsSet:  seg.totOperationSet,
		TotOpsDel:  seg.totOperationDel,
		TotKeyByte: seg.totKeyByte,
		TotValByte: seg.totValByte,
	}, nil
}

// ------------------------------------------------------

// loadBasicSegment loads a basic segment.
func loadBasicSegment(sloc *SegmentLoc, kvs []uint64, buf []byte) (Segment, error) {
	return &segment{
		kvs:             kvs,
		buf:             buf,
		totOperationSet: sloc.TotOpsSet,
		totOperationDel: sloc.TotOpsDel,
		totKeyByte:      sloc.TotKeyByte,
		totValByte:      sloc.TotValByte,
	}, nil
}
