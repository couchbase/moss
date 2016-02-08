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
	"sort"
)

// newSegment() allocates a segment with hinted amount of resources.
func newSegment(totalOps, totalKeyValBytes int) (
	*segment, error) {
	return &segment{
		kvs: make([]uint64, 0, totalOps*2),
		buf: make([]byte, 0, totalKeyValBytes),
	}, nil
}

// Close releases resources associated with the segment.
func (a *segment) Close() error {
	a.kvs = nil
	a.buf = nil
	return nil
}

// Set copies the key and val bytes into the segment as a "set"
// mutation.  The key must be unique (not repeated) within the
// segment.
func (a *segment) Set(key, val []byte) error {
	return a.mutate(operationSet, key, val)
}

// Del copies the key bytes into the segment as a "deletion" mutation.
// The key must be unique (not repeated) within the segment.
func (a *segment) Del(key []byte) error {
	return a.mutate(operationDel, key, nil)
}

// Merge creates or updates a key-val entry in the Collection via the
// MergeOperator defined in the CollectionOptions.  The key must be
// unique (not repeated) within the segment.
func (a *segment) Merge(key, val []byte) error {
	return a.mutate(operationMerge, key, val)
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

	return a.mutateEx(operationSet,
		keyStart, len(keyFromAlloc), len(valFromAlloc))
}

// AllocDel is like Del(), but the caller must provide []byte
// parameters that came from Alloc(), for less buffer copying.
func (a *segment) AllocDel(keyFromAlloc []byte) error {
	bufCap := cap(a.buf)

	keyStart := bufCap - cap(keyFromAlloc)

	return a.mutateEx(operationDel,
		keyStart, len(keyFromAlloc), 0)
}

// AllocMerge is like Merge(), but the caller must provide []byte
// parameters that came from Alloc(), for less buffer copying.
func (a *segment) AllocMerge(keyFromAlloc, valFromAlloc []byte) error {
	bufCap := cap(a.buf)

	keyStart := bufCap - cap(keyFromAlloc)

	return a.mutateEx(operationMerge,
		keyStart, len(keyFromAlloc), len(valFromAlloc))
}

// ------------------------------------------------------

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

	opKlVl := (maskOperation & operation) |
		(maskKeyLength & (uint64(keyLength) << 32)) |
		(maskValLength & (uint64(valLength)))

	a.kvs = append(a.kvs, opKlVl, uint64(keyStart))

	switch operation {
	case operationSet:
		a.totOperationSet += 1
	case operationDel:
		a.totOperationDel += 1
	case operationMerge:
		a.totOperationMerge += 1
	default:
	}

	a.totKeyByte += uint64(keyLength)
	a.totValByte += uint64(valLength)

	return nil
}

// ------------------------------------------------------

// sort() returns a new, sorted segment from a given segment, where
// the underlying buf bytes might be shared.
func (b *segment) sort() *segment {
	rv := *b // Copy fields.

	rv.kvs = append([]uint64(nil), b.kvs...)

	sort.Sort(&rv)

	return &rv
}

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

// findStartKeyInclusivePos() returns the logical entry position for
// the given (inclusive) start key.  With segment keys of [b, d, f],
// looking for 'c' will return 1.  Looking for 'd' will return 1.
// Looking for 'g' will return 3.  Looking for 'a' will return 0.
func (b *segment) findStartKeyInclusivePos(startKeyInclusive []byte) int {
	n := len(b.kvs) / 2

	return sort.Search(n, func(pos int) bool {
		x := pos * 2

		kLength := int((maskKeyLength & b.kvs[x]) >> 32)
		kStart := int(b.kvs[x+1])
		k := b.buf[kStart : kStart+kLength]

		return bytes.Compare(k, startKeyInclusive) >= 0
	})
}

// getOperationKeyVal() returns the operation, key, val for a given
// logical entry position in the segment.
func (b *segment) getOperationKeyVal(pos int) (
	uint64, []byte, []byte) {
	x := pos * 2
	if x < 0 || x >= len(b.kvs) {
		return 0, nil, nil
	}

	opklvl := b.kvs[x]

	kLength := int((maskKeyLength & opklvl) >> 32)
	vLength := int(maskValLength & opklvl)

	kStart := int(b.kvs[x+1])
	vStart := kStart + kLength

	k := b.buf[kStart : kStart+kLength]
	v := b.buf[vStart : vStart+vLength]

	operation := maskOperation & opklvl

	return operation, k, v
}
