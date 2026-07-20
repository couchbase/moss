//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !safe
// +build !safe

package moss

import (
	"unsafe"
)

// Uint64SliceToByteSlice gives access to []uint64 as []byte.  By
// default, an efficient O(1) implementation of this function is used,
// but which requires the unsafe package.  See the "safe" build tag to
// use an O(N) implementation that does not need the unsafe package.
//
// Uses unsafe.Slice/unsafe.SliceData rather than the deprecated
// reflect.SliceHeader, so the backing array stays reachable by the GC
// (building a SliceHeader field-by-field leaves the data referenced
// only by a uintptr, which the GC does not treat as a live pointer).
func Uint64SliceToByteSlice(in []uint64) ([]byte, error) {
	if len(in) == 0 {
		return nil, nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(in))),
		len(in)*8), nil
}

// ByteSliceToUint64Slice gives access to []byte as []uint64.  By
// default, an efficient O(1) implementation of this function is used,
// but which requires the unsafe package.  See the "safe" build tag to
// use an O(N) implementation that does not need the unsafe package.
//
// NOTE: the input's backing array must be 8-byte aligned (moss only
// calls this on page-aligned mmap'd regions), and its length should be
// a multiple of 8; any trailing bytes are ignored.
func ByteSliceToUint64Slice(in []byte) ([]uint64, error) {
	if len(in) == 0 {
		return nil, nil
	}
	return unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(in))),
		len(in)/8), nil
}

// --------------------------------------------------------------

func endian() string { // See golang-nuts / how-to-tell-endian-ness-of-machine,
	var x uint32 = 0x01020304
	if *(*byte)(unsafe.Pointer(&x)) == 0x01 {
		return "big"
	}
	return "little"
}
