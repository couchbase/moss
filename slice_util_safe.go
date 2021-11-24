//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build safe

package moss

import (
	"bytes"
	"encoding/binary"
)

// Uint64SliceToByteSlice gives access to []uint64 as []byte
func Uint64SliceToByteSlice(in []uint64) ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, len(in)*8))
	err := binary.Write(buffer, STORE_ENDIAN, in)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// ByteSliceToUint64Slice gives access to []byte as []uint64
func ByteSliceToUint64Slice(in []byte) ([]uint64, error) {
	buffer := bytes.NewBuffer(in)

	out := make([]uint64, len(in)/8)
	err := binary.Read(buffer, STORE_ENDIAN, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// --------------------------------------------------------------

func endian() string {
	return "unknown" // Need unsafe package to tell endian'ess.
}
