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
	"runtime"
	"testing"
)

// TestUint64ByteSliceAliasing verifies the default unsafe.Slice-based
// conversion returns an O(1) aliasing view (not a copy) and that the
// backing array stays live across a GC (the reflect.SliceHeader-by-
// uintptr pattern it replaced was not GC-safe).
//
// This is tagged !safe because the "safe" build's binary.Read/Write
// implementation intentionally returns a copy, not an aliasing view.
func TestUint64ByteSliceAliasing(t *testing.T) {
	in := []uint64{0x0102030405060708, 0xffeeddccbbaa9988, 42}

	b, err := Uint64SliceToByteSlice(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != len(in)*8 {
		t.Fatalf("len(b) = %d, want %d", len(b), len(in)*8)
	}

	runtime.GC() // Backing array must remain reachable through b.

	// Aliasing (endian-agnostic): mutating in must be visible through b.
	before := b[0]
	in[0] = ^in[0]
	if b[0] == before {
		t.Fatalf("expected b to alias in's memory (view), but b[0] unchanged")
	}

	out, err := ByteSliceToUint64Slice(b)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	for i := range in {
		if out[i] != in[i] {
			t.Fatalf("round-trip mismatch at %d: %#x != %#x", i, out[i], in[i])
		}
	}
}
