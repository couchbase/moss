//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"testing"
)

// TestUint64ByteSliceRoundTrip exercises the []uint64 <-> []byte
// conversions.  It is intentionally NOT build-tagged so it runs
// against both the default (unsafe) implementation in slice_util.go
// and the `-tags safe` implementation in slice_util_safe.go, guarding
// against regressions like the broken `safe` build (which referenced
// the undefined STORE_ENDIAN identifier).
func TestUint64ByteSliceRoundTrip(t *testing.T) {
	cases := [][]uint64{
		{},
		{0},
		{1, 2, 3, 4, 5},
		{0xdeadbeefcafef00d, 0, 1<<63 + 1, maskKeyLength, maskValLength},
	}

	for _, in := range cases {
		b, err := Uint64SliceToByteSlice(in)
		if err != nil {
			t.Fatalf("Uint64SliceToByteSlice(%v) err: %v", in, err)
		}
		if len(b) != len(in)*8 {
			t.Fatalf("expected %d bytes, got %d", len(in)*8, len(b))
		}

		out, err := ByteSliceToUint64Slice(b)
		if err != nil {
			t.Fatalf("ByteSliceToUint64Slice err: %v", err)
		}
		if len(out) != len(in) {
			t.Fatalf("round-trip length mismatch: got %d, want %d", len(out), len(in))
		}
		for i := range in {
			if out[i] != in[i] {
				t.Fatalf("round-trip mismatch at %d: got %#x, want %#x",
					i, out[i], in[i])
			}
		}
	}
}
