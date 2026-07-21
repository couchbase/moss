//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"errors"
	"testing"
)

func TestSegmentKeyValueSizeLimits(t *testing.T) {
	s, _ := newSegment(100, 200)
	err := s.mutateEx(150, 0, 150, 125)
	if err != nil {
		t.Errorf("expected a segment")
	}
	err = s.mutateEx(150, 0, maxKeyLength, maxValLength)
	if err != nil {
		t.Errorf("expected a segment")
	}
	err = s.mutateEx(150, 0, maxKeyLength+1, 125)
	if err != ErrKeyTooLarge {
		t.Errorf("should have erred for large key")
	}
	err = s.mutateEx(150, 0, 100, maxValLength+1)
	if err != ErrValueTooLarge {
		t.Errorf("should have erred for large value")
	}
	err = s.mutateEx(150, 0, maxKeyLength+1, maxValLength+1)
	if err != ErrKeyTooLarge {
		t.Errorf("should have erred for large key")
	}
}

// entry is a helper describing one logical segment entry to encode.
type entry struct {
	op     uint64
	keyLen int
	valLen int
	kbeg   int // start offset into buf
}

// makeSegment builds a *segment directly from raw entries and a raw
// buf, WITHOUT validating that offsets/lengths actually fit within
// buf.  This lets tests simulate a corrupt or truncated (e.g. mmap'd)
// segment whose footer/index claims more bytes than buf holds.
func makeSegment(buf []byte, entries ...entry) *segment {
	kvs := make([]uint64, 0, len(entries)*2)
	for _, e := range entries {
		kvs = append(kvs,
			encodeOpKeyLenValLen(e.op, e.keyLen, e.valLen),
			uint64(e.kbeg))
	}
	return &segment{kvs: kvs, buf: buf}
}

// mustNotPanic runs fn and fails the test (rather than crashing the
// process) if it panics.
func mustNotPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s panicked (would SIGBUS on an mmap'd buf): %v", name, r)
		}
	}()
	fn()
}

// TestSegmentKeyAtBounds verifies the bounds-checked decode helper.
func TestSegmentKeyAtBounds(t *testing.T) {
	// Two valid entries "a" @ [0:1] and "b" @ [1:2] in a 2-byte buf.
	seg := makeSegment([]byte("ab"),
		entry{OperationSet, 1, 0, 0},
		entry{OperationSet, 1, 0, 1},
	)

	if k, err := seg.keyAt(0); err != nil || string(k) != "a" {
		t.Fatalf("keyAt(0) = %q, %v; want \"a\", nil", k, err)
	}
	if k, err := seg.keyAt(1); err != nil || string(k) != "b" {
		t.Fatalf("keyAt(1) = %q, %v; want \"b\", nil", k, err)
	}

	// Out-of-range logical positions must error, not panic.
	for _, pos := range []int{-1, 2, 100} {
		if _, err := seg.keyAt(pos); !errors.Is(err, ErrSegmentCorrupted) {
			t.Fatalf("keyAt(%d) err = %v; want ErrSegmentCorrupted", pos, err)
		}
	}

	// A key whose kbeg+keyLen runs past buf (truncation) must error.
	trunc := makeSegment([]byte("a"), entry{OperationSet, 100, 0, 0})
	if _, err := trunc.keyAt(0); !errors.Is(err, ErrSegmentCorrupted) {
		t.Fatalf("keyAt on truncated buf err = %v; want ErrSegmentCorrupted", err)
	}
}

// TestSegmentFindPosCorruptFirstKey covers the entry-0 decode that the
// pre-fix findStartKeyInclusivePos performed with no bounds check.
func TestSegmentFindPosCorruptFirstKey(t *testing.T) {
	// One entry claiming a 100-byte key, but buf holds only 1 byte.
	seg := makeSegment([]byte("a"), entry{OperationSet, 100, 0, 0})

	mustNotPanic(t, "findStartKeyInclusivePos", func() {
		_, err := seg.findStartKeyInclusivePos([]byte("z"))
		if !errors.Is(err, ErrSegmentCorrupted) {
			t.Fatalf("err = %v; want ErrSegmentCorrupted", err)
		}
	})

	mustNotPanic(t, "findKeyPos", func() {
		_, err := seg.findKeyPos([]byte("z"))
		if !errors.Is(err, ErrSegmentCorrupted) {
			t.Fatalf("err = %v; want ErrSegmentCorrupted", err)
		}
	})
}

// TestSegmentFindPosCorruptMidKey ensures the binary-search loop (not
// just the first-key fast path) is guarded.
func TestSegmentFindPosCorruptMidKey(t *testing.T) {
	// Entries 0 ("a") and 2 ("c") are valid; entry 1 points past buf.
	seg := makeSegment([]byte("ac"),
		entry{OperationSet, 1, 0, 0},    // "a" @ [0:1]
		entry{OperationSet, 5, 0, 1000}, // corrupt: kbeg=1000
		entry{OperationSet, 1, 0, 1},    // "c" @ [1:2]
	)

	mustNotPanic(t, "findStartKeyInclusivePos mid", func() {
		_, err := seg.findStartKeyInclusivePos([]byte("b"))
		if !errors.Is(err, ErrSegmentCorrupted) {
			t.Fatalf("err = %v; want ErrSegmentCorrupted", err)
		}
	})

	mustNotPanic(t, "Cursor mid", func() {
		_, err := seg.Cursor([]byte("b"), nil)
		if !errors.Is(err, ErrSegmentCorrupted) {
			t.Fatalf("Cursor err = %v; want ErrSegmentCorrupted", err)
		}
	})
}

// TestSegmentGetOperationKeyValCorrupt ensures the cursor Current()
// decode path can't SIGBUS/panic on a corrupt entry.
func TestSegmentGetOperationKeyValCorrupt(t *testing.T) {
	seg := makeSegment([]byte("ac"),
		entry{OperationSet, 1, 0, 0},    // valid "a"
		entry{OperationSet, 5, 0, 1000}, // corrupt
	)

	// Valid entry decodes normally.
	if op, k, _ := seg.getOperationKeyVal(0); op != OperationSet || string(k) != "a" {
		t.Fatalf("getOperationKeyVal(0) = %x,%q; want Set,\"a\"", op, k)
	}

	// Corrupt entry and out-of-range positions return zero values, no panic.
	mustNotPanic(t, "getOperationKeyVal corrupt", func() {
		if op, k, v := seg.getOperationKeyVal(1); op != 0 || k != nil || v != nil {
			t.Fatalf("corrupt getOperationKeyVal(1) = %x,%q,%q; want 0,nil,nil", op, k, v)
		}
		if op, k, v := seg.getOperationKeyVal(99); op != 0 || k != nil || v != nil {
			t.Fatalf("out-of-range getOperationKeyVal = %x,%q,%q; want 0,nil,nil", op, k, v)
		}
	})
}

// TestSegmentFindKeyPosOffByOne guards against regression of the
// x+1 > len(kvs) off-by-one: keyAt at exactly Len() must error, and a
// valid segment's last entry must still decode.
func TestSegmentFindKeyPosOffByOne(t *testing.T) {
	seg := makeSegment([]byte("ab"),
		entry{OperationSet, 1, 0, 0},
		entry{OperationSet, 1, 0, 1},
	)
	// Len() == 2; last valid position is 1.
	if _, err := seg.keyAt(seg.Len() - 1); err != nil {
		t.Fatalf("keyAt(Len()-1) err = %v; want nil", err)
	}
	if _, err := seg.keyAt(seg.Len()); !errors.Is(err, ErrSegmentCorrupted) {
		t.Fatalf("keyAt(Len()) err = %v; want ErrSegmentCorrupted", err)
	}
}
