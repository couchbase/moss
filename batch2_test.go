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
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
)

// TestIteratorSingleManyDeletionsNoStackOverflow builds a single
// segment with a huge run of consecutive deletion tombstones between
// two live Sets, then does the single Next() that must skip them all.
// With the old recursive Next() this overflowed the goroutine stack;
// the loop version runs in O(1) stack.  A reduced max-stack makes the
// regression deterministic: recursion would exceed it and crash.
func TestIteratorSingleManyDeletionsNoStackOverflow(t *testing.T) {
	const nDeletes = 200000

	prev := debug.SetMaxStack(1 << 20) // 1 MiB; ample for the loop, not recursion.
	defer debug.SetMaxStack(prev)

	// buf: byte 0 is the shared key, byte 1 is the shared value.
	buf := []byte("kv")
	entries := make([]entry, 0, nDeletes+2)
	entries = append(entries, entry{OperationSet, 1, 1, 0}) // pos 0: live
	for i := 0; i < nDeletes; i++ {
		entries = append(entries, entry{OperationDel, 1, 0, 0})
	}
	entries = append(entries, entry{OperationSet, 1, 1, 0}) // last: live
	seg := makeSegment(buf, entries...)

	cur, err := seg.Cursor(nil, nil)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	iter := &iteratorSingle{s: seg, sc: cur}
	iter.op, iter.k, iter.v = cur.Current()

	// Positioned at the first live entry.
	if _, _, err := iter.Current(); err != nil {
		t.Fatalf("initial Current err: %v", err)
	}

	// This single Next() skips all nDeletes tombstones in one call.
	if err := iter.Next(); err != nil {
		t.Fatalf("Next() over %d deletions err: %v", nDeletes, err)
	}
	if _, _, err := iter.Current(); err != nil {
		t.Fatalf("Current after skip err: %v; expected the trailing live entry", err)
	}

	// Now exhausted.
	if err := iter.Next(); err != ErrIteratorDone {
		t.Fatalf("final Next() err = %v; want ErrIteratorDone", err)
	}
}

type nopWriterAt struct{}

func (nopWriterAt) WriteAt(p []byte, off int64) (int, error) { return len(p), nil }

// TestBufferedSectionWriterMaxBytes verifies the section-writer bound
// that the compaction kvs writer now relies on (A9): a write that would
// exceed max returns io.ErrShortBuffer instead of overrunning into the
// adjacent (buf) section.
func TestBufferedSectionWriterMaxBytes(t *testing.T) {
	w := newBufferedSectionWriter(nopWriterAt{}, 0, 10, 4096, nil)
	defer w.Stop()

	if n, err := w.Write(make([]byte, 8)); err != nil || n != 8 {
		t.Fatalf("Write(8) = %d, %v; want 8, nil", n, err)
	}
	// 8 + 5 = 13 > max(10) -> must be refused.
	if _, err := w.Write(make([]byte, 5)); err != io.ErrShortBuffer {
		t.Fatalf("over-max Write err = %v; want io.ErrShortBuffer", err)
	}
	// A write that exactly reaches max is allowed.
	if _, err := w.Write(make([]byte, 2)); err != nil {
		t.Fatalf("Write to exactly max err = %v; want nil", err)
	}
}

// TestScanFooterSkipsCorruptTrailingFooter persists a real store, then
// appends a page whose header has valid magic+version but an
// implausible length.  ScanFooter must skip that torn footer and
// recover the previous good one, rather than panicking on a
// negative/huge make() (A4).
func TestScanFooterSkipsCorruptTrailingFooter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mossScanFooter")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Persist one good footer containing key "k" -> "v".
	store, err := OpenStore(tmpDir, DefaultStoreOptions)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := NewCollection(DefaultCollectionOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := coll.Start(); err != nil {
		t.Fatal(err)
	}
	b, _ := coll.NewBatch(1, 8)
	_ = b.Set([]byte("k"), []byte("v"))
	if err := coll.ExecuteBatch(b, WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	b.Close()
	ss, err := coll.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	llss, err := store.Persist(ss, StorePersistOptions{})
	if err != nil || llss == nil {
		t.Fatalf("Persist: %v", err)
	}
	ss.Close()
	llss.Close()
	coll.Close()
	store.Close()

	// Locate the data file.
	dents, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	var dataFile string
	for _, d := range dents {
		if filepath.Ext(d.Name()) == StoreSuffix {
			dataFile = filepath.Join(tmpDir, d.Name())
		}
	}
	if dataFile == "" {
		t.Fatal("no .moss data file found")
	}

	// Append a corrupt trailing footer page: valid magic-beg + valid
	// version + implausibly small length (1), then pad to a full page.
	f, err := os.OpenFile(dataFile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := f.Stat()
	p := pageAlignCeil(fi.Size())

	var hdr bytes.Buffer
	hdr.Write(StoreMagicBeg)
	hdr.Write(StoreMagicBeg)
	_ = binary.Write(&hdr, StoreEndian, uint32(StoreVersion))
	_ = binary.Write(&hdr, StoreEndian, uint32(1)) // implausible length

	page := make([]byte, StorePageSize)
	copy(page, hdr.Bytes())
	if _, err := f.WriteAt(page, p); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reopen: must not panic, and must recover "k" -> "v" from the good
	// footer below the corrupt trailing page.
	store2, coll2, err := OpenStoreCollection(tmpDir, DefaultStoreOptions,
		StorePersistOptions{})
	if err != nil {
		t.Fatalf("reopen failed (ScanFooter did not recover): %v", err)
	}
	defer store2.Close()
	defer coll2.Close()

	ss2, err := coll2.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer ss2.Close()
	v, err := ss2.Get([]byte("k"), ReadOptions{})
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(v) != "v" {
		t.Fatalf("recovered value = %q, want %q", v, "v")
	}
}
