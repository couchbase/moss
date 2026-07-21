//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"io"
	"os"
	"path"
	"sync"
	"testing"
)

func TestFileRef(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "mossStore")
	defer os.RemoveAll(tmpDir)

	file, _ := os.OpenFile(path.Join(tmpDir, "test.mmap"),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)

	fref := &FileRef{file: file, refs: 1}

	var m sync.Mutex
	x := 0
	after := 0
	fref.OnBeforeClose(func() {
		m.Lock()
		x++
		m.Unlock()
	})
	fref.OnAfterClose(func() {
		m.Lock()
		after++
		m.Unlock()
	})

	file2 := fref.AddRef()

	fref.m.Lock()
	if fref.refs != 2 {
		t.Errorf("expected 2 refs")
	}
	if fref.file != file2 {
		t.Errorf("expected file == file2")
	}
	fref.m.Unlock()

	fref.DecRef()

	m.Lock()
	if x != 0 {
		t.Errorf("expected x 0")
	}
	if after != 0 {
		t.Errorf("expected after 0")
	}
	m.Unlock()

	fref.m.Lock()
	if fref.refs != 1 {
		t.Errorf("expected 1 refs")
	}
	if fref.file != file2 {
		t.Errorf("expected file == file2")
	}
	fref.m.Unlock()

	fref.DecRef()

	m.Lock()
	if x != 1 {
		t.Errorf("expected x 1")
	}
	if after != 1 {
		t.Errorf("expected after 1")
	}
	m.Unlock()

	fref.m.Lock()
	if fref.refs != 0 {
		t.Errorf("expected 1 refs")
	}
	if fref.file != nil {
		t.Errorf("expected file == nil")
	}
	fref.m.Unlock()

	err := file.Close()
	if err == nil {
		t.Errorf("expected re-close to err")
	}

	fref = nil
	if fref.DecRef() != nil {
		t.Errorf("expected DecRef on nil to nil")
	}

	if ToOsFile(file) != file {
		t.Errorf("expected ToOsFile(file) == file")
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
