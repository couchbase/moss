//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
)

func TestFileRef(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossStore")
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
