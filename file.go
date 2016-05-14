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
	"io"
	"os"
	"sync"
)

// The File interface is implemented by os.File.  App specific
// implementations may add concurrency, caching, stats, fuzzing, etc.
type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

// The OpenFile func signature is similar to os.OpenFile().
type OpenFile func(name string, flag int, perm os.FileMode) (File, error)

// FileRef provides a ref-counting wrapper around a File.
type FileRef struct {
	file File
	m    sync.Mutex // Protects the fields that follow.
	cbs  []func()   // Optional callbacks invoked before final close.
	refs int
}

// OnClose registers event callback func's that are invoked before the
// file is closed.
func (r *FileRef) OnClose(cb func()) {
	r.m.Lock()
	r.cbs = append(r.cbs, cb)
	r.m.Unlock()
}

// AddRef increases the ref-count on the file ref.
func (r *FileRef) AddRef() File {
	if r == nil {
		return nil
	}

	r.m.Lock()
	r.refs++
	file := r.file
	r.m.Unlock()

	return file
}

// DecRef decreases the ref-count on the file ref, and closing the
// underlying file when the ref-count reaches zero.
func (r *FileRef) DecRef() (err error) {
	if r == nil {
		return nil
	}

	r.m.Lock()
	r.refs--
	if r.refs <= 0 {
		for _, cb := range r.cbs {
			cb()
		}
		r.cbs = nil
		err = r.file.Close()
		r.file = nil
	}
	r.m.Unlock()

	return err
}

func (r *FileRef) Close() error {
	return r.DecRef()
}

// OsFile interface let's one convert from a File to an os.File.
type OsFile interface {
	OsFile() *os.File
}

// ToOsFile provides the underlying os.File for a File, if available.
func ToOsFile(f File) *os.File {
	if osFile, ok := f.(*os.File); ok {
		return osFile
	}
	if osFile2, ok := f.(OsFile); ok {
		return osFile2.OsFile()
	}
	return nil
}
