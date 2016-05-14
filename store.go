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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

var STORE_PREFIX = "data-"
var STORE_SUFFIX = ".moss"

var STORE_ENDIAN = binary.LittleEndian
var STORE_PAGE_SIZE = 4096

var STORE_VERSION = uint32(1)
var STORE_MAGIC_BEG []byte = []byte("0m1o2s")
var STORE_MAGIC_END []byte = []byte("3s4p5s")

var lenMagicBeg int = len(STORE_MAGIC_BEG)
var lenMagicEnd int = len(STORE_MAGIC_END)

// footerBegLen includes STORE_VERSION(uint32) & footerLen(uint32).
var footerBegLen int = lenMagicBeg + lenMagicBeg + 4 + 4

// footerEndLen includes footerOffset(int64) & footerLen(uint32) again.
var footerEndLen int = 8 + 4 + lenMagicEnd + lenMagicEnd

// --------------------------------------------------------

// Store represents data persisted in a directory.
type Store struct {
	dir     string
	options *StoreOptions

	m            sync.Mutex // Protects the fields that follow.
	footer       *Footer
	nextFNameSeq int64
}

type StoreOptions struct {
	// OpenFile allows apps to optionally provide their own file
	// opening implementation.  Defaults to os.OpenFile().
	OpenFile OpenFile `json:"-"`

	// Log is a callback invoked when store needs to log a debug
	// message.  Optional, may be nil.
	Log func(format string, a ...interface{}) `json:"-"`
}

type StorePersistOptions struct {
}

// Header represents the JSON stored at the head of a file.
type Header struct {
	Version         uint32 // The file format / STORE_VERSION.
	CreatedAt       string
	CreatedPageSize int
}

// Footer represents a footer record persisted in a file, and
// implements the moss.Snapshot interface.
type Footer struct {
	SegmentLocs []SegmentLoc // Older SegmentLoc's come first.

	fref *FileRef     `json:"-"`
	ss   segmentStack `json:"-"`
}

// SegmentLoc represents a persisted segment.
type SegmentLoc struct {
	KvsOffset uint64 // Byte offset within the file.
	KvsBytes  uint64 // Number of bytes for the persisted segment.kvs.

	BufOffset uint64 // Byte offset within the file.
	BufBytes  uint64 // Number of bytes for the persisted segment.buf.

	TotOpsSet  uint64
	TotOpsDel  uint64
	TotKeyByte uint64
	TotValByte uint64
}

// TotOps returns number of ops in a segment loc.
func (sloc *SegmentLoc) TotOps() int { return int(sloc.KvsBytes / 8 / 2) }

// --------------------------------------------------------

// OpenStore returns a store instance for a directory.  An empty
// directory results in an empty store.
func OpenStore(dir string, options StoreOptions) (*Store, error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var maxFNameSeq int64

	var fnames []string
	for _, fileInfo := range fileInfos {
		fname := fileInfo.Name()
		if strings.HasPrefix(fname, STORE_PREFIX) &&
			strings.HasSuffix(fname, STORE_SUFFIX) {
			fnames = append(fnames, fname)
		}

		fnameSeq, err := ParseFNameSeq(fname)
		if err == nil && fnameSeq > maxFNameSeq {
			maxFNameSeq = fnameSeq
		}
	}

	if options.OpenFile == nil {
		options.OpenFile =
			func(name string, flag int, perm os.FileMode) (File, error) {
				return os.OpenFile(name, flag, perm)
			}
	}

	if len(fnames) <= 0 {
		return &Store{dir: dir, options: &options, nextFNameSeq: 1}, nil
	}

	sort.Strings(fnames)
	for i := len(fnames) - 1; i >= 0; i-- {
		file, err := options.OpenFile(path.Join(dir, fnames[i]), os.O_RDWR, 0600)
		if err != nil {
			continue
		}

		footer, err := ReadFooter(file) // The footer owns the file on success.
		if err != nil {
			file.Close()
			continue
		}

		return &Store{
			dir:          dir,
			options:      &options,
			footer:       footer,
			nextFNameSeq: maxFNameSeq + 1,
		}, nil
	}

	return nil, fmt.Errorf("store: could not successfully open/parse any file")
}

func (s *Store) Dir() string {
	return s.dir
}

func (s *Store) Options() StoreOptions {
	return *s.options
}

func (s *Store) Snapshot() (Snapshot, error) {
	s.m.Lock()
	footer := s.footer
	if footer != nil {
		footer.fref.AddRef()
	} else {
		footer = &Footer{}
	}
	s.m.Unlock()
	return footer, nil
}

func (s *Store) Close() error {
	s.m.Lock()
	footer := s.footer
	s.footer = nil
	s.m.Unlock()

	if footer != nil {
		return footer.Close()
	}
	return nil
}

// --------------------------------------------------------

// Persist helps the store implement the lower-level-update func.
func (s *Store) Persist(higher Snapshot, persistOptions StorePersistOptions) (
	Snapshot, error) {
	ss, ok := higher.(*segmentStack)
	if !ok {
		return nil, fmt.Errorf("store: can only persist segmentStack")
	}
	return s.persistSegmentStack(ss)
}

func (s *Store) persistSegmentStack(ss *segmentStack) (Snapshot, error) {
	ss.addRef()
	defer ss.decRef()

	fref, file, err := s.createOrReuseFile()
	if err != nil {
		return nil, err
	}

	// TODO: Pre-allocate file space up front?
	// TODO: Multiple, concurrent segment persisters?

	ss.ensureSorted(0, len(ss.a)-1)

	segmentLocs := make([]SegmentLoc, 0, len(ss.a))
	for _, segment := range ss.a {
		segmentLoc, err := s.persistSegment(file, segment)
		if err != nil {
			fref.DecRef()
			return nil, err
		}

		segmentLocs = append(segmentLocs, segmentLoc)
	}

	footer, err := initFooter(&Footer{SegmentLocs: segmentLocs, fref: fref}, file)
	if err != nil {
		fref.DecRef()
		return nil, err
	}

	if err = s.persistFooter(file, footer); err != nil {
		fref.DecRef()
		return nil, err
	}

	s.m.Lock()
	if s.footer != nil {
		s.footer.fref.DecRef()
	}
	s.footer = footer
	s.footer.fref.AddRef() // One ref-count held by store.
	s.m.Unlock()

	return footer, nil // The other ref-count returned to caller.
}

// --------------------------------------------------------

// createOrReuseFile either creates a new file or reuses the file from
// the last/current footer.
func (s *Store) createOrReuseFile() (fref *FileRef, file File, err error) {
	s.m.Lock()
	defer s.m.Unlock()

	if s.footer != nil {
		return s.footer.fref, s.footer.fref.AddRef(), nil
	}

	fname := FormatFName(s.nextFNameSeq)
	s.nextFNameSeq++

	if file, err = s.options.OpenFile(path.Join(s.dir, fname),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600); err != nil {
		return nil, nil, err
	}

	if err = s.persistHeader(file); err != nil {
		file.Close()
		os.Remove(path.Join(s.dir, fname))
		return nil, nil, err
	}

	return &FileRef{file: file, refs: 1}, file, err
}

func (s *Store) persistHeader(file File) error {
	buf, err := json.Marshal(Header{
		Version:         STORE_VERSION,
		CreatedAt:       time.Now().Format(time.RFC3339),
		CreatedPageSize: os.Getpagesize(),
	})
	if err != nil {
		return err
	}

	str := "moss-data-store:\n" + string(buf) + "\n"
	str = str + strings.Repeat(" ", STORE_PAGE_SIZE-len(str))
	n, err := file.WriteAt([]byte(str), 0)
	if err != nil {
		return err
	}
	if n != len(str) {
		return fmt.Errorf("store: could not write full header")
	}

	return nil
}

// --------------------------------------------------------

func (s *Store) persistSegment(file File, segIn Segment) (rv SegmentLoc, err error) {
	seg, ok := segIn.(*segment)
	if !ok {
		return rv, fmt.Errorf("store: can only persist segment type")
	}

	finfo, err := file.Stat()
	if err != nil {
		return rv, err
	}
	pos := finfo.Size()

	kvsSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&seg.kvs))

	var kvsBuf []byte
	kvsBufSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&kvsBuf))
	kvsBufSliceHeader.Data = kvsSliceHeader.Data
	kvsBufSliceHeader.Len = kvsSliceHeader.Len * 8
	kvsBufSliceHeader.Cap = kvsSliceHeader.Cap * 8

	kvsPos := pageAlign(pos)
	kvsWritten, err := file.WriteAt(kvsBuf, kvsPos)
	if err != nil {
		return rv, err
	}
	if kvsWritten != len(kvsBuf) {
		return rv, fmt.Errorf("store: persistSegment error writing all kvs")
	}

	bufPos := pageAlign(kvsPos + int64(kvsWritten))
	bufWritten, err := file.WriteAt(seg.buf, bufPos)
	if err != nil {
		return rv, err
	}
	if bufWritten != len(seg.buf) {
		return rv, fmt.Errorf("store: persistSegment error writing all buf")
	}

	return SegmentLoc{
		KvsOffset:  uint64(kvsPos),
		KvsBytes:   uint64(kvsWritten),
		BufOffset:  uint64(bufPos),
		BufBytes:   uint64(bufWritten),
		TotOpsSet:  seg.totOperationSet,
		TotOpsDel:  seg.totOperationDel,
		TotKeyByte: seg.totKeyByte,
		TotValByte: seg.totValByte,
	}, nil
}

// --------------------------------------------------------

func (s *Store) persistFooter(file File, footer *Footer) error {
	jBuf, err := json.Marshal(footer)
	if err != nil {
		return err
	}

	finfo, err := file.Stat()
	if err != nil {
		return err
	}

	footerPos := pageAlign(finfo.Size())
	footerLen := footerBegLen + len(jBuf) + footerEndLen

	footerBuf := bytes.NewBuffer(make([]byte, 0, footerLen))
	footerBuf.Write(STORE_MAGIC_BEG)
	footerBuf.Write(STORE_MAGIC_BEG)
	binary.Write(footerBuf, STORE_ENDIAN, uint32(STORE_VERSION))
	binary.Write(footerBuf, STORE_ENDIAN, uint32(footerLen))
	footerBuf.Write(jBuf)
	binary.Write(footerBuf, STORE_ENDIAN, footerPos)
	binary.Write(footerBuf, STORE_ENDIAN, uint32(footerLen))
	footerBuf.Write(STORE_MAGIC_END)
	footerBuf.Write(STORE_MAGIC_END)

	footerWritten, err := file.WriteAt(footerBuf.Bytes(), footerPos)
	if err != nil {
		return err
	}
	if footerWritten != len(footerBuf.Bytes()) {
		return fmt.Errorf("store: persistFooter error writing all footerBuf")
	}

	return nil
}

// --------------------------------------------------------

// ReadFooter reads the Footer from a file.
func ReadFooter(file File) (*Footer, error) {
	finfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return ScanFooter(file, finfo.Size())
}

// ScanFooter scans a file backwards for a valid Footer.
func ScanFooter(file File, pos int64) (*Footer, error) {
	footerEnd := make([]byte, footerEndLen)
	for {
		for { // Scan backwards for STORE_MAGIC_END, which might be a potential footer.
			if pos <= int64(footerBegLen+footerEndLen) {
				return nil, fmt.Errorf("store: no valid footer found")
			}
			n, err := file.ReadAt(footerEnd, pos-int64(footerEndLen))
			if err != nil {
				return nil, err
			}
			if n != footerEndLen {
				return nil, fmt.Errorf("store: read footer too small")
			}
			if bytes.Equal(STORE_MAGIC_END, footerEnd[8+4:8+4+lenMagicEnd]) &&
				bytes.Equal(STORE_MAGIC_END, footerEnd[8+4+lenMagicEnd:]) {
				break
			}

			pos-- // TODO: optimizations to scan backwards faster.
		}

		// Read and check the potential footer.
		footerEndBuf := bytes.NewBuffer(footerEnd)
		var offset int64
		if err := binary.Read(footerEndBuf, STORE_ENDIAN, &offset); err != nil {
			return nil, err
		}
		var length uint32
		if err := binary.Read(footerEndBuf, STORE_ENDIAN, &length); err != nil {
			return nil, err
		}
		if offset > 0 && offset < pos-int64(footerBegLen+footerEndLen) &&
			length == uint32(pos-offset) {
			data := make([]byte, pos-offset-int64(footerEndLen))
			n, err := file.ReadAt(data, offset)
			if err != nil {
				return nil, err
			}
			if n != len(data) {
				return nil, fmt.Errorf("store: read footer data too small")
			}
			if bytes.Equal(STORE_MAGIC_BEG, data[:lenMagicBeg]) &&
				bytes.Equal(STORE_MAGIC_BEG, data[lenMagicBeg:2*lenMagicBeg]) {
				b := bytes.NewBuffer(data[2*lenMagicBeg:])
				var version uint32
				if err := binary.Read(b, STORE_ENDIAN, &version); err != nil {
					return nil, err
				}
				if version != STORE_VERSION {
					return nil, fmt.Errorf("store: version mismatch, "+
						"current version: %v != found version: %v", STORE_VERSION, version)
				}
				var length0 uint32
				if err := binary.Read(b, STORE_ENDIAN, &length0); err != nil {
					return nil, err
				}
				if length0 != length {
					return nil, fmt.Errorf("store: length mismatch, "+
						"wanted length: %v != found length: %v", length0, length)
				}
				m := &Footer{fref: &FileRef{file: file, refs: 1}}
				if err := json.Unmarshal(data[2*lenMagicBeg+4+4:], m); err != nil {
					return nil, err
				}
				return initFooter(m, file)
			} // Else, perhaps file was unlucky in having STORE_MAGIC_END's.
		} // Else, perhaps a persist file was stored in a file.

		pos-- // Footer was invalid, so keep scanning.
	}
}

// --------------------------------------------------------

// initFooter initializes the Footer instance.
func initFooter(f *Footer, file File) (*Footer, error) {
	osFile := ToOsFile(file)
	if osFile == nil {
		return nil, fmt.Errorf("store: initFooter could not convert to os.File")
	}

	mm, err := mmap.Map(osFile, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("store: initFooter mmap.Map(), err: %v", err)
	}

	f.fref.OnClose(func() { mm.Unmap() })

	f.ss.a = make([]Segment, len(f.SegmentLocs))
	for i := range f.SegmentLocs {
		sloc := &f.SegmentLocs[i]

		kvsBytes := mm[sloc.KvsOffset : sloc.KvsOffset+sloc.KvsBytes]
		kvsBytesSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&kvsBytes))

		var kvs []uint64
		kvsSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&kvs))
		kvsSliceHeader.Data = kvsBytesSliceHeader.Data
		kvsSliceHeader.Len = kvsBytesSliceHeader.Len / 8
		kvsSliceHeader.Cap = kvsSliceHeader.Len

		f.ss.a[i] = &segment{
			kvs: kvs,
			buf: mm[sloc.BufOffset : sloc.BufOffset+sloc.BufBytes],

			totOperationSet: sloc.TotOpsSet,
			totOperationDel: sloc.TotOpsDel,
			totKeyByte:      sloc.TotKeyByte,
			totValByte:      sloc.TotValByte,
		}
	}
	f.ss.refs = 1

	return f, nil
}

func (f *Footer) Close() error {
	return f.fref.DecRef()
}

// Get retrieves a val from the Snapshot, and will return nil val
// if the entry does not exist in the Snapshot.
func (f *Footer) Get(key []byte, readOptions ReadOptions) ([]byte, error) {
	f.fref.AddRef()
	rv, err := f.ss.Get(key, readOptions)
	if err == nil {
		rv = append([]byte(nil), rv...) // Copy.
	}
	f.fref.DecRef()
	return rv, err
}

// StartIterator returns a new Iterator instance on this Snapshot.
//
// On success, the returned Iterator will be positioned so that
// Iterator.Current() will either provide the first entry in the
// range or ErrIteratorDone.
//
// A startKeyInclusive of nil means the logical "bottom-most"
// possible key and an endKeyExclusive of nil means the logical
// "top-most" possible key.
func (f *Footer) StartIterator(startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	f.fref.AddRef()
	iter, err := f.ss.StartIterator(startKeyInclusive, endKeyExclusive, iteratorOptions)
	if err != nil {
		f.fref.DecRef()
		return nil, err
	}
	return &iteratorWrapper{iter: iter, closer: f.fref}, nil
}

// --------------------------------------------------------

// ParseFNameSeq parses a file name like "data-0000123.moss" into 123.
func ParseFNameSeq(fname string) (int64, error) {
	seqStr := fname[len(STORE_PREFIX) : len(fname)-len(STORE_SUFFIX)]
	return strconv.ParseInt(seqStr, 10, 64)
}

// FormatFName returns a file name like "data-0000123.moss" given a seq of 123.
func FormatFName(seq int64) string {
	return fmt.Sprintf("%s%016d%s", STORE_PREFIX, seq, STORE_SUFFIX)
}

// --------------------------------------------------------

// pageAlign returns the pos bumped up to multiple of STORE_PAGE_SIZE.
func pageAlign(pos int64) int64 {
	rem := pos % int64(STORE_PAGE_SIZE)
	if rem != 0 {
		return pos + int64(STORE_PAGE_SIZE) - rem
	}
	return pos
}
