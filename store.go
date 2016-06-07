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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO: Handle endian'ness properly.
// TODO: Better version parsers / checkers / handling.

// --------------------------------------------------------

var STORE_PREFIX = "data-" // File name prefix.
var STORE_SUFFIX = ".moss" // File name suffix.

var STORE_ENDIAN = binary.LittleEndian
var STORE_PAGE_SIZE = 4096

var STORE_VERSION = uint32(2)
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
	refs         int
	footer       *Footer
	nextFNameSeq int64
}

// StoreOptions are provided to OpenStore().
type StoreOptions struct {
	// CollectionOptions should be the same as used with
	// NewCollection().
	CollectionOptions CollectionOptions

	// CompactionPercentage determines when a compaction will run when
	// CompactionConcern is CompactionAllowed.  When the percentage of
	// ops between the non-base level and the base level is greater
	// than CompactionPercentage, then compaction will be run.
	CompactionPercentage float64

	// CompactionBufferPages is the number of pages to use for
	// compaction, where writes are buffered before flushing to disk.
	CompactionBufferPages int

	// CompactionSync of true means perform a file sync at the end of
	// compaction for additional safety.
	CompactionSync bool

	// OpenFile allows apps to optionally provide their own file
	// opening implementation.  When nil, os.OpenFile() is used.
	OpenFile OpenFile `json:"-"`

	// Log is a callback invoked when store needs to log a debug
	// message.  Optional, may be nil.
	Log func(format string, a ...interface{}) `json:"-"`
}

var DefaultStoreOptions = StoreOptions{
	CompactionBufferPages: 512,
}

// StorePersistOptions are provided to Store.Persist().
type StorePersistOptions struct {
	// CompactionConcern controls whether compaction is allowed or
	// forced as part of persistence.
	CompactionConcern CompactionConcern
}

// Header represents the JSON stored at the head of a file, where the
// file header bytes should be less than STORE_PAGE_SIZE length.
type Header struct {
	Version   uint32 // The file format / STORE_VERSION.
	CreatedAt string
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
	Kind string // Used as the key for SegmentLoaders.

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

// A SegmentLoaderFunc is able to load a segment from a SegmentLoc.
type SegmentLoaderFunc func(
	sloc *SegmentLoc, kvs []uint64, buf []byte) (Segment, error)

// SegmentLoaders is a registry of available segment loaders, which
// should be immutable after process init()'ialization.  It is keyed
// by SegmentLoc.Kind.
var SegmentLoaders = map[string]SegmentLoaderFunc{}

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
	for _, fileInfo := range fileInfos { // Find candidate file names.
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
		emptyFooter := &Footer{}
		emptyFooter.ss.options = &options.CollectionOptions

		return &Store{
			dir:          dir,
			options:      &options,
			refs:         1,
			footer:       emptyFooter,
			nextFNameSeq: 1,
		}, nil
	}

	sort.Strings(fnames)
	for i := len(fnames) - 1; i >= 0; i-- {
		file, err := options.OpenFile(path.Join(dir, fnames[i]), os.O_RDWR, 0600)
		if err != nil {
			continue
		}

		footer, err := ReadFooter(&options, file) // The footer owns the file on success.
		if err != nil {
			file.Close()
			continue
		}

		return &Store{
			dir:          dir,
			options:      &options,
			refs:         1,
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
	return *s.options // Copy.
}

func (s *Store) Snapshot() (Snapshot, error) {
	return s.snapshot()
}

func (s *Store) snapshot() (*Footer, error) {
	s.m.Lock()
	footer := s.footer
	if footer != nil {
		footer.fref.AddRef()
	}
	s.m.Unlock()
	return footer, nil
}

func (s *Store) AddRef() {
	s.m.Lock()
	s.refs++
	s.m.Unlock()
}

func (s *Store) Close() error {
	s.m.Lock()

	s.refs--
	if s.refs > 0 {
		s.m.Unlock()
		return nil
	}

	footer := s.footer
	s.footer = nil

	s.m.Unlock()

	return footer.Close()
}

// --------------------------------------------------------

// Persist helps the store implement the lower-level-update func.  The
// higher snapshot may be nil.
func (s *Store) Persist(higher Snapshot, persistOptions StorePersistOptions) (
	Snapshot, error) {
	wasCompacted, err := s.compactMaybe(higher, persistOptions)
	if err != nil {
		return nil, err
	}
	if wasCompacted {
		return s.Snapshot()
	}

	// If we weren't compacted, perform a normal persist operation.
	if higher == nil {
		return s.Snapshot()
	}
	ss, ok := higher.(*segmentStack)
	if !ok {
		return nil, fmt.Errorf("store: can only persist segmentStack")
	}
	return s.persistSegmentStack(ss)
}

func (s *Store) persistSegmentStack(ss *segmentStack) (Snapshot, error) {
	ss.addRef()
	defer ss.decRef()

	fref, file, err := s.startOrReuseFile()
	if err != nil {
		return nil, err
	}

	// TODO: Pre-allocate file space up front?

	ss.ensureSorted(0, len(ss.a)-1)

	numSegmentLocs := len(ss.a)

	s.m.Lock()
	if s.footer != nil {
		numSegmentLocs += len(s.footer.SegmentLocs)
	}
	segmentLocs := make([]SegmentLoc, 0, numSegmentLocs)
	if s.footer != nil {
		segmentLocs = append(segmentLocs, s.footer.SegmentLocs...)
	}
	s.m.Unlock()

	for _, segment := range ss.a {
		segmentLoc, err := s.persistSegment(file, segment)
		if err != nil {
			fref.DecRef()
			return nil, err
		}

		segmentLocs = append(segmentLocs, segmentLoc)
	}

	footer, err := loadFooterSegments(s.options,
		&Footer{SegmentLocs: segmentLocs, fref: fref}, file)
	if err != nil {
		fref.DecRef()
		return nil, err
	}

	if err = s.persistFooter(file, footer); err != nil {
		fref.DecRef()
		return nil, err
	}

	s.m.Lock()
	footerPrev := s.footer
	s.footer = footer
	s.footer.fref.AddRef() // One ref-count held by store.
	s.m.Unlock()

	if footerPrev != nil {
		footerPrev.fref.DecRef()
	}

	return footer, nil // The other ref-count returned to caller.
}

// --------------------------------------------------------

// startOrReuseFile either creates a new file or reuses the file from
// the last/current footer.
func (s *Store) startOrReuseFile() (fref *FileRef, file File, err error) {
	s.m.Lock()
	defer s.m.Unlock()

	if s.footer != nil && s.footer.fref != nil {
		return s.footer.fref, s.footer.fref.AddRef(), nil
	}

	return s.startFileLOCKED()
}

func (s *Store) startFileLOCKED() (*FileRef, File, error) {
	fname, file, err := s.createNextFileLOCKED()
	if err != nil {
		return nil, nil, err
	}

	if err = s.persistHeader(file); err != nil {
		file.Close()
		os.Remove(path.Join(s.dir, fname))
		return nil, nil, err
	}

	return &FileRef{file: file, refs: 1}, file, nil
}

func (s *Store) createNextFileLOCKED() (string, File, error) {
	fname := FormatFName(s.nextFNameSeq)
	s.nextFNameSeq++

	file, err := s.options.OpenFile(path.Join(s.dir, fname),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return "", nil, err
	}

	return fname, file, nil
}

// --------------------------------------------------------

func (s *Store) persistHeader(file File) error {
	buf, err := json.Marshal(Header{
		Version:   STORE_VERSION,
		CreatedAt: time.Now().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}

	str := "moss-data-store:\n" + string(buf) + "\n"
	if len(str) >= STORE_PAGE_SIZE {
		return fmt.Errorf("store: header size too big")
	}
	str = str + strings.Repeat("\n", STORE_PAGE_SIZE-len(str))

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
	segPersister, ok := segIn.(SegmentPersister)
	if !ok {
		return rv, fmt.Errorf("store: can only persist SegmentPersister type")
	}
	return segPersister.Persist(file)
}

// --------------------------------------------------------

// ParseFNameSeq parses a file name like "data-000123.moss" into 123.
func ParseFNameSeq(fname string) (int64, error) {
	seqStr := fname[len(STORE_PREFIX) : len(fname)-len(STORE_SUFFIX)]
	return strconv.ParseInt(seqStr, 16, 64)
}

// FormatFName returns a file name like "data-000123.moss" given a seq of 123.
func FormatFName(seq int64) string {
	return fmt.Sprintf("%s%016x%s", STORE_PREFIX, seq, STORE_SUFFIX)
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

// --------------------------------------------------------

// OpenStoreCollection returns collection based on a persisted store.
// Updates to the collection will be persisted.  An empty directory
// results in an empty collection.  Both the store and collection
// should be closed by the caller.
func OpenStoreCollection(dir string,
	options StoreOptions,
	persistOptions StorePersistOptions) (*Store, Collection, error) {
	store, err := OpenStore(dir, options)
	if err != nil {
		return nil, nil, err
	}

	storeSnapshotInit, err := store.Snapshot()
	if err != nil {
		return nil, nil, err
	}
	storeSnapshotInitWrapper := NewSnapshotWrapper(storeSnapshotInit, store)

	co := options.CollectionOptions
	co.LowerLevelInit = storeSnapshotInitWrapper
	co.LowerLevelUpdate = func(higher Snapshot) (Snapshot, error) {
		ss, err := store.Persist(higher, persistOptions)
		if err != nil {
			return nil, err
		}

		store.AddRef() // Ref-count to be owned by new snapshot wrapper.

		return NewSnapshotWrapper(ss, store), nil
	}

	coll, err := NewCollection(co)
	if err != nil {
		storeSnapshotInitWrapper.Close()
		return nil, nil, err
	}

	err = coll.Start()
	if err != nil {
		storeSnapshotInitWrapper.Close()
		return nil, nil, err
	}

	store.AddRef() // Ref-count owned by caller.

	return store, coll, nil
}
