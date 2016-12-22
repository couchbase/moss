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

// TODO: Improved version parsers / checkers / handling (semver?).

var STORE_PREFIX = "data-" // File name prefix.
var STORE_SUFFIX = ".moss" // File name suffix.

var STORE_ENDIAN = binary.LittleEndian
var STORE_PAGE_SIZE = 4096

// STORE_VERSION must be bumped whenever the file format changes.
var STORE_VERSION = uint32(3)

var STORE_MAGIC_BEG []byte = []byte("0m1o2s")
var STORE_MAGIC_END []byte = []byte("3s4p5s")

var lenMagicBeg int = len(STORE_MAGIC_BEG)
var lenMagicEnd int = len(STORE_MAGIC_END)

// footerBegLen includes STORE_VERSION(uint32) & footerLen(uint32).
var footerBegLen int = lenMagicBeg + lenMagicBeg + 4 + 4

// footerEndLen includes footerOffset(int64) & footerLen(uint32) again.
var footerEndLen int = 8 + 4 + lenMagicEnd + lenMagicEnd

// --------------------------------------------------------

// Header represents the JSON stored at the head of a file, where the
// file header bytes should be less than STORE_PAGE_SIZE length.
type Header struct {
	Version       uint32 // The file format / STORE_VERSION.
	CreatedAt     string
	CreatedEndian string // The endian() of the file creator.
}

// Footer represents a footer record persisted in a file, and also
// implements the moss.Snapshot interface.
type Footer struct {
	m    sync.Mutex // Protects the fields that follow.
	refs int

	SegmentLocs SegmentLocs // Persisted; older SegmentLoc's come first.

	ss *segmentStack // Ephemeral.

	fileName string // Ephemeral; file name; "" when unpersisted.
	filePos  int64  // Ephemeral; byte offset of footer; <= 0 when unpersisted.
}

// --------------------------------------------------------

// Persist helps the store implement the lower-level-update func.  The
// higher snapshot may be nil.
func (s *Store) persist(higher Snapshot, persistOptions StorePersistOptions) (
	Snapshot, error) {
	wasCompacted, err := s.compactMaybe(higher, persistOptions)
	if err != nil {
		return nil, err
	}
	if wasCompacted {
		return s.Snapshot()
	}

	// If no dirty higher items, we're still clean, so just snapshot.
	if higher == nil {
		return s.Snapshot()
	}

	ss, ok := higher.(*segmentStack)
	if !ok {
		return nil, fmt.Errorf("store: can only persist segmentStack")
	}

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

	footer := &Footer{refs: 1, SegmentLocs: segmentLocs}

	err = footer.loadSegments(s.options, fref)
	if err != nil {
		fref.DecRef()
		return nil, err
	}

	err = s.persistFooter(file, footer, !persistOptions.NoSync)
	if err != nil {
		footer.DecRef()
		return nil, err
	}

	footer.AddRef() // One ref-count will be held by the store.

	s.m.Lock()
	prevFooter := s.footer
	s.footer = footer
	s.totPersists++
	s.m.Unlock()

	if prevFooter != nil {
		prevFooter.DecRef()
	}

	return footer, nil // The other ref-count returned to caller.
}

// --------------------------------------------------------

// startOrReuseFile either creates a new file or reuses the file from
// the last/current footer.
func (s *Store) startOrReuseFile() (fref *FileRef, file File, err error) {
	s.m.Lock()
	defer s.m.Unlock()

	if s.footer != nil {
		slocs, _ := s.footer.SegmentStack()
		defer s.footer.DecRef()

		if len(slocs) > 0 {
			fref := slocs[0].mref.fref
			file := fref.AddRef()

			return fref, file, nil
		}
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
		Version:       STORE_VERSION,
		CreatedAt:     time.Now().Format(time.RFC3339),
		CreatedEndian: endian(),
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

func checkHeader(file File) error {
	buf := make([]byte, STORE_PAGE_SIZE)

	n, err := file.ReadAt(buf, int64(0))
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("store: readHeader too short")
	}

	lines := strings.Split(string(buf), "\n")
	if len(lines) < 2 {
		return fmt.Errorf("store: readHeader not enough lines")
	}
	if lines[0] != "moss-data-store:" {
		return fmt.Errorf("store: readHeader wrong file prefix")
	}

	hdr := Header{}
	err = json.Unmarshal([]byte(lines[1]), &hdr)
	if err != nil {
		return err
	}
	if hdr.Version != STORE_VERSION {
		return fmt.Errorf("store: readHeader wrong version")
	}
	if hdr.CreatedEndian != endian() {
		return fmt.Errorf("store: readHeader endian of file was: %s, need: %s",
			hdr.CreatedEndian, endian())
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

// pageAlign returns the pos if it's at the start of a page.  Else,
// pageAlign() returns pos bumped up to the next multiple of
// STORE_PAGE_SIZE.
func pageAlign(pos int64) int64 {
	rem := pos % int64(STORE_PAGE_SIZE)
	if rem != 0 {
		return pos + int64(STORE_PAGE_SIZE) - rem
	}
	return pos
}

// pageOffset returns the page offset for a given pos.
func pageOffset(pos, pageSize int64) int64 {
	rem := pos % pageSize
	if rem != 0 {
		return pos - rem
	}
	return pos
}

// --------------------------------------------------------

func openStore(dir string, options StoreOptions) (*Store, error) {
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
		emptyFooter := &Footer{
			refs: 1,
			ss: &segmentStack{
				options: &options.CollectionOptions,
			},
		}

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

		err = checkHeader(file)
		if err != nil {
			file.Close()
			return nil, err
		}

		footer, err := ReadFooter(&options, file) // Footer owns file on success.
		if err != nil {
			file.Close()
			continue
		}

		if !options.KeepFiles {
			err := removeFiles(dir, append(fnames[0:i], fnames[i+1:]...))
			if err != nil {
				footer.Close()
				return nil, err
			}
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

// --------------------------------------------------------

func (store *Store) openCollection(
	options StoreOptions,
	persistOptions StorePersistOptions) (Collection, error) {
	storeSnapshotInit, err := store.Snapshot()
	if err != nil {
		return nil, err
	}

	co := options.CollectionOptions
	co.LowerLevelInit = storeSnapshotInit
	co.LowerLevelUpdate = func(higher Snapshot) (Snapshot, error) {
		ss, err := store.Persist(higher, persistOptions)
		if err != nil {
			return nil, err
		}

		if storeSnapshotInit != nil {
			storeSnapshotInit.Close()
			storeSnapshotInit = nil
		}

		return ss, err
	}

	coll, err := NewCollection(co)
	if err != nil {
		storeSnapshotInit.Close()
		return nil, err
	}

	err = coll.Start()
	if err != nil {
		storeSnapshotInit.Close()
		return nil, err
	}

	return coll, nil
}

// --------------------------------------------------------

func removeFiles(dir string, fnames []string) error {
	for _, fname := range fnames {
		err := os.Remove(path.Join(dir, fname))
		if err != nil {
			return err
		}
	}

	return nil
}
