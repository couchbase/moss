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

	"github.com/edsrzf/mmap-go"
)

func (s *Store) persistFooter(file File, footer *Footer, sync bool) error {
	if sync {
		err := file.Sync()
		if err != nil {
			return err
		}
	}

	err := s.persistFooterUnsynced(file, footer)
	if err != nil {
		return err
	}

	if sync {
		return file.Sync()
	}

	return nil
}

func (s *Store) persistFooterUnsynced(file File, footer *Footer) error {
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

	footer.fileName = finfo.Name()
	footer.filePos = footerPos

	return nil
}

// --------------------------------------------------------

// ReadFooter reads the last valid Footer from a file.
func ReadFooter(options *StoreOptions, file File) (*Footer, error) {
	finfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fref := &FileRef{file: file, refs: 1}

	f, err := ScanFooter(options, fref, finfo.Name(), finfo.Size())
	if err != nil {
		return nil, err
	}

	fref.DecRef() // ScanFooter added its own ref-counts on success.

	return f, err
}

// --------------------------------------------------------

// ScanFooter scans a file backwards from the given pos for a valid
// Footer, adding ref-counts to fref on success.
func ScanFooter(options *StoreOptions, fref *FileRef, fileName string,
	pos int64) (*Footer, error) {
	footerEnd := make([]byte, footerEndLen)
	for {
		for { // Scan backwards for STORE_MAGIC_END, which might be a potential footer.
			if pos <= int64(footerBegLen+footerEndLen) {
				return nil, ErrNoValidFooter
			}
			n, err := fref.file.ReadAt(footerEnd, pos-int64(footerEndLen))
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

		if offset > 0 &&
			offset < pos-int64(footerBegLen+footerEndLen) &&
			length == uint32(pos-offset) {
			data := make([]byte, pos-offset-int64(footerEndLen))

			n, err := fref.file.ReadAt(data, offset)
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
						"current: %v != found: %v", STORE_VERSION, version)
				}

				var length0 uint32
				if err := binary.Read(b, STORE_ENDIAN, &length0); err != nil {
					return nil, err
				}
				if length0 != length {
					return nil, fmt.Errorf("store: length mismatch, "+
						"wanted: %v != found: %v", length0, length)
				}

				f := &Footer{refs: 1, fileName: fileName, filePos: offset}

				err = json.Unmarshal(data[2*lenMagicBeg+4+4:], f)
				if err != nil {
					return nil, err
				}

				err = f.loadSegments(options, fref)
				if err != nil {
					return nil, err
				}

				return f, nil
			} // Else, perhaps file was unlucky in having STORE_MAGIC_END's.
		} // Else, perhaps a persist file was stored in a file.

		pos-- // Footer was invalid, so keep scanning.
	}
}

// --------------------------------------------------------

// loadSegments() loads the segments of a footer, which must not be
// already loaded.  The footer adds new ref-counts to the fref on
// success.  The footer will be in an already closed state on error.
func (f *Footer) loadSegments(options *StoreOptions, fref *FileRef) (err error) {
	if f.ss != nil && f.ss.a != nil {
		return nil
	}

	osFile := ToOsFile(fref.file)
	if osFile == nil {
		return fmt.Errorf("store: loadSegments convert to os.File error")
	}

	a := make([]Segment, len(f.SegmentLocs))

	// Track mrefs that we need to DecRef() if there's an error.
	mrefs := make([]*mmapRef, 0, len(f.SegmentLocs))

	onError := func() {
		for _, mref := range mrefs {
			mref.DecRef()
		}
	}

	for i := range f.SegmentLocs {
		sloc := &f.SegmentLocs[i]

		mref := sloc.mref
		if mref != nil {
			if mref.fref != fref {
				onError()
				return fmt.Errorf("store: loadSegments fref mismatch")
			}

			mref.AddRef()
		} else {
			// We persist kvs before buf, so KvsOffset < BufOffset.
			begOffset := int64(sloc.KvsOffset)
			endOffset := int64(sloc.BufOffset + sloc.BufBytes)

			nbytes := int(endOffset - begOffset)

			// Some platforms (windows) only support mmap()'ing at an
			// allocation granularity that's != to a page size, so
			// calculate the actual offset/nbytes to use.
			begOffsetActual := pageOffset(begOffset, int64(AllocationGranularity))
			begOffsetDelta := int(begOffset - begOffsetActual)
			nbytesActual := nbytes + begOffsetDelta

			mm, err := mmap.MapRegion(osFile, nbytesActual, mmap.RDONLY, 0, begOffsetActual)
			if err != nil {
				onError()
				return fmt.Errorf("store: loadSegments mmap.Map(), err: %v", err)
			}

			fref.AddRef() // New mref owns 1 fref ref-count.

			buf := mm[begOffsetDelta : begOffsetDelta+nbytes]

			sloc.mref = &mmapRef{fref: fref, mm: mm, buf: buf, refs: 1}

			mref = sloc.mref
		}

		mrefs = append(mrefs, mref)

		segmentLoader, exists := SegmentLoaders[sloc.Kind]
		if !exists || segmentLoader == nil {
			onError()
			return fmt.Errorf("store: unknown SegmentLoc kind, sloc: %+v", sloc)
		}

		var kvs []uint64
		var buf []byte

		if sloc.KvsBytes > 0 {
			if sloc.KvsBytes > uint64(len(mref.buf)) {
				onError()
				return fmt.Errorf("store_footer: KvsOffset/KvsBytes too big,"+
					" len(mref.buf): %d, sloc: %+v, footer: %+v,"+
					" f.SegmentLocs: %+v, i: %d, options: %v",
					len(mref.buf), sloc, f, f.SegmentLocs, i, options)
			}

			kvsBytes := mref.buf[0:sloc.KvsBytes]
			kvs, err = ByteSliceToUint64Slice(kvsBytes)
			if err != nil {
				onError()
				return err
			}
		}

		if sloc.BufBytes > 0 {
			bufStart := sloc.BufOffset - sloc.KvsOffset
			if bufStart+sloc.BufBytes > uint64(len(mref.buf)) {
				onError()
				return fmt.Errorf("store_footer: BufOffset/BufBytes too big,"+
					" len(mref.buf): %d, sloc: %+v, footer: %+v,"+
					" f.SegmentLocs: %+v, i: %d, options: %v",
					len(mref.buf), sloc, f, f.SegmentLocs, i, options)
			}

			buf = mref.buf[bufStart : bufStart+sloc.BufBytes]
		}

		seg, err := segmentLoader(sloc, kvs, buf)
		if err != nil {
			onError()
			return err
		}

		a[i] = seg
	}

	f.ss = &segmentStack{options: &options.CollectionOptions, a: a, refs: 1}

	return nil
}

// --------------------------------------------------------

func (f *Footer) Close() error {
	f.DecRef()
	return nil
}

func (f *Footer) AddRef() {
	f.m.Lock()
	f.refs++
	f.m.Unlock()
}

func (f *Footer) DecRef() {
	f.m.Lock()
	f.refs--
	if f.refs <= 0 {
		f.SegmentLocs.DecRef()
		f.SegmentLocs = nil
		f.ss = nil
	}
	f.m.Unlock()
}

// --------------------------------------------------------

// SegmentStack() returns the current SegmentLocs and segmentStack for
// a footer, while also incrementing the ref-count on the footer.  The
// caller must DecRef() the footer when done.
func (f *Footer) SegmentStack() (SegmentLocs, *segmentStack) {
	f.m.Lock()

	f.refs++

	slocs, ss := f.SegmentLocs, f.ss

	f.m.Unlock()

	return slocs, ss
}

// --------------------------------------------------------

// Get retrieves a val from the footer, and will return nil val
// if the entry does not exist in the footer.
func (f *Footer) Get(key []byte, readOptions ReadOptions) ([]byte, error) {
	_, ss := f.SegmentStack()

	rv, err := ss.Get(key, readOptions)
	if err == nil && rv != nil {
		rv = append(make([]byte, 0, len(rv)), rv...) // Copy.
	}

	f.DecRef()

	return rv, err
}

// StartIterator returns a new Iterator instance on this footer.
//
// On success, the returned Iterator will be positioned so that
// Iterator.Current() will either provide the first entry in the
// range or ErrIteratorDone.
//
// A startKeyIncl of nil means the logical "bottom-most" possible key
// and an endKeyExcl of nil means the logical "top-most" possible key.
func (f *Footer) StartIterator(startKeyIncl, endKeyExcl []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	_, ss := f.SegmentStack()

	iter, err := ss.StartIterator(startKeyIncl, endKeyExcl, iteratorOptions)
	if err != nil {
		f.DecRef()
		return nil, err
	}

	initCloser, ok := iter.(InitCloser)
	if !ok || initCloser == nil {
		iter.Close()
		f.DecRef()
		return nil, ErrUnexpected
	}

	err = initCloser.InitCloser(f)
	if err != nil {
		iter.Close()
		f.DecRef()
		return nil, err
	}

	return iter, nil
}
