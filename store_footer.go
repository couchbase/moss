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

// ReadFooter reads the last valid Footer from a file.
func ReadFooter(options *StoreOptions, file File) (*Footer, error) {
	finfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return ScanFooter(options, file, finfo.Size())
}

// --------------------------------------------------------

// ScanFooter scans a file backwards from the given pos for a valid Footer.
func ScanFooter(options *StoreOptions, file File, pos int64) (*Footer, error) {
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

		if offset > 0 &&
			offset < pos-int64(footerBegLen+footerEndLen) &&
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

				f := &Footer{refs: 1}

				err = json.Unmarshal(data[2*lenMagicBeg+4+4:], f)
				if err != nil {
					return nil, err
				}

				err = f.loadSegments(options, &FileRef{file: file, refs: 1})
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

// loadSegments() loads the segments of a footer, if not already
// loaded.  The FileRef will be owned by the footer on success.
func (f *Footer) loadSegments(options *StoreOptions, fref *FileRef) error {
	if f.ss != nil && f.ss.a != nil {
		return nil
	}

	osFile := ToOsFile(fref.file)
	if osFile == nil {
		return fmt.Errorf("store: loadFooterSegments convert to os.File error")
	}

	mm, err := mmap.Map(osFile, mmap.RDONLY, 0)
	if err != nil {
		return fmt.Errorf("store: loadFooterSegments mmap.Map(), err: %v", err)
	}

	mref := &mmapRef{fref: fref, mm: mm, refs: 1}

	err = f.loadSegmentsFromMRef(&options.CollectionOptions, mref)
	if err != nil {
		mm.Unmap()

		return err
	}

	return nil
}

// loadSegmentsFromMRef() loads footer's segments via the provided
// mmapRef.  The mmapRef will be owned by the footer on success.
func (f *Footer) loadSegmentsFromMRef(options *CollectionOptions,
	mref *mmapRef) (err error) {
	a := make([]Segment, len(f.SegmentLocs))

	for i := range f.SegmentLocs {
		sloc := &f.SegmentLocs[i]

		segmentLoader, exists := SegmentLoaders[sloc.Kind]
		if !exists || segmentLoader == nil {
			return fmt.Errorf("store: unknown SegmentLoc kind, sloc: %+v", sloc)
		}

		var kvs []uint64
		var buf []byte

		if sloc.KvsBytes > 0 {
			kvsBytes := mref.mm[sloc.KvsOffset : sloc.KvsOffset+sloc.KvsBytes]
			kvs, err = ByteSliceToUint64Slice(kvsBytes)
			if err != nil {
				return err
			}

			if sloc.BufBytes > 0 {
				buf = mref.mm[sloc.BufOffset : sloc.BufOffset+sloc.BufBytes]
			}
		}

		seg, err := segmentLoader(sloc, kvs, buf)
		if err != nil {
			return err
		}

		a[i] = seg
	}

	f.ss = &segmentStack{options: options, a: a, refs: 1}

	f.mref = mref

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
		f.mref.DecRef()
		f.mref = nil
		f.ss = nil
	}
	f.m.Unlock()
}

// --------------------------------------------------------

// mrefSegmentStack() returns the current mmapRef and segmentStack for
// a footer.  The caller must DecRef() the returned mmapRef.
func (f *Footer) mrefSegmentStack() (*mmapRef, *segmentStack) {
	f.m.Lock()

	mref := f.mref.AddRef()
	ss := f.ss

	f.m.Unlock()

	return mref, ss
}

// mrefRefresh provides a newer, latest mmapRef to the chain of
// footers.  This allows older footers (perhaps still being used by
// slow readers) to drop their old mmap's and switch to the latest
// mmap's, which should reduce the number of mmap's in concurrent use.
//
// Also, mrefRefresh will splice out old footers from the chain that
// have no more references.
func (f *Footer) mrefRefresh(mrefNew *mmapRef) *Footer {
	if f == nil {
		return nil
	}

	f.m.Lock()

	refs := f.refs
	if refs > 1 {
		mrefOld := f.mref
		if mrefOld != nil {
			err := f.loadSegmentsFromMRef(f.ss.options, mrefNew)
			if err == nil {
				mrefNew.AddRef()
				mrefOld.DecRef()
			}
		}
	}

	prevFooter := f.prevFooter

	f.m.Unlock()

	prevFooterRefreshed := f.mrefRefreshPrevFooter(prevFooter, mrefNew)
	if refs <= 0 {
		return prevFooterRefreshed
	}

	return f
}

func (f *Footer) mrefRefreshPrevFooter(prevFooter *Footer,
	mrefNew *mmapRef) *Footer {
	prevFooterRefreshed := prevFooter.mrefRefresh(mrefNew)
	if prevFooterRefreshed != prevFooter {
		f.m.Lock()
		f.prevFooter = prevFooterRefreshed // Splice the chain.
		f.m.Unlock()
	}

	return prevFooterRefreshed
}

// --------------------------------------------------------

// Get retrieves a val from the footer, and will return nil val
// if the entry does not exist in the footer.
func (f *Footer) Get(key []byte, readOptions ReadOptions) ([]byte, error) {
	mref, ss := f.mrefSegmentStack()

	rv, err := ss.Get(key, readOptions)
	if err == nil && rv != nil {
		rv = append(make([]byte, 0, len(rv)), rv...) // Copy.
	}

	mref.DecRef()

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
	mref, ss := f.mrefSegmentStack()

	iter, err := ss.StartIterator(startKeyIncl, endKeyExcl, iteratorOptions)
	if err != nil {
		mref.DecRef()
		return nil, err
	}

	initCloser, ok := iter.(InitCloser)
	if !ok || initCloser == nil {
		iter.Close()
		mref.DecRef()
		return nil, ErrUnexpected
	}

	err = initCloser.InitCloser(mref)
	if err != nil {
		iter.Close()
		mref.DecRef()
		return nil, err
	}

	return iter, nil
}
