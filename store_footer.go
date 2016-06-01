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
	"reflect"
	"unsafe"

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

// ReadFooter reads the Footer from a file.
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

				return loadFooterSegments(options, m, file)
			} // Else, perhaps file was unlucky in having STORE_MAGIC_END's.
		} // Else, perhaps a persist file was stored in a file.

		pos-- // Footer was invalid, so keep scanning.
	}
}

// --------------------------------------------------------

// loadFooterSegments mmap()'s the segments that the footer points at.
func loadFooterSegments(options *StoreOptions, f *Footer, file File) (*Footer, error) {
	if f.ss.a != nil {
		return f, nil
	}

	osFile := ToOsFile(file)
	if osFile == nil {
		return nil, fmt.Errorf("store: loadFooterSegments convert to os.File error")
	}

	mm, err := mmap.Map(osFile, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("store: loadFooterSegments mmap.Map(), err: %v", err)
	}

	f.fref.OnBeforeClose(func() { mm.Unmap() })

	f.ss.a = make([]Segment, len(f.SegmentLocs))
	for i := range f.SegmentLocs {
		sloc := &f.SegmentLocs[i]

		segmentLoader, exists := SegmentLoaders[sloc.Kind]
		if !exists || segmentLoader == nil {
			return nil, fmt.Errorf("store: unknown SegmentLoc kind, sloc: %+v", sloc)
		}

		var kvs []uint64
		var buf []byte

		if sloc.KvsBytes > 0 {
			kvsBytes := mm[sloc.KvsOffset : sloc.KvsOffset+sloc.KvsBytes]
			kvsBytesSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&kvsBytes))

			kvsSliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&kvs))
			kvsSliceHeader.Data = kvsBytesSliceHeader.Data
			kvsSliceHeader.Len = kvsBytesSliceHeader.Len / 8
			kvsSliceHeader.Cap = kvsSliceHeader.Len

			if sloc.BufBytes > 0 {
				buf = mm[sloc.BufOffset : sloc.BufOffset+sloc.BufBytes]
			}
		}

		seg, err := segmentLoader(sloc, kvs, buf)
		if err != nil {
			return nil, err
		}

		f.ss.a[i] = seg
	}
	f.ss.refs = 1
	f.ss.options = &options.CollectionOptions

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
// A startKeyIncl of nil means the logical "bottom-most" possible key
// and an endKeyExcl of nil means the logical "top-most" possible key.
func (f *Footer) StartIterator(startKeyIncl, endKeyExcl []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	f.fref.AddRef()
	iter, err := f.ss.StartIterator(startKeyIncl, endKeyExcl, iteratorOptions)
	if err != nil {
		f.fref.DecRef()
		return nil, err
	}
	return &iteratorWrapper{iter: iter, closer: f.fref}, nil
}
