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
	"fmt"
	"os"
	"path"
	"time"
)

func (s *Store) compactMaybe(higher Snapshot, persistOptions StorePersistOptions) (
	bool, error) {
	if s.Options().CollectionOptions.ReadOnly {
		// Do not compact in Read-Only mode
		return false, nil
	}

	compactionConcern := persistOptions.CompactionConcern
	if compactionConcern <= 0 {
		return false, nil
	}

	footer, err := s.snapshot()
	if err != nil {
		return false, err
	}

	defer footer.DecRef()

	slocs, ss := footer.SegmentStack()

	defer footer.DecRef()

	if compactionConcern == CompactionAllow {
		totUpperLen := 0
		if ss != nil && len(ss.a) >= 2 {
			for i := 1; i < len(ss.a); i++ {
				totUpperLen += ss.a[i].Len()
			}
		}

		if higher != nil {
			higherSS, ok := higher.(*segmentStack)
			if ok {
				higherStats := higherSS.Stats()
				if higherStats != nil {
					totUpperLen += int(higherStats.CurOps)
				}
			}
		}

		if totUpperLen > 0 {
			var pct float64
			if ss != nil && len(ss.a) > 0 && ss.a[0].Len() > 0 {
				pct = float64(totUpperLen) / float64(ss.a[0].Len())
			}

			if pct >= s.options.CompactionPercentage {
				compactionConcern = CompactionForce
			}
		}
	}

	if compactionConcern != CompactionForce {
		return false, nil
	}

	err = s.compact(footer, higher, persistOptions)
	if err != nil {
		return false, err
	}

	var sizeBefore, sizeAfter int64

	if len(slocs) > 0 {
		mref := slocs[0].mref
		if mref != nil && mref.fref != nil {
			finfo, err := mref.fref.file.Stat()
			if err == nil && len(finfo.Name()) > 0 {
				// Fetch size of old file
				sizeBefore = finfo.Size()
				mref.fref.OnAfterClose(func() {
					os.Remove(path.Join(s.dir, finfo.Name()))
				})
			}
		}
	}

	slocs, _ = footer.SegmentStack()
	defer footer.DecRef()

	if len(slocs) > 0 {
		mref := slocs[0].mref
		if mref != nil && mref.fref != nil {
			finfo, err := mref.fref.file.Stat()
			if err == nil && len(finfo.Name()) > 0 {
				// Fetch size of new file
				sizeAfter = finfo.Size()
			}
		}
	}

	s.m.Lock()
	s.numLastCompactionBeforeBytes = uint64(sizeBefore)
	s.numLastCompactionAfterBytes = uint64(sizeAfter)
	delta := sizeBefore - sizeAfter
	if delta > 0 {
		s.totCompactionDecreaseBytes += uint64(delta)
		if s.maxCompactionDecreaseBytes < uint64(delta) {
			s.maxCompactionDecreaseBytes = uint64(delta)
		}
	} else if delta < 0 {
		delta = -delta
		s.totCompactionIncreaseBytes += uint64(delta)
		if s.maxCompactionIncreaseBytes < uint64(delta) {
			s.maxCompactionIncreaseBytes = uint64(delta)
		}
	}
	s.m.Unlock()

	return true, nil
}

func (s *Store) compact(footer *Footer, higher Snapshot,
	persistOptions StorePersistOptions) error {
	startTime := time.Now()
	var newSS *segmentStack
	if higher != nil {
		ssHigher, ok := higher.(*segmentStack)
		if !ok {
			return fmt.Errorf("store: can only compact higher that's a segmentStack")
		}
		ssHigher.ensureFullySorted()
		newSS = s.mergeSegStacks(footer, ssHigher)
	} else {
		newSS = footer.ss // safe as footer ref count is held positive.
	}

	s.m.Lock()
	frefCompact, fileCompact, err := s.startFileLOCKED()
	s.m.Unlock()
	if err != nil {
		return err
	}

	compactFooter, err := s.writeSegments(newSS, frefCompact, fileCompact)
	if err != nil {
		frefCompact.DecRef()
		return err
	}

	sync := !persistOptions.NoSync
	if !sync {
		sync = s.options != nil && s.options.CompactionSync
	}

	err = s.persistFooter(fileCompact, compactFooter, persistOptions)
	if err != nil {
		frefCompact.DecRef()
		return err
	}

	footerReady, err := ReadFooter(s.options, fileCompact)
	if err != nil {
		frefCompact.DecRef()
		return err
	}

	s.m.Lock()
	footerPrev := s.footer
	s.footer = footerReady // Owns the frefCompact ref-count.
	s.totCompactions++
	s.m.Unlock()

	s.histograms["CompactUsecs"].Add(
		uint64(time.Since(startTime).Nanoseconds()/1000), 1)

	if footerPrev != nil {
		footerPrev.DecRef()
	}

	return nil
}

func (s *Store) mergeSegStacks(footer *Footer, higher *segmentStack) *segmentStack {
	var footerSS *segmentStack
	var lenFooterSS int
	if footer != nil && footer.ss != nil {
		footerSS = footer.ss
		lenFooterSS = len(footerSS.a)
	}
	rv := &segmentStack{
		options:  higher.options,
		a:        make([]Segment, 0, len(higher.a)+lenFooterSS),
		incarNum: higher.incarNum,
	}
	if footerSS != nil {
		rv.a = append(rv.a, footerSS.a...)
	}
	rv.a = append(rv.a, higher.a...)
	for cName, newStack := range higher.childSegStacks {
		if len(rv.childSegStacks) == 0 {
			rv.childSegStacks = make(map[string]*segmentStack)
		}
		if footer == nil {
			rv.childSegStacks[cName] = s.mergeSegStacks(nil, newStack)
			continue
		}

		childFooter, exists := footer.ChildFooters[cName]
		if exists {
			if childFooter.incarNum != higher.incarNum {
				// Fast child collection recreation, must not merge
				// segments from prior incarnation.
				childFooter = nil
			}
		}
		rv.childSegStacks[cName] = s.mergeSegStacks(childFooter, newStack)
	}
	return rv
}

func (s *Store) writeSegments(newSS *segmentStack, frefCompact *FileRef,
	fileCompact File) (compactFooter *Footer, err error) {
	var pos int64
	if newSS.incarNum == 0 {
		pos = int64(StorePageSize)
	} else {
		var finfo os.FileInfo
		finfo, err = fileCompact.Stat()
		if err != nil {
			return nil, err
		}
		pos = finfo.Size()
	}

	stats := newSS.Stats()

	kvsBegPos := pageAlignCeil(pos)
	bufBegPos := pageAlignCeil(kvsBegPos + 1 + (int64(8+8) * int64(stats.CurOps)))

	compactionBufferPages := 0
	if s.options != nil {
		compactionBufferPages = s.options.CompactionBufferPages
	}
	if compactionBufferPages <= 0 {
		compactionBufferPages = DefaultStoreOptions.CompactionBufferPages
	}
	compactionBufferSize := StorePageSize * compactionBufferPages

	compactWriter := &compactWriter{
		kvsWriter: NewBufferedSectionWriter(fileCompact, kvsBegPos, 0, compactionBufferSize),
		bufWriter: NewBufferedSectionWriter(fileCompact, bufBegPos, 0, compactionBufferSize),
	}
	onError := func(err error) error {
		compactWriter.kvsWriter.Stop()
		compactWriter.bufWriter.Stop()
		return err
	}

	err = newSS.mergeInto(0, len(newSS.a), compactWriter, nil, false, false, s.abortCh)
	if err != nil {
		return nil, onError(err)
	}

	if err = compactWriter.kvsWriter.Flush(); err != nil {
		return nil, onError(err)
	}
	if err = compactWriter.bufWriter.Flush(); err != nil {
		return nil, onError(err)
	}

	if err = compactWriter.kvsWriter.Stop(); err != nil {
		return nil, onError(err)
	}
	if err = compactWriter.bufWriter.Stop(); err != nil {
		return nil, onError(err)
	}

	compactFooter = &Footer{
		refs: 1,
		SegmentLocs: []SegmentLoc{
			{
				Kind:       SegmentKindBasic,
				KvsOffset:  uint64(kvsBegPos),
				KvsBytes:   uint64(compactWriter.kvsWriter.Offset() - kvsBegPos),
				BufOffset:  uint64(bufBegPos),
				BufBytes:   uint64(compactWriter.bufWriter.Offset() - bufBegPos),
				TotOpsSet:  compactWriter.totOperationSet,
				TotOpsDel:  compactWriter.totOperationDel,
				TotKeyByte: compactWriter.totKeyByte,
				TotValByte: compactWriter.totValByte,
			},
		},
	}

	for cName, childSegStack := range newSS.childSegStacks {
		if compactFooter.ChildFooters == nil {
			compactFooter.ChildFooters = make(map[string]*Footer)
		}
		childFooter, err := s.writeSegments(childSegStack,
			frefCompact, fileCompact)
		if err != nil {
			return nil, err
		}
		compactFooter.ChildFooters[cName] = childFooter
	}
	return compactFooter, nil
}

type compactWriter struct {
	file      File
	kvsWriter *bufferedSectionWriter
	bufWriter *bufferedSectionWriter

	totOperationSet   uint64
	totOperationDel   uint64
	totOperationMerge uint64
	totKeyByte        uint64
	totValByte        uint64
}

func (cw *compactWriter) Mutate(operation uint64, key, val []byte) error {
	keyStart := cw.bufWriter.Written()

	_, err := cw.bufWriter.Write(key)
	if err != nil {
		return err
	}

	_, err = cw.bufWriter.Write(val)
	if err != nil {
		return err
	}

	keyLen := len(key)
	valLen := len(val)

	opKlVl := encodeOpKeyLenValLen(operation, keyLen, valLen)

	if keyLen <= 0 && valLen <= 0 {
		keyStart = 0
	}

	pair := []uint64{opKlVl, uint64(keyStart)}
	kvsBuf, err := Uint64SliceToByteSlice(pair)
	if err != nil {
		return err
	}

	_, err = cw.kvsWriter.Write(kvsBuf)
	if err != nil {
		return err
	}

	switch operation {
	case OperationSet:
		cw.totOperationSet++
	case OperationDel:
		cw.totOperationDel++
	case OperationMerge:
		cw.totOperationMerge++
	default:
	}

	cw.totKeyByte += uint64(keyLen)
	cw.totValByte += uint64(valLen)

	return nil
}
