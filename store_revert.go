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
)

func (s *Store) snapshotRevert(revertTo Snapshot) error {
	revertToFooter, ok := revertTo.(*Footer)
	if !ok {
		return fmt.Errorf("can only revert a footer")
	}

	s.m.Lock()
	defer s.m.Unlock()

	fileNameCurr := FormatFName(s.nextFNameSeq - 1)
	if fileNameCurr != revertToFooter.fileName {
		return fmt.Errorf("snapshot too old, revertToFooter.fileName: %+v,"+
			" fileNameCurr: %s", revertToFooter.fileName, fileNameCurr)
	}

	if len(revertToFooter.SegmentLocs) <= 0 {
		return fmt.Errorf("revert footer slocs <= 0")
	}

	mref := revertToFooter.SegmentLocs[0].mref
	if mref == nil || mref.fref == nil || mref.fref.file == nil {
		return fmt.Errorf("revert footer parts nil")
	}

	slocs := append(SegmentLocs{}, revertToFooter.SegmentLocs...)
	slocs.AddRef()

	footer := &Footer{
		refs:        1,
		SegmentLocs: slocs,
		ss:          revertToFooter.ss,
	}

	err := s.persistFooter(mref.fref.file, footer, true)
	if err != nil {
		footer.DecRef()
		return err
	}

	footerPrev := s.footer
	s.footer = footer // Owns the footer ref-count.
	s.totPersists++

	if footerPrev != nil {
		footerPrev.DecRef()
	}

	return nil
}
