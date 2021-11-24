//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"fmt"
)

func (s *Store) snapshotRevert(revertTo Snapshot) error {
	s.m.Lock()
	defer s.m.Unlock()

	revertToFooter, ok := revertTo.(*Footer)
	if !ok {
		return fmt.Errorf("can only revert a footer")
	}

	fileNameCurr := FormatFName(s.nextFNameSeq - 1)
	if fileNameCurr != revertToFooter.fileName {
		return fmt.Errorf("snapshot too old, revertToSnapshot.fileName: %+v,"+
			" fileNameCurr: %s", revertToFooter.fileName, fileNameCurr)
	}

	persistOptions := StorePersistOptions{}
	footer, err := s.revertToSnapshot(revertToFooter, persistOptions)
	if err != nil {
		return err
	}

	err = s.persistFooter(revertToFooter.SegmentLocs[0].mref.fref.file, footer,
		persistOptions)
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

func (s *Store) revertToSnapshot(revertToFooter *Footer, options StorePersistOptions) (
	rv *Footer, err error) {
	if len(revertToFooter.SegmentLocs) <= 0 {
		return nil, fmt.Errorf("revert footer slocs <= 0")
	}

	mref := revertToFooter.SegmentLocs[0].mref
	if mref == nil || mref.fref == nil || mref.fref.file == nil {
		return nil, fmt.Errorf("revert footer parts nil")
	}

	slocs := append(SegmentLocs{}, revertToFooter.SegmentLocs...)
	slocs.AddRef()

	footer := &Footer{
		refs:        1,
		SegmentLocs: slocs,
		ss:          revertToFooter.ss,
	}

	for cName, childFooter := range revertToFooter.ChildFooters {
		newChildFooter, err := s.revertToSnapshot(childFooter, options)
		if err != nil {
			footer.DecRef()
			return nil, err
		}
		if len(footer.ChildFooters) == 0 {
			footer.ChildFooters = make(map[string]*Footer)
		}
		footer.ChildFooters[cName] = newChildFooter
	}

	return footer, nil
}
