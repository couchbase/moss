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

func (s *Store) snapshotPrevious(ss Snapshot) (Snapshot, error) {
	footer, ok := ss.(*Footer)
	if !ok {
		return nil, fmt.Errorf("snapshot not a footer")
	}

	slocs, _ := footer.segmentLocs()
	defer footer.DecRef()

	if len(slocs) <= 0 {
		return nil, nil
	}

	mref := slocs[0].mref
	if mref == nil {
		return nil, fmt.Errorf("footer mref nil")
	}

	mref.m.Lock()
	if mref.refs <= 0 {
		mref.m.Unlock()
		return nil, fmt.Errorf("footer mmap has 0 refs")
	}
	fref := mref.fref
	mref.m.Unlock() // Safe since the file beneath the mmap cannot change.

	if fref == nil {
		return nil, fmt.Errorf("footer fref nil")
	}

	fref.m.Lock()
	if fref.refs <= 0 || fref.file == nil {
		fref.m.Unlock()
		return nil, fmt.Errorf("footer has 0 refs")
	}
	fref.m.Unlock() // Safe since the file ref count is positive.

	finfo, err := fref.file.Stat()
	if err != nil {
		return nil, err
	}

	ssPrev, err := ScanFooter(s.options, fref, finfo.Name(), footer.PrevFooterOffset)
	if err == ErrNoValidFooter {
		return nil, nil
	}

	return ssPrev, err
}
