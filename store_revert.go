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

// SnapshotRevert atomically and durably brings the store back to the
// point-in-time as represented by the revertTo snapshot.
// SnapshotRevert() should only be passed a snapshot that came from
// the same store, such as from using Store.Snapshot() or
// Store.SnapshotPrevious().
//
// SnapshotRevert() must not be invoked concurrently with
// Store.Persist(), so it is recommended that SnapshotRevert() should
// be invoked only after the collection has been Close()'ed, which
// helps ensure that you are not racing with concurrent, background
// persistence goroutines.
//
// SnapshotRevert() can fail if the given snapshot is too old,
// especially w.r.t. compactions.  For example, navigate back to an
// older snapshot X via SnapshotPrevious().  Then, do a full
// compaction.  Then, SnapshotRevert(X) will give an error.
func (s *Store) SnapshotRevert(revertTo Snapshot) error {
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

	footer := &Footer{
		SegmentLocs: revertToFooter.SegmentLocs,
		refs:        1,
		mref:        revertToFooter.mref,
		ss:          revertToFooter.ss,
	}

	footer.mref.AddRef()

	err := s.persistFooter(footer.mref.fref.file, footer, true)
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
