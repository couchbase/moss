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

// SnapshotPrevious returns the next older, previous snapshot based on
// a given snapshot, allowing the application to walk backwards into
// the history of a store at previous points in time.  The given
// snapshot must come from the same store.  A nil returned snapshot
// means no previous snapshot is available.  Of note, store
// compactions will trim previous history from a store.
func (s *Store) SnapshotPrevious(ss Snapshot) (Snapshot, error) {
	footer, ok := ss.(*Footer)
	if !ok {
		return nil, fmt.Errorf("snapshot not a footer")
	}

	footer.AddRef()
	defer footer.DecRef()

	if footer.mref == nil || footer.mref.fref == nil || footer.mref.fref.file == nil {
		return nil, fmt.Errorf("footer parts nil")
	}

	finfo, err := footer.mref.fref.file.Stat()
	if err != nil {
		return nil, err
	}

	fref := footer.mref.fref
	fref.AddRef()

	ssPrev, err := ScanFooter(s.options, fref, finfo.Name(), footer.filePos-1)
	if err == ErrNoValidFooter {
		fref.DecRef()
		return nil, nil
	}
	if err != nil {
		fref.DecRef()
		return nil, err
	}

	return ssPrev, nil
}
