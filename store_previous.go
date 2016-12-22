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

func (s *Store) snapshotPrevious(ss Snapshot) (Snapshot, error) {
	footer, ok := ss.(*Footer)
	if !ok {
		return nil, fmt.Errorf("snapshot not a footer")
	}

	slocs, _ := footer.SegmentStack()
	defer footer.DecRef()

	if len(slocs) <= 0 {
		return nil, nil
	}

	mref := slocs[0].mref
	if mref == nil || mref.refs <= 0 {
		return nil, fmt.Errorf("footer mref nil")
	}

	fref := mref.fref
	if fref == nil || fref.refs <= 0 || fref.file == nil {
		return nil, fmt.Errorf("footer fref nil")
	}

	finfo, err := fref.file.Stat()
	if err != nil {
		return nil, err
	}

	ssPrev, err := ScanFooter(s.options, fref, finfo.Name(), footer.filePos-1)
	if err == ErrNoValidFooter {
		return nil, nil
	}

	return ssPrev, err
}
