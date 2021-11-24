//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"testing"
)

func TestSegmentKeyValueSizeLimits(t *testing.T) {
	s, _ := newSegment(100, 200)
	err := s.mutateEx(150, 0, 150, 125)
	if err != nil {
		t.Errorf("expected a segment")
	}
	err = s.mutateEx(150, 0, maxKeyLength, maxValLength)
	if err != nil {
		t.Errorf("expected a segment")
	}
	err = s.mutateEx(150, 0, maxKeyLength+1, 125)
	if err != ErrKeyTooLarge {
		t.Errorf("should have erred for large key")
	}
	err = s.mutateEx(150, 0, 100, maxValLength+1)
	if err != ErrValueTooLarge {
		t.Errorf("should have erred for large value")
	}
	err = s.mutateEx(150, 0, maxKeyLength+1, maxValLength+1)
	if err != ErrKeyTooLarge {
		t.Errorf("should have erred for large key")
	}
}
