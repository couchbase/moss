//  Copyright (c) 2017 Couchbase, Inc.
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
