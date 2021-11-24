//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"encoding/json"
	"testing"
)

func TestCollectionOptionsJSON(t *testing.T) {
	co := CollectionOptions{
		MaxDirtyOps: 123,
		OnError:     func(err error) {},
	}
	b, err := json.Marshal(co)
	if err != nil {
		t.Errorf("expected no error marshal")
	}
	co.MaxDirtyOps = 321
	err = json.Unmarshal(b, &co)
	if err != nil {
		t.Errorf("expected no error unmarshal")
	}
	if co.MaxDirtyOps != 123 {
		t.Errorf("expected 123")
	}
	if co.OnError == nil {
		t.Errorf("expected non-nil func")
	}
}
