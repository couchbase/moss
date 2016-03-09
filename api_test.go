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
