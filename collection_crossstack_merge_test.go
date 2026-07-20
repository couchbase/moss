//  Copyright 2016-Present Couchbase, Inc.
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

// TestCollectionGetCrossStackMerge is a deterministic regression guard
// for the bug where collection.Get() resolved each dirty stack
// independently and returned a partial merge when a key's merge
// operands lived in a newer stack while its base value lived in an
// older stack.  The stacks are crafted directly (no merger running) so
// the layering is exact and does not depend on background timing.
func TestCollectionGetCrossStackMerge(t *testing.T) {
	newColl := func() *collection {
		c, err := NewCollection(CollectionOptions{
			MergeOperator: &MergeOperatorStringAppend{Sep: ":"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c.(*collection)
	}

	// stackOf builds a one-segment segmentStack for key "k" with the
	// given op and value.
	stackOf := func(m *collection, op uint64, val string) *segmentStack {
		buf := append([]byte("k"), val...) // key 'k' at [0:1], val at [1:].
		seg := makeSegment(buf, entry{op, 1, len(val), 0})
		return &segmentStack{options: m.options, refs: 1, a: []Segment{seg}}
	}

	cases := []struct {
		name       string
		build      func(m *collection)
		wantAbsent bool
		want       string
	}{
		{
			name: "operand-in-top,set-base-in-mid",
			build: func(m *collection) {
				m.stackDirtyTop = stackOf(m, OperationMerge, "op1")
				m.stackDirtyMid = stackOf(m, OperationSet, "base")
			},
			want: "base:op1",
		},
		{
			name: "operands-span-top-and-mid,base-in-base",
			build: func(m *collection) {
				m.stackDirtyTop = stackOf(m, OperationMerge, "op2") // newest
				m.stackDirtyMid = stackOf(m, OperationMerge, "op1")
				m.stackDirtyBase = stackOf(m, OperationSet, "base") // oldest
			},
			want: "base:op1:op2",
		},
		{
			name: "operand-in-top,del-in-mid",
			build: func(m *collection) {
				m.stackDirtyTop = stackOf(m, OperationMerge, "op1")
				m.stackDirtyMid = stackOf(m, OperationDel, "")
			},
			want: ":op1", // Del is a nil base.
		},
		{
			name: "set-in-top-shadows-merge-in-mid",
			build: func(m *collection) {
				m.stackDirtyTop = stackOf(m, OperationSet, "new")
				m.stackDirtyMid = stackOf(m, OperationMerge, "op1")
			},
			want: "new",
		},
		{
			name: "operand-only,no-base",
			build: func(m *collection) {
				m.stackDirtyTop = stackOf(m, OperationMerge, "op1")
			},
			want: ":op1", // FullMerge(nil, ["op1"]).
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newColl()
			tc.build(m)

			got, err := m.Get([]byte("k"), ReadOptions{})
			if err != nil {
				t.Fatalf("Get err: %v", err)
			}
			if tc.wantAbsent {
				if got != nil {
					t.Fatalf("expected absent, got %q", got)
				}
				return
			}
			if got == nil || string(got) != tc.want {
				t.Fatalf("Get = %q, want %q", got, tc.want)
			}
		})
	}
}
