//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"sync"
)

// MergeOperatorStringAppend implements a simple merger that appends
// strings.  It was originally built for testing and sample purposes.
type MergeOperatorStringAppend struct {
	Sep        string // The separator string between operands.
	m          sync.Mutex
	numFull    int
	numPartial int
}

// Name returns the name of this merge operator implemenation
func (mo *MergeOperatorStringAppend) Name() string {
	return "MergeOperatorStringAppend"
}

// FullMerge performs the full merge of a string append operation
func (mo *MergeOperatorStringAppend) FullMerge(key, existingValue []byte,
	operands [][]byte) ([]byte, bool) {
	mo.m.Lock()
	mo.numFull++
	mo.m.Unlock()

	s := string(existingValue)
	for _, operand := range operands {
		s = s + mo.Sep + string(operand)
	}
	return []byte(s), true
}

// PartialMerge performs the partial merge of a string append operation
func (mo *MergeOperatorStringAppend) PartialMerge(key,
	leftOperand, rightOperand []byte) ([]byte, bool) {
	mo.m.Lock()
	mo.numPartial++
	mo.m.Unlock()

	return []byte(string(leftOperand) + mo.Sep + string(rightOperand)), true
}
