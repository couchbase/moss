//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build gofuzz

package moss

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/mschoch/smat"
)

func TestGenerateSmatCorpus(t *testing.T) {
	for i, actionSeq := range smatActionSeqs {
		byteSequence, err := actionSeq.ByteEncoding(&smatContext{},
			smat.ActionID('S'), smat.ActionID('T'), actionMap)
		if err != nil {
			t.Fatalf("error from ByteEncoding, err: %v", err)
		}
		os.MkdirAll("workdir/corpus", 0700)
		ioutil.WriteFile(fmt.Sprintf("workdir/corpus/%d", i), byteSequence, 0600)
	}
}

var smatActionSeqs = []smat.ActionSeq{
	{
		smat.ActionID('g'),
		smat.ActionID('B'),
		smat.ActionID('s'),
		smat.ActionID('.'),
		smat.ActionID('d'),
		smat.ActionID('.'),
		smat.ActionID('s'),
		smat.ActionID('.'),
		smat.ActionID('b'),
		smat.ActionID('g'),
		smat.ActionID('H'),
		smat.ActionID('I'),
		smat.ActionID('>'),
		smat.ActionID('i'),
		smat.ActionID('h'),
		smat.ActionID('$'),
		smat.ActionID('g'),
	},
}
