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
