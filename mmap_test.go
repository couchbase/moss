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
	"io/ioutil"
	"os"
	"testing"

	"github.com/edsrzf/mmap-go"

)

func TestMultipleMMapsOnSameFile(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "mossMMap")
	defer os.RemoveAll(tmpDir)

	f, err := os.Create(tmpDir + string(os.PathSeparator) + "test.file")
	if err != nil {
		t.Errorf("expected open file to work, err: %v", err)
	}

	offset := 1024 * 1024 * 1024 // 1 GB.

	f.WriteAt([]byte("hello"), int64(offset))

	var mms []mmap.MMap

	for i := 0; i < 100; i++ { // Re-mmap the file.
		mm, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			t.Errorf("expected mmap to work, err: %v", err)
		}

		if string(mm[offset:offset+5]) != "hello" {
			t.Errorf("expected hello")
		}

		mms = append(mms, mm)
	}

	for _, mm := range mms {
		if string(mm[offset:offset+5]) != "hello" {
			t.Errorf("expected hello")
		}

		for j := 0; j < offset; j += 1024 * 1024 {
			if mm[j] != 0 {
				t.Errorf("expected 0")
			}
		}
	}

	for _, mm := range mms {
		mm.Unmap()
	}

	f.Close()
}
