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
)

// Stats returns a map of stats.
func (s *Store) Stats() (map[string]interface{}, error) {
	finfos, err := ioutil.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}

	var numBytesUsedDisk uint64
	for _, finfo := range finfos {
		if !finfo.IsDir() {
			numBytesUsedDisk += uint64(finfo.Size())
		}
	}

	s.m.Lock()
	totPersists := s.totPersists
	totCompactions := s.totCompactions
	s.m.Unlock()

	footer, err := s.snapshot()
	if err != nil {
		return nil, err
	}

	numSegments := 0
	if footer != nil {
		footer.m.Lock()
		if footer.ss != nil {
			numSegments = len(footer.ss.a)
		}
		footer.m.Unlock()
	}

	footer.Close()

	return map[string]interface{}{
		"num_bytes_used_disk": numBytesUsedDisk,
		"total_persists":      totPersists,
		"total_compactions":   totCompactions,
		"num_segments":        numSegments,
	}, nil
}
