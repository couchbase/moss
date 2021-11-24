//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"io/ioutil"

	"github.com/couchbase/ghistogram"
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
	totCompactionsPartial := s.totCompactionsPartial
	numLastCompactionBeforeBytes := s.numLastCompactionBeforeBytes
	numLastCompactionAfterBytes := s.numLastCompactionAfterBytes
	totCompactionDecreaseBytes := s.totCompactionDecreaseBytes
	totCompactionIncreaseBytes := s.totCompactionIncreaseBytes
	maxCompactionDecreaseBytes := s.maxCompactionDecreaseBytes
	maxCompactionIncreaseBytes := s.maxCompactionIncreaseBytes
	totCompactionBeforeBytes := s.totCompactionBeforeBytes
	totCompactionWrittenBytes := s.totCompactionWrittenBytes
	s.m.Unlock()

	footer, err := s.snapshot()
	if err != nil {
		return nil, err
	}

	var numSegments uint64
	if footer != nil {
		footer.m.Lock()
		if footer.ss != nil {
			numSegments = uint64(len(footer.ss.a))
		}
		footer.m.Unlock()
	}

	footer.Close()

	files, numFilesOpen := s.allFiles()

	return map[string]interface{}{
		"num_bytes_used_disk":              numBytesUsedDisk,
		"total_persists":                   totPersists,
		"total_compactions":                totCompactions,
		"total_compactions_partial":        totCompactionsPartial,
		"total_compaction_before_bytes":    totCompactionBeforeBytes,
		"total_compaction_written_bytes":   totCompactionWrittenBytes,
		"num_segments":                     numSegments,
		"num_last_compaction_before_bytes": numLastCompactionBeforeBytes,
		"num_last_compaction_after_bytes":  numLastCompactionAfterBytes,
		"total_compaction_decrease_bytes":  totCompactionDecreaseBytes,
		"total_compaction_increase_bytes":  totCompactionIncreaseBytes,
		"max_compaction_decrease_bytes":    maxCompactionDecreaseBytes,
		"max_compaction_increase_bytes":    maxCompactionIncreaseBytes,
		"num_files":                        len(files),
		"num_files_open":                   numFilesOpen,
		"files":                            files,
	}, nil
}

// Histograms returns a snapshot of the histograms for this store.
func (s *Store) Histograms() ghistogram.Histograms {
	histogramsSnapshot := make(ghistogram.Histograms)
	histogramsSnapshot.AddAll(s.histograms)
	return histogramsSnapshot
}
