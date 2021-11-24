//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"reflect"
	"sync/atomic"

	"github.com/couchbase/ghistogram"
)

// Stats returns stats for this collection.
func (m *collection) Stats() (*CollectionStats, error) {
	rv := &CollectionStats{}

	m.stats.AtomicCopyTo(rv)

	m.m.Lock()
	m.statsSegmentsLOCKED(rv)
	m.m.Unlock()

	return rv, nil
}

func (m *collection) Histograms() ghistogram.Histograms {
	histogramsSnapshot := make(ghistogram.Histograms)
	histogramsSnapshot.AddAll(m.histograms)
	return histogramsSnapshot
}

// statsSegmentsLOCKED retrieves stats related to segments.
func (m *collection) statsSegmentsLOCKED(rv *CollectionStats) {
	var sssDirtyTop *SegmentStackStats
	var sssDirtyMid *SegmentStackStats
	var sssDirtyBase *SegmentStackStats
	var sssClean *SegmentStackStats

	if m.stackDirtyTop != nil {
		sssDirtyTop = m.stackDirtyTop.Stats()
	}

	if m.stackDirtyMid != nil {
		sssDirtyMid = m.stackDirtyMid.Stats()
	}

	if m.stackDirtyBase != nil {
		sssDirtyBase = m.stackDirtyBase.Stats()
	}

	if m.stackClean != nil {
		sssClean = m.stackClean.Stats()
	}

	sssDirty := &SegmentStackStats{}
	sssDirtyTop.AddTo(sssDirty)
	sssDirtyMid.AddTo(sssDirty)
	sssDirtyBase.AddTo(sssDirty)

	rv.CurDirtyOps = sssDirty.CurOps
	rv.CurDirtyBytes = sssDirty.CurBytes
	rv.CurDirtySegments = sssDirty.CurSegments

	if sssDirtyTop != nil {
		rv.CurDirtyTopOps = sssDirtyTop.CurOps
		rv.CurDirtyTopBytes = sssDirtyTop.CurBytes
		rv.CurDirtyTopSegments = sssDirtyTop.CurSegments
	}

	if sssDirtyMid != nil {
		rv.CurDirtyMidOps = sssDirtyMid.CurOps
		rv.CurDirtyMidBytes = sssDirtyMid.CurBytes
		rv.CurDirtyMidSegments = sssDirtyMid.CurSegments
	}

	if sssDirtyBase != nil {
		rv.CurDirtyBaseOps = sssDirtyBase.CurOps
		rv.CurDirtyBaseBytes = sssDirtyBase.CurBytes
		rv.CurDirtyBaseSegments = sssDirtyBase.CurSegments
	}

	if sssClean != nil {
		rv.CurCleanOps = sssClean.CurOps
		rv.CurCleanBytes = sssClean.CurBytes
		rv.CurCleanSegments = sssClean.CurSegments
	}
}

// AtomicCopyTo copies stats from s to r (from source to result).
func (s *CollectionStats) AtomicCopyTo(r *CollectionStats) {
	rve := reflect.ValueOf(r).Elem()
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		rvef := rve.Field(i)
		svef := sve.Field(i)
		if rvef.CanAddr() && svef.CanAddr() {
			rvefp := rvef.Addr().Interface()
			svefp := svef.Addr().Interface()
			atomic.StoreUint64(rvefp.(*uint64),
				atomic.LoadUint64(svefp.(*uint64)))
		}
	}
}
