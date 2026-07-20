//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !windows
// +build !windows

package moss

import "os"

// AllocationGranularity sets the granularity of allocation.  Some
// operating systems require this to occur on particular boundaries.
//
// mmap() requires its file offset to be a multiple of the operating
// system's page size.  On most amd64 platforms that page size is 4096
// (== StorePageSize), but on others (notably arm64 / Apple Silicon
// macOS) it is 16384, so we align to the actual OS page size rather
// than assuming it equals StorePageSize.
var AllocationGranularity = pageAlignGranularity()

func pageAlignGranularity() int {
	ps := os.Getpagesize()
	if ps < StorePageSize {
		return StorePageSize
	}
	return ps
}

// IsTimingCoarse is true only on those platforms where a nano second
// time resolution is not available (see: windows).
var IsTimingCoarse = false
