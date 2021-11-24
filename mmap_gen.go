//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build !windows

package moss

// AllocationGranularity sets the granularity of allocation.  Some
// operating systems require this to occur on particular boundaries.
var AllocationGranularity = StorePageSize

// IsTimingCoarse is true only on those platforms where a nano second
// time resolution is not available (see: windows).
var IsTimingCoarse = false
