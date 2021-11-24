//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build windows

package moss

// Windows MapViewOfFile() API (rough equivalent of mmap()), requires
// offsets to be multiples of an allocation granularity, which is up
// to 64kiB (or, larger than the usual 4KB page size).
//
// See: https://social.msdn.microsoft.com/Forums/vstudio/en-US/972f36a4-26c9-466b-861a-5f40fa4cf4e7/about-the-dwallocationgranularity?forum=vclanguage
//
var AllocationGranularity = 65536 // 64kiB.

// IsTimingCoarse is true on Windows because of the granularity of the time
// resolution can be as large as 15ms.
// So this variable can help false failures in unit tests.
// See https://stackoverflow.com/a/4019164.
var IsTimingCoarse = true
