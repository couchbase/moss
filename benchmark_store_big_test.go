//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build benchmark_store_big

package moss

import (
	"testing"
)

// Example usage:
//   go test -timeout=10h -bench=BenchmarkStoreBig -tags=benchmark_store_big
//
func BenchmarkStoreBig_numItems1B_keySize20_valSize0_batchSize100000(b *testing.B) {
	benchmarkStore(b, benchStoreSpec{
		numItems: 1000000000, keySize: 20, valSize: 0, batchSize: 100000,
	})
}
