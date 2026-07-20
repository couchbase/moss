//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"context"
	"iter"
)

// All returns a Go 1.23+ range-over-func iterator over the Snapshot's
// key-val entries in [startKeyInclusive, endKeyExclusive), so callers
// can simply:
//
//	seq, errFn := moss.All(ss, nil, nil, moss.IteratorOptions{})
//	for k, v := range seq {
//	    // k and v are only valid until the next iteration; copy to keep.
//	}
//	if err := errFn(); err != nil { ... }
//
// The returned errFn must be called AFTER the range loop; it reports
// any error that terminated iteration early (nil if the range ran to
// completion or the loop broke out cleanly).  It is a thin, additive
// convenience over StartIterator and works with any Snapshot
// implementation.
//
// As with the underlying Iterator, the yielded key and val are only
// valid until the next iteration step; copy them if they must outlive
// it.
func All(ss Snapshot, startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (iter.Seq2[[]byte, []byte], func() error) {
	return allWithContext(context.Background(), ss,
		startKeyInclusive, endKeyExclusive, iteratorOptions)
}

// AllWithContext is like All, but stops iteration early (surfacing
// ctx.Err() via the returned errFn) if ctx is canceled or hits its
// deadline, and starts the underlying iterator with that context.
func AllWithContext(ctx context.Context, ss Snapshot,
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (iter.Seq2[[]byte, []byte], func() error) {
	return allWithContext(ctx, ss,
		startKeyInclusive, endKeyExclusive, iteratorOptions)
}

func allWithContext(ctx context.Context, ss Snapshot,
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (iter.Seq2[[]byte, []byte], func() error) {
	var retErr error

	seq := func(yield func([]byte, []byte) bool) {
		itr, err := ss.StartIteratorWithContext(ctx,
			startKeyInclusive, endKeyExclusive, iteratorOptions)
		if err != nil {
			retErr = err
			return
		}
		defer itr.Close()

		for {
			if err := ctx.Err(); err != nil {
				retErr = err
				return
			}

			k, v, err := itr.Current()
			if err == ErrIteratorDone {
				return
			}
			if err != nil {
				retErr = err
				return
			}

			if !yield(k, v) {
				return // Caller broke out of the range; not an error.
			}

			err = itr.Next()
			if err == ErrIteratorDone {
				return
			}
			if err != nil {
				retErr = err
				return
			}
		}
	}

	return seq, func() error { return retErr }
}
