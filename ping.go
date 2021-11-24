//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

// A ping message is used to notify and wait for asynchronous tasks.
type ping struct {
	kind string // The kind of ping.

	// When non-nil, the pongCh will be closed when task is done.
	pongCh chan struct{}
}

// replyToPings() is a helper function to respond to ping requests.
func replyToPings(pings []ping) {
	for _, ping := range pings {
		if ping.pongCh != nil {
			close(ping.pongCh)
			ping.pongCh = nil
		}
	}
}

// receivePings() collects any available ping requests, but will not
// block if there are no incoming ping requests.
func receivePings(pingCh chan ping, pings []ping,
	kindMatch string, kindSeen bool) ([]ping, bool) {
	for {
		select {
		case pingVal := <-pingCh:
			pings = append(pings, pingVal)
			if pingVal.kind == kindMatch {
				kindSeen = true
			}

		default:
			return pings, kindSeen
		}
	}
}
