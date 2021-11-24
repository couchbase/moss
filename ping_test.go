//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"testing"
)

func TestPings(t *testing.T) {
	c := make(chan ping, 2)

	pong0 := make(chan struct{})
	c <- ping{"baseball", pong0}

	pong1 := make(chan struct{})
	c <- ping{"tennis", pong1}

	pings := []ping{}
	pings, kindSeen := receivePings(c, pings, "tennis", false)
	if len(pings) != 2 {
		t.Errorf("expected 2 pings")
	}
	if !kindSeen {
		t.Errorf("expected tennis seen")
	}

	go replyToPings(pings)

	<-pong0
	<-pong1
}
