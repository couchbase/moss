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
