// Copyright 2024 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// TestLeaderTransferQuorumFreshness checks that a leader cut off right after
// transferring leadership steps down within one election timeout
// (etcd-io/raft#99). Without the RecentActive clear in stepLeader's
// MsgTransferLeader case, replies from before the transfer keep it leader for
// up to two. Run with the transfer at several points in the CheckQuorum cycle.
func TestLeaderTransferQuorumFreshness(t *testing.T) {
	const et = 10

	// tick advances every node and delivers what they emit, honouring cut().
	tick := func(nt *network) {
		for _, id := range []uint64{1, 2, 3} {
			r := nt.peers[id].(*raft)
			r.tick()
			nt.send(nt.filter(r.readMessages())...)
		}
	}

	for _, transferAt := range []int{1, et / 2, et - 1} {
		nt := newNetworkWithConfig(func(c *Config) {
			c.CheckQuorum = true
			c.PreVote = false
		}, nil, nil, nil)
		nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

		n1 := nt.peers[1].(*raft)
		n2 := nt.peers[2].(*raft)
		require.Equal(t, StateLeader, n1.state)

		// Advance to the requested phase of the CheckQuorum cycle.
		for n1.electionElapsed != transferAt {
			tick(nt)
		}

		// Transfer to 2, then cut 1 off before any vote can reach it. The one
		// MsgTimeoutNow that already left is still delivered.
		require.NoError(t, n1.Step(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader}))
		inFlight := n1.readMessages()
		nt.cut(1, 2)
		nt.cut(1, 3)
		for _, m := range inFlight {
			if m.Type == pb.MsgTimeoutNow {
				require.NoError(t, nt.peers[m.To].Step(m))
			}
		}
		nt.send(nt.filter(n2.readMessages())...)
		require.Equal(t, StateLeader, n2.state, "transferAt=%d: transferee should win immediately", transferAt)
		require.Greater(t, n2.Term, n1.Term)

		// Keep 2 and 3 from campaigning again; we are only timing node 1.
		setRandomizedElectionTimeout(n2, 9*et)
		setRandomizedElectionTimeout(nt.peers[3].(*raft), 9*et)

		steppedDown := -1
		for i := 1; i <= 4*et; i++ {
			tick(nt)
			if n1.state != StateLeader {
				steppedDown = i
				break
			}
		}

		require.NotEqual(t, -1, steppedDown,
			"transferAt=%d: deposed leader never stepped down", transferAt)
		require.LessOrEqual(t, steppedDown, et,
			"transferAt=%d: deposed leader served for %d ticks after the transferee won; "+
				"CheckQuorum must bound this at one election timeout (%d)", transferAt, steppedDown, et)
	}
}
