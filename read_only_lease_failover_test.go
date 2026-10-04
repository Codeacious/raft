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
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// TestLeaseBasedReadNotStaleAfterFailover checks that, under
// ReadOnlyLeaseBased, a leader cut off from the group never answers a read
// below what its successor has committed (etcd-io/raft#166). Checked at every
// tick, plus a liveness check that the new leader does eventually commit.
func TestLeaseBasedReadNotStaleAfterFailover(t *testing.T) {
	const et = 10

	tick := func(nt *network) {
		for _, id := range []uint64{1, 2, 3} {
			r := nt.peers[id].(*raft)
			r.tick()
			nt.send(nt.filter(r.readMessages())...)
		}
	}
	// readIndexAt returns the index this node would answer a read with, and
	// whether it answered at all.
	readIndexAt := func(r *raft) (uint64, bool) {
		before := len(r.readStates)
		_ = r.Step(pb.Message{From: r.id, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: []byte("probe")}}})
		r.advanceMessagesAfterAppend()
		if len(r.readStates) == before {
			return 0, false
		}
		return r.readStates[len(r.readStates)-1].Index, true
	}

	nt := newNetworkWithConfig(func(c *Config) {
		c.CheckQuorum = true
		c.PreVote = false
		c.ReadOnlyOption = ReadOnlyLeaseBased
		// Short, because the new-leader safeguard runs on wall-clock time and a
		// tick loop passes none. Paired with the sleep below.
		c.ReadLeaseDurationMicros = 5000
	}, nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})

	n1 := nt.peers[1].(*raft)
	n2 := nt.peers[2].(*raft)
	require.Equal(t, StateLeader, n1.state)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("foo")}}})

	// Run past a CheckQuorum check, so the old leader stays leader as long as it
	// can once cut off.
	for i := 0; i < et+1; i++ {
		tick(nt)
	}
	staleCommitted := n1.raftLog.committed
	require.NotZero(t, staleCommitted)

	nt.cut(1, 2)
	nt.cut(1, 3)
	// Hold the election by hand after exactly one election timeout of silence,
	// the earliest a new leader can appear.
	setRandomizedElectionTimeout(n2, 4*et)
	setRandomizedElectionTimeout(nt.peers[3].(*raft), 4*et)
	for i := 0; i < et; i++ {
		tick(nt)
	}
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgHup})
	require.Equal(t, StateLeader, n2.state, "new leader should win after one election timeout of silence")
	// Whether the old leader has stepped down yet is deliberately not asserted.
	t.Logf("at the moment the new leader won: deposed leader state=%v", n1.state)

	// A client writes to the new leader. Under #166 this commits immediately and
	// the old leader answers reads below it.
	nt.send(pb.Message{From: 2, To: 2, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("bar")}}})

	committedAt, steppedDownAt := -1, -1
	for i := 0; i <= 6*et; i++ {
		time.Sleep(time.Millisecond) // let the safeguard's wall clock run
		// The old leader must never answer a read below what the new leader has
		// committed.
		if idx, ok := readIndexAt(n1); ok && n1.state == StateLeader {
			require.GreaterOrEqual(t, idx, n2.raftLog.committed,
				"tick %d: deposed leader answered a read at index %d while the new leader had "+
					"committed %d -- stale read across failover (etcd-io/raft#166)",
				i, idx, n2.raftLog.committed)
		}
		if n1.state != StateLeader && steppedDownAt < 0 {
			steppedDownAt = i
		}
		if n2.raftLog.committed > staleCommitted && committedAt < 0 {
			committedAt = i
		}
		if steppedDownAt >= 0 && committedAt >= 0 {
			break
		}
		tick(nt)
	}

	// Liveness: a safeguard that never lifts would pass the check above
	// trivially.
	require.NotEqual(t, -1, committedAt, "new leader never committed; safeguard did not lift")
	// Step-down timing is not asserted: a cut-off leader stops serving when its
	// lease expires, however long it goes on believing it leads.
	t.Logf("deposed leader stepped down at tick %d; new leader's first commit at tick %d",
		steppedDownAt, committedAt)
}
