// Copyright 2026 The etcd Authors
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

// TestLaggingHolderRenewalRefused (LIVE-2): a holder whose appends are lost,
// while its heartbeats, asks and grants still get through, must not be renewed
// forever. Its grant pins the commit clamp, so the catch-up check against
// committed always passed and commit stayed wedged although a quorum (1, 3)
// held every entry.
func TestLaggingHolderRenewalRefused(t *testing.T) {
	const durMicros = 100 * 1000
	waitLeaseAskWindow()
	rs := map[uint64]*raft{}
	for id := uint64(1); id <= 3; id++ {
		rs[id] = newRaft(incarnationTestConfig(id, newTestMemoryStorage(withPeers(1, 2, 3)), durMicros))
	}
	none := func(pb.Message) bool { return false }
	r1, r2 := rs[1], rs[2]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, none)
	require.Equal(t, StateLeader, r1.state)
	r2.maybeAskForReadLease()
	pumpMessages(t, rs, none)
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, none)
	require.True(t, r2.readOnly.hasActiveHeldLease())
	stuck := r1.raftLog.committed

	lossy := func(m pb.Message) bool {
		return m.To == 2 && (m.Type == pb.MsgApp || m.Type == pb.MsgSnap)
	}
	// About 5 lease durations, with a write every round.
	deadline := time.Now().Add(5 * durMicros * time.Microsecond)
	for time.Now().Before(deadline) {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w")}}}))
		for _, r := range rs {
			r.tick()
		}
		pumpMessages(t, rs, lossy)
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, StateLeader, r1.state)
	require.Equal(t, stuck, r2.raftLog.lastIndex(), "2 never received an append")
	require.Greater(t, r1.trk.Progress[3].Match, stuck+10, "the quorum (1, 3) moved on")
	require.False(t, r1.readOnly.hasActiveGrantedLease(2), "2's renewal was refused")
	require.Greater(t, r1.raftLog.committed, stuck+10, "commit is not wedged behind 2")
}

// TestNewIncarnationNotTreatedAsRenewal: 5b refuses only renewals. A holder
// that resets while its grant pins commit asks from a new incarnation, whose
// acks can never raise the old grant. That ask must replace the grant, as it
// did before 5b, instead of waiting out the old grant (up to D).
func TestNewIncarnationNotTreatedAsRenewal(t *testing.T) {
	rs, _ := incarnationTestGroup(t, true)
	r1, r2 := rs[1], rs[2]
	none := func(pb.Message) bool { return false }
	w0 := r1.raftLog.committed

	// 2 receives the writes but its acks are lost, so its grant pins commit
	// more than the margin below the quorum (1, 3).
	ackless := func(m pb.Message) bool {
		return (m.From == 2 && (m.Type == pb.MsgAppResp || m.Type == pb.MsgAskAckIndexResp)) ||
			(m.To == 2 && m.Type == pb.MsgAskAckIndex)
	}
	for i := 0; i < 12; i++ {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w")}}}))
		pumpMessages(t, rs, ackless)
	}
	require.Equal(t, w0, r1.raftLog.committed, "2's grant clamps commit")
	oldGrant := r1.readOnly.getGrantedLease(2)
	require.Greater(t, r1.trk.Committed(), oldGrant.AckedIndex+uint64(r1.readOnly.leaseCatchupMargin))

	// 2 times out into pre-candidacy (its pre-votes are lost), then hears the
	// leader again and asks from its new incarnation.
	oldIncarnation := r2.readOnly.incarnationID()
	r2.becomePreCandidate()
	r2.readMessages()
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, none)
	require.Equal(t, StateFollower, r2.state)
	require.NotEqual(t, oldIncarnation, r2.readOnly.incarnationID(), "reset() starts a new incarnation")
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, none)

	require.True(t, r2.readOnly.hasActiveHeldLease(), "the new incarnation was granted")
	require.Equal(t, r2.readOnly.incarnationID(), leaseIncarnation(r1.readOnly.getGrantedLease(2).LeaseId),
		"the new grant replaced the old incarnation's")

	// The grant starts at the asked index; the next append's acks, now from the
	// grant's incarnation, lift the clamp.
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w2")}}}))
	pumpMessages(t, rs, none)
	require.Equal(t, r1.raftLog.lastIndex(), r1.raftLog.committed, "the new incarnation's acks lift the clamp")
}
