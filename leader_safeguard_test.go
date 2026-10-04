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

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// safeguardTestConfig uses a 60 s lease, so the new-leader safeguard can only
// lift early (by confirmations), never by its timer, within a test.
func safeguardTestConfig(id uint64, opt ReadOnlyOption, st *MemoryStorage) *Config {
	cfg := newTestConfig(id, 10, 1, st)
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = opt
	cfg.ReadLeaseDurationMicros = 60 * 1000 * 1000
	cfg.MaxNumReadLeases = 5
	cfg.AskForReadLease = true
	cfg.ReadLeaseCatchupMargin = 10
	return cfg
}

// pumpMessages delivers messages among rs until none are left, skipping any
// that drop reports true for.
func pumpMessages(t *testing.T, rs map[uint64]*raft, drop func(pb.Message) bool) {
	t.Helper()
	for i := 0; i < 100; i++ {
		var msgs []pb.Message
		for _, r := range rs {
			msgs = append(msgs, r.readMessages()...)
		}
		if len(msgs) == 0 {
			return
		}
		for _, m := range msgs {
			if r, ok := rs[m.To]; ok && !drop(m) {
				_ = r.Step(m)
			}
		}
	}
	t.Fatal("messages did not settle")
}

// TestNewLeaderKeepsCandidateConfirmations checks that the vote responses a
// candidate collects still count toward the safeguard shortcut once it wins.
// becomeLeader's reset used to discard them, so the winning votes never
// counted and the shortcut waited for append responses.
func TestNewLeaderKeepsCandidateConfirmations(t *testing.T) {
	for _, opt := range []ReadOnlyOption{ReadOnlyLeaseBased, ReadOnlyGrantLeases} {
		rs := map[uint64]*raft{}
		for id := uint64(1); id <= 3; id++ {
			rs[id] = newRaft(safeguardTestConfig(id, opt, newTestMemoryStorage(withPeers(1, 2, 3))))
		}
		r1 := rs[1]
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
		// Deliver the vote requests and both responses. The first response wins
		// the election; the second arrives at the new leader.
		for _, m := range r1.readMessages() {
			require.NoError(t, rs[m.To].Step(m))
		}
		for _, id := range []uint64{2, 3} {
			for _, m := range rs[id].readMessages() {
				require.NoError(t, r1.Step(m))
			}
		}
		require.Equal(t, StateLeader, r1.state)
		require.True(t, r1.readOnly.safeguard.isConfirmedAtTerm(2), "%v: winning vote should count", opt)
		require.True(t, r1.readOnly.safeguard.isConfirmedAtTerm(3), "%v: late vote should count", opt)

		// Drop the leader's appends, so no append response can confirm anyone.
		r1.readMessages()
		require.NoError(t, r1.Step(pb.Message{
			From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("x")}},
		}), "%v: the vote responses alone should lift the safeguard", opt)
	}
}

// TestDemotedOrRemovedLeaderNoStaleRead covers a previous leader that is
// demoted or removed by an entry the new leader has applied and it has not.
//
// The old leader holds a self-lease and proposes the change itself. Nodes 2
// and 3 apply it; node 1 does not. Node 2 then wins by transfer with node 1 cut
// off, and commits. Node 1 must not answer a read below that commit.
//
// Without the fixes, the demoted case lifted the new leader's safeguard because
// LeaseBased skipped learners, and the removed case because the removed leader
// is in no required set while it keeps its self-lease. Both served stale reads.
func TestDemotedOrRemovedLeaderNoStaleRead(t *testing.T) {
	for _, tt := range []struct {
		name string
		opt  ReadOnlyOption
		cc   pb.ConfChangeType
	}{
		{"leasebased-demote", ReadOnlyLeaseBased, pb.ConfChangeAddLearnerNode},
		{"leasebased-remove", ReadOnlyLeaseBased, pb.ConfChangeRemoveNode},
		{"grantleases-demote", ReadOnlyGrantLeases, pb.ConfChangeAddLearnerNode},
		{"grantleases-remove", ReadOnlyGrantLeases, pb.ConfChangeRemoveNode},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rs := map[uint64]*raft{}
			for id := uint64(1); id <= 3; id++ {
				rs[id] = newRaft(safeguardTestConfig(id, tt.opt, newTestMemoryStorage(withPeers(1, 2, 3))))
			}
			none := func(pb.Message) bool { return false }
			r1, r2 := rs[1], rs[2]
			require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
			pumpMessages(t, rs, none)
			require.Equal(t, StateLeader, r1.state)
			require.NoError(t, r1.Step(pb.Message{
				From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("a")}},
			}))
			pumpMessages(t, rs, none)
			r1.maybeRenewSelfLease()
			r1.maybeSendLeaseRound()
			pumpMessages(t, rs, none)
			require.True(t, r1.readOnly.hasActiveHeldLease(), "old leader should hold a self-lease")
			r1.appliedTo(r1.raftLog.committed, 0) // so it accepts a conf change

			cc := pb.ConfChange{Type: tt.cc, NodeID: 1}
			data, err := cc.Marshal()
			require.NoError(t, err)
			require.NoError(t, r1.Step(pb.Message{
				From: 1, To: 1, Type: pb.MsgProp,
				Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}},
			}))
			ccIndex := r1.raftLog.lastIndex()
			ents, err := r1.raftLog.entries(ccIndex, noLimit)
			require.NoError(t, err)
			require.Equal(t, pb.EntryConfChange, ents[0].Type, "conf change should be appended as is")
			pumpMessages(t, rs, none)
			require.GreaterOrEqual(t, r2.raftLog.committed, ccIndex)
			oldTerm := r1.Term

			for _, id := range []uint64{2, 3} {
				rs[id].applyConfChange(cc.AsV2())
				rs[id].appliedTo(rs[id].raftLog.committed, 0)
			}

			cut1 := func(m pb.Message) bool { return m.To == 1 || m.From == 1 }
			require.NoError(t, r2.Step(pb.Message{From: 1, To: 2, Term: oldTerm, Type: pb.MsgTimeoutNow}))
			pumpMessages(t, rs, cut1)
			require.Equal(t, StateLeader, r2.state)
			_ = r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("b")}}})
			pumpMessages(t, rs, cut1)

			r1.readStates = nil
			require.NoError(t, r1.Step(pb.Message{
				From: 1, To: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: []byte("rd")}},
			}))
			for _, rs := range r1.readStates {
				require.GreaterOrEqual(t, rs.Index, r2.raftLog.committed,
					"old leader served a read below the new leader's commit")
			}
		})
	}
}

// TestSelfRemovalBansSelfLease checks that a leader proposing its own removal
// drops its self-lease before the entry is appended, that the proposal is
// still accepted, and that it cannot take a new self-lease for the term.
func TestSelfRemovalBansSelfLease(t *testing.T) {
	for _, opt := range []ReadOnlyOption{ReadOnlyLeaseBased, ReadOnlyGrantLeases} {
		r := leasedLeader(t, opt)
		proposeSelfLease(r)
		require.True(t, completeLeaseRound(t, r))
		require.True(t, r.readOnly.hasActiveHeldLease())

		cc := pb.ConfChange{Type: pb.ConfChangeRemoveNode, NodeID: 1}
		data, err := cc.Marshal()
		require.NoError(t, err)
		require.NoError(t, r.Step(pb.Message{
			From: 1, To: 1, Type: pb.MsgProp,
			Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}},
		}), "%v: the removal should be accepted", opt)
		require.False(t, r.readOnly.hasActiveHeldLease(), "%v: self-lease should be dropped", opt)

		proposeSelfLease(r)
		completeLeaseRound(t, r)
		require.False(t, r.readOnly.hasActiveHeldLease(), "%v: a leader being removed must not renew", opt)
	}
}

// TestInheritedSelfRemovalBansSelfLease covers the removal a leader did not
// propose: one of itself, left unapplied in the log by an earlier leader. It
// must not take a self-lease for the term.
func TestInheritedSelfRemovalBansSelfLease(t *testing.T) {
	for _, opt := range []ReadOnlyOption{ReadOnlyLeaseBased, ReadOnlyGrantLeases} {
		st := newTestMemoryStorage(withPeers(1, 2, 3))
		cc := pb.ConfChange{Type: pb.ConfChangeRemoveNode, NodeID: 1}
		data, err := cc.Marshal()
		require.NoError(t, err)
		require.NoError(t, st.Append([]pb.Entry{{Term: 1, Index: 1, Type: pb.EntryConfChange, Data: data}}))
		r := newRaft(safeguardTestConfig(1, opt, st))
		r.becomeCandidate()
		r.becomeLeader()
		r.readOnly.safeguard.markPassed()

		proposeSelfLease(r)
		completeLeaseRound(t, r)
		require.False(t, r.readOnly.hasActiveHeldLease(), "%v: inherited self-removal must block the self-lease", opt)
	}
}
