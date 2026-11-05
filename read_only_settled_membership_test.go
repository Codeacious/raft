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

// A leader grants, installs and holds leases only while its membership is
// settled (membershipSettled), and its new-leader safeguard takes the shortcut
// only if membership was settled at election (settledAtElection).

// proposeLearner has leader r propose adding learner 4, and returns the change.
func proposeLearner(t *testing.T, r *raft) pb.ConfChange {
	t.Helper()
	cc := pb.ConfChange{Type: pb.ConfChangeAddLearnerNode, NodeID: 4}
	data, err := cc.Marshal()
	require.NoError(t, err)
	require.NoError(t, r.Step(pb.Message{From: r.id, To: r.id, Type: pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}}))
	r.readMessages()
	return cc
}

// commitAndApply has followers 2 and 3 acknowledge leader r's whole log, and
// applies what commits, cc included.
func commitAndApply(t *testing.T, r *raft, cc pb.ConfChange) {
	t.Helper()
	r.trk.Progress[2].MaybeUpdate(r.raftLog.lastIndex())
	r.trk.Progress[3].MaybeUpdate(r.raftLog.lastIndex())
	r.maybeCommit()
	require.Equal(t, r.raftLog.lastIndex(), r.raftLog.committed)
	r.appliedTo(r.raftLog.committed, 0)
	r.applyConfChange(cc.AsV2())
	r.readMessages()
}

// beatAndEcho sends a lease round and has followers 2 and 3 return it.
func beatAndEcho(t *testing.T, r *raft) {
	t.Helper()
	require.NoError(t, r.Step(pb.Message{From: r.id, To: r.id, Type: pb.MsgBeat}))
	round := latestLeaseRound(r)
	require.NotZero(t, round)
	echoLeaseRound(t, r, round, 2, 3)
}

// TestHeldGrantWaitsForSettledMembership: a follower request held for a lease
// round when the leader appends a conf change is neither granted nor refused by
// that round. It stays held, and the first round after the change applies
// grants it.
func TestHeldGrantWaitsForSettledMembership(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	askLease(t, r, 2, 7)
	require.Contains(t, r.readOnly.leader.pendingGrants, uint64(2))
	r.readMessages()

	cc := proposeLearner(t, r)
	require.False(t, r.membershipSettled())
	beatAndEcho(t, r)
	resps, _ := drainLeaseTraffic(r)
	require.Empty(t, resps, "no answer while membership is unsettled")
	require.Nil(t, r.readOnly.getGrantedLease(2))
	require.Contains(t, r.readOnly.leader.pendingGrants, uint64(2), "the request is held, not refused")

	commitAndApply(t, r, cc)
	require.True(t, r.membershipSettled())
	beatAndEcho(t, r)
	resps, _ = drainLeaseTraffic(r)
	require.Len(t, resps, 1)
	require.False(t, resps[0].Reject)
	require.NotNil(t, r.readOnly.getGrantedLease(2), "the held request is granted once settled")
}

// TestConfChangeDropsSelfLease: a leader appending a conf change drops its
// self-lease, takes none while the change is unapplied, and takes one again
// once it applies.
func TestConfChangeDropsSelfLease(t *testing.T) {
	for _, opt := range []ReadOnlyOption{ReadOnlyLeaseBased, ReadOnlyGrantLeases} {
		r := leasedLeader(t, opt)
		proposeSelfLease(r)
		require.True(t, completeLeaseRound(t, r))
		require.True(t, r.readOnly.hasActiveHeldLease(), "%v", opt)

		cc := proposeLearner(t, r)
		require.False(t, r.readOnly.hasActiveHeldLease(), "%v: the conf change should drop the self-lease", opt)
		proposeSelfLease(r)
		completeLeaseRound(t, r)
		require.False(t, r.readOnly.hasActiveHeldLease(), "%v: no self-lease while unsettled", opt)

		commitAndApply(t, r, cc)
		proposeSelfLease(r)
		require.True(t, completeLeaseRound(t, r))
		require.True(t, r.readOnly.hasActiveHeldLease(), "%v: a self-lease again once settled", opt)
	}
}

// TestRemovedLeaderTakesNoSelfLease: a leader whose inherited tail removes it
// leads on once the removal applies (no stepDownOnRemoval). Membership is then
// settled, and nothing banned it for the term, but as a non-member it takes no
// self-lease: no successor waits for a node outside its configuration.
func TestRemovedLeaderTakesNoSelfLease(t *testing.T) {
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

		commitAndApply(t, r, cc)
		require.Equal(t, StateLeader, r.state, "%v", opt)
		require.NotContains(t, r.trk.Progress, uint64(1), "%v", opt)
		require.True(t, r.membershipSettled(), "%v", opt)
		require.False(t, r.readOnly.leader.selfLeaseBanned, "%v: only membership stops it", opt)

		proposeSelfLease(r)
		completeLeaseRound(t, r)
		require.False(t, r.readOnly.hasActiveHeldLease(), "%v: a removed leader took a self-lease", opt)
	}
}

// TestConfChangeCountersFollowTheApplication: confHandedOut counts conf changes
// handed out in a Ready, and confApplied the application's ApplyConfChange
// calls, rejected changes (NodeID = None) included. Bootstrap's own apply is not
// the application's and is not counted. Membership is unsettled until the two
// agree, even once raft's applied has passed the change.
func TestConfChangeCountersFollowTheApplication(t *testing.T) {
	st := NewMemoryStorage()
	rn, err := NewRawNode(safeguardTestConfig(1, ReadOnlyGrantLeases, st))
	require.NoError(t, err)
	require.NoError(t, rn.Bootstrap([]Peer{{ID: 1}}))
	r := rn.raft
	require.Zero(t, r.confApplied, "bootstrap's own apply should not count")

	// handle persists and advances one Ready, applying each conf change it
	// hands out with apply, and returns the conf changes it handed out.
	handle := func(apply func(pb.ConfChangeV2)) (ccs int) {
		rd := rn.Ready()
		require.NoError(t, st.Append(rd.Entries))
		if !IsEmptyHardState(rd.HardState) {
			require.NoError(t, st.SetHardState(rd.HardState))
		}
		for _, e := range rd.CommittedEntries {
			if cc, ok := confChangeFromEntry(e); ok {
				ccs++
				apply(cc)
			}
		}
		rn.Advance(rd)
		return ccs
	}
	applyIt := func(cc pb.ConfChangeV2) { rn.ApplyConfChange(cc) }
	require.Equal(t, 1, handle(applyIt), "the bootstrap conf change is handed out")
	require.Equal(t, uint64(1), r.confHandedOut)
	require.Equal(t, uint64(1), r.confApplied)

	require.NoError(t, rn.Campaign())
	for rn.HasReady() {
		handle(applyIt)
	}
	require.Equal(t, StateLeader, r.state)
	require.True(t, r.committedEntryInCurrentTerm())

	// The application defers a conf change, then rejects it.
	require.NoError(t, rn.ProposeConfChange(pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: 2}))
	var deferred []pb.ConfChangeV2
	for rn.HasReady() && len(deferred) == 0 {
		handle(func(cc pb.ConfChangeV2) { deferred = append(deferred, cc) })
	}
	require.Len(t, deferred, 1)
	require.GreaterOrEqual(t, r.raftLog.applied, r.pendingConfIndex, "raft's applied passed the change")
	require.Equal(t, r.confApplied+1, r.confHandedOut)
	require.False(t, r.membershipSettled(), "unsettled until the application applies it")

	rn.ApplyConfChange(pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: None})
	require.Equal(t, r.confHandedOut, r.confApplied)
	require.True(t, r.membershipSettled(), "a rejected change counts as applied")
	require.NotContains(t, r.trk.Progress, uint64(2))
}

// TestSafeguardShortcutNeedsNoConfChangeInTail: a new leader whose unapplied
// tail holds an ordinary entry takes the safeguard shortcut once every node
// confirms. One whose tail holds a conf change does not: trk.Progress may miss a
// node holding a prior-term lease, so it waits out the window.
func TestSafeguardShortcutNeedsNoConfChangeInTail(t *testing.T) {
	learner, err := (&pb.ConfChange{Type: pb.ConfChangeAddLearnerNode, NodeID: 4}).Marshal()
	require.NoError(t, err)
	for _, tt := range []struct {
		name       string
		ent        pb.Entry
		noShortcut bool
	}{
		{"normal", pb.Entry{Data: []byte("x")}, false},
		{"conf-change", pb.Entry{Type: pb.EntryConfChange, Data: learner}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rs := map[uint64]*raft{}
			for id := uint64(1); id <= 3; id++ {
				rs[id] = newRaft(safeguardTestConfig(id, ReadOnlyGrantLeases, newTestMemoryStorage(withPeers(1, 2, 3))))
			}
			none := func(pb.Message) bool { return false }
			r1, r2 := rs[1], rs[2]
			require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
			pumpMessages(t, rs, none)
			require.Equal(t, StateLeader, r1.state)
			for _, r := range rs {
				applyCommitted(t, r)
			}

			// The entry reaches 2 and 3, but 1 never learns it committed.
			require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{tt.ent}}))
			pumpMessages(t, rs, func(m pb.Message) bool { return m.To == 1 && m.Type == pb.MsgAppResp })
			require.Less(t, r2.raftLog.applied, r2.raftLog.lastIndex(), "2 should hold an unapplied tail")

			require.NoError(t, r2.Step(pb.Message{From: 1, To: 2, Term: r1.Term, Type: pb.MsgTimeoutNow}))
			pumpMessages(t, rs, none)
			require.Equal(t, StateLeader, r2.state)
			require.Equal(t, tt.noShortcut, r2.readOnly.safeguard.noShortcut)
			require.Equal(t, !tt.noShortcut, r2.committedEntryInCurrentTerm(),
				"the shortcut should lift only without a conf change in the tail")
		})
	}
}

// TestRemovalRefusedWhileTargetHoldsLease: a removal of a node holding a live
// lease from the leader is refused, so the node's grant cannot leave the clamp
// while it serves. Were it accepted, applying it would drop 3's grant, and 3,
// told the removal committed but not yet applied, would serve below a write
// committed without it.
func TestRemovalRefusedWhileTargetHoldsLease(t *testing.T) {
	waitLeaseAskWindow()
	rs := map[uint64]*raft{}
	for id := uint64(1); id <= 3; id++ {
		rs[id] = newRaft(safeguardTestConfig(id, ReadOnlyGrantLeases, newTestMemoryStorage(withPeers(1, 2, 3))))
	}
	none := func(pb.Message) bool { return false }
	r1, r3 := rs[1], rs[3]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, none)
	require.Equal(t, StateLeader, r1.state)
	for _, r := range rs {
		applyCommitted(t, r)
	}
	r3.maybeAskForReadLease()
	pumpMessages(t, rs, none)
	if !r3.readOnly.hasActiveHeldLease() {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
		pumpMessages(t, rs, none)
	}
	require.True(t, r3.readOnly.hasActiveHeldLease(), "3 should hold a lease")

	data, err := (&pb.ConfChange{Type: pb.ConfChangeRemoveNode, NodeID: 3}).Marshal()
	require.NoError(t, err)
	err = r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}})
	pumpMessages(t, rs, none)
	for _, id := range []uint64{1, 2} {
		applyCommitted(t, rs[id])
	}
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp,
		Entries: []pb.Entry{{Data: []byte("w")}}}))
	pumpMessages(t, rs, none)
	for _, s := range stepLocalRead(t, r3, "rd") {
		require.GreaterOrEqual(t, s.Index, r1.raftLog.committed,
			"STALE READ: removed node 3 served a read below a committed write")
	}
	require.ErrorIs(t, err, ErrProposalDropped, "the removal should be refused while 3 holds a lease")
}

// TestRefusedConfChangeLeavesNothingPending: a conf change the leader refuses
// to append (here, over the uncommitted-size limit) must not leave
// pendingConfIndex past the log, or membership would stay unsettled on an idle
// group until other entries filled the gap.
func TestRefusedConfChangeLeavesNothingPending(t *testing.T) {
	cfg := safeguardTestConfig(1, ReadOnlyGrantLeases, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.MaxUncommittedEntriesSize = 1
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	r.readOnly.safeguard.markPassed()
	require.NoError(t, r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp,
		Entries: []pb.Entry{{Data: []byte("x")}}}))
	r.readMessages()

	data, err := (&pb.ConfChange{Type: pb.ConfChangeAddLearnerNode, NodeID: 4}).Marshal()
	require.NoError(t, err)
	err = r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}})
	require.ErrorIs(t, err, ErrProposalDropped, "the uncommitted-size limit should refuse the conf change")
	require.LessOrEqual(t, r.pendingConfIndex, r.raftLog.lastIndex(), "nothing should be pending past the log")

	r.trk.Progress[2].MaybeUpdate(r.raftLog.lastIndex())
	r.trk.Progress[3].MaybeUpdate(r.raftLog.lastIndex())
	r.maybeCommit()
	r.appliedTo(r.raftLog.committed, 0)
	require.True(t, r.membershipSettled(), "settled once the log is applied, with no further writes")
}
