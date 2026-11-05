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

// A voter removed through joint consensus is named only by the enter-joint
// entry. The leave-joint entry that actually removes it names nobody. The lease
// drain finds removal targets in entries (the MsgProp pre-pass and the new
// leader's unapplied-tail scan), so a leader elected after enter-joint was
// applied does not know the outgoing voter is leaving (SAFE-4). These tests
// check that the outgoing voter cannot serve a read below a write committed
// after its removal.

var jointTransitions = []struct {
	name       string
	transition pb.ConfChangeTransition
}{
	{"explicit", pb.ConfChangeTransitionJointExplicit},
	{"auto-leave", pb.ConfChangeTransitionJointImplicit},
}

// applyCommitted applies r's committed entries in order, conf changes included.
func applyCommitted(t *testing.T, r *raft) {
	t.Helper()
	lo, hi := r.raftLog.applied+1, r.raftLog.committed+1
	if lo >= hi {
		return
	}
	ents, err := r.raftLog.slice(lo, hi, noLimit)
	require.NoError(t, err)
	for _, e := range ents {
		if cc, ok := confChangeFromEntry(e); ok {
			r.applyConfChange(cc)
		}
		r.appliedTo(e.Index, 0)
	}
}

// jointRemovalGroup starts a 4-node group led by 1 and commits an enter-joint
// change removing 4. Nodes 2-4 apply it. Node 1 does not, so a leader's
// auto-leave cannot fire before the caller moves leadership away from 1.
func jointRemovalGroup(t *testing.T, opt ReadOnlyOption, transition pb.ConfChangeTransition) map[uint64]*raft {
	return jointChangeGroup(t, opt, transition, pb.ConfChangeRemoveNode)
}

// jointChangeGroup is jointRemovalGroup with change ccType applied to 4.
func jointChangeGroup(t *testing.T, opt ReadOnlyOption, transition pb.ConfChangeTransition,
	ccType pb.ConfChangeType) map[uint64]*raft {
	t.Helper()
	waitLeaseAskWindow()
	rs := map[uint64]*raft{}
	for id := uint64(1); id <= 4; id++ {
		rs[id] = newRaft(safeguardTestConfig(id, opt, newTestMemoryStorage(withPeers(1, 2, 3, 4))))
	}
	none := func(pb.Message) bool { return false }
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, none)
	require.Equal(t, StateLeader, r1.state)
	require.True(t, r1.committedEntryInCurrentTerm())
	for _, r := range rs {
		applyCommitted(t, r)
	}

	enter := pb.ConfChangeV2{Transition: transition,
		Changes: []pb.ConfChangeSingle{{Type: ccType, NodeID: 4}}}
	data, err := enter.Marshal()
	require.NoError(t, err)
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: data}}}))
	pumpMessages(t, rs, none)
	require.Equal(t, r1.raftLog.lastIndex(), r1.raftLog.committed)
	for _, id := range []uint64{2, 3, 4} {
		applyCommitted(t, rs[id])
		require.NotEmpty(t, rs[id].trk.Config.Voters[1], "%x should be in a joint config", id)
	}
	return rs
}

// transferJointLeadership moves leadership from 1 to to, then has 1 apply the
// enter-joint as a follower. The new leader does not apply its own entries, so
// an auto-leave has not been proposed yet.
func transferJointLeadership(t *testing.T, rs map[uint64]*raft, to uint64) {
	t.Helper()
	none := func(pb.Message) bool { return false }
	require.NoError(t, rs[to].Step(pb.Message{From: 1, To: to, Term: rs[1].Term, Type: pb.MsgTimeoutNow}))
	pumpMessages(t, rs, none)
	require.Equal(t, StateLeader, rs[to].state)
	// A joint config is unsettled, so the safeguard takes no shortcut. Lift it
	// rather than wait out a lease duration (no node holds a lease from before
	// the election), and let the next tick retry the suppressed commit.
	require.True(t, rs[to].readOnly.safeguard.noShortcut, "a joint config should disable the shortcut")
	rs[to].readOnly.safeguard.markPassed()
	rs[to].tick()
	pumpMessages(t, rs, none)
	require.True(t, rs[to].committedEntryInCurrentTerm())
	applyCommitted(t, rs[1])
	require.NotEmpty(t, rs[1].trk.Config.Voters[1], "1 should be in a joint config")
}

// leaveJoint has leader r leave the joint configuration. It applies r's log,
// which proposes the auto-leave if the transition is implicit, and otherwise
// proposes an empty ConfChangeV2. A refused proposal is logged, not failed.
func leaveJoint(t *testing.T, r *raft, transition pb.ConfChangeTransition) {
	t.Helper()
	applyCommitted(t, r)
	if transition != pb.ConfChangeTransitionJointExplicit {
		return
	}
	leave := pb.ConfChangeV2{}
	data, err := leave.Marshal()
	require.NoError(t, err)
	if err := r.Step(pb.Message{From: r.id, To: r.id, Type: pb.MsgProp,
		Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: data}}}); err != nil {
		t.Logf("leave-joint refused: %v", err)
	}
}

// TestJointRemovalDoesNotLeakFollowerLease: 2 is elected after the enter-joint
// removing 4 was applied, so its tail scan bans nobody and 4 can be granted a
// lease. Leave-joint names nobody, so it is neither refused nor banned, and
// applying it drops 4's grant from the clamp. 4 learns the leave-joint
// committed, but has not applied it yet, and the leader sends it nothing more.
// It must not serve a read below a later write.
func TestJointRemovalDoesNotLeakFollowerLease(t *testing.T) {
	for _, tt := range jointTransitions {
		t.Run(tt.name, func(t *testing.T) {
			rs := jointRemovalGroup(t, ReadOnlyGrantLeases, tt.transition)
			none := func(pb.Message) bool { return false }
			r2, r4 := rs[2], rs[4]
			transferJointLeadership(t, rs, 2)

			r4.maybeAskForReadLease()
			pumpMessages(t, rs, none)
			if !r4.readOnly.hasActiveHeldLease() {
				require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgBeat}))
				pumpMessages(t, rs, none)
			}
			t.Logf("outgoing voter 4 holds a lease: %v", r4.readOnly.hasActiveHeldLease())

			leaveJoint(t, r2, tt.transition)
			pumpMessages(t, rs, none)
			for _, id := range []uint64{1, 2, 3} {
				applyCommitted(t, rs[id])
			}
			t.Logf("2's config after leave-joint: %s; 4 still granted: %v",
				r2.trk.Config, r2.readOnly.getGrantedLease(4) != nil)

			require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgProp,
				Entries: []pb.Entry{{Data: []byte("w")}}}))
			pumpMessages(t, rs, none)
			t.Logf("write at %d; 2 committed %d; 4 committed %d",
				r2.raftLog.lastIndex(), r2.raftLog.committed, r4.raftLog.committed)

			for _, s := range stepLocalRead(t, r4, "rd") {
				require.GreaterOrEqual(t, s.Index, r2.raftLog.committed,
					"STALE READ: removed voter 4 served a read below a committed write")
			}
		})
	}
}

// TestJointRemovalDoesNotLeakSelfLease: the outgoing voter 4 wins leadership
// while the configuration is joint, so neither its tail scan nor the leave-joint
// proposal bans its self-lease. Once it has left, 4 is in no configuration, so
// the successor's safeguard shortcut does not wait for it. With 4 cut off, 1-3
// elect 2 and commit a write. 4 must not serve a read below it.
func TestJointRemovalDoesNotLeakSelfLease(t *testing.T) {
	for _, mode := range []struct {
		name string
		opt  ReadOnlyOption
	}{{"leasebased", ReadOnlyLeaseBased}, {"grantleases", ReadOnlyGrantLeases}} {
		for _, tt := range jointTransitions {
			t.Run(mode.name+"/"+tt.name, func(t *testing.T) {
				rs := jointRemovalGroup(t, mode.opt, tt.transition)
				none := func(pb.Message) bool { return false }
				r2, r4 := rs[2], rs[4]
				transferJointLeadership(t, rs, 4)

				r4.maybeRenewSelfLease()
				r4.maybeSendLeaseRound()
				pumpMessages(t, rs, none)
				t.Logf("outgoing voter 4 holds a self-lease: %v", r4.readOnly.hasActiveHeldLease())

				leaveJoint(t, r4, tt.transition)
				pumpMessages(t, rs, none)
				for _, r := range rs {
					applyCommitted(t, r)
				}
				t.Logf("4's config after leave-joint: %s; 4 is %s, self-lease: %v",
					r4.trk.Config, r4.state, r4.readOnly.hasActiveHeldLease())

				cut4 := func(m pb.Message) bool { return m.To == 4 || m.From == 4 }
				for _, id := range []uint64{1, 3} {
					rs[id].electionElapsed = rs[id].electionTimeout
				}
				require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgHup}))
				pumpMessages(t, rs, cut4)
				require.Equal(t, StateLeader, r2.state)
				require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgProp,
					Entries: []pb.Entry{{Data: []byte("w")}}}))
				pumpMessages(t, rs, cut4)
				t.Logf("write at %d; 2 committed %d; 4 committed %d",
					r2.raftLog.lastIndex(), r2.raftLog.committed, r4.raftLog.committed)

				for _, s := range stepLocalRead(t, r4, "rd") {
					require.GreaterOrEqual(t, s.Index, r2.raftLog.committed,
						"STALE READ: removed ex-leader 4 served a read below the successor's commit")
				}
			})
		}
	}
}

// TestJointRemovalStillLeavesJoint checks that the lease pause does not hold
// up the removal: a leader elected after enter-joint gives the outgoing voter no
// lease while the config is joint, and its leave-joint, proposed or automatic,
// is accepted and commits.
func TestJointRemovalStillLeavesJoint(t *testing.T) {
	for _, tt := range jointTransitions {
		t.Run(tt.name, func(t *testing.T) {
			rs := jointRemovalGroup(t, ReadOnlyGrantLeases, tt.transition)
			none := func(pb.Message) bool { return false }
			r2, r4 := rs[2], rs[4]
			transferJointLeadership(t, rs, 2)
			require.False(t, r2.membershipSettled(), "a joint config is unsettled")
			r4.maybeAskForReadLease()
			pumpMessages(t, rs, none)
			require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgBeat}))
			pumpMessages(t, rs, none)
			require.False(t, r4.readOnly.hasActiveHeldLease(), "the outgoing voter should get no lease")

			leaveJoint(t, r2, tt.transition)
			pumpMessages(t, rs, none)
			applyCommitted(t, r2)
			require.Empty(t, r2.trk.Config.Voters[1], "2 should have left the joint config")
			require.NotContains(t, r2.trk.Progress, uint64(4))
		})
	}
}

// TestJointConfigPausesLeases checks that no lease is granted or held while the
// config is joint, here demoting 4 to a learner, and that leases resume once the
// leader has left it. 4's request, made while joint, is held rather than
// refused. A demoted node keeps its Progress, so as a learner it is granted a
// lease again.
func TestJointConfigPausesLeases(t *testing.T) {
	for _, tt := range jointTransitions {
		t.Run(tt.name, func(t *testing.T) {
			rs := jointChangeGroup(t, ReadOnlyGrantLeases, tt.transition, pb.ConfChangeAddLearnerNode)
			none := func(pb.Message) bool { return false }
			r2, r4 := rs[2], rs[4]
			require.Contains(t, r2.trk.Config.LearnersNext, uint64(4), "4 should be demoting")
			transferJointLeadership(t, rs, 2)

			askLease := func() {
				time.Sleep(time.Duration(_LEASE_ASK_INTERVAL_MICROS+1000) * time.Microsecond)
				r4.maybeAskForReadLease()
				pumpMessages(t, rs, none)
				require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgBeat}))
				pumpMessages(t, rs, none)
				r2.maybeRenewSelfLease()
				r2.maybeSendLeaseRound()
				pumpMessages(t, rs, none)
			}
			askLease()
			require.False(t, r4.readOnly.hasActiveHeldLease(), "no lease while joint")
			require.False(t, r2.readOnly.hasActiveHeldLease(), "no self-lease while joint")
			require.Contains(t, r2.readOnly.leader.pendingGrants, uint64(4), "4's request should be held")

			leaveJoint(t, r2, tt.transition)
			pumpMessages(t, rs, none)
			for _, r := range rs {
				applyCommitted(t, r)
			}
			require.Empty(t, r2.trk.Config.Voters[1], "2 should have left the joint config")
			require.True(t, r2.trk.Progress[4].IsLearner)
			askLease()
			require.True(t, r4.readOnly.hasActiveHeldLease(), "learner 4 should be granted a lease")
			require.True(t, r2.readOnly.hasActiveHeldLease(), "2 should take a self-lease")
		})
	}
}

// failingEntriesStorage fails Entries calls as storage that lost its unapplied
// entries would: the next call once failNext is set, and every call reading
// from an index at or below failThrough.
type failingEntriesStorage struct {
	*MemoryStorage
	failNext    bool
	failThrough uint64
	failures    int
}

func (s *failingEntriesStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	if s.failNext || lo <= s.failThrough {
		s.failNext = false
		s.failures++
		return nil, ErrCompacted
	}
	return s.MemoryStorage.Entries(lo, hi, maxSize)
}

// persistLog moves r's unstable entries into st, so that reading them goes to
// storage.
func persistLog(t *testing.T, r *raft, st *failingEntriesStorage) {
	t.Helper()
	ents := r.raftLog.nextUnstableEnts()
	if len(ents) == 0 {
		return
	}
	require.NoError(t, st.Append(ents))
	last := ents[len(ents)-1]
	r.raftLog.stableTo(entryID{term: last.Term, index: last.Index})
}

// scanFailureRemovals are the removals of 4 the scan-failure tests leave in
// the new leader's unapplied tail: an enter-joint, and a simple RemoveNode.
func scanFailureRemovals(t *testing.T) []struct {
	name  string
	ent   pb.Entry
	cc    pb.ConfChangeV2
	joint bool
} {
	enter := pb.ConfChangeV2{Transition: pb.ConfChangeTransitionJointExplicit,
		Changes: []pb.ConfChangeSingle{{Type: pb.ConfChangeRemoveNode, NodeID: 4}}}
	enterData, err := enter.Marshal()
	require.NoError(t, err)
	simple := pb.ConfChange{Type: pb.ConfChangeRemoveNode, NodeID: 4}
	simpleData, err := simple.Marshal()
	require.NoError(t, err)
	return []struct {
		name  string
		ent   pb.Entry
		cc    pb.ConfChangeV2
		joint bool
	}{
		{"joint", pb.Entry{Type: pb.EntryConfChangeV2, Data: enterData}, enter, true},
		{"simple", pb.Entry{Type: pb.EntryConfChange, Data: simpleData}, simple.AsV2(), false},
	}
}

// scanFailureGroup starts a 4-node group led by 1, in which 1 appends ent (a
// removal of 4) and is lost before it learns the entry committed. 2 then wins
// an election without 1, with ent in its unapplied tail, and its scan of that
// tail reads st2, which the caller has set up to fail. Returns the group, 2's
// storage and the index of ent. The safeguard is lifted, and ent committed.
func scanFailureGroup(t *testing.T, ent pb.Entry, failStorage func(st2 *failingEntriesStorage, tail uint64)) (
	map[uint64]*raft, *failingEntriesStorage, uint64) {
	t.Helper()
	waitLeaseAskWindow()
	st2 := &failingEntriesStorage{MemoryStorage: newTestMemoryStorage(withPeers(1, 2, 3, 4))}
	rs := map[uint64]*raft{}
	for id := uint64(1); id <= 4; id++ {
		cfg := safeguardTestConfig(id, ReadOnlyGrantLeases, newTestMemoryStorage(withPeers(1, 2, 3, 4)))
		if id == 2 {
			cfg.Storage = st2
		}
		rs[id] = newRaft(cfg)
	}
	none := func(pb.Message) bool { return false }
	r1, r2 := rs[1], rs[2]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, none)
	require.Equal(t, StateLeader, r1.state)
	for _, r := range rs {
		applyCommitted(t, r)
	}

	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{ent}}))
	pumpMessages(t, rs, func(m pb.Message) bool { return m.To == 1 && m.Type == pb.MsgAppResp })
	tail := r2.raftLog.lastIndex()
	require.Less(t, r2.raftLog.committed, tail, "the removal should be uncommitted at 2")

	persistLog(t, r2, st2)
	failStorage(st2, tail)
	for _, id := range []uint64{3, 4} {
		rs[id].electionElapsed = rs[id].electionTimeout
	}
	require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgHup}))
	pumpMessages(t, rs, cutScanFailure1)
	require.Equal(t, StateLeader, r2.state)
	require.True(t, r2.readOnly.safeguard.noShortcut, "a failed scan should disable the shortcut")
	require.False(t, r2.newLeaderSafeguardPassed(), "every node confirmed, but the shortcut is off")
	// Lift the safeguard rather than wait out a lease duration.
	r2.readOnly.safeguard.markPassed()
	require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgProp,
		Entries: []pb.Entry{{Data: []byte("x")}}}))
	pumpMessages(t, rs, cutScanFailure1)
	require.GreaterOrEqual(t, r2.raftLog.committed, tail)
	return rs, st2, tail
}

// cutScanFailure1 drops everything to and from 1, which is gone.
func cutScanFailure1(m pb.Message) bool { return m.To == 1 || m.From == 1 }

// askForScanFailureLeases has 3 and 4 ask leader 2 for a lease and 2 try to
// take a self-lease, without ticking 2.
func askForScanFailureLeases(t *testing.T, rs map[uint64]*raft) {
	t.Helper()
	time.Sleep(time.Duration(_LEASE_ASK_INTERVAL_MICROS+1000) * time.Microsecond)
	for _, id := range []uint64{3, 4} {
		rs[id].maybeAskForReadLease()
	}
	pumpMessages(t, rs, cutScanFailure1)
	require.NoError(t, rs[2].Step(pb.Message{From: 2, To: 2, Type: pb.MsgBeat}))
	pumpMessages(t, rs, cutScanFailure1)
	rs[2].maybeRenewSelfLease()
	rs[2].maybeSendLeaseRound()
	pumpMessages(t, rs, cutScanFailure1)
}

// TestScanFailureDisablesShortcut: 2's election-time scan of its unapplied
// tail fails, so it cannot rule out a conf change there, and its safeguard takes
// no shortcut although every node confirms. Leases do not read the tail: 2
// grants none, and holds none, until it has applied the tail, a removal of 4
// (and, for a joint removal, has left the joint config). It then grants as
// usual, never to 4. In the "unreadable" case the tail never becomes readable,
// and the application applies it from its own copy.
func TestScanFailureDisablesShortcut(t *testing.T) {
	for _, tt := range scanFailureRemovals(t) {
		for _, mode := range []struct {
			name string
			fail func(st2 *failingEntriesStorage, tail uint64)
		}{
			{"fail-once", func(st2 *failingEntriesStorage, _ uint64) { st2.failNext = true }},
			{"unreadable", func(st2 *failingEntriesStorage, tail uint64) { st2.failThrough = tail }},
		} {
			t.Run(tt.name+"/"+mode.name, func(t *testing.T) {
				rs, _, tail := scanFailureGroup(t, tt.ent, mode.fail)
				r2, r3, r4 := rs[2], rs[3], rs[4]

				askForScanFailureLeases(t, rs)
				require.False(t, r3.readOnly.hasActiveHeldLease(), "3 granted before the tail applied")
				require.False(t, r2.readOnly.hasActiveHeldLease(), "2 took a self-lease before the tail applied")

				r2.applyConfChange(tt.cc)
				r2.appliedTo(tail, 0)
				applyCommitted(t, r2)
				applyCommitted(t, r3)
				if tt.joint {
					askForScanFailureLeases(t, rs)
					require.False(t, r3.readOnly.hasActiveHeldLease(), "3 granted while joint")
					leaveJoint(t, r2, pb.ConfChangeTransitionJointExplicit)
					pumpMessages(t, rs, cutScanFailure1)
					applyCommitted(t, r2)
					applyCommitted(t, r3)
				}
				require.NotContains(t, r2.trk.Progress, uint64(4), "4 should have been removed")
				askForScanFailureLeases(t, rs)
				require.True(t, r3.readOnly.hasActiveHeldLease(), "3 should be granted once settled")
				require.True(t, r2.readOnly.hasActiveHeldLease(), "2 should take a self-lease once settled")

				require.NoError(t, r2.Step(pb.Message{From: 2, To: 2, Type: pb.MsgProp,
					Entries: []pb.Entry{{Data: []byte("w")}}}))
				pumpMessages(t, rs, cutScanFailure1)
				t.Logf("write at %d; 2 committed %d; 4 committed %d",
					r2.raftLog.lastIndex(), r2.raftLog.committed, r4.raftLog.committed)
				for _, s := range stepLocalRead(t, r4, "rd") {
					require.GreaterOrEqual(t, s.Index, r2.raftLog.committed,
						"STALE READ: removed voter 4 served a read below a committed write")
				}
			})
		}
	}
}
