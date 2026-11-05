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

// SAFE-8: an etcd leader calls Advance before its apply goroutine has called
// ApplyConfChange (etcd raftNode.start: only a follower waits for a conf change
// to apply), so raft's applied passes a conf change while trk.Config does not
// reflect it yet. becomeLeader reads both: the tail scan starts at applied+1,
// and removingVoters reads trk.Config. A leader re-elected inside that window
// sees the conf change in neither.

// applyLagLease is short enough to sleep past: it doubles as the new-leader
// safeguard window, which an unsettled leader waits out in full. It is long
// enough that leases stay live for the rest of a scenario.
const applyLagLease = 300 * time.Millisecond

// applyLagNet is a group in which node 1 runs as a RawNode whose application
// applies conf changes only when told to, as an etcd leader's apply goroutine
// does, while raft advances past them. The other nodes apply committed entries
// at once, as an etcd follower waits for a conf change to apply before it
// advances.
type applyLagNet struct {
	t        *testing.T
	rn       *RawNode
	st       *MemoryStorage
	peers    map[uint64]*raft
	deferred []pb.ConfChangeV2
}

func newApplyLagNet(t *testing.T, ids ...uint64) *applyLagNet {
	t.Helper()
	waitLeaseAskWindow()
	cfg := func(id uint64, st *MemoryStorage) *Config {
		c := safeguardTestConfig(id, ReadOnlyGrantLeases, st)
		c.ReadLeaseDurationMicros = uint64(applyLagLease / time.Microsecond)
		return c
	}
	st := newTestMemoryStorage(withPeers(1, 2, 3, 4))
	rn, err := NewRawNode(cfg(1, st))
	require.NoError(t, err)
	n := &applyLagNet{t: t, rn: rn, st: st, peers: map[uint64]*raft{}}
	for _, id := range ids {
		peers := withPeers(1, 2, 3, 4)
		if id > 4 {
			// A node being added starts with a config that includes it.
			peers = withPeers(1, 2, 3, 4, id)
		}
		n.peers[id] = newRaft(cfg(id, newTestMemoryStorage(peers)))
	}
	return n
}

// pump delivers messages until none are left. Node 1 persists and advances
// every Ready, deferring the conf changes it hands out; the others apply what
// they commit.
func (n *applyLagNet) pump() {
	n.t.Helper()
	for i := 0; i < 100; i++ {
		var msgs []pb.Message
		if n.rn.HasReady() {
			rd := n.rn.Ready()
			require.NoError(n.t, n.st.Append(rd.Entries))
			if !IsEmptyHardState(rd.HardState) {
				require.NoError(n.t, n.st.SetHardState(rd.HardState))
			}
			for _, e := range rd.CommittedEntries {
				if cc, ok := confChangeFromEntry(e); ok {
					n.deferred = append(n.deferred, cc)
				}
			}
			msgs = append(msgs, rd.Messages...)
			n.rn.Advance(rd)
		}
		for _, r := range n.peers {
			msgs = append(msgs, r.readMessages()...)
			applyCommitted(n.t, r)
		}
		if len(msgs) == 0 && !n.rn.HasReady() {
			return
		}
		for _, m := range msgs {
			if m.To == 1 {
				_ = n.rn.Step(m)
			} else if r, ok := n.peers[m.To]; ok {
				_ = r.Step(m)
			}
		}
	}
	n.t.Fatal("messages did not settle")
}

// applyDeferred has node 1's application apply the conf changes it deferred.
func (n *applyLagNet) applyDeferred() {
	for _, cc := range n.deferred {
		n.rn.ApplyConfChange(cc)
	}
	n.deferred = nil
}

// askForLease has node id ask its leader for a lease, and returns whether it
// holds one.
func (n *applyLagNet) askForLease(id uint64, leader func() *raft) bool {
	n.t.Helper()
	time.Sleep(time.Duration(_LEASE_ASK_INTERVAL_MICROS+1000) * time.Microsecond)
	n.peers[id].maybeAskForReadLease()
	n.pump()
	if !n.peers[id].readOnly.hasActiveHeldLease() {
		require.NoError(n.t, leader().Step(pb.Message{From: leader().id, To: leader().id, Type: pb.MsgBeat}))
		n.pump()
	}
	return n.peers[id].readOnly.hasActiveHeldLease()
}

// transferLeadership moves leadership from node 1 to to, then back.
func (n *applyLagNet) transferLeadership(to uint64) {
	n.t.Helper()
	n.rn.TransferLeader(to)
	n.pump()
	require.Equal(n.t, StateLeader, n.peers[to].state, "%x should lead after the first transfer", to)
}

func (n *applyLagNet) transferBack(from uint64) {
	n.t.Helper()
	require.NoError(n.t, n.peers[from].Step(pb.Message{From: 1, To: from, Type: pb.MsgTransferLeader}))
	n.pump()
	require.Equal(n.t, StateLeader, n.rn.raft.state, "1 should be re-elected by the transfer back")
	n.t.Logf("re-elected 1 takes the safeguard shortcut: %v", !n.rn.raft.readOnly.safeguard.noShortcut)
	if !n.rn.raft.committedEntryInCurrentTerm() {
		// No shortcut: wait out the window, as a real leader does, and let
		// the next tick retry the suppressed commit.
		time.Sleep(applyLagLease + 20*time.Millisecond)
		n.rn.Tick()
		n.pump()
	}
	require.True(n.t, n.rn.raft.committedEntryInCurrentTerm())
}

// TestReelectedBeforeApplyingRemoval: leader 1 commits RemoveNode(3) and
// advances, but its application has not applied it. Leadership moves to 2 and
// back. Re-elected, 1 still has 3 in its config, and the removal is below
// applied, so nothing bans 3, which is granted a lease. When 1's application
// applies the removal, switchToConfig drops 3's grant from the clamp. 3 must
// not serve a read below a write 1 commits after that.
func TestReelectedBeforeApplyingRemoval(t *testing.T) {
	n := newApplyLagNet(t, 2, 3, 4)
	r1 := n.rn.raft
	require.NoError(t, n.rn.Campaign())
	n.pump()
	require.Equal(t, StateLeader, r1.state)

	require.NoError(t, n.rn.ProposeConfChange(pb.ConfChange{Type: pb.ConfChangeRemoveNode, NodeID: 3}))
	n.pump()
	removal := r1.raftLog.lastIndex()
	require.Len(t, n.deferred, 1, "1 should have handed out the removal")
	require.GreaterOrEqual(t, r1.raftLog.applied, removal, "raft's applied passes the removal")
	require.Contains(t, r1.trk.Progress, uint64(3), "1's config does not reflect it yet")

	n.transferLeadership(2)
	n.transferBack(2)
	require.Contains(t, r1.trk.Progress, uint64(3), "1 was re-elected before applying the removal")
	t.Logf("1 re-elected at term %d; applied %d, removal at %d", r1.Term, r1.raftLog.applied, removal)

	leased := n.askForLease(3, func() *raft { return r1 })
	t.Logf("removed node 3 granted a lease by re-elected 1: %v", leased)

	n.applyDeferred()
	require.NotContains(t, r1.trk.Progress, uint64(3))
	require.NoError(t, n.rn.Propose([]byte("w")))
	n.pump()
	t.Logf("write at %d; 1 committed %d; 3 committed %d",
		r1.raftLog.lastIndex(), r1.raftLog.committed, n.peers[3].raftLog.committed)
	for _, s := range stepLocalRead(t, n.peers[3], "rd") {
		require.GreaterOrEqual(t, s.Index, r1.raftLog.committed,
			"STALE READ: removed node 3 served a read below a committed write")
	}
}

// TestReelectedBeforeApplyingAddition: leader 1 commits AddNode(5) and
// advances, but its application has not applied it. Leadership moves to 2,
// which has applied it and grants 5 a lease, and back to 1. Re-elected, 1 has
// no 5 in its config, and the addition is below applied, so its safeguard
// shortcut does not wait for 5. 5 must not serve a read below a write 1
// commits while 5 still holds 2's lease.
func TestReelectedBeforeApplyingAddition(t *testing.T) {
	n := newApplyLagNet(t, 2, 3, 4, 5)
	r1 := n.rn.raft
	require.NoError(t, n.rn.Campaign())
	n.pump()
	require.Equal(t, StateLeader, r1.state)

	require.NoError(t, n.rn.ProposeConfChange(pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: 5}))
	n.pump()
	addition := r1.raftLog.lastIndex()
	require.Len(t, n.deferred, 1, "1 should have handed out the addition")
	require.GreaterOrEqual(t, r1.raftLog.applied, addition, "raft's applied passes the addition")
	require.NotContains(t, r1.trk.Progress, uint64(5), "1's config does not reflect it yet")

	n.transferLeadership(2)
	require.Contains(t, n.peers[2].trk.Progress, uint64(5), "2 has applied the addition")
	leased := n.askForLease(5, func() *raft { return n.peers[2] })
	require.True(t, leased, "5 should be granted a lease by 2")

	n.transferBack(2)
	require.NotContains(t, r1.trk.Progress, uint64(5), "1 was re-elected before applying the addition")
	t.Logf("1 re-elected at term %d; applied %d, addition at %d", r1.Term, r1.raftLog.applied, addition)

	require.NoError(t, n.rn.Propose([]byte("w")))
	n.pump()
	t.Logf("write at %d; 1 committed %d; 5 committed %d; 5 still holds 2's lease: %v",
		r1.raftLog.lastIndex(), r1.raftLog.committed, n.peers[5].raftLog.committed,
		n.peers[5].readOnly.hasActiveHeldLease())
	for _, s := range stepLocalRead(t, n.peers[5], "rd") {
		require.GreaterOrEqual(t, s.Index, r1.raftLog.committed,
			"STALE READ: added node 5 served a read below a write the re-elected leader committed")
	}
}
