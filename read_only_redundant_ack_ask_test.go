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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Redundant ack asks: while the commit clamp binds, the leader asks lagging
// holders for their ack index (MsgAskAckIndex). When an append covering the
// cutoff is already in flight to a holder and its grant has caught up with
// every ack it has sent, the holder's answer can't arrive before that append's
// response, which raises the grant at least as far. The leader marks such an
// ask (Reject, read with IsRedundantAckAsk) so the application can drop it;
// every other ask goes out unmarked, as before. Same tests as raft-rs
// test_redundant_ack_ask.rs.

const ackAskInterval = _ACK_INDEX_ASK_INTERVAL_MICROS * time.Microsecond

// ackAskTestConfig: every node asks for leases (only followers do), with leases
// long enough that every scenario finishes inside one.
func ackAskTestConfig(id uint64, st *MemoryStorage) *Config {
	cfg := newTestConfig(id, 10, 1, st)
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = ReadOnlyGrantLeases
	cfg.ReadLeaseDurationMicros = 60 * 1000 * 1000
	cfg.MaxNumReadLeases = 30
	cfg.AskForReadLease = true
	cfg.ReadLeaseCatchupMargin = 10
	return cfg
}

// ackAskGroup starts an n-node group led by 1 in which every follower holds a
// grant.
func ackAskGroup(t *testing.T, n uint64) (map[uint64]*raft, map[uint64]*MemoryStorage) {
	t.Helper()
	waitLeaseAskWindow()
	var ids []uint64
	for id := uint64(1); id <= n; id++ {
		ids = append(ids, id)
	}
	rs := map[uint64]*raft{}
	sts := map[uint64]*MemoryStorage{}
	for _, id := range ids {
		sts[id] = newTestMemoryStorage(withPeers(ids...))
		rs[id] = newRaft(ackAskTestConfig(id, sts[id]))
	}
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, noDropAck)
	require.Equal(t, StateLeader, r1.state)
	for id := uint64(2); id <= n; id++ {
		rs[id].maybeAskForReadLease()
	}
	pumpMessages(t, rs, noDropAck)
	for i := 0; i < 3 && !allHoldGrants(r1, n); i++ {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
		pumpMessages(t, rs, noDropAck)
	}
	for id := uint64(2); id <= n; id++ {
		require.True(t, r1.readOnly.hasActiveGrantedLease(id), "setup: %d holds a grant", id)
	}
	return rs, sts
}

func allHoldGrants(r1 *raft, n uint64) bool {
	for id := uint64(2); id <= n; id++ {
		if !r1.readOnly.hasActiveGrantedLease(id) {
			return false
		}
	}
	return true
}

func noDropAck(pb.Message) bool { return false }

// ackHop delivers one hop: each message not dropped is stepped at its target;
// returns what the targets sent (readMessages also flushes what waits on
// append, as persisting would).
func ackHop(rs map[uint64]*raft, msgs []pb.Message, drop func(pb.Message) bool) []pb.Message {
	var out []pb.Message
	for _, m := range msgs {
		if drop(m) {
			continue
		}
		r := rs[m.To]
		_ = r.Step(m)
		out = append(out, r.readMessages()...)
	}
	return out
}

type seenAsk struct {
	to     uint64
	marked bool
}

// seenAsks returns the ack asks in msgs, sorted by holder.
func seenAsks(msgs []pb.Message) []seenAsk {
	var a []seenAsk
	for _, m := range msgs {
		if m.Type == pb.MsgAskAckIndex {
			a = append(a, seenAsk{m.To, IsRedundantAckAsk(m)})
		}
	}
	sort.Slice(a, func(i, j int) bool { return a[i].to < a[j].to })
	return a
}

func proposeAck(t *testing.T, rs map[uint64]*raft) []pb.Message {
	t.Helper()
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w")}}}))
	return r1.readMessages()
}

// followerAcks returns the followers' acks to appends, by follower; drop
// selects lost appends.
func followerAcks(rs map[uint64]*raft, appends []pb.Message, drop func(pb.Message) bool) map[uint64]pb.Message {
	acks := map[uint64]pb.Message{}
	for _, m := range ackHop(rs, appends, drop) {
		if m.Type == pb.MsgAppResp {
			acks[m.From] = m
		}
	}
	return acks
}

// TestRedundantAckAskMark: the mark is Reject on MsgAskAckIndex only.
func TestRedundantAckAskMark(t *testing.T) {
	m := pb.Message{Type: pb.MsgAskAckIndex}
	require.False(t, IsRedundantAckAsk(m))
	m.Reject = true
	require.True(t, IsRedundantAckAsk(m))
	m.Type = pb.MsgAppResp
	require.False(t, IsRedundantAckAsk(m), "Reject on another type keeps its own meaning")
}

// TestAckAsksMarkedWhileAppendInFlight: every follower holds a grant and every
// append is in flight. The holders still behind when the quorum forms are
// asked, every ask is marked, and the write commits on the last holder's own
// ack without any answer.
func TestAckAsksMarkedWhileAppendInFlight(t *testing.T) {
	for _, n := range []uint64{3, 5, 7} {
		rs, _ := ackAskGroup(t, n)
		appends := proposeAck(t, rs)
		w := rs[1].raftLog.lastIndex()
		acks := followerAcks(rs, appends, noDropAck)
		var seen []seenAsk
		for id := uint64(2); id <= n; id++ {
			seen = append(seen, seenAsks(ackHop(rs, []pb.Message{acks[id]}, noDropAck))...)
		}
		q := n/2 + 1 // quorum size, leader included
		var behind []seenAsk
		for id := q + 1; id <= n; id++ {
			behind = append(behind, seenAsk{id, true})
		}
		require.Equal(t, behind, seen, "n=%d: the holders behind at the quorum-th ack, marked", n)
		require.Equal(t, w, rs[1].raftLog.committed, "n=%d: the write commits on the acks alone", n)
	}
}

// TestAckAskToProbeHolderUnmarked: a holder in Probe gets an unmarked ask; no
// covering append is in flight.
func TestAckAskToProbeHolderUnmarked(t *testing.T) {
	rs, _ := ackAskGroup(t, 5)
	require.NoError(t, rs[1].Step(pb.Message{From: 5, To: 1, Type: pb.MsgUnreachable}))
	rs[1].readMessages()
	appends := proposeAck(t, rs)
	acks := followerAcks(rs, appends, func(m pb.Message) bool { return m.To == 5 })
	var seen []seenAsk
	for _, id := range []uint64{2, 3} {
		seen = append(seen, seenAsks(ackHop(rs, []pb.Message{acks[id]}, noDropAck))...)
	}
	require.Equal(t, []seenAsk{{4, true}, {5, false}}, seen)
}

// TestAckAskUnmarkedWhenGrantBelowMatch: a grant below the holder's Match (here
// a new incarnation's, set from its commit index) gets an unmarked ask even with
// a covering append in flight: that append's response raises the grant only to
// its own index.
func TestAckAskUnmarkedWhenGrantBelowMatch(t *testing.T) {
	rs, sts := ackAskGroup(t, 5)
	r1 := rs[1]
	to4 := func(m pb.Message) bool { return m.To == 4 }

	// w1 reaches every follower but 4, so the clamp holds it.
	appends := proposeAck(t, rs)
	w1 := r1.raftLog.lastIndex()
	acks1 := followerAcks(rs, appends, to4)
	for _, id := range []uint64{2, 3, 5} {
		ackHop(rs, []pb.Message{acks1[id]}, noDropAck)
	}
	require.Less(t, r1.raftLog.committed, w1, "4's lost append holds w1")

	// 5 restarts in the same term with its log; its promise is lost.
	require.NoError(t, sts[5].Append(rs[5].raftLog.nextUnstableEnts()))
	require.NoError(t, sts[5].SetHardState(rs[5].hardState()))
	rs[5] = newRaft(ackAskTestConfig(5, sts[5]))
	require.Equal(t, r1.Term, rs[5].Term)
	// Heartbeat rounds re-grant it from its commit index; 4 hears nothing.
	cut4 := func(m pb.Message) bool { return m.To == 4 || m.From == 4 }
	for i := 0; i < 3 && !rs[5].readOnly.hasActiveHeldLease(); i++ {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
		pumpMessages(t, rs, cut4)
	}
	require.True(t, rs[5].readOnly.hasActiveHeldLease())
	g5, ok := r1.readOnly.leader.grantedLeases.Get(5)
	require.True(t, ok)
	require.Equal(t, w1, r1.trk.Progress[5].Match)
	require.Less(t, g5.AckedIndex, r1.trk.Progress[5].Match, "setup: 5's new grant is below its Match")

	// w2: 4's append is lost again. Past the ask interval, so the asks at the
	// w2 cutoff aren't held back by those at the w1 one.
	appends = proposeAck(t, rs)
	acks := followerAcks(rs, appends, to4)
	ackHop(rs, []pb.Message{acks[2]}, noDropAck) // cutoff w1
	time.Sleep(ackAskInterval + time.Millisecond)
	atW2 := seenAsks(ackHop(rs, []pb.Message{acks[3]}, noDropAck))
	require.Equal(t, []seenAsk{{4, true}, {5, false}}, atW2,
		"4's grant has caught up, so its ask is marked; 5's grant is below its Match")
}

// TestUnmarkedAskNotHeldBackByMarkedOne: a dropped marked ask doesn't hold back
// the holder's next unmarked one: after 4 drops to Probe, its ask goes out at
// once, inside the ask interval.
func TestUnmarkedAskNotHeldBackByMarkedOne(t *testing.T) {
	rs, _ := ackAskGroup(t, 5)
	appends := proposeAck(t, rs)
	acks := followerAcks(rs, appends, noDropAck)
	t0 := time.Now()
	var first []seenAsk
	for _, id := range []uint64{2, 3} {
		first = append(first, seenAsks(ackHop(rs, []pb.Message{acks[id]}, noDropAck))...)
	}
	require.Equal(t, []seenAsk{{4, true}, {5, true}}, first)

	require.NoError(t, rs[1].Step(pb.Message{From: 4, To: 1, Type: pb.MsgUnreachable}))
	rs[1].readMessages()
	// 5's ack re-runs the clamp, which 4 still holds.
	out := ackHop(rs, []pb.Message{acks[5]}, noDropAck)
	if time.Since(t0) >= ackAskInterval {
		t.Skip("inconclusive: the run took longer than the ask interval")
	}
	require.Equal(t, []seenAsk{{4, false}}, seenAsks(out))
}

// TestLostAckRecoversByHeartbeat: a lost ack with every marked ask dropped. The
// next heartbeat round makes the leader resend from the holder's Next, and that
// append's response lifts the clamp. No ask is delivered at all.
func TestLostAckRecoversByHeartbeat(t *testing.T) {
	rs, _ := ackAskGroup(t, 5)
	r1 := rs[1]
	appends := proposeAck(t, rs)
	w := r1.raftLog.lastIndex()
	acks := followerAcks(rs, appends, noDropAck)
	var sent []seenAsk
	for _, id := range []uint64{2, 3, 4} { // 5's ack is lost
		sent = append(sent, seenAsks(ackHop(rs, []pb.Message{acks[id]}, noDropAck))...)
	}
	require.Contains(t, sent, seenAsk{5, true}, "5 was asked, marked")
	for _, a := range sent {
		require.True(t, a.marked)
	}
	require.Less(t, r1.raftLog.committed, w, "5's lost ack holds the clamp")

	isAsk := func(m pb.Message) bool { return m.Type == pb.MsgAskAckIndex }
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, isAsk)
	require.Equal(t, w, r1.raftLog.committed, "one heartbeat round lifts the clamp")
}

// TestHolderAnswersMarkedAsk: a marked ask that reaches a holder anyway is
// answered as usual.
func TestHolderAnswersMarkedAsk(t *testing.T) {
	rs, _ := ackAskGroup(t, 3)
	ask := pb.Message{From: 1, To: 2, Term: rs[1].Term, Type: pb.MsgAskAckIndex, Reject: true}
	var resp *pb.Message
	for _, m := range ackHop(rs, []pb.Message{ask}, noDropAck) {
		if m.Type == pb.MsgAskAckIndexResp && m.To == 1 {
			m := m
			resp = &m
		}
	}
	require.NotNil(t, resp, "the holder answers")
	require.Equal(t, rs[2].raftLog.lastIndex(), resp.Index)
}
