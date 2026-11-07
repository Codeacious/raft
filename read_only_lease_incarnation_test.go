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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// A holder's promise (readLeaseAckedIndex) lives in its readOnly instance and
// is lost on every reset(). These tests check that the leader only credits a
// grant with acks from the incarnation that asked for it.

func incarnationTestConfig(id uint64, st *MemoryStorage, durMicros uint64) *Config {
	cfg := newTestConfig(id, 10, 1, st)
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = ReadOnlyGrantLeases
	cfg.ReadLeaseDurationMicros = durMicros
	cfg.MaxNumReadLeases = 5
	cfg.AskForReadLease = true
	cfg.ReadLeaseCatchupMargin = 10
	return cfg
}

// waitLeaseAskWindow makes sure canAskForLease's interval, measured from
// lastLeaseAskedTime == 0 (process start), has elapsed.
func waitLeaseAskWindow() {
	for nowMicros() <= _LEASE_ASK_INTERVAL_MICROS+1000 {
		time.Sleep(time.Millisecond)
	}
}

func stepLocalRead(t *testing.T, r *raft, ctx string) []ReadState {
	t.Helper()
	r.readStates = nil
	require.NoError(t, r.Step(pb.Message{From: r.id, To: r.id, Type: pb.MsgReadIndex,
		Entries: []pb.Entry{{Data: []byte(ctx)}}}))
	return r.readStates
}

// incarnationTestGroup starts a 3-node group led by 1 in which 2 holds a lease.
func incarnationTestGroup(t *testing.T, preVote bool) (map[uint64]*raft, map[uint64]*MemoryStorage) {
	t.Helper()
	waitLeaseAskWindow()
	rs := map[uint64]*raft{}
	sts := map[uint64]*MemoryStorage{}
	for id := uint64(1); id <= 3; id++ {
		sts[id] = newTestMemoryStorage(withPeers(1, 2, 3))
		cfg := incarnationTestConfig(id, sts[id], 60*1000*1000)
		cfg.PreVote = preVote
		rs[id] = newRaft(cfg)
	}
	none := func(pb.Message) bool { return false }
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, none)
	require.Equal(t, StateLeader, r1.state)
	require.True(t, r1.committedEntryInCurrentTerm())

	rs[2].maybeAskForReadLease()
	pumpMessages(t, rs, none)
	if !rs[2].readOnly.hasActiveHeldLease() {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
		pumpMessages(t, rs, none)
	}
	require.True(t, rs[2].readOnly.hasActiveHeldLease())
	return rs, sts
}

// proposeHoldingAckFrom2 proposes an entry whose ack from 2 is held back (and
// returned), with ack-index asks to 2 lost, so the clamp holds it uncommitted.
func proposeHoldingAckFrom2(t *testing.T, rs map[uint64]*raft, data string) (uint64, pb.Message) {
	t.Helper()
	r1 := rs[1]
	before := r1.raftLog.committed
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte(data)}}}))
	idx := r1.raftLog.lastIndex()
	var held []pb.Message
	pumpMessages(t, rs, func(m pb.Message) bool {
		if m.From == 2 && m.Type == pb.MsgAppResp && m.Index == idx {
			held = append(held, m)
			return true
		}
		return m.To == 2 && m.Type == pb.MsgAskAckIndex
	})
	require.Len(t, held, 1)
	require.Equal(t, before, r1.raftLog.committed, "the clamp holds the entry back")
	return idx, held[0]
}

// requireStaleAckNotCredited delivers an ack from 2's previous incarnation after
// 2 re-acquired a lease, and checks it advances Match but not the grant.
func requireStaleAckNotCredited(t *testing.T, rs map[uint64]*raft, stale pb.Message, w0, w1 uint64) {
	t.Helper()
	r1, r2 := rs[1], rs[2]
	require.NotEqual(t, r2.readOnly.incarnationID(), stale.LogTerm, "the ack is from the previous incarnation")
	grantBefore := r1.readOnly.getGrantedLease(2).AckedIndex
	require.LessOrEqual(t, grantBefore, r2.readOnly.readLeaseAckedIndex)

	lossy := func(m pb.Message) bool {
		return m.To == 2 && (m.Type == pb.MsgApp || m.Type == pb.MsgAskAckIndex)
	}
	require.NoError(t, r1.Step(stale))
	pumpMessages(t, rs, lossy)
	require.Equal(t, w1, r1.trk.Progress[2].Match, "replication still counts the ack")
	require.Equal(t, grantBefore, r1.readOnly.getGrantedLease(2).AckedIndex, "the grant does not")
	require.Equal(t, w0, r1.raftLog.committed, "the clamp still holds W1")

	for _, s := range stepLocalRead(t, r2, "rd") {
		require.GreaterOrEqual(t, s.Index, r1.raftLog.committed, "read served below a committed write")
	}

	// The current incarnation's own acks lift the clamp.
	none := func(pb.Message) bool { return false }
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w2")}}}))
	pumpMessages(t, rs, none)
	require.Equal(t, r1.raftLog.lastIndex(), r1.raftLog.committed)
}

// TestStaleAckFromPreviousIncarnation: an ack sent before a restart, delivered
// after the restarted holder's new grant, must not lift the clamp (SAFE-2).
func TestStaleAckFromPreviousIncarnation(t *testing.T) {
	rs, sts := incarnationTestGroup(t, false)
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w0")}}}))
	pumpMessages(t, rs, func(pb.Message) bool { return false })
	w0 := r1.raftLog.lastIndex()
	require.Equal(t, w0, r1.raftLog.committed)
	w1, stale := proposeHoldingAckFrom2(t, rs, "w1")

	// 2 crashes and restarts in the same term with its log and HardState.
	require.NoError(t, sts[2].Append(rs[2].raftLog.nextUnstableEnts()))
	require.NoError(t, sts[2].SetHardState(rs[2].hardState()))
	rs[2] = newRaft(incarnationTestConfig(2, sts[2], 60*1000*1000))
	require.Equal(t, r1.Term, rs[2].Term)
	require.Zero(t, rs[2].readOnly.readLeaseAckedIndex, "promise lost on restart")

	// It re-acquires a lease while appends and ack-index asks to it are lost.
	lossy := func(m pb.Message) bool {
		return m.To == 2 && (m.Type == pb.MsgApp || m.Type == pb.MsgAskAckIndex)
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
		pumpMessages(t, rs, lossy)
	}
	require.True(t, rs[2].readOnly.hasActiveHeldLease())

	requireStaleAckNotCredited(t, rs, stale, w0, w1)
}

// TestStaleAckAfterSameTermReset: as above, but the promise is lost by a
// same-term reset (PreCandidate -> Follower on the leader's next heartbeat).
func TestStaleAckAfterSameTermReset(t *testing.T) {
	rs, _ := incarnationTestGroup(t, true)
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w0")}}}))
	pumpMessages(t, rs, func(pb.Message) bool { return false })
	w0 := r1.raftLog.lastIndex()
	w1, stale := proposeHoldingAckFrom2(t, rs, "w1")

	// 2 times out into pre-candidacy (its pre-votes are lost), then hears the leader again.
	oldIncarnation := rs[2].readOnly.incarnationID()
	rs[2].becomePreCandidate()
	rs[2].readMessages()
	lossy := func(m pb.Message) bool {
		return m.To == 2 && (m.Type == pb.MsgApp || m.Type == pb.MsgAskAckIndex)
	}
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, lossy)
	require.Equal(t, StateFollower, rs[2].state)
	require.NotEqual(t, oldIncarnation, rs[2].readOnly.incarnationID(), "reset() starts a new incarnation")
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, lossy)
	require.True(t, rs[2].readOnly.hasActiveHeldLease())

	requireStaleAckNotCredited(t, rs, stale, w0, w1)
}

// TestAckDuringRenewalCreditsNewGrant: the leader replaces a grant when it
// issues the renewal, but the holder keeps the old lease id until the renewal
// arrives. Acks sent in that gap come from the same incarnation and must still
// count, or every renewal would stall commits.
func TestAckDuringRenewalCreditsNewGrant(t *testing.T) {
	rs, _ := incarnationTestGroup(t, false)
	r1, r2 := rs[1], rs[2]
	oldId := r2.readOnly.heldLease.LeaseId

	// 2 asks for a renewal; the leader grants it but the grant is held back.
	renewId := r2.readOnly.getMarkedLeaseId()
	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Term: r1.Term, Type: pb.MsgAskReadLease,
		Index: r2.raftLog.committed, LogTerm: renewId}))
	noGrantTo2 := func(m pb.Message) bool { return m.To == 2 && m.Type == pb.MsgAskReadLeaseResp }
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, noGrantTo2)
	require.Equal(t, renewId, r1.readOnly.getGrantedLease(2).LeaseId, "the leader replaced the grant")
	require.Equal(t, oldId, r2.readOnly.heldLease.LeaseId, "the holder still holds the old lease")
	require.Equal(t, r1.trk.Progress[2].Match, r1.readOnly.getGrantedLease(2).AckedIndex,
		"a normal renewal starts where Match is")

	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w")}}}))
	pumpMessages(t, rs, noGrantTo2)
	w := r1.raftLog.lastIndex()
	require.GreaterOrEqual(t, r1.readOnly.getGrantedLease(2).AckedIndex, w)
	require.Equal(t, w, r1.raftLog.committed)
}

// TestRenewalIgnoresStaleAckMatch: a stale ack from the previous incarnation
// still advances Match. A renewal must not start from that Match: the leader's
// clamp switches to the renewal before the holder installs it, while the
// holder keeps serving under its current grant and promise.
func TestRenewalIgnoresStaleAckMatch(t *testing.T) {
	rs, sts := incarnationTestGroup(t, false)
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w0")}}}))
	pumpMessages(t, rs, func(pb.Message) bool { return false })
	w0 := r1.raftLog.lastIndex()
	w1, stale := proposeHoldingAckFrom2(t, rs, "w1")

	// 2 restarts and re-acquires a lease; then the stale ack arrives.
	require.NoError(t, sts[2].Append(rs[2].raftLog.nextUnstableEnts()))
	require.NoError(t, sts[2].SetHardState(rs[2].hardState()))
	rs[2] = newRaft(incarnationTestConfig(2, sts[2], 60*1000*1000))
	lossy := func(m pb.Message) bool {
		return m.To == 2 && (m.Type == pb.MsgApp || m.Type == pb.MsgAskAckIndex)
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
		pumpMessages(t, rs, lossy)
	}
	require.True(t, rs[2].readOnly.hasActiveHeldLease())
	require.NoError(t, r1.Step(stale))
	pumpMessages(t, rs, lossy)
	require.Equal(t, w1, r1.trk.Progress[2].Match)
	require.Equal(t, w0, r1.raftLog.committed)

	// 2 renews; the leader issues the renewal but it does not arrive.
	renewId := rs[2].readOnly.getMarkedLeaseId()
	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Term: r1.Term, Type: pb.MsgAskReadLease,
		Index: rs[2].raftLog.committed, LogTerm: renewId}))
	noGrant := func(m pb.Message) bool {
		return lossy(m) || (m.To == 2 && m.Type == pb.MsgAskReadLeaseResp)
	}
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, noGrant)
	require.Equal(t, renewId, r1.readOnly.getGrantedLease(2).LeaseId)
	require.LessOrEqual(t, r1.readOnly.getGrantedLease(2).AckedIndex, rs[2].readOnly.readLeaseAckedIndex,
		"the renewal must not exceed the promise 2 is still serving under")

	noHB := func(m pb.Message) bool { return noGrant(m) || (m.To == 2 && m.Type == pb.MsgHeartbeat) }
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w2")}}}))
	pumpMessages(t, rs, noHB)
	require.Equal(t, w0, r1.raftLog.committed, "the clamp still holds W1")
	for _, s := range stepLocalRead(t, rs[2], "rd") {
		require.GreaterOrEqual(t, s.Index, r1.raftLog.committed, "read served below a committed write")
	}
}

// TestAskAckIndexRespIncarnation: a holder's MsgAskAckIndexResp counts only
// from the incarnation that asked for the grant; a Term-0 switch credit
// injected by the application counts regardless.
func TestAskAckIndexRespIncarnation(t *testing.T) {
	rs, _ := incarnationTestGroup(t, false)
	r1 := rs[1]
	acked := func() uint64 { return r1.readOnly.getGrantedLease(2).AckedIndex }
	base := acked()
	inc := rs[2].readOnly.incarnationID()

	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Term: r1.Term, Type: pb.MsgAskAckIndexResp,
		Index: base + 1, LogTerm: inc + 1}))
	require.Equal(t, base, acked(), "another incarnation's answer is ignored")

	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Term: r1.Term, Type: pb.MsgAskAckIndexResp,
		Index: base + 1, LogTerm: inc}))
	require.Equal(t, base+1, acked(), "the asking incarnation's answer counts")

	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAskAckIndexResp,
		Index: base + 2, LogTerm: r1.Term}))
	require.Equal(t, base+2, acked(), "a Term-0 switch credit counts")
}

// TestSwitchCreditTermGate: a Term-0 switch credit counts only in the term
// the switch register answered in (LogTerm), compared modulo 2^32 as the switch
// carries it. The register's index rises only within a term, so a credit from
// another term's register says nothing about this term's stamps.
func TestSwitchCreditTermGate(t *testing.T) {
	tests := []struct {
		name    string
		logTerm func(r *raft) uint64
		counts  bool
	}{
		{"this term", func(r *raft) uint64 { return r.Term }, true},
		{"this term mod 2^32", func(r *raft) uint64 { return r.Term + 1<<32 }, true},
		{"an earlier term", func(r *raft) uint64 { return r.Term - 1 }, false},
		{"a later term", func(r *raft) uint64 { return r.Term + 1 }, false},
		{"no term", func(r *raft) uint64 { return 0 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, _ := incarnationTestGroup(t, false)
			r1 := rs[1]
			base := r1.readOnly.getGrantedLease(2).AckedIndex
			require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAskAckIndexResp,
				Index: base + 1, LogTerm: tt.logTerm(r1)}))
			want := base
			if tt.counts {
				want = base + 1
			}
			require.Equal(t, want, r1.readOnly.getGrantedLease(2).AckedIndex)
		})
	}
}

// TestLeaseIdKeepsIncarnationOnWrap: the lease-id counter wraps within the
// lower 32 bits instead of carrying into the incarnation.
func TestLeaseIdKeepsIncarnationOnWrap(t *testing.T) {
	ro := newReadOnly(ReadOnlyGrantLeases, 500000, 5, true, 10)
	inc := ro.incarnationID()
	require.NotZero(t, inc)
	ro.nextLeaseId = inc<<32 | math.MaxUint32
	a := ro.getMarkedLeaseId()
	b := ro.getMarkedLeaseId()
	require.NotEqual(t, a, b)
	require.Equal(t, inc, leaseIncarnation(a))
	require.Equal(t, inc, leaseIncarnation(b))
}
