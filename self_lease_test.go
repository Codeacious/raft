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

// leasedLeader returns the leader of a 3-node group in mode opt, with the
// new-leader safeguard lifted and an entry committed in its term, so it can
// serve reads and grant leases.
func leasedLeader(t *testing.T, opt ReadOnlyOption) *raft {
	t.Helper()
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = opt
	cfg.ReadLeaseDurationMicros = 500000
	cfg.MaxNumReadLeases = 5
	cfg.AskForReadLease = true
	cfg.ReadLeaseCatchupMargin = 10
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	// A synthetic leader never hears from its peers, so the safeguard would
	// otherwise block every commit, hold every read and refuse every grant.
	r.readOnly.safeguard.markPassed()
	// Reads and lease grants both need an entry committed in this term.
	r.trk.Progress[2].MaybeUpdate(r.raftLog.lastIndex())
	r.trk.Progress[3].MaybeUpdate(r.raftLog.lastIndex())
	r.maybeCommit()
	require.True(t, r.committedEntryInCurrentTerm())
	return r
}

// latestLeaseRound drains the leader's outbox and returns the highest lease round
// carried by a heartbeat in it, or 0 if no heartbeat went out.
func latestLeaseRound(r *raft) uint64 {
	var round uint64
	for _, m := range r.readMessages() {
		if m.Type == pb.MsgHeartbeat {
			round = max(round, m.Index)
		}
	}
	return round
}

// echoLeaseRound steps a MsgHeartbeatResp returning round from each node in
// froms.
func echoLeaseRound(t *testing.T, r *raft, round uint64, froms ...uint64) {
	t.Helper()
	for _, id := range froms {
		require.NoError(t, r.Step(pb.Message{
			From: id, To: 1, Term: r.Term, Type: pb.MsgHeartbeatResp, Index: round,
		}))
	}
}

// completeLeaseRound has both followers return the latest round the leader
// sent. Returns false if no heartbeat was sent.
func completeLeaseRound(t *testing.T, r *raft) bool {
	t.Helper()
	round := latestLeaseRound(r)
	if round == 0 {
		return false
	}
	echoLeaseRound(t, r, round, 2, 3)
	return true
}

// proposeSelfLease requests a renewal and sends a heartbeat for it (a leader
// with no lease counts as urgent).
func proposeSelfLease(r *raft) {
	r.maybeRenewSelfLease()
	r.maybeSendLeaseRound()
}

// setHeldLeaseRemaining gives the leader a self-lease with the given time left.
func setHeldLeaseRemaining(r *raft, remaining uint64) {
	d := r.readOnly.readLeaseDuration
	r.readOnly.heldLease = &ReadLease{NodeId: r.id, StartTime: nowMicros() + remaining - d, Duration: d}
}

// setGrantedLeaseRemaining records a lease granted to id with the given time left.
func setGrantedLeaseRemaining(r *raft, id uint64, remaining uint64) {
	d := r.readOnly.readLeaseDuration
	r.readOnly.leader.grantedLeases.Put(id, &ReadLease{NodeId: id, StartTime: nowMicros() + remaining - d, Duration: d})
}

// askLease steps a lease request with id leaseID from node from.
func askLease(t *testing.T, r *raft, from, leaseID uint64) {
	t.Helper()
	require.NoError(t, r.Step(pb.Message{
		From: from, To: 1, Term: r.Term, Type: pb.MsgAskReadLease,
		Index: r.raftLog.committed, LogTerm: leaseID,
	}))
}

// drainLeaseTraffic drains the leader's outbox and returns the lease responses
// in it and the highest lease round sent.
func drainLeaseTraffic(r *raft) (resps []pb.Message, round uint64) {
	for _, m := range r.readMessages() {
		switch m.Type {
		case pb.MsgAskReadLeaseResp:
			resps = append(resps, m)
		case pb.MsgHeartbeat:
			round = max(round, m.Index)
		}
	}
	return
}

// TestSelfLeaseRequiresFreshQuorum checks that a leader's lease renewal takes
// effect only when a majority returns a round sent after the renewal was
// requested, not an earlier one.
func TestSelfLeaseRequiresFreshQuorum(t *testing.T) {
	for _, opt := range []ReadOnlyOption{ReadOnlyLeaseBased, ReadOnlyGrantLeases} {
		r := leasedLeader(t, opt)

		// A round sent before the renewal was requested does not count, even if
		// it is returned afterwards.
		r.bcastHeartbeat()
		stale := latestLeaseRound(r)
		require.NotZero(t, stale)
		r.maybeRenewSelfLease()
		require.NotZero(t, r.readOnly.leader.pendingSelfStart, "renewal should be armed")
		echoLeaseRound(t, r, stale, 2, 3)
		require.False(t, r.readOnly.hasActiveHeldLease(),
			"a round minted before the renewal must not install it")

		r.maybeSendLeaseRound()
		require.True(t, completeLeaseRound(t, r), "expected an expedited lease round")
		require.True(t, r.readOnly.hasActiveHeldLease(),
			"the lease should install once a quorum echoes a later round")
	}
}

// TestLeaderReadWithoutLeaseFallsBackToQuorum checks that a leader with no live
// lease does not answer a read from its commit index (etcd-io/raft#166), and
// instead confirms leadership with a read-index heartbeat, which also renews
// its lease.
func TestLeaderReadWithoutLeaseFallsBackToQuorum(t *testing.T) {
	r := leasedLeader(t, ReadOnlyLeaseBased)
	r.readStates = nil
	r.readMessages()

	require.NoError(t, r.Step(pb.Message{
		From: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: []byte("ctx")}},
	}))
	require.Empty(t, r.readStates,
		"leader without a self-lease must not answer a read from its own commit index")

	var reads int
	var round uint64
	for _, m := range r.readMessages() {
		if m.Type == pb.MsgHeartbeat && len(m.Context) > 0 {
			reads++
			round = m.Index
		}
	}
	require.Equal(t, 2, reads, "expected exactly one read-index heartbeat round instead")

	// That heartbeat was sent after the read requested a renewal, so returning
	// its round installs the lease with no extra broadcast.
	require.NotZero(t, round)
	for _, id := range []uint64{2, 3} {
		require.NoError(t, r.Step(pb.Message{
			From: id, To: 1, Term: r.Term, Type: pb.MsgHeartbeatResp,
			Index: round, Context: []byte("ctx"),
		}))
	}
	require.True(t, r.readOnly.hasActiveHeldLease())
	require.Len(t, r.readStates, 1, "the read itself completes on the same round")

	// A leader that already holds a lease answers the same read locally.
	r2 := leasedLeader(t, ReadOnlyLeaseBased)
	proposeSelfLease(r2)
	require.True(t, completeLeaseRound(t, r2))
	r2.readStates = nil
	require.NoError(t, r2.Step(pb.Message{
		From: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: []byte("ctx")}},
	}))
	require.Len(t, r2.readStates, 1, "leader holding a self-lease should serve locally")
}

// TestFollowerLeaseGrantWaitsForFreshQuorum checks that a follower's lease
// request is held, not refused, until a majority returns a round sent after it
// arrived, and is then granted.
func TestFollowerLeaseGrantWaitsForFreshQuorum(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	r.readMessages()

	// No answer yet. Node 2 holds no lease, so the heartbeat goes out at once
	// rather than on the next scheduled one.
	askLease(t, r, 2, 9)
	resps, round := drainLeaseTraffic(r)
	require.Empty(t, resps, "an unproven request must be parked, not refused")
	require.NotZero(t, round, "an urgent request must kick off a quorum round")
	require.Contains(t, r.readOnly.leader.pendingGrants, uint64(2))

	// A majority returns the round, and the lease is granted.
	echoLeaseRound(t, r, round, 2, 3)
	resps, _ = drainLeaseTraffic(r)
	require.Len(t, resps, 1, "parked request should be granted once a quorum catches up")
	require.False(t, resps[0].Reject)
	require.Equal(t, uint64(9), resps[0].LogTerm)
	require.NotContains(t, r.readOnly.leader.pendingGrants, uint64(2))
}

// TestFollowerLeaseGrantRefusedOnStaticRequirement checks that a request
// failing one of requirements 1-6 (here, a banned follower) is refused
// immediately rather than held.
func TestFollowerLeaseGrantRefusedOnStaticRequirement(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	r.readOnly.leader.banGrant(2)
	r.readMessages()
	require.NoError(t, r.Step(pb.Message{
		From: 2, To: 1, Term: r.Term, Type: pb.MsgAskReadLease,
		Index: r.raftLog.committed, LogTerm: 11,
	}))
	var resp *pb.Message
	for _, m := range r.readMessages() {
		if m.Type == pb.MsgAskReadLeaseResp {
			resp = &m
		}
	}
	require.NotNil(t, resp, "a banned asker must be refused, not parked")
	require.True(t, resp.Reject)
	require.Equal(t, uint64(11), resp.LogTerm)
	require.NotContains(t, r.readOnly.leader.pendingGrants, uint64(2))
}

// TestBanDropsParkedGrant checks that banning a follower discards its held
// request, so it is never granted.
func TestBanDropsParkedGrant(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	r.readMessages()
	require.NoError(t, r.Step(pb.Message{
		From: 2, To: 1, Term: r.Term, Type: pb.MsgAskReadLease,
		Index: r.raftLog.committed, LogTerm: 13,
	}))
	require.Contains(t, r.readOnly.leader.pendingGrants, uint64(2))

	r.readOnly.leader.banGrant(2)
	require.NotContains(t, r.readOnly.leader.pendingGrants, uint64(2))

	// Nothing is granted even once a majority returns the round.
	require.True(t, completeLeaseRound(t, r))
	for _, m := range r.readMessages() {
		require.NotEqual(t, pb.MsgAskReadLeaseResp, m.Type,
			"a banned asker's parked request must be dropped silently")
	}
}

// TestRejectedAskClearsPendingLease checks that a follower forgets a lease id
// once the leader refuses it.
func TestRejectedAskClearsPendingLease(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = ReadOnlyGrantLeases
	cfg.ReadLeaseDurationMicros = 500000
	cfg.MaxNumReadLeases = 5
	cfg.AskForReadLease = true
	cfg.ReadLeaseCatchupMargin = 10
	r := newRaft(cfg)
	r.becomeFollower(1, 2)

	leaseID := r.readOnly.getMarkedLeaseId()
	require.Contains(t, r.readOnly.pendingLeases, leaseID)

	require.NoError(t, r.Step(pb.Message{
		From: 2, To: 1, Term: r.Term, Type: pb.MsgAskReadLeaseResp,
		Reject: true, LogTerm: leaseID, Entries: []pb.Entry{{}},
	}))
	require.NotContains(t, r.readOnly.pendingLeases, leaseID,
		"a refused ask must drop its pending entry")
}

// TestLeaseRoundDoesNotCoverLaterProposals checks that a request made after a
// round was sent is not granted on replies to that round, and gets a round of
// its own.
func TestLeaseRoundDoesNotCoverLaterProposals(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	r.readMessages()

	// Node 2 asks; a heartbeat goes out for it.
	askLease(t, r, 2, 21)
	resps, round1 := drainLeaseTraffic(r)
	require.Empty(t, resps)
	require.NotZero(t, round1)

	// Node 3 asks after that round was sent. It cannot use that round, and must
	// not trigger a second one while the first still lacks a majority.
	askLease(t, r, 3, 31)
	resps, round2 := drainLeaseTraffic(r)
	require.Empty(t, resps)
	require.Zero(t, round2, "only one expedited round should be in flight at a time")

	// A majority returns the first round: node 2 is granted, node 3 is not, and
	// a second round goes out straight away for it.
	echoLeaseRound(t, r, round1, 2, 3)
	resps, round3 := drainLeaseTraffic(r)
	require.Len(t, resps, 1, "only the proposal that predates the round may be granted")
	require.Equal(t, uint64(2), resps[0].To)
	require.Equal(t, uint64(21), resps[0].LogTerm)
	require.False(t, resps[0].Reject)
	require.Contains(t, r.readOnly.leader.pendingGrants, uint64(3),
		"the later proposal must still be waiting")
	require.Greater(t, round3, round1, "a second round should be sent for what is left")

	// The second round was sent after node 3 asked, so it grants node 3.
	echoLeaseRound(t, r, round3, 2, 3)
	resps, _ = drainLeaseTraffic(r)
	require.Len(t, resps, 1)
	require.Equal(t, uint64(3), resps[0].To)
	require.Equal(t, uint64(31), resps[0].LogTerm)
	require.False(t, resps[0].Reject)
	require.NotContains(t, r.readOnly.leader.pendingGrants, uint64(3))
}

// TestLeaseRenewalRidesScheduledHeartbeat checks that renewals of leases with
// time to spare send nothing of their own, and are all granted from the next
// scheduled heartbeat's round.
func TestLeaseRenewalRidesScheduledHeartbeat(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	setHeldLeaseRemaining(r, 150000)
	setGrantedLeaseRemaining(r, 2, 150000)
	setGrantedLeaseRemaining(r, 3, 150000)
	r.readMessages()

	r.maybeRenewSelfLease()
	r.maybeSendLeaseRound()
	askLease(t, r, 2, 21)
	askLease(t, r, 3, 31)
	resps, round := drainLeaseTraffic(r)
	require.Empty(t, resps)
	require.Zero(t, round, "non-urgent renewals must wait for the scheduled heartbeat")
	require.NotZero(t, r.readOnly.leader.pendingSelfStart)

	require.NoError(t, r.Step(pb.Message{From: 1, Type: pb.MsgBeat}))
	_, round = drainLeaseTraffic(r)
	require.NotZero(t, round, "the scheduled heartbeat should carry a lease round")

	selfStart := r.readOnly.leader.pendingSelfStart
	echoLeaseRound(t, r, round, 2, 3)
	resps, _ = drainLeaseTraffic(r)
	require.Len(t, resps, 2, "one round should cover every proposal made before it")
	for _, m := range resps {
		require.False(t, m.Reject)
	}
	require.Equal(t, selfStart, r.readOnly.heldLease.StartTime,
		"the self-lease is dated from its proposal, not from the echo")
}

// TestLostLeaseRoundRecoveredByNextHeartbeat checks that if a round's replies
// are lost, a later heartbeat's round grants the request.
func TestLostLeaseRoundRecoveredByNextHeartbeat(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	r.readMessages()

	askLease(t, r, 2, 21)
	_, lost := drainLeaseTraffic(r)
	require.NotZero(t, lost)
	// Nothing comes back. The next scheduled heartbeat goes out, and only node 2
	// returns it (node 3 is down), which with the leader is still a majority.
	require.NoError(t, r.Step(pb.Message{From: 1, Type: pb.MsgBeat}))
	_, next := drainLeaseTraffic(r)
	require.Greater(t, next, lost)
	echoLeaseRound(t, r, next, 2)
	resps, _ := drainLeaseTraffic(r)
	require.Len(t, resps, 1)
	require.False(t, resps[0].Reject)
	require.Equal(t, uint64(21), resps[0].LogTerm)
}

// TestExpeditedLeaseRoundRetried checks that an immediate heartbeat with no
// majority reply is resent on the next trigger (here a re-ask) once
// _LEASE_ROUND_RETRY_MICROS has passed, and not before.
func TestExpeditedLeaseRoundRetried(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	r.readMessages()

	askLease(t, r, 2, 21)
	_, lost := drainLeaseTraffic(r)
	require.NotZero(t, lost)

	askLease(t, r, 2, 22)
	_, round := drainLeaseTraffic(r)
	require.Zero(t, round, "a round still inside its retry window must not be resent")

	r.readOnly.leader.leaseRoundSentAt -= _LEASE_ROUND_RETRY_MICROS
	askLease(t, r, 2, 23)
	_, round = drainLeaseTraffic(r)
	require.Greater(t, round, lost, "a presumed-lost round should be replaced")

	echoLeaseRound(t, r, round, 2, 3)
	resps, _ := drainLeaseTraffic(r)
	require.Len(t, resps, 1)
	require.Equal(t, uint64(21), resps[0].LogTerm, "the first request is the one granted")
}

// TestLeaseReAskKeepsFirstRequest checks that a follower's repeated requests do
// not replace the one already held, unless it has been held a full lease
// duration.
func TestLeaseReAskKeepsFirstRequest(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	setGrantedLeaseRemaining(r, 2, 150000)
	r.readMessages()

	askLease(t, r, 2, 21)
	first := r.readOnly.leader.pendingGrants[2]
	require.NoError(t, r.Step(pb.Message{From: 1, Type: pb.MsgBeat}))
	_, round := drainLeaseTraffic(r)
	require.NotZero(t, round)

	// Re-asks after the round went out must not replace the held request.
	askLease(t, r, 2, 22)
	askLease(t, r, 2, 23)
	require.Equal(t, first, r.readOnly.leader.pendingGrants[2])

	echoLeaseRound(t, r, round, 2, 3)
	resps, _ := drainLeaseTraffic(r)
	require.Len(t, resps, 1)
	require.False(t, resps[0].Reject)
	require.Equal(t, uint64(21), resps[0].LogTerm)

	// A request held a full lease duration is replaced by a re-ask, which waits
	// on a new round.
	askLease(t, r, 2, 24)
	pg := r.readOnly.leader.pendingGrants[2]
	pg.proposedAt -= r.readOnly.readLeaseDuration
	r.readOnly.leader.pendingGrants[2] = pg
	askLease(t, r, 2, 25)
	require.Equal(t, uint64(25), r.readOnly.leader.pendingGrants[2].leaseId)
	require.Equal(t, r.readOnly.leader.leaseRound+1, r.readOnly.leader.pendingGrants[2].needRound)
}

// TestStaleParkedGrantRefused checks that a request held for a full lease
// duration before a majority replies is refused rather than granted.
func TestStaleParkedGrantRefused(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	r.readMessages()
	askLease(t, r, 2, 21)
	_, round := drainLeaseTraffic(r)
	pg := r.readOnly.leader.pendingGrants[2]
	pg.proposedAt -= r.readOnly.readLeaseDuration
	r.readOnly.leader.pendingGrants[2] = pg

	echoLeaseRound(t, r, round, 2, 3)
	resps, _ := drainLeaseTraffic(r)
	require.Len(t, resps, 1)
	require.True(t, resps[0].Reject)
	require.Equal(t, uint64(21), resps[0].LogTerm)
	require.Nil(t, r.readOnly.getGrantedLease(2))
}

// TestLateLeaseEchoStillCounts checks that a reply to an older round still
// counts after a newer round has been sent.
func TestLateLeaseEchoStillCounts(t *testing.T) {
	r := leasedLeader(t, ReadOnlyGrantLeases)
	r.readMessages()

	askLease(t, r, 2, 21)
	_, round1 := drainLeaseTraffic(r)
	require.NoError(t, r.Step(pb.Message{From: 1, Type: pb.MsgBeat}))
	_, round2 := drainLeaseTraffic(r)
	require.Greater(t, round2, round1)

	// Only node 2's reply to the older round arrives. With the leader that is a
	// majority at round1, which was sent after the request.
	echoLeaseRound(t, r, round1, 2)
	resps, _ := drainLeaseTraffic(r)
	require.Len(t, resps, 1)
	require.False(t, resps[0].Reject)

	// A reply to a round never sent is ignored.
	require.False(t, r.readOnly.leader.recordLeaseEcho(3, r.readOnly.leader.leaseRound+1))
}

// TestNewVoterCountsOnlyOnceItEchoes checks that a newly added voter counts
// toward the majority only once it has returned a round.
func TestNewVoterCountsOnlyOnceItEchoes(t *testing.T) {
	r := leasedLeader(t, ReadOnlyLeaseBased)
	r.applyConfChange(pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: 4}.AsV2())
	r.readMessages()

	proposeSelfLease(r)
	round := latestLeaseRound(r)
	require.NotZero(t, round)

	// Leader and node 2 are two of four: not a majority.
	echoLeaseRound(t, r, round, 2)
	require.False(t, r.readOnly.hasActiveHeldLease())
	echoLeaseRound(t, r, round, 4)
	require.True(t, r.readOnly.hasActiveHeldLease())
}

// TestSingleVoterLeaseInstallsOnBroadcast checks that a single-voter leader,
// which never receives a reply, grants its own lease on the broadcast alone.
func TestSingleVoterLeaseInstallsOnBroadcast(t *testing.T) {
	cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = ReadOnlyLeaseBased
	cfg.ReadLeaseDurationMicros = 500000
	r := newRaft(cfg)
	r.becomeCandidate()
	r.becomeLeader()
	r.readOnly.safeguard.markPassed()

	proposeSelfLease(r)
	require.True(t, r.readOnly.hasActiveHeldLease())
}

// TestFollowerEchoesLeaseRound checks that a follower returns the heartbeat's
// lease round and read-index context unchanged.
func TestFollowerEchoesLeaseRound(t *testing.T) {
	cfg := newTestConfig(2, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = ReadOnlyGrantLeases
	cfg.ReadLeaseDurationMicros = 500000
	cfg.MaxNumReadLeases = 5
	cfg.ReadLeaseCatchupMargin = 10
	r := newRaft(cfg)
	r.becomeFollower(1, 1)

	require.NoError(t, r.Step(pb.Message{
		From: 1, To: 2, Term: 1, Type: pb.MsgHeartbeat, Index: 7, Context: []byte("ctx"),
	}))
	var resp *pb.Message
	for _, m := range r.readMessages() {
		if m.Type == pb.MsgHeartbeatResp {
			resp = &m
		}
	}
	require.NotNil(t, resp)
	require.Equal(t, uint64(7), resp.Index)
	require.Equal(t, []byte("ctx"), resp.Context)
}
