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

// commitTraceAck delivers the leader's MsgApp to follower id and steps the
// follower's MsgAppResp at the leader.
func commitTraceAck(t *testing.T, rs map[uint64]*raft, apps map[uint64]pb.Message, id uint64) {
	t.Helper()
	m, ok := apps[id]
	require.True(t, ok, "no MsgApp to %d", id)
	require.NoError(t, rs[id].Step(m))
	n := 0
	for _, resp := range rs[id].readMessages() {
		if resp.Type == pb.MsgAppResp {
			require.NoError(t, rs[1].Step(resp))
			n++
		}
	}
	require.Equal(t, 1, n)
}

// commitTraceProposal proposes one entry at leader 1 and returns its index and
// the MsgApps carrying it, by recipient. readMessages steps the leader's
// self-ack, as etcd's Advance does after its WAL save.
func commitTraceProposal(t *testing.T, r1 *raft) (uint64, map[uint64]pb.Message) {
	t.Helper()
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w")}}}))
	idx := r1.raftLog.lastIndex()
	apps := map[uint64]pb.Message{}
	for _, m := range r1.readMessages() {
		if m.Type == pb.MsgApp && len(m.Entries) > 0 && m.Entries[len(m.Entries)-1].Index == idx {
			apps[m.To] = m
		}
	}
	require.Len(t, apps, 2)
	return idx, apps
}

// TestCommitTraceEvents: the leader reports its append, every Match rise (its
// own self-ack included) and the commit advance, in the order it steps them.
// Followers report nothing.
func TestCommitTraceEvents(t *testing.T) {
	var evs []CommitTraceEvent
	followerEvs := 0
	rs := map[uint64]*raft{}
	for id := uint64(1); id <= 3; id++ {
		cfg := newTestConfig(id, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		if id == 1 {
			cfg.CommitTrace = func(ev CommitTraceEvent) { evs = append(evs, ev) }
		} else {
			cfg.CommitTrace = func(CommitTraceEvent) { followerEvs++ }
		}
		rs[id] = newRaft(cfg)
	}
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, func(pb.Message) bool { return false })
	require.Equal(t, StateLeader, r1.state)
	require.Equal(t, r1.raftLog.lastIndex(), r1.raftLog.committed)
	require.NotEmpty(t, evs, "the election's empty entry is traced too")

	evs = nil
	idx, apps := commitTraceProposal(t, r1)
	commitTraceAck(t, rs, apps, 3)
	commitTraceAck(t, rs, apps, 2)
	require.Equal(t, []CommitTraceEvent{
		{Kind: CommitTraceAppend, Peer: 1, Index: idx},
		{Kind: CommitTraceMatch, Peer: 1, Index: idx},
		{Kind: CommitTraceMatch, Peer: 3, Index: idx},
		{Kind: CommitTraceCommit, Peer: 1, Index: idx},
		{Kind: CommitTraceMatch, Peer: 2, Index: idx},
	}, evs)

	// A duplicate ack moves nothing, so it is not traced.
	evs = nil
	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Term: r1.Term, Type: pb.MsgAppResp, Index: idx}))
	require.Empty(t, evs)
	require.Zero(t, followerEvs)
}

// TestCommitTraceGrantInstall: installing a grant sets the holder's
// AckedIndex, which the trace reports as an Acked event with SourceGrant.
func TestCommitTraceGrantInstall(t *testing.T) {
	waitLeaseAskWindow()
	var evs []CommitTraceEvent
	rs := map[uint64]*raft{}
	for id := uint64(1); id <= 3; id++ {
		cfg := incarnationTestConfig(id, newTestMemoryStorage(withPeers(1, 2, 3)), 60*1000*1000)
		if id == 1 {
			cfg.CommitTrace = func(ev CommitTraceEvent) { evs = append(evs, ev) }
		}
		rs[id] = newRaft(cfg)
	}
	none := func(pb.Message) bool { return false }
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, none)
	require.True(t, r1.committedEntryInCurrentTerm())
	before := len(evs)
	rs[2].maybeAskForReadLease()
	pumpMessages(t, rs, none)
	if r1.readOnly.getGrantedLease(2) == nil {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
		pumpMessages(t, rs, none)
	}
	g := r1.readOnly.getGrantedLease(2)
	require.NotNil(t, g)
	var grants []CommitTraceEvent
	for _, ev := range evs[before:] {
		if ev.Kind == CommitTraceAcked && ev.Source == CommitTraceSourceGrant && ev.Peer == 2 {
			grants = append(grants, ev)
		}
	}
	require.Len(t, grants, 1)
	require.LessOrEqual(t, grants[0].Index, g.AckedIndex)
}

// TestCommitTraceAckedSources: under GrantLeases each rise of a holder's
// granted AckedIndex is traced with what raised it, and a non-rise is not.
// With both followers holding, commit waits for the second ack, past quorum.
func TestCommitTraceAckedSources(t *testing.T) {
	rs, _ := incarnationTestGroup(t, false)
	r1 := rs[1]
	require.NotNil(t, r1.readOnly.getGrantedLease(2))
	require.NotNil(t, r1.readOnly.getGrantedLease(3))
	var evs []CommitTraceEvent
	r1.commitTrace = func(ev CommitTraceEvent) { evs = append(evs, ev) }

	idx, apps := commitTraceProposal(t, r1)
	commitTraceAck(t, rs, apps, 2)
	commitTraceAck(t, rs, apps, 3)
	require.Equal(t, []CommitTraceEvent{
		{Kind: CommitTraceAppend, Peer: 1, Index: idx},
		{Kind: CommitTraceMatch, Peer: 1, Index: idx},
		{Kind: CommitTraceMatch, Peer: 2, Index: idx},
		{Kind: CommitTraceAcked, Source: CommitTraceSourceAppResp, Peer: 2, Index: idx},
		{Kind: CommitTraceMatch, Peer: 3, Index: idx},
		{Kind: CommitTraceAcked, Source: CommitTraceSourceAppResp, Peer: 3, Index: idx},
		{Kind: CommitTraceCommit, Peer: 1, Index: idx},
	}, evs)

	evs = nil
	inc := rs[2].readOnly.incarnationID()
	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Term: r1.Term, Type: pb.MsgAskAckIndexResp,
		Index: idx + 1, LogTerm: inc}))
	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Term: r1.Term, Type: pb.MsgAskAckIndexResp,
		Index: idx, LogTerm: inc}))
	require.NoError(t, r1.Step(pb.Message{From: 2, To: 1, Type: pb.MsgAskAckIndexResp,
		Index: idx + 2, LogTerm: r1.Term}))
	require.Equal(t, []CommitTraceEvent{
		{Kind: CommitTraceAcked, Source: CommitTraceSourceAckIndexResp, Peer: 2, Index: idx + 1},
		{Kind: CommitTraceAcked, Source: CommitTraceSourceSwitchCredit, Peer: 2, Index: idx + 2},
	}, evs)
}
