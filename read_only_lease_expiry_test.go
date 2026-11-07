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

// TestExpiredHolderWriteCommits (LIVE-4): once an unreachable holder's grant
// expires in the leader's view, the write its grant held back must commit on
// an idle group. Expiry drives no commit by itself, no append follows, and 3
// never asks for a lease (an ask would re-run the clamp at admission), so only
// tickHeartbeat can re-run it.
func TestExpiredHolderWriteCommits(t *testing.T) {
	const durMicros = 50 * 1000
	waitLeaseAskWindow()
	rs := map[uint64]*raft{}
	for id := uint64(1); id <= 3; id++ {
		cfg := incarnationTestConfig(id, newTestMemoryStorage(withPeers(1, 2, 3)), durMicros)
		cfg.AskForReadLease = id == 2
		rs[id] = newRaft(cfg)
	}
	none := func(pb.Message) bool { return false }
	r1 := rs[1]
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup}))
	pumpMessages(t, rs, none)
	require.True(t, r1.committedEntryInCurrentTerm())
	rs[2].maybeAskForReadLease()
	pumpMessages(t, rs, none)
	if !rs[2].readOnly.hasActiveHeldLease() {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
		pumpMessages(t, rs, none)
	}
	require.True(t, r1.readOnly.hasActiveGrantedLease(2))

	cut2 := func(m pb.Message) bool { return m.To == 2 || m.From == 2 }
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w")}}}))
	pumpMessages(t, rs, cut2)
	w := r1.raftLog.lastIndex()
	require.Less(t, r1.raftLog.committed, w, "the clamp holds w while 2 is unreachable")

	time.Sleep(80 * time.Millisecond) // well past the 50 ms lease
	for i := 0; i < 30; i++ {
		r1.tick()
		pumpMessages(t, rs, cut2)
	}
	require.Equal(t, StateLeader, r1.state)
	require.Equal(t, w, r1.raftLog.committed, "w commits after 2's grant expired")
}

// TestLaggingHolderNotRegrantedAfterExpiry (LIVE-2 residual): a holder whose
// appends are lost is refused renewal by check 5b; once its grant expires, its
// next ask is judged as a new ask by check 5 against committed. If committed
// is still the value the expired grant clamped it to, the holder is re-granted
// at its old index and pins commit for another lease, and on an idle group
// with reads at the holder (an ask every 10 ms) that repeats forever. The ask
// arrives here with no tick in between, so only admission can re-run the
// clamp.
func TestLaggingHolderNotRegrantedAfterExpiry(t *testing.T) {
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

	// More writes than the catch-up margin, none of them reaching 2.
	lossy := func(m pb.Message) bool {
		return m.To == 2 && (m.Type == pb.MsgApp || m.Type == pb.MsgSnap)
	}
	for i := 0; i < 20; i++ {
		require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("w")}}}))
		pumpMessages(t, rs, lossy)
	}
	require.Equal(t, stuck, r1.raftLog.committed, "2's grant clamps commit")
	require.Greater(t, r1.trk.Committed(), stuck+10, "the quorum (1, 3) moved past the margin")

	// The group goes idle and 2's grant expires on both sides. 2 asks at once.
	time.Sleep(durMicros*time.Microsecond + 20*time.Millisecond)
	require.False(t, r1.readOnly.hasActiveGrantedLease(2))
	r2.maybeAskForReadLease()
	require.NoError(t, r1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgBeat}))
	pumpMessages(t, rs, lossy)

	require.Equal(t, r1.trk.Committed(), r1.raftLog.committed, "commit caught up with the quorum")
	require.False(t, r1.readOnly.hasActiveGrantedLease(2), "2 was not re-granted at its old index")
}
