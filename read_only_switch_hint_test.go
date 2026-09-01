// Copyright 2016 The etcd Authors
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

	pb "go.etcd.io/raft/v3/raftpb"
)

const switchHintTestCommitted = 10

// newSwitchHintTestRaft builds a follower of leader 2 at term 1 whose log is
// fully committed at index 10, holding a live read lease granted at index 5.
func newSwitchHintTestRaft(t *testing.T, ackedIndex uint64) *raft {
	t.Helper()

	storage := newTestMemoryStorage(withPeers(1, 2, 3))
	ents := make([]pb.Entry, 0, switchHintTestCommitted)
	for i := uint64(1); i <= switchHintTestCommitted; i++ {
		ents = append(ents, pb.Entry{Term: 1, Index: i})
	}
	if err := storage.Append(ents); err != nil {
		t.Fatalf("append to storage: %v", err)
	}

	cfg := newTestConfig(1, 10, 1, storage)
	cfg.CheckQuorum = true
	cfg.ReadOnlyOption = ReadOnlyGrantLeases
	cfg.ReadLeaseDurationMicros = 500000
	cfg.MaxNumReadLeases = 5
	cfg.AskForReadLease = true
	cfg.ReadLeaseCatchupMargin = 10

	r := newRaft(cfg)
	r.becomeFollower(1, 2)
	r.raftLog.commitTo(switchHintTestCommitted)

	r.readOnly.readLeases.Put(r.id, &ReadLease{
		LeaseId:   1,
		NodeId:    r.id,
		LogIndex:  5, // <= committed, so the catchup margin is satisfied
		StartTime: nowMicros(),
		Duration:  cfg.ReadLeaseDurationMicros,
	})
	r.readOnly.readLeaseAckedIndex = ackedIndex
	return r
}

func stepSwitchHintRead(t *testing.T, r *raft, hint uint64) {
	t.Helper()
	if err := r.Step(pb.Message{
		From:    2,
		To:      1,
		Type:    pb.MsgReadIndex,
		Commit:  hint,
		Entries: []pb.Entry{{Data: []byte("ctx")}},
	}); err != nil {
		t.Fatalf("step MsgReadIndex: %v", err)
	}
}

// TestSwitchHintGate covers the serve/hold/forward decision of the follower
// read-index gate across the range of MsgReadIndex.Commit values. The contract
// is deliberately index-shaped and knows nothing about switches: Commit is an
// extra index this node must have committed before it may serve the read
// locally, and 0 is the only reserved value, meaning "no extra requirement".
//
// A caller that must not be served locally at all therefore says so in the same
// vocabulary, by naming an index this node cannot reach. etcd's assist mode
// does exactly that (math.MaxUint64) when it holds no switch index — a switch
// that reboots puts its register back to 0 while the leader's commit clamp is
// still lifted by the high value that register previously reflected, so serving
// on the follower's own acked index alone can return a read below a committed
// write.
func TestSwitchHintGate(t *testing.T) {
	const (
		served = "served"
		held   = "held"
		fwd    = "forwarded"
	)

	tests := []struct {
		name       string
		hint       uint64
		ackedIndex uint64
		want       string
	}{
		// No extra requirement. This is the ordinary case outside assist
		// (noassist/lease-based have no switch to hint with), so it must stay
		// servable; only this node's own acked index gates.
		{"no hint", 0, 5, served},
		{"no hint, acked ahead of committed", 0, 20, held},

		// A real index, as assist supplies from the switch's saved value.
		{"hint below committed", 5, 5, served},
		{"hint equal to committed", switchHintTestCommitted, 5, served},
		{"hint just ahead of committed", 20, 5, held},
		{"hint far ahead of committed", 5000, 5, fwd},

		// The unreachable index a caller uses to mean "do not serve locally".
		// It must forward in both directions of the acked-index comparison, and
		// must never be held (see TestSwitchHintUnreachableNotHeld).
		{"unreachable hint", math.MaxUint64, 5, fwd},
		{"unreachable hint, acked ahead of committed", math.MaxUint64, 20, fwd},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newSwitchHintTestRaft(t, tt.ackedIndex)
			stepSwitchHintRead(t, r, tt.hint)

			msgs := r.readMessages()
			held := len(r.readIndexDelayer.delayedReadIndexReqs)

			var got string
			switch {
			case len(msgs) == 0 && held == 1:
				got = "held"
			case len(msgs) == 1 && msgs[0].Type == pb.MsgReadIndexResp:
				got = "served"
			case len(msgs) == 1 && msgs[0].Type == pb.MsgReadIndex && msgs[0].To == r.lead:
				got = "forwarded"
			default:
				t.Fatalf("unexpected outcome: %d msgs (%+v), %d held", len(msgs), msgs, held)
			}
			if got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

// TestSwitchHintUnreachableNotHeld pins the hole that an unreachable hint
// closes, and the reason it may not be held. Holding it would be worse than
// useless: the delayer waits for this node's commit index to reach
// max(hint, ackedIndex), which no commit can satisfy, so the request would sit
// out its 5 ms hold and then be re-stepped with m.Index set — and the m.Index
// disjunct of the serve gate would serve it, having never consulted a switch.
// The gap arithmetic is what keeps it out of the delayer, so this also pins
// that max(hint, ackedIndex) - committed does not wrap.
func TestSwitchHintUnreachableNotHeld(t *testing.T) {
	for _, ackedIndex := range []uint64{5, 20} { // below and above committed
		r := newSwitchHintTestRaft(t, ackedIndex)
		stepSwitchHintRead(t, r, math.MaxUint64)

		if n := len(r.readIndexDelayer.delayedReadIndexReqs); n != 0 {
			t.Fatalf("acked %d: unreachable hint was held by the delayer (%d queued); it must be forwarded", ackedIndex, n)
		}
		if n := len(r.readStates); n != 0 {
			t.Fatalf("acked %d: unreachable hint was served locally (%d read states); it must be forwarded", ackedIndex, n)
		}

		msgs := r.readMessages()
		if len(msgs) != 1 || msgs[0].Type != pb.MsgReadIndex || msgs[0].To != r.lead {
			t.Fatalf("acked %d: expected the read forwarded to the leader, got %+v", ackedIndex, msgs)
		}
		// The hint rides along to the leader untouched. That is fine and is part
		// of why no reserved value has to be understood outside this gate: the
		// leader's MsgReadIndex handler never reads Commit.
		if msgs[0].Commit != math.MaxUint64 {
			t.Fatalf("acked %d: forwarded Commit = %d, want it preserved", ackedIndex, msgs[0].Commit)
		}
	}
}

// TestSwitchHintUnreachableWithoutLease covers the other forwarding path: with
// no live lease the gate never looks at Commit at all, so an unreachable hint
// changes nothing. (A MsgAskReadLease rides along here, since a follower with
// no lease asks for one on every read it cannot serve.)
func TestSwitchHintUnreachableWithoutLease(t *testing.T) {
	r := newSwitchHintTestRaft(t, 5)
	r.readOnly.readLeases.Delete(r.id)
	stepSwitchHintRead(t, r, math.MaxUint64)

	var forwarded int
	for _, m := range r.readMessages() {
		if m.Type == pb.MsgReadIndex && m.To == r.lead {
			forwarded++
		}
	}
	if forwarded != 1 {
		t.Fatalf("expected the read forwarded to the leader once, got %d forwards", forwarded)
	}
	if n := len(r.readIndexDelayer.delayedReadIndexReqs); n != 0 {
		t.Fatalf("unreachable hint was held by the delayer (%d queued)", n)
	}
}
