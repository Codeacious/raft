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
	"context"
	"math"
	"testing"
	"time"

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

	// These tests inspect the delayer's queue right after a Step, so hold reads
	// far longer than a test runs; the 5 ms default is easily exceeded under
	// -race. Clearing on cleanup stops the timer, which would otherwise block
	// forever delivering to a bare raft's missing delayedReadsChannel.
	r.readIndexDelayer.holdMicros = uint64(time.Hour / time.Microsecond)
	t.Cleanup(r.readIndexDelayer.clear)

	r.readOnly.heldLease = &ReadLease{
		LeaseId:   1,
		NodeId:    r.id,
		LogIndex:  5, // <= committed, so the catchup margin is satisfied
		StartTime: nowMicros(),
		Duration:  cfg.ReadLeaseDurationMicros,
	}
	r.readOnly.readLeaseAckedIndex = ackedIndex
	return r
}

func stepSwitchHintRead(t *testing.T, r *raft, hint uint64) {
	t.Helper()
	stepSwitchHintReadAtTerm(t, r, hint, r.Term)
}

// stepSwitchHintReadAtTerm steps a locally issued read (From unset, as
// Node.ReadIndexSwitchHint makes one): only those are served from the lease.
func stepSwitchHintReadAtTerm(t *testing.T, r *raft, hint uint64, hintTerm uint64) {
	t.Helper()
	if err := r.Step(pb.Message{
		To:      1,
		Type:    pb.MsgReadIndex,
		Commit:  hint,
		LogTerm: hintTerm,
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
			held := r.readIndexDelayer.heldCount()

			served := len(r.readStates)

			var got string
			switch {
			case len(msgs) == 0 && held == 1 && served == 0:
				got = "held"
			case len(msgs) == 0 && held == 0 && served == 1:
				got = "served"
			case len(msgs) == 1 && held == 0 && served == 0 &&
				msgs[0].Type == pb.MsgReadIndex && msgs[0].To == r.lead:
				got = "forwarded"
			default:
				t.Fatalf("unexpected outcome: %d msgs (%+v), %d held, %d served", len(msgs), msgs, held, served)
			}
			if got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

// TestSwitchHintTermGate: a hint counts only in the term whose leader wrote
// the switch register it came from (SAFE-1). A hint from any other term is
// treated as unreachable and forwarded, even one that would otherwise be
// served. The term is compared modulo 2^32, the width the switch carries.
func TestSwitchHintTermGate(t *testing.T) {
	tests := []struct {
		name     string
		hint     uint64
		hintTerm func(r *raft) uint64
		served   bool
	}{
		{"hint at this term", 5, func(r *raft) uint64 { return r.Term }, true},
		{"hint at this term mod 2^32", 5, func(r *raft) uint64 { return r.Term + 1<<32 }, true},
		{"hint from an earlier term", 5, func(r *raft) uint64 { return r.Term - 1 }, false},
		{"hint from a later term", 5, func(r *raft) uint64 { return r.Term + 1 }, false},
		{"no hint ignores the term", 0, func(r *raft) uint64 { return r.Term + 1 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newSwitchHintTestRaft(t, 5)
			stepSwitchHintReadAtTerm(t, r, tt.hint, tt.hintTerm(r))
			msgs := r.readMessages()
			if r.readIndexDelayer.heldCount() != 0 {
				t.Fatalf("read was held; got %+v", msgs)
			}
			served := len(msgs) == 0 && len(r.readStates) == 1
			forwarded := len(msgs) == 1 && len(r.readStates) == 0 &&
				msgs[0].Type == pb.MsgReadIndex && msgs[0].To == r.lead
			if tt.served && !served || !tt.served && !forwarded {
				t.Fatalf("served=%v forwarded=%v, want served=%v", served, forwarded, tt.served)
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

		if n := r.readIndexDelayer.heldCount(); n != 0 {
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
	r.readOnly.heldLease = nil
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
	if n := r.readIndexDelayer.heldCount(); n != 0 {
		t.Fatalf("unreachable hint was held by the delayer (%d queued)", n)
	}
}

// TestRemoteReadNotServedFromFollowerLease: a follower serves only its own
// reads from its lease (SAFE-5). A read another node forwarded here (that
// node's leader is stale: this node led the term it was sent in, and now
// follows a later leader) is forwarded on to this node's leader, From intact,
// so the leader answers the requester directly.
//
// The replay cases are the ones the SAFE-1 hint-term gate does not stop: a read
// the requester held and then gave up on arrives with Index set and Reject,
// and the m.Index disjunct of the serve gate would admit it without consulting
// this node's acked index or the hint. Twin of raft-rs's
// test_remote_read_is_not_served_from_follower_lease.
func TestRemoteReadNotServedFromFollowerLease(t *testing.T) {
	const requester = 3
	tests := []struct {
		name       string
		ackedIndex uint64
		commit     uint64
		logTerm    func(r *raft) uint64
		index      uint64
		reject     bool
	}{
		{"no hint", 5, 0, func(*raft) uint64 { return 0 }, 0, false},
		{"no hint, acked ahead of committed", 20, 0, func(*raft) uint64 { return 0 }, 0, false},
		{"hint at this term", 5, 5, func(r *raft) uint64 { return r.Term }, 0, false},
		{"give-up replay from an earlier term", 5, 5, func(r *raft) uint64 { return r.Term - 1 }, 5, true},
		// Served before the fix only by the m.Index disjunct.
		{"give-up replay, acked ahead of committed", 20, 0, func(*raft) uint64 { return 0 }, 5, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newSwitchHintTestRaft(t, tt.ackedIndex)
			if err := r.Step(pb.Message{
				From:    requester,
				To:      1,
				Type:    pb.MsgReadIndex,
				Commit:  tt.commit,
				LogTerm: tt.logTerm(r),
				Index:   tt.index,
				Reject:  tt.reject,
				Entries: []pb.Entry{{Data: []byte("remote")}},
			}); err != nil {
				t.Fatalf("step MsgReadIndex: %v", err)
			}
			msgs := r.readMessages()
			if n := len(r.readStates); n != 0 {
				t.Fatalf("a remote read produced %d local read states", n)
			}
			if n := r.readIndexDelayer.heldCount(); n != 0 {
				t.Fatalf("a remote read was held (%d queued)", n)
			}
			if len(msgs) != 1 || msgs[0].Type != pb.MsgReadIndex || msgs[0].To != r.lead {
				t.Fatalf("expected the read forwarded to the leader, got %+v", msgs)
			}
			if msgs[0].From != requester {
				t.Fatalf("forwarded From = %d, want %d so the leader answers the requester", msgs[0].From, requester)
			}
		})
	}
}

// TestLocalReadsServedFromFollowerLease is the other side of SAFE-5: a read
// issued through the entry points etcd uses (Node.ReadIndexSwitchHint, and
// RawNode.ReadIndex) arrives with From unset, so the lease-holding follower
// serves it from its own lease rather than forwarding it as another node's
// read.
func TestLocalReadsServedFromFollowerLease(t *testing.T) {
	t.Run("RawNode.ReadIndex", func(t *testing.T) {
		r := newSwitchHintTestRaft(t, 5)
		rn := &RawNode{raft: r}
		rn.ReadIndex([]byte("api"))
		if msgs := r.readMessages(); len(msgs) != 0 {
			t.Fatalf("a local read sent messages: %+v", msgs)
		}
		if len(r.readStates) != 1 || r.readStates[0].Index != switchHintTestCommitted {
			t.Fatalf("read states = %+v, want one at %d", r.readStates, switchHintTestCommitted)
		}
	})

	// As the in-package Network tests issue them.
	t.Run("From self", func(t *testing.T) {
		r := newSwitchHintTestRaft(t, 5)
		if err := r.Step(pb.Message{From: r.id, To: r.id, Type: pb.MsgReadIndex,
			Entries: []pb.Entry{{Data: []byte("self")}}}); err != nil {
			t.Fatalf("step MsgReadIndex: %v", err)
		}
		if msgs := r.readMessages(); len(msgs) != 0 {
			t.Fatalf("a local read sent messages: %+v", msgs)
		}
		if len(r.readStates) != 1 || r.readStates[0].Index != switchHintTestCommitted {
			t.Fatalf("read states = %+v, want one at %d", r.readStates, switchHintTestCommitted)
		}
	})

	t.Run("Node.ReadIndexSwitchHint", func(t *testing.T) {
		r := newSwitchHintTestRaft(t, 5)
		term := r.Term
		rn := &RawNode{raft: r}
		ss := r.softState()
		rn.prevSoftSt = &ss
		rn.prevHardSt = r.hardState()
		n := newNode(rn)
		go n.run()
		defer n.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := n.ReadIndexSwitchHint(ctx, []byte("api"), 5, term); err != nil {
			t.Fatalf("ReadIndexSwitchHint: %v", err)
		}
		for {
			select {
			case rd := <-n.Ready():
				for _, m := range rd.Messages {
					if m.Type == pb.MsgReadIndex {
						t.Fatalf("a local read was forwarded: %+v", m)
					}
				}
				if len(rd.ReadStates) > 0 {
					if len(rd.ReadStates) != 1 || rd.ReadStates[0].Index != switchHintTestCommitted {
						t.Fatalf("read states = %+v, want one at %d", rd.ReadStates, switchHintTestCommitted)
					}
					return
				}
				n.Advance()
			case <-ctx.Done():
				t.Fatal("no read state before the timeout")
			}
		}
	})
}
