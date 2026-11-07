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

// CommitTraceKind names a leader-side replication event reported to
// Config.CommitTrace.
type CommitTraceKind uint8

const (
	// CommitTraceAppend: the leader appended entries up to Index. Peer is the
	// leader.
	CommitTraceAppend CommitTraceKind = 1 + iota
	// CommitTraceMatch: Peer's Match rose to Index. Peer may be the leader
	// itself: its self-ack, stepped once its own append is durable.
	CommitTraceMatch
	// CommitTraceAcked: lease holder Peer's granted AckedIndex rose to Index
	// (GrantLeases only), or was set to it by a grant (CommitTraceSourceGrant);
	// Source says which. Commit cannot pass the lowest unexpired AckedIndex
	// (maybeCommit). Expiries are not traced.
	CommitTraceAcked
	// CommitTraceCommit: the commit index advanced to Index. Peer is the leader.
	CommitTraceCommit
)

// CommitTraceSource says what raised an AckedIndex. It is
// CommitTraceSourceNone for every kind but CommitTraceAcked.
type CommitTraceSource uint8

const (
	CommitTraceSourceNone CommitTraceSource = iota
	// CommitTraceSourceAppResp: the holder's MsgAppResp.
	CommitTraceSourceAppResp
	// CommitTraceSourceAckIndexResp: the holder's own MsgAskAckIndexResp.
	CommitTraceSourceAckIndexResp
	// CommitTraceSourceSwitchCredit: a Term-0 MsgAskAckIndexResp, a switch
	// credit injected by the application.
	CommitTraceSourceSwitchCredit
	// CommitTraceSourceGrant: a grant or renewal installed with this
	// AckedIndex (grantAckedIndex). A new incarnation's grant can set it lower.
	CommitTraceSourceGrant
)

// CommitTraceEvent is one event reported to Config.CommitTrace. Within one
// leader term, Index only rises per (Kind, Peer), so a rise from a to b covers
// every index in (a, b]. The one exception is a CommitTraceSourceGrant event,
// which sets the holder's AckedIndex.
type CommitTraceEvent struct {
	Kind   CommitTraceKind
	Source CommitTraceSource
	Peer   uint64
	Index  uint64
}

func (r *raft) traceCommitEvent(kind CommitTraceKind, src CommitTraceSource, peer, index uint64) {
	if r.commitTrace != nil {
		r.commitTrace(CommitTraceEvent{Kind: kind, Source: src, Peer: peer, Index: index})
	}
}

// traceCommitAdvance reports a commit advance past prev. maybeCommit defers it
// with the commit index it started from, which covers each of its returns.
func (r *raft) traceCommitAdvance(prev uint64) {
	if c := r.raftLog.committed; c > prev {
		r.traceCommitEvent(CommitTraceCommit, CommitTraceSourceNone, r.id, c)
	}
}

// raiseGrantedAckedIndex is readOnly.updateGrantedAckedIndex, plus a
// CommitTraceAcked event when holder id's AckedIndex actually rises.
func (r *raft) raiseGrantedAckedIndex(id, index uint64, src CommitTraceSource) {
	if r.commitTrace == nil {
		r.readOnly.updateGrantedAckedIndex(id, index)
		return
	}
	rl := r.readOnly.getGrantedLease(id)
	if rl == nil {
		// updateGrantedAckedIndex would do nothing either: not GrantLeases, or
		// id holds no grant.
		return
	}
	before := rl.AckedIndex
	r.readOnly.updateGrantedAckedIndex(id, index)
	if rl.AckedIndex > before {
		r.commitTrace(CommitTraceEvent{Kind: CommitTraceAcked, Source: src, Peer: id, Index: rl.AckedIndex})
	}
}
