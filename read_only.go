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
	"encoding/binary"
	"fmt"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

type ReadLease struct {
	NodeId    uint64
	LogIndex  uint64
	StartTime uint64
	Duration  uint64
}

func (rl *ReadLease) Marshal() []byte {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint64(buf[0:8], rl.NodeId)
	binary.BigEndian.PutUint64(buf[8:16], rl.LogIndex)
	binary.BigEndian.PutUint64(buf[16:24], rl.StartTime)
	binary.BigEndian.PutUint64(buf[24:32], rl.Duration)
	return buf
}

func UnmarshalReadLease(data []byte) (*ReadLease, error) {
	if len(data) != 32 {
		return nil, fmt.Errorf("invalid ReadLease data length: got %d, want 32", len(data))
	}
	return &ReadLease{
		NodeId:    binary.BigEndian.Uint64(data[0:8]),
		LogIndex:  binary.BigEndian.Uint64(data[8:16]),
		StartTime: binary.BigEndian.Uint64(data[16:24]),
		Duration:  binary.BigEndian.Uint64(data[24:32]),
	}, nil
}

type readIndexStatus struct {
	req   pb.Message
	index uint64
	// NB: this never records 'false', but it's more convenient to use this
	// instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
	// this becomes performance sensitive enough (doubtful), quorum.VoteResult
	// can change to an API that is closer to that of CommittedIndex.
	acks map[uint64]bool
}

type readOnly struct {
	option             ReadOnlyOption
	pendingReadIndex   map[string]*readIndexStatus
	readIndexQueue     []string
	readLeaseInfo      map[uint64]*ReadLease
	readLeaseDuration  uint64
	maxReadLeases      int
	shouldAskForLease  bool
	lastLeaseAskedTime uint64
	creationTime       uint64
	safeguardPassed    bool
	soonestExpiryTime  uint64
	leaseCatchupMargin int
}

const _SAFEGUARD_CLOCK_DRIFT_MICROS = 1000 // 1ms clock drift allowed
const _LEASE_ASK_INTERVAL_MICROS = 10000   // 10ms between lease asks

func newReadOnly(option ReadOnlyOption, duration uint64,
	maxleases int, askforlease bool, catchupmargin int) *readOnly {
	return &readOnly{
		option:             option,
		pendingReadIndex:   make(map[string]*readIndexStatus),
		readLeaseInfo:      make(map[uint64]*ReadLease),
		readLeaseDuration:  duration,
		maxReadLeases:      maxleases,
		shouldAskForLease:  askforlease,
		lastLeaseAskedTime: 0,
		creationTime:       uint64(time.Now().UnixMicro()),
		safeguardPassed:    false,
		soonestExpiryTime:  0,
		leaseCatchupMargin: catchupmargin,
	}
}

func (ro *readOnly) safeguardHasPassed() bool {
	if !ro.safeguardPassed {
		now := uint64(time.Now().UnixMicro())
		if now-ro.creationTime > ro.readLeaseDuration+_SAFEGUARD_CLOCK_DRIFT_MICROS {
			ro.safeguardPassed = true
		}
	}
	return ro.safeguardPassed
}

func (ro *readOnly) canAskForLease() bool {
	if !ro.shouldAskForLease {
		return false
	}

	now := uint64(time.Now().UnixMicro())
	return now-ro.lastLeaseAskedTime > _LEASE_ASK_INTERVAL_MICROS
}

func (ro *readOnly) markAskedForLease() {
	ro.lastLeaseAskedTime = uint64(time.Now().UnixMicro())
}

func (ro *readOnly) cleanupExpiredLeases() {
	currentTime := uint64(time.Now().UnixMicro())
	if currentTime < ro.soonestExpiryTime {
		return
	}
	ro.soonestExpiryTime = 0
	for id, lease := range ro.readLeaseInfo {
		if lease.StartTime+lease.Duration <= currentTime {
			delete(ro.readLeaseInfo, id)
		} else {
			ro.updateSoonestExpiryTime(lease.StartTime + lease.Duration)
		}
	}
}

func (ro *readOnly) getNumReadLeases() int {
	ro.cleanupExpiredLeases()
	return len(ro.readLeaseInfo)
}

func (ro *readOnly) getReadLease(id uint64) *ReadLease {
	lease, ok := ro.readLeaseInfo[id]
	if ok {
		return lease
	} else {
		return nil
	}
}

func (ro *readOnly) hasActiveReadLease(id uint64) bool {
	lease := ro.getReadLease(id)
	if lease == nil {
		return false
	}

	if lease.StartTime+lease.Duration > uint64(time.Now().UnixMicro()) {
		return true
	} else {
		ro.cleanupExpiredLeases()
		return false
	}
}

func (ro *readOnly) updateSoonestExpiryTime(expTime uint64) {
	if ro.soonestExpiryTime == 0 || expTime < ro.soonestExpiryTime {
		ro.soonestExpiryTime = expTime
	}
}

func (ro *readOnly) grantNewLease(id uint64, index uint64) *ReadLease {
	// Try and update an old lease first.
	var oldLease *ReadLease = ro.getReadLease(id)
	if oldLease != nil {
		oldLease.LogIndex = index
		oldLease.StartTime = uint64(time.Now().UnixMicro())
		oldLease.Duration = ro.readLeaseDuration
		ro.updateSoonestExpiryTime(oldLease.StartTime + oldLease.Duration)
		return oldLease
	}

	// Otherwise, make a new lease entry.
	newLease := ReadLease{
		NodeId:    id,
		LogIndex:  index,
		StartTime: uint64(time.Now().UnixMicro()),
		Duration:  ro.readLeaseDuration}
	ro.readLeaseInfo[id] = &newLease
	ro.updateSoonestExpiryTime(newLease.StartTime + newLease.Duration)
	return ro.getReadLease(id)
}

func (ro *readOnly) processGrantedLease(lease ReadLease) *ReadLease {
	if lease.StartTime == 0 || lease.Duration == 0 ||
		lease.StartTime+lease.Duration <= uint64(time.Now().UnixMicro()) {
		// Expired lease, do not process.
		return nil
	}

	// Try and update an old lease first.
	// We subtract a small safeguard time to account for clock drift.
	// This function is run by followers/learners, which means their
	// leases will conservatively expire earlier to account for drift.
	var oldLease *ReadLease = ro.getReadLease(lease.NodeId)
	if oldLease != nil {
		oldLease.LogIndex = lease.LogIndex
		oldLease.StartTime = lease.StartTime
		oldLease.Duration = lease.Duration - _SAFEGUARD_CLOCK_DRIFT_MICROS
		ro.updateSoonestExpiryTime(oldLease.StartTime + oldLease.Duration)
		return oldLease
	}

	// Otherwise, make a new lease entry.
	ro.readLeaseInfo[lease.NodeId] = &ReadLease{
		NodeId:    lease.NodeId,
		LogIndex:  lease.LogIndex,
		StartTime: lease.StartTime,
		Duration:  lease.Duration - _SAFEGUARD_CLOCK_DRIFT_MICROS,
	}
	ro.updateSoonestExpiryTime(lease.StartTime + lease.Duration)
	return ro.getReadLease(lease.NodeId)
}

func (ro *readOnly) removeReadLease(id uint64) {
	delete(ro.readLeaseInfo, id)
	ro.soonestExpiryTime = 0
}

// addRequest adds a read only request into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	s := string(m.Entries[0].Data)
	if _, ok := ro.pendingReadIndex[s]; ok {
		return
	}
	ro.pendingReadIndex[s] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]bool)}
	ro.readIndexQueue = append(ro.readIndexQueue, s)
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
func (ro *readOnly) recvAck(id uint64, context []byte) map[uint64]bool {
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return nil
	}

	rs.acks[id] = true
	return rs.acks
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context)
	var rss []*readIndexStatus

	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss, rs)
		if okctx == ctx {
			found = true
			break
		}
	}

	if found {
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
