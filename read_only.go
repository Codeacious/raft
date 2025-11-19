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
	"container/heap"
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
	NodeId     uint64
	LogIndex   uint64
	StartTime  uint64
	Duration   uint64
	AckedIndex uint64
}

type heapItem struct {
	nodeId     uint64
	ackedIndex uint64
}

// readLeaseHeap implements heap.Interface for tracking minimum acked indices.
type readLeaseHeap struct {
	items   []*heapItem
	itemMap map[uint64]int // nodeId -> index in heap
}

type ReadLeaseMap struct {
	leases            map[uint64]*ReadLease
	heap              *readLeaseHeap
	soonestExpiryTime uint64
}

func (h *readLeaseHeap) Len() int { return len(h.items) }

func (h *readLeaseHeap) Less(i, j int) bool {
	return h.items[i].ackedIndex < h.items[j].ackedIndex
}

func (h *readLeaseHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.itemMap[h.items[i].nodeId] = i
	h.itemMap[h.items[j].nodeId] = j
}

func (h *readLeaseHeap) Push(x any) {
	item := x.(*heapItem)
	h.itemMap[item.nodeId] = len(h.items)
	h.items = append(h.items, item)
}

func (h *readLeaseHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	h.items = old[0 : n-1]
	delete(h.itemMap, item.nodeId)
	return item
}

func (h *readLeaseHeap) update(nodeId uint64, ackedIndex uint64) {
	if idx, ok := h.itemMap[nodeId]; ok {
		// Update existing item
		h.items[idx].ackedIndex = ackedIndex
		heap.Fix(h, idx)
	} else {
		// Add new item
		item := &heapItem{
			nodeId:     nodeId,
			ackedIndex: ackedIndex,
		}
		heap.Push(h, item)
	}
}

func (h *readLeaseHeap) remove(nodeId uint64) {
	if idx, ok := h.itemMap[nodeId]; ok {
		heap.Remove(h, idx)
	}
}

func newReadLeaseMap() *ReadLeaseMap {
	h := &readLeaseHeap{
		items:   make([]*heapItem, 0),
		itemMap: make(map[uint64]int),
	}
	heap.Init(h)
	return &ReadLeaseMap{
		leases:            make(map[uint64]*ReadLease),
		heap:              h,
		soonestExpiryTime: 0,
	}
}

func (rlm *ReadLeaseMap) Put(id uint64, lease *ReadLease) {
	rlm.leases[id] = lease
	rlm.heap.update(id, lease.AckedIndex)
	rlm.UpdateSoonestExpiryTime(lease.StartTime + lease.Duration)
}

func (rlm *ReadLeaseMap) Get(id uint64) (*ReadLease, bool) {
	lease, ok := rlm.leases[id]
	return lease, ok
}

func (rlm *ReadLeaseMap) Delete(id uint64) {
	delete(rlm.leases, id)
	rlm.heap.remove(id)
}

func (rlm *ReadLeaseMap) UpdateAckedIndex(id uint64, index uint64) {
	if lease, ok := rlm.leases[id]; ok {
		if index > lease.AckedIndex {
			lease.AckedIndex = index
			rlm.heap.update(id, index)
		}
	}
}

func (rlm *ReadLeaseMap) MinAckedIndex() uint64 {
	if rlm.heap.Len() == 0 {
		return 0
	}
	return rlm.heap.items[0].ackedIndex
}

func (rlm *ReadLeaseMap) Len() int {
	return len(rlm.leases)
}

func (rlm *ReadLeaseMap) Range(f func(id uint64, lease *ReadLease) bool) {
	for id, lease := range rlm.leases {
		if !f(id, lease) {
			break
		}
	}
}

func (rlm *ReadLeaseMap) GetSoonestExpiryTime() uint64 {
	return rlm.soonestExpiryTime
}

func (rlm *ReadLeaseMap) UpdateSoonestExpiryTime(expTime uint64) {
	if rlm.soonestExpiryTime == 0 || expTime < rlm.soonestExpiryTime {
		rlm.soonestExpiryTime = expTime
	}
}

func (rlm *ReadLeaseMap) CleanupExpiredLeases() {
	currentTime := uint64(time.Now().UnixMicro())
	if currentTime < rlm.soonestExpiryTime {
		return
	}

	rlm.soonestExpiryTime = 0

	for id, lease := range rlm.leases {
		if lease.StartTime+lease.Duration <= currentTime {
			rlm.Delete(id)
		} else {
			rlm.UpdateSoonestExpiryTime(lease.StartTime + lease.Duration)
		}
	}
}

func (rlm *ReadLeaseMap) ResetSoonestExpiryTime() {
	rlm.soonestExpiryTime = 0
}

func (rl *ReadLease) Marshal() []byte {
	buf := make([]byte, 40)
	binary.BigEndian.PutUint64(buf[0:8], rl.NodeId)
	binary.BigEndian.PutUint64(buf[8:16], rl.LogIndex)
	binary.BigEndian.PutUint64(buf[16:24], rl.StartTime)
	binary.BigEndian.PutUint64(buf[24:32], rl.Duration)
	binary.BigEndian.PutUint64(buf[32:40], rl.AckedIndex)
	return buf
}

func UnmarshalReadLease(data []byte) (*ReadLease, error) {
	if len(data) != 40 {
		return nil, fmt.Errorf("invalid ReadLease data length: got %d, want 40", len(data))
	}
	return &ReadLease{
		NodeId:     binary.BigEndian.Uint64(data[0:8]),
		LogIndex:   binary.BigEndian.Uint64(data[8:16]),
		StartTime:  binary.BigEndian.Uint64(data[16:24]),
		Duration:   binary.BigEndian.Uint64(data[24:32]),
		AckedIndex: binary.BigEndian.Uint64(data[32:40]),
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
	readLeases         *ReadLeaseMap
	readLeaseDuration  uint64
	maxReadLeases      int
	shouldAskForLease  bool
	lastLeaseAskedTime uint64
	creationTime       uint64
	safeguardPassed    bool
	leaseCatchupMargin int
}

const _SAFEGUARD_CLOCK_DRIFT_MICROS = 1000 // 1ms clock drift allowed
const _LEASE_ASK_INTERVAL_MICROS = 10000   // 10ms between lease asks

func newReadOnly(option ReadOnlyOption, duration uint64,
	maxleases int, askforlease bool, catchupmargin int) *readOnly {
	return &readOnly{
		option:             option,
		pendingReadIndex:   make(map[string]*readIndexStatus),
		readLeases:         newReadLeaseMap(),
		readLeaseDuration:  duration,
		maxReadLeases:      maxleases,
		shouldAskForLease:  askforlease,
		lastLeaseAskedTime: 0,
		creationTime:       uint64(time.Now().UnixMicro()),
		safeguardPassed:    false,
		leaseCatchupMargin: catchupmargin,
	}
}

func (ro *readOnly) safeguardHasPassed() bool {
	if ro.option != ReadOnlyGrantLeases {
		return true
	}
	if !ro.safeguardPassed {
		now := uint64(time.Now().UnixMicro())
		if now-ro.creationTime > ro.readLeaseDuration+_SAFEGUARD_CLOCK_DRIFT_MICROS {
			ro.safeguardPassed = true
		}
	}
	return ro.safeguardPassed
}

func (ro *readOnly) canAskForLease() bool {
	if ro.option != ReadOnlyGrantLeases || !ro.shouldAskForLease {
		return false
	}

	now := uint64(time.Now().UnixMicro())
	return now-ro.lastLeaseAskedTime > _LEASE_ASK_INTERVAL_MICROS
}

func (ro *readOnly) markAskedForLease() {
	ro.lastLeaseAskedTime = uint64(time.Now().UnixMicro())
}

func (ro *readOnly) getNumReadLeases() int {
	if ro.option != ReadOnlyGrantLeases {
		return -1
	}
	ro.readLeases.CleanupExpiredLeases()
	return ro.readLeases.Len()
}

func (ro *readOnly) getReadLease(id uint64) *ReadLease {
	if ro.option != ReadOnlyGrantLeases {
		return nil
	}
	lease, ok := ro.readLeases.Get(id)
	if ok {
		return lease
	} else {
		return nil
	}
}

func (ro *readOnly) hasActiveReadLease(id uint64) bool {
	if ro.option != ReadOnlyGrantLeases {
		return false
	}
	lease := ro.getReadLease(id)
	if lease == nil {
		return false
	}

	if lease.StartTime+lease.Duration > uint64(time.Now().UnixMicro()) {
		return true
	} else {
		ro.readLeases.CleanupExpiredLeases()
		return false
	}
}

func (ro *readOnly) grantNewLease(id uint64, ackedIndex uint64, logIndex uint64) *ReadLease {
	if ro.option != ReadOnlyGrantLeases {
		return nil
	}

	// Otherwise, make a new lease entry.
	newLease := ReadLease{
		NodeId:     id,
		LogIndex:   logIndex,
		StartTime:  uint64(time.Now().UnixMicro()),
		Duration:   ro.readLeaseDuration,
		AckedIndex: ackedIndex}
	ro.readLeases.Put(id, &newLease)
	return ro.getReadLease(id)
}

func (ro *readOnly) processGrantedLease(lease ReadLease) *ReadLease {
	if ro.option != ReadOnlyGrantLeases {
		return nil
	}
	if lease.StartTime == 0 || lease.Duration == 0 ||
		lease.StartTime+lease.Duration <= uint64(time.Now().UnixMicro()) {
		// Expired lease, do not process.
		return nil
	}

	// We subtract a small safeguard time to account for clock drift.
	// This function is run by followers/learners, which means their
	// leases will conservatively expire earlier to account for drift.
	ro.readLeases.Put(lease.NodeId, &ReadLease{
		NodeId:     lease.NodeId,
		LogIndex:   lease.LogIndex,
		StartTime:  lease.StartTime,
		Duration:   lease.Duration - _SAFEGUARD_CLOCK_DRIFT_MICROS,
		AckedIndex: lease.AckedIndex,
	})
	return ro.getReadLease(lease.NodeId)
}

func (ro *readOnly) removeReadLease(id uint64) {
	if ro.option != ReadOnlyGrantLeases {
		return
	}
	ro.readLeases.Delete(id)
}

func (ro *readOnly) markReadLeaseIndex(id uint64, index uint64) {
	if ro.option != ReadOnlyGrantLeases {
		return
	}
	ro.readLeases.UpdateAckedIndex(id, index)
}

func (ro *readOnly) getMinReadLeaseAckedIndex() uint64 {
	if ro.option != ReadOnlyGrantLeases {
		return 0
	}
	ro.readLeases.CleanupExpiredLeases()
	return ro.readLeases.MinAckedIndex()
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
