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
	"sync"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

const _SAFEGUARD_CLOCK_DRIFT_MICROS = 1000         // 1ms clock drift allowed
const _LEASE_ASK_INTERVAL_MICROS = 10000           // 10ms between lease asks
const _ACK_INDEX_ASK_INTERVAL_MICROS = 5000        // 5ms between ack index asks
const _LEASE_RENEWAL_THRESHOLD_MICROS = 150000     // 150ms before expiry
const _READ_INDEX_LOCAL_HOLD_DURATION_MICROS = 750 // 750us to hold a read index request before sending it to leader
const _READ_INDEX_LOCAL_HOLD_LOG_THRESHOLD = 100   // Log index threshold for holding a read index request

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
	LeaseId      uint64
	NodeId       uint64
	LogIndex     uint64
	StartTime    uint64
	Duration     uint64
	AckedIndex   uint64
	LastAckAsked uint64
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
	pendingLeases     map[uint64]uint64 // leaseID -> startTime
	pendingLeaseCtr   uint64
}

type ReadLeaseStats struct {
	TimesReadLeaseUsed uint64
	TimesGotReadQuery  uint64
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
		pendingLeases:     make(map[uint64]uint64),
		pendingLeaseCtr:   1}
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

func (rlm *ReadLeaseMap) GetNewLeaseId() uint64 {
	leaseId := rlm.pendingLeaseCtr
	rlm.pendingLeaseCtr++
	return leaseId
}

func (rl *ReadLease) Marshal() []byte {
	// Wire format: [LeaseId, NodeId, LogIndex, StartTime, Duration, AckedIndex] each 8 bytes
	buf := make([]byte, 48)
	binary.BigEndian.PutUint64(buf[0:8], rl.LeaseId)
	binary.BigEndian.PutUint64(buf[8:16], rl.NodeId)
	binary.BigEndian.PutUint64(buf[16:24], rl.LogIndex)
	binary.BigEndian.PutUint64(buf[24:32], rl.StartTime)
	binary.BigEndian.PutUint64(buf[32:40], rl.Duration)
	binary.BigEndian.PutUint64(buf[40:48], rl.AckedIndex)
	return buf
}

func UnmarshalReadLease(data []byte) (*ReadLease, error) {
	if len(data) != 48 {
		return nil, fmt.Errorf("invalid ReadLease data length: got %d, want 48", len(data))
	}
	return &ReadLease{
		LeaseId:      binary.BigEndian.Uint64(data[0:8]),
		NodeId:       binary.BigEndian.Uint64(data[8:16]),
		LogIndex:     binary.BigEndian.Uint64(data[16:24]),
		StartTime:    binary.BigEndian.Uint64(data[24:32]),
		Duration:     binary.BigEndian.Uint64(data[32:40]),
		AckedIndex:   binary.BigEndian.Uint64(data[40:48]),
		LastAckAsked: 0,
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
	readLeaseStats     ReadLeaseStats
	readLeaseDuration  uint64
	maxReadLeases      int
	shouldAskForLease  bool
	lastLeaseAskedTime uint64
	creationTime       uint64
	safeguardPassed    bool
	leaseCatchupMargin int
}

func newReadOnly(option ReadOnlyOption, duration uint64,
	maxleases int, askforlease bool, catchupmargin int) *readOnly {
	return &readOnly{
		option:             option,
		pendingReadIndex:   make(map[string]*readIndexStatus),
		readLeases:         newReadLeaseMap(),
		readLeaseStats:     ReadLeaseStats{},
		readLeaseDuration:  duration,
		maxReadLeases:      maxleases,
		shouldAskForLease:  askforlease,
		lastLeaseAskedTime: 0,
		creationTime:       uint64(time.Now().UnixMicro()),
		safeguardPassed:    false,
		leaseCatchupMargin: catchupmargin,
	}
}

func (ro *readOnly) getMarkedLeaseId() uint64 {
	if ro.option != ReadOnlyGrantLeases {
		return 0
	}
	nid := ro.readLeases.GetNewLeaseId()
	ro.readLeases.pendingLeases[nid] = uint64(time.Now().UnixMicro())
	return nid
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

func (ro *readOnly) microsUntilLeaseExpired(id uint64) uint64 {
	if ro.option != ReadOnlyGrantLeases {
		return 0
	}
	lease := ro.getReadLease(id)
	if lease == nil {
		return 0
	}

	now := uint64(time.Now().UnixMicro())
	if lease.StartTime+lease.Duration > now {
		return (lease.StartTime + lease.Duration) - now
	} else {
		ro.readLeases.CleanupExpiredLeases()
		return 0
	}
}

func (ro *readOnly) hasActiveReadLease(id uint64) bool {
	return ro.microsUntilLeaseExpired(id) > 0
}

func (ro *readOnly) grantNewLease(nodeId uint64, leaseId uint64,
	ackedIndex uint64, logIndex uint64) *ReadLease {
	if ro.option != ReadOnlyGrantLeases {
		return nil
	}

	// Otherwise, make a new lease entry.
	newLease := ReadLease{
		LeaseId:      leaseId,
		NodeId:       nodeId,
		LogIndex:     logIndex,
		StartTime:    uint64(time.Now().UnixMicro()),
		Duration:     ro.readLeaseDuration,
		AckedIndex:   ackedIndex,
		LastAckAsked: 0}
	ro.readLeases.Put(nodeId, &newLease)
	return ro.getReadLease(nodeId)
}

func (ro *readOnly) processGrantedLease(lease ReadLease) *ReadLease {
	if ro.option != ReadOnlyGrantLeases {
		return nil
	}

	// TODO: Have a feature toggle that skips this and uses lease.StartTime instead,
	// for whenever clock synchronization is enabled
	savedStartTime, ok := ro.readLeases.pendingLeases[lease.LeaseId]
	if !ok {
		// There's a couple reasons this could happen, some of them are benign, so ignore it.
		return nil
	}

	if lease.Duration == 0 ||
		savedStartTime+lease.Duration <= uint64(time.Now().UnixMicro()) {
		// Expired lease, do not process.
		return nil
	}

	// We subtract a small safeguard time to account for clock drift.
	// This function is run by followers/learners, which means their
	// leases will conservatively expire earlier to account for drift.
	ro.readLeases.Put(lease.NodeId, &ReadLease{
		LeaseId:      lease.LeaseId,
		NodeId:       lease.NodeId,
		LogIndex:     lease.LogIndex,
		StartTime:    savedStartTime,
		Duration:     lease.Duration - _SAFEGUARD_CLOCK_DRIFT_MICROS,
		AckedIndex:   lease.AckedIndex,
		LastAckAsked: 0,
	})

	// Clear all pending leases once we get one.
	clear(ro.readLeases.pendingLeases)

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

func (ro *readOnly) markAckAskedForAtTime(id uint64, markTime uint64) {
	if ro.option != ReadOnlyGrantLeases {
		return
	}
	lease := ro.getReadLease(id)
	if lease != nil {
		lease.LastAckAsked = markTime
	}
}

func (ro *readOnly) getNodesWithAckIndexLessThan(cutoff uint64) []uint64 {
	result := make([]uint64, 0)
	if ro.option != ReadOnlyGrantLeases {
		return result
	}
	ro.readLeases.CleanupExpiredLeases()
	ro.readLeases.Range(func(id uint64, lease *ReadLease) bool {
		if lease.AckedIndex < cutoff {
			result = append(result, id)
		}
		return true
	})
	return result
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

type DelayedMsgReadIndex struct {
	msg           pb.Message
	forwardAtTime uint64
	requiredIndex uint64
}

type readIndexDelayer struct {
	delayedReadIndexReqs    []DelayedMsgReadIndex
	delayedReadsTimer       *time.Timer
	delayedReadsChannel     chan pb.Message
	delayedReadsChanDone    chan struct{}
	delayedReadsHandlerLock sync.Mutex
}

func newReadIndexDelayer(outC chan pb.Message, outDone chan struct{}) *readIndexDelayer {
	return &readIndexDelayer{
		delayedReadIndexReqs:    make([]DelayedMsgReadIndex, 0, 50),
		delayedReadsTimer:       nil,
		delayedReadsChannel:     outC,
		delayedReadsChanDone:    outDone,
		delayedReadsHandlerLock: sync.Mutex{},
	}
}

func (ro *readOnly) markReadIndexStat(usedReadLease bool) {
	if usedReadLease {
		ro.readLeaseStats.TimesReadLeaseUsed++
	}
	ro.readLeaseStats.TimesGotReadQuery++
}

func (rd *readIndexDelayer) addDelayedReadIndexReq(msg pb.Message, requiredIndex uint64) {
	rd.delayedReadIndexReqs = append(rd.delayedReadIndexReqs, DelayedMsgReadIndex{
		msg:           msg,
		forwardAtTime: uint64(time.Now().UnixMicro()) + _READ_INDEX_LOCAL_HOLD_DURATION_MICROS,
		requiredIndex: requiredIndex,
	})
	rd.rearmDelayedReadIndexTimer()
}

func (rd *readIndexDelayer) rearmDelayedReadIndexTimer() {
	if rd.delayedReadsTimer == nil {
		rd.delayedReadsTimer = time.AfterFunc(
			time.Duration(_READ_INDEX_LOCAL_HOLD_DURATION_MICROS)*time.Microsecond,
			func() {
				rd.delayedReadsTimer = nil
				toSend := rd.fireDelayedReadIndexRequests(0)
				for _, next := range toSend {
					select {
					case <-rd.delayedReadsChanDone:
						return
					case rd.delayedReadsChannel <- next:
					}
				}

			})
	}
}

func (rd *readIndexDelayer) fireDelayedReadIndexRequests(commitIndex uint64) []pb.Message {
	if len(rd.delayedReadIndexReqs) == 0 {
		return nil
	}
	// We need to ensure the main raft loop and the timer don't fire this at the same time.
	rd.delayedReadsHandlerLock.Lock()
	defer rd.delayedReadsHandlerLock.Unlock()

	var workingSet []pb.Message
	now := uint64(time.Now().UnixMicro())
	for len(rd.delayedReadIndexReqs) > 0 {
		next := rd.delayedReadIndexReqs[0]
		if next.requiredIndex <= commitIndex {
			next.msg.Index = next.requiredIndex
			if workingSet == nil {
				workingSet = make([]pb.Message, 0, 1)
			}
			workingSet = append(workingSet, next.msg)
			rd.delayedReadIndexReqs = rd.delayedReadIndexReqs[1:]
		} else if next.forwardAtTime <= now {
			next.msg.Index = next.requiredIndex
			next.msg.Reject = true
			if workingSet == nil {
				workingSet = make([]pb.Message, 0, 1)
			}
			workingSet = append(workingSet, next.msg)
			rd.delayedReadIndexReqs = rd.delayedReadIndexReqs[1:]
		} else {
			break
		}
	}
	if len(rd.delayedReadIndexReqs) > 0 {
		rd.rearmDelayedReadIndexTimer()
	}
	return workingSet
}

func (rd *readIndexDelayer) clear() {
	rd.delayedReadsHandlerLock.Lock()
	defer rd.delayedReadsHandlerLock.Unlock()
	if rd.delayedReadsTimer != nil {
		rd.delayedReadsTimer.Stop()
		rd.delayedReadsTimer = nil
	}
	rd.delayedReadIndexReqs = make([]DelayedMsgReadIndex, 0, 50)
}
