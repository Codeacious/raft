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
	"slices"
	"sync"
	"time"

	"go.etcd.io/raft/v3/quorum"
	pb "go.etcd.io/raft/v3/raftpb"
)

// TODO: All of these constants should probably be in Raft's Config.
const _SAFEGUARD_CLOCK_DRIFT_MICROS = 1000          // 1ms clock drift allowed
const _LEASE_ASK_INTERVAL_MICROS = 10000            // 10ms between lease asks
const _ACK_INDEX_ASK_INTERVAL_MICROS = 5000         // 5ms between ack index asks
const _LEASE_RENEWAL_THRESHOLD_MICROS = 200000      // 200ms before expiry; two heartbeats at etcd's 100ms default
const _READ_INDEX_LOCAL_HOLD_DURATION_MICROS = 5000 // 5ms to hold a read index request before sending it to leader
const _READ_INDEX_LOCAL_HOLD_LOG_THRESHOLD = 100    // Log index threshold for holding a read index request

// If a lease being renewed has less than this left, the leader sends a
// heartbeat immediately instead of waiting for the next scheduled one.
const _LEASE_EXPEDITE_THRESHOLD_MICROS = 50000 // 50ms

// How long to wait for a majority to reply to such a heartbeat before assuming
// it was lost and sending another.
const _LEASE_ROUND_RETRY_MICROS = 10000 // 10ms

// leaseClockEpoch is a process-local monotonic clock
// for all read-lease timing.
// Every lease timestamp comparison is node-local. A follower
// substitutes its own saved start time in processGrantedLease and discards the
// leader's wire StartTime.
// The cluster must still be clock-rate synchronized
// when using ReadOnlyGrantLeases.
var leaseClockEpoch = time.Now()

// Using nowMicros() ensures no adjustments to wall time
// affect read lease timing.
func nowMicros() uint64 {
	return uint64(time.Since(leaseClockEpoch).Microseconds())
}

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

// ReadLeaseMap is the leader's record of the leases it has granted to
// followers, keyed by follower id.
type ReadLeaseMap struct {
	leases            map[uint64]*ReadLease
	heap              *readLeaseHeap
	soonestExpiryTime uint64
}

// pendingGrant is a follower's lease request, held by the leader until it can
// be granted via quorum thru heartbeats after the request arrived.
type pendingGrant struct {
	leaseId    uint64
	proposedAt uint64 // when the request arrived (nowMicros)
	askedIndex uint64 // the follower's commit index when it asked
	needRound  uint64 // grantable once quorum echoes this lease round or later
}

// leaseEchoIndexer lets computeLeaseQuorumRound find the highest lease round a
// majority of voters have returned. It reuses the majority calculation raft uses
// for the commit index, so joint configurations are handled the same way. The
// leader always counts as having returned its latest round.
type leaseEchoIndexer struct {
	echoes map[uint64]uint64
	self   uint64
	round  uint64
}

func (l leaseEchoIndexer) AckedIndex(id uint64) (quorum.Index, bool) {
	if id == l.self {
		return quorum.Index(l.round), true
	}
	r, ok := l.echoes[id]
	return quorum.Index(r), ok
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

// newLeaseIdSeed returns the first lease id to mint. The upper 32 bits are
// randomized and forced nonzero so that lease ids are not reused across
// restarts or terms.
//
// A counter restarting at 1 would let a grant still in flight from a previous
// incarnation match a freshly re-issued pending id in processGrantedLease,
// which dates the lease from the *later* ask — the follower would then serve
// reads past the leader's view of that lease's expiry, unclamped. Mirrors the
// client read-gate marker seeding in etcd's client/v3/sidechannel.
//
// 31 bits of entropy rather than 32: Intn takes an int, and 1<<32 does not fit
// one on 32-bit platforms. Collision odds are negligible either way.
func newLeaseIdSeed() uint64 {
	return uint64(globalRand.Intn(1<<31-1)+1)<<32 | 1
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
		soonestExpiryTime: 0}
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

func (rlm *ReadLeaseMap) UpdateSoonestExpiryTime(expTime uint64) {
	if rlm.soonestExpiryTime == 0 || expTime < rlm.soonestExpiryTime {
		rlm.soonestExpiryTime = expTime
	}
}

func (rlm *ReadLeaseMap) CleanupExpiredLeases() {
	currentTime := nowMicros()
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

// readOnly holds a node's read-path state: the upstream read-index queue, and
// the read-lease state added on top of it. reset() rebuilds it, so none of it
// outlives a term.
type readOnly struct {
	option           ReadOnlyOption
	pendingReadIndex map[string]*readIndexStatus
	readIndexQueue   []string

	// heldLease is this node's own lease, or nil. A follower's is granted by the
	// leader; a leader grants its own. Reads are served locally only while it is
	// live.
	heldLease *ReadLease
	// readLeaseAckedIndex is this node's own read-lease ack index
	// across all read leases. It *cannot* be cleared so long as
	// the node has any read leases or pending read leases, but
	// it should never need to be cleared regardless.
	readLeaseAckedIndex uint64
	readLeaseStats      ReadLeaseStats
	creationTime        uint64

	// pendingLeases maps each lease id this follower has requested to when it
	// asked. A granted lease is timed from the ask, on this node's clock.
	pendingLeases      map[uint64]uint64
	nextLeaseId        uint64
	lastLeaseAskedTime uint64

	// leader is the lease state only a leader uses.
	leader leaderLeaseState
	// safeguard is the new-leader safeguard.
	safeguard newLeaderSafeguard

	// Configuration. reset() carries these over when it rebuilds readOnly.
	readLeaseDuration  uint64
	maxReadLeases      int
	shouldAskForLease  bool
	leaseCatchupMargin int
}

// leaderLeaseState is what a leader tracks for leases: its lease rounds, the
// pending renewal of its own lease, and the leases it grants to followers.
// Empty on a follower.
type leaderLeaseState struct {
	// leaseRound numbers the leader's heartbeat broadcasts in a leased mode.
	// Each broadcast increments it and sends it in MsgHeartbeat.Index.
	// Followers return it in MsgHeartbeatResp.Index. A majority returning round
	// N shows they still followed this leader when N was sent, so a lease
	// requested before N was sent may be granted. Restarts at 0 each term.
	leaseRound uint64
	// leaseRoundSentAt is when the latest round was sent.
	leaseRoundSentAt uint64
	// followerEchoedRounds is the highest lease round each follower has echoed.
	followerEchoedRounds map[uint64]uint64
	// leaseQuorumRound is the highest round given by a majority of followers.
	// Pending leases waiting on this or an earlier round can be granted.
	leaseQuorumRound uint64

	// pendingSelfStart is when the leader requested a renewal of its own lease,
	// or 0 if none is pending. It becomes the new lease's start time.
	pendingSelfStart uint64
	// pendingSelfRound is the lease round a majority must return before that
	// renewal takes effect.
	pendingSelfRound uint64
	// selfLeaseBanned bars the leader from holding its own lease for the rest of
	// the term. See banSelfLease.
	selfLeaseBanned bool

	// grantedLeases holds the leases this leader has granted to followers.
	grantedLeases *ReadLeaseMap
	// pendingGrants holds follower lease requests waiting on a lease round, by
	// follower id. See parkGrant.
	pendingGrants map[uint64]pendingGrant
	// grantBanned holds nodes barred from being granted a lease for the rest of
	// the term. See banGrant.
	grantBanned map[uint64]struct{}
}

// newLeaderSafeguard keeps a new leader in a leased mode from committing while
// a lease from an earlier term may still be live. See
// raft.newLeaderSafeguardPassed.
// The safeguard can be shortcut/lifted once all nodes have acked they are in the
// new, current term.
type newLeaderSafeguard struct {
	// passed is set once the safeguard lifts, and stays set for the term.
	passed bool
	// startMicros is when this node became leader, which is when the safeguard
	// starts.
	startMicros uint64
	// windowMicros is how long the safeguard lasts.
	windowMicros uint64
	// confirmedAtTerm records peers heard from at the current term.
	confirmedAtTerm map[uint64]struct{}
	// unappliedConfNodes contains node ids named by unapplied conf-change
	// entries in the log at election time. The safeguard shortcut must wait for
	// acks from these nodes as well as every node in the current configuration.
	unappliedConfNodes map[uint64]struct{}
	// scanFailed indicates that the election-time log tail scan to populate
	// unappliedConfNodes failed, so it may be incomplete.
	// Disables the safeguard shortcut entirely.
	scanFailed bool
}

func newReadOnly(option ReadOnlyOption, duration uint64,
	maxleases int, askforlease bool, catchupmargin int) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
		creationTime:     nowMicros(),
		pendingLeases:    make(map[uint64]uint64),
		nextLeaseId:      newLeaseIdSeed(),
		leader: leaderLeaseState{
			followerEchoedRounds: make(map[uint64]uint64),
			grantedLeases:        newReadLeaseMap(),
			pendingGrants:        make(map[uint64]pendingGrant),
			grantBanned:          make(map[uint64]struct{}),
		},
		safeguard: newLeaderSafeguard{
			confirmedAtTerm:    make(map[uint64]struct{}),
			unappliedConfNodes: make(map[uint64]struct{}),
		},
		readLeaseDuration:  duration,
		maxReadLeases:      maxleases,
		shouldAskForLease:  askforlease,
		leaseCatchupMargin: catchupmargin,
	}
}

// getMarkedLeaseId mints a new lease id and records it as asked for now.
func (ro *readOnly) getMarkedLeaseId() uint64 {
	nid := ro.nextLeaseId
	ro.nextLeaseId++
	ro.pendingLeases[nid] = nowMicros()
	return nid
}

// usesReadLeases indicates whether reads may be served locally under a lease.
func (ro *readOnly) usesReadLeases() bool {
	return ro.option == ReadOnlyGrantLeases || ro.option == ReadOnlyLeaseBased
}

// start starts the safeguard. Called from becomeLeader. For the next
// windowMicros this leader commits nothing, because a lease granted in the
// previous term may still be live and this leader cannot see it. Timed in
// microseconds, like lease expiry.
func (s *newLeaderSafeguard) start(windowMicros uint64) {
	s.windowMicros = windowMicros
	s.startMicros = nowMicros()
	s.passed = windowMicros == 0
}

// hasPassed reports whether the safeguard has lifted, either because its window
// has elapsed (marking it passed) or because the confirmation shortcut already
// lifted it.
func (s *newLeaderSafeguard) hasPassed() bool {
	if !s.passed && nowMicros()-s.startMicros > s.windowMicros {
		s.passed = true
	}
	return s.passed
}

// markPassed marks the safeguard as passed. Used by the confirmation shortcut,
// so a later configuration change cannot un-pass a window already lifted.
func (s *newLeaderSafeguard) markPassed() {
	s.passed = true
}

// confirmAtTerm records that id has been heard from at this node's current
// term, and so has run reset() and dropped any prior-term lease.
func (s *newLeaderSafeguard) confirmAtTerm(id uint64) {
	if !s.passed {
		s.confirmedAtTerm[id] = struct{}{}
	}
}

func (s *newLeaderSafeguard) isConfirmedAtTerm(id uint64) bool {
	_, ok := s.confirmedAtTerm[id]
	return ok
}

// banGrant bars id from being granted a read lease for the rest of this term,
// because a configuration change removing it is in flight, and drops its held
// request.
func (l *leaderLeaseState) banGrant(id uint64) {
	l.grantBanned[id] = struct{}{}
	l.dropPendingGrant(id)
}

func (l *leaderLeaseState) isGrantBanned(id uint64) bool {
	_, ok := l.grantBanned[id]
	return ok
}

// banGrants bans every node in ids (see banGrant) and returns the first of them
// that currently holds an active lease, if any.
func (ro *readOnly) banGrants(ids []uint64) (holder uint64, found bool) {
	for _, id := range ids {
		ro.leader.banGrant(id)
		if !found && ro.hasActiveGrantedLease(id) {
			holder, found = id, true
		}
	}
	return holder, found
}

// banSelfLease bars this leader from holding its own lease for the rest of the
// term, and drops the one it holds along with any pending renewal. Used when a
// configuration change removing the leader is in flight. From then on its reads
// go through read-index rounds.
func (ro *readOnly) banSelfLease() {
	ro.leader.selfLeaseBanned = true
	ro.leader.pendingSelfStart = 0
	ro.leader.pendingSelfRound = 0
	ro.heldLease = nil
}

func (ro *readOnly) canAskForLease() bool {
	if ro.option != ReadOnlyGrantLeases || !ro.shouldAskForLease {
		return false
	}

	now := nowMicros()
	return now-ro.lastLeaseAskedTime > _LEASE_ASK_INTERVAL_MICROS
}

func (ro *readOnly) getNumGrantedLeases() int {
	if ro.option != ReadOnlyGrantLeases {
		return -1
	}
	ro.leader.grantedLeases.CleanupExpiredLeases()
	return ro.leader.grantedLeases.Len()
}

func (ro *readOnly) getGrantedLease(id uint64) *ReadLease {
	if ro.option != ReadOnlyGrantLeases {
		return nil
	}
	lease, ok := ro.leader.grantedLeases.Get(id)
	if ok {
		return lease
	}
	return nil
}

func (ro *readOnly) microsUntilGrantedLeaseExpired(id uint64) uint64 {
	if ro.option != ReadOnlyGrantLeases {
		return 0
	}
	lease := ro.getGrantedLease(id)
	if lease == nil {
		return 0
	}
	now := nowMicros()
	if lease.StartTime+lease.Duration > now {
		return (lease.StartTime + lease.Duration) - now
	}
	ro.leader.grantedLeases.CleanupExpiredLeases()
	return 0
}

func (ro *readOnly) hasActiveGrantedLease(id uint64) bool {
	return ro.microsUntilGrantedLeaseExpired(id) > 0
}

// canGrantLeaseSlot reports whether id may take a lease under MaxNumReadLeases:
// either a slot is free or id already holds a lease.
func (ro *readOnly) canGrantLeaseSlot(id uint64) bool {
	return ro.getNumGrantedLeases() < ro.maxReadLeases || ro.hasActiveGrantedLease(id)
}

// microsUntilHeldLeaseExpired returns how long this node's own lease has left,
// or 0 if it has none. Clears an expired lease as a side effect.
func (ro *readOnly) microsUntilHeldLeaseExpired() uint64 {
	if !ro.usesReadLeases() || ro.heldLease == nil {
		return 0
	}
	now := nowMicros()
	if ro.heldLease.StartTime+ro.heldLease.Duration > now {
		return (ro.heldLease.StartTime + ro.heldLease.Duration) - now
	}
	ro.heldLease = nil
	return 0
}

func (ro *readOnly) hasActiveHeldLease() bool {
	return ro.microsUntilHeldLeaseExpired() > 0
}

// getHeldLease returns this node's own lease, or nil if it has none or it has
// expired.
func (ro *readOnly) getHeldLease() *ReadLease {
	if ro.microsUntilHeldLeaseExpired() == 0 {
		return nil
	}
	return ro.heldLease
}

// armSelfLease requests a renewal of the leader's own lease. The renewal takes
// effect once a majority returns the next lease round, but the new lease is
// timed from now, not from when the replies arrive.
func (ro *readOnly) armSelfLease() {
	if !ro.usesReadLeases() {
		return
	}
	ro.leader.pendingSelfStart = nowMicros()
	ro.leader.pendingSelfRound = ro.leader.leaseRound + 1
}

// installPendingSelfLease makes the pending renewal the leader's current lease.
// Call only once a majority has returned pendingSelfRound or later.
func (ro *readOnly) installPendingSelfLease(nodeId uint64, logIndex uint64) {
	if !ro.usesReadLeases() || ro.leader.pendingSelfStart == 0 {
		return
	}
	ro.heldLease = &ReadLease{
		NodeId:    nodeId,
		LogIndex:  logIndex,
		StartTime: ro.leader.pendingSelfStart,
		Duration:  ro.readLeaseDuration,
	}
	ro.leader.pendingSelfStart = 0
	ro.leader.pendingSelfRound = 0
}

// mintLeaseRound starts the next lease round. Called for each heartbeat
// broadcast in a leased mode.
func (l *leaderLeaseState) mintLeaseRound() uint64 {
	l.leaseRound++
	l.leaseRoundSentAt = nowMicros()
	return l.leaseRound
}

// recordLeaseEcho records that follower id returned round. It returns false and
// ignores the reply if this leader never sent that round or id has already
// returned a later one.
func (l *leaderLeaseState) recordLeaseEcho(id uint64, round uint64) bool {
	if round > l.leaseRound {
		return false
	}
	if round <= l.followerEchoedRounds[id] {
		return false
	}
	l.followerEchoedRounds[id] = round
	return true
}

// computeLeaseQuorumRound returns the highest round a majority of voters has
// returned. A voter that has returned none counts as 0.
func (l *leaderLeaseState) computeLeaseQuorumRound(voters quorum.JointConfig, self uint64) uint64 {
	return uint64(voters.CommittedIndex(leaseEchoIndexer{
		echoes: l.followerEchoedRounds, self: self, round: l.leaseRound,
	}))
}

// leaseRoundInFlight reports whether a majority has yet to return the latest
// round.
func (l *leaderLeaseState) leaseRoundInFlight() bool {
	return l.leaseQuorumRound < l.leaseRound
}

// leaseProposalUrgent reports whether any pending renewal or follower request
// is for a lease with less than _LEASE_EXPEDITE_THRESHOLD_MICROS left, or no
// lease at all.
func (ro *readOnly) leaseProposalUrgent() bool {
	if ro.leader.pendingSelfStart != 0 &&
		ro.microsUntilHeldLeaseExpired() < _LEASE_EXPEDITE_THRESHOLD_MICROS {
		return true
	}
	for id := range ro.leader.pendingGrants {
		if ro.microsUntilGrantedLeaseExpired(id) < _LEASE_EXPEDITE_THRESHOLD_MICROS {
			return true
		}
	}
	return false
}

// parkGrant holds a follower's lease request until a majority returns a round
// sent after it. If that follower already has a request held, the older one is
// kept and pg is dropped, unless the older one is stale. Followers re-ask every
// _LEASE_ASK_INTERVAL_MICROS, and replacing the request each time would keep
// moving the round it waits for, so it would never be granted. Returns whether
// pg was held.
func (ro *readOnly) parkGrant(nodeId uint64, pg pendingGrant) bool {
	if ro.option != ReadOnlyGrantLeases {
		return false
	}
	if old, ok := ro.leader.pendingGrants[nodeId]; ok && !ro.parkedGrantStale(old) {
		return false
	}
	pg.needRound = ro.leader.leaseRound + 1
	ro.leader.pendingGrants[nodeId] = pg
	return true
}

// parkedGrantStale reports whether pg has been held for a full lease duration,
// so a lease timed from its request would already have expired.
func (ro *readOnly) parkedGrantStale(pg pendingGrant) bool {
	return nowMicros()-pg.proposedAt >= ro.readLeaseDuration
}

// parkedGrantIds returns the ids of followers with a held request, sorted so
// responses go out in a deterministic order.
func (l *leaderLeaseState) parkedGrantIds() []uint64 {
	if len(l.pendingGrants) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(l.pendingGrants))
	for id := range l.pendingGrants {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// dropPendingGrant removes nodeId's held request, if any. Sends no response.
func (l *leaderLeaseState) dropPendingGrant(nodeId uint64) {
	delete(l.pendingGrants, nodeId)
}

// clearPendingLease forgets a lease id this follower requested, after the
// leader refused it.
func (ro *readOnly) clearPendingLease(leaseId uint64) {
	delete(ro.pendingLeases, leaseId)
}

// activeReadLeaseHolders returns the ids of every node whose lease has not yet
// expired. Allocates, and is meant for Status() rather than any hot path.
func (ro *readOnly) activeReadLeaseHolders() []uint64 {
	if ro.option != ReadOnlyGrantLeases {
		return nil
	}
	var ids []uint64
	ro.leader.grantedLeases.Range(func(id uint64, _ *ReadLease) bool {
		if ro.hasActiveGrantedLease(id) {
			ids = append(ids, id)
		}
		return true
	})
	return ids
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
		StartTime:    nowMicros(),
		Duration:     ro.readLeaseDuration,
		AckedIndex:   ackedIndex,
		LastAckAsked: 0}
	ro.leader.grantedLeases.Put(nodeId, &newLease)
	return ro.getGrantedLease(nodeId)
}

func (ro *readOnly) processGrantedLease(lease ReadLease) *ReadLease {
	if ro.option != ReadOnlyGrantLeases {
		return nil
	}

	// TODO: Have a feature toggle that skips this and uses lease.StartTime instead,
	// for whenever clock synchronization is enabled
	savedStartTime, ok := ro.pendingLeases[lease.LeaseId]
	if !ok {
		// There's a couple reasons this could happen, some of them are benign, so ignore it.
		return nil
	}

	if lease.Duration == 0 ||
		savedStartTime+lease.Duration <= nowMicros() {
		// Expired lease, do not process.
		return nil
	}

	// The granted ack index only ever raises our own.
	ro.readLeaseAckedIndex = max(ro.readLeaseAckedIndex, lease.AckedIndex)

	// We subtract a small safeguard time to account for clock drift.
	// This function is run by followers/learners, which means their
	// leases will conservatively expire earlier to account for drift.
	ro.heldLease = &ReadLease{
		LeaseId:      lease.LeaseId,
		NodeId:       lease.NodeId,
		LogIndex:     lease.LogIndex,
		StartTime:    savedStartTime,
		Duration:     lease.Duration - _SAFEGUARD_CLOCK_DRIFT_MICROS,
		AckedIndex:   ro.readLeaseAckedIndex,
		LastAckAsked: 0,
	}

	// Clear all pending leases once we get one.
	clear(ro.pendingLeases)

	return ro.getHeldLease()
}

// markSelfAckedIndex raises this node's acked index: a promise not to serve a
// local read while its commit index is below this value. It is raised even
// when no lease is held, so the promise carries over to the next lease. Must
// never decrease.
func (ro *readOnly) markSelfAckedIndex(index uint64) {
	if !ro.usesReadLeases() {
		return
	}
	ro.readLeaseAckedIndex = max(ro.readLeaseAckedIndex, index)
}

// updateGrantedAckedIndex raises the leader's record of follower id's acked
// index (see markSelfAckedIndex). The leader does not commit past the lowest
// of these; see maybeCommit.
func (ro *readOnly) updateGrantedAckedIndex(id uint64, index uint64) {
	if ro.option != ReadOnlyGrantLeases {
		return
	}
	ro.leader.grantedLeases.UpdateAckedIndex(id, index)
}

func (ro *readOnly) getMinGrantedAckedIndex() uint64 {
	if ro.option != ReadOnlyGrantLeases {
		return 0
	}
	ro.leader.grantedLeases.CleanupExpiredLeases()
	return ro.leader.grantedLeases.MinAckedIndex()
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
	// holdMicros is how long a read is held before it is forwarded to the
	// leader. Only tests change it from _READ_INDEX_LOCAL_HOLD_DURATION_MICROS.
	holdMicros uint64
}

// newReadIndexDelayer returns an empty delayer. node.run sets its channels.
func newReadIndexDelayer() *readIndexDelayer {
	return &readIndexDelayer{
		delayedReadIndexReqs:    make([]DelayedMsgReadIndex, 0, 50),
		delayedReadsTimer:       nil,
		delayedReadsHandlerLock: sync.Mutex{},
		holdMicros:              _READ_INDEX_LOCAL_HOLD_DURATION_MICROS,
	}
}

func (ro *readOnly) markReadIndexStat(usedReadLease bool) {
	if usedReadLease {
		ro.readLeaseStats.TimesReadLeaseUsed++
	}
	ro.readLeaseStats.TimesGotReadQuery++
}

func (rd *readIndexDelayer) addDelayedReadIndexReq(msg pb.Message, requiredIndex uint64) {
	rd.delayedReadsHandlerLock.Lock()
	defer rd.delayedReadsHandlerLock.Unlock()
	rd.delayedReadIndexReqs = append(rd.delayedReadIndexReqs, DelayedMsgReadIndex{
		msg:           msg,
		forwardAtTime: nowMicros() + rd.holdMicros,
		requiredIndex: requiredIndex,
	})
	rd.rearmDelayedReadIndexTimer()
}

// rearmDelayedReadIndexTimer (re)schedules the timer to fire at the soonest forwardAtTime.
// The caller must hold delayedReadsHandlerLock; both the timer field and the
// request queue are read/written under it.
func (rd *readIndexDelayer) rearmDelayedReadIndexTimer() {
	if len(rd.delayedReadIndexReqs) == 0 {
		if rd.delayedReadsTimer != nil {
			rd.delayedReadsTimer.Stop()
		}
		return
	}

	now := nowMicros()
	front := rd.delayedReadIndexReqs[0].forwardAtTime
	var d time.Duration
	if front > now {
		d = time.Duration(front-now) * time.Microsecond
	}

	if rd.delayedReadsTimer == nil {
		rd.delayedReadsTimer = time.AfterFunc(d, rd.onDelayedReadsTimer)
	} else {
		rd.delayedReadsTimer.Reset(d)
	}
}

func (rd *readIndexDelayer) onDelayedReadsTimer() {
	toSend := rd.fireDelayedReadIndexRequests(0)
	for _, next := range toSend {
		select {
		case <-rd.delayedReadsChanDone:
			return
		case rd.delayedReadsChannel <- next:
		}
	}
}

func (rd *readIndexDelayer) fireDelayedReadIndexRequests(commitIndex uint64) []pb.Message {
	// The raft goroutine and the timer goroutine both call this, so everything,
	// including the empty check, happens under the lock.
	rd.delayedReadsHandlerLock.Lock()
	defer rd.delayedReadsHandlerLock.Unlock()
	if len(rd.delayedReadIndexReqs) == 0 {
		return nil
	}

	var workingSet []pb.Message
	now := nowMicros()
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
	rd.rearmDelayedReadIndexTimer()
	return workingSet
}

// heldCount returns how many reads are currently held. Takes the lock, since
// the timer goroutine may be changing the queue.
func (rd *readIndexDelayer) heldCount() int {
	rd.delayedReadsHandlerLock.Lock()
	defer rd.delayedReadsHandlerLock.Unlock()
	return len(rd.delayedReadIndexReqs)
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
