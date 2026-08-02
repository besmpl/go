package shadowmem

import (
	"internal/runtime/atomic"
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

// promotedReadNode is one stable logical-reader coordinate. Nodes are never
// reused or removed while their frontier is reachable: a cached capability may
// retain one indefinitely. seq protects the epoch/PC pair; odd means that a
// reader is publishing. A closed frontier may skip an odd node because that
// reader's final close check forces it through the canonical transaction.
type promotedReadNode struct {
	seq   atomic.Uint64
	epoch atomic.Uint64
	pc    atomic.Uintptr
	tid   uint32
	next  *promotedReadNode
}

const promotedReadRegistryShards = 64

type promotedReadRegistryShard struct {
	mu    spinlock
	table []*promotedReadNode
	used  uintptr
}

func (shard *promotedReadRegistryShard) find(tid uint32) *promotedReadNode {
	if len(shard.table) == 0 {
		return nil
	}
	mask := uintptr(len(shard.table) - 1)
	index := promotedReadTIDHash(tid) & mask
	for {
		node := shard.table[index]
		if node == nil {
			return nil
		}
		if node.tid == tid {
			return node
		}
		index = (index + 1) & mask
	}
}

func (shard *promotedReadRegistryShard) insert(node *promotedReadNode) {
	if len(shard.table) == 0 || (shard.used+1)*4 > uintptr(len(shard.table))*3 {
		shard.grow()
	}
	shard.insertInto(shard.table, node)
	shard.used++
}

func (shard *promotedReadRegistryShard) grow() {
	size := len(shard.table) * 2
	if size == 0 {
		size = 8
	} else if size < len(shard.table) {
		runtimeThrow("race detector exhausted promoted-reader registry")
	}
	table := make([]*promotedReadNode, size)
	for _, node := range shard.table {
		if node != nil {
			shard.insertInto(table, node)
		}
	}
	shard.table = table
}

func (shard *promotedReadRegistryShard) insertInto(table []*promotedReadNode, node *promotedReadNode) {
	mask := uintptr(len(table) - 1)
	index := promotedReadTIDHash(node.tid) & mask
	for table[index] != nil {
		index = (index + 1) & mask
	}
	table[index] = node
}

func promotedReadTIDHash(tid uint32) uintptr {
	// A full avalanche is important because shard selection already consumes
	// the low six TID bits; consecutive TIDs within one shard differ by 64.
	value := tid
	value ^= value >> 16
	value *= 0x7feb352d
	value ^= value >> 15
	value *= 0x846ca68b
	value ^= value >> 16
	return uintptr(value)
}

// promotedReadRegistry is allocated only after the four-reader inline
// registry fills. Each shard is then allocated only for an observed TID and
// grows as a compact open-addressed table, avoiding sparse radix pages without
// making repeated enrollment lookup linear in the reader population.
type promotedReadRegistry struct {
	shards [promotedReadRegistryShards]atomic.Pointer[promotedReadRegistryShard]
}

// promotedReadFrontier owns the exact unbounded promoted-reader registry.
// revision is zero/even while lock-free publication is admitted and becomes
// odd permanently before an exclusive mutation. Closed frontiers are never
// reopened or repurposed, preventing cached-capability ABA.
type promotedReadFrontier struct {
	revision   atomic.Uint64
	inlineMu   spinlock
	head       atomic.Pointer[promotedReadNode]
	inline     [4]*promotedReadNode
	inlineN    uint8
	inlineFull atomic.Uint32
	registry   atomic.Pointer[promotedReadRegistry]
	legacy     *vectorclock.VectorClock
	lifecycle  uint64
}

func newPromotedReadFrontier(clock *vectorclock.VectorClock, lifecycle uint64) *promotedReadFrontier {
	return &promotedReadFrontier{legacy: clock, lifecycle: lifecycle}
}

// close excludes future lock-free publications. It deliberately does not wait
// for an in-flight odd node: the closer skips that node and the publisher's
// final revision check fails, so the read is represented canonically instead.
func (f *promotedReadFrontier) close() {
	if f == nil {
		return
	}
	for {
		revision := f.revision.Load()
		if revision&1 != 0 {
			return
		}
		if revision == ^uint64(0) {
			runtimeThrow("race detector exhausted promoted-reader revisions")
		}
		if f.revision.CompareAndSwap(revision, revision+1) {
			return
		}
	}
}

func (f *promotedReadFrontier) nodeFor(tid uint32) *promotedReadNode {
	if f == nil || f.revision.Load()&1 != 0 {
		return nil
	}
	// Keep the small-reader case on one compact inline registry. Once full,
	// inline never changes again and inlineFull publishes that immutable view.
	if f.inlineFull.Load() == 0 {
		f.inlineMu.lock()
		if f.revision.Load()&1 != 0 {
			f.inlineMu.unlock()
			return nil
		}
		for i := uint8(0); i < f.inlineN; i++ {
			if f.inline[i].tid == tid {
				f.inlineMu.unlock()
				return f.inline[i]
			}
		}
		if f.inlineN < uint8(len(f.inline)) {
			node := &promotedReadNode{tid: tid}
			f.publishNode(node)
			f.inline[f.inlineN] = node
			f.inlineN++
			if f.inlineN == uint8(len(f.inline)) {
				f.inlineFull.Store(1)
			}
			f.inlineMu.unlock()
			return node
		}
		f.inlineFull.Store(1)
		f.inlineMu.unlock()
	}

	for _, node := range f.inline {
		if node.tid == tid {
			return node
		}
	}

	registry := f.registry.Load()
	if registry == nil {
		// Reuse the now-cold inline lock so a first overflow stampede allocates
		// exactly one 64-pointer directory rather than one losing CAS candidate
		// per concurrent TID.
		f.inlineMu.lock()
		registry = f.registry.Load()
		if registry == nil {
			registry = new(promotedReadRegistry)
			f.registry.Store(registry)
		}
		f.inlineMu.unlock()
	}
	shardIndex := tid & (promotedReadRegistryShards - 1)
	shard := registry.shards[shardIndex].Load()
	if shard == nil {
		candidate := new(promotedReadRegistryShard)
		if registry.shards[shardIndex].CompareAndSwap(nil, candidate) {
			shard = candidate
		} else {
			shard = registry.shards[shardIndex].Load()
		}
	}
	shard.mu.lock()
	defer shard.mu.unlock()
	if f.revision.Load()&1 != 0 {
		return nil
	}
	if node := shard.find(tid); node != nil {
		return node
	}
	node := &promotedReadNode{tid: tid}
	f.publishNode(node)
	shard.insert(node)
	return node
}

func (f *promotedReadFrontier) publishNode(node *promotedReadNode) {
	for {
		head := f.head.Load()
		node.next = head
		if f.head.CompareAndSwap(head, node) {
			return
		}
	}
}

// stableEpochPC returns a paired node publication. False means an in-flight
// publisher; after close that publisher must retry canonically and is therefore
// intentionally absent from this snapshot.
func (node *promotedReadNode) stableEpochPC() (epoch.Epoch, uintptr, bool) {
	first := node.seq.Load()
	if first&1 != 0 {
		return 0, 0, false
	}
	e := epoch.Epoch(node.epoch.Load())
	pc := node.pc.Load()
	return e, pc, node.seq.Load() == first
}

func (f *promotedReadFrontier) refreshLegacyLocked() {
	if f == nil || f.legacy == nil {
		return
	}
	for node := f.head.Load(); node != nil; node = node.next {
		e, _, ok := node.stableEpochPC()
		if !ok || e == 0 {
			continue
		}
		tid, clock := e.Decode()
		// Nodes are immutable per TID. Treat a mismatched packed epoch as a
		// conservative omission; publication helpers never create one.
		if tid == node.tid && uint32(clock) > f.legacy.Get(tid) {
			f.legacy.Set(tid, uint32(clock))
		}
	}
}

// PromotedReadCapability is an immutable exact-address certificate cached by
// one RaceContext. It roots the complete old generation, so allocator reuse can
// only make validation fail; it can never retarget this object.
type PromotedReadCapability struct {
	frontier     *promotedReadFrontier
	node         *promotedReadNode
	state        *VarState
	slot         *ShadowSlot
	addr         uintptr
	lifecycle    uint64
	slotRevision uint64
	frontierRev  uint64
	mask         uint8
	width        uint8
}

// State returns the authoritative ordinary state for a successful capability.
//
//go:nosplit
func (cap *PromotedReadCapability) State() *VarState {
	if cap == nil {
		return nil
	}
	return cap.state
}

func (cap *PromotedReadCapability) mappingValid(addr, size uintptr) bool {
	if cap == nil || cap.addr != addr || uintptr(cap.width) != size || cap.mask == 0 ||
		cap.frontier == nil || cap.node == nil || cap.state == nil || cap.slot == nil ||
		cap.frontierRev&1 != 0 || cap.frontier.revision.Load() != cap.frontierRev ||
		cap.frontier.lifecycle != cap.lifecycle ||
		cap.state.atomicState.Load() != nil {
		return false
	}
	// slotSpinlock.state is only a transient ownership bit, not a mapping
	// generation. Every operation which can redirect or clear an existing lane
	// closes that state's frontier before its first pointer store. The complete
	// lane scan bracketed by frontier revision loads is therefore the exact
	// mapping certificate and remains valid while unrelated canonical readers
	// merely hold the slot lock.
	for lane := uint8(0); lane < shadowSlotLanes; lane++ {
		mapped := cap.slot.states[lane].Load() == cap.state
		if mapped != (cap.mask&(uint8(1)<<lane) != 0) {
			return false
		}
	}
	return cap.frontier.revision.Load() == cap.frontierRev
}

// TryRead publishes current without taking ShadowSlot, accessMu, or VarState.mu.
// Failure is mutation-free from the detector's semantic perspective: an odd
// node which later fails revalidation is invisible to a closer and is followed
// by the caller's canonical retry.
func (cap *PromotedReadCapability) TryRead(addr, size uintptr, current epoch.Epoch, clock *vectorclock.VectorClock, pc uintptr) bool {
	if current == 0 || clock == nil || pc == 0 || !cap.mappingValid(addr, size) {
		return false
	}
	tid, _ := current.Decode()
	if tid != cap.node.tid {
		return false
	}
	write := epoch.Epoch(cap.state.W.Load())
	if write != 0 && !write.HappensBefore(clock) {
		return false
	}
	for {
		seq := cap.node.seq.Load()
		if seq&1 != 0 {
			return false
		}
		if seq >= ^uint64(0)-2 {
			runtimeThrow("race detector exhausted promoted-reader sequence")
		}
		if !cap.node.seq.CompareAndSwap(seq, seq+1) {
			return false
		}
		if !cap.mappingValid(addr, size) || epoch.Epoch(cap.state.W.Load()) != write {
			cap.node.seq.Store(seq + 2)
			return false
		}
		cap.node.pc.Store(pc)
		cap.node.epoch.Store(uint64(current))
		cap.node.seq.Store(seq + 2)
		return cap.mappingValid(addr, size) && epoch.Epoch(cap.state.W.Load()) == write
	}
}
