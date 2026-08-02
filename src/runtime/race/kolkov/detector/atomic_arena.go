package detector

import iatomic "internal/runtime/atomic"

// The atomic arena is detector-owned typed Go storage.  In particular, none of
// the objects reachable from a shadow sidecar is backed by a stack address or
// by untyped/C storage.  Slabs are never moved and remain rooted by the arena;
// handles are diagnostic identities, while normal access uses GC-visible typed
// pointers after the shadow lifecycle/capability protocol has validated them.

const (
	atomicStateSlabSize    = 64
	atomicHistorySlabSize  = 256
	atomicReleaseSlabSize  = 64
	atomicRangeSlabSize    = 256
	atomicFrontierSlabSize = 64
	atomicInlineFrontier   = 4
)

type atomicArenaHandle struct {
	generation uint32
	index      uint32
}

type atomicHistoryEntry struct {
	tid    uint32
	access atomicAccess
}

type atomicHistoryNode struct {
	next  *atomicHistoryNode
	entry atomicHistoryEntry
}

// atomicHistoryIndex is a four-byte sparse radix over the complete uint32 TID
// space. Only overflow nodes are indexed: their slab addresses are stable,
// while deleting an inline entry may move the last of the four inline values.
// Empty paths are unlinked on deletion, so index memory is bounded by the TIDs
// currently represented in overflow rather than by all TIDs ever observed.
type atomicHistoryIndex struct {
	root *atomicHistoryIndex1
}

type atomicHistoryIndex1 struct {
	child [256]*atomicHistoryIndex2
	used  uint16
}

type atomicHistoryIndex2 struct {
	child [256]*atomicHistoryIndex3
	used  uint16
}

type atomicHistoryIndex3 struct {
	child [256]*atomicHistoryIndexLeaf
	used  uint16
}

type atomicHistoryIndexLeaf struct {
	node [256]*atomicHistoryNode
	used uint16
}

func (x *atomicHistoryIndex) find(tid uint32) *atomicHistoryNode {
	p1 := x.root
	if p1 == nil {
		return nil
	}
	p2 := p1.child[byte(tid>>24)]
	if p2 == nil {
		return nil
	}
	p3 := p2.child[byte(tid>>16)]
	if p3 == nil {
		return nil
	}
	leaf := p3.child[byte(tid>>8)]
	if leaf == nil {
		return nil
	}
	return leaf.node[byte(tid)]
}

func (x *atomicHistoryIndex) insert(tid uint32, node *atomicHistoryNode) {
	if x.root == nil {
		x.root = new(atomicHistoryIndex1)
	}
	p1 := x.root
	i1 := byte(tid >> 24)
	p2 := p1.child[i1]
	if p2 == nil {
		p2 = new(atomicHistoryIndex2)
		p1.child[i1] = p2
		p1.used++
	}
	i2 := byte(tid >> 16)
	p3 := p2.child[i2]
	if p3 == nil {
		p3 = new(atomicHistoryIndex3)
		p2.child[i2] = p3
		p2.used++
	}
	i3 := byte(tid >> 8)
	leaf := p3.child[i3]
	if leaf == nil {
		leaf = new(atomicHistoryIndexLeaf)
		p3.child[i3] = leaf
		p3.used++
	}
	i4 := byte(tid)
	if leaf.node[i4] != nil {
		atomicRuntimeThrow("race detector duplicate atomic-history index entry")
	}
	leaf.node[i4] = node
	leaf.used++
}

// remove clears the leaf before its history node may be unlinked or recycled.
// Checking the exact pointer makes a stale delete incapable of removing a
// different TID generation after arena-node reuse.
func (x *atomicHistoryIndex) remove(tid uint32, node *atomicHistoryNode) {
	p1 := x.root
	if p1 == nil {
		atomicRuntimeThrow("race detector missing atomic-history index root")
	}
	i1 := byte(tid >> 24)
	p2 := p1.child[i1]
	if p2 == nil {
		atomicRuntimeThrow("race detector missing atomic-history index branch")
	}
	i2 := byte(tid >> 16)
	p3 := p2.child[i2]
	if p3 == nil {
		atomicRuntimeThrow("race detector missing atomic-history index branch")
	}
	i3 := byte(tid >> 8)
	leaf := p3.child[i3]
	if leaf == nil || leaf.node[byte(tid)] != node {
		atomicRuntimeThrow("race detector stale atomic-history index deletion")
	}
	leaf.node[byte(tid)] = nil
	leaf.used--
	if leaf.used != 0 {
		return
	}
	p3.child[i3] = nil
	p3.used--
	if p3.used != 0 {
		return
	}
	p2.child[i2] = nil
	p2.used--
	if p2.used != 0 {
		return
	}
	p1.child[i1] = nil
	p1.used--
	if p1.used == 0 {
		x.root = nil
	}
}

type atomicHistoryClass struct {
	inline            [atomicInlineFrontier]atomicHistoryEntry
	inlineN           uint8
	overflow          *atomicHistoryNode
	index             atomicHistoryIndex
	live              uint32
	insertedSinceScan uint32
	lastScanLive      uint32
}

func (c *atomicHistoryClass) find(tid uint32) (*atomicHistoryEntry, bool) {
	for i := uint8(0); i < c.inlineN; i++ {
		if c.inline[i].tid == tid {
			return &c.inline[i], true
		}
	}
	if n := c.index.find(tid); n != nil {
		return &n.entry, true
	}
	return nil, false
}

func (c *atomicHistoryClass) insert(a *AtomicHistoryArena, tid uint32) *atomicHistoryEntry {
	if e, ok := c.find(tid); ok {
		return e
	}
	if c.inlineN < atomicInlineFrontier {
		e := &c.inline[c.inlineN]
		c.inlineN++
		e.tid = tid
		c.live++
		c.insertedSinceScan++
		return e
	}
	n := a.allocHistoryNode()
	n.entry.tid = tid
	n.next = c.overflow
	c.overflow = n
	c.index.insert(tid, n)
	c.live++
	c.insertedSinceScan++
	return &n.entry
}

const atomicHistoryEagerPruneThreshold = 16

// shouldPruneBeforeInsert implements geometric lazy compaction. Small ordered
// chains remain compact eagerly. Once the frontier is large, a full scan runs
// only after roughly the size observed by the preceding scan has arrived as
// new unique TIDs, making construction of a concurrent antichain O(G).
func (c *atomicHistoryClass) shouldPruneBeforeInsert() bool {
	if c.live <= atomicHistoryEagerPruneThreshold || c.lastScanLive == 0 {
		return true
	}
	return c.insertedSinceScan >= c.lastScanLive-1
}

func (c *atomicHistoryClass) finishScan() {
	c.insertedSinceScan = 0
	c.lastScanLive = c.live
}

func (c *atomicHistoryClass) visit(fn func(*atomicHistoryEntry) bool) {
	for i := uint8(0); i < c.inlineN; i++ {
		if !fn(&c.inline[i]) {
			return
		}
	}
	for n := c.overflow; n != nil; n = n.next {
		if !fn(&n.entry) {
			return
		}
	}
}

func (c *atomicHistoryClass) removeEmpty(a *AtomicHistoryArena) {
	for i := uint8(0); i < c.inlineN; {
		if !atomicAccessEmpty(c.inline[i].access) {
			i++
			continue
		}
		c.inlineN--
		c.inline[i] = c.inline[c.inlineN]
		c.inline[c.inlineN] = atomicHistoryEntry{}
		c.live--
	}
	link := &c.overflow
	for *link != nil {
		n := *link
		if !atomicAccessEmpty(n.entry.access) {
			link = &n.next
			continue
		}
		c.index.remove(n.entry.tid, n)
		*link = n.next
		c.live--
		a.freeHistoryNode(n)
	}
	c.finishScan()
}

func (c *atomicHistoryClass) clear(a *AtomicHistoryArena) {
	for i := range c.inline {
		c.inline[i] = atomicHistoryEntry{}
	}
	c.inlineN = 0
	head := c.overflow
	c.overflow = nil
	// Drop the complete index before any indexed node enters the arena free
	// list. A recycled slab address can therefore never be found through the
	// previous class or state generation.
	c.index = atomicHistoryIndex{}
	c.live = 0
	c.insertedSinceScan = 0
	c.lastScanLive = 0
	for head != nil {
		next := head.next
		a.freeHistoryNode(head)
		head = next
	}
}

type atomicReleaseRange struct {
	next        *atomicReleaseRange
	first, last uint32
	clock       uint32
}

type atomicStateSlab struct {
	nodes [atomicStateSlabSize]atomicState
	used  uint32
}

type atomicHistorySlab struct {
	nodes [atomicHistorySlabSize]atomicHistoryNode
	used  uint32
}

type atomicReleaseSlab struct {
	nodes [atomicReleaseSlabSize]atomicRelease
	used  uint32
}

type atomicRangeSlab struct {
	nodes [atomicRangeSlabSize]atomicReleaseRange
	used  uint32
}

type atomicFrontierSlab struct {
	nodes [atomicFrontierSlabSize]atomicReadFrontier
	used  uint32
}

// AtomicHistoryArenaStats is quiescent accounting used by tests and runtime
// diagnostics. Live counts are semantic objects, not slab capacity.
type AtomicHistoryArenaStats struct {
	Generation    uint32
	States        uint64
	PeakStates    uint64
	History       uint64
	PeakHistory   uint64
	Releases      uint64
	PeakReleases  uint64
	Ranges        uint64
	PeakRanges    uint64
	Frontiers     uint64
	PeakFrontiers uint64
	Pinned        uint64
	PeakPinned    uint64
	Refills       uint64
	Exhausted     uint64
}

// AtomicHistoryArena owns every atomic sidecar object for one Detector.
// Refill allocates a fixed, fully typed Go slab, which makes all pointer fields
// visible to the garbage collector during runtime callbacks and stack growth.
type AtomicHistoryArena struct {
	mu spinlock

	generation uint32
	nextState  uint32

	states     []*atomicStateSlab
	histories  []*atomicHistorySlab
	releases   []*atomicReleaseSlab
	ranges     []*atomicRangeSlab
	frontiers  []*atomicFrontierSlab
	stateAt    int
	historyAt  int
	releaseAt  int
	rangeAt    int
	frontierAt int

	freeState    *atomicState
	freeHistory  *atomicHistoryNode
	freeRelease  *atomicRelease
	freeRange    *atomicReleaseRange
	freeFrontier *atomicReadFrontier

	stats      AtomicHistoryArenaStats
	pinned     iatomic.Uint64
	peakPinned iatomic.Uint64
}

func newAtomicHistoryArena() *AtomicHistoryArena {
	a := &AtomicHistoryArena{generation: 1}
	a.stats.Generation = 1
	return a
}

func (a *AtomicHistoryArena) refillState() *atomicStateSlab {
	if uint64(len(a.states)) >= uint64(^uint32(0))/atomicStateSlabSize {
		a.exhausted("race detector exhausted atomic-state arena")
	}
	slab := new(atomicStateSlab)
	a.states = append(a.states, slab)
	a.stats.Refills++
	return slab
}

func (a *AtomicHistoryArena) newState(base uintptr) *atomicState {
	a.mu.lock()
	defer a.mu.unlock()
	var s *atomicState
	if a.freeState != nil {
		s = a.freeState
		a.freeState = s.freeNext
	} else {
		for a.stateAt < len(a.states) && a.states[a.stateAt].used == atomicStateSlabSize {
			a.stateAt++
		}
		if a.stateAt == len(a.states) {
			a.refillState()
		}
		slab := a.states[a.stateAt]
		s = &slab.nodes[slab.used]
		slab.used++
	}
	if a.nextState == ^uint32(0) {
		a.exhausted("race detector exhausted atomic-state handles")
	}
	a.nextState++
	*s = atomicState{}
	s.arena = a
	s.handle = atomicArenaHandle{generation: a.generation, index: a.nextState}
	s.base = base
	s.initHistories()
	a.stats.States++
	if a.stats.States > a.stats.PeakStates {
		a.stats.PeakStates = a.stats.States
	}
	return s
}

func (a *AtomicHistoryArena) retainState(s *atomicState) {
	a.mu.lock()
	if s == nil || s.arena != a || s.handle.generation != a.generation || s.handle.index == 0 {
		a.mu.unlock()
		atomicRuntimeThrow("race detector retained stale atomic arena state")
	}
	s.owners++
	a.mu.unlock()
}

func (a *AtomicHistoryArena) releaseState(s *atomicState) {
	if s == nil {
		return
	}
	a.mu.lock()
	if s.arena != a || s.handle.generation != a.generation || s.handle.index == 0 || s.owners == 0 {
		a.mu.unlock()
		atomicRuntimeThrow("race detector atomic arena state ownership imbalance")
	}
	s.owners--
	last := s.owners == 0
	a.mu.unlock()
	if !last {
		return
	}

	// The final binding is released only after its fast capability and the
	// owning VarState access transaction have drained, so no callback can still
	// reach these exact witnesses. Invalidate frontier generations before the
	// state slot becomes reusable; dormant context cache entries then reject the
	// pointer without locking a recycled state.
	s.mu.lock()
	s.rmwQueue.lock()
	invalidRMW := s.rmwOwner || s.rmwGranted || s.rmwWaiters != 0 || s.rmwSema != 0 || iatomic.Load(&s.rmwSpinnerEpoch) != 0 ||
		s.rmwSpinnerTID != 0 || s.rmwSpinnerMisses != 0 || s.rmwCohortOps != 0 || s.rmwForceSpinner ||
		s.rmwPromoteParked || s.rmwPromotedOwner || s.rmwWakeCompetitors != 0 || s.rmwGrantTID != 0
	s.rmwQueue.unlock()
	if s.transactionActive || s.writerRevision.Load()&1 != 0 || invalidRMW {
		s.mu.unlock()
		atomicRuntimeThrow("race detector retired active atomic arena state")
	}
	s.reads.user.clear(a)
	s.reads.internal.clear(a)
	s.writes.user.clear(a)
	s.writes.internal.clear(a)
	s.plainReads.user.clear(a)
	s.plainReads.internal.clear(a)
	s.plainWrites.user.clear(a)
	s.plainWrites.internal.clear(a)
	for node := s.readFrontiers; node != nil; {
		next := node.next
		a.freeFrontierNode(node)
		node = next
	}
	s.readFrontiers = nil
	for lane := range s.releases {
		r := s.releases[lane]
		if r == nil {
			continue
		}
		s.releases[lane] = nil
		if r.refs == 0 {
			atomicRuntimeThrow("race detector atomic release ownership imbalance")
		}
		r.refs--
		if r.refs == 0 {
			a.freeReleaseObject(r)
		}
	}
	s.mu.unlock()

	a.mu.lock()
	*s = atomicState{arena: a, freeNext: a.freeState}
	a.freeState = s
	a.stats.States--
	a.mu.unlock()
}

func (a *AtomicHistoryArena) allocHistoryNode() *atomicHistoryNode {
	a.mu.lock()
	defer a.mu.unlock()
	var n *atomicHistoryNode
	if a.freeHistory != nil {
		n = a.freeHistory
		a.freeHistory = n.next
	} else {
		for a.historyAt < len(a.histories) && a.histories[a.historyAt].used == atomicHistorySlabSize {
			a.historyAt++
		}
		var slab *atomicHistorySlab
		if a.historyAt == len(a.histories) {
			if uint64(len(a.histories)) >= uint64(^uint32(0))/atomicHistorySlabSize {
				a.exhausted("race detector exhausted atomic-history arena")
			}
			slab = new(atomicHistorySlab)
			a.histories = append(a.histories, slab)
			a.stats.Refills++
		} else {
			slab = a.histories[a.historyAt]
		}
		n = &slab.nodes[slab.used]
		slab.used++
	}
	*n = atomicHistoryNode{}
	a.stats.History++
	if a.stats.History > a.stats.PeakHistory {
		a.stats.PeakHistory = a.stats.History
	}
	return n
}

func (a *AtomicHistoryArena) freeHistoryNode(n *atomicHistoryNode) {
	if n == nil {
		return
	}
	a.mu.lock()
	*n = atomicHistoryNode{next: a.freeHistory}
	a.freeHistory = n
	a.stats.History--
	a.mu.unlock()
}

func (a *AtomicHistoryArena) allocRelease() *atomicRelease {
	a.mu.lock()
	defer a.mu.unlock()
	var r *atomicRelease
	if a.freeRelease != nil {
		r = a.freeRelease
		a.freeRelease = r.freeNext
	} else {
		for a.releaseAt < len(a.releases) && a.releases[a.releaseAt].used == atomicReleaseSlabSize {
			a.releaseAt++
		}
		var slab *atomicReleaseSlab
		if a.releaseAt == len(a.releases) {
			if uint64(len(a.releases)) >= uint64(^uint32(0))/atomicReleaseSlabSize {
				a.exhausted("race detector exhausted atomic-release arena")
			}
			slab = new(atomicReleaseSlab)
			a.releases = append(a.releases, slab)
			a.stats.Refills++
		} else {
			slab = a.releases[a.releaseAt]
		}
		r = &slab.nodes[slab.used]
		slab.used++
	}
	*r = atomicRelease{arena: a}
	a.stats.Releases++
	if a.stats.Releases > a.stats.PeakReleases {
		a.stats.PeakReleases = a.stats.Releases
	}
	return r
}

func (a *AtomicHistoryArena) freeReleaseObject(r *atomicRelease) {
	if r == nil {
		return
	}
	r.releaseCausalResources()
	a.freeRangeList(r.runs)
	a.freeRangeList(r.retired)
	a.mu.lock()
	*r = atomicRelease{arena: a, freeNext: a.freeRelease}
	a.freeRelease = r
	a.stats.Releases--
	a.mu.unlock()
}

// releaseCausalResources drops references owned by one release without
// touching arena accounting. It is also used by quiescent reset while a.mu is
// held, where calling freeReleaseObject would deadlock.
func (r *atomicRelease) releaseCausalResources() {
	if r == nil {
		return
	}
	clearAtomicReleaseImports(r)
	clearAtomicReleaseDeferred(r)
	r.view.Release()
	if r.lineage != nil {
		r.lineage.Release()
		r.lineage = nil
	}
}

func (a *AtomicHistoryArena) allocRange() *atomicReleaseRange {
	a.mu.lock()
	defer a.mu.unlock()
	var n *atomicReleaseRange
	if a.freeRange != nil {
		n = a.freeRange
		a.freeRange = n.next
	} else {
		for a.rangeAt < len(a.ranges) && a.ranges[a.rangeAt].used == atomicRangeSlabSize {
			a.rangeAt++
		}
		var slab *atomicRangeSlab
		if a.rangeAt == len(a.ranges) {
			if uint64(len(a.ranges)) >= uint64(^uint32(0))/atomicRangeSlabSize {
				a.exhausted("race detector exhausted atomic-release range arena")
			}
			slab = new(atomicRangeSlab)
			a.ranges = append(a.ranges, slab)
			a.stats.Refills++
		} else {
			slab = a.ranges[a.rangeAt]
		}
		n = &slab.nodes[slab.used]
		slab.used++
	}
	*n = atomicReleaseRange{}
	a.stats.Ranges++
	if a.stats.Ranges > a.stats.PeakRanges {
		a.stats.PeakRanges = a.stats.Ranges
	}
	return n
}

func (a *AtomicHistoryArena) freeRangeList(head *atomicReleaseRange) {
	if head == nil {
		return
	}
	a.mu.lock()
	var count uint64
	tail := head
	for {
		count++
		if tail.next == nil {
			break
		}
		tail = tail.next
	}
	tail.next = a.freeRange
	a.freeRange = head
	a.stats.Ranges -= count
	a.mu.unlock()
}

func (a *AtomicHistoryArena) allocFrontier() *atomicReadFrontier {
	a.mu.lock()
	defer a.mu.unlock()
	var n *atomicReadFrontier
	if a.freeFrontier != nil {
		n = a.freeFrontier
		a.freeFrontier = n.freeNext
	} else {
		for a.frontierAt < len(a.frontiers) && a.frontiers[a.frontierAt].used == atomicFrontierSlabSize {
			a.frontierAt++
		}
		var slab *atomicFrontierSlab
		if a.frontierAt == len(a.frontiers) {
			if uint64(len(a.frontiers)) >= uint64(^uint32(0))/atomicFrontierSlabSize {
				a.exhausted("race detector exhausted atomic-frontier arena")
			}
			slab = new(atomicFrontierSlab)
			a.frontiers = append(a.frontiers, slab)
			a.stats.Refills++
		} else {
			slab = a.frontiers[a.frontierAt]
		}
		n = &slab.nodes[slab.used]
		slab.used++
	}
	oldGeneration := n.generation.Load()
	*n = atomicReadFrontier{}
	n.arena = a
	if oldGeneration == ^uint64(0) {
		a.exhausted("race detector atomic-frontier generation overflow")
	}
	n.generation.Store(oldGeneration + 1)
	a.stats.Frontiers++
	if a.stats.Frontiers > a.stats.PeakFrontiers {
		a.stats.PeakFrontiers = a.stats.Frontiers
	}
	return n
}

func (a *AtomicHistoryArena) freeFrontierNode(n *atomicReadFrontier) {
	if n == nil {
		return
	}
	a.mu.lock()
	generation := n.generation.Load()
	if generation == ^uint64(0) {
		a.mu.unlock()
		a.exhausted("race detector atomic-frontier generation overflow")
		return
	}
	n.generation.Store(generation + 1)
	n.mask.Store(0)
	n.clock.Store(0)
	n.next = nil
	n.freeNext = a.freeFrontier
	a.freeFrontier = n
	a.stats.Frontiers--
	a.mu.unlock()
}

func (a *AtomicHistoryArena) pin() {
	live := a.pinned.Add(1)
	for {
		peak := a.peakPinned.Load()
		if live <= peak || a.peakPinned.CompareAndSwap(peak, live) {
			return
		}
	}
}
func (a *AtomicHistoryArena) unpin() {
	for {
		live := a.pinned.Load()
		if live == 0 {
			atomicRuntimeThrow("race detector atomic arena pin imbalance")
		}
		if a.pinned.CompareAndSwap(live, live-1) {
			return
		}
	}
}

func (a *AtomicHistoryArena) exhausted(message string) {
	a.stats.Exhausted++
	atomicRuntimeThrow(message)
}

func (a *AtomicHistoryArena) Stats() AtomicHistoryArenaStats {
	a.mu.lock()
	s := a.stats
	a.mu.unlock()
	s.Pinned = a.pinned.Load()
	s.PeakPinned = a.peakPinned.Load()
	return s
}

// reset is quiescent. Slab addresses remain stable and stale handles are made
// invalid before any slot may be reused.
func (a *AtomicHistoryArena) reset() {
	a.mu.lock()
	if a.pinned.Load() != 0 {
		a.mu.unlock()
		atomicRuntimeThrow("race detector reset with pinned atomic transactions")
	}
	if a.generation == ^uint32(0) {
		a.mu.unlock()
		atomicRuntimeThrow("race detector atomic arena generation overflow")
	}
	a.generation++
	a.nextState = 0
	a.stateAt, a.historyAt, a.releaseAt, a.rangeAt, a.frontierAt = 0, 0, 0, 0, 0
	a.freeState, a.freeHistory, a.freeRelease, a.freeRange, a.freeFrontier = nil, nil, nil, nil, nil
	for _, s := range a.states {
		*s = atomicStateSlab{}
	}
	for _, s := range a.histories {
		*s = atomicHistorySlab{}
	}
	for _, s := range a.releases {
		for i := uint32(0); i < s.used; i++ {
			s.nodes[i].releaseCausalResources()
		}
		*s = atomicReleaseSlab{}
	}
	for _, s := range a.ranges {
		*s = atomicRangeSlab{}
	}
	// Frontier generations intentionally survive reset, preventing a context
	// cache from accepting a node reused at the same physical address.
	for _, s := range a.frontiers {
		for i := range s.nodes {
			g := s.nodes[i].generation.Load()
			s.nodes[i] = atomicReadFrontier{}
			s.nodes[i].generation.Store(g)
		}
		s.used = 0
	}
	oldStats := a.stats
	a.stats = AtomicHistoryArenaStats{
		Generation: oldStats.Generation + 1,
		PeakStates: oldStats.PeakStates, PeakHistory: oldStats.PeakHistory,
		PeakReleases: oldStats.PeakReleases, PeakRanges: oldStats.PeakRanges,
		PeakFrontiers: oldStats.PeakFrontiers, Refills: oldStats.Refills,
		Exhausted: oldStats.Exhausted,
	}
	a.mu.unlock()
}
