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

type atomicHistoryClass struct {
	inline   [atomicInlineFrontier]atomicHistoryEntry
	inlineN  uint8
	overflow *atomicHistoryNode
}

func (c *atomicHistoryClass) find(tid uint32) (*atomicHistoryEntry, bool) {
	for i := uint8(0); i < c.inlineN; i++ {
		if c.inline[i].tid == tid {
			return &c.inline[i], true
		}
	}
	for n := c.overflow; n != nil; n = n.next {
		if n.entry.tid == tid {
			return &n.entry, true
		}
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
		return e
	}
	n := a.allocHistoryNode()
	n.entry.tid = tid
	n.next = c.overflow
	c.overflow = n
	return &n.entry
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
	}
	link := &c.overflow
	for *link != nil {
		n := *link
		if !atomicAccessEmpty(n.entry.access) {
			link = &n.next
			continue
		}
		*link = n.next
		a.freeHistoryNode(n)
	}
}

func (c *atomicHistoryClass) clear(a *AtomicHistoryArena) {
	for i := range c.inline {
		c.inline[i] = atomicHistoryEntry{}
	}
	c.inlineN = 0
	head := c.overflow
	c.overflow = nil
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
	if s.transactionActive || s.writerRevision.Load()&1 != 0 {
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
	a.freeRangeList(r.runs)
	a.freeRangeList(r.retired)
	a.mu.lock()
	*r = atomicRelease{arena: a, freeNext: a.freeRelease}
	a.freeRelease = r
	a.stats.Releases--
	a.mu.unlock()
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
