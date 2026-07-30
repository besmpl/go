package detector

import (
	iatomic "internal/runtime/atomic"
	"unsafe"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
	"runtime/race/kolkov/vectorclock"
)

// AtomicTokenSlots bounds the widest supported atomic operation. The runtime
// bridge provides a GC-visible stack-resident [8]unsafe.Pointer.
const AtomicTokenSlots = 8

// atomicAccess retains the latest access to each exact lane by one detector
// thread. Per-lane history prevents a later 32-bit access from erasing an
// earlier access to the other half of a shared 64-bit history.
type atomicAccess struct {
	clocks [AtomicTokenSlots]uint32
	pcs    [AtomicTokenSlots]uintptr
}

// atomicHistory keeps report-equivalent access frontiers separate. Internal
// mutex atomic accesses may be suppressed only when paired with an exact
// sync.RWMutex marker read. Letting either implementation class replace a user
// access could therefore hide a real mixed atomic/plain race.
type atomicHistory struct {
	arena    *AtomicHistoryArena
	user     atomicHistoryClass
	internal atomicHistoryClass
}

const atomicPCClassCacheSlots = 64

type atomicPCClassCacheEntry struct {
	// Classification is encoded by slot membership, so concurrent colliding
	// writers cannot publish a PC from one classification with the class from
	// another. Collisions only evict a same-class cache entry and cause safe
	// reclassification on the next lookup.
	userPC     iatomic.Uintptr
	internalPC iatomic.Uintptr
}

var atomicPCClassCache [atomicPCClassCacheSlots]atomicPCClassCacheEntry
var plainPCClassCache [atomicPCClassCacheSlots]atomicPCClassCacheEntry

// atomicInternalMutexPC keeps symbolization off the steady atomic path. The two
// independently atomic class slots make every cache hit pair-consistent without
// a lock or allocation. Check user first as a conservative fail-open choice if
// corrupted test state ever places the same PC in both slots: reporting a
// spurious implementation conflict is preferable to suppressing a user race.
func atomicInternalMutexPC(pc uintptr) bool {
	if pc <= 0x10000 {
		return false
	}
	entry := &atomicPCClassCache[(pc>>4)&(atomicPCClassCacheSlots-1)]
	if entry.userPC.Load() == pc {
		return false
	}
	if entry.internalPC.Load() == pc {
		return true
	}
	internal := pcHasFunctionPrefix(pc, "internal/sync.(*Mutex).")
	if internal {
		entry.internalPC.Store(pc)
	} else {
		entry.userPC.Store(pc)
	}
	return internal
}

func rwMutexMarkerPC(pc uintptr) bool {
	if pc <= 0x10000 {
		return false
	}
	entry := &plainPCClassCache[(pc>>4)&(atomicPCClassCacheSlots-1)]
	if entry.userPC.Load() == pc {
		return false
	}
	if entry.internalPC.Load() == pc {
		return true
	}
	marker := pcHasFunctionPrefix(pc, "sync.(*RWMutex).")
	if marker {
		entry.internalPC.Store(pc)
	} else {
		entry.userPC.Store(pc)
	}
	return marker
}

func pcHasFunctionPrefix(pc uintptr, prefix string) bool {
	if pc <= 0x10000 {
		return false
	}
	return runtimePCFunctionHasPrefix(pc, prefix)
}

const atomicReleaseDeltaCapacity = 64

type atomicReleaseDelta struct {
	version uint64
	tid     uint32
	clock   uint32
}

var nextAtomicReleaseStream iatomic.Uint64

//go:linkname atomicRuntimeThrow runtime.throw
func atomicRuntimeThrow(s string)

func newAtomicReleaseStream() uint64 {
	for {
		old := nextAtomicReleaseStream.Load()
		if old == ^uint64(0) {
			atomicRuntimeThrow("race detector atomic-release stream overflow")
		}
		if nextAtomicReleaseStream.CompareAndSwap(old, old+1) {
			return old + 1
		}
	}
}

// atomicRelease retains an exact canonical snapshot plus a bounded suffix of
// monotonic point updates. Once refs reaches zero, atomicState may recycle the
// object and its buffers, but every reuse receives a new non-ABA stream.
type atomicRelease struct {
	arena    *AtomicHistoryArena
	runs     *atomicReleaseRange
	retired  *atomicReleaseRange
	deltas   [atomicReleaseDeltaCapacity]atomicReleaseDelta
	freeNext *atomicRelease
	stream   uint64
	version  uint64
	refs     uint8
	deltaN   uint8
	deltaAt  uint8
}

// atomicState is deliberately separate from VarState's ordinary FastTrack
// history. Atomic operations synchronize through release, but never race with
// other atomic operations. Copy-on-write ordinary groups may share this pointer:
// its own transaction lock protects all width-aware per-lane state.
type atomicState struct {
	arena    *AtomicHistoryArena
	handle   atomicArenaHandle
	owners   uint32
	freeNext *atomicState
	// mu remains held across the hardware access. Distinct histories involved
	// in one mixed-width operation are locked in stable pointer order.
	mu     spinlock
	base   uintptr
	reads  atomicHistory
	writes atomicHistory
	// plainReads retains exact scalar-reader attribution once an internal
	// mutex marker creates the overlay. VarState has only one aggregate read
	// PC, which is insufficient after concurrent RWMutex marker reads promote.
	plainReads atomicHistory
	// plainWrites is a per-lane pre-store poison frontier. Compiler ordinary
	// write hooks run before the machine store, so a concurrent atomic Store
	// must not publish a release which a later Load could acquire after the
	// ordinary store wins hardware modification order. A synchronizing atomic
	// write may discard only poison events which happen before its context.
	plainWrites atomicHistory

	// releases contains the latest modification snapshot for each exact lane.
	// All lanes written by one operation share one snapshot pointer.
	releases [AtomicTokenSlots]*atomicRelease

	// writerRevision is even only between complete atomic modifications. A
	// cached load may execute without mu only when it observes the same even
	// revision before and after its hardware load. Writers publish odd before
	// the hardware operation and the following even value only after history
	// and release publication are complete.
	writerRevision iatomic.Uint64
	readFrontiers  *atomicReadFrontier

	transactionAddr     uintptr
	transactionSize     uintptr
	transactionContext  *goroutine.RaceContext
	transactionSyncMode int8 // -1 accepts ignored/general completion; 0/1 is exact.
	transactionActive   bool
}

//go:nocheckptr
func retainAtomicArenaState(p unsafe.Pointer) {
	s := (*atomicState)(p)
	s.arena.retainState(s)
}

//go:nocheckptr
func releaseAtomicArenaState(p unsafe.Pointer) {
	s := (*atomicState)(p)
	s.arena.releaseState(s)
}

// atomicReadFrontier is one registered exact read witness. Identity fields are
// immutable while the node is registered. Fast loads update only clock; the
// owning RaceContext serializes its own updates, and ordinary access cannot
// scan the list until the retained AtomicFastPath capability has drained.
type atomicReadFrontier struct {
	next       *atomicReadFrontier
	freeNext   *atomicReadFrontier
	arena      *AtomicHistoryArena
	tid        uint32
	mask       iatomic.Uint32
	clock      iatomic.Uint32
	generation iatomic.Uint64
	pc         uintptr
	internal   bool
}

func (s *atomicState) initHistories() {
	s.reads.arena = s.arena
	s.writes.arena = s.arena
	s.plainReads.arena = s.arena
	s.plainWrites.arena = s.arena
}

func (s *atomicState) beginTransaction(addr, size uintptr, ctx *goroutine.RaceContext, syncMode int8) {
	if s.transactionActive {
		atomicRuntimeThrow("race detector atomic transaction already active")
	}
	s.transactionAddr, s.transactionSize = addr, size
	s.transactionContext, s.transactionSyncMode, s.transactionActive = ctx, syncMode, true
}

func (s *atomicState) validateTransaction(addr, size uintptr, ctx *goroutine.RaceContext, synchronize bool) {
	if !s.transactionActive || s.transactionAddr != addr || s.transactionSize != size || s.transactionContext != ctx ||
		(s.transactionSyncMode >= 0 && (s.transactionSyncMode == 1) != synchronize) {
		atomicRuntimeThrow("race detector invalid or reused atomic transaction token")
	}
}

func (s *atomicState) endTransaction() {
	if !s.transactionActive {
		atomicRuntimeThrow("race detector atomic transaction imbalance")
	}
	s.transactionAddr, s.transactionSize, s.transactionContext = 0, 0, nil
	s.transactionSyncMode, s.transactionActive = 0, false
}

func (s *atomicState) beginWriter() {
	revision := s.writerRevision.Load()
	if revision&1 != 0 || revision >= ^uint64(0)-1 {
		atomicRuntimeThrow("race detector atomic writer revision overflow")
	}
	s.writerRevision.Store(revision + 1)
}

func (s *atomicState) endWriter() {
	revision := s.writerRevision.Load()
	if revision&1 == 0 || revision == ^uint64(0) {
		atomicRuntimeThrow("race detector atomic writer revision imbalance")
	}
	s.writerRevision.Store(revision + 1)
}

func (s *atomicState) registerReadFrontier(ctx *goroutine.RaceContext, mask uint8, pc uintptr, internal bool, spare *atomicReadFrontier) *atomicReadFrontier {
	if spare != nil {
		for node := s.readFrontiers; node != nil; node = node.next {
			if node != spare {
				continue
			}
			if node.tid != ctx.TID || uint8(node.mask.Load()) != mask || node.pc != pc || node.internal != internal {
				atomicRuntimeThrow("race detector active atomic read-frontier ownership mismatch")
			}
			node.clock.Store(uint32(ctx.GetEpoch()))
			return node
		}
	}
	node := spare
	if node != nil {
		generation := node.generation.Load()
		if generation == ^uint64(0) {
			atomicRuntimeThrow("race detector atomic read-frontier generation overflow")
		}
		node.generation.Store(generation + 1)
	} else {
		node = s.arena.allocFrontier()
	}
	node.next = s.readFrontiers
	node.tid = ctx.TID
	node.pc = pc
	node.internal = internal
	node.mask.Store(uint32(mask))
	node.clock.Store(uint32(ctx.GetEpoch()))
	s.readFrontiers = node
	return node
}

func (s *atomicState) deactivateReadFrontier(node *atomicReadFrontier, generation uint64) *atomicReadFrontier {
	if node == nil || node.generation.Load() != generation {
		return nil
	}
	link := &s.readFrontiers
	for *link != nil {
		if *link == node {
			s.foldReadFrontier(node, uint8(node.mask.Load()))
			*link = node.next
			node.mask.Store(0)
			node.clock.Store(0)
			node.next = nil
			return node
		}
		link = &(*link).next
	}
	return nil
}

func (s *atomicState) foldReadFrontier(node *atomicReadFrontier, mask uint8) {
	clock := node.clock.Load()
	if mask == 0 || clock == 0 {
		return
	}
	frontier := &s.reads.user
	if node.internal {
		frontier = &s.reads.internal
	}
	access := &frontier.insert(s.arena, node.tid).access
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) != 0 && clock >= access.clocks[lane] {
			access.clocks[lane] = clock
			access.pcs[lane] = node.pc
		}
	}
}

func (s *atomicState) refreshReadFrontiers(mask uint8) {
	for node := s.readFrontiers; node != nil; node = node.next {
		nodeMask := uint8(node.mask.Load()) & mask
		s.foldReadFrontier(node, nodeMask)
	}
}

func (s *atomicState) clearReadFrontierMask(mask uint8) {
	link := &s.readFrontiers
	for *link != nil {
		node := *link
		for {
			old := node.mask.Load()
			if old&uint32(mask) == 0 || node.mask.CompareAndSwap(old, old&^uint32(mask)) {
				break
			}
		}
		if node.mask.Load() == 0 {
			*link = node.next
			node.clock.Store(0)
			node.next = nil
			continue
		}
		link = &node.next
	}
}

// pruneReadFrontiers removes HB-dominated affected lanes from the registered
// list. Clearing mask before unlinking makes a context cache which still roots
// the node miss safely; a concurrent cached load necessarily crosses this
// writer's revision and retries without publishing to the retired node.
func (s *atomicState) pruneReadFrontiers(ctx *goroutine.RaceContext, mask uint8) {
	link := &s.readFrontiers
	for *link != nil {
		node := *link
		oldMask := uint8(node.mask.Load())
		if oldMask&mask != 0 && node.clock.Load() <= ctx.C.Get(node.tid) {
			newMask := oldMask &^ mask
			node.mask.Store(uint32(newMask))
			if newMask == 0 {
				*link = node.next
				node.clock.Store(0)
				node.next = nil
				continue
			}
		}
		link = &node.next
	}
}

// AtomicToken retains access-locked ordinary equivalence groups across the
// hardware operation. Lanes in the same group contain the same pointer, so the
// token remains bounded and GC-visible without a per-operation allocation.
//
// AtomicToken is an opaque, single-use capability for the trusted runtime ABI.
// Once a Begin call returns a non-empty token, the caller must invoke exactly
// one matching End call with the same token object, address, size, and context;
// AtomicBeginPlain and AtomicEndMode must also use the same synchronize value,
// while AtomicBeginRMW must be completed with synchronize=true. The token must
// not be copied or reused concurrently. Violating this contract can leave
// detector locks retained or release a different transaction.
type AtomicToken [AtomicTokenSlots]unsafe.Pointer

// existingAtomicStateLocked decodes an overlay installed only by this package.
// The arena handle check below validates the decoded layout before it is used.
//
//go:nocheckptr
func existingAtomicStateLocked(vs *shadowmem.VarState) *atomicState {
	p := vs.GetAtomicState()
	if p == nil {
		return nil
	}
	s := (*atomicState)(p)
	if s.arena == nil || s.handle.generation != s.arena.generation || s.handle.index == 0 {
		atomicRuntimeThrow("race detector stale atomic arena handle")
	}
	return s
}

func (s *atomicState) releaseMembership(release *atomicRelease) uint8 {
	var membership uint8
	for lane, current := range s.releases {
		if current == release {
			membership |= uint8(1) << uint8(lane)
		}
	}
	return membership
}

func releaseHasForeignMetadata(release *atomicRelease, own uint32) bool {
	for run := release.runs; run != nil; run = run.next {
		if run.first != own || run.last != own {
			return true
		}
	}
	if release.retired != nil {
		// Retirement is +infinity causal metadata and is always part of the
		// foreign projection proof, including malformed own-TID retirement.
		return true
	}
	return false
}

func (release *atomicRelease) replay(ctx *goroutine.RaceContext, seenVersion uint64) (complete, foreign bool) {
	if seenVersion >= release.version || release.deltaN == 0 {
		return false, false
	}
	firstVersion := release.version - uint64(release.deltaN) + 1
	if seenVersion+1 < firstVersion {
		return false, false
	}
	start := (int(release.deltaAt) + atomicReleaseDeltaCapacity - int(release.deltaN)) % atomicReleaseDeltaCapacity
	expected := seenVersion + 1
	for i := 0; i < int(release.deltaN); i++ {
		delta := release.deltas[(start+i)%atomicReleaseDeltaCapacity]
		if delta.version < expected {
			continue
		}
		if delta.version != expected {
			return false, false
		}
		if delta.tid != ctx.TID {
			foreign = true
		}
		if delta.clock > ctx.C.Get(delta.tid) {
			ctx.C.Set(delta.tid, delta.clock)
		}
		expected++
	}
	return expected == release.version+1, foreign
}

// acquire imports each unique current release once. Exact current-version
// cache hits are O(1); older hits replay a complete bounded delta suffix, and
// every other case joins the exact canonical checkpoint.
func (s *atomicState) acquire(ctx *goroutine.RaceContext, mask uint8) {
	var seen [AtomicTokenSlots]*atomicRelease
	seenCount := 0
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) == 0 {
			continue
		}
		release := s.releases[lane]
		if release == nil {
			continue
		}
		duplicate := false
		for i := 0; i < seenCount; i++ {
			if seen[i] == release {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		seen[seenCount] = release
		seenCount++
		membership := s.releaseMembership(release)
		seenVersion, wasStrong, cacheHit := ctx.LookupAtomicRelease(unsafe.Pointer(release), release.stream, membership)
		if cacheHit && seenVersion == release.version {
			continue
		}

		freshOnlyOwn := ctx.FreshOnlyOwn()
		if cacheHit && seenVersion < release.version {
			if complete, foreign := release.replay(ctx, seenVersion); complete {
				if foreign {
					ctx.NoteForeignImport()
				}
				ctx.RecordAtomicRelease(unsafe.Pointer(release), release.stream, release.version, membership, wasStrong)
				continue
			}
		}

		var finite [32]vectorclock.FiniteRange
		n := 0
		for run := release.runs; run != nil; run = run.next {
			finite[n] = vectorclock.FiniteRange{First: run.first, Last: run.last, Clock: run.clock}
			n++
			if n == len(finite) {
				ctx.C.JoinCanonicalRanges(finite[:])
				n = 0
			}
		}
		if n != 0 {
			ctx.C.JoinCanonicalRanges(finite[:n])
		}
		// Retirement is +infinity causal metadata, not a finite MaxUint32
		// clock. Apply it after finite joins so it remains immutable and drops
		// any covered finite coordinates in the destination.
		var retired [32]vectorclock.RetiredRange
		n = 0
		for run := release.retired; run != nil; run = run.next {
			retired[n] = vectorclock.RetiredRange{First: run.first, Last: run.last}
			n++
			if n == len(retired) {
				ctx.C.RetireRanges(retired[:])
				n = 0
			}
		}
		if n != 0 {
			ctx.C.RetireRanges(retired[:n])
		}
		if releaseHasForeignMetadata(release, ctx.TID) {
			ctx.NoteForeignImport()
		}
		ctx.RecordAtomicRelease(unsafe.Pointer(release), release.stream, release.version, membership, wasStrong || freshOnlyOwn)
	}
}

// retireReleases removes the latest modification snapshot for every affected
// lane. It is used both before normal release publication and for writes while
// user synchronization is disabled: a later enabled load must not acquire a
// release that the ignored modification superseded.
func (s *atomicState) retireReleases(mask uint8) {
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) == 0 {
			continue
		}
		old := s.releases[lane]
		if old == nil {
			continue
		}
		s.releases[lane] = nil
		old.refs--
		if old.refs == 0 {
			s.arena.freeReleaseObject(old)
		}
	}
}

func (s *atomicState) allocateRelease() *atomicRelease {
	if s.arena == nil {
		// Direct package tests may exercise the release state machine without a
		// Detector. Production states always arrive from Detector.atomicArena.
		s.arena = newAtomicHistoryArena()
		s.initHistories()
	}
	release := s.arena.allocRelease()
	release.stream = newAtomicReleaseStream()
	release.version = 1
	return release
}

func snapshotAtomicRelease(release *atomicRelease, ctx *goroutine.RaceContext) {
	release.arena.freeRangeList(release.runs)
	release.arena.freeRangeList(release.retired)
	release.runs, release.retired = nil, nil
	var runTail **atomicReleaseRange = &release.runs
	ctx.C.RangeRuns(func(first, last, clock uint32) bool {
		n := release.arena.allocRange()
		n.first, n.last, n.clock = first, last, clock
		*runTail = n
		runTail = &n.next
		return true
	})
	var retiredTail **atomicReleaseRange = &release.retired
	ctx.C.RangeRetired(func(first, last uint32) bool {
		n := release.arena.allocRange()
		n.first, n.last = first, last
		*retiredTail = n
		retiredTail = &n.next
		return true
	})
	release.deltaN = 0
	release.deltaAt = 0
}

// pointMaxAtomicRelease applies one monotonic coordinate update to canonical
// ranges. The linked representation keeps every node at a stable arena address
// and never allocates a backing slice during a runtime callback.
func pointMaxAtomicRelease(release *atomicRelease, tid, clock uint32) bool {
	link := &release.runs
	var prev *atomicReleaseRange
	for *link != nil && (*link).last < tid {
		prev = *link
		link = &(*link).next
	}
	cur := *link
	if cur == nil || cur.first > tid {
		left := prev != nil && prev.clock == clock && prev.last != ^uint32(0) && prev.last+1 == tid
		right := cur != nil && cur.clock == clock && tid != ^uint32(0) && tid+1 == cur.first
		if left && right {
			prev.last, prev.next = cur.last, cur.next
			cur.next = nil
			release.arena.freeRangeList(cur)
		} else if left {
			prev.last = tid
		} else if right {
			cur.first = tid
		} else {
			n := release.arena.allocRange()
			n.first, n.last, n.clock, n.next = tid, tid, clock, cur
			*link = n
		}
		return true
	}
	if cur.clock >= clock {
		return false
	}
	oldLast, oldClock, oldNext := cur.last, cur.clock, cur.next
	if cur.first == tid && cur.last == tid {
		cur.clock = clock
	} else if cur.first == tid {
		cur.first++
		n := release.arena.allocRange()
		n.first, n.last, n.clock, n.next = tid, tid, clock, cur
		*link = n
		cur = n
	} else if cur.last == tid {
		cur.last--
		n := release.arena.allocRange()
		n.first, n.last, n.clock, n.next = tid, tid, clock, oldNext
		cur.next = n
		cur = n
	} else {
		cur.last = tid - 1
		middle := release.arena.allocRange()
		right := release.arena.allocRange()
		middle.first, middle.last, middle.clock = tid, tid, clock
		right.first, right.last, right.clock, right.next = tid+1, oldLast, oldClock, oldNext
		cur.next, middle.next = middle, right
		cur = middle
	}
	// Merge equal-clock neighbors created by the point update.
	if prev != nil && prev.next == cur && prev.clock == cur.clock && prev.last != ^uint32(0) && prev.last+1 == cur.first {
		prev.last, prev.next = cur.last, cur.next
		cur.next = nil
		release.arena.freeRangeList(cur)
		cur = prev
	}
	if cur.next != nil && cur.clock == cur.next.clock && cur.last != ^uint32(0) && cur.last+1 == cur.next.first {
		next := cur.next
		cur.last, cur.next = next.last, next.next
		next.next = nil
		release.arena.freeRangeList(next)
	}
	return true
}

func bumpAtomicReleaseVersion(release *atomicRelease) uint64 {
	if release.version == ^uint64(0) {
		atomicRuntimeThrow("race detector atomic-release version overflow")
	}
	release.version++
	return release.version
}

func appendAtomicReleaseDelta(release *atomicRelease, version uint64, tid, clock uint32) {
	release.deltas[release.deltaAt] = atomicReleaseDelta{version: version, tid: tid, clock: clock}
	release.deltaAt = (release.deltaAt + 1) % atomicReleaseDeltaCapacity
	if release.deltaN < atomicReleaseDeltaCapacity {
		release.deltaN++
	}
}

func (s *atomicState) exactReleaseForMask(mask uint8) (*atomicRelease, uint8) {
	lane := firstMaskLane(mask)
	release := s.releases[lane]
	if release == nil {
		return nil, 0
	}
	membership := s.releaseMembership(release)
	if membership != mask || release.refs != uint8(countMaskBits(mask)) {
		return nil, membership
	}
	return release, membership
}

// publishRelease uses a point update only from a current strong cache proof. A
// weak current proof permits a monotonic full checkpoint in the same stream;
// every other store replaces the release with a new stream and exact snapshot.
func (s *atomicState) publishRelease(ctx *goroutine.RaceContext, mask uint8) {
	if release, membership := s.exactReleaseForMask(mask); release != nil {
		seenVersion, strong, ok := ctx.LookupAtomicRelease(unsafe.Pointer(release), release.stream, membership)
		if ok && seenVersion == release.version {
			if strong {
				// RaceContext keeps Epoch equal to C[TID]. Reading the cached
				// epoch avoids a sparse-vector search for the owning coordinate
				// on every strong release update.
				clock := uint32(ctx.GetEpoch())
				if pointMaxAtomicRelease(release, ctx.TID, clock) {
					version := bumpAtomicReleaseVersion(release)
					appendAtomicReleaseDelta(release, version, ctx.TID, clock)
				}
				ctx.RecordAtomicRelease(unsafe.Pointer(release), release.stream, release.version, membership, true)
				return
			}
			bumpAtomicReleaseVersion(release)
			snapshotAtomicRelease(release, ctx)
			ctx.RecordAtomicRelease(unsafe.Pointer(release), release.stream, release.version, membership, true)
			return
		}
	}

	s.retireReleases(mask)
	release := s.allocateRelease()
	snapshotAtomicRelease(release, ctx)
	release.refs = uint8(countMaskBits(mask))
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) != 0 {
			s.releases[lane] = release
		}
	}
	ctx.RecordAtomicRelease(unsafe.Pointer(release), release.stream, release.version, mask, true)
}

func countMaskBits(mask uint8) int {
	count := 0
	for mask != 0 {
		mask &= mask - 1
		count++
	}
	return count
}

func atomicAccessEmpty(access atomicAccess) bool {
	for _, clock := range access.clocks {
		if clock != 0 {
			return false
		}
	}
	return true
}

func clearAtomicHistoryMask(history *atomicHistory, mask uint8) {
	clearClass := func(frontier *atomicHistoryClass) {
		frontier.visit(func(entry *atomicHistoryEntry) bool {
			access := &entry.access
			for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
				if mask&(uint8(1)<<lane) != 0 {
					access.clocks[lane], access.pcs[lane] = 0, 0
				}
			}
			return true
		})
		frontier.removeEmpty(history.arena)
	}
	clearClass(&history.user)
	clearClass(&history.internal)
}

// clearReboundMask forgets atomic history for lanes which are being attached
// to this overlay from a state with no matching overlay. The caller holds
// s.mu. A surviving sibling may keep s alive after a partial clear, so these
// coordinates must be cleared before a fresh lane generation adopts it; merely
// observing an empty VarState does not prove the overlay lanes are empty.
func (s *atomicState) clearReboundMask(mask uint8) {
	clearAtomicHistoryMask(&s.reads, mask)
	s.clearReadFrontierMask(mask)
	clearAtomicHistoryMask(&s.writes, mask)
	clearAtomicHistoryMask(&s.plainReads, mask)
	clearAtomicHistoryMask(&s.plainWrites, mask)
	s.retireReleases(mask)
}

// publishReleaseUnlessPoisoned publishes only when every prior ordinary
// pre-store hook on the affected lanes happens before this atomic operation.
// The caller holds s.mu. ordinaryConflict covers a pre-existing ordinary write
// which created the overlay during this transaction and therefore has no
// plainWrites sidecar entry yet.
func (s *atomicState) publishReleaseUnlessPoisoned(ctx *goroutine.RaceContext, mask uint8, ordinaryConflict bool) {
	pruneAtomicAccess(&s.plainWrites, ctx, mask, false)
	_, _, _, poisoned := firstConcurrentAtomic(s.plainWrites, ctx, mask)
	if ordinaryConflict || poisoned {
		s.retireReleases(mask)
		return
	}
	s.publishRelease(ctx, mask)
}

// pruneAtomicAccess removes accesses ordered before current on the affected
// lanes only. A user access can replace either reporting class; an internal
// mutex access can replace only another internal access because an internal
// witness may later be suppressed where the user witness would be reportable.
func pruneAtomicAccess(history *atomicHistory, ctx *goroutine.RaceContext, mask uint8, currentInternal bool) {
	prune := func(frontier *atomicHistoryClass) {
		frontier.visit(func(entry *atomicHistoryEntry) bool {
			access := &entry.access
			// Every lane in one frontier entry has the same owner. Resolve its
			// observed clock once rather than repeating the vector lookup for
			// each byte of a 32- or 64-bit atomic access.
			observed := ctx.C.Get(entry.tid)
			for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
				if mask&(uint8(1)<<lane) == 0 {
					continue
				}
				clock := access.clocks[lane]
				if clock != 0 && clock <= observed {
					access.clocks[lane] = 0
					access.pcs[lane] = 0
				}
			}
			return true
		})
		frontier.removeEmpty(history.arena)
	}

	prune(&history.internal)
	if !currentInternal {
		prune(&history.user)
	}
}

func recordAtomicAccess(history *atomicHistory, ctx *goroutine.RaceContext, pc uintptr, mask uint8, internal bool) {
	frontier := &history.user
	if internal {
		frontier = &history.internal
	}
	// Epoch is the cached C[TID] coordinate. Decode it once for the whole
	// width instead of searching the vector clock once per covered lane.
	clock := uint32(ctx.GetEpoch())
	// Replacing this thread's existing witness cannot hide a race: its clock is
	// monotonic and it represents the same reporting class. Pruning other TIDs
	// only bounds the frontier; stale HB-dominated entries cannot manufacture a
	// false negative and will be removed when a new TID is inserted. Avoiding a
	// full map scan is the common path for long-lived atomic users.
	if entry, ok := frontier.find(ctx.TID); ok {
		access := &entry.access
		for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
			if mask&(uint8(1)<<lane) != 0 {
				access.clocks[lane] = clock
				access.pcs[lane] = pc
			}
		}
		return
	}

	pruneAtomicAccess(history, ctx, mask, internal)
	access := &frontier.insert(history.arena, ctx.TID).access
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) != 0 {
			access.clocks[lane] = clock
			access.pcs[lane] = pc
		}
	}
}

func firstConcurrentAtomicClass(history *atomicHistoryClass, ctx *goroutine.RaceContext, mask uint8) (prev epoch.Epoch, pc uintptr, foundLane uint8, found bool) {
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) == 0 {
			continue
		}
		history.visit(func(entry *atomicHistoryEntry) bool {
			clock := entry.access.clocks[lane]
			if clock > ctx.C.Get(entry.tid) {
				prev, pc, foundLane, found = epoch.NewEpoch(entry.tid, uint64(clock)), entry.access.pcs[lane], lane, true
				return false
			}
			return true
		})
		if found {
			return
		}
	}
	return
}

// firstConcurrentAtomic returns a user witness before considering an internal
// mutex implementation witness, even when the user witness is on a later lane.
// captureAtomic may suppress the latter; choosing it first could otherwise hide
// a genuine same-operation conflict. Within one reporting class, lane order is
// deterministic while map iteration may choose any conflicting thread.
func firstConcurrentAtomic(history atomicHistory, ctx *goroutine.RaceContext, mask uint8) (epoch.Epoch, uintptr, uint8, bool) {
	if prev, pc, lane, conflict := firstConcurrentAtomicClass(&history.user, ctx, mask); conflict {
		return prev, pc, lane, true
	}
	return firstConcurrentAtomicClass(&history.internal, ctx, mask)
}

// firstReportableConcurrentAtomic filters implementation-only conflicts before
// a caller chooses between write and read histories. An internal atomic access
// paired with an internal plain access is intentionally suppressed; returning
// it as a conflict would prevent a write-first caller from examining a genuine
// user read witness in the other history. User atomic witnesses are reportable
// against every plain access and therefore retain priority.
func firstReportableConcurrentAtomic(history atomicHistory, ctx *goroutine.RaceContext, mask uint8, plainPC uintptr) (epoch.Epoch, uintptr, uint8, bool) {
	if prev, pc, lane, conflict := firstConcurrentAtomicClass(&history.user, ctx, mask); conflict {
		return prev, pc, lane, true
	}
	if rwMutexMarkerPC(plainPC) {
		return 0, 0, 0, false
	}
	return firstConcurrentAtomicClass(&history.internal, ctx, mask)
}

func exactConcurrentPlainRead(state *atomicState, ordinary *shadowmem.VarState, ctx *goroutine.RaceContext, mask uint8) (epoch.Epoch, uintptr, bool) {
	var userPrev, internalPrev epoch.Epoch
	var userPC, internalPC uintptr
	classify := func(read epoch.Epoch) bool {
		tid, clock := read.Decode()
		for class, frontier := range [2]*atomicHistoryClass{&state.plainReads.user, &state.plainReads.internal} {
			entry, ok := frontier.find(tid)
			if !ok {
				continue
			}
			access := entry.access
			for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
				if mask&(uint8(1)<<lane) != 0 && access.clocks[lane] == uint32(clock) {
					if class == 0 && userPrev == 0 {
						userPrev, userPC = read, access.pcs[lane]
					} else if class == 1 && internalPrev == 0 {
						internalPrev, internalPC = read, access.pcs[lane]
					}
					return true
				}
			}
		}
		return false
	}

	if ordinary.IsPromoted() {
		complete := true
		ordinary.GetReadClock().Range(func(tid, clock uint32) bool {
			if clock > ctx.C.Get(tid) && !classify(epoch.NewEpoch(tid, uint64(clock))) {
				complete = false
				return false
			}
			return true
		})
		if !complete {
			return 0, 0, false
		}
	} else {
		for _, read := range ordinary.GetReadEpochs() {
			if !read.HappensBefore(ctx.C) && !classify(read) {
				return 0, 0, false
			}
		}
	}
	if userPrev != 0 {
		return userPrev, userPC, true
	}
	return internalPrev, internalPC, internalPrev != 0
}

func (s *atomicState) mask(addr, size uintptr) uint8 {
	if size == 0 || size-1 > ^uintptr(0)-addr {
		return 0
	}
	start := addr
	if start < s.base {
		start = s.base
	}
	end := addr + size
	wordEnd := s.base + AtomicTokenSlots
	if end > wordEnd {
		end = wordEnd
	}
	if start >= end {
		return 0
	}
	width := end - start
	return uint8(((uint16(1) << width) - 1) << (start - s.base))
}

func validAtomicAccess(addr, size uintptr) bool {
	return addr != 0 && (size == 4 || size == 8) && size-1 <= ^uintptr(0)-addr
}

func plainAtomicFastMask(addr, size uintptr) (uint8, bool) {
	if !validAtomicAccess(addr, size) || addr&(size-1) != 0 || (addr&7)+size > AtomicTokenSlots {
		return 0, false
	}
	return uint8(((uint16(1) << size) - 1) << (addr & 7)), true
}

type atomicTokenState struct {
	state *atomicState
	mask  uint8
}

type atomicTokenGroup struct {
	state *shadowmem.VarState
	base  uintptr
	mask  uint8
}

// AtomicBegin starts the general width-aware fallback transaction used by
// unaligned, ignored, and direct detector operations. General operations
// permanently escape any enrolled exact-mask capability they encounter. A
// non-empty token must be completed according to AtomicToken's matching and
// exactly-once contract.
func (d *Detector) AtomicBegin(addr, size uintptr, ctx *goroutine.RaceContext, acquire bool, token *AtomicToken) {
	d.atomicBegin(addr, size, ctx, acquire, false, false, true, true, -1, false, token)
}

// AtomicBeginRMW starts an enabled read-modify-write or compare-and-swap
// transaction. An aligned, exact-mask operation may reuse an existing enrolled
// capability, but an RMW never enrolls a new one. A miss therefore takes the
// general transaction and permanently escapes any incompatible capability.
// Failed compare-and-swaps must complete through AtomicEndMode with write=false;
// every AtomicBeginRMW token must be completed with synchronize=true.
func (d *Detector) AtomicBeginRMW(addr, size uintptr, ctx *goroutine.RaceContext, acquire bool, token *AtomicToken) {
	d.atomicBegin(addr, size, ctx, acquire, true, false, true, true, 1, false, token)
}

// AtomicBeginRMWCooperative starts the public runtime RMW transaction. It is
// identical to AtomicBeginRMW except when an exact retained capability exists
// and another transaction holds its state lock: in that one case it releases
// the capability without producing a token or changing detector state and asks
// the user-goroutine wrapper to yield before retrying. Capability misses and
// general or mixed-width transactions retain the authoritative blocking path.
func (d *Detector) AtomicBeginRMWCooperative(addr, size uintptr, ctx *goroutine.RaceContext, acquire bool, token *AtomicToken) (retry bool) {
	return d.atomicBegin(addr, size, ctx, acquire, true, false, true, true, 1, true, token)
}

// AtomicBeginPlain starts an aligned plain Load or Store transaction.
// After one fully locked enrollment operation, a stable exact mask retains only
// its capability gate and atomicState.mu across the hardware access. Enrollment
// may freeze one ordinary equivalence group (the compiler-initialization shape);
// AtomicEndMode checks that history on every fast completion. The capability
// retains and validates the immutable generation before use. Exact enabled RMW
// and CAS operations may reuse it; every ordinary, mixed, ignored, clear, reset,
// incompatible-width, or reuse-miss operation closes that descriptor forever.
// After one probationary compatible operation, a later compatible setup may
// publish a distinct descriptor once the closed generation and all affected
// states have drained.
// synchronize must be the same enabled-operation decision passed to
// AtomicEndMode. A false value makes the operation general/slow so an ignored
// load or store can never enroll or reuse a synchronization capability.
func (d *Detector) AtomicBeginPlain(addr, size uintptr, ctx *goroutine.RaceContext, acquire, synchronize bool, token *AtomicToken) {
	// End reveals whether a trusted direct caller performed a read or write, so
	// conservatively bracket every locked transaction. Cache hits are the only
	// path which may omit a revision transition.
	syncMode := int8(0)
	if synchronize {
		syncMode = 1
	}
	d.atomicBegin(addr, size, ctx, acquire, synchronize, synchronize, true, synchronize, syncMode, false, token)
}

// AtomicBeginLoad is the trusted enabled public-Load fallback. Unlike the
// general Plain entry point its operation kind is known before hardware access,
// so it need not perturb the writer revision and invalidate other readers.
func (d *Detector) AtomicBeginLoad(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken) {
	d.atomicBegin(addr, size, ctx, true, true, true, false, false, 1, false, token)
}

// AtomicBeginLoadCooperative is the enabled public-Load fallback. It asks the
// user-goroutine wrapper to retry only when an already-retained exact
// capability's state lock is contended. Capability misses, enrollment, and
// general transactions remain on the authoritative blocking path.
func (d *Detector) AtomicBeginLoadCooperative(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken) (retry bool) {
	return d.atomicBegin(addr, size, ctx, true, true, true, false, false, 1, true, token)
}

// AtomicBeginStoreCooperative is the enabled public-Store transaction. Like
// AtomicBeginLoadCooperative, only retained exact-capability lock contention is
// returned to the user goroutine; misses and enrollment continue to block.
func (d *Detector) AtomicBeginStoreCooperative(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken) (retry bool) {
	return d.atomicBegin(addr, size, ctx, false, true, true, true, true, 1, true, token)
}

func deactivateAtomicLoadEntry(entry goroutine.AtomicLoadCacheEntry) *atomicReadFrontier {
	if entry.State == nil || entry.Frontier == nil {
		return nil
	}
	frontier := (*atomicReadFrontier)(entry.Frontier)
	// Sidecar retirement invalidates and recycles registered frontier nodes
	// before reusing their owning state. Reject that stale cache entry without
	// locking through its now-recyclable State pointer.
	if frontier.generation.Load() != entry.Generation {
		return nil
	}
	state := (*atomicState)(entry.State)
	state.mu.lock()
	spare := state.deactivateReadFrontier(frontier, entry.Generation)
	state.mu.unlock()
	return spare
}

// prepareAtomicLoadCache makes one cache slot empty before the locked fallback
// acquires any new state lock. Eviction therefore never creates a state-lock
// ordering cycle. The evicted node is deactivated under its owning state lock
// and retained by its context cache slot as a generation-tagged spare for reuse.
func prepareAtomicLoadCache(ctx *goroutine.RaceContext, fast, state unsafe.Pointer, mask uint8, pc uintptr, internal bool) {
	index := -1
	for i := range ctx.AtomicLoadCache {
		entry := &ctx.AtomicLoadCache[i]
		if entry.Fast == fast && entry.State == state && entry.Mask == mask && entry.PC == pc && entry.Internal == internal {
			index = i
			break
		}
		if index < 0 && entry.Fast == nil {
			index = i
		}
	}
	if index < 0 {
		index = int(ctx.AtomicLoadCacheNext % goroutine.AtomicLoadCacheSlots)
		ctx.AtomicLoadCacheNext = (ctx.AtomicLoadCacheNext + 1) % goroutine.AtomicLoadCacheSlots
	}
	old := ctx.AtomicLoadCache[index]
	ctx.AtomicLoadCache[index] = goroutine.AtomicLoadCacheEntry{}
	if spare := deactivateAtomicLoadEntry(old); spare != nil {
		ctx.AtomicLoadCache[index] = goroutine.AtomicLoadCacheEntry{
			Frontier: unsafe.Pointer(spare), Generation: spare.generation.Load(),
		}
	}
}

func lookupAtomicLoadCache(ctx *goroutine.RaceContext, fast, state unsafe.Pointer, mask uint8, pc uintptr, internal bool) (*atomicReadFrontier, uint64, uint64, bool) {
	for i := range ctx.AtomicLoadCache {
		entry := &ctx.AtomicLoadCache[i]
		if entry.Fast == fast && entry.State == state && entry.Mask == mask && entry.PC == pc && entry.Internal == internal && entry.Frontier != nil {
			return (*atomicReadFrontier)(entry.Frontier), entry.Revision, entry.Generation, true
		}
	}
	return nil, 0, 0, false
}

func atomicLoadCacheSpare(ctx *goroutine.RaceContext, fast *shadowmem.AtomicFastPath, state *atomicState, mask uint8, pc uintptr, internal bool) (*atomicReadFrontier, bool) {
	for i := range ctx.AtomicLoadCache {
		entry := &ctx.AtomicLoadCache[i]
		if entry.Fast == unsafe.Pointer(fast) && entry.State == unsafe.Pointer(state) && entry.Mask == mask && entry.PC == pc && entry.Internal == internal {
			return (*atomicReadFrontier)(entry.Frontier), true
		}
	}
	for i := range ctx.AtomicLoadCache {
		entry := &ctx.AtomicLoadCache[i]
		if entry.Fast == nil && entry.State == nil {
			return (*atomicReadFrontier)(entry.Frontier), true
		}
	}
	return nil, false
}

func recordAtomicLoadCache(ctx *goroutine.RaceContext, fast *shadowmem.AtomicFastPath, state *atomicState, frontier *atomicReadFrontier, revision uint64, mask uint8, pc uintptr, internal bool) {
	index := -1
	for i := range ctx.AtomicLoadCache {
		entry := &ctx.AtomicLoadCache[i]
		if entry.Fast == unsafe.Pointer(fast) && entry.State == unsafe.Pointer(state) && entry.Mask == mask && entry.PC == pc && entry.Internal == internal {
			index = i
			break
		}
		if index < 0 && entry.Fast == nil && entry.State == nil {
			index = i
		}
	}
	if index < 0 {
		// Direct detector callers need not have executed cache preparation.
		// Remaining locked is sound; simply decline to seed a cache entry.
		return
	}
	ctx.AtomicLoadCache[index] = goroutine.AtomicLoadCacheEntry{
		Fast: unsafe.Pointer(fast), State: unsafe.Pointer(state), Frontier: unsafe.Pointer(frontier),
		Revision: revision, Generation: frontier.generation.Load(), PC: pc, Mask: mask, Internal: internal,
	}
}

// DeactivateAtomicLoadCache releases every registered frontier owned by ctx.
// API lifecycle teardown calls this before dropping the context's GC root or
// resetting shadow memory.
func DeactivateAtomicLoadCache(ctx *goroutine.RaceContext) {
	if ctx == nil {
		return
	}
	for i := range ctx.AtomicLoadCache {
		entry := ctx.AtomicLoadCache[i]
		ctx.AtomicLoadCache[i] = goroutine.AtomicLoadCacheEntry{}
		if spare := deactivateAtomicLoadEntry(entry); spare != nil {
			spare.arena.freeFrontierNode(spare)
		} else if entry.State == nil && entry.Frontier != nil {
			spare := (*atomicReadFrontier)(entry.Frontier)
			spare.arena.freeFrontierNode(spare)
		}
	}
	ctx.AtomicLoadCacheNext = 0
}

// AtomicBeginLoadFast attempts the cache-only exact Load path. It retains the
// immutable AtomicFastPath capability but deliberately does not lock or inspect
// atomicState release metadata. A hit is valid only at the exact even writer
// revision imported by an earlier locked load on this context.
//
//go:nocheckptr
func (d *Detector) AtomicBeginLoadFast(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr, token *AtomicToken) (uint64, uint64, bool) {
	if token == nil {
		return 0, 0, false
	}
	for i := range token {
		token[i] = nil
	}
	mask, ok := plainAtomicFastMask(addr, size)
	if ctx == nil || !ok {
		return 0, 0, false
	}
	slot := d.slotMemory.GetSlot(addr)
	if slot == nil {
		prepareAtomicLoadCache(ctx, nil, nil, mask, pc, false)
		return 0, 0, false
	}
	fast := slot.TryAtomicFast(mask)
	if fast == nil {
		prepareAtomicLoadCache(ctx, nil, nil, mask, pc, false)
		return 0, 0, false
	}
	statePointer := fast.Overlay()
	state := (*atomicState)(statePointer)
	internal := atomicInternalMutexPC(pc)
	frontier, cachedRevision, generation, cached := lookupAtomicLoadCache(ctx, unsafe.Pointer(fast), statePointer, mask, pc, internal)
	revision := state.writerRevision.Load()
	var frontierGeneration uint64
	var frontierMask uint8
	if cached {
		frontierGeneration = frontier.generation.Load()
		frontierMask = uint8(frontier.mask.Load())
	}
	if cached && frontierGeneration == generation && frontierMask == 0 {
		// A full HB writer may have pruned this context-owned node before
		// advancing the revision. Keep the detached node in its exact slot so
		// the locked fallback can relink it without allocating. An active node
		// is still prepared normally: an overlapping writer could prune it to a
		// partial mask before the fallback reaches the state lock.
		fast.Release()
		return 0, 0, false
	}
	if !cached || frontierGeneration != generation || frontierMask != mask {
		fast.Release()
		prepareAtomicLoadCache(ctx, unsafe.Pointer(fast), statePointer, mask, pc, internal)
		return 0, 0, false
	}
	if revision&1 != 0 || revision != cachedRevision {
		// The cache binding is still structurally usable. Leave its active
		// frontier registered: the locked fallback imports the new release and
		// refreshes this same cache entry at the latest even revision.
		fast.Release()
		return 0, 0, false
	}
	token[0] = unsafe.Pointer(fast)
	token[1] = unsafe.Pointer(frontier)
	state.arena.pin()
	return revision, generation, true
}

// AtomicEndLoadFast validates the writer seqlock after the hardware load and
// publishes its exact read witness before releasing the lifecycle capability.
// False asks the runtime wrapper to discard the speculative value and repeat
// the load through the existing locked transaction.
//
//go:nocheckptr
func (d *Detector) AtomicEndLoadFast(ctx *goroutine.RaceContext, token *AtomicToken, revision, generation uint64) bool {
	if ctx == nil || token == nil || token[0] == nil || token[1] == nil {
		return false
	}
	fast := (*shadowmem.AtomicFastPath)(token[0])
	state := (*atomicState)(fast.Overlay())
	if revision&1 != 0 || state.writerRevision.Load() != revision {
		for i := range token {
			token[i] = nil
		}
		state.arena.unpin()
		fast.Release()
		return false
	}
	frontier := (*atomicReadFrontier)(token[1])
	if frontier.generation.Load() != generation || frontier.tid != ctx.TID || uint8(frontier.mask.Load()) != fast.Mask() {
		for i := range token {
			token[i] = nil
		}
		state.arena.unpin()
		fast.Release()
		return false
	}
	previousClock := frontier.clock.Load()
	currentClock := uint32(ctx.GetEpoch())
	frontier.clock.Store(currentClock)
	// A writer may begin after the first revision check and retire this node
	// before publication. Validate once more after the atomic witness store. On
	// failure, restore the prior real load (when still linked) and let the
	// runtime discard/reload the speculative hardware value under the lock.
	if state.writerRevision.Load() != revision || frontier.generation.Load() != generation || uint8(frontier.mask.Load()) != fast.Mask() {
		frontier.clock.CompareAndSwap(currentClock, previousClock)
		for i := range token {
			token[i] = nil
		}
		state.arena.unpin()
		fast.Release()
		return false
	}
	ctx.WeakenReadCache()
	for i := range token {
		token[i] = nil
	}
	state.arena.unpin()
	fast.Release()
	return true
}

// atomicBegin starts a width-aware transaction. The slow path splits only
// partially covered ordinary groups, locks each resulting equivalence group,
// attaches the detector overlay while those locks are held, then locks distinct
// atomic histories in stable pointer order across the hardware operation.
func (d *Detector) atomicBegin(addr, size uintptr, ctx *goroutine.RaceContext, acquire, reuseFast, enrollFast, writer, advance bool, syncMode int8, cooperative bool, token *AtomicToken) (retry bool) {
	if token == nil {
		return false
	}
	for i := range token {
		token[i] = nil
	}
	if ctx == nil || !validAtomicAccess(addr, size) {
		return false
	}
	if advance {
		// Validate exhaustion before the hardware access, release import, writer
		// revision, or history publication. End validates the same successor and
		// commits it only after all authoritative state is visible.
		ctx.PreflightClockAdvance()
	}

	firstBase := addr &^ uintptr(7)
	lastBase := (addr + size - 1) &^ uintptr(7)
	fastMask, exactFastShape := plainAtomicFastMask(addr, size)
	exactFastShape = exactFastShape && firstBase == lastBase
	if reuseFast && exactFastShape {
		if slot := d.slotMemory.GetSlot(addr); slot != nil {
			if fast := slot.TryAtomicFast(fastMask); fast != nil {
				state := (*atomicState)(fast.Overlay())
				if cooperative {
					if !state.mu.tryLock() {
						fast.Release()
						return true
					}
				} else {
					state.mu.lock()
				}
				state.arena.pin()
				state.beginTransaction(addr, size, ctx, syncMode)
				if writer {
					state.beginWriter()
				}
				// TryAtomicFast's retained user freezes the descriptor, every
				// covered lane, and its lifecycle until AtomicEnd releases it.
				// Revalidating after the overlay lock repeated eight atomic lane
				// loads without adding a serialization edge.
				if acquire {
					state.acquire(ctx, fastMask)
				}
				token[0] = unsafe.Pointer(fast)
				return false
			}
		}
	}

	firstSlot := d.slotMemory.GetOrCreateSlot(firstBase)
	lastSlot := firstSlot
	if lastBase != firstBase {
		lastSlot = d.slotMemory.GetOrCreateSlot(lastBase)
	}

	type wordSetup struct {
		state            *atomicState
		fromExisting     bool
		provisional      [AtomicTokenSlots]*shadowmem.VarState
		provisionalMasks [AtomicTokenSlots]uint8
		provisionalCount int
	}
	var setups [2]wordSetup
	lockWord := func(base uintptr, slot *shadowmem.ShadowSlot, enroll bool) {
		word := 0
		if base != firstBase {
			word = 1
		}
		setup := &setups[word]
		var operationMask uint8
		for i := uintptr(0); i < size; i++ {
			byteAddr := addr + i
			if byteAddr&^uintptr(7) == base {
				operationMask |= uint8(1) << uint8(byteAddr&7)
			}
		}
		slot.LockAtomicGroups(operationMask, enroll, func(groupMask uint8, state *shadowmem.VarState) {
			for i := uintptr(0); i < size; i++ {
				byteAddr := addr + i
				if byteAddr&^uintptr(7) == base && groupMask&(uint8(1)<<uint8(byteAddr&7)) != 0 {
					token[i] = unsafe.Pointer(state)
				}
			}
			if existing := existingAtomicStateLocked(state); existing != nil && existing.base == base {
				if setup.state == nil {
					setup.state = existing
					setup.fromExisting = true
				} else if !setup.fromExisting {
					// Earlier groups had no overlay and received a provisional empty
					// one. Prefer this real history, clear only the newly attached
					// lanes which may be stale after a partial clear, then rebind the
					// provisional groups while all their access locks remain held.
					existing.mu.lock()
					for i := 0; i < setup.provisionalCount; i++ {
						existing.clearReboundMask(setup.provisionalMasks[i])
					}
					existing.mu.unlock()
					for i := 0; i < setup.provisionalCount; i++ {
						setup.provisional[i].SetAtomicStateOwned(unsafe.Pointer(existing), retainAtomicArenaState, releaseAtomicArenaState)
						setup.provisional[i] = nil
						setup.provisionalMasks[i] = 0
					}
					setup.provisionalCount = 0
					setup.state = existing
					setup.fromExisting = true
				}
				// Two different established overlays retain independent lane
				// histories. Leave both installed; shadow enrollment will reject
				// the shape rather than losing either frontier or release.
				return
			}
			if setup.state == nil {
				setup.state = d.atomicArena.newState(base)
			}
			if setup.fromExisting {
				setup.state.mu.lock()
				setup.state.clearReboundMask(groupMask)
				setup.state.mu.unlock()
			}
			state.SetAtomicStateOwned(unsafe.Pointer(setup.state), retainAtomicArenaState, releaseAtomicArenaState)
			if !setup.fromExisting {
				setup.provisional[setup.provisionalCount] = state
				setup.provisionalMasks[setup.provisionalCount] = groupMask
				setup.provisionalCount++
			}
		})
	}

	// Materialize every participating word before retaining any state lock.
	// Full-block range operations acquire block then state; attempting a later
	// materialization while holding an earlier state would invert that order.
	// Retained state locks are then acquired by ascending application address,
	// matching every other spanning atomic transaction.
	fastEnrollEligible := enrollFast && exactFastShape
	lockWord(firstBase, firstSlot, fastEnrollEligible)
	if lastBase != firstBase {
		lockWord(lastBase, lastSlot, false)
	}
	if fastEnrollEligible {
		// Enrollment occurred under slot -> accessMu. Convert this setup (or a
		// compatible caller which missed publication) into a retained capability
		// before releasing accessMu. A concurrent ordinary/clear operation either
		// closes the binding first and forces this transaction to remain slow, or
		// observes our retained user and waits through the hardware operation.
		groups, groupCount := atomicTokenGroups(token, addr, size)
		if fast := firstSlot.TryAtomicFast(fastMask); fast != nil {
			state := (*atomicState)(fast.Overlay())
			state.mu.lock()
			state.arena.pin()
			state.beginTransaction(addr, size, ctx, syncMode)
			if writer {
				state.beginWriter()
			}
			for i := groupCount - 1; i >= 0; i-- {
				groups[i].state.UnlockAccess()
			}
			if acquire {
				state.acquire(ctx, fastMask)
			}
			for i := range token {
				token[i] = nil
			}
			token[0] = unsafe.Pointer(fast)
			return false
		}
	}

	states, stateCount := atomicTokenStates(token, addr, size)
	for i := 0; i < stateCount; i++ {
		states[i].state.mu.lock()
		states[i].state.arena.pin()
		states[i].state.beginTransaction(addr, size, ctx, syncMode)
		if writer {
			states[i].state.beginWriter()
		}
		if acquire {
			states[i].state.acquire(ctx, states[i].mask)
		}
	}
	return false
}

// AtomicEnd completes a normally synchronizing atomic operation. addr, size,
// ctx, and token must exactly match the preceding Begin call.
func (d *Detector) AtomicEnd(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr, write bool) {
	d.AtomicEndMode(addr, size, ctx, token, pc, write, true)
}

// AtomicEndLoad completes the locked fallback for a public enabled Load and
// seeds the exact cache/frontier when the transaction retained a capability.
func (d *Detector) AtomicEndLoad(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr) {
	d.atomicEndMode(addr, size, ctx, token, pc, false, true, true)
}

// AtomicEndMode publishes one transition per unique overlay membership and
// checks each ordinary equivalence group once while its Begin lock is still
// held. Memory histories and mixed atomic/plain conflicts are retained even
// when synchronize is false. In that mode writes retire superseded releases,
// but no release is published and the context clock does not advance. Enabled
// read-like operations weaken ordinary read-cache entries without advancing the
// owning clock; enabled writes publish a release and then advance it.
func (d *Detector) AtomicEndMode(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr, write, synchronize bool) {
	d.atomicEndMode(addr, size, ctx, token, pc, write, synchronize, false)
}

func (d *Detector) atomicEndMode(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr, write, synchronize, cacheLoad bool) {
	if token == nil || token[0] == nil {
		return
	}
	if ctx == nil || !validAtomicAccess(addr, size) {
		atomicRuntimeThrow("race detector invalid atomic transaction completion")
	}
	if pc == 0 {
		pc = captureCallerPC()
	}
	var next uint64
	if synchronize && write {
		next = ctx.PreflightClockAdvance()
	}
	if fast := atomicFastToken(token); fast != nil {
		d.atomicEndPlainFast(addr, size, ctx, token, fast, pc, write, synchronize, cacheLoad)
		return
	}
	current := ctx.GetEpoch()
	internal := atomicInternalMutexPC(pc)
	firstOrdinary := (*shadowmem.VarState)(token[0])
	firstState := existingAtomicStateLocked(firstOrdinary)
	firstState.validateTransaction(addr, size, ctx, synchronize)
	groups, groupCount := atomicTokenGroups(token, addr, size)
	states, stateCount := atomicTokenStates(token, addr, size)
	for i := 0; i < stateCount; i++ {
		states[i].state.validateTransaction(addr, size, ctx, synchronize)
	}
	for i := 0; i < stateCount; i++ {
		entry := states[i]
		if write {
			ordinaryConflict := false
			for j := 0; j < groupCount; j++ {
				group := groups[j]
				if group.base != entry.state.base || existingAtomicStateLocked(group.state) != entry.state || group.mask&entry.mask == 0 {
					continue
				}
				if prev := group.state.GetW(); prev != 0 && !prev.HappensBefore(ctx.C) {
					ordinaryConflict = true
					break
				}
			}
			// A later write is a valid witness for any earlier read or write
			// that happens before it. Retaining only the HB-maximal frontier
			// bounds sequential fresh-TID churn without capping concurrency.
			entry.state.pruneReadFrontiers(ctx, entry.mask)
			pruneAtomicAccess(&entry.state.reads, ctx, entry.mask, internal)
			recordAtomicAccess(&entry.state.writes, ctx, pc, entry.mask, internal)
			if synchronize {
				entry.state.publishReleaseUnlessPoisoned(ctx, entry.mask, ordinaryConflict)
			} else {
				entry.state.retireReleases(entry.mask)
			}
		} else {
			// A read can replace earlier reads, but it cannot replace a write:
			// a future plain read conflicts only with atomic writes.
			recordAtomicAccess(&entry.state.reads, ctx, pc, entry.mask, internal)
		}
	}

	var pending pendingRangeRace
	for i := 0; i < groupCount; i++ {
		group := groups[i]
		laneAddr := group.base + uintptr(firstMaskLane(group.mask))
		previous := snapshotOrdinaryState(group.state)
		if write {
			if prev := group.state.GetW(); prev != 0 && !prev.HappensBefore(ctx.C) {
				pending.captureAtomic(RaceTypeWriteWrite, laneAddr, previous, prev, current, pc, previous.writePC, pc)
			}
			if prev, conflict := group.state.FirstConcurrentRead(ctx.C); conflict {
				plainPC, _ := group.state.ReadPCForEpoch(prev)
				state := existingAtomicStateLocked(group.state)
				if exactPrev, exactPC, exact := exactConcurrentPlainRead(state, group.state, ctx, group.mask); exact {
					prev, plainPC = exactPrev, exactPC
				}
				previousRead := previous
				previousRead.readPC = plainPC
				pending.captureAtomic(RaceTypeReadWrite, laneAddr, previousRead, prev, current, pc, plainPC, pc)
			}
		} else if prev := group.state.GetW(); prev != 0 && !prev.HappensBefore(ctx.C) {
			pending.captureAtomic(RaceTypeWriteRead, laneAddr, previous, prev, current, pc, previous.writePC, pc)
		}
	}

	for i := stateCount - 1; i >= 0; i-- {
		states[i].state.endTransaction()
		if states[i].state.writerRevision.Load()&1 != 0 {
			states[i].state.endWriter()
		}
		states[i].state.mu.unlock()
		states[i].state.arena.unpin()
	}
	// Keep the ordinary generation retained through the same context/token
	// bookkeeping covered by the fast capability gate. ClearRange drains these
	// access locks before returning, so it cannot observe a completed history
	// publication while the corresponding atomic transaction is still live.
	if synchronize {
		if write {
			ctx.CommitClockAdvance(next)
		} else {
			ctx.WeakenReadCache()
		}
	}
	if write {
		ctx.InvalidateReadRange(addr, size)
	}
	for i := uintptr(0); i < size; i++ {
		token[i] = nil
	}
	for i := groupCount - 1; i >= 0; i-- {
		groups[i].state.UnlockAccess()
	}
	pending.report(d)
}

func atomicFastToken(token *AtomicToken) *shadowmem.AtomicFastPath {
	if token == nil || token[0] == nil || token[1] != nil {
		return nil
	}
	return (*shadowmem.AtomicFastPath)(token[0])
}

// atomicEndPlainFast completes an enrolled exact-mask transaction. The retained
// capability freezes every covered ordinary group until Release. Enrollment
// admits at most one non-empty group, which is checked with the same predicates
// as the locked path after the exact atomic frontier/release transition.
func (d *Detector) atomicEndPlainFast(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, fast *shadowmem.AtomicFastPath, pc uintptr, write, synchronize, cacheLoad bool) {
	var next uint64
	if synchronize && write {
		next = ctx.PreflightClockAdvance()
	}
	state := (*atomicState)(fast.Overlay())
	state.validateTransaction(addr, size, ctx, synchronize)
	mask := fast.Mask()
	internal := atomicInternalMutexPC(pc)
	ordinaryMask := fast.OrdinaryMask()
	ordinary := fast.State()
	var loadFrontier *atomicReadFrontier
	ordinaryWriteConflict := false
	if write && ordinaryMask != 0 {
		if prev := ordinary.GetW(); prev != 0 && !prev.HappensBefore(ctx.C) {
			ordinaryWriteConflict = true
		}
	}
	if write {
		state.pruneReadFrontiers(ctx, mask)
		pruneAtomicAccess(&state.reads, ctx, mask, internal)
		recordAtomicAccess(&state.writes, ctx, pc, mask, internal)
		if synchronize {
			state.publishReleaseUnlessPoisoned(ctx, mask, ordinaryWriteConflict)
		} else {
			state.retireReleases(mask)
		}
	} else {
		if cacheLoad && synchronize {
			if spare, ok := atomicLoadCacheSpare(ctx, fast, state, mask, pc, internal); ok {
				loadFrontier = state.registerReadFrontier(ctx, mask, pc, internal, spare)
			}
		}
		if loadFrontier == nil {
			recordAtomicAccess(&state.reads, ctx, pc, mask, internal)
		}
	}

	var pending pendingRangeRace
	if ordinaryMask != 0 {
		laneAddr := state.base + uintptr(firstMaskLane(ordinaryMask))
		previous := snapshotOrdinaryState(ordinary)
		current := ctx.GetEpoch()
		if write {
			if prev := ordinary.GetW(); prev != 0 && !prev.HappensBefore(ctx.C) {
				pending.captureAtomic(RaceTypeWriteWrite, laneAddr, previous, prev, current, pc, previous.writePC, pc)
			}
			if prev, conflict := ordinary.FirstConcurrentRead(ctx.C); conflict {
				plainPC, _ := ordinary.ReadPCForEpoch(prev)
				if exactPrev, exactPC, exact := exactConcurrentPlainRead(state, ordinary, ctx, ordinaryMask); exact {
					prev, plainPC = exactPrev, exactPC
				}
				previousRead := previous
				previousRead.readPC = plainPC
				pending.captureAtomic(RaceTypeReadWrite, laneAddr, previousRead, prev, current, pc, plainPC, pc)
			}
		} else if prev := ordinary.GetW(); prev != 0 && !prev.HappensBefore(ctx.C) {
			pending.captureAtomic(RaceTypeWriteRead, laneAddr, previous, prev, current, pc, previous.writePC, pc)
		}
	}
	if state.writerRevision.Load()&1 != 0 {
		state.endWriter()
	}
	state.endTransaction()
	if loadFrontier != nil {
		recordAtomicLoadCache(ctx, fast, state, loadFrontier, state.writerRevision.Load(), mask, pc, internal)
	}
	state.mu.unlock()
	state.arena.unpin()

	if synchronize {
		if write {
			ctx.CommitClockAdvance(next)
		} else {
			ctx.WeakenReadCache()
		}
	}
	if write {
		ctx.InvalidateReadRange(addr, size)
	}
	for i := range token {
		token[i] = nil
	}
	fast.Release()
	pending.report(d)
}

// atomicTokenGroups and atomicTokenStates decode a runtime-constructed opaque
// token after atomicEndMode has validated its address, width, and context.
// Token entries are pointers retained from detector-owned objects by Begin.
//
//go:nocheckptr
func atomicTokenGroups(token *AtomicToken, addr, size uintptr) ([AtomicTokenSlots]atomicTokenGroup, int) {
	var groups [AtomicTokenSlots]atomicTokenGroup
	count := 0
	for i := uintptr(0); i < size; i++ {
		state := (*shadowmem.VarState)(token[i])
		bit := uint8(1) << uint8((addr+i)&7)
		found := -1
		for j := 0; j < count; j++ {
			if groups[j].state == state {
				found = j
				break
			}
		}
		if found < 0 {
			groups[count] = atomicTokenGroup{
				state: state,
				base:  (addr + i) &^ uintptr(7),
				mask:  bit,
			}
			count++
		} else {
			groups[found].mask |= bit
		}
	}
	return groups, count
}

//go:nocheckptr
func atomicTokenStates(token *AtomicToken, addr, size uintptr) ([AtomicTokenSlots]atomicTokenState, int) {
	var states [AtomicTokenSlots]atomicTokenState
	count := 0
	for i := uintptr(0); i < size; i++ {
		ordinary := (*shadowmem.VarState)(token[i])
		state := existingAtomicStateLocked(ordinary)
		bit := uint8(1) << uint8((addr+i)&7)
		found := -1
		for j := 0; j < count; j++ {
			if states[j].state == state {
				found = j
				break
			}
		}
		if found < 0 {
			states[count] = atomicTokenState{state: state, mask: bit}
			count++
		} else {
			states[found].mask |= bit
		}
	}
	// Lock exact physical words in ascending application-address order. Handle
	// index is a stable tie-breaker for distinct surviving overlays at one word.
	for i := 1; i < count; i++ {
		entry := states[i]
		j := i
		for j > 0 && (states[j-1].state.base > entry.state.base ||
			(states[j-1].state.base == entry.state.base && states[j-1].state.handle.index > entry.state.handle.index)) {
			states[j] = states[j-1]
			j--
		}
		states[j] = entry
	}
	return states, count
}

func firstMaskLane(mask uint8) uint8 {
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) != 0 {
			return lane
		}
	}
	return 0
}

// captureAtomicReadLocked captures an ordinary read conflicting with a prior
// atomic write. The caller holds vs's access lock; reporting is deferred until
// after the ordinary access has been published and all locks are released.
func (d *Detector) captureAtomicReadLocked(addr, size uintptr, vs *shadowmem.VarState, ctx *goroutine.RaceContext, pc uintptr, marker bool, pending *pendingRangeRace) bool {
	state := existingAtomicStateLocked(vs)
	if state == nil {
		if !marker {
			return false
		}
		state = d.atomicArena.newState(addr &^ uintptr(7))
		for _, read := range vs.GetReadEpochs() {
			tid, clock := read.Decode()
			access := &state.plainReads.user.insert(state.arena, tid).access
			access.clocks[addr&7] = uint32(clock)
		}
		if vs.IsPromoted() {
			state.plainReads.user.clear(state.arena)
			vs.GetReadClock().Range(func(tid, clock uint32) bool {
				access := &state.plainReads.user.insert(state.arena, tid).access
				access.clocks[addr&7] = clock
				return true
			})
		}
		vs.SetAtomicStateOwned(unsafe.Pointer(state), retainAtomicArenaState, releaseAtomicArenaState)
	}
	mask := state.mask(addr, size)
	if mask == 0 {
		return false
	}
	state.mu.lock()
	prev, prevPC, lane, conflict := firstReportableConcurrentAtomic(state.writes, ctx, mask, pc)
	recordAtomicAccess(&state.plainReads, ctx, pc, mask, marker)
	state.mu.unlock()
	if !conflict {
		return false
	}
	pending.captureAtomic(RaceTypeWriteRead, state.base+uintptr(lane), rangeReportState{
		writePC:   prevPC,
		lifecycle: vs.GetLifecycleID(),
	}, prev, ctx.GetEpoch(), pc, pc, prevPC)
	return true
}

// captureAtomicWriteLocked detects an ordinary write conflicting with prior
// atomic reads or writes without reporting under detector locks.
func (d *Detector) captureAtomicWriteLocked(addr, size uintptr, vs *shadowmem.VarState, ctx *goroutine.RaceContext, pc uintptr, pending *pendingRangeRace) bool {
	state := existingAtomicStateLocked(vs)
	if state == nil {
		return false
	}
	mask := state.mask(addr, size)
	if mask == 0 {
		return false
	}
	state.mu.lock()
	// Closing the AtomicFastPath gate before this ordinary transaction entered
	// drained every lock-free load update. Fold their exact latest per-TID
	// witnesses into the canonical map before conflict selection.
	state.refreshReadFrontiers(mask)
	state.pruneReadFrontiers(ctx, mask)
	// An ordinary write is the latest modification for these lanes even when
	// it races with an earlier atomic access. Retire the old release before
	// either conflict path returns so a later atomic acquire cannot resurrect
	// synchronization through the superseded atomic value.
	state.retireReleases(mask)
	recordAtomicAccess(&state.plainWrites, ctx, pc, mask, false)
	if prev, prevPC, lane, conflict := firstReportableConcurrentAtomic(state.writes, ctx, mask, pc); conflict {
		clearAtomicHistoryMask(&state.plainReads, mask)
		state.mu.unlock()
		pending.captureAtomic(RaceTypeWriteWrite, state.base+uintptr(lane), rangeReportState{
			writePC:   prevPC,
			lifecycle: vs.GetLifecycleID(),
		}, prev, ctx.GetEpoch(), pc, pc, prevPC)
		return true
	}
	if prev, prevPC, lane, conflict := firstReportableConcurrentAtomic(state.reads, ctx, mask, pc); conflict {
		clearAtomicHistoryMask(&state.plainReads, mask)
		state.mu.unlock()
		pending.captureAtomic(RaceTypeReadWrite, state.base+uintptr(lane), rangeReportState{
			readPC:    prevPC,
			lifecycle: vs.GetLifecycleID(),
		}, prev, ctx.GetEpoch(), pc, pc, prevPC)
		return true
	}
	clearAtomicHistoryMask(&state.plainReads, mask)
	state.mu.unlock()
	return false
}
