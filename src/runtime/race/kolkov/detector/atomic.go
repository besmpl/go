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
var internalRMWPCClassCache [atomicPCClassCacheSlots]atomicPCClassCacheEntry

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

// atomicInternalRMWFastPC identifies internal/sync Mutex's private atomics.
// RWMutex and WaitGroup already bracket their implementation atomics with
// race.Disable and reach the numeric bridge through sync/atomic method wrappers,
// so neither family belongs in this PC classifier.
func atomicInternalRMWFastPC(pc uintptr) bool {
	if pc <= 0x10000 {
		return false
	}
	entry := &internalRMWPCClassCache[(pc>>4)&(atomicPCClassCacheSlots-1)]
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

// AtomicInternalRMWPC reports whether pc belongs to an internal/sync Mutex
// atomic whose synchronization is modeled by Mutex's explicit race
// Acquire/Release annotations. Access history is still retained.
func AtomicInternalRMWPC(pc uintptr) bool {
	return atomicInternalRMWFastPC(pc)
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

const (
	atomicReleasePromotionCoordinateThreshold = 64
	atomicReleaseSnapshotCheckPeriod          = 64
)

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

// atomicRelease starts with an exact canonical checkpoint plus a bounded suffix
// of monotonic point updates. Fragmented long-lived streams promote to an exact
// immutable causal lineage. Once refs reaches zero, atomicState may recycle the
// object and its buffers, but every reuse receives a new non-ABA stream.
type atomicRelease struct {
	arena    *AtomicHistoryArena
	runs     *atomicReleaseRange
	retired  *atomicReleaseRange
	snapshot *vectorclock.ClockSnapshot
	// lineage is the authoritative exact release image after promotion. view
	// owns one pin of its current version; the lineage itself owns the open
	// segment independently. Canonical ranges remain a dominated migration
	// checkpoint and are not updated on the promoted steady path.
	lineage *vectorclock.ClockLineage
	view    vectorclock.CausalView
	// imports retain unrelated immutable roots which are part of the current
	// promoted release image. Together with view they form one exact logical
	// release; version covers changes to any member of this composite.
	imports [vectorclock.CausalRootCapacity - 1]vectorclock.CausalView
	importN uint8
	// deferred is the exact replaceable weak-publication checkpoint. It keeps
	// immutable bases, roots, and a shared COW dense frontier structural rather
	// than rebuilding the primary lineage's flat anchor.
	deferred *vectorclock.ReleaseProjection
	deltas   [atomicReleaseDeltaCapacity]atomicReleaseDelta
	freeNext *atomicRelease
	stream   uint64
	version  uint64
	// structureVersion changes only when non-primary promoted-release state
	// changes. A strong context binding at the same structure version already
	// owns those immutable components and need only advance the primary view.
	structureVersion uint32
	refs             uint8
	deltaN           uint8
	deltaAt          uint8
}

// atomicReleaseCausalSet returns borrowed views for the exact current promoted
// release. The release transaction lock keeps their ownership alive. Only the
// returned prefix is initialized; every consumer is count-bounded so the hot
// publication path need not clear unused pointer-bearing slots.
func atomicReleaseCausalSet(release *atomicRelease, dst *[vectorclock.CausalRootCapacity]vectorclock.CausalView) int {
	if release == nil || !release.view.Valid() {
		return 0
	}
	dst[0] = release.view
	n := 1 + int(release.importN)
	copy(dst[1:n], release.imports[:release.importN])
	return n
}

func clearAtomicReleaseImports(release *atomicRelease) {
	for i := uint8(0); i < release.importN; i++ {
		release.imports[i].Release()
	}
	clear(release.imports[:])
	release.importN = 0
}

func clearAtomicReleaseDeferred(release *atomicRelease) {
	if release == nil || release.deferred == nil {
		return
	}
	release.deferred.Release()
	release.deferred = nil
}

// addAtomicReleaseRoot normalizes one borrowed root into roots. False means
// that the exact union exceeds the fixed promoted-release capacity.
func addAtomicReleaseRoot(roots *[vectorclock.CausalRootCapacity]vectorclock.CausalView, n *int, incoming vectorclock.CausalView) bool {
	if !incoming.Valid() {
		return true
	}
	for i := 0; i < *n; i++ {
		if !roots[i].SameFamily(incoming) {
			continue
		}
		if incoming.Version() > roots[i].Version() {
			roots[i] = incoming
		}
		return true
	}
	if *n == len(roots) {
		return false
	}
	roots[*n] = incoming
	*n = *n + 1
	return true
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
	mu spinlock
	// rmwQueue serializes public-RMW waiter registration with exact-fast
	// ownership transfer. rmwOwner is true while mu is owned by an exact-fast
	// transaction, including while ownership is reserved for one woken waiter.
	// A waiter retains its AtomicFastPath in its token while sleeping, which
	// freezes both this state and the descriptor generation.
	rmwQueue spinlock
	rmwSema  uint32
	// rmwSpinnerEpoch is a one-bit doorbell. The retained spinner waits on the
	// user G while it is zero; every exact-fast owner completion stores one.
	// Resume consumes that one under rmwQueue before competing for mu again.
	rmwSpinnerEpoch    uint32
	rmwWaiters         uint32
	rmwSpinnerTID      uint32
	rmwSpinnerMisses   uint8
	rmwCohortOps       uint16
	rmwForceSpinner    bool
	rmwPromoteParked   bool
	rmwPromotedOwner   bool
	rmwWakeCompetitors uint32
	rmwGrantTID        uint32
	rmwOwner           bool
	rmwOwnerPolite     bool
	rmwGranted         bool
	rmwRecentOwners    [4]uint32
	rmwRecentAt        uint8
	rmwRecentN         uint8
	base               uintptr
	reads              atomicHistory
	writes             atomicHistory
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
	// directRMWRelease is the exact release proven before hardware. A valid
	// directRMWAppend owns a promoted lineage's writer guard across hardware;
	// PrepareAppend performed every allocation/rotation before that boundary.
	directRMWRelease *atomicRelease
	directRMWAppend  vectorclock.PreparedCausalAppend
}

// rmwCohortLimit bounds the number of public exact-fast completions which may
// pass a registered parked waiter while one retained spinner competes with new
// arrivals. The owner-side count is deliberately independent of spinner
// scheduling: a descheduled spinner cannot let the competitive cohort starve
// the semaphore queue indefinitely.
//
// A promotion crosses the user-goroutine scheduler and is several orders of
// magnitude more expensive than the short exact-fast transaction it protects.
// Limiting that tax to at most one completion in 4096 preserves a deterministic
// progress bound without turning a large waiter population into a scheduler
// benchmark. This is an operation bound, not a time or workload heuristic.
const (
	rmwCohortLimit      = uint16(4096)
	rmwPatientTIDSpread = uint32(128)
	// Reuse the finite progress bound instead of introducing an independent
	// workload-sized threshold. At this population, owner TID locality no
	// longer describes the runnable exact-address cohort reliably.
	rmwPatientWaiters = uint32(rmwCohortLimit)
)

// publicRMWPatientLocked classifies only the retry delay. A waiter population
// which reaches the existing finite progress bound is direct evidence of a
// massive exact-address cohort. Below it, a wide span between the spinner and
// the current owner is an allocation-free proxy. Using only the current owner
// prevents sequential small cohorts with monotonically increasing TIDs from
// being misclassified as one large cohort. The marker never changes admission,
// ownership, modification order, or the 64-miss/4096-completion progress
// bounds.
func (s *atomicState) publicRMWPatientLocked(tid uint32) bool {
	if s.rmwWaiters >= rmwPatientWaiters {
		return true
	}
	if s.rmwRecentN == 0 {
		return false
	}
	index := (s.rmwRecentAt + uint8(len(s.rmwRecentOwners)) - 1) % uint8(len(s.rmwRecentOwners))
	owner := s.rmwRecentOwners[index]
	if owner > tid {
		return owner-tid >= rmwPatientTIDSpread
	}
	return tid-owner >= rmwPatientTIDSpread
}

func (s *atomicState) setPublicRMWPatientTokenLocked(tid uint32, token *AtomicToken) {
	if token == nil {
		return
	}
	token[2] = nil
	if s.publicRMWPatientLocked(tid) {
		// token[0] already retains the exact capability and therefore the state;
		// repeating it is a GC-visible, allocation-free scheduling marker.
		token[2] = token[0]
	}
}

func (s *atomicState) setPublicRMWOwnerLocked(tid uint32) {
	repeat := false
	for i := uint8(0); i < s.rmwRecentN; i++ {
		if s.rmwRecentOwners[i] == tid {
			repeat = true
			break
		}
	}
	s.rmwRecentOwners[s.rmwRecentAt] = tid
	s.rmwRecentAt = (s.rmwRecentAt + 1) % uint8(len(s.rmwRecentOwners))
	if s.rmwRecentN < uint8(len(s.rmwRecentOwners)) {
		s.rmwRecentN++
	}
	s.rmwOwner = true
	s.rmwOwnerPolite = repeat
}

func (s *atomicState) beginExactFastOwner(tid uint32) {
	s.rmwQueue.lock()
	if s.rmwOwner || s.rmwGranted {
		s.rmwQueue.unlock()
		atomicRuntimeThrow("race detector invalid exact atomic owner state")
	}
	s.setPublicRMWOwnerLocked(tid)
	s.rmwQueue.unlock()
}

// tryBeginPublicRMW either acquires the exact-fast lock, registers one retained
// waiter behind an exact-fast owner, or reports a clean retry behind a holder
// which cannot participate in direct handoff.
func (s *atomicState) tryBeginPublicRMW(fast *shadowmem.AtomicFastPath, token *AtomicToken, tid uint32) (acquired, spin, polite bool, park *uint32) {
	if s.mu.tryLock() {
		s.beginExactFastOwner(tid)
		return true, false, false, nil
	}
	if !s.rmwQueue.tryLock() {
		return false, false, false, nil
	}
	if s.mu.tryLock() {
		if s.rmwOwner || s.rmwGranted {
			s.rmwQueue.unlock()
			atomicRuntimeThrow("race detector invalid exact atomic owner acquisition")
		}
		s.setPublicRMWOwnerLocked(tid)
		s.rmwQueue.unlock()
		return true, false, false, nil
	}
	if !s.rmwOwner {
		s.rmwQueue.unlock()
		return false, false, false, nil
	}
	if s.rmwSpinnerTID == 0 && !s.rmwGranted && !s.rmwPromoteParked {
		if iatomic.Load(&s.rmwSpinnerEpoch) != 0 || s.rmwSpinnerMisses != 0 || s.rmwForceSpinner {
			s.rmwQueue.unlock()
			atomicRuntimeThrow("race detector invalid atomic RMW spinner enrollment")
		}
		s.rmwSpinnerTID = tid
		token[0] = unsafe.Pointer(fast)
		s.setPublicRMWPatientTokenLocked(tid, token)
		park = &s.rmwSpinnerEpoch
		s.rmwQueue.unlock()
		return false, true, s.rmwOwnerPolite, park
	}
	if s.rmwWaiters == ^uint32(0) {
		s.rmwQueue.unlock()
		atomicRuntimeThrow("race detector atomic RMW waiter overflow")
	}
	s.rmwWaiters++
	token[0] = unsafe.Pointer(fast)
	s.setPublicRMWPatientTokenLocked(tid, token)
	park = &s.rmwSema
	s.rmwQueue.unlock()
	return false, false, false, park
}

func (s *atomicState) retryPublicRMWSpinner(tid uint32, token *AtomicToken) (acquired, spin, polite bool, park *uint32) {
	s.rmwQueue.lock()
	s.setPublicRMWPatientTokenLocked(tid, token)
	if s.rmwGranted {
		// A semaphore competitor released by a promoted spinner may be runnable
		// while a later cohort reserves mu for a different spinner. The wake is
		// still valid, but it cannot consume that TID-specific grant. Put its
		// retained capability back on the parked queue; the reserved owner will
		// hand mu to one parked waiter when it completes.
		if s.rmwGrantTID != 0 && s.rmwGrantTID != tid {
			if s.rmwWakeCompetitors == 0 {
				s.rmwQueue.unlock()
				atomicRuntimeThrow("race detector invalid forced atomic RMW competitor")
			}
			s.rmwWakeCompetitors--
			if s.rmwWaiters == ^uint32(0) {
				s.rmwQueue.unlock()
				atomicRuntimeThrow("race detector atomic RMW waiter overflow")
			}
			s.rmwWaiters++
			park = &s.rmwSema
			s.rmwQueue.unlock()
			return false, false, false, park
		}
		if !s.rmwOwner || s.mu.state.Load() == 0 {
			s.rmwQueue.unlock()
			atomicRuntimeThrow("race detector invalid atomic RMW grant")
		}
		promoted := false
		if s.rmwGrantTID != 0 {
			if s.rmwGrantTID != tid || s.rmwSpinnerTID != tid || iatomic.Load(&s.rmwSpinnerEpoch) != 1 {
				s.rmwQueue.unlock()
				atomicRuntimeThrow("race detector invalid forced atomic RMW grant")
			}
			iatomic.Store(&s.rmwSpinnerEpoch, 0)
			s.rmwSpinnerTID = 0
			s.rmwSpinnerMisses = 0
			s.rmwCohortOps = 0
			s.rmwForceSpinner = false
			promoted = s.rmwPromoteParked
			s.rmwPromoteParked = false
		} else if s.rmwSpinnerTID != 0 || iatomic.Load(&s.rmwSpinnerEpoch) != 0 {
			s.rmwQueue.unlock()
			atomicRuntimeThrow("race detector invalid parked atomic RMW grant")
		}
		s.rmwGranted = false
		s.rmwGrantTID = 0
		s.setPublicRMWOwnerLocked(tid)
		s.rmwPromotedOwner = promoted
		s.rmwQueue.unlock()
		return true, false, false, nil
	}
	if s.rmwSpinnerTID != tid {
		if s.rmwWakeCompetitors == 0 {
			s.rmwQueue.unlock()
			atomicRuntimeThrow("race detector invalid atomic RMW resume")
		}
		s.rmwWakeCompetitors--
		if s.mu.tryLock() {
			if s.rmwOwner || s.rmwGranted {
				s.rmwQueue.unlock()
				atomicRuntimeThrow("race detector invalid atomic RMW competitor acquisition")
			}
			s.setPublicRMWOwnerLocked(tid)
			s.rmwQueue.unlock()
			return true, false, false, nil
		}
		if s.rmwSpinnerTID == 0 && !s.rmwGranted && !s.rmwPromoteParked {
			if iatomic.Load(&s.rmwSpinnerEpoch) != 0 || s.rmwSpinnerMisses != 0 || s.rmwForceSpinner {
				s.rmwQueue.unlock()
				atomicRuntimeThrow("race detector invalid atomic RMW competitor enrollment")
			}
			s.rmwSpinnerTID = tid
			park = &s.rmwSpinnerEpoch
			s.rmwQueue.unlock()
			return false, true, s.rmwOwnerPolite, park
		}
		if s.rmwWaiters == ^uint32(0) {
			s.rmwQueue.unlock()
			atomicRuntimeThrow("race detector atomic RMW waiter overflow")
		}
		s.rmwWaiters++
		park = &s.rmwSema
		s.rmwQueue.unlock()
		return false, false, false, park
	}
	if iatomic.Load(&s.rmwSpinnerEpoch) != 1 {
		s.rmwQueue.unlock()
		atomicRuntimeThrow("race detector invalid atomic RMW spinner")
	}
	park = &s.rmwSpinnerEpoch
	if s.mu.tryLock() {
		if s.rmwOwner {
			s.rmwQueue.unlock()
			atomicRuntimeThrow("race detector invalid atomic RMW spinner acquisition")
		}
		iatomic.Store(&s.rmwSpinnerEpoch, 0)
		s.rmwSpinnerTID = 0
		s.rmwSpinnerMisses = 0
		s.rmwCohortOps = 0
		s.rmwForceSpinner = false
		s.setPublicRMWOwnerLocked(tid)
		s.rmwQueue.unlock()
		return true, false, false, nil
	}
	// The doorbell corresponds to one completed ownership epoch. Consume it
	// before counting the steal so repeated Resume calls cannot inflate misses.
	iatomic.Store(&s.rmwSpinnerEpoch, 0)
	if s.rmwSpinnerMisses < 64 {
		s.rmwSpinnerMisses++
	}
	if s.rmwSpinnerMisses == 64 {
		s.rmwForceSpinner = true
	}
	s.rmwQueue.unlock()
	return false, true, s.rmwOwnerPolite, park
}

// endExactFastOwner reserves the logically-held mu for exactly one waiter.
// The returned semaphore must be released after leaving systemstack. With no
// registered waiter, it performs the ordinary unlock.
func (s *atomicState) endExactFastOwner() (wake *uint32) {
	s.rmwQueue.lock()
	if s.rmwGranted || s.mu.state.Load() == 0 {
		s.rmwQueue.unlock()
		atomicRuntimeThrow("race detector invalid exact atomic owner completion")
	}
	// A forced spinner with parked waiters becomes a one-operation bridge back
	// to unlocked competition. Do not reserve mu for a parked waiter here: that
	// would make every following owner directly grant the next semaphore waiter
	// before a spinner can enroll, degenerating into one scheduler handoff per
	// hardware operation. If nobody enrolled as spinner during this operation,
	// wake one waiter as an unreserved competitor. Its Resume either acquires mu,
	// becomes the spinner behind a new owner, or rejoins the parked queue.
	if s.rmwPromotedOwner {
		s.rmwPromotedOwner = false
		if s.rmwSpinnerTID == 0 {
			s.rmwOwner = false
			s.rmwOwnerPolite = false
			s.rmwCohortOps = 0
			s.mu.unlock()
			if s.rmwWaiters != 0 {
				s.rmwWaiters--
				if s.rmwWakeCompetitors == ^uint32(0) {
					s.rmwQueue.unlock()
					atomicRuntimeThrow("race detector atomic RMW competitor overflow")
				}
				s.rmwWakeCompetitors++
				wake = &s.rmwSema
			}
			s.rmwQueue.unlock()
			return wake
		}
	}
	if s.rmwSpinnerTID != 0 {
		if s.rmwWaiters != 0 {
			if s.rmwCohortOps < rmwCohortLimit {
				s.rmwCohortOps++
			}
			if s.rmwCohortOps == rmwCohortLimit {
				s.rmwForceSpinner = true
			}
		} else {
			s.rmwCohortOps = 0
		}
		if s.rmwForceSpinner {
			s.rmwOwner = true
			s.rmwGranted = true
			s.rmwGrantTID = s.rmwSpinnerTID
			s.rmwPromoteParked = s.rmwWaiters != 0
			iatomic.Store(&s.rmwSpinnerEpoch, 1)
			s.rmwQueue.unlock()
			return nil
		}
		s.rmwOwner = false
		s.rmwOwnerPolite = false
		s.mu.unlock()
		iatomic.Store(&s.rmwSpinnerEpoch, 1)
		s.rmwQueue.unlock()
		return nil
	}
	if !s.rmwOwner {
		s.mu.unlock()
		s.rmwQueue.unlock()
		return nil
	}
	if s.rmwWaiters != 0 {
		s.rmwWaiters--
		s.rmwGranted = true
		s.rmwCohortOps = 0
		wake = &s.rmwSema
		s.rmwQueue.unlock()
		return wake
	}
	s.rmwOwner = false
	s.rmwOwnerPolite = false
	s.rmwCohortOps = 0
	s.mu.unlock()
	s.rmwQueue.unlock()
	return nil
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
	if s.directRMWRelease != nil || s.directRMWAppend.Valid() {
		atomicRuntimeThrow("race detector retained direct RMW publication proof")
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
	frontier := &s.reads.user
	if internal {
		frontier = &s.reads.internal
	}
	// This locked load is the exact semantic point at which ctx.C may replace
	// older folded reads. Apply the canonical history's geometric compaction
	// policy before moving the new witness to its lock-free frontier. Deferring
	// the proof until cache eviction would be unsound: synchronization after the
	// load may add foreign clocks which did not happen before this read.
	if _, exists := frontier.find(ctx.TID); !exists && frontier.shouldPruneBeforeInsert() {
		pruneAtomicAccess(&s.reads, ctx, mask, internal)
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
	if release.lineage != nil {
		// Promoted imports deliberately use the conservative foreign path. A
		// lineage point lookup cannot prove that no other coordinate exists,
		// and scanning it would recreate the quadratic work it replaces.
		return true
	}
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

const atomicReleaseImportInlineRanges = 32

// joinAtomicReleaseRanges imports one canonical release checkpoint in one
// structural merge. Splitting a long linked checkpoint into fixed-size calls
// makes each call re-merge the destination built by the preceding call, which
// turns a fragmented release into quadratic allocation and copying. The
// temporary slice owns only scalar range values; it never exposes arena-node
// pointers beyond the transaction which keeps release alive.
func joinAtomicReleaseRanges(clock *vectorclock.VectorClock, head *atomicReleaseRange) {
	count := 0
	for run := head; run != nil; run = run.next {
		count++
	}
	if count == 0 {
		return
	}
	var inline [atomicReleaseImportInlineRanges]vectorclock.FiniteRange
	var ranges []vectorclock.FiniteRange
	if count <= len(inline) {
		ranges = inline[:count]
	} else {
		ranges = make([]vectorclock.FiniteRange, count)
	}
	for i, run := 0, head; run != nil; i, run = i+1, run.next {
		ranges[i] = vectorclock.FiniteRange{First: run.first, Last: run.last, Clock: run.clock}
	}
	clock.JoinCanonicalRanges(ranges)
}

// retireAtomicReleaseRanges applies every +infinity interval in one merge for
// the same reason as joinAtomicReleaseRanges. Retirement follows the finite
// join so it remains immutable and removes every covered finite coordinate.
func retireAtomicReleaseRanges(clock *vectorclock.VectorClock, head *atomicReleaseRange) {
	count := 0
	for run := head; run != nil; run = run.next {
		count++
	}
	if count == 0 {
		return
	}
	var inline [atomicReleaseImportInlineRanges]vectorclock.RetiredRange
	var ranges []vectorclock.RetiredRange
	if count <= len(inline) {
		ranges = inline[:count]
	} else {
		ranges = make([]vectorclock.RetiredRange, count)
	}
	for i, run := 0, head; run != nil; i, run = i+1, run.next {
		ranges[i] = vectorclock.RetiredRange{First: run.first, Last: run.last}
	}
	clock.RetireRanges(ranges)
}

func joinAtomicReleaseCausalSet(clock *vectorclock.VectorClock, release *atomicRelease) {
	if release != nil && release.deferred != nil {
		release.deferred.JoinInto(clock)
	}
	var roots [vectorclock.CausalRootCapacity]vectorclock.CausalView
	n := atomicReleaseCausalSet(release, &roots)
	if clock.TryJoinCausalSet(&roots, n) {
		return
	}
	// Capacity overflow is a cold exact path. JoinCausal materializes as needed
	// rather than discarding any member of the composite release.
	for i := 0; i < n; i++ {
		clock.JoinCausal(roots[i])
	}
}

func atomicReleaseDominatesResidual(clock *vectorclock.VectorClock, release *atomicRelease, roots *[vectorclock.CausalRootCapacity]vectorclock.CausalView, n int, ownTID uint32) bool {
	if release != nil && release.deferred != nil {
		return clock.ResidualLessOrEqualReleaseProjection(roots, n, release.deferred, ownTID)
	}
	return clock.ResidualLessOrEqualCausalSet(roots, n, ownTID)
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
		seenVersion, seenStructure, wasStrong, cacheHit := ctx.LookupAtomicReleaseStructure(unsafe.Pointer(release), release.stream, membership)
		if cacheHit && seenVersion == release.version {
			if release.lineage != nil && !wasStrong {
				var roots [vectorclock.CausalRootCapacity]vectorclock.CausalView
				n := atomicReleaseCausalSet(release, &roots)
				if atomicReleaseDominatesResidual(ctx.C, release, &roots, n, ctx.TID) {
					// Another synchronization invalidated the generation proof but
					// added no residual metadata beyond this unchanged release. Restore
					// the exact strong binding without joining or advancing anything.
					ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, true)
				}
			}
			continue
		}

		if release.lineage != nil {
			if cacheHit && wasStrong && seenStructure == release.structureVersion {
				// A strong prior binding with unchanged non-primary structure
				// already owns the deferred projection and imported roots. Only
				// the append-only primary view can have advanced, so importing that
				// view is the exact delta instead of replaying the wide base.
				ctx.NoteForeignImport()
				ctx.C.JoinCausal(release.view)
				ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, true)
				continue
			}
			var roots [vectorclock.CausalRootCapacity]vectorclock.CausalView
			n := atomicReleaseCausalSet(release, &roots)
			strong := wasStrong || atomicReleaseDominatesResidual(ctx.C, release, &roots, n, ctx.TID)
			// Invalidate every other strong publication proof before recording
			// this exact release. This conservative ordering avoids a lineage
			// scan while keeping a current release proof strong for the following
			// RMW publication only when the exact pre-import residual was already
			// dominated by this release.
			ctx.NoteForeignImport()
			joinAtomicReleaseCausalSet(ctx.C, release)
			ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, strong)
			continue
		}

		freshOnlyOwn := ctx.FreshOnlyOwn()
		if cacheHit && seenVersion < release.version {
			if complete, foreign := release.replay(ctx, seenVersion); complete {
				if foreign {
					ctx.NoteForeignImport()
				}
				ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, wasStrong)
				continue
			}
		}

		if release.snapshot != nil {
			ctx.C.JoinSnapshot(release.snapshot)
		} else {
			joinAtomicReleaseRanges(ctx.C, release.runs)
			retireAtomicReleaseRanges(ctx.C, release.retired)
		}
		if releaseHasForeignMetadata(release, ctx.TID) {
			ctx.NoteForeignImport()
		}
		ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, wasStrong || freshOnlyOwn)
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
	if release.lineage != nil {
		atomicRuntimeThrow("race detector rebuilt promoted atomic release")
	}
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
	if release.snapshot != nil {
		// Canonical arena ranges remain authoritative. Rebuild the immutable
		// selector from those ranges rather than retaining any incidental
		// metadata the publishing context may have acquired after its cached
		// proof was recorded.
		release.snapshot = freezeAtomicReleaseCanonical(release)
	} else {
		maybePromoteAtomicRelease(release, ctx)
	}
}

// initializeAtomicReleaseComposite captures a context which already consists
// of a small retained causal-root set plus its own live coordinate without
// materializing those shared roots into eager per-TID ranges. This is the
// important second-synchronization-object case: a goroutine which acquired one
// wide atomic release can publish another exact release in O(root count)
// rather than copying the first release's entire cohort.
//
// False requests the existing exact canonical snapshot fallback. Every bound
// here is only an accelerator bound: no root or residual coordinate is dropped.
func initializeAtomicReleaseComposite(release *atomicRelease, ctx *goroutine.RaceContext) bool {
	if release == nil || ctx == nil || ctx.C == nil || release.lineage != nil || release.snapshot != nil {
		return false
	}
	var roots [vectorclock.CausalRootCapacity]vectorclock.CausalView
	n := ctx.C.BorrowCausalRoots(&roots)
	// One slot is required for this release's independently appendable primary
	// lineage. A full context root set takes the canonical exact fallback.
	if n == 0 || n >= vectorclock.CausalRootCapacity ||
		!ctx.C.ResidualLessOrEqualCausalSet(&roots, n, ctx.TID) {
		return false
	}

	var imports [vectorclock.CausalRootCapacity - 1]vectorclock.CausalView
	for i := 0; i < n; i++ {
		duplicate, ok := roots[i].Duplicate()
		if !ok {
			for j := 0; j < i; j++ {
				imports[j].Release()
			}
			return false
		}
		imports[i] = duplicate
	}

	owned := vectorclock.New()
	owned.Set(ctx.TID, uint32(ctx.GetEpoch()))
	lineage := vectorclock.NewClockLineage(owned)
	owned.Release()
	view := lineage.Pin()
	if !view.Valid() {
		lineage.Release()
		for i := 0; i < n; i++ {
			imports[i].Release()
		}
		return false
	}

	// All fallible construction and retains completed. Replace the dominated
	// canonical checkpoint, then attach the new primary view to its publisher.
	release.arena.freeRangeList(release.runs)
	release.arena.freeRangeList(release.retired)
	release.runs, release.retired = nil, nil
	release.lineage, release.view = lineage, view
	copy(release.imports[:n], imports[:n])
	release.importN = uint8(n)
	bumpAtomicReleaseStructureVersion(release)
	release.deltaN, release.deltaAt = 0, 0
	if !ctx.C.TryJoinCausal(view) {
		atomicRuntimeThrow("race detector failed to attach composite atomic release")
	}
	return true
}

func freezeAtomicReleaseCanonical(release *atomicRelease) *vectorclock.ClockSnapshot {
	clock := vectorclock.NewFromPool()
	joinAtomicReleaseRanges(clock, release.runs)
	retireAtomicReleaseRanges(clock, release.retired)
	snapshot := clock.Freeze()
	clock.Release()
	return snapshot
}

func atomicReleaseHasPromotionCoordinates(release *atomicRelease) bool {
	remaining := uint64(atomicReleasePromotionCoordinateThreshold)
	consume := func(first, last uint32) bool {
		width := uint64(last) - uint64(first) + 1
		if width >= remaining {
			return true
		}
		remaining -= width
		return false
	}
	for run := release.runs; run != nil; run = run.next {
		if consume(run.first, run.last) {
			return true
		}
	}
	for run := release.retired; run != nil; run = run.next {
		if consume(run.first, run.last) {
			return true
		}
	}
	return false
}

func maybePromoteAtomicRelease(release *atomicRelease, ctx *goroutine.RaceContext) {
	_ = ctx // retained in the private selector seam used by focused tests
	if release.lineage != nil || release.snapshot != nil || release.version%atomicReleaseSnapshotCheckPeriod != 0 {
		return
	}
	if atomicReleaseHasPromotionCoordinates(release) {
		// Build the immutable lineage image from the exact canonical ranges,
		// then drop the temporary persistent snapshot. Promoted point updates
		// never rebuild a persistent tree.
		snapshot := freezeAtomicReleaseCanonical(release)
		release.lineage = vectorclock.NewClockLineageFromSnapshot(snapshot)
		release.view = release.lineage.Pin()
		if !release.view.Valid() {
			atomicRuntimeThrow("race detector failed to pin promoted atomic release")
		}
		bumpAtomicReleaseStructureVersion(release)
		release.deltaN, release.deltaAt = 0, 0
	}
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

func bumpAtomicReleaseStructureVersion(release *atomicRelease) {
	if release.structureVersion == ^uint32(0) {
		atomicRuntimeThrow("race detector atomic-release structure version overflow")
	}
	release.structureVersion++
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

func (s *atomicState) maskHasPromotedRelease(mask uint8) bool {
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) != 0 {
			if release := s.releases[lane]; release != nil && release.lineage != nil {
				return true
			}
		}
	}
	return false
}

// publishPromotedComposite attempts the exact bounded weak-publication path.
// It imports unrelated context roots without rebuilding the primary lineage.
// False requests the existing exact reanchor fallback.
func publishPromotedComposite(release *atomicRelease, ctx *goroutine.RaceContext) bool {
	var roots [vectorclock.CausalRootCapacity]vectorclock.CausalView
	n := atomicReleaseCausalSet(release, &roots)
	var contextRoots [vectorclock.CausalRootCapacity]vectorclock.CausalView
	contextN := ctx.C.BorrowCausalRoots(&contextRoots)
	for i := 0; i < contextN; i++ {
		if release.deferred != nil && release.deferred.DominatesCausal(contextRoots[i]) {
			continue
		}
		if !addAtomicReleaseRoot(&roots, &n, contextRoots[i]) {
			return false
		}
	}
	// Causal roots alone are insufficient: mutable/base finite coordinates and
	// retirement metadata must also be contained in the proposed union.
	if !atomicReleaseDominatesResidual(ctx.C, release, &roots, n, ctx.TID) {
		return false
	}

	// Retain every new family before mutating the release. Existing imported
	// ownership can then advance in place, making the commit failure-free.
	var additions [vectorclock.CausalRootCapacity - 1]vectorclock.CausalView
	var additionSet [vectorclock.CausalRootCapacity - 1]bool
	for i := 1; i < n; i++ {
		found := false
		for j := uint8(0); j < release.importN; j++ {
			if release.imports[j].SameFamily(roots[i]) {
				found = true
				break
			}
		}
		if found {
			continue
		}
		owned, ok := roots[i].Duplicate()
		if !ok {
			for j := range additions {
				additions[j].Release()
			}
			return false
		}
		additions[i-1], additionSet[i-1] = owned, true
	}

	prepared, _ := release.lineage.PrepareAppend(ctx.TID, uint32(ctx.GetEpoch()))
	if !prepared.Valid() {
		for i := range additions {
			additions[i].Release()
		}
		atomicRuntimeThrow("race detector failed to prepare promoted atomic release append")
	}

	changed := false
	structureChanged := false
	for i := 1; i < n; i++ {
		advanced := false
		for j := uint8(0); j < release.importN; j++ {
			if !release.imports[j].SameFamily(roots[i]) {
				continue
			}
			if roots[i].Version() > release.imports[j].Version() {
				if !release.imports[j].AdvanceTo(roots[i]) {
					prepared.Abort()
					atomicRuntimeThrow("race detector failed to advance promoted atomic release import")
				}
				changed = true
				structureChanged = true
			}
			advanced = true
			break
		}
		if advanced {
			continue
		}
		if !additionSet[i-1] || release.importN == uint8(len(release.imports)) {
			prepared.Abort()
			atomicRuntimeThrow("race detector lost prepared atomic release import")
		}
		release.imports[release.importN] = additions[i-1]
		additions[i-1] = vectorclock.CausalView{}
		release.importN++
		changed = true
		structureChanged = true
	}
	for i := range additions {
		additions[i].Release()
	}

	oldPrimaryVersion := release.view.Version()
	lineageVersion, appended := prepared.CommitOwned(&release.view)
	if !release.view.Valid() || release.view.Version() != lineageVersion {
		atomicRuntimeThrow("race detector lost promoted atomic release view")
	}
	changed = changed || appended || lineageVersion != oldPrimaryVersion

	// The source context already owns every family in the normalized union.
	// Advance it to the release's newly appended primary view without lowering
	// its independently pinned imported histories.
	n = atomicReleaseCausalSet(release, &roots)
	if !ctx.C.TryJoinCausalSet(&roots, n) {
		// The residual proof may have discharged context families through the
		// deferred projection without removing their older inline pins. If those
		// pins fill the fixed root set, advance through the unrestricted exact
		// path rather than treating representation capacity as a semantic error.
		for i := 0; i < n; i++ {
			ctx.C.JoinCausal(roots[i])
		}
	}
	if changed {
		if structureChanged {
			bumpAtomicReleaseStructureVersion(release)
		}
		bumpAtomicReleaseVersion(release)
	}
	return true
}

// publishRelease uses a point update only from a current strong cache proof. A
// weak current proof permits a monotonic full checkpoint in the same stream;
// every other store replaces the release with a new stream and exact snapshot.
func (s *atomicState) publishRelease(ctx *goroutine.RaceContext, mask uint8) {
	if release, membership := s.exactReleaseForMask(mask); release != nil {
		seenVersion, strong, ok := ctx.LookupAtomicRelease(unsafe.Pointer(release), release.stream, membership)
		if ok && seenVersion == release.version {
			pointProof := strong
			if pointProof {
				// RaceContext keeps Epoch equal to C[TID]. Reading the cached
				// epoch avoids a sparse-vector search for the owning coordinate
				// on every strong release update.
				clock := uint32(ctx.GetEpoch())
				if release.lineage != nil {
					lineageVersion, changed := release.lineage.AppendOwned(&release.view, ctx.TID, clock)
					if !release.view.Valid() || release.view.Version() != lineageVersion {
						atomicRuntimeThrow("race detector lost promoted atomic release view")
					}
					if changed {
						bumpAtomicReleaseVersion(release)
					}
					// The appended view is dominated by ctx: it adds only ctx's
					// already-owned coordinate to the exact release it acquired.
					// Advance ctx's pin too, otherwise its own Get would walk an
					// ever-longer historical same-TID chain after every publication.
					ctx.C.JoinCausal(release.view)
				} else if release.snapshot != nil {
					// Once promoted, the immutable snapshot is the authoritative
					// current release. Updating it directly avoids a linear search of
					// the checkpoint ranges on every random-TID RMW. The arena ranges
					// remain an older dominated checkpoint; a weak publication rebuilds
					// both representations from the complete source context.
					next := release.snapshot.PointMax(ctx.TID, clock)
					if next != release.snapshot {
						release.snapshot = next
						version := bumpAtomicReleaseVersion(release)
						appendAtomicReleaseDelta(release, version, ctx.TID, clock)
					}
				} else if pointMaxAtomicRelease(release, ctx.TID, clock) {
					version := bumpAtomicReleaseVersion(release)
					appendAtomicReleaseDelta(release, version, ctx.TID, clock)
					maybePromoteAtomicRelease(release, ctx)
				}
				ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, true)
				return
			}
			if release.lineage != nil {
				if publishPromotedComposite(release, ctx) {
					ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, true)
					return
				}
				// A current weak cache entry still proves that the existing release
				// is below ctx.C; weakness only means ctx imported additional foreign
				// state. Retain that complete image structurally and replace the prior
				// projection instead of flattening the process-wide frontier into a
				// new lineage anchor. The primary lineage remains independently
				// appendable for later strong owner-only publications.
				if release.deferred == nil {
					release.deferred = new(vectorclock.ReleaseProjection)
				}
				vectorclock.RepinReleaseProjectionForPreparedOwner(ctx.C, ctx.TID, release.deferred)
				clearAtomicReleaseImports(release)
				bumpAtomicReleaseStructureVersion(release)
				bumpAtomicReleaseVersion(release)
				release.deltaN, release.deltaAt = 0, 0
				ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, true)
				return
			}
			bumpAtomicReleaseVersion(release)
			if !initializeAtomicReleaseComposite(release, ctx) {
				snapshotAtomicRelease(release, ctx)
			}
			ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, membership, true)
			return
		}
	}

	// Replacements of a promoted release stay on the scalable representation,
	// but start a new incomparable family: a non-acquiring store must not make
	// the superseded release a causal predecessor. Unpromoted addresses retain
	// the compact canonical small path.
	replaceWithLineage := s.maskHasPromotedRelease(mask)
	s.retireReleases(mask)
	release := s.allocateRelease()
	if initializeAtomicReleaseComposite(release, ctx) {
		// The shared roots and new primary lineage already form the exact source.
	} else if replaceWithLineage {
		// A replacement is intentionally incomparable with the superseded
		// stream, but need not flatten its wide source. Keep a tiny independently
		// appendable owner lineage and retain the exact source as a deferred
		// projection.
		projection := vectorclock.PinReleaseProjectionForPreparedOwner(ctx.C, ctx.TID)
		release.deferred = new(vectorclock.ReleaseProjection)
		*release.deferred = projection
		owned := vectorclock.New()
		owned.Set(ctx.TID, uint32(ctx.GetEpoch()))
		release.lineage = vectorclock.NewClockLineage(owned)
		owned.Release()
		release.view = release.lineage.Pin()
		if !release.view.Valid() {
			atomicRuntimeThrow("race detector failed to pin replacement atomic release")
		}
	} else {
		snapshotAtomicRelease(release, ctx)
	}
	release.refs = uint8(countMaskBits(mask))
	for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
		if mask&(uint8(1)<<lane) != 0 {
			s.releases[lane] = release
		}
	}
	ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, mask, true)
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

	if frontier.shouldPruneBeforeInsert() {
		pruneAtomicAccess(history, ctx, mask, internal)
	}
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
// deterministic while frontier order may choose any conflicting thread.
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
	d.atomicBegin(addr, size, ctx, acquire, false, false, true, true, -1, false, token, nil, nil, nil)
}

// AtomicBeginRMW starts an enabled read-modify-write or compare-and-swap
// transaction. An aligned, exact-mask operation may reuse or enroll a retained
// capability. Mixed-width and otherwise ineligible misses retain the general
// transaction and permanently escape any incompatible capability.
// Failed compare-and-swaps must complete through AtomicEndMode with write=false;
// every AtomicBeginRMW token must be completed with synchronize=true.
func (d *Detector) AtomicBeginRMW(addr, size uintptr, ctx *goroutine.RaceContext, acquire bool, token *AtomicToken) {
	d.atomicBegin(addr, size, ctx, acquire, true, true, true, true, 1, false, token, nil, nil, nil)
}

// AtomicBeginRMWCooperative starts the public runtime RMW transaction. It is
// identical to AtomicBeginRMW except when an exact retained capability exists
// and another public RMW owns its state lock: it retains the capability,
// retains the first contender as a user-G spinner and parks later contenders.
// The spinner and current owner may compete between operations; bounded misses
// force the existing reserved handoff. Contention with a non-handoff owner or
// the bounded registration lock remains a clean retry. Eligible misses enroll
// through canonical setup; general shapes retain the blocking path.
func (d *Detector) AtomicBeginRMWCooperative(addr, size uintptr, ctx *goroutine.RaceContext, acquire bool, token *AtomicToken) (retry, spin, polite bool, park *uint32) {
	retry = d.atomicBegin(addr, size, ctx, acquire, true, true, true, true, 1, true, token, &park, &spin, &polite)
	return retry, spin, polite, park
}

// AtomicResumeRMW consumes ownership reserved by the preceding cooperative
// Begin after the runtime has acquired its returned semaphore on the user G.
// The retained token is still the ordinary exact-fast token shape.
func (d *Detector) AtomicResumeRMW(addr, size uintptr, ctx *goroutine.RaceContext, acquire bool, token *AtomicToken) (retry, spin, polite bool, park *uint32) {
	if ctx == nil || !validAtomicAccess(addr, size) {
		atomicRuntimeThrow("race detector invalid atomic RMW resume")
	}
	fast := atomicFastToken(token)
	mask, exact := plainAtomicFastMask(addr, size)
	if fast == nil || !exact || fast.Mask() != mask || fast.Overlay() == nil {
		atomicRuntimeThrow("race detector invalid atomic RMW resume token")
	}
	ctx.ValidateClockAdvance()
	state := (*atomicState)(fast.Overlay())
	acquired, retrySpin, retryPolite, retryPark := state.retryPublicRMWSpinner(ctx.TID, token)
	if !acquired {
		return true, retrySpin, retryPolite, retryPark
	}
	state.arena.pin()
	syncMode := int8(0)
	if acquire {
		syncMode = 1
	}
	state.beginTransaction(addr, size, ctx, syncMode)
	state.beginWriter()
	if acquire {
		state.acquire(ctx, mask)
		ctx.PreflightClockAdvance()
	}
	return false, false, false, nil
}

func atomicHistoryMaskEmpty(history *atomicHistory, mask uint8) bool {
	empty := true
	check := func(frontier *atomicHistoryClass) {
		frontier.visit(func(entry *atomicHistoryEntry) bool {
			for lane := uint8(0); lane < AtomicTokenSlots; lane++ {
				if mask&(uint8(1)<<lane) != 0 && entry.access.clocks[lane] != 0 {
					empty = false
					return false
				}
			}
			return true
		})
	}
	check(&history.user)
	if empty {
		check(&history.internal)
	}
	return empty
}

// releasePointMaxCannotAllocate proves the exact strong-release update used by
// direct success is an in-place singleton update. End deliberately falls back
// before hardware when a range split or immutable-snapshot update could
// allocate, or when the periodic snapshot selector would promote this release.
func releasePointMaxCannotAllocate(release *atomicRelease, ctx *goroutine.RaceContext) bool {
	if release == nil || release.lineage != nil || release.snapshot != nil || release.version == ^uint64(0) {
		return false
	}
	clock := uint32(ctx.GetEpoch())
	for run := release.runs; run != nil; run = run.next {
		if ctx.TID < run.first || ctx.TID > run.last {
			continue
		}
		if run.clock >= clock {
			return true
		}
		if run.first != ctx.TID || run.last != ctx.TID {
			return false
		}
		return (release.version+1)%atomicReleaseSnapshotCheckPeriod != 0 ||
			!atomicReleaseHasPromotionCoordinates(release)
	}
	return false
}

func (s *atomicState) directRMWReady(ctx *goroutine.RaceContext, mask uint8, reportInternal, synchronize bool) (*atomicRelease, bool) {
	frontier := &s.writes.user
	if reportInternal {
		frontier = &s.writes.internal
	}
	if _, ok := frontier.find(ctx.TID); !ok || !atomicHistoryMaskEmpty(&s.plainWrites, mask) {
		return nil, false
	}
	if !synchronize {
		return nil, true
	}
	release, membership := s.exactReleaseForMask(mask)
	if release == nil || membership != mask {
		return nil, false
	}
	seenVersion, strong, ok := ctx.LookupAtomicRelease(unsafe.Pointer(release), release.stream, membership)
	if !ok || !strong || seenVersion != release.version {
		return nil, false
	}
	if release.lineage != nil {
		return release, true
	}
	if !releasePointMaxCannotAllocate(release, ctx) {
		return nil, false
	}
	return release, true
}

// AtomicBeginInternalRMWCooperative attempts the retained same-owner tier used
// by internal/sync Mutex atomics and synchronized public RMWs whose context has
// activated the bounded exact-capability cache. A direct hit still
// retains the immutable AtomicFastPath and state.mu across hardware. Public
// hits additionally enter the normal cooperative owner protocol before the
// proof is checked. Synchronizing hits require an exact current strong-release
// proof; non-synchronizing Mutex hits use their own cache mode and never import
// or publish release metadata.
//
// direct is true only when successful-write completion is allocation- and
// report-free. A failed CAS must still be completed canonically with the
// returned token; the hardware operation is never replayed.
func (d *Detector) AtomicBeginInternalRMWCooperative(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr, synchronize bool, token *AtomicToken) (retry, direct, spin, polite bool, park *uint32) {
	if token == nil {
		return false, false, false, false, nil
	}
	for i := range token {
		token[i] = nil
	}
	mask, exact := plainAtomicFastMask(addr, size)
	internal := atomicInternalRMWFastPC(pc)
	public := ctx != nil && synchronize && ctx.AtomicRMWCacheActive && !internal
	eligible := ctx != nil && exact && (internal || public)
	if eligible {
		if entry, ok := lookupAtomicRMWCache(ctx, addr, mask, synchronize); ok {
			state := (*atomicState)(entry.State)
			if entry.StateGeneration != 0 && state.handle.generation == entry.StateGeneration {
				fast := (*shadowmem.AtomicFastPath)(entry.Fast)
				if fast.TryRetain(mask) {
					if fast.Overlay() == entry.State && fast.OrdinaryMask() == 0 {
						if public {
							acquired, retrySpin, retryPolite, retryPark := state.tryBeginPublicRMW(fast, token, ctx.TID)
							if !acquired {
								if retryPark == nil && !retrySpin {
									fast.Release()
								}
								return true, false, retrySpin, retryPolite, retryPark
							}
						} else if !state.mu.tryLock() {
							fast.Release()
							return true, false, false, false, nil
						}
						if state.handle.generation != entry.StateGeneration {
							atomicRuntimeThrow("race detector lost retained RMW generation")
						}
						reportInternal := atomicInternalMutexPC(pc)
						release, directReady := state.directRMWReady(ctx, mask, reportInternal, synchronize)
						if directReady {
							if synchronize {
								ctx.PreflightClockAdvance()
							}
							var appendProof vectorclock.PreparedCausalAppend
							if release != nil && release.lineage != nil {
								appendProof, _ = release.lineage.PrepareAppend(ctx.TID, uint32(ctx.GetEpoch()))
								if !appendProof.Valid() {
									atomicRuntimeThrow("race detector failed to prepare direct RMW release")
								}
							}
							state.arena.pin()
							syncMode := int8(0)
							if synchronize {
								syncMode = 1
							}
							state.beginTransaction(addr, size, ctx, syncMode)
							state.beginWriter()
							state.directRMWRelease = release
							state.directRMWAppend = appendProof
							token[0] = unsafe.Pointer(fast)
							return false, true, false, false, nil
						}
						if public {
							// Ownership and the immutable descriptor are already retained.
							// Complete the proof miss through the ordinary exact transition
							// rather than dropping out of and re-entering the waiter queue.
							state.arena.pin()
							state.beginTransaction(addr, size, ctx, 1)
							state.beginWriter()
							state.acquire(ctx, mask)
							ctx.PreflightClockAdvance()
							token[0] = unsafe.Pointer(fast)
							return false, false, false, false, nil
						}
						state.mu.unlock()
					}
					fast.Release()
				}
			}
		}

		// The canonical private miss may enroll or reuse an exact capability in
		// either synchronization mode. This is the only path that can seed a new
		// private same-owner entry, and it retains all mixed-history/release
		// semantics.
		if public {
			retry, spin, polite, park = d.AtomicBeginRMWCooperative(addr, size, ctx, true, token)
			return retry, false, spin, polite, park
		}
		syncMode := int8(0)
		if synchronize {
			syncMode = 1
		}
		retry = d.atomicBegin(addr, size, ctx, synchronize, true, true, true, synchronize, syncMode, true, token, nil, nil, nil)
		return retry, false, false, false, nil
	}

	if synchronize {
		retry, spin, polite, park = d.AtomicBeginRMWCooperative(addr, size, ctx, true, token)
		return retry, false, spin, polite, park
	}
	d.AtomicBegin(addr, size, ctx, false, token)
	return false, false, false, false, nil
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
	d.atomicBegin(addr, size, ctx, acquire, synchronize, synchronize, true, synchronize, syncMode, false, token, nil, nil, nil)
}

// AtomicBeginLoad is the trusted enabled public-Load fallback. Unlike the
// general Plain entry point its operation kind is known before hardware access,
// so it need not perturb the writer revision and invalidate other readers.
func (d *Detector) AtomicBeginLoad(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken) {
	d.atomicBegin(addr, size, ctx, true, true, true, false, false, 1, false, token, nil, nil, nil)
}

// AtomicBeginLoadCooperative is the enabled public-Load fallback. It asks the
// user-goroutine wrapper to retry only when an already-retained exact
// capability's state lock is contended. Capability misses, enrollment, and
// general transactions remain on the authoritative blocking path.
func (d *Detector) AtomicBeginLoadCooperative(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken) (retry bool) {
	return d.atomicBegin(addr, size, ctx, true, true, true, false, false, 1, true, token, nil, nil, nil)
}

// AtomicBeginStoreCooperative is the enabled public-Store transaction. Like
// AtomicBeginLoadCooperative, only retained exact-capability lock contention is
// returned to the user goroutine; misses and enrollment continue to block.
func (d *Detector) AtomicBeginStoreCooperative(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken) (retry bool) {
	return d.atomicBegin(addr, size, ctx, false, true, true, true, true, 1, true, token, nil, nil, nil)
}

func deactivateAtomicLoadEntry(entry goroutine.AtomicLoadCacheEntry) *atomicReadFrontier {
	if entry.State == nil {
		return nil
	}
	state := (*atomicState)(entry.State)
	// Direct Detector.Reset is quiescent but cannot visit external contexts. It
	// resets the arena and invalidates their former ownership references. Never
	// lock or release a state slot which the new arena generation may have reused.
	if entry.StateGeneration == 0 || state.handle.generation != entry.StateGeneration {
		return nil
	}
	state.mu.lock()
	if state.handle.generation != entry.StateGeneration {
		state.mu.unlock()
		return nil
	}
	var spare *atomicReadFrontier
	if entry.Frontier != nil {
		frontier := (*atomicReadFrontier)(entry.Frontier)
		spare = state.deactivateReadFrontier(frontier, entry.Generation)
		// A writer can fully prune and unlink a cached frontier while its owning
		// context still roots the node. Once that context evicts or tears down the
		// entry, recover the detached node as its spare so the caller can either
		// reuse or free it. The cache's state ownership makes this lock target
		// stable; generation, mask, and linkage then prove the node is the exact
		// detached cache-owned generation rather than a live or recycled node.
		if spare == nil && frontier.generation.Load() == entry.Generation && frontier.mask.Load() == 0 && frontier.next == nil {
			spare = frontier
		}
	}
	state.mu.unlock()
	// Every populated cache entry owns one state reference. Drop it only after
	// deactivation has finished under state.mu; the returned detached node is no
	// longer reachable from state and therefore survives final state retirement.
	state.arena.releaseState(state)
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
	// State!=nil is the ownership bit for a populated cache entry. A refresh of
	// the same entry keeps its existing reference; filling an empty or spare-only
	// slot acquires exactly one reference before publishing the state pointer.
	if ctx.AtomicLoadCache[index].State == nil {
		state.arena.retainState(state)
	}
	ctx.AtomicLoadCache[index] = goroutine.AtomicLoadCacheEntry{
		Fast: unsafe.Pointer(fast), State: unsafe.Pointer(state), Frontier: unsafe.Pointer(frontier),
		Revision: revision, Generation: frontier.generation.Load(), PC: pc, StateGeneration: state.handle.generation,
		Mask: mask, Internal: internal,
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
	deactivateAtomicRMWCache(ctx)
}

func releaseAtomicRMWCacheEntry(entry goroutine.AtomicRMWCacheEntry) {
	if entry.State == nil {
		return
	}
	state := (*atomicState)(entry.State)
	// A quiescent Reset invalidates every old arena ownership reference without
	// visiting external contexts. Do not release a slot that a new generation
	// may already have reused.
	if entry.StateGeneration == 0 || state.handle.generation != entry.StateGeneration {
		return
	}
	state.arena.releaseState(state)
}

func deactivateAtomicRMWCache(ctx *goroutine.RaceContext) {
	for i := range ctx.AtomicRMWCache {
		entry := ctx.AtomicRMWCache[i]
		ctx.AtomicRMWCache[i] = goroutine.AtomicRMWCacheEntry{}
		releaseAtomicRMWCacheEntry(entry)
	}
	ctx.AtomicRMWCacheNext = 0
	ctx.AtomicRMWCacheActive = false
}

func lookupAtomicRMWCache(ctx *goroutine.RaceContext, addr uintptr, mask uint8, synchronize bool) (goroutine.AtomicRMWCacheEntry, bool) {
	for i := range ctx.AtomicRMWCache {
		entry := ctx.AtomicRMWCache[i]
		if entry.Fast != nil && entry.State != nil && entry.Addr == addr && entry.Mask == mask && entry.Synchronize == synchronize {
			return entry, true
		}
	}
	return goroutine.AtomicRMWCacheEntry{}, false
}

// retainAtomicRMWCacheCandidate reserves the state ownership which will back a
// newly published cache entry. It is called while the exact transaction still
// owns state.mu and the AtomicFastPath capability, so the state cannot retire
// between validation and the retain. Matching entries already own that retain.
func retainAtomicRMWCacheCandidate(ctx *goroutine.RaceContext, addr uintptr, mask uint8, synchronize bool, state *atomicState) (retained bool) {
	if old, ok := lookupAtomicRMWCache(ctx, addr, mask, synchronize); ok &&
		old.State == unsafe.Pointer(state) && old.StateGeneration == state.handle.generation {
		return false
	}
	state.arena.retainState(state)
	return true
}

func recordAtomicRMWCache(ctx *goroutine.RaceContext, addr uintptr, mask uint8, synchronize bool, fast *shadowmem.AtomicFastPath, state *atomicState, retained bool) {
	index := -1
	for i := range ctx.AtomicRMWCache {
		entry := &ctx.AtomicRMWCache[i]
		if entry.Addr == addr && entry.Mask == mask && entry.Synchronize == synchronize {
			index = i
			break
		}
		if index < 0 && entry.State == nil {
			index = i
		}
	}
	if index < 0 {
		index = int(ctx.AtomicRMWCacheNext % goroutine.AtomicRMWCacheSlots)
		ctx.AtomicRMWCacheNext = (ctx.AtomicRMWCacheNext + 1) % goroutine.AtomicRMWCacheSlots
	}
	old := ctx.AtomicRMWCache[index]
	if old.State == unsafe.Pointer(state) && old.StateGeneration == state.handle.generation {
		// The existing entry already owns the state reference. A defensive extra
		// reservation can arise only if an entry changed between preparation and
		// publication on the same owner; release it rather than leaking ownership.
		if retained {
			state.arena.releaseState(state)
		}
		retained = false
	}
	ctx.AtomicRMWCache[index] = goroutine.AtomicRMWCacheEntry{
		Fast: unsafe.Pointer(fast), State: unsafe.Pointer(state), Addr: addr,
		StateGeneration: state.handle.generation, Mask: mask, Synchronize: synchronize,
	}
	if old.State != nil && (old.State != unsafe.Pointer(state) || old.StateGeneration != state.handle.generation) {
		releaseAtomicRMWCacheEntry(old)
	}
	if !retained && old.State == nil {
		// record is reached with either a pre-existing matching retain or a newly
		// reserved one. Anything else is a trusted-detector programming error.
		atomicRuntimeThrow("race detector internal RMW cache ownership imbalance")
	}
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
func (d *Detector) atomicBegin(addr, size uintptr, ctx *goroutine.RaceContext, acquire, reuseFast, enrollFast, writer, advance bool, syncMode int8, cooperative bool, token *AtomicToken, parkOut **uint32, spinOut, politeOut *bool) (retry bool) {
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
		// revision, or history publication. Allocation-capable representation
		// preparation happens again after the final release import and before the
		// hardware boundary.
		ctx.ValidateClockAdvance()
	}

	firstBase := addr &^ uintptr(7)
	lastBase := (addr + size - 1) &^ uintptr(7)
	fastMask, exactFastShape := plainAtomicFastMask(addr, size)
	exactFastShape = exactFastShape && firstBase == lastBase
	if reuseFast && exactFastShape {
		// Synchronized public RMWs seed the same bounded context cache as the
		// private direct tier. A cached immutable descriptor can retain itself,
		// avoiding the page-table and slot lookup while keeping the complete
		// canonical history/release transition in AtomicEnd.
		if cooperative && parkOut != nil && acquire && writer && advance && syncMode == 1 && ctx.AtomicRMWCacheActive {
			if entry, ok := lookupAtomicRMWCache(ctx, addr, fastMask, true); ok {
				state := (*atomicState)(entry.State)
				if entry.StateGeneration != 0 && state.handle.generation == entry.StateGeneration {
					fast := (*shadowmem.AtomicFastPath)(entry.Fast)
					if fast.TryRetain(fastMask) {
						if fast.Overlay() == entry.State && fast.OrdinaryMask() == 0 {
							acquired, spin, polite, park := state.tryBeginPublicRMW(fast, token, ctx.TID)
							if !acquired {
								if parkOut != nil {
									*parkOut = park
									*spinOut = spin
									*politeOut = polite
								}
								if park == nil && !spin {
									fast.Release()
								}
								return true
							}
							state.arena.pin()
							state.beginTransaction(addr, size, ctx, syncMode)
							state.beginWriter()
							state.acquire(ctx, fastMask)
							ctx.PreflightClockAdvance()
							token[0] = unsafe.Pointer(fast)
							return false
						}
						fast.Release()
					}
				}
			}
		}
		if slot := d.slotMemory.GetSlot(addr); slot != nil {
			if fast := slot.TryAtomicFast(fastMask); fast != nil {
				state := (*atomicState)(fast.Overlay())
				if cooperative {
					if parkOut != nil {
						acquired, spin, polite, park := state.tryBeginPublicRMW(fast, token, ctx.TID)
						if !acquired {
							if acquire && writer && advance && syncMode == 1 {
								ctx.AtomicRMWCacheActive = true
							}
							*parkOut = park
							*spinOut = spin
							*politeOut = polite
							if park == nil && !spin {
								fast.Release()
							}
							return true
						}
					} else if !state.mu.tryLock() {
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
				if advance {
					ctx.PreflightClockAdvance()
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
			if parkOut != nil {
				state.beginExactFastOwner(ctx.TID)
			}
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
			if advance {
				ctx.PreflightClockAdvance()
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
	if advance {
		ctx.PreflightClockAdvance()
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
func (d *Detector) AtomicEndLoad(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr) *uint32 {
	return d.atomicEndMode(addr, size, ctx, token, pc, false, true, true)
}

// AtomicEndMode publishes one transition per unique overlay membership and
// checks each ordinary equivalence group once while its Begin lock is still
// held. Memory histories and mixed atomic/plain conflicts are retained even
// when synchronize is false. In that mode writes retire superseded releases,
// but no release is published and the context clock does not advance. Enabled
// read-like operations weaken ordinary read-cache entries without advancing the
// owning clock; enabled writes publish a release and then advance it.
func (d *Detector) AtomicEndMode(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr, write, synchronize bool) *uint32 {
	return d.atomicEndMode(addr, size, ctx, token, pc, write, synchronize, false)
}

// AtomicEndInternalRMW completes the specialized private/public RMW begin.
// Successful direct writes use only the preflighted in-place transition.
// Failed CAS reads always take canonical completion with the already-executed
// hardware result; they are never replayed merely to preserve the fast-tier
// proof.
func (d *Detector) AtomicEndInternalRMW(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr, write, synchronize, direct bool) *uint32 {
	if direct {
		if write {
			return d.atomicEndRMWDirect(addr, size, ctx, token, pc, synchronize)
		}
		// The retained capability and state lock are exactly the canonical fast
		// token shape. Completing it as a read preserves all history while the
		// hardware CAS result remains authoritative.
		fast := atomicFastToken(token)
		if fast == nil {
			atomicRuntimeThrow("race detector invalid failed direct RMW token")
		}
		state := (*atomicState)(fast.Overlay())
		if state.directRMWAppend.Valid() {
			state.directRMWAppend.Abort()
		}
		state.directRMWRelease = nil
		return d.atomicEndMode(addr, size, ctx, token, pc, false, synchronize, false)
	}

	var fast *shadowmem.AtomicFastPath
	var state *atomicState
	var retained bool
	mask, exact := plainAtomicFastMask(addr, size)
	if write && exact && ((synchronize && ctx.AtomicRMWCacheActive) || atomicInternalRMWFastPC(pc)) {
		fast = atomicFastToken(token)
		if fast != nil && fast.Mask() == mask && fast.OrdinaryMask() == 0 {
			state = (*atomicState)(fast.Overlay())
			retained = retainAtomicRMWCacheCandidate(ctx, addr, mask, synchronize, state)
		}
	}
	wake := d.atomicEndMode(addr, size, ctx, token, pc, write, synchronize, false)
	if state != nil {
		recordAtomicRMWCache(ctx, addr, mask, synchronize, fast, state, retained)
	}
	return wake
}

func (d *Detector) atomicEndRMWDirect(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr, synchronize bool) *uint32 {
	if token == nil || ctx == nil {
		atomicRuntimeThrow("race detector invalid direct RMW completion")
	}
	fast := atomicFastToken(token)
	if fast == nil || fast.OrdinaryMask() != 0 || (!atomicInternalRMWFastPC(pc) && !(synchronize && ctx.AtomicRMWCacheActive)) {
		atomicRuntimeThrow("race detector invalid direct RMW token")
	}
	state := (*atomicState)(fast.Overlay())
	state.validateTransaction(addr, size, ctx, synchronize)
	mask := fast.Mask()
	reportInternal := atomicInternalMutexPC(pc)
	// Begin proved the complete direct transition while holding state.mu, then
	// retained both that lock and the immutable descriptor across hardware. The
	// suspended RaceContext cannot change in between, so repeating the history
	// and release scans here would revalidate state which is structurally frozen.
	next := uint64(0)
	if synchronize {
		next = ctx.ValidateClockAdvance()
	}
	state.pruneReadFrontiers(ctx, mask)
	pruneAtomicAccess(&state.reads, ctx, mask, reportInternal)
	recordAtomicAccess(&state.writes, ctx, pc, mask, reportInternal)
	if synchronize {
		release := state.directRMWRelease
		if release == nil {
			atomicRuntimeThrow("race detector lost direct RMW release proof")
		}
		if state.directRMWAppend.Valid() {
			lineageVersion, changed := state.directRMWAppend.CommitOwned(&release.view)
			if !release.view.Valid() || release.view.Version() != lineageVersion {
				atomicRuntimeThrow("race detector lost prepared direct RMW release view")
			}
			if changed {
				bumpAtomicReleaseVersion(release)
			}
			// The prepared append adds only ctx's already-owned current
			// coordinate to the exact strong release imported by Begin.
			ctx.C.JoinCausal(release.view)
			ctx.RecordAtomicReleaseStructure(unsafe.Pointer(release), release.stream, release.version, release.structureVersion, mask, true)
		} else {
			// Direct readiness proved the affected ordinary-write frontier empty,
			// and the retained descriptor prevents an ordinary access from entering
			// before completion. Publish the already-proven compact release directly.
			state.publishRelease(ctx, mask)
		}
	} else {
		state.retireReleases(mask)
	}
	state.directRMWRelease = nil
	state.endWriter()
	state.endTransaction()
	wake := state.endExactFastOwner()
	state.arena.unpin()
	if synchronize {
		ctx.CommitClockAdvance(next)
	}
	ctx.InvalidateReadRange(addr, size)
	for i := range token {
		token[i] = nil
	}
	fast.Release()
	return wake
}

func (d *Detector) atomicEndMode(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, pc uintptr, write, synchronize, cacheLoad bool) *uint32 {
	if token == nil || token[0] == nil {
		return nil
	}
	if ctx == nil || !validAtomicAccess(addr, size) {
		atomicRuntimeThrow("race detector invalid atomic transaction completion")
	}
	if pc == 0 {
		pc = captureCallerPC()
	}
	if fast := atomicFastToken(token); fast != nil {
		return d.atomicEndPlainFast(addr, size, ctx, token, fast, pc, write, synchronize, cacheLoad)
	}
	var next uint64
	if synchronize && write {
		next = ctx.ValidateClockAdvance()
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
	return nil
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
func (d *Detector) atomicEndPlainFast(addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken, fast *shadowmem.AtomicFastPath, pc uintptr, write, synchronize, cacheLoad bool) *uint32 {
	var next uint64
	if synchronize && write {
		next = ctx.ValidateClockAdvance()
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
	wake := state.endExactFastOwner()
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
	return wake
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
