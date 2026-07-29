package shadowmem

import (
	"internal/runtime/atomic"
	"unsafe"
)

const shadowSlotLanes = 8

const atomicFastEscaped = uint32(1) << 31

// AtomicFastPath is the out-of-line detector binding for one atomic history.
// Its exact lane signature becomes immutable when enrolled. Multiple ordinary
// equivalence groups may publish the same binding when at most one contains
// history; ordinaryMask identifies that frozen group for detector completion.
// The low bits of users count transactions which have revalidated this
// capability and retained it across the hardware operation; the high bit
// permanently closes that capability generation before any lane redirect,
// ordinary access, incompatible atomic operation, reset, or clear can proceed.
//
// state and overlay are GC-visible so a stack-resident detector token keeps the
// complete generation alive even when a concurrent clear removes the slot from
// the page table. A closed capability object is never reopened or repurposed;
// lane bindings may later publish a distinct object, so a cached pointer can
// never revive through address reuse or re-enrollment.
type AtomicFastPath struct {
	overlay      unsafe.Pointer
	state        *VarState
	lifecycle    uint64
	mask         uint8
	ordinaryMask uint8
	enrolled     atomic.Uint32
	users        atomic.Uint32
	// rearm is the exact-mask probation signature for this already-closed
	// descriptor. The first compatible setup after escape records its mask but
	// remains slow; a second consecutive setup with that mask publishes a fresh
	// descriptor. Any incompatible slot transaction clears the signature. This
	// avoids allocating a never-reusable descriptor for alternating compatible
	// setups and general fallback transactions while preserving the invariant
	// that the descriptor itself is never reopened or repurposed.
	rearm atomic.Uint32
}

// State returns the only ordinary state whose frozen history must be checked.
// When OrdinaryMask is zero, it is an arbitrary covered empty state retained
// solely for lifecycle validation.
//
//go:nosplit
func (p *AtomicFastPath) State() *VarState {
	return p.state
}

// Overlay returns the detector-owned atomic transaction state bound to this
// capability. Its concrete type remains private to package detector.
//
//go:nosplit
func (p *AtomicFastPath) Overlay() unsafe.Pointer {
	return p.overlay
}

// Mask returns the exact aligned byte lanes represented by this capability.
//
//go:nosplit
func (p *AtomicFastPath) Mask() uint8 {
	return p.mask
}

func (p *AtomicFastPath) matches(mask uint8) bool {
	return p != nil && p.enrolled.Load() != 0 && p.mask == mask
}

// OrdinaryMask returns the frozen ordinary group that fast completion must check.
// At most one non-empty ordinary group is admitted by enrollment.
//
//go:nosplit
func (p *AtomicFastPath) OrdinaryMask() uint8 {
	return p.ordinaryMask
}

// Release completes a transaction acquired by ShadowSlot.TryAtomicFast.
//
//go:nosplit
func (p *AtomicFastPath) Release() {
	p.users.Add(-1)
}

// tryAcquire retains p unless an escape already permanently closed this
// capability generation. Closed generations are never reopened.
//
//go:nosplit
func (p *AtomicFastPath) tryAcquire() bool {
	if p.enrolled.Load() == 0 {
		return false
	}
	for {
		state := p.users.Load()
		if state&atomicFastEscaped != 0 {
			return false
		}
		// More than 2^31 concurrent hardware transactions on one address is
		// unrealizable; refuse the fast path rather than wrapping into escaped.
		if state == atomicFastEscaped-1 {
			return false
		}
		if p.users.CompareAndSwap(state, state+1) {
			return true
		}
	}
}

// escape closes p before a lane mutation and waits for every transaction that
// won the close race to finish. Fast completion never takes a slot lock, so it
// is safe to wait while holding ShadowSlot.mu.
func (p *AtomicFastPath) escape() {
	delay := uint32(1)
	for {
		state := p.users.Load()
		if state&atomicFastEscaped != 0 || p.users.CompareAndSwap(state, state|atomicFastEscaped) {
			break
		}
		runtimeKolkovSpinWait(delay, delay == spinlockMaxBackoff)
		if delay < spinlockMaxBackoff {
			delay <<= 1
		}
	}
	delay = 1
	for p.users.Load() != atomicFastEscaped {
		runtimeKolkovSpinWait(delay, delay == spinlockMaxBackoff)
		if delay < spinlockMaxBackoff {
			delay <<= 1
		}
	}
}

// escapeIncompatible closes p and cancels any probation signature. Compatible
// setup which is carrying a same-mask probation deliberately skips this helper;
// every ordinary, general, clear, reset, or overlay mutation must use it.
func (p *AtomicFastPath) escapeIncompatible() {
	p.escape()
	p.rearm.Store(0)
}

// rangeBlock owns the ordinary history shared by every completely
// unmaterialized word in one application block. Its lock serializes a bulk
// default transition with first materialization of a word.
type rangeBlock struct {
	mu    spinlock
	state atomic.Pointer[VarState]

	// compact is published before the first exact compact history in this
	// block and deliberately remains non-nil for the block's lifetime. Scalar
	// runtime shortcuts treat its presence as a conservative slow-path gate;
	// the detector resolves exact compact membership while holding mu.
	compact atomic.Pointer[compactGroups]
}

// ShadowSlot owns the eight exact compiler-provided start addresses in one
// aligned application word. Equal state pointers are an equivalence group:
// every lane in the group has seen exactly the same detector operations.
//
// A page-table slot is a materialized override of its block default. On first
// materialization all eight lanes clone the then-current default as one group;
// after that, nil lanes mean exact zero history rather than inheritance.
//
// states must remain the first field. runtime/race_kolkov.go mirrors this
// private layout for its call-free scalar read fast path.
type ShadowSlot struct {
	states [shadowSlotLanes]atomic.Pointer[VarState]
	mu     spinlock
}

// shadowSlot preserves the internal page-table name used by layout tests and
// comments while allowing the detector to use the slot protocol explicitly.
type shadowSlot = ShadowSlot

// State returns the state currently published for lane. It is intended for
// exact-address lookup and diagnostics; callers that mutate the result must use
// Isolate first.
//
//go:nosplit
func (s *ShadowSlot) State(lane uint8) *VarState {
	return s.states[lane].Load()
}

// GetOrCreateLane returns the state for one lane without changing an existing
// equivalence group. Detector mutations use Isolate instead.
//
// Setup is serialized with copy-on-write group operations. In particular, a
// group operation cannot observe nil and overwrite a concurrently published
// scalar state, leaving the scalar caller with an orphan history.
func (s *ShadowSlot) GetOrCreateLane(lane uint8) *VarState {
	state, _ := s.loadOrStoreLane(lane, nil)
	return state
}

// loadOrStoreLane publishes candidate for a nil lane while holding the slot
// protocol lock. A nil candidate is allocated only when the lane is absent.
func (s *ShadowSlot) loadOrStoreLane(lane uint8, candidate *VarState) (*VarState, bool) {
	s.mu.lock()
	defer s.mu.unlock()
	s.escapeAtomicFastLocked(uint8(1)<<lane, false)

	if state := s.states[lane].Load(); state != nil {
		return state, false
	}
	if candidate == nil {
		candidate = NewVarState()
	}
	s.states[lane].Store(candidate)
	return candidate, true
}

// Isolate returns an access-locked state representing exactly lane. The caller
// must call UnlockAccess when its detector transaction is complete.
//
// If lane belongs to a shared equivalence group, Isolate makes a full ordinary
// history clone and redirects only lane to it. A detector-owned atomic overlay
// is retained by pointer and remains protected by its own transaction lock.
func (s *ShadowSlot) Isolate(lane uint8) *VarState {
	s.mu.lock()
	s.escapeAtomicFastLocked(uint8(1)<<lane, false)
	state := s.states[lane].Load()
	if state == nil {
		state = NewVarState()
		state.LockAccess()
		s.states[lane].Store(state)
		s.mu.unlock()
		return state
	}

	state.LockAccess()
	if s.referenceMask(state)&^(uint8(1)<<lane) == 0 {
		s.mu.unlock()
		return state
	}

	clone := state.CloneOrdinaryLocked()
	clone.LockAccess()
	s.states[lane].Store(clone)
	state.UnlockAccess()
	s.mu.unlock()
	return clone
}

// AccessGroups applies visit once to each equivalence group intersecting mask,
// in ascending lane order. visit receives the exact affected lane mask and an
// access-locked state. The complete mask is always traversed: a logical range
// must publish every state transition even when an early group conflicts.
//
// A partially covered group is split by copy-on-write before visit. Groups are
// never merged: distinct non-nil state pointers remain distinct even when the
// operation covers both.
func (s *ShadowSlot) AccessGroups(mask uint8, visit func(mask uint8, state *VarState)) {
	s.accessGroups(mask, false, true, visit)
}

// LockGroups is the transaction form of AccessGroups. It leaves every visited
// unique state access-locked exactly once, in ascending first-lane order. The
// caller must unlock those unique states after completing the transaction.
// This lets a wide atomic operation retain one ordinary group lock across its
// hardware access instead of locking once per byte.
func (s *ShadowSlot) LockGroups(mask uint8, visit func(mask uint8, state *VarState)) {
	s.accessGroups(mask, true, true, visit)
}

// LockAtomicGroups is the slow/setup transaction path for atomic operations.
// When fastEligible is true, an exact mask may enroll after visit returns when
// all covered groups share one detector overlay and at most one group has
// ordinary history. Every covered group publishes the same immutable gate; the
// detector checks the sole frozen history on fast completion. An incompatible
// operation permanently closes the current gate; a later compatible setup may
// publish a fresh generation after the old gate has drained.
//
// visit runs with the slot lock and each target's access lock held. It must
// attach any required detector overlay before returning so enrollment can bind
// that exact pointer and lifecycle generation.
func (s *ShadowSlot) LockAtomicGroups(mask uint8, fastEligible bool, visit func(mask uint8, state *VarState)) {
	s.accessGroups(mask, true, !fastEligible, visit)
}

// accessGroupsBlockLocked is used by a full-block walk that already owns the
// block lock. It still owns the slot lock and state locks exactly as the public
// protocol does.
func (s *ShadowSlot) accessGroupsBlockLocked(mask uint8, visit func(mask uint8, state *VarState)) {
	s.accessGroups(mask, false, true, visit)
}

func (s *ShadowSlot) accessGroups(mask uint8, keepLocked, forbidFast bool, visit func(mask uint8, state *VarState)) {
	if mask == 0 {
		return
	}

	s.mu.lock()
	s.escapeAtomicFastLocked(mask, !forbidFast)
	remaining := mask
	var groupStates [shadowSlotLanes]*VarState
	var groupMasks [shadowSlotLanes]uint8
	groupCount := 0
	for remaining != 0 {
		lane := firstLane(remaining)
		state := s.states[lane].Load()

		if state == nil {
			// All requested nil lanes have the same zero history. Publish one
			// new group for them without merging any existing non-nil groups.
			hit := uint8(0)
			for i := uint8(0); i < shadowSlotLanes; i++ {
				bit := uint8(1) << i
				if remaining&bit != 0 && s.states[i].Load() == nil {
					hit |= bit
				}
			}
			state = NewVarState()
			state.LockAccess()
			for i := uint8(0); i < shadowSlotLanes; i++ {
				if hit&(uint8(1)<<i) != 0 {
					s.states[i].Store(state)
				}
			}
			remaining &^= hit
			visit(hit, state)
			groupStates[groupCount] = state
			groupMasks[groupCount] = hit
			groupCount++
			if !keepLocked {
				state.UnlockAccess()
			}
			continue
		}

		refs := s.referenceMask(state)
		hit := refs & remaining
		remaining &^= hit

		state.LockAccess()
		target := state
		if hit != refs {
			target = state.CloneOrdinaryLocked()
			target.LockAccess()
			for i := uint8(0); i < shadowSlotLanes; i++ {
				if hit&(uint8(1)<<i) != 0 {
					s.states[i].Store(target)
				}
			}
			state.UnlockAccess()
		}

		visit(hit, target)
		groupStates[groupCount] = target
		groupMasks[groupCount] = hit
		groupCount++
		if !keepLocked {
			target.UnlockAccess()
		}
	}
	if forbidFast {
		// A visitor may have attached an overlay while processing an ordinary
		// RWMutex marker or a general atomic operation. Close that new binding
		// before the slot lock is released.
		s.escapeAtomicFastLocked(mask, false)
	} else if keepLocked {
		s.enrollAtomicFastLocked(mask, &groupStates, &groupMasks, groupCount)
	}
	s.mu.unlock()
}

// enrollAtomicFastLocked publishes one exact capability through every covered
// ordinary group. A compiler-instrumented initialization commonly leaves one
// exact-start group with W/read history and one empty sibling group. Freezing
// both behind the same gate is sufficient: fast completion rechecks the sole
// non-empty group, while any later ordinary mutation discovers this binding
// through its own group and permanently closes it before changing state.
//
// The caller holds s.mu and every listed state's access lock. Refusing complex
// shapes is conservative: slow transactions retain their established semantics.
func (s *ShadowSlot) enrollAtomicFastLocked(mask uint8, states *[shadowSlotLanes]*VarState, masks *[shadowSlotLanes]uint8, count int) {
	if count == 0 {
		return
	}

	var primary *VarState
	var ordinaryMask uint8
	var overlay unsafe.Pointer
	for i := 0; i < count; i++ {
		state := states[i]
		binding := state.atomicState.Load()
		if binding == nil {
			return
		}
		users := binding.users.Load()
		// A live enrolled descriptor is already the compatible generation which
		// the caller will retry after releasing the ordinary locks. Every closed
		// descriptor must be fully drained before replacement; observing only the
		// high bit is insufficient because an old fast token could still publish.
		if binding.enrolled.Load() != 0 && users == 0 {
			return
		}
		if users != 0 && users != atomicFastEscaped {
			return
		}
		if i == 0 {
			overlay = binding.overlay
		} else if binding.overlay != overlay {
			return
		}
		if state.GetW() == 0 && state.GetReaderCount() == 0 {
			continue
		}
		if primary != nil {
			return
		}
		primary = state
		ordinaryMask = masks[i]
	}

	if primary == nil {
		primary = states[0]
	}
	// Reuse a virgin unpublished descriptor on first enrollment. Once a
	// descriptor has escaped, require two consecutive compatible setups before
	// allocating a replacement. If a compatible setup alternates with a general
	// fallback transaction, the first setup only arms probation and the fallback
	// clears it without producing an immediately doomed allocation. Stable
	// compatible operations pay one extra slow operation before regaining the
	// fast path.
	//
	// A replacement is always a distinct object: a lock-free probe may have
	// loaded the old pointer before close and must fail forever rather than
	// observing rewritten descriptor fields (ABA).
	binding := primary.atomicState.Load()
	if binding.users.Load() == atomicFastEscaped {
		rearm := uint32(mask)
		if binding.rearm.Load() != rearm {
			binding.rearm.Store(rearm)
			return
		}
		binding.rearm.Store(0)
		binding = &AtomicFastPath{overlay: overlay}
	}
	binding.state = primary
	binding.lifecycle = primary.GetLifecycleID()
	binding.mask = mask
	binding.ordinaryMask = ordinaryMask
	for i := 0; i < count; i++ {
		states[i].atomicState.Store(binding)
	}
	// Publish last. A lock-free probe which observes a shared binding before
	// this store rejects it without reading the non-atomic descriptor fields.
	binding.enrolled.Store(1)
}

// TryAtomicFast retains and revalidates the immutable capability for mask.
// Enrollment publishes every covered lane before enrolled, while every later
// lane redirect or lifecycle change must close the capability and wait for all
// retained users. Therefore a successful retain freezes the originally
// validated lane set through Release; reloading all eight lane pointers here
// would repeat enrollment's proof without closing a race.
//
//go:nosplit
func (s *ShadowSlot) TryAtomicFast(mask uint8) *AtomicFastPath {
	state := s.states[firstLane(mask)].Load()
	if state == nil {
		return nil
	}
	p := state.atomicState.Load()
	// Retain before reading the packed lifecycle generation. Reset and every
	// mapped lane mutation escape the binding before changing that generation,
	// so a successful retain makes the non-atomic lifecycle bytes stable on
	// 32-bit systems as well as 64-bit systems.
	if p == nil || !p.tryAcquire() {
		return nil
	}
	if !p.matches(mask) || p.lifecycle != p.state.GetLifecycleID() {
		p.Release()
		return nil
	}
	return p
}

// escapeAtomicFastLocked permanently closes every distinct capability
// generation touched by mask, except an exact enrolled signature for a
// compatible plain operation. A later setup may replace a drained generation,
// but this object is never reopened or repurposed.
// It is called before any lane redirect and, for ordinary/general visitors,
// once more after the callback to catch a newly attached overlay.
func (s *ShadowSlot) escapeAtomicFastLocked(mask uint8, compatible bool) {
	var seen [shadowSlotLanes]*AtomicFastPath
	seenCount := 0
	for lane := uint8(0); lane < shadowSlotLanes; lane++ {
		if mask&(uint8(1)<<lane) == 0 {
			continue
		}
		state := s.states[lane].Load()
		if state == nil {
			continue
		}
		binding := state.atomicState.Load()
		if binding == nil {
			continue
		}
		duplicate := false
		for i := 0; i < seenCount; i++ {
			if seen[i] == binding {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		seen[seenCount] = binding
		seenCount++
		if compatible {
			if binding.enrolled.Load() == 0 {
				continue
			}
			exact := binding.matches(mask)
			for other := uint8(0); other < shadowSlotLanes && exact; other++ {
				if mask&(uint8(1)<<other) == 0 {
					continue
				}
				laneState := s.states[other].Load()
				exact = laneState != nil && laneState.atomicState.Load() == binding
			}
			if exact {
				continue
			}
			// A closed descriptor may carry a probation signature for a new
			// exact width which differs from its immutable enrolled mask. Preserve
			// that signature across the second compatible setup; every genuinely
			// incompatible transaction reaches the reset below.
			if binding.users.Load() == atomicFastEscaped && binding.rearm.Load() == uint32(mask) {
				continue
			}
		}
		binding.escapeIncompatible()
	}
}

// ClearMask forgets the selected exact-address lanes. A materialized slot never
// falls back to its block default, so nil remains exact zero history.
func (s *ShadowSlot) ClearMask(mask uint8) {
	if mask == 0 {
		return
	}
	s.mu.lock()
	s.clearMaskLocked(mask)
	s.mu.unlock()
}

// clearMaskBlockLocked is ClearMask with the block lock already held.
func (s *ShadowSlot) clearMaskBlockLocked(mask uint8) {
	if mask == 0 {
		return
	}
	s.mu.lock()
	s.clearMaskLocked(mask)
	s.mu.unlock()
}

// clearMaskLocked drains every transaction which can still publish through an
// affected state before removing the lane mapping. Fast plain atomics retain
// their sidecar gate; ordinary accesses and slow/general atomics retain
// accessMu. Closing only the fast gate would let a slow AtomicEnd publish into
// a detached allocator generation after ClearRange returned.
//
// The caller holds s.mu. Distinct state locks are acquired by ascending first
// lane, matching LockGroups/LockAtomicGroups, and released in reverse order.
func (s *ShadowSlot) clearMaskLocked(mask uint8) {
	s.escapeAtomicFastLocked(mask, false)

	var locked [shadowSlotLanes]*VarState
	lockedCount := 0
	for lane := uint8(0); lane < shadowSlotLanes; lane++ {
		if mask&(uint8(1)<<lane) == 0 {
			continue
		}
		state := s.states[lane].Load()
		if state == nil {
			continue
		}
		duplicate := false
		for i := 0; i < lockedCount; i++ {
			if locked[i] == state {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		state.LockAccess()
		locked[lockedCount] = state
		lockedCount++
	}

	// A scalar visitor which released s.mu before clear acquired it may have
	// attached a previously absent overlay while clear waited for accessMu.
	// Close that binding too before its state becomes unreachable.
	s.escapeAtomicFastLocked(mask, false)

	for i := uint8(0); i < shadowSlotLanes; i++ {
		if mask&(uint8(1)<<i) != 0 {
			s.states[i].Store(nil)
		}
	}
	for i := lockedCount - 1; i >= 0; i-- {
		locked[i].UnlockAccess()
	}
}

func (s *ShadowSlot) referenceMask(state *VarState) uint8 {
	mask := uint8(0)
	for i := uint8(0); i < shadowSlotLanes; i++ {
		if s.states[i].Load() == state {
			mask |= uint8(1) << i
		}
	}
	return mask
}

func firstLane(mask uint8) uint8 {
	for lane := uint8(0); ; lane++ {
		if mask&(uint8(1)<<lane) != 0 {
			return lane
		}
	}
}
