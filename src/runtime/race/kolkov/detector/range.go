package detector

import (
	"unsafe"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
)

// rangeReportState is an immutable copy of the previous access metadata needed
// by reportRaceV2PC. Range transitions may overwrite the live VarState before
// the first conflict is reported, so retaining the live pointer is incorrect.
type rangeReportState struct {
	writePC    uintptr
	readPC     uintptr
	writeStack uint64
	readStack  uint64
	lifecycle  uint64
}

func (s rangeReportState) GetWritePC() uintptr    { return s.writePC }
func (s rangeReportState) GetReadPC() uintptr     { return s.readPC }
func (s rangeReportState) GetWriteStack() uint64  { return s.writeStack }
func (s rangeReportState) GetReadStack() uint64   { return s.readStack }
func (s rangeReportState) GetLifecycleID() uint64 { return s.lifecycle }

func snapshotOrdinaryState(state *shadowmem.VarState) rangeReportState {
	return rangeReportState{
		writePC:    state.GetWritePC(),
		readPC:     state.GetReadPC(),
		writeStack: state.GetWriteStack(),
		readStack:  state.GetReadStack(),
		lifecycle:  state.GetLifecycleID(),
	}
}

type pendingRangeRace struct {
	raceType  string
	addr      uintptr
	previous  rangeReportState
	prev      epoch.Epoch
	current   epoch.Epoch
	currentPC uintptr
}

func (r *pendingRangeRace) capture(raceType string, addr uintptr, previous rangeReportState, prev, current epoch.Epoch, currentPC uintptr) {
	if r.raceType != "" {
		return
	}
	r.raceType = raceType
	r.addr = addr
	r.previous = previous
	r.prev = prev
	r.current = current
	r.currentPC = currentPC
}

// captureAtomic records a mixed atomic/plain conflict unless it is the exact
// sync.RWMutex marker-read/internal-Mutex-atomic implementation pair. A user
// access that conflicts with either side remains a real mixed-access race.
//
// Suppression is decided before occupying the pending slot so a suppressible
// conflict cannot hide a genuine conflict later in the same range operation.
func (r *pendingRangeRace) captureAtomic(raceType string, addr uintptr, previous rangeReportState, prev, current epoch.Epoch, currentPC, plainPC, atomicPC uintptr) {
	if r.raceType != "" {
		return
	}
	if rwMutexMarkerPC(plainPC) && atomicInternalMutexPC(atomicPC) {
		return
	}
	r.capture(raceType, addr, previous, prev, current, currentPC)
}

func (r *pendingRangeRace) report(d *Detector) {
	if r.raceType == "" {
		return
	}
	reportPendingRangeRace(d, *r)
}

// Keep the interface conversion needed by the reporting API off the scalar
// no-race path. Inlining this call makes rangeReportState escape even when no
// pending race exists, adding an allocation to every ordinary access.
//
//go:noinline
func reportPendingRangeRace(d *Detector, r pendingRangeRace) {
	d.reportRaceV2PC(r.raceType, r.addr, r.previous, r.prev, r.current, r.currentPC)
}

// validAccessRange rejects both zero-sized and wrapping ranges without forming
// addr+size. Public runtime APIs can convert a negative int length to uintptr,
// so this validation is a correctness boundary rather than just a guard.
//
//go:nosplit
func validAccessRange(addr, size uintptr) bool {
	return size != 0 && size-1 <= ^uintptr(0)-addr
}

// applySizedScalarRead publishes one logical scalar transition through every
// byte alias covered by the compiler-provided width. Equal prior histories stay
// represented by one compact group and one VarState; a partial later access
// copy-on-write splits only its subset. This is what lets an atomic operation
// created after the ordinary access discover an overlap in a later lane.
//
// Compact admission examines every covered word before publishing, so an
// established atomic overlay cannot be bypassed by an exact-start early return.
// The start mapping is cached only after the complete transition is visible.
func (d *Detector) applySizedScalarRead(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	// A prior exact address/width hint lets the second epoch pay a one-time
	// materialization cost. Once the word exists, take its ordinary state only
	// when its complete slot reference mask is exactly the hinted scalar width.
	// Contention waits without retaining the slot lock; atomic overlays must use
	// the established mixed-access path below.
	if ctx.HasWeakReadHintSized(addr, size) && size <= 8-(addr&7) {
		if d.rangeMemory.GetSlot(addr) == nil {
			d.rangeMemory.MaterializeReadHintSlot(addr)
		}
		if state, ok := d.rangeMemory.LockReadHintSlotRange(addr, size); ok {
			var pending pendingRangeRace
			d.applyOrdinaryReadLocked(addr, state, ctx, pc, &pending)
			ctx.RecordReadSized(addr, size, unsafe.Pointer(state))
			state.UnlockAccess()
			d.recordPromotedReadCapability(addr, size, ctx, state)
			pending.report(d)
			return
		}
	}

	var pending pendingRangeRace
	for current, remaining := addr, size; remaining != 0; {
		count := uintptr(4096) - current&4095
		if count > remaining {
			count = remaining
		}
		if !d.rangeMemory.TryCompactReadRange(current, count, ctx.GetEpoch(), ctx.C, pc) {
			d.rangeMemory.AccessRange(current, count, func(word uintptr, groupMask uint8, state *shadowmem.VarState) {
				d.applyRangeReadLocked(word, groupMask, state, ctx, pc, &pending)
			})
		}
		current += count
		remaining -= count
	}

	// Never materialize a compact history merely to seed the redundant-read
	// cache. In particular, a dense palette deliberately has no VarState; Get
	// would turn every otherwise compact ephemeral-object read into a word slot
	// and then one fresh VarState per allocator lifetime. Existing slots still
	// publish their exact GC-rooted state for runtime pointer revalidation.
	// Unmaterialized histories publish the address-only form: runtime accepts it
	// only for non-reclaimable first-module storage, while heap and stack reads
	// conservatively miss because they require a non-nil exact state pointer.
	if slot := d.rangeMemory.GetSlot(addr); slot != nil {
		if state := slot.State(uint8(addr & 7)); state != nil {
			ctx.RecordReadSized(addr, size, unsafe.Pointer(state))
			d.recordPromotedReadCapability(addr, size, ctx, state)
			pending.report(d)
			return
		}
	}
	ctx.RecordAddressOnlyReadRange(addr, size)
	pending.report(d)
}

// applySizedScalarWrite is the write counterpart. One transition is applied per
// distinct prior equivalence class; classes which converge may share compact
// history, while conflicting prior writes/reads are all checked before report.
func (d *Detector) applySizedScalarWrite(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	var pending pendingRangeRace
	for current, remaining := addr, size; remaining != 0; {
		count := uintptr(4096) - current&4095
		if count > remaining {
			count = remaining
		}
		if !d.rangeMemory.TryCompactWriteRange(current, count, ctx.GetEpoch(), ctx.C, pc) {
			d.rangeMemory.AccessRange(current, count, func(word uintptr, groupMask uint8, state *shadowmem.VarState) {
				d.applyRangeWriteLocked(word, groupMask, state, ctx, pc, &pending)
			})
		}
		current += count
		remaining -= count
	}
	pending.report(d)
}

// OnReadRange applies one ordinary read transition to every copy-on-write
// equivalence group intersecting [addr, addr+size). It never seeds the scalar
// temporal cache: a range hook cannot prove which exact scalar hook will recur.
func (d *Detector) OnReadRange(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	if !validAccessRange(addr, size) {
		return
	}
	if d.sampler != nil && !d.sampler.ShouldSample() {
		return
	}
	if pc == 0 {
		pc = captureCallerPC()
	}

	var pending pendingRangeRace
	marker := rwMutexMarkerPC(pc)
	for current, remaining := addr, size; remaining != 0; {
		count := uintptr(4096) - current&4095
		if count > remaining {
			count = remaining
		}
		if marker || !d.rangeMemory.TryCompactReadRange(current, count, ctx.GetEpoch(), ctx.C, pc) {
			d.rangeMemory.AccessRange(current, count, func(word uintptr, groupMask uint8, state *shadowmem.VarState) {
				d.applyRangeReadLocked(word, groupMask, state, ctx, pc, &pending)
			})
		}
		current += count
		remaining -= count
	}

	// All slot and VarState locks have been released and every covered group
	// has transitioned before the single local report is emitted.
	pending.report(d)
}

// OnWriteRange applies one ordinary write transition to every intersecting
// group. It invalidates only scalar cache entries inside the logical range,
// before sampling can skip detector work.
func (d *Detector) OnWriteRange(addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	if !validAccessRange(addr, size) {
		return
	}
	ctx.InvalidateReadRange(addr, size)
	if d.sampler != nil && !d.sampler.ShouldSample() {
		return
	}
	if pc == 0 {
		pc = captureCallerPC()
	}

	var pending pendingRangeRace
	for current, remaining := addr, size; remaining != 0; {
		count := uintptr(4096) - current&4095
		if count > remaining {
			count = remaining
		}
		if !d.rangeMemory.TryCompactWriteRange(current, count, ctx.GetEpoch(), ctx.C, pc) {
			d.rangeMemory.AccessRange(current, count, func(word uintptr, groupMask uint8, state *shadowmem.VarState) {
				d.applyRangeWriteLocked(word, groupMask, state, ctx, pc, &pending)
			})
		}
		current += count
		remaining -= count
	}

	pending.report(d)
}

func (d *Detector) applyRangeReadLocked(word uintptr, groupMask uint8, state *shadowmem.VarState, ctx *goroutine.RaceContext, pc uintptr, pending *pendingRangeRace) {
	current := ctx.GetEpoch()
	groupAddr := word + uintptr(firstMaskLane(groupMask))
	if atomic := existingAtomicStateLocked(state); atomic != nil {
		mask := groupMask
		if atomic.base != word {
			mask = 0
		}
		if mask != 0 {
			atomic.mu.lock()
			if prev, prevPC, lane, conflict := firstConcurrentAtomic(atomic.writes, ctx, mask); conflict {
				pending.captureAtomic(RaceTypeWriteRead, word+uintptr(lane), rangeReportState{
					writePC:   prevPC,
					lifecycle: state.GetLifecycleID(),
				}, prev, current, pc, 0, prevPC)
			}
			// internal/race.Read markers are scalar. A range read is therefore
			// conservatively user code even when it aliases the same epoch.
			recordAtomicAccess(&atomic.plainReads, ctx, pc, mask, false)
			atomic.mu.unlock()
		}
	}
	d.applyOrdinaryReadLocked(groupAddr, state, ctx, pc, pending)
}

// applyOrdinaryReadLocked performs the ordinary FastTrack read transition and
// captures (rather than immediately reports) its first conflict. It is shared
// by range accesses and the mixed atomic/plain scalar slow path so the current
// access remains represented even when it races.
func (d *Detector) applyOrdinaryReadLocked(addr uintptr, state *shadowmem.VarState, ctx *goroutine.RaceContext, pc uintptr, pending *pendingRangeRace) {
	current := ctx.GetEpoch()
	if prev := state.GetW(); prev != 0 && !prev.HappensBefore(ctx.C) {
		pending.capture(RaceTypeWriteRead, addr, snapshotOrdinaryState(state), prev, current, pc)
	}

	state.SetReadPC(pc)
	if !state.IsPromoted() {
		existing := state.GetReadEpoch()
		if existing.Same(current) {
			return
		}
		if existing != 0 {
			existingTID, _ := existing.Decode()
			currentTID, _ := current.Decode()
			if existingTID == currentTID || existing.HappensBefore(ctx.C) {
				state.SetReadEpoch(current)
				return
			}
			state.PromoteToReadClock(current, ctx.C)
			return
		}
		state.SetReadEpoch(current)
		return
	}
	state.JoinReadClock(current, ctx.C)
}

func (d *Detector) applyRangeWriteLocked(word uintptr, groupMask uint8, state *shadowmem.VarState, ctx *goroutine.RaceContext, pc uintptr, pending *pendingRangeRace) {
	current := ctx.GetEpoch()
	groupAddr := word + uintptr(firstMaskLane(groupMask))
	if atomic := existingAtomicStateLocked(state); atomic != nil {
		mask := groupMask
		if atomic.base != word {
			mask = 0
		}
		if mask != 0 {
			atomic.mu.lock()
			// AccessRange closed and drained every compatible capability before
			// entering this visitor, so cached load frontiers are stable and
			// must be folded into the canonical history before conflict choice.
			atomic.refreshReadFrontiers(mask)
			atomic.pruneReadFrontiers(ctx, mask)
			// The ordinary range write supersedes the latest atomic
			// modification on every covered lane, including when the mixed
			// access races and reporting is deferred.
			atomic.retireReleases(mask)
			// Compiler write hooks precede the machine store. Retain a poison
			// witness so a concurrent atomic Store cannot publish synchronization
			// which outlives this ordinary modification in hardware order.
			recordAtomicAccess(&atomic.plainWrites, ctx, pc, mask, false)
			if prev, prevPC, lane, conflict := firstConcurrentAtomic(atomic.writes, ctx, mask); conflict {
				pending.captureAtomic(RaceTypeWriteWrite, word+uintptr(lane), rangeReportState{
					writePC:   prevPC,
					lifecycle: state.GetLifecycleID(),
				}, prev, current, pc, 0, prevPC)
			} else if prev, prevPC, lane, conflict := firstConcurrentAtomic(atomic.reads, ctx, mask); conflict {
				pending.captureAtomic(RaceTypeReadWrite, word+uintptr(lane), rangeReportState{
					readPC:    prevPC,
					lifecycle: state.GetLifecycleID(),
				}, prev, current, pc, 0, prevPC)
			}
			clearAtomicHistoryMask(&atomic.plainReads, mask)
			atomic.mu.unlock()
		}
	}
	d.applyOrdinaryWriteLocked(groupAddr, state, ctx, pc, pending)
}

// applyOrdinaryWriteLocked is the publication-complete write transition used
// when a logical operation must remain represented after its first conflict.
func (d *Detector) applyOrdinaryWriteLocked(addr uintptr, state *shadowmem.VarState, ctx *goroutine.RaceContext, pc uintptr, pending *pendingRangeRace) {
	current := ctx.GetEpoch()
	if prev := state.GetW(); prev != 0 && !prev.HappensBefore(ctx.C) {
		pending.capture(RaceTypeWriteWrite, addr, snapshotOrdinaryState(state), prev, current, pc)
	}
	if prev, conflict := state.FirstConcurrentRead(ctx.C); conflict {
		pending.capture(RaceTypeReadWrite, addr, snapshotOrdinaryState(state), prev, current, pc)
	}

	// Preserve the scalar same-epoch transition. A range operation applies the
	// scalar state machine once to every equivalence group, not once per byte.
	if state.GetW().Same(current) && state.GetReaderCount() == 0 {
		state.SetWritePC(pc)
		return
	}

	owner := state.GetExclusiveWriter()
	currentTID := int64(ctx.TID)
	if owner == 0 {
		state.SetExclusiveWriter(currentTID)
	} else if owner > 0 && owner != currentTID {
		state.SetExclusiveWriter(-1)
	}
	state.SetW(current)
	state.IncrementWriteCount()
	state.SetWritePC(pc)
	state.Demote()
}
