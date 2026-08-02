package syncshadow

import (
	"internal/runtime/atomic"
	"runtime/race/kolkov/vectorclock"
)

const (
	releaseMergeLaneCount       = 16
	releaseMergeInitialCapacity = 4
	releaseMergeBatchCapacity   = 32
)

type mergeSlot struct {
	projection vectorclock.ReleaseProjection
	committed  atomic.Uint32
}

type mergeBatch struct {
	next    *mergeBatch
	claimed atomic.Uint32
	slots   [releaseMergeBatchCapacity]mergeSlot
}

type mergeGeneration struct {
	active         atomic.Uint32
	initialClaimed atomic.Uint32
	initial        [releaseMergeInitialCapacity]mergeSlot
	lanes          [releaseMergeLaneCount]atomic.Pointer[mergeBatch]
}

func mergeLane(tid uint32) uint32 {
	x := tid * 0x9e3779b9
	return (x ^ (x >> 16)) & (releaseMergeLaneCount - 1)
}

func (g *mergeGeneration) enter(sv *SyncVar) bool {
	for active := g.active.Load(); ; active = g.active.Load() {
		if active == ^uint32(0) || !g.active.CompareAndSwap(active, active+1) {
			if active == ^uint32(0) {
				return false
			}
			continue
		}
		if sv.pending.Load() == g && sv.retired.Load() == 0 {
			return true
		}
		g.active.Add(-1)
		return false
	}
}

func (b *mergeBatch) claim() (uint32, bool) {
	for claimed := b.claimed.Load(); claimed < releaseMergeBatchCapacity; claimed = b.claimed.Load() {
		if b.claimed.CompareAndSwap(claimed, claimed+1) {
			return claimed, true
		}
	}
	return 0, false
}

func (g *mergeGeneration) claimInitial() (uint32, bool) {
	for claimed := g.initialClaimed.Load(); claimed < releaseMergeInitialCapacity; claimed = g.initialClaimed.Load() {
		if g.initialClaimed.CompareAndSwap(claimed, claimed+1) {
			return claimed, true
		}
	}
	return 0, false
}

func commitMergeSlot(slot *mergeSlot, projection *vectorclock.ReleaseProjection) {
	slot.projection = *projection
	*projection = vectorclock.ReleaseProjection{}
	slot.committed.Store(1)
}

// append transfers projection ownership only after reserving a slot. The
// committed store is the ReleaseMerge linearization point.
func (g *mergeGeneration) append(sv *SyncVar, tid uint32, projection *vectorclock.ReleaseProjection) bool {
	if g == nil || projection == nil || !g.enter(sv) {
		return false
	}
	if index, ok := g.claimInitial(); ok {
		commitMergeSlot(&g.initial[index], projection)
		g.active.Add(-1)
		return true
	}
	batch := g.lanes[mergeLane(tid)].Load()
	if batch == nil {
		g.active.Add(-1)
		return false
	}
	index, ok := batch.claim()
	if !ok {
		g.active.Add(-1)
		return false
	}
	commitMergeSlot(&batch.slots[index], projection)
	g.active.Add(-1)
	return true
}

func newMergeGeneration() *mergeGeneration {
	return new(mergeGeneration)
}

func appendUnpublished(g *mergeGeneration, projection *vectorclock.ReleaseProjection) {
	g.initialClaimed.Store(1)
	commitMergeSlot(&g.initial[0], projection)
}

func appendLinkedBatch(batch *mergeBatch, projection *vectorclock.ReleaseProjection) {
	batch.claimed.Store(1)
	commitMergeSlot(&batch.slots[0], projection)
}

// TryPublishReleaseMergeForContext publishes only the source projection. It
// never imports the aggregate into src, preserving ordering among WaitGroup
// workers until a waiter performs Acquire.
func (sv *SyncVar) TryPublishReleaseMergeForContext(src *vectorclock.VectorClock, tid uint32, _ uint64) bool {
	if src == nil || sv.retired.Load() != 0 {
		return false
	}
	var projection vectorclock.ReleaseProjection
	if !vectorclock.TryPinReleaseProjectionForPreparedOwner(src, tid, &projection) {
		return false
	}
	if g := sv.pending.Load(); g != nil {
		if g.append(sv, tid, &projection) {
			return true
		}
		projection.Release()
		return false
	}
	// Direct runtime bridges execute with the current M retained and therefore
	// cannot allocate the first generation. Leave publication to the exact
	// canonical fallback; later publishers remain allocation-free while inline
	// or linked capacity is available.
	projection.Release()
	return false
}

// PublishReleaseMergeForContext is the exact allocating fallback. Projection
// capture and batch allocation happen before releaseMu is acquired.
func (sv *SyncVar) PublishReleaseMergeForContext(src *vectorclock.VectorClock, tid uint32, _ uint64) {
	if src == nil {
		return
	}
	projection := vectorclock.PinReleaseProjectionForPreparedOwner(src, tid)
	if g := sv.pending.Load(); g != nil && g.append(sv, tid, &projection) {
		return
	}
	// A first or small fan-out release needs only the compact generation. Do not
	// allocate a 32-slot batch unless the four inline slots are actually full.
	if sv.pending.Load() == nil {
		prepared := newMergeGeneration()
		sv.releaseMu.lock()
		if sv.retired.Load() != 0 {
			sv.releaseMu.unlock()
			projection.Release()
			return
		}
		if g := sv.pending.Load(); g == nil {
			appendUnpublished(prepared, &projection)
			sv.pending.Store(prepared)
			sv.releaseMu.unlock()
			return
		} else if g.append(sv, tid, &projection) {
			sv.releaseMu.unlock()
			return
		}
		sv.releaseMu.unlock()
	}

	batch := new(mergeBatch)
	sv.releaseMu.lock()
	if sv.retired.Load() != 0 {
		sv.releaseMu.unlock()
		projection.Release()
		return
	}
	g := sv.pending.Load()
	if g == nil {
		prepared := newMergeGeneration()
		appendUnpublished(prepared, &projection)
		sv.pending.Store(prepared)
		sv.releaseMu.unlock()
		return
	}
	if g.append(sv, tid, &projection) {
		sv.releaseMu.unlock()
		return
	}
	// A full batch is immutable. Link a prepared successor while holding the
	// generation-management lock, then publish this projection in slot 0.
	lane := mergeLane(tid)
	batch.next = g.lanes[lane].Load()
	appendLinkedBatch(batch, &projection)
	g.lanes[lane].Store(batch)
	sv.releaseMu.unlock()
}

func (sv *SyncVar) takePendingLocked() *mergeGeneration {
	g := sv.pending.Load()
	sv.pending.Store(nil)
	if g == nil {
		return nil
	}
	for g.active.Load() != 0 {
		runtimeKolkovSpinWait(1, false)
	}
	return g
}

func releaseMergeGeneration(g *mergeGeneration, bases *[]*vectorclock.ClockSnapshot, roots *[]vectorclock.CausalView, finite *[]vectorclock.FiniteRange, retired *[]vectorclock.RetiredRange) {
	if g == nil {
		return
	}
	drain := func(slot *mergeSlot) {
		if slot.committed.Load() == 0 {
			return
		}
		if bases != nil && roots != nil && finite != nil && retired != nil {
			slot.projection.Drain(bases, roots, finite, retired)
		}
		slot.projection.Release()
	}
	for i, claimed := uint32(0), g.initialClaimed.Load(); i < claimed; i++ {
		drain(&g.initial[i])
	}
	for lane := range g.lanes {
		for batch := g.lanes[lane].Load(); batch != nil; batch = batch.next {
			for i, claimed := uint32(0), batch.claimed.Load(); i < claimed; i++ {
				drain(&batch.slots[i])
			}
		}
	}
}

func (sv *SyncVar) discardPendingLocked() {
	releaseMergeGeneration(sv.takePendingLocked(), nil, nil, nil, nil)
}

// foldPendingLocked rotates the current generation and integrates its entire
// exact union in one bulk range join. It must be called with releaseMu held.
func (sv *SyncVar) foldPendingLocked() *vectorclock.VectorClock {
	g := sv.takePendingLocked()
	base := sv.materializeFoldedLocked()
	if g == nil {
		return base
	}
	var finite []vectorclock.FiniteRange
	var retired []vectorclock.RetiredRange
	var bases []*vectorclock.ClockSnapshot
	var roots []vectorclock.CausalView
	releaseMergeGeneration(g, &bases, &roots, &finite, &retired)
	vectorclock.AppendCoalescedReleaseComponents(bases, roots, &finite, &retired)
	finite, retired = vectorclock.CanonicalizeReleaseRanges(finite, retired)

	current := sv.current.Load()
	sv.current.Store(nil)
	slot := sv.reusableVersionLocked(current, base, nil, false)
	var result *vectorclock.VectorClock
	if slot == nil {
		if base == nil {
			result = vectorclock.New()
		} else {
			result = base.Clone()
		}
	} else if slot.clock == nil {
		if base == nil {
			slot.clock = vectorclock.New()
		} else {
			slot.clock = base.Clone()
		}
		result = slot.clock
	} else if base == nil {
		slot.clock.Reset()
		result = slot.clock
	} else {
		slot.clock.CopyFrom(base)
		result = slot.clock
	}
	result.JoinCanonicalRanges(finite)
	result.RetireRanges(retired)
	sv.sourceProofValid = false
	sv.folded = false
	sv.publishLocked(slot, result)
	sv.refreshVersionsLocked(result)
	return result
}
