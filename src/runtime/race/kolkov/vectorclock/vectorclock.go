// Package vectorclock implements hybrid dense/sparse vector clocks for
// tracking happens-before relations.
package vectorclock

import (
	iatomic "internal/runtime/atomic"
	"unsafe" // required for go:linkname and pooled-capacity accounting
)

// runtimeThrow terminates rather than permitting a wrapped logical clock to
// continue. Continuing after wrap would manufacture happens-before edges and
// can hide races.
//
//go:linkname runtimeThrow runtime.throw
func runtimeThrow(s string)

const (
	// DenseThreads is the number of logical thread IDs represented inline.
	// Process-lifetime TIDs quickly leave the initial cohort, so keeping a full
	// 4 KiB array in every transient synchronization snapshot makes fork-heavy
	// programs pay for 1024 coordinates even when only one changes. The first
	// cache-line-sized cohort remains direct-indexed; later TIDs use the exact
	// compressed run representation below.
	DenseThreads = 64

	// MaxThreads is retained as the dense-capacity compatibility name. It is
	// not a limit on logical thread IDs; IDs at or above it use sparse storage.
	MaxThreads = DenseThreads

	// maxPooledMetadataBytes bounds the aggregate backing storage retained by
	// one pooled clock. With poolCapacity clocks, sparse peak traffic can therefore
	// retain at most 16 MiB of metadata rather than process-lifetime peaks.
	maxPooledMetadataBytes = 64 << 10

	// A dense tail is introduced only after enough high-TID run metadata has
	// accumulated to amortize the allocation, and only when its four-byte
	// coordinates use at most two thirds of the equivalent twelve-byte runs.
	minDenseTailRuns = 64

	poolShardCount    = 16
	poolShardCapacity = 16
	poolCapacity      = poolShardCount * poolShardCapacity
)

type clockPoolShard struct {
	lock  iatomic.Uint32
	count uint8
	slots [poolShardCapacity]*VectorClock
	// joinScratch is a separately leased merge-input snapshot. Retention is
	// capped at maxPooledMetadataBytes, so all shards together retain <= 1 MiB.
	joinLock    iatomic.Uint32
	joinScratch []finiteRun
}

var poolCursor iatomic.Uint32
var poolShards [poolShardCount]clockPoolShard

func poolGet() *VectorClock {
	shardIndex := (poolCursor.Add(1) - 1) & (poolShardCount - 1)
	shard := &poolShards[shardIndex]
	var vc *VectorClock
	if shard.lock.CompareAndSwap(0, 1) {
		if shard.count != 0 {
			shard.count--
			vc = shard.slots[shard.count]
			shard.slots[shard.count] = nil
		}
		shard.lock.Store(0)
	}
	if vc == nil {
		vc = &VectorClock{}
	}
	vc.poolShard = uint8(shardIndex)
	return vc
}

func poolPut(vc *VectorClock) {
	if vc == nil {
		return
	}
	vc.Reset()
	metadataBytes := uintptr(cap(vc.denseTail))*unsafe.Sizeof(uint32(0)) +
		uintptr(cap(vc.sparseRuns))*unsafe.Sizeof(FiniteRange{}) +
		uintptr(cap(vc.retired))*unsafe.Sizeof(RetiredRange{})
	if metadataBytes > maxPooledMetadataBytes {
		vc.denseTail = nil
		vc.sparseRuns = nil
		vc.retired = nil
	}
	shardIndex := int(vc.poolShard)
	if shardIndex >= poolShardCount {
		return
	}
	shard := &poolShards[shardIndex]
	if !shard.lock.CompareAndSwap(0, 1) {
		return
	}
	if shard.count < poolShardCapacity {
		shard.slots[shard.count] = vc
		shard.count++
	}
	shard.lock.Store(0)
}

// VectorClock stores the first DenseThreads clock components inline and higher
// logical IDs as canonical finite runs. Runs are sorted and non-overlapping;
// zeroes are gaps, and adjacent equal-clock runs are always coalesced. This
// keeps long fresh-TID frontiers proportional to clock changes rather than TIDs.
type VectorClock struct {
	clocks    [DenseThreads]uint32
	maxDense  uint16
	poolShard uint8
	denseTail []uint32
	// denseTailShared means another clock may read the same immutable backing
	// array. Clone and CopyFrom set this on both clocks; canonical mutations
	// detach once, while allocation-free Try operations reject a required write.
	// GC owns the backing lifetime, so sharing needs no explicit reference count.
	denseTailShared bool
	// denseProjectionID certifies that every dense coordinate except the tagged
	// owner still equals a prepared-owner ReleaseProjection with the same ID.
	// The owner is stored as TID+1 so zero remains the invalid tag even for TID 0.
	denseProjectionID    uint64
	denseProjectionOwner uint64
	sparseRuns           []finiteRun
	retired              []RetiredRange
	base                 *ClockSnapshot
	// causal contains independently retained immutable lineage views. The
	// logical clock is the pointwise maximum of these views, base, and the
	// owned mutable representation above.
	causal causalRootSet
	// ownerLineage is private mutable authority for this clock's own logical
	// coordinate. Ordinary copies retain only its immutable causal view.
	ownerLineage *ClockLineage
}

var nextDenseProjectionID iatomic.Uint64

func (vc *VectorClock) invalidateDenseProjectionWitness() {
	vc.denseProjectionID = 0
	vc.denseProjectionOwner = 0
}

func (vc *VectorClock) invalidateDenseProjectionWitnessExcept(tid uint32) {
	if vc.denseProjectionID != 0 && vc.denseProjectionOwner != uint64(tid)+1 {
		vc.invalidateDenseProjectionWitness()
	}
}

func (vc *VectorClock) certifyDenseProjection(ownerTID uint32) uint64 {
	owner := uint64(ownerTID) + 1
	if vc.denseProjectionID != 0 && vc.denseProjectionOwner == owner {
		return vc.denseProjectionID
	}
	id := nextDenseProjectionID.Add(1)
	if id == 0 {
		runtimeThrow("race detector dense projection identity overflow")
	}
	vc.denseProjectionID = id
	vc.denseProjectionOwner = owner
	return id
}

// CausalRootCapacity is the number of unrelated immutable lineage roots a
// VectorClock can retain inline before an exact cold-path materialization.
const CausalRootCapacity = 4

// causalRootSet is packed so all operations over it are bounded and require no
// allocation. Each valid entry owns one independently releasable segment
// reference, and at most one entry belongs to any lineage family.
type causalRootSet struct {
	roots [CausalRootCapacity]CausalView
	count uint8
}

func (s *causalRootSet) Valid() bool { return s != nil && s.count != 0 }

func (s *causalRootSet) Release() {
	if s == nil {
		return
	}
	for i := 0; i < int(s.count); i++ {
		s.roots[i].Release()
		s.roots[i] = CausalView{}
	}
	s.count = 0
}

func (s *causalRootSet) duplicateFrom(other *causalRootSet) bool {
	if s == nil || other == nil || s.count != 0 {
		return false
	}
	for i := 0; i < int(other.count); i++ {
		root, ok := other.roots[i].Duplicate()
		if !ok {
			s.Release()
			return false
		}
		s.roots[i] = root
		s.count++
	}
	return true
}

func (s *causalRootSet) tryJoin(view CausalView) bool {
	if !view.Valid() {
		return true
	}
	for i := 0; i < int(s.count); i++ {
		root := &s.roots[i]
		if !root.SameFamily(view) {
			continue
		}
		if root.Dominates(view) {
			return true
		}
		return root.AdvanceTo(view)
	}
	if int(s.count) == len(s.roots) {
		return false
	}
	root, ok := view.Duplicate()
	if !ok {
		return false
	}
	s.roots[s.count] = root
	s.count++
	return true
}

func (s *causalRootSet) canJoinSet(other *causalRootSet) bool {
	if s == nil || other == nil {
		return false
	}
	families := int(s.count)
	for i := 0; i < int(other.count); i++ {
		known := false
		for j := 0; j < int(s.count); j++ {
			if s.roots[j].SameFamily(other.roots[i]) {
				known = true
				break
			}
		}
		if !known {
			families++
			if families > CausalRootCapacity {
				return false
			}
		}
	}
	return true
}

func (s *causalRootSet) tryJoinSet(other *causalRootSet) bool {
	if !s.canJoinSet(other) {
		return false
	}
	for i := 0; i < int(other.count); i++ {
		if !s.tryJoin(other.roots[i]) {
			runtimeThrow("race detector joined a released causal clock root")
		}
	}
	return true
}

func (s *causalRootSet) Get(tid uint32) uint32 {
	var clock uint32
	for i := 0; i < int(s.count); i++ {
		if candidate := s.roots[i].Get(tid); candidate > clock {
			clock = candidate
		}
	}
	return clock
}

func (s *causalRootSet) IsRetired(tid uint32) bool {
	for i := 0; i < int(s.count); i++ {
		if s.roots[i].IsRetired(tid) {
			return true
		}
	}
	return false
}

func (s *causalRootSet) anchorDominatesAlignedBlock(first, clock uint32) bool {
	if s == nil {
		return false
	}
	for i := 0; i < int(s.count); i++ {
		if s.roots[i].anchorDominatesAlignedBlock(first, clock) {
			return true
		}
	}
	return false
}

// FiniteRange is one inclusive, non-zero vector-clock run. Bulk callers pass
// sorted, non-overlapping ranges to JoinRanges.
type FiniteRange struct {
	First uint32
	Last  uint32
	Clock uint32
}

type finiteRun = FiniteRange

// RetiredRange is an inclusive range of never-reused logical thread IDs.
// Retirement is causal metadata rather than a finite clock value: every TID
// in the range reads as +infinity and can never become finite again.
type RetiredRange struct {
	First uint32
	Last  uint32
}

func New() *VectorClock          { return &VectorClock{} }
func NewFromPool() *VectorClock  { return poolGet() }
func (vc *VectorClock) Release() { poolPut(vc) }

// Reset clears values while retaining run and retirement buffers for reuse.
// Release applies the bounded pool-retention policy after resetting.
func (vc *VectorClock) Reset() {
	vc.causal.Release()
	if vc.ownerLineage != nil {
		vc.ownerLineage.Release()
		vc.ownerLineage = nil
	}
	for i := uint32(0); i <= uint32(vc.maxDense); i++ {
		vc.clocks[i] = 0
	}
	vc.maxDense = 0
	if vc.denseTailShared {
		vc.denseTail = nil
	} else {
		clear(vc.denseTail)
		vc.denseTail = vc.denseTail[:0]
	}
	vc.denseTailShared = false
	vc.invalidateDenseProjectionWitness()
	vc.sparseRuns = vc.sparseRuns[:0]
	vc.retired = vc.retired[:0]
	vc.base = nil
}

func (vc *VectorClock) ownerRoot() *CausalView {
	if vc == nil || vc.ownerLineage == nil {
		return nil
	}
	for i := 0; i < int(vc.causal.count); i++ {
		if vc.ownerLineage.Owns(vc.causal.roots[i]) {
			return &vc.causal.roots[i]
		}
	}
	return nil
}

const ownerLineageInlineFoldPoints = 8

// tryFoldOwnerResidual appends a tiny exact mutable residual to the context's
// linear lineage. This is the steady structured join case: a parent observes
// one or a few completed children and then forks again. Appending preserves
// old pinned versions and avoids rebuilding the increasingly wide ancestry on
// every sequential fork/join iteration. Broader/base/retirement shapes use the
// unrestricted exact reanchor below.
func (vc *VectorClock) tryFoldOwnerResidual(root *CausalView) bool {
	if vc == nil || root == nil || !root.Valid() || vc.ownerLineage == nil ||
		vc.base != nil || vc.causal.count != 1 || len(vc.retired) != 0 {
		return false
	}
	var points [ownerLineageInlineFoldPoints]FiniteRange
	count := 0
	complete := true
	vc.rangeOwnedRuns(func(first, last, clock uint32) bool {
		width := uint64(last) - uint64(first) + 1
		if width > uint64(len(points)-count) {
			complete = false
			return false
		}
		for tid := first; ; tid++ {
			points[count] = FiniteRange{First: tid, Last: tid, Clock: clock}
			count++
			if tid == last {
				break
			}
		}
		return true
	})
	if !complete {
		return false
	}
	for i := 0; i < count; i++ {
		point := points[i]
		_, appended := vc.ownerLineage.AppendOwned(root, point.First, point.Clock)
		if !appended && root.Get(point.First) < point.Clock {
			return false
		}
	}
	vc.clearOwned()
	return true
}

// EnsureOwnerLineage promotes a live context's complete exact clock into a
// compact immutable-version lineage. Repeated forks can pin the same ancestry
// in O(1); tiny child deltas append to the same family, while wider foreign
// joins are folded into a new immutable same-family anchor once.
func (vc *VectorClock) EnsureOwnerLineage(tid uint32) bool {
	if vc == nil || vc.IsRetired(tid) || vc.Get(tid) == 0 {
		return false
	}
	if vc.ownerLineage != nil && vc.ownerLineage.family.isolated.Load() {
		vc.materializeRoots()
	}
	if vc.ownerLineage != nil {
		if vc.ownerLineage.ownerTID != tid {
			return false
		}
		root := vc.ownerRoot()
		if root == nil {
			return false
		}
		if vc.base == nil && vc.ownedEmpty() && vc.causal.count == 1 {
			return true
		}
		if vc.tryFoldOwnerResidual(root) {
			return true
		}

		// Build the replacement before mutating the live clock. CloneDetached
		// deliberately retains only immutable views, so materializing it cannot
		// disturb this clock's private writer authority.
		canonical := vc.CloneDetached()
		canonical.materializeRoots()
		if !vc.ownerLineage.ReanchorOwned(root, canonical, false) {
			canonical.Release()
			return false
		}

		// ReanchorOwned moved the owned pin to the new exact anchor. Transfer
		// that one reference while releasing every now-folded residual root.
		var ownerView CausalView
		for i := 0; i < int(vc.causal.count); i++ {
			if vc.ownerLineage.Owns(vc.causal.roots[i]) {
				ownerView = vc.causal.roots[i]
				vc.causal.roots[i] = CausalView{}
				break
			}
		}
		vc.causal.Release()
		vc.clearOwned()
		vc.base = nil
		vc.causal.roots[0] = ownerView
		vc.causal.count = 1
		canonical.Release()
		return ownerView.Valid()
	}

	// The anchor captures base, roots, mutable finite state, and retirement
	// state before any of them are cleared. Promotion therefore collapses an
	// arbitrarily wide clock to one retained root instead of adding another
	// root beside the inherited ancestry.
	lineage, view := NewOwnerClockLineageFromClock(tid, vc)
	if !view.Valid() {
		lineage.Release()
		return false
	}
	vc.causal.Release()
	vc.clearOwned()
	vc.base = nil
	vc.causal.roots[0] = view
	vc.causal.count = 1
	vc.ownerLineage = lineage
	return true
}

// ContinueOwnerLineage advances an already-promoted, cohort-shared owner
// clock. An isolated family is deliberately left for renewed probation and a
// fresh family rather than republishing a representation which a long-lived
// single child has found unhelpful.
func (vc *VectorClock) ContinueOwnerLineage(tid uint32) bool {
	return vc != nil && vc.ownerLineage != nil &&
		!vc.ownerLineage.family.isolated.Load() && vc.EnsureOwnerLineage(tid)
}

func (vc *VectorClock) Clone() *VectorClock {
	clone := poolGet()
	clone.copyFromZero(vc)
	return clone
}

func (vc *VectorClock) copyFromZero(other *VectorClock) {
	vc.copyFromZeroWithDenseOwnership(other, false)
}

// CloneDetached is Clone with privately owned mutable buffers. It is used for
// immutable publication slots whose next reuse must copy allocation-free
// without making the live source clock copy-on-write.
func (vc *VectorClock) CloneDetached() *VectorClock {
	clone := poolGet()
	clone.copyFromZeroWithDenseOwnership(vc, true)
	return clone
}

// CloneForkDetached returns an exact independently mutable fork image. A
// confirmed fan-out may retain the owner's immutable lineage; a probationary
// fork materializes that lineage in the clone so a later long-lived child does
// not pay causal-root traversal merely because an earlier fan-out promoted its
// parent. The live parent and unrelated canonical clocks remain untouched.
func (vc *VectorClock) CloneForkDetached(shareOwnerLineage bool) *VectorClock {
	clone := vc.CloneDetached()
	if !shareOwnerLineage && vc.ownerLineage != nil {
		clone.materializeRoots()
	}
	return clone
}

const forkLineageCohortRefs = 12

// CollapseIsolatedForkLineage lowers an inherited owner-only root when its
// first acquire observes no sibling cohort. The reference threshold includes
// the lineage writer, the parent's owned view, children, and short-lived
// publication pins; it is only a performance decision. A marked family tells
// its private writer to lower before publishing another version, so subsequent
// one-child synchronization does not repeatedly import the same ancestry root.
func (vc *VectorClock) CollapseIsolatedForkLineage() bool {
	if vc == nil || !vc.causal.Valid() {
		return false
	}
	marked := false
	for i := 0; i < int(vc.causal.count); i++ {
		segment := vc.causal.roots[i].segment
		if segment == nil || !segment.ownerOnly {
			continue
		}
		if segment.family.isolated.Load() {
			marked = true
			break
		}
		// The private writer must not infer isolation merely because a completed
		// fan-out has released its children. Only an inheriting context can
		// observe whether it entered synchronization without live siblings.
		if vc.ownerLineage == nil && segment.refs.Load() < forkLineageCohortRefs {
			segment.family.isolated.Store(true)
			marked = true
			break
		}
	}
	if !marked {
		return false
	}
	vc.materializeRoots()
	return true
}

func (vc *VectorClock) copyFromZeroWithDenseOwnership(other *VectorClock, detachDense bool) {
	vc.base = other.base
	if !vc.causal.duplicateFrom(&other.causal) && other.causal.Valid() {
		runtimeThrow("race detector copied a released causal clock root set")
	}
	cloneLimit := uint32(other.maxDense)
	for i := uint32(0); i <= cloneLimit; i++ {
		vc.clocks[i] = other.clocks[i]
	}
	vc.maxDense = other.maxDense
	if len(other.denseTail) != 0 {
		if detachDense {
			if cap(vc.denseTail) < len(other.denseTail) {
				vc.denseTail = make([]uint32, len(other.denseTail))
			} else {
				vc.denseTail = vc.denseTail[:len(other.denseTail)]
			}
			copy(vc.denseTail, other.denseTail)
			vc.denseTailShared = false
		} else {
			vc.denseTail = other.denseTail
			vc.denseTailShared = true
			other.denseTailShared = true
		}
	}
	if len(other.sparseRuns) != 0 {
		if cap(vc.sparseRuns) < len(other.sparseRuns) {
			capacity := len(other.sparseRuns)
			// Canonical point insertion gives tiny sparse clocks four slots of
			// headroom. Preserve that same bounded preparation across Clone so an
			// immutable synchronization scratch version can absorb the next small
			// shape change without allocating. Larger peak capacities are not
			// inherited, keeping clones proportional to live metadata.
			if capacity < 4 && cap(other.sparseRuns) >= 4 {
				capacity = 4
			}
			vc.sparseRuns = make([]finiteRun, len(other.sparseRuns), capacity)
		} else {
			vc.sparseRuns = vc.sparseRuns[:len(other.sparseRuns)]
		}
		copy(vc.sparseRuns, other.sparseRuns)
	}
	vc.copyRetiredFromZero(other)
}

func (vc *VectorClock) copyRetiredFromZero(other *VectorClock) {
	if len(other.retired) == 0 {
		return
	}
	if cap(vc.retired) < len(other.retired) {
		vc.retired = make([]RetiredRange, len(other.retired))
	} else {
		vc.retired = vc.retired[:len(other.retired)]
	}
	copy(vc.retired, other.retired)
}

func (vc *VectorClock) ownedEmpty() bool {
	return vc.maxDense == 0 && vc.clocks[0] == 0 && len(vc.denseTail) == 0 &&
		len(vc.sparseRuns) == 0 && len(vc.retired) == 0
}

func (vc *VectorClock) denseTailEnd() uint64 {
	return uint64(DenseThreads) + uint64(len(vc.denseTail))
}

func (vc *VectorClock) detachDenseTail() {
	if !vc.denseTailShared {
		return
	}
	vc.invalidateDenseProjectionWitness()
	if len(vc.denseTail) == 0 {
		vc.denseTail = nil
		vc.denseTailShared = false
		return
	}
	next := make([]uint32, len(vc.denseTail))
	copy(next, vc.denseTail)
	vc.denseTail = next
	vc.denseTailShared = false
}

// maybePromoteDenseTail moves a dense prefix of sparse run metadata into
// direct-indexed storage. sparseRuns remains canonical and wholly above the
// dense tail. A tail is never created for fewer than minDenseTailRuns, while
// an existing tail may cheaply absorb a newly adjacent dense prefix.
func (vc *VectorClock) maybePromoteDenseTail() {
	if len(vc.sparseRuns) == 0 {
		return
	}
	start := vc.denseTailEnd()
	best, required := 0, uint64(0)
	for i := 0; i < len(vc.sparseRuns); i++ {
		run := vc.sparseRuns[i]
		if uint64(run.First) < start {
			runtimeThrow("race detector vector-clock representation overlap")
		}
		span := uint64(run.Last) + 1 - start
		runs := uint64(i + 1)
		if span <= runs*2 && (len(vc.denseTail) != 0 || i+1 >= minDenseTailRuns) {
			best, required = i+1, span
		}
	}
	targetLen := uint64(len(vc.denseTail)) + required
	if best == 0 || targetLen > uint64(^uint(0)>>1) {
		return
	}
	vc.invalidateDenseProjectionWitness()
	vc.detachDenseTail()
	oldLen := len(vc.denseTail)
	newLen := int(targetLen)
	if cap(vc.denseTail) < newLen {
		capacity := cap(vc.denseTail) * 2
		if capacity < cap(vc.denseTail) || capacity < newLen {
			capacity = newLen
		}
		if capacity < minDenseTailRuns {
			capacity = minDenseTailRuns
		}
		next := make([]uint32, newLen, capacity)
		copy(next, vc.denseTail)
		vc.denseTail = next
	} else {
		vc.denseTail = vc.denseTail[:newLen]
		clear(vc.denseTail[oldLen:])
	}
	for i := 0; i < best; i++ {
		run := vc.sparseRuns[i]
		first := int(uint64(run.First) - uint64(DenseThreads))
		last := int(uint64(run.Last) + 1 - uint64(DenseThreads))
		for i := first; i < last; i++ {
			vc.denseTail[i] = run.Clock
		}
	}
	copy(vc.sparseRuns, vc.sparseRuns[best:])
	for i := len(vc.sparseRuns) - best; i < len(vc.sparseRuns); i++ {
		vc.sparseRuns[i] = finiteRun{}
	}
	vc.sparseRuns = vc.sparseRuns[:len(vc.sparseRuns)-best]
	if len(vc.sparseRuns) == 0 {
		// Promotion makes this backing array pure retained capacity. Drop it so
		// each live goroutine clock does not keep a formerly fragmented frontier
		// alive after the dense tail has replaced it.
		vc.sparseRuns = nil
	}
}

func (vc *VectorClock) ensureDenseTail(length int) {
	vc.detachDenseTail()
	if length <= len(vc.denseTail) {
		return
	}
	if cap(vc.denseTail) < length {
		capacity := cap(vc.denseTail) * 2
		if capacity < cap(vc.denseTail) || capacity < length {
			capacity = length
		}
		next := make([]uint32, length, capacity)
		copy(next, vc.denseTail)
		vc.denseTail = next
		return
	}
	old := len(vc.denseTail)
	vc.denseTail = vc.denseTail[:length]
	clear(vc.denseTail[old:])
}

func (vc *VectorClock) extendDenseTail(length int) {
	vc.invalidateDenseProjectionWitness()
	vc.detachDenseTail()
	if length <= len(vc.denseTail) {
		return
	}
	vc.ensureDenseTail(length)
	end := uint64(DenseThreads) + uint64(length)
	consumed := 0
	for i := range vc.sparseRuns {
		run := &vc.sparseRuns[i]
		if uint64(run.First) >= end {
			break
		}
		last := uint64(run.Last) + 1
		if last > end {
			last = end
		}
		for tid := uint64(run.First); tid < last; tid++ {
			vc.denseTail[tid-DenseThreads] = run.Clock
		}
		if uint64(run.Last)+1 > end {
			run.First = uint32(end)
			break
		}
		consumed++
	}
	if consumed != 0 {
		copy(vc.sparseRuns, vc.sparseRuns[consumed:])
		for i := len(vc.sparseRuns) - consumed; i < len(vc.sparseRuns); i++ {
			vc.sparseRuns[i] = finiteRun{}
		}
		vc.sparseRuns = vc.sparseRuns[:len(vc.sparseRuns)-consumed]
	}
}

// Join performs the point-wise maximum vc = vc ⊔ other.
func (vc *VectorClock) Join(other *VectorClock) {
	if other == nil || vc == other {
		return
	}
	if vc.base != nil || other.base != nil {
		vc.JoinSnapshot(joinClockSnapshots(other.base, other.ownedSnapshot()))
	} else {
		if len(vc.retired) == 0 {
			for i := uint32(0); i <= uint32(other.maxDense); i++ {
				if other.clocks[i] > vc.clocks[i] {
					vc.clocks[i] = other.clocks[i]
				}
			}
			if other.maxDense > vc.maxDense {
				vc.maxDense = other.maxDense
			}
		} else {
			for i := uint32(0); i <= uint32(other.maxDense); i++ {
				if clock := other.clocks[i]; clock > vc.Get(i) {
					vc.Set(i, clock)
				}
			}
		}
		if len(other.denseTail) != 0 {
			vc.extendDenseTail(len(other.denseTail))
			for i, clock := range other.denseTail {
				if clock != 0 && clock > vc.denseTail[i] {
					tid := uint32(DenseThreads + i)
					if len(vc.retired) == 0 || !vc.IsRetired(tid) {
						vc.denseTail[i] = clock
					}
				}
			}
		}
		// The two clocks may use opposite representations for the same high-TID
		// coordinates. Import the source's sparse prefix covered by our dense tail
		// before joinSparseRuns discards everything below the sparse floor.
		vc.joinSparseRuns(vc.joinDenseTailRanges(other.sparseRuns))
		hadOtherRetirement := len(other.retired) != 0
		vc.RetireRanges(other.retired)
		if !hadOtherRetirement && len(vc.retired) != 0 {
			vc.dropRetiredFiniteEntries()
		}
	}
	for i := 0; i < int(other.causal.count); i++ {
		vc.JoinCausal(other.causal.roots[i])
	}
}

// TryJoin is an allocation-free, nonblocking Join. It intentionally returns
// false before mutation for representation combinations whose exact merge may
// need scratch storage. Dense overlays and immutable checkpoints cover the
// pinned synchronization fast paths; Join remains the unrestricted fallback.
func (vc *VectorClock) TryJoin(other *VectorClock) bool {
	if vc == other || other == nil || other.ownedEmpty() && other.base == nil && !other.causal.Valid() {
		return true
	}
	if vc == nil {
		return false
	}
	if !vc.causal.canJoinSet(&other.causal) {
		return false
	}
	if other.base == nil && other.ownedEmpty() {
		return vc.causal.tryJoinSet(&other.causal)
	}
	if !vc.causal.Valid() && !other.causal.Valid() && clockLessOrEqualClock(other, vc) {
		return true
	}
	if vc.base == nil && vc.ownedEmpty() && !vc.causal.Valid() {
		return vc.TryCopyFrom(other)
	}
	// Equal mutable layouts can be maxed and coalesced in place. Preflight every
	// buffer before adopting an immutable root so failure remains all-or-nothing.
	if len(other.retired) != 0 || len(vc.denseTail) != len(other.denseTail) ||
		len(vc.sparseRuns) != len(other.sparseRuns) {
		return false
	}
	for i := range other.sparseRuns {
		if vc.sparseRuns[i].First != other.sparseRuns[i].First ||
			vc.sparseRuns[i].Last != other.sparseRuns[i].Last {
			return false
		}
	}
	if vc.denseTailShared {
		for i, clock := range other.denseTail {
			if clock > vc.denseTail[i] && !vc.IsRetired(uint32(DenseThreads+i)) {
				return false
			}
		}
	}
	if other.base != nil && !vc.TryJoinSnapshot(other.base) {
		return false
	}
	// The exact join may update any dense coordinate. Invalidating a witness
	// when the values happen to be unchanged is conservative; retaining it
	// across a foreign-coordinate maximum would not be.
	vc.invalidateDenseProjectionWitness()
	for tid := uint32(0); tid <= uint32(other.maxDense); tid++ {
		if clock := other.clocks[tid]; clock > vc.Get(tid) {
			// Inline Set cannot allocate. Retirement in vc simply dominates it.
			vc.Set(tid, clock)
		}
	}
	for i, clock := range other.denseTail {
		if clock > vc.denseTail[i] && !vc.IsRetired(uint32(DenseThreads+i)) {
			vc.denseTail[i] = clock
		}
	}
	for i, run := range other.sparseRuns {
		if run.Clock > vc.sparseRuns[i].Clock {
			vc.sparseRuns[i].Clock = run.Clock
		}
	}
	vc.sparseRuns = coalesceFiniteRuns(vc.sparseRuns)
	return vc.causal.tryJoinSet(&other.causal)
}

// TryJoinCausal pointwise joins one pinned immutable lineage view without
// allocating. A clock owns its own segment reference: the caller remains
// responsible for releasing view. Same-family advancement is an O(1) pointer
// replacement and preserves the snapshot base and mutable owned overlay.
// Unrelated families occupy another inline slot until capacity is reached.
func (vc *VectorClock) TryJoinCausal(view CausalView) bool {
	return vc != nil && vc.causal.tryJoin(view)
}

// BorrowCausalRoots copies vc's retained roots into dst without retaining
// them. The returned views remain owned by vc and are valid only while vc is
// kept alive and unmodified. Unused entries are cleared.
func (vc *VectorClock) BorrowCausalRoots(dst *[CausalRootCapacity]CausalView) int {
	if dst == nil {
		return 0
	}
	clear(dst[:])
	if vc == nil {
		return 0
	}
	n := int(vc.causal.count)
	copy(dst[:n], vc.causal.roots[:n])
	return n
}

// CanJoinCausalSet reports whether all views fit in vc's inline root set after
// same-family normalization. It neither retains nor mutates anything.
func (vc *VectorClock) CanJoinCausalSet(views *[CausalRootCapacity]CausalView, n int) bool {
	if vc == nil || views == nil || n < 0 || n > len(views) {
		return false
	}
	families := int(vc.causal.count)
	for i := 0; i < n; i++ {
		view := views[i]
		if !view.Valid() {
			continue
		}
		known := false
		for j := 0; j < int(vc.causal.count); j++ {
			if vc.causal.roots[j].SameFamily(view) {
				known = true
				break
			}
		}
		for j := 0; !known && j < i; j++ {
			if views[j].Valid() && views[j].SameFamily(view) {
				known = true
			}
		}
		if !known {
			families++
			if families > CausalRootCapacity {
				return false
			}
		}
	}
	return true
}

// TryJoinCausalSet joins a preflighted fixed set allocation-free. Capacity is
// checked before the first mutation, preserving Try's all-or-nothing contract
// for every ordinary live-view call.
func (vc *VectorClock) TryJoinCausalSet(views *[CausalRootCapacity]CausalView, n int) bool {
	if !vc.CanJoinCausalSet(views, n) {
		return false
	}
	for i := 0; i < n; i++ {
		if !vc.causal.tryJoin(views[i]) {
			runtimeThrow("race detector joined a released causal clock root")
		}
	}
	return true
}

const maxCausalResidualProofRun = uint64(64)

// ResidualLessOrEqualCausal proves that every logical coordinate in vc, except
// ownTID, is dominated by view. It is read-only and allocation-free. False
// means "not proven", not necessarily logical inequality: wide finite and
// retirement residuals are rejected conservatively rather than expanded
// without bound.
//
// The synchronization hot path is constant time for vc's causal root when
// view is a later version of the same family; only the small residual base and
// owned overlay are scanned.
func (vc *VectorClock) ResidualLessOrEqualCausal(view CausalView, ownTID uint32) bool {
	views := [CausalRootCapacity]CausalView{view}
	return vc.ResidualLessOrEqualCausalSet(&views, 1, ownTID)
}

// ResidualLessOrEqualCausalSet proves that every logical coordinate in vc,
// except ownTID, is dominated by the pointwise union of views. It is read-only
// and allocation-free. False means not proven; deliberately bounded scans of
// wide residual runs preserve the synchronization hot-path ceiling.
func (vc *VectorClock) ResidualLessOrEqualCausalSet(views *[CausalRootCapacity]CausalView, n int, ownTID uint32) bool {
	if vc == nil {
		return true
	}
	if views == nil || n < 0 || n > len(views) {
		return false
	}
	for i := 0; i < int(vc.causal.count); i++ {
		left := vc.causal.roots[i]
		dominated := false
		for j := 0; j < n; j++ {
			if views[j].Dominates(left) {
				dominated = true
				break
			}
		}
		if !dominated && !causalViewResidualLessOrEqualSet(left, views, n, ownTID) {
			return false
		}
	}
	for _, retired := range vc.retired {
		if !causalSetDominatesRetiredRun(views, n, retired.First, retired.Last, ownTID) {
			return false
		}
	}
	if vc.base != nil {
		ok := true
		snapshotRange(vc.base.retired, func(first, last, _ uint32) bool {
			ok = causalSetDominatesRetiredRun(views, n, first, last, ownTID)
			return ok
		})
		if !ok {
			return false
		}
		snapshotRange(vc.base.finite, func(first, last, clock uint32) bool {
			ok = causalSetDominatesFiniteRun(views, n, first, last, clock, ownTID)
			return ok
		})
		if !ok {
			return false
		}
	}
	ok := true
	vc.rangeOwnedRuns(func(first, last, clock uint32) bool {
		ok = causalSetDominatesFiniteRun(views, n, first, last, clock, ownTID)
		return ok
	})
	return ok
}

func causalSetGet(views *[CausalRootCapacity]CausalView, n int, tid uint32) uint32 {
	var clock uint32
	for i := 0; i < n; i++ {
		if candidate := views[i].Get(tid); candidate > clock {
			clock = candidate
		}
	}
	return clock
}

func causalSetIsRetired(views *[CausalRootCapacity]CausalView, n int, tid uint32) bool {
	for i := 0; i < n; i++ {
		if views[i].IsRetired(tid) {
			return true
		}
	}
	return false
}

func causalSetDominatesFiniteRun(views *[CausalRootCapacity]CausalView, n int, first, last, clock, skip uint32) bool {
	residual := uint64(last) - uint64(first) + 1
	if skip >= first && skip <= last {
		residual--
	}
	if residual > maxCausalResidualProofRun {
		return false
	}
	for tid := uint64(first); tid <= uint64(last); tid++ {
		id := uint32(tid)
		if id != skip && !causalSetIsRetired(views, n, id) && causalSetGet(views, n, id) < clock {
			return false
		}
	}
	return true
}

func causalSetDominatesRetiredRun(views *[CausalRootCapacity]CausalView, n int, first, last, skip uint32) bool {
	residual := uint64(last) - uint64(first) + 1
	if skip >= first && skip <= last {
		residual--
	}
	if residual > maxCausalResidualProofRun {
		return false
	}
	for tid := uint64(first); tid <= uint64(last); tid++ {
		id := uint32(tid)
		if id != skip && !causalSetIsRetired(views, n, id) {
			return false
		}
	}
	return true
}

// causalViewResidualLessOrEqual is the exact cold path for an unrelated or
// newer source root. Anchors and published point heads are immutable for a
// pinned version, so they can be checked without materializing a VectorClock.
func causalViewResidualLessOrEqualSet(left CausalView, right *[CausalRootCapacity]CausalView, n int, skip uint32) bool {
	if !left.Valid() {
		return true
	}
	ok := true
	left.segment.anchor.RangeRetired(func(first, last uint32) bool {
		ok = causalSetDominatesRetiredRun(right, n, first, last, skip)
		return ok
	})
	if !ok {
		return false
	}
	left.segment.anchor.RangeRuns(func(first, last, clock uint32) bool {
		ok = causalSetDominatesFiniteRun(right, n, first, last, clock, skip)
		return ok
	})
	if !ok {
		return false
	}
	for tid := range left.segment.denseHeads {
		logicalTID := left.segment.denseBase + uint32(tid)
		index, _ := left.segment.denseHeads[tid].load()
		if index == 0 || logicalTID == skip {
			continue
		}
		clock := left.Get(logicalTID)
		if !causalSetIsRetired(right, n, logicalTID) && clock > causalSetGet(right, n, logicalTID) {
			return false
		}
	}
	for i := range left.segment.cells {
		key := left.segment.cells[i].key.Load()
		if key == 0 {
			continue
		}
		tid := uint32(key - 1)
		if tid != skip && !causalSetIsRetired(right, n, tid) && left.Get(tid) > causalSetGet(right, n, tid) {
			return false
		}
	}
	return true
}

// JoinCausal is the unrestricted counterpart to TryJoinCausal. On inline-set
// overflow all retained roots are materialized exactly before the incoming root
// is adopted; no causal metadata is dropped.
func (vc *VectorClock) JoinCausal(view CausalView) {
	if vc.TryJoinCausal(view) {
		return
	}
	vc.materializeCausal()
	if !vc.TryJoinCausal(view) {
		runtimeThrow("race detector joined a released causal clock view")
	}
}

func (vc *VectorClock) joinSparseRuns(other []finiteRun) {
	floor64 := vc.denseTailEnd()
	if floor64 > uint64(^uint32(0)) {
		return
	}
	floor := uint32(floor64)
	for len(other) != 0 && other[0].Last < floor {
		other = other[1:]
	}
	if len(other) == 0 {
		return
	}
	otherFirst := other[0].First
	if otherFirst < floor {
		otherFirst = floor
	}
	if otherFirst > other[0].Last {
		return
	}
	if sparseRangesLessOrEqual(other, floor, vc.sparseRuns, vc.retired) {
		return
	}
	// When the incoming sparse frontier dominates every owned destination run,
	// the pointwise union is exactly the incoming layout. Replacing in retained
	// destination storage avoids allocating a temporary merge result on the
	// common acquire path. Retirement is kept separately as +infinity, so this
	// shortcut is valid only when no incoming finite run crosses it.
	if !sparseRunsOverlapRetiredFrom(other, floor, vc.retired) &&
		sparseRangesLessOrEqual(vc.sparseRuns, floor, other, nil) {
		if cap(vc.sparseRuns) < len(other) {
			capacity := cap(vc.sparseRuns) * 2
			if capacity < cap(vc.sparseRuns) || capacity < len(other) {
				capacity = len(other)
			}
			vc.sparseRuns = make([]finiteRun, len(other), capacity)
		} else {
			vc.sparseRuns = vc.sparseRuns[:len(other)]
		}
		copy(vc.sparseRuns, other)
		vc.sparseRuns[0].First = otherFirst
		vc.sparseRuns = coalesceFiniteRuns(vc.sparseRuns)
		vc.maybePromoteDenseTail()
		return
	}
	if len(vc.sparseRuns) == 0 {
		if !sparseRunsOverlapRetiredFrom(other, floor, vc.retired) {
			if cap(vc.sparseRuns) < len(other) {
				vc.sparseRuns = make([]finiteRun, 0, len(other))
			}
			for i := 0; i < len(other); i++ {
				r := other[i]
				if i == 0 {
					r.First = otherFirst
				}
				vc.sparseRuns = appendFiniteRun(vc.sparseRuns, r)
			}
			vc.maybePromoteDenseTail()
			return
		}
	}
	// A growing frontier commonly contributes only TIDs above the receiver's
	// current sparse maximum. Appending that suffix cannot overwrite unread
	// receiver state, so retained destination capacity is safe to reuse.
	if len(vc.sparseRuns) != 0 && vc.sparseRuns[len(vc.sparseRuns)-1].Last < otherFirst &&
		!sparseRunsOverlapRetiredFrom(other, floor, vc.retired) {
		for i := 0; i < len(other); i++ {
			r := other[i]
			if i == 0 {
				r.First = otherFirst
			}
			vc.sparseRuns = appendFiniteRun(vc.sparseRuns, r)
		}
		vc.maybePromoteDenseTail()
		return
	}

	// Repeated release-merge/acquire frequently advances clocks without
	// changing their run boundaries. Update that layout in place and coalesce
	// any newly equal neighbors instead of allocating a replacement slice.
	if len(vc.sparseRuns) == len(other) && !sparseRunsOverlapRetiredFrom(other, floor, vc.retired) {
		sameLayout := true
		for i := 0; i < len(other); i++ {
			r := other[i]
			first := r.First
			if i == 0 && first < floor {
				first = floor
			}
			if vc.sparseRuns[i].First != first || vc.sparseRuns[i].Last != r.Last {
				sameLayout = false
				break
			}
		}
		if sameLayout {
			for i := 0; i < len(other); i++ {
				r := other[i]
				if r.Clock > vc.sparseRuns[i].Clock {
					vc.sparseRuns[i].Clock = r.Clock
				}
			}
			vc.sparseRuns = coalesceFiniteRuns(vc.sparseRuns)
			vc.maybePromoteDenseTail()
			return
		}
	}

	const end = uint64(1) << 32
	left := vc.sparseRuns
	var out []finiteRun
	var joinShard *clockPoolShard
	shardIndex := int(vc.poolShard)
	// Borrow only an already-large-enough snapshot. Otherwise the ordinary
	// allocating merge runs without a snapshot; after it finishes, the old
	// receiver backing can seed the bounded shard scratch for a later join.
	// This avoids ever allocating both a snapshot and a grown output.
	if shardIndex < poolShardCount {
		shard := &poolShards[shardIndex]
		if shard.joinLock.CompareAndSwap(0, 1) {
			savedScratch := shard.joinScratch
			if cap(savedScratch) >= len(left) {
				joinShard = shard
				snapshot := savedScratch[:len(left)]
				copy(snapshot, left)
				left = snapshot
				out = vc.sparseRuns[:0]
			} else {
				shard.joinLock.Store(0)
			}
		}
	}
	if joinShard == nil {
		out = make([]finiteRun, 0, len(left)+len(other))
	}
	i, j, retiredIndex := 0, 0, 0
	pos := uint64(otherFirst)
	if len(left) != 0 && uint64(left[0].First) < pos {
		pos = uint64(left[0].First)
	}
	for retiredIndex < len(vc.retired) && uint64(vc.retired[retiredIndex].Last) < pos {
		retiredIndex++
	}
	for pos < end {
		for i < len(left) && uint64(left[i].Last) < pos {
			i++
		}
		for j < len(other) && uint64(other[j].Last) < pos {
			j++
		}
		for retiredIndex < len(vc.retired) && uint64(vc.retired[retiredIndex].Last) < pos {
			retiredIndex++
		}

		if retiredIndex < len(vc.retired) {
			r := vc.retired[retiredIndex]
			if uint64(r.First) <= pos {
				pos = uint64(r.Last) + 1
				continue
			}
		}

		leftClock, leftNext := uint32(0), end
		if i < len(left) {
			r := left[i]
			if pos < uint64(r.First) {
				leftNext = uint64(r.First)
			} else {
				leftClock = r.Clock
				leftNext = uint64(r.Last) + 1
			}
		}
		rightClock, rightNext := uint32(0), end
		if j < len(other) {
			r := other[j]
			first := r.First
			if j == 0 && first < floor {
				first = floor
			}
			if pos < uint64(first) {
				rightNext = uint64(first)
			} else {
				rightClock = r.Clock
				rightNext = uint64(r.Last) + 1
			}
		}
		next := leftNext
		if rightNext < next {
			next = rightNext
		}
		if retiredIndex < len(vc.retired) && uint64(vc.retired[retiredIndex].First) < next {
			next = uint64(vc.retired[retiredIndex].First)
		}
		clock := leftClock
		if rightClock > clock {
			clock = rightClock
		}
		if clock != 0 && next > pos {
			out = appendFiniteRun(out, finiteRun{First: uint32(pos), Last: uint32(next - 1), Clock: clock})
		}
		if next == end {
			break
		}
		pos = next
	}
	vc.sparseRuns = out
	if joinShard != nil {
		joinShard.joinScratch = left[:0]
		joinShard.joinLock.Store(0)
	} else if shardIndex < poolShardCount && cap(left) != 0 &&
		uintptr(cap(left))*unsafe.Sizeof(FiniteRange{}) <= maxPooledMetadataBytes {
		// The allocating merge no longer reads the receiver's old private
		// backing. Publish it only when it improves the bounded shard buffer.
		shard := &poolShards[shardIndex]
		if shard.joinLock.CompareAndSwap(0, 1) {
			if cap(shard.joinScratch) < cap(left) {
				shard.joinScratch = left[:0]
			}
			shard.joinLock.Store(0)
		}
	}
	vc.maybePromoteDenseTail()
}

func sparseRunsOverlapRetiredFrom(runs []finiteRun, floor uint32, retired []RetiredRange) bool {
	i, j := 0, 0
	for i < len(runs) && runs[i].Last < floor {
		i++
	}
	for i < len(runs) && j < len(retired) {
		first := runs[i].First
		if first < floor {
			first = floor
		}
		if runs[i].Last < first {
			i++
			continue
		}
		if runs[i].Last < retired[j].First {
			i++
			continue
		}
		if retired[j].Last < first {
			j++
			continue
		}
		return true
	}
	return false
}

func sparseRangesLessOrEqual(left []finiteRun, floor uint32, right []finiteRun, rightRetired []RetiredRange) bool {
	runIndex, retiredIndex := 0, 0
	for i := 0; i < len(left); i++ {
		l := left[i]
		cursor := uint64(l.First)
		if cursor < uint64(floor) {
			cursor = uint64(floor)
		}
		limit := uint64(l.Last)
		if cursor > limit {
			continue
		}
		for cursor <= limit {
			for runIndex < len(right) && uint64(right[runIndex].Last) < cursor {
				runIndex++
			}
			for retiredIndex < len(rightRetired) && uint64(rightRetired[retiredIndex].Last) < cursor {
				retiredIndex++
			}
			if retiredIndex < len(rightRetired) {
				r := rightRetired[retiredIndex]
				if uint64(r.First) <= cursor {
					cursor = uint64(r.Last) + 1
					continue
				}
			}
			if runIndex == len(right) || uint64(right[runIndex].First) > cursor || right[runIndex].Clock < l.Clock {
				return false
			}
			covered := uint64(right[runIndex].Last)
			if retiredIndex < len(rightRetired) && uint64(rightRetired[retiredIndex].First) <= covered {
				covered = uint64(rightRetired[retiredIndex].First) - 1
			}
			cursor = covered + 1
		}
	}
	return true
}

func (vc *VectorClock) LessOrEqual(other *VectorClock) bool {
	if vc.causal.Valid() || other.causal.Valid() {
		left, right := vc.CloneDetached(), other.CloneDetached()
		left.materializeCausal()
		right.materializeCausal()
		result := clockLessOrEqualClock(left, right)
		left.Release()
		right.Release()
		return result
	}
	if vc.base != nil || other.base != nil {
		return clockLessOrEqualClock(vc, other)
	}
	// +infinity is dominated only by +infinity. Check marker coverage
	// independently from finite uint32 values so MaxUint32 remains a valid
	// (albeit non-incrementable) finite component.
	if !retiredSubset(vc.retired, other.retired) {
		return false
	}
	for i := uint32(0); i <= uint32(vc.maxDense); i++ {
		if vc.clocks[i] > other.Get(i) {
			return false
		}
	}
	for i, clock := range vc.denseTail {
		if clock != 0 && clock > other.Get(uint32(DenseThreads+i)) {
			return false
		}
	}
	otherEnd := other.denseTailEnd()
	for _, run := range vc.sparseRuns {
		limit := uint64(run.Last) + 1
		if limit > otherEnd {
			limit = otherEnd
		}
		for tid := uint64(run.First); tid < limit; tid++ {
			if run.Clock > other.Get(uint32(tid)) {
				return false
			}
		}
	}
	if otherEnd > uint64(^uint32(0)) {
		return true
	}
	return sparseRangesLessOrEqual(vc.sparseRuns, uint32(otherEnd), other.sparseRuns, other.retired)
}

func clockLessOrEqualClock(left, right *VectorClock) bool {
	if left == right {
		return true
	}
	if left.causal.Valid() || right.causal.Valid() {
		leftCopy, rightCopy := left.Clone(), right.Clone()
		leftCopy.materializeCausal()
		rightCopy.materializeCausal()
		result := clockLessOrEqualClock(leftCopy, rightCopy)
		leftCopy.Release()
		rightCopy.Release()
		return result
	}
	ok := true
	left.RangeRetired(func(first, last uint32) bool {
		for pos, limit := uint64(first), uint64(last)+1; pos < limit; {
			retired, _, next := right.logicalSegment(pos)
			if !retired {
				ok = false
				return false
			}
			if next > limit {
				next = limit
			}
			pos = next
		}
		return true
	})
	if !ok {
		return false
	}
	left.RangeRuns(func(first, last, clock uint32) bool {
		for pos, limit := uint64(first), uint64(last)+1; pos < limit; {
			retired, value, next := right.logicalSegment(pos)
			if !retired && value < clock {
				ok = false
				return false
			}
			if next > limit {
				next = limit
			}
			pos = next
		}
		return true
	})
	return ok
}

func sparseRunsLessOrEqual(left, right []finiteRun, rightRetired []RetiredRange) bool {
	return sparseRangesLessOrEqual(left, 0, right, rightRetired)
}

// PruneLessOrEqual removes each finite component already observed by observed.
// It never imports observed's finite components or retirement metadata.
func (vc *VectorClock) PruneLessOrEqual(observed *VectorClock) {
	vc.materializeRoots()
	if observed.base != nil || observed.causal.Valid() {
		// Promoted ordinary-read frontiers contain only a handful of actual read
		// events, while the observing goroutine may carry a process-wide causal
		// root. Prune that small event set directly through point lookup before
		// materializing the root. The bounded scan preserves the generic pruning
		// ceiling; wider frontiers retain the canonical exact fallback below.
		if vc.pruneSmallOwnedFiniteAgainstLogical(observed) {
			return
		}
		// Pruning observes but must not change the representation or sharing of
		// its argument. The existing sparse pruning engine consumes owned runs,
		// so materialize a private temporary when necessary.
		copy := observed.CloneDetached()
		copy.materializeRoots()
		defer copy.Release()
		observed = copy
	}
	vc.invalidateDenseProjectionWitness()
	vc.detachDenseTail()
	for tid := uint32(0); tid <= uint32(vc.maxDense); tid++ {
		if clock := vc.clocks[tid]; clock != 0 && clock <= observed.Get(tid) {
			vc.clocks[tid] = 0
		}
	}
	for vc.maxDense != 0 && vc.clocks[vc.maxDense] == 0 {
		vc.maxDense--
	}
	for i, clock := range vc.denseTail {
		if clock != 0 && clock <= observed.Get(uint32(DenseThreads+i)) {
			vc.denseTail[i] = 0
		}
	}
	if len(vc.sparseRuns) == 0 {
		return
	}
	vc.pruneSparseAgainstDenseTail(observed)
	if len(vc.sparseRuns) == 0 {
		return
	}
	if !sparseRunsHavePrunable(vc.sparseRuns, observed.sparseRuns, observed.retired) {
		return
	}
	if sparseRunsLessOrEqual(vc.sparseRuns, observed.sparseRuns, observed.retired) {
		vc.sparseRuns = vc.sparseRuns[:0]
		return
	}

	out := make([]finiteRun, 0, len(vc.sparseRuns))
	observedRun, observedRetired := 0, 0
	for _, run := range vc.sparseRuns {
		cursor := uint64(run.First)
		limit := uint64(run.Last)
		for cursor <= limit {
			for observedRun < len(observed.sparseRuns) && uint64(observed.sparseRuns[observedRun].Last) < cursor {
				observedRun++
			}
			for observedRetired < len(observed.retired) && uint64(observed.retired[observedRetired].Last) < cursor {
				observedRetired++
			}
			if observedRetired < len(observed.retired) && uint64(observed.retired[observedRetired].First) <= cursor {
				cursor = uint64(observed.retired[observedRetired].Last) + 1
				continue
			}

			next := limit + 1
			observedClock := uint32(0)
			if observedRun < len(observed.sparseRuns) {
				r := observed.sparseRuns[observedRun]
				if uint64(r.First) <= cursor {
					observedClock = r.Clock
					if end := uint64(r.Last) + 1; end < next {
						next = end
					}
				} else if uint64(r.First) < next {
					next = uint64(r.First)
				}
			}
			if observedRetired < len(observed.retired) {
				if start := uint64(observed.retired[observedRetired].First); start < next {
					next = start
				}
			}
			if run.Clock > observedClock && next > cursor {
				out = appendFiniteRun(out, finiteRun{First: uint32(cursor), Last: uint32(next - 1), Clock: run.Clock})
			}
			cursor = next
		}
	}
	vc.sparseRuns = out
}

// PruneEventSetLessOrEqual is the exact pruning seam for a clock whose finite
// coordinates are independent events rather than a causal snapshot. Promoted
// ordinary-read frontiers use one finite coordinate per logical reader (the
// canonical storage may coalesce adjacent equal epochs), so their semantic
// coordinates can be scanned directly against an arbitrarily wide observing
// clock without materializing its immutable causal roots. This API is for
// event sets, not manufactured wide ranges. Pathologically wide coalesced
// inputs retain PruneLessOrEqual's exact structural fallback.
func (vc *VectorClock) PruneEventSetLessOrEqual(observed *VectorClock) {
	if vc == nil || observed == nil {
		return
	}
	if vc.base != nil || vc.causal.Valid() || len(vc.retired) != 0 {
		vc.PruneLessOrEqual(observed)
		return
	}
	allSingleton := true
	for _, run := range vc.sparseRuns {
		if run.First != run.Last {
			allSingleton = false
			break
		}
	}
	if !allSingleton {
		// Bound point lookup before inspecting the observer. Real promoted read
		// frontiers contain one coordinate per reader; a manufactured or
		// pathological composite range uses the exact structural fallback.
		const maxDirectCompositeEventCoordinates = uint64(64 * 1024)
		var represented uint64
		vc.rangeOwnedRuns(func(first, last, _ uint32) bool {
			represented += uint64(last) - uint64(first) + 1
			return represented <= maxDirectCompositeEventCoordinates
		})
		if represented > maxDirectCompositeEventCoordinates {
			vc.PruneLessOrEqual(observed)
			return
		}
	}
	// A promoted read frontier is an event set even when canonical storage has
	// coalesced adjacent readers with equal epochs into one finite run. Prune it
	// through point lookup: materializing a process-wide causal observer merely
	// to split the represented reader events turns mutex handoff into work
	// proportional to unrelated goroutines.
	total, kept := uint64(0), uint64(0)
	vc.rangeOwnedRuns(func(first, last, clock uint32) bool {
		width := uint64(last) - uint64(first) + 1
		total += width
		kept += observed.countUnobservedEventRange(first, last, clock)
		return true
	})
	if kept == total {
		return
	}
	if kept == 0 {
		for i := uint32(0); i <= uint32(vc.maxDense); i++ {
			vc.clocks[i] = 0
		}
		vc.maxDense = 0
		vc.invalidateDenseProjectionWitness()
		vc.detachDenseTail()
		clear(vc.denseTail)
		vc.denseTail = vc.denseTail[:0]
		clear(vc.sparseRuns)
		vc.sparseRuns = vc.sparseRuns[:0]
		return
	}
	if allSingleton {
		vc.prunePartialSingletonEventSet(observed)
		return
	}
	vc.prunePartialCompositeEventSet(observed)
}

func (observed *VectorClock) countUnobservedEventRange(first, last, clock uint32) uint64 {
	var kept uint64
	for pos, limit := uint64(first), uint64(last); pos <= limit; {
		if pos%clockImageDenseMinBlock == 0 && pos+clockImageDenseMinBlock-1 <= limit &&
			observed.causal.anchorDominatesAlignedBlock(uint32(pos), clock) {
			pos += clockImageDenseMinBlock
			continue
		}
		if observed.Get(uint32(pos)) < clock {
			kept++
		}
		pos++
	}
	return kept
}

func (observed *VectorClock) appendUnobservedEventRange(out []finiteRun, first, last, clock uint32) []finiteRun {
	for pos, limit := uint64(first), uint64(last); pos <= limit; {
		if pos%clockImageDenseMinBlock == 0 && pos+clockImageDenseMinBlock-1 <= limit &&
			observed.causal.anchorDominatesAlignedBlock(uint32(pos), clock) {
			pos += clockImageDenseMinBlock
			continue
		}
		if observed.Get(uint32(pos)) < clock {
			id := uint32(pos)
			out = appendFiniteRun(out, finiteRun{First: id, Last: id, Clock: clock})
		}
		pos++
	}
	return out
}

func (vc *VectorClock) pruneOwnedDenseEvents(observed *VectorClock) {
	vc.invalidateDenseProjectionWitness()
	for tid := uint32(0); tid <= uint32(vc.maxDense); tid++ {
		if clock := vc.clocks[tid]; clock != 0 && observed.Get(tid) >= clock {
			vc.clocks[tid] = 0
		}
	}
	for vc.maxDense != 0 && vc.clocks[vc.maxDense] == 0 {
		vc.maxDense--
	}
	vc.detachDenseTail()
	for i, clock := range vc.denseTail {
		tid := uint32(DenseThreads + i)
		if clock != 0 && observed.Get(tid) >= clock {
			vc.denseTail[i] = 0
		}
	}
}

func (vc *VectorClock) prunePartialSingletonEventSet(observed *VectorClock) {
	vc.pruneOwnedDenseEvents(observed)
	oldSparse := vc.sparseRuns
	out := oldSparse[:0]
	for _, run := range oldSparse {
		if observed.Get(run.First) < run.Clock {
			out = append(out, run)
		}
	}
	if len(out) < len(oldSparse) {
		clear(oldSparse[len(out):])
	}
	vc.sparseRuns = out
}

func (vc *VectorClock) prunePartialCompositeEventSet(observed *VectorClock) {
	// Preserve the input runs while rebuilding because one coalesced run may
	// split into several retained runs. The common promoted frontier has only a
	// handful of runs, so keep that snapshot on this composite-only stack frame.
	// A fragmented bounded frontier copies only its represented read events;
	// this remains far smaller than materializing the observer's causal roots.
	const inlineCompositeRuns = 16
	var composite [inlineCompositeRuns]finiteRun
	var sparse []finiteRun
	if len(vc.sparseRuns) <= len(composite) {
		copy(composite[:], vc.sparseRuns)
		sparse = composite[:len(vc.sparseRuns)]
	} else {
		sparse = append([]finiteRun(nil), vc.sparseRuns...)
	}
	vc.pruneOwnedDenseEvents(observed)
	oldSparse := vc.sparseRuns
	out := oldSparse[:0]
	for _, run := range sparse {
		out = observed.appendUnobservedEventRange(out, run.First, run.Last, run.Clock)
	}
	if len(out) < len(oldSparse) {
		clear(oldSparse[len(out):])
	}
	vc.sparseRuns = out
}

func (vc *VectorClock) pruneSmallOwnedFiniteAgainstLogical(observed *VectorClock) bool {
	if vc == nil || observed == nil {
		return vc == nil
	}
	type point struct {
		tid, clock uint32
	}
	var keep [maxCausalResidualProofRun]point
	keepN := 0
	checked := uint64(0)
	complete := true
	vc.rangeOwnedRuns(func(first, last, clock uint32) bool {
		width := uint64(last) - uint64(first) + 1
		if width > maxCausalResidualProofRun-checked {
			complete = false
			return false
		}
		checked += width
		for tid := uint64(first); tid <= uint64(last); tid++ {
			id := uint32(tid)
			if !observed.IsRetired(id) && observed.Get(id) < clock {
				keep[keepN] = point{tid: id, clock: clock}
				keepN++
			}
		}
		return true
	})
	if !complete {
		return false
	}
	for i := uint32(0); i <= uint32(vc.maxDense); i++ {
		vc.clocks[i] = 0
	}
	vc.maxDense = 0
	vc.invalidateDenseProjectionWitness()
	vc.detachDenseTail()
	clear(vc.denseTail)
	vc.denseTail = vc.denseTail[:0]
	vc.sparseRuns = vc.sparseRuns[:0]
	for i := 0; i < keepN; i++ {
		vc.Set(keep[i].tid, keep[i].clock)
	}
	return true
}

func (vc *VectorClock) pruneSparseAgainstDenseTail(observed *VectorClock) {
	end := observed.denseTailEnd()
	if len(observed.denseTail) == 0 || len(vc.sparseRuns) == 0 || uint64(vc.sparseRuns[0].First) >= end {
		return
	}
	out := make([]finiteRun, 0, len(vc.sparseRuns))
	for _, run := range vc.sparseRuns {
		if uint64(run.First) >= end {
			out = append(out, run)
			continue
		}
		limit := uint64(run.Last) + 1
		if limit > end {
			limit = end
		}
		for tid := uint64(run.First); tid < limit; tid++ {
			if run.Clock > observed.Get(uint32(tid)) {
				out = appendFiniteRun(out, finiteRun{First: uint32(tid), Last: uint32(tid), Clock: run.Clock})
			}
		}
		if uint64(run.Last)+1 > end {
			out = appendFiniteRun(out, finiteRun{First: uint32(end), Last: run.Last, Clock: run.Clock})
		}
	}
	vc.sparseRuns = out
}

func sparseRunsHavePrunable(left, observed []finiteRun, retired []RetiredRange) bool {
	runIndex, retiredIndex := 0, 0
	for _, l := range left {
		for runIndex < len(observed) && observed[runIndex].Last < l.First {
			runIndex++
		}
		for retiredIndex < len(retired) && retired[retiredIndex].Last < l.First {
			retiredIndex++
		}
		if retiredIndex < len(retired) && retired[retiredIndex].First <= l.Last {
			return true
		}
		for i := runIndex; i < len(observed) && observed[i].First <= l.Last; i++ {
			if observed[i].Clock >= l.Clock {
				return true
			}
		}
	}
	return false
}

func (vc *VectorClock) HappensBefore(other *VectorClock) bool { return vc.LessOrEqual(other) }

func (vc *VectorClock) Increment(tid uint32) {
	if vc.IsRetired(tid) {
		runtimeThrow("race detector incremented retired logical goroutine ID")
	}
	if vc.base != nil || vc.causal.Valid() {
		clock := vc.Get(tid)
		if clock == ^uint32(0) {
			runtimeThrow("race detector logical clock overflow")
		}
		vc.Set(tid, clock+1)
		return
	}
	if tid < DenseThreads {
		if vc.clocks[tid] == ^uint32(0) {
			runtimeThrow("race detector logical clock overflow")
		}
		vc.clocks[tid]++
		if uint16(tid) > vc.maxDense {
			vc.maxDense = uint16(tid)
		}
		return
	}
	if offset := uint64(tid) - DenseThreads; offset < uint64(len(vc.denseTail)) {
		if vc.denseTail[offset] == ^uint32(0) {
			runtimeThrow("race detector logical clock overflow")
		}
		vc.invalidateDenseProjectionWitness()
		vc.detachDenseTail()
		vc.denseTail[offset]++
		return
	}
	idx := vc.searchSparseRun(tid)
	if idx == len(vc.sparseRuns) || vc.sparseRuns[idx].First > tid {
		vc.Set(tid, 1)
		return
	}
	run := vc.sparseRuns[idx]
	if run.Clock == ^uint32(0) {
		runtimeThrow("race detector logical clock overflow")
	}
	if run.First == tid && run.Last == tid {
		vc.setSparseSingleton(idx, run.Clock+1)
		if len(vc.denseTail) != 0 {
			vc.maybePromoteDenseTail()
		}
		return
	}
	vc.Set(tid, run.Clock+1)
}

// setSparseSingleton updates a high-TID singleton coordinate in place. Only
// its immediate neighbors can become coalescible, so a full scan of
// sparseRuns is unnecessary.
func (vc *VectorClock) setSparseSingleton(index int, clock uint32) {
	runs := vc.sparseRuns
	run := runs[index]
	mergeLeft := index != 0 && runs[index-1].Clock == clock &&
		runs[index-1].Last != ^uint32(0) && runs[index-1].Last+1 == run.First
	mergeRight := index+1 != len(runs) && runs[index+1].Clock == clock &&
		run.Last != ^uint32(0) && run.Last+1 == runs[index+1].First

	switch {
	case mergeLeft && mergeRight:
		runs[index-1].Last = runs[index+1].Last
		copy(runs[index:], runs[index+2:])
		runs[len(runs)-2] = finiteRun{}
		runs[len(runs)-1] = finiteRun{}
		vc.sparseRuns = runs[:len(runs)-2]
	case mergeLeft:
		runs[index-1].Last = run.Last
		copy(runs[index:], runs[index+1:])
		runs[len(runs)-1] = finiteRun{}
		vc.sparseRuns = runs[:len(runs)-1]
	case mergeRight:
		runs[index].Clock = clock
		runs[index].Last = runs[index+1].Last
		copy(runs[index+1:], runs[index+2:])
		runs[len(runs)-1] = finiteRun{}
		vc.sparseRuns = runs[:len(runs)-1]
	default:
		runs[index].Clock = clock
	}
}

// Get is direct-indexed for common dense IDs and binary-searches sparse runs.
func (vc *VectorClock) Get(tid uint32) uint32 {
	if vc.IsRetired(tid) {
		return ^uint32(0)
	}
	baseClock := uint32(0)
	if vc.base != nil {
		baseClock = snapshotGet(vc.base.finite, tid)
	}
	if causalClock := vc.causal.Get(tid); causalClock > baseClock {
		baseClock = causalClock
	}
	if tid < DenseThreads {
		if vc.clocks[tid] > baseClock {
			return vc.clocks[tid]
		}
		return baseClock
	}
	if offset := uint64(tid) - DenseThreads; offset < uint64(len(vc.denseTail)) {
		if vc.denseTail[offset] > baseClock {
			return vc.denseTail[offset]
		}
		return baseClock
	}
	idx := vc.searchSparseRun(tid)
	if idx < len(vc.sparseRuns) && vc.sparseRuns[idx].First <= tid {
		if vc.sparseRuns[idx].Clock > baseClock {
			return vc.sparseRuns[idx].Clock
		}
	}
	return baseClock
}

func (vc *VectorClock) Set(tid, clock uint32) {
	if vc.ownerLineage != nil && vc.ownerLineage.family.isolated.Load() {
		vc.materializeRoots()
	}
	if vc.ownerLineage != nil && tid == vc.ownerLineage.ownerTID {
		old := vc.Get(tid)
		if clock > old && old != ^uint32(0) {
			if root := vc.ownerRoot(); root != nil {
				if _, appended := vc.ownerLineage.AppendOwned(root, tid, clock); appended {
					return
				}
			}
		}
		if clock != old {
			vc.materializeRoots()
		}
	}
	if vc.base != nil || vc.causal.Valid() {
		old := vc.Get(tid)
		if vc.IsRetired(tid) || old == clock {
			return
		}
		if clock < old {
			vc.materializeRoots()
		}
	}
	// Retirement is immutable. In particular, Set(tid, 0) clears only finite
	// state and cannot resurrect a never-reused logical identity.
	if vc.IsRetired(tid) {
		return
	}
	if tid < DenseThreads {
		vc.clocks[tid] = clock
		if clock != 0 && uint16(tid) > vc.maxDense {
			vc.maxDense = uint16(tid)
		} else if clock == 0 && uint16(tid) == vc.maxDense {
			for vc.maxDense != 0 && vc.clocks[vc.maxDense] == 0 {
				vc.maxDense--
			}
		}
		return
	}
	offset := uint64(tid) - DenseThreads
	if offset < uint64(len(vc.denseTail)) {
		vc.invalidateDenseProjectionWitness()
		vc.detachDenseTail()
		vc.denseTail[offset] = clock
		return
	}
	if offset == uint64(len(vc.denseTail)) && len(vc.denseTail) != 0 && clock != 0 &&
		(len(vc.sparseRuns) == 0 || vc.sparseRuns[0].First > tid) {
		vc.invalidateDenseProjectionWitness()
		vc.detachDenseTail()
		vc.denseTail = append(vc.denseTail, clock)
		return
	}
	idx := vc.searchSparseRun(tid)
	if idx == len(vc.sparseRuns) || vc.sparseRuns[idx].First > tid {
		if clock != 0 {
			vc.replaceSparseRun(idx, 0, []finiteRun{{First: tid, Last: tid, Clock: clock}})
			vc.maybePromoteDenseTail()
		}
		return
	}
	old := vc.sparseRuns[idx]
	if old.Clock == clock {
		return
	}
	if clock != 0 && old.First == tid && old.Last == tid {
		vc.setSparseSingleton(idx, clock)
		if len(vc.denseTail) != 0 {
			vc.maybePromoteDenseTail()
		}
		return
	}
	var replacement [3]finiteRun
	n := 0
	if old.First < tid {
		replacement[n] = finiteRun{First: old.First, Last: tid - 1, Clock: old.Clock}
		n++
	}
	if clock != 0 {
		replacement[n] = finiteRun{First: tid, Last: tid, Clock: clock}
		n++
	}
	if tid < old.Last {
		replacement[n] = finiteRun{First: tid + 1, Last: old.Last, Clock: old.Clock}
		n++
	}
	vc.replaceSparseRun(idx, 1, replacement[:n])
	vc.maybePromoteDenseTail()
}

// PrepareKnownMonotonicSet reserves the only mutable storage which
// SetKnownMonotonicAlive can need. The caller must invoke it before the
// all-or-nothing operation whose commit will publish a larger finite value at
// tid, and must not mutate vc between prepare and commit.
//
// The logical value is intentionally not read here. RaceContext owns a
// process-lifetime TID and keeps its cached epoch equal to that coordinate, so
// walking immutable bases and causal roots merely to rediscover the cached
// value is redundant. Two spare runs cover the worst case: replacing one
// interior coordinate of a canonical sparse run with three runs.
func (vc *VectorClock) PrepareKnownMonotonicSet(tid uint32) {
	if vc.ownerLineage != nil && vc.ownerLineage.family.isolated.Load() {
		vc.materializeRoots()
	}
	if vc.ownerLineage != nil && tid == vc.ownerLineage.ownerTID {
		next := vc.Get(tid) + 1
		if vc.ownerLineage.ReserveOwned(tid, next) {
			vc.prepareKnownMonotonicOverlay(tid)
			return
		}
		vc.materializeRoots()
	}
	vc.prepareKnownMonotonicOverlay(tid)
}

func (vc *VectorClock) prepareKnownMonotonicOverlay(tid uint32) {
	if tid < DenseThreads {
		return
	}
	if uint64(tid)-DenseThreads < uint64(len(vc.denseTail)) {
		vc.detachDenseTail()
		return
	}
	required := len(vc.sparseRuns) + 2
	if cap(vc.sparseRuns) >= required {
		return
	}
	capacity := required * 2
	if capacity < 4 {
		capacity = 4
	}
	next := make([]finiteRun, len(vc.sparseRuns), capacity)
	copy(next, vc.sparseRuns)
	vc.sparseRuns = next
}

// CanSetKnownMonotonicAlive reports whether the trusted commit seam can update
// tid without allocating. Non-blocking synchronization paths use it before
// making any semantic mutation; a false result conservatively selects the
// canonical path, whose preflight reserves the required sparse-run capacity.
func (vc *VectorClock) CanSetKnownMonotonicAlive(tid uint32) bool {
	if vc == nil {
		return false
	}
	if vc.ownerLineage != nil && vc.ownerLineage.family.isolated.Load() {
		return false
	}
	if vc.ownerLineage != nil && tid == vc.ownerLineage.ownerTID {
		return vc.ownerRoot() != nil && vc.ownerLineage.CanAppendOwned(tid, vc.Get(tid)+1)
	}
	if tid < DenseThreads {
		return true
	}
	if uint64(tid)-DenseThreads < uint64(len(vc.denseTail)) {
		return !vc.denseTailShared
	}
	idx := vc.searchSparseRun(tid)
	if idx == len(vc.sparseRuns) || vc.sparseRuns[idx].First > tid {
		return len(vc.sparseRuns) < cap(vc.sparseRuns)
	}
	run := vc.sparseRuns[idx]
	if run.First == tid && run.Last == tid {
		return true
	}
	extra := 0
	if run.First < tid {
		extra++
	}
	if tid < run.Last {
		extra++
	}
	return len(vc.sparseRuns)+extra <= cap(vc.sparseRuns)
}

// SetKnownMonotonicAlive publishes a preflighted increase of one live logical
// coordinate without consulting immutable roots or retirement metadata. It is
// the allocation-free commit half of PrepareKnownMonotonicSet.
//
// The caller must prove that clock is strictly greater than vc.Get(tid), that
// tid is not retired, and that vc has not been mutated since prepare. Those
// conditions let this method update only the owned overlay: its larger value
// necessarily dominates every retained root at tid. Unlike Set, this method
// deliberately skips optional dense-tail promotion so commit cannot allocate.
func (vc *VectorClock) SetKnownMonotonicAlive(tid, clock uint32) {
	if vc.ownerLineage != nil && tid == vc.ownerLineage.ownerTID {
		if root := vc.ownerRoot(); root != nil {
			if _, appended := vc.ownerLineage.AppendOwned(root, tid, clock); appended {
				return
			}
		}
		runtimeThrow("race detector failed prepared owner-lineage commit")
	}
	if tid < DenseThreads {
		vc.clocks[tid] = clock
		if uint16(tid) > vc.maxDense {
			vc.maxDense = uint16(tid)
		}
		return
	}
	offset := uint64(tid) - DenseThreads
	if offset < uint64(len(vc.denseTail)) {
		if vc.denseTailShared {
			runtimeThrow("race detector mutated shared dense clock without preflight")
		}
		vc.invalidateDenseProjectionWitnessExcept(tid)
		vc.denseTail[offset] = clock
		return
	}
	idx := vc.searchSparseRun(tid)
	if idx == len(vc.sparseRuns) || vc.sparseRuns[idx].First > tid {
		vc.replaceSparseRun(idx, 0, []finiteRun{{First: tid, Last: tid, Clock: clock}})
		return
	}
	old := vc.sparseRuns[idx]
	if old.First == tid && old.Last == tid {
		vc.setSparseSingleton(idx, clock)
		return
	}
	var replacement [3]finiteRun
	n := 0
	if old.First < tid {
		replacement[n] = finiteRun{First: old.First, Last: tid - 1, Clock: old.Clock}
		n++
	}
	replacement[n] = finiteRun{First: tid, Last: tid, Clock: clock}
	n++
	if tid < old.Last {
		replacement[n] = finiteRun{First: tid + 1, Last: old.Last, Clock: old.Clock}
		n++
	}
	vc.replaceSparseRun(idx, 1, replacement[:n])
}

func (vc *VectorClock) searchSparseRun(tid uint32) int {
	lo, hi := 0, len(vc.sparseRuns)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if vc.sparseRuns[mid].Last < tid {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (vc *VectorClock) replaceSparseRun(index, remove int, replacement []finiteRun) {
	oldLen := len(vc.sparseRuns)
	newLen := oldLen - remove + len(replacement)
	if cap(vc.sparseRuns) < newLen {
		capacity := newLen * 2
		if capacity < 4 {
			capacity = 4
		}
		next := make([]finiteRun, newLen, capacity)
		copy(next, vc.sparseRuns[:index])
		copy(next[index:], replacement)
		copy(next[index+len(replacement):], vc.sparseRuns[index+remove:])
		vc.sparseRuns = next
	} else {
		vc.sparseRuns = vc.sparseRuns[:newLen]
		copy(vc.sparseRuns[index+len(replacement):], vc.sparseRuns[index+remove:oldLen])
		copy(vc.sparseRuns[index:], replacement)
	}
	vc.sparseRuns = coalesceFiniteRuns(vc.sparseRuns)
}

func coalesceFiniteRuns(runs []finiteRun) []finiteRun {
	out := 0
	for i := 0; i < len(runs); i++ {
		r := runs[i]
		if r.Clock == 0 || r.First > r.Last {
			continue
		}
		if out != 0 {
			last := &runs[out-1]
			if last.Clock == r.Clock && last.Last != ^uint32(0) && last.Last+1 == r.First {
				last.Last = r.Last
				continue
			}
		}
		runs[out] = r
		out++
	}
	for i := out; i < len(runs); i++ {
		runs[i] = finiteRun{}
	}
	return runs[:out]
}

func appendFiniteRun(runs []finiteRun, r finiteRun) []finiteRun {
	if r.Clock == 0 || r.First > r.Last {
		return runs
	}
	if n := len(runs); n != 0 {
		last := &runs[n-1]
		if last.Clock == r.Clock && last.Last != ^uint32(0) && last.Last+1 == r.First {
			last.Last = r.Last
			return runs
		}
	}
	return append(runs, r)
}

// IsRetired reports whether tid has immutable +infinity causal metadata.
func (vc *VectorClock) IsRetired(tid uint32) bool {
	if vc.causal.IsRetired(tid) {
		return true
	}
	if vc.base != nil && snapshotGet(vc.base.retired, tid) != 0 {
		return true
	}
	lo, hi := 0, len(vc.retired)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if vc.retired[mid].Last < tid {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(vc.retired) && vc.retired[lo].First <= tid
}

// RetireRange permanently marks every TID in the inclusive range
// [first,last] as +infinity. TID 0 is reserved and cannot be retired.
// Overlapping and adjacent ranges are coalesced.
func (vc *VectorClock) RetireRange(first, last uint32) {
	vc.RetireRanges([]RetiredRange{{First: first, Last: last}})
}

// RetireRanges permanently adds sorted ranges of never-reused TIDs. Input
// ranges may overlap or be adjacent, but their First fields must be in
// nondecreasing order. Finite components covered by the union are discarded.
func (vc *VectorClock) RetireRanges(ranges []RetiredRange) {
	if len(ranges) == 0 {
		return
	}
	vc.materializeRoots()
	for i, r := range ranges {
		if r.First == 0 || r.First > r.Last || (i != 0 && r.First < ranges[i-1].First) {
			runtimeThrow("race detector received invalid retired TID ranges")
		}
	}
	// Finalizer retirement markers are propagated repeatedly through release
	// clocks. If every incoming marker is already present, the canonical
	// VectorClock invariant also guarantees that no covered finite entry
	// remains to discard.
	if retiredSubset(ranges, vc.retired) {
		return
	}

	// When destination capacity is reusable, move the old ranges behind the
	// incoming prefix before merging toward the front. The prefix is exactly
	// enough scratch space to keep every write behind both unread cursors.
	// A reverse merge cannot merely coalesce with its immediate successor: a
	// broad incoming range may subsume several already-emitted old intervals.
	oldLen := len(vc.retired)
	total := oldLen + len(ranges)
	var merged []RetiredRange
	var old []RetiredRange
	if cap(vc.retired) < total {
		merged = make([]RetiredRange, total)
		old = vc.retired
	} else {
		merged = vc.retired[:total]
		copy(merged[len(ranges):], merged[:oldLen])
		old = merged[len(ranges):]
	}
	write := 0
	appendRange := func(r RetiredRange) {
		if write != 0 {
			last := &merged[write-1]
			if r.First <= last.Last || (last.Last != ^uint32(0) && r.First == last.Last+1) {
				if r.Last > last.Last {
					last.Last = r.Last
				}
				return
			}
		}
		merged[write] = r
		write++
	}
	i, j := 0, 0
	for i < len(old) || j < len(ranges) {
		if j == len(ranges) || (i < len(old) && old[i].First <= ranges[j].First) {
			appendRange(old[i])
			i++
		} else {
			appendRange(ranges[j])
			j++
		}
	}
	clear(merged[write:])
	vc.retired = merged[:write]
	vc.dropRetiredFiniteEntries()
}

func (vc *VectorClock) dropRetiredFiniteEntries() {
	vc.invalidateDenseProjectionWitness()
	vc.detachDenseTail()
	for _, r := range vc.retired {
		first, last := r.First, r.Last
		if first < DenseThreads {
			if last >= DenseThreads {
				last = DenseThreads - 1
			}
			for tid := first; tid <= last; tid++ {
				vc.clocks[tid] = 0
			}
		}
	}
	for vc.maxDense != 0 && vc.clocks[vc.maxDense] == 0 {
		vc.maxDense--
	}
	end := vc.denseTailEnd()
	for _, r := range vc.retired {
		first := uint64(r.First)
		if first < DenseThreads {
			first = DenseThreads
		}
		last := uint64(r.Last) + 1
		if last > end {
			last = end
		}
		for tid := first; tid < last; tid++ {
			vc.denseTail[tid-DenseThreads] = 0
		}
	}

	if len(vc.sparseRuns) == 0 {
		return
	}
	if !sparseRunsOverlapRetired(vc.sparseRuns, vc.retired) {
		return
	}
	out := make([]finiteRun, 0, len(vc.sparseRuns)+len(vc.retired))
	retiredIndex := 0
	for _, run := range vc.sparseRuns {
		cursor := uint64(run.First)
		limit := uint64(run.Last)
		for retiredIndex < len(vc.retired) && uint64(vc.retired[retiredIndex].Last) < cursor {
			retiredIndex++
		}
		for retiredIndex < len(vc.retired) && uint64(vc.retired[retiredIndex].First) <= limit {
			r := vc.retired[retiredIndex]
			if cursor < uint64(r.First) {
				out = appendFiniteRun(out, finiteRun{First: uint32(cursor), Last: r.First - 1, Clock: run.Clock})
			}
			if uint64(r.Last)+1 > cursor {
				cursor = uint64(r.Last) + 1
			}
			if cursor > limit {
				break
			}
			retiredIndex++
		}
		if cursor <= limit {
			out = appendFiniteRun(out, finiteRun{First: uint32(cursor), Last: run.Last, Clock: run.Clock})
		}
	}
	vc.sparseRuns = out
}

func sparseRunsOverlapRetired(runs []finiteRun, retired []RetiredRange) bool {
	i, j := 0, 0
	for i < len(runs) && j < len(retired) {
		if runs[i].Last < retired[j].First {
			i++
			continue
		}
		if retired[j].Last < runs[i].First {
			j++
			continue
		}
		return true
	}
	return false
}

func retiredSubset(left, right []RetiredRange) bool {
	j := 0
	for _, l := range left {
		for j < len(right) && right[j].Last < l.First {
			j++
		}
		if j == len(right) || right[j].First > l.First || right[j].Last < l.Last {
			return false
		}
	}
	return true
}

// Range visits every finite non-zero component in ascending TID order.
// Immutable retirement markers are visited only by RangeRetired. Iteration
// stops when visit returns false.
func (vc *VectorClock) Range(visit func(tid, clock uint32) bool) {
	if vc.base != nil || vc.causal.Valid() {
		vc.RangeRuns(func(first, last, clock uint32) bool {
			for tid := uint64(first); tid <= uint64(last); tid++ {
				if !visit(uint32(tid), clock) {
					return false
				}
			}
			return true
		})
		return
	}
	for tid := uint32(0); tid <= uint32(vc.maxDense); tid++ {
		if clock := vc.clocks[tid]; clock != 0 && !visit(tid, clock) {
			return
		}
	}
	for i, clock := range vc.denseTail {
		if clock != 0 && !visit(uint32(DenseThreads+i), clock) {
			return
		}
	}
	for _, run := range vc.sparseRuns {
		for tid := uint64(run.First); tid <= uint64(run.Last); tid++ {
			if !visit(uint32(tid), run.Clock) {
				return
			}
		}
	}
}

// RangeRuns visits finite non-zero components as inclusive sorted runs. A run
// contains adjacent TIDs with the same clock. Retired markers are intentionally
// excluded and are available through RangeRetired.
func (vc *VectorClock) RangeRuns(visit func(first, last, clock uint32) bool) {
	if vc.causal.Valid() {
		copy := vc.CloneDetached()
		copy.materializeCausal()
		copy.RangeRuns(visit)
		copy.Release()
		return
	}
	if vc.base != nil {
		vc.rangeLogicalRuns(visit)
		return
	}
	vc.rangeOwnedRuns(visit)
}

const clockCoordinateEnd = uint64(1) << 32

func (vc *VectorClock) ownedFiniteSegment(pos uint64) (uint32, uint64) {
	if pos >= clockCoordinateEnd {
		return 0, clockCoordinateEnd
	}
	if pos < DenseThreads {
		value := vc.clocks[pos]
		next := pos + 1
		for next < DenseThreads && vc.clocks[next] == value {
			next++
		}
		return value, next
	}
	tailEnd := vc.denseTailEnd()
	if pos < tailEnd {
		value := vc.denseTail[pos-DenseThreads]
		next := pos + 1
		for next < tailEnd && vc.denseTail[next-DenseThreads] == value {
			next++
		}
		return value, next
	}
	idx := vc.searchSparseRun(uint32(pos))
	if idx < len(vc.sparseRuns) && uint64(vc.sparseRuns[idx].First) <= pos {
		return vc.sparseRuns[idx].Clock, uint64(vc.sparseRuns[idx].Last) + 1
	}
	if idx < len(vc.sparseRuns) {
		return 0, uint64(vc.sparseRuns[idx].First)
	}
	return 0, clockCoordinateEnd
}

func retiredSliceSegment(retired []RetiredRange, pos uint64) (bool, uint64) {
	if pos >= clockCoordinateEnd {
		return false, clockCoordinateEnd
	}
	lo, hi := 0, len(retired)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if uint64(retired[mid].Last) < pos {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(retired) {
		return false, clockCoordinateEnd
	}
	r := retired[lo]
	if pos < uint64(r.First) {
		return false, uint64(r.First)
	}
	return true, uint64(r.Last) + 1
}

// logicalSegment returns the exact logical value at pos and the next possible
// transition across the immutable base and mutable overlay.
func (vc *VectorClock) logicalSegment(pos uint64) (bool, uint32, uint64) {
	owned, ownedNext := vc.ownedFiniteSegment(pos)
	base, baseNext := uint32(0), clockCoordinateEnd
	baseRetired, baseRetiredNext := uint32(0), clockCoordinateEnd
	if vc.base != nil {
		base, baseNext = snapshotSegment(vc.base.finite, pos)
		baseRetired, baseRetiredNext = snapshotSegment(vc.base.retired, pos)
	}
	ownedRetired, ownedRetiredNext := retiredSliceSegment(vc.retired, pos)
	causal, causalRetired := uint32(0), false
	causalNext := clockCoordinateEnd
	if vc.causal.Valid() && pos < clockCoordinateEnd {
		causalRetired = vc.causal.IsRetired(uint32(pos))
		causal = vc.causal.Get(uint32(pos))
		// CausalView intentionally exposes point lookup on the pinned path.
		// Cold range/comparison entry points materialize it once; retain this
		// exact one-coordinate fallback so no internal caller can ignore it.
		causalNext = pos + 1
	}
	next := ownedNext
	if baseNext < next {
		next = baseNext
	}
	if baseRetiredNext < next {
		next = baseRetiredNext
	}
	if ownedRetiredNext < next {
		next = ownedRetiredNext
	}
	if causalNext < next {
		next = causalNext
	}
	if baseRetired != 0 || ownedRetired || causalRetired {
		return true, 0, next
	}
	if base > owned {
		owned = base
	}
	if causal > owned {
		owned = causal
	}
	return false, owned, next
}

func (vc *VectorClock) rangeLogicalRuns(visit func(first, last, clock uint32) bool) {
	pos := uint64(0)
	haveRun := false
	var first, last, runClock uint32
	for pos < clockCoordinateEnd {
		retired, clock, next := vc.logicalSegment(pos)
		if next <= pos {
			runtimeThrow("race detector vector-clock logical iterator stalled")
		}
		if !retired && clock != 0 {
			a, b := uint32(pos), uint32(next-1)
			if haveRun && last != ^uint32(0) && last+1 == a && runClock == clock {
				last = b
			} else {
				if haveRun && !visit(first, last, runClock) {
					return
				}
				first, last, runClock, haveRun = a, b, clock, true
			}
		} else if haveRun {
			if !visit(first, last, runClock) {
				return
			}
			haveRun = false
		}
		pos = next
	}
	if haveRun {
		visit(first, last, runClock)
	}
}

func (vc *VectorClock) rangeOwnedRuns(visit func(first, last, clock uint32) bool) {
	var first, last, runClock uint32
	haveRun := false
	emit := func(nextFirst, nextLast, clock uint32) bool {
		if haveRun && last != ^uint32(0) && nextFirst == last+1 && clock == runClock {
			last = nextLast
			return true
		}
		if haveRun && !visit(first, last, runClock) {
			return false
		}
		first, last, runClock, haveRun = nextFirst, nextLast, clock, true
		return true
	}
	for tid := uint32(0); tid <= uint32(vc.maxDense); tid++ {
		if clock := vc.clocks[tid]; clock != 0 && !emit(tid, tid, clock) {
			return
		}
	}
	for i, clock := range vc.denseTail {
		if clock != 0 && !emit(uint32(DenseThreads+i), uint32(DenseThreads+i), clock) {
			return
		}
	}
	for _, run := range vc.sparseRuns {
		if !emit(run.First, run.Last, run.Clock) {
			return
		}
	}
	if haveRun {
		visit(first, last, runClock)
	}
}

// rangeProjectionRuns visits the owned finite overlay except denseTail. A
// ReleaseProjection retains that backing directly under the dense-tail COW
// contract, so enumerating it here would recreate a wide frontier as runs.
func (vc *VectorClock) rangeProjectionRuns(visit func(first, last, clock uint32) bool) {
	var first, last, runClock uint32
	haveRun := false
	emit := func(nextFirst, nextLast, clock uint32) bool {
		if haveRun && last != ^uint32(0) && nextFirst == last+1 && clock == runClock {
			last = nextLast
			return true
		}
		if haveRun && !visit(first, last, runClock) {
			return false
		}
		first, last, runClock, haveRun = nextFirst, nextLast, clock, true
		return true
	}
	for tid := uint32(0); tid <= uint32(vc.maxDense); tid++ {
		if clock := vc.clocks[tid]; clock != 0 && !emit(tid, tid, clock) {
			return
		}
	}
	for i := 0; i < len(vc.sparseRuns); i++ {
		run := vc.sparseRuns[i]
		if !emit(run.First, run.Last, run.Clock) {
			return
		}
	}
	if haveRun {
		visit(first, last, runClock)
	}
}

// JoinRanges point-wise max-joins sorted, non-overlapping finite ranges in one
// structural pass. Input is validated before vc is mutated. Existing
// retirement markers remain +infinity and cannot regain finite coordinates.
func (vc *VectorClock) JoinRanges(ranges []FiniteRange) {
	for i, r := range ranges {
		if r.First > r.Last || r.Clock == 0 || (i != 0 && r.First <= ranges[i-1].Last) {
			runtimeThrow("race detector received invalid finite vector-clock ranges")
		}
	}
	vc.joinCanonicalRanges(ranges)
}

// JoinCanonicalRanges is the trusted counterpart to JoinRanges for an
// immutable snapshot produced by RangeRuns. Such snapshots are already sorted,
// non-overlapping, and non-zero, so revalidating every run on each acquire is
// unnecessary. Callers must not pass ranges from any other source or mutate the
// snapshot while the join is in progress.
func (vc *VectorClock) JoinCanonicalRanges(ranges []FiniteRange) {
	vc.joinCanonicalRanges(ranges)
}

func (vc *VectorClock) joinCanonicalRanges(ranges []FiniteRange) {
	if vc.base != nil {
		vc.JoinSnapshot(newSnapshot(snapshotBuildFinite(ranges), nil))
		return
	}
	var sparse []FiniteRange
	if len(vc.retired) == 0 {
		sparse = vc.joinUnretiredDenseRanges(ranges)
	} else {
		sparse = vc.joinRetiredDenseRanges(ranges)
	}
	sparse = vc.joinDenseTailRanges(sparse)
	vc.joinSparseRuns(sparse)
}

func (vc *VectorClock) joinDenseTailRanges(ranges []FiniteRange) []FiniteRange {
	if len(vc.denseTail) == 0 {
		return ranges
	}
	vc.invalidateDenseProjectionWitness()
	vc.detachDenseTail()
	end := vc.denseTailEnd()
	for i, r := range ranges {
		if uint64(r.First) >= end {
			return ranges[i:]
		}
		first := r.First
		if first < DenseThreads {
			first = DenseThreads
		}
		last := uint64(r.Last) + 1
		if last > end {
			last = end
		}
		for tid := uint64(first); tid < last; tid++ {
			offset := tid - DenseThreads
			if r.Clock > vc.denseTail[offset] && (len(vc.retired) == 0 || !vc.IsRetired(uint32(tid))) {
				vc.denseTail[offset] = r.Clock
			}
		}
		if uint64(r.Last)+1 > end {
			return ranges[i:]
		}
	}
	return nil
}

func (vc *VectorClock) joinUnretiredDenseRanges(ranges []FiniteRange) []FiniteRange {
	for i, r := range ranges {
		if r.First >= DenseThreads {
			return ranges[i:]
		}
		last := r.Last
		if last >= DenseThreads {
			last = DenseThreads - 1
		}
		for tid := r.First; tid <= last; tid++ {
			if r.Clock > vc.clocks[tid] {
				vc.clocks[tid] = r.Clock
			}
		}
		if uint16(last) > vc.maxDense {
			vc.maxDense = uint16(last)
		}
		if r.Last >= DenseThreads {
			return ranges[i:]
		}
	}
	return nil
}

func (vc *VectorClock) joinRetiredDenseRanges(ranges []FiniteRange) []FiniteRange {
	for i, r := range ranges {
		if r.First >= DenseThreads {
			return ranges[i:]
		}
		last := r.Last
		if last >= DenseThreads {
			last = DenseThreads - 1
		}
		for tid := r.First; tid <= last; tid++ {
			if !vc.IsRetired(tid) && r.Clock > vc.clocks[tid] {
				vc.clocks[tid] = r.Clock
				if uint16(tid) > vc.maxDense {
					vc.maxDense = uint16(tid)
				}
			}
		}
		if r.Last >= DenseThreads {
			return ranges[i:]
		}
	}
	return nil
}

// JoinRange point-wise max-joins one inclusive finite run. Retired coordinates
// remain +infinity. uint64 iteration makes a Last==MaxUint32 endpoint safe.
func (vc *VectorClock) JoinRange(first, last, clock uint32) {
	if first > last || clock == 0 {
		return
	}
	rangeBuffer := [1]FiniteRange{{First: first, Last: last, Clock: clock}}
	vc.JoinRanges(rangeBuffer[:])
}

// RangeRetired visits immutable +infinity ranges in ascending order.
func (vc *VectorClock) RangeRetired(visit func(first, last uint32) bool) {
	if vc.causal.Valid() {
		copy := vc.CloneDetached()
		copy.materializeCausal()
		copy.RangeRetired(visit)
		copy.Release()
		return
	}
	if vc.base != nil {
		pos := uint64(0)
		for pos < clockCoordinateEnd {
			base, baseNext := snapshotSegment(vc.base.retired, pos)
			owned, ownedNext := retiredSliceSegment(vc.retired, pos)
			next := baseNext
			if ownedNext < next {
				next = ownedNext
			}
			if base != 0 || owned {
				first := pos
				for next < clockCoordinateEnd {
					b, bn := snapshotSegment(vc.base.retired, next)
					o, on := retiredSliceSegment(vc.retired, next)
					if b == 0 && !o {
						break
					}
					next = bn
					if on < next {
						next = on
					}
				}
				if !visit(uint32(first), uint32(next-1)) {
					return
				}
			}
			if next <= pos {
				runtimeThrow("race detector vector-clock retirement iterator stalled")
			}
			pos = next
		}
		return
	}
	for _, r := range vc.retired {
		if !visit(r.First, r.Last) {
			return
		}
	}
}

func (vc *VectorClock) GetMaxTID() uint32 {
	if vc.causal.Valid() {
		copy := vc.CloneDetached()
		copy.materializeCausal()
		max := copy.GetMaxTID()
		copy.Release()
		return max
	}
	if vc.base != nil {
		max := vc.ownedMaxTID()
		for _, root := range [2]*snapshotNode{vc.base.finite, vc.base.retired} {
			for root != nil && root.right != nil {
				root = root.right
			}
			if root != nil && root.last > max {
				max = root.last
			}
		}
		return max
	}
	return vc.ownedMaxTID()
}

func (vc *VectorClock) ownedMaxTID() uint32 {
	max := uint32(vc.maxDense)
	if len(vc.denseTail) != 0 {
		for i := len(vc.denseTail) - 1; i >= 0; i-- {
			if vc.denseTail[i] != 0 {
				max = uint32(DenseThreads + i)
				break
			}
		}
	}
	if n := len(vc.sparseRuns); n != 0 && vc.sparseRuns[n-1].Last > max {
		max = vc.sparseRuns[n-1].Last
	}
	if n := len(vc.retired); n != 0 && vc.retired[n-1].Last > max {
		max = vc.retired[n-1].Last
	}
	return max
}

func (vc *VectorClock) CopyFrom(other *VectorClock) {
	if vc == other {
		return
	}
	vc.Reset()
	vc.copyFromZero(other)
}

// CopyFromDetached replaces vc with other while retaining private ownership of
// the dense tail. Unlike CopyFrom it never makes other copy-on-write.
func (vc *VectorClock) CopyFromDetached(other *VectorClock) {
	if vc == other {
		return
	}
	vc.Reset()
	vc.copyFromZeroWithDenseOwnership(other, true)
}

// CanCopyFrom reports whether TryCopyFrom can replace vc with other without
// growing any owned buffer. It does not inspect or mutate semantic contents;
// immutable snapshot roots never require owned capacity.
func (vc *VectorClock) CanCopyFrom(other *VectorClock) bool {
	return vc != nil && other != nil &&
		cap(vc.sparseRuns) >= len(other.sparseRuns) &&
		cap(vc.retired) >= len(other.retired)
}

// CanCopyFromDetached reports whether TryCopyFromDetached can replace vc
// without growing any privately owned buffer.
func (vc *VectorClock) CanCopyFromDetached(other *VectorClock) bool {
	return vc != nil && other != nil && !vc.denseTailShared &&
		cap(vc.denseTail) >= len(other.denseTail) &&
		cap(vc.sparseRuns) >= len(other.sparseRuns) &&
		cap(vc.retired) >= len(other.retired)
}

// TryCopyFrom is CopyFrom with an allocation-free, all-or-nothing contract.
// Immutable roots and the copy-on-write dense tail are shared; sparse and
// retirement buffers remain privately owned.
func (vc *VectorClock) TryCopyFrom(other *VectorClock) bool {
	if vc == other {
		return true
	}
	if !vc.CanCopyFrom(other) {
		return false
	}
	vc.Reset()
	vc.copyFromZero(other)
	return true
}

// TryCopyFromDetached is CopyFromDetached with an allocation-free,
// all-or-nothing contract. It is suitable for inactive immutable publication
// slots: the destination remains private and the live source remains mutable.
func (vc *VectorClock) TryCopyFromDetached(other *VectorClock) bool {
	if vc == other {
		return true
	}
	if !vc.CanCopyFromDetached(other) {
		return false
	}
	vc.Reset()
	vc.copyFromZeroWithDenseOwnership(other, true)
	return true
}

func (vc *VectorClock) String() string {
	result := "{"
	first := true
	vc.Range(func(tid, clock uint32) bool {
		if !first {
			result += ", "
		}
		result += itoa(tid) + ":" + itoa(clock)
		first = false
		return true
	})
	return result + "}"
}

func itoa(n uint32) string {
	if n == 0 {
		return "0"
	}
	tmp, digits := n, 0
	for tmp > 0 {
		digits++
		tmp /= 10
	}
	buf := make([]byte, digits)
	for i := digits - 1; i >= 0; i-- {
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf)
}
