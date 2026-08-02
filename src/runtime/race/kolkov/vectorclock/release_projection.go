package vectorclock

import "unsafe"

// A ReleaseProjection is an exact, immutable, independently owned view of a
// VectorClock suitable for deferred ReleaseMerge publication. The fixed
// overlay keeps the common capture allocation-free; PinReleaseProjection owns
// dynamic range copies when the mutable overlay is larger.
const (
	ReleaseProjectionFiniteCapacity  = 8
	ReleaseProjectionRetiredCapacity = 4
)

type ReleaseProjection struct {
	base      *ClockSnapshot
	denseTail []uint32
	// denseOwned distinguishes the detached backing used by prepared-owner
	// capture from the ordinary borrowed/shared VectorClock backing. Only an
	// owned backing may be overwritten by RepinReleaseProjectionForPreparedOwner.
	denseOwned     bool
	roots          [CausalRootCapacity]CausalView
	finite         [ReleaseProjectionFiniteCapacity]FiniteRange
	retired        [ReleaseProjectionRetiredCapacity]RetiredRange
	dynamicFinite  []FiniteRange
	dynamicRetired []RetiredRange
	rootN          uint8
	finiteN        uint8
	retiredN       uint8
}

// TryPinReleaseProjection captures vc without allocation. It either retains
// every causal root needed by the projection or leaves out empty.
func TryPinReleaseProjection(vc *VectorClock, out *ReleaseProjection) bool {
	if vc == nil || out == nil {
		return false
	}
	out.Release()
	finiteN := 0
	vc.rangeProjectionRuns(func(_, _, _ uint32) bool {
		finiteN++
		return finiteN <= ReleaseProjectionFiniteCapacity
	})
	if finiteN > ReleaseProjectionFiniteCapacity || len(vc.retired) > ReleaseProjectionRetiredCapacity {
		return false
	}
	out.base = vc.base
	out.denseTail = vc.denseTail
	out.finiteN = uint8(finiteN)
	i := 0
	vc.rangeProjectionRuns(func(first, last, clock uint32) bool {
		out.finite[i] = FiniteRange{First: first, Last: last, Clock: clock}
		i++
		return true
	})
	out.retiredN = uint8(len(vc.retired))
	copy(out.retired[:], vc.retired)
	for i := 0; i < int(vc.causal.count); i++ {
		root, ok := vc.causal.roots[i].Duplicate()
		if !ok {
			out.Release()
			return false
		}
		out.roots[i] = root
		out.rootN++
	}
	if len(vc.denseTail) != 0 {
		vc.denseTailShared = true
	}
	return true
}

// TryPinReleaseProjectionForPreparedOwner is the allocation-free capture used
// before a prepared owner-clock commit. A dense owner would become shared by
// ordinary capture and require an allocating detach at commit, so that shape
// is an exact fast-path miss. Inline and sparse owner storage is not aliased by
// a projection and remains safe to commit.
func TryPinReleaseProjectionForPreparedOwner(vc *VectorClock, ownerTID uint32, out *ReleaseProjection) bool {
	if vc == nil {
		return false
	}
	if ownerTID >= DenseThreads && uint64(ownerTID)-DenseThreads < uint64(len(vc.denseTail)) {
		return false
	}
	return TryPinReleaseProjection(vc, out)
}

// PinReleaseProjection is the unrestricted capture path. Large mutable
// overlays are copied into projection-owned slices, preserving immutable bases
// and causal roots without materializing their ancestry.
func PinReleaseProjection(vc *VectorClock) ReleaseProjection {
	var projection ReleaseProjection
	if TryPinReleaseProjection(vc, &projection) {
		return projection
	}
	if vc == nil {
		return projection
	}
	finiteN := 0
	vc.rangeProjectionRuns(func(_, _, _ uint32) bool {
		finiteN++
		return true
	})
	projection.base = vc.base
	projection.denseTail = vc.denseTail
	if finiteN <= ReleaseProjectionFiniteCapacity {
		projection.finiteN = uint8(finiteN)
		i := 0
		vc.rangeProjectionRuns(func(first, last, clock uint32) bool {
			projection.finite[i] = FiniteRange{First: first, Last: last, Clock: clock}
			i++
			return true
		})
	} else {
		projection.dynamicFinite = make([]FiniteRange, 0, finiteN)
		vc.rangeProjectionRuns(func(first, last, clock uint32) bool {
			projection.dynamicFinite = append(projection.dynamicFinite, FiniteRange{First: first, Last: last, Clock: clock})
			return true
		})
	}
	if len(vc.retired) <= ReleaseProjectionRetiredCapacity {
		projection.retiredN = uint8(len(vc.retired))
		copy(projection.retired[:], vc.retired)
	} else {
		projection.dynamicRetired = append([]RetiredRange(nil), vc.retired...)
	}
	for i := 0; i < int(vc.causal.count); i++ {
		root, ok := vc.causal.roots[i].Duplicate()
		if !ok {
			projection.Release()
			runtimeThrow("race detector pinned a released causal clock root")
		}
		projection.roots[i] = root
		projection.rootN++
	}
	if len(vc.denseTail) != 0 {
		vc.denseTailShared = true
	}
	return projection
}

// PinReleaseProjectionForPreparedOwner captures vc while preserving an
// allocation-free monotonic write that was prepared for ownerTID before the
// capture. A normal projection shares the immutable dense-tail backing and
// therefore makes a later dense write detach. Atomic completion cannot detach:
// its hardware operation has already executed. When the prepared coordinate is
// in that backing, give the projection its own exact copy instead and restore
// the source's prior sharing state. Sparse and inline owner coordinates need no
// special handling because projection capture never aliases their mutable
// storage.
func PinReleaseProjectionForPreparedOwner(vc *VectorClock, ownerTID uint32) ReleaseProjection {
	if vc == nil {
		return ReleaseProjection{}
	}
	wasShared := vc.denseTailShared
	projection := PinReleaseProjection(vc)
	if ownerTID >= DenseThreads {
		offset := uint64(ownerTID) - DenseThreads
		if offset < uint64(len(projection.denseTail)) {
			dense := make([]uint32, len(projection.denseTail))
			copy(dense, projection.denseTail)
			projection.denseTail = dense
			projection.denseOwned = true
			vc.denseTailShared = wasShared
		}
	}
	return projection
}

// RepinReleaseProjectionForPreparedOwner replaces out with an exact capture of
// vc while reusing projection-owned buffers when possible. Atomic release
// publication calls this while its state lock excludes readers, so the old
// projection can be released before the replacement is installed. Borrowed
// dense backings are never reused: they may still be the mutable storage of the
// clock from which they were captured.
func RepinReleaseProjectionForPreparedOwner(vc *VectorClock, ownerTID uint32, out *ReleaseProjection) {
	if out == nil {
		return
	}
	if vc == nil {
		out.Release()
		return
	}
	// Retain the complete incoming root set before dropping the old projection.
	// This keeps repin all-or-nothing across the only explicit fallible lifecycle
	// operation and permits the old and new images to share lineage families.
	var retainedRoots [CausalRootCapacity]CausalView
	retainedN := 0
	for i := 0; i < int(vc.causal.count); i++ {
		root, ok := vc.causal.roots[i].Duplicate()
		if !ok {
			for j := 0; j < retainedN; j++ {
				retainedRoots[j].Release()
			}
			runtimeThrow("race detector repinned a released causal clock root")
		}
		retainedRoots[retainedN] = root
		retainedN++
	}

	var reusableDense []uint32
	if out.denseOwned {
		reusableDense = out.denseTail
	}
	reusableFinite := out.dynamicFinite
	reusableRetired := out.dynamicRetired
	for i := 0; i < int(out.rootN); i++ {
		out.roots[i].Release()
	}
	*out = ReleaseProjection{}
	copy(out.roots[:retainedN], retainedRoots[:retainedN])
	out.rootN = uint8(retainedN)

	finiteN := 0
	vc.rangeProjectionRuns(func(_, _, _ uint32) bool {
		finiteN++
		return true
	})
	out.base = vc.base
	if finiteN <= ReleaseProjectionFiniteCapacity {
		out.finiteN = uint8(finiteN)
		i := 0
		vc.rangeProjectionRuns(func(first, last, clock uint32) bool {
			out.finite[i] = FiniteRange{First: first, Last: last, Clock: clock}
			i++
			return true
		})
	} else {
		if cap(reusableFinite) < finiteN {
			reusableFinite = make([]FiniteRange, finiteN)
		} else {
			reusableFinite = reusableFinite[:finiteN]
		}
		i := 0
		vc.rangeProjectionRuns(func(first, last, clock uint32) bool {
			reusableFinite[i] = FiniteRange{First: first, Last: last, Clock: clock}
			i++
			return true
		})
		out.dynamicFinite = reusableFinite
	}
	if len(vc.retired) <= ReleaseProjectionRetiredCapacity {
		out.retiredN = uint8(len(vc.retired))
		copy(out.retired[:], vc.retired)
	} else {
		if cap(reusableRetired) < len(vc.retired) {
			reusableRetired = make([]RetiredRange, len(vc.retired))
		} else {
			reusableRetired = reusableRetired[:len(vc.retired)]
		}
		copy(reusableRetired, vc.retired)
		out.dynamicRetired = reusableRetired
	}
	if len(vc.denseTail) == 0 {
		return
	}
	ownerInDense := false
	if ownerTID >= DenseThreads {
		ownerInDense = uint64(ownerTID)-DenseThreads < uint64(len(vc.denseTail))
	}
	if ownerInDense {
		if cap(reusableDense) < len(vc.denseTail) {
			reusableDense = make([]uint32, len(vc.denseTail))
		} else {
			reusableDense = reusableDense[:len(vc.denseTail)]
		}
		copy(reusableDense, vc.denseTail)
		out.denseTail = reusableDense
		out.denseOwned = true
		return
	}
	out.denseTail = vc.denseTail
	vc.denseTailShared = true
}

func (p *ReleaseProjection) appendDenseRanges(finite *[]FiniteRange) {
	if p == nil || finite == nil {
		return
	}
	for i := 0; i < len(p.denseTail); {
		clock := p.denseTail[i]
		if clock == 0 {
			i++
			continue
		}
		j := i + 1
		for j < len(p.denseTail) && p.denseTail[j] == clock {
			j++
		}
		*finite = append(*finite, FiniteRange{
			First: uint32(DenseThreads + i), Last: uint32(DenseThreads + j - 1), Clock: clock,
		})
		i = j
	}
}

// AppendRanges appends every logical component to finite and retired. The
// resulting ranges are exact but are not necessarily sorted or disjoint;
// CanonicalizeReleaseRanges prepares them for one bulk join.
func (p *ReleaseProjection) AppendRanges(finite *[]FiniteRange, retired *[]RetiredRange) {
	if p == nil {
		return
	}
	if p.base != nil {
		snapshotRange(p.base.finite, func(first, last, clock uint32) bool {
			*finite = append(*finite, FiniteRange{First: first, Last: last, Clock: clock})
			return true
		})
		snapshotRange(p.base.retired, func(first, last, _ uint32) bool {
			*retired = append(*retired, RetiredRange{First: first, Last: last})
			return true
		})
	}
	for i := 0; i < int(p.rootN); i++ {
		root := p.roots[i].materialize()
		root.rangeOwnedRuns(func(first, last, clock uint32) bool {
			*finite = append(*finite, FiniteRange{First: first, Last: last, Clock: clock})
			return true
		})
		*retired = append(*retired, root.retired...)
		root.Release()
	}
	p.appendDenseRanges(finite)
	*finite = append(*finite, p.finite[:p.finiteN]...)
	*finite = append(*finite, p.dynamicFinite...)
	*retired = append(*retired, p.retired[:p.retiredN]...)
	*retired = append(*retired, p.dynamicRetired...)
}

// Drain transfers every independently retained root to roots and appends the
// immutable base and owned overlays. The projection is empty afterward.
func (p *ReleaseProjection) Drain(bases *[]*ClockSnapshot, roots *[]CausalView, finite *[]FiniteRange, retired *[]RetiredRange) {
	if p == nil {
		return
	}
	if p.base != nil {
		*bases = append(*bases, p.base)
	}
	for i := 0; i < int(p.rootN); i++ {
		*roots = append(*roots, p.roots[i])
		p.roots[i] = CausalView{}
	}
	p.appendDenseRanges(finite)
	*finite = append(*finite, p.finite[:p.finiteN]...)
	*finite = append(*finite, p.dynamicFinite...)
	*retired = append(*retired, p.retired[:p.retiredN]...)
	*retired = append(*retired, p.dynamicRetired...)
	*p = ReleaseProjection{}
}

// JoinInto pointwise joins the exact projection without materializing its
// retained causal roots. A shared dense backing is recognized by identity;
// unrelated dense layouts take the ordinary exact merge.
func (p *ReleaseProjection) JoinInto(dst *VectorClock) {
	if p == nil || dst == nil {
		return
	}
	dst.JoinSnapshot(p.base)
	if len(p.denseTail) != 0 {
		shared := len(dst.denseTail) == len(p.denseTail) && len(dst.denseTail) != 0 &&
			&dst.denseTail[0] == &p.denseTail[0]
		if !shared {
			dst.extendDenseTail(len(p.denseTail))
			for i, clock := range p.denseTail {
				if clock > dst.denseTail[i] && !dst.IsRetired(uint32(DenseThreads+i)) {
					dst.denseTail[i] = clock
				}
			}
		}
	}
	dst.JoinCanonicalRanges(p.finite[:p.finiteN])
	dst.JoinCanonicalRanges(p.dynamicFinite)
	if p.retiredN != 0 || len(p.dynamicRetired) != 0 {
		// Retirement is a rare lifecycle path. Applying it before importing this
		// projection's roots avoids immediately lowering those immutable roots;
		// pre-existing destination roots still use the exact canonical fallback.
		dst.RetireRanges(p.retired[:p.retiredN])
		dst.RetireRanges(p.dynamicRetired)
	}
	for i := 0; i < int(p.rootN); i++ {
		dst.JoinCausal(p.roots[i])
	}
}

// DominatesCausal reports whether a retained projection root is a same-family
// version at least as new as view.
func (p *ReleaseProjection) DominatesCausal(view CausalView) bool {
	if p == nil || !view.Valid() {
		return false
	}
	for i := 0; i < int(p.rootN); i++ {
		if p.roots[i].Dominates(view) {
			return true
		}
	}
	return false
}

func (p *ReleaseProjection) isRetired(tid uint32) bool {
	if p == nil {
		return false
	}
	if p.base != nil && snapshotGet(p.base.retired, tid) != 0 {
		return true
	}
	for i := 0; i < int(p.rootN); i++ {
		if p.roots[i].IsRetired(tid) {
			return true
		}
	}
	for _, r := range p.retired[:p.retiredN] {
		if tid >= r.First && tid <= r.Last {
			return true
		}
	}
	for _, r := range p.dynamicRetired {
		if tid >= r.First && tid <= r.Last {
			return true
		}
	}
	return false
}

func (p *ReleaseProjection) get(tid uint32) uint32 {
	if p == nil {
		return 0
	}
	if p.isRetired(tid) {
		return ^uint32(0)
	}
	clock := uint32(0)
	if p.base != nil {
		clock = snapshotGet(p.base.finite, tid)
	}
	for i := 0; i < int(p.rootN); i++ {
		if candidate := p.roots[i].Get(tid); candidate > clock {
			clock = candidate
		}
	}
	if offset := uint64(tid) - DenseThreads; tid >= DenseThreads && offset < uint64(len(p.denseTail)) {
		if candidate := p.denseTail[offset]; candidate > clock {
			clock = candidate
		}
	}
	for _, r := range p.finite[:p.finiteN] {
		if tid >= r.First && tid <= r.Last && r.Clock > clock {
			clock = r.Clock
		}
	}
	for _, r := range p.dynamicFinite {
		if tid >= r.First && tid <= r.Last && r.Clock > clock {
			clock = r.Clock
		}
	}
	return clock
}

func projectionSetDominatesView(p *ReleaseProjection, views *[CausalRootCapacity]CausalView, n int, left CausalView) bool {
	for i := 0; i < n; i++ {
		if views[i].Dominates(left) {
			return true
		}
	}
	if p != nil {
		for i := 0; i < int(p.rootN); i++ {
			if p.roots[i].Dominates(left) {
				return true
			}
		}
	}
	return false
}

func projectionSetIsRetired(p *ReleaseProjection, views *[CausalRootCapacity]CausalView, n int, tid uint32) bool {
	if p != nil && p.isRetired(tid) {
		return true
	}
	return causalSetIsRetired(views, n, tid)
}

func projectionSetGet(p *ReleaseProjection, views *[CausalRootCapacity]CausalView, n int, tid uint32) uint32 {
	clock := uint32(0)
	if p != nil {
		clock = p.get(tid)
	}
	if candidate := causalSetGet(views, n, tid); candidate > clock {
		clock = candidate
	}
	return clock
}

func (p *ReleaseProjection) ownedFiniteRanges() []FiniteRange {
	if p == nil {
		return nil
	}
	if len(p.dynamicFinite) != 0 {
		return p.dynamicFinite
	}
	return p.finite[:p.finiteN]
}

// projectionOwnedFiniteDominates proves the common wide-sparse case in one
// merge scan. Both sides are canonical, so comparing intervals is exact and
// proportional to run count rather than coordinate width. Immutable bases,
// causal roots, and the COW dense tail are checked separately by the caller.
// A false result merely selects the bounded pointwise union proof below.
func projectionOwnedFiniteDominates(vc *VectorClock, p *ReleaseProjection, skip uint32) bool {
	right := p.ownedFiniteRanges()
	rightAt := 0
	dominates := func(first, last, clock uint32) bool {
		for pos, limit := uint64(first), uint64(last)+1; pos < limit; {
			for rightAt < len(right) && uint64(right[rightAt].Last) < pos {
				rightAt++
			}
			if rightAt == len(right) || uint64(right[rightAt].First) > pos || right[rightAt].Clock < clock {
				return false
			}
			next := uint64(right[rightAt].Last) + 1
			if next > limit {
				next = limit
			}
			pos = next
		}
		return true
	}
	ok := true
	vc.rangeProjectionRuns(func(first, last, clock uint32) bool {
		if skip < first || skip > last {
			ok = dominates(first, last, clock)
			return ok
		}
		if first < skip && !dominates(first, skip-1, clock) {
			ok = false
			return false
		}
		if skip < last {
			ok = dominates(skip+1, last, clock)
		}
		return ok
	})
	return ok
}

// ResidualLessOrEqualReleaseProjection proves that vc, excluding ownTID, is
// dominated by projection plus views. Structural identity handles wide shared
// components; small residuals are checked pointwise. False is conservative.
func (vc *VectorClock) ResidualLessOrEqualReleaseProjection(views *[CausalRootCapacity]CausalView, n int, p *ReleaseProjection, ownTID uint32) bool {
	if vc == nil {
		return true
	}
	if views == nil || n < 0 || n > len(views) || p == nil {
		return false
	}
	for i := 0; i < int(vc.causal.count); i++ {
		if !projectionSetDominatesView(p, views, n, vc.causal.roots[i]) {
			return false
		}
	}
	if vc.base != nil && (p.base == nil || !snapshotLineageLessOrEqual(vc.base, p.base)) {
		return false
	}
	if len(vc.denseTail) != 0 {
		shared := len(p.denseTail) >= len(vc.denseTail) && len(p.denseTail) != 0 &&
			&vc.denseTail[0] == &p.denseTail[0]
		if !shared {
			for i, clock := range vc.denseTail {
				tid := uint32(DenseThreads + i)
				if clock == 0 || tid == ownTID || projectionSetIsRetired(p, views, n, tid) {
					continue
				}
				if projectionSetGet(p, views, n, tid) < clock {
					return false
				}
			}
		}
	}
	if len(vc.retired) == 0 && projectionOwnedFiniteDominates(vc, p, ownTID) {
		return true
	}
	checked := uint64(0)
	checkFinite := func(first, last, clock uint32) bool {
		width := uint64(last) - uint64(first) + 1
		if ownTID >= first && ownTID <= last {
			width--
		}
		checked += width
		if checked > maxCausalResidualProofRun {
			return false
		}
		for tid := uint64(first); tid <= uint64(last); tid++ {
			id := uint32(tid)
			if id != ownTID && !projectionSetIsRetired(p, views, n, id) && projectionSetGet(p, views, n, id) < clock {
				return false
			}
		}
		return true
	}
	ok := true
	vc.rangeProjectionRuns(func(first, last, clock uint32) bool {
		ok = checkFinite(first, last, clock)
		return ok
	})
	if !ok {
		return false
	}
	for _, r := range vc.retired {
		width := uint64(r.Last) - uint64(r.First) + 1
		if ownTID >= r.First && ownTID <= r.Last {
			width--
		}
		checked += width
		if checked > maxCausalResidualProofRun {
			return false
		}
		for tid := uint64(r.First); tid <= uint64(r.Last); tid++ {
			id := uint32(tid)
			if id != ownTID && !projectionSetIsRetired(p, views, n, id) {
				return false
			}
		}
	}
	return true
}

func sortReleaseRoots(roots []CausalView) {
	less := func(a, b CausalView) bool {
		af, bf := uintptr(unsafe.Pointer(a.segment.family)), uintptr(unsafe.Pointer(b.segment.family))
		return af < bf || (af == bf && a.version < b.version)
	}
	var quick func(int, int)
	quick = func(lo, hi int) {
		for lo < hi {
			i, j := lo, hi
			pivot := roots[lo+(hi-lo)/2]
			for i <= j {
				for less(roots[i], pivot) {
					i++
				}
				for less(pivot, roots[j]) {
					j--
				}
				if i <= j {
					roots[i], roots[j] = roots[j], roots[i]
					i++
					j--
				}
			}
			if j-lo < hi-i {
				if lo < j {
					quick(lo, j)
				}
				lo = i
			} else {
				if i < hi {
					quick(i, hi)
				}
				hi = j
			}
		}
	}
	if len(roots) > 1 {
		quick(0, len(roots)-1)
	}
}

// coalesceReleaseRoots consumes roots and retains one maximum-version owned
// view per lineage family. Sorting by stable family identity avoids maps on
// detector/runtime stacks and bounds unrelated-family work to O(N log N).
func coalesceReleaseRoots(roots []CausalView) []CausalView {
	sortReleaseRoots(roots)
	out := roots[:0]
	for i := 0; i < len(roots); {
		j := i + 1
		for j < len(roots) && roots[j].SameFamily(roots[i]) {
			j++
		}
		winner := roots[j-1]
		for k := i; k < j-1; k++ {
			roots[k].Release()
		}
		out = append(out, winner)
		i = j
	}
	return out
}

func sortReleaseSnapshots(snapshots []*ClockSnapshot) {
	less := func(a, b *ClockSnapshot) bool {
		al, bl := uintptr(unsafe.Pointer(a.lineage)), uintptr(unsafe.Pointer(b.lineage))
		return al < bl || (al == bl && a.version < b.version)
	}
	var quick func(int, int)
	quick = func(lo, hi int) {
		for lo < hi {
			i, j := lo, hi
			pivot := snapshots[lo+(hi-lo)/2]
			for i <= j {
				for less(snapshots[i], pivot) {
					i++
				}
				for less(pivot, snapshots[j]) {
					j--
				}
				if i <= j {
					snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
					i++
					j--
				}
			}
			if j-lo < hi-i {
				if lo < j {
					quick(lo, j)
				}
				lo = i
			} else {
				if i < hi {
					quick(i, hi)
				}
				hi = j
			}
		}
	}
	if len(snapshots) > 1 {
		quick(0, len(snapshots)-1)
	}
}

// AppendCoalescedReleaseComponents enumerates each monotonic snapshot or
// causal family only once, then releases every drained causal root.
func AppendCoalescedReleaseComponents(bases []*ClockSnapshot, roots []CausalView, finite *[]FiniteRange, retired *[]RetiredRange) {
	sortReleaseSnapshots(bases)
	for i := 0; i < len(bases); {
		j := i + 1
		for j < len(bases) && bases[j].lineage == bases[i].lineage {
			j++
		}
		base := bases[j-1]
		snapshotRange(base.finite, func(first, last, clock uint32) bool {
			*finite = append(*finite, FiniteRange{First: first, Last: last, Clock: clock})
			return true
		})
		snapshotRange(base.retired, func(first, last, _ uint32) bool {
			*retired = append(*retired, RetiredRange{First: first, Last: last})
			return true
		})
		i = j
	}
	for _, root := range coalesceReleaseRoots(roots) {
		materialized := root.materialize()
		materialized.rangeOwnedRuns(func(first, last, clock uint32) bool {
			*finite = append(*finite, FiniteRange{First: first, Last: last, Clock: clock})
			return true
		})
		*retired = append(*retired, materialized.retired...)
		materialized.Release()
		root.Release()
	}
}

// Release drops every independently retained causal root exactly once.
func (p *ReleaseProjection) Release() {
	if p == nil {
		return
	}
	for i := 0; i < int(p.rootN); i++ {
		p.roots[i].Release()
	}
	*p = ReleaseProjection{}
}

type releaseFiniteHeap []FiniteRange

func (h *releaseFiniteHeap) push(r FiniteRange) {
	*h = append(*h, r)
	for i := len(*h) - 1; i > 0; {
		parent := (i - 1) / 2
		if (*h)[parent].Clock > r.Clock || ((*h)[parent].Clock == r.Clock && (*h)[parent].Last >= r.Last) {
			break
		}
		(*h)[i] = (*h)[parent]
		i = parent
		(*h)[i] = r
	}
}

func (h *releaseFiniteHeap) pop() {
	last := len(*h) - 1
	root := (*h)[last]
	*h = (*h)[:last]
	if last == 0 {
		return
	}
	i := 0
	for {
		left := i*2 + 1
		if left >= len(*h) {
			break
		}
		child := left
		right := left + 1
		if right < len(*h) && ((*h)[right].Clock > (*h)[left].Clock ||
			((*h)[right].Clock == (*h)[left].Clock && (*h)[right].Last > (*h)[left].Last)) {
			child = right
		}
		if (*h)[child].Clock < root.Clock || ((*h)[child].Clock == root.Clock && (*h)[child].Last <= root.Last) {
			break
		}
		(*h)[i] = (*h)[child]
		i = child
	}
	(*h)[i] = root
}

func sortFiniteByFirst(ranges []FiniteRange) {
	less := func(a, b FiniteRange) bool {
		return a.First < b.First || (a.First == b.First && (a.Last < b.Last || (a.Last == b.Last && a.Clock < b.Clock)))
	}
	var quick func(int, int)
	quick = func(lo, hi int) {
		for lo < hi {
			i, j := lo, hi
			pivot := ranges[lo+(hi-lo)/2]
			for i <= j {
				for less(ranges[i], pivot) {
					i++
				}
				for less(pivot, ranges[j]) {
					j--
				}
				if i <= j {
					ranges[i], ranges[j] = ranges[j], ranges[i]
					i++
					j--
				}
			}
			if j-lo < hi-i {
				if lo < j {
					quick(lo, j)
				}
				lo = i
			} else {
				if i < hi {
					quick(i, hi)
				}
				hi = j
			}
		}
	}
	if len(ranges) > 1 {
		quick(0, len(ranges)-1)
	}
}

func sortRetiredByFirst(ranges []RetiredRange) {
	less := func(a, b RetiredRange) bool {
		return a.First < b.First || (a.First == b.First && a.Last < b.Last)
	}
	var quick func(int, int)
	quick = func(lo, hi int) {
		for lo < hi {
			i, j := lo, hi
			pivot := ranges[lo+(hi-lo)/2]
			for i <= j {
				for less(ranges[i], pivot) {
					i++
				}
				for less(pivot, ranges[j]) {
					j--
				}
				if i <= j {
					ranges[i], ranges[j] = ranges[j], ranges[i]
					i++
					j--
				}
			}
			if j-lo < hi-i {
				if lo < j {
					quick(lo, j)
				}
				lo = i
			} else {
				if i < hi {
					quick(i, hi)
				}
				hi = j
			}
		}
	}
	if len(ranges) > 1 {
		quick(0, len(ranges)-1)
	}
}

// CanonicalizeReleaseRanges computes the pointwise maximum of arbitrary
// release ranges and unions retirement markers. Its sweep joins the aggregate
// once, avoiding repeated rescans of a growing sparse VectorClock.
func CanonicalizeReleaseRanges(finite []FiniteRange, retired []RetiredRange) ([]FiniteRange, []RetiredRange) {
	sortFiniteByFirst(finite)
	var merged []FiniteRange
	var heap releaseFiniteHeap
	for next, pos := 0, uint64(0); next < len(finite) || len(heap) != 0; {
		if len(heap) == 0 && next < len(finite) && pos < uint64(finite[next].First) {
			pos = uint64(finite[next].First)
		}
		for next < len(finite) && uint64(finite[next].First) <= pos {
			heap.push(finite[next])
			next++
		}
		for len(heap) != 0 && uint64(heap[0].Last) < pos {
			heap.pop()
		}
		if len(heap) == 0 {
			continue
		}
		end := uint64(heap[0].Last) + 1
		if next < len(finite) && uint64(finite[next].First) < end {
			end = uint64(finite[next].First)
		}
		if end > pos {
			r := FiniteRange{First: uint32(pos), Last: uint32(end - 1), Clock: heap[0].Clock}
			if n := len(merged); n != 0 && merged[n-1].Last != ^uint32(0) && merged[n-1].Last+1 == r.First && merged[n-1].Clock == r.Clock {
				merged[n-1].Last = r.Last
			} else {
				merged = append(merged, r)
			}
		}
		pos = end
	}

	sortRetiredByFirst(retired)
	retiredOut := retired[:0]
	for _, r := range retired {
		if r.First > r.Last {
			continue
		}
		if n := len(retiredOut); n != 0 && (retiredOut[n-1].Last == ^uint32(0) || r.First <= retiredOut[n-1].Last+1) {
			if r.Last > retiredOut[n-1].Last {
				retiredOut[n-1].Last = r.Last
			}
		} else {
			retiredOut = append(retiredOut, r)
		}
	}
	if len(retiredOut) == 0 || len(merged) == 0 {
		return merged, retiredOut
	}
	finiteOut := make([]FiniteRange, 0, len(merged))
	ridx := 0
	for _, f := range merged {
		pos := uint64(f.First)
		end := uint64(f.Last) + 1
		for ridx < len(retiredOut) && uint64(retiredOut[ridx].Last) < pos {
			ridx++
		}
		for i := ridx; i < len(retiredOut) && uint64(retiredOut[i].First) < end; i++ {
			if pos < uint64(retiredOut[i].First) {
				finiteOut = append(finiteOut, FiniteRange{First: uint32(pos), Last: retiredOut[i].First - 1, Clock: f.Clock})
			}
			if after := uint64(retiredOut[i].Last) + 1; after > pos {
				pos = after
			}
			if pos >= end {
				break
			}
		}
		if pos < end {
			finiteOut = append(finiteOut, FiniteRange{First: uint32(pos), Last: uint32(end - 1), Clock: f.Clock})
		}
	}
	return finiteOut, retiredOut
}
