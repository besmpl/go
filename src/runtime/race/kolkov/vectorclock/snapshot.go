package vectorclock

import "internal/runtime/atomic"

// ClockSnapshot is an immutable, shareable vector-clock checkpoint. Its two
// roots contain canonical finite and retired intervals. Nodes are ordinary Go
// heap objects and are never pooled or mutated after publication.
type ClockSnapshot struct {
	finite  *snapshotNode
	retired *snapshotNode

	// lineage/version is a constant-time dominance certificate for snapshots
	// produced by successive monotonic PointMax operations. A version later in
	// the same lineage contains every coordinate of an earlier version. Joins
	// which combine unrelated roots deliberately start a fresh lineage.
	lineage *snapshotLineage
	version uint64
}

type snapshotLineage struct {
	tip atomic.Pointer[ClockSnapshot]
}

func newSnapshot(finite, retired *snapshotNode) *ClockSnapshot {
	if finite == nil && retired == nil {
		return nil
	}
	snapshot := &ClockSnapshot{
		finite: finite, retired: retired,
		lineage: &snapshotLineage{}, version: 1,
	}
	snapshot.lineage.tip.Store(snapshot)
	return snapshot
}

func snapshotLineageLessOrEqual(left, right *ClockSnapshot) bool {
	return left != nil && right != nil && left.lineage != nil &&
		left.lineage == right.lineage && left.version <= right.version
}

type snapshotNode struct {
	first, last uint32
	value       uint32
	priority    uint64
	minFirst    uint32
	maxLast     uint32
	left, right *snapshotNode
}

func snapshotPriority(key uint32) uint64 {
	x := uint64(key) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func newSnapshotNode(first, last, value uint32, left, right *snapshotNode) *snapshotNode {
	minFirst, maxLast := first, last
	if left != nil {
		minFirst = left.minFirst
	}
	if right != nil {
		maxLast = right.maxLast
	}
	return &snapshotNode{
		first: first, last: last, value: value, priority: snapshotPriority(first),
		minFirst: minFirst, maxLast: maxLast, left: left, right: right,
	}
}

func snapshotMerge(left, right *snapshotNode) *snapshotNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if left.priority <= right.priority {
		next := snapshotMerge(left.right, right)
		if next == left.right {
			return left
		}
		return newSnapshotNode(left.first, left.last, left.value, left.left, next)
	}
	next := snapshotMerge(left, right.left)
	if next == right.left {
		return right
	}
	return newSnapshotNode(right.first, right.last, right.value, next, right.right)
}

// snapshotSplit partitions canonical intervals at key. An interval crossing
// key is split exactly; unchanged subtrees remain shared.
func snapshotSplit(root *snapshotNode, key uint64) (*snapshotNode, *snapshotNode) {
	if root == nil {
		return nil, nil
	}
	if key <= uint64(root.first) {
		left, middle := snapshotSplit(root.left, key)
		right := newSnapshotNode(root.first, root.last, root.value, middle, root.right)
		return left, right
	}
	if key > uint64(root.last) {
		middle, right := snapshotSplit(root.right, key)
		left := newSnapshotNode(root.first, root.last, root.value, root.left, middle)
		return left, right
	}
	// The right fragment has a new start key and therefore a new deterministic
	// priority. Re-merge both fragments with their subtrees rather than attaching
	// children directly and silently violating the treap heap invariant.
	left := snapshotMerge(root.left, newSnapshotNode(root.first, uint32(key-1), root.value, nil, nil))
	right := snapshotMerge(newSnapshotNode(uint32(key), root.last, root.value, nil, nil), root.right)
	return left, right
}

func snapshotPopMin(root *snapshotNode) (*snapshotNode, *snapshotNode) {
	if root.left == nil {
		return root, root.right
	}
	min, left := snapshotPopMin(root.left)
	return min, newSnapshotNode(root.first, root.last, root.value, left, root.right)
}

func snapshotPopMax(root *snapshotNode) (*snapshotNode, *snapshotNode) {
	if root.right == nil {
		return root, root.left
	}
	max, right := snapshotPopMax(root.right)
	return max, newSnapshotNode(root.first, root.last, root.value, root.left, right)
}

func snapshotConcat(left, right *snapshotNode) *snapshotNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	lmax, leftRest := snapshotPopMax(left)
	rmin, rightRest := snapshotPopMin(right)
	if lmax.value == rmin.value && lmax.last != ^uint32(0) && lmax.last+1 == rmin.first {
		middle := newSnapshotNode(lmax.first, rmin.last, lmax.value, nil, nil)
		return snapshotMerge(snapshotMerge(leftRest, middle), rightRest)
	}
	return snapshotMerge(snapshotMerge(leftRest, newSnapshotNode(lmax.first, lmax.last, lmax.value, nil, nil)),
		snapshotMerge(newSnapshotNode(rmin.first, rmin.last, rmin.value, nil, nil), rightRest))
}

func snapshotRange(root *snapshotNode, visit func(first, last, value uint32) bool) bool {
	if root == nil {
		return true
	}
	return snapshotRange(root.left, visit) && visit(root.first, root.last, root.value) && snapshotRange(root.right, visit)
}

func snapshotGet(root *snapshotNode, tid uint32) uint32 {
	for root != nil {
		if tid < root.first {
			root = root.left
		} else if tid > root.last {
			root = root.right
		} else {
			return root.value
		}
	}
	return 0
}

// snapshotSegment returns the value at pos and the first coordinate at which
// that value can change. A zero value describes a gap. Using searches rather
// than an iterator stack keeps logical clock iteration allocation-free without
// imposing a maximum tree depth.
func snapshotSegment(root *snapshotNode, pos uint64) (uint32, uint64) {
	const end = uint64(1) << 32
	if pos >= end {
		return 0, end
	}
	var next *snapshotNode
	for root != nil {
		if pos < uint64(root.first) {
			next = root
			root = root.left
			continue
		}
		if pos <= uint64(root.last) {
			return root.value, uint64(root.last) + 1
		}
		root = root.right
	}
	if next != nil {
		return 0, uint64(next.first)
	}
	return 0, end
}

func snapshotBuildFinite(ranges []FiniteRange) *snapshotNode {
	var root *snapshotNode
	for _, r := range ranges {
		root = snapshotMerge(root, newSnapshotNode(r.First, r.Last, r.Clock, nil, nil))
	}
	return root
}

func snapshotBuildRetired(ranges []RetiredRange) *snapshotNode {
	var root *snapshotNode
	for _, r := range ranges {
		root = snapshotMerge(root, newSnapshotNode(r.First, r.Last, 1, nil, nil))
	}
	return root
}

func snapshotRangeMax(root *snapshotNode, first, last, value uint32) *snapshotNode {
	if first > last || value == 0 {
		return root
	}
	left, rest := snapshotSplit(root, uint64(first))
	middle, right := snapshotSplit(rest, uint64(last)+1)
	ranges := make([]FiniteRange, 0, 4)
	cursor := uint64(first)
	snapshotRange(middle, func(a, b, old uint32) bool {
		if cursor < uint64(a) {
			ranges = append(ranges, FiniteRange{First: uint32(cursor), Last: a - 1, Clock: value})
		}
		if old < value {
			old = value
		}
		ranges = appendFiniteRun(ranges, FiniteRange{First: a, Last: b, Clock: old})
		cursor = uint64(b) + 1
		return true
	})
	if cursor <= uint64(last) {
		ranges = appendFiniteRun(ranges, FiniteRange{First: uint32(cursor), Last: last, Clock: value})
	}
	return snapshotConcat(snapshotConcat(left, snapshotBuildFinite(ranges)), right)
}

func snapshotRetire(root *snapshotNode, first, last uint32) *snapshotNode {
	if first > last {
		return root
	}
	left, rest := snapshotSplit(root, uint64(first))
	_, right := snapshotSplit(rest, uint64(last)+1)
	middle := newSnapshotNode(first, last, 1, nil, nil)
	return snapshotConcat(snapshotConcat(left, middle), right)
}

func snapshotSameLayoutJoin(left, right *snapshotNode) (*snapshotNode, bool) {
	if left == right {
		return left, true
	}
	if left == nil || right == nil || left.first != right.first || left.last != right.last {
		return nil, false
	}
	l, ok := snapshotSameLayoutJoin(left.left, right.left)
	if !ok {
		return nil, false
	}
	r, ok := snapshotSameLayoutJoin(left.right, right.right)
	if !ok {
		return nil, false
	}
	value := left.value
	if right.value > value {
		value = right.value
	}
	if value == left.value && l == left.left && r == left.right {
		return left, true
	}
	return newSnapshotNode(left.first, left.last, value, l, r), true
}

func snapshotJoinFinite(left, right *snapshotNode) *snapshotNode {
	if left == right || right == nil {
		return left
	}
	if left == nil {
		return right
	}
	if left.maxLast < right.minFirst {
		return snapshotConcat(left, right)
	}
	if right.maxLast < left.minFirst {
		return snapshotConcat(right, left)
	}
	if joined, ok := snapshotSameLayoutJoin(left, right); ok {
		return joined
	}
	result := left
	snapshotRange(right, func(first, last, value uint32) bool {
		result = snapshotRangeMax(result, first, last, value)
		return true
	})
	return result
}

func snapshotJoinRetired(left, right *snapshotNode) *snapshotNode {
	if left == right || right == nil {
		return left
	}
	if left == nil {
		return right
	}
	if left.maxLast < right.minFirst {
		return snapshotConcat(left, right)
	}
	if right.maxLast < left.minFirst {
		return snapshotConcat(right, left)
	}
	result := left
	snapshotRange(right, func(first, last, _ uint32) bool {
		result = snapshotRetire(result, first, last)
		return true
	})
	return result
}

// PointMax returns s unchanged when the update is dominated. Otherwise it
// returns a new snapshot sharing every untouched subtree.
func (s *ClockSnapshot) PointMax(tid, clock uint32) *ClockSnapshot {
	if clock == 0 || (s != nil && snapshotGet(s.retired, tid) != 0) {
		return s
	}
	if s != nil && snapshotGet(s.finite, tid) >= clock {
		return s
	}
	finite, retired := (*snapshotNode)(nil), (*snapshotNode)(nil)
	if s != nil {
		finite, retired = s.finite, s.retired
	}
	next := &ClockSnapshot{finite: snapshotRangeMax(finite, tid, tid, clock), retired: retired}
	if s != nil && s.lineage != nil && s.version != ^uint64(0) {
		next.lineage = s.lineage
		next.version = s.version + 1
		if s.lineage.tip.CompareAndSwap(s, next) {
			return next
		}
	}
	next.lineage = &snapshotLineage{}
	next.version = 1
	next.lineage.tip.Store(next)
	return next
}

func joinClockSnapshots(left, right *ClockSnapshot) *ClockSnapshot {
	if left == right || right == nil {
		return left
	}
	if left == nil {
		return right
	}
	if snapshotLineageLessOrEqual(left, right) {
		return right
	}
	if snapshotLineageLessOrEqual(right, left) {
		return left
	}
	finite := snapshotJoinFinite(left.finite, right.finite)
	retired := snapshotJoinRetired(left.retired, right.retired)
	finite = snapshotDropRetired(finite, retired)
	if finite == left.finite && retired == left.retired {
		return left
	}
	return newSnapshot(finite, retired)
}

func snapshotDropRetired(finite, retired *snapshotNode) *snapshotNode {
	result := finite
	snapshotRange(retired, func(first, last, _ uint32) bool {
		left, rest := snapshotSplit(result, uint64(first))
		_, right := snapshotSplit(rest, uint64(last)+1)
		result = snapshotConcat(left, right)
		return true
	})
	return result
}

func (vc *VectorClock) ownedSnapshot() *ClockSnapshot {
	finite := make([]FiniteRange, 0, len(vc.sparseRuns)+4)
	vc.rangeOwnedRuns(func(first, last, clock uint32) bool {
		finite = append(finite, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	retired := append([]RetiredRange(nil), vc.retired...)
	if len(finite) == 0 && len(retired) == 0 {
		return nil
	}
	return newSnapshot(snapshotBuildFinite(finite), snapshotBuildRetired(retired))
}

func (vc *VectorClock) snapshotLogical() *ClockSnapshot {
	root := joinClockSnapshots(vc.base, vc.ownedSnapshot())
	for i := 0; i < int(vc.causal.count); i++ {
		causal := vc.causal.roots[i].materialize()
		root = joinClockSnapshots(root, causal.ownedSnapshot())
		causal.Release()
	}
	return root
}

func (vc *VectorClock) clearOwned() {
	vc.invalidateDenseProjectionWitness()
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
	vc.sparseRuns = vc.sparseRuns[:0]
	vc.retired = vc.retired[:0]
}

// Freeze returns the exact immutable logical value and installs it as vc's
// shared base. Only the owned overlay is cleared.
func (vc *VectorClock) Freeze() *ClockSnapshot {
	if vc.base != nil && vc.ownedEmpty() && !vc.causal.Valid() {
		return vc.base
	}
	root := vc.snapshotLogical()
	vc.clearOwned()
	vc.causal.Release()
	vc.base = root
	return root
}

// JoinSnapshot pointwise joins one immutable checkpoint. Nil is a no-op.
func (vc *VectorClock) JoinSnapshot(snapshot *ClockSnapshot) {
	if vc.TryJoinSnapshot(snapshot) {
		return
	}
	vc.base = joinClockSnapshots(vc.base, snapshot)
}

// TryJoinSnapshot is the allocation-free, nonblocking form of JoinSnapshot.
// It succeeds when the immutable root can be adopted directly or one side is
// already dominated. A false result leaves vc logically and physically
// unchanged, allowing callers to leave a pinned fast path before falling back
// to the allocating structural join.
func (vc *VectorClock) TryJoinSnapshot(snapshot *ClockSnapshot) bool {
	if snapshot == nil || vc.base == snapshot {
		return true
	}
	// A later point-max version in the same linear lineage dominates the old
	// base by construction. Adopt it before scanning the incoming tree; the
	// context-owned overlay remains in place and is already joined by Get.
	if vc.base == nil || snapshotLineageLessOrEqual(vc.base, snapshot) {
		vc.base = snapshot
		return true
	}
	// The destination may also retain unrelated causal roots. Its immutable
	// base is still an exact lower bound for the complete logical clock, so a
	// same-lineage snapshot already dominated by that base is a no-op without
	// inspecting or materializing the other roots.
	if snapshotLineageLessOrEqual(snapshot, vc.base) {
		return true
	}
	// Exact comparison against an unrelated causal root requires one cold
	// materialization. Preserve TryJoinSnapshot's allocation-free and
	// all-or-nothing contract by delegating that case to JoinSnapshot.
	if vc.causal.Valid() {
		return false
	}
	if snapshotLessOrEqualClock(snapshot, vc) {
		return true
	}
	if snapshotLessOrEqual(vc.base, snapshot) {
		vc.base = snapshot
		return true
	}
	return false
}

func (vc *VectorClock) materializeBase() {
	root := vc.base
	if root == nil {
		return
	}
	vc.base = nil
	snapshotRange(root.finite, func(first, last, clock uint32) bool {
		vc.JoinRange(first, last, clock)
		return true
	})
	snapshotRange(root.retired, func(first, last, _ uint32) bool {
		vc.RetireRange(first, last)
		return true
	})
}

// materializeCausal lowers all pinned causal roots into the existing exact
// base/owned representation. It is a cold boundary for overflow and destructive
// operations; inline synchronization never reaches it before capacity.
func (vc *VectorClock) materializeCausal() {
	if !vc.causal.Valid() {
		return
	}
	roots := vc.causal
	vc.causal = causalRootSet{}
	for i := 0; i < int(roots.count); i++ {
		imported := roots.roots[i].materialize()
		roots.roots[i].Release()
		vc.Join(imported)
		imported.Release()
	}
}

func (vc *VectorClock) materializeRoots() {
	vc.materializeCausal()
	vc.materializeBase()
}

func snapshotLessOrEqual(left, right *ClockSnapshot) bool {
	if left == nil || left == right {
		return true
	}
	if right == nil {
		return false
	}
	if snapshotLineageLessOrEqual(left, right) {
		return true
	}
	ok := true
	snapshotRange(left.retired, func(first, last, _ uint32) bool {
		for pos, limit := uint64(first), uint64(last)+1; pos < limit; {
			retired, next := snapshotSegment(right.retired, pos)
			if retired == 0 {
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
	snapshotRange(left.finite, func(first, last, clock uint32) bool {
		for pos, limit := uint64(first), uint64(last)+1; pos < limit; {
			retired, retiredNext := snapshotSegment(right.retired, pos)
			finite, finiteNext := snapshotSegment(right.finite, pos)
			next := retiredNext
			if finiteNext < next {
				next = finiteNext
			}
			if next > limit {
				next = limit
			}
			if retired == 0 && finite < clock {
				ok = false
				return false
			}
			pos = next
		}
		return true
	})
	return ok
}

func snapshotLessOrEqualClock(left *ClockSnapshot, right *VectorClock) bool {
	if right.causal.Valid() {
		copy := right.CloneDetached()
		copy.materializeCausal()
		result := snapshotLessOrEqualClock(left, copy)
		copy.Release()
		return result
	}
	if left == nil || left == right.base && right.ownedEmpty() && !right.causal.Valid() {
		return true
	}
	if snapshotLineageLessOrEqual(left, right.base) {
		return true
	}
	ok := true
	snapshotRange(left.retired, func(first, last, _ uint32) bool {
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
	snapshotRange(left.finite, func(first, last, clock uint32) bool {
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
