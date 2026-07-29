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
	// The common path keeps the original 4 KiB, direct-indexed clock array.
	DenseThreads = 1024

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
	clocks     [DenseThreads]uint32
	maxDense   uint16
	poolShard  uint8
	denseTail  []uint32
	sparseRuns []finiteRun
	retired    []RetiredRange
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
	for i := uint32(0); i <= uint32(vc.maxDense); i++ {
		vc.clocks[i] = 0
	}
	vc.maxDense = 0
	for i := range vc.denseTail {
		vc.denseTail[i] = 0
	}
	vc.denseTail = vc.denseTail[:0]
	vc.sparseRuns = vc.sparseRuns[:0]
	vc.retired = vc.retired[:0]
}

func (vc *VectorClock) Clone() *VectorClock {
	clone := poolGet()
	clone.copyFromZero(vc)
	return clone
}

func (vc *VectorClock) copyFromZero(other *VectorClock) {
	cloneLimit := uint32(other.maxDense)
	for i := uint32(0); i <= cloneLimit; i++ {
		vc.clocks[i] = other.clocks[i]
	}
	vc.maxDense = other.maxDense
	if len(other.denseTail) != 0 {
		if cap(vc.denseTail) < len(other.denseTail) {
			vc.denseTail = make([]uint32, len(other.denseTail))
		} else {
			vc.denseTail = vc.denseTail[:len(other.denseTail)]
		}
		copy(vc.denseTail, other.denseTail)
	}
	if len(other.sparseRuns) != 0 {
		if cap(vc.sparseRuns) < len(other.sparseRuns) {
			vc.sparseRuns = make([]finiteRun, len(other.sparseRuns))
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

func (vc *VectorClock) denseTailEnd() uint64 {
	return uint64(DenseThreads) + uint64(len(vc.denseTail))
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
	for i, run := range vc.sparseRuns {
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
	for _, run := range vc.sparseRuns[:best] {
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
	if len(vc.sparseRuns) == 0 {
		if !sparseRunsOverlapRetiredFrom(other, floor, vc.retired) {
			if cap(vc.sparseRuns) < len(other) {
				vc.sparseRuns = make([]finiteRun, 0, len(other))
			}
			for i, r := range other {
				if i == 0 {
					r.First = otherFirst
				}
				vc.sparseRuns = appendFiniteRun(vc.sparseRuns, r)
			}
			vc.maybePromoteDenseTail()
			return
		}
	}

	// Repeated release-merge/acquire frequently advances clocks without
	// changing their run boundaries. Update that layout in place and coalesce
	// any newly equal neighbors instead of allocating a replacement slice.
	if len(vc.sparseRuns) == len(other) && !sparseRunsOverlapRetiredFrom(other, floor, vc.retired) {
		sameLayout := true
		for i, r := range other {
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
			for i, r := range other {
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
	out := make([]finiteRun, 0, len(vc.sparseRuns)+len(other))
	i, j, retiredIndex := 0, 0, 0
	pos := uint64(otherFirst)
	if len(vc.sparseRuns) != 0 && uint64(vc.sparseRuns[0].First) < pos {
		pos = uint64(vc.sparseRuns[0].First)
	}
	for retiredIndex < len(vc.retired) && uint64(vc.retired[retiredIndex].Last) < pos {
		retiredIndex++
	}
	for pos < end {
		for i < len(vc.sparseRuns) && uint64(vc.sparseRuns[i].Last) < pos {
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
		if i < len(vc.sparseRuns) {
			r := vc.sparseRuns[i]
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
	for _, l := range left {
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

func sparseRunsLessOrEqual(left, right []finiteRun, rightRetired []RetiredRange) bool {
	return sparseRangesLessOrEqual(left, 0, right, rightRetired)
}

// PruneLessOrEqual removes each finite component already observed by observed.
// It never imports observed's finite components or retirement metadata.
func (vc *VectorClock) PruneLessOrEqual(observed *VectorClock) {
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
	if tid < DenseThreads {
		return vc.clocks[tid]
	}
	if offset := uint64(tid) - DenseThreads; offset < uint64(len(vc.denseTail)) {
		return vc.denseTail[offset]
	}
	idx := vc.searchSparseRun(tid)
	if idx < len(vc.sparseRuns) && vc.sparseRuns[idx].First <= tid {
		return vc.sparseRuns[idx].Clock
	}
	return 0
}

func (vc *VectorClock) Set(tid, clock uint32) {
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
		vc.denseTail[offset] = clock
		return
	}
	if offset == uint64(len(vc.denseTail)) && len(vc.denseTail) != 0 && clock != 0 &&
		(len(vc.sparseRuns) == 0 || vc.sparseRuns[0].First > tid) {
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
	for _, r := range runs {
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

	merged := make([]RetiredRange, 0, len(vc.retired)+len(ranges))
	appendRange := func(r RetiredRange) {
		if n := len(merged); n != 0 {
			last := &merged[n-1]
			if r.First <= last.Last || (last.Last != ^uint32(0) && r.First == last.Last+1) {
				if r.Last > last.Last {
					last.Last = r.Last
				}
				return
			}
		}
		merged = append(merged, r)
	}
	i, j := 0, 0
	for i < len(vc.retired) || j < len(ranges) {
		if j == len(ranges) || (i < len(vc.retired) && vc.retired[i].First <= ranges[j].First) {
			appendRange(vc.retired[i])
			i++
		} else {
			appendRange(ranges[j])
			j++
		}
	}
	vc.retired = merged
	vc.dropRetiredFiniteEntries()
}

func (vc *VectorClock) dropRetiredFiniteEntries() {
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
	for _, r := range vc.retired {
		if !visit(r.First, r.Last) {
			return
		}
	}
}

func (vc *VectorClock) GetMaxTID() uint32 {
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
