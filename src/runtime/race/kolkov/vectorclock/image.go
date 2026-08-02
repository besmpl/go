package vectorclock

// clockImage is a compact, immutable copy of one complete logical vector
// clock. Unlike ClockSnapshot it owns flat storage and therefore keeps no
// persistent tree nodes alive. A clockImage is built before publication and is
// never mutated afterwards, so its read methods require neither locks nor
// allocation.
type clockImage struct {
	clocks       [DenseThreads]uint32
	maxDense     uint16
	denseTail    []uint32
	denseTailMin []uint32
	sparseRuns   []finiteRun
	retired      []RetiredRange
}

const clockImageDenseMinBlock = uint64(DenseThreads)

// clockImageSource supplies canonical finite and retired ranges. Construction
// is the only allocating operation; reads operate solely on the resulting flat
// buffers.
type clockImageSource struct {
	rangeFinite  func(func(first, last, clock uint32) bool)
	rangeRetired func(func(first, last uint32) bool)
}

// newClockImage captures vc's full logical value, including an immutable base
// and any context-owned overlay. Later mutations of vc cannot affect the image.
func newClockImage(vc *VectorClock) *clockImage {
	if vc == nil {
		return &clockImage{}
	}
	return buildClockImage(clockImageSource{
		rangeFinite:  vc.RangeRuns,
		rangeRetired: vc.RangeRetired,
	})
}

// newClockImageFromSnapshot flattens a persistent snapshot without first
// materializing it into a VectorClock.
func newClockImageFromSnapshot(snapshot *ClockSnapshot) *clockImage {
	if snapshot == nil {
		return &clockImage{}
	}
	return buildClockImage(clockImageSource{
		rangeFinite: func(visit func(first, last, clock uint32) bool) {
			snapshotRange(snapshot.finite, visit)
		},
		rangeRetired: func(visit func(first, last uint32) bool) {
			snapshotRange(snapshot.retired, func(first, last, _ uint32) bool {
				return visit(first, last)
			})
		},
	})
}

// buildClockImage chooses a dense high-TID prefix only when its four-byte
// coordinates are clearly cheaper than run metadata. The policy mirrors the
// mutable clock's hybrid promotion rule: at least 64 high runs, and no more
// than two coordinates per run. The chosen prefix always ends at a run
// boundary, leaving sparseRuns canonical and wholly above denseTail.
func buildClockImage(source clockImageSource) *clockImage {
	image := &clockImage{}
	highRuns, denseRuns := 0, 0
	denseEnd := uint64(DenseThreads)

	source.rangeFinite(func(first, last, _ uint32) bool {
		if last < DenseThreads {
			return true
		}
		if first < DenseThreads {
			first = DenseThreads
		}
		highRuns++
		span := uint64(last) + 1 - DenseThreads
		if highRuns >= minDenseTailRuns && span <= uint64(highRuns)*2 &&
			span <= uint64(^uint(0)>>1) {
			denseRuns = highRuns
			denseEnd = uint64(last) + 1
		}
		return true
	})

	if denseEnd > DenseThreads {
		denseLength := int(denseEnd - DenseThreads)
		minBlocks := denseLength / int(clockImageDenseMinBlock)
		backing := make([]uint32, denseLength+minBlocks)
		image.denseTail = backing[:denseLength:denseLength]
		image.denseTailMin = backing[denseLength:]
	}
	if sparseCapacity := highRuns - denseRuns; sparseCapacity != 0 {
		image.sparseRuns = make([]finiteRun, 0, sparseCapacity)
	}

	source.rangeFinite(func(first, last, clock uint32) bool {
		if first < DenseThreads {
			inlineLast := last
			if inlineLast >= DenseThreads {
				inlineLast = DenseThreads - 1
			}
			for tid := first; tid <= inlineLast; tid++ {
				image.clocks[tid] = clock
			}
			if inlineLast > uint32(image.maxDense) {
				image.maxDense = uint16(inlineLast)
			}
			if last < DenseThreads {
				return true
			}
			first = DenseThreads
		}

		if uint64(first) < denseEnd {
			tailLast := last
			if uint64(tailLast) >= denseEnd {
				tailLast = uint32(denseEnd - 1)
			}
			for tid := uint64(first); tid <= uint64(tailLast); tid++ {
				image.denseTail[tid-DenseThreads] = clock
			}
			if last == tailLast {
				return true
			}
			first = tailLast + 1
		}

		image.sparseRuns = append(image.sparseRuns, finiteRun{
			First: first, Last: last, Clock: clock,
		})
		return true
	})

	source.rangeRetired(func(first, last uint32) bool {
		image.retired = append(image.retired, RetiredRange{First: first, Last: last})
		return true
	})
	image.buildDenseTailMin()
	return image
}

// buildDenseTailMin records a logical lower bound for each immutable 64-point
// block. Lineage point updates are monotonic, so an anchor block whose minimum
// dominates an event epoch remains dominated by every pinned version in that
// segment. Retired coordinates contribute +infinity, matching Get.
func (image *clockImage) buildDenseTailMin() {
	if image == nil || len(image.denseTailMin) == 0 {
		return
	}
	for block := uint64(0); block < uint64(len(image.denseTailMin)); block++ {
		first := block * clockImageDenseMinBlock
		last := first + clockImageDenseMinBlock
		minimum := ^uint32(0)
		for offset := first; offset < last; offset++ {
			clock := image.denseTail[offset]
			if image.IsRetired(uint32(DenseThreads + offset)) {
				clock = ^uint32(0)
			}
			if clock < minimum {
				minimum = clock
			}
		}
		image.denseTailMin[block] = minimum
	}
}

// dominatesAlignedBlock reports an exact lower-bound proof for one complete,
// aligned block. Dense-tail minima make the common contiguous-cohort proof
// constant time; one covering sparse finite or retirement run is also enough.
// False is conservative for mixed structural coverage.
func (image *clockImage) dominatesAlignedBlock(first, clock uint32) bool {
	if image == nil || clock == 0 {
		return clock == 0
	}
	last64 := uint64(first) + clockImageDenseMinBlock - 1
	if last64 > uint64(^uint32(0)) ||
		uint64(first)%clockImageDenseMinBlock != 0 {
		return false
	}
	last := uint32(last64)
	if first < DenseThreads {
		for tid := first; tid <= last; tid++ {
			if image.Get(tid) < clock {
				return false
			}
		}
		return true
	}
	offset := uint64(first) - DenseThreads
	if offset+clockImageDenseMinBlock <= uint64(len(image.denseTail)) {
		block := offset / clockImageDenseMinBlock
		return block < uint64(len(image.denseTailMin)) && image.denseTailMin[block] >= clock
	}
	lo, hi := 0, len(image.retired)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if image.retired[mid].Last < first {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(image.retired) && image.retired[lo].First <= first && image.retired[lo].Last >= last {
		return true
	}
	lo, hi = 0, len(image.sparseRuns)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if image.sparseRuns[mid].Last < first {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(image.sparseRuns) && image.sparseRuns[lo].First <= first &&
		image.sparseRuns[lo].Last >= last && image.sparseRuns[lo].Clock >= clock
}

// IsRetired reports whether tid has immutable +infinity causal metadata.
func (image *clockImage) IsRetired(tid uint32) bool {
	if image == nil {
		return false
	}
	lo, hi := 0, len(image.retired)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if image.retired[mid].Last < tid {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(image.retired) && image.retired[lo].First <= tid
}

// Get returns the exact logical clock at tid. Retired coordinates read as
// +infinity, matching VectorClock.Get.
func (image *clockImage) Get(tid uint32) uint32 {
	if image == nil {
		return 0
	}
	if image.IsRetired(tid) {
		return ^uint32(0)
	}
	if tid < DenseThreads {
		return image.clocks[tid]
	}
	if offset := uint64(tid) - DenseThreads; offset < uint64(len(image.denseTail)) {
		return image.denseTail[offset]
	}
	lo, hi := 0, len(image.sparseRuns)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if image.sparseRuns[mid].Last < tid {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(image.sparseRuns) && image.sparseRuns[lo].First <= tid {
		return image.sparseRuns[lo].Clock
	}
	return 0
}

type clockImageRunEmitter struct {
	visit              func(first, last, clock uint32) bool
	first, last, clock uint32
	haveRun, stopped   bool
}

func (emitter *clockImageRunEmitter) add(first, last, clock uint32) bool {
	if clock == 0 {
		return true
	}
	if emitter.haveRun && emitter.last != ^uint32(0) &&
		emitter.last+1 == first && emitter.clock == clock {
		emitter.last = last
		return true
	}
	if emitter.haveRun && !emitter.visit(emitter.first, emitter.last, emitter.clock) {
		emitter.stopped = true
		return false
	}
	emitter.first, emitter.last, emitter.clock = first, last, clock
	emitter.haveRun = true
	return true
}

func (emitter *clockImageRunEmitter) finish() {
	if emitter.haveRun && !emitter.stopped {
		emitter.visit(emitter.first, emitter.last, emitter.clock)
	}
}

// RangeRuns visits finite non-zero components as inclusive canonical runs in
// ascending TID order. Iteration stops when visit returns false.
func (image *clockImage) RangeRuns(visit func(first, last, clock uint32) bool) {
	if image == nil {
		return
	}
	emitter := clockImageRunEmitter{visit: visit}
	for tid := uint32(0); tid <= uint32(image.maxDense); tid++ {
		if clock := image.clocks[tid]; clock != 0 && !emitter.add(tid, tid, clock) {
			return
		}
	}
	for offset, clock := range image.denseTail {
		if clock == 0 {
			continue
		}
		tid := uint32(DenseThreads + offset)
		if !emitter.add(tid, tid, clock) {
			return
		}
	}
	for _, run := range image.sparseRuns {
		if !emitter.add(run.First, run.Last, run.Clock) {
			return
		}
	}
	emitter.finish()
}

// RangeRetired visits immutable +infinity ranges in ascending order. Iteration
// stops when visit returns false.
func (image *clockImage) RangeRetired(visit func(first, last uint32) bool) {
	if image == nil {
		return
	}
	for _, retired := range image.retired {
		if !visit(retired.First, retired.Last) {
			return
		}
	}
}

// coordinateCount is the number of finite, non-zero coordinates in image.
// Retired coordinates are deliberately excluded because they do not produce
// lineage point updates.
func (image *clockImage) coordinateCount() uint64 {
	if image == nil {
		return 0
	}
	count := uint64(0)
	for tid := uint32(0); tid <= uint32(image.maxDense); tid++ {
		if image.clocks[tid] != 0 {
			count++
		}
	}
	for _, clock := range image.denseTail {
		if clock != 0 {
			count++
		}
	}
	for _, run := range image.sparseRuns {
		count += uint64(run.Last) - uint64(run.First) + 1
	}
	return count
}

// appendTo max-joins image into dst and then applies its immutable retirement
// markers. It is intended for the allocating segment-rotation path, not pinned
// reads.
func (image *clockImage) appendTo(dst *VectorClock) {
	if image == nil || dst == nil {
		return
	}
	image.RangeRuns(func(first, last, clock uint32) bool {
		dst.JoinRange(first, last, clock)
		return true
	})
	image.RangeRetired(func(first, last uint32) bool {
		dst.RetireRange(first, last)
		return true
	})
}

// materialize returns an independently mutable clock with the image's exact
// canonical representation. Lineage anchors are already normalized, so copying
// their flat buffers avoids rebuilding the same representation one range at a
// time (and avoids a temporary bulk-import slice on the common path).
func (image *clockImage) materialize() *VectorClock {
	vc := NewFromPool()
	if image == nil {
		return vc
	}
	vc.clocks = image.clocks
	vc.maxDense = image.maxDense
	vc.denseTail = append(vc.denseTail, image.denseTail...)
	vc.sparseRuns = append(vc.sparseRuns, image.sparseRuns...)
	vc.retired = append(vc.retired, image.retired...)
	return vc
}
