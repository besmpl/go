package vectorclock

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

type pointClockModel struct {
	finite  map[uint32]uint32
	retired map[uint32]bool
}

func newPointClockModel() *pointClockModel {
	return &pointClockModel{finite: make(map[uint32]uint32), retired: make(map[uint32]bool)}
}

func (m *pointClockModel) get(tid uint32) uint32 {
	if m.retired[tid] {
		return ^uint32(0)
	}
	return m.finite[tid]
}

func (m *pointClockModel) set(tid, clock uint32) {
	if m.retired[tid] {
		return
	}
	if clock == 0 {
		delete(m.finite, tid)
	} else {
		m.finite[tid] = clock
	}
}

func (m *pointClockModel) joinRange(first, last, clock uint32) {
	for tid := first; tid <= last; tid++ {
		if clock > m.get(tid) {
			m.set(tid, clock)
		}
	}
}

func (m *pointClockModel) retire(first, last uint32) {
	for tid := first; tid <= last; tid++ {
		m.retired[tid] = true
		delete(m.finite, tid)
	}
}

func (m *pointClockModel) join(other *pointClockModel) {
	for tid, retired := range other.retired {
		if retired {
			m.retired[tid] = true
			delete(m.finite, tid)
		}
	}
	for tid, clock := range other.finite {
		if !m.retired[tid] && clock > m.finite[tid] {
			m.finite[tid] = clock
		}
	}
}

func (m *pointClockModel) prune(observed *pointClockModel) {
	for tid, clock := range m.finite {
		if clock <= observed.get(tid) {
			delete(m.finite, tid)
		}
	}
}

func pointModelLessOrEqual(left, right *pointClockModel, maxTID uint32) bool {
	for tid := uint32(0); tid <= maxTID; tid++ {
		if left.retired[tid] && !right.retired[tid] {
			return false
		}
		if !left.retired[tid] && left.finite[tid] > right.get(tid) {
			return false
		}
	}
	return true
}

func assertVectorClockModel(t *testing.T, vc *VectorClock, model *pointClockModel, maxTID uint32) {
	t.Helper()
	assertVectorClockRepresentation(t, vc)
	for tid := uint32(0); tid <= maxTID; tid++ {
		if got, want := vc.Get(tid), model.get(tid); got != want || vc.IsRetired(tid) != model.retired[tid] {
			t.Fatalf("TID %d: Get=%d retired=%v, want %d/%v", tid, got, vc.IsRetired(tid), want, model.retired[tid])
		}
	}
	for i, got := range vc.denseTail {
		tid := uint32(DenseThreads + i)
		if want := model.finite[tid]; got != want {
			t.Fatalf("dense tail clock[%d]=%d, want %d", tid, got, want)
		}
	}
}

func assertVectorClockRepresentation(t *testing.T, vc *VectorClock) {
	t.Helper()
	for tid, clock := range vc.clocks {
		if clock != 0 && vc.IsRetired(uint32(tid)) {
			t.Fatalf("dense clock[%d]=%d overlaps retirement", tid, clock)
		}
	}
	for i, clock := range vc.denseTail {
		tid := uint32(DenseThreads + i)
		if clock != 0 && vc.IsRetired(tid) {
			t.Fatalf("dense tail clock[%d]=%d overlaps retirement", tid, clock)
		}
	}
	denseTailEnd := vc.denseTailEnd()
	retiredIndex := 0
	for i, run := range vc.sparseRuns {
		if uint64(run.First) < denseTailEnd || run.First > run.Last || run.Clock == 0 {
			t.Fatalf("invalid sparse run %d: %+v", i, run)
		}
		if i != 0 {
			previous := vc.sparseRuns[i-1]
			if previous.Last >= run.First || (previous.Last != ^uint32(0) && previous.Last+1 == run.First && previous.Clock == run.Clock) {
				t.Fatalf("non-canonical sparse runs: %+v", vc.sparseRuns)
			}
		}
		for retiredIndex < len(vc.retired) && vc.retired[retiredIndex].Last < run.First {
			retiredIndex++
		}
		if retiredIndex < len(vc.retired) && vc.retired[retiredIndex].First <= run.Last {
			t.Fatalf("sparse run %+v overlaps retirement %+v", run, vc.retired[retiredIndex])
		}
	}
}

func TestVectorClockCompressedRunsRandomDifferential(t *testing.T) {
	const maxTID = DenseThreads + 160
	clocks := [2]*VectorClock{New(), New()}
	models := [2]*pointClockModel{newPointClockModel(), newPointClockModel()}
	seed := uint32(0x9e3779b9)
	next := func(n uint32) uint32 {
		seed = seed*1664525 + 1013904223
		return seed % n
	}

	for step := 0; step < 3000; step++ {
		which := int(next(2))
		other := 1 - which
		vc, model := clocks[which], models[which]
		switch next(8) {
		case 0:
			tid := next(maxTID + 1)
			clock := next(6)
			vc.Set(tid, clock)
			model.set(tid, clock)
		case 1:
			first := next(maxTID + 1)
			last := first + next(12)
			if last > maxTID {
				last = maxTID
			}
			clock := next(5) + 1
			vc.JoinRange(first, last, clock)
			model.joinRange(first, last, clock)
		case 2:
			first := next(maxTID) + 1
			last := first + next(8)
			if last > maxTID {
				last = maxTID
			}
			vc.RetireRange(first, last)
			model.retire(first, last)
		case 3:
			vc.Join(clocks[other])
			model.join(models[other])
		case 4:
			vc.PruneLessOrEqual(clocks[other])
			model.prune(models[other])
		case 5:
			clone := vc.Clone()
			assertVectorClockModel(t, clone, model, maxTID)
			clone.Set(DenseThreads+150, uint32(step%5+1))
			assertVectorClockModel(t, vc, model, maxTID)
			clone.Release()
		case 6:
			copyClock := New()
			copyClock.JoinRange(DenseThreads, maxTID, 99)
			copyClock.CopyFrom(vc)
			assertVectorClockModel(t, copyClock, model, maxTID)
		case 7:
			first := next(maxTID - 20)
			ranges := make([]FiniteRange, 0, 4)
			for i := 0; i < 4 && first <= maxTID; i++ {
				last := first + next(4)
				if last > maxTID {
					last = maxTID
				}
				clock := next(5) + 1
				ranges = append(ranges, FiniteRange{First: first, Last: last, Clock: clock})
				model.joinRange(first, last, clock)
				first = last + next(3) + 1
			}
			vc.JoinRanges(ranges)
		}
		assertVectorClockModel(t, clocks[0], models[0], maxTID)
		assertVectorClockModel(t, clocks[1], models[1], maxTID)
		for left := 0; left < 2; left++ {
			right := 1 - left
			if got, want := clocks[left].LessOrEqual(clocks[right]), pointModelLessOrEqual(models[left], models[right], maxTID); got != want {
				t.Fatalf("step %d: LessOrEqual(%d,%d)=%v, want %v", step, left, right, got, want)
			}
		}
	}
}

func TestVectorClockCompressedRunScalingAndMaxEndpoint(t *testing.T) {
	vc := New()
	vc.JoinRange(DenseThreads, DenseThreads+100_000-1, 1)
	if len(vc.sparseRuns) != 1 {
		t.Fatalf("100k equal coordinates used %d runs, want 1", len(vc.sparseRuns))
	}
	callbacks := 0
	vc.RangeRuns(func(first, last, clock uint32) bool {
		callbacks++
		if first != DenseThreads || last != DenseThreads+100_000-1 || clock != 1 {
			t.Fatalf("compressed run = [%d,%d]@%d", first, last, clock)
		}
		return true
	})
	if callbacks != 1 {
		t.Fatalf("RangeRuns made %d callbacks for one structural run", callbacks)
	}

	child := New()
	child.Set(DenseThreads+100_000, 1)
	vc.RangeRuns(func(first, last, clock uint32) bool {
		child.JoinRange(first, last, clock)
		return true
	})
	if len(child.sparseRuns) != 1 || child.sparseRuns[0].First != DenseThreads || child.sparseRuns[0].Last != DenseThreads+100_000 {
		t.Fatalf("chain acquire expanded frontier: %+v", child.sparseRuns)
	}

	maxClock := New()
	maxClock.JoinRange(^uint32(0)-10, ^uint32(0), 3)
	maxClock.Set(^uint32(0), 4)
	if got := maxClock.Get(^uint32(0)); got != 4 || len(maxClock.sparseRuns) != 2 {
		t.Fatalf("MaxUint32 split = %d, runs=%+v", got, maxClock.sparseRuns)
	}
	maxClock.Set(^uint32(0), 3)
	if len(maxClock.sparseRuns) != 1 {
		t.Fatalf("MaxUint32 merge left %+v", maxClock.sparseRuns)
	}
	maxClock.RetireRange(^uint32(0)-2, ^uint32(0))
	if len(maxClock.sparseRuns) != 1 || maxClock.sparseRuns[0].Last != ^uint32(0)-3 || !maxClock.IsRetired(^uint32(0)) {
		t.Fatalf("MaxUint32 retirement = runs %+v", maxClock.sparseRuns)
	}
}

func TestVectorClockPruneLessOrEqualStructural(t *testing.T) {
	vc := New()
	vc.JoinRange(1, DenseThreads+100_000, 2)
	vc.Set(7, 5)
	observed := New()
	observed.JoinRange(1, DenseThreads+20_000, 2)
	observed.JoinRange(DenseThreads+40_000, DenseThreads+60_000, 3)
	observed.RetireRange(DenseThreads+80_000, DenseThreads+90_000)

	vc.PruneLessOrEqual(observed)
	for _, tid := range []uint32{1, DenseThreads, DenseThreads + 10_000, DenseThreads + 50_000, DenseThreads + 85_000} {
		if tid == 7 {
			continue
		}
		if vc.Get(tid) != 0 {
			t.Fatalf("observed TID %d was not pruned", tid)
		}
	}
	if vc.Get(7) != 5 || vc.IsRetired(DenseThreads+85_000) {
		t.Fatal("prune removed a concurrent value or copied retirement")
	}
	if len(vc.sparseRuns) != 3 {
		t.Fatalf("structural prune produced %d sparse runs, want 3: %+v", len(vc.sparseRuns), vc.sparseRuns)
	}

	self := vc.Clone()
	defer self.Release()
	self.PruneLessOrEqual(self)
	finite := 0
	self.Range(func(_, _ uint32) bool { finite++; return true })
	if finite != 0 {
		t.Fatalf("self prune retained %d finite entries", finite)
	}

	noChange := New()
	noChange.JoinRange(DenseThreads, DenseThreads+100_000, 5)
	behind := New()
	behind.JoinRange(DenseThreads, DenseThreads+100_000, 4)
	if allocs := testing.AllocsPerRun(1000, func() {
		noChange.PruneLessOrEqual(behind)
	}); allocs != 0 {
		t.Fatalf("no-op sparse prune allocated %.2f objects per call", allocs)
	}
}

func TestVectorClockJoinCannotReintroduceFiniteRetiredCoordinate(t *testing.T) {
	const tid = DenseThreads + 50_000
	receiver := New()
	receiver.RetireRange(tid-10, tid+10)
	finite := New()
	finite.Set(tid, 17)

	receiver.Join(finite)
	if receiver.Get(tid) != ^uint32(0) || !receiver.IsRetired(tid) {
		t.Fatalf("join reintroduced retired coordinate %d", tid)
	}
	receiver.Range(func(gotTID, _ uint32) bool {
		if gotTID >= tid-10 && gotTID <= tid+10 {
			t.Fatalf("Range exposed retired coordinate %d", gotTID)
		}
		return true
	})
	receiver.RangeRuns(func(first, last, _ uint32) bool {
		if first <= tid+10 && last >= tid-10 {
			t.Fatalf("RangeRuns exposed retired interval in [%d,%d]", first, last)
		}
		return true
	})
}

func TestVectorClockRepeatedHighTIDAcquireAndRetirementPropagationAllocateNothing(t *testing.T) {
	const first = DenseThreads + 10_000
	receiver := New()
	receiver.JoinRange(first, first+100_000, 7)
	release := New()
	release.JoinRange(first+100, first+90_000, 6)

	if allocs := testing.AllocsPerRun(1000, func() {
		receiver.Join(release)
	}); allocs != 0 {
		t.Fatalf("dominated high-TID acquire allocated %.2f objects per call", allocs)
	}

	marker := RetiredRange{First: first + 110_000, Last: first + 120_000}
	receiver.RetireRanges([]RetiredRange{marker})
	release.RetireRanges([]RetiredRange{marker})
	markers := []RetiredRange{marker}
	if allocs := testing.AllocsPerRun(1000, func() {
		receiver.RetireRanges(markers)
		receiver.dropRetiredFiniteEntries()
		receiver.Join(release)
	}); allocs != 0 {
		t.Fatalf("repeated finalizer marker propagation allocated %.2f objects per call", allocs)
	}
}

func TestVectorClockBulkFragmentedJoinScalingAndRetirement(t *testing.T) {
	const (
		first = DenseThreads + 10_000
		runs  = 4096
	)
	ranges := make([]FiniteRange, runs)
	for i := range ranges {
		tid := first + uint32(i)
		ranges[i] = FiniteRange{First: tid, Last: tid, Clock: uint32(i&1) + 1}
	}
	vc := New()
	vc.JoinRanges(ranges)
	if len(vc.sparseRuns) != runs {
		t.Fatalf("alternating bulk join produced %d runs, want %d", len(vc.sparseRuns), runs)
	}
	for i := range ranges {
		if got := vc.Get(ranges[i].First); got != ranges[i].Clock {
			t.Fatalf("bulk clock[%d] = %d, want %d", ranges[i].First, got, ranges[i].Clock)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		for i := range ranges {
			ranges[i].Clock += 2
		}
		vc.JoinRanges(ranges)
	}); allocs != 0 {
		t.Fatalf("same-layout advancing bulk join allocated %.2f objects per call", allocs)
	}

	retired := New()
	retired.RetireRange(first+100, first+200)
	if allocs := testing.AllocsPerRun(1000, func() {
		retired.JoinRange(first+120, first+180, 9)
	}); allocs != 0 {
		t.Fatalf("retirement-dominated JoinRange allocated %.2f objects per call", allocs)
	}
	retired.JoinRange(first+50, first+250, 9)
	if len(retired.sparseRuns) != 2 || retired.Get(first+150) != ^uint32(0) || retired.Get(first+50) != 9 || retired.Get(first+250) != 9 {
		t.Fatalf("partial retirement bulk join = runs %+v", retired.sparseRuns)
	}
}

func TestVectorClockJoinRangesBoundaryAdjacentAndMaxEndpoint(t *testing.T) {
	plain := New()
	plain.JoinRanges([]FiniteRange{
		{First: DenseThreads + 20, Last: DenseThreads + 21, Clock: 5},
		{First: DenseThreads + 22, Last: DenseThreads + 23, Clock: 5},
	})
	if len(plain.sparseRuns) != 1 || plain.sparseRuns[0].First != DenseThreads+20 || plain.sparseRuns[0].Last != DenseThreads+23 {
		t.Fatalf("adjacent equal bulk input was not canonicalized: %+v", plain.sparseRuns)
	}

	vc := New()
	vc.RetireRange(DenseThreads+2, DenseThreads+3)
	ranges := []FiniteRange{
		{First: DenseThreads - 2, Last: DenseThreads + 1, Clock: 2},
		{First: DenseThreads + 2, Last: DenseThreads + 4, Clock: 2},
		{First: DenseThreads + 5, Last: DenseThreads + 7, Clock: 2}, // Adjacent equal input.
		{First: DenseThreads + 8, Last: DenseThreads + 9, Clock: 3}, // Adjacent unequal input.
		{First: ^uint32(0) - 2, Last: ^uint32(0), Clock: 4},
	}
	vc.JoinRanges(ranges)
	want := map[uint32]uint32{
		DenseThreads - 2: 2,
		DenseThreads + 1: 2,
		DenseThreads + 2: ^uint32(0),
		DenseThreads + 3: ^uint32(0),
		DenseThreads + 4: 2,
		DenseThreads + 7: 2,
		DenseThreads + 8: 3,
		^uint32(0):       4,
	}
	for tid, clock := range want {
		if got := vc.Get(tid); got != clock {
			t.Fatalf("JoinRanges clock[%d] = %d, want %d", tid, got, clock)
		}
	}
	for i := 1; i < len(vc.sparseRuns); i++ {
		previous, current := vc.sparseRuns[i-1], vc.sparseRuns[i]
		if previous.Last >= current.First || (previous.Last != ^uint32(0) && previous.Last+1 == current.First && previous.Clock == current.Clock) {
			t.Fatalf("JoinRanges produced non-canonical runs: %+v", vc.sparseRuns)
		}
	}
}

func TestVectorClockJoinCanonicalRangesMatchesChecked(t *testing.T) {
	source := New()
	source.JoinRange(1, 7, 2)
	source.Set(3, 9)
	source.JoinRange(DenseThreads-2, DenseThreads+3, 4)
	source.JoinRange(DenseThreads+10, DenseThreads+20, 5)
	source.Set(DenseThreads+15, 8)
	source.RetireRange(5, 5)
	source.RetireRange(DenseThreads+12, DenseThreads+13)

	var ranges []FiniteRange
	source.RangeRuns(func(first, last, clock uint32) bool {
		ranges = append(ranges, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	var retired []RetiredRange
	source.RangeRetired(func(first, last uint32) bool {
		retired = append(retired, RetiredRange{First: first, Last: last})
		return true
	})

	for _, tc := range []struct {
		name string
		seed func(*VectorClock)
	}{
		{
			name: "no receiver retirement",
			seed: func(vc *VectorClock) {
				vc.Set(0, 3)
				vc.JoinRange(DenseThreads+2, DenseThreads+16, 3)
			},
		},
		{
			name: "receiver retirement overlaps dense and sparse runs",
			seed: func(vc *VectorClock) {
				vc.Set(0, 3)
				vc.JoinRange(DenseThreads+2, DenseThreads+16, 3)
				vc.RetireRange(2, 4)
				vc.RetireRange(DenseThreads+1, DenseThreads+11)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checked := New()
			canonical := New()
			tc.seed(checked)
			tc.seed(canonical)

			checked.JoinRanges(ranges)
			checked.RetireRanges(retired)
			canonical.JoinCanonicalRanges(ranges)
			canonical.RetireRanges(retired)

			assertVectorClocksEqual(t, canonical, checked)
		})
	}
}

func assertVectorClocksEqual(t *testing.T, got, want *VectorClock) {
	t.Helper()
	assertVectorClockRepresentation(t, got)
	assertVectorClockRepresentation(t, want)
	if got.maxDense != want.maxDense || got.clocks != want.clocks {
		t.Fatal("dense vector-clock state differs")
	}
	if len(got.denseTail) != len(want.denseTail) {
		t.Fatalf("dense tail lengths differ: got %v, want %v", got.denseTail, want.denseTail)
	}
	for i := range want.denseTail {
		if got.denseTail[i] != want.denseTail[i] {
			t.Fatalf("dense tails differ: got %v, want %v", got.denseTail, want.denseTail)
		}
	}
	if len(got.sparseRuns) != len(want.sparseRuns) {
		t.Fatalf("sparse run counts differ: got %v, want %v", got.sparseRuns, want.sparseRuns)
	}
	for i := range want.sparseRuns {
		if got.sparseRuns[i] != want.sparseRuns[i] {
			t.Fatalf("sparse runs differ: got %v, want %v", got.sparseRuns, want.sparseRuns)
		}
	}
	if len(got.retired) != len(want.retired) {
		t.Fatalf("retired range counts differ: got %v, want %v", got.retired, want.retired)
	}
	for i := range want.retired {
		if got.retired[i] != want.retired[i] {
			t.Fatalf("retired ranges differ: got %v, want %v", got.retired, want.retired)
		}
	}
}

func TestVectorClockJoinRangesValidationFailsClosed(t *testing.T) {
	const envName = "KOLKOV_TEST_INVALID_JOIN_RANGES"
	if mode := os.Getenv(envName); mode != "" {
		vc := New()
		var ranges []FiniteRange
		switch mode {
		case "reversed":
			ranges = []FiniteRange{{First: 2, Last: 1, Clock: 1}}
		case "zero":
			ranges = []FiniteRange{{First: 1, Last: 2, Clock: 0}}
		case "overlap":
			ranges = []FiniteRange{
				{First: 1, Last: 3, Clock: 1},
				{First: 3, Last: 4, Clock: 2},
			}
		default:
			t.Fatalf("unknown invalid range mode %q", mode)
		}
		vc.JoinRanges(ranges)
		t.Fatal("JoinRanges returned for invalid input")
	}

	for _, mode := range []string{"reversed", "zero", "overlap"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestVectorClockJoinRangesValidationFailsClosed$")
		cmd.Env = append(os.Environ(), envName+"="+mode)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("%s invalid-range subprocess succeeded:\n%s", mode, out)
		}
		if want := "fatal error: race detector received invalid finite vector-clock ranges"; !strings.Contains(string(out), want) {
			t.Fatalf("%s invalid range did not fail closed:\n%s", mode, out)
		}
	}
}

func TestVectorClockRetiredIntervalsAlgebraAndIteration(t *testing.T) {
	finite := New()
	finite.Set(3, ^uint32(0))

	retired := New()
	retired.Set(2, 4)
	retired.Set(3, 5)
	retired.Set(4, 6)
	retired.RetireRange(2, 4)
	retired.RetireRange(8, 9)
	retired.RetireRange(7, 7) // Adjacent ranges coalesce deterministically.

	if got := retired.Get(3); got != ^uint32(0) || !retired.IsRetired(3) {
		t.Fatalf("retired Get(3) = %d, IsRetired=%v", got, retired.IsRetired(3))
	}
	if finite.IsRetired(3) {
		t.Fatal("a finite MaxUint32 component was mistaken for retirement")
	}
	if !finite.LessOrEqual(retired) {
		t.Fatal("retirement did not dominate a finite MaxUint32 component")
	}
	if retired.LessOrEqual(finite) {
		t.Fatal("finite MaxUint32 incorrectly dominated +infinity retirement")
	}

	var finiteIDs []uint32
	retired.Range(func(tid, _ uint32) bool {
		finiteIDs = append(finiteIDs, tid)
		return true
	})
	if len(finiteIDs) != 0 {
		t.Fatalf("Range exposed covered finite entries: %v", finiteIDs)
	}
	var ranges []RetiredRange
	retired.RangeRetired(func(first, last uint32) bool {
		ranges = append(ranges, RetiredRange{First: first, Last: last})
		return true
	})
	want := []RetiredRange{{First: 2, Last: 4}, {First: 7, Last: 9}}
	if len(ranges) != len(want) {
		t.Fatalf("retired ranges = %v, want %v", ranges, want)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("retired ranges = %v, want %v", ranges, want)
		}
	}

	joined := finite.Clone()
	defer joined.Release()
	joined.Join(retired)
	if !finite.LessOrEqual(joined) || !retired.LessOrEqual(joined) || !joined.IsRetired(3) {
		t.Fatal("Join did not preserve both finite values and retirement markers")
	}
}

func TestVectorClockRetiredCloneCopyResetAndPoolImmutability(t *testing.T) {
	original := New()
	original.Set(6, 11)
	original.RetireRange(10, 20)

	clone := original.Clone()
	defer clone.Release()
	clone.RetireRange(21, 30)
	clone.Set(12, 1)
	if original.IsRetired(25) || !original.IsRetired(12) || original.Get(12) != ^uint32(0) {
		t.Fatal("clone retirement metadata aliased or mutated its source")
	}

	dst := New()
	dst.RetireRange(100, 200)
	dst.CopyFrom(original)
	if dst.IsRetired(150) || !dst.IsRetired(15) || dst.Get(6) != 11 {
		t.Fatal("CopyFrom retained stale retirement or lost source metadata")
	}

	pooled := NewFromPool()
	pooled.RetireRange(1, ^uint32(0))
	pooled.Release()
	reused := NewFromPool()
	defer reused.Release()
	if reused.IsRetired(1) || reused.IsRetired(^uint32(0)) || reused.GetMaxTID() != 0 {
		t.Fatal("pooled VectorClock retained retirement metadata")
	}
}

func TestVectorClockFiniteRunsAndJoinRange(t *testing.T) {
	const last = uint32(70_000)
	vc := New()
	vc.JoinRange(1, last, 1)
	vc.Set(10, 2)
	vc.RetireRange(20, 30)

	type run struct{ first, last, clock uint32 }
	var runs []run
	vc.RangeRuns(func(first, last, clock uint32) bool {
		runs = append(runs, run{first, last, clock})
		return true
	})
	want := []run{
		{1, 9, 1}, {10, 10, 2}, {11, 19, 1}, {31, last, 1},
	}
	if len(runs) != len(want) {
		t.Fatalf("RangeRuns = %v, want %v", runs, want)
	}
	for i := range want {
		if runs[i] != want[i] {
			t.Fatalf("RangeRuns = %v, want %v", runs, want)
		}
	}

	joined := New()
	for _, r := range runs {
		joined.JoinRange(r.first, r.last, r.clock)
	}
	joined.RetireRange(20, 30)
	if !vc.LessOrEqual(joined) || !joined.LessOrEqual(vc) {
		t.Fatal("RangeRuns/JoinRange did not round-trip finite and retired state")
	}
}

func TestVectorClockDenseSparseBoundaryAndHighID(t *testing.T) {
	vc := New()
	values := []struct {
		tid, clock uint32
	}{
		{DenseThreads - 1, 11},
		{DenseThreads, 12},
		{65535, 13},
		{65536, 14},
		{1<<20 + 7, 15},
	}
	for _, value := range values {
		vc.Set(value.tid, value.clock)
	}
	for _, value := range values {
		if got := vc.Get(value.tid); got != value.clock {
			t.Fatalf("clock[%d] = %d, want %d", value.tid, got, value.clock)
		}
	}
	if got := vc.GetMaxTID(); got != 1<<20+7 {
		t.Fatalf("GetMaxTID() = %d, want %d", got, uint32(1<<20+7))
	}
}

func TestVectorClockSparseJoinAndOrder(t *testing.T) {
	left := New()
	left.Set(3, 9)
	left.Set(65536, 2)
	left.Set(1<<20+7, 10)
	right := New()
	right.Set(3, 8)
	right.Set(1024, 4)
	right.Set(65536, 7)
	right.Set(1<<24+9, 6)

	if left.LessOrEqual(right) || right.LessOrEqual(left) {
		t.Fatal("concurrent sparse clocks were ordered")
	}
	left.Join(right)
	wantIDs := []uint32{3, 1024, 65536, 1<<20 + 7, 1<<24 + 9}
	wantClocks := []uint32{9, 4, 7, 10, 6}
	var gotIDs, gotClocks []uint32
	left.Range(func(tid, clock uint32) bool {
		gotIDs = append(gotIDs, tid)
		gotClocks = append(gotClocks, clock)
		return true
	})
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("Range IDs = %v, want %v", gotIDs, wantIDs)
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] || gotClocks[i] != wantClocks[i] {
			t.Fatalf("Range = (%v,%v), want (%v,%v)", gotIDs, gotClocks, wantIDs, wantClocks)
		}
	}
	if !right.LessOrEqual(left) {
		t.Fatal("joined clock does not dominate sparse operand")
	}
}

func TestVectorClockSparseCloneCopyResetAndPoolReuse(t *testing.T) {
	original := New()
	original.Set(7, 1)
	original.Set(65536, 2)
	original.Set(1<<20+7, 3)
	clone := original.Clone()
	defer clone.Release()
	clone.Set(65536, 20)
	clone.Set(1<<24+9, 4)
	if original.Get(65536) != 2 || original.Get(1<<24+9) != 0 {
		t.Fatal("sparse clone aliases original")
	}

	dst := New()
	dst.Set(1<<30, 99)
	dst.CopyFrom(original)
	if dst.Get(1<<30) != 0 || dst.Get(65536) != 2 || dst.Get(1<<20+7) != 3 {
		t.Fatal("CopyFrom retained a stale sparse coordinate or lost source data")
	}

	pooled := NewFromPool()
	pooled.Set(1<<28, 5)
	oldCap := cap(pooled.sparseRuns)
	pooled.Reset()
	if pooled.Get(1<<28) != 0 || len(pooled.sparseRuns) != 0 || cap(pooled.sparseRuns) != oldCap {
		t.Fatal("Reset did not clear sparse values while retaining reusable storage")
	}
	ptr := pooled
	pooled.Release()
	reused := NewFromPool()
	defer reused.Release()
	if reused != ptr {
		t.Fatal("most recently released VectorClock was not reused")
	}
	if reused.Get(1<<28) != 0 || reused.GetMaxTID() != 0 {
		t.Fatal("pooled sparse VectorClock retained data across lifetimes")
	}
}

func TestVectorClockRangeStopsAndAllocatesNothing(t *testing.T) {
	vc := New()
	vc.Set(1, 1)
	vc.Set(65536, 2)
	vc.Set(1<<24, 3)
	visits := 0
	vc.Range(func(_, _ uint32) bool {
		visits++
		return false
	})
	if visits != 1 {
		t.Fatalf("Range visited %d entries after callback stopped it", visits)
	}
	visit := func(_, _ uint32) bool { return true }
	if allocs := testing.AllocsPerRun(1000, func() { vc.Range(visit) }); allocs != 0 {
		t.Fatalf("Range allocated %.2f objects per call", allocs)
	}
}

func TestVectorClockOverflowFailsClosed(t *testing.T) {
	mode := os.Getenv("KOLKOV_TEST_VECTORCLOCK_OVERFLOW")
	if mode != "" {
		vc := New()
		tid := uint32(7)
		if mode == "sparse" {
			tid = 65536
		}
		if mode == "retired" {
			vc.RetireRange(tid, tid)
			vc.Increment(tid)
			t.Fatal("Increment returned for a retired owner TID")
		}
		vc.Set(tid, ^uint32(0))
		vc.Increment(tid)
		t.Fatal("Increment returned after logical clock overflow")
	}
	for _, mode := range []string{"dense", "sparse", "retired"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestVectorClockOverflowFailsClosed$")
		cmd.Env = append(os.Environ(), "KOLKOV_TEST_VECTORCLOCK_OVERFLOW="+mode)
		out, err := cmd.CombinedOutput()
		if err == nil {
			t.Fatalf("%s overflow subprocess succeeded:\n%s", mode, out)
		}
		want := "fatal error: race detector logical clock overflow"
		if mode == "retired" {
			want = "fatal error: race detector incremented retired logical goroutine ID"
		}
		if !strings.Contains(string(out), want) {
			t.Fatalf("%s overflow did not fail closed:\n%s", mode, out)
		}
	}
}

// TestVectorClockNew tests zero initialization.
func TestVectorClockNew(t *testing.T) {
	vc := New()

	// Verify all clocks are zero (check a sample, not all 65536).
	for i := 0; i < 100; i++ {
		if vc.Get(uint32(i)) != 0 {
			t.Errorf("New() Get(%d) = %d, want 0", i, vc.Get(uint32(i)))
		}
	}

	// v0.3.0: maxTID should be 0 for new clock.
	if vc.GetMaxTID() != 0 {
		t.Errorf("New() GetMaxTID() = %d, want 0", vc.GetMaxTID())
	}
}

// TestVectorClockClone tests deep copy independence.
func TestVectorClockClone(t *testing.T) {
	// Create original with some values.
	original := New()
	original.Set(0, 10)
	original.Set(5, 20)
	original.Set(65535, 30)

	// Clone it.
	clone := original.Clone()

	// Verify clone has same values.
	if clone.Get(0) != 10 {
		t.Errorf("Clone().Get(0) = %d, want 10", clone.Get(0))
	}
	if clone.Get(5) != 20 {
		t.Errorf("Clone().Get(5) = %d, want 20", clone.Get(5))
	}
	if clone.Get(65535) != 30 {
		t.Errorf("Clone().Get(65535) = %d, want 30", clone.Get(65535))
	}

	// Modify clone.
	clone.Set(0, 999)
	clone.Set(5, 888)

	// Verify original is unchanged (deep copy).
	if original.Get(0) != 10 {
		t.Errorf("Original modified after clone change: Get(0) = %d, want 10", original.Get(0))
	}
	if original.Get(5) != 20 {
		t.Errorf("Original modified after clone change: Get(5) = %d, want 20", original.Get(5))
	}
}

// TestVectorClockJoinCommutativity tests vc1⊔vc2 == vc2⊔vc1.
func TestVectorClockJoinCommutativity(t *testing.T) {
	// Create two vector clocks with different values.
	vc1 := New()
	vc1.Set(0, 10)
	vc1.Set(1, 30)
	vc1.Set(2, 20)

	vc2 := New()
	vc2.Set(0, 5)
	vc2.Set(1, 40)
	vc2.Set(2, 15)

	// Clone for second test.
	vc1Copy := vc1.Clone()
	vc2Copy := vc2.Clone()

	// Test: vc1 ⊔ vc2.
	vc1.Join(vc2)

	// Test: vc2 ⊔ vc1 (reversed order).
	vc2Copy.Join(vc1Copy)

	// Results should be identical (commutativity).
	// v0.3.0: Use sparse-aware iteration up to maxTID.
	// Use uint32 loop counter to avoid uint32 overflow at maxTID=65535.
	limit := vc1.GetMaxTID()
	if vc2Copy.GetMaxTID() > limit {
		limit = vc2Copy.GetMaxTID()
	}
	for i := uint32(0); i <= limit; i++ {
		if vc1.Get(i) != vc2Copy.Get(i) {
			t.Errorf("Join not commutative at index %d: vc1⊔vc2[%d]=%d, vc2⊔vc1[%d]=%d",
				i, i, vc1.Get(i), i, vc2Copy.Get(i))
		}
	}

	// Verify expected maximums.
	expected := map[uint32]uint32{
		0: 10, // max(10, 5)
		1: 40, // max(30, 40)
		2: 20, // max(20, 15)
	}

	for tid, want := range expected {
		if vc1.Get(tid) != want {
			t.Errorf("Join result[%d] = %d, want %d", tid, vc1.Get(tid), want)
		}
	}
}

// TestVectorClockJoinIdempotent tests vc⊔vc == vc.
func TestVectorClockJoinIdempotent(t *testing.T) {
	vc := New()
	vc.Set(0, 10)
	vc.Set(1, 20)
	vc.Set(5, 30)

	// Clone to compare later.
	original := vc.Clone()

	// Join with itself.
	vc.Join(vc)

	// Should be unchanged.
	// v0.3.0: Use sparse-aware iteration up to maxTID.
	// Use uint32 loop counter to avoid uint32 overflow at maxTID=65535.
	for i := uint32(0); i <= vc.GetMaxTID(); i++ {
		if vc.Get(i) != original.Get(i) {
			t.Errorf("Join not idempotent at index %d: vc⊔vc[%d]=%d, original[%d]=%d",
				i, i, vc.Get(i), i, original.Get(i))
		}
	}
}

// TestVectorClockPartialOrder tests transitivity: vc1⊑vc2 and vc2⊑vc3 => vc1⊑vc3.
func TestVectorClockPartialOrder(t *testing.T) {
	// Create three vector clocks: vc1 ⊑ vc2 ⊑ vc3.
	vc1 := New()
	vc1.Set(0, 10)
	vc1.Set(1, 20)
	vc1.Set(2, 30)

	vc2 := New()
	vc2.Set(0, 15) // >= vc1[0]
	vc2.Set(1, 25) // >= vc1[1]
	vc2.Set(2, 35) // >= vc1[2]

	vc3 := New()
	vc3.Set(0, 20) // >= vc2[0]
	vc3.Set(1, 30) // >= vc2[1]
	vc3.Set(2, 40) // >= vc2[2]

	// Test vc1 ⊑ vc2.
	if !vc1.LessOrEqual(vc2) {
		t.Error("vc1 ⊑ vc2 should be true")
	}

	// Test vc2 ⊑ vc3.
	if !vc2.LessOrEqual(vc3) {
		t.Error("vc2 ⊑ vc3 should be true")
	}

	// Test transitivity: vc1 ⊑ vc3.
	if !vc1.LessOrEqual(vc3) {
		t.Error("Transitivity failed: vc1 ⊑ vc2 and vc2 ⊑ vc3, but vc1 ⊑ vc3 is false")
	}

	// Test reflexivity: vc1 ⊑ vc1.
	if !vc1.LessOrEqual(vc1) {
		t.Error("Reflexivity failed: vc1 ⊑ vc1 should be true")
	}

	// Test non-comparable clocks (concurrent).
	vc4 := New()
	vc4.Set(0, 5)  // < vc1[0]
	vc4.Set(1, 25) // > vc1[1]

	if vc4.LessOrEqual(vc1) {
		t.Error("vc4 ⊑ vc1 should be false (vc4[1] > vc1[1])")
	}
	if vc1.LessOrEqual(vc4) {
		t.Error("vc1 ⊑ vc4 should be false (vc1[0] > vc4[0])")
	}
}

// TestVectorClockGetSet tests Get/Set operations.
func TestVectorClockGetSet(t *testing.T) {
	vc := New()

	// Test setting and getting various thread IDs.
	tests := []struct {
		tid   uint32
		clock uint32
	}{
		{0, 100},
		{1, 200},
		{127, 300},
		{255, 400},
	}

	for _, tt := range tests {
		vc.Set(tt.tid, tt.clock)
		got := vc.Get(tt.tid)
		if got != tt.clock {
			t.Errorf("Set(%d, %d) then Get(%d) = %d, want %d",
				tt.tid, tt.clock, tt.tid, got, tt.clock)
		}
	}

	// Test that other threads remain 0.
	if vc.Get(5) != 0 {
		t.Errorf("Untouched thread Get(5) = %d, want 0", vc.Get(5))
	}
}

// TestVectorClockIncrement tests Increment operation.
func TestVectorClockIncrement(t *testing.T) {
	vc := New()

	// Increment thread 0 multiple times.
	for i := 1; i <= 10; i++ {
		vc.Increment(0)
		got := vc.Get(0)
		if got != uint32(i) {
			t.Errorf("After %d increments, Get(0) = %d, want %d", i, got, i)
		}
	}

	// Increment thread 5.
	vc.Increment(5)
	if vc.Get(5) != 1 {
		t.Errorf("Increment(5) then Get(5) = %d, want 1", vc.Get(5))
	}

	// Verify thread 0 is unchanged.
	if vc.Get(0) != 10 {
		t.Errorf("Thread 0 changed after incrementing thread 5: Get(0) = %d, want 10", vc.Get(0))
	}

	// Test increment from non-zero value.
	vc.Set(10, 99)
	vc.Increment(10)
	if vc.Get(10) != 100 {
		t.Errorf("Increment from 99: Get(10) = %d, want 100", vc.Get(10))
	}
}

func TestVectorClockIncrementSparseSingletonLocalCoalescing(t *testing.T) {
	const tid = DenseThreads + 100
	tests := []struct {
		name string
		runs []finiteRun
		tid  uint32
		want []finiteRun
	}{
		{
			name: "no neighbor",
			runs: []finiteRun{
				{First: tid - 3, Last: tid - 2, Clock: 6},
				{First: tid, Last: tid, Clock: 5},
				{First: tid + 2, Last: tid + 3, Clock: 6},
			},
			tid: tid,
			want: []finiteRun{
				{First: tid - 3, Last: tid - 2, Clock: 6},
				{First: tid, Last: tid, Clock: 6},
				{First: tid + 2, Last: tid + 3, Clock: 6},
			},
		},
		{
			name: "left neighbor",
			runs: []finiteRun{
				{First: tid - 4, Last: tid - 3, Clock: 9},
				{First: tid - 2, Last: tid - 1, Clock: 6},
				{First: tid, Last: tid, Clock: 5},
				{First: tid + 2, Last: tid + 2, Clock: 8},
			},
			tid: tid,
			want: []finiteRun{
				{First: tid - 4, Last: tid - 3, Clock: 9},
				{First: tid - 2, Last: tid, Clock: 6},
				{First: tid + 2, Last: tid + 2, Clock: 8},
			},
		},
		{
			name: "right neighbor",
			runs: []finiteRun{
				{First: tid - 2, Last: tid - 2, Clock: 8},
				{First: tid, Last: tid, Clock: 5},
				{First: tid + 1, Last: tid + 2, Clock: 6},
				{First: tid + 3, Last: tid + 3, Clock: 9},
			},
			tid: tid,
			want: []finiteRun{
				{First: tid - 2, Last: tid - 2, Clock: 8},
				{First: tid, Last: tid + 2, Clock: 6},
				{First: tid + 3, Last: tid + 3, Clock: 9},
			},
		},
		{
			name: "both neighbors",
			runs: []finiteRun{
				{First: tid - 3, Last: tid - 2, Clock: 9},
				{First: tid - 1, Last: tid - 1, Clock: 6},
				{First: tid, Last: tid, Clock: 5},
				{First: tid + 1, Last: tid + 2, Clock: 6},
				{First: tid + 3, Last: tid + 3, Clock: 9},
			},
			tid: tid,
			want: []finiteRun{
				{First: tid - 3, Last: tid - 2, Clock: 9},
				{First: tid - 1, Last: tid + 2, Clock: 6},
				{First: tid + 3, Last: tid + 3, Clock: 9},
			},
		},
		{
			name: "dense boundary",
			runs: []finiteRun{
				{First: DenseThreads, Last: DenseThreads, Clock: 5},
				{First: DenseThreads + 1, Last: DenseThreads + 2, Clock: 6},
			},
			tid: DenseThreads,
			want: []finiteRun{
				{First: DenseThreads, Last: DenseThreads + 2, Clock: 6},
			},
		},
		{
			name: "maximum TID boundary",
			runs: []finiteRun{
				{First: ^uint32(0) - 2, Last: ^uint32(0) - 1, Clock: 6},
				{First: ^uint32(0), Last: ^uint32(0), Clock: 5},
			},
			tid: ^uint32(0),
			want: []finiteRun{
				{First: ^uint32(0) - 2, Last: ^uint32(0), Clock: 6},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := New()
			vc.sparseRuns = append(vc.sparseRuns, tt.runs...)
			vc.Increment(tt.tid)
			if len(vc.sparseRuns) != len(tt.want) {
				t.Fatalf("runs = %+v, want %+v", vc.sparseRuns, tt.want)
			}
			for i := range tt.want {
				if vc.sparseRuns[i] != tt.want[i] {
					t.Fatalf("run %d = %+v, want %+v (all runs: %+v)", i, vc.sparseRuns[i], tt.want[i], vc.sparseRuns)
				}
				if got := vc.Get(tt.want[i].First); got != tt.want[i].Clock {
					t.Fatalf("Get(%d) = %d, want %d", tt.want[i].First, got, tt.want[i].Clock)
				}
				if got := vc.Get(tt.want[i].Last); got != tt.want[i].Clock {
					t.Fatalf("Get(%d) = %d, want %d", tt.want[i].Last, got, tt.want[i].Clock)
				}
			}
		})
	}
}

func TestVectorClockIncrementSparseSingletonLargeFrontier(t *testing.T) {
	const runCount = 32 * 1024
	vc := New()
	vc.sparseRuns = make([]finiteRun, runCount)
	for i := range vc.sparseRuns {
		tid := DenseThreads + uint32(i)
		vc.sparseRuns[i] = finiteRun{First: tid, Last: tid, Clock: uint32(i&1) + 1}
	}
	target := runCount / 2
	vc.sparseRuns[target].Clock = 100
	tid := vc.sparseRuns[target].First

	vc.Increment(tid)
	if len(vc.sparseRuns) != runCount || vc.sparseRuns[target].Clock != 101 {
		t.Fatalf("large-frontier increment changed layout: len=%d target=%+v", len(vc.sparseRuns), vc.sparseRuns[target])
	}
	for i, run := range vc.sparseRuns {
		wantTID := DenseThreads + uint32(i)
		wantClock := uint32(i&1) + 1
		if i == target {
			wantClock = 101
		}
		if run != (finiteRun{First: wantTID, Last: wantTID, Clock: wantClock}) {
			t.Fatalf("run %d = %+v, want [%d,%d]@%d", i, run, wantTID, wantTID, wantClock)
		}
	}
}

func TestVectorClockSetSparseSingletonLocalCoalescing(t *testing.T) {
	const tid = DenseThreads + 200
	tests := []struct {
		name  string
		runs  []finiteRun
		clock uint32
		want  []finiteRun
	}{
		{
			name: "higher jump without neighbor",
			runs: []finiteRun{
				{First: tid - 2, Last: tid - 2, Clock: 20},
				{First: tid, Last: tid, Clock: 3},
				{First: tid + 2, Last: tid + 2, Clock: 20},
			},
			clock: 20,
			want: []finiteRun{
				{First: tid - 2, Last: tid - 2, Clock: 20},
				{First: tid, Last: tid, Clock: 20},
				{First: tid + 2, Last: tid + 2, Clock: 20},
			},
		},
		{
			name: "higher jump merges left",
			runs: []finiteRun{
				{First: tid - 2, Last: tid - 1, Clock: 20},
				{First: tid, Last: tid, Clock: 3},
				{First: tid + 2, Last: tid + 2, Clock: 9},
			},
			clock: 20,
			want: []finiteRun{
				{First: tid - 2, Last: tid, Clock: 20},
				{First: tid + 2, Last: tid + 2, Clock: 9},
			},
		},
		{
			name: "higher jump merges right",
			runs: []finiteRun{
				{First: tid - 2, Last: tid - 2, Clock: 9},
				{First: tid, Last: tid, Clock: 3},
				{First: tid + 1, Last: tid + 2, Clock: 20},
			},
			clock: 20,
			want: []finiteRun{
				{First: tid - 2, Last: tid - 2, Clock: 9},
				{First: tid, Last: tid + 2, Clock: 20},
			},
		},
		{
			name: "higher jump merges both",
			runs: []finiteRun{
				{First: tid - 2, Last: tid - 1, Clock: 20},
				{First: tid, Last: tid, Clock: 3},
				{First: tid + 1, Last: tid + 2, Clock: 20},
			},
			clock: 20,
			want: []finiteRun{
				{First: tid - 2, Last: tid + 2, Clock: 20},
			},
		},
		{
			name: "lower value merges both",
			runs: []finiteRun{
				{First: tid - 2, Last: tid - 1, Clock: 3},
				{First: tid, Last: tid, Clock: 20},
				{First: tid + 1, Last: tid + 2, Clock: 3},
			},
			clock: 3,
			want: []finiteRun{
				{First: tid - 2, Last: tid + 2, Clock: 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := New()
			vc.sparseRuns = append(vc.sparseRuns, tt.runs...)
			vc.Set(tid, tt.clock)
			if len(vc.sparseRuns) != len(tt.want) {
				t.Fatalf("runs = %+v, want %+v", vc.sparseRuns, tt.want)
			}
			for i := range tt.want {
				if vc.sparseRuns[i] != tt.want[i] {
					t.Fatalf("run %d = %+v, want %+v (all runs: %+v)", i, vc.sparseRuns[i], tt.want[i], vc.sparseRuns)
				}
			}
			if got := vc.Get(tid); got != tt.clock {
				t.Fatalf("Get(%d) = %d, want %d", tid, got, tt.clock)
			}
		})
	}
}

// TestVectorClockString tests debug output.
func TestVectorClockString(t *testing.T) {
	tests := []struct {
		name string
		set  map[uint32]uint32
		want string
	}{
		{
			name: "empty",
			set:  map[uint32]uint32{},
			want: "{}",
		},
		{
			name: "single thread",
			set:  map[uint32]uint32{0: 42},
			want: "{0:42}",
		},
		{
			name: "multiple threads",
			set:  map[uint32]uint32{0: 10, 5: 20, 65535: 30},
			want: "{0:10, 5:20, 65535:30}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := New()
			for tid, clock := range tt.set {
				vc.Set(tid, clock)
			}
			got := vc.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestVectorClockJoinEdgeCases tests edge cases for Join.
func TestVectorClockJoinEdgeCases(t *testing.T) {
	t.Run("join with zero", func(t *testing.T) {
		vc1 := New()
		vc1.Set(0, 10)
		vc1.Set(1, 20)

		vc2 := New() // All zeros.

		vc1.Join(vc2)

		// vc1 should be unchanged (max with 0).
		if vc1.Get(0) != 10 || vc1.Get(1) != 20 {
			t.Errorf("Join with zero changed vc1: {0:%d, 1:%d}, want {0:10, 1:20}",
				vc1.Get(0), vc1.Get(1))
		}
	})

	t.Run("join zero with non-zero", func(t *testing.T) {
		vc1 := New() // All zeros.

		vc2 := New()
		vc2.Set(0, 10)
		vc2.Set(1, 20)

		vc1.Join(vc2)

		// vc1 should now equal vc2.
		if vc1.Get(0) != 10 || vc1.Get(1) != 20 {
			t.Errorf("Join zero with non-zero: {0:%d, 1:%d}, want {0:10, 1:20}",
				vc1.Get(0), vc1.Get(1))
		}
	})

	t.Run("join with max uint32", func(t *testing.T) {
		vc1 := New()
		vc1.Set(0, 100)

		vc2 := New()
		vc2.Set(0, 0xFFFFFFFF) // Max uint32.

		vc1.Join(vc2)

		if vc1.Get(0) != 0xFFFFFFFF {
			t.Errorf("Join with max uint32: Get(0) = %d, want %d", vc1.Get(0), 0xFFFFFFFF)
		}
	})
}

// TestVectorClockLessOrEqualEdgeCases tests edge cases for LessOrEqual.
func TestVectorClockLessOrEqualEdgeCases(t *testing.T) {
	t.Run("zero less or equal zero", func(t *testing.T) {
		vc1 := New()
		vc2 := New()

		if !vc1.LessOrEqual(vc2) {
			t.Error("Zero ⊑ Zero should be true")
		}
	})

	t.Run("zero less or equal non-zero", func(t *testing.T) {
		vc1 := New()
		vc2 := New()
		vc2.Set(0, 10)

		if !vc1.LessOrEqual(vc2) {
			t.Error("Zero ⊑ Non-Zero should be true")
		}
	})

	t.Run("non-zero not less or equal zero", func(t *testing.T) {
		vc1 := New()
		vc1.Set(0, 10)
		vc2 := New()

		if vc1.LessOrEqual(vc2) {
			t.Error("Non-Zero ⊑ Zero should be false")
		}
	})

	t.Run("equal clocks", func(t *testing.T) {
		vc1 := New()
		vc1.Set(0, 10)
		vc1.Set(1, 20)

		vc2 := New()
		vc2.Set(0, 10)
		vc2.Set(1, 20)

		if !vc1.LessOrEqual(vc2) {
			t.Error("Equal ⊑ Equal should be true")
		}
	})
}

// ========== BENCHMARKS ==========

// BenchmarkVectorClockJoin benchmarks the Join operation.
// Target: < 500ns/op, 0 allocs/op.
func BenchmarkVectorClockJoin(b *testing.B) {
	b.Run("Dense", func(b *testing.B) {
		vc1 := New()
		vc2 := New()

		// Set up some realistic values.
		for i := 0; i < 10; i++ {
			vc1.Set(uint32(i), uint32(i*10))
			vc2.Set(uint32(i), uint32(i*15))
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vc1.Join(vc2)
		}
	})
	b.Run("DominatedHighTIDAcquire", func(b *testing.B) {
		const first = DenseThreads + 10_000
		dst := New()
		dst.JoinRange(first, first+100_000, 7)
		release := New()
		release.JoinRange(first+100, first+90_000, 6)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst.Join(release)
		}
	})
	b.Run("FragmentedHighTIDAcquire", func(b *testing.B) {
		const (
			first = DenseThreads + 10_000
			runs  = 4096
		)
		ranges := make([]FiniteRange, runs)
		for i := range ranges {
			tid := first + uint32(i)
			ranges[i] = FiniteRange{First: tid, Last: tid, Clock: uint32(i&1) + 1}
		}
		dst := New()
		dst.JoinRanges(ranges)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst.JoinRanges(ranges)
		}
	})

	source := New()
	for tid := uint32(0); tid < DenseThreads; tid++ {
		source.Set(tid, tid&1+1)
	}
	ranges := make([]FiniteRange, 0, DenseThreads)
	source.RangeRuns(func(first, last, clock uint32) bool {
		ranges = append(ranges, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	b.Run("DenseRangeSnapshotChecked", func(b *testing.B) {
		dst := New()
		dst.JoinRanges(ranges)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst.JoinRanges(ranges)
		}
	})
	b.Run("DenseRangeSnapshotCanonical", func(b *testing.B) {
		dst := New()
		dst.JoinCanonicalRanges(ranges)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst.JoinCanonicalRanges(ranges)
		}
	})
}

// BenchmarkVectorClockLessOrEqual benchmarks the LessOrEqual operation.
// Target: < 300ns/op, 0 allocs/op.
func BenchmarkVectorClockLessOrEqual(b *testing.B) {
	vc1 := New()
	vc2 := New()

	// Set up partial order: vc1 ⊑ vc2.
	for i := 0; i < 10; i++ {
		vc1.Set(uint32(i), uint32(i*10))
		vc2.Set(uint32(i), uint32(i*20))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vc1.LessOrEqual(vc2)
	}
}

// BenchmarkVectorClockClone benchmarks the Clone operation.
// Target: < 200ns/op, 1 alloc/op (for the new VC pointer).
func BenchmarkVectorClockClone(b *testing.B) {
	vc := New()

	// Set up some values.
	for i := 0; i < 10; i++ {
		vc.Set(uint32(i), uint32(i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vc.Clone()
	}
}

// BenchmarkVectorClockIncrement benchmarks the Increment operation.
func BenchmarkVectorClockIncrement(b *testing.B) {
	vc := New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vc.Increment(0)
	}
}

func BenchmarkVectorClockIncrementSparseSingleton(b *testing.B) {
	for _, benchmark := range []struct {
		name string
		runs int
	}{
		{name: "32_runs", runs: 32},
		{name: "1024_runs", runs: 1024},
		{name: "32768_runs", runs: 32768},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			vc := New()
			vc.sparseRuns = make([]finiteRun, benchmark.runs)
			for i := range vc.sparseRuns {
				tid := DenseThreads + uint32(i)
				vc.sparseRuns[i] = finiteRun{First: tid, Last: tid, Clock: uint32(i&1) + 1}
			}
			target := benchmark.runs / 2
			vc.sparseRuns[target].Clock = 1 << 30
			tid := vc.sparseRuns[target].First

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vc.Increment(tid)
			}
		})
	}
}

func BenchmarkVectorClockSetSparseSingletonMonotonicJump(b *testing.B) {
	for _, benchmark := range []struct {
		name string
		runs int
	}{
		{name: "32_runs", runs: 32},
		{name: "1024_runs", runs: 1024},
		{name: "32768_runs", runs: 32768},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			vc := New()
			vc.sparseRuns = make([]finiteRun, benchmark.runs)
			for i := range vc.sparseRuns {
				tid := DenseThreads + uint32(i)
				vc.sparseRuns[i] = finiteRun{First: tid, Last: tid, Clock: uint32(i&1) + 1}
			}
			target := benchmark.runs / 2
			clock := uint32(100)
			vc.sparseRuns[target].Clock = clock
			tid := vc.sparseRuns[target].First

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				clock += 2
				vc.Set(tid, clock)
			}
		})
	}
}

// BenchmarkVectorClockGetSet benchmarks Get and Set operations.
func BenchmarkVectorClockGetSet(b *testing.B) {
	vc := New()

	b.Run("Get", func(b *testing.B) {
		vc.Set(0, 100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = vc.Get(0)
		}
	})

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			vc.Set(0, uint32(i))
		}
	})
}

// ========== POOLING TESTS ==========

// TestVectorClockPooling tests sync.Pool lifecycle.
func TestVectorClockPooling(t *testing.T) {
	t.Run("Release bounds retained peak metadata", func(t *testing.T) {
		oversized := New()
		oversized.sparseRuns = make([]finiteRun, 1, 10_000)
		oversized.retired = make([]RetiredRange, 1, 10_000)
		oversized.Reset()
		if cap(oversized.sparseRuns) != 10_000 || cap(oversized.retired) != 10_000 {
			t.Fatalf("Reset discarded reusable metadata capacities %d/%d", cap(oversized.sparseRuns), cap(oversized.retired))
		}
		oversized.sparseRuns = oversized.sparseRuns[:1]
		oversized.retired = oversized.retired[:1]
		oversized.Release()
		if cap(oversized.sparseRuns) != 0 || cap(oversized.retired) != 0 {
			t.Fatalf("Release retained oversized metadata capacities %d/%d", cap(oversized.sparseRuns), cap(oversized.retired))
		}

		small := New()
		small.sparseRuns = make([]finiteRun, 1, 8)
		small.retired = make([]RetiredRange, 1, 8)
		small.Release()
		if cap(small.sparseRuns) != 8 || cap(small.retired) != 8 {
			t.Fatalf("Release discarded useful small metadata capacities %d/%d", cap(small.sparseRuns), cap(small.retired))
		}
	})

	t.Run("NewFromPool returns clean VectorClock", func(t *testing.T) {
		vc := NewFromPool()
		defer vc.Release()

		// Verify all clocks are zero.
		for i := 0; i < 10; i++ {
			if vc.Get(uint32(i)) != 0 {
				t.Errorf("NewFromPool() Get(%d) = %d, want 0", i, vc.Get(uint32(i)))
			}
		}

		// Verify maxTID is 0.
		if vc.GetMaxTID() != 0 {
			t.Errorf("NewFromPool() GetMaxTID() = %d, want 0", vc.GetMaxTID())
		}
	})

	t.Run("Release returns to pool and reuses", func(t *testing.T) {
		// Get first VC from pool, set some values.
		vc1 := NewFromPool()
		vc1.Set(0, 100)
		vc1.Set(5, 200)
		vc1.Set(100, 300)

		// Release back to pool.
		vc1.Release()

		// Get second VC from pool - should be the same object but reset.
		vc2 := NewFromPool()
		defer vc2.Release()

		// Verify it's clean (Reset was called).
		if vc2.Get(0) != 0 {
			t.Errorf("Reused VC Get(0) = %d, want 0 (should be reset)", vc2.Get(0))
		}
		if vc2.Get(5) != 0 {
			t.Errorf("Reused VC Get(5) = %d, want 0 (should be reset)", vc2.Get(5))
		}
		if vc2.GetMaxTID() != 0 {
			t.Errorf("Reused VC GetMaxTID() = %d, want 0 (should be reset)", vc2.GetMaxTID())
		}
	})

	t.Run("Release on nil is safe", func(_ *testing.T) {
		var vc *VectorClock
		// Should not panic.
		vc.Release()
	})

}

// TestVectorClockReset tests Reset method.
//
//nolint:gocognit // Test function with multiple sub-tests
func TestVectorClockReset(t *testing.T) {
	t.Run("Reset clears all values", func(t *testing.T) {
		vc := New()
		vc.Set(0, 10)
		vc.Set(5, 20)
		vc.Set(100, 30)
		vc.Set(65535, 40)

		vc.Reset()

		// Verify all values are zero.
		if vc.Get(0) != 0 {
			t.Errorf("After Reset, Get(0) = %d, want 0", vc.Get(0))
		}
		if vc.Get(5) != 0 {
			t.Errorf("After Reset, Get(5) = %d, want 0", vc.Get(5))
		}
		if vc.Get(100) != 0 {
			t.Errorf("After Reset, Get(100) = %d, want 0", vc.Get(100))
		}
		if vc.Get(65535) != 0 {
			t.Errorf("After Reset, Get(65535) = %d, want 0", vc.Get(65535))
		}

		// Verify maxTID is reset to 0.
		if vc.GetMaxTID() != 0 {
			t.Errorf("After Reset, GetMaxTID() = %d, want 0", vc.GetMaxTID())
		}
	})

	t.Run("Reset is sparse-aware", func(t *testing.T) {
		vc := New()
		// Set only first 10 elements.
		for i := uint32(0); i < 10; i++ {
			vc.Set(i, uint32(i*10))
		}

		// maxTID should be 9.
		if vc.GetMaxTID() != 9 {
			t.Errorf("Before Reset, GetMaxTID() = %d, want 9", vc.GetMaxTID())
		}

		vc.Reset()

		// After reset, maxTID should be 0.
		if vc.GetMaxTID() != 0 {
			t.Errorf("After Reset, GetMaxTID() = %d, want 0", vc.GetMaxTID())
		}

		// All values should be 0.
		for i := uint32(0); i < 10; i++ {
			if vc.Get(i) != 0 {
				t.Errorf("After Reset, Get(%d) = %d, want 0", i, vc.Get(i))
			}
		}
	})
}

// ========== POOLING BENCHMARKS ==========

// BenchmarkVectorClockPooling benchmarks pool vs direct allocation.
func BenchmarkVectorClockPooling(b *testing.B) {
	b.Run("New", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			vc := New()
			vc.Set(0, 1)
			_ = vc.Get(0)
		}
	})

	b.Run("NewFromPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			vc := NewFromPool()
			vc.Set(0, 1)
			_ = vc.Get(0)
			vc.Release()
		}
	})

	b.Run("NewFromPool_NoRelease", func(b *testing.B) {
		// Simulate leak - no Release().
		// This should be same as New() performance-wise.
		for i := 0; i < b.N; i++ {
			vc := NewFromPool()
			vc.Set(0, 1)
			_ = vc.Get(0)
			// Intentionally not calling Release().
		}
	})
}

// BenchmarkVectorClockReset benchmarks Reset operation.
// Target: < 100ns for typical sparse clocks.
//
//nolint:gocognit // Benchmark function with multiple sub-benchmarks
func BenchmarkVectorClockReset(b *testing.B) {
	b.Run("Reset_Sparse10", func(b *testing.B) {
		vc := New()
		// Set 10 elements (typical case).
		for i := uint32(0); i < 10; i++ {
			vc.Set(i, uint32(i*10))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vc.Reset()
			// Re-set for next iteration.
			for j := uint32(0); j < 10; j++ {
				vc.Set(j, uint32(j*10))
			}
		}
	})

	b.Run("Reset_Sparse100", func(b *testing.B) {
		vc := New()
		// Set 100 elements.
		for i := uint32(0); i < 100; i++ {
			vc.Set(i, uint32(i*10))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vc.Reset()
			// Re-set for next iteration.
			for j := uint32(0); j < 100; j++ {
				vc.Set(j, uint32(j*10))
			}
		}
	})

	b.Run("Reset_Dense", func(b *testing.B) {
		vc := New()
		// Set all 65536 elements (worst case).
		for i := uint32(0); i < 65535; i++ {
			vc.Set(i, uint32(i))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vc.Reset()
			// Re-set for next iteration.
			for j := uint32(0); j < 65535; j++ {
				vc.Set(j, uint32(j))
			}
		}
	})
}
