package detector

import (
	"reflect"
	"testing"
	"unsafe"

	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
)

func readCacheEntryForTest(ctx *goroutine.RaceContext, addr uintptr) (uintptr, unsafe.Pointer, uint8) {
	index := (addr >> 3) & (goroutine.ReadCacheSlots - 1)
	return ctx.ReadCache[index], ctx.ReadCacheStates[index], ctx.ReadCacheWidths[index]
}

func TestSizedDenseCompactReadMaterializesOnSecondEpochWithoutSteadyAllocations(t *testing.T) {
	d := NewDetector()
	const (
		base = uintptr(0x81000)
		pc   = uintptr(0x1100)
	)

	// Seven distinct live history classes cross the bitmap-to-palette
	// threshold. Cache publication must leave all of them unmaterialized.
	var contexts [7]*goroutine.RaceContext
	for i := range contexts {
		contexts[i] = goroutine.Alloc(uint32(100 + i))
		addr := base + uintptr(i)*8
		d.OnReadSized(addr, 8, contexts[i], pc)
		if slot := d.rangeMemory.GetSlot(addr); slot != nil {
			t.Fatalf("dense setup address %#x materialized slot %p", addr, slot)
		}
	}

	addr := base + 6*8
	ctx := contexts[6]
	if allocs := testing.AllocsPerRun(1000, func() {
		ctx.IncrementClock()
		d.OnReadSized(addr, 8, ctx, pc)
	}); allocs != 0 {
		t.Fatalf("steady dense sized read allocated %.2f objects per call", allocs)
	}
	if slot := d.rangeMemory.GetSlot(addr); slot == nil {
		t.Fatal("repeated second-epoch dense read did not materialize its word")
	}
	gotAddr, gotState, gotWidth := readCacheEntryForTest(ctx, addr)
	oldState := d.ShadowGet(addr)
	if gotAddr != addr || oldState == nil || gotState != unsafe.Pointer(oldState) || gotWidth != 8 {
		t.Fatalf("dense cache entry = (%#x,%p,%d), want exact state %p", gotAddr, gotState, gotWidth, oldState)
	}

	// A fresh allocator lifetime keeps the permanent word slot but must publish
	// a distinct exact state generation before its cache entry is reusable.
	d.ClearShadowRange(addr, 8)
	ctx.IncrementClock()
	d.OnReadSized(addr, 8, ctx, pc)
	if slot := d.rangeMemory.GetSlot(addr); slot == nil {
		t.Fatal("clear/reuse lost permanent materialized word")
	}
	gotAddr, gotState, gotWidth = readCacheEntryForTest(ctx, addr)
	newState := d.ShadowGet(addr)
	if gotAddr != addr || newState == nil || newState == oldState || gotState != unsafe.Pointer(newState) || gotWidth != 8 {
		t.Fatalf("reused dense cache entry = (%#x,%p,%d), old/new state=%p/%p", gotAddr, gotState, gotWidth, oldState, newState)
	}
}

func TestSizedReadCachePublicationPreservesMaterializedGeneration(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(120)
	const (
		addr = uintptr(0x82000)
		pc   = uintptr(0x1200)
	)

	slot := d.rangeMemory.GetOrCreateSlot(addr)
	d.OnReadSized(addr, 8, ctx, pc)
	old := slot.State(uint8(addr & 7))
	gotAddr, gotState, gotWidth := readCacheEntryForTest(ctx, addr)
	if old == nil || gotAddr != addr || gotState != unsafe.Pointer(old) || gotWidth != 8 {
		t.Fatalf("materialized cache entry = (%#x,%p,%d), state=%p", gotAddr, gotState, gotWidth, old)
	}

	d.ClearShadowRange(addr, 8)
	if state := slot.State(uint8(addr & 7)); state != nil {
		t.Fatalf("clear retained materialized state %p", state)
	}
	// ClearShadowRange intentionally has no RaceContext argument. Until the
	// allocator clears the owning context cache, runtime pointer revalidation
	// must reject this retained old pointer against the nil lane mapping.
	_, retained, _ := readCacheEntryForTest(ctx, addr)
	if retained != unsafe.Pointer(old) {
		t.Fatalf("test setup lost retained old cache state: got %p, want %p", retained, old)
	}

	ctx.IncrementClock()
	d.OnReadSized(addr, 8, ctx, pc)
	fresh := slot.State(uint8(addr & 7))
	gotAddr, gotState, gotWidth = readCacheEntryForTest(ctx, addr)
	if fresh == nil || fresh == old || gotAddr != addr || gotState != unsafe.Pointer(fresh) || gotWidth != 8 {
		t.Fatalf("reused materialized cache entry = (%#x,%p,%d), fresh=%p old=%p", gotAddr, gotState, gotWidth, fresh, old)
	}
}

func TestReadRangeDoesNotSeedScalarCache(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(10)
	ctx.ReadCache = [goroutine.ReadCacheSlots]uintptr{0x1010, 0x2020, 0x3030, 0x4040}
	ctx.ReadCacheWidths = [goroutine.ReadCacheSlots]uint8{1, 1, 1, 1}
	want := ctx.ReadCache

	d.OnReadRange(0x8003, 19, ctx, 0x1111)

	if ctx.ReadCache != want {
		t.Fatalf("range read changed scalar cache: got %#v, want %#v", ctx.ReadCache, want)
	}
}

func TestRepeatedRangeAccessDoesNotAllocate(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(11)
	const (
		addr = uintptr(0x9003)
		size = uintptr(61)
	)

	// Materialize all covered shadow state before measuring steady-state
	// detector bookkeeping.
	d.OnReadRange(addr, size, ctx, 0x1120)
	if allocs := testing.AllocsPerRun(1000, func() {
		d.OnReadRange(addr, size, ctx, 0x1120)
	}); allocs != 0 {
		t.Fatalf("repeated range read allocated %.2f objects per call", allocs)
	}

	d.OnWriteRange(addr, size, ctx, 0x1121)
	if allocs := testing.AllocsPerRun(1000, func() {
		d.OnWriteRange(addr, size, ctx, 0x1121)
	}); allocs != 0 {
		t.Fatalf("repeated range write allocated %.2f objects per call", allocs)
	}
}

func TestSmallRangeUsesExactCompactMembership(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(111)
	const (
		base = uintptr(0x98000)
		addr = base + 5
		size = uintptr(32)
	)

	d.OnWriteRange(addr, size, ctx, 0x1130)
	shared := d.ShadowGet(addr)
	if shared == nil {
		t.Fatal("small range did not publish history")
	}
	for offset := uintptr(0); offset < size; offset++ {
		if got := d.ShadowGet(addr + offset); got != shared {
			t.Fatalf("offset %d state = %p, want shared %p", offset, got, shared)
		}
		if slot := d.rangeMemory.GetSlot(addr + offset); slot != nil {
			t.Fatalf("offset %d materialized slot %p", offset, slot)
		}
	}
	if d.ShadowGet(addr-1) != nil || d.ShadowGet(addr+size) != nil {
		t.Fatal("small compact range changed an exact neighbor")
	}
}

func TestSmallRangeWriteReadAndReadWriteMatchForcedReference(t *testing.T) {
	for _, readFirst := range []bool{false, true} {
		step := 0
		if readFirst {
			step = 1
		}
		optimized := NewDetector()
		reference := NewDetector()
		optimizedCtx := goroutine.Alloc(112)
		referenceCtx := goroutine.Alloc(112)
		const (
			base = uintptr(0x99003)
			size = uintptr(19)
		)

		if readFirst {
			optimized.OnReadRange(base, size, optimizedCtx, 0x1140)
			reference.OnReadRange(base, size, referenceCtx, 0x1140)
		} else {
			optimized.OnWriteRange(base, size, optimizedCtx, 0x1141)
			reference.OnWriteRange(base, size, referenceCtx, 0x1141)
		}
		// Force the reference through authoritative materialized slots before the
		// second transition while leaving optimized eligible for compact range.
		for offset := uintptr(0); offset < size; offset += 8 {
			reference.rangeMemory.GetOrCreateSlot(base + offset)
		}
		optimizedCtx.IncrementClock()
		referenceCtx.IncrementClock()
		if readFirst {
			optimized.OnWriteRange(base, size, optimizedCtx, 0x1142)
			reference.OnWriteRange(base, size, referenceCtx, 0x1142)
		} else {
			optimized.OnReadRange(base, size, optimizedCtx, 0x1143)
			reference.OnReadRange(base, size, referenceCtx, 0x1143)
		}
		for offset := uintptr(0); offset < size; offset++ {
			assertEquivalentOrdinaryState(t, step, offset, optimized.ShadowGet(base+offset), reference.ShadowGet(base+offset))
		}
	}
}

func TestSmallRangeCrossBlockKeepsEarlierCompactFragmentAndReportsLaterConflict(t *testing.T) {
	d := NewDetector()
	writer := goroutine.Alloc(113)
	reader := goroutine.Alloc(114)
	const (
		blockBase = uintptr(0x9a000)
		addr      = blockBase + 4092
		size      = uintptr(8)
		conflict  = blockBase + 4096 + 2
	)
	var report *RaceReport
	d.reportObserver = func(observed *RaceReport) { report = observed }
	d.OnWrite(conflict, writer, 0x1150)
	d.OnReadRange(addr, size, reader, 0x1151)

	if d.RacesDetected() != 1 || report == nil || report.Current.Addr != conflict {
		t.Fatalf("cross-block report = %v, races=%d, want conflict at %#x", report, d.RacesDetected(), conflict)
	}
	for offset := uintptr(0); offset < 4; offset++ {
		state := d.ShadowGet(addr + offset)
		if state == nil || state.GetReadEpoch() != reader.GetEpoch() {
			t.Fatalf("earlier compact fragment offset %d was not retained: %v", offset, state)
		}
		if slot := d.rangeMemory.GetSlot(addr + offset); slot != nil {
			t.Fatalf("earlier compact fragment offset %d materialized slot %p", offset, slot)
		}
	}
}

func TestSampledOutWriteRangeStillInvalidatesExactCacheEntries(t *testing.T) {
	d := NewDetectorWithOptions(DetectorOptions{SamplingEnabled: true, SampleRate: 2})
	ctx := goroutine.Alloc(11)
	ctx.ReadCache = [goroutine.ReadCacheSlots]uintptr{0x1000, 0x2004, 0x3008, 0x4000}
	ctx.ReadCacheWidths = [goroutine.ReadCacheSlots]uint8{1, 1, 1, 1}

	// The sampler's first decision at rate two is false. Cache invalidation is
	// nevertheless a mandatory write-side effect and precedes that decision.
	d.OnWriteRange(0x2000, 0x1010, ctx, 0x2222)

	want := [goroutine.ReadCacheSlots]uintptr{0x1000, 0, 0, 0x4000}
	if ctx.ReadCache != want {
		t.Fatalf("sampled range write cache = %#v, want %#v", ctx.ReadCache, want)
	}
	if state := d.ShadowGet(0x2000); state != nil {
		t.Fatalf("sampled-out range write created shadow state %p", state)
	}
}

func TestWrappingRangesHaveNoSideEffects(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(12)
	ctx.ReadCache = [goroutine.ReadCacheSlots]uintptr{1, 2, 3, 4}
	ctx.ReadCacheWidths = [goroutine.ReadCacheSlots]uint8{1, 1, 1, 1}
	want := ctx.ReadCache
	addr := ^uintptr(0) - 3

	d.OnReadRange(addr, 8, ctx, 0x3333)
	d.OnWriteRange(addr, 8, ctx, 0x4444)

	if ctx.ReadCache != want {
		t.Fatalf("wrapping range changed cache: got %#v, want %#v", ctx.ReadCache, want)
	}
	if state := d.ShadowGet(addr); state != nil {
		t.Fatalf("wrapping range created shadow state %p", state)
	}
}

func TestScalarAccessSplitsRangeEquivalenceGroup(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(13)
	const base = uintptr(0x9000)

	d.OnWriteRange(base, 8, ctx, 0x5000)
	shared := d.ShadowGet(base)
	for lane := uintptr(1); lane < 8; lane++ {
		if got := d.ShadowGet(base + lane); got != shared {
			t.Fatalf("initial range lane %d state = %p, want shared %p", lane, got, shared)
		}
	}

	oldEpoch := ctx.GetEpoch()
	ctx.IncrementClock()
	d.OnWrite(base+3, ctx, 0x5001)

	isolated := d.ShadowGet(base + 3)
	if isolated == shared {
		t.Fatal("scalar write did not isolate its exact lane")
	}
	if got := isolated.GetW(); got != ctx.GetEpoch() {
		t.Fatalf("isolated W = %v, want %v", got, ctx.GetEpoch())
	}
	for lane := uintptr(0); lane < 8; lane++ {
		if lane == 3 {
			continue
		}
		if got := d.ShadowGet(base + lane); got != shared {
			t.Fatalf("unaffected lane %d state = %p, want %p", lane, got, shared)
		}
		if got := d.ShadowGet(base + lane).GetW(); got != oldEpoch {
			t.Fatalf("unaffected lane %d W = %v, want %v", lane, got, oldEpoch)
		}
	}
}

func TestRangeCrossesSlotsWithoutMaterializingAdjacentLanes(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(14)
	const base = uintptr(0xa000)

	d.OnReadRange(base+6, 4, ctx, 0x6000)
	for lane := uintptr(0); lane < 16; lane++ {
		state := d.ShadowGet(base + lane)
		covered := lane >= 6 && lane < 10
		if covered != (state != nil) {
			t.Fatalf("lane %d materialized=%v, want %v", lane, state != nil, covered)
		}
	}
}

func TestRangePublishesEveryTransitionAndReportsOnce(t *testing.T) {
	d := NewDetector()
	first := goroutine.Alloc(15)
	second := goroutine.Alloc(16)
	const base = uintptr(0xb000)

	d.OnWriteRange(base, 16, first, 0x7000)
	d.OnWriteRange(base, 16, second, 0x7001)

	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("logical range reported %d races, want 1", got)
	}
	for lane := uintptr(0); lane < 16; lane++ {
		state := d.ShadowGet(base + lane)
		if state == nil {
			t.Fatalf("lane %d has nil state after complete transition", lane)
		}
		if state.GetW() != second.GetEpoch() {
			t.Fatalf("lane %d W = %v, want complete transition %v", lane, state.GetW(), second.GetEpoch())
		}
	}
}

func TestRangeTransitionsMatchPerLaneScalarReference(t *testing.T) {
	optimized := NewDetector()
	reference := NewDetector()
	optimizedCtx := goroutine.Alloc(17)
	referenceCtx := goroutine.Alloc(17)
	const base = uintptr(0xc000)
	const lanes = uintptr(40)
	seed := uint32(0x63f12a9b)

	for step := 0; step < 750; step++ {
		seed = seed*1664525 + 1013904223
		if seed&31 == 0 {
			optimizedCtx.IncrementClock()
			referenceCtx.IncrementClock()
			continue
		}
		start := uintptr((seed >> 8) % uint32(lanes))
		width := uintptr((seed>>24)%12 + 1)
		if width > lanes-start {
			width = lanes - start
		}
		pc := uintptr(0xd000 + step)
		if seed&1 == 0 {
			optimized.OnReadRange(base+start, width, optimizedCtx, pc)
			for lane := uintptr(0); lane < width; lane++ {
				reference.OnRead(base+start+lane, referenceCtx, pc)
			}
		} else {
			optimized.OnWriteRange(base+start, width, optimizedCtx, pc)
			for lane := uintptr(0); lane < width; lane++ {
				reference.OnWrite(base+start+lane, referenceCtx, pc)
			}
		}

		for lane := uintptr(0); lane < lanes; lane++ {
			assertEquivalentOrdinaryState(t, step, lane, optimized.ShadowGet(base+lane), reference.ShadowGet(base+lane))
		}
	}
}

func TestFullBlockTransitionsMatchPerLaneScalarReference(t *testing.T) {
	optimized := NewDetector()
	reference := NewDetector()
	optimizedCtx := goroutine.Alloc(21)
	referenceCtx := goroutine.Alloc(21)
	const (
		base = uintptr(0x400000)
		size = uintptr(4096)
	)

	optimized.OnWriteRange(base, size, optimizedCtx, 0xe000)
	for offset := uintptr(0); offset < size; offset++ {
		reference.OnWrite(base+offset, referenceCtx, 0xe000)
	}

	optimizedCtx.IncrementClock()
	referenceCtx.IncrementClock()
	optimized.OnRead(base+123, optimizedCtx, 0xe001)
	reference.OnRead(base+123, referenceCtx, 0xe001)

	optimizedCtx.IncrementClock()
	referenceCtx.IncrementClock()
	optimized.OnWriteRange(base, size, optimizedCtx, 0xe002)
	for offset := uintptr(0); offset < size; offset++ {
		reference.OnWrite(base+offset, referenceCtx, 0xe002)
	}

	for offset := uintptr(0); offset < size; offset++ {
		assertEquivalentOrdinaryState(t, 0, offset, optimized.ShadowGet(base+offset), reference.ShadowGet(base+offset))
	}
}

func TestFullBlockRangeReportsExactOverrideAddress(t *testing.T) {
	d := NewDetector()
	writer := goroutine.Alloc(22)
	reader := goroutine.Alloc(23)
	const (
		base   = uintptr(0x500000)
		offset = uintptr(123)
	)
	var report *RaceReport
	d.reportObserver = func(observed *RaceReport) { report = observed }

	d.OnWrite(base+offset, writer, 0xf000)
	d.OnReadRange(base, 4096, reader, 0xf001)

	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("full-block range reported %d races, want 1", got)
	}
	if report == nil || report.Current.Addr != base+offset {
		t.Fatalf("reported address = %v, want %#x", report, base+offset)
	}
}

func TestPartialClearDoesNotResurrectBlockDefault(t *testing.T) {
	d := NewDetector()
	writer := goroutine.Alloc(24)
	reader := goroutine.Alloc(25)
	const (
		base   = uintptr(0x600000)
		offset = uintptr(123)
	)

	d.OnWriteRange(base, 4096, writer, 0x10000)
	d.ClearShadowRange(base+offset, 1)
	if got := d.ShadowGet(base + offset); got != nil {
		t.Fatalf("cleared exact lane retained block default: %p", got)
	}
	if got := d.ShadowGet(base + offset + 1); got == nil || got.GetW() != writer.GetEpoch() {
		t.Fatalf("adjacent exact lane lost block default: %v", got)
	}

	d.OnRead(base+offset, reader, 0x10001)
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("recycled cleared lane reported %d races, want 0", got)
	}
	d.OnRead(base+offset+1, reader, 0x10002)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("adjacent retained lane reported %d races, want 1", got)
	}
}

func TestFullBlockRangeVisitsAtomicWordOverride(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(26)
	atomicWriter := goroutine.Alloc(27)
	plainReader := goroutine.Alloc(28)
	const (
		base   = uintptr(0x700000)
		offset = uintptr(120)
	)

	// Give every unmaterialized word the same ordinary read history. The
	// atomic operation must materialize only its word and keep the detector-
	// owned overlay off the coarse block default.
	d.OnReadRange(base, 4096, seed, 0x2000)
	atomicWriter.C.Set(seed.TID, 1)
	var token AtomicToken
	d.AtomicBegin(base+offset, 8, atomicWriter, false, &token)
	d.AtomicEnd(base+offset, 8, atomicWriter, &token, 0x2001, true)
	defaultState := d.ShadowGet(base)
	defaultState.LockAccess()
	gotAtomic := defaultState.GetAtomicState()
	defaultState.UnlockAccess()
	if got := gotAtomic; got != nil {
		t.Fatalf("block default acquired atomic overlay %p", got)
	}

	var report *RaceReport
	d.reportObserver = func(observed *RaceReport) { report = observed }
	d.OnReadRange(base, 4096, plainReader, 0x2002)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("full-block mixed access reported %d races, want 1", got)
	}
	if report == nil || report.Current.Addr != base+offset {
		t.Fatalf("mixed report address = %v, want %#x", report, base+offset)
	}
}

func assertEquivalentOrdinaryState(t *testing.T, step int, lane uintptr, got, want *shadowmem.VarState) {
	t.Helper()
	if got == nil || want == nil {
		if got != want {
			t.Fatalf("step %d lane %d nil mismatch: got %p want %p", step, lane, got, want)
		}
		return
	}
	if got.GetW() != want.GetW() ||
		got.GetExclusiveWriter() != want.GetExclusiveWriter() ||
		got.GetWriteCount() != want.GetWriteCount() ||
		got.GetWritePC() != want.GetWritePC() ||
		got.GetReadPC() != want.GetReadPC() ||
		got.GetReaderCount() != want.GetReaderCount() ||
		got.IsPromoted() != want.IsPromoted() ||
		got.GetReadEpoch() != want.GetReadEpoch() ||
		got.GetWriteStack() != want.GetWriteStack() ||
		got.GetReadStack() != want.GetReadStack() ||
		!reflect.DeepEqual(got.GetReadEpochs(), want.GetReadEpochs()) {
		t.Fatalf("step %d lane %d ordinary state mismatch:\n got  %s\n want %s", step, lane, got, want)
	}
	if got.GetAtomicState() != nil || want.GetAtomicState() != nil {
		t.Fatalf("step %d lane %d scalar/range state unexpectedly has atomic history", step, lane)
	}
}
