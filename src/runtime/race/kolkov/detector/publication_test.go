package detector

import (
	"testing"
	"unsafe"

	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/vectorclock"
)

func observeReports(d *Detector) *[]*RaceReport {
	reports := make([]*RaceReport, 0, 2)
	d.reportObserver = func(report *RaceReport) {
		reports = append(reports, report)
	}
	return &reports
}

func TestScalarConflictingWriteIsPublishedBeforeReport(t *testing.T) {
	d := NewDetector()
	reports := observeReports(d)
	first := goroutine.Alloc(101)
	second := goroutine.Alloc(102)
	third := goroutine.Alloc(103)
	const addr = uintptr(0x19100)

	d.OnWrite(addr, first, 0x9101)
	d.OnWrite(addr, second, 0x9102)
	if got := d.ShadowGet(addr).GetW(); got != second.GetEpoch() {
		t.Fatalf("conflicting write published W=%v, want %v", got, second.GetEpoch())
	}
	d.OnRead(addr, third, 0x9103)

	if got := d.RacesDetected(); got != 2 {
		t.Fatalf("three-context sequence reported %d races, want 2", got)
	}
	if got := (*reports)[1].Previous.Epoch; got != second.GetEpoch() {
		t.Fatalf("third access raced with epoch %v, want published second write %v", got, second.GetEpoch())
	}
}

func TestScalarConflictingReadIsPublishedBeforeReport(t *testing.T) {
	d := NewDetector()
	reports := observeReports(d)
	first := goroutine.Alloc(104)
	second := goroutine.Alloc(105)
	third := goroutine.Alloc(106)
	const addr = uintptr(0x19200)

	d.OnWrite(addr, first, 0x9201)
	d.OnRead(addr, second, 0x9202)
	// Order the third access after the first write, but not after the second
	// read, so only publication of the conflicting read can produce a race.
	third.C.Set(first.TID, first.C.Get(first.TID))
	d.OnWrite(addr, third, 0x9203)

	if got := d.RacesDetected(); got != 2 {
		t.Fatalf("three-context sequence reported %d races, want 2", got)
	}
	if got := (*reports)[1].Previous.Epoch; got != second.GetEpoch() {
		t.Fatalf("third access raced with epoch %v, want published second read %v", got, second.GetEpoch())
	}
}

func TestScalarReadNoopSeedsAddressOnlyCacheAfterPublication(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(114)
	const addr = uintptr(0x19240)

	// A changed compact read publishes history but deliberately does not cache
	// its unexposed state, preserving allocation-free reuse eligibility.
	d.OnRead(addr, reader, 0x9241)

	slot := goroutine.ReadCacheIndex(addr)
	if got := reader.ReadCache[slot]; got != 0 || reader.ReadCacheStates[slot] != nil {
		t.Fatalf("changed read cache = (%#x,%p), want empty", got, reader.ReadCacheStates[slot])
	}
	state := d.ShadowGet(addr)
	if state == nil {
		t.Fatal("completed read did not publish shadow state")
	}
	if got := state.GetReadEpoch(); got != reader.GetEpoch() {
		t.Fatalf("completed read published epoch %v, want %v", got, reader.GetEpoch())
	}

	// The exact second transition is a semantic no-op. Only this path seeds the
	// address-only entry; nil forces heap/stack runtime probes to miss unless
	// their independent lifetime-safe address rule applies.
	d.OnRead(addr, reader, 0x9241)
	if reader.ReadCache[slot] != addr || reader.ReadCacheStates[slot] != nil {
		t.Fatalf("read no-op cache = (%#x,%p), want (%#x,nil)",
			reader.ReadCache[slot], reader.ReadCacheStates[slot], addr)
	}
	if materialized := d.rangeMemory.GetSlot(addr); materialized != nil {
		t.Fatalf("cacheable compact read materialized slot %p", materialized)
	}
}

func TestSampledCompactReadSeedsCacheOnlyWhenRepresented(t *testing.T) {
	d := NewDetectorWithOptions(DetectorOptions{SamplingEnabled: true, SampleRate: 2})
	reader := goroutine.Alloc(121)
	const addr = uintptr(0x19260)
	cacheSlot := goroutine.ReadCacheIndex(addr)
	const prior = uintptr(0x7777)
	priorState := unsafe.Pointer(reader)
	reader.ReadCache[cacheSlot] = prior
	reader.ReadCacheStates[cacheSlot] = priorState
	reader.ReadCacheWidths[cacheSlot] = 1

	// The first rate-two decision is skipped. It must neither create shadow
	// history nor claim that the skipped read was represented.
	d.OnRead(addr, reader, 0x9261)
	if got := d.ShadowGet(addr); got != nil {
		t.Fatalf("skipped read published shadow state %p", got)
	}
	if reader.ReadCache[cacheSlot] != prior || reader.ReadCacheStates[cacheSlot] != priorState {
		t.Fatalf("skipped read changed cache to (%#x,%p), want (%#x,%p)",
			reader.ReadCache[cacheSlot], reader.ReadCacheStates[cacheSlot], prior, priorState)
	}

	// The second decision is sampled and publishes a changed compact history,
	// but must preserve the colliding entry rather than expose the new state.
	d.OnRead(addr, reader, 0x9261)
	state := d.ShadowGet(addr)
	if state == nil {
		t.Fatal("sampled compact read did not publish shadow history")
	}
	if reader.ReadCache[cacheSlot] != prior || reader.ReadCacheStates[cacheSlot] != priorState {
		t.Fatalf("changed sampled read cache = (%#x,%p), want prior (%#x,%p)",
			reader.ReadCache[cacheSlot], reader.ReadCacheStates[cacheSlot], prior, priorState)
	}
	if slot := d.rangeMemory.GetSlot(addr); slot != nil {
		t.Fatalf("sampled compact read materialized slot %p", slot)
	}

	// The third decision is skipped and the fourth samples the exact no-op.
	// Only then may the address-only cache replace the colliding entry.
	d.OnRead(addr, reader, 0x9261)
	if reader.ReadCache[cacheSlot] != prior || reader.ReadCacheStates[cacheSlot] != priorState {
		t.Fatal("skipped repeat changed the colliding cache entry")
	}
	d.OnRead(addr, reader, 0x9261)
	if reader.ReadCache[cacheSlot] != addr || reader.ReadCacheStates[cacheSlot] != nil {
		t.Fatalf("sampled no-op cache = (%#x,%p), want (%#x,nil)",
			reader.ReadCache[cacheSlot], reader.ReadCacheStates[cacheSlot], addr)
	}
}

func TestConflictingReadSeedsCacheBeforeReport(t *testing.T) {
	d := NewDetector()
	writer := goroutine.Alloc(115)
	reader := goroutine.Alloc(116)
	const addr = uintptr(0x19280)

	d.OnWrite(addr, writer, 0x9281)
	var observed bool
	d.reportObserver = func(*RaceReport) {
		observed = true
		slot := goroutine.ReadCacheIndex(addr)
		if got := reader.ReadCache[slot]; got != addr {
			t.Errorf("report observed cache address %#x, want published read %#x", got, addr)
		}
		state := d.ShadowGet(addr)
		if state == nil {
			t.Error("report ran before conflicting read published shadow state")
			return
		}
		if got := reader.ReadCacheStates[slot]; got != unsafe.Pointer(state) {
			t.Errorf("report observed cache state %p, want authoritative state %p", got, state)
		}
		if got := state.GetReadEpoch(); got != reader.GetEpoch() {
			t.Errorf("report observed read epoch %v, want %v", got, reader.GetEpoch())
		}
	}

	d.OnRead(addr, reader, 0x9282)

	if !observed {
		t.Fatal("conflicting read did not report")
	}
}

func TestPromotedReadSeedsCacheAfterPublication(t *testing.T) {
	d := NewDetector()
	first := goroutine.Alloc(117)
	second := goroutine.Alloc(118)
	const addr = uintptr(0x192c0)

	d.OnRead(addr, first, 0x92c1)
	d.OnRead(addr, second, 0x92c2)

	slot := goroutine.ReadCacheIndex(addr)
	if got := second.ReadCache[slot]; got != addr {
		t.Fatalf("promoting read cached address %#x, want %#x", got, addr)
	}
	state := d.ShadowGet(addr)
	if state == nil || !state.IsPromoted() {
		t.Fatal("concurrent readers did not publish promoted state")
	}
	if got := second.ReadCacheStates[slot]; got != unsafe.Pointer(state) {
		t.Fatalf("promoting read cached state %p, want authoritative state %p", got, state)
	}
	if got := state.GetReadClock().Get(second.TID); got != second.C.Get(second.TID) {
		t.Fatalf("promoted state contains second reader clock %d, want %d", got, second.C.Get(second.TID))
	}
}

func TestScalarReadCacheGenerationChangesAcrossClear(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(119)
	const addr = uintptr(0x192e0)

	// A changed compact read remains uncached. Its exact no-op seeds only an
	// address entry, never a reusable shadow pointer.
	d.OnRead(addr, reader, 0x92e1)
	slot := goroutine.ReadCacheIndex(addr)
	oldState := d.ShadowGet(addr)
	if oldState == nil || reader.ReadCache[slot] != 0 || reader.ReadCacheStates[slot] != nil {
		t.Fatalf("initial changed read state/cache = (%p,%#x,%p), want state and empty cache",
			oldState, reader.ReadCache[slot], reader.ReadCacheStates[slot])
	}
	d.OnRead(addr, reader, 0x92e1)
	if reader.ReadCache[slot] != addr || reader.ReadCacheStates[slot] != nil {
		t.Fatalf("initial no-op cache = (%#x,%p), want (%#x,nil)",
			reader.ReadCache[slot], reader.ReadCacheStates[slot], addr)
	}

	d.ClearShadowRange(addr, 1)
	if got := reader.ReadCache[slot]; got != addr {
		t.Fatalf("direct clear unexpectedly rewrote context-owned address cache: got %#x", got)
	}
	if got := d.ShadowGet(addr); got != nil {
		t.Fatalf("clear retained authoritative state %p", got)
	}
	// The stale address-only entry carries nil state, so heap/stack runtime
	// identity revalidation necessarily misses after allocator reuse.
	if reader.ReadCacheStates[slot] != nil {
		t.Fatal("clear turned address-only cache into a rooted shadow generation")
	}

	d.OnRead(addr, reader, 0x92e2)
	newState := d.ShadowGet(addr)
	if newState == nil || newState == oldState {
		t.Fatalf("reused address state = %p, old generation = %p", newState, oldState)
	}
	if reader.ReadCache[slot] != addr || reader.ReadCacheStates[slot] != nil {
		t.Fatalf("changed reused read altered stale address-only cache: (%#x,%p)",
			reader.ReadCache[slot], reader.ReadCacheStates[slot])
	}
	reader.ClearReadCache()
	d.OnRead(addr, reader, 0x92e2)
	if reader.ReadCache[slot] != addr || reader.ReadCacheStates[slot] != nil {
		t.Fatalf("reused read no-op cache = (%#x,%p), want (%#x,nil)",
			reader.ReadCache[slot], reader.ReadCacheStates[slot], addr)
	}
}

func TestScalarReadCacheGenerationSurvivesAdjacentClear(t *testing.T) {
	d := NewDetector()
	reader := goroutine.Alloc(120)
	const addr = uintptr(0x192f0)

	// The changed compact read stays uncached; its exact no-op seeds an
	// address-only entry which an adjacent clear must preserve.
	d.OnRead(addr, reader, 0x92f1)
	slot := goroutine.ReadCacheIndex(addr)
	state := d.ShadowGet(addr)
	if reader.ReadCache[slot] != 0 || reader.ReadCacheStates[slot] != nil {
		t.Fatal("changed read unexpectedly seeded cache")
	}
	d.OnRead(addr, reader, 0x92f1)
	if reader.ReadCache[slot] != addr || reader.ReadCacheStates[slot] != nil {
		t.Fatalf("read no-op cache = (%#x,%p), want (%#x,nil)",
			reader.ReadCache[slot], reader.ReadCacheStates[slot], addr)
	}
	d.ClearShadowRange(addr+1, 1)
	if got := d.ShadowGet(addr); got != state {
		t.Fatalf("adjacent clear changed cached lane state: got %p, want %p", got, state)
	}
	if reader.ReadCache[slot] != addr || reader.ReadCacheStates[slot] != nil {
		t.Fatalf("adjacent clear changed address-only cache to (%#x,%p)",
			reader.ReadCache[slot], reader.ReadCacheStates[slot])
	}
}

func TestScalarPromotedReaderReportUsesConflictingEpoch(t *testing.T) {
	d := NewDetector()
	reports := observeReports(d)
	first := goroutine.Alloc(107)
	second := goroutine.Alloc(108)
	writer := goroutine.Alloc(109)
	const addr = uintptr(0x19300)

	d.OnRead(addr, first, 0x9301)
	d.OnRead(addr, second, 0x9302)
	if !d.ShadowGet(addr).IsPromoted() {
		t.Fatal("concurrent readers did not promote read tracking")
	}
	d.OnWrite(addr, writer, 0x9303)

	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("promoted-reader write reported %d races, want 1", got)
	}
	if got := (*reports)[0].Previous.Epoch; got != first.GetEpoch() {
		t.Fatalf("promoted-reader report previous epoch=%v, want %v", got, first.GetEpoch())
	}
}

func TestPromotedReadersDoNotInheritCausallyObservedNonReaders(t *testing.T) {
	d := NewDetector()
	first := goroutine.Alloc(110)
	observedNonReader := goroutine.Alloc(111)
	second := goroutine.Alloc(112)
	writer := goroutine.Alloc(113)
	const addr = uintptr(0x19400)

	// The second reader has observed this context, but that observation is not
	// itself a read event at addr.
	second.C.Set(observedNonReader.TID, observedNonReader.C.Get(observedNonReader.TID))
	d.OnRead(addr, first, 0x9401)
	d.OnRead(addr, second, 0x9402)

	state := d.ShadowGet(addr)
	if state == nil || !state.IsPromoted() {
		t.Fatal("concurrent reads did not promote read tracking")
	}
	if got := state.GetReadClock().Get(observedNonReader.TID); got != 0 {
		t.Fatalf("promoted history contains non-reader %d at clock %d", observedNonReader.TID, got)
	}

	// Observe both actual read events, but deliberately not the non-reader.
	// A promoted history polluted with second.C would report a phantom race.
	writer.C.Set(first.TID, first.C.Get(first.TID))
	writer.C.Set(second.TID, second.C.Get(second.TID))
	d.OnWrite(addr, writer, 0x9403)
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("write ordered after every actual reader reported %d phantom races", got)
	}
}

func TestPromotedReadFrontierStaysBoundedAcrossHappensBeforeChain(t *testing.T) {
	d := NewDetector()
	first := goroutine.Alloc(120)
	second := goroutine.Alloc(121)
	const addr = uintptr(0x19500)

	d.OnRead(addr, first, 0x9501)
	d.OnRead(addr, second, 0x9502)
	state := d.ShadowGet(addr)
	if state == nil || !state.IsPromoted() {
		t.Fatal("concurrent reads did not promote read tracking")
	}

	observed := vectorclock.New()
	observed.Set(first.TID, first.C.Get(first.TID))
	observed.Set(second.TID, second.C.Get(second.TID))
	for tid := uint32(122); tid < 378; tid++ {
		reader := goroutine.Alloc(tid)
		reader.C.Join(observed)
		d.OnRead(addr, reader, uintptr(0x9500+tid))
		observed.Set(reader.TID, reader.C.Get(reader.TID))

		count := 0
		state.GetReadClock().Range(func(_, _ uint32) bool {
			count++
			return true
		})
		if count != 1 {
			t.Fatalf("reader %d left promoted frontier size %d, want 1", tid, count)
		}
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("happens-before read chain reported %d races", got)
	}
}
