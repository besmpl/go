// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package detector

import (
	"reflect"
	"testing"
	"unsafe"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
	"runtime/race/kolkov/vectorclock"
)

type ordinaryContextSnapshot struct {
	runs        []vectorclock.FiniteRange
	retired     []vectorclock.RetiredRange
	epoch       epoch.Epoch
	foreign     uint64
	cache       [goroutine.ReadCacheSlots]uintptr
	cacheWidths [goroutine.ReadCacheSlots]uint8
	cacheStates [goroutine.ReadCacheSlots]bool
	invalidated uint32
}

type ordinaryVarSnapshot struct {
	present         bool
	write           epoch.Epoch
	reads           []epoch.Epoch
	promoted        bool
	readClock       []vectorclock.FiniteRange
	exclusiveWriter int64
	writeCount      uint32
	writePC         uintptr
	readPC          uintptr
	writeStack      uint64
	readStack       uint64
	atomic          bool
}

type ordinaryDetectorSnapshot struct {
	contexts [2]ordinaryContextSnapshot
	state    ordinaryVarSnapshot
	races    int
}

func snapshotOrdinaryContext(ctx *goroutine.RaceContext) ordinaryContextSnapshot {
	if ctx == nil {
		return ordinaryContextSnapshot{}
	}
	runs, retired := snapshotContext(ctx)
	var states [goroutine.ReadCacheSlots]bool
	for i := range states {
		states[i] = ctx.ReadCacheStates[i] != nil
	}
	return ordinaryContextSnapshot{
		runs: runs, retired: retired, epoch: ctx.GetEpoch(), foreign: ctx.ForeignGeneration,
		cache: ctx.ReadCache, cacheWidths: ctx.ReadCacheWidths, cacheStates: states,
		invalidated: ctx.ReadCacheInvalidatedClock.Load(),
	}
}

func snapshotOrdinaryDetector(d *Detector, addr uintptr, contexts [2]*goroutine.RaceContext) ordinaryDetectorSnapshot {
	snapshot := ordinaryDetectorSnapshot{races: d.RacesDetected()}
	for i := range contexts {
		snapshot.contexts[i] = snapshotOrdinaryContext(contexts[i])
	}
	state := d.ShadowGet(addr)
	if state == nil {
		return snapshot
	}
	snapshot.state = ordinaryVarSnapshot{
		present:         true,
		write:           state.GetW(),
		reads:           state.GetReadEpochs(),
		promoted:        state.IsPromoted(),
		exclusiveWriter: state.GetExclusiveWriter(),
		writeCount:      state.GetWriteCount(),
		writePC:         state.GetWritePC(),
		readPC:          state.GetReadPC(),
		writeStack:      state.GetWriteStack(),
		readStack:       state.GetReadStack(),
		atomic:          state.GetAtomicState() != nil,
	}
	if clock := state.GetReadClock(); clock != nil {
		clock.RangeRuns(func(first, last, value uint32) bool {
			snapshot.state.readClock = append(snapshot.state.readClock, vectorclock.FiniteRange{First: first, Last: last, Clock: value})
			return true
		})
	}
	return snapshot
}

func runOrdinaryFastRead(d *Detector, addr, size, pc uintptr, ctx *goroutine.RaceContext) bool {
	result, state := d.TryOrdinaryRead(addr, size, ctx, pc)
	if result == shadowmem.OrdinaryFastMiss {
		d.OnReadSized(addr, size, ctx, pc)
		return false
	}
	if result == shadowmem.OrdinaryFastHandledCacheable {
		ctx.RecordReadSized(addr, size, unsafe.Pointer(state))
	}
	return true
}

func runOrdinaryFastWrite(d *Detector, addr, size, pc uintptr, ctx *goroutine.RaceContext) bool {
	ctx.InvalidateReadRange(addr, size)
	if d.TryOrdinaryWrite(addr, size, ctx, pc) {
		return true
	}
	d.OnWriteSized(addr, size, ctx, pc)
	return false
}

func materializeOrdinaryScalar(d *Detector, addr, size uintptr) {
	mask := uint8(((uint16(1) << size) - 1) << (addr & 7))
	d.rangeMemory.GetOrCreateSlot(addr).AccessGroups(mask, func(_ uint8, _ *shadowmem.VarState) {})
}

func TestPromotedReaderCapabilitySurvivesClockAdvance(t *testing.T) {
	const (
		addr = uintptr(0x60b00)
		size = uintptr(8)
	)
	d := NewDetector()
	materializeOrdinaryScalar(d, addr, size)
	first := goroutine.Alloc(501)
	second := goroutine.Alloc(502)
	d.OnReadSized(addr, size, first, 0x5010)
	d.OnReadSized(addr, size, second, 0x5020)
	state := d.ShadowGet(addr)
	if state == nil || !state.IsPromoted() {
		t.Fatal("concurrent readers did not promote exact state")
	}
	if second.LookupPromotedReadCapability(addr, size) == nil {
		t.Fatal("canonical promoted read did not cache a capability")
	}
	second.IncrementClock()
	want := second.GetEpoch()
	result, gotState := d.TryOrdinaryRead(addr, size, second, 0x5021)
	if result != shadowmem.OrdinaryFastHandledCacheable || gotState != state {
		t.Fatalf("warmed result = (%v, %p), want (%v, %p)", result, gotState, shadowmem.OrdinaryFastHandledCacheable, state)
	}
	seen := vectorclock.New()
	seen.Set(first.TID, first.C.Get(first.TID))
	if got, conflict := state.FirstConcurrentRead(seen); !conflict || got != want {
		t.Fatalf("promoted frontier = (%v, %v), want (%v, true)", got, conflict, want)
	}
}

func TestPromotedReaderCapabilityRemainsVisibleToWriter(t *testing.T) {
	const (
		addr = uintptr(0x60c00)
		size = uintptr(8)
	)
	d := NewDetector()
	materializeOrdinaryScalar(d, addr, size)
	first := goroutine.Alloc(511)
	reader := goroutine.Alloc(512)
	d.OnReadSized(addr, size, first, 0x5110)
	d.OnReadSized(addr, size, reader, 0x5120)
	reader.IncrementClock()
	if result, _ := d.TryOrdinaryRead(addr, size, reader, 0x5121); result != shadowmem.OrdinaryFastHandledCacheable {
		t.Fatalf("warmed read result = %v", result)
	}
	writer := goroutine.Alloc(513)
	d.OnWriteSized(addr, size, writer, 0x5130)
	if got := d.RacesDetected(); got == 0 {
		t.Fatal("writer missed lock-free promoted-reader witness")
	}
}

func BenchmarkPromotedReaderDetectorWarm(b *testing.B) {
	const (
		addr = uintptr(0x60d00)
		size = uintptr(8)
	)
	d := NewDetector()
	materializeOrdinaryScalar(d, addr, size)
	first := goroutine.Alloc(521)
	reader := goroutine.Alloc(522)
	d.OnReadSized(addr, size, first, 0x5210)
	d.OnReadSized(addr, size, reader, 0x5220)
	reader.IncrementClock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if result, _ := d.TryOrdinaryRead(addr, size, reader, 0x5221); result != shadowmem.OrdinaryFastHandledCacheable {
			b.Fatal("warmed promoted read missed")
		}
	}
}

func TestMaterializeOrdinaryScalarPreservesCompactHistory(t *testing.T) {
	const (
		addr = uintptr(0x60a00)
		size = uintptr(8)
	)
	d := NewDetector()
	ctx := goroutine.Alloc(400)
	d.OnWriteSized(addr, size, ctx, 0x60a1)
	if slot := d.rangeMemory.GetSlot(addr); slot != nil {
		t.Fatalf("compact setup unexpectedly materialized slot %p", slot)
	}
	want := snapshotOrdinaryDetector(d, addr, [2]*goroutine.RaceContext{ctx})
	if !d.MaterializeOrdinaryScalar(addr, size) {
		t.Fatal("word-local scalar materialization failed")
	}
	if slot := d.rangeMemory.GetSlot(addr); slot == nil {
		t.Fatal("materialization did not publish a permanent slot")
	}
	got := snapshotOrdinaryDetector(d, addr, [2]*goroutine.RaceContext{ctx})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("materialization changed detector history:\ngot:  %#v\nwant: %#v", got, want)
	}
	if d.MaterializeOrdinaryScalar(addr+7, 2) {
		t.Fatal("cross-word scalar materialization unexpectedly succeeded")
	}
}

func TestOrdinaryFastPathMatchesForcedCanonicalEventByEvent(t *testing.T) {
	const (
		addr = uintptr(0x61000)
		size = uintptr(8)
	)
	canonical := NewDetector()
	optimized := NewDetector()
	canonicalCtx := [2]*goroutine.RaceContext{goroutine.Alloc(401), goroutine.Alloc(402)}
	optimizedCtx := [2]*goroutine.RaceContext{goroutine.Alloc(401), goroutine.Alloc(402)}

	assertParity := func(event string) {
		t.Helper()
		want := snapshotOrdinaryDetector(canonical, addr, canonicalCtx)
		got := snapshotOrdinaryDetector(optimized, addr, optimizedCtx)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s fast state differs from canonical:\nfast:      %#v\ncanonical: %#v", event, got, want)
		}
	}

	// The canonical first access materializes the exact eight-lane group and
	// provisions everything required by the non-blocking transaction.
	materializeOrdinaryScalar(canonical, addr, size)
	materializeOrdinaryScalar(optimized, addr, size)
	canonical.OnWriteSized(addr, size, canonicalCtx[0], 0x6101)
	optimized.OnWriteSized(addr, size, optimizedCtx[0], 0x6101)
	materializeOrdinaryScalar(canonical, addr, size)
	materializeOrdinaryScalar(optimized, addr, size)
	assertParity("warm write")

	canonical.OnReadSized(addr, size, canonicalCtx[0], 0x6102)
	if !runOrdinaryFastRead(optimized, addr, size, 0x6102, optimizedCtx[0]) {
		t.Fatal("warmed same-context read missed ordinary fast path")
	}
	assertParity("same-context read")

	canonical.OnWriteSized(addr, size, canonicalCtx[0], 0x6103)
	if !runOrdinaryFastWrite(optimized, addr, size, 0x6103, optimizedCtx[0]) {
		t.Fatal("warmed same-context write missed ordinary fast path")
	}
	assertParity("same-context write")

	// A conflicting reader may conservatively miss. The optimized runner then
	// invokes the canonical event once; parity proves that no partial fast event
	// was left behind before fallback.
	canonical.OnReadSized(addr, size, canonicalCtx[1], 0x6104)
	runOrdinaryFastRead(optimized, addr, size, 0x6104, optimizedCtx[1])
	assertParity("conflicting read fallback")

	canonical.ClearShadowRange(addr, size)
	optimized.ClearShadowRange(addr, size)
	assertParity("allocator clear")
	canonical.OnWriteSized(addr, size, canonicalCtx[1], 0x6105)
	runOrdinaryFastWrite(optimized, addr, size, 0x6105, optimizedCtx[1])
	assertParity("post-clear generation")
}

func TestOrdinaryFastMissIsMutationFree(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(411)
	other := goroutine.Alloc(412)
	const addr = uintptr(0x62000)
	before := snapshotOrdinaryDetector(d, addr, [2]*goroutine.RaceContext{ctx, other})
	if result, state := d.TryOrdinaryRead(addr, 8, ctx, 0x6201); result != shadowmem.OrdinaryFastMiss || state != nil {
		t.Fatalf("cold TryOrdinaryRead = (%d,%p), want miss", result, state)
	}
	if d.TryOrdinaryWrite(addr, 8, ctx, 0x6202) {
		t.Fatal("cold TryOrdinaryWrite unexpectedly handled")
	}
	after := snapshotOrdinaryDetector(d, addr, [2]*goroutine.RaceContext{ctx, other})
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("ordinary misses mutated state:\nafter:  %#v\nbefore: %#v", after, before)
	}

	sampled := NewDetectorWithOptions(DetectorOptions{SamplingEnabled: true, SampleRate: 1})
	sampled.OnWriteSized(addr, 8, ctx, 0x6203)
	before = snapshotOrdinaryDetector(sampled, addr, [2]*goroutine.RaceContext{ctx, nil})
	if result, _ := sampled.TryOrdinaryRead(addr, 8, ctx, 0x6204); result != shadowmem.OrdinaryFastMiss {
		t.Fatal("sampling-enabled detector did not force ordinary miss")
	}
	after = snapshotOrdinaryDetector(sampled, addr, [2]*goroutine.RaceContext{ctx, nil})
	if !reflect.DeepEqual(after, before) {
		t.Fatal("sampling-forced miss mutated detector state")
	}
}
