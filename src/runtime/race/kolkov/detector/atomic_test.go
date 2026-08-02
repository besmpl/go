package detector

import (
	internalsync "internal/sync"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
	"runtime/race/kolkov/vectorclock"
)

func completeAtomicSize(d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, acquire, write bool, pc uintptr) {
	var token AtomicToken
	d.AtomicBegin(addr, size, ctx, acquire, &token)
	d.AtomicEnd(addr, size, ctx, &token, pc, write)
}

func completeAtomic(d *Detector, addr uintptr, ctx *goroutine.RaceContext, acquire, write bool) {
	completeAtomicSize(d, addr, 8, ctx, acquire, write, 0x1000)
}

func TestAtomicReleaseAcquireIsAddressScoped(t *testing.T) {
	d := NewDetector()
	writer := goroutine.Alloc(1)
	reader := goroutine.Alloc(2)

	const releasedAddr = 0x1000
	const unrelatedAddr = 0x2000
	completeAtomic(d, releasedAddr, writer, false, true)
	published := writer.C.Get(writer.TID) - 1 // AtomicEnd advances once after publishing.

	completeAtomic(d, unrelatedAddr, reader, true, false)
	if got := reader.C.Get(writer.TID); got != 0 {
		t.Fatalf("unrelated atomic acquire joined writer clock %d, want 0", got)
	}

	completeAtomic(d, releasedAddr, reader, true, false)
	if got := reader.C.Get(writer.TID); got != published {
		t.Fatalf("same-address atomic acquire joined writer clock %d, want %d", got, published)
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("atomic-only synchronization reported %d races, want 0", got)
	}
}

func TestAtomicHistoryAndReleasePreserveHighLogicalIDs(t *testing.T) {
	d := NewDetector()
	writer := goroutine.Alloc(65536)
	defer writer.C.Release()
	writer.C.Set(1<<20+7, 9)
	reader := goroutine.Alloc(1<<24 + 9)
	defer reader.C.Release()
	const addr = uintptr(0x18000)

	completeAtomic(d, addr, writer, false, true)
	state := atomicHistoryForTest(t, d, addr)
	if _, ok := atomicHistoryAccess(state.writes, writer.TID); !ok {
		t.Fatalf("atomic write history lost high TID %d", writer.TID)
	}
	completeAtomic(d, addr, reader, true, false)
	if got := reader.C.Get(writer.TID); got != 1 {
		t.Fatalf("acquire clock[%d] = %d, want 1", writer.TID, got)
	}
	if got := reader.C.Get(1<<20 + 7); got != 9 {
		t.Fatalf("acquire clock[%d] = %d, want 9", uint32(1<<20+7), got)
	}
}

func TestAtomicOperationsDoNotRaceWithEachOther(t *testing.T) {
	d := NewDetector()
	first := goroutine.Alloc(3)
	second := goroutine.Alloc(4)
	const addr = 0x3000

	// Concurrent stores deliberately have no happens-before edge. They remain
	// race-free because both accesses are atomic.
	completeAtomic(d, addr, first, false, true)
	completeAtomic(d, addr, second, false, true)
	completeAtomic(d, addr, first, true, false)

	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("atomic-only accesses reported %d races, want 0", got)
	}
}

// Every synchronizing atomic access must retain a distinct writer epoch. The
// first store is acquired by reader, but the second store is not ordered before
// reader's later plain access. Reusing the first store's epoch for the second
// would manufacture happens-before and suppress this required mixed-access
// report.
func TestAtomicWriterEpochCannotBeCoalescedAcrossAcquire(t *testing.T) {
	d := NewDetector()
	writer := goroutine.Alloc(4_001)
	reader := goroutine.Alloc(4_002)
	const addr = uintptr(0x3800)

	completeAtomic(d, addr, writer, false, true)
	completeAtomic(d, addr, reader, true, false)
	completeAtomic(d, addr, writer, false, true)
	d.OnRead(addr, reader, 0x1380)

	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("post-acquire atomic/plain conflict reported %d races, want 1", got)
	}
}

func TestFailedCASHistoryIsAtomicRead(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(5)
	const addr = 0x4000

	var token AtomicToken
	d.AtomicBegin(addr, 8, ctx, true, &token)
	d.AtomicEnd(addr, 8, ctx, &token, 0x1234, false)

	vs := d.ShadowGet(addr)
	vs.LockAccess()
	atomicHistory := existingAtomicStateLocked(vs)
	read, readOK := atomicHistoryAccess(atomicHistory.reads, ctx.TID)
	_, writeOK := atomicHistoryAccess(atomicHistory.writes, ctx.TID)
	vs.UnlockAccess()
	if !readOK || read.pcs[0] != 0x1234 {
		t.Fatalf("failed CAS read history = %+v, present=%v", read, readOK)
	}
	if writeOK {
		t.Fatal("failed CAS unexpectedly published an atomic write")
	}
}

func TestAtomicHistoryKeepsHBMaximalDiamond(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0x4200)
	const pc = uintptr(0x2200)

	root := goroutine.Alloc(70)
	completeAtomicSize(d, addr, 8, root, false, true, pc)

	left := goroutine.Alloc(71)
	left.C.Set(root.TID, 1)
	completeAtomicSize(d, addr, 8, left, false, true, pc)

	right := goroutine.Alloc(72)
	right.C.Set(root.TID, 1)
	completeAtomicSize(d, addr, 8, right, false, true, pc)

	state := atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.writes); got != 2 {
		t.Fatalf("diamond frontier cardinality = %d, want 2", got)
	}
	if _, ok := atomicHistoryAccess(state.writes, root.TID); ok {
		t.Fatal("diamond frontier retained HB-dominated root")
	}
	for _, ctx := range []*goroutine.RaceContext{left, right} {
		if _, ok := atomicHistoryAccess(state.writes, ctx.TID); !ok {
			t.Fatalf("diamond frontier lost concurrent TID %d", ctx.TID)
		}
	}

	join := goroutine.Alloc(73)
	join.C.Set(left.TID, 1)
	join.C.Set(right.TID, 1)
	completeAtomicSize(d, addr, 8, join, false, true, pc)
	if got := atomicHistoryCardinality(state.writes); got != 1 {
		t.Fatalf("joined frontier cardinality = %d, want 1", got)
	}
	if _, ok := atomicHistoryAccess(state.writes, join.TID); !ok {
		t.Fatalf("joined frontier lost replacement TID %d", join.TID)
	}
}

func TestAtomicHistoryPrunesOnlyCoveredLanes(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0x4400)
	wide := goroutine.Alloc(74)
	completeAtomicSize(d, addr, 8, wide, false, true, 0x2300)

	low := goroutine.Alloc(75)
	low.C.Set(wide.TID, 1)
	completeAtomicSize(d, addr, 4, low, false, true, 0x2300)

	state := atomicHistoryForTest(t, d, addr)
	wideAccess, ok := atomicHistoryAccess(state.writes, wide.TID)
	if !ok {
		t.Fatal("narrow replacement erased the wide access's uncovered lanes")
	}
	lowAccess, ok := atomicHistoryAccess(state.writes, low.TID)
	if !ok {
		t.Fatal("narrow replacement was not represented")
	}
	for lane := uint8(0); lane < 8; lane++ {
		if lane < 4 {
			if wideAccess.clocks[lane] != 0 || lowAccess.clocks[lane] == 0 {
				t.Fatalf("low lane %d histories wide=%d low=%d, want 0/nonzero", lane, wideAccess.clocks[lane], lowAccess.clocks[lane])
			}
		} else if wideAccess.clocks[lane] == 0 || lowAccess.clocks[lane] != 0 {
			t.Fatalf("high lane %d histories wide=%d low=%d, want nonzero/0", lane, wideAccess.clocks[lane], lowAccess.clocks[lane])
		}
	}
	for lane := uint8(0); lane < 8; lane++ {
		if got := atomicHistoryLaneCardinality(state.writes, lane); got != 1 {
			t.Fatalf("lane %d frontier cardinality = %d, want 1", lane, got)
		}
	}
}

func TestAtomicHistoryReadWritePruningIsAsymmetric(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0x4600)
	const pc = uintptr(0x2400)

	writer := goroutine.Alloc(76)
	completeAtomicSize(d, addr, 8, writer, false, true, pc)
	reader := goroutine.Alloc(77)
	reader.C.Set(writer.TID, 1)
	completeAtomicSize(d, addr, 8, reader, false, false, pc)

	state := atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.writes); got != 1 {
		t.Fatalf("later read pruned write frontier: got %d writes, want 1", got)
	}
	if got := atomicHistoryCardinality(state.reads); got != 1 {
		t.Fatalf("read frontier cardinality = %d, want 1", got)
	}

	laterWriter := goroutine.Alloc(78)
	laterWriter.C.Set(writer.TID, 1)
	laterWriter.C.Set(reader.TID, 1)
	completeAtomicSize(d, addr, 8, laterWriter, false, true, pc)
	if got := atomicHistoryCardinality(state.writes); got != 1 {
		t.Fatalf("later write frontier cardinality = %d, want 1", got)
	}
	if got := atomicHistoryCardinality(state.reads); got != 0 {
		t.Fatalf("later write retained %d dominated reads, want 0", got)
	}
	if _, ok := atomicHistoryAccess(state.writes, laterWriter.TID); !ok {
		t.Fatal("later write was not represented")
	}
}

func TestAtomicHistoryRetainsArbitraryConcurrentAntichain(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0x4800)
	const writers = 512
	for i := uint32(0); i < writers; i++ {
		ctx := goroutine.Alloc(10_000 + i)
		completeAtomicSize(d, addr, 8, ctx, false, true, 0x2500)
		ctx.C.Release()
	}
	state := atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.writes); got != writers {
		t.Fatalf("concurrent frontier cardinality = %d, want %d", got, writers)
	}
	for lane := uint8(0); lane < 8; lane++ {
		if got := atomicHistoryLaneCardinality(state.writes, lane); got != writers {
			t.Fatalf("lane %d concurrent frontier cardinality = %d, want %d", lane, got, writers)
		}
	}
}

func TestAtomicHistoryPreservesUserWitnessAcrossInternalAccess(t *testing.T) {
	mutexPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	if !atomicInternalMutexPC(mutexPC) {
		t.Fatalf("internal mutex PC %#x was not classified", mutexPC)
	}
	d := NewDetector()
	const addr = uintptr(0x4a00)

	user := goroutine.Alloc(79)
	completeAtomicSize(d, addr, 8, user, false, true, 0x2600)
	internal := goroutine.Alloc(80)
	internal.C.Set(user.TID, 1)
	completeAtomicSize(d, addr, 8, internal, false, true, mutexPC)

	state := atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.writes); got != 2 {
		t.Fatalf("internal write pruned user witness: frontier cardinality = %d, want 2", got)
	}
	plainInternal := goroutine.Alloc(81)
	prev, pc, _, conflict := firstConcurrentAtomic(state.writes, plainInternal, 0xff)
	if !conflict || prev == 0 || pc != 0x2600 {
		t.Fatalf("selected mixed witness = (%v, %#x, %v), want user PC %#x", prev, pc, conflict, uintptr(0x2600))
	}
	d.OnRead(addr, plainInternal, markerPC)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("same-lane internal witness hid user race: got %d reports, want 1", got)
	}

	userReplacement := goroutine.Alloc(82)
	userReplacement.C.Set(user.TID, 1)
	userReplacement.C.Set(internal.TID, 1)
	completeAtomicSize(d, addr, 8, userReplacement, false, true, 0x2601)
	if got := atomicHistoryCardinality(state.writes); got != 1 {
		t.Fatalf("user replacement retained %d histories, want 1", got)
	}

	// The same class rule applies when a later write prunes the read frontier.
	// An internal write is not a report-equivalent replacement for a user read.
	d = NewDetector()
	userReader := goroutine.Alloc(85)
	completeAtomicSize(d, addr, 8, userReader, false, false, 0x2602)
	internalWriter := goroutine.Alloc(86)
	internalWriter.C.Set(userReader.TID, 1)
	completeAtomicSize(d, addr, 8, internalWriter, false, true, mutexPC)
	state = atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.reads); got != 1 {
		t.Fatalf("internal write pruned user read witness: got %d reads, want 1", got)
	}
}

func TestAtomicInternalRMWPCClassifiesOnlyInternalMutex(t *testing.T) {
	mutexPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	if !AtomicInternalRMWPC(mutexPC) {
		t.Fatalf("internal Mutex PC %#x was not classified", mutexPC)
	}
	for _, pc := range []uintptr{
		reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1,
		reflect.ValueOf((*sync.WaitGroup).Add).Pointer() + 1,
	} {
		if AtomicInternalRMWPC(pc) {
			t.Fatalf("RaceDisable implementation PC %#x was classified", pc)
		}
	}
	userPC := reflect.ValueOf(completeAtomicSize).Pointer() + 1
	if AtomicInternalRMWPC(userPC) {
		t.Fatalf("user PC %#x was classified as an implementation atomic", userPC)
	}
}

func TestMixedAtomicSelectionSkipsUnreportableInternalWitnesses(t *testing.T) {
	mutexPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	const userPC = uintptr(0x2610)

	seedInternalWriteAndUserRead := func(t *testing.T, addr uintptr) (*Detector, uint32) {
		t.Helper()
		d := NewDetector()
		completeAtomicSize(d, addr, 8, goroutine.Alloc(87), false, true, mutexPC)
		userReader := goroutine.Alloc(88)
		completeAtomicSize(d, addr, 8, userReader, false, false, userPC)
		return d, userReader.TID
	}

	for _, test := range []struct {
		name  string
		addr  uintptr
		write func(*Detector, uintptr, *goroutine.RaceContext)
	}{
		{
			name: "scalar",
			addr: 0x4e00,
			write: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) {
				d.OnWrite(addr, ctx, markerPC)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, userTID := seedInternalWriteAndUserRead(t, test.addr)
			var reports []*RaceReport
			d.reportObserver = func(report *RaceReport) { reports = append(reports, report) }
			test.write(d, test.addr, goroutine.Alloc(89))
			if got := d.RacesDetected(); got != 1 {
				t.Fatalf("internal write witness blocked user read: got %d reports, want 1", got)
			}
			if len(reports) != 1 || reports[0].Previous.GoroutineID != userTID || reports[0].Previous.Type != AccessRead {
				t.Fatalf("selected report = %+v, want prior user read from TID %d", reports, userTID)
			}
		})
	}

	t.Run("internal-only write is suppressed", func(t *testing.T) {
		d := NewDetector()
		const addr = uintptr(0x5200)
		completeAtomicSize(d, addr, 8, goroutine.Alloc(92), false, true, mutexPC)
		d.OnWrite(addr, goroutine.Alloc(93), markerPC)
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("internal-only mixed write reported %d races", got)
		}
	})

	t.Run("internal-only read is suppressed", func(t *testing.T) {
		d := NewDetector()
		const addr = uintptr(0x5400)
		completeAtomicSize(d, addr, 8, goroutine.Alloc(94), false, false, mutexPC)
		d.OnWriteRange(addr, 8, goroutine.Alloc(95), markerPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("range access with marker PC reported %d races, want 1", got)
		}
	})

	t.Run("user plain keeps internal atomic reportable", func(t *testing.T) {
		d := NewDetector()
		const addr = uintptr(0x5600)
		internalWriter := goroutine.Alloc(96)
		completeAtomicSize(d, addr, 8, internalWriter, false, true, mutexPC)
		d.OnWrite(addr, goroutine.Alloc(97), userPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("user plain/internal atomic conflict reported %d races, want 1", got)
		}
	})

	t.Run("internal plain keeps user atomic reportable", func(t *testing.T) {
		d := NewDetector()
		const addr = uintptr(0x5800)
		userWriter := goroutine.Alloc(98)
		completeAtomicSize(d, addr, 8, userWriter, false, true, userPC)
		d.OnReadRange(addr, 8, goroutine.Alloc(99), markerPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("internal plain/user atomic conflict reported %d races, want 1", got)
		}
	})

	t.Run("reportable write retains priority over read", func(t *testing.T) {
		d := NewDetector()
		const addr = uintptr(0x5a00)
		userWriter := goroutine.Alloc(100)
		completeAtomicSize(d, addr, 8, userWriter, false, true, userPC)
		completeAtomicSize(d, addr, 8, goroutine.Alloc(101), false, false, userPC+1)
		var report *RaceReport
		d.reportObserver = func(observed *RaceReport) { report = observed }
		d.OnWrite(addr, goroutine.Alloc(102), markerPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("mixed write/read histories reported %d races, want 1", got)
		}
		if report == nil || report.Previous.GoroutineID != userWriter.TID || report.Previous.Type != AccessWrite {
			t.Fatalf("selected report = %+v, want prior user write from TID %d", report, userWriter.TID)
		}
	})
}

func TestAtomicPCClassCacheCollisionKeepsClassificationConsistent(t *testing.T) {
	// These synthetic PCs deliberately select the same direct-mapped entry.
	// Pre-populating the independent slots exercises lookup without asking the
	// runtime symbolizer to interpret synthetic program counters.
	const userPC = uintptr(0x20010)
	const internalPC = userPC + atomicPCClassCacheSlots*16
	if (userPC>>4)&(atomicPCClassCacheSlots-1) != (internalPC>>4)&(atomicPCClassCacheSlots-1) {
		t.Fatal("test PCs do not collide")
	}
	entry := &atomicPCClassCache[(userPC>>4)&(atomicPCClassCacheSlots-1)]
	entry.userPC.Store(userPC)
	entry.internalPC.Store(internalPC)
	defer func() {
		entry.userPC.Store(0)
		entry.internalPC.Store(0)
	}()

	if atomicInternalMutexPC(userPC) {
		t.Fatal("colliding user PC was read with the internal classification")
	}
	if !atomicInternalMutexPC(internalPC) {
		t.Fatal("colliding internal PC was read with the user classification")
	}

	// Concurrent same-entry publications cannot tear because each class owns
	// one atomic word. Readers must continue to observe both classifications.
	const iterations = 10_000
	errors := make(chan string, 2)
	var writers sync.WaitGroup
	writers.Add(2)
	go func() {
		defer writers.Done()
		for i := 0; i < iterations; i++ {
			entry.userPC.Store(userPC)
			if atomicInternalMutexPC(userPC) {
				errors <- "user PC changed classification"
				return
			}
		}
	}()
	go func() {
		defer writers.Done()
		for i := 0; i < iterations; i++ {
			entry.internalPC.Store(internalPC)
			if !atomicInternalMutexPC(internalPC) {
				errors <- "internal PC changed classification"
				return
			}
		}
	}()
	writers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func TestAtomicHistoryPrunesWhenSynchronizationDisabled(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0x4c00)
	first := goroutine.Alloc(83)
	var token AtomicToken
	d.AtomicBegin(addr, 8, first, false, &token)
	d.AtomicEndMode(addr, 8, first, &token, 0x2700, true, false)

	second := goroutine.Alloc(84)
	second.C.Set(first.TID, 1)
	d.AtomicBegin(addr, 8, second, false, &token)
	d.AtomicEndMode(addr, 8, second, &token, 0x2700, true, false)

	state := atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.writes); got != 1 {
		t.Fatalf("disabled-sync frontier cardinality = %d, want 1", got)
	}
	if _, ok := atomicHistoryAccess(state.writes, second.TID); !ok {
		t.Fatal("disabled-sync replacement was not represented")
	}
	for lane, release := range state.releases {
		if release != nil {
			t.Fatalf("disabled-sync lane %d retained release %p", lane, release)
		}
	}
	if got := second.C.Get(second.TID); got != 1 {
		t.Fatalf("disabled-sync operation advanced clock to %d, want 1", got)
	}
}

func TestAtomicClockAdvancesExactlyOnceAfterEnd(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(6)
	const addr = uintptr(0x5000)
	before := ctx.C.Get(ctx.TID)
	var token AtomicToken

	d.AtomicBegin(addr, 8, ctx, false, &token)
	if got := ctx.C.Get(ctx.TID); got != before {
		t.Fatalf("AtomicBegin advanced clock to %d, want %d", got, before)
	}
	d.AtomicEnd(addr, 8, ctx, &token, 0x2000, true)
	if got := ctx.C.Get(ctx.TID); got != before+1 {
		t.Fatalf("AtomicEnd clock = %d, want %d", got, before+1)
	}
	for i, state := range token {
		if state != nil {
			t.Fatalf("token[%d] retained %p after End", i, state)
		}
	}
}

func TestAtomicAcquireOnlyCompletionKeepsClockAndWeakensReadCache(t *testing.T) {
	tests := []struct {
		name       string
		expectFast bool
		begin      func(*Detector, uintptr, *goroutine.RaceContext, *AtomicToken)
	}{
		{
			name: "load",
			begin: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext, token *AtomicToken) {
				d.AtomicBegin(addr, 8, ctx, true, token)
			},
		},
		{
			name:       "failed CAS",
			expectFast: true,
			begin: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext, token *AtomicToken) {
				d.AtomicBeginRMW(addr, 8, ctx, true, token)
			},
		},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := NewDetector()
			ctx := goroutine.Alloc(uint32(90 + i))
			defer ctx.C.Release()
			addr := uintptr(0x5100 + i*0x100)
			cacheAddr := uintptr(0x9000 + i*8)
			cacheState := unsafe.Pointer(new(byte))
			ctx.RecordReadSized(cacheAddr, 4, cacheState)
			before := ctx.GetEpoch()
			beforeClock := ctx.C.Get(ctx.TID)
			ctx.InvalidateReadCacheAt(before)

			var token AtomicToken
			test.begin(d, addr, ctx, &token)
			if fast := atomicFastToken(&token) != nil; fast != test.expectFast {
				t.Fatalf("fast token = %v, want %v", fast, test.expectFast)
			}
			d.AtomicEnd(addr, 8, ctx, &token, 0x2010+uintptr(i), false)

			if got := ctx.GetEpoch(); got != before {
				t.Fatalf("acquire-only completion advanced epoch from %v to %v", before, got)
			}
			if got := ctx.C.Get(ctx.TID); got != beforeClock {
				t.Fatalf("acquire-only completion advanced own clock to %d, want %d", got, beforeClock)
			}
			if !ctx.HasWeakReadHintSized(cacheAddr, 4) {
				t.Fatal("acquire-only completion did not weaken the ordinary read cache")
			}
			slot := goroutine.ReadCacheIndex(cacheAddr)
			if got := ctx.ReadCacheStates[slot]; got != nil {
				t.Fatalf("acquire-only completion retained read-cache state %p", got)
			}
			_, clock := before.Decode()
			if got := ctx.ReadCacheInvalidatedClock.Load(); got != uint32(clock) {
				t.Fatalf("acquire-only completion changed external invalidation marker to %d, want %d", got, clock)
			}
		})
	}
}

func TestAtomicInvalidAccessDoesNotRetainState(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(60)
	invalid := []struct {
		addr uintptr
		size uintptr
	}{
		{addr: 0, size: 8},
		{addr: 0x1000, size: 0},
		{addr: 0x1000, size: 2},
		{addr: ^uintptr(0) - 2, size: 4},
	}
	for _, test := range invalid {
		var token AtomicToken
		d.AtomicBegin(test.addr, test.size, ctx, true, &token)
		for i, state := range token {
			if state != nil {
				t.Fatalf("AtomicBegin(%#x, %d) retained token[%d]=%p", test.addr, test.size, i, state)
			}
		}
		d.AtomicEndMode(test.addr, test.size, ctx, &token, 0x2222, true, true)
	}

	// Public detector helpers also accept a nil token/context defensively.
	d.AtomicBegin(0x1000, 8, ctx, true, nil)
	var token AtomicToken
	d.AtomicBegin(0x1000, 8, nil, true, &token)
	d.AtomicEndMode(0x1000, 8, nil, &token, 0x2222, true, true)
	if got := d.ShadowGet(0x1000); got != nil {
		t.Fatalf("invalid atomic calls created shadow state %p", got)
	}
}

func TestAtomicWidthOverlapAndAdjacentBoundaries(t *testing.T) {
	for _, size := range []uintptr{4, 8} {
		t.Run(string(rune('0'+size)), func(t *testing.T) {
			const base = uintptr(0x6000)

			d := NewDetector()
			completeAtomicSize(d, base, size, goroutine.Alloc(7), false, true, 0x3000)
			d.OnRead(base+size-1, goroutine.Alloc(8), 0x3001)
			if got := d.RacesDetected(); got != 1 {
				t.Fatalf("plain start inside atomic width reported %d races, want 1", got)
			}

			d = NewDetector()
			completeAtomicSize(d, base, size, goroutine.Alloc(7), false, true, 0x3010)
			d.OnRead(base+size, goroutine.Alloc(8), 0x3011)
			if got := d.RacesDetected(); got != 0 {
				t.Fatalf("plain access at x+size reported %d races, want 0", got)
			}

			d = NewDetector()
			completeAtomicSize(d, base, size, goroutine.Alloc(7), false, true, 0x3020)
			d.OnReadRange(base+size-2, 4, goroutine.Alloc(8), 0x3021)
			if got := d.RacesDetected(); got != 1 {
				t.Fatalf("range overlapping atomic tail reported %d races, want 1", got)
			}

			d = NewDetector()
			completeAtomicSize(d, base, size, goroutine.Alloc(7), false, true, 0x3030)
			d.OnReadRange(base+size, 4, goroutine.Alloc(8), 0x3031)
			if got := d.RacesDetected(); got != 0 {
				t.Fatalf("adjacent range at x+size reported %d races, want 0", got)
			}
		})
	}
}

func TestUnalignedAtomicSpansTwoShadowWordsExactly(t *testing.T) {
	tests := []struct {
		name string
		off  uintptr
		size uintptr
	}{
		{name: "four", off: 6, size: 4},
		{name: "eight", off: 3, size: 8},
	}
	const base = uintptr(0x6800)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			addr := base + test.off
			d := NewDetector()
			writer := goroutine.Alloc(61)
			completeAtomicSize(d, addr, test.size, writer, false, true, 0x3300)

			first := atomicHistoryForTest(t, d, addr)
			last := atomicHistoryForTest(t, d, addr+test.size-1)
			if first.base != base {
				t.Fatalf("first overlay base = %#x, want %#x", first.base, base)
			}
			if last.base != base+8 {
				t.Fatalf("last overlay base = %#x, want %#x", last.base, base+8)
			}
			if first == last {
				t.Fatal("spanning operation shared one overlay across shadow words")
			}

			for _, probe := range []struct {
				addr uintptr
				want int
			}{
				{addr: addr - 1, want: 0},
				{addr: addr, want: 1},
				{addr: addr + test.size - 1, want: 1},
				{addr: addr + test.size, want: 0},
			} {
				probeDetector := NewDetector()
				completeAtomicSize(probeDetector, addr, test.size, goroutine.Alloc(61), false, true, 0x3300)
				probeDetector.OnRead(probe.addr, goroutine.Alloc(62), 0x3301)
				if got := probeDetector.RacesDetected(); got != probe.want {
					t.Fatalf("plain read at %#x reported %d races, want %d", probe.addr, got, probe.want)
				}
			}
		})
	}
}

func TestMixedAtomicRaceAddressReuseHasNewDedupLifecycle(t *testing.T) {
	d := NewDetector()
	writer := goroutine.Alloc(63)
	reader := goroutine.Alloc(64)
	const addr = uintptr(0x6a00)

	completeAtomicSize(d, addr, 4, writer, false, true, 0x3400)
	d.OnRead(addr, reader, 0x3401)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("first lifecycle reported %d races, want 1", got)
	}

	d.ClearShadowRange(addr, 4)
	completeAtomicSize(d, addr, 4, writer, false, true, 0x3400)
	d.OnRead(addr, reader, 0x3401)
	if got := d.RacesDetected(); got != 2 {
		t.Fatalf("reused mixed atomic address reported %d races, want 2", got)
	}
}

func TestRangeFindsAtomicConflictOnNonFirstGroupLane(t *testing.T) {
	const base = uintptr(0x7000)

	d := NewDetector()
	completeAtomicSize(d, base+4, 4, goroutine.Alloc(9), false, true, 0x4000)
	d.OnReadRange(base, 8, goroutine.Alloc(10), 0x4001)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("range missed later-lane atomic conflict: got %d races, want 1", got)
	}

	d = NewDetector()
	completeAtomicSize(d, base+4, 4, goroutine.Alloc(9), false, true, 0x4010)
	d.OnReadRange(base, 4, goroutine.Alloc(10), 0x4011)
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("range adjacent to atomic half reported %d races, want 0", got)
	}
}

func TestAtomicEndChecksOrdinaryRangeOnce(t *testing.T) {
	d := NewDetector()
	plain := goroutine.Alloc(11)
	atomicReader := goroutine.Alloc(12)
	const addr = uintptr(0x8000)

	d.OnWriteRange(addr, 8, plain, 0x5000)
	completeAtomicSize(d, addr, 8, atomicReader, true, false, 0x5001)

	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("one atomic operation over conflicting range reported %d races, want 1", got)
	}
}

func TestAtomicReleaseSnapshotReplacedPerLane(t *testing.T) {
	d := NewDetector()
	wideWriter := goroutine.Alloc(13)
	narrowWriter := goroutine.Alloc(14)
	reader := goroutine.Alloc(15)
	const data = uintptr(0x9000)
	const flag = uintptr(0xa000)

	// The old wide release contains wideWriter's ordinary data write.
	d.OnWrite(data, wideWriter, 0x6000)
	completeAtomicSize(d, flag, 8, wideWriter, false, true, 0x6001)

	// A non-acquiring narrow store supersedes the low lanes. A later narrow
	// acquire must not resurrect the stale wide release through overlap.
	completeAtomicSize(d, flag, 4, narrowWriter, false, true, 0x6002)
	completeAtomicSize(d, flag, 4, reader, true, false, 0x6003)
	if got := reader.C.Get(wideWriter.TID); got != 0 {
		t.Fatalf("narrow acquire joined stale wide release clock %d", got)
	}
	if write := d.ShadowGet(data).GetW(); write.HappensBefore(reader.C) {
		t.Fatalf("stale release made concurrent ordinary write %v happen-before reader %v", write, reader.C)
	}
}

func TestOrdinaryScalarWritesRetireAtomicReleaseAllLanes(t *testing.T) {
	d := NewDetector()
	publisher := goroutine.Alloc(154)
	plainWriter := goroutine.Alloc(155)
	reader := goroutine.Alloc(156)
	const data = uintptr(0xa100)
	const flag = uintptr(0xa200)

	// The release carries publisher's earlier data write. Every subsequent
	// ordinary flag write is a mixed-access race, but it still becomes the
	// latest modification and must retire that lane before the conflict path
	// returns.
	d.OnWrite(data, publisher, 0x6100)
	completeAtomicSize(d, flag, 8, publisher, false, true, 0x6101)
	state := atomicHistoryForTest(t, d, flag)
	release := state.releases[0]
	if release == nil {
		t.Fatal("wide store published no release")
	}
	if release.refs != 8 {
		t.Fatalf("wide release %p refs=%d, want 8", release, release.refs)
	}

	for lane := uintptr(0); lane < 8; lane++ {
		d.OnWrite(flag+lane, plainWriter, 0x6110+lane)
		state.mu.lock()
		got := state.releases[lane]
		refs := release.refs
		state.mu.unlock()
		if got != nil {
			t.Fatalf("ordinary scalar write left lane %d release %p", lane, got)
		}
		if want := uint8(7 - lane); refs != want {
			t.Fatalf("release refs after lane %d = %d, want %d", lane, refs, want)
		}
	}

	completeAtomicSize(d, flag, 8, reader, true, false, 0x6120)
	if got := reader.C.Get(publisher.TID); got != 0 {
		t.Fatalf("acquire joined release superseded by scalar writes: clock=%d", got)
	}
	if write := d.ShadowGet(data).GetW(); write.HappensBefore(reader.C) {
		t.Fatalf("superseded release made data write %v happen-before reader %v", write, reader.C)
	}
	before := d.RacesDetected()
	d.OnRead(data, reader, 0x6121)
	if got := d.RacesDetected(); got != before+1 {
		t.Fatalf("downstream data read changed race count from %d to %d, want %d", before, got, before+1)
	}
}

func TestOrdinarySizedWriteRetiresExactAtomicReleaseLanes(t *testing.T) {
	d := NewDetector()
	publisher := goroutine.Alloc(163)
	plainWriter := goroutine.Alloc(164)
	const flag = uintptr(0xa280)

	completeAtomicSize(d, flag, 8, publisher, false, true, 0x6180)
	state := atomicHistoryForTest(t, d, flag)
	release := state.releases[0]
	if release == nil || release.refs != 8 {
		t.Fatalf("wide release = %p refs=%d, want non-nil/8", release, release.refs)
	}

	// Compiler scalar hooks retain ordinary start-address semantics, but their
	// physical width must retire every overlapping atomic lane.
	d.OnWriteSized(flag+2, 2, plainWriter, 0x6181)
	state.mu.lock()
	defer state.mu.unlock()
	for lane := uint8(0); lane < 8; lane++ {
		got := state.releases[lane]
		if lane == 2 || lane == 3 {
			if got != nil {
				t.Fatalf("two-byte ordinary write left lane %d release %p", lane, got)
			}
		} else if got != release {
			t.Fatalf("two-byte ordinary write changed disjoint lane %d from %p to %p", lane, release, got)
		}
	}
	if release.refs != 6 {
		t.Fatalf("release refs after two-byte ordinary write = %d, want 6", release.refs)
	}
	poison, ok := atomicHistoryAccess(state.plainWrites, plainWriter.TID)
	if !ok || poison.clocks[2] == 0 || poison.clocks[3] == 0 {
		t.Fatalf("two-byte ordinary poison = %+v, present=%v", poison, ok)
	}
	for lane := uint8(0); lane < 8; lane++ {
		if lane != 2 && lane != 3 && poison.clocks[lane] != 0 {
			t.Fatalf("two-byte ordinary poison leaked into lane %d", lane)
		}
	}
}

func TestConcurrentOrdinaryPreStorePoisonBlocksAtomicPublication(t *testing.T) {
	d := NewDetector()
	plainWriter := goroutine.Alloc(165)
	atomicWriter := goroutine.Alloc(166)
	reader := goroutine.Alloc(167)
	const (
		data = uintptr(0xa2c0)
		flag = uintptr(0xa2d0)
	)

	// The compiler invokes the ordinary hook before the machine store. Model an
	// interleaving in which that hook completes, a concurrent atomic Store wins
	// temporarily, and the ordinary machine store can still overwrite it later.
	// Publishing the atomic writer's data in this window would create false HB.
	completeAtomicSize(d, flag, 4, goroutine.Alloc(168), false, true, 0x6190)
	d.OnWriteSized(flag, 4, plainWriter, 0x6191)
	d.OnWrite(data, atomicWriter, 0x6192)
	completeAtomicSize(d, flag, 4, atomicWriter, false, true, 0x6193)

	state := atomicHistoryForTest(t, d, flag)
	state.mu.lock()
	for lane := uint8(0); lane < 4; lane++ {
		if release := state.releases[lane]; release != nil {
			state.mu.unlock()
			t.Fatalf("concurrent pre-store poison allowed lane %d release %p", lane, release)
		}
	}
	state.mu.unlock()

	completeAtomicSize(d, flag, 4, reader, true, false, 0x6194)
	if got := reader.C.Get(atomicWriter.TID); got != 0 {
		t.Fatalf("reader acquired poisoned atomic publication clock %d", got)
	}
	if write := d.ShadowGet(data).GetW(); write.HappensBefore(reader.C) {
		t.Fatalf("poisoned publication ordered data write %v before reader %v", write, reader.C)
	}
}

func TestHBBeforeOrdinaryPoisonPermitsAtomicPublication(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(169)
	plainWriter := goroutine.Alloc(170)
	atomicWriter := goroutine.Alloc(171)
	reader := goroutine.Alloc(172)
	const flag = uintptr(0xa2e0)

	completeAtomicSize(d, flag, 4, seed, false, true, 0x61a0)
	plainWriter.C.Set(seed.TID, 1)
	d.OnWriteSized(flag, 4, plainWriter, 0x61a1)
	atomicWriter.C.Set(seed.TID, 1)
	atomicWriter.C.Set(plainWriter.TID, plainWriter.C.Get(plainWriter.TID))
	completeAtomicSize(d, flag, 4, atomicWriter, false, true, 0x61a2)

	state := atomicHistoryForTest(t, d, flag)
	state.mu.lock()
	if got := atomicHistoryCardinality(state.plainWrites); got != 0 {
		state.mu.unlock()
		t.Fatalf("HB-dominated ordinary poison retained %d frontiers, want 0", got)
	}
	for lane := uint8(0); lane < 4; lane++ {
		if state.releases[lane] == nil {
			state.mu.unlock()
			t.Fatalf("HB-dominated poison suppressed lane %d release", lane)
		}
	}
	state.mu.unlock()

	completeAtomicSize(d, flag, 4, reader, true, false, 0x61a3)
	if got := reader.C.Get(atomicWriter.TID); got == 0 {
		t.Fatal("reader failed to acquire publication after HB-dominated poison")
	}
}

func TestAtomicReadRetainsOrdinaryPreStorePoison(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(173)
	plainWriter := goroutine.Alloc(174)
	atomicReader := goroutine.Alloc(175)
	const flag = uintptr(0xa2f0)

	completeAtomicSize(d, flag, 4, seed, false, true, 0x61b0)
	d.OnWriteSized(flag, 4, plainWriter, 0x61b1)
	completeAtomicSize(d, flag, 4, atomicReader, true, false, 0x61b2)

	state := atomicHistoryForTest(t, d, flag)
	state.mu.lock()
	poison, ok := atomicHistoryAccess(state.plainWrites, plainWriter.TID)
	state.mu.unlock()
	if !ok || poison.clocks[0] == 0 {
		t.Fatalf("atomic read discarded ordinary poison: %+v, present=%v", poison, ok)
	}
}

func TestClearDropsOrdinaryPreStorePoisonGeneration(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(176)
	plainWriter := goroutine.Alloc(177)
	replacement := goroutine.Alloc(178)
	const flag = uintptr(0xa380)

	completeAtomicSize(d, flag, 4, seed, false, true, 0x61c0)
	old := atomicHistoryForTest(t, d, flag)
	oldHandle := old.handle
	d.OnWriteSized(flag, 4, plainWriter, 0x61c1)
	if got := atomicHistoryCardinality(old.plainWrites); got == 0 {
		t.Fatal("ordinary write did not seed pre-store poison")
	}

	d.ClearShadowRange(flag, 4)
	completeAtomicSize(d, flag, 4, replacement, false, true, 0x61c2)
	fresh := atomicHistoryForTest(t, d, flag)
	if fresh.handle == oldHandle {
		t.Fatal("cleared address retained poisoned atomic overlay generation")
	}
	if got := atomicHistoryCardinality(fresh.plainWrites); got != 0 {
		t.Fatalf("fresh atomic overlay inherited %d ordinary poison frontiers", got)
	}
}

func TestOrdinaryRangeWriteRetiresCoveredAtomicReleaseLanes(t *testing.T) {
	d := NewDetector()
	publisher := goroutine.Alloc(157)
	plainWriter := goroutine.Alloc(158)
	reader := goroutine.Alloc(159)
	const data = uintptr(0xa300)
	const flag = uintptr(0xa400)

	d.OnWrite(data, publisher, 0x6200)
	completeAtomicSize(d, flag, 8, publisher, false, true, 0x6201)
	state := atomicHistoryForTest(t, d, flag)
	release := state.releases[0]
	if release == nil {
		t.Fatal("wide store published no release")
	}
	if release.refs != 8 {
		t.Fatalf("wide release %p refs=%d, want 8", release, release.refs)
	}

	// Overwrite only the low atomic half through the ordinary range hook. The
	// low acquire below must not synchronize through the old wide release;
	// untouched high lanes keep their latest modification snapshot.
	d.OnWriteRange(flag, 4, plainWriter, 0x6202)
	state.mu.lock()
	for lane := uint8(0); lane < 4; lane++ {
		if got := state.releases[lane]; got != nil {
			state.mu.unlock()
			t.Fatalf("ordinary range write left covered lane %d release %p", lane, got)
		}
	}
	for lane := uint8(4); lane < 8; lane++ {
		if got := state.releases[lane]; got != release {
			state.mu.unlock()
			t.Fatalf("ordinary range write changed untouched lane %d release from %p to %p", lane, release, got)
		}
	}
	refs := release.refs
	state.mu.unlock()
	if refs != 4 {
		t.Fatalf("partially retired release refs = %d, want 4", refs)
	}

	completeAtomicSize(d, flag, 4, reader, true, false, 0x6203)
	if got := reader.C.Get(publisher.TID); got != 0 {
		t.Fatalf("low acquire joined release superseded by range write: clock=%d", got)
	}
	if write := d.ShadowGet(data).GetW(); write.HappensBefore(reader.C) {
		t.Fatalf("superseded release made data write %v happen-before reader %v", write, reader.C)
	}
	before := d.RacesDetected()
	d.OnRead(data, reader, 0x6204)
	if got := d.RacesDetected(); got != before+1 {
		t.Fatalf("downstream data read changed race count from %d to %d, want %d", before, got, before+1)
	}
}

func TestOrdinaryRangeReadPreservesAtomicRelease(t *testing.T) {
	d := NewDetector()
	publisher := goroutine.Alloc(160)
	plainReader := goroutine.Alloc(161)
	atomicReader := goroutine.Alloc(162)
	const data = uintptr(0xa500)
	const flag = uintptr(0xa600)

	d.OnWrite(data, publisher, 0x6300)
	completeAtomicSize(d, flag, 8, publisher, false, true, 0x6301)
	state := atomicHistoryForTest(t, d, flag)
	release := state.releases[0]
	if release == nil {
		t.Fatal("wide store published no release")
	}

	// An ordinary read may race with an atomic write, but unlike an ordinary
	// write it does not become the latest modification and must not retire the
	// release snapshot.
	d.OnReadRange(flag, 4, plainReader, 0x6302)
	state.mu.lock()
	for lane := uint8(0); lane < 8; lane++ {
		if got := state.releases[lane]; got != release {
			state.mu.unlock()
			t.Fatalf("ordinary range read changed lane %d release from %p to %p", lane, release, got)
		}
	}
	refs := release.refs
	state.mu.unlock()
	if refs != 8 {
		t.Fatalf("release refs after ordinary range read = %d, want 8", refs)
	}

	completeAtomicSize(d, flag, 4, atomicReader, true, false, 0x6303)
	if got := atomicReader.C.Get(publisher.TID); got == 0 {
		t.Fatal("atomic acquire lost release after ordinary range read")
	}
	if write := d.ShadowGet(data).GetW(); !write.HappensBefore(atomicReader.C) {
		t.Fatalf("preserved release did not order data write %v before reader %v", write, atomicReader.C)
	}
}

func TestAtomicReboundOverlayClearsDetachedLaneRelease(t *testing.T) {
	d := NewDetector()
	wideWriter := goroutine.Alloc(151)
	lowWriter := goroutine.Alloc(152)
	reader := goroutine.Alloc(153)
	const addr = uintptr(0xa800)

	// Give the high lanes a release from wideWriter, then supersede only the
	// low lanes with an unrelated release. Clearing the low VarState leaves the
	// shared overlay reachable through the high lanes.
	completeAtomicSize(d, addr, 8, wideWriter, false, true, 0x6800)
	completeAtomicSize(d, addr, 4, lowWriter, false, true, 0x6801)
	surviving := atomicHistoryForTest(t, d, addr+4)
	d.ClearShadowRange(addr, 4)

	// A later wide setup safely converges the new low generation onto the
	// surviving overlay. It must clear the detached lanes before an acquire, or
	// the reader would resurrect lowWriter's stale release.
	completeAtomicSize(d, addr, 8, reader, true, false, 0x6802)
	if got := atomicHistoryForTest(t, d, addr); got != surviving {
		t.Fatalf("rebound low lanes use overlay %p, want surviving %p", got, surviving)
	}
	if got := reader.C.Get(lowWriter.TID); got != 0 {
		t.Fatalf("wide acquire joined detached low-lane release clock %d", got)
	}
	if got := reader.C.Get(wideWriter.TID); got == 0 {
		t.Fatal("wide acquire lost surviving high-lane release")
	}
	if access, ok := atomicHistoryAccess(surviving.writes, lowWriter.TID); ok {
		for lane := uint8(0); lane < 4; lane++ {
			if access.clocks[lane] != 0 {
				t.Fatalf("rebound lane %d retained detached write clock %d", lane, access.clocks[lane])
			}
		}
	}
}

func TestAtomicWideOperationKeepsSplitOverlayMembershipExact(t *testing.T) {
	d := NewDetector()
	low := goroutine.Alloc(16)
	high := goroutine.Alloc(17)
	wide := goroutine.Alloc(18)
	const addr = uintptr(0xb000)

	completeAtomicSize(d, addr, 4, low, false, true, 0x7000)
	completeAtomicSize(d, addr+4, 4, high, false, true, 0x7001)
	lowOverlay := atomicHistoryForTest(t, d, addr)
	highOverlay := atomicHistoryForTest(t, d, addr+4)
	if lowOverlay == highOverlay {
		t.Fatal("independent low/high atomic histories unexpectedly merged")
	}

	completeAtomicSize(d, addr, 8, wide, true, true, 0x7002)
	if lowOverlay.releases[4] != nil || lowOverlay.releases[7] != nil {
		t.Fatal("wide operation published high-lane release into low overlay")
	}
	if highOverlay.releases[0] != nil || highOverlay.releases[3] != nil {
		t.Fatal("wide operation published low-lane release into high overlay")
	}
	if access, _ := atomicHistoryAccess(lowOverlay.writes, wide.TID); access.clocks[4] != 0 {
		t.Fatal("wide operation copied high access history into low overlay")
	}
	if access, _ := atomicHistoryAccess(highOverlay.writes, wide.TID); access.clocks[3] != 0 {
		t.Fatal("wide operation copied low access history into high overlay")
	}

	// Forget only the low generation. A new wide operation may safely converge
	// the empty low lanes onto the surviving high overlay after clearing the
	// rebound lane histories. The distinct old low overlay remains detached.
	d.ClearShadowRange(addr, 4)
	completeAtomicSize(d, addr, 8, goroutine.Alloc(19), true, false, 0x7003)
	freshLow := atomicHistoryForTest(t, d, addr)
	if freshLow != highOverlay {
		t.Fatalf("rebound low lanes use overlay %p, want surviving high overlay %p", freshLow, highOverlay)
	}
	if freshLow == lowOverlay {
		t.Fatal("rebound low lanes reused the detached low overlay")
	}
	if got := atomicHistoryForTest(t, d, addr+4); got != highOverlay {
		t.Fatalf("surviving high overlay changed from %p to %p", highOverlay, got)
	}
}

func TestAtomicReleaseSnapshotSharedAndReusedForWideStore(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(20)
	const addr = uintptr(0xc000)

	completeAtomicSize(d, addr, 8, ctx, false, true, 0x8000)
	state := atomicHistoryForTest(t, d, addr)
	release := state.releases[0]
	if release == nil {
		t.Fatal("wide store published no release snapshot")
	}
	if release.refs != 8 {
		t.Fatalf("wide release = %p refs=%d, want 8", release, release.refs)
	}
	ordinary := d.ShadowGet(addr)
	for lane := uintptr(0); lane < 8; lane++ {
		if state.releases[lane] != release {
			t.Fatalf("lane %d release = %p, want shared %p", lane, state.releases[lane], release)
		}
		if got := d.ShadowGet(addr + lane); got != ordinary {
			t.Fatalf("fresh wide atomic ordinary lane %d = %p, want one group %p", lane, got, ordinary)
		}
	}

	completeAtomicSize(d, addr, 8, ctx, false, true, 0x8001)
	if got := state.releases[0]; got != release {
		t.Fatalf("steady wide store replaced recyclable snapshot %p with %p", release, got)
	}
}

func TestAtomicSequentialFreshTIDRMWKeepsBoundedFrontierAndPromotesLineage(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0xc400)
	const firstTID = uint32(20_000)
	const contexts = uint32(1024)
	for i := uint32(0); i < contexts; i++ {
		ctx := goroutine.Alloc(firstTID + i)
		completeAtomicSize(d, addr, 8, ctx, true, true, 0x2800)
		ctx.C.Release()
	}

	state := atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.writes); got != 1 {
		t.Fatalf("sequential fresh-TID write frontier = %d, want 1", got)
	}
	if got := atomicHistoryCardinality(state.reads); got != 0 {
		t.Fatalf("successful RMW retained %d read histories, want 0", got)
	}
	release := state.releases[0]
	if release == nil {
		t.Fatal("sequential RMW chain published no release")
	}
	if release.lineage == nil || !release.view.Valid() {
		t.Fatal("many-owner contiguous release did not promote to a causal lineage")
	}
	for _, tid := range []uint32{firstTID, firstTID + contexts/2, firstTID + contexts - 1} {
		if got := release.view.Get(tid); got != 1 {
			t.Fatalf("promoted release clock[%d] = %d, want 1", tid, got)
		}
	}
	if retired := atomicReleaseRetiredForTest(release); len(retired) != 0 {
		t.Fatalf("fresh-TID chain unexpectedly encoded %d retired intervals", len(retired))
	}
}

func TestAtomicReleasePreservesRetiredIntervals(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0xc800)
	const retiredFirst = uint32(100_000)
	const retiredLast = uint32(199_999)

	writer := goroutine.Alloc(90)
	writer.C.RetireRange(retiredFirst, retiredLast)
	completeAtomicSize(d, addr, 8, writer, false, true, 0x2900)
	state := atomicHistoryForTest(t, d, addr)
	release := state.releases[0]
	if release == nil || len(atomicReleaseRetiredForTest(release)) != 1 {
		t.Fatalf("published retired intervals = %+v, want one", release)
	}
	if got := atomicReleaseRetiredForTest(release)[0]; got.First != retiredFirst || got.Last != retiredLast {
		t.Fatalf("published retired interval = [%d,%d], want [%d,%d]", got.First, got.Last, retiredFirst, retiredLast)
	}
	runs := atomicReleaseRunsForTest(release)
	if len(runs) != 1 || runs[0].First != writer.TID || runs[0].Last != writer.TID {
		t.Fatalf("retirement expanded into finite runs: %+v", runs)
	}

	reader := goroutine.Alloc(91)
	completeAtomicSize(d, addr, 8, reader, true, false, 0x2901)
	for _, tid := range []uint32{retiredFirst, retiredFirst + 12_345, retiredLast} {
		if !reader.C.IsRetired(tid) {
			t.Fatalf("atomic acquire did not propagate retired TID %d", tid)
		}
	}

	// A release after the acquire must retain the interval as metadata rather
	// than materializing all 100,000 logical IDs.
	completeAtomicSize(d, addr, 8, reader, true, true, 0x2902)
	republished := state.releases[0]
	republishedRetired := atomicReleaseRetiredForTest(republished)
	if len(republishedRetired) != 1 || republishedRetired[0].First != retiredFirst || republishedRetired[0].Last != retiredLast {
		t.Fatalf("republished retired intervals = %+v, want [%d,%d]", republishedRetired, retiredFirst, retiredLast)
	}
}

func TestAtomicAcquireBulkJoinsFragmentedRelease(t *testing.T) {
	const (
		first = uint32(50_000)
		runs  = 4096
	)
	input := make([]vectorclock.FiniteRange, runs)
	for i := range input {
		tid := first + uint32(i)
		input[i] = vectorclock.FiniteRange{First: tid, Last: tid, Clock: uint32(i&1) + 1}
	}
	a, release := atomicReleaseForTest(input)
	state := atomicState{arena: a}
	state.releases[0] = release
	ctx := goroutine.Alloc(92)
	state.acquire(ctx, 1)
	for _, i := range []int{0, 1, runs / 2, runs - 1} {
		r := input[i]
		if got := ctx.C.Get(r.First); got != r.Clock {
			t.Fatalf("fragmented acquire clock[%d] = %d, want %d", r.First, got, r.Clock)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		state.acquire(ctx, 1)
	}); allocs != 0 {
		t.Fatalf("repeated fragmented atomic acquire allocated %.2f objects per call", allocs)
	}
}

func TestAtomicReleaseOnePassImportMatchesCanonicalVectorClockJoin(t *testing.T) {
	finite := []vectorclock.FiniteRange{
		{First: 1, Last: 4, Clock: 3},
		{First: vectorclock.DenseThreads - 2, Last: vectorclock.DenseThreads + 2, Clock: 5},
		{First: vectorclock.DenseThreads + 3, Last: vectorclock.DenseThreads + 40, Clock: 7},
		{First: ^uint32(0) - 5, Last: ^uint32(0), Clock: 9},
	}
	retired := []vectorclock.RetiredRange{
		{First: 2, Last: 2},
		{First: vectorclock.DenseThreads, Last: vectorclock.DenseThreads + 4},
		{First: ^uint32(0) - 2, Last: ^uint32(0) - 1},
	}
	_, release := atomicReleaseForTestWithRetired(finite, retired)

	actual := vectorclock.New()
	reference := vectorclock.New()
	seed := []vectorclock.FiniteRange{
		{First: 3, Last: 6, Clock: 11},
		{First: vectorclock.DenseThreads + 20, Last: vectorclock.DenseThreads + 50, Clock: 2},
		{First: ^uint32(0) - 8, Last: ^uint32(0) - 7, Clock: 13},
	}
	actual.JoinCanonicalRanges(seed)
	reference.JoinCanonicalRanges(seed)
	actual.RetireRange(vectorclock.DenseThreads+10, vectorclock.DenseThreads+11)
	reference.RetireRange(vectorclock.DenseThreads+10, vectorclock.DenseThreads+11)

	joinAtomicReleaseRanges(actual, release.runs)
	retireAtomicReleaseRanges(actual, release.retired)
	reference.JoinCanonicalRanges(finite)
	reference.RetireRanges(retired)

	if !actual.HappensBefore(reference) || !reference.HappensBefore(actual) {
		t.Fatal("one-pass atomic-release import differs from canonical vector-clock join")
	}
	for _, tid := range []uint32{
		1, 2, 4, 5,
		vectorclock.DenseThreads - 2, vectorclock.DenseThreads, vectorclock.DenseThreads + 4,
		vectorclock.DenseThreads + 10, vectorclock.DenseThreads + 40, vectorclock.DenseThreads + 50,
		^uint32(0) - 8, ^uint32(0) - 2, ^uint32(0),
	} {
		if got, want := actual.Get(tid), reference.Get(tid); got != want {
			t.Fatalf("one-pass atomic-release clock[%d] = %d, want %d", tid, got, want)
		}
	}
}

func TestAtomicReleaseOnePassImportAllocationsStayBoundedWhenFragmented(t *testing.T) {
	measure := func(runCount int) float64 {
		const first = uint32(50_000)
		finite := make([]vectorclock.FiniteRange, runCount)
		retired := make([]vectorclock.RetiredRange, runCount)
		for i := 0; i < runCount; i++ {
			tid := first + uint32(i)
			finite[i] = vectorclock.FiniteRange{First: tid, Last: tid, Clock: uint32(i&1) + 1}
			retiredTID := first + uint32(runCount) + 1 + uint32(i*2)
			retired[i] = vectorclock.RetiredRange{First: retiredTID, Last: retiredTID}
		}
		_, release := atomicReleaseForTestWithRetired(finite, retired)
		var sink uint32
		allocs := testing.AllocsPerRun(10, func() {
			clock := vectorclock.New()
			joinAtomicReleaseRanges(clock, release.runs)
			retireAtomicReleaseRanges(clock, release.retired)
			sink = clock.Get(first)
		})
		if sink != 1 {
			t.Fatalf("fragmented one-pass import clock[%d] = %d, want 1", first, sink)
		}
		return allocs
	}

	medium := measure(256)
	large := measure(4096)
	t.Logf("one-pass fragmented import allocations: 256 runs %.2f, 4096 runs %.2f", medium, large)
	// One exact-size finite buffer and one exact-size retirement buffer make
	// source-side allocation constant in the number of linked ranges. Allow a
	// small fixed difference for destination representation thresholds.
	if large > medium+3 {
		t.Fatalf("one-pass import allocations grew with fragmentation: 256 runs %.2f, 4096 runs %.2f", medium, large)
	}
}

func TestInitializeAtomicReleaseCompositeSharesRootsExactly(t *testing.T) {
	upstream := vectorclock.New()
	upstream.Set(70_001, 17)
	upstream.Set(70_002, 23)
	lineage := vectorclock.NewClockLineage(upstream)
	upstream.Release()
	defer lineage.Release()
	root := lineage.Pin()
	defer root.Release()

	ctx := goroutine.Alloc(70_101)
	defer ctx.C.Release()
	ctx.C.JoinCausal(root)
	expected := ctx.C.Clone()
	defer expected.Release()

	state := atomicState{}
	release := state.allocateRelease()
	if !initializeAtomicReleaseComposite(release, ctx) {
		t.Fatal("composite release initialization rejected a root-plus-own clock")
	}
	defer state.arena.freeReleaseObject(release)
	if release.lineage == nil || !release.view.Valid() || release.importN != 1 {
		t.Fatalf("composite shape = lineage %v, view %v, imports %d; want lineage/view/1 import",
			release.lineage != nil, release.view.Valid(), release.importN)
	}
	if release.runs != nil || release.retired != nil || release.snapshot != nil {
		t.Fatal("composite release retained an eager canonical representation")
	}

	actual := vectorclock.New()
	defer actual.Release()
	joinAtomicReleaseCausalSet(actual, release)
	if !actual.HappensBefore(expected) || !expected.HappensBefore(actual) {
		t.Fatal("composite release differs from the complete source clock")
	}
	if got := actual.Get(ctx.TID); got != uint32(ctx.GetEpoch()) {
		t.Fatalf("composite own clock = %d, want %d", got, ctx.GetEpoch())
	}
	if got := actual.Get(70_001); got != 17 {
		t.Fatalf("composite imported clock = %d, want 17", got)
	}
}

func TestInitializeAtomicReleaseCompositeFallsBackForUncoveredResidual(t *testing.T) {
	upstream := vectorclock.New()
	upstream.Set(71_001, 19)
	lineage := vectorclock.NewClockLineage(upstream)
	upstream.Release()
	defer lineage.Release()
	root := lineage.Pin()
	defer root.Release()

	ctx := goroutine.Alloc(71_101)
	defer ctx.C.Release()
	ctx.C.JoinCausal(root)
	ctx.C.Set(71_201, 29)

	state := atomicState{}
	release := state.allocateRelease()
	defer state.arena.freeReleaseObject(release)
	if initializeAtomicReleaseComposite(release, ctx) {
		t.Fatal("composite release initialization accepted an uncovered residual clock")
	}
	if release.lineage != nil || release.view.Valid() || release.importN != 0 {
		t.Fatal("failed composite initialization mutated the release")
	}
}

func TestAtomicAcquireJoinsCompleteReleaseSnapshot(t *testing.T) {
	state := atomicState{}
	publisher := goroutine.Alloc(94)
	const upstream = uint32(60_000)
	publisher.C.Set(upstream, 17)
	state.publishRelease(publisher, 1)

	release := state.releases[0]
	if release == nil {
		t.Fatal("publication produced no release")
	}

	receiver := goroutine.Alloc(95)
	state.acquire(receiver, 1)
	if got := receiver.C.Get(publisher.TID); got != publisher.C.Get(publisher.TID) {
		t.Fatalf("first acquire publisher clock = %d, want %d", got, publisher.C.Get(publisher.TID))
	}
	if got := receiver.C.Get(upstream); got != 17 {
		t.Fatalf("first acquire causal predecessor = %d, want 17", got)
	}

	// Rejoining a dominated snapshot is idempotent.
	before := receiver.C.Clone()
	defer before.Release()
	state.acquire(receiver, 1)
	if !receiver.C.HappensBefore(before) || !before.HappensBefore(receiver.C) {
		t.Fatal("dominated repeated acquire changed the receiver clock")
	}

	for _, test := range []struct {
		name  string
		prime func(*goroutine.RaceContext)
	}{
		{
			name: "publisher epoch only",
			prime: func(ctx *goroutine.RaceContext) {
				ctx.C.Set(publisher.TID, publisher.C.Get(publisher.TID))
			},
		},
		{
			name: "retired publisher",
			prime: func(ctx *goroutine.RaceContext) {
				ctx.C.RetireRange(publisher.TID, publisher.TID)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			partial := goroutine.Alloc(96)
			test.prime(partial)
			state.acquire(partial, 1)
			if got := partial.C.Get(upstream); got != 17 {
				t.Fatalf("acquire with preexisting publisher coordinate joined upstream clock %d, want 17", got)
			}
		})
	}
}

func TestAtomicWeakAcquireCheckpointsUnrelatedForeignClock(t *testing.T) {
	var state atomicState
	publisher := goroutine.Alloc(120_001)
	state.publishRelease(publisher, 0xff)
	release := state.releases[0]
	stream, version := release.stream, release.version

	const unrelated = uint32(220_001)
	inherited := goroutine.Alloc(120_002)
	// Model an inherited/non-atomic predecessor which is not in the current
	// release. The first acquire is weak and publication must checkpoint the
	// whole context rather than point-publishing only inherited's own lane.
	inherited.C.Set(unrelated, 17)
	state.acquire(inherited, 0xff)
	state.publishRelease(inherited, 0xff)

	if got := state.releases[0]; got != release || got.stream != stream {
		t.Fatalf("weak acquired publication reset lineage: got %p stream %d, want %p stream %d", got, got.stream, release, stream)
	}
	if release.version != version+1 || release.deltaN != 0 {
		t.Fatalf("checkpoint version/deltas = %d/%d, want %d/0", release.version, release.deltaN, version+1)
	}
	receiver := goroutine.Alloc(120_003)
	state.acquire(receiver, 0xff)
	if got := receiver.C.Get(unrelated); got != 17 {
		t.Fatalf("checkpoint lost unrelated clock: got %d, want 17", got)
	}
}

func TestAtomicNonAcquiringStoreReplacesPriorCompositeRelease(t *testing.T) {
	var state atomicState
	first := goroutine.Alloc(121_001)
	const predecessor = uint32(221_001)
	first.C.Set(predecessor, 23)
	state.publishRelease(first, 0xff)
	oldStream := state.releases[0].stream

	staleStore := goroutine.Alloc(121_002)
	state.publishRelease(staleStore, 0xff)
	if got := state.releases[0].stream; got == oldStream {
		t.Fatalf("non-acquiring store retained superseded stream %d", got)
	}
	receiver := goroutine.Alloc(121_003)
	state.acquire(receiver, 0xff)
	if got := receiver.C.Get(first.TID); got != 0 {
		t.Fatalf("replacement retained first publisher clock %d", got)
	}
	if got := receiver.C.Get(predecessor); got != 0 {
		t.Fatalf("replacement max-accumulated predecessor clock %d", got)
	}
	if got := receiver.C.Get(staleStore.TID); got != staleStore.C.Get(staleStore.TID) {
		t.Fatalf("replacement lost current publisher clock %d", got)
	}
}

func TestAtomicAlternatingRMWOwnersRemainOneDeltaStream(t *testing.T) {
	var state atomicState
	left := goroutine.Alloc(130_001)
	right := goroutine.Alloc(130_101)
	state.publishRelease(left, 0xff)
	left.IncrementClock()
	stream := state.releases[0].stream

	for i := 0; i < 160; i++ {
		ctx := left
		if i&1 == 0 {
			ctx = right
		}
		state.acquire(ctx, 0xff)
		state.publishRelease(ctx, 0xff)
		ctx.IncrementClock()
		if got := state.releases[0].stream; got != stream {
			t.Fatalf("owner switch %d reset stream to %d, want %d", i, got, stream)
		}
	}

	release := state.releases[0]
	if release.deltaN != atomicReleaseDeltaCapacity {
		t.Fatalf("bounded suffix contains %d deltas, want %d", release.deltaN, atomicReleaseDeltaCapacity)
	}
	reference := goroutine.Alloc(130_201)
	state.acquire(reference, 0xff)
	for _, ctx := range []*goroutine.RaceContext{left, right} {
		want := ctx.C.Get(ctx.TID) - 1
		if got := reference.C.Get(ctx.TID); got != want {
			t.Fatalf("forced full reference clock[%d] = %d, want %d", ctx.TID, got, want)
		}
	}
}

func TestAtomicReleaseDeltaReplayCapacityBoundary(t *testing.T) {
	var state atomicState
	left := goroutine.Alloc(135_001)
	right := goroutine.Alloc(135_101)
	atCapacity := goroutine.Alloc(135_201)
	beyondCapacity := goroutine.Alloc(135_301)
	state.publishRelease(left, 1)
	left.IncrementClock()
	release := state.releases[0]
	initialVersion := release.version

	// Prime two readers with the same canonical base. Exactly capacity point
	// updates remain replayable; the next update evicts the first required
	// delta and must force an exact canonical join.
	state.acquire(atCapacity, 1)
	state.acquire(beyondCapacity, 1)
	for i := 0; i < atomicReleaseDeltaCapacity; i++ {
		ctx := left
		if i&1 == 0 {
			ctx = right
		}
		state.acquire(ctx, 1)
		state.publishRelease(ctx, 1)
		ctx.IncrementClock()
	}

	if complete, foreign := release.replay(atCapacity, initialVersion); !complete || !foreign {
		t.Fatalf("capacity-boundary replay = complete %v, foreign %v; want true, true", complete, foreign)
	}
	for _, ctx := range []*goroutine.RaceContext{left, right} {
		want := releaseClockForTest(release, ctx.TID)
		if got := atCapacity.C.Get(ctx.TID); got != want {
			t.Fatalf("capacity-boundary clock[%d] = %d, want %d", ctx.TID, got, want)
		}
	}

	state.acquire(right, 1)
	state.publishRelease(right, 1)
	right.IncrementClock()
	if complete, _ := release.replay(beyondCapacity, initialVersion); complete {
		t.Fatal("replay beyond the retained delta suffix unexpectedly completed")
	}
	state.acquire(beyondCapacity, 1)
	for _, ctx := range []*goroutine.RaceContext{left, right} {
		want := releaseClockForTest(release, ctx.TID)
		if got := beyondCapacity.C.Get(ctx.TID); got != want {
			t.Fatalf("post-fallback clock[%d] = %d, want %d", ctx.TID, got, want)
		}
	}
}

func releaseClockForTest(release *atomicRelease, tid uint32) uint32 {
	if release.lineage != nil {
		clock := release.view.Get(tid)
		for i := uint8(0); i < release.importN; i++ {
			if imported := release.imports[i].Get(tid); imported > clock {
				clock = imported
			}
		}
		if release.deferred != nil {
			deferred := vectorclock.New()
			release.deferred.JoinInto(deferred)
			if projected := deferred.Get(tid); projected > clock {
				clock = projected
			}
			deferred.Release()
		}
		return clock
	}
	if release.snapshot != nil {
		clock := vectorclock.New()
		clock.JoinSnapshot(release.snapshot)
		return clock.Get(tid)
	}
	for run := release.runs; run != nil; run = run.next {
		if run.first <= tid && tid <= run.last {
			return run.clock
		}
	}
	return 0
}

func TestAtomicReleasePointMaxMaintainsCanonicalRanges(t *testing.T) {
	a, release := atomicReleaseForTest([]vectorclock.FiniteRange{
		{First: 10, Last: 14, Clock: 1},
		{First: 16, Last: 16, Clock: 3},
	})
	_ = a
	if !pointMaxAtomicRelease(release, 12, 2) {
		t.Fatal("interior point max reported no change")
	}
	want := []vectorclock.FiniteRange{
		{First: 10, Last: 11, Clock: 1},
		{First: 12, Last: 12, Clock: 2},
		{First: 13, Last: 14, Clock: 1},
		{First: 16, Last: 16, Clock: 3},
	}
	if got := atomicReleaseRunsForTest(release); !reflect.DeepEqual(got, want) {
		t.Fatalf("interior point ranges = %+v, want %+v", got, want)
	}
	if !pointMaxAtomicRelease(release, 15, 3) {
		t.Fatal("gap point max reported no change")
	}
	want = []vectorclock.FiniteRange{
		{First: 10, Last: 11, Clock: 1},
		{First: 12, Last: 12, Clock: 2},
		{First: 13, Last: 14, Clock: 1},
		{First: 15, Last: 16, Clock: 3},
	}
	if got := atomicReleaseRunsForTest(release); !reflect.DeepEqual(got, want) {
		t.Fatalf("gap merge ranges = %+v, want %+v", got, want)
	}
	if pointMaxAtomicRelease(release, 12, 1) {
		t.Fatal("decreasing point update changed canonical release")
	}
}

func TestAtomicReleaseLineagePromotionThreshold(t *testing.T) {
	ranges := make([]vectorclock.FiniteRange, atomicReleasePromotionCoordinateThreshold)
	ctx := goroutine.Alloc(139_001)
	for i := range ranges {
		tid := uint32(200_000 + i*2)
		ranges[i] = vectorclock.FiniteRange{First: tid, Last: tid, Clock: uint32(i + 1)}
		ctx.C.Set(tid, uint32(i+1))
	}
	_, release := atomicReleaseForTest(ranges)
	for _, version := range []uint64{63, 65} {
		release.version = version
		maybePromoteAtomicRelease(release, ctx)
		if release.lineage != nil {
			t.Fatalf("version %d promoted outside the periodic check", version)
		}
	}
	release.version = 64
	maybePromoteAtomicRelease(release, ctx)
	if release.lineage == nil || !release.view.Valid() {
		t.Fatal("64-range release did not promote at version 64")
	}
	root := release.lineage
	maybePromoteAtomicRelease(release, ctx)
	if release.lineage != root {
		t.Fatal("already-promoted release replaced its lineage root")
	}
}

func TestAtomicReleaseContiguousCoordinatesPromote(t *testing.T) {
	_, release := atomicReleaseForTest([]vectorclock.FiniteRange{{
		First: 400_000,
		Last:  400_000 + atomicReleasePromotionCoordinateThreshold - 1,
		Clock: 1,
	}})
	release.version = atomicReleaseSnapshotCheckPeriod
	maybePromoteAtomicRelease(release, goroutine.Alloc(139_049))
	if release.lineage == nil || !release.view.Valid() {
		t.Fatal("contiguous many-owner release did not promote")
	}
}

func TestAtomicReleasePromotionUsesAuthoritativeCanonicalRanges(t *testing.T) {
	ranges := make([]vectorclock.FiniteRange, atomicReleasePromotionCoordinateThreshold)
	ctx := goroutine.Alloc(139_050)
	for i := range ranges {
		ranges[i] = vectorclock.FiniteRange{
			First: uint32(210_000 + i*2),
			Last:  uint32(210_000 + i*2),
			Clock: uint32(i%5 + 1),
		}
	}
	// This coordinate is deliberately absent from the release. Promotion must
	// snapshot the continuously-authoritative arena ranges, not the caller.
	ctx.C.Set(999_999, 77)
	_, release := atomicReleaseForTest(ranges)
	release.version = atomicReleaseSnapshotCheckPeriod
	maybePromoteAtomicRelease(release, ctx)
	if release.lineage == nil || !release.view.Valid() {
		t.Fatal("fragmented release did not promote")
	}
	imported := vectorclock.New()
	imported.JoinCausal(release.view)
	if got := imported.Get(999_999); got != 0 {
		t.Fatalf("promotion imported incidental publisher coordinate = %d, want 0", got)
	}
	for _, r := range ranges {
		if got := imported.Get(r.First); got != r.Clock {
			t.Fatalf("promoted canonical clock[%d] = %d, want %d", r.First, got, r.Clock)
		}
	}
}

func TestAtomicPromotedPointUpdatesStayEqualToCanonical(t *testing.T) {
	ranges := make([]vectorclock.FiniteRange, atomicReleasePromotionCoordinateThreshold)
	for i := range ranges {
		ranges[i] = vectorclock.FiniteRange{
			First: uint32(220_000 + i*2),
			Last:  uint32(220_000 + i*2),
			Clock: 3,
		}
	}
	ctx := goroutine.Alloc(139_060)
	_, release := atomicReleaseForTest(ranges)
	release.version = atomicReleaseSnapshotCheckPeriod
	maybePromoteAtomicRelease(release, ctx)
	updates := []struct {
		tid, clock uint32
	}{
		{220_001, 3}, // bridge and coalesce two equal canonical runs
		{220_010, 9}, // split an existing run
		{^uint32(0), 11},
	}
	for _, update := range updates {
		if !pointMaxAtomicRelease(release, update.tid, update.clock) {
			t.Fatalf("point update (%d,%d) was unexpectedly dominated", update.tid, update.clock)
		}
		if _, appended := release.lineage.AppendOwned(&release.view, update.tid, update.clock); !appended {
			t.Fatalf("lineage point update (%d,%d) was unexpectedly dominated", update.tid, update.clock)
		}
		canonical, promoted := vectorclock.New(), vectorclock.New()
		joinAtomicReleaseRanges(canonical, release.runs)
		retireAtomicReleaseRanges(canonical, release.retired)
		promoted.JoinCausal(release.view)
		var canonicalRuns, promotedRuns []vectorclock.FiniteRange
		canonical.RangeRuns(func(first, last, clock uint32) bool {
			canonicalRuns = append(canonicalRuns, vectorclock.FiniteRange{First: first, Last: last, Clock: clock})
			return true
		})
		promoted.RangeRuns(func(first, last, clock uint32) bool {
			promotedRuns = append(promotedRuns, vectorclock.FiniteRange{First: first, Last: last, Clock: clock})
			return true
		})
		if !reflect.DeepEqual(promotedRuns, canonicalRuns) {
			t.Fatalf("point update (%d,%d) diverged\ncanonical: %+v\npromoted:  %+v", update.tid, update.clock, canonicalRuns, promotedRuns)
		}
	}
}

func TestAtomicPromotedAcquireMatchesCanonicalFallback(t *testing.T) {
	ranges := make([]vectorclock.FiniteRange, 80)
	checkpoint := vectorclock.New()
	for i := range ranges {
		tid := uint32(300_000 + i*2)
		clock := uint32(i%7 + 1)
		ranges[i] = vectorclock.FiniteRange{First: tid, Last: tid, Clock: clock}
		checkpoint.Set(tid, clock)
	}
	legacyArena, legacyRelease := atomicReleaseForTest(ranges)
	promotedArena, promotedRelease := atomicReleaseForTest(ranges)
	legacyRelease.refs, promotedRelease.refs = 1, 1
	legacyRelease.stream, promotedRelease.stream = newAtomicReleaseStream(), newAtomicReleaseStream()
	legacyRelease.version, promotedRelease.version = 64, 64
	promotedRelease.lineage = vectorclock.NewClockLineage(checkpoint)
	promotedRelease.view = promotedRelease.lineage.Pin()
	legacy := atomicState{arena: legacyArena}
	promoted := atomicState{arena: promotedArena}
	legacy.releases[0], promoted.releases[0] = legacyRelease, promotedRelease
	want, got := goroutine.Alloc(139_102), goroutine.Alloc(139_103)
	legacy.acquire(want, 1)
	promoted.acquire(got, 1)
	for _, r := range ranges {
		if a, b := want.C.Get(r.First), got.C.Get(r.First); a != b || a != r.Clock {
			t.Fatalf("promoted clock[%d] = %d, canonical = %d, want %d", r.First, b, a, r.Clock)
		}
	}
}

func lineageAtomicStateForTest(anchor *vectorclock.VectorClock) (*AtomicHistoryArena, *atomicState, *atomicRelease) {
	a := newAtomicHistoryArena()
	release := a.allocRelease()
	release.stream = newAtomicReleaseStream()
	release.version = atomicReleaseSnapshotCheckPeriod
	release.refs = 1
	release.lineage = vectorclock.NewClockLineage(anchor)
	release.view = release.lineage.Pin()
	state := &atomicState{arena: a}
	state.releases[0] = release
	return a, state, release
}

func TestAtomicPromotedStrongAppendAndAcquire(t *testing.T) {
	publisher := goroutine.Alloc(139_110)
	_, state, release := lineageAtomicStateForTest(publisher.C)
	state.acquire(publisher, 1)
	before := release.version
	publisher.IncrementClock()
	state.publishRelease(publisher, 1)
	if release.version != before+1 {
		t.Fatalf("strong append version = %d, want %d", release.version, before+1)
	}
	if got, want := release.view.Get(publisher.TID), uint32(publisher.GetEpoch()); got != want {
		t.Fatalf("strong append clock = %d, want %d", got, want)
	}
	receiver := goroutine.Alloc(139_111)
	state.acquire(receiver, 1)
	if got, want := receiver.C.Get(publisher.TID), uint32(publisher.GetEpoch()); got != want {
		t.Fatalf("promoted acquire clock = %d, want %d", got, want)
	}
}

func TestAtomicPromotedDominatedResidualRestoresStrongProof(t *testing.T) {
	anchor := vectorclock.New()
	const predecessor = uint32(239_115)
	anchor.Set(predecessor, 9)
	_, state, release := lineageAtomicStateForTest(anchor)
	ctx := goroutine.Alloc(139_115)
	ctx.C.Set(predecessor, 7)
	state.acquire(ctx, 1)
	_, strong, ok := ctx.LookupAtomicRelease(unsafe.Pointer(release), release.stream, 1)
	if !ok || !strong {
		t.Fatalf("dominated residual acquire proof = present %v strong %v, want true/true", ok, strong)
	}

	// A later unrelated acquire can invalidate the global generation without
	// adding anything beyond this release. The exact current-version acquire
	// must restore the strong proof without a lineage join.
	ctx.NoteForeignImport()
	if _, strong, _ := ctx.LookupAtomicRelease(unsafe.Pointer(release), release.stream, 1); strong {
		t.Fatal("test setup did not weaken promoted release proof")
	}
	state.acquire(ctx, 1)
	if _, strong, _ := ctx.LookupAtomicRelease(unsafe.Pointer(release), release.stream, 1); !strong {
		t.Fatal("dominated exact-version acquire did not restore strong proof")
	}

	before := release.version
	ctx.IncrementClock()
	state.publishRelease(ctx, 1)
	if release.version != before+1 || release.view.Get(predecessor) != 9 {
		t.Fatalf("dominated residual point publish = version %d predecessor %d, want %d/9", release.version, release.view.Get(predecessor), before+1)
	}
}

func TestAtomicPromotedStaleContextAcrossRotation(t *testing.T) {
	publisher := goroutine.Alloc(139_120)
	_, state, release := lineageAtomicStateForTest(publisher.C)
	state.acquire(publisher, 1)
	stale := goroutine.Alloc(139_121)
	state.acquire(stale, 1)
	old, ok := release.view.Duplicate()
	if !ok {
		t.Fatal("failed to pin stale promoted view")
	}
	defer old.Release()

	for i := 0; i < 4200; i++ {
		publisher.IncrementClock()
		state.publishRelease(publisher, 1)
	}
	if release.view.SameFamily(old) == false {
		t.Fatal("rotation changed release lineage family")
	}
	state.acquire(stale, 1)
	if got, want := stale.C.Get(publisher.TID), release.view.Get(publisher.TID); got != want {
		t.Fatalf("stale post-rotation acquire clock = %d, want %d", got, want)
	}
}

func TestAtomicPromotedWeakForeignCheckpoint(t *testing.T) {
	publisher := goroutine.Alloc(139_130)
	_, state, release := lineageAtomicStateForTest(publisher.C)
	old, ok := release.view.Duplicate()
	if !ok {
		t.Fatal("failed to pin old promoted view")
	}
	defer old.Release()
	before := release.version

	const unrelated = uint32(239_130)
	inherited := goroutine.Alloc(139_131)
	inherited.C.Set(unrelated, 17)
	state.acquire(inherited, 1)
	if _, strong, ok := inherited.LookupAtomicRelease(unsafe.Pointer(release), release.stream, 1); !ok || strong {
		t.Fatalf("undominated residual proof = present %v strong %v, want true/false", ok, strong)
	}
	state.publishRelease(inherited, 1)
	if release.version != before+1 {
		t.Fatalf("weak checkpoint version = %d, want %d", release.version, before+1)
	}
	if !release.view.SameFamily(old) || !release.view.Dominates(old) {
		t.Fatal("weak checkpoint did not advance the existing lineage family")
	}
	receiver := goroutine.Alloc(139_132)
	state.acquire(receiver, 1)
	if got := receiver.C.Get(unrelated); got != 17 {
		t.Fatalf("weak checkpoint lost foreign clock = %d, want 17", got)
	}
}

func TestAtomicValueStyleTwoAddressOnboardingUsesPointPublicationAfterWarmup(t *testing.T) {
	const typeTID = uint32(239_135)
	typeArena, typeRelease := atomicReleaseForTest([]vectorclock.FiniteRange{{First: typeTID, Last: typeTID, Clock: 7}})
	typeRelease.refs = 1
	typeRelease.stream = newAtomicReleaseStream()
	typeRelease.version = 1
	typeState := atomicState{arena: typeArena}
	typeState.releases[0] = typeRelease

	dataAnchor := vectorclock.New()
	_, dataState, dataRelease := lineageAtomicStateForTest(dataAnchor)
	// The first context imports a genuinely new type coordinate, so its first
	// data publication must checkpoint that residual into the data lineage.
	warm := goroutine.Alloc(139_135)
	typeState.acquire(warm, 1)
	dataState.acquire(warm, 1)
	if _, strong, _ := warm.LookupAtomicRelease(unsafe.Pointer(dataRelease), dataRelease.stream, 1); strong {
		t.Fatal("cold Value-style data acquire unexpectedly had a strong proof")
	}
	dataState.publishRelease(warm, 1)
	if got := releaseClockForTest(dataRelease, typeTID); got != 7 {
		t.Fatalf("Value-style warmup lost type predecessor = %d, want 7", got)
	}

	// Every later fresh context first imports the same type release. That import
	// invalidates global generations, but its residual is already dominated by
	// the data lineage and must use O(1) point publication rather than reanchor.
	for i := uint32(0); i < 128; i++ {
		ctx := goroutine.Alloc(140_000 + i)
		typeState.acquire(ctx, 1)
		dataState.acquire(ctx, 1)
		if _, strong, ok := ctx.LookupAtomicRelease(unsafe.Pointer(dataRelease), dataRelease.stream, 1); !ok || !strong {
			t.Fatalf("Value-style owner %d proof = present %v strong %v, want true/true", i, ok, strong)
		}
		before := dataRelease.version
		dataState.publishRelease(ctx, 1)
		if dataRelease.version != before+1 {
			t.Fatalf("Value-style owner %d publication version = %d, want %d", i, dataRelease.version, before+1)
		}
	}
}

func promotedSourceForTest(predecessor, clock uint32) (*atomicState, *atomicRelease) {
	anchor := vectorclock.New()
	anchor.Set(predecessor, clock)
	_, state, release := lineageAtomicStateForTest(anchor)
	return state, release
}

func TestAtomicValueDataCountWaitGroupStyleThreeFamilyOnboarding(t *testing.T) {
	// Model three independently promoted synchronization words feeding one
	// long-lived data release: an atomic.Value type word, a public atomic count,
	// and the release merged by WaitGroup before its synchronizing handoff.
	const (
		typeTID  = uint32(239_136)
		countTID = uint32(239_137)
		waitTID  = uint32(239_138)
	)
	typeState, _ := promotedSourceForTest(typeTID, 7)
	countState, _ := promotedSourceForTest(countTID, 11)
	waitState, _ := promotedSourceForTest(waitTID, 13)

	dataAnchor := vectorclock.New()
	_, dataState, dataRelease := lineageAtomicStateForTest(dataAnchor)
	primary, ok := dataRelease.view.Duplicate()
	if !ok {
		t.Fatal("failed to pin data release primary")
	}
	defer primary.Release()

	warm := goroutine.Alloc(139_139)
	typeState.acquire(warm, 1)
	countState.acquire(warm, 1)
	waitState.acquire(warm, 1)
	dataState.acquire(warm, 1)
	if _, strong, ok := warm.LookupAtomicRelease(unsafe.Pointer(dataRelease), dataRelease.stream, 1); !ok || strong {
		t.Fatalf("three-family cold proof = present %v strong %v, want true/false", ok, strong)
	}
	warm.IncrementClock()
	before := dataRelease.version
	dataState.publishRelease(warm, 1)
	if dataRelease.version != before+1 || dataRelease.importN != 3 {
		t.Fatalf("three-family onboarding = version %d imports %d, want %d/3", dataRelease.version, dataRelease.importN, before+1)
	}
	if !dataRelease.view.SameFamily(primary) {
		t.Fatal("three-family onboarding reanchored the primary lineage")
	}
	for _, coordinate := range [...]struct {
		tid, want uint32
	}{{typeTID, 7}, {countTID, 11}, {waitTID, 13}} {
		tid, want := coordinate.tid, coordinate.want
		if got := releaseClockForTest(dataRelease, tid); got != want {
			t.Fatalf("composite release clock[%d] = %d, want %d", tid, got, want)
		}
	}
	// Weakening the cache without adding a logical coordinate must not create a
	// synthetic composite release version.
	before = dataRelease.version
	warm.NoteForeignImport()
	dataState.publishRelease(warm, 1)
	if dataRelease.version != before {
		t.Fatalf("unchanged composite version = %d, want %d", dataRelease.version, before)
	}

	// Once all three families are onboarded, a fresh owner gets a strong proof
	// and publishes by appending only its own point to the primary lineage.
	next := goroutine.Alloc(139_140)
	typeState.acquire(next, 1)
	countState.acquire(next, 1)
	waitState.acquire(next, 1)
	dataState.acquire(next, 1)
	if _, strong, ok := next.LookupAtomicRelease(unsafe.Pointer(dataRelease), dataRelease.stream, 1); !ok || !strong {
		t.Fatalf("three-family warm proof = present %v strong %v, want true/true", ok, strong)
	}
	next.IncrementClock()
	before = dataRelease.version
	dataState.publishRelease(next, 1)
	if dataRelease.version != before+1 || dataRelease.importN != 3 || !dataRelease.view.SameFamily(primary) {
		t.Fatalf("three-family warm publish = version %d imports %d primary-family %v", dataRelease.version, dataRelease.importN, dataRelease.view.SameFamily(primary))
	}
}

func TestAtomicPromotedCompositeStaleVersionAndHistoricalImportPin(t *testing.T) {
	const predecessor = uint32(239_145)
	sourceOwner := goroutine.Alloc(139_145)
	sourceOwner.C.Set(predecessor, 5)
	_, sourceState, sourceRelease := lineageAtomicStateForTest(sourceOwner.C)
	_, dataState, dataRelease := lineageAtomicStateForTest(vectorclock.New())

	warm := goroutine.Alloc(139_146)
	sourceState.acquire(warm, 1)
	dataState.acquire(warm, 1)
	warm.IncrementClock()
	dataState.publishRelease(warm, 1)
	if dataRelease.importN != 1 {
		t.Fatalf("initial composite imports = %d, want 1", dataRelease.importN)
	}
	historical, ok := dataRelease.imports[0].Duplicate()
	if !ok {
		t.Fatal("failed to pin historical composite import")
	}
	defer historical.Release()

	stale := goroutine.Alloc(139_147)
	dataState.acquire(stale, 1)
	staleVersion, _, _ := stale.LookupAtomicRelease(unsafe.Pointer(dataRelease), dataRelease.stream, 1)

	// Advance the imported family, then weak-publish it into the data release.
	if _, changed := sourceRelease.lineage.AppendOwned(&sourceRelease.view, predecessor, 17); !changed {
		t.Fatal("failed to advance source family")
	}
	bumpAtomicReleaseVersion(sourceRelease)
	updater := goroutine.Alloc(139_148)
	sourceState.acquire(updater, 1)
	dataState.acquire(updater, 1)
	updater.IncrementClock()
	dataState.publishRelease(updater, 1)
	if dataRelease.version <= staleVersion || dataRelease.imports[0].Get(predecessor) != 17 {
		t.Fatalf("advanced composite = version %d clock %d, stale version %d", dataRelease.version, dataRelease.imports[0].Get(predecessor), staleVersion)
	}
	if got := historical.Get(predecessor); got != 5 {
		t.Fatalf("historical import pin advanced to %d, want 5", got)
	}

	dataState.acquire(stale, 1)
	if got := stale.C.Get(predecessor); got != 17 {
		t.Fatalf("stale composite acquire clock = %d, want 17", got)
	}
	seen, _, _ := stale.LookupAtomicRelease(unsafe.Pointer(dataRelease), dataRelease.stream, 1)
	if seen != dataRelease.version {
		t.Fatalf("stale composite cache version = %d, want %d", seen, dataRelease.version)
	}
	_ = sourceRelease
}

func TestAtomicPromotedCompositeOverflowFallsBackToExactDeferredProjection(t *testing.T) {
	_, dataState, dataRelease := lineageAtomicStateForTest(vectorclock.New())
	var sources [4]*atomicState
	for i := range sources {
		sources[i], _ = promotedSourceForTest(uint32(239_160+i), uint32(20+i))
	}

	warm := goroutine.Alloc(139_170)
	for i := 0; i < 3; i++ {
		sources[i].acquire(warm, 1)
	}
	dataState.acquire(warm, 1)
	warm.IncrementClock()
	dataState.publishRelease(warm, 1)
	if dataRelease.importN != 3 {
		t.Fatalf("overflow setup imports = %d, want 3", dataRelease.importN)
	}
	oldPrimary, ok := dataRelease.view.Duplicate()
	if !ok {
		t.Fatal("failed to pin pre-overflow primary")
	}
	defer oldPrimary.Release()

	overflow := goroutine.Alloc(139_171)
	dataState.acquire(overflow, 1)
	sources[3].acquire(overflow, 1)
	overflow.IncrementClock()
	before := dataRelease.version
	dataState.publishRelease(overflow, 1)
	if dataRelease.version != before+1 || dataRelease.importN != 0 {
		t.Fatalf("overflow fallback = version %d imports %d, want %d/0", dataRelease.version, dataRelease.importN, before+1)
	}
	if dataRelease.deferred == nil {
		t.Fatal("overflow fallback did not retain deferred exact projection")
	}
	deferred := dataRelease.deferred
	if !dataRelease.view.SameFamily(oldPrimary) {
		t.Fatal("overflow fallback replaced primary family")
	}
	for i := range sources {
		tid, want := uint32(239_160+i), uint32(20+i)
		if got := releaseClockForTest(dataRelease, tid); got != want {
			t.Fatalf("overflow fallback clock[%d] = %d, want %d", tid, got, want)
		}
	}

	// A later weak publication replaces the exact deferred image under the
	// same state lock. The wrapper and its owned buffers are reusable; retaining
	// the prior object would recreate the allocation churn this fallback avoids.
	const laterTID = uint32(239_199)
	overflow.C.Set(laterTID, 41)
	overflow.NoteForeignImport()
	overflow.IncrementClock()
	dataState.publishRelease(overflow, 1)
	if dataRelease.deferred != deferred {
		t.Fatal("repeated weak publication replaced deferred projection wrapper")
	}
	if got := releaseClockForTest(dataRelease, laterTID); got != 41 {
		t.Fatalf("repinned fallback clock[%d] = %d, want 41", laterTID, got)
	}
}

func TestAtomicPromotedCompositePublisherRootCapacityFallsBackExactly(t *testing.T) {
	_, _, release := lineageAtomicStateForTest(vectorclock.New())
	ctx := goroutine.Alloc(139_172)
	for i := 0; i < vectorclock.CausalRootCapacity; i++ {
		source, _ := promotedSourceForTest(uint32(239_180+i), uint32(30+i))
		source.acquire(ctx, 1)
	}
	if got := ctx.C.BorrowCausalRoots(new([vectorclock.CausalRootCapacity]vectorclock.CausalView)); got != vectorclock.CausalRootCapacity {
		t.Fatalf("publisher roots = %d, want full capacity %d", got, vectorclock.CausalRootCapacity)
	}
	projection := vectorclock.PinReleaseProjection(ctx.C)
	release.deferred = new(vectorclock.ReleaseProjection)
	*release.deferred = projection
	ctx.IncrementClock()

	if !publishPromotedComposite(release, ctx) {
		t.Fatal("deferred projection did not discharge full publisher root set")
	}
	if got, want := release.view.Get(ctx.TID), uint32(ctx.GetEpoch()); got != want {
		t.Fatalf("release owner clock = %d, want %d", got, want)
	}
	if got, want := ctx.C.Get(ctx.TID), uint32(ctx.GetEpoch()); got != want {
		t.Fatalf("publisher owner clock = %d, want %d", got, want)
	}
	for i := 0; i < vectorclock.CausalRootCapacity; i++ {
		tid, want := uint32(239_180+i), uint32(30+i)
		if got := ctx.C.Get(tid); got != want {
			t.Fatalf("publisher clock[%d] = %d, want %d", tid, got, want)
		}
	}
}

func TestAtomicPromotedReplacementStartsNewFamily(t *testing.T) {
	publisher := goroutine.Alloc(139_140)
	const predecessor = uint32(239_140)
	publisher.C.Set(predecessor, 23)
	_, state, oldRelease := lineageAtomicStateForTest(publisher.C)
	old, ok := oldRelease.view.Duplicate()
	if !ok {
		t.Fatal("failed to pin old promoted view")
	}
	defer old.Release()
	oldStream := oldRelease.stream

	replacement := goroutine.Alloc(139_141)
	const replacementPredecessor = uint32(239_141)
	replacement.C.Set(replacementPredecessor, 29)
	state.publishRelease(replacement, 1)
	current := state.releases[0]
	if current == nil || current.lineage == nil || !current.view.Valid() {
		t.Fatal("promoted replacement did not retain scalable representation")
	}
	if current.stream == oldStream || current.view.SameFamily(old) {
		t.Fatal("non-acquiring replacement retained the predecessor family")
	}
	if got := current.view.Get(predecessor); got != 0 {
		t.Fatalf("replacement retained predecessor clock = %d", got)
	}
	if got, want := current.view.Get(replacement.TID), uint32(replacement.GetEpoch()); got != want {
		t.Fatalf("replacement publisher clock = %d, want %d", got, want)
	}
	if current.deferred == nil || current.view.Get(replacementPredecessor) != 0 {
		t.Fatal("replacement did not keep foreign source metadata exclusively deferred")
	}
	if got := releaseClockForTest(current, replacementPredecessor); got != 29 {
		t.Fatalf("replacement exact clock[%d] = %d, want 29", replacementPredecessor, got)
	}
	receiver := goroutine.Alloc(139_142)
	state.acquire(receiver, 1)
	if got := receiver.C.Get(replacementPredecessor); got != 29 {
		t.Fatalf("replacement acquire clock[%d] = %d, want 29", replacementPredecessor, got)
	}
}

func TestAtomicPromotedSteadyPublishDoesNotAllocate(t *testing.T) {
	publisher := goroutine.Alloc(139_150)
	_, state, release := lineageAtomicStateForTest(publisher.C)
	state.acquire(publisher, 1)
	if !release.lineage.Reserve(2048) {
		t.Fatal("failed to reserve promoted steady segment")
	}
	allocs := testing.AllocsPerRun(1, func() {
		for i := 0; i < 512; i++ {
			publisher.IncrementClock()
			state.publishRelease(publisher, 1)
		}
	})
	if allocs != 0 {
		t.Fatalf("promoted steady append allocated %.2f objects", allocs)
	}
}

func TestAtomicRecycledReleaseDropsCausalResources(t *testing.T) {
	var state atomicState
	ctx := goroutine.Alloc(139_201)
	state.publishRelease(ctx, 1)
	release := state.releases[0]
	release.lineage = vectorclock.NewClockLineage(ctx.C)
	release.view = release.lineage.Pin()
	importLineage := vectorclock.NewClockLineage(ctx.C)
	release.imports[0] = importLineage.Pin()
	release.importN = 1
	deferredClock := vectorclock.New()
	deferredClock.JoinCausal(release.imports[0])
	projection := vectorclock.PinReleaseProjection(deferredClock)
	release.deferred = new(vectorclock.ReleaseProjection)
	*release.deferred = projection
	state.retireReleases(1)
	if release.lineage != nil || release.view.Valid() || release.importN != 0 || release.imports[0].Valid() || release.deferred != nil {
		t.Fatal("arena recycle retained causal release resources")
	}
	deferredClock.Release()
	importLineage.Release()
}

func TestAtomicArenaResetDropsReleaseCausalOwnership(t *testing.T) {
	ctx := goroutine.Alloc(139_202)
	a, _, release := lineageAtomicStateForTest(ctx.C)
	importLineage := vectorclock.NewClockLineage(ctx.C)
	release.imports[0] = importLineage.Pin()
	release.importN = 1
	deferredClock := vectorclock.New()
	deferredClock.JoinCausal(release.imports[0])
	projection := vectorclock.PinReleaseProjection(deferredClock)
	release.deferred = new(vectorclock.ReleaseProjection)
	*release.deferred = projection
	importPinned, ok := release.imports[0].Duplicate()
	if !ok {
		t.Fatal("failed to pin imported release before reset")
	}
	pinned, ok := release.view.Duplicate()
	if !ok {
		t.Fatal("failed to pin promoted release before reset")
	}
	a.reset()
	if release.lineage != nil || release.view.Valid() || release.importN != 0 || release.imports[0].Valid() || release.deferred != nil {
		t.Fatal("arena reset retained release causal ownership")
	}
	if got, want := pinned.Get(ctx.TID), uint32(ctx.GetEpoch()); got != want {
		t.Fatalf("independent pin after reset = %d, want %d", got, want)
	}
	pinned.Release()
	if got, want := importPinned.Get(ctx.TID), uint32(ctx.GetEpoch()); got != want {
		t.Fatalf("independent import pin after reset = %d, want %d", got, want)
	}
	importPinned.Release()
	deferredClock.Release()
	importLineage.Release()
}

func TestAtomicStaleCacheBeyondDeltaRingFullJoinsAndPointPublishes(t *testing.T) {
	var state atomicState
	left := goroutine.Alloc(140_001)
	right := goroutine.Alloc(140_101)
	stale := goroutine.Alloc(140_201)
	state.publishRelease(left, 1)
	left.IncrementClock()
	stream := state.releases[0].stream
	state.acquire(stale, 1)
	stale.IncrementClock()

	for i := 0; i < atomicReleaseDeltaCapacity+16; i++ {
		ctx := left
		if i&1 == 0 {
			ctx = right
		}
		state.acquire(ctx, 1)
		state.publishRelease(ctx, 1)
		ctx.IncrementClock()
	}
	release := state.releases[0]
	before := release.version
	state.acquire(stale, 1)
	if got := stale.C.Get(right.TID); got != right.C.Get(right.TID)-1 {
		t.Fatalf("stale full join clock[%d] = %d, want %d", right.TID, got, right.C.Get(right.TID)-1)
	}
	state.publishRelease(stale, 1)
	if release.stream != stream || release.version != before+1 {
		t.Fatalf("post-fallback publication = stream/version %d/%d, want %d/%d", release.stream, release.version, stream, before+1)
	}
}

func TestAtomicRecycledReleasePointerCannotHitOldStream(t *testing.T) {
	var state atomicState
	first := goroutine.Alloc(150_001)
	second := goroutine.Alloc(150_002)
	reader := goroutine.Alloc(150_003)
	state.publishRelease(first, 1)
	old := state.releases[0]
	oldStream := old.stream
	state.acquire(reader, 1)

	state.retireReleases(1)
	state.publishRelease(second, 1)
	current := state.releases[0]
	if current != old {
		t.Fatalf("test did not recycle release pointer: old=%p current=%p", old, current)
	}
	if current.stream == oldStream {
		t.Fatalf("recycled release retained stream %d", oldStream)
	}
	state.acquire(reader, 1)
	if got := reader.C.Get(second.TID); got != second.C.Get(second.TID) {
		t.Fatalf("same-pointer recycle cache hit skipped new release: got %d, want %d", got, second.C.Get(second.TID))
	}
}

func TestAtomicReleaseCacheCollisionFallsBackWithoutLosingImports(t *testing.T) {
	var states [3]atomicState
	publishers := [3]*goroutine.RaceContext{
		goroutine.Alloc(151_001),
		goroutine.Alloc(151_101),
		goroutine.Alloc(151_201),
	}
	for i := range states {
		states[i].publishRelease(publishers[i], 1)
	}
	first := states[0].releases[0]
	stream, version := first.stream, first.version

	ctx := goroutine.Alloc(151_301)
	for i := range states {
		states[i].acquire(ctx, 1)
	}
	// Three exact bindings exceed the two-entry cache, so reacquiring the first
	// release is a canonical fallback. Its subsequent publication must be a
	// same-stream checkpoint containing imports from the other two releases.
	states[0].acquire(ctx, 1)
	states[0].publishRelease(ctx, 1)
	if first.stream != stream || first.version != version+1 || first.deltaN != 0 {
		t.Fatalf("collision fallback publication = stream/version/deltas %d/%d/%d, want %d/%d/0", first.stream, first.version, first.deltaN, stream, version+1)
	}
	receiver := goroutine.Alloc(151_401)
	states[0].acquire(receiver, 1)
	for _, publisher := range publishers {
		if got := receiver.C.Get(publisher.TID); got != publisher.C.Get(publisher.TID) {
			t.Fatalf("collision checkpoint clock[%d] = %d, want %d", publisher.TID, got, publisher.C.Get(publisher.TID))
		}
	}
}

func TestAtomicWarmedAlternatingDeltaStreamAllocatesZero(t *testing.T) {
	var state atomicState
	left := goroutine.Alloc(160_001)
	right := goroutine.Alloc(160_101)
	state.publishRelease(left, 0xff)
	left.IncrementClock()
	next := right
	step := func() {
		state.acquire(next, 0xff)
		state.publishRelease(next, 0xff)
		next.IncrementClock()
		if next == left {
			next = right
		} else {
			next = left
		}
	}
	for i := 0; i < 128; i++ {
		step()
	}
	if allocs := testing.AllocsPerRun(1000, step); allocs != 0 {
		t.Fatalf("warmed delta-stream owner switch allocated %.2f objects", allocs)
	}
}

func TestRecordAtomicAccessDefersPruneForExistingTID(t *testing.T) {
	a := newAtomicHistoryArena()
	history := atomicHistory{arena: a}
	current := goroutine.Alloc(96)
	const staleTID = uint32(97)
	current.C.Set(staleTID, 3)
	history.user.insert(a, staleTID).access = atomicAccess{clocks: [AtomicTokenSlots]uint32{3}}
	history.user.insert(a, current.TID).access = atomicAccess{clocks: [AtomicTokenSlots]uint32{1}}

	recordAtomicAccess(&history, current, 0x2b00, 1, false)
	if _, ok := history.user.find(staleTID); !ok {
		t.Fatal("existing-TID update eagerly scanned and pruned the frontier")
	}
	if entry, _ := history.user.find(current.TID); entry.access.clocks[0] != current.C.Get(current.TID) || entry.access.pcs[0] != 0x2b00 {
		t.Fatalf("existing-TID witness = %+v, want current epoch/PC", entry.access)
	}

	// The next new-TID insertion performs the deferred bound-maintenance pass.
	newcomer := goroutine.Alloc(98)
	newcomer.C.Join(current.C)
	recordAtomicAccess(&history, newcomer, 0x2b01, 1, false)
	if _, ok := history.user.find(staleTID); ok {
		t.Fatal("new-TID insertion did not prune the deferred HB-dominated witness")
	}
	if _, ok := history.user.find(newcomer.TID); !ok {
		t.Fatal("new-TID insertion did not record its witness")
	}
}

func BenchmarkAtomicAcquireFragmentedRelease(b *testing.B) {
	for _, runs := range []int{256, 4096} {
		b.Run(strconv.Itoa(runs), func(b *testing.B) {
			const first = uint32(50_000)
			input := make([]vectorclock.FiniteRange, runs)
			for i := range input {
				tid := first + uint32(i)
				input[i] = vectorclock.FiniteRange{First: tid, Last: tid, Clock: uint32(i&1) + 1}
			}
			a, release := atomicReleaseForTest(input)
			state := atomicState{arena: a}
			state.releases[0] = release
			ctx := goroutine.Alloc(93)
			state.acquire(ctx, 1) // Benchmark the dominated fragmented hot path.
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				state.acquire(ctx, 1)
			}
		})
	}
}

func BenchmarkAtomicAcquirePromotedFallback(b *testing.B) {
	const runs = 256
	input := make([]vectorclock.FiniteRange, runs)
	for i := range input {
		tid := uint32(70_000 + i*2)
		input[i] = vectorclock.FiniteRange{First: tid, Last: tid, Clock: uint32(i%7 + 1)}
	}
	for _, promoted := range []bool{false, true} {
		name := "Canonical"
		if promoted {
			name = "Lineage"
		}
		b.Run(name, func(b *testing.B) {
			a, release := atomicReleaseForTest(input)
			release.version = atomicReleaseSnapshotCheckPeriod
			if promoted {
				maybePromoteAtomicRelease(release, goroutine.Alloc(94))
			}
			state := atomicState{arena: a}
			state.releases[0] = release
			ctx := goroutine.Alloc(95)
			state.acquire(ctx, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				release.stream = newAtomicReleaseStream()
				state.acquire(ctx, 1)
			}
		})
	}
}

func BenchmarkAtomicPromotedAcquirePublish(b *testing.B) {
	publisher := goroutine.Alloc(96)
	_, state, release := lineageAtomicStateForTest(publisher.C)
	state.acquire(publisher, 1)
	release.lineage.Reserve(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		publisher.IncrementClock()
		state.acquire(publisher, 1)
		state.publishRelease(publisher, 1)
	}
}

func BenchmarkAtomicPromotedAlternatingOwners(b *testing.B) {
	const owners = 1024
	contexts := make([]*goroutine.RaceContext, owners)
	for i := range contexts {
		contexts[i] = goroutine.Alloc(uint32(100_000 + i))
	}
	_, state, _ := lineageAtomicStateForTest(contexts[0].C)
	// Exclude first-import setup and seed each exact strong proof.
	for _, ctx := range contexts {
		state.acquire(ctx, 1)
		state.publishRelease(ctx, 1)
		ctx.IncrementClock()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := contexts[i&(owners-1)]
		state.acquire(ctx, 1)
		state.publishRelease(ctx, 1)
		ctx.IncrementClock()
	}
}

func BenchmarkAtomicPromotedDominatedGenerationRecovery(b *testing.B) {
	ctx := goroutine.Alloc(97)
	_, state, release := lineageAtomicStateForTest(ctx.C)
	state.acquire(ctx, 1)
	release.lineage.Reserve(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Model the intervening atomic.Value type-word acquire: it invalidates
		// generation proofs, but adds metadata already dominated by data view.
		ctx.NoteForeignImport()
		state.acquire(ctx, 1)
		state.publishRelease(ctx, 1)
		ctx.IncrementClock()
	}
}

func BenchmarkAtomicReleaseRunSnapshot(b *testing.B) {
	for _, coordinates := range []uint32{1, 1024} {
		name := "one"
		if coordinates != 1 {
			name = "1024"
		}
		b.Run(name, func(b *testing.B) {
			d := NewDetector()
			ctx := goroutine.Alloc(30_000)
			if coordinates > 1 {
				ctx.C.JoinRange(40_000, 40_000+coordinates-1, 1)
			}
			const addr = uintptr(0xcc00)
			var token AtomicToken
			d.AtomicBegin(addr, 8, ctx, false, &token)
			d.AtomicEnd(addr, 8, ctx, &token, 0x2a00, true)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d.AtomicBegin(addr, 8, ctx, false, &token)
				d.AtomicEnd(addr, 8, ctx, &token, 0x2a00, true)
			}
		})
	}
}

func TestPlainAccessConflictingWithAtomicRemainsRepresented(t *testing.T) {
	d := NewDetector()
	atomicReader := goroutine.Alloc(30)
	plainWriter := goroutine.Alloc(31)
	laterReader := goroutine.Alloc(32)
	const addr = uintptr(0x11000)

	completeAtomicSize(d, addr, 8, atomicReader, false, false, 0x8100)
	d.OnWrite(addr+3, plainWriter, 0x8101)
	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("mixed atomic/plain conflict reported %d races, want 1", got)
	}
	if got := d.ShadowGet(addr + 3).GetW(); got != plainWriter.GetEpoch() {
		t.Fatalf("conflicting plain write W = %v, want represented %v", got, plainWriter.GetEpoch())
	}

	d.OnRead(addr+3, laterReader, 0x8102)
	if got := d.RacesDetected(); got != 2 {
		t.Fatalf("later plain reader missed represented write: got %d races, want 2", got)
	}
}

func TestSynchronizationTokenDoesNotSuppressMemoryConflicts(t *testing.T) {
	const addr = uintptr(0x11400)

	t.Run("ordinary", func(t *testing.T) {
		d := NewDetector()
		d.OnRelease(addr, goroutine.Alloc(37))
		d.OnWrite(addr, goroutine.Alloc(38), 0x8150)
		d.OnRead(addr, goroutine.Alloc(39), 0x8151)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("ordinary conflict at sync token reported %d races, want 1", got)
		}
	})

	t.Run("mixed atomic plain", func(t *testing.T) {
		d := NewDetector()
		d.OnRelease(addr, goroutine.Alloc(40))
		completeAtomicSize(d, addr, 4, goroutine.Alloc(41), false, true, 0x8160)
		d.OnRead(addr, goroutine.Alloc(42), 0x8161)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("mixed conflict at sync token reported %d races, want 1", got)
		}
	})
}

func TestInternalMutexAtomicPCIsNotAPlainMarker(t *testing.T) {
	// Atomic wrappers retain the first non-sync/atomic frame. Use the actual
	// internal mutex method PC so this exercises the same classification as the
	// runtime path rather than a test-only flag.
	mutexPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	if !atomicInternalMutexPC(mutexPC) || rwMutexMarkerPC(mutexPC) {
		t.Fatalf("internal mutex PC %#x did not receive atomic-only classification", mutexPC)
	}

	const addr = uintptr(0x11800)
	t.Run("atomic after marker read", func(t *testing.T) {
		d := NewDetector()
		plainReader := goroutine.Alloc(33)
		atomicWriter := goroutine.Alloc(34)
		d.OnRead(addr, plainReader, mutexPC)
		completeAtomicSize(d, addr, 4, atomicWriter, false, true, mutexPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("same-family mixed access reported %d races, want 1", got)
		}
	})

	t.Run("marker read after atomic", func(t *testing.T) {
		d := NewDetector()
		atomicWriter := goroutine.Alloc(35)
		plainReader := goroutine.Alloc(36)
		completeAtomicSize(d, addr, 4, atomicWriter, false, true, mutexPC)
		d.OnRead(addr, plainReader, mutexPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("same-family reverse mixed access reported %d races, want 1", got)
		}
	})
}

func TestPCFunctionPrefixDoesNotAllocate(t *testing.T) {
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	atomicPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	userPC := reflect.ValueOf(TestPCFunctionPrefixDoesNotAllocate).Pointer() + 1

	if !pcHasFunctionPrefix(markerPC, "sync.(*RWMutex).") {
		t.Fatalf("RWMutex PC %#x did not match its function prefix", markerPC)
	}
	if !pcHasFunctionPrefix(atomicPC, "internal/sync.(*Mutex).") {
		t.Fatalf("internal mutex PC %#x did not match its function prefix", atomicPC)
	}
	if pcHasFunctionPrefix(userPC, "sync.(*RWMutex).") {
		t.Fatalf("user PC %#x matched the RWMutex function prefix", userPC)
	}

	var matched bool
	if allocs := testing.AllocsPerRun(1000, func() {
		matched = pcHasFunctionPrefix(markerPC, "sync.(*RWMutex).")
		matched = pcHasFunctionPrefix(atomicPC, "internal/sync.(*Mutex).") && matched
		matched = !pcHasFunctionPrefix(userPC, "sync.(*RWMutex).") && matched
	}); allocs != 0 {
		t.Fatalf("function-prefix classification allocated %.2f objects per iteration", allocs)
	}
	if !matched {
		t.Fatal("function-prefix classification changed during allocation check")
	}
}

func TestRWMutexMarkerAndInternalMutexAtomicAreSuppressed(t *testing.T) {
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	atomicPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	if !rwMutexMarkerPC(markerPC) || atomicInternalMutexPC(markerPC) {
		t.Fatalf("RWMutex PC %#x did not receive marker-only classification", markerPC)
	}
	if !atomicInternalMutexPC(atomicPC) || rwMutexMarkerPC(atomicPC) {
		t.Fatalf("internal mutex PC %#x did not receive atomic-only classification", atomicPC)
	}

	const addr = uintptr(0x11880)
	t.Run("atomic after marker read", func(t *testing.T) {
		d := NewDetector()
		d.OnRead(addr, goroutine.Alloc(55), markerPC)
		completeAtomicSize(d, addr, 4, goroutine.Alloc(56), false, true, atomicPC)
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("internal mutex atomic write reported %d RWMutex marker-read races", got)
		}
	})

	t.Run("marker read after atomic", func(t *testing.T) {
		d := NewDetector()
		completeAtomicSize(d, addr, 4, goroutine.Alloc(57), false, true, atomicPC)
		d.OnRead(addr, goroutine.Alloc(58), markerPC)
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("RWMutex marker read reported %d internal mutex atomic-write races", got)
		}
	})
}

func TestRWMutexMarkerReadDoesNotSeedUserReadCache(t *testing.T) {
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	atomicPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	const addr = uintptr(0x118c0)

	t.Run("marker then same-epoch user read", func(t *testing.T) {
		d := NewDetector()
		reader := goroutine.Alloc(65)
		d.OnRead(addr, reader, markerPC)
		cacheSlot := goroutine.ReadCacheIndex(addr)
		if reader.ReadCache[cacheSlot] == addr || reader.ReadCacheStates[cacheSlot] != nil {
			t.Fatalf("marker published redundant-read cache entry (%#x,%p)", reader.ReadCache[cacheSlot], reader.ReadCacheStates[cacheSlot])
		}

		// Mirror the runtime's sequential cache predicate. Before the fix the
		// marker populated this slot, so the genuine user read was skipped and
		// the later internal atomic conflict was incorrectly suppressed.
		if reader.ReadCache[cacheSlot] != addr || reader.ReadCacheStates[cacheSlot] == nil {
			d.OnRead(addr, reader, 0x8320)
		}
		completeAtomicSize(d, addr, 4, goroutine.Alloc(66), false, true, atomicPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("marker/user read followed by internal atomic write reported %d races, want 1", got)
		}
	})

	t.Run("marker only remains suppressible", func(t *testing.T) {
		d := NewDetector()
		reader := goroutine.Alloc(67)
		d.OnRead(addr, reader, markerPC)
		cacheSlot := goroutine.ReadCacheIndex(addr)
		if reader.ReadCache[cacheSlot] == addr || reader.ReadCacheStates[cacheSlot] != nil {
			t.Fatalf("marker-only read published cache entry (%#x,%p)", reader.ReadCache[cacheSlot], reader.ReadCacheStates[cacheSlot])
		}
		completeAtomicSize(d, addr, 4, goroutine.Alloc(68), false, true, atomicPC)
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("marker-only/internal atomic pair reported %d races", got)
		}
	})

	t.Run("marker preserves colliding user cache entry", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(69)
		markerAddr := addr + 8
		for goroutine.ReadCacheIndex(markerAddr) != goroutine.ReadCacheIndex(addr) {
			markerAddr += 8
		}
		// A cold user read stays compact; repeat it to promote the address and
		// establish the colliding cache entry whose preservation is under test.
		d.OnRead(addr, ctx, 0x8321)
		d.OnRead(addr, ctx, 0x8321)
		cacheSlot := goroutine.ReadCacheIndex(addr)
		cachedState := ctx.ReadCacheStates[cacheSlot]
		if ctx.ReadCache[cacheSlot] != addr || cachedState != nil {
			t.Fatalf("user no-op cache entry = (%#x,%p), want (%#x,nil)", ctx.ReadCache[cacheSlot], cachedState, addr)
		}
		d.OnRead(markerAddr, ctx, markerPC)
		if ctx.ReadCache[cacheSlot] != addr || ctx.ReadCacheStates[cacheSlot] != cachedState {
			t.Fatalf("colliding marker replaced user cache entry with (%#x,%p)", ctx.ReadCache[cacheSlot], ctx.ReadCacheStates[cacheSlot])
		}
	})
}

func TestInternalMutexAtomicDoesNotSuppressUserPlainAccess(t *testing.T) {
	mutexPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	const addr = uintptr(0x11900)

	t.Run("atomic after user plain", func(t *testing.T) {
		d := NewDetector()
		d.OnRead(addr, goroutine.Alloc(43), 0x8300)
		completeAtomicSize(d, addr, 4, goroutine.Alloc(44), false, true, mutexPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("internal atomic/user plain conflict reported %d races, want 1", got)
		}
	})

	t.Run("user plain after atomic", func(t *testing.T) {
		d := NewDetector()
		completeAtomicSize(d, addr, 4, goroutine.Alloc(45), false, true, mutexPC)
		d.OnRead(addr, goroutine.Alloc(46), 0x8301)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("user plain/internal atomic conflict reported %d races, want 1", got)
		}
	})

	t.Run("atomic read after user plain write", func(t *testing.T) {
		d := NewDetector()
		d.OnWrite(addr, goroutine.Alloc(50), 0x8302)
		completeAtomicSize(d, addr, 4, goroutine.Alloc(51), false, false, mutexPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("internal atomic read/user plain write conflict reported %d races, want 1", got)
		}
	})

	t.Run("shared reader PC is not misattributed", func(t *testing.T) {
		d := NewDetector()
		d.OnRead(addr, goroutine.Alloc(52), 0x8303)
		// The aggregate read PC now names internal/sync, but the first
		// conflicting epoch belongs to the preceding user access.
		d.OnRead(addr, goroutine.Alloc(53), markerPC)
		completeAtomicSize(d, addr, 4, goroutine.Alloc(54), false, true, mutexPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("shared user/internal reads followed by atomic write reported %d races, want 1", got)
		}
	})

	t.Run("untracked range reader keeps internal atomic reportable", func(t *testing.T) {
		d := NewDetector()
		d.OnRead(addr, goroutine.Alloc(59), markerPC)
		d.OnReadRange(addr, 1, goroutine.Alloc(60), 0x8304)
		completeAtomicSize(d, addr, 4, goroutine.Alloc(61), false, true, mutexPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("range user/internal marker followed by atomic write reported %d races, want 1", got)
		}
	})

	t.Run("same-epoch range reader replaces marker classification", func(t *testing.T) {
		d := NewDetector()
		reader := goroutine.Alloc(62)
		d.OnRead(addr, reader, markerPC)
		d.OnReadRange(addr, 1, reader, 0x8305)
		completeAtomicSize(d, addr, 4, goroutine.Alloc(63), false, true, mutexPC)
		if got := d.RacesDetected(); got != 1 {
			t.Fatalf("same-epoch user range read followed by internal atomic write reported %d races, want 1", got)
		}
	})

	t.Run("range write clears scalar reader classification", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(64)
		d.OnRead(addr, ctx, markerPC)
		d.OnWriteRange(addr, 1, ctx, 0x8306)
		state := atomicHistoryForTest(t, d, addr)
		if got := atomicHistoryCardinality(state.plainReads); got != 0 {
			t.Fatalf("range write retained %d stale scalar reader classifications", got)
		}
	})
}

func TestRangeReadIsNeverTreatedAsRWMutexMarker(t *testing.T) {
	mutexPC := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	d := NewDetector()
	const addr = uintptr(0x11a00)
	completeAtomicSize(d, addr, 8, goroutine.Alloc(47), false, true, mutexPC)
	completeAtomicSize(d, addr+8, 8, goroutine.Alloc(48), false, true, 0x8400)

	var reports []*RaceReport
	d.reportObserver = func(report *RaceReport) { reports = append(reports, report) }
	d.OnReadRange(addr, 16, goroutine.Alloc(49), markerPC)

	if got := d.RacesDetected(); got != 1 {
		t.Fatalf("range reported %d races, want the later genuine conflict", got)
	}
	if len(reports) != 1 || reports[0].Current.Addr != addr {
		t.Fatalf("reported conflict = %+v, want first range address %#x", reports, addr)
	}
}

func TestAtomicReleaseAcquireOrdersPlainData(t *testing.T) {
	for _, size := range []uintptr{4, 8} {
		d := NewDetector()
		writer := goroutine.Alloc(21)
		reader := goroutine.Alloc(22)
		const data = uintptr(0xd000)
		const flag = uintptr(0xe000)

		d.OnWrite(data, writer, 0x9000)
		completeAtomicSize(d, flag, size, writer, false, true, 0x9001)
		completeAtomicSize(d, flag, size, reader, true, false, 0x9002)
		d.OnRead(data, reader, 0x9003)
		if got := d.RacesDetected(); got != 0 {
			t.Fatalf("size %d release/acquire reported %d plain-data races", size, got)
		}
	}
}

type recordingSlotShadow struct {
	shadowmem.SlotShadow
	secondWord uintptr
	seenSecond chan struct{}
	once       sync.Once
}

func (s *recordingSlotShadow) GetOrCreateSlot(addr uintptr) *shadowmem.ShadowSlot {
	slot := s.SlotShadow.GetOrCreateSlot(addr)
	if addr&^uintptr(7) == s.secondWord {
		s.once.Do(func() { close(s.seenSecond) })
	}
	return slot
}

func TestAtomicBeginMaterializesSpanningWordsBeforeRetainingStateLocks(t *testing.T) {
	d := NewDetector()
	const (
		firstWord = uintptr(0x2f000)
		addr      = firstWord + 4
	)
	recorder := &recordingSlotShadow{
		SlotShadow: d.slotMemory,
		secondWord: firstWord + 8,
		seenSecond: make(chan struct{}),
	}
	d.slotMemory = recorder

	// Hold the first word's state so AtomicBegin stops as soon as it starts
	// retaining group locks. The second word must already be materialized by
	// then; otherwise a full-block range can form block -> state -> block.
	firstState := recorder.SlotShadow.GetOrCreateSlot(firstWord).GetOrCreateLane(4)
	firstState.LockAccess()
	ctx := goroutine.Alloc(90)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var token AtomicToken
		d.AtomicBegin(addr, 8, ctx, false, &token)
		d.AtomicEnd(addr, 8, ctx, &token, 0x2f100, false)
	}()

	materializedBeforeLock := false
	select {
	case <-recorder.seenSecond:
		materializedBeforeLock = true
	case <-time.After(time.Second):
	}
	firstState.UnlockAccess()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("spanning atomic did not complete after releasing the first state")
	}
	if !materializedBeforeLock {
		t.Fatal("spanning atomic retained a state lock before materializing its second word")
	}
}

func TestAtomicMixedWidthTransactionsDoNotDeadlock(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0xf000)
	completeAtomicSize(d, addr, 4, goroutine.Alloc(23), false, true, 0xa000)
	completeAtomicSize(d, addr+4, 4, goroutine.Alloc(24), false, true, 0xa001)

	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ctx := goroutine.Alloc(uint32(25 + worker))
			for i := 0; i < 250; i++ {
				switch (worker + i) % 3 {
				case 0:
					completeAtomicSize(d, addr, 8, ctx, true, i&1 == 0, 0xb000+uintptr(worker))
				case 1:
					completeAtomicSize(d, addr, 4, ctx, true, i&1 == 0, 0xb010+uintptr(worker))
				default:
					completeAtomicSize(d, addr+4, 4, ctx, true, i&1 == 0, 0xb020+uintptr(worker))
				}
			}
		}(worker)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("mixed-width atomic transactions deadlocked")
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("atomic-only stress reported %d races", got)
	}
}

func atomicHistoryForTest(t *testing.T, d *Detector, addr uintptr) *atomicState {
	t.Helper()
	state := d.ShadowGet(addr)
	if state == nil {
		t.Fatalf("address %#x has no ordinary shadow state", addr)
	}
	state.LockAccess()
	history := existingAtomicStateLocked(state)
	state.UnlockAccess()
	if history == nil {
		t.Fatalf("address %#x has no atomic history", addr)
	}
	return history
}

func atomicHistoryAccess(history atomicHistory, tid uint32) (atomicAccess, bool) {
	if entry, ok := history.user.find(tid); ok {
		return entry.access, true
	}
	entry, ok := history.internal.find(tid)
	if !ok {
		return atomicAccess{}, false
	}
	return entry.access, true
}

func atomicHistoryCardinality(history atomicHistory) int {
	count := 0
	history.user.visit(func(*atomicHistoryEntry) bool { count++; return true })
	history.internal.visit(func(*atomicHistoryEntry) bool { count++; return true })
	return count
}

func atomicHistoryLaneCardinality(history atomicHistory, lane uint8) int {
	count := 0
	visit := func(frontier *atomicHistoryClass) {
		frontier.visit(func(entry *atomicHistoryEntry) bool {
			if entry.access.clocks[lane] != 0 {
				count++
			}
			return true
		})
	}
	visit(&history.user)
	visit(&history.internal)
	return count
}

func atomicReleaseRunsForTest(release *atomicRelease) []vectorclock.FiniteRange {
	var runs []vectorclock.FiniteRange
	for n := release.runs; n != nil; n = n.next {
		runs = append(runs, vectorclock.FiniteRange{First: n.first, Last: n.last, Clock: n.clock})
	}
	return runs
}

func atomicReleaseRetiredForTest(release *atomicRelease) []vectorclock.RetiredRange {
	var runs []vectorclock.RetiredRange
	for n := release.retired; n != nil; n = n.next {
		runs = append(runs, vectorclock.RetiredRange{First: n.first, Last: n.last})
	}
	return runs
}

func atomicReleaseForTest(runs []vectorclock.FiniteRange) (*AtomicHistoryArena, *atomicRelease) {
	return atomicReleaseForTestWithRetired(runs, nil)
}

func atomicReleaseForTestWithRetired(runs []vectorclock.FiniteRange, retired []vectorclock.RetiredRange) (*AtomicHistoryArena, *atomicRelease) {
	a := newAtomicHistoryArena()
	r := a.allocRelease()
	var tail **atomicReleaseRange = &r.runs
	for _, run := range runs {
		n := a.allocRange()
		n.first, n.last, n.clock = run.First, run.Last, run.Clock
		*tail = n
		tail = &n.next
	}
	tail = &r.retired
	for _, run := range retired {
		n := a.allocRange()
		n.first, n.last = run.First, run.Last
		*tail = n
		tail = &n.next
	}
	return a, r
}
