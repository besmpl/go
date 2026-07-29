package detector

import (
	"sync"
	"testing"
	"unsafe"

	"runtime/race/kolkov/goroutine"
)

// TestOnAcquire_FirstAcquire verifies OnAcquire on first mutex lock (no previous releases).
func TestOnAcquire_FirstAcquire(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	mutexAddr := uintptr(0x1234)

	// First acquire - no release clock exists yet.
	initialClock := ctx.C.Get(0)
	d.OnAcquire(mutexAddr, ctx)

	// Clock should be incremented (no join happens since no release clock).
	if ctx.C.Get(0) != initialClock+1 {
		t.Errorf("Expected clock to increment from %d to %d, got %d",
			initialClock, initialClock+1, ctx.C.Get(0))
	}
}

func TestOnAcquireInvalidatesOnlyStrongAtomicReleaseProof(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(70_001)
	source := goroutine.Alloc(70_002)
	var releaseRoot byte
	release := unsafe.Pointer(&releaseRoot)
	ctx.RecordAtomicRelease(release, 3, 5, 1, true)

	const syncAddr = uintptr(0x12f0)
	d.OnRelease(syncAddr, source)
	before := ctx.ForeignGeneration
	d.OnAcquire(syncAddr, ctx)
	if ctx.ForeignGeneration != before+1 {
		t.Fatalf("sync acquire foreign generation = %d, want %d", ctx.ForeignGeneration, before+1)
	}
	seen, strong, ok := ctx.LookupAtomicRelease(release, 3, 1)
	if !ok || strong || seen != 5 {
		t.Fatalf("sync acquire cache proof = (%d,%v,%v), want weak version 5", seen, strong, ok)
	}
}

// TestOnRelease_FirstRelease verifies OnRelease on first mutex unlock.
func TestOnRelease_FirstRelease(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	mutexAddr := uintptr(0x1234)

	// Set some clock values.
	ctx.C.Set(0, 10)
	ctx.Epoch = ctx.GetEpoch() // Sync epoch

	// First release - should capture clock.
	d.OnRelease(mutexAddr, ctx)

	// Verify release clock was captured.
	syncVar := d.syncShadow.GetOrCreate(mutexAddr)
	releaseClock := syncVar.GetReleaseClock()

	if releaseClock == nil {
		t.Fatal("Expected release clock to be set")
	}

	// Release clock should have the clock value at time of release (before increment).
	if releaseClock.Get(0) != 10 {
		t.Errorf("Expected release clock[0]=10, got %d", releaseClock.Get(0))
	}

	// Context clock should be incremented after release.
	if ctx.C.Get(0) != 11 {
		t.Errorf("Expected context clock[0]=11 (incremented), got %d", ctx.C.Get(0))
	}
}

// TestOnAcquire_AcquireAfterRelease verifies happens-before from Unlock to Lock.
func TestOnAcquire_AcquireAfterRelease(t *testing.T) {
	d := NewDetector()
	mutexAddr := uintptr(0x1234)

	// Thread 0: Lock, do work, Unlock.
	ctx0 := goroutine.Alloc(0)
	d.OnAcquire(mutexAddr, ctx0) // Lock
	ctx0.IncrementClock()        // Do some work
	ctx0.IncrementClock()        // More work
	d.OnRelease(mutexAddr, ctx0) // Unlock (captures clock)

	// Thread 0 clock at release: some value (let's check).
	thread0ClockAtRelease := ctx0.C.Get(0) - 1 // -1 because OnRelease incremented

	// Thread 1: Lock (should see Thread 0's release clock).
	ctx1 := goroutine.Alloc(1)
	initialThread1Clock := ctx1.C.Get(1) // Should be 0

	d.OnAcquire(mutexAddr, ctx1) // Lock

	// Thread 1 should have joined with Thread 0's release clock.
	// ctx1.C[0] should now equal Thread 0's clock at release.
	if ctx1.C.Get(0) != thread0ClockAtRelease {
		t.Errorf("Expected Thread 1 to see Thread 0's clock %d, got %d",
			thread0ClockAtRelease, ctx1.C.Get(0))
	}

	// Thread 1's own clock should be incremented.
	if ctx1.C.Get(1) != initialThread1Clock+1 {
		t.Errorf("Expected Thread 1's clock to increment to %d, got %d",
			initialThread1Clock+1, ctx1.C.Get(1))
	}
}

// TestOnReleaseMerge_RWMutexScenario tests RWMutex read unlock merging.
func TestOnReleaseMerge_RWMutexScenario(t *testing.T) {
	d := NewDetector()
	mutexAddr := uintptr(0x1234)

	// Reader 1: RLock, read, RUnlock.
	reader1 := goroutine.Alloc(0)
	d.OnAcquire(mutexAddr, reader1)      // RLock
	reader1.IncrementClock()             // Do some work
	reader1.IncrementClock()             // More work
	d.OnReleaseMerge(mutexAddr, reader1) // RUnlock (merge)

	reader1ClockAtRelease := reader1.C.Get(0) - 1 // -1 because OnReleaseMerge incremented

	// Reader 2: RLock, read, RUnlock.
	reader2 := goroutine.Alloc(1)
	d.OnAcquire(mutexAddr, reader2)      // RLock (sees Reader 1's clock)
	reader2.IncrementClock()             // Do some work
	reader2.IncrementClock()             // More work
	d.OnReleaseMerge(mutexAddr, reader2) // RUnlock (merge)

	reader2ClockAtRelease := reader2.C.Get(1) - 1 // -1 because OnReleaseMerge incremented

	// Writer: Lock (should see union of both readers' clocks).
	writer := goroutine.Alloc(2)
	d.OnAcquire(mutexAddr, writer) // Lock

	// Writer should see both readers' clocks.
	if writer.C.Get(0) < reader1ClockAtRelease {
		t.Errorf("Writer did not see Reader 1's clock. Expected >= %d, got %d",
			reader1ClockAtRelease, writer.C.Get(0))
	}
	if writer.C.Get(1) < reader2ClockAtRelease {
		t.Errorf("Writer did not see Reader 2's clock. Expected >= %d, got %d",
			reader2ClockAtRelease, writer.C.Get(1))
	}
}

// TestMutexProtectedNoRace verifies mutex-protected code does NOT report races.
func TestMutexProtectedNoRace(t *testing.T) {
	d := NewDetector()
	mutexAddr := uintptr(0x1234)
	varAddr := uintptr(0x5678)

	// Thread 0: Lock, write, Unlock.
	ctx0 := goroutine.Alloc(0)
	d.OnAcquire(mutexAddr, ctx0) // Lock
	d.OnWrite(varAddr, ctx0, 0)  // Write x = 42
	d.OnRelease(mutexAddr, ctx0) // Unlock

	// Thread 1: Lock, read, Unlock.
	ctx1 := goroutine.Alloc(1)
	d.OnAcquire(mutexAddr, ctx1) // Lock (sees Thread 0's clock!)
	d.OnRead(varAddr, ctx1, 0)   // Read x (should NOT race)
	d.OnRelease(mutexAddr, ctx1) // Unlock

	// Verify no races detected.
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (mutex protected), got %d", d.RacesDetected())
	}
}

// TestUnprotectedRaceStillDetected verifies unprotected concurrent access still reports races.
func TestUnprotectedRaceStillDetected(t *testing.T) {
	d := NewDetector()
	varAddr := uintptr(0x5678)

	// Thread 0: Write (no lock).
	ctx0 := goroutine.Alloc(0)
	ctx0.IncrementClock() // Initialize clock
	d.OnWrite(varAddr, ctx0, 0)

	// Thread 1: Read (no lock) - SHOULD RACE because no mutex established happens-before.
	// However, if Thread 0's write happens-before Thread 1's read naturally (same thread order),
	// we need to make them truly concurrent.
	ctx1 := goroutine.Alloc(1)
	ctx1.IncrementClock() // Initialize clock (concurrent with Thread 0)
	d.OnRead(varAddr, ctx1, 0)

	// For this test, we actually expect 0 races because we're detecting based on happens-before,
	// and without explicit synchronization primitives, the threads have no happens-before relationship.
	// BUT - since Thread 0 wrote and Thread 1 read, and there's no HB, this SHOULD be a race.
	// Let's check the actual behavior.
	// Actually, the issue is that Thread 0 wrote at clock 0, and Thread 1 read at clock 0.
	// The happens-before check will look at ctx1.C[0] (which is 0) and compare with
	// the write epoch (which is 0@0). Since 0 <= 0, it will NOT report a race.
	// To make this a proper race, Thread 0 needs to have advanced its clock.

	// Re-do the test properly:
	d.Reset()
	ctx0 = goroutine.Alloc(0)
	ctx1 = goroutine.Alloc(1)

	// Thread 0: Advance clock, then write.
	for i := 0; i < 5; i++ {
		ctx0.IncrementClock() // Advance Thread 0's clock to 5
	}
	d.OnWrite(varAddr, ctx0, 0) // Write at clock 5 (will increment to 6)

	// Thread 1: Read without seeing Thread 0's write (no mutex sync).
	// Thread 1's vector clock for Thread 0 should be 0 (hasn't seen Thread 0's work).
	// ctx1.C[0] = 0, but write was at clock 6.
	// Since ctx1.C[0] (0) < write.clock (6), happens-before check fails → RACE!
	d.OnRead(varAddr, ctx1, 0)

	// Verify race was detected.
	if d.RacesDetected() != 1 {
		t.Errorf("Expected 1 race (unprotected), got %d", d.RacesDetected())
	}
}

// TestMultipleMutexes verifies different mutexes don't interfere.
func TestMultipleMutexes(t *testing.T) {
	d := NewDetector()
	mutex1Addr := uintptr(0x1000)
	mutex2Addr := uintptr(0x2000)
	var1Addr := uintptr(0x3000)
	var2Addr := uintptr(0x4000)

	ctx0 := goroutine.Alloc(0)
	ctx1 := goroutine.Alloc(1)

	// Thread 0: Lock mutex1, write var1, unlock mutex1.
	d.OnAcquire(mutex1Addr, ctx0)
	d.OnWrite(var1Addr, ctx0, 0)
	d.OnRelease(mutex1Addr, ctx0)

	// Thread 1: Lock mutex2, write var2, unlock mutex2.
	d.OnAcquire(mutex2Addr, ctx1)
	d.OnWrite(var2Addr, ctx1, 0)
	d.OnRelease(mutex2Addr, ctx1)

	// Thread 1: Lock mutex1 (different mutex), read var1 - should NOT race.
	d.OnAcquire(mutex1Addr, ctx1)
	d.OnRead(var1Addr, ctx1, 0)
	d.OnRelease(mutex1Addr, ctx1)

	// Thread 0: Lock mutex2, read var2 - should NOT race.
	d.OnAcquire(mutex2Addr, ctx0)
	d.OnRead(var2Addr, ctx0, 0)
	d.OnRelease(mutex2Addr, ctx0)

	// Verify no races (both variables properly protected by their mutexes).
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (multiple mutexes), got %d", d.RacesDetected())
	}
}

// TestLockReentry verifies same thread can lock/unlock multiple times.
func TestLockReentry(t *testing.T) {
	d := NewDetector()
	mutexAddr := uintptr(0x1234)
	varAddr := uintptr(0x5678)
	ctx := goroutine.Alloc(0)

	// First lock/unlock cycle.
	d.OnAcquire(mutexAddr, ctx)
	d.OnWrite(varAddr, ctx, 0)
	d.OnRelease(mutexAddr, ctx)

	// Second lock/unlock cycle (same thread).
	d.OnAcquire(mutexAddr, ctx)
	d.OnRead(varAddr, ctx, 0)
	d.OnRelease(mutexAddr, ctx)

	// Third lock/unlock cycle.
	d.OnAcquire(mutexAddr, ctx)
	d.OnWrite(varAddr, ctx, 0)
	d.OnRelease(mutexAddr, ctx)

	// Verify no races (same thread, sequential access).
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (lock reentry), got %d", d.RacesDetected())
	}
}

// TestConcurrentLocksEstablishHappensBefore verifies competing lock acquisitions.
func TestConcurrentLocksEstablishHappensBefore(t *testing.T) {
	d := NewDetector()
	mutexAddr := uintptr(0x1234)
	varAddr := uintptr(0x5678)

	// Thread 0: Lock, write, unlock.
	ctx0 := goroutine.Alloc(0)
	d.OnAcquire(mutexAddr, ctx0)
	d.OnWrite(varAddr, ctx0, 0)
	d.OnRelease(mutexAddr, ctx0)

	// Thread 1: Lock, write, unlock (happens-after Thread 0).
	ctx1 := goroutine.Alloc(1)
	d.OnAcquire(mutexAddr, ctx1)
	d.OnWrite(varAddr, ctx1, 0) // Overwrites Thread 0's write - NO RACE
	d.OnRelease(mutexAddr, ctx1)

	// Thread 2: Lock, read, unlock (happens-after Thread 1).
	ctx2 := goroutine.Alloc(2)
	d.OnAcquire(mutexAddr, ctx2)
	d.OnRead(varAddr, ctx2, 0) // Reads Thread 1's write - NO RACE
	d.OnRelease(mutexAddr, ctx2)

	// Verify no races (all accesses happen-before each other via mutex).
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (concurrent locks), got %d", d.RacesDetected())
	}
}

// TestDetectorReset_ClearsSyncShadow verifies Reset clears sync shadow memory.
func TestDetectorReset_ClearsSyncShadow(t *testing.T) {
	d := NewDetector()
	mutexAddr := uintptr(0x1234)
	ctx := goroutine.Alloc(0)

	// Create a release clock.
	d.OnAcquire(mutexAddr, ctx)
	d.OnRelease(mutexAddr, ctx)

	// Verify release clock exists.
	syncVar := d.syncShadow.GetOrCreate(mutexAddr)
	if syncVar.GetReleaseClock() == nil {
		t.Fatal("Expected release clock to exist before reset")
	}

	// Reset detector.
	d.Reset()

	// After reset, sync shadow should be cleared.
	// GetOrCreate will return a NEW SyncVar with nil release clock.
	syncVarAfterReset := d.syncShadow.GetOrCreate(mutexAddr)
	if syncVarAfterReset.GetReleaseClock() != nil {
		t.Error("Expected release clock to be nil after reset")
	}
}

// TestClearShadowRange_ClearsSyncLifecycle verifies that an allocator-reused
// address does not inherit happens-before state from its previous object.
func TestClearShadowRange_ClearsSyncLifecycle(t *testing.T) {
	d := NewDetector()
	addr := uintptr(0x4320)
	outsideAddr := addr + 8
	oldState := d.syncShadow.GetOrCreate(addr)
	outsideState := d.syncShadow.GetOrCreate(outsideAddr)

	d.ClearShadowRange(addr, 8)
	if d.syncShadow.HasEntry(addr) {
		t.Fatal("ClearShadowRange retained synchronization state")
	}
	if got := d.syncShadow.GetOrCreate(outsideAddr); got != outsideState {
		t.Fatal("ClearShadowRange removed adjacent sync state")
	}
	if fresh := d.syncShadow.GetOrCreate(addr); fresh == oldState {
		t.Fatal("ClearShadowRange retained stale happens-before state")
	}
}

// === BENCHMARKS ===

// BenchmarkOnAcquire benchmarks mutex lock tracking.
// Target: <500ns/op (VectorClock join overhead acceptable).
func BenchmarkOnAcquire(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	mutexAddr := uintptr(unsafe.Pointer(&d)) // Use detector's address as mutex

	// Set up a release clock (mutex has been unlocked before).
	d.OnRelease(mutexAddr, ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnAcquire(mutexAddr, ctx)
	}
}

// BenchmarkOnRelease benchmarks mutex unlock tracking.
// Target: <300ns/op (VectorClock copy overhead acceptable).
func BenchmarkOnRelease(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	mutexAddr := uintptr(unsafe.Pointer(&d))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnRelease(mutexAddr, ctx)
	}
}

// BenchmarkOnReleaseMerge benchmarks RWMutex unlock tracking.
// Target: <500ns/op (VectorClock merge overhead acceptable).
func BenchmarkOnReleaseMerge(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	mutexAddr := uintptr(unsafe.Pointer(&d))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnReleaseMerge(mutexAddr, ctx)
	}
}

// BenchmarkMutexProtectedAccess benchmarks full lock/access/unlock cycle.
func BenchmarkMutexProtectedAccess(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	mutexAddr := uintptr(0x1234)
	varAddr := uintptr(0x5678)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnAcquire(mutexAddr, ctx)
		d.OnWrite(varAddr, ctx, 0)
		d.OnRelease(mutexAddr, ctx)
	}
}

// Channels and WaitGroups do not have detector-specific callbacks. The Go
// runtime models them with the same acquire/release primitives as locks:
// buffered channels use one synchronization address per buffer slot, close
// uses the channel address, and WaitGroup.Done uses ReleaseMerge.

// channelSlotExchange mirrors runtime.racereleaseacquire: consume the clock
// left by the prior slot user, then publish this context for the next user.
func channelSlotExchange(d *Detector, slot uintptr, ctx *goroutine.RaceContext) {
	d.OnAcquire(slot, ctx)
	d.OnRelease(slot, ctx)
}

func TestBufferedChannelSlotsPreserveSendPairing(t *testing.T) {
	d := NewDetector()
	const (
		slot0 = uintptr(0x2000)
		slot1 = uintptr(0x2008)
	)
	sender0 := goroutine.Alloc(0)
	sender1 := goroutine.Alloc(1)
	receiver := goroutine.Alloc(2)

	channelSlotExchange(d, slot0, sender0)
	channelSlotExchange(d, slot1, sender1)
	sender0AtSend := sender0.C.Get(sender0.TID) - 1
	sender1AtSend := sender1.C.Get(sender1.TID) - 1

	// The first receive from a capacity-two channel consumes slot zero. It must
	// not accidentally join the later send stored in slot one.
	channelSlotExchange(d, slot0, receiver)
	if got := receiver.C.Get(sender0.TID); got != sender0AtSend {
		t.Fatalf("slot-zero receive saw sender-zero clock %d, want %d", got, sender0AtSend)
	}
	if got := receiver.C.Get(sender1.TID); got != 0 {
		t.Fatalf("slot-zero receive spuriously saw slot-one sender clock %d", got)
	}

	channelSlotExchange(d, slot1, receiver)
	if got := receiver.C.Get(sender1.TID); got != sender1AtSend {
		t.Fatalf("slot-one receive saw sender-one clock %d, want %d", got, sender1AtSend)
	}
}

func TestBufferedReceiveAfterCloseDoesNotObserveClose(t *testing.T) {
	d := NewDetector()
	const (
		slot      = uintptr(0x3000)
		closeAddr = uintptr(0x4000)
	)
	sender := goroutine.Alloc(0)
	closer := goroutine.Alloc(1)
	receiver := goroutine.Alloc(2)

	channelSlotExchange(d, slot, sender)
	d.OnRelease(closeAddr, closer)
	senderAtSend := sender.C.Get(sender.TID) - 1
	closerAtClose := closer.C.Get(closer.TID) - 1

	// A receive of a value already buffered before close synchronizes with that
	// value's send, not with close. Only the later empty receive observes close.
	channelSlotExchange(d, slot, receiver)
	if got := receiver.C.Get(sender.TID); got != senderAtSend {
		t.Fatalf("buffered receive saw sender clock %d, want %d", got, senderAtSend)
	}
	if got := receiver.C.Get(closer.TID); got != 0 {
		t.Fatalf("buffered receive spuriously observed close clock %d", got)
	}

	d.OnAcquire(closeAddr, receiver)
	if got := receiver.C.Get(closer.TID); got != closerAtClose {
		t.Fatalf("closed-empty receive saw close clock %d, want %d", got, closerAtClose)
	}
}

func TestConcurrentWaitGroupDoneCallbacksMergeEveryClock(t *testing.T) {
	const workers = 32
	d := NewDetector()
	const waitGroupAddr = uintptr(0x5000)
	children := make([]*goroutine.RaceContext, workers)
	start := make(chan struct{})
	var callbacks sync.WaitGroup
	callbacks.Add(workers)
	for i := range children {
		children[i] = goroutine.Alloc(uint32(i))
		children[i].IncrementClock()
		go func(ctx *goroutine.RaceContext) {
			defer callbacks.Done()
			<-start
			// sync.WaitGroup.Add(-1) calls race.ReleaseMerge on the WaitGroup.
			d.OnReleaseMerge(waitGroupAddr, ctx)
		}(children[i])
	}
	close(start)
	callbacks.Wait()

	waiter := goroutine.Alloc(workers)
	d.OnAcquire(waitGroupAddr, waiter)
	for _, child := range children {
		want := child.C.Get(child.TID) - 1
		if got := waiter.C.Get(child.TID); got != want {
			t.Fatalf("waiter saw child %d clock %d, want %d", child.TID, got, want)
		}
	}
}

func TestConcurrentChannelSlotCallbacksRemainIsolated(t *testing.T) {
	const workers = 16
	d := NewDetector()
	const slotsBase = uintptr(0x6000)
	senders := make([]*goroutine.RaceContext, workers)
	start := make(chan struct{})
	var callbacks sync.WaitGroup
	callbacks.Add(workers)
	for i := range senders {
		senders[i] = goroutine.Alloc(uint32(i))
		go func(i int, ctx *goroutine.RaceContext) {
			defer callbacks.Done()
			<-start
			channelSlotExchange(d, slotsBase+uintptr(i)*8, ctx)
		}(i, senders[i])
	}
	close(start)
	callbacks.Wait()

	for i, sender := range senders {
		receiver := goroutine.Alloc(uint32(workers + i))
		d.OnAcquire(slotsBase+uintptr(i)*8, receiver)
		want := sender.C.Get(sender.TID) - 1
		if got := receiver.C.Get(sender.TID); got != want {
			t.Fatalf("slot %d saw sender clock %d, want %d", i, got, want)
		}
		other := senders[(i+1)%workers]
		if got := receiver.C.Get(other.TID); got != 0 {
			t.Fatalf("slot %d spuriously saw slot %d sender clock %d", i, (i+1)%workers, got)
		}
	}
}
