package detector

import (
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
	d.OnWrite(varAddr, ctx0)     // Write x = 42
	d.OnRelease(mutexAddr, ctx0) // Unlock

	// Thread 1: Lock, read, Unlock.
	ctx1 := goroutine.Alloc(1)
	d.OnAcquire(mutexAddr, ctx1) // Lock (sees Thread 0's clock!)
	d.OnRead(varAddr, ctx1)      // Read x (should NOT race)
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
	d.OnWrite(varAddr, ctx0)

	// Thread 1: Read (no lock) - SHOULD RACE because no mutex established happens-before.
	// However, if Thread 0's write happens-before Thread 1's read naturally (same thread order),
	// we need to make them truly concurrent.
	ctx1 := goroutine.Alloc(1)
	ctx1.IncrementClock() // Initialize clock (concurrent with Thread 0)
	d.OnRead(varAddr, ctx1)

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
	d.OnWrite(varAddr, ctx0) // Write at clock 5 (will increment to 6)

	// Thread 1: Read without seeing Thread 0's write (no mutex sync).
	// Thread 1's vector clock for Thread 0 should be 0 (hasn't seen Thread 0's work).
	// ctx1.C[0] = 0, but write was at clock 6.
	// Since ctx1.C[0] (0) < write.clock (6), happens-before check fails → RACE!
	d.OnRead(varAddr, ctx1)

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
	d.OnWrite(var1Addr, ctx0)
	d.OnRelease(mutex1Addr, ctx0)

	// Thread 1: Lock mutex2, write var2, unlock mutex2.
	d.OnAcquire(mutex2Addr, ctx1)
	d.OnWrite(var2Addr, ctx1)
	d.OnRelease(mutex2Addr, ctx1)

	// Thread 1: Lock mutex1 (different mutex), read var1 - should NOT race.
	d.OnAcquire(mutex1Addr, ctx1)
	d.OnRead(var1Addr, ctx1)
	d.OnRelease(mutex1Addr, ctx1)

	// Thread 0: Lock mutex2, read var2 - should NOT race.
	d.OnAcquire(mutex2Addr, ctx0)
	d.OnRead(var2Addr, ctx0)
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
	d.OnWrite(varAddr, ctx)
	d.OnRelease(mutexAddr, ctx)

	// Second lock/unlock cycle (same thread).
	d.OnAcquire(mutexAddr, ctx)
	d.OnRead(varAddr, ctx)
	d.OnRelease(mutexAddr, ctx)

	// Third lock/unlock cycle.
	d.OnAcquire(mutexAddr, ctx)
	d.OnWrite(varAddr, ctx)
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
	d.OnWrite(varAddr, ctx0)
	d.OnRelease(mutexAddr, ctx0)

	// Thread 1: Lock, write, unlock (happens-after Thread 0).
	ctx1 := goroutine.Alloc(1)
	d.OnAcquire(mutexAddr, ctx1)
	d.OnWrite(varAddr, ctx1) // Overwrites Thread 0's write - NO RACE
	d.OnRelease(mutexAddr, ctx1)

	// Thread 2: Lock, read, unlock (happens-after Thread 1).
	ctx2 := goroutine.Alloc(2)
	d.OnAcquire(mutexAddr, ctx2)
	d.OnRead(varAddr, ctx2) // Reads Thread 1's write - NO RACE
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
		d.OnWrite(varAddr, ctx)
		d.OnRelease(mutexAddr, ctx)
	}
}

// === Channel Synchronization Tests (Phase 4 Task 4.2) ===

// TestOnChannelSendAfter_FirstSend verifies OnChannelSendAfter on first send.
func TestOnChannelSendAfter_FirstSend(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	chAddr := uintptr(0x2000)

	// Set some clock values.
	ctx.C.Set(0, 10)
	ctx.Epoch = ctx.GetEpoch() // Sync epoch

	// First send - should capture clock.
	d.OnChannelSendAfter(chAddr, ctx)

	// Verify send clock was captured.
	syncVar := d.syncShadow.GetOrCreate(chAddr)
	sendClock := syncVar.GetChannelSendClock()

	if sendClock == nil {
		t.Fatal("Expected send clock to be set")
	}

	// Send clock should have the clock value at time of send (before increment).
	if sendClock.Get(0) != 10 {
		t.Errorf("Expected send clock[0]=10, got %d", sendClock.Get(0))
	}

	// Context clock should be incremented after send.
	if ctx.C.Get(0) != 11 {
		t.Errorf("Expected context clock[0]=11 (incremented), got %d", ctx.C.Get(0))
	}
}

// TestOnChannelRecvAfter_RecvAfterSend verifies happens-before from Send to Recv.
func TestOnChannelRecvAfter_RecvAfterSend(t *testing.T) {
	d := NewDetector()
	chAddr := uintptr(0x2000)

	// Thread 0 (sender): Send on channel.
	sender := goroutine.Alloc(0)
	sender.IncrementClock()              // Do some work
	sender.IncrementClock()              // More work
	d.OnChannelSendAfter(chAddr, sender) // Send (captures clock)

	senderClockAtSend := sender.C.Get(0) - 1 // -1 because OnChannelSendAfter incremented

	// Thread 1 (receiver): Receive from channel.
	receiver := goroutine.Alloc(1)
	initialReceiverClock := receiver.C.Get(1) // Should be 0

	d.OnChannelRecvAfter(chAddr, receiver) // Receive (merges sender's clock)

	// Receiver should have joined with sender's clock.
	// receiver.C[0] should now equal sender's clock at send.
	if receiver.C.Get(0) != senderClockAtSend {
		t.Errorf("Expected Receiver to see Sender's clock %d, got %d",
			senderClockAtSend, receiver.C.Get(0))
	}

	// Receiver's own clock should be incremented.
	if receiver.C.Get(1) != initialReceiverClock+1 {
		t.Errorf("Expected Receiver's clock to increment to %d, got %d",
			initialReceiverClock+1, receiver.C.Get(1))
	}
}

// TestChannelSynchronizedNoRace verifies channel-synchronized code does NOT report races.
func TestChannelSynchronizedNoRace(t *testing.T) {
	d := NewDetector()
	chAddr := uintptr(0x2000)
	varAddr := uintptr(0x3000)

	// Thread 0 (sender): Write, then send.
	sender := goroutine.Alloc(0)
	d.OnWrite(varAddr, sender)           // Write x = 42
	d.OnChannelSendAfter(chAddr, sender) // Send on ch

	// Thread 1 (receiver): Receive, then read.
	receiver := goroutine.Alloc(1)
	d.OnChannelRecvAfter(chAddr, receiver) // Receive from ch (sees sender's clock!)
	d.OnRead(varAddr, receiver)            // Read x (should NOT race)

	// Verify no races detected.
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (channel synchronized), got %d", d.RacesDetected())
	}
}

// TestUnprotectedChannelRaceStillDetected verifies unprotected concurrent access still reports races.
func TestUnprotectedChannelRaceStillDetected(t *testing.T) {
	d := NewDetector()
	varAddr := uintptr(0x3000)

	// Thread 0: Write (no channel synchronization).
	sender := goroutine.Alloc(0)
	for i := 0; i < 5; i++ {
		sender.IncrementClock() // Advance sender's clock to 5
	}
	d.OnWrite(varAddr, sender) // Write at clock 5 (will increment to 6)

	// Thread 1: Read WITHOUT seeing Thread 0's write (no channel sync).
	// Thread 1's vector clock for Thread 0 should be 0 (hasn't seen Thread 0's work).
	// ctx1.C[0] = 0, but write was at clock 6.
	// Since ctx1.C[0] (0) < write.clock (6), happens-before check fails → RACE!
	receiver := goroutine.Alloc(1)
	d.OnRead(varAddr, receiver)

	// Verify race was detected.
	if d.RacesDetected() != 1 {
		t.Errorf("Expected 1 race (unprotected), got %d", d.RacesDetected())
	}
}

// TestChannelClose_RecvAfterClose verifies happens-before from Close to Recv.
func TestChannelClose_RecvAfterClose(t *testing.T) {
	d := NewDetector()
	chAddr := uintptr(0x2000)
	varAddr := uintptr(0x3000)

	// Thread 0 (closer): Write, then close channel.
	closer := goroutine.Alloc(0)
	d.OnWrite(varAddr, closer)       // Write x = 42
	d.OnChannelClose(chAddr, closer) // Close ch (captures clock)

	// Thread 1 (receiver): Receive from closed channel, then read.
	receiver := goroutine.Alloc(1)
	d.OnChannelRecvAfter(chAddr, receiver) // Receive from closed ch (sees closer's clock!)
	d.OnRead(varAddr, receiver)            // Read x (should NOT race)

	// Verify no races detected.
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (channel close synchronized), got %d", d.RacesDetected())
	}
}

// TestMultipleChannels verifies different channels don't interfere.
func TestMultipleChannels(t *testing.T) {
	d := NewDetector()
	ch1Addr := uintptr(0x2000)
	ch2Addr := uintptr(0x3000)
	var1Addr := uintptr(0x4000)
	var2Addr := uintptr(0x5000)

	ctx0 := goroutine.Alloc(0)
	ctx1 := goroutine.Alloc(1)

	// Thread 0: Write var1, send on ch1.
	d.OnWrite(var1Addr, ctx0)
	d.OnChannelSendAfter(ch1Addr, ctx0)

	// Thread 1: Write var2, send on ch2.
	d.OnWrite(var2Addr, ctx1)
	d.OnChannelSendAfter(ch2Addr, ctx1)

	// Thread 1: Receive from ch1 (different channel), read var1 - should NOT race.
	d.OnChannelRecvAfter(ch1Addr, ctx1)
	d.OnRead(var1Addr, ctx1)

	// Thread 0: Receive from ch2, read var2 - should NOT race.
	d.OnChannelRecvAfter(ch2Addr, ctx0)
	d.OnRead(var2Addr, ctx0)

	// Verify no races (both variables properly synchronized by their channels).
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (multiple channels), got %d", d.RacesDetected())
	}
}

// TestChannelSequentialSends verifies sequential send/recv pairs.
func TestChannelSequentialSends(t *testing.T) {
	d := NewDetector()
	chAddr := uintptr(0x2000)
	varAddr := uintptr(0x3000)
	ctx := goroutine.Alloc(0)

	// First send/recv cycle (same thread for simplicity).
	d.OnWrite(varAddr, ctx)
	d.OnChannelSendAfter(chAddr, ctx)
	d.OnChannelRecvAfter(chAddr, ctx)
	d.OnRead(varAddr, ctx)

	// Second send/recv cycle.
	d.OnWrite(varAddr, ctx)
	d.OnChannelSendAfter(chAddr, ctx)
	d.OnChannelRecvAfter(chAddr, ctx)
	d.OnRead(varAddr, ctx)

	// Third send/recv cycle.
	d.OnWrite(varAddr, ctx)
	d.OnChannelSendAfter(chAddr, ctx)
	d.OnChannelRecvAfter(chAddr, ctx)
	d.OnRead(varAddr, ctx)

	// Verify no races (sequential access, same thread).
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (sequential sends), got %d", d.RacesDetected())
	}
}

// TestChannelAndMutexTogether verifies channels and mutexes work independently.
func TestChannelAndMutexTogether(t *testing.T) {
	d := NewDetector()
	mutexAddr := uintptr(0x1000)
	chAddr := uintptr(0x2000)
	var1Addr := uintptr(0x3000)
	var2Addr := uintptr(0x4000)

	ctx0 := goroutine.Alloc(0)
	ctx1 := goroutine.Alloc(1)

	// Thread 0: Lock mutex, write var1, unlock.
	d.OnAcquire(mutexAddr, ctx0)
	d.OnWrite(var1Addr, ctx0)
	d.OnRelease(mutexAddr, ctx0)

	// Thread 0: Write var2, send on channel.
	d.OnWrite(var2Addr, ctx0)
	d.OnChannelSendAfter(chAddr, ctx0)

	// Thread 1: Receive from channel, read var2 - should NOT race.
	d.OnChannelRecvAfter(chAddr, ctx1)
	d.OnRead(var2Addr, ctx1)

	// Thread 1: Lock mutex, read var1 - should NOT race.
	d.OnAcquire(mutexAddr, ctx1)
	d.OnRead(var1Addr, ctx1)
	d.OnRelease(mutexAddr, ctx1)

	// Verify no races (both mutex and channel work correctly together).
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (channel + mutex), got %d", d.RacesDetected())
	}
}

// TestDetectorReset_ClearesChannelState verifies Reset clears channel state.
func TestDetectorReset_ClearsChannelState(t *testing.T) {
	d := NewDetector()
	chAddr := uintptr(0x2000)
	ctx := goroutine.Alloc(0)

	// Create channel state.
	d.OnChannelSendAfter(chAddr, ctx)

	// Verify send clock exists.
	syncVar := d.syncShadow.GetOrCreate(chAddr)
	if syncVar.GetChannelSendClock() == nil {
		t.Fatal("Expected send clock to exist before reset")
	}

	// Reset detector.
	d.Reset()

	// After reset, sync shadow should be cleared.
	// GetOrCreate will return a NEW SyncVar with nil send clock.
	syncVarAfterReset := d.syncShadow.GetOrCreate(chAddr)
	if syncVarAfterReset.GetChannelSendClock() != nil {
		t.Error("Expected send clock to be nil after reset")
	}
}

// === BENCHMARKS (Phase 4 Task 4.2) ===

// BenchmarkOnChannelSendBefore benchmarks channel send before tracking.
// Target: <100ns/op (minimal overhead).
func BenchmarkOnChannelSendBefore(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	chAddr := uintptr(unsafe.Pointer(&d)) // Use detector's address as channel

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnChannelSendBefore(chAddr, ctx)
	}
}

// BenchmarkOnChannelSendAfter benchmarks channel send after tracking.
// Target: <500ns/op (VectorClock copy overhead acceptable).
func BenchmarkOnChannelSendAfter(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	chAddr := uintptr(unsafe.Pointer(&d))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnChannelSendAfter(chAddr, ctx)
	}
}

// BenchmarkOnChannelRecvBefore benchmarks channel receive before tracking.
// Target: <100ns/op (minimal overhead).
func BenchmarkOnChannelRecvBefore(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	chAddr := uintptr(unsafe.Pointer(&d))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnChannelRecvBefore(chAddr, ctx)
	}
}

// BenchmarkOnChannelRecvAfter benchmarks channel receive after tracking.
// Target: <500ns/op (VectorClock join overhead acceptable).
func BenchmarkOnChannelRecvAfter(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	chAddr := uintptr(unsafe.Pointer(&d))

	// Set up a send clock (channel has been sent before).
	d.OnChannelSendAfter(chAddr, ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnChannelRecvAfter(chAddr, ctx)
	}
}

// BenchmarkOnChannelClose benchmarks channel close tracking.
// Target: <300ns/op (VectorClock copy overhead acceptable).
func BenchmarkOnChannelClose(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	chAddr := uintptr(unsafe.Pointer(&d))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset channel state for each iteration (close is one-time operation).
		d.syncShadow.Reset()
		d.OnChannelClose(chAddr, ctx)
	}
}

// BenchmarkChannelSynchronizedAccess benchmarks full send/recv cycle.
func BenchmarkChannelSynchronizedAccess(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	chAddr := uintptr(0x2000)
	varAddr := uintptr(0x3000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWrite(varAddr, ctx)
		d.OnChannelSendAfter(chAddr, ctx)
		d.OnChannelRecvAfter(chAddr, ctx)
		d.OnRead(varAddr, ctx)
	}
}

// === WaitGroup Tests (Phase 4 Task 4.3) ===

// TestOnWaitGroupAdd_Basic verifies OnWaitGroupAdd increments counter.
func TestOnWaitGroupAdd_Basic(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	wgAddr := uintptr(0x1234)

	initialClock := ctx.C.Get(0)

	// OnWaitGroupAdd(1) should increment counter.
	d.OnWaitGroupAdd(wgAddr, 1, ctx)

	// Verify clock was incremented.
	if ctx.C.Get(0) != initialClock+1 {
		t.Errorf("Expected clock to increment from %d to %d, got %d",
			initialClock, initialClock+1, ctx.C.Get(0))
	}

	// Verify counter was set.
	syncVar := d.syncShadow.GetOrCreate(wgAddr)
	if syncVar.GetWaitGroupCounter() != 1 {
		t.Errorf("Expected counter=1, got %d", syncVar.GetWaitGroupCounter())
	}
}

// TestOnWaitGroupDone_Basic verifies OnWaitGroupDone merges clock.
func TestOnWaitGroupDone_Basic(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	wgAddr := uintptr(0x1234)

	// First, Add(1) to set counter.
	d.OnWaitGroupAdd(wgAddr, 1, ctx)

	// Advance clock to simulate work.
	ctx.IncrementClock()
	ctx.IncrementClock()
	clockBeforeDone := ctx.C.Get(0)

	// OnWaitGroupDone should merge clock into doneClock.
	d.OnWaitGroupDone(wgAddr, ctx)

	// Verify clock was incremented.
	if ctx.C.Get(0) != clockBeforeDone+1 {
		t.Errorf("Expected clock to increment from %d to %d, got %d",
			clockBeforeDone, clockBeforeDone+1, ctx.C.Get(0))
	}

	// Verify doneClock was set.
	syncVar := d.syncShadow.GetOrCreate(wgAddr)
	doneClock := syncVar.GetWaitGroupDoneClock()
	if doneClock == nil {
		t.Fatal("Expected doneClock to be set")
	}
	// doneClock should have the clock value BEFORE Done incremented it.
	if doneClock.Get(0) != clockBeforeDone {
		t.Errorf("Expected doneClock[0]=%d, got %d", clockBeforeDone, doneClock.Get(0))
	}

	// Verify counter was decremented.
	if syncVar.GetWaitGroupCounter() != 0 {
		t.Errorf("Expected counter=0 after Done, got %d", syncVar.GetWaitGroupCounter())
	}
}

// TestOnWaitGroupWaitAfter_MergesDoneClock verifies Wait merges doneClock.
func TestOnWaitGroupWaitAfter_MergesDoneClock(t *testing.T) {
	d := NewDetector()
	wgAddr := uintptr(0x1234)

	// Child goroutine: Add(1), do work, Done().
	childCtx := goroutine.Alloc(1)
	d.OnWaitGroupAdd(wgAddr, 1, childCtx)
	for i := 0; i < 5; i++ {
		childCtx.IncrementClock()
	}
	childClockBeforeDone := childCtx.C.Get(1)
	d.OnWaitGroupDone(wgAddr, childCtx)

	// Parent goroutine: Wait().
	parentCtx := goroutine.Alloc(0)
	initialParentClock := parentCtx.C.Get(0)

	d.OnWaitGroupWaitBefore(wgAddr, parentCtx)
	d.OnWaitGroupWaitAfter(wgAddr, parentCtx)

	// Parent should now see child's clock.
	if parentCtx.C.Get(1) != childClockBeforeDone {
		t.Errorf("Expected parent to see child's clock %d, got %d",
			childClockBeforeDone, parentCtx.C.Get(1))
	}

	// Parent's own clock should have incremented (WaitBefore + WaitAfter).
	// WaitBefore: +1, WaitAfter: +1 = +2 total.
	expectedParentClock := initialParentClock + 2
	if parentCtx.C.Get(0) != expectedParentClock {
		t.Errorf("Expected parent clock %d, got %d",
			expectedParentClock, parentCtx.C.Get(0))
	}
}

// TestWaitGroupProtectedNoRace verifies WaitGroup-protected code does NOT report races.
func TestWaitGroupProtectedNoRace(t *testing.T) {
	d := NewDetector()
	wgAddr := uintptr(0x1234)
	varAddr := uintptr(0x5678)

	// Child goroutine: write, then Done().
	childCtx := goroutine.Alloc(1)
	d.OnWaitGroupAdd(wgAddr, 1, childCtx)
	d.OnWrite(varAddr, childCtx) // Child writes
	d.OnWaitGroupDone(wgAddr, childCtx)

	// Parent goroutine: Wait(), then read.
	parentCtx := goroutine.Alloc(0)
	d.OnWaitGroupWaitBefore(wgAddr, parentCtx)
	d.OnWaitGroupWaitAfter(wgAddr, parentCtx) // Parent sees child's clock
	d.OnRead(varAddr, parentCtx)              // Parent reads (should NOT race)

	// Verify no races detected.
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (WaitGroup protected), got %d", d.RacesDetected())
	}
}

// TestWaitGroupMultipleChildren verifies multiple children synchronize correctly.
func TestWaitGroupMultipleChildren(t *testing.T) {
	d := NewDetector()
	wgAddr := uintptr(0x1234)
	var1Addr := uintptr(0x5000)
	var2Addr := uintptr(0x6000)
	var3Addr := uintptr(0x7000)

	// Parent: Add(3).
	parentCtx := goroutine.Alloc(0)
	d.OnWaitGroupAdd(wgAddr, 3, parentCtx)

	// Child 1: Write var1, Done().
	child1Ctx := goroutine.Alloc(1)
	d.OnWrite(var1Addr, child1Ctx)
	d.OnWaitGroupDone(wgAddr, child1Ctx)

	// Child 2: Write var2, Done().
	child2Ctx := goroutine.Alloc(2)
	d.OnWrite(var2Addr, child2Ctx)
	d.OnWaitGroupDone(wgAddr, child2Ctx)

	// Child 3: Write var3, Done().
	child3Ctx := goroutine.Alloc(3)
	d.OnWrite(var3Addr, child3Ctx)
	d.OnWaitGroupDone(wgAddr, child3Ctx)

	// Parent: Wait(), then read all variables.
	d.OnWaitGroupWaitBefore(wgAddr, parentCtx)
	d.OnWaitGroupWaitAfter(wgAddr, parentCtx)
	d.OnRead(var1Addr, parentCtx) // Should NOT race
	d.OnRead(var2Addr, parentCtx) // Should NOT race
	d.OnRead(var3Addr, parentCtx) // Should NOT race

	// Verify no races detected.
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (3 children synchronized), got %d", d.RacesDetected())
	}

	// Verify parent sees all children's clocks.
	if parentCtx.C.Get(1) == 0 || parentCtx.C.Get(2) == 0 || parentCtx.C.Get(3) == 0 {
		t.Error("Parent did not see all children's clocks")
	}
}

// TestWaitGroupUnprotectedStillDetectsRace verifies unprotected access still races.
func TestWaitGroupUnprotectedStillDetectsRace(t *testing.T) {
	d := NewDetector()
	varAddr := uintptr(0x5678)

	// Child goroutine: Write (NO WaitGroup Done).
	childCtx := goroutine.Alloc(1)
	for i := 0; i < 5; i++ {
		childCtx.IncrementClock()
	}
	d.OnWrite(varAddr, childCtx)

	// Parent goroutine: Read (NO WaitGroup Wait).
	parentCtx := goroutine.Alloc(0)
	d.OnRead(varAddr, parentCtx)

	// Verify race was detected (no happens-before established).
	if d.RacesDetected() != 1 {
		t.Errorf("Expected 1 race (unprotected), got %d", d.RacesDetected())
	}
}

// TestWaitGroupNestedUsage verifies nested WaitGroup patterns.
func TestWaitGroupNestedUsage(t *testing.T) {
	d := NewDetector()
	wg1Addr := uintptr(0x1000)
	wg2Addr := uintptr(0x2000)
	varAddr := uintptr(0x5678)

	// Parent: Add(1) for wg1.
	parentCtx := goroutine.Alloc(0)
	d.OnWaitGroupAdd(wg1Addr, 1, parentCtx)

	// Child: Add(1) for wg2, write, Done(wg2), Done(wg1).
	childCtx := goroutine.Alloc(1)
	d.OnWaitGroupAdd(wg2Addr, 1, childCtx)

	// Grandchild: Write, Done(wg2).
	grandchildCtx := goroutine.Alloc(2)
	d.OnWrite(varAddr, grandchildCtx)
	d.OnWaitGroupDone(wg2Addr, grandchildCtx)

	// Child: Wait(wg2), Done(wg1).
	d.OnWaitGroupWaitBefore(wg2Addr, childCtx)
	d.OnWaitGroupWaitAfter(wg2Addr, childCtx)
	d.OnWaitGroupDone(wg1Addr, childCtx)

	// Parent: Wait(wg1), read.
	d.OnWaitGroupWaitBefore(wg1Addr, parentCtx)
	d.OnWaitGroupWaitAfter(wg1Addr, parentCtx)
	d.OnRead(varAddr, parentCtx)

	// Verify no races (transitivity of happens-before).
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (nested WaitGroups), got %d", d.RacesDetected())
	}
}

// TestWaitGroupReadAfterWaitNoRace verifies read after Wait does not race.
func TestWaitGroupReadAfterWaitNoRace(t *testing.T) {
	d := NewDetector()
	wgAddr := uintptr(0x1234)
	varAddr := uintptr(0x5678)

	// Parent: Add(2).
	parentCtx := goroutine.Alloc(0)
	d.OnWaitGroupAdd(wgAddr, 2, parentCtx)

	// Child 1: Write, Done().
	child1Ctx := goroutine.Alloc(1)
	d.OnWrite(varAddr, child1Ctx)
	d.OnWaitGroupDone(wgAddr, child1Ctx)

	// Child 2: Done() (no write).
	child2Ctx := goroutine.Alloc(2)
	d.OnWaitGroupDone(wgAddr, child2Ctx)

	// Parent: Wait(), read.
	d.OnWaitGroupWaitBefore(wgAddr, parentCtx)
	d.OnWaitGroupWaitAfter(wgAddr, parentCtx)
	d.OnRead(varAddr, parentCtx)

	// Verify no races.
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races, got %d", d.RacesDetected())
	}
}

// TestWaitGroupMultipleWaits verifies multiple Wait() calls work correctly.
func TestWaitGroupMultipleWaits(t *testing.T) {
	d := NewDetector()
	wgAddr := uintptr(0x1234)
	varAddr := uintptr(0x5678)

	// Child: Add(1), Write, Done().
	childCtx := goroutine.Alloc(1)
	d.OnWaitGroupAdd(wgAddr, 1, childCtx)
	d.OnWrite(varAddr, childCtx)
	d.OnWaitGroupDone(wgAddr, childCtx)

	// Parent 1: Wait(), read.
	parent1Ctx := goroutine.Alloc(0)
	d.OnWaitGroupWaitBefore(wgAddr, parent1Ctx)
	d.OnWaitGroupWaitAfter(wgAddr, parent1Ctx)
	d.OnRead(varAddr, parent1Ctx)

	// Parent 2: Wait(), read (should also be safe).
	parent2Ctx := goroutine.Alloc(2)
	d.OnWaitGroupWaitBefore(wgAddr, parent2Ctx)
	d.OnWaitGroupWaitAfter(wgAddr, parent2Ctx)
	d.OnRead(varAddr, parent2Ctx)

	// Verify no races (both parents see child's write).
	if d.RacesDetected() != 0 {
		t.Errorf("Expected 0 races (multiple waits), got %d", d.RacesDetected())
	}
}

// BenchmarkOnWaitGroupAdd benchmarks WaitGroup Add tracking.
// Target: <200ns/op (minimal overhead).
func BenchmarkOnWaitGroupAdd(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	wgAddr := uintptr(unsafe.Pointer(&d))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWaitGroupAdd(wgAddr, 1, ctx)
	}
}

// BenchmarkOnWaitGroupDone benchmarks WaitGroup Done tracking.
// Target: <500ns/op (VectorClock merge overhead acceptable).
func BenchmarkOnWaitGroupDone(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	wgAddr := uintptr(unsafe.Pointer(&d))

	// Set up Add(1) first.
	d.OnWaitGroupAdd(wgAddr, 1, ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWaitGroupDone(wgAddr, ctx)
		// Reset counter for next iteration.
		d.syncShadow.GetOrCreate(wgAddr).WaitGroupAdd(1)
	}
}

// BenchmarkOnWaitGroupWaitBefore benchmarks WaitGroup WaitBefore tracking.
// Target: <100ns/op (minimal overhead).
func BenchmarkOnWaitGroupWaitBefore(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	wgAddr := uintptr(unsafe.Pointer(&d))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWaitGroupWaitBefore(wgAddr, ctx)
	}
}

// BenchmarkOnWaitGroupWaitAfter benchmarks WaitGroup WaitAfter tracking.
// Target: <500ns/op (VectorClock merge overhead acceptable).
func BenchmarkOnWaitGroupWaitAfter(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(0)
	wgAddr := uintptr(unsafe.Pointer(&d))

	// Set up Done() first so there's a doneClock to merge.
	childCtx := goroutine.Alloc(1)
	d.OnWaitGroupAdd(wgAddr, 1, childCtx)
	d.OnWaitGroupDone(wgAddr, childCtx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWaitGroupWaitAfter(wgAddr, ctx)
	}
}

// BenchmarkWaitGroupSynchronizedAccess benchmarks full WaitGroup cycle.
func BenchmarkWaitGroupSynchronizedAccess(b *testing.B) {
	d := NewDetector()
	parentCtx := goroutine.Alloc(0)
	childCtx := goroutine.Alloc(1)
	wgAddr := uintptr(0x1234)
	varAddr := uintptr(0x5678)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWaitGroupAdd(wgAddr, 1, parentCtx)
		d.OnWrite(varAddr, childCtx)
		d.OnWaitGroupDone(wgAddr, childCtx)
		d.OnWaitGroupWaitBefore(wgAddr, parentCtx)
		d.OnWaitGroupWaitAfter(wgAddr, parentCtx)
		d.OnRead(varAddr, parentCtx)
		// Reset for next iteration.
		d.Reset()
	}
}
