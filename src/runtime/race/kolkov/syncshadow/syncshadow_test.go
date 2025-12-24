package syncshadow

import (
	"testing"

	"runtime/race/kolkov/vectorclock"
)

// TestNewSyncShadow verifies SyncShadow initialization.
func TestNewSyncShadow(t *testing.T) {
	shadow := NewSyncShadow()
	if shadow == nil {
		t.Fatal("NewSyncShadow returned nil")
	}
}

// TestGetOrCreate_FirstAccess verifies SyncVar creation on first access.
func TestGetOrCreate_FirstAccess(t *testing.T) {
	shadow := NewSyncShadow()
	addr := uintptr(0x1234)

	sv := shadow.GetOrCreate(addr)
	if sv == nil {
		t.Fatal("GetOrCreate returned nil")
	}

	// First access should have nil releaseClock (not yet released).
	if sv.GetReleaseClock() != nil {
		t.Error("Expected nil releaseClock on first access")
	}
}

// TestGetOrCreate_Cached verifies same SyncVar returned on repeated access.
func TestGetOrCreate_Cached(t *testing.T) {
	shadow := NewSyncShadow()
	addr := uintptr(0x1234)

	sv1 := shadow.GetOrCreate(addr)
	sv2 := shadow.GetOrCreate(addr)

	if sv1 != sv2 {
		t.Error("GetOrCreate returned different SyncVar instances for same address")
	}
}

// TestGetOrCreate_DifferentAddresses verifies separate SyncVars for different addresses.
func TestGetOrCreate_DifferentAddresses(t *testing.T) {
	shadow := NewSyncShadow()
	addr1 := uintptr(0x1234)
	addr2 := uintptr(0x5678)

	sv1 := shadow.GetOrCreate(addr1)
	sv2 := shadow.GetOrCreate(addr2)

	if sv1 == sv2 {
		t.Error("GetOrCreate returned same SyncVar for different addresses")
	}
}

// TestGetOrCreate_Concurrent verifies thread-safe concurrent access.
func TestGetOrCreate_Concurrent(t *testing.T) {
	shadow := NewSyncShadow()
	addr := uintptr(0x1234)
	numGoroutines := 100

	// Launch concurrent goroutines all accessing the same address.
	results := make(chan *SyncVar, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			results <- shadow.GetOrCreate(addr)
		}()
	}

	// Collect all results.
	firstSV := <-results
	for i := 1; i < numGoroutines; i++ {
		sv := <-results
		if sv != firstSV {
			t.Errorf("Concurrent GetOrCreate returned different SyncVar instances")
		}
	}
}

// TestReset verifies Reset clears all state.
func TestReset(t *testing.T) {
	shadow := NewSyncShadow()
	addr1 := uintptr(0x1234)
	addr2 := uintptr(0x5678)

	// Create SyncVars for two addresses.
	sv1Before := shadow.GetOrCreate(addr1)
	sv2Before := shadow.GetOrCreate(addr2)

	// Set release clocks to verify they persist.
	vc := vectorclock.New()
	vc.Set(0, 10)
	sv1Before.SetReleaseClock(vc)
	sv2Before.SetReleaseClock(vc)

	// Reset shadow memory.
	shadow.Reset()

	// After reset, GetOrCreate should return NEW SyncVar instances.
	sv1After := shadow.GetOrCreate(addr1)
	sv2After := shadow.GetOrCreate(addr2)

	if sv1After == sv1Before {
		t.Error("Reset did not clear SyncVar for addr1")
	}
	if sv2After == sv2Before {
		t.Error("Reset did not clear SyncVar for addr2")
	}

	// New SyncVars should have nil releaseClock.
	if sv1After.GetReleaseClock() != nil {
		t.Error("SyncVar after Reset has non-nil releaseClock")
	}
	if sv2After.GetReleaseClock() != nil {
		t.Error("SyncVar after Reset has non-nil releaseClock")
	}
}

// TestSyncVar_GetReleaseClock_Nil verifies nil return on uninitialized SyncVar.
func TestSyncVar_GetReleaseClock_Nil(t *testing.T) {
	sv := &SyncVar{}
	clock := sv.GetReleaseClock()
	if clock != nil {
		t.Error("Expected nil releaseClock on uninitialized SyncVar")
	}
}

// TestSyncVar_SetReleaseClock_First verifies first SetReleaseClock allocates.
func TestSyncVar_SetReleaseClock_First(t *testing.T) {
	sv := &SyncVar{}

	// Create a clock to set.
	vc := vectorclock.New()
	vc.Set(0, 10)
	vc.Set(1, 20)

	// First SetReleaseClock should allocate and copy.
	sv.SetReleaseClock(vc)

	// Verify releaseClock is now non-nil.
	releaseClock := sv.GetReleaseClock()
	if releaseClock == nil {
		t.Fatal("SetReleaseClock did not allocate releaseClock")
	}

	// Verify values were copied correctly.
	if releaseClock.Get(0) != 10 {
		t.Errorf("Expected clock[0]=10, got %d", releaseClock.Get(0))
	}
	if releaseClock.Get(1) != 20 {
		t.Errorf("Expected clock[1]=20, got %d", releaseClock.Get(1))
	}

	// Verify it's a copy, not a reference.
	if releaseClock == vc {
		t.Error("SetReleaseClock did not copy, it's a reference")
	}
}

// TestSyncVar_SetReleaseClock_Update verifies subsequent SetReleaseClock updates in place.
func TestSyncVar_SetReleaseClock_Update(t *testing.T) {
	sv := &SyncVar{}

	// First SetReleaseClock.
	vc1 := vectorclock.New()
	vc1.Set(0, 10)
	sv.SetReleaseClock(vc1)
	firstClock := sv.GetReleaseClock()

	// Second SetReleaseClock with different values.
	vc2 := vectorclock.New()
	vc2.Set(0, 20)
	vc2.Set(1, 30)
	sv.SetReleaseClock(vc2)
	secondClock := sv.GetReleaseClock()

	// Verify same VectorClock instance (updated in place, no new allocation).
	if firstClock != secondClock {
		t.Error("SetReleaseClock allocated new clock instead of updating in place")
	}

	// Verify values were updated.
	if secondClock.Get(0) != 20 {
		t.Errorf("Expected clock[0]=20, got %d", secondClock.Get(0))
	}
	if secondClock.Get(1) != 30 {
		t.Errorf("Expected clock[1]=30, got %d", secondClock.Get(1))
	}
}

// TestSyncVar_MergeReleaseClock_First verifies first MergeReleaseClock allocates.
func TestSyncVar_MergeReleaseClock_First(t *testing.T) {
	sv := &SyncVar{}

	// Create a clock to merge.
	vc := vectorclock.New()
	vc.Set(0, 10)
	vc.Set(1, 20)

	// First MergeReleaseClock should allocate and copy (same as SetReleaseClock).
	sv.MergeReleaseClock(vc)

	// Verify releaseClock is now non-nil.
	releaseClock := sv.GetReleaseClock()
	if releaseClock == nil {
		t.Fatal("MergeReleaseClock did not allocate releaseClock")
	}

	// Verify values were copied correctly.
	if releaseClock.Get(0) != 10 {
		t.Errorf("Expected clock[0]=10, got %d", releaseClock.Get(0))
	}
	if releaseClock.Get(1) != 20 {
		t.Errorf("Expected clock[1]=20, got %d", releaseClock.Get(1))
	}
}

// TestSyncVar_MergeReleaseClock_Join verifies subsequent MergeReleaseClock performs join.
func TestSyncVar_MergeReleaseClock_Join(t *testing.T) {
	sv := &SyncVar{}

	// First merge: {0:10, 1:20}.
	vc1 := vectorclock.New()
	vc1.Set(0, 10)
	vc1.Set(1, 20)
	sv.MergeReleaseClock(vc1)

	// Second merge: {0:15, 2:30}.
	// Result should be: {0:max(10,15)=15, 1:max(20,0)=20, 2:max(0,30)=30}.
	vc2 := vectorclock.New()
	vc2.Set(0, 15)
	vc2.Set(2, 30)
	sv.MergeReleaseClock(vc2)

	// Verify join (element-wise max) was performed.
	releaseClock := sv.GetReleaseClock()
	if releaseClock.Get(0) != 15 {
		t.Errorf("Expected clock[0]=15 (max(10,15)), got %d", releaseClock.Get(0))
	}
	if releaseClock.Get(1) != 20 {
		t.Errorf("Expected clock[1]=20 (max(20,0)), got %d", releaseClock.Get(1))
	}
	if releaseClock.Get(2) != 30 {
		t.Errorf("Expected clock[2]=30 (max(0,30)), got %d", releaseClock.Get(2))
	}
}

// TestSyncVar_MergeReleaseClock_RWMutexScenario tests realistic RWMutex scenario.
func TestSyncVar_MergeReleaseClock_RWMutexScenario(t *testing.T) {
	sv := &SyncVar{}

	// Reader 1 (TID=0) unlocks at clock=10.
	reader1Clock := vectorclock.New()
	reader1Clock.Set(0, 10)
	sv.MergeReleaseClock(reader1Clock)

	// Reader 2 (TID=1) unlocks at clock=15.
	reader2Clock := vectorclock.New()
	reader2Clock.Set(1, 15)
	sv.MergeReleaseClock(reader2Clock)

	// Writer (TID=2) locks and should see both readers' clocks.
	releaseClock := sv.GetReleaseClock()
	if releaseClock.Get(0) != 10 {
		t.Errorf("Expected clock[0]=10 (Reader 1), got %d", releaseClock.Get(0))
	}
	if releaseClock.Get(1) != 15 {
		t.Errorf("Expected clock[1]=15 (Reader 2), got %d", releaseClock.Get(1))
	}

	// Writer's clock should join with both readers.
	writerClock := vectorclock.New()
	writerClock.Set(2, 5) // Writer was at clock 5 before lock
	writerClock.Join(releaseClock)

	// After join, writer should have max of all clocks.
	if writerClock.Get(0) != 10 {
		t.Errorf("Expected writer clock[0]=10, got %d", writerClock.Get(0))
	}
	if writerClock.Get(1) != 15 {
		t.Errorf("Expected writer clock[1]=15, got %d", writerClock.Get(1))
	}
	if writerClock.Get(2) != 5 {
		t.Errorf("Expected writer clock[2]=5, got %d", writerClock.Get(2))
	}
}

// === Channel State Tests (Phase 4 Task 4.2) ===

// TestSyncVar_GetOrCreateChannel verifies lazy channel state creation.
func TestSyncVar_GetOrCreateChannel(t *testing.T) {
	sv := &SyncVar{}

	// Initially, GetChannel should return nil (not a channel).
	if sv.GetChannel() != nil {
		t.Error("Expected nil channel state before GetOrCreateChannel")
	}

	// GetOrCreateChannel should create and return ChannelState.
	chState1 := sv.GetOrCreateChannel()
	if chState1 == nil {
		t.Fatal("GetOrCreateChannel returned nil")
	}

	// Second call should return same instance.
	chState2 := sv.GetOrCreateChannel()
	if chState1 != chState2 {
		t.Error("GetOrCreateChannel returned different instances")
	}

	// GetChannel should now return the created instance.
	if sv.GetChannel() != chState1 {
		t.Error("GetChannel returned different instance than GetOrCreateChannel")
	}
}

// TestSyncVar_ChannelSendClock verifies send clock management.
func TestSyncVar_ChannelSendClock(t *testing.T) {
	sv := &SyncVar{}

	// Initially, GetChannelSendClock should return nil.
	if sv.GetChannelSendClock() != nil {
		t.Error("Expected nil send clock before SetChannelSendClock")
	}

	// Create a clock to set.
	vc1 := vectorclock.New()
	vc1.Set(0, 10)
	vc1.Set(1, 20)

	// SetChannelSendClock should capture the clock.
	sv.SetChannelSendClock(vc1)

	// Verify send clock was set.
	sendClock := sv.GetChannelSendClock()
	if sendClock == nil {
		t.Fatal("SetChannelSendClock did not set send clock")
	}
	if sendClock.Get(0) != 10 {
		t.Errorf("Expected sendClock[0]=10, got %d", sendClock.Get(0))
	}
	if sendClock.Get(1) != 20 {
		t.Errorf("Expected sendClock[1]=20, got %d", sendClock.Get(1))
	}

	// Verify it's a copy, not a reference.
	if sendClock == vc1 {
		t.Error("SetChannelSendClock did not copy, it's a reference")
	}

	// Update send clock with different values.
	vc2 := vectorclock.New()
	vc2.Set(0, 30)
	vc2.Set(2, 40)
	sv.SetChannelSendClock(vc2)

	// Verify clock was updated in place.
	sendClockUpdated := sv.GetChannelSendClock()
	if sendClockUpdated != sendClock {
		t.Error("SetChannelSendClock allocated new clock instead of updating in place")
	}
	if sendClockUpdated.Get(0) != 30 {
		t.Errorf("Expected sendClock[0]=30, got %d", sendClockUpdated.Get(0))
	}
	if sendClockUpdated.Get(2) != 40 {
		t.Errorf("Expected sendClock[2]=40, got %d", sendClockUpdated.Get(2))
	}
}

// TestSyncVar_ChannelRecvClock verifies receive clock management.
func TestSyncVar_ChannelRecvClock(t *testing.T) {
	sv := &SyncVar{}

	// Initially, GetChannelRecvClock should return nil.
	if sv.GetChannelRecvClock() != nil {
		t.Error("Expected nil recv clock before SetChannelRecvClock")
	}

	// Create a clock to set.
	vc := vectorclock.New()
	vc.Set(1, 15)

	// SetChannelRecvClock should capture the clock.
	sv.SetChannelRecvClock(vc)

	// Verify recv clock was set.
	recvClock := sv.GetChannelRecvClock()
	if recvClock == nil {
		t.Fatal("SetChannelRecvClock did not set recv clock")
	}
	if recvClock.Get(1) != 15 {
		t.Errorf("Expected recvClock[1]=15, got %d", recvClock.Get(1))
	}
}

// TestSyncVar_ChannelCloseClock verifies close clock management.
func TestSyncVar_ChannelCloseClock(t *testing.T) {
	sv := &SyncVar{}

	// Initially, GetChannelCloseClock should return nil.
	if sv.GetChannelCloseClock() != nil {
		t.Error("Expected nil close clock before SetChannelCloseClock")
	}

	// Initially, IsChannelClosed should return false.
	if sv.IsChannelClosed() {
		t.Error("Expected IsChannelClosed=false before close")
	}

	// Create a clock to set.
	vc := vectorclock.New()
	vc.Set(0, 100)

	// SetChannelCloseClock should capture the clock and mark as closed.
	sv.SetChannelCloseClock(vc)

	// Verify close clock was set.
	closeClock := sv.GetChannelCloseClock()
	if closeClock == nil {
		t.Fatal("SetChannelCloseClock did not set close clock")
	}
	if closeClock.Get(0) != 100 {
		t.Errorf("Expected closeClock[0]=100, got %d", closeClock.Get(0))
	}

	// Verify isClosed flag was set.
	if !sv.IsChannelClosed() {
		t.Error("Expected IsChannelClosed=true after close")
	}

	// Verify it's a copy, not a reference.
	if closeClock == vc {
		t.Error("SetChannelCloseClock did not copy, it's a reference")
	}

	// Calling SetChannelCloseClock again should be idempotent (no panic).
	vc2 := vectorclock.New()
	vc2.Set(0, 200)
	sv.SetChannelCloseClock(vc2)

	// Close clock should NOT change (first close wins).
	closeClock2 := sv.GetChannelCloseClock()
	if closeClock2.Get(0) != 100 {
		t.Errorf("Expected closeClock to remain 100, got %d", closeClock2.Get(0))
	}
}

// TestSyncVar_ChannelState_Independent verifies channel and mutex state are independent.
func TestSyncVar_ChannelState_Independent(t *testing.T) {
	sv := &SyncVar{}

	// Set mutex release clock.
	mutexClock := vectorclock.New()
	mutexClock.Set(0, 10)
	sv.SetReleaseClock(mutexClock)

	// Set channel send clock.
	chanClock := vectorclock.New()
	chanClock.Set(1, 20)
	sv.SetChannelSendClock(chanClock)

	// Verify both are independent.
	if sv.GetReleaseClock().Get(0) != 10 {
		t.Error("Mutex release clock was affected by channel state")
	}
	if sv.GetChannelSendClock().Get(1) != 20 {
		t.Error("Channel send clock was affected by mutex state")
	}

	// Verify they don't share memory.
	if sv.GetReleaseClock() == sv.GetChannelSendClock() {
		t.Error("Mutex and channel clocks share memory")
	}
}

// === WaitGroup Tests (Phase 4 Task 4.3) ===

// TestSyncVar_GetOrCreateWaitGroup verifies lazy WaitGroup state allocation.
func TestSyncVar_GetOrCreateWaitGroup(t *testing.T) {
	sv := &SyncVar{}

	// Initially, GetWaitGroup should return nil.
	if sv.GetWaitGroup() != nil {
		t.Error("Expected nil WaitGroup before GetOrCreateWaitGroup")
	}

	// GetOrCreateWaitGroup should allocate WaitGroupState.
	wgState := sv.GetOrCreateWaitGroup()
	if wgState == nil {
		t.Fatal("GetOrCreateWaitGroup returned nil")
	}

	// Second call should return same instance (no new allocation).
	wgState2 := sv.GetOrCreateWaitGroup()
	if wgState != wgState2 {
		t.Error("GetOrCreateWaitGroup created new instance instead of reusing")
	}

	// GetWaitGroup should now return the allocated state.
	if sv.GetWaitGroup() != wgState {
		t.Error("GetWaitGroup returned different instance")
	}
}

// TestSyncVar_WaitGroupAdd verifies counter management.
func TestSyncVar_WaitGroupAdd(t *testing.T) {
	sv := &SyncVar{}

	// Initially, counter should be 0.
	if sv.GetWaitGroupCounter() != 0 {
		t.Errorf("Expected counter=0, got %d", sv.GetWaitGroupCounter())
	}

	// WaitGroupAdd(1) should increment counter to 1.
	sv.WaitGroupAdd(1)
	if sv.GetWaitGroupCounter() != 1 {
		t.Errorf("Expected counter=1 after Add(1), got %d", sv.GetWaitGroupCounter())
	}

	// WaitGroupAdd(3) should increment counter to 4.
	sv.WaitGroupAdd(3)
	if sv.GetWaitGroupCounter() != 4 {
		t.Errorf("Expected counter=4 after Add(3), got %d", sv.GetWaitGroupCounter())
	}

	// WaitGroupAdd(-1) should decrement counter to 3 (simulating Done).
	sv.WaitGroupAdd(-1)
	if sv.GetWaitGroupCounter() != 3 {
		t.Errorf("Expected counter=3 after Add(-1), got %d", sv.GetWaitGroupCounter())
	}

	// Multiple Done() calls should bring counter back to 0.
	sv.WaitGroupAdd(-1)
	sv.WaitGroupAdd(-1)
	sv.WaitGroupAdd(-1)
	if sv.GetWaitGroupCounter() != 0 {
		t.Errorf("Expected counter=0 after all Done(), got %d", sv.GetWaitGroupCounter())
	}
}

// TestSyncVar_MergeWaitGroupDoneClock verifies doneClock accumulation.
func TestSyncVar_MergeWaitGroupDoneClock(t *testing.T) {
	sv := &SyncVar{}

	// Initially, GetWaitGroupDoneClock should return nil.
	if sv.GetWaitGroupDoneClock() != nil {
		t.Error("Expected nil doneClock before any Done()")
	}

	// First Done() call - should copy the clock.
	clock1 := vectorclock.New()
	clock1.Set(0, 10)
	clock1.Set(1, 5)
	sv.MergeWaitGroupDoneClock(clock1)

	doneClock := sv.GetWaitGroupDoneClock()
	if doneClock == nil {
		t.Fatal("MergeWaitGroupDoneClock did not set doneClock")
	}
	if doneClock.Get(0) != 10 || doneClock.Get(1) != 5 {
		t.Errorf("Expected doneClock[0]=10, [1]=5, got [0]=%d, [1]=%d",
			doneClock.Get(0), doneClock.Get(1))
	}

	// Verify it's a copy, not a reference.
	if doneClock == clock1 {
		t.Error("MergeWaitGroupDoneClock did not copy, it's a reference")
	}

	// Second Done() call - should merge (element-wise max).
	clock2 := vectorclock.New()
	clock2.Set(0, 8)  // Lower than 10 - should NOT update
	clock2.Set(1, 12) // Higher than 5 - should update
	clock2.Set(2, 7)  // New thread - should set
	sv.MergeWaitGroupDoneClock(clock2)

	doneClock = sv.GetWaitGroupDoneClock()
	if doneClock.Get(0) != 10 {
		t.Errorf("Expected doneClock[0]=10 (max(10,8)), got %d", doneClock.Get(0))
	}
	if doneClock.Get(1) != 12 {
		t.Errorf("Expected doneClock[1]=12 (max(5,12)), got %d", doneClock.Get(1))
	}
	if doneClock.Get(2) != 7 {
		t.Errorf("Expected doneClock[2]=7 (new thread), got %d", doneClock.Get(2))
	}

	// Third Done() call - verify continued accumulation.
	clock3 := vectorclock.New()
	clock3.Set(0, 20)
	clock3.Set(3, 15)
	sv.MergeWaitGroupDoneClock(clock3)

	doneClock = sv.GetWaitGroupDoneClock()
	if doneClock.Get(0) != 20 {
		t.Errorf("Expected doneClock[0]=20 (max(10,20)), got %d", doneClock.Get(0))
	}
	if doneClock.Get(1) != 12 {
		t.Errorf("Expected doneClock[1]=12 (unchanged), got %d", doneClock.Get(1))
	}
	if doneClock.Get(2) != 7 {
		t.Errorf("Expected doneClock[2]=7 (unchanged), got %d", doneClock.Get(2))
	}
	if doneClock.Get(3) != 15 {
		t.Errorf("Expected doneClock[3]=15 (new thread), got %d", doneClock.Get(3))
	}
}

// TestSyncVar_WaitGroupState_Independent verifies WaitGroup state is independent.
func TestSyncVar_WaitGroupState_Independent(t *testing.T) {
	sv := &SyncVar{}

	// Set mutex release clock.
	mutexClock := vectorclock.New()
	mutexClock.Set(0, 10)
	sv.SetReleaseClock(mutexClock)

	// Set channel send clock.
	chanClock := vectorclock.New()
	chanClock.Set(1, 20)
	sv.SetChannelSendClock(chanClock)

	// Set WaitGroup done clock.
	wgClock := vectorclock.New()
	wgClock.Set(2, 30)
	sv.MergeWaitGroupDoneClock(wgClock)

	// Verify all three are independent.
	if sv.GetReleaseClock().Get(0) != 10 {
		t.Error("Mutex release clock was affected by other state")
	}
	if sv.GetChannelSendClock().Get(1) != 20 {
		t.Error("Channel send clock was affected by other state")
	}
	if sv.GetWaitGroupDoneClock().Get(2) != 30 {
		t.Error("WaitGroup done clock was affected by other state")
	}

	// Verify they don't share memory.
	if sv.GetReleaseClock() == sv.GetChannelSendClock() ||
		sv.GetReleaseClock() == sv.GetWaitGroupDoneClock() ||
		sv.GetChannelSendClock() == sv.GetWaitGroupDoneClock() {
		t.Error("Different sync primitives share memory")
	}
}

// TestSyncVar_WaitGroupCounterAndClock verifies counter and clock are synchronized.
func TestSyncVar_WaitGroupCounterAndClock(t *testing.T) {
	sv := &SyncVar{}

	// Simulate typical WaitGroup usage pattern:
	// Add(2) → Done() → Done()

	// Parent: Add(2)
	sv.WaitGroupAdd(2)
	if sv.GetWaitGroupCounter() != 2 {
		t.Errorf("Expected counter=2 after Add(2), got %d", sv.GetWaitGroupCounter())
	}

	// Child 1: Done()
	child1Clock := vectorclock.New()
	child1Clock.Set(1, 10)
	sv.MergeWaitGroupDoneClock(child1Clock)
	sv.WaitGroupAdd(-1) // Done is Add(-1)

	if sv.GetWaitGroupCounter() != 1 {
		t.Errorf("Expected counter=1 after first Done(), got %d", sv.GetWaitGroupCounter())
	}
	doneClock := sv.GetWaitGroupDoneClock()
	if doneClock.Get(1) != 10 {
		t.Errorf("Expected doneClock[1]=10, got %d", doneClock.Get(1))
	}

	// Child 2: Done()
	child2Clock := vectorclock.New()
	child2Clock.Set(2, 15)
	sv.MergeWaitGroupDoneClock(child2Clock)
	sv.WaitGroupAdd(-1) // Done is Add(-1)

	if sv.GetWaitGroupCounter() != 0 {
		t.Errorf("Expected counter=0 after second Done(), got %d", sv.GetWaitGroupCounter())
	}
	doneClock = sv.GetWaitGroupDoneClock()
	if doneClock.Get(1) != 10 || doneClock.Get(2) != 15 {
		t.Errorf("Expected doneClock[1]=10, [2]=15, got [1]=%d, [2]=%d",
			doneClock.Get(1), doneClock.Get(2))
	}

	// Counter=0 means Wait() can return, and waiter will merge doneClock.
}
