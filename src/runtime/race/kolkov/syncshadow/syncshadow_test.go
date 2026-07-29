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

// collidingAddresses returns addresses that map to the same top-level hash
// bucket. Keeping this in the package (rather than hard-coding addresses) makes
// the collision regression independent of the particular hash mixer.
func collidingAddresses(t *testing.T, count int) []uintptr {
	t.Helper()

	target := fastHashSync(1)
	addrs := make([]uintptr, 0, count)
	for page := uintptr(0); len(addrs) < count; page++ {
		addr := page<<syncPageShift | 1
		if fastHashSync(addr) == target {
			addrs = append(addrs, addr)
		}
		if page == ^uintptr(0)>>syncPageShift {
			t.Fatalf("could not find %d colliding addresses", count)
		}
	}
	return addrs
}

// TestGetOrCreate_CollisionOverflowPreservesEntries guards the happens-before
// state of every live synchronization object when a hash bucket is crowded.
func TestGetOrCreate_CollisionOverflowPreservesEntries(t *testing.T) {
	shadow := NewSyncShadow()
	addrs := collidingAddresses(t, 17)
	states := make([]*SyncVar, len(addrs))

	for i, addr := range addrs {
		states[i] = shadow.GetOrCreate(addr)
	}

	for i, addr := range addrs {
		if got := shadow.GetOrCreate(addr); got != states[i] {
			t.Fatalf("GetOrCreate(%#x) lost its SyncVar after collision overflow", addr)
		}
	}
}

// TestGetOrCreate_Concurrent verifies thread-safe concurrent access.
func TestGetOrCreate_Concurrent(t *testing.T) {
	shadow := NewSyncShadow()
	addr := uintptr(0x1234)
	numGoroutines := 100

	// Launch concurrent goroutines all accessing the same address.
	start := make(chan struct{})
	results := make(chan *SyncVar, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			<-start
			results <- shadow.GetOrCreate(addr)
		}()
	}
	close(start)

	// Collect all results.
	firstSV := <-results
	for i := 1; i < numGoroutines; i++ {
		sv := <-results
		if sv != firstSV {
			t.Errorf("Concurrent GetOrCreate returned different SyncVar instances")
		}
	}
}

// TestClearRange_AddressReuse verifies that allocator lifecycle clearing removes
// only entries in the half-open range and gives a reused address fresh HB state.
func TestClearRange_AddressReuse(t *testing.T) {
	shadow := NewSyncShadow()
	first := uintptr(0x1ff0)
	size := uintptr(0x30) // Crosses an application-page boundary.
	last := first + size - 1
	outside := []uintptr{first - 1, last + 1}
	inside := []uintptr{first, first + 7, 0x2000, last}

	outsideStates := make([]*SyncVar, len(outside))
	for i, addr := range outside {
		outsideStates[i] = shadow.GetOrCreate(addr)
	}
	insideStates := make([]*SyncVar, len(inside))
	clock := vectorclock.New()
	clock.Set(3, 9)
	for i, addr := range inside {
		insideStates[i] = shadow.GetOrCreate(addr)
		insideStates[i].SetReleaseClock(clock)
	}

	shadow.ClearRange(first, size)

	for i, addr := range outside {
		if !shadow.HasEntry(addr) {
			t.Errorf("ClearRange removed out-of-range entry %#x", addr)
		}
		if got := shadow.GetOrCreate(addr); got != outsideStates[i] {
			t.Errorf("ClearRange changed out-of-range SyncVar %#x", addr)
		}
	}
	for i, addr := range inside {
		if shadow.HasEntry(addr) {
			t.Errorf("ClearRange retained stale entry %#x", addr)
		}
		fresh := shadow.GetOrCreate(addr)
		if fresh == insideStates[i] {
			t.Errorf("reused address %#x retained its old SyncVar", addr)
		}
		if fresh.GetReleaseClock() != nil {
			t.Errorf("reused address %#x inherited a release clock", addr)
		}
	}
}

// TestClearRange_LargeSparseRange exercises the path which scans live page
// records instead of walking every page in a large, mostly empty span.
func TestClearRange_LargeSparseRange(t *testing.T) {
	shadow := NewSyncShadow()
	first := uintptr(0x1000)
	lastPage := first + uintptr(syncDirectClearPages+1)*(1<<syncPageShift)
	inside := []uintptr{first, first + 0x12345, lastPage + 17}
	outside := lastPage + 1<<syncPageShift

	for _, addr := range inside {
		shadow.GetOrCreate(addr)
	}
	outsideState := shadow.GetOrCreate(outside)
	shadow.ClearRange(first, lastPage+18-first)

	for _, addr := range inside {
		if shadow.HasEntry(addr) {
			t.Errorf("large ClearRange retained entry %#x", addr)
		}
	}
	if got := shadow.GetOrCreate(outside); got != outsideState {
		t.Fatal("large ClearRange removed the first out-of-range entry")
	}
}

// TestGetOrCreate_CachedHasNoAllocations protects the common sync lookup path.
func TestGetOrCreate_CachedHasNoAllocations(t *testing.T) {
	shadow := NewSyncShadow()
	addr := uintptr(0x1234)
	shadow.GetOrCreate(addr)

	if allocs := testing.AllocsPerRun(1000, func() {
		shadow.GetOrCreate(addr)
	}); allocs != 0 {
		t.Fatalf("cached GetOrCreate allocated %.2f times per lookup", allocs)
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
	if joined := sv.JoinReleaseClock(vectorclock.New()); joined {
		t.Fatal("JoinReleaseClock reported a release for an empty SyncVar")
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
	joined := vectorclock.New()
	if !sv.JoinReleaseClock(joined) {
		t.Fatal("JoinReleaseClock did not report the published release")
	}
	if joined.Get(0) != 10 || joined.Get(1) != 20 {
		t.Fatalf("joined release = {%d,%d}, want {10,20}", joined.Get(0), joined.Get(1))
	}

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

func TestSyncVarConcurrentReleaseAcquireSeesCompleteClock(t *testing.T) {
	var sv SyncVar
	first := vectorclock.New()
	first.Set(1, 11)
	first.Set(1<<20, 101)
	second := vectorclock.New()
	second.Set(1, 22)
	second.Set(1<<20, 202)
	sv.SetReleaseClock(first)

	const iterations = 2000
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() {
		<-start
		for i := 0; i < iterations; i++ {
			if i&1 == 0 {
				sv.SetReleaseClock(first)
			} else {
				sv.SetReleaseClock(second)
			}
		}
		done <- struct{}{}
	}()
	go func() {
		<-start
		for i := 0; i < iterations; i++ {
			acquired := vectorclock.New()
			sv.JoinReleaseClock(acquired)
			dense, sparse := acquired.Get(1), acquired.Get(1<<20)
			if !((dense == 11 && sparse == 101) || (dense == 22 && sparse == 202)) {
				t.Errorf("acquire observed partial release clock: dense=%d sparse=%d", dense, sparse)
				break
			}
		}
		done <- struct{}{}
	}()
	close(start)
	<-done
	<-done
}

// BenchmarkGetOrCreate_Cached measures the common synchronization lookup after
// the address owner and its page index have been published.
func BenchmarkGetOrCreate_Cached(b *testing.B) {
	shadow := NewSyncShadow()
	addr := uintptr(0x1234)
	want := shadow.GetOrCreate(addr)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := shadow.GetOrCreate(addr); got != want {
			b.Fatal("cached lookup changed SyncVar identity")
		}
	}
}
