package api

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"runtime/race/kolkov/vectorclock"
)

// poolSize returns the current number of free TIDs in the pool.
// Must be called without tidPoolMu held.
func poolSize() int {
	tidPoolMu.lock()
	n := len(freeTIDs)
	tidPoolMu.unlock()
	return n
}

// expectedPoolSize is MaxThreads-1 (TID 0 is excluded as sentinel).
var expectedPoolSize = vectorclock.MaxThreads - 1

// TestTIDPoolInitialization verifies TID pool starts with MaxThreads-1 TIDs.
func TestTIDPoolInitialization(t *testing.T) {
	initTIDPool()

	n := poolSize()
	if n != expectedPoolSize {
		t.Errorf("TID pool size = %d, want %d", n, expectedPoolSize)
	}

	// Verify first TID is 1 (TID 0 excluded) and last is MaxThreads-1.
	tidPoolMu.lock()
	if freeTIDs[0] != 1 {
		t.Errorf("freeTIDs[0] = %d, want 1", freeTIDs[0])
	}
	last := freeTIDs[len(freeTIDs)-1]
	if last != uint16(vectorclock.MaxThreads-1) {
		t.Errorf("freeTIDs[last] = %d, want %d", last, vectorclock.MaxThreads-1)
	}
	tidPoolMu.unlock()
}

// TestTIDAllocation verifies TID allocation from pool.
func TestTIDAllocation(t *testing.T) {
	initTIDPool()

	tid, startClock := allocTID()

	// Should get TID 1 (first in pool, TID 0 is excluded).
	if tid != 1 {
		t.Errorf("First allocTID() = %d, want 1", tid)
	}
	if startClock < 1 {
		t.Errorf("startClock = %d, want >= 1", startClock)
	}

	n := poolSize()
	if n != expectedPoolSize-1 {
		t.Errorf("After allocation, pool size = %d, want %d", n, expectedPoolSize-1)
	}
}

// TestTIDAllocationSequential verifies TIDs allocated sequentially.
func TestTIDAllocationSequential(t *testing.T) {
	initTIDPool()

	tids := make([]uint16, 10)
	for i := 0; i < 10; i++ {
		tids[i], _ = allocTID()
	}

	// Should get TIDs: 1, 2, 3, ..., 10 (TID 0 excluded from pool).
	for i := 0; i < 10; i++ {
		expected := uint16(i + 1)
		if tids[i] != expected {
			t.Errorf("TID %d = %d, want %d", i, tids[i], expected)
		}
	}

	n := poolSize()
	if n != expectedPoolSize-10 {
		t.Errorf("After 10 allocations, pool size = %d, want %d", n, expectedPoolSize-10)
	}
}

// TestTIDFree verifies TID is returned to pool.
func TestTIDFree(t *testing.T) {
	initTIDPool()

	tid, _ := allocTID()

	n := poolSize()
	if n != expectedPoolSize-1 {
		t.Errorf("After allocation, pool size = %d, want %d", n, expectedPoolSize-1)
	}

	// Free the TID with clock=1.
	freeTID(tid, 1)

	n = poolSize()
	if n != expectedPoolSize {
		t.Errorf("After freeing, pool size = %d, want %d", n, expectedPoolSize)
	}
}

// TestTIDReuse verifies freed TID is reused with bumped clock.
func TestTIDReuse(t *testing.T) {
	initTIDPool()

	// Allocate TID 1.
	tid1, _ := allocTID()
	if tid1 != 1 {
		t.Fatalf("First allocation = %d, want 1", tid1)
	}

	// Free TID 1 with clock=10.
	freeTID(tid1, 10)

	// Next allocation should get TID 2 (FIFO: freed TID goes to back).
	tid2, _ := allocTID()
	if tid2 != 2 {
		t.Errorf("Second allocation after free = %d, want 2", tid2)
	}

	// Allocate remaining pool until we get the recycled TID 1 back.
	// After draining the pool, the freed TID 1 should come back with bumped clock.
	var recycledClock uint32
	for i := 0; i < expectedPoolSize; i++ {
		tid, clock := allocTID()
		if tid == tid1 {
			recycledClock = clock
			break
		}
		_ = clock
	}

	// Recycled TID should have clock > 10 (the clock at free time).
	if recycledClock <= 10 {
		t.Errorf("Recycled TID clock = %d, want > 10", recycledClock)
	}
}

// TestTIDConcurrentAllocation verifies concurrent TID allocation is safe.
func TestTIDConcurrentAllocation(t *testing.T) {
	initTIDPool()

	const numGoroutines = 100
	tids := make([]uint16, numGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tids[idx], _ = allocTID()
		}(i)
	}

	wg.Wait()

	// Verify all TIDs are unique.
	tidSet := make(map[uint16]bool)
	for i, tid := range tids {
		if tidSet[tid] {
			t.Errorf("Duplicate TID %d at index %d", tid, i)
		}
		tidSet[tid] = true
	}

	if len(tidSet) != numGoroutines {
		t.Errorf("Expected %d unique TIDs, got %d", numGoroutines, len(tidSet))
	}
}

// TestTIDConcurrentFree verifies concurrent TID free is safe.
func TestTIDConcurrentFree(t *testing.T) {
	initTIDPool()

	const count = 100
	type tidInfo struct {
		tid   uint16
		clock uint32
	}
	tids := make([]tidInfo, count)
	for i := 0; i < count; i++ {
		tid, clock := allocTID()
		tids[i] = tidInfo{tid, clock}
	}

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			freeTID(tids[idx].tid, tids[idx].clock)
		}(i)
	}

	wg.Wait()

	n := poolSize()
	if n != expectedPoolSize {
		t.Errorf("After concurrent free, pool size = %d, want %d", n, expectedPoolSize)
	}
}

// TestParseAllGIDs verifies parsing of runtime.Stack output.
func TestParseAllGIDs(t *testing.T) {
	stackTrace := []byte(`goroutine 1 [running]:
main.main()
	/path/to/main.go:10 +0x20

goroutine 5 [chan receive]:
main.worker()
	/path/to/worker.go:20 +0x40

goroutine 123 [semacquire]:
sync.(*WaitGroup).Wait()
	/path/to/sync.go:30 +0x60
`)

	gids := parseAllGIDs(stackTrace)

	expected := []int64{1, 5, 123}
	if len(gids) != len(expected) {
		t.Fatalf("parseAllGIDs() returned %d GIDs, want %d", len(gids), len(expected))
	}

	for i, gid := range gids {
		if gid != expected[i] {
			t.Errorf("GID %d = %d, want %d", i, gid, expected[i])
		}
	}
}

// TestParseAllGIDs_EmptyInput verifies parsing empty input.
func TestParseAllGIDs_EmptyInput(t *testing.T) {
	gids := parseAllGIDs([]byte{})
	if len(gids) != 0 {
		t.Errorf("parseAllGIDs(empty) returned %d GIDs, want 0", len(gids))
	}
}

// TestParseAllGIDs_NoGoroutines verifies parsing with no goroutine lines.
func TestParseAllGIDs_NoGoroutines(t *testing.T) {
	stackTrace := []byte("some random text\nwithout goroutine lines\n")
	gids := parseAllGIDs(stackTrace)
	if len(gids) != 0 {
		t.Errorf("parseAllGIDs(no goroutines) returned %d GIDs, want 0", len(gids))
	}
}

// TestGetLiveGoroutineIDs verifies we can get all live GIDs.
func TestGetLiveGoroutineIDs(t *testing.T) {
	done := make(chan bool)
	const numGoroutines = 5

	for i := 0; i < numGoroutines; i++ {
		go func() {
			<-done
		}()
	}

	gids := getLiveGoroutineIDs()

	if len(gids) < numGoroutines+1 {
		t.Errorf("getLiveGoroutineIDs() returned %d GIDs, want >= %d", len(gids), numGoroutines+1)
	}

	gidSet := make(map[int64]bool)
	for _, gid := range gids {
		if gidSet[gid] {
			t.Errorf("Duplicate GID %d", gid)
		}
		gidSet[gid] = true
	}

	close(done)
}

// TestCleanupDeadGoroutines verifies cleanup reclaims TIDs.
func TestCleanupDeadGoroutines(t *testing.T) {
	Init()

	testGID := getGoroutineID()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := getCurrentContext()
			_ = ctx
		}()
	}
	wg.Wait()

	poolSizeBefore := poolSize()

	cleanupDeadGoroutines()
	time.Sleep(10 * time.Millisecond)

	poolSizeAfter := poolSize()

	if poolSizeAfter < poolSizeBefore {
		t.Errorf("Pool size after cleanup = %d, decreased from %d (expected increase)", poolSizeAfter, poolSizeBefore)
	}

	t.Logf("Test GID: %d, Pool before: %d, Pool after: %d", testGID, poolSizeBefore, poolSizeAfter)
}

// TestMaybeCleanup verifies periodic cleanup is triggered.
func TestMaybeCleanup(t *testing.T) {
	Init()

	allocCounter.Store(0)

	for i := 0; i < 1000; i++ {
		maybeCleanup()
	}

	count := allocCounter.Load()
	if count != 1000 {
		t.Errorf("After 1000 maybeCleanup calls, counter = %d, want 1000", count)
	}

	time.Sleep(50 * time.Millisecond)
}

// TestIntegration_1000Goroutines tests 1000 concurrent goroutines with TID reuse.
func TestIntegration_1000Goroutines(t *testing.T) {
	Init()

	const numGoroutines = 1000
	const batchSize = 100

	for batch := 0; batch < numGoroutines/batchSize; batch++ {
		var wg sync.WaitGroup

		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx := getCurrentContext()
				_ = ctx.TID
			}()
		}

		wg.Wait()

		if batch%10 == 0 {
			cleanupDeadGoroutines()
			time.Sleep(10 * time.Millisecond)
		}
	}

	if enabled.Load() == 0 {
		t.Error("Detector disabled after 1000 goroutines")
	}

	cleanupDeadGoroutines()
	time.Sleep(100 * time.Millisecond)

	n := poolSize()
	if n < 150 {
		t.Errorf("After 1000 goroutines with cleanup, pool size = %d, want >= 150", n)
	}

	t.Logf("After 1000 goroutines: pool size = %d, detector enabled = %v", n, enabled.Load())
}

// TestIntegration_LongLivedAndShortLived tests mix of goroutine lifetimes.
func TestIntegration_LongLivedAndShortLived(t *testing.T) {
	Init()

	longLivedDone := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			ctx := getCurrentContext()
			_ = ctx
			<-longLivedDone
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := getCurrentContext()
			_ = ctx
		}()
	}
	wg.Wait()

	cleanupDeadGoroutines()
	time.Sleep(10 * time.Millisecond)

	n := poolSize()
	if n < 200 {
		t.Errorf("After mixed lifetimes, pool size = %d, want >= 200", n)
	}

	close(longLivedDone)

	t.Logf("Mixed lifetimes: pool size = %d", n)
}

// TestTIDPoolThreadSafety verifies TID pool operations are thread-safe.
func TestTIDPoolThreadSafety(t *testing.T) {
	initTIDPool()

	const numWorkers = 50
	const operationsPerWorker = 100

	var wg sync.WaitGroup

	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for op := 0; op < operationsPerWorker; op++ {
				tid, clock := allocTID()
				runtime.Gosched()
				freeTID(tid, clock)
			}
		}()
	}

	wg.Wait()

	n := poolSize()
	if n != expectedPoolSize {
		t.Errorf("After concurrent alloc/free, pool size = %d, want %d", n, expectedPoolSize)
	}
}

// BenchmarkAllocTID benchmarks TID allocation.
func BenchmarkAllocTID(b *testing.B) {
	initTIDPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = allocTID()
		if i%expectedPoolSize == expectedPoolSize-1 {
			initTIDPool()
		}
	}
}

// BenchmarkFreeTID benchmarks TID free.
func BenchmarkFreeTID(b *testing.B) {
	initTIDPool()

	const count = 256
	type tidInfo struct {
		tid   uint16
		clock uint32
	}
	tids := make([]tidInfo, count)
	for i := 0; i < count; i++ {
		tid, clock := allocTID()
		tids[i] = tidInfo{tid, clock}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ti := tids[i%count]
		freeTID(ti.tid, ti.clock)
	}
}

// BenchmarkGetLiveGoroutineIDs benchmarks goroutine ID enumeration.
func BenchmarkGetLiveGoroutineIDs(b *testing.B) {
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func() { <-done }()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = getLiveGoroutineIDs()
	}

	close(done)
}

// BenchmarkCleanupDeadGoroutines benchmarks cleanup with realistic goroutine count.
func BenchmarkCleanupDeadGoroutines(b *testing.B) {
	Init()

	for i := 0; i < 100; i++ {
		go func() {
			ctx := getCurrentContext()
			_ = ctx
			time.Sleep(time.Millisecond)
		}()
	}

	time.Sleep(50 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cleanupDeadGoroutines()
	}
}

// BenchmarkMaybeCleanup benchmarks cleanup trigger check.
func BenchmarkMaybeCleanup(b *testing.B) {
	Init()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		maybeCleanup()
	}
}
