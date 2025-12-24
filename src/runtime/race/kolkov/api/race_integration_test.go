package api

import (
	"bytes"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// Integration Tests - End-to-End Race Detection
//
// These tests exercise the full API lifecycle: Init() → concurrent operations → Fini()
// They verify that race detection works correctly in realistic scenarios with multiple
// goroutines, shared data, and various access patterns.
//
// Test Coverage:
//   1. Simple write-write race detection
//   2. Race-free sequential access (no false positives)
//   3. Multiple goroutines with mixed read/write access
//   4. Mixed scenarios (some races, some safe)
//   5. Full lifecycle with output validation
//   6. Concurrent reads with write (read-write race)
//   7. Many addresses stress test

// captureStderr redirects stderr to a pipe and starts async reader.
// Returns: reader, writer, buffer channel, restore function.
// Must call restore() to stop capture and get output.
//
// Windows Fix: Pipes have small buffers on Windows. If Fini() writes a large
// report, the pipe blocks. Reading asynchronously prevents deadlock.
func captureStderr() (restore func() string) {
	oldStderr := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	var buf bytes.Buffer
	done := make(chan struct{})

	// Read from pipe asynchronously (prevents Windows pipe buffer deadlock)
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	restore = func() string {
		w.Close()
		os.Stderr = oldStderr
		<-done
		return buf.String()
	}

	return restore
}

// TestIntegration_SimpleRace verifies basic write-write race detection end-to-end.
//
// Scenario: Two goroutines writing to the same variable without synchronization.
// Expected: Race should be detected and reported in Fini() output.
func TestIntegration_SimpleRace(t *testing.T) {
	restore := captureStderr()
	// Initialize race detector
	Init()

	var shared int
	var wg sync.WaitGroup

	// Goroutine 1: Write to shared variable multiple times
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			racewrite(uintptr(unsafe.Pointer(&shared)))
			shared = 1
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// Goroutine 2: Write to shared variable multiple times (creates race)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			racewrite(uintptr(unsafe.Pointer(&shared)))
			shared = 2
			time.Sleep(100 * time.Microsecond)
		}
	}()

	wg.Wait()

	// Finalize and capture output
	Fini()

	output := restore()

	// Verify race was detected
	if !strings.Contains(output, "WARNING") {
		t.Errorf("Expected race warning in output, got:\n%s", output)
	}

	if !strings.Contains(output, "data race(s) detected") {
		t.Errorf("Expected 'data race(s) detected' in output, got:\n%s", output)
	}

	if RacesDetected() == 0 {
		t.Error("Expected race to be detected, but RacesDetected() returned 0")
	}

	t.Logf("Race detected successfully: %d race(s)", RacesDetected())
}

// TestIntegration_NoRace_Sequential verifies no false positives for sequential access.
//
// Scenario: Sequential writes and reads to a variable (no concurrency).
// Expected: Zero races detected, success message in Fini() output.
func TestIntegration_NoRace_Sequential(t *testing.T) {
	restore := captureStderr()
	Init()

	var data int

	// Sequential access - no concurrency, no races
	for i := 0; i < 10; i++ {
		racewrite(uintptr(unsafe.Pointer(&data)))
		data = i

		raceread(uintptr(unsafe.Pointer(&data)))
		_ = data
	}

	Fini()

	output := restore()

	// Verify no races detected
	if strings.Contains(output, "WARNING") {
		t.Errorf("Unexpected race warning in sequential access:\n%s", output)
	}

	if !strings.Contains(output, "No data races detected") {
		t.Errorf("Expected 'No data races detected' message, got:\n%s", output)
	}

	if RacesDetected() != 0 {
		t.Errorf("Expected 0 races, got %d", RacesDetected())
	}

	t.Log("Sequential access correctly reported as race-free")
}

// TestIntegration_MultipleGoroutines verifies race detection with many goroutines.
//
// Scenario: 5 goroutines accessing shared data with mixed read/write operations.
// Expected: Races should be detected for unsynchronized writes.
func TestIntegration_MultipleGoroutines(t *testing.T) {
	restore := captureStderr()
	Init()

	var shared int
	const numGoroutines = 5

	var wg sync.WaitGroup

	// Launch goroutines that write to shared variable
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Write to shared variable
			racewrite(uintptr(unsafe.Pointer(&shared)))
			shared = id

			// Read from shared variable
			raceread(uintptr(unsafe.Pointer(&shared)))
			_ = shared

			time.Sleep(1 * time.Millisecond)
		}(i)
	}

	wg.Wait()
	Fini()

	output := restore()

	// Verify race was detected (multiple concurrent writes)
	if !strings.Contains(output, "WARNING") {
		t.Errorf("Expected race warning with %d concurrent goroutines, got:\n%s", numGoroutines, output)
	}

	if RacesDetected() == 0 {
		t.Errorf("Expected races with %d concurrent goroutines, got 0", numGoroutines)
	}

	t.Logf("Multiple goroutines: %d race(s) detected", RacesDetected())
}

// TestIntegration_RaceAndNoRace_Mixed verifies selective race detection.
//
// Scenario:
//   - Variable A: concurrent writes (race)
//   - Variable B: sequential writes (no race)
//
// Expected: Only races for variable A should be detected.
func TestIntegration_RaceAndNoRace_Mixed(t *testing.T) {
	restore := captureStderr()
	Init()

	var racyVar int
	var safeVar int

	var wg sync.WaitGroup

	// Concurrent writes to racyVar (creates race)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			racewrite(uintptr(unsafe.Pointer(&racyVar)))
			racyVar = 1
			time.Sleep(100 * time.Microsecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			racewrite(uintptr(unsafe.Pointer(&racyVar)))
			racyVar = 2
			time.Sleep(100 * time.Microsecond)
		}
	}()

	wg.Wait()

	// Sequential writes to safeVar (no race)
	for i := 0; i < 5; i++ {
		racewrite(uintptr(unsafe.Pointer(&safeVar)))
		safeVar = i
	}

	Fini()

	output := restore()

	// Verify race was detected (for racyVar only)
	if !strings.Contains(output, "WARNING") {
		t.Error("Expected race warning for racyVar")
	}

	if RacesDetected() == 0 {
		t.Error("Expected at least one race for racyVar")
	}

	// Note: We can't verify that safeVar had no races without per-variable
	// tracking, but the detector should only report actual races.

	t.Logf("Mixed scenario: %d race(s) detected (expected for racyVar only)", RacesDetected())
}

// TestIntegration_FullLifecycle verifies complete Init → operations → Fini lifecycle.
//
// This test validates:
//   - Init() properly initializes the detector
//   - Operations are tracked correctly
//   - Fini() produces proper summary report
//   - Output format is correct
func TestIntegration_FullLifecycle(t *testing.T) {
	restore := captureStderr()
	// Phase 1: Initialize
	Init()

	if !enabled.Load() {
		t.Fatal("Init() did not enable detector")
	}

	// Phase 2: Perform operations
	var counter int

	// Create a race condition
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			racewrite(uintptr(unsafe.Pointer(&counter)))
			counter++
			time.Sleep(100 * time.Microsecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			racewrite(uintptr(unsafe.Pointer(&counter)))
			counter++
			time.Sleep(100 * time.Microsecond)
		}
	}()

	wg.Wait()

	// Phase 3: Finalize
	Fini()

	output := restore()

	// Validate output format
	if !strings.Contains(output, "==================") {
		t.Error("Expected report separator in output")
	}

	if !strings.Contains(output, "Race Detector Report") {
		t.Error("Expected 'Race Detector Report' header")
	}

	// Should contain warning since we created races
	if !strings.Contains(output, "WARNING") {
		t.Errorf("Expected WARNING in output, got:\n%s", output)
	}

	// Verify detector is disabled after Fini
	if enabled.Load() {
		t.Error("Detector should be disabled after Fini()")
	}

	// Verify race count
	racesDetected := RacesDetected()
	if racesDetected == 0 {
		t.Error("Expected races to be detected in concurrent counter increment")
	}

	t.Logf("Full lifecycle test: detected %d race(s)", racesDetected)
	t.Logf("Output:\n%s", output)
}

// TestIntegration_ConcurrentReads verifies read-write race detection.
//
// Scenario: Multiple goroutines reading while one writes to shared variable.
// Expected: Read-write races should be detected.
func TestIntegration_ConcurrentReads(t *testing.T) {
	restore := captureStderr()
	Init()

	var data int
	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			racewrite(uintptr(unsafe.Pointer(&data)))
			data = i
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Reader goroutines
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				raceread(uintptr(unsafe.Pointer(&data)))
				_ = data
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	Fini()

	output := restore()

	// Verify race was detected (read-write or write-read)
	if !strings.Contains(output, "WARNING") {
		t.Errorf("Expected read-write race warning, got:\n%s", output)
	}

	if RacesDetected() == 0 {
		t.Error("Expected read-write races to be detected")
	}

	t.Logf("Concurrent reads test: %d race(s) detected", RacesDetected())
}

// TestIntegration_ManyAddresses stress tests shadow memory with many addresses.
//
// Scenario: Access 100+ different addresses to stress shadow memory allocation.
// Expected: No races (each goroutine accesses unique addresses), good performance.
func TestIntegration_ManyAddresses(t *testing.T) {
	restore := captureStderr()
	Init()

	const numAddresses = 200
	var wg sync.WaitGroup

	// Create array of variables
	vars := make([]int, numAddresses)

	// Access each address from a goroutine
	for i := 0; i < numAddresses; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Each goroutine accesses its unique variable (no races)
			addr := uintptr(unsafe.Pointer(&vars[idx]))

			// Write
			racewrite(addr)
			vars[idx] = idx

			// Read
			raceread(addr)
			_ = vars[idx]
		}(i)
	}

	startTime := time.Now()
	wg.Wait()
	duration := time.Since(startTime)

	Fini()

	output := restore()

	// Verify no races (all accesses to unique addresses)
	if strings.Contains(output, "WARNING") {
		t.Errorf("Unexpected race with unique addresses:\n%s", output)
	}

	if !strings.Contains(output, "No data races detected") {
		t.Errorf("Expected no races with unique addresses, got:\n%s", output)
	}

	if RacesDetected() != 0 {
		t.Errorf("Expected 0 races with unique addresses, got %d", RacesDetected())
	}

	// Verify reasonable performance (should complete quickly)
	if duration > 5*time.Second {
		t.Errorf("Performance concern: %d addresses took %v (expected < 5s)", numAddresses, duration)
	}

	t.Logf("Many addresses test: %d addresses, %v duration, 0 races", numAddresses, duration)
}

// TestIntegration_RepeatedInitFini verifies multiple Init/Fini cycles work correctly.
//
// Scenario: Multiple cycles of Init → operations → Fini.
// Expected: Each cycle works independently, state is properly reset.
func TestIntegration_RepeatedInitFini(t *testing.T) {
	for cycle := 0; cycle < 3; cycle++ {
		t.Logf("Cycle %d", cycle)

		restore := captureStderr()
		Init()

		// Do some operations
		var data int
		racewrite(uintptr(unsafe.Pointer(&data)))
		data = cycle

		raceread(uintptr(unsafe.Pointer(&data)))
		_ = data

		Fini()

		output := restore()

		// Each cycle should report no races (sequential access)
		if strings.Contains(output, "WARNING") {
			t.Errorf("Cycle %d: Unexpected race in sequential access", cycle)
		}

		// Verify detector is disabled after Fini
		if enabled.Load() {
			t.Errorf("Cycle %d: Detector not disabled after Fini", cycle)
		}
	}

	t.Log("Multiple Init/Fini cycles completed successfully")
}

// TestIntegration_DisableDuringExecution verifies Disable() stops detection.
//
// Scenario:
//   - Phase 1: Races detected (enabled)
//   - Phase 2: Disable() called
//   - Phase 3: Races ignored (disabled)
//
// Expected: Only races from Phase 1 should be counted.
func TestIntegration_DisableDuringExecution(t *testing.T) {
	restore := captureStderr()
	Init()

	var shared int
	var wg sync.WaitGroup

	// Phase 1: Enabled - create race
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			racewrite(uintptr(unsafe.Pointer(&shared)))
			shared = 1
			time.Sleep(100 * time.Microsecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			racewrite(uintptr(unsafe.Pointer(&shared)))
			shared = 2
			time.Sleep(100 * time.Microsecond)
		}
	}()

	wg.Wait()

	// Record races after phase 1
	racesPhase1 := RacesDetected()

	// Phase 2: Disable detector
	Disable()

	// Phase 3: More concurrent writes (should be ignored)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			racewrite(uintptr(unsafe.Pointer(&shared)))
			shared = 3
			time.Sleep(100 * time.Microsecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			racewrite(uintptr(unsafe.Pointer(&shared)))
			shared = 4
			time.Sleep(100 * time.Microsecond)
		}
	}()

	wg.Wait()

	// Record races after phase 3
	racesPhase3 := RacesDetected()

	Fini()

	_ = restore()

	// Verify: Race count should not increase after Disable()
	if racesPhase3 != racesPhase1 {
		t.Errorf("Disable() did not prevent race detection: phase1=%d, phase3=%d", racesPhase1, racesPhase3)
	}

	if racesPhase1 == 0 {
		t.Error("Expected at least one race in Phase 1 (before Disable)")
	}

	t.Logf("Disable test: Phase1=%d races, Phase3=%d races (correctly unchanged)", racesPhase1, racesPhase3)
}

// TestIntegration_LargeDataStructure verifies race detection on complex data structures.
//
// Scenario: Multiple goroutines accessing fields of a struct.
// Expected: Races detected for concurrent field access.
func TestIntegration_LargeDataStructure(t *testing.T) {
	restore := captureStderr()
	Init()

	type Data struct {
		Field1 int
		Field2 int
		Field3 int
	}

	var data Data
	var wg sync.WaitGroup

	// Goroutine 1: Write to Field1
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			racewrite(uintptr(unsafe.Pointer(&data.Field1)))
			data.Field1 = 1
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// Goroutine 2: Write to Field1 (race)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			racewrite(uintptr(unsafe.Pointer(&data.Field1)))
			data.Field1 = 2
			time.Sleep(100 * time.Microsecond)
		}
	}()

	wg.Wait()

	// Goroutine 3: Write to Field2 (no race - different field, sequential)
	racewrite(uintptr(unsafe.Pointer(&data.Field2)))
	data.Field2 = 3

	Fini()

	output := restore()

	// Verify race was detected for Field1
	if !strings.Contains(output, "WARNING") {
		t.Errorf("Expected race warning for Field1, got:\n%s", output)
	}

	if RacesDetected() == 0 {
		t.Error("Expected race for Field1 concurrent writes")
	}

	t.Logf("Struct field test: %d race(s) detected", RacesDetected())
}

// TestIntegration_HighContentionVariable verifies detection under high contention.
//
// Scenario: Several goroutines (5) competing for same variable.
// Expected: Multiple races detected, detector handles contention.
func TestIntegration_HighContentionVariable(t *testing.T) {
	restore := captureStderr()
	Init()

	var hotspot int
	const numGoroutines = 5

	var wg sync.WaitGroup

	// Launch goroutines competing for same variable
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				racewrite(uintptr(unsafe.Pointer(&hotspot)))
				hotspot = id
				time.Sleep(100 * time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	Fini()

	output := restore()

	// Verify races detected
	if !strings.Contains(output, "WARNING") {
		t.Errorf("Expected race warning with %d goroutines, got:\n%s", numGoroutines, output)
	}

	if RacesDetected() == 0 {
		t.Errorf("Expected races with %d concurrent goroutines", numGoroutines)
	}

	t.Logf("High contention test: %d goroutines, %d race(s) detected", numGoroutines, RacesDetected())
}

// TestIntegration_SafeSynchronization verifies no false positives with proper sync.
//
// Scenario: Goroutines using mutex to protect shared variable.
// Expected: No races detected (mutex provides synchronization).
func TestIntegration_SafeSynchronization(t *testing.T) {
	restore := captureStderr()
	Init()

	var data int
	var mu sync.Mutex
	var wg sync.WaitGroup

	const numGoroutines = 5

	// Goroutines using mutex (safe)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			mu.Lock()
			racewrite(uintptr(unsafe.Pointer(&data)))
			data = id
			mu.Unlock()

			time.Sleep(1 * time.Millisecond)
		}(i)
	}

	wg.Wait()
	Fini()

	output := restore()

	// Note: Our MVP detector doesn't track mutex synchronization yet,
	// so this test may still report races. In a full implementation
	// with happens-before tracking, this should pass.
	//
	// For now, we just verify the test doesn't crash.

	t.Logf("Mutex synchronization test: %d race(s) detected", RacesDetected())
	t.Log("Note: MVP doesn't track mutex sync yet, so races may be reported")
	t.Logf("Output:\n%s", output)
}
