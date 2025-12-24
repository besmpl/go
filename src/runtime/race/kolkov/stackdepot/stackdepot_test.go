package stackdepot

import (
	"strings"
	"sync"
	"testing"
)

// TestCaptureStack tests basic stack capture and retrieval.
func TestCaptureStack(t *testing.T) {
	Reset() // Clean slate for test.

	// Capture current stack.
	hash := CaptureStack()

	if hash == 0 {
		t.Fatal("CaptureStack returned zero hash")
	}

	// Retrieve stack by hash.
	stack := GetStack(hash)
	if stack == nil {
		t.Fatal("GetStack returned nil for valid hash")
	}

	// Verify stack has non-zero program counters.
	hasNonZero := false
	for _, pc := range stack.PC {
		if pc != 0 {
			hasNonZero = true
			break
		}
	}

	if !hasNonZero {
		t.Error("Stack has no non-zero program counters")
	}
}

// TestStackDeduplication tests that identical stacks produce the same hash.
func TestStackDeduplication(t *testing.T) {
	Reset() // Clean slate.

	// Capture stack twice from within a loop at the same location.
	// This ensures both captures have identical call stacks.
	var hash1, hash2 uint64
	for i := 0; i < 2; i++ {
		hash := CaptureStack()
		if i == 0 {
			hash1 = hash
		} else {
			hash2 = hash
		}
	}

	if hash1 == 0 || hash2 == 0 {
		t.Fatal("CaptureStack returned zero hash")
	}

	// Hashes should be equal (same call site → same stack).
	if hash1 != hash2 {
		t.Errorf("Expected same hash for same stack, got %x != %x", hash1, hash2)
	}

	// Should retrieve the same StackTrace object.
	stack1 := GetStack(hash1)
	stack2 := GetStack(hash2)

	if stack1 != stack2 {
		t.Error("Expected same StackTrace pointer (deduplication)")
	}

	// Verify only one unique stack in depot.
	uniqueStacks, _ := Stats()
	if uniqueStacks != 1 {
		t.Errorf("Expected 1 unique stack after deduplication, got %d", uniqueStacks)
	}
}

// TestGetStackNotFound tests retrieval of non-existent hash.
func TestGetStackNotFound(t *testing.T) {
	Reset()

	// Try to get stack with hash that doesn't exist.
	stack := GetStack(0x123456789abcdef0)

	if stack != nil {
		t.Error("Expected nil for non-existent hash")
	}
}

// TestGetStackZeroHash tests that zero hash returns nil.
func TestGetStackZeroHash(t *testing.T) {
	stack := GetStack(0)

	if stack != nil {
		t.Error("Expected nil for zero hash")
	}
}

// TestFormatStack tests stack trace formatting.
func TestFormatStack(t *testing.T) {
	Reset()

	// Capture stack from this test function.
	hash := CaptureStack()
	stack := GetStack(hash)

	if stack == nil {
		t.Fatal("Failed to capture stack")
	}

	// Format the stack.
	formatted := stack.FormatStack()

	if formatted == "" {
		t.Error("FormatStack returned empty string")
	}

	// Should contain the test function name.
	if !strings.Contains(formatted, "TestFormatStack") {
		t.Errorf("Stack should contain test function name, got:\n%s", formatted)
	}

	// Should contain file name.
	if !strings.Contains(formatted, "stackdepot_test.go") {
		t.Errorf("Stack should contain file name, got:\n%s", formatted)
	}

	// Should have proper formatting (function name with parentheses).
	if !strings.Contains(formatted, "()") {
		t.Errorf("Stack should have function names with (), got:\n%s", formatted)
	}
}

// TestFormatStackNil tests formatting of nil stack.
func TestFormatStackNil(t *testing.T) {
	var stack *StackTrace
	formatted := stack.FormatStack()

	expected := "  <unknown>\n"
	if formatted != expected {
		t.Errorf("Expected %q, got %q", expected, formatted)
	}
}

// TestHashStackDifferentStacks tests that different stacks produce different hashes.
func TestHashStackDifferentStacks(t *testing.T) {
	Reset()

	// Capture stacks from different call sites.
	hash1 := captureFromSite1()
	hash2 := captureFromSite2()

	if hash1 == 0 || hash2 == 0 {
		t.Fatal("CaptureStack returned zero hash")
	}

	// Different call sites should produce different hashes.
	if hash1 == hash2 {
		t.Error("Expected different hashes for different call sites")
	}

	// Should have 2 unique stacks.
	uniqueStacks, _ := Stats()
	if uniqueStacks != 2 {
		t.Errorf("Expected 2 unique stacks, got %d", uniqueStacks)
	}
}

// captureFromSite1 captures stack from call site 1.
func captureFromSite1() uint64 {
	return CaptureStack()
}

// captureFromSite2 captures stack from call site 2.
func captureFromSite2() uint64 {
	return CaptureStack()
}

// TestConcurrentCapture tests concurrent stack capture (thread safety).
func TestConcurrentCapture(t *testing.T) {
	Reset()

	const numGoroutines = 100
	const capturesPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	hashes := make(chan uint64, numGoroutines*capturesPerGoroutine)

	// Launch goroutines that capture stacks concurrently.
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < capturesPerGoroutine; j++ {
				hash := CaptureStack()
				hashes <- hash
			}
		}()
	}

	wg.Wait()
	close(hashes)

	// Verify all captures succeeded (non-zero hash).
	captureCount := 0
	for hash := range hashes {
		captureCount++
		if hash == 0 {
			t.Error("CaptureStack returned zero hash during concurrent capture")
		}

		// Verify stack is retrievable.
		stack := GetStack(hash)
		if stack == nil {
			t.Errorf("GetStack returned nil for hash %x", hash)
		}
	}

	expectedCaptures := numGoroutines * capturesPerGoroutine
	if captureCount != expectedCaptures {
		t.Errorf("Expected %d captures, got %d", expectedCaptures, captureCount)
	}

	// All captures from same goroutine should have same stack (same call site).
	// But different goroutines may have different stacks.
	// We just verify that depot didn't crash and all hashes are valid.
	uniqueStacks, totalMemory := Stats()
	t.Logf("Unique stacks: %d, Total memory: %d bytes", uniqueStacks, totalMemory)

	if uniqueStacks == 0 {
		t.Error("Expected at least one unique stack")
	}
}

// TestReset tests that Reset clears the depot.
func TestReset(t *testing.T) {
	// Capture some stacks.
	_ = CaptureStack()
	_ = CaptureStack()

	// Verify depot has entries.
	uniqueStacks, _ := Stats()
	if uniqueStacks == 0 {
		t.Fatal("Expected non-empty depot before Reset")
	}

	// Reset.
	Reset()

	// Verify depot is empty.
	uniqueStacks, totalMemory := Stats()
	if uniqueStacks != 0 {
		t.Errorf("Expected empty depot after Reset, got %d unique stacks", uniqueStacks)
	}
	if totalMemory != 0 {
		t.Errorf("Expected zero memory after Reset, got %d bytes", totalMemory)
	}
}

// TestStats tests the Stats function.
func TestStats(t *testing.T) {
	Reset()

	// Initially empty.
	uniqueStacks, totalMemory := Stats()
	if uniqueStacks != 0 {
		t.Errorf("Expected 0 unique stacks initially, got %d", uniqueStacks)
	}
	if totalMemory != 0 {
		t.Errorf("Expected 0 memory initially, got %d", totalMemory)
	}

	// Capture from different call sites.
	_ = captureFromSite1()
	_ = captureFromSite2()

	// Should have 2 unique stacks.
	uniqueStacks, totalMemory = Stats()
	if uniqueStacks != 2 {
		t.Errorf("Expected 2 unique stacks, got %d", uniqueStacks)
	}

	// Memory should be reasonable (96 bytes per stack: 64 bytes StackTrace + 32 bytes overhead).
	expectedMemory := int64(2 * 96)
	if totalMemory != expectedMemory {
		t.Errorf("Expected %d bytes, got %d", expectedMemory, totalMemory)
	}
}

// BenchmarkCaptureStack benchmarks stack capture performance.
func BenchmarkCaptureStack(b *testing.B) {
	Reset()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = CaptureStack()
	}
}

// BenchmarkCaptureStackDeduplication benchmarks deduplication case.
func BenchmarkCaptureStackDeduplication(b *testing.B) {
	Reset()

	// Pre-capture stack to populate depot.
	_ = CaptureStack()

	b.ReportAllocs()
	b.ResetTimer()

	// All subsequent captures should hit deduplication path.
	for i := 0; i < b.N; i++ {
		_ = CaptureStack()
	}
}

// BenchmarkGetStack benchmarks stack retrieval performance.
func BenchmarkGetStack(b *testing.B) {
	Reset()

	// Capture a stack to retrieve.
	hash := CaptureStack()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = GetStack(hash)
	}
}

// BenchmarkFormatStack benchmarks stack formatting performance.
func BenchmarkFormatStack(b *testing.B) {
	Reset()

	// Capture a stack to format.
	hash := CaptureStack()
	stack := GetStack(hash)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = stack.FormatStack()
	}
}
