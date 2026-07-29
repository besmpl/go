// Package stackdepot implements stack trace storage and deduplication for race reports.
//
// Stack Depot is a global storage for stack traces that deduplicates identical stacks.
// This saves memory by storing each unique stack only once, referenced by a 64-bit hash.
//
// Design (ThreadSanitizer v2 approach):
//   - Fixed-size stack traces (8 frames, 64 bytes per stack)
//   - Hash-based deduplication (FNV-1a hash)
//   - CAS-based storage (runtime-compatible, no sync.Map)
//   - Memory overhead: 64 bytes per unique stack + 8 bytes hash per VarState
package stackdepot

import (
	"internal/runtime/atomic"
	"unsafe"
)

// NOTE: We use runtime.Callers and runtime.CallersFrames via go:linkname
// because this package is part of runtime and can access internal functions.

//go:linkname runtimeCallers runtime.Callers
func runtimeCallers(skip int, pc []uintptr) int

//go:linkname runtimeCallersFrames runtime.CallersFrames
func runtimeCallersFrames(callers []uintptr) *runtimeFrames

type runtimeFrames struct{}

//go:linkname runtimeFramesNext runtime.kolkovFramesNext
func runtimeFramesNext(f *runtimeFrames) (pc uintptr, function, file string, line int, more bool)

const (
	// MaxFrames is the maximum number of stack frames to capture.
	// ThreadSanitizer uses 8 frames as a good balance between detail and memory.
	MaxFrames = 8

	// depotSize is the number of slots in the stack depot.
	depotSize = 65536
)

// StackTrace represents a captured stack trace with fixed size.
//
// Memory layout: 8 × 8 bytes = 64 bytes per stack trace.
// Stored in global depot, deduplicated by hash.
type StackTrace struct {
	PC [MaxFrames]uintptr // Program counters (64 bytes).
}

// depotCell is a cell in the CAS-based stack depot.
type depotCell struct {
	hash  uint64
	trace *StackTrace
}

// stackDepot is the global deduplication store for stack traces.
// Uses CAS-based fixed-size array for runtime-compatible storage.
var stackDepot [depotSize]atomic.Pointer[depotCell]

// CaptureStack captures the current stack trace and returns its hash.
//
// The stack is stored in the global depot for later retrieval.
// If the same stack was captured before, returns existing hash (deduplication).
//
// Returns:
//   - uint64 hash: Unique identifier for this stack (0 if no stack available)
//
// Thread Safety: Safe for concurrent calls from multiple goroutines.
func CaptureStack() uint64 {
	// Capture current stack trace.
	// Skip 2 frames: runtimeCallers itself + CaptureStack
	var pcs [MaxFrames]uintptr
	n := runtimeCallers(2, pcs[:])

	if n == 0 {
		return 0
	}

	// Compute hash for deduplication (FNV-1a).
	hash := hashStack(pcs[:n])

	// Try to find or store in depot.
	idx := hash & (depotSize - 1)

	// Linear probe up to 8 slots.
	for i := uint64(0); i < 8; i++ {
		slot := (idx + i) & (depotSize - 1)
		cellPtr := stackDepot[slot].Load()

		// Found existing with same hash.
		if cellPtr != nil && cellPtr.hash == hash {
			return hash
		}

		// Empty slot - try to insert.
		if cellPtr == nil {
			trace := &StackTrace{PC: pcs}
			newCell := &depotCell{hash: hash, trace: trace}
			if stackDepot[slot].CompareAndSwap(nil, newCell) {
				return hash
			}
			// CAS failed, check if same hash was inserted.
			cellPtr = stackDepot[slot].Load()
			if cellPtr != nil && cellPtr.hash == hash {
				return hash
			}
		}
	}

	// Overflow - store anyway (rare).
	trace := &StackTrace{PC: pcs}
	newCell := &depotCell{hash: hash, trace: trace}
	stackDepot[idx].Store(newCell)
	return hash
}

// GetStack retrieves a stack trace by hash.
//
// Returns nil if hash not found or is 0.
//
// Thread Safety: Safe for concurrent calls.
func GetStack(hash uint64) *StackTrace {
	if hash == 0 {
		return nil
	}

	idx := hash & (depotSize - 1)

	// Linear probe up to 8 slots.
	for i := uint64(0); i < 8; i++ {
		slot := (idx + i) & (depotSize - 1)
		cellPtr := stackDepot[slot].Load()

		if cellPtr == nil {
			return nil
		}

		if cellPtr.hash == hash {
			return cellPtr.trace
		}
	}

	return nil
}

// FNV-1a constants.
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// hashStack computes FNV-1a hash of program counters.
//
// FNV-1a implementation without hash/fnv import (runtime-compatible).
//
//go:nosplit
func hashStack(pcs []uintptr) uint64 {
	hash := uint64(fnvOffset64)

	for _, pc := range pcs {
		// Hash each byte of the PC.
		pcBytes := (*[8]byte)(unsafe.Pointer(&pc))
		for j := 0; j < 8; j++ {
			hash ^= uint64(pcBytes[j])
			hash *= fnvPrime64
		}
	}

	return hash
}

// FormatStack formats a stack trace as a string for race reports.
//
// The output format matches Go's official race detector.
// This function filters out runtime internal frames to show only user code.
func (st *StackTrace) FormatStack() string {
	if st == nil {
		return "  <unknown>\n"
	}

	frames := runtimeCallersFrames(st.PC[:])

	result := ""
	for {
		pc, function, file, line, more := runtimeFramesNext(frames)
		if pc == 0 {
			break
		}

		// Skip runtime internal frames.
		if hasPrefix(function, "runtime.") {
			if !more {
				break
			}
			continue
		}

		// Format: "  function_name()\n"
		result += "  " + function + "()\n"

		// Format: "      file.go:line\n"
		result += "      " + file + ":" + itoa(line) + "\n"

		if !more {
			break
		}
	}

	if result == "" {
		return "  <runtime internal>\n"
	}

	return result
}

// hasPrefix checks if s starts with prefix (no strings import).
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// itoa converts int to string (no fmt import).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	neg := n < 0
	if neg {
		n = -n
	}

	// Max int has 19 digits.
	var buf [20]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	if neg {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}

// Reset clears the stack depot (for testing).
//
// Thread Safety: NOT safe for concurrent calls.
func Reset() {
	for i := range stackDepot {
		stackDepot[i].Store(nil)
	}
}

// Stats returns statistics about the stack depot.
//
// Returns:
//   - uniqueStacks: Number of unique stacks stored
//   - totalMemory: Approximate memory usage in bytes
func Stats() (uniqueStacks int, totalMemory int64) {
	for i := range stackDepot {
		if stackDepot[i].Load() != nil {
			uniqueStacks++
		}
	}

	// Each StackTrace is 64 bytes (8 frames × 8 bytes).
	// Plus overhead: ~24 bytes per cell (hash + pointer + padding).
	const bytesPerStack = 64 + 24
	totalMemory = int64(uniqueStacks) * bytesPerStack

	return uniqueStacks, totalMemory
}
