package detector

import (
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/stackdepot"
	"unsafe"
)

// Ensure unsafe is imported for go:linkname.
var _ = unsafe.Sizeof(0)

// Runtime functions via linkname (for report formatting).

//go:linkname runtimeCallersReport runtime.Callers
func runtimeCallersReport(skip int, pc []uintptr) int

//go:linkname runtimeCallersFramesReport runtime.CallersFrames
func runtimeCallersFramesReport(callers []uintptr) *runtimeFramesReport

type runtimeFramesReport struct{}

type runtimeFrameReport struct {
	PC        uintptr
	Func      uintptr // *runtime.Func — opaque, for struct layout alignment
	Function  string
	File      string
	Line      int
	startLine int
	Entry     uintptr
	funcInfo  [2]uintptr // runtime.funcInfo — two pointers (*_func, *moduledata)
}

//go:linkname runtimeFramesNextReport runtime.framesNext
func runtimeFramesNextReport(f *runtimeFramesReport) (frame runtimeFrameReport, more bool)

// kolkovIncrementErrors increments the runtime's race error counter.
// The runtime uses this counter in RaceErrors() to determine exit code 66.
//
//go:linkname kolkovIncrementErrors runtime.kolkovIncrementErrors
func kolkovIncrementErrors()

// Note: printstring and printuint are declared in detector.go via linkname.

// Simple string utilities (avoiding strings package).

func hasPrefixStr(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func containsStr(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// itoaReport converts int to string.
func itoaReport(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
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

// uitoaReport converts uint to string.
func uitoaReport(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// hexReport converts uintptr to hex string.
func hexReport(n uintptr) string {
	if n == 0 {
		return "0x0"
	}
	const digits = "0123456789abcdef"
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n&0xf]
		n >>= 4
	}
	i--
	buf[i] = 'x'
	i--
	buf[i] = '0'
	return string(buf[i:])
}

// hexAddr16 formats uintptr as 16-digit hex address with 0x prefix.
func hexAddr16(n uintptr) string {
	const digits = "0123456789abcdef"
	var buf [18]byte // "0x" + 16 hex digits
	buf[0] = '0'
	buf[1] = 'x'
	for i := 17; i >= 2; i-- {
		buf[i] = digits[n&0xf]
		n >>= 4
	}
	return string(buf[:])
}

// AccessType represents the type of memory access (Read or Write).
type AccessType int

const (
	// AccessRead indicates a read memory access.
	AccessRead AccessType = iota
	// AccessWrite indicates a write memory access.
	AccessWrite
)

// String returns the string representation of an AccessType.
func (a AccessType) String() string {
	switch a {
	case AccessRead:
		return "Read"
	case AccessWrite:
		return "Write"
	default:
		return "Unknown"
	}
}

// Race type constants for deduplication and reporting.
const (
	// RaceTypeWriteWrite indicates a write-write data race.
	RaceTypeWriteWrite = "write-write"
	// RaceTypeReadWrite indicates a read-write data race.
	RaceTypeReadWrite = "read-write"
	// RaceTypeWriteRead indicates a write-read data race.
	RaceTypeWriteRead = "write-read"
)

// Stack trace configuration constants.
const (
	// maxStackDepth is the maximum number of stack frames to capture.
	maxStackDepth = 32
)

// AccessInfo represents information about a single memory access.
//
// This structure captures all details needed to report a memory access
// that participated in a data race.
//
// Phase 5 Task 5.1: Basic information (type, address, goroutine ID)
// Phase 5 Task 5.2: Added stack trace capture.
type AccessInfo struct {
	// Type indicates whether this was a Read or Write access.
	Type AccessType

	// Addr is the memory address that was accessed.
	Addr uintptr

	// GoroutineID is the ID of the goroutine that performed the access.
	// Extracted from Epoch.TID() field.
	GoroutineID uint32

	// Epoch is the logical timestamp when the access occurred.
	// Contains both clock (timestamp) and TID (goroutine ID).
	Epoch epoch.Epoch

	// StackTrace contains program counters (PCs) for the call stack.
	// Captured at the time of the access using runtime.Callers().
	// Phase 5 Task 5.2: Added for stack trace support.
	StackTrace []uintptr
}

// RaceReport represents a detected data race between two accesses.
//
// A race occurs when two goroutines access the same memory location
// without synchronization, and at least one access is a write.
//
// Phase 5 Task 5.1: Basic race information
// Phase 5 Task 5.2: Added stack traces
// Phase 5 Task 5.3: Added deduplication key.
type RaceReport struct {
	// Current is the most recent access that triggered race detection.
	Current AccessInfo

	// Previous is the earlier conflicting access.
	Previous AccessInfo

	// DeduplicationKey uniquely identifies this race location.
	// Computed as a hash of "{type}:{addr}:{gid1}:{gid2}" where gid1 < gid2.
	// This is used to prevent duplicate reports for the same race.
	// Added in Phase 5 Task 5.3.
	DeduplicationKey uint64
}

// generateDeduplicationKey generates a unique hash key for a race location.
//
// The hash is computed from: "{type}:{addr}:{gid1}:{gid2}" where:
//   - type: Race type string (RaceTypeWriteWrite, RaceTypeReadWrite, RaceTypeWriteRead)
//   - addr: Memory address
//   - gid1, gid2: Goroutine IDs sorted numerically (smaller first)
//
// This ensures that a race between goroutines A and B at address X always
// generates the same key regardless of which goroutine detected it first.
//
// Parameters:
//   - raceType: Type of race (RaceTypeWriteWrite, RaceTypeReadWrite, RaceTypeWriteRead)
//   - addr: Memory address where race occurred
//   - gid1, gid2: Goroutine IDs involved in the race
//
// Returns a uint64 hash suitable for use in reportedRacesMap.
//
// Phase 5 Task 5.3: Deduplication key generation.
func generateDeduplicationKey(raceType string, addr uintptr, gid1, gid2 uint32) uint64 {
	// Sort goroutine IDs to ensure consistent key ordering.
	// This makes race (G1 vs G2) and race (G2 vs G1) generate the same key.
	minGID := min(gid1, gid2)
	maxGID := max(gid1, gid2)

	// Simple FNV-1a like hash (no hash/fnv import allowed).
	const (
		fnvOffset = 14695981039346656037
		fnvPrime  = 1099511628211
	)

	hash := uint64(fnvOffset)

	// Hash the race type string.
	for i := 0; i < len(raceType); i++ {
		hash ^= uint64(raceType[i])
		hash *= fnvPrime
	}

	// Hash the address (8 bytes).
	hash ^= uint64(addr)
	hash *= fnvPrime

	// Hash minGID (4 bytes).
	hash ^= uint64(minGID)
	hash *= fnvPrime

	// Hash maxGID (4 bytes).
	hash ^= uint64(maxGID)
	hash *= fnvPrime

	return hash
}

// captureStackTrace captures the current call stack.
//
// This function uses runtime.Callers() to capture program counters (PCs)
// for the current call stack, starting from the caller's caller.
//
// Parameters:
//   - skip: Number of frames to skip (2 = skip captureStackTrace and its caller)
//
// Returns a slice of program counters that can be converted to stack frames
// using runtime.CallersFrames(). Maximum depth is limited to maxStackDepth (32).
//
// Phase 5 Task 5.2: Stack trace capture implementation.
func captureStackTrace(skip int) []uintptr {
	pcs := make([]uintptr, maxStackDepth)
	n := runtimeCallersReport(skip, pcs)
	return pcs[:n]
}

// formatStackTrace formats a stack trace for display in race reports.
//
// This function converts program counters (PCs) into a formatted string
// matching Go's official race detector output:
//
//	main.reader()
//	    /path/to/file.go:15 +0x3b
//	main.worker()
//	    /path/to/file.go:25 +0x5c
//
// Parameters:
//   - pcs: Program counters from runtime.Callers()
//
// Returns a formatted string ready for inclusion in race reports.
//
// Phase 5 Task 5.2: Stack trace formatting implementation.
// v0.7.1: Improved handling of single-PC inputs (from lazy stack capture).
func formatStackTrace(pcs []uintptr) (result string) {
	// Use defer/recover to catch any panics from corrupted frame data.
	// This is a safety net - corrupted PCs can cause crashes when formatting.
	defer func() {
		if r := recover(); r != nil {
			// Return a fallback message on panic
			result = "  (stack trace unavailable due to corrupted data)\n"
		}
	}()

	return formatStackTraceInner(pcs)
}

// formatStackTraceInner does the actual stack trace formatting.
// Separated from formatStackTrace so defer/recover can catch panics.
func formatStackTraceInner(pcs []uintptr) string {
	if len(pcs) == 0 {
		return "  (no stack trace available)\n"
	}

	// Filter out invalid PCs before calling CallersFrames.
	// Invalid PCs cause corrupted frame data from the runtime.
	validPCs := make([]uintptr, 0, len(pcs))
	for _, pc := range pcs {
		// Valid code addresses are typically > 0x10000 on most platforms.
		// Addresses like 0, 0x3e, etc. are invalid and cause crashes.
		if pc > 0x10000 {
			validPCs = append(validPCs, pc)
		}
	}

	if len(validPCs) == 0 {
		return "  (no valid stack trace available)\n"
	}

	frames := runtimeCallersFramesReport(validPCs)
	var result string
	var firstFrame *runtimeFrameReport // Store first frame in case all get filtered

	for {
		frame, more := runtimeFramesNextReport(frames)

		// Skip invalid frames:
		// - PC == 0: no valid instruction pointer
		// - PC < 0x10000: suspicious low address, probably corruption
		if frame.PC == 0 || frame.PC < 0x10000 {
			if !more {
				break
			}
			continue
		}

		// Store first frame as fallback (v0.7.1: show something even if internal)
		if firstFrame == nil {
			frameCopy := frame
			firstFrame = &frameCopy
		}

		// Skip internal frames (this may panic on corrupted string - caught by recover)
		if isInternalStackFrame(frame.Function) {
			if !more {
				break
			}
			continue
		}

		result += formatFrame(&frame)

		if !more {
			break
		}
	}

	if result == "" {
		// v0.7.1: If all frames were filtered but we had a single PC,
		// show that frame anyway (better than nothing for debugging)
		if len(pcs) == 1 && firstFrame != nil {
			return formatFrame(firstFrame)
		}
		return "  (previous access stack trace not available)\n"
	}

	return result
}

// isInternalStackFrame returns true if the function should be filtered from stack traces.
//
// v0.7.2: Expanded filter to cover ALL racedetector internal functions.
// This fixes Issue #17 where internal frames (reportRaceV2, raceread, RaceRead, etc.)
// were appearing in stack traces instead of user code.
func isInternalStackFrame(funcName string) bool {
	// Safety check: empty or nil-like function names are internal
	if len(funcName) == 0 {
		return true
	}

	// Filter Go runtime internals
	if hasPrefixStr(funcName, "runtime.") {
		return true
	}

	// Don't filter test functions (they contain "Test" or "_test")
	// This ensures test stack traces show the test function name
	if containsStr(funcName, ".Test") || containsStr(funcName, "_test.") {
		return false
	}

	// Filter ALL racedetector internal packages
	// This catches: internal/race/api, internal/race/detector, etc.
	if containsStr(funcName, "kolkov/racedetector/internal/") {
		return true
	}

	// Filter public race package (race.RaceRead, race.RaceWrite, etc.)
	if containsStr(funcName, "kolkov/racedetector/race.") {
		return true
	}

	return false
}

// formatFrame formats a single stack frame as a string.
func formatFrame(frame *runtimeFrameReport) string {
	// Format: "  function()\n      file:line +0xPC\n"
	return "  " + frame.Function + "()\n      " + frame.File + ":" + itoaReport(frame.Line) + " +0x" + hexReportShort(frame.PC&0xfff) + "\n"
}

// hexReportShort converts uintptr to hex string without "0x" prefix.
func hexReportShort(n uintptr) string {
	if n == 0 {
		return "0"
	}
	const digits = "0123456789abcdef"
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n&0xf]
		n >>= 4
	}
	return string(buf[i:])
}

// NewRaceReportWithStacks creates a RaceReport with complete stack traces.
//
// This is an enhanced version of NewRaceReport that retrieves previous access
// stack traces from VarState, enabling complete race reports showing BOTH
// the current and previous access locations.
//
// Lazy Stack Capture (v0.3.0 Performance):
// This function is called ONLY when a race is detected (off hot path).
// It captures full stack traces lazily using stored PC values from VarState.
// This moves the expensive stack capture (~500ns) from hot path to race reporting.
//
// Parameters:
//   - raceType: One of RaceTypeWriteWrite, RaceTypeReadWrite, RaceTypeWriteRead
//   - addr: Memory address where race occurred
//   - vsInterface: VarState interface{} containing previous access PC
//   - prevEpoch: Epoch of previous conflicting access
//   - currEpoch: Epoch of current access
//
// Returns a fully populated RaceReport with both current and previous stacks.
//
// v0.2.0 Task 6: Complete race reports with both stacks.
// v0.3.0 Performance: Lazy stack capture using stored PC values.
//
//nolint:gocognit // Complex but necessary logic for race report generation
func NewRaceReportWithStacks(raceType string, addr uintptr, vsInterface interface{}, prevEpoch, currEpoch epoch.Epoch) *RaceReport {
	// Extract goroutine IDs from epochs.
	currTID, _ := currEpoch.Decode()
	prevTID, _ := prevEpoch.Decode()

	// Capture stack trace for current access.
	// Skip 3 frames: captureStackTrace, NewRaceReportWithStacks, reportRaceV2
	currentStack := captureStackTrace(3)

	// Retrieve previous access stack from VarState.
	var previousStack []uintptr

	// Type assert to get VarState interface with PC/stack methods (v0.3.0 Performance).
	// We use interface{} to avoid import cycle with shadowmem package.
	type pcGetter interface {
		GetWritePC() uintptr
		GetReadPC() uintptr
		GetWriteStack() uint64 // Legacy: for backward compatibility
		GetReadStack() uint64  // Legacy: for backward compatibility
	}

	//nolint:nestif // Complex but necessary for stack retrieval logic with type assertion
	if vs, ok := vsInterface.(pcGetter); ok {
		var prevPC uintptr
		var prevStackHash uint64

		// Determine which PC/stack to retrieve based on race type.
		if raceType == RaceTypeWriteWrite || raceType == RaceTypeReadWrite {
			// Previous access was a write - get write PC.
			prevPC = vs.GetWritePC()
			prevStackHash = vs.GetWriteStack() // Legacy fallback
		} else {
			// Previous access was a read - get read PC.
			prevPC = vs.GetReadPC()
			prevStackHash = vs.GetReadStack() // Legacy fallback
		}

		// Lazy stack capture (v0.3.0 Performance):
		// We store only the caller PC on the hot path (~5ns instead of ~500ns).
		// When a race is detected, we use the stored PC to show at least the function name.
		if prevPC != 0 {
			// Use the stored PC to create a minimal stack trace.
			// This shows at least the function name and file:line of the previous access.
			// Trade-off: Performance (50x faster hot path) vs. Full stack depth.
			// For full stack, we'd need to store all frames at access time (~500ns).
			previousStack = []uintptr{prevPC}
		} else if prevStackHash != 0 {
			// Legacy fallback: Use old stack hash if PC not available.
			// This supports transition period where some accesses may still use old method.
			prevStackTrace := stackdepot.GetStack(prevStackHash)
			if prevStackTrace != nil {
				// Convert StackTrace to []uintptr.
				for _, pc := range prevStackTrace.PC {
					if pc == 0 {
						break
					}
					previousStack = append(previousStack, pc)
				}
			}
		}
	}

	report := &RaceReport{
		Current: AccessInfo{
			Addr:        addr,
			GoroutineID: uint32(currTID),
			Epoch:       currEpoch,
			StackTrace:  currentStack,
		},
		Previous: AccessInfo{
			Addr:        addr,
			GoroutineID: uint32(prevTID),
			Epoch:       prevEpoch,
			StackTrace:  previousStack, // ✅ Now has previous stack!
		},
	}

	// Determine access types based on race type string.
	switch raceType {
	case RaceTypeWriteWrite:
		report.Current.Type = AccessWrite
		report.Previous.Type = AccessWrite
	case RaceTypeReadWrite:
		report.Current.Type = AccessWrite
		report.Previous.Type = AccessRead
	case RaceTypeWriteRead:
		report.Current.Type = AccessRead
		report.Previous.Type = AccessWrite
	default:
		// Unknown race type - default to write-write for safety.
		report.Current.Type = AccessWrite
		report.Previous.Type = AccessWrite
	}

	// Generate deduplication key (Phase 5 Task 5.3).
	report.DeduplicationKey = generateDeduplicationKey(
		raceType,
		addr,
		uint32(prevTID),
		uint32(currTID),
	)

	return report
}

// NewRaceReport creates a RaceReport from epoch information.
//
// This is a convenience constructor that extracts goroutine IDs from epochs
// and determines access types based on the race type string.
//
// Parameters:
//   - raceType: One of RaceTypeWriteWrite, RaceTypeReadWrite, RaceTypeWriteRead
//   - addr: Memory address where race occurred
//   - prevEpoch: Epoch of previous conflicting access
//   - currEpoch: Epoch of current access
//
// Returns a fully populated RaceReport ready for formatting.
//
// Phase 5 Task 5.2: Captures stack trace for current access.
// Phase 5 Task 5.3: Generates deduplication key.
// Previous access stack trace is not available (would require storing
// stack traces in shadow memory, planned for future enhancement).
//
// Deprecated: Use NewRaceReportWithStacks() instead (v0.2.0 Task 6).
func NewRaceReport(raceType string, addr uintptr, prevEpoch, currEpoch epoch.Epoch) *RaceReport {
	// Extract goroutine IDs from epochs.
	currTID, _ := currEpoch.Decode()
	prevTID, _ := prevEpoch.Decode()

	// Capture stack trace for current access.
	// Skip 3 frames: captureStackTrace, NewRaceReport, reportRaceV2
	// We'll filter detector internal frames in formatStackTrace()
	currentStack := captureStackTrace(3)

	report := &RaceReport{
		Current: AccessInfo{
			Addr:        addr,
			GoroutineID: uint32(currTID),
			Epoch:       currEpoch,
			StackTrace:  currentStack,
		},
		Previous: AccessInfo{
			Addr:        addr,
			GoroutineID: uint32(prevTID),
			Epoch:       prevEpoch,
			// StackTrace not available - previous access happened earlier.
			// Future enhancement: store stack traces in shadow memory.
			StackTrace: nil,
		},
	}

	// Determine access types based on race type string.
	switch raceType {
	case RaceTypeWriteWrite:
		report.Current.Type = AccessWrite
		report.Previous.Type = AccessWrite
	case RaceTypeReadWrite:
		report.Current.Type = AccessWrite
		report.Previous.Type = AccessRead
	case RaceTypeWriteRead:
		report.Current.Type = AccessRead
		report.Previous.Type = AccessWrite
	default:
		// Unknown race type - default to write-write for safety.
		report.Current.Type = AccessWrite
		report.Previous.Type = AccessWrite
	}

	// Generate deduplication key (Phase 5 Task 5.3).
	// This uniquely identifies the race location to prevent duplicate reports.
	report.DeduplicationKey = generateDeduplicationKey(
		raceType,
		addr,
		uint32(prevTID),
		uint32(currTID),
	)

	return report
}

// Print prints the race report directly to stderr.
//
// The output format matches the official Go race detector as closely as possible:
//
//	==================
//	WARNING: DATA RACE
//	Write at 0x00c0000180a0 by goroutine 7:
//	  main.writer()
//	      /path/to/file.go:10 +0x48
//	  main.worker()
//	      /path/to/file.go:25 +0x5c
//
//	Previous write at 0x00c0000180a0 by goroutine 6:
//	  (previous access stack trace not available - see Task 5.3)
//	  [epoch: 5@100]
//	==================
//
// Phase 5 Task 5.1: Basic format with operation types and goroutine IDs
// Phase 5 Task 5.2: Added stack trace capture. for current access
//
// The report is printed directly to stderr using runtime print functions.
func (r *RaceReport) Print() {
	printstring("==================\n")
	printstring("WARNING: DATA RACE\n")

	// Current access (the one that triggered detection).
	printstring(r.Current.Type.String())
	printstring(" at ")
	printstring(hexAddr16(r.Current.Addr))
	printstring(" by goroutine ")
	printstring(uitoaReport(uint64(r.Current.GoroutineID)))
	printstring(":\n")

	// Format stack trace for current access.
	if len(r.Current.StackTrace) > 0 {
		printstring(formatStackTrace(r.Current.StackTrace))
	} else {
		printstring("  (no stack trace captured)\n")
	}

	// Show epoch for debugging (can be removed in production).
	printstring("  [epoch: ")
	printstring(r.Current.Epoch.String())
	printstring("]\n\n")

	// Previous conflicting access.
	printstring("Previous ")
	printstring(r.Previous.Type.String())
	printstring(" at ")
	printstring(hexAddr16(r.Previous.Addr))
	printstring(" by goroutine ")
	printstring(uitoaReport(uint64(r.Previous.GoroutineID)))
	printstring(":\n")

	// Format stack trace for previous access (if available).
	if len(r.Previous.StackTrace) > 0 {
		printstring(formatStackTrace(r.Previous.StackTrace))
	} else {
		// Previous access stack trace not available.
		// This happens if the VarState was just created or PC was not captured.
		printstring("  (previous access stack trace not available)\n")
	}

	printstring("  [epoch: ")
	printstring(r.Previous.Epoch.String())
	printstring("]\n")

	printstring("==================\n")
}

// String returns a formatted string representation of the race report.
//
// Useful for testing and debugging.
//
// Phase 5 Task 5.2: Now includes stack traces.
func (r *RaceReport) String() string {
	result := "==================\n"
	result += "WARNING: DATA RACE\n"

	// Current access.
	result += r.Current.Type.String() + " at " + hexAddr16(r.Current.Addr) +
		" by goroutine " + uitoaReport(uint64(r.Current.GoroutineID)) + ":\n"

	if len(r.Current.StackTrace) > 0 {
		result += formatStackTrace(r.Current.StackTrace)
	} else {
		result += "  (no stack trace captured)\n"
	}
	result += "  [epoch: " + r.Current.Epoch.String() + "]\n\n"

	// Previous access.
	result += "Previous " + r.Previous.Type.String() + " at " + hexAddr16(r.Previous.Addr) +
		" by goroutine " + uitoaReport(uint64(r.Previous.GoroutineID)) + ":\n"

	if len(r.Previous.StackTrace) > 0 {
		result += formatStackTrace(r.Previous.StackTrace)
	} else {
		result += "  (previous access stack trace not available)\n"
	}
	result += "  [epoch: " + r.Previous.Epoch.String() + "]\n"

	result += "==================\n"
	return result
}

// reportRaceV2 is the new race reporting function that uses RaceReport struct.
//
// This replaces the MVP reportRace() function with a more structured approach
// that matches Go's official race detector output format.
//
// Deduplication Strategy (Phase 5 Task 5.3):
// - Generate a unique key for each race location: "{type}:{addr}:{gid1}:{gid2}"
// - Check if this key has been reported before (using sync.Map)
// - If yes: silently skip reporting (return early)
// - If no: report the race and mark this key as reported
//
// This prevents spam from the same race occurring multiple times during execution.
//
// Stack Traces (v0.2.0 Task 6):
// - Retrieves previous access stack from VarState
// - Captures current access stack
// - Shows BOTH stacks in race report for complete debugging context
//
// Parameters:
//   - raceType: Type of race (RaceTypeWriteWrite, RaceTypeReadWrite, RaceTypeWriteRead)
//   - addr: Memory address where race occurred
//   - vs: VarState containing previous access stack hash
//   - prevEpoch: Epoch of previous conflicting access
//   - currEpoch: Epoch of current access
//
// Thread Safety: Uses detector mutex to prevent interleaved output.
//
// Phase 5 Task 5.1: ✅ Basic structured reporting
// Phase 5 Task 5.2: ✅ Stack trace capture for current access
// Phase 5 Task 5.3: ✅ Deduplication to prevent duplicate reports
// v0.2.0 Task 6: ✅ Complete race reports with both stacks.
func (d *Detector) reportRaceV2(raceType string, addr uintptr, vs interface{}, prevEpoch, currEpoch epoch.Epoch) {
	// Suppress false positives on sync primitive addresses.
	// Go's sync.Mutex/RWMutex use atomic CAS on their internal fields (e.g., m.state),
	// which triggers raceread/racewrite. Since Go 1.26's internal/sync.Mutex does NOT
	// call race.Disable() around its CAS (unlike older versions), the CAS happens
	// BEFORE race.Acquire. Our detector sees an unsynchronized write because the
	// goroutine's VectorClock hasn't been updated yet (Acquire hasn't happened).
	// The actual synchronization is tracked via raceacquire/racerelease on the same
	// address, so these "races" are false positives.
	if d.syncShadow != nil && d.syncShadow.HasEntry(addr) {
		return
	}

	// Create structured race report (this generates the deduplication key).
	report := NewRaceReportWithStacks(raceType, addr, vs, prevEpoch, currEpoch)

	// Phase 5 Task 5.3: Check if this race has already been reported.
	// Use loadOrStore for atomic check-and-set operation.
	// Returns true if the key already exists (race already reported).
	alreadyReported := d.reportedRaces.loadOrStore(report.DeduplicationKey)
	if alreadyReported {
		// This race has already been reported - skip it silently.
		// We don't increment the race counter for duplicates.
		return
	}

	// This is a new race - report it!
	// Lock to prevent interleaved output from multiple goroutines.
	d.mu.lock()
	defer d.mu.unlock()

	// Increment race counter for statistics.
	// Only count unique races (deduplication is applied).
	d.racesDetected++

	// Notify the runtime so RaceErrors() returns the correct count.
	// The runtime uses this to set exit code 66 when races are found.
	kolkovIncrementErrors()

	// Print to stderr using runtime print functions.
	report.Print()
}
