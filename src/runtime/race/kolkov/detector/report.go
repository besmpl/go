package detector

import (
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/stackdepot"
)

type runtimeFrameReport struct {
	PC       uintptr
	Function string
	File     string
	Line     int
}

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

	// StackTrace contains a captured call stack or the best available recorded
	// access PC.
	StackTrace []uintptr

	// CreationStackTrace contains the recorded go-statement PC for this
	// goroutine. Goroutine creation is a lifecycle event, so retaining this
	// metadata does not add work to the memory-access hot path.
	CreationStackTrace []uintptr

	// CreationRunning reports whether the goroutine was still live when this
	// report took its lifecycle snapshot.
	CreationRunning bool
}

// Keep creation records for all live detector goroutines and a bounded tail of
// finished goroutines. A conflict can be discovered after its earlier
// goroutine exits, so dropping metadata immediately at GoEnd would lose the
// standard "created at" section. Conversely, retaining every process-lifetime
// TID would grow without bound in goroutine-heavy programs.
const retiredGoroutineCreationSlots = 4096

type goroutineCreationRecord struct {
	tid uint32
	pc  uintptr
}

type goroutineCreationRegistry struct {
	mu spinlock

	// active is proportional to live goroutines rather than lifetime history.
	active map[uint32]uintptr

	// retired is a fixed-size FIFO tail. Eviction can reduce only diagnostic
	// fidelity for very old finished goroutines; it cannot affect detection.
	// Reports are rare, so a linear lookup is preferable to a second map and its
	// stale-entry bookkeeping.
	retired     [retiredGoroutineCreationSlots]goroutineCreationRecord
	retiredNext uint32
}

// RegisterGoroutineCreation records the go statement which created tid. A zero
// PC has no symbolizable information and is intentionally omitted.
func (d *Detector) RegisterGoroutineCreation(tid uint32, pc uintptr) {
	if tid == 0 || pc == 0 {
		return
	}
	registry := &d.goroutineCreations
	registry.mu.lock()
	if registry.active == nil {
		registry.active = make(map[uint32]uintptr)
	}
	registry.active[tid] = pc
	registry.mu.unlock()
}

// RetireGoroutineCreation moves a live creation record into the bounded
// finished tail so a later conflict with stale shadow history can still name
// the goroutine's creation site.
func (d *Detector) RetireGoroutineCreation(tid uint32) {
	if tid == 0 {
		return
	}
	registry := &d.goroutineCreations
	registry.mu.lock()
	pc, ok := registry.active[tid]
	if ok {
		delete(registry.active, tid)
		slot := registry.retiredNext % retiredGoroutineCreationSlots
		registry.retired[slot] = goroutineCreationRecord{tid: tid, pc: pc}
		registry.retiredNext++
	}
	registry.mu.unlock()
}

func (registry *goroutineCreationRegistry) reset() {
	registry.mu.lock()
	registry.active = nil
	registry.retired = [retiredGoroutineCreationSlots]goroutineCreationRecord{}
	registry.retiredNext = 0
	registry.mu.unlock()
}

func (d *Detector) lookupGoroutineCreation(tid uint32) (pc uintptr, running, ok bool) {
	if tid == 0 {
		return 0, false, false
	}
	registry := &d.goroutineCreations
	registry.mu.lock()
	if pc, ok = registry.active[tid]; ok {
		registry.mu.unlock()
		return pc, true, true
	}
	for i := range registry.retired {
		record := registry.retired[i]
		if record.tid == tid {
			registry.mu.unlock()
			return record.pc, false, true
		}
	}
	registry.mu.unlock()
	return 0, false, false
}

// RaceReport represents a detected data race between two accesses.
//
// A race occurs when two goroutines access the same memory location
// without synchronization, and at least one access is a write.
type RaceReport struct {
	// Current is the most recent access that triggered race detection.
	Current AccessInfo

	// Previous is the earlier conflicting access.
	Previous AccessInfo

	// DeduplicationKey uniquely identifies this race location.
	// It combines the race kind, address, lifecycle, and normalized
	// goroutine/access-site pairs. The bounded deduplication table also keeps
	// the full fields so a hash collision cannot suppress a distinct report.
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

func lifecycleIDForReport(v interface{}) uint64 {
	if state, ok := v.(interface{ GetLifecycleID() uint64 }); ok {
		return state.GetLifecycleID()
	}
	return 0
}

func mixDeduplicationLifecycle(hash, lifecycle uint64) uint64 {
	const fnvPrime = 1099511628211
	hash ^= lifecycle
	return hash * fnvPrime
}

func mixDeduplicationSite(hash uint64, pc uintptr) uint64 {
	const fnvPrime = 1099511628211
	hash ^= uint64(pc)
	return hash * fnvPrime
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
// Single-PC inputs are supported for previously recorded access sites.
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
		pc, function, file, line, more := runtimeFramesNextReport(frames)
		frame := runtimeFrameReport{PC: pc, Function: function, File: file, Line: line}

		// Skip invalid frames:
		// - PC == 0: no valid instruction pointer
		// - PC < 0x10000: suspicious low address, probably corruption
		if frame.PC == 0 || frame.PC < 0x10000 {
			if !more {
				break
			}
			continue
		}

		// Store the first frame as a fallback when every frame is internal.
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
		// If all frames were filtered but we had a single PC, show it anyway;
		// it is likely the recorded user access site.
		if len(pcs) == 1 && firstFrame != nil {
			return formatFrame(firstFrame)
		}
		return "  (previous access stack trace not available)\n"
	}

	return result
}

// isInternalStackFrame returns true if the function should be filtered from stack traces.
//
// Detector implementation details are filtered from user reports.
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

	// Filter the pure-Go detector implementation packages. Runtime callbacks
	// use this import path even though the public stack should start at the
	// compiler-instrumented access site.
	if containsStr(funcName, "runtime/race/kolkov/") ||
		containsStr(funcName, "kolkov/racedetector/internal/") {
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

func formatGoroutineCreation(access *AccessInfo) string {
	if len(access.CreationStackTrace) == 0 {
		return ""
	}
	status := "finished"
	if access.CreationRunning {
		status = "running"
	}
	return "\nGoroutine " + uitoaReport(uint64(access.GoroutineID)) + " (" + status + ") created at:\n" +
		formatStackTrace(access.CreationStackTrace)
}

// NewRaceReportWithStacks creates a RaceReport with the current full stack and
// the best available previous access location.
//
// This is an enhanced version of NewRaceReport that retrieves the previous
// access PC or legacy stack from VarState.
//
// Lazy stack capture:
// This function is called ONLY when a race is detected (off hot path).
// It captures the current stack at report time and uses metadata recorded for
// the previous access, keeping stack capture off the access hot path.
//
// Parameters:
//   - raceType: One of RaceTypeWriteWrite, RaceTypeReadWrite, RaceTypeWriteRead
//   - addr: Memory address where race occurred
//   - vsInterface: VarState interface{} containing previous access PC
//   - prevEpoch: Epoch of previous conflicting access
//   - currEpoch: Epoch of current access
//
// Returns a RaceReport with the current full stack and either the stored
// previous PC, a legacy previous stack, or no previous stack when neither is
// available.
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

	// Type assert to get VarState interface with PC/stack methods.
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
		if raceType == RaceTypeWriteWrite || raceType == RaceTypeWriteRead {
			// Previous access was a write - get write PC.
			prevPC = vs.GetWritePC()
			prevStackHash = vs.GetWriteStack() // Legacy fallback
		} else {
			// Previous access was a read - get read PC.
			prevPC = vs.GetReadPC()
			prevStackHash = vs.GetReadStack() // Legacy fallback
		}

		// The hot path stores only the caller PC rather than capturing a stack.
		// When a race is detected, we use the stored PC to show at least the function name.
		if prevPC != 0 {
			// Use the stored PC to create a minimal stack trace.
			// This shows at least the function name and file:line of the previous access.
			// Trade-off: Performance (50x faster hot path) vs. Full stack depth.
			// For full stack, we'd need to store all frames at access time (~500ns).
			previousStack = []uintptr{prevPC}
		} else if prevStackHash != 0 {
			// Legacy fallback: use the stack hash if no PC is available.
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
			StackTrace:  previousStack,
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

	// Generate the key used to deduplicate equivalent reports.
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
// Previous access stack trace is unavailable because this constructor receives
// no previous-access metadata.
//
// Deprecated: Use NewRaceReportWithStacks instead.
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
			// StackTrace is unavailable without previous-access metadata.
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

	// Generate the key used to deduplicate equivalent reports.
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
//	  (previous access stack trace not available)
//	  [epoch: 5@100]
//	==================
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

	// Include the exact detector epoch as a PureGo diagnostic extension.
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

	printstring(formatGoroutineCreation(&r.Current))
	if r.Previous.GoroutineID != r.Current.GoroutineID {
		printstring(formatGoroutineCreation(&r.Previous))
	}

	printstring("==================\n")
}

// String returns a formatted string representation of the race report.
//
// Useful for testing and debugging.
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

	result += formatGoroutineCreation(&r.Current)
	if r.Previous.GoroutineID != r.Current.GoroutineID {
		result += formatGoroutineCreation(&r.Previous)
	}

	result += "==================\n"
	return result
}

// reportRaceV2 reports a race using the structured RaceReport format.
//
// Deduplication strategy:
//   - Generate a key from the race kind, address, lifecycle, and normalized
//     goroutine/access-site pairs
//   - Check the key and its full collision-resistant fields in a bounded table
//   - If yes: silently skip reporting (return early)
//   - If no: report the race and mark this key as reported
//
// This prevents spam from the same race occurring multiple times during execution.
//
// Stack traces:
//   - Captures the current stack, or uses an explicit current access PC
//   - Retrieves the best available previous PC or legacy stack from VarState
//
// Parameters:
//   - raceType: Type of race (RaceTypeWriteWrite, RaceTypeReadWrite, RaceTypeWriteRead)
//   - addr: Memory address where race occurred
//   - vs: VarState containing previous access PC or legacy stack hash
//   - prevEpoch: Epoch of previous conflicting access
//   - currEpoch: Epoch of current access
//
// Thread Safety: Uses detector mutex to prevent interleaved output.
func (d *Detector) reportRaceV2(raceType string, addr uintptr, vs interface{}, prevEpoch, currEpoch epoch.Epoch) {
	d.reportRaceV2PC(raceType, addr, vs, prevEpoch, currEpoch, 0)
}

// reportRaceV2PC reports a race and uses currentPC as the current access site
// when supplied by a compiler hook. Unwinding from detector code running on
// systemstack cannot reliably recover that user frame.
func (d *Detector) reportRaceV2PC(raceType string, addr uintptr, vs interface{}, prevEpoch, currEpoch epoch.Epoch, currentPC uintptr) {
	// Create structured race report (this generates the deduplication key).
	report := NewRaceReportWithStacks(raceType, addr, vs, prevEpoch, currEpoch)
	if currentPC != 0 {
		report.Current.StackTrace = []uintptr{currentPC}
	}
	if creationPC, running, ok := d.lookupGoroutineCreation(report.Current.GoroutineID); ok {
		report.Current.CreationStackTrace = []uintptr{creationPC}
		report.Current.CreationRunning = running
	}
	if creationPC, running, ok := d.lookupGoroutineCreation(report.Previous.GoroutineID); ok {
		report.Previous.CreationStackTrace = []uintptr{creationPC}
		report.Previous.CreationRunning = running
	}

	// Use loadOrStore for atomic check-and-set operation.
	// Returns true if the key already exists (race already reported).
	firstTID := report.Current.GoroutineID
	secondTID := report.Previous.GoroutineID
	firstPC := currentPC
	var secondPC uintptr
	if len(report.Previous.StackTrace) != 0 {
		secondPC = report.Previous.StackTrace[0]
	}
	if firstTID > secondTID {
		firstTID, secondTID = secondTID, firstTID
		firstPC, secondPC = secondPC, firstPC
	}
	lifecycle := lifecycleIDForReport(vs)
	report.DeduplicationKey = mixDeduplicationLifecycle(report.DeduplicationKey, lifecycle)
	report.DeduplicationKey = mixDeduplicationSite(report.DeduplicationKey, firstPC)
	report.DeduplicationKey = mixDeduplicationSite(report.DeduplicationKey, secondPC)
	alreadyReported := d.reportedRaces.loadOrStore(report.DeduplicationKey, reportedRaceKey{
		raceType:  raceType,
		addr:      addr,
		firstTID:  firstTID,
		secondTID: secondTID,
		firstPC:   firstPC,
		secondPC:  secondPC,
		lifecycle: lifecycle,
	})
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
	if d.reportObserver != nil {
		d.reportObserver(report)
	}

	// Notify the runtime so RaceErrors() returns the correct count.
	// The runtime uses this to set exit code 66 when races are found.
	kolkovIncrementErrors()

	// Print to stderr using runtime print functions.
	report.Print()
	kolkovReportDone()
}
