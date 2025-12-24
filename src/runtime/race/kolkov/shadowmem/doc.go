// Package shadowmem implements shadow memory cells for FastTrack race detection.
//
// Shadow memory is the foundation of dynamic race detection. It tracks the access
// history for every instrumented memory location, enabling the detector to identify
// conflicting accesses that constitute data races.
//
// # Overview
//
// For every variable in the program that is accessed with race instrumentation,
// the shadow memory maintains a VarState cell that records:
//   - W: The last write epoch (thread ID + logical clock)
//   - R: The last read epoch (or read set in Phase 3)
//
// The FastTrack algorithm uses these epochs to determine if two accesses are
// ordered by the happens-before relation. If not, and at least one is a write,
// a race is detected.
//
// # Components
//
// VarState: A single shadow memory cell (8 bytes) tracking W and R epochs.
//
// ShadowMemory: The global map from memory addresses to VarState cells.
//
// # Usage
//
// Create a shadow memory instance:
//
//	sm := shadowmem.NewShadowMemory()
//
// On a write access at address addr:
//
//	vs := sm.GetOrCreate(addr)
//	if !currentEpoch.HappensBefore(vs.W) || !currentEpoch.HappensBefore(vs.R) {
//	    // Race detected!
//	}
//	vs.W = currentEpoch
//
// On a read access at address addr:
//
//	vs := sm.GetOrCreate(addr)
//	if !currentEpoch.HappensBefore(vs.W) {
//	    // Write-read race detected!
//	}
//	vs.R = currentEpoch
//
// # Performance
//
// Shadow memory access is on the critical path - every instrumented memory
// operation calls GetOrCreate(). Performance targets:
//
//   - Get (hit): <10ns/op, 0 allocs/op
//   - GetOrCreate (hit): <10ns/op, 0 allocs/op
//   - GetOrCreate (miss): <50ns/op, 1 alloc/op (VarState)
//
// The MVP uses sync.Map for thread-safe storage. Phase 5 will optimize
// with a custom lock-free hashmap for better performance.
//
// # Thread Safety
//
// All operations are thread-safe. Multiple goroutines can concurrently call
// GetOrCreate, Get, and access VarState fields without external locking.
//
// Note: Reset() is NOT thread-safe and should only be called during
// initialization or testing when no other operations are in progress.
//
// # Memory Layout
//
// VarState is exactly 8 bytes (2 × uint32 epochs), enabling efficient
// packing and cache-friendly access patterns.
//
// For MVP, we track at exact address granularity. In Phase 5, we may
// compress to 8-byte alignment for better memory efficiency:
//
//	addr = alignDown(rawAddr, 8)
//
// # Future Optimizations (Phase 5)
//
//   - Custom lock-free hashmap instead of sync.Map
//   - Address compression (8-byte alignment)
//   - Epoch compression (shared clock tables)
//   - Shadow cell pooling (reduce allocation overhead)
//
// See docs/PRODUCTION_ROADMAP.md for optimization details.
package shadowmem
