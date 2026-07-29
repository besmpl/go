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
//   - R: the last read epoch or an adaptive multi-reader set
//
// The FastTrack algorithm uses these epochs to determine if two accesses are
// ordered by the happens-before relation. If not, and at least one is a write,
// a race is detected.
//
// # Components
//
// VarState: A 128-byte shadow state on 64-bit systems. It includes ordinary
// FastTrack history, adaptive read state, lifecycle identity, and a lazy atomic
// overlay pointer.
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
// # Thread Safety
//
// Page-table and standalone CAS backends publish eight-lane word slots. The
// page table additionally protects coarse block defaults while words remain
// uniform and unmaterialized. Block, slot, and VarState locks serialize
// copy-on-write transitions while atomic fields support read-only fast paths.
//
// Note: Reset() is NOT thread-safe and should only be called during
// initialization or testing when no other operations are in progress.
//
// # Memory Layout
//
// Every compiler-provided address remains exact. Range operations share state
// only while all unmaterialized words have identical history, and clone a
// complete word before scalar, partial, atomic, or partial-clear divergence.
// ClearRange forgets only the selected lanes; their next states receive new
// lifecycle identities.
package shadowmem
