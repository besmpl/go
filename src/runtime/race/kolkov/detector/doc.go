// Package detector implements the core FastTrack race detection algorithm.
//
// This package provides the OnWrite and OnRead handlers that are called
// by the compiler-instrumented code to detect data races. It implements
// the FastTrack algorithm (PLDI 2009) which provides efficient and precise
// dynamic race detection.
//
// # Architecture
//
// The detector consists of three main components:
//
//  1. OnWrite/OnRead handlers: Called on every memory access
//  2. Shadow memory: Tracks access history (VarState) for each address
//  3. Race detection logic: Implements FastTrack happens-before checks
//
// # FastTrack Algorithm Overview
//
// FastTrack uses a hybrid epoch/vector-clock approach:
//
//   - Epoch: Compact (TID, Clock) pair for single-threaded access
//   - VectorClock: Full happens-before info for multi-threaded access
//   - Adaptive: Automatically switches between representations
//
// For MVP (Phase 1), we implement the core write-write and read-write
// detection using epoch-only tracking (simplified FastTrack).
//
// # Performance Characteristics
//
// Target performance for MVP:
//   - OnWrite: <100ns per call (hot path, called millions of times)
//   - OnRead: <100ns per call (hot path, called millions of times)
//   - Zero heap allocations on hot path
//   - //go:nosplit directives on critical functions
//
// # Thread Safety
//
// All detector operations are thread-safe. The ShadowMemory uses sync.Map
// for concurrent access, and RaceContext operations are lock-free where possible.
//
// # Example Usage
//
// The detector is called automatically by compiler-instrumented code:
//
//	var x int
//	x = 42  // Compiler inserts: OnWrite(&x)
//
// For MVP, the detector is initialized with a global instance:
//
//	detector := NewDetector()
//	detector.OnWrite(0x12345678)  // Detect write to address
//
// # Race Detection Rules
//
// The detector implements the following rules from FastTrack [FT WRITE]:
//
//  1. Same-epoch fast path: If write epoch matches current epoch, skip checks
//  2. Write-write race: If !currentEpoch.HappensBefore(vs.W), report race
//  3. Read-write race: If !currentEpoch.HappensBefore(vs.R), report race
//  4. Update shadow: vs.W = currentEpoch
//  5. Advance clock: ctx.IncrementClock()
//
// # Future Enhancements
//
// Phase 2: Full vector clock support for read-shared variables
// Phase 3: Adaptive epoch/VC promotion for optimal performance
// Phase 4: Advanced synchronization primitive support
// Phase 5: Optimized shadow memory with address compression
//
// See PRODUCTION_ROADMAP.md for detailed phase planning.
package detector
