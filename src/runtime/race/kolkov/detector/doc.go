// Package detector implements the core FastTrack race detection algorithm.
//
// This package provides the OnWrite and OnRead handlers that are called
// by the compiler-instrumented code to detect data races. It implements
// the FastTrack algorithm (PLDI 2009) which provides efficient and precise
// dynamic race detection.
//
// # Architecture
//
// The detector consists of these components:
//
//  1. scalar and range handlers for ordinary memory accesses
//  2. width-aware transactions for atomic memory accesses
//  3. exact-address shadow history with adaptive read tracking
//  4. synchronization shadow state and race reporting
//
// # FastTrack Algorithm Overview
//
// FastTrack uses a hybrid epoch/vector-clock approach:
//
//   - Epoch: Compact (TID, Clock) pair for single-threaded access
//   - VectorClock: Full happens-before info for multi-threaded access
//   - Adaptive: Automatically switches between representations
//
// # Performance Characteristics
//
// Steady scalar and atomic hot paths avoid heap allocation. Per-context read
// caching, epoch fast paths, inline readers, and pooled vector clocks reduce
// work without weakening exact-address or range precision.
//
// # Thread Safety
//
// Shadow slots serialize copy-on-write state changes, VarState access locks
// linearize detector transitions, and atomic overlays retain their lock across
// the corresponding hardware operation. Each RaceContext has one logical owner.
//
// # Example Usage
//
// The detector is called automatically by compiler-instrumented code:
//
//	var x int
//	x = 42  // Compiler inserts: OnWrite(&x)
//
//	d := NewDetector()
//	ctx := goroutine.Alloc(1)
//	d.OnWrite(0x12345678, ctx, 0)
//
// # Race Detection Rules
//
// The detector implements the following rules from FastTrack [FT WRITE]:
//
//  1. Same-epoch fast path: If write epoch matches current epoch, skip checks
//  2. Write-write race: If !vs.W.HappensBefore(ctx.C), report race
//  3. Read-write race: If a represented read does not happen before ctx.C,
//     report race
//  4. Update shadow: vs.W = currentEpoch
//
// Logical clocks advance at synchronization events, not ordinary accesses.
package detector
