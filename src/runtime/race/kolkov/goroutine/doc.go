// Package goroutine implements per-goroutine race detection state for FastTrack.
//
// RaceContext maintains the logical clock and cached epoch for each goroutine,
// enabling efficient happens-before tracking. Each goroutine gets its own
// RaceContext which stores:
//   - TID: Thread/Goroutine ID (0-255, fixed for MVP)
//   - C: Full vector clock tracking all threads
//   - Epoch: Cached C[TID] for O(1) fast-path access
//
// The epoch cache is critical for performance - it allows most race checks
// to avoid vector clock operations (FastTrack's 96%+ epoch-only fast path).
//
// Performance requirements:
//   - GetEpoch(): <1ns (must be //go:nosplit, just field access)
//   - IncrementClock(): <200ns (VectorClock update + epoch sync)
//
// The package ensures the epoch cache always stays synchronized with C[TID]
// through atomic updates in IncrementClock().
package goroutine
