// Package goroutine implements per-goroutine race detection state for FastTrack.
//
// RaceContext maintains the logical clock and cached epoch for each goroutine,
// enabling efficient happens-before tracking. Each goroutine gets its own
// RaceContext which stores:
//   - TID: process-lifetime monotonic uint32 logical ID
//   - C: hybrid dense/sparse vector clock tracking observed logical IDs
//   - Epoch: Cached C[TID] for O(1) fast-path access
//
// The epoch cache is critical for performance - it allows most race checks
// to avoid vector clock operations (FastTrack's 96%+ epoch-only fast path).
//
// Logical IDs are not recycled. Clock advancement atomically publishes the
// cached epoch so lifecycle handoffs can read the owning component without
// traversing a live mutable vector clock.
package goroutine
