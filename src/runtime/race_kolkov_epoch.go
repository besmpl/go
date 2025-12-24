// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

// Pure-Go race detector: Epoch implementation.
//
// Epoch represents a single thread's logical time as a compact 64-bit value:
// - Top 16 bits: Thread ID (0-65,535)
// - Bottom 48 bits: Clock value (0-281 trillion)
//
// This encoding enables O(1) happens-before checks which are the foundation
// of FastTrack's performance (96%+ operations use epoch-only fast path).

package runtime

import (
	"internal/runtime/atomic"
)

// kolkovEpoch is a 64-bit logical timestamp encoding both thread ID and clock value.
// Layout: [TID:16][Clock:48]
//
// Example: 0x0005000000001234 represents TID=5, Clock=0x1234 (4660 decimal).
//
// Limits:
//   - Max TID: 65,535 (16-bit) - supports up to 65K concurrent goroutines.
//   - Max Clock: 281,474,976,710,655 (48-bit) - 281 trillion operations.
type kolkovEpoch uint64

const (
	// kolkovTIDBits is the number of bits allocated for thread ID.
	// 16 bits = 65,536 threads max.
	kolkovTIDBits = 16

	// kolkovClockBits is the number of bits allocated for clock value.
	// 48 bits = 281,474,976,710,655 operations max.
	kolkovClockBits = 48

	// kolkovClockMask is the bitmask for extracting clock value (0x0000FFFFFFFFFFFF).
	kolkovClockMask = (1 << kolkovClockBits) - 1

	// kolkovMaxTID is the maximum thread ID value (65,535).
	kolkovMaxTID = uint32((1 << kolkovTIDBits) - 1)

	// kolkovMaxClock is the maximum clock value (281,474,976,710,655).
	kolkovMaxClock = uint64((1 << kolkovClockBits) - 1)

	// kolkovMaxTIDWarning is the threshold for warning about TID approaching overflow (90% of max).
	kolkovMaxTIDWarning = uint32((1 << kolkovTIDBits) * 9 / 10)

	// kolkovMaxClockWarning is the threshold for warning about clock approaching overflow (90% of max).
	kolkovMaxClockWarning = uint64((1 << kolkovClockBits) * 9 / 10)
)

// Overflow detection flags - accessed atomically.
var (
	kolkovTIDOverflowDetected   atomic.Uint32
	kolkovClockOverflowDetected atomic.Uint32
	kolkovTIDNearOverflow       atomic.Uint32
	kolkovClockNearOverflow     atomic.Uint32
)

// kolkovNewEpoch creates an epoch from thread ID and clock value.
//
// The TID is stored in the top 16 bits, clock in the bottom 48 bits.
//
// Overflow detection:
// - If TID > MaxTID: Sets tidOverflowDetected flag and clamps to MaxTID.
// - If clock > MaxClock: Sets clockOverflowDetected flag and clamps to MaxClock.
// - At 90% thresholds: Sets warning flags for early detection.
//
// Clamping prevents wrap-around which causes false negatives.
//
//go:nosplit
func kolkovNewEpoch(tid uint16, clock uint64) kolkovEpoch {
	// Convert tid to uint32 for comparison with MaxTID constant.
	tid32 := uint32(tid)

	// Check for TID overflow.
	// Note: Since tid is uint16, it cannot exceed MaxTID (65,535).
	// This check is defensive for future changes.
	if tid32 > kolkovMaxTID {
		kolkovTIDOverflowDetected.Store(1)
		tid = uint16(kolkovMaxTID)
		tid32 = kolkovMaxTID
	}

	// Check for clock overflow.
	if clock > kolkovMaxClock {
		kolkovClockOverflowDetected.Store(1)
		clock = kolkovMaxClock
	}

	// Warn at 90% threshold (early warning).
	if tid32 > kolkovMaxTIDWarning {
		kolkovTIDNearOverflow.Store(1)
	}
	if clock > kolkovMaxClockWarning {
		kolkovClockNearOverflow.Store(1)
	}

	return kolkovEpoch(uint64(tid)<<kolkovClockBits | (clock & kolkovClockMask))
}

// decode extracts the thread ID and clock value from an epoch.
//
//go:nosplit
func (e kolkovEpoch) decode() (tid uint16, clock uint64) {
	tid = uint16(e >> kolkovClockBits)
	clock = uint64(e) & kolkovClockMask
	return
}

// tid returns just the thread ID from an epoch.
//
//go:nosplit
func (e kolkovEpoch) tid() uint16 {
	return uint16(e >> kolkovClockBits)
}

// clock returns just the clock value from an epoch.
//
//go:nosplit
func (e kolkovEpoch) clock() uint64 {
	return uint64(e) & kolkovClockMask
}

// happensBefore checks if this epoch happened before a vector clock.
//
// This is the CRITICAL O(1) operation that makes FastTrack fast!
// Called millions of times, must be zero-allocation, inline-candidate.
//
// Returns true if epoch's clock <= vc[epoch's TID].
//
//go:nosplit
func (e kolkovEpoch) happensBefore(vc *kolkovVectorClock) bool {
	tid, clock := e.decode()
	return clock <= uint64(vc.get(tid))
}

// happensBeforeClock checks if this epoch's clock <= given clock value.
// Used when we already have the clock value from vector clock.
//
//go:nosplit
func (e kolkovEpoch) happensBeforeClock(clock uint64) bool {
	return e.clock() <= clock
}

// same checks if two epochs are identical (same TID and clock).
//
// Used for fast-path same-epoch optimization (71% writes, 63% reads).
//
//go:nosplit
func (e kolkovEpoch) same(other kolkovEpoch) bool {
	return e == other
}

// isZero checks if the epoch is zero (uninitialized).
//
//go:nosplit
func (e kolkovEpoch) isZero() bool {
	return e == 0
}

// withClock returns a new epoch with the same TID but updated clock.
//
//go:nosplit
func (e kolkovEpoch) withClock(newClock uint64) kolkovEpoch {
	tid := e.tid()
	return kolkovNewEpoch(tid, newClock)
}

// increment returns a new epoch with clock+1.
//
//go:nosplit
func (e kolkovEpoch) increment() kolkovEpoch {
	tid, clock := e.decode()
	return kolkovNewEpoch(tid, clock+1)
}

// kolkovCheckOverflows returns the current state of overflow detection flags.
//
// Returns:
//   - tidOverflow: true if TID overflow was detected
//   - clockOverflow: true if clock overflow was detected
//   - tidWarning: true if TID reached 90% threshold
//   - clockWarning: true if clock reached 90% threshold
//
//go:nosplit
func kolkovCheckOverflows() (tidOverflow, clockOverflow, tidWarning, clockWarning bool) {
	return kolkovTIDOverflowDetected.Load() == 1,
		kolkovClockOverflowDetected.Load() == 1,
		kolkovTIDNearOverflow.Load() == 1,
		kolkovClockNearOverflow.Load() == 1
}

// kolkovResetOverflowFlags clears all overflow detection flags.
// Used primarily for testing.
//
//go:nosplit
func kolkovResetOverflowFlags() {
	kolkovTIDOverflowDetected.Store(0)
	kolkovClockOverflowDetected.Store(0)
	kolkovTIDNearOverflow.Store(0)
	kolkovClockNearOverflow.Store(0)
}
