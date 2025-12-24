// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

// Pure-Go race detector: VectorClock implementation.
//
// Vector clocks are used in FastTrack algorithm for read-shared data (0.1% of accesses).
// Most operations (99%+) use lightweight epochs, but when concurrent reads occur,
// we promote to vector clocks to precisely track partial order across all threads.

package runtime

const (
	// kolkovMaxThreads is the maximum number of concurrent threads supported.
	// 65,536 threads = 16-bit TID space (matches epoch TID bits).
	//
	// Memory: 65,536 × 4 bytes = 256KB per VectorClock.
	// This is only allocated for read-shared variables (rare in FastTrack).
	kolkovMaxThreads = 65536
)

// kolkovVectorClock represents logical time across multiple threads.
//
// Each element clocks[tid] stores the clock value for thread tid.
// This is a fixed-size array to avoid heap allocations on hot paths.
//
// v0.3.0 SPARSE OPTIMIZATION:
// Track maxTID to avoid iterating over 65536 elements when most are zero.
// Typical programs have ~10-100 active goroutines, so this reduces iteration
// from 65536 to ~100 elements (655x speedup for Join/LessOrEqual).
type kolkovVectorClock struct {
	clocks [kolkovMaxThreads]uint32 // Clock values per thread.
	maxTID uint16                   // Highest TID with non-zero clock.
}

// kolkovNewVectorClock creates a zero-initialized vector clock.
//
// Note: For runtime usage, consider preallocating or using fixalloc.
func kolkovNewVectorClock() *kolkovVectorClock {
	return &kolkovVectorClock{}
}

// reset clears the VectorClock to zero state.
//
// Only clears up to maxTID+1 elements for efficiency (sparse optimization).
//
//go:nosplit
func (vc *kolkovVectorClock) reset() {
	// Clear only elements up to maxTID (sparse-aware).
	for i := uint32(0); i <= uint32(vc.maxTID); i++ {
		vc.clocks[i] = 0
	}
	vc.maxTID = 0
}

// clone creates a deep copy of the vector clock.
//
// v0.3.0: Only copies up to maxTID for efficiency.
func (vc *kolkovVectorClock) clone() *kolkovVectorClock {
	c := &kolkovVectorClock{maxTID: vc.maxTID}
	for i := uint32(0); i <= uint32(vc.maxTID); i++ {
		c.clocks[i] = vc.clocks[i]
	}
	return c
}

// join performs point-wise maximum: vc = vc ⊔ other.
//
// This is the synchronization operation for happens-before in FastTrack.
// Used when a thread acquires a lock: Ct := Ct ⊔ Lm.
//
// Algorithm: For each thread i, vc[i] = max(vc[i], other[i])
//
// v0.3.0 SPARSE-AWARE: Only iterates up to max(vc.maxTID, other.maxTID).
//
//go:nosplit
func (vc *kolkovVectorClock) join(other *kolkovVectorClock) {
	// Determine the range to iterate (sparse optimization).
	limit := uint32(vc.maxTID)
	if uint32(other.maxTID) > limit {
		limit = uint32(other.maxTID)
	}

	// Point-wise maximum only up to limit.
	for i := uint32(0); i <= limit; i++ {
		if other.clocks[i] > vc.clocks[i] {
			vc.clocks[i] = other.clocks[i]
		}
	}

	// Update maxTID if other had higher TIDs.
	if other.maxTID > vc.maxTID {
		vc.maxTID = other.maxTID
	}
}

// lessOrEqual checks partial order: vc ⊑ other.
//
// Returns true if vc[i] <= other[i] for all threads i.
// This implements the happens-before relation check.
//
// v0.3.0 SPARSE-AWARE: Only checks up to vc.maxTID.
//
//go:nosplit
func (vc *kolkovVectorClock) lessOrEqual(other *kolkovVectorClock) bool {
	// Only need to check up to vc.maxTID (elements beyond are 0).
	for i := uint32(0); i <= uint32(vc.maxTID); i++ {
		if vc.clocks[i] > other.clocks[i] {
			return false
		}
	}
	return true
}

// happensBefore checks if this VectorClock happened-before another.
//
// This is an alias for lessOrEqual for API clarity.
//
//go:nosplit
func (vc *kolkovVectorClock) happensBefore(other *kolkovVectorClock) bool {
	return vc.lessOrEqual(other)
}

// increment advances the clock for thread tid.
//
// This is called on every memory access by thread tid.
//
//go:nosplit
func (vc *kolkovVectorClock) increment(tid uint16) {
	vc.clocks[tid]++
	if tid > vc.maxTID {
		vc.maxTID = tid
	}
}

// get returns the clock value for thread tid.
//
//go:nosplit
func (vc *kolkovVectorClock) get(tid uint16) uint32 {
	return vc.clocks[tid]
}

// set sets the clock value for thread tid.
//
//go:nosplit
func (vc *kolkovVectorClock) set(tid uint16, clock uint32) {
	vc.clocks[tid] = clock
	if clock > 0 && tid > vc.maxTID {
		vc.maxTID = tid
	}
}

// getMaxTID returns the highest TID with non-zero clock.
//
//go:nosplit
func (vc *kolkovVectorClock) getMaxTID() uint16 {
	return vc.maxTID
}

// copyFrom copies all values from another VectorClock into this one.
//
// v0.3.0: Uses sparse-aware copying for efficiency.
//
//go:nosplit
func (vc *kolkovVectorClock) copyFrom(other *kolkovVectorClock) {
	for i := uint32(0); i <= uint32(other.maxTID); i++ {
		vc.clocks[i] = other.clocks[i]
	}
	// Clear elements beyond other.maxTID if vc had higher maxTID.
	if vc.maxTID > other.maxTID {
		for i := uint32(other.maxTID) + 1; i <= uint32(vc.maxTID); i++ {
			vc.clocks[i] = 0
		}
	}
	vc.maxTID = other.maxTID
}

// epochHappensBefore checks if an epoch happened before this vector clock.
//
// This is the CRITICAL O(1) operation that makes FastTrack fast!
// Returns true if epoch's clock <= vc[epoch's TID].
//
//go:nosplit
func (vc *kolkovVectorClock) epochHappensBefore(e kolkovEpoch) bool {
	tid, clock := e.decode()
	return clock <= uint64(vc.clocks[tid])
}

// setFromEpoch initializes the vector clock from an epoch.
//
// Sets vc[tid] = clock, all other elements remain 0.
// Used when promoting from epoch to vector clock.
//
//go:nosplit
func (vc *kolkovVectorClock) setFromEpoch(e kolkovEpoch) {
	vc.reset()
	tid, clock := e.decode()
	// Truncate clock to uint32 for storage.
	vc.clocks[tid] = uint32(clock)
	vc.maxTID = tid
}
