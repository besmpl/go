// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

// Pure-Go race detector: Goroutine context implementation.
//
// Each goroutine has its own RaceContext tracking logical time and happens-before
// relationships. The context maintains both a full vector clock (C) and a cached
// epoch for the current thread.

package runtime

import (
	"internal/runtime/atomic"
)

// kolkovRaceContext represents the race detection state for a single goroutine.
//
// Each goroutine has its own context tracking logical time and happens-before
// relationships. The context maintains both a full vector clock (c) and a cached
// epoch for the current thread.
//
// The epoch cache enables FastTrack's critical optimization: most operations
// (96%+) only need the epoch value, avoiding expensive vector clock operations.
//
// Layout:
//   - tid: Thread/Goroutine ID (0-65535, uint16)
//   - c: Full vector clock [65536]uint32 tracking all threads
//   - epoch: Cached value of c[tid] as compact 64-bit epoch
//
// Invariant: epoch must ALWAYS equal kolkovNewEpoch(tid, c[tid]).
type kolkovRaceContext struct {
	// tid is the thread/goroutine identifier (0-65535).
	tid uint16

	// c is the full vector clock tracking logical time for all threads.
	c *kolkovVectorClock

	// epoch is the cached epoch for this goroutine.
	// CRITICAL: This field is on the hot path for every memory access!
	epoch kolkovEpoch
}

// TID pool management for goroutine IDs.
var (
	// kolkovNextTID is the atomic counter for allocating thread IDs.
	kolkovNextTID uint32 = 1 // Start at 1, 0 is reserved.

	// kolkovFreeTIDs is a simple stack for TID reuse.
	// Protected by kolkovTIDPoolLock.
	kolkovFreeTIDs     [65536]uint16
	kolkovFreeTIDCount uint32
	kolkovTIDPoolLock  mutex
)

// kolkovAllocTID allocates a new thread ID.
//
// Returns a 16-bit TID in range [1, 65535].
// If TIDs are exhausted, wraps around (with warning).
//
//go:nosplit
func kolkovAllocTID() uint16 {
	// First, try to reuse a freed TID.
	lock(&kolkovTIDPoolLock)
	if kolkovFreeTIDCount > 0 {
		kolkovFreeTIDCount--
		tid := kolkovFreeTIDs[kolkovFreeTIDCount]
		unlock(&kolkovTIDPoolLock)
		return tid
	}
	unlock(&kolkovTIDPoolLock)

	// Allocate new TID atomically.
	tid := atomic.Xadd(&kolkovNextTID, 1)

	// Check for overflow (TID space exhausted).
	if tid > 65535 {
		// Wrap around - this is a serious condition in production.
		atomic.Store(&kolkovNextTID, 1)
		return 1
	}

	return uint16(tid)
}

// kolkovReleaseTID returns a TID to the pool for reuse.
//
//go:nosplit
func kolkovReleaseTID(tid uint16) {
	if tid == 0 {
		return
	}
	lock(&kolkovTIDPoolLock)
	if kolkovFreeTIDCount < 65536 {
		kolkovFreeTIDs[kolkovFreeTIDCount] = tid
		kolkovFreeTIDCount++
	}
	unlock(&kolkovTIDPoolLock)
}

// kolkovAllocContext creates and initializes a new RaceContext for the given thread ID.
//
// The context is initialized with:
//   - tid set to the provided tid
//   - c initialized with c[tid]=1 (this thread at time 1, others at 0)
//   - epoch set to kolkovNewEpoch(tid, 1) (TID@1)
//
// IMPORTANT: Clock starts at 1, not 0. This is critical for race detection:
// - Clock 0 means "never happened" (default in VectorClock)
// - Two accesses at clock 0 would appear to "happen-before" each other
// - Starting at 1 ensures unsynchronized accesses are detected as races
func kolkovAllocContext(tid uint16) *kolkovRaceContext {
	ctx := &kolkovRaceContext{
		tid: tid,
		c:   kolkovNewVectorClock(),
	}
	// Initialize epoch cache to TID@1 (clock 1 for new goroutine).
	ctx.c.set(tid, 1)
	ctx.epoch = kolkovNewEpoch(tid, 1)
	return ctx
}

// kolkovAllocContextWithParent creates a RaceContext that inherits parent's clock.
//
// This is the key function for happens-before at goroutine creation (fork):
//  1. child.c := parent.c (Copy parent's clock - inherit HB relations)
//  2. child.c[child.tid] = 1 (Initialize child's own component)
//  3. child.epoch = kolkovNewEpoch(tid, 1)
//
// After this, any operation in child "sees" all operations that happened
// in parent before the fork (go func() statement).
func kolkovAllocContextWithParent(tid uint16, parentClock *kolkovVectorClock) *kolkovRaceContext {
	ctx := &kolkovRaceContext{
		tid: tid,
		c:   kolkovNewVectorClock(),
	}

	// Step 1: Inherit parent's clock (HB edge: parent fork -> child start).
	if parentClock != nil {
		ctx.c.copyFrom(parentClock)
	}

	// Step 2: Initialize child's own clock component.
	ctx.c.set(tid, 1)

	// Step 3: Initialize cached epoch.
	ctx.epoch = kolkovNewEpoch(tid, 1)

	return ctx
}

// getTID returns the thread ID for this context.
//
//go:nosplit
func (rc *kolkovRaceContext) getTID() uint16 {
	return rc.tid
}

// getEpoch returns the cached epoch for this goroutine.
//
// This is the CRITICAL HOT PATH operation - called on every memory access.
//
//go:nosplit
func (rc *kolkovRaceContext) getEpoch() kolkovEpoch {
	return rc.epoch
}

// incrementClock advances the logical clock for this goroutine.
//
// This is called on every memory access by this goroutine to represent
// forward progress in logical time.
//
//go:nosplit
func (rc *kolkovRaceContext) incrementClock() {
	// Step 1: Increment the vector clock for this thread.
	rc.c.increment(rc.tid)

	// Step 2: Update the cached epoch to match c[tid].
	rc.epoch = kolkovNewEpoch(rc.tid, uint64(rc.c.get(rc.tid)))
}

// join merges another vector clock into this context's clock.
//
// Used for synchronization: Ct := Ct ⊔ Lm
//
//go:nosplit
func (rc *kolkovRaceContext) join(other *kolkovVectorClock) {
	rc.c.join(other)
	// Update cached epoch after join.
	rc.epoch = kolkovNewEpoch(rc.tid, uint64(rc.c.get(rc.tid)))
}
