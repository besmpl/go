package syncshadow

import (
	"runtime/race/kolkov/vectorclock"
)

// WaitGroupState tracks happens-before relationships for a sync.WaitGroup.
//
// WaitGroup creates happens-before edges for goroutine lifecycle synchronization.
// The Go memory model guarantees:
//   - WaitGroup.Done() happens-before the corresponding WaitGroup.Wait() returns
//   - Multiple goroutines can call Done(), creating a synchronization point
//   - Wait() blocks until all goroutines have called Done()
//
// Implementation:
//   - doneClock: Accumulates vector clocks from all Done() calls
//   - counter: Tracks Add/Done balance (for optional validation)
//
// Layout:
//   - doneClock: VectorClock accumulating all Done() operations
//   - counter: int32 tracking current wait count (optional, for debugging)
//
// Operations:
//   - OnAdd(delta): Increment counter by delta
//   - OnDone(): Merge current thread's clock into doneClock, decrement counter
//   - OnWaitBefore(): Prepare for wait (optional validation)
//   - OnWaitAfter(): Merge accumulated doneClock into waiter's clock
//
// Memory:
//   - Size: ~1KB (1 VectorClock) + 4 bytes (counter)
//   - Allocated lazily on first WaitGroup operation
//
// Lifecycle:
//   - Created on first WaitGroup operation (Add/Done/Wait)
//   - Never freed (WaitGroups typically live for program lifetime or are GC'd)
//
// Example (parent-child synchronization):
//
//	var wg sync.WaitGroup
//	var data int
//
//	// Parent goroutine
//	wg.Add(1)          // OnAdd increments counter to 1
//	go func() {
//	    data = 42      // Child writes
//	    wg.Done()      // OnDone: merge child's clock into doneClock
//	}()
//
//	wg.Wait()          // OnWaitAfter: merge doneClock into parent's clock
//	_ = data           // Parent reads (happens-after child write)
type WaitGroupState struct {
	// doneClock accumulates vector clocks from all Done() calls.
	// nil means no Done() has been called yet.
	//
	// On Done(), the thread's clock is merged into doneClock.
	// On Wait(), the waiter merges doneClock into its own clock.
	doneClock *vectorclock.VectorClock

	// counter tracks the current wait count (Add minus Done).
	// Used for optional validation to detect misuse patterns.
	//
	// - Add(delta): counter += delta
	// - Done(): counter -= 1
	// - Wait(): blocks until counter == 0
	//
	// This is primarily for debugging and validation, not required for
	// correctness of happens-before tracking.
	counter int32
}

// ChannelState tracks happens-before relationships for a channel.
//
// Channels create bidirectional happens-before edges between send and receive operations.
// The Go memory model guarantees:
//   - Unbuffered channel: Send synchronizes-with Receive (bidirectional)
//   - Buffered channel: kth Receive happens-before (k+C)th Send completes
//   - Channel close: close(ch) happens-before all receives that observe closure
//
// For MVP (Task 4.2), we treat all channels as unbuffered for simplicity.
// This is conservative - it won't produce false negatives (missed races),
// but may be slightly less permissive than the full memory model.
//
// Layout:
//   - sendClock: VectorClock from the last send operation
//   - recvClock: VectorClock from the last receive operation
//   - closeClock: VectorClock from channel close (nil if not closed)
//   - isClosed: Flag indicating if channel is closed
//
// Operations:
//   - OnSendAfter: Captures sender's clock (sendClock := sender.C)
//   - OnRecvAfter: Merges sender's clock into receiver (recv.C.Join(sendClock))
//   - OnClose: Captures close clock, sets isClosed flag
//
// Memory:
//   - Size: ~3KB (3 VectorClocks x 1KB each) + 1 byte flag
//   - Allocated lazily on first channel operation
//
// Lifecycle:
//   - Created on first channel operation (send/recv/close)
//   - Never freed (channels typically live for program lifetime or are GC'd)
//
// Example (unbuffered channel):
//
//	// Goroutine 1 (sender)
//	ch <- value         // OnSendAfter captures sender's clock
//
//	// Goroutine 2 (receiver)
//	<-ch                // OnRecvAfter merges sender's clock into receiver
//	// Receiver now happens-after sender
type ChannelState struct {
	// sendClock is the vector clock from the last send operation.
	// nil means no send has occurred yet (uninitialized channel).
	//
	// On Send, the sender's clock is captured into sendClock.
	// On Receive, the receiver merges sendClock into its own clock.
	sendClock *vectorclock.VectorClock

	// recvClock is the vector clock from the last receive operation.
	// nil means no receive has occurred yet.
	//
	// For bidirectional synchronization (unbuffered channels), recvClock
	// can be merged back into sender's clock if needed.
	// MVP: Not used for now, reserved for future bidirectional sync.
	recvClock *vectorclock.VectorClock

	// closeClock is the vector clock when the channel was closed.
	// nil means channel is not closed yet.
	//
	// On Close, the closer's clock is captured into closeClock.
	// All subsequent receives will merge closeClock (happens-before closure).
	closeClock *vectorclock.VectorClock

	// isClosed indicates if the channel has been closed.
	// true means close(ch) was called.
	//
	// After close, receives are allowed (until channel is drained),
	// but sends will panic. We track this for correctness.
	isClosed bool
}

// SyncVar tracks happens-before relationships for a synchronization primitive.
//
// Each sync primitive (mutex, rwmutex, channel, etc.) has its own SyncVar
// that stores the vector clock from the last Release operation. This enables
// the FastTrack algorithm to establish happens-before edges across threads.
//
// Layout:
//   - releaseClock: VectorClock captured at last Release (Unlock)
//
// Operations:
//   - Acquire: Thread merges releaseClock into its own clock
//   - Release: Thread copies its clock into releaseClock
//   - ReleaseMerge: Thread merges its clock into releaseClock (for RWMutex)
//
// Memory:
//   - Size: ~8 bytes (pointer to VectorClock)
//   - VectorClock: 1KB (256 uint32s)
//   - Total per mutex: ~1KB
//
// Lifecycle:
//   - Created on first Lock/Unlock of a mutex
//   - Never freed (mutexes typically live for program lifetime)
//   - releaseClock allocated lazily on first Release
//
// Example:
//
//	sv := &SyncVar{}
//	// First unlock: sv.releaseClock = nil
//	sv.SetReleaseClock(threadClock)  // Allocates and copies
//	// Next lock: threadClock.Join(sv.releaseClock)
//	sv.SetReleaseClock(threadClock)  // Updates existing clock
type SyncVar struct {
	// releaseClock is the vector clock from the last Release operation.
	// nil means no Release has occurred yet (uninitialized mutex).
	//
	// On Acquire (Lock), threads merge this into their own clock to establish
	// happens-before from the previous Unlock.
	//
	// On Release (Unlock), this is updated to the current thread's clock.
	releaseClock *vectorclock.VectorClock

	// channel tracks happens-before relationships for channel operations.
	// nil means this SyncVar is not used for a channel (it's a mutex/rwmutex).
	//
	// Allocated lazily on first channel operation (send/recv/close).
	// Phase 4 Task 4.2: Channel synchronization support.
	channel *ChannelState

	// waitGroup tracks happens-before relationships for WaitGroup operations.
	// nil means this SyncVar is not used for a WaitGroup.
	//
	// Allocated lazily on first WaitGroup operation (Add/Done/Wait).
	// Phase 4 Task 4.3: WaitGroup synchronization support.
	waitGroup *WaitGroupState
}

// GetReleaseClock returns the release clock for this sync variable.
//
// Returns nil if no Release has occurred yet (uninitialized mutex).
// The caller should check for nil before using the clock.
//
// Thread Safety: NOT thread-safe on its own. The caller (SyncShadow) must
// ensure synchronization via sync.Map or other mechanisms.
//
// Example:
//
//	sv := &SyncVar{}
//	clock := sv.GetReleaseClock()  // Returns nil (no releases yet)
//	sv.SetReleaseClock(someClock)
//	clock = sv.GetReleaseClock()   // Returns someClock
func (sv *SyncVar) GetReleaseClock() *vectorclock.VectorClock {
	return sv.releaseClock
}

// SetReleaseClock sets the release clock for this sync variable.
//
// This is called during Release (Unlock) to capture the current thread's
// vector clock. The clock is copied (not referenced) to avoid aliasing issues.
//
// If releaseClock is nil (first Release), a new VectorClock is allocated.
// Otherwise, the existing clock is updated in place to avoid allocations.
//
// Parameters:
//   - clock: The vector clock to copy (must not be nil)
//
// Performance:
//   - First call: Allocates VectorClock (~1KB) and copies
//   - Subsequent calls: Updates in place (no allocations)
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
//
// Example:
//
//	sv := &SyncVar{}
//	ctx := goroutine.Alloc(0)
//	sv.SetReleaseClock(ctx.C)  // First call: allocates + copies
//	ctx.IncrementClock()
//	sv.SetReleaseClock(ctx.C)  // Second call: updates in place
func (sv *SyncVar) SetReleaseClock(clock *vectorclock.VectorClock) {
	if sv.releaseClock == nil {
		// First Release: Allocate a new VectorClock and copy.
		sv.releaseClock = clock.Clone()
	} else {
		// Subsequent Release: Update in place to avoid allocations.
		// v0.3.0: Use CopyFrom for sparse-aware copying.
		sv.releaseClock.CopyFrom(clock)
	}
}

// MergeReleaseClock merges a clock into the release clock (for RWMutex).
//
// This is used for RWMutex write unlock (racereleasemerge) where multiple
// readers may have overlapping critical sections. We merge all their clocks
// to capture the union of happens-before relationships.
//
// If releaseClock is nil (first Release), the clock is copied.
// Otherwise, the join operation (element-wise max) is performed in place.
//
// Parameters:
//   - clock: The vector clock to merge (must not be nil)
//
// Performance:
//   - First call: Allocates VectorClock (~1KB) and copies
//   - Subsequent calls: Element-wise max (no allocations)
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
//
// Example (RWMutex scenario):
//
//	sv := &SyncVar{}
//	// Reader 1 unlocks
//	sv.MergeReleaseClock(reader1Clock)  // First unlock: copy
//	// Reader 2 unlocks
//	sv.MergeReleaseClock(reader2Clock)  // Second unlock: merge
//	// Writer locks
//	writerClock.Join(sv.GetReleaseClock())  // Gets union of both readers
func (sv *SyncVar) MergeReleaseClock(clock *vectorclock.VectorClock) {
	if sv.releaseClock == nil {
		// First Release: Allocate a new VectorClock and copy.
		sv.releaseClock = clock.Clone()
	} else {
		// Subsequent Release: Merge (join) the clocks.
		// For each thread, take the maximum clock value.
		sv.releaseClock.Join(clock)
	}
}

// === Channel State Management (Phase 4 Task 4.2) ===

// GetOrCreateChannel returns the ChannelState for this SyncVar, creating it if needed.
//
// This is called on the first channel operation (send/recv/close) to lazily
// allocate the ChannelState. Subsequent operations reuse the same instance.
//
// Returns:
//   - *ChannelState: The channel state (never nil after this call)
//
// Thread Safety: NOT thread-safe on its own. The caller (SyncShadow) must
// ensure synchronization via sync.Map.
//
// Example:
//
//	sv := &SyncVar{}
//	chState := sv.GetOrCreateChannel()  // Allocates ChannelState
//	chState2 := sv.GetOrCreateChannel() // Returns same instance
//	assert(chState == chState2)
func (sv *SyncVar) GetOrCreateChannel() *ChannelState {
	if sv.channel == nil {
		sv.channel = &ChannelState{}
	}
	return sv.channel
}

// GetChannel returns the ChannelState for this SyncVar, or nil if not a channel.
//
// This is a read-only accessor for checking if a SyncVar is being used
// as a channel (vs mutex/rwmutex).
//
// Returns:
//   - *ChannelState: The channel state, or nil if this is not a channel
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) GetChannel() *ChannelState {
	return sv.channel
}

// SetChannelSendClock captures the sender's clock on channel send.
//
// This is called after a channel send completes. The sender's clock is
// copied into the channel's sendClock for the receiver to merge.
//
// Parameters:
//   - clock: The sender's vector clock (must not be nil)
//
// Performance:
//   - First call: Allocates VectorClock (~1KB) and copies
//   - Subsequent calls: Updates in place (no allocations)
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
//
// Example:
//
//	chState := sv.GetOrCreateChannel()
//	sv.SetChannelSendClock(senderCtx.C)  // Capture sender's clock
func (sv *SyncVar) SetChannelSendClock(clock *vectorclock.VectorClock) {
	chState := sv.GetOrCreateChannel()
	if chState.sendClock == nil {
		// First send: Allocate and copy.
		chState.sendClock = clock.Clone()
	} else {
		// Subsequent send: Update in place.
		// v0.3.0: Use CopyFrom for sparse-aware copying.
		chState.sendClock.CopyFrom(clock)
	}
}

// GetChannelSendClock returns the channel's send clock.
//
// Returns nil if no send has occurred yet.
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) GetChannelSendClock() *vectorclock.VectorClock {
	if sv.channel == nil {
		return nil
	}
	return sv.channel.sendClock
}

// SetChannelRecvClock captures the receiver's clock on channel receive.
//
// This is called after a channel receive completes. The receiver's clock is
// copied into the channel's recvClock for potential bidirectional sync.
//
// Parameters:
//   - clock: The receiver's vector clock (must not be nil)
//
// Performance:
//   - First call: Allocates VectorClock (~1KB) and copies
//   - Subsequent calls: Updates in place (no allocations)
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) SetChannelRecvClock(clock *vectorclock.VectorClock) {
	chState := sv.GetOrCreateChannel()
	if chState.recvClock == nil {
		// First recv: Allocate and copy.
		chState.recvClock = clock.Clone()
	} else {
		// Subsequent recv: Update in place.
		// v0.3.0: Use CopyFrom for sparse-aware copying.
		chState.recvClock.CopyFrom(clock)
	}
}

// GetChannelRecvClock returns the channel's receive clock.
//
// Returns nil if no receive has occurred yet.
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) GetChannelRecvClock() *vectorclock.VectorClock {
	if sv.channel == nil {
		return nil
	}
	return sv.channel.recvClock
}

// SetChannelCloseClock captures the closer's clock on channel close.
//
// This is called when close(ch) is executed. The closer's clock is
// copied into the channel's closeClock, and isClosed is set to true.
//
// Parameters:
//   - clock: The closer's vector clock (must not be nil)
//
// Performance: Allocates VectorClock (~1KB) and copies (one-time).
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) SetChannelCloseClock(clock *vectorclock.VectorClock) {
	chState := sv.GetOrCreateChannel()
	if chState.closeClock == nil {
		// Channel close is one-time operation - allocate and copy.
		chState.closeClock = clock.Clone()
		chState.isClosed = true
	}
	// If already closed, this is a programming error (panic in real code),
	// but we silently ignore for robustness.
}

// GetChannelCloseClock returns the channel's close clock.
//
// Returns nil if channel has not been closed yet.
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) GetChannelCloseClock() *vectorclock.VectorClock {
	if sv.channel == nil {
		return nil
	}
	return sv.channel.closeClock
}

// IsChannelClosed returns true if the channel has been closed.
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) IsChannelClosed() bool {
	if sv.channel == nil {
		return false
	}
	return sv.channel.isClosed
}

// === WaitGroup State Management (Phase 4 Task 4.3) ===

// GetOrCreateWaitGroup returns the WaitGroupState for this SyncVar, creating it if needed.
//
// This is called on the first WaitGroup operation (Add/Done/Wait) to lazily
// allocate the WaitGroupState. Subsequent operations reuse the same instance.
//
// Returns:
//   - *WaitGroupState: The WaitGroup state (never nil after this call)
//
// Thread Safety: NOT thread-safe on its own. The caller (SyncShadow) must
// ensure synchronization via sync.Map.
//
// Example:
//
//	sv := &SyncVar{}
//	wgState := sv.GetOrCreateWaitGroup()  // Allocates WaitGroupState
//	wgState2 := sv.GetOrCreateWaitGroup() // Returns same instance
//	assert(wgState == wgState2)
func (sv *SyncVar) GetOrCreateWaitGroup() *WaitGroupState {
	if sv.waitGroup == nil {
		sv.waitGroup = &WaitGroupState{}
	}
	return sv.waitGroup
}

// GetWaitGroup returns the WaitGroupState for this SyncVar, or nil if not a WaitGroup.
//
// This is a read-only accessor for checking if a SyncVar is being used
// as a WaitGroup (vs mutex/rwmutex/channel).
//
// Returns:
//   - *WaitGroupState: The WaitGroup state, or nil if this is not a WaitGroup
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) GetWaitGroup() *WaitGroupState {
	return sv.waitGroup
}

// WaitGroupAdd increments the WaitGroup counter by delta.
//
// This is called on WaitGroup.Add(delta). The counter is used for optional
// validation to detect misuse patterns (e.g., Done without Add).
//
// Parameters:
//   - delta: The delta to add to the counter (positive for Add, negative for Done)
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization. In practice, this is protected by the actual WaitGroup's
// internal mutex.
//
// Example:
//
//	wgState := sv.GetOrCreateWaitGroup()
//	sv.WaitGroupAdd(1)  // Add(1)
//	sv.WaitGroupAdd(3)  // Add(3) - counter now 4
//	sv.WaitGroupAdd(-1) // Done() - counter now 3
func (sv *SyncVar) WaitGroupAdd(delta int) {
	wgState := sv.GetOrCreateWaitGroup()
	wgState.counter += int32(delta) //nolint:gosec // G115: WaitGroup delta is typically small (<1000), overflow unlikely
}

// MergeWaitGroupDoneClock merges a thread's clock into the WaitGroup's doneClock.
//
// This is called on WaitGroup.Done() to accumulate the happens-before
// relationship. All Done() calls are merged into a single doneClock that
// will be propagated to the waiter.
//
// Parameters:
//   - clock: The thread's vector clock (must not be nil)
//
// Performance:
//   - First call: Allocates VectorClock (~1KB) and copies
//   - Subsequent calls: Element-wise max (no allocations)
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
//
// Example:
//
//	// Child goroutine 1
//	wgState := sv.GetOrCreateWaitGroup()
//	sv.MergeWaitGroupDoneClock(child1Ctx.C)  // First Done: copy
//	// Child goroutine 2
//	sv.MergeWaitGroupDoneClock(child2Ctx.C)  // Second Done: merge
//	// Parent waits
//	parentCtx.C.Join(sv.GetWaitGroupDoneClock())  // Gets union of both children
func (sv *SyncVar) MergeWaitGroupDoneClock(clock *vectorclock.VectorClock) {
	wgState := sv.GetOrCreateWaitGroup()
	if wgState.doneClock == nil {
		// First Done: Allocate and copy.
		wgState.doneClock = clock.Clone()
	} else {
		// Subsequent Done: Merge (join) the clocks.
		// For each thread, take the maximum clock value.
		wgState.doneClock.Join(clock)
	}
}

// GetWaitGroupDoneClock returns the WaitGroup's accumulated done clock.
//
// Returns nil if no Done() has been called yet.
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
//
// Example:
//
//	doneClock := sv.GetWaitGroupDoneClock()
//	if doneClock != nil {
//	    waiterCtx.C.Join(doneClock)  // Merge into waiter's clock
//	}
func (sv *SyncVar) GetWaitGroupDoneClock() *vectorclock.VectorClock {
	if sv.waitGroup == nil {
		return nil
	}
	return sv.waitGroup.doneClock
}

// GetWaitGroupCounter returns the current WaitGroup counter value.
//
// This is primarily for debugging and validation. Returns 0 if no
// WaitGroup operations have occurred.
//
// Thread Safety: NOT thread-safe on its own. The caller must ensure
// synchronization.
func (sv *SyncVar) GetWaitGroupCounter() int32 {
	if sv.waitGroup == nil {
		return 0
	}
	return sv.waitGroup.counter
}
