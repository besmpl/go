// Package syncshadow implements shadow memory for synchronization primitives.
//
// This package tracks happens-before relationships created by the runtime's
// generic acquire, release, and release-merge operations.
//
// Key Concepts:
//
// Shadow Memory for Sync Primitives:
//   - Each runtime-selected synchronization address has a SyncVar
//   - SyncVar stores the releaseClock - the vector clock at last Release (Unlock)
//   - On Acquire (Lock), the thread merges the releaseClock into its own clock
//   - This establishes the happens-before: Unlock(m) → Lock(m)
//   - Buffered channels use per-slot addresses; close uses the channel address
//   - WaitGroup Done uses ReleaseMerge and Wait uses Acquire at one address
//
// FastTrack Sync Algorithm:
//
//	Acquire(m):  Ct := Ct ⊔ Lm  (thread clock joins lock clock)
//	             Ct[t]++         (increment thread's clock)
//
//	Release(m):  Lm := Ct        (lock clock = thread clock)
//	             Ct[t]++         (increment thread's clock)
//
// Where:
//   - Ct is the vector clock for thread t
//   - Lm is the release clock for mutex m
//   - ⊔ is the join operation (element-wise maximum)
//
// Example:
//
//	// Thread 1
//	mu.Lock()         // Acquire: C1 ⊔= L_mu
//	x = 42            // Write at C1
//	mu.Unlock()       // Release: L_mu = C1
//
//	// Thread 2 (happens after Thread 1's unlock)
//	mu.Lock()         // Acquire: C2 ⊔= L_mu (gets Thread 1's clock!)
//	y = x             // Read at C2 - NO RACE (Thread 1's write happened-before)
//	mu.Unlock()       // Release: L_mu = C2
//
// Performance:
//   - Existing-address GetOrCreate: lock-free page/segment chain lookup
//   - First access: one sharded writer lock and lazy state allocation
//   - ClearRange: indexed by touched pages, with a sparse scan for huge spans
//   - Memory: state and vector clocks are allocated lazily and retain sparse
//     storage capacity for reuse
package syncshadow
