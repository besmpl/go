// Package syncshadow implements shadow memory for synchronization primitives.
//
// This package tracks happens-before relationships created by synchronization
// operations like mutex Lock/Unlock. It is used in Phase 4 of the FastTrack
// algorithm to eliminate false positives on properly synchronized code.
//
// Key Concepts:
//
// Shadow Memory for Sync Primitives:
//   - Each sync primitive (mutex, rwmutex, etc.) has a SyncVar in shadow memory
//   - SyncVar stores the releaseClock - the vector clock at last Release (Unlock)
//   - On Acquire (Lock), the thread merges the releaseClock into its own clock
//   - This establishes the happens-before: Unlock(m) → Lock(m)
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
//   - GetOrCreate: O(1) sync.Map lookup
//   - Memory: ~1KB per active mutex (VectorClock = 1KB)
//
// Phase 4 Implementation:
//   - Task 4.1: Mutex Acquire/Release tracking (this package)
//   - Task 4.2: RWMutex support (read/write locks)
//   - Task 4.3: Channel synchronization
//   - Task 4.4: Atomic operations
package syncshadow
