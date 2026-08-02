// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import _ "unsafe" // for go:linkname

// kolkovSpinWait backs the pure-Go race detector's runtime-compatible locks.
// Production detector hooks run on g0, which cannot enter the goroutine
// scheduler. The locks which saturate on the detector hot path have deliberately
// short critical sections, so both the initial and saturated waits remain
// bounded processor pauses rather than entering the kernel scheduler. Package
// tests and direct detector callers may run on a user goroutine, where yielding
// to the goroutine scheduler prevents a waiter from monopolizing the only P
// while the lock owner is runnable.
//
//go:linkname kolkovSpinWait
//go:nosplit
func kolkovSpinWait(cycles uint32, yield bool) {
	if !yield {
		procyield(cycles)
		return
	}

	gp := getg()
	if gp == gp.m.curg {
		if canPreemptM(gp.m) {
			Gosched()
			return
		}
		procyield(cycles)
		return
	}
	procyield(cycles)
}
