// Package epoch implements compact logical timestamps for FastTrack.
package epoch

import (
	"internal/runtime/atomic"
	"runtime/race/kolkov/vectorclock"
	_ "unsafe" // required for go:linkname
)

//go:linkname runtimeThrow runtime.throw
func runtimeThrow(s string)

// Epoch packs a 32-bit monotonic logical thread ID and a 32-bit clock.
type Epoch uint64

const (
	TIDBits         = 32
	ClockBits       = 32
	ClockMask       = uint64(1<<ClockBits) - 1
	MaxTID          = uint32(^uint32(0))
	MaxClock        = uint64(^uint32(0))
	MaxTIDWarning   = uint32((uint64(MaxTID) + 1) * 9 / 10)
	MaxClockWarning = (MaxClock + 1) * 9 / 10
)

var (
	tidOverflowDetected   atomic.Uint32
	clockOverflowDetected atomic.Uint32
	tidNearOverflow       atomic.Uint32
	clockNearOverflow     atomic.Uint32
)

func NewEpoch(tid uint32, clock uint64) Epoch {
	if clock > MaxClock {
		clockOverflowDetected.Store(1)
		runtimeThrow("race detector logical clock overflow")
	}
	if clock > MaxClockWarning {
		clockNearOverflow.Store(1)
	}
	if tid > MaxTIDWarning {
		tidNearOverflow.Store(1)
	}
	return Epoch(uint64(tid)<<ClockBits | (clock & ClockMask))
}

func (e Epoch) Decode() (tid uint32, clock uint64) {
	return uint32(uint64(e) >> ClockBits), uint64(e) & ClockMask
}

func (e Epoch) HappensBefore(vc *vectorclock.VectorClock) bool {
	tid, clock := e.Decode()
	return clock <= uint64(vc.Get(tid))
}
func (e Epoch) Same(other Epoch) bool { return e == other }
func (e Epoch) String() string {
	tid, clock := e.Decode()
	return itoa64(clock) + "@" + itoa64(uint64(tid))
}
func itoa64(n uint64) string {
	if n == 0 {
		return "0"
	}
	tmp, digits := n, 0
	for tmp > 0 {
		digits++
		tmp /= 10
	}
	buf := make([]byte, digits)
	for i := digits - 1; i >= 0; i-- {
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf)
}
func CheckOverflows() (bool, bool, bool, bool) {
	return tidOverflowDetected.Load() == 1, clockOverflowDetected.Load() == 1, tidNearOverflow.Load() == 1, clockNearOverflow.Load() == 1
}
func ResetOverflowFlags() {
	tidOverflowDetected.Store(0)
	clockOverflowDetected.Store(0)
	tidNearOverflow.Store(0)
	clockNearOverflow.Store(0)
}
