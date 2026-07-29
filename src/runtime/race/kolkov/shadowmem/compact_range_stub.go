//go:build !amd64 && !arm64

package shadowmem

import (
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
)

func (pt *PageTableShadow) TryCompactReadRange(_ uintptr, _ uintptr, _ epoch.Epoch, _ *vectorclock.VectorClock, _ uintptr) bool {
	return false
}

func (pt *PageTableShadow) TryCompactWriteRange(_ uintptr, _ uintptr, _ epoch.Epoch, _ *vectorclock.VectorClock, _ uintptr) bool {
	return false
}
