package detector

import (
	"runtime/race/kolkov/goroutine"
	"testing"
)

func TestReleaseMergeDoesNotOrderWorkersButAcquireOrdersWaiter(t *testing.T) {
	d := NewDetector()
	const addr = uintptr(0x77aa00)
	first := goroutine.Alloc(701)
	second := goroutine.Alloc(702)
	waiter := goroutine.Alloc(703)

	firstClock := first.C.Get(first.TID)
	secondClock := second.C.Get(second.TID)
	d.OnReleaseMerge(addr, first)
	if got := second.C.Get(first.TID); got != 0 {
		t.Fatalf("first worker leaked into second before Done: %d", got)
	}
	d.OnReleaseMerge(addr, second)
	if got := first.C.Get(second.TID); got != 0 {
		t.Fatalf("second worker leaked into first before Wait: %d", got)
	}
	d.OnAcquire(addr, waiter)
	if got := waiter.C.Get(first.TID); got < firstClock {
		t.Fatalf("waiter first-worker clock = %d, want >= %d", got, firstClock)
	}
	if got := waiter.C.Get(second.TID); got < secondClock {
		t.Fatalf("waiter second-worker clock = %d, want >= %d", got, secondClock)
	}
}
