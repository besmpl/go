package shadowmem

import (
	"fmt"
	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/vectorclock"
	"sync"
	"testing"
)

func promotedCapabilityFixture(t *testing.T, tid uint32) (*PageTableShadow, uintptr, *VarState, *PromotedReadCapability, *vectorclock.VectorClock) {
	t.Helper()
	pt := NewPageTableShadow()
	addr := uintptr(0x7a001)
	state := pt.GetOrCreateSlot(addr).Isolate(uint8(addr & 7))
	state.SetReadEpoch(epoch.NewEpoch(1, 1))
	state.PromoteToReadClock(epoch.NewEpoch(2, 1), nil)
	state.UnlockAccess()
	capability := pt.PromotedReadCapability(addr, 1, tid, state)
	if capability == nil {
		t.Fatal("promoted capability was not created")
	}
	clock := vectorclock.New()
	clock.Set(tid, 2)
	return pt, addr, state, capability, clock
}

func TestPromotedReadCapabilityPublishesPairedEpochPC(t *testing.T) {
	_, addr, state, capability, clock := promotedCapabilityFixture(t, 7)
	wantEpoch := epoch.NewEpoch(7, 2)
	const wantPC = uintptr(0xabc123)
	if !capability.TryRead(addr, 1, wantEpoch, clock, wantPC) {
		t.Fatal("warmed promoted read missed")
	}
	gotEpoch, gotPC, ok := capability.node.stableEpochPC()
	if !ok || gotEpoch != wantEpoch || gotPC != wantPC {
		t.Fatalf("paired publication = (%v, %#x, %v), want (%v, %#x, true)", gotEpoch, gotPC, ok, wantEpoch, wantPC)
	}
	seen := vectorclock.New()
	seen.Set(1, 1)
	seen.Set(2, 1)
	if got, conflict := state.FirstConcurrentRead(seen); !conflict || got != wantEpoch {
		t.Fatalf("writer snapshot = (%v, %v), want (%v, true)", got, conflict, wantEpoch)
	}
	if got := state.GetReadPC(); got != wantPC {
		t.Fatalf("conflict PC = %#x, want %#x", got, wantPC)
	}
}

func TestPromotedReadCapabilityFailsAfterExclusiveCloseAndReset(t *testing.T) {
	_, addr, state, capability, clock := promotedCapabilityFixture(t, 9)
	state.ClosePromotedReadFrontier()
	if capability.TryRead(addr, 1, epoch.NewEpoch(9, 2), clock, 1) {
		t.Fatal("closed frontier accepted cached read")
	}
	state.Reset()
	if capability.TryRead(addr, 1, epoch.NewEpoch(9, 3), clock, 2) {
		t.Fatal("reset lifecycle accepted old capability")
	}
}

func TestPromotedReadCloseDuringOddPublicationForcesCanonicalRetry(t *testing.T) {
	_, addr, state, capability, clock := promotedCapabilityFixture(t, 8)
	current := epoch.NewEpoch(8, 2)
	seq := capability.node.seq.Load()
	if seq&1 != 0 || !capability.node.seq.CompareAndSwap(seq, seq+1) {
		t.Fatal("failed to claim reader node")
	}
	state.ClosePromotedReadFrontier()
	seen := vectorclock.New()
	seen.Set(1, 1)
	seen.Set(2, 1)
	if got, conflict := state.FirstConcurrentRead(seen); conflict {
		t.Fatalf("closer consumed in-flight node %v", got)
	}
	capability.node.pc.Store(0x8822)
	capability.node.epoch.Store(uint64(current))
	capability.node.seq.Store(seq + 2)
	if capability.mappingValid(addr, 1) {
		t.Fatal("publisher accepted a frontier closed during its odd interval")
	}
	// This is the canonical retry required after the failed final check. The
	// read is not lost even though the closer deliberately skipped its odd node.
	state.JoinReadClock(current, clock)
	if got, conflict := state.FirstConcurrentRead(seen); !conflict || got != current {
		t.Fatalf("canonical retry witness = (%v, %v), want (%v, true)", got, conflict, current)
	}
}

func TestPromotedReadRegistryIsExactAndUnbounded(t *testing.T) {
	pt, addr, state, _, _ := promotedCapabilityFixture(t, 3)
	const readers = 10_000
	for tid := uint32(4); tid < readers+4; tid++ {
		if cap := pt.PromotedReadCapability(addr, 1, tid, state); cap == nil {
			t.Fatalf("reader %d was capped", tid)
		}
	}
	count := 0
	for node := state.readClock.head.Load(); node != nil; node = node.next {
		count++
	}
	if count != readers+1 {
		t.Fatalf("registry nodes = %d, want %d", count, readers+1)
	}
}

func TestPromotedReadNodeConcurrentSameTIDInstall(t *testing.T) {
	frontier := newPromotedReadFrontier(vectorclock.New(), 1)
	for tid := uint32(1); tid <= 4; tid++ {
		if frontier.nodeFor(tid) == nil {
			t.Fatalf("inline prefill TID %d returned nil", tid)
		}
	}
	const workers = 64
	results := make([]*promotedReadNode, workers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	for i := range results {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			results[i] = frontier.nodeFor(123_456)
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()

	want := results[0]
	if want == nil {
		t.Fatal("concurrent install returned nil")
	}
	for i, node := range results {
		if node != want {
			t.Fatalf("worker %d node = %p, want stable node %p", i, node, want)
		}
	}
	count := 0
	matching := 0
	for node := frontier.head.Load(); node != nil; node = node.next {
		count++
		if node.tid == 123_456 {
			matching++
		}
	}
	if count != 5 || matching != 1 {
		t.Fatalf("registered nodes = %d (matching=%d), want 5 (matching=1)", count, matching)
	}
}

func TestPromotedReadNodeConcurrentDifferentTIDInstall(t *testing.T) {
	frontier := newPromotedReadFrontier(vectorclock.New(), 1)
	const workers = 1_024
	const firstTID = uint32(100_000)
	results := make([]*promotedReadNode, workers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	for i := range results {
		go func(i int) {
			defer done.Done()
			ready.Done()
			<-start
			results[i] = frontier.nodeFor(firstTID + uint32(i))
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()

	seen := make(map[*promotedReadNode]struct{}, workers)
	for i, node := range results {
		if node == nil {
			t.Fatalf("worker %d returned nil", i)
		}
		if node.tid != firstTID+uint32(i) {
			t.Fatalf("worker %d node TID = %d, want %d", i, node.tid, firstTID+uint32(i))
		}
		if _, duplicate := seen[node]; duplicate {
			t.Fatalf("worker %d reused node %p", i, node)
		}
		seen[node] = struct{}{}
		if got := frontier.nodeFor(node.tid); got != node {
			t.Fatalf("TID %d lookup = %p, want stable node %p", node.tid, got, node)
		}
	}
	count := 0
	for node := frontier.head.Load(); node != nil; node = node.next {
		count++
	}
	if count != workers {
		t.Fatalf("registered nodes = %d, want %d", count, workers)
	}
}

func TestPromotedReadCapabilityPartialClearClosesSurvivingGroup(t *testing.T) {
	pt := NewPageTableShadow()
	addr := uintptr(0x7c000)
	slot := pt.GetOrCreateSlot(addr)
	var state *VarState
	slot.AccessGroups(0x3, func(_ uint8, current *VarState) {
		state = current
		state.SetReadEpoch(epoch.NewEpoch(1, 1))
		state.PromoteToReadClock(epoch.NewEpoch(2, 1), nil)
	})
	capability := pt.PromotedReadCapability(addr, 2, 15, state)
	if capability == nil {
		t.Fatal("two-lane capability was not created")
	}
	pt.ClearRange(addr, 1)
	clock := vectorclock.New()
	clock.Set(15, 1)
	if capability.TryRead(addr, 2, epoch.NewEpoch(15, 1), clock, 0x15) {
		t.Fatal("partial clear retained stale group capability")
	}
	if slot.State(1) != state {
		t.Fatal("partial clear did not preserve surviving group lane")
	}
	if state.readClock.revision.Load()&1 == 0 {
		t.Fatal("partial clear left surviving group's old frontier open")
	}
}

func TestPromotedReadCapabilityIgnoresTransientSlotAndStateLocks(t *testing.T) {
	_, addr, state, capability, clock := promotedCapabilityFixture(t, 17)
	capability.slot.mu.lock()
	state.LockAccess()
	current := epoch.NewEpoch(17, 2)
	if !capability.TryRead(addr, 1, current, clock, 0x1717) {
		state.UnlockAccess()
		capability.slot.mu.unlock()
		t.Fatal("transient canonical-reader locks invalidated an unchanged mapping")
	}
	state.UnlockAccess()
	capability.slot.mu.unlock()
	if got, _, ok := capability.node.stableEpochPC(); !ok || got != current {
		t.Fatalf("published epoch = (%v, %v), want (%v, true)", got, ok, current)
	}
}

func TestPromotedReadCapabilityRejectsMismatchedTID(t *testing.T) {
	_, addr, _, capability, clock := promotedCapabilityFixture(t, 18)
	if capability.TryRead(addr, 1, epoch.NewEpoch(19, 2), clock, 0x1818) {
		t.Fatal("capability accepted an epoch belonging to another TID")
	}
	if got, _, ok := capability.node.stableEpochPC(); !ok || got != 0 {
		t.Fatalf("mismatched publication changed node to (%v, %v)", got, ok)
	}
}

func TestPromotedReadCapabilityConcurrentPublicationKeepsEpochPCPaired(t *testing.T) {
	_, addr, _, capability, clock := promotedCapabilityFixture(t, 20)
	const publications = 10_000
	clock.Set(20, publications)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for value := uint64(1); value <= publications; value++ {
			current := epoch.NewEpoch(20, value)
			if !capability.TryRead(addr, 1, current, clock, uintptr(value)) {
				t.Errorf("publication %d missed", value)
				return
			}
		}
	}()
	observations := 0
	observe := func() bool {
		current, pc, ok := capability.node.stableEpochPC()
		if !ok || current == 0 {
			return false
		}
		_, value := current.Decode()
		if pc != uintptr(value) {
			t.Fatalf("mixed publication epoch=%v pc=%#x", current, pc)
		}
		observations++
		return true
	}
	for {
		select {
		case <-done:
			if !observe() {
				t.Fatal("final publication was not stably observable")
			}
			if observations == 0 {
				t.Fatal("no stable publication was observed")
			}
			return
		default:
		}
		observe()
	}
}

func TestPromotedReadCapabilityConcurrentClosePreservesReadViaRetry(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		_, addr, state, capability, clock := promotedCapabilityFixture(t, 21)
		current := epoch.NewEpoch(21, 2)
		start := make(chan struct{})
		result := make(chan bool, 1)
		go func() {
			<-start
			result <- capability.TryRead(addr, 1, current, clock, 0x2121)
		}()
		close(start)
		state.ClosePromotedReadFrontier()
		if !<-result {
			// This is the detector caller's required canonical retry.
			state.JoinReadClock(current, clock)
		}
		seen := vectorclock.New()
		seen.Set(1, 1)
		seen.Set(2, 1)
		if got, conflict := state.FirstConcurrentRead(seen); !conflict || got != current {
			t.Fatalf("iteration %d witness = (%v, %v), want (%v, true)", iteration, got, conflict, current)
		}
	}
}

func TestPromotedReadCapabilityWarmPathAllocations(t *testing.T) {
	_, addr, _, capability, clock := promotedCapabilityFixture(t, 11)
	current := epoch.NewEpoch(11, 2)
	if allocs := testing.AllocsPerRun(1000, func() {
		if !capability.TryRead(addr, 1, current, clock, 7) {
			t.Fatal("warmed promoted read missed")
		}
	}); allocs != 0 {
		t.Fatalf("warmed promoted read allocations = %v, want 0", allocs)
	}
}

func BenchmarkPromotedReadCapabilityWarm(b *testing.B) {
	pt := NewPageTableShadow()
	addr := uintptr(0x7b001)
	state := pt.GetOrCreateSlot(addr).Isolate(uint8(addr & 7))
	state.SetReadEpoch(epoch.NewEpoch(1, 1))
	state.PromoteToReadClock(epoch.NewEpoch(2, 1), nil)
	state.UnlockAccess()
	capability := pt.PromotedReadCapability(addr, 1, 13, state)
	if capability == nil {
		b.Fatal("promoted capability was not created")
	}
	clock := vectorclock.New()
	clock.Set(13, 2)
	current := epoch.NewEpoch(13, 2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !capability.TryRead(addr, 1, current, clock, 9) {
			b.Fatal("warmed promoted read missed")
		}
	}
}

func BenchmarkPromotedReadRegistryColdParallel(b *testing.B) {
	for _, readers := range []int{64, 1_024, 10_000} {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				frontier := newPromotedReadFrontier(vectorclock.New(), uint64(iteration+1))
				start := make(chan struct{})
				var ready sync.WaitGroup
				var done sync.WaitGroup
				ready.Add(readers)
				done.Add(readers)
				for i := 0; i < readers; i++ {
					go func(tid uint32) {
						defer done.Done()
						ready.Done()
						<-start
						if frontier.nodeFor(tid) == nil {
							b.Errorf("reader %d returned nil", tid)
						}
					}(uint32(i + 1))
				}
				ready.Wait()
				close(start)
				done.Wait()
			}
			b.ReportMetric(float64(readers), "readers/op")
		})
	}
}
