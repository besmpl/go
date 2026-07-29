package detector

import (
	"testing"

	"runtime/race/kolkov/goroutine"
)

type scalarAtomicOverlapCase struct {
	name       string
	plainAddr  uintptr
	plainSize  uintptr
	atomicAddr uintptr
	wantRace   int
}

func scalarAtomicOverlapCases(base uintptr) []scalarAtomicOverlapCase {
	return []scalarAtomicOverlapCase{
		{
			name:       "uint16 tail byte overlaps uint32",
			plainAddr:  base + 3,
			plainSize:  2,
			atomicAddr: base + 4,
			wantRace:   1,
		},
		{
			name:       "aligned uint64 overlaps tail uint32",
			plainAddr:  base,
			plainSize:  8,
			atomicAddr: base + 4,
			wantRace:   1,
		},
		{
			name:       "same word later lane",
			plainAddr:  base + 2,
			plainSize:  4,
			atomicAddr: base + 4,
			wantRace:   1,
		},
		{
			name:       "second shadow word",
			plainAddr:  base + 4,
			plainSize:  8,
			atomicAddr: base + 8,
			wantRace:   1,
		},
		{
			name:       "same word adjacent",
			plainAddr:  base,
			plainSize:  4,
			atomicAddr: base + 4,
			wantRace:   0,
		},
		{
			name:       "next word adjacent",
			plainAddr:  base,
			plainSize:  8,
			atomicAddr: base + 8,
			wantRace:   0,
		},
	}
}

func TestAtomicFirstSizedScalarFindsEveryPhysicalOverlap(t *testing.T) {
	const base = uintptr(0x1c000)
	for _, test := range scalarAtomicOverlapCases(base) {
		for _, plainWrite := range []bool{false, true} {
			name := "read"
			if plainWrite {
				name = "write"
			}
			t.Run(test.name+"/plain "+name, func(t *testing.T) {
				d := NewDetector()
				atomicWriter := goroutine.Alloc(301)
				plain := goroutine.Alloc(302)
				completeAtomicSize(d, test.atomicAddr, 4, atomicWriter, false, true, 0xa101)

				if plainWrite {
					d.OnWriteSized(test.plainAddr, test.plainSize, plain, 0xa102)
				} else {
					d.OnReadSized(test.plainAddr, test.plainSize, plain, 0xa103)
				}
				if got := d.RacesDetected(); got != test.wantRace {
					t.Fatalf("reported %d races, want %d", got, test.wantRace)
				}
			})
		}
	}
}

func TestSizedScalarFirstRemainsVisibleToLaterPartialAtomic(t *testing.T) {
	const base = uintptr(0x1d000)
	for _, test := range scalarAtomicOverlapCases(base) {
		for _, plainWrite := range []bool{false, true} {
			name := "read then atomic write"
			if plainWrite {
				name = "write then atomic read"
			}
			t.Run(test.name+"/plain "+name, func(t *testing.T) {
				d := NewDetector()
				plain := goroutine.Alloc(303)
				atomic := goroutine.Alloc(304)
				if plainWrite {
					d.OnWriteSized(test.plainAddr, test.plainSize, plain, 0xa201)
					completeAtomicSize(d, test.atomicAddr, 4, atomic, true, false, 0xa202)
				} else {
					d.OnReadSized(test.plainAddr, test.plainSize, plain, 0xa203)
					completeAtomicSize(d, test.atomicAddr, 4, atomic, false, true, 0xa204)
				}
				if got := d.RacesDetected(); got != test.wantRace {
					t.Fatalf("reported %d races, want %d", got, test.wantRace)
				}
			})
		}
	}
}

func TestSizedScalarCompactHistoryAliasesCoveredLanesOnly(t *testing.T) {
	const base = uintptr(0x1e000)
	for _, write := range []bool{false, true} {
		name := "read"
		if write {
			name = "write"
		}
		t.Run(name, func(t *testing.T) {
			d := NewDetector()
			ctx := goroutine.Alloc(305)
			const addr = base + 4 // Crosses an aligned shadow-word boundary.
			if write {
				d.OnWriteSized(addr, 8, ctx, 0xa301)
			} else {
				d.OnReadSized(addr, 8, ctx, 0xa302)
			}

			start := d.ShadowGet(addr)
			if start == nil {
				t.Fatal("sized scalar published no start history")
			}
			for offset := uintptr(0); offset < 8; offset++ {
				if got := d.ShadowGet(addr + offset); got != start {
					t.Fatalf("covered lane +%d state = %p, want shared %p", offset, got, start)
				}
			}
			if got := d.ShadowGet(addr - 1); got != nil {
				t.Fatalf("preceding adjacent lane acquired history %p", got)
			}
			if got := d.ShadowGet(addr + 8); got != nil {
				t.Fatalf("following adjacent lane acquired history %p", got)
			}
			if first := d.rangeMemory.GetSlot(addr); first != nil {
				t.Fatalf("compact scalar materialized first word slot %p", first)
			}
			if second := d.rangeMemory.GetSlot(addr + 7); second != nil {
				t.Fatalf("compact scalar materialized second word slot %p", second)
			}
		})
	}
}

func TestSizedScalarVisitsEveryCrossWordAtomicOverlay(t *testing.T) {
	const base = uintptr(0x1f000)
	for _, write := range []bool{false, true} {
		name := "read"
		if write {
			name = "write"
		}
		t.Run(name, func(t *testing.T) {
			d := NewDetector()
			firstAtomic := goroutine.Alloc(306)
			secondAtomic := goroutine.Alloc(307)
			plain := goroutine.Alloc(308)
			completeAtomicSize(d, base+4, 4, firstAtomic, false, true, 0xa401)
			completeAtomicSize(d, base+8, 4, secondAtomic, false, true, 0xa402)

			if write {
				d.OnWriteSized(base+4, 8, plain, 0xa403)
			} else {
				d.OnReadSized(base+4, 8, plain, 0xa404)
			}
			if got := d.RacesDetected(); got != 1 {
				t.Fatalf("one logical scalar reported %d races, want 1", got)
			}

			first := atomicHistoryForTest(t, d, base+4)
			second := atomicHistoryForTest(t, d, base+8)
			for index, state := range []*atomicState{first, second} {
				state.mu.lock()
				history := state.plainReads
				if write {
					history = state.plainWrites
				}
				access, ok := atomicHistoryAccess(history, plain.TID)
				state.mu.unlock()
				if !ok {
					t.Fatalf("overlay %d retained no plain %s history", index, name)
				}
				wantMask := uint8(0x0f)
				if index == 0 {
					wantMask = 0xf0
				}
				for lane := uint8(0); lane < 8; lane++ {
					got := access.clocks[lane] != 0
					want := wantMask&(uint8(1)<<lane) != 0
					if got != want {
						t.Fatalf("overlay %d lane %d represented=%v, want %v", index, lane, got, want)
					}
				}
			}
		})
	}
}

func TestSizedScalarDeduplicatesSharedAtomicOverlayGroups(t *testing.T) {
	const base = uintptr(0x20000)
	for _, write := range []bool{false, true} {
		name := "read"
		if write {
			name = "write"
		}
		t.Run(name, func(t *testing.T) {
			d := NewDetector()
			atomicWriter := goroutine.Alloc(309)
			plain := goroutine.Alloc(310)
			completeAtomicSize(d, base, 8, atomicWriter, false, true, 0xa501)

			// Split the ordinary equivalence group without changing its shared
			// atomic overlay. The sized scalar must visit both groups but apply each
			// physical lane to the overlay exactly once.
			slot := d.rangeMemory.GetSlot(base)
			if slot == nil {
				t.Fatal("atomic setup did not materialize its shadow word")
			}
			isolated := slot.Isolate(0)
			isolated.UnlockAccess()

			if write {
				d.OnWriteSized(base, 8, plain, 0xa502)
			} else {
				d.OnReadSized(base, 8, plain, 0xa503)
			}
			if got := d.RacesDetected(); got != 1 {
				t.Fatalf("one shared overlay reported %d races, want 1", got)
			}

			state := atomicHistoryForTest(t, d, base)
			state.mu.lock()
			history := state.plainReads
			if write {
				history = state.plainWrites
			}
			access, ok := atomicHistoryAccess(history, plain.TID)
			state.mu.unlock()
			if !ok {
				t.Fatalf("shared overlay retained no plain %s history", name)
			}
			for lane, clock := range access.clocks {
				if clock == 0 {
					t.Fatalf("shared overlay lane %d was not represented", lane)
				}
			}
		})
	}
}
