package api

import (
	"internal/synctest"
	"runtime"
	"testing"
	"time"

	"runtime/race/kolkov/vectorclock"
)

func TestSynctestTimerContextReleased(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	// Warm the sole P's persistent timer context before taking the baseline.
	warm := make(chan struct{})
	time.AfterFunc(time.Nanosecond, func() { close(warm) })
	<-warm
	baseline := detachedContextKeySet()

	synctest.Run(func() {
		fired := make(chan struct{})
		time.AfterFunc(time.Second, func() { close(fired) })
		<-fired
	})

	got := detachedContextKeySet()
	if len(got) != len(baseline) {
		t.Fatalf("detached context count after synctest timer = %d, want %d", len(got), len(baseline))
	}
	for key := range baseline {
		if _, ok := got[key]; !ok {
			t.Fatalf("detached context baseline changed: missing %#x", key)
		}
	}
}

func detachedContextKeySet() map[uintptr]struct{} {
	detachedContextsMu.lock()
	keys := make(map[uintptr]struct{}, len(detachedContexts))
	for key := range detachedContexts {
		keys[key] = struct{}{}
	}
	detachedContextsMu.unlock()
	return keys
}

func TestGoStartFromExplicitContext(t *testing.T) {
	oldEnabled := enabled.Load()
	enabled.Store(1)
	defer enabled.Store(oldEnabled)

	parentPtr := raceContextStartFromRuntime(0x1233, 0)
	if parentPtr <= 1 {
		t.Fatalf("parent context = %#x", parentPtr)
	}
	defer raceContextEndFromRuntime(parentPtr)
	detachedContextsMu.lock()
	parent := detachedContexts[parentPtr]
	detachedContextsMu.unlock()
	if parent == nil {
		t.Fatal("parent context is not rooted")
	}
	parentTID := parent.TID
	parent.C.Set(65536, 9)

	spawnID := raceGoStartFromContext(0x1234, parentPtr)
	if spawnID == 0 {
		t.Fatal("explicit context start returned no spawn token")
	}

	const childGoid = int64(1 << 50)
	contextsMap.Delete(childGoid)
	childPtr := raceGoSetChildIDWithCtx(childGoid, spawnID)
	if childPtr <= 1 {
		t.Fatalf("child context = %#x", childPtr)
	}
	defer raceGoEndFromRuntime(childGoid)

	child, ok := contextsMap.Load(childGoid)
	if !ok {
		t.Fatal("child context is not rooted")
	}
	if child.C.Get(parentTID) != 1 || child.C.Get(65536) != 9 {
		t.Fatalf("child did not inherit explicit parent clock: %s", child.C)
	}
	if parent.C.Get(parentTID) != 2 {
		t.Fatalf("parent clock after spawn = %d, want 2", parent.C.Get(parentTID))
	}

	raceGoEndFromRuntime(childGoid)
	if _, ok := contextsMap.Load(childGoid); ok {
		t.Fatal("ended child remains rooted")
	}
	raceContextEndFromRuntime(parentPtr)
	detachedContextsMu.lock()
	_, parentRooted := detachedContexts[parentPtr]
	detachedContextsMu.unlock()
	if parentRooted {
		t.Fatal("ended explicit parent remains rooted")
	}
}

func TestConsumeSpawnContextByIDPreservesParentAssociation(t *testing.T) {
	parentA := vectorclock.New()
	parentA.Set(10, 41)
	parentB := vectorclock.New()
	parentB.Set(11, 79)
	infoA := &spawnInfo{id: 101, parentGID: 20001, parentClock: parentA}
	infoB := &spawnInfo{id: 202, parentGID: 20002, parentClock: parentB}

	contexts := []*spawnInfo{infoA, infoB}

	// Model the cross-P ordering that broke newest-entry matching: parent A
	// starts, parent B starts, then A binds its child first.
	got := consumeSpawnContextByIDLocked(&contexts, 101, 30001)
	if got != parentA {
		t.Fatalf("child A got parent clock %p, want %p", got, parentA)
	}
	if got.Get(10) != 41 || got.Get(11) != 0 {
		t.Fatalf("child A inherited wrong clock: C[10]=%d C[11]=%d", got.Get(10), got.Get(11))
	}
	if infoA.parentClock != nil {
		t.Fatal("consumed entry retained a pointer to the transferred clock")
	}
	if len(contexts) != 1 || contexts[0] != infoB {
		t.Fatalf("consumed entry not removed exactly: %#v", contexts)
	}
	if infoA.childGoid != 30001 || infoA.consumed.Load() != 1 {
		t.Fatalf("consumed entry not bound to child: child=%d consumed=%d", infoA.childGoid, infoA.consumed.Load())
	}
}

func TestConsumeSpawnContextByIDDoesNotConsumeAnotherEntry(t *testing.T) {
	clock := vectorclock.New()
	info := &spawnInfo{id: 101, parentClock: clock}

	contexts := []*spawnInfo{info}

	if got := consumeSpawnContextByIDLocked(&contexts, 202, 30001); got != nil {
		t.Fatalf("unknown token consumed clock %p", got)
	}
	if len(contexts) != 1 || contexts[0] != info || info.parentClock != clock {
		t.Fatal("unknown token mutated pending spawn entry")
	}
}

func TestClaimSpawnContextByIDTransfersCreationPC(t *testing.T) {
	clock := vectorclock.New()
	const creationPC = uintptr(0x123456)
	info := &spawnInfo{id: 303, parentClock: clock, pc: creationPC}
	contexts := []*spawnInfo{info}

	claimed := claimSpawnContextByIDLocked(&contexts, 303, 40001)
	if !claimed.found {
		t.Fatal("matching spawn token was not claimed")
	}
	if claimed.parentClock != clock {
		t.Fatalf("claimed parent clock = %p, want %p", claimed.parentClock, clock)
	}
	if claimed.creationPC != creationPC {
		t.Fatalf("claimed creation PC = %#x, want %#x", claimed.creationPC, creationPC)
	}
	if len(contexts) != 0 || info.parentClock != nil {
		t.Fatal("claimed spawn retained ownership in the pending slice")
	}
}
