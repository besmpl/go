package detector

import (
	internalsync "internal/sync"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/goroutine"
	"runtime/race/kolkov/shadowmem"
	"runtime/race/kolkov/vectorclock"
)

func completePlainAtomicForTest(d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, acquire, write bool, pc uintptr) bool {
	var token AtomicToken
	d.AtomicBeginPlain(addr, size, ctx, acquire, true, &token)
	fast := atomicFastToken(&token) != nil
	d.AtomicEnd(addr, size, ctx, &token, pc, write)
	return fast
}

func completeRMWAtomicForTest(d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, write bool, pc uintptr) bool {
	var token AtomicToken
	d.AtomicBeginRMW(addr, size, ctx, true, &token)
	fast := atomicFastToken(&token) != nil
	d.AtomicEnd(addr, size, ctx, &token, pc, write)
	return fast
}

func enrollPlainAtomicForTest(t *testing.T, d *Detector, addr, size uintptr, ctx *goroutine.RaceContext) {
	t.Helper()
	if !completePlainAtomicForTest(d, addr, size, ctx, false, true, 0x5100) {
		t.Fatal("first plain atomic operation did not convert its locked enrollment")
	}
	if !completePlainAtomicForTest(d, addr, size, ctx, false, true, 0x5101) {
		t.Fatal("second plain atomic operation did not use enrolled fast transaction")
	}
}

func rearmPlainAtomicForTest(t *testing.T, d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr) {
	t.Helper()
	if completePlainAtomicForTest(d, addr, size, ctx, true, false, pc) {
		t.Fatal("first compatible operation bypassed escaped-generation probation")
	}
	if !completePlainAtomicForTest(d, addr, size, ctx, true, false, pc+1) {
		t.Fatal("second compatible operation did not publish a fresh fast generation")
	}
}

func plainAtomicCapabilityForTest(t *testing.T, d *Detector, addr, size uintptr) *shadowmem.AtomicFastPath {
	t.Helper()
	mask, ok := plainAtomicFastMask(addr, size)
	if !ok {
		t.Fatalf("plain fast mask for %#x/%d is invalid", addr, size)
	}
	slot := d.slotMemory.GetSlot(addr)
	if slot == nil {
		t.Fatalf("plain fast slot for %#x is nil", addr)
	}
	fast := slot.TryAtomicFast(mask)
	if fast == nil {
		t.Fatalf("plain fast capability for %#x/%d is unavailable", addr, size)
	}
	fast.Release()
	return fast
}

func waitForAtomicFastEscapeForTest(t *testing.T, slot *shadowmem.ShadowSlot, mask uint8, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		probe := slot.TryAtomicFast(mask)
		if probe == nil {
			break
		}
		probe.Release()
		select {
		case <-done:
			t.Fatal("clear returned before closing the progress-sentinel capability")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("clear did not reach the progress-sentinel capability")
		}
		runtime.Gosched()
	}
	select {
	case <-done:
		t.Fatal("clear returned while the slow atomic token was retained")
	default:
	}
}

func TestPlainAtomicFastTokenAndCompatibleSlowMiss(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(201)
	const addr = uintptr(0x31000)
	enrollPlainAtomicForTest(t, d, addr, 8, ctx)

	// Model a caller which resolved the slot before enrollment publication and
	// therefore reached the compatible setup path. It must neither redirect the
	// exact group nor close the already-published capability.
	slot := d.slotMemory.GetOrCreateSlot(addr)
	var locked *shadowmem.VarState
	slot.LockAtomicGroups(0xff, true, func(mask uint8, state *shadowmem.VarState) {
		if mask != 0xff {
			t.Fatalf("compatible setup mask = %#x, want 0xff", mask)
		}
		locked = state
	})
	locked.UnlockAccess()

	var token AtomicToken
	d.AtomicBeginPlain(addr, 8, ctx, false, true, &token)
	if token[0] == nil || token[1] != nil || atomicFastToken(&token) == nil {
		t.Fatalf("fast token discriminator = [%p %p], want capability followed by nil", token[0], token[1])
	}
	d.AtomicEnd(addr, 8, ctx, &token, 0x5102, true)
}

func TestAtomicRMWOnlyAddressEnrollsAndReuses(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(202)
	const addr = uintptr(0x31100)

	var token AtomicToken
	d.AtomicBeginRMW(addr, 8, ctx, true, &token)
	first := atomicFastToken(&token)
	if first == nil || token[1] != nil {
		t.Fatalf("first RMW-only token = [%p %p], want converted fast enrollment", token[0], token[1])
	}
	d.AtomicEnd(addr, 8, ctx, &token, 0x5110, true)

	d.AtomicBeginRMW(addr, 8, ctx, true, &token)
	if fast := atomicFastToken(&token); fast != first || token[1] != nil {
		t.Fatalf("second RMW-only token = [%p %p], want enrolled capability %p", token[0], token[1], first)
	}
	d.AtomicEnd(addr, 8, ctx, &token, 0x5111, true)
}

func TestAtomicRMWOnlyCapabilityClearRebindAndOrdinaryInvalidation(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(217)
	const addr = uintptr(0x31180)

	if !completeRMWAtomicForTest(d, addr, 8, ctx, true, 0x5112) {
		t.Fatal("RMW-only seed did not enroll a capability")
	}
	oldSlot := d.slotMemory.GetSlot(addr)
	old := plainAtomicCapabilityForTest(t, d, addr, 8)
	d.ClearShadowRange(addr, 8)
	if probe := oldSlot.TryAtomicFast(0xff); probe != nil {
		probe.Release()
		t.Fatal("cleared slot retained its RMW capability")
	}
	if !completeRMWAtomicForTest(d, addr, 8, ctx, true, 0x5113) {
		t.Fatal("RMW-only access did not enroll after clear")
	}
	fresh := plainAtomicCapabilityForTest(t, d, addr, 8)
	if fresh == old {
		t.Fatal("RMW-only clear/rebind reopened the cleared capability")
	}

	d.OnRead(addr, ctx, 0x5114)
	if slot := d.slotMemory.GetSlot(addr); slot != nil {
		if probe := slot.TryAtomicFast(0xff); probe != nil {
			probe.Release()
			t.Fatal("ordinary access left the RMW capability usable")
		}
	}
	if completeRMWAtomicForTest(d, addr, 8, ctx, true, 0x5115) {
		t.Fatal("first RMW after ordinary invalidation bypassed probation")
	}
	if !completeRMWAtomicForTest(d, addr, 8, ctx, true, 0x5116) {
		t.Fatal("second RMW after ordinary invalidation did not enroll")
	}
	if rebound := plainAtomicCapabilityForTest(t, d, addr, 8); rebound == fresh {
		t.Fatal("RMW after ordinary invalidation reopened the escaped capability")
	}
}

func TestAtomicRMWReusesExactCapability(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(203)
	failed := goroutine.Alloc(204)
	success := goroutine.Alloc(205)
	const addr = uintptr(0x31200)
	if !completeRMWAtomicForTest(d, addr, 8, seed, true, 0x511f) {
		t.Fatal("RMW-only seed did not enroll a capability")
	}
	capability := plainAtomicCapabilityForTest(t, d, addr, 8)
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	previousRelease := state.releases[0]
	state.mu.unlock()
	if previousRelease == nil {
		t.Fatal("plain enrollment published no release for failed CAS to acquire")
	}
	// Exercise failed-CAS completion against the promoted representation: the
	// acquire is semantic, but a failed hardware CAS must not append or advance
	// the release generation.
	anchor := vectorclock.New()
	joinAtomicReleaseRanges(anchor, previousRelease.runs)
	retireAtomicReleaseRanges(anchor, previousRelease.retired)
	previousRelease.lineage = vectorclock.NewClockLineage(anchor)
	previousRelease.view = previousRelease.lineage.Pin()
	previousStream, previousVersion := previousRelease.stream, previousRelease.version

	var token AtomicToken
	d.AtomicBeginRMW(addr, 8, failed, true, &token)
	if fast := atomicFastToken(&token); fast != capability || token[1] != nil {
		t.Fatalf("failed CAS token = [%p %p], want exact capability %p", token[0], token[1], capability)
	}
	if got := failed.C.Get(seed.TID); got == 0 {
		t.Fatal("failed CAS did not acquire the existing release")
	}
	d.AtomicEnd(addr, 8, failed, &token, 0x5120, false)

	state.mu.lock()
	failedRead, hasFailedRead := atomicHistoryAccess(state.reads, failed.TID)
	_, hasFailedWrite := atomicHistoryAccess(state.writes, failed.TID)
	for lane, release := range state.releases {
		if release != previousRelease {
			state.mu.unlock()
			t.Fatalf("failed CAS changed lane %d release from %p to %p", lane, previousRelease, release)
		}
	}
	if previousRelease.stream != previousStream || previousRelease.version != previousVersion {
		state.mu.unlock()
		t.Fatalf("failed CAS mutated release stream/version from %d/%d to %d/%d", previousStream, previousVersion, previousRelease.stream, previousRelease.version)
	}
	state.mu.unlock()
	if !hasFailedRead || failedRead.clocks[0] == 0 || hasFailedWrite {
		t.Fatalf("failed CAS history = read %+v/%v write=%v, want read only", failedRead, hasFailedRead, hasFailedWrite)
	}

	d.AtomicBeginRMW(addr, 8, success, true, &token)
	if fast := atomicFastToken(&token); fast != capability || token[1] != nil {
		t.Fatalf("successful RMW token = [%p %p], want exact capability %p", token[0], token[1], capability)
	}
	d.AtomicEnd(addr, 8, success, &token, 0x5121, true)
	state.mu.lock()
	_, hasSuccessWrite := atomicHistoryAccess(state.writes, success.TID)
	state.mu.unlock()
	if !hasSuccessWrite {
		t.Fatal("successful RMW did not record a write")
	}
	if fresh := plainAtomicCapabilityForTest(t, d, addr, 8); fresh != capability {
		t.Fatalf("exact RMW replaced capability %p with %p", capability, fresh)
	}
}

func TestAtomicRMWCooperativeContentionIsClean(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(206)
	contender := goroutine.Alloc(207)
	defer seed.C.Release()
	defer contender.C.Release()
	const addr = uintptr(0x31240)
	if !completeRMWAtomicForTest(d, addr, 8, seed, true, 0x5121) {
		t.Fatal("RMW-only seed did not enroll a capability")
	}
	capability := plainAtomicCapabilityForTest(t, d, addr, 8)
	state := atomicHistoryForTest(t, d, addr)

	state.mu.lock()
	beforeRevision := state.writerRevision.Load()
	beforePinned := state.arena.pinned.Load()
	beforeReads := atomicHistoryCardinality(state.reads)
	beforeWrites := atomicHistoryCardinality(state.writes)
	beforeEpoch := contender.GetEpoch()
	beforeSeedClock := contender.C.Get(seed.TID)
	var token AtomicToken
	token[0] = unsafe.Pointer(new(byte))
	retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, contender, true, &token)
	if !retry {
		state.mu.unlock()
		t.Fatal("cooperative RMW contention did not request a retry")
	}
	for i, retained := range token {
		if retained != nil {
			state.mu.unlock()
			t.Fatalf("contention retained token[%d] = %p", i, retained)
		}
	}
	if state.transactionActive {
		state.mu.unlock()
		t.Fatal("contention began an atomic transaction")
	}
	if got := state.writerRevision.Load(); got != beforeRevision {
		state.mu.unlock()
		t.Fatalf("contention changed writer revision from %d to %d", beforeRevision, got)
	}
	if got := state.arena.pinned.Load(); got != beforePinned {
		state.mu.unlock()
		t.Fatalf("contention changed arena pins from %d to %d", beforePinned, got)
	}
	if got := atomicHistoryCardinality(state.reads); got != beforeReads {
		state.mu.unlock()
		t.Fatalf("contention changed read history cardinality from %d to %d", beforeReads, got)
	}
	if got := atomicHistoryCardinality(state.writes); got != beforeWrites {
		state.mu.unlock()
		t.Fatalf("contention changed write history cardinality from %d to %d", beforeWrites, got)
	}
	state.mu.unlock()
	if got := contender.GetEpoch(); got != beforeEpoch {
		t.Fatalf("contention changed contender epoch from %v to %v", beforeEpoch, got)
	}
	if got := contender.C.Get(seed.TID); got != beforeSeedClock {
		t.Fatalf("contention imported release clock %d, want %d", got, beforeSeedClock)
	}
	if fresh := plainAtomicCapabilityForTest(t, d, addr, 8); fresh != capability {
		t.Fatalf("contention replaced capability %p with %p", capability, fresh)
	}

	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, contender, true, &token); retry {
		t.Fatal("uncontended cooperative RMW requested a retry")
	}
	if fast := atomicFastToken(&token); fast != capability {
		t.Fatalf("uncontended cooperative token capability = %p, want %p", fast, capability)
	}
	d.AtomicEnd(addr, 8, contender, &token, 0x5122, true)
}

func TestAtomicRMWCooperativeHandsOffOneRetainedWaiter(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(2070)
	owner := goroutine.Alloc(2071)
	first := goroutine.Alloc(2072)
	second := goroutine.Alloc(2073)
	defer seed.C.Release()
	defer owner.C.Release()
	defer first.C.Release()
	defer second.C.Release()
	const addr = uintptr(0x31248)
	if !completeRMWAtomicForTest(d, addr, 8, seed, true, 0x5123) {
		t.Fatal("RMW-only seed did not enroll a capability")
	}

	var ownerToken AtomicToken
	if retry, _, _, park := d.AtomicBeginRMWCooperative(addr, 8, owner, true, &ownerToken); retry || park != nil {
		t.Fatalf("owner begin = retry %v, park %p", retry, park)
	}
	capability := atomicFastToken(&ownerToken)
	if capability == nil {
		t.Fatal("owner did not retain exact capability")
	}

	var firstToken, secondToken AtomicToken
	firstRetry, firstSpin, _, firstPark := d.AtomicBeginRMWCooperative(addr, 8, first, true, &firstToken)
	secondRetry, secondSpin, _, secondPark := d.AtomicBeginRMWCooperative(addr, 8, second, true, &secondToken)
	if !firstRetry || !firstSpin || firstPark == nil || !secondRetry || secondSpin || secondPark == nil || firstPark == secondPark {
		t.Fatalf("waiter registration = first(%v,%p) second(%v,%p)", firstRetry, firstPark, secondRetry, secondPark)
	}
	if *firstPark != 0 {
		t.Fatalf("spinner doorbell = %d, want 0 before owner completion", *firstPark)
	}
	if atomicFastToken(&firstToken) != capability || atomicFastToken(&secondToken) != capability {
		t.Fatal("parked waiter did not retain the exact capability")
	}

	wake := d.AtomicEndMode(addr, 8, owner, &ownerToken, 0x5124, true, true)
	if wake != nil {
		t.Fatalf("owner unexpectedly woke parked waiter %p", wake)
	}
	if *firstPark != 1 {
		t.Fatalf("spinner doorbell = %d, want 1 after owner completion", *firstPark)
	}
	state := atomicHistoryForTest(t, d, addr)
	if !state.mu.tryLock() {
		t.Fatal("spinner cohort completion did not unlock for competition")
	}
	state.mu.unlock()

	// A failed CAS still owns the granted transaction and hands the next waiter
	// onward without replaying its already-authoritative hardware result.
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, first, true, &firstToken); retry {
		t.Fatal("spinner did not acquire unlocked state")
	}
	if *firstPark != 0 {
		t.Fatalf("accepted spinner doorbell = %d, want consumed 0", *firstPark)
	}
	wake = d.AtomicEndMode(addr, 8, first, &firstToken, 0x5125, false, true)
	if wake != secondPark {
		t.Fatalf("failed-CAS completion wake = %p, want %p", wake, secondPark)
	}
	d.AtomicResumeRMW(addr, 8, second, true, &secondToken)
	if wake = d.AtomicEndMode(addr, 8, second, &secondToken, 0x5126, true, true); wake != nil {
		t.Fatalf("last waiter returned unexpected wake %p", wake)
	}
	if !state.mu.tryLock() {
		t.Fatal("last waiter did not release exact state lock")
	}
	state.mu.unlock()
}

func TestAtomicRMWRecentOwnerSelectsPoliteSpinnerHint(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(2080)
	repeat := goroutine.Alloc(2081)
	spinner := goroutine.Alloc(2209)
	unique := goroutine.Alloc(2083)
	other := goroutine.Alloc(2084)
	defer seed.C.Release()
	defer repeat.C.Release()
	defer spinner.C.Release()
	defer unique.C.Release()
	defer other.C.Release()
	const addr = uintptr(0x31250)
	if !completeRMWAtomicForTest(d, addr, 8, seed, true, 0x5127) {
		t.Fatal("seed did not enroll capability")
	}
	complete := func(ctx *goroutine.RaceContext, pc uintptr) {
		var token AtomicToken
		if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, ctx, true, &token); retry {
			t.Fatal("unexpected owner retry")
		}
		d.AtomicEndMode(addr, 8, ctx, &token, pc, true, true)
	}
	complete(repeat, 0x5128)
	var ownerToken, spinnerToken AtomicToken
	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, repeat, true, &ownerToken); retry {
		t.Fatal("repeat owner retried")
	}
	retry, spin, polite, park := d.AtomicBeginRMWCooperative(addr, 8, spinner, true, &spinnerToken)
	if !retry || !spin || !polite || park == nil || *park != 0 {
		t.Fatalf("repeat-owner spinner = retry %v spin %v polite %v park %p", retry, spin, polite, park)
	}
	if spinnerToken[2] == nil {
		t.Fatal("wide repeat-owner cohort did not select patient spinner delay")
	}
	d.AtomicEndMode(addr, 8, repeat, &ownerToken, 0x5129, true, true)
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, spinner, true, &spinnerToken); retry {
		t.Fatal("repeat-owner spinner did not acquire")
	}
	d.AtomicEndMode(addr, 8, spinner, &spinnerToken, 0x512a, true, true)

	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, unique, true, &ownerToken); retry {
		t.Fatal("unique owner retried")
	}
	retry, spin, polite, park = d.AtomicBeginRMWCooperative(addr, 8, other, true, &spinnerToken)
	if !retry || !spin || polite || park == nil || *park != 0 {
		t.Fatalf("unique-owner spinner = retry %v spin %v polite %v park %p", retry, spin, polite, park)
	}
	if spinnerToken[2] != nil {
		t.Fatal("narrow unique-owner cohort selected patient spinner delay")
	}
	d.AtomicEndMode(addr, 8, unique, &ownerToken, 0x512b, true, true)
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, other, true, &spinnerToken); retry {
		t.Fatal("unique-owner spinner did not acquire")
	}
	d.AtomicEndMode(addr, 8, other, &spinnerToken, 0x512c, true, true)
}

func TestAtomicRMWMassiveWaiterCohortSelectsPatientSpinnerHint(t *testing.T) {
	var state atomicState
	state.rmwQueue.lock()
	defer state.rmwQueue.unlock()

	state.rmwWaiters = rmwPatientWaiters - 1
	if state.publicRMWPatientLocked(1) {
		t.Fatal("sub-threshold waiter cohort selected patient spinner delay")
	}
	state.rmwWaiters = rmwPatientWaiters
	if !state.publicRMWPatientLocked(1) {
		t.Fatal("massive waiter cohort did not select patient spinner delay")
	}
}

func TestAtomicResumeRMWWithoutSynchronizationKeepsHistoryButNotClockOrRelease(t *testing.T) {
	d := NewDetector()
	owner := goroutine.Alloc(2090)
	waiter := goroutine.Alloc(2091)
	defer owner.C.Release()
	defer waiter.C.Release()
	const addr = uintptr(0x52200)

	var ownerToken AtomicToken
	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, owner, true, &ownerToken); retry {
		t.Fatal("initial owner unexpectedly retried")
	}
	var waiterToken AtomicToken
	retry, _, _, park := d.AtomicBeginRMWCooperative(addr, 8, waiter, true, &waiterToken)
	if !retry || park == nil {
		t.Fatalf("contended waiter = retry %v park %p, want true/non-nil", retry, park)
	}
	wake := d.AtomicEndMode(addr, 8, owner, &ownerToken, 0x5221, true, true)
	if wake != nil || *park != 1 {
		t.Fatalf("owner handoff = wake %p doorbell %d, want nil/1", wake, *park)
	}

	before := waiter.GetEpoch()
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, waiter, false, &waiterToken); retry {
		t.Fatal("granted non-synchronizing waiter retried")
	}
	if wake := d.AtomicEndMode(addr, 8, waiter, &waiterToken, 0x5222, true, false); wake != nil {
		t.Fatalf("last waiter returned unexpected wake %p", wake)
	}
	if got := waiter.GetEpoch(); got != before {
		t.Fatalf("non-synchronizing resume advanced epoch from %v to %v", before, got)
	}
	state := atomicHistoryForTest(t, d, addr)
	if _, ok := atomicHistoryAccess(state.writes, waiter.TID); !ok {
		t.Fatal("non-synchronizing resumed write lost atomic history")
	}
	if release, _ := state.exactReleaseForMask(0xff); release != nil {
		t.Fatal("non-synchronizing resumed write retained a release")
	}
}

func TestAtomicRMWSpinnerForcesGrantAfterOwnershipEpochMisses(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(2090)
	owner := goroutine.Alloc(2091)
	spinner := goroutine.Alloc(2092)
	thief := goroutine.Alloc(2300)
	defer seed.C.Release()
	defer owner.C.Release()
	defer spinner.C.Release()
	defer thief.C.Release()
	const addr = uintptr(0x31258)
	if !completeRMWAtomicForTest(d, addr, 8, seed, true, 0x512d) {
		t.Fatal("seed did not enroll capability")
	}

	var ownerToken, spinnerToken AtomicToken
	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, owner, true, &ownerToken); retry {
		t.Fatal("owner retried")
	}
	retry, spin, _, doorbell := d.AtomicBeginRMWCooperative(addr, 8, spinner, true, &spinnerToken)
	if !retry || !spin || doorbell == nil || *doorbell != 0 {
		t.Fatalf("spinner enrollment = retry %v spin %v doorbell %p/%d", retry, spin, doorbell, valueOrZero(doorbell))
	}
	if spinnerToken[2] != nil {
		t.Fatal("narrow initial owner selected patient spinner delay")
	}
	state := atomicHistoryForTest(t, d, addr)
	activeOwner := owner

	for miss := uint8(1); miss <= 64; miss++ {
		if wake := d.AtomicEndMode(addr, 8, activeOwner, &ownerToken, 0x512e+uintptr(miss), true, true); wake != nil {
			t.Fatalf("ownership epoch %d returned spinner wake %p", miss, wake)
		}
		if *doorbell != 1 {
			t.Fatalf("ownership epoch %d doorbell = %d, want 1", miss, *doorbell)
		}
		// This test isolates the older epoch-miss force path. The locality
		// certificate has its own bounded takeover tests below.
		if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, thief, true, &ownerToken); retry {
			t.Fatalf("thief ownership epoch %d retried", miss)
		}
		activeOwner = thief
		retry, spin, _, park := d.AtomicResumeRMW(addr, 8, spinner, true, &spinnerToken)
		if !retry || !spin || park != doorbell || *doorbell != 0 {
			t.Fatalf("miss %d resume = retry %v spin %v park %p doorbell %d", miss, retry, spin, park, *doorbell)
		}
		if spinnerToken[2] == nil {
			t.Fatalf("miss %d did not refresh patient delay for wide replacement owner", miss)
		}
		state.rmwQueue.lock()
		gotMisses, forced := state.rmwSpinnerMisses, state.rmwForceSpinner
		state.rmwQueue.unlock()
		if gotMisses != miss || forced != (miss == 64) {
			t.Fatalf("miss %d state = misses %d forced %v", miss, gotMisses, forced)
		}
	}

	if wake := d.AtomicEndMode(addr, 8, thief, &ownerToken, 0x516e, true, true); wake != nil {
		t.Fatalf("forced spinner grant returned wake %p", wake)
	}
	if *doorbell != 1 || state.mu.state.Load() == 0 {
		t.Fatalf("forced grant doorbell/lock = %d/%d, want 1/locked", *doorbell, state.mu.state.Load())
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, spinner, true, &spinnerToken); retry {
		t.Fatal("forced spinner grant did not resume")
	}
	if *doorbell != 0 {
		t.Fatalf("accepted forced grant retained doorbell %d", *doorbell)
	}
	d.AtomicEndMode(addr, 8, spinner, &spinnerToken, 0x516f, true, true)
	state.rmwQueue.lock()
	invalid := state.rmwSpinnerTID != 0 || state.rmwSpinnerMisses != 0 || state.rmwForceSpinner ||
		state.rmwCohortOps != 0 || state.rmwPromoteParked || state.rmwPromotedOwner ||
		state.rmwWakeCompetitors != 0 || state.rmwGranted ||
		state.rmwGrantTID != 0 || state.rmwSpinnerEpoch != 0
	state.rmwQueue.unlock()
	if invalid {
		t.Fatal("accepted forced grant retained spinner lifecycle state")
	}
}

func TestAtomicRMWForcedSpinnerReopensParkedCompetition(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(2130)
	owner := goroutine.Alloc(2131)
	spinner := goroutine.Alloc(2132)
	parked := goroutine.Alloc(2133)
	thief := goroutine.Alloc(2134)
	defer seed.C.Release()
	defer owner.C.Release()
	defer spinner.C.Release()
	defer parked.C.Release()
	defer thief.C.Release()
	const addr = uintptr(0x31298)
	if !completeRMWAtomicForTest(d, addr, 8, seed, true, 0x5205) {
		t.Fatal("seed did not enroll capability")
	}

	var ownerToken, spinnerToken, parkedToken AtomicToken
	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, owner, true, &ownerToken); retry {
		t.Fatal("owner retried")
	}
	retry, spin, _, doorbell := d.AtomicBeginRMWCooperative(addr, 8, spinner, true, &spinnerToken)
	if !retry || !spin || doorbell == nil || *doorbell != 0 {
		t.Fatal("spinner did not enroll")
	}
	retry, spin, _, parkedSema := d.AtomicBeginRMWCooperative(addr, 8, parked, true, &parkedToken)
	if !retry || spin || parkedSema == nil || parkedSema == doorbell {
		t.Fatal("parked waiter did not enroll")
	}

	active := owner
	for miss := uint8(1); miss <= 64; miss++ {
		if wake := d.AtomicEndMode(addr, 8, active, &ownerToken, 0x5205+uintptr(miss), true, true); wake != nil {
			t.Fatalf("miss epoch %d returned wake %p", miss, wake)
		}
		if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, thief, true, &ownerToken); retry {
			t.Fatalf("thief epoch %d retried", miss)
		}
		active = thief
		retry, spin, _, park := d.AtomicResumeRMW(addr, 8, spinner, true, &spinnerToken)
		if !retry || !spin || park != doorbell || *doorbell != 0 {
			t.Fatalf("spinner miss %d = retry %v spin %v park %p bell %d", miss, retry, spin, park, *doorbell)
		}
	}
	if wake := d.AtomicEndMode(addr, 8, thief, &ownerToken, 0x5246, true, true); wake != nil {
		t.Fatalf("forced grant returned wake %p", wake)
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, spinner, true, &spinnerToken); retry {
		t.Fatal("forced spinner did not resume")
	}
	if wake := d.AtomicEndMode(addr, 8, spinner, &spinnerToken, 0x5247, true, true); wake != parkedSema {
		t.Fatalf("forced spinner wake = %p, want competitor %p", wake, parkedSema)
	}
	state := atomicHistoryForTest(t, d, addr)
	state.rmwQueue.lock()
	reopened := !state.rmwGranted && !state.rmwOwner && state.rmwWakeCompetitors == 1 && state.mu.state.Load() == 0
	state.rmwQueue.unlock()
	if !reopened {
		t.Fatal("forced spinner entered a reserved semaphore chain")
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, parked, true, &parkedToken); retry {
		t.Fatal("unreserved competitor did not acquire")
	}

	// A new contender can immediately become a spinner behind the competitor;
	// the queue is no longer forced through one reserved handoff per operation.
	var nextToken AtomicToken
	retry, spin, _, nextDoorbell := d.AtomicBeginRMWCooperative(addr, 8, owner, true, &nextToken)
	if !retry || !spin || nextDoorbell == nil || *nextDoorbell != 0 {
		t.Fatalf("next spinner = retry %v spin %v doorbell %p/%d", retry, spin, nextDoorbell, valueOrZero(nextDoorbell))
	}
	d.AtomicEndMode(addr, 8, parked, &parkedToken, 0x5248, true, true)
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, owner, true, &nextToken); retry {
		t.Fatal("next spinner did not acquire reopened competition")
	}
	d.AtomicEndMode(addr, 8, owner, &nextToken, 0x5249, true, true)
}

func TestAtomicRMWLateCompetitorRequeuesBehindForcedSpinner(t *testing.T) {
	d := NewDetector()
	contexts := make([]*goroutine.RaceContext, 7)
	for i := range contexts {
		contexts[i] = goroutine.Alloc(uint32(2160 + i))
		defer contexts[i].C.Release()
	}
	seed, owner, firstSpinner, lateCompetitor, thief, secondSpinner, nextOwner :=
		contexts[0], contexts[1], contexts[2], contexts[3], contexts[4], contexts[5], contexts[6]
	const addr = uintptr(0x312d8)
	if !completeRMWAtomicForTest(d, addr, 8, seed, true, 0x5260) {
		t.Fatal("seed did not enroll capability")
	}

	var ownerToken, firstSpinnerToken, lateToken AtomicToken
	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, owner, true, &ownerToken); retry {
		t.Fatal("owner retried")
	}
	retry, spin, _, firstDoorbell := d.AtomicBeginRMWCooperative(addr, 8, firstSpinner, true, &firstSpinnerToken)
	if !retry || !spin || firstDoorbell == nil {
		t.Fatal("first spinner did not enroll")
	}
	retry, spin, _, sema := d.AtomicBeginRMWCooperative(addr, 8, lateCompetitor, true, &lateToken)
	if !retry || spin || sema == nil || sema == firstDoorbell {
		t.Fatal("late competitor did not park")
	}

	active := owner
	for miss := uint8(1); miss <= 64; miss++ {
		if wake := d.AtomicEndMode(addr, 8, active, &ownerToken, 0x5260+uintptr(miss), true, true); wake != nil {
			t.Fatalf("first epoch %d returned wake %p", miss, wake)
		}
		if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, thief, true, &ownerToken); retry {
			t.Fatalf("first thief epoch %d retried", miss)
		}
		active = thief
		if retry, spin, _, park := d.AtomicResumeRMW(addr, 8, firstSpinner, true, &firstSpinnerToken); !retry || !spin || park != firstDoorbell {
			t.Fatalf("first spinner miss %d did not retry", miss)
		}
	}
	if wake := d.AtomicEndMode(addr, 8, thief, &ownerToken, 0x52a1, true, true); wake != nil {
		t.Fatalf("first forced grant returned wake %p", wake)
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, firstSpinner, true, &firstSpinnerToken); retry {
		t.Fatal("first forced spinner did not resume")
	}
	if wake := d.AtomicEndMode(addr, 8, firstSpinner, &firstSpinnerToken, 0x52a2, true, true); wake != sema {
		t.Fatalf("competitor wake = %p, want %p", wake, sema)
	}

	// Leave the released competitor runnable. A new cohort can acquire the
	// unlocked state and reserve it for another spinner before that competitor
	// reaches Resume.
	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, nextOwner, true, &ownerToken); retry {
		t.Fatal("next owner retried")
	}
	var secondSpinnerToken AtomicToken
	retry, spin, _, secondDoorbell := d.AtomicBeginRMWCooperative(addr, 8, secondSpinner, true, &secondSpinnerToken)
	if !retry || !spin || secondDoorbell == nil {
		t.Fatal("second spinner did not enroll")
	}
	active = nextOwner
	for miss := uint8(1); miss <= 64; miss++ {
		if wake := d.AtomicEndMode(addr, 8, active, &ownerToken, 0x52a2+uintptr(miss), true, true); wake != nil {
			t.Fatalf("second epoch %d returned wake %p", miss, wake)
		}
		if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, thief, true, &ownerToken); retry {
			t.Fatalf("second thief epoch %d retried", miss)
		}
		active = thief
		if retry, spin, _, park := d.AtomicResumeRMW(addr, 8, secondSpinner, true, &secondSpinnerToken); !retry || !spin || park != secondDoorbell {
			t.Fatalf("second spinner miss %d did not retry", miss)
		}
	}
	if wake := d.AtomicEndMode(addr, 8, thief, &ownerToken, 0x52e3, true, true); wake != nil {
		t.Fatalf("second forced grant returned wake %p", wake)
	}

	if retry, spin, _, park := d.AtomicResumeRMW(addr, 8, lateCompetitor, true, &lateToken); !retry || spin || park != sema {
		t.Fatalf("late competitor = retry %v spin %v park %p, want parked retry", retry, spin, park)
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, secondSpinner, true, &secondSpinnerToken); retry {
		t.Fatal("second forced spinner did not resume")
	}
	if wake := d.AtomicEndMode(addr, 8, secondSpinner, &secondSpinnerToken, 0x52e4, true, true); wake != sema {
		t.Fatalf("second spinner wake = %p, want %p", wake, sema)
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, lateCompetitor, true, &lateToken); retry {
		t.Fatal("requeued competitor did not receive reserved handoff")
	}
	d.AtomicEndMode(addr, 8, lateCompetitor, &lateToken, 0x52e5, true, true)
}

func TestAtomicRMWCohortPromotesParkedWaiterWithDescheduledSpinner(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(2094)
	owner := goroutine.Alloc(2095)
	spinner := goroutine.Alloc(2096)
	parked := goroutine.Alloc(2097)
	thief := goroutine.Alloc(2098)
	late := goroutine.Alloc(2099)
	defer seed.C.Release()
	defer owner.C.Release()
	defer spinner.C.Release()
	defer parked.C.Release()
	defer thief.C.Release()
	defer late.C.Release()
	const addr = uintptr(0x31260)
	if !completeRMWAtomicForTest(d, addr, 8, seed, true, 0x5172) {
		t.Fatal("seed did not enroll capability")
	}

	var ownerToken, spinnerToken, parkedToken AtomicToken
	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, owner, true, &ownerToken); retry {
		t.Fatal("owner retried")
	}
	retry, spin, _, doorbell := d.AtomicBeginRMWCooperative(addr, 8, spinner, true, &spinnerToken)
	if !retry || !spin || doorbell == nil || *doorbell != 0 {
		t.Fatalf("spinner enrollment = retry %v spin %v doorbell %p/%d", retry, spin, doorbell, valueOrZero(doorbell))
	}
	retry, spin, _, parkedSema := d.AtomicBeginRMWCooperative(addr, 8, parked, true, &parkedToken)
	if !retry || spin || parkedSema == nil || parkedSema == doorbell {
		t.Fatalf("parked enrollment = retry %v spin %v park %p", retry, spin, parkedSema)
	}

	// Do not resume the spinner. New owners repeatedly steal the unlocked
	// exact-fast lock while its one-bit doorbell remains set. The owner-side
	// cohort counter must still force the spinner after a bounded number of
	// completed hardware epochs; coalescing doorbell stores cannot hide them.
	active := owner
	state := atomicHistoryForTest(t, d, addr)
	for completion := uint16(1); completion <= rmwCohortLimit; completion++ {
		if wake := d.AtomicEndMode(addr, 8, active, &ownerToken, 0x5173+uintptr(completion), true, true); wake != nil {
			t.Fatalf("cohort completion %d returned wake %p", completion, wake)
		}
		if completion == rmwCohortLimit {
			break
		}
		if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, thief, true, &ownerToken); retry {
			t.Fatalf("thief completion %d retried", completion)
		}
		active = thief
	}

	state.rmwQueue.lock()
	forced, promote, granted, grantTID, cohortOps, locked := state.rmwForceSpinner, state.rmwPromoteParked,
		state.rmwGranted, state.rmwGrantTID, state.rmwCohortOps, state.mu.state.Load()
	state.rmwQueue.unlock()
	if !forced || !promote || !granted || grantTID != spinner.TID || cohortOps != rmwCohortLimit || locked == 0 || *doorbell != 1 {
		t.Fatalf("bounded grant = forced %v promote %v granted %v tid %d cohort %d lock %d bell %d",
			forced, promote, granted, grantTID, cohortOps, locked, *doorbell)
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, spinner, true, &spinnerToken); retry {
		t.Fatal("forced spinner did not resume")
	}

	if wake := d.AtomicEndMode(addr, 8, spinner, &spinnerToken, 0x51b4, true, true); wake != parkedSema {
		t.Fatalf("forced spinner competitor wake = %p, want %p", wake, parkedSema)
	}
	state.rmwQueue.lock()
	unreserved := state.rmwWakeCompetitors == 1 && !state.rmwGranted && !state.rmwOwner && state.mu.state.Load() == 0
	state.rmwQueue.unlock()
	if !unreserved {
		t.Fatal("forced spinner completion did not reopen unlocked competition")
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, parked, true, &parkedToken); retry {
		t.Fatal("woken competitor did not acquire unlocked state")
	}
	var lateToken AtomicToken
	retry, spin, _, lateDoorbell := d.AtomicBeginRMWCooperative(addr, 8, late, true, &lateToken)
	if !retry || !spin || lateDoorbell == nil || *lateDoorbell != 0 {
		t.Fatalf("reopened spinner = retry %v spin %v doorbell %p/%d", retry, spin, lateDoorbell, valueOrZero(lateDoorbell))
	}
	if wake := d.AtomicEndMode(addr, 8, parked, &parkedToken, 0x51b5, false, true); wake != nil {
		t.Fatalf("competitor owner returned wake %p", wake)
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 8, late, true, &lateToken); retry {
		t.Fatal("reopened spinner did not acquire")
	}
	if wake := d.AtomicEndMode(addr, 8, late, &lateToken, 0x51b6, true, true); wake != nil {
		t.Fatalf("last cohort completion returned wake %p", wake)
	}

	state.rmwQueue.lock()
	invalid := state.rmwOwner || state.rmwGranted || state.rmwWaiters != 0 || state.rmwSpinnerTID != 0 ||
		state.rmwSpinnerMisses != 0 || state.rmwCohortOps != 0 || state.rmwForceSpinner ||
		state.rmwPromoteParked || state.rmwPromotedOwner || state.rmwWakeCompetitors != 0 ||
		state.rmwGrantTID != 0 || state.rmwSpinnerEpoch != 0
	state.rmwQueue.unlock()
	if invalid || state.mu.state.Load() != 0 {
		t.Fatal("bounded parked promotion retained lifecycle state")
	}
}

func valueOrZero(value *uint32) uint32 {
	if value == nil {
		return 0
	}
	return *value
}

func TestAtomicRMWSpinnerSurvivesStolenNonPublicExactOwner(t *testing.T) {
	d := NewDetector()
	internal := goroutine.Alloc(2100)
	owner := goroutine.Alloc(2101)
	spinner := goroutine.Alloc(2102)
	defer DeactivateAtomicLoadCache(internal)
	defer internal.C.Release()
	defer owner.C.Release()
	defer spinner.C.Release()
	pc := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	const addr = uintptr(0x31268)
	if fast, direct := completeInternalRMWForTest(d, addr, 4, internal, pc, false, true); !fast || direct {
		t.Fatalf("internal enrollment = fast %v direct %v", fast, direct)
	}
	if fast, direct := completeInternalRMWForTest(d, addr, 4, internal, pc, false, true); !fast || !direct {
		t.Fatalf("internal retained seed = fast %v direct %v", fast, direct)
	}

	var ownerToken, spinnerToken AtomicToken
	if retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 4, owner, true, &ownerToken); retry {
		t.Fatal("public owner retried")
	}
	retry, spin, _, doorbell := d.AtomicBeginRMWCooperative(addr, 4, spinner, true, &spinnerToken)
	if !retry || !spin || doorbell == nil || *doorbell != 0 {
		t.Fatalf("spinner enrollment = retry %v spin %v doorbell %p/%d", retry, spin, doorbell, valueOrZero(doorbell))
	}
	d.AtomicEndMode(addr, 4, owner, &ownerToken, 0x5170, true, true)

	var internalToken AtomicToken
	retry, direct, _, _, _ := d.AtomicBeginInternalRMWCooperative(addr, 4, internal, pc, false, &internalToken)
	if retry || !direct {
		t.Fatalf("non-public steal = retry %v direct %v", retry, direct)
	}
	state := atomicHistoryForTest(t, d, addr)
	state.rmwQueue.lock()
	publicOwner := state.rmwOwner
	state.rmwQueue.unlock()
	if publicOwner {
		t.Fatal("direct internal thief was recorded as a public owner")
	}
	if retry, spin, _, park := d.AtomicResumeRMW(addr, 4, spinner, true, &spinnerToken); !retry || !spin || park != doorbell || *doorbell != 0 {
		t.Fatalf("stolen resume = retry %v spin %v park %p doorbell %d", retry, spin, park, *doorbell)
	}
	if wake := d.AtomicEndInternalRMW(addr, 4, internal, &internalToken, pc, true, false, direct); wake != nil {
		t.Fatalf("non-public owner returned spinner wake %p", wake)
	}
	if *doorbell != 1 {
		t.Fatalf("non-public owner completion doorbell = %d, want 1", *doorbell)
	}
	if retry, _, _, _ := d.AtomicResumeRMW(addr, 4, spinner, true, &spinnerToken); retry {
		t.Fatal("spinner did not acquire after non-public owner completion")
	}
	d.AtomicEndMode(addr, 4, spinner, &spinnerToken, 0x5171, true, true)
}

func TestAtomicLoadStoreCooperativeContentionIsClean(t *testing.T) {
	tests := []struct {
		name  string
		begin func(*Detector, uintptr, *goroutine.RaceContext, *AtomicToken) bool
		end   func(*Detector, uintptr, *goroutine.RaceContext, *AtomicToken)
	}{
		{
			name: "load",
			begin: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext, token *AtomicToken) bool {
				return d.AtomicBeginLoadCooperative(addr, 8, ctx, token)
			},
			end: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext, token *AtomicToken) {
				d.AtomicEndLoad(addr, 8, ctx, token, 0x5132)
			},
		},
		{
			name: "store",
			begin: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext, token *AtomicToken) bool {
				return d.AtomicBeginStoreCooperative(addr, 8, ctx, token)
			},
			end: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext, token *AtomicToken) {
				d.AtomicEnd(addr, 8, ctx, token, 0x5133, true)
			},
		},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := NewDetector()
			seed := goroutine.Alloc(uint32(208 + i*2))
			contender := goroutine.Alloc(uint32(209 + i*2))
			defer seed.C.Release()
			defer contender.C.Release()
			addr := uintptr(0x31260 + i*0x10)
			enrollPlainAtomicForTest(t, d, addr, 8, seed)
			capability := plainAtomicCapabilityForTest(t, d, addr, 8)
			state := atomicHistoryForTest(t, d, addr)

			state.mu.lock()
			beforeRevision := state.writerRevision.Load()
			beforePinned := state.arena.pinned.Load()
			beforeReads := atomicHistoryCardinality(state.reads)
			beforeWrites := atomicHistoryCardinality(state.writes)
			beforeEpoch := contender.GetEpoch()
			beforeSeedClock := contender.C.Get(seed.TID)
			var token AtomicToken
			token[0] = unsafe.Pointer(new(byte))
			retry := test.begin(d, addr, contender, &token)
			if !retry {
				state.mu.unlock()
				t.Fatal("cooperative contention did not request a retry")
			}
			for lane, retained := range token {
				if retained != nil {
					state.mu.unlock()
					t.Fatalf("contention retained token[%d] = %p", lane, retained)
				}
			}
			if state.transactionActive || state.writerRevision.Load() != beforeRevision || state.arena.pinned.Load() != beforePinned ||
				atomicHistoryCardinality(state.reads) != beforeReads || atomicHistoryCardinality(state.writes) != beforeWrites {
				state.mu.unlock()
				t.Fatal("contention mutated transaction, revision, pins, or atomic history")
			}
			state.mu.unlock()
			if got := contender.GetEpoch(); got != beforeEpoch {
				t.Fatalf("contention changed epoch from %v to %v", beforeEpoch, got)
			}
			if got := contender.C.Get(seed.TID); got != beforeSeedClock {
				t.Fatalf("contention imported release clock %d, want %d", got, beforeSeedClock)
			}
			if fresh := plainAtomicCapabilityForTest(t, d, addr, 8); fresh != capability {
				t.Fatalf("contention replaced capability %p with %p", capability, fresh)
			}

			if retry := test.begin(d, addr, contender, &token); retry {
				t.Fatal("uncontended cooperative begin requested a retry")
			}
			if fast := atomicFastToken(&token); fast != capability {
				t.Fatalf("uncontended token capability = %p, want %p", fast, capability)
			}
			test.end(d, addr, contender, &token)
		})
	}
}

func TestAtomicAcquireOnlyFastCompletionKeepsClockAndWeakensReadCache(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(209)
	defer seed.C.Release()
	const addr = uintptr(0x31280)
	enrollPlainAtomicForTest(t, d, addr, 8, seed)

	tests := []struct {
		name  string
		begin func(*goroutine.RaceContext, *AtomicToken)
	}{
		{
			name: "load",
			begin: func(ctx *goroutine.RaceContext, token *AtomicToken) {
				d.AtomicBeginPlain(addr, 8, ctx, true, true, token)
			},
		},
		{
			name: "failed CAS",
			begin: func(ctx *goroutine.RaceContext, token *AtomicToken) {
				d.AtomicBeginRMW(addr, 8, ctx, true, token)
			},
		},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := goroutine.Alloc(uint32(210 + i))
			defer ctx.C.Release()
			cacheAddr := uintptr(0x9200 + i*8)
			ctx.RecordReadSized(cacheAddr, 4, unsafe.Pointer(new(byte)))
			before := ctx.GetEpoch()
			beforeClock := ctx.C.Get(ctx.TID)
			ctx.InvalidateReadCacheAt(before)

			var token AtomicToken
			test.begin(ctx, &token)
			if atomicFastToken(&token) == nil {
				t.Fatal("acquire-only operation did not reuse the enrolled fast token")
			}
			d.AtomicEnd(addr, 8, ctx, &token, 0x5130+uintptr(i), false)

			if got := ctx.GetEpoch(); got != before {
				t.Fatalf("acquire-only completion advanced epoch from %v to %v", before, got)
			}
			if got := ctx.C.Get(ctx.TID); got != beforeClock {
				t.Fatalf("acquire-only completion advanced own clock to %d, want %d", got, beforeClock)
			}
			if !ctx.HasWeakReadHintSized(cacheAddr, 4) {
				t.Fatal("acquire-only completion did not weaken the ordinary read cache")
			}
			_, clock := before.Decode()
			if got := ctx.ReadCacheInvalidatedClock.Load(); got != uint32(clock) {
				t.Fatalf("acquire-only completion changed external invalidation marker to %d, want %d", got, clock)
			}
		})
	}
}

func TestAtomicRMWMaskMismatchFallsBackAndEscapes(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(206)
	const addr = uintptr(0x31300)
	enrollPlainAtomicForTest(t, d, addr, 8, ctx)
	old := plainAtomicCapabilityForTest(t, d, addr, 8)

	var token AtomicToken
	d.AtomicBeginRMW(addr, 4, ctx, true, &token)
	if atomicFastToken(&token) != nil || token[0] == nil || token[1] == nil {
		t.Fatalf("mismatched RMW token = [%p %p], want fully locked transaction", token[0], token[1])
	}
	d.AtomicEnd(addr, 4, ctx, &token, 0x5130, false)
	if slot := d.slotMemory.GetSlot(addr); slot != nil {
		if fast := slot.TryAtomicFast(0xff); fast != nil {
			fast.Release()
			t.Fatalf("mismatched RMW left escaped capability %p usable (old %p)", fast, old)
		}
	}
}

func TestAtomicRMWFastCompletionPreservesOrdinaryWritePoison(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(207)
	plainWriter := goroutine.Alloc(208)
	rearm := goroutine.Alloc(209)
	writer := goroutine.Alloc(210)
	failed := goroutine.Alloc(214)
	const addr = uintptr(0x31400)
	enrollPlainAtomicForTest(t, d, addr, 8, seed)

	// An ordinary pre-store hook closes the old generation and poisons its lanes.
	// Compatible atomic reads can later re-enroll without discarding that poison.
	d.OnWriteSized(addr, 8, plainWriter, 0x5140)
	rearmPlainAtomicForTest(t, d, addr, 8, rearm, 0x5141)
	capability := plainAtomicCapabilityForTest(t, d, addr, 8)
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	poison, hasPoison := atomicHistoryAccess(state.plainWrites, plainWriter.TID)
	state.mu.unlock()
	if !hasPoison || poison.clocks[0] == 0 {
		t.Fatalf("re-enrollment lost ordinary write poison: %+v, present=%v", poison, hasPoison)
	}

	if !completeRMWAtomicForTest(d, addr, 8, writer, true, 0x5143) {
		t.Fatal("successful exact RMW did not reuse poisoned capability")
	}
	state.mu.lock()
	poison, hasPoison = atomicHistoryAccess(state.plainWrites, plainWriter.TID)
	for lane, release := range state.releases {
		if release != nil {
			state.mu.unlock()
			t.Fatalf("poisoned RMW published lane %d release %p", lane, release)
		}
	}
	state.mu.unlock()
	if !hasPoison || poison.clocks[0] == 0 {
		t.Fatalf("successful RMW discarded concurrent poison: %+v, present=%v", poison, hasPoison)
	}

	if !completeRMWAtomicForTest(d, addr, 8, failed, false, 0x5144) {
		t.Fatal("failed exact CAS did not reuse poisoned capability")
	}
	state.mu.lock()
	_, hasPoison = atomicHistoryAccess(state.plainWrites, plainWriter.TID)
	state.mu.unlock()
	if !hasPoison {
		t.Fatal("failed CAS discarded ordinary write poison")
	}
	if fresh := plainAtomicCapabilityForTest(t, d, addr, 8); fresh != capability {
		t.Fatalf("poisoned RMW replaced capability %p with %p", capability, fresh)
	}
}

func TestAtomicRMWFastClearDrainsRetainedTransaction(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(215)
	const addr = uintptr(0x31500)
	if !completeRMWAtomicForTest(d, addr, 8, ctx, true, 0x514f) {
		t.Fatal("RMW-only seed did not enroll a capability")
	}

	var token AtomicToken
	d.AtomicBeginRMW(addr, 8, ctx, true, &token)
	if atomicFastToken(&token) == nil {
		t.Fatal("RMW did not retain enrolled capability")
	}
	slot := d.slotMemory.GetSlot(addr)
	done := make(chan struct{})
	go func() {
		d.ClearShadowRange(addr, 8)
		close(done)
	}()
	waitForAtomicFastEscapeForTest(t, slot, 0xff, done)

	d.AtomicEnd(addr, 8, ctx, &token, 0x5150, true)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clear did not finish after RMW AtomicEnd")
	}
	if got := d.ShadowGet(addr); got != nil {
		t.Fatalf("clear retained RMW generation %p", got)
	}
}

func TestAtomicRMWFastResetDrainsRetainedTransaction(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(216)
	const addr = uintptr(0x31600)
	if !completeRMWAtomicForTest(d, addr, 8, ctx, true, 0x515f) {
		t.Fatal("RMW-only seed did not enroll a capability")
	}

	var token AtomicToken
	d.AtomicBeginRMW(addr, 8, ctx, true, &token)
	fast := atomicFastToken(&token)
	if fast == nil {
		t.Fatal("RMW did not retain enrolled capability")
	}
	ordinary := fast.State()
	oldLifecycle := ordinary.GetLifecycleID()
	slot := d.slotMemory.GetSlot(addr)
	done := make(chan struct{})
	go func() {
		ordinary.Reset()
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		probe := slot.TryAtomicFast(0xff)
		if probe == nil {
			break
		}
		probe.Release()
		if time.Now().After(deadline) {
			t.Fatal("Reset did not close the retained RMW capability")
		}
		runtime.Gosched()
	}
	select {
	case <-done:
		t.Fatal("Reset completed before RMW AtomicEnd")
	default:
	}

	d.AtomicEnd(addr, 8, ctx, &token, 0x5160, true)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Reset did not finish after RMW AtomicEnd")
	}
	if ordinary.GetLifecycleID() == oldLifecycle || ordinary.GetAtomicState() != nil {
		t.Fatalf("Reset retained RMW lifecycle %d or overlay %p", ordinary.GetLifecycleID(), ordinary.GetAtomicState())
	}
}

type atomicSemanticSnapshot struct {
	writesUser     map[uint32]atomicAccess
	writesInternal map[uint32]atomicAccess
	readsUser      map[uint32]atomicAccess
	readsInternal  map[uint32]atomicAccess
	releases       [AtomicTokenSlots]atomicReleaseSnapshot
}

type atomicReleaseSnapshot struct {
	present bool
	runs    []vectorclock.FiniteRange
	retired []vectorclock.RetiredRange
}

func cloneAtomicFrontier(frontier *atomicHistoryClass) map[uint32]atomicAccess {
	if frontier == nil {
		return nil
	}
	clone := make(map[uint32]atomicAccess)
	frontier.visit(func(entry *atomicHistoryEntry) bool { clone[entry.tid] = entry.access; return true })
	return clone
}

func snapshotAtomicSemantics(t *testing.T, d *Detector, addr uintptr) atomicSemanticSnapshot {
	t.Helper()
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	defer state.mu.unlock()
	snapshot := atomicSemanticSnapshot{
		writesUser:     cloneAtomicFrontier(&state.writes.user),
		writesInternal: cloneAtomicFrontier(&state.writes.internal),
		readsUser:      cloneAtomicFrontier(&state.reads.user),
		readsInternal:  cloneAtomicFrontier(&state.reads.internal),
	}
	for lane, release := range state.releases {
		if release == nil {
			continue
		}
		snapshot.releases[lane] = atomicReleaseSnapshot{
			present: true,
			runs:    atomicReleaseRunsForTest(release),
			retired: atomicReleaseRetiredForTest(release),
		}
	}
	return snapshot
}

func snapshotContext(ctx *goroutine.RaceContext) (runs []vectorclock.FiniteRange, retired []vectorclock.RetiredRange) {
	ctx.C.RangeRuns(func(first, last, clock uint32) bool {
		runs = append(runs, vectorclock.FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	ctx.C.RangeRetired(func(first, last uint32) bool {
		retired = append(retired, vectorclock.RetiredRange{First: first, Last: last})
		return true
	})
	return runs, retired
}

func runAtomicSemanticSequence(t *testing.T, plain bool) (atomicSemanticSnapshot, [3][]vectorclock.FiniteRange, [3][]vectorclock.RetiredRange) {
	t.Helper()
	d := NewDetector()
	contexts := [3]*goroutine.RaceContext{
		goroutine.Alloc(211),
		goroutine.Alloc(212),
		goroutine.Alloc(213),
	}
	contexts[0].C.Set(900, 7)
	contexts[2].C.Set(901, 11)
	const addr = uintptr(0x32000)
	type operation struct {
		ctx     int
		acquire bool
		write   bool
		pc      uintptr
	}
	operations := []operation{
		{ctx: 0, write: true, pc: 0x5200},
		{ctx: 1, acquire: true, pc: 0x5201},
		{ctx: 2, write: true, pc: 0x5202},
		{ctx: 1, acquire: true, pc: 0x5203},
	}
	for i, operation := range operations {
		var token AtomicToken
		if plain {
			d.AtomicBeginPlain(addr, 8, contexts[operation.ctx], operation.acquire, true, &token)
			if gotFast := atomicFastToken(&token) != nil; !gotFast {
				t.Fatalf("plain operation %d did not convert/use fast enrollment", i)
			}
		} else {
			d.AtomicBegin(addr, 8, contexts[operation.ctx], operation.acquire, &token)
			if atomicFastToken(&token) != nil {
				t.Fatalf("forced-slow operation %d produced a fast token", i)
			}
		}
		d.AtomicEnd(addr, 8, contexts[operation.ctx], &token, operation.pc, operation.write)
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("atomic-only sequence reported %d races", got)
	}

	var runs [3][]vectorclock.FiniteRange
	var retired [3][]vectorclock.RetiredRange
	for i, ctx := range contexts {
		runs[i], retired[i] = snapshotContext(ctx)
	}
	return snapshotAtomicSemantics(t, d, addr), runs, retired
}

func TestPlainAtomicFastMatchesForcedSlowSemantics(t *testing.T) {
	fastState, fastRuns, fastRetired := runAtomicSemanticSequence(t, true)
	slowState, slowRuns, slowRetired := runAtomicSemanticSequence(t, false)
	if !reflect.DeepEqual(fastState, slowState) {
		t.Fatalf("fast atomic state differs from forced slow:\nfast=%#v\nslow=%#v", fastState, slowState)
	}
	if !reflect.DeepEqual(fastRuns, slowRuns) || !reflect.DeepEqual(fastRetired, slowRetired) {
		t.Fatalf("fast contexts differ from forced slow:\nfast=%v retired=%v\nslow=%v retired=%v", fastRuns, fastRetired, slowRuns, slowRetired)
	}
}

func TestPlainAtomicFastRetainsConcurrentAntichain(t *testing.T) {
	d := NewDetector()
	root := goroutine.Alloc(221)
	left := goroutine.Alloc(222)
	right := goroutine.Alloc(223)
	const addr = uintptr(0x33000)
	if !completePlainAtomicForTest(d, addr, 8, root, false, true, 0x5300) {
		t.Fatal("root store did not convert fast-path enrollment")
	}
	left.C.Set(root.TID, 1)
	right.C.Set(root.TID, 1)
	if !completePlainAtomicForTest(d, addr, 8, left, false, true, 0x5301) ||
		!completePlainAtomicForTest(d, addr, 8, right, false, true, 0x5302) {
		t.Fatal("enrolled concurrent store did not use fast path")
	}
	state := atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.writes); got != 2 {
		t.Fatalf("fast concurrent-store frontier cardinality = %d, want 2", got)
	}
	for _, ctx := range []*goroutine.RaceContext{left, right} {
		if _, ok := atomicHistoryAccess(state.writes, ctx.TID); !ok {
			t.Fatalf("fast frontier lost concurrent TID %d", ctx.TID)
		}
	}
}

func TestPlainAtomicFastConcurrentEnrollmentSurvivesStaleProbes(t *testing.T) {
	d := NewDetector()
	const (
		addr    = uintptr(0x33800)
		workers = 8
	)
	start := make(chan struct{})
	usedFast := make(chan bool, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ctx := goroutine.Alloc(uint32(260 + worker))
			<-start
			usedFast <- completePlainAtomicForTest(d, addr, 8, ctx, false, true, 0x5380+uintptr(worker))
		}(worker)
	}
	close(start)
	wg.Wait()
	close(usedFast)
	fastCount := 0
	for fast := range usedFast {
		if fast {
			fastCount++
		}
	}
	if fastCount == 0 {
		t.Fatal("simultaneous compatible operations never converted or observed enrollment")
	}
	probe := goroutine.Alloc(280)
	if !completePlainAtomicForTest(d, addr, 8, probe, true, false, 0x5390) {
		t.Fatal("simultaneous compatible setup permanently closed capability")
	}
	state := atomicHistoryForTest(t, d, addr)
	if got := atomicHistoryCardinality(state.writes); got != workers {
		t.Fatalf("concurrent enrollment frontier cardinality = %d, want %d", got, workers)
	}
}

func TestPlainAtomicFastRearmsFreshAfterIncompatibleAccess(t *testing.T) {
	tests := []struct {
		name   string
		escape func(d *Detector, addr uintptr, ctx *goroutine.RaceContext)
	}{
		{
			name: "ordinary read",
			escape: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) {
				d.OnRead(addr, ctx, 0x5400)
			},
		},
		{
			name: "general atomic",
			escape: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) {
				var token AtomicToken
				d.AtomicBegin(addr, 8, ctx, true, &token)
				d.AtomicEnd(addr, 8, ctx, &token, 0x5401, false)
			},
		},
		{
			name: "mixed width",
			escape: func(d *Detector, addr uintptr, ctx *goroutine.RaceContext) {
				var token AtomicToken
				d.AtomicBeginPlain(addr, 4, ctx, true, true, &token)
				d.AtomicEnd(addr, 4, ctx, &token, 0x5402, false)
			},
		},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := NewDetector()
			ctx := goroutine.Alloc(uint32(230 + i))
			addr := uintptr(0x34000 + i*0x100)
			enrollPlainAtomicForTest(t, d, addr, 8, ctx)
			old := plainAtomicCapabilityForTest(t, d, addr, 8)
			oldState, oldOverlay := old.State(), old.Overlay()
			oldMask, oldOrdinaryMask := old.Mask(), old.OrdinaryMask()
			test.escape(d, addr, ctx)
			rearmPlainAtomicForTest(t, d, addr, 8, ctx, 0x5403)
			fresh := plainAtomicCapabilityForTest(t, d, addr, 8)
			if fresh == old {
				t.Fatal("compatible setup reopened the escaped capability generation")
			}
			if old.State() != oldState || old.Overlay() != oldOverlay || old.Mask() != oldMask || old.OrdinaryMask() != oldOrdinaryMask {
				t.Fatal("compatible setup repurposed the escaped capability descriptor")
			}
		})
	}
}

func TestPlainAtomicFastRejectsUnalignedAndCrossWordShapes(t *testing.T) {
	tests := []struct {
		name string
		addr uintptr
		size uintptr
	}{
		{name: "unaligned32", addr: 0x34801, size: 4},
		{name: "crossWord64", addr: 0x34904, size: 8},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := NewDetector()
			ctx := goroutine.Alloc(uint32(290 + i))
			for operation := 0; operation < 2; operation++ {
				if completePlainAtomicForTest(d, test.addr, test.size, ctx, true, false, 0x5480+uintptr(operation)) {
					t.Fatalf("operation %d used fast path for unsupported shape", operation)
				}
			}
		})
	}
}

func TestPlainAtomicFastMixedWidthUsesFreshGenerations(t *testing.T) {
	tests := []struct {
		name         string
		enrollOffset uintptr
		enrollSize   uintptr
		otherOffset  uintptr
		otherSize    uintptr
	}{
		{name: "wide then narrow lower", enrollSize: 8, otherSize: 4},
		{name: "wide then narrow upper", enrollSize: 8, otherOffset: 4, otherSize: 4},
		{name: "narrow lower then wide", enrollSize: 4, otherSize: 8},
		{name: "narrow upper then wide", enrollOffset: 4, enrollSize: 4, otherSize: 8},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := NewDetector()
			ctx := goroutine.Alloc(uint32(300 + i))
			base := uintptr(0x34a00 + i*0x100)
			enrollAddr := base + test.enrollOffset
			otherAddr := base + test.otherOffset
			// Keep one initialized ordinary group in the mask while widths split
			// and reconverge; probation must not depend on the all-empty shape.
			d.OnWrite(enrollAddr, ctx, 0x5490+uintptr(i))
			enrollPlainAtomicForTest(t, d, enrollAddr, test.enrollSize, ctx)
			generations := []*shadowmem.AtomicFastPath{
				plainAtomicCapabilityForTest(t, d, enrollAddr, test.enrollSize),
			}

			rearmPlainAtomicForTest(t, d, otherAddr, test.otherSize, ctx, 0x54a0)
			generations = append(generations, plainAtomicCapabilityForTest(t, d, otherAddr, test.otherSize))
			rearmPlainAtomicForTest(t, d, enrollAddr, test.enrollSize, ctx, 0x54a2)
			generations = append(generations, plainAtomicCapabilityForTest(t, d, enrollAddr, test.enrollSize))
			rearmPlainAtomicForTest(t, d, otherAddr, test.otherSize, ctx, 0x54a4)
			generations = append(generations, plainAtomicCapabilityForTest(t, d, otherAddr, test.otherSize))
			for current := 1; current < len(generations); current++ {
				for old := 0; old < current; old++ {
					if generations[current] == generations[old] {
						t.Fatalf("width transition %d reused capability generation %d (%p)", current, old, generations[current])
					}
				}
			}
			if got := d.RacesDetected(); got != 0 {
				t.Fatalf("same-context width transitions reported %d races", got)
			}
		})
	}
}

func TestPlainAtomicFastIgnoredModeEscapesBeforeRetain(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(310)
	const addr = uintptr(0x34c00)
	enrollPlainAtomicForTest(t, d, addr, 8, ctx)
	old := plainAtomicCapabilityForTest(t, d, addr, 8)

	before := ctx.GetEpoch()
	var token AtomicToken
	d.AtomicBeginPlain(addr, 8, ctx, false, false, &token)
	if atomicFastToken(&token) != nil || token[0] == nil || token[1] == nil {
		t.Fatalf("ignored plain begin token = [%p %p], want fully locked slow transaction", token[0], token[1])
	}
	d.AtomicEndMode(addr, 8, ctx, &token, 0x54c0, true, false)
	if got := ctx.GetEpoch(); got != before {
		t.Fatalf("ignored store advanced epoch: got %v, want %v", got, before)
	}
	rearmPlainAtomicForTest(t, d, addr, 8, ctx, 0x54c1)
	if fresh := plainAtomicCapabilityForTest(t, d, addr, 8); fresh == old {
		t.Fatal("enabled operation reopened capability escaped by ignored begin")
	}
}

func TestPlainAtomicFastMarkerHistoryRearmsWithClassification(t *testing.T) {
	d := NewDetector()
	plain := goroutine.Alloc(241)
	atomicCtx := goroutine.Alloc(242)
	const addr = uintptr(0x35000)
	markerPC := reflect.ValueOf((*sync.RWMutex).RLock).Pointer() + 1
	d.OnRead(addr, plain, markerPC)
	atomicCtx.C.Set(plain.TID, 1)

	if !completePlainAtomicForTest(d, addr, 8, atomicCtx, false, true, 0x5500) ||
		!completePlainAtomicForTest(d, addr, 8, atomicCtx, false, true, 0x5501) {
		t.Fatal("ordinary marker history did not enroll the classified fast path")
	}
	state := atomicHistoryForTest(t, d, addr)
	if atomicHistoryCardinality(state.plainReads) == 0 {
		t.Fatal("test setup did not retain exact plain-reader classification")
	}
}

func TestPlainAtomicFastClearDrainsAndRejectsABA(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(251)
	const addr = uintptr(0x36000)
	enrollPlainAtomicForTest(t, d, addr, 8, ctx)
	oldState := d.ShadowGet(addr)

	var token AtomicToken
	beforeEnd := ctx.GetEpoch()
	const cacheAddr = addr + 8
	ctx.RecordReadSized(cacheAddr, 4, unsafe.Pointer(new(byte)))
	d.AtomicBeginPlain(addr, 8, ctx, true, true, &token)
	oldFast := atomicFastToken(&token)
	if oldFast == nil {
		t.Fatal("held transaction did not use fast path")
	}
	slot := d.slotMemory.GetSlot(addr)
	if slot == nil {
		t.Fatal("held transaction has no shadow slot")
	}
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		d.ClearShadowRange(addr, 8)
		close(done)
	}()
	<-started
	// Observe the close side of the capability gate rather than assuming the
	// clear goroutine ran within a fixed scheduling window. Until clear closes
	// the gate, a probe can retain and immediately release this exact binding;
	// once it cannot, clear must still be waiting for token's retained user.
	escapeDeadline := time.After(time.Second)
	for {
		probe := slot.TryAtomicFast(0xff)
		if probe == nil {
			break
		}
		probe.Release()
		select {
		case <-escapeDeadline:
			t.Fatal("clear did not close the retained capability")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-done:
		t.Fatal("clear completed while fast transaction retained old generation")
	default:
	}
	d.AtomicEnd(addr, 8, ctx, &token, 0x5600, false)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clear did not complete after fast transaction released")
	}
	beforeTID, beforeClock := beforeEnd.Decode()
	afterTID, afterClock := ctx.GetEpoch().Decode()
	if afterTID != beforeTID || afterClock != beforeClock {
		t.Fatalf("acquire-only transaction advanced clock: got (%d,%d), want (%d,%d)", afterTID, afterClock, beforeTID, beforeClock)
	}
	if !ctx.HasWeakReadHintSized(cacheAddr, 4) {
		t.Fatal("clear returned before retained transaction cache bookkeeping")
	}

	var newToken AtomicToken
	d.AtomicBeginPlain(addr, 8, ctx, false, true, &newToken)
	newFast := atomicFastToken(&newToken)
	if newFast == nil || newFast == oldFast {
		t.Fatalf("cleared lifecycle capability = %p, old = %p", newFast, oldFast)
	}
	d.AtomicEnd(addr, 8, ctx, &newToken, 0x5601, true)
	if newState := d.ShadowGet(addr); newState == nil || newState == oldState {
		t.Fatalf("cleared lifecycle state = %p, old = %p", newState, oldState)
	}
}

func TestSlowAtomicClearDrainsThroughEndBookkeeping(t *testing.T) {
	tests := []struct {
		name        string
		synchronize bool
	}{
		{name: "general", synchronize: true},
		{name: "ignored plain", synchronize: false},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := NewDetector()
			base := uintptr(0x37000 + i*0x100)
			ctx := goroutine.Alloc(uint32(320 + i*2))
			sentinelCtx := goroutine.Alloc(uint32(321 + i*2))
			enrollPlainAtomicForTest(t, d, base+4, 4, sentinelCtx)
			slot := d.slotMemory.GetSlot(base)
			if slot == nil {
				t.Fatal("sentinel enrollment did not materialize its shadow slot")
			}
			probe := slot.TryAtomicFast(0xf0)
			if probe == nil {
				t.Fatal("sentinel capability was not live before slow begin")
			}
			probe.Release()

			before := ctx.GetEpoch()
			var token AtomicToken
			if test.synchronize {
				d.AtomicBegin(base, 4, ctx, false, &token)
			} else {
				d.AtomicBeginPlain(base, 4, ctx, false, false, &token)
			}
			active := true
			defer func() {
				if active {
					d.AtomicEndMode(base, 4, ctx, &token, 0x5700, true, test.synchronize)
				}
			}()
			if atomicFastToken(&token) != nil || token[0] == nil || token[1] == nil || token[4] != nil {
				t.Fatalf("slow begin token = [%p %p %p], want repeated target state and no fast capability", token[0], token[1], token[4])
			}
			oldState := (*shadowmem.VarState)(token[0])
			if got := d.ShadowGet(base); got != oldState {
				t.Fatalf("retained target mapping = %p, want %p", got, oldState)
			}
			probe = slot.TryAtomicFast(0xf0)
			if probe == nil {
				t.Fatal("slow begin closed the disjoint progress sentinel")
			}
			probe.Release()
			// Both normal synchronization and an ignored write must finish their
			// cache bookkeeping before releasing the generation ClearRange drains.
			ctx.RecordRead(base, token[0])

			done := make(chan struct{})
			go func() {
				d.ClearShadowRange(base, 8)
				close(done)
			}()
			waitForAtomicFastEscapeForTest(t, slot, 0xf0, done)
			if got := d.ShadowGet(base); got != oldState {
				t.Fatalf("clear detached retained slow generation: got %p, want %p", got, oldState)
			}

			d.AtomicEndMode(base, 4, ctx, &token, 0x5701, true, test.synchronize)
			active = false
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("clear did not finish after slow AtomicEndMode")
			}
			if token != (AtomicToken{}) {
				t.Fatalf("AtomicEndMode retained token entries: %v", token)
			}
			beforeTID, beforeClock := before.Decode()
			afterTID, afterClock := ctx.GetEpoch().Decode()
			wantClock := beforeClock
			if test.synchronize {
				wantClock++
			}
			if afterTID != beforeTID || afterClock != wantClock {
				t.Fatalf("post-End epoch = (%d,%d), want (%d,%d)", afterTID, afterClock, beforeTID, wantClock)
			}
			cacheSlot := goroutine.ReadCacheIndex(base)
			if ctx.ReadCache[cacheSlot] != 0 || ctx.ReadCacheStates[cacheSlot] != nil {
				t.Fatalf("post-End cache entry = (%#x,%p), want invalid", ctx.ReadCache[cacheSlot], ctx.ReadCacheStates[cacheSlot])
			}
			if got := d.ShadowGet(base); got != nil {
				t.Fatalf("clear retained target generation %p", got)
			}

			var next AtomicToken
			d.AtomicBegin(base, 4, ctx, false, &next)
			fresh := (*shadowmem.VarState)(next[0])
			if fresh == nil || fresh == oldState {
				t.Fatalf("reused lifecycle state = %p, old = %p", fresh, oldState)
			}
			d.AtomicEnd(base, 4, ctx, &next, 0x5702, true)
		})
	}
}

func TestSlowAtomicClearDrainsUnalignedCrossWordToken(t *testing.T) {
	d := NewDetector()
	const (
		base = uintptr(0x37200)
		addr = base + 4
	)
	ctx := goroutine.Alloc(330)
	sentinelCtx := goroutine.Alloc(331)
	enrollPlainAtomicForTest(t, d, base, 4, sentinelCtx)
	firstSlot := d.slotMemory.GetSlot(base)
	if firstSlot == nil {
		t.Fatal("sentinel enrollment did not materialize the first word")
	}
	probe := firstSlot.TryAtomicFast(0x0f)
	if probe == nil {
		t.Fatal("sentinel capability was not live before spanning begin")
	}
	probe.Release()

	before := ctx.GetEpoch()
	var token AtomicToken
	d.AtomicBegin(addr, 8, ctx, false, &token)
	active := true
	defer func() {
		if active {
			d.AtomicEnd(addr, 8, ctx, &token, 0x5720, true)
		}
	}()
	if atomicFastToken(&token) != nil || token[0] == nil || token[4] == nil {
		t.Fatalf("cross-word begin token = [%p %p], want two retained slow groups", token[0], token[4])
	}
	oldFirst := (*shadowmem.VarState)(token[0])
	oldSecond := (*shadowmem.VarState)(token[4])
	if oldFirst == oldSecond {
		t.Fatal("cross-word token unexpectedly reused one state across two words")
	}
	probe = firstSlot.TryAtomicFast(0x0f)
	if probe == nil {
		t.Fatal("spanning begin closed the disjoint first-word progress sentinel")
	}
	probe.Release()
	ctx.RecordRead(addr, token[0])

	done := make(chan struct{})
	go func() {
		d.ClearShadowRange(base, 16)
		close(done)
	}()
	// The disjoint 0x0f capability is closed only after ClearRange owns the
	// first slot. It then blocks on the token's 0xf0 state, deterministically
	// proving that neither cross-word generation was detached early.
	waitForAtomicFastEscapeForTest(t, firstSlot, 0x0f, done)
	if got := d.ShadowGet(addr); got != oldFirst {
		t.Fatalf("clear detached first retained generation: got %p, want %p", got, oldFirst)
	}
	if got := d.ShadowGet(base + 8); got != oldSecond {
		t.Fatalf("clear detached second retained generation: got %p, want %p", got, oldSecond)
	}

	d.AtomicEnd(addr, 8, ctx, &token, 0x5721, true)
	active = false
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clear did not finish after spanning AtomicEnd")
	}
	if token != (AtomicToken{}) {
		t.Fatalf("spanning AtomicEnd retained token entries: %v", token)
	}
	beforeTID, beforeClock := before.Decode()
	afterTID, afterClock := ctx.GetEpoch().Decode()
	if afterTID != beforeTID || afterClock != beforeClock+1 {
		t.Fatalf("spanning post-End epoch = (%d,%d), want (%d,%d)", afterTID, afterClock, beforeTID, beforeClock+1)
	}
	cacheSlot := goroutine.ReadCacheIndex(addr)
	if ctx.ReadCache[cacheSlot] != 0 || ctx.ReadCacheStates[cacheSlot] != nil {
		t.Fatalf("spanning post-End cache entry = (%#x,%p), want invalid", ctx.ReadCache[cacheSlot], ctx.ReadCacheStates[cacheSlot])
	}
	if got := d.ShadowGet(addr); got != nil {
		t.Fatalf("clear retained first cross-word generation %p", got)
	}
	if got := d.ShadowGet(base + 8); got != nil {
		t.Fatalf("clear retained second cross-word generation %p", got)
	}

	var next AtomicToken
	d.AtomicBegin(addr, 8, ctx, false, &next)
	freshFirst := (*shadowmem.VarState)(next[0])
	freshSecond := (*shadowmem.VarState)(next[4])
	if freshFirst == nil || freshFirst == oldFirst || freshSecond == nil || freshSecond == oldSecond {
		t.Fatalf("cross-word reused cleared lifecycle: first %p/%p, second %p/%p", freshFirst, oldFirst, freshSecond, oldSecond)
	}
	d.AtomicEnd(addr, 8, ctx, &next, 0x5722, true)
}

func TestSlowAtomicClearDrainsUnalignedCrossWordSecondHalf(t *testing.T) {
	d := NewDetector()
	const (
		base = uintptr(0x37400)
		addr = base + 4
	)
	ctx := goroutine.Alloc(332)
	sentinelCtx := goroutine.Alloc(333)
	enrollPlainAtomicForTest(t, d, base+12, 4, sentinelCtx)
	secondSlot := d.slotMemory.GetSlot(base + 8)
	if secondSlot == nil {
		t.Fatal("sentinel enrollment did not materialize the second word")
	}

	var token AtomicToken
	d.AtomicBegin(addr, 8, ctx, false, &token)
	active := true
	defer func() {
		if active {
			d.AtomicEnd(addr, 8, ctx, &token, 0x5740, true)
		}
	}()
	if atomicFastToken(&token) != nil || token[0] == nil || token[4] == nil {
		t.Fatalf("cross-word begin token = [%p %p], want two retained slow groups", token[0], token[4])
	}
	oldFirst := (*shadowmem.VarState)(token[0])
	oldSecond := (*shadowmem.VarState)(token[4])
	probe := secondSlot.TryAtomicFast(0xf0)
	if probe == nil {
		t.Fatal("spanning begin closed the disjoint second-word progress sentinel")
	}
	probe.Release()

	done := make(chan struct{})
	go func() {
		d.ClearShadowRange(base+8, 8)
		close(done)
	}()
	waitForAtomicFastEscapeForTest(t, secondSlot, 0xf0, done)
	if got := d.ShadowGet(base + 8); got != oldSecond {
		t.Fatalf("clear detached retained second-word generation: got %p, want %p", got, oldSecond)
	}

	d.AtomicEnd(addr, 8, ctx, &token, 0x5741, true)
	active = false
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second-word clear did not finish after spanning AtomicEnd")
	}
	if got := d.ShadowGet(base + 8); got != nil {
		t.Fatalf("clear retained second-word generation %p", got)
	}
	if got := d.ShadowGet(addr); got != oldFirst {
		t.Fatalf("second-word clear changed adjacent first generation: got %p, want %p", got, oldFirst)
	}

	var next AtomicToken
	d.AtomicBegin(addr, 8, ctx, false, &next)
	freshSecond := (*shadowmem.VarState)(next[4])
	if freshSecond == nil || freshSecond == oldSecond {
		t.Fatalf("second word reused cleared lifecycle state = %p, old = %p", freshSecond, oldSecond)
	}
	if freshFirst := (*shadowmem.VarState)(next[0]); freshFirst != oldFirst {
		t.Fatalf("adjacent first-word lifecycle changed: got %p, want %p", freshFirst, oldFirst)
	}
	d.AtomicEnd(addr, 8, ctx, &next, 0x5742, true)
}

type initializedAtomicReportSnapshot struct {
	present      bool
	currentType  AccessType
	previousType AccessType
	addr         uintptr
	currentTID   uint32
	previousTID  uint32
	currentEpoch epoch.Epoch
	previous     epoch.Epoch
	currentPC    uintptr
	previousPC   uintptr
}

type initializedAtomicResult struct {
	usedFast bool
	races    int
	report   initializedAtomicReportSnapshot
	atomic   atomicSemanticSnapshot
	runs     []vectorclock.FiniteRange
	retired  []vectorclock.RetiredRange
}

func snapshotInitializedAtomicReport(report *RaceReport) initializedAtomicReportSnapshot {
	if report == nil {
		return initializedAtomicReportSnapshot{}
	}
	snapshot := initializedAtomicReportSnapshot{
		present:      true,
		currentType:  report.Current.Type,
		previousType: report.Previous.Type,
		addr:         report.Current.Addr,
		currentTID:   report.Current.GoroutineID,
		previousTID:  report.Previous.GoroutineID,
		currentEpoch: report.Current.Epoch,
		previous:     report.Previous.Epoch,
	}
	if len(report.Current.StackTrace) != 0 {
		snapshot.currentPC = report.Current.StackTrace[0]
	}
	if len(report.Previous.StackTrace) != 0 {
		snapshot.previousPC = report.Previous.StackTrace[0]
	}
	return snapshot
}

func runInitializedAtomicForTest(t *testing.T, addr, size uintptr, seedWrite, promoted, currentWrite, ordered, plain bool) initializedAtomicResult {
	t.Helper()
	d := NewDetector()
	seed := goroutine.Alloc(4001)
	var promotedReader *goroutine.RaceContext
	const (
		seedPC    = uintptr(0x7c01)
		currentPC = uintptr(0x7c02)
	)
	if seedWrite {
		d.OnWrite(addr, seed, seedPC)
	} else {
		d.OnRead(addr, seed, seedPC)
		if promoted {
			promotedReader = goroutine.Alloc(4003)
			d.OnRead(addr, promotedReader, seedPC+1)
		}
	}
	current := goroutine.Alloc(4002)
	if ordered {
		current = goroutine.AllocWithParentClock(4002, seed.C, 1)
	}
	var report *RaceReport
	d.reportObserver = func(observed *RaceReport) { report = observed }

	var token AtomicToken
	if plain {
		d.AtomicBeginPlain(addr, size, current, false, true, &token)
	} else {
		d.AtomicBegin(addr, size, current, false, &token)
	}
	active := token[0] != nil
	defer func() {
		if active {
			d.AtomicEnd(addr, size, current, &token, currentPC, currentWrite)
		}
	}()
	// A promoted VarState retains the exact reader epochs but only one aggregate
	// read PC. Populate the overlay sidecar while Begin owns it so both the fast
	// and forced-slow completion paths exercise exact reader attribution.
	if promoted {
		recordReaders := func(state *atomicState, ordinary *shadowmem.VarState, mask uint8) {
			recordAtomicAccess(&state.plainReads, seed, seedPC, mask, false)
			recordAtomicAccess(&state.plainReads, promotedReader, seedPC+1, mask, false)
			if previous, previousPC, exact := exactConcurrentPlainRead(state, ordinary, current, mask); !exact || previous != seed.GetEpoch() || previousPC != seedPC {
				t.Fatalf("exact promoted reader = %v/%#x/%v, want %v/%#x/true", previous, previousPC, exact, seed.GetEpoch(), seedPC)
			}
		}
		if fast := atomicFastToken(&token); fast != nil {
			recordReaders((*atomicState)(fast.Overlay()), fast.State(), fast.OrdinaryMask())
		} else {
			groups, groupCount := atomicTokenGroups(&token, addr, size)
			for i := 0; i < groupCount; i++ {
				if groups[i].state.GetReaderCount() != 0 {
					recordReaders(existingAtomicStateLocked(groups[i].state), groups[i].state, groups[i].mask)
				}
			}
		}
	}
	usedFast := atomicFastToken(&token) != nil
	if plain && !usedFast {
		t.Fatalf("initialized %d-byte plain operation did not convert enrollment", size)
	}
	if !plain && usedFast {
		t.Fatalf("forced-slow initialized %d-byte operation returned a fast token", size)
	}
	d.AtomicEnd(addr, size, current, &token, currentPC, currentWrite)
	active = false
	if token != (AtomicToken{}) {
		t.Fatalf("AtomicEnd retained initialized token entries: %v", token)
	}
	runs, retired := snapshotContext(current)
	return initializedAtomicResult{
		usedFast: usedFast,
		races:    d.RacesDetected(),
		report:   snapshotInitializedAtomicReport(report),
		atomic:   snapshotAtomicSemantics(t, d, addr),
		runs:     runs,
		retired:  retired,
	}
}

func TestPlainAtomicFastInitializedHistoryMatchesForcedSlow(t *testing.T) {
	tests := []struct {
		name         string
		offset       uintptr
		size         uintptr
		seedWrite    bool
		promoted     bool
		currentWrite bool
		ordered      bool
		wantRace     bool
	}{
		{name: "write-write-32", size: 4, seedWrite: true, currentWrite: true, wantRace: true},
		{name: "write-read-64", size: 8, seedWrite: true, wantRace: true},
		{name: "read-write-32", size: 4, currentWrite: true, wantRace: true},
		{name: "upper-half-write-write-32", offset: 4, size: 4, seedWrite: true, currentWrite: true, wantRace: true},
		{name: "promoted-read-write-64", size: 8, promoted: true, currentWrite: true, wantRace: true},
		{name: "ordered-write-write-64", size: 8, seedWrite: true, currentWrite: true, ordered: true},
		{name: "ordered-read-write-64", size: 8, currentWrite: true, ordered: true},
	}
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			addr := uintptr(0x38000+i*0x100) + test.offset
			fast := runInitializedAtomicForTest(t, addr, test.size, test.seedWrite, test.promoted, test.currentWrite, test.ordered, true)
			slow := runInitializedAtomicForTest(t, addr, test.size, test.seedWrite, test.promoted, test.currentWrite, test.ordered, false)
			if !fast.usedFast || slow.usedFast {
				t.Fatalf("path selection = fast %v slow %v", fast.usedFast, slow.usedFast)
			}
			fast.usedFast = false
			if !reflect.DeepEqual(fast, slow) {
				t.Fatalf("initialized fast result differs from forced slow:\nfast=%#v\nslow=%#v", fast, slow)
			}
			if got := fast.races != 0; got != test.wantRace {
				t.Fatalf("race result = %v (%d reports), want %v", got, fast.races, test.wantRace)
			}
			if test.wantRace {
				if !fast.report.present || fast.report.addr != addr || fast.report.currentPC != 0x7c02 || fast.report.previousPC != 0x7c01 {
					t.Fatalf("initialized race report = %#v, want exact address and PCs", fast.report)
				}
			}
		})
	}
}

func TestPlainAtomicFastResetWaitsThroughAtomicEnd(t *testing.T) {
	d := NewDetector()
	seed := goroutine.Alloc(4101)
	const (
		addr   = uintptr(0x38600)
		seedPC = uintptr(0x7d01)
	)
	d.OnWrite(addr, seed, seedPC)
	current := goroutine.AllocWithParentClock(4102, seed.C, 1)
	var token AtomicToken
	d.AtomicBeginPlain(addr, 8, current, false, true, &token)
	active := token[0] != nil
	defer func() {
		if active {
			d.AtomicEnd(addr, 8, current, &token, 0x7d02, true)
		}
	}()
	fast := atomicFastToken(&token)
	if fast == nil {
		t.Fatal("initialized transaction did not retain a fast capability")
	}
	state := fast.State()
	oldLifecycle := state.GetLifecycleID()
	slot := d.slotMemory.GetSlot(addr)
	if slot == nil {
		t.Fatal("initialized transaction did not materialize a slot")
	}

	done := make(chan struct{})
	go func() {
		state.Reset()
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		probe := slot.TryAtomicFast(0xff)
		if probe == nil {
			break
		}
		probe.Release()
		if time.Now().After(deadline) {
			t.Fatal("Reset did not close the retained capability")
		}
		runtime.Gosched()
	}
	select {
	case <-done:
		t.Fatal("Reset completed before AtomicEnd released the capability")
	default:
	}
	if state.GetW() != seed.GetEpoch() || state.GetWritePC() != seedPC || state.GetLifecycleID() != oldLifecycle {
		t.Fatalf("Reset changed retained history: W=%v PC=%#x lifecycle=%d, want %v/%#x/%d",
			state.GetW(), state.GetWritePC(), state.GetLifecycleID(), seed.GetEpoch(), seedPC, oldLifecycle)
	}

	d.AtomicEnd(addr, 8, current, &token, 0x7d02, true)
	active = false
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Reset did not finish after AtomicEnd")
	}
	if token != (AtomicToken{}) {
		t.Fatalf("AtomicEnd retained token entries: %v", token)
	}
	if state.GetW() != 0 || state.GetWritePC() != 0 || state.GetAtomicState() != nil || state.GetLifecycleID() == oldLifecycle {
		t.Fatalf("post-Reset state = W=%v PC=%#x atomic=%p lifecycle=%d (old %d)",
			state.GetW(), state.GetWritePC(), state.GetAtomicState(), state.GetLifecycleID(), oldLifecycle)
	}
	if probe := slot.TryAtomicFast(0xff); probe != nil {
		probe.Release()
		t.Fatal("old capability revalidated after Reset")
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("ordered initialized transaction reported %d races", got)
	}
}

func completeInternalRMWForTest(d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr, synchronize, write bool) (fast, direct bool) {
	var token AtomicToken
	retry, direct, _, _, _ := d.AtomicBeginInternalRMWCooperative(addr, size, ctx, pc, synchronize, &token)
	if retry {
		panic("unexpected uncontended internal RMW retry")
	}
	fast = atomicFastToken(&token) != nil
	d.AtomicEndInternalRMW(addr, size, ctx, &token, pc, write, synchronize, direct)
	return fast, direct
}

type countingSlotShadow struct {
	shadowmem.SlotShadow
	getSlotCalls int
}

func (s *countingSlotShadow) GetSlot(addr uintptr) *shadowmem.ShadowSlot {
	s.getSlotCalls++
	return s.SlotShadow.GetSlot(addr)
}

func beginPublicRMWForTest(d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, token *AtomicToken) {
	retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, size, ctx, true, token)
	if retry {
		panic("unexpected uncontended public RMW retry")
	}
}

func completePublicRMWForTest(d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr, write bool) *shadowmem.AtomicFastPath {
	var token AtomicToken
	beginPublicRMWForTest(d, addr, size, ctx, &token)
	fast := atomicFastToken(&token)
	d.AtomicEndInternalRMW(addr, size, ctx, &token, pc, write, true, false)
	return fast
}

type publicRMWTestHelper interface {
	Helper()
	Fatalf(string, ...any)
}

func completePublicDirectRMWForTest(t publicRMWTestHelper, d *Detector, addr, size uintptr, ctx *goroutine.RaceContext, pc uintptr, write bool) (*shadowmem.AtomicFastPath, bool) {
	t.Helper()
	var token AtomicToken
	retry, direct, spin, polite, park := d.AtomicBeginInternalRMWCooperative(addr, size, ctx, pc, true, &token)
	if retry {
		t.Fatalf("unexpected uncontended public RMW retry: direct=%v spin=%v polite=%v park=%p token=%v", direct, spin, polite, park, token)
	}
	fast := atomicFastToken(&token)
	if wake := d.AtomicEndInternalRMW(addr, size, ctx, &token, pc, write, true, direct); wake != nil {
		t.Fatalf("uncontended public RMW returned wake %p", wake)
	}
	if token != (AtomicToken{}) {
		t.Fatalf("public RMW retained token entries: %v", token)
	}
	return fast, direct
}

func assertPublicRMWOwnerIdle(t *testing.T, state *atomicState) {
	t.Helper()
	state.rmwQueue.lock()
	defer state.rmwQueue.unlock()
	if state.rmwOwner || state.rmwGranted || state.rmwGrantTID != 0 || state.rmwWaiters != 0 || state.rmwSpinnerTID != 0 {
		t.Fatalf("public RMW owner state remained live: owner=%v granted=%v grantTID=%d waiters=%d spinner=%d",
			state.rmwOwner, state.rmwGranted, state.rmwGrantTID, state.rmwWaiters, state.rmwSpinnerTID)
	}
	if state.mu.state.Load() != 0 {
		t.Fatalf("public RMW state lock remained held: %d", state.mu.state.Load())
	}
	if state.directRMWRelease != nil || state.directRMWAppend.Valid() {
		t.Fatalf("public RMW retained direct publication proof: release=%p append=%v", state.directRMWRelease, state.directRMWAppend.Valid())
	}
}

func TestPublicRMWSameOwnerDirectCompletionAndFailedCASFallback(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(124)
	defer DeactivateAtomicLoadCache(ctx)
	ctx.AtomicRMWCacheActive = true
	const (
		addr = uintptr(0x92400)
		pc   = uintptr(0x6150)
	)

	if fast, direct := completePublicDirectRMWForTest(t, d, addr, 8, ctx, pc, true); fast == nil || direct {
		t.Fatalf("public enrollment = fast %p direct %v, want non-nil/false", fast, direct)
	}
	entry, ok := lookupAtomicRMWCache(ctx, addr, 0xff, true)
	if !ok {
		t.Fatal("public enrollment did not retain the exact capability")
	}
	state := (*atomicState)(entry.State)

	beforeDirect := ctx.GetEpoch()
	if fast, direct := completePublicDirectRMWForTest(t, d, addr, 8, ctx, pc+1, true); fast != (*shadowmem.AtomicFastPath)(entry.Fast) || !direct {
		t.Fatalf("same-owner public completion = fast %p direct %v, want %p/true", fast, direct, entry.Fast)
	}
	tid, clock := beforeDirect.Decode()
	if got, want := ctx.GetEpoch(), epoch.NewEpoch(tid, clock+1); got != want {
		t.Fatalf("direct public RMW epoch = %v, want %v", got, want)
	}
	state.mu.lock()
	write, hasWrite := atomicHistoryAccess(state.writes, ctx.TID)
	release, membership := state.exactReleaseForMask(0xff)
	state.mu.unlock()
	if !hasWrite {
		t.Fatal("direct public RMW did not retain its exact write history")
	}
	for lane, got := range write.clocks {
		if got != uint32(clock) {
			t.Fatalf("direct public RMW write lane %d clock = %d, want %d", lane, got, clock)
		}
	}
	if release == nil || membership != 0xff {
		t.Fatalf("direct public RMW release = %p/%#x, want exact full mask", release, membership)
	}
	if seen, strong, ok := ctx.LookupAtomicRelease(unsafe.Pointer(release), release.stream, membership); !ok || !strong || seen != release.version {
		t.Fatalf("direct public RMW release proof = version %d strong %v ok %v, want %d/true/true", seen, strong, ok, release.version)
	}
	assertPublicRMWOwnerIdle(t, state)

	beforeFailed := ctx.GetEpoch()
	if fast, direct := completePublicDirectRMWForTest(t, d, addr, 8, ctx, pc+2, false); fast != (*shadowmem.AtomicFastPath)(entry.Fast) || !direct {
		t.Fatalf("failed public CAS begin = fast %p direct %v, want %p/true", fast, direct, entry.Fast)
	}
	if got := ctx.GetEpoch(); got != beforeFailed {
		t.Fatalf("failed public CAS advanced epoch to %v, want %v", got, beforeFailed)
	}
	state.mu.lock()
	_, hasRead := atomicHistoryAccess(state.reads, ctx.TID)
	state.mu.unlock()
	if !hasRead {
		t.Fatal("failed public CAS did not complete canonically as a read")
	}
	assertPublicRMWOwnerIdle(t, state)
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("direct public atomic-only sequence reported %d races", got)
	}
}

func TestPublicRMWDirectProofMissesFailClosed(t *testing.T) {
	t.Run("foreign owner", func(t *testing.T) {
		d := NewDetector()
		owner := goroutine.Alloc(125)
		other := goroutine.Alloc(126)
		defer DeactivateAtomicLoadCache(owner)
		defer DeactivateAtomicLoadCache(other)
		owner.AtomicRMWCacheActive = true
		other.AtomicRMWCacheActive = true
		const (
			addr = uintptr(0x92500)
			pc   = uintptr(0x6160)
		)

		completePublicDirectRMWForTest(t, d, addr, 8, owner, pc, true)
		if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, owner, pc+1, true); !direct {
			t.Fatal("same owner did not establish the direct tier")
		}
		if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, other, pc+2, true); direct {
			t.Fatal("foreign owner used a stale same-owner release proof")
		}
		if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, owner, pc+3, true); direct {
			t.Fatal("old owner bypassed canonical acquire after foreign publication")
		}
		if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, owner, pc+4, true); !direct {
			t.Fatal("canonical reacquire did not restore the same-owner direct tier")
		}
		entry, ok := lookupAtomicRMWCache(owner, addr, 0xff, true)
		if !ok {
			t.Fatal("owner lost its bounded public RMW cache entry")
		}
		assertPublicRMWOwnerIdle(t, (*atomicState)(entry.State))
	})

	t.Run("clear and ordinary history", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(127)
		ordinary := goroutine.Alloc(128)
		defer DeactivateAtomicLoadCache(ctx)
		defer DeactivateAtomicLoadCache(ordinary)
		ctx.AtomicRMWCacheActive = true
		const (
			addr = uintptr(0x92600)
			pc   = uintptr(0x6170)
		)

		old, _ := completePublicDirectRMWForTest(t, d, addr, 8, ctx, pc, true)
		if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, ctx, pc+1, true); !direct {
			t.Fatal("same owner did not establish the direct tier before clear")
		}
		d.ClearShadowRange(addr, 8)
		if old.TryRetain(0xff) {
			old.Release()
			t.Fatal("cleared public direct capability retained after generation replacement")
		}
		if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, ctx, pc+2, true); direct {
			t.Fatal("clear/address reuse accepted a stale direct proof")
		}
		d.OnRead(addr, ordinary, pc+3)
		if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, ctx, pc+4, true); direct {
			t.Fatal("ordinary history accepted a public direct completion")
		}
	})
}

func TestPublicRMWDirectPreparedPromotedRelease(t *testing.T) {
	d := NewDetector()
	const (
		addr = uintptr(0x92700)
		pc   = uintptr(0x6180)
	)
	contexts := make([]*goroutine.RaceContext, atomicReleasePromotionCoordinateThreshold+8)
	for i := range contexts {
		ctx := goroutine.Alloc(200 + uint32(i))
		ctx.AtomicRMWCacheActive = true
		contexts[i] = ctx
		defer DeactivateAtomicLoadCache(ctx)
		if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, ctx, pc+uintptr(i), true); direct {
			t.Fatalf("fresh public owner %d unexpectedly used direct completion", i)
		}
	}

	owner := contexts[len(contexts)-1]
	entry, ok := lookupAtomicRMWCache(owner, addr, 0xff, true)
	if !ok {
		t.Fatal("promoted public owner did not retain its exact capability")
	}
	state := (*atomicState)(entry.State)
	state.mu.lock()
	release, membership := state.exactReleaseForMask(0xff)
	if release == nil || membership != 0xff || release.lineage == nil || !release.view.Valid() {
		state.mu.unlock()
		t.Fatalf("wide public release = %p/%#x lineage=%p, want promoted exact release", release, membership, func() *vectorclock.ClockLineage {
			if release == nil {
				return nil
			}
			return release.lineage
		}())
	}
	beforeReleaseVersion := release.version
	beforeLineageVersion := release.view.Version()
	state.mu.unlock()

	if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, owner, pc+0x100, true); !direct {
		t.Fatal("same owner did not prepare direct completion for a promoted release")
	}
	state.mu.lock()
	if release.version <= beforeReleaseVersion || release.view.Version() <= beforeLineageVersion {
		state.mu.unlock()
		t.Fatalf("prepared direct publication did not advance release/view: release %d->%d view %d->%d",
			beforeReleaseVersion, release.version, beforeLineageVersion, release.view.Version())
	}
	afterSuccessVersion := release.version
	afterSuccessView := release.view.Version()
	state.mu.unlock()
	if seen, strong, ok := owner.LookupAtomicRelease(unsafe.Pointer(release), release.stream, 0xff); !ok || !strong || seen != release.version {
		t.Fatalf("prepared direct release proof = version %d strong %v ok %v, want %d/true/true", seen, strong, ok, release.version)
	}

	if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, owner, pc+0x101, false); !direct {
		t.Fatal("promoted failed CAS did not begin through the direct tier")
	}
	state.mu.lock()
	if release.version != afterSuccessVersion || release.view.Version() != afterSuccessView {
		state.mu.unlock()
		t.Fatalf("failed CAS published prepared release: release %d->%d view %d->%d",
			afterSuccessVersion, release.version, afterSuccessView, release.view.Version())
	}
	state.mu.unlock()
	assertPublicRMWOwnerIdle(t, state)

	if _, direct := completePublicDirectRMWForTest(t, d, addr, 8, owner, pc+0x102, true); !direct {
		t.Fatal("aborted failed-CAS append did not preserve later direct completion")
	}
	assertPublicRMWOwnerIdle(t, state)
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("prepared promoted public RMW sequence reported %d races", got)
	}
}

func TestPublicRMWCacheSkipsSlotLookupAndKeepsCanonicalCompletion(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(121)
	defer DeactivateAtomicLoadCache(ctx)
	const (
		addr = uintptr(0x92100)
		pc   = uintptr(0x6120)
	)

	first := completePublicRMWForTest(d, addr, 8, ctx, pc, true)
	if first == nil {
		t.Fatal("public RMW seed did not retain an exact capability")
	}
	if _, ok := lookupAtomicRMWCache(ctx, addr, 0xff, true); ok {
		t.Fatal("uncontended public RMW activated the contention cache")
	}
	state := atomicHistoryForTest(t, d, addr)
	state.mu.lock()
	var contended AtomicToken
	retry, _, _, _ := d.AtomicBeginRMWCooperative(addr, 8, ctx, true, &contended)
	state.mu.unlock()
	if !retry || contended != (AtomicToken{}) || !ctx.AtomicRMWCacheActive {
		t.Fatalf("contended begin = retry %v token %v active %v, want true/empty/true", retry, contended, ctx.AtomicRMWCacheActive)
	}
	if seeded := completePublicRMWForTest(d, addr, 8, ctx, pc+1, true); seeded != first {
		t.Fatalf("cache-seeding public RMW capability = %p, want %p", seeded, first)
	}
	entry, ok := lookupAtomicRMWCache(ctx, addr, 0xff, true)
	if !ok || entry.Fast != unsafe.Pointer(first) {
		t.Fatalf("public RMW cache = (%v, %p), want capability %p", ok, entry.Fast, first)
	}

	probe := &countingSlotShadow{SlotShadow: d.slotMemory}
	d.slotMemory = probe
	beforeWrite := ctx.GetEpoch()
	second := completePublicRMWForTest(d, addr, 8, ctx, pc+2, true)
	if second != first {
		t.Fatalf("cached public RMW capability = %p, want %p", second, first)
	}
	if probe.getSlotCalls != 0 {
		t.Fatalf("cached public RMW performed %d shadow-slot lookups, want 0", probe.getSlotCalls)
	}
	tid, clock := beforeWrite.Decode()
	wantWrite := epoch.NewEpoch(tid, clock+1)
	if got := ctx.GetEpoch(); got != wantWrite {
		t.Fatalf("cached public RMW epoch = %v, want one advance from %v", got, beforeWrite)
	}

	beforeFailedCAS := ctx.GetEpoch()
	failed := completePublicRMWForTest(d, addr, 8, ctx, pc+3, false)
	if failed != first {
		t.Fatalf("cached failed-CAS capability = %p, want %p", failed, first)
	}
	if probe.getSlotCalls != 0 {
		t.Fatalf("cached failed CAS performed %d shadow-slot lookups, want 0", probe.getSlotCalls)
	}
	if got := ctx.GetEpoch(); got != beforeFailedCAS {
		t.Fatalf("failed CAS advanced epoch to %v, want %v", got, beforeFailedCAS)
	}
	state.mu.lock()
	_, hasRead := atomicHistoryAccess(state.reads, ctx.TID)
	state.mu.unlock()
	if !hasRead {
		t.Fatal("cached failed CAS did not complete canonically as a read")
	}

	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("cached public RMW sequence reported %d races", got)
	}
}

func TestPublicRMWCacheFailsClosedAfterClearAndMismatch(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(122)
	defer DeactivateAtomicLoadCache(ctx)
	const (
		addr = uintptr(0x92200)
		pc   = uintptr(0x6130)
	)
	ctx.AtomicRMWCacheActive = true

	old := completePublicRMWForTest(d, addr, 8, ctx, pc, true)
	_, ok := lookupAtomicRMWCache(ctx, addr, 0xff, true)
	if !ok {
		t.Fatal("public RMW did not seed cache before clear")
	}
	d.ClearShadowRange(addr, 8)
	if old.TryRetain(0xff) {
		old.Release()
		t.Fatal("cleared cached capability retained after address reuse")
	}

	probe := &countingSlotShadow{SlotShadow: d.slotMemory}
	d.slotMemory = probe
	completePublicRMWForTest(d, addr, 8, ctx, pc+1, true)
	if probe.getSlotCalls == 0 {
		t.Fatal("closed cached capability did not fall back to slot lookup")
	}

	// A stale arena-generation discriminator must also miss before using the
	// cached pointer. Restore it before completion refreshes the same owned entry
	// so the test does not manufacture an ownership leak.
	_, ok = lookupAtomicRMWCache(ctx, addr, 0xff, true)
	if !ok {
		t.Fatal("post-clear public RMW did not refresh cache")
	}
	for i := range ctx.AtomicRMWCache {
		cached := &ctx.AtomicRMWCache[i]
		if cached.Addr != addr || cached.Mask != 0xff || !cached.Synchronize {
			continue
		}
		generation := cached.StateGeneration
		cached.StateGeneration++
		probe.getSlotCalls = 0
		var token AtomicToken
		beginPublicRMWForTest(d, addr, 8, ctx, &token)
		cached.StateGeneration = generation
		d.AtomicEndInternalRMW(addr, 8, ctx, &token, pc+2, true, true, false)
		if probe.getSlotCalls == 0 {
			t.Fatal("stale cache generation did not fall back to slot lookup")
		}
		return
	}
	t.Fatal("post-clear cache entry was not addressable")
}

func TestPublicRMWCacheBoundedEvictionAndOverlayMismatch(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(123)
	defer DeactivateAtomicLoadCache(ctx)
	ctx.AtomicRMWCacheActive = true
	const pc = uintptr(0x6140)
	addrs := [3]uintptr{0x92300, 0x92308, 0x92310}
	fast := make([]*shadowmem.AtomicFastPath, len(addrs))
	for i, addr := range addrs {
		fast[i] = completePublicRMWForTest(d, addr, 8, ctx, pc+uintptr(i), true)
		if fast[i] == nil {
			t.Fatalf("address %d did not enroll an exact capability", i)
		}
	}
	populated := 0
	for i := range ctx.AtomicRMWCache {
		if ctx.AtomicRMWCache[i].State != nil {
			populated++
		}
	}
	if populated != goroutine.AtomicRMWCacheSlots {
		t.Fatalf("populated cache entries = %d, want bounded %d", populated, goroutine.AtomicRMWCacheSlots)
	}
	if _, ok := lookupAtomicRMWCache(ctx, addrs[0], 0xff, true); ok {
		t.Fatal("third exact address did not evict the oldest two-slot entry")
	}

	// Replace one entry's descriptor root with the other live descriptor while
	// keeping its state ownership intact. Direct retain succeeds, but overlay
	// identity must reject it before state mutation and use the slot fallback.
	firstIndex, secondIndex := -1, -1
	for i := range ctx.AtomicRMWCache {
		switch ctx.AtomicRMWCache[i].Addr {
		case addrs[1]:
			firstIndex = i
		case addrs[2]:
			secondIndex = i
		}
	}
	if firstIndex < 0 || secondIndex < 0 {
		t.Fatalf("post-eviction cache indices = %d/%d, want both retained", firstIndex, secondIndex)
	}
	ctx.AtomicRMWCache[firstIndex].Fast = ctx.AtomicRMWCache[secondIndex].Fast
	probe := &countingSlotShadow{SlotShadow: d.slotMemory}
	d.slotMemory = probe
	if got := completePublicRMWForTest(d, addrs[1], 8, ctx, pc+3, true); got != fast[1] {
		t.Fatalf("overlay-mismatch fallback capability = %p, want %p", got, fast[1])
	}
	if probe.getSlotCalls == 0 {
		t.Fatal("overlay-mismatched cache entry did not use slot fallback")
	}
	entry, ok := lookupAtomicRMWCache(ctx, addrs[1], 0xff, true)
	if !ok || entry.Fast != unsafe.Pointer(fast[1]) {
		t.Fatalf("overlay-mismatch refresh = (%v, %p), want %p", ok, entry.Fast, fast[1])
	}
}

func TestInternalRMWSameOwnerDirectAndFailedCASCanonicalCompletion(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(101)
	defer DeactivateAtomicLoadCache(ctx)
	pc := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	const addr = uintptr(0x8d000)

	if fast, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, true, true); !fast || direct {
		t.Fatalf("enrollment completion = fast %v direct %v, want true/false", fast, direct)
	}
	if fast, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, true, true); !fast || !direct {
		t.Fatalf("same-owner completion = fast %v direct %v, want true/true", fast, direct)
	}
	entry, ok := lookupAtomicRMWCache(ctx, addr, 0x0f, true)
	if !ok {
		t.Fatal("same-owner completion did not retain cache entry")
	}
	state := (*atomicState)(entry.State)
	state.mu.lock()
	var contended AtomicToken
	retry, direct, _, _, _ := d.AtomicBeginInternalRMWCooperative(addr, 4, ctx, pc, true, &contended)
	state.mu.unlock()
	if !retry || direct || contended[0] != nil {
		t.Fatalf("contended begin = retry %v direct %v token %p, want true/false/nil", retry, direct, contended[0])
	}

	// A failed compare-and-swap executes hardware once, then completes the
	// already-retained transaction canonically as a read. The subsequent success
	// proves that completion neither escaped the capability nor lost its release.
	if fast, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, true, false); !fast || !direct {
		t.Fatalf("failed-CAS begin = fast %v direct %v, want true/true", fast, direct)
	}
	if fast, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, true, true); !fast || !direct {
		t.Fatalf("post-failed-CAS completion = fast %v direct %v, want true/true", fast, direct)
	}
	if got := d.RacesDetected(); got != 0 {
		t.Fatalf("internal atomic-only sequence reported %d races", got)
	}
}

func TestInternalRMWNonSynchronizingModeIsSeparateAndRetained(t *testing.T) {
	d := NewDetector()
	ctx := goroutine.Alloc(102)
	defer DeactivateAtomicLoadCache(ctx)
	pc := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	const addr = uintptr(0x8e000)
	before := ctx.GetEpoch()

	if fast, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, false, true); !fast || direct {
		t.Fatalf("disabled enrollment = fast %v direct %v, want true/false", fast, direct)
	}
	if fast, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, false, true); !fast || !direct {
		t.Fatalf("disabled same-owner completion = fast %v direct %v, want true/true", fast, direct)
	}
	if got := ctx.GetEpoch(); got != before {
		t.Fatalf("disabled RMW advanced epoch from %v to %v", before, got)
	}
	if _, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, true, true); direct {
		t.Fatal("synchronizing operation reused RaceDisable cache mode")
	}
}

func TestInternalRMWNonSynchronizingMissDoesNotRequireClockSuccessor(t *testing.T) {
	// NewEpoch records process-wide near-overflow diagnostics. Restore those
	// globals so this boundary test cannot disable unrelated fast-path tests
	// which run later in the same package process.
	epoch.ResetOverflowFlags()
	defer epoch.ResetOverflowFlags()
	d := NewDetector()
	ctx := goroutine.Alloc(102)
	defer ctx.C.Release()
	ctx.C.Set(ctx.TID, ^uint32(0))
	ctx.Epoch = epoch.NewEpoch(ctx.TID, epoch.MaxClock)
	pc := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
	const addr = uintptr(0x8e100)

	if fast, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, false, true); !fast || direct {
		t.Fatalf("non-synchronizing max-clock miss = fast %v direct %v, want true/false", fast, direct)
	}
	if _, got := ctx.GetEpoch().Decode(); got != epoch.MaxClock {
		t.Fatalf("non-synchronizing max-clock miss changed epoch to %v", got)
	}
}

func TestInternalRMWDirectFallsBackOnOwnerReleaseAndOrdinaryHistory(t *testing.T) {
	t.Run("owner", func(t *testing.T) {
		d := NewDetector()
		owner := goroutine.Alloc(103)
		other := goroutine.Alloc(104)
		defer DeactivateAtomicLoadCache(owner)
		defer DeactivateAtomicLoadCache(other)
		pc := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
		const addr = uintptr(0x8f000)
		completeInternalRMWForTest(d, addr, 4, owner, pc, true, true)
		if _, direct := completeInternalRMWForTest(d, addr, 4, other, pc, true, true); direct {
			t.Fatal("foreign context used owner's retained entry")
		}
		if _, direct := completeInternalRMWForTest(d, addr, 4, owner, pc, true, true); direct {
			t.Fatal("stale same-owner release proof bypassed canonical acquire")
		}
	})

	t.Run("ordinary", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(105)
		defer DeactivateAtomicLoadCache(ctx)
		pc := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
		const addr = uintptr(0x90000)
		d.OnWrite(addr, ctx, 0x9001)
		for i := 0; i < 3; i++ {
			if _, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, true, true); direct {
				t.Fatal("direct completion accepted frozen ordinary history")
			}
		}
	})

	t.Run("clear", func(t *testing.T) {
		d := NewDetector()
		ctx := goroutine.Alloc(106)
		defer DeactivateAtomicLoadCache(ctx)
		pc := reflect.ValueOf((*internalsync.Mutex).Lock).Pointer() + 1
		const addr = uintptr(0x91000)
		completeInternalRMWForTest(d, addr, 4, ctx, pc, true, true)
		completeInternalRMWForTest(d, addr, 4, ctx, pc, true, true)
		d.ClearShadowRange(addr, 4)
		if _, direct := completeInternalRMWForTest(d, addr, 4, ctx, pc, true, true); direct {
			t.Fatal("cleared lifecycle reused dormant direct entry")
		}
	})
}
