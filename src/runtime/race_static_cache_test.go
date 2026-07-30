// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package runtime_test

import (
	"bytes"
	"context"
	"internal/testenv"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

var (
	raceStaticNoptrData uintptr = 1
	raceStaticNoptrBSS  uintptr
	raceStaticData      = unsafe.Pointer(&raceStaticNoptrData)
	raceStaticBSS       unsafe.Pointer
	raceStaticHeap      *uintptr
)

func TestRaceFirstModuleStaticData(t *testing.T) {
	globals := []struct {
		name string
		addr uintptr
	}{
		{"noptrdata", uintptr(unsafe.Pointer(&raceStaticNoptrData))},
		{"noptrbss", uintptr(unsafe.Pointer(&raceStaticNoptrBSS))},
		{"data", uintptr(unsafe.Pointer(&raceStaticData))},
		{"bss", uintptr(unsafe.Pointer(&raceStaticBSS))},
	}
	for _, global := range globals {
		if !runtime.RaceFirstModuleStaticData(global.addr) {
			t.Errorf("%s global at %#x was not classified as first-module static data", global.name, global.addr)
		}
	}

	stack := uintptr(0)
	if runtime.RaceFirstModuleStaticData(uintptr(unsafe.Pointer(&stack))) {
		t.Errorf("stack address %#x was classified as first-module static data", uintptr(unsafe.Pointer(&stack)))
	}

	raceStaticHeap = new(uintptr)
	heapAddr := uintptr(unsafe.Pointer(raceStaticHeap))
	if runtime.RaceFirstModuleStaticData(heapAddr) {
		t.Errorf("heap address %#x was classified as first-module static data", heapAddr)
	}
	raceStaticHeap = nil
}

// TestRaceStaticReadCacheSynchronization verifies that the static-address
// shortcut still observes cache invalidation when the reader's clock advances.
// The first read happens-before the child write through goroutine creation. The
// atomic handoff is hidden from the detector, so only the final read races.
func TestRaceStaticReadCacheSynchronization(t *testing.T) {
	goTool := testenv.GoToolPath(t)
	src := filepath.Join(t.TempDir(), "main.go")
	const program = `package main

import (
	"runtime"
	"sync/atomic"
)

var value int
var ready uint32

//go:noinline
func readValue() int { return value }

func main() {
	if readValue() == 42 {
		panic("unreachable")
	}
	go func() {
		value = 1
		runtime.RaceDisable()
		atomic.StoreUint32(&ready, 1)
		runtime.RaceEnable()
	}()
	runtime.RaceDisable()
	for atomic.LoadUint32(&ready) == 0 {
		runtime.Gosched()
	}
	runtime.RaceEnable()
	if readValue() == 42 {
		panic("unreachable")
	}
}

`
	if err := os.WriteFile(src, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := testenv.Command(t, goTool, "run", "-race", src)
	cmd.Env = append(withoutRaceBackendEnv(cmd.Environ()), "CGO_ENABLED=0", "GOMAXPROCS=2")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("racy static read unexpectedly succeeded:\n%s", out)
	}
	if !bytes.Contains(out, []byte("WARNING: DATA RACE")) {
		t.Fatalf("static read after cache invalidation did not report a race:\n%s", out)
	}
}

// TestRaceStaticByteReadCacheSynchronization exercises the one-byte raceread
// ABI path. A weakened high-bit width must not be accepted as a current-epoch
// static cache entry after synchronization.
func TestRaceStaticByteReadCacheSynchronization(t *testing.T) {
	goTool := testenv.GoToolPath(t)
	src := filepath.Join(t.TempDir(), "main.go")
	const program = `package main

import (
	"runtime"
	"sync/atomic"
)

var value byte
var ready uint32

//go:noinline
func readValue() byte { return value }

func main() {
	if readValue() == 42 {
		panic("unreachable")
	}
	go func() {
		value = 1
		runtime.RaceDisable()
		atomic.StoreUint32(&ready, 1)
		runtime.RaceEnable()
	}()
	runtime.RaceDisable()
	for atomic.LoadUint32(&ready) == 0 {
		runtime.Gosched()
	}
	runtime.RaceEnable()
	if readValue() == 42 {
		panic("unreachable")
	}
}
`
	if err := os.WriteFile(src, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := testenv.Command(t, goTool, "run", "-race", src)
	cmd.Env = append(withoutRaceBackendEnv(cmd.Environ()), "CGO_ENABLED=0", "GOMAXPROCS=2")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("racy static byte read unexpectedly succeeded:\n%s", out)
	}
	if !bytes.Contains(out, []byte("WARNING: DATA RACE")) {
		t.Fatalf("static byte read after weak cache hint did not report a race:\n%s", out)
	}
}

// TestRaceStaticSizedReadOverlap verifies that the compiler's exact-width
// scalar hook retains every byte in the static read cache. The atomic store
// touches only the upper half of value, so start-address-only tracking would
// miss the final conflicting read.
func TestRaceStaticSizedReadOverlap(t *testing.T) {
	goTool := testenv.GoToolPath(t)
	src := filepath.Join(t.TempDir(), "main.go")
	const program = `package main

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

var value uint64
var ready uint32

//go:noinline
func readValue() uint64 { return value }

func main() {
	if readValue() == 42 {
		panic("unreachable")
	}
	go func() {
		upper := (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&value)) + 4))
		atomic.StoreUint32(upper, 1)
		runtime.RaceDisable()
		atomic.StoreUint32(&ready, 1)
		runtime.RaceEnable()
	}()
	runtime.RaceDisable()
	for atomic.LoadUint32(&ready) == 0 {
		runtime.Gosched()
	}
	runtime.RaceEnable()
	if readValue() == 42 {
		panic("unreachable")
	}
}
`
	if err := os.WriteFile(src, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := testenv.Command(t, goTool, "run", "-race", src)
	cmd.Env = append(withoutRaceBackendEnv(cmd.Environ()), "CGO_ENABLED=0", "GOMAXPROCS=2")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("partially overlapping static access unexpectedly succeeded:\n%s", out)
	}
	if !bytes.Contains(out, []byte("WARNING: DATA RACE")) {
		t.Fatalf("partially overlapping static access did not report a race:\n%s", out)
	}
}

// TestRaceStaticReadCacheFinalizerHandoff reproduces the one-way finalizer
// synchronization that used to leave a source goroutine's static read cache
// valid. A blocked first finalizer lets main queue the writing finalizer, seed
// its cache, and then release the finalizer goroutine without a detector-visible
// synchronization edge back to main.
func TestRaceStaticReadCacheFinalizerHandoff(t *testing.T) {
	goTool := testenv.GoToolPath(t)
	src := filepath.Join(t.TempDir(), "main.go")
	const program = `package main

import (
	"runtime"
	"sync/atomic"
)

type finalObject struct { anchor *byte }

var value int
var blockerStarted atomic.Uint32
var releaseBlocker atomic.Uint32
var writerFinished atomic.Uint32

//go:noinline
func readValue() int { return value }

func hiddenStore(flag *atomic.Uint32, value uint32) {
	runtime.RaceDisable()
	flag.Store(value)
	runtime.RaceEnable()
}

func hiddenLoad(flag *atomic.Uint32) uint32 {
	runtime.RaceDisable()
	value := flag.Load()
	runtime.RaceEnable()
	return value
}

//go:noinline
func installBlocker() {
	object := &finalObject{anchor: new(byte)}
	runtime.SetFinalizer(object, func(*finalObject) {
		hiddenStore(&blockerStarted, 1)
		for hiddenLoad(&releaseBlocker) == 0 {
			runtime.Gosched()
		}
	})
	runtime.KeepAlive(object)
}

//go:noinline
func installWriter() {
	object := &finalObject{anchor: new(byte)}
	runtime.SetFinalizer(object, func(*finalObject) {
		value = 1
		hiddenStore(&writerFinished, 1)
	})
	runtime.KeepAlive(object)
}

func wait(flag *atomic.Uint32) {
	for hiddenLoad(flag) == 0 {
		runtime.Gosched()
	}
}

func main() {
	installBlocker()
	for hiddenLoad(&blockerStarted) == 0 {
		runtime.GC()
		runtime.Gosched()
	}

	// This object is queued while the finalizer goroutine is still blocked, so
	// its callback runs under a fresh racefingo snapshot.
	installWriter()
	runtime.GC()

	// Seed an exact static cache entry before the snapshot that orders the
	// finalizer write after this read.
	if readValue() == 42 {
		panic("unreachable")
	}
	if readValue() == 42 {
		panic("unreachable")
	}
	hiddenStore(&releaseBlocker, 1)
	wait(&writerFinished)

	// No synchronization edge flows from the finalizer back to main. This read
	// races with the write even though the pre-handoff cache entry still exists.
	if readValue() == 42 {
		panic("unreachable")
	}
}
`
	if err := os.WriteFile(src, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := testenv.CommandContext(t, ctx, goTool, "run", "-race", src)
	cmd.Env = append(withoutRaceBackendEnv(cmd.Environ()),
		"CGO_ENABLED=0",
		"GOMAXPROCS=2",
		"GORACE=atexit_sleep_ms=0 halt_on_error=1 exitcode=66",
	)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("finalizer cache regression timed out: %v\n%s", ctx.Err(), out)
	}
	if err == nil {
		t.Fatalf("racy post-finalizer static read unexpectedly succeeded:\n%s", out)
	}
	if !bytes.Contains(out, []byte("WARNING: DATA RACE")) {
		t.Fatalf("post-finalizer static read did not report a race:\n%s", out)
	}
}

func withoutRaceBackendEnv(env []string) []string {
	clean := make([]string, 0, len(env))
	for _, value := range env {
		if strings.HasPrefix(value, "CGO_ENABLED=") ||
			strings.HasPrefix(value, "GOMAXPROCS=") ||
			strings.HasPrefix(value, "GODEBUG=") ||
			strings.HasPrefix(value, "GORACE=") {
			continue
		}
		clean = append(clean, value)
	}
	return clean
}
