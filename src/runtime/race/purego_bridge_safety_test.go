// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package race_test

import (
	"bytes"
	"context"
	"internal/testenv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestPureGoBridgeGCAndStackReuse exercises the runtime-to-detector bridge while
// stacks are repeatedly grown, discarded, and reused under aggressive GC. The
// detector runs on g0 to pin stack-derived uintptr arguments, so this test also
// covers atomic tokens rooted in the suspended user stack and detector work
// reached from timer, finalizer, and cleanup goroutines.
func TestPureGoBridgeGCAndStackReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess stress test in short mode")
	}

	const source = `package main

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type node struct {
	value uint64
	next  *node
}

type finalObject struct {
	link    *byte
	payload [64]byte
}

var sink atomic.Uint64

// heapBytes keeps range operations visible to race instrumentation. A fixed-size
// backing array local to exercise would stay on its stack and be skipped.
//go:noinline
func heapBytes() []byte {
	return make([]byte, 1024)
}

//go:noinline
func growStack(depth int, value uint64) uint64 {
	var pad [512]byte
	pad[0] = byte(value)
	pad[len(pad)-1] = byte(value >> 8)
	if depth != 0 {
		value += growStack(depth-1, value+1)
	}
	runtime.KeepAlive(&pad)
	return value + uint64(pad[0]) + uint64(pad[len(pad)-1])
}

//go:noinline
func exercise(id uint64) {
	var localMutex sync.Mutex
	var word atomic.Uint64
	var pointer atomic.Pointer[node]
	src := heapBytes()
	dst := heapBytes()

	localMutex.Lock()
	localMutex.Unlock()
	word.Store(id + 1)
	for i := range src {
		src[i] = byte(id + uint64(i))
	}
	copy(dst, src)
	clear(dst[17 : len(dst)-19])

	// Grow the active stack while word and pointer remain live, then use their
	// potentially relocated addresses in complete atomic transactions.
	value := growStack(24, id+1)
	localMutex.Lock()
	localMutex.Unlock()
	if got := word.Load(); got != id+1 {
		panic("atomic load mismatch")
	}
	if got := word.Add(2); got != id+3 {
		panic("atomic add mismatch")
	}
	if old := word.Swap(id + 5); old != id+3 {
		panic("atomic swap mismatch")
	}
	if !word.CompareAndSwap(id+5, id+7) {
		panic("atomic compare-and-swap mismatch")
	}

	first := &node{value: id}
	second := &node{value: id + 1, next: first}
	pointer.Store(first)
	if got := pointer.Load(); got != first {
		panic("atomic pointer load mismatch")
	}
	if old := pointer.Swap(second); old != first {
		panic("atomic pointer swap mismatch")
	}
	if !pointer.CompareAndSwap(second, first) {
		panic("atomic pointer compare-and-swap mismatch")
	}

	sink.Add(value + word.Load() + pointer.Load().value + uint64(src[0]))
}

//go:noinline
func installFinalizer(done chan<- struct{}) {
	value := &finalObject{link: new(byte)}
	runtime.SetFinalizer(value, func(*finalObject) {
		done <- struct{}{}
	})
	runtime.KeepAlive(value)
}

//go:noinline
func installCleanup(done chan<- struct{}) {
	value := &finalObject{link: new(byte)}
	runtime.AddCleanup(value, func(done chan<- struct{}) {
		done <- struct{}{}
	}, done)
	runtime.KeepAlive(value)
}

func waitForCollectors(done <-chan struct{}) {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for collected := 0; collected != 2; {
		runtime.GC()
		select {
		case <-done:
			collected++
		case <-deadline.C:
			panic("finalizer or cleanup did not run")
		default:
			runtime.Gosched()
		}
	}
}

func main() {
	gcStop := make(chan struct{})
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		for {
			select {
			case <-gcStop:
				return
			default:
				runtime.GC()
				runtime.Gosched()
			}
		}
	}()

	const (
		rounds  = 32
		workers = 24
	)
	for round := range rounds {
		var group sync.WaitGroup
		group.Add(workers)
		for worker := range workers {
			id := uint64(round*workers + worker)
			go func() {
				defer group.Done()
				exercise(id)
			}()
		}
		group.Wait()

		timerDone := make(chan struct{})
		time.AfterFunc(0, func() {
			exercise(uint64(round*workers + workers))
			close(timerDone)
		})
		<-timerDone
	}

	close(gcStop)
	<-gcDone

	collectorDone := make(chan struct{}, 2)
	installFinalizer(collectorDone)
	installCleanup(collectorDone)
	waitForCollectors(collectorDone)

	if sink.Load() == 0 {
		panic("unreachable zero checksum")
	}
}
`

	dir := t.TempDir()
	src := filepath.Join(dir, "main.go")
	bin := filepath.Join(dir, "bridge-stress")
	if err := os.WriteFile(src, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	build := testenv.CommandContext(t, buildCtx, testenv.GoToolPath(t), "build", "-race", "-o", bin, src)
	build.Env = pureGoRaceEnv(build.Environ())
	if out, err := build.CombinedOutput(); err != nil {
		if buildCtx.Err() != nil {
			t.Fatalf("timed out building bridge stress program: %v\n%s", buildCtx.Err(), out)
		}
		t.Fatalf("build bridge stress program: %v\n%s", err, out)
	}

	for _, gomaxprocs := range []string{"1", "2"} {
		t.Run("gomaxprocs-"+gomaxprocs, func(t *testing.T) {
			runCtx, cancelRun := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancelRun()
			run := testenv.CommandContext(t, runCtx, bin)
			run.Env = bridgeStressEnv(run.Environ(), gomaxprocs)
			out, err := run.CombinedOutput()
			if runCtx.Err() != nil {
				t.Fatalf("bridge stress timed out: %v\n%s", runCtx.Err(), out)
			}
			if err != nil {
				t.Fatalf("bridge stress failed: %v\n%s", err, out)
			}
			for _, bad := range [][]byte{
				[]byte("WARNING: DATA RACE"),
				[]byte("fatal error:"),
				[]byte("runtime: fatal"),
			} {
				if bytes.Contains(out, bad) {
					t.Fatalf("bridge stress printed %q:\n%s", bad, out)
				}
			}
		})
	}
}

func bridgeStressEnv(env []string, gomaxprocs string) []string {
	env = pureGoRaceEnv(env)
	clean := make([]string, 0, len(env)+3)
	for _, value := range env {
		if strings.HasPrefix(value, "GOMAXPROCS=") ||
			strings.HasPrefix(value, "GOGC=") ||
			strings.HasPrefix(value, "GORACE=") {
			continue
		}
		clean = append(clean, value)
	}
	return append(clean,
		"GOMAXPROCS="+gomaxprocs,
		"GOGC=1",
		"GORACE=atexit_sleep_ms=0 halt_on_error=1 exitcode=66",
	)
}

// TestPureGoCompactRangeFrameBudget prevents a recurrence of the oversized
// monolithic frame that left almost no headroom on the fixed race-build g0
// stack. This is a per-frame regression gate, not a proof of the full call
// graph; TestPureGoBridgeGCAndStackReuse exercises the linked paths dynamically.
func TestPureGoCompactRangeFrameBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cross-architecture compiler frame check in short mode")
	}

	const maxFrame = 8 << 10
	goTool := testenv.GoToolPath(t)
	for _, goarch := range []string{"amd64", "arm64"} {
		t.Run(goarch, func(t *testing.T) {
			dir := t.TempDir()
			archive := filepath.Join(dir, "shadowmem.a")
			compileLog := filepath.Join(dir, "compile.log")
			logFile, err := os.Create(compileLog)
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cmd := testenv.CommandContext(t, ctx, goTool, "build", "-race",
				"-gcflags=runtime/race/kolkov/shadowmem=-S",
				"-o", archive, "runtime/race/kolkov/shadowmem")
			cmd.Env = append(pureGoRaceEnv(cmd.Environ()),
				"GOOS=linux", "GOARCH="+goarch, "CGO_ENABLED=0")
			cmd.Stdout = logFile
			cmd.Stderr = logFile
			runErr := cmd.Run()
			closeErr := logFile.Close()
			if runErr != nil || closeErr != nil {
				log, _ := os.ReadFile(compileLog)
				if len(log) > 64<<10 {
					log = log[len(log)-(64<<10):]
				}
				if ctx.Err() != nil {
					t.Fatalf("%s frame build timed out: %v\n%s", goarch, ctx.Err(), log)
				}
				t.Fatalf("%s frame build failed: run=%v close=%v\n%s", goarch, runErr, closeErr, log)
			}

			log, err := os.ReadFile(compileLog)
			if err != nil {
				t.Fatal(err)
			}
			for _, check := range []struct {
				name   string
				symbol string
			}{
				{name: "compactGroups.tryRange", symbol: "runtime/race/kolkov/shadowmem.(*compactGroups).tryRange"},
				{name: "compactPalette.tryRangeLocked", symbol: "runtime/race/kolkov/shadowmem.(*compactPalette).tryRangeLocked"},
				{name: "compactGroup.firstAnchor", symbol: "runtime/race/kolkov/shadowmem.(*compactGroup).firstAnchor"},
				{name: "compactGroups.mergeEquivalent", symbol: "runtime/race/kolkov/shadowmem.(*compactGroups).mergeEquivalent"},
			} {
				frame, ok := compilerFrameSize(log, check.symbol)
				if !ok {
					t.Fatalf("%s compiler output did not contain %s frame", goarch, check.name)
				}
				if frame > maxFrame {
					t.Fatalf("%s %s frame = %d bytes, limit %d", goarch, check.name, frame, maxFrame)
				}
				t.Logf("%s %s frame: %d bytes (limit %d)", goarch, check.name, frame, maxFrame)
			}
		})
	}
}

func compilerFrameSize(assembly []byte, symbol string) (int, bool) {
	needle := []byte("TEXT\t" + symbol + "(SB), ABIInternal, $")
	position := bytes.Index(assembly, needle)
	if position < 0 {
		return 0, false
	}
	position += len(needle)
	frame := 0
	start := position
	for position < len(assembly) && assembly[position] >= '0' && assembly[position] <= '9' {
		frame = frame*10 + int(assembly[position]-'0')
		position++
	}
	return frame, position > start
}
