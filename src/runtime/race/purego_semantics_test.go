// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package race_test

import (
	"bytes"
	"internal/testenv"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestPureGoSemantics(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		wantRace bool
		goarch   string
	}{
		{
			name:     "nested-disable-tracks-memory",
			wantRace: true,
			source: `package main

import "runtime"

var value int

func main() {
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() {
		<-start
		runtime.RaceDisable()
		runtime.RaceDisable()
		value = 1
		runtime.RaceEnable()
		runtime.RaceEnable()
		done <- struct{}{}
	}()
	go func() {
		<-start
		_ = value
		done <- struct{}{}
	}()
	close(start)
	<-done
	<-done
}
`,
		},
		{
			name: "nested-enable-restores-sync",
			source: `package main

import "runtime"

var value int

func main() {
	runtime.RaceDisable()
	runtime.RaceDisable()
	runtime.RaceEnable()
	runtime.RaceEnable()
	done := make(chan struct{})
	go func() {
		value = 1
		close(done)
	}()
	<-done
	_ = value
}
`,
		},
		{
			name:     "retained-static-scalar-revalidates-foreign-write",
			wantRace: true,
			source: `package main

import (
	"runtime"
	"sync/atomic"
)

var value uint64
var ready uint32

func main() {
	for i := range 1000 {
		_ = value
		value = uint64(i)
	}
	start := make(chan struct{})
	go func() {
		<-start
		value = 1001
		runtime.RaceDisable()
		atomic.StoreUint32(&ready, 1)
		runtime.RaceEnable()
	}()
	close(start)
	runtime.RaceDisable()
	for atomic.LoadUint32(&ready) == 0 {
		runtime.Gosched()
	}
	runtime.RaceEnable()
	_ = value
	value = 1002
}
`,
		},
		{
			name: "retained-static-scalar-respects-acquire",
			source: `package main

var value uint64

func main() {
	for i := range 1000 {
		_ = value
		value = uint64(i)
	}
	done := make(chan struct{})
	go func() {
		value = 1001
		close(done)
	}()
	<-done
	_ = value
	value = 1002
}
`,
		},
		{
			name: "buffered-channel-slot-handoff",
			source: `package main

var value int

func main() {
	ch := make(chan int, 1)
	go func() {
		value = 1
		ch <- 1
	}()
	<-ch
	_ = value
}
`,
		},
		{
			name:     "buffered-receive-does-not-acquire-later-send",
			wantRace: true,
			source: `package main

import (
	"runtime"
	"sync/atomic"
)

var value int
var ready uint32

func main() {
	runtime.GOMAXPROCS(2)
	ch := make(chan int, 2)
	ch <- 1
	go func() {
		value = 1
		ch <- 2
		runtime.RaceDisable()
		atomic.StoreUint32(&ready, 1)
		runtime.RaceEnable()
	}()
	runtime.RaceDisable()
	for atomic.LoadUint32(&ready) == 0 {
		runtime.Gosched()
	}
	runtime.RaceEnable()
	<-ch
	_ = value
}
`,
		},
		{
			name: "buffered-receive-acquires-paired-send",
			source: `package main

var value int

func main() {
	ch := make(chan int, 2)
	ch <- 1
	go func() {
		value = 1
		ch <- 2
	}()
	<-ch
	<-ch
	_ = value
}
`,
		},
		{
			name:     "buffered-pre-close-receive-does-not-acquire-close",
			wantRace: true,
			source: `package main

import (
	"runtime"
	"sync/atomic"
)

var value int
var ready uint32

func main() {
	runtime.GOMAXPROCS(2)
	ch := make(chan int, 1)
	ch <- 1
	go func() {
		value = 1
		close(ch)
		runtime.RaceDisable()
		atomic.StoreUint32(&ready, 1)
		runtime.RaceEnable()
	}()
	runtime.RaceDisable()
	for atomic.LoadUint32(&ready) == 0 {
		runtime.Gosched()
	}
	runtime.RaceEnable()
	<-ch
	_ = value
}
`,
		},
		{
			name: "closed-empty-receive-acquires-close",
			source: `package main

var value int

func main() {
	ch := make(chan int, 1)
	ch <- 1
	go func() {
		value = 1
		close(ch)
	}()
	<-ch
	<-ch
	_ = value
}
`,
		},
		{
			name: "enabled-target-channel-sync",
			source: `package main

import "runtime"

var value int

func main() {
	runtime.GOMAXPROCS(1)
	ch := make(chan int, 1)
	ch <- 0
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(ready)
		ch <- 1
		_ = value
		close(done)
	}()
	<-ready
	value = 1
	<-ch
	<-done
}
`,
		},
		{
			name: "unbuffered-rendezvous-orders-both-sides",
			source: `package main

var sent, received int

func main() {
	ch := make(chan struct{})
	done := make(chan struct{})
	go func() {
		sent = 1
		ch <- struct{}{}
		_ = received
		close(done)
	}()
	received = 1
	<-ch
	_ = sent
	<-done
}
`,
		},
		{
			name:     "unbuffered-rendezvous-does-not-order-later-writes",
			wantRace: true,
			source: `package main

var value int

func main() {
	ch := make(chan struct{})
	done := make(chan struct{})
	go func() {
		ch <- struct{}{}
		value = 1
		close(done)
	}()
	<-ch
	value = 2
	<-done
}
`,
		},
		{
			name:     "disabled-target-channel-sync",
			wantRace: true,
			source: `package main

import "runtime"

var value int

func main() {
	runtime.GOMAXPROCS(1)
	ch := make(chan int, 1)
	ch <- 0
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		runtime.RaceDisable()
		close(ready)
		ch <- 1
		_ = value
		runtime.RaceEnable()
		close(done)
	}()
	<-ready
	value = 1
	<-ch
	<-done
}
`,
		},
		{
			name:     "disabled-current-target-channel-sync",
			wantRace: true,
			source: `package main

import "runtime"

var value int

func main() {
	runtime.GOMAXPROCS(1)
	ch := make(chan int, 1)
	ch <- 0
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(ready)
		ch <- 1
		_ = value
		close(done)
	}()
	<-ready
	runtime.RaceDisable()
	value = 1
	<-ch
	runtime.RaceEnable()
	<-done
}
`,
		},
		{
			name:     "disabled-sync-is-suppressed",
			wantRace: true,
			source: `package main

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

var value int
var ready uint32
var syncToken byte

func main() {
	done := make(chan struct{}, 2)
	go func() {
		runtime.RaceDisable()
		value = 1
		runtime.RaceRelease(unsafe.Pointer(&syncToken))
		atomic.StoreUint32(&ready, 1)
		runtime.RaceEnable()
		done <- struct{}{}
	}()
	go func() {
		for atomic.LoadUint32(&ready) == 0 {
			runtime.Gosched()
		}
		runtime.RaceAcquire(unsafe.Pointer(&syncToken))
		_ = value
		done <- struct{}{}
	}()
	<-done
	<-done
}
`,
		},
		{
			name:     "disabled-atomic-retires-release",
			wantRace: true,
			source: `package main

import (
	"runtime"
	"sync/atomic"
)

var value int
var flag uint32
var replaced uint32

func main() {
	done := make(chan struct{}, 3)
	go func() {
		value = 1
		atomic.StoreUint32(&flag, 1)
		done <- struct{}{}
	}()
	go func() {
		for atomic.LoadUint32(&flag) != 1 {
			runtime.Gosched()
		}
		runtime.RaceDisable()
		atomic.StoreUint32(&flag, 2)
		atomic.StoreUint32(&replaced, 1)
		runtime.RaceEnable()
		done <- struct{}{}
	}()
	go func() {
		for atomic.LoadUint32(&replaced) == 0 {
			runtime.Gosched()
		}
		_ = atomic.LoadUint32(&flag)
		_ = value
		done <- struct{}{}
	}()
	<-done
	<-done
	<-done
}
`,
		},
		{
			name:     "failed-cas-does-not-publish",
			wantRace: true,
			source: `package main

import (
	"sync/atomic"
)

var value int
var flag uint32 = 1

func main() {
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() {
		<-start
		value = 1
		if atomic.CompareAndSwapUint32(&flag, 2, 3) {
			panic("compare-and-swap unexpectedly succeeded")
		}
		done <- struct{}{}
	}()
	go func() {
		<-start
		_ = atomic.LoadUint32(&flag)
		_ = value
		done <- struct{}{}
	}()
	close(start)
	<-done
	<-done
}
`,
		},
		{
			name:     "unaligned-cross-word-atomic",
			wantRace: true,
			goarch:   "amd64",
			source: `package main

import (
	"sync/atomic"
	"unsafe"
)

var four [16]byte
var eight [24]byte

func main() {
	start := make(chan struct{})
	done := make(chan struct{}, 4)
	go func() {
		<-start
		atomic.StoreUint32((*uint32)(unsafe.Pointer(&four[6])), 1)
		done <- struct{}{}
	}()
	go func() {
		<-start
		_ = four[9]
		done <- struct{}{}
	}()
	go func() {
		<-start
		atomic.StoreUint64((*uint64)(unsafe.Pointer(&eight[5])), 1)
		done <- struct{}{}
	}()
	go func() {
		<-start
		_ = eight[12]
		done <- struct{}{}
	}()
	close(start)
	for range 4 {
		<-done
	}
}
`,
		},
		{
			name: "after-func-recursive-creation-handoff",
			source: `package main

import "time"

func main() {
	i := 2
	done := make(chan struct{})
	var f func()
	f = func() {
		i--
		if i >= 0 {
			time.AfterFunc(0, f)
		} else {
			close(done)
		}
	}
	time.AfterFunc(0, f)
	<-done
}
`,
		},
		{
			name: "after-func-creation-handoff",
			source: `package main

import "time"

func main() {
	done := make(chan struct{})
	value := 0
	_ = value
	time.AfterFunc(10*time.Millisecond, func() {
		value = 1
		close(done)
	})
	<-done
}
`,
		},
		{
			name: "after-func-pre-creation-write-handoff",
			source: `package main

import "time"

func main() {
	value := 0
	_ = value
	done := make(chan struct{})
	value = 2
	time.AfterFunc(time.Nanosecond, func() {
		value = 1
		close(done)
	})
	<-done
	value = 3
}
`,
		},
		{
			name: "after-func-reset-handoff",
			source: `package main

import "time"

func main() {
	value := 0
	_ = value
	done := make(chan struct{})
	timer := time.AfterFunc(time.Hour, func() {
		value = 1
		close(done)
	})
	timer.Stop()
	value = 2
	timer.Reset(time.Nanosecond)
	<-done
	value = 3
}
`,
		},
		{
			name:     "after-func-post-creation-write-races",
			wantRace: true,
			source: `package main

import "time"

func main() {
	value := 0
	_ = value
	done := make(chan struct{})
	time.AfterFunc(10*time.Millisecond, func() {
		value = 1
		close(done)
	})
	value = 2
	<-done
}
`,
		},
		{
			name:     "after-func-sibling-callbacks-race",
			wantRace: true,
			source: `package main

import "time"

func main() {
	value := 0
	_ = value
	done := make(chan struct{}, 2)
	time.AfterFunc(10*time.Millisecond, func() {
		value = 1
		done <- struct{}{}
	})
	time.AfterFunc(20*time.Millisecond, func() {
		value = 2
		done <- struct{}{}
	})
	<-done
	<-done
}
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.goarch != "" && test.goarch != runtime.GOARCH {
				t.Skipf("unaligned hardware atomic is not supported by the %s test host", runtime.GOARCH)
			}
			runPureGoProgram(t, test.source, test.wantRace)
		})
	}
}

func TestPureGoConcurrentSynctestTimers(t *testing.T) {
	const source = `package timerctx_test

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestConcurrentImmediateTimers(t *testing.T) {
	for range 10 {
		synctest.Test(t, func(t *testing.T) {
			const creators = 8
			const timersPerCreator = 8
			start := make(chan struct{})
			var callbacks sync.WaitGroup
			callbacks.Add(creators * timersPerCreator)
			for range creators {
				go func() {
					<-start
					for range timersPerCreator {
						time.AfterFunc(0, callbacks.Done)
					}
				}()
			}
			close(start)
			callbacks.Wait()
		})
	}
}
`
	runPureGoTestProgram(t, source, false)
}

func runPureGoProgram(t *testing.T, source string, wantRace bool) {
	t.Helper()
	src := filepath.Join(t.TempDir(), "main.go")
	if err := os.WriteFile(src, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := testenv.Command(t, testenv.GoToolPath(t), "run", "-race", src)
	cmd.Env = pureGoRaceEnv(cmd.Environ())
	out, err := cmd.CombinedOutput()
	gotRace := bytes.Contains(out, []byte("WARNING: DATA RACE"))
	if gotRace != wantRace {
		t.Fatalf("race result = %v, want %v (go run error: %v):\n%s", gotRace, wantRace, err, out)
	}
	if wantRace && err == nil {
		t.Fatalf("racy program exited successfully:\n%s", out)
	}
	if !wantRace && err != nil {
		t.Fatalf("non-racy program failed: %v\n%s", err, out)
	}
	if bytes.Contains(out, []byte("fatal error:")) || bytes.Contains(out, []byte("runtime: fatal")) {
		t.Fatalf("program crashed in the runtime:\n%s", out)
	}
}

func runPureGoTestProgram(t *testing.T, source string, wantRace bool) {
	t.Helper()
	src := filepath.Join(t.TempDir(), "main_test.go")
	if err := os.WriteFile(src, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := testenv.Command(t, testenv.GoToolPath(t), "test", "-race", src)
	cmd.Env = pureGoRaceEnv(cmd.Environ())
	out, err := cmd.CombinedOutput()
	gotRace := bytes.Contains(out, []byte("WARNING: DATA RACE"))
	if gotRace != wantRace {
		t.Fatalf("race result = %v, want %v (go test error: %v):\n%s", gotRace, wantRace, err, out)
	}
	if wantRace && err == nil {
		t.Fatalf("racy test exited successfully:\n%s", out)
	}
	if !wantRace && err != nil {
		t.Fatalf("non-racy test failed: %v\n%s", err, out)
	}
	if bytes.Contains(out, []byte("fatal error:")) || bytes.Contains(out, []byte("runtime: fatal")) {
		t.Fatalf("test crashed in the runtime:\n%s", out)
	}
}

func pureGoRaceEnv(env []string) []string {
	clean := make([]string, 0, len(env)+2)
	for _, value := range env {
		if strings.HasPrefix(value, "CGO_ENABLED=") ||
			strings.HasPrefix(value, "GODEBUG=") ||
			strings.HasPrefix(value, "GOMAXPROCS=") ||
			strings.HasPrefix(value, "GORACE=") {
			continue
		}
		clean = append(clean, value)
	}
	return append(clean, "CGO_ENABLED=0", "GOMAXPROCS=2")
}
