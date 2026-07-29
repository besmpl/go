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
	"testing"
)

func TestPureGoAtomicPointerPublicationFromNoRacePackage(t *testing.T) {
	const source = `package main

import (
	"runtime"
	"sync"
)

type value struct {
	a uintptr
	b uintptr
	c uintptr
}

func main() {
	runtime.GOMAXPROCS(2)
	var values sync.Map
	go func() {
		for i := 0; i < 10000; i++ {
			values.Store(i, &value{uintptr(i), uintptr(i + 1), uintptr(i + 2)})
		}
	}()
	var sum uintptr
	for i := 0; i < 10000; i++ {
		for {
			stored, ok := values.Load(i)
			if !ok {
				runtime.Gosched()
				continue
			}
			v := stored.(*value)
			sum += v.a + v.b + v.c
			break
		}
	}
	if sum == 0 {
		panic("unreachable")
	}
}
`
	dir := t.TempDir()
	src := filepath.Join(dir, "main.go")
	bin := filepath.Join(dir, "publication")
	if err := os.WriteFile(src, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}

	goTool := testenv.GoToolPath(t)
	build := testenv.Command(t, goTool, "build", "-race", "-o", bin, src)
	build.Env = pureGoRaceEnv(build.Environ())
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build publication program: %v\n%s", err, out)
	}

	run := testenv.Command(t, bin)
	run.Env = pureGoRaceEnv(run.Environ())
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("atomic pointer publication failed: %v\n%s", err, out)
	} else if bytes.Contains(out, []byte("WARNING: DATA RACE")) {
		t.Fatalf("atomic pointer publication reported a race:\n%s", out)
	}

	objdump := testenv.Command(t, goTool, "tool", "objdump", "-s", `internal/sync\.\(\*HashTrieMap.*\)\.Load`, bin)
	dump, err := objdump.CombinedOutput()
	if err != nil {
		t.Fatalf("objdump HashTrieMap.Load: %v\n%s", err, dump)
	}
	callsPointerMethod := bytes.Contains(dump, []byte("sync/atomic.(*Pointer"))
	if !callsPointerMethod && !bytes.Contains(dump, []byte("sync/atomic.loadPointer")) {
		t.Fatalf("HashTrieMap.Load bypasses the pure-Go atomic load seam:\n%s", dump)
	}

	if callsPointerMethod {
		objdump = testenv.Command(t, goTool, "tool", "objdump", "-s", `sync/atomic\.\(\*Pointer.*\)\.Load`, bin)
		dump, err = objdump.CombinedOutput()
		if err != nil {
			t.Fatalf("objdump atomic Pointer.Load: %v\n%s", err, dump)
		}
		if !bytes.Contains(dump, []byte("sync/atomic.loadPointer")) {
			t.Fatalf("atomic Pointer.Load bypasses the pure-Go load seam:\n%s", dump)
		}
	}

	objdump = testenv.Command(t, goTool, "tool", "objdump", "-s", `sync/atomic\.loadPointer`, bin)
	dump, err = objdump.CombinedOutput()
	if err != nil {
		t.Fatalf("objdump atomic load seam: %v\n%s", err, dump)
	}
	if !bytes.Contains(dump, []byte("sync/atomic.LoadPointer")) {
		t.Fatalf("pure-Go atomic load seam bypasses the detector entry point:\n%s", dump)
	}
}
