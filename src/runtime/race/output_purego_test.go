// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race && !cgo

package race_test

import (
	"bytes"
	"internal/testenv"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestPureGoOutput is a smoke test for the output contract shared by race
// detector implementations. TestOutput, which is built only with cgo, checks
// ThreadSanitizer-specific formatting and its remaining GORACE options.
func TestPureGoOutput(t *testing.T) {
	goTool := testenv.GoToolPath(t)
	list := testenv.Command(t, goTool, "list", "-race", "-f",
		`GoFiles={{join .GoFiles ","}} CgoFiles={{join .CgoFiles ","}} SysoFiles={{join .SysoFiles ","}}`,
		"runtime/race")
	list.Env = withoutRaceSelectionEnv(list.Environ())
	list.Env = append(list.Env, "CGO_ENABLED=0")
	backend, err := list.CombinedOutput()
	if err != nil {
		t.Fatalf("identify pure-Go race backend: %v\n%s", err, backend)
	}
	backendText := strings.TrimSpace(string(backend))
	if !strings.Contains(backendText, "race_kolkov_import.go") ||
		!strings.Contains(backendText, "CgoFiles= SysoFiles=") {
		t.Fatalf("pure-Go backend is not selected without cgo/TSAN objects: %s", backendText)
	}

	cleanSrc := filepath.Join(t.TempDir(), "main.go")
	if err := os.WriteFile(cleanSrc, []byte("package main\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	clean := testenv.Command(t, goTool, "run", "-race", cleanSrc)
	clean.Env = withoutRaceSelectionEnv(clean.Environ())
	clean.Env = append(clean.Env, "CGO_ENABLED=0")
	cleanOut, err := clean.CombinedOutput()
	if err != nil {
		t.Fatalf("clean program failed: %v\n%s", err, cleanOut)
	}
	if len(cleanOut) != 0 {
		t.Fatalf("clean program printed unexpected detector output:\n%s", cleanOut)
	}

	src := filepath.Join(t.TempDir(), "main.go")
	const program = `package main

var value int

func main() {
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func(v int) {
			<-start
			value = v
			done <- struct{}{}
		}(i)
	}
	close(start)
	<-done
	<-done
	println(value)
}
`
	if err := os.WriteFile(src, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := testenv.Command(t, goTool, "run", "-race", src)
	cmd.Env = withoutRaceSelectionEnv(cmd.Environ())
	cmd.Env = append(cmd.Env, "CGO_ENABLED=0", "GOMAXPROCS=2")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("racy program unexpectedly succeeded:\n%s", out)
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() == 0 {
		t.Fatalf("racy program did not exit with a nonzero status: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("WARNING: DATA RACE")) {
		t.Fatalf("racy program did not print a race warning:\n%s", out)
	}
	if bytes.Contains(out, []byte("Race Detector Report")) {
		t.Fatalf("runtime finalization printed the explicit API summary:\n%s", out)
	}
	if bytes.Contains(out, []byte("fatal error:")) || bytes.Contains(out, []byte("runtime: fatal")) {
		t.Fatalf("racy program crashed in the runtime:\n%s", out)
	}
	accessLocation := regexp.MustCompile(regexp.QuoteMeta(src) + `:[1-9][0-9]*`)
	if !accessLocation.Match(out) {
		t.Fatalf("race warning did not contain a usable access location:\n%s", out)
	}
	creationLocation := regexp.MustCompile(`(?m)Goroutine [0-9]+ \((?:running|finished)\) created at:\n  .+\n      ` +
		regexp.QuoteMeta(src) + `:[1-9][0-9]* \+0x[0-9a-f]+`)
	if !creationLocation.Match(out) {
		t.Fatalf("race warning did not contain a usable goroutine creation location:\n%s", out)
	}
}

func TestPureGoGORACEProcessControl(t *testing.T) {
	goTool := testenv.GoToolPath(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "main.go")
	bin := filepath.Join(dir, "race-process-control")
	const program = `package main

var value int

func main() {
	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		<-start
		value = 1
		close(done)
	}()
	close(start)
	value = 2
	<-done
	println("continued after race")
}
`
	if err := os.WriteFile(src, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}

	build := testenv.Command(t, goTool, "build", "-race", "-o", bin, src)
	build.Env = withoutRaceSelectionEnv(build.Environ())
	build.Env = append(build.Env, "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build pure-Go race process-control test: %v\n%s", err, out)
	}

	tests := []struct {
		name          string
		gorace        string
		wantExit      int
		wantContinued bool
	}{
		{name: "defaults", wantExit: 66, wantContinued: true},
		{name: "exitcode", gorace: "atexit_sleep_ms=0 halt_on_error=0 exitcode=13", wantExit: 13, wantContinued: true},
		{name: "halt_on_error", gorace: "atexit_sleep_ms=0 exitcode=23 halt_on_error=1", wantExit: 23},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := testenv.Command(t, bin)
			cmd.Env = withoutRaceSelectionEnv(cmd.Environ())
			if test.gorace != "" {
				cmd.Env = append(cmd.Env, "GORACE="+test.gorace)
			}
			out, err := cmd.CombinedOutput()
			exitErr, ok := err.(*exec.ExitError)
			if !ok || exitErr.ExitCode() != test.wantExit {
				t.Fatalf("exit error = %v, want status %d\n%s", err, test.wantExit, out)
			}
			if !bytes.Contains(out, []byte("WARNING: DATA RACE")) {
				t.Fatalf("missing race report:\n%s", out)
			}
			continued := bytes.Contains(out, []byte("continued after race"))
			if continued != test.wantContinued {
				t.Fatalf("continued marker present = %v, want %v:\n%s", continued, test.wantContinued, out)
			}
		})
	}
}

func withoutRaceSelectionEnv(env []string) []string {
	clean := make([]string, 0, len(env))
	for _, value := range env {
		if strings.HasPrefix(value, "CGO_ENABLED=") ||
			strings.HasPrefix(value, "GODEBUG=") ||
			strings.HasPrefix(value, "GOMAXPROCS=") ||
			strings.HasPrefix(value, "GORACE=") {
			continue
		}
		clean = append(clean, value)
	}
	return clean
}
