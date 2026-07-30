// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime_test

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"internal/testenv"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

var raceBackendPureGoArchitectures = []string{"amd64", "arm64", "loong64", "ppc64le", "riscv64", "s390x"}

func TestRaceBackendSourceSelection(t *testing.T) {
	testenv.MustHaveGoBuild(t)

	pureRuntime := raceBackendGoList(t, 0, runtime.GOOS, runtime.GOARCH, "-race", "-f", "{{join .GoFiles \" \"}}", "runtime")
	for _, want := range []string{"race_kolkov.go", "race_kolkov_atomic.go", "race_kolkov_detector.go", "race_kolkov_frames.go"} {
		if !strings.Contains(pureRuntime, want) {
			t.Errorf("PureGo runtime sources do not contain %s:\n%s", want, pureRuntime)
		}
	}
	for _, bad := range []string{"race.go", "race0.go", "race_kolkov_bridges_tsan.go"} {
		if raceBackendFileSelected(pureRuntime, bad) {
			t.Errorf("PureGo runtime unexpectedly selected %s:\n%s", bad, pureRuntime)
		}
	}

	pureRace := raceBackendGoList(t, 0, runtime.GOOS, runtime.GOARCH, "-race", "-f", "Go={{join .GoFiles \" \"}} Syso={{join .SysoFiles \" \"}} Imports={{join .Imports \" \"}}", "runtime/race")
	if !strings.Contains(pureRace, "race_kolkov_import.go") || !strings.Contains(pureRace, "Syso= Imports=runtime/race/kolkov/api") {
		t.Errorf("PureGo runtime/race selection is not stateful-Go-only:\n%s", pureRace)
	}
	apiSources := raceBackendGoList(t, 0, runtime.GOOS, runtime.GOARCH, "-race", "-f", "{{join .GoFiles \" \"}}", "runtime/race/kolkov/api")
	if !raceBackendFileSelected(apiSources, "goid_runtime.go") || raceBackendFileSelected(apiSources, "goid_runtime_norace.go") {
		t.Errorf("race API selected wrong goid implementation: %s", apiSources)
	}
	reportSources := raceBackendGoList(t, 0, runtime.GOOS, runtime.GOARCH, "-race", "-f", "{{join .GoFiles \" \"}}", "runtime/race/kolkov/detector")
	if !raceBackendFileSelected(reportSources, "report_runtime_race.go") || raceBackendFileSelected(reportSources, "report_runtime_norace.go") {
		t.Errorf("race detector selected wrong report implementation: %s", reportSources)
	}

	nonRaceRuntime := raceBackendGoList(t, 0, runtime.GOOS, runtime.GOARCH, "-f", "{{join .GoFiles \" \"}}", "runtime")
	if !raceBackendFileSelected(nonRaceRuntime, "race0.go") {
		t.Errorf("non-race runtime selected detector sources:\n%s", nonRaceRuntime)
	}
	for _, bad := range []string{"race_kolkov.go", "race_kolkov_atomic.go", "race_kolkov_detector.go", "race_kolkov_bridges_tsan.go", "race_kolkov_frames.go"} {
		if raceBackendFileSelected(nonRaceRuntime, bad) {
			t.Errorf("non-race runtime unexpectedly selected %s", bad)
		}
	}
	nonRaceAPI := raceBackendGoList(t, 0, runtime.GOOS, runtime.GOARCH, "-f", "{{join .GoFiles \" \"}}", "runtime/race/kolkov/api")
	if !raceBackendFileSelected(nonRaceAPI, "goid_runtime_norace.go") || raceBackendFileSelected(nonRaceAPI, "goid_runtime.go") {
		t.Errorf("non-race API selected wrong goid implementation: %s", nonRaceAPI)
	}

	t.Run("TSAN", func(t *testing.T) {
		testenv.MustHaveCGO(t)
		tsanRuntime := raceBackendGoList(t, 1, runtime.GOOS, runtime.GOARCH, "-race", "-f", "{{join .GoFiles \" \"}}", "runtime")
		for _, want := range []string{"race.go", "race_kolkov_bridges_tsan.go", "race_kolkov_frames.go"} {
			if !raceBackendFileSelected(tsanRuntime, want) {
				t.Errorf("TSAN runtime sources do not contain %s:\n%s", want, tsanRuntime)
			}
		}
		for _, bad := range []string{"race0.go", "race_kolkov.go", "race_kolkov_atomic.go", "race_kolkov_detector.go"} {
			if raceBackendFileSelected(tsanRuntime, bad) {
				t.Errorf("TSAN runtime unexpectedly selected %s:\n%s", bad, tsanRuntime)
			}
		}
		tsanRace := raceBackendGoList(t, 1, runtime.GOOS, runtime.GOARCH, "-race", "-f", "Go={{join .GoFiles \" \"}} Syso={{join .SysoFiles \" \"}} Imports={{join .Imports \" \"}}", "runtime/race")
		if strings.Contains(tsanRace, "race_kolkov_import.go") || strings.Contains(tsanRace, "Syso= ") {
			t.Errorf("TSAN runtime/race did not select its system object:\n%s", tsanRace)
		}
	})
}

func TestRaceBackendSupportedArchitectures(t *testing.T) {
	testenv.MustHaveGoBuild(t)
	for _, goarch := range raceBackendPureGoArchitectures {
		t.Run(goarch, func(t *testing.T) {
			selection := raceBackendGoList(t, 0, "linux", goarch, "-race", "-f", "Go={{join .GoFiles \" \"}} Syso={{join .SysoFiles \" \"}}", "runtime/race")
			if !strings.Contains(selection, "race_kolkov_import.go") || !strings.Contains(selection, "Syso=") {
				t.Fatalf("linux/%s did not select PureGo runtime/race: %s", goarch, selection)
			}
			if fields := strings.Fields(strings.TrimPrefix(selection[strings.Index(selection, "Syso="):], "Syso=")); len(fields) != 0 {
				t.Fatalf("linux/%s selected TSAN objects with cgo disabled: %s", goarch, selection)
			}
		})
	}
}

func TestRaceBackendSizedAndObjectPCABI(t *testing.T) {
	root := filepath.Join(testenv.GOROOT(t), "src", "runtime")
	files := []string{"race.go", "race_kolkov.go"}
	want := map[string]string{
		"raceReadObjectPC":   "func(t *_type, addr unsafe.Pointer, callerpc, pc uintptr)",
		"raceWriteObjectPC":  "func(t *_type, addr unsafe.Pointer, callerpc, pc uintptr)",
		"race_ReadObjectPC":  "func(t *abi.Type, addr unsafe.Pointer, callerpc, pc uintptr)",
		"race_WriteObjectPC": "func(t *abi.Type, addr unsafe.Pointer, callerpc, pc uintptr)",
		"racereadn":          "func(addr, size uintptr)",
		"racewriten":         "func(addr, size uintptr)",
	}
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, file), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := make(map[string]string)
			for _, decl := range parsed.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				if _, monitored := want[function.Name.Name]; monitored {
					got[function.Name.Name] = raceBackendFuncType(function.Type)
				}
			}
			for name, signature := range want {
				if got[name] != signature {
					t.Errorf("%s %s ABI = %q, want %q", file, name, got[name], signature)
				}
			}
		})
	}
}

func TestRaceBackendFinalSymbols(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping backend link test in short mode")
	}
	testenv.MustHaveGoBuild(t)
	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	program := []byte(`package main

var scalar uint64
var boxed any

func main() {
	scalar++
	boxed = scalar
}
`)
	if err := os.WriteFile(source, program, 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		cgo    int
		want   []string
		forbid []string
	}{
		{name: "PureGo", cgo: 0, want: []string{"runtime.racereadn", "runtime.racewriten", "runtime.raceReadObjectPC"}, forbid: []string{"__tsan", "runtime/cgo"}},
		{name: "TSAN", cgo: 1, want: []string{"__tsan", "runtime.racereadn", "runtime.racewriten", "runtime.raceReadObjectPC"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.cgo == 1 {
				testenv.MustHaveCGO(t)
			}
			binary := filepath.Join(dir, test.name)
			raceBackendRun(t, test.cgo, runtime.GOOS, runtime.GOARCH, "build", "-race", "-o", binary, source)
			nm := raceBackendRun(t, test.cgo, runtime.GOOS, runtime.GOARCH, "tool", "nm", binary)
			for _, want := range test.want {
				if !strings.Contains(nm, want) {
					t.Errorf("linked %s binary is missing %q", test.name, want)
				}
			}
			for _, bad := range test.forbid {
				if strings.Contains(nm, bad) {
					t.Errorf("linked %s binary contains forbidden %q", test.name, bad)
				}
			}
		})
	}
}

func TestRaceMathBitsGeneratedCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cross-architecture dependency proof in short mode")
	}
	testenv.MustHaveGoBuild(t)

	deps := raceBackendGoList(t, 0, runtime.GOOS, runtime.GOARCH, "-race", "-deps", "runtime/race")
	if !raceBackendLine(deps, "math/bits") {
		t.Fatal("PureGo detector dependency graph no longer contains monitored math/bits edge")
	}
	if raceBackendLine(deps, "runtime/cgo") {
		t.Fatal("PureGo detector dependency graph contains runtime/cgo")
	}

	for _, goarch := range raceBackendPureGoArchitectures {
		t.Run(goarch, func(t *testing.T) {
			export := strings.TrimSpace(raceBackendGoList(t, 0, "linux", goarch, "-race", "-export", "-f", "{{.Export}}", "math/bits"))
			nm := raceBackendRun(t, 0, "linux", goarch, "tool", "nm", export)
			for _, bad := range []string{"runtime.raceread", "runtime.racewrite"} {
				if strings.Contains(nm, bad) {
					t.Fatalf("linux/%s race-compiled math/bits archive contains generated detector call %q", goarch, bad)
				}
			}
		})
	}
}

func raceBackendFuncType(function *ast.FuncType) string {
	var result strings.Builder
	result.WriteString("func(")
	for fieldIndex, field := range function.Params.List {
		if fieldIndex != 0 {
			result.WriteString(", ")
		}
		for nameIndex, name := range field.Names {
			if nameIndex != 0 {
				result.WriteString(", ")
			}
			result.WriteString(name.Name)
		}
		if len(field.Names) != 0 {
			result.WriteByte(' ')
		}
		result.WriteString(raceBackendExpr(field.Type))
	}
	result.WriteByte(')')
	return result.String()
}

func raceBackendExpr(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name
	case *ast.StarExpr:
		return "*" + raceBackendExpr(expr.X)
	case *ast.SelectorExpr:
		return raceBackendExpr(expr.X) + "." + expr.Sel.Name
	default:
		return fmt.Sprintf("%T", expr)
	}
}

func raceBackendFileSelected(list, file string) bool {
	for _, selected := range strings.Fields(list) {
		if selected == file {
			return true
		}
	}
	return false
}

func raceBackendLine(list, line string) bool {
	for _, candidate := range strings.Split(list, "\n") {
		if strings.TrimSpace(candidate) == line {
			return true
		}
	}
	return false
}

func raceBackendGoList(t *testing.T, cgo int, goos, goarch string, args ...string) string {
	t.Helper()
	return raceBackendRun(t, cgo, goos, goarch, append([]string{"list"}, args...)...)
}

func raceBackendRun(t *testing.T, cgo int, goos, goarch string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := testenv.CommandContext(t, ctx, testenv.GoToolPath(t), args...)
	command.Env = raceBackendEnv(command.Environ(), cgo, goos, goarch)
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("go %s timed out: %v\n%s", strings.Join(args, " "), ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(bytes.TrimSpace(output))
}

func raceBackendEnv(env []string, cgo int, goos, goarch string) []string {
	clean := make([]string, 0, len(env)+3)
	for _, value := range env {
		if strings.HasPrefix(value, "CGO_ENABLED=") || strings.HasPrefix(value, "GOOS=") || strings.HasPrefix(value, "GOARCH=") || strings.HasPrefix(value, "GOFLAGS=") {
			continue
		}
		clean = append(clean, value)
	}
	return append(clean, fmt.Sprintf("CGO_ENABLED=%d", cgo), "GOOS="+goos, "GOARCH="+goarch)
}
