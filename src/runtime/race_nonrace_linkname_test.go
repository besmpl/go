// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime_test

import (
	"internal/testenv"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNonRaceKolkovLinknamesAbsent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping final-link symbol check in short mode")
	}
	testenv.MustHaveGoBuild(t)
	forbidden := []string{
		"kolkovIncrementErrors",
		"kolkovReportDone",
		"kolkovFramesNext",
		"kolkovPCFunctionHasPrefix",
		"kolkovGetGoid",
	}
	race0, err := os.ReadFile(filepath.Join(testenv.GOROOT(t), "src", "runtime", "race0.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, symbol := range forbidden {
		if strings.Contains(string(race0), symbol) {
			t.Errorf("non-race runtime source contains %s", symbol)
		}
	}

	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	if err := os.WriteFile(source, []byte("package main\nimport _ \"runtime/race\"\nfunc main() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "race-nonrace")
	raceBackendRun(t, 0, runtime.GOOS, runtime.GOARCH, "build", "-o", binary, source)
	nm := raceBackendRun(t, 0, runtime.GOOS, runtime.GOARCH, "tool", "nm", binary)
	for _, symbol := range forbidden {
		if strings.Contains(nm, symbol) {
			t.Errorf("non-race runtime/race test binary contains %s", symbol)
		}
	}
}
