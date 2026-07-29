// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package work

import (
	"fmt"
	goversion "go/version"
	"internal/testenv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cmd/go/internal/cache"
	"cmd/go/internal/cfg"
	"cmd/go/internal/load"
)

// TestDevelopmentToolContentInvalidatesActionIDs verifies the cache-safety
// property required by a toolchain that changes compiler-generated object ABI:
// rebuilding either tool must invalidate cached compile and link actions even
// when the displayed Go version and the toolexec path stay the same.
func TestDevelopmentToolContentInvalidatesActionIDs(t *testing.T) {
	versionFile, err := os.ReadFile(filepath.Join(testenv.GOROOT(t), "VERSION"))
	if err != nil {
		t.Fatal(err)
	}
	version := strings.TrimSpace(strings.SplitN(string(versionFile), "\n", 2)[0])
	if !strings.Contains(version, "devel") {
		t.Fatalf("VERSION %q is a release identity; development tools would reuse cache entries by version", version)
	}
	if !goversion.IsValid(version) {
		t.Fatalf("VERSION %q is not a valid Go toolchain version", version)
	}

	oldToolexec := cfg.BuildToolexec
	oldToolchain := cfg.BuildToolchainName
	cfg.BuildToolexec = []string{os.Args[0], "-test.run=^TestDevelopmentToolIDHelper$", "--"}
	cfg.BuildToolchainName = "gc"
	t.Setenv("GO_WANT_DEVELOPMENT_TOOL_ID_HELPER", "1")
	t.Cleanup(func() {
		cfg.BuildToolexec = oldToolexec
		cfg.BuildToolchainName = oldToolchain
	})

	old := developmentActionIDs(t, version, "old-action/old-content")
	oldAgain := developmentActionIDs(t, version, "old-action/old-content")
	new := developmentActionIDs(t, version, "new-action/new-content")

	if old != oldAgain {
		t.Fatalf("unchanged development tools produced unstable action IDs:\nfirst  %+v\nsecond %+v", old, oldAgain)
	}
	if old.compile == new.compile {
		t.Fatal("compiler content change did not invalidate the compile action ID")
	}
	if old.link == new.link {
		t.Fatal("linker content change did not invalidate the link action ID")
	}
	if old.compileTool != "old-content" || old.linkTool != "old-content" {
		t.Fatalf("old tool IDs do not use the content ID: %+v", old)
	}
	if new.compileTool != "new-content" || new.linkTool != "new-content" {
		t.Fatalf("new tool IDs do not use the content ID: %+v", new)
	}
}

type developmentToolActionIDs struct {
	compileTool string
	linkTool    string
	compile     cache.ActionID
	link        cache.ActionID
}

func developmentActionIDs(t *testing.T, version, buildID string) developmentToolActionIDs {
	t.Helper()
	t.Setenv("GO_TOOL_ID_TEST_VERSION", version)
	t.Setenv("GO_TOOL_ID_TEST_BUILDID", buildID)

	workDir := t.TempDir() + string(filepath.Separator)
	b := &Builder{WorkDir: workDir}
	p := &load.Package{PackagePublic: load.PackagePublic{
		Dir:        workDir,
		ImportPath: "example.com/cacheidentity",
		Name:       "main",
	}}
	a := &Action{Package: p}

	return developmentToolActionIDs{
		compileTool: b.toolID("compile"),
		linkTool:    b.toolID("link"),
		compile:     b.buildActionID(a),
		link:        b.linkActionID(a),
	}
}

// TestDevelopmentToolIDHelper is re-executed through -toolexec. It models two
// rebuilds of the same development toolchain whose only visible difference is
// the content-bearing build ID printed by -V=full.
func TestDevelopmentToolIDHelper(t *testing.T) {
	if os.Getenv("GO_WANT_DEVELOPMENT_TOOL_ID_HELPER") != "1" {
		return
	}

	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) != 3 || args[2] != "-V=full" {
		fmt.Fprintf(os.Stderr, "unexpected helper arguments: %q\n", os.Args)
		os.Exit(2)
	}
	name := strings.TrimSuffix(filepath.Base(args[1]), ".exe")
	fmt.Printf("%s version %s buildID=%s\n", name, os.Getenv("GO_TOOL_ID_TEST_VERSION"), os.Getenv("GO_TOOL_ID_TEST_BUILDID"))
	os.Exit(0)
}
