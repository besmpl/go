//go:build !amd64 && !arm64

package shadowmem

// compactGroups is referenced by the architecture-independent rangeBlock
// layout. Compact page-table routing is implemented only on amd64 and arm64;
// other targets use shadow_pagetable_stub.go and need no compact operations.
type compactGroups struct{}
