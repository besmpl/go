//go:build race

package benchmarks

// The race-enabled comparison binaries omit g64. On high-core-count hosts it
// creates thousands of instrumented workers and can make the detector report
// against the benchmark driver's own control state instead of producing a
// sample. The lower parallelism classes still measure the same map/RWMutex
// workload. Non-race diagnostic builds retain g64.
var mapReadWriteParallelisms = [...]int{1, 4, 16}
