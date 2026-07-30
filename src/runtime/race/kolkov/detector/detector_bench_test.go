package detector

import (
	"testing"

	"runtime/race/kolkov/epoch"
	"runtime/race/kolkov/goroutine"
)

// BenchmarkOnWrite_NoRace benchmarks OnWrite in the common case (no race).
//
// This represents the typical path where writes happen in proper order
// with happens-before relationships.
//
// Target: <100ns per operation for MVP, <50ns ideal.
func BenchmarkOnWrite_NoRace(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	addr := uintptr(0x1000)

	// Setup: First write to initialize shadow cell.
	d.OnWrite(addr, ctx, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		d.OnWrite(addr, ctx, 0)
	}
}

// BenchmarkOnWrite_NoRace_NewAddress benchmarks OnWrite for addresses
// that haven't been accessed before (cold path with allocation).
//
// This measures the overhead of creating new shadow cells.
func BenchmarkOnWrite_NoRace_NewAddress(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	baseAddr := uintptr(0x10000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Each iteration writes to a new address (cold path).
		addr := baseAddr + uintptr(i*8)
		d.OnWrite(addr, ctx, 0)
	}
}

// BenchmarkOnWrite_SameEpoch benchmarks the same-epoch fast path.
//
// This is the CRITICAL optimization path that handles 71% of writes
// according to the FastTrack paper.
//
// Target: <20ns per operation (just a comparison and early return).
func BenchmarkOnWrite_SameEpoch(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	const (
		addr = uintptr(0x2000)
		pc   = uintptr(0x2001)
	)

	// Setup: Write once to create shadow cell.
	d.OnWrite(addr, ctx, pc)

	// Get shadow cell and manually set it to current epoch.
	vs := d.shadowMemory.Get(addr)
	vs.SetW(ctx.GetEpoch())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// This should hit the same-epoch fast path every time.
		// Note: We need to manually maintain vs.GetW() == currentEpoch
		// because IncrementClock advances the epoch.
		currentEpoch := ctx.GetEpoch()
		vs.SetW(currentEpoch)
		d.OnWrite(addr, ctx, pc)
	}
}

// BenchmarkOnWrite_WithRace benchmarks OnWrite when races are detected.
//
// This measures the overhead of race reporting (stderr output).
// Note: This will produce race reports during benchmarking.
func BenchmarkOnWrite_WithRace(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	addr := uintptr(0x3000)

	// Setup to always trigger write-write race.
	vs := d.shadowMemory.GetOrCreate(addr)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Set previous write to future epoch to trigger race.
		vs.SetW(epoch.NewEpoch(1, 1000000))
		ctx.C.Set(1, uint32(i))
		ctx.Epoch = epoch.NewEpoch(1, uint64(i))

		d.OnWrite(addr, ctx, 0)
	}
}

// BenchmarkOnWrite_MultipleAddresses benchmarks writes to different addresses.
//
// This tests the overhead when shadow memory needs to handle many different cells.
func BenchmarkOnWrite_MultipleAddresses(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	const numAddresses = 1000
	baseAddr := uintptr(0x100000)

	// Pre-populate shadow memory.
	for i := 0; i < numAddresses; i++ {
		addr := baseAddr + uintptr(i*8)
		d.OnWrite(addr, ctx, 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Round-robin through addresses.
		addr := baseAddr + uintptr((i%numAddresses)*8)
		d.OnWrite(addr, ctx, 0)
	}
}

// BenchmarkHappensBeforeWrite benchmarks the happens-before check.
//
// This is a critical operation called on every write to check for races.
// Target: <10ns per operation.
func BenchmarkHappensBeforeWrite(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	ctx.C.Set(1, 100)

	prevWrite := epoch.NewEpoch(1, 50) // No race (50 <= 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = d.happensBeforeWrite(prevWrite, ctx)
	}
}

// BenchmarkHappensBeforeRead benchmarks the read happens-before check.
//
// Similar to write happens-before, this is on the critical path.
// Target: <10ns per operation.
func BenchmarkHappensBeforeRead(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	ctx.C.Set(1, 100)

	prevRead := epoch.NewEpoch(1, 50)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = d.happensBeforeRead(prevRead, ctx)
	}
}

// BenchmarkShadowMemoryGetOrCreate benchmarks shadow memory access.
//
// This is called on every memory access to retrieve the VarState cell.
// Performance is critical for overall detector throughput.
func BenchmarkShadowMemoryGetOrCreate(b *testing.B) {
	d := NewDetector()
	addr := uintptr(0x5000)

	// Pre-create the shadow cell (hot path).
	d.shadowMemory.GetOrCreate(addr)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = d.shadowMemory.GetOrCreate(addr)
	}
}

// BenchmarkParallelOnWrite benchmarks OnWrite under concurrent load.
//
// This tests the thread-safety overhead of concurrent writes.
func BenchmarkParallelOnWrite(b *testing.B) {
	d := NewDetector()
	baseAddr := uintptr(0x200000)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own context
		ctx := goroutine.Alloc(1) // In real usage, would have unique TIDs
		i := 0
		for pb.Next() {
			// Each goroutine writes to its own address space.
			addr := baseAddr + uintptr(i*8)
			d.OnWrite(addr, ctx, 0)
			i++
		}
	})
}

// BenchmarkReportRace benchmarks the race reporting function.
//
// This is NOT on the hot path (only called when races are found),
// but we benchmark it to understand the overhead.
func BenchmarkReportRace(b *testing.B) {
	d := NewDetector()
	addr := uintptr(0xDEADBEEF)
	prevEpoch := epoch.NewEpoch(1, 100)
	currEpoch := epoch.NewEpoch(1, 200)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		d.reportRace("benchmark-race", addr, prevEpoch, currEpoch)
	}
}

// BenchmarkReset benchmarks the detector reset operation.
//
// This is used in testing and is NOT on the hot path.
func BenchmarkReset(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)

	// Populate with some data.
	for i := 0; i < 100; i++ {
		addr := uintptr(0x10000 + i*8)
		d.OnWrite(addr, ctx, 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		d.Reset()

		// Re-populate after reset to keep benchmark consistent.
		for j := 0; j < 100; j++ {
			addr := uintptr(0x10000 + j*8)
			d.OnWrite(addr, ctx, 0)
		}
	}
}

// BenchmarkOnRead_NoRace benchmarks OnRead in the common case (no race).
//
// This represents the typical path where reads happen with proper
// happens-before relationships.
//
// Target: <100ns per operation for MVP, <50ns ideal.
func BenchmarkOnRead_NoRace(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	const (
		addr = uintptr(0x1000)
		pc   = uintptr(0x1001)
	)

	// Runtime/compiler hooks always provide the caller PC. A zero PC deliberately
	// exercises the direct-API stack-capture fallback rather than this hot path.
	d.OnRead(addr, ctx, pc)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		d.OnRead(addr, ctx, pc)
	}
}

// BenchmarkOnReadSized_CompactAlias measures the authoritative detector miss
// path for a compiler uint64 read. Runtime cache hits bypass this function; this
// benchmark guards the compact physical-alias publication itself.
func BenchmarkOnReadSized_CompactAlias(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	const (
		addr = uintptr(0x1804) // Exercise membership across two shadow words.
		pc   = uintptr(0x1801)
	)
	d.OnReadSized(addr, 8, ctx, pc)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.OnReadSized(addr, 8, ctx, pc)
	}
}

// BenchmarkOnWriteSized_CompactAlias is the write counterpart.
func BenchmarkOnWriteSized_CompactAlias(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	const (
		addr = uintptr(0x2804)
		pc   = uintptr(0x2801)
	)
	d.OnWriteSized(addr, 8, ctx, pc)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d.OnWriteSized(addr, 8, ctx, pc)
	}
}

// BenchmarkOnRead_NoRace_NewAddress benchmarks OnRead for addresses
// that haven't been accessed before (cold path with allocation).
//
// This measures the overhead of creating new shadow cells.
func BenchmarkOnRead_NoRace_NewAddress(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	baseAddr := uintptr(0x10000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Each iteration reads from a new address (cold path).
		addr := baseAddr + uintptr(i*8)
		d.OnRead(addr, ctx, 0)
	}
}

// BenchmarkOnRead_SameEpoch benchmarks the supplied-PC detector path for
// repeated reads in the same epoch. A zero PC selects the separate fallback
// stack-capture path and would obscure this path's zero-allocation contract.
// Latency includes shadow lookup, lane isolation, and read-cache publication;
// it is not just an epoch comparison and early return.
func BenchmarkOnRead_SameEpoch(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	const (
		addr = uintptr(0x2000)
		pc   = uintptr(0x2001)
	)

	// Setup: Read once to create shadow cell.
	d.OnRead(addr, ctx, pc)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		d.OnRead(addr, ctx, pc)
	}
}

// BenchmarkOnRead_WithRace benchmarks OnRead when races are detected.
//
// This measures the overhead of race reporting (stderr output).
// Note: This will produce race reports during benchmarking.
func BenchmarkOnRead_WithRace(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	addr := uintptr(0x3000)

	// Setup to always trigger write-read race.
	vs := d.shadowMemory.GetOrCreate(addr)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Set previous write to future epoch to trigger race.
		vs.SetW(epoch.NewEpoch(1, 1000000))
		ctx.C.Set(1, uint32(i))
		ctx.Epoch = epoch.NewEpoch(1, uint64(i))

		d.OnRead(addr, ctx, 0)
	}
}

// BenchmarkOnRead_MultipleAddresses benchmarks reads from different addresses.
//
// This tests the overhead when shadow memory needs to handle many different cells.
func BenchmarkOnRead_MultipleAddresses(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	const numAddresses = 1000
	baseAddr := uintptr(0x100000)

	// Pre-populate shadow memory.
	for i := 0; i < numAddresses; i++ {
		addr := baseAddr + uintptr(i*8)
		d.OnRead(addr, ctx, 0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Round-robin through addresses.
		addr := baseAddr + uintptr((i%numAddresses)*8)
		d.OnRead(addr, ctx, 0)
	}
}

// BenchmarkOnRead_AfterWrite benchmarks read after write (common pattern).
//
// This simulates the typical pattern where a variable is written then read.
func BenchmarkOnRead_AfterWrite(b *testing.B) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	addr := uintptr(0x4000)

	// Initial write to set up shadow memory.
	d.OnWrite(addr, ctx, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		d.OnRead(addr, ctx, 0)
	}
}

// BenchmarkParallelOnRead benchmarks OnRead under concurrent load.
//
// This tests the thread-safety overhead of concurrent reads.
func BenchmarkParallelOnRead(b *testing.B) {
	d := NewDetector()
	baseAddr := uintptr(0x200000)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own context
		ctx := goroutine.Alloc(1) // In real usage, would have unique TIDs
		i := 0
		for pb.Next() {
			// Each goroutine reads from its own address space.
			addr := baseAddr + uintptr(i*8)
			d.OnRead(addr, ctx, 0)
			i++
		}
	})
}

// BenchmarkParallelReadWrite benchmarks mixed reads and writes under load.
//
// This simulates real-world concurrent access patterns.
func BenchmarkParallelReadWrite(b *testing.B) {
	d := NewDetector()
	baseAddr := uintptr(0x300000)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own context
		ctx := goroutine.Alloc(1) // In real usage, would have unique TIDs
		i := 0
		for pb.Next() {
			addr := baseAddr + uintptr(i*8)
			// Alternate between reads and writes.
			if i%2 == 0 {
				d.OnRead(addr, ctx, 0)
			} else {
				d.OnWrite(addr, ctx, 0)
			}
			i++
		}
	})
}

// BenchmarkOnReadOnWrite_Comparison directly compares OnRead vs OnWrite performance.
//
// This helps verify that OnRead is as fast or faster than OnWrite.
func BenchmarkOnReadOnWrite_Comparison(b *testing.B) {
	b.Run("OnRead", func(b *testing.B) {
		d := NewDetector()
		ctx := goroutine.Alloc(1)
		addr := uintptr(0x5000)
		d.OnRead(addr, ctx, 0) // Setup

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			d.OnRead(addr, ctx, 0)
		}
	})

	b.Run("OnWrite", func(b *testing.B) {
		d := NewDetector()
		ctx := goroutine.Alloc(1)
		addr := uintptr(0x6000)
		d.OnWrite(addr, ctx, 0) // Setup

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			d.OnWrite(addr, ctx, 0)
		}
	})
}

func BenchmarkOnReadRange(b *testing.B) {
	for _, size := range []uintptr{8, 64, 1024, 64 << 10, 1 << 20} {
		b.Run(rangeBenchmarkLabel(size), func(b *testing.B) {
			d := NewDetector()
			ctx := goroutine.Alloc(1)
			const addr = uintptr(0x800000)
			d.OnReadRange(addr, size, ctx, 0x1000)

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d.OnReadRange(addr, size, ctx, 0x1000)
			}
		})
	}
}

func BenchmarkOnWriteRange(b *testing.B) {
	for _, size := range []uintptr{8, 64, 1024, 64 << 10, 1 << 20} {
		b.Run(rangeBenchmarkLabel(size), func(b *testing.B) {
			d := NewDetector()
			ctx := goroutine.Alloc(1)
			const addr = uintptr(0x900000)
			d.OnWriteRange(addr, size, ctx, 0x2000)

			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d.OnWriteRange(addr, size, ctx, 0x2000)
			}
		})
	}
}

func rangeBenchmarkLabel(size uintptr) string {
	switch size {
	case 8:
		return "8"
	case 64:
		return "64"
	case 1024:
		return "1024"
	case 64 << 10:
		return "64KiB"
	case 1 << 20:
		return "1MiB"
	default:
		return "other"
	}
}

func BenchmarkAtomicStore32(b *testing.B) {
	benchmarkAtomicStore(b, 4)
}

func BenchmarkAtomicStore64(b *testing.B) {
	benchmarkAtomicStore(b, 8)
}

func benchmarkAtomicStore(b *testing.B, size uintptr) {
	d := NewDetector()
	ctx := goroutine.Alloc(1)
	const addr = uintptr(0xa00000)
	var token AtomicToken
	d.AtomicBegin(addr, size, ctx, false, &token)
	d.AtomicEnd(addr, size, ctx, &token, 0x3000, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.AtomicBegin(addr, size, ctx, false, &token)
		d.AtomicEnd(addr, size, ctx, &token, 0x3000, true)
	}
}

func BenchmarkAtomicLoad64(b *testing.B) {
	d := NewDetector()
	writer := goroutine.Alloc(1)
	reader := goroutine.Alloc(2)
	const addr = uintptr(0xb00000)
	var token AtomicToken
	d.AtomicBegin(addr, 8, writer, false, &token)
	d.AtomicEnd(addr, 8, writer, &token, 0x4000, true)
	d.AtomicBegin(addr, 8, reader, true, &token)
	d.AtomicEnd(addr, 8, reader, &token, 0x4001, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.AtomicBegin(addr, 8, reader, true, &token)
		d.AtomicEnd(addr, 8, reader, &token, 0x4001, false)
	}
}
