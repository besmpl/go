package vectorclock

import (
	"math/rand"
	"reflect"
	"testing"
)

type flatClockImage struct {
	finite  []FiniteRange
	retired []RetiredRange
}

func flattenVectorClock(vc *VectorClock) flatClockImage {
	var flat flatClockImage
	vc.RangeRuns(func(first, last, clock uint32) bool {
		flat.finite = append(flat.finite, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	vc.RangeRetired(func(first, last uint32) bool {
		flat.retired = append(flat.retired, RetiredRange{First: first, Last: last})
		return true
	})
	return flat
}

func flattenClockImage(image *clockImage) flatClockImage {
	var flat flatClockImage
	image.RangeRuns(func(first, last, clock uint32) bool {
		flat.finite = append(flat.finite, FiniteRange{First: first, Last: last, Clock: clock})
		return true
	})
	image.RangeRetired(func(first, last uint32) bool {
		flat.retired = append(flat.retired, RetiredRange{First: first, Last: last})
		return true
	})
	return flat
}

func requireExactClockImage(t *testing.T, vc *VectorClock, image *clockImage, probes []uint32) {
	t.Helper()
	want, got := flattenVectorClock(vc), flattenClockImage(image)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("image differs from logical clock\nwant: %#v\n got: %#v", want, got)
	}
	for _, tid := range probes {
		if image.Get(tid) != vc.Get(tid) || image.IsRetired(tid) != vc.IsRetired(tid) {
			t.Fatalf("TID %d: image=%d/%v clock=%d/%v", tid,
				image.Get(tid), image.IsRetired(tid), vc.Get(tid), vc.IsRetired(tid))
		}
	}
}

func TestClockImageSnapshotBaseOverlayDifferential(t *testing.T) {
	vc := New()
	vc.JoinRange(0, 1023, 3)
	vc.JoinRange(50_000, 60_000, 7)
	vc.Set(^uint32(0), 13)
	vc.RetireRange(55_000, 55_100)
	vc.Freeze()

	// These changes remain in the owned overlay and overlap both dense and
	// sparse parts of the immutable base.
	vc.Set(17, 9)
	vc.Set(59_999, 11)
	vc.Set(70_000, 15)
	vc.RetireRange(50_010, 50_020)

	probes := []uint32{0, 17, 1023, 1024, 49_999, 50_000, 50_010, 55_050,
		59_999, 60_000, 60_001, 70_000, ^uint32(0)}
	requireExactClockImage(t, vc, newClockImage(vc), probes)
	requireExactClockImage(t, vc, newClockImageFromSnapshot(vc.snapshotLogical()), probes)
}

func TestClockImageDeterministicDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(0xc10c1a6e))
	vc := New()
	tids := []uint32{0, 1, 17, 1023, 1024, 1025, 4096, 65_535,
		1 << 30, ^uint32(0) - 1, ^uint32(0)}

	for step := 0; step < 500; step++ {
		tid := tids[rng.Intn(len(tids))]
		switch rng.Intn(6) {
		case 0, 1, 2:
			vc.Set(tid, uint32(rng.Intn(20)))
		case 3:
			other := New()
			for i := 0; i < 4; i++ {
				other.Set(tids[rng.Intn(len(tids))], uint32(rng.Intn(20)+1))
			}
			vc.Join(other)
		case 4:
			if tid != 0 && tid != ^uint32(0) {
				vc.RetireRange(tid, tid)
			}
		case 5:
			vc.Freeze()
		}

		image := newClockImage(vc)
		requireExactClockImage(t, vc, image, tids)
	}
}

func TestClockImageRetirementAndHighSparseTIDs(t *testing.T) {
	vc := New()
	vc.Set(7, 2)
	vc.Set(1<<31, 3)
	vc.JoinRange(^uint32(0)-20, ^uint32(0)-10, 4)
	vc.Set(^uint32(0), 5)
	vc.RetireRange(6, 8)
	vc.RetireRange(^uint32(0)-15, ^uint32(0)-12)
	vc.RetireRange(^uint32(0), ^uint32(0))

	image := newClockImage(vc)
	probes := []uint32{6, 7, 8, 1 << 31, ^uint32(0) - 21, ^uint32(0) - 20,
		^uint32(0) - 15, ^uint32(0) - 12, ^uint32(0) - 10, ^uint32(0)}
	requireExactClockImage(t, vc, image, probes)
	if len(image.sparseRuns) == 0 || len(image.retired) != 3 {
		t.Fatalf("high-TID image layout: sparse=%d retired=%d", len(image.sparseRuns), len(image.retired))
	}
}

func TestClockImageDenseTailForContiguousCoordinates(t *testing.T) {
	const coordinates = 10_000
	ranges := make([]FiniteRange, coordinates)
	for i := range ranges {
		tid := uint32(DenseThreads + i)
		ranges[i] = FiniteRange{First: tid, Last: tid, Clock: uint32(i&1) + 1}
	}
	vc := New()
	vc.JoinCanonicalRanges(ranges)
	image := newClockImage(vc)

	if len(image.denseTail) != coordinates || len(image.sparseRuns) != 0 {
		t.Fatalf("contiguous layout: dense tail=%d sparse=%d, want %d/0",
			len(image.denseTail), len(image.sparseRuns), coordinates)
	}
	if got := image.coordinateCount(); got != coordinates {
		t.Fatalf("coordinate count = %d, want %d", got, coordinates)
	}
	for _, offset := range []uint32{0, 1, coordinates / 2, coordinates - 1} {
		tid := uint32(DenseThreads) + offset
		if got, want := image.Get(tid), offset&1+1; got != want {
			t.Fatalf("Get(%d) = %d, want %d", tid, got, want)
		}
	}
}

func TestClockImageMutationIsolationAndAppend(t *testing.T) {
	source := New()
	source.Set(1, 3)
	source.JoinRange(2000, 2100, 5)
	source.RetireRange(2050, 2060)
	source.Freeze()
	source.Set(2001, 8)

	image := newClockImage(source)
	want := flattenClockImage(image)
	source.Reset()
	source.Set(1, 99)
	source.Set(3000, 7)
	if got := flattenClockImage(image); !reflect.DeepEqual(got, want) {
		t.Fatalf("source mutation changed image\nwant: %#v\n got: %#v", want, got)
	}

	dst := New()
	image.appendTo(dst)
	if got := flattenVectorClock(dst); !reflect.DeepEqual(got, want) {
		t.Fatalf("appended image differs\nwant: %#v\n got: %#v", want, got)
	}
}

func TestClockImageReadsDoNotAllocate(t *testing.T) {
	vc := New()
	vc.JoinRange(0, 1023, 3)
	vc.JoinRange(50_000, 60_000, 7)
	vc.RetireRange(55_000, 55_100)
	image := newClockImage(vc)

	if allocs := testing.AllocsPerRun(100, func() {
		_ = image.Get(59_000)
		_ = image.IsRetired(55_050)
		image.RangeRuns(func(_, _, _ uint32) bool { return true })
		image.RangeRetired(func(_, _ uint32) bool { return true })
	}); allocs != 0 {
		t.Fatalf("clockImage read allocations = %.1f, want zero", allocs)
	}
}

var benchmarkImage *clockImage
var benchmarkImageValue uint32

func BenchmarkClockImage(b *testing.B) {
	vc := New()
	ranges := make([]FiniteRange, 10_000)
	for i := range ranges {
		tid := uint32(DenseThreads + i)
		ranges[i] = FiniteRange{First: tid, Last: tid, Clock: uint32(i&1) + 1}
	}
	vc.JoinCanonicalRanges(ranges)
	vc.RetireRange(7000, 7010)
	image := newClockImage(vc)

	b.Run("construct-10k", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkImage = newClockImage(vc)
		}
	})
	b.Run("reads-10k", func(b *testing.B) {
		b.ReportAllocs()
		value := uint32(0)
		for i := 0; i < b.N; i++ {
			value ^= image.Get(uint32(DenseThreads + i%10_000))
			image.RangeRuns(func(first, last, clock uint32) bool {
				value ^= first ^ last ^ clock
				return true
			})
		}
		benchmarkImageValue = value
	})
}
