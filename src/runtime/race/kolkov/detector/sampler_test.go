package detector

import (
	"sync"
	"sync/atomic"
	"testing"

	"runtime/race/kolkov/goroutine"
)

// createTestContext creates a RaceContext for testing.
//
//nolint:unparam // tid is parameterized for future multi-goroutine tests.
func createTestContext(tid uint32) *goroutine.RaceContext {
	return goroutine.Alloc(tid)
}

// === Sampler Unit Tests ===

func TestNewSampler_DefaultConfig(t *testing.T) {
	s := NewSampler(SamplerConfig{})

	// Rate 0 should normalize to 1
	if s.config.Rate != 1 {
		t.Errorf("Expected rate 1, got %d", s.config.Rate)
	}

	// Disabled by default
	if s.IsEnabled() {
		t.Error("Expected sampler to be disabled by default")
	}
}

func TestNewSampler_EnabledWithRate(t *testing.T) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    10,
	})

	if !s.IsEnabled() {
		t.Error("Expected sampler to be enabled")
	}

	if s.GetEffectiveRate() != 10 {
		t.Errorf("Expected rate 10, got %d", s.GetEffectiveRate())
	}
}

func TestSampler_DisabledAlwaysSamples(t *testing.T) {
	s := NewSampler(SamplerConfig{
		Enabled: false,
		Rate:    100,
	})

	// When disabled, ShouldSample should always return true
	for i := 0; i < 1000; i++ {
		if !s.ShouldSample() {
			t.Error("ShouldSample should always return true when disabled")
		}
	}
}

func TestSampler_Rate1AlwaysSamples(t *testing.T) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    1,
	})

	// Rate 1 means sample every access
	for i := 0; i < 1000; i++ {
		if !s.ShouldSample() {
			t.Error("ShouldSample should always return true with rate 1")
		}
	}
}

func TestSampler_Rate10SamplesApproximately10Percent(t *testing.T) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    10,
	})

	sampled := 0
	total := 10000

	for i := 0; i < total; i++ {
		if s.ShouldSample() {
			sampled++
		}
	}

	// With rate 10, we expect ~10% (1000 samples from 10000)
	// Allow 2% tolerance (800-1200 samples)
	expectedMin := 800
	expectedMax := 1200

	if sampled < expectedMin || sampled > expectedMax {
		t.Errorf("Expected ~1000 samples (10%%), got %d (%.1f%%)",
			sampled, float64(sampled)/float64(total)*100)
	}
}

func TestSampler_Rate100SamplesApproximately1Percent(t *testing.T) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    100,
	})

	sampled := 0
	total := 100000

	for i := 0; i < total; i++ {
		if s.ShouldSample() {
			sampled++
		}
	}

	// With rate 100, we expect ~1% (1000 samples from 100000)
	// Allow 0.2% tolerance (800-1200 samples)
	expectedMin := 800
	expectedMax := 1200

	if sampled < expectedMin || sampled > expectedMax {
		t.Errorf("Expected ~1000 samples (1%%), got %d (%.2f%%)",
			sampled, float64(sampled)/float64(total)*100)
	}
}

func TestSampler_ShouldSampleWithStats(t *testing.T) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    10,
	})

	total := 1000
	sampled := 0

	for i := 0; i < total; i++ {
		if s.ShouldSampleWithStats() {
			sampled++
		}
	}

	stats := s.GetStats()

	if stats.TotalAccesses != uint64(total) {
		t.Errorf("Expected %d total accesses, got %d", total, stats.TotalAccesses)
	}

	if stats.SampledAccesses != uint64(sampled) {
		t.Errorf("Expected %d sampled accesses, got %d", sampled, stats.SampledAccesses)
	}

	if stats.SkippedAccesses != uint64(total-sampled) {
		t.Errorf("Expected %d skipped accesses, got %d", total-sampled, stats.SkippedAccesses)
	}
}

func TestSampler_ConcurrentAccess(t *testing.T) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    10,
	})

	var wg sync.WaitGroup
	var totalSampled uint64

	goroutines := 10
	iterations := 10000

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sampled := uint64(0)
			for i := 0; i < iterations; i++ {
				if s.ShouldSample() {
					sampled++
				}
			}
			atomic.AddUint64(&totalSampled, sampled)
		}()
	}

	wg.Wait()

	// With rate 10 and 100000 total accesses, expect ~10000 samples
	total := goroutines * iterations
	expectedMin := int(float64(total) * 0.08) // 8%
	expectedMax := int(float64(total) * 0.12) // 12%

	if int(totalSampled) < expectedMin || int(totalSampled) > expectedMax {
		t.Errorf("Expected ~10%% samples (%d-%d), got %d (%.1f%%)",
			expectedMin, expectedMax, totalSampled,
			float64(totalSampled)/float64(total)*100)
	}
}

func TestSampler_ExpectedDetectionRate(t *testing.T) {
	tests := []struct {
		name            string
		rate            uint64
		accessesPerRace int
		minExpected     float64
		maxExpected     float64
	}{
		{
			name:            "disabled",
			rate:            1,
			accessesPerRace: 10,
			minExpected:     1.0,
			maxExpected:     1.0,
		},
		{
			name:            "rate10_10accesses",
			rate:            10,
			accessesPerRace: 10,
			minExpected:     0.60,
			maxExpected:     0.70,
		},
		{
			name:            "rate10_100accesses",
			rate:            10,
			accessesPerRace: 100,
			minExpected:     0.99,
			maxExpected:     1.0,
		},
		{
			name:            "rate100_10accesses",
			rate:            100,
			accessesPerRace: 10,
			minExpected:     0.08,
			maxExpected:     0.12,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSampler(SamplerConfig{
				Enabled: tc.rate > 1,
				Rate:    tc.rate,
			})

			rate := s.ExpectedDetectionRate(tc.accessesPerRace)

			if rate < tc.minExpected || rate > tc.maxExpected {
				t.Errorf("Expected detection rate %.2f-%.2f, got %.2f",
					tc.minExpected, tc.maxExpected, rate)
			}
		})
	}
}

// === Detector with Sampling Tests ===

func TestNewDetectorWithOptions_DefaultNoSampling(t *testing.T) {
	d := NewDetectorWithOptions(DetectorOptions{})

	if d.IsSamplingEnabled() {
		t.Error("Expected sampling to be disabled by default")
	}

	if d.GetSampleRate() != 1 {
		t.Errorf("Expected sample rate 1, got %d", d.GetSampleRate())
	}

	if d.GetSamplerStats() != nil {
		t.Error("Expected nil sampler stats when disabled")
	}
}

func TestNewDetectorWithOptions_WithSampling(t *testing.T) {
	d := NewDetectorWithOptions(DetectorOptions{
		SamplingEnabled: true,
		SampleRate:      10,
	})

	if !d.IsSamplingEnabled() {
		t.Error("Expected sampling to be enabled")
	}

	if d.GetSampleRate() != 10 {
		t.Errorf("Expected sample rate 10, got %d", d.GetSampleRate())
	}

	stats := d.GetSamplerStats()
	if stats == nil {
		t.Error("Expected non-nil sampler stats")
	}
}

func TestNewDetector_BackwardCompatible(t *testing.T) {
	// NewDetector() should behave exactly like v0.2.0
	d := NewDetector()

	if d.IsSamplingEnabled() {
		t.Error("NewDetector() should not enable sampling")
	}

	// Verify detection still works
	ctx := createTestContext(1)
	d.OnWrite(0x1234, ctx, 0)
	d.OnRead(0x1234, ctx, 0)

	// No race with same goroutine
	if d.RacesDetected() != 0 {
		t.Error("Should not detect race within same goroutine")
	}
}

// === Benchmarks ===

// BenchmarkSampler_ShouldSample_Disabled measures overhead when disabled.
// Target: <1ns (branch prediction should optimize this).
func BenchmarkSampler_ShouldSample_Disabled(b *testing.B) {
	s := NewSampler(SamplerConfig{
		Enabled: false,
		Rate:    10,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ShouldSample()
	}
}

// BenchmarkSampler_ShouldSample_Enabled measures overhead when enabled.
// Target: <10ns (atomic increment + modulo).
func BenchmarkSampler_ShouldSample_Enabled(b *testing.B) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    10,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ShouldSample()
	}
}

// BenchmarkSampler_ShouldSample_Enabled_Concurrent measures concurrent overhead.
func BenchmarkSampler_ShouldSample_Enabled_Concurrent(b *testing.B) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    10,
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = s.ShouldSample()
		}
	})
}

// BenchmarkSampler_ShouldSampleWithStats measures stats tracking overhead.
func BenchmarkSampler_ShouldSampleWithStats(b *testing.B) {
	s := NewSampler(SamplerConfig{
		Enabled: true,
		Rate:    10,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.ShouldSampleWithStats()
	}
}

// BenchmarkDetector_OnWrite_NoSampling baseline without sampling.
func BenchmarkDetector_OnWrite_NoSampling(b *testing.B) {
	d := NewDetector()
	ctx := createTestContext(1)
	addr := uintptr(0x1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWrite(addr+uintptr(i%1000), ctx, 0)
	}
}

// BenchmarkDetector_OnWrite_WithSampling_Rate10 measures overhead with sampling.
func BenchmarkDetector_OnWrite_WithSampling_Rate10(b *testing.B) {
	d := NewDetectorWithOptions(DetectorOptions{
		SamplingEnabled: true,
		SampleRate:      10,
	})
	ctx := createTestContext(1)
	addr := uintptr(0x1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWrite(addr+uintptr(i%1000), ctx, 0)
	}
}

// BenchmarkDetector_OnWrite_WithSampling_Rate100 measures high sampling rate.
func BenchmarkDetector_OnWrite_WithSampling_Rate100(b *testing.B) {
	d := NewDetectorWithOptions(DetectorOptions{
		SamplingEnabled: true,
		SampleRate:      100,
	})
	ctx := createTestContext(1)
	addr := uintptr(0x1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnWrite(addr+uintptr(i%1000), ctx, 0)
	}
}

// BenchmarkDetector_OnRead_NoSampling baseline without sampling.
func BenchmarkDetector_OnRead_NoSampling(b *testing.B) {
	d := NewDetector()
	ctx := createTestContext(1)
	addr := uintptr(0x1000)

	// Pre-populate shadow memory
	for i := 0; i < 1000; i++ {
		d.OnWrite(addr+uintptr(i), ctx, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnRead(addr+uintptr(i%1000), ctx, 0)
	}
}

// BenchmarkDetector_OnRead_WithSampling_Rate10 measures overhead with sampling.
func BenchmarkDetector_OnRead_WithSampling_Rate10(b *testing.B) {
	d := NewDetectorWithOptions(DetectorOptions{
		SamplingEnabled: true,
		SampleRate:      10,
	})
	ctx := createTestContext(1)
	addr := uintptr(0x1000)

	// Pre-populate shadow memory
	for i := 0; i < 1000; i++ {
		d.OnWrite(addr+uintptr(i), ctx, 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.OnRead(addr+uintptr(i%1000), ctx, 0)
	}
}
