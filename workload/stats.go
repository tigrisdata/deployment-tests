package workload

import (
	"sort"
	"time"
)

// WorkerResult holds per-worker benchmark results
type WorkerResult struct {
	SuccessOps    int64
	ErrorOps      int64
	Latencies     []time.Duration
	TTFBDurations []time.Duration // Time to first byte (GET only)
	TotalBytes    int64
}

// BenchmarkResult holds aggregated benchmark results
type BenchmarkResult struct {
	TestType    string
	Operation   string
	ObjectSize  int64
	Endpoint    string
	Concurrency int

	// Operation counts
	TotalOps   int64
	SuccessOps int64
	ErrorOps   int64

	// Latency metrics
	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
	P50Latency time.Duration
	P95Latency time.Duration
	P99Latency time.Duration

	// TTFB metrics (GET only)
	MinTTFB time.Duration
	MaxTTFB time.Duration
	AvgTTFB time.Duration
	P50TTFB time.Duration
	P95TTFB time.Duration
	P99TTFB time.Duration

	// Throughput metrics
	Duration       time.Duration
	TotalBytes     int64
	Throughput     float64 // bytes per second
	ThroughputMBps float64 // MB per second (convenience field)
	OpsPerSecond   float64
}

// AggregateResults combines worker results into a single benchmark result
func AggregateResults(testType, operation string, objectSize int64, endpoint string, concurrency int, workerResults []WorkerResult, duration time.Duration) *BenchmarkResult {
	var allLatencies []time.Duration
	var allTTFBDurations []time.Duration
	var totalSuccess, totalErrors, totalBytes int64

	// Collect all results from workers
	for _, wr := range workerResults {
		totalSuccess += wr.SuccessOps
		totalErrors += wr.ErrorOps
		totalBytes += wr.TotalBytes
		allLatencies = append(allLatencies, wr.Latencies...)
		allTTFBDurations = append(allTTFBDurations, wr.TTFBDurations...)
	}

	if len(allLatencies) == 0 {
		return &BenchmarkResult{
			TestType:    testType,
			Operation:   operation,
			ObjectSize:  objectSize,
			Endpoint:    endpoint,
			Concurrency: concurrency,
			TotalOps:    totalSuccess + totalErrors,
			SuccessOps:  0,
			ErrorOps:    totalErrors,
			Duration:    duration,
		}
	}

	// Sort for percentile calculations
	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})

	// Calculate latency statistics
	minLatency := allLatencies[0]
	maxLatency := allLatencies[len(allLatencies)-1]

	var totalDuration time.Duration
	for _, d := range allLatencies {
		totalDuration += d
	}
	avgLatency := totalDuration / time.Duration(len(allLatencies))

	p50Latency := percentile(allLatencies, 0.50)
	p95Latency := percentile(allLatencies, 0.95)
	p99Latency := percentile(allLatencies, 0.99)

	// Calculate TTFB statistics (GET only)
	var minTTFB, maxTTFB, avgTTFB, p50TTFB, p95TTFB, p99TTFB time.Duration
	if len(allTTFBDurations) > 0 {
		sort.Slice(allTTFBDurations, func(i, j int) bool {
			return allTTFBDurations[i] < allTTFBDurations[j]
		})

		minTTFB = allTTFBDurations[0]
		maxTTFB = allTTFBDurations[len(allTTFBDurations)-1]

		var totalTTFB time.Duration
		for _, d := range allTTFBDurations {
			totalTTFB += d
		}
		avgTTFB = totalTTFB / time.Duration(len(allTTFBDurations))

		p50TTFB = percentile(allTTFBDurations, 0.50)
		p95TTFB = percentile(allTTFBDurations, 0.95)
		p99TTFB = percentile(allTTFBDurations, 0.99)
	}

	// Calculate throughput
	throughput := float64(totalBytes) / duration.Seconds()
	opsPerSecond := float64(totalSuccess) / duration.Seconds()

	return &BenchmarkResult{
		TestType:       testType,
		Operation:      operation,
		ObjectSize:     objectSize,
		Endpoint:       endpoint,
		Concurrency:    concurrency,
		TotalOps:       totalSuccess + totalErrors,
		SuccessOps:     totalSuccess,
		ErrorOps:       totalErrors,
		MinLatency:     minLatency,
		MaxLatency:     maxLatency,
		AvgLatency:     avgLatency,
		P50Latency:     p50Latency,
		P95Latency:     p95Latency,
		P99Latency:     p99Latency,
		MinTTFB:        minTTFB,
		MaxTTFB:        maxTTFB,
		AvgTTFB:        avgTTFB,
		P50TTFB:        p50TTFB,
		P95TTFB:        p95TTFB,
		P99TTFB:        p99TTFB,
		Duration:       duration,
		TotalBytes:     totalBytes,
		Throughput:     throughput,
		ThroughputMBps: throughput / (1024 * 1024),
		OpsPerSecond:   opsPerSecond,
	}
}

// percentile calculates the p-th percentile from a sorted slice
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}

	pos := p * float64(len(sorted)-1)
	lo := int(pos)
	hi := lo + 1

	if hi >= len(sorted) {
		return sorted[lo]
	}

	// Linear interpolation
	frac := pos - float64(lo)
	loVal := float64(sorted[lo])
	hiVal := float64(sorted[hi])
	return time.Duration(loVal + frac*(hiVal-loVal))
}
