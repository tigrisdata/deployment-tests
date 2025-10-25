package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tigrisdata/deployment-test/workload"
)

// PerformanceTest implements the Test interface for performance testing
type PerformanceTest struct {
	validator *TigrisValidator
}

// NewPerformanceTest creates a new performance test
func NewPerformanceTest(validator *TigrisValidator) *PerformanceTest {
	return &PerformanceTest{
		validator: validator,
	}
}

// Name returns the display name of the test
func (t *PerformanceTest) Name() string {
	return "Performance Tests"
}

// Type returns the type of test
func (t *PerformanceTest) Type() TestType {
	return TestTypePerformance
}

// Setup performs any necessary setup before running the test
func (t *PerformanceTest) Setup(ctx context.Context) error {
	return nil
}

// Cleanup performs any necessary cleanup after running the test
func (t *PerformanceTest) Cleanup(ctx context.Context) error {
	return nil
}

// Run executes the performance test
func (t *PerformanceTest) Run(ctx context.Context) TestStatus {
	startTime := time.Now()

	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" PERFORMANCE TESTS")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	allPassed := true
	details := make(map[string]interface{})

	// Test global endpoint
	endpoints := []string{"global"}

	for _, endpoint := range endpoints {
		if endpoint == "global" && t.validator.config.GlobalEndpoint == "" {
			continue
		}

		fmt.Printf("\n%sTesting Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, endpoint, ColorReset)
		fmt.Println(strings.Repeat("-", 60))

		endpointDetails := make(map[string]interface{})

		// PUT performance tests
		fmt.Println("PUT Performance Tests:")
		putResults := make([]map[string]interface{}, 0)
		for _, size := range t.validator.config.BenchmarkSizes {
			multipartInfo := ""
			if size.UseMultipart {
				multipartInfo = fmt.Sprintf(", multipart: %d MiB parts", size.MultipartSize/(1024*1024))
			}
			fmt.Printf("  Testing %s (%d records, %d ops%s)...\n", size.DisplayName, size.RecordCount, size.OpCount, multipartInfo)
			result := t.runPerformanceBenchmark("PUT", size, endpoint)

			// Display latency metrics
			fmt.Printf("    Latency    - %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s\n",
				ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgLatency),
				ColorBrightWhite, ColorReset, formatDurationAligned(result.P95Latency),
				ColorBrightWhite, ColorReset, formatDurationAligned(result.P99Latency))

			// Display throughput metrics
			if result.SuccessOps > 0 {
				if result.ErrorOps > 0 {
					fmt.Printf("    Throughput - %s%8.3f MB/s%s | %s%8.3f ops/s%s | %s%d success, %d failed%s\n",
						ColorBrightWhite, result.ThroughputMBps, ColorReset,
						ColorBrightWhite, result.OpsPerSecond, ColorReset,
						ColorBrightGreen, result.SuccessOps, result.ErrorOps, ColorReset)
				} else {
					fmt.Printf("    Throughput - %s%8.3f MB/s%s | %s%8.3f ops/s%s | %s%d success%s\n",
						ColorBrightWhite, result.ThroughputMBps, ColorReset,
						ColorBrightWhite, result.OpsPerSecond, ColorReset,
						ColorBrightGreen, result.SuccessOps, ColorReset)
				}
				putResults = append(putResults, map[string]interface{}{
					"size":           size.DisplayName,
					"success":        result.SuccessOps > 0,
					"throughputMBps": result.ThroughputMBps,
					"opsPerSecond":   result.OpsPerSecond,
					"avgLatency":     result.AvgLatency,
					"p95Latency":     result.P95Latency,
					"p99Latency":     result.P99Latency,
				})
			} else {
				fmt.Printf("    %sFAILED%s (no successful operations)\n", ColorBrightRed, ColorReset)
				allPassed = false
				putResults = append(putResults, map[string]interface{}{
					"size":    size.DisplayName,
					"success": false,
				})
			}
		}
		endpointDetails["PUT"] = putResults

		// GET performance tests (reuse objects created by PUT tests)
		fmt.Println("\nGET Performance Tests:")
		getResults := make([]map[string]interface{}, 0)
		for _, size := range t.validator.config.BenchmarkSizes {
			fmt.Printf("  Testing %s (%d records, %d ops)...\n", size.DisplayName, size.RecordCount, size.OpCount)
			result := t.runPerformanceBenchmark("GET", size, endpoint)

			// Display latency metrics
			fmt.Printf("    Latency    - %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s\n",
				ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgLatency),
				ColorBrightWhite, ColorReset, formatDurationAligned(result.P95Latency),
				ColorBrightWhite, ColorReset, formatDurationAligned(result.P99Latency))

			// Display TTFB metrics (only for GET operations)
			fmt.Printf("    TTFB       - %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s\n",
				ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgTTFB),
				ColorBrightWhite, ColorReset, formatDurationAligned(result.P95TTFB),
				ColorBrightWhite, ColorReset, formatDurationAligned(result.P99TTFB))

			// Display throughput metrics
			if result.SuccessOps > 0 {
				if result.ErrorOps > 0 {
					fmt.Printf("    Throughput - %s%8.3f MB/s%s | %s%8.3f ops/s%s | %s%d success, %d failed%s\n",
						ColorBrightWhite, result.ThroughputMBps, ColorReset,
						ColorBrightWhite, result.OpsPerSecond, ColorReset,
						ColorBrightGreen, result.SuccessOps, result.ErrorOps, ColorReset)
				} else {
					fmt.Printf("    Throughput - %s%8.3f MB/s%s | %s%8.3f ops/s%s | %s%d success%s\n",
						ColorBrightWhite, result.ThroughputMBps, ColorReset,
						ColorBrightWhite, result.OpsPerSecond, ColorReset,
						ColorBrightGreen, result.SuccessOps, ColorReset)
				}
				getResults = append(getResults, map[string]interface{}{
					"size":           size.DisplayName,
					"success":        result.SuccessOps > 0,
					"throughputMBps": result.ThroughputMBps,
					"opsPerSecond":   result.OpsPerSecond,
					"avgLatency":     result.AvgLatency,
					"p95Latency":     result.P95Latency,
					"p99Latency":     result.P99Latency,
					"avgTTFB":        result.AvgTTFB,
					"p95TTFB":        result.P95TTFB,
					"p99TTFB":        result.P99TTFB,
				})
			} else {
				fmt.Printf("    %sFAILED%s (no successful operations)\n", ColorBrightRed, ColorReset)
				allPassed = false
				getResults = append(getResults, map[string]interface{}{
					"size":    size.DisplayName,
					"success": false,
				})
			}
		}
		endpointDetails["GET"] = getResults
		details[endpoint] = endpointDetails
	}

	duration := time.Since(startTime)
	message := "All performance tests passed"
	if !allPassed {
		message = "Some performance tests failed"
	}

	return TestStatus{
		Passed:   allPassed,
		Duration: duration,
		Message:  message,
		Details:  details,
	}
}

// runPerformanceBenchmark runs a single benchmark that collects both latency and throughput metrics
func (t *PerformanceTest) runPerformanceBenchmark(operation string, size BenchmarkSize, endpoint string) workload.BenchmarkResult {
	// Determine operation type
	var opType workload.OpType
	if operation == "PUT" {
		opType = workload.OpPUT
	} else {
		opType = workload.OpGET
	}

	// Create workload configuration (YCSB-style)
	config := workload.WorkloadConfig{
		Bucket:          t.validator.config.BucketName,
		Endpoint:        endpoint,
		Prefix:          fmt.Sprintf("%s/%s-%d", t.validator.config.Prefix, operation, size.ObjectSize),
		ObjectSize:      size.ObjectSize,
		RecordCount:     size.RecordCount, // Number of records to preload
		OperationCount:  size.OpCount,     // Number of operations to run
		TestDuration:    0,                // Not used, using operation count only
		Concurrency:     t.validator.config.Concurrency,
		OperationType:   opType,
		Seed:            time.Now().UnixNano(),
		KeyDistribution: workload.DistUniform,
		ReuseObjects:    false,
		UseMultipart:    size.UseMultipart,  // Enable multipart for large objects
		MultipartSize:   size.MultipartSize, // Part size for multipart uploads
	}

	// Get S3 client for endpoint
	client := t.validator.clients[endpoint]

	// Create workload generator
	gen, err := workload.NewGenerator(config, client)
	if err != nil {
		// Handle error
		return workload.BenchmarkResult{}
	}

	// Run benchmark (includes preload for GET)
	result, err := gen.RunBenchmark(context.Background())
	if err != nil {
		// Handle error
		return workload.BenchmarkResult{}
	}

	return *result
}
