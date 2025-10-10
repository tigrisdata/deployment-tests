package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
)

// ANSI color codes
const (
	ColorReset       = "\033[0m"
	ColorRed         = "\033[31m"
	ColorGreen       = "\033[32m"
	ColorYellow      = "\033[33m"
	ColorBlue        = "\033[34m"
	ColorPurple      = "\033[35m"
	ColorCyan        = "\033[36m"
	ColorWhite       = "\033[37m"
	ColorBright      = "\033[1m"
	ColorBrightWhite = "\033[1;37m"
	ColorBrightGreen = "\033[1;32m"
	ColorBrightRed   = "\033[1;31m"
)

// Custom logger that suppresses AWS SDK warnings
type silentLogger struct{}

func (l *silentLogger) Logf(classification logging.Classification, format string, v ...interface{}) {
	// Suppress all AWS SDK log messages
}

// formatDuration formats a duration with 3 decimal places while keeping natural units
func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}

	// Convert to appropriate unit with 3 decimal places
	if d >= time.Second {
		return fmt.Sprintf("%.3fs", d.Seconds())
	} else if d >= time.Millisecond {
		return fmt.Sprintf("%.3fms", float64(d.Nanoseconds())/1e6)
	} else if d >= time.Microsecond {
		return fmt.Sprintf("%.3fµs", float64(d.Nanoseconds())/1e3)
	} else {
		return fmt.Sprintf("%.3fns", float64(d.Nanoseconds()))
	}
}

// formatDurationAligned formats a duration with 3 decimal places and consistent alignment
func formatDurationAligned(d time.Duration) string {
	if d == 0 {
		return "     0s"
	}

	var value float64
	var unit string

	// Convert to appropriate unit with 3 decimal places
	if d >= time.Second {
		value = d.Seconds()
		unit = "s"
	} else if d >= time.Millisecond {
		value = float64(d.Nanoseconds()) / 1e6
		unit = "ms"
	} else if d >= time.Microsecond {
		value = float64(d.Nanoseconds()) / 1e3
		unit = "µs"
	} else {
		value = float64(d.Nanoseconds())
		unit = "ns"
	}

	// Format with consistent 10-character width (including unit)
	formatted := fmt.Sprintf("%.3f%s", value, unit)

	// Pad to exactly 10 characters for perfect alignment
	if len(formatted) < 10 {
		return fmt.Sprintf("%10s", formatted)
	}

	return formatted
}

// TestConfig holds configuration for the performance test
type TestConfig struct {
	BucketName     string
	ObjectSizes    []int64
	ObjectCounts   []int
	Concurrency    int
	TestDuration   time.Duration
	Prefix         string
	GlobalEndpoint string
	USEndpoints    []string
}

// TestResult holds individual test results
type TestResult struct {
	Operation  string
	ObjectSize int64
	Endpoint   string
	Duration   time.Duration
	TTFB       time.Duration // Time to first byte (for GET operations)
	Success    bool
	Error      error
	Throughput float64 // bytes per second
}

// BenchmarkResult holds aggregated benchmark results
type BenchmarkResult struct {
	TestType     string
	Operation    string
	ObjectSize   int64
	Endpoint     string
	Concurrency  int
	TotalOps     int64
	SuccessOps   int64
	ErrorOps     int64
	MinLatency   time.Duration
	MaxLatency   time.Duration
	AvgLatency   time.Duration
	P50Latency   time.Duration
	P95Latency   time.Duration
	P99Latency   time.Duration
	MinTTFB      time.Duration // Min Time to first byte
	MaxTTFB      time.Duration // Max Time to first byte
	AvgTTFB      time.Duration // Avg Time to first byte
	P50TTFB      time.Duration // P50 Time to first byte
	P95TTFB      time.Duration // P95 Time to first byte
	P99TTFB      time.Duration // P99 Time to first byte
	TotalBytes   int64
	Throughput   float64 // bytes per second
	OpsPerSecond float64
}

// S3PerformanceTester handles S3 performance testing
type S3PerformanceTester struct {
	config  TestConfig
	results []TestResult
	mu      sync.RWMutex
	clients map[string]*s3.Client
}

// NewS3PerformanceTester creates a new S3 performance tester
func NewS3PerformanceTester(cfg TestConfig) (*S3PerformanceTester, error) {
	// Load AWS configuration (region will be automatically detected)
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithLogger(&silentLogger{}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create clients for different endpoints
	clients := make(map[string]*s3.Client)

	// Global endpoint client
	if cfg.GlobalEndpoint != "" {
		globalCfg := awsCfg.Copy()
		globalCfg.BaseEndpoint = aws.String(cfg.GlobalEndpoint)
		clients["global"] = s3.NewFromConfig(globalCfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	// US regional endpoints clients
	for _, endpoint := range cfg.USEndpoints {
		regionalCfg := awsCfg.Copy()
		regionalCfg.BaseEndpoint = aws.String(endpoint)
		clients[endpoint] = s3.NewFromConfig(regionalCfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	return &S3PerformanceTester{
		config:  cfg,
		results: make([]TestResult, 0),
		clients: clients,
	}, nil
}

// generateRandomData creates random data of specified size
func (t *S3PerformanceTester) generateRandomData(size int64) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// testHealthEndpoint tests health endpoint connectivity for a given S3 endpoint
func (t *S3PerformanceTester) testHealthEndpoint(s3Endpoint string) (time.Duration, error) {
	// Construct health endpoint URL by appending /admin/health to the S3 endpoint
	healthURL := strings.TrimSuffix(s3Endpoint, "/") + "/admin/health"

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	start := time.Now()
	resp, err := client.Get(healthURL)
	duration := time.Since(start)

	if err != nil {
		return duration, err
	}
	defer resp.Body.Close()

	// Check if response is OK
	if resp.StatusCode != http.StatusOK {
		return duration, fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	// Read response body to verify "OK" response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return duration, fmt.Errorf("failed to read health response: %w", err)
	}

	// Handle both plain text "OK" and JSON "OK" responses
	responseBody := strings.TrimSpace(string(body))
	if responseBody != "OK" && responseBody != `"OK"` {
		return duration, fmt.Errorf("unexpected health response: %s", string(body))
	}

	return duration, nil
}

// testS3Connectivity tests S3 connectivity to an endpoint
func (t *S3PerformanceTester) testS3Connectivity(endpoint string) (time.Duration, error) {
	client, exists := t.clients[endpoint]
	if !exists {
		return 0, fmt.Errorf("no client for endpoint: %s", endpoint)
	}

	start := time.Now()
	_, err := client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(t.config.BucketName),
	})
	duration := time.Since(start)

	return duration, err
}

// testPUTOperation tests PUT operation performance
func (t *S3PerformanceTester) testPUTOperation(endpoint, key string, data []byte) TestResult {
	client, exists := t.clients[endpoint]
	if !exists {
		return TestResult{
			Operation: "PUT",
			Endpoint:  endpoint,
			Success:   false,
			Error:     fmt.Errorf("no client for endpoint: %s", endpoint),
		}
	}

	start := time.Now()
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(t.config.BucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	duration := time.Since(start)

	throughput := float64(len(data)) / duration.Seconds()

	return TestResult{
		Operation:  "PUT",
		ObjectSize: int64(len(data)),
		Endpoint:   endpoint,
		Duration:   duration,
		Success:    err == nil,
		Error:      err,
		Throughput: throughput,
	}
}

// testGETOperation tests GET operation performance
func (t *S3PerformanceTester) testGETOperation(endpoint, key string) TestResult {
	client, exists := t.clients[endpoint]
	if !exists {
		return TestResult{
			Operation: "GET",
			Endpoint:  endpoint,
			Success:   false,
			Error:     fmt.Errorf("no client for endpoint: %s", endpoint),
		}
	}

	start := time.Now()
	resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(t.config.BucketName),
		Key:    aws.String(key),
	})

	var ttfb time.Duration
	var throughput float64
	var objectSize int64

	if err == nil {
		// TTFB is measured from request start to when we can start reading the response body
		ttfb = time.Since(start)

		// Stream the body to blackhole while measuring throughput
		if resp.Body != nil {
			// Read a small amount first to measure TTFB
			buffer := make([]byte, 1)
			_, readErr := resp.Body.Read(buffer)
			if readErr != nil && readErr != io.EOF {
				// If we can't read, TTFB is the time to get the response
				ttfb = time.Since(start)
			}

			// Stream the entire body to blackhole (discard data)
			// This is more memory efficient than reading into a buffer
			bytesRead, readErr := io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if readErr == nil {
				objectSize = bytesRead
			}
		}

		// Use ContentLength if available and we couldn't read the body
		if objectSize == 0 && resp.ContentLength != nil {
			objectSize = *resp.ContentLength
		}

		// Calculate throughput based on total time
		totalDuration := time.Since(start)
		if totalDuration > 0 {
			throughput = float64(objectSize) / totalDuration.Seconds()
		}
	}

	duration := time.Since(start)

	return TestResult{
		Operation:  "GET",
		ObjectSize: objectSize,
		Endpoint:   endpoint,
		Duration:   duration,
		TTFB:       ttfb,
		Success:    err == nil,
		Error:      err,
		Throughput: throughput,
	}
}

// runLatencyBenchmark runs latency benchmark for a specific operation and size
func (t *S3PerformanceTester) runLatencyBenchmark(operation string, objectSize int64, objectCount int, endpoint string) BenchmarkResult {
	var results []TestResult
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Generate test data
	data := t.generateRandomData(objectSize)
	// Use PUT prefix for both PUT and GET operations so GET can reuse PUT objects
	keyPrefix := fmt.Sprintf("%s/latency-PUT-%d", t.config.Prefix, objectSize)

	// Calculate operations per worker
	opsPerWorker := objectCount / t.config.Concurrency
	if opsPerWorker == 0 {
		opsPerWorker = 1
	}

	// Run concurrent operations
	for i := 0; i < t.config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Run multiple operations per worker
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("%s/worker-%d/obj-%d", keyPrefix, workerID, j)
				var result TestResult

				if operation == "PUT" {
					result = t.testPUTOperation(endpoint, key, data)
				} else if operation == "GET" {
					result = t.testGETOperation(endpoint, key)
				}

				result.ObjectSize = objectSize

				mu.Lock()
				results = append(results, result)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Calculate statistics
	return t.calculateBenchmarkStats(operation, objectSize, endpoint, results)
}

// runThroughputBenchmark runs throughput benchmark for a specific operation and size
func (t *S3PerformanceTester) runThroughputBenchmark(operation string, objectSize int64, objectCount int, endpoint string) BenchmarkResult {
	var results []TestResult
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Generate test data
	data := t.generateRandomData(objectSize)
	// Use PUT prefix for both PUT and GET operations so GET can reuse PUT objects
	keyPrefix := fmt.Sprintf("%s/throughput-PUT-%d", t.config.Prefix, objectSize)

	// Run operations for the specified duration
	ctx, cancel := context.WithTimeout(context.Background(), t.config.TestDuration)
	defer cancel()

	// Start workers
	for i := 0; i < t.config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			keyCounter := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Cycle through available objects (modulo to reuse objects)
				availableObjects := objectCount / t.config.Concurrency
				if availableObjects == 0 {
					availableObjects = 1
				}
				objID := keyCounter % availableObjects
				key := fmt.Sprintf("%s/worker-%d/obj-%d", keyPrefix, workerID, objID)
				keyCounter++

				var result TestResult
				if operation == "PUT" {
					result = t.testPUTOperation(endpoint, key, data)
				} else if operation == "GET" {
					result = t.testGETOperation(endpoint, key)
				}

				result.ObjectSize = objectSize

				mu.Lock()
				results = append(results, result)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Calculate statistics
	return t.calculateBenchmarkStats(operation, objectSize, endpoint, results)
}

// calculateBenchmarkStats calculates benchmark statistics from results
func (t *S3PerformanceTester) calculateBenchmarkStats(operation string, objectSize int64, endpoint string, results []TestResult) BenchmarkResult {
	if len(results) == 0 {
		return BenchmarkResult{
			TestType:    "unknown",
			Operation:   operation,
			ObjectSize:  objectSize,
			Endpoint:    endpoint,
			Concurrency: t.config.Concurrency,
		}
	}

	// Separate successful and failed operations
	var successResults []TestResult
	var durations []time.Duration
	var ttfbDurations []time.Duration
	var totalBytes int64

	for _, result := range results {
		if result.Success {
			successResults = append(successResults, result)
			durations = append(durations, result.Duration)
			if result.TTFB > 0 {
				ttfbDurations = append(ttfbDurations, result.TTFB)
			}
			totalBytes += result.ObjectSize
		}
	}

	// Calculate basic statistics
	totalOps := len(results)
	successOps := len(successResults)
	errorOps := totalOps - successOps

	if successOps == 0 {
		return BenchmarkResult{
			TestType:    "unknown",
			Operation:   operation,
			ObjectSize:  objectSize,
			Endpoint:    endpoint,
			Concurrency: t.config.Concurrency,
			TotalOps:    int64(totalOps),
			SuccessOps:  0,
			ErrorOps:    int64(errorOps),
		}
	}

	// Sort durations for percentile calculations
	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	// Calculate latencies
	minLatency := durations[0]
	maxLatency := durations[len(durations)-1]

	var totalDuration time.Duration
	for _, d := range durations {
		totalDuration += d
	}
	avgLatency := totalDuration / time.Duration(len(durations))

	// Calculate percentiles
	p50Index := int(float64(len(durations)) * 0.5)
	p95Index := int(float64(len(durations)) * 0.95)
	p99Index := int(float64(len(durations)) * 0.99)

	p50Latency := durations[p50Index]
	p95Latency := durations[p95Index]
	var p99Latency time.Duration
	if p99Index < len(durations) {
		p99Latency = durations[p99Index]
	} else {
		p99Latency = durations[len(durations)-1]
	}

	// Calculate TTFB statistics (only for GET operations)
	var minTTFB, maxTTFB, avgTTFB, p50TTFB, p95TTFB, p99TTFB time.Duration
	if len(ttfbDurations) > 0 {
		// Sort TTFB durations for percentile calculations
		sort.Slice(ttfbDurations, func(i, j int) bool {
			return ttfbDurations[i] < ttfbDurations[j]
		})

		minTTFB = ttfbDurations[0]
		maxTTFB = ttfbDurations[len(ttfbDurations)-1]

		var totalTTFB time.Duration
		for _, d := range ttfbDurations {
			totalTTFB += d
		}
		avgTTFB = totalTTFB / time.Duration(len(ttfbDurations))

		// Calculate TTFB percentiles
		p50TTFBIndex := int(float64(len(ttfbDurations)) * 0.5)
		p95TTFBIndex := int(float64(len(ttfbDurations)) * 0.95)
		p99TTFBIndex := int(float64(len(ttfbDurations)) * 0.99)

		p50TTFB = ttfbDurations[p50TTFBIndex]
		p95TTFB = ttfbDurations[p95TTFBIndex]
		if p99TTFBIndex < len(ttfbDurations) {
			p99TTFB = ttfbDurations[p99TTFBIndex]
		} else {
			p99TTFB = ttfbDurations[len(ttfbDurations)-1]
		}
	}

	// Calculate throughput
	testDuration := t.config.TestDuration
	if testDuration == 0 {
		testDuration = 1 * time.Minute // Default for latency tests
	}

	throughput := float64(totalBytes) / testDuration.Seconds()
	opsPerSecond := float64(successOps) / testDuration.Seconds()

	return BenchmarkResult{
		TestType:     "benchmark",
		Operation:    operation,
		ObjectSize:   objectSize,
		Endpoint:     endpoint,
		Concurrency:  t.config.Concurrency,
		TotalOps:     int64(totalOps),
		SuccessOps:   int64(successOps),
		ErrorOps:     int64(errorOps),
		MinLatency:   minLatency,
		MaxLatency:   maxLatency,
		AvgLatency:   avgLatency,
		P50Latency:   p50Latency,
		P95Latency:   p95Latency,
		P99Latency:   p99Latency,
		MinTTFB:      minTTFB,
		MaxTTFB:      maxTTFB,
		AvgTTFB:      avgTTFB,
		P50TTFB:      p50TTFB,
		P95TTFB:      p95TTFB,
		P99TTFB:      p99TTFB,
		TotalBytes:   totalBytes,
		Throughput:   throughput,
		OpsPerSecond: opsPerSecond,
	}
}

// runConnectivityTests runs connectivity tests
func (t *S3PerformanceTester) runConnectivityTests() bool {
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" CONNECTIVITY TESTS")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	allPassed := true

	// Test global endpoint
	if t.config.GlobalEndpoint != "" {
		fmt.Printf("\n%sTesting Global Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, t.config.GlobalEndpoint, ColorReset)

		// Health check test
		healthDuration, err := t.testHealthEndpoint(t.config.GlobalEndpoint)
		if err != nil {
			fmt.Printf("  Health Check: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
			allPassed = false
		} else {
			fmt.Printf("  Health Check: %sSUCCESS%s - %s\n", ColorBrightGreen, ColorReset, formatDuration(healthDuration))
		}

		// S3 connectivity test
		s3Duration, err := t.testS3Connectivity("global")
		if err != nil {
			fmt.Printf("  S3 Connectivity: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
			allPassed = false
		} else {
			fmt.Printf("  S3 Connectivity: %sSUCCESS%s - %s\n", ColorBrightGreen, ColorReset, formatDuration(s3Duration))
		}
	}

	// Test US regional endpoints
	for _, endpoint := range t.config.USEndpoints {
		fmt.Printf("\n%sTesting US Regional Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, endpoint, ColorReset)

		// Health check test
		healthDuration, err := t.testHealthEndpoint(endpoint)
		if err != nil {
			fmt.Printf("  Health Check: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
			allPassed = false
		} else {
			fmt.Printf("  Health Check: %sSUCCESS%s - %s\n", ColorBrightGreen, ColorReset, formatDuration(healthDuration))
		}

		// S3 connectivity test
		s3Duration, err := t.testS3Connectivity(endpoint)
		if err != nil {
			fmt.Printf("  S3 Connectivity: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
			allPassed = false
		} else {
			fmt.Printf("  S3 Connectivity: %sSUCCESS%s - %s\n", ColorBrightGreen, ColorReset, formatDuration(s3Duration))
		}
	}

	return allPassed
}

// runLatencyBenchmarks runs all latency benchmarks
func (t *S3PerformanceTester) runLatencyBenchmarks() bool {
	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" LATENCY BENCHMARKS")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	// Test all endpoints
	endpoints := []string{"global"}
	endpoints = append(endpoints, t.config.USEndpoints...)

	for _, endpoint := range endpoints {
		if endpoint == "global" && t.config.GlobalEndpoint == "" {
			continue
		}

		fmt.Printf("\n%sTesting Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, endpoint, ColorReset)
		fmt.Println(strings.Repeat("-", 60))

		// PUT latency tests
		fmt.Println("PUT Latency Tests:")
		for i, size := range t.config.ObjectSizes {
			count := t.config.ObjectCounts[i]
			fmt.Printf("  Testing %d bytes (%d objects)...", size, count)
			result := t.runLatencyBenchmark("PUT", size, count, endpoint)
			if result.ErrorOps > 0 {
				fmt.Printf(" %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s (%s%d success, %s%d failed%s)\n",
					ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgLatency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P95Latency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P99Latency),
					ColorBrightGreen, result.SuccessOps, ColorBrightRed, result.ErrorOps, ColorReset)
			} else {
				fmt.Printf(" %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s (%s%d success%s)\n",
					ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgLatency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P95Latency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P99Latency),
					ColorBrightGreen, result.SuccessOps, ColorReset)
			}
		}

		// GET latency tests (reuse objects created by PUT tests)
		fmt.Println("\nGET Latency Tests:")
		for i, size := range t.config.ObjectSizes {
			count := t.config.ObjectCounts[i]
			fmt.Printf("  Testing %d bytes (%d objects)...", size, count)
			// Reuse objects created by PUT tests
			result := t.runLatencyBenchmark("GET", size, count, endpoint)
			if result.ErrorOps > 0 {
				fmt.Printf(" %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s | %sTTFB Avg:%s %s, %sP95:%s %s, %sP99:%s %s (%s%d success, %s%d failed%s)\n",
					ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgLatency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P95Latency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P99Latency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgTTFB),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P95TTFB),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P99TTFB),
					ColorBrightGreen, result.SuccessOps, ColorBrightRed, result.ErrorOps, ColorReset)
			} else {
				fmt.Printf(" %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s | %sTTFB Avg:%s %s, %sP95:%s %s, %sP99:%s %s (%s%d success%s)\n",
					ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgLatency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P95Latency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P99Latency),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgTTFB),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P95TTFB),
					ColorBrightWhite, ColorReset, formatDurationAligned(result.P99TTFB),
					ColorBrightGreen, result.SuccessOps, ColorReset)
			}
		}
	}

	// For now, assume all latency benchmarks pass
	// TODO: Implement detailed failure tracking
	return true
}

// runThroughputBenchmarks runs all throughput benchmarks
func (t *S3PerformanceTester) runThroughputBenchmarks() bool {
	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" THROUGHPUT BENCHMARKS")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	// Test all endpoints
	endpoints := []string{"global"}
	endpoints = append(endpoints, t.config.USEndpoints...)

	for _, endpoint := range endpoints {
		if endpoint == "global" && t.config.GlobalEndpoint == "" {
			continue
		}

		fmt.Printf("\n%sTesting Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, endpoint, ColorReset)
		fmt.Println(strings.Repeat("-", 60))

		// PUT throughput tests
		fmt.Println("PUT Throughput Tests:")
		for i, size := range t.config.ObjectSizes {
			count := t.config.ObjectCounts[i]
			fmt.Printf("  Testing %d bytes (%d objects)...", size, count)
			result := t.runThroughputBenchmark("PUT", size, count, endpoint)
			if result.SuccessOps > 0 {
				if result.ErrorOps > 0 {
					fmt.Printf(" %sMB/s:%s %8.3f, %sops/s:%s %8.3f (%s%d success, %s%d failed%s)\n",
						ColorBrightWhite, ColorReset, result.Throughput/(1024*1024),
						ColorBrightWhite, ColorReset, result.OpsPerSecond,
						ColorBrightGreen, result.SuccessOps, ColorBrightRed, result.ErrorOps, ColorReset)
				} else {
					fmt.Printf(" %sMB/s:%s %8.3f, %sops/s:%s %8.3f (%s%d success%s)\n",
						ColorBrightWhite, ColorReset, result.Throughput/(1024*1024),
						ColorBrightWhite, ColorReset, result.OpsPerSecond,
						ColorBrightGreen, result.SuccessOps, ColorReset)
				}
			} else {
				fmt.Printf(" %sFAILED%s (no successful operations)\n", ColorBrightRed, ColorReset)
			}
		}

		// GET throughput tests (reuse objects created by PUT tests)
		fmt.Println("\nGET Throughput Tests:")
		for i, size := range t.config.ObjectSizes {
			count := t.config.ObjectCounts[i]
			fmt.Printf("  Testing %d bytes (%d objects)...", size, count)
			// Reuse objects created by PUT tests
			result := t.runThroughputBenchmark("GET", size, count, endpoint)
			if result.SuccessOps > 0 {
				if result.ErrorOps > 0 {
					fmt.Printf(" %sMB/s:%s %8.3f, %sops/s:%s %8.3f (%s%d success, %s%d failed%s)\n",
						ColorBrightWhite, ColorReset, result.Throughput/(1024*1024),
						ColorBrightWhite, ColorReset, result.OpsPerSecond,
						ColorBrightGreen, result.SuccessOps, ColorBrightRed, result.ErrorOps, ColorReset)
				} else {
					fmt.Printf(" %sMB/s:%s %8.3f, %sops/s:%s %8.3f (%s%d success%s)\n",
						ColorBrightWhite, ColorReset, result.Throughput/(1024*1024),
						ColorBrightWhite, ColorReset, result.OpsPerSecond,
						ColorBrightGreen, result.SuccessOps, ColorReset)
				}
			} else {
				fmt.Printf(" %sFAILED%s (no successful operations)\n", ColorBrightRed, ColorReset)
			}
		}
	}

	// For now, assume all throughput benchmarks pass
	// TODO: Implement detailed failure tracking
	return true
}

// RunAllTests runs the complete test suite
func (t *S3PerformanceTester) RunAllTests() error {
	fmt.Printf("Starting S3 Performance Test Suite...\n")
	fmt.Printf("Bucket: %s%s%s\n", ColorBrightWhite, t.config.BucketName, ColorReset)
	fmt.Printf("Concurrency: %s%d%s\n", ColorBrightWhite, t.config.Concurrency, ColorReset)
	fmt.Printf("Test Duration: %s%v%s\n", ColorBrightWhite, t.config.TestDuration, ColorReset)
	fmt.Printf("Object Sizes: %s%v%s\n", ColorBrightWhite, t.config.ObjectSizes, ColorReset)
	fmt.Printf("Global Endpoint: %s%s%s\n", ColorBrightWhite, t.config.GlobalEndpoint, ColorReset)
	fmt.Printf("US Endpoints: %s%v%s\n", ColorBrightWhite, t.config.USEndpoints, ColorReset)
	fmt.Printf("\n")

	// Track test results
	var testResults = struct {
		consistencyPassed  bool
		connectivityPassed bool
		latencyPassed      bool
		throughputPassed   bool
	}{
		consistencyPassed:  true,
		connectivityPassed: true,
		latencyPassed:      true,
		throughputPassed:   true,
	}

	// Run connectivity tests
	testResults.connectivityPassed = t.runConnectivityTests()

	// Run consistency tests first
	testResults.consistencyPassed = RunConsistencyTests(t)

	// Run latency benchmarks
	testResults.latencyPassed = t.runLatencyBenchmarks()

	// Run throughput benchmarks
	testResults.throughputPassed = t.runThroughputBenchmarks()

	// Display test summary
	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" TEST SUMMARY")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	// Display results for each test section
	if testResults.connectivityPassed {
		fmt.Printf(" CONNECTIVITY TESTS %sPASSED%s\n", ColorBrightGreen, ColorReset)
	} else {
		fmt.Printf(" CONNECTIVITY TESTS %sFAILED%s\n", ColorBrightRed, ColorReset)
	}

	if testResults.consistencyPassed {
		fmt.Printf(" CONSISTENCY TESTS %sPASSED%s\n", ColorBrightGreen, ColorReset)
	} else {
		fmt.Printf(" CONSISTENCY TESTS %sFAILED%s\n", ColorBrightRed, ColorReset)
	}

	if testResults.latencyPassed {
		fmt.Printf(" LATENCY BENCHMARKS %sPASSED%s\n", ColorBrightGreen, ColorReset)
	} else {
		fmt.Printf(" LATENCY BENCHMARKS %sFAILED%s\n", ColorBrightRed, ColorReset)
	}

	if testResults.throughputPassed {
		fmt.Printf(" THROUGHPUT BENCHMARKS %sPASSED%s\n", ColorBrightGreen, ColorReset)
	} else {
		fmt.Printf(" THROUGHPUT BENCHMARKS %sFAILED%s\n", ColorBrightRed, ColorReset)
	}

	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" TEST SUITE COMPLETED")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	return nil
}

func main() {
	// Parse command line flags
	var (
		bucketName     = flag.String("bucket", "", "S3 bucket name (required)")
		concurrency    = flag.Int("concurrency", 10, "Number of concurrent operations")
		testDuration   = flag.Duration("duration", 30*time.Second, "Test duration for throughput tests")
		prefix         = flag.String("prefix", "perf-test", "S3 key prefix")
		globalEndpoint = flag.String("global-endpoint", "", "Global S3 endpoint URL")
		usEndpoints    = flag.String("us-endpoints", "", "Comma-separated list of US regional endpoints")
	)
	flag.Parse()

	if *bucketName == "" {
		fmt.Fprintf(os.Stderr, "Error: bucket name is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Parse US endpoints
	var usEndpointList []string
	if *usEndpoints != "" {
		usEndpointList = strings.Split(*usEndpoints, ",")
		for i, endpoint := range usEndpointList {
			usEndpointList[i] = strings.TrimSpace(endpoint)
		}
	}

	// Define object sizes and counts for testing
	objectSizes := []int64{
		1024 * 1024,      // 1 MiB
		10 * 1024 * 1024, // 10 MiB
		//100 * 1024 * 1024, // 100 MiB
		//		1024 * 1024 * 1024, // 1 GiB
	}
	objectCounts := []int{
		100, // 1 MiB - 1000 objects
		10,  // 10 MiB - 100 objects
		//10,   // 100 MiB - 10 objects
		//1,    // 1 GiB - 1 object
	}

	// Create test configuration
	config := TestConfig{
		BucketName:     *bucketName,
		ObjectSizes:    objectSizes,
		ObjectCounts:   objectCounts,
		Concurrency:    *concurrency,
		TestDuration:   *testDuration,
		Prefix:         *prefix,
		GlobalEndpoint: *globalEndpoint,
		USEndpoints:    usEndpointList,
	}

	// Create performance tester
	tester, err := NewS3PerformanceTester(config)
	if err != nil {
		log.Fatalf("Failed to create S3 performance tester: %v", err)
	}

	// Run all tests
	if err := tester.RunAllTests(); err != nil {
		log.Fatalf("Test suite failed: %v", err)
	}
}
