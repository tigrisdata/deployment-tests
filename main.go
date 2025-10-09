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

// TestConfig holds configuration for the performance test
type TestConfig struct {
	BucketName     string
	ObjectSizes    []int64
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
	awsCfg, err := config.LoadDefaultConfig(context.TODO())
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
	duration := time.Since(start)

	var throughput float64
	if err == nil && resp.ContentLength != nil {
		throughput = float64(*resp.ContentLength) / duration.Seconds()
	}

	return TestResult{
		Operation:  "GET",
		ObjectSize: 0, // Will be set based on actual content length
		Endpoint:   endpoint,
		Duration:   duration,
		Success:    err == nil,
		Error:      err,
		Throughput: throughput,
	}
}

// runLatencyBenchmark runs latency benchmark for a specific operation and size
func (t *S3PerformanceTester) runLatencyBenchmark(operation string, objectSize int64, endpoint string) BenchmarkResult {
	var results []TestResult
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Generate test data
	data := t.generateRandomData(objectSize)
	keyPrefix := fmt.Sprintf("%s/latency-%s-%d", t.config.Prefix, operation, objectSize)

	// Run concurrent operations
	for i := 0; i < t.config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			key := fmt.Sprintf("%s/worker-%d", keyPrefix, workerID)
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
		}(i)
	}

	wg.Wait()

	// Calculate statistics
	return t.calculateBenchmarkStats(operation, objectSize, endpoint, results)
}

// runThroughputBenchmark runs throughput benchmark for a specific operation and size
func (t *S3PerformanceTester) runThroughputBenchmark(operation string, objectSize int64, endpoint string) BenchmarkResult {
	var results []TestResult
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Generate test data
	data := t.generateRandomData(objectSize)
	keyPrefix := fmt.Sprintf("%s/throughput-%s-%d", t.config.Prefix, operation, objectSize)

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

				key := fmt.Sprintf("%s/worker-%d/obj-%d", keyPrefix, workerID, keyCounter)
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
	var totalBytes int64

	for _, result := range results {
		if result.Success {
			successResults = append(successResults, result)
			durations = append(durations, result.Duration)
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
		TotalBytes:   totalBytes,
		Throughput:   throughput,
		OpsPerSecond: opsPerSecond,
	}
}

// runConnectivityTests runs connectivity tests
func (t *S3PerformanceTester) runConnectivityTests() {
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("CONNECTIVITY TESTS")
	fmt.Println(strings.Repeat("=", 80))

	// Test global endpoint
	if t.config.GlobalEndpoint != "" {
		fmt.Printf("\n%sTesting Global Endpoint: %s%s\n", ColorBrightWhite, t.config.GlobalEndpoint, ColorReset)

		// Health check test
		healthDuration, err := t.testHealthEndpoint(t.config.GlobalEndpoint)
		if err != nil {
			fmt.Printf("  Health Check: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
		} else {
			fmt.Printf("  Health Check: %sSUCCESS%s - %v\n", ColorBrightGreen, ColorReset, healthDuration)
		}

		// S3 connectivity test
		s3Duration, err := t.testS3Connectivity("global")
		if err != nil {
			fmt.Printf("  S3 Connectivity: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
		} else {
			fmt.Printf("  S3 Connectivity: %sSUCCESS%s - %v\n", ColorBrightGreen, ColorReset, s3Duration)
		}
	}

	// Test US regional endpoints
	for _, endpoint := range t.config.USEndpoints {
		fmt.Printf("\n%sTesting US Regional Endpoint: %s%s\n", ColorBrightWhite, endpoint, ColorReset)

		// Health check test
		healthDuration, err := t.testHealthEndpoint(endpoint)
		if err != nil {
			fmt.Printf("  Health Check: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
		} else {
			fmt.Printf("  Health Check: %sSUCCESS%s - %v\n", ColorBrightGreen, ColorReset, healthDuration)
		}

		// S3 connectivity test
		s3Duration, err := t.testS3Connectivity(endpoint)
		if err != nil {
			fmt.Printf("  S3 Connectivity: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
		} else {
			fmt.Printf("  S3 Connectivity: %sSUCCESS%s - %v\n", ColorBrightGreen, ColorReset, s3Duration)
		}
	}
}

// runLatencyBenchmarks runs all latency benchmarks
func (t *S3PerformanceTester) runLatencyBenchmarks() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("LATENCY BENCHMARKS")
	fmt.Println(strings.Repeat("=", 80))

	// Test all endpoints
	endpoints := []string{"global"}
	endpoints = append(endpoints, t.config.USEndpoints...)

	for _, endpoint := range endpoints {
		if endpoint == "global" && t.config.GlobalEndpoint == "" {
			continue
		}

		fmt.Printf("\n%sTesting Endpoint: %s%s\n", ColorBrightWhite, endpoint, ColorReset)
		fmt.Println(strings.Repeat("-", 60))

		// PUT latency tests
		fmt.Println("PUT Latency Tests:")
		for _, size := range t.config.ObjectSizes {
			fmt.Printf("  Testing %d bytes...", size)
			result := t.runLatencyBenchmark("PUT", size, endpoint)
			fmt.Printf(" Avg: %v, P95: %v, P99: %v\n",
				result.AvgLatency, result.P95Latency, result.P99Latency)
		}

		// GET latency tests
		fmt.Println("\nGET Latency Tests:")
		for _, size := range t.config.ObjectSizes {
			fmt.Printf("  Testing %d bytes...", size)
			result := t.runLatencyBenchmark("GET", size, endpoint)
			fmt.Printf(" Avg: %v, P95: %v, P99: %v\n",
				result.AvgLatency, result.P95Latency, result.P99Latency)
		}
	}
}

// runThroughputBenchmarks runs all throughput benchmarks
func (t *S3PerformanceTester) runThroughputBenchmarks() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("THROUGHPUT BENCHMARKS")
	fmt.Println(strings.Repeat("=", 80))

	// Test all endpoints
	endpoints := []string{"global"}
	endpoints = append(endpoints, t.config.USEndpoints...)

	for _, endpoint := range endpoints {
		if endpoint == "global" && t.config.GlobalEndpoint == "" {
			continue
		}

		fmt.Printf("\n%sTesting Endpoint: %s%s\n", ColorBrightWhite, endpoint, ColorReset)
		fmt.Println(strings.Repeat("-", 60))

		// PUT throughput tests
		fmt.Println("PUT Throughput Tests:")
		for _, size := range t.config.ObjectSizes {
			fmt.Printf("  Testing %d bytes...", size)
			result := t.runThroughputBenchmark("PUT", size, endpoint)
			if result.SuccessOps > 0 {
				fmt.Printf(" %.2f MB/s, %.2f ops/s\n",
					result.Throughput/(1024*1024), result.OpsPerSecond)
			} else {
				fmt.Printf(" %sFAILED%s (no successful operations)\n", ColorBrightRed, ColorReset)
			}
		}

		// GET throughput tests
		fmt.Println("\nGET Throughput Tests:")
		for _, size := range t.config.ObjectSizes {
			fmt.Printf("  Testing %d bytes...", size)
			result := t.runThroughputBenchmark("GET", size, endpoint)
			if result.SuccessOps > 0 {
				fmt.Printf(" %.2f MB/s, %.2f ops/s\n",
					result.Throughput/(1024*1024), result.OpsPerSecond)
			} else {
				fmt.Printf(" %sFAILED%s (no successful operations)\n", ColorBrightRed, ColorReset)
			}
		}
	}
}

// RunAllTests runs the complete test suite
func (t *S3PerformanceTester) RunAllTests() error {
	fmt.Printf("Starting S3 Performance Test Suite...\n")
	fmt.Printf("Bucket: %s\n", t.config.BucketName)
	fmt.Printf("Concurrency: %d\n", t.config.Concurrency)
	fmt.Printf("Test Duration: %v\n", t.config.TestDuration)
	fmt.Printf("Object Sizes: %v\n", t.config.ObjectSizes)
	fmt.Printf("Global Endpoint: %s\n", t.config.GlobalEndpoint)
	fmt.Printf("US Endpoints: %v\n", t.config.USEndpoints)

	// Run connectivity tests
	t.runConnectivityTests()

	// Run latency benchmarks
	t.runLatencyBenchmarks()

	// Run throughput benchmarks
	t.runThroughputBenchmarks()

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("TEST SUITE COMPLETED")
	fmt.Println(strings.Repeat("=", 80))

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

	// Define object sizes for testing
	objectSizes := []int64{
		1024 * 1024, // 1 MiB
		//		100 * 1024 * 1024,  // 100 MiB
		//		1024 * 1024 * 1024, // 1 GiB
	}

	// Create test configuration
	config := TestConfig{
		BucketName:     *bucketName,
		ObjectSizes:    objectSizes,
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
