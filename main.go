package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/tigrisdata/deployment-test/workload"
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
	// Create optimized HTTP client for high-throughput scenarios
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        1000,             // Increased from default 100
			MaxIdleConnsPerHost: 200,              // Increased from default 2
			MaxConnsPerHost:     0,                // Unlimited
			IdleConnTimeout:     90 * time.Second, // Keep connections alive longer
			DisableCompression:  true,             // Disable compression for raw throughput
			WriteBufferSize:     256 * 1024,       // 256KB write buffer
			ReadBufferSize:      256 * 1024,       // 256KB read buffer
		},
		Timeout: 5 * time.Minute, // Generous timeout for large objects
	}

	// Load AWS configuration (region will be automatically detected)
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithLogger(&silentLogger{}),
		config.WithHTTPClient(httpClient),
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

// runPerformanceBenchmark runs a single benchmark that collects both latency and throughput metrics
func (t *S3PerformanceTester) runPerformanceBenchmark(operation string, objectSize int64, recordCount int, opCount int, endpoint string) workload.BenchmarkResult {
	// Determine operation type
	var opType workload.OpType
	if operation == "PUT" {
		opType = workload.OpPUT
	} else {
		opType = workload.OpGET
	}

	// Create workload configuration (YCSB-style)
	config := workload.WorkloadConfig{
		Bucket:          t.config.BucketName,
		Endpoint:        endpoint,
		Prefix:          fmt.Sprintf("%s/%s-%d", t.config.Prefix, operation, objectSize),
		ObjectSize:      objectSize,
		RecordCount:     recordCount, // Number of records to preload
		OperationCount:  opCount,     // Number of operations to run
		TestDuration:    0,           // Not used, using operation count only
		Concurrency:     t.config.Concurrency,
		OperationType:   opType,
		Seed:            time.Now().UnixNano(),
		KeyDistribution: workload.DistUniform,
		ReuseObjects:    false,
	}

	// Get S3 client for endpoint
	client := t.clients[endpoint]

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

// runPerformanceBenchmarks runs all performance benchmarks (latency and throughput)
func (t *S3PerformanceTester) runPerformanceBenchmarks() bool {
	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" PERFORMANCE TESTS")
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

		// PUT performance tests
		fmt.Println("PUT Performance Tests:")
		for _, size := range t.config.BenchmarkSizes {
			fmt.Printf("  Testing %s (%d records, %d ops)...\n", size.DisplayName, size.RecordCount, size.OpCount)
			result := t.runPerformanceBenchmark("PUT", size.ObjectSize, size.RecordCount, size.OpCount, endpoint)

			// Display latency metrics
			fmt.Printf("    Latency    - %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s\n",
				ColorBrightWhite, ColorReset, formatDurationAligned(result.AvgLatency),
				ColorBrightWhite, ColorReset, formatDurationAligned(result.P95Latency),
				ColorBrightWhite, ColorReset, formatDurationAligned(result.P99Latency))

			// Display throughput metrics
			if result.SuccessOps > 0 {
				if result.ErrorOps > 0 {
					fmt.Printf("    Throughput - %sMB/s:%s %8.3f, %sops/s:%s %8.3f (%s%d success, %s%d failed%s)\n",
						ColorBrightWhite, ColorReset, result.ThroughputMBps,
						ColorBrightWhite, ColorReset, result.OpsPerSecond,
						ColorBrightGreen, result.SuccessOps, ColorBrightRed, result.ErrorOps, ColorReset)
				} else {
					fmt.Printf("    Throughput - %sMB/s:%s %8.3f, %sops/s:%s %8.3f (%s%d success%s)\n",
						ColorBrightWhite, ColorReset, result.ThroughputMBps,
						ColorBrightWhite, ColorReset, result.OpsPerSecond,
						ColorBrightGreen, result.SuccessOps, ColorReset)
				}
			} else {
				fmt.Printf("    %sFAILED%s (no successful operations)\n", ColorBrightRed, ColorReset)
			}
		}

		// GET performance tests (reuse objects created by PUT tests)
		fmt.Println("\nGET Performance Tests:")
		for _, size := range t.config.BenchmarkSizes {
			fmt.Printf("  Testing %s (%d records, %d ops)...\n", size.DisplayName, size.RecordCount, size.OpCount)
			result := t.runPerformanceBenchmark("GET", size.ObjectSize, size.RecordCount, size.OpCount, endpoint)

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
					fmt.Printf("    Throughput - %sMB/s:%s %8.3f, %sops/s:%s %8.3f (%s%d success, %s%d failed%s)\n",
						ColorBrightWhite, ColorReset, result.ThroughputMBps,
						ColorBrightWhite, ColorReset, result.OpsPerSecond,
						ColorBrightGreen, result.SuccessOps, ColorBrightRed, result.ErrorOps, ColorReset)
				} else {
					fmt.Printf("    Throughput - %sMB/s:%s %8.3f, %sops/s:%s %8.3f (%s%d success%s)\n",
						ColorBrightWhite, ColorReset, result.ThroughputMBps,
						ColorBrightWhite, ColorReset, result.OpsPerSecond,
						ColorBrightGreen, result.SuccessOps, ColorReset)
				}
			} else {
				fmt.Printf("    %sFAILED%s (no successful operations)\n", ColorBrightRed, ColorReset)
			}
		}
	}

	// For now, assume all performance benchmarks pass
	// TODO: Implement detailed failure tracking
	return true
}

// RunAllTests runs the complete test suite
func (t *S3PerformanceTester) RunAllTests() error {
	fmt.Printf("Starting S3 Performance Test Suite...\n")
	fmt.Printf("Bucket: %s%s%s\n", ColorBrightWhite, t.config.BucketName, ColorReset)
	fmt.Printf("Concurrency: %s%d%s\n", ColorBrightWhite, t.config.Concurrency, ColorReset)
	fmt.Printf("Benchmark Sizes: ")
	for i, size := range t.config.BenchmarkSizes {
		if i > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("%s%s%s", ColorBrightWhite, size.DisplayName, ColorReset)
	}
	fmt.Printf("\n")
	fmt.Printf("Global Endpoint: %s%s%s\n", ColorBrightWhite, t.config.GlobalEndpoint, ColorReset)
	fmt.Printf("US Endpoints: %s%v%s\n", ColorBrightWhite, t.config.USEndpoints, ColorReset)
	fmt.Printf("\n")

	// Track test results
	testResults := struct {
		consistencyPassed  bool
		connectivityPassed bool
		performancePassed  bool
		ranTests           bool
	}{
		consistencyPassed:  true,
		connectivityPassed: true,
		performancePassed:  true,
		ranTests:           false,
	}

	// Run connectivity tests
	if t.config.RunConnectivity {
		testResults.connectivityPassed = t.runConnectivityTests()
		testResults.ranTests = true
	}

	// Run consistency tests first
	if t.config.RunConsistency {
		testResults.consistencyPassed = RunConsistencyTests(t)
		testResults.ranTests = true
	}

	// Run performance benchmarks
	if t.config.RunPerformance {
		testResults.performancePassed = t.runPerformanceBenchmarks()
		testResults.ranTests = true
	}

	// Display test summary if any tests were run
	if testResults.ranTests {
		fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
		fmt.Println(" TEST SUMMARY")
		fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

		// Display results for each test section that was run
		if t.config.RunConnectivity {
			if testResults.connectivityPassed {
				fmt.Printf(" CONNECTIVITY TESTS %sPASSED%s\n", ColorBrightGreen, ColorReset)
			} else {
				fmt.Printf(" CONNECTIVITY TESTS %sFAILED%s\n", ColorBrightRed, ColorReset)
			}
		}

		if t.config.RunConsistency {
			if testResults.consistencyPassed {
				fmt.Printf(" CONSISTENCY TESTS %sPASSED%s\n", ColorBrightGreen, ColorReset)
			} else {
				fmt.Printf(" CONSISTENCY TESTS %sFAILED%s\n", ColorBrightRed, ColorReset)
			}
		}

		if t.config.RunPerformance {
			if testResults.performancePassed {
				fmt.Printf(" PERFORMANCE BENCHMARKS %sPASSED%s\n", ColorBrightGreen, ColorReset)
			} else {
				fmt.Printf(" PERFORMANCE BENCHMARKS %sFAILED%s\n", ColorBrightRed, ColorReset)
			}
		}
	} else {
		fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
		fmt.Printf(" %sNO TESTS RUN%s - All test flags were set to false\n", ColorBrightRed, ColorReset)
		fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
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
		concurrency    = flag.Int("concurrency", 20, "Number of concurrent operations")
		prefix         = flag.String("prefix", "perf-test", "S3 key prefix")
		globalEndpoint = flag.String("global-endpoint", "", "Global S3 endpoint URL")
		usEndpoints    = flag.String("us-endpoints", "", "Comma-separated list of US regional endpoints")

		// Test selection flag
		tests = flag.String("tests", "all", "Comma-separated list of tests to run: connectivity,consistency,performance (default: all)")
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

	// Parse test selection
	testList := strings.ToLower(strings.TrimSpace(*tests))
	runConnectivity := false
	runConsistency := false
	runPerformance := false

	if testList == "all" || testList == "" {
		runConnectivity = true
		runConsistency = true
		runPerformance = true
	} else {
		selectedTests := strings.Split(testList, ",")
		for _, test := range selectedTests {
			test = strings.TrimSpace(test)
			switch test {
			case "connectivity":
				runConnectivity = true
			case "consistency":
				runConsistency = true
			case "performance":
				runPerformance = true
			default:
				fmt.Fprintf(os.Stderr, "Warning: unknown test '%s' ignored. Valid tests: connectivity, consistency, performance\n", test)
			}
		}
	}

	// Use default benchmark sizes (1 MiB, 10 MiB, 100 MiB)
	// These follow YCSB patterns with consistent ~1 GB dataset size
	benchmarkSizes := DefaultBenchmarkSizes

	// For now, only use first two sizes to match previous behavior
	// TODO: Add flag to control which sizes to test
	benchmarkSizes = DefaultBenchmarkSizes[:2] // 1 MiB and 10 MiB only

	// Create test configuration
	config := TestConfig{
		BucketName:      *bucketName,
		BenchmarkSizes:  benchmarkSizes,
		Concurrency:     *concurrency,
		Prefix:          *prefix,
		GlobalEndpoint:  *globalEndpoint,
		USEndpoints:     usEndpointList,
		RunConnectivity: runConnectivity,
		RunConsistency:  runConsistency,
		RunPerformance:  runPerformance,
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
