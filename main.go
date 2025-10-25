package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
)

const (
	// ANSI color codes
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

	// Defaults
	DefaultConcurrency         = 20
	DefaultPrefix              = "t3-validator"
	DefaultGlobalEndpoint      = "https://oracle.storage.dev"
	DefaultRegionalEndpointIAD = "https://iad.storage.dev"
	DefaultRegionalEndpointORD = "https://ord.storage.dev"
	DefaultRegionalEndpointSJC = "https://sjc.storage.dev"
	DefaultTests               = "all"
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

// TigrisValidator handles Tigris performance testing
type TigrisValidator struct {
	config  TestConfig
	clients map[string]*s3.Client
}

// NewTigrisValidator creates a new Tigris validator
func NewTigrisValidator(cfg TestConfig) (*TigrisValidator, error) {
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

	// Regional endpoints clients
	for _, endpoint := range cfg.RegionalEndpoints {
		regionalCfg := awsCfg.Copy()
		regionalCfg.BaseEndpoint = aws.String(endpoint)
		clients[endpoint] = s3.NewFromConfig(regionalCfg, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	return &TigrisValidator{
		config:  cfg,
		clients: clients,
	}, nil
}

// RunAllTests runs the complete test suite
func (t *TigrisValidator) RunAllTests() error {
	fmt.Printf("Starting Tigris Performance Test Suite...\n")
	fmt.Printf("Bucket: %s%s%s\n", ColorBrightWhite, t.config.BucketName, ColorReset)
	fmt.Printf("Global Endpoint: %s%s%s\n", ColorBrightWhite, t.config.GlobalEndpoint, ColorReset)
	fmt.Printf("Regional Endpoints: %s%v%s\n", ColorBrightWhite, t.config.RegionalEndpoints, ColorReset)
	fmt.Printf("\n")

	// Create test instances
	var tests []Test

	if t.config.RunConnectivity {
		tests = append(tests, NewConnectivityTest(t))
	}

	if t.config.RunConsistency {
		tests = append(tests, NewConsistencyTest(t))
	}

	if t.config.RunPerformance {
		tests = append(tests, NewPerformanceTest(t))
	}

	if t.config.RunTranscode {
		tests = append(tests, NewTranscodeTest(t))
	}

	// Check if any tests are configured to run
	if len(tests) == 0 {
		fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
		fmt.Printf(" %sNO TESTS RUN%s - All test flags were set to false\n", ColorBrightRed, ColorReset)
		fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
		return nil
	}

	// Run tests
	ctx := context.Background()
	results := make(map[string]TestStatus)

	for _, test := range tests {
		// Setup
		if err := test.Setup(ctx); err != nil {
			log.Printf("Failed to setup %s: %v", test.Name(), err)
			continue
		}

		// Run test
		result := test.Run(ctx)
		results[string(test.Type())] = result

		// Cleanup
		if err := test.Cleanup(ctx); err != nil {
			log.Printf("Failed to cleanup %s: %v", test.Name(), err)
		}
	}

	// Display test summary
	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" TEST SUMMARY")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	for testType, result := range results {
		testName := strings.ToUpper(testType) + " TESTS"
		if testType == string(TestTypePerformance) {
			testName = "PERFORMANCE BENCHMARKS"
		} else if testType == string(TestTypeTranscode) {
			testName = "TRANSCODING WORKLOAD TESTS"
		}

		if result.Passed {
			fmt.Printf(" %s %sPASSED%s\n", testName, ColorBrightGreen, ColorReset)
		} else {
			fmt.Printf(" %s %sFAILED%s\n", testName, ColorBrightRed, ColorReset)
		}
	}

	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" TEST SUITE COMPLETED")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	return nil
}

func main() {
	// Parse command line flags
	var (
		bucketName        = flag.String("bucket", "", "Bucket name (required)")
		concurrency       = flag.Int("concurrency", DefaultConcurrency, "Number of concurrent operations")
		prefix            = flag.String("prefix", DefaultPrefix, "Object key prefix")
		globalEndpoint    = flag.String("global-endpoint", DefaultGlobalEndpoint, "Global Tigris endpoint URL")
		regionalEndpoints = flag.String("regional-endpoints", strings.Join([]string{DefaultRegionalEndpointIAD, DefaultRegionalEndpointORD, DefaultRegionalEndpointSJC}, ","), "Comma-separated list of regional Tigris endpoints")
		tests             = flag.String("tests", DefaultTests, "Comma-separated list of tests to run: connectivity,consistency,performance,transcode (default: all)")
	)
	flag.Parse()

	if *bucketName == "" {
		fmt.Fprintf(os.Stderr, "Error: bucket name is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Parse regional endpoints
	var regionalEndpointList []string
	if *regionalEndpoints != "" {
		regionalEndpointList = strings.Split(*regionalEndpoints, ",")
		for i, endpoint := range regionalEndpointList {
			regionalEndpointList[i] = strings.TrimSpace(endpoint)
		}
	}

	// Parse test selection
	testList := strings.ToLower(strings.TrimSpace(*tests))
	runConnectivity := false
	runConsistency := false
	runPerformance := false
	runTranscode := false

	if testList == "all" || testList == "" {
		runConnectivity = true
		runConsistency = true
		runPerformance = true
		runTranscode = true
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
			case "transcode":
				runTranscode = true
			default:
				fmt.Fprintf(os.Stderr, "Warning: unknown test '%s' ignored. Valid tests: connectivity, consistency, performance, transcode\n", test)
			}
		}
	}

	// Use default benchmark sizes (1 MiB, 10 MiB, 100 MiB)
	// These follow YCSB patterns with consistent ~1 GB dataset size
	benchmarkSizes := DefaultBenchmarkSizes

	// Create test configuration
	config := TestConfig{
		BucketName:        *bucketName,
		BenchmarkSizes:    benchmarkSizes,
		Concurrency:       *concurrency,
		Prefix:            *prefix,
		GlobalEndpoint:    *globalEndpoint,
		RegionalEndpoints: regionalEndpointList,
		RunConnectivity:   runConnectivity,
		RunConsistency:    runConsistency,
		RunPerformance:    runPerformance,
		RunTranscode:      runTranscode,
		TranscodeConfig:   DefaultTranscodeConfig,
	}

	// Create performance tester
	tester, err := NewTigrisValidator(config)
	if err != nil {
		log.Fatalf("Failed to create Tigris validator: %v", err)
	}

	// Run all tests
	if err := tester.RunAllTests(); err != nil {
		log.Fatalf("Test suite failed: %v", err)
	}
}
