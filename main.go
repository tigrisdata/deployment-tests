package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// PerformanceMetrics holds performance statistics
type PerformanceMetrics struct {
	Operation     string
	Count         int64
	TotalDuration time.Duration
	MinDuration   time.Duration
	MaxDuration   time.Duration
	AvgDuration   time.Duration
	Errors        int64
	Throughput    float64 // operations per second
}

// TestConfig holds configuration for the performance test
type TestConfig struct {
	BucketName   string
	ObjectSize   int64
	NumObjects   int
	NumWorkers   int
	TestDuration time.Duration
	Prefix       string
}

// S3PerformanceTester handles S3 performance testing
type S3PerformanceTester struct {
	client  *s3.Client
	config  TestConfig
	metrics map[string]*PerformanceMetrics
	mu      sync.RWMutex
}

// NewS3PerformanceTester creates a new S3 performance tester
func NewS3PerformanceTester(cfg TestConfig) (*S3PerformanceTester, error) {
	// Load AWS configuration (region will be automatically detected)
	awsCfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)

	return &S3PerformanceTester{
		client:  client,
		config:  cfg,
		metrics: make(map[string]*PerformanceMetrics),
	}, nil
}

// generateRandomData creates random data of specified size
func (t *S3PerformanceTester) generateRandomData(size int64) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// recordMetric records a performance metric
func (t *S3PerformanceTester) recordMetric(operation string, duration time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	metric, exists := t.metrics[operation]
	if !exists {
		metric = &PerformanceMetrics{
			Operation:   operation,
			MinDuration: duration,
			MaxDuration: duration,
		}
		t.metrics[operation] = metric
	}

	metric.Count++
	metric.TotalDuration += duration
	metric.AvgDuration = metric.TotalDuration / time.Duration(metric.Count)

	if duration < metric.MinDuration {
		metric.MinDuration = duration
	}
	if duration > metric.MaxDuration {
		metric.MaxDuration = duration
	}

	if err != nil {
		metric.Errors++
	}
}

// testPutObject tests PUT object performance
func (t *S3PerformanceTester) testPutObject(ctx context.Context, key string, data []byte) {
	start := time.Now()

	_, err := t.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(t.config.BucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	duration := time.Since(start)
	t.recordMetric("PutObject", duration, err)
}

// testGetObject tests GET object performance
func (t *S3PerformanceTester) testGetObject(ctx context.Context, key string) {
	start := time.Now()

	_, err := t.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(t.config.BucketName),
		Key:    aws.String(key),
	})

	duration := time.Since(start)
	t.recordMetric("GetObject", duration, err)
}

// testDeleteObject tests DELETE object performance
func (t *S3PerformanceTester) testDeleteObject(ctx context.Context, key string) {
	start := time.Now()

	_, err := t.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.config.BucketName),
		Key:    aws.String(key),
	})

	duration := time.Since(start)
	t.recordMetric("DeleteObject", duration, err)
}

// testListObjects tests LIST objects performance
func (t *S3PerformanceTester) testListObjects(ctx context.Context) {
	start := time.Now()

	_, err := t.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(t.config.BucketName),
		Prefix: aws.String(t.config.Prefix),
	})

	duration := time.Since(start)
	t.recordMetric("ListObjects", duration, err)
}

// worker runs performance tests in a worker goroutine
func (t *S3PerformanceTester) worker(ctx context.Context, workerID int, wg *sync.WaitGroup) {
	defer wg.Done()

	data := t.generateRandomData(t.config.ObjectSize)

	for i := 0; i < t.config.NumObjects; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("%s/worker-%d/object-%d", t.config.Prefix, workerID, i)

		// Test PUT
		t.testPutObject(ctx, key, data)

		// Test GET
		t.testGetObject(ctx, key)

		// Test LIST (every 10th operation)
		if i%10 == 0 {
			t.testListObjects(ctx)
		}

		// Test DELETE
		t.testDeleteObject(ctx, key)
	}
}

// RunPerformanceTest runs the complete performance test
func (t *S3PerformanceTester) RunPerformanceTest(ctx context.Context) error {
	fmt.Printf("Starting S3 performance test...\n")
	fmt.Printf("Bucket: %s\n", t.config.BucketName)
	fmt.Printf("Object size: %d bytes\n", t.config.ObjectSize)
	fmt.Printf("Objects per worker: %d\n", t.config.NumObjects)
	fmt.Printf("Number of workers: %d\n", t.config.NumWorkers)
	fmt.Printf("Test duration: %v\n", t.config.TestDuration)
	fmt.Printf("Prefix: %s\n\n", t.config.Prefix)

	// Create context with timeout
	testCtx, cancel := context.WithTimeout(ctx, t.config.TestDuration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	// Start workers
	for i := 0; i < t.config.NumWorkers; i++ {
		wg.Add(1)
		go t.worker(testCtx, i, &wg)
	}

	// Wait for all workers to complete or timeout
	wg.Wait()
	totalDuration := time.Since(startTime)

	// Calculate throughput
	t.mu.RLock()
	for _, metric := range t.metrics {
		metric.Throughput = float64(metric.Count) / totalDuration.Seconds()
	}
	t.mu.RUnlock()

	return nil
}

// PrintResults prints the performance test results
func (t *S3PerformanceTester) PrintResults() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("S3 PERFORMANCE TEST RESULTS")
	fmt.Println(strings.Repeat("=", 80))

	t.mu.RLock()
	defer t.mu.RUnlock()

	for operation, metric := range t.metrics {
		fmt.Printf("\nOperation: %s\n", operation)
		fmt.Printf("  Total operations: %d\n", metric.Count)
		fmt.Printf("  Errors: %d (%.2f%%)\n", metric.Errors, float64(metric.Errors)/float64(metric.Count)*100)
		fmt.Printf("  Total duration: %v\n", metric.TotalDuration)
		fmt.Printf("  Average duration: %v\n", metric.AvgDuration)
		fmt.Printf("  Min duration: %v\n", metric.MinDuration)
		fmt.Printf("  Max duration: %v\n", metric.MaxDuration)
		fmt.Printf("  Throughput: %.2f ops/sec\n", metric.Throughput)
	}
}

func main() {
	// Parse command line flags
	var (
		bucketName   = flag.String("bucket", "", "S3 bucket name (required)")
		objectSize   = flag.Int64("size", 1024*1024, "Object size in bytes (default: 1MB)")
		numObjects   = flag.Int("objects", 100, "Number of objects per worker")
		numWorkers   = flag.Int("workers", 4, "Number of concurrent workers")
		testDuration = flag.Duration("duration", 5*time.Minute, "Test duration")
		prefix       = flag.String("prefix", "perf-test", "S3 key prefix")
	)
	flag.Parse()

	if *bucketName == "" {
		fmt.Fprintf(os.Stderr, "Error: bucket name is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Create test configuration
	config := TestConfig{
		BucketName:   *bucketName,
		ObjectSize:   *objectSize,
		NumObjects:   *numObjects,
		NumWorkers:   *numWorkers,
		TestDuration: *testDuration,
		Prefix:       *prefix,
	}

	// Create performance tester
	tester, err := NewS3PerformanceTester(config)
	if err != nil {
		log.Fatalf("Failed to create S3 performance tester: %v", err)
	}

	// Run performance test
	ctx := context.Background()
	if err := tester.RunPerformanceTest(ctx); err != nil {
		log.Fatalf("Performance test failed: %v", err)
	}

	// Print results
	tester.PrintResults()
}
