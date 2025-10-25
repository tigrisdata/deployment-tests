package workload

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// CreateWorkerS3Client creates a new S3 client with an isolated HTTP connection pool
// This should be called per-worker goroutine to ensure each worker has dedicated connections
func CreateWorkerS3Client(baseClient *s3.Client) *s3.Client {
	// Get original options from base client
	originalOptions := baseClient.Options()

	// Create a new HTTP client with optimized settings for each worker
	// This ensures each worker has its own connection pool
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			MaxConnsPerHost:     0,                // Unlimited
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			WriteBufferSize:     256 * 1024,
			ReadBufferSize:      256 * 1024,
		},
		Timeout: 5 * time.Minute,
	}

	// Build AWS config from the original client
	awsCfg := aws.Config{
		Region:      originalOptions.Region,
		Credentials: originalOptions.Credentials,
		RetryMode:   originalOptions.RetryMode,
	}
	if originalOptions.RetryMaxAttempts > 0 {
		awsCfg.RetryMaxAttempts = originalOptions.RetryMaxAttempts
	}

	// Create S3 options copying from original
	s3Options := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = originalOptions.UsePathStyle
			o.BaseEndpoint = originalOptions.BaseEndpoint
			o.Region = originalOptions.Region
			o.Credentials = originalOptions.Credentials
			o.RetryMaxAttempts = originalOptions.RetryMaxAttempts
			o.RetryMode = originalOptions.RetryMode
			o.HTTPClient = httpClient // Set worker-specific HTTP client
		},
	}

	return s3.NewFromConfig(awsCfg, s3Options...)
}

// WorkloadGenerator orchestrates workload generation and benchmark execution
type WorkloadGenerator struct {
	config      *WorkloadConfig
	baseClient  *s3.Client  // Base S3 client to clone for workers
	keyGen      *KeyGenerator
	workerSeeds []int64
}

// NewGenerator creates a new workload generator
func NewGenerator(config WorkloadConfig, s3Client *s3.Client) (*WorkloadGenerator, error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Initialize key generator
	keyGen := NewKeyGenerator(config.Prefix, config.RecordCount, config.KeyDistribution, config.Concurrency)

	// Generate worker seeds for reproducibility
	masterRng := rand.New(rand.NewSource(config.Seed))
	workerSeeds := make([]int64, config.Concurrency)
	for i := 0; i < config.Concurrency; i++ {
		workerSeeds[i] = masterRng.Int63()
	}

	return &WorkloadGenerator{
		config:      &config,
		baseClient:  s3Client,
		keyGen:      keyGen,
		workerSeeds: workerSeeds,
	}, nil
}

// createWorkerClient creates a new S3Operations for a worker with its own HTTP connection pool
func (wg *WorkloadGenerator) createWorkerClient() *S3Operations {
	// Use the exported function to create a worker-specific S3 client
	workerClient := CreateWorkerS3Client(wg.baseClient)

	// Wrap in S3Operations
	return NewS3Operations(workerClient, wg.config.Bucket, wg.config.UseMultipart, wg.config.MultipartSize)
}

// Preload executes the preload phase, uploading objects to S3
func (wg *WorkloadGenerator) Preload(ctx context.Context) error {
	preload := NewPreloadPhase(wg.createWorkerClient, wg.keyGen, wg.config, wg.workerSeeds)
	return preload.Run(ctx)
}

// Run executes the benchmark workload and returns results
func (wg *WorkloadGenerator) Run(ctx context.Context) (*BenchmarkResult, error) {
	workerPool := NewWorkerPool(wg.createWorkerClient, wg.keyGen, wg.config, wg.workerSeeds)

	var workerResults []WorkerResult
	var duration time.Duration

	// Execute based on configuration (fixed ops or duration)
	if wg.config.OperationCount > 0 {
		workerResults, duration = workerPool.RunFixedOps(ctx)
	} else {
		workerResults, duration = workerPool.RunDuration(ctx)
	}

	// Aggregate results
	result := AggregateResults(
		wg.determineTestType(),
		wg.config.OperationType.String(),
		wg.config.ObjectSize,
		wg.config.Endpoint,
		wg.config.Concurrency,
		workerResults,
		duration,
	)

	return result, nil
}

// RunBenchmark executes both preload and run phases
func (wg *WorkloadGenerator) RunBenchmark(ctx context.Context) (*BenchmarkResult, error) {
	// Phase 1: Preload (only for GET operations or if objects don't exist)
	if wg.config.OperationType == OpGET || wg.config.OperationType == OpMIXED {
		if err := wg.Preload(ctx); err != nil {
			return nil, fmt.Errorf("preload phase failed: %w", err)
		}
	}

	// Phase 2: Run benchmark
	return wg.Run(ctx)
}

// determineTestType returns the test type string based on configuration
func (wg *WorkloadGenerator) determineTestType() string {
	if wg.config.OperationCount > 0 {
		return "latency"
	}
	return "throughput"
}
