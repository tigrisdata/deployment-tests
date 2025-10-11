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

// WorkloadGenerator orchestrates workload generation and benchmark execution
type WorkloadGenerator struct {
	config       *WorkloadConfig
	awsConfig    s3.Options      // S3 options from original client
	s3Options    []func(*s3.Options) // Additional S3 options for client creation
	keyGen       *KeyGenerator
	workerSeeds  []int64
}

// NewGenerator creates a new workload generator
func NewGenerator(config WorkloadConfig, s3Client *s3.Client) (*WorkloadGenerator, error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Store S3-specific options for recreating clients
	// We need to capture the options from the original client
	originalOptions := s3Client.Options()
	s3Options := []func(*s3.Options){
		func(o *s3.Options) {
			// Copy all relevant options from the original client
			o.UsePathStyle = originalOptions.UsePathStyle
			o.BaseEndpoint = originalOptions.BaseEndpoint
			o.Region = originalOptions.Region
			o.Credentials = originalOptions.Credentials
			o.RetryMaxAttempts = originalOptions.RetryMaxAttempts
			o.RetryMode = originalOptions.RetryMode
		},
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
		awsConfig:   originalOptions,
		s3Options:   s3Options,
		keyGen:      keyGen,
		workerSeeds: workerSeeds,
	}, nil
}

// createWorkerClient creates a new S3Operations for a worker with its own HTTP connection pool
func (wg *WorkloadGenerator) createWorkerClient() *S3Operations {
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

	// Build AWS config from the stored options
	awsCfg := aws.Config{
		Region:      wg.awsConfig.Region,
		Credentials: wg.awsConfig.Credentials,
		RetryMode:   wg.awsConfig.RetryMode,
	}
	if wg.awsConfig.RetryMaxAttempts > 0 {
		awsCfg.RetryMaxAttempts = wg.awsConfig.RetryMaxAttempts
	}

	// Create a new S3 client with worker-specific HTTP client and original S3 options
	clientOptions := append(wg.s3Options, func(o *s3.Options) {
		o.HTTPClient = httpClient
	})

	s3Client := s3.NewFromConfig(awsCfg, clientOptions...)

	// Wrap in S3Operations
	return NewS3Operations(s3Client, wg.config.Bucket, wg.config.UseMultipart, wg.config.MultipartSize)
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
