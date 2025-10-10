package workload

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// WorkloadGenerator orchestrates workload generation and benchmark execution
type WorkloadGenerator struct {
	config      *WorkloadConfig
	s3Client    *s3.Client
	ops         *S3Operations
	keyGen      *KeyGenerator
	workerSeeds []int64
}

// NewGenerator creates a new workload generator
func NewGenerator(config WorkloadConfig, s3Client *s3.Client) (*WorkloadGenerator, error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Initialize operations wrapper
	ops := NewS3Operations(s3Client, config.Bucket)

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
		s3Client:    s3Client,
		ops:         ops,
		keyGen:      keyGen,
		workerSeeds: workerSeeds,
	}, nil
}

// Preload executes the preload phase, uploading objects to S3
func (wg *WorkloadGenerator) Preload(ctx context.Context) error {
	preload := NewPreloadPhase(wg.ops, wg.keyGen, wg.config, wg.workerSeeds)
	return preload.Run(ctx)
}

// Run executes the benchmark workload and returns results
func (wg *WorkloadGenerator) Run(ctx context.Context) (*BenchmarkResult, error) {
	workerPool := NewWorkerPool(wg.ops, wg.keyGen, wg.config, wg.workerSeeds)

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
