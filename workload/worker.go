package workload

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// WorkerPool manages a pool of workers for benchmark execution
type WorkerPool struct {
	clientFactory func() *S3Operations
	keyGen        *KeyGenerator
	config        *WorkloadConfig
	workerSeeds   []int64
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(clientFactory func() *S3Operations, keyGen *KeyGenerator, config *WorkloadConfig, workerSeeds []int64) *WorkerPool {
	return &WorkerPool{
		clientFactory: clientFactory,
		keyGen:        keyGen,
		config:        config,
		workerSeeds:   workerSeeds,
	}
}

// RunFixedOps runs a fixed number of operations across all workers
func (wp *WorkerPool) RunFixedOps(ctx context.Context) ([]WorkerResult, time.Duration) {
	var wg sync.WaitGroup
	resultCh := make(chan WorkerResult, wp.config.Concurrency)

	opsPerWorker := wp.config.OperationCount / wp.config.Concurrency
	if opsPerWorker == 0 {
		opsPerWorker = 1
	}

	start := time.Now()

	// Start workers
	for workerID := 0; workerID < wp.config.Concurrency; workerID++ {
		wg.Add(1)
		seed := wp.workerSeeds[workerID]

		go func(wID int, s int64) {
			defer wg.Done()
			result := wp.runWorkerFixedOps(ctx, wID, s, opsPerWorker)
			resultCh <- result
		}(workerID, seed)
	}

	wg.Wait()
	close(resultCh)
	duration := time.Since(start)

	// Collect results
	results := make([]WorkerResult, 0, wp.config.Concurrency)
	for result := range resultCh {
		results = append(results, result)
	}

	return results, duration
}

// RunDuration runs operations for a specified duration across all workers
func (wp *WorkerPool) RunDuration(ctx context.Context) ([]WorkerResult, time.Duration) {
	var wg sync.WaitGroup
	resultCh := make(chan WorkerResult, wp.config.Concurrency)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, wp.config.TestDuration)
	defer cancel()

	start := time.Now()

	// Start workers
	for workerID := 0; workerID < wp.config.Concurrency; workerID++ {
		wg.Add(1)
		seed := wp.workerSeeds[workerID]

		go func(wID int, s int64) {
			defer wg.Done()
			result := wp.runWorkerDuration(ctx, wID, s)
			resultCh <- result
		}(workerID, seed)
	}

	wg.Wait()
	close(resultCh)
	duration := time.Since(start)

	// Collect results
	results := make([]WorkerResult, 0, wp.config.Concurrency)
	for result := range resultCh {
		results = append(results, result)
	}

	return results, duration
}

// runWorkerFixedOps runs a fixed number of operations for a single worker
func (wp *WorkerPool) runWorkerFixedOps(ctx context.Context, workerID int, seed int64, opsToRun int) WorkerResult {
	// Create per-worker S3 client for isolated connection pool
	workerOps := wp.clientFactory()

	localRng := rand.New(rand.NewSource(seed))

	// Pre-allocate result slices
	latencies := make([]time.Duration, 0, opsToRun)
	ttfbDurations := make([]time.Duration, 0, opsToRun)

	var successOps, errorOps, totalBytes int64

	// For small objects (<=10MB), generate once and reuse for efficiency
	// For large objects (>10MB), generate per-operation using streaming
	var data []byte
	useStreamingUpload := wp.config.ObjectSize > ChunkedGenerationThreshold

	if !useStreamingUpload && (wp.config.OperationType == OpPUT || wp.config.OperationType == OpMIXED) {
		// Small object: generate once and reuse
		data = generateDataWithRNG(localRng, wp.config.ObjectSize)
	}

	for opIndex := 0; opIndex < opsToRun; opIndex++ {
		// Check for context cancellation (YCSB pattern - check every operation)
		select {
		case <-ctx.Done():
			// Return partial results
			return WorkerResult{
				SuccessOps:    successOps,
				ErrorOps:      errorOps,
				Latencies:     latencies,
				TTFBDurations: ttfbDurations,
				TotalBytes:    totalBytes,
			}
		default:
		}

		// Execute operation with background context to prevent cancellation mid-flight
		result := wp.executeOperation(context.Background(), workerOps, workerID, opIndex, localRng, data)

		// Record results
		if result.Success {
			successOps++
			totalBytes += result.BytesRead
			latencies = append(latencies, result.Duration)
			if result.TTFB > 0 {
				ttfbDurations = append(ttfbDurations, result.TTFB)
			}
		} else {
			errorOps++
		}
	}

	return WorkerResult{
		SuccessOps:    successOps,
		ErrorOps:      errorOps,
		Latencies:     latencies,
		TTFBDurations: ttfbDurations,
		TotalBytes:    totalBytes,
	}
}

// runWorkerDuration runs operations for a duration for a single worker
func (wp *WorkerPool) runWorkerDuration(ctx context.Context, workerID int, seed int64) WorkerResult {
	// Create per-worker S3 client for isolated connection pool
	workerOps := wp.clientFactory()

	localRng := rand.New(rand.NewSource(seed))

	// Pre-allocate result slices with reasonable capacity
	latencies := make([]time.Duration, 0, 10000)
	ttfbDurations := make([]time.Duration, 0, 10000)

	var successOps, errorOps, totalBytes int64
	var opIndex int

	// Generate per-worker data buffer (for PUT operations)
	var data []byte
	if wp.config.OperationType == OpPUT || wp.config.OperationType == OpMIXED {
		data = generateDataWithRNG(localRng, wp.config.ObjectSize)
	}

	for {
		// Check for context cancellation (YCSB pattern - check every operation)
		select {
		case <-ctx.Done():
			// Return results when time is up
			return WorkerResult{
				SuccessOps:    successOps,
				ErrorOps:      errorOps,
				Latencies:     latencies,
				TTFBDurations: ttfbDurations,
				TotalBytes:    totalBytes,
			}
		default:
		}

		// Execute operation with background context to prevent cancellation mid-flight
		// The timeout context is only for stopping the loop, not cancelling operations
		result := wp.executeOperation(context.Background(), workerOps, workerID, opIndex, localRng, data)

		// Record results
		if result.Success {
			successOps++
			totalBytes += result.BytesRead
			latencies = append(latencies, result.Duration)
			if result.TTFB > 0 {
				ttfbDurations = append(ttfbDurations, result.TTFB)
			}
		} else {
			errorOps++
		}

		opIndex++
	}
}

// executeOperation executes a single S3 operation
func (wp *WorkerPool) executeOperation(ctx context.Context, ops *S3Operations, workerID, opIndex int, rng *rand.Rand, data []byte) OperationResult {
	// Get the key to operate on
	key := wp.keyGen.NextKey(workerID, opIndex, rng)

	// Execute the appropriate operation
	switch wp.config.OperationType {
	case OpPUT:
		// Use streaming upload for large objects, byte slice for small
		if wp.config.ObjectSize > ChunkedGenerationThreshold {
			return ops.PutObjectAuto(ctx, key, wp.config.ObjectSize)
		}
		return ops.PutObject(ctx, key, data)
	case OpGET:
		return ops.GetObject(ctx, key)
	case OpMIXED:
		// Decide whether to read or write based on ratio
		shouldRead := rng.Float64() < wp.config.ReadWriteRatio
		if shouldRead {
			return ops.GetObject(ctx, key)
		}
		// For writes, use streaming for large objects
		if wp.config.ObjectSize > ChunkedGenerationThreshold {
			return ops.PutObjectAuto(ctx, key, wp.config.ObjectSize)
		}
		return ops.PutObject(ctx, key, data)
	default:
		return OperationResult{Success: false, Error: &ConfigError{Field: "OperationType", Message: "unknown operation type"}}
	}
}
