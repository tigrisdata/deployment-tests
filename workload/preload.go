package workload

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
)

// PreloadPhase handles the preloading of objects before benchmark execution
type PreloadPhase struct {
	ops          *S3Operations
	keyGen       *KeyGenerator
	config       *WorkloadConfig
	workerSeeds  []int64
}

// NewPreloadPhase creates a new preload phase handler
func NewPreloadPhase(ops *S3Operations, keyGen *KeyGenerator, config *WorkloadConfig, workerSeeds []int64) *PreloadPhase {
	return &PreloadPhase{
		ops:         ops,
		keyGen:      keyGen,
		config:      config,
		workerSeeds: workerSeeds,
	}
}

// Run executes the preload phase
func (p *PreloadPhase) Run(ctx context.Context) error {
	var preloadErrors atomic.Int32
	var successCount atomic.Int32
	var wg sync.WaitGroup

	// Calculate records per worker
	recordsPerWorker := p.config.RecordCount / p.config.Concurrency
	if recordsPerWorker == 0 {
		recordsPerWorker = 1
	}

	// Start worker goroutines for parallel preloading
	for workerID := 0; workerID < p.config.Concurrency; workerID++ {
		wg.Add(1)
		seed := p.workerSeeds[workerID]

		go func(wID int, s int64) {
			defer wg.Done()

			// Per-worker RNG for data generation
			localRng := rand.New(rand.NewSource(s))
			data := generateDataWithRNG(localRng, p.config.ObjectSize)

			for i := 0; i < recordsPerWorker; i++ {
				// Check for context cancellation every 10 objects
				if i%10 == 0 {
					select {
					case <-ctx.Done():
						return
					default:
					}
				}

				key := p.keyGen.PreloadKey(wID, i)
				result := p.ops.PutObject(ctx, key, data)

				if result.Success {
					successCount.Add(1)
				} else {
					preloadErrors.Add(1)
				}
			}
		}(workerID, seed)
	}

	wg.Wait()

	// Check if we have too many errors
	totalErrors := preloadErrors.Load()
	totalSuccess := successCount.Load()

	if totalErrors > int32(p.config.RecordCount/10) {
		return fmt.Errorf("preload failed: %d errors out of %d attempts", totalErrors, totalSuccess+totalErrors)
	}

	return nil
}

// generateDataWithRNG creates random data using a specific RNG instance
func generateDataWithRNG(rng *rand.Rand, size int64) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(rng.Intn(256))
	}
	return data
}
