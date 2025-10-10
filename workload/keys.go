package workload

import (
	"fmt"
	"math/rand"
)

// KeyGenerator generates S3 object keys based on the configured distribution
type KeyGenerator struct {
	prefix       string
	numObjects   int
	distribution KeyDistribution
	concurrency  int
}

// NewKeyGenerator creates a new key generator
func NewKeyGenerator(prefix string, numObjects int, distribution KeyDistribution, concurrency int) *KeyGenerator {
	return &KeyGenerator{
		prefix:       prefix,
		numObjects:   numObjects,
		distribution: distribution,
		concurrency:  concurrency,
	}
}

// NextKey returns the next key to use based on the distribution
// workerID identifies the worker, opIndex is the operation index, rng is for randomness
func (kg *KeyGenerator) NextKey(workerID, opIndex int, rng *rand.Rand) string {
	var keyNum int

	// Calculate objects per worker for this worker's range
	objectsPerWorker := kg.numObjects / kg.concurrency
	if objectsPerWorker == 0 {
		objectsPerWorker = 1
	}

	switch kg.distribution {
	case DistUniform:
		// Random uniform distribution within worker's object range
		// This ensures we only access objects that were preloaded by this worker
		keyNum = opIndex % objectsPerWorker
	case DistSequential:
		// Sequential access within worker's partition
		keyNum = opIndex % objectsPerWorker
	case DistZipfian:
		// Zipfian distribution (hot keys) - 80/20 rule approximation
		// Still within worker's range
		if rng.Float64() < 0.8 {
			// 80% of accesses go to 20% of keys
			keyNum = rng.Intn(objectsPerWorker / 5)
		} else {
			// 20% of accesses go to 80% of keys
			keyNum = objectsPerWorker/5 + rng.Intn((objectsPerWorker*4)/5)
		}
	default:
		// Default to sequential
		keyNum = opIndex % objectsPerWorker
	}

	return kg.FormatKey(workerID, keyNum)
}

// FormatKey formats a key with the given worker ID and key number
func (kg *KeyGenerator) FormatKey(workerID, keyNum int) string {
	if kg.prefix != "" {
		return fmt.Sprintf("%s/worker-%d/obj-%d", kg.prefix, workerID, keyNum)
	}
	return fmt.Sprintf("worker-%d/obj-%d", workerID, keyNum)
}

// PreloadKey returns the key for preloading at the given index
func (kg *KeyGenerator) PreloadKey(workerID, objIndex int) string {
	return kg.FormatKey(workerID, objIndex)
}
