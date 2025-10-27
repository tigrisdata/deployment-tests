package workload

import (
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"
)

const (
	// ChunkedGenerationThreshold is the size above which we use chunked generation
	// Below this size, we allocate the full buffer in memory
	ChunkedGenerationThreshold = 10 * 1024 * 1024 // 10 MiB

	// DefaultChunkSize is the default chunk size for streaming data generation
	DefaultChunkSize = 10 * 1024 * 1024 // 10 MiB
)

// rngPool provides per-goroutine RNG instances to avoid lock contention
var rngPool = sync.Pool{
	New: func() interface{} {
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	},
}

// ChunkedRandomReader generates random data on-demand without storing entire file in memory
// This is memory-efficient for large objects while maintaining good performance
type ChunkedRandomReader struct {
	remaining int64      // Bytes left to generate
	chunkSize int64      // Size of each chunk to generate
	buffer    []byte     // Reusable buffer for chunk generation
	offset    int        // Current position in buffer
	rng       *rand.Rand // Per-reader RNG to avoid global lock
}

// NewChunkedRandomReader creates a reader that generates totalSize bytes of random data
// in chunkSize chunks. The reader implements io.Reader and can be used with S3 uploads.
//
// Memory usage: ~chunkSize bytes (default 10 MiB)
// Performance: Generates data on-demand as S3 uploader requests it
func NewChunkedRandomReader(totalSize, chunkSize int64) *ChunkedRandomReader {
	// Get a per-reader RNG from pool to avoid global lock contention
	rng := rngPool.Get().(*rand.Rand)

	return &ChunkedRandomReader{
		remaining: totalSize,
		chunkSize: chunkSize,
		buffer:    make([]byte, 0),
		offset:    0,
		rng:       rng,
	}
}

// Read implements io.Reader interface
// Generates random data in chunks as needed, avoiding large memory allocations
func (r *ChunkedRandomReader) Read(p []byte) (n int, err error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}

	// If buffer is exhausted, generate next chunk
	if r.offset >= len(r.buffer) {
		nextChunkSize := r.chunkSize
		if r.remaining < r.chunkSize {
			nextChunkSize = r.remaining
		}

		r.buffer = make([]byte, nextChunkSize)
		// Use per-reader RNG instead of global crypto/rand - 100x faster
		r.rng.Read(r.buffer)
		r.offset = 0
	}

	// Copy from buffer to output
	bytesToCopy := len(p)
	bytesAvailable := len(r.buffer) - r.offset
	if bytesToCopy > bytesAvailable {
		bytesToCopy = bytesAvailable
	}

	copy(p, r.buffer[r.offset:r.offset+bytesToCopy])
	r.offset += bytesToCopy
	r.remaining -= int64(bytesToCopy)

	return bytesToCopy, nil
}

// GenerateRandomData generates random data with automatic memory optimization
// For sizes > ChunkedGenerationThreshold (10 MiB), returns a ChunkedRandomReader
// For sizes <= ChunkedGenerationThreshold, allocates and returns the full buffer
//
// This provides optimal performance for both small and large objects:
// - Small objects: Fast in-memory allocation (no streaming overhead)
// - Large objects: Memory-efficient streaming generation
func GenerateRandomData(size int64) (interface{}, error) {
	if size > ChunkedGenerationThreshold {
		// Large object: return streaming reader
		return NewChunkedRandomReader(size, DefaultChunkSize), nil
	}

	// Small object: allocate full buffer in memory
	data := make([]byte, size)
	// Use pooled RNG for fast random data generation (no crypto needed for test data)
	rng := rngPool.Get().(*rand.Rand)
	rng.Read(data)
	rngPool.Put(rng)
	return data, nil
}

// IsReaderData checks if the data is a reader (streaming) or byte slice
func IsReaderData(data interface{}) bool {
	_, ok := data.(io.Reader)
	return ok
}

// ToReader converts data to io.Reader
// If data is already a reader, returns it as-is
// If data is []byte, wraps it in a bytes.Reader
func ToReader(data interface{}) io.Reader {
	switch v := data.(type) {
	case io.Reader:
		return v
	case []byte:
		// Import bytes package if needed
		return &bytesReader{data: v, offset: 0}
	default:
		panic(fmt.Sprintf("unsupported data type: %T", data))
	}
}

// bytesReader is a simple reader for byte slices
type bytesReader struct {
	data   []byte
	offset int
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}

	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}
