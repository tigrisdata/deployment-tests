package workload

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// bufferPool is a sync.Pool for reusing read buffers during GET operations
// This reduces memory allocations and GC pressure for high-throughput workloads
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Allocate 256KB buffer - good balance for most object sizes
		buf := make([]byte, 256*1024)
		return &buf
	},
}

// discardWriterAt is an io.WriterAt that discards all data but counts bytes
// This is used with the S3 downloader for benchmarking without storing data
type discardWriterAt struct {
	written int64
	mu      sync.Mutex
}

func (d *discardWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	d.mu.Lock()
	d.written += int64(len(p))
	d.mu.Unlock()
	return len(p), nil
}

func (d *discardWriterAt) BytesWritten() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.written
}

// OperationResult holds the result of a single S3 operation
type OperationResult struct {
	Success   bool
	Duration  time.Duration
	TTFB      time.Duration // Time to first byte (GET only)
	BytesRead int64
	Error     error
}

// S3Operations provides high-level S3 operation wrappers
type S3Operations struct {
	client        *s3.Client
	bucketName    string
	useMultipart  bool
	multipartSize int64
	uploader      *manager.Uploader   // AWS SDK's built-in multipart uploader
	downloader    *manager.Downloader // AWS SDK's built-in parallel downloader
}

// NewS3Operations creates a new S3 operations wrapper
func NewS3Operations(client *s3.Client, bucketName string, useMultipart bool, multipartSize int64) *S3Operations {
	const minPartSize = 5 * 1024 * 1024 // AWS S3 minimum part size is 5 MiB

	// Enforce AWS minimum part size
	if useMultipart && multipartSize < minPartSize {
		multipartSize = minPartSize
	}

	// Create AWS SDK's built-in uploader with optimized settings
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = multipartSize
		u.Concurrency = 10          // Upload up to 10 parts in parallel
		u.LeavePartsOnError = false // Clean up failed uploads
	})

	// Create AWS SDK's built-in downloader with optimized settings
	downloader := manager.NewDownloader(client, func(d *manager.Downloader) {
		d.PartSize = multipartSize
		d.Concurrency = 10       // Download up to 10 parts in parallel
		d.PartBodyMaxRetries = 3 // Retry failed parts
	})

	return &S3Operations{
		client:        client,
		bucketName:    bucketName,
		useMultipart:  useMultipart,
		multipartSize: multipartSize,
		uploader:      uploader,
		downloader:    downloader,
	}
}

// PutObject uploads an object to S3
func (ops *S3Operations) PutObject(ctx context.Context, key string, data []byte) OperationResult {
	// Use multipart upload for large objects if enabled
	if ops.useMultipart && int64(len(data)) >= ops.multipartSize {
		return ops.putObjectMultipart(ctx, key, data)
	}

	// Standard single-part upload
	start := time.Now()

	_, err := ops.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(ops.bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	duration := time.Since(start)

	return OperationResult{
		Success:   err == nil,
		Duration:  duration,
		BytesRead: int64(len(data)),
		Error:     err,
	}
}

// putObjectMultipart uploads an object using AWS SDK's built-in multipart uploader
// This automatically handles parallel part uploads, retries, and cleanup
func (ops *S3Operations) putObjectMultipart(ctx context.Context, key string, data []byte) OperationResult {
	start := time.Now()

	// Use AWS SDK's built-in uploader - it handles:
	// - Automatic multipart upload for large objects
	// - Parallel part uploads (configurable concurrency)
	// - Automatic retry and error handling
	// - Cleanup on failure
	_, err := ops.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(ops.bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	duration := time.Since(start)

	return OperationResult{
		Success:   err == nil,
		Duration:  duration,
		BytesRead: int64(len(data)),
		Error:     err,
	}
}

// GetObject downloads an object from S3
func (ops *S3Operations) GetObject(ctx context.Context, key string) OperationResult {
	// Use parallel download for large objects if multipart is enabled
	// The downloader will automatically use parallel range requests
	if ops.useMultipart {
		return ops.getObjectParallel(ctx, key)
	}

	// Standard single-request download for small objects
	start := time.Now()

	resp, err := ops.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ops.bucketName),
		Key:    aws.String(key),
	})

	var ttfb time.Duration
	var bytesRead int64

	if err == nil && resp.Body != nil {
		// TTFB is measured when we get the response
		ttfb = time.Since(start)

		// Get buffer from pool for reading
		bufPtr := bufferPool.Get().(*[]byte)
		buf := *bufPtr

		// Read the entire body using pooled buffer
		// This reduces allocations compared to io.Copy with io.Discard
		for {
			n, readErr := resp.Body.Read(buf)
			bytesRead += int64(n)
			if readErr != nil {
				if readErr != io.EOF {
					err = readErr
				}
				break
			}
		}

		// Return buffer to pool for reuse
		bufferPool.Put(bufPtr)
		resp.Body.Close()
	}

	duration := time.Since(start)

	return OperationResult{
		Success:   err == nil,
		Duration:  duration,
		TTFB:      ttfb,
		BytesRead: bytesRead,
		Error:     err,
	}
}

// getObjectParallel downloads an object using AWS SDK's parallel downloader
// This automatically splits the download into parallel range requests
func (ops *S3Operations) getObjectParallel(ctx context.Context, key string) OperationResult {
	start := time.Now()

	// Create a discard writer that counts bytes
	writer := &discardWriterAt{}

	// Use AWS SDK's downloader - it handles:
	// - Automatic parallel range requests
	// - Retry logic for failed parts
	// - Optimal chunking strategy
	bytesDownloaded, err := ops.downloader.Download(ctx, writer, &s3.GetObjectInput{
		Bucket: aws.String(ops.bucketName),
		Key:    aws.String(key),
	})

	duration := time.Since(start)

	// For parallel downloads, TTFB is when first chunk arrives
	// The downloader doesn't expose this directly, so we estimate it
	// as a fraction of total time (first chunk typically arrives quickly)
	ttfb := duration / 10 // Rough estimate

	return OperationResult{
		Success:   err == nil,
		Duration:  duration,
		TTFB:      ttfb,
		BytesRead: bytesDownloaded,
		Error:     err,
	}
}

// MixedOperation performs either a PUT or GET based on the ratio
// readRatio: 0.0 = always PUT, 1.0 = always GET, 0.5 = 50/50 mix
func (ops *S3Operations) MixedOperation(ctx context.Context, key string, data []byte, shouldRead bool) OperationResult {
	if shouldRead {
		return ops.GetObject(ctx, key)
	}
	return ops.PutObject(ctx, key, data)
}
