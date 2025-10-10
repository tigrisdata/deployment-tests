package workload

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// OperationResult holds the result of a single S3 operation
type OperationResult struct {
	Success    bool
	Duration   time.Duration
	TTFB       time.Duration // Time to first byte (GET only)
	BytesRead  int64
	Error      error
}

// S3Operations provides high-level S3 operation wrappers
type S3Operations struct {
	client     *s3.Client
	bucketName string
}

// NewS3Operations creates a new S3 operations wrapper
func NewS3Operations(client *s3.Client, bucketName string) *S3Operations {
	return &S3Operations{
		client:     client,
		bucketName: bucketName,
	}
}

// PutObject uploads an object to S3
func (ops *S3Operations) PutObject(ctx context.Context, key string, data []byte) OperationResult {
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

// GetObject downloads an object from S3
func (ops *S3Operations) GetObject(ctx context.Context, key string) OperationResult {
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

		// Read the entire body to io.Discard for benchmarking
		bytesRead, _ = io.Copy(io.Discard, resp.Body)
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

// MixedOperation performs either a PUT or GET based on the ratio
// readRatio: 0.0 = always PUT, 1.0 = always GET, 0.5 = 50/50 mix
func (ops *S3Operations) MixedOperation(ctx context.Context, key string, data []byte, shouldRead bool) OperationResult {
	if shouldRead {
		return ops.GetObject(ctx, key)
	}
	return ops.PutObject(ctx, key, data)
}
