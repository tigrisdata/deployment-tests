package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tigrisdata/deployment-test/workload"
)

// TranscodeMetrics holds metrics for transcode operations
type TranscodeMetrics struct {
	TotalOps      int64
	SuccessOps    int64
	ErrorOps      int64
	AvgLatency    time.Duration
	P95Latency    time.Duration
	P99Latency    time.Duration
	AvgTTFB       time.Duration
	P95TTFB       time.Duration
	P99TTFB       time.Duration
	ThroughputMBs float64
	OpsPerSecond  float64
	Latencies     []time.Duration
	TTFBs         []time.Duration
}

// ConsistencyMetrics holds metrics for read-after-write consistency
type ConsistencyMetrics struct {
	TotalChecks         int64
	ImmediateReads      int64
	EventualReads       int64
	FailedReads         int64
	AvgLatency          time.Duration
	P95Latency          time.Duration
	P99Latency          time.Duration
	TargetMetPercentage float64 // Percentage meeting <200ms target
	Latencies           []time.Duration
}

// TranscodeTest implements the Test interface for transcoding workload testing
type TranscodeTest struct {
	validator *TigrisValidator
}

// NewTranscodeTest creates a new transcoding test
func NewTranscodeTest(validator *TigrisValidator) *TranscodeTest {
	return &TranscodeTest{
		validator: validator,
	}
}

// Name returns the display name of the test
func (t *TranscodeTest) Name() string {
	return "Transcoding Workload Tests"
}

// Type returns the type of test
func (t *TranscodeTest) Type() TestType {
	return TestTypeTranscode
}

// Setup performs preload of source files using per-worker S3 clients
func (t *TranscodeTest) Setup(ctx context.Context) error {
	cfg := t.validator.config.TranscodeConfig
	baseClient := t.validator.clients["global"]

	fmt.Printf("\n%sSetup Phase: Uploading %d source files (%s each)...%s\n",
		ColorBrightWhite, cfg.SourceFileCount, formatBytes(cfg.SourceFileSize), ColorReset)

	start := time.Now()
	var completed int32

	// Upload source files in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, cfg.SourceFileCount)

	for i := 0; i < cfg.SourceFileCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Create per-worker S3 client for isolated connection pool
			// This prevents connection pool contention between parallel uploads
			workerClient := workload.CreateWorkerS3Client(baseClient)
			ops := workload.NewS3Operations(workerClient, t.validator.config.BucketName, true, 10*1024*1024)

			key := fmt.Sprintf("%s/transcode/sources/video-%03d.bin", t.validator.config.Prefix, idx)

			// Use streaming upload with chunked data generation
			uploadStart := time.Now()
			result := ops.PutObjectAuto(ctx, key, cfg.SourceFileSize)
			uploadDuration := time.Since(uploadStart)

			if !result.Success {
				errChan <- fmt.Errorf("failed to upload source file %d: %w", idx, result.Error)
				return
			}

			current := atomic.AddInt32(&completed, 1)
			fmt.Printf("  Progress: %d/%d files uploaded (%s)\n",
				current, cfg.SourceFileCount, formatDuration(uploadDuration))
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	if len(errChan) > 0 {
		return <-errChan
	}

	duration := time.Since(start)
	totalSize := int64(cfg.SourceFileCount) * cfg.SourceFileSize
	fmt.Printf("  %sCompleted: %d files (%s total) in %s%s\n\n",
		ColorBrightGreen, cfg.SourceFileCount, formatBytes(totalSize), formatDuration(duration), ColorReset)

	return nil
}

// Run executes the transcoding workload test
func (t *TranscodeTest) Run(ctx context.Context) TestStatus {
	startTime := time.Now()

	fmt.Printf("\n%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" TRANSCODING WORKLOAD TESTS")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	cfg := t.validator.config.TranscodeConfig

	fmt.Printf("\nConfiguration:\n")
	fmt.Printf("  Source Files: %d files, %s each\n", cfg.SourceFileCount, formatBytes(cfg.SourceFileSize))
	fmt.Printf("  Chunk Size: %s per read\n", formatBytes(cfg.ChunkSize))
	fmt.Printf("  Segment Size: %s - %s per write\n", formatBytes(cfg.SegmentSizeMin), formatBytes(cfg.SegmentSizeMax))
	fmt.Printf("  Parallel Jobs: %d parallel jobs\n", cfg.JobCount)
	fmt.Printf("  Test Duration: %s\n", cfg.TestDuration)

	// Setup phase
	fmt.Printf("\n%s", strings.Repeat("-", 60))
	if err := t.Setup(ctx); err != nil {
		return TestStatus{
			Passed:   false,
			Duration: time.Since(startTime),
			Message:  fmt.Sprintf("Setup failed: %v", err),
			Details:  nil,
		}
	}

	// Run transcode simulation
	fmt.Printf("%s\n%sTranscoding Simulation (%d parallel jobs, %s duration):%s\n",
		strings.Repeat("-", 60), ColorBrightWhite, cfg.JobCount, cfg.TestDuration, ColorReset)

	readMetrics, writeMetrics, consistencyMetrics := t.runTranscodeSimulation(ctx)

	// Display results
	t.displayResults(readMetrics, writeMetrics, consistencyMetrics)

	// Cleanup
	fmt.Printf("\n%sCleanup Phase: Removing test objects...%s ", ColorBrightWhite, ColorReset)
	if err := t.Cleanup(ctx); err != nil {
		fmt.Printf("%sFAILED%s\n", ColorBrightRed, ColorReset)
	} else {
		fmt.Printf("%sDONE%s\n", ColorBrightGreen, ColorReset)
	}

	totalDuration := time.Since(startTime)
	fmt.Printf("\nTotal Duration: %s\n", formatDuration(totalDuration))

	// Determine pass/fail based on consistency metrics
	passed := consistencyMetrics.TargetMetPercentage >= 95.0 // 95% should meet <200ms target

	message := "Transcoding workload test completed"
	if !passed {
		message = "Transcoding workload test failed consistency requirements"
	}

	return TestStatus{
		Passed:   passed,
		Duration: totalDuration,
		Message:  message,
		Details: map[string]interface{}{
			"reads":       readMetrics,
			"writes":      writeMetrics,
			"consistency": consistencyMetrics,
		},
	}
}

// Cleanup performs cleanup after test
func (t *TranscodeTest) Cleanup(ctx context.Context) error {
	client := t.validator.clients["global"]
	prefix := fmt.Sprintf("%s/transcode/", t.validator.config.Prefix)

	// List all objects with the transcode prefix
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(t.validator.config.BucketName),
		Prefix: aws.String(prefix),
	})

	var objectsToDelete []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			objectsToDelete = append(objectsToDelete, *obj.Key)
		}
	}

	// Delete objects in batches
	for i := 0; i < len(objectsToDelete); i += 100 {
		end := i + 100
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}

		batch := objectsToDelete[i:end]
		for _, key := range batch {
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(t.validator.config.BucketName),
				Key:    aws.String(key),
			})
			if err != nil {
				return fmt.Errorf("failed to delete object %s: %w", key, err)
			}
		}
	}

	return nil
}

// workerTranscodeMetrics holds per-worker metrics to avoid lock contention
type workerTranscodeMetrics struct {
	readLatencies  []time.Duration
	readTTFBs      []time.Duration
	writeLatencies []time.Duration
	consLatencies  []time.Duration
	readSuccess    int64
	readErrors     int64
	writeSuccess   int64
	writeErrors    int64
	consImmediate  int64
	consEventual   int64
	consFailed     int64
}

// runTranscodeSimulation simulates the transcoding workload
func (t *TranscodeTest) runTranscodeSimulation(ctx context.Context) (*TranscodeMetrics, *TranscodeMetrics, *ConsistencyMetrics) {
	cfg := t.validator.config.TranscodeConfig
	baseClient := t.validator.clients["global"]

	// Create context with timeout
	simCtx, cancel := context.WithTimeout(ctx, cfg.TestDuration)
	defer cancel()

	// Estimate operations per worker for pre-allocation
	// Assume ~1 op/second per worker for duration = ~300 ops for 5 minutes
	estimatedOpsPerWorker := int(cfg.TestDuration.Seconds())

	var wg sync.WaitGroup
	workerMetricsChan := make(chan *workerTranscodeMetrics, cfg.JobCount)

	// Start worker jobs
	for i := 0; i < cfg.JobCount; i++ {
		wg.Add(1)
		go func(jobID int) {
			defer wg.Done()

			// Create per-worker S3 client for isolated connection pool
			workerClient := workload.CreateWorkerS3Client(baseClient)
			ops := workload.NewS3Operations(workerClient, t.validator.config.BucketName, false, 0)

			// Create per-worker RNG to avoid global lock contention
			workerRng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(jobID)))

			// Per-worker metrics (no locks needed!)
			localMetrics := &workerTranscodeMetrics{
				readLatencies:  make([]time.Duration, 0, estimatedOpsPerWorker),
				readTTFBs:      make([]time.Duration, 0, estimatedOpsPerWorker),
				writeLatencies: make([]time.Duration, 0, estimatedOpsPerWorker),
				consLatencies:  make([]time.Duration, 0, estimatedOpsPerWorker),
			}

			for {
				select {
				case <-simCtx.Done():
					// Send worker metrics to aggregation channel
					workerMetricsChan <- localMetrics
					return
				default:
					// Simulate encoder reading a chunk and writing output

					// 1. Read a chunk from random source file (range GET)
					sourceIdx := workerRng.Intn(cfg.SourceFileCount)
					sourceKey := fmt.Sprintf("%s/transcode/sources/video-%03d.bin", t.validator.config.Prefix, sourceIdx)

					// Calculate random offset within source file
					maxOffset := cfg.SourceFileSize - cfg.ChunkSize
					if maxOffset < 0 {
						maxOffset = 0
					}
					startByte := workerRng.Int63n(maxOffset + 1)
					endByte := startByte + cfg.ChunkSize - 1

					readResult := ops.GetObjectRange(simCtx, sourceKey, startByte, endByte)

					// Collect metrics without locks
					if readResult.Success {
						localMetrics.readSuccess++
						localMetrics.readLatencies = append(localMetrics.readLatencies, readResult.Duration)
						localMetrics.readTTFBs = append(localMetrics.readTTFBs, readResult.TTFB)
					} else {
						localMetrics.readErrors++
					}

					// 2. Write output segment (small file)
					segmentSize := cfg.SegmentSizeMin + workerRng.Int63n(cfg.SegmentSizeMax-cfg.SegmentSizeMin+1)
					segmentData := make([]byte, segmentSize)
					workerRng.Read(segmentData) // Use worker RNG, not global

					outputKey := fmt.Sprintf("%s/transcode/outputs/video-%03d/segment-%d-%d.bin",
						t.validator.config.Prefix, sourceIdx, jobID, time.Now().UnixNano())

					writeResult := ops.PutObject(simCtx, outputKey, segmentData)

					// Collect metrics without locks
					if writeResult.Success {
						localMetrics.writeSuccess++
						localMetrics.writeLatencies = append(localMetrics.writeLatencies, writeResult.Duration)
					} else {
						localMetrics.writeErrors++
					}

					// 3. Immediately read back to check consistency
					if writeResult.Success {
						readStart := time.Now()
						readBackResult := ops.GetObject(simCtx, outputKey)
						readBackLatency := time.Since(readStart)

						// Collect metrics without locks
						if readBackResult.Success {
							localMetrics.consLatencies = append(localMetrics.consLatencies, readBackLatency)
							if readBackLatency < 200*time.Millisecond {
								localMetrics.consImmediate++
							} else {
								localMetrics.consEventual++
							}
						} else {
							localMetrics.consFailed++
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(workerMetricsChan)

	// Aggregate metrics from all workers
	readMetrics := &TranscodeMetrics{}
	writeMetrics := &TranscodeMetrics{}
	consistencyMetrics := &ConsistencyMetrics{}

	// Estimate total capacity for pre-allocation
	totalOps := estimatedOpsPerWorker * cfg.JobCount
	readMetrics.Latencies = make([]time.Duration, 0, totalOps)
	readMetrics.TTFBs = make([]time.Duration, 0, totalOps)
	writeMetrics.Latencies = make([]time.Duration, 0, totalOps)
	consistencyMetrics.Latencies = make([]time.Duration, 0, totalOps)

	// Combine all worker metrics
	for workerMetrics := range workerMetricsChan {
		// Aggregate read metrics
		readMetrics.SuccessOps += workerMetrics.readSuccess
		readMetrics.ErrorOps += workerMetrics.readErrors
		readMetrics.Latencies = append(readMetrics.Latencies, workerMetrics.readLatencies...)
		readMetrics.TTFBs = append(readMetrics.TTFBs, workerMetrics.readTTFBs...)

		// Aggregate write metrics
		writeMetrics.SuccessOps += workerMetrics.writeSuccess
		writeMetrics.ErrorOps += workerMetrics.writeErrors
		writeMetrics.Latencies = append(writeMetrics.Latencies, workerMetrics.writeLatencies...)

		// Aggregate consistency metrics
		consistencyMetrics.ImmediateReads += workerMetrics.consImmediate
		consistencyMetrics.EventualReads += workerMetrics.consEventual
		consistencyMetrics.FailedReads += workerMetrics.consFailed
		consistencyMetrics.TotalChecks += workerMetrics.consImmediate + workerMetrics.consEventual + workerMetrics.consFailed
		consistencyMetrics.Latencies = append(consistencyMetrics.Latencies, workerMetrics.consLatencies...)
	}

	// Calculate statistics
	t.calculateMetrics(readMetrics, cfg.TestDuration)
	t.calculateMetrics(writeMetrics, cfg.TestDuration)
	t.calculateConsistencyMetrics(consistencyMetrics)

	return readMetrics, writeMetrics, consistencyMetrics
}

// calculateMetrics calculates statistics for transcode metrics
func (t *TranscodeTest) calculateMetrics(m *TranscodeMetrics, duration time.Duration) {
	if len(m.Latencies) > 0 {
		m.AvgLatency = average(m.Latencies)
		m.P95Latency = percentile(m.Latencies, 0.95)
		m.P99Latency = percentile(m.Latencies, 0.99)
	}

	if len(m.TTFBs) > 0 {
		m.AvgTTFB = average(m.TTFBs)
		m.P95TTFB = percentile(m.TTFBs, 0.95)
		m.P99TTFB = percentile(m.TTFBs, 0.99)
	}

	if duration > 0 {
		m.OpsPerSecond = float64(m.SuccessOps) / duration.Seconds()
	}
}

// calculateConsistencyMetrics calculates consistency statistics
func (t *TranscodeTest) calculateConsistencyMetrics(m *ConsistencyMetrics) {
	if len(m.Latencies) > 0 {
		m.AvgLatency = average(m.Latencies)
		m.P95Latency = percentile(m.Latencies, 0.95)
		m.P99Latency = percentile(m.Latencies, 0.99)
	}

	if m.TotalChecks > 0 {
		m.TargetMetPercentage = float64(m.ImmediateReads) * 100.0 / float64(m.TotalChecks)
	}
}

// displayResults displays the test results
func (t *TranscodeTest) displayResults(read, write *TranscodeMetrics, cons *ConsistencyMetrics) {
	fmt.Printf("\n%sRead Operations (Range Requests, %s chunks):%s\n",
		ColorBrightWhite, formatBytes(t.validator.config.TranscodeConfig.ChunkSize), ColorReset)
	fmt.Printf("  Latency    - %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s\n",
		ColorBrightWhite, ColorReset, formatDurationAligned(read.AvgLatency),
		ColorBrightWhite, ColorReset, formatDurationAligned(read.P95Latency),
		ColorBrightWhite, ColorReset, formatDurationAligned(read.P99Latency))
	fmt.Printf("  TTFB       - %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s\n",
		ColorBrightWhite, ColorReset, formatDurationAligned(read.AvgTTFB),
		ColorBrightWhite, ColorReset, formatDurationAligned(read.P95TTFB),
		ColorBrightWhite, ColorReset, formatDurationAligned(read.P99TTFB))
	fmt.Printf("  Throughput - %s%.2f ops/s%s | %s%d success%s",
		ColorBrightWhite, read.OpsPerSecond, ColorReset,
		ColorBrightGreen, read.SuccessOps, ColorReset)
	if read.ErrorOps > 0 {
		fmt.Printf(", %s%d failed%s", ColorBrightRed, read.ErrorOps, ColorReset)
	}
	fmt.Printf("\n")

	fmt.Printf("\n%sWrite Operations (Output Segments, %s - %s):%s\n",
		ColorBrightWhite,
		formatBytes(t.validator.config.TranscodeConfig.SegmentSizeMin),
		formatBytes(t.validator.config.TranscodeConfig.SegmentSizeMax),
		ColorReset)
	fmt.Printf("  Latency    - %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s\n",
		ColorBrightWhite, ColorReset, formatDurationAligned(write.AvgLatency),
		ColorBrightWhite, ColorReset, formatDurationAligned(write.P95Latency),
		ColorBrightWhite, ColorReset, formatDurationAligned(write.P99Latency))
	fmt.Printf("  Throughput - %s%.2f ops/s%s | %s%d success%s",
		ColorBrightWhite, write.OpsPerSecond, ColorReset,
		ColorBrightGreen, write.SuccessOps, ColorReset)
	if write.ErrorOps > 0 {
		fmt.Printf(", %s%d failed%s", ColorBrightRed, write.ErrorOps, ColorReset)
	}
	fmt.Printf("\n")

	fmt.Printf("\n%sRead-After-Write Consistency:%s\n", ColorBrightWhite, ColorReset)
	fmt.Printf("  Convergence - %sAvg:%s %s, %sP95:%s %s, %sP99:%s %s\n",
		ColorBrightWhite, ColorReset, formatDurationAligned(cons.AvgLatency),
		ColorBrightWhite, ColorReset, formatDurationAligned(cons.P95Latency),
		ColorBrightWhite, ColorReset, formatDurationAligned(cons.P99Latency))

	immediatePercent := 0.0
	eventualPercent := 0.0
	failedPercent := 0.0
	if cons.TotalChecks > 0 {
		immediatePercent = float64(cons.ImmediateReads) * 100.0 / float64(cons.TotalChecks)
		eventualPercent = float64(cons.EventualReads) * 100.0 / float64(cons.TotalChecks)
		failedPercent = float64(cons.FailedReads) * 100.0 / float64(cons.TotalChecks)
	}

	fmt.Printf("  Distribution - Immediate (<200ms): %5.1f%%, Eventual (>200ms): %5.1f%%, Failed: %5.1f%%\n",
		immediatePercent, eventualPercent, failedPercent)

	targetColor := ColorBrightGreen
	if cons.TargetMetPercentage < 95.0 {
		targetColor = ColorBrightRed
	}
	fmt.Printf("  Target (<200ms): %s%.1f%% within target%s\n",
		targetColor, cons.TargetMetPercentage, ColorReset)
}

// Helper functions for statistics
func average(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}

func percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)

	// Simple bubble sort for small datasets
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// formatBytes formats bytes into human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
