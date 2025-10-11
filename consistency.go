package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/transport/http"
)

// ConvergenceMetric represents a single convergence measurement
type ConvergenceMetric struct {
	Attempts        int
	ConvergenceTime time.Duration
	Immediate       bool // True if converged on first attempt
	TimedOut        bool // True if timed out without converging
}

// ConvergenceStats holds aggregated statistics from multiple iterations
type ConvergenceStats struct {
	Iterations       int
	AvgTime          time.Duration
	P95Time          time.Duration
	P99Time          time.Duration
	ImmediateCount   int
	EventualCount    int
	TimeoutCount     int
	ConvergenceTimes []time.Duration // For percentile calculation
}

// calculateStats computes statistics from collected metrics
func calculateStats(metrics []ConvergenceMetric) ConvergenceStats {
	stats := ConvergenceStats{
		Iterations:       len(metrics),
		ConvergenceTimes: make([]time.Duration, 0, len(metrics)),
	}

	if len(metrics) == 0 {
		return stats
	}

	var totalTime time.Duration
	for _, m := range metrics {
		if m.TimedOut {
			stats.TimeoutCount++
			continue
		}

		if m.Immediate {
			stats.ImmediateCount++
		} else {
			stats.EventualCount++
		}

		stats.ConvergenceTimes = append(stats.ConvergenceTimes, m.ConvergenceTime)
		totalTime += m.ConvergenceTime
	}

	successfulCount := stats.ImmediateCount + stats.EventualCount
	if successfulCount > 0 {
		stats.AvgTime = totalTime / time.Duration(successfulCount)

		// Sort for percentile calculation
		sort.Slice(stats.ConvergenceTimes, func(i, j int) bool {
			return stats.ConvergenceTimes[i] < stats.ConvergenceTimes[j]
		})

		// Calculate P95
		p95Index := int(float64(len(stats.ConvergenceTimes)) * 0.95)
		if p95Index >= len(stats.ConvergenceTimes) {
			p95Index = len(stats.ConvergenceTimes) - 1
		}
		stats.P95Time = stats.ConvergenceTimes[p95Index]

		// Calculate P99
		p99Index := int(float64(len(stats.ConvergenceTimes)) * 0.99)
		if p99Index >= len(stats.ConvergenceTimes) {
			p99Index = len(stats.ConvergenceTimes) - 1
		}
		stats.P99Time = stats.ConvergenceTimes[p99Index]
	}

	return stats
}

// getRegionDisplayName returns the region name extracted from the endpoint URL
func getRegionDisplayName(region string) string {
	// Extract region from endpoint URL by taking the first component
	// For example: "https://iad1.storage.dev" -> "iad1"
	if strings.HasPrefix(region, "https://") {
		// Remove "https://" and take the first part before "."
		urlPart := strings.TrimPrefix(region, "https://")
		parts := strings.Split(urlPart, ".")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	return region
}

func WithHeader(key, value string) func(*s3.Options) {
	return func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, http.AddHeaderValue(key, value))
	}
}

func applyRemoteRegionsChecks(regionToClients map[string]*s3.Client, regions []string, bucket string, key string) {
	const iterations = 50

	clog := Start(fmt.Sprintf("PUT|GET (Read-After-Write Consistency) (%d iterations)", iterations), Opts{ID: "T1", Region: regions})
	overallStart := time.Now()

	// Collect metrics for each region pair (including same region)
	regionMetrics := make(map[string][]ConvergenceMetric)
	for i := 0; i < len(regions); i++ {
		regionMetrics[regions[i]] = make([]ConvergenceMetric, 0, iterations)
	}

	// Run multiple iterations
	for iter := 0; iter < iterations; iter++ {
		// Use unique key for each iteration
		iterKey := fmt.Sprintf("%s-iter-%d", key, iter)

		eTagToValidate := put(regionToClients[regions[0]], bucket, iterKey)
		if eTagToValidate == "" {
			clog.Infof("PUT operation failed on iteration %d", iter)
			continue
		}

		// Validate in all regions (including same region)
		for i := 0; i < len(regions); i++ {
			metric := validateETag(regionToClients[regions[i]], regions[0], regions[i], bucket, iterKey, eTagToValidate, clog, false)
			regionMetrics[regions[i]] = append(regionMetrics[regions[i]], metric)
		}
	}

	// Calculate and display statistics for each region
	for i := 0; i < len(regions); i++ {
		stats := calculateStats(regionMetrics[regions[i]])
		clog.StatsSummaryf(regions[0], regions[i], stats)
	}

	clog.Successf(time.Since(overallStart), "Read-After-Write Consistency test completed")
}

func applyListConsistencyChecks(regionToClients map[string]*s3.Client, regions []string, bucket string) {
	const iterations = 10

	clog := Start(fmt.Sprintf("PUT|LIST (List-After-Write Consistency) (%d iterations)", iterations), Opts{ID: "T2", Region: regions})
	overallStart := time.Now()

	// Collect metrics for each region pair (including same region)
	regionMetrics := make(map[string][]ConvergenceMetric)
	for i := 0; i < len(regions); i++ {
		regionMetrics[regions[i]] = make([]ConvergenceMetric, 0, iterations)
	}

	// Run multiple iterations
	for iter := 0; iter < iterations; iter++ {
		// Create unique prefix and keys for this iteration
		prefix := fmt.Sprintf("list-iter-%d-", iter)
		keys := []string{
			fmt.Sprintf("%sobj-1", prefix),
			fmt.Sprintf("%sobj-2", prefix),
			fmt.Sprintf("%sobj-3", prefix),
		}

		// PUT objects to source region
		var resPut map[string]string
		var err error
		for attempts := 0; attempts < 10; attempts++ {
			resPut, err = putResults(regionToClients[regions[0]], bucket, keys)
			if err == nil {
				break
			}
		}
		if err != nil {
			clog.Infof("PUT operation failed on iteration %d", iter)
			continue
		}

		// Validate in all regions (including same region)
		for i := 0; i < len(regions); i++ {
			attempts, convergenceTime, passed := validateRegionsList(regionToClients[regions[i]], bucket, resPut, prefix, 3)

			metric := ConvergenceMetric{
				Attempts:        attempts,
				ConvergenceTime: convergenceTime,
				Immediate:       attempts == 0,
				TimedOut:        !passed,
			}
			regionMetrics[regions[i]] = append(regionMetrics[regions[i]], metric)
		}
	}

	// Calculate and display statistics for each region
	for i := 0; i < len(regions); i++ {
		stats := calculateStats(regionMetrics[regions[i]])
		clog.StatsSummaryf(regions[0], regions[i], stats)
	}

	clog.Successf(time.Since(overallStart), "List-After-Write Consistency test completed")
}

// validateRegionsList validates list consistency with a specific prefix and returns metrics
// Returns: (attempts, convergenceTime, passed)
func validateRegionsList(s3client *s3.Client, bucket string, resPut map[string]string, prefix string, limit int) (int, time.Duration, bool) {
	const (
		maxDuration     = 1 * time.Minute
		pollingInterval = 100 * time.Millisecond
		maxAttempts     = 600 // Safety limit
	)

	var (
		firstPollTime time.Time
		passed        = false
	)

	for attempts := 0; attempts < maxAttempts; attempts++ {
		pollTime := time.Now()
		resList, err := listResults(s3client, bucket, prefix, limit)
		// Handle errors - distinguish from mismatch
		if err != nil {
			// Check timeout before continuing
			if !firstPollTime.IsZero() && time.Since(firstPollTime) > maxDuration {
				break
			}
			time.Sleep(pollingInterval)
			continue
		}

		// Record first successful poll time for timeout tracking
		if firstPollTime.IsZero() {
			firstPollTime = pollTime
		}

		// Check if count matches
		if len(resPut) != len(resList) {
			// Check timeout before next attempt
			if time.Since(firstPollTime) > maxDuration {
				break
			}
			time.Sleep(pollingInterval)
			continue
		}

		// Check if all keys and ETags match
		passed = func() bool {
			for k, v := range resPut {
				vv, ok := resList[k]
				if !ok {
					return false
				}
				if vv != v {
					return false
				}
			}
			return true
		}()

		if passed {
			// Calculate convergence time based on polling intervals elapsed
			convergenceTime := time.Duration(attempts) * pollingInterval
			return attempts, convergenceTime, true
		}

		// Check timeout before next attempt
		if time.Since(firstPollTime) > maxDuration {
			break
		}

		// Sleep before next poll
		time.Sleep(pollingInterval)
	}

	return 0, 0, false
}

// validateETag validates ETag and returns convergence metrics
func validateETag(s3Client *s3.Client, sourceRegion string, remoteRegion string, bucket string, key string, expectedEtag string, clog *Logger, verbose bool) ConvergenceMetric {
	if verbose {
		clog.Infof("attempting read in remote region %s%s%s now\n", ColorYellow, getRegionDisplayName(remoteRegion), ColorReset)
	}

	const (
		maxDuration     = 1 * time.Minute
		pollingInterval = 100 * time.Millisecond
		maxAttempts     = 600 // Safety limit
	)

	var firstPollTime time.Time

	for attempts := 0; attempts < maxAttempts; attempts++ {
		pollTime := time.Now()
		eTagRemote, err := getEtagWithError(s3Client, bucket, key)
		// Handle errors - distinguish from ETag mismatch
		if err != nil {
			if verbose {
				clog.Infof("attempt '%d' GetObject failed: %v (will retry)", attempts, err)
			}

			// Check timeout before continuing
			if !firstPollTime.IsZero() && time.Since(firstPollTime) > maxDuration {
				if verbose {
					clog.Failf("timeout after %v waiting for ETag=%s written-to=%s replicated-to=%s",
						time.Since(firstPollTime), expectedEtag, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion))
				}
				return ConvergenceMetric{Attempts: attempts + 1, TimedOut: true}
			}

			time.Sleep(pollingInterval)
			continue
		}

		// Record first successful poll time for timeout tracking
		if firstPollTime.IsZero() {
			firstPollTime = pollTime
		}

		// Check if ETag matches
		if expectedEtag == eTagRemote {
			convergenceTime := time.Duration(attempts) * pollingInterval
			immediate := attempts == 0

			if verbose {
				if immediate {
					// Immediate success - no convergence time needed
					clog.Successf(0, "ETag %s immediately consistent in region %s", eTagRemote, getRegionDisplayName(remoteRegion))
				} else {
					// Calculate convergence time based on polling intervals elapsed
					clog.SuccessAfterf(attempts, convergenceTime, "Converged after ETag %s written at region %s remote region %s", eTagRemote, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion))
				}
			}

			return ConvergenceMetric{
				Attempts:        attempts,
				ConvergenceTime: convergenceTime,
				Immediate:       immediate,
				TimedOut:        false,
			}
		}

		if verbose {
			clog.Infof("retrying attempt '%d' waiting for ETag=%s written-to=%s replicated-to=%s dest has ETag=%s",
				attempts, expectedEtag, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion), eTagRemote)
		}

		// Check timeout before next attempt
		if time.Since(firstPollTime) > maxDuration {
			if verbose {
				clog.Failf("timeout after %v (attempts: %d) waiting for ETag=%s written-to=%s replicated-to=%s dest has ETag=%s",
					time.Since(firstPollTime), attempts+1, expectedEtag, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion), eTagRemote)
			}
			return ConvergenceMetric{Attempts: attempts + 1, TimedOut: true}
		}

		// Sleep before next poll
		time.Sleep(pollingInterval)
	}

	return ConvergenceMetric{Attempts: maxAttempts, TimedOut: true}
}

func put(cl *s3.Client, buc string, key string) string {
	kb64 := make([]byte, 32*1024)
	rand.Read(kb64)

	res, err := cl.PutObject(
		context.TODO(),
		&s3.PutObjectInput{
			Bucket: aws.String(buc),
			Key:    aws.String(key),
			Body:   bytes.NewReader(kb64),
		},
		WithHeader("Cache-Control", "no-cache"),
	)
	if err != nil {
		log.Printf("unexpected put failed '%v'", err)
		return ""
	}

	return *res.ETag
}

// getEtagWithError returns the ETag and any error encountered
func getEtagWithError(cl *s3.Client, buc string, key string) (string, error) {
	og, err := cl.GetObject(context.TODO(),
		&s3.GetObjectInput{
			Bucket: aws.String(buc),
			Key:    aws.String(key),
		},
		WithHeader("Cache-Control", "no-cache"),
	)
	if err != nil {
		return "", err
	}
	defer og.Body.Close()
	_, _ = io.ReadAll(og.Body)

	if og.ETag == nil {
		return "", fmt.Errorf("ETag is nil")
	}
	return *og.ETag, nil
}

func putResults(cl *s3.Client, buc string, keys []string) (map[string]string, error) {
	kb64 := make([]byte, 32*1024)
	_, _ = rand.Read(kb64)

	results := make(map[string]string)
	for _, key := range keys {
		o, err := cl.PutObject(
			context.TODO(),
			&s3.PutObjectInput{
				Bucket: aws.String(buc),
				Key:    aws.String(key),
				Body:   bytes.NewReader(kb64),
			},
			WithHeader("Cache-Control", "no-cache"),
		)
		if err != nil {
			return nil, err
		}
		results[key] = *o.ETag
	}

	return results, nil
}

func listResults(cl *s3.Client, buc string, prefix string, limit int) (map[string]string, error) {
	keyToETag := make(map[string]string)
	ol, err := cl.ListObjects(
		context.TODO(),
		&s3.ListObjectsInput{
			Bucket:  aws.String(buc),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int32(int32(limit)),
		},
	)
	if err != nil {
		return nil, err
	}
	for _, oo := range ol.Contents {
		keyToETag[*oo.Key] = *oo.ETag
	}

	return keyToETag, nil
}

// Logger represents a consistency test logger
type Logger struct {
	testName string
	opts     Opts
}

// Opts represents options for consistency tests
type Opts struct {
	ID     string
	Region []string
}

// Start creates a new consistency log
func Start(testName string, opts Opts) *Logger {
	fmt.Printf("\n%s%s%s\n", ColorBrightWhite, testName, ColorReset)
	return &Logger{
		testName: testName,
		opts:     opts,
	}
}

// Successf logs a success message
func (c *Logger) Successf(duration time.Duration, format string, args ...interface{}) {
	fmt.Printf("  %sSUCCESS%s - %s (%s%s%s)\n\n",
		ColorBrightGreen, ColorReset,
		fmt.Sprintf(format, args...),
		ColorBrightWhite, formatDuration(duration), ColorReset)
}

// Failf logs a failure message
func (c *Logger) Failf(format string, args ...interface{}) {
	fmt.Printf("  %sFAILED%s - %s\n",
		ColorBrightRed, ColorReset,
		fmt.Sprintf(format, args...))
}

// Infof logs an info message
func (c *Logger) Infof(format string, args ...interface{}) {
	fmt.Printf("  INFO - %s\n",
		fmt.Sprintf(format, args...))
}

// SuccessAfterf logs a success message after attempts
func (c *Logger) SuccessAfterf(attempts int, duration time.Duration, format string, args ...interface{}) {
	fmt.Printf("  %sSUCCESS%s - %s (attempts: %s%d%s, %s%s%s)\n\n",
		ColorBrightGreen, ColorReset,
		fmt.Sprintf(format, args...),
		ColorBrightWhite, attempts, ColorReset,
		ColorBrightWhite, formatDuration(duration), ColorReset)
}

// StatsSummaryf logs aggregated statistics
func (c *Logger) StatsSummaryf(sourceRegion, targetRegion string, stats ConvergenceStats) {
	immediatePercent := 0.0
	eventualPercent := 0.0
	timeoutPercent := 0.0

	if stats.Iterations > 0 {
		immediatePercent = float64(stats.ImmediateCount) * 100.0 / float64(stats.Iterations)
		eventualPercent = float64(stats.EventualCount) * 100.0 / float64(stats.Iterations)
		timeoutPercent = float64(stats.TimeoutCount) * 100.0 / float64(stats.Iterations)
	}

	fmt.Printf("  %s%s -> %s%s (%d iterations)\n",
		ColorYellow, getRegionDisplayName(sourceRegion), getRegionDisplayName(targetRegion), ColorReset,
		stats.Iterations)

	if stats.ImmediateCount+stats.EventualCount > 0 {
		fmt.Printf("    Convergence - Avg: %s%8s%s, P95: %s%8s%s, P99: %s%8s%s\n",
			ColorBrightWhite, formatDuration(stats.AvgTime), ColorReset,
			ColorBrightWhite, formatDuration(stats.P95Time), ColorReset,
			ColorBrightWhite, formatDuration(stats.P99Time), ColorReset)
	}

	fmt.Printf("    Distribution - Immediate: %5.1f%%, Eventual: %5.1f%%, Timeout: %5.1f%%\n",
		immediatePercent, eventualPercent, timeoutPercent)
}

// RunConsistencyTests runs the existing consistency tests
func RunConsistencyTests(t *TigrisValidator) bool {
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Printf(" %sCONSISTENCY TESTS%s\n", ColorBrightWhite, ColorReset)
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	allPassed := true

	// Test global endpoint if available
	if t.config.GlobalEndpoint != "" {
		fmt.Printf("\n%sTesting Global Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, t.config.GlobalEndpoint, ColorReset)
		if !runConsistencyTestsForEndpoint(t, "global", t.config.GlobalEndpoint) {
			allPassed = false
		}
	}

	// Test Regional endpoints
	for _, endpoint := range t.config.RegionalEndpoints {
		fmt.Printf("\n%sTesting Regional Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, endpoint, ColorReset)
		if !runConsistencyTestsForEndpoint(t, endpoint, endpoint) {
			allPassed = false
		}
	}

	return allPassed
}

// runConsistencyTestsForEndpoint runs consistency tests for a specific endpoint
func runConsistencyTestsForEndpoint(t *TigrisValidator, endpointName, endpointURL string) bool {
	client, exists := t.clients[endpointName]
	if !exists {
		fmt.Printf("  %sNo client available for endpoint%s\n", ColorBrightRed, ColorReset)
		return false
	}

	// Create a map of region to clients for the existing functions
	regionToClients := make(map[string]*s3.Client)
	regionToClients[endpointURL] = client

	// Add other regional clients if available
	for _, ep := range t.config.RegionalEndpoints {
		if ep != endpointName {
			if regionalClient, exists := t.clients[ep]; exists {
				regionToClients[ep] = regionalClient
			}
		}
	}

	// Build regions array with endpointURL as the first element (source region)
	regions := make([]string, 0, len(regionToClients))
	regions = append(regions, endpointURL) // Source region first
	for region := range regionToClients {
		if region != endpointURL {
			regions = append(regions, region)
		}
	}

	// Track if any test fails
	allPassed := true

	applyRemoteRegionsChecks(regionToClients, regions, t.config.BucketName, fmt.Sprintf("%s/consistency-test", t.config.Prefix))
	applyListConsistencyChecks(regionToClients, regions, t.config.BucketName)

	return allPassed
}
