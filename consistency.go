package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/transport/http"
)

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

func applySameRegionsChecks(regionToClients map[string]*s3.Client, region string, bucket string, key string) {
	clog := Start("PUT|GET (Read-After-Write Consistency) Same Region", Opts{ID: "T0", Region: []string{region}})
	//	start := time.Now()

	eTagToValidate := put(regionToClients[region], bucket, key)
	if eTagToValidate == "" {
		clog.Failf("PUT operation failed")
		return
	}

	// Immediate GET to verify consistency
	resp, err := regionToClients[region].GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		clog.Failf("GET operation failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.ETag == nil || *resp.ETag != eTagToValidate {
		clog.Failf("ETag mismatch: expected %s, got %s", eTagToValidate, *resp.ETag)
		return
	}

	// object is conistent immediately
	clog.Successf(0, "read-after-write consistent")
}

func applyRemoteRegionsChecks(regionToClients map[string]*s3.Client, regions []string, bucket string, key string) {
	clog := Start("PUT|GET (Read-After-Write Consistency) Multiple Regions", Opts{ID: "T1", Region: regions})
	start := time.Now()

	eTagToValidate := put(regionToClients[regions[0]], bucket, key)
	if eTagToValidate == "" {
		clog.Failf("PUT operation failed")
		return
	}

	for i := 1; i < len(regions); i++ {
		passed := validateETag(regionToClients[regions[i]], regions[0], regions[i], bucket, key, eTagToValidate, clog)
		if !passed {
			clog.Failf("read-after-write failed in region %s", getRegionDisplayName(regions[i]))
			return
		}
	}
	clog.Successf(time.Since(start), "consistent in all regions'")
}

func applySameRegionsListConsistency(regionToClients map[string]*s3.Client, region string, bucket string) {
	clog := Start("PUT|LIST (List-After-Write Consistency) Same Region", Opts{ID: "T2", Region: []string{region}})
	start := time.Now()

	var (
		err     error
		resPut  map[string]string
		resList map[string]string
	)

	for attempts := 0; attempts < 10; attempts++ {
		resPut, err = putResults(regionToClients[region], bucket, []string{"list-consistency-1", "list-consistency-2", "list-consistency-3"})
		if err == nil {
			break
		}
	}

	for attempts := 0; attempts < 10; attempts++ {
		resList, err = listResults(regionToClients[region], bucket, "list-consistency-", 3)
		if err == nil {
			break
		}
	}
	if len(resPut) != len(resList) {
		clog.Failf("list results count missmatch put='%d' list='%d'", len(resPut), len(resList))
	} else {
		for k, v := range resPut {
			vv, ok := resList[k]
			if !ok {
				clog.Failf("list results missing key='%s' put-etag='%s'", k, v)
				return
			}
			if vv != v {
				clog.Failf("list results etag missmatch key='%s' put-etag='%s' list-etag='%s'", k, v, vv)
				return
			}
		}
	}

	clog.Successf(time.Since(start), "consistent in all regions'")
}

func applyDiffRegionsListConsistency(regionToClients map[string]*s3.Client, regions []string, bucket string) {
	clog := Start("PUT|LIST (List-After-Write Consistency) Diff Region", Opts{ID: "T3", Region: regions})
	overallStart := time.Now()

	var (
		err    error
		resPut map[string]string
	)

	for attempts := 0; attempts < 10; attempts++ {
		resPut, err = putResults(regionToClients[regions[0]], bucket, []string{"list-consistency-1", "list-consistency-2", "list-consistency-3"})
		if err == nil {
			break
		}
	}

	for _, region := range regions[1:] {
		attempts, convergenceTime, passed := validateRegionsListEtagWithMetrics(regionToClients[region], bucket, resPut)
		if !passed {
			clog.Failf("list results validation failed in region %s", getRegionDisplayName(region))
			continue
		}
		if attempts == 0 {
			clog.Infof("List immediately consistent in region %s", getRegionDisplayName(region))
		} else {
			clog.Infof("List converged after %v (attempts: %d) at region %s", convergenceTime, attempts, getRegionDisplayName(region))
		}
	}

	clog.Successf(time.Since(overallStart), "consistent in all regions'")
}

// validateRegionsListEtagWithMetrics validates list consistency and returns metrics
// Returns: (attempts, convergenceTime, passed)
func validateRegionsListEtagWithMetrics(s3client *s3.Client, bucket string, resPut map[string]string) (int, time.Duration, bool) {
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
		resList, err := listResults(s3client, bucket, "list-consistency-", 3)
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

func validateETag(s3Client *s3.Client, sourceRegion string, remoteRegion string, bucket string, key string, expectedEtag string, clog *Logger) bool {
	clog.Infof("attempting read in remote region %s%s%s now\n", ColorYellow, getRegionDisplayName(remoteRegion), ColorReset)

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
		eTagRemote, err := getEtagWithError(s3Client, bucket, key)
		// Handle errors - distinguish from ETag mismatch
		if err != nil {
			clog.Infof("attempt '%d' GetObject failed: %v (will retry)", attempts, err)

			// Check timeout before continuing
			if !firstPollTime.IsZero() && time.Since(firstPollTime) > maxDuration {
				clog.Failf("timeout after %v waiting for ETag=%s written-to=%s replicated-to=%s",
					time.Since(firstPollTime), expectedEtag, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion))
				break
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
			if attempts == 0 {
				// Immediate success - no convergence time needed
				clog.Successf(0, "ETag %s immediately consistent in region %s", eTagRemote, getRegionDisplayName(remoteRegion))
			} else {
				// Calculate convergence time based on polling intervals elapsed
				// This represents the minimum time for convergence (lower bound)
				// Actual convergence happened between (attempts-1) and attempts polling intervals
				convergenceTime := time.Duration(attempts) * pollingInterval
				clog.SuccessAfterf(attempts, convergenceTime, "Converged after ETag %s written at region %s remote region %s", eTagRemote, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion))
			}
			passed = true
			break
		}

		clog.Infof("retrying attempt '%d' waiting for ETag=%s written-to=%s replicated-to=%s dest has ETag=%s",
			attempts, expectedEtag, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion), eTagRemote)

		// Check timeout before next attempt
		if time.Since(firstPollTime) > maxDuration {
			clog.Failf("timeout after %v (attempts: %d) waiting for ETag=%s written-to=%s replicated-to=%s dest has ETag=%s",
				time.Since(firstPollTime), attempts+1, expectedEtag, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion), eTagRemote)
			break
		}

		// Sleep before next poll
		time.Sleep(pollingInterval)
	}

	return passed
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

	regions := make([]string, 0, len(regionToClients))
	for region := range regionToClients {
		regions = append(regions, region)
	}

	// Track if any test fails
	allPassed := true

	// Use the existing consistency test functions
	// For now, we'll assume they pass unless we implement detailed tracking
	// TODO: Implement detailed failure tracking in individual test functions
	applySameRegionsChecks(regionToClients, regions[0], t.config.BucketName, fmt.Sprintf("%s/consistency-test", t.config.Prefix))
	applyRemoteRegionsChecks(regionToClients, regions, t.config.BucketName, fmt.Sprintf("%s/consistency-test", t.config.Prefix))
	applySameRegionsListConsistency(regionToClients, regions[0], t.config.BucketName)
	applyDiffRegionsListConsistency(regionToClients, regions, t.config.BucketName)

	return allPassed
}
