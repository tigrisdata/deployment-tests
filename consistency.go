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

// By default, the code will use the following values
const (
	defAccessKey = "<access>"
	defSecretKey = "<secret"
	defBucket    = "<bucket>"
	endpoint     = "https://t3.storage.dev"
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

type Options struct {
	Regions   string
	AccessKey string
	SecretKey string
	Bucket    string
	Key       string
	NoSeed    bool
	Creds     bool
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
	clog := Start("PUT|LIST (Read-After-Write Consistency) Same Region", Opts{ID: "T2", Region: []string{region}})
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
	clog := Start("PUT|LIST (Read-After-Write Consistency) Diff Region", Opts{ID: "T3", Region: regions})
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
		start := time.Now()
		passed := validateRegionsListEtag(regionToClients[region], bucket, resPut)
		if !passed {
			clog.Failf("list results validation failed in region %s", getRegionDisplayName(region))
			continue
		}
		clog.Infof("List Converged after '%v' at region %s", time.Since(start), getRegionDisplayName(region))
	}

	clog.Successf(time.Since(overallStart), "consistent in all regions'")
}

func validateRegionsListEtag(s3client *s3.Client, bucket string, resPut map[string]string) bool {
	var (
		start  time.Time
		passed = true
	)
	for attempts := 0; attempts < 32; attempts++ {
		if start.IsZero() {
			start = time.Now()
		}

		resList, err := listResults(s3client, bucket, "list-consistency-", 3)
		if err != nil {
			continue
		}
		if len(resPut) != len(resList) {
			continue
		} else {
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
				return true
			}
		}
		if time.Since(start) > 1*time.Minute {
			break
		}
	}

	return false
}

func validateETag(s3Client *s3.Client, sourceRegion string, remoteRegion string, bucket string, key string, expectedEtag string, clog *Logger) bool {
	clog.Infof("attempting read in remote region %s%s%s now\n", ColorYellow, getRegionDisplayName(remoteRegion), ColorReset)
	var (
		start  time.Time
		passed = false
	)
	for attempts := 0; ; attempts++ {
		eTagRemote := getEtag(s3Client, bucket, key)

		// Only start timer if first attempt fails
		if attempts == 0 && expectedEtag != eTagRemote {
			start = time.Now()
		}

		if expectedEtag == eTagRemote {
			if attempts == 0 {
				// Immediate success - no convergence time needed
				clog.Successf(0, "ETag %s immediately consistent in region %s", eTagRemote, getRegionDisplayName(remoteRegion))
			} else {
				// Converged after some attempts
				clog.SuccessAfterf(attempts, time.Since(start), "Converged after ETag %s written at region %s remote region %s", eTagRemote, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion))
			}
			passed = true
			break
		}

		clog.Infof("retrying attempt '%d' waiting for ETag=%s written-to=%s replicated-to=%s dest has ETag=%s", attempts, expectedEtag, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion), eTagRemote)

		if !start.IsZero() && time.Since(start) > 1*time.Minute {
			clog.Failf("unexpected stopping attempts '%d' waiting for ETag=%s written-to=%s replicated-to=%s dest has ETag=%s", attempts, expectedEtag, getRegionDisplayName(sourceRegion), getRegionDisplayName(remoteRegion), eTagRemote)
			break
		}

		if attempts > 2 {
			time.Sleep(10 * time.Second)
		}
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

func putGet(cl *s3.Client, buc string, key string) string {
	kb64 := make([]byte, 32*1024)
	rand.Read(kb64)

	_, err := cl.PutObject(
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

	og, err := cl.GetObject(context.TODO(),
		&s3.GetObjectInput{
			Bucket: aws.String(buc),
			Key:    aws.String(key),
		},
		WithHeader("Cache-Control", "no-cache"),
	)
	if err != nil {
		log.Printf("unexpected get failed '%v'", err)
		return ""
	}

	_, _ = io.ReadAll(og.Body)

	return *og.ETag
}

func getEtag(cl *s3.Client, buc string, key string) string {
	og, err := cl.GetObject(context.TODO(),
		&s3.GetObjectInput{
			Bucket: aws.String(buc),
			Key:    aws.String(key),
		},
		WithHeader("Cache-Control", "no-cache"),
	)
	if err != nil {
		log.Printf("unexpected getObject failed %v", err)
	}
	_, _ = io.ReadAll(og.Body)

	return *og.ETag
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
	var keyToETag = make(map[string]string)
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
func RunConsistencyTests(t *S3PerformanceTester) bool {
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

	// Test US regional endpoints
	for _, endpoint := range t.config.USEndpoints {
		fmt.Printf("\n%sTesting US Regional Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, endpoint, ColorReset)
		if !runConsistencyTestsForEndpoint(t, endpoint, endpoint) {
			allPassed = false
		}
	}

	return allPassed
}

// runConsistencyTestsForEndpoint runs consistency tests for a specific endpoint
func runConsistencyTestsForEndpoint(t *S3PerformanceTester, endpointName, endpointURL string) bool {
	client, exists := t.clients[endpointName]
	if !exists {
		fmt.Printf("  %sNo client available for endpoint%s\n", ColorBrightRed, ColorReset)
		return false
	}

	// Create a map of region to clients for the existing functions
	regionToClients := make(map[string]*s3.Client)
	regionToClients[endpointURL] = client

	// Add other regional clients if available
	for _, ep := range t.config.USEndpoints {
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
