package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ConnectivityTest implements the Test interface for connectivity testing
type ConnectivityTest struct {
	validator *TigrisValidator
}

// NewConnectivityTest creates a new connectivity test
func NewConnectivityTest(validator *TigrisValidator) *ConnectivityTest {
	return &ConnectivityTest{
		validator: validator,
	}
}

// Name returns the display name of the test
func (t *ConnectivityTest) Name() string {
	return "Connectivity Tests"
}

// Type returns the type of test
func (t *ConnectivityTest) Type() TestType {
	return TestTypeConnectivity
}

// Setup performs any necessary setup before running the test
func (t *ConnectivityTest) Setup(ctx context.Context) error {
	return nil
}

// Cleanup performs any necessary cleanup after running the test
func (t *ConnectivityTest) Cleanup(ctx context.Context) error {
	return nil
}

// Run executes the connectivity test
func (t *ConnectivityTest) Run(ctx context.Context) TestStatus {
	startTime := time.Now()

	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)
	fmt.Println(" CONNECTIVITY TESTS")
	fmt.Printf("%s%s%s\n", ColorYellow, strings.Repeat("=", 80), ColorReset)

	allPassed := true
	details := make(map[string]interface{})

	// Test global endpoint
	if t.validator.config.GlobalEndpoint != "" {
		fmt.Printf("\n%sTesting Global Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, t.validator.config.GlobalEndpoint, ColorReset)

		s3Duration, err := t.testS3Connectivity("global")
		if err != nil {
			fmt.Printf("  S3 Connectivity: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
			allPassed = false
			details["global"] = map[string]interface{}{
				"passed":   false,
				"error":    err.Error(),
				"duration": s3Duration,
			}
		} else {
			fmt.Printf("  S3 Connectivity: %sSUCCESS%s - %s\n", ColorBrightGreen, ColorReset, formatDuration(s3Duration))
			details["global"] = map[string]interface{}{
				"passed":   true,
				"duration": s3Duration,
			}
		}
	}

	// Test Regional endpoints
	for _, endpoint := range t.validator.config.RegionalEndpoints {
		fmt.Printf("\n%sTesting Regional Endpoint: %s%s%s\n", ColorBrightWhite, ColorYellow, endpoint, ColorReset)

		s3Duration, err := t.testS3Connectivity(endpoint)
		if err != nil {
			fmt.Printf("  S3 Connectivity: %sFAILED%s - %v\n", ColorBrightRed, ColorReset, err)
			allPassed = false
			details[endpoint] = map[string]interface{}{
				"passed":   false,
				"error":    err.Error(),
				"duration": s3Duration,
			}
		} else {
			fmt.Printf("  S3 Connectivity: %sSUCCESS%s - %s\n", ColorBrightGreen, ColorReset, formatDuration(s3Duration))
			details[endpoint] = map[string]interface{}{
				"passed":   true,
				"duration": s3Duration,
			}
		}
	}

	duration := time.Since(startTime)
	message := "All connectivity tests passed"
	if !allPassed {
		message = "Some connectivity tests failed"
	}

	return TestStatus{
		Passed:   allPassed,
		Duration: duration,
		Message:  message,
		Details:  details,
	}
}

// testS3Connectivity tests S3 connectivity to an endpoint
func (t *ConnectivityTest) testS3Connectivity(endpoint string) (time.Duration, error) {
	client, exists := t.validator.clients[endpoint]
	if !exists {
		return 0, fmt.Errorf("no client for endpoint: %s", endpoint)
	}

	start := time.Now()
	_, err := client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(t.validator.config.BucketName),
	})
	duration := time.Since(start)

	return duration, err
}
