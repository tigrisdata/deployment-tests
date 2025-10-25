package main

import (
	"context"
	"time"
)

// TestType represents the type of test
type TestType string

const (
	TestTypeConnectivity TestType = "connectivity"
	TestTypeConsistency  TestType = "consistency"
	TestTypePerformance  TestType = "performance"
)

// TestStatus represents the result of a test
type TestStatus struct {
	Passed   bool
	Duration time.Duration
	Message  string
	Details  map[string]interface{} // For storing test-specific details
}

// Test is the common interface that all test implementations must satisfy
type Test interface {
	// Name returns the display name of the test
	Name() string

	// Type returns the type of test
	Type() TestType

	// Run executes the test and returns the result
	Run(ctx context.Context) TestStatus

	// Setup performs any necessary setup before running the test
	Setup(ctx context.Context) error

	// Cleanup performs any necessary cleanup after running the test
	Cleanup(ctx context.Context) error
}
