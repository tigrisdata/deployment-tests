package main

import (
	"context"
	"fmt"
	"time"
)

// TestType represents the type of test
type TestType string

const (
	TestTypeConnectivity TestType = "connectivity"
	TestTypeConsistency  TestType = "consistency"
	TestTypePerformance  TestType = "performance"
	TestTypeTranscode    TestType = "transcode"
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
