package main

import (
	"fmt"
	"time"
)

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
