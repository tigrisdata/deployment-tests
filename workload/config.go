package workload

import "time"

// OpType defines the type of S3 operation
type OpType int

const (
	OpPUT OpType = iota
	OpGET
	OpMIXED
)

func (o OpType) String() string {
	switch o {
	case OpPUT:
		return "PUT"
	case OpGET:
		return "GET"
	case OpMIXED:
		return "MIXED"
	default:
		return "UNKNOWN"
	}
}

// KeyDistribution defines how keys are selected during benchmark
type KeyDistribution string

const (
	DistUniform    KeyDistribution = "uniform"    // Random uniform distribution
	DistSequential KeyDistribution = "sequential" // Sequential access
	DistZipfian    KeyDistribution = "zipfian"    // Zipfian distribution (hot keys)
)

// WorkloadConfig defines the configuration for a workload benchmark
// Following YCSB pattern: RecordCount for preload, OperationCount for run phase
type WorkloadConfig struct {
	// S3 Configuration
	Bucket   string
	Endpoint string
	Prefix   string

	// Workload Parameters (YCSB-style)
	ObjectSize     int64         // Size of objects in bytes
	RecordCount    int           // Number of records to preload (YCSB: recordcount)
	OperationCount int           // Number of operations to run (YCSB: operationcount), 0 = use duration
	TestDuration   time.Duration // Duration to run (alternative to OperationCount)
	Concurrency    int

	// Operation Mix
	OperationType  OpType  // PUT, GET, or MIXED
	ReadWriteRatio float64 // For MIXED workload: 0.0 = all writes, 1.0 = all reads

	// Randomness & Distribution
	Seed            int64
	KeyDistribution KeyDistribution

	// Performance Options
	ReuseObjects bool // Cycle through objects (for throughput tests)
}

// Validate checks if the configuration is valid
func (c *WorkloadConfig) Validate() error {
	if c.Bucket == "" {
		return &ConfigError{Field: "Bucket", Message: "bucket name is required"}
	}
	if c.Endpoint == "" {
		return &ConfigError{Field: "Endpoint", Message: "endpoint is required"}
	}
	if c.ObjectSize <= 0 {
		return &ConfigError{Field: "ObjectSize", Message: "object size must be positive"}
	}
	if c.RecordCount <= 0 {
		return &ConfigError{Field: "RecordCount", Message: "record count must be positive"}
	}
	if c.Concurrency <= 0 {
		return &ConfigError{Field: "Concurrency", Message: "concurrency must be positive"}
	}
	if c.OperationCount == 0 && c.TestDuration == 0 {
		return &ConfigError{Field: "OperationCount/TestDuration", Message: "either OperationCount or TestDuration must be set"}
	}
	if c.OperationType == OpMIXED {
		if c.ReadWriteRatio < 0 || c.ReadWriteRatio > 1 {
			return &ConfigError{Field: "ReadWriteRatio", Message: "ratio must be between 0 and 1"}
		}
	}
	return nil
}

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return "config error in field " + e.Field + ": " + e.Message
}
