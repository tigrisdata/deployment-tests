package main

// BenchmarkSize defines a test configuration for a specific object size
type BenchmarkSize struct {
	ObjectSize  int64  // Size in bytes
	DisplayName string // e.g., "1 MiB", "10 MiB"
	RecordCount int    // Records to preload (YCSB: recordcount)
	OpCount     int    // Operations for benchmark (YCSB: operationcount)
}

// DefaultBenchmarkSizes defines default test configurations following YCSB patterns
// Strategy: Keep dataset size roughly constant (~1 GB) across different object sizes
var DefaultBenchmarkSizes = []BenchmarkSize{
	{
		ObjectSize:  1 * 1024 * 1024, // 1 MiB
		DisplayName: "1 MiB",
		RecordCount: 10,  // 1000 records = ~1 GB dataset
		OpCount:     100, // 1000 ops for P99 accuracy
	},
	{
		ObjectSize:  10 * 1024 * 1024, // 10 MiB
		DisplayName: "10 MiB",
		RecordCount: 10,  // 100 records = ~1 GB dataset
		OpCount:     100, // 1000 ops
	},
	{
		ObjectSize:  100 * 1024 * 1024, // 100 MiB
		DisplayName: "100 MiB",
		RecordCount: 10,  // 10 records = ~1 GB dataset
		OpCount:     100, // 100 ops
	},
}

// TestConfig holds configuration for the performance test
type TestConfig struct {
	BucketName      string
	BenchmarkSizes  []BenchmarkSize // Test configurations for different object sizes
	Concurrency     int
	Prefix          string
	GlobalEndpoint  string
	USEndpoints     []string
	RunConnectivity bool
	RunConsistency  bool
	RunPerformance  bool
}
